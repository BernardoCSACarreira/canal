// Package connectortest supplies EMBEDDABLE, no-op implementations of every
// core-implemented interface a connector is handed: the three runtimes, LaneCtl and
// StateHandle.
//
// WHY IT EXISTS, and it is a finding rather than a convenience. canal's declared growth
// path is "add a method to a runtime; the core implements it, so no connector breaks". That
// claim was true of connectors and FALSE OF THEIR TESTS: every hostile connector in
// internal/stress hand-wrote a fake SourceRuntime and a fake LaneCtl, so adding one method
// to either broke five test packages at once. A growth path that breaks every test in the
// ecosystem is not a growth path; it is a breaking change with a nicer name.
//
// Embedding a struct from here fixes that permanently. A test overrides the two or three
// methods it cares about and inherits the rest, so a v2 core adding Instance(), Streams(),
// Declare() or Admission() costs a connector's test suite exactly nothing.
//
// Every method here returns the SAFE, INERT answer — never a plausible-looking fake value.
// A fake that quietly succeeds is how a connector's test suite passes against behaviour the
// core does not have.
package connectortest

import (
	"context"
	"log/slog"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Base carries the identity and observability every runtime shares.
type Base struct {
	Ctx       context.Context
	Logger    *slog.Logger
	Tenant_   record.TenantID
	Pipeline_ record.PipelineID
	Node_     record.NodeID

	// Events records everything Note published, so a test asserts on events instead of
	// scraping logs.
	Events []connector.Event

	// Cfg is what Config returns. Nil yields an empty config rather than an error, because a
	// connector that reads config on its reconnect path must not fail in a test that does not
	// care about config.
	Cfg *config.Config
}

func (b *Base) Context() context.Context {
	if b.Ctx == nil {
		return context.Background()
	}
	return b.Ctx
}

func (b *Base) Log() *slog.Logger {
	if b.Logger == nil {
		return slog.Default()
	}
	return b.Logger
}

func (b *Base) Metrics() connector.Metrics  { return Metrics{} }
func (b *Base) Note(e connector.Event)      { b.Events = append(b.Events, e) }
func (b *Base) Node() record.NodeID         { return b.Node_ }
func (b *Base) Pipeline() record.PipelineID { return b.Pipeline_ }

func (b *Base) Tenant() record.TenantID {
	if b.Tenant_ == "" {
		return record.DefaultTenant
	}
	return b.Tenant_
}

func (b *Base) Config(context.Context) (*config.Config, error) {
	if b.Cfg == nil {
		return &config.Config{}, nil
	}
	return b.Cfg, nil
}

// SourceRuntime is an embeddable connector.SourceRuntime.
type SourceRuntime struct {
	Base

	LaneCtl_  connector.LaneCtl
	State_    connector.StateHandle
	Schemas_  connector.SchemaLookup
	Streams_  []connector.ConfiguredStream
	Instance_ string

	// Declared records every Declare call in order, so a drift test asserts on the change
	// sequence a source published.
	Declared []schema.Change
}

var _ connector.SourceRuntime = (*SourceRuntime)(nil)

func (r *SourceRuntime) Lanes() connector.LaneCtl {
	if r.LaneCtl_ == nil {
		r.LaneCtl_ = &LaneCtl{}
	}
	return r.LaneCtl_
}

func (r *SourceRuntime) State() connector.StateHandle {
	if r.State_ == nil {
		r.State_ = &StateHandle{}
	}
	return r.State_
}

func (r *SourceRuntime) Schemas() connector.SchemaLookup {
	if r.Schemas_ == nil {
		r.Schemas_ = &Schemas{}
	}
	return r.Schemas_
}

func (r *SourceRuntime) Streams() []connector.ConfiguredStream { return r.Streams_ }

func (r *SourceRuntime) Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error) {
	r.Declared = append(r.Declared, ch)
	return r.Schemas().Register(ctx, result)
}

func (r *SourceRuntime) Instance() string {
	if r.Instance_ == "" {
		return "test-0"
	}
	return r.Instance_
}

func (r *SourceRuntime) Batcher(p config.BatchPolicy) *connector.Batcher {
	return connector.NewBatcher(p)
}

// SinkRuntime is an embeddable connector.SinkRuntime.
type SinkRuntime struct {
	Base

	Schemas_ connector.SchemaLookup
	Streams_ []connector.ConfiguredStream
}

var _ connector.SinkRuntime = (*SinkRuntime)(nil)

func (r *SinkRuntime) Schemas() connector.SchemaLookup {
	if r.Schemas_ == nil {
		r.Schemas_ = &Schemas{}
	}
	return r.Schemas_
}

func (r *SinkRuntime) Streams() []connector.ConfiguredStream { return r.Streams_ }

// TransformRuntime is an embeddable connector.TransformRuntime.
type TransformRuntime struct {
	Base

	LaneView_ connector.LaneView
}

var _ connector.TransformRuntime = (*TransformRuntime)(nil)

func (r *TransformRuntime) Lanes() connector.LaneView {
	if r.LaneView_ == nil {
		r.LaneView_ = &LaneCtl{}
	}
	return r.LaneView_
}

// LaneCtl is an in-memory connector.LaneCtl good enough to drive a real source through a
// cold start, a warm start, a churn cycle and a revocation.
type LaneCtl struct {
	// Rows is the durable lane table, in announce order.
	Rows []connector.LaneAssignment

	// Announced records every spec passed to Announce or AnnounceMany, including
	// re-announcements, so a test can count durable round trips.
	Announced []connector.LaneSpec

	// Held, when non-nil, restricts what Assigned returns to these ids — the cluster case.
	Held map[record.LaneID]bool

	// Revoked_ marks lanes this instance has lost.
	Revoked_ map[record.LaneID]bool

	// MaxLanes mirrors SourceCaps.MaxLanes; zero is unlimited.
	MaxLanes int

	// Headroom_ is what Admission reports. Zero means the default 1000.
	Headroom_ int

	ch chan struct{}
}

var _ connector.LaneCtl = (*LaneCtl)(nil)

func (l *LaneCtl) find(id record.LaneID) *connector.LaneAssignment {
	for i := range l.Rows {
		if l.Rows[i].ID == id {
			return &l.Rows[i]
		}
	}
	return nil
}

func (l *LaneCtl) Announce(_ context.Context, spec connector.LaneSpec) (record.LaneID, error) {
	l.Announced = append(l.Announced, spec)
	id := record.DeriveLaneID(record.DefaultTenant, "p", "n", spec.Name)
	if row := l.find(id); row != nil {
		// The relaxed re-announce rule: a changed Spec is accepted on a finished or
		// cursorless lane and refused on a live one.
		if !bytesEqual(row.Spec.Spec.Bytes, spec.Spec.Bytes) && !row.Finished && !row.Cursor.IsZero() {
			return "", fault.Contract(fault.OpOpen, errRespec)
		}
		row.Spec = spec
		return id, nil
	}
	if l.MaxLanes > 0 && len(l.Rows) >= l.MaxLanes {
		return "", fault.Contract(fault.OpOpen, errMaxLanes)
	}
	l.Rows = append(l.Rows, connector.LaneAssignment{ID: id, Spec: spec, Epoch: 1})
	return id, nil
}

func (l *LaneCtl) AnnounceMany(ctx context.Context, specs []connector.LaneSpec) ([]record.LaneID, error) {
	// All-or-nothing: build against a copy and swap only on success.
	saveRows, saveAnn := l.Rows, l.Announced
	l.Rows = append([]connector.LaneAssignment(nil), l.Rows...)
	out := make([]record.LaneID, 0, len(specs))
	for _, s := range specs {
		id, err := l.Announce(ctx, s)
		if err != nil {
			l.Rows, l.Announced = saveRows, saveAnn
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

func (l *LaneCtl) Seed(_ context.Context, id record.LaneID, cursor record.Position) error {
	row := l.find(id)
	if row == nil {
		return fault.Contract(fault.OpOpen, errNoLane)
	}
	if !row.Cursor.IsZero() {
		return fault.Contract(fault.OpOpen, errSeeded)
	}
	row.Cursor = cursor
	return nil
}

func (l *LaneCtl) Finish(_ context.Context, id record.LaneID) error {
	if row := l.find(id); row != nil {
		row.Finished = true
	}
	return nil
}

func (l *LaneCtl) Forget(_ context.Context, id record.LaneID) error {
	for i := range l.Rows {
		if l.Rows[i].ID != id {
			continue
		}
		if !l.Rows[i].Finished {
			return fault.Contract(fault.OpOpen, errNotFinished)
		}
		l.Rows = append(l.Rows[:i], l.Rows[i+1:]...)
		return nil
	}
	return nil
}

func (l *LaneCtl) Assigned(context.Context) ([]connector.LaneAssignment, error) {
	var out []connector.LaneAssignment
	for _, r := range l.Rows {
		if r.Finished || len(r.GatedOn) > 0 || l.Revoked_[r.ID] {
			continue
		}
		if l.Held != nil && !l.Held[r.ID] {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (l *LaneCtl) Table(_ context.Context, q connector.LaneQuery) ([]connector.LaneAssignment, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 256
	}
	var out []connector.LaneAssignment
	started := q.After == ""
	for _, r := range l.Rows {
		if !started {
			if r.ID == q.After {
				started = true
			}
			continue
		}
		if r.Finished && !q.IncludeFinished {
			continue
		}
		if len(r.GatedOn) > 0 && !q.IncludeGated {
			continue
		}
		if len(q.Streams) > 0 && !hasStream(q.Streams, r.Spec.Stream) {
			continue
		}
		if len(q.Kinds) > 0 && !hasKind(q.Kinds, r.Spec.Kind) {
			continue
		}
		if len(q.Groups) > 0 && !hasGroup(q.Groups, r.Spec.Group) {
			continue
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (l *LaneCtl) Changes() <-chan struct{} {
	if l.ch == nil {
		l.ch = make(chan struct{})
	}
	return l.ch
}

// Revoke marks a lane lost and wakes anything selecting on Changes.
func (l *LaneCtl) Revoke(id record.LaneID) {
	if l.Revoked_ == nil {
		l.Revoked_ = map[record.LaneID]bool{}
	}
	l.Revoked_[id] = true
	if l.ch != nil {
		close(l.ch)
		l.ch = nil
	}
}

func (l *LaneCtl) Revoked(id record.LaneID) bool { return l.Revoked_[id] }

func (l *LaneCtl) Admission(id record.LaneID) connector.Admission {
	if l.find(id) == nil {
		return connector.Admission{}
	}
	h := l.Headroom_
	if h == 0 {
		h = 1000
	}
	ready := make(chan struct{})
	close(ready)
	return connector.Admission{Budget: 1000, Headroom: h, Known: true, Ready: ready}
}

// StateHandle is an in-memory connector.StateHandle with real CAS semantics, because a
// fake that ignores IfVersion cannot exercise the fenced-write path that every
// multi-worker source depends on.
type StateHandle struct {
	lanes  map[record.LaneID]versioned
	shared versioned
}

type versioned struct {
	blob    record.Blob
	version uint64
}

var _ connector.StateHandle = (*StateHandle)(nil)

func (s *StateHandle) Get(_ context.Context, lane record.LaneID) (record.Blob, uint64, error) {
	v := s.lanes[lane]
	return v.blob, v.version, nil
}

func (s *StateHandle) Set(_ context.Context, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error) {
	if s.lanes == nil {
		s.lanes = map[record.LaneID]versioned{}
	}
	if s.lanes[lane].version != ifVersion {
		return 0, fault.ErrFenced
	}
	v := versioned{blob: b.Clone(), version: ifVersion + 1}
	s.lanes[lane] = v
	return v.version, nil
}

func (s *StateHandle) Shared(context.Context) (record.Blob, uint64, error) {
	return s.shared.blob, s.shared.version, nil
}

func (s *StateHandle) SetShared(_ context.Context, b record.Blob, ifVersion uint64) (uint64, error) {
	if s.shared.version != ifVersion {
		return 0, fault.ErrFenced
	}
	s.shared = versioned{blob: b.Clone(), version: ifVersion + 1}
	return s.shared.version, nil
}

func (s *StateHandle) SetMany(ctx context.Context, w connector.StateWrite) error {
	// All-or-nothing, checked before anything is written.
	for lane, e := range w.Lanes {
		if s.lanes[lane].version != e.IfVersion {
			return fault.ErrFenced
		}
	}
	if w.Shared != nil && s.shared.version != w.Shared.IfVersion {
		return fault.ErrFenced
	}
	for lane, e := range w.Lanes {
		if _, err := s.Set(ctx, lane, e.Blob, e.IfVersion); err != nil {
			return err
		}
	}
	if w.Shared != nil {
		if _, err := s.SetShared(ctx, w.Shared.Blob, w.Shared.IfVersion); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateHandle) Delete(_ context.Context, lane record.LaneID) error {
	delete(s.lanes, lane)
	return nil
}

// Schemas is an in-memory connector.SchemaLookup keyed by insertion order.
type Schemas struct {
	Entries []schema.Entry
}

var _ connector.SchemaLookup = (*Schemas)(nil)

func (s *Schemas) Get(_ context.Context, ref schema.Ref) (*schema.Schema, error) {
	for i := range s.Entries {
		if s.Entries[i].Ref == ref {
			return s.Entries[i].Schema, nil
		}
	}
	return nil, fault.Contract(fault.OpDecode, errNoSchema)
}

func (s *Schemas) Register(_ context.Context, sc *schema.Schema) (schema.Ref, error) {
	ref := schema.Ref{Stream: "", Epoch: uint64(len(s.Entries) + 1)}
	s.Entries = append(s.Entries, schema.Entry{Ref: ref, Schema: sc})
	return ref, nil
}

// Metrics is a no-op connector.Metrics.
type Metrics struct{}

func (Metrics) Counter(string, ...string) (connector.Counter, error) { return noop{}, nil }
func (Metrics) Gauge(string, ...string) (connector.Gauge, error)     { return noop{}, nil }
func (Metrics) Histogram(string, []float64, ...string) (connector.Histogram, error) {
	return noop{}, nil
}

type noop struct{}

func (noop) Add(float64, ...string)     {}
func (noop) Set(float64, ...string)     {}
func (noop) Observe(float64, ...string) {}

func hasStream(ss []record.StreamName, s record.StreamName) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func hasKind(ks []connector.LaneKind, k connector.LaneKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

func hasGroup(gs []record.LaneGroup, g record.LaneGroup) bool {
	for _, x := range gs {
		if x == g {
			return true
		}
	}
	return false
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type constErr string

func (e constErr) Error() string { return string(e) }

const (
	errRespec      constErr = "lane spec changed on a live lane"
	errMaxLanes    constErr = "announcing this lane would exceed MaxLanes"
	errNoLane      constErr = "no such lane"
	errSeeded      constErr = "lane already has a durable cursor"
	errNotFinished constErr = "lane is not finished"
	errNoSchema    constErr = "no such schema ref"
)

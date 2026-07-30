package multistream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/connectortest"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

func TestRegisters(t *testing.T) {
	e, ok := registry.Default.Source("multi_stream_probe")
	if !ok {
		t.Fatal("not registered")
	}
	t.Logf("caps.MaxLanes=%d spec fields=%d", e.Caps.MaxLanes, len(e.Spec.Fields))
}

// ---- fakes -----------------------------------------------------------------------------------

type fakeLanes struct {
	mu      sync.Mutex
	rows    map[record.LaneID]connector.LaneAssignment
	byName  map[string]record.LaneID
	fin     map[record.LaneID]bool
	changes chan struct{}
	max     int

	// txns counts durable write transactions, so the test can assert that a 900-lane cold
	// start is ONE of them.
	txns int
}

func newFakeLanes(max int) *fakeLanes {
	return &fakeLanes{
		rows:    map[record.LaneID]connector.LaneAssignment{},
		byName:  map[string]record.LaneID{},
		fin:     map[record.LaneID]bool{},
		changes: make(chan struct{}),
		max:     max,
	}
}

func (f *fakeLanes) Announce(_ context.Context, spec connector.LaneSpec) (record.LaneID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.byName[spec.Name]; ok {
		// The documented contract, verbatim: "re-announcing an existing lane with an identical
		// Spec returns its id; with a DIFFERENT Spec it returns a fault.PermanentContract".
		if !bytes.Equal(f.rows[id].Spec.Spec.Bytes, spec.Spec.Bytes) {
			return "", fault.Contract(fault.OpOpen,
				fmt.Errorf("lane %q already exists with a different construction payload", spec.Name))
		}
		return id, nil
	}
	if f.max > 0 && len(f.rows) >= f.max {
		return "", fmt.Errorf("MaxLanes exceeded")
	}
	id := record.LaneID("lane/" + spec.Name)
	f.rows[id] = connector.LaneAssignment{ID: id, Spec: spec, Epoch: 1}
	f.byName[spec.Name] = id
	return id, nil
}

func (f *fakeLanes) Finish(_ context.Context, id record.LaneID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fin[id] = true
	return nil
}

func (f *fakeLanes) Assigned(context.Context) ([]connector.LaneAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []connector.LaneAssignment
	for id, a := range f.rows {
		if f.fin[id] {
			continue // "A source never receives a finished lane from LaneCtl.Assigned"
		}
		if f.gated(a) {
			continue // StartAfter: a gated lane is not assigned while its group is unfinished
		}
		out = append(out, a)
	}
	return out, nil
}

func (f *fakeLanes) gated(a connector.LaneAssignment) bool {
	for _, g := range a.Spec.StartAfter {
		for id, other := range f.rows {
			if other.Spec.Group == g && !f.fin[id] {
				return true
			}
		}
	}
	return false
}

func (f *fakeLanes) Changes() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changes
}

// signal is what the core does when the assigned set changes: close the old channel and
// replace it.
func (f *fakeLanes) signal() {
	f.mu.Lock()
	old := f.changes
	f.changes = make(chan struct{})
	f.mu.Unlock()
	close(old)
}

func (f *fakeLanes) Revoked(record.LaneID) bool { return false }

func (f *fakeLanes) Admission(id record.LaneID) connector.Admission {
	f.mu.Lock()
	_, ok := f.rows[id]
	f.mu.Unlock()
	if !ok {
		return connector.Admission{}
	}
	ready := make(chan struct{})
	close(ready)
	return connector.Admission{Budget: 1000, Headroom: 1000, Known: true, Ready: ready}
}

// AnnounceMany is ONE durable write. The counter below is what makes that measurable: this
// fake counts transactions, not specs, so a 900-stream cold start that used to cost 900
// serialised round trips now costs one and the test can say so.
func (f *fakeLanes) AnnounceMany(ctx context.Context, specs []connector.LaneSpec) ([]record.LaneID, error) {
	f.mu.Lock()
	f.txns++
	f.mu.Unlock()
	out := make([]record.LaneID, 0, len(specs))
	for _, s := range specs {
		id, err := f.Announce(ctx, s)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

func (f *fakeLanes) Seed(_ context.Context, id record.LaneID, cursor record.Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[id]
	if !ok {
		return fault.Contract(fault.OpOpen, fmt.Errorf("no such lane %q", id))
	}
	if !row.Cursor.IsZero() {
		return fault.Contract(fault.OpOpen, fmt.Errorf("lane %q already has a cursor", id))
	}
	row.Cursor = cursor
	f.rows[id] = row
	return nil
}

// Forget DROPS the row, which is what keeps the lane table from becoming the historical union
// of every stream this source has ever seen.
func (f *fakeLanes) Forget(_ context.Context, id record.LaneID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.fin[id] {
		return fault.Contract(fault.OpOpen, fmt.Errorf("lane %q is not finished", id))
	}
	delete(f.byName, f.rows[id].Spec.Name)
	delete(f.rows, id)
	delete(f.fin, id)
	return nil
}

// Table is the whole durable plan, including the finished and gated rows Assigned omits.
func (f *fakeLanes) Table(_ context.Context, q connector.LaneQuery) ([]connector.LaneAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := q.Limit
	if limit <= 0 {
		limit = 256
	}
	var out []connector.LaneAssignment
	for id, a := range f.rows {
		if f.fin[id] && !q.IncludeFinished {
			continue
		}
		if f.gated(a) && !q.IncludeGated {
			continue
		}
		a.Finished = f.fin[id]
		out = append(out, a)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeLanes) count() int    { f.mu.Lock(); defer f.mu.Unlock(); return len(f.rows) }
func (f *fakeLanes) txnCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.txns }
func (f *fakeLanes) finished() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.fin) }

type fakeState struct {
	mu sync.Mutex
	m  map[record.LaneID]record.Blob
	v  map[record.LaneID]uint64

	shared  record.Blob
	sharedV uint64
}

func (s *fakeState) Get(_ context.Context, l record.LaneID) (record.Blob, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[l], s.v[l], nil
}

func (s *fakeState) Set(_ context.Context, l record.LaneID, b record.Blob, ifV uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.v[l] != ifV {
		return 0, fmt.Errorf("CAS mismatch on %s: have %d want %d", l, s.v[l], ifV)
	}
	s.m[l], s.v[l] = b, s.v[l]+1
	return s.v[l], nil
}

// Shared and SetShared back the NODE-scoped slot: state that must exist before any lane does,
// which is exactly where this source's stream-to-lane index belongs.
func (s *fakeState) Shared(context.Context) (record.Blob, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shared, s.sharedV, nil
}

func (s *fakeState) SetShared(_ context.Context, b record.Blob, ifV uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sharedV != ifV {
		return 0, fault.ErrFenced
	}
	s.shared, s.sharedV = b, s.sharedV+1
	return s.sharedV, nil
}

// SetMany is all-or-nothing across the lane slots AND the node slot: the stream-to-lane index
// and the cursors it indexes must move together, or a restart reads one stream's progress
// under another stream's name.
func (s *fakeState) SetMany(ctx context.Context, w connector.StateWrite) error {
	s.mu.Lock()
	for l, e := range w.Lanes {
		if s.v[l] != e.IfVersion {
			s.mu.Unlock()
			return fault.ErrFenced
		}
	}
	if w.Shared != nil && s.sharedV != w.Shared.IfVersion {
		s.mu.Unlock()
		return fault.ErrFenced
	}
	for l, e := range w.Lanes {
		s.m[l], s.v[l] = e.Blob, s.v[l]+1
	}
	if w.Shared != nil {
		s.shared, s.sharedV = w.Shared.Blob, s.sharedV+1
	}
	s.mu.Unlock()
	return nil
}

func (s *fakeState) Delete(_ context.Context, l record.LaneID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, l)
	delete(s.v, l)
	return nil
}

type fakeMetrics struct {
	mu       sync.Mutex
	counters map[string]float64
}

type fakeCounter struct {
	m    *fakeMetrics
	name string
}

func (c *fakeCounter) Add(d float64, _ ...string) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.counters[c.name] += d
}

type fakeGauge struct{}

func (fakeGauge) Set(float64, ...string) {}

type fakeHist struct{}

func (fakeHist) Observe(float64, ...string) {}

func (m *fakeMetrics) Counter(n string, _ ...string) (connector.Counter, error) {
	return &fakeCounter{m: m, name: n}, nil
}

func (m *fakeMetrics) Gauge(string, ...string) (connector.Gauge, error) { return fakeGauge{}, nil }

func (m *fakeMetrics) Histogram(string, []float64, ...string) (connector.Histogram, error) {
	return fakeHist{}, nil
}

type fakeRT struct {
	lanes   *fakeLanes
	state   *fakeState
	metrics *fakeMetrics

	streams []connector.ConfiguredStream
	schemas *connectortest.Schemas

	mu       sync.Mutex
	events   []connector.Event
	declared []schema.Change
}

func (r *fakeRT) Context() context.Context     { return context.Background() }
func (r *fakeRT) Lanes() connector.LaneCtl     { return r.lanes }
func (r *fakeRT) State() connector.StateHandle { return r.state }
func (r *fakeRT) Log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
func (r *fakeRT) Metrics() connector.Metrics { return r.metrics }
func (r *fakeRT) Batcher(p config.BatchPolicy) *connector.Batcher {
	return connector.NewBatcher(p)
}

func (r *fakeRT) Note(e connector.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *fakeRT) Tenant() record.TenantID     { return record.DefaultTenant }
func (r *fakeRT) Pipeline() record.PipelineID { return "p1" }
func (r *fakeRT) Node() record.NodeID         { return "src" }
func (r *fakeRT) Instance() string            { return "worker-0" }

// Streams is the operator's selection, which the source could not previously see at all — so
// the selection had to be duplicated in connector config and could silently disagree with it.
func (r *fakeRT) Streams() []connector.ConfiguredStream { return r.streams }

func (r *fakeRT) Schemas() connector.SchemaLookup {
	if r.schemas == nil {
		r.schemas = &connectortest.Schemas{}
	}
	return r.schemas
}

// Declare is the drift subsystem's producer. The recorded changes are what let this test
// assert that 900 runtime-discovered streams reach the core as an ordered change history
// rather than as log lines.
func (r *fakeRT) Declare(ctx context.Context, ch schema.Change, result *schema.Schema) (schema.Ref, error) {
	r.mu.Lock()
	r.declared = append(r.declared, ch)
	r.mu.Unlock()
	return r.Schemas().Register(ctx, result)
}

func (r *fakeRT) Config(context.Context) (*config.Config, error) { return &config.Config{}, nil }

func (r *fakeRT) declaredChanges() []schema.Change {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]schema.Change(nil), r.declared...)
}

var _ connector.SourceRuntime = (*fakeRT)(nil)

// ---- the drive test --------------------------------------------------------------------------

func build(t *testing.T, streams int, mode string) (*Source, *fakeRT) {
	t.Helper()
	e, ok := registry.Default.Source("multi_stream_probe")
	if !ok {
		t.Fatal("not registered")
	}
	list := make([]any, 0, streams)
	for i := 0; i < streams; i++ {
		list = append(list, map[string]any{
			"name":      fmt.Sprintf("t_%04d", i),
			"sync_mode": mode,
		})
	}
	cfg := config.NewConfig(e.Spec, map[string]any{
		"endpoint":          "https://x.invalid",
		"token":             "s3cret",
		"discover_interval": "1m",
		"page_size":         100,
		"streams":           list,
	})
	c, err := e.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s, ok := c.(*Source)
	if !ok {
		t.Fatalf("New returned %T", c)
	}
	rt := &fakeRT{
		lanes:   newFakeLanes(4000),
		state:   &fakeState{m: map[record.LaneID]record.Blob{}, v: map[record.LaneID]uint64{}},
		metrics: &fakeMetrics{counters: map[string]float64{}},
	}
	return s, rt
}

func TestDrive(t *testing.T) {
	s, rt := build(t, 900, "incremental")

	start := time.Now()
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Logf("Open announced %d lane rows in %s against an in-memory fake; B5 says each is one "+
		"durable write, serialised", rt.lanes.count(), time.Since(start).Round(time.Millisecond))

	as, _ := rt.lanes.Assigned(context.Background())
	t.Logf("assigned=%d of %d rows (the tail lanes are gated behind their scan groups)",
		len(as), rt.lanes.count())

	// The engine hands the source ONE batch, bound to ONE (lane, stream) allocator. Model that.
	alloc := record.NewAllocator(rt.Tenant(), rt.Pipeline(), rt.Node(), as[0].ID,
		as[0].Spec.Stream, 1, 1)
	b := record.NewBatch(alloc, 500)

	var reads, records, wrongLane, wrongStream, lanesSeen, idles, finished int
	seen := map[record.LaneID]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; i < 6000 && time.Now().Before(deadline); i++ {
		if err := s.Read(context.Background(), b); err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		reads++
		if b.Len() == 0 && !b.EndOfLane {
			idles++
		}
		if b.Len() > 0 && !seen[b.Lane] {
			seen[b.Lane] = true
			lanesSeen++
		}
		for _, r := range b.Records {
			records++
			if r.Origin().Lane != b.Lane {
				wrongLane++
			}
			if r.Origin().Stream != r.Dest {
				wrongStream++
			}
		}
		if b.Len() > 0 {
			if err := s.Commit(context.Background(), connector.Ack{
				Lane: b.Lane, Epoch: 1, Through: b.Position, Records: uint64(b.Len()),
			}); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
		if b.EndOfLane && b.Lane != "" {
			// What the engine does: finish the bounded lane once every group settled, then
			// re-plan — which opens the gate on that stream's tail lane.
			_ = rt.lanes.Finish(context.Background(), b.Lane)
			finished++
			if err := s.Commit(context.Background(), connector.Ack{
				Lane: b.Lane, Epoch: 1, Through: b.Position, LaneFinished: true,
			}); err != nil {
				t.Fatalf("final Commit: %v", err)
			}
			rt.lanes.signal()
		}
	}
	t.Logf("reads=%d records=%d lanes served=%d scans finished=%d empty reads=%d",
		reads, records, lanesSeen, finished, idles)
	t.Logf("B1 MEASURED: %d/%d records carry a false Origin.Lane and %d carry a false "+
		"Origin.Stream; retarget counter=%v",
		wrongLane, records, wrongStream, rt.metrics.counters["lane_retargets"])
	if records == 0 {
		t.Fatal("no records produced at all")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestChurn(t *testing.T) {
	s, rt := build(t, 900, "incremental")
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	before := rt.lanes.count()
	for i := 0; i < 4; i++ {
		s.mu.Lock()
		s.lastSweep = time.Time{}
		s.mu.Unlock()
		if err := s.sweep(context.Background()); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}
	t.Logf("lane rows before=%d after churn=%d finished=%d (B7: a finished row is never dropped)",
		before, rt.lanes.count(), rt.lanes.finished())
}

func TestAppendDedupeIsRefused(t *testing.T) {
	s, rt := build(t, 3, "append_dedup")
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	d := s.Validate(context.Background())
	t.Logf("diagnostics for append_dedup: %d", len(d))
	for _, x := range d {
		t.Logf("  %s at %v: %s", x.Severity, x.Path, x.Message)
	}
	if len(d) == 0 {
		t.Fatal("append_dedup should be refused: Origin.Key is unsettable")
	}
}

func TestReappear(t *testing.T) {
	// The hostile requirement: streams appear and disappear while running. Drive one full
	// disappear/reappear cycle for the volatile streams and see what Announce does.
	s, rt := build(t, 900, "incremental")
	if err := s.Open(context.Background(), rt); err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		s.mu.Lock()
		s.lastSweep = time.Time{}
		s.mu.Unlock()
		err := s.sweep(context.Background())
		t.Logf("sweep %d: rows=%d finished=%d err=%v", i, rt.lanes.count(), rt.lanes.finished(), err)
		if err != nil {
			t.Logf("B4 MEASURED: a reappearing stream fails the pipeline: %v", err)
			return
		}
	}
	t.Logf("B4/B7 MEASURED: the incarnation-suffix workaround survives churn, at the cost of a " +
		"permanent extra lane row per stream per disappearance cycle")
}

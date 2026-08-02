package engine_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// THESE ASSERT THAT RECORDS ARRIVE, not that ReadLanes was called.
//
// The distinction is the whole point of the change under test. connector.LaneReader, its
// SourceCaps.ReadsLanes declaration and registry.ResolvedSource.ReadConcurrency all existed and were
// resolved and reported by the negotiation; a test asserting "the engine called ReadLanes" would
// have passed against a build that called it and dropped every batch but the first. What was
// actually wrong was that a source announcing thirty lanes had twenty-nine read by nobody, so what
// these count is records out of the sink, per lane.
//
// laneSource.Read makes the negative case loud rather than subtle: it is a permanent internal fault,
// so a build that falls back to the one-lane path fails the run instead of quietly delivering a
// thirtieth of the data. That is a real source's correct body too — the contract says so.

// laneSource announces lanes and produces on all of them through connector.LaneReader.
type laneSource struct {
	lanes   int
	perLane int

	// spin makes ReadLanes return nil having reported nothing, which the source contract calls a
	// spin and requires the core to fault on.
	spin bool

	mu     sync.Mutex
	sent   map[record.LaneID]int
	done   map[record.LaneID]bool
	acks   []connector.Ack
	calls  []string // one sorted lane-tag list per ReadLanes call
	inUse  map[record.LaneID]bool
	live   int
	peak   int
	shared bool // two live calls carried the same lane
}

func newLaneSource(lanes, perLane int) *laneSource {
	return &laneSource{
		lanes: lanes, perLane: perLane,
		sent: map[record.LaneID]int{}, done: map[record.LaneID]bool{},
		inUse: map[record.LaneID]bool{},
	}
}

func (s *laneSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) > 0 {
		return nil
	}
	specs := make([]connector.LaneSpec, 0, s.lanes)
	for i := range s.lanes {
		specs = append(specs, connector.LaneSpec{
			Name: fmt.Sprintf("chunk%02d", i), Stream: "lines",
			Kind: connector.LaneKindScan, Ordering: connector.OrderingPrefix,
			Boundedness: connector.Bounded,
		})
	}
	_, err = rt.Lanes().AnnounceMany(ctx, specs)
	return err
}

func (s *laneSource) Close(context.Context) error { return nil }

// Read is the body the LaneReader contract prescribes for a source that declares ReadsLanes: the
// core never calls it, and saying so out loud is what turns a silent fallback into a failed run.
func (s *laneSource) Read(context.Context, *record.Batch) error {
	return fault.Internal(fault.OpRead,
		fmt.Errorf("this source declares ReadsLanes; the core must call ReadLanes"))
}

func (s *laneSource) ReadLanes(ctx context.Context, dst []*record.Batch) error {
	s.enter(dst)
	defer s.leave(dst)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
	}
	if s.spin {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range dst {
		if s.done[b.Lane] {
			continue
		}
		n := s.sent[b.Lane]
		if n < s.perLane {
			s.sent[b.Lane] = n + 1
			if r := b.Add(); r != nil {
				// The payload names its own lane, so the sink's lines are the per-lane arrival
				// count without the test having to reach into the engine for it.
				r.Payload = record.BytesPayload([]byte(fmt.Sprintf("%s-%03d", laneTag(b.Lane), n)))
			}
			n++
		}
		if n >= s.perLane {
			// The lane is retired on its OWN batch. A source holding thirty lanes has no way to say
			// "this one is finished" other than this field, and until the engine read it the answer
			// was to finish all thirty with ErrEndOfInput or none of them.
			s.done[b.Lane] = true
			b.EndOfLane = true
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(n))
		b.Position = record.Position{
			Token: record.Blob{Version: 1, Bytes: buf[:]}, Order: buf[:],
			Safe: true, At: time.Now(), Label: fmt.Sprintf("row %d", n),
		}
	}
	return nil
}

// enter and leave record what one live ReadLanes call carries, which is how the disjointness and
// concurrency claims in the contract get asserted rather than assumed.
func (s *laneSource) enter(dst []*record.Batch) {
	tags := make([]string, 0, len(dst))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range dst {
		if s.inUse[b.Lane] {
			s.shared = true
		}
		s.inUse[b.Lane] = true
		tags = append(tags, laneTag(b.Lane))
	}
	sort.Strings(tags)
	s.calls = append(s.calls, strings.Join(tags, ","))
	s.live++
	if s.live > s.peak {
		s.peak = s.live
	}
}

func (s *laneSource) leave(dst []*record.Batch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range dst {
		delete(s.inUse, b.Lane)
	}
	s.live--
}

func (s *laneSource) Commit(_ context.Context, a connector.Ack) error {
	s.mu.Lock()
	s.acks = append(s.acks, a)
	s.mu.Unlock()
	return nil
}

func (s *laneSource) finishedLanes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, a := range s.acks {
		if a.LaneFinished {
			n++
		}
	}
	return n
}

func (s *laneSource) observed() (calls []string, peak int, shared bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...), s.peak, s.shared
}

func registerLaneSource(t *testing.T, s *laneSource, concurrency int) string {
	t.Helper()
	name := fmt.Sprintf("lane_reader_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*laneSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Lane-reading source",
			Summary: "Announces several bounded lanes and produces on all of them at once.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          s.lanes,
			ReadsLanes:        true,
			ReadConcurrency:   concurrency,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*laneSource, error) { return s, nil },
	})
	return name
}

// runLanes runs one bounded pipeline over src to completion and returns what the sink received.
func runLanes(t *testing.T, src *laneSource, concurrency, parallelism int) ([]string, error) {
	t.Helper()
	dir := t.TempDir()

	state, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { state.Close() })

	out := &collector{}
	s := pipelineSpec(registerCollector(t, "lane_sink", out), filepath.Join(dir, "unused.txt"))
	s.Graph[0].Name, s.Graph[0].Config = registerLaneSource(t, src, concurrency), map[string]any{}
	s.Parallelism = parallelism

	p, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{
		State: state, Worker: "w1",
		FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { p.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Run FIRST and snapshot after. `return out.got(), p.Run(ctx)` evaluates left to right, so it
	// reports what the sink held before the pipeline started — an empty slice, every time.
	runErr := p.Run(ctx)
	return out.got(), runErr
}

// perLane counts arrivals by the lane tag each payload names itself with.
func perLane(lines []string) map[string]int {
	out := map[string]int{}
	for _, ln := range lines {
		if i := strings.LastIndex(ln, "-"); i > 0 {
			out[ln[:i]]++
		}
	}
	return out
}

// EVERY LANE IS READ. This is the defect the change exists for: a worker holding eight lanes read
// one of them and the other seven produced nothing, forever, with no error and no warning.
func TestASourceThatReadsLanesHasEveryLaneRead(t *testing.T) {
	src := newLaneSource(8, 5)
	lines, err := runLanes(t, src, 1, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := perLane(lines)
	if len(got) != 8 {
		t.Fatalf("records arrived for %d of 8 lanes: %v", len(got), got)
	}
	for i := range 8 {
		tag := fmt.Sprintf("chunk%02d", i)
		if got[tag] != 5 {
			t.Errorf("lane %s delivered %d records, want 5 — %v", tag, got[tag], got)
		}
	}
}

// ONE CALL CARRIES EVERY LANE at ReadConcurrency 1, which is the multiplexed shape: a source with
// one upstream connection deciding which lane the next record belongs to cannot be asked to block
// in eight goroutines at once.
func TestOneReadConcurrencyReadsEveryLaneInOneCall(t *testing.T) {
	src := newLaneSource(6, 3)
	if _, err := runLanes(t, src, 1, 0); err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls, peak, shared := src.observed()
	if peak != 1 {
		t.Errorf("%d ReadLanes calls ran at once; the source declared ReadConcurrency 1", peak)
	}
	if shared {
		t.Error("two live ReadLanes calls carried the same lane")
	}
	if len(calls) == 0 {
		t.Fatal("ReadLanes was never called")
	}
	// The first call is the only one guaranteed to carry every lane: lanes retire as they finish.
	if want := "chunk00,chunk01,chunk02,chunk03,chunk04,chunk05"; calls[0] != want {
		t.Errorf("the first call carried %q, want every lane %q", calls[0], want)
	}
}

// CONCURRENT CALLS NEVER SHARE A LANE, which is the property that lets a source declaring
// ReadConcurrency > 1 do no locking between calls: the batches themselves never overlap.
func TestConcurrentLaneReadsNeverShareALane(t *testing.T) {
	src := newLaneSource(8, 5)
	lines, err := runLanes(t, src, 4, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, peak, shared := src.observed()
	if shared {
		t.Error("two live ReadLanes calls carried the same lane; disjointness is what makes " +
			"a source declaring ReadConcurrency > 1 safe without locks")
	}
	if peak > 4 {
		t.Errorf("%d ReadLanes calls ran at once; the source declared ReadConcurrency 4", peak)
	}
	if peak < 2 {
		t.Errorf("ReadLanes never ran concurrently (peak %d) against a declared concurrency of 4, "+
			"so this proves nothing about disjointness", peak)
	}
	if got := perLane(lines); len(got) != 8 {
		t.Errorf("records arrived for %d of 8 lanes: %v", len(got), got)
	}
}

// THE OPERATOR'S NUMBER WINS DOWNWARD. spec.Parallelism is "the maximum number of lanes one worker
// reads concurrently", and it was validated against MaxLanes at build time and then never read
// again by anything.
func TestParallelismCapsWhatTheSourceDeclared(t *testing.T) {
	src := newLaneSource(8, 5)
	if _, err := runLanes(t, src, 8, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, peak, _ := src.observed(); peak > 2 {
		t.Errorf("%d ReadLanes calls ran at once against parallelism 2, though the source "+
			"declared ReadConcurrency 8", peak)
	}
}

// A LANE RETIRES ON ITS OWN BATCH and the rest keep reading.
//
// record.Batch.EndOfLane was set by connectors — linefile has always set it — and read by nothing in
// the engine. On the one-lane path that was survivable by accident, because linefile returns
// ErrEndOfInput on the following call and the lane retired one round-trip late. For a source holding
// eight lanes there is no following call to be saved by: the field is the only way to say that one
// of them is done, and every one of these eight ends this way.
func TestALaneRetiresOnItsOwnBatch(t *testing.T) {
	src := newLaneSource(8, 5)
	if _, err := runLanes(t, src, 1, 0); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := src.finishedLanes(); got != 8 {
		t.Errorf("%d lanes were acknowledged as finished, want 8; a source that retires a lane "+
			"learns the retirement became durable from LaneFinished and from nothing else", got)
	}
}

// lastGaspSource is a PLAIN source — no LaneReader — that hands over records and its terminal error
// in the same call, which is what "cancellation means drain" asks every source to do.
type lastGaspSource struct {
	records int
	spin    bool

	mu   sync.Mutex
	done bool
}

func (s *lastGaspSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	_, err = rt.Lanes().AnnounceMany(ctx, []connector.LaneSpec{{
		Name: "only", Stream: "lines", Kind: connector.LaneKindScan,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Bounded,
	}})
	return err
}

func (s *lastGaspSource) Close(context.Context) error                 { return nil }
func (s *lastGaspSource) Commit(context.Context, connector.Ack) error { return nil }

func (s *lastGaspSource) Read(_ context.Context, dst *record.Batch) error {
	if s.spin {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return fault.ErrEndOfInput
	}
	s.done = true
	for i := range s.records {
		if r := dst.Add(); r != nil {
			r.Payload = record.BytesPayload([]byte(fmt.Sprintf("gasp-%03d", i)))
		}
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(s.records))
	dst.Position = record.Position{
		Token: record.Blob{Version: 1, Bytes: buf[:]}, Order: buf[:], Safe: true, At: time.Now(),
	}
	// The records AND the terminal error, from one call. A source is told never to discard what it
	// has already produced on an error path, on the promise that the engine admits the batch first.
	return fault.ErrEndOfInput
}

func runPlain(t *testing.T, src *lastGaspSource) ([]string, error) {
	t.Helper()
	dir := t.TempDir()

	state, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { state.Close() })

	name := fmt.Sprintf("last_gasp_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*lastGaspSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Last-gasp source",
			Summary: "Returns its final records and its terminal error from the same call.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          1,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*lastGaspSource, error) { return src, nil },
	})

	out := &collector{}
	s := pipelineSpec(registerCollector(t, "gasp_sink", out), filepath.Join(dir, "unused.txt"))
	s.Graph[0].Name, s.Graph[0].Config = name, map[string]any{}

	p, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{
		State: state, Worker: "w1", FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { p.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runErr := p.Run(ctx)
	return out.got(), runErr
}

// RECORDS HANDED OVER WITH THE TERMINAL ERROR ARE NOT LOST.
//
// Source.Read tells a source it "must never discard records it has already produced into dst on an
// error path: the engine admits what is in the batch BEFORE handling the error". The engine did not:
// every error branch returned without looking at the batch, so a source that did exactly what it was
// told lost its last window — silently, with the run reported as a clean completion.
func TestRecordsHandedOverWithTheFinalErrorAreNotLost(t *testing.T) {
	lines, err := runPlain(t, &lastGaspSource{records: 7})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(lines) != 7 {
		t.Errorf("%d of 7 records survived a source that returned them alongside ErrEndOfInput: %v",
			len(lines), lines)
	}
}

// The spin rule holds on the ONE-LANE path too, which is a separate call site and was separately
// unimplemented.
func TestAPlainSourceReportingNothingTwiceIsAContractFault(t *testing.T) {
	_, err := runPlain(t, &lastGaspSource{spin: true})
	if err == nil {
		t.Fatal("a source that reported nothing on every call ran to completion")
	}
	if got := fault.ClassOf(err); got != fault.PermanentContract {
		t.Errorf("the run failed as %s, want permanent_contract: %v", got, err)
	}
}

// scriptedSource emits one record per Read until it has emitted n, retires its lane on the last
// one, and then blocks — so a test can hold every record in flight and drive settlement itself.
type scriptedSource struct {
	records int

	mu   sync.Mutex
	sent int
	acks []connector.Ack
}

func (s *scriptedSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	_, err = rt.Lanes().AnnounceMany(ctx, []connector.LaneSpec{{
		Name: "only", Stream: "lines", Kind: connector.LaneKindScan,
		Ordering: connector.OrderingPrefix, Boundedness: connector.Bounded,
	}})
	return err
}

func (s *scriptedSource) Close(context.Context) error { return nil }

func (s *scriptedSource) Commit(_ context.Context, a connector.Ack) error {
	s.mu.Lock()
	s.acks = append(s.acks, a)
	s.mu.Unlock()
	return nil
}

func (s *scriptedSource) Read(ctx context.Context, dst *record.Batch) error {
	s.mu.Lock()
	if s.sent >= s.records {
		s.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	s.sent++
	n := s.sent
	s.mu.Unlock()

	if r := dst.Add(); r != nil {
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%03d", n)))
	}
	// ONE BATCH PER RECORD, so each takes its own settlement group and the lane can be partly
	// settled — which is the state this fixture exists to reach.
	dst.EndOfLane = n == s.records
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	dst.Position = record.Position{
		Token: record.Blob{Version: 1, Bytes: buf[:]}, Order: buf[:], Safe: true, At: time.Now(),
	}
	return nil
}

func (s *scriptedSource) finishedAcks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, a := range s.acks {
		if a.LaneFinished {
			n++
		}
	}
	return n
}

// A LANE IS NOT ACKNOWLEDGED AS FINISHED WHILE IT STILL HAS RECORDS IN FLIGHT.
//
// Ack.LaneFinished says it is "true on the FINAL ack for a finished lane. After it, Commit is not
// called for the lane again", and a prefix lane's ack reports the RESOLVED prefix — so an ungated
// flag rides an ack for position one while two through five are still in a sink that has not made
// them durable. The source is then told the lane is done, four records early, and never told about
// the rest.
//
// It needs a sink whose durability boundary falls mid-lane, which is why this drives the flusher's
// durableUpTo by hand rather than waiting for a timing accident.
func TestALaneWithRecordsStillInFlightIsNotAcknowledgedAsFinished(t *testing.T) {
	dir := t.TempDir()
	state, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { state.Close() })

	src := &scriptedSource{records: 5}
	name := fmt.Sprintf("scripted_%d", sinkSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*scriptedSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Scripted source",
			Summary: "Emits a fixed number of one-record batches and retires its lane on the last.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          1,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*scriptedSource, error) { return src, nil },
	})

	// Nothing is durable until the test says so, so all five records pile up in flight and the
	// EndOfLane batch is admitted while every one of them is unsettled.
	sink := &flusher{deferAll: true}
	s := pipelineSpec(registerFlusher(t, "gate_sink", sink), filepath.Join(dir, "unused.txt"))
	s.Graph[0].Name, s.Graph[0].Config = name, map[string]any{}

	p, _, diags := engine.Build(context.Background(), registry.Default, s, engine.Deps{
		State: state, Worker: "w1", FlushInterval: 5 * time.Millisecond, GracePeriod: 2 * time.Second,
	})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { p.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Every record accepted by the sink, none durable.
	waitFor(t, "all five records to reach the sink", func() bool {
		accepted, _, _ := sink.snapshot()
		return len(accepted) == 5
	})
	if got := src.finishedAcks(); got != 0 {
		t.Fatalf("%d finished-acknowledgements arrived while nothing is durable; this test "+
			"isolates nothing", got)
	}

	// One record becomes durable. The lane's prefix advances to it and an acknowledgement goes
	// out — carrying four records that are still in the sink.
	sink.set(func(f *flusher) { f.deferAll, f.durableUpTo = false, 1 })
	waitFor(t, "the first record to become durable", func() bool {
		_, durable, _ := sink.snapshot()
		return len(durable) >= 1
	})
	// Given a moment for the acknowledgement that advance produces to be delivered.
	waitFor(t, "the source to be acknowledged for the settled prefix", func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return len(src.acks) > 0
	})
	if got := src.finishedAcks(); got != 0 {
		_, durable, _ := sink.snapshot()
		t.Fatalf("the lane was acknowledged as finished with %d of 5 records durable; after that "+
			"acknowledgement the source is entitled to stop expecting any more", 5-len(durable))
	}

	// The rest settles. Now — and only now — the lane is finished, exactly once.
	sink.set(func(f *flusher) { f.durableUpTo = 0 })
	waitFor(t, "the lane's final acknowledgement", func() bool { return src.finishedAcks() > 0 })
	if got := src.finishedAcks(); got != 1 {
		t.Errorf("%d acknowledgements claimed the lane had finished, want exactly 1", got)
	}
}

// waitFor polls until cond holds, which keeps every gate in these tests on STATE rather than on a
// sleep long enough to usually work.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A SOURCE THAT REPORTS NOTHING TWICE RUNNING IS A CONTRACT FAULT.
//
// Both Source.Read and LaneReader.ReadLanes say the core raises fault.PermanentContract on the
// second consecutive empty unpositioned return, and until this change nothing did — in either
// shape. The symptom of not doing it is a pipeline pinning a core and looking, from every metric
// outside the source, exactly like a busy one.
func TestASourceReportingNothingTwiceIsAContractFault(t *testing.T) {
	src := newLaneSource(2, 1)
	src.spin = true

	_, err := runLanes(t, src, 1, 0)
	if err == nil {
		t.Fatal("a source that reported nothing on every call ran to completion")
	}
	if got := fault.ClassOf(err); got != fault.PermanentContract {
		t.Errorf("the run failed as %s, want permanent_contract: %v", got, err)
	}
}

package engine_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE CONTROL GOROUTINE, ASSERTED. build.go has described a source node as running exactly two
// goroutines — the read goroutine and the control goroutine for Commit, Heartbeat, Backlog and Nack —
// since it was written, and only the read goroutine existed. Seven connectors in this module
// implement Heartbeat, five implement Backlog, three implement Nack, and nothing ever called any of
// them.
//
// Every test here uses a source that BLOCKS IN Read after its first batch, because that is the
// premise the whole design rests on: a tail source with nothing to say is parked inside a blocking
// call, so anything that must still happen has to happen on another goroutine.

// controlSource announces one lane, emits one batch, then blocks in Read.
type controlSource struct {
	mu sync.Mutex

	emit     int
	produced bool
	release  chan struct{}

	// ordering decides whether a nack carries a Handle or a Position.
	ordering connector.Ordering

	// backlog is the answer Backlog returns; zeroAsOf omits AsOf so the host has to stamp it.
	backlog  uint64
	zeroAsOf bool

	// keepProducing makes Read return a batch every time instead of parking, so a lane that is
	// genuinely busy can be told apart from one that is quiet. beatErr fails every heartbeat.
	keepProducing bool
	beatErr       error

	// seq makes each batch's position strictly greater than the last, which the ledger requires.
	seq uint64

	// what the control goroutine did.
	beats []time.Duration
	polls int
	nacks []connector.Nack
}

func (s *controlSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil {
		return err
	}
	if len(as) > 0 {
		return nil
	}
	_, err = rt.Lanes().Announce(ctx, connector.LaneSpec{
		Name: "tail", Stream: "lines", Kind: connector.LaneKindStream,
		Ordering: s.ordering, Boundedness: connector.Unbounded, Group: "tail",
	})
	return err
}

func (s *controlSource) Read(ctx context.Context, dst *record.Batch) error {
	s.mu.Lock()
	done := s.produced && !s.keepProducing
	s.produced = true
	n := s.emit
	s.seq++
	seq := s.seq
	busy := s.keepProducing
	s.mu.Unlock()

	if busy {
		// A lane that never stops producing. Slowed slightly so the test is a steady stream rather
		// than a hot loop, and still far busier than the control interval.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}

	if done {
		// PARKED IN A BLOCKING CALL, which is the state a tail source spends its life in. Nothing on
		// this goroutine can heartbeat, poll a backlog or deliver a nack while it is here.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
			return fault.ErrEndOfInput
		}
	}

	dst.Reset()
	for i := 0; i < n; i++ {
		r := dst.Add()
		if r == nil {
			break
		}
		r.Payload = record.BytesPayload([]byte(fmt.Sprintf("row-%d", i)))
		r.SetHandle([]byte(fmt.Sprintf("receipt-%d", i)))
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	dst.Position = record.Position{
		Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: time.Now(), Label: fmt.Sprintf("batch %d", seq),
	}
	return nil
}

func (s *controlSource) Commit(context.Context, connector.Ack) error { return nil }
func (s *controlSource) Close(context.Context) error                 { return nil }

func (s *controlSource) Heartbeat(_ context.Context, _ record.LaneID, idle time.Duration) error {
	s.mu.Lock()
	s.beats = append(s.beats, idle)
	err := s.beatErr
	s.mu.Unlock()
	return err
}

func (s *controlSource) Backlog(_ context.Context, _ record.LaneID) (connector.Backlog, error) {
	s.mu.Lock()
	s.polls++
	n := s.backlog
	zero := s.zeroAsOf
	s.mu.Unlock()
	b := connector.Backlog{Records: connector.Count(n), Exact: true}
	if !zero {
		b.AsOf = time.Now()
	}
	return b, nil
}

func (s *controlSource) Nack(_ context.Context, _ record.LaneID, ns []connector.Nack) error {
	s.mu.Lock()
	s.nacks = append(s.nacks, ns...)
	s.mu.Unlock()
	return nil
}

func (s *controlSource) seen() (beats []time.Duration, polls int, nacks []connector.Nack) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.beats...), s.polls, append([]connector.Nack(nil), s.nacks...)
}

var controlSeq = &sinkSeq

func registerControlSource(t *testing.T, s *controlSource) string {
	t.Helper()
	name := fmt.Sprintf("control_source_%d", controlSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*controlSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Control source",
			Summary: "Declares Heartbeater, BacklogReporter and Nackable.",
			Notes:   "Origin.Key is the row index, stable across re-reads.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:            connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering: s.ordering,
			Boundedness:     []connector.Boundedness{connector.Unbounded},
			LaneKinds:       []connector.LaneKind{connector.LaneKindStream},
			MaxLanes:        1,
			StableKeys:      true,
			Replayable:      true,
			// THE DECLARATIONS. The registry leaves each handle nil unless the capability is declared
			// here, so these three booleans are what the engine's control loop keys off — not a type
			// assertion behind the operator's back.
			Heartbeats:        true,
			ReportsBacklog:    true,
			Nackable:          true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*controlSource, error) { return s, nil },
	})
	return name
}

func controlSpec(srcName, sinkName string) spec.Spec {
	return controlSpecWithTerminal(srcName, sinkName, fault.TerminalStop)
}

// controlSpecWithTerminal names the terminal disposition, because it is what decides whether an
// abandoned record exists at all: DefaultRetry.Terminal is TerminalStop, which settles NOTHING and
// stops the pipeline, so a nack test under the default policy has no abandoned record to nack.
func controlSpecWithTerminal(srcName, sinkName string, term fault.Terminal) spec.Spec {
	retry := fault.DefaultRetry
	retry.Terminal = term
	return spec.Spec{
		Tenant: "acme", ID: "ctl",
		Retry: retry,
		Graph: []spec.Node{
			{ID: "in", Kind: registry.KindSource, Name: srcName},
			{ID: "out", Kind: registry.KindSink, Name: sinkName,
				Config: map[string]any{"codec": map[string]any{"encoder": "raw", "framer": "newline"}},
				Inputs: []spec.Edge{{From: "in"}}},
		},
		Streams: []spec.StreamConfig{{
			Stream: "lines",
			Read:   []connector.LaneKind{connector.LaneKindStream},
			Write:  connector.DestAppend,
		}},
	}
}

// A QUIET LANE IS HEARTBEATED, and the lane reports itself idle only because the source accepted it.
//
// This is the failure pkg/connector names outright: with no messages to acknowledge, a logical
// replication slot is never reclaimed and the disk fills. negotiate.go already refuses the
// un-survivable form of it at submit time — and a source that declared Heartbeater passed that gate
// and then pinned its upstream anyway, because nothing heartbeated.
func TestAQuietLaneIsHeartbeatedAndReportsItselfIdle(t *testing.T) {
	src := &controlSource{emit: 4, release: make(chan struct{}), ordering: connector.OrderingPrefix}
	srcName := registerControlSource(t, src)
	sinkName := registerProbe(t, "control_sink", &probeSink{})

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, controlSpec(srcName, sinkName),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 25 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	var s telemetry.PipelineStatus
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if beats, _, _ := src.seen(); len(beats) >= 2 {
			s = p.Status()
			if len(s.Lanes) == 1 && s.Lanes[0].Idle {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	beats, _, _ := src.seen()
	if len(beats) < 2 {
		t.Fatalf("the source received %d heartbeats while parked in a blocking Read; the control "+
			"goroutine is not running", len(beats))
	}
	// The heartbeat carries the REAL elapsed idle time, not the tick interval — a source deciding
	// whether to advance its slot needs to know how long it has actually been quiet.
	for i, d := range beats {
		if d <= 0 {
			t.Errorf("heartbeat %d carried idle=%v; it must be the measured elapsed quiet time", i, d)
		}
	}

	if len(s.Lanes) != 1 {
		t.Fatalf("%d lanes in the document, want 1", len(s.Lanes))
	}
	if !s.Lanes[0].Idle {
		t.Error("the lane is not reported idle although its source accepted a heartbeat")
	}
	if s.Lanes[0].IdleSince == nil {
		t.Error("Idle is set with no IdleSince; an operator cannot tell a minute of quiet from a week")
	}
	// CheckpointAge is still reported truthfully. Idle is what tells an alert rule to ignore it, and
	// suppressing the age instead would break the one always-available stall signal.
	if s.Lanes[0].CheckpointAge == nil {
		t.Error("an idle lane stopped reporting its checkpoint age; Idle suppresses the ALERT, not the fact")
	}

	// AND THE CONDITION IS NOT A STALL. This is the false alarm Heartbeater exists to kill: hundreds
	// of healthy quiet streams each reporting a forever-rising checkpoint age for the sole offence of
	// having nothing to say.
	prog := conditionOf(t, s, telemetry.CondProgressing)
	if prog.Reason == telemetry.ReasonStalled {
		t.Errorf("a lane its own source reported idle is being called stalled: %s", prog.Message)
	}

	close(src.release)
	cancel()
	<-done
}

// THE NEGATIVE HALF, and it needs a source that CAN heartbeat. A lane that is producing must not be
// heartbeated, or "heartbeat everything always" satisfies the test above and Idle means nothing.
func TestALaneThatIsProducingIsNotHeartbeated(t *testing.T) {
	src := &controlSource{emit: 2, release: make(chan struct{}),
		ordering: connector.OrderingPrefix, keepProducing: true}
	srcName := registerControlSource(t, src)
	sinkName := registerProbe(t, "control_busy_sink", &probeSink{})

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	const iv = 30 * time.Millisecond
	p, _, diags := engine.Build(context.Background(), registry.Default, controlSpec(srcName, sinkName),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: iv, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Six control intervals of continuous production. Every one of them is an opportunity to
	// heartbeat a lane that is plainly not quiet.
	time.Sleep(6 * iv)
	beats, _, _ := src.seen()
	s := p.Status()
	cancel()
	<-done

	if len(beats) != 0 {
		t.Errorf("a lane producing a batch every millisecond was heartbeated %d times over 6 control "+
			"intervals; the idle flag would then mean nothing at all", len(beats))
	}
	for _, l := range s.Lanes {
		if l.Idle {
			t.Errorf("lane %s is reported idle while it is producing continuously", l.ID)
		}
	}
	if len(s.Lanes) == 0 || s.Lanes[0].RecordsRead == 0 {
		t.Fatal("the source produced nothing, so this test proves nothing")
	}
}

// IDLE MEANS THE SOURCE WAS TOLD AND AGREED. A heartbeat that failed leaves the lane reporting a
// climbing checkpoint age, which is the honest reading: nobody has confirmed the lane is quiet rather
// than stuck, and the false-alarm suppression must not be reachable by a call that errored.
func TestAFailedHeartbeatDoesNotMarkTheLaneIdle(t *testing.T) {
	src := &controlSource{emit: 2, release: make(chan struct{}), ordering: connector.OrderingPrefix,
		beatErr: fault.Transient(fault.OpRead, errBeat)}
	srcName := registerControlSource(t, src)
	sinkName := registerProbe(t, "control_beatfail_sink", &probeSink{})

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, controlSpec(srcName, sinkName),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 20 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if beats, _, _ := src.seen(); len(beats) >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	s := p.Status()
	beats, _, _ := src.seen()
	close(src.release)
	cancel()
	runErr := <-done

	if len(beats) < 3 {
		t.Fatalf("only %d heartbeats were attempted; a refused heartbeat must be retried on the next "+
			"tick rather than giving up", len(beats))
	}
	if len(s.Lanes) != 1 {
		t.Fatalf("%d lanes, want 1", len(s.Lanes))
	}
	if s.Lanes[0].Idle {
		t.Error("the lane is reported idle although every heartbeat was refused; nobody has confirmed " +
			"it is quiet rather than stuck")
	}
	// A FAILING AUXILIARY CALL MUST NOT STOP A PIPELINE THAT IS OTHERWISE FINE. Asserted on the run's
	// error rather than on the phase, because the phase is only terminal once Run has returned and the
	// document above was read while it was still going.
	if errors.Is(runErr, errBeat) {
		t.Errorf("a refused heartbeat ended the run: %v\n"+
			"it delays an upstream's retention release, which is a real cost and a visible one; "+
			"stopping a healthy pipeline over a transient auxiliary call is a larger one", runErr)
	}
}

var errBeat = errors.New("the replication slot is unreachable")

// A source that declares no capability at all is called about nothing, and can never report a lane
// idle — which is the honest answer rather than a convenient one.
func TestASourceThatDeclaresNothingIsNeverAskedAnything(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 40000)

	// linefile declares none of the three, so the control loop must not even start for it — and that
	// is itself the assertion: a source that declared nothing is called about nothing.
	c := &collector{}
	sinkName := registerCollector(t, "control_none", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(sinkName, path),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 10 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// A source with no Heartbeater can never report a lane idle, which is the honest answer: nobody
	// has confirmed the lane is quiet rather than stuck.
	s := p.Status()
	for _, l := range s.Lanes {
		if l.Idle {
			t.Errorf("lane %s is reported idle although its source cannot heartbeat", l.ID)
		}
	}
}

// A BACKLOG REACHES THE READ MODEL. LaneStatus.Backlog was nil for every pipeline because the engine
// never asked, and "how far behind am I" is the question a source implements BacklogReporter to be
// able to answer.
func TestABacklogReachesTheDocumentWithAnAsOf(t *testing.T) {
	// zeroAsOf: the source omits AsOf, so the host must stamp the moment it asked. A polled backlog
	// with no AsOf implies a liveness it does not have.
	src := &controlSource{emit: 2, release: make(chan struct{}),
		ordering: connector.OrderingPrefix, backlog: 4096, zeroAsOf: true}
	srcName := registerControlSource(t, src)
	sinkName := registerProbe(t, "control_backlog_sink", &probeSink{})

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, controlSpec(srcName, sinkName),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 20 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	var got *telemetry.Backlog
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s := p.Status()
		if len(s.Lanes) == 1 && s.Lanes[0].Backlog != nil {
			got = s.Lanes[0].Backlog
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got == nil {
		_, polls, _ := src.seen()
		t.Fatalf("no backlog in the document after %d polls", polls)
	}
	if got.Records == nil || *got.Records != 4096 {
		t.Errorf("backlog records is %v, want 4096", got.Records)
	}
	if !got.Exact {
		t.Error("an exact count was reported as an estimate; the two must not render identically")
	}
	if got.AsOf.IsZero() {
		t.Error("the backlog has no AsOf, so a stale number is indistinguishable from a fresh one")
	}
	cancel()
	<-done
}

// A TERMINAL DISPOSITION REACHES THE SOURCE. This is the one thing a source can never infer: the
// cursor advances past an abandoned record exactly as if it had been delivered, so without a nack a
// parked-message source parks nothing and an upstream that tracks failures records a success.
func TestAnAbandonedRecordIsNackedToItsSource(t *testing.T) {
	src := &controlSource{emit: 3, release: make(chan struct{}), ordering: connector.OrderingDiscrete}
	srcName := registerControlSource(t, src)

	// A sink that permanently refuses one record. PermanentMapping is terminal, so the routing drops
	// it rather than retrying forever, and dropping is what has to be reported upstream.
	poison := &flaky{answer: func(_ int, recs []record.Ref) (connector.WriteResult, error) {
		var res connector.WriteResult
		for _, rf := range recs {
			res.Failed = append(res.Failed, fault.RecordFault{
				Record: rf.ID, Class: fault.PermanentMapping, Op: fault.OpWrite,
				User: "the destination cannot represent this row",
			})
			break
		}
		return res, nil
	}}
	sinkName := registerFlaky(t, "control_poison_sink", poison, false)

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default,
		controlSpecWithTerminal(srcName, sinkName, fault.TerminalDrop),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 20 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.Now().Add(8 * time.Second)
	var nacks []connector.Nack
	for time.Now().Before(deadline) {
		if _, _, ns := src.seen(); len(ns) > 0 {
			nacks = ns
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(src.release)
	cancel()
	<-done

	if len(nacks) == 0 {
		t.Fatal("a record was abandoned and the source was never told; its cursor advanced past a " +
			"record that was never delivered")
	}
	n := nacks[0]
	if n.Class != fault.PermanentMapping {
		t.Errorf("nack class is %s, want permanent_mapping: the source needs to know whether retrying "+
			"could ever have helped", n.Class)
	}
	if n.Reason != telemetry.ReasonTerminalFault {
		t.Errorf("nack reason is %q, want %q", n.Reason, telemetry.ReasonTerminalFault)
	}
	// A DISCRETE LANE IS NACKED BY HANDLE, never by record id: an id is assigned by the core after
	// Read returned, so the source has never seen it and cannot map it to a delivery.
	if len(n.Handle) == 0 {
		t.Error("a discrete lane's nack carries no delivery handle, so the source cannot park the message")
	}
	if string(n.Handle) != "receipt-0" && string(n.Handle) != "receipt-1" && string(n.Handle) != "receipt-2" {
		t.Errorf("the handle is %q, which is not one this source issued", n.Handle)
	}
}

// A PREFIX LANE IS NACKED BY POSITION instead, because it has no per-delivery handle to name. The two
// fields are not interchangeable and the lane's ordering is what decides.
func TestAPrefixLaneIsNackedByPosition(t *testing.T) {
	src := &controlSource{emit: 3, release: make(chan struct{}), ordering: connector.OrderingPrefix}
	srcName := registerControlSource(t, src)

	poison := &flaky{answer: func(_ int, recs []record.Ref) (connector.WriteResult, error) {
		var res connector.WriteResult
		for _, rf := range recs {
			res.Failed = append(res.Failed, fault.RecordFault{
				Record: rf.ID, Class: fault.PermanentMapping, Op: fault.OpWrite, User: "unrepresentable",
			})
			break
		}
		return res, nil
	}}
	sinkName := registerFlaky(t, "control_prefix_poison", poison, false)

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default,
		controlSpecWithTerminal(srcName, sinkName, fault.TerminalDrop),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: 20 * time.Millisecond, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.Now().Add(8 * time.Second)
	var nacks []connector.Nack
	for time.Now().Before(deadline) {
		if _, _, ns := src.seen(); len(ns) > 0 {
			nacks = ns
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(src.release)
	cancel()
	<-done

	if len(nacks) == 0 {
		t.Fatal("a prefix lane's abandoned record was never nacked")
	}
	n := nacks[0]
	if n.Position.IsZero() {
		t.Error("a prefix lane's nack carries no position, so the source has nothing to act on")
	}
	if len(n.Handle) != 0 {
		t.Errorf("a prefix lane's nack carries a delivery handle %q; handles are for discrete lanes", n.Handle)
	}
}

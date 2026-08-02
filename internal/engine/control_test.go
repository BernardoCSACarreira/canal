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
			s = p.Status(telemetry.StatusQuery{})
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

	// THE INTERVAL IS LARGE BECAUSE THE ASSERTION IS ABOUT SCHEDULING AS MUCH AS ABOUT THE ENGINE.
	//
	// A heartbeat fires when a lane produced nothing for a whole control interval, so this test's
	// premise is that a source emitting every millisecond cannot be quiet for one. At 30ms that
	// premise is false on a loaded machine: it failed in CI on macOS with a single beat, and the
	// engine was RIGHT — the read goroutine had genuinely not been scheduled for 30ms, so the lane
	// genuinely was quiet. The margin is what makes the premise true, and half a second of a source
	// getting no CPU at all is a broken runner rather than a scheduling hiccup.
	//
	// It cannot be fixed by gating on state instead of the clock, which is the usual answer here: the
	// quantity being asserted IS an absence over an interval, and an absence has no state to wait
	// for. Widening the interval is the only lever.
	const iv = 500 * time.Millisecond
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

	// GATED ON THE FIRST RECORD, not on the clock. Sleeping from the moment Run is launched assumes
	// the pipeline is past openSinks, recoverCheckpoint, openSources and an Announce that fsyncs — the
	// same assumption that made TestACancelledRunFlushesWithDrain pass locally and fail in CI.
	//
	// The beat count is snapshotted at the gate rather than required to be zero, because a heartbeat
	// BEFORE the first record is correct: until a lane has produced anything, quietness is measured
	// from the start of the run and such a lane genuinely is quiet. What must not happen is a
	// heartbeat while records are flowing.
	before := -1
	gate := time.Now().Add(30 * time.Second)
	for time.Now().Before(gate) {
		if s := p.Status(telemetry.StatusQuery{}); len(s.Lanes) == 1 && s.Lanes[0].RecordsRead > 0 {
			beats, _, _ := src.seen()
			before = len(beats)
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if before < 0 {
		cancel()
		<-done
		t.Fatal("the source never produced a record, so this test proves nothing")
	}

	// Four control intervals of continuous production, SAMPLED rather than slept through.
	//
	// The samples are what make this test correct on a starved machine instead of merely unlikely to
	// hit one. A heartbeat means the lane produced nothing for a whole interval, and on a loaded
	// runner that can be TRUE — the read goroutine simply did not get scheduled, and the engine is
	// right to heartbeat a lane that genuinely went quiet. So the assertion cannot be "no heartbeats"
	// alone; it has to be "no heartbeats WHILE the lane was actually producing", and the only way to
	// know which happened is to watch the record count go up.
	//
	// A stall shows up here as a pair of consecutive samples far apart in time with no records
	// between them. The sampler is starved by the same stall, which is exactly why the gap is visible
	// to it at all.
	type sample struct {
		at    time.Time
		reads uint64
	}
	samples := []sample{{time.Now(), p.Status(telemetry.StatusQuery{}).Lanes[0].RecordsRead}}
	for deadline := time.Now().Add(4 * iv); time.Now().Before(deadline); {
		time.Sleep(iv / 10)
		d := p.Status(telemetry.StatusQuery{})
		if len(d.Lanes) == 1 {
			samples = append(samples, sample{time.Now(), d.Lanes[0].RecordsRead})
		}
	}
	beats, _, _ := src.seen()
	s := p.Status(telemetry.StatusQuery{})
	cancel()
	<-done

	var quietest time.Duration
	for i := 1; i < len(samples); i++ {
		if samples[i].reads == samples[i-1].reads {
			if gap := samples[i].at.Sub(samples[i-1].at); gap > quietest {
				quietest = gap
			}
		}
	}

	switch {
	case len(beats) == before:
		// Nothing to explain.
	case quietest >= iv:
		// THE ENGINE WAS RIGHT AND THE MACHINE WAS SLOW. Reported rather than ignored, so a run that
		// proved nothing does not read as a run that proved something.
		t.Logf("inconclusive: the lane produced nothing for %v, which is past the %v control "+
			"interval, so the %d heartbeat(s) were correct. The runner starved the source",
			quietest, iv, len(beats)-before)
	default:
		t.Errorf("a lane producing a batch every millisecond was heartbeated %d times over 4 control "+
			"intervals of %v, and its longest gap without a record was only %v.\n"+
			"  It kept producing throughout and was heartbeated anyway; the idle flag would then "+
			"mean nothing at all", len(beats)-before, iv, quietest)
	}
	for _, l := range s.Lanes {
		if l.Idle {
			t.Errorf("lane %s is reported idle while it is producing continuously", l.ID)
		}
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
	s := p.Status(telemetry.StatusQuery{})
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
	s := p.Status(telemetry.StatusQuery{})
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
		s := p.Status(telemetry.StatusQuery{})
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

// --- many lanes ---------------------------------------------------------------------------------

// fanSource announces several lanes across two streams, then parks. Only the first is read — this
// runtime reads one lane per source — which is enough: the point is a document describing a lane
// table larger than one page.
type fanSource struct {
	lanes   int
	release chan struct{}
	done    bool
}

func (s *fanSource) Open(ctx context.Context, rt connector.SourceRuntime) error {
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil || len(as) > 0 {
		return err
	}
	specs := make([]connector.LaneSpec, 0, s.lanes)
	for i := 0; i < s.lanes; i++ {
		stream := "orders"
		if i%2 == 1 {
			stream = "customers"
		}
		specs = append(specs, connector.LaneSpec{
			Name: fmt.Sprintf("chunk-%03d", i), Stream: record.StreamName(stream),
			Kind: connector.LaneKindScan, Ordering: connector.OrderingPrefix,
			Boundedness: connector.Bounded, Group: "scan", Weight: uint64(10 * (i + 1)),
			Label: fmt.Sprintf("chunk %d of %d", i+1, s.lanes),
		})
	}
	_, err = rt.Lanes().AnnounceMany(ctx, specs)
	return err
}

func (s *fanSource) Read(ctx context.Context, dst *record.Batch) error {
	if s.done {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
			return fault.ErrEndOfInput
		}
	}
	s.done = true
	dst.Reset()
	for i := 0; i < 3; i++ {
		if r := dst.Add(); r != nil {
			r.Payload = record.BytesPayload([]byte("row"))
		}
	}
	var b [8]byte
	b[7] = 3
	dst.Position = record.Position{Token: record.Blob{Version: 1, Bytes: b[:]}, Order: b[:],
		Safe: true, At: time.Now(), Label: "row 3"}
	return nil
}

func (s *fanSource) Commit(context.Context, connector.Ack) error { return nil }
func (s *fanSource) Close(context.Context) error                 { return nil }

func registerFanSource(t *testing.T, s *fanSource) string {
	t.Helper()
	name := fmt.Sprintf("fan_source_%d", controlSeq.Add(1))
	registry.AddSource(registry.Default, registry.SourceDef[*fanSource]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Fan source",
			Summary: "Announces many lanes across two streams.",
			Notes:   "Origin.Key is the chunk index, stable across re-reads.",
			Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SourceCaps{
			Caps:              connector.Caps{APIVersion: connector.APIVersion},
			DefaultOrdering:   connector.OrderingPrefix,
			Boundedness:       []connector.Boundedness{connector.Bounded},
			LaneKinds:         []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:          256,
			StableKeys:        true,
			Replayable:        true,
			UpstreamRetention: connector.RetentionUnbounded,
		},
		New: func(context.Context, *config.Config) (*fanSource, error) { return s, nil },
	})
	return name
}

func fanSpec(srcName, sinkName string) spec.Spec {
	s := controlSpec(srcName, sinkName)
	s.Streams = []spec.StreamConfig{
		{Stream: "orders", Read: []connector.LaneKind{connector.LaneKindScan}, Write: connector.DestAppend},
		{Stream: "customers", Read: []connector.LaneKind{connector.LaneKindScan}, Write: connector.DestAppend},
	}
	return s
}

// THE ROLLUP DESCRIBES EVERY LANE, THE PAGE DESCRIBES ONE PAGE, and conflating them would make the
// summary a statement about an arbitrary subset — which looks like an answer and is not one.
//
// This is the end-to-end half: a real pipeline with a real lane table, paged through the real
// projection. The arithmetic itself is asserted directly in scale_internal_test.go.
func TestTheRollupCoversEveryLaneEvenWhenThePageDoesNot(t *testing.T) {
	const lanes = 12
	src := &fanSource{lanes: lanes, release: make(chan struct{})}
	srcName := registerFanSource(t, src)
	sinkName := registerProbe(t, "fan_sink", &probeSink{})

	dir := t.TempDir()
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, fanSpec(srcName, sinkName),
		engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
			ControlInterval: time.Second, GracePeriod: 2 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.Now().Add(8 * time.Second)
	var full telemetry.PipelineStatus
	for time.Now().Before(deadline) {
		full = p.Status(telemetry.StatusQuery{})
		if full.LaneCount == lanes {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if full.LaneCount != lanes {
		t.Fatalf("the lane table holds %d lanes, want %d", full.LaneCount, lanes)
	}

	// One page of three, and the rollup still covers all twelve.
	page := p.Status(telemetry.StatusQuery{LaneLimit: ptrTo(3)})
	if len(page.Lanes) != 3 {
		t.Fatalf("asked for 3 lanes and got %d", len(page.Lanes))
	}
	if !page.LanesTruncated || page.LanesCursor == "" {
		t.Error("a cut page did not carry a cursor, so the rest is unreachable")
	}
	if page.LaneCount != lanes {
		t.Errorf("LaneCount is %d on a paged document, want the true total %d", page.LaneCount, lanes)
	}
	if len(page.Streams) != 2 {
		t.Fatalf("%d streams in the rollup, want 2", len(page.Streams))
	}
	var rolled int
	for _, s := range page.Streams {
		rolled += s.Lanes
	}
	if rolled != lanes {
		t.Errorf("the rollup covers %d lanes on a 3-lane page, want all %d: a summary of the page is "+
			"a statement about an arbitrary subset", rolled, lanes)
	}

	// The banner case: no lanes at all, and the rollup is still there.
	banner := p.Status(telemetry.StatusQuery{LaneLimit: ptrTo(0)})
	if len(banner.Lanes) != 0 {
		t.Errorf("a zero limit returned %d lanes", len(banner.Lanes))
	}
	if len(banner.Streams) != 2 || banner.LaneCount != lanes {
		t.Errorf("the banner lost the rollup: %d streams, laneCount %d", len(banner.Streams), banner.LaneCount)
	}

	// Drill down, which is what the rollup is for.
	only := p.Status(telemetry.StatusQuery{Stream: "orders"})
	if len(only.Lanes) != lanes/2 {
		t.Errorf("the orders drill-down returned %d lanes, want %d", len(only.Lanes), lanes/2)
	}
	for _, l := range only.Lanes {
		if l.Stream != "orders" {
			t.Errorf("lane %s is on stream %s and came back from an orders filter", l.ID, l.Stream)
		}
	}

	// A single worker reporting on itself has no staleness threshold, and nil says so.
	if full.StaleAfterSeconds != nil {
		t.Errorf("StaleAfterSeconds is %v for a single worker; nil is what means no threshold applies",
			*full.StaleAfterSeconds)
	}

	close(src.release)
	cancel()
	<-done
}

func ptrTo[T any](v T) *T { return &v }

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/metrics"
	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE READ MODEL HAD NO PRODUCER AND THEREFORE NO TEST. These are the properties the architecture
// states about it, asserted against documents a real pipeline produced rather than against ones
// constructed by hand — which is R8's rule and also the only way the honesty invariant below means
// anything, since the whole risk is that the ENGINE wires two conditions to one input.

func conditionOf(t *testing.T, s telemetry.PipelineStatus, want telemetry.ConditionType) telemetry.Condition {
	t.Helper()
	for _, c := range s.Conditions {
		if c.Type == want {
			return c
		}
	}
	t.Fatalf("condition %s is missing from the document; the set is closed and must be complete", want)
	return telemetry.Condition{}
}

// probeSink is a sink a status test can aim at a specific engine state.
//
// It exists because the shared flaky fixture recomputes WriteResult.Written from what it kept, so
// it cannot under-report — and under-reporting is the cleanest deterministic route to PhaseFailed.
// onOpen is the Note hook: the module's real Note callers are the stress connectors, which live in
// their own registries, so this stands in for them.
type probeSink struct {
	onOpen     func(connector.SinkRuntime)
	underCount bool
}

func (p *probeSink) Open(_ context.Context, rt connector.SinkRuntime, _ connector.Opening) error {
	if p.onOpen != nil {
		p.onOpen(rt)
	}
	return nil
}
func (p *probeSink) Close(context.Context) error { return nil }

func (p *probeSink) Write(_ context.Context, req *connector.Request) (connector.WriteResult, error) {
	if p.underCount {
		return connector.WriteResult{}, nil // accounts for nothing at all
	}
	return connector.AllWritten(req.Count), nil
}

func registerProbe(t *testing.T, prefix string, s *probeSink) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, sinkSeq.Add(1))
	registry.AddSink(registry.Default, registry.SinkDef[*probeSink]{
		Meta: registry.Meta{
			Name: name, Version: "1.0.0", Title: "Probe",
			Summary: "A sink a status test can aim.", Support: registry.SupportCommunity,
		},
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{
			Caps:           connector.Caps{APIVersion: connector.APIVersion},
			Modes:          []connector.DestMode{connector.DestAppend},
			MaxConcurrency: 1,
		},
		New: func(context.Context, *config.Config) (*probeSink, error) { return s, nil },
	})
	return name
}

// TestConnectedNeverImpliesProgressing is the invariant docs/architecture.md states and says is
// asserted by a test — the test it was describing did not exist, because nothing built a document.
//
// "CondSourceReady: True must never be able to imply CondProgressing: True. A fixture in which the
// source and sink are connected and the durable cursor has not moved for an hour must render as
// unhealthy."
//
// A metrics UI that cannot distinguish THE ENDPOINT ANSWERED from YOUR DATA ARRIVED is actively
// misleading, and this is the machine-readable form of that rule. The pipeline here is genuinely in
// that state: both components opened, the source is reading, and the sink never returns from Write,
// so not one record settles and no cursor is ever persisted.
func TestConnectedNeverImpliesProgressing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 5)

	release := make(chan struct{})
	defer close(release)
	stuck := &flaky{answer: func(int, []record.Ref) (connector.WriteResult, error) {
		<-release
		return connector.WriteResult{}, nil
	}}
	name := registerFlaky(t, "status_stuck_sink", stuck, false)

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default,
		routeSpec(name, path, fault.DefaultRetry, ""),
		engine.Deps{State: st, Worker: "test",
			FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Wait until both ends are connected AND the progress window has closed on a cursor that never
	// moved. The window is three flush intervals with a one-second floor, so a stall shorter than
	// that is correctly still reported as unknown rather than as a stall.
	var s telemetry.PipelineStatus
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s = p.Status(telemetry.StatusQuery{})
		src := conditionOf(t, s, telemetry.CondSourceReady)
		prog := conditionOf(t, s, telemetry.CondProgressing)
		if src.Status == telemetry.StatusTrue && prog.Status != telemetry.StatusUnknown {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := conditionOf(t, s, telemetry.CondSourceReady); got.Status != telemetry.StatusTrue {
		t.Fatalf("the source is not reported connected (%v), so this test proves nothing", got)
	}
	if got := conditionOf(t, s, telemetry.CondSinkReady); got.Status != telemetry.StatusTrue {
		t.Fatalf("the sink is not reported connected (%v), so this test proves nothing", got)
	}

	// THE ASSERTION. Both ends are up and canal's own durable cursor has not moved once.
	prog := conditionOf(t, s, telemetry.CondProgressing)
	if prog.Status == telemetry.StatusTrue {
		t.Errorf("Progressing is true while no durable cursor has ever advanced.\n"+
			"  reason: %s\n  message: %s\n"+
			"a connected endpoint is not delivered data, and a UI that cannot tell them apart is worse "+
			"than one with no health signal at all", prog.Reason, prog.Message)
	}
	if prog.Reason != telemetry.ReasonStalled {
		t.Errorf("Progressing is %s/%s, want false/stalled: records are in flight and nothing is durable",
			prog.Status, prog.Reason)
	}
	// And the pipeline must not read as caught up either, because it plainly is not.
	if got := conditionOf(t, s, telemetry.CondCaughtUp); got.Status == telemetry.StatusTrue {
		t.Errorf("CaughtUp is true with %d records in flight", s.Lanes[0].InFlight)
	}
	cancel()
	<-done
}

// A pipeline that has been built and not started is PENDING, and everything the run would establish
// is unknown rather than false. Reporting "not connected" for a connection nobody has attempted
// makes a never-started pipeline indistinguishable from a broken one.
func TestABuiltPipelineIsPendingAndAdmitsWhatItDoesNotKnow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)

	c := &collector{}
	name := registerCollector(t, "status_pending", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	spec := pipelineSpec(name, path)
	spec.Revision = 7
	p, _, diags := engine.Build(context.Background(), registry.Default, spec,
		engine.Deps{State: st, Worker: "test", GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	s := p.Status(telemetry.StatusQuery{})
	if s.Phase != telemetry.PhasePending {
		t.Errorf("phase is %s, want pending", s.Phase)
	}
	if s.Generation != 7 || s.ObservedGeneration != 7 {
		t.Errorf("generations are %d/%d, want 7/7 from the spec revision", s.Generation, s.ObservedGeneration)
	}
	if len(s.Conditions) != len(telemetry.ConditionTypes) {
		t.Fatalf("%d conditions, want the whole closed set of %d", len(s.Conditions), len(telemetry.ConditionTypes))
	}
	for _, want := range []telemetry.ConditionType{
		telemetry.CondSourceReady, telemetry.CondSinkReady, telemetry.CondProgressing,
	} {
		if got := conditionOf(t, s, want); got.Status != telemetry.StatusUnknown {
			t.Errorf("%s is %s before the pipeline started; it must be unknown, because nothing has been "+
				"attempted and false would read as broken", want, got.Status)
		}
	}
	// The version is a cursor, so it must move.
	if next := p.Status(telemetry.StatusQuery{}).Version; next <= s.Version {
		t.Errorf("version went %d -> %d; it is the SSE cursor and must be monotonic", s.Version, next)
	}
}

// A bounded pipeline that read its input to the end is COMPLETED, not stopped and not stalled.
// Without the distinction a finished batch job looks identical to a stuck stream — which is exactly
// the gap PhaseCompleted exists to close.
func TestAFinishedRunIsCompletedAndNotStalled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	want := writeLines(t, path, 20)

	c := &collector{}
	name := registerCollector(t, "status_done", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test",
			FlushInterval: 5 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := p.Status(telemetry.StatusQuery{})
	if s.Phase != telemetry.PhaseCompleted {
		t.Errorf("phase is %s, want completed", s.Phase)
	}
	if got := conditionOf(t, s, telemetry.CondDegraded); got.Status != telemetry.StatusFalse {
		t.Errorf("a clean run is degraded: %s/%s %s", got.Status, got.Reason, got.Message)
	}
	// NOT PROGRESSING, and the reason says why. A finished lane's cursor is final and durable, so
	// "not advancing" is the success case and must not carry the stall reason an alert fires on.
	prog := conditionOf(t, s, telemetry.CondProgressing)
	if prog.Reason == telemetry.ReasonStalled {
		t.Errorf("a completed pipeline reports progressing/%s; a finished lane is not a stalled one",
			prog.Reason)
	}
	if s.LaneCount != 1 {
		t.Fatalf("%d lanes, want 1", s.LaneCount)
	}
	if !s.Lanes[0].Finished {
		t.Error("the lane is not reported finished although the run completed")
	}
	if s.Lanes[0].RecordsCommitted != uint64(len(want)) {
		t.Errorf("the lane settled %d records, want %d", s.Lanes[0].RecordsCommitted, len(want))
	}
	if s.LastFault != nil {
		t.Errorf("a clean run reported a fault: %+v", s.LastFault)
	}
	// The reconcile delta is records in minus records out, and a quiescent pipeline that lost
	// nothing must report exactly zero.
	if s.Throughput.ReconcileDelta == nil || *s.Throughput.ReconcileDelta != 0 {
		t.Errorf("reconcile delta is %v, want 0 for a pipeline that settled everything it read",
			s.Throughput.ReconcileDelta)
	}
}

// EVERY UNKNOWN IS A NIL POINTER, NEVER A ZERO. The document's own rule, asserted on a real one: a
// field the engine cannot measure must marshal to null so the frontend's shared unknown renderer
// gets it, rather than to a confident 0 that renders as a fact.
func TestUnmeasuredFieldsMarshalAsNullAndNotZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 6)

	c := &collector{}
	name := registerCollector(t, "status_nulls", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test",
			FlushInterval: 5 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := p.Status(telemetry.StatusQuery{})
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Nothing in this build measures a backlog, an event-time lag or a position fraction, and no
	// source declares Heartbeater. Each must be absent, not zero.
	//
	// The RATES are here for a different reason and they are the likeliest field to be faked: a rate
	// needs two samples to exist at all, and this is the first materialisation, so there is exactly
	// one. "0 records/s" and "not enough data yet" look identical on a dashboard and mean opposite
	// things — the first says the pipeline is stuck.
	for _, want := range []string{
		`"backlog": null`,
		`"progress": null`,
		`"recordsPerSecondIn": null`,
		`"recordsPerSecondOut": null`,
		`"bytesPerSecondOut": null`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("expected %s in the document; a measured-looking zero is worse than an absence:\n%s",
				want, body)
		}
	}

	// THE OTHER HALF OF THE SAME RULE, and it is the half that keeps the first one from being
	// satisfied by nilling everything. A field the connector DOES supply has to be rendered: linefile
	// authors Position.Label ("byte 55") and stamps Position.At, so the cursor labels and the
	// event-time lag are known here and must not be nil.
	lane := s.Lanes[0]
	if lane.Position == nil || *lane.Position == "" {
		t.Error("the durable cursor has no label although the source authors one; this is the field a UI " +
			"shows as the position for an arbitrary connector with no connector-specific code")
	}
	if lane.Resolved == nil {
		t.Error("the delivered prefix has no label; Position and Resolved are two fields for two facts")
	}
	if lane.EventTimeLag == nil {
		t.Error("no event-time lag although the source stamps Position.At")
	}
	// And the collections must not be null either, which is the other half of the same contract.
	if strings.Contains(string(body), `: null,`) && strings.Contains(string(body), `"lanes": null`) {
		t.Errorf("a collection marshalled to null:\n%s", body)
	}
	// Config is deliberately absent rather than raw: it is the only field that could carry a secret,
	// and the redaction it needs is not plumbed. Absent is the safe direction.
	if strings.Contains(string(body), `"config"`) {
		t.Errorf("the document carries a config tree, which is the one field that can leak a secret:\n%s", body)
	}
}

// LastTransitionTime is the moment a condition last CHANGED. Recomputing it as "now" on every read
// makes it useless — every scrape would look like a transition — and it is the field an operator
// reads to answer "how long has it been like this".
func TestTransitionTimesDoNotMoveWhileNothingChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 4)

	c := &collector{}
	name := registerCollector(t, "status_transitions", c)
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test",
			FlushInterval: 5 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	first := p.Status(telemetry.StatusQuery{})
	time.Sleep(20 * time.Millisecond)
	second := p.Status(telemetry.StatusQuery{})

	for _, a := range first.Conditions {
		b := conditionOf(t, second, a.Type)
		if b.Status != a.Status || b.Reason != a.Reason {
			continue // a genuine transition; its time is allowed to move
		}
		if !b.LastTransitionTime.Equal(a.LastTransitionTime) {
			t.Errorf("%s did not change (%s/%s) but its transition time moved %v -> %v",
				a.Type, a.Status, a.Reason, a.LastTransitionTime, b.LastTransitionTime)
		}
	}
}

// canal_condition was the metric the closed set declared and nothing produced, which left "did my
// config change take effect" unalertable. Every condition gets a series per status so that
// `canal_condition{condition="spec_applied",status="false"} == 1` is an expression rather than an
// absence somebody has to remember to check for.
func TestEveryConditionIsExportedAsExactlyOneTrueSeries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 10)

	c := &collector{}
	name := registerCollector(t, "status_conditions", c)
	body, err := runWithMetrics(t, pipelineSpec(name, path), dir)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Nine conditions times three statuses, all present, bounded by construction because both label
	// values come from closed sets.
	wantSeries := len(telemetry.ConditionTypes) * 3
	got := 0
	active := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, telemetry.MConditions+"{") {
			continue
		}
		got++
		cond := labelValue(line, "condition")
		if strings.HasSuffix(strings.TrimSpace(line), " 1") {
			active[cond]++
		}
	}
	if got != wantSeries {
		t.Errorf("%d canal_condition series, want %d (%d conditions x 3 statuses)",
			got, wantSeries, len(telemetry.ConditionTypes))
	}
	for _, ct := range telemetry.ConditionTypes {
		if active[string(ct)] != 1 {
			t.Errorf("condition %s has %d series set to 1, want exactly 1: a condition is in one state",
				ct, active[string(ct)])
		}
	}
}

// labelValue pulls one label out of a Prometheus sample line.
func labelValue(line, label string) string {
	i := strings.Index(line, label+`="`)
	if i < 0 {
		return ""
	}
	rest := line[i+len(label)+2:]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// A run that ended on a fault says so on the two fields an operator reads for it: the phase and the
// last fault. A failed pipeline with no explanation is the state this document exists to prevent.
func TestAFailedRunReportsItsFault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 8)

	// A SINK THAT UNDER-REPORTS is a contract violation and fails the pipeline outright, rather than
	// abandoning one record: its success cannot be trusted, so settling on it would advance the source
	// past records nobody wrote. It is the cleanest deterministic way to reach PhaseFailed.
	name := registerProbe(t, "status_broken_sink", &probeSink{underCount: true})

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	reg := metrics.New()
	p, _, diags := engine.Build(context.Background(), registry.Default,
		routeSpec(name, path, fault.DefaultRetry, ""),
		engine.Deps{State: st, Worker: "test", Metrics: reg,
			FlushInterval: 5 * time.Millisecond, GracePeriod: time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := p.Run(ctx); err == nil {
		t.Fatal("Run succeeded against a sink that refuses every write")
	}

	s := p.Status(telemetry.StatusQuery{})
	if s.Phase != telemetry.PhaseFailed {
		t.Errorf("phase is %s, want failed", s.Phase)
	}
	if s.LastFault == nil {
		t.Fatal("a failed run reported no last fault, so the status page cannot say what happened")
	}
	if s.LastFault.At.IsZero() {
		t.Error("the fault has no timestamp; an operator cannot tell a fresh failure from an old one")
	}
	if got := conditionOf(t, s, telemetry.CondDegraded); got.Status != telemetry.StatusTrue {
		t.Errorf("Degraded is %s after a fault ended the run", got.Status)
	}
	// The node the fault belongs to must own it in the rollup, or a fan-out branch's failure is
	// attributed to the pipeline at large.
	var faults uint64
	for _, n := range s.Nodes {
		if n.ID == "out" {
			faults = n.Faults
		}
	}
	if faults == 0 {
		t.Error("the sink node reports no faults although every write to it failed")
	}
}

// A connector's Note is the read model's RecentEvents, which baseRuntime.Note has promised in a
// comment since it was written while the events went nowhere but the log.
func TestConnectorEventsReachTheDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 4)

	name := registerProbe(t, "status_events", &probeSink{
		onOpen: func(rt connector.SinkRuntime) {
			rt.Note(connector.Event{
				Kind: connector.EventNote, Message: "probe sink opened", Detail: "from Open",
			})
		},
	})
	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer st.Close()

	p, _, diags := engine.Build(context.Background(), registry.Default, pipelineSpec(name, path),
		engine.Deps{State: st, Worker: "test",
			FlushInterval: 5 * time.Millisecond, GracePeriod: 5 * time.Second})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// linefile notes one event per lane it announces.
	s := p.Status(telemetry.StatusQuery{})
	if len(s.RecentEvents) == 0 {
		t.Fatal("no connector events reached the document although the source announces a lane")
	}
	for _, e := range s.RecentEvents {
		if e.At.IsZero() {
			t.Errorf("event %q has no time; it would sort to the front as the oldest thing that happened",
				e.Message)
		}
		if e.Node == "" {
			t.Errorf("event %q names no node, so an operator cannot tell which component raised it",
				e.Message)
		}
	}
}

// THE RUNNER IS PUBLISHED BEFORE IT IS OPEN, and that is deliberate: Pipeline.Run stores it so a
// status read during a slow open reports "starting" rather than "pending". The two are different
// answers and opening a source against an unreachable upstream is exactly when somebody is watching.
//
// It also means every field the status path touches can be read from another goroutine while the run
// goroutine is still building the pipeline. That was a real data race on both counts — the topology
// maps were being inserted into while Status ranged them, and the runner's own fields were being
// assigned after the store — and it was found by CI on macOS/arm64 rather than by two clean local
// -race runs, because it needs a Status to land inside the open window at all.
//
// This hammers that window on purpose. Four goroutines read the document as fast as they can from
// before Run is even called, so the reads span construction, the sink open, the checkpoint recovery,
// the source open and the first records. Both halves of the fix were confirmed by putting the defect
// back: unlocking the topology accessors reproduces it, and so does publishing the runner before its
// fields are assigned.
func TestStatusIsSafeToReadWhileThePipelineIsStillOpening(t *testing.T) {
	for i := 0; i < 5; i++ {
		src := &controlSource{emit: 2, release: make(chan struct{}), ordering: connector.OrderingPrefix}
		srcName := registerControlSource(t, src)
		sinkName := registerProbe(t, "opening_race_sink", &probeSink{})

		dir := t.TempDir()
		st, err := wal.Open(filepath.Join(dir, "state"))
		if err != nil {
			t.Fatalf("opening the store: %v", err)
		}
		p, _, diags := engine.Build(context.Background(), registry.Default, controlSpec(srcName, sinkName),
			engine.Deps{State: st, Worker: "test", FlushInterval: 5 * time.Millisecond,
				ControlInterval: 20 * time.Millisecond, GracePeriod: time.Second})
		if diags.HasErrors() {
			t.Fatalf("Build: %v", diags)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stop := make(chan struct{})
		var readers sync.WaitGroup
		for j := 0; j < 4; j++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
						// The document must be well-formed at every instant, not merely eventually:
						// a half-built one is what a scrape would render.
						if s := p.Status(telemetry.StatusQuery{}); len(s.Conditions) != len(telemetry.ConditionTypes) {
							t.Errorf("a document read mid-open carried %d conditions, want the whole "+
								"closed set of %d", len(s.Conditions), len(telemetry.ConditionTypes))
							return
						}
					}
				}
			}()
		}

		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		time.Sleep(60 * time.Millisecond)
		close(src.release)
		cancel()
		<-done
		close(stop)
		readers.Wait()

		_ = p.Close(context.Background())
		_ = st.Close()
		if t.Failed() {
			return
		}
	}
}

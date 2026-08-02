package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/engine"
	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// Deps.Status WAS THE LAST ENTRY ON THE INERT-FIELD ALLOWLIST. store.StatusStore.Report publishes
// one worker's read model and Aggregate merges every worker's into one; with a single worker the
// document was complete by construction, so nothing ever called either.
//
// These assert what a worker PUBLISHES, not that Report was called. The difference matters here more
// than usual: a report is consumed by an aggregator this branch does not write, so the only thing
// standing between a wrong document and a wrong cluster-wide view is what these check.

func runReporting(t *testing.T, st store.StatusStore, tune ...func(*engine.Deps)) *engine.Pipeline {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 3)

	state, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	t.Cleanup(func() { state.Close() })

	deps := engine.Deps{
		State: state, Status: st, Worker: "w1",
		FlushInterval: 5 * time.Millisecond, StatusInterval: 5 * time.Millisecond,
		GracePeriod: time.Second,
	}
	for _, f := range tune {
		f(&deps)
	}
	p, _, diags := engine.Build(context.Background(), registry.Default,
		pipelineSpec(registerCollector(t, "report_sink", &collector{}), path), deps)
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	t.Cleanup(func() { p.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return p
}

// A WORKER PUBLISHES ITS OWN VIEW, and the document has to be usable by something that never saw
// this process — so it carries the identity an aggregator keys on and the phase it ended in.
func TestAWorkerPublishesItsReadModel(t *testing.T) {
	status := memstore.NewStatus()
	runReporting(t, status)

	reports := status.Reports("acme", "p1")
	if len(reports) != 1 {
		t.Fatalf("%d workers published, want 1", len(reports))
	}
	got, ok := reports["w1"]
	if !ok {
		t.Fatalf("nothing was published under this worker's id; the reports are keyed %v", reports)
	}

	if got.Status.Tenant != "acme" || got.Status.Pipeline != "p1" {
		t.Errorf("the published document is for %s/%s; an aggregator keys on those and cannot "+
			"attribute a report without them", got.Status.Tenant, got.Status.Pipeline)
	}
	// THE LAST WORD IS THE MOST USEFUL ONE. reportLoop publishes once more on the way out, so a
	// stopped worker shows as stopped rather than as a report that simply stopped arriving — which
	// an aggregator would render as missing.
	if got.Status.Phase != telemetry.PhaseCompleted {
		t.Errorf("the last published phase is %s, want completed: without a final report the "+
			"aggregate cannot tell a clean exit from a worker that fell off the map", got.Status.Phase)
	}
	if got.At.IsZero() {
		t.Error("the report carries no arrival time, so no staleness threshold can be applied to it")
	}
}

// A PUBLISHED REPORT CARRIES NO LANE PAGE. The lane list is the largest thing in the document and
// the least useful part to somebody assembling a cluster-wide view; LaneCount and the per-stream
// rollup carry the shape without the payload.
func TestAPublishedReportOmitsTheLanePageAndKeepsTheCounts(t *testing.T) {
	status := memstore.NewStatus()
	runReporting(t, status)

	got := status.Reports("acme", "p1")["w1"]
	if len(got.Status.Lanes) != 0 {
		t.Errorf("the published document carries %d lanes; a source with 10^5 scan chunks would "+
			"make every worker's report the largest thing in the cluster", len(got.Status.Lanes))
	}
	if got.Status.LaneCount == 0 {
		t.Error("the document reports no lanes at all; omitting the PAGE must not omit the COUNT, " +
			"or an aggregator cannot tell an idle worker from a busy one")
	}
	if !got.Status.LanesTruncated {
		t.Error("the lane list is empty and not marked truncated, which is the same lie as a status " +
			"document that silently omits a worker")
	}
}

// REPORTING IS BEST-EFFORT, and the interface says so: a failure must never affect the data path,
// because the data plane keeps running with the entire control plane down.
func TestAStatusStoreThatRefusesEveryReportDoesNotAffectTheRun(t *testing.T) {
	broken := &refusingStatus{}
	runReporting(t, broken)

	if broken.calls.Load() == 0 {
		t.Fatal("the store was never called, so this test proves nothing about failing calls")
	}
	// runReporting fails the test if Run returns an error, so reaching here is the assertion: every
	// report failed and the pipeline still ran to completion.
}

// A WORKER WITH NO STATUS STORE PUBLISHES NOTHING AND RUNS THE SAME. This is the standalone shape,
// and it must be untouched.
func TestAWorkerWithNoStatusStoreRunsUnchanged(t *testing.T) {
	p := runReporting(t, nil, func(d *engine.Deps) { d.Status = nil })
	if got := p.Status(telemetry.StatusQuery{}).Phase; got != telemetry.PhaseCompleted {
		t.Errorf("the standalone pipeline ended %s, want completed", got)
	}
}

// Aggregate REFUSES RATHER THAN GUESSES. Merging N documents is a decision per field, and the
// interface names the part that is uniquely easy to get wrong — Complete false with Missing
// populated. Half of that answered convincingly is worse than none of it answered at all.
func TestAggregateRefusesUntilItIsImplemented(t *testing.T) {
	_, err := memstore.NewStatus().Aggregate(context.Background(), "acme", "p1", telemetry.StatusQuery{})
	if err == nil {
		t.Fatal("Aggregate returned a document; nothing merges reports yet, and a plausible answer " +
			"from an aggregator that has not been written is the worst possible outcome")
	}
}

// refusingStatus fails every report, the way a control plane that is down does.
type refusingStatus struct{ calls atomic.Int64 }

func (r *refusingStatus) Report(context.Context, store.WorkerID, telemetry.PipelineStatus) error {
	r.calls.Add(1)
	return errors.New("dial tcp 127.0.0.1:5432: connection refused")
}

func (r *refusingStatus) Aggregate(context.Context, record.TenantID, record.PipelineID,
	telemetry.StatusQuery,
) (telemetry.PipelineStatus, error) {
	return telemetry.PipelineStatus{}, errors.New("dial tcp 127.0.0.1:5432: connection refused")
}

// A RUN LONGER THAN ITS GRACE PERIOD KEEPS DELIVERING, and until this branch it did not.
//
// The delivery context was built as WithTimeout(WithoutCancel(ctx), GracePeriod) at run START, so
// the grace period was a deadline on the whole run rather than on the drain: every delivery past it
// failed with "context deadline exceeded", the read loop took that as a terminal read fault, and the
// source stopped. A pipeline with a one-second grace died one second in.
//
// Nothing caught it because the default is thirty seconds and no test ran that long while asserting
// the pipeline was still alive. This one runs four grace periods deep and checks records are still
// arriving at the end — which is the property, rather than the absence of an error.
func TestAPipelineOutlivesItsGracePeriod(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.txt")
	writeLines(t, path, 2)
	src := &leaseSource{}

	st, err := wal.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("opening the state store: %v", err)
	}
	defer st.Close()

	const grace = 250 * time.Millisecond
	spec := pipelineSpec(registerCollector(t, "grace_sink", &collector{}), path)
	spec.Graph[0].Name, spec.Graph[0].Config = registerLeaseSource(t, src), map[string]any{}
	spec.Streams[0].Read = []connector.LaneKind{connector.LaneKindStream}

	p, _, diags := engine.Build(context.Background(), registry.Default, spec,
		engine.Deps{State: st, Worker: "single", FlushInterval: 5 * time.Millisecond,
			GracePeriod: grace})
	if diags.HasErrors() {
		t.Fatalf("Build: %v", diags)
	}
	defer p.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Gated on the pipeline having lived four grace periods AND still acknowledging, so a run that
	// quietly died at one grace period cannot pass by having committed a lot before it did.
	time.Sleep(4 * grace)
	before := src.committed()
	deadline := time.Now().Add(10 * time.Second)
	for src.committed() <= before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	after := src.committed()
	cancel()
	<-done

	if before == 0 {
		t.Fatal("nothing was acknowledged at all, so this test cannot tell a live pipeline from a dead one")
	}
	if after <= before {
		t.Errorf("the pipeline acknowledged %d records in its first %v and none after.\n"+
			"  The delivery context's deadline is being measured from the start of the run rather "+
			"than from the moment reading stops, so the grace period kills the run it was meant to "+
			"bound the shutdown of", before, 4*grace)
	}
}

package engine

import (
	"context"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// PUBLISHING THE READ MODEL, AND WHY IT IS ALLOWED TO FAIL.
//
// Deps.Status was the last entry on the inert-field allowlist. store.StatusStore.Report publishes
// one worker's view of a pipeline and Aggregate merges every worker's into one document; with a
// single worker the document was complete by construction, so nothing ever called either.
//
// This file is Report. A worker with a store.StatusStore publishes its own read model on a timer,
// which is what gives an aggregator something to merge — and the aggregator is deliberately NOT
// here: merging N documents into one is a decision per field, and the interface names the part that
// is uniquely easy to get wrong. See internal/example/memstore, whose Aggregate refuses rather than
// guesses, and the allowlist entry that records it.
//
// EVERY FAILURE HERE IS INERT ON THE DATA PATH, and the interface says so in as many words: "It is
// best-effort: a failure to report status must never affect the data path, because the data plane
// keeps running with the entire control plane down." So a report that fails is logged and dropped.
// Retrying would be worse than useless — the next tick carries a fresher document than the one that
// failed, and a queue of stale reports is a queue of answers nobody wants.

// reportLoop publishes this worker's read model until the run stops.
//
// It takes ctx rather than the read context, so a draining pipeline goes on reporting. A drain is
// exactly when somebody is watching, and a worker that stopped publishing the moment it stopped
// reading would vanish from the aggregate at the least convenient moment — which an aggregator would
// correctly render as a missing worker rather than a stopping one.
func (r *runner) reportLoop(ctx context.Context, stop <-chan struct{}) {
	if r.deps.Status == nil {
		return
	}
	t := time.NewTicker(r.deps.StatusInterval)
	defer t.Stop()

	// Reported once up front, so a worker appears in the aggregate as soon as it exists rather than
	// one interval later. A worker that is slow to open is the case somebody is most likely to be
	// looking for, and it is the case that would be missing.
	r.report(ctx)
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			r.report(ctx)
		}
	}
}

// reportFinal publishes the last word, from run's own exit rather than from the loop.
//
// THE PHASE IS THE POINT, and it is why this is not the loop's job. The loop is stopped alongside
// the control and flush loops, which happens BEFORE the run settles on the phase it ended in — so a
// final report from there says "draining" for a pipeline that completed. Registered as a defer at
// the top of run, this runs after everything, including after the terminal phase is set.
//
// The context is detached because shutdown is precisely when the run context is cancelled, and a
// final report that cancels itself is no report at all.
func (r *runner) reportFinal(ctx context.Context) {
	if r.deps.Status == nil {
		return
	}
	r.report(context.WithoutCancel(ctx))
}

// report publishes one document.
//
// The QUERY ASKS FOR NO LANES. A published report is for an aggregator to merge, and the lane page a
// worker happens to hold is the largest thing in the document and the least useful part of it to
// somebody assembling a cluster-wide view — LaneCount and the per-stream rollup carry the shape
// without the payload. An aggregator that needs lanes asks the worker for them.
func (r *runner) report(ctx context.Context) {
	none := 0
	doc := r.p.Status(telemetry.StatusQuery{LaneLimit: &none})
	if err := r.deps.Status.Report(ctx, r.deps.Worker, doc); err != nil && ctx.Err() == nil {
		r.deps.Log.Warn("could not publish this worker's status; the data path is unaffected",
			"worker", r.deps.Worker, "error", err)
	}
}

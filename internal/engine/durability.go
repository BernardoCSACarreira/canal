package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// WHERE DURABILITY IS EARNED.
//
// For most sinks it is Write returning cleanly, and the engine settled there and nowhere else —
// which is why [Executable] had to refuse every sink that earns it later. This file is the other
// answer: a sink that declares [connector.Flusher] accepts records on Write and makes them durable
// on Flush, so between those two calls the records are WRITTEN BUT NOT SAFE and must not be settled.
//
// Settling early is design rule R4's violation with extra steps. The prefix would advance, the
// cursor would be persisted, the source would be told to move on, and a crash before the next Flush
// would lose everything the sink was still holding. So the records wait here instead, holding their
// ledger references, which is also exactly the backpressure a sink with a slow durability cadence
// should produce.
//
// THE FLUSH RUNS BEFORE THE CURSOR IS PERSISTED. Phase two writes a position that must already be
// durable at the destination, so [runner.flushOnce] flushes sinks first and only then asks the
// ledger what has resolved. Reversing those two is unrecoverable in exactly the way ADR 0006 is
// about.

// deferred holds the records a sink has accepted but not yet made durable.
//
// Keyed by node, because two Flusher sinks flush independently and a record delivered to both is
// durable at each on its own schedule.
type deferred struct {
	mu      sync.Mutex
	pending map[record.NodeID][]*record.Record

	// covered is the highest position per lane among the records a node is holding. It is the
	// DURABLE BLAST RADIUS a Committable has to carry: after a restart, "which span of which lane
	// does this staged artifact cover" is answerable from the checkpoint alone, which is exactly
	// when a failed commit needs to name what it is withholding.
	covered map[record.NodeID]map[record.LaneID]record.Position

	// flushing serialises Flush per node. SinkCaps.MaxConcurrency is one for every Flusher in the
	// module and the contract permits a sink to assume it, so the record-count trigger below must
	// not be able to run a second Flush alongside the flush loop's.
	flushing map[record.NodeID]*sync.Mutex

	// trigger is the pending count at which a sink is flushed without waiting for the tick.
	trigger int
}

func newDeferred(trigger int) *deferred {
	return &deferred{
		pending:  map[record.NodeID][]*record.Record{},
		covered:  map[record.NodeID]map[record.LaneID]record.Position{},
		flushing: map[record.NodeID]*sync.Mutex{},
		trigger:  trigger,
	}
}

// lock returns the per-node flush mutex, creating it on first use.
func (d *deferred) lock(node record.NodeID) *sync.Mutex {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.flushing[node]
	if !ok {
		m = &sync.Mutex{}
		d.flushing[node] = m
	}
	return m
}

// due reports whether a node has enough pending work to be flushed now.
func (d *deferred) due(node record.NodeID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.trigger > 0 && len(d.pending[node]) >= d.trigger
}

// hold records that a sink has accepted, pending its next successful flush or commit.
//
// at is the position of the batch they arrived in, kept per lane so a committable can name the span
// it covers.
func (d *deferred) hold(node record.NodeID, recs []*record.Record, lane record.LaneID, at record.Position) {
	if len(recs) == 0 {
		return
	}
	d.mu.Lock()
	d.pending[node] = append(d.pending[node], recs...)
	if d.covered[node] == nil {
		d.covered[node] = map[record.LaneID]record.Position{}
	}
	// Later batches from one lane arrive in order, so the last one seen is the furthest.
	d.covered[node][lane] = at
	d.mu.Unlock()
}

// takeCovered removes and returns the covered span for a node, alongside take.
func (d *deferred) takeCovered(node record.NodeID) map[record.LaneID]record.Position {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.covered[node]
	delete(d.covered, node)
	return out
}

// take removes and returns everything held for a node, so the flush that follows owns them.
//
// Taking BEFORE the flush rather than after is deliberate: a Write completing while Flush is in
// flight must not have its record silently covered by a flush that started before it arrived. The
// records taken here are exactly the ones the sink had when the flush began, and anything that
// lands during it is held for the next one.
func (d *deferred) take(node record.NodeID) []*record.Record {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.pending[node]
	delete(d.pending, node)
	return out
}

// returnUnflushed puts records back at the FRONT of the queue, preserving order.
//
// Used when a flush reports records it did not make durable. They are still the sink's oldest
// outstanding work, so they must stay ahead of anything that arrived while the flush ran.
func (d *deferred) returnUnflushed(node record.NodeID, recs []*record.Record) {
	if len(recs) == 0 {
		return
	}
	d.mu.Lock()
	d.pending[node] = append(recs, d.pending[node]...)
	d.mu.Unlock()
}

// outstanding reports how many records a node is holding, for the leak report at shutdown.
func (d *deferred) outstanding() map[record.NodeID]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[record.NodeID]int{}
	for node, recs := range d.pending {
		if len(recs) > 0 {
			out[node] = len(recs)
		}
	}
	return out
}

// defersDurability reports whether a sink earns its acknowledgement after Write returns.
func defersDurability(sk *registry.ResolvedSink) bool {
	return sk.Flusher != nil || sk.Committer != nil
}

// commitsDurability reports whether a sink publishes through the two-phase commit protocol rather
// than through Flush. A sink implementing both is a Committer: its Flush is a signal carrier, and
// the acknowledgement is earned at Commit, which is what AckPoint already says.
func commitsDurability(sk *registry.ResolvedSink) bool { return sk.Committer != nil }

// flushSinks makes every deferring sink's accepted records durable and settles them.
//
// It is called from the flush loop with [connector.FlushCheckpoint], from the drain with
// FlushDrain, and when a bounded source finishes with FlushEndOfInput — the distinction matters to
// a staging sink, which seals an undersized file at end of input and would hold it forever on a
// periodic checkpoint.
func (r *runner) flushSinks(ctx context.Context, reason connector.FlushReason) {
	for _, id := range r.mainSinks {
		sk := r.p.sinks[id]
		if sk.Flusher == nil {
			continue
		}

		// A COMMITTER'S FLUSH IS A SIGNAL, NOT A DURABILITY POINT, and it must still be delivered.
		//
		// Its records are published by commit.go, so nothing is settled from the result — that is
		// what keeps the operator-visible acknowledgement point at "commit" rather than sliding to
		// "flush" as a side effect of the sink needing one bool. But skipping the CALL is a
		// different thing entirely: internal/stress/txn-sink sets its finalize flag in Flush and
		// reads it in the next PrepareCommit, so a skipped flush meant it never sealed an undersized
		// file, answered RetryLater forever, and hung the drain. Found by running it.
		if commitsDurability(sk) {
			if err := r.signalFlush(ctx, id, sk, reason); err != nil {
				r.fail(err)
				return
			}
			continue
		}
		if !defersDurability(sk) {
			continue
		}
		m := r.deferred.lock(id)
		m.Lock()
		held := r.deferred.take(id)

		// A TERMINAL REASON IS A SIGNAL AND IS ALWAYS DELIVERED, even with nothing held.
		//
		// "Nothing pending" is a statement about the CORE's records. It says nothing about what the
		// sink is holding internally — an undersized staging file, an open multipart upload — and
		// FlushEndOfInput is precisely how such a sink learns to seal it rather than wait for more
		// that is never coming. Skipping the call because the core had nothing outstanding withheld
		// the one signal the capability exists to carry.
		//
		// A periodic checkpoint with nothing pending is skipped, because there it really is a no-op
		// and an fsync per tick on an idle pipeline is a cost with no purchase.
		if len(held) == 0 && reason == connector.FlushCheckpoint {
			m.Unlock()
			continue
		}
		err := r.flushOne(ctx, id, sk, held, reason)
		m.Unlock()
		if err != nil {
			r.fail(err)
			return
		}
	}
}

// flushIfDue flushes a sink that has accumulated enough pending work to be worth it, without
// waiting for the next tick.
//
// WHY THIS EXISTS, measured rather than assumed. With a Flusher, in-flight means NOT YET DURABLE,
// so every record between two flushes counts against the lane budget. Flushing only on the timer
// therefore caps throughput at budget/interval — with the defaults, 1000 records per 250ms. A
// 200,000-record run that used to finish in a second took 30 seconds and then failed its drain,
// which is how this was found.
//
// Deps.FlushRecords was declared for exactly this and had never been read. The trigger is capped by
// the LANE BUDGET as well, because once the budget is full nothing more can be admitted and a flush
// is the only thing that can unblock the pipeline: at that point flushing is not eager, it is the
// whole remaining move.
func (r *runner) flushIfDue(ctx context.Context, id record.NodeID) {
	sk := r.p.sinks[id]
	if !defersDurability(sk) || commitsDurability(sk) || !r.deferred.due(id) {
		return
	}
	m := r.deferred.lock(id)
	m.Lock()
	defer m.Unlock()

	// Re-checked under the lock: the flush loop may have taken this work while we waited.
	held := r.deferred.take(id)
	if len(held) == 0 {
		return
	}
	if err := r.flushOne(ctx, id, sk, held, connector.FlushCheckpoint); err != nil {
		r.fail(err)
	}
}

// signalFlush delivers a flush to a Committer purely for its reason.
//
// The WriteResult is deliberately discarded: a Committer's durability is earned at Commit, and
// reading Failed or Deferred here would settle — or refuse to settle — records the commit protocol
// owns. The only thing that matters is that the sink was told, and that a refusal is loud.
func (r *runner) signalFlush(ctx context.Context, id record.NodeID, sk *registry.ResolvedSink,
	reason connector.FlushReason,
) error {
	_, err := sandbox(ctx, r.p.obs, id, sk.Name, reason,
		func(c context.Context, rn connector.FlushReason) (connector.WriteResult, error) {
			return sk.Flusher.Flush(c, rn)
		})
	if err != nil {
		r.p.obs.fault(id, err)
		return fmt.Errorf("engine: signalling %s to sink %s: %w", reason, id, err)
	}
	return nil
}

// flushOne flushes one sink and settles what it made durable.
func (r *runner) flushOne(ctx context.Context, id record.NodeID, sk *registry.ResolvedSink,
	held []*record.Record, reason connector.FlushReason,
) error {
	res, err := sandbox(ctx, r.p.obs, id, sk.Name, reason,
		func(c context.Context, rn connector.FlushReason) (connector.WriteResult, error) {
			return sk.Flusher.Flush(c, rn)
		})
	if err != nil {
		// NOTHING IS SETTLED and everything goes back. A failed flush means the sink still holds
		// these records, so the ledger must keep holding their references — the alternative is a
		// cursor that advances past data nobody has made durable.
		r.deferred.returnUnflushed(id, held)
		r.p.obs.fault(id, err)
		return fmt.Errorf("engine: flushing sink %s: %w", id, err)
	}

	// Failed and Deferred mean different things and neither settles. Failed is "not durable, resend
	// it"; Deferred is "accepted, not durable yet, do NOT resend" — the fourth quadrant a sink whose
	// durability cadence is coarser than the checkpoint interval cannot do without.
	notDurable := make(map[record.RecordID]bool, len(res.Failed)+len(res.Deferred))
	for i := range res.Failed {
		notDurable[res.Failed[i].Record] = true
	}
	for _, rid := range res.Deferred {
		notDurable[rid] = true
	}

	durable := make([]*record.Record, 0, len(held))
	var back []*record.Record
	for _, rec := range held {
		if notDurable[rec.Origin().ID] {
			back = append(back, rec)
			continue
		}
		durable = append(durable, rec)
	}
	r.deferred.returnUnflushed(id, back)

	if len(durable) > 0 {
		r.p.ledger.Settle(outcomesFor(id, durable, connector.WriteResult{Duplicates: res.Duplicates}))
		r.p.obs.wrote(id, sk.Name, len(durable))
	}
	if len(res.Failed) > 0 {
		// A flush that names failures is reporting records the sink could not make durable. They
		// stay pending and the next flush retries them; naming it here is what makes a sink that
		// never succeeds visible before the drain times out.
		r.deps.Log.Warn("a sink could not make some records durable; they stay in flight",
			"node", id, "failed", len(res.Failed), "reason", reason)
	}
	return nil
}

// reportUndrained names records still held by a deferring sink when the run ends.
//
// These are not lost — they were never settled, so no cursor advanced past them and they will be
// re-read on restart — but the operator should know the pipeline stopped holding work a sink had
// accepted and never made durable.
func (r *runner) reportUndrained() {
	for node, n := range r.deferred.outstanding() {
		r.deps.Log.Warn("a sink still holds records it never made durable; they will be re-read on restart",
			"node", node, "records", n, "reason", telemetry.ReasonDrainTimeout)
		r.p.obs.recordsAbandoned.Add(0, r.p.obs.pipeline, "", telemetry.ReasonDrainTimeout)
	}
}

// settleOrHold is the branch [runner.writeOnce] takes after a successful Write.
//
// A sink that is durable on return settles now. One that defers holds, and the ledger keeps its
// references until the flush that earns them.
func (r *runner) settleOrHold(id record.NodeID, sk *registry.ResolvedSink,
	landed []*record.Record, res connector.WriteResult, lane record.LaneID, at record.Position,
) {
	if len(landed) == 0 {
		return
	}
	if defersDurability(sk) {
		r.deferred.hold(id, landed, lane, at)
		return
	}
	r.p.ledger.Settle(outcomesFor(id, landed, res))
	r.p.obs.wrote(id, sk.Name, len(landed))
}

// verify at compile time that the ledger outcome vocabulary this file relies on has not moved.
var _ = ledger.Delivered
var _ = fault.OpFlush

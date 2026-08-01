package engine

import (
	"context"
	"fmt"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// MARK AND ROUTE, which pkg/record has offered connector authors since it was written and the engine
// never implemented.
//
// record.MarkFailed says it exactly: "attaches a fault and lets the record continue. The engine's
// configured routing decides whether that means a Failed edge, a drop, or a pipeline stop." Those
// three are fault.Terminal's three values, so the doc was describing a mapping that did not exist —
// a source could mark a record, the record continued, and it arrived at the destination as though
// nothing had been said about it. Silently delivering a record its own producer declared broken is
// the worst available outcome, and it was the default one.
//
// It is the LAST of this shape: a public method whose documented effect nothing implemented. The
// field guard in internal/arch found the class; this is the member of it that had an accessor and
// no caller, which is why a field-level check could not see it.
//
// WHY THE TERMINAL DISPOSITION AND NOT THE RETRY LOOP. A marked record has already been produced —
// there is nothing to retry, because the failure is a statement about the record rather than about
// an attempt to deliver it. spec.Retry.Terminal is what an operator configured for "this record is
// not going to make it", and reaching for it is what makes mark-and-route need no vocabulary of its
// own, which is the property MarkFailed's own comment claims for it.

// routeMarked pulls records the source declared broken out of a batch and disposes of them.
//
// It runs BEFORE admission, like the clock check, so a routed record never takes a settlement
// reference and the batch's position still advances over it. The returned batch may be shorter, and
// may be empty — a batch whose every record was marked is a legal zero-record positioned batch,
// which the ledger already knows how to admit.
func (r *runner) routeMarked(ctx context.Context, node record.NodeID, b *record.Batch) (*record.Batch, error) {
	if b.Len() == 0 {
		return b, nil
	}
	// Cheap scan first: the overwhelmingly common case is that nothing is marked, and a source that
	// never calls MarkFailed should pay one comparison per record and no allocation.
	var marked int
	for _, rec := range b.Records {
		if _, bad := rec.Failed(); bad {
			marked++
		}
	}
	if marked == 0 {
		return b, nil
	}

	kept := b.Records[:0]
	for _, rec := range b.Records {
		ferr, bad := rec.Failed()
		if !bad {
			kept = append(kept, rec)
			continue
		}
		if ferr == nil {
			// Defensive: Failed reports (nil, false) for an unmarked record, so this is unreachable
			// through the public API. Keeping the record is the conservative branch.
			kept = append(kept, rec)
			continue
		}

		switch r.p.spec.Retry.Terminal {
		case fault.TerminalDeadLetter:
			// Delivered BEFORE the record is dropped, exactly as on the write path: a dead letter
			// that could not be written is not a dead letter, and dropping on the strength of a
			// failed delivery is the silent loss the route exists to prevent.
			if err := r.deadLetter(ctx, rec, ferr); err != nil {
				return b, err
			}
			r.markedGone(node, rec, ferr, telemetry.ReasonTerminalFault)

		case fault.TerminalDrop:
			r.markedGone(node, rec, ferr, telemetry.ReasonTerminalFault)

		default: // fault.TerminalStop
			return b, fmt.Errorf(
				"engine: source %s marked record %v failed and terminal is stop: %w",
				node, rec.Origin().ID, ferr)
		}
	}
	b.Records = kept
	return b, nil
}

// markedGone counts and logs a record the source declared broken, and tells the source so.
//
// It does NOT settle anything: the record never reached the ledger, so there is no group to settle
// and no reference to discharge. The nack is the whole of what the source gets back, and it is the
// same nack a record abandoned on the write path produces — one vocabulary for "this did not make
// it", whichever end of the pipeline decided.
func (r *runner) markedGone(node record.NodeID, rec *record.Record, ferr error, reason string) {
	class := fault.ClassOf(ferr)
	r.deps.Log.Error("a record its own source marked failed will not be delivered",
		"node", node, "record", rec.Origin().ID, "lane", rec.Origin().Lane,
		"class", class, "blame", class.Blames(), "reason", reason, "error", ferr)

	r.p.obs.recordsAbandoned.Add(1, r.p.obs.pipeline, laneLabel(rec.Origin().Lane), reason)
	r.nack(rec, record.Position{}, ferr, reason, 0)
}

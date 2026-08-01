package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/registry"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// writeSet drives one set of records at one sink to a terminal state, retrying what the policy
// allows.
//
// A WHOLE-REQUEST ERROR AND A PER-RECORD FAILURE ARE THE SAME EVENT AT DIFFERENT GRANULARITIES, and
// this loop treats them that way: a request-level error is attributed to every record the request
// carried, a WriteResult.Failed entry to the one it names, and both feed the same routing table.
// Writing them as two code paths is how the per-record case ended up with no retry at all — every
// named failure was settled Abandoned on its first occurrence, so a sink that failed one record in
// a batch for a transient reason lost it, permanently, with the cursor moving past.
//
// Nothing is settled until it is decided. A record that will be retried holds its reference, so the
// lane's prefix cannot advance past it, which is exactly the backpressure a struggling sink should
// produce.
func (r *runner) writeSet(ctx context.Context, id record.NodeID, records []*record.Record) error {
	sk := r.p.sinks[id]
	cc := r.p.codecs[id]
	policy := r.p.spec.Retry

	attempts := make(map[record.RecordID]*attempt, len(records))
	att := func(rec *record.Record) *attempt {
		k := rec.Origin().ID
		a, ok := attempts[k]
		if !ok {
			a = &attempt{started: time.Now()}
			attempts[k] = a
		}
		return a
	}

	pending := records
	for len(pending) > 0 {
		failed, err := r.writeOnce(ctx, id, sk, cc, pending)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}

		var (
			retry []*record.Record
			wait  time.Duration
		)
		// Iterating pending rather than the failure map keeps the order deterministic, which
		// matters for the log an operator reads after a bad minute.
		for _, rec := range pending {
			ferr, bad := failed[rec.Origin().ID]
			if !bad {
				continue
			}
			// Counted BEFORE routing: route normalises Unclassified to PermanentInternal, and the
			// counter that must stay zero for a compliant connector only works on the raw class.
			r.p.obs.fault(id, ferr)
			rt := route(ferr, sk.Caps.Idempotent, policy, att(rec), time.Now())

			switch rt.disp {
			case dispRetry:
				retry = append(retry, rec)
				// One wait for the whole round, the longest anybody asked for. Sleeping per record
				// would serialise a batch's backoff into the sum of its parts.
				if rt.delay > wait {
					wait = rt.delay
				}

			case dispDeadLetter:
				// The dead letter is delivered BEFORE the record is abandoned. A dead letter that
				// could not be written is not a dead letter, and abandoning on the strength of a
				// failed delivery would be exactly the silent loss the route exists to prevent.
				if err := r.deadLetter(ctx, rec, ferr); err != nil {
					return err
				}
				r.abandon(id, rec, ferr, rt.reason)

			case dispDrop:
				r.abandon(id, rec, ferr, rt.reason)

			case dispStall:
				// SCOPE (R10). The specified behaviour is to block THIS LANE and leave the rest of
				// the pipeline running. With one lane per source and no control plane to unblock
				// it, "block the lane" and "stop" have the same observable outcome, so this stops
				// and says why rather than pretending to a granularity that does not exist yet.
				return fault.Contract(fault.OpWrite, fmt.Errorf(
					"engine: sink %s may or may not have written record %v, and it is not idempotent: %w"+
						"\n  the pipeline is stopped rather than risking a duplicate or a loss"+
						"\n  resolve it at the destination, then set retry.on_indeterminate to retry or dead_letter",
					id, rec.Origin().ID, ferr))

			default:
				return fmt.Errorf("engine: sink %s: %w", id, ferr)
			}
		}

		if len(retry) == 0 {
			return nil
		}
		r.deps.Log.Warn("retrying records a sink did not accept",
			"node", id, "records", len(retry), "wait", wait, "attempt", att(retry[0]).count)
		r.p.obs.waited(id, fault.ClassOf(failed[retry[0].Origin().ID]), wait)
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		pending = retry
	}
	return nil
}

// writeOnce presents one set of records and returns the per-record faults it came back with.
//
// A returned error is a failure of the ENGINE or a broken contract — never a failure of the write,
// which is reported through the map so the caller can route it.
func (r *runner) writeOnce(ctx context.Context, id record.NodeID, sk *registry.ResolvedSink,
	cc *codecChain, pending []*record.Record,
) (map[record.RecordID]error, error) {
	reqs, err := buildRequests(ctx, sk, cc, pending)
	if err != nil {
		// An encode failure belongs to the RECORD, not the sink, so it is routed like any other
		// per-record fault rather than killing the run: a single unencodable value in a batch of
		// five hundred should dead-letter that value, not stop the pipeline.
		failed := make(map[record.RecordID]error, len(pending))
		for _, rec := range pending {
			failed[rec.Origin().ID] = err
		}
		return failed, nil
	}

	failed := map[record.RecordID]error{}
	for _, rq := range reqs {
		res, werr := sandbox(ctx, r.p.obs, id, sk.Name, rq.req,
			func(c context.Context, q *connector.Request) (connector.WriteResult, error) {
				return sk.Sink.Write(c, q)
			})
		if werr != nil {
			// The call failed as a whole, so every record it carried shares its fate — and its
			// class, which is what carries whether the effect may have landed.
			for _, rec := range rq.records {
				failed[rec.Origin().ID] = werr
			}
			continue
		}

		// A sink that under-reports is a sink whose success cannot be trusted, and settling on it
		// would advance the source past records nobody wrote.
		if ok, want := res.Reconcile(rq.req.Count); !ok {
			return nil, fault.Contract(fault.OpWrite, fmt.Errorf(
				"engine: sink %s accounted for %d of %d records; a WriteResult must name every record it did not write",
				id, want, rq.req.Count))
		}
		if len(res.Deferred) > 0 {
			// Deferred is a Flusher's answer. From Write it means the sink is holding records it
			// has not made durable while declaring no way to ever tell us they became so.
			return nil, fault.Contract(fault.OpWrite, fmt.Errorf(
				"engine: sink %s deferred %d records from Write; only a Flusher may defer, and this sink declares none",
				id, len(res.Deferred)))
		}

		named := make(map[record.RecordID]*fault.Fault, len(res.Failed))
		for i := range res.Failed {
			named[res.Failed[i].Record] = res.Failed[i].Fault()
		}
		landed := make([]*record.Record, 0, len(rq.records))
		for _, rec := range rq.records {
			if f, bad := named[rec.Origin().ID]; bad {
				failed[rec.Origin().ID] = f
				continue
			}
			landed = append(landed, rec)
		}
		// A sink that earns its acknowledgement later HOLDS instead of settling. See durability.go.
		r.settleOrHold(id, sk, landed, res)
		if n := len(res.Duplicates); n > 0 {
			r.p.obs.recordsDuplicate.Add(float64(n), r.p.obs.pipeline, string(id))
		}
	}
	return failed, nil
}

// abandon settles a record as terminally not delivered.
//
// The prefix advances past it and the acknowledgement carries a non-zero abandoned count, which is
// what stops a poison record livelocking the lane. It is logged at error level with the class and
// the reason, because a record leaving the pipeline without reaching its destination is the single
// most consequential thing this engine can do quietly.
func (r *runner) abandon(id record.NodeID, rec *record.Record, ferr error, reason string) {
	class := fault.ClassOf(ferr)
	r.deps.Log.Error("a record will not be delivered",
		"node", id, "record", rec.Origin().ID, "lane", rec.Origin().Lane,
		"class", class, "blame", class.Blames(), "reason", reason, "error", ferr)

	r.p.ledger.Settle([]ledger.Outcome{{
		Record:      rec.Origin().ID,
		Node:        id,
		Disposition: ledger.Abandoned,
		Fault:       asFault(ferr),
	}})
	r.p.obs.recordsAbandoned.Add(1, r.p.obs.pipeline, laneLabel(rec.Origin().Lane), reason)
}

// deadLetter writes one record to every sink whose inbound edge carries failed records.
//
// IT DOES NOT RETRY. A dead-letter route that retries has a second retry policy nobody configured,
// and a failure here stops the pipeline rather than being swallowed — which is the conservative
// reading, because the alternative is discarding a record twice over.
func (r *runner) deadLetter(ctx context.Context, rec *record.Record, cause error) error {
	if len(r.failedSinks) == 0 {
		// Build refuses a dead-lettering policy with nowhere to dead-letter, so reaching here means
		// the graph and the policy disagreed after validation. Stopping is the only safe answer.
		return fault.Bug(fault.OpWrite, fmt.Errorf(
			"engine: the retry policy dead-letters but the graph has no failed edge; record %v would be lost",
			rec.Origin().ID))
	}
	for _, id := range r.failedSinks {
		sk := r.p.sinks[id]
		reqs, err := buildRequests(ctx, sk, r.p.codecs[id], []*record.Record{rec})
		if err != nil {
			return fmt.Errorf("engine: encoding record %v for dead-letter sink %s: %w", rec.Origin().ID, id, err)
		}
		for _, rq := range reqs {
			if _, err := sandbox(ctx, r.p.obs, id, sk.Name, rq.req,
				func(c context.Context, q *connector.Request) (connector.WriteResult, error) {
					return sk.Sink.Write(c, q)
				}); err != nil {
				return fmt.Errorf("engine: dead-lettering record %v to %s: %w", rec.Origin().ID, id, err)
			}
		}
	}
	r.deps.Log.Warn("record dead-lettered",
		"record", rec.Origin().ID, "sinks", len(r.failedSinks),
		"reason", telemetry.ReasonTerminalFault, "cause", cause)
	return nil
}

// asFault extracts the classified fault from an error chain, classifying an unclassified one rather
// than dropping it.
//
// An error reaching settlement with no class is a connector defect, and PermanentInternal is what
// the class vocabulary says an unclassified fault becomes. Returning nil instead would leave the
// ledger unable to say why a record was abandoned.
func asFault(err error) *fault.Fault {
	var f *fault.Fault
	if errors.As(err, &f) {
		return f
	}
	return fault.Bug(fault.OpWrite, err)
}

// sleep waits for d, or returns early if ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// partitionSinks splits the sink nodes by what their inbound edges carry.
//
// A node can be in both lists: an edge selecting EdgeAll carries main records and failed ones, and
// a graph is free to send both to one destination. A node in NEITHER has no inbound edge at all,
// which validateGraph refuses, so the case is impossible rather than merely unhandled.
func partitionSinks(s spec.Spec, sinks map[record.NodeID]*registry.ResolvedSink) (main, failed []record.NodeID) {
	for id := range sinks {
		n, ok := s.Node(id)
		if !ok {
			continue
		}
		for _, e := range n.Inputs {
			if e.Select.CarriesMain() {
				main = append(main, id)
				break
			}
		}
		for _, e := range n.Inputs {
			if e.Select.CarriesFailed() {
				failed = append(failed, id)
				break
			}
		}
	}
	// Sorted, because writing to sinks in map order made the ORDER OF SIDE EFFECTS depend on Go's
	// map seed. The same defect already produced a nondeterministic DurabilityEdge once.
	slices.Sort(main)
	slices.Sort(failed)
	return main, failed
}

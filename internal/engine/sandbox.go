package engine

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// sandbox runs one plugin call in a goroutine with recover, and selects on ctx.
//
// A panic becomes fault.PermanentInternal naming the component; a hang lets the host ABANDON the call and
// keep running.
//
// WHY THIS EXISTS AT ALL. A subprocess boundary gives panic containment and hang abandonment for free. An
// in-process boundary that gives neither is not BEHAVIOURALLY the same boundary, and two adapters that
// differ on "a connector panic kills the host" are not interchangeable no matter how identical their
// signatures. Since the out-of-process seam must later satisfy the same interfaces with no core change,
// the in-process path pays this cost now.
//
// THE HONEST COST, accepted and measured: the goroutine leaks until the wedged call returns, and every
// call costs one goroutine. The leak is counted as canal_abandoned_plugin_calls_total, and a non-zero
// value is an alertable condition rather than a footnote.
func sandbox[Req, Res any](ctx context.Context, o *obs, node record.NodeID, name string, req Req,
	fn func(context.Context, Req) (Res, error),
) (Res, error) {
	type result struct {
		res Res
		err error
	}
	// Buffered by one so that an abandoned call's goroutine can finish and exit rather than blocking
	// forever on a send nobody will receive. Abandoning must leak at most a goroutine, never a leak that
	// grows.
	done := make(chan result, 1)

	go func() {
		defer func() {
			if p := recover(); p != nil {
				var zero Res
				done <- result{res: zero, err: fault.Bug(fault.OpUnknown,
					fmt.Errorf("component %q panicked: %v\n%s", name, p, debug.Stack()))}
			}
		}()
		res, err := fn(ctx, req)
		done <- result{res: res, err: err}
	}()

	// A COMPLETED CALL WINS OVER A CANCELLED CONTEXT, and the non-blocking check below is what makes
	// that true.
	//
	// Both cases can be ready at the same instant — a plugin that returned exactly as the context was
	// cancelled — and Go picks at random among ready cases. Taking the ctx branch there reports a
	// call that ALREADY SUCCEEDED as abandoned, so a sink write that landed is never settled, its
	// group stays open forever, and the drain that waits for the ledger to empty times out. That is
	// how it was found: a pipeline cancelled mid-batch failed to commit anything, because one write
	// in five hundred lost the coin toss.
	//
	// Preferring the result costs nothing. The call is over; the only question is whether its answer
	// is thrown away.
	select {
	case r := <-done:
		return r.res, r.err
	default:
	}

	select {
	case r := <-done:
		return r.res, r.err
	case <-ctx.Done():
		// The goroutine above is now leaked until the wedged call returns. Counting it here is what
		// turns the honest cost named in this file's comment into an alertable condition instead of
		// a footnote nobody can act on.
		o.abandonedCall(node)
		var zero Res
		return zero, fault.Internal(fault.OpUnknown,
			fmt.Errorf("component %q did not return before its deadline; the call was abandoned: %w", name, ctx.Err()))
	}
}

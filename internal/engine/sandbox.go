package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// inflight counts the plugin calls the core has outstanding on each node.
//
// IT EXISTS BECAUSE ABANDONING A CALL DOES NOT END IT. sandbox gives up on a wedged call and returns,
// and the goroutine carrying it is still inside the component — that is the documented cost. What was
// not accounted for is that the host then does what the API tells it to and calls Pipeline.Close,
// which enters the SAME component while the abandoned call is still assigning its fields. The race
// detector finds it by sweeping a cancel across the startup window: linefile writes s.f in Open and
// reads it in Close with nothing between them, and every connector in the module has that shape.
//
// A connector author cannot defend against it, because the contract never told them Open and Close
// could overlap — see [connector.Source], which now says the core will not do it. This is how that
// promise is kept: Close settles a node before entering it, and refuses to enter one whose call has
// not come back.
type inflight struct {
	mu sync.Mutex
	n  map[record.NodeID]int

	// idle holds one channel per BUSY node, closed when its count reaches zero. A channel rather than
	// a sync.Cond because the waiter has a deadline, and Cond cannot be waited on with one.
	idle map[record.NodeID]chan struct{}
}

// enter records a call starting on a node.
func (i *inflight) enter(node record.NodeID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.n == nil {
		i.n, i.idle = map[record.NodeID]int{}, map[record.NodeID]chan struct{}{}
	}
	if i.n[node] == 0 {
		i.idle[node] = make(chan struct{})
	}
	i.n[node]++
}

// leave records a call returning, waking anything waiting on the node's last one.
func (i *inflight) leave(node record.NodeID) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.n[node]--
	if i.n[node] > 0 {
		return
	}
	delete(i.n, node)
	if ch := i.idle[node]; ch != nil {
		close(ch)
		delete(i.idle, node)
	}
}

// settle waits until a node has no call outstanding, and reports whether it got there.
//
// A false return is not a failure to be retried: it means a call the core already gave up on is
// STILL RUNNING, so the component cannot be entered at all. The alertable signal for that state is
// canal_abandoned_plugin_calls_total, which is necessarily non-zero whenever this returns false —
// every outstanding call at close time is one sandbox abandoned.
func (i *inflight) settle(ctx context.Context, node record.NodeID) bool {
	i.mu.Lock()
	ch := i.idle[node]
	i.mu.Unlock()
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// errAbandoned marks a call the sandbox gave up waiting for.
//
// IT IS THE ONE ERROR AFTER WHICH THE REQUEST IS NOT THE CALLER'S TO TOUCH. Every other error means
// the component returned and handed its argument back; this one means the component's goroutine is
// still running and still writing to whatever it was given. A caller that reads a batch after it —
// even to ask how many records are in it — races the source's next Add, which is what the race
// detector found the first time the read loop tried to admit a batch before inspecting the error.
var errAbandoned = errors.New("the call was abandoned")

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
func sandbox[Req, Res any](ctx context.Context, p *Pipeline, node record.NodeID, name string, req Req,
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

	// ENTERED BEFORE THE GOROUTINE STARTS, and that ordering is the guarantee rather than a style.
	// sandbox returns the instant ctx is done, and an ALREADY-cancelled context takes that branch
	// before the goroutine has necessarily been scheduled — so counting inside the goroutine would
	// let Close find the node idle and walk into a component the call is about to enter.
	//
	// Leaving is deferred first so it runs last, after the send and after any recover. That half is
	// conservatism, not correctness: fn has already returned by then, so nothing is inside the
	// component either way. See inflight.
	p.inflight.enter(node)

	go func() {
		defer p.inflight.leave(node)
		defer func() {
			if pn := recover(); pn != nil {
				var zero Res
				done <- result{res: zero, err: fault.Bug(fault.OpUnknown,
					fmt.Errorf("component %q panicked: %v\n%s", name, pn, debug.Stack()))}
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
		p.obs.abandonedCall(node)
		var zero Res
		return zero, fault.Internal(fault.OpUnknown,
			fmt.Errorf("component %q did not return before its deadline; %w: %w", name, errAbandoned, ctx.Err()))
	}
}

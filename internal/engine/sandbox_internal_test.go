package engine

import (
	"context"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// AN ABANDONED CALL MUST BE COUNTED BEFORE IT CAN BE MISSED, which is why sandbox enters the node
// before it starts the goroutine rather than inside it.
//
// The hole is narrow and total. sandbox returns the moment ctx is done, and a context that is ALREADY
// done takes that branch immediately — possibly before the goroutine has been scheduled at all. If
// the count were taken inside the goroutine, settle would find the node idle, Close would enter the
// component, and then the goroutine would run and enter it too. Counting first makes the ordering a
// property of the code rather than of the scheduler.
//
// Asserted over many iterations because that is what the injected version needs: entering inside the
// goroutine is not WRONG every time, it is wrong whenever the goroutine has not been scheduled yet.
// Moving the enter into the goroutine fails this every run; the loop is what makes "every run" true.
func TestAnAbandonedCallIsCountedBeforeSandboxCanReturn(t *testing.T) {
	const node = record.NodeID("n")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	release := make(chan struct{})
	defer close(release)

	for i := 0; i < 500; i++ {
		p := &Pipeline{}
		if _, err := sandbox(cancelled, p, node, "wedged", 0,
			func(context.Context, int) (int, error) {
				<-release
				return 0, nil
			}); err == nil {
			t.Fatalf("iteration %d: sandbox returned no error for an already-cancelled context, so "+
				"nothing was abandoned and this test proves nothing", i)
		}

		// The call is outstanding by construction: it cannot return until release closes, which is
		// after this loop. A node that reports itself idle here is one Close would walk straight into.
		//
		// Settled against the ALREADY-CANCELLED context, which turns settle into an instant "is this
		// node busy" query: an idle node returns true before it looks at the deadline, and a busy one
		// has both channels ready and takes the deadline. Five hundred iterations of a real timeout
		// would be five hundred timeouts of waiting to learn nothing.
		if p.inflight.settle(cancelled, node) {
			t.Fatalf("iteration %d: the node reports no calls outstanding while an abandoned call is "+
				"still inside it.\n  Close settles on this answer, so it would enter the component "+
				"alongside the abandoned call — the exact race the counting exists to prevent", i)
		}
	}
}

// settle answers immediately for a node with nothing outstanding, and does not invent a wait.
//
// Close settles EVERY component, and almost none of them ever had a call abandoned. A settle that
// blocked for the grace period on an idle node would turn a clean shutdown of a ten-node graph into
// ten grace periods of waiting.
func TestSettlingAnIdleNodeIsImmediate(t *testing.T) {
	var in inflight

	// Never touched at all.
	if !in.settle(context.Background(), "never-used") {
		t.Error("a node that has never had a call reports as busy")
	}

	// Touched, and finished.
	in.enter("n")
	in.leave("n")
	start := time.Now()
	if !in.settle(context.Background(), "n") {
		t.Error("a node whose call returned reports as busy")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("settling an idle node took %v; it must not wait for a deadline that will not come", took)
	}

	// And the count is per node: one busy node must not hold up another.
	in.enter("busy")
	if !in.settle(context.Background(), "n") {
		t.Error("a busy node made an unrelated idle node report as busy; the count is per node")
	}

	// Nested calls on one node settle only when the last of them leaves.
	in.enter("busy")
	in.leave("busy")
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	if in.settle(ctx, "busy") {
		t.Error("a node with one of two calls still outstanding reported as idle")
	}
	in.leave("busy")
	if !in.settle(context.Background(), "busy") {
		t.Error("a node reports as busy after its last call left")
	}
}

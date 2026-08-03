package engine

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// THE ORDER OF THE TWO CALLS IS THE CORRECTNESS, and this is the seam that makes it assertable.
//
// laneCtl.notify() CLOSES the channel it holds and installs a fresh one. So a channel taken BEFORE the
// table is read is a promise to be woken for anything after that moment, and a channel taken AFTER is
// a promise about a future that has already partly happened: an announce landing in between closes a
// channel nobody holds, and the fresh one the loop waits on is not closed again until the NEXT
// announce. For a source that announces once, from its own goroutine, that lane is never read.
//
// The window is two adjacent statements wide, so no fixture driving a real connector can aim at it —
// TestALaneAnnouncedAfterReadingStartedIsRead stays green with the order reversed, measured, because
// its source announces from inside ReadLanes and the next read produces another notify that recovers
// the lost wake-up. What makes it deterministic is an Assigned that ANNOUNCES AS A SIDE EFFECT, which
// puts the notify exactly between the two calls whichever order they are in.

// announcingWatcher is a lane table whose Assigned announces a lane the first time it is called.
type announcingWatcher struct {
	mu       sync.Mutex
	changes  chan struct{}
	lanes    []connector.LaneAssignment
	reads    int
	announce connector.LaneAssignment
}

func newAnnouncingWatcher(have, late record.LaneID) *announcingWatcher {
	return &announcingWatcher{
		changes:  make(chan struct{}),
		lanes:    []connector.LaneAssignment{{ID: have}},
		announce: connector.LaneAssignment{ID: late},
	}
}

func (w *announcingWatcher) Changes() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.changes
}

// Assigned returns the table as it stands and THEN announces, which is the interleaving under test.
//
// The announce is exactly what laneCtl.notify() does: close the live channel, install a fresh one, and
// have the table report one more lane from here on.
func (w *announcingWatcher) Assigned(context.Context) ([]connector.LaneAssignment, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := append([]connector.LaneAssignment(nil), w.lanes...)
	w.reads++
	if w.reads == 1 {
		w.lanes = append(w.lanes, w.announce)
		close(w.changes)
		w.changes = make(chan struct{})
	}
	return out, nil
}

func testRunnerForWatch() *runner {
	return &runner{deps: Deps{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
}

// A LANE ANNOUNCED BETWEEN THE TWO CALLS IS STILL PICKED UP.
func TestAnAnnounceBetweenTakingTheChannelAndReadingTheTableIsNotLost(t *testing.T) {
	w := newAnnouncingWatcher("early", "late")
	r := testRunnerForWatch()

	type result struct {
		next    []connector.LaneAssignment
		rebuild bool
	}
	got := make(chan result, 1)
	go func() {
		next, rebuild := r.awaitLaneChange(context.Background(), w, "in",
			make(chan struct{}), map[record.LaneID]bool{"early": true})
		got <- result{next, rebuild}
	}()

	select {
	case res := <-got:
		if !res.rebuild {
			t.Fatal("no rebuild was asked for, so the announced lane is read by nobody")
		}
		var names []record.LaneID
		for _, a := range res.next {
			names = append(names, a.ID)
		}
		if len(res.next) != 2 {
			t.Errorf("the rebuild covers %v, want both lanes", names)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("awaitLaneChange is still waiting for a wake-up that will never come.\n" +
			"  The announce closed a channel taken after the table was read, so nothing holds it —\n" +
			"  and the fresh channel this is blocked on is not closed until the NEXT announce")
	}
}

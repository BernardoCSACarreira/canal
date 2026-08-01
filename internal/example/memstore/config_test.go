package memstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

func sp(tenant record.TenantID, id record.PipelineID) spec.Spec {
	return spec.Spec{Tenant: tenant, ID: id}
}

// THE REVISION IS THE STORE'S, NOT THE CALLER'S. store.ConfigStore.Put says the returned revision
// comes from a durable write, which is design rule R13's whole point; the field on the spec is a
// copy stamped on the way in so that a spec read back agrees with the number beside it. Two
// candidate answers to "what revision is this" is how the config watch ends up comparing the wrong
// pair of numbers.
func TestPutStampsTheStoredSpecWithTheRevisionItReturns(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	rev, err := c.Put(ctx, sp("acme", "p1"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev == 0 {
		t.Fatal("Put returned revision 0, which is the value that means 'must not exist'")
	}

	got, gotRev, err := c.Get(ctx, "acme", "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotRev != rev {
		t.Errorf("Get returned revision %d for a spec Put at %d", gotRev, rev)
	}
	if got.Revision != rev {
		t.Errorf("the stored spec's own Revision is %d, not the store's %d; a spec that has left the "+
			"store no longer knows what it is", got.Revision, rev)
	}
}

// The revision is STORE-WIDE, which is what makes Watch(fromRevision) orderable across pipelines. A
// per-key counter would hand two unrelated pipelines the same revision 3 and leave a watcher
// resuming from 3 unable to say which of them it has seen.
func TestTheRevisionIsStoreWideAndMonotonic(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	first, _ := c.Put(ctx, sp("acme", "p1"), 0)
	second, _ := c.Put(ctx, sp("acme", "p2"), 0)
	if second <= first {
		t.Fatalf("two different pipelines got revisions %d and %d; the counter is per-key, not "+
			"store-wide, and a watch cursor cannot order them", first, second)
	}

	third, err := c.Put(ctx, sp("acme", "p1"), first)
	if err != nil {
		t.Fatalf("Put with the matching revision: %v", err)
	}
	if third <= second {
		t.Errorf("updating p1 gave revision %d, which is not past p2's %d", third, second)
	}
}

// Compare-and-set, both directions: zero means must-not-exist and non-zero means must-match.
func TestPutIsCompareAndSet(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	rev, err := c.Put(ctx, sp("acme", "p1"), 0)
	if err != nil {
		t.Fatalf("the first Put: %v", err)
	}
	if _, err := c.Put(ctx, sp("acme", "p1"), 0); err == nil {
		t.Error("a second Put with ifRevision 0 succeeded; zero means the spec must not exist")
	}
	if _, err := c.Put(ctx, sp("acme", "p1"), rev+99); err == nil {
		t.Error("a Put with a revision the store is not at succeeded; that is a lost update")
	}
	if _, err := c.Put(ctx, sp("acme", "p1"), rev); err != nil {
		t.Errorf("a Put with the current revision failed: %v", err)
	}
	if _, err := c.Put(ctx, spec.Spec{ID: "no-tenant"}, 0); err == nil {
		t.Error("a spec with no tenant was stored; it is not addressable by Get")
	}
}

// ErrNoSpec IS WHAT SEPARATES DELETED FROM DOWN. Both reach a caller as a non-nil error and the
// operator response to each is opposite, so it has to survive the fault wrapping an implementation
// puts around it.
func TestAMissingSpecIsErrNoSpecThroughTheFault(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	_, _, err := c.Get(ctx, "acme", "never-stored")
	if !errors.Is(err, store.ErrNoSpec) {
		t.Fatalf("Get for a missing pipeline gave %v; errors.Is must find store.ErrNoSpec through "+
			"the fault, or a caller cannot tell a withdrawn pipeline from an unreachable store", err)
	}

	rev, _ := c.Put(ctx, sp("acme", "p1"), 0)
	if err := c.Delete(ctx, "acme", "p1", rev+1); err == nil {
		t.Error("Delete with a non-matching revision succeeded")
	}
	if err := c.Delete(ctx, "acme", "p1", rev); err != nil {
		t.Fatalf("Delete with the matching revision: %v", err)
	}
	if _, _, err := c.Get(ctx, "acme", "p1"); !errors.Is(err, store.ErrNoSpec) {
		t.Errorf("Get after Delete gave %v, want store.ErrNoSpec", err)
	}
	if err := c.Delete(ctx, "acme", "p1", 0); !errors.Is(err, store.ErrNoSpec) {
		t.Errorf("deleting an absent spec gave %v, want store.ErrNoSpec", err)
	}
}

// A delete has to advance the store revision, or a watcher resuming from what it last saw is handed
// the deletion forever or never.
func TestDeleteAdvancesTheStoreRevision(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	rev, _ := c.Put(ctx, sp("acme", "p1"), 0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, err := c.Watch(ctx, rev)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if err := c.Delete(ctx, "acme", "p1", rev); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	e := recv(t, events)
	if !e.Deleted {
		t.Error("the deletion event is not marked Deleted; a watcher that must diff to find out " +
			"will get it wrong once")
	}
	if e.Revision <= rev {
		t.Errorf("the deletion is at revision %d, not past the Put's %d", e.Revision, rev)
	}
}

// A LATE WATCHER IS CAUGHT UP ON WHAT THE STORE STILL HOLDS, in revision order, and is NOT told
// about deletions it missed — nothing is retained to replay them from, and inventing one would claim
// a pipeline was deleted when it may never have existed. That gap is why the interface says a watch
// is a convenience: the reconcile timer closes it.
//
// THE SIZE OF THIS FIXTURE IS THE TEST. The catch-up ranges a Go map, so an implementation that
// forgets to sort emits whatever order the runtime felt like — and over a handful of entries in one
// bucket that order is a rotation of the insertion order, one of whose rotations IS sorted. The first
// version of this used three pipelines and one watch, and deleting the store's sort failed it three
// times in thirty runs: a test that mostly proves nothing. Twenty entries span several buckets and
// six independent watches compound it, and the same deletion now fails it a hundred times out of a
// hundred.
func TestWatchCatchesUpOnWhatTheStoreStillHolds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := memstore.NewConfig()

	c.Put(ctx, sp("acme", "p0"), 0)
	var want []uint64
	for i := 1; i <= 20; i++ {
		rev, err := c.Put(ctx, sp("acme", record.PipelineID(fmt.Sprintf("p%02d", i))), 0)
		if err != nil {
			t.Fatalf("seeding: %v", err)
		}
		want = append(want, rev)
	}

	// Connect as though each watcher had already seen everything up to p0.
	for w := 0; w < 6; w++ {
		events, err := c.Watch(ctx, want[0]-1)
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		for _, rev := range want {
			if e := recv(t, events); e.Revision != rev {
				t.Fatalf("watch %d caught up with revision %d, want %d; the catch-up must be in "+
					"revision order or a consumer cannot resume from what it last saw", w, e.Revision, rev)
			}
		}
	}

	// And then the live stream, on a fresh watch that drains its catch-up first.
	events, err := c.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	for i := 0; i <= len(want); i++ {
		recv(t, events)
	}
	live, _ := c.Put(ctx, sp("acme", "live"), 0)
	if e := recv(t, events); e.Revision != live {
		t.Errorf("the live event is revision %d, want %d", e.Revision, live)
	}
}

// THE DROP POLICY IS ASSERTED, NOT DESCRIBED. A full buffer discards rather than blocks, because
// blocking would let one wedged watcher stall every Put in the process. A policy nothing can observe
// is indistinguishable from a delivery guarantee that happens to hold on small inputs, which is
// exactly what Dropped exists to prevent.
func TestAFullWatcherLosesEventsRatherThanBlockingThePut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := memstore.NewConfig()

	if _, err := c.Watch(ctx, 0); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Nothing ever reads that channel. Every Put must still return.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			if _, err := c.Put(ctx, sp("acme", record.PipelineID("p"+string(rune('a'+i%26))+string(rune('a'+i/26)))), 0); err != nil {
				// A repeat of an existing pipeline is a CAS failure, which is fine here: the point
				// is that Put RETURNS.
				continue
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Put blocked on a watcher nobody is reading; one wedged consumer must not stall the store")
	}
	if c.Dropped() == 0 {
		t.Error("nothing was reported dropped after 200 events into a 64-deep buffer, so either the " +
			"buffer is unbounded or the counter is not wired: both make the drop policy unassertable")
	}
}

// List is the projection that exists so answering "what pipelines are there" does not deserialise
// every graph in a tenant — and it is scoped to one tenant.
func TestListIsScopedToTheTenantAndOrdered(t *testing.T) {
	ctx := context.Background()
	c := memstore.NewConfig()

	c.Put(ctx, sp("acme", "zeta"), 0)
	c.Put(ctx, sp("acme", "alpha"), 0)
	c.Put(ctx, sp("other", "alpha"), 0)

	got, err := c.List(ctx, "acme")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d specs for acme, want 2; a tenant must not see another's", len(got))
	}
	if got[0].ID != "alpha" || got[1].ID != "zeta" {
		t.Errorf("List returned %s, %s; the order must be stable for a listing to page", got[0].ID, got[1].ID)
	}
	if got[0].Revision == 0 {
		t.Error("the summary carries revision 0, so a listing cannot tell which specs have changed")
	}
}

// A watch closes when its context is done, and closing must not race a concurrent publish.
func TestWatchClosesWhenItsContextIsDone(t *testing.T) {
	c := memstore.NewConfig()
	ctx, cancel := context.WithCancel(context.Background())
	events, err := c.Watch(ctx, 0)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Publishing while the cancel lands is the race worth having a test for: the send and the close
	// both take the store's lock, which is what makes send-on-closed unreachable rather than rare.
	go func() {
		for i := 0; i < 50; i++ {
			c.Put(context.Background(), sp("acme", record.PipelineID("p"+string(rune('a'+i)))), 0)
		}
	}()
	cancel()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the watch channel was never closed after its context was cancelled")
		}
	}
}

func recv(t *testing.T, ch <-chan store.ConfigEvent) store.ConfigEvent {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("the watch channel closed while an event was expected")
		}
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("no watch event arrived")
		return store.ConfigEvent{}
	}
}

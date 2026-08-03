package memstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// THIS STORE DECLARED EpochFencing AND DID NOT IMPLEMENT IT, for as long as it had existed.
//
// Set compared store.Batch.Epoch — the batch's DEFAULT — where the rule is per key. A per-key epoch
// set through PutFenced was ignored in both directions: a stale write for one lane rode in on a
// healthy batch epoch, and a whole batch was judged by a number none of its keys carried.
//
// pkg/store/wal has TestEpochFencing and TestPerKeyEpochIsHonoured, which is exactly why wal was
// correct. This package had no test for its state store at all — only its config store and its
// coordinator — so the same capability was pinned in one implementation and unpinned in the other,
// and the unpinned one was wrong. Every coordinated engine test runs against THIS store, so the
// per-lane fencing the engine writes was going to a fence that was not there.
//
// Kept in this package rather than shared with wal's copy, because a conformance kit for
// store.StateStore does not exist yet and inventing one to hold two tests is the wrong order.

func laneKey(pipeline, lane string) store.Key {
	return store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: []string{pipeline, lane}}
}

// A PER-KEY STALE EPOCH FENCES THE WHOLE BATCH, and the half that was valid must not be applied.
func TestPerKeyEpochIsHonoured(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	b := store.NewBatch(1)
	b.PutFenced(laneKey("p", "lane-a"), []byte("a"), 0, 10)
	b.PutFenced(laneKey("p", "lane-b"), []byte("b"), 0, 20)
	if err := s.Set(ctx, *b); err != nil {
		t.Fatalf("initial multi-lane write: %v", err)
	}

	// lane-a is still held at 10; lane-b's lease moved on to 20 and this worker is behind on it.
	//
	// THE BATCH DEFAULT IS DELIBERATELY HIGHER THAN EVERY PER-KEY EPOCH, and that is what makes this
	// test able to fail. With a default of 1 — the obvious choice, and what the equivalent test in
	// pkg/store/wal uses — the default is below every stored epoch, so a store comparing the BATCH's
	// number refuses the write too and passes for a reason that has nothing to do with per-key
	// fencing. At 100 the batch-level check accepts and only a per-key check refuses, so the
	// assertion below distinguishes the two implementations instead of accepting either.
	b = store.NewBatch(100)
	b.PutFenced(laneKey("p", "lane-a"), []byte("a2"), 1, 10)
	b.PutFenced(laneKey("p", "lane-b"), []byte("b2"), 1, 15)
	if err := s.Set(ctx, *b); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a per-key stale epoch gave %v, want fault.ErrFenced", err)
	}

	got, err := s.Get(ctx, []store.Key{laneKey("p", "lane-a"), laneKey("p", "lane-b")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v := string(got[laneKey("p", "lane-a").String()].Value); v != "a" {
		t.Errorf("the valid half of a fenced batch was applied: lane-a is %q, want %q", v, "a")
	}
	if v := string(got[laneKey("p", "lane-b").String()].Value); v != "b" {
		t.Errorf("the stale half of a fenced batch was applied: lane-b is %q, want %q", v, "b")
	}
}

// THE HIGH-WATER MARK IS PER KEY TOO. Recording the batch's default would raise every key's floor to
// the highest epoch any ONE lane in the batch was held at, which then refuses valid writes for every
// other lane in it — the same defect as the check, in the direction that looks like a fence working.
func TestOneLanesEpochDoesNotRaiseAnothers(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()

	// One batch, two lanes, wildly different epochs.
	b := store.NewBatch(1)
	b.PutFenced(laneKey("p", "low"), []byte("1"), 0, 2)
	b.PutFenced(laneKey("p", "high"), []byte("1"), 0, 99)
	if err := s.Set(ctx, *b); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// The low lane writes again at its own epoch, which is far below the other lane's.
	b = store.NewBatch(1)
	b.PutFenced(laneKey("p", "low"), []byte("2"), 1, 2)
	if err := s.Set(ctx, *b); err != nil {
		t.Fatalf("a valid write at the lane's own epoch was refused: %v; the other lane in the "+
			"first batch raised this key's floor, which is the per-batch bug in the direction that "+
			"looks like the fence working", err)
	}
}

// A LOWER EPOCH IS REFUSED AND AN EQUAL ONE IS NOT, because a lease is renewed at the same epoch and
// every renewal's writes have to keep landing.
func TestTheSameEpochKeepsWritingAndALowerOneDoesNot(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	k := laneKey("p", "lane")

	b := store.NewBatch(1)
	b.PutFenced(k, []byte("1"), 0, 5)
	if err := s.Set(ctx, *b); err != nil {
		t.Fatalf("first write: %v", err)
	}

	b = store.NewBatch(1)
	b.PutFenced(k, []byte("2"), 1, 5)
	if err := s.Set(ctx, *b); err != nil {
		t.Fatalf("a second write at the SAME epoch was refused: %v; a renewed lease keeps its "+
			"epoch, so this is every write after the first renewal", err)
	}

	// Batch default above the stored epoch, so only the per-key comparison can refuse this. See the
	// note in TestPerKeyEpochIsHonoured.
	b = store.NewBatch(100)
	b.PutFenced(k, []byte("3"), 2, 4)
	if err := s.Set(ctx, *b); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a write one epoch behind gave %v, want fault.ErrFenced", err)
	}
}

// THE CAPABILITY IS THE CLAIM, so it is worth asserting the store still makes it: these tests are
// only meaningful against a store that says it fences, and a build that quietly stopped declaring it
// would leave them passing against nothing.
func TestThisStoreStillClaimsToFence(t *testing.T) {
	if !memstore.New().Capabilities().EpochFencing {
		t.Fatal("this store no longer declares EpochFencing; either implement it or delete the " +
			"tests above, because they assert a promise nothing makes")
	}
}

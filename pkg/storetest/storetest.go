// Package storetest is the conformance suite for [store.StateStore].
//
// WHY THIS EXISTS, and it is not a hypothetical. internal/example/memstore declared
// StoreCaps.EpochFencing and did not implement it: its Set compared the batch's DEFAULT epoch where
// the rule is per key, so a per-key epoch set through store.Batch.PutFenced was ignored in both
// directions. Every coordinated engine test runs against that store, which means the fence those
// tests were written to exercise was not there at all.
//
// pkg/store/wal had a test for the same property and it COULD NOT FAIL. Both of its batches used a
// default epoch below every stored epoch, so a store comparing the batch-wide number refused the
// write too and the assertion passed either way — measured: replacing the per-key accessor with the
// batch field in wal's Set left its whole package green. wal was correct by accident of
// implementation, with a test named for the property standing next to it proving nothing. That test
// was then copied into memstore along with the hole.
//
// So: two implementations, two separate proofs of one contract, and both proofs defective in the same
// way. A contract is only real if something INDEPENDENT of the implementation can check it, which is
// what pkg/connectortest already does for connectors and what this does for stores.
//
// WHAT IS IN SCOPE is what the interface promises to every caller: round-trips, compare-and-set,
// batch atomicity, per-key epoch fencing, range order and scoping, and — the one that catches the
// defect above — that every capability a store DECLARES actually holds. What is out of scope is how
// an implementation keeps its bytes: wal's torn-tail, corrupt-byte, compaction and single-open tests
// stay in wal, because they are properties of a file format rather than of the contract.
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// Subject is an implementation under test.
type Subject struct {
	// Name appears in failures, so a shared suite says which store broke.
	Name string

	// New returns a fresh, EMPTY store. It must register its own cleanup.
	New func(t *testing.T) store.StateStore

	// Reopen closes s and returns a store over the SAME underlying state, or nil when the
	// implementation has no such notion.
	//
	// It is optional because "a write survives a restart" is only a meaningful question for a store
	// with somewhere to survive in. An in-memory store answers StoreCaps.FlushIsDurable truthfully —
	// the bytes ARE durable by the time Set returns, in RAM — and has no reopen to offer, which is a
	// difference StoreCaps.Durability already carries.
	Reopen func(t *testing.T, s store.StateStore) store.StateStore
}

const (
	tenant   = record.TenantID("acme")
	pipeline = record.PipelineID("p1")
)

func laneKey(lane record.LaneID) store.Key { return store.LaneKey(tenant, pipeline, lane) }

// Run executes the whole suite against one implementation.
func Run(t *testing.T, s Subject) {
	t.Helper()
	if s.Name == "" || s.New == nil {
		t.Fatal("storetest.Subject needs a Name and a New")
	}
	t.Run(s.Name+"/round_trip", func(t *testing.T) { testRoundTrip(t, s) })
	t.Run(s.Name+"/compare_and_set", func(t *testing.T) { testCAS(t, s) })
	t.Run(s.Name+"/batch_is_atomic", func(t *testing.T) { testAtomic(t, s) })
	t.Run(s.Name+"/epoch_is_per_key", func(t *testing.T) { testEpochPerKey(t, s) })
	t.Run(s.Name+"/one_lanes_epoch_does_not_raise_anothers", func(t *testing.T) { testEpochIsolation(t, s) })
	t.Run(s.Name+"/range_is_ordered_and_scoped", func(t *testing.T) { testRange(t, s) })
	t.Run(s.Name+"/reads_are_copies", func(t *testing.T) { testReadsAreCopies(t, s) })
	t.Run(s.Name+"/deletes_are_fenced", func(t *testing.T) { testFencedDelete(t, s) })
	t.Run(s.Name+"/declared_capabilities_hold", func(t *testing.T) { testDeclaredCapabilities(t, s) })
	t.Run(s.Name+"/survives_reopen", func(t *testing.T) { testReopen(t, s) })
}

// put writes one key at the given expected version, fenced by the batch default.
func put(t *testing.T, s store.StateStore, k store.Key, val string, ifVersion uint64) error {
	t.Helper()
	b := store.NewBatch(1)
	b.Put(k, []byte(val), ifVersion)
	return s.Set(context.Background(), *b)
}

func get(t *testing.T, s store.StateStore, k store.Key) (store.Versioned, bool) {
	t.Helper()
	got, err := s.Get(context.Background(), []store.Key{k})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v, ok := got[k.String()]
	return v, ok
}

func testRoundTrip(t *testing.T, sub Subject) {
	s := sub.New(t)
	k, absent := laneKey("a"), laneKey("nope")

	if _, ok := get(t, s, absent); ok {
		t.Error("a key that was never written came back from Get; absent must be MISSING from the " +
			"result, which is what makes it distinguishable from a key whose value is empty")
	}
	if err := put(t, s, k, "one", 0); err != nil {
		t.Fatalf("writing a new key: %v", err)
	}
	v, ok := get(t, s, k)
	if !ok {
		t.Fatal("the key just written is missing from Get")
	}
	if string(v.Value) != "one" {
		t.Errorf("read back %q, want %q", v.Value, "one")
	}
	if v.Version == 0 {
		t.Error("the store returned version 0 for a key that exists; 0 is the must-not-exist " +
			"precondition, so a live key sharing it makes compare-and-set unusable")
	}

	if err := s.Delete(context.Background(), []store.Key{k}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := get(t, s, k); ok {
		t.Error("the key is still readable after Delete")
	}
}

func testCAS(t *testing.T, sub Subject) {
	s := sub.New(t)
	k := laneKey("a")

	if err := put(t, s, k, "one", 0); err != nil {
		t.Fatalf("writing a new key at version 0: %v", err)
	}
	// 0 means MUST NOT EXIST, so the same write again is a conflict rather than an overwrite.
	if err := put(t, s, k, "again", 0); err == nil {
		t.Error("a second write at version 0 succeeded; 0 is the must-not-exist precondition, and " +
			"accepting it lets two workers both believe they created a lane's row")
	}
	v, _ := get(t, s, k)
	if err := put(t, s, k, "two", v.Version+7); err == nil {
		t.Error("a write at a version the store does not hold succeeded")
	}
	if err := put(t, s, k, "two", v.Version); err != nil {
		t.Fatalf("a write at the current version was refused: %v", err)
	}
	after, _ := get(t, s, k)
	if string(after.Value) != "two" {
		t.Errorf("the value is %q after a valid compare-and-set, want %q", after.Value, "two")
	}
	if after.Version == v.Version {
		t.Error("the version did not change across a successful write, so the next compare-and-set " +
			"cannot tell this value from the previous one")
	}
}

func testAtomic(t *testing.T, sub Subject) {
	s := sub.New(t)
	good, bad := laneKey("good"), laneKey("bad")

	// bad already exists, so a batch demanding version 0 for it must be refused as a whole.
	if err := put(t, s, bad, "existing", 0); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	b := store.NewBatch(1)
	b.Put(good, []byte("new"), 0)
	b.Put(bad, []byte("clobber"), 0)
	if err := s.Set(context.Background(), *b); err == nil {
		t.Fatal("a batch with one impossible precondition was accepted")
	}

	if _, ok := get(t, s, good); ok {
		t.Error("the OTHER key in a refused batch landed anyway.\n" +
			"  A partial write of a checkpoint is unrecoverable: it spans lane cursors, the schema " +
			"epoch and the pending committables, and half of it is a state no recovery can read")
	}
	if v, _ := get(t, s, bad); string(v.Value) != "existing" {
		t.Errorf("the conflicting key is now %q; a refused batch changed it", v.Value)
	}
}

// testEpochPerKey is the assertion memstore failed and wal could not make.
//
// THE BATCH DEFAULT IS DELIBERATELY HIGH. At a default below the stored epochs, a store comparing the
// batch-wide number refuses the write for the same reason a per-key comparison does, and the test
// passes against both the correct and the broken implementation. Above them, only the per-key
// comparison can produce the refusal — which is what the assertion is about.
func testEpochPerKey(t *testing.T, sub Subject) {
	s := sub.New(t)
	if !s.Capabilities().EpochFencing {
		t.Skipf("%s does not declare EpochFencing", sub.Name)
	}
	held, moved := laneKey("held"), laneKey("moved")

	first := store.NewBatch(1)
	first.PutFenced(held, []byte("h1"), 0, 10)
	first.PutFenced(moved, []byte("m1"), 0, 20)
	if err := s.Set(context.Background(), *first); err != nil {
		t.Fatalf("seeding at epochs 10 and 20: %v", err)
	}

	second := store.NewBatch(100)                // above both, so the batch number alone cannot refuse anything
	second.PutFenced(held, []byte("h2"), 1, 10)  // still held at 10
	second.PutFenced(moved, []byte("m2"), 1, 15) // stale: this lane is at 20
	err := s.Set(context.Background(), *second)
	if err == nil {
		t.Fatal("a write fenced at epoch 15 was accepted for a key last written at 20.\n" +
			"  The batch's default was 100, so a store comparing that would have accepted — this is " +
			"the per-key comparison, and it is the whole of what EpochFencing declares")
	}
	if !errors.Is(err, fault.ErrFenced) && fault.ClassOf(err) != fault.Fenced {
		t.Errorf("the refusal was %v, which does not classify as fenced; a caller cannot tell it "+
			"from a store failure and will retry it forever", err)
	}
	if v, _ := get(t, s, held); string(v.Value) != "h1" {
		t.Errorf("the validly-held key is %q, so the refused batch was partially applied", v.Value)
	}
}

func testEpochIsolation(t *testing.T, sub Subject) {
	s := sub.New(t)
	if !s.Capabilities().EpochFencing {
		t.Skipf("%s does not declare EpochFencing", sub.Name)
	}
	low, high := laneKey("low"), laneKey("high")

	// One batch, two lanes, two epochs far apart — the shape store.Versioned.Epoch exists for.
	b := store.NewBatch(1)
	b.PutFenced(low, []byte("l1"), 0, 2)
	b.PutFenced(high, []byte("h1"), 0, 90)
	if err := s.Set(context.Background(), *b); err != nil {
		t.Fatalf("writing two lanes at two epochs: %v", err)
	}

	// The low lane must still be writable at its OWN epoch. A store recording the batch's highest
	// number against every key would have raised this lane's floor to 90 and locked its holder out.
	next := store.NewBatch(1)
	next.PutFenced(low, []byte("l2"), 1, 2)
	if err := s.Set(context.Background(), *next); err != nil {
		t.Errorf("a lane held at epoch 2 can no longer be written at epoch 2: %v\n"+
			"  Another lane in the same batch was at 90, and its epoch has been applied to this key", err)
	}
}

func testRange(t *testing.T, sub Subject) {
	s := sub.New(t)
	ctx := context.Background()

	for _, lane := range []record.LaneID{"c", "a", "b"} {
		if err := put(t, s, laneKey(lane), string(lane), 0); err != nil {
			t.Fatalf("seeding %s: %v", lane, err)
		}
	}
	// A key in another space, which must not appear under the lane prefix.
	if err := put(t, s, store.CheckpointKey(tenant, pipeline), "cp", 0); err != nil {
		t.Fatalf("seeding the checkpoint: %v", err)
	}

	seq, err := s.Range(ctx, store.Key{Tenant: tenant, Space: store.SpaceLane})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	var seen []string
	for k, v := range seq {
		if len(v.Value) == 0 {
			t.Errorf("Range yielded %s with no value", k)
		}
		seen = append(seen, k.String())
	}
	if len(seen) != 3 {
		t.Fatalf("Range yielded %d keys under the lane prefix, want 3: %v\n"+
			"  A prefix that leaks another space makes recovery read rows it cannot decode", len(seen), seen)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] >= seen[i] {
			t.Errorf("Range yielded %q before %q; the contract says key order, and recovery relies "+
				"on it to rebuild a lane table deterministically", seen[i-1], seen[i])
		}
	}
}

func testReadsAreCopies(t *testing.T, sub Subject) {
	s := sub.New(t)
	k := laneKey("a")
	if err := put(t, s, k, "original", 0); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	v, _ := get(t, s, k)
	if len(v.Value) > 0 {
		v.Value[0] = 'X'
	}
	if len(v.Key.Parts) > 0 {
		v.Key.Parts[0] = "hacked"
	}
	again, ok := get(t, s, k)
	if !ok {
		t.Fatal("the key disappeared after a caller mutated what Get returned; the mutated Key.Parts " +
			"changed the identity the store had filed it under")
	}
	if string(again.Value) != "original" {
		t.Errorf("the stored value is %q after a CALLER mutated what Get returned.\n"+
			"  A store handing out its own memory lets any reader corrupt state it does not own, "+
			"and the engine passes those bytes through json.Unmarshal and on to a connector — so the "+
			"code that would have to be trusted not to touch them is third-party",
			again.Value)
	}

	// THE SAME RULE IN THE OTHER DIRECTION. A store that keeps the caller's slice is corrupted by a
	// caller that reuses its buffer after Set — which is the mirror of the bug above, and a store built
	// on live Go values has both or neither.
	written := []byte("as-written")
	b := store.NewBatch(1)
	b.Put(laneKey("mine"), written, 0)
	if err := s.Set(context.Background(), *b); err != nil {
		t.Fatalf("writing: %v", err)
	}
	written[0] = 'X'
	back, _ := get(t, s, laneKey("mine"))
	if string(back.Value) != "as-written" {
		t.Errorf("the stored value is %q after the CALLER mutated the slice it passed to Set; the "+
			"store retained the caller's memory instead of copying it", back.Value)
	}
}

// testDeclaredCapabilities is the check the whole package exists for.
//
// StoreCaps is what Build trusts when it refuses or admits a guarantee tier, so a declaration that is
// not backed by behaviour is worse than a missing feature: the deployment is admitted and the
// protocol it was admitted for does not hold. This runs the behaviour behind each flag and requires it
// only when the flag is set — so a store is free to decline a capability, and not free to claim one.
// A DELETE IS FENCED LIKE A WRITE, and it is the one mutation where that matters most.
//
// Every other fenced operation degrades to a rejected write. This one degrades to destroying state the
// current holder owns and is reading, so a store that honours an epoch on Put and ignores it on Del has
// the fence exactly where it is cheapest and not where it is dearest. It was unaskable until
// store.Deletion carried an epoch: StateStore.Delete takes bare keys, so a store had nothing to compare.
//
// The batch default is HIGH on purpose, for the reason testEpochPerKey gives: below the stored epoch, a
// store comparing the batch-wide number refuses too and the assertion passes either way.
func testFencedDelete(t *testing.T, sub Subject) {
	s := sub.New(t)
	if !s.Capabilities().EpochFencing {
		t.Skipf("%s does not declare EpochFencing", sub.Name)
	}
	ctx := context.Background()
	k := laneKey("owned")

	held := store.NewBatch(1)
	held.PutFenced(k, []byte("the holder's"), 0, 12)
	if err := s.Set(ctx, *held); err != nil {
		t.Fatalf("seeding at epoch 12: %v", err)
	}

	stale := store.NewBatch(100)
	stale.DelFenced(k, 4)
	if err := s.Set(ctx, *stale); err == nil {
		t.Error("a delete fenced at epoch 4 removed a key last written at 12.\n" +
			"  That is a worker whose lease moved on erasing the state its successor is reading, and no " +
			"epoch takes it back because the bytes are gone")
	} else if !errors.Is(err, fault.ErrFenced) && fault.ClassOf(err) != fault.Fenced {
		t.Errorf("the refusal was %v, which does not classify as fenced", err)
	}
	if v, ok := get(t, s, k); !ok || string(v.Value) != "the holder's" {
		t.Errorf("the key is %q (present=%v) after a refused delete", v.Value, ok)
	}

	// The CURRENT holder may of course remove it, or the fence is just a broken feature.
	current := store.NewBatch(1)
	current.DelFenced(k, 12)
	if err := s.Set(ctx, *current); err != nil {
		t.Fatalf("the holder could not delete its own key: %v", err)
	}
	if _, ok := get(t, s, k); ok {
		t.Error("the key survived a delete at its own epoch")
	}

	// AND THE FLOOR OUTLIVES THE KEY, so a stale write cannot resurrect what a current holder removed.
	// The row is gone; the epoch it was removed at is not.
	resurrect := store.NewBatch(100)
	resurrect.PutFenced(k, []byte("stale worker's"), 0, 5)
	if err := s.Set(ctx, *resurrect); err == nil {
		t.Error("a write at epoch 5 recreated a key deleted at 12; dropping the epoch with the row " +
			"lets any superseded worker undo the removal")
	}
}

func testDeclaredCapabilities(t *testing.T, sub Subject) {
	s := sub.New(t)
	caps := s.Capabilities()
	ctx := context.Background()

	if caps.CAS {
		k := laneKey("cas")
		if err := put(t, s, k, "one", 0); err != nil {
			t.Fatalf("CAS is declared but a new key could not be written: %v", err)
		}
		if err := put(t, s, k, "two", 0); err == nil {
			t.Error("CAS is declared and a write at the must-not-exist version overwrote a live key")
		}
	}

	if caps.AtomicMultiKey {
		a, b := laneKey("atomic-a"), laneKey("atomic-b")
		if err := put(t, s, b, "existing", 0); err != nil {
			t.Fatalf("seeding: %v", err)
		}
		batch := store.NewBatch(1)
		batch.Put(a, []byte("new"), 0)
		batch.Put(b, []byte("clobber"), 0)
		if err := s.Set(ctx, *batch); err == nil {
			t.Error("AtomicMultiKey is declared and a batch with an impossible precondition was accepted")
		} else if _, ok := get(t, s, a); ok {
			t.Error("AtomicMultiKey is declared and a refused batch still applied one of its keys")
		}
	}

	if caps.EpochFencing {
		k := laneKey("fenced")
		first := store.NewBatch(1)
		first.PutFenced(k, []byte("new"), 0, 9)
		if err := s.Set(ctx, *first); err != nil {
			t.Fatalf("EpochFencing is declared but a write at epoch 9 failed: %v", err)
		}
		// The default is above 9 on purpose: see testEpochPerKey.
		stale := store.NewBatch(100)
		stale.PutFenced(k, []byte("stale"), 1, 3)
		if err := s.Set(ctx, *stale); err == nil {
			t.Error("EpochFencing is DECLARED AND NOT IMPLEMENTED: a write fenced at epoch 3 landed " +
				"on a key last written at 9.\n" +
				"  StoreCaps is what Build trusts to admit a guarantee tier, so this deployment is " +
				"admitted for a protocol it cannot keep — which is exactly how the in-memory store " +
				"shipped a fence that was not there")
		}
	}

	// NO Supports() LOOP HERE, DELIBERATELY. An earlier version walked every guarantee tier and
	// checked that a refusal carried a reason — which reads like conformance and is not. Supports is
	// shared code in pkg/store with its own tests, including a volatile-caps case, so running it once
	// per subject re-tests the same function N times and cannot fail for anything an IMPLEMENTATION
	// does. What belongs here is only what differs between implementations, which is everything above.
	//
	// The flags are still checked against reality, which is the whole point: caps.Supports reads them,
	// so a flag that lies makes every answer it gives wrong no matter how coherently it is phrased.
}

func testReopen(t *testing.T, sub Subject) {
	if sub.Reopen == nil {
		t.Skipf("%s has no reopen: see Subject.Reopen for why that is a legitimate answer", sub.Name)
	}
	s := sub.New(t)
	k := laneKey("survivor")
	if err := put(t, s, k, "written-before", 0); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	before, _ := get(t, s, k)

	s = sub.Reopen(t, s)

	after, ok := get(t, s, k)
	if !ok {
		t.Fatal("a key written before the store was reopened is gone.\n" +
			"  StoreCaps.FlushIsDurable says Set does not return before the bytes are durable, and " +
			"the commit protocol's phase two rests on it")
	}
	if string(after.Value) != "written-before" {
		t.Errorf("the value is %q after a reopen, want %q", after.Value, "written-before")
	}
	if after.Version != before.Version {
		t.Errorf("the version is %d after a reopen and was %d before; a compare-and-set written "+
			"against the old number would be refused for no reason a caller can see",
			after.Version, before.Version)
	}
}

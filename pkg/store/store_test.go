package store_test

import (
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// pkg/store is the deployment seam — swapping its four interfaces is the entire difference between
// a laptop and a cluster — and it had no test of its own. Two things in it are load-bearing beyond
// their size: StoreCaps.Supports gates every guarantee tier canal will promise, and Key.String is
// the identity a store indexes by, so two different keys rendering alike is silent overwriting.

// A store that is not durable on return cannot support ANY tier. This is the first gate and the
// bluntest one.
func TestSupportsRefusesAStoreThatIsNotDurableOnReturn(t *testing.T) {
	c := store.StoreCaps{CAS: true, EpochFencing: true, AtomicMultiKey: true,
		Durability: connector.DurabilityCluster}
	for _, g := range []connector.Guarantee{
		connector.AtMostOnce, connector.AtLeastOnce,
		connector.EffectivelyOnce, connector.ExactlyOnce,
	} {
		ok, why := c.Supports(g)
		if ok {
			t.Errorf("%s was allowed on a store that is not durable when Set returns", g)
		}
		if why == "" {
			t.Errorf("%s was refused with no reason; the reason becomes a submit-time diagnostic", g)
		}
	}
}

// Without compare-and-set or epoch fencing two workers could both write one lane's progress, which
// is unsafe at every tier rather than only at the strong ones.
func TestSupportsRefusesWithoutCASOrFencing(t *testing.T) {
	base := store.StoreCaps{FlushIsDurable: true, AtomicMultiKey: true,
		Durability: connector.DurabilityCluster, CAS: true, EpochFencing: true}

	noCAS := base
	noCAS.CAS = false
	noFence := base
	noFence.EpochFencing = false

	for name, c := range map[string]store.StoreCaps{"no CAS": noCAS, "no fencing": noFence} {
		if ok, _ := c.Supports(connector.AtLeastOnce); ok {
			t.Errorf("%s: at_least_once was allowed", name)
		}
	}
}

// THE DURABILITY DOMAIN IS A SEPARATE QUESTION FROM FlushIsDurable, and conflating them is what let
// an in-memory map pass exactly_once once already.
//
// The split is deliberate: at-least-once survives a volatile position HONESTLY, because losing it
// means re-reading and re-reading produces duplicates rather than loss. Above at-least-once the
// calculus inverts — the dedupe additions and pending committables ARE the guarantee, and a
// guarantee that does not outlive the process is a claim.
func TestSupportsDrawsTheLineAtAtLeastOnceForAVolatileStore(t *testing.T) {
	volatile := store.StoreCaps{
		FlushIsDurable: true, CAS: true, EpochFencing: true, AtomicMultiKey: true,
		Durability: connector.DurabilityNone,
	}

	for _, g := range []connector.Guarantee{connector.AtMostOnce, connector.AtLeastOnce} {
		if ok, why := volatile.Supports(g); !ok {
			t.Errorf("%s was refused on a volatile store, but re-reading a lost position produces "+
				"duplicates and not loss: %s", g, why)
		}
	}
	for _, g := range []connector.Guarantee{connector.EffectivelyOnce, connector.ExactlyOnce} {
		ok, why := volatile.Supports(g)
		if ok {
			t.Errorf("%s was allowed on a store whose state does not survive a restart", g)
		}
		if why == "" {
			t.Errorf("%s was refused with no reason", g)
		}
	}
}

// A checkpoint torn between its cursors and its committables is unrecoverable, so the strong tiers
// need one atomic write even when the store is perfectly durable.
func TestSupportsRequiresAtomicMultiKeyAboveAtLeastOnce(t *testing.T) {
	c := store.StoreCaps{
		FlushIsDurable: true, CAS: true, EpochFencing: true,
		Durability: connector.DurabilityCluster, AtomicMultiKey: false,
	}
	if ok, _ := c.Supports(connector.AtLeastOnce); !ok {
		t.Error("at_least_once needs no multi-key atomicity and was refused")
	}
	for _, g := range []connector.Guarantee{connector.EffectivelyOnce, connector.ExactlyOnce} {
		if ok, _ := c.Supports(g); ok {
			t.Errorf("%s was allowed on a store that cannot write several keys atomically", g)
		}
	}
}

// The capability set canal's own WAL declares must support everything up to node-durable
// exactly-once, or the store shipped with the product cannot run the product.
func TestAFullyCapableStoreSupportsEverything(t *testing.T) {
	c := store.StoreCaps{
		FlushIsDurable: true, CAS: true, EpochFencing: true, AtomicMultiKey: true,
		Durability: connector.DurabilityNode,
	}
	for _, g := range []connector.Guarantee{
		connector.AtMostOnce, connector.AtLeastOnce,
		connector.EffectivelyOnce, connector.ExactlyOnce,
	} {
		if ok, why := c.Supports(g); !ok {
			t.Errorf("%s was refused by a fully capable store: %s", g, why)
		}
	}
}

// --- keys -------------------------------------------------------------------------------------

// TWO DIFFERENT KEYS MUST NOT RENDER ALIKE. Key.String is the identity a store indexes by — the WAL
// keys its map on it and store.Batch keys its writes on it — so a collision is one key silently
// overwriting another, across pipelines inside one tenant.
func TestDistinctKeysRenderDistinctly(t *testing.T) {
	keys := []store.Key{
		store.ConnectorKey("acme", "a", "b/c", "lane"),
		store.ConnectorKey("acme", "a/b", "c", "lane"),
		store.LaneKey("acme", "p", "lane-1"),
		store.LaneKey("acme", "p", "lane-2"),
		store.LaneKey("acme", "p2", "lane-1"),
		store.CheckpointKey("acme", "p"),
		store.CheckpointKey("acme", "p2"),
		store.ConnectorNodeKey("acme", "p", "n"),
	}

	seen := map[string]store.Key{}
	for _, k := range keys {
		s := k.String()
		if prev, dup := seen[s]; dup {
			t.Errorf("two different keys render as %q:\n  %#v\n  %#v\n"+
				"a store indexes on this string, so one silently overwrites the other", s, prev, k)
			continue
		}
		seen[s] = k
	}
}

// Prefix is what Range scans on, so a key that is NOT under a prefix must not be swept up by it.
func TestPrefixMatchesOnlyWhatItContains(t *testing.T) {
	inside := store.LaneKey("acme", "p", "lane-1")
	prefix := store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: []string{"p"}}

	if !inside.Prefix(prefix) {
		t.Error("a lane of pipeline p is not under the prefix for pipeline p")
	}
	for name, k := range map[string]store.Key{
		"another tenant":   store.LaneKey("other", "p", "lane-1"),
		"another pipeline": store.LaneKey("acme", "p2", "lane-1"),
		"another space":    store.CheckpointKey("acme", "p"),
	} {
		if k.Prefix(prefix) {
			t.Errorf("%s matched the prefix; Range would sweep up somebody else's rows", name)
		}
	}
	// A prefix longer than the key cannot contain it.
	if prefix.Prefix(inside) {
		t.Error("a shorter key reported itself under a longer prefix")
	}
}

// --- batches ----------------------------------------------------------------------------------

// Writes is keyed by Key.String so that two writes to one key in one batch are impossible BY
// CONSTRUCTION — the later one replaces the earlier rather than both landing in an undefined order.
func TestABatchCannotWriteOneKeyTwice(t *testing.T) {
	b := store.NewBatch(1)
	k := store.LaneKey("acme", "p", "lane-1")

	b.Put(k, []byte("first"), 0)
	b.Put(k, []byte("second"), 3)

	if b.Len() != 1 {
		t.Fatalf("the batch holds %d writes, want 1", b.Len())
	}
	got := b.Writes[k.String()]
	if string(got.Value) != "second" || got.IfVersion != 3 {
		t.Errorf("the later write did not replace the earlier: %+v", got)
	}
}

// PutFenced carries its OWN epoch, for a key whose lease is not the batch's. Without it a
// multi-lane atomic write is fenced by one epoch for lanes held under several.
func TestPutFencedCarriesItsOwnEpoch(t *testing.T) {
	b := store.NewBatch(7)
	plain := store.LaneKey("acme", "p", "a")
	fenced := store.LaneKey("acme", "p", "b")

	b.Put(plain, []byte("x"), 0)
	b.PutFenced(fenced, []byte("y"), 0, 9)

	if e := b.Writes[plain.String()].Epoch; e != 0 {
		t.Errorf("a plain Put carries epoch %d; it should defer to the batch's", e)
	}
	if e := b.Writes[fenced.String()].Epoch; e != 9 {
		t.Errorf("a fenced Put carries epoch %d, want 9", e)
	}
	if b.Epoch != 7 {
		t.Errorf("the batch's default epoch is %d, want 7", b.Epoch)
	}
}

func TestBatchLenCountsDeletes(t *testing.T) {
	b := store.NewBatch(1)
	if b.Len() != 0 {
		t.Errorf("a new batch has length %d", b.Len())
	}
	b.Put(store.LaneKey("acme", "p", "a"), []byte("x"), 0)
	b.Del(store.LaneKey("acme", "p", "b"))
	if b.Len() != 2 {
		t.Errorf("length is %d, want 2: a delete is a mutation too", b.Len())
	}
}

// A zero-value Batch must be usable, because a caller constructing one as a literal is reachable
// and a nil map panics on assignment.
func TestAZeroBatchIsUsable(t *testing.T) {
	var b store.Batch
	b.Put(store.LaneKey("acme", "p", "a"), []byte("x"), 0)
	if b.Len() != 1 {
		t.Errorf("a zero-value batch did not accept a write: length %d", b.Len())
	}
}

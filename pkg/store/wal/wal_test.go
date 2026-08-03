package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/storetest"
)

func open(t *testing.T, dir string) *StateStore {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func key(parts ...string) store.Key {
	return store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: parts}
}

func put(t *testing.T, s *StateStore, k store.Key, val string, ifVersion uint64) {
	t.Helper()
	b := store.NewBatch(1)
	b.Put(k, []byte(val), ifVersion)
	if err := s.Set(context.Background(), *b); err != nil {
		t.Fatalf("Set %s: %v", k, err)
	}
}

func get(t *testing.T, s *StateStore, k store.Key) (store.Versioned, bool) {
	t.Helper()
	m, err := s.Get(context.Background(), []store.Key{k})
	if err != nil {
		t.Fatalf("Get %s: %v", k, err)
	}
	v, ok := m[k.String()]
	return v, ok
}

// --- the contract ------------------------------------------------------------

func TestPutGetDelete(t *testing.T) {
	s := open(t, t.TempDir())

	if _, ok := get(t, s, key("p", "l1")); ok {
		t.Fatal("an empty store returned a value")
	}

	put(t, s, key("p", "l1"), "one", 0)
	v, ok := get(t, s, key("p", "l1"))
	if !ok || string(v.Value) != "one" || v.Version != 1 {
		t.Fatalf("got %q v%d, want \"one\" v1", v.Value, v.Version)
	}

	put(t, s, key("p", "l1"), "two", 1)
	v, _ = get(t, s, key("p", "l1"))
	if string(v.Value) != "two" || v.Version != 2 {
		t.Fatalf("got %q v%d, want \"two\" v2", v.Value, v.Version)
	}

	if err := s.Delete(context.Background(), []store.Key{key("p", "l1")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := get(t, s, key("p", "l1")); ok {
		t.Fatal("the key survived a delete")
	}
}

func TestCompareAndSet(t *testing.T) {
	s := open(t, t.TempDir())
	put(t, s, key("p", "l1"), "one", 0)

	b := store.NewBatch(1)
	b.Put(key("p", "l1"), []byte("again"), 0) // 0 means "must not exist"
	if err := s.Set(context.Background(), *b); err == nil {
		t.Error("IfVersion 0 was accepted against an existing key")
	}

	b = store.NewBatch(1)
	b.Put(key("p", "l1"), []byte("stale"), 99)
	if err := s.Set(context.Background(), *b); err == nil {
		t.Error("a stale IfVersion was accepted")
	}

	v, _ := get(t, s, key("p", "l1"))
	if string(v.Value) != "one" {
		t.Errorf("a rejected write changed the value to %q", v.Value)
	}
}

// TestBatchIsAllOrNothing is the property every tier above at-least-once rests on.
func TestBatchIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	put(t, s, key("p", "a"), "a1", 0)

	// One good write and one whose precondition cannot hold.
	b := store.NewBatch(1)
	b.Put(key("p", "a"), []byte("a2"), 1)
	b.Put(key("p", "b"), []byte("b1"), 77) // b does not exist, so version 77 is impossible
	if err := s.Set(context.Background(), *b); err == nil {
		t.Fatal("a batch with an impossible precondition was accepted")
	}

	if v, _ := get(t, s, key("p", "a")); string(v.Value) != "a1" {
		t.Errorf("the good half of a rejected batch was applied: %q", v.Value)
	}
	if _, ok := get(t, s, key("p", "b")); ok {
		t.Error("the failing half of a rejected batch was applied")
	}

	// And nothing reached the log either.
	_ = s.Close()
	s2 := open(t, dir)
	if v, _ := get(t, s2, key("p", "a")); string(v.Value) != "a1" {
		t.Errorf("after reopen the rejected batch is visible: %q", v.Value)
	}
}

func TestEpochFencing(t *testing.T) {
	s := open(t, t.TempDir())

	b := store.NewBatch(5)
	b.Put(key("p", "l1"), []byte("at five"), 0)
	if err := s.Set(context.Background(), *b); err != nil {
		t.Fatalf("epoch 5: %v", err)
	}

	b = store.NewBatch(4) // a worker whose lease was reclaimed
	b.Put(key("p", "l1"), []byte("from the past"), 1)
	err := s.Set(context.Background(), *b)
	if !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a stale epoch gave %v, want fault.ErrFenced", err)
	}
	if v, _ := get(t, s, key("p", "l1")); string(v.Value) != "at five" {
		t.Errorf("a fenced write landed: %q", v.Value)
	}
}

// TestPerKeyEpochIsHonoured covers the case a batch-wide epoch cannot express.
//
// A worker holding 32 lanes holds 32 leases at 32 epochs. With only a batch epoch it has one number
// to offer: too high and a fenced worker's write is accepted for lanes it has lost, too low and a
// valid write is refused for lanes it holds. store.Batch.EpochFor exists for exactly this, and the
// in-memory example store does not call it — it reads w.Epoch directly, so PutFenced is ignored
// there and a multi-lane atomic write is unfenced.
func TestPerKeyEpochIsHonoured(t *testing.T) {
	s := open(t, t.TempDir())

	b := store.NewBatch(1)
	b.PutFenced(key("p", "lane-a"), []byte("a"), 0, 10)
	b.PutFenced(key("p", "lane-b"), []byte("b"), 0, 20)
	if err := s.Set(context.Background(), *b); err != nil {
		t.Fatalf("initial multi-lane write: %v", err)
	}

	// lane-a is still held at 10; lane-b's lease moved on and this worker is behind on it.
	//
	// THE BATCH DEFAULT IS 100 SO THAT THIS TEST CAN FAIL. It was 1, which is below every stored
	// epoch — so a store comparing the BATCH's number rather than each key's refused the write too,
	// and this test passed either way. Measured: replacing EpochFor(v) with w.Epoch in Set left the
	// whole package green. A sibling store made exactly that mistake and shipped it, because its
	// equivalent test was copied from this one along with the hole.
	//
	// At 100 the batch-level comparison accepts and only the per-key one refuses, which is the
	// difference the test's own name claims to be about.
	b = store.NewBatch(100)
	b.PutFenced(key("p", "lane-a"), []byte("a2"), 1, 10)
	b.PutFenced(key("p", "lane-b"), []byte("b2"), 1, 15) // stale: lane-b is at 20
	if err := s.Set(context.Background(), *b); !errors.Is(err, fault.ErrFenced) {
		t.Fatalf("a per-key stale epoch gave %v, want fault.ErrFenced", err)
	}
	if v, _ := get(t, s, key("p", "lane-a")); string(v.Value) != "a" {
		t.Errorf("the valid half of a fenced batch was applied: %q", v.Value)
	}
}

func TestRangeIsOrderedAndScoped(t *testing.T) {
	s := open(t, t.TempDir())
	put(t, s, key("p", "c"), "3", 0)
	put(t, s, key("p", "a"), "1", 0)
	put(t, s, key("p", "b"), "2", 0)
	put(t, s, store.Key{Tenant: "other", Space: store.SpaceLane, Parts: []string{"p", "a"}}, "x", 0)
	put(t, s, store.Key{Tenant: "acme", Space: store.SpaceDedupe, Parts: []string{"p", "a"}}, "y", 0)

	seq, err := s.Range(context.Background(), store.Key{Tenant: "acme", Space: store.SpaceLane, Parts: []string{"p"}})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	var got []string
	for _, v := range seq {
		got = append(got, string(v.Value))
	}
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("Range gave %v, want [1 2 3] — ordered, and neither the other tenant nor the other space", got)
	}
}

// TestReadsAreCopies is design rule R13's "return values, not pointers into state".
func TestReadsAreCopies(t *testing.T) {
	s := open(t, t.TempDir())
	put(t, s, key("p", "l1"), "original", 0)

	v, _ := get(t, s, key("p", "l1"))
	v.Value[0] = 'X'
	v.Key.Parts[0] = "hacked"

	again, _ := get(t, s, key("p", "l1"))
	if string(again.Value) != "original" {
		t.Errorf("mutating a returned value changed the store: %q", again.Value)
	}
	if again.Key.Parts[0] != "p" {
		t.Errorf("mutating a returned key changed the store: %v", again.Key.Parts)
	}
}

func TestCapabilitiesSupportExactlyOnce(t *testing.T) {
	s := open(t, t.TempDir())
	c := s.Capabilities()
	if c.Durability != connector.DurabilityNode {
		t.Errorf("Durability is %s, want node", c.Durability)
	}
	if ok, why := c.Supports(connector.ExactlyOnce); !ok {
		t.Errorf("a durable store cannot back exactly_once: %s", why)
	}
}

// --- durability ---------------------------------------------------------------

func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	for i := 0; i < 50; i++ {
		put(t, s, key("p", fmt.Sprintf("lane-%02d", i)), fmt.Sprintf("value-%d", i), 0)
	}
	put(t, s, key("p", "lane-00"), "overwritten", 1)
	if err := s.Delete(context.Background(), []store.Key{key("p", "lane-01")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := open(t, dir)
	if v, _ := get(t, s2, key("p", "lane-00")); string(v.Value) != "overwritten" || v.Version != 2 {
		t.Errorf("lane-00 came back as %q v%d", v.Value, v.Version)
	}
	if _, ok := get(t, s2, key("p", "lane-01")); ok {
		t.Error("a deleted key came back")
	}
	if v, _ := get(t, s2, key("p", "lane-49")); string(v.Value) != "value-49" {
		t.Errorf("lane-49 came back as %q", v.Value)
	}
}

// TestTornTailAtEveryOffset is the test that justifies hand-rolling this.
//
// A process killed mid-append leaves a partial frame. Every byte offset is a possible kill point, so
// every byte offset is tried: the log is truncated there, reopened, and the result must be a PREFIX
// of what was committed — never an error, never a value that was never written, never a value older
// than one the store had already acknowledged behind an fsync.
//
// The prefix property is the real assertion. "It opens" would pass for a store that silently
// discarded everything.
func TestTornTailAtEveryOffset(t *testing.T) {
	// Build a reference log and record what was true after each committed batch.
	golden := t.TempDir()
	s := open(t, golden)
	const n = 12
	for i := 0; i < n; i++ {
		put(t, s, key("p", fmt.Sprintf("k%02d", i)), fmt.Sprintf("v%02d", i), 0)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	full, err := os.ReadFile(filepath.Join(golden, logName))
	if err != nil {
		t.Fatalf("reading the reference log: %v", err)
	}

	for cut := 0; cut <= len(full); cut++ {
		t.Run(fmt.Sprintf("cut-%03d", cut), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, logName), full[:cut], 0o644); err != nil {
				t.Fatalf("writing the truncated log: %v", err)
			}

			s, err := Open(dir)
			if err != nil {
				// The only acceptable failure is a file too short to hold the header, which is not a
				// torn frame — it is a file that was never a log.
				if cut < headerLen {
					return
				}
				t.Fatalf("Open after truncation at %d: %v", cut, err)
			}
			defer s.Close()

			// Whatever survived must be a prefix: some k00..kX present, contiguous, with the right
			// values, and nothing beyond.
			seen := 0
			for i := 0; i < n; i++ {
				v, ok := get(t, s, key("p", fmt.Sprintf("k%02d", i)))
				if !ok {
					break
				}
				if want := fmt.Sprintf("v%02d", i); string(v.Value) != want {
					t.Fatalf("k%02d is %q, want %q", i, v.Value, want)
				}
				seen++
			}
			for i := seen; i < n; i++ {
				if _, ok := get(t, s, key("p", fmt.Sprintf("k%02d", i))); ok {
					t.Fatalf("k%02d exists although k%02d does not: recovery is not a prefix", i, seen)
				}
			}

			// And the store must still be WRITABLE afterwards: recovery that leaves the file
			// unaligned would corrupt the next append rather than fail it.
			put(t, s, key("p", "after"), "recovered", 0)
			if v, _ := get(t, s, key("p", "after")); string(v.Value) != "recovered" {
				t.Fatal("the store is not writable after recovery")
			}
		})
	}
}

// TestCorruptByteAtEveryOffset is the other half: not a short file, but a wrong one.
//
// Bit rot and a partially-overwritten sector both produce a full-length frame with wrong contents.
// The CRC must catch it, and recovery must again yield a prefix rather than a value that was never
// written — silently accepting a corrupted checkpoint is far worse than losing it.
func TestCorruptByteAtEveryOffset(t *testing.T) {
	golden := t.TempDir()
	s := open(t, golden)
	const n = 6
	for i := 0; i < n; i++ {
		put(t, s, key("p", fmt.Sprintf("k%02d", i)), fmt.Sprintf("v%02d", i), 0)
	}
	_ = s.Close()
	full, err := os.ReadFile(filepath.Join(golden, logName))
	if err != nil {
		t.Fatalf("reading the reference log: %v", err)
	}

	for pos := headerLen; pos < len(full); pos++ {
		corrupt := make([]byte, len(full))
		copy(corrupt, full)
		corrupt[pos] ^= 0xFF

		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, logName), corrupt, 0o644); err != nil {
			t.Fatalf("writing the corrupted log: %v", err)
		}

		s, err := Open(dir)
		if err != nil {
			// A frame whose CRC matches but which does not parse is reported rather than dropped.
			// That is deliberate — see replay — and it is an acceptable outcome here.
			continue
		}

		seen := 0
		for i := 0; i < n; i++ {
			v, ok := get(t, s, key("p", fmt.Sprintf("k%02d", i)))
			if !ok {
				break
			}
			if want := fmt.Sprintf("v%02d", i); string(v.Value) != want {
				t.Errorf("byte %d corrupted: k%02d read back as %q, want %q — a corrupt frame was accepted",
					pos, i, v.Value, want)
			}
			seen++
		}
		for i := seen; i < n; i++ {
			if _, ok := get(t, s, key("p", fmt.Sprintf("k%02d", i))); ok {
				t.Errorf("byte %d corrupted: k%02d exists although k%02d does not", pos, i, seen)
			}
		}
		_ = s.Close()
	}
}

// --- operational --------------------------------------------------------------

func TestCompactionPreservesState(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	// Overwrite a small key set until the log is mostly dead weight, then force the rewrite.
	val := make([]byte, 4096)
	for i := range val {
		val[i] = 'x'
	}
	for round := uint64(1); round <= 400; round++ {
		b := store.NewBatch(1)
		for k := 0; k < 3; k++ {
			b.Put(key("p", fmt.Sprintf("hot-%d", k)), append(val, byte(round)), ifFirst(round))
		}
		if err := s.Set(context.Background(), *b); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}

	before := s.bytes
	s.mu.Lock()
	err := s.compactLocked()
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if s.bytes >= before {
		t.Errorf("compaction grew the log: %d -> %d", before, s.bytes)
	}

	// The live set must be intact, in memory and on disk.
	check := func(s *StateStore, when string) {
		for k := 0; k < 3; k++ {
			v, ok := get(t, s, key("p", fmt.Sprintf("hot-%d", k)))
			if !ok {
				t.Fatalf("%s: hot-%d vanished", when, k)
			}
			if v.Version != 400 {
				t.Errorf("%s: hot-%d is at version %d, want 400", when, k, v.Version)
			}
		}
	}
	check(s, "after compaction")
	_ = s.Close()
	check(open(t, dir), "after reopen")
}

// ifFirst returns the IfVersion for a given round: 0 on the first write, round-1 after.
func ifFirst(round uint64) uint64 {
	if round == 1 {
		return 0
	}
	return round - 1
}

func TestSecondOpenIsRefused(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	_ = s

	if _, err := Open(dir); err == nil {
		t.Fatal("a second process opened the same store; concurrent writers would corrupt it")
	}
}

func TestOpenRejectsAForeignFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, logName), []byte("this is somebody else's file entirely"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open accepted a file that is not a canal log; it would have overwritten it")
	}
}

func TestOpenRejectsANewerFormat(t *testing.T) {
	dir := t.TempDir()
	hdr := append([]byte(magic), 0x00, 0xFF) // version 255
	if err := os.WriteFile(filepath.Join(dir, logName), hdr, 0o644); err != nil {
		t.Fatal(err)
	}
	// ADR 0020: an older binary must refuse a newer format rather than reinterpret it.
	if _, err := Open(dir); err == nil {
		t.Fatal("Open accepted a format version it does not understand")
	}
}

// TestCapabilitiesNeedNoOpenStore pins [Caps] to the method that reports it.
//
// The two exist separately so a tool can ask what a WAL-backed deployment would negotiate without
// opening — and therefore exclusively locking — a directory a running pipeline may hold. That is
// only safe while they answer identically, and a capability set that drifts between the two would
// have `canal check` reporting a contract `canal run` does not honour.
func TestCapabilitiesNeedNoOpenStore(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	defer s.Close()

	if got, want := s.Capabilities(), Caps(); got != want {
		t.Errorf("an opened store reports %+v but Caps reports %+v", got, want)
	}
}

// THE CONTRACT IS CHECKED BY THE CONTRACT'S OWN SUITE, not only by tests that live next to this
// implementation.
//
// Everything above stays because it is about this store's FILE FORMAT — torn tails, a corrupt byte at
// every offset, compaction, a second open, a foreign file. Those are wal's properties and belong here.
// What the suite takes over is what every StateStore owes its callers, and TestPerKeyEpochIsHonoured
// above is the reason: it could not fail until the batch default was raised, and a sibling store
// inherited that hole when the test was copied. A shared suite is the only version of this check that
// cannot be copied wrong twice.
func TestConformance(t *testing.T) {
	// The directory is remembered HERE rather than exposed as an accessor on the store. Reopen needs to
	// know where the bytes are, and adding an exported Dir() to production code so that a test can call
	// it is the wrong direction — nothing else wants it.
	var mu sync.Mutex
	dirs := map[*StateStore]string{}

	storetest.Run(t, storetest.Subject{
		Name: "wal",
		New: func(t *testing.T) store.StateStore {
			dir := t.TempDir()
			s := open(t, dir)
			mu.Lock()
			dirs[s] = dir
			mu.Unlock()
			t.Cleanup(func() { s.Close() })
			return s
		},
		// A reopen over the SAME directory, which is what makes "the bytes were durable when Set
		// returned" an assertable claim rather than a declared one.
		Reopen: func(t *testing.T, s store.StateStore) store.StateStore {
			w, ok := s.(*StateStore)
			if !ok {
				t.Fatalf("the subject is %T, not this package's store", s)
			}
			mu.Lock()
			dir, known := dirs[w]
			mu.Unlock()
			if !known {
				t.Fatal("this store was not created by the New above, so its directory is unknown")
			}
			if err := w.Close(); err != nil {
				t.Fatalf("closing before reopen: %v", err)
			}
			return open(t, dir)
		},
	})
}

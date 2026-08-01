package record_test

import (
	"math"
	"testing"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// pkg/record is where settlement identity comes from, and it had no test of its own. Every
// guarantee in canal rests on a record id being unique within a generation, on a derived record
// keeping its root, and on a position comparison degrading rather than guessing — and all three
// were asserted only through the engine, which tests the engine's use of them.

func alloc() *record.Allocator {
	return record.NewAllocator("acme", "p1", "n1", "lane-1", "events", 1, 1)
}

// TWO RECORDS SHARING AN ID makes "both branches settled" indistinguishable from "one branch
// double-settled", which is a group resolving early and a cursor committed past unwritten data.
// It is the single most consequential invariant in this package.
func TestEveryRecordGetsItsOwnID(t *testing.T) {
	a := alloc()
	seen := map[record.RecordID]bool{}

	for batchNo := 0; batchNo < 20; batchNo++ {
		b := record.NewBatch(a, 50)
		for i := 0; i < 50; i++ {
			r := b.Add()
			if r == nil {
				t.Fatalf("batch %d: Add returned nil below the cap", batchNo)
			}
			id := r.Origin().ID
			if seen[id] {
				t.Fatalf("record id %d was issued twice", id)
			}
			seen[id] = true
		}
		// Derived records must be fresh too: there is deliberately no Copy that preserves an id.
		for _, parent := range b.Records[:5] {
			d := b.Derive(parent)
			if d == nil {
				continue
			}
			if seen[d.Origin().ID] {
				t.Fatalf("a derived record reused id %d", d.Origin().ID)
			}
			seen[d.Origin().ID] = true
		}
	}
	if len(seen) < 1000 {
		t.Fatalf("only %d ids were issued; the test is not exercising what it claims", len(seen))
	}
}

// The cap is a hard bound, and Add returns nil at it rather than growing. A source that ignores the
// nil gets a panic in its own code, which is the correct place for that mistake to surface.
func TestAddStopsAtTheCap(t *testing.T) {
	b := record.NewBatch(alloc(), 3)
	for i := 0; i < 3; i++ {
		if b.Add() == nil {
			t.Fatalf("Add returned nil at %d, below the cap of 3", i)
		}
	}
	if b.Add() != nil {
		t.Error("Add went past the cap")
	}
	if b.Len() != 3 {
		t.Errorf("the batch holds %d records, want 3", b.Len())
	}
	if b.Cap() != 3 {
		t.Errorf("Cap is %d, want 3", b.Cap())
	}
}

// A nil allocator must not panic. NewBatch(nil, n) is reachable from a connector that mishandles the
// batch it was handed, and a panic there surfaces inside the core rather than at the mistake.
func TestABatchWithNoAllocatorRefusesRatherThanPanics(t *testing.T) {
	b := record.NewBatch(nil, 4)
	if b.Add() != nil {
		t.Error("Add produced a record with no allocator")
	}
	if b.AddFor("other") != nil {
		t.Error("AddFor produced a record with no allocator")
	}
	if b.Derive(nil) != nil {
		t.Error("Derive produced a record from nil")
	}
}

// One lane serving several streams is the NORMAL shape of a shared log — one binlog coordinate
// interleaving many tables — and AddFor is what makes it expressible without one lane per table.
func TestAddForStampsTheGivenStream(t *testing.T) {
	b := record.NewBatch(alloc(), 4)

	def := b.Add()
	if def.Origin().Stream != "events" {
		t.Errorf("Add stamped stream %q, want the allocator's", def.Origin().Stream)
	}
	other := b.AddFor("orders")
	if other.Origin().Stream != "orders" {
		t.Errorf("AddFor stamped stream %q, want orders", other.Origin().Stream)
	}
	if other.Dest != "orders" {
		t.Errorf("Dest is %q, want orders", other.Dest)
	}
	// Both share the batch's settlement group: one lane, one group, several streams.
	if def.Origin().Group != other.Origin().Group {
		t.Error("two records in one batch landed in different settlement groups")
	}
}

// A derived record keeps its ROOT and names its PARENT, which is what makes a one-to-many
// expansion's settlement attributable back to the record that caused it.
func TestDerivePreservesTheRoot(t *testing.T) {
	b := record.NewBatch(alloc(), 10)
	root := b.Add()

	child := b.Derive(root)
	if child == nil {
		t.Fatal("Derive returned nil below the cap")
	}
	if child.Origin().Parent != root.Origin().ID {
		t.Errorf("parent is %d, want %d", child.Origin().Parent, root.Origin().ID)
	}
	if child.Origin().Root != root.Origin().ID {
		t.Errorf("root is %d, want %d", child.Origin().Root, root.Origin().ID)
	}

	// A grandchild keeps the ORIGINAL root rather than adopting its parent as one.
	grand := b.Derive(child)
	if grand.Origin().Root != root.Origin().ID {
		t.Errorf("the grandchild's root is %d, want the original %d",
			grand.Origin().Root, root.Origin().ID)
	}
	if grand.Origin().Parent != child.Origin().ID {
		t.Errorf("the grandchild's parent is %d, want %d", grand.Origin().Parent, child.Origin().ID)
	}
	// Identity is fresh at every materialisation.
	if grand.Origin().ID == child.Origin().ID || child.Origin().ID == root.Origin().ID {
		t.Error("a derivation reused an id")
	}
}

func TestResetClearsTheBatchWithoutLosingItsIdentity(t *testing.T) {
	b := record.NewBatch(alloc(), 8)
	for i := 0; i < 4; i++ {
		b.Add()
	}
	b.Position = record.Position{Seq: 9}
	b.EndOfLane = true

	b.Reset()
	if b.Len() != 0 {
		t.Errorf("Reset left %d records", b.Len())
	}
	if b.EndOfLane {
		t.Error("Reset left EndOfLane set; a reused batch would claim the lane ended")
	}
	if b.Lane != "lane-1" {
		t.Errorf("Reset changed the lane to %q; a source must never retarget it", b.Lane)
	}
	// It is still usable, which is the point of reusing it.
	if b.Add() == nil {
		t.Error("the batch is unusable after Reset")
	}
}

// --- positions ------------------------------------------------------------------------------

// EVERY CORE CALL SITE HANDLES false BY DEGRADING RATHER THAN GUESSING, which is what keeps a
// source with no order-preserving encoding a first-class citizen. A Compare that invented an answer
// would make an uncomparable source silently unsafe.
func TestCompareRefusesRatherThanGuesses(t *testing.T) {
	ordered := func(b ...byte) record.Position { return record.Position{Order: b} }

	if _, ok := ordered(1).Compare(record.Position{}); ok {
		t.Error("comparing against a position with no Order reported an answer")
	}
	if _, ok := (record.Position{}).Compare(ordered(1)); ok {
		t.Error("comparing from a position with no Order reported an answer")
	}

	for _, tc := range []struct {
		a, b record.Position
		want int
	}{
		{ordered(1), ordered(2), -1},
		{ordered(2), ordered(1), +1},
		{ordered(1, 2), ordered(1, 2), 0},
		{ordered(1), ordered(1, 0), -1}, // a prefix sorts before its extension
	} {
		got, ok := tc.a.Compare(tc.b)
		if !ok {
			t.Fatalf("%v vs %v: refused although both carry Order", tc.a.Order, tc.b.Order)
		}
		if got != tc.want {
			t.Errorf("%v vs %v: got %d, want %d", tc.a.Order, tc.b.Order, got, tc.want)
		}
	}
}

func TestIsZeroAndComparable(t *testing.T) {
	if !(record.Position{}).IsZero() {
		t.Error("the zero position does not report itself as zero")
	}
	if (record.Position{Seq: 1}).IsZero() {
		t.Error("a positioned record reported itself as zero")
	}
	if (record.Position{Token: record.Blob{Version: 1, Bytes: []byte{1}}}).IsZero() {
		t.Error("a position carrying a token reported itself as zero")
	}
	if (record.Position{}).Comparable() {
		t.Error("a position with no Order reported itself comparable")
	}
}

// On false the caller OMITS the metric series entirely rather than emitting zero, so the boundary
// conditions here decide whether a progress gauge lies.
func TestFractionOnlyAnswersWhenItCan(t *testing.T) {
	at := func(f float64) record.Position { return record.Position{Scalar: &f} }

	for _, tc := range []struct {
		name       string
		lo, o, hi  record.Position
		want       float64
		wantAnswer bool
	}{
		{"midpoint", at(0), at(50), at(100), 0.5, true},
		{"at the floor", at(0), at(0), at(100), 0, true},
		{"at the ceiling", at(0), at(100), at(100), 1, true},
		{"clamped below", at(10), at(0), at(100), 0, true},
		{"clamped above", at(0), at(150), at(100), 1, true},
		{"no scalar anywhere", record.Position{}, record.Position{}, record.Position{}, 0, false},
		{"missing one scalar", at(0), record.Position{}, at(100), 0, false},
		{"empty range", at(5), at(5), at(5), 0, false},
		{"inverted range", at(100), at(50), at(0), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := record.Fraction(tc.lo, tc.o, tc.hi)
			if ok != tc.wantAnswer {
				t.Fatalf("answered %v, want %v", ok, tc.wantAnswer)
			}
			if ok && math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// --- payloads -------------------------------------------------------------------------------

// A byte payload and a structured one are different kinds, and a codec that asks the wrong question
// must get a refusal rather than a plausible-looking empty answer.
func TestBytesPayloadIsDistinguishable(t *testing.T) {
	p := record.BytesPayload([]byte("hello"))
	b, ok := p.Bytes()
	if !ok {
		t.Fatal("a byte payload did not answer Bytes")
	}
	if string(b) != "hello" {
		t.Errorf("got %q, want hello", b)
	}
	if !p.HasBytes() || p.HasStructured() {
		t.Errorf("a byte payload reports has_bytes=%v has_structured=%v", p.HasBytes(), p.HasStructured())
	}
	if _, ok := p.Structured(); ok {
		t.Error("a byte payload answered Structured; the json encoder refuses this case by asking")
	}

	// BytesCopy is the one a connector must use when it retains the slice, because Bytes hands out
	// the buffer the core owns and reuses.
	cp, _ := p.BytesCopy()
	cp[0] = 'H'
	again, _ := p.Bytes()
	if string(again) != "hello" {
		t.Errorf("BytesCopy aliased the payload: it now reads %q", again)
	}

	st := record.StructPayload(record.String("x"))
	if !st.HasStructured() || st.HasBytes() {
		t.Errorf("a structured payload reports has_bytes=%v has_structured=%v", st.HasBytes(), st.HasStructured())
	}
	if _, ok := st.Bytes(); ok {
		t.Error("a structured payload answered Bytes")
	}
}

func TestGroupsAreUniquePerBatch(t *testing.T) {
	a := alloc()
	seen := map[record.GroupID]bool{}
	for i := 0; i < 100; i++ {
		g := record.NewBatch(a, 1).Group()
		if seen[g] {
			t.Fatalf("group %d was issued twice; two batches sharing a group settle as one", g)
		}
		seen[g] = true
	}
}

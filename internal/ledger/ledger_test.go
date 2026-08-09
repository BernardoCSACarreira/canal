// Regression tests for the two fatal defects the rule-compliance audit found in this package.
//
// Both were reachable in ordinary operation, neither had a test, and one had no detector of any
// kind. They are pinned here because the audit's own finding was that internal/ledger is the
// machinery the engine calls first and that nothing was asserting its behaviour.
package ledger

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

func newLedger(t *testing.T) *Ledger {
	t.Helper()
	// GroupTTL zero disables the reaper, which Config documents as legal only in a test.
	return New(Config{Tenant: "default", Pipeline: "p", DefaultBudget: 1000})
}

func newBatch(lane record.LaneID, n int) *record.Batch {
	a := record.NewAllocator("default", "p", "in", lane, "s", 1, 1)
	b := record.NewBatch(a, n)
	for i := 0; i < n; i++ {
		b.Add()
	}
	return b
}

// TestSendDuringCloseDoesNotPanic pins the send-on-closed-channel panic.
//
// Ledger.send used to read l.closed under the mutex, RELEASE it, and only then write to l.acks.
// Close set the flag and closed the channel in that gap, so a settlement racing a shutdown took the
// process down with "send on closed channel". The window is the one the engine reserves for late
// acks while draining, so it was reachable on an ordinary shutdown rather than only under load.
//
// The reproduction is DETERMINISTIC rather than probabilistic. Racing send against Close and hoping
// to land in the gap does not reproduce reliably — a first version of this test looped two hundred
// times against the unfixed code and passed every time, because a drained buffer lets every send
// complete before Close can run.
//
// Filling the buffer instead parks senders inside the channel operation itself, which is precisely
// the state the old code could not survive: closing a channel that goroutines are blocked sending on
// panics immediately and unconditionally. Verified by reverting send and Close, at which point this
// test fails with "send on closed channel".
func TestSendDuringCloseDoesNotPanic(t *testing.T) {
	l := newLedger(t)
	if err := l.Lane("lane-1", connector.OrderingPrefix, 1000, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	// No drain: fill the buffer so that later senders block inside `l.acks <- a`.
	const senders = 96 // comfortably more than the channel's capacity of 64
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// send is unexported and is reached through the settlement path in production. Calling
			// it directly is what isolates the shutdown window from everything else.
			l.send(connector.Ack{Lane: "lane-1"})
		}()
	}

	// Wait until the buffer is genuinely full, so Close is guaranteed to run with senders parked.
	for len(l.Acks()) < cap(l.acks) {
		runtime.Gosched()
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait() // every parked sender must have been released, not left leaked

	// The channel is closed and drainable, and the acks that did fit are still readable.
	n := 0
	for range l.Acks() {
		n++
	}
	if n == 0 {
		t.Error("no acknowledgement survived; Close discarded the whole buffer")
	}
}

// TestCloseIsIdempotent guards the early return in Close, since Close now also closes stopSend and
// waits on a WaitGroup — both of which panic or hang if run twice.
func TestCloseIsIdempotent(t *testing.T) {
	l := newLedger(t)
	go func() {
		for range l.Acks() {
		}
	}()
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestSendAfterCloseIsDropped is the other half: once closed, a late settlement must be discarded
// rather than delivered or fatal. Dropping is the safe direction — an acknowledgement never sent
// means the source is never told to advance, so those records are re-read after restart.
func TestSendAfterCloseIsDropped(t *testing.T) {
	l := newLedger(t)
	go func() {
		for range l.Acks() {
		}
	}()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l.send(connector.Ack{Lane: "lane-1"}) // must not panic
}

// TestGroupIDReuseIsRefused pins the defect that had no detector.
//
// Admit used to assign l.groups[b.Group()] unconditionally. Re-admitting a live group id dropped
// the earlier group from the map while its ticket stayed in the tracker, so the lane's prefix could
// never advance past it and the lane wedged permanently. The leak reaper walks l.groups, and the
// orphaned group is precisely what is no longer in it — so nothing reported the stall, which is
// worse than a crash.
func TestGroupIDReuseIsRefused(t *testing.T) {
	l := newLedger(t)
	defer func() { _ = l.Close() }()
	go func() {
		for range l.Acks() {
		}
	}()

	if err := l.Lane("lane-1", connector.OrderingPrefix, 1000, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	first := newBatch("lane-1", 2)
	if err := l.Admit(context.Background(), first); err != nil {
		t.Fatalf("first Admit: %v", err)
	}

	// A second batch carrying the same group id. NewBatchLike deliberately shares the group — it is
	// how the splitter reframes without re-identifying — so this is exactly the shape a buggy
	// reframing node or a connector minting its own ids would produce.
	reused := record.NewBatchLike(first, 2)
	reused.Add()
	if got, want := reused.Group(), first.Group(); got != want {
		t.Fatalf("fixture is wrong: NewBatchLike group %q != %q", got, want)
	}

	err := l.Admit(context.Background(), reused)
	if err == nil {
		t.Fatal("re-admitting a live group id was accepted; the lane would wedge with no detector")
	}
	if !strings.Contains(err.Error(), "already in flight") {
		t.Fatalf("expected a contract fault naming the reused group, got: %v", err)
	}

	// The refusal must not itself leak the tracker ticket it took before discovering the duplicate.
	st := l.Stats("lane-1")
	if st.PendingGroups != 1 {
		t.Errorf("pending groups = %d, want 1: the refused Admit leaked or dropped a ticket", st.PendingGroups)
	}
	if st.RecordsRead != uint64(first.Len()) {
		t.Errorf("admitted = %d, want %d: the refused batch was counted as admitted", st.RecordsRead, first.Len())
	}
}

// TestIdlePositionsDoNotGrowUnbounded pins the first of the two unbounded backpressure paths.
//
// TrackResolved contributes zero weight by design, so a thousand quiet streams emitting a position
// every second cost no budget. But the budget is weight-based, so nothing bounded the NODE count: a
// lane whose prefix is stuck behind one unsettled record grew a node per idle poll forever. The
// audit measured 419.6 MiB at five million nodes, invisible to every backpressure signal.
func TestIdlePositionsDoNotGrowUnbounded(t *testing.T) {
	tr := NewTracker[record.Position](8)
	// One unsettled tracked node, so the prefix can never advance past it.
	if _, err := tr.Track(context.Background(), record.Position{Seq: 0}, 1, 1); err != nil {
		t.Fatalf("Track: %v", err)
	}

	const idle = 100_000
	for i := 1; i <= idle; i++ {
		tr.TrackResolved(record.Position{Seq: uint64(i)})
	}

	_, nodes, _, _ := tr.Pending()
	if nodes > 4 {
		t.Errorf("%d nodes after %d idle positions behind a stuck prefix; coalescing is not working", nodes, idle)
	}

	// Coalescing must not lose the position: settling the blocker has to advance to the LAST one.
	tr2 := NewTracker[record.Position](8)
	k, err := tr2.Track(context.Background(), record.Position{Seq: 0}, 1, 1)
	if err != nil {
		t.Fatalf("Track: %v", err)
	}
	for i := 1; i <= 10; i++ {
		tr2.TrackResolved(record.Position{Seq: uint64(i)})
	}
	pos, moved := tr2.Release(k, 1)
	if !moved {
		t.Fatal("releasing the blocker did not advance the prefix")
	}
	if pos.Seq != 10 {
		t.Errorf("prefix advanced to Seq %d, want 10: coalescing dropped the latest position", pos.Seq)
	}
}

// TestDiscreteLaneIsBounded pins the second unbounded path.
//
// A discrete lane was given no tracker, on the reasoning that it has no cursor and therefore no
// prefix to resolve. But the tracker is also the in-flight budget, so skipping it meant Admit never
// blocked: the audit measured 200,000 records admitted against a configured budget of 8.
func TestDiscreteLaneIsBounded(t *testing.T) {
	l := newLedger(t)
	defer func() { _ = l.Close() }()
	go func() {
		for range l.Acks() {
		}
	}()

	const budget = 8
	if err := l.Lane("d", connector.OrderingDiscrete, budget, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	// Admit past the budget with a cancellable context. Once the budget is exhausted Admit must
	// block, so the context deadline is what ends the loop — an unbounded lane runs to completion.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	admitted := 0
	alloc := record.NewAllocator("default", "p", "in", "d", "s", 1, 1)
	for i := 0; i < 1000; i++ {
		b := record.NewBatch(alloc, 1)
		b.Add()
		if err := l.Admit(ctx, b); err != nil {
			break
		}
		admitted++
	}

	if admitted > budget*4 {
		t.Errorf("admitted %d records against a budget of %d; the discrete lane is unbounded", admitted, budget)
	}
	if admitted == 0 {
		t.Error("admitted nothing at all; the budget is not backpressure, it is a wall")
	}
	t.Logf("admitted %d against budget %d before blocking", admitted, budget)
}

// A SHED ADVANCES THE POSITION PAST WHAT IT DROPPED, and that is the property that makes shedding
// different from a noisier kind of backpressure.
//
// If the prefix did not move, the source would re-read exactly the records the operator configured
// the pipeline to drop, the lane would still be full, and the shed would repeat forever — a
// permanently stuck cursor dressed up as a load-shedding policy. TrackResolved is how a position
// enters the ordered prefix carrying no references, so it still takes its place BEHIND anything
// outstanding rather than committing past unsettled records.
func TestAShedAdvancesThePositionPastTheDroppedRecords(t *testing.T) {
	l := New(Config{Tenant: "acme", Pipeline: "p", DefaultBudget: 2, GroupTTL: time.Minute})
	defer l.Close()
	if err := l.Lane("lane-1", connector.OrderingPrefix, 2, connector.WhenFullReject); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	at := func(n byte) record.Position {
		return record.Position{Order: []byte{n}, Token: record.Blob{Version: 1, Bytes: []byte{n}}, Safe: true}
	}

	// Fill the budget.
	first := batchAt(t, "lane-1", at(1), 2)
	if err := l.Admit(context.Background(), first); err != nil {
		t.Fatalf("admitting the first batch: %v", err)
	}

	// The next batch has nowhere to go and the policy sheds it.
	shed := batchAt(t, "lane-1", at(2), 2)
	err := l.Admit(context.Background(), shed)
	if !errors.Is(err, ErrShed) {
		t.Fatalf("admitting into a full lane under reject returned %v, want ErrShed", err)
	}
	if got := l.Stats("lane-1").AbandonedTotal; got != 2 {
		t.Errorf("%d records counted as abandoned, want the 2 that were shed", got)
	}

	// Settling the first batch must now resolve the prefix ALL THE WAY PAST the shed position,
	// because nothing is outstanding in front of it any more.
	outs := make([]Outcome, 0, 2)
	for _, r := range first.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}
	l.Settle(outs)

	st := l.Stats("lane-1")
	if !st.ResolvedOK {
		t.Fatal("nothing resolved after the only outstanding batch settled")
	}
	if len(st.Resolved.Order) == 0 || st.Resolved.Order[0] != 2 {
		t.Errorf("the resolved prefix is at %v, want the shed batch's position 2: a shed that does "+
			"not advance makes the source re-read exactly what it was told to drop", st.Resolved.Order)
	}
}

// batchAt builds a positioned batch of n records for a lane.
func batchAt(t *testing.T, lane record.LaneID, pos record.Position, n int) *record.Batch {
	t.Helper()
	a := record.NewAllocator("acme", "p", "in", lane, "s", 1, 1)
	b := record.NewBatch(a, n)
	for i := 0; i < n; i++ {
		if b.Add() == nil {
			t.Fatalf("Add returned nil at %d", i)
		}
	}
	b.Position = pos
	return b
}

// SETTLED IS PHASE TWO AND COMMITTED IS PHASE THREE, and reporting one under the other's name
// overstates how far the source has been told it may advance.
//
// The two diverge by exactly the window the three-phase design exists to manage: a record is settled
// the moment a sink accepts it, and committed only once canal's own write of its position is durable
// AND the acknowledgement has been produced. LaneStats reported the tracker's settled count as
// RecordsCommitted while the ledger computed the real number and discarded it — found by the
// write-only field check in internal/arch.
func TestSettledAndCommittedAreDifferentNumbers(t *testing.T) {
	l := New(Config{Tenant: "acme", Pipeline: "p", DefaultBudget: 16, GroupTTL: time.Minute})
	defer l.Close()
	if err := l.Lane("lane-1", connector.OrderingPrefix, 16, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	pos := record.Position{Order: []byte{1}, Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true}
	b := batchAt(t, "lane-1", pos, 4)
	if err := l.Admit(context.Background(), b); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	// PHASE TWO. The sink has accepted every record and nothing is durable yet.
	outs := make([]Outcome, 0, 4)
	for _, r := range b.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}
	l.Settle(outs)

	st := l.Stats("lane-1")
	if st.Settled != 4 {
		t.Fatalf("%d settled after every record landed, want 4", st.Settled)
	}
	if st.RecordsCommitted != 0 {
		t.Errorf("%d records reported committed before any position was made durable; the source has "+
			"been told nothing at all yet", st.RecordsCommitted)
	}

	// PHASE THREE. Only now has the position been flushed and the source may be told.
	l.Committed(map[record.LaneID]record.Position{"lane-1": pos})

	st = l.Stats("lane-1")
	if st.RecordsCommitted != 4 {
		t.Errorf("%d records committed after the position was made durable, want 4", st.RecordsCommitted)
	}
	if st.Settled != 4 {
		t.Errorf("settled changed to %d during phase three; it is a phase-two count", st.Settled)
	}
}

// THE TWO FENCES A REVOKED LANE HAS IN THIS PACKAGE, PINNED SEPARATELY.
//
// The engine-level fixture in internal/engine proves the property that matters — a fenced worker
// never advances its upstream — but it cannot pin either of these, and the reason is worth writing
// down: the protection is REDUNDANT. Three rules each independently prevent the acknowledgement, so
// deleting any one of them leaves the end-to-end test green. Only deleting all three moves it.
//
// Defence in depth is the right design for "specified data loss", and it is also how a layer rots
// unnoticed. These two assertions are what make the ledger's share of it individually falsifiable:
// Flushable must not offer a revoked lane, and Committed must not acknowledge one even if asked.
func TestARevokedLaneIsNeitherFlushedNorAcknowledged(t *testing.T) {
	l := New(Config{Tenant: "acme", Pipeline: "p", DefaultBudget: 16, GroupTTL: time.Minute})
	defer l.Close()
	if err := l.Lane("lane-1", connector.OrderingPrefix, 16, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	pos := record.Position{Order: []byte{1}, Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true}
	b := batchAt(t, "lane-1", pos, 4)
	if err := l.Admit(context.Background(), b); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	// The records settle BEFORE the revocation is known, which is the whole difficulty: this worker
	// did the work, the sink took it, and only then did the lane move. The settlement is real and
	// the acknowledgement it would produce is what must never leave.
	outs := make([]Outcome, 0, 4)
	for _, r := range b.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}
	l.Settle(outs)

	if _, ok := l.Flushable()["lane-1"]; !ok {
		t.Fatal("the lane was not flushable after every record settled, so this test would prove " +
			"nothing about what revocation changes")
	}

	// Re-admit and re-settle, because Flushable consumed the pending position above.
	pos2 := record.Position{Order: []byte{2}, Token: record.Blob{Version: 1, Bytes: []byte{2}}, Safe: true}
	b2 := batchAt(t, "lane-1", pos2, 4)

	// batchAt builds a FRESH allocator seeded at group 1 and record id 1, so this batch would
	// otherwise be group 1 with records 1-4 — the same identity the first batch had. It passes
	// either way today, because settling the first batch removed it from l.groups and its records
	// from l.byRec before this one is admitted. That is a property of the ORDER of this test rather
	// than of anything the ledger promises, and the engine's own allocators had exactly this
	// collision until allocatorFor started seeding them apart. Distinct here on purpose, so a later
	// edit that admits both at once fails on the assertion rather than on a duplicate group.
	b2.SetGroup(record.GroupID(99))
	if err := l.Admit(context.Background(), b2); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	outs = outs[:0]
	for _, r := range b2.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}

	unsettled := l.Revoke("lane-1")
	l.Settle(outs)

	if got := l.Flushable(); len(got) != 0 {
		t.Errorf("a revoked lane was offered for flushing: %v.\n"+
			"  Persisting that cursor is this worker telling the store it got further than the new "+
			"holder knows about, and no epoch takes it back", got)
	}

	// Asked directly, as the flush loop would if the fence above were ever bypassed.
	acks := make(chan connector.Ack, 4)
	go func() {
		for a := range l.Acks() {
			acks <- a
		}
	}()
	l.Committed(map[record.LaneID]record.Position{"lane-1": pos2})

	select {
	case a := <-acks:
		t.Errorf("a revoked lane was acknowledged through %v (%d records); every one of those lets "+
			"the upstream discard records the new holder has not delivered",
			a.Through.Order, a.Records)
	case <-time.After(200 * time.Millisecond):
	}

	if unsettled == 0 {
		t.Error("Revoke reported no unsettled records while four were in flight; the overlap the " +
			"new holder has to absorb is the number this reports and an operator's only sight of it")
	}

	// The accounting is the fence's third face: a lane whose acknowledgements never left must
	// report zero records committed, or the read model advertises progress the source was never
	// told about — a number documented as "reached the source" that nothing reached.
	if got := l.Stats("lane-1").RecordsCommitted; got != 0 {
		t.Errorf("a revoked lane reports %d records committed while every acknowledgement was "+
			"suppressed; the counter's contract is records whose position reached the source", got)
	}
}

// A REVOCATION IS SURVIVABLE, AND THE WORK IT FENCED STAYS FENCED.
//
// The fence used to be a per-lane flag nothing could clear, so losing a lease cost the lane for the
// rest of the process: a re-claimed lane would have been read normally and acknowledged never, pinning
// the upstream's retention with nothing to say why, so the engine refused the re-claim outright. A
// lease lost to a GC pause was a lane lost until a restart.
//
// A watermark can be outlived. This walks the whole sequence — hold at one epoch, lose it with records
// still in flight, let those settle, be granted a higher epoch, and produce again — and pins the two
// halves that have to be true at once: the new epoch works, and the old epoch's records are not in its
// acknowledgement.
func TestALaneRevokedAtOneEpochWorksAgainAtTheNext(t *testing.T) {
	l := New(Config{Tenant: "acme", Pipeline: "p", DefaultBudget: 16, GroupTTL: time.Minute})
	defer l.Close()
	if err := l.Lane("lane-1", connector.OrderingPrefix, 16, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}
	acks := make(chan connector.Ack, 8)
	go func() {
		for a := range l.Acks() {
			acks <- a
		}
	}()
	drain := func() []connector.Ack {
		var out []connector.Ack
		for {
			select {
			case a := <-acks:
				out = append(out, a)
			case <-time.After(150 * time.Millisecond):
				return out
			}
		}
	}
	settle := func(b *record.Batch) {
		outs := make([]Outcome, 0, b.Len())
		for _, r := range b.Records {
			outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
		}
		l.Settle(outs)
	}
	at := func(n byte) record.Position {
		return record.Position{Order: []byte{n}, Token: record.Blob{Version: 1, Bytes: []byte{n}}, Safe: true}
	}

	// --- held at epoch 5 ---
	l.SetEpoch("lane-1", 5)

	// One batch that RESOLVES before the lease goes, and is never flushed. Its position sits in
	// pendingFlush at revocation time, which is the half of the hazard the advance filter cannot reach:
	// the advance was published while this worker legitimately held the lane, so only Revoke itself can
	// drop it. Left behind, the re-claimed lane commits it under an epoch the store now accepts — and if
	// another worker held the lane in between, that moves the durable cursor BACKWARDS.
	early := batchAt(t, "lane-1", at(1), 2)
	early.SetGroup(record.GroupID(1))
	if err := l.Admit(context.Background(), early); err != nil {
		t.Fatalf("admitting the early batch at epoch 5: %v", err)
	}
	settle(early)

	// And one still in flight when it goes.
	lost := batchAt(t, "lane-1", at(2), 4)
	lost.SetGroup(record.GroupID(2))
	if err := l.Admit(context.Background(), lost); err != nil {
		t.Fatalf("admitting at epoch 5: %v", err)
	}

	// --- the lease goes while they are outstanding ---
	if unsettled := l.Revoke("lane-1"); unsettled != 4 {
		t.Errorf("Revoke reported %d unsettled records, want 4; that number is the duplicate window "+
			"an operator is told to expect", unsettled)
	}

	// NO Committed PROBE HERE, deliberately. Asking a fenced lane for an acknowledgement directly is
	// worth pinning and is pinned next door, in TestARevokedLaneIsNeitherFlushedNorAcknowledged. Doing
	// it here set this lane's committed cursor TO the position the fenced work had reached, so
	// Flushable's never-move-backwards invariant filtered the stale position instead of Revoke — and
	// the check below passed whether or not Revoke discarded anything.
	//
	// They settle anyway, which Revoke's own doc requires: the ticket must be discharged or the lane's
	// budget never frees. What they must not do is turn into an acknowledgement.
	settle(lost)
	if got := l.Flushable(); len(got) != 0 {
		t.Errorf("a fenced lane was offered for flushing: %v", got)
	}

	// --- re-claimed at a higher epoch ---
	l.SetEpoch("lane-1", 6)

	// NOTHING IS FLUSHABLE THE INSTANT THE FENCE LIFTS, checked before any new work exists so a stale
	// position cannot be masked by a fresh one. The fenced records settled, which advanced the tracker's
	// prefix and left a pendingFlush behind; kept, that position would be committed under the new epoch
	// — through a store fence that now accepts it — telling the upstream to discard through a cursor
	// this worker produced while holding nothing. Revoke drops it, so the lane resumes from the last
	// DURABLE position, which is what the new holder is promised.
	if got := l.Flushable(); len(got) != 0 {
		t.Errorf("a re-claimed lane offered %v for flushing before it had read anything; that is the "+
			"fenced epoch's position surviving into the epoch that replaced it", got)
	}

	kept := batchAt(t, "lane-1", at(3), 3)
	kept.SetGroup(record.GroupID(99)) // batchAt seeds every allocator alike; see the note in the test above
	if err := l.Admit(context.Background(), kept); err != nil {
		t.Fatalf("admitting at epoch 6 after a revocation at 5: %v\n"+
			"  This is the whole point of a watermark: the lane is this worker's again", err)
	}
	settle(kept)

	if _, ok := l.Flushable()["lane-1"]; !ok {
		t.Fatal("a re-claimed lane never became flushable again, so its cursor can never advance " +
			"and the upstream pins its retention forever — which is the outcome the engine used to " +
			"avoid by refusing the re-claim")
	}
	l.Committed(map[record.LaneID]record.Position{"lane-1": at(3)})

	got := drain()
	if len(got) != 1 {
		t.Fatalf("%d acknowledgements after the re-claim, want exactly 1: %+v", len(got), got)
	}
	if got[0].Epoch != 6 {
		t.Errorf("the acknowledgement carries epoch %d, want 6", got[0].Epoch)
	}
	if got[0].Records != 3 {
		t.Errorf("the acknowledgement covers %d records, want 3.\n"+
			"  Seven means the four fenced records were counted into the epoch that replaced them, "+
			"which tells the upstream to discard work this worker delivered while holding nothing",
			got[0].Records)
	}
}

// RECORDSCOMMITTED INCREMENTS EXACTLY WHEN AN ACKNOWLEDGEMENT IS EMITTED, BY EXACTLY Ack.Records.
// The field's own doc is the contract: records whose position reached the SOURCE — not records
// whose cursor happened to be in a flushed map.
//
// The sequence here is the reachable violation. A discrete lane's settlements accumulate ackRecords
// with no ordering guard, but when none of the settled records carried a handle, emitDiscrete has
// nothing to deliver and skips the lane WITHOUT consuming the accumulator. If that lane's cursor
// then reaches Committed — which Flushable permits, because an empty positioned batch resolves a
// position on any lane and Flushable has no ordering filter — the prefix loop counted the
// accumulator into recordsCommitted and continued at its ordering check without resetting it. The
// next handled settlement emitted an acknowledgement carrying the SAME records and counted them
// again: the source was told N+M once, the read model said 2N+M.
func TestRecordsCommittedCountsOnlyWhatAnAcknowledgementCarried(t *testing.T) {
	l := New(Config{Tenant: "acme", Pipeline: "p", DefaultBudget: 16, GroupTTL: time.Minute})
	defer l.Close()
	if err := l.Lane("lane-1", connector.OrderingDiscrete, 16, connector.WhenFullBlock); err != nil {
		t.Fatalf("Lane: %v", err)
	}

	// Four records, none with a handle. They settle, the accumulator holds 4, and no
	// acknowledgement can be emitted for them yet.
	pos1 := record.Position{Order: []byte{1}, Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true}
	b1 := batchAt(t, "lane-1", pos1, 4)
	if err := l.Admit(context.Background(), b1); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	outs := make([]Outcome, 0, 4)
	for _, r := range b1.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}
	l.Settle(outs)

	// The engine flushes this lane's cursor and reports it durable. No acknowledgement has been
	// produced — there is still nothing to deliver — so the committed count must not move.
	pos2 := record.Position{Order: []byte{2}, Token: record.Blob{Version: 1, Bytes: []byte{2}}, Safe: true}
	l.Committed(map[record.LaneID]record.Position{"lane-1": pos2})

	if got := l.Stats("lane-1").RecordsCommitted; got != 0 {
		t.Errorf("%d records reported committed while no acknowledgement has ever been emitted for "+
			"this lane; the source has been told nothing", got)
	}

	// Two more records, with handles. Their settlement emits the lane's first acknowledgement,
	// which carries everything accumulated so far: all six records, two handles.
	pos3 := record.Position{Order: []byte{3}, Token: record.Blob{Version: 1, Bytes: []byte{3}}, Safe: true}
	b2 := batchAt(t, "lane-1", pos3, 2)
	b2.SetGroup(record.GroupID(99))
	for i, r := range b2.Records {
		r.SetHandle([]byte{byte(0x10 + i)})
	}
	if err := l.Admit(context.Background(), b2); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	outs = outs[:0]
	for _, r := range b2.Records {
		outs = append(outs, Outcome{Record: r.Origin().ID, Node: "out", Disposition: Delivered})
	}
	l.Settle(outs)

	var ack connector.Ack
	select {
	case ack = <-l.Acks():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("no acknowledgement after a handled settlement on a discrete lane")
	}
	if ack.Records != 6 {
		t.Fatalf("the acknowledgement carries %d records, want 6: the four handle-less records are "+
			"reported in the count of the first acknowledgement that can be emitted", ack.Records)
	}

	if got := l.Stats("lane-1").RecordsCommitted; got != 6 {
		t.Errorf("RecordsCommitted is %d after an acknowledgement carrying 6, want 6.\n"+
			"  Ten means the four handle-less records were counted once by Committed — which never "+
			"acknowledged them — and again by the acknowledgement that did", got)
	}
}

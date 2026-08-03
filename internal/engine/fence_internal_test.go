package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/BernardoCSACarreira/canal/internal/example/memstore"
	"github.com/BernardoCSACarreira/canal/internal/ledger"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// THESE ASSERT THE EPOCH THAT REACHES THE STORE, which is the only thing the fence is made of.
//
// The end-to-end fixture in revocation_test.go cannot pin any of this, and the reason is the same one
// that made those three revocation rules mutually redundant: Ledger.Flushable already refuses to
// offer a revoked lane, so a position for one never arrives at laneCtl.stage in a running pipeline.
// Deleting everything here leaves that test green. So the writes are driven directly, with a lease
// table in the state a reclaim leaves behind.
//
// What a consumer observes is a store.Batch: which keys it carries and what epoch fences each. That
// is what these read, rather than any engine-side flag that happens to be nearby.

// recordingStore captures the batches the engine writes and answers everything else as a no-op.
type recordingStore struct {
	store.StateStore
	batches []store.Batch
}

func (r *recordingStore) Set(_ context.Context, b store.Batch) error {
	r.batches = append(r.batches, b)
	return nil
}

func (r *recordingStore) last() store.Batch {
	if len(r.batches) == 0 {
		return store.Batch{}
	}
	return r.batches[len(r.batches)-1]
}

// epochOf reports the epoch fencing one key in a batch, and whether the key is in it at all.
func epochOf(b store.Batch, k store.Key) (uint64, bool) {
	v, ok := b.Writes[k.String()]
	if !ok {
		return 0, false
	}
	return b.EpochFor(v), true
}

// fenceFixture is a lane table and a lease table over a recording store.
type fenceFixture struct {
	ctl    *laneCtl
	leases *leaseTable
	store  *recordingStore
	fields specRefFields
}

// newFenceFixture builds a COORDINATED worker holding `held` at the epochs given, with every lane in
// `known` present in the durable lane table.
func newFenceFixture(t *testing.T, known []record.LaneID, held map[record.LaneID]uint64) *fenceFixture {
	t.Helper()
	rs := &recordingStore{StateStore: memstore.New()}
	fields := specRefFields{tenant: "acme", pipeline: "p1"}
	// Log is set EXPLICITLY because this fixture constructs the runtime directly rather than through
	// Build, which is where a nil logger gets defaulted to slog.Default(). Nothing here exercises
	// that normalisation, so anything it would have filled in has to be filled in here.
	deps := Deps{
		State: rs, Worker: "w1", Coordinator: memstore.NewCoordinator(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	lt := newLeaseTable(deps, fields, 1)
	for id, epoch := range held {
		lt.held[id] = store.Lease{
			Assignment: store.AssignmentID(id), Worker: "w1", Epoch: epoch,
			Expires: time.Now().Add(time.Minute),
		}
	}

	ctl := newLaneCtl(deps, fields, "in",
		func(record.LaneID, connector.Ordering, int) error { return nil },
		func(record.LaneID) connector.Admission { return connector.Admission{} },
		lt)
	for _, id := range known {
		ctl.lanes[id] = &laneRecord{Spec: connector.LaneSpec{Name: string(id)}}
	}
	return &fenceFixture{ctl: ctl, leases: lt, store: rs, fields: fields}
}

func (f *fenceFixture) laneKey(id record.LaneID) store.Key {
	return store.LaneKey(f.fields.tenant, f.fields.pipeline, id)
}

// ZERO MEANS OPPOSITE THINGS IN THE TWO APIS, and that is the whole reason fenceFor exists.
//
// epochFor reports 0 for a lane this worker does not hold, because store.Assignment uses 0 for
// "unclaimed". store.Versioned reads a 0 as "fence this with the batch's epoch". Wire the first into
// the second and an unheld lane's write does not get refused — it gets the batch default and LANDS,
// which is the one outcome the fence exists to prevent, arrived at by passing the right value to the
// wrong reader.
func TestFenceForRefusesWhereEpochForReportsZero(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, map[record.LaneID]uint64{"a": 7})

	if got := f.leases.epochFor("b"); got != 0 {
		t.Fatalf("epochFor reported %d for an unheld lane, want 0 — this test's premise is gone", got)
	}
	if epoch, mine := f.leases.fenceFor("b", time.Now()); mine {
		t.Errorf("fenceFor allowed a write for an unheld lane at epoch %d; that epoch reaches "+
			"PutFenced, where a zero means the batch's default and the write lands", epoch)
	}

	if epoch, mine := f.leases.fenceFor("a", time.Now()); !mine || epoch != 7 {
		t.Errorf("fenceFor gave (%d, %v) for a lane held at epoch 7, want (7, true)", epoch, mine)
	}
}

// AN EXPIRED LEASE IS NOT A HELD ONE. The store is the authority, but a worker that can already see
// its lease has lapsed should stop before the write rather than in its error path — which is what
// store.Lease.Valid's own doc says the comparison is for.
func TestFenceForRefusesAnExpiredLease(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, nil)
	f.leases.held["a"] = store.Lease{
		Assignment: "a", Worker: "w1", Epoch: 4, Expires: time.Now().Add(-time.Second),
	}
	if epoch, mine := f.leases.fenceFor("a", time.Now()); mine {
		t.Errorf("fenceFor allowed a write at epoch %d under a lease that expired a second ago", epoch)
	}
}

// A WORKER WITH NO COORDINATOR MAY ALWAYS WRITE, and at the constant rather than at zero. Zero would
// leave a standalone store full of rows fenced below anything a coordinator later hands out.
func TestFenceForIsTheConstantWithoutACoordinator(t *testing.T) {
	lt := newLeaseTable(Deps{Worker: "w1"}, specRefFields{tenant: "acme", pipeline: "p1"}, 1)
	epoch, mine := lt.fenceFor("anything-at-all", time.Now())
	if !mine || epoch != singleWorkerEpoch {
		t.Errorf("fenceFor gave (%d, %v) with no coordinator, want (%d, true)",
			epoch, mine, singleWorkerEpoch)
	}
}

// THE CURSOR CARRIES ITS OWN LANE'S EPOCH, not the batch's.
//
// A batch here spans every lane whose prefix advanced this tick, each held under its own lease. One
// number for all of them cannot fence: high enough for the lanes still held is high enough for the
// ones already lost, which is what store.Versioned.Epoch exists to say.
func TestEachLanesCursorIsFencedByItsOwnLease(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a", "b"}, map[record.LaneID]uint64{"a": 3, "b": 9})

	batch := store.NewBatch(singleWorkerEpoch)
	done, err := f.ctl.stage(batch, map[record.LaneID]record.Position{
		"a": {Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true},
		"b": {Token: record.Blob{Version: 1, Bytes: []byte{2}}, Safe: true},
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	done(true)

	for lane, want := range map[record.LaneID]uint64{"a": 3, "b": 9} {
		got, ok := epochOf(*batch, f.laneKey(lane))
		if !ok {
			t.Errorf("lane %s's cursor is not in the batch at all", lane)
			continue
		}
		if got != want {
			t.Errorf("lane %s's cursor is fenced at epoch %d, want %d (the batch default is %d, so "+
				"this is the one-number-for-every-lane bug)", lane, got, want, singleWorkerEpoch)
		}
	}
}

// A LANE THIS WORKER HAS LOST IS DROPPED FROM THE BATCH, and the lanes it still holds are not.
//
// Dropping rather than failing, because the batch is atomic and the other lanes' cursors are this
// worker's to commit. Failing would let one reclaimed lane stall every other lane's progress, which
// is the blast radius store.Lease's doc rules out: the loser's lane, not its whole process.
func TestAReclaimedLanesCursorIsNotWrittenAndDoesNotStopTheOthers(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a", "b"}, map[record.LaneID]uint64{"b": 9})

	batch := store.NewBatch(singleWorkerEpoch)
	done, err := f.ctl.stage(batch, map[record.LaneID]record.Position{
		"a": {Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true},
		"b": {Token: record.Blob{Version: 1, Bytes: []byte{2}}, Safe: true},
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	done(true)

	if epoch, ok := epochOf(*batch, f.laneKey("a")); ok {
		t.Errorf("a lane this worker no longer holds had its cursor staged at epoch %d; persisting "+
			"it tells the store this worker got further than the new holder knows about", epoch)
	}
	if _, ok := epochOf(*batch, f.laneKey("b")); !ok {
		t.Error("the lane this worker still holds lost its cursor too; one reclaimed lane must not " +
			"stall the rest")
	}
	// The dropped lane's in-memory cursor must not advance either, or the next flush would compute a
	// delta from a position that was never persisted.
	if got := f.ctl.lanes["a"].Cursor; !got.IsZero() {
		t.Errorf("the dropped lane's in-memory cursor advanced to %v anyway", got.Token)
	}
}

// RETIRING A LANE IS A WRITE ON ITS BEHALF. Single-lane, so there is nothing to drop — refusing is
// the whole answer, and it classifies as Fenced so a caller can tell it from a store failure.
func TestRetiringALaneThisWorkerLostIsRefused(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, nil)

	err := f.ctl.mutate(context.Background(), "a", func(r *laneRecord) error {
		r.Finished = true
		return nil
	})
	if err == nil {
		t.Fatal("retiring a lane this worker no longer holds succeeded")
	}
	if got := fault.ClassOf(err); got != fault.Fenced {
		t.Errorf("the refusal classified as %s, want fenced: %v", got, err)
	}
	if len(f.store.batches) != 0 {
		t.Errorf("%d batches reached the store for a refused write", len(f.store.batches))
	}
}

// DELETE IS THE ONE LANE MUTATION THE STORE CANNOT REFUSE, so this side has to.
//
// StateStore.Delete takes bare keys and no epoch, so there is no number for a store to compare and it
// will do as it is told. Every other fenced operation degrades to a rejected write; this one degrades
// to destroying connector state the NEW holder owns and is reading. The advisory check is therefore
// worth more here than anywhere else it appears, which is the opposite of how it looks.
func TestDeletingALaneThisWorkerLostIsRefused(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a", "b"}, map[record.LaneID]uint64{"a": 5})
	deleted := &countingDeleter{StateStore: memstore.New()}
	sh := &stateHandle{deps: Deps{State: deleted, Log: f.ctl.deps.Log},
		tenant: "acme", pipeline: "p1", node: "in", leases: f.leases}
	ctx := context.Background()

	if err := sh.Delete(ctx, "b"); err == nil {
		t.Error("deleting the state of a lane this worker no longer holds succeeded")
	} else if got := fault.ClassOf(err); got != fault.Fenced {
		t.Errorf("the refusal classified as %s, want fenced: %v", got, err)
	}
	if deleted.calls != 0 {
		t.Errorf("%d deletes reached the store; it cannot refuse them, which is why this must", deleted.calls)
	}

	// The lane this worker DOES hold is still deletable, or the check is just a broken feature.
	if err := sh.Delete(ctx, "a"); err != nil {
		t.Errorf("deleting a held lane's state was refused: %v", err)
	}
	if deleted.calls != 1 {
		t.Errorf("%d deletes reached the store for a held lane, want 1", deleted.calls)
	}
}

// countingDeleter counts the deletes that get past the engine.
type countingDeleter struct {
	store.StateStore
	calls int
}

func (c *countingDeleter) Delete(_ context.Context, _ []store.Key) error {
	c.calls++
	return nil
}

// A FENCED FINISH COSTS THE LANE AND NOT THE PIPELINE.
//
// Making Finish refusable gave this call site a way to kill the whole run: finishLane escalated every
// error from it to r.fail, so a lane reclaimed in the window between the read loop's revocation check
// and its retirement would have taken every other lane down with it. store.Lease's doc draws the line
// this asserts — "the loser's lane — not its whole process — is revoked" — and a change that tightens
// a write must not widen a blast radius on the way.
func TestAFencedFinishDoesNotFailTheRun(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, nil) // held by nobody: Finish will be refused
	r := &runner{p: &Pipeline{ledger: ledger.New(ledger.Config{Tenant: "acme", Pipeline: "p1"})},
		deps: f.ctl.deps}
	defer r.p.ledger.Close()

	r.finishLane(context.Background(), &sourceRuntime{lanes: f.ctl}, "a")

	if err := r.firstError(); err != nil {
		t.Errorf("a lane this worker had lost failed the whole run: %v\n"+
			"  Every other lane this worker holds stops with it, which is the blast radius the "+
			"per-lane lease exists to avoid", err)
	}
}

// PER-LANE CONNECTOR STATE IS FENCED LIKE THE CURSOR, and the node-shared value is fenced by nothing,
// because it belongs to no lane.
func TestConnectorStateIsFencedPerLaneAndNotWhenShared(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, map[record.LaneID]uint64{"a": 5})
	sh := &stateHandle{deps: f.ctl.deps, tenant: "acme", pipeline: "p1", node: "in", leases: f.leases}
	ctx := context.Background()

	if _, err := sh.Set(ctx, "a", record.Blob{Version: 1, Bytes: []byte("x")}, 0); err != nil {
		t.Fatalf("Set on a held lane: %v", err)
	}
	if got, ok := epochOf(f.store.last(), store.ConnectorKey("acme", "p1", "in", "a")); !ok || got != 5 {
		t.Errorf("a held lane's connector state was fenced at %d (present=%v), want 5", got, ok)
	}

	if _, err := sh.Set(ctx, "b", record.Blob{Version: 1, Bytes: []byte("x")}, 0); err == nil {
		t.Error("writing connector state for a lane this worker does not hold succeeded")
	} else if got := fault.ClassOf(err); got != fault.Fenced {
		t.Errorf("the refusal classified as %s, want fenced: %v", got, err)
	}

	// SHARED STATE IS NOT A LANE'S. A connector keeps a schema handle or a connection token here;
	// there is no lease over it, and refusing it because the node's lanes moved away would deny a
	// write that was never a lane's to fence.
	before := len(f.store.batches)
	if _, err := sh.SetShared(ctx, record.Blob{Version: 1, Bytes: []byte("y")}, 0); err != nil {
		t.Fatalf("SetShared: %v", err)
	}
	if len(f.store.batches) != before+1 {
		t.Fatal("SetShared did not write")
	}
	if got, ok := epochOf(f.store.last(), store.ConnectorNodeKey("acme", "p1", "in")); !ok ||
		got != singleWorkerEpoch {
		t.Errorf("node state was fenced at %d (present=%v), want the batch default %d",
			got, ok, singleWorkerEpoch)
	}
}

// SetMany REFUSES THE WHOLE CALL, which is the opposite of what stage does with a lost lane.
//
// The difference is who asked. stage batches cursors the ENGINE chose to persist for whichever lanes
// advanced, so dropping one leaves the rest correct. SetMany is a connector calling an interface whose
// entire purpose is that the whole set lands or none of it does; writing a subset would break the
// promise the method exists to make.
func TestSetManyRefusesRatherThanWritingASubset(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a", "b"}, map[record.LaneID]uint64{"a": 5})
	sh := &stateHandle{deps: f.ctl.deps, tenant: "acme", pipeline: "p1", node: "in", leases: f.leases}

	err := sh.SetMany(context.Background(), connector.StateWrite{
		Lanes: map[record.LaneID]connector.Write{
			"a": {Blob: record.Blob{Version: 1, Bytes: []byte("x")}},
			"b": {Blob: record.Blob{Version: 1, Bytes: []byte("y")}},
		},
	})
	if err == nil {
		t.Fatal("an atomic multi-lane write succeeded with one lane no longer held")
	}
	if got := fault.ClassOf(err); got != fault.Fenced {
		t.Errorf("the refusal classified as %s, want fenced: %v", got, err)
	}
	if len(f.store.batches) != 0 {
		t.Errorf("%d batches reached the store; an atomic write that refuses must write nothing, "+
			"and a partial one is worse than none because the connector believes both landed",
			len(f.store.batches))
	}
}

// THE SHARED SLOT TRAVELS IN THE SAME TRANSACTION, and it used to travel nowhere at all.
//
// StateWrite.Shared was read by nothing: a connector that set it got its lane entries written and its
// node entry silently dropped. That is the exact case StateHandle.SetMany's own doc gives as the
// reason atomicity matters — "a stream-to-lane index in the node slot and the lane cursors it indexes
// must move together or a restart reads one stream's progress under another stream's name" — so the
// one scenario the method was justified by was the one it did not serve.
func TestSetManyCarriesTheSharedSlotInTheSameBatch(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, map[record.LaneID]uint64{"a": 5})
	sh := &stateHandle{deps: f.ctl.deps, tenant: "acme", pipeline: "p1", node: "in", leases: f.leases}

	err := sh.SetMany(context.Background(), connector.StateWrite{
		Lanes: map[record.LaneID]connector.Write{
			"a": {Blob: record.Blob{Version: 1, Bytes: []byte("lane")}},
		},
		Shared: &connector.Write{Blob: record.Blob{Version: 1, Bytes: []byte("index")}},
	})
	if err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if n := len(f.store.batches); n != 1 {
		t.Fatalf("%d batches reached the store, want exactly 1 — the whole point is one transaction", n)
	}
	batch := f.store.last()

	if got, ok := epochOf(batch, store.ConnectorKey("acme", "p1", "in", "a")); !ok || got != 5 {
		t.Errorf("the lane entry is fenced at %d (present=%v), want 5", got, ok)
	}
	// The node slot belongs to no lane, so it rides at the batch's default rather than a lease epoch.
	if got, ok := epochOf(batch, store.ConnectorNodeKey("acme", "p1", "in")); !ok {
		t.Error("the shared slot was dropped; the lane cursors it indexes landed without it, which " +
			"is the split-brain restart SetMany exists to prevent")
	} else if got != singleWorkerEpoch {
		t.Errorf("the shared slot is fenced at %d, want the batch default %d", got, singleWorkerEpoch)
	}
}

// A CONNECTOR'S OWN EPOCH WINS FOR A LANE IT HOLDS, which is what connector.Write.Epoch says: zero
// means the epoch the core records, and a number means that number. Whether this worker may write at
// all stays the core's answer — a connector cannot name its way past a lease it does not hold.
func TestSetManyHonoursAnEpochTheConnectorSupplies(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a"}, map[record.LaneID]uint64{"a": 5})
	sh := &stateHandle{deps: f.ctl.deps, tenant: "acme", pipeline: "p1", node: "in", leases: f.leases}

	err := sh.SetMany(context.Background(), connector.StateWrite{
		Lanes: map[record.LaneID]connector.Write{
			"a": {Blob: record.Blob{Version: 1, Bytes: []byte("x")}, Epoch: 42},
		},
	})
	if err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if got, ok := epochOf(f.store.last(), store.ConnectorKey("acme", "p1", "in", "a")); !ok || got != 42 {
		t.Errorf("the entry is fenced at %d (present=%v), want the 42 the caller asked for", got, ok)
	}
}

// THE STORE IS THE AUTHORITY, and this is what the engine's numbers buy against a real one.
//
// Everything above asserts what the engine SENDS. This asserts that sending it has the effect the
// whole change is for: a worker writing under a superseded epoch is refused by the store itself, not
// merely by its own bookkeeping. Without the per-lane epoch both writes carry the same constant and
// the second one lands.
func TestTheStoreRefusesAWriteFromASupersededEpoch(t *testing.T) {
	st := memstore.New()
	if !st.Capabilities().EpochFencing {
		t.Fatal("this store does not declare EpochFencing, so it cannot answer the question this " +
			"test asks — and the engine's per-lane epochs would be numbers nothing enforces")
	}
	ctx := context.Background()
	key := store.LaneKey("acme", "p1", "a")

	// The new holder writes at the epoch it was granted.
	newer := store.NewBatch(singleWorkerEpoch)
	newer.PutFenced(key, []byte(`{"cursor":"new"}`), 0, 9)
	if err := st.Set(ctx, *newer); err != nil {
		t.Fatalf("the new holder's write was refused: %v", err)
	}

	// The old one, still believing it holds the lane, writes at its own.
	//
	// Its batch default is HIGH on purpose. At singleWorkerEpoch the default is below the stored
	// epoch, so a store that (wrongly) compares the batch's number refuses this too and the test
	// passes without ever exercising the per-key path.
	older := store.NewBatch(100)
	older.PutFenced(key, []byte(`{"cursor":"stale"}`), 1, 3)
	err := st.Set(ctx, *older)
	if err == nil {
		t.Fatal("the store accepted a write fenced at epoch 3 after one at epoch 9")
	}
	if !errors.Is(err, fault.ErrFenced) && fault.ClassOf(err) != fault.Fenced {
		t.Errorf("the store refused with %v, which does not classify as fenced", err)
	}

	got, err := st.Get(ctx, []store.Key{key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var body map[string]string
	if err := json.Unmarshal(got[key.String()].Value, &body); err != nil {
		t.Fatalf("decoding the stored row: %v", err)
	}
	if body["cursor"] != "new" {
		t.Errorf("the stored cursor is %q; the superseded worker overwrote the holder's", body["cursor"])
	}
}

// THE EPOCH THAT FENCED THE WRITE IS THE EPOCH THE ROW RECORDS.
//
// Two numbers that could disagree and must not: the one handed to store.Batch.PutFenced, which the store
// compares, and the one written into the lane row, which is the only durable record of who produced the
// cursor. Stamping the row from anything else — the batch default, or whatever the worker happens to hold
// when a checkpoint is taken — attributes a cursor to a lease that never saw it, and a restart then reads
// back progress under the wrong owner with nothing to contradict it.
//
// Asserted against the ENCODED row rather than the in-memory struct, because the row is what survives.
func TestTheRowRecordsTheEpochThatFencedItsWrite(t *testing.T) {
	f := newFenceFixture(t, []record.LaneID{"a", "b"}, map[record.LaneID]uint64{"a": 3, "b": 9})

	batch := store.NewBatch(singleWorkerEpoch)
	done, err := f.ctl.stage(batch, map[record.LaneID]record.Position{
		"a": {Token: record.Blob{Version: 1, Bytes: []byte{1}}, Safe: true},
		"b": {Token: record.Blob{Version: 1, Bytes: []byte{2}}, Safe: true},
	})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	done(true)

	for lane, want := range map[record.LaneID]uint64{"a": 3, "b": 9} {
		k := f.laneKey(lane)
		fencedAt, ok := epochOf(*batch, k)
		if !ok {
			t.Errorf("lane %s is not in the batch at all", lane)
			continue
		}
		v := batch.Writes[k.String()]
		var row laneRecord
		if err := json.Unmarshal(v.Value, &row); err != nil {
			t.Errorf("lane %s's row does not decode: %v", lane, err)
			continue
		}
		if fencedAt != want {
			t.Errorf("lane %s's write is fenced at %d, want %d", lane, fencedAt, want)
		}
		if row.CursorEpoch != want {
			t.Errorf("lane %s's row records epoch %d while its write is fenced at %d (want %d).\n"+
				"  The row is the only durable record of which lease produced this cursor, so a "+
				"disagreement here is a restart reading progress under the wrong owner",
				lane, row.CursorEpoch, fencedAt, want)
		}
	}
}

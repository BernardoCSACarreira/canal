package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/config"
	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// This file is the core-implemented half of the connector contract: everything a connector reaches
// for through SourceRuntime and SinkRuntime. A connector author writes four methods; the engine owns
// the rest, and "the rest" is this.
//
// SCOPE, stated plainly (design rule R10). This is the SINGLE-WORKER runtime. Lane assignment is
// local: this process announces a lane, holds it, and nothing can take it away, so the lease epoch
// is a constant. The multi-worker path — store.Coordinator, leases that expire, a planner running
// Reconcile on a timer, lanes moving between workers — is not built, and the places that would
// change are marked. Nothing here PRETENDS to be coordinated: laneCtl.Revoked always answers false
// because in a single process it truthfully never happens, rather than because the check is missing.

// A LANE'S DURABLE WRITES CARRY THAT LANE'S OWN LEASE EPOCH, which is what makes the state store —
// and not merely the coordinator — the thing that separates two workers racing on one lane.
//
// This used to be a STILL-TO-DO here: every write went out at singleWorkerEpoch, so a worker whose
// lease had moved on was stopped by the engine's own checks and by nothing underneath them. The store
// has enforced per-key epochs the whole time (wal rejects a write below the highest epoch seen for a
// key, and declares EpochFencing) and was simply never fed the number.
//
// It is per-call-site rather than one batch epoch, because a batch here spans every lane whose prefix
// advanced this tick and each is held under its own lease. Where each site landed, and why:
//
//   - stage, the CURSOR — PutFenced per lane, and a lane this worker has lost is DROPPED from the
//     batch rather than failing it. This is the write the whole fencing story is about.
//   - mutate, the LANE ROW — PutFenced, and refuses. Single-lane, so there is nothing to drop.
//   - stateHandle.Set and SetMany — PutFenced per lane. Set refuses; SetMany refuses for the whole
//     call, because a connector asked for one atomic write and a subset breaks that promise.
//   - announceMany — the batch default, because the lane does not exist yet so nobody holds a lease
//     over it. Idempotent on a derived id, so there is no race to lose.
//   - stateHandle.SetShared — the batch default. Node state belongs to no lane.
//
// STILL TO DO: Delete is unfenceable through StateStore.Delete, which takes bare keys and no epoch.

// singleWorkerEpoch is the epoch a lane is held under when there is no coordinator, and the DEFAULT
// fence on every batch — the value that applies to keys which are not a single lane's.
//
// It is 1 rather than 0 because store.Versioned treats a zero epoch as "use the batch's", and
// because a real coordinator will hand out increasing epochs from 1 — so the first coordinated
// deployment does not have to reason about a store full of rows fenced at zero.
const singleWorkerEpoch = 1

// laneRecord is one lane's durable row: its write-once construction spec and its write-many cursor,
// which are two differently-lifetimed things on one value.
//
// The engine owns this encoding. store.LaneRow exists for the COORDINATOR's view of a lane — what a
// planner needs to assign work — and says in its own doc that the engine owns the spec's codec and
// version while the store moves bytes. This is that codec.
type laneRecord struct {
	Spec   connector.LaneSpec `json:"spec"`
	Cursor record.Position    `json:"cursor"`

	Finished   bool      `json:"finished,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// laneCtl implements connector.LaneCtl over the state store.
//
// Announce is DURABLE BEFORE IT RETURNS, which linefile's own comment relies on and which is the
// difference between a lane that survives a restart and one that is re-announced with a fresh
// identity every time. record.DeriveLaneID makes the identity a function of (tenant, pipeline, node,
// name), so the same name is the same lane and finds its own cursor.
type laneCtl struct {
	mu sync.Mutex

	// writeMu serialises DURABLE WRITES to the lane table, separately from mu which guards the
	// in-memory view.
	//
	// A compare-and-set version read under mu goes stale the moment another writer commits, and mu
	// cannot simply be held across the store call because that would block every reader for the
	// duration of an fsync. Two writers exist in practice — the flush loop persisting cursors and the
	// read loop finishing a bounded lane — and without this they raced: "lane row is at version 3,
	// not 2", found by running the suite under -race with -count=3.
	writeMu sync.Mutex

	deps   Deps
	fields specRefFields
	node   record.NodeID
	lanes  map[record.LaneID]*laneRecord
	order  []record.LaneID

	// versions carries each row's compare-and-set version, so a write that raced another writer is
	// refused by the store rather than silently overwriting. Zero means "must not exist", which is
	// what a first Announce needs.
	versions map[record.LaneID]uint64

	// changes is closed-and-replaced when the lane table moves, so a source can select on it. In a
	// single-worker runtime the only thing that moves it is this process announcing or finishing.
	changes chan struct{}

	ledgerFor func(id record.LaneID, ord connector.Ordering, budget int) error
	admission func(id record.LaneID) connector.Admission

	// afterAnnounce plans and claims the lanes that have just appeared, and it is called from
	// Announce rather than on a timer for a reason every source in this module demonstrates: a source
	// announces its lanes inside Open and then immediately asks Assigned. Assigned now answers "the
	// lanes you hold a lease on", so a claim that waited for the next tick would tell a source it has
	// nothing to read — and a source that believes that announces them all over again.
	//
	// Nil for a worker with no coordinator, where a lane is held from the moment it is announced.
	afterAnnounce func(context.Context)

	// leases is this worker's claims, or a table with no coordinator behind it. Every question this
	// type answers about WHO HOLDS a lane goes through it; every question about what a lane IS comes
	// from the durable row. See lease.go.
	leases *leaseTable
}

// specRefFields is the slice of the pipeline spec the runtime needs, kept narrow so the runtime does
// not close over the whole spec and grow a dependency on parts of it unrelated to lanes.
type specRefFields struct {
	tenant   record.TenantID
	pipeline record.PipelineID
	budget   int
}

func newLaneCtl(deps Deps, f specRefFields, node record.NodeID,
	ledgerFor func(record.LaneID, connector.Ordering, int) error,
	admission func(record.LaneID) connector.Admission,
	leases *leaseTable,
) *laneCtl {
	return &laneCtl{
		leases:    leases,
		deps:      deps,
		node:      node,
		fields:    f,
		lanes:     map[record.LaneID]*laneRecord{},
		versions:  map[record.LaneID]uint64{},
		changes:   make(chan struct{}),
		ledgerFor: ledgerFor,
		admission: admission,
	}
}

// load reads every lane row this node already owns, so Assigned can answer a warm start.
func (l *laneCtl) load(ctx context.Context) error {
	prefix := store.Key{
		Tenant: l.fields.tenant,
		Space:  store.SpaceLane,
		Parts:  []string{string(l.fields.pipeline)},
	}
	seq, err := l.deps.State.Range(ctx, prefix)
	if err != nil {
		return fmt.Errorf("engine: reading the lane table: %w", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	for k, v := range seq {
		var rec laneRecord
		if err := json.Unmarshal(v.Value, &rec); err != nil {
			// A lane row that does not parse is not something to skip: it is state this build
			// cannot interpret, and continuing would silently re-read the whole lane from scratch.
			return fault.Contract(fault.OpOpen,
				fmt.Errorf("engine: lane row %s is unreadable by this build: %w", k, err))
		}
		id := record.LaneID(k.Parts[len(k.Parts)-1])
		l.lanes[id] = &rec
		l.versions[id] = v.Version
		l.order = append(l.order, id)
	}

	// Every loaded lane must be registered with the ledger, exactly as Announce does for a new one.
	//
	// This is the WARM-START path and it had no coverage until a resume test existed: a source that
	// finds its lane through Assigned never calls Announce, so nothing told the ledger the lane was
	// there, and the first Admit after a restart failed with "lane is not registered". The cold path
	// worked, which is why it looked fine.
	for _, id := range l.order {
		rec := l.lanes[id]
		budget := rec.Spec.Budget
		if budget == 0 {
			budget = l.fields.budget
		}
		if err := l.ledgerFor(id, rec.Spec.Ordering, budget); err != nil {
			return err
		}
	}
	return nil
}

// Announce registers a lane and makes it durable before returning.
func (l *laneCtl) Announce(ctx context.Context, s connector.LaneSpec) (record.LaneID, error) {
	ids, err := l.AnnounceMany(ctx, []connector.LaneSpec{s})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// AnnounceMany registers several lanes in ONE durable write.
//
// One write rather than N: a source announcing 900 streams should not pay 900 fsyncs, and a partial
// announcement is a lane table that disagrees with itself.
func (l *laneCtl) AnnounceMany(ctx context.Context, specs []connector.LaneSpec) ([]record.LaneID, error) {
	ids, err := l.announceMany(ctx, specs)
	if err != nil || l.afterAnnounce == nil {
		return ids, err
	}

	// PLANNED AND CLAIMED OUTSIDE writeMu, which is the point of this wrapper existing at all.
	// afterAnnounce talks to the coordinator, and writeMu serialises DURABLE WRITES to the lane
	// table — so calling it inside meant a slow control plane blocked the flush loop persisting
	// cursors. lease.go opens by saying every failure there is inert on the data path; holding the
	// durability lock across a coordinator round trip is exactly how that stops being true.
	l.afterAnnounce(ctx)
	return ids, nil
}

// announceMany does the durable half, under writeMu.
func (l *laneCtl) announceMany(ctx context.Context, specs []connector.LaneSpec) ([]record.LaneID, error) {
	if len(specs) == 0 {
		return nil, nil
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.mu.Lock()

	// ANNOUNCE IS THE ONE LANE WRITE THAT CANNOT BE FENCED BY A LANE'S LEASE, because the lane does
	// not exist yet and therefore nobody holds one. A planner assigns from the durable lane table, so
	// the row has to be there before there is anything to grant a lease over; fencing this write on a
	// lease would be waiting for a token that only its own completion can produce.
	//
	// Safe, and not merely unavoidable. The id is DERIVED from tenant, pipeline, node and the
	// connector's own lane name, so two workers announcing the same lane compute the same id and the
	// write is idempotent on it — there is no race for one of them to lose. What a lease protects is
	// the CURSOR and the retirement, which are writes about a lane's progress rather than its
	// existence, and both of those are fenced. See stage and mutate.
	batch := store.NewBatch(singleWorkerEpoch)
	ids := make([]record.LaneID, 0, len(specs))
	fresh := make([]*laneRecord, 0, len(specs))

	for _, s := range specs {
		if s.Name == "" {
			l.mu.Unlock()
			return nil, fault.Contract(fault.OpOpen,
				fmt.Errorf("engine: a lane spec has no Name; the lane id is derived from it, so an empty one makes every restart a different lane"))
		}
		id := record.DeriveLaneID(l.fields.tenant, l.fields.pipeline, l.node, s.Name)
		ids = append(ids, id)

		if _, known := l.lanes[id]; known {
			// Announce is idempotent on the lane name, which is what lets Open be retried.
			fresh = append(fresh, nil)
			continue
		}
		rec := &laneRecord{Spec: s}
		fresh = append(fresh, rec)

		body, err := json.Marshal(rec)
		if err != nil {
			l.mu.Unlock()
			return nil, fmt.Errorf("engine: encoding lane %s: %w", id, err)
		}
		batch.Put(store.LaneKey(l.fields.tenant, l.fields.pipeline, id), body, 0)
	}
	l.mu.Unlock()

	if batch.Len() > 0 {
		if err := l.deps.State.Set(ctx, *batch); err != nil {
			return nil, err
		}
	}

	l.mu.Lock()
	for i, id := range ids {
		if fresh[i] != nil {
			l.lanes[id] = fresh[i]
			// A write that presented IfVersion 0 and succeeded left the row at version 1. Tracking
			// this is not bookkeeping for its own sake: the next write presents it, and presenting a
			// stale version is how two writers silently overwrite each other.
			l.versions[id] = 1
			l.order = append(l.order, id)
		}
	}
	l.mu.Unlock()

	// The ledger has to know about a lane before anything can be admitted to it.
	for i, id := range ids {
		s := specs[i]
		budget := s.Budget
		if budget == 0 {
			budget = l.fields.budget
		}
		if err := l.ledgerFor(id, s.Ordering, budget); err != nil {
			return nil, err
		}
	}

	l.notify()
	return ids, nil
}

// Assigned returns the lanes this worker holds and may read.
//
// A finished lane is omitted, which is the discriminator linefile uses: an empty result is a cold
// start, a non-empty one is a warm start carrying the durable cursor.
func (l *laneCtl) Assigned(context.Context) ([]connector.LaneAssignment, error) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]connector.LaneAssignment, 0, len(l.order))
	for _, id := range l.order {
		rec := l.lanes[id]
		if rec.Finished {
			continue
		}
		// A LANE THIS WORKER DOES NOT HOLD IS NOT OFFERED. In a standalone run holds() is true for
		// everything and this changes nothing; with a coordinator it is the difference between a
		// source reading its own lanes and a source reading somebody else's.
		if !l.leases.holds(id, now) {
			continue
		}
		if gated := l.gatedOnLocked(rec); len(gated) > 0 {
			// StartAfter is enforced from the durable table, so a lane whose predecessors are not
			// finished is not handed out at all. This is the snapshot-to-stream handoff, and it
			// holds because the gate is data rather than connector convention.
			continue
		}
		out = append(out, connector.LaneAssignment{
			ID:     id,
			Spec:   rec.Spec,
			Cursor: rec.Cursor,
			Epoch:  l.leases.epochFor(id),
			Worker: string(l.deps.Worker),
		})
	}
	return out, nil
}

// Table returns every lane, including the ones Assigned filters out.
func (l *laneCtl) Table(_ context.Context, q connector.LaneQuery) ([]connector.LaneAssignment, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]connector.LaneAssignment, 0, len(l.order))
	for _, id := range l.order {
		rec := l.lanes[id]
		if rec.Finished && !q.IncludeFinished {
			continue
		}
		if len(q.Groups) > 0 && !containsAny(q.Groups, rec.Spec.Group) {
			continue
		}
		if len(q.Streams) > 0 && !containsAny(q.Streams, rec.Spec.Stream) {
			continue
		}
		if len(q.Kinds) > 0 && !containsAny(q.Kinds, rec.Spec.Kind) {
			continue
		}
		if !q.IncludeGated && len(l.gatedOnLocked(rec)) > 0 {
			continue
		}
		out = append(out, connector.LaneAssignment{
			ID: id, Spec: rec.Spec, Cursor: rec.Cursor,
			Epoch: l.leases.epochFor(id), Worker: string(l.deps.Worker),
			Finished: rec.Finished, FinishedAt: rec.FinishedAt,
			GatedOn: l.gatedOnLocked(rec),
		})
	}
	return out, nil
}

// gatedOnLocked names the groups rec is still waiting for. The caller holds the mutex.
func (l *laneCtl) gatedOnLocked(rec *laneRecord) []record.LaneGroup {
	if len(rec.Spec.StartAfter) == 0 {
		return nil
	}
	var waiting []record.LaneGroup
	for _, g := range rec.Spec.StartAfter {
		for _, other := range l.lanes {
			if other.Spec.Group != g {
				continue
			}
			// DURABLY finished, not merely finished: a gate that opens on a fact that did not
			// survive a crash is a gate that opens twice.
			if !other.Finished || other.FinishedAt.IsZero() {
				waiting = append(waiting, g)
				break
			}
		}
	}
	return waiting
}

// Seed sets a lane's starting cursor before it has read anything.
func (l *laneCtl) Seed(ctx context.Context, id record.LaneID, cursor record.Position) error {
	return l.mutate(ctx, id, func(rec *laneRecord) error {
		if rec.Cursor.Token.IsZero() {
			rec.Cursor = cursor
			return nil
		}
		return fault.Contract(fault.OpOpen,
			fmt.Errorf("engine: lane %s has already made progress; Seed only sets a STARTING cursor", id))
	})
}

// Finish marks a lane complete and durable.
func (l *laneCtl) Finish(ctx context.Context, id record.LaneID) error {
	err := l.mutate(ctx, id, func(rec *laneRecord) error {
		rec.Finished = true
		// Stamped by the core at the moment the write is made durable, so FinishedAt means "this
		// survived" rather than "we intended it".
		rec.FinishedAt = time.Now().UTC()
		return nil
	})
	if err == nil {
		l.notify() // a gate may now be open
	}
	return err
}

// Forget removes a lane and its durable row.
func (l *laneCtl) Forget(ctx context.Context, id record.LaneID) error {
	l.mu.Lock()
	if _, ok := l.lanes[id]; !ok {
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()

	if err := l.deps.State.Delete(ctx, []store.Key{
		store.LaneKey(l.fields.tenant, l.fields.pipeline, id),
	}); err != nil {
		return err
	}

	l.mu.Lock()
	delete(l.lanes, id)
	for i, x := range l.order {
		if x == id {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
	l.mu.Unlock()
	l.notify()
	return nil
}

// commit records a lane's durably-committed cursor. Called by the engine at phase two, never by a
// connector.
func (l *laneCtl) commit(ctx context.Context, positions map[record.LaneID]record.Position) error {
	if len(positions) == 0 {
		return nil
	}
	batch := store.NewBatch(singleWorkerEpoch)
	done, err := l.stage(batch, positions)
	if err != nil {
		done(false)
		return err
	}
	if batch.Len() == 0 {
		done(false)
		return nil
	}
	if err := l.deps.State.Set(ctx, *batch); err != nil {
		done(false)
		return err
	}
	done(true)
	return nil
}

// stage adds this node's lane rows to a batch and returns the function that finishes the write.
//
// It exists so a CHECKPOINT AND THE CURSORS IT COVERS GO INTO ONE store.Batch. A committable
// persisted separately from the cursor it covers can be orphaned by a crash between the two writes,
// which is the failure the checkpoint envelope exists to prevent — so the batch has to be assembled
// above the per-node lane tables rather than inside them.
//
// The returned func MUST be called: it holds this node's write lock until it is. Pass true when the
// batch landed and false when it did not, which is the difference between advancing the in-memory
// view and leaving it alone.
func (l *laneCtl) stage(batch *store.Batch, positions map[record.LaneID]record.Position) (func(bool), error) {
	l.writeMu.Lock()

	now := time.Now()
	l.mu.Lock()
	staged := make(map[record.LaneID]record.Position, len(positions))
	for id, pos := range positions {
		rec, ok := l.lanes[id]
		if !ok {
			continue
		}

		// THE CURSOR IS THE WRITE THE WHOLE FENCING STORY IS ABOUT, so it carries the lane's OWN
		// lease epoch rather than the batch's default. A batch here spans every lane whose prefix
		// advanced this tick, each held under its own lease at its own epoch, and one number for all
		// of them is unfenceable in both directions: high enough for the lanes still held is high
		// enough for the ones already lost.
		//
		// A LANE THIS WORKER NO LONGER HOLDS IS DROPPED FROM THE BATCH, not written with a number
		// that would land. Dropping rather than failing, because the batch is atomic and the other
		// lanes' cursors are this worker's to commit — failing the write would let one reclaimed lane
		// stall every other lane's progress, which is the blast radius store.Lease's doc rules out:
		// "the loser's lane — not its whole process — is revoked".
		//
		// Reaching this at all means an earlier fence let it through. Ledger.Flushable does not offer
		// a revoked lane, so a position for one should never arrive here — which is why it is logged
		// rather than passed over in silence.
		epoch, mine := l.leases.fenceFor(id, now)
		if !mine {
			l.deps.Log.Warn("not persisting a cursor for a lane this worker no longer holds",
				"node", l.node, "lane", id)
			continue
		}

		next := *rec
		next.Cursor = pos
		body, err := json.Marshal(&next)
		if err != nil {
			l.mu.Unlock()
			l.writeMu.Unlock()
			return func(bool) {}, fmt.Errorf("engine: encoding lane %s: %w", id, err)
		}
		batch.PutFenced(store.LaneKey(l.fields.tenant, l.fields.pipeline, id), body, l.versions[id], epoch)
		staged[id] = pos
	}
	l.mu.Unlock()

	return func(landed bool) {
		defer l.writeMu.Unlock()
		if !landed {
			return
		}
		l.mu.Lock()
		for id, pos := range staged {
			l.lanes[id].Cursor = pos
			l.versions[id]++
		}
		l.mu.Unlock()
	}, nil
}

// mutate applies f to one lane row and persists the result.
func (l *laneCtl) mutate(ctx context.Context, id record.LaneID, f func(*laneRecord) error) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.mu.Lock()
	rec, ok := l.lanes[id]
	if !ok {
		l.mu.Unlock()
		return fault.Contract(fault.OpPersist, fmt.Errorf("engine: lane %s was never announced", id))
	}
	next := *rec
	if err := f(&next); err != nil {
		l.mu.Unlock()
		return err
	}
	body, err := json.Marshal(&next)
	l.mu.Unlock()
	if err != nil {
		return fmt.Errorf("engine: encoding lane %s: %w", id, err)
	}

	// Fenced by the lane's own lease, like the cursor: retiring a lane and gating another on it are
	// writes on that lane's behalf, and a worker that has lost it has no business making either.
	// Unlike the cursor this is a SINGLE-lane write, so there is nothing to drop — refusing is the
	// whole answer, and fault.ErrFenced is what the state store itself would have returned.
	epoch, mine := l.leases.fenceFor(id, time.Now())
	if !mine {
		return fault.New(fault.Fenced, fault.OpPersist,
			fmt.Errorf("engine: lane %s is no longer held by this worker", id))
	}

	batch := store.NewBatch(singleWorkerEpoch)
	batch.PutFenced(store.LaneKey(l.fields.tenant, l.fields.pipeline, id), body, l.versionOf(id), epoch)
	if err := l.deps.State.Set(ctx, *batch); err != nil {
		return err
	}

	l.mu.Lock()
	*l.lanes[id] = next
	l.versions[id]++
	l.mu.Unlock()
	return nil
}

// Changes reports when the lane table moves.
func (l *laneCtl) Changes() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.changes
}

func (l *laneCtl) notify() {
	l.mu.Lock()
	close(l.changes)
	l.changes = make(chan struct{})
	l.mu.Unlock()
}

// Revoked reports whether this worker has lost a lane.
//
// FALSE FOR A STANDALONE RUN, TRUTHFULLY: with no coordinator there is no lease and no other worker,
// so nothing can reclaim anything. With one, this is the lease table's answer, and the connectors
// that were already checking it keep working unchanged — which is what they were checking for.
func (l *laneCtl) Revoked(id record.LaneID) bool { return !l.leases.holds(id, time.Now()) }

// laneRows projects the durable lane table as the coordinator sees it.
//
// THE SPEC IS OPAQUE TO THE STORE, which store.LaneRow says in its own doc: the engine owns the
// codec and the version, and the coordinator moves bytes. So the row carries identity, gating and
// finish state — everything a planner needs to decide placement — and the connector's LaneSpec stays
// on this side of the seam.
func (l *laneCtl) laneRows() []store.LaneRow {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]store.LaneRow, 0, len(l.order))
	for _, id := range l.order {
		rec := l.lanes[id]
		out = append(out, store.LaneRow{
			ID: id, Name: rec.Spec.Name, Group: rec.Spec.Group, After: rec.Spec.StartAfter,
			Bounded:    rec.Spec.Boundedness == connector.Bounded,
			Finished:   rec.Finished,
			FinishedAt: rec.FinishedAt,
			Weight:     rec.Spec.Weight,
		})
	}
	return out
}

// Admission reports the lane's in-flight budget and headroom.
func (l *laneCtl) Admission(id record.LaneID) connector.Admission {
	if l.admission == nil {
		return connector.Admission{}
	}
	return l.admission(id)
}

// --- state handle ---------------------------------------------------------------

// stateHandle implements connector.StateHandle over the state store.
type stateHandle struct {
	deps     Deps
	tenant   record.TenantID
	pipeline record.PipelineID
	node     record.NodeID

	// leases fences the LANE-scoped writes. A connector's per-lane state is written on a lane's
	// behalf exactly as its cursor is, so it earns the same epoch and the same refusal; the
	// node-shared value is not a lane's and gets neither. See set and SetShared.
	leases *leaseTable
}

func (s *stateHandle) key(lane record.LaneID) store.Key {
	return store.ConnectorKey(s.tenant, s.pipeline, s.node, lane)
}

func (s *stateHandle) Get(ctx context.Context, lane record.LaneID) (record.Blob, uint64, error) {
	return s.get(ctx, s.key(lane))
}

func (s *stateHandle) Shared(ctx context.Context) (record.Blob, uint64, error) {
	return s.get(ctx, store.ConnectorNodeKey(s.tenant, s.pipeline, s.node))
}

func (s *stateHandle) get(ctx context.Context, k store.Key) (record.Blob, uint64, error) {
	m, err := s.deps.State.Get(ctx, []store.Key{k})
	if err != nil {
		return record.Blob{}, 0, err
	}
	v, ok := m[k.String()]
	if !ok {
		return record.Blob{}, 0, nil
	}
	var b record.Blob
	if err := json.Unmarshal(v.Value, &b); err != nil {
		return record.Blob{}, 0, fault.Contract(fault.OpOpen,
			fmt.Errorf("engine: connector state at %s is unreadable: %w", k, err))
	}
	return b, v.Version, nil
}

// Set writes ONE lane's connector state, fenced by that lane's lease.
func (s *stateHandle) Set(ctx context.Context, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error) {
	epoch, mine := s.leases.fenceFor(lane, time.Now())
	if !mine {
		return 0, fault.New(fault.Fenced, fault.OpPersist,
			fmt.Errorf("engine: lane %s is no longer held by this worker", lane))
	}
	return s.set(ctx, s.key(lane), b, ifVersion, epoch)
}

// SetShared writes the node's own state, which belongs to NO lane and is therefore fenced by none.
//
// A connector keeps a schema registry handle, a connection token, a discovered-topology snapshot
// here — state about the source rather than about a partition of it. There is no lease over it to
// fence with, and inventing one would refuse a write for a node whose lanes had all moved away while
// the node itself is still perfectly this worker's. The batch's default epoch is the honest answer,
// and it is why this is a separate method rather than Set with an empty lane.
func (s *stateHandle) SetShared(ctx context.Context, b record.Blob, ifVersion uint64) (uint64, error) {
	return s.set(ctx, store.ConnectorNodeKey(s.tenant, s.pipeline, s.node), b, ifVersion, 0)
}

// set writes one key. A zero epoch means the batch's default, which is store.Versioned's own rule.
func (s *stateHandle) set(ctx context.Context, k store.Key, b record.Blob, ifVersion, epoch uint64) (uint64, error) {
	body, err := json.Marshal(b)
	if err != nil {
		return 0, fmt.Errorf("engine: encoding connector state: %w", err)
	}
	batch := store.NewBatch(singleWorkerEpoch)
	batch.PutFenced(k, body, ifVersion, epoch)
	if err := s.deps.State.Set(ctx, *batch); err != nil {
		return 0, err
	}
	return ifVersion + 1, nil
}

// SetMany writes several lanes' state in ONE atomic write, each fenced by its own lane's lease.
//
// IT REFUSES RATHER THAN DROPPING, which is the opposite of what laneCtl.stage does with a lost lane,
// and the difference is who asked for the write. stage batches cursors the ENGINE decided to persist
// for whichever lanes advanced, so dropping one leaves the rest correct. This is a connector calling
// an interface whose entire purpose is that the whole set lands or none of it does — writing a subset
// would break the promise the method exists to make, and a connector that wanted per-lane granularity
// would have called Set per lane.
func (s *stateHandle) SetMany(ctx context.Context, w connector.StateWrite) error {
	now := time.Now()
	batch := store.NewBatch(singleWorkerEpoch)
	for lane, e := range w.Lanes {
		epoch, mine := s.leases.fenceFor(lane, now)
		if !mine {
			return fault.New(fault.Fenced, fault.OpPersist, fmt.Errorf(
				"engine: lane %s is no longer held by this worker, and this write is atomic over %d lanes",
				lane, len(w.Lanes)))
		}
		// THE CONNECTOR'S OWN EPOCH WINS WHEN IT SUPPLIES ONE, which is what connector.Write.Epoch
		// says: zero means the epoch the core records for the lane. Whether this worker may write at
		// all is still the core's answer above — a connector cannot name its way past a lease it
		// does not hold — but which epoch a held lane's entry carries is the caller's to state.
		if e.Epoch != 0 {
			epoch = e.Epoch
		}
		body, err := json.Marshal(e.Blob)
		if err != nil {
			return fmt.Errorf("engine: encoding connector state for lane %s: %w", lane, err)
		}
		batch.PutFenced(s.key(lane), body, e.IfVersion, epoch)
	}

	// THE SHARED SLOT TRAVELS IN THE SAME TRANSACTION, and until now it did not travel at all.
	//
	// StateWrite.Shared was read by nothing: a connector that set it got its lane entries written and
	// its node entry silently dropped. That is the exact failure SetMany's own doc gives as the reason
	// atomicity matters here — "a stream-to-lane index in the node slot and the lane cursors it
	// indexes must move together or a restart reads one stream's progress under another stream's
	// name" — so the one scenario the method was justified by was the one it did not serve.
	//
	// Fenced by the batch's default, like SetShared: the node slot belongs to no lane.
	if w.Shared != nil {
		body, err := json.Marshal(w.Shared.Blob)
		if err != nil {
			return fmt.Errorf("engine: encoding shared connector state: %w", err)
		}
		batch.PutFenced(store.ConnectorNodeKey(s.tenant, s.pipeline, s.node), body,
			w.Shared.IfVersion, w.Shared.Epoch)
	}

	if batch.Len() == 0 {
		return nil
	}
	return s.deps.State.Set(ctx, *batch)
}

// Delete removes one lane's connector state.
//
// IT IS THE ONE LANE MUTATION THE STORE CANNOT FENCE, because StateStore.Delete takes bare keys and
// no epoch — so there is no number to be refused on and the store will do as it is told. That makes
// the advisory check MORE important here rather than less: every other fenced operation degrades to
// a rejected write, and this one degrades to destroying state the new holder owns and is reading.
//
// So it refuses locally, which is all this side can do, and the gap is named rather than implied. A
// per-key epoch on Delete is a StateStore signature change.
func (s *stateHandle) Delete(ctx context.Context, lane record.LaneID) error {
	if _, mine := s.leases.fenceFor(lane, time.Now()); !mine {
		return fault.New(fault.Fenced, fault.OpPersist,
			fmt.Errorf("engine: lane %s is no longer held by this worker", lane))
	}
	return s.deps.State.Delete(ctx, []store.Key{s.key(lane)})
}

// --- runtimes -------------------------------------------------------------------

// baseRuntime is what both runtimes share.
type baseRuntime struct {
	ctx      context.Context
	deps     Deps
	tenant   record.TenantID
	pipeline record.PipelineID
	node     record.NodeID
	streams  []connector.ConfiguredStream
	cfg      *config.Config

	// component is the REGISTERED name, which is what namespaces this connector's metrics. The node
	// id would not do: two nodes running the same connector should share a series family, and one
	// connector deployed twice must not look like two different components.
	component string

	mu     sync.Mutex
	events []connector.Event
}

func (b *baseRuntime) Context() context.Context    { return b.ctx }
func (b *baseRuntime) Tenant() record.TenantID     { return b.tenant }
func (b *baseRuntime) Pipeline() record.PipelineID { return b.pipeline }
func (b *baseRuntime) Node() record.NodeID         { return b.node }

// Metrics hands the connector a namespaced view of the process registry.
//
// A connector can therefore record whatever it likes and can never collide with, shadow or
// overwrite a core metric: every name it uses becomes canal_connector_<component>_<name>.
//
// A deployment with no registry still gets a working handle — noopMetrics — because a connector that
// increments a counter must not be broken by the host's choice not to export.
func (b *baseRuntime) Metrics() connector.Metrics {
	if b.deps.Metrics == nil {
		return noopMetrics{}
	}
	return b.deps.Metrics.ForConnector(b.component)
}

func (b *baseRuntime) Schemas() connector.SchemaLookup { return noopSchemas{} }

func (b *baseRuntime) Streams() []connector.ConfiguredStream { return b.streams }

func (b *baseRuntime) Config(context.Context) (*config.Config, error) { return b.cfg, nil }

func (b *baseRuntime) Log() *slog.Logger {
	return b.deps.Log.With("tenant", b.tenant, "pipeline", b.pipeline, "node", b.node)
}

// Note records a connector-raised event, for the log and for the read model's RecentEvents.
//
// THE RING IS BOUNDED, and it has to be. This was an unbounded append fed by third-party code with
// no rate limit of any kind: a source noting one event per record grew it for the life of the
// process. Dropping the oldest is the right direction — a status document shows the LAST few events,
// and an operator reading it wants what just happened, not what happened an hour ago.
func (b *baseRuntime) Note(e connector.Event) {
	if e.At.IsZero() {
		// Stamped by the host when the connector did not. An event with no time sorts to the front of
		// the document and reads as the oldest thing that happened, which is the opposite of true.
		e.At = time.Now()
	}
	b.mu.Lock()
	b.events = append(b.events, e)
	if n := len(b.events); n > maxRetainedEvents {
		b.events = append(b.events[:0], b.events[n-maxRetainedEvents:]...)
	}
	b.mu.Unlock()
	b.Log().Info("connector event", "kind", e.Kind, "severity", e.Severity, "message", e.Message)
}

// maxRetainedEvents is how many events one component keeps for the read model.
const maxRetainedEvents = 64

// recent copies what this component has noted, for the status document.
func (b *baseRuntime) recent() []connector.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]connector.Event(nil), b.events...)
}

type sourceRuntime struct {
	baseRuntime
	lanes *laneCtl
	state *stateHandle
}

func (r *sourceRuntime) Lanes() connector.LaneCtl     { return r.lanes }
func (r *sourceRuntime) State() connector.StateHandle { return r.state }
func (r *sourceRuntime) Instance() string             { return string(r.deps.Worker) }

func (r *sourceRuntime) Declare(context.Context, schema.Change, *schema.Schema) (schema.Ref, error) {
	// The schema table is durable state this runtime does not write yet. Refusing is correct rather
	// than returning a reference that resolves to nothing: a source that declares a schema and gets
	// a dangling ref back would produce records nothing downstream can decode.
	return schema.Ref{}, fault.Contract(fault.OpOpen,
		fmt.Errorf("engine: schema declaration is not implemented in the single-worker runtime"))
}

func (r *sourceRuntime) Batcher(p config.BatchPolicy) *connector.Batcher {
	return connector.NewBatcher(p)
}

type sinkRuntime struct {
	baseRuntime
}

// --- placeholders ----------------------------------------------------------------

// noopMetrics satisfies connector.Metrics without recording anything.
//
// LABELLED SCAFFOLDING (R10). A connector that increments a counter is not broken by this, and the
// metric names it registers are still exercised, so wiring a real collector later changes only this
// type. What it must never do is pretend: nothing here is exported, scraped or shown.
type noopMetrics struct{}

func (noopMetrics) Counter(string, ...string) (connector.Counter, error) {
	return noopInstrument{}, nil
}
func (noopMetrics) Gauge(string, ...string) (connector.Gauge, error) { return noopInstrument{}, nil }
func (noopMetrics) Histogram(string, []float64, ...string) (connector.Histogram, error) {
	return noopInstrument{}, nil
}

type noopInstrument struct{}

func (noopInstrument) Add(float64, ...string)     {}
func (noopInstrument) Set(float64, ...string)     {}
func (noopInstrument) Observe(float64, ...string) {}

// noopSchemas satisfies connector.SchemaLookup with an empty table.
//
// LABELLED SCAFFOLDING (R10). It returns not-found rather than a wrong schema, so a codec that
// resolves a reference fails honestly instead of decoding against something invented.
type noopSchemas struct{}

func (noopSchemas) Get(_ context.Context, ref schema.Ref) (*schema.Schema, error) {
	return nil, fault.Contract(fault.OpOpen,
		fmt.Errorf("engine: no schema table in the single-worker runtime; %v cannot be resolved", ref))
}

// versionOf is the compare-and-set version the next write to id must present.
func (l *laneCtl) versionOf(id record.LaneID) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.versions[id]
}

// containsAny reports whether want appears in have.
func containsAny[T comparable](have []T, want T) bool {
	for _, h := range have {
		if h == want {
			return true
		}
	}
	return false
}

// Register would mint a reference for a new schema.
//
// It refuses for the same reason Get does: a ref handed back by a table that does not persist would
// dangle the moment the process restarts, and a codec holding it would decode against nothing.
func (noopSchemas) Register(context.Context, *schema.Schema) (schema.Ref, error) {
	return schema.Ref{}, fault.Contract(fault.OpOpen,
		fmt.Errorf("engine: no schema table in the single-worker runtime; a schema cannot be registered"))
}

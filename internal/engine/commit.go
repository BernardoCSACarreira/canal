package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// THE TWO-PHASE COMMIT, and the first code in this module to construct an [Checkpoint].
//
// A [connector.Committer] stages data on Write and publishes it on Commit, so its records are not
// durable until a commit succeeds — one step further out than a [connector.Flusher], and the reason
// its ack point is "commit". The protocol, in the only order that is safe:
//
//  1. PrepareCommit mints committables covering everything the sink has staged. The engine stamps
//     the node and the durable blast radius onto each one.
//  2. THE CHECKPOINT IS WRITTEN, atomically, carrying both the committables and the lane cursors
//     they cover. This is the step that makes a lost confirmation self-repairing.
//  3. Commit publishes them. Only now are the records durable, and only now are they settled.
//
// STEP TWO BEFORE STEP THREE IS THE WHOLE POINT. A crash between them leaves a committable in the
// checkpoint that the destination may or may not have published — and recovery hands it straight
// back to the sink, which answers already_committed or commits it. A crash with the order reversed
// leaves a published artifact nothing records, and the next checkpoint advances a cursor past it.
//
// ONE ATOMIC WRITE, ACROSS EVERY SOURCE NODE. The cursors live in per-lane rows and the committables
// live in the checkpoint record, and they go into ONE store.Batch — which the WAL's AtomicMultiKey
// makes a single frame. Writing them separately would let a crash orphan a committable behind a
// cursor that moved without it, which is the one thing the checkpoint envelope exists to prevent.

// checkpointer owns the monotonic checkpoint id and the subsuming pending set.
type checkpointer struct {
	mu sync.Mutex

	// id is the last checkpoint written. A higher id SUBSUMES every lower one.
	id uint64

	// pending is the committables minted at each checkpoint and not yet confirmed committed,
	// keyed by the checkpoint that minted them. It is persisted INSIDE the checkpoint, which is
	// what lets a restart find work the previous process left in doubt.
	pending map[uint64][]connector.Committable

	// writerState is each Committer sink's own in-progress work, keyed by node — an open multipart
	// upload, a staging table name. Keyed, because one unkeyed slice handed every sink every other
	// sink's blobs at RestoreState.
	writerState map[record.NodeID][]record.Blob
}

func newCheckpointer() *checkpointer {
	return &checkpointer{
		pending:     map[uint64][]connector.Committable{},
		writerState: map[record.NodeID][]record.Blob{},
	}
}

// next reserves the next checkpoint id.
func (c *checkpointer) next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id++
	return c.id
}

// committerSinks returns the sink nodes that publish through commit, in a stable order.
func (r *runner) committerSinks() []record.NodeID {
	var out []record.NodeID
	for _, id := range r.mainSinks {
		if commitsDurability(r.p.sinks[id]) {
			out = append(out, id)
		}
	}
	return out
}

// prepare mints the committables for one checkpoint, stamping what the engine owns.
//
// Node, Lanes and Cursors are the ENGINE's to fill, never the connector's. A recovered committable
// with no author cannot be routed back to the sink that can commit it — Commit has no "not mine"
// answer — and one with no cursor span cannot say what a failed commit is withholding.
func (r *runner) prepare(ctx context.Context, point connector.CommitPoint) ([]connector.Committable, error) {
	var all []connector.Committable
	for _, id := range r.committerSinks() {
		sk := r.p.sinks[id]

		covered := r.deferred.takeCovered(id)
		held := r.deferred.take(id)

		cs, err := sandbox(ctx, r.p.obs, id, sk.Name, point,
			func(c context.Context, p connector.CommitPoint) ([]connector.Committable, error) {
				return sk.Committer.PrepareCommit(c, p)
			})
		if err != nil {
			// Nothing was minted, so the records stay held and the checkpoint carries no committable
			// for this sink. They are re-offered at the next point.
			r.deferred.returnUnflushed(id, held)
			r.p.obs.fault(id, err)
			return nil, fmt.Errorf("engine: preparing a commit for sink %s: %w", id, err)
		}
		if len(cs) == 0 {
			// A sink with nothing staged mints nothing, which is normal on an idle tick. Anything it
			// was holding stays held.
			r.deferred.returnUnflushed(id, held)
			continue
		}

		for i := range cs {
			cs[i].Checkpoint = point.ID
			cs[i].Node = id
			cs[i].Lanes = slices.Sorted(maps.Keys(covered))
			cs[i].Cursors = covered
			if cs[i].Records == 0 {
				cs[i].Records = int64(len(held))
			}
		}
		all = append(all, cs...)

		// The records are now the committables' responsibility. They are NOT settled — that waits
		// for a successful commit — but they are no longer the deferred set's to re-offer, because
		// re-offering them would stage them twice.
		r.awaiting.hold(point.ID, id, held)
	}
	return all, nil
}

// publish runs step three: Commit, then settlement for whatever landed.
func (r *runner) publish(ctx context.Context, id uint64, cs []connector.Committable) error {
	byNode := map[record.NodeID][]connector.Committable{}
	for _, c := range cs {
		byNode[c.Node] = append(byNode[c.Node], c)
	}

	for _, node := range slices.Sorted(maps.Keys(byNode)) {
		sk := r.p.sinks[node]
		mine := byNode[node]

		outs, err := sandbox(ctx, r.p.obs, node, sk.Name, mine,
			func(c context.Context, in []connector.Committable) ([]connector.CommitOutcome, error) {
				return sk.Committer.Commit(c, in)
			})
		if err != nil {
			// IN DOUBT, and deliberately left that way. The committables stay in the pending set and
			// the records stay unsettled, so the next checkpoint offers them again and recovery
			// finds them if this process dies. Settling here would publish nothing and advance
			// everything; abandoning here would discard data the destination may already hold.
			r.p.obs.fault(node, err)
			r.deps.Log.Error("a commit failed; its committables stay pending and its records unsettled",
				"node", node, "checkpoint", id, "committables", len(mine), "error", err)
			return fmt.Errorf("engine: committing at sink %s: %w", node, err)
		}
		if err := r.applyOutcomes(ctx, node, id, mine, outs); err != nil {
			return err
		}
	}
	return nil
}

// applyOutcomes turns the sink's per-committable answers into settlement.
//
// Five dispositions and NONE of them silently discards data, which is the property that separates
// this from a commit path that "logs the error and continues".
func (r *runner) applyOutcomes(ctx context.Context, node record.NodeID, id uint64,
	sent []connector.Committable, outs []connector.CommitOutcome,
) error {
	answered := map[string]connector.CommitOutcome{}
	for _, o := range outs {
		answered[blobKey(o.Handle)] = o
	}

	// THE WHOLE BATCH IS CLASSIFIED BEFORE ANYTHING IS ACTED ON, because one committable's answer
	// changes what another one means.
	//
	// A sink folding a re-minted committable together with the shorter one it subsumes answers
	// Aborted for the loser — whose documented meaning is "as if never triggered, artifacts
	// retained", NOT "these records are gone". Acting on it in isolation discarded the records the
	// WINNER covers, so they were never settled, the prefix never advanced, and the drain timed out.
	// It only reproduced under -race, because it takes two checkpoints in one commit to happen.
	var confirmed, aborted []connector.Committable
	for _, c := range sent {
		o, ok := answered[blobKey(c.Handle)]
		if !ok {
			// A committable the sink did not answer for is IN DOUBT, exactly as a failed call is.
			// Treating silence as success is how a commit path loses data quietly.
			r.deps.Log.Warn("a sink did not answer for a committable; it stays pending",
				"node", node, "checkpoint", id)
			continue
		}

		switch o.Disposition {
		case connector.DispositionCommitted, connector.DispositionAlreadyCommitted:
			// AlreadyCommitted is a SUCCESS: it is what a re-offered committable answers after a
			// crash between the checkpoint write and the commit, and it is the mechanism that makes
			// the lost confirmation self-repairing.
			confirmed = append(confirmed, c)

		case connector.DispositionRetryLater:
			r.deps.Log.Warn("a committable was not published and will be offered again",
				"node", node, "checkpoint", id, "records", c.Records)

		case connector.DispositionDeadLetter:
			// The records are NOT delivered and the prefix must not advance past them. Routing them
			// is not possible from a committable — it names cursors, not records — so this stops the
			// pipeline with the blast radius named, which is the honest answer until the failed edge
			// can carry a span.
			return fault.Contract(fault.OpCommitSink, fmt.Errorf(
				"engine: sink %s dead-lettered a committable covering %d records over lanes %v at checkpoint %d: %w",
				node, c.Records, c.Lanes, id, faultOf(o)))

		case connector.DispositionAborted:
			aborted = append(aborted, c)

		default:
			return fault.Contract(fault.OpCommitSink, fmt.Errorf(
				"engine: sink %s answered checkpoint %d with disposition %d, which is not in the closed set",
				node, id, o.Disposition))
		}
	}

	if len(confirmed) == 0 && len(aborted) > 0 {
		// Aborted with NOTHING confirmed alongside it is a real rollback, not a subsumption: this
		// data is not at the destination and will not be. The records were never settled, so nothing
		// advanced past them — but the pipeline cannot proceed either, because settling them would
		// claim a delivery that did not happen and leaving them would hang the drain. Stopping and
		// naming the span is the only answer that neither loses nor lies.
		c := aborted[0]
		return fault.Contract(fault.OpCommitSink, fmt.Errorf(
			"engine: sink %s rolled back a committable covering %d records over lanes %v at checkpoint %d; "+
				"they were never published and will be re-read on restart",
			node, c.Records, c.Lanes, id))
	}

	if len(confirmed) == 0 {
		return nil
	}

	// PUBLISHED, THEREFORE DURABLE, THEREFORE SETTLED. This is the moment a Committer's records
	// become safe, and the only moment the prefix is allowed to move past them.
	//
	// The highest confirmed id is what subsumes; a re-minted committable carries a newer one than
	// the checkpoint that first held its records.
	high := id
	for _, c := range confirmed {
		if c.Checkpoint > high {
			high = c.Checkpoint
		}
	}
	recs := r.awaiting.release(high, node)
	if len(recs) > 0 {
		r.p.ledger.Settle(outcomesFor(node, recs, connector.WriteResult{}))
		r.p.obs.recordsWritten.Add(float64(len(recs)), r.p.obs.pipeline, string(node), r.p.sinks[node].Name)
	}
	r.checkpointer.confirm(high, node)
	return nil
}

func faultOf(o connector.CommitOutcome) error {
	if o.Fault != nil {
		return o.Fault
	}
	return fmt.Errorf("no fault was attached")
}

// blobKey renders a handle for map lookup. A Blob is a version plus bytes and neither is comparable
// as a map key on its own.
func blobKey(b record.Blob) string { return fmt.Sprintf("%d:%x", b.Version, b.Bytes) }

// confirm drops a node's committables at checkpoint id AND EVERY LOWER ONE from the pending set,
// which is the subsuming contract applied to the durable side.
func (c *checkpointer) confirm(id uint64, node record.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for at := range c.pending {
		if at > id {
			continue
		}
		var kept []connector.Committable
		for _, cm := range c.pending[at] {
			if cm.Node != node {
				kept = append(kept, cm)
			}
		}
		if len(kept) == 0 {
			delete(c.pending, at)
			continue
		}
		c.pending[at] = kept
	}
}

// offerable returns the whole pending set, newest-minted last.
//
// The core hands back the ACCUMULATED set rather than only what this checkpoint minted, because the
// subsuming contract is what lets a sink fold "a re-minted longer-span committable plus the shorter
// one it subsumes" into one commit. Offering only the newest leaves the older ones in the pending
// set forever, growing the checkpoint every tick.
func (c *checkpointer) offerable() []connector.Committable {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []connector.Committable
	for _, at := range slices.Sorted(maps.Keys(c.pending)) {
		out = append(out, c.pending[at]...)
	}
	return out
}

// hasPending reports whether any committable is still awaiting confirmation. A checkpoint must keep
// being written while one exists, because the pending set is what recovery reads.
func (c *checkpointer) hasPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) > 0
}

// stage records the committables a checkpoint is about to persist.
func (c *checkpointer) stage(id uint64, cs []connector.Committable) {
	if len(cs) == 0 {
		return
	}
	c.mu.Lock()
	c.pending[id] = append(c.pending[id], cs...)
	c.mu.Unlock()
}

// snapshot returns the pending set for serialisation.
func (c *checkpointer) snapshot() map[uint64][]connector.Committable {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[uint64][]connector.Committable, len(c.pending))
	for k, v := range c.pending {
		out[k] = slices.Clone(v)
	}
	return out
}

// awaitingCommit holds the records a checkpoint's committables cover, until the commit lands.
//
// Separate from [deferred] because the lifetimes differ: deferred is "the sink has these and has not
// made them durable", this is "a committable is responsible for these and the answer is pending".
// Merging them would let a prepare re-offer records a commit is already in doubt about.
type awaitingCommit struct {
	mu sync.Mutex
	by map[uint64]map[record.NodeID][]*record.Record
}

func newAwaitingCommit() *awaitingCommit {
	return &awaitingCommit{by: map[uint64]map[record.NodeID][]*record.Record{}}
}

func (a *awaitingCommit) hold(id uint64, node record.NodeID, recs []*record.Record) {
	if len(recs) == 0 {
		return
	}
	a.mu.Lock()
	if a.by[id] == nil {
		a.by[id] = map[record.NodeID][]*record.Record{}
	}
	a.by[id][node] = append(a.by[id][node], recs...)
	a.mu.Unlock()
}

// release returns every record this node is awaiting at checkpoint id OR ANY LOWER ONE.
//
// "A higher id SUBSUMES every lower one" is the checkpoint contract, and it is load-bearing here
// rather than decorative. A sink that answered RetryLater and re-mints at the next point holds the
// SAME staged data under a NEW committable — and the records were held under the id of the FIRST
// prepare, because the second one had nothing left to take. Releasing only the confirmed id would
// strand them: never settled, prefix never advances, drain never completes. That is exactly what
// happened before this loop replaced a single map lookup.
func (a *awaitingCommit) release(id uint64, node record.NodeID) []*record.Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []*record.Record
	for at := range a.by {
		if at > id {
			continue
		}
		out = append(out, a.by[at][node]...)
		delete(a.by[at], node)
		if len(a.by[at]) == 0 {
			delete(a.by, at)
		}
	}
	return out
}

func (a *awaitingCommit) outstanding() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, byNode := range a.by {
		for _, recs := range byNode {
			n += len(recs)
		}
	}
	return n
}

// --- the durable envelope ----------------------------------------------------------------------

// buildCheckpoint assembles the record that goes to the store.
func (r *runner) buildCheckpoint(ctx context.Context, id uint64) (*Checkpoint, error) {
	cp := &Checkpoint{
		Header: Header{
			ID:           id,
			Tenant:       r.p.spec.Tenant,
			Pipeline:     r.p.spec.ID,
			Generation:   r.p.spec.Revision,
			Epoch:        singleWorkerEpoch,
			Worker:       string(r.deps.Worker),
			CanalVersion: r.deps.Version,
			Connectors:   r.connectorVersions(),
			CommittedAt:  time.Now(),
		},
		Lanes:        map[record.LaneID]LaneState{},
		Committables: r.checkpointer.snapshot(),
		SchemaEpoch:  0,
	}

	// WriterState is snapshotted per node, under the same id, so a restore hands each sink back its
	// own blobs and nobody else's.
	ws := map[record.NodeID][]record.Blob{}
	for _, node := range r.mainSinks {
		sk := r.p.sinks[node]
		if sk.State == nil {
			continue
		}
		bs, err := sandbox(ctx, r.p.obs, node, sk.Name, id,
			func(c context.Context, cid uint64) ([]record.Blob, error) {
				return sk.State.SnapshotState(c, cid)
			})
		if err != nil {
			return nil, fmt.Errorf("engine: snapshotting writer state for %s: %w", node, err)
		}
		if len(bs) > 0 {
			ws[node] = bs
		}
	}
	if len(ws) > 0 {
		cp.WriterState = ws
	}

	cp.Stamp() // rule four of the format contract: version at serialise time
	return cp, nil
}

// connectorVersions records which component builds wrote this state, so an operator reading a
// checkpoint can see what to roll back to.
func (r *runner) connectorVersions() map[string]string {
	out := map[string]string{}
	for _, node := range slices.Sorted(maps.Keys(r.p.sources)) {
		s := r.p.sources[node]
		out[s.Name] = strconv.Itoa(s.Caps.APIVersion)
	}
	for _, node := range slices.Sorted(maps.Keys(r.p.sinks)) {
		s := r.p.sinks[node]
		out[s.Name] = strconv.Itoa(s.Caps.APIVersion)
	}
	return out
}

// persistCheckpoint writes the checkpoint AND every lane cursor it covers in one atomic batch.
func (r *runner) persistCheckpoint(ctx context.Context, cp *Checkpoint,
	positions map[record.LaneID]record.Position,
) error {
	batch := store.NewBatch(singleWorkerEpoch)

	// Lane rows are staged by their owning laneCtl, in node order so two flushes cannot take the
	// per-node write locks in opposite orders and deadlock.
	var dones []func(bool)
	finish := func(landed bool) {
		for _, d := range dones {
			d(landed)
		}
	}
	for _, node := range slices.Sorted(maps.Keys(r.lanes)) {
		done, err := r.lanes[node].stage(batch, positions)
		dones = append(dones, done)
		if err != nil {
			finish(false)
			return err
		}
	}

	body, err := json.Marshal(cp)
	if err != nil {
		finish(false)
		return fmt.Errorf("engine: encoding the checkpoint: %w", err)
	}
	batch.Put(store.CheckpointKey(r.p.spec.Tenant, r.p.spec.ID), body, r.checkpointVersion)

	if err := r.deps.State.Set(ctx, *batch); err != nil {
		finish(false)
		return fmt.Errorf("engine: writing checkpoint %d: %w", cp.Header.ID, err)
	}
	r.checkpointVersion++
	finish(true)
	return nil
}

// --- recovery ------------------------------------------------------------------------------

// recoverCheckpoint reads the last checkpoint and resolves what the previous process left in doubt.
//
// It runs at Open, before any record moves, and it is the half of the protocol that makes a lost
// confirmation self-repairing. A crash between the checkpoint write and the commit leaves
// committables here; without this they are orphaned staged artifacts, and the next checkpoint
// advances a cursor past records nobody published.
//
// THE RECOVERED SET IS NOT RE-COMMITTED BLINDLY. It goes to the sink, which is the only party that
// knows whether each artifact landed: [connector.StaleResolver] gives a per-item answer and is
// preferred whenever present, because a commit that can time out AFTER succeeding cannot be
// resolved with one naked error for a batch. [connector.Committer.AbortStale] is the fallback, and
// it is sufficient only for a sink whose abort cannot partially succeed.
//
// THE CURSORS NEED NOTHING. A committable's records were unsettled when the checkpoint was written,
// so the persisted cursor is BEHIND them by construction: whatever the sink decides, the records
// are re-read. That is a duplicate under at-least-once and it is what dedupe exists for above it.
func (r *runner) recoverCheckpoint(ctx context.Context) error {
	key := store.CheckpointKey(r.p.spec.Tenant, r.p.spec.ID)
	got, err := r.deps.State.Get(ctx, []store.Key{key})
	if err != nil {
		return fmt.Errorf("engine: reading the checkpoint: %w", err)
	}
	v, ok := got[key.String()]
	if !ok {
		return nil // cold start
	}
	r.checkpointVersion = v.Version

	var cp Checkpoint
	if err := json.Unmarshal(v.Value, &cp); err != nil {
		// A checkpoint that does not parse is state this build cannot interpret. Continuing would
		// silently re-read everything AND orphan every staged artifact it describes.
		return fault.Contract(fault.OpOpen,
			fmt.Errorf("engine: the checkpoint is unreadable by this build: %w", err))
	}
	if cp.WrittenByNewerBuild() {
		// Rule three of the format contract: never reject forward state. Report it and carry on
		// with the fields this build understands.
		r.deps.Log.Warn("the checkpoint was written by a newer build; unknown fields are ignored",
			"format", cp.Header.Format, "this_build", CheckpointFormat,
			"reason", telemetry.ReasonStateWrittenByNewer)
	}

	// The id must not restart from zero, or a new checkpoint would claim to subsume nothing.
	r.checkpointer.mu.Lock()
	r.checkpointer.id = cp.Header.ID
	r.checkpointer.mu.Unlock()

	if err := r.restoreWriterState(ctx, cp.WriterState); err != nil {
		return err
	}
	return r.resolveStale(ctx, cp.Committables)
}

// restoreWriterState hands each sink back its OWN in-progress work.
//
// Keyed by node, which the checkpoint's own comment explains at length: one unkeyed slice handed
// every sink every other sink's blobs, and two nodes running the same connector decode each other's
// perfectly and adopt each other's open uploads.
func (r *runner) restoreWriterState(ctx context.Context, ws map[record.NodeID][]record.Blob) error {
	for _, node := range slices.Sorted(maps.Keys(ws)) {
		sk, ok := r.p.sinks[node]
		if !ok || sk.State == nil {
			// The graph changed under the state. Dropping the blobs is right — this node is gone or
			// no longer keeps state — but it is worth saying out loud.
			r.deps.Log.Warn("recovered writer state for a node that no longer keeps any; discarding",
				"node", node, "blobs", len(ws[node]))
			continue
		}
		if _, err := sandbox(ctx, r.p.obs, node, sk.Name, ws[node],
			func(c context.Context, bs []record.Blob) (struct{}, error) {
				return struct{}{}, sk.State.RestoreState(c, bs)
			}); err != nil {
			return fmt.Errorf("engine: restoring writer state for %s: %w", node, err)
		}
	}
	return nil
}

// resolveStale offers every recovered committable back to the sink that minted it.
func (r *runner) resolveStale(ctx context.Context, pending map[uint64][]connector.Committable) error {
	byNode := map[record.NodeID][]connector.Committable{}
	for _, cs := range pending {
		for _, c := range cs {
			byNode[c.Node] = append(byNode[c.Node], c)
		}
	}
	if len(byNode) == 0 {
		return nil
	}

	for _, node := range slices.Sorted(maps.Keys(byNode)) {
		cs := byNode[node]
		sk, ok := r.p.sinks[node]
		if !ok || sk.Committer == nil {
			// Nobody can resolve these. Refusing to start is the only safe answer: the artifacts are
			// staged at a destination and this build has no way to publish or reclaim them.
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"engine: %d committables were recovered for node %s, which is not a Committer in this graph; "+
					"they are staged at a destination and nothing here can resolve them", len(cs), node))
		}

		r.deps.Log.Warn("resolving committables left in doubt by a previous run",
			"node", node, "committables", len(cs))

		var outs []connector.CommitOutcome
		var err error
		if sk.Stale != nil {
			// PREFERRED. A per-item answer strictly dominates one naked error for a batch, and the
			// three answers it can give — landed, reclaimable, in doubt — are exactly the three
			// states a timed-out commit leaves behind.
			outs, err = sandbox(ctx, r.p.obs, node, sk.Name, cs,
				func(c context.Context, in []connector.Committable) ([]connector.CommitOutcome, error) {
					return sk.Stale.ResolveStale(c, in)
				})
		} else {
			_, err = sandbox(ctx, r.p.obs, node, sk.Name, cs,
				func(c context.Context, in []connector.Committable) (struct{}, error) {
					return struct{}{}, sk.Committer.AbortStale(c, in)
				})
			// AbortStale has no per-item answer, so its success means the whole batch is reclaimed.
			for _, c := range cs {
				outs = append(outs, connector.CommitOutcome{
					Handle: c.Handle, Disposition: connector.DispositionAborted,
				})
			}
		}
		if err != nil {
			return fmt.Errorf("engine: resolving stale committables at %s: %w", node, err)
		}

		if err := r.reportResolved(node, cs, outs); err != nil {
			return err
		}
		r.checkpointer.confirm(cp0, node)
	}
	return nil
}

// cp0 is the checkpoint id recovered committables are re-keyed under.
//
// They came from several past checkpoints and none of those ids means anything to this generation;
// what matters is only that they are pending until resolved. Zero is the id no live checkpoint uses.
const cp0 uint64 = 0

// reportResolved turns recovery outcomes into log lines and, where the answer is loud, a refusal.
func (r *runner) reportResolved(node record.NodeID, cs []connector.Committable,
	outs []connector.CommitOutcome,
) error {
	answered := map[string]connector.CommitOutcome{}
	for _, o := range outs {
		answered[blobKey(o.Handle)] = o
	}
	for _, c := range cs {
		o, ok := answered[blobKey(c.Handle)]
		if !ok {
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"engine: sink %s did not answer for a recovered committable covering lanes %v; "+
					"it is neither published nor reclaimed and this build will not guess", node, c.Lanes))
		}
		switch o.Disposition {
		case connector.DispositionCommitted, connector.DispositionAlreadyCommitted:
			// The previous run's commit had landed after all, or landed now. Its records are at the
			// destination and the cursor behind them will re-read: duplicates, not loss.
			r.deps.Log.Info("a recovered committable was published",
				"node", node, "records", c.Records, "lanes", c.Lanes, "disposition", o.Disposition)
		case connector.DispositionAborted:
			r.deps.Log.Info("a recovered committable was reclaimed; its records will be re-read",
				"node", node, "records", c.Records, "lanes", c.Lanes)
		case connector.DispositionRetryLater:
			// IN DOUBT. Neither committing nor rolling back is safe, so the pipeline does not start.
			// Rolling back what landed loses data; committing what did not creates it.
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"engine: sink %s cannot tell whether a recovered committable covering lanes %v landed; "+
					"resolve it at the destination before restarting", node, c.Lanes))
		default:
			return fault.Contract(fault.OpOpen, fmt.Errorf(
				"engine: sink %s answered a recovered committable with disposition %d", node, o.Disposition))
		}
	}
	return nil
}

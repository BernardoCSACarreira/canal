package connector

import (
	"context"
	"sync"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// StateHandle is a byte-oriented, epoch-fenced durable store with TWO scopes: one slot
// per lane, and one slot per source NODE.
//
// A source that keeps its progress upstream — a broker that holds the offset, a queue
// where deleting the message IS the commit, a webhook with no position at all — never
// touches it. A source whose progress is a cursor uses [AutoPersist] and writes no
// persistence code at all.
//
// It is NOT optional for a source declaring UpstreamRetention: [PrunesOnCommit]. For
// that class of source, canal's own durable record must exist before the source is told
// to advance, so Build refuses such a source against a deployment with no usable state
// store.
//
// THE NODE SCOPE EXISTS BECAUSE SOME STATE MUST PREDATE EVERY LANE. A stream-to-lane
// index, a full-refresh pass counter, the set of lane ids a planner has already
// announced: all of them are read before the first lane is announced and none of them
// belongs to a lane. With only a lane scope they had nowhere durable to live, and the
// workaround — inventing a bookkeeping lane that produces no records — is a lane the
// ledger must budget, gate, fence, revoke and report on for no reason.
//
// Schema history is NOT what the node slot is for. It lives in the core's checkpoint,
// published through [SourceRuntime.Declare], committed atomically with the cursors that
// decode against it. A source persisting its own schema history has two records of one
// fact that can diverge, which is the failure the single-checkpoint design exists to
// prevent.
type StateHandle interface {
	// Get returns the blob and its CAS version, or (zero, 0, nil) if absent.
	Get(ctx context.Context, lane record.LaneID) (record.Blob, uint64, error)

	// Set writes if the stored version matches ifVersion (0 means "must not exist"), and
	// if the caller still holds the lane's epoch. It returns the new version.
	//
	// A version or epoch mismatch is fault.ErrFenced, NOT a PermanentContract: another
	// worker holds this lane, so this LANE is revoked and the pipeline is not. A stale
	// compare-and-set on one lane terminating a whole worker is a defect, not a safety
	// measure.
	Set(ctx context.Context, lane record.LaneID, b record.Blob, ifVersion uint64) (uint64, error)

	// Shared reads the NODE-scoped slot: state that must exist before any lane does. Same
	// (blob, version) shape as [StateHandle.Get].
	Shared(ctx context.Context) (record.Blob, uint64, error)

	// SetShared writes the node-scoped slot, CAS-fenced on ifVersion.
	//
	// It is fenced on the NODE's assignment rather than on a lane's, because it belongs to
	// no lane. In a deployment where several workers run the same source node, a
	// node-scoped write is therefore contended and the loser gets fault.ErrFenced and
	// re-reads — which is correct: the slot holds a plan, and two workers writing two
	// plans is the thing to refuse.
	SetShared(ctx context.Context, b record.Blob, ifVersion uint64) (uint64, error)

	// SetMany writes several lanes AND the node slot atomically — all or nothing across
	// the whole [StateWrite]. One SQL transaction, one bbolt transaction, one etcd
	// transaction.
	//
	// A compacted-log store cannot meet this, and the surveyed implementation that tried
	// documents the resulting unrecoverable state in its own javadoc with "no obvious way
	// to resolve the issue". Atomicity here is not a nicety: a stream-to-lane index in the
	// node slot and the lane cursors it indexes must move together or a restart reads one
	// stream's progress under another stream's name.
	SetMany(ctx context.Context, w StateWrite) error

	// Delete removes a lane's state. Used by the offsets-reset operation.
	Delete(ctx context.Context, lane record.LaneID) error
}

// StateWrite is one atomic [StateHandle.SetMany] transaction.
type StateWrite struct {
	// Lanes is the per-lane slots to write. Each entry is fenced on its OWN lane's epoch.
	Lanes map[record.LaneID]Write

	// Shared is the node-scoped slot, written in the same transaction. Nil leaves it
	// untouched.
	Shared *Write
}

// Write is one entry of a [StateHandle.SetMany] batch.
type Write struct {
	Blob      record.Blob
	IfVersion uint64

	// Epoch is the lease epoch this entry is fenced on. ZERO MEANS the epoch the core
	// currently records for this entry's lane, which is every single-lane case and was the
	// only behaviour available.
	//
	// It exists because each lane is its own assignment with its OWN lease epoch, while the
	// store's write batch carried one epoch for the whole transaction. A worker holding 32
	// lanes at 32 different epochs could not fence an atomic 32-lane write: one epoch was
	// either stale for some lanes, refusing a valid write, or newer than some lanes' true
	// epoch, letting a fenced worker through. A per-entry epoch makes the multi-lane atomic
	// write that SetMany exists for actually fenceable.
	Epoch uint64
}

// AutoPersist wires the common case so the ninety-percent source writes no persistence
// code: on every Commit, write the position's Token under the lane, epoch- and
// CAS-fenced; on Open, hand back the stored token.
//
// It is a HELPER OVER THE INTERFACE constructed by the connector, not a core behaviour —
// except that the core's own three-phase commit has already persisted the lane cursor
// before Commit was called. AutoPersist therefore exists for sources whose upstream
// needs a SECOND, source-shaped write (a consumer-group commit, a slot advance) and for
// sources that want their own encoding.
//
// One surveyed framework leaves this reduction out of tree, which is why every one of
// its CDC connectors re-wires it with its own key, its own format and its own bugs.
func AutoPersist(rt SourceRuntime) *Persister {
	return &Persister{state: rt.State(), cache: map[record.LaneID]cachedBlob{}}
}

type cachedBlob struct {
	blob    record.Blob
	version uint64
}

// Persister is the [AutoPersist] helper's state. It is safe for concurrent use, which is
// why a source using it needs no mutex of its own even though Commit runs on the control
// goroutine while Read runs on the read goroutine.
type Persister struct {
	state StateHandle

	mu    sync.Mutex
	cache map[record.LaneID]cachedBlob
}

// Commit writes the acknowledged position's token under the lane, honouring the stored
// CAS version so a fenced worker's write fails with fault.ErrFenced rather than
// overwriting the new holder's progress.
//
// It is a no-op for an [OrderingDiscrete] lane, whose progress is per-handle and has no
// token to persist.
func (p *Persister) Commit(ctx context.Context, a Ack) error {
	if p == nil || p.state == nil {
		return nil
	}
	if a.Through.Token.IsZero() {
		return nil
	}
	p.mu.Lock()
	prev := p.cache[a.Lane]
	p.mu.Unlock()

	v, err := p.state.Set(ctx, a.Lane, a.Through.Token, prev.version)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.cache[a.Lane] = cachedBlob{blob: a.Through.Token, version: v}
	p.mu.Unlock()
	return nil
}

// Restore reads back what Commit last wrote for a lane. It reports false when the lane
// has no persisted token, which a source treats as "no progress yet" and never as "start
// from now".
func (p *Persister) Restore(lane record.LaneID) (record.Blob, bool) {
	if p == nil {
		return record.Blob{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.cache[lane]
	if !ok || c.blob.IsZero() {
		return record.Blob{}, false
	}
	return c.blob, true
}

// Load populates the cache for a lane from the durable store. A source calls it once per
// assigned lane inside Open, before its first Read.
func (p *Persister) Load(ctx context.Context, lane record.LaneID) (record.Blob, bool, error) {
	if p == nil || p.state == nil {
		return record.Blob{}, false, nil
	}
	b, v, err := p.state.Get(ctx, lane)
	if err != nil {
		return record.Blob{}, false, err
	}
	p.mu.Lock()
	p.cache[lane] = cachedBlob{blob: b, version: v}
	p.mu.Unlock()
	return b, !b.IsZero(), nil
}

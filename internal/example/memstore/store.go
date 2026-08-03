// Package memstore is an in-memory store.StateStore, store.ConfigStore and store.Coordinator for
// tests.
//
// It is SCAFFOLDING AND IT IS LABELLED AS SUCH (design rule R10): it declares
// FlushIsDurable false-by-honesty in one respect — a process crash loses everything — and
// [StateStore.Capabilities] says so through [connector.DurabilityNone]. It exists to prove
// the store interface is implementable and to let Build be exercised without a disk, NOT to
// stand in for a real store. A pipeline built against it gets the guarantees a memory store
// can actually give.
//
// The single-node implementation the goals call for is bbolt, where one Set is one
// transaction and therefore genuinely atomic and genuinely durable. This is not that.
package memstore

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"sync"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// StateStore is an in-memory, mutex-guarded, atomic-per-Set state store.
type StateStore struct {
	mu   sync.Mutex
	data map[string]store.Versioned

	// epochs records the highest epoch seen per key, which is how a stale write is fenced. A
	// real store keeps this as a column on the row.
	epochs map[string]uint64
}

// New returns an empty store.
func New() *StateStore {
	return &StateStore{data: map[string]store.Versioned{}, epochs: map[string]uint64{}}
}

// clone deep-copies a row so no slice crosses the boundary in either direction.
//
// NEITHER STORE HAD BOTH HALVES OF THIS, which is what the conformance suite was written to find and
// found on its first run. This store returned its own slices from Get, so a reader corrupted state it
// did not own. pkg/store/wal cloned on the way out and filed the CALLER'S slice into its index on the
// way in, so a caller reusing its buffer rewrote a row after the fsync that was meant to finalise it.
// One defect, mirrored, and each store's own tests covered only the half it happened to get right.
//
// A file-backed store does not get this for free either — I assumed it did, and wal's index is where
// that assumption broke.
//
// The engine hands these bytes to json.Unmarshal and then to a CONNECTOR, so the code that would
// otherwise have to be trusted not to touch them is third-party. See store.StateStore.Get.
func clone(v store.Versioned) store.Versioned {
	out := v
	if v.Value != nil {
		out.Value = append([]byte(nil), v.Value...)
	}
	if v.Key.Parts != nil {
		out.Key.Parts = append([]string(nil), v.Key.Parts...)
	}
	return out
}

// Get reads several keys. An absent key is simply missing from the result.
func (s *StateStore) Get(_ context.Context, keys []store.Key) (map[string]store.Versioned, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]store.Versioned, len(keys))
	for _, k := range keys {
		if v, ok := s.data[k.String()]; ok {
			out[k.String()] = clone(v)
		}
	}
	return out, nil
}

// Range iterates every key under a prefix, in key order.
func (s *StateStore) Range(_ context.Context, prefix store.Key) (iter.Seq2[store.Key, store.Versioned], error) {
	s.mu.Lock()
	var keys []string
	for k, v := range s.data {
		if v.Key.Prefix(prefix) {
			keys = append(keys, k)
		}
	}
	snapshot := make([]store.Versioned, 0, len(keys))
	sort.Strings(keys)
	for _, k := range keys {
		snapshot = append(snapshot, clone(s.data[k]))
	}
	s.mu.Unlock()

	return func(yield func(store.Key, store.Versioned) bool) {
		for _, v := range snapshot {
			if !yield(v.Key, v) {
				return
			}
		}
	}, nil
}

// Set applies the whole batch or none of it, honouring per-key compare-and-set and per-key
// epoch fencing.
//
// The two-pass shape is the point: every precondition is checked before anything is written,
// so a rejected batch leaves no partial state. A store that cannot do this cannot back any
// tier above at-least-once.
func (s *StateStore) Set(_ context.Context, w store.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range w.Writes {
		// PER KEY, NOT PER BATCH, which is what EpochFencing means and what this store declares.
		//
		// It compared w.Epoch — the batch's DEFAULT — so a per-key epoch set through
		// store.Batch.PutFenced was ignored in both directions: a stale write for one lane rode in
		// on a healthy batch epoch, and a fenced worker's whole batch was judged by a number none of
		// its keys carried. store.Versioned.Epoch exists precisely because a worker holding 32 lanes
		// at 32 epochs has no single number to offer, and EpochFor is the accessor that resolves it.
		// pkg/store/wal has always done this correctly; this store advertised the same capability and
		// did not, so every coordinated test ran against a fence that was not there.
		if seen, ok := s.epochs[k]; ok && w.EpochFor(v) < seen {
			return fault.ErrFenced
		}
		cur, exists := s.data[k]
		switch {
		case v.IfVersion == 0 && exists:
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("memstore: %s exists but the write required it not to", k))
		case v.IfVersion != 0 && (!exists || cur.Version != v.IfVersion):
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("memstore: %s is at version %d, not %d", k, cur.Version, v.IfVersion))
		}
	}
	for k, v := range w.Writes {
		next := s.data[k].Version + 1
		v.Version = next
		// Copied on the way IN too, so a caller reusing its buffer after Set cannot rewrite what
		// landed. wal cannot have this bug; a store of live Go values has to decline it explicitly.
		s.data[k] = clone(v)
		// The high-water mark is per key too, for the same reason: recording the batch's default here
		// would raise every key's floor to the highest epoch any ONE lane in the batch was held at.
		if e := w.EpochFor(v); e > s.epochs[k] {
			s.epochs[k] = e
		}
	}
	for _, k := range w.Deletes {
		delete(s.data, k.String())
	}
	return nil
}

// Delete removes keys unconditionally.
func (s *StateStore) Delete(_ context.Context, keys []store.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.data, k.String())
	}
	return nil
}

// Capabilities reports what this store can honestly promise.
//
// FlushIsDurable is true in the sense the commit protocol needs — the write is visible and
// atomic when Set returns — but Durability is [connector.DurabilityNone], which is what tells
// Build that this deployment survives nothing. Build refuses a pruning-upstream source against
// it for exactly that reason.
func (s *StateStore) Capabilities() store.StoreCaps {
	return store.StoreCaps{
		AtomicMultiKey: true,
		CAS:            true,
		EpochFencing:   true,
		Durability:     connector.DurabilityNone,
		FlushIsDurable: true,
	}
}

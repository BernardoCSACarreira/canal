// Package memstore is an in-memory store.StateStore and store.ConfigStore for tests.
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

// Get reads several keys. An absent key is simply missing from the result.
func (s *StateStore) Get(_ context.Context, keys []store.Key) (map[string]store.Versioned, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]store.Versioned, len(keys))
	for _, k := range keys {
		if v, ok := s.data[k.String()]; ok {
			out[k.String()] = v
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
		snapshot = append(snapshot, s.data[k])
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
		if seen, ok := s.epochs[k]; ok && w.Epoch < seen {
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
		s.data[k] = v
		if w.Epoch > s.epochs[k] {
			s.epochs[k] = w.Epoch
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

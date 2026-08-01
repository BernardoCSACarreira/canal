package memstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// watchBuffer is how far behind a watcher may fall before it starts losing events.
//
// A FULL BUFFER DROPS RATHER THAN BLOCKS, which is a correctness statement and not a tuning
// parameter. Blocking would make one wedged watcher stall every Put in the process, and
// [store.ConfigStore.Watch] says a watch is a convenience and never a correctness dependency
// precisely so that this drop is legal. The reconcile timer on the other side is what makes it safe;
// [ConfigStore.Dropped] is what makes it observable rather than a claim in a comment.
const watchBuffer = 64

// ConfigStore is an in-memory, revisioned [store.ConfigStore].
//
// SCAFFOLDING, like the rest of this package (design rule R10): a process exit loses every spec. It
// exists to prove the interface is implementable and to give the engine's config watch something
// real to watch, NOT to stand in for bbolt or Postgres.
//
// THE REVISION IS STORE-WIDE AND MONOTONIC, not per-key. That is etcd's ModRevision shape rather
// than a Postgres row version, and it is what makes Watch(fromRevision) orderable at all: with a
// per-key counter two unrelated pipelines both have a revision 3, and a watcher resuming "from 3"
// cannot say which of them it has already seen.
type ConfigStore struct {
	mu    sync.Mutex
	specs map[pipelineKey]storedSpec
	rev   uint64

	watchers map[uint64]chan store.ConfigEvent
	nextID   uint64

	// dropped counts events a full watcher buffer discarded. It is read by tests, which is the point:
	// a drop policy nothing can observe is indistinguishable from a delivery guarantee that happens to
	// hold on small inputs.
	dropped atomic.Uint64
}

// pipelineKey is a STRUCT rather than a joined string because a tenant id containing whatever
// separator was chosen would otherwise let one tenant address another's pipeline.
type pipelineKey struct {
	tenant record.TenantID
	id     record.PipelineID
}

type storedSpec struct {
	spec      spec.Spec
	rev       uint64
	updatedAt time.Time
}

// NewConfig returns an empty config store.
func NewConfig() *ConfigStore {
	return &ConfigStore{
		specs:    map[pipelineKey]storedSpec{},
		watchers: map[uint64]chan store.ConfigEvent{},
	}
}

// Dropped reports how many watch events a full buffer discarded over this store's life.
func (c *ConfigStore) Dropped() uint64 { return c.dropped.Load() }

// Get returns a spec and its stored revision, or [store.ErrNoSpec].
func (c *ConfigStore) Get(_ context.Context, t record.TenantID, id record.PipelineID) (spec.Spec, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.specs[pipelineKey{t, id}]
	if !ok {
		return spec.Spec{}, 0, fault.Permanent(fault.OpRead, store.ErrNoSpec)
	}
	return s.spec, s.rev, nil
}

// List returns the listing projection for one tenant, in pipeline-id order.
func (c *ConfigStore) List(_ context.Context, t record.TenantID) ([]spec.Summary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]spec.Summary, 0, len(c.specs))
	for k, s := range c.specs {
		if k.tenant != t {
			continue
		}
		// Summarise is the spec package's own projection rather than a conversion written here, so a
		// listing cannot drift per store implementation.
		out = append(out, s.spec.Summarise(s.updatedAt))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Put writes a spec if the stored revision matches ifRevision, and returns the new one.
//
// The stored spec's own Revision field is STAMPED with the new revision, so a spec read back out of
// the store agrees with the number returned beside it. Without that a caller has two candidate
// answers to "what revision is this" and no rule for choosing.
func (c *ConfigStore) Put(_ context.Context, s spec.Spec, ifRevision uint64) (uint64, error) {
	if s.Tenant == "" || s.ID == "" {
		return 0, fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: a spec needs both a tenant and an id to be addressable, got %q/%q",
				s.Tenant, s.ID))
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	k := pipelineKey{s.Tenant, s.ID}
	cur, exists := c.specs[k]
	switch {
	case ifRevision == 0 && exists:
		return 0, fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: %s/%s is at revision %d but the write required it not to exist",
				s.Tenant, s.ID, cur.rev))
	case ifRevision != 0 && (!exists || cur.rev != ifRevision):
		return 0, fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: %s/%s is at revision %d, not %d", s.Tenant, s.ID, cur.rev, ifRevision))
	}

	c.rev++
	s.Revision = c.rev
	c.specs[k] = storedSpec{spec: s, rev: c.rev, updatedAt: time.Now()}
	c.publishLocked(store.ConfigEvent{Tenant: s.Tenant, Pipeline: s.ID, Revision: c.rev})
	return c.rev, nil
}

// Delete removes a spec. ifRevision zero is unconditional.
func (c *ConfigStore) Delete(_ context.Context, t record.TenantID, id record.PipelineID, ifRevision uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	k := pipelineKey{t, id}
	cur, exists := c.specs[k]
	if !exists {
		return fault.Permanent(fault.OpPersist, store.ErrNoSpec)
	}
	if ifRevision != 0 && cur.rev != ifRevision {
		return fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: %s/%s is at revision %d, not %d", t, id, cur.rev, ifRevision))
	}

	// A DELETE ADVANCES THE STORE REVISION. It has to: a watcher resuming from the revision it last
	// saw would otherwise be handed the deletion again forever, or never.
	c.rev++
	delete(c.specs, k)
	c.publishLocked(store.ConfigEvent{Tenant: t, Pipeline: id, Revision: c.rev, Deleted: true})
	return nil
}

// Watch streams config changes. The channel is closed when ctx is done.
//
// WHAT CATCHING UP MEANS HERE, EXACTLY. A watcher that connects with a fromRevision the store is
// already past is sent one event per pipeline it currently holds above that revision, in revision
// order, and then the live stream. What it is NOT sent is a deletion that happened before it
// connected: nothing is retained to replay it from, and inventing one would mean claiming a pipeline
// was deleted when it may simply never have existed.
//
// That gap is legal and it is why the interface says a watch is a convenience: the consumer
// reconciles on a timer as well, and a missed deletion costs one timer period of staleness rather
// than a wrong answer. Retaining history to close it would make this test store the only
// implementation in the module with a compaction policy to get wrong.
func (c *ConfigStore) Watch(ctx context.Context, fromRevision uint64) (<-chan store.ConfigEvent, error) {
	c.mu.Lock()

	ch := make(chan store.ConfigEvent, watchBuffer)
	id := c.nextID
	c.nextID++
	c.watchers[id] = ch

	catchup := make([]store.ConfigEvent, 0, len(c.specs))
	for k, s := range c.specs {
		if s.rev > fromRevision {
			catchup = append(catchup, store.ConfigEvent{Tenant: k.tenant, Pipeline: k.id, Revision: s.rev})
		}
	}
	sort.Slice(catchup, func(i, j int) bool { return catchup[i].Revision < catchup[j].Revision })
	for _, e := range catchup {
		c.sendLocked(ch, e)
	}
	c.mu.Unlock()

	// The close happens UNDER THE SAME LOCK every send takes, which is what makes "send on a closed
	// channel" unreachable rather than unlikely.
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		defer c.mu.Unlock()
		if w, ok := c.watchers[id]; ok {
			delete(c.watchers, id)
			close(w)
		}
	}()
	return ch, nil
}

// publishLocked fans one event out to every watcher. It runs under c.mu.
func (c *ConfigStore) publishLocked(e store.ConfigEvent) {
	for _, ch := range c.watchers {
		c.sendLocked(ch, e)
	}
}

// sendLocked delivers one event or counts it as dropped. It runs under c.mu.
func (c *ConfigStore) sendLocked(ch chan store.ConfigEvent, e store.ConfigEvent) {
	select {
	case ch <- e:
	default:
		c.dropped.Add(1)
	}
}

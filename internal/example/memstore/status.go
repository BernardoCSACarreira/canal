package memstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// StatusStore is an in-memory [store.StatusStore].
//
// SCAFFOLDING, like the rest of this package: a process exit loses every report. It exists so a
// worker has somewhere real to publish its read model, and so the aggregator that has to merge those
// documents has real ones to merge rather than fixtures somebody wrote by hand.
//
// AGGREGATE IS NOT IMPLEMENTED HERE YET, and it says so by returning a fault rather than by
// returning something plausible. Merging N workers' documents into one is a decision per field —
// what a paged lane list means across workers, what one worker being on an older revision does to a
// condition — and the interface's own doc names the part that is uniquely easy to get wrong:
// Complete false with Missing populated when a worker has not been heard from. Half of that
// answered convincingly is worse than none of it answered at all.
type StatusStore struct {
	mu      sync.Mutex
	reports map[reportKey]Report

	// Now is the clock, for the same reason the coordinator has one: staleness is the whole point of
	// an aggregated document and a test cannot wait out a real threshold.
	Now func() time.Time
}

// Report is one worker's published view and when it arrived.
type Report struct {
	Status telemetry.PipelineStatus
	At     time.Time
}

type reportKey struct {
	tenant   record.TenantID
	pipeline record.PipelineID
	worker   store.WorkerID
}

// NewStatus returns an empty status store.
func NewStatus() *StatusStore {
	return &StatusStore{reports: map[reportKey]Report{}, Now: time.Now}
}

// Report publishes one worker's view, replacing whatever it published before.
//
// LAST WRITE WINS PER WORKER, with no history. A status document is a snapshot of now; keeping the
// previous ones would mean deciding how long to keep them and what a consumer of an old one is
// supposed to do with it, and neither question has an answer the interface asks for.
func (s *StatusStore) Report(_ context.Context, w store.WorkerID, doc telemetry.PipelineStatus) error {
	if w == "" {
		return fault.Contract(fault.OpPersist,
			fmt.Errorf("memstore: a status report needs a worker id to be attributed to"))
	}
	if doc.Tenant == "" || doc.Pipeline == "" {
		return fault.Contract(fault.OpPersist, fmt.Errorf(
			"memstore: a status report needs a tenant and a pipeline, got %q/%q", doc.Tenant, doc.Pipeline))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports[reportKey{doc.Tenant, doc.Pipeline, w}] = Report{Status: doc, At: s.Now()}
	return nil
}

// Aggregate is not implemented. See the type's doc for why it is a refusal rather than a guess.
func (s *StatusStore) Aggregate(_ context.Context, t record.TenantID, id record.PipelineID,
	_ telemetry.StatusQuery,
) (telemetry.PipelineStatus, error) {
	return telemetry.PipelineStatus{}, fault.Contract(fault.OpRead, fmt.Errorf(
		"memstore: aggregating %s/%s is not implemented; workers publish through Report and nothing "+
			"merges them yet", t, id))
}

// Reports returns what a pipeline's workers have published, keyed by worker.
//
// It is how a test sees what a worker actually said, which is the only observation this store
// supports until Aggregate exists — and it is the point of the store: a report nothing can read back
// is a write to /dev/null with extra steps.
func (s *StatusStore) Reports(t record.TenantID, id record.PipelineID) map[store.WorkerID]Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[store.WorkerID]Report{}
	for k, r := range s.reports {
		if k.tenant == t && k.pipeline == id {
			out[k.worker] = r
		}
	}
	return out
}

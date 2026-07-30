package store

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/telemetry"
)

// StatusStore collects per-worker status into one document.
//
// It is the fourth and last store interface. A fifth would mean the abstraction is wrong.
type StatusStore interface {
	// Report publishes one worker's view. It is best-effort: a failure to report status must never
	// affect the data path, because the data plane keeps running with the entire control plane down.
	Report(ctx context.Context, w WorkerID, s telemetry.PipelineStatus) error

	// Aggregate merges every worker's view into one document.
	//
	// It MUST set Complete false and populate Missing when it did not hear from every known worker. A
	// status document that silently omits a worker is the same lie as a health check returning 200 for
	// a broken pipeline, and it is the one thing an aggregator is uniquely able to get wrong.
	Aggregate(ctx context.Context, t record.TenantID, id record.PipelineID) (telemetry.PipelineStatus, error)
}

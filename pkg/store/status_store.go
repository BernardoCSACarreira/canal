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
	// a broken pipeline, and it is the one thing an aggregator is uniquely able to get wrong. It MUST
	// also set StaleAfterSeconds to the age past which it stops counting a worker's last report,
	// because "heard from" is not a claim until it has a definition.
	//
	// THE QUERY IS HERE BEFORE ANY IMPLEMENTATION IS. This signature took no cursor, offset or limit,
	// against an effort whose own targets are 900 runtime-discovered streams at 32-way chunking —
	// ~29,000 lanes in one pipeline — and 400 pipelines across 40 pods, which is 16,000 whole
	// documents in flight with an O(workers x lanes) merge per read. Adding selection later would
	// change this interface, the document and the SSE protocol in one breaking step. Adding it now,
	// while nothing implements it, costs one parameter.
	Aggregate(ctx context.Context, t record.TenantID, id record.PipelineID,
		q telemetry.StatusQuery) (telemetry.PipelineStatus, error)
}

package store

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/spec"
)

// ConfigStore holds pipeline specs: revisioned compare-and-set plus a watch.
//
// It deals in spec.Spec, which lives in its own leaf package, so store never imports the engine. That
// is one of the three import cycles this layout deliberately does not have.
type ConfigStore interface {
	// Get returns a spec and its stored revision.
	Get(ctx context.Context, t record.TenantID, id record.PipelineID) (spec.Spec, uint64, error)

	// List returns the listing projection, so answering "what pipelines are there" does not require
	// deserialising every graph in a tenant.
	List(ctx context.Context, t record.TenantID) ([]spec.Summary, error)

	// Put writes a spec if the stored revision matches ifRevision (0 means "must not exist") and
	// returns the new revision.
	//
	// The returned revision comes from a DURABLE write: an API that returns an id and a timestamp for
	// something held in an in-memory map is design rule R13's violation, and it is why this signature
	// returns the store's revision rather than the caller's.
	Put(ctx context.Context, s spec.Spec, ifRevision uint64) (uint64, error)

	Delete(ctx context.Context, t record.TenantID, id record.PipelineID, ifRevision uint64) error

	// Watch streams config changes from a revision. The channel is closed when ctx is done.
	//
	// A watch is a convenience, never a correctness dependency: the planner reconciles on a timer as
	// well, so no protocol depends on delivery.
	Watch(ctx context.Context, fromRevision uint64) (<-chan ConfigEvent, error)
}

// ConfigEvent is one observed config change.
type ConfigEvent struct {
	Tenant   record.TenantID   `json:"tenant"`
	Pipeline record.PipelineID `json:"pipeline"`
	Revision uint64            `json:"revision"`

	// Deleted distinguishes a removal from an update. A watch that signals only "something changed"
	// forces the reader to diff, and a reader that must diff will get it wrong once.
	Deleted bool `json:"deleted"`
}

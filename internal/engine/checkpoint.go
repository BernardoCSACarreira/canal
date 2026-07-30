package engine

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// CheckpointFormat is canal's own checkpoint envelope version.
//
// The four-part format contract, which applies to this envelope AND to every connector-authored blob
// inside it:
//
//  1. Additive-only encoding. Every new field is omitempty, so state written by a newer build stays
//     structurally readable by an older one, which ignores unknown keys.
//  2. Absent or zero version means LEGACY, and legacy means "none of the newer fields were recorded —
//     behave exactly as the previous version did". Never "reject", never "assume the default is
//     correct".
//  3. NEVER REJECT state whose version is GREATER than the current one. The format is additive-only, so
//     a newer record stays structurally readable, and rejecting it breaks the readable-by-N+1 rule that
//     makes a binary DOWNGRADE survivable. An unrecognised field is ignored and REPORTED as a degraded
//     condition, so the operator knows a rollback is running on forward state.
//  4. Stamp the version at SERIALISE time, not at construction time, so a parsed legacy record is
//     silently upgraded on its first write.
//
// Every change to this format ships with an upgrade test that writes with build N, reads with N+1,
// writes with N+1, reads with N, and asserts a lossless round trip of everything both understand. A
// format change with no upgrade test does not merge.
const CheckpointFormat uint32 = 1

// Checkpoint is ONE durable record under one monotonic id.
//
// It is assembled by the engine, serialised to bytes, and written through store.StateStore in ONE atomic
// operation — which is why the schema epoch cannot diverge from the positions it decodes, and why a
// committable cannot be orphaned by a cursor that advanced without it.
//
// Two independently-committed stores — offsets in one place, schema history in another — are the
// counter-example, whose canonical failure is "encountered a change event whose schema isn't known". One
// record, one commit.
//
// There is exactly ONE snapshot format, always self-contained and relocatable. A
// canonical-versus-native times aligned-versus-unaligned matrix is an operator tax whose sharp edges are
// avoidable by never creating the matrix.
type Checkpoint struct {
	// Header is core-readable, small, and a CLOSED vocabulary. Opacity here is why one surveyed system
	// has no source-side lag metric and why its offsets API can only show an operator a blob.
	Header Header `json:"header"`

	// Lanes carries each lane's construction spec (write-once) and cursor (write-many) plus its finish
	// state.
	//
	// This is the two-level checkpoint collapsed into one representation: a source with one shared log
	// position has one lane; a source with independent per-stream progress has many. A per-stream-only
	// model needs a global tier retrofitted onto it; a lane-shaped model needs neither, because a lane IS
	// the granularity of independent commitment.
	Lanes map[record.LaneID]LaneState `json:"lanes"`

	// Committables is the subsuming-contract PENDING SET: staged artifacts by the checkpoint id that
	// minted them, published on confirm up to and including that id.
	//
	// Persisted INSIDE the checkpoint, which is what makes a lost confirmation self-repairing.
	Committables map[uint64][]connector.Committable `json:"committables,omitempty"`

	// WriterState is each sink node's own in-progress work: an open multipart upload, a staging table
	// name. KEYED BY NODE, exactly as TransformState always was.
	//
	// It was one unkeyed slice for the whole pipeline, so a graph with two WriterState sinks handed
	// each of them the other's blobs at RestoreState. The blobs are opaque and versioned, so the lucky
	// case is a loud contract fault; the unlucky case is two nodes running the same connector, where
	// the blobs decode perfectly and each sink adopts the other's open uploads. That the transform
	// side of the same checkpoint was already keyed by node is the strongest evidence available that
	// the unkeyed sink side was an oversight rather than a decision.
	WriterState map[record.NodeID][]record.Blob `json:"writer_state,omitempty"`

	// TransformState is per-node state for stateful transforms.
	TransformState map[record.NodeID][]record.Blob `json:"transform_state,omitempty"`

	// SchemaEpoch is the schema-table generation these cursors decode against, committed ATOMICALLY with
	// the positions.
	SchemaEpoch uint64 `json:"schema_epoch"`
}

// Header is the checkpoint's core-readable metadata.
type Header struct {
	// Format is canal's own envelope version. See [CheckpointFormat].
	Format uint32 `json:"format"`

	// ID is monotonic and framework-assigned. A higher id SUBSUMES every lower one.
	ID uint64 `json:"id"`

	Tenant   record.TenantID   `json:"tenant"`
	Pipeline record.PipelineID `json:"pipeline"`

	// Generation is the applied config revision. It answers "did my config change take effect?", which
	// one surveyed status API structurally cannot.
	Generation uint64 `json:"generation"`

	// Epoch is the writing worker's lease token. The store REJECTS a write whose epoch is stale for any
	// lane it touches.
	Epoch  uint64 `json:"epoch"`
	Worker string `json:"worker"`

	// CanalVersion and Connectors record which builds wrote this state, so an operator can see what to
	// roll back to.
	CanalVersion string            `json:"canal_version"`
	Connectors   map[string]string `json:"connectors"`

	// CommittedAt drives the checkpoint-age metric, which is the primary alert signal.
	CommittedAt time.Time `json:"committed_at"`

	// RecordsIn and RecordsOut are the per-checkpoint reconciliation pair. A persistent divergence is the
	// only cheap way to notice a sink that silently drops, and it is CHECKED, not merely recorded.
	RecordsIn  int64 `json:"records_in"`
	RecordsOut int64 `json:"records_out"`
	BytesOut   int64 `json:"bytes_out"`
}

// LaneState is one lane's durable row.
//
// Spec is write-once construction state and Cursor is write-many progress state: two differently
// lifetimed fields on ONE value. Conflating them forces a parallel state-class hierarchy with downcasts;
// separating them here deletes that hierarchy.
type LaneState struct {
	Spec   record.Blob     `json:"spec"`
	Cursor record.Position `json:"cursor"`

	Group record.LaneGroup   `json:"group,omitempty"`
	After []record.LaneGroup `json:"after,omitempty"`

	Kind     connector.LaneKind `json:"kind"`
	Ordering connector.Ordering `json:"ordering"`
	Bounded  bool               `json:"bounded"`
	Finished bool               `json:"finished"`

	// FinishedAt records WHEN the finish became durable, not merely that it happened. A gate that fires
	// on "finished" without knowing whether that fact survived a crash is a gate that can open twice.
	FinishedAt time.Time `json:"finished_at,omitempty"`

	Label  string `json:"label,omitempty"`
	Weight uint64 `json:"weight,omitempty"`

	// Version is the compare-and-set version the store returned.
	Version uint64 `json:"version"`
}

// Stamp sets the envelope version at serialise time, implementing rule four of the format contract. The
// engine calls it immediately before marshalling and nowhere else.
func (c *Checkpoint) Stamp() { c.Header.Format = CheckpointFormat }

// WrittenByNewerBuild reports whether c was written by a build with a higher envelope version.
//
// The answer is NEVER a rejection (rule three). It raises a degraded condition so an operator running a
// rollback on forward state knows it, and the reader carries on with the fields it understands.
func (c *Checkpoint) WrittenByNewerBuild() bool { return c.Header.Format > CheckpointFormat }

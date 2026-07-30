package connector

import (
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Ordering declares how a lane's positions may be resolved.
type Ordering uint8

const (
	// OrderingPrefix means positions within the lane are totally ordered and a position
	// may be committed only when every earlier position has settled: a monotonic
	// cursor, an LSN, a binlog coordinate, a file byte offset.
	OrderingPrefix Ordering = iota

	// OrderingDiscrete means deliveries within the lane are independent and settle
	// individually, in any order: a queue receipt handle, a delivery tag, an ack id.
	// There is no cursor, so there is no prefix to resolve, and every record MUST carry
	// a handle set through record.Record.SetHandle.
	OrderingDiscrete
)

var orderingNames = [...]string{
	OrderingPrefix:   "prefix",
	OrderingDiscrete: "discrete",
}

// String returns the stable snake_case token for o.
func (o Ordering) String() string {
	if int(o) < len(orderingNames) {
		return orderingNames[o]
	}
	return "prefix"
}

// Boundedness declares whether a lane ends.
//
// A pipeline's TYPE is the boundedness of its lanes: all bounded is a batch pipeline,
// one unbounded is streaming, both is hybrid. There is no pipeline.type field and
// nothing in the core switches on a phase.
type Boundedness uint8

const (
	// Unbounded means the lane tails forever.
	Unbounded Boundedness = iota
	// Bounded means the lane finishes and EndOfLane will arrive.
	Bounded
)

var boundednessNames = [...]string{
	Unbounded: "unbounded",
	Bounded:   "bounded",
}

// String returns the stable snake_case token for b.
func (b Boundedness) String() string {
	if int(b) < len(boundednessNames) {
		return boundednessNames[b]
	}
	return "unbounded"
}

// LaneKind is a REPORTING-ONLY classification. The core stores it, exports it to the
// read model, and uses it to compute scan-progress percentages.
//
// NOTHING IN THE CORE BRANCHES ON IT, and a CI grep asserts that. Two surveyed systems
// smuggled a phase into their opaque checkpoint and both lost snapshot progress
// reporting, snapshot-specific parallelism and re-parallelised resume. Phase belongs in
// core as data, never as control flow.
type LaneKind uint8

const (
	// LaneKindStream is incremental, ongoing reading.
	LaneKindStream LaneKind = iota
	// LaneKindScan is a full read of existing state.
	LaneKindScan
	// LaneKindBackfill is a bounded historical catch-up that is not a full scan.
	LaneKindBackfill
)

var laneKindNames = [...]string{
	LaneKindStream:   "stream",
	LaneKindScan:     "scan",
	LaneKindBackfill: "backfill",
}

// String returns the stable snake_case token for k.
func (k LaneKind) String() string {
	if int(k) < len(laneKindNames) {
		return laneKindNames[k]
	}
	return "stream"
}

// LaneSpec is what a source announces. It is simultaneously the lane's construction
// payload, its resume payload and its row in the assignment table.
//
// One type for all three, so they cannot drift — the structural defence against design
// rule R1's dual-representation failure, where one entity had two identifiers and the
// two drifted.
type LaneSpec struct {
	// Name is the source's own stable identifier. record.LaneID is derived from
	// (tenant, pipeline, node, Name), so the same Name across restarts is the same lane
	// and reuses its persisted state.
	//
	// It MUST be derived from stable content properties, not from an ephemeral handle.
	// A file source needs content fingerprinting precisely because inodes get reused,
	// and a lane whose name changes on restart silently re-reads everything.
	Name string

	Stream      record.StreamName
	Kind        LaneKind
	Ordering    Ordering
	Boundedness Boundedness

	// Group labels this lane for ordering constraints. Opaque to the core, which only
	// ever tests two groups for equality.
	Group record.LaneGroup

	// StartAfter names lane groups that must be FINISHED AND DURABLE before this lane
	// may be assigned or read. This is the snapshot-to-stream handoff, as core-enforced
	// data.
	//
	// The core enforces it from the durable lane table, which the planner reads, so it
	// holds cluster-wide: a worker that happens to hold only the tail lane cannot start
	// tailing while other workers are still scanning. A handoff that is pure connector
	// convention is unimplementable in a cluster, and this field is the whole fix.
	StartAfter []record.LaneGroup

	// Spec is the write-once opaque payload the source needs to CONSTRUCT this lane: a
	// key range, a starting LSN, a shard id, a topic partition. Persisted with the lane
	// row and handed back verbatim. The core never parses it.
	//
	// Write-once construction state and write-many progress state are two differently
	// lifetimed fields on one value. Conflating them is what forced one surveyed system
	// into a parallel state-class hierarchy with downcasts.
	Spec record.Blob

	// Weight is an estimated record count, for progress reporting. Zero means unknown,
	// and the core reports unknown rather than zero.
	Weight uint64

	// Label is a human-readable rendering shown verbatim in the UI: "scan chunk 3/8: id
	// in ['acme','beta')", "changelog tail".
	Label string

	// Budget overrides the pipeline's default in-flight budget for this lane. Zero means
	// the default. A scan lane usually wants a larger budget than a tail.
	Budget int

	// MidLaneResume overrides SourceCaps.MidLaneResume for THIS lane. Nil means the
	// source-wide declaration.
	//
	// It exists because mid-lane resumability is a per-lane property that was declared
	// once per source. One connector's atomic chunk lanes cannot be resumed halfway —
	// the chunk is re-read from its lower bound or not at all — while its tail lane
	// resumes from any LSN. A source holding both had to declare one value, and either
	// choice was wrong for half its lanes: false forces all-or-nothing commits on a
	// tail that never needed them, and true tells the core it may commit inside a chunk
	// that cannot restart there.
	//
	// This is a RUNTIME override affecting only where the core commits. The submit-time
	// negotiation still reads the source-wide declaration, because lanes do not exist
	// at submit time; a source whose lanes disagree declares the PERMISSIVE value in
	// SourceCaps and the restrictive one here, and the core's commit points are then
	// correct for every lane.
	MidLaneResume *bool
}

// LaneAssignment is what the core hands back at Open: the spec, the durable cursor, and
// the epoch that fences writes for it.
type LaneAssignment struct {
	ID   record.LaneID
	Spec LaneSpec

	// Cursor is the last DURABLY COMMITTED position for this lane, or the zero Position
	// when there is none — start from the beginning of the lane.
	//
	// A source distinguishes a cold start from a warm one by whether Assigned returned
	// anything, never by testing a position against nil. The discriminator is data.
	Cursor record.Position

	// Epoch is this worker's fencing token for this lane. Every durable write the core
	// makes on this lane's behalf carries it, and the store rejects a stale one. A
	// connector never uses it directly; it is here so the read model and the log can
	// show it.
	Epoch uint64

	// Finished is true for a lane the core already considers complete. A source never
	// receives a finished lane from LaneCtl.Assigned; it does from
	// [LaneView.Table] with LaneQuery.IncludeFinished, which is what a watermark
	// protocol and a self-retiring transform read.
	Finished bool

	// FinishedAt is when the finish became DURABLE, and is zero until it is. A gate that
	// fires on "finished" without knowing whether that fact survived a crash is a gate
	// that can open twice.
	FinishedAt time.Time

	// GatedOn names the lane groups this lane is still waiting for. Non-empty means the
	// lane exists, is planned, and may not be read yet — which is the state
	// LaneCtl.Assigned omits and a cluster-wide watermark protocol must see.
	GatedOn []record.LaneGroup

	// Worker is the instance currently holding the lane's lease, empty when unheld. It
	// is REPORTING only: leadership and holding are never trusted for correctness, the
	// Epoch is.
	Worker string
}

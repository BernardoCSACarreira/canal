package connector

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// LaneView is the READ-ONLY view of the durable lane table.
//
// It is separate from [LaneCtl] because a transform and the read model need to SEE the
// lane plan without being able to change it. ADR 0008's prescribed shadow transform —
// the one that de-duplicates a concurrent scan against its own tail — cannot be written
// without knowing how many scan lanes exist and whether they have finished, and it must
// not be able to announce one.
type LaneView interface {
	// Table returns rows of the DURABLE, CLUSTER-WIDE lane table, including rows this
	// worker does not hold, rows that are gated, and rows that are finished.
	//
	// This is the enumeration [LaneCtl.Assigned] deliberately does not provide.
	// Assigned answers "what am I responsible for right now"; Table answers "what does
	// the plan look like". A concurrent-snapshot watermark protocol needs the second:
	// the tail lane's emit-or-drop decision is a function of every chunk's range and
	// finish state, cluster-wide, and computing it from one worker's assignment is how
	// a chunked scan silently drops rows that another worker had already emitted.
	//
	// It is PAGINATED because the answer can be 10^5 rows. Pass the last returned
	// ID as q.After to continue; an empty result means the end. A query is evaluated
	// against the table as of the call and is not a snapshot across pages, so a
	// consumer that must see a consistent plan re-reads until two consecutive passes
	// agree — which is cheaper and more honest than a core-held cursor whose lifetime
	// nobody can bound.
	Table(ctx context.Context, q LaneQuery) ([]LaneAssignment, error)
}

// LaneQuery filters and pages [LaneView.Table]. The zero value returns the first page
// of unfinished, ungated rows for every stream, which is the common question.
type LaneQuery struct {
	// After resumes after this lane id in the table's own order. Empty starts at the
	// beginning.
	After record.LaneID

	// Limit caps the page. Zero means the core's default page size, and a Limit larger
	// than the core's maximum is silently reduced — reported through the shorter page,
	// never through an error.
	Limit int

	// Streams and Kinds narrow the query. Empty means every stream and every kind.
	Streams []record.StreamName
	Kinds   []LaneKind

	// Groups narrows to lanes carrying one of these lane groups.
	Groups []record.LaneGroup

	// IncludeFinished includes rows the core considers complete, which is what a
	// watermark protocol and a self-retiring transform both need.
	IncludeFinished bool

	// IncludeGated includes rows whose StartAfter gate has not opened.
	IncludeGated bool
}

// LaneCtl is how a running source announces and retires lanes. It is INJECTED, not
// implemented: the core provides it through [SourceRuntime].
//
// This is the planner/placer separation: the source declares how work divides — by
// announcing lanes, continuously, whenever it likes — and the runtime decides where each
// lane runs. Neither learns the other's algorithm.
//
// The difference from an enumerator role is that there is no separate enumerator and no
// assignment protocol in the required interface. A lane is "a progress domain the source
// told us about". That is what makes "scan with twenty lanes then stream with one"
// expressible without restarting anything, and it is what makes the eight-year advisory
// task-count problem of one surveyed system structurally absent: SourceCaps.MaxLanes is
// enforced on the first violation.
//
// IT IS ALSO THE CHEAPEST PLACE IN CANAL TO ABSORB A NEW NEED, and that is deliberate.
// Nothing outside the core implements it, so every method added here costs connector
// authors nothing and costs the future out-of-process adapter one forwarder that canal
// itself writes once. Six of the eight hostile connectors needed something from the lane
// table; all six are served from this one interface rather than from six new optional
// capabilities on [Source].
type LaneCtl interface {
	LaneView

	// Announce declares a lane. The core persists the row — atomically, together with
	// any state write in flight — BEFORE returning. On return the lane exists durably,
	// so a crash cannot lose the fact that it was planned.
	//
	// Announce is idempotent on LaneSpec.Name: re-announcing an existing lane with an
	// identical Spec returns its id.
	//
	// RE-ANNOUNCING WITH A DIFFERENT SPEC is fault.PermanentContract when the lane has
	// a durable cursor and is not finished, because silently rewriting the construction
	// payload under a live resume point is how a resume lands at the wrong place. It is
	// ACCEPTED, and replaces the Spec, when the lane is finished or has no durable
	// cursor: a stream that disappeared and came back, a chunk re-planned before
	// anything was read from it, and a full-refresh pass that legitimately re-derives
	// its range are all that case, and refusing them stopped the pipeline for a
	// re-announcement that was correct. The acceptance emits EventLaneAnnounced with
	// the old and new Spec versions so the rewrite is visible rather than silent.
	//
	// It returns a fault.PermanentContract when announcing a NEW lane would exceed
	// SourceCaps.MaxLanes. Re-announcing a lane that already exists never fails on the
	// cap, however low the running binary's declared cap is — otherwise a rollback to a
	// binary with a narrower cap cannot read the lane plan it is holding, which is a
	// self-inflicted outage on the recovery path.
	Announce(ctx context.Context, spec LaneSpec) (record.LaneID, error)

	// AnnounceMany announces every spec in ONE durable write, returning ids in the same
	// order.
	//
	// A 900-stream cold start or a 32-way chunk plan is one transaction, not 900
	// serialised fsync round-trips inside an Open the engine retries from the
	// beginning. StateHandle.SetMany already proves the store can do atomic multi-key
	// writes; this is the lane table's use of the same guarantee.
	//
	// It is all-or-nothing: on error NO lane was announced. Per-spec idempotency and
	// the re-announce rule are exactly [LaneCtl.Announce]'s.
	AnnounceMany(ctx context.Context, specs []LaneSpec) ([]record.LaneID, error)

	// Seed installs a lane's INITIAL cursor, for a lane whose resume point is knowable
	// only after the lane exists.
	//
	// The shape it fixes: LaneSpec.Spec is write-once construction state authored
	// BEFORE StartAfter's gate opens, but a tail lane's true starting position is the
	// high watermark taken when the scan it waits behind finished — which is after.
	// Writing it through StateHandle.Set instead is fenced on the TARGET lane's epoch,
	// which the announcing worker does not hold.
	//
	// It fails with fault.PermanentContract when the lane already has a durable cursor:
	// seeding is establishing a starting point, never rewinding a live one. Idempotent
	// for an identical position.
	Seed(ctx context.Context, id record.LaneID, cursor record.Position) error

	// Finish requests retirement. The core does not consider the lane finished until
	// every group admitted for it has settled AND that fact is durable, so this is a
	// request, not an assertion. Idempotent.
	//
	// It is legal for an UNBOUNDED lane. The resulting Ack carries LaneFinished, so a
	// source retiring a revoked partition or a dropped stream learns the retirement
	// became durable instead of guessing.
	Finish(ctx context.Context, id record.LaneID) error

	// Forget removes a FINISHED lane's row and its connector state.
	//
	// Finish marks a lane complete but keeps the row, which is right for a bounded scan
	// whose completion is a fact worth remembering. It is wrong for churn: a source
	// whose streams come and go accumulates the historical union of every stream it has
	// ever seen plus one row per reappearance, and the lane table becomes the largest
	// thing in the checkpoint.
	//
	// Forgetting is a source-initiated DECLARATION that this lane will never be
	// resumed. It fails with fault.PermanentContract on a lane that is not finished,
	// because forgetting a live lane is a silent re-read of everything. Re-announcing a
	// forgotten name afterwards is a cold start, by definition.
	Forget(ctx context.Context, id record.LaneID) error

	// Assigned returns the lanes this source instance is responsible for right now.
	//
	// In one-process mode that is every announced, unfinished, ungated lane. In a
	// cluster it is the subset this worker holds a lease on. The returned slice is a
	// snapshot the caller may retain. Finished and gated rows are excluded; see
	// [LaneView.Table] for those.
	//
	// Distribution is restart with a different subset: a source written to read Assigned
	// and reconstruct from Spec plus Cursor — which is what restart already requires — is
	// a source that scales horizontally with no further code.
	Assigned(ctx context.Context) ([]LaneAssignment, error)

	// Changes is closed and replaced whenever the assigned set changes. A source that
	// wants to react selects on it and calls Assigned again. A source that does not care
	// ignores it and will simply stop being asked for revoked lanes.
	Changes() <-chan struct{}

	// Revoked reports whether a lane is no longer this instance's to read. The source
	// must stop producing for it.
	//
	// Records already handed over settle for accounting, so buffers drain and counters
	// stay correct, but their acknowledgement is NEVER delivered to Source.Commit. That
	// is the fence, and it is why an upstream can never be advanced past data the new
	// holder has not delivered.
	Revoked(id record.LaneID) bool

	// Admission reports how much a lane can absorb RIGHT NOW, and gives an edge to wait
	// on.
	//
	// Blocking in Ledger.Admit is canal's whole source-side backpressure mechanism, and
	// for a PULL source that is sufficient: it observes nothing except that Read is not
	// called again yet. A PUSH source cannot use it. It holds a peer's open request with
	// a five-second deadline, and its only refusal path was to hand the batch over and
	// discover the pressure by timing out — measured at 601ms to produce a 503 that was
	// knowable at zero.
	//
	// Admission is that fact, made observable before the call. It is a snapshot: acting
	// on it is a heuristic and never a guarantee, because the core still enforces the
	// budget by blocking. A source that ignores it stays correct.
	Admission(id record.LaneID) Admission
}

// Admission is the current in-flight allowance for one lane.
type Admission struct {
	// Budget is the CONFIGURED in-flight cap in records; Headroom is how many more may
	// be admitted before Admit blocks. Two numbers because a lane at 0 of 1000 and a
	// lane at 0 of 10 are different diagnoses.
	Budget   int
	Headroom int

	// Known is false when the lane is not registered — revoked, forgotten, never
	// announced. Headroom is then meaningless and a caller must not read it as zero
	// headroom.
	Known bool

	// Blocked reports that an admission is waiting on this lane right now.
	Blocked bool

	// Ready is closed when headroom becomes available, and replaced. It is the edge a
	// push source selects on together with its peer's deadline, so a refusal is issued
	// at the moment the answer is known rather than at the moment the clock runs out.
	//
	// It is nil when Known is false. A channel here is legal for the same reason
	// [LaneCtl.Changes] is: LaneCtl is core-implemented, and the future out-of-process
	// adapter forwards it as a stream that canal writes once.
	Ready <-chan struct{}
}

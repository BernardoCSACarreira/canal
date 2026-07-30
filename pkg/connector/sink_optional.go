package connector

import (
	"context"
	"time"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Flusher declares that Write does NOT by itself make data durable and that durability is
// achieved by Flush.
//
// This is the honest form of buffered writing. The core does not settle records on Write
// for a Flusher sink; it settles them on the Flush that covers them. Making the ack point
// depend on which interface is present — rather than on a bool the sink sets — means
// weakening durability requires implementing a VISIBLE interface rather than being sloppy
// in prose. The negotiated ack point is disclosed to the operator.
//
// The core calls Flush before every checkpoint, on drain, on end of input and before
// applying a schema change, and it tracks exactly which requests each Flush covers, so a
// partial flush is representable.
type Flusher interface {
	// Flush makes every request written since the previous successful Flush durable.
	//
	// reason lets a sink finalise differently at end-of-input — close the file, write the
	// manifest — than at a periodic checkpoint.
	//
	// A partial flush returns (res, err) naming what did not make it, exactly as Write
	// does, keyed on record.RecordID. Reporting an integer count here makes the durable set
	// uncomputable and the prefix unresolvable.
	//
	// A sink whose durability cadence is COARSER than the core's checkpoint cadence answers
	// with [WriteResult].Deferred: accepted, not durable yet, do not resend. That is the
	// fourth quadrant Write does not need and a coarse Flusher cannot do without.
	Flush(ctx context.Context, reason FlushReason) (WriteResult, error)
}

// FlushReason tells a [Flusher] why it is being flushed.
type FlushReason uint8

const (
	// FlushCheckpoint is a periodic durability boundary.
	FlushCheckpoint FlushReason = iota + 1
	// FlushEndOfInput is the last flush of a bounded pipeline: finalise.
	FlushEndOfInput
	// FlushDrain is a graceful shutdown.
	FlushDrain
	// FlushSchemaChange means the core is about to apply a schema change; quiesce first.
	FlushSchemaChange
)

var flushReasonNames = map[FlushReason]string{
	FlushCheckpoint:   "checkpoint",
	FlushEndOfInput:   "end_of_input",
	FlushDrain:        "drain",
	FlushSchemaChange: "schema_change",
}

// String returns the stable snake_case token for r.
func (r FlushReason) String() string {
	if s, ok := flushReasonNames[r]; ok {
		return s
	}
	return "checkpoint"
}

// StructuredSink is the escape hatch for SDK-shaped destinations that must be given
// structured records rather than bytes: a warehouse streaming-insert API, a document
// driver, a vendor client that only accepts its own types.
//
// It is a DECLARED capability, not a runtime fallback: Request.Rows is populated and
// Request.Body is not, decided at Build time. The core refuses to attach an encoder to
// such a sink rather than silently double-encoding.
type StructuredSink interface {
	// AcceptsStructured must return true. The method exists so the registration
	// cross-check has a target to find; its return value is deliberately never read by
	// the core, and that is documented rather than left to be discovered.
	AcceptsStructured() bool
}

// Partitioner groups records into requests by a key the sink chooses. Usually a constant,
// sometimes a template over the record.
//
// Partitioned batching is an ENGINE combinator: the sink supplies the key, the engine
// keeps one open batch per key with its own limits. Per-tenant or per-table batching with
// no batching code in the sink.
type Partitioner interface {
	Partition(r *record.Record) (string, error)
}

// SchemaApplier declares that this sink can act on schema changes.
//
// WHICH kinds it supports is DECLARED DATA on [SinkCaps], not a method: a method would
// force Build to instantiate a sink in order to negotiate, which is exactly what
// "capabilities are queryable without instantiating anything" forbids.
//
// Before calling this the engine quiesces: it stops admitting, drains every in-flight
// record for the affected stream, calls Flush with [FlushSchemaChange], applies the
// change, writes a checkpoint, and resumes. Without the quiesce, records written under
// the old schema race the change.
type SchemaApplier interface {
	ApplySchemaChange(ctx context.Context, ch schema.Change) error
}

// Committer upgrades a sink to exactly-once via two-phase commit. THE ORDERING IS A
// NORMATIVE CONTRACT, not an implementation detail, and it is stated COMPLETELY because
// every method it used to omit is one that touches the buffer the others are sealing.
//
// One checkpoint, in this exact order:
//
//	Flush(FlushCheckpoint)      make written data durable in the sink's own terms
//	  -> PrepareCommit(point)   SEALS: mints this checkpoint's committables
//	  -> SnapshotState(id)      RECORDS: WriterState's view of what is left OPEN
//	  -> [ lane cursors, schema epoch, writer state and committables are written as ONE
//	       durable record under point.ID ]
//	  -> Commit(committables)   called ONLY after that record is durable
//
// PrepareCommit BEFORE SnapshotState, and the order matters in one direction only: a
// sealed artifact must appear in the committables and must NOT also appear in the writer
// state, or recovery both commits it from the pending set and reopens it from the writer
// state and the file lands twice. Snapshotting after sealing makes that impossible; the
// reverse order makes it inevitable.
//
// Recovery, in this exact order:
//
//	Open(rt, Opening{Restored: &id})
//	  -> RestoreState(blobs)    RECOGNITION comes first
//	  -> ResolveStale / AbortStale
//	  -> Commit(recovered)
//	  -> first Write
//
// RestoreState BEFORE the stale resolution, because AbortStale's own job is to discard
// committables "this sink no longer recognises", and recognition IS the restored writer
// state. Aborting first asks the sink to disown artifacts it has not yet remembered
// owning, and a sink that answers honestly destroys its own committed data.
//
// THE CHECKPOINT SUBSUMING CONTRACT, verbatim and normative: confirmations are not
// guaranteed to arrive; ids strictly increase and a higher id SUBSUMES every lower one;
// an implementation must behave as if a non-confirmed checkpoint never happened. Abort
// means "as if never triggered; the next successful checkpoint covers a longer span" — it
// does NOT mean discard the artifacts.
//
// Commit MUST be idempotent and signals per item with [DispositionAlreadyCommitted].
//
// CONCURRENCY: every method here is exclusive with Write and with itself. See
// [Sink.Write]'s concurrency paragraph, which enumerates the full set.
type Committer interface {
	// PrepareCommit mints the committables for a checkpoint. Every committable names the
	// lanes and cursors it covers, so a failed commit can be dead-lettered with the
	// affected span named and the blast radius exact.
	//
	// It takes a [CommitPoint] rather than a bare id because the id alone cannot grow, and
	// the first thing it needed to carry was WHY. A staging sink must finalise differently
	// at end of input — seal the undersized file, write the manifest — than at a periodic
	// checkpoint, and with no reason on this call the only way to learn end-of-input was to
	// declare [Flusher] purely as a signal carrier, which MOVES THE OPERATOR-VISIBLE
	// ACKNOWLEDGEMENT POINT from commit to flush as a side effect of needing one bool.
	PrepareCommit(ctx context.Context, p CommitPoint) ([]Committable, error)

	// Commit publishes everything up to and including the point that minted these. Values
	// in, values out — no mutable request callback, because a callback cannot cross a wire
	// and cannot be table-tested.
	Commit(ctx context.Context, cs []Committable) ([]CommitOutcome, error)

	// AbortStale discards committables the core found in a recovered checkpoint that this
	// sink no longer recognises, and committables whose Expires has passed.
	//
	// Without it, a crash between PrepareCommit and checkpoint durability orphans a staged
	// artifact forever while the next checkpoint commits a cursor past its records.
	//
	// It reports ONE error for the whole batch, which is sufficient only for a sink whose
	// abort cannot partially succeed. A sink whose commit can time out AFTER landing must
	// implement [StaleResolver] instead; the core prefers it whenever it is present.
	AbortStale(ctx context.Context, cs []Committable) error
}

// CommitPoint is the checkpoint a [Committer] is preparing for.
type CommitPoint struct {
	// ID is monotonic and framework-assigned. A higher id SUBSUMES every lower one.
	ID uint64 `json:"id"`

	// Reason is why this checkpoint is happening, in [FlushReason]'s vocabulary — one enum
	// for one concept, not a second parallel one (design rule R9). FlushEndOfInput is the
	// one a staging sink must see: it is the difference between "seal what is full" and
	// "seal everything, including the 4 MB file you were hoping would reach 128 MB".
	Reason FlushReason `json:"reason"`

	// Deadline is when the core will stop waiting. Zero means no deadline. A sink that
	// cannot seal everything in time returns what it sealed and leaves the rest for the
	// next point, which the subsuming contract makes safe.
	Deadline time.Time `json:"deadline,omitempty"`
}

// StaleResolver is [Committer.AbortStale] with a PER-ITEM answer, and it exists because a
// commit that can time out after succeeding cannot be resolved with one naked error.
//
// The situation: the core hands back a committable found in a recovered checkpoint. For
// each one, this sink knows one of three different things — the transaction PREPARED AND
// LANDED and must be committed, not aborted; it is reclaimable and may be rolled back; or
// it is IN DOUBT and must be neither, because rolling back what landed loses data and
// committing what did not creates it. One error for the whole batch can express none of
// them, so the only safe implementation was to attempt the commit from inside the abort
// path, turning abort into a second commit path with no protocol behind it.
//
// It reuses the five existing [Disposition] values and invents nothing: Committed for one
// that landed, Aborted for one reclaimed, RetryLater for one still in doubt, DeadLetter
// for one whose covered span must be routed and whose prefix must NOT advance,
// AlreadyCommitted for one a previous attempt finished.
//
// The core prefers it over AbortStale for every sink that declares it. A sink implements
// one or the other, never both meaningfully.
type StaleResolver interface {
	ResolveStale(ctx context.Context, cs []Committable) ([]CommitOutcome, error)
}

// Committable is one staged, not-yet-published artifact.
type Committable struct {
	Checkpoint uint64 `json:"checkpoint"`

	// Node is the sink node that minted this committable, stamped by the ENGINE.
	//
	// A fan-out graph has several Committer sinks and one pending set, so a recovered
	// committable with no author cannot be routed back to the sink that can commit it.
	// Committer.Commit has no "not mine" answer, so the alternative was for every sink to
	// decode every other sink's opaque Handle and guess.
	Node record.NodeID `json:"node"`

	// Handle is connector-authored and versioned, like everything else that crosses a role
	// boundary or hits disk.
	Handle record.Blob `json:"handle"`

	// Lanes and Cursors are the committable's identity and DURABLE blast radius: a failed
	// commit can name exactly which span of which lanes is affected, after a restart, which
	// is precisely when it matters.
	Lanes   []record.LaneID                   `json:"lanes"`
	Cursors map[record.LaneID]record.Position `json:"cursors,omitempty"`
	Records int64                             `json:"records"`

	// FirstRec and LastRec are IN-GENERATION provenance for a log line, and they are
	// deliberately NOT PERSISTED.
	//
	// record.RecordID is documented as generation-local and never durable, so persisting a
	// pair of them inside a checkpoint produced two ids that name nothing after a restart —
	// which is exactly when a recovered committable is dead-lettered and exactly when
	// DispositionDeadLetter needs to know what it is withholding. Cursors is the durable
	// answer; these two stay for the same-generation log message and vanish with the
	// generation, which is the only honest lifetime for them.
	FirstRec record.RecordID `json:"-"`
	LastRec  record.RecordID `json:"-"`

	// Attempt is 1 the first time this committable is presented to Commit, incrementing on
	// every re-presentation. Engine-populated.
	//
	// Without it a sink could not tell a first commit from a re-presentation after a lost
	// confirmation, so DispositionAlreadyCommitted was either a guess or a probe of the
	// destination on every single commit. connector.Request.Attempt already exists for the
	// same reason on the write path.
	Attempt int `json:"attempt"`

	// Expires bounds how long a staged artifact may sit unpublished. On expiry the core
	// calls ResolveStale or AbortStale and raises a degraded condition. A silent skip with
	// a warning ratio is the honest treatment's opposite.
	Expires time.Time `json:"expires"`
}

// CommitOutcome is what happened to one committable.
type CommitOutcome struct {
	Handle      record.Blob  `json:"handle"`
	Disposition Disposition  `json:"disposition"`
	Fault       *fault.Fault `json:"fault,omitempty"`
}

// Disposition is the closed outcome set for a committable.
//
// FIVE values and NONE of them silently discards data. A commit path documented as "only
// logs the error, discards the committable and continues" is what canal must not copy.
// [DispositionDeadLetter] routes the covered records and does NOT advance the prefix past
// them.
type Disposition uint8

const (
	DispositionCommitted Disposition = iota + 1
	DispositionAlreadyCommitted
	DispositionRetryLater
	DispositionDeadLetter
	DispositionAborted
)

var dispositionNames = map[Disposition]string{
	DispositionCommitted:        "committed",
	DispositionAlreadyCommitted: "already_committed",
	DispositionRetryLater:       "retry_later",
	DispositionDeadLetter:       "dead_letter",
	DispositionAborted:          "aborted",
}

// String returns the stable snake_case token for d.
func (d Disposition) String() string {
	if s, ok := dispositionNames[d]; ok {
		return s
	}
	return "aborted"
}

// WriterState lets a sink carry in-progress work across a restart: an open multipart
// upload, a staging table. It is restored through Opening.Restored plus RestoreState.
//
// It is a SEPARATE interface, not an embedding. Nesting an interface inside another
// interface is what prevented one surveyed sink API from evolving, and it is forbidden
// throughout canal.
//
// ORDERING against [Committer] is normative and stated there: SnapshotState runs AFTER
// PrepareCommit within a checkpoint, and RestoreState runs BEFORE the stale resolution at
// recovery. Either order reversed makes a Committer+WriterState sink double-commit a
// sealed file, so the two interfaces are documented as one protocol even though they stay
// two types.
//
// The state is keyed BY NODE in the checkpoint, exactly as transform state already was. A
// single unkeyed slice meant a graph with two WriterState sinks handed each of them the
// other's blobs at restore, and the blobs are opaque, so the sink that could not decode
// them either failed loudly or — worse — decoded them successfully because both sinks used
// the same connector.
type WriterState interface {
	SnapshotState(ctx context.Context, id uint64) ([]record.Blob, error)
	RestoreState(ctx context.Context, bs []record.Blob) error
}

// TokenSink is the strongest tier: the destination stores canal's checkpoint token
// transactionally with the data, so "the data landed but the state did not" is structurally
// impossible.
type TokenSink interface {
	// WriteWithToken writes the request and canal's token in ONE destination transaction.
	WriteWithToken(ctx context.Context, req *Request, token record.Blob) (WriteResult, error)

	// LoadToken returns the token the destination holds, read at Open.
	LoadToken(ctx context.Context) (record.Blob, bool, error)
}

// Preparer creates or verifies the destination before any data flows.
type Preparer interface {
	Prepare(ctx context.Context, streams []ConfiguredStream, ss []schema.Entry) error
}

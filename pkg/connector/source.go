package connector

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/record"
)

// Source is the required source interface. Four methods. FROZEN: no method will ever be
// added to it.
//
// New core capabilities arrive through the [SourceRuntime] interface — which the core
// implements, so growing it breaks nothing — or as new optional interfaces plus a
// [SourceCaps] field.
type Source interface {
	// Open establishes whatever connection the source needs and reads its assigned lanes.
	// It may do I/O.
	//
	// It will be called again, with backoff, after any method returns
	// fault.ErrNotConnected, so it MUST be idempotent.
	//
	// CONTEXT LIFETIME: ctx is scoped to the OPENING and may be cancelled the instant
	// Open returns. A source needing a connection-lifetime context takes rt.Context().
	// This is stated on the method because a connector holding connections tied to a dead
	// context is the commonest first bug.
	//
	// CONCURRENCY: Open and Close never run concurrently with each other or with any other
	// method on this component, so the fields Open assigns need no lock against the fields
	// Close reads. The core enforces it rather than assuming it: a cancelled Open is
	// ABANDONED and its goroutine keeps running, and Close waits for that goroutine — up to
	// the grace period — before entering the component, and declines to enter at all if it
	// has not come back. That guarantee was missing once, and every connector in the module
	// had a latent data race between the two.
	Open(ctx context.Context, rt SourceRuntime) error

	// Read fills dst and sets dst.Position for a prefix lane. It blocks until at least one
	// record is available, a position advances, the connection drops, or ctx is done. dst
	// is owned by the caller and reused; the source must not retain it. Records are
	// produced ONLY through dst.Add or dst.AddFor.
	//
	// dst.Lane is ALREADY SET to the lane this batch's allocator stamps, and a source must
	// not change it: the ledger refuses a batch whose records disagree with it. A source
	// serving more than one lane implements [LaneReader] and is handed one batch per lane.
	//
	// RETURNING ZERO RECORDS WITH AN ADVANCED dst.Position IS LEGAL and is how a lane makes
	// progress without producing anything: a page of already-seen rows, a fully filtered
	// tail, a planner that only planned, a poll that only proved idleness. The ledger
	// admits it with zero references and resolves it at admission, so the position enters
	// the prefix in order. Returning zero records AND no position is a spin: the core
	// raises fault.PermanentContract on the second consecutive one.
	//
	// A FAULT RETURNED FROM READ SPENDS A RETRY ATTEMPT, with two exceptions that are
	// flow control rather than failure and are therefore uncounted: fault.NotConnected,
	// which requests a reconnect, and fault.Throttled, which requests a wait. Both are
	// bounded by RetryPolicy.Backoff.MaxElapsed and by nothing else, which is what lets a
	// source honour an upstream's Retry-After for as long as the upstream asks without
	// burning the four attempts a poison record needs.
	//
	// CANCELLATION MEANS DRAIN, NOT ABORT. If ctx is cancelled, or is cancelled while
	// Read is running, the source stops retrieving new records from upstream and returns
	// whatever it has already buffered. When nothing is left it returns ctx.Err(). Almost
	// nobody gets this right by accident, which is why it is stated here rather than
	// assumed. A source must never discard records it has already produced into dst on an
	// error path: the engine admits what is in the batch BEFORE handling the error.
	//
	// It returns fault.ErrNotConnected to request a reconnect, or fault.ErrEndOfInput
	// when every lane it holds is finished and no more will be announced.
	//
	// CONCURRENCY: Read is never called concurrently with itself. It MAY run concurrently
	// with Commit, Heartbeat, Backlog and Nack, which the core calls on ONE separate
	// control goroutine and never concurrently with each other. So a source needs at most
	// one mutex, protecting only state shared between its read path and its progress
	// path — and a source using [AutoPersist] needs none, because [Persister] is already
	// safe.
	//
	// The alternative — promising that Commit never runs concurrently with a blocking
	// Read — is unsatisfiable: an idle tail source would either never commit or need locks
	// the contract denies.
	Read(ctx context.Context, dst *record.Batch) error

	// Commit is called when a lane's progress has advanced: records this source handed
	// over are now durable downstream AND canal's own record of that position is durably
	// flushed.
	//
	// WHAT THIS MEANS IS ENTIRELY THE SOURCE'S DECISION. Advance a cursor. Delete the
	// queue messages. Commit a consumer-group offset. Advance a replication slot. Do
	// nothing. The core does not care and does not check.
	//
	// Commit is NEVER called for a lane whose lease this worker has lost. That is the
	// fence, and it is why an upstream can never be advanced past data the new holder has
	// not delivered.
	//
	// Returning an error is ESCALATED, not logged and dropped: the engine classifies it,
	// retries per policy, and raises a degraded condition with reason commit_failed if it
	// cannot succeed. "We delivered the data and silently lost the progress record" is not
	// reachable here.
	Commit(ctx context.Context, a Ack) error

	// Close releases resources. It is called EXACTLY ONCE, ALWAYS, including after a
	// failed Open and including when Open was never called at all — config validation
	// constructs a component and then closes it.
	//
	// It receives a FRESH context carrying the shutdown grace period, never the cancelled
	// read context. All network calls in Close must have a timeout.
	Close(ctx context.Context) error
}

// Ack is what the core tells a source about settled work.
//
// It is a plain serialisable struct: no closures, no channels, no interface fields, so it
// crosses a process boundary unchanged. The one progress primitive being a method over a
// plain struct is what makes the out-of-process seam free later.
type Ack struct {
	Lane  record.LaneID `json:"lane"`
	Epoch uint64        `json:"epoch"`

	// Through is set for [OrderingPrefix] lanes: the highest position such that it and
	// everything before it in this lane has settled, its Safe flag is true, AND canal's
	// own durable write of it has been flushed. Zero when Handles is used instead.
	Through record.Position `json:"through,omitempty"`

	// Handles is set for [OrderingDiscrete] lanes: exactly the delivery handles that
	// LANDED, in no particular order. Nil for prefix lanes.
	//
	// Handles rather than positions, because a queue source must be able to settle
	// individual messages. One position per batch would force a queue source to delete
	// all ten or emit ten one-record batches with ten times the API calls.
	Handles [][]byte `json:"handles,omitempty"`

	// AbandonedHandles is the discrete-lane counterpart of [Ack.Abandoned]: exactly the
	// handles that reached a terminal disposition instead of landing.
	//
	// Both lists are populated for a PARTIALLY abandoned group. The ledger used to withhold
	// such a group's handles ENTIRELY — nine messages landed, one was dead-lettered, and
	// the source was told about none of the ten — so nine handlers waited to their deadline
	// for an answer that already existed. A count next to a list was the shape of the bug:
	// a source could see that something had been abandoned and never which.
	AbandonedHandles [][]byte `json:"abandoned_handles,omitempty"`

	// Records is how many records this ack covers.
	Records uint64 `json:"records"`

	// Abandoned is how many of those reached a terminal disposition — dead-letter or
	// drop — rather than landing at the sink.
	//
	// A source whose commit is DESTRUCTIVE, such as deleting a queue message, may refuse
	// to advance when this is non-zero, leaving the message for another consumer. The
	// core surfaces the number and the source chooses; the core never makes that choice
	// on a source's behalf.
	Abandoned uint64 `json:"abandoned"`

	// AbandonedBy attributes the abandonments to the sink nodes that caused them, so a
	// destructive-commit source can tell a BY-DESIGN load shed from a real dead-letter.
	//
	// In a fan-out to four sinks where one branch is a best-effort metrics feed declared
	// spec.Edge.BestEffort, a shed on that branch is expected and a shed on the warehouse
	// branch is not. One uint64 made them identical, so the only safe reading of any
	// non-zero Abandoned was "refuse to advance", which turns a deliberate best-effort
	// branch into a permanent stall of the whole lane.
	//
	// A BestEffort edge contributes no reference at all, so it never appears here; nodes
	// that do appear are nodes whose loss is real.
	AbandonedBy map[record.NodeID]uint64 `json:"abandoned_by,omitempty"`

	// LaneFinished is true on the final ack for a FINISHED lane, bounded or not. After it,
	// Commit is not called for the lane again.
	//
	// It covers retirement by [LaneCtl.Finish] and by record.Batch.EndOfLane on an
	// unbounded lane just as it covers a bounded lane running out, so a source retiring a
	// revoked partition or a dropped stream learns that the retirement became durable
	// rather than inferring it from silence.
	LaneFinished bool `json:"lane_finished"`
}

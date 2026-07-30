package connector

import (
	"context"

	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/record"
	"github.com/BernardoCSACarreira/canal/pkg/schema"
)

// Sink is the required sink interface. Three methods. FROZEN.
//
// A sink has NO acknowledgement callback and NO progress awareness whatsoever. It signals
// durability by returning a clean [WriteResult]. The core owns the mapping from "this
// request landed" to "that lane's cursor advanced".
//
// This asymmetry is the single most valuable property of the whole design: a new sink
// cannot get progress wrong, because it is never shown progress.
type Sink interface {
	// Open connects and prepares. Same context narrowing and same re-callability as
	// [Source.Open]: ctx is scoped to the opening, and a connection-lifetime context comes
	// from rt.Context().
	//
	// It receives what it needs to create or alter the destination BEFORE the first record
	// that needs it — which is why Open exists rather than folding into the constructor.
	Open(ctx context.Context, rt SinkRuntime, o Opening) error

	// Write delivers one request.
	//
	// SUCCESS AND FAILURE SHAPES, DECIDED TOGETHER (design rule R7). All four quadrants
	// are specified, and the unnamed-record default is SAFE in every one:
	//
	//	(res, nil), res.Failed empty
	//	    every record in the request is DURABLE: it survives the loss of this process
	//	    and of the destination's process. The core will advance the cursor past them. A
	//	    sink returning this before durability has LIED, and that is the only way to
	//	    violate design rule R4 in this design — which is the point.
	//
	//	(res, nil), res.Failed non-empty
	//	    every record NOT named in res.Failed is durable; the named ones are not, each
	//	    with its own class and reason.
	//
	//	(res, err), res.Failed non-empty
	//	    as above, and err is the headline for logs and status.
	//
	//	(res, err), res.Failed empty
	//	    NOTHING in the request is claimed durable. This is the graceful degradation
	//	    path for a sink that cannot report granularity, and it is the correct default:
	//	    the whole request is retried.
	//
	// A sink MUST NOT partially apply and report total success. If it cannot know what
	// landed it returns an error with Failed empty and class fault.Indeterminate.
	//
	// RECONCILIATION IS MANDATORY, not advisory: res.Written must equal
	// req.Count - len(res.Failed). The core checks it and raises fault.PermanentContract
	// on a mismatch. A sink that miscounts is a sink whose durability claim cannot be
	// trusted, and this check is what stops "the sink reported only server-rejected rows
	// and the core committed past everything else".
	//
	// CONCURRENCY, EXHAUSTIVELY: up to SinkCaps.MaxConcurrency Write calls may be in flight
	// on one Sink. EVERY OTHER METHOD THE CORE CALLS ON A SINK IS EXCLUSIVE WITH WRITE AND
	// WITH ITSELF — Open, Flush, Close, PrepareCommit, Commit, ResolveStale, AbortStale,
	// SnapshotState, RestoreState, WriteWithToken, LoadToken, ApplySchemaChange and Prepare.
	// The core quiesces the write path before each of them.
	//
	// The earlier list named only Open, Flush and Close, and every method it omitted is one
	// that touches the same buffer Write appends to: PrepareCommit seals it, SnapshotState
	// reads it, RestoreState replaces it. A sink cannot know that an unlisted method is
	// exclusive, so the only safe reading of the old sentence was one mutex over everything
	// and MaxConcurrency 1 — a declared concurrency capability that no correct sink could
	// use. Enumerating the set is the whole fix, and the enumeration is closed: a method
	// added to a sink capability in future is exclusive unless it says otherwise here.
	Write(ctx context.Context, req *Request) (WriteResult, error)

	// Close flushes and releases. Called exactly once, always, including after a failed
	// Open, with a fresh context carrying the grace period. Must not block indefinitely.
	Close(ctx context.Context) error
}

// Opening is what a sink is given at Open.
//
// A struct rather than parameters, so adding a field later is not a breaking change to
// every sink. This is the growth mechanism for the sink side, and it is why [Sink] itself
// can be frozen.
type Opening struct {
	// Restored is the id of the checkpoint being resumed from, absent on a cold start.
	// Data rather than two constructors, which is right on the sink side because a
	// writer's setup does not otherwise differ.
	Restored *uint64 `json:"restored,omitempty"`

	// Schemas are the schemas this sink will see, so it can create or alter the
	// destination before the first record needing it.
	Schemas []schema.Entry `json:"schemas"`

	// Streams names the logical streams that will be written, with the operator's chosen
	// destination mode per stream.
	//
	// Source-side mode and destination-side mode are ORTHOGONAL: the source never learns
	// whether the sink overwrites, appends or upserts, which is what makes M times N
	// combinations free.
	Streams []ConfiguredStream `json:"streams"`

	// Guarantee is the tier the core computed and validated. A sink MAY assert on it: one
	// requiring upsert semantics and handed [DestAppend] should fail Open loudly rather
	// than write wrong data.
	Guarantee Guarantee `json:"guarantee"`
}

// ConfiguredStream is one stream the sink will be asked to write, and how.
type ConfiguredStream struct {
	Stream record.StreamName `json:"stream"`
	Mode   DestMode          `json:"mode"`
	Keys   [][]string        `json:"keys,omitempty"`
}

// DestMode is the destination-side write mode.
type DestMode uint8

const (
	DestAppend DestMode = iota
	DestUpsert
	DestOverwrite
	DestDelete
)

var destModeNames = [...]string{
	DestAppend:    "append",
	DestUpsert:    "upsert",
	DestOverwrite: "overwrite",
	DestDelete:    "delete",
}

// String returns the stable snake_case token for m.
func (m DestMode) String() string {
	if int(m) < len(destModeNames) {
		return destModeNames[m]
	}
	return "append"
}

// Request is one already-encoded, already-framed, already-compressed unit of work. The
// sink implements TRANSPORT ONLY.
//
// This is the property that makes "add a sink: three methods, register, done" literally
// true, and it is the extensibility constraint applied to codecs as well as to connectors:
// N codecs times M connectors, with no multiplication. Add a codec and every sink gains
// it; add a sink and it gains every codec.
//
// A Request has no mutators and carries no control records. The engine does not retain it
// past Write's return, and a sink must not retain it either.
type Request struct {
	// Body is the encoded, framed, compressed payload. Empty only when Count is 0 or the
	// sink declares [StructuredSink].
	Body []byte

	// Records identifies what is in Body, in Body's order, without the payloads. This is
	// what a sink uses to build a [WriteResult].
	Records []record.Ref

	// Rows is non-nil only for a sink declaring [StructuredSink], and is then the records
	// themselves with no encoding applied.
	//
	// Exactly one of Body and Rows is populated, decided at Build time, never at runtime —
	// so there is no per-request branch and no possibility of double-encoding.
	Rows []*record.Record

	// Partition is the key the engine's partitioner produced, if any. Every record in this
	// request shares it. This is how a generic sink gets per-table, per-tenant or per-day
	// batching without writing batching code.
	Partition string

	Count             int
	UncompressedBytes int

	// ContentType comes from the encoder; ContentEncoding from the compressor, empty if
	// none.
	ContentType     string
	ContentEncoding string

	// Schema is the schema of these records. All records in one request share a schema
	// epoch: the engine never mixes epochs in one request, so a sink never reconciles two
	// shapes.
	Schema *schema.Ref

	// Attempt is 1 on first delivery, incrementing on retry. A sink may switch to a safer,
	// slower path on a late attempt.
	Attempt int

	// IdempotencyKey is a stable, engine-derived key for this exact request content,
	// present when the source declares StableKeys. A sink that supports server-side
	// idempotency forwards it; one that does not ignores it. This is idempotency layer
	// three.
	IdempotencyKey string
}

// WriteResult reports what happened, per record where the sink can say.
type WriteResult struct {
	// Failed names records that did not land, each with its own class and reason.
	Failed []fault.RecordFault `json:"failed,omitempty"`

	// Deferred names records that were ACCEPTED but are not yet durable, and that MUST NOT
	// be resent.
	//
	// It is the fourth answer a [Flusher] needs and the quadrant table did not have. Flush
	// inherits Write's four quadrants, where an empty Failed plus a nil error means
	// everything covered is durable and a non-empty Failed plus a nil error means the named
	// ones are not durable AND must be retried. A sink whose durability cadence is coarser
	// than the core's checkpoint cadence — a 30-second warehouse batch inside a 1-second
	// checkpoint — has to say something else entirely: nothing new landed, nothing is lost,
	// do not resend, do not advance the cursor past these. Saying it as Failed causes a
	// resend of data the sink is still holding; saying it as durable is a lie.
	//
	// The core settles nothing for a deferred record and keeps it in flight against the
	// lane's budget, so the honest answer costs the sink exactly the backpressure it
	// deserves. It is meaningful only from [Flusher.Flush]; from Write it is
	// fault.PermanentContract, because a Write that defers everything is a Flusher that
	// did not declare itself.
	Deferred []record.RecordID `json:"deferred,omitempty"`

	// Duplicates names records the destination recognised as already present. They count
	// as DURABLE — that is the whole point of an idempotent write — and are reported
	// separately so the rate is visible. A spike after restart is expected; a sustained
	// one is a symptom.
	//
	// Reporting a duplicate as SUCCESS is the direct fix for the design-rule-R5 bug where
	// an event became permanently unresubmittable: "duplicate" must mean "already durably
	// stored", never "present in a RAM cache".
	Duplicates []record.RecordID `json:"duplicates,omitempty"`

	// Written and Bytes feed the reconciliation pair — records in versus records out per
	// checkpoint — and are CHECKED, not merely reported.
	Written int64 `json:"written"`
	Bytes   int64 `json:"bytes"`

	// DestToken lets a sink hand back its own commit identity for display. Opaque,
	// rendered, never parsed.
	DestToken string `json:"dest_token,omitempty"`
}

// Reconcile checks the mandatory identity
//
//	Written == count - len(Failed) - len(Deferred)
//
// and reports the discrepancy when it does not hold. Deferred records are neither durable
// nor failed, so they are subtracted exactly as Failed is: three dispositions, one
// identity, no record unaccounted for.
//
// It lives here, next to the contract it enforces, so the check cannot drift from the
// documentation of the check.
func (r WriteResult) Reconcile(count int) (ok bool, want int64) {
	want = int64(count) - int64(len(r.Failed)) - int64(len(r.Deferred))
	return r.Written == want, want
}

// AllWritten is the happy-path convenience: every record in a request of n records is
// durable.
func AllWritten(n int) WriteResult { return WriteResult{Written: int64(n)} }

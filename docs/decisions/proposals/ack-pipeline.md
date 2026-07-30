# Proposal: the acknowledgement pipeline

**Status:** draft. One of several competing architecture proposals; a synthesis is expected to be drawn
from the strongest parts of each. This document commits fully to its angle and does not hedge.

**Angle:** end-to-end acknowledgement as the primary correctness primitive. There is no central offset
store on the commit path. A source hands out a batch together with a position; durability flows
*backward* from the sink; and the source alone decides what a settled batch means for its own progress
— advance a cursor, delete a queue message, commit a resume token, or nothing at all. Durable
persistence of a source's position is an **optional helper**, not a core coupling.

---

## Thesis

The bet is that *"this data is durable downstream"* is a strictly more general primitive than *"the
framework stored offset X for partition Y"*, and that every hard property canal needs — backpressure,
fan-out, 1→N expansion, rebatching, filtering, snapshot-then-stream, restart safety — is either free
or nearly free once acknowledgement is the spine, whereas each one is a special case in the core if
offsets are the spine.

The two known costs of a pure ack pipeline are that the framework cannot answer *"where are we?"* and
that every source reimplements contiguous-prefix resolution badly. Both costs come from the same
mistake, made independently by Benthos and Vector: **the ack was expressed as a closure the connector
owns.** A closure is invisible to the core, uncountable, untimeable, unbounded, and unshippable over a
wire.

So this design keeps the ack graph and deletes the closure. The core owns a **Ledger**: a per-lane
contiguous-prefix resolver over connector-authored opaque positions. The source's side of the contract
is one ordinary method, `Commit(ctx, Ack) error`. Because the resolver is core-owned, the core knows
the committed watermark, the pending count, the oldest unsettled group, and the exact size of the
replay window — which is the entire observability story, delivered without the core ever interpreting a
position. Because the contract is a method over bytes rather than a closure, it survives a process
boundary unchanged.

One sentence: **the ack graph is the mechanism, the ledger is the observer, the connector's position is
opaque bytes plus a display string, and persistence is opt-in.**

---

## Reading order for the interfaces

Nine packages, strictly layered. Every type below is real Go, `go 1.23`.

---

## 1. `record` — the envelope

Decided first, per design rule R2. It depends on nothing.

```go
// Package record defines canal's canonical in-flight record model.
//
// The envelope is a plain struct owned by the core. It is deliberately not
// generic: a type parameter on the record would propagate to Source, Sink,
// Buffer, Transform, Codec and the registry, and would then have to be erased
// at the registry boundary, which buys nothing (see docs/research/go-idiom.md).
//
// Growth discipline: new capabilities are added as *fields on core structs*,
// never as methods on connector interfaces. A struct field is a
// source-compatible addition; an interface method is not. This is the single
// rule that lets canal reach v3 without breaking a v1 connector.
package record

import (
	"iter"
	"time"
)

// PipelineID identifies a configured pipeline. Stable across restarts and
// across config revisions of the same pipeline.
type PipelineID string

// StreamName is a source-declared logical stream: a table, a topic, a
// collection, an endpoint, a file glob. The core never parses it and attaches
// no meaning to its shape. For a source with exactly one stream, connectors
// should use the literal name "default".
type StreamName string

// LaneID identifies one independently-ordered, independently-resumable
// progress domain within a source. It is assigned by the core when the source
// announces the lane, and is stable across restarts.
//
// A lane is the *only* unit of parallelism and the *only* unit of resume in
// canal. A source with a single monotonic cursor has one lane. A source doing
// an eight-way parallel scan while tailing a log has nine.
type LaneID string

// GroupID identifies one settlement group: the set of records that were
// admitted to the ledger together and that resolve together. One source batch
// becomes one group. A 1→N transform adds references to the same group rather
// than creating a new one, which is why fan-out and expansion need no core
// special-casing.
type GroupID uint64

// RecordID is a stable, framework-assigned, process-local identity for one
// in-flight record. It is assigned once at admission and never changes, not
// through transforms, not through rebatching, not through fan-out.
//
// It exists because positional identity within a batch is a proven mistake:
// Benthos marks its own WalkMessages "// Deprecated: This method is harmful"
// for exactly this reason. Every per-record outcome in canal — partial batch
// failure, retry targeting, dead-lettering, settlement — is keyed on RecordID
// and never on an index.
type RecordID uint64

// Op is the change operation of a Change facet. Closed set: it is used as a
// metric label, so it must stay bounded.
type Op uint8

const (
	OpUnknown Op = iota
	OpInsert
	OpUpdate
	OpDelete
	OpTruncate
	// OpScanRead marks a record produced by a full-scan lane rather than by an
	// incremental one. It is informational: no core code branches on it. The
	// built-in "scan_shadow" transform uses it, and idempotent sinks may.
	OpScanRead
)

// Completeness states how much of the pre/post image a Change facet actually
// carries. It exists because a source frequently *cannot* produce a full
// before-image, or even a full after-image (Postgres logical decoding with an
// unchanged TOAST value being the canonical case). A sink that needs a full
// image can then refuse honestly instead of silently writing a hole.
type Completeness uint8

const (
	CompletenessUnknown Completeness = iota
	CompletenessFull                 // Before and After are both complete where present
	CompletenessAfterOnly            // After is complete; Before is absent by design
	CompletenessAfterPartial         // After omits unchanged fields
	CompletenessKeyOnly              // only Keys are meaningful
)
```

### 1.1 The value model

```go
// Value is canal's field value type: a sealed sum with a closed member set.
//
// It is an interface with an unexported method rather than `any` plus a
// documented type set (which is what database/sql does) because third parties
// must not be able to widen the set — a widened set is a checkpoint-format
// break and a codec break at the same time. It is not a type parameter
// because a record holds heterogeneous fields.
type Value interface {
	isValue()
	// Kind reports the member of the closed set. Safe as a metric label.
	Kind() Kind
}

// Kind enumerates the closed value set.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindBytes
	KindTime
	KindDecimal
	KindList
	KindMap
)

type (
	Null    struct{}
	Bool    bool
	Int     int64
	Float   float64
	String  string
	Bytes   []byte
	Time    time.Time
	Decimal struct { // arbitrary-precision, transport-neutral
		Unscaled []byte // two's-complement big-endian
		Scale    int32
	}
	List []Value
	Map  map[string]Value
)

func (Null) isValue()    {}
func (Bool) isValue()    {}
func (Int) isValue()     {}
func (Float) isValue()   {}
func (String) isValue()  {}
func (Bytes) isValue()   {}
func (Time) isValue()    {}
func (Decimal) isValue() {}
func (List) isValue()    {}
func (Map) isValue()     {}

func (Null) Kind() Kind    { return KindNull }
func (Bool) Kind() Kind    { return KindBool }
func (Int) Kind() Kind     { return KindInt }
func (Float) Kind() Kind   { return KindFloat }
func (String) Kind() Kind  { return KindString }
func (Bytes) Kind() Kind   { return KindBytes }
func (Time) Kind() Kind    { return KindTime }
func (Decimal) Kind() Kind { return KindDecimal }
func (List) Kind() Kind    { return KindList }
func (Map) Kind() Kind     { return KindMap }
```

### 1.2 Position — the whole reason the core can report progress

```go
// Position is a source's own notion of "where am I", in the only form the core
// is willing to hold: opaque bytes it never parses, plus enough core-readable
// scalars to answer an operator's questions.
//
// This is the single most important type in this proposal. It is what makes an
// ack-based design observable, which is the property Benthos and Vector both
// lack and which their own dossiers call disqualifying for canal.
type Position struct {
	// Seq is assigned by the core, monotonically increasing within a lane, at
	// the moment the batch is admitted. The connector must not set it; the core
	// overwrites it. Because it is core-assigned, the core can compute records
	// read, records committed, in-flight depth and replay-window size for any
	// source whatsoever without understanding Token.
	Seq uint64

	// Token is the connector's resume payload: a binlog coordinate, an LSN, a
	// resume token, a key range boundary, a file+byte offset, a page cursor.
	// The core never parses, compares or truncates it. It is written to and read
	// from durable storage verbatim.
	//
	// Invariant the connector must uphold: given Token, the source can resume
	// such that no record at or before this position is skipped. Duplicates are
	// permitted; gaps are not.
	Token []byte

	// TokenCodec versions Token's encoding, connector-authored, connector-owned.
	// Everything in canal that crosses a role boundary or hits disk is
	// (version, []byte). A connector must refuse a TokenCodec it does not
	// recognise with fault.PermanentContract, so a downgrade fails loudly
	// instead of misparsing.
	TokenCodec uint16

	// Label is a short human-readable rendering of this position, authored by
	// the connector. "binlog.000042:1273", "lsn 0/1A2B3C4", "id > 'acme-991'".
	//
	// The core renders it and never parses it. This is how the frontend shows a
	// meaningful position for an arbitrary connector with zero
	// connector-specific UI code: the connector supplies the string, the UI
	// supplies the box.
	Label string

	// Safe reports whether resuming from Token is gap-free. A source that emits
	// records mid-transaction sets Safe=false on those positions and Safe=true
	// only at a transaction boundary, because resuming mid-transaction can skip
	// a TABLE_MAP or a partial commit.
	//
	// The ledger resolves the contiguous prefix over *all* positions but only
	// ever commits a position with Safe=true. So "the committed position is the
	// last safe point at or before the delivered prefix" is a core invariant
	// rather than a per-connector convention that MySQL connectors get right and
	// others do not.
	Safe bool

	// At is when the source observed this position. Used for
	// canal_checkpoint_age_seconds and for the freshness display. Zero means
	// unknown, and the core reports unknown rather than zero.
	At time.Time
}
```

### 1.3 Origin — structurally immutable provenance

```go
// Origin is a record's immutable provenance. It is set once, by the core, at
// admission, and there is no exported way to mutate it.
//
// This is a direct response to Kafka Connect's KIP-793 retrofit: its SMTs
// rewrite topic/partition while offset accounting needs the pre-transform
// coordinates, so originalTopic/originalPartition/originalOffset had to be
// bolted on plus prose warnings in two javadocs. Here a transform cannot
// corrupt settlement identity because it has no access to the fields
// settlement uses.
type Origin struct {
	Pipeline PipelineID
	Source   string     // registered connector name, not an instance label
	Lane     LaneID
	Group    GroupID
	ID       RecordID
	Stream   StreamName // the source's stream. Transforms rewrite Record.Dest, never this.
	Key      []byte     // source-derived stable identity, may be nil
	ReadAt   time.Time
	// Parent is non-zero when this record was derived from another by a 1→N
	// transform. Both share Group, so both settle together.
	Parent RecordID
}
```

### 1.4 Payload, Meta, Change, Record

```go
// Payload is a dual-view body: raw bytes and/or a structured value, whichever
// is currently materialised, converted lazily and at most once.
//
// Mutability is in the accessor name, following Benthos, because the alternative
// (a Mutable() bool flag) is a rule nobody reads.
type Payload struct {
	b   []byte
	v   Value
	has uint8 // bit 0: bytes valid, bit 1: structured valid
}

func BytesPayload(b []byte) Payload      { return Payload{b: b, has: 1} }
func StructuredPayload(v Value) Payload  { return Payload{v: v, has: 2} }

// Bytes returns a read-only view of the encoded body, materialising it from the
// structured view if necessary using the pipeline's configured encoder.
// The caller must not retain or modify the returned slice.
func (p *Payload) Bytes() ([]byte, error)

// BytesCopy returns an owned copy. The core calls this, not the connector,
// whenever a record crosses into a buffer or a fan-out — ownership transfer is
// never trusted to a connector.
func (p *Payload) BytesCopy() ([]byte, error)

// Structured returns a read-only structured view. Mutating the returned Value
// is a contract violation.
func (p *Payload) Structured() (Value, error)

// StructuredMut returns a structured view the caller may mutate, copying first
// if the current view is shared. Invalidates the byte view.
func (p *Payload) StructuredMut() (Value, error)

func (p *Payload) SetBytes(b []byte)
func (p *Payload) SetStructured(v Value)
func (p *Payload) Len() int      // encoded length if known, else -1
func (p *Payload) IsEmpty() bool

// Meta is a namespaced metadata sidecar, addressable separately from the
// payload so that a transform touching metadata never rewrites the body and a
// codec encoding the body never has to decide what to do with metadata.
//
// Namespaces: "canal" is reserved and core-owned (read-only to connectors),
// "source" is the connector's, "user" is the operator's. Secrets live in a
// fourth, unlisted namespace that is never serialised, never logged, never
// exported to the read model, and never visible to a codec.
type Meta struct{ /* ... */ }

func (m *Meta) Get(ns, key string) (Value, bool)
func (m *Meta) Set(ns, key string, v Value) error // error on ns == "canal"
func (m *Meta) Delete(ns, key string)
func (m *Meta) All(ns string) iter.Seq2[string, Value]
func (m *Meta) Namespaces() iter.Seq[string]

// SetSecret stores a value that the core guarantees will not appear in any
// serialised form, any log line, any metric label or any API response.
func (m *Meta) SetSecret(key, v string)
func (m *Meta) Secret(key string) (string, bool)

// Change is the optional typed change-event facet.
//
// It exists because total genericity provably does not hold: Benthos's MySQL
// and Postgres CDC inputs invent different op vocabularies and different
// position keys, so every CDC-aware sink ends up special-casing every source.
// It is optional data rather than a core-mandated shape, so the core still
// never switches on source type and the relational shape is never forced onto a
// webhook, a metrics scrape or a file tail.
type Change struct {
	Version      uint16 // facet vocabulary version, core-owned
	Op           Op
	Before       *Payload // nil is legal; Completeness says why
	After        *Payload
	Completeness Completeness
	Keys         []string // field names forming the record key, in order
	TxID         string   // opaque transaction identity, for grouping
	CommitTime   time.Time
}

// SchemaRef names a schema by content fingerprint plus an epoch that orders
// schema changes within a lane. The schema body itself lives in the pipeline's
// schema table, deduplicated, so a record carries 24 bytes rather than a schema.
type SchemaRef struct {
	Fingerprint [16]byte
	Epoch       uint64
	Stream      StreamName
}

// Record is the envelope.
type Record struct {
	origin Origin // set once by the core; no exported mutator exists

	// Dest is the routing target a transform may rewrite. It starts equal to
	// Origin().Stream. Two fields for two concepts: one is identity, one is
	// destination. This is the fix for the conflation, not a violation of R9.
	Dest StreamName

	EventTime time.Time // zero means the source has no event time
	Payload   Payload
	Meta      Meta
	Change    *Change    // optional facet
	Schema    *SchemaRef // optional

	fault error // set by MarkFailed; read by the engine's policy, not by connectors
}

func (r *Record) Origin() Origin { return r.origin }

// Copy returns a deep copy carrying the same Origin and the same RecordID: it
// is the same record, materialised twice (for fan-out to two sinks).
func (r *Record) Copy() *Record

// Derive returns a new record in the same settlement group with a fresh
// RecordID and Parent set. This is how a 1→N transform expands: the group is
// unchanged, so settlement still resolves correctly, and the framework
// refcounts. The caller must call the ledger's Fanout for the group.
func (r *Record) Derive() *Record

// MarkFailed attaches a fault to this record and lets it continue. The engine's
// configured policy decides whether that means DLQ, drop or pipeline failure.
func (r *Record) MarkFailed(err error)
func (r *Record) Failed() (error, bool)

// Ref is the minimum a sink needs in order to report a per-record outcome:
// identity and provenance, no payload.
type Ref struct {
	ID     RecordID
	Group  GroupID
	Lane   LaneID
	Stream StreamName
	Key    []byte
}

func (r *Record) Ref() Ref

// Batch is a caller-owned, reusable buffer. The engine allocates one per stage
// and passes the same pointer every iteration, following driver.Rows.Next.
//
// A batch — not a record — is the unit a Position attaches to. This matches
// what real connectors do (Benthos's MySQL input tracks one position per
// flushed batch) and it means commit points align with source-meaningful
// boundaries rather than with a clock.
type Batch struct {
	Records  []*Record
	Lane     LaneID
	Position Position // position AFTER the last record in Records

	// EndOfLane is set by the source on the final batch of a bounded lane. The
	// engine finishes the lane once this batch settles. A source may also set it
	// with zero records.
	EndOfLane bool

	group GroupID // set by the core at admission
}

func (b *Batch) Reset()
func (b *Batch) Append(r *Record)
func (b *Batch) Len() int
func (b *Batch) Group() GroupID
func (b *Batch) All() iter.Seq[*Record] // ergonomic adapter only; never the primary API
```

---

## 2. `fault` — error classification

Depends on nothing but `record`.

```go
// Package fault defines canal's closed error-classification set and the retry
// policy shape.
//
// The set is closed for three reasons: a closed set is a legitimate bounded
// metric label; a closed set makes "a hint the framework ignores" impossible
// (Benthos's ErrBackOff is honoured only on Connect, which is exactly this bug);
// and a closed set can be rendered by a UI without connector-specific code.
package fault

import (
	"time"

	"github.com/BernardoCSACarreira/canal/record"
)

// Class is the ownership taxonomy: whose problem is this?
//
// This is the question an operator UI actually needs answered — my config,
// their system, or a blip — and it is the axis Airbyte and Vector converged on
// independently. It is declared by the connector at the point of raise, which
// is the only place where the answer is actually known.
type Class uint8

const (
	// Unclassified is the zero value and is always a defect. The engine treats
	// it as PermanentInternal and increments canal_unclassified_faults_total,
	// which the conformance kit asserts stays at zero.
	Unclassified Class = iota

	// TransientUpstream: the remote system is temporarily unable. 503, timeout,
	// connection reset, throttle. Back off and retain progress.
	TransientUpstream

	// TransientInternal: canal itself is temporarily unable. Out of buffer,
	// lease contention, local disk pressure. Back off and retain progress.
	TransientInternal

	// PermanentUpstream: the remote system refuses and will keep refusing.
	// 401, 403, table dropped, quota exhausted. Stop and go terminal — do not
	// poison-retry.
	PermanentUpstream

	// PermanentMapping: this record cannot be represented for this destination.
	// Unencodable value, missing required field, type mismatch. Dead-letter the
	// record; the pipeline is healthy.
	PermanentMapping

	// PermanentContract: the two ends disagree structurally. Unknown TokenCodec,
	// unsupported schema change, capability mismatch discovered at runtime.
	// Stop the pipeline: retrying cannot help and continuing risks corruption.
	PermanentContract

	// DuplicateIdempotent: the write was refused because it already landed.
	// This is a *success* for settlement purposes and is counted separately so
	// that "we are re-delivering a lot" is visible.
	DuplicateIdempotent

	// ClockSkew: a timestamp is implausible relative to the local clock.
	// Behaviour is configured (clamp or reject), never silently chosen.
	ClockSkew

	// NotConnected: the component has lost its connection and needs Open called
	// again before any further call. Control flow, not failure.
	NotConnected

	// EndOfInput: this source instance has nothing more to produce, ever. Its
	// lanes are all finished. Control flow, not failure.
	EndOfInput
)

// Retryable reports whether the class permits another attempt. Used by the
// engine; not overridable by a connector, because "the connector said retry but
// the framework disagreed" is the bug this closed set exists to prevent.
func (c Class) Retryable() bool {
	switch c {
	case TransientUpstream, TransientInternal, NotConnected:
		return true
	}
	return false
}

// Settles reports whether the class should settle the affected records as
// delivered. Only DuplicateIdempotent does.
func (c Class) Settles() bool { return c == DuplicateIdempotent }

// Terminates reports whether the class must stop the pipeline rather than
// dead-letter the record.
func (c Class) Terminates() bool {
	switch c {
	case PermanentUpstream, PermanentContract:
		return true
	}
	return false
}

func (c Class) String() string // closed, snake_case, stable: safe as a metric label

// Op is the stage at which a fault was raised. Closed set; a metric label.
type Op uint8

const (
	OpUnknown Op = iota
	OpOpen
	OpRead
	OpDecode
	OpTransform
	OpEncode
	OpWrite
	OpCommit
	OpBuffer
	OpPersist
	OpDiscover
	OpValidate
)

func (o Op) String() string

// Fault is canal's error type. Connectors construct it; the engine reads it.
//
// The two message fields are deliberately separate, following Airbyte: the
// operator sees UserMessage, the log carries DevMessage, and neither is a
// truncation of the other.
type Fault struct {
	Class Class
	Op    Op

	Stream record.StreamName // optional
	Lane   record.LaneID     // optional
	Record record.RecordID   // zero when not record-scoped

	// RetryAfter is honoured verbatim by the engine's backoff when non-zero:
	// this is where a Retry-After header lands.
	RetryAfter time.Duration

	// UserMessage is shown to an operator. No stack traces, no Go types, no
	// internal identifiers. It should say what to do.
	UserMessage string

	// DevMessage is for the log and the developer. Anything goes.
	DevMessage string

	Err error
}

func (f *Fault) Error() string
func (f *Fault) Unwrap() error

// New constructs a Fault. Connector authors are expected to use this or one of
// the Class-named helpers rather than fmt.Errorf, and the conformance kit fails
// a connector whose returned errors do not classify.
func New(c Class, op Op, err error) *Fault

func Transient(op Op, err error) *Fault  // TransientUpstream
func Permanent(op Op, err error) *Fault  // PermanentUpstream
func Mapping(op Op, err error) *Fault    // PermanentMapping
func Contract(op Op, err error) *Fault   // PermanentContract

// ClassOf walks the error chain with errors.As and returns the innermost
// declared class, or Unclassified. Every engine call site uses this; nothing
// uses == or a type switch on a concrete error.
func ClassOf(err error) Class

// Sentinels. These wrap into Fault so errors.Is works through connector
// wrapping, following driver.ErrBadConn's documented contract.
var (
	ErrNotConnected = New(NotConnected, OpUnknown, errNotConnected)
	ErrEndOfInput   = New(EndOfInput, OpRead, errEndOfInput)

	// ErrSkip declines an optional fast path for this call only, continuing as
	// if the optional interface were not implemented. Supported only where
	// explicitly documented (currently: RecordSink.WriteRecords,
	// Partitioner.Partition, BacklogReporter.Backlog).
	ErrSkip = New(TransientInternal, OpUnknown, errSkip)
)

// RETRY-SAFETY OBLIGATION, stated here because it cannot be enforced by the
// compiler and is asserted by the conformance kit:
//
//	A connector may return a retryable Class only when it knows the effect did
//	not land. If the remote may have applied the write, the class must be
//	PermanentUpstream or DuplicateIdempotent, never TransientUpstream.
//
// This is driver.ErrBadConn's rule ("should NOT be returned if there's a
// possibility that the database server might have performed the operation")
// generalised, and it is design rules R4 and R5 in one sentence.
```

### 2.1 Per-record batch failure, keyed on identity

```go
// BatchError reports a partially-applied write. It degrades to its headline for
// consumers that cannot handle granularity, following Benthos's good idea, and
// it is keyed on record.RecordID rather than on a batch index, avoiding
// Benthos's bad one.
//
// There is deliberately no positional walk method. Not deprecated — absent.
type BatchError struct {
	headline *Fault
	failed   map[record.RecordID]*Fault
}

// NewBatchError requires a headline: the fault that applies to every record for
// which Fail was not called. If Fail is never called, every record in the batch
// carries the headline.
func NewBatchError(headline *Fault) *BatchError

// Fail records a per-record outcome. Chainable. Calling Fail at least once
// switches the batch to "everything not named here succeeded".
func (e *BatchError) Fail(id record.RecordID, f *Fault) *BatchError

// Succeeded explicitly marks a record as landed even though the batch as a
// whole errored. Needed by sinks whose response is "rows 0-9 ok, then I died".
func (e *BatchError) Succeeded(id record.RecordID) *BatchError

func (e *BatchError) Headline() *Fault
func (e *BatchError) Len() int
func (e *BatchError) Each(fn func(record.RecordID, *Fault) bool)
func (e *BatchError) Error() string
func (e *BatchError) Unwrap() error // returns headline
```

### 2.2 Retry policy — unbounded retry is inexpressible

```go
// RetryPolicy is the complete retry contract. There is no "retry forever"
// value: Terminal has no valid zero, so a policy that never gives up cannot be
// constructed. Benthos livelocks on a poison record because its default was
// unbounded; Vector's effectively-infinite default is only tenable because of
// its buffer layers. canal refuses the option.
type RetryPolicy struct {
	MaxAttempts int // >= 1; 1 means no retry
	Backoff     Backoff
	Terminal    Terminal // required
}

// Terminal is the disposition once MaxAttempts is exhausted.
type Terminal uint8

const (
	TerminalInvalid Terminal = iota // zero value; Validate rejects it
	TerminalDLQ                     // route to the pipeline's dead-letter sink, settle Abandoned
	TerminalDrop                    // discard and count, settle Abandoned
	TerminalFail                    // stop the pipeline; do not settle
)

// Backoff is full-jitter exponential, the only strategy canal offers, plus an
// escalation attempt. Following database/sql's retry(), the final attempt uses
// a *different* strategy — the engine forces a reconnect before it — because an
// escalation is more useful than one more identical try.
type Backoff struct {
	Initial     time.Duration
	Max         time.Duration
	MaxElapsed  time.Duration // 0 means bounded only by MaxAttempts
	Multiplier  float64
	FullJitter  bool
}

func (p RetryPolicy) Validate() error // rejects TerminalInvalid, MaxAttempts < 1
func (p RetryPolicy) Next(attempt int) (time.Duration, bool)
```

---

## 3. `config` — one declaration, four consumers

Depends on `fault`.

```go
// Package config is the connector's self-description. One artefact drives
// validation, defaults, reference docs, JSON Schema for editors, and a live
// form in the frontend.
//
// This is the entire answer to canal's frontend goal. Every system that shipped
// a usable connector UI declared config as data and no other way.
package config

import (
	"context"
	"time"
)

// Spec is a connector's config declaration. Built once at init time, frozen
// thereafter, and exported as data.
type Spec struct {
	Summary     string
	Description string
	Footnotes   string
	Version     string
	Deprecated  bool
	Fields      []Field
	// Examples are complete, valid configs. The conformance kit parses and
	// validates every one, so a stale example fails CI (design rule R10).
	Examples []Example
	// Lints are declarative cross-field rules, evaluated offline with no I/O.
	Lints []Lint
}

func NewSpec() *Spec
func (s *Spec) Field(f Field) *Spec
func (s *Spec) Lint(l Lint) *Spec

// FieldType is the closed set of declarable field types.
type FieldType uint8

const (
	TypeString FieldType = iota
	TypeInt
	TypeFloat
	TypeBool
	TypeDuration
	TypeBytesSize   // "64MiB"
	TypeEnum
	TypeObject      // nested Fields
	TypeArray       // Item
	TypeMap         // Item as value type
	TypeUnion       // Variants, discriminated
	TypeComponent    // a nested registered component (see ComponentKinds)
	TypeExpression   // a record-scoped expression, see §3.2
	TypeSchemaMapping // sink field mapping, see §3.2
)

// Field is one declared config field. Note that this struct is *data*: it
// serialises to JSON, so the frontend, the docs generator, the linter and the
// validator all consume the identical artefact and cannot drift.
type Field struct {
	Name        string // wire name, snake_case
	Title       string // display name; defaults to a title-cased Name
	Description string
	Type        FieldType

	Default  any
	Optional bool
	Advanced bool

	// Secret means the core redacts this value everywhere: logs, metrics,
	// the read model, error messages, config round-trips. Zero connector
	// involvement.
	Secret bool

	Deprecated  string // non-empty: the replacement, or "no replacement"
	Examples    []any
	Enum        []EnumValue // TypeEnum
	Fields      []Field     // TypeObject
	Item        *Field      // TypeArray, TypeMap
	Variants    []Variant   // TypeUnion
	ComponentKinds []Kind   // TypeComponent: which registries may satisfy it

	Min, Max *float64
	Pattern  string

	// ShowIf hides the field in a form unless the predicate holds. Declarative,
	// so the browser evaluates it without a round trip.
	ShowIf *Predicate
	// RequiredIf makes the field conditionally required. Segment's
	// action-destinations proved conditional-required-as-a-predicate is what a
	// specialised sink UI actually needs.
	RequiredIf *Predicate

	// Choices names a dynamic-choice hook the connector implements (see
	// connector.ChoiceProvider). The frontend calls
	// GET /v1/connectors/{name}/choices/{hook} with the partial config. This is
	// how "pick a table from this database" works with no core knowledge of
	// tables.
	Choices string
}

// EnumValue carries display metadata so a form renders a select honestly.
type EnumValue struct {
	Value       string
	Title       string
	Description string
	Deprecated  bool
}

// Variant is one arm of a tagged union: a discriminator constant plus its
// fields. This is what Kafka Connect's ConfigDef cannot express and fakes with
// dotted prefixes, and what Airbyte gets from JSON Schema oneOf + const.
type Variant struct {
	Tag         string // value of the union's discriminator field
	Title       string
	Description string
	Fields      []Field
}

// Predicate is a declarative, side-effect-free condition over the config being
// edited. Closed set of operators so it is trivially evaluable in Go and in the
// browser, and so it cannot become an embedded language by accident.
type Predicate struct {
	Path  string // dotted path, relative to the enclosing object
	Op    PredOp
	Value any
	All   []Predicate // conjunction
	Any   []Predicate // disjunction
	Not   *Predicate
}

type PredOp uint8

const (
	PredEquals PredOp = iota
	PredNotEquals
	PredIn
	PredPresent
	PredTruthy
	PredGreaterThan
	PredLessThan
	PredMatches
)

func (p *Predicate) Eval(c *Config) bool
```

### 3.1 Parsed config, diagnostics, validation

```go
// Config is a parsed, defaulted, validated config tree handed to a connector
// constructor. There is no Configure() callback and no map re-parsed inside the
// connector: by the time a connector exists, its config is correct.
type Config struct{ /* ... */ }

func (c *Config) String(path ...string) (string, error)
func (c *Config) Int(path ...string) (int, error)
func (c *Config) Float(path ...string) (float64, error)
func (c *Config) Bool(path ...string) (bool, error)
func (c *Config) Duration(path ...string) (time.Duration, error)
func (c *Config) BytesSize(path ...string) (int64, error)
func (c *Config) StringList(path ...string) ([]string, error)
func (c *Config) StringMap(path ...string) (map[string]string, error)
func (c *Config) Object(path ...string) (*Config, error)
func (c *Config) List(path ...string) ([]*Config, error)
func (c *Config) Union(path ...string) (tag string, cfg *Config, err error)
func (c *Config) Has(path ...string) bool

// Secret returns a secret value. It is a distinct accessor so that a code review
// can grep for every place a secret is read, and so the core can count reads.
func (c *Config) Secret(path ...string) (string, error)

// Raw returns the config as a redacted, JSON-serialisable tree: secrets are
// replaced with a marker. The read model and every log line use this.
func (c *Config) Redacted() map[string]any

// Severity of a diagnostic.
type Severity uint8

const (
	SeverityError Severity = iota
	SeverityWarning
	SeverityInfo
)

// Code is the closed diagnostic vocabulary. Closed because it is both a metric
// label and an i18n key namespace (design rule R9: one wire enum plus one i18n
// key namespace).
type Code uint8

const (
	CodeUnknownField Code = iota // the one that catches typo'd YAML
	CodeMissingField
	CodeWrongType
	CodeOutOfRange
	CodeInvalidEnum
	CodeInvalidPattern
	CodeDeprecated
	CodeUnknownComponent
	CodeIncompatibleCapability
	CodeUnreachable       // tier 2: connector did I/O and could not connect
	CodeAuthFailed        // tier 2
	CodeNotFound          // tier 2: named object does not exist
	CodePermissionDenied  // tier 2
	CodeCustom
)

// Diagnostic is one problem, anchored to a field path so a form can render it
// inline. Line/Column are populated when the config came from a text file.
type Diagnostic struct {
	Path     string
	Severity Severity
	Code     Code
	Message  string
	Hint     string // what to do about it
	Line     int
	Column   int
}

// Diagnostics is the return of every validation entry point. All problems at
// once, never fail-fast: a form that shows one error at a time is a form
// operators fight.
type Diagnostics []Diagnostic

func (d Diagnostics) HasErrors() bool
func (d Diagnostics) Error() string
func (d Diagnostics) Filter(s Severity) Diagnostics

// Lint is a declarative cross-field rule evaluated with no I/O. Tier one of
// two-tier validation.
type Lint struct {
	When     Predicate
	Code     Code
	Path     string
	Severity Severity
	Message  string
	Hint     string
}

// Validate runs tier one: structure, types, ranges, enums, unknown fields,
// declared lints. Pure, offline, fast, browser-runnable via the exported JSON
// Schema plus predicate set.
func (s *Spec) Validate(raw map[string]any) (*Config, Diagnostics)

// JSONSchema exports the spec as JSON Schema draft 2020-12 with canal's
// presentation keywords (x-canal-advanced, x-canal-secret, x-canal-show-if,
// x-canal-choices, x-canal-component-kinds). Editors get completion for free;
// the frontend does not use it as its form model — it uses Spec directly, which
// is a closed vocabulary rather than arbitrary JSON Schema.
func (s *Spec) JSONSchema() ([]byte, error)

// Docs renders reference documentation. One artefact, so docs cannot drift from
// behaviour.
func (s *Spec) Docs() ([]byte, error)
```

### 3.2 Composite field specs — the reason every connector's `retry:` block looks the same

```go
// Fields provides pre-built, reusable field fragments with matching extractors.
// This pairing is the single most transplantable idea in Benthos's config model:
// every connector's retry, batching, TLS, buffer and codec block looks
// identical, documents itself identically and renders identically, with zero
// coordination between connector authors and zero switches in the core.
var Fields fields

type fields struct{}

func (fields) Retry(name string) Field
func (fields) Batching(name string) Field
func (fields) TLS(name string) Field
func (fields) BasicAuth(name string) Field
func (fields) OAuth2(name string) Field
func (fields) HTTPClient(name string) Field
func (fields) Codec(name string) Field   // encoder + framer + compression
func (fields) Buffer(name string) Field
func (fields) RateLimit(name string) Field
func (fields) LaneBudget(name string) Field
func (fields) Snapshot(name string) Field // never | initial | always, explicit, never inferred
func (fields) SchemaDrift(name string) Field
func (fields) DedupeKey(name string) Field

// Matching extractors, on *Config.
func (c *Config) Retry(path ...string) (fault.RetryPolicy, error)
func (c *Config) Batching(path ...string) (BatchPolicy, error)
func (c *Config) TLS(path ...string) (*tls.Config, error)
func (c *Config) Codec(path ...string) (CodecChain, error)
// ... one per composite, always named for the field constructor.

// Mapping is a declarative field mapping over the generic record, used by
// TypeSchemaMapping. It is how a specialised sink UI ("map these record fields
// onto these destination columns") is expressed as *data* and therefore needs
// no core change when a new sink wants it.
type Mapping struct {
	Target string // destination field name
	Source Source // where the value comes from
	Required bool
	Default  any
}

// Source is the closed set of places a mapped value may come from. Closed
// deliberately: an embedded expression language is a real dependency and this
// design declines it. Ninety percent of sink mappings are one of these five.
type Source struct {
	Kind      SourceKind
	Path      string // PayloadField: dotted path into the structured payload
	Namespace string // MetaField
	Key       string // MetaField
	Literal   any    // Literal
}

type SourceKind uint8

const (
	SourcePayloadField SourceKind = iota
	SourceMetaField
	SourceOriginKey
	SourceEventTime
	SourceLiteral
	SourceWholePayload
)
```

---

## 4. `connector` — the plugin surface

Depends on `record`, `fault`, `config`. It depends on **nothing else, ever** — not on `engine`, not on
`ledger`, not on `store`. That constraint is what makes a connector package cheap.

### 4.1 Lanes

```go
// Package connector defines every interface a connector author implements and
// every handle the core injects.
//
// Rule for the whole package: behaviour is an optional exported interface; the
// *fact* of that behaviour is declarative data in a Caps struct;
// registration cross-checks the two and panics on disagreement; and the core
// type-asserts in exactly one place (Resolve).
//
// The reason for both halves: a type assertion cannot cross a process boundary,
// and a flag with no methods behind it is worthless. Data crosses the wire;
// interfaces do the work.
package connector

// Ordering declares how a lane's positions may be resolved.
type Ordering uint8

const (
	// OrderingPrefix: positions within the lane are totally ordered and a
	// position may be committed only when every earlier position in the lane has
	// settled. This is the contiguous-prefix case: a monotonic cursor, an LSN, a
	// binlog coordinate, a file byte offset.
	OrderingPrefix Ordering = iota

	// OrderingDiscrete: positions within the lane are independent and may be
	// committed in any order, individually. This is the queue case: an SQS
	// receipt handle, an AMQP delivery tag, a Pub/Sub ack id. There is no cursor,
	// so there is no prefix to resolve.
	OrderingDiscrete
)

// Boundedness declares whether a lane ends.
type Boundedness uint8

const (
	BoundednessUnbounded Boundedness = iota // tails forever
	BoundednessBounded                      // finishes; EndOfLane will arrive
)

// LaneKind is a reporting-only classification. The core stores it, exports it to
// the read model, and uses it to compute snapshot progress percentages.
//
// NOTHING IN THE CORE BRANCHES ON IT. This is deliberate and load-bearing:
// Debezium and Airbyte both smuggled a phase into their opaque checkpoint and
// both lost snapshot progress reporting, snapshot-specific parallelism and
// re-parallelised resume. Phase belongs in core as data, never as control flow.
type LaneKind uint8

const (
	LaneKindStream LaneKind = iota // incremental, ongoing
	LaneKindScan                   // full read of existing state
	LaneKindBackfill               // bounded historical catch-up that is not a full scan
)

// LaneSpec is what a source announces. It is simultaneously the lane's
// construction payload, its resume payload, and its row in the assignment table.
//
// One type for all three, following io.SectionReader.Outer(): if the resume
// payload and the construction payload are the same type they cannot drift,
// which is the structural defence against design rule R1's dual-representation
// failure.
type LaneSpec struct {
	// Name is the source's own stable identifier for this lane. The core derives
	// LaneID from (pipeline, source, Name), so the same Name across restarts is
	// the same lane and reuses its persisted state.
	//
	// It must be derived from stable content properties, not from an ephemeral
	// handle — Vector's file source needs content fingerprinting precisely
	// because inodes get reused.
	Name string

	Stream      record.StreamName
	Kind        LaneKind
	Ordering    Ordering
	Boundedness Boundedness

	// Resume is the opaque payload the source needs to construct or reconstruct
	// this lane: a key range, a starting LSN, a shard id, a page token. It is
	// persisted with the lane row and handed back verbatim at Open.
	Resume      []byte
	ResumeCodec uint16

	// Weight is an estimated record count for progress reporting. Zero means
	// unknown, and the core reports unknown rather than zero.
	Weight uint64

	// Label is a human-readable rendering, shown verbatim in the UI.
	// "scan chunk 3/8: id in ['acme','beta')", "binlog tail".
	Label string

	// Budget overrides the pipeline's default in-flight budget for this lane.
	// Zero means use the default. A scan lane usually wants a larger budget than
	// a tail lane.
	Budget int
}

// LaneAssignment is what the core hands back: a spec plus the state the source
// last persisted for it.
type LaneAssignment struct {
	ID   record.LaneID
	Spec LaneSpec

	// Committed is the last position the core observed being committed for this
	// lane before the process stopped, if the source used the state helper. Zero
	// value means "no committed position": start from the beginning of the lane.
	Committed record.Position

	// StateVersion is the CAS version for the source's persisted blob. Passed to
	// StateHandle.Set to fence a stale writer.
	StateVersion uint64
}

// LaneCtl is how a running source announces and retires lanes. It is injected,
// not implemented.
//
// This is the planner/placer separation, in the ack-pipeline flavour: the source
// declares how work divides (by announcing lanes, continuously, whenever it
// likes), and the runtime decides where each lane runs. Neither learns the
// other's algorithm.
//
// The difference from a Flink-style enumerator is that there is no separate
// enumerator role and no assignment protocol in the required interface. A lane
// is just "a progress domain the source told us about". That is what makes
// "snapshot with 20 lanes then stream with 1" expressible without restarting
// anything, and it is what makes one-shot planning's eight-year tasks.max
// problem structurally absent.
type LaneCtl interface {
	// Open announces a lane. The core persists the spec — atomically, together
	// with any state write in flight — before returning. On return the lane
	// exists durably, so a crash cannot lose the fact that it was planned.
	//
	// This durability is the entire snapshot-to-stream handoff invariant: a
	// source announces its tail lane (capturing the log position) BEFORE it
	// announces any scan lane, and the core makes that fact survive a crash
	// without understanding what a log position is.
	Open(ctx context.Context, spec LaneSpec) (record.LaneID, error)

	// Finish retires a lane. The core will not consider the lane finished until
	// every group admitted for it has settled, so Finish is a request, not an
	// assertion. Idempotent.
	Finish(ctx context.Context, id record.LaneID) error

	// Assigned returns the lanes this source instance is responsible for right
	// now, with their persisted state. In standalone mode that is every announced
	// lane. In a cluster it is the subset this worker holds a lease on.
	//
	// A source calls this in Open and may call it again: the set can shrink while
	// running (a lease lapsed) or grow (a lease was claimed). The returned slice
	// is a snapshot.
	Assigned(ctx context.Context) ([]LaneAssignment, error)

	// Revoked returns a channel closed when a lane is no longer this instance's
	// to read. The source must stop producing for it. Records already handed over
	// still settle normally.
	Revoked(id record.LaneID) <-chan struct{}

	// Budget reports the current in-flight allowance for a lane, in records. A
	// source may use it to size its own reads. It is informational: the core
	// enforces the budget by blocking admission.
	Budget(id record.LaneID) int
}
```

### 4.2 The optional persistence helper — the angle, stated as a type

```go
// StateHandle is a lane-scoped, byte-oriented, compare-and-set durable store.
//
// THIS IS THE ONLY DURABILITY THE CORE OFFERS A SOURCE, AND IT IS OPTIONAL.
// Nothing in the core ever writes to it on a source's behalf. A source that
// does not need it (Kafka: the broker holds the offset; SQS: deleting the
// message *is* the commit; an HTTP webhook: there is no position) never touches
// it and stores nothing.
//
// This is the deliberate inversion of every offset-store design. In Kafka
// Connect the framework owns durability and the connector authors a map; here
// the connector owns durability and the framework offers a byte bucket. The
// consequence that matters: a pipeline whose source's progress lives in the
// upstream system keeps committing with canal's control plane entirely down,
// because commit does not touch canal's storage at all.
type StateHandle interface {
	// Get returns the blob and its CAS version, or (nil, 0, nil) if absent.
	Get(ctx context.Context, lane record.LaneID) ([]byte, uint64, error)

	// Set writes a blob if the stored version matches ifVersion. Zero means
	// "must not exist". Returns the new version. A version mismatch is
	// fault.PermanentContract: another writer holds this lane, and continuing
	// would clobber it. This is the second fencing token after the lease.
	Set(ctx context.Context, lane record.LaneID, blob []byte, ifVersion uint64) (uint64, error)

	// SetMany writes several lanes atomically. Required to be all-or-nothing
	// across the whole map — one SQL transaction, one bbolt transaction, one etcd
	// Txn. Kafka Connect's compacted-topic store cannot meet this and its own
	// javadoc documents the resulting unrecoverable state.
	SetMany(ctx context.Context, w map[record.LaneID]VersionedBlob) error

	Delete(ctx context.Context, lane record.LaneID) error
}

type VersionedBlob struct {
	Blob      []byte
	IfVersion uint64
}

// AutoPersist wires the common case so that the 90% source writes no
// persistence code at all: on every Commit, write the position's Token under
// the lane, CAS-fenced; on Open, hand back the stored token.
//
// It is a *helper over the interface*, not a core behaviour: it is constructed
// by the connector, in the connector's package, from its runtime. The core still
// never persists a position on its own initiative.
//
// This is the reduction Benthos leaves out-of-tree (github.com/Jeffail/checkpoint)
// and that every one of its CDC connectors therefore re-wires with its own key,
// its own format and its own bugs.
func AutoPersist(rt *SourceRuntime) *Persister

type Persister struct{ /* ... */ }

// Commit is a drop-in Source.Commit implementation.
func (p *Persister) Commit(ctx context.Context, ack Ack) error

// Restore returns the token last committed for a lane, or nil.
func (p *Persister) Restore(lane record.LaneID) ([]byte, uint16)
```

### 4.3 Source

```go
// Source is the required source interface. Four methods, frozen.
//
// Every blocking method takes a context from the first commit. Kafka Connect has
// none, and KIP-419 (a safe teardown callback) has been unfixed for seven years.
type Source interface {
	// Open establishes whatever connection the source needs and reads its
	// assigned lanes. It may do I/O. It will be called again, with backoff, after
	// any method returns fault.ErrNotConnected, so it must be idempotent.
	//
	// The context is scoped to the *opening*, not to the connection: it may be
	// cancelled once Open returns. A source needing a connection-lifetime context
	// takes it from rt.Context(). This narrowing is stated on the method because
	// database/sql found it necessary to narrow the same parameter in prose.
	Open(ctx context.Context, rt *SourceRuntime) error

	// Read fills dst with the next batch and sets dst.Lane and dst.Position.
	// It blocks until at least one record is available, the connection drops, or
	// ctx is done. dst is owned by the caller and reused; the source must not
	// retain it.
	//
	// Returns fault.ErrNotConnected to request a reconnect, or fault.ErrEndOfInput
	// when every lane it holds is finished and no more will be announced. A
	// bounded pipeline terminates cleanly this way.
	//
	// Read is NEVER called concurrently with itself. The source may therefore be
	// lock-free internally. The core guarantees it; the contract states it,
	// following driver.Conn.
	Read(ctx context.Context, dst *record.Batch) error

	// Commit is called when a lane's progress has advanced: some set of records
	// this source handed over is now durable downstream (or has reached a
	// terminal disposition that the operator configured as acceptable).
	//
	// WHAT THIS MEANS IS ENTIRELY THE SOURCE'S DECISION. Advance a cursor. Delete
	// the queue messages. Commit a consumer-group offset. Write the token via
	// rt.State(). Do nothing. The core does not care and does not check.
	//
	// Returning an error is escalated, not logged and dropped: the engine
	// classifies it, retries per policy, and raises a CommitStalled condition if
	// it cannot succeed. "We delivered the data and silently lost the progress
	// record" — Benthos's documented behaviour — is not reachable here.
	//
	// Commit is never called concurrently with itself, and never concurrently
	// with Read. Sources need no locking between them.
	Commit(ctx context.Context, ack Ack) error

	// Close releases resources. All network calls made by Close must have a
	// timeout: the core cannot enforce this and a hanging Close blocks shutdown.
	Close(ctx context.Context) error
}

// Ack is what the core tells a source about settled work.
type Ack struct {
	Lane record.LaneID

	// Through is set for OrderingPrefix lanes: the highest position such that it
	// and everything before it in this lane has settled, and Safe is true.
	// Zero-valued (Seq == 0) when Discrete is used instead.
	Through record.Position

	// Discrete is set for OrderingDiscrete lanes: exactly the positions that
	// settled, in no particular order. Nil for prefix lanes.
	Discrete []record.Position

	// Records is how many records this ack covers, for the source's own metrics
	// and for heartbeat decisions.
	Records uint64

	// Outcome distinguishes clean delivery from terminal disposition.
	Outcome Outcome

	// Abandoned is the count of records within this ack that reached a terminal
	// disposition (DLQ or drop) rather than landing at the sink. A source may
	// refuse to advance on a non-zero Abandoned — for example a source whose
	// commit is destructive (deleting a queue message) may prefer to leave the
	// message for another consumer. The core surfaces the number rather than
	// making the choice.
	Abandoned uint64

	// LaneFinished is true on the final ack for a bounded lane. After this, the
	// core will not call Commit for the lane again and the source may release
	// anything it held for it.
	LaneFinished bool
}

type Outcome uint8

const (
	OutcomeDelivered Outcome = iota // everything landed at the sink
	OutcomeMixed                    // some records were abandoned; see Ack.Abandoned
)
```

### 4.4 Optional source interfaces

```go
// Discoverer lets a source enumerate what it can read, before a pipeline runs.
// This is what populates a stream picker with zero frontend code and makes drift
// a diff against a stored catalog.
//
// A source with no catalog (webhook, socket, metrics scrape) simply does not
// implement it, and the UI shows "streams known only at runtime" rather than an
// empty table. That answers the minimum-viable-Discover question: the minimum is
// not implementing it.
type Discoverer interface {
	Discover(ctx context.Context) (Catalog, error)
}

// Catalog is a point-in-time description of what a source offers.
type Catalog struct {
	Streams   []StreamDesc
	Truncated bool // the source stopped early; there are more streams
	At        time.Time
}

type StreamDesc struct {
	Name record.StreamName
	// Schema is optional: nil means "not knowable without reading".
	Schema *Schema
	// Keys are candidate identity fields, in preference order.
	Keys [][]string
	// Supports declares which lane kinds this stream can produce, so the UI can
	// grey out "initial scan" for a stream that cannot be scanned.
	Supports []LaneKind
	// EstimatedRecords, EstimatedBytes: zero means unknown.
	EstimatedRecords uint64
	EstimatedBytes   uint64
	Label            string
}

// Nackable lets a source observe permanent failures. Most sources do not want
// this — the core owns retry and dead-lettering — but a source that must, for
// example, park a message or notify upstream, implements it.
type Nackable interface {
	Nack(ctx context.Context, lane record.LaneID, fs []fault.Fault) error
}

// BacklogReporter answers "how much is left". Optional because for many sources
// it is unanswerable or expensive. May return fault.ErrSkip for "not right now".
type BacklogReporter interface {
	Backlog(ctx context.Context, lane record.LaneID) (Backlog, error)
}

type Backlog struct {
	Records uint64
	Bytes   uint64
	// Exact distinguishes a count from an estimate. Reported as its own gauge,
	// never as a label, because a label would split the series in two whenever
	// the source changed strategy and break every graph.
	Exact bool
	// EventTimeLag is how far behind the newest available record this lane is.
	// Nil means the source has no event time.
	EventTimeLag *time.Duration
}

// Heartbeater lets a source be told "nothing has arrived for a while", so it can
// keep an upstream from pinning its own retention. Postgres logical replication
// needs exactly this: with no messages to acknowledge, the WAL is never
// reclaimed and the disk fills.
type Heartbeater interface {
	Heartbeat(ctx context.Context, lane record.LaneID, idle time.Duration) error
}
```

### 4.5 Sink

```go
// Sink is the required sink interface. Three methods, frozen.
//
// A sink has no acknowledgement callback and no progress awareness whatsoever.
// It signals durability by returning nil from Write. The core owns the mapping
// from "Write returned nil" to "the source's lane advanced". This asymmetry is
// the single most valuable property of the ack model: a new sink cannot get
// progress wrong, because it is never shown progress.
type Sink interface {
	// Open connects. Same context narrowing and same re-callability as
	// Source.Open.
	Open(ctx context.Context, rt *SinkRuntime) error

	// Write delivers one request. Returning nil MEANS DURABLE: the data will
	// survive the loss of this process and of the sink's own process. If the sink
	// cannot promise that, it must not return nil — design rule R4 is this
	// sentence.
	//
	// Returning a *fault.BatchError expresses partial application, keyed on
	// record.RecordID. Returning any other error fails the whole request.
	//
	// Multiple Write calls may run concurrently up to Caps.MaxConcurrency.
	Write(ctx context.Context, req *Request) error

	// Close flushes and releases. Must not block indefinitely.
	Close(ctx context.Context) error
}

// Request is one already-encoded, already-framed, already-compressed unit of
// work. The sink implements *transport only*.
//
// This is the property that makes "add a sink: three methods, register, done"
// literally true, and it is constraint #4 applied to codecs as well as to
// connectors: N codecs times M connectors, with no multiplication.
type Request struct {
	// Body is the encoded payload. Empty only if Count is zero.
	Body []byte

	// Partition is the key the runtime's partitioner produced, if any. Every
	// record in this request shares it. This is how a generic sink gets
	// per-table, per-tenant or per-day batching without writing batching code.
	Partition string

	// Records identifies what is in Body, in Body's order, without carrying the
	// payloads. It is what a sink uses to build a *fault.BatchError.
	Records []record.Ref

	Count int
	// UncompressedBytes is the size before the compression stage, for metrics.
	UncompressedBytes int

	ContentType     string // from the encoder
	ContentEncoding string // from the compressor, empty if none

	// Schema is the schema of the records in this request, when the pipeline has
	// one. All records in a request share a schema epoch: the runtime never mixes
	// epochs in one request, so a sink never has to reconcile two shapes.
	Schema *record.SchemaRef

	// Attempt is 1 on first delivery, incrementing on retry. A sink may use it to
	// switch to a safer, slower path on a late attempt.
	Attempt int
}

// Response is optional richness a sink may return instead of a bare nil, through
// the ResponseSink interface. Every field is (value, ok) so "this sink cannot
// tell you" is a typed answer rather than a zero that reads as success —
// database/sql's Result idiom, with the per-record channel it lacks.
type Response struct {
	Written    int
	WrittenOK  bool
	Duplicates int
	DuplicatesOK bool
	// DestinationToken lets a transactional sink hand back its own commit
	// identity for display. Opaque, rendered, never parsed.
	DestinationToken string
}
```

### 4.6 Optional sink interfaces

```go
// RecordSink is the escape hatch for SDK-shaped destinations that must be given
// structured records rather than bytes: a BigQuery streaming insert, a Mongo
// bulk write, a vendor client that only accepts its own types.
//
// Declaring Caps.StructuredInput without implementing RecordSink panics at
// registration. Implementing it without declaring it is invisible. The core
// type-asserts in exactly one place.
type RecordSink interface {
	// WriteRecords delivers records with no encoding applied. Returning
	// fault.ErrSkip falls back to the byte path for this batch only, so a sink
	// can fast-path what it can and decline the rest.
	WriteRecords(ctx context.Context, b *record.Batch) error
}

// ResponseSink returns richer per-request outcomes.
type ResponseSink interface {
	WriteResponse(ctx context.Context, req *Request) (Response, error)
}

// Partitioner groups records into requests by a key the sink chooses. Usually
// trivial (constant), sometimes a template over the record.
type Partitioner interface {
	Partition(r *record.Record) (string, error)
}

// SchemaApplier declares that this sink can act on schema changes. It reports
// which kinds it supports; the core refuses at build time to run a drift policy
// of "evolve" against a sink that does not support the change kinds a stream can
// produce. A sink declares what it supports and nothing more — it never has to
// answer "should I ALTER TABLE?", which is the unanswerable question a
// no-schema-in-core design lands on every sink.
type SchemaApplier interface {
	SupportedChanges() []SchemaChangeKind
	ApplySchemaChange(ctx context.Context, ch SchemaChange) error
}

// Preparer lets a sink create or verify its destination before any data flows.
type Preparer interface {
	Prepare(ctx context.Context, streams []StreamDesc) error
}
```

### 4.7 Shared optional interfaces

```go
// Validator is tier two of two-tier validation: it may do I/O and it returns
// per-field diagnostics, all of them, never a fail-fast throw.
//
// It is called at submit time and on demand from the UI's "Test connection"
// button. It is the same method for sources and sinks.
type Validator interface {
	Validate(ctx context.Context) config.Diagnostics
}

// Prober is a cheap liveness check callable before and during normal operation
// without initialising the component.
type Prober interface {
	Probe(ctx context.Context) ProbeResults
}

type ProbeResult struct {
	Label string
	Path  []string
	Err   error
}
type ProbeResults []ProbeResult

func ProbeOK(label string) ProbeResults
func ProbeFailed(label string, err error) ProbeResults

// ChoiceProvider backs config.Field.Choices: a named hook returning valid values
// for a field given the partial config typed so far. "List the tables in this
// database", "list the topics", "list the buckets".
//
// This is how a specialised connector UI is built with zero core knowledge: the
// core exposes GET /v1/connectors/{name}/choices/{hook} and forwards.
type ChoiceProvider interface {
	Choices(ctx context.Context, hook string, partial *config.Config) ([]config.EnumValue, error)
}
```

### 4.8 Buffer — the one place the ack chain may be cut

```go
// Buffer is a pluggable stage between the source side and the sink side.
//
// It is the ONLY component permitted to shorten the ack chain, and it may do so
// only by declaring Caps.Durable — at which point the *core*, not the buffer,
// settles the group on a successful Put. A buffer cannot lie its way into early
// acknowledgement, because it does not perform the settling.
//
// This is Vector's best structural idea (a disk buffer takes ownership of the
// finalizers, so the durability boundary is configurable per pipeline and ack
// semantics follow automatically) with the loophole closed.
type Buffer interface {
	Open(ctx context.Context, rt *BufferRuntime) error

	// Put offers a batch. Returns how many records were accepted and why the
	// rest were not. Bounded by construction: Put can always refuse, and a
	// buffer with no refusal path is not a buffer (design rule R6).
	Put(ctx context.Context, b *record.Batch) (Accepted, error)

	// Get fills dst from the buffer, blocking until something is available,
	// Drain has been called and the buffer is empty (returns fault.ErrEndOfInput),
	// or ctx is done.
	Get(ctx context.Context, dst *record.Batch) error

	// Drain declares that no more Puts will come. Idempotent. This is how
	// end-of-stream propagates through a stateful stage, which is exactly the
	// problem a bounded-lane handoff has.
	Drain(ctx context.Context) error

	// Depth reports current occupancy for metrics and for the read model.
	Depth() Depth

	Close(ctx context.Context) error
}

type Accepted struct {
	Records int
	// Refused is non-empty when the buffer took only part of the batch. The
	// engine applies the configured WhenFull policy to the remainder.
	Refused []record.RecordID
}

type Depth struct {
	Records, RecordCapacity int
	Bytes, ByteCapacity     int64
	OldestAge               time.Duration
}

// WhenFull is the configured policy when a buffer refuses. There is no
// "unbounded" member: unbounded growth is inexpressible.
type WhenFull uint8

const (
	WhenFullBlock WhenFull = iota // apply backpressure. Default.
	// WhenFullDropNewest discards the incoming records and counts them. Newest,
	// not oldest: dropping the oldest would discard data whose prefix the source
	// may already have been told is safe, breaking the cursor invariant.
	WhenFullDropNewest
	// WhenFullReject settles the group as Abandoned and lets the source decide
	// (it sees Ack.Abandoned > 0). The rejection path R6 demands.
	WhenFullReject
	// WhenFullOverflow spills to the next buffer in the chain: a small memory
	// buffer in front of a large disk one, so the common case never touches disk
	// and a sustained sink outage still does not block.
	WhenFullOverflow
)
```

### 4.9 Codecs — four independent stages

```go
// Encoder turns one record into bytes. Registered by name; a connector never
// names one.
type Encoder interface {
	// Encode appends to dst and returns the extended slice, so the engine reuses
	// one buffer per stage. Must not retain dst and must not panic.
	Encode(dst []byte, r *record.Record) ([]byte, error)
	ContentType() string
}

// Decoder turns one frame into zero or more records. One frame to many records
// is in the signature, which is what correctly handles a JSON array in one
// frame, a multi-record WAL message, or a multiline log entry.
type Decoder interface {
	Decode(frame []byte, dst *record.Batch) error
}

// Framer delimits an encoded payload.
type Framer interface {
	Frame(dst []byte, payload []byte) ([]byte, error)
	// Terminator returns bytes to append once per request, after the last frame.
	// Nil for most framers; needed for e.g. a JSON array closing bracket.
	Terminator() []byte
}

// Deframer splits a byte stream into frames. The signature is deliberately
// bufio.SplitFunc's, so every existing Go splitter is a canal deframer and the
// idiom is already familiar.
type Deframer interface {
	Split(data []byte, atEOF bool) (advance int, frame []byte, err error)
}

// Compressor and Decompressor are the third stage.
type Compressor interface {
	Compress(dst []byte, src []byte) ([]byte, error)
	ContentEncoding() string
}

type Decompressor interface {
	Decompress(dst []byte, src []byte) ([]byte, error)
}

// CodecChain is the resolved encode-side chain a pipeline runs. The core builds
// it from config; no connector sees it.
type CodecChain struct {
	Encoder      Encoder
	Framer       Framer
	Compressor   Compressor // nil means none
	Decoder      Decoder
	Deframer     Deframer
	Decompressor Decompressor
}
```

### 4.10 Transform

```go
// Transform is the only stage type between source and sink. The full return
// vocabulary lives in the out batch's length: N→0 filters, N→N maps, N→M
// expands or regroups. No extra vocabulary, no separate interfaces for the
// cases, no 1-to-1 restriction (which is what Kafka Connect's SMTs are stuck
// with).
//
// Records placed in out MUST be derived from records in in — via Copy or
// Derive — because settlement identity lives on the record and a freshly
// constructed record belongs to no group. The core enforces this: a record in
// out whose Group is not one of in's groups is a
// fault.PermanentContract at build-time conformance test and a hard error at
// runtime. Benthos states the same rule in a doc comment and cannot enforce it.
type Transform interface {
	Open(ctx context.Context, rt *TransformRuntime) error
	Apply(ctx context.Context, in *record.Batch, out *record.Batch) error
	Close(ctx context.Context) error
}
```

### 4.11 Capabilities — declared data plus optional interfaces

```go
// SourceCaps is the declarative half. It is data: it serialises, it crosses a
// process boundary, it is queryable by the registry and the UI without
// instantiating anything, and it is checkable at submit time.
//
// Every bool named After an interface is cross-checked at registration.
type SourceCaps struct {
	// DefaultOrdering applies to lanes that do not override it.
	DefaultOrdering Ordering
	// Boundedness declares what this source can produce.
	Boundedness []Boundedness
	// LaneKinds declares which lane kinds this source can announce. A source
	// that cannot do a full scan does not list LaneKindScan, and the UI greys out
	// the option rather than failing at 3am.
	LaneKinds []LaneKind

	// MaxLanes caps announced lanes. Zero means unlimited. HARD-ENFORCED at
	// announce time: exceeding it is a pipeline failure, not a warning. Kafka
	// Connect's tasks.max stayed advisory for eight years and needed KIP-1004 to
	// fix; canal refuses on the first violation.
	MaxLanes int

	// Interface-backed flags. Registration panics if a flag is set and the
	// interface is absent, or vice versa.
	Discoverable    bool // Discoverer
	Nackable        bool // Nackable
	ReportsBacklog  bool // BacklogReporter
	Heartbeats      bool // Heartbeater
	Validates       bool // Validator
	Probes          bool // Prober
	ProvidesChoices bool // ChoiceProvider

	// Pure declarations with no interface behind them.
	ProducesEventTime bool
	ProducesChange    bool
	ProducesSchema    bool
	// Replayable means the source can re-read from a committed position. False
	// means a lost in-flight window is lost data, and the engine refuses
	// AtLeastOnce for it.
	Replayable bool
	// StableKeys means Origin.Key is populated and stable. Required for
	// EffectivelyOnce and for the dedupe transform.
	StableKeys bool
}

// SinkCaps is the sink half.
type SinkCaps struct {
	// MaxConcurrency is how many Write calls may be in flight. Required, >= 1.
	MaxConcurrency int
	// MaxRequestRecords / MaxRequestBytes are hard limits the engine's batcher
	// and splitter respect. Zero means no limit.
	MaxRequestRecords int
	MaxRequestBytes   int

	// Idempotent means re-delivering an identical request is harmless. Required
	// for EffectivelyOnce.
	Idempotent bool
	// PartialFailure means Write may return a *fault.BatchError. When false the
	// engine never attempts sub-batch retry, so it does not need to guess.
	PartialFailure bool

	// RequiresCodec is false only for a sink that always takes structured input.
	RequiresCodec bool

	StructuredInput bool // RecordSink
	ReturnsResponse bool // ResponseSink
	Partitions      bool // Partitioner
	AppliesSchema   bool // SchemaApplier
	Prepares        bool // Preparer
	Validates       bool // Validator
	Probes          bool // Prober
	ProvidesChoices bool // ChoiceProvider

	AcceptsChange bool
}

// BufferCaps.
type BufferCaps struct {
	// Durable means: once Put returns, the records survive the loss of this
	// process, and this buffer will deliver them. Only a Durable buffer may
	// shorten the ack chain, and only the core performs the shortening.
	//
	// The conformance kit asserts this with a kill -9: a buffer that declares
	// Durable and loses a record fails its own registration test.
	Durable bool
	Bounded bool // must be true; the field exists so the assertion is explicit
	Chains  bool // supports WhenFullOverflow as a downstream
}
```

### 4.12 `Resolve` — the single type-assertion site

```go
// ResolvedSource is a source with every optional capability snapshotted into a
// plain, serialisable struct at one well-defined moment.
//
// This is database/sql's ColumnType idiom, and it is the answer to "an optional
// interface is hostile to a UI". A UI cannot type-assert, must not be a Go
// caller, and must contain no per-connector knowledge. So the core collapses the
// capability set into a struct, once, and everything downstream — the engine,
// the API, the frontend, the conformance kit — reads only the struct.
//
// Every "did the connector support this" question becomes a field plus a
// companion bool, so "unknown" is distinguishable from "zero", which is what
// makes an honest UI possible.
//
// Resolve is called in exactly one place in the entire codebase. Benthos pays
// nine hand-written forwarders per capability because it type-asserts at every
// wrapper; canal pays one function.
type ResolvedSource struct {
	Source Source
	Name   string
	Caps   SourceCaps

	Discoverer      Discoverer      // nil if absent
	Nackable        Nackable
	Backlog         BacklogReporter
	Heartbeater     Heartbeater
	Validator       Validator
	Prober          Prober
	ChoiceProvider  ChoiceProvider
}

func ResolveSource(name string, s Source, c SourceCaps) (*ResolvedSource, error)

type ResolvedSink struct {
	Sink   Sink
	Name   string
	Caps   SinkCaps

	RecordSink     RecordSink
	ResponseSink   ResponseSink
	Partitioner    Partitioner
	SchemaApplier  SchemaApplier
	Preparer       Preparer
	Validator      Validator
	Prober         Prober
	ChoiceProvider ChoiceProvider
}

func ResolveSink(name string, s Sink, c SinkCaps) (*ResolvedSink, error)
```

### 4.13 Runtimes — what the core injects

```go
// SourceRuntime is the source's handle onto the core. Everything a connector
// needs from canal arrives here, which is what keeps the connector package's
// import list to five entries.
type SourceRuntime struct{ /* ... */ }

// Context returns a context whose lifetime is the *component's*, not a single
// call's. This is where a source takes a context to store, and it exists so that
// no connector has to invent a shutdown.Signaller — which every real Benthos
// connector does, because its Connect context is call-scoped.
func (rt *SourceRuntime) Context() context.Context

func (rt *SourceRuntime) Lanes() LaneCtl
func (rt *SourceRuntime) State() StateHandle
func (rt *SourceRuntime) Log() *slog.Logger
func (rt *SourceRuntime) Metrics() Metrics
func (rt *SourceRuntime) Batcher() *Batcher

// Emit publishes a pipeline event: a schema change, a lane note, a drift
// observation. Events are ordered, bounded, and appear in the read model's
// recentEvents ring. Drift is an event, not a log line and not a metric.
func (rt *SourceRuntime) Emit(e Event)

// Pipeline and Instance identify this component for correlation. A connector may
// read them; it may not use them to change behaviour.
func (rt *SourceRuntime) Pipeline() record.PipelineID
func (rt *SourceRuntime) Instance() string

// Metrics is the connector's metric surface. The CORE owns metric naming,
// tagging, cardinality and export; a connector registers through this handle and
// can never name a metric.
//
// Names are namespaced under canal_connector_<name>_<metric> automatically and
// the label set is fixed by the core. A connector that wants an unbounded label
// gets an error, not a cardinality explosion.
type Metrics interface {
	Counter(name string, labels ...string) Counter
	Gauge(name string, labels ...string) Gauge
	Histogram(name string, buckets []float64, labels ...string) Histogram
}

type Counter interface{ Add(delta float64, labelValues ...string) }
type Gauge interface{ Set(v float64, labelValues ...string) }
type Histogram interface{ Observe(v float64, labelValues ...string) }

// SinkRuntime, TransformRuntime, BufferRuntime are the same shape minus what
// does not apply. A SinkRuntime has no Lanes() and no State(): a sink is
// structurally incapable of holding progress.
type SinkRuntime struct{ /* Context, Log, Metrics, Emit, Pipeline, Instance */ }
type TransformRuntime struct{ /* Context, Log, Metrics, Emit */ }
type BufferRuntime struct{ /* Context, Log, Metrics, Emit, DataDir */ }
```

### 4.14 Batching as a policy component, not a goroutine

```go
// Batcher is pure policy with no goroutine, so a source owns its own select loop
// and the batcher never fights it. This inversion is Benthos's cleanest small
// API and it is worth copying exactly.
type Batcher struct{ /* ... */ }

// Add returns true when the policy has triggered and Flush should be called.
func (b *Batcher) Add(r *record.Record) bool

// UntilNext reports how long until a timed flush is due. ok is false when the
// policy has no timed component, in which case the duration is meaningless.
func (b *Batcher) UntilNext() (time.Duration, bool)

func (b *Batcher) Flush(ctx context.Context, dst *record.Batch) error
func (b *Batcher) Close(ctx context.Context) error

// Splitter is the inverse, which Benthos lacks and documents as a limitation
// ("a batch policy has the capability to create batches, but not to break them
// down"). Sinks almost always have a hard maximum request size, so the core owns
// both directions.
type Splitter struct{ /* ... */ }

func NewSplitter(maxRecords int, maxBytes int) *Splitter
func (s *Splitter) Split(in *record.Batch) []*record.Batch

// BatchPolicy is the declarative form, extracted from config.Fields.Batching.
type BatchPolicy struct {
	MaxRecords int
	MaxBytes   int
	MaxAge     time.Duration
	// FlushOn is a declarative predicate over the record that forces a flush,
	// e.g. "close the batch at a transaction boundary". Closed operator set, same
	// as config.Predicate.
	FlushOn *config.Predicate
}
```

---

## 5. `ledger` — the core-owned ack graph, and the answer to the hard question

Depends on `record` and `fault`. This is the package that makes the angle work.

### 5.1 The generic contiguous-prefix tracker

```go
// Package ledger owns canal's acknowledgement graph.
//
// Benthos and Vector both leave this to connectors, and both pay the same two
// prices: the framework cannot report progress, and every source reimplements a
// non-trivial algorithm out-of-tree. Benthos's reusable version
// (github.com/Jeffail/checkpoint) lives outside both its repositories, so each
// connector re-wires it with its own key, its own serialisation and its own bugs.
//
// Here it is core, generic over the payload, and no connector ever sees it.
package ledger

// Tracker receives an ordered feed of tracked payloads and an unordered feed of
// resolutions, and reports the highest payload that may safely be committed —
// that is, the last payload in the contiguous resolved prefix.
//
// This is one of the two places generics genuinely help in canal: Tracker is
// used with record.Position in production and with plain ints in its own tests,
// there is exactly one type parameter, and nothing erases it at a registry
// boundary.
//
// Safe for concurrent use.
type Tracker[P any] struct{ /* doubly-linked list of pending nodes */ }

// NewTracker creates a tracker whose pending weight is capped at budget. Track
// blocks when admitting more would exceed it, and that blocking IS canal's
// backpressure at the source edge — one mechanism, not Benthos's five.
func NewTracker[P any](budget uint64) *Tracker[P]

// Track admits a payload with a logical weight (a record count). It blocks while
// pending+weight > budget, returning ctx.Err() if the wait is cancelled.
//
// The returned Resolve reports the new committable payload when this resolution
// advanced the contiguous prefix, and ok=false when an earlier payload is still
// outstanding — meaning "resolved, but commit nothing".
func (t *Tracker[P]) Track(ctx context.Context, payload P, weight uint64) (Resolve[P], error)

// Resolve is idempotent: calling it twice resolves once.
type Resolve[P any] func() (P, bool)

// Committed returns the current committable payload.
func (t *Tracker[P]) Committed() (P, bool)

// Pending reports outstanding weight, outstanding node count and the age of the
// oldest outstanding node. These three numbers are the whole diagnostic story
// for "why is progress not advancing", and no ack-based system currently exposes
// them.
func (t *Tracker[P]) Pending() (weight uint64, nodes int, oldest time.Duration)

// Budget returns the configured cap. The engine exports it as
// canal_lane_replay_window_records, because the budget is *exactly* the
// worst-case number of records re-read after a crash. That number is the single
// most-asked operational question about an at-least-once pipeline, and this is
// the first design in the prior art that can answer it.
func (t *Tracker[P]) Budget() uint64

// Abandon resolves a node as terminally not-delivered. It advances the prefix
// exactly as a resolution does, but records the abandonment so the Ack carries a
// non-zero Abandoned count.
//
// This is what makes a poison record unable to livelock the pipeline: the
// terminal disposition abandons, the prefix moves, the source unblocks. Benthos
// has no such move, which is why its default unbounded retry deadlocks on a
// poison record.
func (t *Tracker[P]) Abandon(r Resolve[P])
```

### 5.2 The Ledger

```go
// Ledger is the per-pipeline settlement graph. One Ledger holds one Tracker per
// prefix-ordered lane and one pending set per discrete-ordered lane.
type Ledger struct{ /* ... */ }

type Config struct {
	Pipeline      record.PipelineID
	DefaultBudget int
	// GroupTTL bounds how long a group may stay unsettled before the ledger
	// declares it lost. A leak is then NAMED rather than silently resolving as
	// not-delivered.
	//
	// Vector gets safety from Rust's Drop (dropping an unsettled finalizer
	// resolves the batch as not-delivered, which is the safe direction). Go has
	// no Drop, so canal uses a reaper — and the reaper is strictly better,
	// because it turns "someone forgot to settle" from a silent stall into a
	// LeakDetected condition with the offending stage, lane and group named.
	// Benthos had to bolt a nack timeout on for the same reason and reports only
	// a log line.
	GroupTTL time.Duration
}

func New(cfg Config) *Ledger

// Lane registers a lane with the ledger. Called by the engine when a source
// announces one.
func (l *Ledger) Lane(id record.LaneID, ord connector.Ordering, budget int) error

// Admit takes a source batch, assigns Position.Seq, assigns a GroupID, assigns a
// RecordID and Origin to every record, and returns once admission is within
// budget.
//
// BLOCKING HERE IS THE ENTIRE SOURCE-SIDE BACKPRESSURE MECHANISM. There is no
// separate credit protocol, no separate in-flight semaphore, no separate
// checkpoint-limit knob. One concept.
func (l *Ledger) Admit(ctx context.Context, b *record.Batch) error

// Fanout increases a group's outstanding reference count by n-1, for a 1→N
// expansion or a 2-way sink fan-out. Called by the engine, never by a connector.
//
// Fan-out correctness falls out of refcounting: an event routed to three sinks
// resolves only when all three finish. No core code path per topology.
func (l *Ledger) Fanout(g record.GroupID, n int) error

// Settle records one record's terminal disposition. Worst outcome wins within a
// group, pessimistically: Delivered + Failed = Failed. One bad record cannot be
// masked by its successful siblings.
func (l *Ledger) Settle(id record.RecordID, d Disposition, f *fault.Fault)

type Disposition uint8

const (
	Delivered   Disposition = iota // landed durably at the sink (or in a Durable buffer)
	Duplicated                     // sink reported it already had this; counts as delivered
	Abandoned                      // reached a terminal disposition: DLQ or drop
	Failed                         // not delivered and not terminal; the group stays pending
)

// Acks is the stream the engine pumps into Source.Commit. A channel is fine
// here: it is core-internal and never crosses the plugin boundary.
func (l *Ledger) Acks() <-chan connector.Ack

// Watermark returns the committed position for a lane. This is the read model's
// position field and the source of canal_checkpoint_age_seconds.
func (l *Ledger) Watermark(lane record.LaneID) (record.Position, bool)

// Stats is the ledger's contribution to the read model. Every field is derived
// from core-owned state, so it is available for any connector, including one that
// has never heard of canal's metrics.
func (l *Ledger) Stats(lane record.LaneID) LaneStats

type LaneStats struct {
	Committed        record.Position
	CommittedOK      bool
	Admitted         uint64 // records
	Settled          uint64
	AbandonedTotal   uint64
	InFlight         uint64
	PendingGroups    int
	OldestPendingAge time.Duration
	ReplayWindow     uint64 // == budget: worst-case re-read on crash
	Blocked          bool   // Admit is currently waiting on budget
	BlockedFor       time.Duration
}

// Drain settles nothing but waits for everything outstanding to settle, up to
// ctx. Called on graceful shutdown. Acks continue to be delivered to the source
// during a drain — a graceful stop must not throw away a commit that is one
// millisecond from being safe.
func (l *Ledger) Drain(ctx context.Context) error

// Leaks returns groups that exceeded GroupTTL, with the stage that last held
// them. Non-empty means a bug, and the engine raises it as a condition.
func (l *Ledger) Leaks() []Leak

type Leak struct {
	Group   record.GroupID
	Lane    record.LaneID
	Age     time.Duration
	Records int
	Stage   string
}
```

### 5.3 The hard question, answered directly

> *How do you get restart-safety and gap-free resume for a source whose progress is a single monotonic
> cursor, when acks can complete out of order?*

Six mechanisms, all core-owned, none visible to a connector.

**(1) The core assigns sequence, not the connector.** `Ledger.Admit` stamps `Position.Seq`
monotonically per lane at hand-over. The source never sees it and cannot get it wrong. This is the
stable ordering identity that Benthos lacks and that its own author deprecated positional identity for.

**(2) Out-of-order resolution, in-order commit.** `Tracker` resolves in any order and reports only the
contiguous prefix. If deliveries 1..7 are handed out and 2, 5, 6, 7 settle, the committed position is
1's — nothing after it is safe. When 2, 3, 4 settle, the committed position jumps to 7's in one step.
The cursor is therefore always monotonic and always behind or equal to the true safe point. **A gap is
structurally unrepresentable:** to commit position *N* the tracker must have observed every position
below *N* resolve.

**(3) `Position.Safe` handles the sub-batch hazard.** A contiguous prefix is not automatically a legal
resume point: Benthos's MySQL connector only advances to transaction boundaries, because resuming
mid-transaction loses the `TABLE_MAP` events that precede row events. Rather than leaving that to every
connector, `Safe` is a field, and the tracker's commit rule is *the last `Safe` position at or before
the resolved prefix*. A connector that has no such distinction sets `Safe: true` everywhere and pays
nothing.

**(4) The replay window is the budget, and the budget is visible.** Pending weight is capped. On a crash
the source re-reads from the last committed safe position, so the duplicate window is bounded above by
`budget` records — a number the operator configured and that canal exports as
`canal_lane_replay_window_records`. "How much will I re-read after a kill -9?" has an exact answer,
which is not true of any prior-art ack pipeline.

**(5) Head-of-line blocking cannot become a livelock.** If delivery 5 fails permanently, the prefix
stalls at 4 and pending climbs to the budget, at which point `Admit` blocks and the source stops
reading. That is Benthos's documented deadlock. Here `fault.RetryPolicy.Terminal` has no valid zero
value, so every pipeline has a terminal disposition; on exhaustion the engine calls
`Tracker.Abandon`, the prefix advances past 5, and the source resumes with `Ack.Abandoned == 1`.
**Unbounded retry is not a default that was chosen badly — it is inexpressible.**

**(6) Sources with no cursor use the other resolver.** A lane declared `OrderingDiscrete` gets no
tracker at all: each group resolves individually and `Ack.Discrete` carries exactly the settled
positions. SQS, AMQP and Pub/Sub take this path; MySQL binlog and a file tail take the prefix path. One
declared enum, two core-owned strategies, zero connector algorithms.

The residual honest cost: at-least-once, never exactly-once, and the duplicate window is real. This
design declines to pretend otherwise — see §11.

---

## 6. `telemetry` — the read model

Depends on `record` and `ledger`.

```go
// Package telemetry owns metric naming, the closed label vocabulary, and the
// single read-model document.
//
// The core owns naming and export; a connector registers through
// connector.Metrics and can never name a metric or invent a label.
package telemetry

// Phase is the pipeline's coarse state. Kubernetes-shaped: one phase plus
// orthogonal conditions, because one enum is never enough and a product of enums
// is a combinatorial nightmare.
type Phase uint8

const (
	PhasePending Phase = iota
	PhaseStarting
	PhaseRunning
	PhaseDraining
	PhaseStopped
	PhaseFailed
)

// ConditionType is the closed set. Six types times three statuses is 18 bounded
// series per pipeline, which is why conditions can be metrics as well as
// read-model fields — so "my config change silently did not apply" becomes an
// alert instead of a mystery.
type ConditionType uint8

const (
	CondSourceConnected ConditionType = iota
	CondSinkConnected
	CondProgressing   // the committed watermark is advancing
	CondBackpressured // a stage is blocking
	CondDegraded      // sustained retries, or a Durable buffer unavailable
	CondSpecApplied   // observedGeneration == generation
)

type Status uint8

const (
	StatusUnknown Status = iota
	StatusTrue
	StatusFalse
)

type Condition struct {
	Type               ConditionType
	Status             Status
	Reason             string // closed vocabulary, i18n key
	Message            string
	LastTransitionTime time.Time
}

// PipelineStatus is THE read model. One canonical struct, one source of truth,
// three serialisations (HTTP snapshot, SSE stream, CLI).
//
// Rule that makes it a contract rather than a struct: every unknown is a nil
// pointer, never a zero. The frontend has one shared "unknown" renderer and a
// pinned fixture in which every optional field is absent asserts that the UI
// renders no zeros. A metrics UI that cannot distinguish "the endpoint answered"
// from "your data arrived" is actively misleading.
type PipelineStatus struct {
	Pipeline           record.PipelineID `json:"pipeline"`
	Generation         uint64            `json:"generation"`
	ObservedGeneration uint64            `json:"observedGeneration"`
	AsOf               time.Time         `json:"asOf"`
	Version            uint64            `json:"version"` // monotonic: SSE cursor and ETag
	// Complete is false when the aggregator did not hear from every worker. A
	// status document that silently omits a worker is the same lie as a health
	// check that returns 200 for a broken pipeline.
	Complete bool `json:"complete"`

	Phase      Phase       `json:"phase"`
	Conditions []Condition `json:"conditions"`

	Guarantee string `json:"guarantee"` // negotiated, not requested

	Throughput  Throughput      `json:"throughput"`
	Lanes       []LaneStatus    `json:"lanes"`
	Buffers     []BufferStatus  `json:"buffers"`
	Workers     []WorkerStatus  `json:"workers"`
	Scan        *ScanProgress   `json:"scan"`    // nil when no scan lane exists
	RecentEvents []Event        `json:"recentEvents"`
	LastFault   *FaultInfo      `json:"lastFault"`
	Config      map[string]any  `json:"config"` // redacted
}

// LaneStatus is the per-lane view, and it is the whole reason this design's
// observability is not a compromise: every field is derived from core-owned
// ledger state plus two connector-authored display strings.
type LaneStatus struct {
	ID     record.LaneID  `json:"id"`
	Name   string         `json:"name"`
	Stream string         `json:"stream"`
	Kind   string         `json:"kind"`     // reporting only
	Label  string         `json:"label"`    // LaneSpec.Label, rendered verbatim
	Worker string         `json:"worker"`

	// Position is Position.Label, rendered verbatim. The core never parsed it.
	Position     *string    `json:"position"`
	PositionSeq  uint64     `json:"positionSeq"`
	CommittedAt  *time.Time `json:"committedAt"`
	CheckpointAge *float64  `json:"checkpointAgeSeconds"` // primary alert signal

	RecordsRead      uint64 `json:"recordsRead"`
	RecordsCommitted uint64 `json:"recordsCommitted"`
	RecordsAbandoned uint64 `json:"recordsAbandoned"`
	InFlight         uint64 `json:"inFlight"`
	ReplayWindow     uint64 `json:"replayWindowRecords"`

	Blocked      bool     `json:"blocked"`
	BlockedFor   *float64 `json:"blockedForSeconds"`
	OldestPendingAge *float64 `json:"oldestPendingAgeSeconds"`

	// Backlog is nil when the source cannot report it. Nil renders as "unknown",
	// not as zero.
	Backlog      *Backlog `json:"backlog"`
	EventTimeLag *float64 `json:"eventTimeLagSeconds"`
	Finished     bool     `json:"finished"`
}

type ScanProgress struct {
	LanesTotal    int      `json:"lanesTotal"`
	LanesFinished int      `json:"lanesFinished"`
	// Fraction is nil unless enough lanes declared a Weight.
	Fraction  *float64 `json:"fraction"`
	StartedAt time.Time `json:"startedAt"`
	ETA       *time.Time `json:"eta"`
}

// Names is the closed metric-name set, pinned by a golden-file test against real
// /metrics output. The label vocabulary is closed too: pipeline, lane, stage,
// connector, class, op, buffer, worker, outcome, reason, phase, type, status.
// Nothing per-record-key, no error message, no unbounded stream label at
// pipeline granularity.
const (
	MRecordsRead      = "canal_records_read_total"       // {pipeline,lane,connector}
	MRecordsWritten   = "canal_records_written_total"    // {pipeline,connector}
	MRecordsCommitted = "canal_records_committed_total"  // {pipeline,lane}
	MRecordsAbandoned = "canal_records_abandoned_total"  // {pipeline,lane,reason}
	MRecordsFailed    = "canal_records_failed_total"     // {pipeline,stage,class}
	MCheckpointAge    = "canal_checkpoint_age_seconds"   // {pipeline,lane}
	MReplayWindow     = "canal_lane_replay_window_records"
	MInFlight         = "canal_inflight_records"         // {pipeline,lane}
	MOldestPending    = "canal_oldest_pending_age_seconds"
	MStageBlocked     = "canal_stage_blocked_seconds_total"
	MStageUtilization = "canal_stage_utilization_ratio"  // the bottleneck finder
	MBufferDepth      = "canal_buffer_depth"
	MBufferRefused    = "canal_buffer_refused_total"
	MLedgerLeaks      = "canal_ledger_leaks_total"       // must stay zero
	MUnclassified     = "canal_unclassified_faults_total" // must stay zero
	// ... the full set lives in docs/metrics.md and is normative.
)
```

---

## 7. `registry` — value-typed, init-registered

Depends on `connector` and `config`.

```go
// Package registry holds component definitions keyed by kind and name.
//
// The global registry is a *default instance of a value type* with Clone/With/
// Without, so a test or a sandbox gets an isolated registry instead of mutating
// process-global state. Benthos reached this shape (Environment) after starting
// with a global; canal starts there.
package registry

type Kind uint8

const (
	KindSource Kind = iota
	KindSink
	KindTransform
	KindBuffer
	KindEncoder
	KindDecoder
	KindFramer
	KindDeframer
	KindCompressor
)

// SourceDef is what a connector package registers. Note that New receives
// pre-parsed, pre-validated, pre-defaulted config — there is no Configure
// callback and no map re-parsed inside the connector — and that New must NOT do
// I/O. I/O belongs in Open, which the engine retries.
type SourceDef struct {
	Name string
	Spec *config.Spec
	Caps connector.SourceCaps
	New  func(ctx context.Context, cfg *config.Config) (connector.Source, error)
}

type SinkDef struct {
	Name string
	Spec *config.Spec
	Caps connector.SinkCaps
	New  func(ctx context.Context, cfg *config.Config) (connector.Sink, error)
}

// ... TransformDef, BufferDef, EncoderDef, FramerDef, CompressorDef, all the
// same three-field shape.

type Registry struct{ /* ... */ }

func New() *Registry

// Default is the process registry. Connector packages call
// registry.Default.AddSource(...) from an init function.
var Default = New()

// AddSource registers a source and CROSS-CHECKS Caps against the interfaces the
// returned value satisfies, by constructing a zero-config probe instance where
// possible and otherwise deferring to first construction.
//
// Declaring Caps.Discoverable without implementing Discoverer PANICS at
// registration. Implementing Discoverer without declaring it panics too: a
// silent capability is a capability the UI cannot see, and Benthos left one of
// its optional interfaces unexported, so nobody could implement it.
func (r *Registry) AddSource(d SourceDef) error
func (r *Registry) AddSink(d SinkDef) error
// ... one per kind

func (r *Registry) Source(name string) (SourceDef, bool)
func (r *Registry) Sink(name string) (SinkDef, bool)

// Clone returns an independent copy. With and Without derive a modified registry
// without touching the original — for tests, for a sandboxed tenant, for a
// restricted deployment that must not offer a shell-exec sink.
func (r *Registry) Clone() *Registry
func (r *Registry) With(defs ...any) (*Registry, error)
func (r *Registry) Without(k Kind, names ...string) *Registry

// Walk enumerates everything registered, which is the entire input to
// GET /v1/connectors. The API has no per-connector code because there is nothing
// per-connector to have code about.
func (r *Registry) Walk(fn func(k Kind, name string, spec *config.Spec, caps any) bool)
```

---

## 8. `engine` — assembly and the stage loops

Depends on everything above.

```go
// Package engine assembles and runs pipelines.
//
// The outer shape is small and is an IMPLEMENTATION FACT, never an enumerated
// stage list in any API: source -> decode -> transform* -> buffer? -> batch ->
// encode -> sink. Design rule R1 forbids a fixed stage count in the contract,
// and the abandoned first attempt froze eight stages into its OpenAPI schema
// with minItems: 8, maxItems: 8.
//
// All topological variety comes from components containing components: a
// config.TypeComponent field is how fan-out (a broker sink whose config holds
// N sinks), fan-in (a broker source), fallback, retry-wrapping and DLQ routing
// exist with zero core special-casing. That is the same mechanism as a
// component-valued config field, so it costs nothing extra.
package engine

// Guarantee is what a pipeline promises. Deliberately three values, and
// deliberately not including "exactly once": this design does not implement
// two-phase commit and refuses to name something exactly-once that is not.
type Guarantee uint8

const (
	// AtMostOnce: settle on hand-over. Fast, lossy on crash, explicitly chosen.
	AtMostOnce Guarantee = iota
	// AtLeastOnce: settle on sink durability. Duplicates bounded by the lane
	// budget on crash.
	AtLeastOnce
	// EffectivelyOnce: AtLeastOnce plus an idempotent sink plus stable record
	// keys, so the duplicates are absorbed at the destination. Requires
	// SinkCaps.Idempotent and SourceCaps.StableKeys.
	EffectivelyOnce
)

// Spec is a pipeline's declarative definition. This is what the ConfigStore
// holds and what the API accepts.
type Spec struct {
	ID       record.PipelineID
	Revision uint64

	Source     ComponentRef
	Decode     *CodecRef // nil when the source produces structured records
	Transforms []ComponentRef
	Buffer     *ComponentRef // nil means direct hand-off
	Encode     *CodecRef     // required unless the sink declares !RequiresCodec
	Sink       ComponentRef
	DLQ        *ComponentRef

	Batching   connector.BatchPolicy
	Retry      fault.RetryPolicy
	WhenFull   connector.WhenFull
	LaneBudget int
	Guarantee  Guarantee // requested; Build returns the negotiated value
	Drift      DriftPolicy
	Parallelism int // max concurrently-read lanes on one worker
}

type ComponentRef struct {
	Kind  registry.Kind
	Name  string
	Label string         // instance label; namespaces this component's metrics
	Config map[string]any
}

type CodecRef struct {
	Encoder     *ComponentRef
	Framer      *ComponentRef
	Compressor  *ComponentRef
}

// DriftPolicy is the five-mode set, adopted wholesale from Flink CDC because it
// is the only complete shipped answer, with never-destructive Lenient as the
// default. It is core config, not a per-sink decision, because "should I ALTER
// TABLE?" is an unanswerable question to land on every sink author.
type DriftPolicy uint8

const (
	DriftLenient DriftPolicy = iota // apply additive changes; never destructive. Default.
	DriftEvolve                     // apply everything the sink supports
	DriftTryEvolve                  // apply what is supported; ignore the rest, with an event
	DriftIgnore                     // never apply; keep writing the old shape
	DriftFail                       // stop the pipeline on any change
)

// Build assembles a pipeline. This is where capability negotiation happens, and
// it happens as a PURE FUNCTION OF CONFIG, before anything starts.
//
// The negotiated guarantee is min(source, sink, buffer, requested). An impossible
// pipeline is refused here, at submit time, with per-field diagnostics — not at
// 3am with a quieter delivery guarantee. This is the fix for Vector's most
// dangerous silent degradation (its acknowledgement negotiation degrades without
// telling anyone) and it turns design rule R4 from prose into a type.
//
// Concretely, Build refuses:
//   - EffectivelyOnce with a sink that is not Idempotent, or a source without
//     StableKeys
//   - AtLeastOnce with a source that is not Replayable
//   - a scan lane kind the source does not declare
//   - DriftEvolve against a sink whose SupportedChanges does not cover the
//     stream's possible changes
//   - Retry.Terminal == TerminalInvalid
//   - TerminalDLQ with no DLQ configured
//   - a Durable buffer requirement against a buffer that does not declare it
//   - Parallelism > SourceCaps.MaxLanes
func Build(ctx context.Context, r *registry.Registry, s Spec, d Deps) (*Pipeline, Negotiated, config.Diagnostics)

// Negotiated is the resolved, honest answer, surfaced in the read model so an
// operator sees what they got rather than what they asked for.
type Negotiated struct {
	Guarantee      Guarantee
	Reasons        []string // why it is not what was requested
	DurabilityEdge string   // "sink" or "buffer:<label>": where an ack is earned
	ReplayWindow   int
	SettleAt       string
}

// Deps is the deployment seam, injected. Four interfaces, no more (§9).
type Deps struct {
	Config      store.ConfigStore
	State       store.StateStore
	Coordinator store.Coordinator
	Status      store.StatusSink
	Metrics     telemetry.Factory
	Logger      *slog.Logger
	DataDir     string
}

type Pipeline struct{ /* ... */ }

// Run blocks until the pipeline terminates. A bounded pipeline returns nil when
// every lane finishes and every group settles. Cancelling ctx begins a graceful
// drain; a second cancellation is a hard stop.
func (p *Pipeline) Run(ctx context.Context) error

// Status returns the current read model. Cheap: assembled from ledger stats and
// counters, no I/O.
func (p *Pipeline) Status() telemetry.PipelineStatus

// Shutdown ordering is three-phase and stated here because it is where every
// prior-art system has a scar:
//   1. Stop reading. Source.Read is no longer called. Announced lanes stay open.
//   2. Drain. Everything in flight is written and settled; Acks continue to be
//      delivered to Source.Commit throughout, because a graceful stop must not
//      throw away a commit that is one millisecond from safe.
//   3. Close, in reverse order: sink, buffer, transforms, source. Ledger leaks
//      are reported. If the drain does not complete within the grace period, the
//      unsettled groups are named in the final log line and in the last status
//      document — never silently discarded.
func (p *Pipeline) Shutdown(ctx context.Context) error
```

### 8.1 Built-in components that keep the core small

```go
// These are ordinary registered components, in their own packages, with no
// privileges. Each one is something other frameworks put in the core.

// broker sink: fan-out to N child sinks. Config holds a list of TypeComponent.
// The engine calls ledger.Fanout(group, n) and each child settles independently;
// the group resolves when all have. Fan-out correctness is refcounting.

// fallback sink: try child 1, on a retryable class try child 2.

// dlq sink wrapper: any sink may be a DLQ. A DLQ that works for sources too,
// which Kafka Connect's does not (its per-record reporter is sink-only).

// scan_shadow transform: drops a record with Change.Op == OpScanRead whose
// Origin.Key has already been seen from a stream lane in this run. This is Flink
// CDC's dedup filter demoted from a core stage to an optional component: it
// self-retires when the scan lanes close, its index is a hash set from day one
// (Flink CDC's original O(chunks) per-record filter needed a binary-search
// retrofit), and a pipeline that does not need it does not pay for it.

// dedupe transform: keyed on (pipeline, source, stream, Origin.Key) — never on a
// bare record id, which is design rule R5's first bug. Its state store is
// injected, not module-global, and the key is marked seen AFTER the downstream
// settle, never before, which is R5's second bug.

// memory buffer: BufferCaps{Durable: false}. Ack chain passes through.
// wal buffer:    BufferCaps{Durable: true}.  Segmented log, per-record length
//                prefix and CRC32C, separate ledger file, and a CanDecode(version)
//                predicate checked BEFORE decoding so an upgrade fails loudly
//                rather than misparsing.
```

---

## 9. `store` — the deployment seam

Four interfaces. If a fifth appears, the abstraction is wrong.

```go
// Package store is the only thing that differs between a laptop and a cluster.
package store

// ConfigStore holds pipeline specs. Revisioned CAS plus a watch.
type ConfigStore interface {
	Get(ctx context.Context, id record.PipelineID) (engine.Spec, uint64, error)
	List(ctx context.Context) ([]record.PipelineID, error)
	Put(ctx context.Context, s engine.Spec, ifRevision uint64) (uint64, error)
	Delete(ctx context.Context, id record.PipelineID, ifRevision uint64) error
	Watch(ctx context.Context, fromRevision uint64) (<-chan ConfigEvent, error)
}

// StateStore is the durability substrate. BYTES IN, BYTES OUT: it never sees a
// domain type. This is precisely what makes the standalone/distributed swap free,
// and it is Kafka Connect's single best idea.
//
// It is also, in this design, OPTIONAL FROM THE SOURCE'S POINT OF VIEW. It backs
// connector.StateHandle for sources that choose to use it, and nothing else. A
// pipeline whose source keeps its progress upstream (Kafka, SQS, a webhook) never
// touches it.
type StateStore interface {
	Get(ctx context.Context, keys [][]byte) (map[string]Versioned, error)
	// Set must be atomic across the whole map, with per-key CAS. One SQL
	// transaction, one bbolt transaction, one etcd Txn. Kafka Connect's compacted
	// topic cannot meet this and its own javadoc documents the resulting
	// unrecoverable state with "no obvious way to resolve the issue".
	Set(ctx context.Context, kv map[string]Versioned) error
	Delete(ctx context.Context, keys [][]byte) error
	Keys(ctx context.Context, prefix []byte) ([][]byte, error)
}

type Versioned struct {
	Value     []byte
	IfVersion uint64
	Version   uint64
}

// Coordinator provides membership, leases and leader election.
//
// LEADERSHIP IS FOR PLANNING ONLY AND IS NEVER TRUSTED FOR CORRECTNESS. The
// k8s.io/client-go leader-election package documents that its implementation
// "does not guarantee that only one client is acting as a leader", so anything
// whose correctness depends on single-leadership is broken by construction. The
// LEASE is the fencing token, and StateStore's per-key CAS is the second fence.
type Coordinator interface {
	Join(ctx context.Context, w WorkerInfo) (Membership, error)
	Campaign(ctx context.Context) (Leadership, error)

	// Plan writes the assignment rows for a pipeline's announced lanes. Called
	// only by the leader. The plan is DURABLE STATE, not a leader's in-memory
	// result, so a leader crash loses nothing.
	Plan(ctx context.Context, id record.PipelineID, lanes []connector.LaneSpec, gen uint64) error

	// Claim takes a lease on an assignment. Claim/Renew/Release are the entire
	// placement protocol; there is no stop-the-world rebalance.
	Claim(ctx context.Context, a AssignmentID, w WorkerID, ttl time.Duration) (Lease, error)
	Renew(ctx context.Context, l Lease) (Lease, error)
	Release(ctx context.Context, l Lease) error
	Assignments(ctx context.Context, id record.PipelineID) ([]Assignment, error)
}

// StatusSink collects per-worker status into one document.
type StatusSink interface {
	Report(ctx context.Context, w WorkerID, s telemetry.PipelineStatus) error
	Aggregate(ctx context.Context, id record.PipelineID) (telemetry.PipelineStatus, error)
}
```

| Interface | `canal run` (laptop) | `canal serve` (cluster) |
|---|---|---|
| `ConfigStore` | bbolt file, or `-f pipelines.yaml` projected in | Postgres, then etcd / k8s CRD as conformance targets |
| `StateStore` | bbolt file (one `Set` = one transaction, so atomic) | Postgres table |
| `Coordinator` | `singlenode{}`: always leader, all lanes local, leases no-ops | Postgres advisory lock + a leases table + `FOR UPDATE SKIP LOCKED` |
| `StatusSink` | in-process | `worker_status` rows with a TTL |

Postgres first, not etcd or Kafka: one dependency delivers revisioned CAS, atomic multi-key writes,
advisory-lock leader election, a leases table, `SKIP LOCKED` work claiming, `LISTEN`/`NOTIFY` for watch
and a status table. etcd stays a second implementation and a conformance target that stops the interface
acquiring Postgres-isms.

---

## 10. Package layout and dependency direction

```
canal/
├── record/                 envelope, Value, Position, Origin, Batch, Change.  imports: stdlib only
├── fault/                  Class, Op, Fault, BatchError, RetryPolicy.        imports: record
├── config/                 Spec, Field, Predicate, Config, Diagnostics, JSON Schema, composites.
│                                                                             imports: fault
├── connector/              Source, Sink, Buffer, Transform, codecs, Caps, LaneCtl, StateHandle,
│   │                       runtimes, Resolve, Batcher, Splitter, AutoPersist.
│   │                                                                         imports: record, fault, config
│   └── conformance/        the connector test kit: run it against any connector.
│                                                                             imports: connector, record, fault
├── ledger/                 Tracker[P], Ledger, Disposition, LaneStats, reaper.
│                                                                             imports: record, fault, connector
├── telemetry/              metric names, closed label set, Metrics impl, PipelineStatus read model.
│                                                                             imports: record, fault, ledger
├── registry/               Registry value type, Default, *Def structs, Walk, Clone/With/Without.
│                                                                             imports: connector, config
├── engine/                 Build, Pipeline, stage loops, retry, DLQ, backpressure, drift, negotiation.
│                                                                             imports: all of the above + store
├── store/                  ConfigStore, StateStore, Coordinator, StatusSink interfaces.
│   ├── singlenode/         laptop implementations.                           imports: store
│   ├── bbolt/              embedded ConfigStore + StateStore.                 imports: store
│   └── postgres/           cluster implementations.                           imports: store
├── codecs/                 json, ndjson, avro, csv, protobuf, raw + newline/length/character framers
│   │                       + gzip/zstd. Each its own subpackage.             imports: connector, registry
├── buffers/
│   ├── memory/                                                               imports: connector, registry
│   └── wal/                segmented log, CRC32C, CanDecode predicate.       imports: connector, registry
├── transforms/             filter, map, dedupe, scan_shadow, route, split, expand.
│                                                                             imports: connector, registry
├── connectors/             THIRD-PARTY-SHAPED. Nothing here is privileged.
│   ├── file/                                                                 imports: record, fault, config,
│   ├── http/                                                                         connector, registry
│   ├── stdio/
│   └── ...
├── api/                    HTTP + SSE: registry walk, spec export, validate, status, choices.
│                                                                             imports: engine, registry, telemetry, store
├── ui/                     TypeScript. Browser only (design rule R11).
└── cmd/canal/              run | serve | validate | discover | status | offsets.
                                                                              imports: everything
```

**The one statement that matters.** A connector package imports exactly five canal packages:
`record`, `fault`, `config`, `connector`, `registry`. It cannot import `engine`, `ledger`, `store`,
`telemetry` or `api`, and the dependency test enforces it. There is therefore no connector-visible
surface through which the core could grow a switch statement on connector identity, because the core's
types are not reachable from a connector at all.

Dependency direction is strictly downward with one deliberate cycle-break: `ledger` imports `connector`
(for `Ordering` and `Ack`) but `connector` never imports `ledger`. `connector` defines the vocabulary;
`ledger` implements the algorithm over it.

---

## 11. Walkthroughs

### (a) One record, from a trivial source to a trivial sink

The connector, in full. This is the whole cost of adding a source.

```go
package linefile

func init() {
	registry.Default.AddSource(registry.SourceDef{
		Name: "line_file",
		Spec: config.NewSpec().
			Field(config.Field{Name: "path", Type: config.TypeString,
				Description: "File to read, line by line."}).
			Field(config.Fields.LaneBudget("in_flight")),
		Caps: connector.SourceCaps{
			DefaultOrdering: connector.OrderingPrefix,
			Boundedness:     []connector.Boundedness{connector.BoundednessBounded},
			LaneKinds:       []connector.LaneKind{connector.LaneKindScan},
			MaxLanes:        1,
			Replayable:      true,
		},
		New: func(ctx context.Context, cfg *config.Config) (connector.Source, error) {
			p, err := cfg.String("path")
			return &src{path: p}, err
		},
	})
}

type src struct {
	path string
	rt   *connector.SourceRuntime
	lane record.LaneID
	f    *os.File
	sc   *bufio.Scanner
	off  int64
	p    *connector.Persister
}

func (s *src) Open(ctx context.Context, rt *connector.SourceRuntime) error {
	s.rt, s.p = rt, connector.AutoPersist(rt)
	as, err := rt.Lanes().Assigned(ctx)
	if err != nil { return err }
	if len(as) == 0 {
		s.lane, err = rt.Lanes().Open(ctx, connector.LaneSpec{
			Name: s.path, Stream: "lines", Kind: connector.LaneKindScan,
			Ordering: connector.OrderingPrefix, Boundedness: connector.BoundednessBounded,
			Label: "reading " + s.path,
		})
		if err != nil { return err }
	} else {
		s.lane = as[0].ID
		if tok, _ := s.p.Restore(s.lane); tok != nil {
			s.off = int64(binary.BigEndian.Uint64(tok))
		}
	}
	if s.f, err = os.Open(s.path); err != nil {
		return fault.Transient(fault.OpOpen, err)
	}
	if _, err = s.f.Seek(s.off, io.SeekStart); err != nil {
		return fault.Permanent(fault.OpOpen, err)
	}
	s.sc = bufio.NewScanner(s.f)
	return nil
}

func (s *src) Read(ctx context.Context, dst *record.Batch) error {
	dst.Reset()
	dst.Lane = s.lane
	for dst.Len() < 500 && s.sc.Scan() {
		line := s.sc.Bytes()
		s.off += int64(len(line)) + 1
		r := &record.Record{Payload: record.BytesPayload(slices.Clone(line))}
		dst.Append(r)
	}
	if err := s.sc.Err(); err != nil {
		return fault.Transient(fault.OpRead, err)
	}
	dst.EndOfLane = dst.Len() == 0 || s.sc.Err() == nil && !s.sc.Scan()
	var tok [8]byte
	binary.BigEndian.PutUint64(tok[:], uint64(s.off))
	dst.Position = record.Position{
		Token: tok[:], TokenCodec: 1, Safe: true, At: time.Now(),
		Label: fmt.Sprintf("byte %d", s.off),
	}
	if dst.Len() == 0 { return fault.ErrEndOfInput }
	return nil
}

func (s *src) Commit(ctx context.Context, a connector.Ack) error { return s.p.Commit(ctx, a) }
func (s *src) Close(ctx context.Context) error                   { return s.f.Close() }
```

The sink is shorter still:

```go
func init() {
	registry.Default.AddSink(registry.SinkDef{
		Name: "stdout",
		Spec: config.NewSpec(),
		Caps: connector.SinkCaps{MaxConcurrency: 1, RequiresCodec: true, Idempotent: false},
		New: func(context.Context, *config.Config) (connector.Sink, error) { return &sink{}, nil },
	})
}

type sink struct{ w *bufio.Writer }

func (k *sink) Open(_ context.Context, rt *connector.SinkRuntime) error {
	k.w = bufio.NewWriter(os.Stdout); return nil
}
func (k *sink) Write(_ context.Context, req *connector.Request) error {
	if _, err := k.w.Write(req.Body); err != nil { return fault.Transient(fault.OpWrite, err) }
	// nil MEANS DURABLE, so the flush is not optional and not deferred.
	return k.w.Flush()
}
func (k *sink) Close(context.Context) error { return k.w.Flush() }
```

**The trace for one record.**

1. `Build` resolves both, snapshots capabilities into `ResolvedSource`/`ResolvedSink`, negotiates
   `AtLeastOnce` (source `Replayable`, sink not `Idempotent`, so not `EffectivelyOnce`), and returns
   `Negotiated{DurabilityEdge: "sink", ReplayWindow: 1000}`.
2. `Pipeline.Run` calls `Source.Open`. The source finds no assignment, calls `LaneCtl.Open`. The core
   assigns `LaneID("lines/./data.txt")`, writes the lane row through `StateStore.Set` **before
   returning**, and registers the lane with the ledger as `OrderingPrefix, budget 1000`.
3. The read loop calls `Read(ctx, batch)`. The source appends one record and sets
   `Position{Token: 0x2A, Safe: true, Label: "byte 42"}`.
4. `Ledger.Admit(ctx, batch)`: budget has room, so no block. The core assigns `GroupID(1)`,
   `RecordID(1)`, `Origin{Lane, Group: 1, ID: 1, Stream: "lines", ReadAt: now}`, and
   `Position.Seq = 1`. `Tracker.Track(position, weight 1)` returns a `Resolve`, held internally against
   group 1. `canal_records_read_total{lane} = 1`.
5. No decoder configured (the source produced bytes and the encoder is `raw`), no transforms, no buffer.
   The batcher's `Add` returns true at `MaxRecords: 1`.
6. Encode stage: `raw.Encode` appends the payload, `newline.Frame` appends `\n`, no compressor. The
   engine builds `Request{Body: "hello\n", Records: []Ref{{ID: 1, Group: 1}}, Count: 1,
   ContentType: "application/octet-stream", Attempt: 1}`.
7. `Sink.Write` returns nil. The engine calls `Ledger.Settle(RecordID(1), Delivered, nil)`.
   `canal_records_written_total = 1`.
8. Group 1's refcount reaches zero. The ledger invokes the held `Resolve`, which returns
   `(position, true)` — the prefix advanced. Because `position.Safe` is true, the ledger emits
   `Ack{Lane, Through: position, Records: 1, Outcome: OutcomeDelivered}` on `Acks()`.
9. The commit pump calls `Source.Commit(ctx, ack)`. `Persister.Commit` writes
   `StateHandle.Set(lane, token, ifVersion: 0)` → version 1. `canal_records_committed_total = 1`,
   `canal_checkpoint_age_seconds` resets, `LaneStatus.Position = "byte 42"`.
10. The next `Read` returns `ErrEndOfInput`. The engine drains (nothing outstanding), finishes the lane,
    closes sink then source, and `Run` returns nil.

The sink saw no offset, no position, no lane, no ack callback. The source saw no sink. Nothing in the
core mentioned files or stdout.

### (b) Full scan, then incremental stream, crashing at 40% of the scan

A generic "keyed store with a change log" source, configured `snapshot: initial`, `scan_lanes: 8`.

**Cold start.**

1. `Open` calls `Assigned` → empty. Cold start.
2. The source captures the change-log position **first**: `logPos = tx.CurrentLogPosition()`.
3. It announces the tail lane **before any scan lane**:
   ```go
   tail, _ := rt.Lanes().Open(ctx, connector.LaneSpec{
       Name: "tail", Stream: "orders", Kind: connector.LaneKindStream,
       Ordering: connector.OrderingPrefix, Boundedness: connector.BoundednessUnbounded,
       Resume: encodeLogPos(logPos), ResumeCodec: 3, Label: "changelog from " + logPos.String(),
   })
   ```
   `LaneCtl.Open` persists the row through `StateStore.Set` before returning. **This single call is the
   entire handoff invariant**: the fact that the tail must resume from `logPos` is durable before one
   scan row moves, and the core made it durable without knowing what a log position is. Debezium and
   Airbyte both smuggled this into an opaque checkpoint and both lost snapshot progress, snapshot
   parallelism and re-parallelised resume.
4. The source splits the key space into eight ranges — using `scan.KeyRanges`, a helper library, not a
   core stage — and announces eight bounded scan lanes with `Resume: encodeRange(lo, hi)`,
   `Weight: estimate`, `Label: "chunk 3/8: id in ['acme','beta')"`.
5. `Read` serves the eight scan lanes round-robin, up to `Parallelism`. It does **not** read the tail
   lane yet. Nothing in the core knows or cares: `Read` simply does not produce tail records. There is no
   phase, no state machine, no `if snapshotting`.
6. Scan records carry `Change{Op: OpScanRead, After: …, Completeness: CompletenessAfterOnly}` and
   `Origin.Key` from the primary key. `Position.Token` is the last key read in that range;
   `Safe: true` (a key boundary is always a legal resume point for a range scan).
7. Each scan lane has its own tracker, its own budget, its own watermark. `ScanProgress` reports
   `lanesTotal: 8, lanesFinished: 2, fraction: 0.41` — computed from lane weights by the core, for any
   source, with no connector code.

**`kill -9` at 40%.**

Durable state at that instant: nine lane rows (`tail@logPos`, `scan/0..7` with their ranges), plus a
state blob per lane holding the last committed token. Lanes 0 and 1 are marked finished. Lanes 2 and 3
hold mid-range tokens. Lanes 4–7 have no committed token. In-flight records — at most `budget` per lane
— are lost and will be re-read.

**Restart.**

8. `Open` → `Assigned` returns **seven** assignments: `tail@logPos`, `scan/2` at `key > 'acme-991'`,
   `scan/3` at `key > 'zeta-2'`, and `scan/4..7` at their range starts. Lanes 0 and 1 are absent because
   they finished. **Nothing re-scans a finished range, and nothing restarts a partial range from zero** —
   which is the exact failure Benthos still has (a 500M-row snapshot restarts from zero) and that Airbyte
   needed a protocol change (`is_resumable`, `CheckpointMixin`) to fix.
9. The source reconstructs each lane from `Spec.Resume` plus `Committed.Token` — the same bytes it
   authored, handed back verbatim. It does not re-announce; it does not need to know it crashed.
10. Re-parallelisation is free: the five unfinished scan lanes are five independent rows, so in a cluster
    a different worker may claim each. The scan can resume with more workers than it started with — the
    thing one-shot planning makes inexpressible.
11. Scan lanes finish. Each final batch carries `EndOfLane: true`; the core finishes the lane only once
    every group for it has settled, then emits `Ack{LaneFinished: true}`.
12. The source now reads the tail lane, from `logPos` — captured before the scan, durable since step 3.
    Records replay from there, including changes that occurred *during* the scan.
13. Duplicate handling at the handoff is explicit, configured, and visible, not implicit:
    - **Idempotent keyed sink (the common case):** nothing to do. Negotiated `EffectivelyOnce`; the
      replayed changes upsert over the scan rows.
    - **Non-idempotent sink:** the operator adds the `scan_shadow` transform, which drops an
      `OpScanRead` record whose key has already been seen from the tail lane in this run, and self-retires
      when the last scan lane closes. It is a component, not a core stage; a pipeline that does not need
      it does not pay for it.
    - The core refuses at `Build` to run a mutable-state scan into a non-idempotent sink with no
      `scan_shadow` and `Guarantee: EffectivelyOnce` requested — a diagnostic at submit time, not an
      anomaly at 3am.
14. `LaneStatus` for the tail lane now shows `kind: "stream"`, `label: "changelog from …"`,
    `position: "lsn 0/1A2B3C4"`, `checkpointAgeSeconds: 0.4`. `ScanProgress` is nil, so the UI stops
    showing a scan bar. Nothing switched on a phase; the scan section is nil because no scan lane exists.

### (c) A sink failing mid-batch, recovering without loss

Configured `Retry{MaxAttempts: 4, Backoff: 100ms→5s full-jitter, Terminal: TerminalDLQ}`,
`Batching{MaxRecords: 500}`, sink declares `PartialFailure: true`.

1. Group 77 admitted: 500 records, `Position{Seq: 41000, Token: lsn-9, Safe: true}`. Groups 78 and 79
   are also in flight (out-of-order settlement is normal).
2. Encode → one `Request` with 500 `record.Ref`s.
3. `Write` returns:
   ```go
   be := fault.NewBatchError(fault.Transient(fault.OpWrite, errServerBusy))
   for _, id := range rejected { be.Fail(id, fault.Mapping(fault.OpWrite, errBadValue)) }
   return be
   ```
   with 12 rejected: 9 transient, 3 mapping failures.
4. The engine reads the `BatchError`. Because `Fail` was called at least once, the other 488 succeeded:
   `Ledger.Settle(id, Delivered, nil)` × 488. Group 77's refcount drops to 12 and the group stays pending.
   **No prefix movement yet, so no premature commit.** The wall-clock re-emission bug — Kafka Connect's
   `offset.flush.interval.ms` re-emitting fully-acked chunks after a crash, producing KAFKA-4942's
   famous log line — is unreachable, because nothing here is on a timer.
5. The engine rebuilds a retry request from **exactly the 12 failed `RecordID`s**. There is no positional
   correlation, no sort-group side table, no fallback to retrying the whole batch — which is what
   Benthos does when its correlation fails, amplifying duplicates.
6. The 3 `PermanentMapping` records skip retry entirely (`Class.Retryable() == false`) and go straight to
   terminal disposition. The 9 transient records retry: attempt 2 lands 7, attempt 3 lands 2. Zero
   remaining.
7. Terminal disposition for the 3: the DLQ sink writes them with full provenance —
   `Origin` (lane, group, stream, key, read time), the fault class, the user message, the attempt count,
   and the redacted config revision. `Write` returns nil → `Ledger.Settle(id, Abandoned, f)` × 3.
8. Group 77 reaches zero. `Resolve` fires. But group 76 is still pending, so `Resolve` returns
   `(_, false)`: **resolved, commit nothing.** The prefix has not moved.
9. Group 76 settles. Now the prefix advances through 76 and 77 in one step, to the highest `Safe`
   position at or before 77 — which is 77's own. One ack:
   `Ack{Through: {Seq: 41000, Token: lsn-9}, Records: 1000, Outcome: OutcomeMixed, Abandoned: 3}`.
10. The source commits `lsn-9`. `canal_records_committed_total += 1000`,
    `canal_records_abandoned_total{reason="dlq"} += 3`.

**The loss-free property, stated exactly.** If the DLQ write in step 7 fails, the settle does **not**
happen. Group 77 stays pending. The prefix stays at 76. `canal_oldest_pending_age_seconds` climbs, and at
`GroupTTL` the ledger raises `CondDegraded` with the group, lane and stage named. The lane's pending
weight reaches its budget and `Admit` blocks, so the source stops reading, so upstream feels the
backpressure. **Nothing is acknowledged, nothing is committed, and nothing is lost.** The pipeline stalls
loudly instead of losing data quietly — the exact inversion of design rule R4's original violation.

And if the process is killed at any point in this sequence, the source resumes from `lsn-9`'s
predecessor and re-delivers everything after it. The 488 already-written records are re-written. With an
idempotent sink they are absorbed; without one they are duplicates, bounded by the budget, counted, and
disclosed in the read model as `replayWindowRecords`.

### (d) Serving the frontend with zero connector-specific code in the core

Everything the UI needs comes from five connector-authored artefacts, all of them **data**:
`config.Spec`, `*Caps`, `Position.Label`, `LaneSpec.Label`, and `config.Diagnostics`. There is no type
assertion above the `Resolve` layer and no connector name anywhere in `api/`.

**Building a "new pipeline" form.**

```
GET /v1/connectors
  → registry.Walk → [{kind, name, summary, deprecated, caps}]
    The UI groups by kind and greys out what is impossible: a source whose LaneKinds omits
    LaneKindScan gets no "initial scan" toggle. No connector list in the frontend.

GET /v1/connectors/mystore/spec
  → the config.Spec, verbatim, as JSON: fields, types, titles, descriptions, defaults,
    optional/advanced/secret flags, enums with per-value descriptions, nesting, tagged-union
    variants with their discriminators, show-if and required-if predicates, choice hook names.
    The form is a fold over a closed FieldType vocabulary — ~200 lines of TypeScript, once,
    for every connector that will ever exist.

GET /v1/connectors/mystore/choices/list_streams?config=<partial>
  → ChoiceProvider.Choices(ctx, "list_streams", partial) → []EnumValue
    This is how "pick a table" works. The core forwards a hook name and a partial config; it has
    no idea that tables exist.

POST /v1/pipelines:validate
  → tier 1: Spec.Validate — structure, types, ranges, enums, unknown fields (CodeUnknownField is
    what catches typo'd YAML, the classic silent failure of every config-driven tool), declared lints.
  → tier 2: connector.Validator.Validate — may do I/O: connect, authenticate, check the stream
    exists, check write permission.
  → engine.Build: capability negotiation. Every impossible combination becomes a Diagnostic anchored
    to the field that caused it.
  → 200 with { diagnostics: [...], negotiated: { guarantee, durabilityEdge, replayWindow, reasons } }
    ALL problems at once, each with a path, a code, a message and a hint. Never fail-fast; a form
    that surfaces one error at a time is a form operators fight.
```

The `negotiated` block is the honesty mechanism. An operator who asked for `EffectivelyOnce` and got
`AtLeastOnce` sees the downgrade **and its reason** on the submit screen, before saving. Vector's
equivalent degradation is silent; this design makes it a field.

**Live status.**

```
GET /v1/pipelines/{id}/status            → telemetry.PipelineStatus, ETag = Version
GET /v1/pipelines/{id}/status/watch      → SSE, id: = Version, full document every N events
GET /metrics                             → Prometheus; the closed name and label set
```

Every number in `PipelineStatus` is derived from core-owned state:

| UI element | Source | Connector code required |
|---|---|---|
| lane list, kinds, labels | `LaneSpec` rows | the `Label` string |
| "position: lsn 0/1A2B3C4" | `Position.Label` | the `Label` string |
| checkpoint age (primary alert) | `ledger.Watermark().At` | none |
| records read / committed / abandoned | ledger counters | none |
| in-flight, replay window | `Tracker.Pending()`, `Budget()` | none |
| scan progress bar and ETA | lane weights + finished count | `Weight`, optional |
| "this stage is the bottleneck" | `canal_stage_utilization_ratio` | none |
| backlog / lag | `BacklogReporter` | optional; nil renders "unknown" |
| phase, conditions | engine state machine | none |
| last error, class, what to do | `Fault.Class` + `UserMessage` | the two strings |

**Two disciplines that make it stay honest.** Every unknown is a nil pointer with one shared "unknown"
renderer, and a pinned fixture in which every optional field is absent asserts the UI renders no zeros —
so "the connector cannot tell you the lag" never displays as "the lag is zero". And `Complete: false`
when the aggregator has not heard from every worker, so a partial document says "partial" instead of
quietly under-reporting. The CLI (`canal status --watch`) consumes the same SSE endpoint the browser
does, which is the best integration test the read model will ever get.

Adding a connector adds: one row to `GET /v1/connectors`, one spec document, and lanes in the status
page. Zero lines of core or frontend code.

### (e) The same pipeline standalone, then horizontally scaled

**`canal run -f pipeline.yaml` on a laptop.** No dependencies. `Deps` is
`{singlenode.Coordinator{}, bbolt.ConfigStore, bbolt.StateStore, singlenode.StatusSink}`.
`Coordinator.Plan` writes assignment rows to bbolt; `Claim` always succeeds; leases never expire;
`Campaign` returns leadership immediately. `LaneCtl.Assigned` returns every announced lane. One process
runs the leader role, the worker role, the API and the pipeline.

**`canal serve` against Postgres, three workers.** `Deps` swaps to
`{postgres.Coordinator, postgres.ConfigStore, postgres.StateStore, postgres.StatusSink}`.

What differs:

- One worker wins `pg_try_advisory_lock` and becomes planner. It watches announced lane rows and writes
  `assignments(pipeline, assignment_id, revision, lane_spec, worker_id, lease_expires_at, generation)`.
  It **only plans**: it does not route data, does not proxy status, holds nothing the data path needs.
- Each worker claims unclaimed rows with `SELECT … FOR UPDATE SKIP LOCKED`, takes a 30s lease, and
  renews at 10s. It then constructs its own `Source` instance whose `LaneCtl.Assigned` returns exactly
  its claimed subset. **The source code is byte-identical** — it was already written to read `Assigned`
  and reconstruct from `Resume`, because that is what restart requires. Distribution is restart, with a
  different subset.
- A worker's lease lapsing closes `Revoked(lane)`; the source stops producing for it; in-flight records
  still settle; another worker claims the row after expiry. Reassignment is deliberately delayed (default
  120s against a 30s TTL) so a bouncing worker reclaims its own lanes instead of triggering a
  cluster-wide reshuffle.
- `MaxLanes` is enforced at plan time and a violation fails the pipeline loudly, rather than being
  advisory for eight years.

What must never differ, and does not: `Source`, `Sink`, `Buffer`, `Transform`, every codec, `Record`,
`Position`, `Ack`, `LaneSpec`, `Caps`, `config.Spec`. The connector-facing API is byte-identical in both
modes, which is the correct seam and the one thing Kafka Connect got unambiguously right.

**The property this angle buys, which an offset-store design cannot.** Leadership is never trusted for
correctness — `k8s.io/client-go`'s own leader-election docs state the implementation "does not guarantee
that only one client is acting as a leader". The lease is the fencing token and `StateStore`'s per-key
CAS is the second fence. So:

*Kill the leader, kill the config store, kill the API.* Every worker holding a valid lease keeps reading,
transforming, writing, settling and **committing**, because `Source.Commit` does not consult the cluster.
For a source whose progress lives upstream — a consumer-group offset, an SQS delete, a replication-slot
advance — commit does not touch canal's storage **at all**, so the data plane is completely independent
of the control plane. That class of pipeline runs indefinitely with canal's entire control plane down.

For a source that borrowed `StateHandle`, persistence pauses while Postgres is unreachable. The ack chain
keeps working, so nothing is lost and nothing is falsely acknowledged; the replay window grows, and the
core discloses it: `canal_state_persist_staleness_seconds` climbs and `CondDegraded` is raised with
reason `state_store_unreachable`. Honest degradation, visible, bounded. (A local WAL with async upload
would remove even this; it is deliberately not in scope.)

**The rebalance that never happens.** There is no stop-the-world rebalance protocol, because there is
nothing global to agree on: assignment is per-lane, claimed by lease, and the plan is durable state
rather than a leader's in-memory computation. Kafka Connect's stop-the-world rebalance and its buggy
replacement are both absent — not solved, absent.

---

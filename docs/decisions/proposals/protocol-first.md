# Proposal: `protocol-first, in-process today`

**Status:** draft (R12). Proposal only — not normative until adopted as an ADR.
**Angle:** the contract is a *message protocol*; the Go interface set is one binding of it.

---

## Thesis

canal's extensibility boundary is **not a Go interface** — it is a **typed, versioned, ordered stream
of frames**. `Record`, `Schema`, `Mark`, `SplitDelta`, `Estimate`, `StreamStatus`, `Log`, `Fault`,
`Control`: one closed tagged union, defined once, in a package with no dependencies and no behaviour.
Every operation in canal is `(ctx, serialisable request) → (stream of frames, error)`. The Go
interfaces a connector author implements are a **binding** of that protocol — the in-process binding —
and the gRPC subprocess binding and a future WASM binding are two more bindings of *the same frames*,
added without touching `proto/`, `engine/`, or a single connector.

The single bet: **if the contract is data, then genericity, wire-shippability, UI-renderability,
checkpoint durability and the standalone/cluster seam are all the same property, purchased once.** A
frame is source-agnostic because it has no source-shaped field. A frame crosses a process boundary
because it is data. A frame renders in a browser because it is data. A frame is checkpointable because
it is data. The reader↔enumerator boundary is a protocol, so it is *also* the worker↔planner boundary,
so horizontal scaling is a transport swap and not a rewrite. Constraint #1, #3 and the frontend goal
stop being three problems.

And the cost that usually kills this bet — paying serialisation in-process — is refused explicitly: a
frame is a **Go struct in a caller-owned batch**, filled by direct appends in-process and by a decoder
over a wire. In-process canal marshals **nothing**. Ever. The protocol is a *shape* discipline, not a
runtime tax.

---

## 0. The four rules that generate everything else

**P1 — Everything in a frame is data.** No `func`, no `chan`, no `interface{}` except one sanctioned
payload slot, no pointer-as-identity, no host handle. Enforced by a reflective conformance test over
the whole `proto` type graph, not by discipline (R8).

**P2 — Handles are parameters, never payload.** `Emitter`, `Host`, `Batch` are interfaces *supplied by
the binding* on each side of the boundary. The in-process binding implements them with method calls;
the gRPC binding implements them with stream sends. A handle inside a frame would be a design defect;
a handle as a parameter is how Go surfaces a bidirectional stream.

**P3 — The unit crossing a boundary is a batch of frames written into a caller-owned buffer.**
`Fetch(ctx, *Batch) error`. In-process this is zero-copy and zero-alloc; over a wire it is one decode
into the same struct. This is `database/sql`'s `Rows.Next(dest []Value)` generalised from values to
frames, and it is what makes the protocol free in-process.

**P4 — Behaviour is an optional interface; the *fact* of that behaviour is declared data.** Declaring
a capability without implementing its interface panics at registration. The engine type-asserts in
exactly one function and materialises a plain struct (`database/sql`'s `rowsColumnInfoSetupConnLocked`
pattern). The declaration is what crosses a wire; the assertion is what dispatches in-process.

---

## 1. Package layout and dependency direction

```
canal/
  proto/                THE CONTRACT. stdlib only. Data types, enums, codecs, versioning.
                        Imports: nothing outside stdlib. Imported by: everything.
    frame.go            Frame union, Kind, Criticality, Batch buffer
    record.go           Record, Payload, Meta, Value, Change facet, Origin
    cursor.go           Cursor, Mark, MarkID, Blob, Checkpoint, CheckpointHeader, SplitState
    split.go            SplitID, SplitRef, Split, Estimate, Phase
    catalog.go          Catalog, Stream, ConfiguredCatalog, SyncMode pair
    schema.go           Schema, SchemaID, Field, ValueKind, SchemaChange, DriftPolicy
    spec.go             Spec, Field, Predicate, Directive, Validator, Diagnostic, Widget
    capability.go       Capability constants, Capabilities set, Guarantee tiers
    fault.go            Class, Fault, Disposition, RetryPolicy
    control.go          Hello/Accept, Assign/Revoke, Confirm, Drain, ConfigPatch
    codec.go            Versioned[T], wire-safety conformance harness
    version.go          const Version; frame-kind criticality table

  connector/            WHAT A CONNECTOR AUTHOR IMPORTS. Imports proto only.
    source.go           Source, Enumerator, Reader + optional source interfaces
    sink.go             Sink + optional sink interfaces
    host.go             Host (log, metrics, secrets, config patch)
    spec.go             Spec builders: TLS(), Retry(), OneOf(), Bind()
    registry.go         Registry value type, SourceDescriptor, SinkDescriptor, Register
    testkit/            conformance suite every connector runs. Imports connector + proto.

  transport/
    inproc/             Bind(Source) -> engine-facing session. Direct calls. Zero marshalling.
    grpc/               NOT BUILT. Package doc states the mapping frame->protobuf message.
                        Imports proto only. Never imports engine.

  codec/                Serializer, Framer, Compressor registries. Imports proto.
    json/ avro/ csv/ ndjson/ gzip/ ...

  runtime/              THE DEPLOYMENT SEAM. Four interfaces, two assemblies. Imports proto.
    store.go            ConfigStore, CheckpointStore
    coordinate.go       Coordinator, Lease, Membership, Leadership
    status.go           StatusAggregator, PipelineStatus read model
    singlenode/         bbolt + always-leader. Imports runtime + proto.
    postgres/           Imports runtime + proto.

  engine/               THE RUNTIME. Imports proto, connector, codec, runtime.
    pipeline.go         assembly, Negotiate(), generation/observedGeneration
    readerloop.go       reader driver: assignment, fetch, mark, in-flight accounting
    writerloop.go       sink driver: batching, per-record outcomes, commit tiers
    enumloop.go         enumerator driver + chunked-snapshot engine (watermarks, dedup filter)
    resolve.go          contiguous-prefix resolver, per split
    buffer.go           Buffer stage, WhenFull
    transform.go        Transform chain, provenance-preserving derivation
    metrics.go          core-owned metric names and the closed label vocabulary
    probe.go            THE ONLY FILE CONTAINING A CONNECTOR TYPE ASSERTION

  api/                  HTTP + SSE. Imports proto, runtime. NEVER imports engine or connector impls.
  cmd/canal/            run | serve | discover | validate | spec. Imports everything.
  connectors/           Out-of-tree-shaped in-tree connectors. Import connector + proto ONLY.
```

**The load-bearing rule: `connectors/*` cannot compile against `engine/`.** There is no import path
from a connector to the runtime. A connector that needs an engine type is a design defect that fails
`go vet ./...` with a custom analyser in `tools/importcheck`.

**Second rule:** `api/` cannot import `engine/`. The frontend is served from the read model in
`runtime/`, which means the API is structurally incapable of depending on connector-specific anything.

---

## 2. The protocol: frames

```go
// Package proto is canal's contract. It contains data types, enumerations and
// their versioned serialisers, and nothing else: no goroutines, no I/O, no
// interfaces except the two sanctioned extension points documented below.
//
// # Wire safety
//
// Every exported type in this package satisfies the wire-safety invariant: its
// transitive field graph contains no func, chan, unsafe.Pointer, or interface
// type other than Value (a closed dynamic type set) and Payload.structured
// (which is never serialised; see Payload). TestWireSafety in codec_test.go
// asserts this reflectively over every registered type, so the invariant is
// checked by CI rather than by review.
//
// # Versioning
//
// Version is bumped only for a change no receiver can ignore. Adding a frame
// kind, a field, an enum member or a capability is additive and does not bump
// it. Every frame kind declares a Criticality: an unknown Ignorable kind is
// counted and skipped; an unknown MustUnderstand kind aborts the session at
// Hello time, never mid-stream.
package proto

// Version is the protocol version this build speaks.
const Version uint32 = 1

// Kind discriminates a Frame. Kinds are append-only; a kind is never removed
// and never renumbered.
type Kind uint8

const (
	KindInvalid Kind = 0

	// --- data plane, source -> engine -> sink ---
	KindRecord       Kind = 1  // one datum
	KindMark         Kind = 2  // "everything before me in this split is at Cursor"
	KindSchemaChange Kind = 3  // ordered, in-band DDL

	// --- source -> engine only ---
	KindSplitDelta   Kind = 10 // splits discovered, assigned, completed or revoked
	KindEstimate     Kind = 11 // connector's work estimate
	KindStreamStatus Kind = 12 // per-stream lifecycle
	KindHead         Kind = 13 // current head of an unbounded stream (enables lag)

	// --- sink -> engine only ---
	KindOutcome     Kind = 20 // per-record write outcome (R7)
	KindCommittable Kind = 21 // 2PC staging handle
	KindMarkRequest Kind = 22 // "please give me a checkpoint boundary"

	// --- either direction ---
	KindLog         Kind = 30
	KindFault       Kind = 31
	KindMetricSample Kind = 32
	KindConfigPatch  Kind = 33 // credential rotation
)

// Criticality says what a receiver must do with a kind it does not know.
type Criticality uint8

const (
	Ignorable      Criticality = 0 // skip, increment canal_frames_ignored_total
	MustUnderstand Criticality = 1 // refuse the session at Hello, never mid-stream
)

// CriticalityOf is the single table mapping kinds to criticality. A new kind
// added in a later version is Ignorable unless correctness demands otherwise;
// KindMark and KindSchemaChange are MustUnderstand because silently skipping
// either loses durability or corrupts a sink.
func CriticalityOf(k Kind) Criticality { /* table lookup */ }

// Frame is the closed tagged union that is canal's entire contract.
//
// It is a struct, not an interface, for three reasons: a struct union is
// wire-shippable without a registry of concrete types; it is allocation-free
// when stored in a Batch's backing array; and exhaustiveness over Kind is
// checkable by a linter. Exactly one pointer field is non-nil, selected by Kind.
//
// Ownership: when a Frame is handed to a receiver (Batch.Append, Emitter.Send,
// Sink.Write), ownership of every byte slice it references transfers to the
// receiver. The sender must not read, mutate or retain them afterwards. In
// builds tagged `canaldebug` the sender's slices are poisoned with 0xDE on
// transfer, so a retention bug fails a test instead of corrupting data.
type Frame struct {
	Kind Kind

	Record       *Record
	Mark         *Mark
	SchemaChange *SchemaChange
	SplitDelta   *SplitDelta
	Estimate     *Estimate
	StreamStatus *StreamStatus
	Head         *Head
	Outcome      *Outcome
	Committable  *Committable
	MarkRequest  *MarkRequest
	Log          *LogEntry
	Fault        *Fault
	Metric       *MetricSample
	ConfigPatch  *ConfigPatch
}
```

### 2.1 The batch: why the protocol is free in-process

```go
// Batch is a bounded, caller-owned buffer of Frames. It is the only unit that
// crosses a connector boundary.
//
// In-process, a Batch is filled by direct method calls on the connector's own
// goroutine: no channel, no marshalling, no allocation past the first fill.
// Over a wire, the gRPC binding decodes into the same Batch. Neither the engine
// nor the connector can tell which happened, and that is the whole point of
// the protocol-first bet.
//
// A Batch is single-goroutine. Passing one to a second goroutine is a defect
// that -race will find.
type Batch struct {
	frames  []Frame        // reused across Fetch calls; len is reset, cap is kept
	records []Record       // slab; Frame.Record points into it
	maxRecs int
	maxBytes int
	bytes   int
	splits  *splitTable    // SplitRef -> SplitID, session-scoped
	stamp   stamper        // assigns RecordID and Origin
}

// Record lends the connector the next free Record slot with its framework-owned
// identity fields already stamped. The connector fills the exported fields and
// must not retain the pointer past the next Record/Mark/Flush call.
//
// This is the mechanism that makes provenance structurally immutable: a
// connector cannot construct a Record with an id or an Origin of its choosing,
// because those fields are unexported and this is the only constructor.
func (b *Batch) Record(split SplitRef) *Record

// Mark appends a position-carrying checkpoint mark for one split. Records
// appended before it are covered by it. Cursor is opaque to the engine.
func (b *Batch) Mark(split SplitRef, c Cursor)

// Schema appends an ordered in-band schema change. Records appended after it
// are governed by it.
func (b *Batch) Schema(sc SchemaChange)

// Emit appends an already-built frame. Used by transforms and by the wire
// bindings; connectors normally use the typed helpers.
func (b *Batch) Emit(f Frame)

// Full reports whether the engine's declared limits are reached. A reader
// should return from Fetch when Full is true; the engine also enforces it.
func (b *Batch) Full() bool

func (b *Batch) Len() int
func (b *Batch) Frames() []Frame     // read-only view for the engine
func (b *Batch) Reset()              // engine-only; returns slots to the slab

// Derive lends a slot for a record produced *from* in, copying in's Origin
// verbatim and assigning a fresh in-flight id. Transforms have no other way to
// produce a record, so a transform cannot corrupt checkpoint identity — the
// Kafka Connect KIP-793 retrofit is closed by construction rather than by
// prose in two javadocs.
func (b *Batch) Derive(in *Record) *Record

// Drop marks a record as intentionally not forwarded, with a bounded reason.
// Used by filters and by DLQ routing. `intentional` is a label on the metric,
// not a separate metric (Vector's encoding).
func (b *Batch) Drop(id RecordID, reason DropReason)
```

### 2.2 Identity: `SplitRef` interning

```go
// SplitID identifies a unit of work durably and across processes. It is a
// struct, not a string: Flink CDC's string-only split id forced structure to be
// encoded into it and re-parsed in three places, and that scar is avoidable for
// the price of two extra fields.
type SplitID struct {
	Stream StreamID // logical dataset this split belongs to
	Key    string   // connector-chosen, stable, opaque to the engine
	Gen    uint32   // bumped when the same Key is re-planned (re-chunking, reset)
}

// SplitRef is a session-scoped integer alias for a SplitID, established by the
// SplitDelta frame that assigns the split and valid until the session ends.
//
// This is deliberate: a Record carries a 12-byte identity instead of a string,
// which matters at six figures of records per second. It is not a host handle
// and it does not violate P1 — it is an interned symbol established in-band, in
// the same way an HTTP/2 stream id or an HPACK index is. A receiver that sees an
// unknown SplitRef aborts the session; it never guesses.
type SplitRef uint32

// RecordID is the framework-assigned, stable in-flight identity of a record.
// Benthos marks its positional equivalent "// Deprecated: This method is
// harmful" in its own source, and roughly two hundred lines of sort-group
// correlation exist there purely because records have no stable id. canal pays
// twelve bytes instead.
type RecordID struct {
	Split SplitRef
	Seq   uint64 // monotonic within (Split, session)
}
```

### 2.3 Record, payload, metadata, change facet

```go
// Record is the canonical in-flight datum. Three layers with three different
// lifetimes: the payload (rewritten by codecs and transforms), the metadata
// namespace (annotated by anyone, addressed separately, never inside the
// payload), and the optional Change facet (present only when the source has
// change semantics to report).
//
// Nothing in this type is source-shaped. There is no table, no collection, no
// topic, no partition, no database. StreamID is a name; Cursor is bytes.
type Record struct {
	id     RecordID // framework-owned; see Batch.Record
	origin Origin   // framework-owned; immutable across transforms

	// Key is the record's identity in the *destination's* terms, or empty.
	// Sinks that upsert use it; sinks that append ignore it. A source may leave
	// it empty and let the configured catalog's PrimaryKey derive it.
	Key Payload

	// Value is the payload.
	Value Payload

	// Meta is the separately-addressable annotation namespace. Framework keys
	// are "canal."-prefixed and write-protected.
	Meta Meta

	// Change is the optional typed change-event facet. nil for sources with no
	// change semantics — a webhook, a metrics scrape, a file tail. Its presence
	// is data, not a type, which is why the core never switches on source type
	// and why this does not make the core relational-shaped.
	Change *Change

	// EventTime is when the event happened at the source, zero if unknown.
	// Zero means unknown and is reported as unknown, never as epoch.
	EventTime time.Time

	// SchemaID names the schema governing Value, or is zero for schemaless.
	SchemaID SchemaID
}

func (r *Record) ID() RecordID  { return r.id }
func (r *Record) Origin() Origin { return r.origin }

// Origin is provenance. It is captured once, at admission, and copied verbatim
// by Batch.Derive. A transform cannot write it. Checkpoint accounting reads
// only this, never the mutable fields — which is exactly the separation
// Kafka Connect had to retrofit as originalTopic/originalPartition/
// originalOffset after SMTs were allowed to rewrite the live ones.
type Origin struct {
	Connector string   // registered source name
	Stream    StreamID // stream as discovered, before any rename transform
	Split     SplitID  // full id, not the ref: survives the session
	Cursor    Cursor   // position of this record within the split
	Parent    RecordID // non-zero if produced by an unframer from a larger frame
	IngestAt  time.Time
}

// Payload holds one value in two interchangeable views. Which view is
// authoritative is tracked internally; conversion happens on demand through the
// pipeline's declared codec. Mutability is in the accessor name, so a
// read-only borrow and a copy-on-write are never confused.
type Payload struct {
	bytes      []byte
	structured any // Value, map[string]Value, []Value, or a codec-registered type
	shared     bool
}

// Bytes returns the encoded form, encoding the structured form if necessary.
// The returned slice is owned by the caller only after a transfer of ownership;
// otherwise it is borrowed until the next mutation of this Payload.
func (p *Payload) Bytes(ctx context.Context, enc Serializer) ([]byte, error)

// Structured returns a read-only view. Mutating it is a defect. Decodes from
// bytes if necessary.
func (p *Payload) Structured(ctx context.Context, dec Deserializer) (any, error)

// StructuredMut returns a mutable view, deep-copying first if the value is
// shared with another record (fan-out).
func (p *Payload) StructuredMut(ctx context.Context, dec Deserializer) (any, error)

func (p *Payload) SetBytes(b []byte)
func (p *Payload) SetStructured(v any)
func (p *Payload) Size() int
func (p *Payload) IsZero() bool

// Meta is the metadata namespace. It is addressed separately from the payload
// so that a codec change cannot lose annotations and an annotation cannot
// collide with a payload field — the two mistakes that made Airbyte and Singer
// independently invent magic prefixed columns (_ab_cdc_*, _sdc_*).
type Meta struct {
	kv      map[string]Value
	secrets map[string]string // never serialised into a payload; redacted in logs,
	                          // in the DLQ, in the read model and in traces
	changes []FieldChange     // per-field lossiness log
}

func (m *Meta) Get(key string) (Value, bool)
func (m *Meta) Set(key string, v Value) error // errors on a "canal." key
func (m *Meta) Keys() []string
func (m *Meta) Secret(key string) (string, bool)
func (m *Meta) SetSecret(key, v string)

// NoteChange records that a field could not be carried faithfully. This is the
// honesty mechanism design-rules demands of the UI, made machine-readable and
// assertable in tests: a truncated string or a nulled unreadable column is a
// countable fact, not a silent difference.
func (m *Meta) NoteChange(fc FieldChange)

type FieldChange struct {
	Path   []string // field path, composite-safe
	Kind   FieldChangeKind // Nulled | Truncated | Rounded | Redacted | Unavailable
	Reason string
}

// Value is the closed dynamic type of a metadata entry and of a structured
// field. Exactly one of: nil, bool, int64, uint64, float64, string, []byte,
// time.Time, Decimal, map[string]Value, []Value.
//
// The set is closed and documented, in the manner of database/sql/driver.Value.
// Decimal carries explicit precision and scale rather than being a float or a
// naming convention, because "logical types as a naming convention that
// connectors and converters silently disagree about" is Kafka Connect's
// cleanest single mistake.
type Value any

type Decimal struct {
	Unscaled  []byte // big-endian two's complement
	Scale     int32
	Precision int32
}

// Change is the optional typed change-event facet.
type Change struct {
	Op Op

	// Before is the pre-image, or nil. Completeness says whether to trust it.
	Before *Payload
	// After is the post-image, or nil for a delete.
	After *Payload

	// BeforeCompleteness and AfterCompleteness answer the question Benthos's
	// Postgres input cannot: whether an image is whole. An unchanged TOAST
	// value, a REPLICA IDENTITY of DEFAULT, or a Mongo update without a
	// full-document lookup all produce a partial image, and a sink that cannot
	// distinguish partial from complete will silently write nulls over data.
	BeforeCompleteness Completeness
	AfterCompleteness  Completeness

	// TxID groups records committed together at the source, or is empty.
	TxID string
	// Sequence is a source-defined total order token within the change stream,
	// opaque and comparable only via the source's CursorComparer.
	Sequence []byte
	// CommitTime is when the source committed the change, zero if unknown.
	CommitTime time.Time
}

type Op uint8

const (
	OpUnknown  Op = 0
	OpInsert   Op = 1
	OpUpdate   Op = 2
	OpDelete   Op = 3
	OpSnapshot Op = 4 // read during a bounded backfill; not a change
	OpTruncate Op = 5 // stream-scoped, no payload
)

type Completeness uint8

const (
	Absent   Completeness = 0 // not supplied
	Partial  Completeness = 1 // some fields present; absent != null
	Complete Completeness = 2 // every field of the governing schema is present
)
```

**On the open question "does the change facet need before-images given
`unchanged_toast_value`?"** — yes, and the reason the question feels unanswerable is that Benthos has
no way to say "this image is partial". `Completeness` plus `Meta.changes` makes a partial image a
declared, countable, testable fact. A sink that cannot merge partials declares
`RequiresCompleteImages` and the pipeline is refused at submit time rather than corrupting rows at 3am.

---

## 3. Cursors, marks, and the checkpoint

```go
// Blob is a versioned opaque payload authored by a connector. The engine never
// interprets Bytes. Version is the connector's own, and is what lets a
// connector change its state format across an upgrade — the thing whose absence
// is the root cause of Airbyte's reset-on-config-change pain.
type Blob struct {
	Version uint32
	Bytes   []byte
}

// Cursor is a position within one split. Opaque to the engine, always.
type Cursor Blob

// MarkID is a monotonically increasing checkpoint identifier, scoped to
// (pipeline, generation).
type MarkID uint64

// Mark is the position-carrying in-band checkpoint frame. A source emits one
// when it reaches a boundary it considers meaningful; the engine may also
// request one (MarkNow) when a sink asks or a policy fires.
//
// This is the spine of canal's commit protocol. Kafka Connect's alternative — a
// wall-clock offset.flush.interval.ms decoupled from batches — re-emits
// fully-acked snapshot chunks after a crash and produces the KAFKA-4942 log
// line users routinely misdiagnose.
type Mark struct {
	ID     MarkID
	Split  SplitRef
	Cursor Cursor

	// Records and Bytes count what this split emitted *before* this mark, since
	// the previous mark. The sink's Outcome frames carry the matching counts, so
	// every mark is an auditable emitted-vs-persisted reconciliation point for
	// the price of two integers.
	Records uint64
	Bytes   uint64

	// Phase is reporting only. The engine stores it, exposes it and never
	// branches on it. This is the whole answer to the trap that Kafka
	// Connect/Debezium and Airbyte fell into independently: both smuggled the
	// snapshot phase into the opaque checkpoint and both lost snapshot progress,
	// snapshot parallelism and re-parallelised resume.
	Phase Phase

	At time.Time
}

type Phase uint8

const (
	PhaseUnknown     Phase = 0
	PhaseBackfilling Phase = 1
	PhaseStreaming   Phase = 2
	PhaseCatchingUp  Phase = 3 // streaming but behind a known head
	PhaseIdle        Phase = 4
	PhaseCompleted   Phase = 5 // bounded split reached End
)

// Checkpoint is the durable aggregate: a small typed header the core reads,
// plus opaque versioned blobs it does not. One record, written atomically.
//
// Full opacity (Kafka Connect, Singer) costs lag and progress, which is
// disqualifying given canal's frontend goal — Connect's own documentation
// admits no source-side lag metric is possible. Full structure (Debezium) costs
// the core-defined shape and a live mutating context object that is
// unshippable over a wire. The header/blob split buys the reportable fields and
// keeps future phases free.
type Checkpoint struct {
	Header CheckpointHeader

	// Shared is the connector's pipeline-wide position, for sources with one
	// global log. nil for sources without one. Airbyte shipped per-stream state
	// first, suffered, and retrofitted the shared form plus a legacy path;
	// canal has both from commit one.
	Shared *Blob

	// Splits is the per-split state. This is simultaneously the resume set, the
	// assignment plan, the in-flight accounting basis and the progress
	// denominator — Flink's insight that the reader's checkpoint *is* its split
	// list, made readable at the header level.
	Splits []SplitState

	// Enumerator is the planner's own state: unassigned splits, the chunker's
	// cursor, the finished-chunk set reference.
	Enumerator Blob

	// Committables are staged sink handles for the two-phase tier. Empty for
	// at-least-once pipelines.
	Committables []Committable

	// SchemaEpoch is committed atomically with position, so decoding a
	// historical event can always find its historical schema. Debezium's two
	// independently-committed stores are the counter-example.
	SchemaEpoch uint64

	// DedupeEntries are written in the same atomic Set as this checkpoint, which
	// is what makes design-rule R5 ("committed after the write") structural: a
	// dedupe key becomes visible in the same transaction that makes the data it
	// protects durable, and never before.
	DedupeEntries []DedupeEntry
}

type CheckpointHeader struct {
	Pipeline   PipelineID
	Generation uint64 // pipeline spec revision this checkpoint belongs to
	MarkID     MarkID
	Phase      Phase // aggregate: the least-advanced split's phase
	At         time.Time

	RecordsRead      uint64
	RecordsCommitted uint64
	BytesCommitted   uint64

	// SpecHash is the hash of the connector's Spec at write time. On restore the
	// core asks the connector "is this still valid under the new config?" rather
	// than guessing, which is the operation Airbyte lacks.
	SpecHash string
	Protocol uint32
}

type SplitState struct {
	Split   Split  // the split value itself, so resume needs no re-planning
	Cursor  Cursor // resume point
	Phase   Phase
	Records uint64
	Bytes   uint64
	Done    bool
	Owner   WorkerID // informational; the lease is authoritative
}
```

### 3.1 The contiguous-prefix resolver

```go
// Resolver, in engine/resolve.go, is the one piece of non-trivial checkpoint
// logic the core owns so that no connector ever writes it. Per split it tracks
// which marks are fully covered by sink durability and advances the committed
// cursor to the highest mark whose entire prefix is durable — never past a hole.
//
// Ack-graph-only designs (Benthos, Vector) push this algorithm into every
// source, out of tree, reimplemented and untested each time; and they cannot
// answer "where are we?" at all, which is disqualifying for canal's frontend.
type Resolver struct{ /* per-split ordered pending set */ }

// Written records that a record was durably written by the sink.
func (r *Resolver) Written(id RecordID, bytes int)

// Failed records a terminal per-record failure and its disposition. A
// DeadLetter disposition counts as durable for prefix purposes once the DLQ
// write is itself durable; a Terminal disposition freezes the prefix.
func (r *Resolver) Failed(id RecordID, f Fault, d Disposition)

// Committed returns the marks that are now fully durable, in id order, and the
// resulting per-split cursors. Implements the Checkpoint Subsuming Contract:
// ids strictly increase and a commit of m subsumes every m' < m.
func (r *Resolver) Committed() (marks []MarkID, splits []SplitState)

// Oldest returns the age of the oldest un-durable record, which is the input to
// canal_oldest_committable_age_seconds and to poison detection.
func (r *Resolver) Oldest() (RecordID, time.Time, bool)
```

---

## 4. Splits, catalog, schema

```go
// Split is one unit of work. Boundedness is declared, not inferred: bounded
// splits get job semantics and a terminal PhaseCompleted, unbounded splits get
// lease semantics, and a snapshot-then-stream pipeline is simply a pipeline
// whose split set contains both. The handoff is not a phase transition in the
// core; it is the split set changing, which is data.
type Split struct {
	ID     SplitID
	Bounded bool

	// Ordered declares that Cursors within this split are totally ordered by the
	// source's CursorComparer. Only an ordered split may be checkpointed
	// mid-split; an unordered one is all-or-nothing. Meltano's sorted/unsorted
	// flag, generalised.
	Ordered bool

	// Start is where to resume. End is meaningful only when Bounded.
	Start Cursor
	End   Cursor

	// SchemaID pins the schema at split start. Deduplicated in enumerator state
	// so a million-split plan does not carry a million schema copies.
	SchemaID SchemaID

	Estimate *Estimate

	// Attrs is connector-opaque split detail (a key range, a file offset, a
	// shard id). The engine stores and ships it and never reads it. This is the
	// field whose absence forced Flink CDC to encode structure into a string id.
	Attrs Blob
}

type Estimate struct {
	Records *uint64 // nil means unknown, and unknown is reported as unknown
	Bytes   *uint64
	Exact   bool // counted vs estimated, as a field not a label, so switching
	             // strategies does not split every graph in two
	At time.Time
}

// SplitDelta is the enumerator's output frame and the assignment frame in one.
// The same frame moves over a channel in single-node mode and over gRPC between
// a planner and a worker in cluster mode.
type SplitDelta struct {
	Assign []Split   // includes the SplitRef binding for each
	Refs   []SplitRef

	Revoke []SplitID

	Completed []CompletedSplit

	// NoMoreSplits, per stream, is what lets a bounded pipeline terminate and a
	// reader return io.EOF honestly.
	NoMoreSplits []StreamID
}

type CompletedSplit struct {
	ID     SplitID
	Cursor Cursor // final cursor, e.g. the HIGH watermark of a chunk
	Records uint64
}
```

### 4.1 Catalog and configured catalog

```go
// Catalog is what a source discovered. It is persisted verbatim, so drift is a
// diff against the stored copy rather than a per-record surprise in every sink.
type Catalog struct {
	Streams []Stream
	// Complete is false when discovery was truncated (a source with a million
	// objects). The UI must say "partial" rather than imply completeness.
	Complete bool
	At       time.Time
}

type Stream struct {
	ID   StreamID
	Namespace string // optional grouping label; a name, not a database
	SchemaID SchemaID

	// SupportedModes is what this source can do for this stream. The operator
	// chooses from it; the core validates the choice against it.
	SupportedModes []SourceMode

	// Cursors and Keys are lists of field paths, so composite and nested keys
	// need no later breaking change.
	CursorFields [][]string
	KeyFields    [][]string

	// SourceDefinedCursor and SourceDefinedKey let a connector constrain
	// operator choice in data rather than in code.
	SourceDefinedCursor bool
	SourceDefinedKey    bool

	Bounded   bool // this stream terminates
	Chunkable bool // declared; must be backed by the Chunker interface
	Estimate  *Estimate
	Docs      string
}

// SourceMode and SinkMode are orthogonal enums. The source must never learn
// whether the sink overwrites, appends or dedups — that orthogonality is
// exactly what makes M x N connector combinations free.
type SourceMode uint8

const (
	SourceFullRefresh SourceMode = 1 // bounded scan, every run
	SourceIncremental SourceMode = 2 // cursor-based
	SourceBackfillThenIncremental SourceMode = 3
	SourceChangeStream SourceMode = 4 // stream only, no backfill
)

type SinkMode uint8

const (
	SinkAppend    SinkMode = 1
	SinkUpsert    SinkMode = 2
	SinkOverwrite SinkMode = 3
	SinkSoftDelete SinkMode = 4
)

// ConfiguredCatalog embeds what was discovered and adds what was chosen, so the
// two can be diffed and audited. Two types, never one.
type ConfiguredCatalog struct {
	Discovered Catalog
	Streams    []ConfiguredStream
}

type ConfiguredStream struct {
	ID StreamID
	Selected bool
	SourceMode SourceMode
	SinkMode   SinkMode
	CursorField [][]string // chosen, or the source-defined one
	KeyField    [][]string
	SelectedFields [][]string // empty = all
	DestName    string        // rename, applied by the engine not the sink
	DriftPolicy DriftPolicy
}
```

### 4.2 Schema and drift

```go
// Schema is canal's small closed type system. It is rendered to JSON Schema for
// the UI and to a codec's own type system by that codec, but it is neither of
// those. Airbyte's choice of an untyped JSON blob deferred the type problem to
// dbt-based normalisation, which became the worst part of that product and was
// eventually deleted.
type Schema struct {
	ID     SchemaID // content-addressed hash; identical schemas share an id
	Fields []SchemaField
	// Open declares that records may carry fields not listed. A schemaless
	// source sets Open with no fields.
	Open bool
}

type SchemaField struct {
	Path     []string
	Kind     ValueKind
	Nullable bool
	// Precision/Scale for Decimal; Length for String/Bytes; TimeUnit and
	// Timezone for temporal kinds. Explicit, not a naming convention.
	Precision, Scale, Length int32
	TimeUnit TimeUnit
	Fields   []SchemaField // Struct/List/Map element schemas
	Docs     string
	Default  *Value
}

type ValueKind uint8

const (
	KindNull ValueKind = iota
	KindBool
	KindInt64
	KindUint64
	KindFloat64
	KindDecimal
	KindString
	KindBytes
	KindDate
	KindTime
	KindTimestamp
	KindInterval
	KindStruct
	KindList
	KindMap
	KindJSON // deliberately last: an escape hatch, and a countable one
)

// SchemaChange is an ordered in-band event, not a metric and not a log line.
// The engine quiesces and flushes the affected streams before forwarding it to
// a sink, and commits the resulting SchemaEpoch atomically with position.
type SchemaChange struct {
	Stream StreamID
	Epoch  uint64
	Kind   SchemaChangeKind
	From   SchemaID
	To     SchemaID
	Detail []FieldDelta
	At     time.Time
}

type SchemaChangeKind uint8

const (
	SchemaAddField SchemaChangeKind = iota
	SchemaDropField
	SchemaWidenType
	SchemaNarrowType
	SchemaRenameField
	SchemaChangeNullability
	SchemaChangeKey
	SchemaAddStream
	SchemaDropStream
	SchemaTruncateStream
	SchemaReplaceStream
)

// DriftPolicy is core config, per stream, with lenient as the never-destructive
// default. Five modes wholesale from Flink CDC — the only complete shipped
// drift answer. It stops an unanswerable question landing on every sink.
type DriftPolicy uint8

const (
	DriftLenient    DriftPolicy = 0 // apply additive changes, ignore destructive
	DriftEvolve     DriftPolicy = 1 // apply everything the sink supports
	DriftTryEvolve  DriftPolicy = 2 // apply what works, log and continue otherwise
	DriftIgnore     DriftPolicy = 3 // never touch the destination
	DriftError      DriftPolicy = 4 // stop the pipeline
)
```

---

## 5. The connector surface

Everything so far is data. This section is the **in-process binding**: idiomatic Go interfaces that map
1:1 onto the frames above. A connector author reads only this section and `proto`.

### 5.1 Source

```go
// Package connector is what a connector author imports. It imports proto and
// nothing else, so a connector cannot compile against canal's engine.
package connector

// Source is the required source surface. Four methods. Everything else is an
// optional interface (§5.4) whose fact is declared in the descriptor (§7).
//
// Config arrives pre-parsed, pre-validated and pre-defaulted in the factory
// (§7), so there is no Configure callback and no map re-parsed inside the
// connector. Restored state arrives the same way.
type Source interface {
	// Streams is discovery. It is required — not optional — because a persisted
	// catalog is what turns drift into a diff and gives the UI a stream picker
	// with zero connector-specific code. A source with no catalog (a webhook, a
	// socket, a metrics scrape) returns connector.SingleStream(name, schema),
	// which is one line, so the cost of requiring it is one line.
	Streams(ctx context.Context) (proto.Catalog, error)

	// Enumerator plans how work divides. It is called on exactly one process:
	// the planner. It never learns where a split will run.
	Enumerator(ctx context.Context, cc proto.ConfiguredCatalog) (Enumerator, error)

	// Reader is called on every worker that will read. It never learns how the
	// plan was computed. This is the separation Kafka Connect (taskConfigs +
	// assignor) and Flink (SplitEnumerator + scheduler) invented independently,
	// and it is the one near-axiom in canal's research that both major
	// architectures agree on.
	Reader(ctx context.Context, cc proto.ConfiguredCatalog) (Reader, error)

	// Close releases resources. It is always called exactly once, including
	// after a failed Streams, Enumerator or Reader, and including on
	// cancellation. Kafka Connect's KIP-419 — a safe teardown callback — has
	// been unfixed for seven years because this was not true from the start.
	Close(ctx context.Context) error
}

// Enumerator plans splits. It is a frame handler: every method is a reaction to
// an inbound frame and every output goes through Emitter, which the transport
// binding supplies. There is no Run loop the connector owns, because a loop
// cannot cross a process boundary but a sequence of frame handlers can.
//
// The engine calls these methods from a single goroutine. An enumerator that
// wants concurrency spawns its own and emits from it; Emitter is safe for
// concurrent use.
type Enumerator interface {
	// Start is called once, after any state restore. It typically emits the
	// initial split set, or begins discovering it.
	Start(ctx context.Context, e Emitter) error

	// ReaderReady means a reader has spare capacity. Assignment is pull-based:
	// the enumerator hands out splits in response to demand, which gives load
	// balancing with no load model and kills rebalance storms. The push
	// alternative — Kafka Connect's one-shot N task configs at connector start —
	// makes "snapshot with 20 workers then stream with 2" inexpressible, and
	// left tasks.max advisory for eight years.
	ReaderReady(ctx context.Context, e Emitter, r ReaderID, want int) error

	// SplitsCompleted reports bounded splits that reached End. The enumerator
	// decides what that means: hand out the next chunk, declare NoMoreSplits, or
	// promote the stream split from PhaseBackfilling to PhaseStreaming.
	SplitsCompleted(ctx context.Context, e Emitter, done []proto.CompletedSplit) error

	// ReaderLost returns a lost worker's unfinished splits to the planner, with
	// the last durable cursor for each. Unfinished work returning to the planner
	// is the property that makes worker loss a reassignment rather than a
	// pipeline restart.
	ReaderLost(ctx context.Context, e Emitter, r ReaderID, back []proto.SplitState) error

	// Snapshot returns the enumerator's own state for checkpoint m. It must be a
	// pure read: the engine may call it while readers are mid-fetch.
	Snapshot(ctx context.Context, m proto.MarkID) (proto.Blob, error)

	// Confirmed implements the Checkpoint Subsuming Contract on the planner side:
	// checkpoint m is durable, so state needed only to reconstruct m' < m may be
	// released. It may be skipped entirely; never block waiting for it.
	Confirmed(ctx context.Context, m proto.MarkID) error

	Close(ctx context.Context) error
}

// Emitter is the enumerator's output channel. It is a handle, supplied by the
// binding — in-process a direct call into the engine's enumerator loop, over
// gRPC a stream send. Handles as parameters are fine; handles inside frames are
// not (rule P2).
type Emitter interface {
	Send(ctx context.Context, f proto.Frame) error
	// Assign is the common case, spelled out.
	Assign(ctx context.Context, r ReaderID, splits []proto.Split) error
	NoMoreSplits(ctx context.Context, s proto.StreamID) error
	Estimate(ctx context.Context, e proto.Estimate, scope proto.StreamID) error
	Fault(ctx context.Context, f proto.Fault) error
}

// Reader reads assigned splits. It is the hot path, and it is the one place in
// canal where the shape is chosen for performance rather than for symmetry.
type Reader interface {
	// AddSplits and RemoveSplits are the reader half of the assignment protocol.
	// A reader must accept splits at any time, including while a Fetch is
	// blocked — so a reader that buffers internally serialises these against its
	// own fetch loop, and the engine never calls them concurrently with Fetch.
	AddSplits(ctx context.Context, splits []proto.Split, refs []proto.SplitRef) error
	RemoveSplits(ctx context.Context, ids []proto.SplitID) error

	// Fetch fills b with frames and returns. It is the only blocking data-path
	// method and it takes a context, from the first commit.
	//
	// It returns nil with b possibly empty (no data yet — the engine will not
	// spin, because Fetch is expected to block until data, ctx expiry, or a
	// short internal deadline). It returns io.EOF only when every assigned split
	// is exhausted and the enumerator has declared NoMoreSplits for every
	// selected stream: that is the honest terminal condition a bounded pipeline
	// needs and that Kafka Connect never had.
	//
	// A reader emits its own Mark frames at boundaries it considers meaningful.
	// It must emit at least one Mark per split per MarkNow request.
	Fetch(ctx context.Context, b *proto.Batch) error

	// MarkNow asks for a Mark on every assigned split at the next safe boundary,
	// carrying id m. Best-effort: a reader already at a boundary marks
	// immediately, a reader mid-transaction marks after it. The engine calls it
	// when a sink requests a boundary, when a size or time policy fires, or on
	// graceful drain — so checkpoint granularity is negotiated between source
	// and sink instead of dictated by whichever one happened to be asked.
	MarkNow(ctx context.Context, m proto.MarkID) error

	Close(ctx context.Context) error
}

type ReaderID string
```

### 5.2 Sink

```go
// Sink is the required sink surface. Two methods.
//
// Returning nil from Write means every record in b that is not named in res is
// DURABLE. This is design-rule R4 expressed as a type rather than as prose in a
// connector RFC: there is no other way for a sink to say "accepted", so a sink
// cannot accidentally acknowledge a buffer.
type Sink interface {
	// Write consumes b. Frames in b are Records, Marks and SchemaChanges in
	// order. A sink that does not implement SchemaApplier never sees a
	// SchemaChange — the engine filters it and applies the drift policy itself.
	//
	// A sink that cannot write a record names it in res with a Fault. A sink
	// that recognises a record as already written names it Duplicate. A sink
	// with a staging concept names it Deferred and returns Committables.
	//
	// Marks in b are informational to a base-tier sink: the engine derives which
	// marks are now durable from the per-record outcomes and its own resolver,
	// so a base-tier sink needs zero progress awareness and therefore cannot get
	// it wrong.
	Write(ctx context.Context, b *proto.Batch, res *WriteResult) error

	Close(ctx context.Context) error
}

// WriteResult is how a sink reports exceptions. Silence means success, so the
// common path allocates nothing and writes nothing.
//
// Four outcomes, not Flink's six: Written (the default, unstated), Duplicate,
// Deferred, and Failed — because retryability, DLQ-ability and terminality are
// properties of the Fault's Class, and a second vocabulary for the same concept
// would violate R9.
type WriteResult struct{ /* per-record entries, keyed by RecordID */ }

// Failed reports a per-record failure. This is the field whose absence made two
// of canal's predecessor rules ("retry only the failed subset", "checkpoint only
// when no unresolved failures") literally unimplementable.
func (r *WriteResult) Failed(id proto.RecordID, f proto.Fault)

// Duplicate reports an idempotent no-op. It counts as durable for checkpoint
// purposes and increments canal_records_deduplicated_total, so an idempotent
// sink's retry is visible rather than indistinguishable from a fresh write.
func (r *WriteResult) Duplicate(id proto.RecordID)

// Deferred reports acceptance without durability. Legal only for a sink that
// also implements Committer; the engine rejects it otherwise, at the call site,
// with a Fault of class PermanentContract.
func (r *WriteResult) Deferred(id proto.RecordID)

// Committable stages a handle for the two-phase tier. It is stored inside the
// checkpoint and replayed to Commit after the checkpoint is durable.
func (r *WriteResult) Committable(c proto.Committable)

// RequestMark asks the engine for a checkpoint boundary. A sink that batches to
// a natural boundary (a file roll, a transaction limit, a target file size) says
// so here, and the engine calls Reader.MarkNow. Granularity is therefore
// negotiated rather than capped by a source that knows nothing about the sink.
func (r *WriteResult) RequestMark()

// Stats lets a sink report what it counted, which the engine reconciles against
// the Mark's own counts. Emitted != persisted becomes a flagged condition rather
// than a silent difference — one integer on each side of every checkpoint.
func (r *WriteResult) Stats(records, bytes uint64)
```

### 5.3 Optional interfaces (the growth path)

```go
// ---------- source-side ----------

// CursorComparer makes cursors totally ordered within a split. The engine needs
// it for the chunked-snapshot dedup filter, for position fractions, and for lag.
// Cap: proto.CapCompareCursor.
type CursorComparer interface {
	CompareCursor(a, b proto.Cursor) (int, error)
}

// Chunker splits one bounded split into many. The core owns the eight-step
// watermark algorithm; the connector's whole obligation is "give me ordered
// chunks by key".
//
// `from` is the chunker's own resumable cursor, checkpointed in enumerator
// state, so slicing a 500-million-row object resumes mid-slice instead of
// restarting. Flink CDC's finished-chunk set proved too large to ship in one
// message, so canal pages it by reference from the first commit.
// Cap: proto.CapChunk.
type Chunker interface {
	Chunks(ctx context.Context, s proto.Split, target ChunkTarget, from proto.Cursor) (
		chunks []proto.Split, next proto.Cursor, err error)
}

type ChunkTarget struct {
	Records uint64
	Bytes   uint64
	MaxChunks int
}

// Replayer declares that a bounded split can be re-read from an arbitrary
// cursor within it. Required for chunk-level resume and for the watermark
// protocol's replay step. Cap: proto.CapReplay.
type Replayer interface {
	CanReplay(s proto.Split) bool
}

// Headwatcher reports the current head of an unbounded stream. This is the one
// observability field Airbyte lacks and the reason it cannot show lag: a source
// has no way to say "I am four minutes behind the log head". Cap: proto.CapHead.
type Headwatcher interface {
	Head(ctx context.Context, s proto.StreamID) (proto.Cursor, time.Time, error)
}

// MarkConfirmer acts when a mark becomes durable: advance a server-side consumer
// group, delete a queue message, release a replication slot.
//
// Contract, verbatim from Flink's Checkpoint Subsuming Contract: mark ids
// strictly increase; a confirm of m subsumes every m' < m; a confirm may be
// SKIPPED ENTIRELY, so never wait for one and never treat its absence as
// failure. Cap: proto.CapMarkConfirm.
type MarkConfirmer interface {
	MarkConfirmed(ctx context.Context, m proto.MarkID) error
}

// StateMigrator answers "is this restored state still valid under the current
// config?" so the engine never guesses. Returning false with a reason is what
// turns Airbyte's silent reset-on-config-change into a visible, explained event.
// Cap: proto.CapStateMigrate.
type StateMigrator interface {
	MigrateState(ctx context.Context, b proto.Blob, from uint32) (proto.Blob, bool, string)
}

// ---------- sink-side ----------

// SchemaApplier receives ordered schema changes. A sink declares which kinds it
// can apply and nothing more; the drift policy decides what happens to the rest.
// Cap: proto.CapSchemaApply.
type SchemaApplier interface {
	SupportedChanges() []proto.SchemaChangeKind
	ApplySchema(ctx context.Context, sc proto.SchemaChange) error
	// PreviewSchema lets the UI show the operator what will change before it
	// happens, with zero core knowledge of the destination's DDL.
	PreviewSchema(ctx context.Context, sc proto.SchemaChange) ([]string, error)
}

// Committer is the two-phase tier: exactly-once for a generic destination with
// no Kafka-in-the-middle assumption. The engine calls Commit only after the
// checkpoint containing the committables is durable, and re-calls it on restart
// — so a lost confirmation is repaired by the next one. Cap: proto.CapCommit.
type Committer interface {
	Commit(ctx context.Context, cs []proto.Committable) ([]proto.CommitOutcome, error)
	// Abandon discards committables from a checkpoint that will never commit.
	Abandon(ctx context.Context, cs []proto.Committable) error
}

// TokenSink stores the engine's checkpoint token inside its own transaction, so
// "data landed but state didn't" is structurally impossible: one durability
// domain. Only works for transactional destinations, which is why it is a tier
// and not the contract. Cap: proto.CapToken.
type TokenSink interface {
	// LoadToken is called before the first Write; the engine resumes from the
	// returned token rather than from the CheckpointStore.
	LoadToken(ctx context.Context) (proto.Blob, error)
	// The token to store with the next Write's transaction is passed via
	// WriteResult's inbound side: proto.Batch carries it on the Mark frame.
}

// StructuredInput declares that this sink wants Payload.Structured, not bytes —
// the escape hatch for a sink built on a vendor SDK that takes a typed object.
// It bypasses the Serializer stage only; the framer and compressor still do not
// apply. Cap: proto.CapStructuredInput.
type StructuredInput interface {
	AcceptsStructured() bool
}

// BatchPolicy is connector-declared batching the framework enforces. The sink
// gets no goroutine and no timer — Benthos's goroutine-free Batcher shape.
// Cap: proto.CapBatchPolicy.
type BatchPolicy interface {
	Batching() proto.Batching
}

// ---------- either side ----------

// Validatable is tier two of validation: it may do I/O, it returns per-field
// diagnostics, and it returns ALL of them. Never fail-fast, never one bool and
// one string. Cap: proto.CapValidate.
type Validatable interface {
	Validate(ctx context.Context) []proto.Diagnostic
}

// NamedTests are the individually-runnable connection checks a setup form shows
// as a list of green and red lines. Cap: proto.CapNamedTests.
type NamedTests interface {
	Tests() []proto.TestSpec
	RunTest(ctx context.Context, name string) proto.TestResult
}

// ChoiceProvider resolves a Spec field's ChoicesFrom hook. Static declaration
// plus a named dynamic-fetch call is what satisfies both the UI and the
// out-of-process constraint; Kafka Connect's live Recommender callback is why
// its config surface cannot cross a process boundary. Cap: proto.CapDynamicChoices.
type ChoiceProvider interface {
	Choices(ctx context.Context, field string, partial proto.ConfigDoc) ([]proto.Choice, error)
}
```

### 5.4 Host: what the connector is given

```go
// Host is the connector's channel to the runtime. It is supplied in the factory
// (§7) and must not be stashed in a package variable.
//
// Every method maps onto an uplink frame, which is what makes it implementable
// by the gRPC binding as a client stub over the same session: Log -> KindLog,
// Counter/Gauge samples -> KindMetricSample, PatchConfig -> KindConfigPatch.
type Host interface {
	// Log is structured logging. The core adds pipeline, connector, stream and
	// split attributes; the connector adds semantics. Secrets in Meta are
	// redacted by the handler, not by the connector.
	Log() *slog.Logger

	// Counter and Gauge register connector-semantic instruments. The CORE owns
	// naming, tagging and export: the name given here becomes
	// canal_connector_<name>_total with the closed label set already applied. A
	// connector cannot name a canal metric and cannot add a label, so no
	// connector can blow up cardinality.
	Counter(name string, unit proto.Unit, help string) Counter
	Gauge(name string, unit proto.Unit, help string) Gauge

	// Estimate reports a work estimate. Only the connector can compute this
	// cheaply, which is why it is a connector obligation and throughput is not.
	Estimate(ctx context.Context, e proto.Estimate, scope proto.StreamID) error

	// StreamStatus reports the per-stream lifecycle machine. It is not a
	// substitute for the pipeline's health machine; they answer different
	// questions and partial success ("six of seven streams complete") is only
	// representable because both exist.
	StreamStatus(ctx context.Context, s proto.StreamID, st proto.StreamPhase, f *proto.Fault) error

	// PatchConfig durably persists a scoped config patch. Credential rotation is
	// unavoidable and a fire-and-forget message loses it; this returns an error
	// so a connector knows whether its new refresh token survived. It is a
	// patch at declared paths, never a whole new config blob.
	PatchConfig(ctx context.Context, patch proto.ConfigPatch) error

	// Secrets resolves a secret reference declared in the Spec. The connector
	// never sees the storage mechanism and the core never sees the plaintext in
	// a log or in the read model.
	Secrets() SecretResolver
}
```

---

## 6. Config self-description

This is the angle's biggest asset, so it gets the most care. The choice: **one explicit Go-declared
`Spec` value that is pure data, emits JSON Schema, and is bound to a struct by a declared mapping.**

Three rejected alternatives, each for a stated reason:

- **Struct tags + reflection** (Vector, dlt). No artefact to render. Required-ness and validation
  messages are discovered at run time. Presentation metadata gets smuggled into tag strings. It drives
  docs, not a live form. dlt is a cautionary data point here, not a model.
- **Connector-supplied raw JSON Schema** (Airbyte, Meltano). The UI must interpret arbitrary JSON
  Schema, which is a permanently incomplete job, and a connector can express things the form cannot
  render.
- **A framework-owned spec with live callbacks** (Kafka Connect's `Recommender`). A callback in the
  config surface is exactly why Connect's config cannot cross a process boundary.

canal's spec is a **closed, declarative tree** — strictly less expressive than JSON Schema, strictly
more expressive than `ConfigDef`, and 100% renderable because every node is one of a known set. JSON
Schema is an **output**, for off-the-shelf validators, not the source of truth.

```go
// Spec is a connector's complete config self-description. It is pure data: it
// crosses a wire, it renders in a browser, it validates without instantiating
// anything, and it is the single artefact behind validation, defaults, docs,
// JSON Schema, the live form and specialised sink UX.
type Spec struct {
	Fields []Field
	Groups []Group     // presentation grouping; ordering is Field.Order within Group
	Tests  []TestSpec  // named connection tests, shown as a list of results
	// Mappings is the sink-side record->field mapping surface (§6.3).
	Mappings []Mapping
}

type Field struct {
	Path string // dotted path. "auth.oauth.client_id"

	Type FieldType

	Title       string
	Description string
	// PatternDescription is the human sentence for a regex, because no operator
	// should be shown a regex as an error message.
	PatternDescription string
	Examples    []proto.Value
	Docs        string // URL

	Default *proto.Value

	// Required and Visible are PREDICATES over the rest of the config, not
	// booleans. "api_key required unless auth.mode == oauth" is the difference
	// between a real setup form and a flat property list.
	Required Predicate
	Visible  Predicate

	// Secret means the core redacts, encrypts and masks with zero connector
	// knowledge. The connector never handles the storage.
	Secret bool

	// Choices is a static enumeration. ChoicesFrom names a dynamic hook the core
	// resolves by calling ChoiceProvider.Choices — static declaration plus a
	// named fetch, which is the only shape that serves both the form and the
	// future process boundary.
	Choices     []Choice
	ChoicesFrom string

	Validators []Validator

	// Widget is a presentation hint from a closed set, so the frontend has a
	// finite switch and no unknown case.
	Widget Widget
	Order  int
	Group  string

	// Fields nests. Variants is a tagged union with a const discriminator — the
	// one thing JSON Schema does that ConfigDef provably cannot, and the reason
	// ConfigDef fakes nesting with dotted prefixes.
	Fields   []Field
	Variants []Variant

	// Component makes this field hold another registered component's config.
	// This is the SANCTIONED meta-component hole: Benthos punched an
	// undocumented one to let a component contain a component, and paid for it
	// with an unexported optional interface. Declaring it means fan-out, DLQ
	// routing, retry wrappers, buffers and transform chains are all just
	// component-valued config fields, with observability nesting for free.
	Component proto.ComponentKind // Source | Sink | Transform | Buffer | Codec | ""
	Repeated  bool                // []Component, e.g. a fan-out's branch list
}

type FieldType uint8

const (
	FieldString FieldType = iota
	FieldInt
	FieldFloat
	FieldBool
	FieldDuration   // parsed to time.Duration; rendered as a duration widget
	FieldSize       // bytes, with SI/IEC suffixes
	FieldEnum
	FieldObject
	FieldArray
	FieldOneOf
	FieldComponent
	FieldFieldPath  // [][]string — a field path picker, driven by the catalog
	FieldStreamRef  // a stream picker, driven by the catalog
)

// Predicate is a closed six-node AST over the config document. It is
// deliberately NOT an expression language: it is browser-evaluable in about
// forty lines of TypeScript, it has no grammar, no parser and no dependency,
// and it cannot express anything the form cannot render.
//
// This is the answer to the open question "whether to adopt an embedded
// expression language": no, not in v1. The closed AST covers every real
// conditional-required case in the prior art, and adopting a grammar later is
// additive (a seventh node kind) rather than breaking.
type Predicate struct {
	Kind PredicateKind
	Path string        // for Eq/NotEq/Present/Truthy
	Value *proto.Value // for Eq/NotEq/In
	In    []proto.Value
	Sub   []Predicate  // for And/Or/Not
}

type PredicateKind uint8

const (
	PredAlways PredicateKind = iota
	PredNever
	PredEq
	PredNotEq
	PredIn
	PredPresent
	PredAnd
	PredOr
	PredNot
)

// Validator is declarative. Every kind is checkable offline, in Go and in the
// browser, from the same data. This is tier one of two-tier validation; tier two
// is the optional Validatable interface, which may do I/O.
type Validator struct {
	Kind ValidatorKind
	Pattern string
	Min, Max *proto.Value
	MinLen, MaxLen *int
	Message string // overrides the default, for the human sentence
}

type ValidatorKind uint8

const (
	ValPattern ValidatorKind = iota
	ValRange
	ValLength
	ValOneOfChoices
	ValURL
	ValHostPort
	ValDuration
	ValJSONPointer
	ValFieldPath // must resolve against the discovered catalog
)

// Diagnostic is the per-field validation result. ALL failures are returned at
// once, attributed to their field, so a form can show every one of them. Do not
// copy Airbyte's check, which returns one bool and one string and is useless to
// a form.
type Diagnostic struct {
	Path     string // "" for a whole-config diagnostic
	Severity Severity // Error | Warning | Info
	Code     DiagCode
	// Message is user-facing. Detail is developer-facing. One string cannot
	// serve both audiences, and the UI needs the first.
	Message string
	Detail  string
	Hint    string // the next action, which design-rules requires of every surface
}

type DiagCode uint16

const (
	DiagRequired DiagCode = iota
	DiagType
	DiagPattern
	DiagRange
	DiagLength
	DiagChoice
	DiagUnknownField // LintUnknown: an unknown key is a warning, not silence
	DiagConflict
	DiagUnreachable
	DiagConnectFailed
	DiagPermission
	DiagCapabilityMissing
	DiagCatalogMismatch
)
```

### 6.1 Binding: no accessor ladder, no reflection magic

```go
// Bind declares, once, how spec paths map onto a Go struct. It is the answer to
// Benthos's biggest ergonomic tax — a (T, error) accessor ladder repeated in
// every connector — without paying dlt's price of having no artefact at all.
//
// Registration cross-checks the binding against the spec in both directions:
// every spec field must be bound and every bound field must exist in the spec,
// or Register panics at init. Drift between the struct and the schema is
// therefore structurally impossible rather than prevented by discipline (R8).
type Binder struct{ /* path -> setter */ }

func (b *Binder) String(path string, dst *string) *Binder
func (b *Binder) Int(path string, dst *int64) *Binder
func (b *Binder) Duration(path string, dst *time.Duration) *Binder
func (b *Binder) Secret(path string, dst *SecretRef) *Binder
func (b *Binder) FieldPaths(path string, dst *[][]string) *Binder
func (b *Binder) Component(path string, dst *proto.ComponentRef) *Binder
func (b *Binder) OneOf(path string, variants map[string]func(*Binder)) *Binder
func (b *Binder) Nested(path string, sub func(*Binder)) *Binder
```

A connector author therefore writes, in full:

```go
type conf struct {
	URI      string
	Timeout  time.Duration
	Token    connector.SecretRef
}

var spec = connector.NewSpec().
	Field(connector.Str("uri").Title("Endpoint").Required(connector.Always()).
		Validate(connector.URL())).
	Field(connector.Dur("timeout").Default(30 * time.Second)).
	Field(connector.Str("token").Secret().Required(connector.WhenEq("auth.mode", "token"))).
	Include(connector.TLS("tls")).      // composite: reusable, identical everywhere
	Include(connector.Retry("retry")).  // composite: same knobs in every connector
	Test("connect", "Reach the endpoint").
	Bind(func(b *connector.Binder, c *conf) {
		b.String("uri", &c.URI).Duration("timeout", &c.Timeout).Secret("token", &c.Token)
	})
```

`connector.TLS`, `connector.Retry`, `connector.Batching`, `connector.Proxy`, `connector.Backoff` are
**composite field specs paired with extractors**, so retry, backoff, batching and TLS are configured
identically in every connector with zero coordination — Benthos's best config idea, kept.

### 6.2 JSON Schema is an output

```go
// JSONSchema renders the spec as a JSON Schema document with x-canal-*
// annotations for presentation, predicates and mapping directives. It is for
// off-the-shelf validators, editor completion and CI diffing. It is NOT the
// source of truth and the frontend does not consume it — the frontend consumes
// the Spec, whose every node it can render, which is the difference between a
// form generator that works and one that is permanently incomplete.
func (s Spec) JSONSchema() []byte

// Hash is the SpecHash stored in every checkpoint header.
func (s Spec) Hash() string
```

### 6.3 Sink field mappings: specialised sink UX with zero core changes

```go
// Mapping is a sink-declared field with a default extraction directive over
// canal's generic Record. The sink says "I want a field called `user_id`, and by
// default get it from Value.user.id, falling back to Key". The UI renders that
// as an editable mapping form. The core knows only "fields" and "directives".
//
// This is Segment's action-destinations model, and it is the complete answer to
// "how does a generic UI configure a specialised sink without the core knowing
// anything about it" — which is constraint #1 and the frontend goal at once.
// Adding a sink with a rich bespoke form is: declare Mappings, implement Write,
// register. No core change, no frontend change.
type Mapping struct {
	Name string
	Title, Description string
	Type FieldType
	Required Predicate
	Default Directive
	Multiple bool
}

// Directive is a closed seven-node extraction AST over a Record. Same reasoning
// as Predicate: browser-evaluable, no grammar, no dependency, additive growth.
type Directive struct {
	Kind DirectiveKind
	Path []string      // for DirValuePath / DirKeyPath / DirMetaPath
	Lit  *proto.Value  // for DirLiteral
	Sub  []Directive   // for DirCoalesce / DirTemplate
	Text string        // for DirTemplate literal segments
}

type DirectiveKind uint8

const (
	DirValuePath DirectiveKind = iota // Value.a.b.c
	DirKeyPath                        // Key.a.b
	DirMetaPath                       // Meta["x"]
	DirLiteral
	DirIngestTime                     // Origin.IngestAt
	DirEventTime
	DirCoalesce                       // first non-null of Sub
	DirTemplate                       // concat of Text and Sub
	DirOp                             // Change.Op as a string
	DirStream                         // Origin.Stream
)

// Resolve is in engine/, is generic, and is the only code that reads a
// Directive. A sink never evaluates one.
func Resolve(d Directive, r *proto.Record) (proto.Value, error)
```

---

## 7. Registry, descriptors, capabilities, negotiation

```go
// SourceDescriptor is a registration entry. It has a deliberate split: an
// embedded SourceInfo that is pure wire-safe data (what the registry publishes,
// what the API serves, what the UI renders, what a gRPC handshake exchanges),
// and a local New factory that never crosses anything.
//
// Kafka Connect's descriptor contains live callbacks throughout, which is why
// its config surface cannot be shipped. Splitting the data half out costs one
// struct and buys the entire "publish a cached connector descriptor so the UI
// can list connectors and render forms without instantiating anything" property.
type SourceDescriptor struct {
	proto.SourceInfo
	New func(ctx context.Context, r OpenRequest) (Source, error)
}

type SinkDescriptor struct {
	proto.SinkInfo
	New func(ctx context.Context, r OpenRequest) (Sink, error)
}

// SourceInfo (in proto) is data only.
type SourceInfo struct {
	Name    string // registry key. lowercase, dotted: "postgres.cdc"
	Version string
	Title   string
	Summary string
	DocsURL string
	Support SupportLevel // Certified | Community | Experimental
	Icon    string       // data URI or well-known id

	Spec         Spec
	Capabilities Capabilities
	Protocol     uint32
	SpecHash     string
	Modes        []SourceMode // union over all streams; the per-stream truth is in the catalog
}

// OpenRequest is the whole constructor argument. Config arrives pre-parsed,
// pre-validated and pre-defaulted; state arrives pre-restored. There is no
// Configure callback and no map re-parsed inside the connector.
type OpenRequest struct {
	Config   proto.ConfigDoc // already validated against Spec, defaults applied
	Host     Host
	Restored *proto.Blob     // connector-authored state, or nil on a fresh start
	Catalog  *proto.ConfiguredCatalog
	Pipeline proto.PipelineID
	Worker   proto.WorkerID
}

// Capabilities is the declared fact. Declaring a capability whose interface the
// type does not satisfy PANICS at Register, so declaration and implementation
// cannot drift. This is P4: data crosses a wire, type assertions do not, and
// having both with an enforced cross-check is the only way to keep required
// interfaces tiny and frozen while still being wire-ready.
type Capabilities struct{ set map[Capability]struct{} }

func (c Capabilities) Has(x Capability) bool
func (c Capabilities) List() []Capability
func Caps(xs ...Capability) Capabilities

type Capability string

const (
	CapChunk           Capability = "chunk"
	CapReplay          Capability = "replay"
	CapCompareCursor   Capability = "compare_cursor"
	CapHead            Capability = "head"
	CapMarkConfirm     Capability = "mark_confirm"
	CapStateMigrate    Capability = "state_migrate"
	CapSchemaApply     Capability = "schema_apply"
	CapCommit          Capability = "commit"
	CapToken           Capability = "token"
	CapStructuredInput Capability = "structured_input"
	CapBatchPolicy     Capability = "batch_policy"
	CapValidate        Capability = "validate"
	CapNamedTests      Capability = "named_tests"
	CapDynamicChoices  Capability = "dynamic_choices"
	CapConfigPatch     Capability = "config_patch"
)

// Registry is a VALUE type with Clone/With/Without, so tests and sandboxes get
// an isolated instance and the package-level Register mutates a default
// instance. Module-scope global state that tests must work around is a named
// mistake in canal's own design rules.
type Registry struct{ /* immutable maps, copied on With */ }

func (r Registry) WithSource(d SourceDescriptor) Registry
func (r Registry) WithSink(d SinkDescriptor) Registry
func (r Registry) WithTransform(d TransformDescriptor) Registry
func (r Registry) WithCodec(d CodecDescriptor) Registry
func (r Registry) Without(kind proto.ComponentKind, name string) Registry
func (r Registry) Source(name string) (SourceDescriptor, bool)
func (r Registry) Sources() []proto.SourceInfo // data only; for the API
func (r Registry) Clone() Registry

// Default is the init-time registry. Register panics on a duplicate name, on a
// declared-but-unimplemented capability, on a spec/binding mismatch, and on a
// protocol version this build cannot speak — all at init, so a broken connector
// fails at process start, never at 3am.
func Register(d any)
func Default() Registry
```

### 7.1 Negotiation: an impossible pipeline is refused at submit time

```go
// Negotiate is a PURE FUNCTION of declared data, callable before anything
// starts. It computes the delivery guarantee as min(source, sink, requested) and
// returns diagnostics for everything that does not line up.
//
// This is what closes Vector's most dangerous silent degradation — an ack
// negotiation that quietly downgrades — and it turns design-rule R4 from prose
// into a value the API returns to the operator with the submit button greyed out.
func Negotiate(src proto.SourceInfo, snk proto.SinkInfo, p proto.PipelineSpec,
	cat proto.ConfiguredCatalog) (proto.Plan, []proto.Diagnostic)

type Plan struct {
	Guarantee Guarantee // AtMostOnce | AtLeastOnce | EffectivelyOnce | ExactlyOnce
	// Why explains the guarantee in one sentence per contributing factor, so the
	// UI can say "at-least-once because sink `http` declares no commit tier"
	// rather than showing a badge with no explanation.
	Why []string

	// Chunked says the core's chunked-snapshot engine is active, which requires
	// CapChunk AND CapReplay AND CapCompareCursor on the source. The engine
	// derives a policy from a capability CONJUNCTION, which is exactly what
	// database/sql does to decide whether a connection survives a rollback.
	Chunked bool
	// Lag says a source-side lag metric will exist (requires CapHead +
	// CapCompareCursor). If false, the read model reports lag as UNKNOWN — never
	// as zero.
	Lag bool
	DriftPolicies map[proto.StreamID]proto.DriftPolicy
	Batching proto.Batching
	Buffers  []proto.BufferPlan
	MarkPolicy proto.MarkPolicy
}
```

---

## 8. Errors

```go
// Class is canal's closed error taxonomy: the seven ownership classes from
// design-rules, plus not-connected and end-of-input. A CLOSED set is what makes
// the class a legitimate bounded metric label and what makes "a hint the
// framework ignores" impossible — Benthos's ErrBackOff is honoured only on
// Connect, so a connector can return a hint the framework discards.
type Class uint8

const (
	ClassUnknown Class = 0

	ClassTransientUpstream Class = 1 // their system, temporarily: 503, timeout, throttle
	ClassTransientInternal Class = 2 // our system, temporarily: buffer full, lease lost
	ClassPermanentUpstream Class = 3 // their system, durably: 404, dropped table, revoked grant
	ClassPermanentMapping  Class = 4 // this record cannot be represented: bad UTF-8, overflow
	ClassPermanentContract Class = 5 // our config or code is wrong: auth invalid, capability missing
	ClassDuplicate         Class = 6 // idempotent success; not a failure
	ClassClockSkew         Class = 7 // a timestamp outside the accepted window

	ClassNotConnected Class = 8 // no session; reconnect, do not retry the record
	ClassEndOfInput   Class = 9 // this split is exhausted; not a failure
)

// Disposition is what happens to the work, derived from the Class by a single
// table in engine/ that no connector can override. There is no per-connector
// retry semantics to get wrong.
type Disposition uint8

const (
	RetainAndBackoff Disposition = iota // keep progress, back off, stay visible as degraded
	DeadLetter                          // route the record, advance the prefix
	Terminal                            // stop the pipeline, freeze the prefix
	Succeed                             // count it and move on (Duplicate, EndOfInput)
)

// Fault is the classified error value. It implements error, and it is data, so
// it is a frame, so it survives a process boundary with its classification
// intact — which a Go error type assertion does not.
type Fault struct {
	Class Class

	// Message is user-facing and appears in the read model. Detail is
	// developer-facing and appears in logs. Stack is developer-only.
	Message string
	Detail  string
	Stack   string

	// Attribution. Stream lets partial success be representable: six of seven
	// streams complete, one faulted, the six committed.
	Stream StreamID
	Split  SplitID
	Record RecordID
	Stage  Stage // Read | Transform | Encode | Buffer | Write | Commit | Checkpoint

	// RetryAfter honours a server's Retry-After rather than guessing.
	RetryAfter time.Duration
	At         time.Time
	// Cause is a wrapped Fault, for a chain. Not a Go error: a Fault, so the
	// chain is data.
	Cause *Fault
}

func (f *Fault) Error() string
func (f *Fault) Is(target error) bool

// Classify wraps a plain Go error with a class. This is the connector's ENTIRE
// error obligation: one function call at the point of raise. Vector reduces the
// whole connector obligation to one Classify function driving both retry and
// acks, and that economy is worth copying.
func Classify(c Class, err error, msg string) *Fault

// RetryPolicy is {maxAttempts, backoff, terminal disposition}. MaxAttempts is
// ALWAYS finite — Benthos livelocks on a poison record because unbounded retry
// was the default, and its indefinite block was patched with a lint before the
// default was flipped in a major version.
type RetryPolicy struct {
	MaxAttempts int           // >= 1, no zero-means-infinite trap
	Initial     time.Duration
	Max         time.Duration
	Multiplier  float64
	Jitter      JitterKind // FullJitter is the default everywhere
	Terminal    Disposition
}

// DeadLetter works for sources too — an undecodable frame from an unframer, a
// record that fails the configured catalog's schema, a cursor that will not
// parse. Kafka Connect's per-record reporter is sink-only, which is why source
// decode failures there are a log line and a lost record.
type DeadLetter interface {
	Send(ctx context.Context, r *proto.Record, f *proto.Fault) error
}
```

---

## 9. Codecs, buffers, transforms, engine assembly

```go
// Serialisation is three independently registered stages, and connectors
// implement TRANSPORT ONLY. This is the property that makes "add a sink: two
// methods, register, done" literally true, and it is constraint #4 applied to
// codecs. N codecs x M connectors never multiplies; framing is genuinely
// orthogonal to encoding; a new transport gets every parser for free.
type Serializer interface {
	Encode(ctx context.Context, r *proto.Record, dst *[]byte) error
	ContentType() string
}

type Deserializer interface {
	Decode(ctx context.Context, src []byte, r *proto.Record) error
}

// Framer delimits records within a byte stream on the sink side.
type Framer interface {
	Frame(dst *[]byte, payload []byte) error
	Terminator() []byte
}

// Unframer is the source side, and its signature says "one frame to many
// records" — which is what makes a source transport symmetric with a sink
// transport. The N records produced from one inbound frame share
// Origin.Parent, so their acks aggregate to one upstream ack automatically:
// Benthos's ack-aggregating scanner reduction, made structural by the parent id
// rather than by positional correlation.
type Unframer interface {
	Scan(ctx context.Context, src io.Reader, b *proto.Batch, split proto.SplitRef) error
}

type Compressor interface {
	Compress(dst *[]byte, src []byte) error
	Decompress(dst *[]byte, src []byte) error
}

// Buffer is ONE interface. Bounded by construction: there is no way to express
// unbounded growth. Push returns how many frames it accepted, so partial
// acceptance and rejection are in the signature — design-rule R6 requires a
// rejection path be expressible, and Kafka Connect's void put() cannot express
// one.
type Buffer interface {
	Push(ctx context.Context, b *proto.Batch) (accepted int, err error)
	Pop(ctx context.Context, b *proto.Batch) error
	Depth() (records, bytes uint64)
	Capacity() (records, bytes uint64)
	Close(ctx context.Context) error
}

// WhenFull is in the type, not in prose. Vector's insight, kept.
type WhenFull uint8

const (
	FullBlock    WhenFull = iota // backpressure, transitively
	FullReject                   // return accepted < len, count the refusal
	FullOverflow                 // spill to the next buffer tier (memory -> disk)
	FullDropNewest               // preserve the acked prefix; drops are COUNTED
)

// Transform. One interface, full return-type vocabulary through the out batch:
// 1-to-0 is Drop, 1-to-1 is Derive once, 1-to-N is Derive many, N-to-1 is Derive
// once from many, and regroup falls out because the out batch is the unit.
// Windowing and async lookups are expressible because Apply may block on ctx.
type Transform interface {
	Apply(ctx context.Context, in *proto.Batch, out *proto.Batch) error
	Close(ctx context.Context) error
}

// The engine's outer shape is a fixed IMPLEMENTATION FACT and never an
// enumerated stage list in any API — design-rule R1, whose violation (eight
// stages frozen into an OpenAPI schema with minItems 8, maxItems 8) is the
// reason R1 exists. All variety comes from components containing components via
// Field.Component, so fan-out, fan-in, routing, fallback, retry wrappers and DLQ
// cost the core nothing.
type Pipeline struct{ /* engine-internal */ }

func Build(ctx context.Context, reg connector.Registry, spec proto.PipelineSpec,
	rt runtime.Runtime) (*Pipeline, []proto.Diagnostic, error)

func (p *Pipeline) Run(ctx context.Context) error
// Drain is graceful stop: finish at the next mark, commit, then exit. It is
// DISTINCT from ctx cancellation, which is abort. A process boundary gives you
// this distinction for free (SIGTERM vs SIGKILL); an in-process design must
// build it, and Kafka Connect's failure to do so is KIP-419, unfixed for seven
// years.
func (p *Pipeline) Drain(ctx context.Context) error
func (p *Pipeline) Status() runtime.PipelineStatus
```

### 9.1 The one type assertion

```go
// probe is the ONLY function in canal that type-asserts a connector. It runs
// once, at pipeline build, and materialises a plain owned struct — the
// database/sql rowsColumnInfoSetupConnLocked pattern. Every "the connector did
// not say" case is a second return value, never a zero value and never a panic.
//
// One function means: adding a capability touches one file; a gRPC binding
// implements the capability set from declared data with no assertions at all;
// and Benthos's nine hand-written forwarders per capability do not exist.
func probe(src connector.Source, info proto.SourceInfo) sourceCaps {
	var c sourceCaps
	if v, ok := src.(connector.Chunker); ok { c.chunker, c.hasChunker = v, true }
	if v, ok := src.(connector.CursorComparer); ok { c.cmp, c.hasCmp = v, true }
	if v, ok := src.(connector.Headwatcher); ok { c.head, c.hasHead = v, true }
	// ... one line per capability, and a cross-check against info.Capabilities
	// that panics on a mismatch, because Register already guaranteed agreement.
	return c
}
```

---

## 10. The deployment seam

```go
// Package runtime is the standalone/enterprise seam: FOUR interfaces and
// nothing else. A fifth means the abstraction is wrong.
package runtime

type Runtime struct {
	Config      ConfigStore
	Checkpoints CheckpointStore
	Coordinator Coordinator
	Status      StatusAggregator
}

// CheckpointStore is deliberately bytes-in, bytes-out. The durability substrate
// never sees a domain type, which is exactly what makes the standalone/
// distributed swap free — Kafka Connect's single best idea.
type CheckpointStore interface {
	Get(ctx context.Context, keys [][]byte) (map[string][]byte, error)
	// Set MUST be atomic across the whole map. This is the requirement Kafka
	// Connect's compacted-topic store cannot meet, and its own javadoc documents
	// the resulting unrecoverable compaction-plus-partial-write state with "no
	// obvious way to resolve the issue". Every backend must satisfy it with one
	// transaction: one SQL txn, one bbolt txn, one etcd Txn.
	Set(ctx context.Context, kv map[string][]byte) error
	Delete(ctx context.Context, keys [][]byte) error
	Keys(ctx context.Context, prefix []byte) ([][]byte, error)
}

type ConfigStore interface {
	Get(ctx context.Context, id proto.PipelineID) (proto.PipelineSpec, Revision, error)
	// Put is revisioned CAS. A config change that silently did not apply is the
	// failure generation/observedGeneration exists to make into an alert.
	Put(ctx context.Context, s proto.PipelineSpec, expect Revision) (Revision, error)
	List(ctx context.Context) ([]proto.PipelineID, error)
	Watch(ctx context.Context, from Revision) (<-chan ConfigEvent, error)
	Delete(ctx context.Context, id proto.PipelineID, expect Revision) error
}

type Coordinator interface {
	Join(ctx context.Context, w proto.WorkerInfo) (Membership, error)
	// Campaign is for PLANNING ONLY. Data flow must never depend on it: the
	// verified Kubernetes caveat is that leader election does not guarantee
	// fencing, so leadership can never be trusted for correctness.
	Campaign(ctx context.Context) (Leadership, error)
	// Claim/Renew/Release: the assignment LEASE is the fencing token. The plan is
	// durable state, not a leader's in-memory result, so the data plane keeps
	// flowing and checkpointing with the entire control plane down.
	Claim(ctx context.Context, a AssignmentID, w proto.WorkerID, ttl time.Duration) (Lease, error)
	Renew(ctx context.Context, l Lease) (Lease, error)
	Release(ctx context.Context, l Lease) error
	Assignments(ctx context.Context, p proto.PipelineID) ([]AssignmentStatus, error)
}

type StatusAggregator interface {
	Report(ctx context.Context, w proto.WorkerID, s WorkerStatus) error
	Status(ctx context.Context, p proto.PipelineID, opt StatusOptions) (PipelineStatus, error)
	Watch(ctx context.Context, p proto.PipelineID, from uint64) (<-chan PipelineStatus, error)
}
```

| Interface | `canal run` (laptop) | `canal serve` (cluster) |
|---|---|---|
| `ConfigStore` | bbolt file, or `-f pipelines.yaml` projected in | Postgres (first), etcd, k8s CRD |
| `CheckpointStore` | bbolt (one `Set` = one txn) | Postgres table, etcd, object store + WAL |
| `Coordinator` | `singlenode{}`: always leader, all assignments local, leases no-op | Postgres leases table + advisory locks |
| `StatusAggregator` | direct in-process read | workers write status rows; API reads |

**Postgres first, not etcd or Kafka:** one dependency delivers revisioned CAS
(`UPDATE … WHERE revision = $2`), atomic multi-key `Set` (one transaction), leases (a table plus
`now()`), and leader election (an advisory lock) — and a compacted log as the control-plane state machine
is a documented unrecoverable-state generator.

---

## 11. The read model (what the frontend consumes)

One canonical struct, one source of truth, three serialisations: `GET /v1/pipelines/{id}/status`
(ETag), `…/status/watch` (SSE, `id:` = `Version`), `GET /metrics` (Prometheus). The CLI
`canal status --watch` is a client of the same SSE endpoint, which is the best integration test the
read model will ever have.

```go
type PipelineStatus struct {
	Pipeline           proto.PipelineID
	Generation         Revision // spec revision in the store
	ObservedGeneration Revision // revision actually running, computed from worker reports
	AsOf               time.Time
	Version            uint64 // monotonic; SSE cursor and ETag
	// Complete is false when the aggregator did not hear from every worker. A
	// status document that silently omits a worker's contribution is the
	// "endpoint answered != data arrived" failure at the API level.
	Complete bool

	Phase      Phase       // k8s-shaped
	Conditions []Condition // 6 types x 3 statuses: Ready, Progressing, Degraded,
	                       // Backpressured, SchemaDrift, CheckpointStale
	Progress   Progress
	Assignments []AssignmentStatus
	Streams    []StreamStatus // opt-in via ?streams=true; the unbounded part
	RecentEvents []Event      // bounded ring buffer
	LastError  *proto.Fault
	Plan       proto.Plan     // the negotiated guarantee AND its Why sentences
}

type Progress struct {
	CheckpointAt     *time.Time // nil => never committed
	CheckpointAge    *float64   // the primary alert
	RecordsRead      uint64
	RecordsWritten   uint64
	RecordsCommitted uint64
	RecordsInFlight  uint64
	EventTimeLag     *float64          // nil => no event time from this source
	CursorLag        *float64          // nil => source declares no CapHead
	Backlog          *Backlog          // nil => unknowable
	Snapshot         *SnapshotProgress // nil => not backfilling
	Throughput       Throughput
}
```

**Every unknown is nil, never a zero.** Pinned fixtures include a case where every optional field is
absent, and a test asserts the UI renders no zeros. Metric labels come from a hard-closed vocabulary
(`pipeline`, `stage`, `connector`, `class`, `reason`, `buffer`, `outcome`, `worker`, `phase`,
`type`, `status`, `op`) and nothing per-stream, per-key or per-message ever becomes a label — the
per-stream detail lives in `Streams`, which is paginated.

---

## 12. Walkthroughs

### (a) A trivial source and a trivial sink move one record

The complete source:

```go
type ticker struct{ n uint64 }

func (t *ticker) Streams(ctx context.Context) (proto.Catalog, error) {
	return connector.SingleStream("ticks", connector.OpenSchema()), nil
}
func (t *ticker) Enumerator(ctx context.Context, cc proto.ConfiguredCatalog) (connector.Enumerator, error) {
	return connector.OneUnboundedSplit("ticks"), nil // helper: emits one split, never completes
}
func (t *ticker) Reader(ctx context.Context, cc proto.ConfiguredCatalog) (connector.Reader, error) {
	return t, nil
}
func (t *ticker) AddSplits(ctx context.Context, s []proto.Split, refs []proto.SplitRef) error {
	t.ref = refs[0]; return nil
}
func (t *ticker) RemoveSplits(ctx context.Context, ids []proto.SplitID) error { return nil }
func (t *ticker) Fetch(ctx context.Context, b *proto.Batch) error {
	r := b.Record(t.ref)                       // slot lent, id and Origin stamped
	r.Value.SetStructured(map[string]proto.Value{"n": int64(t.n)})
	r.EventTime = time.Now()
	t.n++
	b.Mark(t.ref, proto.Cursor{Version: 1, Bytes: enc(t.n)})
	return nil
}
func (t *ticker) MarkNow(ctx context.Context, m proto.MarkID) error { return nil } // already at one
func (t *ticker) Close(ctx context.Context) error                   { return nil }
```

The complete sink:

```go
type stdoutSink struct{ w *bufio.Writer }

func (s *stdoutSink) Write(ctx context.Context, b *proto.Batch, res *connector.WriteResult) error {
	for _, f := range b.Frames() {
		if f.Kind != proto.KindRecord { continue }
		if _, err := s.w.Write(f.Record.Value.Raw()); err != nil {
			res.Failed(f.Record.ID(), *proto.Classify(proto.ClassTransientInternal, err, "stdout write failed"))
			return nil // per-record failure reported, not a batch failure
		}
	}
	return s.w.Flush() // nil return == durable, which is R4 as a type
}
func (s *stdoutSink) Close(ctx context.Context) error { return s.w.Flush() }
```

**The trace.** `canal run --source ticker --sink stdout`.

1. `cmd/canal` builds `runtime.Runtime` with `singlenode` (bbolt at `~/.canal/state.db`).
2. `engine.Build` looks up both descriptors in `connector.Default()`. It calls
   `Negotiate(tickerInfo, stdoutInfo, spec, cat)`: source declares no `CapCommit`-relevant tier, sink
   declares none, so `Plan.Guarantee = AtLeastOnce`, `Plan.Why = ["sink stdout declares no commit
   tier"]`, `Plan.Lag = false` (no `CapHead`) so the read model will report lag as **unknown**, not zero.
   Zero diagnostics, so build proceeds.
3. `probe(src, info)` type-asserts once. `ticker` satisfies no optional interface; every `has*` is false.
4. No checkpoint exists in the store, so `OpenRequest.Restored` is nil and `New` is called with the
   validated `ConfigDoc`.
5. `Enumerator.Start(ctx, e)` emits `SplitDelta{Assign: [{ID:{Stream:"ticks",Key:"0",Gen:0}, Bounded:false,
   Ordered:true}], Refs:[1]}`. The engine binds `SplitRef(1)` in the session's split table and calls
   `Reader.AddSplits`.
6. The reader loop calls `Fetch(ctx, b)` with a batch capped at `maxRecs=256`, `maxBytes=1MiB`. The
   connector appends one `Record` and one `Mark`. **No allocation** (slots come from the slab), **no
   marshalling** (the frame is a struct), **no channel** (the reader loop calls `Fetch` directly on its
   own goroutine).
7. The engine stamps nothing further — `Batch.Record` already set
   `id = {Split:1, Seq:0}` and `origin = {Connector:"ticker", Stream:"ticks", Split:{…}, IngestAt:now}`.
   `Resolver.admit` registers `{1,0}` as in-flight under `MarkID` 1.
8. The batch flows to the sink loop through a bounded buffer (`cap 2 batches`, `WhenFull=FullBlock`).
   Depth 1 of 2 shows up on `canal_buffer_depth`.
9. The engine applies the pipeline codec: `Serializer=ndjson` encodes `Value.structured` into
   `Value.bytes` (`stdoutSink` did not declare `CapStructuredInput`).
10. `Sink.Write(ctx, b, res)` writes and flushes and returns nil. `res` is empty.
11. The writer loop reads `res`: no `Failed`, no `Deferred`, so every record in the batch is durable.
    It calls `Resolver.Written({1,0}, 42)`.
12. `Resolver.Committed()` returns `marks=[1]`, `splits=[{Split:…, Cursor:{1,enc(1)}, Records:1}]`.
13. The engine writes ONE `CheckpointStore.Set` containing the checkpoint key, the per-split key and
    (in a dedup pipeline) dedupe keys — atomic across the map.
14. `Reader.MarkConfirmed` is skipped: `ticker` does not implement `MarkConfirmer`, and `probe` recorded
    that once so there is no per-batch assertion.
15. `kill -9`. Restart: `CheckpointStore.Get` returns the checkpoint, `SpecHash` matches, `Restored`
    contains the split state, the enumerator emits the split with `Start = {1, enc(1)}`, and the reader
    resumes at n=1. Exactly the R3 milestone.

**Count what the author wrote:** two required methods for the sink, four plus three reader methods for
the source, one `Spec`, one `Register`. No engine type. No switch statement anywhere in core.

### (b) Full initial scan, then incremental streaming, crashing halfway through the scan

A source declaring `Modes: [SourceBackfillThenIncremental]` and capabilities
`{CapChunk, CapReplay, CapCompareCursor, CapHead, CapMarkConfirm}` on a stream `orders` with 500M rows.

**Plan.** `Negotiate` sees the conjunction `CapChunk ∧ CapReplay ∧ CapCompareCursor` and sets
`Plan.Chunked = true`, activating the core's chunked-snapshot engine. `CapHead` sets `Plan.Lag = true`.

**Startup.**
1. `Enumerator.Start` emits, in this order:
   - the **unbounded stream split** `{Key:"stream", Bounded:false, Ordered:true}` with
     `Start = LOW`, where `LOW` is the head cursor obtained from `Headwatcher.Head` **before any
     backfill read**. The core owns this ordering — it is the handoff invariant, and it lives in
     `engine/enumloop.go`, not in the connector.
   - an `Estimate{Records:500e6, Exact:false}` for `orders`.
2. The core calls `Chunker.Chunks(ctx, ordersSplit, target{Records: 1e6}, Cursor{})`, gets back 64
   bounded chunk splits plus `next` (the chunker's own resume cursor after the 64th chunk). The core
   emits the 64 chunks as `SplitDelta.Assign` and stores `next` in **enumerator state** — so slicing
   itself is resumable. It does not slice all 500 chunks up front; the finished-chunk set is **paged by
   reference** from day one, so no message ever carries 500 ids.
3. Chunk splits get `Phase: PhaseBackfilling`. The stream split gets `PhaseStreaming`. Phase is
   reporting-only: the engine stores it in the mark and the header and **never branches on it**.

**Steady state.** The reader loop reads the stream split and up to `parallelism` chunk splits
concurrently. For each chunk the core runs the watermark protocol:

- read `LOW_c = Head()` → read the chunk to `End` → read `HIGH_c = Head()` → any stream record in
  `(LOW_c, HIGH_c]` whose key falls inside the chunk's key range **replaces** the snapshot row.
- The range filter is **indexed from day one** (a sorted key-range interval tree, binary searched),
  not the O(chunks)-per-record scan Flink CDC shipped and had to retrofit.
- When `Head()` advances past `HIGH_c`, the filter for chunk `c` **self-retires**, so steady-state cost
  is zero.

Marks flow normally: each chunk emits a `Mark` every `1e5` records with `Phase: PhaseBackfilling`; the
stream split emits marks with `PhaseStreaming`.

**Crash at 250M rows.** `kill -9` mid-chunk-31 with chunks 0..30 completed and chunk 31 at 62%.

The last durable checkpoint contains:
```
Header{Generation:7, MarkID:918, Phase:PhaseBackfilling, RecordsCommitted:250_113_402, SpecHash:"9c1…"}
Shared:     nil
Splits:     [ {stream, Cursor:LSN_9931, PhaseStreaming, Done:false},
              {chunk-31, Cursor:key_after_62pct, PhaseBackfilling, Done:false},
              {chunk-32..63, Cursor:zero, PhaseBackfilling, Done:false} ]
Enumerator: Blob{v1, {chunkerNext: key_64M, finishedChunksRef: "ck/7/finished/0000",
                      finishedCount: 31, retiredFilters: [0..24]}}
SchemaEpoch: 3
```

**Resume.**
1. `CheckpointStore.Get` → checkpoint. `SpecHash` matches, `Generation` matches; if the connector
   declared `CapStateMigrate` and the version differs, `MigrateState` decides — the core never guesses,
   and an invalid state produces a *visible, explained* reset event rather than Airbyte's silent one.
2. `New(ctx, OpenRequest{Restored: &Enumerator})` — pre-restored, in the constructor.
3. `Enumerator.Start` emits `SplitDelta.Assign` for chunk-31 **with `Start = key_after_62pct`** and for
   chunks 32..63 with zero starts. It does **not** re-slice: `chunkerNext` says where slicing stopped.
   Chunks 0..30 are not re-emitted; the paged finished set says they are done.
4. The stream split resumes at `LSN_9931`.
5. Chunk-31 replays from 62%, not from zero, and not from the start of the object. **250M rows are not
   re-read.** This is the trap the entire decision space warns about — Benthos still restarts a 500M-row
   snapshot from zero, and Airbyte needed a protocol change (`is_resumable`, `CheckpointMixin`) to fix
   it — and it is avoided here because backfill state is *split state in a typed header*, not something
   smuggled into an opaque blob.
6. The dedup filters for chunks 25..31 are rebuilt from split state; 0..24 stay retired.

**Handoff.** When `SplitsCompleted` reports chunk 63, the enumerator calls
`Chunker.Chunks(…, from: chunkerNext)`, gets an empty result, and emits `NoMoreSplits("orders")`. The
core retires the last filter, and the stream split — which has been running the whole time, at
`PhaseCatchingUp` until `Head() - Cursor` closed — flips its reported phase to `PhaseStreaming`.

**Nothing switched.** No enum was tested. The split set changed, and the split set is data. The
operator-facing model is Airbyte's per-stream `(SourceMode, SinkMode)` pair; the mechanism is
boundedness; phase is a label in a header.

### (c) A sink fails mid-batch and the pipeline recovers without loss

A batch of 500 records spanning marks 41 and 42, going to a sink that writes 100 at a time.

```go
func (s *bulkSink) Write(ctx context.Context, b *proto.Batch, res *connector.WriteResult) error {
	for _, chunk := range chunks(b.Frames(), 100) {
		resp, err := s.api.Bulk(ctx, chunk)
		if err != nil {
			// Whole-chunk transport failure: classify once, name every record.
			f := proto.Classify(proto.ClassTransientUpstream, err, "bulk endpoint unavailable")
			f.RetryAfter = retryAfter(err)
			for _, fr := range chunk { res.Failed(fr.Record.ID(), *f) }
			continue
		}
		for i, item := range resp.Items {
			switch {
			case item.OK:            // nothing: silence means Written
			case item.Duplicate:     res.Duplicate(chunk[i].Record.ID())
			case item.Invalid:       res.Failed(chunk[i].Record.ID(),
				*proto.Classify(proto.ClassPermanentMapping, nil, item.Reason))
			default:                 res.Failed(chunk[i].Record.ID(),
				*proto.Classify(proto.ClassTransientUpstream, nil, item.Reason))
			}
		}
	}
	res.Stats(uint64(written), uint64(bytes))
	return nil
}
```

**The trace.** Records 0..199 written. Record 200 is invalid (`PermanentMapping`). Records 201..299
succeed. Records 300..399 hit a 503 (`TransientUpstream`, `Retry-After: 5s`). Records 400..499 succeed.

1. `Write` returns nil. `res` names 1 `PermanentMapping` and 100 `TransientUpstream`.
2. The writer loop consults the **single disposition table**: `PermanentMapping → DeadLetter`,
   `TransientUpstream → RetainAndBackoff`.
3. Record 200 goes to the DLQ (`DeadLetter.Send`) with its full `Origin` — pre-transform stream, split
   and cursor, because provenance is immutable and `Batch.Derive` copied it. The DLQ write is durable
   before the prefix advances past it. `canal_records_dropped_total{reason="dlq"}` increments; the
   `intentional` label is false.
4. Records 300..399 are **retained**, not dropped. The writer loop schedules a retry with the
   `RetryPolicy` (`MaxAttempts: 8`, full jitter, honouring `RetryAfter: 5s`). It does **not** re-send
   records already `Written` or `Duplicate`, so "retry only the failed subset" is implementable — which
   in canal's predecessor it literally was not, because no per-record error array existed (R7).
5. `Resolver.Written` is called for 0..199, 201..299, 400..499. `Resolver.Failed(200, …, DeadLetter)`
   counts as durable. `Resolver.Committed()` returns **nothing**: mark 41 covers records 0..349, and
   300..349 are a hole. **The prefix does not advance past a hole.** This is why the resolver is in core
   and not in every connector.
6. `canal_oldest_committable_age_seconds` starts climbing. `Conditions` gains
   `{Type: Degraded, Status: True, Reason: "TransientUpstream", Message: "bulk endpoint unavailable"}`
   with `LastError` set — the visible `degraded` state design-rules requires of sustained backoff, not a
   silent stall.
7. Backpressure is transitive: the sink loop is not draining the buffer, the buffer reaches capacity 2,
   `Push` blocks (`FullBlock`), `Fetch` is not called, and the source stops reading. Bounded by
   construction, at every edge, with `canal_stage_blocked_seconds_total` explaining exactly where.
8. Retry 2, after 5.4s: the 100 records land. `Resolver.Written` for 300..399. `Committed()` now
   returns `marks=[41, 42]` — **41 and 42 together**, because the subsuming contract says a commit of
   42 subsumes 41, so one `CheckpointStore.Set` suffices and no intermediate write happened.
9. `Reader.MarkConfirmed(ctx, 42)` fires once (the source declares `CapMarkConfirm`), which advances
   the upstream replication slot past both marks at once.
10. `Conditions` clears `Degraded`. `Progress.RecordsCommitted` moves by 499, and
    `res.Stats` reconciles against the marks' own counts: 500 emitted, 499 persisted, 1 dead-lettered —
    **emitted == persisted + dropped**, asserted, not assumed.

**If instead attempt 8 had failed:** `RetryPolicy.Terminal` decides. `Terminal` freezes the prefix and
sets `Phase: Failed` with `LastError`; `DeadLetter` routes the 100 and advances. Either way it is
finite — the default is never unbounded, because Benthos livelocks on a poison record and had to flip
that default in a major version.

**No loss:** the only thing that advanced the durable cursor was a `Resolver.Committed()` derived from
a nil return from `Write`. There is no timer, no wall clock, and no other path to a checkpoint.

### (d) Serving the frontend with zero connector-specific code in core

**Listing connectors** — `GET /v1/connectors`. `api/` calls `registry.Sources()`, which returns
`[]proto.SourceInfo`: pure data, no instantiation, cached at init. The UI renders name, title, icon,
support level, docs URL and declared capabilities. Adding a connector adds a row. No core edit, no
frontend edit.

**Rendering the config form** — `GET /v1/connectors/postgres.cdc/spec` returns the `Spec` verbatim.
The frontend has one recursive renderer with a switch over `FieldType` (13 arms, closed) and
`Widget` (a closed set). It evaluates `Required` and `Visible` predicates client-side with a ~40-line
evaluator over the 9 `PredicateKind`s — so `"api_key required unless auth.mode == oauth"` works
**live, in the browser, with no round trip**, and there is no expression grammar to parse.

`Secret: true` fields render as password inputs and never round-trip their values.
`ChoicesFrom: "list_schemas"` renders as a select with a fetch button that calls
`POST /v1/connectors/postgres.cdc/choices/list_schemas` with the partial config; the core dispatches to
`ChoiceProvider.Choices`. Static declaration plus a named hook is why this survives being moved
out-of-process later.

**Validating** — `POST /v1/connectors/postgres.cdc/validate`. Tier one runs the declarative
`Validator`s offline. Tier two calls `Validatable.Validate` if declared. Both return `[]Diagnostic`,
**all of them**, each with `Path`, user `Message`, dev `Detail` and `Hint`. The form attaches each to its
field. `DiagUnknownField` surfaces typos as warnings instead of silently ignoring them.

**Named tests** — `POST /v1/connectors/postgres.cdc/tests` returns `[]TestResult`, one line per test
("Reach the host: ok", "Replication slot exists: failed — the role lacks REPLICATION"). Not one bool
and one string.

**Stream picker** — `POST /v1/pipelines/{id}/discover` calls `Source.Streams` and persists the
`Catalog`. The UI renders a tree with, per stream, a `SourceMode` select restricted to
`Stream.SupportedModes`, a `SinkMode` select restricted to the sink's declared modes, and cursor/key
pickers driven by `CursorFields`/`KeyFields`. `SourceDefinedCursor` disables the picker — the connector
constrained the operator in **data**. `Catalog.Complete = false` makes the UI say "partial".

**Specialised sink UX** — a sink's `Spec.Mappings` is a list of fields with `Directive` defaults. The UI
renders "user_id ← Value.user.id (coalesce Key)" as an editable mapping row, with a live preview
computed by `engine.Resolve` against a sample record. **A sink with a completely bespoke setup
experience requires zero core change and zero frontend change**, because the mapping is data and the
resolver is generic. This is constraint #1 and the frontend goal satisfied by one mechanism.

**Live metrics and progress** — `GET /v1/pipelines/{id}/status` returns `PipelineStatus`;
`…/status/watch` streams it over SSE keyed by `Version`. Everything in `Progress` that the connector
cannot know (counts, bytes, rates, durations, queue depth, checkpoint age) is **core-measured**, so no
connector can fail to be instrumented. The four things only the connector can know arrive as frames:
`Estimate` (the backlog denominator), `StreamStatus` (per-stream lifecycle), `Head` (the lag numerator)
and `Fault` (typed, attributed errors).

**Honesty is structural.** `Plan.Guarantee` and `Plan.Why` are in the status document, so the UI shows
"at-least-once — because sink `http` declares no commit tier" and cannot imply exactly-once. Unknowns
are `nil` and render through one shared `Unknown` component, so a source with no `CapHead` shows
"lag: unknown" rather than "lag: 0s". `Complete: false` forces "partial". `Generation !=
ObservedGeneration` renders "config change not yet applied" — and is also an alertable metric, which
nothing in the prior art can express.

**Grep test.** `api/` and the frontend contain the string `"postgres"` exactly zero times. The
custom import analyser fails the build if `api/` gains an import of `connectors/` or `engine/`.

### (e) The same pipeline standalone, then horizontally scaled

**Standalone: `canal run -f orders.yaml`.**

One process. `runtime.Runtime{singlenode.Config, singlenode.Checkpoints (bbolt), singlenode.Coordinator,
singlenode.Status}`. `singlenode.Coordinator.Campaign` returns leadership immediately; `Claim` returns a
lease that never expires; `Assignments` reads a local slice. The enumerator runs on a goroutine in this
process; `Emitter` is a bounded channel into `engine/enumloop.go`. The reader runs on another goroutine;
`Fetch` is a direct method call. **Zero marshalling anywhere.** One bbolt file holds config,
checkpoints and dedupe state, and `Set` is one bbolt transaction, so atomicity across the map holds.

**Cluster: `canal serve --store postgres://…` on nine pods.**

`runtime.Runtime{postgres.Config, postgres.Checkpoints, postgres.Coordinator, postgres.Status}`. The
same binary, the same flag set, the same connectors, the same `Spec`, the same `Checkpoint` bytes.

What changes:
- `Coordinator.Campaign` becomes a Postgres advisory lock. **One** pod plans.
- The planner runs `Enumerator`. Its `Emitter.Assign` no longer writes to a local channel: it writes
  **durable assignment rows** to Postgres. The plan is state, not a leader's in-memory result.
- Each worker `Claim`s assignment rows and `Renew`s the lease. **The lease is the fencing token** — not
  leadership, because the verified Kubernetes caveat is that leader election does not guarantee fencing.
  A worker whose lease lapses stops reading before another can claim it.
- Each worker's `Reader` gets `AddSplits` from its claimed rows, which arrive as the **same
  `SplitDelta` frame** that arrived over a channel in single-node mode. The reader cannot tell.
- `StatusAggregator` becomes rows written by workers and read by the API; `Complete` is false while any
  worker's report is stale.

What does **not** change, and this is the whole point:
- **The connector-facing surface is byte-identical.** No connector method has a different signature, a
  different call order, or a different meaning. `Source`, `Enumerator`, `Reader`, `Sink` are unchanged.
- **The checkpoint format is byte-identical.** A bbolt checkpoint written by `canal run` can be loaded
  by `canal serve` after a `canal state export | canal state import` — same header, same blobs, same
  `SpecHash`.
- **The data plane does not depend on the control plane.** With Postgres down, workers keep reading,
  writing and **checkpointing to their last-known store connection**; they stop only when their lease
  expires, and they stop *cleanly*, at a mark. No planning happens, no rebalance happens, and nothing
  is lost. This is the single most valuable deployment property and it is worth sacrificing elegance for.
- **Nine pods with one Postgres, or one process with one bbolt file, differ in four constructor calls.**

**Scaling the backfill and not the stream** — the case Kafka Connect's one-shot planning makes
inexpressible. `Enumerator.ReaderReady(r, want)` is pull-based, so during `PhaseBackfilling` the
planner hands 64 chunk splits to 20 workers as they ask for work; when chunks are exhausted, the 20
workers get `NoMoreSplits`, release their leases, and only the 1 worker holding the unbounded stream
split keeps reading. **"Snapshot with 20 workers then stream with 2" is a consequence of pull-based
assignment over a changing split set**, not a feature. `tasks.max` stayed advisory in Connect for eight
years because the plan was computed once at connector start.

**Why the protocol angle pays here.** `reader ↔ enumerator` was defined as a frame exchange because a
frame exchange is what an out-of-process connector needs. That decision, made for constraint #3, is
*also* what makes worker↔planner a transport swap: the frames were already wire-shaped, so cluster mode
adds a Postgres-backed `Emitter` and changes nothing else. **One design decision, two payoffs, and the
second was free.**

---

## 13. Decisions taken

| id | choice |
|---|---|
| **record-envelope** | Generic envelope + optional typed `Change` facet, with three separately-lifetimed layers (dual-view `Payload`, addressable `Meta` + secrets, facet), a framework-assigned `RecordID`, and immutable `Origin`. Plus `Completeness` on both images, which the distiller did not propose. |
| **serialization-boundary** | Separately registered `Serializer` + `Framer`/`Unframer` + `Compressor`; connectors implement transport only. `Unframer` writes into a `Batch` so ack aggregation is structural via `Origin.Parent`. `CapStructuredInput` is the SDK-shaped-sink escape hatch. |
| **checkpoint-representation** | Opaque-with-a-typed-header: readable `CheckpointHeader` + opaque `Blob`s for shared position, per-split state, enumerator state, committables, schema epoch, dedupe entries — one atomic `Set`. Core owns the contiguous-prefix resolver per split. |
| **commit-protocol** | Position-carrying in-band `Mark` frames as the spine; the sink's nil return from `Write` is the ack; the core owns position mapping. `Committer` and `TokenSink` are opt-in tiers. Checkpoint Subsuming Contract verbatim. Sink may `RequestMark`. |
| **work-unit-and-planning** | Enumerator + reader, splits as first-class values with a **struct** `SplitID`, pull-based `ReaderReady` assignment, durable assignment rows claimed by lease, and session-interned `SplitRef` for the hot path. |
| **snapshot-model** | Split boundedness is the mechanism; completion is data; `(SourceMode, SinkMode)` per stream is the operator model; `Phase` is a reporting-only header field the core never branches on; core owns the handoff invariant (`Head()` before any backfill read). |
| **snapshot-chunking** | The eight-step algorithm in core behind the `Chunker ∧ Replayer ∧ CursorComparer` conjunction. Both documented scars pre-fixed: indexed range filter, finished-chunk set paged by reference. Chunker's own cursor is checkpointed. |
| **schema-and-drift** | `Streams` required, catalog persisted; schema pinned on the split and deduplicated in enumerator state; changes as ordered in-band `SchemaChange` frames with the epoch committed atomically with position; five-mode `DriftPolicy` with lenient default; sinks declare `SupportedChanges` and nothing more. |
| **capability-surface** | Both, with the strict rule: behaviour is an optional exported interface, the fact is `Capabilities` data, cross-checked at `Register` (panic on mismatch), type-asserted in exactly one function that materialises a plain struct. |
| **config-self-description** | One explicit Go-declared `Spec` (pure data) that **emits** JSON Schema rather than being it. Composite field specs, nesting, tagged unions, a closed 9-node `Predicate` AST for conditional required/visible, named dynamic-choice hooks, per-field `Diagnostic`s with `DiagUnknownField`, sanctioned `Component`-valued fields, and Segment-style sink `Mapping`s with a closed 10-node `Directive` AST. Declared `Bind` mapping, cross-checked at registration, so no accessor ladder and no drift. **No embedded expression language.** |
| **error-classification** | Nine-class closed `Class` set declared by the connector at the point of raise via one `Classify` call; a single core disposition table; `RetryPolicy{MaxAttempts finite, full jitter, Terminal}`; DLQ that works for sources too; split user `Message` from dev `Detail`. |
| **flow-control-and-batching** | Bounded by construction on every edge; capacity-2 batch hand-off; one framework-owned per-split in-flight concept; `Buffer` as a pluggable stage with `WhenFull` in the type; `Push` returns `accepted int` so rejection is in the signature; batching as connector-declared policy the framework enforces, goroutine-free. |
| **topology-and-transforms** | Fixed small outer shape as an implementation fact, never an enumerated stage list in any API; all variety from `Field.Component` recursive composition (the sanctioned meta-component declaration); one `Transform` interface whose out-batch gives the full return-type vocabulary; `Batch.Derive` makes provenance structurally immutable. |
| **deployment-seam** | Four interfaces (`ConfigStore`, `CheckpointStore` with atomic `Set`, `Coordinator`, `StatusAggregator`), `singlenode` + Postgres-first, leader plans only, lease is the fencing token, plan is durable rows, data plane independent of control plane, k8s-shaped `Phase` + `Conditions`. |

### Where I disagree with the distiller

1. **`SplitRef` interning.** The distiller's split identity is a struct, full stop. I keep the struct as
   the *durable* identity and add a session-scoped integer alias for the per-record path, established
   in-band. Twelve bytes per record instead of a string, and it stays wire-legal because it is an
   interned symbol, not a host handle.
2. **Four write outcomes, not Flink's six.** Retryability, DLQ-ability and terminality are properties of
   `Fault.Class`. A second vocabulary for the same concept is R9's definition of a modelling error.
3. **`Completeness` on change images.** The open question asked whether before-images are needed. The
   real defect is that a *partial* image is indistinguishable from a complete one. Declaring
   completeness is what makes the facet safe for a generic sink.
4. **No embedded expression language, ever, in v1.** The distiller left this open. Two closed ASTs —
   `Predicate` (9 nodes) and `Directive` (10 nodes) — cover every real case in the prior art, are
   browser-evaluable in ~80 lines of TypeScript, add no dependency, and grow additively.
5. **`Streams` is required, not optional.** With `connector.SingleStream` as a one-line helper, the cost
   of requiring discovery is one line, and requiring it means the UI's stream picker, the drift diff and
   the field-path pickers are never conditional on a capability.
6. **JSON Schema is an output, not the contract.** The distiller says "one Go-declared spec that emits
   JSON Schema" — I agree and want to make the consequence explicit: the *frontend does not consume the
   JSON Schema*. It consumes the `Spec`, because every `Spec` node is renderable and arbitrary JSON
   Schema is not.
7. **Dedupe shares the checkpoint's durability domain.** The open question asked. Answer: dedupe entries
   go in the same atomic `Set` as the checkpoint, which makes R5's "committed after the write"
   structural rather than a discipline. No separate strict-CAS store.
8. **`iter.Seq` stays out of the plugin surface.** There is nowhere to put an error, nowhere to put a
   cursor, no context, and `iter.Pull` costs a coroutine per split with a panic-on-misuse contract.
   `Fetch(ctx, *Batch) error` is the cursor; `src.All(ctx)` is an `iter.Seq2` **adapter** in `testkit`
   for tests and CLI tooling only — the same relationship `sql.Rows` has to `range`.

---

## 14. Honest weaknesses

1. **The `Batch` ownership contract is the sharpest edge in the design.** "Ownership of every byte slice
   transfers on send; the sender must not retain" is a rule the compiler cannot check. The 0xDE-poisoning
   debug build catches retention in tests, but a connector author who returns a slice into a reused read
   buffer *and* has no test for it ships a data-corruption bug. Go has no borrow checker and I am
   inventing borrow discipline. A safer design would copy on admission and cost one allocation per
   record; I chose the fast path and a debug-mode tripwire, and that is a genuine risk I am taking on
   behalf of connector authors.

2. **Nothing here is validated against Conduit.** `docs/research/conduit.md` is a fetch manifest, not a
   dossier — zero primary source was read. Conduit is the closest prior art that exists and it already
   ships one Go interface satisfied by both in-process and gRPC connectors. My entire angle is a bet on
   that being the right shape, and the one system that has actually done it was not read. **This must be
   re-run before any interface is frozen.** If Conduit gave something up to make one interface serve both
   transports — context fidelity, streaming shape, typed errors degrading to strings — I am about to
   rediscover it the expensive way.

3. **The gRPC binding is asserted, not demonstrated.** I claim every frame is wire-shippable and the
   reflective conformance test enforces it, but "the type graph is serialisable" is weaker than "a
   working subprocess connector exists". Three specific things I have not proven: `Host` as a client stub
   over the same session (metric registration over a wire is a round trip per sample unless batched, and
   I have not designed the batching); `Emitter` back-pressure semantics when the enumerator is in another
   process; and the `SplitRef` session table surviving a mid-session reconnect. Benthos's gRPC path had to
   demote `AutoRetryNacks` to a bool, and I may have an equivalent demotion waiting.

4. **The chunked-snapshot engine is a large, source-agnostic, hard-to-test lump in core, resting on one
   source.** The eight-step watermark algorithm is verified against Flink CDC alone; Debezium verified
   nothing, so there is no independent cross-check of the DDD-3 protocol. It is the single biggest piece
   of complexity in `engine/`, it is only exercised by connectors declaring three capabilities, and a
   correctness bug in it produces silent duplicates or silent loss during a backfill — the worst possible
   failure mode in the worst possible place.

5. **Two closed ASTs will grow, and every growth is a frontend change.** `Predicate` and `Directive`
   avoid an expression-language dependency, but the moment someone wants `length(x) > 3` or
   `regex_extract(...)` in a sink mapping, the answer is either "add a node kind, ship Go and TypeScript
   together" or "adopt the grammar after all". I have a version-negotiation story for frames and no
   version-negotiation story for AST node kinds: an old frontend meeting a new `DirectiveKind` renders
   the mapping as uneditable. That is a real coupling between core and frontend releases that the rest of
   the design carefully avoids.

6. **`Spec` + `Bind` is more code than struct tags, and authors will resent it.** A `Spec` for a
   twenty-field connector is fifty lines of builder calls plus a `Bind` block, versus twenty struct
   tags. The registration-time cross-check makes drift impossible, but the *ergonomics* are worse than
   the thing every Go developer expects, and "worse than the obvious thing" is how a plugin API loses
   contributors. A `spec.FromStruct(&conf{})` generator would help and would reintroduce exactly the
   reflection-derived surface I rejected.

7. **`Reader.Fetch` blocking semantics are underspecified.** I say it returns nil with a possibly-empty
   batch and "is expected to block until data, ctx expiry, or a short internal deadline". That is prose,
   and prose is what canal's design rules exist to distrust. A reader that returns immediately with an
   empty batch spins the engine; a reader that blocks for its full context deadline delays marks and
   drain. Flink solved this with a future-based readiness signal, which is less natural to write in Go. I
   have taken the natural-to-write option and left the pathological case to a documented convention plus
   an engine-side spin detector — which is a mitigation, not a fix.

8. **Nine classes may be too many and `ClassUnknown` will absorb everything.** A closed set makes the
   class a legitimate metric label, but the difference between `TransientUpstream` and `NotConnected`, or
   between `PermanentContract` and `PermanentUpstream`, requires connector-author judgement at every
   raise site. Vector's version of this is unverified in the research, and the honest prediction is that
   the distribution will be 80% `TransientUpstream` and 15% `ClassUnknown`, at which point the taxonomy
   is decoration. It needs a conformance test in `testkit` that fails a connector whose faults are
   mostly `Unknown`, and I have not designed that test.

9. **`Phase` is reporting-only "by rule", and rules erode.** The whole snapshot-model argument rests on
   the core never branching on `Phase`. There is nothing structural stopping a future contributor from
   writing `if h.Phase == PhaseBackfilling`. The two mature frameworks that fell into this trap did not
   set out to. A stronger design would put `Phase` in a type the engine cannot compare — and I did not,
   because the read model needs to compare it.

10. **Standalone mode's atomicity claim depends on a single bbolt file, which pins a process to a
    volume.** `Set` is atomic because it is one bbolt transaction. That is correct and it also means
    `canal run` cannot be two processes, and a laptop pipeline with a disk buffer cannot move hosts. It is
    the right trade for a dev binary and it is the same constraint Vector's disk buffers imposed, which
    Vector's own users experience as a limitation.

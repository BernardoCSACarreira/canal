# Proposal: `database/sql` for data movement

**Status:** draft (R12: this is a draft proposal, not normative).
**Angle:** the smallest possible required interface surface; every richer behaviour is an optional
interface discovered by type assertion, exactly as `database/sql/driver` discovers `QueryerContext`,
`SessionResetter` and `RowsColumnTypeNullable`.

---

## 0. Thesis

A source is five required methods across three tiny interfaces. A sink is five. Everything else —
snapshots, chunking, parallelism, schema discovery, two-phase commit, resume, lag, batching,
upstream acknowledgement — is an **optional interface** that the engine finds by type assertion, and
whose presence *changes which plan the engine builds*, never how well a plan performs.

The bet is that `database/sql/driver`'s ratio (11 required methods, 20 optional interfaces, a
world-class driver implements ~30) is the correct shape for a connector framework, and that the two
classic objections to it — *"a type assertion cannot cross a process boundary"* and *"a missing
optional interface degrades behaviour invisibly"* — are both solved by the same single mechanism,
lifted from `sql.ColumnType`:

> **The engine probes every optional interface exactly once, at a named moment, and collapses the
> result into a core-owned, serialisable `CapabilitySet` in which every absence carries a reason
> string. After that moment nothing in the engine type-asserts, and nothing in the engine has a
> nil-check on a connector.**

That struct is simultaneously: the thing the UI reads, the input to admission control, the fencing
against silent downgrade, and the seam an out-of-process connector plugs into — because the probe
table's output is a set of **function fields**, and function fields can be filled by an RPC stub just
as easily as by a type assertion.

Three rules follow, and they are the whole design:

**DG-1 — Probe once, at admission.** Type assertions on connector values exist in exactly one file,
`internal/capability/table.go`, and they exist there as *data* (a row per capability with a `Probe`
and a `Bind` closure). No type assertion on a hot path. No type assertion after `Start`.

**DG-2 — A missing capability changes *what* runs, never *how well* it runs.** The engine never
substitutes a weaker algorithm for a requested one. Missing `Chunker` does not mean "a slower
snapshot"; it means the plan contains one unchunked assignment, and the plan is a document the
operator can read. Missing `TwoPhaseWriter` under `delivery: exactly_once` is an **admission
refusal**, not a quieter guarantee. This is `ctxDriverBegin` refusing a non-default isolation level
rather than ignoring it, applied to every guarantee canal has.

**DG-3 — Declared must equal implemented, and absence must be explained.** A factory declares the
capability masks it claims. The engine cross-checks the claim against the probe at admission; a
mismatch is a `PermanentContract` refusal. And for every capability that is *absent*, the
`CapabilitySet` entry carries a non-empty `Reason` — enforced by a core test, not by discipline
(R8). The UI therefore never renders a missing capability as a blank; it renders *why*.

What this buys against the design rules: R1 (no fixed stage list anywhere in the contract — the
pipeline shape is derived from capabilities), R2 (the envelope is decided here, first, and is
transport-free), R4 (the ack point is a reified, admission-checked property, not prose), R7 (the
per-record failure shape is in the *required* sink signature, so it cannot be skipped), R9 (one
closed capability vocabulary, one metric vocabulary, one condition vocabulary), R10 (defaults are
labelled as defaults in the plan document).

### 0.1 The honest boundary of "zero core edits"

Constraint #4 says adding a connector must touch no core file. This design satisfies that
absolutely: a new connector adds a package, an `init()`, and nothing else — no registry edit, no
enum value, no switch arm, no capability row.

It does **not** claim that adding a *new capability to canal itself* is free: that adds one interface
and one row to the capability table. That is core's own business, and the row is data. The rule I am
committing to is precise:

> Core may branch on capability presence and on its own plan strategies. Core may **never** branch on
> connector name, connector kind, source type, or any string a connector supplied.

There is no `switch cfg.Type` in this design, anywhere, ever.

---

## 1. Package layout

```
canal/                          THE CONTRACT. Imports: stdlib only. Never imports engine, store, api.
  record.go                     Record, RecordID, Payload, Meta, StreamRef, Origin
  value.go                      Value (sealed sum type) + closed member set + IsValue
  facet.go                      Facet (sealed), Change facet, FacetOf[T] accessor
  batch.go                      RecordBatch, ownership + recycle contract
  source.go                     REQUIRED: SourceFactory, Source, Reader
  source_caps.go                OPTIONAL source-tier interfaces (one per capability)
  sink.go                       REQUIRED: SinkFactory, Sink, Writer, WriteResult
  sink_caps.go                  OPTIONAL sink-tier interfaces
  cursor.go                     Cursor, Blob (version + bytes), Assignment, AssignmentID, KeyRange
  checkpoint.go                 Checkpoint, AssignmentState, Committable, CheckpointStats
  caps.go                       CapabilityID (closed), CapMask, CapabilitySet, CapabilityEntry
  spec.go                       Spec, Field, Union, Predicate, Choice, Constraint  (config-as-data)
  config.go                     Config (pre-parsed/validated/defaulted) + Get[T] + Decode
  diagnostic.go                 Diagnostic, Severity, FieldPath
  errors.go                     Class (closed), Error, sentinels, RetryPolicy
  runtime.go                    Runtime, MetricID (closed), Signal (sealed), Counter/Gauge/Histogram
  catalog.go                    Catalog, Stream, Schema, Column, SchemaChange, ChangeKind
  status.go                     Phase, Condition, Progress, PlanStrategy, DeliveryClass
  registry.go                   Registry (value type) + Default + Register
  codec.go                      Encoder, Decoder, Framer, Compressor (separately registered stages)
  transform.go                  Transform + return vocabulary

canal/canalkit/                 Ergonomic adapters. Imports: canal.
                                ReaderFunc, WriterFunc, StaticSpec, SimpleSource, BlobCursor[T],
                                JSONBlob[T], PagedChunker — a trivial connector is one function.

canal/canaltest/                Conformance kit. Imports: canal, testing.
                                AssertDeclarations, AssertReaderContract, AssertRetrySafety,
                                AssertCursorRoundTrip, GoldenCapabilityReport.

canal/internal/capability/      THE ONLY TYPE ASSERTIONS. Imports: canal.
  table.go                        capability rows: {ID, Tier, Iface, Probe, Bind, Unlocks, Satisfies}
  probe.go                        Probe(any, Tier) CapabilitySet
  bound.go                        boundSource / boundReader / boundWriter facades (function fields)
  remote.go                       NewRemoteReader/Writer: same facades, RPC-filled. (later phase)

canal/internal/admit/           Admission control. Imports: canal, capability.
                                Requirements lattice, DeliveryClass computation, PlanStrategy
                                selection, Refusal, Downgrade. Produces an immutable Plan.

canal/internal/engine/          The runtime. Imports: canal, capability, admit.
  pipeline.go  planner.go  readloop.go  writeloop.go  batcher.go  ackgraph.go
  prefix.go      (contiguous-prefix resolver, per assignment)
  chunkscan.go   (the core chunked-snapshot engine: watermarks + indexed range filter)
  drift.go       (five-mode drift policy)

canal/internal/codecreg/        Built-in codecs/framers/compressors. Imports: canal.
canal/internal/store/           ConfigStore, CheckpointStore impls: memory, bolt, postgres.
canal/internal/coord/           Coordinator impls: single, postgres (leases).
canal/internal/api/             HTTP read/write model. Imports: canal, admit, store, coord.
canal/internal/status/          StatusStore + aggregation. Imports: canal.

canal/connectors/<name>/        One package per connector. Imports: canal (+canalkit). Nothing else.
canal/cmd/canal/                main. Imports engine, api, store, coord, and connectors for init().

web/                            TypeScript. Reads only internal/api's JSON. Zero connector knowledge.
```

**Dependency direction, stated as a rule:** arrows point *into* `canal` and never out of it. `canal`
imports only the standard library, so a connector package's dependency footprint is `canal` plus
whatever driver it wraps. `internal/engine` may import `canal`; `canal` may never import
`internal/*`. `web/` may import nothing — it consumes JSON.

**Why `capability` is `internal/` and not part of the contract:** connector authors must never probe
each other, and the UI must never see the probe. They see `canal.CapabilitySet`, which lives in the
contract package because it is a value the API serialises.

---

## 2. The record model (R2: decided first, transport-free)

```go
package canal

// RecordID is the framework-assigned, stable identity of a record while it is in flight.
//
// It is assigned by the engine the instant a record leaves a Reader and is never changed by any
// transform, batcher, codec or sink. It exists because positional identity within a batch is a
// proven mistake: Benthos marks its own WalkMessages "// Deprecated: This method is harmful"
// precisely because a batch-shaped transform can reorder, expand or drop records and leave the
// framework unable to correlate an outcome back to a source position.
//
// RecordID is in-flight only. It is not persisted, it is not a dedupe key, and it is not stable
// across restarts. The durable identities are Origin.Cursor (position) and Meta dedupe keys.
type RecordID uint64

// Record is canal's canonical envelope. It is the spine of the system (design rule R2) and it is
// deliberately free of any transport, source or sink shape.
//
// Layers, in order of lifetime:
//   - ID and Origin are assigned once by the engine and are immutable.
//   - Key, Value and Meta are freely mutable by transforms.
//   - Facets are optional typed views, added by a Reader or a transform, read by whoever cares.
type Record struct {
	// ID is the engine-assigned in-flight identity. Readers must leave it zero; the engine
	// assigns it. A transform that produces new records must call Derive so provenance is carried.
	ID RecordID

	// Key is the optional identity/routing payload. Nil means "no key". A sink that needs a key
	// (an upsert target, a partition) declares KeyRequired in its Spec and admission enforces it,
	// so "no key" is refused at submit time rather than discovered at 3am.
	Key *Payload

	// Value is the payload. It is never nil; a deletion is expressed as a Change facet with a nil
	// After image, not as a nil Value.
	Value Payload

	// Meta is a separately addressable namespace. It is NOT mixed into Value, because a transform
	// that rewrites the payload must not be able to destroy routing or provenance metadata, and
	// because Vector's regret is precisely that its early design had nowhere else to put it.
	Meta Meta

	// Stream identifies the logical object this record came from (a table, a topic, an endpoint,
	// a file glob member). It is opaque to the engine except as a metric label and a catalog key.
	Stream StreamRef

	// origin is immutable provenance. Unexported on purpose: this is the structural defence
	// against Kafka Connect's KIP-793 retrofit, where SMTs mutated topic/partition while offset
	// accounting needed the pre-transform coordinates, forcing originalTopic/originalPartition/
	// originalOffset to be bolted on plus prose warnings in two javadocs.
	//
	// A transform cannot corrupt checkpoint identity because a transform cannot reach this field.
	origin Origin

	facets facetSet
}

// Origin returns a copy of the record's immutable provenance.
func (r *Record) Origin() Origin { return r.origin }

// Origin is where a record came from, in the coordinates the checkpoint uses.
type Origin struct {
	Assignment AssignmentID // which unit of work produced it
	Cursor     Cursor       // the source position AFTER this record (empty if the source has none)
	Seq        uint64       // monotonic within Assignment; the contiguous-prefix resolver's key
	ReadAt     time.Time    // when the engine received it
	SourceTime time.Time    // zero if the source did not supply one; never defaulted to ReadAt
	Bounded    bool         // true if produced by a bounded assignment (a backfill chunk)
	Label      string       // REPORTING ONLY: e.g. "backfill chunk 12/400". Never branched on.
}

// Derive mints a new record whose provenance is inherited from r. It is the only way a transform
// can create a record that the ack graph can account for. The engine assigns a fresh ID; Origin is
// copied verbatim, including Seq, so N records derived from one source record share one position
// and the prefix resolver treats them as a unit.
func (r *Record) Derive() *Record

// StreamRef names a logical stream. Namespace is optional (schema, database, folder); Name is
// required. Two fields, not one dotted string, because Flink's string-only split id forced CDC to
// encode structure into it and parse it back out in three separate places.
type StreamRef struct {
	Namespace string
	Name      string
}

// Meta is a record's metadata namespace. Values are canal Values so metadata is codec-encodable
// without a second serialisation story.
type Meta struct{ /* copy-on-write map[string]Value */ }

func (m Meta) Get(key string) (Value, bool)
func (m *Meta) Set(key string, v Value)
func (m *Meta) Delete(key string)
func (m Meta) Keys() []string

// Secret marks a metadata key whose value must never be logged, exported as a metric label, or
// serialised into the status document. The core redacts it; no connector has to remember to.
func (m *Meta) SetSecret(key string, v Value)
func (m Meta) IsSecret(key string) bool
```

### 2.1 Payload: the dual view, with mutability in the accessor name

```go
// Payload is a record body in one of two views: raw bytes as they arrived, or a structured Value.
// Both views may be populated; the engine converts lazily and caches, so a pipeline that never
// inspects a payload never parses it, and a transform that touches one field does not force a
// re-encode of the whole record unless it mutates.
//
// The mutability rule is in the accessor name, which is Benthos's best ergonomic decision:
// Bytes/Structured hand back a read-only view and may share memory; BytesMut/StructuredMut
// guarantee an owned copy the caller may modify.
type Payload struct{ /* bytes []byte; structured Value; which uint8 */ }

func NewBytesPayload(b []byte) Payload
func NewStructuredPayload(v Value) Payload

// Bytes returns the payload as bytes, encoding the structured view on demand using the pipeline's
// configured Encoder. The returned slice must not be modified.
func (p *Payload) Bytes(ctx context.Context) ([]byte, error)

// BytesMut returns an owned, modifiable copy.
func (p *Payload) BytesMut(ctx context.Context) ([]byte, error)

// Structured returns the payload as a Value, decoding the byte view on demand using the pipeline's
// configured Decoder. The returned Value must not be modified.
func (p *Payload) Structured(ctx context.Context) (Value, error)

func (p *Payload) StructuredMut(ctx context.Context) (Value, error)

func (p *Payload) SetBytes(b []byte)
func (p *Payload) SetStructured(v Value)

// Size reports the payload's size in bytes without forcing an encode where possible; ok is false
// when the size is not knowable without encoding. (value, ok) rather than a lying zero: the
// sql.ColumnType discipline.
func (p *Payload) Size() (n int, ok bool)
```

### 2.2 Value: a sealed sum type, not `any`

`database/sql` uses `type Value any` plus a documented six-member closed set plus a runtime
validator. That is a pre-generics design. Go 1.23 lets canal close the set properly without paying
for generics on the envelope.

```go
// Value is canal's field value type. It is a SEALED interface: the only implementations are in this
// package, so the set cannot be widened by a third party and every type switch in the engine and in
// every codec is over named types.
//
// Rejected alternative: `type Value any` with a documented closed set (database/sql). It has zero
// friction but no compile-time safety and a third party can and will put a *sql.NullString in it.
// Rejected alternative: Record[T]. A type parameter on the record forces one onto Source, Sink,
// Buffer, Codec, Registry and Pipeline, and the registry then cannot hold them in one map without
// erasing back to any. A type parameter that must be erased at the registry boundary buys nothing.
type Value interface {
	isValue()
	Kind() Kind
}

// Kind is the closed set of value kinds. Closed because it is a legitimate bounded metric label and
// a UI vocabulary (R9: one wire enum).
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt    // int64
	KindUint   // uint64 — added because Mongo/Kafka offsets overflow int64 and Connect's omission bites
	KindFloat  // float64
	KindDecimal
	KindString
	KindBytes
	KindTime
	KindList
	KindMap
	KindStream // a nested, lazily-read sub-stream (database/sql's cursor Value, generalised)
)

type (
	Null    struct{}
	Bool    bool
	Int     int64
	Uint    uint64
	Float   float64
	Decimal struct { Unscaled []byte; Scale int32 } // exact; never a float
	String  string
	Bytes   []byte
	Time    struct { T time.Time; Precision TimePrecision }
	List    []Value
	Map     struct{ /* ordered: key order is preserved because some sinks care */ }
	Stream  struct{ /* lazily-read nested record stream; closed when the parent is closed */ }
)

// Every kind has a compile-time conformance assertion, the database/sql idiom used twice in
// thirty lines there and everywhere here.
var (
	_ Value = Null{}
	_ Value = Bool(false)
	_ Value = Int(0)
	// ...
)
```

### 2.3 Facets: the optional typed view

```go
// Facet is an optional typed view attached to a record. It is how canal gets CDC semantics without
// putting a relational shape in the core envelope (constraint #1).
//
// The generic path never looks at facets. A CDC-aware sink asks for one and gets (facet, ok). A
// webhook source attaches none and every generic sink still works. This is the resolution of the
// record-envelope trilemma: total genericity provably fails (Benthos's MySQL and Postgres CDC inputs
// invent different op vocabularies and different position keys), and a typed CDC envelope in core
// breaks constraint #1.
type Facet interface {
	facetKind() FacetKind
	FacetVersion() int
}

type FacetKind string

const (
	FacetChange FacetKind = "change" // core-defined
	FacetExt    FacetKind = "ext"    // third-party, namespaced by Name
)

// FacetOf is the accessor. This is a good use of generics: the type parameter is on the accessor,
// not on Record, so nothing propagates into Source, Sink, Buffer, Codec or Registry.
func FacetOf[T Facet](r *Record) (T, bool)

func SetFacet(r *Record, f Facet)

// Change is the one typed facet core defines. Its vocabulary is closed and versioned.
type Change struct {
	Op        ChangeOp
	Before    *Payload // nil when unavailable — see BeforeState for WHY it is nil
	After     *Payload // nil for a delete
	BeforeState Availability
	Keys      []string // field paths forming the record identity within its stream
	TxID      string   // empty if unknown
	CommitTime time.Time
	SchemaEpoch int64
}

type ChangeOp uint8

const (
	OpInsert ChangeOp = iota + 1
	OpUpdate
	OpDelete
	OpTruncate // stream-level; carries no payload
	OpUpsert   // the source cannot distinguish insert from update, and says so
)

// Availability answers the question the Benthos Postgres input raises: unchanged_toast_value means
// the connector cannot always produce even a complete after-image, let alone a before-image. A
// three-valued availability is the honest answer, and it lets a sink refuse at admission rather
// than write a row of nulls.
type Availability uint8

const (
	AvailUnknown     Availability = iota // the connector did not say
	AvailComplete                        // every field present
	AvailPartial                         // some fields omitted; see Change.Omitted
	AvailUnavailable                     // the source cannot produce it at all
)
```

### 2.4 Batches and buffer ownership

```go
// RecordBatch is the unit that crosses every interface boundary. Bounded by construction (R6): it
// carries both caps and both counters, because either can bind.
type RecordBatch struct {
	Records []*Record

	maxRecords int
	maxBytes   int
}

func NewRecordBatch(maxRecords, maxBytes int) *RecordBatch

// Append adds a record. It reports ok=false when the batch is full; there is no growth path, so
// unbounded accumulation is inexpressible in the type (R6).
func (b *RecordBatch) Append(r *Record) (ok bool)

func (b *RecordBatch) Len() int
func (b *RecordBatch) Bytes() int
func (b *RecordBatch) Full() bool
func (b *RecordBatch) Reset()

// Ownership contract, stated once and referenced from every signature:
//
//  1. The engine owns the *RecordBatch and its backing array. A Reader appends to the batch it is
//     handed and must not retain any *Record it appended after Read returns. (The io.ReaderAt
//     "Implementations must not retain p" rule.)
//  2. The engine does not recycle a batch until every record in it has reached a terminal
//     disposition (committed, dead-lettered, or dropped by policy). A Reader therefore never sees a
//     buffer that is still in flight.
//  3. A Writer must not retain records after Write returns, except inside a Committable it produced
//     via TwoPhaseWriter.Prepare, whose lifetime is the checkpoint's.
```

---

## 3. Positions, assignments, checkpoints

```go
// Blob is the universal role-boundary payload: a connector-supplied version plus opaque bytes.
//
// Everything that crosses a role boundary or hits disk in canal is a Blob. This is Flink's
// SimpleVersionedSerializer, which is forty lines and buys binary upgrades plus the future gRPC
// boundary for free. The core never inspects Bytes.
type Blob struct {
	Version int    // connector-assigned. A connector MUST decode every version it has ever written.
	Bytes   []byte // opaque to the engine, forever
}

func (b Blob) IsZero() bool

// Cursor is a source position. It is a Blob with a distinct name because it has a distinct
// invariant: resuming a Reader at a Cursor must yield the records strictly after that position, and
// nothing before it.
type Cursor Blob

// AssignmentID is a STRUCT, not a string. Flink's string-only split id forced CDC to encode
// structure into it and parse it back out in three places; that scar is not repeated here.
type AssignmentID struct {
	Stream StreamRef
	Shard  string // connector-chosen: a partition, a file, a key range name, "" for the only one
	Gen    int64  // bumped when the planner re-splits; makes a stale lease detectable
}

func (a AssignmentID) String() string // for logs and metric labels only; never parsed back

// Assignment is canal's unit of work: simultaneously parallelism, resumability, ordering scope,
// in-flight accounting and the assignment unit. It is created by the connector's Planner (or
// synthesised by the engine), placed on a worker by the Coordinator, and handed to Source.Reader.
//
// Neither side learns the other's algorithm: the connector plans how work divides, the runtime
// decides where each piece runs. That separation was invented independently by Kafka Connect
// (taskConfigs + assignor) and Flink (SplitEnumerator + scheduler).
type Assignment struct {
	ID AssignmentID

	// Bounded is THE MECHANISM for snapshot-then-stream. A bounded assignment terminates: its
	// Reader will eventually return io.EOF. An unbounded one never does.
	//
	// A backfill is a bounded assignment. Completion is data (the assignment finished), not a flag
	// and not a phase enum the engine branches on. This is why canal has no pipeline "type": a
	// batch pipeline is one whose assignments are all bounded, a CDC pipeline is one whose
	// assignments are all unbounded, and snapshot-then-stream is a plan containing both.
	Bounded bool

	// Range, when non-nil, is a half-open key range [Low, High) for a chunked backfill. Set only by
	// a Chunker. The engine passes it through; only the connector interprets the Values.
	Range *KeyRange

	// Resume is where to start. Empty means "from the connector's natural beginning". A Reader must
	// honour it if and only if its Source implements Seekable — which is exactly what declaring
	// Seekable asserts.
	//
	// Note that Resume, Range and Params together are precisely the arguments the connector needs to
	// reconstruct this reader — the io.SectionReader.Outer() idiom. The resume payload and the
	// construction payload are THE SAME TYPE, so they cannot drift (R1/R9).
	Resume Cursor

	// Params is connector-authored, versioned, opaque construction state.
	Params Blob

	// Label is REPORTING ONLY: "backfill chunk 12 of 400", "binlog tail". The engine never branches
	// on it. It exists so the UI can be specific without core knowing anything.
	Label string

	// Weight is the connector's optional relative cost hint for placement. Zero means unknown, and
	// the Coordinator then places round-robin rather than inventing a load model.
	Weight int64
}

type KeyRange struct {
	Fields []string // the field paths the range is over
	Low    []Value  // inclusive; nil means unbounded below
	High   []Value  // exclusive; nil means unbounded above
}

// Checkpoint is the durable progress record: a small core-readable header plus opaque per-assignment
// blobs. This is the resolution of the checkpoint-representation fork.
//
// Full opacity (Kafka Connect, Singer) costs source-side lag and progress — Connect admits this in
// its own documentation — and canal has a frontend goal, so that is disqualifying. Full structure
// (Debezium) costs the keyhole and wire-shippability. So: the HEADER is structured and core-owned,
// the PAYLOADS are opaque and connector-owned.
//
// The durability substrate below this type is bytes-in/bytes-out and never sees a domain type. That
// property is exactly what makes Connect's standalone/distributed swap free, and canal keeps it.
type Checkpoint struct {
	PipelineID string
	Generation int64 // config generation this checkpoint was taken under
	Epoch      int64 // monotonic checkpoint number. Flink's subsuming contract keys on this.
	CreatedAt  time.Time

	// Phase is REPORTING ONLY, and this comment is load-bearing. Two mature frameworks
	// (Connect/Debezium and Airbyte, independently) smuggled the snapshot phase into the opaque
	// checkpoint and both lost snapshot progress reporting, snapshot-specific parallelism, and
	// re-parallelised resume. Canal puts phase in the header so it can be REPORTED, and derives all
	// control flow from Assignments[].Bounded and Assignments[].Done instead.
	//
	// There is no `switch cp.Phase` in the engine. A test asserts that (grep-based, in CI).
	Phase Phase

	// Assignments is the per-assignment state. Per-assignment decomposition is why canal does not
	// need merge-patch checkpoint semantics: a source with four thousand partitions writes four
	// thousand small rows, and only the dirty ones are rewritten.
	Assignments []AssignmentState

	// Committables are opt-in two-phase staging handles, present only when the sink implements
	// TwoPhaseWriter. Recovering them from the checkpoint is what makes exactly-once work for a
	// generic destination with no Kafka in the middle.
	Committables []Committable

	// SinkToken is the opposite tier: when the sink implements TokenWriter, the sink stores this
	// checkpoint's token transactionally with the data and the engine reads it back on restart.
	// Then "data landed but state didn't" is structurally impossible.
	SinkToken Blob

	SchemaEpoch int64
	Stats       CheckpointStats
}

// AssignmentState is one assignment's durable progress.
type AssignmentState struct {
	ID AssignmentID

	// Resume is the position to restart from: the end of the longest contiguous committed prefix.
	// Computed by the engine's prefix resolver, per assignment, from acks — NOT from a wall clock.
	//
	// Wall-clock offset commits are a named trap: Connect's 60s offset.flush.interval.ms re-emits
	// fully-acked snapshot chunks after a crash and produces KAFKA-4942's log line that users
	// routinely misdiagnose. Canal advances a position when and only when the records before it are
	// durable at the sink.
	Resume Cursor

	Params Blob // carried forward verbatim so the assignment can be reconstructed

	// Done is the completion signal for a bounded assignment. This is the ONLY thing the engine
	// reads to know a backfill chunk is finished. It is data, not a phase.
	Done bool

	// ChunkerCursor is the chunk SPLITTER's own position, so slicing a 500M-row table resumes
	// mid-object. Airbyte needed a protocol change (is_resumable, CheckpointMixin) to fix the
	// absence of this, and Benthos still restarts a 500M-row snapshot from zero. Treating "a
	// snapshot has no state" as an economy is the most expensive economy in this problem space.
	ChunkerCursor Blob

	// FinishedChunks is a REFERENCE, not an inline set. Flink CDC's finished-chunk set proved too
	// large to ship in one message; canal pages it from the start.
	FinishedChunks ChunkSetRef

	Records int64
	Bytes   int64
	Label   string // reporting only
	UpdatedAt time.Time
}

// Committable is an opaque staged-write handle. Its lifetime is owned by the checkpoint, and the
// engine implements Flink's Checkpoint Subsuming Contract verbatim: committing checkpoint N implies
// every committable from every checkpoint <= N has been committed or is being retried, so a lost
// commit confirmation is repaired by the next checkpoint rather than needing its own recovery path.
type Committable struct {
	Epoch      int64
	Assignment AssignmentID
	Handle     Blob
	Records    int64
}

type CheckpointStats struct {
	RecordsCommitted int64
	BytesCommitted   int64
	AssignmentsTotal int
	AssignmentsDone  int
	CommitDuration   time.Duration
}

// ChunkSetRef points at a paged, engine-owned set of finished chunk ranges.
type ChunkSetRef struct {
	Pages int
	Key   string
}
```

---

## 4. The source: five required methods

```go
// SourceFactory is what a connector registers. It is the database/sql `Driver`+`DriverContext`
// analogue: identity, config self-description, and a constructor. It holds no state and does no I/O.
//
// REQUIRED: 2 methods.
type SourceFactory interface {
	// Spec declares the connector's identity, its configuration as data, and the capability masks
	// it claims. It must be a pure function — callable by the UI with no config, no network, and no
	// instance. This is what lets the frontend render a form for a connector that has never run.
	Spec() Spec

	// Open builds a configured Source. cfg arrives pre-parsed, pre-validated against Spec and
	// pre-defaulted: there is no Configure() callback and no map re-parsed inside the connector.
	//
	// Open must not perform network I/O. It is called by the UI to probe capabilities and by
	// admission to compute a plan; both must be cheap and side-effect free. Connect to the remote
	// system in Reader, or in Validate if you implement Validator.
	Open(cfg Config) (Source, error)
}

// Source is a configured, not-yet-connected source. It is the database/sql `Conn`-shaped tier: the
// planning tier. Everything on it that is not Reader is an optional capability.
//
// REQUIRED: 1 method.
type Source interface {
	// Reader returns a live read handle for exactly one Assignment. The engine calls Reader once per
	// assignment per worker; a Source may therefore be asked for many concurrent Readers, and
	// implementations must be safe for that. A Reader itself is single-goroutine.
	Reader(ctx context.Context, req OpenRequest) (Reader, error)
}

// Reader is a live cursor over one assignment. The database/sql `Rows` analogue.
//
// REQUIRED: 2 methods.
type Reader interface {
	// Read appends records to dst, blocking until at least one record is available, dst is full, or
	// ctx is done. It returns io.EOF when the assignment is exhausted — which happens only for a
	// bounded assignment.
	//
	// Returning (0 records, nil) is legal and means "nothing right now"; the engine will call again.
	// It is NOT a busy-wait invitation: a Reader that has nothing should block on ctx or its own
	// notification until it does, and the engine bounds the call with a poll deadline.
	//
	// ctx is on this method from the first commit and on every blocking method in this document,
	// because Kafka Connect has none and KIP-419 — a safe teardown callback — has been unfixed for
	// seven years as a direct result.
	Read(ctx context.Context, dst *RecordBatch) error

	// Close releases resources. It is called exactly once, always, including on error paths and on
	// cancellation, and it is given a fresh context with the shutdown grace period — never the
	// cancelled read context. Connect's stop() "can be called more than once" ambiguity is not
	// reproduced: Close is once, guaranteed, by the engine.
	Close(ctx context.Context) error
}

// OpenRequest is everything a Reader needs. One struct, so adding a field is not a breaking change
// to the interface — the single most important ergonomic decision in this document, because
// "adding a method to a required interface" is a named trap: Connect forced catch(NoSuchMethodError)
// into its official javadoc, Flink has five default-throwing methods and three Sink API rewrites,
// and Benthos carries an unfixable "// TODO: V5".
//
// Growth strategy, stated as a rule: required interfaces are FROZEN. Growth happens in (a) new
// optional interfaces and (b) new fields on request/response structs. Never a new required method.
type OpenRequest struct {
	Assignment Assignment
	Runtime    Runtime  // metrics, logging, in-band signals — see §8
	Batching   BatchHint // the engine's current preference; advisory
	Delivery   DeliveryClass // what admission agreed to; a Reader may use it to skip work
}
```

### 4.1 Optional source capabilities

Each is a separate exported interface. None is ever a field on a required interface. Every one has a
row in the capability table (§6) and therefore a name the UI can render and a reason when absent.

```go
// ---------- Source tier (configured, pre-run) ----------

// Validator does I/O-backed validation and returns ALL diagnostics at once, per field. Never
// fail-fast, never a single bool: Airbyte's check returns one bool and one string, which is useless
// to a form. This is the second tier of two-tier validation; the first is the declarative
// constraints in Spec, which the core evaluates offline.
type Validator interface {
	Validate(ctx context.Context) []Diagnostic
}

// Prober is a liveness check — the driver.Pinger analogue. Its result populates the Connected
// condition, and Connected is deliberately NOT allowed to imply Progressing.
type Prober interface {
	Probe(ctx context.Context) error
}

// Discoverer returns the streams this source can produce. Persisting its result gives the UI a
// stream picker with zero frontend code, and makes drift a diff against a stored catalog.
//
// A source with no catalog concept (a webhook, a socket, a metrics scrape) simply does not implement
// this, and the engine synthesises a single default stream. That default is LABELLED as synthesised
// in the plan document (R10), so the UI says "this source has one stream, discovered implicitly"
// rather than showing an empty picker.
type Discoverer interface {
	Discover(ctx context.Context) (*Catalog, error)
}

// Planner divides the work. It is called once at start and again whenever the engine asks. It does
// not know or care where the assignments will run.
type Planner interface {
	Plan(ctx context.Context, req PlanRequest) (*PlanResult, error)
}

// Replanner makes the assignment set DYNAMIC. It blocks until the plan should change, which is what
// makes "snapshot with 20 workers, then stream with 2" expressible.
//
// One-shot planning at connector start is a named trap: in Connect, re-splitting requires a task
// restart, so that sentence is literally inexpressible, and tasks.max stayed advisory for eight
// years until KIP-1004.
type Replanner interface {
	// NextPlan blocks until the plan changes, then returns the new one. It returns io.EOF when no
	// further plan changes will ever occur (all work enumerated), which is how the engine knows a
	// bounded pipeline can Complete.
	NextPlan(ctx context.Context, state PlanState) (*PlanResult, error)
}

// Seekable is a marker with a payload. Implementing it ASSERTS that Reader honours
// OpenRequest.Assignment.Resume, returning records strictly after that position and none before it.
//
// It is a distinct capability from Positioner because reading a position and honouring one are
// different promises, and canal computes resumability as their CONJUNCTION — the same shape as
// database/sql deriving keepConnOnRollback from hasSessionResetter && hasConnectionValidator.
type Seekable interface {
	// CursorVersion is the highest Cursor.Version this build writes. It must decode every version
	// <= this. The engine records it in the checkpoint so a downgrade is detected, not crashed into.
	CursorVersion() int
}

// Chunkable declares the source can slice a stream's key space into ranges. Combined with
// Comparable and Replayable it unlocks canal's core chunked-snapshot engine, and the connector's
// entire obligation shrinks to "give me ordered chunks by key" and "let me replay from a position".
type Chunkable interface {
	// Chunks returns an iterator over key ranges for one stream. The iterator carries its OWN
	// resumable cursor, so slicing resumes mid-object rather than restarting.
	Chunks(ctx context.Context, req ChunkRequest) (ChunkIter, error)
}

// ChunkIter yields chunk ranges and its own position.
type ChunkIter interface {
	Next(ctx context.Context) (KeyRange, error) // io.EOF when the key space is exhausted
	Cursor() Blob                               // the splitter's own resume point
	Total() (n int64, exact bool)                // denominator for progress; (0,false) is honest
	Close(ctx context.Context) error
}

// Comparable lets the engine order key values without knowing their domain. Required by the chunked
// snapshot engine's range filter.
type Comparable interface {
	// CompareKeys returns -1, 0, +1. ok is false when the values are not comparable in this
	// source's ordering, which the engine treats as "cannot chunk this stream" — reported, refused
	// if chunking was required, never silently downgraded.
	CompareKeys(a, b []Value) (cmp int, ok bool)
}

// Replayable declares that reading the same assignment from the same Cursor yields the same records
// in the same order. It is the precondition for at-least-once and above, and for the chunked
// snapshot's replay step.
type Replayable interface {
	// ReplayWindow reports how far back replay is guaranteed. Zero duration means unlimited.
	// A retention-limited source (a Kafka topic, a binlog) says so, and the engine can then warn
	// when checkpoint_age approaches it — the single most useful pre-incident signal canal can emit.
	ReplayWindow() time.Duration
}

// SchemaDescriber gives out-of-band schema for a stream, so the UI can show a column list before a
// pipeline has ever run. A source that can only learn its schema by reading does not implement this,
// and the UI shows "schema known only at runtime" rather than an empty table.
type SchemaDescriber interface {
	DescribeSchema(ctx context.Context, s StreamRef) (*Schema, error)
}

// CapabilityReporter is the negotiation hook: the connector MASKS OFF capabilities it statically
// implements but cannot deliver under this configuration.
//
// This exists because a Go type either implements an interface or it does not — capability shape is
// static per type, but real capability is often config-dependent (a Postgres source with a
// replication slot configured can stream; without one it cannot). The engine computes
//
//	Actual = StaticProbe(instance) ∩ ReportedMask
//
// and REFUSES a report that tries to ADD a capability the instance does not implement. Every masked
// capability must carry a Note, and the Note is what the UI renders. That is the difference between
// canal and driver.ErrSkip: ErrSkip is silent and per-call; this is loud, once, before admission.
type CapabilityReporter interface {
	Capabilities(ctx context.Context) (mask CapMask, notes []CapabilityNote, err error)
}

type CapabilityNote struct {
	ID     CapabilityID
	Reason string // REQUIRED. "no comparable key column configured for stream public.events"
}

// ---------- Reader tier (live) ----------

// Positioner exposes the source's position. Read-only: resume is passed in via Assignment.Resume,
// so unlike io.Seeker this is not one method doing two jobs. The write side is the constructor,
// which is the SectionReader.Outer() discipline.
type Positioner interface {
	// Position returns the cursor immediately after the last record appended by the most recent
	// Read. The engine also reads Origin.Cursor per record; Position is the batch-level answer for
	// sources that can only report coarsely.
	Position() Cursor
}

// Phased is the io/database/sql RowsNextResultSet analogue and the cheapest possible snapshot→stream
// handoff for a source that wants to own it: ONE stream object spans multiple phases, and the engine
// checkpoints at the boundary.
//
// It is the fallback tier. A source that also implements Planner + Seekable gets the better,
// parallel, re-parallelisable plan instead. Both are supported; which one runs is in the plan
// document, so the operator can see which they got.
type Phased interface {
	// HasNextPhase reports whether another phase follows the current one. It returns an error —
	// unlike RowsNextResultSet's bare bool — because discovering this may require I/O and
	// database/sql has nowhere to put that failure.
	HasNextPhase(ctx context.Context) (bool, error)

	// NextPhase advances, abandoning any unconsumed records in the current phase. That abandonment
	// is deliberate: it is the engine's explicit "abort the snapshot, go straight to streaming" move.
	NextPhase(ctx context.Context) error

	// PhaseLabel names the current phase FOR REPORTING ONLY.
	PhaseLabel() string
}

// Bounded is a marker with a denominator. Implementing it declares that Read will eventually return
// io.EOF for a bounded assignment.
type Bounded interface {
	// Remaining reports work left. exact=false means it is an estimate; (0, false) means unknown,
	// and the UI must then render "12,481 records so far, total unknown" rather than a progress bar
	// stuck at zero. Total and remaining are separate numbers and the consumer divides — never a
	// pre-divided ratio, which destroys the information that the denominator was unknown.
	Remaining(ctx context.Context) (n int64, exact bool)
}

// Acknowledger is for sources whose upstream needs telling: SQS delete, Pulsar ack, a REST cursor
// commit, a replication-slot advance. The engine calls it after the records are durable at the sink
// and never before (R4).
type Acknowledger interface {
	// Ack reports the durable prefix and any records that were dead-lettered rather than delivered.
	// One method with a request struct rather than Ack/Nack, so a source that must tell upstream
	// about a poison record can, and one that cannot simply ignores the field.
	Ack(ctx context.Context, req AckRequest) error
}

type AckRequest struct {
	Assignment  AssignmentID
	Through     Cursor    // the durable contiguous prefix
	DeadLettered []RecordRef // records that went to the DLQ instead of the sink
	Epoch       int64
}

// LagReporter closes Kafka Connect's documented, admitted gap: no source-side lag metric is possible
// in Connect, by its own statement. Here it is one optional method.
type LagReporter interface {
	Lag(ctx context.Context) (Lag, error)
}

type Lag struct {
	Records  int64         // -1 when unknown
	Bytes    int64         // -1 when unknown
	Duration time.Duration // how far behind the source's own head; 0 when unknown
	Measured time.Time
}

// Quiescer stops producing and flushes in flight without closing. The engine uses it for the
// quiesce-and-flush that must precede applying a schema change downstream.
type Quiescer interface {
	Quiesce(ctx context.Context) error
}

// Classifier lets a connector own its error taxonomy in one function — Vector's best idea, reduced
// to its essence. Without it the engine classifies by unwrapping canal.Error and falling back to
// ClassTransientInternal, which is the safe direction (retain progress, back off).
type Classifier interface {
	Classify(err error) Class
}
```

---

## 5. The sink: five required methods

```go
// SinkFactory mirrors SourceFactory exactly. REQUIRED: 2 methods.
type SinkFactory interface {
	Spec() Spec
	Open(cfg Config) (Sink, error)
}

// Sink is a configured, not-yet-connected sink. REQUIRED: 1 method.
type Sink interface {
	// Writer returns a live write handle for one write partition. The engine decides how many; a
	// sink that wants a say implements SinkPlanner.
	Writer(ctx context.Context, req WriteOpen) (Writer, error)
}

// Writer is a live write handle. REQUIRED: 2 methods.
type Writer interface {
	// Write delivers a batch. Returning a nil error means EVERY record in the batch is DURABLE at
	// the destination — unless this Writer also implements Flusher, in which case nil means
	// "accepted" and durability is Flush's promise.
	//
	// That distinction is not left to prose. It is reified as CapabilitySet.AckPoint, it is shown in
	// the UI, and admission checks it against the requested delivery class. Design rule R4 says a
	// stage that cannot promise durability must not return a success the sender is told to checkpoint
	// on; here the DEFAULT (no Flusher) is the strict reading, and weakening it requires implementing
	// an interface whose presence is visible.
	//
	// The per-record failure shape is in the return type, not in a later KIP. Kafka Connect's
	// `void put(Collection<SinkRecord>)` cannot express partial acceptance and KIP-731 has been Under
	// Discussion since 2021; canal's R7 says write the failure shape at the same time as the success
	// shape, so WriteResult is mandatory and its per-record outcomes are keyed by RecordID, never by
	// position (the Benthos-harmful-method trap).
	Write(ctx context.Context, b *RecordBatch) (*WriteResult, error)

	Close(ctx context.Context) error
}

type WriteOpen struct {
	Partition int    // 0..N-1 of the write partitions the engine created
	Total     int
	Runtime   Runtime
	Delivery  DeliveryClass
	Schemas   map[StreamRef]*Schema // pinned as of this writer's schema epoch; may be empty
}

// WriteResult is the "what actually happened" channel. Every aggregate is (value, ok) so
// "this sink cannot tell you" is a typed answer rather than a zero that reads as success — the
// database/sql Result discipline, with the per-record channel Result is missing.
type WriteResult struct {
	// Accepted is the count of records durably written (or accepted, if AckPoint is OnFlush).
	Accepted int

	// Outcomes holds one entry per record that did NOT plainly succeed. An empty slice with
	// Accepted == b.Len() is the fast path and costs one allocation of zero bytes.
	//
	// Keyed by RecordID. Six named outcomes including partial success, because R7 in the type system
	// is the whole point.
	Outcomes []RecordOutcome

	// Rows, when ok, is a destination-reported affected-row count. Distinct from Accepted, because
	// an upsert of 100 records may affect 40 rows and an operator needs both numbers.
	Rows    int64
	RowsOK  bool

	// Durable reports whether this call made the data durable. For a Writer with no Flusher it is
	// always true; the field exists so the engine's ack path has one uniform thing to read rather
	// than a capability check on the hot path (DG-1).
	Durable bool
}

type RecordOutcome struct {
	ID     RecordID
	Status OutcomeStatus
	Err    error  // a canal.Error carrying a Class; nil for the success statuses
	Detail string // destination-supplied, for the DLQ record and the status document
}

// OutcomeStatus is a CLOSED six-member set. Closed because it is a bounded metric label (R9) and
// because a closed set is what makes "a hint the framework ignores" impossible: every member has a
// defined engine action, asserted by a table-driven test.
type OutcomeStatus uint8

const (
	OutcomeWritten          OutcomeStatus = iota + 1 // durable
	OutcomeAcceptedNotDurable                        // accepted; awaits Flush or Commit
	OutcomeDuplicate                                 // already present; idempotent success, NOT an error
	OutcomeRejected                                  // permanent: the destination will never accept it
	OutcomeRetriable                                 // transient: re-offer this record
	OutcomePartial                                   // some fields landed; treated as Rejected unless
	                                                 // the sink also declares PartialTolerant
)

// Helper constructors so a sink never hand-rolls the common shapes. The driver.RowsAffected idiom.
func AllWritten(n int) *WriteResult
func AllAccepted(n int) *WriteResult
func Mixed(accepted int, outcomes ...RecordOutcome) *WriteResult
```

### 5.1 Optional sink capabilities

```go
// Validator, Prober, CapabilityReporter, Classifier, Quiescer: identical interfaces to the source
// tier, deliberately. One vocabulary, one implementation in the capability table, one UI rendering.

// Flusher separates acceptance from durability. Implementing it MOVES THE ACK POINT, which is why
// the capability set records AckPoint explicitly and admission cross-checks it.
type Flusher interface {
	// Flush makes every record accepted by prior Write calls durable. Returning nil is the
	// durability promise. Returning an error must name which records failed, via WriteResult
	// semantics on the returned FlushResult.
	Flush(ctx context.Context) (*FlushResult, error)
}

type FlushResult struct {
	Durable  int
	Outcomes []RecordOutcome
}

// TwoPhaseWriter is the exactly-once tier for a generic destination — no Kafka in the middle
// required. It implements Flink's two-phase commit with the committables living inside the
// checkpoint, so a lost commit confirmation is repaired by the next checkpoint.
type TwoPhaseWriter interface {
	// Prepare stages everything written since the last Prepare and returns an opaque handle. After
	// Prepare returns, the staged data must survive this process dying.
	Prepare(ctx context.Context) (Blob, error)

	// Commit makes staged data visible. It MUST be idempotent per handle and it MUST tolerate
	// handles from checkpoints older than the newest (the Subsuming Contract): committing epoch N
	// implies every handle from epoch <= N is committed or being retried.
	Commit(ctx context.Context, handles []Committable) ([]CommitOutcome, error)

	// Abort discards staged data. Called on restart for handles the engine did not choose to commit.
	Abort(ctx context.Context, handles []Committable) error
}

type CommitOutcome struct {
	Handle  Blob
	Status  OutcomeStatus
	Err     error
}

// TokenWriter is the other exactly-once tier: the destination stores canal's checkpoint token in the
// same transaction as the data, so "data landed but state didn't" is structurally impossible because
// there is exactly one durability domain.
//
// Only a transactional destination can do this, and it makes the destination authoritative on
// restart — which is a real trade, so it is a capability and not the default.
type TokenWriter interface {
	// WriteWithToken writes the batch and the token atomically.
	WriteWithToken(ctx context.Context, b *RecordBatch, token Blob) (*WriteResult, error)

	// LoadToken returns the last token this destination durably stored. The engine prefers it over
	// its own CheckpointStore when this capability is present, and says so in the plan document.
	LoadToken(ctx context.Context) (Blob, error)
}

// Idempotent declares that re-writing a record the sink has already written is a no-op keyed on
// something stable. It is what promotes at-least-once to effectively-once with no 2PC.
type Idempotent interface {
	// IdempotencyKeyFields names the fields (or Meta keys) forming the key, so the engine can refuse
	// at admission when the configured stream lacks them — rather than discovering duplicates later.
	IdempotencyKeyFields() []string
}

// KeyRequirer declares the sink cannot function without Record.Key. Admission checks it against the
// source's catalog and refuses the pipeline at submit time.
type KeyRequirer interface{ RequiresKey() bool }

// BatchPolicyProvider lets the sink declare its batching envelope; the FRAMEWORK enforces it. The
// sink spawns no goroutine and owns no timer — Benthos's goroutine-free Batcher API is the right
// shape, because five overlapping user-facing knobs with a documented deadlock between two of them
// is what happens otherwise.
type BatchPolicyProvider interface {
	BatchPolicy() BatchPolicy
}

type BatchPolicy struct {
	MaxRecords int
	MaxBytes   int
	MaxAge     time.Duration
	// GroupBy names Meta keys / fields that must not be mixed within one batch (a destination table,
	// a tenant). The engine groups; the sink never sorts.
	GroupBy []string
}

// StructuredWriter is the declared escape hatch for SDK-shaped sinks that want Values, not bytes.
// Declaring it tells the engine to SKIP the encoder stage entirely for this sink, which is visible
// in the plan document rather than being an accident of a codec that silently did nothing.
type StructuredWriter interface{ AcceptsStructured() bool }

// SchemaSink applies drift. The sink declares WHICH change kinds it can apply and nothing more; the
// unanswerable question "should I ALTER TABLE?" is answered by core policy, not by every sink
// reinventing it.
type SchemaSink interface {
	SupportedChanges() ChangeKindMask
	ApplySchemaChange(ctx context.Context, ch SchemaChange) error
}

// SinkPlanner lets the sink ask for a write-side shape. The engine may grant less (bounded by
// config) but never more, and what it granted is in the plan document.
type SinkPlanner interface {
	WritePlan(ctx context.Context, req SinkPlanRequest) (SinkPlanResult, error)
}

// CheckpointRequester lets a sink ask for a checkpoint boundary — "I have just rolled a file, commit
// now". This is what makes commit points align with destination-meaningful boundaries instead of a
// clock. It is a capability, not a required method, and the engine treats it as advisory.
type CheckpointRequester interface {
	// CheckpointRequested returns a channel that is signalled when the sink wants a boundary.
	CheckpointRequested() <-chan struct{}
}

// PartialTolerant declares the sink can meaningfully report OutcomePartial and that the engine
// should route partials to the DLQ with the partial detail rather than treating them as rejections.
type PartialTolerant interface{ TolerantesPartial() bool }
```

---

## 6. The capability system — the heart of the proposal

### 6.1 The closed vocabulary

```go
// CapabilityID is a CLOSED enum. Closed because it is: a bounded metric label, a UI vocabulary, a
// JSON wire enum, and an admission-control key. R9 says one wire enum plus one i18n namespace; this
// is that enum.
type CapabilityID uint16

const (
	// Source tier
	CapValidate CapabilityID = iota + 1
	CapProbe
	CapDiscover
	CapPlan
	CapReplan
	CapSeek
	CapChunk
	CapCompare
	CapReplay
	CapDescribeSchema
	CapReportCaps
	CapClassify
	CapQuiesce
	// Reader tier
	CapPosition
	CapPhases
	CapBounded
	CapAck
	CapLag
	// Sink tier
	CapFlush
	CapTwoPhase
	CapToken
	CapIdempotent
	CapRequireKey
	CapBatchPolicy
	CapStructuredInput
	CapSchemaApply
	CapWritePlan
	CapRequestCheckpoint
	CapPartialTolerant

	capMax
)

// CapMask is a bitset over CapabilityID. Cheap set algebra for admission; never exported to the wire
// as a number (the wire form is the entry list, so a mask value can never be misread across
// versions).
type CapMask uint64

func (m CapMask) Has(id CapabilityID) bool
func (m CapMask) With(ids ...CapabilityID) CapMask
func (m CapMask) Without(ids ...CapabilityID) CapMask
func (m CapMask) And(o CapMask) CapMask
func (m CapMask) Sub(o CapMask) CapMask
func (m CapMask) IDs() []CapabilityID
```

### 6.2 The reified set — the `sql.ColumnType` transplant

```go
// CapabilitySet is the collapsed, core-owned, SERIALISABLE answer to "what can this connector do,
// under this configuration, right now". It is produced exactly once per connector per pipeline
// admission, and after that moment it is the ONLY thing the engine consults.
//
// This type is the resolution of both classic objections to optional interfaces:
//
//   - "A type assertion cannot cross a process boundary." A CapabilitySet is data. An out-of-process
//     connector reports its mask over the wire, the core builds the identical struct, and every
//     downstream line of engine code is unchanged. There are no per-capability forwarders to write,
//     because the engine does not call through interfaces — it calls through the bound facade (§6.4),
//     whose fields are functions, and a function field can be an RPC stub.
//
//   - "A missing optional interface degrades behaviour invisibly." Every entry carries a Reason when
//     absent, a Source saying HOW canal knows, and an Unlocks list saying what the operator lost.
//     Absence is a rendered sentence, not a blank.
type CapabilitySet struct {
	Connector string // the registered name
	Kind      Kind_   // KindSource | KindSink
	Tier      Tier    // which tier this set describes
	Mask      CapMask
	Entries   []CapabilityEntry // one per CapabilityID applicable to this tier — ALWAYS complete

	// Derived, core-computed properties. These exist so the engine and the UI read a fact rather
	// than recomputing a conjunction, and so the conjunction is written down exactly once.
	AckPoint   AckPoint
	Delivery   DeliveryClass // the best this connector alone can support
	Resumable  bool          // CapPosition && CapSeek — the keepConnOnRollback idiom
	Chunkable  bool          // CapChunk && CapCompare && CapReplay
	ProgressKnown bool       // CapBounded, or a Chunker with an exact Total
	CursorVersion int
	ReplayWindow  time.Duration
}

// CapabilityEntry is one capability's full story. Note the shape: a Present bool with a companion
// explanation, which is sql.ColumnType's (value, ok) discipline extended with the ONE thing
// database/sql lacks — a reason.
type CapabilityEntry struct {
	ID      CapabilityID
	Name    string // stable machine token, e.g. "chunk"
	Title   string // i18n key, not a sentence
	Present bool

	// Source says how canal knows. It is what makes DG-3 auditable.
	Source CapSource

	// Reason is REQUIRED when Present is false, and MUST be empty when Present is true. A core test
	// walks every entry of every fixture and fails on a violation. This is R8 applied to the
	// capability report: the invariant is structural, not documented.
	Reason string

	// Unlocks is the operator-facing consequence list: what this capability's presence enables.
	// Rendered next to the absence reason, so "Chunk: absent — the source declares no comparable key"
	// is immediately followed by "would enable: parallel backfill, mid-backfill resume, backfill ETA".
	Unlocks []string
}

// CapSource is how canal learned a capability's state. Closed set.
type CapSource uint8

const (
	CapSrcProbed    CapSource = iota + 1 // the Go type implements the interface
	CapSrcAbsent                         // the Go type does not implement the interface
	CapSrcMasked                         // implemented, but the connector masked it off for this config
	CapSrcDeclaredRemote                 // an out-of-process connector declared it over the wire
	CapSrcUndeclared                     // implemented but NOT declared in Spec → a DG-3 violation
)

// AckPoint is when a sink's success becomes durable. Reified because R4 says delivery semantics are a
// property of the implementation, not of the prose describing it — so canal makes it a value that
// admission reads and the UI renders.
type AckPoint uint8

const (
	AckOnWrite  AckPoint = iota + 1 // Write returning nil means durable
	AckOnFlush                      // Flusher present: Flush returning nil means durable
	AckOnCommit                     // TwoPhaseWriter present: Commit means durable
	AckOnToken                      // TokenWriter present: one durability domain with the data
)
```

### 6.3 The capability table — the only type assertions in canal

```go
package capability // internal

// row is one capability, as data. Adding a capability to canal = adding one interface to the
// contract package and one row here. Adding a CONNECTOR touches neither.
type row struct {
	ID      canal.CapabilityID
	Name    string
	Title   string
	Tier    canal.Tier
	Iface   string // the Go interface name, for diagnostics: "canal.Chunkable"
	Unlocks []string

	// Probe is the ONE type assertion for this capability, in the ONE place canal has them.
	Probe func(v any) bool

	// Bind extracts the capability's methods as closures into the bound facade. This is the field
	// that makes the out-of-process future free: for an in-process connector Bind type-asserts and
	// takes method values; for a remote connector the remote package supplies an equivalent
	// closure set built from RPC stubs. The engine cannot tell the difference and has no code path
	// that could.
	Bind func(v any, into *bound)

	// AbsentReason is the default explanation, used when the connector offered none.
	AbsentReason func(name string) string
}

var table = []row{
	{
		ID: canal.CapChunk, Name: "chunk", Tier: canal.TierSource,
		Iface: "canal.Chunkable",
		Unlocks: []string{"parallel backfill", "mid-backfill resume", "backfill ETA"},
		Probe: func(v any) bool { _, ok := v.(canal.Chunkable); return ok },
		Bind: func(v any, into *bound) {
			if c, ok := v.(canal.Chunkable); ok { into.chunks = c.Chunks }
		},
		AbsentReason: func(n string) string {
			return n + " does not implement canal.Chunkable, so its backfill cannot be split by key"
		},
	},
	// ... one row per CapabilityID. ~29 rows. No switch statement anywhere.
}

// Probe collapses an instance into a CapabilitySet. Called exactly once per connector per admission.
//
// declared is the mask from the factory's Spec. reported is the optional mask from
// CapabilityReporter. The three-way reconciliation is DG-3:
//
//	probed      = what the Go type actually implements
//	actual      = probed ∩ reported            (a connector may mask off, never add)
//	undeclared  = probed \ declared            → CapSrcUndeclared, a PermanentContract refusal
//	overdeclared= declared \ probed            → a PermanentContract refusal
func Probe(v any, tier canal.Tier, declared canal.CapMask,
	reported canal.CapMask, notes []canal.CapabilityNote) canal.CapabilitySet
```

**Why `Bind` and not just `Probe`.** If the engine kept the connector as an `any` and type-asserted
at each call site, it would (a) violate DG-1, (b) put a branch on the hot path, and (c) make the
out-of-process path require one hand-written forwarder per capability — which is exactly Benthos's
scar: nine forwarders for `ConnectionTestable`, and a gRPC path that had to demote `AutoRetryNacks`
to a bool because a type assertion does not survive a wire.

`Bind` converts the type assertion into a **function field** at probe time. After that, the engine's
code reads:

```go
if br.caps.Resumable {
    cur := br.position()          // a function field. never nil when caps.Resumable is true.
}
```

There is no interface, no assertion, no nil check. The invariant "`position != nil` iff
`Mask.Has(CapPosition)`" is established in one place and asserted by one test.

### 6.4 The bound facade

```go
// bound holds every optional capability as a closure. Exactly one of these per connector instance.
// Nil fields are impossible to reach: the engine gates on caps.Mask, and a test asserts the
// bijection between mask bits and non-nil fields for every row in the table.
type bound struct {
	caps canal.CapabilitySet

	// source tier
	validate     func(context.Context) []canal.Diagnostic
	probe        func(context.Context) error
	discover     func(context.Context) (*canal.Catalog, error)
	plan         func(context.Context, canal.PlanRequest) (*canal.PlanResult, error)
	nextPlan     func(context.Context, canal.PlanState) (*canal.PlanResult, error)
	chunks       func(context.Context, canal.ChunkRequest) (canal.ChunkIter, error)
	compareKeys  func(a, b []canal.Value) (int, bool)
	describeSchema func(context.Context, canal.StreamRef) (*canal.Schema, error)
	classify     func(error) canal.Class
	quiesce      func(context.Context) error

	// reader tier
	position     func() canal.Cursor
	hasNextPhase func(context.Context) (bool, error)
	nextPhase    func(context.Context) error
	phaseLabel   func() string
	remaining    func(context.Context) (int64, bool)
	ack          func(context.Context, canal.AckRequest) error
	lag          func(context.Context) (canal.Lag, error)

	// sink tier
	flush        func(context.Context) (*canal.FlushResult, error)
	prepare      func(context.Context) (canal.Blob, error)
	commit       func(context.Context, []canal.Committable) ([]canal.CommitOutcome, error)
	abort        func(context.Context, []canal.Committable) error
	writeToken   func(context.Context, *canal.RecordBatch, canal.Blob) (*canal.WriteResult, error)
	loadToken    func(context.Context) (canal.Blob, error)
	applySchema  func(context.Context, canal.SchemaChange) error
	writePlan    func(context.Context, canal.SinkPlanRequest) (canal.SinkPlanResult, error)
	ckptRequested func() <-chan struct{}
}

// The out-of-process constructor. Same struct, same engine, closures backed by RPC. This is the
// entire cost of satisfying constraint #3's future requirement, and it is paid in ONE file.
func NewRemote(declared canal.CapMask, stubs RemoteStubs) (canal.Source, canal.CapabilitySet)
```

### 6.5 Runtime decline: `ErrCapabilityDeclined`

```go
// ErrCapabilityDeclined is canal's driver.ErrSkip, with one crucial difference: it is legal only
// from a capability method invoked during NEGOTIATION (Plan, Chunks' first Next, WritePlan,
// DescribeSchema, LoadToken), never after the pipeline has started.
//
// database/sql's ErrSkip is silent and per-call, which is exactly the invisible degradation this
// proposal exists to prevent. Returning it here removes the capability from the CapabilitySet BEFORE
// admission, with the error's message becoming the entry's Reason. After Start it is a
// PermanentContract error that fails the pipeline, because at that point the plan has already been
// admitted on the strength of the capability.
var ErrCapabilityDeclined = errors.New("canal: capability declined for this configuration")
```

---

## 7. Config as data — one artefact, four consumers

```go
// Spec is a connector's complete self-description. It is a pure value: no config, no I/O, no
// instance. Four consumers read it and there is no second source of truth (R8):
//
//   1. the offline validator (declarative constraints)
//   2. the JSON Schema exporter (for CLI, IaC, editor completion)
//   3. the live form in the frontend (Fields, ShowIf, DynamicChoices, Secret)
//   4. the docs generator
//
// Config declared by the connector as data, where that data IS the UI schema, is the one pattern
// every system that shipped a usable connector UI converged on, independently, with no exceptions.
type Spec struct {
	Name    string // registry key. lowercase, dot-separated: "postgres.cdc"
	Kind    Kind_  // KindSource | KindSink | KindTransform | KindCodec
	Title   string // i18n key
	Summary string // i18n key
	Version string // semver of the connector itself
	Stability Stability // Experimental | Beta | Stable | Deprecated. R10: scaffolding is labelled.

	Fields []Field

	// SourceCaps / ReaderCaps / SinkCaps / WriterCaps are the DECLARED masks. Declared ⊇ actual, and
	// a mismatch either way is a PermanentContract refusal (DG-3). They exist because the UI must
	// know what a connector could do before any instance exists.
	SourceCaps CapMask
	ReaderCaps CapMask
	SinkCaps   CapMask
	WriterCaps CapMask

	// Mappings are Segment-style sink field mappings: default extraction expressions over the
	// generic record. This is how a specialised sink UX ("map this field to Braze's external_id")
	// ships with ZERO core changes, satisfying constraint #1's second sentence.
	Mappings []FieldMapping

	Examples []Example
}

// Field is one configuration field. It supports nesting, tagged unions and component-valued fields,
// because ConfigDef cannot describe any of those and fakes them with dotted prefixes.
type Field struct {
	Path     string    // dotted path; the ONLY identifier. One representation per entity (R1).
	Type     FieldType
	Title    string
	Doc      string
	Required bool
	Default  any     // nil means no default; typed per FieldType
	Secret   bool    // core redacts in logs, metrics, status and API responses. Zero connector code.
	Advanced bool    // collapsed by default in the form
	Sensitive bool   // shown but never persisted in plaintext; resolved from a SecretRef

	Enum  []Choice
	// DynamicChoices names a hook the connector exposes (see ChoiceProvider) so a form can populate
	// a dropdown from the remote system. A NAME, not a function, so it survives serialisation.
	DynamicChoices string

	// ShowIf is a declarative predicate over the whole config. Declarative because a Recommender
	// callback cannot be evaluated in a browser and canal wants the form to be live without a
	// round-trip per keystroke.
	ShowIf *Predicate
	// RequiredIf makes conditional-required a predicate rather than a bool.
	RequiredIf *Predicate

	Union     *Union        // tagged union: a discriminator field plus variants
	Object    []Field       // nesting, natively
	Item      *Field        // array element spec
	Component *ComponentRef // a field whose value is another registered component → recursion

	Constraints []Constraint
}

type FieldType uint8

const (
	FTString FieldType = iota + 1
	FTInt; FTFloat; FTBool; FTDuration; FTByteSize; FTEnum
	FTObject; FTArray; FTUnion; FTComponent; FTSecret; FTStreamSelector
)

// Union is the tagged union ConfigDef cannot express: a const discriminator plus per-variant fields.
// It is what "auth: {method: oauth|basic|iam, ...}" needs, and it maps to JSON Schema oneOf + const.
type Union struct {
	Discriminator string // path of the field holding the tag
	Variants      []UnionVariant
}

type UnionVariant struct {
	Tag    string
	Title  string
	Fields []Field
}

// Predicate is a small, closed, browser-evaluable expression tree. Deliberately NOT a general
// embedded expression language: a language is a real dependency and a parser in two runtimes, and
// canal only needs equality, membership, presence and boolean combination.
type Predicate struct {
	Op    PredOp // Eq | Ne | In | Present | Absent | And | Or | Not | Truthy
	Path  string
	Value any
	Args  []*Predicate
}

// Constraint is a declarative offline check. Evaluated by core, exported to JSON Schema, and
// evaluated identically in the browser from the same data. R8: the shared constants are generated
// from one source, and here the "source" is the spec itself.
type Constraint struct {
	Kind ConstraintKind // Min | Max | MinLen | MaxLen | Pattern | OneOf | HostPort | URL | NonEmpty
	Arg  any
	Message string // i18n key
}

// ComponentRef makes a field's value another component. This is the mechanism behind recursive
// composition (§13): a "dead_letter" field whose value is a Sink, a "buffer" field whose value is a
// Buffer, a "transforms" array whose values are Transforms. Fan-out, routing, fallback, retry and
// DLQ then exist with zero core special-casing, and observability nests automatically because the
// engine knows the component tree.
type ComponentRef struct {
	Kind  Kind_    // which registry to draw from
	Multi bool     // a list of components rather than one
}

// FieldMapping is Segment's insight: a sink's field spec carries a DEFAULT EXTRACTION EXPRESSION over
// the generic record, so specialised sink UX is data.
type FieldMapping struct {
	Path      string // destination field
	Title     string
	Required  bool
	// Default is a record path expression: "$.value.user.email", "$.meta.tenant",
	// "$.change.after.id". A closed, browser-evaluable path grammar — not an expression language.
	Default   string
	AllowedKinds []Kind
}

// ---- The configured value ----

// Config is pre-parsed, pre-validated against the Spec, and pre-defaulted. There is no Configure()
// callback, and no connector ever re-parses a map. Secrets are already resolved.
type Config struct{ /* immutable tree */ }

// Get is the typed accessor. Generics used where they genuinely help: one function replaces the
// (T, error) accessor ladder that is Benthos's biggest ergonomic tax, and there is no error return
// because the value was already validated against the Spec — a missing or mistyped path is a
// PROGRAMMING error (the connector asked for a field it did not declare) and panics in tests via
// canaltest.
func Get[T any](c Config, path string) T

// GetOK is for genuinely optional fields with no default.
func GetOK[T any](c Config, path string) (T, bool)

// Decode fills a struct via `canal:"path"` tags. Most connectors use this once in Open and never
// touch Config again.
func (c Config) Decode(dst any) error

// Sub returns the config subtree for a component-valued field, and Component builds the component.
func (c Config) Sub(path string) Config
func (c Config) Component(path string) (any, error)

// Generation is the config generation, so every Condition can carry ObservedGeneration and the UI
// can answer "did my change take effect?" — the question Connect's status API structurally cannot.
func (c Config) Generation() int64

// ChoiceProvider is the optional hook behind Field.DynamicChoices.
type ChoiceProvider interface {
	Choices(ctx context.Context, hook string, partial Config) ([]Choice, error)
}

// Diagnostic is the per-field validation result. All errors at once, never fail-fast: a form needs
// every problem, and Airbyte's one-bool-one-string check is useless to a form.
type Diagnostic struct {
	Severity Severity // Error | Warning | Info
	Path     string   // "" for a whole-config diagnostic
	Code     string   // stable machine token, bounded vocabulary
	Message  string   // user-facing
	Detail   string   // developer-facing. Two audiences, two fields.
	DocsRef  string
}

// LintUnknown is emitted for a config key the Spec does not declare — a WARNING by default and an
// ERROR under strict mode, never a silent ignore.
const CodeLintUnknown = "lint.unknown_field"
```

---

## 8. Errors, retry, and the DLQ

```go
// Class is canal's CLOSED failure taxonomy: the seven-class ownership taxonomy from design-rules,
// plus not-connected and end-of-input. Closed because (a) it is a bounded metric label, (b) a closed
// set makes "a connector returns a hint the framework ignores" impossible — Benthos's ErrBackOff is
// honoured only on Connect — and (c) each member has exactly one prescribed engine action, asserted
// by a table-driven test.
//
// The axis is OWNERSHIP, because that is the question an operator UI actually needs answered: is this
// my config, their system, or a blip?
type Class uint8

const (
	ClassTransientUpstream Class = iota + 1 // their system, will probably recover. back off, retain progress.
	ClassTransientInternal                  // our side, will probably recover. back off, retain progress.
	ClassPermanentUpstream                  // their system rejects this permanently. terminal or DLQ.
	ClassPermanentMapping                   // this record cannot be mapped to the destination. DLQ.
	ClassPermanentContract                  // the connector or config violates canal's contract. terminal.
	ClassDuplicateSuccess                   // already applied. NOT an error: counts as delivered.
	ClassClockSkew                          // timestamps outside policy. clamp or reject per config.
	ClassNotConnected                       // no connection yet or lost. reconnect, do not consume retries.
	ClassEndOfInput                         // bounded work finished. NOT an error path; io.EOF carries it.
)

// Error is canal's error type. Connectors are expected to return it, and the engine unwraps with
// errors.As. It carries both audiences' text, because one string cannot serve an operator and a
// connector author.
type Error struct {
	Class Class
	Op    string // "read", "write", "commit", "plan", "chunk" — bounded vocabulary
	Connector string
	Stream    StreamRef
	Assignment AssignmentID
	RecordID  RecordID // zero when not record-scoped

	// RetrySafe is the ErrBadConn discipline, and it is the sharpest clause in this document:
	// a connector may report RetrySafe only when it KNOWS the effect did not land. Even if the
	// destination returned an error, if there is any possibility the operation was performed,
	// RetrySafe must be false. This is design rules R4 and R5 in one field, and canaltest asserts it.
	RetrySafe bool

	RetryAfter time.Duration // honour a Retry-After header; zero means "use the policy"

	UserMessage string // shown in the UI
	DevMessage  string // shown in logs and the DLQ record
	Err         error  // wrapped
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func ClassOf(err error) Class // errors.As, then Classifier, then ClassTransientInternal

// Sentinels. errors.Is, never ==, so connectors may wrap (the database/sql mandate).
var (
	ErrNotConnected       = &Error{Class: ClassNotConnected}
	ErrCapabilityDeclined = errors.New("canal: capability declined for this configuration")
	// Stream end is io.EOF, deliberately. It is the Go idiom and it costs no new vocabulary.
)

// RetryPolicy is per-class, and it is a TABLE, not a loop. Unbounded retry as the default is a named
// trap: Benthos livelocks on a poison record and had to flip the default in a major version.
type RetryPolicy struct {
	MaxAttempts int           // REQUIRED to be > 0. Zero is a config error, not "infinite".
	Backoff     Backoff       // full-jitter exponential, always
	MaxElapsed  time.Duration
	// Escalate is the last attempt under a DIFFERENT strategy — database/sql's retry() does N
	// attempts with cachedOrNewConn then one with alwaysNewConn. An escalation, not a loop.
	Escalate    EscalateAction // Reconnect | ReopenAssignment | Replan | None
	Terminal    Disposition    // Fail | DeadLetter | Drop
}

type Disposition uint8
const (
	DispFail Disposition = iota + 1 // pipeline → Failed. loudest, safest default for unknown classes.
	DispDeadLetter
	DispDrop // requires an explicit config acknowledgement; counted, never silent
)

// DeadLetter is the DLQ record. It works for SOURCES too — a record that cannot be decoded on the way
// in has nowhere to go in Connect, whose per-record reporter is sink-only.
type DeadLetter struct {
	Record     *Record // as it was at the point of failure
	Origin     Origin  // pre-transform coordinates, always available because Origin is immutable
	Class      Class
	Stage      string  // "decode" | "transform:2" | "encode" | "write" — the component path
	Attempts   int
	FirstSeen  time.Time
	LastError  string
	Detail     string
}

// After N consecutive backoffs the engine transitions the pipeline to Degraded (a Condition, not a
// Phase) with the last error string attached, so sustained backoff is VISIBLE rather than being an
// unusually quiet log.
```

---

## 9. Runtime: metrics the core names, signals the connector sends

```go
// Runtime is what a connector is handed at Open. It is the only way a connector talks to the
// platform, and it is deliberately narrow.
type Runtime interface {
	// Logger is pre-tagged with pipeline, connector, assignment and component path. A connector adds
	// attributes; it never sets those.
	Logger() *slog.Logger

	// Metric returns a handle for a CORE-DECLARED metric. The core owns naming, tagging and export;
	// a connector cannot name a metric, so cardinality cannot explode from a connector's choice.
	// MetricID is a closed enum, so this is enforced by the type system rather than by a string check.
	Counter(id MetricID) Counter
	Gauge(id MetricID) Gauge
	Histogram(id MetricID) Histogram

	// Signal is the in-band control channel: how a connector reports things that belong in the
	// STATUS DOCUMENT rather than in the metrics registry. Debezium's RowsScanned map keyed by table
	// name is the cautionary tale — fine as a JMX attribute, a cardinality catastrophe the moment an
	// exporter flattens it. Per-stream progress goes here; metrics stay low-cardinality.
	Signal(ctx context.Context, s Signal) error
}

// MetricID is the CLOSED metric vocabulary. One name per concept (R9).
type MetricID uint16

const (
	MRecordsRead MetricID = iota + 1
	MRecordsWritten
	MRecordsCommitted        // distinct from written. §3.2 of the observability dossier: committed
	MRecordsDeadLettered     // and written must be two different numbers, always.
	MRecordsDropped
	MBytesRead
	MBytesWritten
	MReadDuration
	MWriteDuration
	MCommitDuration
	MCheckpointAge           // the best single health metric in the system
	MQueueDepth
	MQueueCapacity
	MInFlightRecords
	MBackpressureSeconds
	MRetryAttempts
	MConnectAttempts
	MSourceLagRecords
	MSourceLagSeconds
	MAssignmentsAssigned
	MAssignmentsDone
	MSchemaChanges
	metricMax
)

// The closed label vocabulary. Labels are applied BY THE CORE. A connector cannot add one.
//   pipeline, connector, kind(source|sink), stream, assignment, class, outcome, component
// `stream` is bounded by the configured catalog, which is the only per-record-ish label allowed, and
// admission warns when the configured stream count exceeds a threshold.

// Signal is a SEALED set of in-band control/report messages.
type Signal interface{ signal() }

type (
	// SignalProgress is how a connector reports a denominator it knows and the engine does not.
	SignalProgress struct {
		Assignment AssignmentID
		Stream     StreamRef
		Units      int64
		UnitsDone  int64
		UnitsExact bool
		Label      string
	}

	// SignalCheckpointRequest asks for a commit boundary now — "I just rolled a file".
	SignalCheckpointRequest struct{ Reason string }

	// SignalSchemaChange is an ORDERED, in-band schema change. In-band because it must be ordered
	// with respect to the records around it, and because a schema epoch committed atomically with a
	// position is the only way a historical event can be decoded with its historical schema.
	SignalSchemaChange struct {
		Stream StreamRef
		Change SchemaChange
	}

	// SignalCatalogChanged tells the engine to re-run Discover and diff.
	SignalCatalogChanged struct{ Reason string }

	// SignalDegraded is a connector saying "I am working but unhappy" — rate limited, partial
	// permissions, replica lag. It becomes a Condition, not a log line.
	SignalDegraded struct{ Reason, Message string }
)
```

### 9.1 Status: phase plus conditions

```go
// Phase is exactly one coarse value, for a badge. Adopted from Kubernetes verbatim.
type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseStarting  Phase = "Starting"
	PhaseRunning   Phase = "Running"
	PhasePaused    Phase = "Paused"
	PhaseStopping  Phase = "Stopping"
	PhaseStopped   Phase = "Stopped"
	PhaseFailed    Phase = "Failed"
	PhaseCompleted Phase = "Completed" // Connect's missing state. A bounded pipeline finishes.
)

type ConditionStatus string
const (
	CondTrue    ConditionStatus = "True"
	CondFalse   ConditionStatus = "False"
	CondUnknown ConditionStatus = "Unknown" // mandatory, not a convenience
)

type Condition struct {
	Type               string // Configured|Connected|Progressing|CaughtUp|Degraded|Assigned|SchemaStable
	Status             ConditionStatus
	Reason             string // CamelCase machine token, bounded vocabulary
	Message            string
	LastTransitionTime time.Time
	ObservedGeneration int64
}

// The honesty contract, made machine-checkable: Connected:True must never imply Progressing:True.
// They are separate conditions with separate transition times, so the UI CANNOT collapse them, and a
// fixture test asserts that a healthy connection with a stalled commit renders as unhealthy.

// Progress is the per-assignment, per-stream progress the UI needs. Total and done are separate
// numbers and the consumer divides; Exact tells it whether it may.
type Progress struct {
	Assignment AssignmentID
	Stream     StreamRef
	Label      string // reporting only
	Bounded    bool
	Units      int64
	UnitsDone  int64
	UnitsExact bool
	Records    int64
	Bytes      int64
	StartedAt  time.Time
	Cursor     string // a redacted, human-readable rendering — never the raw blob
	Lag        *Lag
}
```

---

## 10. Registry

```go
// Registry is a VALUE TYPE with Clone/With/Without, and the global registry is just a default
// instance of it. That shape is what makes tests and sandboxing possible: a test builds a registry
// containing one fake source and one recording sink, with no global mutation and no init-order
// coupling — the thing the abandoned attempt's module-scope dedupe store got wrong.
type Registry struct{ /* immutable maps keyed by Spec.Name */ }

func NewRegistry() Registry

func (r Registry) With(f any) Registry     // f must implement one of the *Factory interfaces
func (r Registry) Without(name string) Registry
func (r Registry) Clone() Registry

func (r Registry) Source(name string) (SourceFactory, bool)
func (r Registry) Sink(name string) (SinkFactory, bool)
func (r Registry) Transform(name string) (TransformFactory, bool)
func (r Registry) Encoder(name string) (EncoderFactory, bool)
func (r Registry) Decoder(name string) (DecoderFactory, bool)
func (r Registry) Framer(name string) (FramerFactory, bool)
func (r Registry) Compressor(name string) (CompressorFactory, bool)

// Specs is the entire input the frontend needs to render a connector catalogue: every name, title,
// stability, field spec and declared capability mask, with no instance and no I/O.
func (r Registry) Specs() []Spec

// Default is the init-time registry. Register panics on a duplicate name and on a Spec that fails
// structural validation (a Field whose ShowIf references an undeclared path, a declared capability
// whose interface the factory type does not implement at the FACTORY tier, a Union whose
// discriminator is not a declared field).
//
// Panicking at init is correct for these: they are programming errors in a package the binary chose
// to link, discoverable by starting the binary once. Tier-dependent declaration mismatches (which
// need an instance) are checked at admission instead and surface as a Refusal.
var Default = NewRegistryPtr()

func Register(f any)      // delegates to Default
func Snapshot() Registry  // an immutable value copy of Default, taken once at pipeline build

// A connector package is, in its entirety:
//
//	package pg
//	func init() { canal.Register(sourceFactory{}) }
//
// There is no core file to edit, no enum to extend, no switch arm to add, and no import in core
// pointing at the connector. cmd/canal has a single blank-import block, which is the ONLY place in
// the tree that knows which connectors exist — and it is a main package, not core.
```

---

## 11. Admission: where silent downgrade goes to die

```go
package admit

// Request is everything admission needs. It runs before any connection is opened and before any
// worker is assigned, so an impossible pipeline is refused at SUBMIT TIME rather than at 3am.
type Request struct {
	Pipeline   canal.PipelineConfig
	Registry   canal.Registry
	SourceCaps canal.CapabilitySet // source tier
	ReaderCaps canal.CapabilitySet // reader tier, probed from a dry-run Reader on a probe assignment
	SinkCaps   canal.CapabilitySet
	WriterCaps canal.CapabilitySet
	Catalog    *canal.Catalog // may be nil when the source has no Discoverer
}

// Result is an IMMUTABLE document. If Refusals is non-empty the pipeline does not start, full stop.
type Result struct {
	Admitted bool
	Plan     canal.Plan
	Refusals []Refusal
	Downgrades []canal.Downgrade
	Warnings []canal.Diagnostic
	CapabilityReports []canal.CapabilitySet // stored verbatim, served to the UI
}

// Refusal names the requirement, the missing capability, the connector that lacks it, and the exact
// Go interface a connector author would implement to fix it. It is the single most useful error
// message canal produces.
type Refusal struct {
	Requirement Requirement
	Missing     []canal.CapabilityID
	Connector   string
	Iface       string // "canal.TwoPhaseWriter"
	Message     string // "delivery=exactly_once requires the sink to stage writes; s3.parquet
	                   //  implements neither canal.TwoPhaseWriter nor canal.TokenWriter"
	FixHint     string
	Path        string // the config path that asked for the impossible thing
}

// Requirement is the CLOSED set of things a pipeline configuration can demand. Each maps to a
// capability conjunction. Turning R4 from prose into a type is exactly this table.
type Requirement uint8

const (
	ReqResumeOnRestart Requirement = iota + 1 // CapPosition ∧ CapSeek
	ReqAtLeastOnce                            // ReqResumeOnRestart ∧ CapReplay ∧ sink AckPoint reached
	ReqEffectivelyOnce                        // ReqAtLeastOnce ∧ (CapIdempotent ∨ CapTwoPhase ∨ CapToken)
	ReqExactlyOnce                            // ReqAtLeastOnce ∧ (CapTwoPhase ∨ CapToken)
	ReqParallelBackfill                       // CapPlan ∧ CapChunk ∧ CapCompare ∧ CapSeek
	ReqResumableBackfill                      // CapChunk ∧ CapSeek  (the ChunkerCursor path)
	ReqBackfillProgress                       // CapBounded ∨ (CapChunk with exact Total)
	ReqSourceLag                              // CapLag
	ReqDynamicRepartition                     // CapReplan
	ReqSchemaEvolution                        // CapSchemaApply on the sink ∧ a drift policy != ignore
	ReqStreamSelection                        // CapDiscover
	ReqUpstreamAck                            // CapAck
	ReqPreflightValidation                    // CapValidate
)

// Downgrade is an operator-SIGNED, DURABLE record that a requirement was waived. This is the whole
// anti-silent-downgrade design in one type:
//
//   - The engine never waives anything on its own. Ever.
//   - A waiver exists only because the operator wrote `allow_downgrade: [exactly_once]` in the
//     pipeline config, naming the specific requirement.
//   - The waiver is persisted with the pipeline, stamped with who and when, and it raises
//     Degraded=True with Reason=CapabilityDowngraded for the pipeline's entire life.
//   - The UI renders the pipeline's effective guarantee, not the requested one, in fixed order:
//     what works, what does not, the qualifier, the next action.
type Downgrade struct {
	Requirement Requirement
	Requested   string // "exactly_once"
	Effective   string // "at_least_once"
	Missing     []canal.CapabilityID
	Connector   string
	AcknowledgedBy string
	AcknowledgedAt time.Time
	Reason      string
}

// Plan is the immutable, human-readable derivation of everything the capability sets implied. It is
// stored with the pipeline and served to the UI, and it is what makes DG-2 auditable: the operator can
// read which strategy they got and why.
type Plan struct {
	Generation int64
	Strategy   PlanStrategy
	StrategyWhy string // "source implements Planner+Chunkable+Comparable+Replayable"
	Delivery   canal.DeliveryClass
	DeliveryWhy string // "min(source=at_least_once, sink=exactly_once, requested=exactly_once)"
	AckPoint   canal.AckPoint
	ReadParallelism  int
	WriteParallelism int
	Batching   canal.BatchPolicy
	BatchingFrom string // "sink declared" | "config" | "core default"
	Codec      CodecPlan
	DriftPolicy DriftMode
	Defaults   []DefaultNote // R10: every default is LABELLED as a default, with its source
}

// PlanStrategy is core's OWN algorithm choice — the one enum in canal the engine legitimately
// switches on, because it is core's business and no connector supplies it. Four strategies, ordered
// from weakest to strongest; admission picks the strongest the capability set supports, unless the
// config pins one, in which case an unsupported pin is a Refusal.
type PlanStrategy uint8

const (
	// StrategyOpaque: no Planner, no Phased. One unbounded assignment. The connector owns everything
	// internally. Honest and fully supported; reported as SnapshotVisibility=none so the UI says
	// "this source manages its own snapshot; canal cannot report its progress".
	StrategyOpaque PlanStrategy = iota + 1

	// StrategyPhased: Phased present. One reader spans phases; the engine checkpoints at the
	// boundary and reports the phase label. Sequential, single-reader.
	StrategyPhased

	// StrategyPlanned: Planner + Seekable. Parallel bounded backfill assignments plus one unbounded
	// streaming assignment whose Resume was captured at plan time (the low watermark). Sequential
	// handoff: streaming starts when the last bounded assignment reports Done.
	StrategyPlanned

	// StrategyChunked: Planner + Chunkable + Comparable + Replayable. Canal's core chunked-snapshot
	// engine: LOW/HIGH/END watermarks, concurrent streaming during backfill, an INDEXED range filter
	// from day one (Flink CDC's original O(chunks)-per-record filter needed a binary-search retrofit),
	// and a PAGED finished-chunk set (Flink CDC's proved too large to ship in one message).
	StrategyChunked
)

// Admit is the whole function. It is pure: same inputs, same Result, no I/O. That makes it
// exhaustively table-testable, which is how "an impossible pipeline is refused at submit time"
// stops being an aspiration.
func Admit(req Request) Result
```

**The four things admission computes, and the exact conjunctions:**

| Derived | Conjunction | On failure |
|---|---|---|
| `Resumable` | `CapPosition ∧ CapSeek` | `resume_on_restart` refused; pipeline may still run from scratch if config says so |
| `Delivery` | `min(sourceClass, sinkClass, requested)` | `requested > computed` → Refusal naming both sides |
| `Strategy` | strongest supported ≤ pinned | pinned unsupported → Refusal |
| `AckPoint` | `Token > Commit > Flush > Write` | `AckOnFlush` with `exactly_once` → Refusal |

`min(...)` is never rounded up and never rounded silently down: `requested < computed` is allowed
(the operator asked for less), `requested > computed` is a Refusal, and the *effective* value is what
the UI shows.

---

## 12. The deployment seam: four interfaces, nothing else

```go
// These four interfaces are the ONLY difference between `canal run pipeline.yaml` on a laptop and a
// twenty-worker Kubernetes deployment. The connector-facing API — every type in §2 through §9 — is
// byte-identical in both, which is the correct seam and the one Kafka Connect got right.

// ConfigStore holds pipeline configuration. CAS, watchable.
type ConfigStore interface {
	Get(ctx context.Context, id string) (StoredConfig, error)
	Put(ctx context.Context, c StoredConfig, expectGeneration int64) (StoredConfig, error)
	List(ctx context.Context) ([]StoredConfig, error)
	Delete(ctx context.Context, id string, expectGeneration int64) error
	Watch(ctx context.Context) (<-chan ConfigEvent, error)
}

// CheckpointStore is bytes-in / bytes-out and NEVER sees a domain type. That property is exactly what
// made Connect's standalone/distributed swap free, and it is preserved verbatim.
//
// Set is ATOMIC ACROSS THE MAP: all assignment rows for one epoch land or none do. Non-negotiable —
// a partially written checkpoint is the KafkaConfigBackingStore failure mode whose own javadoc
// documents an unrecoverable state with "no obvious way to resolve the issue". A compacted log is
// therefore explicitly NOT a candidate implementation for canal's control-plane state.
type CheckpointStore interface {
	Load(ctx context.Context, pipeline string) (epoch int64, blobs map[string][]byte, err error)
	Set(ctx context.Context, pipeline string, epoch int64, blobs map[string][]byte,
		expectEpoch int64) error
	// LoadPage backs ChunkSetRef: the finished-chunk set is paged, not inlined.
	LoadPage(ctx context.Context, pipeline, key string, page int) ([]byte, error)
	// Edit is a first-class operator operation, not a database surgery ticket.
	Edit(ctx context.Context, pipeline string, mutate func(map[string][]byte) error) error
}

// Coordinator places assignments. The leader ONLY PLANS; workers hold LEASES.
//
// The critical property, and the reason this shape is worth its complexity: the data plane keeps
// flowing and keeps checkpointing with the ENTIRE control plane down. Leadership can never be trusted
// for correctness — the verified Kubernetes caveat is that leader election does not guarantee
// fencing — so the LEASE is the fencing token, and the plan is durable state rather than a leader's
// in-memory result.
type Coordinator interface {
	// Claim atomically takes ownership of assignments for this worker, returning fenced leases.
	Claim(ctx context.Context, worker string, want int) ([]Lease, error)
	// Renew extends leases. A worker that cannot renew must stop writing before the TTL expires.
	Renew(ctx context.Context, leases []Lease) ([]Lease, error)
	Release(ctx context.Context, leases []Lease) error
	// PutPlan is the leader's only write. Assignments become durable rows claimed by lease.
	PutPlan(ctx context.Context, pipeline string, plan []canal.Assignment, gen int64) error
	GetPlan(ctx context.Context, pipeline string) ([]canal.Assignment, int64, error)
	// Campaign is best-effort. Losing it costs planning, never correctness.
	Campaign(ctx context.Context, worker string) (<-chan bool, error)
}

// Lease is the fencing token. Every CheckpointStore.Set from a worker carries its lease token, and
// the store rejects a write from a superseded lease. That is how two workers cannot silently clobber
// each other — the exact failure the ack-graph-only designs (Benthos, Vector) cannot prevent.
type Lease struct {
	Assignment canal.AssignmentID
	Worker     string
	Token      int64 // monotonic; the fencing token
	Expires    time.Time
	Gen        int64
}

// StatusAggregator collects per-worker status into the pipeline read model the UI consumes.
type StatusAggregator interface {
	Report(ctx context.Context, r WorkerReport) error
	Pipeline(ctx context.Context, id string) (PipelineStatus, error)
	List(ctx context.Context) ([]PipelineStatus, error)
	Events(ctx context.Context, id string, since time.Time) ([]Event, error)
}

// Implementations:
//   singleNode: in-process. ConfigStore = a file, CheckpointStore = bbolt, Coordinator = "I own
//               everything, leases never expire", StatusAggregator = a struct in memory.
//               Zero external dependencies. `canal run pipeline.yaml` and nothing else.
//   postgres:   one schema, atomic multi-key writes, CAS on generation, SELECT ... FOR UPDATE
//               SKIP LOCKED for lease claiming. First and only cluster backend.
//
// NOT a candidate: a compacted log as the control-plane state machine. See CheckpointStore's comment.
```

---

## 13. Codecs, transforms, topology

```go
// Serialisation is THREE separately registered stages and connectors implement TRANSPORT ONLY. This
// is what makes "add a sink: three methods, register, done" literally true, and it is constraint #4
// applied to codecs as well as to connectors: N codecs × M connectors never multiplies.
type Encoder interface {
	Encode(ctx context.Context, r *Record, dst []byte) ([]byte, error)
}

type Decoder interface {
	// Decode turns one frame into ZERO OR MORE records. One-frame-to-many is in the signature, so a
	// CSV file, a JSON array or a multi-record protobuf frame needs no special case anywhere.
	Decode(ctx context.Context, frame []byte, dst *RecordBatch) error
}

// Framer is genuinely orthogonal to encoding, which is why it is a separate stage: a new transport
// gets every existing parser for free, and the source side becomes symmetric with the sink side.
type Framer interface {
	// Scan pulls the next frame. It carries an ACK AGGREGATOR so that N records from one frame
	// settle the frame exactly once — Benthos's scanner reduction, which is the piece that makes
	// framing compose with the ack graph instead of fighting it.
	Scan(ctx context.Context, r io.Reader) (frame []byte, ack AckFunc, err error)
}

type Compressor interface {
	Compress(ctx context.Context, in []byte) ([]byte, error)
	Decompress(ctx context.Context, in []byte) ([]byte, error)
}

// Each has a Factory with a Spec, registered exactly like a connector. A sink declaring
// StructuredWriter causes the engine to omit the Encoder stage — and to SAY SO in Plan.Codec.

// ---- Transforms ----

// Transform is one required method with the FULL return vocabulary, so expand, filter, regroup and
// mark-and-continue are all encoded in return values with no extra concepts.
type Transform interface {
	// Apply consumes in and appends to out. Dropping a record is appending nothing. Expanding is
	// appending several via Record.Derive. Filtering, regrouping and enrichment are all this method.
	//
	// A record that fails is reported per-record in the outcomes slice, keyed by RecordID, so a
	// batch-shaped transform never needs positional correlation.
	Apply(ctx context.Context, in *RecordBatch, out *RecordBatch) ([]RecordOutcome, error)
}

// Optional transform capabilities: Validator, Prober, Classifier (shared vocabulary), plus:
type StatefulTransform interface {
	// SnapshotState/RestoreState make a transform's state part of the checkpoint. Optional, because
	// most transforms are pure and should pay nothing.
	SnapshotState(ctx context.Context) (Blob, error)
	RestoreState(ctx context.Context, b Blob) error
}

// ---- Topology ----
//
// There is NO enumerated stage list anywhere in the contract (R1). The pipeline's outer shape is an
// implementation fact of internal/engine, and all variety comes from ComponentRef: a component whose
// config contains other components.
//
// A "fan-out sink" is a registered Sink whose config has a component-valued `sinks: []Sink` field.
// A "switch" is a Sink with `cases: []{when: Predicate, sink: Sink}`. A DLQ is a `dead_letter: Sink`
// field on any component. Retry-with-fallback is a Sink with `primary: Sink, fallback: Sink`. None of
// these needs one line of core code, and observability nests automatically because the engine knows
// the component tree from the spec and tags metrics with the component path.
//
// The one thing that needs sanctioning rather than punching a hole (Benthos's undocumented one):
type MetaComponent interface {
	// Children reports the sub-components this component owns, so the engine can build the tree,
	// nest metrics, propagate Quiesce/Close, and render the topology in the UI. A declaration, not
	// a reflection hack.
	Children() []ComponentHandle
}

// ---- Catalog and schema ----

type Catalog struct {
	Streams    []Stream
	Complete   bool   // false when discovery was truncated; the UI says so rather than lying
	DiscoveredAt time.Time
	Synthesised bool  // R10: true when the ENGINE invented this catalog because there is no
	                  // Discoverer. Labelled scaffolding, surfaced as such.
}

type Stream struct {
	Ref      StreamRef
	Schema   *Schema // nil when unknown until runtime
	Keys     [][]string
	Bounded  bool          // can this stream be fully scanned?
	Chunkable bool         // can it be split by key?
	CursorFields []string  // fields forming a monotonic cursor, if any
	Estimate *StreamEstimate // rows/bytes, exact or not; nil when there is no denominator
	Modes    ModeSet       // which per-stream modes this stream SUPPORTS
}

// ModeSet is the Airbyte-shaped operator model: capability is DECLARED per stream, selection is
// CONFIGURED per stream, and the two are validated against each other. Source-side and sink-side
// modes stay orthogonal, so M × N is free and canal needs no pipeline "type" at all.
type ModeSet struct {
	Read  []ReadMode  // FullRefresh | Incremental | FullThenIncremental | CDC
	Write []WriteMode // Append | Upsert | Overwrite | SoftDelete
}

type SchemaChange struct {
	Stream StreamRef
	Kind   ChangeKind // AddColumn|DropColumn|RenameColumn|WidenType|NarrowType|AddStream|DropStream|
	                  // ChangeKey|TruncateStream
	Epoch  int64      // committed ATOMICALLY with the position, so a historical event is decodable
	Before *Schema
	After  *Schema
}

// DriftMode is the five-mode policy, adopted wholesale — it is the only complete shipped answer.
type DriftMode uint8

const (
	DriftLenient DriftMode = iota + 1 // DEFAULT. Never destructive: add columns, widen types,
	                                  // ignore drops. The safe direction.
	DriftEvolve                       // apply everything the sink declares it supports
	DriftTryEvolve                    // apply what is supported, warn and continue on the rest
	DriftException                    // stop the pipeline on any change
	DriftIgnore                       // pass records through unchanged; requires acknowledgement
)
```

---

## 14. Walkthroughs

### (a) A trivial source and a trivial sink move one record

**The connector author's entire source, verbatim:**

```go
package httppoll

import "github.com/BernardoCSACarreira/canal"
import "github.com/BernardoCSACarreira/canal/canalkit"

func init() { canal.Register(factory{}) }

type factory struct{}

func (factory) Spec() canal.Spec {
	return canal.Spec{
		Name: "http.poll", Kind: canal.KindSource, Title: "http.poll.title",
		Stability: canal.StabilityBeta,
		Fields: []canal.Field{
			{Path: "url", Type: canal.FTString, Required: true,
				Constraints: []canal.Constraint{{Kind: canal.ConstraintURL}}},
			{Path: "interval", Type: canal.FTDuration, Default: 30 * time.Second},
			{Path: "token", Type: canal.FTSecret, Secret: true},
		},
		// Declares NOTHING. No optional capability at all.
	}
}

func (factory) Open(cfg canal.Config) (canal.Source, error) {
	return &src{url: canal.Get[string](cfg, "url"), every: canal.Get[time.Duration](cfg, "interval")}, nil
}

type src struct{ url string; every time.Duration }

func (s *src) Reader(ctx context.Context, req canal.OpenRequest) (canal.Reader, error) {
	return canalkit.ReaderFunc(func(ctx context.Context, dst *canal.RecordBatch) error {
		body, err := s.fetch(ctx)
		if err != nil { return err }
		dst.Append(canalkit.BytesRecord(canal.StreamRef{Name: "poll"}, body))
		return nil
	}), nil
}
```

`canalkit.ReaderFunc` supplies `Close` as a no-op. So the *literal* implementation cost is `Spec`,
`Open`, `Reader` and one closure — three methods and a function, exactly the angle's claim. The sink
is symmetric: `Spec`, `Open`, `Writer`, and a closure returning `canal.AllWritten(b.Len())`.

**What the engine does, step by step:**

1. `canal.Snapshot()` → immutable `Registry`. `registry.Source("http.poll")` → the factory.
2. `factory.Spec()` is validated structurally, then the submitted YAML is parsed against it,
   defaulted (`interval=30s`), secrets resolved, and frozen into a `Config` with `Generation: 1`.
3. `factory.Open(cfg)` → `*src`. No network yet, by contract.
4. `capability.Probe(src, TierSource, spec.SourceCaps, 0, nil)` walks all 29 table rows. Every
   `Probe` returns false. The resulting `CapabilitySet` has `Mask == 0` and **29 entries, every one
   with `Present: false`, `Source: CapSrcAbsent`, and a `Reason`** from the row's `AbsentReason`.
   Derived fields: `Resumable: false`, `Chunkable: false`, `ProgressKnown: false`,
   `Delivery: AtMostOnce`.
5. Same for the sink. The sink implements no `Flusher`, so `AckPoint: AckOnWrite` — the *strict*
   reading, arrived at by default (R4: the default is the safe direction).
6. `admit.Admit`: the config requested nothing, so `Required = {}`. `Refusals` is empty.
   `Delivery = min(AtMostOnce, AtLeastOnce, unspecified→best-effort) = AtMostOnce`.
   `Strategy = StrategyOpaque` because neither `Planner` nor `Phased` was probed;
   `StrategyWhy: "source implements neither canal.Planner nor canal.Phased"`.
   `Plan.Defaults` records that batching came from `core default` and parallelism from
   `core default (1: source declares no Planner)`.
7. The engine synthesises one `Assignment{ID:{Stream:{Name:"poll"}}, Bounded:false, Label:"default
   (synthesised: source declares no Planner)"}` and one write partition.
8. Read loop: `Reader(ctx, OpenRequest{...})`, then `Read(ctx, batch)`. One record appended. The
   engine assigns `RecordID: 1` and sets `origin = Origin{Assignment: …, Seq: 1, Cursor: {}, ReadAt:
   now}` — note `Cursor` is empty, and *that* empty cursor is why `Resumable` is false; it is one
   fact, not two.
9. Decoder stage: the pipeline's configured `Decoder` (JSON by default) is *not* invoked, because the
   record already has a bytes payload and no transform asked for `Structured()`. Lazy by design.
10. Batcher: `BatchPolicy` from `core default` — `MaxRecords: 500, MaxAge: 1s`. The single record
    ages out after 1s and the batch is closed.
11. `Writer.Write(ctx, batch)` → `AllWritten(1)`, `Durable: true`.
12. Ack graph: record 1 reaches terminal status `OutcomeWritten`. `MRecordsWritten` and
    `MRecordsCommitted` both increment — and they are *separate metrics* even here, so the fact that
    they are equal is an observation rather than an assumption.
13. Checkpoint: the prefix resolver produces `AssignmentState{Resume: Cursor{}}`. **The engine writes
    it anyway**, with `Records: 1`, so the UI can show throughput. Because `Resume` is empty, restart
    starts over — and the status document says `Resumable: false — the source does not implement
    canal.Positioner`, in that exact sentence, before the operator ever runs it.
14. The batch is recycled only now, after terminal disposition. The `Reader` never saw it in flight.

The proof obligation this discharges: **five methods, no optional interfaces, no core edits, and the
one honest weakness (no resumability) is a rendered sentence rather than a surprise.**

---

### (b) Full initial scan, then incremental streaming, crashing halfway and resuming

The source implements `Planner`, `Seekable`, `Chunkable`, `Comparable`, `Replayable`, `Positioner`,
`Bounded`, `Discoverer`, `Validator`, `LagReporter`. That is nine optional interfaces on top of the
five required methods — the angle's "Debezium-class source implements 9 more" claim, concretely.

**Admission.** `capability.Probe` yields `Chunkable: true` (`CapChunk ∧ CapCompare ∧ CapReplay`) and
`Resumable: true` (`CapPosition ∧ CapSeek`). `admit.Admit` selects `StrategyChunked` with
`StrategyWhy: "source implements Planner+Chunkable+Comparable+Replayable"`, `Delivery:
EffectivelyOnce` (`sink declares canal.Idempotent`), `ReadParallelism: 20` (config cap), and
`ReqBackfillProgress` satisfied via the chunk iterator's exact `Total`.

**Planning.**

1. Engine calls `bound.plan(ctx, PlanRequest{Catalog, Parallelism: 20, Resume: nil})`.
2. The connector returns a `PlanResult` containing:
   - one **unbounded** assignment for the log tail, with `Resume` set to the position it read *now* —
     the **low watermark**. This is the handoff invariant, and the *core* owns it: admission asserts
     that a `StrategyChunked` plan contains exactly one unbounded assignment per stream and that its
     `Resume` is non-empty. A connector that returns an empty one is a `PermanentContract` refusal.
   - `Chunkable: true` for `public.events`, so the engine — not the connector — drives chunking.
3. Engine calls `bound.chunks(ctx, ChunkRequest{Stream: events, Target: 100_000, Resume: Blob{}})`,
   gets a `ChunkIter`, and pulls 400 `KeyRange`s, minting 400 **bounded** assignments with
   `Range` set and `Label: "backfill chunk N/400"`.
4. `Coordinator.PutPlan` writes 401 durable assignment rows. `ChunkIter.Cursor()` is stored as
   `ChunkerCursor` after every page, so the *slicing itself* is resumable — the thing Airbyte needed
   a protocol change for and Benthos still cannot do.
5. Workers `Claim` leases. 20 backfill chunks run concurrently. The log-tail assignment **also runs
   concurrently**, from the low watermark, buffering into the pipeline; its records for
   `public.events` pass through the engine's **indexed range filter**: for each streamed record, a
   binary search over the finished-chunk ranges (built from `bound.compareKeys`) decides whether the
   backfill has already covered that key. Indexed from day one, because Flink CDC's original
   O(chunks)-per-record filter needed a binary-search retrofit.

**Crash at 47%.** `kill -9` with 188 chunks `Done`, 20 in flight, the tail assignment 3 minutes
behind.

What is durable at that instant: the 401 assignment rows (from `PutPlan`); per-assignment
`AssignmentState` rows for the 188 finished chunks (`Done: true`) and for the 20 in-flight ones, each
with a `Resume` cursor equal to *the end of its own longest contiguous committed prefix*; the tail
assignment's `Resume`; and `ChunkerCursor`. The 20 in-flight chunks' cursors are correct because the
prefix resolver advances a position only when the records before it are durable at the sink — never
on a timer. Kafka Connect's 60-second wall-clock flush would here re-emit fully-acked chunks and
print KAFKA-4942's log line; canal cannot, because there is no clock in the ack path.

**Resume.**

1. `CheckpointStore.Load` → epoch, blobs. `Coordinator.GetPlan` → the same 401 assignments, same
   `Gen`. **No replanning.** Split ownership and progress transfer together because they are rows in
   one store.
2. Engine skips the 188 `Done: true` assignments entirely. Their records are never re-read.
3. For each of the 20 partial chunks: `Source.Reader(ctx, OpenRequest{Assignment: {ID, Bounded: true,
   Range: <same range>, Resume: <its own cursor>, Params: <verbatim>}})`. Because `Seekable` is
   declared, the reader resumes strictly after the cursor. Because `Range` is the *same struct* that
   was persisted, and because the resume payload and the construction payload are the same type,
   there is nothing to re-derive and nothing that can drift.
4. The tail assignment resumes from its own cursor, 3 minutes back. `Replayable.ReplayWindow()` is
   checked against `MCheckpointAge`: if the age exceeded the window, admission-on-restart raises a
   `Refusal` — *"the source guarantees 6h of replay; this checkpoint is 9h old"* — rather than
   silently starting a lossy stream.
5. The finished-chunk set is re-read from `ChunkSetRef` (paged), and the range filter is rebuilt.
6. Chunking continues from `ChunkerCursor`: chunks 401+ are enumerated without re-slicing the first
   400.
7. As each remaining chunk completes, its range **retires from the filter**, so steady-state cost
   returns to zero.
8. When the last bounded assignment reports `Done`, the engine emits `Phase: streaming` into the
   checkpoint header, records a `PhaseBoundary` event, and — crucially — *nothing about control flow
   changed*. The engine did not read `Phase`. It observed that no unbounded-false assignment remained
   undone. Phase is a label on a fact, which is precisely why canal keeps both progress reporting and
   re-parallelised resume that Connect/Debezium and Airbyte each lost.
9. `Progressing` and `CaughtUp` become separate conditions: `CaughtUp: True` only once
   `LagReporter.Lag()` is inside the objective. "Is the initial load finished *and* has the stream
   drained the backlog it accumulated during it" is answerable, in one field.

**And if the source only implements `Phased`?** Then `StrategyPhased` runs instead: one reader, the
engine calls `bound.hasNextPhase` at `io.EOF` and `bound.nextPhase` to advance, checkpointing at the
boundary. Progress is reportable only if `Bounded.Remaining` is available. The operator sees
`Strategy: Phased`, `StrategyWhy: "source implements canal.Phased but not canal.Chunkable"`, and
`Chunk: absent — would enable: parallel backfill, mid-backfill resume, backfill ETA`. **No core code
differs between the two cases beyond which of four strategy constructors ran.**

---

### (c) A sink fails mid-batch; the pipeline recovers without loss

Batch of 500 records, `RecordID` 1041–1540, from one assignment, `Seq` 1041–1540 contiguous. Sink is
a REST destination: `Idempotent`, `BatchPolicyProvider`, no `Flusher`, so `AckPoint: AckOnWrite` and
`Delivery: EffectivelyOnce`.

1. `Writer.Write(ctx, batch)` returns

```go
&canal.WriteResult{
    Accepted: 342,
    Durable:  true,
    Outcomes: []canal.RecordOutcome{
        {ID: 1383, Status: canal.OutcomeRejected,  Err: mappingErr, Detail: "field 'amount': not a number"},
        {ID: 1384, Status: canal.OutcomeRetriable, Err: throttleErr},
        // ... 1384–1540 all Retriable: the destination 429'd mid-request
        {ID: 1402, Status: canal.OutcomeDuplicate},
    },
}
```

Note what is *not* happening: no positional correlation, no sorting, no "the first 342 succeeded".
Every outcome is keyed by `RecordID`, which is why a batch-shaped transform upstream could have
expanded, filtered and reordered these records and this result would still be interpretable. That is
the direct structural fix for the Benthos `WalkMessages` scar.

2. The engine partitions by `OutcomeStatus` using the closed table — one action per member, no
   connector-supplied hint anywhere:
   - `OutcomeWritten` (342) and `OutcomeDuplicate` (1402): terminal success. `MRecordsWritten +=
     343`. `ClassDuplicateSuccess` counts as delivered, not as an error — R5's "duplicate must mean
     already durably stored".
   - `1383` `OutcomeRejected` with `ClassPermanentMapping`: policy for that class is
     `Terminal: DispDeadLetter`. A `DeadLetter{Record, Origin, Class, Stage: "write", Detail}` is
     written *durably* before the record is considered terminal, and `MRecordsDeadLettered++`.
     `Origin` is intact and pre-transform, because `Origin` is unexported and no transform could have
     touched it.
   - `1384–1540` (157) `OutcomeRetriable` with `ClassTransientUpstream` and `RetryAfter: 30s` from the
     destination's header: re-offered as a fresh batch after the honoured delay. `MRetryAttempts++`.
3. **The prefix resolver is the whole answer to "without loss."** For this assignment the committed
   set is now `{…1041..1382, 1402}`. The longest contiguous committed prefix ends at `Seq 1382`, so
   `AssignmentState.Resume` advances to record 1382's cursor and **not one record further**. 1402's
   success is remembered in memory but cannot advance the position past the 157 outstanding records.
   1383 is dead-lettered, which counts as terminal, so once 1384 commits the prefix jumps.
4. Retry batch: 5 of the 157 come back `OutcomeRejected` on attempt 3. `RetryPolicy` for
   `ClassTransientUpstream` is `{MaxAttempts: 5, Backoff: fullJitterExp, Escalate: Reconnect,
   Terminal: DispDeadLetter}` — the 5th attempt happens *after* a reconnect, a different strategy for
   the last attempt rather than a repeat of the same one. They dead-letter. `Degraded: True,
   Reason: SustainedBackoff` is raised with the last error string, so 30 seconds of throttling is
   *visible*, not quiet.
5. All 500 terminal. Prefix advances to 1540. Checkpoint epoch N+1 written atomically:
   `AssignmentState{Resume: cursor(1540), Records: +500}`, `Stats.RecordsCommitted += 343+152`.
   `Acknowledger.Ack(ctx, AckRequest{Through: cursor(1540), DeadLettered: [1383, +5]})` — so the
   *source's* upstream also learns which records were dead-lettered rather than delivered, which
   Connect's sink-only reporter cannot express.
6. **Crash between step 4 and step 5.** Restart resumes from `cursor(1382)` — the last durable
   prefix. Records 1383–1540 are re-read. 1383 is re-attempted and re-dead-lettered (idempotently,
   keyed on `Origin.Assignment + Origin.Seq`, which is stable across restarts unlike `RecordID`).
   1384–1540 are re-written; the sink is `Idempotent`, so the 152 that had landed are no-ops and come
   back `OutcomeDuplicate`. **No loss, and no duplicates at the destination**, which is exactly what
   `EffectivelyOnce` means and exactly what admission promised.
7. **If the sink were not `Idempotent`,** admission would have computed `Delivery: AtLeastOnce`, the
   plan document would say `DeliveryWhy: "min(source=effectively_once, sink=at_least_once,
   requested=effectively_once)"` → and because `requested > computed`, the pipeline would have been
   **refused at submit time** with `Refusal{Requirement: ReqEffectivelyOnce, Iface:
   "canal.Idempotent", Message: "…rest.webhook implements neither canal.Idempotent, canal.TwoPhaseWriter
   nor canal.TokenWriter"}`. Not a quieter guarantee at 3am. That is DG-2.
8. **If the sink implemented `TwoPhaseWriter`** the flow differs only in the commit step:
   `bound.prepare` at the checkpoint boundary returns a handle stored *inside* the checkpoint;
   `bound.commit` runs after the checkpoint is durable; a crash between them is repaired by the next
   checkpoint's commit, because `Commit` must tolerate handles from epochs ≤ N (the Subsuming
   Contract). No core code above the commit step changes.

---

### (d) Serving the frontend with zero connector-specific code in core

The frontend consumes exactly four documents. Every one is produced by core from connector-declared
*data*, and none has a per-connector code path.

**1. `GET /v1/connectors` → `[]Spec` (no instance, no config, no I/O).**

`Registry.Specs()`. The form renderer walks `Spec.Fields`:

- `Field.Type` picks the widget. `FTUnion` renders a discriminator select plus the matched variant's
  fields — the tagged union `ConfigDef` cannot express and had to fake with dotted prefixes.
- `Field.ShowIf` is a `Predicate`: a closed, ~9-operator tree the browser evaluates on every keystroke
  with no round-trip. This is why `Predicate` is deliberately not a general expression language —
  the browser must be able to evaluate it, and a language means a parser in two runtimes.
- `Field.Secret` ⇒ password input, and core redacts the value in every other document. No connector
  code participates in redaction.
- `Field.DynamicChoices` is a *name*; the form calls
  `POST /v1/connectors/{name}/choices/{hook}` with the partial config and gets `[]Choice`.
- `Field.Constraints` are rendered as inline client-side validation *and* exported by
  `GET /v1/connectors/{name}/schema.json` as JSON Schema, so the CLI, Terraform and editor completion
  validate against the same one artefact (R8: shared constants generated from one source).
- `Field.Component` ⇒ a nested component picker filtered by `Kind`, which is how a `dead_letter:`
  field, a `buffer:` field or a `transforms: []` array renders recursively with no core knowledge
  that those fields exist.
- `Spec.Stability` renders the labelled-scaffolding badge (R10), asserted in a fixture test.

**2. `POST /v1/pipelines/{id}/dryrun` → `admit.Result`.** This is the capability document, and it is
the frontend's most interesting page. It contains four `CapabilitySet`s, and each has *every*
applicable entry, so the UI renders a complete matrix:

```
Source  postgres.cdc                          Sink  s3.parquet
  ✓ discover     probed                         ✓ batch_policy   probed
  ✓ plan         probed                         ✓ flush          probed  → AckPoint: OnFlush
  ✓ chunk        probed                         ✗ two_phase      absent
  ✓ compare      probed                             s3.parquet does not implement
  ✓ seek         probed  (cursor v2)                canal.TwoPhaseWriter, so a batch cannot be
  ✓ position     probed                             staged and committed atomically
  ✓ replay       probed  (window 6h)                would enable: exactly-once delivery,
  ✗ lag          masked                             cross-batch atomic commit
      the configured user lacks             ✗ idempotent     absent
      pg_read_all_stats, so replication         s3.parquet does not implement canal.Idempotent
      lag cannot be measured                    would enable: effectively-once delivery
      would enable: source lag metric,      … 12 more
      CaughtUp condition
  … 6 more
```

Every ✗ line has a sentence. The `lag` line is a *masked* capability: the connector implements
`LagReporter` but returned a `CapabilityNote` from `Capabilities(ctx)` because this config cannot use
it. Core did not invent that sentence and does not know what `pg_read_all_stats` is. **This is the
answer to "how does a UI learn what the engine found": it reads the reified set, and the reified set
is complete by construction — 29 entries, always, absence explained, always.**

The same document carries `Plan` (`Strategy`, `StrategyWhy`, `Delivery`, `DeliveryWhy`, `AckPoint`,
parallelism, `Defaults[]` each labelled with its source), `Refusals[]` and `Downgrades[]`. The UI
renders, in the fixed order the design rules mandate: what works, what does not, the qualifier, the
next action — where "the next action" is literally `Refusal.FixHint`.

**3. `GET /v1/pipelines/{id}/status` → `PipelineStatus`.**

```go
type PipelineStatus struct {
	ID, Name   string
	Generation int64
	Phase      Phase        // one badge
	Conditions []Condition  // Configured|Connected|Progressing|CaughtUp|Degraded|Assigned|SchemaStable
	Plan       Plan
	Delivery   DeliveryClass // EFFECTIVE, never requested
	Downgrades []Downgrade
	Assignments []Progress   // per assignment: Bounded, Units/UnitsDone/UnitsExact, Label, Lag
	Streams    []StreamProgress
	Checkpoint CheckpointSummary // Epoch, CreatedAt, Age, RecordsCommitted
	Workers    []WorkerSummary
	Events     []Event          // schema drift, phase boundary, downgrade, degraded transitions
	LastError  *ErrorSummary    // Class, UserMessage, when, count
}
```

`Connected: True` and `Progressing: False` are structurally distinguishable, so a probe returning 200
cannot render as healthy — the machine-readable form of "a metrics UI that cannot distinguish *the
endpoint answered* from *your data arrived* is actively misleading". A pinned fixture asserts exactly
that rendering (deterministic read-model fixtures, from the design rules' carry-forward list).

Progress: `Units`/`UnitsDone`/`UnitsExact` are three fields and never a pre-divided ratio, so
"12,481 records so far, total unknown" is expressible and a bar stuck at 0% is not. Per-stream
progress lives *here*, in the status document, not in the metrics registry — the Debezium
`RowsScanned`-map-flattened-by-an-exporter cardinality trap, avoided by construction because
`Runtime.Signal(SignalProgress{})` and `Runtime.Counter(MetricID)` are different methods with
different destinations.

**4. `GET /metrics` → Prometheus, closed vocabulary.** Names come from `MetricID`, labels from core's
closed label set. A connector *cannot* name a metric because `Counter` takes a `MetricID`, not a
string — the type system enforces what every other framework enforces with a code review.

**The zero-connector-code claim, audited.** `internal/api` imports `canal`, `admit`, `store`,
`coord`, `status`. It imports no connector package and has no map from connector name to anything.
`web/` contains no connector name. The only per-connector artefact anywhere is
`Spec` — data, returned by a pure function, versioned, and covered by a golden test per connector via
`canaltest.GoldenCapabilityReport`. A specialised UX for a less-generic sink ships as `Spec.Mappings`
plus richer `Field` metadata: still data, still zero core changes, exactly what constraint #1's second
sentence demands.

---

### (e) The same pipeline standalone, then horizontally scaled

**Standalone.** `canal run pipeline.yaml`:

```go
eng := engine.New(engine.Deps{
    Registry:   canal.Snapshot(),
    Config:     store.NewFileConfig("pipeline.yaml"),
    Checkpoint: store.NewBolt("./canal.db"),
    Coord:      coord.NewSingle(),   // "I own everything; leases never expire"
    Status:     status.NewMemory(),
})
```

`coord.NewSingle()` is ~60 lines: `Claim` returns leases for every assignment with `Expires:
time.Time{}` (never), `Campaign` immediately signals leader, `PutPlan`/`GetPlan` write to the same
bbolt file. One process, one binary, no external dependency, `kill -9`-durable because bbolt is.

**Scaled.** Same binary, same connectors, same pipeline YAML:

```go
eng := engine.New(engine.Deps{
    Registry:   canal.Snapshot(),
    Config:     store.NewPostgresConfig(pool),
    Checkpoint: store.NewPostgresCheckpoint(pool),
    Coord:      coord.NewPostgres(pool),
    Status:     status.NewPostgres(pool),
})
```

**What differs: exactly those four constructor arguments.** Not one type in §2–§9 changes. Not one
connector method signature changes. No connector learns which mode it is in — there is no
`Runtime.IsDistributed()`, deliberately, because the moment that exists a connector will branch on it
and the seam is gone. The connector-facing API is byte-identical in both modes, which is the property
Connect got right and is worth copying wholesale.

**What actually happens when you scale to 20 workers:**

1. Each worker starts, `Campaign`s. One wins; the others proceed to step 3 regardless.
2. The leader runs `admit.Admit` (pure), calls `bound.plan` and `bound.chunks`, and `PutPlan`s 401
   durable assignment rows with `Gen: 7`. **That is the leader's only job.** It does not assign, it
   does not hold in-memory placement state, it does not decide where anything runs.
3. Every worker (leader included) loops: `Claim(ctx, workerID, want: capacity - held)`. Postgres
   `SELECT … FOR UPDATE SKIP LOCKED` hands out unclaimed rows. Pull-based assignment gives load
   balancing with **no load model** and kills rebalance storms, because there is no global
   stop-the-world rebalance to storm — a worker takes what it can carry.
4. Each claimed assignment gets `Source.Reader(ctx, OpenRequest{Assignment: <the row>})`. The worker
   holds a `Lease{Token: 8811}`. Every `CheckpointStore.Set` carries that token, and the store rejects
   a superseded token. **The lease is the fencing token**, not leadership — because the verified
   Kubernetes caveat is that leader election does not guarantee fencing, so leadership can never be
   trusted for correctness.
5. **Kill the leader.** Nothing stops. Workers keep reading, writing, and checkpointing, because a
   checkpoint write needs a lease and a store, not a leader. What is lost is *replanning*: no new
   chunk pages are enumerated and no reassignment of a dead worker's leases happens until a new
   leader wins. `Assigned: Unknown, Reason: CoordinatorUnreachable` appears in the conditions. This
   is the single most valuable deployment property canal can have and it is worth the CAS store.
6. **Kill a worker.** Its leases expire after TTL. `Claim` on other workers picks them up, each
   resuming from that assignment's own `Resume` cursor. Unfinished work returns to the pool
   automatically because the *plan* is durable rows rather than a leader's in-memory result. There is
   no task restart, no re-splitting, no stop-the-world.
7. **`Replanner` under load.** When the last backfill chunk completes, `bound.nextPlan` returns a plan
   with 400 fewer assignments and 1 unbounded one. Workers `Release` what vanished and `Claim` what
   appeared. "Snapshot with 20 workers then stream with 2" is *this*, and it needs no interface
   change — the sentence that is literally inexpressible in Connect.
8. **Scaling back down to standalone.** Point the same binary at bbolt with `coord.NewSingle()` and
   the *same* `CheckpointStore` contents migrated as opaque bytes. The store is bytes-in/bytes-out and
   never saw a domain type, so the migration is a copy. That property is precisely what made
   Connect's standalone/distributed swap free, and canal keeps it deliberately.
9. **Exactly-once is not distributed-only.** `TwoPhaseWriter` staging lives inside the checkpoint,
   which both `CheckpointStore` implementations support atomically. Connect's exactly-once being
   distributed-mode-only is a consequence of it living in a Kafka transaction; canal has no such
   coupling.

---

## 15. Flow control, stated explicitly

```go
// Every edge in the engine is a bounded channel. Unbounded growth is inexpressible: RecordBatch
// cannot grow, and every queue has both a record cap and a byte cap because either can bind.
//
// ONE framework-owned in-flight concept, per assignment, replaces Benthos's five knobs with three
// user-facing names and a documented deadlock between two of them:
//
//	max_in_flight_records / max_in_flight_bytes   per assignment, framework-enforced
//
// Buffer is a PLUGGABLE STAGE whose when_full is in the type, so "what happens when it fills" is a
// value the operator chose rather than a behaviour they discovered. Deep buffering is a named trap:
// Flink spent two major features (unaligned checkpoints, buffer debloating) undoing an early default,
// so canal's defaults are small and the subsystem those features exist to fix never gets built.
type Buffer interface {
	// Push blocks, rejects or overflows according to the configured WhenFull. It returns the
	// disposition so the caller can count it — a drop is always counted, never silent.
	Push(ctx context.Context, b *RecordBatch) (Disposition2, error)
	Pop(ctx context.Context, dst *RecordBatch) error
	Depth() (records, bytes int)
	Capacity() (records, bytes int)
	Close(ctx context.Context) error
}

type WhenFull uint8
const (
	WhenFullBlock WhenFull = iota + 1 // DEFAULT. Backpressure is transitive with no protocol.
	WhenFullReject                    // R6: rejection is an expressible outcome
	WhenFullDropOldest                // preserves the acked prefix; counted
	WhenFullDropNewest                // preserves the acked prefix; counted
)

// Batcher and Splitter are framework-owned and goroutine-free from the connector's point of view: a
// sink declares a BatchPolicy and the engine enforces it. The inverse (one record → many) is the
// Decoder's one-frame-to-many signature.
//
// The four numbers that make backpressure diagnosable, all core-named metrics:
//   MQueueDepth, MQueueCapacity, MBackpressureSeconds, MInFlightRecords — per component path.
// One place to look when a pipeline is slow, which is what Benthos's five mechanisms cost it.
```

---

## 16. Decisions taken

| id | Choice | Why, in this design |
|---|---|---|
| **record-envelope** | Domain-free envelope + separately addressable `Meta` (with `SetSecret`) + dual-view `Payload` + framework-assigned `RecordID` + **optional typed `Change` facet** + **unexported immutable `Origin`** | Follows the distiller. The facet is *data*, so the core never switches on source type and constraint #1 holds; total genericity provably fails because CDC dialects diverge per connector. `Origin` unexported is the structural fix for KIP-793 — a transform *cannot* corrupt checkpoint identity because it cannot reach the field. `RecordID` is mandatory because Benthos's positional identity is deprecated-as-harmful by its own author. `Value` is a **sealed sum type**, not `any` and not a type parameter: a type parameter that must be erased at the registry boundary buys nothing. |
| **serialization-boundary** | **Vector's three stages** — `Encoder`/`Decoder` + `Framer` + `Compressor`, separately registered; connectors implement transport only. Plus Benthos's ack-aggregating scanner in `Framer.Scan` and a declared `StructuredWriter` escape hatch | This is the property that makes "add a sink: three methods, register, done" literally true. N codecs × M connectors never multiplies. `Decoder`'s one-frame-to-many is in the signature. The escape hatch is *declared*, so a skipped encoder stage appears in `Plan.Codec` instead of being an accident. |
| **checkpoint-representation** | **Opaque-with-a-typed-header**, decomposed **per assignment**. Core reads the header (epoch, phase-for-reporting, counts, `Done`); payloads are versioned `Blob`s. Core owns the contiguous-prefix resolver, per assignment | Full opacity costs lag and progress, which Connect admits and which is disqualifying given the frontend goal. Full structure costs the keyhole and wire-shippability. Per-assignment decomposition is why canal needs no merge-patch semantics: 4,000 partitions are 4,000 small rows, and only the dirty ones are rewritten. `ChunkerCursor` is a first-class field because "a snapshot has no state" is the most expensive economy in this space. |
| **commit-protocol** | **Position-carrying acks with the sink's nil return as the ack**, core-owned position mapping, plus **two opt-in tiers**: `TwoPhaseWriter` (Flink's Subsuming Contract verbatim) and `TokenWriter` (one durability domain). `AckPoint` is reified, and `CheckpointRequester` lets a sink ask for a boundary | The tier *is* which interfaces a connector implements, so admission computes `min(source, sink, requested)` and refuses the impossible at submit time. That closes Vector's most dangerous silent degradation and turns R4 from prose into a type. There is **no clock in the ack path**, which is why KAFKA-4942's re-emission cannot happen. |
| **work-unit-and-planning** | **Enumerator + reader with `Assignment` as a first-class value.** `AssignmentID` is a **struct**; `Bounded` and `Range` are declared; `Planner` is static, `Replanner` is dynamic; the plan is **durable rows claimed by lease** | Every system that omitted splits is structurally unable to scale a stateful source, and retrofitting them changes the source interface, so it cannot be deferred. Struct id avoids Flink's parse-it-back-in-three-places scar. Durable rows + pull-based claiming give load balancing with no load model and no rebalance storm, and make "20 workers for backfill, 2 for stream" expressible. |
| **snapshot-model** | **Split boundedness is the mechanism** (`Assignment.Bounded`; completion is `AssignmentState.Done`, data not a flag). Airbyte's orthogonal per-stream `ModeSet` is the operator model. `Phase` is **reporting-only in the checkpoint header**, and a CI grep asserts the engine never switches on it. `Phased` is offered as a cheaper fallback tier | Two mature frameworks independently smuggled phase into the opaque checkpoint and both lost progress, parallelism and resumable resume — conclusive. Canal has **no pipeline type**: batch = all assignments bounded, CDC = all unbounded, hybrid = both, and no enum distinguishes them. The core owns the handoff invariant (admission asserts a chunked plan has exactly one unbounded assignment per stream with a non-empty low-watermark `Resume`). |
| **snapshot-chunking** | **The eight-step algorithm in core**, behind three opt-in capabilities (`Chunkable`, `Comparable`, `Replayable`), with both documented scars pre-fixed: **indexed** range filter from day one, **paged** finished-chunk set via `ChunkSetRef` | In core, the connector's obligation shrinks to "give me ordered chunks by key" and "let me replay from a position" — both source-agnostic. Per-connector it gets built badly N times, and Benthos demonstrates that. The filter self-retires, so steady-state cost is zero. |
| **schema-and-drift** | `Discoverer` optional but **strongly incentivised** (its absence produces a `Synthesised: true` catalog, labelled as such); schema travels **on the assignment and as a facet**; changes are **ordered in-band `SignalSchemaChange`** with a `SchemaEpoch` committed atomically with the position; the **five-mode drift policy adopted wholesale** with `DriftLenient` (never destructive) as default; sinks declare `SupportedChanges()` and nothing more | Debezium's two independently-committed stores are the counter-example: if decoding a historical event needs historical schema, then schema is checkpoint state. Drift policy in core is what stops an unanswerable question landing on every sink. **Disagreement with the distiller:** `Discover` is *not required*, because requiring it would force every webhook, socket and metrics-scrape source to fake one — and my angle's whole thesis is that the required surface stays minimal. The absence is made loud instead. |
| **capability-surface** | **The distiller's "both", pushed as far as it goes.** Behaviour is an optional exported interface; the fact is declarative data cross-checked at registration and admission (DG-3); type assertions live in **one table**, as data, with `Probe` *and* `Bind`; the result is a reified `CapabilitySet` where **every absence carries a reason**; `CapabilityReporter` masks off config-impossible capabilities; `ErrCapabilityDeclined` is legal only during negotiation | This is the decision my whole proposal is about. `Bind`-into-function-fields is the piece the decision space was missing: it answers "a type assertion cannot cross a process boundary" without nine hand-written forwarders per capability, because the engine never calls through an interface. And `Reason`-on-absence answers "a flag without methods is worthless" from the other side — a flag with a sentence attached is the most valuable thing in the UI. |
| **config-self-description** | **One Go-declared `Spec`** that emits JSON Schema: composite fields, native nesting, tagged `Union`s, declarative `Predicate`s for `ShowIf`/`RequiredIf`, named `DynamicChoices` hooks, per-field `Diagnostic`s, `LintUnknown`, `Secret`, `Stability`, and Segment-style `Mappings` | Making the spec the single source of truth is the entire answer to the frontend goal with no per-connector UI code, and the only shape satisfying "specialised sink UX later must not require core changes". `Predicate` is a closed 9-op tree rather than an embedded language: **disagreement with the open question** — a language is a parser in two runtimes and a real dependency, and equality/membership/presence/boolean is all a live form needs. |
| **error-classification** | **The seven-class ownership taxonomy + `NotConnected` + `EndOfInput` (as `io.EOF`)**, declared at the point of raise via `canal.Error`, with optional `Classifier`. `RetryPolicy` is `{MaxAttempts>0, Backoff, Escalate, Terminal}` per class; `Escalate` does the last attempt under a *different* strategy; DLQ works for sources; `RetrySafe` is the `ErrBadConn` predicate | A closed set makes the class a legitimate bounded metric label and makes "a hint the framework ignores" impossible. `MaxAttempts: 0` is a config error, not "infinite" — unbounded retry as a default is a livelock generator two systems have already patched. `RetrySafe` is R4+R5 in one field and is asserted by `canaltest`. |
| **flow-control-and-batching** | **Bounded by construction on every edge.** One framework-owned per-assignment in-flight concept. `Buffer` is a pluggable stage with `WhenFull ∈ {block, reject, drop_oldest, drop_newest}` in the type. Batching is connector-**declared** policy, framework-**enforced**, goroutine-free for the connector. Four core-named backpressure metrics | R6 requires a rejection path be expressible; `RecordBatch.Append` returning `ok` makes growth inexpressible. Flink's negative lesson is decisive: small bounded buffers remove an entire subsystem. Drops are always counted. |
| **topology-and-transforms** | **Fixed small outer shape as an implementation fact, never an enumerated stage list in the contract** (R1). All variety from `ComponentRef` — components containing components. `Transform` is one method with the full return vocabulary. `MetaComponent.Children()` is the *sanctioned* "I am a stage not a leaf" declaration | R1 forbids a fixed stage count in the contract. Recursive composition is the same mechanism as component-valued config fields, so fan-out, fan-in, routing, fallback and DLQ cost nothing extra, and observability nests automatically. Benthos punches an undocumented hole here; canal declares it. |
| **deployment-seam** | **Four interfaces and nothing else**: `ConfigStore`, `CheckpointStore` (`Set` atomic across the map, bytes-only), `Coordinator` (leader plans, **lease is the fencing token**), `StatusAggregator`. `singleNode` for the laptop, Postgres for the cluster. Status is k8s-shaped `Phase` + `Conditions`. No `Runtime.IsDistributed()`, ever | The verified k8s caveat that leader election does not guarantee fencing means leadership can never be trusted for correctness. Making the data plane keep flowing and checkpointing with the entire control plane down is the single most valuable deployment property. A compacted log is explicitly **excluded** as a control-plane state machine — `KafkaConfigBackingStore`'s own javadoc documents an unrecoverable state with "no obvious way to resolve the issue". |

---

## 17. Honest weaknesses

1. **Capability shape is static per Go type, and config-dependent capability is therefore awkward.**
   A source that can stream only when a replication slot is configured cannot conditionally implement
   `Positioner`. My answers are (a) return a different concrete type from `Open`, or (b) implement the
   interface and mask it via `CapabilityReporter`. Both work; (b) is more common and it means the
   `CapabilitySet` is the authority while the *Go type* over-promises. A connector author reading only
   the interfaces will be surprised by that, and `canaltest` can only check consistency, not intent.

2. **The capability count is the real cost, and it is front-loaded.** Twenty-nine capabilities means
   twenty-nine interfaces, twenty-nine table rows, twenty-nine `bound` fields, twenty-nine entries in
   every report, and twenty-nine `Unlocks` sentences to write and translate. `database/sql` reached
   twenty over fifteen years; canal is proposing twenty-nine on day one. Some are certainly wrong, and
   the ones that are wrong will be discovered *after* connectors depend on them. My mitigation is that
   deleting a capability is a table row plus a deprecation — much cheaper than deleting a required
   method — but it is still not free.

3. **`admit.Admit` is a large pure function and the whole design leans on it.** Every guarantee in
   canal is enforced in one place. That is excellent for testability and terrible for blast radius: a
   bug in the `Delivery` lattice silently promises exactly-once to a pipeline that cannot deliver it,
   and my "no silent downgrade" claim collapses. It needs exhaustive table tests over the full
   capability power set, which is 2^29 in principle and needs a smart reduction in practice.

4. **The probe needs a Reader instance to probe the Reader tier, before the pipeline exists.** I
   handwaved this in walkthrough (b) as "probed from a dry-run Reader on a probe assignment". That
   dry-run opens a real connection and constructs a real reader that is then thrown away, which is a
   side effect at admission time — precisely the thing I forbade in `Open`. The honest alternatives
   are: declare reader-tier capabilities in the `Spec` only (weakening DG-3 to a declaration for that
   tier), or accept the dry-run. I lean to the former and have not fully committed.

5. **`ErrCapabilityDeclined` being illegal after `Start` is a real ergonomic loss.** `driver.ErrSkip`'s
   per-call fallback is genuinely useful — a sink that can batch 90% of records and not the rest.
   I traded it away for auditability, and the cost lands on exactly the connector that would most
   benefit. The mitigation is `OutcomeRetriable`/`OutcomePartial` per record, which covers the sink
   case but not, say, a `Chunker` that hits an unchunkable stream mid-plan.

6. **Recursive composition via `ComponentRef` makes the config tree unbounded, and the UI must handle
   arbitrary depth.** Benthos shows this works, but it also shows the cost: a deeply nested
   `switch → fallback → dlq → switch` config is very hard to read in a form, and my `Predicate` is
   deliberately too weak to express the routing conditions people will want. I expect pressure toward
   an expression language and I have argued against it; that argument may not survive contact.

7. **Fan-in with shared state remains inexpressible.** Two sources joining into one sink with a
   shared lookup is not in this design. `Assignment` is per-source-stream, the ack graph is per
   assignment, and the checkpoint is per assignment. A join needs a shuffle, and I have deliberately
   not built one. If canal ever needs joins, this design does not stretch — it gets replaced at that
   layer.

8. **The `Change` facet's `Before` image is still under-specified.** `Availability` is honest about
   *whether* a before-image exists, but the decision space's open question stands: Postgres's
   `unchanged_toast_value` means even the *after* image can be partial, and I have only one
   `Availability` field plus an `Omitted` list. A sink doing a merge needs to know the difference
   between "this field is null" and "this field was not transmitted", and I am not confident one
   `Availability` enum plus a path list is enough.

9. **Per-assignment checkpoint decomposition assumes assignments are numerous but individually
   small.** A source with one enormous assignment and a 40 MB cursor blob rewrites 40 MB per
   checkpoint. I asserted that per-assignment decomposition mitigates the merge-patch problem; that is
   true for the many-partitions case and false for the fat-cursor case, and I have no answer for the
   latter beyond "the connector should not do that".

10. **Nothing here is verified against Conduit**, which the research explicitly flags as the closest
    prior art and which already ships one Go interface satisfied by both in-process and gRPC
    connectors. My `Bind`-function-field mechanism is the load-bearing novelty of this proposal and it
    is exactly what Conduit would have evidence about. It should be checked before anything freezes.

11. **`Runtime.Counter(MetricID)` means a connector genuinely cannot emit a connector-specific
    number.** That is the correct default and it will be resented. `Signal(SignalProgress{})` routes to
    the status document, which covers progress but not, say, "bytes fetched from the vendor's paginated
    API by page size". I am betting that gap is acceptable; I am not certain.

12. **The four-strategy `PlanStrategy` enum is the one place core switches on something.** I argued it
    is legitimate because it is core's own algorithm choice and no connector supplies it. But four
    strategies means four code paths through the read loop's setup, and a capability combination I did
    not anticipate (say `Chunkable` without `Planner`) falls into a strategy that was designed for a
    different shape. The lattice needs to be total, and I have only proven it is total for the four
    combinations I enumerated.

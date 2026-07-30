# canal core architecture — proposal: the split is the unit

**Status: draft.** Not normative. One of four competing proposals in `docs/decisions/proposals/`. Nothing here
may be cited as "MUST" until a decision record in `docs/decisions/` adopts it (R12).

**Constraints honoured:** source/sink agnostic (#1), bespoke-but-informed (#2), in-process Go interfaces + a
registry designed so an out-of-process implementation satisfies the *same* interface (#3), the deliverable is
the interface set (#4), Go 1.23 with generics only where they earn their keep (#5).

**Normative inputs:** `docs/design-rules.md` (R1–R13, and the nine open decisions at its end — all nine are
closed in §21). `docs/research/_decision-space.md` (D1–D14 — all fourteen are answered in §20).

---

## 0. The bet

**One abstraction — the split — is simultaneously the unit of work, the unit of resumability, the unit of
ordering, the unit of assignment, the unit of in-flight accounting, and the unit of progress reporting. A source
is a planner that emits splits and a reader that consumes them. Everything else in canal's end-state goal list
is a consequence rather than a feature.**

Concretely, the claim is that these six things are the *same* problem and canal should only solve it once:

| End-state goal | How it falls out of splits |
|---|---|
| Batch / snapshot pipeline | a plan whose splits are all bounded, and which reaches `NoMoreSplits` |
| Streaming CDC pipeline | a plan containing one unbounded split over the change log |
| Hybrid snapshot-then-stream | a plan that emits bounded scan splits first and an unbounded split after them |
| Parallel snapshot | many bounded splits, placed on many readers, by the same placer that places one |
| Resumable chunked scan | a split's cursor advances *inside* the split; the checkpoint is the split list |
| Exactly-once restart/replay | ownership transfers with a snapshot, never with a message |
| Horizontal scale-out | the enumerator/reader boundary is the process boundary; one enumerator, N readers |
| Standalone single binary | the same boundary, with a channel instead of a network |

The corollary that makes this worth doing rather than merely elegant: **the checkpoint stops being a separate
subsystem.** The reader's checkpoint *is* its split list (FLIP-27), so there is no `Offset` type, no offset
store, no `sourcePartition`/`sourceOffset` map, no two-level "global vs per-stream state" struct, and no ack
closure. Airbyte needed `GLOBAL` *and* `STREAM` state because it had per-stream positions and no way to express
a shared log; in this design "one shared log position" is *one unbounded split* and "independent per-stream
progress" is *one unbounded split per stream*, and the source chooses between them **by planning differently**
rather than by declaring a flag the core has to interpret. That collapse — two checkpoint shapes and a
capability flag into zero — is the single strongest evidence for the bet.

**The honest cost, stated up front and answered in §18:** an enumerator/reader pair is a lot of ceremony for
someone writing a source that polls one HTTP endpoint. The answer is a *default enumerator* (`canal.OneSplit`)
that plans exactly one unbounded split and is nine lines of core code, so a trivial source implements one
function of shape `func(ctx, Position) ([]Record, Position, error)` and never types the word "split". Critically
this is sugar over the real model, not a second model: that source's checkpoint bytes, metrics, progress
document and scale-out path are identical to a chunked CDC source's, and upgrading it to N splits later touches
zero core code and zero checkpoint-format code. §18 shows both spellings side by side; §22.1 admits where the
sugar leaks.

**What this proposal deliberately does not adopt from Flink**, because the angle is "FLIP-27 in Go", not "Flink
in Go":

- No barriers, no alignment, no in-flight-buffer persistence. The checkpoint is one durable record written in
  one store transaction while the pipeline briefly stops admitting records (§13.4). Flink's own dossier verdict
  is that properties 2 and 3 of the barrier protocol exist for shuffled graphs at 1000-subtask scale and canal
  must not pay for them. The *interface* stays barrier-shaped (`Snapshot(ctx, CheckpointID)` on every stage) so
  a real barrier protocol could be inserted later without touching a connector.
- No type parameters on any plugin interface (FLIP-191 could not remove `GlobalCommitter` "due to the typed
  parameters"; FLIP-372 names inner-interface coupling as what blocked `TwoPhaseCommittingSink`). Generics
  appear on three concrete helpers and one registration function, and nowhere else.
- **The enumerator does not assign splits to workers.** Flink lets the enumerator call
  `assignSplit(split, subtaskId)`, which forces the connector to know that workers exist and to hold a
  placement policy. canal's enumerator emits splits into a plan; a core-owned placer places them. That is
  Part-0 axiom #1 ("the connector plans, the runtime places") taken seriously, and it is what makes
  standalone and distributed the *same* code path rather than two.
- No enumerator threading model prose. Flink needs `callAsync`/`runInCoordinatorThread` and a javadoc warning,
  and Flink CDC still shipped two `ConcurrentModificationException` fixes for touching enumerator state
  off-thread. Here the core calls every `Enumerator` method from exactly one goroutine and says so in the
  interface doc comment; the enumerator has no locks and starts no goroutines. This is strictly nicer in Go and
  it costs one sentence.

---

## 1. The model in one page

```
                    ┌───────────────────────────────────────────────────────────┐
                    │  PLAN  (durable, observable, one per pipeline generation) │
                    │  unassigned splits │ completions │ streams │ schemas      │
                    └───────────────┬───────────────────────────────────────────┘
   Enumerator ──emits splits──────► │ ◄──reports completions/returns── Reader(s)
   (one, anywhere)                  │  placed by the core's Placer
                                    ▼
   ┌──────────┐   SplitBatch    ┌────────┐   Batch    ┌────────┐   Batch   ┌──────┐
   │  Reader  ├────────────────►│ decode ├───────────►│transform├─────────►│buffer│
   └──────────┘  (+cursor)      └────────┘            └────────┘           └──┬───┘
                                                                              │
   ┌──────────────────────────────────────────────────────────────────────┐    ▼
   │ prefix.Tracker[Position]  — per split, resolves the committable      │ ┌──────┐
   │ prefix as records settle at the sink. Fan-out/filter/rebatch safe.   │ │encode│
   └───────────────────────┬──────────────────────────────────────────────┘ └──┬───┘
                           │ committable cursor per split                      ▼
                           ▼                                              ┌────────┐
              ┌────────────────────────┐                                  │ Writer │
              │ Checkpointer           │ ◄── committables ─────────────── │+Committer│
              │ one record, one txn,   │                                  └────────┘
              │ one monotonic id       │
              └────────────────────────┘
```

Five facts to hold on to:

1. **A `Split` is a value.** Immutable `Spec` (what to read, written by the enumerator) + mutable `Cursor`
   (how far we got, written by the reader), both connector-opaque `VersionedBlob`s, wrapped in a
   core-readable envelope (id, kind, key range, position range, watermark, schema reference).
2. **The reader's checkpoint is its split list.** `Reader.Snapshot(ctx, id) ([]Split, error)`. There is no
   other source-side durable state.
3. **The sink has no progress awareness.** `Write` returns which records landed; the core maps that back to
   split cursors through the prefix tracker. A new sink literally cannot get checkpointing wrong.
4. **Nothing crossing a boundary is a closure.** Every method is `(ctx, serialisable) → (serialisable, error)`.
   There is no `AckFunc` anywhere in this design, which removes the one closure the decision space's Chain E
   flagged as the risk to wire-shippability.
5. **The plan is data** (R1). It is one persisted document that is simultaneously the checkpoint's
   enumerator half, the assignment table, and the UI's progress source. One entity, one representation (R9).

---

## 2. Vocabulary (R9: one concept, one word, no cross-maps)

| Word | Means exactly | Never means |
|---|---|---|
| **Stream** | a logical object a source can read from — a table, a topic, a collection, an endpoint, "the one stream" | a Go channel; the whole pipeline |
| **Split** | a unit of readable work over one stream, with a resumable cursor | a shard of the *sink*; a DAG node; a goroutine |
| **Plan** | the enumerator's durable output: which splits exist and their status | an execution plan in the SQL sense |
| **Position** | an opaque connector-authored point in a *read order* (log offset, page cursor, byte offset) | a key |
| **Key** | an opaque connector-authored point in a *key space* (primary key, partition key) | a position |
| **Cursor** | the `Position` at which a split may be resumed | an iterator object |
| **Watermark** | the `Position` at which a bounded scan split became consistent (Flink CDC's `HIGH`) | event-time watermark — canal has none in core |
| **Checkpoint** | one durable record under one monotonic id, containing everything | a savepoint; a buffer flush |
| **Committable** | an artifact a sink staged and can publish later (2PC) | a record; a batch |
| **Generation** | monotonically increasing integer bumped on every pipeline spec change or full restart | a version string |
| **Phase** | derived reporting label over the plan; never read by control flow | a state machine input |
| **Condition** | k8s-shaped `{Type, Status, Reason, Message, LastTransitionTime, ObservedGeneration}` | a Go predicate |

There is deliberately no word for "task", "job", "connector instance" or "partition". A worker holds
*assignments*; an assignment names a *split*.

---

## 3. Package layout and dependency direction

```
github.com/BernardoCSACarreira/canal
│
│  ── LEAVES (no canal imports except each other; no I/O; no goroutines) ────────
├── opt/           Opt[T]                                              (imports: nothing)
├── ord/           Ordinal, Position, Key, Bound, KeyRange, Range      (opt)
├── blob/          VersionedBlob, Serializer[T], codec-version rules   (nothing)
├── record/        Record, Payload, Meta, Change, Origin, RecordID,
│                  Batch                                              (opt, ord, blob, fail, schema)
├── schema/        Schema, Field, Type, LogicalParams, Fingerprint,
│                  Ref, Catalog, Stream, SchemaChange, DriftPolicy     (opt)
├── fail/          Class, Error, Stage, Diagnostic, Disposition        (nothing)
├── split/         SplitID, Split, SplitKind, Plan, PlanState,
│                  Completion, Assignment, Phase                       (opt, ord, blob, schema)
├── cfg/           Spec, Field, FieldKind, Predicate, Config,
│                  Composites (batching/retry/tls/codec/...)           (opt, fail)
│
│  ── THE PLUGIN SURFACE (what a connector author imports) ──────────────────────
├── connector/     Source, Enumerator, Reader, Sink, Writer, Committer,
│                  every Supports* interface, every Context, every
│                  Capabilities struct, Framer/Encoder/Decoder         (all leaves)
├── registry/      Registry, RegisterSource/Sink/Codec/Transform,
│                  Descriptor, capability cross-check                  (connector, cfg)
├── canal/  ⟵ façade: type-aliases the above so a connector author writes ONE import
│
│  ── THE DURABILITY SUBSTRATE (bytes in, bytes out; knows no domain type) ──────
├── store/         CheckpointStore, ConfigStore, Coordinator,
│                  StatusStore, Lease, Revision                        (nothing but ids + []byte)
├── stores/mem     in-process, for tests
├── stores/bolt    single-file, for `canal run`
├── stores/pg      Postgres, for `canal serve`
│
│  ── THE ENGINE (imports everything; nothing imports it) ───────────────────────
├── engine/        Pipeline, Planner, Placer, ReaderHost, SinkHost,
│                  Checkpointer, Dedupe, Buffer, TransformChain,
│                  ChunkFilter, Backoff, Metrics impl
├── engine/prefix  Tracker[T] — the contiguous-prefix resolver
├── engine/remote  gRPC Enumerator/Reader/Sink implementations  ⟵ constraint #3 lives HERE, not in connector/
├── control/       HTTP API, read model, status aggregation, live tap
├── connectors/…   every shipped connector (imports canal/ only)
└── cmd/canal      run | serve | worker | validate | offsets
```

**The four rules that make this a dependency *direction* and not a diagram:**

1. `connector/` imports no engine package, no store package, and nothing that opens a socket. A connector
   author cannot reach the runtime, so the runtime can be replaced without touching connectors.
2. `store/` imports nothing from `record/`, `split/` or `connector/`. It moves `(key, revision, []byte)`.
   This is Connect's `OffsetBackingStore` shape, and it is the mechanism that makes the standalone↔distributed
   swap free (D14). A `store` implementation that needed to understand a `Split` would be a defect.
3. `engine/remote` is where gRPC would live. **It contains implementations of `connector.Reader`,
   `connector.Enumerator` and `connector.Sink` that forward over a wire.** It defines no new interface. This
   is the concrete discharge of constraint #3: the out-of-process boundary is an ordinary plugin.
4. There is a test, `internal/archtest`, that walks the module graph and fails on any edge violating the
   above. Drift is prevented structurally, not by discipline (R8).

---

## 4. Leaf types

### 4.1 `opt` — the one generic that appears in signatures

```go
// Package opt provides the single optional-value type used across canal's
// interfaces. It exists because "absent" and "zero" are different facts in
// almost every canal signature — an absent cursor means "not a safe resume
// point", a zero cursor means "the beginning" — and because a nil pointer in a
// serialisable struct is a wire-format hazard.
package opt

// Opt holds a value that may be absent. It is a value type: copying an Opt
// copies the value. The zero Opt is absent.
type Opt[T any] struct {
	v  T
	ok bool
}

func Some[T any](v T) Opt[T] { return Opt[T]{v: v, ok: true} }
func None[T any]() Opt[T]    { return Opt[T]{} }

// Get returns the value and whether it is present. This is the only accessor;
// there is deliberately no Must or Unwrap, because every canal call site has a
// meaningful absent branch.
func (o Opt[T]) Get() (T, bool) { return o.v, o.ok }
func (o Opt[T]) Ok() bool       { return o.ok }

// Or returns the value if present, else def.
func (o Opt[T]) Or(def T) T {
	if o.ok {
		return o.v
	}
	return def
}
```

This is one of exactly four places generics appear (`opt.Opt`, `blob.Serializer`, `prefix.Tracker`,
`registry.RegisterSource`). Every one is a concrete helper or a registration function. No plugin interface has
a type parameter — that rule is load-bearing and §9.4 explains why.

### 4.2 `blob` — everything durable or boundary-crossing is `(version, bytes)`

```go
// Package blob defines the only shape in which connector-authored state is
// allowed to reach disk or a wire.
package blob

// VersionedBlob is connector-authored opaque state tagged with the version of
// the serialiser that produced it. The core stores it, ships it, and hands it
// back with its version; the core never interprets Bytes.
//
// The zero VersionedBlob is "absent", and every core call site that accepts one
// documents what absent means for it. Use opt.Opt[VersionedBlob] where absent
// and empty must be distinguished.
type VersionedBlob struct {
	Version int    `json:"version"`
	Bytes   []byte `json:"bytes"`
}

func (b VersionedBlob) IsZero() bool { return b.Version == 0 && b.Bytes == nil }
func (b VersionedBlob) Len() int     { return len(b.Bytes) }

// Serializer converts a connector's own type to and from bytes, and is handed
// the version its input was written with. This is Flink's
// SimpleVersionedSerializer, verbatim in structure, and it is the mechanism that
// buys canal both binary-upgrade compatibility (decision 9 in design-rules.md)
// and the future out-of-process boundary in one move.
//
// Contract:
//   - Version() is a small positive integer, incremented whenever Serialize's
//     output shape changes in a way an older Deserialize could not read.
//   - Deserialize MUST accept every version this connector build has ever
//     written, or return a *fail.Error with Class ClassPermanentContract naming
//     the version it cannot read. Silently misparsing an old blob is the one
//     unrecoverable bug class in this design.
//   - Serialize MUST be deterministic for a given value: the checkpoint store
//     compares bytes to skip no-op writes.
type Serializer[T any] interface {
	Version() int
	Serialize(v T) ([]byte, error)
	Deserialize(version int, b []byte) (T, error)
}

// Pack and Unpack are the two helpers every connector uses instead of touching
// VersionedBlob's fields.
func Pack[T any](s Serializer[T], v T) (VersionedBlob, error)
func Unpack[T any](s Serializer[T], b VersionedBlob) (T, error)

// JSON returns a Serializer[T] backed by encoding/json at version 1. It exists
// so that "I have not thought about my state format yet" is a one-liner that is
// still forward-compatible, and so that no connector's first commit invents a
// bespoke binary format. Its doc comment says, and canal's connector review
// checklist enforces, that a connector whose state is large or hot should
// replace it.
func JSON[T any]() Serializer[T]
```

### 4.3 `ord` — position and key, and the two optional facets that make the core smart without making it nosy

This is the load-bearing type of the whole proposal, so the reasoning is worth stating before the code.

The core needs to answer four questions that all look like "compare two opaque connector values":

- *Is this record's key inside this chunk's range?* (the chunked-snapshot dedup filter, §13.7)
- *Is this record's position after this chunk's watermark?* (same filter)
- *How far through this split are we?* (progress, ETA, the frontend's progress bar)
- *Which of these two positions is the resume point?* (`min(HIGH)` at handoff, §19.2)

Connect answers none of them, and its dossier states the consequence outright: *"there is no source-side lag
metric … a direct consequence of the framework not understanding the offset map it stores."* Flink answers them
by making the connector implement comparison methods (`Offset.isBefore/isAfter/isAtOrAfter`) — which works
in-process and **cannot cross a process boundary**, exactly the trap the decision space names as trap 11.

canal's answer: **comparability is optional *data*, not an optional *method*.** A connector that can produce an
order-preserving byte encoding of its position attaches it; the core compares with `bytes.Compare`. A connector
that can additionally project its position onto a number attaches that; the core computes fractions. A
connector that can do neither attaches neither, and the core degrades to counting splits — and, per FLIP-33
discipline, **omits the series it cannot compute rather than emitting zero**.

```go
// Package ord defines opaque connector-authored ordinals: points in a read
// order (Position) and points in a key space (Key).
package ord

// Ordinal is an opaque connector-authored value with two optional facets that
// let the core reason about it without interpreting it.
//
// Bytes is the canonical payload. It is what the connector will be handed back
// and must be sufficient to resume, filter or bound a read. The core NEVER
// interprets Bytes.
//
// Order, when non-nil, is an order-preserving encoding: for any two ordinals a
// and b produced by the same connector for the same stream,
// bytes.Compare(a.Order, b.Order) has the same sign as the connector's own
// notion of a before/after b. Supplying Order is what unlocks: chunked
// snapshots, mid-split checkpointing of an ordered split, the watermark filter,
// min/max over watermarks, and monotonicity assertions. It is the single
// highest-leverage optional field in canal.
//
// Scalar, when non-nil, is a monotone numeric projection used ONLY for progress
// arithmetic — (cur-lo)/(hi-lo). It need not be exact, dense or meaningful in
// any unit; it must be monotone with Order. Supplying it turns a split's
// progress from "started/finished" into a percentage.
//
// Version tags the serialiser that produced Bytes and Order together. Order's
// encoding is part of the versioned contract: changing it changes the total
// order and therefore invalidates persisted key ranges, so a change to Order
// MUST bump Version and the connector MUST be able to compare across versions
// or declare the old version unreadable.
type Ordinal struct {
	Version int      `json:"version"`
	Bytes   []byte   `json:"bytes"`
	Order   []byte   `json:"order,omitempty"`
	Scalar  *float64 `json:"scalar,omitempty"`
}

func (o Ordinal) IsZero() bool    { return o.Version == 0 && o.Bytes == nil }
func (o Ordinal) Comparable() bool { return o.Order != nil }

// Compare returns -1, 0 or +1 and true when both ordinals carry Order and were
// written by mutually comparable versions. It returns (0, false) otherwise, and
// EVERY core call site handles false by degrading rather than by guessing. That
// discipline is what keeps a non-comparable source a first-class citizen.
func (o Ordinal) Compare(b Ordinal) (int, bool)

// Fraction returns the position of o within [lo, hi] as a value in [0,1], and
// true only when all three carry Scalar and hi.Scalar > lo.Scalar. It returns
// (0, false) otherwise, and the caller then omits the metric series entirely.
func Fraction(lo, o, hi Ordinal) (float64, bool)

// Position is a point in a read order: a log offset, a page token, a byte
// offset, a resume token, a sequence number. Positions bound change reads and
// are what a split's cursor and a scan split's watermark are made of.
type Position struct{ Ordinal }

// Key is a point in a key space: a primary key, a partition key, a document id.
// Keys bound scan splits and identify records for upsert and dedup. A Key's
// Bytes MUST be a canonical encoding usable directly as an upsert/dedup key by
// a sink that knows nothing about the source.
type Key struct{ Ordinal }
```

`Position` and `Key` are distinct named types over one mechanism. They are not interchangeable and there is no
function mapping one to the other — which is precisely the R9 test. The distinction is real: Flink CDC keeps
`splitStart/splitEnd` (keys) and `highWatermark` (an offset) as different types for the same reason, and
conflating them would make `Split.Range` meaningless.

```go
// BoundKind makes -∞ and +∞ data rather than nil-with-a-comment. This is Flink
// CDC's ChunkBoundType{START, MIDDLE, END} generalised, and it exists because
// chunking a key space MUST produce ranges unbounded at both ends: rows
// inserted below the observed minimum or above the observed maximum after
// splitting began must still be covered by some chunk.
type BoundKind uint8

const (
	BoundNegInf BoundKind = iota + 1 // open below: matches every key
	BoundValue                       // closed at Key
	BoundPosInf                      // open above: matches every key
)

// Bound is one end of a range. Ranges are always half-open [Lo, Hi).
type Bound struct {
	Kind BoundKind `json:"kind"`
	Key  Key       `json:"key,omitempty"` // set only when Kind == BoundValue
}

func NegInf() Bound         { return Bound{Kind: BoundNegInf} }
func PosInf() Bound         { return Bound{Kind: BoundPosInf} }
func At(k Key) Bound        { return Bound{Kind: BoundValue, Key: k} }

// KeyRange is a half-open interval of a key space. The zero KeyRange is
// (-inf, +inf) and means "the whole stream": a source that does not chunk never
// constructs one.
type KeyRange struct {
	Lo Bound `json:"lo"`
	Hi Bound `json:"hi"`
}

// Contains reports whether k falls in the range, and whether the question could
// be answered at all. It is answerable iff every Key involved carries Order.
// The chunked-snapshot filter (engine.ChunkFilter) requires true; the pipeline
// is refused at submit time if a source declares Chunkable and its keys are not
// comparable (§10.3), so this can never be discovered in production.
func (r KeyRange) Contains(k Key) (bool, bool)

// Overlaps, Split and Normalise round out the range algebra the enumerator and
// the chunk filter need. All are pure functions over Order bytes.
func (r KeyRange) Overlaps(o KeyRange) (bool, bool)
func (r KeyRange) IsWhole() bool

// PositionRange is a half-open interval of a read order, used to bound a change
// read: [From, To). An absent To means unbounded — which is exactly what makes a
// split unbounded, and therefore what makes a pipeline streaming rather than
// batch. Boundedness is not declared anywhere; it is read off this field.
type PositionRange struct {
	From opt.Opt[Position] `json:"from,omitempty"` // absent = "from the earliest available"
	To   opt.Opt[Position] `json:"to,omitempty"`   // absent = "never stop"
}

func (r PositionRange) Bounded() bool { return r.To.Ok() }
```

`PositionRange.To` being absent is the *only* representation of "this pipeline never ends". There is no
`Boundedness()` method on a source, no `pipeline.type` config enum, and no `mode` field. A batch pipeline is a
plan all of whose splits are bounded and whose enumerator reported `PlanComplete`. That is R1 applied to the
one place every prior system put a mode flag.

---

## 5. The record model (R2 — decided first, before any transport)

Three separately-lifetimed layers plus core-owned provenance. The layers come from the decision space's D1
recommendation; provenance-as-a-fourth-thing is this proposal's addition, and it is what the split model needs.

```
Record
├─ Payload   one value, two lazily-cached views (bytes ⇄ structured), mutability in the accessor name
├─ Meta      separate addressable namespace, three tiers, never serialised to a sink by default
├─ Change    OPTIONAL typed facet: Op, Key, Before, After, CommitTime, TxID   (ok bool on access)
└─ origin    CORE-OWNED provenance: split, sequence, position, upstream id — structurally immutable
```

### 5.1 `Record`

```go
package record

// Record is canal's canonical in-flight record. It is a value type; copying a
// Record is cheap and copies neither the payload bytes nor the metadata map
// (both are copy-on-write behind the accessors).
//
// A connector constructs Records with New/FromBytes/FromStructured and never
// sets provenance: the core stamps id and origin when it ingests a SplitBatch.
// A transform receives Records and may change Payload, Meta, Change and the
// error, but CANNOT change id or origin, because they are unexported and the
// only way to make a derived record is Derive(), which forwards them unchanged.
//
// That structural immutability is Part-0 axiom #9 and it is why canal will never
// need Connect's SinkRecord.originalTopic/originalKafkaPartition/
// originalKafkaOffset retrofit (KIP-793): a transform physically cannot corrupt
// checkpoint identity.
type Record struct {
	// unexported, core-owned, immutable across the pipeline
	id     RecordID
	origin Origin
	err    *fail.Error

	// connector- and transform-owned
	Payload Payload
	Meta    Meta
	Change  opt.Opt[Change]
	Schema  opt.Opt[schema.Ref]

	// ObservedAt is stamped by the core at ingest from the pipeline's clock. It
	// is what the clock-skew policy (§12.5) clamps against and what the
	// end-to-end latency metric subtracts from. It is not the event time; that
	// is Change.CommitTime and it is the source's claim, not canal's.
	ObservedAt time.Time
}

// ID returns the framework-assigned in-flight identity. It is unique within one
// pipeline generation and is the key for per-record error reporting, DLQ
// correlation, retry targeting and the live tap. It is NOT durable and NOT a
// dedup key — see Origin for those.
//
// Having this at all is a deliberate divergence from Benthos, whose positional
// batch identity forced ~200 lines of Indexer/SortGroup correlation and a public
// method its own author marked "Deprecated: This method is harmful".
func (r Record) ID() RecordID { return r.id }

// Origin returns the record's provenance. Read-only by construction.
func (r Record) Origin() Origin { return r.origin }

// Error returns the error attached to this record, if any. Errors travel ON the
// record (Benthos's SetError/GetError) which is what makes mark-and-route error
// handling need no extra interface vocabulary: a transform marks, the sink stage
// rejects, the DLQ stage routes, and none of them need a second channel.
func (r Record) Error() *fail.Error { return r.err }

// WithError returns a copy carrying err. Provenance is forwarded.
func (r Record) WithError(err *fail.Error) Record

// Derive returns a copy of r with the same id and origin and the given payload.
// It is how a transform produces 1→1 output. For 1→N, DeriveN assigns child ids
// under the same origin so the prefix tracker can count descendants (§13.6).
func (r Record) Derive(p Payload) Record
func (r Record) DeriveN(n int) []Record

// RecordID is unique within a pipeline generation. Encoded as a single uint64 so
// it costs nothing to carry, log or index, and so a wire form needs no struct.
type RecordID uint64

// Origin is provenance: where this record came from and how it is counted. Every
// field is set exactly once, by the core, at ingest.
type Origin struct {
	// Split is the split that produced the record. It is the ordering scope: two
	// records with the same Split are ordered by Seq; two records with different
	// Splits have NO defined relative order anywhere in canal.
	Split split.SplitID `json:"split"`

	// Seq is monotonic within Split across the whole life of the pipeline
	// generation, assigned by the core as it drains SplitBatches. It is what the
	// prefix tracker keys on, and it is what makes "the committable prefix" a
	// well-defined phrase.
	Seq uint64 `json:"seq"`

	// Pos is the split position AT this record, when the source named one. It is
	// finer-grained than the batch cursor: a source may name a position per
	// record while only permitting a resume at transaction boundaries. Pos feeds
	// the chunked-snapshot watermark filter, event-time-free lag, and the live
	// tap. It is NEVER used as a resume point — SplitBatch.Cursor is.
	Pos opt.Opt[ord.Position] `json:"pos,omitempty"`

	// Upstream is the source system's own identifier for this record, if it has
	// one ("" if not). This is layer 1 of the three idempotency layers; layer 2
	// is Change.Key; layer 3 is (Split, Seq). A source with no upstream id MUST
	// leave this empty rather than inventing one, and if it derives one it MUST
	// derive it deterministically from stable fields and document the derivation
	// in its descriptor's Notes field, which the UI renders.
	Upstream string `json:"upstream,omitempty"`

	// Snapshot marks a record produced by a bounded scan rather than a change
	// read. It is set by the core from the split's Kind, not by the connector, so
	// it cannot disagree with the plan. Sinks use it to choose insert-vs-upsert
	// semantics; the core uses it to decide the record is not resumable-against
	// when the split declares no cursor.
	Snapshot bool `json:"snapshot,omitempty"`
}

// Ref is the durable, cross-restart identity of a record, used by the DLQ, the
// audit log and the live tap. It is derived, never stored on the record.
type Ref struct {
	Pipeline   PipelineRef `json:"pipeline"` // carries TenantID — see §16.5
	Generation uint64      `json:"generation"`
	Split      split.SplitID `json:"split"`
	Seq        uint64      `json:"seq"`
}

func (r Record) Ref(p PipelineRef, gen uint64) Ref
```

### 5.2 `Payload` — bytes are first-class, structure is a view

```go
// Payload holds one value with two lazily-cached views. The design points, all
// taken from Benthos because it is the only surveyed system that got this right:
//
//   - A pipeline that never inspects the body pays nothing: a source that
//     produced bytes and a sink that wants bytes never allocate a structured
//     view.
//   - Mutability is encoded in the accessor NAME, and AsStructuredMut deep-clones
//     lazily only if the value is still owned upstream.
//   - HasBytes/HasStructured let a sink take the cheap path explicitly rather
//     than guessing.
//
// Diverging from Airbyte deliberately: an untyped JSON object is NOT the only
// option. Airbyte's choice to make it so deferred the type problem to dbt-based
// normalisation, which became the worst part of that product and was eventually
// deleted.
type Payload struct {
	bytes      []byte
	structured any
	owned      bool // false ⇒ structured is shared and AsStructuredMut must clone
}

func FromBytes(b []byte) Payload
func FromStructured(v any) Payload

// AsBytes returns the payload as bytes, encoding the structured view with the
// pipeline's configured codec if only that exists. It caches the result.
func (p *Payload) AsBytes() ([]byte, error)

// AsStructured returns a read-only structured view, decoding bytes with the
// pipeline's configured codec if only those exist. Callers MUST NOT mutate the
// returned value; use AsStructuredMut.
func (p *Payload) AsStructured() (any, error)

// AsStructuredMut returns a structured view the caller may mutate, deep-cloning
// first if and only if the current value is shared.
func (p *Payload) AsStructuredMut() (any, error)

func (p Payload) HasBytes() bool      { return p.bytes != nil }
func (p Payload) HasStructured() bool { return p.structured != nil }
func (p Payload) IsEmpty() bool       { return p.bytes == nil && p.structured == nil }

// SizeHint returns the byte length if known without encoding, else -1. The
// batcher's byte-size trigger uses it and falls back to encoding only when a
// byte limit is actually configured.
func (p Payload) SizeHint() int
```

### 5.3 `Meta` — a separate addressable namespace, three tiers

```go
// Meta is a namespace addressable separately from the payload. It is NEVER
// serialised into a sink's payload unless a metadata filter explicitly selects
// keys, which is Vector's `.field` vs `%field` split adopted as a type rather
// than a syntax. Reserved prefixes inside the body — Airbyte's _ab_cdc_*,
// Singer's _sdc_* — are the failure mode this prevents: they cause R9 vocabulary
// sprawl and they silently ship provenance junk to the destination.
//
// Three tiers, because an expensive derived value must be able to ride along
// without a per-record clone:
//
//	Set/Get              string values; the common case; cheap to copy
//	SetAny/GetAny        arbitrary Go values, copied by reference
//	SetImmutable/...     values the engine may share freely across copies and
//	                     across fan-out branches, because the value promises it
//	                     will not be mutated. A resolved schema is the motivating
//	                     example.
//
// Deliberately NOT adopted from Benthos: MetaSet's "empty string deletes the
// key". "Present but empty" is a real fact about upstream data and must be
// representable. Delete is explicit.
type Meta struct{ m map[string]any }

func (m *Meta) Set(k, v string)
func (m Meta) Get(k string) (string, bool)
func (m *Meta) SetAny(k string, v any)
func (m Meta) GetAny(k string) (any, bool)
func (m *Meta) SetImmutable(k string, v Immutable)
func (m *Meta) Delete(k string)
func (m Meta) Len() int
func (m Meta) All() iter.Seq2[string, any] // Go 1.23 range-over-func: read-only iteration

// Immutable marks a metadata value the engine may share across record copies and
// fan-out branches. Copy is called by the engine only when it must hand the
// value to something that will mutate it.
type Immutable interface{ Copy() any }

// Secrets is a compartment separate from Meta, adopted from Vector. Values put
// here are redacted by every logger, every metric label path, the live tap and
// the HTTP API, structurally: the read model has no field that can carry them.
// A connector that needs to pass a token to a downstream stage puts it here.
type Secrets struct{ m map[string]string }
```

### 5.4 `Change` — the optional typed facet

```go
// Change is the OPTIONAL typed change-event facet. Access is
// `if c, ok := rec.Change.Get(); ok { ... }`, and a sink author must handle
// absent — which is stated in the Sink interface's doc comment and asserted by
// the connector conformance suite.
//
// Why a facet and not a subtype: constraint #1 forbids a Mongo-shaped or
// relational-shaped core. A webhook receiver, a metrics scrape and a file tail
// have no before-image, no key and no operation, and forcing them to invent one
// is what produced Flink CDC's blank fourth OperationType. Making it optional
// DATA rather than a type hierarchy means the generic path is unchanged, the
// core never switches on source type, and a CDC-aware sink can upsert without
// per-source knowledge.
//
// Why a facet and not metadata conventions: Benthos proves the alternative
// fails. Its MySQL input invents MessageOperation{read,insert,update,delete,
// snapshot_complete,xid} and flattens it to MetaSet("operation", ...); its
// Postgres input uses a DIFFERENT key set (lsn vs binlog_position) and a
// different op vocabulary. There is no cross-source contract, so every CDC-aware
// sink special-cases every source — constraint #1 violated by drift.
type Change struct {
	Op Op `json:"op"`

	// Key identifies the changed entity. Key.Bytes MUST be a canonical encoding
	// a sink can use directly as an upsert key. Key.Order, when present, is what
	// lets the core run the chunked-snapshot filter and what lets a chunk-ranged
	// dedup work. Absent for sources with no notion of identity.
	Key opt.Opt[ord.Key] `json:"key,omitempty"`

	// Before and After are pre- and post-images. Both optional: many real
	// sources cannot produce a complete before-image (Postgres without REPLICA
	// IDENTITY FULL) and some cannot produce a complete after-image either
	// (Benthos ships an `unchanged_toast_value` sentinel for exactly this).
	// A sink that needs a before-image and does not get one raises
	// ClassPermanentMapping rather than guessing.
	Before opt.Opt[Payload] `json:"before,omitempty"`
	After  opt.Opt[Payload] `json:"after,omitempty"`

	// CommitTime is the source's claim about when the change happened upstream.
	// It is the input to the clock-skew policy (§12.5) and to the event-time lag
	// metric. Zero means "the source did not say", and the lag series is then
	// omitted rather than computed from ObservedAt.
	CommitTime time.Time `json:"commit_time,omitempty"`

	// TxID groups changes committed together, when the source exposes it. The
	// core uses it for one thing only: a source may declare that a resume point
	// is legal only at a transaction boundary, and the core then refuses to
	// commit a cursor mid-TxID even if the batch offered one.
	TxID string `json:"tx_id,omitempty"`
}

// Op is a closed vocabulary. Five values, deliberately fewer than Flink CDC's
// six: there is no Replace (Flink CDC's own docs leave its meaning blank, which
// is evidence the vocabulary was not derivable from first principles) and no
// separate Read — a scan row is OpSnapshot, which is strictly more informative.
type Op uint8

const (
	OpInsert   Op = iota + 1 // Before absent, After present
	OpUpdate                 // After present; Before present iff the source can
	OpDelete                 // Before present iff the source can; After absent
	OpSnapshot               // a row observed by a bounded scan, not a change
	OpTruncate               // the whole stream was emptied; Key absent
)
```

**Open, and named as open (decision-space open question 3):** whether `Before` earns its place. If the first
three real sources cannot populate it, it is over-designed and should be deleted before v1 rather than carried.
The facet is versioned data, so deleting a field is a `Serializer` version bump and not an interface change —
which is the point of making it data.

### 5.5 `Batch`

```go
// Batch is an ordered slice of records that the engine moves as one unit
// between stages. Its ordering is meaningful only within one Origin.Split;
// records from different splits may be interleaved arbitrarily and a sink MUST
// NOT infer order across splits. That sentence is the entire ordering contract
// of canal and it is stated at the type, not in a manual.
type Batch []Record

// Splits groups a batch by origin split, in first-appearance order. Sinks that
// need per-split ordering (an append-only log per partition) call this; sinks
// that do not, ignore it.
func (b Batch) Splits() iter.Seq2[split.SplitID, Batch]

// ByteSize returns the sum of SizeHint over records, or -1 if any is unknown.
func (b Batch) ByteSize() int
```

---

## 6. Splits and the Plan — the centre of the design

### 6.1 `SplitID` — a struct, never a parsed string

```go
package split

// SplitID identifies a split. It is a STRUCT, not a string, and this is a direct
// response to a documented Flink CDC scar: Flink mandates only
// `SourceSplit.splitId() string`, so Flink CDC encoded "table:chunkID" into it
// and parses it back out in three places, with a warning to implementors that a
// custom id breaks the parser. The typo `source.meta.wartermark` in a public
// Flink CDC path has been unfixable for years for the same reason — a stringly
// identifier becomes a permanent wire contract.
//
// String() exists for logs and metric labels ONLY. No canal code parses it, and
// there is a test asserting that no canal package calls strings.Split on a split
// id.
type SplitID struct {
	Stream schema.StreamID `json:"stream"`
	Kind   Kind            `json:"kind"`
	// Seq disambiguates splits of the same Kind over the same Stream. The
	// enumerator assigns it and must never reuse a value within a pipeline
	// generation, because completions and dedup filters are keyed on it.
	Seq uint32 `json:"seq"`
}

func (id SplitID) String() string // "orders/scan/7", "orders/change/0"

// Kind is a closed two-value vocabulary. Everything else that prior art models
// as a third kind is expressed by BOUNDS, not by a new kind:
//
//	a snapshot chunk            = Scan   with a KeyRange
//	a whole-table snapshot      = Scan   with the whole KeyRange
//	a CDC tail                  = Change with PositionRange{From: x, To: absent}
//	a per-chunk backfill replay = Change with PositionRange{From: LOW, To: HIGH}
//	a bounded historical replay = Change with PositionRange{From: a,   To: b}
//
// That table is the entire justification for the angle. Flink CDC discovered the
// same collapse from the other end: its per-chunk backfill is
// `createBackfillStreamSplit(low, high)` — a StreamSplit with bounds — and dlt
// arrived at it independently with an incremental cursor carrying initial_value
// and end_value. Two kinds, and boundedness is data.
type Kind uint8

const (
	// Scan reads the CURRENT STATE of a key range. It terminates when the range
	// is exhausted. Records it produces carry Origin.Snapshot = true and, if the
	// source populates the Change facet, Op = OpSnapshot.
	Scan Kind = iota + 1

	// Change reads the CHANGE LOG from a position. It terminates iff
	// Range.Positions.To is present.
	Change
)
```

### 6.2 `Split` — one value, two connector blobs with different lifetimes

```go
// Split is a unit of readable work. It is a value: the core copies it freely,
// persists it, ships it and hands it to readers.
//
// The two connector-opaque blobs have DIFFERENT LIFETIMES and that separation is
// load-bearing:
//
//	Spec   written once by the enumerator; never modified. "What to read."
//	Cursor written repeatedly by the reader.                "How far I got."
//
// Flink CDC needs a parallel SplitState class hierarchy (SourceSplitState /
// SnapshotSplitState / StreamSplitState) as the mutable mirror of its immutable
// split, plus asSnapshotSplitState()/asStreamSplitState() downcasts at every
// call site. Putting the mutable part in a named field of the same value type
// deletes that hierarchy: there is one type, and the thing that changes is one
// field.
//
// The separation also makes AddSplitsBack sound: the enumerator gets back a
// Split whose Spec it recognises and whose Cursor it does not need to
// understand, so it can re-place the split without re-planning it.
type Split struct {
	ID SplitID `json:"id"`

	// Range is the core-readable extent of the work. For Kind Scan, Keys is
	// meaningful and Positions.To is normally absent. For Kind Change, Positions
	// is meaningful and Keys is normally whole. Both may be set: a bounded
	// replay restricted to a key range is a legitimate split and is exactly what
	// the per-chunk backfill is.
	Range Extent `json:"range"`

	// Spec is connector-authored, immutable, and describes what to read beyond
	// what Range already says: which key column to order by, which fields to
	// project, which consistency mode to use. The core never interprets it.
	//
	// Everything the core needs for placement, progress, filtering and reporting
	// is in the exported envelope fields, deliberately: a core that had to open
	// Spec to build a progress bar would be Connect, which cannot.
	Spec blob.VersionedBlob `json:"spec"`

	// Cursor is the resume point INSIDE this split, connector-authored. Absent
	// means "start from the beginning of Range". The core persists it and hands
	// it back; it never interprets it.
	//
	// This one field is what Benthos does not have, and its absence is why a
	// crash at 90% of a 500M-row Benthos snapshot restarts from zero: its phase
	// decision is `pos == nil` and there is no representation for "partially
	// snapshotted".
	Cursor opt.Opt[blob.VersionedBlob] `json:"cursor,omitempty"`

	// Watermark is the position at which this split's read became consistent.
	// For a Scan split it is the HIGH watermark and ITS PRESENCE MEANS THE SCAN
	// FINISHED — completion is data, not a separate boolean (Flink CDC's
	// `isSnapshotReadFinished() { return highWatermark != null; }`, which is the
	// right shape and worth copying exactly).
	Watermark opt.Opt[ord.Position] `json:"watermark,omitempty"`

	// Schema references the schema in force for this split, by fingerprint. The
	// schema itself lives once in PlanState.Schemas.
	//
	// Flink CDC carries schemas ON the split and had to invent
	// SchemalessSnapshotSplit plus a fillTableSchemas() rehydration step because
	// thousands of chunk splits each carrying a schema blew up the checkpoint.
	// Referencing by fingerprint from the first commit avoids inventing the
	// second type.
	Schema opt.Opt[schema.Ref] `json:"schema,omitempty"`

	// Group co-locates splits: two splits with the same non-zero Group are always
	// placed on the same reader. This is how a per-chunk backfill lands on the
	// reader that read the chunk, and how splits sharing one upstream connection
	// or one consistent-read transaction stay together. It is the generic,
	// worker-agnostic form of Flink's requesterHostname affinity hint: the
	// connector expresses a CONSTRAINT as data and never names a worker.
	Group GroupID `json:"group,omitempty"`

	// Ordered declares that records from this split arrive in Range order. It is
	// what LICENSES mid-split checkpointing: for an unordered split the core will
	// only commit a cursor the reader explicitly offers at a batch boundary and
	// will refuse to interpolate. Meltano's sorted/unsorted distinction is the
	// same property, and without it the core must either checkpoint incorrectly
	// or refuse to checkpoint at all.
	Ordered bool `json:"ordered"`

	// EstimatedRecords and EstimatedBytes are hints for progress and ETA only.
	// Absent is normal and the UI shows "unknown" rather than zero — per FLIP-33
	// discipline, an unmeasurable quantity is omitted, never rendered as 0.
	EstimatedRecords opt.Opt[int64] `json:"estimated_records,omitempty"`
	EstimatedBytes   opt.Opt[int64] `json:"estimated_bytes,omitempty"`
}

// Extent is the core-readable extent of a split.
type Extent struct {
	Keys      ord.KeyRange      `json:"keys"`
	Positions ord.PositionRange `json:"positions"`
}

// Bounded reports whether the split will terminate on its own. A Scan split
// always will; a Change split will iff its position range is bounded. Nothing
// declares boundedness — it is read off the data.
func (s Split) Bounded() bool {
	return s.ID.Kind == Scan || s.Range.Positions.Bounded()
}

// Finished reports whether the split's work is complete. For a Scan split that
// is Watermark presence; for a bounded Change split it is the cursor having
// reached To; for an unbounded Change split it is never.
func (s Split) Finished() bool

// GroupID co-locates splits on one reader. Zero means unconstrained.
type GroupID uint32
```

### 6.3 `Plan` — topology as data (R1), and one entity with one representation (R9)

This is where the proposal earns its keep operationally. The plan is *one* document that is simultaneously:

- the enumerator's half of the checkpoint,
- the assignment table the coordinator leases against,
- the UI's progress source,
- the snapshot→stream handoff artifact,
- the input to the chunked-snapshot dedup filter.

The abandoned canal attempt modelled buffers twice — as stages 3/5/7 *and* as segments keyed by
`followsStageOrdinal` 2/4/6 — and R1/R9 exist because of it. Having exactly one representation of "what work
exists and who is doing it" is the direct application of that lesson.

```go
// PlanState is the enumerator's durable state. It is written into the checkpoint
// and served, verbatim modulo redaction, to the control API's progress endpoint.
// One entity, one representation (R9).
type PlanState struct {
	// Enumerator is the connector's own opaque state: discovery cursors, the
	// chunk splitter's position, whatever it needs. Handed back to
	// Source.RestoreEnumerator.
	Enumerator blob.VersionedBlob `json:"enumerator"`

	// Unassigned holds splits that exist but no reader currently owns.
	//
	// IMPORTANT: a split stays here until a reader has ACKNOWLEDGED holding it
	// (which the core learns from the reader's next Snapshot). Splits handed to
	// the placer but not yet acknowledged remain Unassigned, so a lost assignment
	// message loses nothing and a duplicated one is absorbed by Reader.Assign
	// being idempotent on SplitID.
	//
	// This is deliberately simpler than Flink's SplitAssignmentTracker, which
	// keys uncheckpointed assignments by checkpoint id and un-assigns those
	// beyond the restore point. Both are correct; the difference is that Flink
	// treats assignment as reliable-until-proven-otherwise and canal treats it as
	// lossy from the start, which is the same philosophy as Flink CDC's own 30s
	// reconciliation sweep and CheckpointListener's "notifications might not
	// happen". Never build a protocol whose correctness depends on a message
	// arriving.
	Unassigned []Split `json:"unassigned"`

	// Assigned records ownership: split id → the assignment the core has
	// confirmed. This is the assignment table; in coordinated mode each entry is
	// backed by a lease row and the lease generation is the fencing token.
	Assigned map[string]Assignment `json:"assigned"` // key = SplitID.String()

	// Completions accumulate (key range, watermark, when it became durable) for
	// every finished bounded split. This is the ENTIRE snapshot→stream contract,
	// and nothing about it is relational or even database-specific: it is "a
	// partition of the key space, and the position from which changes to that
	// partition are still needed."
	Completions []Completion `json:"completions"`

	// ExpectedCompletions is the number of bounded splits the enumerator intends
	// to produce, when it knows. It exists so that a restored plan can assert
	// len(Completions) == ExpectedCompletions before handing off to the stream
	// phase, and so the UI can render a denominator. Flink CDC needed exactly
	// this (totalFinishedSplitSize) and retrofitted it, along with a truncation
	// path, after discovering the finished-split set was too large to ship in one
	// message. canal pages Completions by reference from day one (§16.4) and
	// carries the count from day one.
	ExpectedCompletions opt.Opt[int] `json:"expected_completions,omitempty"`

	// Streams is discovery progress. Remaining/Done is what stops a restart from
	// re-discovering work already done or missing newly-added work — Flink CDC's
	// remainingTables/alreadyProcessedTables pair, generalised.
	Streams []StreamState `json:"streams"`

	// Schemas is the deduplicated schema table splits reference by fingerprint.
	Schemas []schema.Entry `json:"schemas"`

	// NoMoreSplits is set when the enumerator has reported PlanComplete. With
	// every split finished, this is what makes the pipeline terminate as
	// Completed — the terminal status Kafka Connect lacks entirely.
	NoMoreSplits bool `json:"no_more_splits"`

	// Revision increments on every mutation. It is what the control API's
	// conditional read and the UI's incremental poll use, and what makes the
	// plan safe to CAS.
	Revision uint64 `json:"revision"`
}

// Completion is the handoff record: a key range and the position from which
// changes to that range are still needed.
type Completion struct {
	Split     SplitID       `json:"split"`
	Keys      ord.KeyRange  `json:"keys"`
	Watermark ord.Position  `json:"watermark"`

	// DurableAt is the checkpoint id at which this completion became durable.
	// The enumerator MUST NOT build a handoff split from completions that are not
	// yet durable, because a crash would then start the change read from a
	// watermark whose scan data was never written. Flink CDC persists exactly
	// this (splitFinishedCheckpointIds: "Map to record splitId and the
	// checkpointId mark the split is finished") and it is easy to omit and
	// catastrophic to omit.
	DurableAt CheckpointID `json:"durable_at"`
}

// Assignment is one split's ownership record.
type Assignment struct {
	Split     SplitID   `json:"split"`
	Worker    WorkerID  `json:"worker"`
	// Epoch is the worker's incarnation. (WorkerID, Epoch) from the first commit,
	// because Flink retrofitted attempt numbers onto subtask-id-keyed interfaces
	// and paid two default-throwing methods and a permanent compatibility wart.
	Epoch     uint64    `json:"epoch"`
	Lease     store.Lease `json:"lease"`
	AssignedAt time.Time  `json:"assigned_at"`
	// Revoking is set when the core wants the split back: the reader is asked to
	// Revoke, and only when it returns the split (or the lease expires) is the
	// assignment removed. Two-phase revocation is what makes drain graceful.
	Revoking bool `json:"revoking,omitempty"`
}

// StreamState is per-stream discovery and progress bookkeeping.
type StreamState struct {
	Stream    schema.StreamID `json:"stream"`
	Discovered time.Time      `json:"discovered"`
	// Retired marks a stream that discovery no longer sees. Its splits are
	// retired and its completions dropped by the pure reconciliation function
	// Reconcile (below) — Flink CDC's filterOutdatedSplitInfos, which must be a
	// pure function so that ExpectedCompletions stays correct and completeness
	// predicates keep holding.
	Retired opt.Opt[time.Time] `json:"retired,omitempty"`
	// Mode is the operator's per-stream choice, validated at submit time against
	// the discovered capabilities. See §11.4.
	Mode schema.StreamMode `json:"mode"`
}

// Reconcile folds a freshly discovered catalog into a restored plan, dropping
// state for streams that no longer exist and adjusting ExpectedCompletions so
// completeness predicates stay correct. It is a PURE FUNCTION and it is tested
// with table-driven fixtures, because "restored state met a changed world" is
// the case naive implementations get wrong and it is unobservable until it is a
// stuck pipeline.
func Reconcile(p PlanState, c schema.Catalog, now time.Time) (PlanState, []Retirement)

// Phase is DERIVED from the plan. It is a reporting label with a closed
// vocabulary and it is NEVER an input to control flow.
//
// This is the direct fix for decision-space trap 5, "smuggling phase into the
// opaque checkpoint", which Connect/Debezium and Airbyte did independently and
// both lost snapshot progress reporting, snapshot-specific parallelism and
// resumability-at-different-parallelism as a result. Two independent occurrences
// is conclusive.
//
// It is a function, not a field, so there is nothing to set and nothing to
// switch on: `grep -r "case Phase" ` returns only the renderer.
func PhaseOf(p PlanState) Phase

type Phase uint8

const (
	PhaseDiscovering Phase = iota + 1 // no splits yet, discovery in progress
	PhaseScanning                     // bounded Scan splits outstanding
	PhaseCatchingUp                   // Change splits outstanding, and a Backlogger says we are behind
	PhaseStreaming                    // Change splits outstanding, caught up
	PhaseCompleted                    // NoMoreSplits && every split finished
)
```

### 6.4 Why "one plan" and not "N per-stream states"

The decision space's D3 recommends a two-level checkpoint (`Shared` + `Streams`) because Airbyte needed both and
retrofitted the second. This proposal argues the two-level struct is an artifact of not having splits, and that
carrying it would be modelling one entity twice (R9).

The reasoning, concretely:

- **Airbyte's `GLOBAL` exists because `STREAM` broke for CDC**: with one replication log you cannot commit
  "table A up to LSN 500" without also committing B, because rewinding to re-read A re-delivers B. In this
  design a shared log is **one `Change` split**, so there is exactly one cursor and the problem is not
  expressible. There is nothing to make atomic because there is nothing to split.
- **Airbyte's `STREAM` exists because one slow stream must not pin the others.** Here that is **one `Change`
  split per stream**, each with its own cursor, each independently committable because each is its own ordering
  scope. Again nothing to declare.
- **"Whether the commit is atomic across streams" therefore stops being a capability the source declares and
  the core must trust.** It becomes an observable property of the plan: count the `Change` splits. A UI can
  show it. A submit-time validator can check it. Nobody can lie about it.

The cost of this collapse is real and stated in §22.3: a source whose upstream *is* one log but which wants
per-stream progress reporting has to either accept a single coarse cursor or emit per-stream splits it cannot
independently resume. The design's answer is that the second is a lie and the first is the truth, and that
showing one cursor with N stream labels is more honest than showing N cursors that must move together.

---

## 7. The source: `Source` → `Enumerator` + `Reader`

### 7.1 `Source` — the factory, and the cold/warm split

```go
package connector

// Source is a connector's factory. It holds validated configuration and nothing
// else: no connection, no goroutine, no mutable state. It is constructed once per
// (pipeline, generation) in every process that needs it, so constructing it must
// be cheap and must not perform I/O.
//
// Config arrives PRE-PARSED, PRE-VALIDATED and PRE-DEFAULTED at construction.
// There is no Configure() callback and no map[string]string the connector
// re-parses inside itself — which is Connect's actual behaviour via
// AbstractConfig and Part-0 axiom #8.
type Source interface {
	// Discover returns the catalog of streams this source can read, with a schema
	// and declared per-stream capabilities for each. It may perform I/O and may
	// be slow; the core calls it at submit time, on operator request, and on a
	// configurable schedule to compute drift as a diff (§11.4).
	//
	// A source with no natural catalog returns a single-stream catalog. That is a
	// one-liner via schema.OneStream(name, s), and §18 shows it. Making Discover
	// required is a real tax on webhook/socket sources and the tax is paid
	// deliberately: it is what buys the stream-picker UI, drift-as-diff and
	// per-stream mode selection with zero per-connector frontend code.
	Discover(ctx context.Context) (schema.Catalog, error)

	// NewEnumerator is the COLD path: no prior state exists.
	NewEnumerator(ctx context.Context, ec EnumeratorContext) (Enumerator, error)

	// RestoreEnumerator is the WARM path: state was written by a previous
	// generation of this pipeline, tagged with the serialiser version that wrote
	// it.
	//
	// Two methods rather than one method plus an `isRestored` flag, because Flink
	// separated createEnumerator/restoreEnumerator deliberately and the dossier
	// names the reason: cold start and warm start are different code paths with
	// different signatures, so the connector cannot accidentally treat "no state"
	// and "restored state" the same way. Kafka Connect's single start(props) is
	// the counter-example and Benthos's `if streamSnapshot && pos == nil` is what
	// it degenerates into.
	RestoreEnumerator(ctx context.Context, ec EnumeratorContext, state blob.VersionedBlob) (Enumerator, error)

	// NewReader creates a reader. There is deliberately NO RestoreReader: a
	// reader's entire durable state is its split list, and splits arrive through
	// Assign. That asymmetry is the FLIP-27 payoff and it is worth naming — the
	// reader side of resumption needs no API at all.
	NewReader(ctx context.Context, rc ReaderContext) (Reader, error)

	// Close releases anything the factory itself holds. Called after every
	// Enumerator and Reader it produced has been closed.
	Close(ctx context.Context) error
}
```

### 7.2 `Enumerator` — the planner

```go
// Enumerator plans work. Exactly one Enumerator is live per (pipeline,
// generation) across the entire deployment, held by whichever process holds the
// planner lease.
//
// THREADING: the core calls every method on an Enumerator from a single
// goroutine, and never concurrently. An Enumerator therefore needs no mutexes and
// MUST NOT start goroutines that touch its own state. If it needs background I/O
// it returns PlanIdle with a RetryAfter and does the work in the next Poll, or it
// uses EnumeratorContext.Go, which runs the result back on the enumerator
// goroutine.
//
// This is Flink's managed single-threaded coordinator model, obtained in Go for
// the price of one paragraph instead of callAsync + runInCoordinatorThread + a
// javadoc warning + two shipped ConcurrentModificationException fixes.
//
// WIRE SHAPE: every method is (ctx, serialisable) → (serialisable, error). There
// are no callbacks, no futures, no channels and no closures in any signature, so
// a gRPC Enumerator in engine/remote satisfies this interface with no core change
// (constraint #3).
type Enumerator interface {
	// Poll advances planning by at most one step. It must return promptly — the
	// core gives it a deadline — and it must not block waiting for work to exist.
	//
	// view is a read-only projection of the current plan: what is unassigned, what
	// is assigned, what has completed, how many readers are registered and how
	// much capacity they report. The enumerator uses it to decide whether to
	// enumerate more work; it does NOT decide who gets it.
	Poll(ctx context.Context, view PlanView) (Enumeration, error)

	// Report delivers facts the core learned from readers: splits that finished
	// (with their watermarks), splits returned by a reader that lost them or was
	// asked to give them up, and connector-defined events a reader sent.
	//
	// Report is where the snapshot→stream handoff is driven: the enumerator
	// accumulates completions and, once they are all durable, the NEXT Poll
	// returns the Change split. Splitting "learn a fact" from "decide what to do"
	// into two methods keeps Poll the only place splits are minted, which is what
	// makes the enumerator's own state machine testable as a pure transition
	// table.
	Report(ctx context.Context, r Report) error

	// Snapshot returns the enumerator's own opaque state for checkpoint id.
	//
	// The state MUST NOT include splits the core has placed — the core tracks
	// those in PlanState.Unassigned/Assigned and hands them back on restore. It
	// SHOULD include discovery cursors, the chunk splitter's position, and
	// anything else the enumerator cannot recompute cheaply.
	//
	// Flink's javadoc for the equivalent says the snapshot "should assume that
	// all operations that happened before the snapshot have successfully
	// completed". canal's version is stronger and simpler: the enumerator
	// snapshots only what the CORE cannot see, because everything the core can
	// see is already in the plan.
	Snapshot(ctx context.Context, id CheckpointID) (blob.VersionedBlob, error)

	// Close releases resources. It is called on lease loss as well as on
	// shutdown, so it must be safe to call while a Poll is not in flight and must
	// not assume the pipeline is stopping.
	Close(ctx context.Context) error
}

// PlanView is the read-only projection of the plan handed to Poll. It is a value
// and it is serialisable, so a remote enumerator receives it over the wire
// unchanged.
type PlanView struct {
	Generation uint64 `json:"generation"`

	// Unassigned, Assigned and Finished are COUNTS plus the ids, not the splits.
	// A plan with 200k chunk splits must not serialise 200k Specs into every Poll
	// request. The enumerator that needs a split back asks for it via
	// EnumeratorContext.Split(id).
	UnassignedIDs []SplitID `json:"unassigned_ids"`
	AssignedIDs   []SplitID `json:"assigned_ids"`

	// Completions are paged: Poll receives at most CompletionPageSize of them
	// plus the total. This is the pre-fix for Flink CDC's documented scar, where
	// the finished-chunk set turned out too large to ship in one message and
	// required four new event types, a totalFinishedSplitSize field and a
	// truncation path retrofitted after the fact.
	Completions      []split.Completion `json:"completions"`
	CompletionsTotal int                `json:"completions_total"`
	CompletionsFrom  int                `json:"completions_from"`

	// AllCompletionsDurable is true iff every completion the core knows about has
	// DurableAt <= the last durable checkpoint. The enumerator MUST gate the
	// snapshot→stream handoff on this, and the core makes the check trivial so
	// nobody reimplements it wrongly.
	AllCompletionsDurable bool `json:"all_completions_durable"`

	// Readers is how much aggregate capacity exists. Pull-based assignment means
	// the enumerator does not need a load model; it needs only "is anyone asking
	// for work". Demand is the number of splits readers have room for right now.
	Readers int `json:"readers"`
	Demand  int `json:"demand"`

	// LastCheckpoint is the highest durable checkpoint id.
	LastCheckpoint CheckpointID `json:"last_checkpoint"`

	// Catalog is the most recent discovery result, already reconciled. The
	// enumerator plans against this rather than calling Discover itself, so
	// discovery is cached, observable and rate-limited in one place.
	Catalog schema.Catalog `json:"catalog"`
}

// Enumeration is Poll's result. Splits are ALWAYS INCREMENTAL — they are added to
// the plan, never a replacement for it. Flink documents the same semantic on
// SplitsAssignment ("The assignment is always incremental") and it is worth
// stating on the type because the alternative invites an enumerator to recompute
// and resend its whole plan every tick.
type Enumeration struct {
	Status     PlanStatus    `json:"status"`
	RetryAfter time.Duration `json:"retry_after,omitempty"` // honoured only when PlanIdle

	// Splits to add. Each must have a SplitID not already present in the plan;
	// the core rejects a duplicate id as ClassPermanentContract rather than
	// silently overwriting, because a reused id silently corrupts completions.
	Splits []split.Split `json:"splits,omitempty"`

	// Retire withdraws splits that are no longer relevant — a stream was dropped,
	// a chunk turned out empty. Retiring an ASSIGNED split asks the holder to
	// revoke it first. Retiring a split with a completion also drops the
	// completion and decrements ExpectedCompletions, through split.Reconcile.
	Retire []SplitID `json:"retire,omitempty"`

	// Events are connector-defined control messages for readers. Delivery is
	// best-effort by contract; see §7.5.
	Events []ReaderEvent `json:"events,omitempty"`

	// Expect updates ExpectedCompletions when the enumerator learns the total.
	Expect opt.Opt[int] `json:"expect,omitempty"`

	// Backlog, when set, declares "I am working through a backlog" so the core
	// can favour throughput over latency: bigger batches, longer flush intervals,
	// no per-record timer. This is Flink's setIsProcessingBacklog as data instead
	// of a method, so it survives the wire.
	Backlog opt.Opt[bool] `json:"backlog,omitempty"`
}

// PlanStatus tells the core when to call Poll again. Three-valued, exactly like
// Flink's InputStatus, and for the same reason: a two-valued answer cannot
// distinguish "come back immediately" from "come back later" from "never come
// back", and the third is what makes a bounded pipeline terminate.
type PlanStatus uint8

const (
	// PlanMore: more splits are available now. The core calls Poll again
	// immediately (subject to backpressure from Demand).
	PlanMore PlanStatus = iota + 1

	// PlanIdle: nothing right now, but there may be later. The core calls Poll
	// again after RetryAfter, or sooner if a Report arrives or Demand rises.
	PlanIdle

	// PlanComplete: no further splits will EVER be produced. Terminal. The core
	// sets PlanState.NoMoreSplits, stops calling Poll, and the pipeline reaches
	// PhaseCompleted once every split has finished.
	//
	// This is the terminal state Kafka Connect cannot express: it has no
	// END_OF_INPUT, no Completed status, and a batch pipeline there is a streaming
	// pipeline that stops producing.
	PlanComplete
)

// Report is a batch of reader-originated facts. Batched, not one call per fact,
// because in the coordinated deployment each call is an RPC and a parallel
// snapshot finishing 200 chunks in a second must not be 200 round trips.
type Report struct {
	// Finished names splits whose read completed, with the watermark at which
	// each became consistent. For a Scan split this watermark is what the handoff
	// is built from.
	Finished []Finish `json:"finished,omitempty"`

	// Returned carries splits back with their last-known cursors, because a
	// reader was revoked, drained, or lost its lease. The enumerator normally
	// does nothing with these — the core puts them back in Unassigned — but a
	// connector that must release an upstream resource per split gets the hook.
	//
	// This is Flink's addSplitsBack, and the property it buys is worth restating:
	// worker loss returns UNFINISHED WORK WITH ITS CURSOR to the planner, instead
	// of requiring a replacement worker to be started, told which offsets to
	// resume from, and trusted to do it. Ownership transfers atomically with a
	// snapshot, never with a message.
	Returned []split.Split `json:"returned,omitempty"`

	// Events are connector-defined messages from readers.
	Events []EnumeratorEvent `json:"events,omitempty"`

	// Failed reports a reader that could not read a split at all, with the
	// classified reason. The enumerator may retire the split, re-plan it
	// differently (a chunk that is too large to read in one go), or do nothing
	// and let the core's retry policy handle it.
	Failed []SplitFailure `json:"failed,omitempty"`
}

type Finish struct {
	Split     SplitID      `json:"split"`
	Watermark ord.Position `json:"watermark"`
	Records   int64        `json:"records"`
	Bytes     int64        `json:"bytes"`
}

type SplitFailure struct {
	Split    SplitID    `json:"split"`
	Class    fail.Class `json:"class"`
	Attempts int        `json:"attempts"`
	Reason   string     `json:"reason"`  // operator-facing
	Detail   string     `json:"detail"`  // developer-facing
}
```

### 7.3 `Reader` — the worker

```go
// Reader reads assigned splits. Many Readers may be live per (pipeline,
// generation); each is owned by one worker.
//
// THREADING: the core calls Assign, Revoke, Fetch, Snapshot and Close from a
// single goroutine per Reader, never concurrently. A Reader that wants
// concurrency inside itself — a fetcher goroutine per split, an errgroup over a
// connection pool — owns that entirely, and Fetch is where its results surface.
// That is the same confinement Flink achieves with a fetcher thread plus a
// FutureCompletingBlockingQueue, expressed as "the core will not call you
// concurrently" instead of ~400 lines of queue.
type Reader interface {
	// Assign adds splits. Always incremental, and IDEMPOTENT ON SplitID: assigning
	// a split the reader already holds is a no-op, because assignment delivery is
	// treated as lossy and may be retried. A reader that panics or errors on a
	// duplicate assignment is broken.
	//
	// The reader must begin reading only splits it can serve; if it has no
	// capacity it should still accept the split (the core placed it against
	// declared capacity) and read it when it can.
	Assign(ctx context.Context, splits []split.Split) error

	// Revoke asks the reader to stop reading the named splits and return them with
	// their current cursors. The returned slice must contain exactly the splits
	// named, in any order, with Cursor set to the last position at which a resume
	// would be correct.
	//
	// Revoke is called for rebalancing, for graceful drain, and when a split is
	// retired. It is separate from Close because assignment-scoped lifecycle and
	// instance-scoped lifecycle are different things — Connect's
	// SinkTask.open(partitions)/close(partitions) versus start/stop, a separation
	// its own javadoc insists on and which Benthos and Vector lack entirely.
	Revoke(ctx context.Context, ids []SplitID) ([]split.Split, error)

	// Fetch returns the next available records. It MUST NOT block indefinitely: it
	// returns FetchIdle with a RetryAfter when nothing is available, and the core
	// does not busy-wait.
	//
	// Fetch is the one hot-path method in the entire plugin surface. Everything
	// else is per-split or per-checkpoint.
	Fetch(ctx context.Context) (Fetch, error)

	// Snapshot returns the current state of EVERY split the reader holds,
	// including splits it has finished but whose completion the enumerator has not
	// yet acknowledged.
	//
	// THIS IS THE READER'S ENTIRE DURABLE STATE. There is no other source-side
	// checkpoint. The core persists the returned splits verbatim and, on restore,
	// hands them straight back through Assign. The returned slice is also what
	// makes reconciliation free: the core compares it against PlanState.Assigned
	// at every checkpoint and repairs any divergence.
	Snapshot(ctx context.Context, id CheckpointID) ([]split.Split, error)

	// Close releases the reader. Splits it still holds are NOT implicitly
	// returned; the core recovers them from the last checkpoint, which is the
	// only source of truth that survives a kill -9.
	Close(ctx context.Context) error
}

// Fetch is one round of reading. It may carry batches from several splits so that
// a reader holding thousands of splits is not forced into one round trip per
// split, while each batch keeps its own cursor and therefore its own ordering
// scope.
type Fetch struct {
	Batches []SplitBatch `json:"batches,omitempty"`

	Status     FetchStatus   `json:"status"`
	RetryAfter time.Duration `json:"retry_after,omitempty"` // honoured only when FetchIdle
}

// SplitBatch is records from exactly one split, plus the split's cursor after
// them. Ordering within a SplitBatch is the split's read order; ordering across
// SplitBatches is undefined.
type SplitBatch struct {
	Split   SplitID         `json:"split"`
	Records []record.Record `json:"records"`

	// Cursor is the position at which this split may be resumed, AFTER the
	// records in this batch have been durably written downstream.
	//
	// ABSENT MEANS "NOT A SAFE RESUME POINT". The core will not commit against
	// these records; it will hold them in the prefix tracker until a later batch
	// offers a cursor, and commit that cursor once everything up to it has
	// settled. This is how a CDC reader expresses "we are mid-transaction, do not
	// resume here" and how a snapshot reader over an unordered stream expresses
	// "I cannot resume at all, replay me from the start".
	//
	// This is a typed property, not the nil convention Benthos uses ("// This has
	// no offset - it's a snapshot message"), and it is the mechanism that answers
	// D3's "model safe-resume-point distinctly from record-position" without a
	// second field on every record.
	Cursor opt.Opt[blob.VersionedBlob] `json:"cursor,omitempty"`

	// Watermark and Done finish a split. Done means the split's work is complete
	// and the reader will produce nothing further for it; Watermark carries the
	// position at which it became consistent and is REQUIRED for a Scan split
	// whose source declares Replayable, because the handoff is built from it.
	//
	// The core does not report the completion to the enumerator until every record
	// in this batch has settled downstream. A completion reported early would let
	// the change read start after a watermark whose scan rows were never written.
	Done      bool                  `json:"done,omitempty"`
	Watermark opt.Opt[ord.Position] `json:"watermark,omitempty"`

	// Schema, when set, declares that the records that follow are under a new
	// schema. The core records it into the split's checkpointable state whether
	// or not the pipeline emits schema-change events downstream, applies the
	// pipeline's drift policy, and quiesces the sink before any DDL — see §11.5.
	Schema opt.Opt[schema.Ref] `json:"schema,omitempty"`

	// Heartbeat marks a batch with no records whose only purpose is to advance
	// Cursor. It exists because an idle stream's stored position must not age out
	// of the upstream's retention window: Flink CDC's MySQL source documents
	// exactly this failure ("the binlog file or GTID set may have been cleaned in
	// its last committed binlog position. The CDC job may restart fails in this
	// case") and ships a 30-second heartbeat for it. Benthos ships it as a
	// per-connector hack. It belongs in core, and here it costs one bool.
	Heartbeat bool `json:"heartbeat,omitempty"`
}

// FetchStatus is three-valued for the same reason PlanStatus is.
type FetchStatus uint8

const (
	FetchMore    FetchStatus = iota + 1 // call Fetch again immediately
	FetchIdle                           // nothing now; call again after RetryAfter
	FetchDrained                        // every assigned split is Done; the reader is empty
)
```

### 7.4 The contexts — how the core grows capabilities without breaking connectors

```go
// EnumeratorContext is the core's capability grant to an enumerator.
//
// EVERY new core capability is added HERE, never to Enumerator or Reader. The
// core implements the context, so adding a method to it breaks nothing. Connect's
// counter-example is instructive: adding pluginMetrics() to ConnectorContext put
// `catch (NoSuchMethodError | NoClassDefFoundError e)` into the OFFICIAL JAVADOC
// as the recommended calling convention. Go has no default methods at all, so
// this rule is not a preference — it is the only growth path that exists.
type EnumeratorContext interface {
	// Pipeline identifies the pipeline and tenant. Used for logging and for keys
	// the connector may need to scope; the connector never constructs store keys
	// itself.
	Pipeline() PipelineRef

	// Log returns a *slog.Logger already tagged with pipeline, connector, role and
	// generation. A connector never names a log field the core also names.
	Log() *slog.Logger

	// Metrics returns a metric registrar. The CONNECTOR NEVER NAMES A METRIC:
	// it registers a counter/gauge/histogram by a semantic role from a closed
	// enum, and the core owns the name, the unit, the tags and the export. This
	// is Connect's PluginMetrics and Flink's typed SourceReaderMetricGroup, and
	// it is the only defence against label-cardinality explosions and against
	// two connectors naming the same quantity differently (R9).
	Metrics() Metrics

	// Split returns a split by id from the plan, for an enumerator that needs to
	// inspect one it did not just mint (PlanView carries ids, not splits).
	Split(id SplitID) (split.Split, bool)

	// Go runs fn on a worker goroutine and delivers the result back ON THE
	// ENUMERATOR GOROUTINE as an EnumeratorEvent, so the enumerator can do slow
	// I/O without ever touching its own state off-thread. This is callAsync with
	// the footgun removed: there is no way to mutate enumerator state from fn,
	// because fn cannot see the enumerator.
	Go(fn func(ctx context.Context) (EnumeratorEvent, error))

	// RequestCheckpoint asks the core to take a checkpoint soon. An enumerator
	// calls it when it has reached a boundary worth making durable — every chunk
	// completion, the moment before a handoff. Advisory: the core coalesces
	// requests and honours its own minimum interval.
	RequestCheckpoint(reason string)

	// Secrets resolves a secret reference from the config without the value ever
	// appearing in a log, a metric, the read model or an error string.
	Secrets() SecretResolver

	// Clock is the pipeline's clock. Injected so that skew policy, lease
	// arithmetic and heartbeat intervals are all testable without sleeping.
	Clock() Clock
}

// ReaderContext is the core's capability grant to a reader.
type ReaderContext interface {
	Pipeline() PipelineRef
	Worker() WorkerID
	Epoch() uint64
	Log() *slog.Logger
	Metrics() Metrics
	Secrets() SecretResolver
	Clock() Clock

	// Framer returns the pipeline's configured deframer, so a byte-oriented
	// source does not implement framing. Its shape deliberately matches
	// bufio.SplitFunc — Go already has the right signature for this and inventing
	// a second one would be gratuitous:
	//
	//	func(data []byte, atEOF bool) (advance int, token []byte, err error)
	//
	// A reader over a byte stream loops on Framer().Scan and emits one record per
	// token. One frame becoming many records is the DECODER's job (§8.4), which is
	// how a JSON array in one frame produces N records correctly.
	Framer() Framer

	// SendEvent sends a connector-defined event to the enumerator. Best-effort by
	// contract (§7.5).
	SendEvent(e EnumeratorEvent)

	// RequestSplits signals that this reader has capacity for n more splits. This
	// is the pull side of pull-based assignment: the reader asks when it has room,
	// so the core needs no load model and there is no rebalance storm to have.
	// KIP-415 replaced stop-the-world rebalancing after documented storms and its
	// replacement shipped its own imbalance bug; pull-based assignment does not
	// have the problem to solve.
	RequestSplits(n int)

	// RequestCheckpoint, as on EnumeratorContext.
	RequestCheckpoint(reason string)
}
```

### 7.5 Control events, and the rule that no protocol depends on delivery

```go
// ReaderEvent and EnumeratorEvent are the bidirectional in-band control channel,
// built from day one because Flink's entire out-of-band control plane is one line
// (`public interface SourceEvent extends Serializable {}`) and Debezium had to
// invent a DATABASE SIGNALLING TABLE — requiring DDL and write access on the
// user's source — purely because Kafka Connect offers no way to command a running
// task. canal owns its runtime and has no excuse.
//
// Unlike Flink's empty marker interface, these are (kind, blob) pairs so they
// cross a process boundary with no registration step and no reflection.
type ReaderEvent struct {
	// To names the target. Absent means broadcast to every reader holding a split.
	To      opt.Opt[SplitID]   `json:"to,omitempty"`
	Kind    string             `json:"kind"`
	Payload blob.VersionedBlob `json:"payload"`
}

type EnumeratorEvent struct {
	From    opt.Opt[SplitID]   `json:"from,omitempty"`
	Kind    string             `json:"kind"`
	Payload blob.VersionedBlob `json:"payload"`
}
```

**The rule, stated on both types and enforced by the connector conformance suite:** *every control event may be
lost, duplicated or reordered.* An enumerator or reader whose correctness depends on one arriving is broken.
The core provides the repair mechanism so nobody hand-rolls it:

```go
// Reconciler is core machinery, not connector machinery. On a configurable
// interval (default 30s) it:
//
//  1. compares PlanState.Assigned against each reader's last Snapshot and
//     repairs divergence in both directions (re-Assign a split the reader lost;
//     record an assignment the plan missed);
//  2. re-delivers Completions the enumerator has not acknowledged;
//  3. re-delivers ReaderEvents whose effect the plan cannot yet observe.
//
// Flink CDC does exactly this with a 30-second syncWithReaders sweep, and the
// comment on it names why: "when the IncrementalSourceEnumerator restores or the
// communication failed ... it may missed some notification event." Building the
// sweep first, rather than after the bug reports, is the whole lesson.
type Reconciler struct { /* engine-internal */ }
```

Idempotent reconciliation instead of acknowledgement bookkeeping is also what makes the *distributed* case cheap:
a worker that was partitioned for 40 seconds rejoins and converges without any special path.

---

## 8. The sink: `Sink` → `Writer` + optional `Committer`

### 8.1 The tier table — the guarantee is which interfaces you implement

| Sink implements | End-to-end tier | What the core does |
|---|---|---|
| `Writer` | at-least-once | advance the per-split committable prefix when `Write` reports records durable |
| `+ SupportsWriterState` | at-least-once, in-progress work survives restart | restore writer state before the first `Write` |
| `+ SupportsCommitter` | exactly-once via 2PC | ready/pending sets under the Checkpoint Subsuming Contract |
| `+ SupportsTokenStore` | exactly-once, one durability domain | recover the checkpoint token from the destination |

This is Flink's design and it is better than a config enum for one specific reason: **a config enum can lie and
an interface cannot.** The core computes the pipeline guarantee as
`min(source capability, sink capability, declared intent)` and **refuses an impossible pipeline at submit time**
(§10.3). Vector's most dangerous documented behaviour is the opposite — enable acknowledgements, wire a
non-propagating sink, get best-effort silently, even though every input to that decision is known at config
time.

### 8.2 `Sink` and `Writer`

```go
// Sink is a connector's factory, symmetric with Source: validated config, no
// connection, no goroutine, no I/O in the constructor.
type Sink interface {
	// NewWriter creates a writer. Assignment-scoped setup happens in
	// Writer.Open, not here.
	NewWriter(ctx context.Context, wc WriterContext) (Writer, error)

	// Close releases anything the factory holds.
	Close(ctx context.Context) error
}

// Writer writes batches. This is the ENTIRE required sink surface, and it
// deliberately contains no progress concept: no offsets, no positions, no
// checkpoint callback, no ack function. A sink cannot get checkpointing wrong
// because a sink is not told what a checkpoint is.
//
// That is Benthos's single best decision (WriteBatch(ctx, batch) error is its
// complete output contract) preserved intact, while the core keeps the position
// mapping Benthos gives up. The core knows which records came from which split at
// which sequence, so "batch N landed" is mechanically convertible into "split S is
// committable up to cursor C" — see §13.6.
//
// THREADING: the core calls Open, Write, Flush and Close from a single goroutine
// per Writer, never concurrently. Concurrency across writers is the core's, via
// declared MaxInFlight (§12.2).
type Writer interface {
	// Open is assignment-scoped setup: create a session, begin a transaction,
	// ensure a target exists. It is called before the first Write and again after
	// any reconnect, and it receives what the writer needs to prepare the
	// destination — notably the schemas in force and, for a restore, the id of the
	// checkpoint being resumed from.
	Open(ctx context.Context, o Opening) error

	// Write attempts to make every record in b durable in the destination.
	//
	// SUCCESS AND FAILURE SHAPES, decided together (R7):
	//
	//	(res, nil) with res.Failed empty
	//	    → every record in b is DURABLE. This return value is the acknowledgement
	//	      and the core will advance the checkpoint past these records. A writer
	//	      that returns it before the data is durable has lied, and that is the
	//	      only way to violate R4 in this design — which is the point: R4 becomes
	//	      a type obligation rather than prose in an RFC.
	//
	//	(res, nil) with res.Failed non-empty
	//	    → every record NOT named in res.Failed is durable; the named ones are
	//	      not, each with its own class and reason.
	//
	//	(res, err) with res.Failed non-empty
	//	    → same as above, and err is the headline summary for logs and status.
	//	      This is Benthos's BatchError shape ("if Failed is not called then all
	//	      messages are assumed to have failed; if it is called at least once
	//	      then all indexes not explicitly failed are assumed successful") with
	//	      its cause fixed: canal correlates by record.RecordID, so there is no
	//	      positional correspondence to lose and no Indexer/SortGroup machinery
	//	      to write.
	//
	//	(res, err) with res.Failed empty
	//	    → NOTHING in b is claimed durable. This is the graceful degradation
	//	      path for a sink that cannot report granularity, and it is the correct
	//	      default: the whole batch is retried.
	//
	// Write MUST NOT partially apply and report total success. If it cannot know
	// what landed, it returns an error with Failed empty and Class
	// ClassTransientUpstream, and the core retries the whole batch — which is why
	// idempotency layers exist (§13.8).
	Write(ctx context.Context, b record.Batch) (WriteResult, error)

	// Flush makes previously written data durable when the writer buffers
	// internally. Called before every checkpoint, on drain, and on end of input.
	// A writer that makes each Write durable implements Flush as `return nil`.
	//
	// reason exists so a writer can behave differently on end-of-input (close the
	// file, finalise the manifest) than on a periodic checkpoint.
	Flush(ctx context.Context, reason FlushReason) error

	// Close releases the writer. Called after a final Flush on graceful shutdown,
	// and without one on failure.
	Close(ctx context.Context) error
}

// Opening is what a writer is given at Open. A struct rather than parameters so
// that adding a field later is not a breaking change to every sink.
type Opening struct {
	// Restored is the checkpoint being resumed from, absent on a cold start. This
	// is Flink's InitContext.getRestoredCheckpointId() returning OptionalLong: the
	// cold-start-vs-restore discriminator expressed as data rather than as two
	// constructors, which is the right choice on the SINK side because a writer's
	// setup does not otherwise differ.
	Restored opt.Opt[CheckpointID] `json:"restored,omitempty"`

	// Schemas are the schemas the writer will see, so it can create or alter the
	// destination BEFORE the first record that needs it. This is why Open exists
	// at all rather than folding into NewWriter.
	Schemas []schema.Entry `json:"schemas"`

	// Streams names the logical streams that will be written, with the operator's
	// chosen destination mode per stream (append / overwrite / upsert).
	Streams []schema.ConfiguredStream `json:"streams"`

	// Guarantee is the tier the core computed and validated. A writer may assert
	// on it — a writer that requires upsert semantics and is handed
	// DestinationAppend should fail Open loudly rather than write wrong data.
	Guarantee Tier `json:"guarantee"`
}

type FlushReason uint8

const (
	FlushCheckpoint FlushReason = iota + 1
	FlushEndOfInput
	FlushDrain
	FlushSchemaChange // the core is about to apply a DDL; quiesce first (§11.5)
)

// WriteResult reports what happened, per record where the sink can say.
type WriteResult struct {
	// Failed names records that did not land. Every other record in the batch is
	// durable if err == nil, or if err != nil and Failed is non-empty.
	Failed []RecordFailure `json:"failed,omitempty"`

	// Duplicates names records the destination recognised as already present. They
	// count as DURABLE (that is the whole point of an idempotent write) but are
	// reported separately so the operator can see the rate. A high duplicate rate
	// after a restart is expected; a sustained one is a symptom.
	//
	// Reporting duplicates as success rather than failure is the direct fix for the
	// R5 bug where an event became permanently unresubmittable: "duplicate" here
	// means "already durably stored", never "present in a RAM cache".
	Duplicates []record.RecordID `json:"duplicates,omitempty"`

	// Written and Bytes feed the reconciliation metric pair (records in vs records
	// out per checkpoint), which is the only cheap way to notice a sink that
	// silently drops.
	Written int64 `json:"written"`
	Bytes   int64 `json:"bytes"`
}

// RecordFailure is one record's failure, classified by the connector at the point
// of raise, with the two audiences separated.
type RecordFailure struct {
	Record record.RecordID `json:"record"`
	Class  fail.Class      `json:"class"`
	// Reason is operator-facing and appears in the UI and the DLQ record. It must
	// name the count, the component, an example, the fix and the consequence — the
	// standard Benthos sets in its strict-mode warning and the best operator
	// message in the study.
	Reason string `json:"reason"`
	// Detail is developer-facing: the upstream error string, the stack. It goes to
	// logs and the DLQ, never to the UI's primary surface. One string cannot serve
	// both audiences and Airbyte's separate message/internal_message is right.
	Detail string `json:"detail,omitempty"`
}
```

### 8.3 `SupportsCommitter` — two-phase commit, with values instead of mutable callbacks

```go
// SupportsCommitter upgrades a sink to exactly-once via two-phase commit.
//
// The strict ordering is documented here because it is a contract and not an
// implementation detail:
//
//	Flush(FlushCheckpoint)
//	  → PrepareCommit(id)   returns the committables for checkpoint id
//	  → [ writer state, committables and split cursors are written as ONE
//	      durable record under id ]
//	  → Commit(requests)    called ONLY after that record is durable
//
// Failure anywhere before durability means the checkpoint did not happen and the
// next one covers a longer span. Failure between durability and Commit means
// Commit runs again after restart against the SAME committables, so Commit MUST
// BE IDEMPOTENT and signals so per item with DispositionAlreadyCommitted.
type SupportsCommitter interface {
	// PrepareCommit mints the committables for checkpoint id: the staged artifacts
	// that will become visible when the checkpoint is durable. It is called after
	// Flush and before the checkpoint is written.
	//
	// A committable is (version, bytes) — never a handle, a file descriptor, an
	// open transaction object or a closure. That constraint is what makes 2PC
	// survive a process restart AND a process boundary, so it is a feature rather
	// than a tax.
	PrepareCommit(ctx context.Context, id CheckpointID) ([]Committable, error)

	// Commit publishes committables. It is handed requests and returns outcomes,
	// one per request, in any order, keyed by Committable.ID.
	//
	// This diverges from Flink deliberately. Flink's CommitRequest is a MUTABLE
	// CALLBACK OBJECT with six signalling methods, which cannot cross a process
	// boundary and cannot be table-tested. canal passes values in and values out.
	// Same expressive power, wire-safe, and the outcome set is a closed enum a
	// metric can be labelled with.
	Commit(ctx context.Context, reqs []CommitRequest) ([]CommitOutcome, error)

	// Close releases committer resources.
	CloseCommitter(ctx context.Context) error
}

// Committable is a staged artifact.
type Committable struct {
	// ID is core-assigned and stable across restarts, so an outcome can name a
	// request and a metric can count retries per artifact.
	ID CommittableID `json:"id"`
	// Blob is the connector's own description of the artifact: a staged object
	// key, a transaction id, a temp table name.
	Blob blob.VersionedBlob `json:"blob"`
	// Splits records which splits' records this artifact covers, so that a
	// committable that ends in DispositionDeadLetter marks exactly the right
	// splits degraded rather than the whole pipeline.
	Splits []SplitID `json:"splits"`
	// Expires is when this committable's underlying resource stops being
	// commitable — a transaction timeout, a staged-upload TTL. The core warns as it
	// approaches and FAILS LOUDLY when it passes. Flink's transactionTimeout +
	// ignoreFailuresAfterTransactionTimeout + a warning ratio is the honest
	// treatment; silently skipping an expired committable is not.
	Expires opt.Opt[time.Time] `json:"expires,omitempty"`
}

type CommitRequest struct {
	Committable Committable `json:"committable"`
	// Attempts is how many times this committable has been offered before. It lets
	// a committer distinguish "first try" from "we have been here 40 times", and it
	// is what a bounded-retry policy is enforced against.
	Attempts int `json:"attempts"`
}

type CommitOutcome struct {
	// ID echoes the request.
	ID CommittableID `json:"id"`
	Disposition Disposition `json:"disposition"`
	// Committable is set only for DispositionRetryUpdated, carrying the revised
	// artifact — Flink's updateAndRetryLater, which is the partial-success case
	// and the reason a boolean return is insufficient.
	Committable opt.Opt[Committable] `json:"committable,omitempty"`
	Class  fail.Class `json:"class,omitempty"`
	Reason string     `json:"reason,omitempty"`
	Detail string     `json:"detail,omitempty"`
	// RetryAfter is honoured for the two retry dispositions; the core applies
	// full-jitter exponential backoff if absent.
	RetryAfter time.Duration `json:"retry_after,omitempty"`
}

// Disposition is the closed per-item outcome vocabulary. FIVE values, where Flink
// has six, and the difference is deliberate: Flink's signalFailedWithKnownReason
// is documented as "only logs the error, discards the committable and continues",
// which is a terminal outcome that silently discards data. canal has no such
// outcome. Every permanent failure produces a dead-letter record and a visible
// status change.
type Disposition uint8

const (
	// DispositionCommitted: durable now.
	DispositionCommitted Disposition = iota + 1
	// DispositionAlreadyCommitted: was durable before this call. Idempotent
	// no-op. Counted separately so a restart's replay is visible.
	DispositionAlreadyCommitted
	// DispositionRetryLater: transient. The core re-offers on the next commit
	// cycle with Attempts incremented, bounded by the retry policy.
	DispositionRetryLater
	// DispositionRetryUpdated: partially succeeded; retry with the returned
	// Committable instead.
	DispositionRetryUpdated
	// DispositionDeadLetter: permanent. The core writes a dead-letter record per
	// covered split with full provenance, sets Degraded with the reason, and does
	// NOT advance the prefix past the covered records.
	DispositionDeadLetter
)
```

### 8.4 Codecs — the sink never names a wire format

Per D2, serialisation is not a connector concern. Three independently registered stage types, all configured on
the pipeline, all composable with every connector:

```go
// Decoder turns one framed byte token into one OR MORE records. One-frame-to-many
// is in the signature because a JSON array in one frame, a multi-record protobuf
// envelope and a CSV chunk are all real, and a signature that returns one record
// makes them impossible or dishonest.
type Decoder interface {
	Decode(ctx context.Context, frame []byte, in record.Record) ([]record.Record, error)
	// Accepts declares which schema shapes this decoder can produce, so
	// configuring a decoder that cannot serve the sink's requirement is a SUBMIT
	// TIME error. This is Vector's codec input_type()/schema_requirement() as
	// declared data.
	Accepts() schema.Requirement
}

// Encoder turns records into bytes. It receives a whole batch because some wire
// formats are batch-shaped (a JSON array, an Avro OCF block, a Parquet row group)
// and pretending otherwise forces every such codec to buffer behind the
// interface.
type Encoder interface {
	Encode(ctx context.Context, b record.Batch) ([]byte, error)
	Requires() schema.Requirement
}

// Framer both splits an incoming byte stream and delimits an outgoing one. Its
// scanning half deliberately matches bufio.SplitFunc, because Go already has the
// right signature and inventing a second is gratuitous.
type Framer interface {
	Scan(data []byte, atEOF bool) (advance int, token []byte, err error)
	Frame(dst []byte, payload []byte) []byte
}

// Compressor is the third stage. Separate because compression is orthogonal to
// both encoding and framing: newline-delimited-JSON-gzipped is three independent
// choices, not one codec named "ndjson_gzip".
type Compressor interface {
	Compress(dst, src []byte) ([]byte, error)
	Decompress(dst, src []byte) ([]byte, error)
}

// SupportsStructuredInput is the narrow, declared escape hatch for a sink whose
// SDK requires structured input — a BigQuery Storage Write client, a Mongo driver,
// a typed gRPC stub. Such a sink receives records and the core REFUSES to attach
// an encoder to it, rather than silently double-encoding.
//
// It is a declared capability cross-checked at registration, not a type assertion
// discovered at runtime, because a type assertion cannot cross a process boundary
// (decision-space trap 11: Benthos's gRPC path had to demote AutoRetryNacks to a
// bool in the init response for exactly this reason).
type SupportsStructuredInput interface {
	// StructuredInput is a marker. The capability struct declares it; this
	// interface makes the declaration checkable against the type.
	StructuredInput()
}
```

**One-source-unit-to-many-records requires an ack aggregator, and the framework provides it.** A decoder that
turns one frame into 200 records must not let the core commit that frame's cursor until all 200 have settled.
The core owns this: `Origin.Seq` is assigned per *source record*, decoded children share their parent's
`Origin`, and `prefix.Tracker` counts descendants (§13.6). Benthos ships a 45-line
`AutoAggregateBatchScannerAcks` reduction so no connector author hand-writes fan-out ack counting; here no
connector author can even see the counting, because it is a property of `Origin` rather than of a callback.

---

## 9. Optional capability interfaces — the complete set

### 9.1 The rule

**Behaviour is an optional interface. The *fact* of that behaviour is declarative data. The two are cross-checked
at registration.**

This is FLIP-372's explicit written design rule (*"Every new feature should be added with a
`Supports<FeatureName>` interface … No redefining interface methods during interface inheritance … Minimal
inheritance extension"*) combined with the observation that a type assertion cannot cross a process boundary.
Declaring `Chunkable: true` without implementing `SupportsChunkedSnapshot` is a **registration-time panic**
(§10.2), so the flag can never be a lie and the interface can never be undiscoverable.

Every optional interface is **exported** (Benthos left `batchedCache` unexported, so implementers had to learn it
from prose rather than from the type system — a defect not to copy). Every optional interface is type-asserted in
**exactly one place**, `registry.resolve`, producing a struct of non-nil-or-nil function values, which is the
generic forwarding helper Benthos lacks and the reason it needs nine hand-written probe forwarders per
capability.

### 9.2 Source-side optional interfaces

```go
// SupportsChunkedSnapshot lets the core run its generic chunked-snapshot engine
// (§13.7) over this source. The source's obligation shrinks to two operations;
// everything else — the watermark protocol, the resumable splitter cursor, the
// per-chunk backfill split, the stream-phase filter, the filter's retirement,
// chunk-level progress — is core code, written once.
//
// That is the payoff for putting chunking in core rather than in each connector:
// Benthos proves the alternative, where every source reimplements chunking, none
// of them resumably, and the platform can see none of it.
type SupportsChunkedSnapshot interface {
	// SplitKeySpace produces the next batch of chunk ranges for a stream, starting
	// from `after` (absent = from the beginning). It returns at most `max` ranges
	// and a cursor to resume splitting from.
	//
	// Ranges MUST be unbounded at both ends of the key space — (-inf, k1), [k1,k2),
	// ..., [kn, +inf) — so that rows inserted outside the observed min/max after
	// splitting began are still covered by some chunk. That requirement is the
	// single most commonly-missed part of the algorithm and it is stated at the
	// method.
	SplitKeySpace(ctx context.Context, s schema.StreamID, after opt.Opt[blob.VersionedBlob], max int) (ChunkPage, error)

	// ReadRange is the bounded read of one chunk. It is what the reader calls for a
	// Scan split, and it is separate from Fetch only conceptually — a source
	// implements Fetch by dispatching on the assigned split's Kind. Declaring
	// ReadRange here documents the CONTRACT the core relies on: the read must
	// observe a consistent view of the range, and the source must be able to name
	// the positions immediately before and after it.
	ReadRange(ctx context.Context, s split.Split, want int) (SplitBatch, error)
}

// ChunkPage is one page of chunk ranges plus the splitter's own resume cursor.
// Persisting that cursor is what makes "the enumerator was halfway through slicing
// a 4-billion-row table when we crashed" resumable — Flink CDC's ChunkSplitterState
// {currentSplittingTableId, nextChunkStart, nextChunkId}, generalised and made a
// return value instead of a class.
type ChunkPage struct {
	Ranges []ord.KeyRange             `json:"ranges"`
	Next   opt.Opt[blob.VersionedBlob] `json:"next,omitempty"` // absent = this stream is fully split
	// Estimated totals for the denominator in the UI. Optional; omitted rather
	// than zeroed.
	EstimatedRanges opt.Opt[int] `json:"estimated_ranges,omitempty"`
}

// SupportsReplay lets the core read changes between two positions, which is what
// makes the per-chunk backfill possible and therefore what makes a chunked
// snapshot exactly-once rather than merely parallel.
//
// A source implementing this can be handed a Change split with a bounded
// PositionRange and must read exactly [From, To).
type SupportsReplay interface {
	// EarliestPosition is the oldest position still readable. The core uses it to
	// detect a checkpoint that has aged out of retention and to raise
	// ClassPermanentUpstream with an actionable message rather than failing
	// obscurely on the first read.
	EarliestPosition(ctx context.Context) (ord.Position, error)
	// CurrentPosition is the newest position, used for the position-fraction
	// progress metric and for bounding a snapshot-only pipeline.
	CurrentPosition(ctx context.Context) (ord.Position, error)
}

// Backlogger volunteers "how much work is left upstream". Optional because for
// many sources it is unknowable or expensive.
//
// Exact and AsOf are MANDATORY fields, not conveniences: a SELECT COUNT(*) and a
// reltuples estimate must not render identically in a UI, and a polled backlog
// without AsOf implies a liveness it does not have.
type Backlogger interface {
	Backlog(ctx context.Context, s schema.StreamID) (Backlog, error)
}

type Backlog struct {
	Records opt.Opt[int64] `json:"records,omitempty"`
	Bytes   opt.Opt[int64] `json:"bytes,omitempty"`
	Exact   bool           `json:"exact"`
	AsOf    time.Time      `json:"as_of"`
}

// SupportsPauseResume lets the core pause reading of specific splits without
// revoking them — used when one split is far ahead of others and the sink is the
// bottleneck, and when an operator pauses a stream.
//
// It is a BATCHED ATOMIC call, not N per-split calls, with the documented safety
// property that no other Reader method runs in parallel so the update is atomic.
// That is Flink's pauseOrResumeSplits shape, and Flink shipped it as a
// default-throwing method on a required interface — which is precisely the mistake
// this whole section exists to avoid.
type SupportsPauseResume interface {
	PauseOrResume(ctx context.Context, pause, resume []SplitID) error
}

// ConnectionTestable runs named connection tests without starting a pipeline. It
// returns a LIST OF RESULTS, never a bool and never one bool plus one string
// (Airbyte's check, which is useless to a form).
type ConnectionTestable interface {
	ConnectionTest(ctx context.Context) []TestResult
}

type TestResult struct {
	Name    string        `json:"name"`   // "resolve host", "authenticate", "read permission on orders"
	OK      bool          `json:"ok"`
	Class   fail.Class    `json:"class,omitempty"`
	Message string        `json:"message,omitempty"`
	Took    time.Duration `json:"took"`
	// Field, when set, attaches this result to a config field so the UI can show
	// it inline instead of in a modal.
	Field opt.Opt[cfg.Path] `json:"field,omitempty"`
}

// DynamicChoices resolves a named dynamic-choice hook the config spec referenced
// (`ChoicesFrom: "list_tables"`). It is a NAMED HOOK resolved by a core→connector
// call rather than a callback embedded in the spec, because a callback cannot
// cross a process boundary and cannot be served to a browser. This is Connect's
// Recommender turned into data plus a call.
type DynamicChoices interface {
	Choices(ctx context.Context, hook string, partial cfg.Config) ([]cfg.Choice, error)
}

// Validatable is the network-touching half of two-tier validation. It returns
// PER-FIELD DIAGNOSTICS, ALL AT ONCE — never fail-fast, never one bool and one
// string.
type Validatable interface {
	Validate(ctx context.Context, c cfg.Config) []cfg.Diagnostic
}

// AdoptsState declares which other connectors' persisted state this connector can
// read, so replacing a connector with a rewrite is a DECLARATION rather than an
// operator runbook. Flink's WithCompatibleState, generalised to the whole plugin.
type AdoptsState interface {
	AdoptsStateOf() []string // e.g. ["postgres@1", "postgres-legacy@3"]
}
```

### 9.3 Sink-side optional interfaces

```go
// SupportsWriterState makes in-progress sink work survive a restart: a partly
// written object, an open multipart upload, a staged batch.
type SupportsWriterState interface {
	SnapshotWriter(ctx context.Context, id CheckpointID) ([]blob.VersionedBlob, error)
	RestoreWriter(ctx context.Context, state []blob.VersionedBlob) error
}

// SupportsTokenStore is the strongest available guarantee: the sink stores canal's
// own checkpoint token transactionally with the data, so "the data landed but the
// state didn't" is structurally impossible. Available only for destinations with
// transactions (a SQL target, an object store with a manifest), and therefore an
// optional capability rather than the default.
//
// The token is opaque to the sink — it is the core's encoded checkpoint id and
// generation — which is what keeps the destination from becoming authoritative
// about anything except its own durability.
type SupportsTokenStore interface {
	WriteWithToken(ctx context.Context, b record.Batch, token []byte) (WriteResult, error)
	RecoverToken(ctx context.Context) (opt.Opt[[]byte], error)
}

// SupportsSchemaApply lets a sink apply a schema change to its destination. The
// core owns the DRIFT POLICY (what to do) and the sink owns the MECHANISM (how to
// do it), which is what stops "we found a new column" from becoming an
// unanswerable question in every sink.
type SupportsSchemaApply interface {
	// AppliesKinds declares which change kinds this sink can perform, so the core
	// can refuse an impossible drift policy at submit time.
	AppliesKinds() []schema.ChangeKind
	// ApplySchemaChange is called after the core has quiesced and flushed
	// everything written under the old schema.
	ApplySchemaChange(ctx context.Context, c schema.Change) error
}

// SupportsPartitionedBatching lets a sink declare that batches should be grouped
// by a key it computes, and the core then keeps one open batch per key with its
// own limits. Per-tenant or per-table batching with no batching code in the sink.
type SupportsPartitionedBatching interface {
	PartitionKey(r record.Record) (string, error)
}

// SupportsBatchSplit is the inverse of batching and it is a real gap in every
// surveyed system: "a batch policy has the capability to create batches, but not
// to break them down", while sinks almost always have a hard maximum request size.
// A sink declaring this is handed oversized batches and returns the sub-batches it
// wants; the core then tracks each independently.
type SupportsBatchSplit interface {
	SplitBatch(b record.Batch) []record.Batch
}
```

### 9.4 Why no type parameters on any of these

Every interface above is monomorphic. `Committable`, `Split`, `blob.VersionedBlob` are concrete types, not type
parameters. The reason is documented twice in prior art:

- FLIP-191 needed **an entirely new package** because *"there is not an easy way to replace/remove the
  `GlobalCommitter` … due to the typed parameters."*
- FLIP-372 names inner-interface coupling as what *"prevented the evolution of the `TwoPhaseCommittingSink`."*
- Flink shipped **three incompatible Sink APIs in eight minor releases**.

Constraint #5 says gratuitous generics are a defect. The concrete meaning adopted here: **generics on a concrete
helper are good; generics on a plugin interface are a future migration.** `opt.Opt`, `blob.Serializer`,
`prefix.Tracker` and `registry.RegisterSource` are helpers. Nothing a connector implements has a type parameter,
and there is an `archtest` assertion that no exported interface in `connector/` does.

---

## 10. Declared capabilities, the registry, and submit-time refusal

### 10.1 The capability structs

```go
// SourceCapabilities is plain serialisable data, declared at registration, and it
// is the ONLY thing the core, the registry API and the UI need in order to know
// what a source can do. It is queryable WITHOUT INSTANTIATING ANYTHING, which is
// what makes the connector-catalogue UI possible.
//
// A struct with named fields, never a tuple. Benthos's constructor returns
// (out, batchPolicy, maxInFlight, err) and every new capability widens the tuple;
// this grows by adding a field, which is a source-compatible change.
type SourceCapabilities struct {
	// APIVersion is the (api_version, capabilities) handshake. Benthos's gRPC
	// boundary has none, so an older plugin cannot know it is missing a required
	// semantic. The core refuses to load a connector whose APIVersion it does not
	// support, with a message naming both versions.
	APIVersion int `json:"api_version"`

	// Chunkable ⇒ implements SupportsChunkedSnapshot.
	Chunkable bool `json:"chunkable"`
	// Replayable ⇒ implements SupportsReplay.
	Replayable bool `json:"replayable"`
	// Pausable ⇒ implements SupportsPauseResume.
	Pausable bool `json:"pausable"`
	// Backlog ⇒ implements Backlogger.
	Backlog bool `json:"backlog"`
	// Testable ⇒ implements ConnectionTestable.
	Testable bool `json:"testable"`

	// ComparablePositions declares that every Position this source emits carries
	// ord.Order. Chunkable requires it; a source declaring Chunkable without it is
	// refused at registration, so "the chunk filter cannot compare anything" is
	// impossible rather than a production discovery.
	ComparablePositions bool `json:"comparable_positions"`
	// ComparableKeys declares the same for Keys. Also required by Chunkable.
	ComparableKeys bool `json:"comparable_keys"`

	// MidSplitResume declares that a split's cursor may be committed at any batch
	// boundary the reader offers one. False means the only legal resume points are
	// split start and split completion — a source over an unordered paginated API,
	// for instance. The core then refuses to commit interpolated cursors rather
	// than doing it silently.
	MidSplitResume bool `json:"mid_split_resume"`

	// MaxSplitsPerReader is the source's own cap on concurrent splits per reader,
	// usually driven by a connection pool. The placer honours it. Hard-enforced,
	// because tasks.max was advisory in Connect for eight years until KIP-1004 had
	// to enforce it against "buggy or hostile connectors ... threatening cluster
	// stability", shipping a deprecated escape hatch for connectors already
	// violating it.
	MaxSplitsPerReader int `json:"max_splits_per_reader"`

	// MaxSplitsTotal is the cap on the plan size. Exceeding it is a
	// ClassPermanentContract failure of the ENUMERATOR, named as such, not a
	// gradual OOM.
	MaxSplitsTotal int `json:"max_splits_total"`

	// Batching is the source-side batch policy the core enforces on the input side
	// — which every CDC source needs, and which Benthos can only do per connector.
	Batching cfg.BatchPolicy `json:"batching"`

	// ChangeFacet declares whether this source populates record.Change, and which
	// Ops it can emit. A sink requiring upserts is refused against a source that
	// cannot emit a Key.
	ChangeFacet   bool             `json:"change_facet"`
	Ops           []record.Op      `json:"ops,omitempty"`
	SchemaChanges []schema.ChangeKind `json:"schema_changes,omitempty"`

	// StructuredPayload declares that this source produces structured payloads
	// natively, so the core knows not to attach a decoder.
	StructuredPayload bool `json:"structured_payload"`
}

// SinkCapabilities, symmetric.
type SinkCapabilities struct {
	APIVersion int `json:"api_version"`

	Committer   bool `json:"committer"`     // ⇒ SupportsCommitter
	WriterState bool `json:"writer_state"`  // ⇒ SupportsWriterState
	TokenStore  bool `json:"token_store"`   // ⇒ SupportsTokenStore
	SchemaApply bool `json:"schema_apply"`  // ⇒ SupportsSchemaApply
	Testable    bool `json:"testable"`
	Partitioned bool `json:"partitioned"`   // ⇒ SupportsPartitionedBatching
	Splittable  bool `json:"splittable"`    // ⇒ SupportsBatchSplit

	// Tier is the strongest end-to-end guarantee this sink can participate in.
	// Derived from the flags above at registration and cross-checked against them,
	// so it is a convenience for the UI rather than an independent claim.
	Tier Tier `json:"tier"`

	// Modes declares which destination modes this sink implements. A pipeline
	// configured for upsert against a sink declaring only append is refused at
	// submit time — which is the check whose absence makes Flink CDC's
	// exactly-once claim silently false for an append-only sink.
	Modes []schema.DestinationMode `json:"modes"`

	// StructuredInput ⇒ SupportsStructuredInput; the core attaches no encoder.
	StructuredInput bool `json:"structured_input"`

	// PerRecordFailures declares that Write can report which individual records
	// failed. False means the core always retries whole batches, which is correct
	// but coarser, and the UI says so instead of the operator inferring it.
	PerRecordFailures bool `json:"per_record_failures"`

	// Idempotent declares that writing the same record twice is harmless. It is
	// what lets the core choose at-least-once with duplicates over blocking, and it
	// is what makes chunked-snapshot output safe.
	Idempotent bool `json:"idempotent"`

	Batching    cfg.BatchPolicy `json:"batching"`
	MaxInFlight int             `json:"max_in_flight"`
}

type Tier uint8

const (
	TierAtLeastOnce Tier = iota + 1
	TierExactlyOnce2PC
	TierExactlyOnceTransactional
)
```

### 10.2 Registration — a generic function that checks flags against types at init

```go
package registry

// SourceSpec is the complete, declarative description of a source connector. It
// is DATA: nothing on it is a callback except Open, which is the constructor.
//
// The type parameters E and R exist for exactly one reason: they let Register
// verify at registration time that the declared capabilities match the types that
// will implement them, without instantiating anything and without reflection.
// They do not appear on any interface a connector implements.
type SourceSpec[E connector.Enumerator, R connector.Reader] struct {
	// Name is the registry key and the wire identifier. Immutable forever: a
	// rename is a new connector plus AdoptsStateOf on the new one.
	Name string
	// Version is the connector's own semantic version, surfaced in the UI and
	// recorded in every checkpoint header so an operator can see which build wrote
	// the state.
	Version string
	// Title, Summary, Docs and Notes are what the UI renders. Notes is where a
	// source that derives Origin.Upstream documents the derivation, per R5.
	Title, Summary, Docs, Notes string
	// Support is a closed enum {Community, Beta, Certified, Deprecated} rendered as
	// a badge, and it is the ONE place a support level is expressed — no second
	// vocabulary, no cross-map (R9).
	Support SupportLevel

	Config       *cfg.Spec
	Capabilities connector.SourceCapabilities

	// Open constructs the Source from validated config.
	Open func(ctx context.Context, c cfg.Config, sc connector.SourceContext) (connector.Source, error)
}

// RegisterSource adds a source to the global registry.
//
// It PANICS at init time if:
//   - the name is already registered;
//   - a declared capability has no corresponding interface implementation on E or
//     R (checked via a nil typed value: `var e E; _, ok := any(e).(SupportsChunkedSnapshot)`
//     works because method sets belong to types, so no instance is needed);
//   - an implemented optional interface is not declared (the reverse check —
//     silently-unused capabilities are as bad as lying ones);
//   - Chunkable is declared without ComparableKeys and ComparablePositions;
//   - the config spec fails its own lint (unknown composite, duplicate path,
//     predicate referencing a nonexistent field);
//   - APIVersion is outside the range this core supports.
//
// Panicking at init is the right severity. Kafka Connect is still on
// plugin.discovery=HYBRID_WARN years into migrating off classpath scanning, and
// its capability tri-states are nullable enums checked at runtime. An init-time
// registry with a static consistency check simply does not have those problems,
// and a mismatch is a programming error the connector author must see on their
// first `go test ./...`.
func RegisterSource[E connector.Enumerator, R connector.Reader](s SourceSpec[E, R])

// RegisterSink, RegisterCodec, RegisterFramer, RegisterCompressor,
// RegisterTransform and RegisterBuffer are the same shape.
func RegisterSink[W connector.Writer](s SinkSpec[W])

// Registry is a VALUE TYPE with a default global instance, which is Benthos's
// design and the right one: Clone/With/Without give tests, sandboxing and
// allow-listing with no global mutation and no build tags.
//
// Vector's compile-time feature-flag registry is the same idea with worse
// ergonomics; Connect's ServiceLoader scanning is a solved-by-avoidance problem.
type Registry struct{ /* unexported maps */ }

func Global() *Registry
func (r *Registry) Clone() *Registry
func (r *Registry) Without(names ...string) *Registry
func (r *Registry) With(other *Registry) *Registry

// Descriptor is the cached, instantiation-free projection the control API serves
// and the UI renders. Producing it requires no connector code to run, which is
// what makes the connector list page fast and safe.
type Descriptor struct {
	Kind         string                    `json:"kind"` // "source" | "sink" | "codec" | ...
	Name         string                    `json:"name"`
	Version      string                    `json:"version"`
	Title        string                    `json:"title"`
	Summary      string                    `json:"summary"`
	Docs         string                    `json:"docs"`
	Notes        string                    `json:"notes,omitempty"`
	Support      SupportLevel              `json:"support"`
	Config       *cfg.Spec                 `json:"config"`
	Capabilities json.RawMessage           `json:"capabilities"`
	// JSONSchema is generated from Config, for editors and for a browser that
	// wants off-the-shelf validation. Generated, never hand-maintained, so it
	// cannot drift (R8).
	JSONSchema json.RawMessage `json:"json_schema"`
}

func (r *Registry) Descriptors() []Descriptor
func (r *Registry) Descriptor(kind, name string) (Descriptor, bool)

// resolve is the ONE place in canal that type-asserts an optional interface. It
// returns a struct of nilable function values that the engine calls without
// further assertions, which is the forwarding helper Benthos lacks — its
// ConnectionTestable probe needs nine hand-written forwarders through
// airGapReader, maxInFlight, autoRetryNacksBatched and friends.
func resolve(src connector.Source, e connector.Enumerator, rd connector.Reader) resolvedSource
```

### 10.3 Submit-time refusal — the checks that must never be production discoveries

`engine.Validate(spec) []cfg.Diagnostic` runs before a pipeline is accepted, and returns **all** problems with
per-field attribution. The checks, each with the prior-art failure it prevents:

| Check | Prevents |
|---|---|
| declared guarantee ≤ `min(source, sink)` tier | Vector's silent acknowledgement degradation (trap 16) |
| chunked snapshot ⇒ sink declares upsert mode **and** `Idempotent` | Flink CDC's exactly-once claim being silently false against an append-only sink |
| chunked snapshot ⇒ source declares `ComparableKeys` + `ComparablePositions` | a chunk filter that cannot compare, discovered mid-snapshot |
| chunked snapshot ⇒ source declares `Replayable` | a per-chunk backfill that cannot be read |
| encoder attached ⇔ sink is not `StructuredInput` | silent double-encoding |
| decoder's `Accepts()` ⊇ encoder's `Requires()` | a codec pair that cannot compose, discovered on record 1 |
| per-stream source mode ∈ discovered `SupportedModes` | Airbyte's configured-vs-supported mismatch |
| per-stream destination mode ∈ sink's `Modes` | upsert configured against an append-only sink |
| drift policy's change kinds ⊆ sink's `AppliesKinds()` | a schema change nobody can apply, at 3am |
| `MidSplitResume` false ⇒ buffer is not durable-ack | a cursor committed at a position the source cannot resume from |
| declared parallelism ≤ `MaxSplitsPerReader × workers` | Connect's eight-year advisory `tasks.max` |
| every `Predicate` in the config spec references an existing path | a form whose conditional fields never appear |
| store supports atomic multi-key `Commit` when tier > at-least-once | Connect's documented unrecoverable compacted-topic state |

The last one is worth naming: **the deployment's stores are part of what is validated.** `canal run` with the
bolt store satisfies atomic multi-key commit (one bbolt transaction); a hypothetical store that cannot is
refused for 2PC pipelines at submit time rather than corrupting them at recovery time.

---

## 11. Config declaration, discovery, schema and drift

### 11.1 `cfg.Spec` — one declaration, five consumers

One spec per connector is simultaneously: the validator, the defaulter, the docs source, the JSON Schema for
editors, and the UI form. Benthos proves this is the *entire* answer to the frontend goal with zero
per-connector UI code, and Connect proves the same with fourteen fields per key of which five are purely
presentational.

```go
package cfg

// Spec is a connector's complete configuration declaration. THE DECLARATIVE FORM
// IS PRIMARY and the Go builder below is sugar over it, so the in-process and
// out-of-process vocabularies cannot diverge — Benthos's did, and its
// out-of-process plugins lost Secret, Optional, Examples, nesting and lint rules.
type Spec struct {
	Fields []Field `json:"fields"`
	// Groups order and label field groups in the UI. Presentation lives in the
	// spec because the alternative is presentation living in the frontend, which
	// means per-connector frontend code.
	Groups []Group `json:"groups,omitempty"`
	// Constraints are cross-field rules the browser can evaluate.
	Constraints []Predicate `json:"constraints,omitempty"`
}

// Field is one configuration key. Addressed by variadic Path segments, NEVER a
// dotted string — Connect's ConfigDef cannot describe nesting and fakes
// transforms=a,b + transforms.a.type with dotted prefixes and bespoke enrich()
// logic.
type Field struct {
	Path Path     `json:"path"`
	Kind Kind     `json:"kind"`

	Title       string `json:"title"`
	Description string `json:"description"`
	// ShortDescription is explicitly for inline help in a form UI. Benthos names
	// it that way and the name is the documentation.
	ShortDescription string `json:"short_description,omitempty"`

	// The tri-state required/has-default/optional, plus a presence test. Benthos's
	// Default-doubles-as-required-marker with Optional() as the third state is
	// exactly right and needs no fourth flag.
	Default  opt.Opt[any] `json:"default,omitempty"`
	Optional bool         `json:"optional,omitempty"`

	Advanced   bool   `json:"advanced,omitempty"`
	Deprecated string `json:"deprecated,omitempty"` // non-empty = deprecated, and this is the replacement advice
	// Secret ⇒ the core owns redaction, masking, encryption at rest and
	// secret-reference indirection. The API NEVER returns the value. This is one
	// bool in the connector's own spec buying the whole secret story with zero
	// per-connector knowledge.
	Secret   bool     `json:"secret,omitempty"`
	Examples []any    `json:"examples,omitempty"`
	Version  string   `json:"version,omitempty"` // "added in 1.4"

	// Presentation, copied from Connect because a usable form needs it.
	Group string `json:"group,omitempty"`
	Order int    `json:"order,omitempty"`
	Width Width  `json:"width,omitempty"`

	// Choices is a static closed set. ChoicesFrom names a DynamicChoices hook
	// resolved by a core→connector call at form-render time — data plus a named
	// hook rather than a live callback, because a callback cannot cross a process
	// boundary and cannot be served to a browser.
	Choices     []Choice `json:"choices,omitempty"`
	ChoicesFrom string   `json:"choices_from,omitempty"`

	// RequiredIf and VisibleIf are DECLARATIVE PREDICATES over the config, so the
	// BROWSER CAN EVALUATE THEM TOO. Connect's Recommender.visible() and Segment's
	// predicate `required` are the precedents; expressing them as data rather than
	// as callbacks is what makes live form behaviour possible without a round trip
	// per keystroke.
	RequiredIf opt.Opt[Predicate] `json:"required_if,omitempty"`
	VisibleIf  opt.Opt[Predicate] `json:"visible_if,omitempty"`

	// Children for KindObject / KindObjectList / KindObjectMap.
	Children []Field `json:"children,omitempty"`
	// Variants for KindUnion: a discriminated union with a const tag. This is the
	// one thing JSON Schema's oneOf+const does that ConfigDef provably cannot, and
	// auth-method selection needs it in almost every real connector.
	Discriminator string    `json:"discriminator,omitempty"`
	Variants      []Variant `json:"variants,omitempty"`

	// Component makes a field hold ANOTHER REGISTERED COMPONENT, resolved from
	// config. This one field is how fan-out, routing, retry-at-sink, fallback and
	// transform chains exist with no core special-casing (§15) — it is the same
	// mechanism as topology composition, so it costs nothing extra.
	Component opt.Opt[ComponentKind] `json:"component,omitempty"`
}

type Kind uint8

const (
	KindString Kind = iota + 1
	KindInt
	KindFloat
	KindBool
	KindDuration
	KindBytesSize
	KindEnum
	KindStringList
	KindStringMap
	KindObject
	KindObjectList
	KindObjectMap
	KindUnion
	KindComponent
	KindExpression // a field-extraction expression over a record; see §11.6
)

// Predicate is a small CLOSED vocabulary a browser can interpret. Deliberately
// not an embedded expression language: Benthos's Bloblang makes lint rules, batch
// predicates and default mappings all data a browser could evaluate, which is
// genuinely clever and genuinely a large dependency. Six operators cover every
// real conditional-form case seen in the dossiers, and Validatable covers the
// rest.
type Predicate struct {
	Op    PredicateOp `json:"op"`
	Path  Path        `json:"path,omitempty"`
	Value any         `json:"value,omitempty"`
	Paths []Path      `json:"paths,omitempty"`
	Args  []Predicate `json:"args,omitempty"` // for And/Or/Not
}

type PredicateOp uint8

const (
	PredEquals PredicateOp = iota + 1
	PredIn
	PredPresent
	PredMutuallyExclusive
	PredAtLeastOneOf
	PredAnd
	PredOr
	PredNot
)
```

### 11.2 Composite field specs — the mechanism that makes connectors small

```go
// Composites are reusable field groups paired with typed extractors. THIS IS THE
// KILLER FEATURE, and it is the reason "adding a sink is: declare fields,
// implement Write, register" holds: retry, backoff, batching, TLS, buffering,
// codec choice and metadata filtering become CONFIGURATION, identical across every
// connector, documented identically, rendered identically, with zero coordination
// between connector authors and zero core switches.
//
// Each composite is a (spec-fragment, extractor) pair so the shape and the reader
// cannot drift.
func BatchingField(name string, def BatchPolicy) Field
func Batching(c Config, p Path) (BatchPolicy, error)

func RetryField(name string, def RetryPolicy) Field
func Retry(c Config, p Path) (RetryPolicy, error)

func TLSField(name string) Field
func TLS(c Config, p Path) (*tls.Config, error)

func BufferField(name string, def BufferPolicy) Field
func Buffer(c Config, p Path) (BufferPolicy, error)

func CodecField(name string) Field      // encoder | decoder, from the codec registry
func FramingField(name string) Field
func CompressionField(name string) Field
func MetadataFilterField(name string) Field
func MaxInFlightField(name string, def int) Field
func URLField(name string) Field

// BatchPolicy has FOUR ORTHOGONAL TRIGGERS. Any satisfied trigger flushes.
type BatchPolicy struct {
	Count  int           `json:"count,omitempty"`
	Bytes  int64         `json:"bytes,omitempty"`
	Period time.Duration `json:"period,omitempty"`
	// Until is a predicate over the record — the fourth trigger, which lets a
	// batch close on a transaction boundary or a schema change. Without it, a CDC
	// source cannot batch on transaction boundaries without its own batcher.
	Until opt.Opt[Predicate] `json:"until,omitempty"`
}

// RetryPolicy is {maxAttempts, backoff, terminal disposition} — not Benthos's
// binary "replay indefinitely" vs "delete". Unbounded retry by default is a
// livelock generator, and Benthos's community-reported symptom is a pipeline
// making no progress on one poison record.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"` // 0 is invalid; -1 means unbounded and must be set explicitly
	Initial     time.Duration `json:"initial"`
	Max         time.Duration `json:"max"`
	// Jitter is full by default. Full-jitter exponential everywhere, and
	// Retry-After honoured on rate limits, is the policy table from
	// design-rules.md's "ideas worth carrying forward".
	Jitter    JitterKind        `json:"jitter"`
	Terminal  TerminalKind      `json:"terminal"` // DeadLetter | Pause | Fail
	RespectRetryAfter bool      `json:"respect_retry_after"`
}

// Config is the parsed, validated, defaulted configuration handed to a
// constructor. Field access uses ONE generic function plus one deferred error,
// rather than Benthos's (T, error) accessor per type — which produces a 20-line
// error ladder per constructor for errors the spec already made impossible, and is
// the single biggest ergonomic tax in that API.
type Config struct{ /* unexported */ }

func Field[T any](c Config, p ...string) T
func (c Config) Err() error
func (c Config) Contains(p ...string) bool

// Decode fills a struct derived from the same Spec, for connectors that prefer it.
// Generated from the spec, so the struct and the spec cannot disagree.
func Decode[T any](c Config, dst *T) error

// Diagnostic is what both validation tiers return: per-field, all at once, with a
// severity and machine-readable position.
type Diagnostic struct {
	Path     Path     `json:"path,omitempty"`
	Severity Severity `json:"severity"` // Error | Warning | Info
	Code     Code     `json:"code"`     // closed enum, includes CodeUnknownField
	Message  string   `json:"message"`
	// Line and Column for YAML/JSON sources, so an editor can underline. Silently
	// ignored config keys are the classic YAML-tool failure, so CodeUnknownField
	// exists and defaults to Error.
	Line, Column int `json:"line,omitempty"`
	// Recommended offers valid values, which is what turns an error into a fix.
	Recommended []any `json:"recommended,omitempty"`
}
```

### 11.3 Discovery and the catalog

```go
package schema

// Catalog is what Discover returns: what the source FOUND. It is persisted
// verbatim, and ConfiguredCatalog embeds it verbatim, so "what the source found"
// and "what the operator chose" are structurally distinct and diffable.
//
// Drift is then a DIFF against the persisted catalog rather than a runtime
// surprise, and the UI gets a stream picker with zero connector-specific frontend
// code.
type Catalog struct {
	Streams []Stream  `json:"streams"`
	// DiscoveredAt is mandatory: a catalog without it implies a currency it does
	// not have.
	DiscoveredAt time.Time `json:"discovered_at"`
	// Partial is set when discovery was truncated (a source with millions of
	// objects). The UI then offers a filter rather than pretending the list is
	// complete.
	Partial bool `json:"partial"`
}

type Stream struct {
	ID     StreamID `json:"id"`
	Title  string   `json:"title"`
	Schema Schema   `json:"schema"`

	// Keys and Cursors are [][]string — LISTS OF FIELD PATHS — so composite and
	// nested keys need no later breaking change. Airbyte's choice, and it is
	// correct.
	Keys    [][]string `json:"keys,omitempty"`
	Cursors [][]string `json:"cursors,omitempty"`

	// SourceDefinedCursor / SourceDefinedKey let a connector CONSTRAIN OPERATOR
	// CHOICE IN DATA rather than in code: the UI greys the selector out and the
	// submit-time validator enforces it.
	SourceDefinedCursor bool `json:"source_defined_cursor"`
	SourceDefinedKey    bool `json:"source_defined_key"`

	// SupportedModes is per-stream capability. Note that this is per STREAM, not
	// per connector: a source may be able to tail one table and only full-scan
	// another.
	SupportedModes []StreamMode `json:"supported_modes"`

	// Chunkable and Resumable are per-stream too, for the same reason.
	Chunkable bool `json:"chunkable"`
	Resumable bool `json:"resumable"`

	EstimatedRecords opt.Opt[int64] `json:"estimated_records,omitempty"`
	EstimatedBytes   opt.Opt[int64] `json:"estimated_bytes,omitempty"`
}

// StreamMode and DestinationMode are ORTHOGONAL ENUMS, and that orthogonality is
// the single cleanest thing in the Airbyte protocol: the source never learns
// whether the sink overwrites, appends or upserts, which is what makes M×N
// combinations free.
//
// Note what this replaces: there is no "pipeline type". A connection can
// full-scan five lookup tables and tail two large ones in the same pipeline with
// no pipeline-type branching anywhere. Pipeline type is data (R1).
type StreamMode uint8

const (
	ModeFullScan    StreamMode = iota + 1 // Scan splits only; re-scan every run
	ModeIncremental                       // Change splits only, from a cursor
	ModeScanThenTail                      // Scan splits, then a Change split (the hybrid)
)

type DestinationMode uint8

const (
	DestAppend DestinationMode = iota + 1
	DestOverwrite
	DestUpsert       // requires a key
	DestUpsertDelete // upsert plus honouring OpDelete
)

// ConfiguredCatalog is the operator's choice, embedding the discovery it was made
// against.
type ConfiguredCatalog struct {
	Discovered Catalog            `json:"discovered"`
	Streams    []ConfiguredStream `json:"streams"`
}

type ConfiguredStream struct {
	ID          StreamID        `json:"id"`
	Selected    bool            `json:"selected"`
	Mode        StreamMode      `json:"mode"`
	Destination DestinationMode `json:"destination"`
	Key         [][]string      `json:"key,omitempty"`
	Cursor      [][]string      `json:"cursor,omitempty"`
	// Fields selects a projection. Empty = all.
	Fields [][]string `json:"fields,omitempty"`
	// Target renames the destination object.
	Target string `json:"target,omitempty"`
}

// OneStream builds a single-stream catalog. This is the whole answer to "what is
// the minimum viable Discover for a source with no catalog" — a webhook receiver
// writes one line.
func OneStream(id StreamID, s Schema) Catalog
```

### 11.4 Schema, the canonical type set, and drift

```go
// Schema is a structural description. Identity is a SHA-256 STRUCTURAL
// FINGERPRINT, not a name and not a registry id, so two independently discovered
// identical schemas are one entry in the plan's schema table and a per-sink
// conversion can be memoised.
type Schema struct {
	Fields []SchemaField `json:"fields"`
}

func (s Schema) Fingerprint() Fingerprint

// Ref is how a split and a record point at a schema without carrying it.
type Ref struct{ FP Fingerprint }

// Entry is the plan's stored schema, deduplicated.
type Entry struct {
	FP     Fingerprint `json:"fp"`
	Stream StreamID    `json:"stream"`
	Schema Schema      `json:"schema"`
	// Epoch increments per stream on each change, and the epoch is what makes
	// "which schema was in force for this historical log record" answerable.
	Epoch uint64 `json:"epoch"`
}

type SchemaField struct {
	Name     string        `json:"name"`
	Type     Type          `json:"type"`
	Nullable bool          `json:"nullable"`
	// Logical carries parameterised logical types in a SEPARATE STRUCT, so adding
	// a new parameterised type does not widen the Type enum.
	//
	// Deliberately NOT Connect's approach — a naming convention
	// (name() == "org.apache.kafka.connect.data.Decimal") plus a parameters() map —
	// which its own dossier names as a documented source of silent disagreement
	// between bad connectors and bad converters.
	Logical  opt.Opt[Logical] `json:"logical,omitempty"`
	Children []SchemaField    `json:"children,omitempty"` // struct/list/map
	Default  opt.Opt[any]     `json:"default,omitempty"`
	Doc      string           `json:"doc,omitempty"`
}

// Type is the canonical type set: the LOSSLESS INTERSECTION of the formats canal
// intends to support. Deliberately small.
type Type uint8

const (
	TypeBool Type = iota + 1
	TypeInt64
	TypeFloat64
	TypeString
	TypeBytes
	TypeStruct
	TypeList
	TypeMap
	// TypeUnknown is the honest escape hatch for a source that cannot report a
	// type, carried through as bytes plus the source's own type name in Logical.
	TypeUnknown
)

type Logical struct {
	Kind LogicalKind `json:"kind"` // Decimal | Date | Time | Timestamp | UUID | JSON | Enum | Interval
	// Precision/Scale/TZ/Values as needed. BigDecimal-style arbitrary precision is
	// representable via Precision == 0, which is the explicit escape hatch for
	// sources that cannot report precision.
	Precision int      `json:"precision,omitempty"`
	Scale     int      `json:"scale,omitempty"`
	TZ        string   `json:"tz,omitempty"`
	Values    []string `json:"values,omitempty"`
	SourceName string  `json:"source_name,omitempty"` // for TypeUnknown round-tripping
}

// Change is a schema-change event. It is ORDERED IN-BAND: the core requires
// "schema before data", records it into the split's checkpointable state whether
// or not the pipeline emits it downstream, applies it ONLY during a Change split
// (a Scan split's schema is pinned at its start), and emits it downstream only if
// the pipeline asks.
type Change struct {
	Stream StreamID   `json:"stream"`
	Kind   ChangeKind `json:"kind"`
	From   opt.Opt[Ref] `json:"from,omitempty"`
	To     Ref        `json:"to"`
	// Field names the affected field for column-level kinds.
	Field []string `json:"field,omitempty"`
	At    time.Time `json:"at"`
}

type ChangeKind uint8

const (
	ChangeCreateStream ChangeKind = iota + 1
	ChangeAddField
	ChangeDropField
	ChangeRenameField
	ChangeAlterFieldType
	ChangeDropStream
	ChangeTruncateStream
)

// DriftPolicy is CORE CONFIGURATION, not a per-sink decision. Five modes, with
// never-destructive Lenient as the DEFAULT — which is Flink CDC's
// pipeline.schema.change.behavior, the only complete shipped answer to drift in
// any surveyed system, and the best-documented artifact of the lot.
type DriftPolicy struct {
	Mode DriftMode `json:"mode"`
	// Include/Exclude with prefix matching, exclude wins. Truncate and Drop are
	// ignored by default in Lenient; create is auto-added when includes are given.
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
}

type DriftMode uint8

const (
	// DriftLenient is the default and is defined as NEVER DESTRUCTIVE: an
	// AlterFieldType becomes RenameField + AddField, keeping the old field and
	// adding a new one, so no data is lost and no historical row becomes
	// unreadable.
	DriftLenient DriftMode = iota + 1
	DriftEvolve            // apply the change as given, including destructive ones
	DriftTryEvolve         // apply if the sink can; fall back to Lenient
	DriftIgnore            // track but never apply; new fields are dropped, and this is COUNTED
	DriftFail              // stop the pipeline with a named terminal condition
)
```

### 11.5 The quiesce rule

Applying a DDL downstream while records written under the old schema are in flight is a race with silent data
corruption as the prize. The core owns the sequence and no connector participates in it:

1. The reader's `SplitBatch.Schema` announces the new schema.
2. The core stops admitting records for that stream at the transform chain's input.
3. `Writer.Flush(ctx, FlushSchemaChange)` drains everything written under the old schema.
4. If the sink implements `SupportsSchemaApply` and the drift policy permits the kind,
   `ApplySchemaChange(ctx, change)` runs.
5. The new schema `Entry` and the split's new `Schema` ref are written **into the same checkpoint record as the
   split cursor**, so schema epoch and position are atomic.
6. Admission resumes.

Step 5 is the one that matters. Debezium's canonical failure — *"Encountered a change event whose schema isn't
known"* — is what two independently-committed stores (offsets and schema history) produce. One record, one
commit, no divergence class.

### 11.6 Sink field mappings — specialised UX with no core knowledge

```go
// A sink declares the fields IT wants, each with a default extraction expression
// over canal's generic record. The core knows only "fields" and "expressions"; the
// UI is automatically specialised per sink; adding a sink is "declare fields,
// implement Write, register".
//
// This is Segment's action-destination model, and it is the complete answer to
// constraint #1's "specialised UI/UX for less-generic connectors comes later and
// must NOT require core changes".
//
// The expression vocabulary is a small closed set of path selectors over the four
// record namespaces, precisely so a browser can render, evaluate and preview it
// without shipping an interpreter:
//
//	payload.customer.email     the structured payload
//	meta.source_table          the metadata namespace
//	change.key                 the change facet
//	change.after.status
//	origin.split               provenance
//	literal:"unknown"          a constant
//	coalesce(a, b, c)          first present
//
// Anything richer belongs in a transform, which is a registered component with a
// real language available to it — not in the sink's field mapping.
type Mapping struct {
	Field  string `json:"field"`
	Expr   string `json:"expr"`
	Static bool   `json:"static,omitempty"` // operator may not change it
}
```

---

## 12. Flow control: bounded by construction, accounted per split

### 12.1 Every edge is bounded, and the bound is measured in batches

R6 says a buffer without a rejection path is not a buffer. This design generalises it: **every edge in the
pipeline is a bounded Go channel, so unbounded growth is not expressible anywhere**, and slowness is transitive
by construction.

The hand-off between a reader's I/O goroutine and the pipeline is `chan SplitBatch` with **capacity 2**. Two
*batches*, not two records. Flink's `source.reader.element.queue.capacity` defaults to 2 and spent two major
features (FLIP-76 unaligned checkpoints, buffer debloating) undoing an early deep-buffering default; the
negative lesson is stated outright in its docs — *the more you buffer, the worse your checkpoint latency and
recovery time*. Choosing small bounded buffers at commit one removes an entire subsystem.

In Go this is a buffered channel plus `select` on `ctx.Done()`. Flink needed ~400 lines
(`FutureCompletingBlockingQueue`) to express it.

### 12.2 One in-flight concept, per split

```go
// InFlight is the SINGLE framework-owned accounting concept, replacing the five
// overlapping mechanisms Benthos accumulated (unbuffered transaction channel,
// output max_in_flight, input max_in_flight wrapper, checkpoint_limit via Capped,
// buffer limit) with their three user-facing names and their one DOCUMENTED
// DEADLOCK between two of them.
//
// It is PER SPLIT, and that is what makes it simultaneously:
//   - the ordering scope (§5.5),
//   - the checkpoint window bound (a split cannot have more than Limit records
//     outstanding, so the prefix tracker's memory is bounded by construction),
//   - the backpressure mechanism (Track blocks when full — four lines of
//     sync.Cond, which is where backpressure comes from for free),
//   - the fairness unit (a stalled split cannot starve others, which is exactly
//     the pathology Connect's own SubmittedRecords javadoc warns about: one
//     offline partition growing unboundedly while other partitions keep
//     dispatching).
type InFlight struct {
	// Limit is records per split, from the source's declared capabilities and
	// overridable per pipeline.
	Limit int
	// Bytes is a secondary cap so one split of 4 MiB records cannot dominate.
	Bytes int64
}
```

The engine exposes, per split: `inflight_records`, `inflight_bytes`, `blocked_seconds_total`, and a
`utilization` gauge in [0,1]. The **soft/hard backpressure split** is preserved as two distinct series —
*waiting for buffer capacity* and *blocked on the downstream stage* are different diagnoses and one number
cannot tell an operator which they have.

### 12.3 The buffer is a pluggable stage whose rejection path is in the type

```go
// Buffer is an optional stage between the transform chain and the encode/write
// stage. Its doc comment is deliberately discouraging, following Benthos's
// BatchBuffer, whose wording is the model: buffers are advanced components that
// weaken delivery guarantees, and if you are not sure you need one you do not.
//
// ONE buffer interface, bounded by construction (R6). Not two mutually
// incompatible abstractions in one package, which is what the abandoned canal
// attempt shipped.
type Buffer interface {
	// Push offers a batch. The return value NAMES THE OUTCOME rather than being a
	// silent success.
	Push(ctx context.Context, b record.Batch) (PushOutcome, error)
	// Pop takes the next batch, blocking until one exists or ctx is done.
	Pop(ctx context.Context) (record.Batch, error)
	// Trim discards everything durably handled up to a checkpoint. A buffer with no
	// trim-after-commit is a memory leak with a plan; the abandoned attempt's
	// shipping buffer omitted it along with metrics, 2 of the 4 operations its own
	// RFC mandated.
	Trim(ctx context.Context, id CheckpointID) error
	// Depth and Bytes are MANDATORY, not optional instrumentation.
	Depth(ctx context.Context) (records int64, bytes int64, err error)
	// Durable declares whether Push implies durability. This single bool decides
	// where the acknowledgement boundary sits, and the core wires the checkpoint
	// accordingly — see below.
	Durable() bool
	Close(ctx context.Context) error
}

// PushOutcome is R6 encoded as a type.
type PushOutcome uint8

const (
	PushAccepted PushOutcome = iota + 1
	// PushRejected: the buffer is full and configured to reject. The core applies
	// backpressure upstream and counts the event. There is no configuration under
	// which records are silently lost.
	PushRejected
	// PushOverflowed: the batch went to the next buffer tier (memory overflowing
	// into disk). Buffers chain.
	PushOverflowed
	// PushDroppedNewest: the batch was dropped under an explicit drop policy. THE
	// NEWEST is dropped, never the oldest, so the already-committable prefix
	// invariant holds. Counted in canal_buffer_dropped_total; a pipeline with a
	// non-zero value here reports Degraded.
	PushDroppedNewest
)

// WhenFull is the configured policy: block | reject | overflow | drop_newest.
// Vector's enum IS design-rule R6 encoded, and it is the reason unbounded growth
// is inexpressible in that system.
type WhenFull uint8
```

**The durability boundary decides where the checkpoint advances, automatically.** If `Durable()` is true the
buffer takes ownership of the records' prefix accounting on `Push`, and the checkpoint advances on buffer write.
If false, accounting passes through to the sink and the checkpoint advances on write confirmation. This is R4
implemented rather than written down, and it is why the checkpoint store and the buffer need **no shared WAL**:
the ordering is explicit and the checkpoint is always strictly downstream of whatever the durability boundary is.

The abandoned attempt's most dangerous gap was precisely this: its connector RFC told adapters to advance their
upstream checkpoint on a `202` that was emitted after appending to an unbounded, unsynced, in-memory slice in a
process with no signal handling. Making `Durable()` a method on the buffer, consulted by the core when it wires
the prefix tracker, means the documented advance rule and the actual durability of the thing being acknowledged
**cannot** be unrelated — they are the same expression.

### 12.4 Batching

```go
// Batcher is inverted control with no goroutine, so it drops into any select loop.
// Benthos's API, taken exactly, because it is what lets a SOURCE batch on the
// input side — which every CDC source needs and which no other surveyed system
// offers generically.
type Batcher interface {
	// Add returns true when the batch is ready to flush.
	Add(r record.Record) bool
	// UntilNext returns the duration until the period trigger fires, and false if
	// no timer is pending. The caller puts it in a select.
	UntilNext() (time.Duration, bool)
	// Flush returns and clears the current batch.
	Flush(ctx context.Context) (record.Batch, error)
	Close(ctx context.Context) error
}

// Splitter is the inverse and the gap every surveyed system has. A sink with a
// hard maximum request size gets it from configuration, not from its own code.
type Splitter interface {
	Split(b record.Batch) []record.Batch
}
```

Ack idempotency and ack deadlines are **framework guarantees**: the prefix tracker's settle operation is
idempotent by construction (settling a sequence twice is a no-op on a set), and there is **one timer wheel** for
in-flight deadlines rather than a goroutine-plus-timer per batch. Benthos reimplements `sync.Once` wrapping in
three separate places for the first and retrofits the second; neither is a design decision, only a cost of not
deciding early.

### 12.5 Clock-skew policy

Two timestamps exist and they mean different things: `Record.ObservedAt` (core-stamped, from the injected
`Clock`) and `Change.CommitTime` (the source's claim). Skew policy is configured **once, on the pipeline**, and
applies at ingest:

```go
type SkewPolicy struct {
	Mode SkewMode      `json:"mode"`
	Max  time.Duration `json:"max"` // default 5m
}

type SkewMode uint8

const (
	// SkewClamp (default): a CommitTime more than Max in the future is clamped to
	// ObservedAt, the original is preserved in Meta under a core-owned key, and
	// canal_clock_skew_seconds records the magnitude. Nothing is lost and the lag
	// metric stops being negative.
	SkewClamp SkewMode = iota + 1
	// SkewReject: the record is classified ClassClockSkew and dead-lettered. For
	// pipelines where a wrong timestamp is worse than a missing record.
	SkewReject
	// SkewPass: no policy. The lag series is then omitted rather than emitted
	// negative, because a negative lag is a lie and zero is a different lie.
	SkewPass
)
```

Records *behind* `ObservedAt` are normal — that is lag, not skew — and are never clamped.

---

## 13. Checkpoint, commit, dedupe, and the chunked-snapshot engine

### 13.1 The checkpoint record

```go
// CheckpointID is monotonic per pipeline generation, assigned by the core. Never
// reused, never reset, and strictly increasing across a generation bump.
type CheckpointID uint64

// Checkpoint is ONE DURABLE RECORD under ONE monotonic id, written in ONE store
// transaction. Everything that must be mutually consistent is inside it:
// positions, sink committables, writer state, schema epoch, dedupe watermarks.
//
// This is Flink's checkpoint property #1 (atomic position + derived state) obtained
// by writing one record rather than by running a barrier protocol. Properties #2
// and #3 — multi-input alignment and consistency across a shuffled graph — are
// what the whole aligned/unaligned/in-flight-persistence complexity exists to
// serve, and canal does not pay for them.
//
// The INTERFACE stays barrier-shaped: every stage has Snapshot(ctx, id), so a real
// barrier protocol could be inserted underneath with zero connector changes.
type Checkpoint struct {
	Header CheckpointHeader `json:"header"`

	// Plan is the enumerator side: its opaque state, unassigned splits,
	// assignments, completions, streams, schemas.
	Plan split.PlanState `json:"plan"`

	// Workers is the reader side: per worker, the splits it holds WITH THEIR
	// CURSORS. This is the whole of source-side progress. There is no offsets map
	// and no per-stream state struct, because a split's cursor is both.
	Workers map[string]WorkerState `json:"workers"`

	// Committables is the subsuming-contract PENDING SET: checkpoint id → the
	// artifacts minted at that id and not yet published. The whole map is stored
	// in every checkpoint, so a lost confirmation is repaired by the next
	// checkpoint rather than by bookkeeping.
	Committables map[CheckpointID][]Committable `json:"committables,omitempty"`

	// WriterState is per-writer opaque state from SupportsWriterState.
	WriterState map[string][]blob.VersionedBlob `json:"writer_state,omitempty"`

	// Dedupe carries the durable dedupe watermarks and, for unordered splits only,
	// the bounded key set. See §13.8.
	Dedupe DedupeState `json:"dedupe"`
}

// CheckpointHeader is the CORE-READABLE part, with a closed vocabulary. Opacity is
// non-negotiable for the payloads and a typed header is non-negotiable for the
// envelope: opacity is exactly why Connect has no source-side lag metric and why
// its offsets REST API can only show an operator a blob, and the header is the
// minimum the core needs to serve the frontend goal.
type CheckpointHeader struct {
	ID         CheckpointID `json:"id"`
	Pipeline   PipelineRef  `json:"pipeline"`
	Generation uint64       `json:"generation"`

	// Fence is the lease/epoch token of the writer. The store REJECTS a write
	// carrying a stale fence. This is the only safe use of leader election, and the
	// reason is verified: the k8s leaderelection package documents that it "does
	// not guarantee that only one client is acting as a leader (a.k.a. fencing)"
	// and that clients infer leadership from LOCALLY CAPTURED TIMESTAMPS.
	// Leadership must therefore never be trusted for correctness.
	Fence store.Fence `json:"fence"`

	// FormatVersion is canal's own envelope version, distinct from every
	// connector's blob version. Append-only field evolution; a core that reads a
	// FormatVersion it does not know REFUSES TO START with a named error rather
	// than partially parsing.
	FormatVersion int `json:"format_version"`

	// Phase is DERIVED from Plan and stored only so a checkpoint is
	// self-describing to an operator inspecting it offline. Never read by control
	// flow.
	Phase split.Phase `json:"phase"`

	// CommittedAt drives canal_checkpoint_age_seconds, the primary health metric:
	// always available, unfakeable, and one alert catches every stall mode.
	CommittedAt time.Time `json:"committed_at"`

	// RecordsIn and RecordsOut are per-checkpoint reconciliation. If a sink
	// silently drops, this pair is the only cheap way to notice.
	RecordsIn  int64 `json:"records_in"`
	RecordsOut int64 `json:"records_out"`

	// Connectors records name@version for the source, sink and every codec, so an
	// operator can see which build wrote this state and a restore can refuse an
	// incompatible one (or accept it via AdoptsStateOf).
	Connectors map[string]string `json:"connectors"`
}

type WorkerState struct {
	Worker WorkerID      `json:"worker"`
	Epoch  uint64        `json:"epoch"`
	Splits []split.Split `json:"splits"`
}
```

### 13.2 `store.CheckpointStore` — bytes in, bytes out, atomic across the whole map

```go
package store

// CheckpointStore persists checkpoints. It knows NOTHING about records, splits,
// schemas or connectors: it moves keys and bytes. That is Connect's
// OffsetBackingStore shape and it is precisely what makes the
// standalone↔distributed swap free.
type CheckpointStore interface {
	// Commit writes an entire checkpoint ATOMICALLY ACROSS EVERY KEY, or writes
	// nothing.
	//
	// This is the requirement Connect's compacted topic cannot meet, and the
	// consequence is documented in its own javadoc: compaction plus a partial write
	// can leave a commit marker whose task configs have been compacted away,
	// "leaving us in an inconsistent state with no obvious way to resolve the
	// issue" — so the class has to expose which connectors are inconsistent for the
	// herder to paper over. One SQL transaction, one bbolt transaction, one etcd
	// Txn. Kafka as the coordination store is explicitly rejected.
	//
	// fence is checked against the stored fence for this pipeline; a stale fence
	// returns ErrFenced and writes nothing.
	Commit(ctx context.Context, p PipelineKey, fence Fence, entries []Entry) error

	// Load returns the highest committed checkpoint's entries.
	Load(ctx context.Context, p PipelineKey) ([]Entry, CheckpointID, error)

	// LoadAt returns a specific checkpoint, for operator inspection and for
	// deliberate rewind.
	LoadAt(ctx context.Context, p PipelineKey, id CheckpointID) ([]Entry, error)

	// List enumerates retained checkpoint ids, newest first.
	List(ctx context.Context, p PipelineKey, limit int) ([]CheckpointID, error)

	// Prune removes checkpoints older than keep, never removing the newest.
	Prune(ctx context.Context, p PipelineKey, keep int) error
}

// Entry is one key and its bytes.
type Entry struct {
	Key   string
	Bytes []byte
}

// Fence is a monotonic token. Every write carries one; the store rejects a write
// whose fence is lower than the highest it has seen for that pipeline.
type Fence struct {
	Generation uint64
	Epoch      uint64
}
```

**One snapshot format, always self-contained and relocatable.** There is no aligned/unaligned/native/canonical
matrix. Flink's matrix is an operator tax whose sharp edges (*"aligned checkpoints are not relocatable"*, *"you
cannot upgrade minor versions from an unaligned checkpoint"*) are avoided by never creating it.

### 13.3 Checkpoint edit is a first-class operation

`GET/PATCH/DELETE /pipelines/{id}/checkpoint` exists from the first commit (Connect's KIP-875, which was a
retrofit). It is renderable *because* the header is typed and the splits are structured: an operator sees a table
of splits with their key ranges, their cursors' `Scalar` projections, their watermarks and their completion
checkpoints, and can reset one split without resetting the pipeline. Editing requires the pipeline to be
`Paused` and records an audit entry with the principal.

Resetting one split is the operation Airbyte's `STREAM`-state users want and cannot have: it is
`DELETE /checkpoint/splits/{splitID}`, which drops that split's cursor and returns it to `Unassigned`.

### 13.4 The commit protocol, end to end

```
    [ pipeline admits records ]
              │
   RequestCheckpoint (from a timer, an enumerator boundary, a sink request, or
   an operator) ──► the Checkpointer takes the barrier:
              │
      1. stop admitting new records at the reader hand-off (a select stops
         draining the chan; readers block on Fetch's caller, not on I/O)
      2. drain the transform chain and the buffer to the writer
      3. Writer.Flush(ctx, FlushCheckpoint)
      4. if SupportsCommitter: PrepareCommit(ctx, id) → committables
         → added to Committables[id]
      5. if SupportsWriterState: SnapshotWriter(ctx, id)
      6. Reader.Snapshot(ctx, id) on every reader → []Split (with cursors)
      7. Enumerator.Snapshot(ctx, id) → blob
      8. the prefix tracker's resolved cursors are folded into the splits
      9. CheckpointStore.Commit(ctx, pipeline, fence, entries)   ← ONE TRANSACTION
              │
     ─────── the checkpoint is now durable ───────
              │
     10. if SupportsCommitter: Commit(ctx, requests for every id ≤ this one)
     11. Buffer.Trim(ctx, id)
     12. resume admission
```

Steps 1–2 are the "briefly stop admitting records" that replaces barriers. At single-process and
single-digit-worker scale this is measured in milliseconds and it removes an entire subsystem. The cost is real
and named in §22.5.

**The Checkpoint Subsuming Contract is implemented verbatim** and it is the single most valuable paragraph in
the Flink codebase for canal:

- `CheckpointID`s strictly increase and a higher one **subsumes** all lower ones.
- Step 10 publishes everything in `Committables` with a key **≤** the confirmed id, so a lost confirmation is
  repaired by the next checkpoint.
- An **aborted** checkpoint means *"as if it never triggered; the next successful checkpoint covers a longer
  time span"* — it does **not** mean "discard the artifacts". `Committables` is not cleared on abort. Getting
  this backwards is a data-loss bug that looks like a cleanup.
- A **failed commit escalates**; it is not a log line. This is the direct fix for Benthos's
  `if err = aFn(...); err != nil { …Error… }` and `_ = ackFn(...)`, which silently lose progress *after*
  delivery, and it is R4 in mechanism form.

### 13.5 Where the checkpoint boundary comes from

Not a wall clock, primarily. `offset.flush.interval.ms=60000` decoupled from batches and boundaries is
decision-space trap 3: it re-emits a fully-acked snapshot chunk after a crash because the timer had not fired,
and it produces the ecosystem's most misdiagnosed log line.

canal's boundaries, in priority order:

1. **The enumerator requests one** at a source-meaningful boundary: every chunk completion, and immediately
   before a snapshot→stream handoff. `EnumeratorContext.RequestCheckpoint`.
2. **The reader requests one** at a safe resume point it cares about — a transaction boundary, the end of a page.
3. **The sink requests one** via `WriterContext.RequestCheckpoint`, which fixes the granularity defect of
   source-driven boundaries: if the source offers a cursor every 10,000 records the sink can still ask to commit
   after 500 it has durably written, so granularity is **negotiated** rather than dictated by whichever side
   knows nothing about the other. Connect's `SinkTaskContext.requestCommit()` generalised.
4. **A minimum interval and a maximum interval** as a floor and a ceiling, so a chatty source cannot checkpoint
   per record and a silent one still refreshes `CommittedAt`.

### 13.6 `prefix.Tracker` — the contiguous-prefix resolver, per split, in core

```go
package prefix

// Tracker resolves the committable prefix of a split as records settle
// downstream, tolerating out-of-order settlement and 1→N/N→M reshaping.
//
// This is the ~200-line algorithm that Connect (SubmittedRecords: per-partition
// FIFO deques, out-of-order acks, a watermark over the contiguous acked prefix)
// and Benthos (Jeffail/checkpoint: Capped[T].Track returning a resolver that is
// non-nil only when the committable prefix advances; Uncapped[T] as a
// doubly-linked list splicing resolved nodes and promoting payloads) invented
// independently. It belongs IN canal — Benthos's lives OUTSIDE both its repos, so
// every connector reimplements progress with its own cache key, format, flush
// policy and bugs — and it must be PER SPLIT so the ordering scope is explicit.
//
// Capacity is the in-flight bound: Track BLOCKS when full, which is where
// backpressure comes from and is four lines of sync.Cond.
type Tracker[T any] struct{ /* unexported */ }

func New[T any](capacity int) *Tracker[T]

// Track registers a record's origin sequence with the cursor that becomes
// committable once it and everything before it has settled. n is the number of
// DESCENDANTS the pipeline will produce from this record: 1 for a pass-through, 0
// for a filtered record (which settles immediately), k for a 1→k expansion, and
// k for a fan-out to k sinks.
//
// That single parameter is how the tracker absorbs every topology Benthos's ack
// graph handles for free — fan-out is "settle when all branches settle", filter is
// "settle immediately", rebatching just works — WITHOUT a closure, which is what
// keeps the design wire-shippable (Chain E).
func (t *Tracker[T]) Track(ctx context.Context, seq uint64, cursor opt.Opt[T], n int) error

// Settle marks one descendant of seq as durable. Idempotent.
func (t *Tracker[T]) Settle(seq uint64) 

// Fail marks a descendant as permanently failed. The prefix DOES NOT advance past
// seq; the record is dead-lettered by the caller and the split is marked degraded.
// A pipeline that dead-letters and advances anyway has silently lost data, so this
// is a distinct method rather than a flag on Settle.
func (t *Tracker[T]) Fail(seq uint64, e *fail.Error)

// Resolve returns the highest cursor whose entire prefix has settled, and whether
// it advanced since the last call. Only cursors that were present are returned —
// a batch that offered no cursor (not a safe resume point) contributes a barrier
// the prefix must pass but never a commit target.
func (t *Tracker[T]) Resolve() (T, bool)

// Pending returns outstanding count and bytes, for the metrics.
func (t *Tracker[T]) Pending() (int, int64)

// Stuck returns the oldest unsettled sequence and how long it has been
// outstanding, which is what turns "the checkpoint is not advancing" into "split
// orders/scan/7 sequence 41,022 has been outstanding for 12 minutes".
func (t *Tracker[T]) Stuck() (uint64, time.Duration, bool)
```

`OffsetStorageWriter`'s **double-buffered flush** is copied too: `beginFlush` swaps the resolved cursors aside
under a semaphore, the store write proceeds asynchronously, and a failed flush **merges back** rather than
losing progress accrued during it.

### 13.7 The chunked-snapshot engine — core machinery, gated on declared capabilities

Implemented once, in `engine`, over any source that declares `Chunkable + Replayable + ComparableKeys +
ComparablePositions`. A source with none of those gets a single unchunked scan split and **says so in its
descriptor**, so the operator sees the limitation before running it rather than after.

The eight steps, all core code:

1. **Chunk the key space** via `SplitKeySpace`, requiring ranges unbounded at both ends, persisting the returned
   splitter cursor into `PlanState.Enumerator` so slicing a huge object resumes mid-way.
2. **Per chunk, the offset-signal algorithm.** The reader records `LOW` (`CurrentPosition`), reads the chunk into
   a buffer, records `HIGH`, replays `[LOW, HIGH)` via a bounded `Change` split in the same `Group`, **upserts**
   the replayed records into the buffer, emits the buffer, and reports `HIGH` as the split's `Watermark`.
3. **Report `(SplitID, KeyRange, HIGH)`** as a `Completion`, with `DurableAt` set when the checkpoint containing
   it commits.
4. **Build the handoff split** starting at `min(HIGH)` across all completions — *not* the latest position. That
   is the no-gap guarantee: the earliest-finished chunk's watermark bounds how far back the change read must
   start, and the per-chunk filter suppresses the redundant range for every other chunk.
5. **Filter during the change phase:** emit iff `record.Change.Key ∈ range(chunk) && record.Origin.Pos > HIGH(chunk)`.
   Both comparisons are `bytes.Compare` over `ord.Order`, so the filter is fully generic.
6. **Index the filter from day one.** Completions are held sorted by `Keys.Lo.Order` and looked up by binary
   search. Flink CDC shipped an O(chunks) linear scan per record and retrofitted sorted-ranges-plus-binary-search
   behind a dialect gate; the fix is four lines if written first.
7. **Retire the filter per stream** once the change position passes `max(HIGH)` for that stream, so steady-state
   cost is exactly zero. The filter is transient, not permanent.
8. **Start the change phase only after a complete checkpoint following snapshot completion**, so no change
   record can precede a scan record for the same key. `PlanView.AllCompletionsDurable` is what the enumerator
   gates on, and the core computes it so nobody reimplements it wrongly.

**No locks, no write access to the source, no signalling table.** The watermark is the log's own position
(Flink CDC's verified approach), not an injected marker, because demanding DDL or write access on a source is a
deployment tax many operators will refuse.

### 13.8 Dedupe (R5) — three mechanisms, all derived from the split

R5 demands dedupe state that is **scoped**, **durable**, and **committed after the data it protects**. The split
model makes all three fall out, and it eliminates the storage entirely in the common case.

```go
// DedupeState lives inside the Checkpoint, so it is committed in the same
// transaction as the cursors it derives from — which is R5's "committed after the
// write" satisfied structurally rather than by ordering discipline in a code path.
type DedupeState struct {
	// Keys holds bounded per-split key sets, ONLY for splits declaring Ordered =
	// false. An ordered split needs no set at all.
	Keys map[string]KeySet `json:"keys,omitempty"`
	// Window is the declared retention: how far back a duplicate is recognised.
	// It is a CONFIGURED, DOCUMENTED quantity, not an emergent property of a FIFO's
	// capacity. The abandoned attempt's 50,000-entry process-lifetime FIFO against
	// product docs describing a "platform retention window" matched neither
	// documented semantics.
	Window time.Duration `json:"window"`
}
```

| Split shape | Dedupe mechanism | Storage |
|---|---|---|
| **Ordered** split (`Split.Ordered == true`) | `record.Origin.Pos ≤ committed cursor` ⇒ already durable. A position comparison. | **zero** |
| **Chunked snapshot** during change phase | the key-range + `HIGH` watermark filter, which self-retires | zero at steady state |
| **Unordered** split | a bounded durable key set scoped to `(tenant, pipeline, source, split)` | bounded, in the checkpoint |

Three things this fixes explicitly, each an observed defect:

- **The key carries tenant and source.** The abandoned attempt keyed on the bare event `id`, so two connectors
  or two tenants emitting `1, 2, 3` silently discarded each other's events. Here the key is the map key
  `(tenant, pipeline, source, split)` plus the record key, and there is no way to construct a narrower one
  because the store key is built by the core.
- **"Duplicate" means "already durably stored".** For ordered splits it is *literally* "at or before the
  committed cursor", which is the strongest possible reading. It never means "present in a RAM cache that may
  have evicted it".
- **A duplicate is reported as success, not failure.** `WriteResult.Duplicates` counts it as durable. The
  abandoned attempt recorded the id as seen *before* the append, so any failure in between made the event
  permanently unresubmittable behind a healthy-looking `202`.

The store for the unordered case is the `CheckpointStore`, whose `Commit` is atomic and fenced — explicitly
**not** a cache. Benthos's `Cache.Add` documents that *"it is okay for caches to return nil on duplicates if it
isn't possible to implement"*, which makes the one primitive that could underpin exactly-once optional. The
dedupe store is injected (never module-scope global state), because the abandoned attempt's TypeScript edge had
a module-scope seen-set that leaked across every app instance and forced tests to randomise ids to pass.

### 13.9 Three layers of idempotency

1. **`Origin.Upstream`** — the source system's own id, when it has one. A source with none leaves it empty; a
   source that derives one must derive it **deterministically from stable fields** and document the derivation in
   its descriptor's `Notes`, which the UI renders next to the connector.
2. **`Change.Key`** — the canonical entity key, which is what a sink upserts on.
3. **`(Split, Seq)`** — canal's own durable coordinate, which is what the prefix tracker and the dedupe
   mechanisms use and which exists for every record from every source unconditionally.

`Record.ID()` is the fourth, non-durable layer: the in-flight submit guard, unique within a generation, used for
per-record failure correlation and DLQ targeting.

---

## 14. Error and failure classification

### 14.1 The closed class set

```go
package fail

// Class is a CLOSED classification the connector attaches AT THE POINT OF RAISE.
// Nine values plus two control sentinels. Closed, because it is used as a metric
// label and because every call site must be able to honour every value.
//
// The ownership axis — "is this my config, their system, or a blip?" — is the
// question an operator UI actually needs answered, and only the connector can
// answer it. This is the transplantable part of Airbyte's design, and Vector
// reduces the connector's entire obligation to one Classify(resp, err) function,
// which is the best effort/benefit ratio available and drives BOTH retry policy
// and end-to-end acknowledgement from one place.
type Class uint8

const (
	// ClassTransientUpstream: the remote system is temporarily unable. Back off,
	// retain progress, retry. Honour Retry-After if present.
	ClassTransientUpstream Class = iota + 1
	// ClassTransientInternal: canal itself is temporarily unable — a store
	// contention, a lease renewal race. Back off, retain progress.
	ClassTransientInternal
	// ClassPermanentUpstream: the remote system will not accept this, ever, as
	// configured. A dropped table, a revoked credential, a position that has aged
	// out of retention. STOP, go terminal, do not poison-retry.
	ClassPermanentUpstream
	// ClassPermanentMapping: this RECORD cannot be represented in the destination.
	// A value out of range, a required before-image that is absent, an unmappable
	// type. Dead-letter the record; the pipeline continues.
	ClassPermanentMapping
	// ClassPermanentContract: canal or a connector violated its own contract — a
	// duplicate SplitID, a declared capability with no implementation, an
	// unreadable state version, a plan exceeding MaxSplitsTotal. This is a bug.
	// STOP and surface it as a bug, not as an operational condition.
	ClassPermanentContract
	// ClassDuplicate: already durably stored. A SUCCESS with a distinct signal, and
	// never a failure. This value existing is what stops the R5 catastrophe where a
	// retry of the mandated same id returns "duplicate" and the event is lost
	// behind a healthy-looking 202.
	ClassDuplicate
	// ClassClockSkew: a timestamp outside the configured tolerance (§12.5).
	ClassClockSkew
	// ClassNotConnected: the connection is down. The core runs the connection state
	// machine: reconnect with backoff, then Degraded, then Paused. A dead
	// out-of-process plugin subprocess reports THIS, so process supervision reuses
	// the connection state machine rather than inventing a second one.
	ClassNotConnected
	// ClassEndOfInput: this split, or this whole source, is exhausted. A CONTROL
	// SIGNAL, not a failure. It is what terminates a bounded pipeline gracefully on
	// the same runtime as a streaming one — there is no separate batch mode.
	ClassEndOfInput
)

// Retryable and Terminal are derived from Class, in one place. A connector never
// sets a `retryable bool` and there is no way for a class and a flag to disagree.
func (c Class) Retryable() bool
func (c Class) Terminal() bool

// Stage is where the failure happened. Classifying per stage is Connect's good
// idea (converter / transformation / put-poll / produce) generalised to canal's
// actual stages, and it is what makes canal_records_failed_total{stage,class}
// diagnosable.
type Stage uint8

const (
	StagePlan Stage = iota + 1 // the enumerator
	StageRead                  // the reader
	StageDeframe
	StageDecode
	StageTransform
	StageEncode
	StageFrame
	StageCompress
	StageBuffer
	StageWrite
	StageCommit     // the committer
	StageCheckpoint // the store
	StageSchema     // applying a schema change
)

// Error is canal's error type. It carries the classification, the stage, the
// audience-split messages, and the provenance needed for a DLQ record.
//
// RICH ERRORS DEGRADE TO SIMPLE ONES: Error implements error, so `errors.Is`,
// `errors.As` and `%w` all work, and a consumer that understands nothing but
// error.Error() gets a useful headline. That is Benthos's BatchError headline
// design without its positional-correlation cause.
type Error struct {
	Class Class
	Stage Stage
	// Reason is operator-facing: name the count, the component, an example, the fix
	// and the consequence.
	Reason string
	// Detail is developer-facing: the upstream text, the stack.
	Detail string
	// Split, Record and Attempts are provenance, set by the core where the
	// connector did not.
	Split   opt.Opt[split.SplitID]
	Record  opt.Opt[record.RecordID]
	Attempts int
	// RetryAfter honours an upstream Retry-After header or equivalent.
	RetryAfter time.Duration
	// Cause wraps the underlying error.
	Cause error
}

func (e *Error) Error() string { return e.Reason }
func (e *Error) Unwrap() error { return e.Cause }

// Constructors, one per class, so a connector never constructs an Error literal
// and never forgets the class.
func TransientUpstream(reason string, cause error) *Error
func PermanentMapping(reason string, cause error) *Error
func NotConnected(reason string, cause error) *Error
func EndOfInput() *Error
// ... one per Class

// Sentinels for errors.Is, so a caller can test without switching on Class.
var (
	ErrNotConnected = &Error{Class: ClassNotConnected}
	ErrEndOfInput   = &Error{Class: ClassEndOfInput}
)
```

**Every call site honours every class.** This is stated as a rule because Benthos's `ErrBackOff` is honoured
*only* on `Connect` — a connector can return a hint the framework silently ignores everywhere else, which is
worse than not having the hint. canal's conformance suite asserts, for each of the eleven classes, that returning
it from each of `Poll`, `Report`, `Fetch`, `Assign`, `Write`, `Flush`, `Commit` and `ApplySchemaChange` produces
the documented behaviour.

### 14.2 Dead-lettering, for sources as well as sinks

```go
// DeadLetter is the record the core writes when a record is permanently
// unroutable. It works for SOURCE-side failures too, which Connect's
// ErrantRecordReporter does not (it is sink-only), and source-side decode
// failures are extremely common.
type DeadLetter struct {
	Ref       record.Ref  `json:"ref"`
	Stage     Stage       `json:"stage"`
	Class     Class       `json:"class"`
	Reason    string      `json:"reason"`
	Detail    string      `json:"detail"`
	Attempts  int         `json:"attempts"`
	Connector string      `json:"connector"`
	Split     split.SplitID `json:"split"`
	Position  opt.Opt[ord.Position] `json:"position,omitempty"`
	At        time.Time   `json:"at"`
	// Payload is the record as canal last saw it, encoded with the DLQ's own codec.
	// Secrets are stripped structurally, not by a redaction pass.
	Payload []byte `json:"payload"`
}

// DeadLetterSink is an ORDINARY REGISTERED SINK, configured on the pipeline. There
// is no special DLQ subsystem: it is a sink, so it gets batching, retry, metrics
// and its own status for free, and `canal run` defaults it to a JSONL file.
```

### 14.3 Strict mode is the default

A record carrying an error is **rejected at the sink**, not written. Benthos's `error_handling.strict` defaults
to `false` with a note that it *"is expected to become the default in the next major version"*; for a
data-movement tool the strict behaviour is correct and canal starts there. A pipeline that wants
best-effort-write-anyway configures it explicitly and the UI labels it.

### 14.4 Sustained backoff is a visible state, and the metric is time

Sustained retrying sets `Condition{Type: Degraded, Status: True, Reason, Message}` carrying the last error's
operator-facing text, and escalates to `Phase: Paused` after a configured duration. The alertable metric is
**`canal_backoff_seconds_total{pipeline,stage,class}`** — cumulative *time spent waiting*, not a retry count. A
retry count says retries happened; retry seconds says the pipeline spends 80% of its life backing off.

---

## 15. Topology: fixed outer shape, unlimited variety by composition

### 15.1 The shape

```
source → [deframe] → [decode] → [transform chain] → [buffer] → [encode] → [frame] → [compress] → sink
```

The shape is **fixed and small, and it is an implementation fact, not a contract**. It is *not* enumerated in the
API schema as a stage list with a count. R1's whole content is that the abandoned attempt froze eight stages into
its OpenAPI as `minItems: 8, maxItems: 8` with `ordinal` constrained to `1..8`, making a new stage a breaking
contract change — and modelled buffers twice on top of that.

The control API instead describes **whatever stages exist**, as a list, generated from the composed pipeline:

```go
// StageInfo is what the API reports per stage. Discovered from the composed
// pipeline, never enumerated in a schema. Adding a stage type is not a contract
// change.
type StageInfo struct {
	Path      string      `json:"path"`      // "sink.retry.child.broker[1]" — the observability path
	Kind      string      `json:"kind"`      // "source" | "decode" | "transform" | "buffer" | "sink" | ...
	Component string      `json:"component"` // registered name
	Children  []StageInfo `json:"children,omitempty"`
	Metrics   StageMetrics `json:"metrics"`
}
```

### 15.2 Variety comes from components containing components

All topology variety comes from **component-valued config fields** (`cfg.Field.Component`, §11.1), resolved from
config, with observability nesting automatically by path. This is Benthos's design and it means `broker`,
`switch`, `fallback`, `retry` and `reject_errored` exist with **zero core special-casing** — and it costs nothing
extra because it is the same mechanism as composite config fields.

```go
// Meta is the SANCTIONED declaration that a component is a stage rather than a
// leaf. Benthos punches this hole with an UNDOCUMENTED Unwrap() type assertion
// plus five XUnwrapper() any back doors; declaring it is one method and it lets
// the core attach nested metrics, propagate Snapshot(id) into children, and
// render the topology tree.
type Meta interface {
	// Children returns the components this one contains, with the path segment each
	// occupies. The core walks this for metrics nesting, checkpoint propagation and
	// the StageInfo tree.
	Children() []Child
}

type Child struct {
	Segment   string
	Component any // a Writer, Transform, Buffer, ...
}
```

### 15.3 Transforms are core, with the full return vocabulary

```go
// Transform is a core stage. Its return type expresses 1→1, 1→N, 1→0 and N→M with
// NO EXTRA VOCABULARY:
//
//	[]Batch{b}              pass through (or modified)
//	[]Batch{}               filter everything
//	[]Batch{b1, b2}         regroup / window
//	one record → k records  expand (via Record.DeriveN, which preserves Origin)
//
// Connect's Transformation<R> is strictly 1→1 (or 1→0 by returning null),
// synchronous, stateless, on the task thread, with conditional application bolted
// on later by KIP-585 and even then one predicate per transform with no boolean
// combinators. That limitation is the reason its transform ecosystem is stunted.
type Transform interface {
	Process(ctx context.Context, b record.Batch) ([]record.Batch, error)
	Close(ctx context.Context) error
}

// StatefulTransform is the optional upgrade for a transform that accumulates. Its
// state joins the same checkpoint record, which is exactly the property that makes
// "position and derived state are consistent" true — the R5 failure shape in
// general form.
type StatefulTransform interface {
	Transform
	Snapshot(ctx context.Context, id CheckpointID) (blob.VersionedBlob, error)
	Restore(ctx context.Context, state blob.VersionedBlob) error
}
```

**A transform cannot touch provenance.** `Record.id` and `Record.origin` are unexported and the only way to emit
a derived record is `Derive`/`DeriveN`, which forward them. Connect gets this structurally too, and its
`SinkRecord.originalTopic/originalKafkaPartition/originalKafkaOffset` retrofit (KIP-793) is what happens when
identity and payload are conflated in a system that *doesn't*.

### 15.4 Ordering, stated at the knob

`transforms.concurrency` — the field that destroys ordering — carries its own warning **in the spec**, rendered
by the UI:

> Ordering within a split is preserved only at concurrency 1. Above 1, records from one split may be reordered.
> Sinks configured for `upsert` against a source without a monotonic version field will produce wrong results.

Benthos's `threads: -1 → runtime.NumCPU()` silently destroys ordering and does not say so at the config field.
canal states the ordering scope where the operator is making the choice.

### 15.5 Fan-out semantics, named rather than discovered

Fan-out is a `broker` sink with `mode ∈ {all, all_fail_fast, all_sequential, round_robin}`. Under
at-least-once, `all` means: if sink B fails, the record is not settled and is **re-sent to A as well**, so
duplicates in A are the price of not losing it in B. `all_fail_fast` is the escape hatch. The trade is documented
at the config field and appears in the UI, because fan-out is in canal's end-state goals and duplicate-vs-loss
must be a **named choice**, not a discovery.

The prefix tracker handles all of it with `Track(seq, cursor, n=branches)`; there is no fan-out code path in the
checkpointer.

---

## 16. The standalone ↔ distributed seam

### 16.1 Four interfaces, two assemblies

```go
// Runtime is the whole of what differs between `canal run` and `canal serve`.
// If a FIFTH interface appears here, the abstraction is wrong.
type Runtime struct {
	Config      store.ConfigStore      // revisioned CAS + Watch
	Checkpoints store.CheckpointStore  // bytes in / bytes out; Commit atomic across the map
	Coordinator store.Coordinator      // membership, planner election, assignment leases
	Status      store.StatusStore      // per-worker status → one PipelineStatus
}
```

| Interface | `canal run` (standalone) | `canal serve` (coordinated) |
|---|---|---|
| `ConfigStore` | bbolt file, or a YAML file projected in read-only | Postgres (default), etcd, k8s CRD |
| `CheckpointStore` | bbolt file — one `Commit` = one txn = atomic | Postgres table, etcd `Txn`, object store + manifest |
| `Coordinator` | `singleNode{}` — always planner, all assignments local, leases are no-ops that still carry a fence | Postgres advisory lock + a leases table, or etcd election, or k8s `Lease` |
| `StatusStore` | direct in-process read | workers write status rows with a TTL column |

```go
package store

// Coordinator provides membership, planner election and assignment leases. It
// provides NOTHING the data path needs while a lease is valid — see §16.3.
type Coordinator interface {
	// Join registers this process and returns its identity. Epoch increments on
	// every Join by the same WorkerID, so (WorkerID, Epoch) identifies an
	// incarnation from the first commit.
	Join(ctx context.Context, w WorkerID) (Membership, error)

	// Members lists live members, for the placer and the UI.
	Members(ctx context.Context) ([]Membership, error)

	// CampaignPlanner attempts to become the pipeline's planner. It returns a lease
	// that must be renewed. LEADERSHIP IS NEVER TRUSTED FOR CORRECTNESS: the
	// returned Fence is carried on every checkpoint write and the store rejects a
	// stale one. The verified k8s caveat is decisive — that implementation "does not
	// guarantee that only one client is acting as a leader (a.k.a. fencing)" and
	// clients infer leadership from locally captured timestamps.
	CampaignPlanner(ctx context.Context, p PipelineKey) (Lease, error)

	// Claim atomically claims assignments for this worker, taking at most n. In
	// Postgres this is SELECT ... FOR UPDATE SKIP LOCKED, so WORK CLAIMING NEEDS NO
	// LEADER — the placer writes rows and workers claim them.
	Claim(ctx context.Context, p PipelineKey, w WorkerID, n int) ([]Assignment, error)

	// Place writes assignment rows. Called only by the planner.
	Place(ctx context.Context, p PipelineKey, fence Fence, a []Assignment) error

	// Renew extends a lease. Losing a renewal is how a worker learns it has been
	// fenced, and the worker then stops reading, drops its splits and rejoins.
	Renew(ctx context.Context, l Lease) (Lease, error)

	// Release relinquishes cleanly.
	Release(ctx context.Context, l Lease) error
}

// Lease carries a fence. Nothing else in canal is a fencing token.
type Lease struct {
	Key       string
	Holder    WorkerID
	Fence     Fence
	ExpiresAt time.Time
}
```

**Postgres first, etcd as a conformance target.** One dependency delivers every primitive: revisioned CAS
(`UPDATE … WHERE revision = $2`), atomic multi-key writes (one transaction), planner election
(`pg_try_advisory_lock`, released on session loss), leases (a table plus a conditional update), leaderless work
claiming (`FOR UPDATE SKIP LOCKED`), `Watch` (`LISTEN`/`NOTIFY` with a `max(revision)` poll fallback), and status
aggregation (a table with a TTL column). etcd is a *better semantic* fit — `ModRevision` CAS and
`Watch(fromRevision)` are literally the interface — so it stays as the conformance target that stops the
interface acquiring Postgres-isms, not as a shipped dependency.

**Kafka as the coordination store is explicitly rejected.** Reproducing `KafkaConfigBackingStore`'s documented
unrecoverable compaction-plus-partial-write state, which needs seven record types and a `commit-{id}` marker to
fake set-atomicity, when a transactional database is available, would be perverse.

### 16.2 The transport is not an interface — it is an implementation of `Reader`

This is the payoff of the angle for D14 and it is worth stating alone.

There is **no** `Transport` interface, no `WorkerClient`, no `EnumeratorProxy` abstraction. `engine/remote`
contains:

```go
// remoteReader satisfies connector.Reader by forwarding to a worker process. The
// engine cannot tell it apart from an in-process reader, because Reader's methods
// are all (ctx, serialisable) → (serialisable, error) and its entire durable state
// is a []Split that already has a wire form.
type remoteReader struct{ conn *grpc.ClientConn; worker WorkerID }

func (r *remoteReader) Assign(ctx context.Context, s []split.Split) error
func (r *remoteReader) Revoke(ctx context.Context, ids []SplitID) ([]split.Split, error)
func (r *remoteReader) Fetch(ctx context.Context) (connector.Fetch, error)
func (r *remoteReader) Snapshot(ctx context.Context, id CheckpointID) ([]split.Split, error)
func (r *remoteReader) Close(ctx context.Context) error

// remoteEnumerator and remoteWriter, symmetrically.
// A dead subprocess or a broken connection is reported as fail.ErrNotConnected, so
// PROCESS SUPERVISION REUSES THE CONNECTION STATE MACHINE and needs no new states.
```

The same three implementations serve **both** of constraint #3's futures: an out-of-process *connector* (a
subprocess speaking gRPC, satisfying `Reader`) and an out-of-process *worker* (a pod in a cluster, satisfying
`Reader`). They are the same problem, and the split model is why: a reader's interface is small, its state is a
list of values, and its hot path is one method returning a value.

There is a proto file, `engine/remote/canal/v1/*.proto`, and the discipline is that **it could be generated from
the Go interfaces**. That is the property Connect fails on five counts (`Class<? extends Task> taskClass()`,
`Map<String,?>` payloads, behavioural `Schema`/`Struct`, `Future<Void>`/`Callback<Void>`, no `Context`) and which
is why no out-of-process Connect exists and cannot.

### 16.3 The leader only plans

The planner writes assignment rows. It does **not** route data, proxy status, hold a checkpoint, or own anything
the data path reads. Therefore:

> **The data plane keeps running and keeps checkpointing with the entire control plane down**, because a worker
> holding a valid lease needs nothing from anyone until it expires.

This is the single most important deployment property in the design and it is worth sacrificing elegance for.
Concretely, with Postgres unreachable a worker: keeps `Fetch`ing its assigned splits, keeps writing to its sink,
keeps resolving the prefix — and cannot checkpoint. `canal_checkpoint_age_seconds` climbs, `Degraded` goes true
with reason `checkpoint_store_unreachable`, and when the store returns the next checkpoint covers the longer span
(the subsuming contract, again). Nothing is lost; progress is merely not durable meanwhile, which is the honest
behaviour.

### 16.4 Assignment, lease timing, and delayed reassignment

- Lease TTL **30s**, renewed at 10s.
- Reassignment of orphaned splits is **delayed 120s** with exponential backoff on consecutive revoking rounds, so
  a bouncing worker reclaims its own assignments instead of triggering a reshuffle. This is KIP-415's
  `scheduledRebalance`/`delay` — introduced *after* documented rebalance storms that *"could take several
  minutes to stabilize"*, whose incremental-cooperative replacement then shipped its own imbalance bug
  (KAFKA-12495). Both are configurable.
- Placement is **pull-based**: `ReaderContext.RequestSplits(n)` raises `PlanView.Demand`, and the placer places
  against demand. No load model, and structurally no rebalance storm to have.
- `Split.Group` co-location and `SourceCapabilities.MaxSplitsPerReader` are hard constraints on the placer.
- **Per-phase parallelism changes with no restart.** The enumerator emits 20 `Scan` splits and later 1 `Change`
  split; the placer spreads the 20 and places the 1; readers that end up with nothing are told `NoMoreSplits`
  and idle or exit. Snapshot at 20, stream at 1, no restart. This is the requirement Kafka Connect structurally
  cannot meet — its parallelism is fixed by `taskConfigs(maxTasks)` at connector start and re-splitting requires
  `requestTaskReconfiguration()`, which restarts tasks — and it is the main reason one-shot planning is rejected.
- **Big plans are paged, from day one.** `PlanView.Completions` and the control API's split list are paged by
  reference. Flink CDC discovered its finished-chunk set was too large to ship in one message and retrofitted
  four new event types, a `totalFinishedSplitSize` field and a truncation path.

### 16.5 Tenancy and authentication at every ingress

Decided before the first multi-tenant field, per R13.

```go
// PipelineRef carries the tenant. There is no pipeline identity WITHOUT a tenant,
// so a store key cannot be constructed without one and a "forgot to scope the
// query" bug is a compile error rather than a data leak.
type PipelineRef struct {
	Tenant TenantID `json:"tenant"`
	ID     string   `json:"id"`
}

func (p PipelineRef) StoreKey() string // "t/<tenant>/p/<id>", built here and nowhere else

// Principal is what every ingress resolves before doing anything. The HTTP API,
// the gRPC worker API and the CLI all go through the same resolution.
type Principal struct {
	Tenant  TenantID `json:"tenant"`
	Subject string   `json:"subject"`
	Scopes  []Scope  `json:"scopes"`
}

type Scope uint8

const (
	ScopeReadPipelines Scope = iota + 1
	ScopeWritePipelines
	ScopeReadCheckpoints
	ScopeEditCheckpoints // separate: editing a checkpoint can lose data
	ScopeReadRecords     // the live tap sees payloads; distinct from reading metrics
	ScopeAdmin
)
```

- **Single-tenant is `Tenant: "default"`**, not "absent". `canal run` uses it and there is no unscoped code path
  that a multi-tenant deployment later has to find and fix. The abandoned attempt realised "tenancy" as "single
  OS user" and had no authentication, authorization or tenant scoping anywhere.
- **Ingresses:** the control HTTP API (bearer token → `Principal`; mTLS also supported), the worker gRPC API
  (mTLS, and the worker's own identity is a `Principal` with a fixed scope set), and the metrics endpoint (which
  serves **no** per-record data and therefore has a separate, weaker auth requirement — a deliberate split, since
  requiring admin auth for Prometheus scraping is what makes people disable auth).
- **`ScopeReadRecords` is separate from `ScopeReadPipelines`** because the live tap shows payloads. An operator
  who may restart a pipeline is not automatically permitted to read the data flowing through it.

---

## 17. Observability, status, and the read model

### 17.1 The connector never names a metric

```go
// Metrics is the registrar handed to connectors through their context. A connector
// registers a metric by SEMANTIC ROLE from a closed enum; the core owns the name,
// the unit, the tag set and the export.
//
// This is Connect's PluginMetrics (auto-tagged connector/task/converter) and
// Flink's typed SourceReaderMetricGroup/SinkWriterMetricGroup. Letting a connector
// name a metric produces label-cardinality explosions and two connectors naming
// the same quantity differently, which is R9 in the metrics namespace.
type Metrics interface {
	Counter(r Role) Counter
	Gauge(r Role) Gauge
	Histogram(r Role) Histogram
	// Custom is the escape hatch, and it is DELIBERATELY AWKWARD: the name is
	// prefixed with the connector's registered name, the label set is fixed to the
	// pipeline topology labels, and a connector using it appears in the UI's
	// "non-standard metrics" list. Awkward, available, visible.
	Custom(name string, unit Unit) Counter
}

// Role is the closed vocabulary. A connector picks a role; the core knows what to
// call it.
type Role uint8

const (
	RoleUpstreamRequests Role = iota + 1
	RoleUpstreamErrors
	RoleUpstreamLatency
	RoleUpstreamBytes
	RoleUpstreamThrottled
	RoleConnectAttempts
	RoleRecordsRead
	RoleRecordsWritten
	RoleSplitsPlanned
	RoleSplitsFinished
	// ... small, closed, and extended only in core
)
```

**The metric label vocabulary is hard-closed to pipeline topology** — `{tenant, pipeline, generation, stage,
component, class}` — and enforced at registration. Unbounded per-stream and per-split detail is served from the
**read model**, not from labels. That split is what stops a 200,000-chunk snapshot from producing 200,000 metric
series.

### 17.2 Never a metric called `lag`

Four separately-named quantities, each documented as an explicit subtraction, per FLIP-33's discipline, and
**each omitted entirely when unmeasurable — never emitted as zero**:

| Series | Definition | Available when |
|---|---|---|
| `canal_checkpoint_age_seconds` | `now − Header.CommittedAt` | **always** |
| `canal_event_time_lag_seconds{stage}` | `now − Change.CommitTime` at that stage | the source populates `CommitTime` |
| `canal_backlog_records` / `canal_backlog_bytes` | from `Backlogger`, with `exact` as a label and `as_of` as a companion gauge | the source implements `Backlogger` |
| `canal_position_fraction{split}` | `ord.Fraction(from, cursor, to)` | positions carry `Scalar` |

`canal_checkpoint_age_seconds` is the **primary health metric**: always available, unfakeable, and one alert
catches every stall mode — a wedged reader, a failing sink, an unreachable store, a lost lease. It is the metric
canal's own runbook is written around.

Recovery is instrumented **in phases** — state load, restore, connector open, first record — because if
restart-and-resume is a headline feature then restart time is a metric, not an assumption.

### 17.3 Status: `Phase` + `Conditions[]`

```go
// PipelineStatus is the k8s shape, because one enum provably cannot describe
// "running, healthy connection, 40 minutes behind, sink returning 429s".
type PipelineStatus struct {
	Phase RunPhase `json:"phase"`
	// ObservedGeneration answers "did my config change take effect?", which
	// Connect's status API structurally cannot.
	ObservedGeneration uint64      `json:"observed_generation"`
	Conditions         []Condition `json:"conditions"`
	// StoppingSince and DrainDeadline are set in RunStopping. DRAINED and
	// DRAIN-TIMEOUT are DISTINCT events, because the second means records may
	// replay and an operator must be able to tell.
	StoppingSince opt.Opt[time.Time] `json:"stopping_since,omitempty"`
	DrainDeadline  opt.Opt[time.Time] `json:"drain_deadline,omitempty"`
}

type RunPhase uint8

const (
	RunPending RunPhase = iota + 1
	RunStarting
	RunRunning
	RunPaused
	RunStopping
	RunStopped
	RunFailed
	// RunCompleted: a bounded pipeline finished. Kafka Connect lacks this entirely,
	// so every batch pipeline there looks like a streaming one that stopped
	// producing.
	RunCompleted
)

type Condition struct {
	Type               ConditionType `json:"type"`
	Status             TriState      `json:"status"` // True | False | Unknown
	Reason             string        `json:"reason"` // closed enum of machine-readable reasons
	Message            string        `json:"message"`
	LastTransitionTime time.Time     `json:"last_transition_time"`
	ObservedGeneration uint64        `json:"observed_generation"`
}

type ConditionType uint8

const (
	CondConfigured ConditionType = iota + 1
	CondConnected
	CondAssigned
	CondProgressing
	CondCaughtUp
	CondDegraded
)
```

**`Connected: True` must never be able to imply `Progressing: True`.** That separation is the machine-readable
form of the honesty rule, and it is asserted by a fixture test: a pipeline with a healthy connection and a stalled
commit **must** render as unhealthy. The abandoned attempt's UI spec got this right in prose — required order:
what works, what does not, any pilot qualifier, the next action; and forbade implying a latency or availability
commitment from a probe returning 200 — and enforced it with machine-readable attributes on every badge so the
copy could be asserted in tests. That mechanism is kept: every condition renders with its `Type`, `Status` and
`Reason` as data attributes, and there are pinned fixture tests over the rendered output.

`Phase` (the *plan's* phase: discovering/scanning/catching-up/streaming/completed) and `RunPhase` (the pipeline's
operational phase) are **different concepts with different names**, deliberately: a pipeline can be
`RunRunning` + `PhaseScanning` + `Degraded`, and collapsing those into one enum is exactly what makes a status API
useless. Phase is not health.

### 17.4 The read model — what the frontend actually consumes

```
GET  /connectors                          → []Descriptor            (from the registry, no instantiation)
GET  /connectors/{kind}/{name}            → Descriptor              (config spec + capabilities + JSON Schema)
POST /connectors/{kind}/{name}/validate   → []cfg.Diagnostic        (two-tier, all errors at once)
POST /connectors/{kind}/{name}/choices    → []cfg.Choice            (a named DynamicChoices hook)
POST /connectors/{kind}/{name}/test       → []TestResult             (named connection tests)
POST /sources/{name}/discover             → schema.Catalog
GET  /pipelines/{id}                      → PipelineSpec + PipelineStatus
GET  /pipelines/{id}/plan                 → PlanSummary              (progress; see below)
GET  /pipelines/{id}/plan/splits?page=    → []SplitSummary           (paged; the per-split table)
GET  /pipelines/{id}/stages               → []StageInfo              (the topology tree, discovered)
GET  /pipelines/{id}/checkpoint           → CheckpointSummary
PATCH/DELETE /pipelines/{id}/checkpoint   → operator edit (requires Paused + ScopeEditCheckpoints)
GET  /pipelines/{id}/catalog/drift        → schema.CatalogDiff       (drift as a diff)
GET  /pipelines/{id}/dlq?page=            → []DeadLetter
GET  /pipelines/{id}/tap?stage=&n=        → sampled records          (requires ScopeReadRecords)
GET  /metrics                             → Prometheus text
GET  /ready                               → status code + JSON status tree
```

```go
// PlanSummary is the progress document, and it is COMPUTED ENTIRELY FROM THE PLAN
// with zero connector-specific code. This is the concrete answer to the frontend
// goal.
type PlanSummary struct {
	Phase split.Phase `json:"phase"`

	Splits struct {
		Total      int `json:"total"`
		Unassigned int `json:"unassigned"`
		Assigned   int `json:"assigned"`
		Finished   int `json:"finished"`
		// Expected is the denominator when the enumerator knows it. ABSENT rather
		// than zero when it does not, and the UI shows an indeterminate bar.
		Expected opt.Opt[int] `json:"expected,omitempty"`
	} `json:"splits"`

	// Fraction is the overall progress in [0,1], computed as finished-split count
	// plus each in-progress split's own position fraction, divided by Expected.
	// ABSENT when Expected is absent or no split reports a fraction.
	Fraction opt.Opt[float64] `json:"fraction,omitempty"`

	// ETA is absent unless Fraction is present and the rate has been stable.
	ETA opt.Opt[time.Duration] `json:"eta,omitempty"`

	Streams []StreamProgress `json:"streams"`
	Workers []WorkerProgress `json:"workers"`
}

type SplitSummary struct {
	ID       split.SplitID  `json:"id"`
	Kind     string         `json:"kind"`
	Stream   string         `json:"stream"`
	State    string         `json:"state"` // unassigned | assigned | reading | finished | paused | failed
	Worker   opt.Opt[string] `json:"worker,omitempty"`
	KeyRange string         `json:"key_range,omitempty"` // rendered from Order bytes as hex or the connector's display hint
	Fraction opt.Opt[float64] `json:"fraction,omitempty"`
	Records  int64          `json:"records"`
	Since    time.Time      `json:"since"`
	LastError opt.Opt[string] `json:"last_error,omitempty"`
}
```

**Deterministic read-model fixtures are pinned in tests** so the shape the UI consumes cannot drift silently, and
**tests assert real responses against the schema**, not the schema against itself. The abandoned attempt's
canonical bug — a nil slice marshalling as `null` against a `required: array` field, with the test asserting
`len(...) != 0` which passes for `null` — is prevented by two structural choices: every list field in the read
model is initialised non-nil by a constructor, and the conformance test round-trips real handler output through
the published JSON Schema.

**Shared constants are generated from one source.** The `Class`, `Op`, `Phase`, `RunPhase`, `ConditionType` and
`Disposition` enums have exactly one Go definition each, and the TypeScript enums plus the i18n key namespace are
**generated** from them by `go generate`. The abandoned attempt hard-coded its contract version in five places
with nothing checking they agreed, and grew four string vocabularies for three tiers.

### 17.5 The live tap

`GET /pipelines/{id}/tap?stage=sink.retry.child&n=20` returns a sample of records at any stage, rendered through
the same secret-stripping as the DLQ. Any edge is tappable because every edge is a bounded channel the engine
owns, and the sampler attaches without changing the topology. A single binary can therefore serve its own
dashboard with no Prometheus, which matters because `canal run` on a laptop is the adoption lever.

---

## 18. Making the simple case simple

This section exists because it is where the angle is most vulnerable, and the answer has to be shown rather than
asserted.

### 18.1 The trivial source, in full

An HTTP-poll source with a cursor. **This is the whole connector.**

```go
package httppoll

import "github.com/BernardoCSACarreira/canal"

func init() {
	canal.RegisterPollSource(canal.PollSourceSpec{
		Name:    "http_poll",
		Version: "1.0.0",
		Title:   "HTTP poll",
		Summary: "Polls a JSON endpoint and emits each array element as a record.",
		Config: &cfg.Spec{Fields: []cfg.Field{
			{Path: cfg.P("url"), Kind: cfg.KindString, Title: "URL"},
			{Path: cfg.P("token"), Kind: cfg.KindString, Title: "Bearer token", Secret: true, Optional: true},
			{Path: cfg.P("cursor_field"), Kind: cfg.KindString, Title: "Cursor field",
				Default: opt.Some[any]("updated_at")},
			cfg.RetryField("retry", cfg.DefaultRetry),
			cfg.BatchingField("batching", cfg.BatchPolicy{Count: 500, Period: time.Second}),
		}},
		Open: func(ctx context.Context, c cfg.Config, sc canal.SourceContext) (canal.PollSource, error) {
			return &poller{
				url:    cfg.Field[string](c, "url"),
				field:  cfg.Field[string](c, "cursor_field"),
				client: sc.HTTPClient(),
			}, c.Err()
		},
	})
}

type poller struct {
	url, field string
	client     *http.Client
}

// Poll is the ENTIRE data path. `from` is whatever the previous Poll returned,
// handed back after a restart, from durable storage.
func (p *poller) Poll(ctx context.Context, from opt.Opt[ord.Position]) (canal.Poll, error) {
	since := ""
	if pos, ok := from.Get(); ok {
		since = string(pos.Bytes)
	}
	rows, next, err := p.fetch(ctx, since) // ordinary HTTP + JSON
	if err != nil {
		return canal.Poll{}, fail.TransientUpstream("polling "+p.url, err)
	}
	recs := make([]record.Record, 0, len(rows))
	for _, row := range rows {
		recs = append(recs, record.FromStructured(row))
	}
	return canal.Poll{
		Records: recs,
		// Cursor is an opaque string; canal.PollSource lifts it into an
		// ord.Position with Order == Bytes, so it is comparable for free when the
		// field is lexicographically ordered.
		Cursor: opt.Some(next),
		Idle:   len(rows) == 0,
	}, nil
}
```

No `Discover`, no `Enumerator`, no `Reader`, no `Split`, no `Snapshot`, no serialiser, no `Assign`, no
`Revoke`. **Thirty-five lines of connector, of which twelve are the config spec.**

### 18.2 What `canal.RegisterPollSource` does

It is a thin, non-magical adapter — roughly forty lines of core code — and printing it is the proof that the sugar
is sugar:

```go
// PollSource is the reduced interface for a source with no work to plan.
type PollSource interface {
	Poll(ctx context.Context, from opt.Opt[ord.Position]) (Poll, error)
}

type Poll struct {
	Records []record.Record
	Cursor  opt.Opt[string] // absent = not a safe resume point
	Idle    bool
	// Done, when set, ends the split — which is how a bounded poll source
	// (paginate to the end, then stop) says so, and it is the one extra field a
	// batch-shaped poller needs.
	Done bool
	RetryAfter time.Duration
}

// RegisterPollSource wraps a PollSource into a full Source:
//
//   Discover        → schema.OneStream(spec.Name, schema.Schema{}) — one stream, no fields
//   NewEnumerator   → oneSplit{}, whose first Poll returns exactly one split
//                     (Kind: Change, whole KeyRange, PositionRange{From: absent, To: absent},
//                      Ordered: true) and then PlanIdle forever, or PlanComplete
//                      once the split reports Done
//   RestoreEnumerator → the same, with the split's absence/presence read from the plan
//   NewReader       → pollReader{}, which holds at most one split, calls Poll on
//                     Fetch, wraps the returned cursor as ord.Position{Bytes: s, Order: []byte(s)},
//                     and returns []Split{itsSplit} from Snapshot
//   Capabilities    → {Chunkable: false, Replayable: false, ComparablePositions: true,
//                      MidSplitResume: true, MaxSplitsPerReader: 1, MaxSplitsTotal: 1}
func RegisterPollSource(s PollSourceSpec)
```

`oneSplit` is the default enumerator, and it is genuinely nine lines:

```go
type oneSplit struct{ emitted bool; done bool }

func (o *oneSplit) Poll(ctx context.Context, v connector.PlanView) (connector.Enumeration, error) {
	switch {
	case o.done:
		return connector.Enumeration{Status: connector.PlanComplete}, nil
	case !o.emitted:
		o.emitted = true
		return connector.Enumeration{
			Status: connector.PlanMore,
			Splits: []split.Split{{
				ID:      split.SplitID{Stream: o.stream, Kind: split.Change, Seq: 0},
				Ordered: true,
			}},
			Expect: opt.Some(0), // no bounded splits expected
		}, nil
	default:
		return connector.Enumeration{Status: connector.PlanIdle, RetryAfter: 30 * time.Second}, nil
	}
}
```

### 18.3 What the trivial source gets for free, and why that is the argument

Because it is a *real* split-model source rather than a special case:

- a **durable, resumable cursor** it never wrote persistence code for;
- **`canal_checkpoint_age_seconds`**, `records_read`, `backoff_seconds_total`, per-stage failure counters;
- a **progress document** and a status card in the UI;
- **batching and retry from configuration**, identical to every other connector;
- **backpressure**, in-flight bounds, a bounded buffer, a DLQ;
- **the same checkpoint file format** as a chunked CDC source, so `canal offsets` and the operator-edit API work
  on it;
- **standalone and coordinated deployment** with the identical connector binary, since the placer places one
  split as happily as 200,000.

And the upgrade path is the point: when someone later wants that endpoint polled by tenant in parallel, they
replace `oneSplit` with a real enumerator that emits one `Change` split per tenant. **Zero core edits, zero
checkpoint-format changes, zero switch statements, and the existing checkpoint remains readable** because the
split with `Seq: 0` is still valid and the new ones are simply added. That is constraint #4 satisfied not just for
"add a connector" but for "make an existing connector scale".

### 18.4 The trivial sink, in full

```go
func init() {
	canal.RegisterSink[*fileWriter](canal.SinkSpec[*fileWriter]{
		Name:    "jsonl_file",
		Version: "1.0.0",
		Title:   "JSONL file",
		Config: &cfg.Spec{Fields: []cfg.Field{
			{Path: cfg.P("path"), Kind: cfg.KindString, Title: "Path"},
			cfg.BatchingField("batching", cfg.BatchPolicy{Count: 1000, Period: time.Second}),
		}},
		Capabilities: connector.SinkCapabilities{
			APIVersion: 1,
			Tier:       connector.TierAtLeastOnce,
			Modes:      []schema.DestinationMode{schema.DestAppend},
			Idempotent: false,
		},
		Open: func(ctx context.Context, c cfg.Config, sc canal.SinkContext) (connector.Sink, error) {
			return &fileSink{path: cfg.Field[string](c, "path")}, c.Err()
		},
	})
}

type fileWriter struct{ f *os.File; w *bufio.Writer }

func (w *fileWriter) Open(ctx context.Context, o connector.Opening) error { return nil }

func (w *fileWriter) Write(ctx context.Context, b record.Batch) (connector.WriteResult, error) {
	var res connector.WriteResult
	for _, r := range b {
		p, err := r.Payload.AsBytes()
		if err != nil {
			// Per-record failure, classified at the point of raise. R7: the failure
			// shape exists because it was written with the success shape.
			res.Failed = append(res.Failed, connector.RecordFailure{
				Record: r.ID(), Class: fail.ClassPermanentMapping,
				Reason: "record could not be encoded for jsonl_file", Detail: err.Error(),
			})
			continue
		}
		if _, err := w.w.Write(append(p, '\n')); err != nil {
			return res, fail.TransientUpstream("writing "+w.f.Name(), err)
		}
		res.Written++
		res.Bytes += int64(len(p)) + 1
	}
	return res, nil
}

// Flush is where at-least-once is EARNED: returning nil from Write is the
// acknowledgement, so Write must not claim durability that Flush provides. This
// writer buffers, so it flushes AND fsyncs here, and the core does not advance the
// checkpoint until Flush returns.
func (w *fileWriter) Flush(ctx context.Context, _ connector.FlushReason) error {
	if err := w.w.Flush(); err != nil {
		return fail.TransientUpstream("flushing", err)
	}
	return w.f.Sync()
}

func (w *fileWriter) Close(ctx context.Context) error { return w.f.Close() }
```

Four methods, one of which is `return nil`. Note the honesty structurally enforced: because `Flush` is what makes
data durable and the core does not commit until `Flush` returns, this sink **cannot** commit a checkpoint against
unsynced bytes. The abandoned attempt's `202`-after-append-to-an-in-memory-slice is not expressible.

---

## 19. Walkthroughs

### 19.1 (a) One record, end to end

`canal run --source http_poll --sink jsonl_file --set source.url=https://api.example/orders --set sink.path=./out.jsonl`

**T0 — build.** `registry.Global()` was populated at `init()`. The CLI resolves `http_poll` and `jsonl_file`,
renders their `cfg.Spec`s against the flags, and runs `engine.Validate`:

- declared guarantee `at_least_once` ≤ `min(source: at_least_once, sink: at_least_once)` — ok
- `Chunkable` false, so no chunking checks apply
- sink is not `StructuredInput`, so an encoder is attached: the default `json` encoder, `newline` framer, no
  compression
- `PredPresent(url)` and `PredPresent(path)` satisfied

Runtime assembled: `stores/bolt` at `./canal.db` for `ConfigStore` and `CheckpointStore`;
`store.SingleNode{}` as `Coordinator`; in-process `StatusStore`. Generation 1. Fence `{Generation: 1, Epoch: 1}`.

**T1 — cold start.** `CheckpointStore.Load` returns nothing, so `Source.NewEnumerator` (not `RestoreEnumerator`).
`Source.Discover` returns `OneStream("http_poll", {})`. Planner goroutine starts.

**T2 — plan.** `Enumerator.Poll(ctx, PlanView{Readers: 1, Demand: 1, ...})` returns:

```
Enumeration{
  Status: PlanMore,
  Splits: [ Split{ ID: {Stream:"http_poll", Kind:Change, Seq:0},
                   Range: {Keys: whole, Positions: {From: absent, To: absent}},
                   Ordered: true, Cursor: absent } ],
  Expect: Some(0),
}
```

`PlanState.Unassigned = [that split]`, `Revision = 1`. `PhaseOf(plan)` → `PhaseStreaming` (a `Change` split
outstanding, and no `Backlogger` says otherwise).

**T3 — place.** The placer sees `Demand: 1` (the reader called `RequestSplits(1)` on start) and one unassigned
split with `Group: 0`. `Coordinator.Place` writes the assignment; with `SingleNode` that is an in-memory row plus a
fence. `Reader.Assign(ctx, [split])`. The split stays in `Unassigned` until the reader's first `Snapshot` confirms
it holds it.

**T4 — read.** `Reader.Fetch` calls `poller.Poll(ctx, absent)`. The endpoint returns one object. The reader
returns:

```
Fetch{ Status: FetchMore,
       Batches: [ SplitBatch{ Split: http_poll/change/0,
                              Records: [ Record{Payload: structured{...}} ],
                              Cursor: Some(blob{V:1, Bytes:"2026-07-30T09:00:00Z"}) } ] }
```

**T5 — ingest.** The core stamps the record: `id = 1`, `origin = {Split: http_poll/change/0, Seq: 1, Pos: absent,
Upstream: "", Snapshot: false}`, `ObservedAt = now`. Skew policy: no `Change.CommitTime`, nothing to clamp.
`prefix.Tracker.Track(seq=1, cursor=Some("2026-07-30T09:00:00Z"), n=1)`.

**T6 — encode and write.** No decode (the payload is already structured). No transforms. Buffer is `memory` with
`Durable() == false`, so the acknowledgement boundary stays at the sink. `json` encoder → bytes; `newline` framer →
bytes + `\n`. `Writer.Write(ctx, batch)` returns `WriteResult{Written: 1, Bytes: 148}`, `err == nil`.

**T7 — settle.** `err == nil` and `Failed` empty ⇒ every record durable ⇒ `Tracker.Settle(1)`.
`Tracker.Resolve()` → `("2026-07-30T09:00:00Z", true)`.

**T8 — checkpoint.** The batching timer fires and the min-checkpoint-interval has elapsed. The Checkpointer:
stops admission; drains (nothing pending); `Writer.Flush(FlushCheckpoint)` → bufio flush + fsync;
no committer, no writer state; `Reader.Snapshot(1)` → `[Split{..., Cursor: Some("2026-07-30T09:00:00Z")}]`;
`Enumerator.Snapshot(1)` → `blob{V:1, Bytes:"{\"emitted\":true}"}`; folds the resolved cursor into the split
(already there); one `CheckpointStore.Commit`:

```
key t/default/p/cli-1/ckpt/1/header  → {id:1, generation:1, fence:{1,1}, format_version:1,
                                        phase:"streaming", committed_at:"...", records_in:1, records_out:1,
                                        connectors:{"source":"http_poll@1.0.0","sink":"jsonl_file@1.0.0",
                                                    "encoder":"json@1","framer":"newline@1"}}
key t/default/p/cli-1/ckpt/1/plan    → {enumerator:{...}, unassigned:[], assigned:{...},
                                        completions:[], expected_completions:0, no_more_splits:false, revision:2}
key t/default/p/cli-1/ckpt/1/w/local → {worker:"local", epoch:1,
                                        splits:[{id:{...}, cursor:{version:1,bytes:"2026-07-30T09:00:00Z"},
                                                 ordered:true}]}
key t/default/p/cli-1/ckpt/1/dedupe  → {keys:{}, window:"0s"}
```

One bbolt transaction. `Buffer.Trim(1)`. Admission resumes.

**T9 — `kill -9`, then restart.** `Load` returns checkpoint 1. `Source.RestoreEnumerator(ctx, ec,
blob{"emitted":true})` — the **warm** path, a different method, so the enumerator cannot mistake this for a cold
start and re-emit the split. Splits come back through `Reader.Assign`. `Fetch` calls
`poller.Poll(ctx, Some(Position{Bytes:"2026-07-30T09:00:00Z"}))`. **No record is re-emitted and none is lost.**

That is R3's first milestone: one record from a real source to a real sink, durably, with a checkpoint that
survives `kill -9`.

### 19.2 (b) Full initial scan → incremental stream, crashing halfway through the scan

A chunkable source over a stream `orders` with ~20M rows and a change log. `SourceCapabilities`:
`{Chunkable: true, Replayable: true, ComparableKeys: true, ComparablePositions: true, MidSplitResume: true,
MaxSplitsPerReader: 4}`. Sink declares `Modes: [DestUpsert]` and `Idempotent: true`. Operator selects
`Mode: ModeScanThenTail`, `Destination: DestUpsert`.

**Submit-time validation** (this is where a class of production disaster is prevented):

- chunked snapshot ⇒ sink declares `DestUpsert` **and** `Idempotent` — ok. Had the sink been append-only, the
  pipeline would be **refused here**, because Flink CDC's exactly-once claim depends on the sink upserting by key
  and that dependency is otherwise a silent falsehood.
- `ComparableKeys` + `ComparablePositions` — ok, so the chunk filter can compare.
- `Replayable` — ok, so the per-chunk backfill is readable.

**Phase 1 — chunking.** `Enumerator.Poll` calls `SupportsChunkedSnapshot.SplitKeySpace(ctx, "orders", absent, 64)`,
which returns 64 ranges plus a splitter cursor. It emits 64 `Scan` splits and `Expect: Some(...)`. The next `Poll`
continues from the cursor. After five pages: 320 `Scan` splits, `ExpectedCompletions: Some(320)`, splitter cursor
persisted in the enumerator blob.

The ranges are unbounded at both ends: `(-inf, 62_500)`, `[62_500, 125_000)`, …, `[19_937_500, +inf)`. A row
inserted with id 30,000,000 after chunking began falls in the last chunk and is not missed.

**Phase 2 — parallel scan.** Four readers on one worker (or four workers), each `RequestSplits(4)`. The placer
places 16 at a time. For split `orders/scan/7`, range `[437_500, 500_000)`, the reader runs the **core's**
offset-signal sequence:

```
LOW  = SupportsReplay.CurrentPosition(ctx)          → lsn 0x0000_4A1F
buf  = SupportsChunkedSnapshot.ReadRange(ctx, split) → 62,500 rows, ordered by key
HIGH = SupportsReplay.CurrentPosition(ctx)          → lsn 0x0000_4B02
replay: a bounded Change split, same Group, PositionRange{From: LOW, To: HIGH}
        → 41 change records; the CORE upserts them into buf by Change.Key
emit buf as OpSnapshot records, in key order
report Done with Watermark = HIGH
```

Every one of those five steps except `ReadRange`, `CurrentPosition` and the connector's own key encoding is core
code, written once. The connector's obligation is *"give me ordered rows in a key range"* and *"let me replay from
a position"*.

The reader emits the 62,500 rows in batches of 2,000, each carrying a `Cursor` (the last key read, encoded by the
connector), so mid-chunk resume works. `MidSplitResume: true` and `Ordered: true` license that.

**Phase 3 — the crash.** At the point of failure:

| | state |
|---|---|
| finished scan splits | 143, each with a `Completion{Keys, Watermark, DurableAt}` |
| in-progress splits | 4, one of them `orders/scan/7` with `Cursor = key 471_030` (24,530 of 62,500 rows emitted and durably written) |
| unassigned splits | 173 |
| last durable checkpoint | 88 |

`kill -9`. Not a graceful shutdown: no `Close`, no final `Flush`.

**Phase 4 — restore.** `Load` returns checkpoint 88, whose `Plan` and four `WorkerState`s were written in one
transaction. `RestoreEnumerator` receives the enumerator blob containing the splitter cursor.
`split.Reconcile(plan, freshCatalog, now)` runs: `orders` still exists, `ExpectedCompletions` unchanged, nothing
retired.

The four in-progress splits were in `WorkerState.splits` at checkpoint 88 with their cursors. Because the
restarted process has different worker identities, the placer treats them as unassigned and re-places them —
**with their cursors intact**. `orders/scan/7` is assigned with `Cursor = key 471_030`.

**Phase 5 — resume.** `Reader.Assign` receives it; `ReadRange` is called with a split whose `Cursor` says
"471,030", so the connector's query is `WHERE key > 471030 AND key < 500000 ORDER BY key`. **37,970 rows remain
and 24,530 are not re-read.**

Note precisely what did *not* happen: Benthos would restart the entire 20M-row snapshot from zero, because its
phase decision is `pos == nil` and there is no representation for "partially snapshotted"; every snapshot batch is
tracked with a nil position and the first checkpoint is written only at `snapshot_complete`. Airbyte had the
identical outcome by an independent route and needed a protocol change (`is_resumable`, `CheckpointMixin`) to fix
it. **Two mature frameworks encoded snapshot phase in an opaque checkpoint because neither modelled phases**, and
this is the walkthrough where the split model's cost is repaid.

Also note what the *operator* saw during all of this: `PlanSummary{Phase: scanning, Splits: {Total: 320,
Finished: 143, Expected: 320}, Fraction: 0.4585}`. Snapshot becomes the **most** observable phase rather than the
least, and there is no connector-specific code producing that number.

**Phase 6 — the handoff.** The last `Scan` split finishes at checkpoint 191. `Enumerator.Poll` sees
`PlanView{CompletionsTotal: 320, AllCompletionsDurable: true}` and only now emits:

```
Split{ ID: {Stream:"orders", Kind:Change, Seq:0},
       Range: {Keys: whole, Positions: {From: Some(min(HIGH) over 320 completions), To: absent}},
       Ordered: true }
```

Four properties, each of which is a bug if omitted:

1. **`From = min(HIGH)`, not the latest position.** The earliest-finished chunk's watermark bounds how far back
   the change read must start. Starting at the latest position would skip changes to early chunks that happened
   while later chunks were being scanned.
2. **`AllCompletionsDurable` gates it.** A change read starting from a watermark whose scan rows were never
   durably written would lose data. `Completion.DurableAt` is what makes the gate checkable, and Flink CDC
   persists exactly this map for exactly this reason.
3. **Only after a complete checkpoint following snapshot completion**, so no change record can precede a scan
   record for the same key.
4. **The stream split's parallelism is 1 while the scan's was 320 — with no restart.** Readers with nothing left
   are told `NoMoreSplits` and idle. This is the requirement one-shot planning cannot meet.

**Phase 7 — the filter, and its retirement.** During the change phase the core filters each record:

```go
// engine.ChunkFilter — core code, generic, indexed from day one.
func (f *ChunkFilter) shouldEmit(r record.Record) bool {
    if f.retired[r.Origin().Split.Stream] { return true }         // steady state: one map lookup
    k, ok := r.Change.Get().Key.Get(); if !ok { return true }     // no key ⇒ cannot have been in a chunk
    pos, ok := r.Origin().Pos.Get();    if !ok { return true }
    c, found := f.findByKeyBinary(r.Origin().Split.Stream, k)     // binary search over sorted Order bytes
    if !found { return true }
    cmp, ok := pos.Compare(c.Watermark.Ordinal)
    return !ok || cmp > 0
}
```

Binary search over `Keys.Lo.Order` **from day one**. Flink CDC shipped an O(chunks) linear scan per record and
retrofitted sorted-ranges-plus-binary-search behind a `supportsSplitKeyOptimization` dialect gate; with 320
chunks that was 320 range comparisons per change record.

Once the change position passes `max(HIGH)` for `orders`, `f.retired["orders"] = true` and the filter cost drops
to one map lookup **forever**. The filter is transient, not permanent.

**Phase 8 — steady state.** `PhaseOf(plan)` → `PhaseCatchingUp` while `Backlogger` reports a backlog, then
`PhaseStreaming`. `Completions` are retained for audit but no longer consulted. `Condition{CaughtUp: True}`.
Heartbeat batches (`SplitBatch{Heartbeat: true, Cursor: Some(...)}`) advance the cursor every 30s while idle so the
stored position never ages out of the replication slot's retention.

**What if the source is not chunkable?** The enumerator emits **one** `Scan` split over the whole key range. It is
still resumable if the source offers a cursor (`MidSplitResume`), still checkpointed, still reported on — it is
simply serial. The descriptor says `chunkable: false` and the UI shows "single-threaded snapshot" before the
operator runs it. The degradation is graceful, declared and visible, which is the whole point of capabilities
being data.

### 19.3 (c) A sink fails mid-batch, and the pipeline recovers without loss

Two variants, because they exercise different machinery.

#### 19.3.1 Variant A — at-least-once, partial failure

Pipeline: one `Change` split `orders/change/0`, batching `{Count: 500}`, sink `Tier: at_least_once`,
`PerRecordFailures: true`, retry `{MaxAttempts: 8, Initial: 200ms, Max: 30s, Jitter: full, Terminal: DeadLetter}`.

**T0.** Records with `Origin.Seq` 12,001…12,500 are tracked: `Tracker.Track(seq, cursor, n=1)` for each. Only
seq 12,500's batch carried a `Cursor`; the intervening ones did not (the source names a safe resume point only at
transaction boundaries), so they contribute barriers the prefix must pass but no commit target.

**T1.** `Writer.Write(ctx, batch)` returns:

```
WriteResult{
  Written: 497, Bytes: 402_113,
  Failed: [
    {Record: 12_207, Class: ClassTransientUpstream,  Reason: "429 from api.example; retry after 2s", ...},
    {Record: 12_208, Class: ClassTransientUpstream,  Reason: "429 from api.example; retry after 2s", ...},
    {Record: 12_411, Class: ClassPermanentMapping,   Reason: "field `amount` value 1e400 exceeds destination range NUMERIC(38,9)", Detail: "..."},
  ],
}, err = nil
```

**T2 — what the core does with each of the four groups**, and this is the whole of R7 in operation:

| Group | Core action |
|---|---|
| 497 unnamed records | durable ⇒ `Tracker.Settle(seq)` for each |
| 12,207 / 12,208, `ClassTransientUpstream` | re-batched and retried with full-jitter backoff, honouring the `Retry-After` of 2s (`RetryPolicy.RespectRetryAfter`); attempt counter per record |
| 12,411, `ClassPermanentMapping` | dead-lettered immediately — **no poison retry**, because `Class.Retryable()` is false for this class. A `DeadLetter` record is written through the configured DLQ sink with stage `write`, class, both message audiences, the split id, the position and the payload. Then `Tracker.Fail(12_411, err)`. |

Two records retry; one is dead-lettered. Note that classification came from the **connector at the point of
raise** — only the sink knows that a 429 is transient and a numeric overflow is not — and that both message
audiences were separated at that point, so the UI shows the operator-facing string and the log gets the upstream
text.

**T3 — the prefix does not advance.** `Tracker.Resolve()` returns `(_, false)`: the contiguous settled prefix
stops at 12,206. Even though 497 records after it are durable, **the cursor cannot move past an unsettled
sequence**. That is the entire correctness argument, and it is why the tracker is per-split rather than global: a
stall in `orders/change/0` does not hold back any other split's cursor.

`Tracker.Stuck()` reports `(12_207, 4.2s)`, which the UI renders as *"split orders/change/0, sequence 12,207,
outstanding 4.2s"* — a far better diagnosis than "the checkpoint is not advancing".

**T4 — checkpoint during the failure.** A checkpoint triggers while 12,207 is still retrying. The Checkpointer
runs, `Reader.Snapshot` returns the split with its cursor — which is still the **pre-12,001 cursor**, because
`Resolve` never advanced. Checkpoint 402 is written with the old cursor. This is correct and it is the safe
direction: on a crash here, records 12,001…12,500 are re-read and re-delivered. Duplicates, not loss. `Idempotent`
or `DestUpsert` on the sink is what makes the duplicates harmless, and `WriteResult.Duplicates` is what makes them
visible.

**T5 — resolution.** 12,207 and 12,208 succeed on attempt 3. `Tracker.Settle` both. But 12,411 was `Fail`ed, so
`Resolve` still returns `(_, false)` — **a dead-lettered record does not let the prefix advance past it.**

This is the point where a design either loses data or admits a decision, so the decision is explicit:
`RetryPolicy.Terminal == DeadLetter` means *"the record is durably recorded in the DLQ, and the DLQ write is
therefore the durability that licenses advancing"*. So the sequence is: DLQ write returns success →
`Tracker.SettleDeadLettered(12_411)` → prefix advances to 12,500 → next checkpoint commits the cursor.

If the DLQ write **fails**, the prefix does not advance, the pipeline goes `Degraded` then `Paused`, and
`canal_checkpoint_age_seconds` climbs. There is no configuration under which a record vanishes: it lands in the
destination, or in the DLQ, or the pipeline stops. That is the direct answer to decision-space trap 18 (*a terminal
outcome that silently discards data*), which Flink's `signalFailedWithKnownReason` — documented as *"only logs the
error, discards the committable and continues"* — is.

**T6 — what the operator sees throughout.**

```
Phase: Running   ObservedGeneration: 3
Conditions:
  Configured   True
  Connected    True    ← note: does NOT imply Progressing
  Assigned     True
  Progressing  False   Reason=CommitStalled  Message="split orders/change/0 sequence 12,207 outstanding 4.2s"
  CaughtUp     Unknown
  Degraded     True    Reason=SinkRejecting   Message="429 from api.example; 2 records retrying, 1 dead-lettered"
Metrics:
  canal_checkpoint_age_seconds       41
  canal_records_failed_total{stage="write",class="transient-upstream"}  2
  canal_records_failed_total{stage="write",class="permanent-mapping"}   1
  canal_backoff_seconds_total{stage="write",class="transient-upstream"} 6.4
```

#### 19.3.2 Variant B — exactly-once via 2PC, crash between snapshot and commit

Sink implements `SupportsCommitter` (staged object writes finalised by a manifest commit). Declared tier
`exactly_once_2pc`.

```
T0  Write(batch) → 4 staged objects, not visible to readers of the destination
T1  checkpoint 77 triggers
      Flush(FlushCheckpoint)              → staged objects fsynced
      PrepareCommit(77)                   → [Committable{ID:c1..c4, Blob:{keys...},
                                                          Splits:[orders/change/0],
                                                          Expires: Some(now+1h)}]
      Committables[77] = [c1..c4]
      SnapshotWriter(77)                  → writer's own in-progress state
      Reader.Snapshot(77), Enumerator.Snapshot(77)
      CheckpointStore.Commit(...)         → ONE transaction, includes Committables[77]
T2  --- checkpoint 77 is durable ---
T3  *** kill -9, BEFORE Commit(requests) runs ***
```

**Recovery.** `Load` returns checkpoint 77, whose `Committables` map contains `{77: [c1..c4]}`.
`RestoreWriter(state)`, then — **before admitting any record** — the core calls
`Commit(ctx, [{c1,Attempts:1},{c2,...},{c3,...},{c4,...}])`.

The committer finds c1 and c2 already in the manifest (the process died mid-loop) and returns:

```
[ {ID:c1, Disposition: DispositionAlreadyCommitted},
  {ID:c2, Disposition: DispositionAlreadyCommitted},
  {ID:c3, Disposition: DispositionCommitted},
  {ID:c4, Disposition: DispositionRetryUpdated,
          Committable: Some(c4'),   // half the objects were already in the manifest; c4' names the rest
          Class: ClassTransientUpstream, Reason: "manifest write partially applied", RetryAfter: 500ms} ]
```

`AlreadyCommitted` is counted separately (`canal_committables_already_total`), so a replay after restart is
**visible** rather than indistinguishable from a fresh commit. `RetryUpdated` is re-offered with `Attempts: 2` and
the revised committable — Flink's `updateAndRetryLater`, which exists precisely because a boolean return cannot
express partial success and a batch-level exception cannot say *which* part.

Only when every committable of id ≤ 77 has reached `Committed` or `AlreadyCommitted` does the core clear
`Committables[≤77]` and resume admission.

**Three subsuming-contract properties exercised here:**

- If checkpoint 78 had completed before the crash, `Commit` would publish everything **up to and including** 78,
  covering 77's committables too. A lost confirmation is repaired by the next checkpoint, not by bookkeeping.
- An **aborted** checkpoint 78 does **not** discard 77's committables. `Committables` is not cleared on abort;
  the next successful checkpoint simply covers a longer span. Getting this backwards is a data-loss bug dressed as
  cleanup.
- If c4's `Expires` had passed, the core **fails loudly**: `Condition{Degraded: True, Reason: CommittableExpired}`
  plus a terminal error naming the committable, the split and the expiry. It does not skip it. Flink's
  `transactionTimeout` + `ignoreFailuresAfterTransactionTimeout` + a warning ratio is the honest treatment and the
  silent-skip variant is not.

### 19.4 (d) Serving the frontend, with zero connector-specific code in core

The UI is a renderer of three connector-declared documents plus one core-computed one. No `if connector == "x"`
exists anywhere in `control/` or in the frontend, and there is an `archtest` assertion that `control/` imports no
package under `connectors/`.

**Step 1 — the connector list.** `GET /connectors` → `[]Descriptor`, straight from the registry. **Nothing is
instantiated**, so this page is fast and cannot be broken by a connector whose constructor panics. Each card
renders `Title`, `Summary`, `Support` (a badge from a closed enum with one vocabulary), `Version`, and a
capability chip row generated from the `Capabilities` JSON:

```
postgres  Certified  2.1.0    [chunked snapshot] [replay] [comparable positions] [resumable]
mongodb   Beta       0.9.0    [replay] [comparable positions]
http_poll Community  1.0.0    [single split]
```

Those chips are the operator's answer to *"will my snapshot be parallel and resumable?"* **before** they run
anything. That question is unanswerable in Connect, Benthos, Vector and Airbyte.

**Step 2 — the config form.** `GET /connectors/source/postgres` → `Descriptor.Config`. The form renderer walks
`Fields`, and every presentational decision is data:

| Field property | UI effect |
|---|---|
| `Kind` | widget: text / number / duration / bytes / select / checkbox / list / object / union |
| `Group`, `Order`, `Width` | placement |
| `Title`, `Description`, `ShortDescription` | label, help panel, inline hint |
| `Advanced` | behind a "Advanced" disclosure, so the common tab stays short |
| `Secret` | password widget; value never returned by the API on read-back |
| `Default` / `Optional` | required marker, placeholder |
| `Deprecated` | strikethrough plus the replacement advice from the field itself |
| `Choices` | static select |
| `ChoicesFrom: "list_schemas"` | select that calls `POST .../choices` with the partial config |
| `VisibleIf` / `RequiredIf` | **evaluated in the browser**, so conditional fields appear per keystroke with no round trip |
| `Variants` + `Discriminator` | a tagged-union selector: "Auth method: [password ▾]" swapping the child fields |
| `Component: SinkKind` | a nested component picker that recursively renders that component's own spec |

The composite fields mean `batching`, `retry`, `tls` and `buffer` render **identically for every connector**, with
identical help text, because there is one spec fragment for each.

**Step 3 — validation, two tiers.** Every keystroke runs the declarative tier in the browser: kinds, ranges,
`Predicate` constraints, `CodeUnknownField`. On blur or on demand, `POST .../validate` runs `Validatable.Validate`,
which may hit the network, and returns **all** diagnostics at once, each attached to a `Path`:

```json
[ {"path":["host"],"severity":"error","code":"unreachable",
   "message":"cannot resolve db.internal: no such host"},
  {"path":["replication_slot"],"severity":"warning","code":"missing",
   "message":"slot `canal_orders` does not exist; canal will create it",
   "recommended":["canal_orders"]},
  {"path":["publication"],"severity":"error","code":"permission",
   "message":"role `canal` lacks REPLICATION; grant it with: ALTER ROLE canal REPLICATION"} ]
```

Never one bool and one string (Airbyte's `check`), never fail-fast on the first problem. `POST .../test` returns
`[]TestResult` — *named* tests with durations, so "authenticate: ok 40ms / read orders: failed" is renderable as a
checklist.

**Step 4 — the stream picker.** `POST /sources/postgres/discover` → `schema.Catalog`. The picker is a table of
streams with schema previews, `EstimatedRecords`, and per-stream selectors for `Mode` (constrained to
`SupportedModes`) and `Destination` (constrained to the *sink's* `Modes`). `SourceDefinedCursor: true` greys the
cursor selector out — **the connector constrained operator choice in data, not in code.**

The operator's choice becomes a `ConfiguredCatalog` that **embeds the discovered catalog verbatim**, so
`GET /pipelines/{id}/catalog/drift` is a pure diff of a fresh `Discover` against the embedded one:

```json
{"added_streams":["public.refunds"],
 "removed_streams":[],
 "changed":[{"stream":"public.orders",
             "changes":[{"kind":"add_field","field":["discount_bps"],"type":"int64"}]}],
 "policy":"lenient","actionable":true}
```

Drift is a diff, not a runtime surprise at 3am.

**Step 5 — live progress.** `GET /pipelines/{id}/plan` → `PlanSummary`, computed entirely from `PlanState`:

```json
{"phase":"scanning",
 "splits":{"total":320,"unassigned":173,"assigned":4,"finished":143,"expected":320},
 "fraction":0.4585,
 "eta":"00:31:12",
 "streams":[{"stream":"public.orders","mode":"scan_then_tail",
             "splits_finished":143,"splits_total":320,"records":8_941_204,
             "backlog":{"records":11_058_796,"exact":false,"as_of":"2026-07-30T09:41:02Z"}}],
 "workers":[{"worker":"w-1","epoch":4,"splits":2,"records_per_second":41_882}]}
```

`GET /pipelines/{id}/plan/splits?page=2` → the per-split table, paged. Every number here derives from `Split`,
`Completion` and `Position.Scalar`. **A source that does not supply `Scalar` yields `fraction: null` and the UI
renders an indeterminate bar** — it does not render `0%`, which would be a lie, and the series is omitted from
Prometheus entirely rather than exported as zero.

**Step 6 — the topology tree.** `GET /pipelines/{id}/stages` → `[]StageInfo`, *discovered* from the composed
pipeline. A pipeline whose sink is `retry → broker[fan_out] → {s3, bigquery}` renders as a tree with per-node
throughput and error counts, because component nesting propagates the observability path automatically. **Adding a
new meta-component type does not change the API schema** — R1's actual content.

**Step 7 — honest status.** The status card renders `Phase` + `Conditions[]`, in the fixed order *what works /
what does not / any qualifier / the next action*, with each badge carrying `data-condition`, `data-status` and
`data-reason` attributes so the copy is assertable in tests. A healthy connection with a stalled commit renders as
unhealthy, and there is a pinned fixture test that fails if it ever renders as healthy.

**Step 8 — the tap.** `GET /pipelines/{id}/tap?stage=sink.retry.child.broker[1]&n=20` samples records at that
edge, with `Secrets` structurally absent from the response type. Requires `ScopeReadRecords`, which an operator who
may merely restart the pipeline does not have.

**What core knows about postgres, mongodb or bigquery in all of the above: their names, as map keys.**

### 19.5 (e) The same pipeline, standalone then horizontally scaled

The spec, the connector code, the checkpoint format and the record path are **byte-identical** in both. What
changes is four constructor arguments.

#### Standalone

```
$ canal run --file orders.yaml
```

```go
rt := engine.Runtime{
    Config:      bolt.NewConfigStore("./canal.db"),
    Checkpoints: bolt.NewCheckpointStore("./canal.db"),
    Coordinator: store.SingleNode{},   // always planner; Claim returns everything; leases are
                                       // no-ops that STILL CARRY A FENCE, so the fenced-write
                                       // path is exercised in dev and cannot rot
    Status:      store.InProcess{},
}
p, _ := engine.Build(spec, rt, registry.Global())
p.Run(ctx)
```

One process. The planner goroutine holds the `Enumerator`. Four `Reader` goroutines hold splits. The placer places
locally. `Reader.Assign` is a direct method call. `CheckpointStore.Commit` is one bbolt transaction. **`canal run`
with no infrastructure produces a working pipeline with a durable checkpoint on a laptop with nothing installed** —
R3's milestone and canal's single biggest adoption lever over Connect and Airbyte.

#### Coordinated

```
$ canal serve  --coordinator postgres://…   # control API + planner election
$ canal worker --coordinator postgres://…   # ×6, in a k8s Deployment
```

```go
rt := engine.Runtime{
    Config:      pg.NewConfigStore(db),
    Checkpoints: pg.NewCheckpointStore(db),
    Coordinator: pg.NewCoordinator(db),
    Status:      pg.NewStatusStore(db),
}
```

**What moves.** Exactly one thing: the `Enumerator` runs in whichever process won `CampaignPlanner`, and the six
worker processes hold `Reader`s. The planner reaches those readers through `engine/remote.remoteReader`, which
**satisfies `connector.Reader`**. The engine's placer, checkpointer and prefix trackers are unchanged code paths —
they cannot tell.

**Placement.** Each worker's readers call `RequestSplits(4)`, so `PlanView.Demand` is up to 24. The placer writes
assignment rows; workers `Claim` them with `SELECT … FOR UPDATE SKIP LOCKED`, so **work claiming needs no leader**
and a planner election in progress does not stall assignment of already-planned work. 320 scan splits spread across
24 reader slots. Then the single `Change` split goes to one slot, and the other 23 are told `NoMoreSplits`.

**Checkpointing.** One global monotonic id. The Checkpointer (co-located with the planner) requests
`Reader.Snapshot(id)` from all 24 readers in parallel, collects `[]Split` from each, and writes one Postgres
transaction containing the plan row and 6 worker rows. A reader that misses the window **aborts** the checkpoint;
the subsuming contract makes the next one cover a longer span. No barriers, no alignment, no in-flight persistence.

**Worker loss.** Worker `w-3` is `OOMKilled` holding 4 splits.

1. Its lease is not renewed. After the 30s TTL the coordinator marks the assignments orphaned.
2. **Reassignment is delayed 120s** with exponential backoff on consecutive revoking rounds, so a bouncing worker
   reclaims its own assignments rather than triggering a reshuffle. Both timers configurable. This is KIP-415's
   `scheduledRebalance`/`delay`, which existed only after documented rebalance storms that *"could take several
   minutes to stabilize"* — and whose incremental-cooperative replacement then shipped its own imbalance bug
   (KAFKA-12495). Pull-based assignment plus a deliberate delay is the answer both of those iterated toward.
3. The 4 splits are recovered **from the last checkpoint** — the only truth that survives `OOMKilled` — with their
   cursors, and returned to `Unassigned`. `Enumerator.Report{Returned: [...]}` gives the connector a hook it
   usually ignores.
4. They are re-placed on other workers and resume mid-split. **No work is lost and none is redone beyond the last
   checkpoint.**

Compare what the other systems must do here. Vector cannot: two instances with the same stateful source config
both read the same input and duplicate, its only horizontally-scalable stateful source is `kafka` (borrowing
someone else's coordinator), and its disk buffers pin a process to a volume. Benthos and Airbyte have no split
concept and therefore no reassignment. Connect restarts a task and trusts it to resume from a stored offset.

**Fencing, and why the lease is the only token.** Suppose `w-3` was not dead, only network-partitioned, and comes
back believing it still holds the splits. It calls `Reader.Snapshot` and the checkpointer tries to write with
fence `{Generation: 5, Epoch: 11}` while the store has seen `{5, 14}`. `CheckpointStore.Commit` returns
`ErrFenced`, the worker learns it is fenced, drops its splits without writing, and rejoins with a new epoch.
**Leadership is never trusted for correctness** — the verified k8s caveat is that its leader election *"does not
guarantee that only one client is acting as a leader (a.k.a. fencing)"* and clients infer leadership from locally
captured timestamps. The only safe use is to reduce contention, and every correctness decision goes through the
fenced write.

**Control plane outage.** Postgres becomes unreachable for four minutes. All 6 workers keep `Fetch`ing, keep
writing to the sink, keep resolving prefixes. They **cannot** checkpoint. `canal_checkpoint_age_seconds` climbs to
240, `Progressing: True` (records are moving) while `Degraded: True, Reason: CheckpointStoreUnreachable`. When
Postgres returns, the next checkpoint covers the longer span. **The data plane survived a total control-plane
outage**, which is the single most important deployment property in this design and the reason the planner is
allowed to do nothing but plan.

**Disk buffers.** If a pipeline configures a durable disk buffer, that buffer is **worker-affine state**: the
`Split.Group` mechanism plus an affinity annotation pins those splits to that worker, and the interaction with
rescheduling is decided explicitly rather than discovered — Vector's documented sharp edge is precisely that a disk
buffer pins a process to a volume and nothing in its model says so.

**What did NOT change between the two deployments:** the connector code, the `cfg.Spec`, the record path, the
`Split` type, the checkpoint keys and bytes, `PlanState`, the metric names, the status document, the read model,
the DLQ format. A checkpoint written by `canal run` on a laptop restores into a coordinated cluster and vice versa,
because the format is self-contained and relocatable and there is exactly one of it.

---

## 20. What this proposal chose for each decision in the decision space

### D1 — the record/envelope model → **(c) generic envelope + optional typed facet**, plus core-owned provenance

Three separately-lifetimed layers (§5): `Payload` with two lazily-cached views and mutability in the accessor name;
`Meta` as a separate addressable namespace with three tiers plus a `Secrets` compartment; `Change` as an optional
typed facet accessed as `(facet, ok)`.

Agreeing with the decision space: option (a) does not hold (Benthos's two CDC inputs invent incompatible metadata
vocabularies, so every CDC-aware sink special-cases every source — constraint #1 violated by drift), and option (b)
forces relational shape onto webhooks and metrics scrapes.

**Adding a fourth layer the decision space did not name:** `Origin`, core-owned and structurally immutable, carrying
`{Split, Seq, Pos, Upstream, Snapshot}`. This is not optional polish — it is what the split model *needs*: `Seq`
is what the prefix tracker keys on, `Split` is the ordering scope, `Pos` is what the chunk filter compares, and
`Snapshot` is derived from the split's `Kind` so it cannot disagree with the plan. Making provenance unexported with
`Derive()` as the only way to produce a descendant is what guarantees canal never needs Connect's
`originalTopic`/`originalKafkaPartition`/`originalKafkaOffset` retrofit.

**Diverging on two smaller points.** `Op` has five values not six — no `Replace`, whose meaning Flink CDC's own
docs leave blank. And `Change.Key` is an `ord.Key` rather than a field list, so the *same value* serves as the
sink's upsert key and the chunk filter's comparison input: one representation, no cross-map (R9).

Adopted verbatim: Benthos's payload accessors and metadata tiers, Vector's namespace separation and `secrets`
compartment, Benthos's error-on-the-record. Rejected: `MetaSet`'s empty-string-deletes, and untyped JSON as the only
payload option.

### D2 — serialisation → **(a)+(b): serializer + framer + compression as separate registered stages**, with the declared escape hatch

§8.4. Sources and sinks implement **transport only**. `Decoder` returns `[]Record` from one frame because
one-frame-to-many is real. `Framer.Scan` matches `bufio.SplitFunc` because Go already has the right signature.
`Compressor` is separate because ndjson-gzip is three orthogonal choices.

Codecs participate in submit-time validation via `Accepts()`/`Requires()`, so an incompatible pair is a startup
error (Vector's `input_type()`/`schema_requirement()` as declared data). `SupportsStructuredInput` is the narrow
declared escape hatch and the core refuses to attach an encoder to such a sink.

**Diverging on the fan-out ack aggregator.** The decision space says to copy Benthos's
`SimpleBatchScanner` + `AutoAggregateBatchScannerAcks` reduction. This proposal doesn't need it: `Origin.Seq` is
per *source* record, decoded children share their parent's `Origin`, and `prefix.Tracker.Track(seq, cursor, n)`
counts descendants. The counting is a property of provenance rather than of a callback, so **no connector author can
even see it**, let alone hand-write it.

### D3 — checkpoint ownership → **the split carries the position; opaque payloads under a typed header; two-level collapses to one**

§13.1. This is the decision where the proposal diverges most from the decision space, deliberately, and §6.4 argues
it at length.

Kept: opacity at the boundary (`VersionedBlob` from a connector-supplied `Serializer`), because opacity is what
bought Airbyte an entire new phase model with no wire change; the typed header, because opacity alone is why
Connect has no source-side lag metric; the schema epoch inside the checkpoint record; the contiguous-prefix
resolver **in core** and **per split**; `OffsetStorageWriter`'s double-buffered flush; checkpoint keys scoped to
`(tenant, pipeline, split)` in a shared store, never a local file; checkpoint edit as a first-class API operation;
a failed commit as an escalatable condition; `SupportsTokenStore` as an optional capability rather than the default.

**Dropped: `Shared` and `Streams` as separate checkpoint members.** A shared log position is one `Change` split; per-stream
positions are one `Change` split per stream. The source chooses by planning. "Is the commit atomic across streams"
becomes an *observable property of the plan* rather than a capability flag the core must trust.

**Dropped: `PositionComparer` as an interface.** Comparability is `ord.Order` — data — because a type assertion
cannot cross a process boundary (trap 11) and a comparison method cannot be served to a browser. `Fraction` becomes
`ord.Scalar`. Both optional, both degrade by omitting the series rather than emitting zero. **This is the single
change that most improves the wire story**, and it is forced by the angle: the core must reason about split bounds
and watermarks, so it needs comparison, so comparison must be portable.

Kept as typed properties rather than conventions: "not resumable here" is `SplitBatch.Cursor` absent; "safe resume
point ≠ record position" is `SplitBatch.Cursor` versus `Origin.Pos`.

Rejected, with the decision space: merge-patch checkpoints (the per-split decomposition already avoids most of the
motivating cost; measure before adding patch semantics).

### D4 — commit timing → **the core owns the position mapping; the sink's return is the ack; 2PC as the tier-3 capability; never a wall clock alone**

§13.4–13.6. `Writer.Write` returning `nil` **is** the acknowledgement, and the core — not the sink — maps it to a
split cursor. That keeps Benthos's "a new sink cannot get checkpointing wrong" while supplying the position Benthos
gives up.

Boundaries are in-band and source-meaningful (§13.5), with the granularity defect fixed both ways: the sink can
**request** a boundary. And the convention defect is fixed structurally — advancing requires `Write` to return
success, so "acknowledge before durable" requires a sink to *lie about a return value* rather than merely be sloppy
about forwarding a message. R4 becomes a type obligation.

Tiers are which interfaces you implement, validated at submit time as `min(source, sink, declared)`. The Checkpoint
Subsuming Contract is implemented verbatim including "abort ≠ discard". Committables have `Expires` and expiry
fails loudly. **`Disposition` has five values where Flink has six**, because
`signalFailedWithKnownReason`'s documented behaviour (*"only logs the error, discards the committable and
continues"*) is a terminal outcome that silently drops data, and there is no such outcome here.

**Diverging on the ack primitive.** The decision space (Chain E) worries that a richer ack breaks wire-shippability
and concludes the ack must stay `(ctx, err) error` so it can become an opaque `uint64`. This proposal has **no ack
primitive at all** — no `AckFunc`, no correlation id — because `WriteResult` names records by `RecordID` and the
core holds the `(RecordID → Origin)` map. Chain E's risk is not mitigated; it is deleted.

### D5 — the connector/task split → **(b) enumerator + reader, verbatim in structure, with three divergences**

§7. Adopted: the reader's checkpoint is its split list; `Report{Returned}` as `addSplitsBack`; pull-based
assignment; a struct split identity with `String()` for logs only; boundedness as data; `Ordered` as the property
that licenses mid-split checkpointing; per-split ordering scope stated at the `Batch` type; the plan as durable
state with assignment rows, fences and leases; hard-enforced parallelism caps; delayed reassignment with backoff;
assignment-scoped (`Assign`/`Revoke`) versus instance-scoped (`NewReader`/`Close`) lifecycle;
`(WorkerID, Epoch)` from the first commit; per-phase parallelism with no restart.

Three divergences, all in the same direction — **less connector knowledge of the runtime**:

1. **The enumerator does not assign.** No `assignSplit(split, subtaskId)`, no `handleSplitRequest(subtaskId, host)`,
   no `addReader(subtaskId)`. The enumerator emits splits; a core placer places them. Affinity is expressed as
   `Split.Group` (co-location) — data, not a worker id. This is Part-0 axiom #1 taken literally, and it is what
   makes standalone and coordinated the same code path rather than two.
2. **Three methods, not seven.** `Poll` mints, `Report` learns, `Snapshot` persists. Flink's `start`,
   `handleSplitRequest`, `addSplitsBack`, `addReader`, `snapshotState`, `notifyCheckpointComplete`,
   `handleSourceEvent` collapse into them, because every one of the collapsed methods was "a fact arrived" or "give
   me work", and separating *learning a fact* from *deciding what to do* makes the enumerator's state machine a
   pure transition table that is testable without a runtime.
3. **Threading is a documented core guarantee, not a managed API.** No `callAsync`, no `runInCoordinatorThread`, no
   javadoc warning about shared state. `EnumeratorContext.Go` exists for slow I/O and delivers its result back as an
   event, so there is no way to mutate enumerator state off-thread — which is what Flink CDC's two shipped
   `ConcurrentModificationException` fixes were about.

### D6 — snapshot modelling → **(d) boundedness of splits as the mechanism, (b) per-stream modes as the operator model, derived phase for reporting**

§6.1–6.2, §11.3, §19.2. The backfill *is* a bounded `Change` split; snapshot chunks *are* `Scan` splits. No phase
enum in the data path and nothing to switch on. `PhaseOf(PlanState)` is a **function**, so there is no field to set
and `grep -r "case Phase"` returns only the renderer.

The operator model is Airbyte's orthogonal pair (`StreamMode` × `DestinationMode`) on the *configured* catalog,
validated against the *discovered* one. Pipeline type is data (R1) and there is no pipeline-type enum anywhere.

Also adopted: every phase is checkpointable including a full scan; the core owns the handoff invariant
(`min(HIGH)`, gated on `AllCompletionsDurable`); scan records are typed as such via `Origin.Snapshot`;
`ClassEndOfInput` terminates a bounded pipeline on the same runtime with a terminal `RunCompleted`; backlog mode as
declared data; heartbeats in core; the in-band control channel from day one plus an idempotent reconciliation sweep
because every control message may be lost.

Noted and not taken: Estuary-style reduction annotations (option (e)), which would dissolve the handoff entirely
but require a merge model in core record semantics and push canal toward being an opinionated stream processor. It
remains the alternative to the `Op` enum, worth one focused investigation, and it would *simplify* D4, D6 and D7
simultaneously — which is why it is recorded here rather than dismissed.

### D7 — snapshot chunking → **(b) core owns a generic chunked engine, behind opt-in capabilities, with both documented scars pre-fixed**

§13.7, §19.2. All eight steps in core, gated on `Chunkable + Replayable + ComparableKeys + ComparablePositions`.
Both Flink CDC scars fixed before they happen: **the filter is indexed from day one** (sorted `Order` bytes plus
binary search, not an O(chunks) scan behind a later dialect gate), and **the completion set is paged by reference
from day one** (`PlanView.Completions` + `CompletionsTotal` + `CompletionsFrom`, not four retrofitted event types
plus a truncation path).

Both hard requirements are submit-time checks, not production discoveries: chunked output requires a keyed-upsert
sink, and mid-split checkpointing requires an ordered split. The read-only watermark variant is required — the log's
own position, never an injected marker — because demanding DDL or write access on a source is a deployment tax many
operators will refuse.

The chunk splitter's own cursor is persisted (`ChunkPage.Next`), so slicing a huge object resumes mid-way.

### D8 — schema → **(c) + (d) + (e): discovery, schema-on-split-by-reference, in-band change events, core drift policy, canonical type set**

§11.3–11.5. `Discover` is required and its output persisted; `ConfiguredCatalog` embeds it verbatim so drift is a
diff. Keys and cursors are `[][]string`. `SourceDefinedCursor`/`SourceDefinedKey` let a connector constrain
operator choice in data.

**Diverging on where the schema rides.** The decision space says schema rides on the split, deduplicated in the
enumerator's state. This proposal makes the split carry a **`schema.Ref` (a fingerprint)** and the schema table live
once in `PlanState.Schemas`. Same effect, but it means Flink CDC's `SchemalessSnapshotSplit` variant plus its
`fillTableSchemas` rehydration step are never invented — the deduplication is the *only* representation, so there is
no second type and no rehydration path to forget.

Adopted: in-band ordered change events with schema-before-data; tracked into checkpointable split state whether or
not emitted; applied only on a `Change` split (a `Scan` split's schema is pinned at its start); emission opt-in;
schema epoch committed atomically with position. The five-mode drift policy wholesale with never-destructive
`lenient` as default and per-kind include/exclude with prefix matching. Sink split into write + apply facets, with
the **quiesce-and-flush** sequence owned by the core (§11.5). The canonical type set as a lossless intersection with
parameterised logical types in a separate struct, an unknown-precision escape hatch, structural fingerprinting as
identity, and per-sink conversion memoised by fingerprint. Logical types are **not** a naming convention with a
parameter map (Connect's documented source of silent disagreement). Vector's global mutable `log_schema` rejected
unconditionally.

### D9 — required vs optional surface → **(b) and (c), with registration-time cross-checking made possible by one generic function**

§9, §10. Behaviour is an optional interface; the fact of it is declared data; they are cross-checked at
registration. Required surfaces stay small: `Enumerator` 4, `Reader` 5, `Writer` 4, `Sink` 2, `Source` 5.

**The contribution here is making the registration-time panic actually implementable in Go.**
`RegisterSource[E Enumerator, R Reader](spec)` uses the type parameters to assert capability interfaces against
`var e E` — a nil typed value, since method sets belong to types — so the check needs **no instance and no
reflection**. It runs at `init()`, so a connector author sees a mismatch on their first `go test ./...`. It also
runs in the reverse direction: an implemented-but-undeclared capability panics too, because a silently unused
capability is as bad as a lying flag.

Everything else adopted: type-assert in exactly one place (`registry.resolve`); never add a method to a required
interface; **no type parameters and no nested interfaces on any plugin interface**; grow core capabilities through
the context; an explicit `(APIVersion, Capabilities)` handshake; every optional interface exported; every method
`(ctx, serialisable) → (serialisable, error)`; the declarative spec primary with the Go builder as sugar;
`AdoptsStateOf` so a rewrite is a declaration.

`engine/remote` is the existence proof rather than a promise: three types satisfying `Reader`, `Enumerator` and
`Writer`, reporting a dead subprocess as `ClassNotConnected` so process supervision reuses the connection state
machine.

### D10 — config self-description → **one Go-declared spec that emits JSON Schema, with composites, predicates, named dynamic hooks and sink field mappings**

§11.1–11.2, §11.6, §19.4. All thirteen recommendations taken. Highlights and the one divergence:

- Variadic `Path` segments, never dotted strings. Nesting and tagged unions first-class (`Variants` +
  `Discriminator`, which is the one thing `oneOf`+`const` does that `ConfigDef` provably cannot).
- Composite field specs paired with extractors, which is the mechanism that makes D2, D11 and D12 *configuration*.
- Conditional required/visible as declarative predicates over a **six-operator closed set**, evaluable in the
  browser. **Diverging from Benthos's Bloblang**: an embedded expression language would make lint rules, batch
  predicates, partition templates and Segment-style default mappings all browser-evaluable data, and it is
  genuinely clever — but it is a large dependency and a permanent vocabulary. Six operators plus `Validatable`
  covers every conditional-form case in the dossiers. The cost is admitted in §22.7.
- Named `ChoicesFrom` hooks rather than live `Recommender` callbacks, because a callback cannot cross a wire.
- Two-tier validation returning per-field diagnostics, plus named `ConnectionTest` results as a list.
- Typed diagnostics with `CodeUnknownField` defaulting to Error and line/column.
- Segment's sink field-mapping model with a small closed expression vocabulary (§11.6) — the complete answer to
  "specialised sink UX with zero core knowledge".
- `Secret: true` in the connector's own spec buys redaction, masking, encryption at rest and reference indirection.
- **No `(T, error)` accessor ladder**: `cfg.Field[T](c, path...)` plus one deferred `c.Err()`.
- The registry publishes a cached `Descriptor` so the UI lists connectors and renders forms without instantiating
  anything.

### D11 — error and retry classification → **a closed nine-class set plus two control sentinels, connector-classified, honoured at every call site**

§14. The `design-rules.md` taxonomy becomes real Go, extended with Benthos's two wire-representable control
sentinels. `Retryable()`/`Terminal()` are derived from `Class` in one place, so a class and a flag cannot disagree.
The class is a bounded metric label precisely because the set is closed. Retry policy is
`{MaxAttempts, backoff, Terminal}` with `-1` required to be explicit for unbounded. Per-stage classification with
thirteen named stages. Per-record DLQ **for sources as well as sinks** (Connect's `ErrantRecordReporter` is
sink-only, and source-side decode failures are extremely common). Error travels on the record and strict mode is the
**default**. Rich errors degrade to simple ones with no positional-correlation machinery. Audience split. `retry`
and `fallback` are composable components with a **sanctioned** stage declaration (`Meta.Children()`) rather than
Benthos's undocumented `Unwrap()` hole. Sustained backoff sets `Degraded` and the alertable metric is
`canal_backoff_seconds_total` — cumulative time, not a count.

### D12 — backpressure and batching → **bounded by construction, framework-owned, per-split, one accounting concept**

§12. Every edge bounded. The reader hand-off is `chan SplitBatch` with capacity **2 batches** (Flink's default,
after two major features spent undoing deep buffering). **One** in-flight concept, per split, which is
simultaneously the ordering scope, the checkpoint-window bound, the backpressure mechanism and the fairness unit —
replacing Benthos's five overlapping knobs with their three user-facing names and one documented deadlock.

`PushOutcome` puts R6's rejection path in the type; drops are counted and are always the **newest**; buffers chain;
`Durable()` decides where the acknowledgement boundary sits so the checkpoint store and the buffer need no shared
WAL. Batching is declared as an **options struct** with four orthogonal triggers including a predicate, and a
`Splitter` primitive for the inverse gap. Benthos's `Batcher` API taken exactly. Partitioned batching is a
framework combinator. `PauseOrResume` is one batched atomic call. Metrics include the soft/hard backpressure split.
Ack idempotency is structural and there is one timer wheel. The upstream-retention tension is documented and
heartbeats are part of the answer.

### D13 — topology → **(b) + (e): fixed outer shape, recursive composition, transforms core with the full return vocabulary**

§15. The fixed shape is an implementation fact, not an API schema. All variety from component-valued config fields
with automatic observability nesting. `Meta.Children()` is the sanctioned stage declaration. Transforms return
`[]Batch` so 1→1, 1→N, 1→0 and N→M need no extra vocabulary. Provenance structurally immutable across transforms.
Ack composition is `Track(seq, cursor, n)` rather than an ack graph, and it handles fan-out, filter and rebatch with
no checkpointer code path. Fan-out semantics documented at the config field with `fail_fast` variants shipped. No
barriers for multi-input consistency. Ordering scope stated **at the concurrency knob**.

A split is explicitly **not** a DAG node, per the decision space's warning: splits partition *input work*; stages
compose *processing*. `Split.Group` co-locates splits on a reader; it has nothing to do with topology.

### D14 — the standalone↔distributed seam → **(d) four interfaces, two assemblies, a leader that only plans**

§16. All non-negotiables adopted: byte-identical connector API in both modes; `CheckpointStore.Commit` atomic
across the whole map; the leader only plans, so the data plane survives a total control-plane outage; the assignment
lease is the only fencing token and leadership is never trusted for correctness; Postgres first with etcd as the
conformance target; Kafka as coordination store rejected; `canal run` works on a laptop with nothing installed;
root-config/stream-config split; disk buffers as explicitly worker-affine; running state separated from durably
written snapshots; exactly one relocatable snapshot format; `Phase` + `Conditions` + `ObservedGeneration` with
`Connected` unable to imply `Progressing`; `canal_checkpoint_age_seconds` primary and never a metric called `lag`;
hard-closed label vocabulary with per-stream detail from the read model; k8s-operator-ready without an operator;
metrics over HTTP natively; a tap-any-edge sampler and a `/ready` returning a status tree; recovery instrumented in
phases.

**The one thing this proposal adds:** there is no transport interface. §16.2. `engine/remote` implements
`connector.Reader`/`Enumerator`/`Writer`, so the seam is not an abstraction the engine knows about — the engine
literally cannot tell local from remote. That is only possible because the split model makes a reader's state a
list of values and its hot path one method returning a value.

---

## 21. Design rules, and the nine open decisions closed

### 21.1 R1–R13

| Rule | How this design satisfies it |
|---|---|
| **R1** topology is data, never schema | The outer shape is an implementation fact; the API reports `[]StageInfo` *discovered* from the composed pipeline, with no stage count and no ordinal. Pipeline "type" does not exist — it is `StreamMode` per configured stream. `PlanState` is the one representation of what work exists and who holds it; buffers are one entity in one place. Adding a stage type or a meta-component is not a contract change. |
| **R2** canonical record model decided first | §5 is decided before any transport appears in the document, and no transport DTO exists anywhere: the wire form in `engine/remote` is *generated from* `record.Record`, never the reverse. |
| **R3** one end-to-end path before breadth | §23's build order is one source, one sink, one record, `kill -9`, resume — before discovery, before chunking, before the control API, before the UI. §19.1 is that milestone written as a trace. |
| **R4** an acknowledgement means durable | `Writer.Write` returning `nil` **is** the acknowledgement and the core advances only on it, so violating R4 requires a sink to lie about a return value. `Buffer.Durable()` decides where the boundary sits and the core wires the tracker from it, so the documented advance rule and the actual durability are the same expression. §18.4 shows a buffered writer that structurally cannot commit against unsynced bytes. |
| **R5** dedupe keys scoped, committed after the write | §13.8: the key is `(tenant, pipeline, source, split)` + record key, built by the core so a narrower one is unconstructible. `DedupeState` lives inside the `Checkpoint`, so it commits in the same transaction as the cursors it derives from. For ordered splits "duplicate" *literally* means "at or before the committed cursor". A duplicate is reported as **success** (`WriteResult.Duplicates`), so a retry of the same id can never be lost behind a healthy-looking response. The window is configured and documented, not emergent from a FIFO's capacity. The store is injected, never module-scope. |
| **R6** bounded buffer with a rejection path | §12: every edge is a bounded channel, so unbounded growth is inexpressible. **One** `Buffer` interface with `PushOutcome ∈ {Accepted, Rejected, Overflowed, DroppedNewest}`, mandatory `Depth`/`Bytes`, and mandatory `Trim`. Drops are counted and always the newest. |
| **R7** name the failure shape with the success shape | `WriteResult.Failed []RecordFailure` and `CommitOutcome.Disposition` (five values) are defined in the same code blocks as their success paths, and the four `(res, err)` combinations are enumerated in `Write`'s doc comment. `Report.Failed`, `SplitFailure` and `[]Diagnostic` do the same for planning and configuration. |
| **R8** conformance asserted against real responses | Read-model handlers are round-tripped through the published JSON Schema in tests; every list field is non-nil by constructor so the `null`-vs-`[]` bug is unreachable; all shared enums are generated by `go generate` from one Go definition into TypeScript and the i18n key namespace; `internal/archtest` enforces the dependency graph. |
| **R9** one concept, one vocabulary | §2 is the vocabulary table. `Position` and `Key` are distinct types with **no** mapping function between them. `Phase` (plan) and `RunPhase` (operational) are deliberately different concepts with different names. `Support`, `Class`, `Op`, `Disposition` each have exactly one definition. The R9 test — "a function mapping between two representations of the same concept is evidence of a modelling error" — is applied as a review checklist item. |
| **R10** scaffolding labelled and tested against what it stands in for | `stores/mem` and the `canal.PollSource` sugar are the only scaffolding, both exercised by the same conformance suite as the real thing: `stores/mem` runs the identical store conformance tests as `bolt` and `pg` (including fenced-write rejection), and the poll-source adapter is tested through the full `Source`/`Enumerator`/`Reader` conformance suite, not a reduced one. `store.SingleNode` still carries a real `Fence`, so the fenced-write path cannot rot in dev. |
| **R11** one server-side runtime | One Go binary: `canal run | serve | worker | validate | offsets`. TypeScript exists only in the browser and its enums are generated from Go. |
| **R12** normative or draft, pick one | This document is marked draft in its first line and cites nothing as MUST. |
| **R13** state implying persistence is persistent | Every store returns values, never pointers into state. `PipelineRef` carries `TenantID` so an unscoped key is unconstructible. `Principal` and `Scope` exist at every ingress before the first multi-tenant field. Nothing returns a UUID and a `createdAt` from a map. |

### 21.2 The nine open decisions

**1. Buffer durability model — WAL vs segment-per-partition, and whether checkpoint state shares that WAL.**
**Segment-per-split**, and the checkpoint stays **independent**. Segment-per-split because the split is already the
ordering scope and the in-flight accounting unit, so trim-after-commit is a per-split truncation at a known
sequence rather than a compaction over an interleaved log. Independent because `Buffer.Durable()` makes the
acknowledgement boundary explicit and the checkpoint is *always strictly downstream* of it: a durable buffer takes
prefix ownership on `Push`, a memory buffer passes it through to the sink. Crash consistency therefore does not
require a shared WAL — it requires knowing which side of the boundary you are on, which is one bool. Segments are
named `<split>/<lo-seq>-<hi-seq>` and a torn tail segment is truncated at the last complete record on open.

**2. Dedupe key scope and backing store.** Scope `(TenantID, PipelineRef, source name, SplitID)` + record key,
constructed only by the core. Store: the `CheckpointStore`, whose `Commit` is atomic and fenced — explicitly not a
cache, since Benthos's `Cache.Add` documents that returning nil on duplicates is acceptable, which makes the one
primitive that could underpin exactly-once optional. And for **ordered splits the set does not exist at all**: a
position comparison against the committed cursor is the whole mechanism (§13.8).

**3. Backpressure signalling, including what a sender is told when the buffer is full.** `PushOutcome`, four
values. `block` propagates transitively through bounded channels; `reject` returns `PushRejected`, which the
upstream stage surfaces as `ClassTransientInternal` with a `RetryAfter`, so an HTTP ingress replies `503` with
`Retry-After` and a pull source simply stops pulling; `overflow` chains to the next tier; `drop_newest` counts and
sets `Degraded`. There is no configuration under which records are silently lost.

**4. The per-event partial-failure response shape.** `WriteResult{Failed []RecordFailure, Duplicates []RecordID,
Written, Bytes}` with the four `(res, err)` combinations enumerated in the doc comment, plus `CommitOutcome` with
five dispositions for the 2PC path, plus `[]cfg.Diagnostic` for config and `[]SplitFailure` for planning. Every one
correlates by a stable id, never by position.

**5. The canonical record model.** §5. `{Payload, Meta, Change?, Schema?, origin, id, err, ObservedAt}`.

**6. Tenancy and authentication at every ingress.** §16.5. `TenantID` inside `PipelineRef` so store keys are
unconstructible without it; `Principal{Tenant, Subject, Scopes}` resolved at the control API (bearer or mTLS), the
worker gRPC API (mTLS) and the CLI; single-tenant is `"default"`, never absent; `ScopeReadRecords` separate from
`ScopeReadPipelines`; `ScopeEditCheckpoints` separate from `ScopeReadCheckpoints`; the metrics endpoint serves no
per-record data and has its own weaker requirement so nobody disables auth to make Prometheus work.

**7. A connector state machine with a last-error surface, and the API fields exposing it.** `RunPhase` (8 values,
including `RunCompleted`) + `Condition[]` (6 types × `TriState`, each with `Reason`, `Message`,
`LastTransitionTime`, `ObservedGeneration`) + `StoppingSince`/`DrainDeadline` with *drained* and *drain-timeout* as
distinct events. `healthy → degraded → paused → terminal` is expressible and strictly more informative:
`Degraded: True` with a reason and the last error's operator-facing text, escalating to `RunPaused` after a
configured duration, `RunFailed` on a terminal class. `Connected: True` cannot imply `Progressing: True`, and a
fixture test asserts it.

**8. Clock-skew policy: clamp or reject, and where it is configured.** §12.5. Configured **once, on the pipeline**,
as `SkewPolicy{Mode ∈ {Clamp, Reject, Pass}, Max}` with `Clamp` and 5 minutes as defaults. Applied at ingest
against `Record.ObservedAt` from the injected `Clock`. Clamping preserves the original under a core-owned metadata
key and records the magnitude; `Reject` dead-letters with `ClassClockSkew`; `Pass` omits the lag series rather than
emitting a negative. Records *behind* `ObservedAt` are lag, not skew, and are never clamped.

**9. Checkpoint state format compatibility across binary upgrades.** Three mechanisms:
(i) every connector-authored blob is `VersionedBlob` and `Deserialize` is handed the version it was written with,
with a documented obligation to accept every version that build has written or raise `ClassPermanentContract`
naming the version; (ii) `CheckpointHeader.FormatVersion` versions canal's own envelope, evolved by
**append-only field addition**, and a core reading an unknown `FormatVersion` **refuses to start** with a named
error rather than partially parsing; (iii) `CheckpointHeader.Connectors` records `name@version` for the source,
sink and every codec, so a restore under a different build is visible to an operator and an incompatible one is
refused unless the new connector declares `AdoptsStateOf`. Exactly one snapshot format, always self-contained and
relocatable — no aligned/unaligned/native/canonical matrix to create sharp edges in.

---

## 22. Honest weaknesses

### 22.1 The trivial-source sugar leaks in three places

§18 makes an HTTP-poll source thirty-five lines, but the abstraction is visible at the seams:

- **Error messages name splits.** A `PollSource` author who misconfigures something sees
  `split http_poll/change/0: ...` in logs and in the UI. That is a concept they never wrote.
- **The UI shows a one-row split table.** Honest, but noise for a single-split connector, and it invites the
  question "why is there a split table at all?".
- **`canal offsets` shows a `Split` structure** for something the author thinks of as a cursor string.

None of these is fatal, and each is fixable with presentation (collapse a single-split plan into a "cursor" card).
But the concept genuinely does leak upward, and the rival "ack-graph" and "protocol-first" angles do not have this
problem because they have no split concept for a trivial source to leak.

### 22.2 The enumerator is a stateful component that itself needs a state machine, and connector authors will get it wrong

A real chunked-snapshot enumerator has to track discovery progress, splitter cursors, per-stream completion,
handoff gating and newly-added streams. Flink CDC's equivalent is a five-state FSM with seven predicate helpers and
illegal transitions that throw. This proposal moves the *hard* parts into core (the chunking engine, the handoff
gate, reconciliation, `Reconcile` as a pure function), but a connector author writing a *bespoke* enumerator — one
that plans splits the core's chunking engine cannot — is writing a small distributed-systems component and is going
to get it wrong.

Mitigations that do not fully solve it: `EnumeratorContext.Go` removes the off-thread mutation class; `Poll`'s
three-valued return makes "when do I get called again" explicit; the conformance suite includes a crash-injection
harness that restores an enumerator at every checkpoint boundary and asserts no split is lost or duplicated.
Residual risk: a connector author who writes `Poll` as "recompute my whole plan and return it" will produce
duplicate `SplitID`s, and while the core rejects those with `ClassPermanentContract`, they will only find out at
runtime.

### 22.3 Collapsing two-level checkpoint state into "how many splits you plan" is a real loss for one case

§6.4 argues the collapse. The case it loses: a source whose upstream is **one** log (so one cursor) but which wants
**per-stream** progress reporting. In Airbyte's two-level model you could show per-stream `stream_state` even when
commits were global. Here you get one cursor and per-stream *counters* from the read model, but no per-stream
position.

I believe the honest answer is that per-stream positions over a shared log are a lie — resetting one resets all —
and that showing one cursor with N stream labels is better than showing N cursors that must move together. But it
is a genuine regression in reporting fidelity for a real, common shape (Postgres logical replication over 40
tables), and an operator who wants "how far behind is table X specifically" cannot have it.

### 22.4 `ord.Order` puts a real burden on connector authors, and a wrong implementation fails silently-ish

Making comparability data rather than a method is the design's best wire-portability move, and it asks connector
authors to write an **order-preserving byte encoding**. That is easy to get subtly wrong: signed integers naively
big-endian-encoded sort negatives above positives; floats need bit manipulation; multi-column keys need
length-prefixing or a terminator to avoid prefix collisions; strings need a collation decision.

The consequences of getting it wrong are bad and not immediately obvious: the chunk filter suppresses records it
should emit (silent data loss) or emits records it should suppress (duplicates, which are harmless only if the sink
upserts). Mitigations: `ord` ships correct encoders for every Go primitive and for tuples, the conformance suite
includes a property test asserting `Compare` agrees with a connector-supplied reference comparator over 10,000
random values, and `Chunkable` is refused at registration without `ComparableKeys`. But a connector that ships its
own encoder and passes a weak property test can still be wrong in production, and unlike a comparison *method* — which
you can at least unit-test against the source's own ordering — an encoding bug is a data bug.

### 22.5 "Briefly stop admitting records" is a latency spike, and its cost is proportional to worker count

§13.4 replaces barriers with a stop-the-world checkpoint. At one process and four readers this is sub-millisecond.
At 24 readers across 6 pods it is one round of parallel RPCs to collect `Reader.Snapshot`, plus a Postgres
transaction — call it 20–80ms, during which no records are admitted. At a 10-second checkpoint interval that is
under 1% throughput loss, which is acceptable. At a 1-second interval driven by an enumerator requesting a
checkpoint per chunk completion during a 320-chunk snapshot, it is not.

This is a real scaling ceiling that Flink spent FLIP-76 and buffer debloating removing. The design's answer is a
minimum checkpoint interval coalescing enumerator requests, plus the *claim* that the barrier-shaped interface lets
a real barrier protocol be inserted later with no connector change. That claim is untested — and inserting a
barrier protocol under `Reader.Fetch` returning batches would require records to carry a checkpoint id, which is a
change to the record type even if not to the interfaces.

### 22.6 The chunked-snapshot engine is a large amount of core machinery justified only by genericity

§13.7 is the biggest single piece of core code in this proposal, and its value depends entirely on being reused. If
canal ships and only one or two sources ever declare `Chunkable`, the engine will have cost more than N per-connector
implementations would have — and it will be *harder* to debug, because the failure is split between core generic
code and connector-specific `ReadRange`/`CurrentPosition` behaviour, with the interesting state (`Completion`
watermarks, filter retirement) in core where the connector author cannot see it.

The counter-evidence is strong (Benthos proves per-connector chunking is built badly N times) but it is
counter-evidence about *other* projects' connector counts, not canal's.

### 22.7 The closed predicate vocabulary will be outgrown

§11.1 ships six predicate operators instead of an expression language. Benthos's Bloblang makes lint rules, batch
predicates, templated partition keys and Segment-style default mappings all browser-evaluable data with one
mechanism. The sink field-mapping vocabulary in §11.6 is a *seventh* small closed language, and `BatchPolicy.Until`
reuses `Predicate` for a purpose it was not designed for.

That is three closed vocabularies where one language would do, and the pressure to add operators will be constant.
The decision space names this as an uncosted decision with reach into D10, D12 and D13, and this proposal does not
cost it either — it defers it, which means the migration (if it happens) will have to keep the closed vocabulary
working forever.

### 22.8 Five required methods on `Source` is more than the three-verb ideal

`Discover`, `NewEnumerator`, `RestoreEnumerator`, `NewReader`, `Close`. Plus `Enumerator`'s four and `Reader`'s five:
**fourteen methods** before a connector author writes any business logic, against Benthos's three per role. The
sugar in §18 hides them for trivial sources, and none of the fourteen is on a per-record hot path, but a reviewer
who counts methods will conclude this is the heaviest of the four angles. That conclusion is correct.

Two of the fourteen are defensible only on the "different signatures prevent confusion" argument
(`NewEnumerator`/`RestoreEnumerator`), and one — `Revoke` — is only needed for graceful drain and rebalancing, which
a v1 could arguably do by `Close` plus checkpoint recovery. A leaner variant is possible and this proposal did not
take it, because both were chosen for correctness reasons that only show up under failure.

### 22.9 `Split.Group` is an under-specified escape hatch

Co-location by `GroupID` is how a per-chunk backfill lands on the reader that read the chunk and how splits sharing
a connection stay together. It is one `uint32` with a "same group ⇒ same reader" contract, and it is doing a lot of
work with very little specification: nothing says what happens when a group is larger than
`MaxSplitsPerReader`, whether a group can span streams, or whether group membership can change. Those are all
answerable, and none is answered here. It is the field most likely to acquire a second meaning and violate R9.

---

## 23. Build order (R3: one end-to-end path before any breadth)

1. `opt`, `ord`, `blob`, `fail`, `record` — types and their tests. No I/O.
2. `split` (`Split`, `SplitID`, `PlanState`, `PhaseOf`, `Reconcile`) and `store` interfaces with `stores/mem`.
3. `connector` interfaces + `registry` with the capability cross-check. Two connectors: `mem_source` (one split,
   N records) and `discard_sink`. Nothing real yet.
4. `engine`: placer, reader host, sink host, `prefix.Tracker`, checkpointer. `stores/bolt`.
   **Milestone: one record from `mem_source` to a `jsonl_file` sink, durably, checkpointed, surviving `kill -9`.**
   Nothing else ships before this passes. This is the point at which the abandoned attempt had four services and
   twelve documents and an in-memory slice.
5. `cfg` spec + the composite fields + two-tier validation. `cmd/canal run`.
6. One real source and one real sink, chosen so the source has a change log and a splittable key space — because
   both halves of the model must be exercised by something real before the generic engines are written.
7. Codecs: `json`, `newline`, `gzip`. `Decoder`/`Encoder`/`Framer` plumbed.
8. `Discover` + catalog + per-stream modes. The chunked-snapshot engine. `SupportsCommitter`.
9. `control` HTTP API + read model + status. Only now the UI, which has something true to display.
10. `stores/pg`, `Coordinator`, `engine/remote`. `cmd/canal serve|worker`.
11. Buffers, transforms, meta-components (`broker`, `switch`, `retry`, `fallback`), drift policy.

Steps 9 and 10 are deliberately late and step 4 is deliberately a hard gate.

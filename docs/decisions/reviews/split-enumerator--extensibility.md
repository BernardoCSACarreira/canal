# Review — `split-enumerator.md`, judged on EXTENSIBILITY

**Reviewer lens:** extensibility only. The question is whether *"to add a source or a sink, implement the
interface, register it, done"* survives contact with the interface set as actually written in
`docs/decisions/proposals/split-enumerator.md`, and whether the design can grow a new capability, a new kind of
component, and a v2 of the core without touching or breaking connectors.

**Score: 6.5 / 10.**

**Verdict in one paragraph.** The split abstraction earns its central claim on the *source* side, and it does so
in a way no rival angle can: because a reader's entire durable state is `[]Split` and every hot method is
`(ctx, value) → (value, error)`, `engine/remote` can implement `connector.Reader` rather than the design having
to invent a `Transport` interface — the cleanest discharge of constraint #3 in any of the surveyed systems, and a
consequence of the angle rather than an addition to it. The source-side upgrade path (§18.3: one split becomes N
splits, zero core edits, existing checkpoint still readable) is a *demonstrated* extensibility property, not an
asserted one. Against that, the sink half of the primary claim is unfinished: there is no interface by which an
encoded, framed, compressed payload reaches `Writer.Write(ctx, record.Batch)`, so D2's "N codecs × M connectors
never multiplies" is unrepresentable in the interface set that is supposed to deliver it, and fixing it changes a
required interface. The flagship optional capability, `SupportsChunkedSnapshot`, bundles a planner method and a
reader method into one interface that in coordinated mode is split across two processes and that the
registration cross-check cannot verify. And the growth machinery is unevenly applied: capability declaration,
APIVersion handshake and submit-time refusal exist for 2 of the 7 registered plugin kinds, so "add a third kind
of thing" gets none of the safety the whole argument rests on.

---

## 1. What the angle buys, stated first so the defects can be weighed against it

These are not concessions. They are the reasons the score is 6.5 and not 4.

**1.1 The out-of-process seam is not an abstraction.** §16.2 is the strongest single page in the document.
There is no `Transport`, no `WorkerClient`, no `EnumeratorProxy`. `engine/remote.remoteReader` *is a*
`connector.Reader`. The engine's placer, checkpointer and prefix trackers cannot tell local from remote. That
property is only available because the split model makes a reader's state a list of values and its hot path one
method returning a value — and it simultaneously answers both of constraint #3's futures (an out-of-process
*connector* and an out-of-process *worker*) with the same three types. Every rival shape has to name the seam;
this one does not. This is the single most transplantable idea in the proposal.

**1.2 Comparability as data, not as a method.** §4.3's `Ordinal{Bytes, Order, Scalar}` replaces Flink's
`Offset.isBefore/isAfter` and the decision space's proposed `PositionComparer` interface. The reasoning is
correct and load-bearing: a comparison *method* cannot cross a process boundary (trap 11) and cannot be served
to a browser, and the core genuinely needs to compare (chunk filter, `min(HIGH)`, progress fraction). Making it
an optional *field* with `(value, ok)` at every call site, and omitting the metric series rather than emitting
zero, is better than the decision space's own recommendation. §20/D3 is right to call it "the single change that
most improves the wire story".

**1.3 The registration cross-check is implementable, which is the contribution.** `RegisterSource[E Enumerator,
R Reader]` asserting `var e E; _, ok := any(e).(SupportsChunkedSnapshot)` — no instance, no reflection, because
method sets belong to types — is the mechanism that makes "capability = data, behaviour = interface" enforceable
rather than aspirational. Declaring `Chunkable: true` without the methods is a panic on the connector author's
first `go test ./...`. D9 asked for this and did not say how; this says how.

**1.4 `PhaseOf(PlanState)` is a function.** Phase cannot be set, so it cannot be switched on, so
`grep -r "case Phase"` returns only the renderer. That is trap 5 ("smuggling phase into the opaque checkpoint")
closed structurally rather than by discipline, and it is the cleanest instance of the design's general move:
turn a flag the core must trust into a property of data the core can observe.

**1.5 Cold and warm start are different methods.** `NewEnumerator` / `RestoreEnumerator` makes Benthos's
`if streamSnapshot && pos == nil` unrepresentable. Two methods where one plus a bool would do is normally a
smell; here it is the fix for the single most-repeated defect in the prior art (traps 4 and 5, two independent
occurrences each).

**1.6 The core-readable envelope around two opaque blobs.** `Split{ID, Range, Spec, Cursor, Watermark, Schema,
Group, Ordered, Estimated*}` gives the core everything it needs for placement, progress, filtering and reporting
*without opening `Spec`*. `Watermark` presence meaning "the scan finished" is completion-as-data. The
consequence is §19.4 step 5: a real progress document with a denominator, a fraction and an ETA, computed with
zero connector-specific code — which is the frontend goal met structurally. Connect cannot do this and the
dossier says exactly why.

**1.7 `Track(seq, cursor, n)`.** Absorbing fan-out, filter, 1→N and N→M into a descendant *count* deletes the
`AckFunc` closure while keeping Benthos's composability. Chain E's stated risk ("a richer ack breaks the wire
chain") is not mitigated, it is deleted. That is a genuinely better answer than the decision space's.

**1.8 Composite config fields plus an instantiation-free `Descriptor`.** §11.2 and §19.4 are the concrete
mechanism by which retry, batching, TLS, codec choice and buffering stop being per-connector code, and by which
the UI renders a connector it has never heard of without running any connector code. This is the part of the
"add a connector, done" claim that unambiguously holds.

---

## 2. Defects

### D-1 (fatal) — Encoded bytes have no path into `Writer.Write`. D2's composition claim is unrepresentable.

§8.4 states the contract: *"Sources implement transport only … sinks implement transport only (take an
already-encoded, already-framed, already-compressed payload and make a request)."* The interfaces as written:

```go
type Encoder interface { Encode(ctx context.Context, b record.Batch) ([]byte, error) }
type Writer  interface { Write(ctx context.Context, b record.Batch) (WriteResult, error) }
```

`Encode` consumes a batch and produces **one** `[]byte`. `Write` consumes a **batch of records**. There is no
type in the design that carries "an encoded, framed, compressed payload" to a sink, and no method on `Writer`
that accepts one.

The three possible readings all fail:

- *The encoder rewrites each record's payload.* Then `Encode`'s signature is wrong — it returns one slice for
  the whole batch, not one per record — and the batch-shaped justification given at the type (*"some wire
  formats are batch-shaped (a JSON array, an Avro OCF block, a Parquet row group)"*) is precisely the case where
  per-record decomposition is impossible. A Parquet row group cannot be split back across records.
- *The sink encodes.* That is what §18.4 actually does: `fileWriter.Write` calls `r.Payload.AsBytes()` per
  record and appends `'\n'` — i.e. the reference trivial sink performs both encoding and framing itself,
  contradicting §8.4, duplicating the `newline` framer, and misattributing an encode failure to
  `StageWrite`/`ClassPermanentMapping` when it is a codec failure at `StageEncode`. §19.1 T6 makes the
  contradiction explicit: *"json encoder → bytes; newline framer → bytes + `\n`; `Writer.Write(ctx, batch)`"* —
  bytes were produced and then a *batch* was passed, and the bytes went nowhere.
- *Only `SupportsStructuredInput` sinks receive records, everyone else receives bytes.* Then `Write`'s signature
  should be the byte form and the structured form should be the escape hatch, which is the inverse of what is
  written, and `SupportsStructuredInput` — declared as *"the narrow escape hatch"* — becomes the only path.

**Consequence.** The mechanism that is supposed to keep sinks small does not exist. Every sink either
re-implements encoding and framing (as the reference sink does), or `Writer.Write`'s signature changes — and
`Write` is one of the four methods on the required, frozen sink interface. Fixing this after v1 is exactly the
"add/change a method on a required interface" trap (trap 9) that the proposal spends §9 and §21/D9 forbidding.
For a review whose criterion is "adding a sink is: declare fields, implement `Write`, register", this is the
load-bearing hole.

*At fault:* `connector.Encoder.Encode(ctx, record.Batch) ([]byte, error)` against
`connector.Writer.Write(ctx, record.Batch) (WriteResult, error)`.

---

### D-2 (major) — `SupportsChunkedSnapshot` is one interface spanning two roles, two lifecycles and, in coordinated mode, two processes; and the registration cross-check cannot see the type that would implement it.

```go
type SupportsChunkedSnapshot interface {
    SplitKeySpace(ctx, s schema.StreamID, after opt.Opt[blob.VersionedBlob], max int) (ChunkPage, error)
    ReadRange(ctx, s split.Split, want int) (SplitBatch, error)
}
```

§19.2 Phase 1: *"`Enumerator.Poll` calls `SupportsChunkedSnapshot.SplitKeySpace(...)`"*. §19.2 Phase 2: the
reader calls `ReadRange`. §16 puts the enumerator in whichever process won `CampaignPlanner` and the readers in
worker processes. So the two methods of one interface are invoked by two different objects with different
lifecycles (`Enumerator` is singleton-per-generation and single-goroutine and *"must return promptly"*; `Reader`
is per-worker and holds connections) living in two different processes.

§10.2 compounds it: the panic fires when *"a declared capability has no corresponding interface implementation
**on E or R**"*. `Chunkable: true` therefore requires the Enumerator type **or** the Reader type to implement
*both* methods. Neither placement is sound: an Enumerator that implements `ReadRange` does bulk data reads on
the planner process from a method the core calls with a deadline; a Reader that implements `SplitKeySpace` is
asked to plan work it does not own, on a process that has no planner lease. §9.2's own hedge — *"it is separate
from `Fetch` only conceptually — a source implements `Fetch` by dispatching on the assigned split's Kind"* —
concedes that `ReadRange` is not really called, which then contradicts §19.2 where the core calls it directly.

This is FLIP-372's named cause of `TwoPhaseCommittingSink`'s inability to evolve (interface coupling), and the
proposal cites it as a reason for its own rules in §9.4.

**Consequence.** The capability that justifies the largest piece of core machinery in the proposal (§13.7, and
the reason the split model is claimed to pay for itself) is unimplementable as declared, and the
registration-time panic — the design's stated contribution — cannot verify the one capability whose
misdeclaration causes silent data loss. Splitting it later into a planner-side and a reader-side interface
changes `SourceCapabilities.Chunkable`'s meaning, the `resolve` function, and every chunkable connector's
registration.

*At fault:* `connector.SupportsChunkedSnapshot` and the `E or R` cross-check in `registry.RegisterSource`.

---

### D-3 (major) — The reverse registration check makes a core upgrade break third-party connectors that did not change.

§10.2: `RegisterSource` panics if *"an implemented optional interface is not declared (the reverse check —
silently-unused capabilities are as bad as lying ones)"*.

Go interface satisfaction is structural. The design already ships ~20 optional interfaces with method names that
are not distinctive: `SplitBatch(b record.Batch) []record.Batch`, `Backlog(ctx, schema.StreamID) (Backlog,
error)`, `Choices(ctx, string, cfg.Config) ([]cfg.Choice, error)`, `PartitionKey(record.Record) (string,
error)`, `AdoptsStateOf() []string`, `StructuredInput()`, `AppliesKinds() []schema.ChangeKind`. Note that
`SupportsBatchSplit.SplitBatch` collides in name with the *type* `SplitBatch`, and `SupportsWriterState.
SnapshotWriter` sits alongside `Reader.Snapshot` — this is a namespace where accidental satisfaction is likely,
not hypothetical.

**Consequence.** When canal v2 adds an optional interface, any existing connector whose type happens to satisfy
it **panics at `init()`** in a binary that compiled cleanly, with no change on the connector's side. The
connector author's only remedy is to rename their own method. The mechanism designed to make capability growth
safe is itself the mechanism by which capability growth breaks connectors — the precise failure mode the brief
asks about ("what happens when the core needs a new method in v2"). The forward check (declared-without-methods)
is right and cheap; the reverse check should be a `go vet`-style lint or a `WARN` in the descriptor, never a
panic, and the proposal gives no way to opt out of it.

*At fault:* the reverse-direction panic in `registry.RegisterSource` / `RegisterSink`.

---

### D-4 (major) — `Origin.Seq` has no durable allocator, so `record.Ref` is not unique within a generation once a split is reassigned.

§5.1: *"`Seq` is monotonic within `Split` across the whole life of the pipeline generation, assigned by the core
as it drains `SplitBatch`es. It is what the prefix tracker keys on."* §5.1 also declares
`Ref{Pipeline, Generation, Split, Seq}` to be *"the durable, cross-restart identity of a record, used by the
DLQ, the audit log and the live tap"*, and §13.9 makes `(Split, Seq)` idempotency layer 3, *"which exists for
every record from every source unconditionally"*.

Nothing persists a per-split `Seq` high-water mark. `Split` has no such field; `WorkerState` holds `[]Split`;
`prefix.Tracker` is per-split and process-local. §19.5 then reassigns splits to *different workers in different
processes* mid-generation ("worker loss… re-placed on other workers and resume mid-split"), and §19.2 Phase 4
does the same after a crash. The new host cannot derive the old high-water mark: the cursor is an opaque
`VersionedBlob`.

**Consequence.** After any reassignment, records get `Seq` values already used within the same generation for
different records. Every consumer of `Ref` degrades: DLQ entries collide, the audit log is ambiguous, live-tap
correlation is wrong, and idempotency layer 3 — the layer that is supposed to exist unconditionally for every
source — silently stops being an identity. This is invisible until someone diffs a DLQ. Extensibility relevance:
the fix is a field on the persisted `Split` (a checkpoint-format change plus a rule the core must maintain), and
until then every connector inherits a broken durable identity it did nothing wrong to get.

A second, smaller instance of the same conflation: §8.4 says decoded children *share their parent's* `Origin`,
so N records from one frame share one `(Split, Seq)`. Per-record DLQ targeting — the property §5.1 claims
`RecordID` unlocks — therefore works in flight and not durably.

*At fault:* `record.Origin.Seq` and `record.Ref`, against `Report{Returned}` / re-placement in §16.4.

---

### D-5 (major) — The wire-shippability invariant is asserted globally and violated by six of the design's own interfaces and by every `Context`.

§1 fact 4: *"Nothing crossing a boundary is a closure. Every method is `(ctx, serialisable) → (serialisable,
error)`. There is no `AckFunc` anywhere."* §16.2 restates it as the property that Connect fails on five counts.

Counter-examples in the proposal's own code:

| Interface / method | Violation |
|---|---|
| `SupportsStructuredInput.StructuredInput()` | no ctx, no args, no return — meaningless over a wire |
| `AdoptsStateOf() []string` | no ctx, no error |
| `SupportsSchemaApply.AppliesKinds() []schema.ChangeKind` | no ctx, no error |
| `SupportsPartitionedBatching.PartitionKey(record.Record) (string, error)` | no ctx, and **per record** |
| `SupportsBatchSplit.SplitBatch(record.Batch) []record.Batch` | no ctx, no error, per batch |
| `Framer.Scan(data []byte, atEOF bool) (int, []byte, error)` | no ctx; per frame |
| `ConnectionTestable.ConnectionTest(ctx) []TestResult` | no error |
| `EnumeratorContext.Go(fn func(ctx) (EnumeratorEvent, error))` | **takes a closure** |
| `Log() *slog.Logger`, `Metrics() Metrics`, `Clock() Clock`, `Secrets() SecretResolver`, `Framer() Framer` | behavioural objects, not values |
| `SourceContext.HTTPClient()` (§18.1) | returns a `*http.Client`; also a transport-specific method on a generic context |

Two of these are not merely inelegant. `PartitionKey` and `Framer.Scan` are per-record and per-frame; for
`engine/remote` they are a round trip per record — the design's own discipline is that *"`Fetch` is the one
hot-path method in the entire plugin surface"*, and two optional capabilities break it. And
`ReaderContext.Framer()` is structurally impossible out-of-process: the framer is a *core-registered component*
living in the core process, and a remote reader that loops on `Framer().Scan` cannot be handed one.

**Consequence.** Constraint #3 holds for `Enumerator`, `Reader` and `Writer` and fails for the capability set
and for all four `Context` types. An out-of-process connector therefore needs a hand-written plugin-side SDK
that re-implements the contexts and re-specifies the capabilities — which is exactly how Benthos's declarative
and Go vocabularies diverged (its out-of-process plugins lost `Secret`, `Optional`, `Examples`, nesting and lint
rules), the failure the proposal cites in §11.1 as the reason the declarative form must be primary. The claim
"the proto could be generated from the Go interfaces" is true of four interfaces and false of the rest.

Aggravating: `SourceContext`, `SinkContext` and `WriterContext` are used in code (§18.1, §18.4, §13.5) and never
specified. The contexts are declared to be *the* growth path for core capabilities (§7.4); two of five are
defined.

*At fault:* the `(ctx, serialisable) → (serialisable, error)` claim in §1/§16.2 versus
`SupportsPartitionedBatching`, `SupportsBatchSplit`, `Framer`, `SupportsStructuredInput`, `AdoptsStateOf`,
`EnumeratorContext.Go` and `ReaderContext.Framer`.

---

### D-6 (major) — The capability, handshake and submit-time-refusal machinery covers 2 of 7 registered plugin kinds. Adding a third kind of thing gets none of the design's safety.

§10.2 lists `RegisterSource`, `RegisterSink`, `RegisterCodec`, `RegisterFramer`, `RegisterCompressor`,
`RegisterTransform`, `RegisterBuffer` and says the last five are *"the same shape"*. They are not shown, and
there is no `TransformCapabilities`, `BufferCapabilities` or `CodecCapabilities` anywhere in the document. Only
`SourceCapabilities` and `SinkCapabilities` carry `APIVersion`. §10.3's thirteen submit-time checks reason about
sources, sinks and codec `Accepts()`/`Requires()` — and about nothing else.

Concrete holes this leaves, all of which the design's own invariants depend on:

- `Split.Ordered` is what *licenses mid-split checkpointing*. A transform configured at
  `transforms.concurrency > 1` reorders records within a split (§15.4 admits it, in prose, at the config field).
  Nothing declares that a transform preserves order and nothing refuses the combination at submit time — so the
  cursor-commit correctness property is protected by a UI warning.
- A transform that drops or rewrites the `Change` facet silently invalidates a `DestUpsert` sink and a chunked
  snapshot's key filter. §10.3 checks the *source's* `ChangeFacet` against the sink's `Modes` and never
  re-checks after the transform chain.
- `StatefulTransform`'s participation in the checkpoint is undeclared, so the core cannot know at submit time
  whether the pipeline's guarantee tier is achievable.
- A `Buffer` with `Durable() == true` moves the acknowledgement boundary (§12.3) — the single most
  safety-critical property in the design — and it is a runtime method call, not declared data, so it cannot be
  cross-checked at registration and cannot be shown in a descriptor.

**Consequence.** The design's central safety argument is *"an impossible pipeline is refused at submit time"*.
That argument is available for two component kinds. Extending it to the other five is per-kind core surgery: a
new capability struct, a new `RegisterX` generic, a new `Descriptor.Kind` string, a new `cfg.ComponentKind` enum
value, new checks in `engine.Validate`, a new `resolve` variant. So the answer to "does adding a third kind of
thing require core surgery" is: adding a *component* of an existing kind is free; adding a *kind* costs six core
edits, and the five kinds that already exist have been declared free by assertion rather than by construction.

*At fault:* `registry.RegisterCodec/RegisterFramer/RegisterCompressor/RegisterTransform/RegisterBuffer` having no
capability struct, and `engine.Validate`'s check table.

---

### D-7 (major) — Committing and schema-applying are pipeline-singleton roles typed as per-`Writer` methods, and `RegisterSink[W Writer]`'s single type parameter makes separating them a breaking change to every sink.

`SupportsCommitter` (`PrepareCommit`, `Commit`, `CloseCommitter`) and `SupportsSchemaApply` (`AppliesKinds`,
`ApplySchemaChange`) are cross-checked against `W` in `RegisterSink[W connector.Writer](s SinkSpec[W])`, so both
are method sets on the *writer* type. But:

- `SinkCapabilities.MaxInFlight` implies N concurrent writers in one process, and §16 puts writers on six worker
  processes. §13.4 step 10 says *"if `SupportsCommitter`: `Commit(ctx, requests for every id ≤ this one)`"* —
  singular, with no statement of which writer instance receives it.
- Committables minted by writer A (`PrepareCommit`) may only be committable through A's session — an open
  transaction handle is exactly what `Committable` being `(version, bytes)` is designed to avoid, but a temp
  table or a multipart upload id may still be scoped to a connection.
- §11.5 step 4 calls `ApplySchemaChange(ctx, change)` once, after a global quiesce. Six writers each
  implementing it means either six concurrent `ALTER`s or an unstated leader-among-writers rule.

**Consequence.** The correct model is Flink's: a `Committer` and a schema applier are separate, singleton
components with their own lifecycle. Introducing them later means `RegisterSink[W, C]` — and Go has no default
type parameters, so **every existing sink registration breaks**. The alternative, a second function
(`RegisterSinkWithCommitter`), is the vocabulary sprawl R9 exists to prevent. This is FLIP-191's *"there is not
an easy way to replace/remove the `GlobalCommitter` … due to the typed parameters"* reproduced one level up: the
type parameters are off the plugin interface (correctly) and onto the registration function, where their arity
is still a permanent coupling between the core and every connector's `init()`.

*At fault:* `registry.SinkSpec[W]` / `RegisterSink[W connector.Writer]` carrying `SupportsCommitter` and
`SupportsSchemaApply` on `W`.

---

### D-8 (major) — `ord.Order` and the source's own read order are two representations of one ordering with no cross-check, and the flagship capability is gated on them agreeing.

§22.4 admits the encoding burden honestly. The sharper problem it does not state: `ord.Order` is not merely
hard to write, it must **agree with a second ordering that lives in the source system**.

`SupportsChunkedSnapshot.ReadRange` reads a chunk in the source's order (`WHERE key > x AND key < y ORDER BY
key`, §19.2 Phase 5). `engine.ChunkFilter` decides emit-or-suppress using `bytes.Compare` over `Key.Order`
(§19.2 Phase 7). Correctness of the snapshot→stream handoff requires the two to induce the *same* total order.
They are authored independently: one by the source system's collation and type semantics, one by the connector's
encoder.

Real cases where they differ: a `text` primary key under any non-`C` collation (`ORDER BY` uses ICU/locale
rules; byte comparison does not); case-insensitive collations; `citext`; MySQL's `utf8mb4_general_ci`; a
`numeric` key whose decimal ordering does not match any fixed-width byte encoding; a composite key whose
components have different collations.

The proposed mitigation is circular: *"a property test asserting `Compare` agrees with a
connector-supplied reference comparator over 10,000 random values"*. The reference comparator is supplied by the
same author with the same misconception; it tests the encoder against itself, not against the database's
`ORDER BY`. Nothing in the design compares `ord.Order` to a live `SELECT … ORDER BY` from the source.

**Consequence.** The failure mode is the chunk filter suppressing change records it should emit — silent data
loss — on exactly the class of connector this design exists to serve (`Chunkable + Replayable +
ComparableKeys + ComparablePositions`). The extensibility statement becomes: *adding a chunkable source is
"implement two interfaces, register, and independently re-derive your source system's collation semantics as an
order-preserving byte encoding, correctly, for every key type your users may have"*. That is a materially
different claim from "implement the interface and register it, done", and it is the largest gap between the
proposal's headline and its content.

The available fix is cheap and absent: make the conformance suite require the connector to expose a
`SELECT … ORDER BY` sample and assert `Order` agrees with the *source's* ordering over real data, and refuse
`Chunkable` for a stream whose declared key type has no verified encoder.

*At fault:* `ord.Ordinal.Order`'s contract ("the same sign as the connector's own notion") versus
`SupportsChunkedSnapshot.ReadRange`'s reliance on the source system's ordering.

---

### D-9 (major) — `Position` and `Key` are declared non-interchangeable and are freely interchangeable, because `Ordinal` is embedded and exported.

```go
type Position struct{ Ordinal }
type Key      struct{ Ordinal }
```

§4.3 asserts: *"`Position` and `Key` are distinct named types over one mechanism. They are not interchangeable
and there is no function mapping one to the other — which is precisely the R9 test."* Embedding makes `.Ordinal`
that function, and it is exported. `Compare` and `Fraction` are promoted from / declared on `Ordinal`, so
`somePosition.Compare(someKey.Ordinal)` compiles, and `ord.Fraction(lo Ordinal, o Ordinal, hi Ordinal)` accepts
any mixture. The proposal's own filter code does exactly this reach-through:
`cmp, ok := pos.Compare(c.Watermark.Ordinal)` (§19.2 Phase 7).

**Consequence.** A key/position mix-up returns `(cmp, true)` — a confident, meaningless answer — on the
`ChunkFilter` path whose wrong answers are data loss or duplicates. R9's stated test is failed in the type
system while being claimed in prose. Fix is trivial (unexport the embedded field, or make `Compare` a method on
each named type taking that named type) and its absence is the kind of thing that hardens into a permanent wire
contract, as the proposal itself argues about Flink CDC's `wartermark`.

*At fault:* `ord.Position`/`ord.Key` embedding an exported `Ordinal`, plus `ord.Fraction(lo, o, hi Ordinal)`.

---

### D-10 (minor) — "A sink literally cannot get checkpointing wrong" is true only at tier 1.

§1 fact 3 and §8.2 make this the sink design's headline. At tier 3 it is false: `PrepareCommit` returns
`[]Committable` and `Committable.Splits []SplitID` is connector-populated. §8.3 explains why it must be —
*"so that a committable that ends in `DispositionDeadLetter` marks exactly the right splits degraded rather than
the whole pipeline"* — which means a 2PC sink author must correctly attribute staged artifacts to split ids or
the dead-letter blast radius and the prefix-holding behaviour are wrong. The sink has acquired progress
awareness, in the split vocabulary §2 says never means a shard of the sink.

**Consequence.** The claim should be scoped in the tier table, not stated globally, and `Committable.Splits`
should be core-derived from the `Origin`s of the records the writer was handed since the last `PrepareCommit` —
which the core already knows and the connector should not have to track.

---

### D-11 (minor) — There is no exported way to construct the three values a connector must be tested against.

`record.Record` has unexported `id`, `origin`, `err`, and provenance is stamped by the core at ingest. There is
no exported builder. `cfg.Config` is `struct{ /* unexported */ }` with no constructor. `SourceContext`,
`EnumeratorContext`, `ReaderContext`, `WriterContext` are interfaces with `Metrics`, `Clock`, `SecretResolver`
and `Framer` members, and no test doubles are offered.

**Consequence.** A sink that branches on `Origin.Snapshot` (which §5.1 tells sinks to do: *"Sinks use it to
choose insert-vs-upsert semantics"*), on `Origin.Split`, or on the `Change` facet cannot be table-tested,
because the input cannot be constructed. A connector's `Open(ctx, cfg.Config, sc)` cannot be unit-tested at all.
The unexported-provenance mechanism is right (it is what avoids KIP-793) and it needs a `recordtest` /
`connectortest` escape hatch that the proposal does not have. The "conformance suite" is invoked eight times
across §9, §10, §14, §21 and §22 as the answer to almost every residual risk and is never specified — for a
deliverable that is *the interface set*, the suite is part of the interface set.

---

### D-12 (minor) — Closed core enums contradict "adding a stage type is not a contract change", and a third-party component cannot name its own stage or metric.

§15.1: *"Adding a stage type is not a contract change."* But `fail.Stage` is a closed 13-value `uint8`
(`StagePlan … StageSchema`) used as a metric label in `canal_records_failed_total{stage,class}` and in every
`DeadLetter`, while `StageInfo.Kind` is an open `string`. That is one concept with two representations and an
implied mapping — the R9 test the proposal applies to everything else. A registered meta-component (`broker`,
`switch`, or a third party's) cannot appear in the stage label without a core edit, and its DLQ records must be
attributed to someone else's stage.

Same shape twice more: `Metrics.Role` is *"small, closed, and extended only in core"*, with `Custom` documented
as *"deliberately awkward"*; `cfg.Field.Component opt.Opt[ComponentKind]` is a closed enum of component kinds,
so a new kind widens a core enum and every form-renderer branch on it.

**Consequence.** These are defensible choices (label cardinality, R9 discipline) but they should be stated as
what they are: the *component* axis is open and the *vocabulary* axis is closed, and every new kind of thing
costs core edits in four enumerations. §15.1's sentence overclaims.

---

### D-13 (minor) — `SplitID.Seq` is a single namespace shared between the enumerator and the core, and "`Poll` is the only place splits are minted" is contradicted by the chunk engine.

§7.2: *"`Poll` [is] the only place splits are minted, which is what makes the enumerator's own state machine
testable as a pure transition table"*, and `SplitID.Seq` *"must never [be reused] within a pipeline generation,
because completions and dedup filters are keyed on it"*. §13.7 step 2 and §19.2 Phase 2 then have the *core*
mint a per-chunk backfill split: *"replay: a bounded `Change` split, same `Group`, `PositionRange{From: LOW,
To: HIGH}`"*.

Either horn is a defect. If that backfill split is in the plan, the core is minting `SplitID{Stream, Change,
Seq}` values in the same namespace as an enumerator that has no way to learn which `Seq`s were taken — so a
source that plans one `Change` split per stream *and* declares `Chunkable` collides, and `Enumeration.Splits`'
duplicate-id rejection (`ClassPermanentContract`) fires on a legitimate plan. If it is not in the plan, then
`Split.Group`'s primary stated justification — *"this is how a per-chunk backfill lands on the reader that read
the chunk"* — is vacuous, and `Group` reduces to the under-specified `uint32` §22.9 already flags (no statement
of behaviour when a group exceeds `MaxSplitsPerReader`, whether groups span streams, or whether membership can
change).

**Consequence.** A connector author cannot know which `SplitID.Seq` values are safe to allocate, and the
invariant that makes the enumerator a pure transition table is not actually held by the design.

---

### D-14 (minor) — `StatefulTransform` embeds `Transform`, violating the design's own no-nested-interfaces rule.

```go
type StatefulTransform interface {
    Transform
    Snapshot(ctx context.Context, id CheckpointID) (blob.VersionedBlob, error)
    Restore(ctx context.Context, state blob.VersionedBlob) error
}
```

§9.4 and §20/D9: *"never nest an interface inside another interface … FLIP-372 names inner-interface coupling as
what 'prevented the evolution of the `TwoPhaseCommittingSink`'."* This is the only place the rule is broken and
it is broken in the plugin surface. `Transform` cannot now be deprecated or re-shaped without dragging
`StatefulTransform`, which is the mechanism FLIP-372 describes.

---

## 3. Further defects, argued more briefly

**3.1 Two capability vocabularies for the same facts, with no cross-check and no precedence.**
`SourceCapabilities.{Chunkable, Replayable, ComparableKeys, ComparablePositions}` are per *connector* and
cross-checked at registration. `schema.Stream.{Chunkable, Resumable, SupportedModes}` are per *stream* and come
from `Discover`. §10.3 validates chunking against the connector struct and validates per-stream modes against
the stream fields. Nothing says which wins when `SourceCapabilities.Chunkable == false` and
`Stream.Chunkable == true` — and the registration cross-check cannot see streams, because its whole virtue is
that it instantiates nothing. A connector author must keep two declarations consistent by hand. R9.

**3.2 `Payload.AsBytes()` reaches ambient pipeline state from a leaf package.** §3 rule 1 makes `record` a leaf
importing no engine package. §5.2's doc comment says `AsBytes` encodes *"with the pipeline's configured codec"*.
The only ways to satisfy both are a hidden interface-typed field on `Payload` (a wire hazard, a copy cost, and a
closure crossing a boundary — all three forbidden elsewhere) or a package-level codec, which is Vector's global
mutable `log_schema` shape that §11.4 rejects *unconditionally*. As written, a sink author's
`r.Payload.AsBytes()` (as in §18.4) has behaviour that depends on configuration invisible at the call site and
untestable in isolation.

**3.3 `APIVersion` is either vacuous or too coarse.** In-process, a connector compiled against the current core
cannot have a stale `APIVersion` unless the author hardcodes an old integer, so the handshake does nothing.
Out-of-process, one integer covers ~20 interfaces and ~40 prose invariants, so the only available response to a
mismatch is refusal. Contrast `blob.Serializer`, which has per-blob versions for exactly this reason. There is
no per-capability version and no mechanism for one core to serve v1 and v2 semantics simultaneously.

**3.4 The one-import façade cannot exist under the declared toolchain.** §3 promises
`canal/ ⟵ façade: type-aliases the above so a connector author writes ONE import`, and §18.4 writes
`canal.RegisterSink[*fileWriter](canal.SinkSpec[*fileWriter]{…})`. `canal.SinkSpec[W]` requires a *generic type
alias*, which Go added in 1.24; `go.mod` declares 1.23.6. The alternatives are to require 1.24, or to
hand-mirror every `Spec` type in the façade and convert field by field — which is precisely the declarative-vs-
native vocabulary drift the proposal faults Benthos for in §11.1. Small, but it lands on the ergonomic claim
that makes "implement and register" feel cheap.

**3.5 `PlanView` pages the wrong field.** `Completions` is paged with `CompletionsTotal`/`CompletionsFrom` as
the pre-fix for Flink CDC's documented scar. `UnassignedIDs []SplitID` and `AssignedIDs []SplitID` are not, in a
design that expects 200k-split plans, and `Poll` is called *"again immediately"* on `PlanMore`. 200k `SplitID`
structs per Poll, each carrying a `StreamID` string, is the same scar in the same method. `Catalog` is likewise
shipped whole on every `Poll`.

**3.6 "`Snapshot(ctx, id)` on every stage" is not delivered.** §0 and §13.1 justify not building barriers by
promising the interface stays barrier-shaped: *"put `Snapshot(ctx, id)` on every stage so a real barrier protocol
could be inserted later with zero connector changes."* `Buffer`, `Transform`, `Encoder`, `Decoder`, `Framer` and
`Compressor` have no `Snapshot`. Adding it later adds a method to registered plugin interfaces — trap 9. §22.5
admits the barrier claim is untested; it is also not structurally prepared for.

**3.7 The required surface is fourteen methods (§22.8, admitted).** `Source` 5 + `Enumerator` 4 + `Reader` 5.
The `PollSource` sugar reduces it to one, and §18.2 correctly shows the sugar is forty lines of adapter over the
real model rather than a second model — which is the right answer and materially better than a special case. But
the sugar's ceiling is one split, and the moment a connector wants two, the author is writing an enumerator
state machine (§22.2, admitted). The honest characterisation of the extensibility claim is: *trivial* connectors
are one function; *scalable* connectors are a small distributed-systems component.

---

## 4. Where the claim actually stands

| Claim under test | Holds? |
|---|---|
| Core contains no per-connector knowledge | **Yes.** Registry by name, `Descriptor` served without instantiation, `archtest` forbids `control/ → connectors/`. Cleanest in the field. |
| No switch statements growing per connector | **Yes** in the core. `PhaseOf` as a function, boundedness as data, no pipeline-type enum. Note every chunkable *source* switches on `split.Kind` internally. |
| Add a source: implement, register, done | **Partly.** True for `PollSource` (35 lines). For a chunkable source: two interfaces, an enumerator state machine, and an order-preserving byte codec that must agree with the source's collation (D-8). |
| Add a sink: implement, register, done | **No.** D-1: encoded bytes have no path into `Write`, so the reference sink re-implements encoding and framing. Tier 3 additionally requires split attribution (D-10). |
| Add a third kind of thing | **Partly.** Codec, framer, compressor, transform and buffer kinds exist, which is more than any rival anticipates — but they have no capability declaration, no APIVersion and no submit-time validation (D-6), and a genuinely new kind costs six core edits. |
| A new capability without breaking connectors | **Mostly yes**, via optional interfaces + keyed capability structs + context growth. Two mechanical holes: the reverse registration panic (D-3) and `RegisterSink[W]`'s arity (D-7). |
| The same interface absorbs an out-of-process implementation | **Yes for the four hot interfaces** — and this is the proposal's best property (§16.2). **No for the capability set and the contexts** (D-5), and `ReaderContext.Framer()` is structurally impossible remotely. |
| A connector can be tested in isolation | **Shapes yes, plumbing no.** `Poll(ctx, PlanView) → Enumeration` is the most testable planner interface in the field; but `record.Record`, `cfg.Config` and the contexts have no exported construction path (D-11). |
| Interface versioning / v2 | **Best doctrine of the four angles** (freeze required interfaces, grow contexts, capability = data), **undermined by two mechanisms** (D-3, D-7) and by a single coarse `APIVersion` (3.3). |

## 5. Why 6.5

Against a rival that has no split concept, this proposal wins the parts of extensibility that cannot be
retrofitted: the process boundary as an ordinary plugin, comparability as portable data, the plan as the one
representation of work-and-ownership, and the source-side scale-up path from one split to N with zero core edits
and a still-readable checkpoint. Those are structural and they are exactly the properties Chain B and Chain E of
the decision space say cannot be deferred.

It loses points where the interface set has not caught up with the prose. D-1 is a hole at the centre of the
sink half of the primary criterion and its fix changes a frozen interface. D-2 makes the flagship capability
unimplementable as declared. D-3 and D-7 are the two places where growing the core breaks connectors, which is
the specific question this lens exists to ask. D-6 means the safety argument that justifies the whole
capability system covers less than a third of the plugin kinds. None of these is unfixable and none invalidates
the split bet; together they mean the interface set is not yet the deliverable it claims to be.

## 6. What to transplant even if this proposal loses

1. `engine/remote` implementing `connector.Reader`/`Enumerator`/`Writer` instead of defining a `Transport`
   interface — the out-of-process seam as an ordinary plugin, and the strongest available discharge of
   constraint #3.
2. Comparability and progress projection as *data* (`Ordinal.Order` / `Ordinal.Scalar`) rather than a
   `PositionComparer` method — the only form of that capability that crosses a process boundary or reaches a
   browser, and better than the decision space's own recommendation.
3. Registration-time cross-check of declared flags against method sets via `var e E` on a type parameter — no
   instance, no reflection, failing on the author's first `go test ./...`.
4. `PhaseOf(PlanState)` as a function rather than a stored field, so there is nothing to set and nothing to
   switch on.
5. Separate `NewEnumerator` / `RestoreEnumerator` constructors, which make `pos == nil` phase inference
   unrepresentable.
6. A core-readable envelope wrapping two connector-opaque blobs with *different lifetimes* (immutable `Spec`,
   mutable `Cursor`), with completion expressed as `Watermark` presence rather than a boolean.
7. `prefix.Tracker.Track(seq, cursor, n)` — descendant *count* as the fan-out/filter/rebatch mechanism, which
   deletes the ack closure while keeping arbitrary-topology composability.
8. Composite `(spec-fragment, extractor)` config field pairs plus a `Descriptor` served from the registry with
   nothing instantiated — the concrete mechanism that turns retry, batching, TLS and codec choice into
   configuration rather than per-connector code.

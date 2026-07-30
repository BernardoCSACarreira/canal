# canal — the decision space

**Status:** draft, analytical. Not normative. This document does not decide anything; it enumerates the
independent architectural forks canal must choose between, the real options with attribution, and a
recommendation per fork with the reasoning shown. `docs/design-rules.md` (R1–R12) is the normative
constraint set; this file is subordinate to it.

**Evidence base:** the nine dossiers in `docs/research/`. Their verification status is wildly uneven and
that must colour how much weight each claim carries:

| Dossier | Primary source read? | Weight |
|---|---|---|
| `kafka-connect.md` | Yes — `apache/kafka@trunk`, every signature read from source | High |
| `benthos.md` | Yes — `redpanda-data/benthos@4ade611`, `connect@017e01f`, `Jeffail/checkpoint@main` | High |
| `flink-checkpointing.md` | Mostly — `release-1.20` files fetched; Flink CDC 3.x pipeline API from docs only | High for FLIP-27/191/372, medium for CDC 3.x |
| `observability-controlplane.md` | Partly — 7 sources verified (incl. FLIP-33, k8s leaderelection), rest recalled | Medium |
| `vector.md` | **No** — model recall, signatures tagged RECALL | Medium on architecture, low on names |
| `airbyte-singer.md` | **No** — model recall with per-claim confidence labels | Medium on structure, low on names |
| `sdk-design.md` | **No** — no signatures written at all | Low; conceptual only |
| `debezium.md` | **No** — nothing verified; it is a fetch manifest | Conceptual hypotheses only |
| `conduit.md` | **No** — empty by design | None |

Rule applied throughout: **architectural claims from unverified dossiers are used only where an independently
verified dossier corroborates the same structure.** Where a claim rests on one unverified source alone it is
marked `[thin]`. No identifier from an unverified dossier is proposed as a canal identifier.

Two dossiers are missing entirely and their absence biases this analysis in a knowable direction:

- **Conduit** is the closest prior art to canal that exists — Go, in-process interfaces *plus* a gRPC plugin
  boundary satisfying the same interface, opaque `Position`, single binary. It is the one system that has
  already solved constraint #3. Decisions 3, 8 and 13 below would all be sharpened by it. **Re-run that
  research before freezing the interface set.**
- **Debezium** verified nothing. Every "Debezium does X" below is a hypothesis. Where the same idea appears
  in Flink CDC (which *was* read), the Flink CDC citation is used instead and Debezium is dropped.

---

## Part 0 — Convergent wisdom (treat as near-axioms, not decisions)

These are places where every mature system either converged, or the one that dissented documented its
regret. Spending decision budget on them is waste.

1. **The connector plans, the runtime places.** The connector says how work divides; the core decides which
   process runs which piece; neither learns the other's algorithm. Kafka Connect (`taskConfigs(maxTasks)` +
   assignor) and Flink (`SplitEnumerator` + scheduler) invented this independently.
2. **Config is declared as data by the connector, and that data is simultaneously the UI schema.** Connect's
   `ConfigDef.ConfigKey` has fourteen fields of which five (`group`, `orderInGroup`, `width`, `displayName`,
   `importance`) are purely presentational. Benthos, Airbyte, Vector, Fivetran, Meltano, Segment all landed
   on the same shape. Nobody who shipped a usable connector UI did it any other way.
3. **Validation is two-tier:** cheap declarative/offline (per keystroke, no I/O) plus an overridable
   `Validate(ctx, config)` that may hit the network, returning **per-field diagnostics, all errors at once**
   (`List<ConfigValue>{value, errorMessages, recommendedValues, visible}`), never a fail-fast throw and never
   one bool + one string.
4. **Registration is an init-time registry keyed by name, and the registry is a value type with a default
   global instance.** Benthos `RegisterX`/`MustRegisterX` into `globalEnvironment`, plus
   `Environment.Clone/With/Without` for tests, sandboxing and allow-listing. Classpath/ServiceLoader scanning
   (Connect, still on `plugin.discovery=HYBRID_WARN` years into the migration) is a solved-by-avoidance
   problem. Vector's compile-time feature-flag registry is the same idea with worse ergonomics.
5. **The durability substrate is bytes-in/bytes-out and never sees a domain type.** Connect's
   `OffsetBackingStore` (`ByteBuffer` in, `ByteBuffer` out) is precisely what makes its standalone↔distributed
   swap free. Copy the shape; add atomicity across a multi-key write, which Connect's compacted topic cannot
   provide and which its own javadoc documents as producing unrecoverable states.
6. **Everything that crosses a role boundary or hits disk is `(version, []byte)` from a connector-supplied
   serialiser.** Flink's `SimpleVersionedSerializer` — `getVersion()`, `serialize(E)`,
   `deserialize(int version, byte[])` — is forty lines and buys both binary-upgrade compatibility and the
   future out-of-process boundary in one move.
7. **`context.Context` on every method that can block, from the first commit.** Connect has none and pays
   with KIP-419 (a safe teardown callback) being unfixed for seven-plus years; Benthos carries
   `// TODO V5: Add context here` in `stream_builder.go` because it is now a breaking change.
8. **Config lands pre-parsed, pre-validated, pre-defaulted in the constructor.** No `Configure()` callback, no
   `Map<String,String>` re-parsed inside the connector (Connect's actual behaviour, via `AbstractConfig`).
9. **The record is immutable with respect to provenance.** Connect gets this structurally:
   `SourceRecord.newRecord(...)` forwards `sourcePartition`/`sourceOffset` unchanged, so a transform *cannot*
   corrupt checkpoint identity. Connect's `SinkRecord.originalTopic/originalKafkaPartition/originalKafkaOffset`
   retrofit (KIP-793) is what it costs when identity and payload are conflated.
10. **Capability negotiation is a pure function of config, callable before start.** Connect's
    `exactlyOnceSupport(config)`, `canDefineTransactionBoundaries(config)`, `validate(config)`; Airbyte's
    `supported_sync_modes` vs configured `sync_mode`. An impossible pipeline is refused at submit time.
11. **The core owns metric naming, tagging and export; the connector registers metrics through its
    context.** Connect's `PluginMetrics` (auto-tagged `connector`/`task`/`converter`), Flink's typed
    `SourceReaderMetricGroup`/`SinkWriterMetricGroup`. Never let a connector name a metric.
12. **Snapshot/backfill records flow through the same envelope, the same batcher, the same ack path and the
    same sinks as stream records.** One pipeline, two producers. Every system that has a snapshot got this
    right; it is the only part of Benthos's snapshot story that is not a defect.

---

## How to read the decisions

Each decision states the fork, why it is load-bearing *for canal specifically*, the options with the systems
that chose them, honest costs, a recommendation, and the concrete consequences of taking it. Coupling is
recorded per decision and collected into a graph in Part 2.

---

## D1 — The record / envelope model

**Question.** Is canal's canonical record (a) a domain-free opaque envelope, (b) a typed CDC envelope, or
(c) a domain-free envelope with an optional typed change-event *facet*?

### Options

**(a) Domain-free envelope — payload + metadata, nothing else.**
*Benthos* (`Message{part, onErr}`, no exported fields, no key/partition/timestamp/op/schema; everything
CDC-shaped lives in metadata by convention), *Vector* (`LogEvent{fields: Arc<Value>, metadata}` — no op, no
key, no before/after, no sequence number), *Kafka Connect* (has key/value/headers/timestamp but explicitly
**no operation type**).

- Pros: maximally source/sink-agnostic; transforms are writable once; a generic UI can render anything;
  smallest possible core.
- Cons: it does not hold. Benthos's MySQL CDC input invents `MessageOperation{read,insert,update,delete,
  snapshot_complete,xid}` and flattens it to `MetaSet("operation", …)`; Postgres uses a *different* key set
  (`lsn` vs `binlog_position`) and a different op vocabulary (`begin`/`commit` markers behind a flag). There
  is **no cross-source contract at all**, so every CDC-aware sink special-cases every source — which is
  exactly what constraint #1 forbids. Connect's omission of `op` is the documented reason every CDC connector
  reinvented the envelope incompatibly. Airbyte and Singer independently reinvented magic prefixed columns
  (`_ab_cdc_*`, `_sdc_*`) for the same missing fields.

**(b) Typed CDC envelope in core — `{before, after, op, source, ts_ms}`.**
*Debezium* `[unverified]`, *Flink CDC 3.x* `DataChangeEvent` with an `OperationType` enum `[doc-only]`.

- Pros: CDC semantics are expressible generically; a sink can upsert without per-source knowledge; lag and
  progress become computable.
- Cons: forces relational/document shape onto sources that have neither (an HTTP webhook, a metrics scrape, a
  file tail). Risks the exact "Mongo-shaped core" constraint #1 prohibits. Flink CDC's own op enum contains a
  blank fourth value (`Replace`) `[doc-only]` — evidence the vocabulary was not derivable from first
  principles.

**(c) Generic envelope + optional typed facet.** No system does this cleanly today; it is the synthesis the
Benthos dossier proposes and the Vector dossier independently arrives at ("Vector's two-namespace split plus a
third, *typed* layer that Vector didn't need").

- Pros: the generic path is unchanged and the core never switches on source type; sources that can populate
  op/key/before/after do, and sinks that want them ask and get `(facet, ok)`; nothing is Mongo-shaped because
  the facet is *optional data*, not a subtype.
- Cons: two ways to express the same thing during the transition; the facet's vocabulary still has to be
  designed once and versioned; a sink author must handle "facet absent".

### Recommendation — (c), with three separately-lifetimed layers

```
Record
├─ Payload   : one value, two lazily-cached views (bytes ⇄ structured), mutability in the accessor name
├─ Meta      : separately addressable namespace, never serialised to the sink by default
└─ Change    : OPTIONAL typed facet — Op, Key, Before, After, CommitTime, TxID   (ok bool on access)
```

Take verbatim from *Benthos*: one payload with `AsBytes`/`AsStructured`/`AsStructuredMut` +
`HasBytes()`/`HasStructured()` so a sink can take the cheap path and a pipeline that never inspects the body
pays nothing; mutability encoded in the accessor name with lazy deep-clone only if the value is still owned
upstream. Take from *Vector*: metadata as a **separate addressable namespace** (`.field` vs `%field`), not
reserved prefixes in the body — the alternative is R9 vocabulary sprawl and accidental serialisation of
provenance junk into sink payloads. Take from *Vector* the `secrets` compartment. Take from *Benthos*
`SetError`/`GetError` **on** the record, which is what makes mark-and-route error handling need no extra
interface vocabulary.

Diverge from all of them on three points:

1. **Stable framework-assigned in-flight ID on every record.** Benthos's positional batch identity is the
   single worst thing in its API: `(*BatchError).WalkMessages` is marked `// Deprecated: This method is
   harmful` *by its author*, `Indexer`/`SortGroup` exists only to recover positions after filtering and
   reordering, and the strict-mode mixed-batch path in `async_writer.go` is ~60 lines of sort-group gymnastics
   to answer "some of this batch is bad". Every line of that is the cost of no stable identity. This also
   unlocks per-record DLQ, retry targeting, and dedup keys (R5).
2. **Do not adopt `MetaSet`'s "empty string deletes the key"** (Benthos) — "present but empty" must be
   representable.
3. **Do not make the payload an untyped JSON object as the only option** (Airbyte's mistake — it deferred the
   type problem to dbt-based normalization, which became the worst part of the product and was eventually
   deleted). Bytes are first-class; structure is a view.

Also adopt *Benthos*'s three metadata tiers (string / mutable-any / `ImmutableValue` with an engine-called
`Copy()`), because that is the mechanism by which an expensive derived value — notably a resolved schema —
rides along without a per-record clone.

**Consequences.** Serialisation must be able to encode the whole three-layer record losslessly (D2, and
Vector's `native`/`native_json` codec is the precedent: one blessed wire form used for worker-to-worker
transport, buffer/WAL persistence and dead-letter payloads — if those three differ you get three notions of
what a record is, R2 failing in a new way). The `Change` facet is what makes D7's chunked-snapshot upsert
semantics and D4's dedup-by-key expressible. Position does **not** go on the record's public surface: it is
provenance (D3).

**Coupled to:** D2 (a facet the codec cannot encode is not a facet), D3 (positions are provenance, not
payload), D7 (snapshot rows need a distinguishable op and a null position), D8 (schema rides as a facet).

---

## D2 — Is serialisation a connector concern or a separate pluggable stage?

**Question.** Does a source/sink implement encoding, or only transport?

### Options

**(a) Codec is a pipeline-level plugin the connector never names.**
*Kafka Connect*: `Converter.fromConnectData/toConnectData` plus a distinct `HeaderConverter`, configured
per-pipeline as `key.converter`/`value.converter`/`header.converter`. **Any connector composes with any wire
format**, and the same connector output goes out as schemaless JSON, JSON-with-embedded-schema, or a
registry-backed Avro ID with no connector change. The dossier calls this "the single cleanest idea in
Connect's design".

**(b) Two independent stages: serializer + framer (+ compression).**
*Vector*: `encoding.codec ∈ {json,text,logfmt,avro,protobuf,csv,gelf,cef,native,native_json,raw_message}` ×
`framing.method ∈ {newline_delimited,character_delimited,length_delimited,bytes}` × `compression`, and the
whole thing inverted on the source side (`framing` then `decoding`). `[thin — Vector unverified, but the split
is corroborated structurally by Benthos's separate `BatchScannerCreator` stage.]`

- Pros: N codecs × M connectors never multiplies — add a codec, every connector gains it; add a connector, it
  gains every codec. Framing is genuinely orthogonal to encoding (newline-JSON, length-prefixed protobuf,
  comma-delimited text are two independent choices, not three codecs). Makes the source side symmetric so a
  TCP source and a file source share all byte→event logic. **One frame → many records is in the signature**
  (`SmallVec<[Event;1]>`), which correctly handles a JSON array in one frame.
- Cons: three config blocks per connector instead of one; a codec that needs transport-level context (e.g.
  Kafka's schema-registry magic byte) has to be given it.

**(c) Connector owns serialisation.** *Benthos* mostly (`WriteBatch` receives records and each sink marshals),
*Airbyte/Singer* (the wire format *is* JSONL, non-negotiable), *Debezium engine* `[unverified]` expresses codec
choice in the type system at construction (`create(Json.class)`).

- Pros: simplest possible core; a sink that speaks a proprietary SDK does not fight a codec abstraction.
- Cons: every sink reimplements JSON/Avro/compression; codec bugs multiply by connector count; and it makes
  "the same source to a different wire format" a code change.

### Recommendation — (a)+(b): serializer + framer + compression as separate registered stages, plus an escape hatch

Sources implement **transport only** (get bytes / get rows); sinks implement **transport only** (take an
already-encoded, already-framed, already-compressed payload and make a request). Ship the three stages as
independently registered plugin types with reusable config field specs, exactly as Benthos ships
`NewBatchPolicyField`/`FieldBatchPolicy`.

Two refinements the dossiers force:

1. **The codec participates in config-time type validation.** Vector's codec configs expose `input_type()`
   and `schema_requirement()`, so configuring `avro` on a sink fed metrics is a *startup* error. Adopt: a
   codec declares what it can encode and the core rejects the pipeline at submit time (this is D9's
   capability machinery reused).
2. **A codec/scanner that turns one source unit into many records must own an ack aggregator, and the
   framework must provide it.** Benthos's `BatchScannerCreator.Create(io.ReadCloser, AckFunc, details)` with
   the fan-out contract "only once all message batches extracted should the source ack be called", plus the
   `SimpleBatchScanner` + `AutoAggregateBatchScannerAcks` reduction (a 45-line `pending int32` + `finished
   bool` + `ackOnce` implementation). Copy that reduction so no connector author ever hand-writes fan-out ack
   counting.
3. **Escape hatch, explicit and narrow:** a sink whose SDK requires structured input (BigQuery Storage Write,
   a Mongo driver) declares `SupportsStructuredInput` and receives records instead of bytes. This is a
   declared capability (D9), not a type assertion, and the core refuses to attach a codec to such a sink
   rather than silently double-encoding.

Adopt Go's own precedent on the deframing side: `bufio.SplitFunc`'s
`func(data []byte, atEOF bool) (advance int, token []byte, err error)` is almost exactly the deframer shape —
match it rather than invent one.

**Consequences.** Connectors get smaller and more uniform, which is directly constraint #4. The codec registry
is a second instance of D-registry machinery, which validates that machinery early. Schema *encoding* becomes
the codec's choice while schema *presence* stays on the record (D8) — Connect's cleanest decision.

**Coupled to:** D1 (the codec must encode all three layers losslessly), D8 (schema in the record, encoding in
the codec), D9 (codec capabilities and the structured-input escape hatch are declared data), D11 (a codec
failure is a distinct error class and stage — Connect classifies per stage: converter / transform / put /
produce).

---

## D3 — Checkpoint ownership and representation

**Question.** What *is* a checkpoint: opaque connector bytes, a structured map the core can read, nothing at
all (an ack graph), or a hybrid? And who authors, stores and interprets it?

This is the highest-leverage decision in the document. Every system's characteristic strengths and
characteristic failures trace back to it.

### Options

**(a) No checkpoint model — an ack graph only.**
*Benthos*. The entire progress primitive is `type AckFunc func(ctx context.Context, err error) error`. There
is no `Offset`, no `Position`, no `Commit()`, no checkpoint store interface. Durable progress is
connector-private, built on the ack graph plus a `Cache` resource; the reusable contiguous-prefix resolver
(`Jeffail/checkpoint`) lives **outside both repos**.

- Pros, and they are large: sinks need **zero** progress awareness (`WriteBatch(ctx, batch) error` is the
  complete contract, so a new sink can never get checkpointing wrong); **any topology composes** because
  progress is a callback graph, not a scalar — fan-out is "ack when all branches ack", filtering is "ack
  immediately", 1→N expansion is "ack when all children ack", N→M rebatching just works, and none of it needs
  a code path in the core; the source's *native* mechanism is used directly (consumer-group commit, SQS
  delete, AMQP ack) with no translation and no dual-write; out-of-order ack resolution means slow records do
  not head-of-line-block; **~50 lines of core**.
- Cons, and they are disqualifying for canal: **the framework cannot answer "where are we?"** — there is no
  `GetCheckpoint()` anywhere, so no generic lag, no generic progress, no generic "what would we replay". canal's
  end-state includes a metrics/config frontend; this alone rules (a) out. Also: every source reimplements the
  same non-trivial algorithm with its own cache key, format, flush policy and bugs; no cross-connector
  atomicity (`Cache.Set` is unversioned with no CAS — "two workers on the same slot will silently clobber each
  other", no epoch, no lease); in-flight state unbounded unless the connector bounds it (hence three
  overlapping mechanisms — `checkpoint_limit`, `max_in_flight`, `Capped` — all re-imposing backpressure the ack
  model gave away); and a **failed commit is logged and dropped** (`if err = aFn(...); err != nil { …Error… }`,
  `_ = ackFn(...)`), silently losing progress *after* delivery.

**(b) Opaque, connector-authored `(partition, offset)` pair, framework-stored.**
*Kafka Connect*: `Map<String,?> sourcePartition` + `Map<String,?> sourceOffset`, restricted to "Maps of Strings
to primitive types". The framework owns commit; the connector only emits and re-reads.

- Pros: fully source-agnostic while the core owns durability, restart, the offsets REST API and operator
  editing (KIP-875). The batch read API (`offsets(Collection<partition>)`) degrades partially by design.
- Cons: opacity has a measurable price the dossier states outright — "there is no source-side lag metric …
  a direct consequence of the framework not understanding the offset map it stores". The `Map<String,
  primitive>` restriction is a keyhole: Debezium has to string-encode incremental-snapshot chunk state through
  it `[unverified but structurally corroborated by Airbyte doing the same]`.

**(c) Structured, framework-readable state.** *Debezium*'s `OffsetContext` `[unverified]`.

- Pros: the framework can *read* it and therefore decide snapshot-vs-stream on restart, compute progress, show
  it to operators. This is why Debezium can publish `MilliSecondsBehindSource` and `RemainingTableCount` and
  Connect structurally cannot `[thin]`.
- Cons: the state must fit whatever shape the core defines; a live mutating context object is unshippable over
  a wire (fails constraint #3) and couples phases to each other.

**(d) Two-level: optional shared position + per-stream positions.**
*Airbyte*: `AirbyteStateType ∈ {GLOBAL, STREAM, LEGACY}`, where `GLOBAL` = `shared_state` + `[stream_states]`
and `STREAM` = independently committable per-stream state. `[recall, but the *reason* is structural and
checkable]` **`GLOBAL` exists because CDC broke `STREAM`**: with one replication log you cannot commit "table
A up to LSN 500" without also committing B, because rewinding to re-read A re-delivers B. Airbyte shipped
`STREAM` first, suffered, then retrofitted `GLOBAL` plus a `LEGACY` compatibility path.

**(e) The reader's checkpoint *is* its split list.** *Flink* FLIP-27:
`List<SplitT> SourceReader.snapshotState(long checkpointId)`. There is no separate offset concept — position
lives inside the split, and the split is also the unit of assignment and of parallelism.

- Pros: snapshot resume, stream resume and rebalance become **one mechanism**. Ownership transfers atomically
  with the snapshot, never with a message: the enumerator's snapshot *excludes* assigned splits, the reader's
  *includes* them, and `addSplitsBack(splits, subtaskId)` returns a lost reader's last-checkpointed splits.
  Exactly-once split ownership without a distributed lock.
- Cons: presupposes the split model (D5); the core still cannot read *inside* a split's position.

**(f) Checkpoint stored in the destination by the sink.** *dlt* (`_dlt_pipeline_state` table written alongside
the data, restored from there) and *Estuary* (a materialisation may declare that it stores the runtime
checkpoint transactionally) `[both unverified]`; *Connect*'s folk "offsets in the sink" pattern, reachable by
returning `{}` from `preCommit()` to take over entirely.

- Pros: "the data landed but the state didn't" is structurally impossible for transactional destinations —
  one durability domain.
- Cons: only works for destinations with transactions; makes the destination authoritative, so a source
  cannot be re-pointed at a different sink without state surgery.

### Recommendation — opaque-with-a-typed-header, two-level, and the split carries the position

```
Checkpoint (one durable record, one monotonic id)
├─ Header (core-readable, small, closed vocabulary)
│    CheckpointID       uint64        monotonic, framework-assigned
│    PipelineID, Generation
│    Phase              enum          discovering | snapshotting | catching-up | streaming | completed
│    CommittedAt        time.Time     → canal_checkpoint_age_seconds, the primary health metric
│    RecordsIn, RecordsOut int64      → per-checkpoint reconciliation (Airbyte's sourceStats/destinationStats)
├─ Shared   Optional[VersionedBlob]   the one log position, when the source has one   (Airbyte GLOBAL)
├─ Streams  map[StreamID]VersionedBlob per-stream / per-split positions               (Airbyte STREAM)
├─ Splits   []VersionedBlob            the reader's split list, positions inside      (Flink FLIP-27)
├─ Committables map[CheckpointID][]VersionedBlob  the sink's pending set              (Flink §3.5)
└─ SchemaEpoch Optional[VersionedBlob] schema state, committed atomically with position
```

with `VersionedBlob = {SerialiserVersion int, Bytes []byte}` from a connector-supplied serialiser
(`SimpleVersionedSerializer`, verbatim).

Why this exact shape:

- **Opaque bytes at the boundary is non-negotiable** and Airbyte proves the value: because `stream_state` was
  uninterpreted, Airbyte added an entire new phase model (resumable full refresh) **with no wire change** —
  only a capability flag and a new convention. Opacity is what buys future phases for free.
- **A typed header is also non-negotiable**, because opacity is exactly why Connect has no source-side lag
  metric and why its offset REST API can only show operators a blob. The header is the minimum the core needs
  to serve the frontend goal: age, phase, generation, counts.
- **Two-level is not optional.** A model with only per-stream positions cannot express a shared log position
  (CDC); a model with only a global blob cannot express independent per-stream progress (one slow stream pins
  everything, and resetting one stream resets all). Airbyte needed both and retrofitted the second. Design both
  in at commit one. **Whether the commit is atomic across streams is a property the source declares, not
  something the core guesses.**
- **The core never requires positions to be comparable.** An LSN, a resume token, a page cursor and a
  filename+byte-offset are all legitimate positions and only some support arithmetic. Instead let a source
  *volunteer* comparability and backlog as optional capabilities (D9): `PositionComparer.Fraction(from,to)
  Optional[float64]` and `Backlogger.Backlog(ctx) (Backlog{Records, Bytes Optional[int64]; Exact bool; AsOf
  time.Time}, error)`. `Exact` and `AsOf` are mandatory: a `SELECT COUNT(*)` and a `reltuples` estimate must
  not render identically, and a polled backlog without `AsOf` implies liveness it does not have.
  **When a quantity is unmeasurable, omit the metric series entirely — never emit 0.**
- **Schema epoch is inside the checkpoint.** If decoding a historical log event requires the historical
  schema, then schema *is* checkpoint state. Debezium's two independently-committed stores (offsets +
  schema-history) are the counter-example whose canonical failure is "Encountered a change event whose schema
  isn't known" `[unverified, but the structural argument stands on its own and Flink CDC corroborates by
  carrying schemas *on the split*]`. One record, one commit.
- **Own the contiguous-prefix resolver in core.** Copy the algorithm of `Jeffail/checkpoint`
  (`Capped[T].Track(ctx, payload, batchSize) (func() *T, error)`, resolve returning non-nil *only* when the
  committable prefix advances; `Uncapped[T]` as a doubly-linked list splicing resolved nodes and promoting
  payloads) and Connect's `SubmittedRecords` (per-partition FIFO deques, out-of-order acks, watermark over the
  contiguous acked prefix). These are the same ~200-line algorithm invented twice. It must be **in canal**, not
  out-of-tree per connector, and it must be **per-split** so ordering scope is explicit.
- **Copy `OffsetStorageWriter`'s double-buffered flush**: `beginFlush` swaps the pending map aside under a
  semaphore, `doFlush` writes async, `cancelFlush` merges back on failure — so progress accrues during a flush
  and a failed flush loses nothing.
- **A record may carry a null position meaning "not resumable"** (Benthos's snapshot rows: `// This has no
  offset - it's a snapshot message`), and the core must skip committing those. Make it a typed property, not a
  nil convention.
- **Model "safe resume point" distinctly from "record position"**, because CDC must resume only at
  transaction boundaries — Benthos's MySQL input threads a separate `latestXIDPos` for exactly this.
- **A failed commit is an escalatable condition**, not a log line. This is the direct fix for Benthos's
  silent-progress-loss and it is R4 in mechanism form.
- **Checkpoint keys are `(pipeline, source, split/stream)` in the shared store**, never a local file, because
  a checkpoint in a file on a pod's disk cannot be reassigned (Vector's structural limitation).
- **Checkpoint edit is a first-class API operation** (Connect's KIP-875 `GET/PATCH/DELETE /offsets`), and the
  header is what makes it renderable.
- Take *Estuary/dlt* option (f) as an **optional capability, not the default**: a `TransactionalSink` that
  stores the core's checkpoint token atomically with the data and returns it on recovery. That is exactly-once
  for a generic sink without the core owning the destination (D4).
- Reject **merge-patch checkpoints** for now (Estuary) `[unverified]` despite the real scaling argument
  (a source with thousands of partitions re-serialising all state per commit). Revisit only if per-split blobs
  prove too large; the per-split decomposition above already avoids most of it.

**Consequences.** The core gains a genuine progress model and therefore the frontend goal becomes achievable
without per-connector UI code. The cost is a real checkpoint store interface with atomic multi-key writes,
CAS and a fencing token (D14) — which Benthos's `Cache` explicitly declines to provide ("It is okay for caches
to return nil on duplicates if it isn't possible to implement"), so **canal must not overload a cache
interface for this**.

**Coupled to:** D4 (commit timing consumes this representation), D5 (splits carry positions), D6/D7 (phase and
snapshot cursors live in the header/blobs), D8 (schema epoch is in the same record), D9 (comparability and
backlog are opt-in capabilities), D14 (the store's atomicity/CAS/fencing requirements).

---

## D4 — When does the checkpoint commit relative to sink durability? (and what each guarantee tier demands)

**Question.** What advances durable progress, and what must a sink implement to support each guarantee level?

### Options

**(a) Wall-clock timer, decoupled from batches and boundaries.**
*Kafka Connect*: `offset.flush.interval.ms` default **60000**, `offset.flush.timeout.ms` **5000**, commit path
calls `awaitAllMessages(timeout)`.

- Cons, all documented: a snapshot chunk can be fully emitted and acked and still be re-emitted after a crash
  because the timer had not fired; produces the ecosystem's most famous log line ("Failed to flush, timed out
  while waiting for producer to flush outstanding N messages", KAFKA-4942) which users misdiagnose by tuning
  the wrong knob; and three overlapping commit hooks with no clear contract (`commit()`, `commitRecord()` where
  a null `metadata` means either "transform filtered it" or "dropped under errors.tolerance=all" —
  indistinguishable, and sink-side `flush()` vs `preCommit()` where returning `{}` silently means "I own
  offsets now").

**(b) Ack callback: returning nil from the sink *is* the ack.**
*Benthos*: `err == nil → ack; err != nil → nack`, one line in `async_writer.go`. *Vector*: reference-counted
`EventFinalizer`s on the record with worst-wins status merge, firing on `Drop`.

- Pros: the sink contract stays three methods; Vector's variant is correct-by-default because **forgetting to
  settle produces "do not checkpoint"** — the safe direction — and fan-out correctness falls out of `Arc`
  semantics rather than a special case. The connector-facing surface of Vector's whole ack system is one
  `event_status()` function mapping "what the remote said" to `Delivered|Errored|Rejected`. Best
  API-effort ratio in the study.
- Cons: the ack carries no position, so the framework still cannot say where it is (that is D3's problem, and
  (b) composes fine with D3's answer); Benthos's version drops failed acks; Vector's `can_acknowledge` ×
  `acknowledgements` negotiation **degrades silently** — enable acks, wire a non-propagating sink, get
  best-effort with no error, even though every input to that decision is known at config time.

**(c) The acknowledgement carries the position.**
*Singer/Airbyte* (the destination yields a STATE message only after everything preceding it is durable; the
platform persists what the destination yielded), *Fivetran* (an explicit ordered in-band checkpoint operation
the connector emits, committed after the sink acknowledges) `[both recall-only but mutually corroborating]`.

- Pros: **deletes all low-water-mark bookkeeping** — no `SubmittedRecords`, no out-of-order ack problem —
  because ordering is guaranteed by the stream and the destination only yields monotonically. Commit points
  align with *source-meaningful* boundaries (end of page, end of chunk) instead of a wall clock. Checkpoints
  become countable, timestamped events, which is a free UI progress signal. And it is wire-expressible: a
  checkpoint that is a message in a stream survives gRPC; a framework timer reading connector-held state does
  not.
- Cons: **the destination can only acknowledge at the granularity the source offered.** If the source emits a
  checkpoint every 10,000 records the sink cannot commit at 500 even having durably written 500 — there is no
  position to name. So checkpoint granularity is `min(source's checkpoint frequency, sink's batch size)` and
  the *uninformed* side sets the upper bound; Airbyte connectors tune this blind. Also, in Airbyte it is a
  **convention, not a mechanism**: a destination that buffers in memory and yields state eagerly produces
  exactly the R4 catastrophe and the protocol cannot express "durable".

**(d) Snapshot-then-confirm with committables (two-phase commit).**
*Flink* Sink API v2, and the ordering is documented in the interfaces themselves:
`flush(endOfInput)` → `prepareCommit() → Collection<CommittableT>` → `snapshotState(id) → List<WriterStateT>`
(committables persisted **inside** the checkpoint) → *checkpoint durable* →
`notifyCheckpointComplete(id)` → `Committer.commit(requests)`.

Plus the **Checkpoint Subsuming Contract**, which is the single most valuable paragraph in the Flink codebase
for canal and is entirely independent of distributed dataflow: notifications are **not guaranteed** to arrive;
ids strictly increase and a higher id subsumes all lower ones; an implementation must behave as if a
non-confirmed checkpoint never happened. The prescribed recipe: a **transient "ready set"** of artifacts, a
**checkpointed `map[ckptID][]Committable` "pending set"**, and on confirm publish everything **up to and
including** that id. Abort means "as if never triggered — the next successful checkpoint covers a longer time
span", *not* "discard the artifacts".

- Pros: exactly-once for a generic destination with no assumption that the middle is Kafka (which is precisely
  the assumption that made Connect's KIP-618 possible and its sink side impossible). A lost confirmation is
  repaired by the next one. The per-item result type — `CommitRequest` with `getCommittable`,
  `getNumberOfRetries`, `signalFailedWithKnownReason`, `signalFailedWithUnknownReason`, `retryLater`,
  `updateAndRetryLater(c)`, `signalAlreadyCommitted` — is **six named outcomes, not a boolean and not one
  exception for a batch**, which is design-rule R7 satisfied in the type system, including the
  partial-success case.
- Cons: more interface surface; requires the sink to have a staging concept; committables must be
  `(version, bytes)` (which is the constraint that makes the pattern survive process boundaries and upgrades,
  so it is a feature).

**(e) The sink stores the checkpoint token transactionally.** *Estuary*, *dlt* `[unverified]`. Strictly the
strongest guarantee where available; unavailable for most sinks.

### Recommendation — (c) as the spine, (d) as the tier-3 capability, (b)'s discipline everywhere, never (a)

**The core rule:** the checkpoint advances **only** when the sink has confirmed durability for every record
preceding it, and the confirmation **names the position**. Concretely:

1. **The source emits checkpoint boundaries in-band** as ordered markers in the record stream (Fivetran's
   shape). The connector chooses where they may fall — end of page, end of chunk, transaction boundary — so
   commit points are source-meaningful. This is also what makes phase transitions checkpointable (D6).
2. **The sink's successful `WriteBatch` return is the ack** (Benthos's discipline: no progress callback on the
   sink, so a new sink cannot get checkpointing wrong), and the *core* maps "batch N returned nil" back to
   "advance the contiguous prefix" using the per-split resolver from D3. This preserves Benthos's
   composability under fan-out/filter/rebatch while giving the core the position Benthos lacks.
3. **Fix (c)'s granularity defect:** the sink can *request* a checkpoint boundary
   (`Context.RequestCheckpoint()`, Connect's `SinkTaskContext.requestCommit()` generalised), so granularity is
   negotiated rather than dictated by the side that knows nothing about the other.
4. **Fix (c)'s convention defect:** because the sink returns/confirms a position and the core advances **only**
   on that return value, "acknowledge before durable" requires a sink to *lie about a return value* rather than
   merely be sloppy about forwarding a message. R4 becomes a type rather than prose.
5. **The delivery tier is which interfaces the connector implements** — Flink's design, and better than a
   config enum:

   | Sink implements | End-to-end tier | What the core does |
   |---|---|---|
   | `Sink` (`Write`/`WriteBatch` + `Flush`) | at-least-once | advance prefix on nil return |
   | `+ SupportsWriterState` | at-least-once, resumable in-progress work | restore writer state at start |
   | `+ SupportsCommitter` (committables + `Commit`) | exactly-once via 2PC | subsuming-contract ready/pending sets |
   | `+ TransactionalSink` (stores the core's token) | exactly-once, one durability domain | recover token from the sink |

   The core computes the pipeline guarantee as **min(source capability, sink capability, declared mode)** and
   **refuses an impossible pipeline at submit time**. The pipeline config declares its *intended* guarantee and
   the core validates it against component capabilities — which is precisely the check whose absence is
   Vector's most dangerous silent degradation.
6. **Implement the Checkpoint Subsuming Contract verbatim** for tier 3, including "abort ≠ discard".
7. **Keep the interface barrier-shaped without building barriers.** Write `{positions, committables,
   connector state, schema epoch}` as **one durable record under one monotonic id**, but put `Snapshot(ctx, id)`
   on every stage so a real barrier protocol could be inserted later with zero connector changes. Flink's own
   verdict: property 1 (atomic position+state) is what canal needs; properties 2–3 (multi-input alignment,
   consistency across a shuffled graph at 1000-subtask scale) are what the entire aligned/unaligned/
   in-flight-persistence complexity exists to serve, and canal must not pay for them. A single-process Go tool
   can briefly stop admitting records while it snapshots.
8. **Give committables an expiry and fail loudly when one expires.** Flink's `transactionTimeout` +
   `ignoreFailuresAfterTransactionTimeout` + a warning ratio is the honest treatment; silent skipping is not.
   Relatedly: **never publish an outcome that silently drops data** — Flink's `signalFailedWithKnownReason` is
   documented as "only logs the error, discards the committable and continues", which canal must not copy.
   Every terminal failure produces a dead-letter record and a visible status change.
9. **Duplicates on restart are the default and must be written down next to the code that causes them.**
   Flink CDC's in-source comments — "this behaviour downgrades the delivery guarantee to at-least-once" and
   "only the intersection of both is exactly-once" — are the standard.

**Consequences.** Sinks stay trivial at tier 1 (constraint #4 preserved) and the machinery only appears for
connectors that opt in. The core needs the per-split ack resolver, a committable serialiser per sink, and a
`Snapshot(ctx,id)` on every stage. Dedup (R5) becomes a *separate* concern requiring a strict-CAS store, not
the cache.

**Coupled to:** D3 (representation), D5 (per-split ordering is what licenses the prefix watermark), D6/D7
(phase transitions are checkpoint boundaries; chunked-snapshot output must be keyed upserts, and that is a
capability the core checks at submit time), D9 (tiers are declared capabilities), D11 (commit failure is an
error class), D12 (in-flight bound is what caps the pending set), D14 (atomic multi-key store).

---

## D5 — The connector/task split and how work is partitioned

**Question.** What is the unit of work, who creates it, who assigns it, and can the set change while running?

### Options

**(a) One-shot planning: the connector emits N opaque task configs at start.**
*Kafka Connect*: `List<Map<String,String>> taskConfigs(int maxTasks)`; the framework never inspects them; the
assignor places `{connectors} ∪ {tasks}` across workers.

- Pros: the cleanest statement of the planner/placer separation, and the reason it is in Part 0.
- Cons: **work is split exactly once, at connector start.** Re-splitting requires
  `context.requestTaskReconfiguration()`, which *restarts tasks*. There is no dynamic work-stealing and no
  split enumerator, so "snapshot with 20 workers then stream with 2" is **inexpressible** — and that asymmetry
  is a first-class fact of CDC (snapshot is embarrassingly parallel, log streaming is inherently serial with a
  single cursor). `tasks.max` was advisory for eight years until KIP-1004 had to enforce it because "buggy or
  hostile connectors could generate excessive task configurations, threatening cluster stability", shipping a
  deprecated `tasks.max.enforce=false` escape hatch for connectors already violating it. Stop-the-world
  rebalancing was the original design (KIP-415: "rebalance storms … could take several minutes to stabilize"),
  and the incremental cooperative replacement shipped its own bug class (KAFKA-12495, unbalanced distribution).

**(b) Enumerator + reader, with splits as first-class values.**
*Flink* FLIP-27. `Source<T, SplitT, EnumChkT>` is a **serialisable factory**, not a runtime object, with
`createEnumerator` and `restoreEnumerator` as **separate methods** so cold start and warm start cannot be
confused (contrast Connect's single `start(props)`). `SourceSplit` is deliberately almost empty
(`String splitId()`). `SplitEnumerator` has `handleSplitRequest(subtaskId, host)`, `addSplitsBack(splits,
subtaskId)`, `addReader(subtaskId)`, `snapshotState(id)`. `SplitsAssignment` is documented as **always
incremental**. The enumerator gets a **managed single-threaded model** (`callAsync`,
`runInCoordinatorThread`) so user code never writes a lock — and Flink CDC still shipped two
`ConcurrentModificationException` fixes for touching enumerator state off-thread.

- Pros: split assignment is **pull-based** (the reader requests work when it has capacity), which gives load
  balancing with no load model and eliminates the rebalance-storm problem class. `addSplitsBack` means worker
  loss returns unfinished work to the planner instead of restarting a worker to re-read a stored offset.
  Splits are simultaneously the unit of parallelism, resumability, ordering scope, in-flight accounting and
  assignment — **one concept, five jobs**.
- Cons: more moving parts than (a); the enumerator is a stateful component that itself needs checkpointing and
  a state machine; Flink mandates only `splitId() string`, so Flink CDC encoded `table:chunkID` into it and
  parses it back out in three places, warning implementors that a custom id breaks the parser. **Make split
  identity a struct and use the string id only for logging.**

**(c) No split concept at all.**
*Benthos* (an input is a single instance; snapshot parallelism is an internal `errgroup` over a table queue),
*Vector* (a source is `Run(ctx)` and owns everything it can see), *Airbyte/Singer* (one `read` invocation per
connection per sync, one process, streams read sequentially; `stream_slices()` is a split enumerator the
platform cannot see because there is no protocol message for "here are my splits").

- Pros: trivially correct checkpointing, because a single ordered stream *is* the ordering guarantee.
- Cons: that guarantee is exactly what forbids parallelism — Airbyte traded distribution for a trivially
  correct checkpoint model, Connect made the opposite trade and needed low-water-mark machinery. And it makes
  horizontal scale structurally impossible for stateful pull sources: two Vector instances with the same `file`
  source config both read the same files and duplicate. Vector's only horizontally-scalable stateful source is
  `kafka`, and only because it borrows someone else's coordinator. Adding a partition concept later is a
  **breaking interface change**, so it belongs in v1 even if the first coordinator is "one worker, all splits".

### Recommendation — (b), with splits as the universal unit and the plan as durable state

Adopt the enumerator/reader split, verbatim in structure:

- **The reader's checkpoint is its split list** (D3), so position, assignment and progress are one concept.
- **`AddSplitsBack(splits, workerID)`** on worker loss; ownership transfers atomically with the snapshot.
- **Pull-based assignment**: the worker asks for work when it has capacity.
- **The enumerator is a goroutine owning its state, driven by one `select` over a command channel.** This is
  strictly nicer in Go than Flink's `callAsync`/`runInCoordinatorThread` and removes the class of bug Flink CDC
  hit twice. No mutexes, no prose warning about shared state.
- **Split identity is a struct** (`{StreamID, Kind, Bounds…}`); the opaque string is for logs only.
- **A split declares its boundedness.** Bounded splits get job semantics and a terminal `Completed`; unbounded
  splits get lease/assignment semantics. A hybrid pipeline is simply a pipeline containing both — which is how
  D6 dissolves.
- **A split declares whether it is ordered by its cursor.** Meltano's sorted/unsorted distinction `[thin]` is
  the property that *licenses mid-split checkpointing*; without it the core either checkpoints incorrectly or
  refuses to checkpoint at all.
- **Ordering scope is explicit: ordered within a split, unordered across splits.** Benthos can only be
  parallel *or* ordered; naming the scope lets canal be both.
- **The plan is durable state, not the in-memory result of a leader's computation** (D14): an `assignments`
  row per assignment with `spec_revision`, `plan`, `worker_id`, `lease_expires_at`, `generation`. Workers claim
  rows by CAS and renew leases.
- **Hard-enforce the declared parallelism cap at plan time** and fail loudly if the planner exceeds it —
  KIP-1004 retrofitted this after eight years.
- **Delay reassignment of orphaned work with exponential backoff on consecutive revoking rounds** (KIP-415's
  `scheduledRebalance`/`delay`), so a bouncing worker reclaims its own assignments instead of triggering a
  reshuffle. Concretely: lease TTL ~30s, reassignment delay ~120s, both configurable.
- **Per-phase parallelism must be expressible without a restart**: the enumerator may emit twenty snapshot
  splits and then one stream split. This is the requirement Connect cannot meet and it is the main reason (a) is
  rejected.
- **Separate assignment-scoped lifecycle from instance-scoped lifecycle**: `Open(assigned)` / `Close(revoked)`
  distinct from `Start` / `Stop` (Connect's `SinkTask.open(partitions)`/`close(partitions)`, which its own
  javadoc separates from `start`/`stop`).
- **Worker identity is `(workerID, epoch)` from the first commit** — Flink retrofitted attempt numbers onto
  subtask-id-keyed interfaces and paid two `default`-throwing methods and a permanent compatibility wart.

**Consequences.** This is the decision that makes D6, D7, D12 and D14 tractable, and it is the one that cannot
be deferred: every system that omitted it (Benthos, Vector, Airbyte) is structurally unable to scale a stateful
source, and retrofitting it changes the source interface.

**Coupled to:** D3 (splits carry positions and are the checkpoint), D6 (phase = which splits exist), D7
(chunks *are* splits), D9 (the enumerator is where `SupportsChunkedSnapshot` is exercised), D12 (in-flight
accounting is per-split), D13 (a split is not a DAG node — keep them distinct), D14 (splits are the assignment
unit and the lease subject).

---

## D6 — How is snapshot modelled, and how does it hand off to streaming?

**Question.** Is "full scan then tail" a first-class core concept, a per-stream mode, or connector-internal?

### Options

**(a) Connector-internal, keyed on "is there a checkpoint?".**
*Benthos*: `if i.streamSnapshot && pos == nil { snapshot = NewSnapshot(...) }`. Three goroutines under an
`errgroup`, an internal `messageOperationSnapshotComplete` sentinel on a private channel, `pos = startPos`.
*Airbyte* CDC sources: first sync sees empty state, runs a snapshot internally, switches to log reading; the
phase flag lives in the state blob. *Kafka Connect*/Debezium: the same, smuggled through `sourceOffset`.

- Cons, and Benthos's are measured: because the phase decision is `pos == nil`, there is **no representation
  for "partially snapshotted"**, so a crash at 90% of a 500M-row snapshot **restarts from zero**; every
  snapshot batch is tracked with a nil position and the first checkpoint is written only at
  `snapshot_complete`. The platform cannot report snapshot progress, cannot parallelise the snapshot differently
  from the tail, and cannot resume at a different parallelism. Airbyte's identical outcome by an independent
  route is the strongest available evidence: **two mature frameworks both encoded the snapshot phase in the
  opaque checkpoint because neither modelled phases.**

**(b) Per-stream mode as configuration data (no phase, no transition).**
*Airbyte*: `SyncMode ∈ {full_refresh, incremental}` × `DestinationSyncMode ∈ {append, overwrite, append_dedup}`
on the **configured catalog**, per stream. *Singer/Meltano*: `FULL_TABLE | INCREMENTAL | LOG_BASED` per stream
`[both recall-only]`.

- Pros, and they are excellent: **pipeline "type" is not a type at all — it is per-stream configuration of one
  uniform engine.** One connection can full-refresh five lookup tables and incrementally tail two large ones in
  the same sync with no pipeline-type branching anywhere (satisfies R1 for the mode axis, and directly answers
  canal's "multiple types of pipelines" goal without N pipeline classes). **Source-side and destination-side
  modes are orthogonal enums** — the source never learns whether the sink overwrites, appends or dedups, which
  is what makes M×N combinations free. And capability is *declared* (`supported_sync_modes`) while selection is
  *configured*, so the core validates the second against the first.
- Cons: **the handoff is not modelled.** `sync_mode` is a single value; there is no phase, no ordered pair, no
  transition event. So `LOG_BASED`/CDC taps hand-roll it inside their own state — i.e. (b) collapses into (a)
  for exactly the case that matters most.

**(c) Named phases in core with a coordinator.** *Debezium* `[unverified hypothesis]`: separate
`SnapshotChangeEventSource` / `StreamingChangeEventSource` sequenced by a `ChangeEventSourceCoordinator`.

- Pros: phases become first-class named things the core can report on, and the core (not the connector) decides
  whether to snapshot.
- Cons: nothing verified. Also, a phase *enum* in core invites exactly the `switch` statements constraint #4
  forbids — and the Debezium dossier itself flags that snapshot modes started as an enum switched inside each
  connector and were later extracted into a `Snapshotter` SPI, i.e. an admission that the enum was the wrong
  shape `[unverified]`.

**(d) Boundedness of splits, with the backfill modelled as a bounded instance of the stream split type.**
*Flink CDC* — and this is verified from source. `SnapshotSplit` and `StreamSplit` are both `SourceSplitBase`;
the per-chunk backfill is `createBackfillStreamSplit(low, high)` — *a `StreamSplit` with `[low, high)` bounds*.
`isSnapshotReadFinished()` is `highWatermark != null` — **completion is data, not a flag.** The global stream
split carries `isSnapshotCompleted` and the accumulated `List<FinishedSnapshotSplitInfo>`.
*dlt* reaches the same place from the other end `[unverified]`: an incremental cursor with `initial_value` and
`end_value` makes a backfill a **bounded incremental run**, so snapshot and stream are the same code path with
different bounds, trivially chunkable and parallelisable.

- Pros: **one reader implementation serves both** the per-chunk replay and the endless stream. "Snapshot then
  stream" becomes "the split set changes over time", not a phase enum, so there is nothing to switch on.
  Progress, completion and ETA fall out of split counts.
- Cons: requires D5; requires the source to expose comparable positions *for the sources that want chunking*
  (an opt-in capability, not a core requirement).

**(e) Make duplicates harmless so no handoff is needed.** *Estuary* `[unverified]`: schema-level reduction
annotations mean same-key documents combine deterministically, so re-emitting a row during a backfill is not a
correctness problem — "snapshot then stream needs no handoff protocol at all".

- Pros: genuinely dissolves the problem, and is the most elegant answer in the set.
- Cons: requires a reduction/merge model in the core record semantics, which is a large commitment and pushes
  canal toward being an opinionated stream processor rather than a data-movement tool. Unverified. **Note it as
  the alternative to an op-type enum and revisit only if the op enum proves inadequate.**

### Recommendation — (d) as the mechanism, (b) as the operator-facing model, plus a phase in the checkpoint header

1. **Splits carry boundedness; the backfill is a bounded stream split.** No phase enum in the data path, no
   switches. This is the mechanism.
2. **The operator-facing model is Airbyte's orthogonal per-stream pair**: a source-side mode and a sink-side
   mode on the *configured* stream, validated against the connector's *discovered* capabilities. Pipeline type
   is data (R1). Keep the two enums orthogonal — that orthogonality is the single cleanest thing in the Airbyte
   protocol and it is what makes M×N free.
3. **Phase is a small closed field in the checkpoint header** (`discovering | snapshotting | catching-up |
   streaming | completed`) whose *only* consumers are progress reporting, status and metrics — never control
   flow. This is how the core reports which phase is active without switching on it, and it is what makes
   `RemainingSplits`/`percent-complete` renderable generically. Naming phases in core is the item both the
   Debezium and Airbyte dossiers independently demand.
4. **Every phase is checkpointable, including a full scan.** "Snapshot has no state" cost Airbyte a protocol
   change (resumable full refresh, `is_resumable`, `IncrementalMixin` → `CheckpointMixin`). Make `Resumable` a
   declared capability and make a full scan checkpoint like anything else.
5. **The core owns the handoff invariant**, not each connector: `stream_from = position captured at snapshot
   start` (Benthos's MySQL input derives this by hand with `FLUSH TABLES WITH READ LOCK` → `START TRANSACTION
   WITH CONSISTENT SNAPSHOT` → read log coords → `UNLOCK TABLES`; Flink CDC's per-chunk `LOW` watermark is the
   generalisation). For the chunked case: **start the stream at `min(highWatermark)` across all chunks**.
6. **Only start the incremental stream after a complete checkpoint following snapshot completion**, so no
   stream record can precede a snapshot record for the same key (verbatim from Flink CDC's documented rule).
7. **Snapshot records are typed as such and carry a null position**; the core does not commit against them.
8. **`ErrEndOfInput` terminates a bounded pipeline gracefully on the same runtime** as a streaming one
   (Benthos), and `EndOfInput()` + `ErrEndOfBuffer` is how end-of-phase propagates *through* a stateful stage.
   There is no separate "batch mode". Add the terminal `Completed` status that Connect lacks.
9. **A source declares "I am processing backlog"** (Flink's `setIsProcessingBacklog`) so the core batches
   aggressively during a scan and optimises latency once caught up.
10. **Give the source a heartbeat concept** — a way to advance the durable position with no records emitted —
    so an idle stream's stored position never falls out of the upstream retention window (Benthos ships this as
    a per-connector hack; it belongs in core).
11. **Build the in-band control channel from day one.** Flink's entire out-of-band control plane is one line:
    `public interface SourceEvent extends Serializable {}`, bidirectional via
    `sendEventToSourceReader`/`sendSourceEventToCoordinator`. Debezium had to invent a *database signalling
    table* (`execute-snapshot`/`pause-snapshot`/`resume-snapshot`) purely because Connect offers no way to
    command a running task — its only task-directed controls are "change the config" and
    `requestTaskReconfiguration()`, both of which are restarts. canal owns its runtime and has no excuse.
    **And assume every control message can be lost**: add an idempotent periodic reconciliation sweep (Flink
    CDC re-requests unacked finished splits every 30s) rather than relying on acknowledgement.

**Consequences.** "Multiple types of pipelines" stops being a taxonomy problem. A batch pipeline is a pipeline
whose splits are all bounded; a CDC pipeline is one unbounded split plus a shared position; hybrid is both. The
frontend gets a real progress bar from split id-sets.

**Coupled to:** D5 (splits and boundedness), D7 (chunking is the mechanism inside a snapshot split), D3 (phase
in the header, per-split cursors), D4 (phase transitions are checkpoint boundaries and the chunked path
requires keyed upserts), D9 (`Resumable`, `SupportsChunkedSnapshot`, `Backlogger` are capabilities), D16/status
(phase is not health).

---

## D7 — Is snapshot chunking / parallelism a core concept or connector-internal?

**Question.** Who owns chunk splitting, the watermark protocol, the dedup filter, and chunk-level resume?

### Options

**(a) Connector-internal.** *Benthos* (per-table workers from a bounded pool; keyset pagination on the PK;
`snapshot_batch_size`, `max_parallel_snapshot_tables`, and a `.Deprecated()` `snapshot_memory_safety_factor`
fossil), *Airbyte* (`stream_slices()` inside the connector process), *Connect*/Debezium (connector reads
min/max PK and slices by `chunk.size`).

- Pros: the core stays tiny; each source uses its own most efficient pagination.
- Cons: every source reimplements chunking, none of them resumably (Benthos's restarts from zero); the platform
  cannot see chunks so it cannot show progress, cannot assign chunks to workers, and cannot re-parallelise.

**(b) Core owns a generic chunked-snapshot engine.** *Flink CDC*, read from source. The full recipe:

1. Chunk the key space with explicit `START`/`MIDDLE`/`END` bounds, **unbounded at both ends** (`(-∞, 25)`,
   `[25,50)`, …, `[100, +∞)`) so rows inserted outside the observed min/max after splitting began are still
   covered. Two strategies: fixed step length for numeric/auto-increment keys, and a `SELECT MAX(k) FROM
   (… WHERE k > last ORDER BY k LIMIT n)` probe otherwise.
2. **Per chunk, the Offset Signal Algorithm:** record `LOW` → `SELECT` and buffer the chunk → record `HIGH` →
   read the log from `LOW` to `HIGH` → **upsert** the log records into the buffered chunk → emit the whole
   buffer as inserts → emit `END`. `WatermarkKind` is a three-value enum: `LOW`, `HIGH`, `END`.
   The interval semantics are documented per dispatcher: `(low, high)` is snapshot data, `(high, end)` is
   backfill data which *may duplicate* snapshot data, "**only the intersection of both is exactly-once**".
3. **Checkpoint the splitter's own cursor**: `ChunkSplitterState{currentSplittingTableId, nextChunkStart,
   nextChunkId}` with `ChunkBoundType{START, MIDDLE, END}` — so `-∞`/`+∞` are data, not null-with-a-comment,
   and slicing a huge object resumes mid-way.
4. Report `(splitId, HIGH)` back to the enumerator, which accumulates `FinishedSnapshotSplitInfo{keyRange,
   highWatermark}` — **that tuple is the entire snapshot→stream contract**, and nothing about it is relational
   or even database-specific: "a partition of the key space, and the position from which changes to that
   partition are still needed".
5. Stream phase filter: emit iff `record.key ∈ range(chunk) && position > HIGH(chunk)`.
6. **The filter self-retires**: once the stream position passes `max(HIGH)` for an object, drop the filter for
   that object forever, so steady-state cost is zero.
7. Persist `splitFinishedCheckpointIds map[splitId]ckptId` — **when** each completion became durable, not
   merely that it completed.
8. `filterOutdatedSplitInfos(state, currentFilter)` — reconcile restored state against current discovery as a
   **pure function**, dropping state for objects that no longer exist and adjusting the expected total so
   completeness predicates (`totalFinishedSplitSize == len(finished)`) stay correct.

- Pros: it is the industry-best answer, it is **built entirely on top of FLIP-27 with no core changes** (which
  is itself the proof that the split abstraction is right), it needs no locks and does not stop the stream, and
  the only requirements on a source are: **positions are comparable** (`isBefore`/`isAfter`/`isAtOrAfter`), the
  **key space is splittable**, and changes are **replayable from a position**. All three are source-agnostic
  and all three are opt-in.
- Cons: real complexity in core. Flink CDC's own scars: the original per-record filter was an **O(chunks)
  linear scan**, later retrofitted with sorted ranges + binary search behind a `supportsSplitKeyOptimization`
  dialect gate; and the finished-chunk set turned out **too large to ship in one message**, requiring four new
  events, a separate `totalFinishedSplitSize` field and a truncation path after the fact.

### Recommendation — (b), in core, behind opt-in capabilities, with both scars pre-fixed

Implement the eight-step recipe as canal's generic snapshot engine, gated on two declared capabilities:

```
SupportsChunkedSnapshot : the source can enumerate key-space chunks and read a bounded range
SupportsReplay          : the source can replay changes from a given position
PositionComparer        : positions are orderable / fractionable   (also D3's lag input)
```

A source with none of these does a single unchunked, unresumable scan and says so; a source with all three gets
parallel, resumable, lock-free, stream-concurrent snapshotting for free. **That is the payoff for putting it in
core: the connector's obligation shrinks to "give me ordered chunks by key" and "let me replay from a
position".**

Pre-fix both documented scars: **index the chunk-range filter from day one** (sorted ranges + binary search),
and **assume the finished-chunk set is too large to ship in one message** — page it by reference from the
start.

Two hard requirements the core must check at submit time, not discover in production:

- **Chunked-snapshot output is keyed upserts, not appends.** Flink CDC's exactly-once claim depends on the sink
  upserting by key. Make it an explicit capability requirement checked when the pipeline is submitted.
- **Mid-split checkpointing requires an ordered split** (D5's sorted/unsorted declaration).

The watermark-based approach also needs a way to *inject or recognise a marker* in the stream. Debezium's
signal table is the write-access-requiring version; the **read-only variant that uses the log's own position as
the watermark** is what Flink CDC actually does (`displayCurrentOffset`) and is what canal should require,
because demanding DDL/write access on the source is a deployment tax many operators will refuse `[the read-only
variant's existence in Debezium is unverified; Flink CDC's position-based approach is verified]`.

**Consequences.** This is the single largest piece of core machinery canal will build, and it is justified only
because it is generic. If it were per-connector it would be built badly N times (Benthos proves this). It also
makes snapshot the *most observable* phase (chunks remaining/done, percent complete, ETA) rather than the least.

**Coupled to:** D5 (chunks are splits), D6 (bounded backfill split), D3 (splitter cursor + per-chunk cursor in
the checkpoint), D4 (keyed-upsert requirement, and "only start the stream after a complete checkpoint"), D8
(a chunk's schema is pinned at its start), D9 (three opt-in capabilities).

---

## D8 — Schema propagation and drift

**Question.** Does the core have a schema concept, how does it travel, and who decides what happens when it
changes?

### Options

**(a) No schema in core.** *Benthos* historically (the payload is "scalar / `map[string]any` / `[]any` all the
way down"; the MySQL input attaches one via `MetaSetImmut("schema", …)` by convention), *Vector*
(`log_schema` is a *global, mutable-at-load* singleton read by components at runtime — the documented wart,
with `LogNamespace` as the migration out of it), *Flink DataStream* (the schema is the Java type; drift means
**restart the job**).

- Cons: sinks discover drift record-by-record and every sink reinvents "should I ALTER TABLE?". Vector's global
  mutable schema is a named regret. Flink's answer is unusable for canal.

**(b) In-band, per-record schema; encoding decided by the codec.**
*Kafka Connect*: `keySchema`/`valueSchema` on every `ConnectRecord`; whether the schema goes on the wire is the
`Converter`'s choice (embedded literal, registry id, or absent). Logical types are a naming convention
(`name() == "org.apache.kafka.connect.data.Decimal"` + `parameters()["scale"]`).

- Pros: **schema is in the record model, schema *encoding* is in the codec** — the dossier's "single cleanest
  idea in Connect's design", and the reason one connector's output can go out three different ways.
- Cons: per-record cost; core does **nothing** about evolution — no compatibility checker, no drift policy, no
  schema-change event. `SchemaProjector.project(source, record, target)` is a single static method and is the
  entire out-of-the-box evolution story. Logical-types-as-convention means bad connectors and bad converters
  silently disagree constantly.

**(c) Discovery as an explicit operation returning a persisted catalog.**
*Airbyte/Singer*: a `discover` command returns a catalog of streams with JSON Schema per stream plus
`source_defined_cursor` / `source_defined_primary_key` / `supported_sync_modes`; the **`ConfiguredCatalog`
embeds the discovered catalog verbatim** so "what the source found" and "what the operator chose" are
structurally distinct and diffable. *Fivetran*, *Meltano*, *Flink CDC 3.x* `MetadataAccessor` `[doc-only]`
all have an equivalent.

- Pros: discovery is why the UI works — a stream picker with zero connector-specific frontend code, and **drift
  becomes a diff** against the persisted catalog rather than a runtime surprise. Keys/cursors as
  `[][]string` (lists of field paths) means composite and nested keys need no later breaking change.
- Cons: a snapshot-in-time; a source with millions of objects makes discovery expensive; and it does not by
  itself say what to do when the diff is non-empty.

**(d) Schema travels with the unit of work, changes are in-band records, drift policy is core config.**
*Flink CDC*, verified. `SourceSplitBase.getTableSchemas()`; a `SchemalessSnapshotSplit` variant exists because
carrying schemas on thousands of chunk splits would blow up the checkpoint, with the schemas deduplicated into
one map on the enumerator state and `fillTableSchemas` re-hydrating on the way out. Schema changes arrive
in-band and **mutate checkpointable split state** (`splitState.asStreamSplitState().recordSchema(...)`), are
**only processed on a stream split** (a chunk's schema is pinned at its start), and **emission is opt-in**
(`includeSchemaChanges`) — the framework always tracks, the user stream only sees it if asked. Control records
bypass the dedup filter unconditionally.

Plus the best-documented drift artefact in the study: `pipeline.schema.change.behavior ∈ {exception, evolve,
try_evolve, lenient, ignore}` with `lenient` as the **default** and defined as never-destructive — an
`AlterColumnTypeEvent` becomes `RenameColumnEvent` + `AddColumnEvent`, keeping the old column and adding a new
one so no data is lost. Plus per-event-type control (`include.schema.changes`/`exclude.schema.changes` over
`add.column, alter.column.type, create.table, drop.column, drop.table, rename.column, truncate.table`, prefix
matching, exclude wins), with `TruncateTableEvent`/`DropTableEvent` ignored by default in lenient mode and
`create.table` auto-added when includes are specified.

- Pros: it is the only complete, shipped answer to drift in any surveyed system, and it is **entirely
  core-level policy** — a generic sink only declares which change kinds it supports and the core decides the
  behaviour. Sinks can DDL the target *before* the first record that needs it.
- Cons: needs a schema-change event vocabulary in core; needs a quiesce-and-flush before applying DDL
  downstream (Flink CDC's "after making some internal synchronizations and flushes" `[doc-only]`), otherwise
  records written under the old schema race the `ALTER`.

**(e) Canonical type set with structural fingerprinting.** *Benthos* `public/schema`: a `Common` type set
defined as the lossless intersection of supported formats, separate optional `LogicalParams` for parameterised
types, an explicit arbitrary-precision `BigDecimal` escape hatch for sources that cannot report precision, a
SHA-256 **structural fingerprint** as schema identity, and a `Cache[T]` + one `ConvertFunc[T]` per sink so
**N×M conversion becomes N+M and is memoised**.

### Recommendation — (c) + (d) + (e): discovery, schema-on-split, in-band change events, core drift policy

1. **`Discover(ctx, config) (Catalog, error)` is a required source method and its output is persisted.**
   Two types: a discovered `Catalog` and a `ConfiguredCatalog` that embeds it verbatim. Drift is then a diff,
   and the UI gets a stream picker for free. A discovered field may declare itself non-negotiable
   (`source_defined_cursor`-style) so a connector constrains operator choices *in data*, not in code. Keys and
   cursors are `[][]string`.
2. **Schema rides on the split, not on every record**, deduplicated in the enumerator's state, re-hydrated on
   assignment. Per-record schema (Connect) is the fallback for sources with genuinely per-record schemas; the
   record carries schema as a **typed optional facet** (D1), not a metadata string key, and Benthos's
   `ImmutableValue` tier is the mechanism that avoids a per-record clone.
3. **Schema changes are ordered in-band events with a "schema before data" ordering rule**, tracked into
   checkpointed state whether or not they are emitted downstream, applied only during the stream phase, and the
   emission is opt-in. The schema epoch is committed **atomically with the position** (D3).
4. **Adopt the five-mode drift policy wholesale**, with never-destructive `lenient` as the default and
   `drop.*`/`truncate.*` opt-in, plus per-event-type include/exclude with prefix matching. Deciding the policy
   in core is what stops "attach whatever we found" from pushing an unanswerable question onto every sink.
5. **Split the sink into write + apply-schema facets** (`MetadataApplier`'s shape) and **quiesce and flush
   in-flight data before applying a DDL downstream.**
6. **Adopt (e)'s canonical type set** as the lossless intersection of the formats canal intends to support,
   with parameterised logical types in a separate struct (so a new parameterised type does not widen the enum),
   an explicit unknown-precision escape hatch, structural fingerprinting as identity, and the per-sink
   `Cache + ConvertFunc` memoisation. **Do not** make logical types a naming convention with a parameter map
   (Connect's approach and its documented source of silent disagreement).
7. **Reject Vector's global mutable `log_schema`** unconditionally; it is a named regret with a migration
   already underway.

**Consequences.** Discovery becomes a required source method, which is a real cost for sources with no natural
catalog (a webhook receiver returns a single-stream catalog). The payoff is the entire stream-picker UI plus
drift-as-diff plus per-stream mode selection (D6), all with zero per-connector frontend code.

**Coupled to:** D1 (schema as a facet), D2 (schema presence in the record, encoding in the codec), D3 (schema
epoch in the checkpoint), D6 (per-stream modes are chosen against the discovered catalog), D7 (a chunk's
schema is pinned at its start), D10 (the catalog and the config spec are both connector-declared data the UI
renders).

---

## D9 — Required vs optional interface surface, and the capability-upgrade mechanism

**Question.** How does canal add a capability in v3 without breaking every connector written for v1 — given Go
has no default methods?

This decision constrains every other decision's ability to evolve. It is the second-highest-leverage fork in
the document.

### Options

**(a) Fat interface with nullable/no-op defaults.**
*Kafka Connect*: `exactlyOnceSupport(config)` returns **null** meaning "assume unsupported";
`canDefineTransactionBoundaries` defaults `UNSUPPORTED`; `alterOffsets` defaults `false`; `commit()` and
`commitRecord()` are no-ops.

- Cons: a tri-state capability encoded as a nullable enum, and Java `default` methods only prevent
  *compilation* breakage, not *linkage* breakage. The official javadoc for `ConnectorContext.pluginMetrics()`
  literally instructs connector authors to write `catch (NoSuchMethodError | NoClassDefFoundError e)`.
  **Go has no `default` escape hatch at all**, so (a) is strictly worse in Go than in Java.

**(b) Optional interfaces discovered by type assertion.**
*Flink*, as an explicit written design rule in FLIP-372: *"Every new feature should be added with a
`Supports<FeatureName>` interface… No redefining interface methods during interface inheritance — it would
prevent future deprecation. Minimal inheritance extension."* Also *Benthos* (`ConnectionTestable`,
unexported `batchedCache`).

- Pros: in Go this is `interface{...}` + type assertion and it is **strictly better than Java's version
  because Go has no `default` methods to abuse**. It is the only mechanism that lets required interfaces stay
  tiny and frozen.
- Cons, measured in Benthos: every internal wrapper (`airGapReader`, `maxInFlight`, `autoRetryInputBatched`,
  `forceTimelyNacksInputBatched`, …) must **manually forward the probe** — nine hand-written forwarders for one
  capability. And critically: **a type assertion cannot cross a process boundary.** Benthos's own gRPC path had
  to demote the `AutoRetryNacks` decision to a **boolean in the init response**. Benthos also left
  `batchedCache` *unexported*, so implementers discover it from docs rather than the type system — a defect not
  to copy.

**(c) Declarative capability data returned at init.**
*Benthos*'s gRPC boundary (`BatchInputInitResponse{error, auto_replay_nacks bool}`) and its constructor
returns (`BatchOutputConstructor → (out, batchPolicy, maxInFlight, err)`), *Airbyte*
(`supported_sync_modes`, `is_resumable`, `supported_destination_sync_modes` in the catalog/spec), *Vector*
(`can_acknowledge()`, per-component published guarantee matrix), *Fivetran/Meltano* (capability string
constants) `[the last three recall-only, but Benthos's proto is verified]`.

- Pros: **data crosses a process boundary; type assertions do not.** This is the pattern that makes an
  interface transport-portable, and it is directly constraint #3. It also makes capabilities queryable by the
  registry/UI without instantiating anything, and checkable at submit time.
- Cons: a capability *flag* still needs *methods* to be useful — a bool saying "I support chunked snapshots"
  is worthless without the chunking methods. And Benthos's tuple-return grows with every capability
  (`(out, batchPolicy, maxInFlight, err)` is already four), with `maxInFlight < 1` validated at construction
  with a stringly error.

### Recommendation — (b) **and** (c), with a strict rule about which is which

**The rule: behaviour is an optional interface; the *fact* of that behaviour is declarative data.**

```
Required (tiny, frozen, versioned):
    Source : Connect(ctx) / ReadBatch(ctx) / Close(ctx)      + Discover(ctx, cfg)
    Sink   : Connect(ctx) / WriteBatch(ctx, batch) / Close(ctx)

Declared at init, as a plain serialisable struct (never a tuple):
    Capabilities{ Batching BatchPolicy; MaxInFlight int; Ordering OrderingScope;
                  Resumable, Chunkable, Replayable, Comparable, StructuredInput bool;
                  DeliveryTier Tier; SchemaChangeKinds []Kind; APIVersion int; ... }

Optional interfaces, one capability each, type-asserted in exactly ONE place:
    SplitEnumerator, SupportsChunkedSnapshot, SupportsReplay, PositionComparer,
    Backlogger, SupportsCommitter, SupportsWriterState, TransactionalSink,
    SupportsSchemaApply, ConnectionTestable, SupportsStructuredInput
```

Enforcement details the dossiers make mandatory:

- **The declared struct and the implemented interfaces are cross-checked at registration.** Declaring
  `Chunkable: true` without implementing `SupportsChunkedSnapshot` is a **registration-time panic**, not a
  runtime nil-check. This is what stops (c)'s "flag without methods" failure and makes (b)'s assertions
  discoverable.
- **Type-assert in exactly one place** — a single `resolveCapabilities(any) resolvedConnector` at
  construction, producing a struct of non-nil-or-nil function values. This is the generic forwarding helper
  Benthos lacks, and it eliminates the nine-forwarder tax by construction.
- **Never add a method to a required interface.** Flink's five `default`-throwing
  `UnsupportedOperationException` methods and **three Sink API rewrites** are the price of doing otherwise.
- **Never put a type parameter on an interface a future feature might need to vary, and never nest an
  interface inside another interface.** FLIP-191 needed a whole new package because *"there is not an easy way
  to replace/remove the `GlobalCommitter` … due to the typed parameters"*, and FLIP-372 names inner-interface
  coupling as what *"prevented the evolution of the `TwoPhaseCommittingSink`"*. This is the concrete meaning of
  constraint #5's "gratuitous generics are a defect": generics on a *concrete* helper (`Capped[T]`,
  `Optional[T]`, `Cache[T]`) are good; generics on a *plugin interface* are a future migration.
- **Grow core capabilities through the context interface, not the connector interface.** The core implements
  the context and passes it in, so adding a capability to the core never breaks a connector. Connect's
  `pluginMetrics()`-with-a-try-catch is the counter-example.
- **Include an explicit `(api_version, capabilities)` handshake at init.** Benthos's gRPC boundary has none,
  so an older plugin cannot know it is missing a required semantic.
- **Every optional interface is exported** (Benthos's unexported `batchedCache` is a defect).
- **Every method is `(ctx, serialisable request) → (serialisable response, error)`.** No `Class` handles, no
  closures in request/response, no futures, no behavioural schema objects, no interface-typed fields. This is
  the checklist Connect fails on five counts — `Class<? extends Task> taskClass()`, `Map<String,?>` payloads,
  behavioural `Schema`/`Struct`, `Future<Void>`/`Callback<Void>`, and no `Context` — and it is precisely why
  no out-of-process Connect exists and cannot. Benthos's
  `connect/proto/redpanda/runtime/v1alpha1/{input,message}.proto` is the existence proof of the opposite:
  four RPCs, an opaque `uint64 batch_id` standing in for the ack closure, `oneof payload { bytes | Value }`,
  `StructValue metadata`, and a closed `Error` with `not_connected`/`end_of_input`/`backoff`. **Design canal's
  Go interfaces so that that proto could be generated from them.** The reason the ack closure survives is that
  its signature is `(ctx, err) error` and carries no other state — a richer ack would have needed a schema.
- **Let the out-of-process host be an ordinary in-process plugin** and report a dead subprocess as
  `ErrNotConnected`, so process supervision reuses the connection state machine.
- **Define the declarative spec as primary and the Go builder as sugar over it**, so the manifest and
  in-process vocabularies cannot diverge — Benthos's did, and out-of-process plugins lost `Secret`, `Optional`,
  `Examples`, nesting and lint rules.
- **Let a connector declare which other connectors' persisted state it can adopt**
  (`CompatibleStateNames() []string`, Flink's `WithCompatibleState`) so a future rewrite is a declaration, not
  an operator runbook.

**Consequences.** The required surface stays at three-to-four methods, which is constraint #4 satisfied
literally. Every other decision in this document gains a safe growth path. The cost is one careful
`resolveCapabilities` function and a registration-time consistency check.

**Coupled to:** *everything*. Specifically D2 (structured-input escape hatch), D3 (`PositionComparer`,
`Backlogger`), D4 (delivery tiers as interfaces), D5 (`SplitEnumerator`), D6/D7 (`Resumable`, `Chunkable`,
`Replayable`), D8 (`SupportsSchemaApply`, schema-change kinds), D10 (capabilities are part of the connector
descriptor the UI reads), D12 (batching/in-flight envelope is declared data).

---

## D10 — Config self-description for the UI

**Question.** In what form does a connector declare its configuration, and is that form sufficient to render a
form, validate live, generate docs, and specialise sink UX — with zero per-connector frontend code and zero
core edits?

### Options

**(a) A framework-owned typed spec in the host language.**
*Kafka Connect* `ConfigDef` — 14 fields per key, of which `group`, `orderInGroup`, `width`, `displayName`,
`importance` are purely presentational; `Validator` (offline) and **`Recommender`** (`validValues(name,
parsedConfig)` + `visible(name, parsedConfig)` — *valid values and visibility are functions of the whole
current config*) with an explicit `dependents` edge list and a topological walk so recommenders see parsed
upstream values. Served verbatim over HTTP as **definition+value pairs** (`ConfigInfos{groups, configs:
[{definition, value}]}`), so a frontend renders a connector it has never heard of.
*Benthos* `ConfigSpec`/`ConfigField` — a fluent builder with a closed type set (18 constructors) and modifiers
`Description`, `ShortDescription` (explicitly "inline help within a form UI"), `Advanced`, `Deprecated`,
`Default`, `Optional`, `Secret`, `Example(s)`, `Version`, `LintRule`; **`Default` doubles as the
required/optional marker** with `Optional()` as the third state and `Contains(path…)` to test presence;
exported as JSON Schema with `is_advanced`/`is_deprecated`/`is_optional`/`is_secret`; docs auto-generated with
Common/Advanced tabs; a typed lint enum with line/column including **`LintUnknown`** for unrecognised fields.

- Pros: this is the reason both systems have usable UIs. Benthos's **reusable composite field specs paired with
  extractors** (`NewBatchPolicyField`/`FieldBatchPolicy`, `NewBackOffField`/`FieldBackOff`, TLS, URL,
  `max_in_flight`, metadata filter, scanner) are the killer feature: retry/backoff/batching/TLS become
  *configuration*, identical across every connector, documented identically, rendered identically, with **zero
  coordination between connector authors and zero core switches**. And a connector's config can *contain other
  components* (`NewOutputField` → `FieldOutput` → `OwnedOutput`) with automatic observability nesting via
  `IntoPath` — which is how `retry`/`fallback`/`switch`/`broker` exist with no core special-casing (D13).
- Cons: `ConfigDef` cannot describe nested/repeated structures — `transforms=a,b` + `transforms.a.type=…`
  is faked with dotted prefixes and needs bespoke `enrich()` logic; everything crosses as `Map<String,String>`
  and is re-parsed inside the task. Benthos fixes nesting but its `FieldX(path…) (T, error)` accessors produce a
  20-line error ladder per constructor for errors the spec already made impossible.

**(b) Connector-supplied JSON Schema with presentation keywords.**
*Airbyte* `connectionSpecification` (draft-07) with `title`, `description`, `examples`, `order`, `enum`,
`pattern` + human `pattern_descriptor`, `multiline`, `group`, `hidden`, `always_show`, `display_type`,
`airbyte_secret: true`, and **`oneOf` + a `const` discriminator for tagged unions** (auth method, file format)
`[recall-only]`. *Meltano*, *Fivetran* similar `[recall-only]`.

- Pros: off-the-shelf validation tooling; `oneOf` is the one thing JSON Schema does that `ConfigDef` provably
  cannot; **`secret: true` in the connector's own schema means the core handles redaction/encryption/masking
  with zero per-connector knowledge**.
- Cons: the UI must interpret arbitrary JSON Schema, which is a large and permanently-incomplete job; and
  Airbyte's `check` returns one boolean plus one string, which is useless to a form.

**(c) Generated from code.** *Vector* `#[configurable]` derive → JSON Schema, doc comments as the only docs
source `[recall-only]`; *dlt* config derived from the function signature `[recall-only]`.

- Pros: no drift between struct and schema; doc comments as single source of truth.
- Cons: needs codegen or reflection; presentation metadata has to be smuggled into attributes; Vector's output
  drives docs, not a live form.

**(d) Sink field specs with default extraction expressions.** *Segment action-destinations* `[recall-only]`:
each destination declares an `authentication` block and named actions; each field is an `InputField` with
`label`, `description`, `type`, `required`, `default`, `choices`, `multiple`, `dynamic`, `depends_on`. Three
ideas nothing else has: **`required` is a predicate over the rest of the config**; **`dynamic` names a
fetcher** the platform calls at form-render time; and **`default` values are a mapping DSL** — path expressions
over the inbound event, so the connector ships a *default mapping from the generic record to its own fields* and
the UI renders that mapping as an editable form.

- This is a complete answer to "how does a generic UI configure a *specialised* sink without the core knowing
  anything about it", which is constraint #1's "specialised UI/UX later must not require core changes" plus the
  frontend goal, simultaneously.

### Recommendation — one Go-declared spec that emits JSON Schema, with composite fields, conditional predicates, named dynamic hooks, and sink field mappings

The connector descriptor is **plain data, and nothing on it is a callback**:

1. **One spec per connector, the single source of truth** for validation, defaults, required/optional, docs,
   JSON Schema for editors, and the form UI. Benthos proves this is the entire answer to the frontend goal with
   zero per-connector UI code.
2. **The declarative form is primary; the Go builder is sugar over it** (D9's rule), so in-process and
   out-of-process vocabularies cannot diverge.
3. **Field modifier set:** copy Benthos's verbatim, including `ShortDescription`, `Advanced`, `Secret`,
   `Examples`, `Version`, `Deprecated`, and the tri-state `required / has-default / optional` + presence test.
   Add Connect's `group`/`order`/`width`/`displayName`/`importance`.
4. **Nesting and tagged unions are first-class**: objects, object lists, object maps (Benthos has these) plus
   a discriminated-union field type (Airbyte's `oneOf` + `const`, which `ConfigDef` cannot express). Address by
   **variadic path segments**, never a dotted string.
5. **Reusable composite field specs paired with extractors** for `batching`, `retry`/`backoff`, `tls`,
   `max_in_flight`, `buffer`, `metadata_filter`, `codec`, `framing`, `compression`. This is the mechanism by
   which D2, D11 and D12 become configuration rather than per-connector code.
6. **Conditional required/visible as declarative predicates over the config**, not callbacks — Connect's
   `Recommender.visible()` and Segment's predicate `required`, expressed as data so **the browser can evaluate
   them too**. Benthos does this with a Bloblang mapping (`LintRule`), which is genuinely clever and
   genuinely a dependency; without an embedded expression language, ship a small closed predicate set
   (`requires`, `mutuallyExclusive`, `atLeastOneOf`, `equals`, `in`) that a UI can interpret, plus a Go
   `Validate(ctx, cfg)` hook for anything richer.
7. **Named dynamic-choice hooks** (`choicesFrom: "list_tables"`) resolved by a separate core→connector call —
   Connect's `Recommender` as *data plus a named hook* rather than a live callback, because a callback cannot
   cross a process boundary.
8. **Two-tier validation returning per-field diagnostics** (Part 0 items 3 and 10): declarative/offline for
   every keystroke, plus `Validate(ctx, config) []FieldDiagnostic` that may hit the network. Never one bool and
   one string (Airbyte's `check`). Add **named connection tests returned as a list of results**, not a bool —
   Benthos's `ConnectionTestResults` shape, and Benthos's `ConnectionTest` over a whole *configured stream*
   without running it is an excellent config-UI affordance.
9. **Typed lint enum with line/column, including `LintUnknown`** for unrecognised fields — silently-ignored
   config keys are the classic YAML-tool failure. Expose linting over the API so the UI validates before
   submitting. Support config walking/marshalling so a UI can load-edit-save without destroying comments.
10. **For sinks, adopt Segment's field-mapping model:** the sink declares the fields it wants plus a **default
    extraction expression over canal's generic record**. The core only knows "fields" and "expressions"; the UI
    is automatically specialised per sink; adding a sink is "declare fields, implement write, register".
11. **Secrets:** declared `secret: true` in the connector's own spec; core owns redaction, masking, encryption
    at rest, and secret-reference indirection. Never return secrets verbatim from the API (a Connect complaint
    `[recall]`).
12. **Do not reproduce the `(T, error)` accessor ladder.** With Go 1.23 generics use `Field[T](conf, path…)`
    with one deferred `conf.Err()`, or decode into a typed struct derived from the same spec. This is the
    single biggest ergonomic tax in Benthos's API and it is avoidable.
13. **Publish a cached connector descriptor from the registry** (id, name, version, docs, support level,
    capabilities, config schema) so the UI can list connectors and render forms **without instantiating
    anything**.

**Consequences.** The frontend goal is satisfied structurally: the UI is a renderer of connector-declared data
plus the read model (D14). No connector addition touches core or frontend code.

**Coupled to:** D9 (the descriptor carries capabilities; declarative-first), D2 (codec/framing/compression are
composite fields), D11 (retry policy is a composite field), D12 (batching and buffer are composite fields),
D8 (the catalog is the other half of what the UI renders), D13 (component-valued fields are how topology
composes).

---

## D11 — Error and retry classification

**Question.** Who classifies a failure, how many classes are there, and what is the terminal disposition?

### Options

**(a) Two-valued, marker-exception-based.** *Kafka Connect*: `RetriableException` → retry (bounded by
`errors.retry.timeout`, `errors.retry.delay.max.ms`); anything else → task `FAILED`. `ToleranceType {NONE,
ALL}` is the entire policy vocabulary — no per-error-class policy, no circuit breaker, no
backoff-then-park. Classification is applied per **stage** (converter / transformation / put-poll / produce),
which is the good part. Plus a framework DLQ with rich provenance headers and `ErrantRecordReporter.report(rec,
err)` so a sink can reject individual records without failing the pipeline — **but only for sinks**.

**(b) Sentinel errors doing control flow, plus a wrapped hint.** *Benthos*: `ErrNotConnected` → reconnect;
`ErrEndOfInput` → graceful termination; `ErrEndOfBuffer`; an `ErrBackOff` wrapper that is **honoured only on
`Connect`** — a hint the framework ignores elsewhere. Retry is `AutoRetryNacks`, **unbounded by default**, so a
permanently poisonous record retries forever and the community-reported symptom is a pipeline making no
progress. `error_handling.strict` (default `false`, "expected to become the default in the next major version")
makes a processing error terminal for that message.

**(c) Per-item outcome objects.** *Flink*'s `CommitRequest` with six named outcomes
(`signalFailedWithKnownReason`, `signalFailedWithUnknownReason`, `retryLater`, `updateAndRetryLater(c)`,
`signalAlreadyCommitted`, plus `getNumberOfRetries()`), and *Benthos*'s `BatchError` — `NewBatchError(batch,
headline)` + `Failed(i, err)` where "if `Failed` is not called then all messages are assumed to have failed; if
it is called at least once then all indexes not explicitly failed are assumed successful", with a **headline
error that degrades gracefully** for consumers that do not understand granularity.

**(d) Classification by ownership, attached by the connector at the point of raise.** *Airbyte*
`AirbyteTraceMessage` with `failure_type ∈ {config_error, system_error, transient_error}` `[recall-only]`,
separate **user-facing `message` and developer-facing `internal_message` + stack trace**, and an attached
`stream_descriptor` so partial success is representable. Vector's `EventStatus{Dropped, Delivered, Errored,
Rejected}` distinguishes transient (`Errored`, retry may help) from permanent (`Rejected`) `[recall-only]`.

### Recommendation — a small **closed** class set, declared by the connector, honoured at every call site

1. **The class set is closed and every call site honours it.** Benthos's `ErrBackOff` being respected only on
   `Connect` is the failure mode to avoid: a connector must not be able to return a hint the framework ignores.
   The wire-representable version already exists in Benthos's proto (`Error{message, oneof{backoff,
   not_connected, end_of_input}}`) — canal's set extends it with the ownership axis:
   `transient-upstream | transient-internal | permanent-upstream | permanent-mapping | permanent-contract |
   duplicate-idempotent-success | clock-skew` (the taxonomy `design-rules.md` already commits to), plus the
   control sentinels `not-connected` and `end-of-input`.
2. **The connector classifies at the point of raise.** This is the transplantable part of Airbyte's design: the
   *ownership* question ("is this my config, their system, or a blip?") is the one an operator UI actually needs
   answered, and only the connector can answer it. Vector's version reduces the connector's entire obligation
   to one `Classify(resp, err) Class` function, which is the best effort/benefit ratio available and drives
   **both** retry policy and end-to-end acknowledgement from one place.
3. **The class is a bounded metric label**, legitimately, precisely *because* the set is closed:
   `canal_records_failed_total{pipeline,stage,class}` with ~9 values. Never label with an error *message* or an
   upstream error *code*.
4. **Retry policy is `{maxAttempts, backoff, terminal disposition}`**, where disposition includes
   dead-letter/quarantine — not Benthos's binary "replay indefinitely" vs "delete". Unbounded retry by default
   is a livelock generator.
5. **Classify per stage** (Connect's good idea): read / deframe / decode / transform / encode / frame /
   compress / write / commit. The stage is a label and the DLQ record names it.
6. **Per-record error routing is a framework concern with a DLQ**, and unlike Connect it must work for
   **sources too** (Connect's `ErrantRecordReporter` is sink-only). Rich provenance on the DLQ record: stage,
   class, connector, split, attempt count, original position, timestamp, error text.
7. **The error travels on the record** (Benthos `SetError`/`GetError`), which is what makes mark-and-route
   handling need no extra interface vocabulary, and `strict` mode — reject already-failed records at the sink
   rather than writing them — is the right **default** for a data-movement tool (Benthos is migrating to that
   default; start there).
8. **Rich errors degrade to simple ones.** Copy `BatchError`'s headline design *and* fix its cause: with stable
   per-record IDs (D1) there is no positional correlation problem and `WalkMessagesIndexedBy`/sort-groups are
   unnecessary.
9. **Split error text by audience**: a user-facing message for the UI and a developer-facing internal message +
   stack for logs. One string cannot serve both.
10. **`retry` and `fallback` are composable component wrappers**, not core features (D13) — Benthos's `retry`
    output is an ordinary registered sink with `maxInFlight = 1` wrapping a child resolved from its own config,
    and it exists because propagating a nack reprocesses the message, which is unsafe when processing had side
    effects. But give such wrappers a **sanctioned** way to declare "I am a pipeline stage, not a leaf" instead
    of Benthos's undocumented `Unwrap()` type assertion and five `XUnwrapper() any` back doors.
11. **Sustained backoff sets `Degraded: True` with a reason and a last-error message**, and the alertable metric
    is `canal_backoff_seconds_total{pipeline,stage,class}` — cumulative *time* spent waiting. A retry count says
    retries happened; retry seconds says the pipeline spends 80% of its life backing off.
12. **Match the operator-facing message standard** Benthos sets in its strict-mode warning: name the count, the
    component, an example error, the fix, and the consequence.

**Coupled to:** D1 (stable record ID makes per-record errors cheap; error on the record), D4 (commit failure is
its own class and must escalate), D9 (the class set is wire-representable data), D10 (retry/backoff is a
composite config field), D12 (backoff interacts with in-flight accounting), D13 (retry/fallback as components).

---

## D12 — Backpressure and batching ownership

**Question.** What bounds the system, who decides batch shape, and what is the expressible outcome when a stage
is full?

### Options

**(a) Nothing, effectively.** *Kafka Connect* source path: `poll()` is called again the instant the previous
batch reaches the producer, with a hardcoded `SEND_FAILED_BACKOFF_MS = 100` as the only pause; the only
backpressure is incidental (`producer.buffer.memory` + `max.block.ms`); `SubmittedRecords`' per-partition
deques are **unbounded** and the javadoc warns about exactly the pathology (one offline partition → unbounded
growth while other partitions keep dispatching). KIP-731 ("Record Rate Limiting") has been Under Discussion
since 2021, naming the "noisy neighbor" and "firehose" problems. `void put(Collection<SinkRecord>)` cannot
express partial acceptance or "slow down"; the sink side has real backpressure only because Kafka's consumer is
pull-based (`consumer.pause/resume`, `SinkTaskContext.pause/resume/timeout`).

**(b) Blocking channels everywhere, with N optional bounding knobs.** *Benthos*: an **unbuffered** transaction
channel as the base mechanism, plus output `max_in_flight` (connector-declared), input `max_in_flight`
(optional wrapper), `checkpoint_limit` via `Capped` (per-connector), and a buffer `limit` — **five mechanisms,
three user-facing names, one documented deadlock between two of them** ("Batching policies at the output level
will stall if this field limits the number of messages below the batching threshold"), and no single place to
observe "why is this pipeline slow".

**(c) Bounded channels everywhere plus a buffer stage whose rejection path is in the type.** *Vector*
`[recall-only, but the enum is the design]`: every edge is a bounded channel so "slowness is transitive by
construction"; every sink has a configurable buffer with `type ∈ {memory, disk}`, a cap, and
**`when_full ∈ {block, drop_newest, overflow}`** — so unbounded growth is *inexpressible*, drops are
**counted** (`buffer_discarded_events_total`), `drop_newest` (not oldest) preserves the already-acked prefix,
and buffers **chain** (small memory overflowing into large disk). Plus adaptive request concurrency (ARC) as a
tower layer.

**(d) Tiny bounded hand-off queue and a non-blocking read API.** *Flink* `SourceReaderBase`:
`FutureCompletingBlockingQueue<RecordsWithSplitIds<E>>` with
`source.reader.element.queue.capacity` default **2** — two *batches*, not two records. Blocking I/O is confined
to the fetcher thread; the task thread must not block; readiness is a future. And the negative lesson stated
outright: **the more you buffer, the worse your checkpoint latency and recovery time** — Flink spent two major
features (FLIP-76 unaligned checkpoints, buffer debloating) undoing deep buffering. Choosing small bounded
buffers early *removes an entire subsystem*.

### Recommendation — bounded by construction, framework-owned, per-split, with one accounting concept

1. **Every edge is bounded; nothing in the topology can grow without limit.** This is R6 generalised from the
   buffer to every edge (Vector's principle) and it is the base mechanism.
2. **One bounded hand-off queue between the connector's I/O goroutine and the pipeline**, capacity measured in
   **batches** and defaulting to a very small number (Flink's 2). In Go this is `chan RecordBatch` with `cap 2`
   plus `select` on `ctx.Done()`; Flink needed ~400 lines to express what a buffered channel does natively.
   Blocking I/O stays on the fetcher side.
3. **Unify in-flight accounting into ONE framework-owned, per-split concept**, replacing Benthos's five
   overlapping knobs, and expose it. Per-split is what makes it also the ordering scope (D5) and the
   checkpoint-window bound (D3's `Capped` capacity, which blocks `Track` and thereby gives backpressure for
   free — four lines of `cond.Wait()`).
4. **The buffer is a separate pluggable stage whose interface documents exactly when it weakens guarantees**
   (Benthos's `BatchBuffer` doc comment is the model: "Buffers are advanced component types that weaken
   delivery guarantees… if you aren't absolutely sure… it likely shouldn't be", and it explicitly names the two
   choices — ack on write to the buffer, or ack when read and acknowledged downstream).
5. **`when_full` is in the type: `block | reject | overflow`.** R6 says "a buffer without a rejection path is
   not a buffer"; Vector's enum *is* that rule encoded. Drops are counted; silent loss is unconfigurable. Drop
   the **newest**, never the oldest, so the acked prefix invariant holds. Buffers chain.
6. **The durability boundary is configurable and ack semantics follow it automatically.** A durable buffer
   takes ownership of the ack handles on write, so the checkpoint advances on buffer write; a memory buffer
   passes them through to the sink. This is R4 implemented rather than written down, and it is why Vector can
   keep the checkpoint store and the buffer as **independent** stores without a shared WAL: the ordering is
   explicit and the checkpoint is strictly downstream of the durability boundary.
7. **Batching is policy the connector declares and the framework enforces**, returned as an **options struct**
   (not Benthos's growing tuple). Four orthogonal triggers — count, byte size, period, **predicate over the
   record** — plus processors that run at flush time. And **model the inverse too**: "a batch policy has the
   capability to create batches, but not to break them down" is a real gap, and sinks almost always have a hard
   maximum request size, so ship a splitter as a framework primitive.
8. **Steal Benthos's `Batcher` API exactly**: `Add(msg) bool` / `UntilNext() (d, ok)` / `Flush(ctx)` /
   `Close(ctx)`. It is goroutine-free inverted control that drops into any `select` loop, and it is what lets a
   source batch on the *input* side (which every CDC source needs).
9. **Partitioned batching is a framework combinator**: the sink supplies a partitioner (usually a template over
   the record) and the framework keeps one open batch per key with its own limits — per-tenant/per-table
   batching with no batching code in the sink `[Vector, recall-only, but the shape is obviously right]`.
10. **Split-level pause/resume is a batched atomic call** (`PauseOrResume(toPause, toResume []SplitID)`), not N
    per-split calls — Flink's shape, with the documented safety property that no other method runs in parallel
    so the update is atomic.
11. **Metrics:** a `utilization` 0–1 gauge per component plus the four numbers that make backpressure
    diagnosable, and the **soft/hard back-pressure split** (waiting for a buffer vs blocked on the downstream
    are different diagnoses). Lint the deadlock Benthos merely documents.
12. **Put a deadline in the ack contract and enforce it centrally with one timer wheel**, rather than
    retrofitting a goroutine-plus-timer per batch. **Guarantee ack idempotency in the framework** — Benthos
    reimplements `sync.Once` wrapping in three separate places.
13. **Note the tension canal inherits and must document**: a source that stops reading may cause the *source
    system* to accumulate WAL / retain a replication slot. Backpressure and upstream resource retention trade
    off, and the heartbeat mechanism (D6) is part of the answer.

**Coupled to:** D3 (`Capped` capacity is the in-flight bound; buffer ownership of acks changes when the
checkpoint advances), D4 (the pending set must be bounded), D5 (accounting is per-split), D10 (batching, buffer,
retry are composite config fields), D13 (the buffer is a stage in the fixed outer shape).

---

## D13 — Pipeline topology, and whether transforms are core

**Question.** Is the pipeline a fixed linear chain, a recursively composed tree, or a general DAG — and does
the core know about transforms?

### Options

**(a) Fixed linear stages frozen into the contract.** The **abandoned canal attempt**: eight stages with
`minItems: 8, maxItems: 8`, `ordinal` constrained `1..8`, buffers modelled twice. R1 exists because of it.
Adding a stage was a breaking contract change.

**(b) Fixed *outer* shape, unlimited variety from recursive composition.** *Benthos*:
`input → buffer → pipeline → output`, always. All topology variety comes from components that *contain* other
components, resolved from config: `broker` (with `fan_out`, `fan_out_fail_fast`, `fan_out_sequential`,
`round_robin`, `greedy`), `switch`, `fallback`, `retry`, `reject_errored`, `drop_on`. Observability nests
automatically via `IntoPath`.

- Pros: config, docs and UI stay tractable while fan-out/fan-in/routing/DLQ all exist with **zero core
  special-casing**. Fan-out semantics under at-least-once are honestly documented: if sink B fails the record is
  nacked and **re-sent to A as well**, so duplicates in A are the price of not losing it in B — with
  `_fail_fast` variants as the escape hatch (whose existence is the admission that the default is sometimes
  wrong).
- Cons: needs a sanctioned "I am a stage, not a leaf" mechanism (Benthos punches a hole with an undocumented
  `Unwrap() output.Streamed` assertion and five `XUnwrapper() any` back doors). Deep nesting in YAML is hard to
  read. Fan-in with shared state is not expressible.

**(c) General DAG.** *Flink*. Justified by a distributed dataflow engine and paid for with barriers, alignment,
in-flight persistence and a compatibility matrix.

- The Flink dossier's own verdict: canal's "DAG" is a pipeline, not a shuffled graph. **Do not pay for this.**

**(d) Transforms in core, 1→1.** *Kafka Connect* `Transformation<R>`: `R apply(R record)` — strictly 1→1 (or
1→0 by returning null), synchronous, stateless, on the task thread. No 1→N, no N→1, no aggregation, no async
lookups, no windowing. Conditional application had to be bolted on later (KIP-585 `Predicate`), and even then
one predicate per transform with **no boolean combinators**.

**(e) Transforms with the full return-type vocabulary.** *Benthos*: `Process(ctx, msg) (MessageBatch, error)`
and `ProcessBatch(ctx, batch) ([]MessageBatch, error)` — 1→N (expand), 1→0 (filter), N→M batches
(regroup/window), and error → mark-and-continue, **all encoded in the return types with no extra vocabulary**.
*Vector* has three transform shapes (function / fallible-function / task) `[recall-only]`.

### Recommendation — (b) + (e): fixed outer shape, recursive composition, transforms as a first-class core stage with the full return vocabulary

1. **The outer shape is fixed and small** — `source → [decode] → [transform chain] → [buffer] → [encode] →
   sink` — and it is *not* in the API schema as an enumerated stage list. R1: the API describes whatever stages
   exist; a fixed stage count is a design smell. The fixed shape is an implementation fact, not a contract.
2. **All variety comes from components containing components**, resolved from config with automatic
   observability nesting. This is how fan-out, fan-in, routing, DLQ, retry-at-sink and transform chains exist
   without a single core switch — and it is the same mechanism as D10's component-valued config fields, so it
   costs nothing extra.
3. **Give meta-components a sanctioned declaration** that they are a stage rather than a leaf. Do not ship
   Benthos's undocumented `Unwrap()` hole.
4. **Transforms are core, with Benthos's return-type vocabulary**, not Connect's 1→1. Connect's limitation is
   the reason its transform ecosystem is stunted and the reason `Predicate` needed a KIP.
5. **Provenance is structurally immutable across transforms** (Part 0 item 9): a transform cannot alter a
   record's identity or position. Connect gets this for free and its `original*` retrofit is what happens
   otherwise.
6. **Ack composition across topology is the ack graph's job** (D4) — fan-out is "ack when all branches ack",
   filter is "ack immediately", 1→N is "ack when all children ack". This is why Benthos's tiny ack primitive is
   preserved even though canal adds a position model: it is what makes arbitrary composition free.
7. **Document the fan-out guarantee explicitly** and ship the `fail_fast` variants. Fan-out is in canal's
   end-state goals and the duplicate-vs-loss trade must be a named choice, not a discovery.
8. **Do not build barrier-based distributed snapshots** for multi-input consistency; if fan-in with shared
   state is ever needed, quiesce-then-snapshot is adequate at single-process scale (D4 item 7).
9. **Transform concurrency destroys ordering.** Benthos's `threads: -1 → runtime.NumCPU()` silently does this
   and does not say so at the config field. canal must state the ordering scope at the knob.

**Coupled to:** D1 (transforms must not touch provenance; error on the record), D4 (ack composition), D10
(component-valued config fields are the composition mechanism), D5 (a split is not a DAG node — keep the two
concepts separate), D12 (buffer is a stage in the fixed shape).

---

## D14 — The standalone ↔ distributed seam

**Question.** What exactly differs between `canal run` on a laptop and a coordinated multi-worker deployment,
and what must never differ?

### Options

**(a) Two herders + three swappable stores.** *Kafka Connect*: the connector-facing API is **byte-identical**
in both modes; what swaps is `StandaloneHerder`/`DistributedHerder`, `MemoryConfigBackingStore`/
`KafkaConfigBackingStore`, `FileOffsetBackingStore`/`KafkaOffsetBackingStore`, in-memory/`KafkaStatusBackingStore`,
plus Kafka group membership. Exactly-once source is distributed-only.

- Pros: this is the correct seam and the dossier says to copy it exactly. `OffsetBackingStore`'s bytes-in/
  bytes-out interface is what makes the swap trivial.
- Cons: the coordination substrate is a **single-partition compacted Kafka topic used as a replicated state
  machine**, with seven record types and a `commit-{id}` marker to fake set-atomicity. Its own javadoc
  documents an **unrecoverable** state: compaction plus a partial write can leave a `commit-foo (2 tasks)`
  whose `task-foo-1-config` has been compacted away, "leaving us in an inconsistent state with no obvious way
  to resolve the issue" — so the class has to expose which connectors are inconsistent for the herder to paper
  over. Also no process isolation (KIP-987: "the shared-JVM model does not permit strong isolation between
  threads"), with the de-facto k8s answer being one Connect cluster per workload — abandoning the shared cluster
  the architecture exists for.

**(b) One binary, two modes by convention only, no coordination.** *Vector*: agent and aggregator roles are
convention; **no clustering, no work assignment, no rebalancing, no shared state, no leader election**. Scale =
N independent processes behind a load balancer, which works only for the stateless tier. Disk buffers pin a
process to a volume. The enterprise control plane was placed *outside* the OSS binary.

- The clearest available statement of a requirement declined. canal cannot follow it.

**(c) Two very different answers.** *Airbyte*: out-of-process connectors orchestrated by a durable workflow
engine (Temporal) with Postgres for config and jobs `[recall-only]`; scaling is "more workers running more
subprocesses". *Benthos*: one binary, a root-config/stream-config split (service-wide observability in one
file, N pipeline documents managed as data over HTTP), and **no coordination in the core at all**.

**(d) Leader plans only; workers hold leases; the data plane is independent.** The synthesis the
observability/control-plane dossier proposes, assembled from Connect's two-level split, KIP-415's delayed
reassignment, and the verified k8s `leaderelection` caveat.

### Recommendation — (d): four interfaces, two assemblies, and a leader that only plans

```go
type Runtime struct {
    Config      ConfigStore      // revisioned CAS + Watch
    Checkpoints CheckpointStore  // bytes in / bytes out; Set atomic across the whole map
    Coordinator Coordinator      // membership, leader election, assignment leases
    Status      StatusAggregator // per-worker status → one PipelineStatus
}
```

**If a fifth interface appears, the abstraction is wrong.**

| Interface | `canal run` (standalone) | `canal serve` (coordinated) |
|---|---|---|
| `ConfigStore` | bbolt/SQLite file, or a YAML file projected in | Postgres (default), etcd, k8s CRD |
| `CheckpointStore` | bbolt file (one `Set` = one txn = atomic) | Postgres table, etcd, object store + WAL |
| `Coordinator` | `singleNode{}` — always leader, all assignments local, leases no-ops | Postgres advisory locks + leases table, etcd election, or k8s `Lease` |
| `StatusAggregator` | direct in-process read | fan-out over gRPC, or workers write status rows |

Non-negotiables:

- **The connector-facing API is byte-identical in both modes.** Connect and Flink (MiniCluster vs YARN)
  converged on this independently. Anything that differs is a defect.
- **`CheckpointStore.Set` is atomic across the whole map.** This is the requirement Connect's compacted topic
  cannot meet and which produced its documented unrecoverable state. One SQL transaction, one bbolt
  transaction, one etcd `Txn`. **Kafka as the coordination store is explicitly rejected**: reproducing that
  failure mode in 2026 with a transactional database available would be perverse.
- **The leader only plans.** It writes assignment rows; it does not route data, proxy status, or hold anything
  the data path needs. **Therefore the data plane keeps running and keeps checkpointing with the entire control
  plane down**, because a worker holding a valid lease needs nothing from anyone until it expires. This is the
  single most important deployment property in the design and it is worth sacrificing elegance for. Connect
  approximates it; Airbyte historically did not.
- **The assignment lease is the fencing token.** The verified k8s caveat is decisive: *"This implementation does
  not guarantee that only one client is acting as a leader (a.k.a. fencing)"*, and clients infer leadership from
  **locally captured timestamps**. So leadership must never be trusted for correctness — every checkpoint write
  carries the lease/generation and the store rejects a stale one. That is the only safe use of leader election.
- **Postgres first, etcd second.** One dependency delivers every primitive: revisioned CAS
  (`UPDATE … WHERE revision=$2`), atomic multi-key writes (one txn), leader election
  (`pg_try_advisory_lock`, released on session loss), leases (a table + conditional update), work claiming
  without a leader (`SELECT … FOR UPDATE SKIP LOCKED`), `Watch` (`LISTEN`/`NOTIFY` with a `max(revision)` poll
  fallback), status aggregation (a table with a TTL column). etcd is a *better semantic* fit — `ModRevision`
  CAS and `Watch(fromRevision)` are literally the interface — so keep it as the **conformance target that stops
  the interface acquiring Postgres-isms**, not as a shipped dependency.
- **`canal run` with no arguments must produce a working pipeline with a durable checkpoint on a laptop with
  nothing installed.** That is R3's first milestone and canal's single biggest adoption lever over Connect and
  Airbyte. Singer's pipe is the best dev experience in the study; match it with
  `canal run --source X --sink jsonl` over the same registry, interfaces and checkpoint format as coordinated
  mode.
- **Root-config / stream-config split** (Benthos): service-wide observability and stores in one file, N
  pipeline documents managed as data over HTTP.
- **Disk buffers are worker-affine state** and their interaction with rescheduling must be decided explicitly,
  not discovered in production (Vector's documented sharp edge).
- **Separate "where state lives while running" from "where snapshots are durably written"** — Flink needed
  several releases to untangle state backend from checkpoint storage.
- **Ship exactly one snapshot/checkpoint format**, always self-contained and relocatable. Flink's
  canonical-savepoint / native-savepoint / aligned / unaligned matrix is an operator tax whose sharp edges
  ("aligned checkpoints are not relocatable", "you cannot upgrade minor versions from an unaligned
  checkpoint") are avoidable by never creating the matrix.
- **Operator-facing status is `Phase` + `Conditions[]`**, the k8s shape, because one enum provably cannot
  describe "running, healthy connection, 40 minutes behind, sink returning 429s": `Phase ∈ {Pending, Starting,
  Running, Paused, Stopping, Stopped, Failed, Completed}` (note `Completed`, which Connect lacks) plus
  `Condition{Type ∈ {Configured, Connected, Progressing, CaughtUp, Degraded, Assigned}, Status ∈
  {True,False,Unknown}, Reason, Message, LastTransitionTime, ObservedGeneration}`. **`Connected: True` must
  never be able to imply `Progressing: True`** — that separation is the machine-readable form of the honesty
  rule, and a fixture test can assert that a healthy connection with a stalled commit renders as unhealthy.
  `ObservedGeneration` answers "did my config change take effect?", which Connect's status API structurally
  cannot. `Stopping` carries `stoppingSince` and `drainDeadline`, and *drained* vs *drain-timeout* is a distinct
  event because the second means records may replay.
- **`canal_checkpoint_age_seconds` is the primary health metric**: always available, unfakeable, and one alert
  catches every stall mode. **Never ship a metric called `lag`** — ship the four separately-named quantities
  (event-time lag by stage, backlog records/bytes, position fraction, buffer depth) each documented as an
  explicit subtraction, per FLIP-33's discipline, and omit the series entirely when a quantity is unmeasurable.
- **Hard-close the metric label vocabulary to pipeline topology** and enforce at registration; serve unbounded
  per-stream detail from the read model, not from labels.
- **Be k8s-operator-ready without building an operator**: a `Phase`+`Conditions`+`ObservedGeneration` status
  document maps onto a CRD status subresource with no redesign.
- **Expose metrics over HTTP natively.** Connect is JMX-only and every deployment bolts on an exporter.
- **Ship a tap-any-edge live event sampler** and a `/ready` endpoint returning both a status code and a JSON
  status tree, plus per-stream counters with quantiles, so a single binary can serve its own dashboard with no
  Prometheus.
- **Instrument recovery itself in phases** (state load, restore, initialise, first record) — if
  restart-and-resume is a headline feature, restart time is a metric, not an assumption.

**Coupled to:** D3 (the store's atomicity/CAS/fencing requirements come from the checkpoint shape), D5
(assignments are the lease subject and the plan is durable state), D9 (capabilities are what let the core refuse
an impossible deployment), D10 (config storage and the descriptor feed the same UI), D12 (buffer durability is
worker-affine).

---

## Part 2 — The coupling graph

```
                              ┌──────────────────────────┐
                              │ D9 capability surface    │  ← constrains EVERY other decision's
                              │ (optional ifaces + data) │     ability to evolve
                              └───────────┬──────────────┘
                                          │ (all)
   D1 record ──────► D2 serialisation ────┴──► D10 config spec ──► D13 topology
      │  ▲                  │                        ▲   │              ▲
      │  └──── D8 schema ───┘                        │   └── D11 errors ┘
      │           │                                  │
      ▼           ▼                                  │
   D3 checkpoint representation ◄────────────────────┘
      │      ▲          ▲
      ▼      │          │
   D4 commit protocol   │
      │      ▲          │
      ▼      │          │
   D5 splits / planning ┴── D6 snapshot model ── D7 chunked-snapshot engine
      │                          │                     │
      ▼                          ▼                     ▼
   D12 flow control ─────────────┴──► D14 standalone↔distributed seam
```

### The chains that actually decide extensibility

**Chain A — the frontend chain (D9 → D10 → D8 → D14).**
The frontend goal is satisfied *only* if capabilities and config are **declarative data**. If any capability is
a Go callback or a type assertion the UI must instantiate a connector to learn about it, and if any config
metadata lives in code the UI needs per-connector knowledge. `D9 = data` forces `D10 = data` forces the
descriptor to be servable from the registry, which combined with D8's persisted catalog and D14's read model is
the entire frontend with zero per-connector UI code. **Break D9 and the frontend goal becomes N frontends.**

**Chain B — the checkpoint chain (D5 → D3 → D4 → D14).**
Choosing splits (D5) makes the reader's checkpoint its split list (D3), which makes positions per-split, which
makes the contiguous-prefix watermark valid *per split* (D4), which makes splits the assignment and lease
subject (D14). Conversely: **choose "no split concept" and you get Vector** — no horizontal scale for stateful
sources, checkpoints in local files that cannot be reassigned, and adding splits later is a breaking source
interface change. This chain is the reason D5 cannot be deferred to phase 2.

**Chain C — the snapshot chain (D5 → D6 → D7 → D4).**
Splits with declared boundedness (D5) make snapshot-vs-stream a property of the split set rather than a phase
enum (D6), which makes chunks *be* splits (D7), which makes chunk completion a checkpoint boundary (D4) and
chunk progress a generic metric. Break D5 and you get Benthos: snapshot phase as `pos == nil`, no
representation for "partially snapshotted", restart from zero.

**Chain D — the extensibility chain (D9 → D2 → D1).**
"Adding a sink is: implement three methods, register, done" holds only if serialisation is not the sink's job
(D2) and the record model is stable and generic (D1). Push encoding into sinks and every sink is 10× larger and
codec bugs multiply by connector count. Put CDC semantics *only* in metadata conventions (Benthos) and every
CDC-aware sink special-cases every source, which is constraint #1 violated by drift rather than by design.

**Chain E — the wire chain (D9 → D1 → D3 → D4).**
Constraint #3 (a future gRPC implementation satisfies the same interface) survives only if: every request and
response is a plain serialisable struct (D9), the record has a wire form (D1), the checkpoint is
`(version, bytes)` (D3), and the ack is small enough to become a correlation id (D4 — Benthos's `AckFunc(ctx,
err) error` → `uint64 batch_id`, which works *only* because the ack carries no other state). **A richer ack
breaks the wire chain.** This is the specific reason canal's position-carrying acknowledgement (D4) must be
"the sink confirms batch N" with the core holding the position mapping, rather than "the sink returns a
position object".

### Hard couplings worth stating as one-liners

- Stable per-record ID (D1) ⇒ per-record error routing, DLQ and dedup are cheap; **no** stable ID ⇒ ~200 lines
  of positional sort-group correlation (Benthos) and a public method marked `// Deprecated: This method is
  harmful`.
- Opaque checkpoint (D3) ⇒ new phases cost nothing (Airbyte's RFR) **but** lag/progress are impossible
  (Connect) — hence the typed header.
- Sink has no progress callback (D4) ⇒ a new sink cannot get checkpointing wrong; the core must therefore own
  the position mapping.
- Splits as the ordering scope (D5) ⇒ canal can be parallel **and** ordered; Benthos can be one at a time.
- Chunked snapshot (D7) ⇒ the sink **must** upsert by key; that is a submit-time capability check, not a
  runtime discovery.
- Schema epoch in the checkpoint (D3+D8) ⇒ no two-store divergence class of failure.
- Buffer owns acks when durable (D12) ⇒ the checkpoint store and the buffer need no shared WAL.
- Leader plans only (D14) ⇒ the data plane survives total control-plane outage; anything else and it does not.

---

## Part 3 — Traps (mistakes multiple systems made and had to undo)

1. **Conflating record identity with record payload.** Connect's SMTs mutate `topic`/`partition` while offset
   accounting needs pre-transform coordinates, so `SinkRecord` grew `originalTopic`/`originalKafkaPartition`/
   `originalKafkaOffset` (KIP-793) plus prose warnings in two javadocs. Fix: provenance is structurally
   immutable and identity is framework-assigned.
2. **Positional identity within a batch.** Benthos's `WalkMessages` is marked *"Deprecated: This method is
   harmful"* by its author; `Indexer`/`SortGroup` and ~60 lines of strict-mode gymnastics exist solely because
   records have no stable id. When correlation fails the fallback retries the whole batch — correct priority,
   visible cost.
3. **Wall-clock offset commits.** Connect's `offset.flush.interval.ms=60000` decoupled from batches and
   boundaries produces re-emitted chunks after crashes and KAFKA-4942's famous misdiagnosed log line. Fix:
   in-band, source-chosen checkpoint boundaries.
4. **Treating "snapshot has no state" as an economy.** Airbyte's full-refresh streams restarted from 0 and
   needed a protocol change (`is_resumable`, `CheckpointMixin`) to fix; Benthos still restarts a 500M-row
   snapshot from zero. Fix: every phase is checkpointable from day one.
5. **Smuggling phase into the opaque checkpoint.** Connect/Debezium and Airbyte did this independently, and
   both lost snapshot progress reporting, snapshot-specific parallelism and resumability at a different
   parallelism. Two independent occurrences is conclusive.
6. **One-shot work splitting.** Connect splits once at start; re-splitting restarts tasks; "20 workers for the
   snapshot, 2 for the stream" is inexpressible. `tasks.max` was advisory for eight years until KIP-1004 had to
   enforce it against "buggy or hostile connectors… threatening cluster stability".
7. **Stop-the-world rebalancing.** KIP-415 replaced it after documented rebalance storms; the replacement
   shipped its own imbalance bug (KAFKA-12495). Fix: pull-based assignment plus deliberately delayed
   reassignment.
8. **A compacted log as a control-plane state machine.** `KafkaConfigBackingStore` needs seven record types and
   a `commit-` marker to fake set-atomicity, and its own javadoc documents an unrecoverable
   compaction-plus-partial-write state. Fix: a transactional, revisioned store.
9. **Adding a method to a required interface.** Connect's `pluginMetrics()` forced
   `catch (NoSuchMethodError | NoClassDefFoundError)` into the official javadoc; Flink has five
   `default`-throwing `UnsupportedOperationException` methods and **three** Sink API rewrites; Benthos could not
   add `IncrFloat64` and carries `// TODO: V5` plus a runtime type-assertion fallback in the adapter. Fix:
   frozen required interfaces, new capabilities as new interfaces + declared data.
10. **Type parameters and nested interfaces in a plugin API.** FLIP-191 needed a whole new package because the
    `GlobalCommitter` could not be removed "due to the typed parameters"; FLIP-372 names inner-interface
    coupling as what prevented `TwoPhaseCommittingSink`'s evolution.
11. **Optional capability by type assertion, then trying to ship it over a wire.** Benthos's
    `ConnectionTestable` needs nine hand-written forwarders and its gRPC path had to demote `AutoRetryNacks` to
    a bool. Fix: capability = data, behaviour = interface, asserted in exactly one place.
12. **Unbounded retry as the default.** Benthos's auto-retry livelocks on a poison record; `switch` +
    `reject` produced a documented indefinite block that was patched with a *lint* and then had its default
    flipped in a major version. Fix: bounded retry with a named terminal disposition.
13. **Deep buffering.** Flink paid for it with FLIP-76 unaligned checkpoints *and* buffer debloating — two
    major features spent undoing an early default. Small bounded buffers remove an entire subsystem.
14. **A global mutable schema singleton.** Vector's `log_schema` is a documented wart with `LogNamespace` as the
    escape route `[recall-only]`.
15. **An untyped JSON object as the canonical record.** Airbyte deferred the type problem to dbt-based
    normalization, which became the worst part of the product and was eventually deleted `[recall-only]`.
16. **Silent capability degradation.** Vector's acknowledgement negotiation degrades to best-effort with **no
    error** even though every input to the decision is known at config time `[recall-only]`. Fix: declare the
    intended guarantee in pipeline config and validate it at submit time.
17. **A dedup/CAS primitive that is allowed to be unreliable.** Benthos's `Cache.Add`: *"It is okay for caches
    to return nil on duplicates if it isn't possible to implement"* — the one primitive that could underpin
    exactly-once is explicitly optional. Fix: a separate strict-CAS store for dedup and fencing.
18. **A terminal outcome that silently discards data.** Flink's `signalFailedWithKnownReason` "only logs the
    error, discards the committable and continues".
19. **Package/type names as a permanent wire contract.** Flink CDC's `…source.meta.wartermark` typo has been
    unfixable in a public path for years. Also: an opaque split-id string with structure encoded into it
    (`table:chunkID`), parsed back out in three places.
20. **Plugin discovery by scanning.** Connect is still on `plugin.discovery=HYBRID_WARN` years into migrating
    off classpath scanning. An init-time registry never has this problem.
21. **Modelling one entity twice.** The abandoned canal attempt modelled buffers both as stages 3/5/7 and as
    "segments" keyed by `followsStageOrdinal` 2/4/6, with a `kind` enum having exactly one permitted value.
    R1 and R9 exist because of it.

---

## Part 4 — Open questions this document does not close

1. **Conduit is unread and it is the closest prior art that exists.** It has already shipped the thing
   constraint #3 describes: one Go interface satisfied by both an in-process connector and a gRPC subprocess.
   D3 (opaque `Position` and who may interpret it), D9 (whether an `Unimplemented*` embed makes their interfaces
   additive-safe, and what the gRPC boundary cost them in context propagation / typed-error fidelity), and D13
   would all change materially. **Re-run that research before freezing interfaces.**
2. **Debezium verified nothing.** The watermark algorithm's exact steps, whether classic initial snapshot is
   resumable at all, the `Snapshotter` SPI's shape, and the entire "what they got wrong" half are unknown.
   D7 currently rests on Flink CDC alone (which is verified, so the recommendation stands, but the DDD-3
   cross-check is missing).
3. **Does the `Change` facet's op vocabulary need `before`-image support at all?** Benthos's Postgres input
   cannot always produce even a complete *after* image (`unchanged_toast_value` exists because REPLICA IDENTITY
   is not FULL). If before-images are unavailable from most real sources, the facet may be over-designed.
   Decide by surveying three intended sources before writing the type.
4. **Estuary-style reduction annotations as the alternative to an op enum.** If same-key documents combine
   deterministically, the snapshot→stream handoff dissolves entirely (D6 option (e)) and duplicate delivery
   becomes harmless. Unverified, and a large commitment. Worth one focused investigation because it would
   *simplify* D4, D6 and D7 simultaneously.
5. **Merge-patch checkpoints.** A source with thousands of partitions re-serialising all state per commit is a
   real scaling problem (Estuary's stated motivation). The per-split decomposition mitigates it; measure before
   adding patch semantics.
6. **Do Go 1.23 range-over-func iterators belong in the source interface?** They are ergonomic for a bounded
   scan but they invert control away from the runtime, cannot carry a per-batch ack, and cannot express
   "nothing available, come back when this channel closes". Provisional answer: **no** in the plugin interface
   (use `ReadBatch(ctx) (Batch, AckFunc, error)` with sentinel errors, plus a three-valued status if a
   non-blocking variant is needed), but **yes** as a *helper* the framework offers connector authors
   (`ReadBatchFromSeq(seq iter.Seq2[Record, error])`) so a simple scan-shaped source is a few lines. This needs a
   prototype before it is settled.
7. **Where does dedupe state live** (R5 demands durable, scoped, committed-after-write), and does it share the
   checkpoint's durability domain or need its own strict-CAS store? D3 says "not the cache"; it does not say
   what instead.
8. **What is the minimum viable `Discover` for a source with no catalog?** Making it required (D8) is a real
   tax on webhook/socket/metrics sources. A single-stream default implementation may be enough, but the ergonomic
   cost should be measured on a real trivial connector.
9. **Expression language or not.** Benthos's Bloblang makes lint rules, batch predicates, templated partition
   keys and Segment-style default mappings all *data* a browser can evaluate. Without one, each of those needs a
   bespoke closed vocabulary. This is a single decision with reach into D10, D12 and D13, and it is not costed
   here.
10. **Metric cardinality vs per-stream detail.** The recommendation is a hard-closed label vocabulary with
    per-stream detail served from the read model. Whether the UI's needs actually fit that split has not been
    tested against a concrete dashboard design.

---

## Appendix — the interface set implied by these recommendations

**Illustrative, not normative.** This exists to prove the fourteen recommendations are mutually consistent and
to make the wire-shippability property checkable by inspection. Names are placeholders. Nothing here is copied
from an unverified dossier.

```go
// ---- required surface: three verbs per role, frozen ----------------------------

type Source interface {
    Connect(ctx context.Context) error                       // retried with backoff by the core
    ReadBatch(ctx context.Context) (Batch, AckFunc, error)    // ErrNotConnected | ErrEndOfInput
    Close(ctx context.Context) error
}

type Sink interface {
    Connect(ctx context.Context) error
    WriteBatch(ctx context.Context, b Batch) error            // returning nil IS the acknowledgement
    Close(ctx context.Context) error
}

// The only progress primitive. Small enough to become an opaque uint64 over a wire.
type AckFunc func(ctx context.Context, err error) error

// ---- declared at construction, as data (never a tuple) -------------------------

type Capabilities struct {
    APIVersion   int
    Batching     BatchPolicy
    MaxInFlight  int
    Ordering     OrderingScope       // per-split | none
    DeliveryTier Tier                // at-least-once | 2pc | transactional
    Resumable    bool                // mid-split checkpointing is legal
    SplitOrdered bool                // records arrive in cursor order within a split
    Chunkable    bool                // implements SupportsChunkedSnapshot
    Replayable   bool                // implements SupportsReplay
    Comparable   bool                // implements PositionComparer
    SchemaChangeKinds []SchemaChangeKind
}
// Registration panics if a declared flag has no corresponding interface implementation.

// ---- optional capabilities: one interface each, asserted in exactly one place ---

type Planner            interface { Enumerate(ctx, ...) ; AddSplitsBack(...) ; Snapshot(ctx, id) (VersionedBlob, error) }
type SupportsChunkedSnapshot interface { SplitKeySpace(...) ; ReadRange(...) }
type SupportsReplay     interface { ReplayFrom(ctx, from, to Position) (...) }
type PositionComparer   interface { Fraction(from, to Position) Optional[float64] }
type Backlogger         interface { Backlog(ctx) (Backlog, error) }
type SupportsCommitter  interface { PrepareCommit(ctx) ([]Committable, error) ; Commit(ctx, []CommitRequest) error }
type SupportsWriterState interface { Snapshot(ctx, id) ([]VersionedBlob, error) ; Restore(ctx, []VersionedBlob) error }
type TransactionalSink  interface { WriteWithToken(ctx, Batch, token []byte) error ; RecoverToken(ctx) ([]byte, error) }
type SupportsSchemaApply interface { ApplySchemaChange(ctx, SchemaChange) error }
type ConnectionTestable interface { ConnectionTest(ctx) []TestResult }

// ---- everything durable or boundary-crossing is (version, bytes) ---------------

type VersionedBlob struct { Version int; Bytes []byte }

type Serialiser[T any] interface {
    Version() int
    Serialise(T) ([]byte, error)
    Deserialise(version int, b []byte) (T, error)   // handed the version it was written with
}

// ---- the checkpoint: opaque payloads, typed header ----------------------------

type Checkpoint struct {
    Header       CheckpointHeader                    // core-readable, closed vocabulary
    Shared       Optional[VersionedBlob]             // the one log position, if any
    Streams      map[StreamID]VersionedBlob
    Splits       []VersionedBlob
    Committables map[uint64][]VersionedBlob          // the subsuming-contract pending set
    SchemaEpoch  Optional[VersionedBlob]             // committed atomically with position
}

type CheckpointHeader struct {
    ID          uint64
    Generation  uint64          // = the worker's lease/fencing token
    Phase       Phase           // reporting only; never control flow
    CommittedAt time.Time       // → canal_checkpoint_age_seconds
    RecordsIn   int64
    RecordsOut  int64           // per-checkpoint reconciliation
}
```

**Wire-shippability check.** Every method above is `(ctx, serialisable) → (serialisable, error)`. No `Class`
handles, no closures in a request or response, no futures, no behavioural schema objects, no interface-typed
fields. The one closure — `AckFunc` — has signature `(ctx, err) error` and carries no state, so it becomes an
opaque correlation id over a wire, exactly as Benthos's proto demonstrates. Connect fails this check on five
counts, which is why no out-of-process Connect exists; Benthos passes it, which is why its gRPC boundary
satisfies the same interfaces.

# Prior art: Debezium

> **STATUS: DRAFT — RESEARCH MAP ONLY. NOT ARCHAEOLOGY. DO NOT CITE AS NORMATIVE.**
>
> This file does **not** contain the deliverable it was commissioned to contain. The run that
> produced it had **zero access to primary source**: every network-capable tool path
> (`curl` via Bash, WebFetch, WebSearch, Monitor, and the browser tools) failed for the whole
> session with an upstream safety-classifier outage —
> `"claude-sonnet-5 is temporarily unavailable, so auto mode cannot determine the safety of
> <tool> right now"` — across ~45 attempts spanning the full run. Only local file reads worked.
>
> The commission was explicit that **a hallucinated interface signature is the worst possible
> output**, and `docs/design-rules.md` R12 forbids documents that are normative and draft at once.
> So this file contains **no fenced Java code purporting to be a real signature**, because I could
> not read one. What it contains instead is a *precise, cheap-to-execute fetch plan* — exact URLs,
> exact symbols to locate, and the exact question each section must answer — plus clearly fenced
> conceptual recollection that is **explicitly not evidence**.
>
> **Re-run this research before any of it informs an interface decision.** Everything below marked
> `[UNVERIFIED]` is recollection, not a reading, and must be replaced by quoted source.

---

## How to complete this dossier

Cached-source convention, matching `kafka-connect.md`: fetch into
`<scratchpad>/dz/`, filenames = upstream path with `/` → `_`
(e.g. `pipeline_spi_OffsetContext.java`). Pin the ref: use a **tag**, not `main`
(`https://raw.githubusercontent.com/debezium/debezium/v<X.Y.Z>.Final/...`), and record the exact
tag in the `## Scope` section, because Debezium's snapshot SPI has been refactored repeatedly and
`main` will not match any release a reader has.

### Primary sources to fetch, in priority order

**A. Core pipeline SPI** (repo `debezium/debezium`, module `debezium-core`, root
`debezium-core/src/main/java/io/debezium/`):

| Path under root | Why it matters |
|---|---|
| `pipeline/spi/OffsetContext.java` | The checkpoint interface. Sections 3, 4. |
| `pipeline/spi/Partition.java` | Stream identity. Section 3. |
| `pipeline/spi/ChangeRecordEmitter.java` | Record production. Section 2. |
| `pipeline/spi/SnapshotResult.java` | Snapshot outcome type + status enum. Section 4. |
| `pipeline/source/spi/ChangeEventSource.java` | The shared supertype + `ChangeEventSourceContext`. Sections 1, 6. |
| `pipeline/source/spi/SnapshotChangeEventSource.java` | Snapshot phase interface. Sections 1, 4. |
| `pipeline/source/spi/StreamingChangeEventSource.java` | Streaming phase interface. Sections 1, 4. |
| `pipeline/source/spi/ChangeEventSourceFactory.java` | How the two phases are constructed. Sections 1, 10. |
| `pipeline/source/spi/SnapshotProgressListener.java` | Progress callbacks → the UI. Sections 4, 11. |
| `pipeline/source/spi/DataChangeEventListener.java` | Per-event metering hook. Section 11. |
| `pipeline/ChangeEventSourceCoordinator.java` | **The single most important file.** Owns the snapshot→stream handoff. Sections 4, 6. |
| `pipeline/EventDispatcher.java` | Where records, schema changes and heartbeats converge. Sections 2, 5. |
| `pipeline/source/AbstractSnapshotChangeEventSource.java` | Template method for snapshotting. Section 4. |
| `relational/RelationalSnapshotChangeEventSource.java` | The lock/isolation/consistent-point logic. Section 4. |
| `pipeline/source/snapshot/incremental/` (whole dir) | Incremental snapshot: `IncrementalSnapshotChangeEventSource`, `IncrementalSnapshotContext`, `AbstractIncrementalSnapshotChangeEventSource`, `SignalBasedIncrementalSnapshotChangeEventSource`. Section 4. |
| `pipeline/signal/` (whole dir) | Signal channel SPI (`SignalChannelReader`, `SignalAction`, `SignalPayload`). Sections 4, 6. |
| `pipeline/notification/` (whole dir) | Notification/progress API (`Notification`, `NotificationService`, channel SPI). Sections 4, 11. |
| `schema/DatabaseSchema.java`, `schema/HistorizedDatabaseSchema.java` | Schema state. Section 5. |
| `relational/history/SchemaHistory.java` | The schema-history store SPI. Sections 3, 5, 12. |
| `connector/common/BaseSourceTask.java` | Kafka Connect adaptation + error/retry handling. Sections 6, 10. |
| `connector/common/CdcSourceTaskContext.java` | Metrics/context plumbing. Section 11. |
| `config/Field.java`, `config/Configuration.java`, `config/CommonConnectorConfig.java` | Config model — is it `ConfigDef` or bespoke? Section 7. |
| `pipeline/metrics/` (whole dir) | Metric MBeans + naming. Section 11. |
| `data/Envelope.java` | **The record envelope.** Section 2. |
| `snapshot/SnapshotterService.java` / `spi/snapshot/Snapshotter.java` | Snapshot-mode SPI (recent refactor — confirm actual path). Sections 4, 7. |
| `engine/DebeziumEngine.java` (module `debezium-api`, root `debezium-api/src/main/java/io/debezium/engine/`) | Embedded API. Sections 1, 6, 10. |
| `engine/ChangeEvent.java`, `engine/RecordChangeEvent.java`, `engine/format/*` | Embedded record + output formats. Sections 2, 10. |

**B. Design documents** (repo `debezium/debezium-design-documents`, raw path
`https://raw.githubusercontent.com/debezium/debezium-design-documents/main/DDD-N.md`):

- **DDD-3** — incremental snapshotting. *This is the single most valuable document for canal.*
- Enumerate the directory listing (`https://api.github.com/repos/debezium/debezium-design-documents/contents/`)
  and read every DDD whose title mentions snapshot, signal, notification, offsets, or schema history.
  At minimum look for: read-only incremental snapshots, blocking snapshots, the snapshot-mode SPI
  refactor, and the notification API.

**C. The upstream research Debezium's design cites**:
- Netflix DBLog paper — *"DBLog: A Watermark Based Change-Data-Capture Framework"*,
  Andreas Andreakis & Ioannis Papapanagiotou, arXiv **2010.12597**
  (`https://arxiv.org/abs/2010.12597`). DDD-3's watermark algorithm is derived from this.
  Read §"Watermark based Chunk Selection" and quote the algorithm verbatim.

**D. Documentation** (`https://debezium.io/documentation/reference/stable/...`):
- `connectors/postgresql.html`, `connectors/mysql.html` — the two canonical snapshot-mode tables
  and the `source` block field list. Diff them: **what differs between connectors is exactly what
  canal must not put in core.**
- `configuration/signalling.html`, `configuration/notification.html`
- `operations/debezium-server.html`, `operations/embedded.html`
- `integrations/*` for the schema-history topic semantics.

**E. Failure modes** — Jira `https://issues.redhat.com/projects/DBZ`, plus the
`debezium/debezium` GitHub issues. Search terms that will find the real pain:
`snapshot stuck`, `incremental snapshot not resuming`, `schema history topic`,
`Encountered change event ... whose schema isn't known`, `snapshot.mode=schema_only recovery`,
`offset flush timed out`, `signal table permissions`, `WAL bloat replication slot`.

---

## 1. Core interfaces

**Question to answer:** what is the literal method set of the phase interfaces, and how do the two
phases share state?

**What must be quoted:** `ChangeEventSource` (and its nested `ChangeEventSourceContext` — the
cancellation/liveness token), `SnapshotChangeEventSource.execute(...)`,
`StreamingChangeEventSource.execute(...)`, `ChangeEventSourceFactory`'s
`getSnapshotChangeEventSource(...)` / `getStreamingChangeEventSource(...)` /
`getIncrementalSnapshotChangeEventSource(...)`, and the generic parameters on all of them.

`[UNVERIFIED]` The shape I expect to find, stated as a *hypothesis to test*, not a finding:
Debezium's source-side abstraction is **not** one `poll()`-style interface. It is a **two-phase
model with a coordinator**: separate `SnapshotChangeEventSource` and `StreamingChangeEventSource`
interfaces, both `execute(...)`-style and both **blocking/push** (they run to completion or until
cancelled and push into an `EventDispatcher`) rather than pull. A `ChangeEventSourceCoordinator`
owns sequencing. Both are parameterised over connector-specific `Partition` and `OffsetContext`
types via generics.

**Why this matters to canal, if confirmed:** it is the opposite of Kafka Connect's single
`poll()`. Phases become *first-class named things in core* rather than hidden inside a connector's
`poll()` — which is exactly the gap `kafka-connect.md` §"Snapshot Handling" identifies. It also
means the core, not the connector, decides *whether* to snapshot, and can report which phase is
running. **Confirm the generic signature precisely** — whether the phase interfaces are generic
over `<P extends Partition, O extends OffsetContext>` is load-bearing for whether canal should
use Go type parameters here or an opaque `[]byte` checkpoint. Constraint 5 (no gratuitous
generics) turns on this answer.

**Also record:** there is no `Sink` interface in Debezium core at all — the sink is Kafka Connect's
or Debezium Server's. Section 10 must state this plainly; it is a structural difference from canal.

## 2. Record model

**Question:** what exactly is in the envelope, and how much of it is generic vs relational?

**Must quote:** `io.debezium.data.Envelope` — its field-name constants, its `Operation` enum and
each enum's wire code, and its schema-builder API. Then the `source` info block from
`AbstractSourceInfoStructMaker` / `CommonConnectorConfig` plus **two** connector-specific
`SourceInfo` classes (MySQL and Postgres) side by side.

`[UNVERIFIED]` Expected: a `before` / `after` / `source` / `op` / `ts_ms` / `transaction`
envelope, with `op` a short string code (create/update/delete/read, where the snapshot-read code
is distinct from insert — **verify the exact letters**), and deletes emitted as a delete event
optionally followed by a tombstone. Key is the primary key struct; value is the envelope.

**The archaeology that actually matters here:** the split between the **generic** part of `source`
(connector, name, ts_ms, snapshot, db, table) and the **per-connector** part (MySQL: file/pos/gtid;
Postgres: lsn/txId/sequence; Mongo: resume token). Quote both and diff them. That diff is the
empirical answer to canal's constraint 1: it shows precisely which fields a source-agnostic core
can own and which must be connector-opaque. Do not generalise from one connector.

**Question canal must answer from this:** Debezium's `op` field proves a generic framework can
carry an operation kind without becoming Mongo-shaped (cf. `kafka-connect.md`: Connect's omission
of `op` is why every CDC connector reinvents the envelope). Record whether `op` lives in *core* or
in a per-connector schema maker.

## 3. Checkpoint model

**Question:** why are Debezium offsets *structured maps* rather than opaque bytes, and what does
that buy that opacity does not?

**Must quote:** `OffsetContext` in full (particularly its `getOffset()` return type and its
`getSourceInfoSchema()` / event-tracking methods, if those are what they are called),
`Partition.getSourcePartition()`, and `OffsetContext.Loader` (the restart path — this is the
critical one: it is the deserialiser that turns a stored map back into a live context).

`[UNVERIFIED]` Expected: `Partition` → the `Map<String,?>` Connect source partition;
`OffsetContext` → a live, mutating object whose `getOffset()` projects to a
`Map<String, primitive>` for Connect to store; a nested `Loader` interface per connector to
rehydrate. Snapshot state (in-progress flag, last-key, chunk window) is *inside* that same map.

**The interface-design finding to extract (verify, then state):** offsets are structured because
the framework needs to **read** them, not just store them — to decide snapshot-vs-stream on
restart, to compute progress, and to expose them to operators. `kafka-connect.md` already
identifies the cost of opacity: "there is no source-side lag metric … a direct consequence of the
framework not understanding the offset map it stores." Debezium is the natural experiment on the
other side. Quantify it: list every decision Debezium makes *by inspecting* offset contents.

**Must also cover:** the two-store problem. Offsets live in the Connect offset store; **schema
history lives in a completely separate store** (`SchemaHistory` SPI / schema-history topic). These
are committed independently and can therefore diverge. Establish from source and from Jira whether
there is any atomicity between them, and what happens on restart when they disagree. **This is the
single most transplantable failure lesson for canal** — see §13 and cross-reference design-rules
R4 (an acknowledgement means durable) and open decision #1 (whether checkpoint state shares the
buffer WAL for crash consistency).

**Must also record:** commit timing is inherited from Kafka Connect
(`offset.flush.interval.ms`), i.e. **wall-clock, not chunk-boundary** — verify this is still true,
and whether Debezium does anything to align commits with snapshot chunk completion.

## 4. Snapshot handling

This is the section the commission cares most about. Four distinct sub-problems; keep them
separate.

### 4a. Snapshot modes as a first-class connector concern

**Must quote:** the mode enum(s) and, critically, the `Snapshotter` SPI if the refactor has landed
(look for `spi/snapshot/Snapshotter.java` and `SnapshotterService`). Then quote the mode tables
from *both* the MySQL and Postgres docs.

`[UNVERIFIED]` Expected modes include `initial`, `never`, `when_needed`, `schema_only`,
`initial_only`, plus newer additions (`no_data`, `recovery`, `configuration_based`, and a custom
mode) — **the set differs per connector and has been renamed across versions; do not assert a
canonical list without reading both docs at the pinned version.**

**The finding to extract:** the modes are not a boolean. They are a small decision matrix over two
independent axes — *(does data get snapshotted?)* × *(does schema get snapshotted?)* × *(is
streaming entered afterwards?)*. `schema_only` exists precisely because schema and data are
separable. `initial_only` exists because "batch pipeline" is a legitimate terminal outcome, and it
implies a **completion state** — which Kafka Connect lacks (`kafka-connect.md`: "no `COMPLETED`
state"). Verify whether Debezium can actually report completion, or whether the task just idles.

**Then note the refactor itself as evidence:** modes started as an enum switched on inside each
connector and were later extracted into a `Snapshotter` SPI. If confirmed, that is a documented
admission that a mode enum in core is the wrong shape — directly relevant to canal's
"zero switch statements" criterion (constraint 4).

### 4b. Blocking initial snapshot → streaming handoff

**Must read:** `RelationalSnapshotChangeEventSource` (the ordering of: open transaction → set
isolation level → acquire lock → **read the current log position** → release lock → read rows →
begin streaming from the recorded position), and `ChangeEventSourceCoordinator` for who calls what.

**The question to answer precisely:** *where does the streaming start position come from?* Not
"after the snapshot" — the exact ordering. Quote the code. The invariant is that the log position
is captured **at the snapshot's consistent point**, before or atomically with row reading, so the
stream replays everything that happened during the snapshot. Rows read during the snapshot are
therefore re-delivered as change events → the pipeline is at-least-once and **the sink must be
idempotent by key**. State that explicitly; it is a hard requirement canal inherits, and it
connects to design-rules "three layers of idempotency".

**Also capture:** the lock story per connector — global read lock vs table-level vs none, the
`snapshot.locking.mode` options, and the "no locks, rely on MVCC/repeatable-read" path. Note that
the *existence* of a locking mode knob is a per-connector concern that must not reach canal's core.

### 4c. Incremental snapshots (DDD-3 / DBLog watermarks)

**This is the highest-value single artefact.** Read DDD-3 and arXiv 2010.12597 and quote the
algorithm step by step.

`[UNVERIFIED]` Expected mechanism, to be replaced with quoted text: the table is chunked by
primary key; for each chunk the source (1) writes a **low watermark** into a signal/watermark
table, (2) `SELECT`s the chunk, (3) writes a **high watermark**, then (4) as the live stream passes
through the window between the two watermarks, it **removes from the chunk buffer** any row whose
key also appeared in the stream — so the streamed (newer) version wins and the buffered snapshot
row is dropped. The de-duplicated remainder is emitted as read events. Net effect: a consistent
chunk snapshot **with no locks and without stopping the stream**.

**Why this is the idea to steal:** it solves *snapshot concurrent with streaming* using only
(a) an ordered stream, (b) a way to inject a marker into that stream, and (c) key-based
comparison. All three are expressible source-agnostically. Verify whether Debezium's
implementation depends on anything relational beyond "there is a key and an ordered log" — if it
does not, canal can put watermark reconciliation in core.

**Must also capture:** the read-only variant (which avoids needing a writable signal table by
using the log's own position as the watermark) — this matters because requiring write access to
the source is a deployment tax, and canal will hit the same objection.

### 4d. Signals, resumability, chunking, parallelism

**Must quote:** the signal SPI (`SignalChannelReader`, `SignalAction`) and the action names
(`execute-snapshot`, `stop-snapshot`, `pause-snapshot`, `resume-snapshot`, and the blocking-snapshot
action — confirm exact strings), plus the channel implementations (source signal table, Kafka
topic, JMX, file).

**The finding:** Debezium had to invent an **out-of-band control channel** because Kafka Connect
has no way to command a running task (`kafka-connect.md` confirms this from the Connect side).
canal should have an **in-band control plane from day one** — a first-class `Command` path into a
running pipeline, with the same commands (start/stop/pause/resume a snapshot of a named set of
streams) — rather than discovering the need later and bolting on a signal table.

**Resumability:** establish from `IncrementalSnapshotContext` exactly what is persisted (the
pending-table list, the current chunk's key window, the last-processed key) and confirm the
docs' claim that this survives restart. Then note the sharp edge: it is persisted **inside the
offset map**, subject to the `Map<String, primitive>` restriction, so complex state gets
string-encoded. That is a keyhole canal can simply not have — but only if canal's checkpoint type
is a length-prefixed opaque blob *plus* a small typed, core-readable header (see §14).

**Parallelism:** determine honestly whether incremental snapshot chunks can be processed
concurrently or are strictly serial per connector task. Do not assume.

## 5. Schema handling

**Must quote:** `DatabaseSchema` / `HistorizedDatabaseSchema`, `SchemaHistory` (the SPI, its
`record(...)` and `recover(...)` methods), and the schema-change event structure.

**The three findings to establish:**

1. **Schema state is checkpointed, separately from offsets.** The historized connectors replay the
   schema-history store on startup to rebuild the DDL state as of the stored offset, because a log
   event from the past can only be decoded with the schema *as it was then*. Quote the recovery
   path.
2. **Schema changes are both in-band and out-of-band.** They are emitted as schema-change events
   (to a separate topic) *and* used internally to update the in-memory schema. Establish which is
   authoritative.
3. **The two-store divergence is the top operational failure.** Find the canonical error text
   (something in the family of "Encountered a change event whose schema isn't known") and the
   documented recovery procedure (`snapshot.mode=recovery` / rebuilding the history topic). Quote
   it. This is the concrete pain §13 needs.

**For canal:** if a source needs historical schema to decode historical log entries, then
*schema is part of the checkpoint*, not a side table — and it must be committed atomically with
position. Debezium's two independent stores are the counter-example to learn from.

## 6. Lifecycle

**Must quote:** `ChangeEventSourceContext` (the `isRunning()`-style liveness check that phase
implementations must poll), `BaseSourceTask`'s start/stop/poll and its retry logic, and
`ChangeEventSourceCoordinator`'s start/stop.

`[UNVERIFIED]` Expected: cancellation is **cooperative polling of a context predicate**, not a
`Context`-with-deadline. Java has no `context.Context`; canal does, and should pass it into every
blocking phase method. Confirm whether there is any deadline/timeout carried with cancellation
(I expect not) and say so.

**Error taxonomy — the specific thing to extract:** find the retriable-vs-fatal classification.
Look for `RetriableException` handling in `BaseSourceTask`, `errors.max.retries` /
`retriable.restart.connector.wait.ms` config, and any per-connector exception-mapping predicate
(e.g. a `isRetriable(Throwable)` hook). Record: how many classes are there really? Is it two
(retriable / fatal) like Connect, or richer? Does a connector get to *classify its own* errors?

That last question is the important one. canal's design-rules already commit to a **seven-class**
taxonomy (transient-upstream, transient-internal, permanent-upstream, permanent-mapping,
permanent-contract, duplicate-idempotent-success, clock-skew). Debezium's classification is the
realistic baseline to beat, and the mechanism by which a *connector* declares a class — rather
than the core guessing from an exception type — is the transplantable part.

## 7. Config model

**Must quote:** `io.debezium.config.Field` — every field of it — and compare against Kafka
Connect's `ConfigDef.ConfigKey` (already fully documented in `kafka-connect.md` §"Config Model",
fourteen public final fields including `group`, `orderInGroup`, `width`, `displayName`,
`dependents`, `Recommender`). Then find how `Field` groups are declared and whether Debezium
exposes validation results per-field.

`[UNVERIFIED]` Expected: Debezium has its **own** `Field` abstraction with validators and
dependents, which it converts to a Connect `ConfigDef` at the boundary — i.e. it did **not** adopt
`ConfigDef` as its internal model. Establish why (probably: it needs config outside Connect, for
the embedded engine and Debezium Server). If true, that is direct evidence for canal: **the config
schema must be owned by the framework core and be transport-independent**, renderable to a UI, a
CLI, and a file loader alike.

**Specifically check for a UI-facing group/order/width/display concept.** If Debezium's `Field`
lacks the presentational metadata that `ConfigDef.ConfigKey` has, note it — because canal needs
that metadata (goal: "a frontend driven by what connectors declare about themselves").

## 8. Backpressure

**Must read:** `ChangeEventQueue` (in `debezium-core`, likely `io.debezium.connector.base`) — its
bounding parameters (`max.queue.size`, `max.batch.size`, `max.queue.size.in.bytes`,
`poll.interval.ms`) and its blocking behaviour on enqueue.

**The finding to establish:** unlike Kafka Connect's source path (which has *no* flow control —
see `kafka-connect.md` §"Backpressure": `poll()` is re-invoked the instant the prior batch reaches
the producer), Debezium interposes a **bounded blocking queue** between the change-event source
threads and `poll()`. If confirmed, this is the mechanism by which a push-style source gets
backpressure for free: the producer blocks on enqueue, which propagates all the way back to
"stop reading the WAL".

Verify: does it block, or drop, or grow? Is the byte-based bound advisory or enforced? What
happens to a source that cannot be paused mid-transaction? And critically — **does blocking the
queue risk the upstream connection timing out or the replication slot growing unboundedly?** That
tension (backpressure vs. upstream resource retention) is real and canal will inherit it: a source
that stops reading may cause the *source system* to accumulate WAL. Record whether Debezium
documents this.

This maps directly onto design-rules R6 ("bounded by construction … backpressure is an
expressible outcome — block or reject — not unbounded growth to OOM") and open decision #3.

## 9. Delivery guarantees

**Establish and state plainly:** at-least-once, with duplicates on restart, in both the snapshot
and streaming phases. The snapshot-window re-delivery in §4b is not a bug, it is the design.
Confirm from docs (there is a FAQ/normative statement to quote).

**Then check:** whether Debezium supports Connect's exactly-once source (KIP-618) and under what
restrictions — and whether the *snapshot* phase can participate in producer transactions at all.
Also record the transaction-metadata feature (BEGIN/END marker events with event counts per
collection), because that is Debezium's answer to "how does a consumer reconstruct source
transaction boundaries" — a genuinely useful, source-agnostic idea if the source has a transaction
id.

**Dedup:** establish that there is none in the framework; it is pushed to the sink, keyed by
primary key. Note the implication for canal per design-rules R5 (dedupe keys scoped and committed
after the write).

## 10. Plugin boundary

**Must establish three distinct boundaries and not conflate them:**

1. **Kafka Connect mode** — a Debezium connector is a Connect `SourceConnector` plugin: JAR on
   `plugin.path`, classloader-isolated, reflectively instantiated. All of
   `kafka-connect.md` §"Plugin Boundary" applies, including the Java-ABI sharp edge.
2. **Embedded / Debezium Engine mode** — `io.debezium.engine.DebeziumEngine` in `debezium-api`.
   **Must quote its builder API in full**: `create(<format>)`, `.using(Properties)`,
   `.notifying(Consumer<...> | ChangeConsumer)`, `.using(CompletionCallback)`,
   `.using(ConnectorCallback)`, `.build()`, and the `Runnable`/`Closeable` execution contract.
   Also quote `DebeziumEngine.RecordCommitter` and `ChangeConsumer.handleBatch(...)` — the
   committer is the interesting part, because **it is where the embedded consumer acknowledges
   durability and thereby advances the checkpoint.** That is precisely canal's sink-ack →
   checkpoint edge, and design-rules R4 makes it the highest-stakes interface in the system.
   Get this signature exactly right.
   Also record the `format(...)` type-level output selection (`Json`, `Avro`, `Connect`,
   `CloudEvents`, `Protobuf` …) — a codec choice expressed in the *type system* at engine
   construction, which is a real design idea for canal's pluggable codecs.
3. **Debezium Server** — a Quarkus app wrapping the engine, with its own **sink SPI**. Find it
   (`debezium-server` repo, look for `DebeziumEngine.ChangeConsumer` implementations and the
   `debezium.sink.type` dispatch). List the shipped sinks (Kinesis, Pub/Sub, Pulsar, Event Hubs,
   Redis, NATS, Pravega, HTTP, RabbitMQ, Kafka, Infinispan, RocketMQ — **verify**). Establish
   whether that sink interface is a *framework* interface or an app-level one.

**The finding for canal:** Debezium's core is deliberately **not** coupled to Connect — the same
`ChangeEventSource` machinery runs under Connect, under the embedded engine, and under Debezium
Server. That three-way reuse is only possible because the core's interfaces are expressed in
Debezium's own types (`Envelope`, `OffsetContext`, `Field`) and adapted at the edges. **That is
exactly canal's constraint 2 (own the interfaces, no upstream lock-in), validated by a system that
did it.** Quote the layering.

**Out-of-process:** establish there is none, and assess whether the interfaces could survive it.
Apply the same five-point test used in `kafka-connect.md` §"Out-of-process": are there `Class`
handles, untyped `Object` payloads, behavioural (non-data) schema objects, in-process futures, and
missing cancellation context? Debezium's `OffsetContext` being a *live mutating object* rather than
a value is the likely blocker — record that, because it is the trap canal must avoid to satisfy
constraint 3.

## 11. Observability

**Must enumerate from `pipeline/metrics/`** the three MBean groups and every attribute name.

`[UNVERIFIED]` Expected: JMX MBeans split into **snapshot** metrics, **streaming** metrics, and a
**schema-history** MBean, named
`debezium.<connector>:type=connector-metrics,context=<snapshot|streaming|schema-history>,server=<name>`
— verify the exact ObjectName pattern.

**The attributes to hunt for, because they are the ones a UI actually needs and Connect lacks:**
`TotalTableCount` / `RemainingTableCount` / `SnapshotCompleted` / `SnapshotAborted` /
`SnapshotDurationInSeconds` / `RowsScanned` (per-table map!) / `ChunkId` / `ChunkFrom` / `ChunkTo`
for snapshots, and `MilliSecondsBehindSource` / `Connected` / `NumberOfCommittedTransactions` /
`SourceEventPosition` / `LastTransactionId` for streaming, plus queue-depth gauges.

**The finding:** `MilliSecondsBehindSource` and `RemainingTableCount` are exactly the two metrics
`kafka-connect.md` identifies as structurally impossible in Connect ("no source-side lag metric …
a direct consequence of the framework not understanding the offset map it stores"). Debezium can
produce them **because** its offsets are structured and its phases are named. That is the
observability argument for canal's checkpoint design, and it should be stated as such: *a core that
understands position and phase can compute lag and progress generically, for every connector,
with no connector-specific UI code* — which is precisely canal's frontend goal.

**Also capture:** the **notification API** (`pipeline/notification/`). Quote the `Notification`
type (id, aggregate type, type, additional data, timestamp) and the channel SPI (SinkNotification
channel, JMX channel, log channel). Note the notification *types* for snapshot progress
(started / in-progress / table-scan-completed / completed / aborted / paused / resumed).

**This is a distinct idea from metrics and canal needs both:** metrics answer "how fast / how far
behind", notifications answer "what discrete thing just happened, with structured payload". A
snapshot is a long-running job with lifecycle events, and gauges alone cannot represent it.
Debezium adding notifications *after* metrics is evidence that gauges were insufficient.

**Health/status:** determine whether Debezium has its own connector state machine or inherits
Connect's `{UNASSIGNED, RUNNING, PAUSED, FAILED, ...}`. canal's open decision #7 wants
`healthy → degraded → paused → terminal` with a last-error surface; record what Debezium actually
offers as the realistic baseline.

## 12. Deployment

**Establish:** Debezium has **no distribution model of its own**. Scaling is entirely inherited:
Connect distributed mode (see `kafka-connect.md` §"Deployment" for the full assignor / config-topic
/ rebalance analysis), or one Debezium Server process, or one embedded engine.

**The critical constraint to verify and state loudly:** most Debezium connectors are
**single-task** — `tasks.max` is effectively 1 for log-based CDC because a database's transaction
log is a single ordered stream with a single cursor. Confirm this from the docs per connector
(and note which connectors are the exception).

**The consequence for canal:** horizontal scale-out for log-based CDC is *not* achievable by
splitting the stream; it is achievable only by (a) partitioning by table/collection across
independent pipelines, or (b) parallelising the *snapshot* phase while keeping the stream serial.
That asymmetry — **snapshot is embarrassingly parallel, streaming is inherently serial** — is a
first-class architectural fact and canal's work-assignment model must express "this phase wants N
workers, that phase wants 1". `kafka-connect.md` notes Connect literally cannot express this
("work is split exactly once, at connector start … 'snapshot with 20 workers then stream with 2'
is inexpressible"). Debezium is the proof that the requirement is real.

**State stores to enumerate:** Connect offset store (offsets), schema-history store (schema),
and for Debezium Server the file/Redis-backed equivalents. Note that Debezium Server's standalone
offset store is a **file** — the same standalone/distributed storage-swap seam canal wants.

## 13. What they got right / what they got wrong

### Right (verify each before asserting)
- Two named phases with a coordinator, rather than one undifferentiated `poll()`.
- Structured, framework-readable offsets — enabling lag, progress, and restart decisions.
- Snapshot modes as a declared, connector-scoped policy, later extracted into an SPI.
- Watermark-based incremental snapshot: no locks, no stream interruption, resumable, chunked.
- An out-of-band control channel with a pluggable channel SPI (table / topic / JMX / file).
- A structured notification API for long-running-job lifecycle, separate from gauges.
- Core interfaces owned by Debezium, adapted to three different runtimes (Connect, embedded,
  Server) — no upstream framework lock-in.
- A bounded blocking queue giving a push-style source real backpressure.
- Transaction-boundary metadata events for consumers that need source transaction grouping.

### Wrong — **this half must be documented pain with citations, and I could verify none of it.**
Hunt for and quote, from Jira/GitHub/mailing list, at minimum:
1. **The schema-history / offset divergence class of failure.** Two independently-committed stores
   that must agree. Find the canonical error and the recovery runbook. Assess how often users hit
   it.
2. **Snapshot state through the `Map<String, primitive>` keyhole** — complex chunk state
   string-encoded because the offset store cannot hold structure.
3. **Snapshot-mode taxonomy churn** — modes renamed/added/deprecated across versions
   (`schema_only` → `no_data`, etc.). A vocabulary that changed repeatedly is direct evidence for
   design-rules R9 (one concept, one vocabulary). Get the actual rename history.
4. **The signal table as a deployment tax** — requiring write access (and DDL) on the *source*
   database, which many operators refuse; hence the read-only variant. Find the objections.
5. **Blocking initial snapshot on large tables** — long-running, non-resumable in the original
   design (verify whether classic initial snapshot is resumable *at all*, or whether restart means
   starting over; I believe the classic path is **not** chunk-resumable and that this was a primary
   motivation for DDD-3 — confirm).
6. **Single-task scaling ceiling** and the operational workarounds.
7. **Replication-slot / WAL growth** when the connector is slow, stopped, or backpressured — the
   backpressure-vs-upstream-retention tension from §8, as experienced.
8. **Offset flush timeouts** inherited from Connect (`kafka-connect.md` documents KAFKA-4942 and
   the "Failed to flush, timed out …" log line).
9. **`OffsetContext` as a live mutating object** shared between phases — a leaky abstraction that
   couples snapshot and streaming implementations and blocks any out-of-process boundary.
10. Any **deprecated abstraction or rewrite**: the `Snapshotter` SPI extraction, the signal/
    notification channel SPI generalisation, `SchemaHistory` renames. Each rewrite is a design
    lesson already paid for by someone else.

## 14. Steal this

Conceptual, and **contingent on the verification above**. Each is written as a claim to test, not
a settled conclusion.

1. **Name the phases in core.** A pipeline has declared phases (snapshot / stream / batch), the
   core sequences them and reports which is active. Do not hide the phase inside a connector's
   read loop — that single choice is what makes progress, completion, and lag reportable
   generically, and it is exactly what Kafka Connect cannot do.
2. **Make the checkpoint opaque-with-a-typed-header.** A small core-readable header
   (phase, stream id, a comparable position token, snapshot-in-progress flag, schema epoch) plus
   an opaque connector blob. Full opacity costs you lag and progress metrics; full structure costs
   you the `Map<String, primitive>` keyhole. Debezium and Connect are the two failure modes at
   either extreme.
3. **Commit schema state and position atomically, in one checkpoint record.** If decoding a
   historical event needs the historical schema, schema *is* checkpoint state. Debezium's two
   independent stores are the counter-example — and design-rules R4 and open decision #1 already
   point here.
4. **Adopt watermark-based chunked snapshotting (DBLog/DDD-3) in core, not per connector.** It
   needs only an ordered stream, a marker injectable into that stream, and key comparison — all
   source-agnostic. Then a source's obligation shrinks to "give me ordered chunks by key" and
   "let me inject or recognise a marker".
5. **Build the control channel in on day one.** A first-class in-band command path
   (start/stop/pause/resume a snapshot over a named stream set) with a pluggable channel. Debezium
   needed a signal *table* only because Connect gave it no way to talk to a running task — canal
   owns its runtime and has no such excuse.
6. **Ship a notification/event API alongside metrics.** Long-running jobs need discrete
   structured lifecycle events with payloads, not just gauges. Debezium added them after metrics;
   start with both.
7. **Model per-phase parallelism.** Snapshot is embarrassingly parallel; log streaming is
   inherently serial. Work assignment must express "phase A wants N workers, phase B wants 1"
   without a restart.
8. **Express codec choice in the type system at pipeline construction** (Debezium Engine's
   `create(Json.class)` pattern), so a mis-wired codec is a compile error, not a runtime cast.
9. **Own the interfaces; adapt at the edges.** Debezium's core runs unchanged under Kafka Connect,
   an embedded engine, and a standalone server because nothing upstream leaks into its core types.
   That is constraint 2 already validated in production.
10. **Diff two connectors before promoting any field to core.** Debezium's `source` block is the
    empirical boundary between generic and connector-specific. Never generalise from one connector
    — that is how a core becomes Mongo-shaped.

---

## Unverified — the complete list

**Everything in this file is unverified.** No primary source was read. Specifically not verified:

- Every interface, class, and method **name** used above (they are recollection, and Debezium has
  renamed several of these across versions).
- Every method **signature** — deliberately not written down at all, rather than guessed.
- The snapshot-mode enum values and which connectors support which.
- Whether the `Snapshotter` SPI refactor has landed, and its real path/shape.
- The `Envelope` field names and `Operation` enum wire codes.
- The DDD-3 watermark algorithm's exact steps, and its relationship to arXiv 2010.12597.
- Every metric and MBean `ObjectName` pattern.
- Every notification type name and the notification channel SPI shape.
- The `DebeziumEngine` builder API and `RecordCommitter` / `ChangeConsumer` signatures.
- The Debezium Server sink list and whether its sink SPI is framework-level.
- The error/retry classification mechanism and config key names.
- `ChangeEventQueue`'s real bounding and blocking semantics.
- **The entire "what they got wrong" section** — no Jira issue, GitHub issue, or community report
  was read. Nothing in §13's second half may be asserted until it is.
- Whether classic (non-incremental) initial snapshot is resumable after restart.
- The single-task/`tasks.max` claim per connector.

# Prior art: Apache Kafka Connect

> Auto-salvaged from the first architecture research pass. Primary-source analysis of
> `apache/kafka` @ `trunk`; every signature was read from source, not recalled.

## Scope

Apache Kafka Connect (Java framework, `connect/api` + `connect/runtime` modules, analysed at apache/kafka `trunk`, July 2026). Raw sources fetched and cached locally at `/private/tmp/claude-501/-Users-bernardocarreira-Documents-work-data-analytics/9fad647d-7c79-4ad7-b63f-ba664a732b9c/scratchpad/kc/` (filenames are the upstream paths with `/` → `_`, e.g. `source_SourceTask.java`, `sink_SinkTask.java`, `clients_ConfigDef.java`, `runtime_AbstractWorkerSourceTask.java`, `runtime_SubmittedRecords.java`, `storage_KafkaConfigBackingStore.java`, `runtime_ConnectMetricsRegistry.java`). Every signature below was read from those files, not recalled. Where I could not verify something I say so inline.

## Core Interfaces

## The four-type skeleton

Connect's whole plugin surface is **two abstract classes per direction**, plus a context interface injected by the runtime. Everything is `Map<String,String>` at the boundary.

### Common base

```java
// org.apache.kafka.connect.components.ConnectPlugin
public interface ConnectPlugin extends Versioned {
    ConfigDef config();
}
// org.apache.kafka.connect.components.Versioned
public interface Versioned { String version(); }

// org.apache.kafka.connect.connector.Task
public interface Task {
    String version();
    void start(Map<String, String> props);
    void stop();
}
```

### Connector (the planner — no data flows through it)

```java
// org.apache.kafka.connect.connector.Connector
public abstract class Connector implements ConnectPlugin {
    protected ConnectorContext context;

    public void initialize(ConnectorContext ctx);
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs); // "only used to recover from failures"
    protected ConnectorContext context();

    public abstract void start(Map<String, String> props);
    public void reconfigure(Map<String, String> props);          // default: stop(); start(props);
    public abstract Class<? extends Task> taskClass();
    public abstract List<Map<String, String>> taskConfigs(int maxTasks);
    public abstract void stop();

    public Config validate(Map<String, String> connectorConfigs); // default: configDef.validate(props)
    @Override public abstract ConfigDef config();
}
```

Note `taskClass()` returns a `Class` — the runtime reflectively instantiates it. That is the single most Java-specific wart; in Go this becomes a factory func.

### SourceConnector / SourceTask

```java
public abstract class SourceConnector extends Connector {
    @Override protected SourceConnectorContext context();
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig);            // default: null (!)
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> cc); // default: UNSUPPORTED
    public boolean alterOffsets(Map<String, String> connectorConfig,
                                Map<Map<String, ?>, Map<String, ?>> offsets);                      // default: false
}

public abstract class SourceTask implements Task {
    public static final String TRANSACTION_BOUNDARY_CONFIG = "transaction.boundary";
    public enum TransactionBoundary { POLL, INTERVAL, CONNECTOR;
        public static final TransactionBoundary DEFAULT = POLL;
        public static TransactionBoundary fromProperty(String property);
    }
    protected SourceTaskContext context;

    public void initialize(SourceTaskContext context);
    @Override public abstract void start(Map<String, String> props);
    public abstract List<SourceRecord> poll() throws InterruptedException;
    public void commit() throws InterruptedException { }                                  // no-op default
    @Override public abstract void stop();
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException { }                                               // no-op default
}
```

`poll()` javadoc, verbatim: *"If no data is currently available, this method should block but return control to the caller regularly (by returning `null`) in order for the task to transition to the `PAUSED` state if requested to do so."* And `stop()`: *"this method necessarily may be invoked from a different thread than `poll()` and `commit()`."*

### SinkConnector / SinkTask

```java
public abstract class SinkConnector extends Connector {
    public static final String TOPICS_CONFIG = "topics";
    @Override protected SinkConnectorContext context();
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<TopicPartition, Long> offsets); // default false
}

public abstract class SinkTask implements Task {
    public static final String TOPICS_CONFIG = "topics";
    public static final String TOPICS_REGEX_CONFIG = "topics.regex";
    protected SinkTaskContext context;

    public void initialize(SinkTaskContext context);
    @Override public abstract void start(Map<String, String> props);
    public abstract void put(Collection<SinkRecord> records);
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) { }
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flush(currentOffsets);
        return currentOffsets;
    }
    public void open(Collection<TopicPartition> partitions) { }
    public void close(Collection<TopicPartition> partitions) { }
    @Override public abstract void stop();
}
```

`put()` returns **void** — the sink cannot tell the framework "I took 300 of your 500 records" or "slow down". The only backchannel is throwing `RetriableException`, or `context.timeout()/pause()/requestCommit()`.

### Contexts (the runtime's capability grant to the plugin)

```java
public interface ConnectorContext {
    void requestTaskReconfiguration();
    void raiseError(Exception e);
    PluginMetrics pluginMetrics();                       // @since 4.1
}
public interface SourceConnectorContext extends ConnectorContext { OffsetStorageReader offsetStorageReader(); }
public interface SinkConnectorContext   extends ConnectorContext { /* empty marker */ }

public interface SourceTaskContext {
    Map<String, String> configs();
    OffsetStorageReader offsetStorageReader();
    default TransactionContext transactionContext() { /* returns null-ish default */ }
    PluginMetrics pluginMetrics();
}

public interface SinkTaskContext {
    Map<String, String> configs();
    void offset(Map<TopicPartition, Long> offsets);      // rewind request
    void offset(TopicPartition tp, long offset);
    void timeout(long timeoutMs);                        // retry backoff for the current batch
    Set<TopicPartition> assignment();
    void pause(TopicPartition... partitions);            // real backpressure
    void resume(TopicPartition... partitions);
    void requestCommit();
    default ErrantRecordReporter errantRecordReporter() { /* null on old runtimes */ }
    PluginMetrics pluginMetrics();
}

public interface ErrantRecordReporter { Future<Void> report(SinkRecord record, Throwable error); }

public interface TransactionContext {           // KIP-618, connector-defined transaction boundaries
    void commitTransaction();
    void commitTransaction(SourceRecord record);
    void abortTransaction();
    void abortTransaction(SourceRecord record);
}
```

### The design property worth transplanting

There is **no `Pipeline` interface, no `Source`+`Sink` symmetry, and no shared supertype for a "connector instance"**. Source and sink are two independent shapes glued together only by `ConnectRecord`. That is both Connect's biggest simplification (each side is small) and its biggest limitation (you cannot express source→sink directly; everything must transit Kafka).


## Plugin Boundary

## In-process, classloader-isolated, reflectively instantiated

Connectors are Java classes in a JAR on `plugin.path`. The runtime creates a child-first `PluginClassLoader` per plugin directory so plugin dependencies don't collide with the worker's or with each other. Instantiation is reflective: the connector names its task type via `Class<? extends Task> taskClass()`, and the runtime `newInstance()`s it. Everything runs in the worker JVM, on worker threads.

## Discovery: two mechanisms, mid-migration

1. **Classpath scanning** (original) — reflectively enumerate subclasses of `SourceConnector`/`SinkConnector`/`Converter`/`Transformation`/….
2. **`ServiceLoader`** — the API javadocs now instruct implementors: *"Kafka Connect may discover implementations of this interface using the Java `ServiceLoader` mechanism. To support this, implementations… should also contain a service provider configuration file in `META-INF/services/org.apache.kafka.connect.source.SourceConnector`."*

Selected by `plugin.discovery ∈ {ONLY_SCAN, HYBRID_WARN, HYBRID_FAIL, SERVICE_LOAD}`, default **`HYBRID_WARN`** — i.e. Kafka is still migrating off scanning years later, because scanning is slow (multi-second worker startup) and the ecosystem's JARs lack service files. **Registry-at-init (canal's chosen model) skips this entire problem.**

## The plugin contract as data

```java
public interface Versioned { String version(); }
public interface ConnectPlugin extends Versioned { ConfigDef config(); }
```

`Connector`, `Task`, `Converter`, `HeaderConverter`, `Transformation`, `Predicate` all carry `version()` + (mostly) `config()`. **Every plugin type is uniformly self-describing.** That uniformity is what makes a generic UI possible; copy it.

## Versioning: KIP-891 (Kafka 4.1)

Multiple versions of the same plugin can be loaded simultaneously in one worker, with versions selectable per plugin instance in the connector config (`connector.version`, and equivalents for converters/transforms/predicates), including version *ranges* rather than exact pins. Motivation: without it *"you would need to run the connectors in two different connect clusters"* to run two versions. Fixed in 4.1.0 (KAFKA-18120). I did not verify the exact config key spellings or the range grammar from source — treat those as needing confirmation.

## ABI/protocol compatibility: the sharp edge

Because the boundary is Java interfaces compiled against a specific `connect-api` jar, **adding a method to a public interface breaks plugins built against older runtimes at load time.** Kafka's own workaround is now written into the javadoc of `ConnectorContext.pluginMetrics()`:

> *"Connectors that use this method but want to maintain backward compatibility… should guard the call to this method with a try-catch block, since calling this method will result in a `NoSuchMethodError` or `NoClassDefFoundError` when the connector is deployed to Connect runtimes older than Kafka 4.1."*

```java
PluginMetrics pluginMetrics;
try { pluginMetrics = context.pluginMetrics(); }
catch (NoSuchMethodError | NoClassDefFoundError e) { pluginMetrics = null; }
```

Same pattern for `SinkTaskContext.errantRecordReporter()` and `SourceTaskContext.transactionContext()`. Java's `default` methods let Connect *add* methods without breaking *compilation*, but not without breaking *linkage* in the other direction. **Go interfaces have the same problem and no `default` escape hatch at all**, which is decisive for canal:

- Keep the **required** interfaces tiny and frozen.
- Put every optional capability behind a **separate, independently-assertable interface** (`interface Snapshotter { … }`, `interface Transactional { … }`), discovered by type assertion — Connect's `default`-returning-null methods are the same idea done worse.
- Put the **context** behind an interface the *core* implements and pass it in, so growing the core's capabilities never breaks a connector.

## Out-of-process: does not exist, and the interface would not survive it

Connect has no gRPC/subprocess connector boundary. The obstacles in its interface, all of which canal must avoid:

1. `Class<? extends Task> taskClass()` — a JVM type handle. Unshippable over a wire.
2. `Map<String, ?>` / `Object value` — untyped payloads with no declared encoding.
3. `Schema` and `Struct` are **behavioural interfaces** (`Schema.field(name)`, `Struct.validate()`), not data. Serialising them means inventing a wire schema Connect never defined.
4. `Future<Void> report(...)`, `Callback<Void>` — in-process async primitives with no timeout/cancellation.
5. No `Context`, so a remote call has no cancellation or deadline to carry.

Kafka does acknowledge the isolation cost: **KIP-987 (Under Discussion)** states *"the shared-JVM model does not permit strong isolation between threads"*, and proposes assigning each job to its own worker so *"resource constraints [can] be specified at the process-boundary"* — i.e. process isolation retrofitted at the *deployment* layer because the *plugin* layer can't provide it.

**Concretely, for canal's constraint #3:** every method should be `(ctx, request) → (response, error)` with request/response being **plain serialisable structs** (no interfaces, no closures, no `Class`, no futures). If `Record`, `Schema`, and `Checkpoint` are structs of bytes+primitives rather than interfaces with behaviour, the in-process registry implementation and a future gRPC shim satisfy the identical interface. Connect fails this test on points 1–4; that failure is the reason no out-of-process Connect exists.


## Record Model

## `ConnectRecord<R>` — self-typed envelope with a copy-constructor contract

```java
public abstract class ConnectRecord<R extends ConnectRecord<R>> {
    private final String topic;
    private final Integer kafkaPartition;
    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;
    private final Headers headers;

    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key,
                               Schema valueSchema, Object value, Long timestamp);
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key,
                               Schema valueSchema, Object value, Long timestamp, Iterable<Header> headers);
}
```

The `R extends ConnectRecord<R>` F-bounded self type exists purely so `Transformation<R>` can be written once and return the *same concrete* record type. In Go this is exactly what a generic `Transform[R Record[R]]` — or simpler, a single concrete `Record` struct — replaces.

`newRecord(...)` is the **structural editing primitive**: records are immutable, and every SMT produces a new record by re-specifying the mutable fields while the subclass silently carries the immutable provenance fields forward.

### Source specialisation

```java
public class SourceRecord extends ConnectRecord<SourceRecord> {
    private final Map<String, ?> sourcePartition;
    private final Map<String, ?> sourceOffset;
    public Map<String, ?> sourcePartition();
    public Map<String, ?> sourceOffset();
}
```

`newRecord()` on `SourceRecord` forwards `sourcePartition`/`sourceOffset` unchanged — so **an SMT cannot alter provenance**, by construction. That is a deliberate and very good invariant.

### Sink specialisation

```java
public class SinkRecord extends ConnectRecord<SinkRecord> {
    private final long kafkaOffset;
    private final TimestampType timestampType;
    private final String originalTopic;              // KIP-793, Kafka 3.6
    private final Integer originalKafkaPartition;
    private final long originalKafkaOffset;
}
```

The `original*` triple is a **retrofit scar**: SMTs may rewrite `topic`/`kafkaPartition`, but offset accounting must use the pre-transform coordinates. KIP-793's motivation states it plainly: *"the topic/partition/offset it can return in preCommit will correspond to the transformed topic/partition/offset, when the framework actually expects it to be the original."* Both `SinkTask.flush()` and `preCommit()` javadocs now have to warn about this in prose.

### Key vs value, headers, raw vs structured

- **Key and value are fully parallel and independently schema'd** (`keySchema`+`key`, `valueSchema`+`value`). There is no "primary key" concept above that.
- **Headers are first-class and ordered**: `Headers` (Connect's own, not the client's) with per-header `Schema`, `Header{ String key(); Schema schema(); Object value(); }`. Headers get their own converter.
- **Raw bytes and structured data coexist by making `value` an `Object`**: if `valueSchema == null` the value is schemaless (a `Map`/`List`/primitive); if `valueSchema.type() == BYTES` it is opaque `byte[]`. There is no separate "raw" record type. The passthrough case is `ByteArrayConverter` + `Schema.OPTIONAL_BYTES_SCHEMA`.
- **No operation type, no before/after images in the core model.** Connect has *no* notion of insert/update/delete/CDC op. Debezium encodes that as a `Struct` with `before`/`after`/`op`/`source` fields inside `value`; deletes are Kafka tombstones (`value == null`). Connect only knows tombstones because `RecordIsTombstone` is a built-in `Predicate`.

That last point is the single most important thing for canal: **Connect's envelope is deliberately payload-agnostic and pays for it by having zero CDC semantics in core.** A generic framework can afford one more field (an op/kind enum) without becoming Mongo-shaped — Connect's omission is why every CDC connector reinvents the envelope incompatibly.


## Lifecycle

## Connector lifecycle

`config()` → `validate(props)` (may be called **before** `start()`) → `initialize(ctx[, existingTaskConfigs])` → `start(props)` → `taskClass()` + `taskConfigs(maxTasks)` → [`reconfigure(props)` | `stop()`].

Pre-flight methods explicitly callable before `start()`: `validate()`, `exactlyOnceSupport()`, `canDefineTransactionBoundaries()`, `alterOffsets()` — each javadoc says so ("*Similar to `validate`, this method may be called by the runtime before the `start` method is invoked*"). **That is a good pattern: capability negotiation and config validation are pure functions of the config, not of a live instance.**

## Task lifecycle

Source: `initialize(SourceTaskContext)` → `start(props)` → loop{ `poll()` → per-record `commitRecord()` → periodic `commit()` } → `stop()`.
Sink: `initialize(SinkTaskContext)` → `start(props)` → `open(partitions)` → loop{ `put(records)` → periodic `preCommit()`/`flush()` } → `close(partitions)` → `stop()`. On rebalance: `close(old)` then `open(new)`.

The `SinkTask` javadoc spells the five phases out explicitly (Initialization → Partition Assignment → Record Processing → Partition Rebalancing → Shutdown). `open`/`close` are the **resource-scoping hook keyed to assignment**, distinct from `start`/`stop` which are keyed to task existence. That distinction is worth copying: canal will need "this worker now owns splits {A,B}" separately from "this connector instance is booting".

## Cancellation — and why it's bad

There is **no `Context`/cancellation token anywhere**. Cancellation is: a different thread calls `stop()`, which must set a flag and unblock `poll()`. The javadoc even prescribes the pattern: *"if a task uses a `java.nio.channels.Selector`… this method could set a flag that will force `poll()` to exit immediately and invoke `wakeup()`."* Interruption of the task thread is separate and happens after `task.shutdown.graceful.timeout.ms` (default **5000**).

The contract is genuinely broken, and Kafka knows it. **KIP-419 ("Safely notify Kafka Connect SourceTask is stopped", KAFKA-7841) is still "Under Discussion"** — I verified `stopped()` does **not** exist in trunk `SourceTask.java`. Its problem statement: `stop()` *"can actually be called more than once in some situations for the same SourceTask"*, and after `stop()` *"any of `poll()`, `commit()` and `commitRecord()` can still be called."* So **there is no method a source connector can safely release resources in.** Seven-plus years unfixed.

For canal: pass `context.Context` into every blocking method (`Read(ctx)`, `Write(ctx)`, `Commit(ctx)`), and define exactly one terminal callback that is guaranteed-last and guaranteed-once.

## Error and retry classification

Two-valued, marker-exception-based:

- `org.apache.kafka.connect.errors.RetriableException` → framework retries the same call (bounded by `errors.retry.timeout`, backoff capped by `errors.retry.delay.max.ms`).
- anything else → task → `FAILED`, stop.
- `ConnectorContext.raiseError(Exception)` lets a *connector* (not task) push itself to `FAILED` asynchronously.
- `SinkTaskContext.timeout(long)` sets how long before the failed batch is retried.

Per KIP-298 the retry/tolerate classification is applied per **stage**: converter, transformation, `put()`/`poll()`, and Kafka produce/consume. Retries happen only on `RetriableException`; tolerance (`errors.tolerance=all`) then decides whether to skip.

`ToleranceType { NONE, ALL }` — that's the whole policy vocabulary. There is no per-error-class policy, no circuit breaker, no exponential-backoff-then-park.

**Restart** is not a lifecycle callback at all: it is `POST /connectors/{n}/restart?includeTasks=&onlyFailed=` (KIP-745), implemented by writing a `restart-connector-{id}` record into the config topic.


## Checkpoint Model

## Offsets are opaque, connector-authored, framework-stored

The representation is the whole idea:

```java
Map<String, ?> sourcePartition;   // opaque stream identity, e.g. {"db":"x","table":"y"} or {"filename":"a.log"}
Map<String, ?> sourceOffset;      // opaque position within it,  e.g. {"pos":12345, "snapshot":true}
```

`OffsetStorageReader` javadoc constrains it: *"Offsets are always defined as Maps of Strings to primitive types, i.e. all types supported by `Schema` other than Array, Map, and Struct."*

```java
public interface OffsetStorageReader {
    <T> Map<String, Object> offset(Map<String, T> partition);
    <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions);
}
```

The batch form exists explicitly because a task owning many partitions needs one round trip, and it **degrades partially**: *"when errors occur, this method omits the associated data and tries to return as many of the requested values as possible… implementations should take care to check that the data is actually available."*

### Who owns it

The **framework** owns commit. The connector only *emits* offsets attached to records and *reads* them at startup. `SourceTask.commit()` and `commitRecord()` exist only for connectors that *additionally* need to write progress into their own external system — javadoc: *"SourceTasks are not required to implement this functionality; Kafka Connect will record offsets automatically."*

### When it commits — source

Time-driven, not batch-driven. `WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.flush.interval.ms"` default **60000 ms**; `"offset.flush.timeout.ms"` default **5000 ms**.

The ack-tracking machinery is the transplantable jewel (`runtime/SubmittedRecords.java`):

```java
final Map<Map<String, Object>, Deque<SubmittedRecord>> records = new HashMap<>();
public SubmittedRecord submit(SourceRecord record);      // before handing to producer
public CommittableOffsets committableOffsets();          // advances head over contiguously ack'd/dropped records
public boolean awaitAllMessages(long timeout, TimeUnit unit);
```

Javadoc: *"The latest-eligible offsets for each source partition can be retrieved via `committableOffsets()`, where every record up to and including the record for each returned offset has been either acknowledged or dropped."* A per-partition FIFO deque, acked out of order from producer callbacks, with the commit watermark advancing only over the contiguous acked prefix. This is a correct, generic, sink-agnostic "commit the low-water mark" algorithm — copy it.

`storage/OffsetStorageWriter.java` is double-buffered with a `Semaphore(1)`: `offset(partition, offset)` accumulates into `data`; `beginFlush()` (or `beginFlush(timeout, unit)`) swaps `data` → `toFlush` and takes the semaphore; `doFlush(Callback)` writes asynchronously; `cancelFlush()` merges `toFlush` back into `data` on failure. So new offsets keep accruing while a flush is in flight, and a failed flush loses nothing.

Persistence is via `OffsetBackingStore`, a **bytes-in/bytes-out** interface — the offset store knows nothing about maps or schemas:

```java
public interface OffsetBackingStore {
    void start(); void stop();
    Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys);
    Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback);
    Set<Map<String, Object>> connectorPartitions(String connectorName);
    void configure(WorkerConfig config);
}
```

Serialization of the offset map is done by the *same* pluggable `Converter` used for records (`OffsetStorageWriter` holds `keyConverter`/`valueConverter`). Implementations: `KafkaOffsetBackingStore` (compacted `connect-offsets` topic, key = `[connectorName, sourcePartition]`), `FileOffsetBackingStore` (standalone), `MemoryOffsetBackingStore`.

### When it commits — sink

Completely different mechanism: the sink's "offset" is the **input** Kafka consumer offset, committed via the consumer group. `preCommit()` is the connector's veto/override:

> *"@return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by topic-partition that are safe to commit."*

So a sink that batches asynchronously returns only the offsets whose records are durable downstream. Returning `{}` disables framework commits entirely (the connector then owns durability *and* progress).

### Restart

Source: `initialize(SourceTaskContext)` → `start(props)` → task calls `context.offsetStorageReader().offsets(myPartitions)` and seeks. Sink: framework restores from the consumer group; `open(partitions)` tells the task which partitions it now owns, and the task may call `context.offset(tp, n)` to force a rewind (used when the downstream system is the real source of truth).

### Interaction with sink acknowledgement

The load-bearing subtlety: for a **source**, "committed" means *the destination Kafka producer acked* — which is why Connect's source path is at-least-once by default and why exactly-once required putting the offset write *inside the producer transaction* (KIP-618). For a **sink**, "committed" means *whatever `preCommit()` says*, i.e. the framework trusts the connector. canal must decide this explicitly: **is the checkpoint advanced by the sink's ack, or by a timer?** Connect answers "sink ack" for sources (producer callback → `SubmittedRecords.ack()`) and "connector's word" for sinks, and the asymmetry is a documented source of confusion.


## Snapshot Handling

## Connect has no snapshot concept at all. This is its clearest structural gap.

Verified by reading the interface: `SourceTask` has `poll()`, `commit()`, `commitRecord()`, `start()`, `stop()`. There is:

- **no bounded/finite task notion** — no `isDone()`, no way for `poll()` to say "this stream is exhausted";
- **no completion state** — `AbstractStatus.State` is `{UNASSIGNED, RUNNING, PAUSED, FAILED, DESTROYED, RESTARTING, STOPPED}`; there is no `COMPLETED`. `STOPPED` (KIP-875) is an operator-requested target state, not a self-reported terminal state;
- **no phase model** — nothing distinguishes "I am scanning" from "I am tailing";
- **no chunk/split abstraction** — work is split exactly once, by `taskConfigs(maxTasks)`, at connector start.

### So how do real snapshot sources cope? Three workarounds, all in connector-land:

1. **Encode snapshot progress in the opaque offset map.** Debezium stuffs an incremental-snapshot context (chunk boundaries, last-processed PK, whether snapshotting) into `sourceOffset`, precisely because that is the only durable per-partition state the framework will store. Debezium's docs: the offset context *"is enough to resume the snapshot after a connector restart, be it intentionally or after a crash."* This works, but it forces snapshot state through a `Map<String, primitive>` keyhole.

2. **Never terminate — just switch modes inside `poll()`.** The handoff snapshot→stream is entirely internal to the task: the task reads the log position first, snapshots, then replays the log from that position, emitting both through the same `poll()`. The framework has no idea a transition happened, cannot report progress on it, and cannot parallelise the snapshot differently from the stream.

3. **Out-of-band control plane.** Debezium had to invent a **signalling table / Kafka signal topic** (`execute-snapshot`, `pause-snapshot`, `resume-snapshot`) because Connect offers no way to send a command to a running task. Connect's only task-directed control is: change the connector config (→ task restart), or `requestTaskReconfiguration()` (→ task restart). Both are restarts.

### Chunking / parallelising / resuming

- **Resumable:** yes, but only because the connector hand-rolls it into `sourceOffset`.
- **Chunked:** yes, connector-side (Debezium reads min/max PK and slices by `chunk.size`).
- **Parallelised:** poorly. Parallelism = number of tasks = fixed by `taskConfigs(maxTasks)` at start. To re-split (e.g. "the snapshot needs 20 tasks, the stream needs 2") the connector must call `context.requestTaskReconfiguration()`, and the dev guide says the framework then *"request[s] new configuration information and update[s] the tasks"* — i.e. tears down and restarts tasks. There is **no dynamic work-stealing, no split enumerator, no per-split assignment**. Contrast Flink's `SplitEnumerator`/`SourceReader`, which was designed for exactly this and which Connect cannot express.

### The `commit()` / snapshot mismatch

Offsets flush on `offset.flush.interval.ms` (60s default), completely decoupled from snapshot chunk boundaries. So a snapshot chunk can be fully emitted and acked, and still be re-emitted after a crash, because the flush timer hadn't fired. There is no "commit at this record" primitive for sources except the exactly-once `TransactionContext.commitTransaction(record)` — which only exists when cluster-wide EoS is enabled.

**Lesson for canal:** model phases and splits in the *core*, generically — a source declares splits, the core assigns and checkpoints them, and a split may report exhaustion. Snapshot-then-stream then falls out as "the split set changes over time" rather than being smuggled through an opaque offset map.


## Delivery Guarantees

## Default: at-least-once, both directions

Source: offsets flush every `offset.flush.interval.ms` (60s) *after* producer acks, so a crash replays up to one flush interval. Sink: consumer offsets commit after `preCommit()` says records are durable, so a crash replays the uncommitted batch. `put()` must therefore be **idempotent or tolerant of redelivery** — Connect states this as the sink author's problem and gives no help beyond `SinkRecord`'s original topic/partition/offset triple as a natural dedup key.

## Exactly-once source: KIP-618 (Kafka 3.3+)

Mechanism, in one sentence: **the source records and their offsets are written to Kafka in the same producer transaction**, so *"source offsets will be committed to Kafka if and only if the source records for that batch were also written to Kafka."*

Moving parts:

- Per-task transactional producer, `transactional.id = ${groupId}-${connector}-${taskId}`.
- Offsets written into the offsets topic *inside* the transaction (hence the per-connector `offsets.storage.topic` connector property; the worker presents a merged view of the dedicated topic over the global one).
- **Connector-declared capability**, not an operator guess: `exactlyOnceSupport(config)` → `ExactlyOnceSupport{SUPPORTED, UNSUPPORTED}` (default `null`, meaning "assume unsupported"); connector config `exactly.once.support = requested | required`.
- **Transaction boundary is negotiable**: `transaction.boundary = poll | interval | connector` (`SourceTask.TransactionBoundary`, default `POLL`), `transaction.boundary.interval.ms`, and for `connector` mode the task drives it via `TransactionContext.commitTransaction([record])` / `abortTransaction([record])`, gated on `canDefineTransactionBoundaries(config)` → `ConnectorTransactionBoundaries{SUPPORTED, UNSUPPORTED}`.
- **Zombie fencing** is the hard part: `tasks-fencing-{connector}` records in the config topic record the expected task-producer count; the leader exposes an internal `PUT /connectors/{connector}/fence`; `Admin.fenceProducers()` bumps producer epochs via `InitProducerId` so a stranded old task's writes are rejected. Workers request fencing confirmation before starting a new task generation.
- Worker config `exactly.once.source.support = DISABLED | PREPARING | ENABLED`, requiring a **two-phase rolling upgrade**.

Stated limitations (from KIP-618): distributed mode only; cluster-wide all-or-nothing, not per-connector; a source partition must be assigned to at most one task at a time; and duplicates are still possible if a connector bypasses the framework's offset API.

## Exactly-once sink: not provided

There is no transactional-sink / two-phase-commit protocol in Connect. There is **no `prepare()`/`commit()` pair on `SinkTask`** — `preCommit()` is a one-shot "which offsets are safe", not a 2PC prepare, and it cannot participate in an external transaction that survives worker death. Contrast Flink's `TwoPhaseCommittingSink` (`Committer` + recoverable committables checkpointed alongside state). Connect sinks reach EoS only by being idempotent or by hand-rolling an atomic external commit that stores the Kafka offsets in the destination (the "offsets in the sink" pattern, e.g. an offsets table in the target DB) — and then returning `{}` from `preCommit()` to take over entirely.

## Idempotency / dedup

Nothing in core. The framework does not hash records, does not track seen keys, and has no dedup window. `SinkRecord.equals()` includes the original topic/partition/offset, which per KIP-793's PR discussion means *"only a re-delivery can cause this equality check to fire"* — that's the extent of it.

**For canal:** a symmetric two-phase sink commit (`Prepare() → committable; Commit(committables)`) with committables persisted in the same checkpoint as source positions is strictly more general than Connect's producer-transaction trick, and it does not assume the destination is Kafka. Connect could not do this because it hard-coded Kafka as the middle; canal has no such constraint and should not adopt the asymmetry.


## Backpressure

## Source side: essentially none, by admission

The loop, from `runtime/AbstractWorkerSourceTask.java` (~lines 358–380):

```java
if (toSend == null) {
    prepareToPollTask();
    long start = time.milliseconds();
    toSend = poll();
    if (toSend != null) recordPollReturned(toSend.size(), time.milliseconds() - start);
}
if (toSend == null) { batchDispatched(); continue; }
if (!sendRecords()) {
    stopRequestedLatch.await(SEND_FAILED_BACKOFF_MS, TimeUnit.MILLISECONDS);   // = 100 ms, hardcoded
}
```

So: **`poll()` is called again the instant the previous batch has been handed to the producer.** There is no rate limit, no credit, no queue depth signal, no "the sink is behind" feedback. The *only* backpressure is incidental: `producer.send()` blocks when the client's `buffer.memory` fills, up to `max.block.ms`. Batch size is entirely the task's choice (whatever `poll()` returns). Every real connector therefore implements its own `poll.interval.ms` / `batch.max.rows`.

Concurrency: **one thread per task**, period. `poll()` → transform → convert → `producer.send()` all on that thread; acks land on the producer's I/O thread and hit `SubmittedRecord.ack()` (whose class javadoc notes it *"is not thread-safe, though a `SubmittedRecord` can be acknowledged from a different thread"*). Parallelism = task count = `min(tasks.max, len(taskConfigs))`.

Buffering: `SubmittedRecords`' per-partition deques are **unbounded**, and the javadoc flags the failure mode: `committableOffsets()` *"may take some time to complete if a large number of records has built up, which may occur if a Kafka partition is offline and all records targeting that partition go unacknowledged while records targeting other partitions continue to be dispatched."* Connect exposes `source-record-active-count` / `-max` / `-avg` so you can *watch* the unbounded buffer grow, but cannot make it push back.

Kafka acknowledges this as a hole. **KIP-731 "Record Rate Limiting for Kafka Connect" is still "Under Discussion"** (since 2021), and its motivation names both symptoms: the *"noisy neighbor problem"* (*"one bad-behaving Task can cause issues for other Tasks on the same node… by exhausting shared resources"*) and the *"firehose problem"* (*"Connect tends to run full-throttle and can overwhelm downstream systems"*). It proposes a pluggable `RateLimiter<R>` with a `throttleTime()` method and `record.rate.limit` / `record.batch.rate.limit` per task. Not shipped.

## Sink side: real backpressure, via the consumer

The sink path has what the source path lacks, because Kafka's consumer is pull-based:

- Batch size is bounded by the consumer's `max.poll.records` (a client config, not a connector-facing knob).
- `WorkerSinkTask` calls `consumer.pause(consumer.assignment())` when a batch must be redelivered (`pausedForRedelivery`) and `consumer.resume(...)` after.
- `SinkTaskContext.pause(TopicPartition...)` / `resume(...)` let the **connector itself** stop the flow per partition — the one genuine, connector-driven flow-control primitive in the whole API.
- `SinkTaskContext.timeout(ms)` sets the retry delay for a failing batch.

## What canal should conclude

Connect's asymmetry is an accident of Kafka being the fixed middle. A generic framework has no such excuse. Make backpressure **explicit and bidirectional in the interface**: either the sink returns how much it accepted, or the core hands the source a bounded, credit-issuing writer (`Push(ctx, batch) error` that blocks), or both. `put(...) void` + `poll() List` cannot express flow control, and every workaround Connect has (`context.pause`, `buffer.memory`, per-connector `poll.interval.ms`, KIP-731) is a patch over that.


## Schema Handling

## Schema is in-band, per-record, and structurally compared

```java
public interface Schema {
    enum Type { INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT }
    Type type();
    boolean isOptional();
    Object defaultValue();
    String name();                       // logical/record name, e.g. "org.acme.Order"
    Integer version();                   // monotonic version, connector-assigned
    String doc();
    Map<String, String> parameters();    // extension point: logical types live here
    Schema keySchema();                  // MAP only
    Schema valueSchema();                // MAP/ARRAY only
    List<Field> fields();                // STRUCT only
    Field field(String fieldName);
    Schema schema();
}
```

Plus interned constants (`Schema.STRING_SCHEMA`, `Schema.OPTIONAL_INT64_SCHEMA`, …).

**Logical types are a naming convention, not a type-system feature:** `Decimal`, `Date`, `Time`, `Timestamp` are helper classes producing an `INT32`/`INT64`/`BYTES` schema with `name()` set to `"org.apache.kafka.connect.data.Decimal"` and `parameters()` carrying `"scale"`. Clever and extensible — bad connectors and bad converters silently disagree about them constantly.

`SchemaBuilder` is a fluent builder (`SchemaBuilder.struct().name("X").version(2).field("id", Schema.INT64_SCHEMA).field("name", Schema.OPTIONAL_STRING_SCHEMA).build()`), and `Struct` is the dynamic instance with `put(String, Object)` / typed `getInt64(...)` / `validate()` (which enforces required-field presence).

### Discovery and propagation

- **Discovery is entirely the connector's job.** Nothing in the API asks a source "what is your schema?" There is no `Schema describe()` method. The source just attaches a `Schema` to each record it emits.
- **Propagation is per-record, in-band.** `keySchema`/`valueSchema` travel on every `ConnectRecord`. Sinks read `record.valueSchema()`; the idiomatic sink caches "last schema seen per topic" and reacts when the object changes.
- **Whether schema goes on the wire is the `Converter`'s choice, not the connector's.** `JsonConverter` with `schemas.enable=true` embeds a schema literal in every message (notoriously bloated); an Avro/Protobuf converter with a registry sends a schema ID and keeps schemas out-of-band; `ByteArrayConverter` sends no schema. **This separation — schema is in the record model, schema *encoding* is in the codec — is the single cleanest idea in Connect's design.**

### Evolution / drift

Connect core does **nothing**. There is no compatibility checker, no drift policy, no "schema changed" event. What exists:

- `Schema.version()` (an `Integer`) as a hint, and `ConnectSchema.equals()` comparing type+name+version+fields+params, so sinks can cheaply detect "different schema object".
- `org.apache.kafka.connect.data.SchemaProjector`, a **single static method**: `public static Object project(Schema source, Object record, Schema target) throws SchemaProjectorException` — projects a value from one schema version to another (used by sinks to widen old records to a newer schema). It is the entire out-of-the-box evolution story.
- Compatibility enforcement, if any, lives in the registry behind the converter (Confluent Schema Registry) — i.e. outside Apache Kafka.
- DDL/schema-change *events* are not modelled: Debezium publishes them to a separate schema-change topic, and KIP-585's own motivation cites *"Debezium use cases requiring different transformations for schema-change versus data-change topics"* as the reason predicates were needed.

**Verdict:** in-band per-record schema + codec-decides-encoding is right and worth copying. "No drift policy in core" is a real gap: every sink reinvents "should I ALTER TABLE?"


## Config Model

## `ConfigDef` — a self-describing config schema, and the reason Connect has a usable UI

This is the part of Connect most worth stealing outright.

```java
// A connector declares its config surface as data:
@Override public ConfigDef config();
```

Each entry is a `ConfigKey` with **fourteen public final fields**, verified from `clients/.../ConfigDef.java`:

```java
public static class ConfigKey {
    public final String name;
    public final Type type;                  // BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    public final String documentation;
    public final Object defaultValue;        // or the sentinel ConfigDef.NO_DEFAULT_VALUE  → "required"
    public final Validator validator;
    public final Importance importance;      // HIGH, MEDIUM, LOW
    public final String group;               // UI section
    public final int orderInGroup;           // UI ordering
    public final Width width;                // NONE, SHORT, MEDIUM, LONG  → input widget size
    public final String displayName;         // human label
    public final List<String> dependents;    // re-validate/re-recommend these when I change
    public final Recommender recommender;
    public final boolean internalConfig;     // hide from docs/UI
    public final String alternativeString;
}
```

Note what's in there that is purely presentational: `group`, `orderInGroup`, `width`, `displayName`, `importance`. **The config schema is deliberately also a UI schema.** That is why third-party Connect UIs can render a form for a connector nobody has ever seen.

### Validation and dynamic recommendation

```java
public interface Validator {
    void ensureValid(String name, Object value);      // throws ConfigException
}
public interface Recommender {
    List<Object> validValues(String name, Map<String, Object> parsedConfig);
    boolean visible(String name, Map<String, Object> parsedConfig);
}
```

Built-in validators: `Range`, `ValidList`, `ValidString`, `CaseInsensitiveValidString`, `NonNullValidator`, `NonEmptyString`, `NonEmptyStringWithoutControlChars`, `ListSize`, `LambdaValidator`, `CompositeValidator`.

`Recommender` is the key insight: **valid values and visibility are functions of the whole current config**, so "if `mode=timestamp`, then `timestamp.column.name` becomes visible and required, and its dropdown is populated by querying the live database" is expressible. `dependents` is the dependency edge that tells the runtime which keys to re-recommend when one changes. `ConfigDef.validate()` does a topological walk from parentless configs so recommenders see parsed upstream values.

Two-tier validation:
- **Declarative, offline:** `ConfigDef` + `Validator` — no I/O.
- **Imperative, connects to the world:** `Connector.validate(Map)` override — the default just runs the `ConfigDef`, but connectors override it to test credentials, list tables, check permissions. **This split is important: cheap structural validation for every keystroke, expensive semantic validation on submit.**

### The result carries per-key diagnostics, not an exception

```java
public class ConfigValue {
    private final String name;
    private Object value;
    private List<Object> recommendedValues;
    private final List<String> errorMessages;
    private boolean visible;
}
```

`Connector.validate()` returns `Config(List<ConfigValue>)` — **all** errors, keyed by field, never a fail-fast throw. Exactly what a form needs.

### Surfaced to the UI verbatim

`PUT /connector-plugins/{pluginName}/config/validate` (verified in `ConnectorPluginsResource.java`, also `GET /connector-plugins/{pluginName}/config` and `GET /connector-plugins`) returns:

```java
public record ConfigInfos(@JsonProperty("name") String name,
                          @JsonProperty("error_count") int errorCount,
                          @JsonProperty("groups") List<String> groups,
                          @JsonProperty("configs") List<ConfigInfo> configs) { }

public record ConfigInfo(@JsonProperty("definition") ConfigKeyInfo configKey,
                         @JsonProperty("value") ConfigValueInfo configValue) { }

public record ConfigKeyInfo(name, type, required, default_value, importance, documentation,
                            group, order_in_group, width, display_name, dependents) { }
public record ConfigValueInfo(name, value, recommended_values, errors, visible) { }
```

That is a **definition/value pair per key** — schema and current state in one payload. A frontend needs no per-connector knowledge whatsoever.

### Where it's weak

Everything is `Map<String,String>`; `ConfigDef` parses/validates but the task still receives raw strings and re-parses via `AbstractConfig`. Nested/repeated structures are faked with dotted prefixes (`transforms=a,b` + `transforms.a.type=…`), which is why `transforms`/`predicates` need bespoke `enrich()` logic in `ConnectorConfig` rather than being expressible in `ConfigDef` itself. There is no way to declare "a list of sub-objects".


## Observability

## Metrics: JMX, hierarchical groups, per-task and per-worker

From `runtime/ConnectMetricsRegistry.java`. Groups (each a JMX MBean group, tagged by `connector` and where relevant `task`):

- **`connector-metrics`** — `connector-class`, `connector-version`, `connector-type`, `status`.
- **`connector-task-metrics`** — `status`, `running-ratio`, `pause-ratio`, `offset-commit-avg-time-ms`, `offset-commit-max-time-ms`, `offset-commit-failure-percentage`, `offset-commit-success-percentage`, `batch-size-avg`, `batch-size-max`, plus identity/version of the task, key/value/header converters (`key-converter-class`, `value-converter-version`, …).
- **`source-task-metrics`** — `source-record-poll-rate`/`-total`, `source-record-write-rate`/`-total`, `poll-batch-avg-time-ms`/`-max-time-ms`, `source-record-active-count`/`-avg`/`-max`, and (EoS) `transaction-size-min`/`-avg`/`-max`.
- **`sink-task-metrics`** — `sink-record-read-rate`/`-total`, `sink-record-send-rate`/`-total`, **`sink-record-lag-max`**, `partition-count`, `offset-commit-completion-rate`/`-total`, `offset-commit-skip-rate`/`-total`, `put-batch-avg-time-ms`/`-max-time-ms`, `sink-record-active-count`/`-avg`/`-max`.
- **`task-error-metrics`** — `total-record-failures`, `total-record-errors`, `total-records-skipped`, `total-retries`, `total-errors-logged`, `deadletterqueue-produce-requests`, `deadletterqueue-produce-failures`, `last-error-timestamp`.
- **`connect-worker-metrics`** — `connector-count`, `task-count`, `connector-startup-attempts-total`/`-success-total`/`-failure-total`/`-success-percentage`/`-failure-percentage`, same for tasks, and per-connector task rollups: `connector-total-task-count`, `connector-running-task-count`, `connector-paused-task-count`, `connector-failed-task-count`, `connector-unassigned-task-count`, `connector-restarting-task-count`, `connector-destroyed-task-count`.
- **`connect-worker-rebalance-metrics`** — `completed-rebalances-total`, `rebalance-avg-time-ms`, `rebalance-max-time-ms`, `time-since-last-rebalance-ms`, `rebalancing`, `epoch`, `leader-name`.

Design notes worth copying: the naming is rigidly `<noun>-<aggregation>` (`-rate`/`-total`/`-avg`/`-max`/`-percentage`/`-ratio`/`-ms`); **`running-ratio`/`pause-ratio` are duty-cycle gauges**, which answer "is this task actually working?" better than any counter; `*-active-count` exposes in-flight depth; and `source-record-poll-*` vs `source-record-write-*` are deliberately separate so you can see the transform/filter drop rate as the gap between them.

**Rebalance metrics are a first-class group.** If canal has coordinated multi-worker deployment, treat coordination as a metered subsystem from day one.

`PluginMetrics` (Kafka 4.1) lets a **connector register its own metrics** through its context, with `connector`/`task` tags auto-applied. `Converter`s can opt in by implementing `Monitorable`, and get a `converter=key|value|header` tag automatically. That's the right shape: the plugin declares metrics, the core owns naming, tagging, and export.

## Health/status model

```java
public enum State { UNASSIGNED, RUNNING, PAUSED, FAILED, DESTROYED, RESTARTING, STOPPED }
```
(`DESTROYED` is never user-visible; `STOPPED` only for connectors.) Status is `(state, workerId, generation, trace)` — `trace` carries the failure stacktrace, which is what makes `GET /connectors/{n}/status` genuinely useful. Status is persisted to the compacted `connect-status` topic via `KafkaStatusBackingStore`, so any worker can answer for any connector.

**Notably absent: no `COMPLETED`, no per-split progress, no lag-for-sources, no throughput target/SLO.** `sink-record-lag-max` exists only because Kafka consumers have a natural log-end offset; a source has no comparable "how far behind am I" metric because the offset map is opaque to the framework. That is a direct cost of the opaque-offset design and a real thing to fix in canal: if the core understands a position type well enough to compare two positions, it can compute lag and snapshot-percent-complete generically.

## What a UI can read

`GET /connectors`, `GET /connectors/{n}`, `GET /connectors/{n}/status`, `GET /connectors/{n}/tasks`, `GET /connectors/{n}/tasks/{id}/status`, `GET /connectors/{n}/topics` (which topics a connector actually touched), `GET /connectors/{n}/offsets` (KIP-875), `GET /connector-plugins`, `GET /connector-plugins/{p}/config`, `PUT /connector-plugins/{p}/config/validate`, plus `PUT .../pause|resume|stop`, `POST .../restart`, `PATCH|DELETE .../offsets`. Metrics are JMX-only — **there is no HTTP metrics endpoint**, so every real deployment bolts on a JMX exporter. Don't repeat that; expose Prometheus (or equivalent) natively.


## Deployment

## Two modes, one plugin API — the split canal needs

The connector-facing interfaces are **identical** in both modes. What swaps is three storage implementations plus the herder. That is the correct seam and canal should copy it exactly.

| Concern | Standalone | Distributed |
|---|---|---|
| Orchestrator | `StandaloneHerder` | `DistributedHerder` |
| Connector configs | `MemoryConfigBackingStore` | `KafkaConfigBackingStore` (compacted `connect-configs`) |
| Offsets | `FileOffsetBackingStore` | `KafkaOffsetBackingStore` (compacted `connect-offsets`) |
| Status | in-memory | `KafkaStatusBackingStore` (compacted `connect-status`) |
| Membership | none | Kafka group protocol (`WorkerCoordinator`), `group.id` |
| Exactly-once source | **not supported** | supported |

`OffsetBackingStore`'s `ByteBuffer`-in/`ByteBuffer`-out interface is what makes this swap trivial: the durability substrate never sees a domain type.

## Work assignment

Two-level, and the levels are load-bearing:

1. **Logical split — the connector's job.** `taskConfigs(int maxTasks)` returns *N* opaque config maps. The connector decides how to shard (tables per task, partitions per task, files per task). The framework never inspects them. `tasks.max` caps *N*, now hard-enforced (**KIP-1004**, Kafka 3.8): exceeding it marks the connector `FAILED`, with `tasks.max.enforce=false` as a deprecated escape hatch. Before 3.8 this was merely a suggestion — *"buggy or hostile connectors could generate excessive task configurations, threatening cluster stability."*
2. **Physical placement — the framework's job.** The set `{connectors} ∪ {tasks}` is distributed across workers by the assignor.

**This separation is the single best idea to transplant: the connector says *how the work divides*, the runtime says *where each piece runs*, and neither knows the other's algorithm.**

## Coordination

Workers join a Kafka consumer group (`group.id`); the group leader computes the assignment and distributes it in the group protocol payload. The leader also owns cluster-wide writes (task configs, fencing rounds) — non-leader workers **forward REST writes to the leader**, authenticated with a rotating `session-key` record in the config topic (KIP-507, `ConnectProtocolCompatibility.SESSIONED`).

`IncrementalCooperativeAssignor` (**KIP-415**, Kafka 2.3) replaced stop-the-world rebalancing. From the source: `scheduledRebalance`, `delay`, `revokedInPrevious`, `consecutiveRevokingRebalancesBackoff` — on worker loss the assignor **deliberately leaves the orphaned work unassigned for `scheduled.rebalance.max.delay.ms`** so a bouncing worker reclaims its own tasks instead of triggering a reshuffle, and it applies exponential backoff when consecutive rounds both revoke (*"Consecutive revoking rebalances observed. Computing delay and next scheduled rebalance."*). KIP-415's own motivation: stopping the world every rebalance *"could lead to significant delays, and in some cases — also known as rebalance storms — it could bring the cluster into a state of consecutive rebalances and the Connect cluster could take several minutes to stabilize."*

## The config topic as a replicated state machine

`KafkaConfigBackingStore` is worth reading in full as prior art for canal's control plane. Seven record types on a **single-partition, compacted** topic used as a write-ahead log:

`connector-{id}` (source of truth, non-ephemeral — *"If the entire Connect cluster goes down, this is all that is really needed to recover"*), `task-{id}-{n}` (ephemeral), `commit-{id}` (**the atomicity marker**), `target-state-{id}`, `tasks-fencing-{id}` (KIP-618), `restart-connector-{id}` (KIP-745), `session-key` (KIP-507), `logger-cluster-{…}`.

The `commit-` record is the key trick: task configs are written one record at a time, then a `commit-{connector}` record with the task count makes the whole set visible atomically — *"when reading the log, we must buffer config updates for a connector's tasks and only apply atomically them once a commit message has been read."* Without it, *"some partitions may be double-assigned because the old config and new config may use completely different assignments."*

And the honest failure disclosure, from the same javadoc: **compaction plus a partial write can leave an unrecoverable inconsistency** — a `commit-foo (2 tasks)` whose `task-foo-1-config` has been compacted away, or a mid-write leader failure with no second commit, *"leaving us in an inconsistent state with no obvious way to resolve the issue."* The class therefore exposes which connectors have inconsistent data so the herder can act. **A real transactional/versioned control-plane store (etcd revision, a DB row with a version column, multi-record txn) avoids this entirely — Connect only has this problem because it insisted on using a compacted Kafka topic.**

## k8s reality

Connect's unit of scaling is the *worker*, and heterogeneous connectors on shared workers cause hotspots that the assignor cannot see. KIP-987 (Under Discussion) proposes explicit `static.connectors`/`static.tasks` pinning so *"external management systems [can] correlate worker resource utilization directly to specific jobs, using existing process-monitoring tools at the container or VM boundary."* The de-facto k8s answer today is one Connect cluster per workload — i.e. abandoning the shared cluster the architecture was built for.


## What They Got Right

- Connector/Task split: the connector is a pure planner (no data flows through it) that turns one config into N opaque task configs via `taskConfigs(int maxTasks)`, while the runtime independently decides which worker runs which task — the connector never knows about placement and the runtime never knows about sharding logic.

- Offsets as an opaque `(Map<String,?> partition, Map<String,?> offset)` pair authored by the connector but stored, batched, and committed by the framework — the core is source-agnostic yet still owns durability, restart, and (via the REST API) operator-visible offset editing.

- `ConfigDef` as a self-describing config schema that doubles as a UI schema: `group`, `orderInGroup`, `width`, `displayName`, `importance`, `dependents`, `Validator`, and `Recommender` mean a frontend can render and live-validate a form for a connector it has never heard of, with zero core edits.

- Two-tier validation: cheap declarative `ConfigDef`/`Validator` checks, plus an overridable `Connector.validate(Map)` that may hit the network (test credentials, list tables) and returns `List<ConfigValue>` with per-field `errorMessages`, `recommendedValues`, and `visible` — all errors at once, never a fail-fast throw.

- Serialization as a wholly separate pluggable concern: `Converter.fromConnectData/toConnectData` and a distinct `HeaderConverter` are configured per-pipeline (`key.converter`, `value.converter`, `header.converter`), so connectors never mention a wire format and any connector composes with any codec.

- In-band per-record schema (`keySchema`/`valueSchema` on every record) while the *encoding* of that schema is the converter's choice — the same connector output can go out as schemaless JSON, JSON-with-embedded-schema, or a registry-backed Avro ID.

- Immutable records with a `newRecord(...)` copy-constructor contract, where provenance fields (`sourcePartition`/`sourceOffset`) are silently carried forward — so a transform structurally *cannot* corrupt checkpoint identity.

- `SubmittedRecords`: per-source-partition FIFO deques with out-of-order acks and a commit watermark that advances only over the contiguous acknowledged prefix. A correct, fully generic low-water-mark algorithm, ~200 lines.

- `OffsetStorageWriter`'s double-buffered flush (`beginFlush` swaps under a `Semaphore(1)`, `doFlush` writes async, `cancelFlush` merges back) so new offsets accrue during a flush and a failed flush loses nothing.

- `SinkTask.open(partitions)`/`close(partitions)` as an assignment-scoped resource lifecycle *distinct from* `start`/`stop` — the task learns exactly when it gains and loses ownership of a unit of work.

- Uniform plugin self-description: `ConnectPlugin extends Versioned { ConfigDef config(); String version(); }` implemented by connectors, tasks, converters, header converters, transforms, and predicates alike — one introspection path for the UI across every extension point.

- Capability negotiation as pure functions of config, callable before `start()`: `exactlyOnceSupport(config)`, `canDefineTransactionBoundaries(config)`, `validate(config)`, `alterOffsets(config, offsets)` — the runtime can refuse an impossible deployment at submit time instead of at 3am.

- Standalone vs distributed differ only in three swappable storage implementations plus the herder; the connector-facing API is byte-identical, so `FileOffsetBackingStore` for a laptop and a compacted Kafka topic for a cluster are the same code path.

- `errors.tolerance` + DLQ + `ErrantRecordReporter` as framework-level concerns: per-record error routing with rich provenance headers, so connectors don't each invent a poison-message policy.

- The `commit-{connector}` marker record making a multi-record task-config write atomic to readers — a clean way to get set-atomicity out of a log that has no multi-record transactions.

- KIP-415's assignor deliberately *delaying* reassignment of orphaned work (`scheduled.rebalance.max.delay.ms`) with exponential backoff on consecutive revoking rounds, so a bouncing worker reclaims its own tasks instead of triggering a cluster-wide reshuffle.

- Duty-cycle metrics (`running-ratio`, `pause-ratio`) and in-flight depth (`*-active-count`) alongside the usual rates and totals, plus rebalance as its own first-class metric group.

- `PluginMetrics` via the context: the plugin declares metrics, the core owns naming, tagging (`connector`, `task`, `converter=key|value|header`), and export.


## What They Got Wrong

- `List<SourceRecord> poll()` has no flow control whatsoever. `AbstractWorkerSourceTask` calls `poll()` again the instant the prior batch reaches the producer (`if (toSend == null) { toSend = poll(); } ... sendRecords()`, with a hardcoded `SEND_FAILED_BACKOFF_MS = 100` as the only pause). Every connector reinvents `poll.interval.ms`. KIP-731 exists to fix this and has been 'Under Discussion' since 2021, naming the 'noisy neighbor' and 'firehose' problems: 'Connect tends to run full-throttle and can overwhelm downstream systems.'

- `void put(Collection<SinkRecord>)` cannot express partial acceptance or backpressure. There is no return value, so the only signals are throwing `RetriableException`, `context.timeout(ms)`, and `context.pause(...)`. Meanwhile the source side has no equivalent of even those.

- `poll()` must 'block but return control to the caller regularly (by returning null)' so the task can be PAUSED. Cooperative cancellation via a sentinel return value, with no `Context`, no deadline, and no cancellation channel anywhere in the API. `stop()` is documented as arriving 'from a different thread than poll() and commit()'.

- The task lifecycle has no safe teardown point, and Kafka knows it. KIP-419 (KAFKA-7841) states `stop()` 'can actually be called more than once in some situations for the same SourceTask' and that afterwards 'any of poll(), commit() and commitRecord() can still be called.' It proposes a guaranteed-final `stopped()` callback and has been 'Under Discussion' for ~7 years; I verified `stopped()` is absent from trunk.

- Source offset commits are on a wall-clock timer (`offset.flush.interval.ms`, default 60000) with a 5000 ms `offset.flush.timeout.ms`, decoupled from poll batches and from any notion of a logical boundary. The commit path calls `SubmittedRecords.awaitAllMessages(timeout)` — i.e. it waits for the in-flight queue to drain — producing the ecosystem's most famous log line, 'Failed to flush, timed out while waiting for producer to flush outstanding N messages' (KAFKA-4942), which users routinely try to fix by tuning `offset.flush.timeout.ms` when the actual fix is producer `batch.size`/`linger.ms`.

- Three overlapping, differently-scoped commit hooks with no clear contract: `SourceTask.commit()` ('the offsets being committed won't necessarily correspond to the latest offsets returned by this source task via poll()'), `SourceTask.commitRecord(record, metadata)` (where `metadata` is null both for transform-filtered records and for records dropped under `errors.tolerance=all` — two very different events, indistinguishable), and on the sink side `flush()` vs `preCommit()` where `preCommit()` returning `{}` silently means 'I own offset management now'.

- `SinkRecord` had to grow `originalTopic()`, `originalKafkaPartition()`, `originalKafkaOffset()` (KIP-793, Kafka 3.6) because SMTs mutate `topic`/`kafkaPartition` while offset accounting needs pre-transform coordinates. KIP-793: 'the topic/partition/offset it can return in preCommit will correspond to the transformed topic/partition/offset, when the framework actually expects it to be the original.' Record identity and record payload were conflated; the fix is a prose warning in two javadocs.

- Nothing in the framework understands a snapshot. No `COMPLETED` state (`AbstractStatus.State` has none), no bounded-task concept, no split/chunk abstraction, no phase model, no way for `poll()` to say 'exhausted'. Debezium consequently smuggles incremental-snapshot context through the `sourceOffset` map (restricted to 'Maps of Strings to primitive types') and had to invent an out-of-band signalling table / Kafka signal topic (`execute-snapshot`, `pause-snapshot`, `resume-snapshot`) because Connect offers no channel to command a running task.

- Work is split exactly once, at connector start, by `taskConfigs(maxTasks)`. Re-splitting requires `context.requestTaskReconfiguration()`, which restarts tasks. There is no dynamic work-stealing and no split enumerator, so 'snapshot with 20 workers then stream with 2' is inexpressible.

- `taskConfigs(int maxTasks)` passed `tasks.max` as a suggestion for eight years. KIP-1004 (Kafka 3.8) had to make it enforced because 'buggy or hostile connectors could generate excessive task configurations, threatening cluster stability' — with a deprecated `tasks.max.enforce=false` escape hatch for the connectors already violating it.

- Stop-the-world rebalancing was the original design. KIP-415: stopping the world on every config submit or worker change 'could lead to significant delays, and in some cases — also known as rebalance storms — it could bring the cluster into a state of consecutive rebalances and the Connect cluster could take several minutes to stabilize.' The incremental cooperative replacement then shipped its own bug class, e.g. KAFKA-12495, 'Unbalanced connectors/tasks distribution will happen in Connect's incremental cooperative assignor.'

- The config topic is a single-partition compacted Kafka topic, and `KafkaConfigBackingStore`'s own javadoc documents an unrecoverable failure mode: compaction plus a partial write can leave a `commit-foo (2 tasks)` record whose `task-foo-1-config` has been compacted away, or a mid-write leader failure with no second commit, 'leaving us in an inconsistent state with no obvious way to resolve the issue.' The class has to expose which connectors are inconsistent so the herder can paper over it.

- `Transformation<R>` is `R apply(R record)` — strictly 1→1 (or 1→0 by returning null), synchronous, stateless, on the task thread. No 1→N, no N→1, no aggregation, no joins, no async external lookups, no windowing. Conditional application had to be bolted on later (KIP-585 `Predicate`), and even that allows exactly one predicate per transform with no boolean combinators — 'multiple predicates require separate transform chain entries.'

- The plugin boundary is a hard Java ABI. Adding `pluginMetrics()` to `ConnectorContext` forced this into the official javadoc: connectors 'should guard the call to this method with a try-catch block, since calling this method will result in a NoSuchMethodError or NoClassDefFoundError when deployed to Connect runtimes older than Kafka 4.1' — with a literal `catch (NoSuchMethodError | NoClassDefFoundError e)` snippet. Same for `errantRecordReporter()` and `transactionContext()`.

- Plugin discovery is still mid-migration years on: `plugin.discovery ∈ {ONLY_SCAN, HYBRID_WARN, HYBRID_FAIL, SERVICE_LOAD}` defaults to `HYBRID_WARN`, because classpath scanning is slow but the ecosystem's JARs lack `META-INF/services` files.

- No process isolation, acknowledged as unfixable within the model. KIP-987 (Under Discussion): 'the shared-JVM model does not permit strong isolation between threads', so it proposes pinning jobs to workers to get resource limits 'at the process-boundary'. The de-facto answer is one Connect cluster per workload — abandoning the shared cluster the architecture exists for.

- `Class<? extends Task> taskClass()`, `Map<String, ?>` payloads, behavioural `Schema`/`Struct` interfaces, `Future<Void>`/`Callback<Void>`, and no `Context` together make the interface structurally unshippable over a wire. There is no out-of-process Connect connector and there cannot be one without changing the interface.

- Exactly-once for sources is cluster-wide all-or-nothing, distributed-mode-only, and requires a two-phase rolling upgrade through `exactly.once.source.support = DISABLED → PREPARING → ENABLED`, plus a zombie-fencing protocol (`tasks-fencing-` records, an internal `PUT /connectors/{c}/fence`, `Admin.fenceProducers()`). Bolting transactions onto an interface that never had a commit-boundary concept cost an enormous amount of machinery.

- There is no transactional sink at all — no `prepare()`/`commit()` pair, no recoverable committables. `preCommit()` is a one-shot 'which offsets are safe', not a 2PC prepare, so sink exactly-once is 'be idempotent, or take over offset management entirely and store offsets in your destination.'

- `exactlyOnceSupport(config)` defaults to returning **null**, meaning 'assume unsupported' — a tri-state capability encoded as a nullable enum for backwards compatibility, exactly the shape a separately-assertable optional interface would have avoided.

- Metrics are JMX-only; there is no HTTP metrics endpoint, so every deployment bolts on an exporter. And there is no source-side lag metric (only `sink-record-lag-max`, which exists solely because Kafka consumers have a log-end offset) — a direct consequence of the framework not understanding the offset map it stores.

- Everything crosses the boundary as `Map<String,String>`, re-parsed inside the task via `AbstractConfig`, and nested/repeated config is faked with dotted prefixes (`transforms=a,b` + `transforms.a.type=...`) which `ConfigDef` cannot describe — hence bespoke `enrich()` logic in `ConnectorConfig` for transforms and predicates.


## Steal This

- Split every connector into a planner and a worker: the planner turns one config into N opaque, serialisable work assignments (Connect's `taskConfigs(maxTasks)`), and the core alone decides which process runs which assignment — neither side learns the other's algorithm.

- Make the checkpoint an opaque, connector-authored `(stream identity, position)` pair that the core stores, batches, commits, exposes over the API, and lets operators edit — but unlike Connect give it a comparable position type so the core can compute lag and progress generically.

- Copy `SubmittedRecords` outright: a per-stream FIFO of in-flight records, acked out of order from the sink, with the commit watermark advancing only over the contiguous acknowledged prefix — it is the whole at-least-once correctness argument in ~200 lines and it is sink-agnostic.

- Copy `OffsetStorageWriter`'s double-buffered flush: a `beginFlush` that atomically swaps the pending map aside, an async `doFlush`, and a `cancelFlush` that merges the batch back — so progress keeps accruing during a flush and a failed flush loses nothing.

- Persist checkpoints through a bytes-in/bytes-out store interface (`Get([]key) ([]value, error)`, `Set(map[key]value) error`) so a file, BoltDB, Postgres, or a Kafka topic are drop-in substitutes and the durability layer never sees a domain type — this is exactly what makes Connect's standalone/distributed swap free.

- Declare config as a data structure with presentation metadata baked in (`group`, `orderInGroup`, `width`, `displayName`, `importance`, `dependents`, validator, recommender), and serve it verbatim over the API as definition+value pairs, so the frontend renders any connector with zero connector-specific code.

- Split validation into a pure declarative pass and an overridable `Validate(ctx, config)` that may do I/O, and return per-field diagnostics (`value`, `errors []string`, `recommendedValues`, `visible`) rather than a single error — forms need all failures at once.

- Steal `Recommender`: valid values and field visibility are functions of the *whole parsed config*, with an explicit `dependents` edge list, so 'if mode=timestamp then show and populate timestamp.column from the live DB' is expressible declaratively.

- Make codecs a pipeline-level plugin the connector never names, with a separate codec for record metadata/headers — Connect's `key.converter`/`value.converter`/`header.converter` triple is why any connector composes with any wire format.

- Carry schema in-band on every record while leaving schema *encoding* entirely to the codec (embedded literal, registry ID, or absent) — this is Connect's cleanest single decision.

- Make records immutable with an explicit copy-with-modifications constructor that structurally cannot alter provenance, so no transform can ever corrupt checkpoint identity — Connect gets this for free because `SourceRecord.newRecord()` forwards `sourcePartition`/`sourceOffset` unchanged.

- Add the one field Connect omitted: an operation/kind discriminator (and optional before-image slot) on the core envelope, so CDC semantics are expressible generically instead of every connector inventing an incompatible `{before, after, op}` struct inside the payload.

- Separate assignment-scoped lifecycle from instance-scoped lifecycle: `Open(assignedSplits)` / `Close(revokedSplits)` distinct from `Start` / `Stop`, so a worker learns exactly when it gains and loses ownership of a unit of work.

- Put every optional capability behind its own small, independently type-assertable Go interface (`Snapshotter`, `Transactional`, `RateLimited`, `OffsetAlterer`) rather than Connect's nullable-default methods on a fat interface — this is the only way to grow the plugin surface without breaking existing connectors, and Go has no `default` methods to fall back on.

- Keep every method `(ctx, requestStruct) → (responseStruct, error)` with plain serialisable structs on both sides — no `Class` handles, no closures, no futures, no behavioural `Schema`/`Struct` interfaces — so an in-process registry impl and a future gRPC shim satisfy the identical interface; Connect fails exactly this test and that is why out-of-process Connect does not exist.

- Thread `context.Context` through every blocking call and define exactly one terminal callback that is guaranteed-last and guaranteed-once — Connect's seven-year-unfixed KIP-419 is the cost of not doing this.

- Model capability negotiation as pure functions of config callable before start (`SupportsExactlyOnce(config)`, `SupportsSnapshot(config)`, `Validate(config)`), so an impossible pipeline is rejected at submit time, not at 3am.

- Make backpressure explicit in the interface rather than incidental: have the sink report how much it accepted (or hand the source a bounded, credit-issuing writer), so flow control is a first-class signal instead of the producer's buffer filling up — Connect's `void put(...)` plus `List poll()` cannot express it and KIP-731 has been stuck for five years proving it.

- Model phases and splits in the core: a source enumerates splits, splits are individually assigned and checkpointed, and a split may report exhaustion — snapshot-then-stream then falls out as 'the split set changed' instead of being smuggled through the offset map and driven by an out-of-band signalling table.

- Give the core a genuine two-phase sink commit (`Prepare() → committable; Commit([]committable)`) with committables persisted in the same checkpoint as source positions — strictly more general than Connect's producer-transaction trick and it makes no assumption about what the destination is.

- Make the control-plane store transactional and versioned (etcd revision, or a row with a version column) rather than a compacted log needing marker records — `KafkaConfigBackingStore`'s own javadoc documents the unrecoverable compaction-plus-partial-write state that a real CAS store simply cannot enter.

- If work must be reassigned across workers, deliberately delay reassignment of orphaned work with exponential backoff on consecutive revoking rounds (KIP-415's `scheduledRebalance`/`delay`) so a restarting worker reclaims its own assignments instead of triggering a cluster-wide reshuffle.

- Hard-enforce the declared parallelism cap at plan time and fail the pipeline loudly if the planner exceeds it — KIP-1004 had to retrofit this after eight years of connectors treating `tasks.max` as advisory.

- Adopt Connect's metric vocabulary wholesale: rigid `<noun>-<aggregation>` naming, separate read-rate vs write-rate so the transform drop rate is visible as the gap, `*-active-count` for in-flight depth, duty-cycle ratios (`running-ratio`, `pause-ratio`), and coordination/rebalance as its own metric group — but expose it over HTTP natively instead of JMX-only.

- Let plugins register their own metrics through the context while the core owns naming, tagging, and export (Connect's `PluginMetrics`, auto-tagged with connector/task/codec-role).

- Make per-record error policy a framework concern with a DLQ and rich provenance headers (`__connect.errors.topic`, `.stage`, `.exception.class.name`, `.exception.stacktrace`) plus an `ErrantRecordReporter`-style handle so a sink can reject individual records without failing the pipeline — and unlike Connect, make it work for sources too, and give it a richer policy vocabulary than `{none, all}`.

- Register connectors in a plain init-time registry keyed by name+version, supporting multiple versions of one connector concurrently and version selection per pipeline (KIP-891's goal) — and skip classpath/ServiceLoader scanning entirely, which Connect is still migrating off after years of `plugin.discovery=HYBRID_WARN`.

- Add a terminal `COMPLETED` state to the status model (Connect has `UNASSIGNED/RUNNING/PAUSED/FAILED/RESTARTING/STOPPED` and no way for a bounded job to say it finished) so batch and snapshot pipelines are first-class rather than infinite streams that happen to go quiet.


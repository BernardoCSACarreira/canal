# Prior art: Apache Flink — checkpointing, Source API v2 (FLIP-27), Sink API v2 (FLIP-191/372), Flink CDC

> **Scope and provenance.** Primary-source analysis of `apache/flink` @ `release-1.20` and
> `apache/flink-cdc` @ `master` (July 2026), plus the Confluence FLIP pages for FLIP-27, FLIP-143,
> FLIP-191, FLIP-76 and FLIP-372. Every signature quoted below was read from a fetched source file,
> not recalled. Raw sources are cached locally at
> `/private/tmp/claude-501/-Users-bernardocarreira-Documents-personal-canal/9fad647d-7c79-4ad7-b63f-ba664a732b9c/scratchpad/fl/`
> (Flink core/runtime/streaming + Hugo docs markdown, filenames prefixed `src_`, `snk_`, `st_`,
> `rt_`, `core_`, `base_`, `cfg_`, `doc_`, `flip*`) and
> `.../scratchpad/cdc/f/` (Flink CDC `flink-cdc-base` sources + docs). Where I could not verify
> something from source I say so inline, and it is listed in the `unverified` return field.
>
> **Why this system matters to canal.** Flink is the only widely-deployed system in this space that
> models *work discovery*, *work assignment*, *resumable chunked snapshot*, *snapshot→stream handoff*
> and *two-phase sink commit* as **first-class, source/sink-agnostic core interfaces**. Kafka Connect
> has none of these (see `docs/research/kafka-connect.md`). Flink also carries the cost: three
> incompatible Sink APIs in eight minor releases, and a distributed-dataflow checkpoint protocol that
> a single-process Go tool should mostly *not* copy. This dossier separates the two.

---

## 0. The three-layer picture (read this first)

Flink has **three distinct mechanisms** that all get called "checkpointing", and conflating them is
the single biggest risk when borrowing from it:

| Layer | Mechanism | What it buys | Worth copying for canal? |
|---|---|---|---|
| **L1. Distributed snapshot** | Chandy-Lamport barriers injected at sources, aligned or unaligned, coordinated by `CheckpointCoordinator` on the JobManager | A *globally consistent cut* across an arbitrary operator DAG with shuffles, keyed state and cycles | **No** — justified only by a distributed dataflow engine. canal's DAG is a pipeline, not a shuffled graph. |
| **L2. Per-operator state snapshot + confirm** | `CheckpointedFunction.snapshotState/initializeState`, `CheckpointListener.notifyCheckpointComplete/Aborted`, the **Checkpoint Subsuming Contract** | Durable connector-authored state, restart, and a *"the checkpoint is durable now, you may commit externally"* signal | **Yes, wholesale.** This is the shape of canal's checkpoint interface. |
| **L3. Split enumeration + committables** | `SplitEnumerator`/`SourceReader`/`SourceSplit`; `SinkWriter.prepareCommit` → `Committer.commit`; Flink CDC's chunk/watermark/backfill algorithm | Parallel resumable snapshots, dynamic work assignment, snapshot→stream handoff, exactly-once sinks over non-transactional-middle systems | **Yes, this is the crown jewel.** Independent of L1. |

L3 does **not** depend on L1. `SplitEnumerator.snapshotState(long checkpointId)` only needs *some*
monotonic snapshot id and *some* durable store; the barrier protocol is how Flink obtains a
consistent id, not what makes splits work. That decoupling is the load-bearing insight of this whole
document.

---

## 1. Core interfaces

### 1.1 Source (FLIP-27) — the factory

`flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java`, `@Public`:

```java
@Public
public interface Source<T, SplitT extends SourceSplit, EnumChkT>
        extends SourceReaderFactory<T, SplitT> {

    Boundedness getBoundedness();

    SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext)
            throws Exception;

    SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception;

    // ------------------------------------------------------------------------
    //  serializers for the metadata
    // ------------------------------------------------------------------------

    SimpleVersionedSerializer<SplitT> getSplitSerializer();

    SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer();
}
```

and the separately-extracted factory (`SourceReaderFactory.java`, `@Public`):

```java
@Public
public interface SourceReaderFactory<T, SplitT extends SourceSplit> extends Serializable {
    SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception;
}
```

Four things to notice, all deliberate:

1. **`Source` is a serialisable factory, not a runtime object.** Three generic parameters:
   record type `T`, split type `SplitT`, enumerator-checkpoint type `EnumChkT`.
2. **`createEnumerator` and `restoreEnumerator` are separate methods.** Cold start and warm start are
   different code paths with different signatures — the connector cannot accidentally treat "no
   state" and "restored state" the same way. Contrast Kafka Connect's single `start(props)`.
3. **The `Source` supplies its own serialisers for both the split and the enumerator checkpoint.**
   The framework owns durability; the connector owns encoding. This is the seam that makes the
   framework fully connector-agnostic while still persisting connector state.
4. `getBoundedness()` is a *property of the source instance*, declared before anything runs.

`SourceSplit` is deliberately almost empty (`SourceSplit.java`, `@Public`):

```java
@Public
public interface SourceSplit {
    String splitId();
}
```

`Boundedness` (`@Public`) has exactly two values, `BOUNDED` and `CONTINUOUS_UNBOUNDED`, with this
javadoc on `BOUNDED`: *"a BOUNDED stream expects the source to put a boundary of the records it
emits. Such boundaries could be number of records, number of bytes, elapsed time…"*, and on
`CONTINUOUS_UNBOUNDED`: *"A CONTINUOUS_UNBOUNDED stream may also eventually stop at some point. But
before that happens, Flink always assumes the sources are going to run forever."*

### 1.2 SplitEnumerator — the planner, and the piece Kafka Connect has no analogue for

`SplitEnumerator.java`, `@Public`:

```java
@Public
public interface SplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends AutoCloseable, CheckpointListener {

    void start();

    void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

    void addSplitsBack(List<SplitT> splits, int subtaskId);

    void addReader(int subtaskId);

    CheckpointT snapshotState(long checkpointId) throws Exception;

    @Override
    void close() throws IOException;

    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {}
}
```

The `snapshotState` javadoc states the invariant that makes the whole thing correct:

> *"The snapshot should contain the latest state of the enumerator: It should assume that all
> operations that happened before the snapshot have successfully completed. For example all splits
> assigned to readers via `SplitEnumeratorContext#assignSplit(SourceSplit, int)` and
> `assignSplits(SplitsAssignment)`) don't need to be included in the snapshot anymore."*

Combined with `addSplitsBack(List<SplitT> splits, int subtaskId)` — *"This will only happen when a
`SourceReader` fails and there are splits assigned to it after the last successful checkpoint"* —
this gives **exactly-once split ownership without a distributed lock**: the enumerator's snapshot
excludes assigned splits, the reader's snapshot includes them, and on reader failure the *reader's*
last-checkpointed splits come back to the enumerator. Ownership of a split is transferred atomically
with the snapshot, never with a message.

### 1.3 SourceReader — the worker

`SourceReader.java`, `@Public`:

```java
@Public
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    void start();

    InputStatus pollNext(ReaderOutput<T> output) throws Exception;

    List<SplitT> snapshotState(long checkpointId);

    CompletableFuture<Void> isAvailable();

    void addSplits(List<SplitT> splits);

    void notifyNoMoreSplits();

    default void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @PublicEvolving
    default void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        throw new UnsupportedOperationException(/* long message, see §13 */);
    }
}
```

`pollNext` javadoc: *"The implementation must make sure this method is non-blocking. … it is
recommended not doing so. Instead, emit one record into the ReaderOutput and return a
`InputStatus#MORE_AVAILABLE` to let the caller thread know there are more records available."*

`InputStatus` (`flink-core/.../core/io/InputStatus.java`) is a three-valued enum:
`MORE_AVAILABLE`, `NOTHING_AVAILABLE`, `END_OF_INPUT`. This is the **non-blocking pull protocol**,
and it is much better than `List<SourceRecord> poll()`: the reader tells the runtime *whether to come
back immediately, wait on a future, or stop forever*. `END_OF_INPUT` is the thing Kafka Connect
cannot express at all.

`isAvailable()` javadoc, verbatim, is the contract that makes the future-based readiness signal safe:

> *"The contract is the following: If the reader has data available, then all futures previously
> returned by this method must eventually complete. Otherwise the source might stall indefinitely.
> … It is not a problem to have occasional 'false positives', meaning to complete a future even if
> no data is available. However, one should not use an 'always complete' future in cases no data is
> available, because that will result in busy waiting loops."*

**Crucially: `snapshotState(long checkpointId)` returns `List<SplitT>`.** The reader's checkpoint
*is its split set*. There is no separate "offset" concept — position lives inside the split. That
single decision is what makes snapshot resume, stream resume and rebalance the same mechanism.

### 1.4 Contexts (the runtime's capability grant)

`SourceReaderContext.java` (`@Public`):

```java
@Public
public interface SourceReaderContext {
    SourceReaderMetricGroup metricGroup();
    Configuration getConfiguration();
    String getLocalHostName();
    int getIndexOfSubtask();
    void sendSplitRequest();
    void sendSourceEventToCoordinator(SourceEvent sourceEvent);
    UserCodeClassLoader getUserCodeClassLoader();
    default int currentParallelism() { throw new UnsupportedOperationException(); }
}
```

`SplitEnumeratorContext.java` (`@Public`) — abridged to the load-bearing members:

```java
@Public
public interface SplitEnumeratorContext<SplitT extends SourceSplit> {
    SplitEnumeratorMetricGroup metricGroup();
    void sendEventToSourceReader(int subtaskId, SourceEvent event);
    default void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        throw new UnsupportedOperationException();
    }
    int currentParallelism();
    Map<Integer, ReaderInfo> registeredReaders();
    default Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        throw new UnsupportedOperationException();
    }
    void assignSplits(SplitsAssignment<SplitT> newSplitAssignments);
    default void assignSplit(SplitT split, int subtask) {
        assignSplits(new SplitsAssignment<>(split, subtask));
    }
    void signalNoMoreSplits(int subtask);
    <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler);
    <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler,
                       long initialDelayMillis, long periodMillis);
    void runInCoordinatorThread(Runnable runnable);
    @PublicEvolving
    default void setIsProcessingBacklog(boolean isProcessingBacklog) {
        throw new UnsupportedOperationException();
    }
}
```

The class javadoc names three purposes, the third of which is the interesting one:

> *"1. Host information necessary for the SplitEnumerator to make split assignment decisions.
> 2. Accept and track the split assignment from the enumerator.
> 3. **Provide a managed threading model so the split enumerators do not need to create their own
> internal threads.**"*

`callAsync` + `runInCoordinatorThread` mean **the enumerator is single-threaded user code**, and all
mutation of enumerator state is serialised onto one coordinator thread. `runInCoordinatorThread`
javadoc: *"Instead of using lock for thread safety, this API allows to run such externally triggered
action in the coordinator thread. Hence, we can ensure all enumerator actions are serialized in the
single coordinator thread."* The `callAsync` javadoc warns: *"It is important to make sure that the
callable does not modify any shared state, especially the states that will be a part of the
`SplitEnumerator#snapshotState(long)`."*

**This is directly transplantable to Go and is nicer in Go than in Java**: the enumerator becomes a
goroutine owning its state, driven by a single `select` over a command channel. No mutexes, no
"don't touch shared state" prose warning.

`SplitsAssignment` is a plain value with an important documented semantic:

```java
@Public
public final class SplitsAssignment<SplitT extends SourceSplit> {
    private final Map<Integer, List<SplitT>> assignment;
    public Map<Integer, List<SplitT>> assignment();
}
```
> *"The assignment is always incremental. In another word, splits in the assignment are simply added
> to the existing assignment."*

`SourceEvent` is the entire out-of-band control channel, and it is one line:

```java
@Public
public interface SourceEvent extends Serializable {}
```

An empty marker interface, bidirectional (`sendEventToSourceReader` / `sendSourceEventToCoordinator`).
Compare Kafka Connect, which had to invent a **database signalling table** because it has no such
channel (Debezium `execute-snapshot`/`pause-snapshot`). One empty interface replaces that entire
out-of-band control plane.

### 1.5 The output side of the source

`ReaderOutput.java` extends `SourceOutput<T>` (`@Public`):

```java
@Public
public interface ReaderOutput<T> extends SourceOutput<T> {
    @Override void collect(T record);
    @Override void collect(T record, long timestamp);
    @Override void emitWatermark(Watermark watermark);
    @Override void markIdle();

    SourceOutput<T> createOutputForSplit(String splitId);
    void releaseOutputForSplit(String splitId);
}
```

The per-split output is the mechanism for **per-split watermarks and per-split idleness** —
FLIP-27's stated motivation #3. And the javadoc carries the leak warning:

> *"**IMPORTANT:** After the split has been finished, it is crucial to release the created output
> again. Otherwise it will continue to contribute to the watermark generation like a perpetually
> stalling source split, and may hold back the watermark indefinitely."*

### 1.6 Sink API v2, as of 1.19+/1.20 (FLIP-372 mixin form)

Base sink, `flink-core/.../api/connector/sink2/Sink.java`, `@Public`:

```java
@Public
public interface Sink<InputT> extends Serializable {

    @Deprecated
    SinkWriter<InputT> createWriter(InitContext context) throws IOException;

    default SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return createWriter(new InitContextWrapper(context));
    }
}
```

Class javadoc: *"A basic `Sink` is a stateless sink that can flush data on checkpoint to achieve
at-least-once consistency. Sinks with additional requirements should implement `SupportsWriterState`
or `SupportsCommitter`."* And: *"The `Sink` needs to be serializable. All configuration should be
validated eagerly. The respective sink writers are transient and will only be created in the
subtasks on the taskmanagers."*

`SinkWriter.java`, `@Public`:

```java
@Public
public interface SinkWriter<InputT> extends AutoCloseable {

    void write(InputT element, Context context) throws IOException, InterruptedException;

    /**
     * Called on checkpoint or end of input so that the writer to flush all pending data for
     * at-least-once.
     */
    void flush(boolean endOfInput) throws IOException, InterruptedException;

    default void writeWatermark(Watermark watermark) throws IOException, InterruptedException {}

    @Public
    interface Context {
        long currentWatermark();
        Long timestamp();
    }
}
```

The two-phase-commit mixins:

```java
@Public
public interface SupportsCommitter<CommittableT> {
    Committer<CommittableT> createCommitter(CommitterInitContext context) throws IOException;
    SimpleVersionedSerializer<CommittableT> getCommittableSerializer();
}

@Public
public interface CommittingSinkWriter<InputT, CommittableT> extends SinkWriter<InputT> {
    /**
     * Prepares for a commit.
     * <p>This method will be called after {@link #flush(boolean)} and before {@link
     * StatefulSinkWriter#snapshotState(long)}.
     */
    Collection<CommittableT> prepareCommit() throws IOException, InterruptedException;
}

@Public
public interface SupportsWriterState<InputT, WriterStateT> {
    StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
            WriterInitContext context, Collection<WriterStateT> recoveredState) throws IOException;
    SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();

    @PublicEvolving
    interface WithCompatibleState {
        Collection<String> getCompatibleWriterStateNames();
    }
}

@Public
public interface StatefulSinkWriter<InputT, WriterStateT> extends SinkWriter<InputT> {
    List<WriterStateT> snapshotState(long checkpointId) throws IOException;
}
```

**The strict ordering `flush(endOfInput)` → `prepareCommit()` → `snapshotState(checkpointId)` is
documented in the interface itself**, and it is the correct order: flush buffers, mint the
committables, then persist writer state *including* the fact that those committables exist.

`Committer.java`, `@Public`, is the piece to steal outright:

```java
@Public
public interface Committer<CommT> extends AutoCloseable {

    void commit(Collection<CommitRequest<CommT>> committables)
            throws IOException, InterruptedException;

    @Public
    interface CommitRequest<CommT> {
        CommT getCommittable();
        int getNumberOfRetries();
        void signalFailedWithKnownReason(Throwable t);
        void signalFailedWithUnknownReason(Throwable t);
        void retryLater();
        void updateAndRetryLater(CommT committable);
        void signalAlreadyCommitted();
    }
}
```

Class javadoc states the idempotency requirement plainly:

> *"A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
> restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all
> committables may have already been committed. These `CommitRequest`s must not change the external
> system and implementers are asked to signal `CommitRequest#signalAlreadyCommitted()`."*

`CommitRequest` is a **six-outcome per-item result type**, not a boolean and not an exception. This
directly answers canal's design-rule R7 ("if the design says retry the failures, the contract must
name them"): the failure shape is written into the interface, per committable, including the
partial-success case (`updateAndRetryLater(CommT)` — *"This method can be used if a committable
partially succeeded"*).

`WriterInitContext`/`CommitterInitContext` both extend an `@Internal` `InitContext`:

```java
@Internal
public interface InitContext {
    long INITIAL_CHECKPOINT_ID = 1;
    OptionalLong getRestoredCheckpointId();
    @PublicEvolving JobInfo getJobInfo();
    @PublicEvolving TaskInfo getTaskInfo();
    // getSubtaskId/getNumberOfParallelSubtasks/getAttemptNumber/getJobId all @Deprecated,
    // defaulting through getTaskInfo()/getJobInfo() per FLIP-382
}
```

`CommitterInitContext` adds exactly one method: `SinkCommitterMetricGroup metricGroup();`.
`getRestoredCheckpointId()` returning `OptionalLong` is the "am I a cold start or a restore, and from
where" signal, expressed as data rather than as two constructors.

Three optional topology mixins exist in `flink-streaming-java`
(`org.apache.flink.streaming.api.connector.sink2`), all `@Experimental`:

```java
@Experimental
public interface SupportsPreWriteTopology<InputT> { /* see §8 */ }

@Experimental
public interface SupportsPreCommitTopology<WriterResultT, CommittableT> {
    DataStream<CommittableMessage<CommittableT>> addPreCommitTopology(
            DataStream<CommittableMessage<WriterResultT>> committables);
    SimpleVersionedSerializer<WriterResultT> getWriteResultSerializer();
}

@Experimental
public interface SupportsPostCommitTopology<CommittableT> {
    void addPostCommitTopology(DataStream<CommittableMessage<CommittableT>> committables);
}
```

`SupportsPostCommitTopology.addPostCommitTopology` javadoc: *"All operations need to be idempotent:
on recovery, any number of committables may be replayed that have already been committed. It's
mandatory that these committables have no effect on the external system."*

These three leak `DataStream` (a Flink dataflow type) into the sink SPI. **That is the exact mistake
canal must not make** — see §10 and §13.

### 1.7 The pattern to extract from §1

Flink's connector SPI is: **one tiny required interface per role, plus N independently-implementable
`Supports*` mixins, plus a context interface the *core* implements.** FLIP-372 states this as an
explicit design rule:

> *"Every new feature should be added with a `Supports<FeatureName>` interface (similar to the Source
> API), like `SupportsCommiter`, `SupportsWriterState`, `SupportsPreWriteTopology`,
> `SupportsPreCommitToplogy`, `SupportsPostCommitTopology`"*
> *"No redefining interface methods during interface inheritance — it would prevent future
> deprecation"*
> *"Minimal inheritance extension — for more flexibility in the future."*

In Go this is `interface{ ... }` + type assertion, and it is *strictly better* than Java's version
because Go has no `default` methods to abuse (see §10). This is the single most important structural
lesson in the dossier.

---

## 2. Record model

### 2.1 Flink core: there is no record model, and that is the point

The DataStream API is generic over `T`. `Source<T, SplitT, EnumChkT>` produces `T`; `Sink<InputT>`
consumes `InputT`; `ReaderOutput<T>.collect(T)`. **The core framework has no envelope at all** — no
key, no headers, no operation type, no schema. The only universal metadata in the whole pipeline is:

- an optional `long timestamp` on each record — `collect(T record)` vs `collect(T record, long timestamp)`;
- `Watermark` (event-time progress) as an out-of-band stream element;
- an idleness signal, `markIdle()`.

`SinkWriter.Context` exposes exactly two things back to the sink:

```java
interface Context {
    long currentWatermark();
    Long timestamp();     // nullable — "or null if the element does not have an assigned timestamp"
}
```

That is the *entire* per-record metadata surface of Flink's connector API. Everything else is inside
`T`, which the connector and the user's job agree on privately.

**Consequence, and it is a real cost:** because there is no core envelope, every Flink connector
family invented its own. Flink CDC had to build a whole second record model on top (below). Table/SQL
built `RowData`. This is the same failure mode as Kafka Connect's missing op-type field, arrived at
from the opposite direction: Connect has an envelope with no CDC semantics; Flink has no envelope at
all.

For canal (design rule **R2**, "the canonical record model is decided first"): Flink is evidence that
*"generic over T"* is not an answer. A data-movement tool must name the envelope, or every connector
pair will need a bespoke adapter.

### 2.2 Flink CDC's event model — the CDC-shaped layer Flink core lacks

From `flink-cdc/docs/content/docs/developer-guide/understand-flink-cdc-api.md` (verbatim):

> *"Each change event contains the table ID it belongs to, and the payload that the event carries."*
>
> **`DataChangeEvent`** — *"It consists of 5 fields:"*
> - *`Table ID`: table ID it belongs to*
> - *`Before`: pre-image of the data*
> - *`After`: post-image of the data*
> - *`Operation type`: type of the change operation*
> - *`Meta`: metadata of the change*
>
> *"For the operation type field, we pre-define 4 operation types:"*
> - *Insert: new data entry, with `before = null` and `after = new data`*
> - *Delete: removal of data, with `before = removed` data and `after = null`*
> - *Update: update of existed data, with `before = data before change` and `after = data after change`*
> - *Replace:* (sic — the upstream doc leaves this one blank)
>
> **`SchemaChangeEvent`** — `AddColumnEvent`, `AlterColumnTypeEvent`, `CreateTableEvent`,
> `DropColumnEvent`, `RenameColumnEvent`.

So: **schema changes are records in the same stream as data changes.** And the schema is
*deliberately not* attached to the data event, with an explicit stated trade-off:

> *"As you may have noticed, data change event doesn't have its schema bound with it. This reduces the
> size of data change event and the overhead of serialization, but makes it not self-descriptive.
> Then how does the framework know how to interpret the data change event?
>
> To resolve the problem, the framework adds a requirement to the flow of events: a
> `CreateTableEvent` must be emitted before any `DataChangeEvent` if a table is new to the framework,
> and `SchemaChangeEvent` must be emitted before any `DataChangeEvent` if the schema of a table is
> changed. This requirement makes sure that the framework has been aware of the schema before
> processing any data changes."*

This is the **in-band-but-not-per-record** schema model, and it is the third distinct option beyond
Kafka Connect's per-record schema and a registry:

| Model | Cost | Used by |
|---|---|---|
| Schema attached to every record | serialisation overhead, bloat | Kafka Connect `ConnectRecord` |
| Schema out-of-band in a registry | external dependency, drift | Confluent SR |
| **Schema as an ordered in-band event, records reference the last-seen schema** | ordering becomes load-bearing; a lost/reordered schema event corrupts interpretation | **Flink CDC** |

I did **not** read the literal Java of `DataChangeEvent`/`SchemaChangeEvent`/`OperationType` from
source — the five-field/four-op description above is quoted from the official developer guide, not
from the class. Treat exact field names and the `Replace` semantics as unverified.

### 2.3 The `flink-cdc-base` (Flink-source-level) record model, which *was* read from source

The lower-level CDC connectors reuse **Kafka Connect's `SourceRecord`** as the in-flight type and
batch it:

```java
// flink-cdc-base/.../source/meta/split/SourceRecords.java  (verified to exist; wraps a List<SourceRecord>)
// used as:  RecordEmitter<SourceRecords, T, SourceSplitState>
public class IncrementalSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, SourceSplitState> {

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), output, splitState);
        }
    }
```

and the dispatch inside `processElement` is a **five-way classification of the wire record**:

```java
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getWatermark(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            TableChanges changes = getTableChangeRecord(element);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                emitElement(element, output);
            }
        } else if (isDataChangeRecord(element)) {
            updateStreamSplitState(splitState, element);
            reportMetrics(element);
            emitElement(element, output);
        } else if (isHeartbeatEvent(element)) {
            updateStreamSplitState(splitState, element);
        } else {
            // unknown element
            LOG.info("Meet unknown element {} for splitState = {}, just skip.", element, splitState);
            sourceReaderMetrics.addNumRecordsInErrors(1L);
        }
    }
```

**Five in-band record kinds: watermark/control, schema change, data change, heartbeat, unknown.** Three of
them (`watermark`, `heartbeat`, `unknown`) never reach the user and exist purely to advance state.
And note `updateStreamSplitState`: **every data record and every heartbeat mutates the split's
position**:

```java
    protected void updateStreamSplitState(SourceSplitState splitState, SourceRecord element) {
        if (splitState.isStreamSplitState()) {
            Offset position = getOffsetPosition(element);
            splitState.asStreamSplitState().setStartingOffset(position);
        }
    }
```

That is the whole "position tracking" mechanism: the split state is a *mutable* mirror of the
immutable split, advanced per record, and serialised at checkpoint. `SourceSplitState` /
`SnapshotSplitState` / `StreamSplitState` are the mutable counterparts of `SourceSplitBase` /
`SnapshotSplit` / `StreamSplit`.

**Steal the split/split-state pair.** An immutable assignment (`Split`) plus a mutable in-reader
cursor (`SplitState`) that serialises back to a `Split` at checkpoint is a cleaner factoring than
Kafka Connect's "opaque offset map attached to each record", because the position is *one object per
unit of work* rather than *one map per record*.

Also note the heartbeat: a record kind whose only job is to advance the checkpointable position when
no data is flowing. Flink CDC's MySQL doc explains why it must exist:

> *"If the table updates infrequently, the binlog file or GTID set may have been cleaned in its last
> committed binlog position. The CDC job may restart fails in this case. So the heartbeat event will
> help update binlog position. By default heartbeat event is enabled in MySQL CDC source and the
> interval is set to 30 seconds."*

canal needs this concept in core: **a source must be able to advance its checkpoint without emitting
a record.** Otherwise an idle stream's stored position ages out of the upstream's retention window.

### 2.4 Raw bytes vs structured data

There is no coexistence problem, because Flink never sees the payload. Serialisation is a *job-level*
concern (`SerializationSchema`/`DeserializationSchema`), reached from the sink writer only as an
initialisation view:

```java
// Sink.InitContext (deprecated form) / WriterInitContext
SerializationSchema.InitializationContext asSerializationSchemaInitializationContext();
```

Flink CDC likewise takes the codec as a constructor parameter of the emitter, not as part of the
connector interface:

```java
protected final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
...
    protected void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        sourceReaderMetrics.markRecord();
        sourceReaderMetrics.updateRecordCounters(element);
        outputCollector.output = output;
        outputCollector.currentMessageTimestamp = getMessageTimestamp(element);
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }
```

The codec is injected, adapts `SourceOutput` to a `Collector<T>`, and the connector logic never names
a wire format. Same conclusion as Kafka Connect's `Converter`: **codec is a pipeline-level plugin,
injected, never named by the connector.** Two independent systems converged on this; canal should
just adopt it.

---

## 3. Checkpoint model

### 3.1 L1 — the barrier protocol, and what it actually buys

From `docs/content/docs/concepts/stateful-stream-processing.md`, verbatim:

> *"Flink's mechanism for drawing these snapshots is described in 'Lightweight Asynchronous Snapshots
> for Distributed Dataflows'. It is inspired by the standard Chandy-Lamport algorithm for distributed
> snapshots and is specifically tailored to Flink's execution model."*

The mechanism, quoting the doc:

> **Barriers.** *"A core element in Flink's distributed snapshotting are the stream barriers. These
> barriers are injected into the data stream and flow with the records as part of the data stream.
> **Barriers never overtake records, they flow strictly in line.** A barrier separates the records in
> the data stream into the set of records that goes into the current snapshot, and the records that go
> into the next snapshot. Each barrier carries the ID of the snapshot whose records it pushed in front
> of it. … Multiple barriers from different snapshots can be in the stream at the same time, which
> means that various snapshots may happen concurrently."*
>
> **Injection point = source position.** *"Stream barriers are injected into the parallel data flow at
> the stream sources. The point where the barriers for snapshot n are injected (let's call it Sn) is
> the position in the source stream up to which the snapshot covers the data. For example, in Apache
> Kafka, this position would be the last record's offset in the partition. This position Sn is
> reported to the checkpoint coordinator (Flink's JobManager)."*
>
> **Completion.** *"Once a sink operator (the end of a streaming DAG) has received the barrier n from
> all of its input streams, it acknowledges that snapshot n to the checkpoint coordinator. After all
> sinks have acknowledged a snapshot, it is considered completed."*
>
> **The property.** *"Once snapshot n has been completed, the job will never again ask the source for
> records from before Sn, since at that point these records (and their descendant records) will have
> passed through the entire data flow topology."*
>
> **Alignment.** *"As soon as the operator receives snapshot barrier n from an incoming stream, it
> cannot process any further records from that stream until it has received the barrier n from the
> other inputs as well. Otherwise, it would mix records that belong to snapshot n and with records
> that belong to snapshot n+1."*
>
> **Snapshot contents.** *"For each parallel stream data source, the offset/position in the stream when
> the snapshot was started. For each operator, a pointer to the state that was stored as part of the
> snapshot."*

`CheckpointBarrier` is a real class (`flink-runtime/.../io/network/api/CheckpointBarrier.java`)
carrying `(checkpointId, timestamp, CheckpointOptions)`, and `CheckpointOptions` carries the
alignment mode:

```java
// flink-runtime/.../checkpoint/CheckpointOptions.java
public enum AlignmentType {
    AT_LEAST_ONCE,
    ALIGNED,
    UNALIGNED,
    FORCED_ALIGNED
}
public static final long NO_ALIGNED_CHECKPOINT_TIME_OUT = Long.MAX_VALUE;
```

with a hard precondition that a savepoint can never be unaligned:

```java
        checkArgument(alignmentType != AlignmentType.UNALIGNED || !checkpointType.isSavepoint(), ...);
        checkArgument(alignedCheckpointTimeout == NO_ALIGNED_CHECKPOINT_TIME_OUT
                        || alignmentType != AlignmentType.UNALIGNED, ...);
```

**Unaligned checkpoints** (Flink 1.11+, FLIP-76), from the concepts doc:

> *"The basic idea is that checkpoints can overtake all in-flight data as long as the in-flight data
> becomes part of the operator state. Note that this approach is actually closer to the
> Chandy-Lamport algorithm, but Flink still inserts the barrier in the sources to avoid overloading
> the checkpoint coordinator."*
>
> *"The operator reacts on the first barrier that is stored in its input buffers. It immediately
> forwards the barrier to the downstream operator by adding it to the end of the output buffers. The
> operator marks all overtaken records to be stored asynchronously and creates a snapshot of its own
> state."*

And from `docs/content/docs/ops/state/checkpointing_under_backpressure.md`:

> *"when a Flink job is running under heavy backpressure, the dominant factor in the end-to-end time
> of a checkpoint can be the time to propagate checkpoint barriers to all operators/subtasks"*
>
> *"Unaligned checkpoints contain in-flight data (i.e., data stored in buffers) as part of the
> checkpoint state, allowing checkpoint barriers to overtake these buffers. Thus, **the checkpoint
> duration becomes independent of the current throughput** as checkpoint barriers are effectively not
> embedded into the stream of data anymore."*
>
> Timeout escalation: *"each checkpoint will still begin as an aligned checkpoint, but when the global
> checkpoint duration exceeds the `aligned-checkpoint-timeout`, if the aligned checkpoint has not
> completed, then the checkpoint will proceed as an unaligned checkpoint."*
> (`execution.checkpointing.aligned-checkpoint-timeout`)

### 3.2 What barriers buy that per-record offset commits do not — the direct answer

**A barrier-based distributed snapshot gives you a *causally consistent cut* across state that is
derived from records, not just a position in the input.**

Concretely, three properties, in order of how much canal should care:

1. **Consistency between input position and derived state.** A per-record/periodic offset commit says
   "I have read up to offset N". A barrier snapshot says "here is the operator state that results
   from having read *exactly* the records up to Sn, and nothing after". If a stage holds accumulated
   state (a dedupe set, a running aggregate, a per-key last-seen version, a buffer of pending
   committables), then committing an offset independently of that state is **wrong** — it is exactly
   canal's design rule **R5** failure (dedupe key recorded before the write). Barriers make
   "position" and "state" a single atomic artifact by construction.
   **This property canal needs.** It does not need barriers to get it: in a single process with a
   linear pipeline, you get it by writing `{source position, sink committables, dedupe watermark}`
   into **one** durable record.

2. **Consistency across *multiple* independent inputs joining at one operator.** This is what
   *alignment* specifically is for: *"Otherwise, it would mix records that belong to snapshot n and
   with records that belong to snapshot n+1."* Needed for joins, unions, fan-in with shared state.
   **canal needs this only if it has fan-in with shared state**, and even then a simpler
   "quiesce-then-snapshot" is adequate at single-process scale.

3. **Consistency without stopping the world, across a graph with shuffles and arbitrary
   parallelism.** The entire complexity of aligned-vs-unaligned, `CheckpointBarrierHandler`,
   in-flight-buffer persistence, `SharingFilesStrategy`, and the compatibility matrix in §13 exists
   to serve *this* property. **canal does not need it and must not pay for it.** A single-process Go
   tool can afford to briefly stop admitting new records to the pipeline while it takes a snapshot; a
   1000-subtask Flink job cannot.

**Verdict for canal:** adopt property 1 (atomic position+state), get it by writing one durable record
rather than by a barrier protocol, and keep the *interface* barrier-shaped (a monotonic
`checkpointID` handed to every stage's `Snapshot(id)`) so that a future distributed canal can insert
a real barrier protocol underneath **without changing the connector interface**. That is precisely
the property that makes Flink's L2/L3 interfaces reusable outside Flink.

### 3.3 L2 — the interface a connector actually implements

`flink-streaming-java/.../api/checkpoint/CheckpointedFunction.java`, `@Public` — two methods, and
that is the whole thing:

```java
@Public
public interface CheckpointedFunction {

    void snapshotState(FunctionSnapshotContext context) throws Exception;

    void initializeState(FunctionInitializationContext context) throws Exception;
}
```

Javadoc, on the intent of each:

> `initializeState`: *"This method is called when the parallel function instance is created during
> distributed execution. Functions typically set up their state storing data structures in this
> method."*
>
> `snapshotState`: *"This method is called when a snapshot for a checkpoint is requested. This acts as
> a hook to the function to ensure that all state is exposed by means previously offered through
> `FunctionInitializationContext` … In addition, **functions can use this method as a hook to
> flush/commit/synchronize with external systems.**"*

Note what `snapshotState` does *not* do: it does not return the state. State is *registered* at
init time in a framework-owned store and *made current* at snapshot time. The contexts:

```java
// flink-runtime/.../state/ManagedInitializationContext  (FunctionInitializationContext extends it)
boolean isRestored();
OperatorStateStore getOperatorStateStore();
KeyedStateStore getKeyedStateStore();

// ManagedSnapshotContext (FunctionSnapshotContext extends it)
long getCheckpointId();
long getCheckpointTimestamp();
```

*(Method names above read from the fetched `st_ManagedInitializationContext.java` /
`st_ManagedSnapshotContext.java`; these two files are small and I read them as part of the fetch set.
`isRestored()` is the cold-start-vs-restore discriminator, mirroring
`InitContext.getRestoredCheckpointId()` on the sink side.)*

### 3.4 Operator state vs keyed state — the split, and what it is really about

`OperatorStateStore` (`@PublicEvolving`) offers three shapes, and the difference between them is
**purely a rescaling-redistribution policy**:

```java
@PublicEvolving
public interface OperatorStateStore {
    <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) throws Exception;
    <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception;
    <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception;
    Set<String> getRegisteredStateNames();
    Set<String> getRegisteredBroadcastStateNames();
}
```

- `getListState` — *"The redistribution scheme of this list state upon operator rescaling is a
  **round-robin** pattern, such that the logical whole state (a concatenation of all the lists of
  state elements previously managed by each operator before the restore) is evenly divided into as
  many sublists as there are parallel operators."*
- `getUnionListState` — *"the redistribution scheme … is a **broadcast** pattern, such that the
  logical whole state … is restored to all parallel operators so that each of them will get the union
  of all state items before the restore."*
- `getBroadcastState` — *"**CAUTION: the user has to guarantee that all task instances store the same
  elements in this type of state.**"*

And the crucial semantic constraint on list state:

> *"Under the context of operator state, the list is a collection of state items that are independent
> of each other and eligible for redistribution across operator instances in case of changed operator
> parallelism. In other words, **these state items are the finest granularity at which non-keyed state
> can be redistributed, and should not be correlated with each other.**"*

Keyed state (`KeyedStateStore`) is the other half: `ValueState`, `ListState`, `ReducingState`,
`AggregatingState`, `MapState`, all implicitly scoped to the current key, redistributed by key-group.
`CheckpointedFunction`'s javadoc: *"**Note:** The `KeyedStateStore` can only be used when the
transformation supports keyed state, i.e., when it is applied on a keyed stream (after a
`keyBy(...)`)."*

**The honest reading of the operator/keyed split:** it is *not* a general-purpose state taxonomy. It
is **"state whose rescaling policy is round-robin/broadcast over an opaque list" vs "state whose
rescaling policy is derived from a key hash"**. Both categories exist only because parallelism can
change and state must be redistributed.

For canal, the transplantable part is much smaller and much more useful than the Flink API:
**every piece of durable connector state must declare how it redistributes when the number of workers
changes.** In canal's terms:

- *per-split state* → travels with the split (Flink: the reader's `List<SplitT>`). Redistribution is
  free because the split is the unit of assignment. **This is the one canal needs.**
- *whole-connector state* → needs a policy: single-owner, union-to-all, or shard-by-key.
- *keyed state* → only if canal ever does stateful transforms; do not build it for phase 1.

Flink's `getListState` "items must not be correlated with each other" warning is the rule that makes
round-robin redistribution safe. If canal exposes any list-shaped state, it must carry the same
warning, or better: **only allow per-split state, so the question never arises.**

### 3.5 Who commits, when, and how the sink acknowledgement interacts

`flink-core/.../api/common/state/CheckpointListener.java`, `@Public`, is where the transactional
handshake lives. It is worth quoting at length because the *contract* is the interesting artifact,
not the two method signatures:

```java
@Public
public interface CheckpointListener {
    void notifyCheckpointComplete(long checkpointId) throws Exception;
    default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}
```

> **Invocation Guarantees.** *"It is NOT guaranteed that the implementation will receive a
> notification for each completed or aborted checkpoint. While these notifications come in most
> cases, notifications might not happen, for example, when a failure/restore happens directly after a
> checkpoint completed."*
>
> **Exceptions.** *"The notifications from this interface come 'after the fact', meaning after the
> checkpoint has been aborted or completed. Throwing an exception will not change the
> completion/abortion of the checkpoint. Exceptions thrown from this method result in task- or job
> failure and recovery."*
>
> **Checkpoint Subsuming Contract.** *"Checkpoint IDs are strictly increasing. A checkpoint with
> higher ID always subsumes a checkpoint with lower ID. For example, when checkpoint T is confirmed
> complete, the code can assume that no checkpoints with lower ID (T-1, T-2, etc.) are pending any
> more. **No checkpoint with lower ID will ever be committed after a checkpoint with a higher ID.**"*
>
> *"This does not necessarily mean that all of the previous checkpoints actually completed
> successfully. It is also possible that some checkpoint timed out or was not fully acknowledged by
> all tasks. **Implementations must then behave as if that checkpoint did not happen.** The
> recommended way to do this is to let the completion of a new checkpoint (higher ID) subsume the
> completion of all earlier checkpoints (lower ID)."*
>
> *"This property is easy to achieve for cases where increasing 'offsets', 'watermarks', or other
> progress indicators are communicated on checkpoint completion. A newer checkpoint will have a higher
> 'offset' (more progress) than the previous checkpoint, so it automatically subsumes the previous
> one. Remember the 'offset to commit' for a checkpoint ID and commit it when that specific checkpoint
> (by ID) gets the notification that it is complete."*

And then the doc gives the **explicit recipe** for the non-monotonic case (publishing specific
artifacts, i.e. exactly canal's sink-committable problem):

> *"During processing, have two sets of artifacts.
> 1. A 'ready set': Artifacts that are ready to be published as part of the next checkpoint. Artifacts
>    are added to this set as soon as they are ready to be committed. This set is 'transient', it is
>    not stored in Flink's state persisted anywhere.
> 2. A 'pending set': Artifacts being committed with a checkpoint. The actual publishing happens when
>    the checkpoint is complete. This is a map of `long => List<Artifact>`, mapping from the id of the
>    checkpoint when the artifact was ready to the artifacts.
>
> On checkpoint, add that set of artifacts from the 'ready set' to the 'pending set', associated with
> the checkpoint ID. **The whole 'pending set' gets stored in the checkpoint state.**
> On `notifyCheckpointComplete()` publish all IDs/artifacts from the 'pending set' **up to** the
> checkpoint with that ID. Remove these from the 'pending set'.
>
> That way, even if some checkpoints did not complete, or if the notification that they completed got
> lost, the artifacts will be published as part of the next checkpoint that completes."*

`notifyCheckpointAborted` has an equally important warning:

> *"**Important:** The fact that a checkpoint has been aborted does NOT mean that the data and
> artifacts produced between the previous checkpoint and the aborted checkpoint are to be discarded.
> The expected behavior is as if this checkpoint was never triggered in the first place, and the next
> successful checkpoint simply covers a longer time span."*

**This is the single most valuable paragraph in the whole Flink codebase for canal.** It is the
complete, battle-tested answer to "how does a sink commit externally without either losing data or
double-committing, given that the confirm signal is unreliable?" — and it is *entirely independent of
distributed dataflow*. It applies verbatim to a single Go process.

The ordering is therefore:

```
   write records ──► flush(endOfInput=false)
                        │
                        ▼
                   prepareCommit()  ──► Collection<CommittableT>   (pending set for ckpt N)
                        │
                        ▼
              snapshotState(N)  ──► writer state INCLUDING pending committables
                        │
                        ▼
               [ checkpoint N becomes durable ]
                        │
                        ▼
            notifyCheckpointComplete(N)  ──► Committer.commit(all pending ≤ N)
```

Failure between `snapshotState(N)` and `commit` ⇒ restore from N, committables are in the restored
writer state, `commit` runs again ⇒ **must be idempotent**, hence
`CommitRequest.signalAlreadyCommitted()`.

canal's design rule **R4** ("an acknowledgement means durable") maps onto this exactly: the source's
position may only advance when the checkpoint containing the sink's committables is durable **and**
the committer has confirmed. Flink's answer is: *durability of the checkpoint* is the fence, and the
external commit happens strictly after it.

### 3.6 Survival across restart, and the format-compatibility question

Two independent versioning mechanisms, and canal needs both (this closes open decision #9 in
`docs/design-rules.md`, "checkpoint state format compatibility across binary upgrades"):

**(a) `SimpleVersionedSerializer` — for connector-authored blobs (splits, enumerator checkpoints,
committables, writer state).** `flink-core/.../core/io/SimpleVersionedSerializer.java`,
`@PublicEvolving`:

```java
@PublicEvolving
public interface SimpleVersionedSerializer<E> extends Versioned {
    @Override int getVersion();
    byte[] serialize(E obj) throws IOException;
    E deserialize(int version, byte[] serialized) throws IOException;
}
```

with the usage contract in the class javadoc:

```java
byte[] serializedData = serializer.serialize(someObject);
int version = serializer.getVersion();
MyType deserialized = serializer.deserialize(version, serializedData);

byte[] someOldData = ...;
int oldVersion = ...;
MyType deserializedOldObject = serializer.deserialize(oldVersion, someOldData);
```

That is the whole idea: **the framework stores `(version, bytes)`; the connector's deserialiser is
handed the version it was written with and must handle old ones.** Forty lines of interface, complete
forward/backward compatibility story, zero framework knowledge of the payload. **Copy this verbatim
into Go** — it is `Version() int`, `Serialize(E) ([]byte, error)`, `Deserialize(version int, b []byte) (E, error)`.

**(b) `TypeSerializerSnapshot` / `TypeSerializerSchemaCompatibility` — for framework-managed typed
state.** The compatibility verdict is a four-valued result type
(`flink-core/.../api/common/typeutils/TypeSerializerSchemaCompatibility.java`, `@PublicEvolving`):

```java
    enum Type {
        /** This indicates that the new serializer continued to be used as is. */
        COMPATIBLE_AS_IS,
        /**
         * This indicates that it is possible to use the new serializer after performing a full-scan
         * migration over all state, by reading bytes with the previous serializer and then writing
         * it again with the new serializer …
         */
        COMPATIBLE_AFTER_MIGRATION,
        /**
         * This indicates that a reconfigured version of the new serializer is compatible, and
         * should be used instead of the original new serializer.
         */
        COMPATIBLE_WITH_RECONFIGURED_SERIALIZER,
        /**
         * This indicates that the new serializer is incompatible, even with migration. This
         * normally implies that the deserialized Java class can not be commonly recognized by the
         * previous and new serializer.
         */
        INCOMPATIBLE
    }
```

with static factories `compatibleAsIs()`, `compatibleAfterMigration()`,
`compatibleWithReconfiguredSerializer(TypeSerializer<T>)`, `incompatible()`, and on `incompatible()`:
*"In this case, there is no possible way for the new serializer to continue to be used, even with
migration. **Recovery of the Flink job will fail.**"*

**Four-valued, not boolean, and the "reconfigure" case is the interesting one** — the new serialiser
can be *rebuilt* from the old snapshot's metadata (e.g. an enum whose value order changed). This is a
much better shape than "version int + hope".

---

## 4. Snapshot handling — Flink CDC's incremental snapshot algorithm

This is the industry-best snapshot-to-stream handoff, and the whole reason to study Flink CDC. It is
built **entirely on top of FLIP-27** — no changes to Flink core were needed, which is itself the
proof that the FLIP-27 abstraction is right.

### 4.1 The documented algorithm

From `flink-cdc/docs/content/docs/connectors/flink-sources/mysql-cdc.md` (verbatim):

> *"Incremental snapshot reading is a new mechanism to read snapshot of a table. Compared to the old
> snapshot mechanism, the incremental snapshot has many advantages, including:
> * (1) MySQL CDC Source **can be parallel** during snapshot reading
> * (2) MySQL CDC Source can perform **checkpoints in the chunk granularity** during snapshot reading
> * (3) MySQL CDC Source **doesn't need to acquire global read lock** (FLUSH TABLES WITH READ LOCK)
>   before snapshot reading"*

The per-chunk algorithm, quoted in full:

> *"**Chunk Reading Algorithm.** … each executes **Offset Signal Algorithm** to get a final consistent
> output of the snapshot chunk. The Offset Signal Algorithm simply describes as following:*
>
> * *(1) Record current binlog position as `LOW` offset*
> * *(2) Read and buffer the snapshot chunk records by executing statement
>   `SELECT * FROM MyTable WHERE id > chunk_low AND id <= chunk_high`*
> * *(3) Record current binlog position as `HIGH` offset*
> * *(4) Read the binlog records that belong to the snapshot chunk from `LOW` offset to `HIGH` offset*
> * *(5) Upsert the read binlog records into the buffered chunk records, and emit all records in the
>   buffer as final output (all as INSERT records) of the snapshot chunk*
> * *(6) Continue to read and emit binlog records belong to the chunk after the `HIGH` offset in
>   **single binlog reader**.*
>
> *The algorithm is inspired by [DBLog Paper](https://arxiv.org/pdf/2010.12597v1.pdf)."*

And the handoff rule:

> *"After all snapshot chunks finished, the source will continue to read binlog in a single task. In
> order to guarantee the global data order of snapshot records and binlog records, **binlog reader will
> start to read data until there is a complete checkpoint after snapshot chunks finished** to make
> sure all snapshot data has been consumed by downstream. The binlog reader tracks the consumed
> binlog position in state, thus source of binlog phase can support checkpoint in row level."*

Chunk splitting, two strategies:

> *"For numeric and auto incremental splitting column, MySQL CDC Source efficiently splits chunks by
> **fixed step length**"* → `(-∞, 25), [25, 50), [50, 75), [75, 100), [100, +∞)`
>
> *"For other primary key column type, MySQL CDC Source executes the statement in the form of
> `SELECT MAX(STR_ID) AS chunk_high FROM (SELECT * FROM TestTable WHERE STR_ID > 'uuid-001' ORDER BY
> STR_ID ASC LIMIT 25)` to get the low and high value for each chunk"* → `(-∞, 'uuid-001'), ['uuid-001', 'uuid-009'), …, [uuid-def, +∞)`

Note the **unbounded first and last chunk** (`-∞` / `+∞`). That is not sloppiness: it makes the chunk
set cover rows inserted outside the observed min/max after splitting began.

### 4.2 The three watermark kinds — the entire protocol in one enum

`flink-cdc-base/.../source/meta/wartermark/WatermarkKind.java` *(note the upstream package typo,
`wartermark`)*:

```java
/** The watermark kind. */
public enum WatermarkKind {
    LOW,
    HIGH,
    END;
}
```

and `AbstractScanFetchTask.execute` is the algorithm, read from source, with the semantics stated in
javadoc on each dispatch method:

```java
    @Override
    public void execute(Context context) throws Exception {
        DataSourceDialect dialect = context.getDataSourceDialect();
        SourceConfig sourceConfig = context.getSourceConfig();
        taskRunning = true;

        // hook: getPreLowWatermarkAction()
        final Offset lowWatermark = dialect.displayCurrentOffset(sourceConfig);
        LOG.info("Snapshot step 1 - Determining low watermark {} for split {}", lowWatermark, snapshotSplit);
        dispatchLowWaterMarkEvent(context, snapshotSplit, lowWatermark);
        // hook: getPostLowWatermarkAction()

        LOG.info("Snapshot step 2 - Snapshotting data");
        executeDataSnapshot(context);

        // hook: getPreHighWatermarkAction()
        // Directly set HW = LW if backfill is skipped. Stream events created during snapshot
        // phase could be processed later in stream reading phase.
        //
        // Note that this behaviour downgrades the delivery guarantee to at-least-once. We can't
        // promise that the snapshot is exactly the view of the table at low watermark moment,
        // so stream events created during snapshot might be replayed later in stream reading
        // phase.
        Offset highWatermark =
                context.getSourceConfig().isSkipSnapshotBackfill()
                        ? lowWatermark
                        : dialect.displayCurrentOffset(sourceConfig);
        LOG.info("Snapshot step 3 - Determining high watermark {} for split {}", highWatermark, snapshotSplit);
        dispatchHighWaterMarkEvent(context, snapshotSplit, highWatermark);
        // hook: getPostHighWatermarkAction()

        // optimization that skip the stream read when the low watermark equals high watermark
        final StreamSplit backfillStreamSplit = createBackfillStreamSplit(lowWatermark, highWatermark);
        final boolean streamBackfillRequired =
                backfillStreamSplit.getEndingOffset().isAfter(backfillStreamSplit.getStartingOffset());

        if (!streamBackfillRequired) {
            LOG.info("Skip the backfill {} for split {}: low watermark >= high watermark", ...);
            dispatchEndWaterMarkEvent(context, backfillStreamSplit, backfillStreamSplit.getEndingOffset());
        } else {
            executeBackfillTask(context, backfillStreamSplit);
        }
        taskRunning = false;
    }
```

The javadoc on the three dispatchers is the precise definition of the intervals:

- `dispatchLowWaterMarkEvent`: *"which means the beginning of snapshot data."*
- `dispatchHighWaterMarkEvent`: *"which means the end of snapshot data and also the beginning of
  backfill data. **Data change events between (low_watermark, high_watermark) are snapshot data.**"*
- `dispatchEndWaterMarkEvent`: *"which means the end of backfill data. **Data change events between
  (high_watermark, end_watermark) are backfill data, which maybe duplicate of snapshot data. Thus,
  only the intersection of both is exactly-once.**"*

And crucially, the **backfill is itself modelled as a `StreamSplit`**:

```java
    protected StreamSplit createBackfillStreamSplit(Offset lowWatermark, Offset highWatermark) {
        return new StreamSplit(
                snapshotSplit.splitId(),
                lowWatermark,
                highWatermark,
                new ArrayList<>(),
                snapshotSplit.getTableSchemas(),
                0);
    }
```

A *bounded* stream split with `[low, high)` as its range. The same reader machinery reads the
per-chunk backfill and the global stream. **One split type, two lifetimes (bounded and unbounded).**
That is the trick that keeps the implementation small.

### 4.3 The split types

`SourceSplitBase` (`@Experimental`) is a sealed-ish union:

```java
@Experimental
public abstract class SourceSplitBase implements SourceSplit {

    protected final String splitId;

    public final boolean isSnapshotSplit() {
        return getClass() == SnapshotSplit.class || getClass() == SchemalessSnapshotSplit.class;
    }
    public final boolean isStreamSplit() { return getClass() == StreamSplit.class; }
    public final SnapshotSplit asSnapshotSplit() { return (SnapshotSplit) this; }
    public final StreamSplit asStreamSplit() { return (StreamSplit) this; }

    @Override public String splitId() { return splitId; }

    public abstract Map<TableId, TableChange> getTableSchemas();
}
```

(`getClass() == X.class` is a hand-rolled sealed type — see §13. In Go this is a discriminated union
done properly: `Kind` field or a small interface with a `kind()` method.)

`SnapshotSplit`:

```java
public class SnapshotSplit extends SourceSplitBase {
    private final TableId tableId;
    private final RowType splitKeyType;
    private final Map<TableId, TableChange> tableSchemas;
    @Nullable private final Object[] splitStart;
    @Nullable private final Object[] splitEnd;

    /** The high watermark is not null when the split read finished. */
    @Nullable private final Offset highWatermark;

    @Nullable transient byte[] serializedFormCache;

    public boolean isSnapshotReadFinished() { return highWatermark != null; }

    public static String generateSplitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }
    public static TableId extractTableId(String splitId);
    public static int extractChunkId(String splitId);
}
```

Two things: (a) **`highWatermark != null` *is* the "this chunk is done" flag** — completion is data,
not a separate boolean; (b) the split carries `tableSchemas` — **the schema travels with the unit of
work** (see §5), with a `SchemalessSnapshotSplit` variant to drop it when transmitting many splits.

`StreamSplit` is the richer one:

```java
public class StreamSplit extends SourceSplitBase {
    public static final String STREAM_SPLIT_ID = "stream-split";

    private final Offset startingOffset;
    private final Offset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChange> tableSchemas;
    private final int totalFinishedSplitSize;
    private final boolean isSuspended;
    /** Indicates whether initial state snapshot was completed right before this split. */
    private final boolean isSnapshotCompleted;

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }
```

`totalFinishedSplitSize` vs `finishedSnapshotSplitInfos.size()` exists because the metadata is too
large to ship in one message — the split arrives with a *count* and the reader pulls the actual
entries in groups (§4.6). Plus factory helpers that are pure functions on the split:
`appendFinishedSplitInfos`, `filterOutdatedSplitInfos`, `fillTableSchemas`, `toNormalStreamSplit`,
`toSuspendedStreamSplit`, and the private `forwardHighWatermarkToStartingOffset`.

`FinishedSnapshotSplitInfo` is the handoff record — the *only* thing the stream phase needs to know
about the snapshot phase:

```java
public class FinishedSnapshotSplitInfo implements OffsetDeserializerSerializer {
    private final TableId tableId;
    private final String splitId;
    private final Object[] splitStart;
    private final Object[] splitEnd;
    private final Offset highWatermark;
    private final OffsetFactory offsetFactory;

    public byte[] serialize();
    public void serialize(final DataOutputSerializer out) throws IOException;
}
```

i.e. **`(key range, offset at which that key range became consistent)`**. That tuple is the entire
snapshot→stream contract. It is completely generic: nothing about it is MySQL-specific, or even
relational — it is "a partition of the key space, and the position from which changes to that
partition are still needed."

### 4.4 The dedup filter — how the stream phase avoids re-emitting snapshot rows

`IncrementalSourceStreamFetcher.shouldEmit`, javadoc first:

> *"The watermark signal algorithm is the stream split reader only sends the change event that belongs
> to its finished snapshot splits. For each snapshot split, the change event is valid since the offset
> is after its high watermark.*
>
> ```
>  E.g: the data input is :
>     snapshot-split-0 info : [0,    1024) highWatermark0
>     snapshot-split-1 info : [1024, 2048) highWatermark1
>   the data output is:
>   only the change event belong to [0,    1024) and offset is after highWatermark0 should send,
>   only the change event belong to [1024, 2048) and offset is after highWatermark1 should send.
> ```
> *"*

```java
    protected boolean shouldEmit(SourceRecord sourceRecord) {
        if (taskContext.isDataChangeRecord(sourceRecord)) {
            TableId tableId = taskContext.getTableId(sourceRecord);
            Offset position = taskContext.getStreamOffset(sourceRecord);
            if (hasEnterPureStreamPhase(tableId, position)) {
                return true;
            }
            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                // if backfill skipped, don't need to filter
                if (isBackfillSkipped) {
                    return true;
                }
                List<FinishedSnapshotSplitInfo> tableSplits = finishedSplitsInfo.get(tableId);
                if (supportsSplitKeyOptimization) {
                    Object[] splitKey = taskContext.getSplitKey(sourceRecord);
                    FinishedSnapshotSplitInfo matchedSplit =
                            SplitKeyUtils.findSplitByKeyBinary(tableSplits, splitKey);
                    return matchedSplit != null
                            && position.isAfter(matchedSplit.getHighWatermark());
                } else {
                    for (FinishedSnapshotSplitInfo splitInfo : tableSplits) {
                        if (taskContext.isRecordBetween(
                                        sourceRecord, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                                && position.isAfter(splitInfo.getHighWatermark())) {
                            return true;
                        }
                    }
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        return true;
    }
```

plus the "we can stop filtering this table entirely" fast path, which is the important
garbage-collection step:

```java
    private boolean hasEnterPureStreamPhase(TableId tableId, Offset position) {
        if (pureStreamPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureStreamPhaseTables.add(tableId);
            return true;
        }
        ...
    }
```

Once the stream position passes `max(highWatermark)` for a table, the per-chunk filter is retired
forever for that table. **The filter is transient, not permanent** — this is what makes the
steady-state cost zero.

Note also the O(chunks) linear scan in the non-optimised branch, and the later
`supportsSplitKeyOptimization` binary-search retrofit (`SplitKeyUtils.findSplitByKeyBinary`,
`SplitKeyUtils::sortFinishedSplitInfos`). See §13.

### 4.5 Resumability and chunk-level checkpointing — the state that makes it work

`SnapshotPendingSplitsState` is the enumerator's checkpoint during the snapshot phase. Read it as a
specification of *what you must persist to resume a parallel snapshot*:

```java
public class SnapshotPendingSplitsState extends PendingSplitsState {

    /** The tables in the checkpoint. */
    private final List<TableId> remainingTables;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<TableId> alreadyProcessedTables;

    /** The splits in the checkpoint. */
    private final List<SchemalessSnapshotSplit> remainingSplits;

    /** The snapshot splits that the IncrementalSourceEnumerator has assigned to readers. */
    private final Map<String, SchemalessSnapshotSplit> assignedSplits;

    /** The offsets of finished (snapshot) splits that the enumerator has received from readers. */
    private final Map<String, Offset> splitFinishedOffsets;

    /** The AssignerStatus that indicates the snapshot assigner status. */
    private final AssignerStatus assignerStatus;

    private final boolean isTableIdCaseSensitive;
    private final boolean isRemainingTablesCheckpointed;

    private final Map<TableId, TableChanges.TableChange> tableSchemas;

    /** Map to record splitId and the checkpointId mark the split is finished. */
    private final Map<String, Long> splitFinishedCheckpointIds;

    /** The data structure to record the state of a ChunkSplitter. */
    private final ChunkSplitterState chunkSplitterState;
```

Six of these are the general lesson:

1. `remainingTables` + `alreadyProcessedTables` — **discovery progress**, so a restart doesn't
   re-discover work already done or miss newly-added work.
2. `remainingSplits` — enumerated but unassigned.
3. `assignedSplits` — assigned but not confirmed finished. (Note this contradicts the FLIP-27
   javadoc's "assigned splits don't need to be in the snapshot"; CDC keeps them because it needs
   their key ranges to build the stream split later, not to reassign them.)
4. `splitFinishedOffsets` — **the accumulating `(splitId → highWatermark)` map** that becomes
   `List<FinishedSnapshotSplitInfo>`.
5. `splitFinishedCheckpointIds` — *"Map to record splitId and the checkpointId mark the split is
   finished"*, i.e. **when** each split's completion became durable. This is what lets the assigner
   only trust a finished split after its checkpoint completed.
6. `chunkSplitterState` — **the splitting itself is resumable mid-table.**

`ChunkSplitterState` deserves its own quote, because "the enumerator was halfway through slicing a
huge table when we crashed" is exactly the case naive implementations get wrong:

```java
public class ChunkSplitterState {

    public static final ChunkSplitterState NO_SPLITTING_TABLE_STATE =
            new ChunkSplitterState(null, null, null);

    /** Record current splitting table id in the chunk splitter. */
    @Nullable private final TableId currentSplittingTableId;
    /** Record next chunk start. */
    @Nullable private final ChunkBound nextChunkStart;
    /** Record next chunk id. */
    @Nullable private final Integer nextChunkId;

    public static final class ChunkBound {
        public static final ChunkBound START_BOUND = new ChunkBound(ChunkBoundType.START, null);
        public static final ChunkBound END_BOUND = new ChunkBound(ChunkBoundType.END, null);
        private final ChunkBoundType boundType;
        @Nullable private final Object value;
        public static ChunkBound middleOf(Object obj) { ... }
    }

    public enum ChunkBoundType { START, MIDDLE, END }
}
```

Three fields — *(which table, next chunk lower bound, next chunk id)* — and `ChunkBoundType
{START, MIDDLE, END}` is how `-∞`/`+∞` are represented as data rather than as `null` with a comment.

### 4.6 The assigner interface and its state machine

`SplitAssigner` (`@Experimental`) is a **CDC-level abstraction that sits inside the FLIP-27
enumerator** — the enumerator is a thin shell delegating to it:

```java
@Experimental
public interface SplitAssigner extends Closeable {

    void open();

    /**
     * Gets the next split.
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    Optional<SourceSplitBase> getNext();

    /** Whether the split assigner is still waiting for callback of finished splits. */
    boolean waitingForFinishedSplits();

    /** Indicates there is no more splits available in this assigner. */
    boolean noMoreSplits();

    /**
     * Gets the finished splits' information. This is useful metadata to generate a stream split
     * that considering finished snapshot splits.
     */
    List<FinishedSnapshotSplitInfo> getFinishedSplitInfos();

    /**
     * Callback to handle the finished splits with finished change log offset. This is useful for
     * determine when to generate stream split and what stream split to generate.
     */
    void onFinishedSplits(Map<String, Offset> splitFinishedOffsets);

    void addSplits(Collection<SourceSplitBase> splits);

    PendingSplitsState snapshotState(long checkpointId);

    void notifyCheckpointComplete(long checkpointId);

    AssignerStatus getAssignerStatus();

    /** Starts assign newly added tables. */
    void startAssignNewlyAddedTables();

    void onStreamSplitUpdated();

    void close() throws IOException;
}
```

Three implementations: `SnapshotSplitAssigner`, `StreamSplitAssigner`, `HybridSplitAssigner`. **The
pipeline *type* is a strategy object, not a mode flag.** `HybridSplitAssigner` wraps a
`SnapshotSplitAssigner` and adds one boolean:

```java
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private boolean isStreamSplitAssigned;
    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;
```

and `getNext()` is the entire handoff decision, read from source:

```java
    @Override
    public Optional<SourceSplitBase> getNext() {
        if (isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            enumeratorMetrics.exitSnapshotPhase();
            // stream split assigning
            if (isStreamSplitAssigned) {
                return Optional.empty();
            } else if (isInitialAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the stream split. Otherwise, records emitted from stream split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isStreamSplitAssigned = true;
                enumeratorMetrics.enterStreamReading();
                StreamSplit streamSplit = createStreamSplit();
                return Optional.of(streamSplit);
            } else if (isNewlyAddedAssigningFinished(snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create stream split, but send event to wake up the binlog reader
                isStreamSplitAssigned = true;
                enumeratorMetrics.enterStreamReading();
                return Optional.empty();
            } else {
                // stream split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }
```

and `createStreamSplit()` is where the handoff artifact is built:

```java
    public StreamSplit createStreamSplit() {
        final List<SchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(SourceSplitBase::splitId))
                        .collect(Collectors.toList());

        Map<String, Offset> splitFinishedOffsets = snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        Offset minOffset = null, maxOffset = null;
        for (SchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max offset of change log
            Offset changeLogOffset = splitFinishedOffsets.get(split.splitId());
            if (minOffset == null || changeLogOffset.isBefore(minOffset)) { minOffset = changeLogOffset; }
            if (maxOffset == null || changeLogOffset.isAfter(maxOffset))  { maxOffset = changeLogOffset; }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(), split.splitId(),
                            split.getSplitStart(), split.getSplitEnd(),
                            changeLogOffset, offsetFactory));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        Offset stoppingOffset = offsetFactory.createNoStoppingOffset();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new StreamSplit(
                STREAM_SPLIT_ID,
                minOffset == null ? offsetFactory.createInitialOffset() : minOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size(),
                false,
                true);
    }
```

**The stream split starts at `min(highWatermark)` across all chunks, not at the latest offset.** That
is the no-gap guarantee: the earliest-finished chunk's watermark bounds how far back the stream must
replay, and the per-chunk `shouldEmit` filter suppresses the redundant range for every other chunk.
No gap (you never skip past a chunk's watermark), no duplicates (you filter below it).

`AssignerStatus` is a five-state FSM with the transition diagram literally drawn in the javadoc:

```
       INITIAL_ASSIGNING(start)
             | onFinish()
             ↓
   INITIAL_ASSIGNING_FINISHED(end)
             | startAssignNewlyTables()
             ↓
    NEWLY_ADDED_ASSIGNING --onFinish()--> NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED
        --onStreamSplitUpdated()--> NEWLY_ADDED_ASSIGNING_FINISHED(end)
             ↑                                             |
             |--------- startAssignNewlyTables() ----------|
```

with a nice detail: **illegal transitions throw, by construction**, because the enum overrides the
transition methods per constant and the base implementations throw:

```java
    public AssignerStatus onFinish() {
        throw new IllegalStateException(
                format("Invalid call, assigner under %s state can not call onFinish()",
                        fromStatusCode(this.getStatusCode())));
    }
```

and the status is persisted by integer code (`getStatusCode()` / `fromStatusCode(int)`) — an explicit
wire representation, not `Enum.ordinal()` or `name()`. Correct: the on-disk encoding of a state
machine is a versioned contract.

Six predicate helpers give call sites intent-named reads instead of enum comparisons:
`isSnapshotAssigningFinished`, `isAssigningFinished`, `isAssigningSnapshotSplits`,
`isInitialAssigningFinished`, `isNewlyAddedAssigningFinished`,
`isNewlyAddedAssigningSnapshotFinished`, `isNewlyAddedAssigning`.

### 4.7 The reader↔enumerator event protocol for the handoff

From `IncrementalSourceEnumerator.handleSourceEvent`, the full set of `SourceEvent` subtypes in play:

| Event | Direction | Purpose |
|---|---|---|
| `FinishedSnapshotSplitsReportEvent` | reader → enum | *"receives finished split offsets {} from subtask {}"* — carries `Map<String, Offset> getFinishedOffsets()` |
| `FinishedSnapshotSplitsAckEvent` | enum → reader | ack of the above, carries the split-id list |
| `FinishedSnapshotSplitsRequestEvent` | enum → reader | *"tell all …Reader(s) to report there finished but unacked splits"* — the reconciliation poll |
| `StreamSplitMetaRequestEvent` | reader → enum | request meta group `i` of the finished-split list |
| `StreamSplitMetaEvent` | enum → reader | one group of serialised `FinishedSnapshotSplitInfo` |
| `StreamSplitUpdateRequestEvent` | enum → reader | newly-added tables: please absorb new finished splits |
| `StreamSplitUpdateAckEvent` | reader → enum | done absorbing → drives `onStreamSplitUpdated()` |
| `StreamSplitAssignedEvent` | reader → enum | *"receives notice from subtask {} for the stream split assignment"* — tells the enumerator *which* subtask owns the stream |
| `LatestFinishedSplitsNumberRequestEvent` / `LatestFinishedSplitsNumberEvent` | both | reader re-syncs the expected total |

The **reconciliation timer** is the piece worth stealing:

```java
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    @Override
    public void start() {
        splitAssigner.open();
        requestStreamSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader, this::syncWithReaders,
                CHECK_EVENT_INTERVAL, CHECK_EVENT_INTERVAL);
    }

    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the IncrementalSourceEnumerator restores or the communication failed between
        // IncrementalSourceEnumerator and JdbcIncrementalSourceReader, it may missed some
        // notification event.
        // tell all JdbcIncrementalSourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }
        requestStreamSplitUpdateIfNeed();
    }
```

**Every event in the protocol is treated as lossy, and a 30s idempotent reconciliation sweep repairs
missed notifications.** This is the same design philosophy as `CheckpointListener`'s "notifications
are best effort": *never build a protocol whose correctness depends on a message arriving.* Copy
this. It is much more robust than acknowledgement-based bookkeeping, and it is the thing that makes
the whole assigner survivable across enumerator restore.

The other transplantable behaviour: **the enumerator retires idle readers when the snapshot is done**:

```java
    private boolean shouldCloseIdleReader(int nextAwaiting) {
        // When no unassigned split anymore, Signal NoMoreSplitsEvent to awaiting reader in two
        // situations:
        // 1. When Set StartupMode = snapshot mode(also bounded), there's no more splits in the assigner.
        // 2. When set scan.incremental.close-idle-reader.enabled = true, there's no more splits in the assigner.
        return splitAssigner.noMoreSplits()
                && (boundedness == Boundedness.BOUNDED
                        || (sourceConfig.isCloseIdleReaders()
                                && streamSplitTaskId != null
                                && streamSplitTaskId != (nextAwaiting)));
    }
```

Snapshot at parallelism 8, stream at parallelism 1, **without a job restart** — via
`signalNoMoreSplits(subtask)`. That is the thing Kafka Connect structurally cannot do (its
parallelism is fixed by `taskConfigs(maxTasks)` at start).

### 4.8 Summary: the eight-part recipe canal should implement

1. Split the key space into **chunks with explicit `START`/`MIDDLE`/`END` bounds**, unbounded at both
   ends.
2. **Persist the splitter's own cursor** (`ChunkSplitterState`) so slicing resumes mid-table.
3. Per chunk: record `LOW` position → read chunk → record `HIGH` position → replay `[LOW, HIGH)` →
   upsert into buffer → emit buffer as inserts → emit `END`.
4. Model the per-chunk replay as a **bounded instance of the same stream-split type**.
5. Report `(splitId, HIGH)` back to the enumerator; the enumerator accumulates
   `(keyRange, HIGH)` tuples.
6. On snapshot completion, build the stream split starting at **`min(HIGH)`**, carrying the tuple set.
7. Stream phase filters: emit iff `record.key ∈ range(chunk) && record.pos > HIGH(chunk)`; retire the
   filter per table once `pos ≥ max(HIGH)`.
8. Only start the stream phase after **a complete checkpoint following snapshot completion**.

Every step is source-agnostic. Nothing requires a relational database, a binlog, or ordering beyond
"positions are comparable". The only requirements on a source to support this are:
`Offset` is **comparable** (`isBefore`/`isAfter`/`isAtOrAfter`), the key space is **splittable**, and
changes are **replayable from a position**. Those become canal's optional
`SupportsChunkedSnapshot` / `SupportsReplay` capability interfaces.

---

## 5. Schema handling

### 5.1 Flink core: nothing (DataStream), or everything (Table API)

The DataStream connector API has **no schema concept**. `Source<T, …>` / `Sink<InputT>` are generic;
the "schema" is the Java type and its `TypeSerializer`, resolved at job-compile time. There is no
discovery method, no schema-change event, no drift policy.

The Table/SQL layer has a full catalog and a factory SPI (§7), but it is *out-of-band and static*: the
schema is declared in DDL or fetched from a catalog before the job graph is built, and **cannot change
while the job runs**. A DDL change upstream requires a job restart.

So Flink core answers "schema drift" with: **restart the job**. That is not usable for canal.

### 5.2 Flink CDC (Flink-source level): schema travels with the split, and changes are records

At the `flink-cdc-base` level, the schema is carried **on the split**:

```java
// SourceSplitBase
public abstract Map<TableId, TableChange> getTableSchemas();
```

`SnapshotSplit` holds `Map<TableId, TableChange> tableSchemas`; `StreamSplit` holds the same. There is
a `SchemalessSnapshotSplit` variant, created by
`SnapshotSplit.toSchemalessSnapshotSplit()`, used inside the enumerator's state
(`SnapshotPendingSplitsState.remainingSplits` is `List<SchemalessSnapshotSplit>`) with the schemas
deduplicated into a single `Map<TableId, TableChanges.TableChange> tableSchemas` field on the state —
because carrying the schema on every one of thousands of chunk splits would blow up the checkpoint.
`StreamSplit.fillTableSchemas(streamSplit, tableSchemas)` re-hydrates it on the way out.

**Schema changes arrive in-band and mutate split state**, from `IncrementalSourceRecordEmitter`:

```java
        } else if (isSchemaChangeEvent(element) && splitState.isStreamSplitState()) {
            TableChanges changes = getTableChangeRecord(element);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asStreamSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                emitElement(element, output);
            }
        }
```

Three properties worth naming:

1. **Schema changes are only processed on a stream split** (`&& splitState.isStreamSplitState()`) —
   during a snapshot chunk, DDL is not applied, because the chunk's schema is pinned at its start.
2. **The schema is recorded into checkpointable split state** before/independently of whether it is
   emitted. So after restart the reader knows the schema without re-reading history.
3. **Emission is opt-in** (`includeSchemaChanges`) — the *framework* always tracks it; the *user
   stream* only sees it if asked.

And `shouldEmit`'s last line: `// always send the schema change event and signal event / we need
record them to state of Flink / return true;` — control records bypass the dedup filter
unconditionally.

There's also the schema-lifecycle-vs-table-lifecycle interaction, handled by a pure function on the
split (`StreamSplit.filterOutdatedSplitInfos`):

> *"When restore from a checkpoint, the finished split infos may contain some splits from the deleted
> tables. We need to remove these splits from the total finished split infos and update the size."*

i.e. **the restored checkpoint is reconciled against current discovery**, dropping state for objects
that no longer exist, and adjusting `totalFinishedSplitSize` so `isCompletedSplit()` stays correct.
canal will need exactly this: a restored checkpoint must be filtered against the current include/
exclude configuration, and the reconciliation must be a pure, testable function of
`(restoredState, currentFilter)`.

### 5.3 Flink CDC 3.x pipeline level: `MetadataAccessor` + `MetadataApplier`

From the developer guide (docs, not source — see `unverified`):

> *"Data source works as a factory of `EventSource` and `MetadataAccessor` … `MetadataAccessor` serves
> as the metadata reader of the external system, by **listing namespaces, schemas and tables, and
> provide the table schema (table structure) of the given table ID**."*
>
> *"Symmetrical with data source, data sink consists of `EventSink` and `MetadataApplier`, which writes
> data change events and apply schema changes (metadata changes) to external system."*
>
> *"`MetadataApplier` will be used to handle schema changes. When the framework receives schema change
> event from source, **after making some internal synchronizations and flushes**, it will apply the
> schema change to external system via this applier."*

**This is the right factoring and canal should copy the shape:** a source has a *discovery* facet
distinct from its *reading* facet; a sink has an *apply-schema* facet distinct from its *write* facet.
Four small interfaces instead of two fat ones. And the "internal synchronizations and flushes" is the
critical ordering: the framework must **quiesce and flush in-flight data before applying a DDL
downstream**, otherwise records written under the old schema race the `ALTER`.

### 5.4 The drift-policy enum — the best documented artifact of the lot

`flink-cdc/docs/content/docs/core-concept/schema-evolution.md` defines
`pipeline.schema.change.behavior ∈ {exception, evolve, try_evolve, lenient, ignore}`:

| Mode | Behaviour (verbatim) |
|---|---|
| `exception` | *"all schema change behaviors are forbidden. An exception will be thrown from `SchemaOperator` once it was captured. This is useful when your downstream sink is not expected to handle any schema changes."* |
| `evolve` | *"schema operator will apply all upstream schema change events to downstream sink. If the attempt fails, an exception will be thrown from the `SchemaRegistry` and trigger a global failover."* |
| `try_evolve` | *"if specific schema change events are not supported by downstream sink, the failure will be tolerated and `SchemaOperator` will try to convert all following data records in case of schema discrepancy."* — with the warning *"such data casting and converting isn't guaranteed to be lossless. Some fields with incompatible data types might be lost."* |
| `lenient` (**default**) | *"schema operator will convert all upstream schema change events to downstream sink after converting them to ensure no data will be lost. For example, an `AlterColumnTypeEvent` will be converted to two individual schema change events including `RenameColumnEvent` and `AddColumnEvent`: Previous column (with the unchanged type) will be kept and a new column (with the new type) will be added."* |
| `ignore` | *"all schema change events will be silently swallowed by `SchemaOperator` and never attempt to apply them to downstream sink."* |

Plus **per-event-type control** at the sink:
`include.schema.changes` / `exclude.schema.changes`, over the vocabulary
`add.column`, `alter.column.type`, `create.table`, `drop.column`, `drop.table`, `rename.column`,
`truncate.table`, with prefix matching (*"passing `drop` … is equivalent to passing `drop.column` and
`drop.table`"*), `exclude` having higher priority, and a safety carve-out:

> *"In Lenient mode, `TruncateTableEvent` and `DropTableEvent` will be ignored by default."*
> *"`CreateTableEvent` is the foundation for all subsequent schema change processing. When
> `include.schema.changes` is explicitly specified, `create.table` will be automatically added unless
> the user explicitly excludes it."*

**Steal all of this.** It is the only complete, shipped answer to schema drift in any of the systems
surveyed, and it is *entirely a core-level policy* — a generic sink only needs to declare which change
kinds it supports; the core decides `exception|evolve|try_evolve|lenient|ignore`. `lenient`'s
"never destructive: rename-old + add-new instead of alter" is the correct default for a data-movement
tool, and `drop`-class events being opt-in by default is the correct safety posture.

### 5.5 State schema evolution (a different problem: evolving *canal's own* state format)

`docs/content/docs/dev/datastream/fault-tolerance/serialization/schema_evolution.md`:

> *"To evolve the schema of a given state type, you would take the following steps:
> 1. Take a **savepoint** of your Flink streaming job.
> 2. Update state types in your application (e.g., modifying your Avro type schema).
> 3. Restore the job from the savepoint. When accessing state for the first time, Flink will assess
>    whether or not the schema had been changed for the state, and migrate state schema if necessary."*
>
> *"This process is performed internally by Flink by first checking if the new serializer for the state
> has different serialization schema than the previous serializer; **if so, the previous serializer is
> used to read the state to objects, and written back to bytes again with the new serializer.**"*
>
> *"Currently, schema evolution is supported only for POJO and Avro types."*

POJO rules, verbatim:
> *"1. Fields can be removed. … 2. New fields can be added. The new field will be initialized to the
> default value for its type … 3. **Declared fields types cannot change.** 4. **Class name of the POJO
> type cannot change**, including the namespace of the class."*

Note the deliberate restriction: **state schema evolution requires a *canonical savepoint*, not a
checkpoint** (see the compatibility matrix in §13). Migration is read-with-old / write-with-new, i.e.
a full-scan rewrite, driven by the four-valued `TypeSerializerSchemaCompatibility` of §3.6.

For canal: the honest lesson is that **framework-managed typed state is where the evolution pain
lives**. `SimpleVersionedSerializer` (connector owns encoding, framework stores `(version, bytes)`)
has *no* such restrictions — the connector just handles old versions in `Deserialize`. Prefer the
`SimpleVersionedSerializer` model for everything canal persists, and do not build a typed managed-state
system unless forced to.

---

## 6. Lifecycle

### 6.1 Source lifecycle, method by method

**Enumerator** (JobManager side, single coordinator thread):

```
Source.createEnumerator(ctx)  |  Source.restoreEnumerator(ctx, checkpoint)
        ↓
    start()
        ↓
    ┌── addReader(subtaskId)                      (a reader registered)
    ├── handleSplitRequest(subtaskId, hostname)   (a reader wants work)
    ├── handleSourceEvent(subtaskId, event)       (custom protocol)
    ├── addSplitsBack(splits, subtaskId)          (a reader died; splits return)
    ├── snapshotState(checkpointId) -> CheckpointT
    └── notifyCheckpointComplete(checkpointId)
        ↓
    close()   throws IOException
```

**Reader** (TaskManager side, task thread):

```
Source.createReader(readerCtx)
        ↓
    start()
        ↓
    ┌── addSplits(List<SplitT>)
    ├── notifyNoMoreSplits()
    ├── handleSourceEvents(event)
    ├── isAvailable() -> CompletableFuture<Void>
    ├── pollNext(output) -> InputStatus{MORE_AVAILABLE|NOTHING_AVAILABLE|END_OF_INPUT}
    ├── snapshotState(checkpointId) -> List<SplitT>
    ├── notifyCheckpointComplete(checkpointId)
    └── pauseOrResumeSplits(toPause, toResume)
        ↓
    close()   (AutoCloseable)
```

Both extend `AutoCloseable` **and** `CheckpointListener`. That is the whole lifecycle: no separate
`configure`, no `open`/`close` pair distinct from `start`/`close`, no `stop`.

**Assignment-scoped lifecycle is expressed as data, not callbacks.** Kafka Connect has
`SinkTask.open(partitions)`/`close(partitions)` for "you now own these"; FLIP-27 instead has
`addSplits(List<SplitT>)` + the split appearing in `snapshotState`'s return. Ownership is a *set that
the reader reports*, not an event pair the reader must mirror correctly. **This is strictly better** —
there is no way for the reader's idea of what it owns to drift from the framework's, because the
framework asks the reader.

### 6.2 Sink lifecycle

```
Sink.createWriter(WriterInitContext)
   |  SupportsWriterState.restoreWriter(WriterInitContext, Collection<WriterStateT>)
        ↓
    write(element, ctx)   … many times
        ↓
    flush(endOfInput)                       ← "on checkpoint or end of input"
        ↓
    prepareCommit() -> Collection<CommittableT>     (CommittingSinkWriter)
        ↓
    snapshotState(checkpointId) -> List<WriterStateT>   (StatefulSinkWriter)
        ↓
    [checkpoint durable]
        ↓
    Committer.commit(Collection<CommitRequest<CommT>>)   ← separate object, own lifecycle
        ↓
    close()  (both AutoCloseable)
```

`SupportsCommitter.createCommitter(CommitterInitContext)` is called independently of
`createWriter` — **the committer is a separate object with a separate lifecycle**, because in Flink it
runs in a different operator (possibly a different process). That separation is exactly what canal
needs for the future out-of-process boundary: the writer and the committer must not share memory.

`flush(boolean endOfInput)` folding "checkpoint" and "end of input" into one method with a flag is a
small, good decision: there is exactly one flush path, and the terminal case is a parameter rather
than a second method that implementors forget.

### 6.3 Context and cancellation — Flink's weakest area, and the clearest thing to do differently

**There is no `Context`, no deadline, and no cancellation token anywhere in the Source or Sink API.**
Cancellation is:

- `close()` called from the task thread after the task is cancelled;
- for the pull loop: `pollNext` must be non-blocking, so cancellation is "stop calling it";
- for `SplitReader` (the `connector-base` helper layer), an explicit `wakeUp()`:

```java
@PublicEvolving
public interface SplitReader<E, SplitT extends SourceSplit> extends AutoCloseable {

    /**
     * Fetch elements into the blocking queue for the given splits. The fetch call could be blocking
     * but it should get unblocked when {@link #wakeUp()} is invoked. In that case, the
     * implementation may either decide to return without throwing an exception, or it can just
     * throw an interrupted exception. In either case, this method should be reentrant, meaning that
     * the next fetch call should just resume from where the last fetch call was waken up or
     * interrupted.
     */
    RecordsWithSplitIds<E> fetch() throws IOException;

    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);   // "This call should be non-blocking."

    void wakeUp();

    default void pauseOrResumeSplits(Collection<SplitT> splitsToPause,
                                     Collection<SplitT> splitsToResume) { throw new UnsupportedOperationException(...); }
}
```

`wakeUp()` is Kafka Connect's `SourceTask.stop()`-sets-a-flag pattern with a better name, and it
carries the same burden: *"this method should be reentrant, meaning that the next fetch call should
just resume from where the last fetch call was waken up or interrupted."* The connector author must
implement reentrancy by hand, with no timeout and no reason code.

There is also a hardcoded close timeout in CDC's stream fetcher:

```java
    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;
...
            if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn("Failed to close the stream fetcher in {} seconds.", READER_CLOSE_TIMEOUT_SECONDS);
            }
```

and a configurable one in `connector-base`:

```java
    public static final ConfigOption<Long> SOURCE_READER_CLOSE_TIMEOUT =
            ConfigOptions.key("source.reader.close.timeout").longType().defaultValue(30000L)
                    .withDescription("The timeout when closing the source reader");
```

**For canal:** `context.Context` on every blocking method — `Read(ctx)`, `Write(ctx, batch)`,
`PrepareCommit(ctx)`, `Commit(ctx, committables)`, `Snapshot(ctx, id)`, `Enumerate(ctx)`. Go gives you
`wakeUp()`, deadlines, and reason propagation for free, and it makes the future gRPC boundary work
without inventing a cancellation protocol. Flink's `wakeUp()`+`AutoCloseable`+30s-timeout is what you
get when the language has no such primitive; there is no reason to reproduce it.

### 6.4 Graceful shutdown of a bounded pipeline

`InputStatus.END_OF_INPUT` + `signalNoMoreSplits(subtask)` + `notifyNoMoreSplits()` +
`Boundedness.BOUNDED` together give a *proper* termination protocol:

```java
// SourceReaderBase
    private InputStatus finishedOrAvailableLater() {
        final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (elementsQueue.isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            return InputStatus.END_OF_INPUT;
        } else {
            return InputStatus.MORE_AVAILABLE;
        }
    }
```

`END_OF_INPUT` requires **both** "the enumerator said no more splits" **and** "all fetchers shut down"
**and** "the queue is drained". Three conditions, not one. And on the sink side `flush(true)` is the
matching signal. Kafka Connect has no equivalent of any of this — no `COMPLETED` state at all.

**canal needs `END_OF_INPUT` in core from day one** (design-rules: a batch/snapshot pipeline must be
able to say "finished", and the status model must have a terminal success state).

### 6.5 Error and retry classification

This is Flink's other weak area, and worth naming honestly:

- **Source**: any exception from `pollNext`/`createEnumerator`/`restoreEnumerator` fails the task/job.
  `Source.createEnumerator` javadoc: *"The implementor is free to forward all exceptions directly.
  Exceptions thrown from this method cause JobManager failure/recovery."* `createReader`: *"Exceptions
  thrown from this method cause task failure/recovery."* **There is no retriable-vs-fatal
  distinction at the connector API level.** Recovery is via the job-level restart strategy.
- **Committer**: `commit` javadoc: *"@throws IOException for reasons that may yield a complete restart
  of the job."* — but the *per-committable* `CommitRequest` gives a real four-way classification:
  `retryLater()`, `updateAndRetryLater(c)`, `signalFailedWithKnownReason(t)`,
  `signalFailedWithUnknownReason(t)`, plus `signalAlreadyCommitted()` and `getNumberOfRetries()`.
- `CheckpointListener`: *"Exceptions thrown from this method result in task- or job failure and
  recovery"*, and *"Throwing an exception will not change the completion/abortion of the checkpoint."*
- `SplitEnumeratorContext.callAsync` javadoc: *"Note that an exception thrown from the handler would
  result in failing the job."*

So: **the only granular error model in the whole connector SPI is `CommitRequest`.** Everything else
is "throw and the job restarts". `CheckpointFailureReason` exists in `flink-runtime` as a rich
internal enum, but it is not exposed to connectors.

`CommitRequest.signalFailedWithKnownReason` is also where Flink admits an unfinished design:

> *"The commit failed for known reason and should not be retried. **Currently calling this method only
> logs the error, discards the comittable and continues.** In the future the behaviour might be
> configurable."*

(typo `comittable` is upstream). That is **silent data loss configured by a method name** — see §13.

**For canal:** the `CommitRequest` shape is right and should be generalised to *both* directions and
to per-record errors, backed by canal's own failure taxonomy (from `docs/design-rules.md`:
transient-upstream, transient-internal, permanent-upstream, permanent-mapping, permanent-contract,
duplicate-idempotent-success, clock-skew). Flink proves the per-item-request pattern works; canal's
own taxonomy is richer than Flink's four outcomes and should be the vocabulary.

---

## 7. Config model

### 7.1 DataStream connectors: no config model at all

A FLIP-27 `Source` or a Sink v2 `Sink` is **a serialisable Java object constructed by user code**,
typically via a hand-written builder (`KafkaSource.builder()...build()`). There is:

- no `configure(Map)` method,
- no declared option set,
- no validation hook,
- no way for a UI to discover what a connector accepts.

The only stated contract is in the `Sink` javadoc: *"The `Sink` needs to be serializable. **All
configuration should be validated eagerly.**"* — validation is "throw in your builder".

`SourceReaderContext.getConfiguration()` hands the reader *Flink's* `Configuration`, not the
connector's own config. This is how `connector-base` reads its two knobs:

```java
@PublicEvolving
public class SourceReaderOptions {
    public static final ConfigOption<Long> SOURCE_READER_CLOSE_TIMEOUT =
            ConfigOptions.key("source.reader.close.timeout").longType().defaultValue(30000L)
                    .withDescription("The timeout when closing the source reader");

    public static final ConfigOption<Integer> ELEMENT_QUEUE_CAPACITY =
            ConfigOptions.key("source.reader.element.queue.capacity").intType().defaultValue(2)
                    .withDescription("The capacity of the element queue in the source reader.");

    public final long sourceReaderCloseTimeout;
    public final int elementQueueCapacity;
    public SourceReaderOptions(Configuration config) { ... }
}
```

`ConfigOption<T>` (`flink-core/.../configuration/ConfigOption.java`) is a typed option descriptor with
key, type, default, description, deprecated/fallback keys — good, but it is a *cluster* config
primitive that connectors borrow, not a connector-facing schema.

**Verdict: Flink's DataStream connector API fails canal's UI requirement outright.** There is nothing
for a frontend to read.

### 7.2 Table/SQL factories: the self-describing layer, and it is much thinner than Kafka Connect's

`flink-table-common/.../table/factories/Factory.java`, `@PublicEvolving`:

```java
@PublicEvolving
public interface Factory {

    String factoryIdentifier();

    Set<ConfigOption<?>> requiredOptions();

    Set<ConfigOption<?>> optionalOptions();
}
```

Class javadoc, which is where the interesting admissions and conventions live:

> *"A factory is uniquely identified by `Class` and `#factoryIdentifier()`."*
>
> *"The list of available factories is discovered using Java's Service Provider Interfaces (SPI).
> Classes that implement this interface can be added to
> `META_INF/services/org.apache.flink.table.factories.Factory` in JAR files."*
>
> *"Every factory declares a set of required and optional options. **This information will not be used
> during discovery but is helpful when generating documentation and performing validation.** A factory
> may discover further (nested) factories, the options of the nested factories must not be declared in
> the sets of this factory."*
>
> *"**It is the responsibility of each factory to perform validation before returning an instance.**"*

and a naming convention canal should simply adopt as-is:

> *"* Try to **reuse** key names as much as possible. …
> * Key names should be declared in **lower case**. Use "-" instead of dots or camel case to split words.
> * Key names should be **hierarchical** where appropriate. Think about how one would define such a
>   hierarchy in JSON or YAML file (e.g. `sink.bulk-flush.max-actions`).
> * In case of a hierarchy, try not to use the higher level again in the key name (e.g. do
>   `sink.partitioner` instead of `sink.sink-partitioner`) to **keep the keys short**.
> * Key names which can be templated, e.g. to refer to a specific column, should be listed using '#'
>   as the placeholder symbol. For example, use `fields.#.min`."*

Also: `factoryIdentifier()` *"should be declared as one lower case word (e.g. `kafka`). **If multiple
factories exist for different versions, a version should be appended using "-"** (e.g.
`elasticsearch-7`)."* — versioning by *name suffix*, which is a workaround for having no version
field. canal should have `(name, version)` as structured data, not a string convention.

### 7.3 What Flink's config model lacks that Kafka Connect's `ConfigDef` has

| Capability | Kafka Connect `ConfigDef` | Flink `Factory` + `ConfigOption` |
|---|---|---|
| Typed option, default, doc | yes | yes |
| Required vs optional | `NO_DEFAULT_VALUE` sentinel | **explicit two sets** — better |
| Validator | `Validator.ensureValid` | no interface; *"responsibility of each factory"* |
| Per-field error list | `Config(List<ConfigValue>)` with `errorMessages` | **none** — throw |
| Dynamic valid values / visibility | `Recommender` + `dependents` | **none** |
| UI presentation metadata (`group`, `orderInGroup`, `width`, `displayName`, `importance`) | yes | **none** |
| Served over HTTP for a UI | `PUT /connector-plugins/{p}/config/validate` | **no** |
| Nested/repeated structures | faked with dotted prefixes | *"A factory may discover further (nested) factories"* — delegated, and the nested options are explicitly excluded from the parent's sets |

Two things Flink does *better*: **explicit `requiredOptions()`/`optionalOptions()` sets** (a
sentinel-free required/optional distinction), and **nested factory delegation** — a `kafka` sink
factory declares its own options and defers `value.format`'s options to the format factory, rather than
enumerating them. That composition rule is worth stealing: **each plugin declares only its own
options; nested plugins declare theirs; the core composes the union for the UI.** It is the answer to
Connect's `transforms.a.type=…` dotted-prefix hack, which `ConfigDef` cannot describe.

Everything else in the table favours Kafka Connect. **canal's config model should be `ConfigDef`-shaped
(see `docs/research/kafka-connect.md` §Config Model) with Flink's required/optional split and nested-
factory composition rule bolted on.**

### 7.4 Flink CDC's pipeline YAML — the operator-facing shape

Flink CDC 3.x exposes pipeline-level policy as YAML config, e.g.

```yaml
pipeline:
  schema.change.behavior: evolve
```

and per-sink `include.schema.changes` / `exclude.schema.changes`. Notable connector-level options read
from `mysql-cdc.md` that canal will need equivalents for, because they are *generic* concerns wearing
MySQL clothes:

| Option | Generic concern |
|---|---|
| `scan.incremental.snapshot.enabled` | opt into chunked snapshot vs legacy single-shot |
| `scan.incremental.snapshot.chunk.size` | chunk row count |
| `scan.incremental.snapshot.chunk.key-column` | which key to split on when the natural one is absent |
| `scan.incremental.snapshot.unbounded-chunk-first.enabled` | *"Whether to assign the unbounded chunks first during snapshot reading phase. This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk."* |
| `scan.incremental.close-idle-reader.enabled` | retire snapshot workers when the stream phase begins |
| `scan.startup.mode ∈ {initial, earliest-offset, latest-offset, specific-offset, timestamp, snapshot}` | **the pipeline-type/startup matrix** |
| `heartbeat.interval` (default 30s) | keep the checkpointed position fresh while idle |

`scan.startup.mode` is the one to look at hard: **six startup modes, one of which (`snapshot`) makes the
pipeline bounded** — *"Only the snapshot phase is performed and exits after the snapshot phase reading
is completed."* That is canal's "several kinds of pipeline" requirement expressed as *one option on one
source*, not as separate pipeline classes. The `HybridSplitAssigner`/`SnapshotSplitAssigner`/
`StreamSplitAssigner` triple is the strategy object behind it.

**Steal the shape:** pipeline type is `(startup mode) × (assigner strategy)` selected by config, and
`Boundedness` falls out of it. Not a separate `StreamingPipeline`/`BatchPipeline` type hierarchy.

---

## 8. Backpressure

### 8.1 The core mechanism: credit-based flow control, and a *non-blocking* connector API

Flink's inter-operator backpressure is credit-based network flow control below the connector API. What
the *connector* sees is the deliberate design that `pollNext` must not block:

- `pollNext(output)` returns `NOTHING_AVAILABLE` and the runtime waits on `isAvailable()`'s future;
- `write(element, ctx)` may block (`throws IOException, InterruptedException`) — and blocking *is* the
  backpressure signal, propagating upstream through the network stack.

So the source is **pull with an explicit readiness future**, and the sink is **push that may block**.
No credits are exposed to connector code.

### 8.2 The hand-off queue in `connector-base` — bounded by construction, and *tiny*

`SourceReaderBase` is the recommended base class (`SourceReader` javadoc: *"For most non-trivial source
reader, it is recommended to use `SourceReaderBase` which provides an efficient hand-over protocol to
avoid blocking I/O inside the task thread and supports various split-threading models."*). Its core:

```java
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
```

with capacity:

```java
    public static final ConfigOption<Integer> ELEMENT_QUEUE_CAPACITY =
            ConfigOptions.key("source.reader.element.queue.capacity").intType().defaultValue(2)
                    .withDescription("The capacity of the element queue in the source reader.");
```

**Two.** Not two records — two *batches* (`RecordsWithSplitIds<E>`, one per `SplitReader.fetch()`
call). The architecture is:

```
 SplitReader.fetch()  ──►  FutureCompletingBlockingQueue (capacity 2)  ──►  pollNext()  ──► ReaderOutput
   [fetcher thread(s)]              bounded hand-off                    [task thread]
   blocking I/O allowed                                                 must not block
```

and `pollNext` returns `MORE_AVAILABLE` on the *record* granularity within a batch:

```java
    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        ...
                // We always emit MORE_AVAILABLE here, even though we do not strictly know whether
                // ...
                return trace(InputStatus.MORE_AVAILABLE);
```

and availability is either an already-complete constant or the queue's future:

```java
                ? FutureCompletingBlockingQueue.AVAILABLE
                : elementsQueue.getAvailabilityFuture();
```

**This is the single most directly transplantable design in the whole document for canal's buffer
(design rule R6, "a buffer without a rejection path is not a buffer"; open decision #3, "backpressure
signalling").** Concretely:

- **one bounded queue** between the I/O goroutine and the pipeline goroutine, capacity in *batches*
  and defaulting to a very small number;
- blocking I/O confined to the fetcher side, never on the emitting side;
- readiness as a channel/future rather than a poll-and-sleep;
- `wakeUp()` — in Go, closing a channel or cancelling a `ctx` — to unblock a parked fetch.

In Go this is literally `chan RecordBatch` with `cap == 2`, `select` on it plus `ctx.Done()`. Flink
needed ~400 lines of `FutureCompletingBlockingQueue` to express what a buffered channel does natively.
**Capacity 2 is the important number**: enough to overlap one fetch with one emit, small enough that
memory is bounded by `2 × batch size × readers` and that shutdown drains fast.

### 8.3 Split-level flow control

Two mechanisms, both `default`-throwing (§13):

```java
// SourceReader
default void pauseOrResumeSplits(Collection<String> splitsToPause, Collection<String> splitsToResume);
// SplitReader
default void pauseOrResumeSplits(Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume);
```

with the javadoc note that makes them safe: *"Note that no other methods can be called in parallel, so
updating subscriptions can be done atomically. This method is simply providing connectors with more
expressive APIs the opportunity to update all subscriptions at once."*

Their stated purpose is watermark alignment, not throughput — but the **shape** is the right one for
canal: a *batched, atomic* "pause these units of work, resume those" call, rather than N per-split
calls. Kafka Connect's `SinkTaskContext.pause(TopicPartition...)` is the same idea, less carefully
specified.

### 8.4 Backlog mode — a first-class "I am catching up, optimise for throughput" signal

```java
    /**
     * Reports to JM whether this source is currently processing backlog.
     *
     * <p>When source is processing backlog, it means the records being emitted by this source is
     * already stale and there is no processing latency requirement for these records. This allows
     * downstream operators to optimize throughput instead of reducing latency for intermediate
     * results.
     *
     * <p>If no API has been explicitly invoked to specify the backlog status of a source, the
     * source is considered to have isProcessingBacklog=false by default.
     */
    @PublicEvolving
    default void setIsProcessingBacklog(boolean isProcessingBacklog) {
        throw new UnsupportedOperationException();
    }
```

**A source declares "I am in bulk-catch-up mode" and the runtime changes its batching/latency
trade-off.** For canal this is exactly the snapshot-vs-stream distinction, and it should be a core
concept: during initial scan, batch aggressively and ignore latency; once caught up, favour latency.
Flink CDC's snapshot phase is precisely `isProcessingBacklog = true`.

### 8.5 What happens when the sink is slower than the source

In Flink: `write()` blocks → the sink task's input buffers fill → credit is withheld → upstream network
buffers fill → the source task's output buffers fill → `collect()` blocks inside `pollNext` → the
fetcher's queue (capacity 2) fills → `SplitReader.fetch()` blocks or is not called. The whole chain is
bounded; nothing grows without limit.

The *cost* is that checkpoint barriers travel in this same congested stream, which is the entire reason
unaligned checkpoints exist:

> *"when a Flink job is running under heavy backpressure, the dominant factor in the end-to-end time of
> a checkpoint can be the time to propagate checkpoint barriers to all operators/subtasks … and can be
> observed by high alignment time and start delay metrics."*

Buffer debloating is the other half of the answer:

> *"Flink 1.14 introduced a new tool to automatically control the amount of buffered in-flight data
> between Flink operators/subtasks. The buffer debloating mechanism can be enabled by setting the
> property `taskmanager.network.memory.buffer-debloat.enabled` to `true`."*
> *"When using buffer debloating with unaligned checkpoints, the added benefit will be smaller
> checkpoint sizes and quicker recovery times (there will be less in-flight data to persist and
> recover)."*

**The lesson for canal is a negative one and it is important:** *the more you buffer, the worse your
checkpoint latency and recovery time get.* Flink spent two major features (FLIP-76 unaligned
checkpoints, buffer debloating) undoing the consequences of deep buffering. canal should choose small
bounded buffers from the start — which also means canal does **not** need unaligned checkpoints, because
there is no deep in-flight buffer to snapshot. This is a case where the right early decision removes an
entire subsystem.

Relevant metrics for observing this (from `MetricNames.java`, all verified):
`isBackPressured`, `backPressuredTimeMsPerSecond`, `idleTimeMsPerSecond`, `busyTimeMsPerSecond`,
`softBackPressuredTimeMsPerSecond` / `hardBackPressuredTimeMsPerSecond`,
`maxSoftBackPressureTimeMs` / `maxHardBackPressureTimeMs`, `estimatedTimeToConsumeBuffersMs`,
`debloatedBufferSize`, `checkpointAlignmentTime`, `checkpointStartDelayNanos`.

The **soft/hard back-pressure split** is a distinction worth copying: "waiting for a buffer" vs
"blocked on the downstream" are different diagnoses.

---

## 9. Delivery guarantees

### 9.1 The three tiers, expressed as which interfaces you implement

Flink expresses the guarantee **in the type system**, not in a config enum. From the `Sink` javadoc:

> *"A basic `Sink` is a **stateless sink that can flush data on checkpoint to achieve at-least-once
> consistency**. Sinks with additional requirements should implement `SupportsWriterState` or
> `SupportsCommitter`."*

| Implement | Guarantee |
|---|---|
| `Sink` + `SinkWriter` (`write` + `flush`) | at-least-once |
| `+ SupportsWriterState` / `StatefulSinkWriter` | at-least-once with resumable writer state (e.g. in-progress files) |
| `+ SupportsCommitter` / `CommittingSinkWriter` + `Committer` | **exactly-once via two-phase commit** |

And on the source side, `AlignmentType.AT_LEAST_ONCE` vs `ALIGNED`/`UNALIGNED` selects whether the
snapshot is a consistent cut at all (`execution.checkpointing.mode`).

**This is a better design than a config enum**, and canal should copy it: *the guarantee is a
capability the connector declares by implementing an interface*, and the core can compute the
end-to-end guarantee as `min(source capability, sink capability, pipeline mode)` and **refuse an
impossible pipeline at submit time**. Kafka Connect's `exactlyOnceSupport(config)` returning a nullable
enum is the same idea done worse.

### 9.2 The two-phase commit protocol, exactly

Recapping §3.5 as a protocol rather than as a lifecycle:

1. `SinkWriter.write(...)` — data goes to a *staging* location that is invisible to readers.
2. `flush(false)` — staging durably written.
3. `CommittingSinkWriter.prepareCommit()` → `Collection<CommittableT>` — **the committable is a
   *reference* to staged data, not the data**. Typically `(path, size)` or `(transactionId)`.
4. `StatefulSinkWriter.snapshotState(checkpointId)` → `List<WriterStateT>` — the pending committables
   are persisted here.
5. Checkpoint becomes durable.
6. `CheckpointListener.notifyCheckpointComplete(id)` → `Committer.commit(requests)` makes staged data
   visible.

Recovery: restore at step 5's state, re-run 6. Hence idempotency + `signalAlreadyCommitted()`.

`SupportsCommitter` javadoc states the split's purpose: *"To facilitate the separation the
`CommittingSinkWriter` creates committables on checkpoint or end of input and the sends it to the
`Committer`."*

The committable must be **serialisable via `SimpleVersionedSerializer<CommittableT>`**, which is what
lets the committer be a different operator, a different process, or (in a future canal) a different
binary version. **That single constraint — "a committable is bytes, not a live object" — is what makes
the whole pattern survive process boundaries and upgrades.**

### 9.3 `TwoPhaseCommitSinkFunction` — the deprecated predecessor, still the best-documented version

`flink-streaming-java/.../api/functions/sink/TwoPhaseCommitSinkFunction.java`:

```java
/**
 * This is a recommended base class for all of the {@link SinkFunction} that intend to implement
 * exactly-once semantic. It does that by implementing two phase commit algorithm on top of the
 * {@link CheckpointedFunction} and {@link CheckpointListener}. User should provide custom {@code
 * TXN} (transaction handle) and implement abstract methods handling this transaction handle.
 *
 * @deprecated This interface will be removed in future versions. Use the new {@link
 *     org.apache.flink.api.connector.sink2.Sink} interface instead.
 */
@Deprecated
@PublicEvolving
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {
```

The six abstract/overridable methods are the canonical XA-style vocabulary:

```java
    /** Write value within a transaction. */
    protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;

    /** Method that starts a new transaction. */
    protected abstract TXN beginTransaction() throws Exception;

    /**
     * Pre commit previously created transaction. Pre commit must make all of the necessary steps to
     * prepare the transaction for a commit that might happen in the future. After this point the
     * transaction might still be aborted, but underlying implementation must ensure that commit
     * calls on already pre committed transactions will always succeed.
     *
     * <p>Usually implementation involves flushing the data.
     */
    protected abstract void preCommit(TXN transaction) throws Exception;

    /**
     * Commit a pre-committed transaction. If this method fail, Flink application will be restarted
     * and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
     * same transaction.
     */
    protected abstract void commit(TXN transaction);

    /**
     * Invoked on recovered transactions after a failure. User implementation must ensure that this
     * call will eventually succeed. If it fails, Flink application will be restarted and it will be
     * invoked again. **If it does not succeed eventually, a data loss will occur.** Transactions will
     * be recovered in an order in which they were created.
     */
    protected void recoverAndCommit(TXN transaction) { commit(transaction); }

    /** Abort a transaction. */
    protected abstract void abort(TXN transaction);

    /** Abort a transaction that was rejected by a coordinator after a failure. */
    protected void recoverAndAbort(TXN transaction) { abort(transaction); }

    /**
     * Callback for subclasses which is called after restoring (each) user context.
     * @param handledTransactions transactions which were already committed or aborted and do not
     *     need further handling
     */
    protected void finishRecoveringContext(Collection<TXN> handledTransactions) {}
```

(emphasis mine on the data-loss sentence; the rest is verbatim.)

The pending-transaction bookkeeping, verbatim from the fields, is precisely the "ready set / pending
set" recipe from `CheckpointListener`'s javadoc, made concrete:

```java
    protected final LinkedHashMap<Long, TransactionHolder<TXN>> pendingCommitTransactions =
            new LinkedHashMap<>();
    protected transient ListState<State<TXN, CONTEXT>> state;
    @Nullable private TransactionHolder<TXN> currentTransactionHolder;
    /** Specifies the maximum time a transaction should remain open. */
    private long transactionTimeout = Long.MAX_VALUE;
    private boolean ignoreFailuresAfterTransactionTimeout;
    /**
     * If a transaction's elapsed time reaches this percentage of the transactionTimeout, a warning
     * message will be logged. Value must be in range [0,1]. Negative value disables warnings.
     */
    private double transactionTimeoutWarningRatio = -1;
```

`LinkedHashMap<Long, …>` keyed by checkpoint id, **insertion-ordered** — because
`recoverAndCommit`'s contract is *"Transactions will be recovered in an order in which they were
created."*

And the `snapshotState` body is the ordering, in six lines:

```java
        if (currentTransactionHolder != null) {
            preCommit(currentTransactionHolder.handle);
            pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
        }
        ...
            currentTransactionHolder = beginTransactionInternal();
```

The `notifyCheckpointComplete` comment block is the clearest statement anywhere of *why* the subsuming
contract exists, and it names the two real-world causes:

```java
        // the following scenarios are possible here
        //
        //  (1) there is exactly one transaction from the latest checkpoint that
        //      was triggered and completed. That should be the common case.
        //      Simply commit that transaction in that case.
        //
        //  (2) there are multiple pending transactions because one previous
        //      checkpoint was skipped. That is a rare case, but can happen
        //      for example when:
        //
        //        - the master cannot persist the metadata of the last
        //          checkpoint (temporary outage in the storage system) but
        //          could persist a successive checkpoint (the one notified here)
        //
        //        - other tasks could not persist their status during
        //          the previous checkpoint, but did not trigger a failure because they
        //          could hold onto their state and could successfully persist it in
        //          a successive checkpoint (the one notified here)
        //
        //      In both cases, the prior checkpoint never reach a committed state, but
        //      this checkpoint is always expected to subsume the prior one and cover all
        //      changes since the last successful one. As a consequence, we need to commit
        //      all pending transactions.
```

**Transaction timeout as a first-class concern** is the other thing to steal. External transactions
expire (Kafka's `transaction.max.timeout.ms`, a DB's idle-in-transaction timeout). Flink exposes:

```java
    public TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> setTransactionTimeout(long transactionTimeout);
    // + ignoreFailuresAfterTransactionTimeout, + transactionTimeoutWarningRatio
```

and the recovery path consults it:

```java
    private void recoverAndCommitInternal(TransactionHolder<TXN> transactionHolder) {
        try {
            logWarningIfTimeoutAlmostReached(transactionHolder);
            recoverAndCommit(transactionHolder.handle);
        } catch (final Exception e) {
            final long elapsedTime = clock.millis() - transactionHolder.transactionStartTime;
            if (ignoreFailuresAfterTransactionTimeout && elapsedTime > transactionTimeout) {
                LOG.error(/* ... transactionTimeout ... */);
            } else { throw e; }
        }
    }
```

So: **on recovery, a transaction older than the timeout is knowingly abandoned, loudly, if configured
to be.** That is an honest treatment of an unavoidable failure mode (long outage → the external
transaction is gone → the data is unrecoverable), and it is *configurable rather than silent*. canal
needs the same: a committable has an expiry, and expired committables produce a loud terminal state,
not a silent skip.

`recoverAndCommit` / `recoverAndAbort` being *separate from* `commit` / `abort` (defaulting to them) is
also worth copying: **the recovery path is usually the same but sometimes must differ**, and giving it
its own overridable name documents that.

### 9.4 Source-side exactly-once

Flink CDC's claim, from the MySQL doc:

> *"The MySQL CDC connector is a Flink Source connector which will read table snapshot chunks first and
> then continues to read binlog, both snapshot phase and binlog phase, MySQL CDC connector read with
> **exactly-once processing** even failures happen."*

The mechanism is entirely the split machinery: the reader's `snapshotState` returns its splits, the
splits carry positions, and the checkpoint contains both. There is no source-side transaction at all.
**Exactly-once "processing" here means exactly-once *effect on Flink state*, not exactly-once
*emission*** — and the `AbstractScanFetchTask` comment says so where the guarantee degrades:

> *"Directly set HW = LW if backfill is skipped. Stream events created during snapshot phase could be
> processed later in stream reading phase. **Note that this behaviour downgrades the delivery guarantee
> to at-least-once.** We can't promise that the snapshot is exactly the view of the table at low
> watermark moment, so stream events created during snapshot might be replayed later in stream reading
> phase."*

and where duplicates are structurally unavoidable:

> `dispatchEndWaterMarkEvent`: *"Data change events between (high_watermark, end_watermark) are backfill
> data, **which maybe duplicate of snapshot data. Thus, only the intersection of both is
> exactly-once.**"*

**Read that carefully — it is the most honest sentence in the CDC codebase.** The per-chunk backfill
replays a range that overlaps the buffered snapshot, and correctness comes from the **upsert in step
(5)** of the Offset Signal Algorithm: *"Upsert the read binlog records into the buffered chunk records,
and emit all records in the buffer as final output (**all as INSERT records**) of the snapshot chunk."*

So the actual guarantee is: **snapshot output is idempotent-by-construction (keyed upsert), stream
output is exactly-once (filtered by watermark), and the two are stitched at `min(HIGH)`.** The whole
scheme depends on the *sink* treating snapshot rows as upserts on a key.

**This is the crucial requirement canal must surface in its interfaces:** a chunked snapshot with
backfill produces **keyed upserts, not appends**. A sink that cannot upsert by key cannot be the
destination of a chunked snapshot without a dedupe stage. That is a capability negotiation
(`SupportsUpsert` / `SupportsIdempotentWrite`) that must exist in core, checked at submit time.

### 9.5 Idempotency and dedup

Flink core has **no dedup facility at all** — no seen-set, no key concept, nothing. Idempotency is
pushed to two places, both explicit in javadoc:

- `Committer`: *"A commit must be idempotent … These `CommitRequest`s must not change the external
  system and implementers are asked to signal `CommitRequest#signalAlreadyCommitted()`."*
- `SupportsPostCommitTopology.addPostCommitTopology`: *"**All operations need to be idempotent**: on
  recovery, any number of committables may be replayed that have already been committed. It's mandatory
  that these committables have no effect on the external system."*

Flink CDC's dedup is the watermark filter of §4.4 — and note its two best properties, which canal should
replicate: it is **bounded** (only chunks of tables still in the snapshot→stream transition), and it is
**self-retiring** (`pureStreamPhaseTables` gate). This is the opposite of a "50,000-entry
process-lifetime FIFO" (canal's design rule **R5** describes exactly that anti-pattern in the abandoned
attempt). The dedup window here is derived from the algorithm's own state, is persisted in the split, and
provably terminates.

**The generalisable rule: a dedup filter must have a proof of termination and its state must live in the
checkpoint.** Flink CDC has both; a RAM LRU has neither.

---

## 10. Plugin boundary

### 10.1 In-process, jar-on-classpath, no isolation

Flink connectors are **Java classes in a jar on the classpath** (or in `lib/`, or shaded into the user
jar). The DataStream connector API has:

- **no registry**;
- **no discovery** — the user's job code constructs the `Source`/`Sink` directly
  (`env.fromSource(kafkaSource, watermarkStrategy, "src")`);
- **no version negotiation**;
- **no process isolation**.

`UserCodeClassLoader` appears in both `SourceReaderContext.getUserCodeClassLoader()` and
`WriterInitContext.getUserCodeClassLoader()`, so there is *classloader* separation between framework
and user code, but not between connectors.

The Table/SQL layer has the only discovery mechanism: **Java SPI**, per `Factory`'s javadoc:

> *"The list of available factories is discovered using Java's Service Provider Interfaces (SPI).
> Classes that implement this interface can be added to
> `META_INF/services/org.apache.flink.table.factories.Factory` in JAR files."*

Identified by `(Class, factoryIdentifier())`, with version-by-name-suffix (`elasticsearch-7`).

**canal's chosen model (init-time Go registry, single static binary) is strictly better than both**, and
Kafka Connect's `plugin.discovery=HYBRID_WARN` migration saga (see the Kafka Connect dossier) is the
cautionary tale. Nothing to learn here except *what to skip*.

### 10.2 Serialisability as the discipline that would make out-of-process possible

Flink's interfaces score *much* better than Kafka Connect's on the "could this be gRPC?" test, and the
reason is a set of deliberate constraints:

| Constraint | Where it's stated | Why it matters for a wire boundary |
|---|---|---|
| `Source`/`Sink` are `Serializable` | `interface Sink<InputT> extends Serializable`; `SourceReaderFactory … extends Serializable` | the *plan* ships from JM to TM |
| Splits ship as bytes | `SimpleVersionedSerializer<SplitT> getSplitSerializer()` | split assignment is a wire message |
| Enumerator checkpoint ships as bytes | `getEnumeratorCheckpointSerializer()` | enumerator state is a wire/disk artifact |
| Committables ship as bytes | `SimpleVersionedSerializer<CommittableT> getCommittableSerializer()` | committer can be a different operator/process |
| Writer state ships as bytes | `SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer()` | writer can be restored elsewhere |
| Control events ship as bytes | `interface SourceEvent extends Serializable` | reader↔enumerator is already a message protocol |
| **All configuration validated eagerly** | `Sink` javadoc | the plan is a value, not a live connection |

**Flink already runs its enumerator in a different process from its readers.** `SplitEnumerator` lives
on the JobManager; `SourceReader` lives on TaskManagers; `assignSplits` and `SourceEvent` are RPCs.
That is a *proof by construction* that the FLIP-27 factoring survives a process boundary — which is
exactly what canal's constraint #3 asks for.

**The rule to extract, and it is the most important rule in this section:** *everything that crosses a
role boundary is `(version, []byte)` produced by a serialiser the connector supplies.* Splits,
enumerator state, committables, writer state, control events. If canal holds that line, an
out-of-process connector is a transport swap, not an interface change.

### 10.3 Where Flink breaks its own rule — and what canal must avoid

The three sink topology mixins take and return `DataStream`:

```java
DataStream<CommittableMessage<CommittableT>> addPreCommitTopology(
        DataStream<CommittableMessage<WriterResultT>> committables);

void addPostCommitTopology(DataStream<CommittableMessage<CommittableT>> committables);
```

`DataStream` is a **job-graph builder**, not data. A connector implementing `SupportsPreCommitTopology`
is *writing Flink dataflow topology*. It:

- cannot be implemented out-of-process, at all;
- couples the connector to Flink's DataStream API version;
- pushes correctness obligations onto the connector author that the framework cannot check — see the
  javadoc:

> *"It's important that all `CommittableMessage`s are modified appropriately, such that all messages
> with the same subtask id will also be processed by the same `Committer` subtask and the
> `CommittableSummary` matches the respective count. **If committables are combined or split in any way,
> the summary needs to be adjusted.** … Subtask ids don't need to be consecutive or small. The global
> committer will use `CommittableSummary#getNumberOfSubtasks()` to determine if all committables have
> been received, so that number needs to correctly reflect the number of distinct subtask ids."*

That is a lot of invariant maintenance delegated to connector authors, and all three interfaces are
`@Experimental` years after introduction.

**For canal:** the legitimate need behind `SupportsPreCommitTopology` is real — *aggregate many writer
results into one commit* (Iceberg's motivating case, FLIP-372 §Motivation). Express it as **data**, not
as topology: a `CommittableAggregator` interface

```
Aggregate(ctx, []WriterResult) ([]Committable, error)
```

with both sides `(version, []byte)`-serialisable. Same capability, no dataflow types, works
out-of-process.

### 10.4 Versioning and compatibility

Three separate compatibility surfaces, and Flink handles them at three different quality levels:

1. **Persisted-artifact compatibility — good.** `SimpleVersionedSerializer.deserialize(version, bytes)`.
   The connector owns it and can support arbitrarily old versions.
2. **State-format compatibility — elaborate.** `TypeSerializerSnapshot` /
   `TypeSerializerSchemaCompatibility` four-valued resolution, plus the savepoint-format matrix (§13).
3. **Java ABI compatibility — bad, and the same trap as Kafka Connect.** Flink's stability annotations
   (`@Public`, `@PublicEvolving`, `@Experimental`, `@Internal`, `@Deprecated`) are the *only* mechanism,
   and adding a method to a `@Public` interface is handled by `default` methods that throw:

```java
    default void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        throw new UnsupportedOperationException();
    }
    default Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        throw new UnsupportedOperationException();
    }
    default int currentParallelism() { throw new UnsupportedOperationException(); }
    @PublicEvolving
    default void setIsProcessingBacklog(boolean isProcessingBacklog) {
        throw new UnsupportedOperationException();
    }
    @PublicEvolving
    default void pauseOrResumeSplits(Collection<String> a, Collection<String> b) {
        throw new UnsupportedOperationException("… will be dropped in a future Flink release.");
    }
```

**Five `default`-throwing methods** across `SplitEnumeratorContext`, `SourceReaderContext`,
`SourceReader` and `SplitReader`. Each is a capability that *should* have been a separate mixin —
`SupportsSpeculativeExecution`, `SupportsBacklogReporting`, `SupportsSplitPausing` — and FLIP-372
explicitly acknowledges this by making mixins the rule going forward. The old ones are stuck.

**Go's version is unambiguous and canal should state it as policy:**

- required interfaces: tiny, frozen, never grow;
- every optional capability: its own interface, discovered by type assertion, absence is a legal answer;
- contexts: interfaces the **core** implements and passes in, so core capability growth never breaks a
  connector;
- everything persisted or crossing a boundary: `(version, []byte)` with a connector-supplied
  serialiser.

Note that `SourceReaderContext`/`WriterInitContext` are already the right shape (core implements, passes
in) — which is why adding `currentParallelism()` to `SourceReaderContext` only needed a `default`, not a
new interface. **Contexts are the pressure-release valve; use them.**

### 10.5 One more compatibility mechanism worth stealing

```java
    @PublicEvolving
    interface WithCompatibleState {
        /**
         * A collection of state names of sinks from which the state can be restored. For example,
         * the new {@code FileSink} can resume from the state of an old {@code StreamingFileSink} as
         * a drop-in replacement when resuming from a checkpoint/savepoint.
         */
        Collection<String> getCompatibleWriterStateNames();
    }
```

**A connector can declare "I can adopt the persisted state of these *other, differently-named*
connectors."** That is how Flink migrated users from `StreamingFileSink` to `FileSink` without losing
in-progress files. canal will eventually rewrite a connector; this one method makes that migration a
declaration instead of an operator runbook. Cheap to add, impossible to retrofit.

---

## 11. Observability

### 11.1 Metric names, verified from `MetricNames.java`

Naming convention: **camelCase noun, `PerSecond` suffix for rates** —
`public static final String SUFFIX_RATE = "PerSecond";`

**Generic I/O (every operator, so both source and sink):**
`numRecordsIn`, `numRecordsOut`, `numRecordsInPerSecond`, `numRecordsOutPerSecond`,
`numBytesIn`, `numBytesOut`, `numBytesInPerSecond`, `numBytesOutPerSecond`,
`numBuffersIn`, `numBuffersOut`, `numBuffersOutPerSecond`.

**Source-specific:**
`numRecordsInErrors`, `currentFetchEventTimeLag`, `currentEmitEventTimeLag`, `watermarkLag`,
`pendingRecords`, `pendingBytes`, `sourceIdleTime`, `watermarkAlignmentDrift`.

**Sink-specific:**
`numRecordsSend`, `numBytesSend`, `numRecordsSendErrors`, `numRecordsOutErrors`, `currentSendTime`.

**Watermarks:** `currentInputWatermark`, `currentOutputWatermark`, `currentInput%dWatermark`.

**Task health / backpressure:**
`isBackPressured`, `idleTimeMsPerSecond`, `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`,
`softBackPressuredTimeMsPerSecond`, `hardBackPressuredTimeMsPerSecond`,
`maxSoftBackPressureTimeMs`, `maxHardBackPressureTimeMs`,
`accumulateIdleTimeMs`, `accumulateBusyTimeMs`, `accumulateBackPressuredTimeMs`,
`estimatedTimeToConsumeBuffersMs`, `debloatedBufferSize`, `numFiredTimers`, `numFiredTimersPerSecond`.

**Checkpointing (verified from `MetricNames.java` + `docs/ops/metrics.md`):**
`checkpointAlignmentTime`, `checkpointStartDelayNanos`, `initializationTime`,
`lastCheckpointDuration`, `lastCheckpointSize`, `lastCheckpointFullSize`,
`lastCompletedCheckpointId`, `lastCheckpointExternalPath`, `lastCheckpointRestoreTimestamp`,
`numberOfInProgressCheckpoints`, `numberOfCompletedCheckpoints`, `numberOfFailedCheckpoints`,
`totalNumberOfCheckpoints`.

Note `lastCheckpointSize` vs `lastCheckpointFullSize` — the doc explains:
*"The checkpointed size of the last checkpoint (in bytes), this metric could be different from
lastCheckpointFullSize if incremental checkpoint or changelog is enabled."*

**Recovery breakdown (a genuinely unusual and valuable set):**
`MailboxStartDurationMs`, `ReadOutputDataDurationMs`, `InitializeStateDurationMs`,
`GateRestoreDurationMs`, `DownloadStateDurationMs`, `RestoreStateDurationMs`,
`RestoredStateSizeBytes`, `RestoreAsyncCompactionDurationMs`.

**Cluster:** `numRunningJobs`, `taskSlotsAvailable`, `taskSlotsTotal`,
`numRegisteredTaskManagers`, `numPendingTaskManagers`, `numRestarts` (with `fullRestarts`
`@Deprecated`), `startWorkFailurePerSecond`, `numSlowExecutionVertices`,
`numEffectiveSpeculativeExecutions`.

Three observations worth acting on:

- **`pendingRecords` / `pendingBytes` are set *by the connector*** via
  `SourceReaderMetricGroup.setPendingRecordsGauge(Gauge)` / `setPendingBytesGauge(Gauge)`. This is how
  Flink gets a source-side lag metric that Kafka Connect *structurally cannot have* (its offsets are
  opaque). The connector, which understands its own position type, supplies the estimate.
  **canal should do the same: the connector supplies `PendingRecords()`/`PendingBytes()` as an optional
  capability, and the core owns naming and export.**
- **`currentFetchEventTimeLag` vs `currentEmitEventTimeLag`** — the gap between "when we read it from
  the source" and "when we handed it downstream" isolates *connector* lag from *pipeline* lag. Two
  numbers, one diagnosis.
- **Recovery is itself instrumented, broken into eight phases.** If restart-and-resume is a headline
  feature (it is, for canal), then restart time must be measured, not assumed.

### 11.2 The metric-group hierarchy — the plugin/core split canal should copy

Typed metric groups are handed to connectors through the contexts:

| Role | Group type | Where obtained |
|---|---|---|
| Source reader | `SourceReaderMetricGroup` | `SourceReaderContext.metricGroup()` |
| Split enumerator | `SplitEnumeratorMetricGroup` | `SplitEnumeratorContext.metricGroup()` |
| Sink writer | `SinkWriterMetricGroup` | `WriterInitContext.metricGroup()` |
| Committer | `SinkCommitterMetricGroup` | `CommitterInitContext.metricGroup()` |
| Generic operator I/O | `OperatorIOMetricGroup` | via the above |

and `SourceReader`'s class javadoc **tells the connector which metrics to provide, with a
recommendation level**:

> *"Implementations can provide the following metrics:
> * `OperatorIOMetricGroup#getNumRecordsInCounter()` (**highly recommended**)
> * `OperatorIOMetricGroup#getNumBytesInCounter()` (recommended)
> * `SourceReaderMetricGroup#getNumRecordsInErrorsCounter()` (recommended)
> * `SourceReaderMetricGroup#setPendingRecordsGauge(Gauge)`
> * `SourceReaderMetricGroup#setPendingBytesGauge(Gauge)`"*

**A typed metric group per role, handed in via context, with the expected metrics documented on the
interface.** That is exactly the right shape: plugin declares the values, core owns the names, the
tags, and the export. (Kafka Connect only reached this with `PluginMetrics` in 4.1.)

### 11.3 Flink CDC's snapshot-progress metrics — the model for canal's UI

`SourceEnumeratorMetrics` (read from source) is the best example anywhere of *pipeline-phase*
observability:

```java
    // Metric names
    public static final String IS_SNAPSHOTTING = "isSnapshotting";
    public static final String IS_STREAM_READING = "isStreamReading";
    public static final String NUM_TABLES_SNAPSHOTTED = "numTablesSnapshotted";
    public static final String NUM_TABLES_REMAINING = "numTablesRemaining";
    public static final String NUM_SNAPSHOT_SPLITS_PROCESSED = "numSnapshotSplitsProcessed";
    public static final String NUM_SNAPSHOT_SPLITS_REMAINING = "numSnapshotSplitsRemaining";
    public static final String NUM_SNAPSHOT_SPLITS_FINISHED = "numSnapshotSplitsFinished";
    public static final String SNAPSHOT_START_TIME = "snapshotStartTime";
    public static final String SNAPSHOT_END_TIME = "snapshotEndTime";
    public static final String NAMESPACE_GROUP_KEY = "namespace";
    public static final String SCHEMA_GROUP_KEY = "schema";
    public static final String TABLE_GROUP_KEY = "table";
```

with phase transitions as explicit calls — `enterSnapshotPhase()`, `exitSnapshotPhase()`,
`enterStreamReading()`, `exitStreamReading()` — driven from the assigner
(`HybridSplitAssigner.getNext()` calls `enumeratorMetrics.exitSnapshotPhase()` /
`enterStreamReading()`).

And **per-object metric sub-groups**, three levels deep:

```java
            MetricGroup metricGroup =
                    parentGroup
                            .addGroup(NAMESPACE_GROUP_KEY, databaseName)
                            .addGroup(SCHEMA_GROUP_KEY, schemaName)
                            .addGroup(TABLE_GROUP_KEY, tableName);
            metricGroup.gauge(NUM_SNAPSHOT_SPLITS_PROCESSED, () -> numSnapshotSplitsProcessed.intValue());
            metricGroup.gauge(NUM_SNAPSHOT_SPLITS_REMAINING, () -> numSnapshotSplitsRemaining.intValue());
            metricGroup.gauge(NUM_SNAPSHOT_SPLITS_FINISHED, () -> numSnapshotSplitsFinished.intValue());
            metricGroup.gauge(SNAPSHOT_START_TIME, () -> snapshotStartTime);
            metricGroup.gauge(SNAPSHOT_END_TIME, () -> snapshotEndTime);
```

Note the **three distinct split counters** — `remaining`, `processed`, `finished` — and that they are
maintained as *chunk-id sets*, not counters, so double-counting is impossible:

```java
        private Set<Integer> remainingSplitChunkIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private Set<Integer> processedSplitChunkIds = ...;
        private Set<Integer> finishedSplitChunkIds  = ...;

        public void addNewSplit(String newSplitId) {
            int chunkId = SnapshotSplit.extractChunkId(newSplitId);
            if (!remainingSplitChunkIds.contains(chunkId)) {
                remainingSplitChunkIds.add(chunkId);
                numSnapshotSplitsRemaining.getAndAdd(1);
            }
        }
        public void reprocessSplit(String reprocessSplitId) {
            addNewSplit(reprocessSplitId); removeProcessedSplit(reprocessSplitId);
        }
        public void finishProcessSplit(String processedSplitId) {
            addProcessedSplit(processedSplitId); removeRemainingSplit(processedSplitId);
        }
        public void tryToMarkSnapshotEndTime() {
            if (numSnapshotSplitsRemaining.get() == 0
                    && (numSnapshotSplitsFinished.get() == numSnapshotSplitsProcessed.get())) {
                snapshotEndTime = System.currentTimeMillis();
            }
        }
```

**`processed` ≠ `finished`**: processed = a reader emitted the chunk; finished = the enumerator received
and durably recorded its high watermark. The gap between them is *exactly* the in-flight
acknowledgement window, and `tryToMarkSnapshotEndTime` only fires when both are equal and remaining is
zero. And `reprocessSplit` handles the reassignment-after-failure case by moving a chunk backwards from
processed to remaining — **the progress metric can go down, correctly.**

**This is precisely what canal's frontend needs, and it is all derivable generically from the split
model:** phase (`isSnapshotting`/`isStreamReading`), objects remaining/done, splits
remaining/processed/finished, snapshot start/end time, per-object breakdown. **canal gets a real
progress bar and an ETA for free, from the core, for any source that supports chunked snapshot** —
something Kafka Connect fundamentally cannot offer because it does not know what a chunk is.

`SourceReaderMetrics` (CDC, reader side) additionally tracks fetch delay:

```java
    protected void reportMetrics(SourceRecord element) {
        Long messageTimestamp = getMessageTimestamp(element);
        if (messageTimestamp != null && messageTimestamp > 0L) {
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null) {
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
        }
    }
```

with an honest note in the emitter about why snapshot records are excluded from event-time lag:

> *"Records in snapshot mode have a zero timestamp in the message. We use the output without timestamp
> to collect the record. Metric `currentEmitEventTimeLag` will not be updated in the source operator in
> this case."*

**Lag is meaningless during a snapshot** — and the code says so rather than reporting a garbage number.
canal's UI must make the same distinction (design-rules: *"A metrics UI that cannot distinguish 'the
endpoint answered' from 'your data arrived' is actively misleading"*): during snapshot show
*percent-complete*, during stream show *lag*, and never show a lag number computed from snapshot rows.

### 11.4 Health/status model, and what a UI can read

Flink has **no per-connector status model.** There is job status (`RUNNING`/`FAILING`/`FINISHED`/…) and
task status, both at the *execution graph* level. A connector cannot report `degraded` or a last-error
string; a failing connector throws and the job restarts. `isSnapshotting`/`isStreamReading` are the only
phase signals and they are CDC-specific gauges, not framework concepts.

A UI reads: the REST API (job/vertex/checkpoint/savepoint endpoints; I fetched
`docs/content/docs/ops/rest_api.md` but it is a 4 KB include-stub that generates from OpenAPI, so I did
**not** verify individual endpoint paths — listed in `unverified`), plus metrics via reporters
(Prometheus, JMX, etc.).

**Gap for canal:** the state machine canal's design rules require (`healthy → degraded → paused →
terminal` with a last-error surface) does not exist in Flink and must be invented. Kafka Connect's
`(state, workerId, generation, trace)` — with `trace` carrying the stack — is the better prior art
there. Flink's contribution is *phase* (snapshot/stream) and *progress*, which Connect lacks. **canal
needs both axes: `status` (health) and `phase` (what work is happening), and they are orthogonal.**

---

## 12. Deployment

### 12.1 Modes

From `docs/content/docs/deployment/overview.md` and `docs/deployment/*`, Flink has:

- **Session mode** — long-lived cluster, many jobs share TaskManagers.
- **Application mode** — cluster per application, `main()` runs on the JobManager.
- **Per-job mode** (deprecated) — cluster per job.
- Resource providers: Standalone, Kubernetes (native + operator), YARN.
- **MiniCluster** — the in-JVM cluster used by tests and `LocalStreamEnvironment`.

MiniCluster is the closest analogue to canal's "standalone single-binary dev mode", and the important
property is that **the connector API is identical** in MiniCluster and on a 500-node YARN cluster.
Same as Kafka Connect's standalone/distributed split: *the plugin API must not know which mode it is
in.* Two independent systems, same conclusion; canal should treat it as settled.

The `Boundedness` + execution-mode interaction (`docs/dev/datastream/execution_mode.md`) is the other
relevant axis: a `BOUNDED` source lets Flink choose `BATCH` runtime mode (blocking shuffles, no
checkpoints, sorted keyed input) versus `STREAMING`. **One job definition, two runtimes, selected by the
source's declared boundedness.** For canal: `Boundedness` on the source should likewise be what selects
"run as a batch job that terminates" vs "run forever", not a separate pipeline type.

### 12.2 Work assignment — the two-level split, done better than Kafka Connect

```
  SplitEnumerator (JobManager, 1 instance, single coordinator thread)
        │  assignSplits(SplitsAssignment{subtaskId -> [splits]})
        │  signalNoMoreSplits(subtaskId)
        ▼
  SourceReader × parallelism (TaskManagers)
        │  sendSplitRequest()      ← pull-based work request
        │  snapshotState() -> List<SplitT>
        ▼
  on failure: addSplitsBack(splits, subtaskId)
```

Compared with Kafka Connect's `taskConfigs(int maxTasks)` (§Deployment in the Connect dossier), the
FLIP-27 model is strictly more capable on four axes:

| | Kafka Connect | FLIP-27 |
|---|---|---|
| When is work split? | once, at connector start | **continuously, by a live enumerator** |
| Can new work appear? | only via `requestTaskReconfiguration()` → task restart | `assignSplits` at any time, no restart |
| Work request direction | push (framework assigns task configs) | **pull** (`sendSplitRequest()`) → natural load balancing |
| Failure handling | task restarts, re-reads offsets | `addSplitsBack` returns the *exact* unfinished splits |
| Can parallelism change per phase? | no | yes, via `signalNoMoreSplits` per subtask |

**The pull model is the key difference.** A reader asks for work when it has capacity; the enumerator
never needs a load model. `handleSplitRequest(int subtaskId, @Nullable String requesterHostname)` even
supports locality: *"Optional, the hostname where the requesting task is running. This can be used to
make split assignments locality-aware."*

`ReaderInfo` (`src_ReaderInfo.java`) carries `(subtaskId, location)`, and
`SplitEnumeratorContext.registeredReaders()` is the enumerator's view of the worker fleet.

**For canal this maps almost unchanged onto a work-queue model**: a single enumerator goroutine (or, in
enterprise mode, a single leader-elected enumerator) owning the split set, workers pulling splits,
splits returning to the queue on worker loss, and the whole assignment state in one durable checkpoint.
No rebalance protocol, no assignor, no `scheduled.rebalance.max.delay.ms`. **Pull-based assignment
eliminates the entire class of problem KIP-415 exists to fix.**

### 12.3 Rebalancing / rescaling

Flink does not "rebalance" a running job in the Connect sense; it **rescales by restart-from-snapshot**
(or reactive/adaptive scheduling, which is a restart under the hood). The mechanism that makes this
safe is the state-redistribution policy of §3.4:

- per-split reader state — free to redistribute, splits go back to the enumerator and out again;
- operator list state — round-robin over independent items;
- union list state — broadcast the union;
- keyed state — by key group.

`SplitEnumeratorContext.currentParallelism()` carries the warning:

> *"Note that due to auto-scaling, the parallelism may change over time. Therefore the SplitEnumerator
> should not cache the return value of this method, but always invoke this method to get the latest
> parallelism."*

And speculative execution adds an attempt dimension (`registeredReadersOfAttempts()`,
`sendEventToSourceReader(subtaskId, attemptNumber, event)`) — with `registeredReaders()`'s javadoc
noting: *"if a subtask has multiple concurrent attempts, the map will contain the earliest attempt of
that subtask. This is for compatibility purpose."* **Retrofitting an attempt dimension onto an
interface keyed only by subtask id cost two `default`-throwing methods and a compatibility wart.**
If canal ever wants at-most-one-writer semantics or speculative retries, **the worker identity must be
`(workerID, epoch)` from day one**, never a bare integer.

### 12.4 Coordination and state store

- **Coordinator:** `CheckpointCoordinator` on the JobManager triggers checkpoints, tracks
  acknowledgements, and declares completion. `SourceCoordinator` hosts the `SplitEnumerator` on a
  single thread.
- **State store:** pluggable state backends (`HashMapStateBackend`, `EmbeddedRocksDBStateBackend`) plus
  a **checkpoint storage** (`JobManagerCheckpointStorage`, `FileSystemCheckpointStorage`). Checkpoint
  *storage* and state *backend* are separate axes — a distinction Flink had to untangle over several
  releases, and one canal should get right first time: **where state lives while running** vs **where
  snapshots are durably written** are different questions.
- **HA:** JobManager HA (ZooKeeper or Kubernetes ConfigMap-based) stores the pointer to the latest
  completed checkpoint. The *checkpoint* is in DFS; only the *pointer* needs a consensus store.

**That last split is the deployment lesson for canal's dev-vs-enterprise story.** The enterprise mode
needs a consensus store for exactly two things — *who is the leader/enumerator* and *what is the latest
committed checkpoint pointer* — and everything else can live in ordinary durable storage. Dev mode
replaces both with a local file. That is a much smaller coordination surface than Kafka Connect's
(config topic + status topic + offsets topic + group protocol + session keys + fencing records).

`CheckpointType`/`SnapshotType` also expose a nice detail about incremental checkpoints:

```java
    public static final CheckpointType CHECKPOINT =
            new CheckpointType("Checkpoint", SharingFilesStrategy.FORWARD_BACKWARD);
    public static final CheckpointType FULL_CHECKPOINT =
            new CheckpointType("Full Checkpoint", SharingFilesStrategy.FORWARD);
```

`SharingFilesStrategy` — whether a snapshot's files may be shared forward, backward, or not at all — is
the mechanism behind incremental checkpoints and "self-contained and relocatable" savepoints. If canal
ever does incremental checkpoints, **file-sharing direction must be an explicit property of the
snapshot kind**, or "delete old checkpoints" becomes unsafe.

---

## 13. What they got right / what they got wrong

### 13.1 What they got right

- **Work discovery separated from work reading.** FLIP-27's own motivation:
  *"The logic for 'work discovery' (splits, partitions, etc) and actually 'reading' the data is
  intermingled in the SourceFunction interface and in the DataStream API, leading to complex
  implementations like the Kafka and Kinesis source."* The payoff, also from the FLIP:
  *"This separation between enumerator and reader allows mixing and matching different enumeration
  strategies with split readers. For example, the current Kafka connector has different strategies for
  partition discovery that are intermingled with the rest of the code. With the new interfaces in
  place, we would only need one split reader implementation and there could be several split
  enumerators for the different partition discovery strategies."*

- **Splits as explicit, first-class, serialisable units of work.** FLIP-27 motivation:
  *"Partitions/shards/splits are not explicit in the interface. This makes it hard to implement certain
  functionalities in a source-independent way, for example event-time alignment, per-partition
  watermarks, dynamic split assignment, work stealing."* Making the split explicit bought all four.

- **The reader's checkpoint *is* its split list** (`List<SplitT> snapshotState(long)`). One concept
  instead of "assignment + offsets", so resume, rebalance and snapshot-progress are the same mechanism.

- **Batch/stream unification via `Boundedness`.** FLIP-27: *"One currently implements different sources
  for batch and streaming execution"* — and *"This allows users to write programs working in both batch
  and stream execution mode."* One source, two runtimes, chosen from a declared property.

- **`InputStatus{MORE_AVAILABLE, NOTHING_AVAILABLE, END_OF_INPUT}` + `isAvailable()` future.**
  A non-blocking pull protocol with an explicit terminal state and an explicit readiness signal, and a
  precisely specified contract for the future (*"all futures previously returned by this method must
  eventually complete"*, false positives allowed, always-complete forbidden).

- **Bounded hand-off queue with a default capacity of 2.** `SourceReaderBase`'s fetcher-thread /
  task-thread split confines blocking I/O to one side and bounds memory by construction.

- **The Checkpoint Subsuming Contract, and the "ready set / pending set" recipe.** The complete answer
  to committing external side effects against an unreliable confirm signal — and it is independent of
  distributed dataflow.

- **Two-phase sink commit with serialisable committables.**
  `flush → prepareCommit → snapshotState → [durable] → commit`, with the ordering documented on the
  interface, and `SimpleVersionedSerializer<CommittableT>` forcing committables to be bytes.

- **`Committer.CommitRequest` as a six-outcome per-item result.** Including the partial-success case
  (`updateAndRetryLater`), the already-done case (`signalAlreadyCommitted`), and `getNumberOfRetries()`.

- **`SimpleVersionedSerializer`.** Three methods, `(version, bytes)` in/out, connector-owned encoding,
  framework-owned storage. Complete upgrade story in forty lines.

- **Four-valued serialiser compatibility resolution**
  (`COMPATIBLE_AS_IS` / `COMPATIBLE_AFTER_MIGRATION` / `COMPATIBLE_WITH_RECONFIGURED_SERIALIZER` /
  `INCOMPATIBLE`) rather than a boolean — and the reconfigure case is genuinely useful.

- **`WithCompatibleState.getCompatibleWriterStateNames()`.** A connector declares which *other*
  connectors' persisted state it can adopt. Turns a rewrite migration into a declaration.

- **The mixin design rule**, stated explicitly in FLIP-372: every new feature is a `Supports<Feature>`
  interface; *"No redefining interface methods during interface inheritance — it would prevent future
  deprecation"*; *"Minimal inheritance extension."*

- **A managed single-threaded model for the enumerator** (`callAsync`, `runInCoordinatorThread`) so
  connector authors never write locks: *"Instead of using lock for thread safety, this API allows to run
  such externally triggered action in the coordinator thread."*

- **Pull-based split assignment** (`sendSplitRequest()`), which removes the need for a load model and
  the entire rebalance-storm problem class.

- **`addSplitsBack(splits, subtaskId)`** — failure returns the *exact* unfinished work, not "restart and
  re-read offsets".

- **Per-role typed metric groups handed in via context**, with the expected metrics documented on the
  interface, and **connector-supplied `pendingRecords`/`pendingBytes`** giving a source-side lag metric
  that an opaque-offset framework cannot have.

- **Flink CDC's incremental snapshot algorithm** (§4): chunked, parallel, lock-free, chunk-granularity
  checkpointing, resumable mid-split *and* mid-splitting, with a provably-terminating watermark filter
  for the handoff. This is the best snapshot-to-stream design in the industry and it was built with
  **zero changes to Flink core**.

- **`min(highWatermark)` as the stream start**, plus the self-retiring per-chunk filter. No gap, no
  duplicates, zero steady-state cost.

- **The five-mode schema-drift policy** (`exception|evolve|try_evolve|lenient|ignore`) with `lenient`
  (never-destructive) as the default and `drop`/`truncate` opt-in. The only complete shipped answer to
  drift in any surveyed system.

- **A lossy-by-design control protocol with a 30s idempotent reconciliation sweep**
  (`syncWithReaders` → `FinishedSnapshotSplitsRequestEvent`), because *"it may missed some notification
  event."*

- **CDC's `AssignerStatus` FSM with illegal transitions throwing by construction**, an integer wire code
  for persistence, the transition diagram in the javadoc, and intent-named predicate helpers.

- **`splitFinishedCheckpointIds: Map<String, Long>`** — recording *when* each split's completion became
  durable, not merely that it completed.

- **`ChunkSplitterState`** — the splitter's own cursor is checkpointed, with `ChunkBoundType
  {START, MIDDLE, END}` representing ±∞ as data.

- **Honesty in comments where the guarantee degrades.**
  *"Note that this behaviour downgrades the delivery guarantee to at-least-once"* (skip-backfill), and
  *"Data change events between (high_watermark, end_watermark) are backfill data, which maybe duplicate
  of snapshot data. Thus, only the intersection of both is exactly-once."* Also
  *"Metric `currentEmitEventTimeLag` will not be updated in the source operator in this case"* rather
  than reporting a garbage lag during snapshot.

- **`setIsProcessingBacklog(boolean)`** — a first-class "I am catching up, optimise throughput over
  latency" signal from the source to the runtime.

- **Three distinct split counters maintained as id-sets** (`remaining`/`processed`/`finished`) so
  progress cannot double-count and *can correctly go backwards* (`reprocessSplit`).

- **Recovery is instrumented in eight phases** (`InitializeStateDurationMs`, `DownloadStateDurationMs`,
  `GateRestoreDurationMs`, …). If restart-and-resume is a feature, restart time is a metric.

### 13.2 What they got wrong — concrete, documented pain

**Three incompatible Sink APIs in eight minor releases, each rewrite driven by an interface that could
not evolve.**

- Sink V1 (FLIP-143, 1.12). FLIP-191's own retrospective: *"While developing the unified Sink API **it
  was already noted that the unified Sink API might not be flexible enough to support all scenarios from
  the beginning**."*
- Sink V2 (FLIP-191, 1.15): *"we learned from our users that the abstraction of having a writer and a
  committer to implement a two-phase commit is very helpful **but the `GlobalCommitter` in its current
  design cannot fulfill all necessary duties**. Unfortunately, **there is not an easy way to
  replace/remove the `GlobalCommitter` from the existing interfaces without breaking already built sinks
  due to the typed parameters of the interface.** Therefore we propose Sink V2 that subsumes the current
  Sink interface."* — i.e. the fix for a bad generic parameter was **a whole new package**
  (`api.connector.sink2`).
- Sink V2-mixin (FLIP-372, 1.19): *"**During the implementation of FLIP-371 we broke the backward
  compatibility of the Sink API.** See the discussion on FLINK-25857"*, and *"The general consensus was
  to reject the original approach (minimal changes to the Sink API), and move forward to **rewrite the
  Sink API using mixin interfaces**."* The root cause named explicitly: *"In the case of
  `PrecommittingSinkWriter` which was an inner class of `TwoPhaseCommittingSink` this prevented the
  evolution of the `TwoPhaseCommittingSink`, **because of the generic types of the classes were tightly
  coupled.**"*

  **Lesson, stated as a rule for canal: never put a type parameter on an interface that a future feature
  might need to vary, and never nest an interface inside another interface.** Both mistakes are
  permanent once published.

**A `@Deprecated` abstract method that you must implement but must not `@Override`.** From `Sink.java`,
verbatim:

> *"@deprecated Please implement `#createWriter(WriterInitContext)`. For backward compatibility reasons
> — to keep `Sink` a functional interface — Flink did not provide a default implementation. New `Sink`
> implementations should implement this method, but **it will not be used**, and it will be removed in
> 1.20.0 release. **Do not use `@Override` annotation when implementing this method, to prevent
> compilation errors when migrating to 1.20.x release.**"*

An interface whose javadoc instructs you to omit `@Override` to survive a future release. That is a
migration scar visible in the type signature. (And the promise is broken: the method is still present,
still abstract, in `release-1.20`.)

**`InitContext` is `@Internal` but is the supertype of two `@Public` interfaces.** `WriterInitContext`
and `CommitterInitContext` are `@Public` and extend an `@Internal` interface — so half of a public
interface's surface is nominally not public API. Plus five `@Deprecated` methods on it
(`getSubtaskId`, `getNumberOfParallelSubtasks`, `getAttemptNumber`, `getJobId`, …) all defaulting through
`getTaskInfo()`/`getJobInfo()` per FLIP-382, i.e. **a third metadata-access refactor**.

**Five `default`-throwing `UnsupportedOperationException` methods on `@Public` interfaces**, each a
capability that should have been a mixin:

| Method | Interface |
|---|---|
| `sendEventToSourceReader(int, int, SourceEvent)` | `SplitEnumeratorContext` |
| `registeredReadersOfAttempts()` | `SplitEnumeratorContext` |
| `setIsProcessingBacklog(boolean)` | `SplitEnumeratorContext` |
| `currentParallelism()` | `SourceReaderContext` |
| `pauseOrResumeSplits(...)` | `SourceReader` **and** `SplitReader` |

The `pauseOrResumeSplits` message is the most telling artifact in the codebase — a
runtime exception whose text is a design apology plus an escape-hatch config flag that is itself
announced as doomed:

> *"This source reader does not support pausing or resuming splits which can lead to unaligned splits.
> Unaligned splits are splits where the output watermarks of the splits have diverged more than the
> allowed limit. **It is highly discouraged to use unaligned source splits, as this leads to
> unpredictable watermark alignment** if there is more than a single split per reader. It is recommended
> to implement pausing splits for this source. **At your own risk**, you can allow unaligned source
> splits by setting the configuration parameter
> `pipeline.watermark-alignment.allow-unaligned-source-splits` to true. **Beware that this configuration
> parameter will be dropped in a future Flink release.**"*

Note also that the default *"will be removed in future releases"* — so a source that does not implement
it will stop compiling-by-inheritance later. **A `default` that throws is a breaking change on a
timer.**

**Retrofitting speculative execution onto subtask-id-keyed interfaces.** `registeredReaders()` javadoc:
*"Note that if a subtask has multiple concurrent attempts, the map will contain the earliest attempt of
that subtask. **This is for compatibility purpose. It's recommended to use `registeredReadersOfAttempts()`
instead.**"* And `sendEventToSourceReader(subtaskId, attemptNumber, event)`: *"The `SplitEnumerator` must
invoke this method instead of `sendEventToSourceReader(int, SourceEvent)` if it is used in cases that a
subtask can have multiple concurrent execution attempts, e.g. if speculative execution is enabled.
**Otherwise an error will be thrown**."* — a correctness requirement that can only be discovered at
runtime.

**The three sink topology mixins leak `DataStream` into the connector SPI**, are `@Experimental` years
on, and delegate invariants the framework cannot verify: *"If committables are combined or split in any
way, the summary needs to be adjusted"*; *"so that number needs to correctly reflect the number of
distinct subtask ids."* No out-of-process implementation is possible.

**`signalFailedWithKnownReason` is documented silent data loss.**

> *"The commit failed for known reason and should not be retried. **Currently calling this method only
> logs the error, discards the comittable and continues.** In the future the behaviour might be
> configurable."*

A published `@Public` API where one of six documented outcomes is "your data is dropped, we log it" —
with a note that the behaviour is provisional, and an upstream typo (`comittable`) in the same sentence.
Compare canal's design rule **R4**: this is exactly "a stage returning success for something that is
not durable", enshrined in an interface.

**`TwoPhaseCommitSinkFunction` states an unavoidable data-loss path in javadoc** — and it is honest, but
it is still the sharp edge every 2PC sink inherits:

> `recoverAndCommit`: *"User implementation must ensure that this call will eventually succeed. If it
> fails, Flink application will be restarted and it will be invoked again. **If it does not succeed
> eventually, a data loss will occur.**"*

plus `ignoreFailuresAfterTransactionTimeout`, which converts that into a *configured* loss. Unavoidable
in principle; the wrong thing would be to hide it.

**Unaligned checkpoints: a long list of documented limitations**, all from
`docs/ops/state/checkpointing_under_backpressure.md`:

- *"Flink currently does not support concurrent unaligned checkpoints… savepoints can also not happen
  concurrently to unaligned checkpoints, so they will take slightly longer."*
- **Watermark semantics change:** *"Unaligned checkpoints break with an implicit guarantee in respect to
  watermarks during recovery… **Flink generates watermarks after it restores in-flight data**. If your
  pipeline uses an operator that applies the latest watermark on each record will produce **different
  results** than for aligned checkpoints."* Workaround: *"store the watermark in the operator state… per
  key group in a union state to support rescaling."*
- **Long-running record processing defeats it:** *"Flink can not interrupt processing of a single input
  record, and unaligned checkpoints have to wait for the currently processed record to be fully
  processed… back pressure can block unaligned checkpoints until all the network buffers required to
  process the single input record are available."*
- **Whole classes of edge are excluded:** *"There are types of connections with properties that are
  impossible to keep with channel data stored in checkpoints… unaligned checkpoints are disabled for such
  connections."* Pointwise (*"For the forward channels, we lack the key context entirely. No record in
  the forward channel has any key group assigned; it's also impossible to calculate it"*) and broadcast
  (*"it might happen that an operator will get the state with changes applied for a record that it will
  soon consume from its checkpointed channels"*).
- **A documented data-loss recovery lever:** `execution.checkpointing.recover-without-channel-state.checkpoint-id`,
  introduced with *"Actions described below are a last resort as **they will lead to data loss**… Do not
  set this property, unless a corruption inside the persisted in-flight data has lead to an otherwise
  unrecoverable situation."*

**Checkpoints and savepoints are not interchangeable, and the matrix is genuinely hostile.**
From `docs/ops/state/checkpoints_vs_savepoints.md` (✓ = supported, x = not, ! = *"currently work, Flink
doesn't officially guarantee support for them, so there is a certain level of risk"*):

| Operation | Canonical Savepoint | Native Savepoint | Aligned Checkpoint | Unaligned Checkpoint |
|---|---|---|---|---|
| State backend change | ✓ | x | x | x |
| State Processor API (writing) | ✓ | x | x | x |
| State Processor API (reading) | ✓ | ! | ! | x |
| Self-contained and relocatable | ✓ | ✓ | x | x |
| **Schema evolution** | ✓ | ! | ! | ! |
| Arbitrary job upgrade | ✓ | ✓ | ✓ | **x** |
| Flink minor version upgrade | ✓ | ✓ | ✓ | **x** |
| Rescaling | ✓ | ✓ | ✓ | ✓ |

So: **you cannot upgrade Flink across a minor version from an unaligned checkpoint**, and schema
evolution is only *guaranteed* from a canonical savepoint. The distinction is explained as
*"analogous to how backups are different from recovery logs in traditional database systems"* — a good
analogy that nonetheless means operators must know which artifact they hold before attempting an
upgrade. **Checkpoints being non-relocatable is the sharpest one** (`x` for "self-contained and
relocatable"): you cannot move a retained checkpoint directory.

**State schema evolution is far narrower than it sounds.** *"Currently, schema evolution is supported
only for POJO and Avro types."* For POJOs: *"Declared fields types cannot change"*, *"Class name of the
POJO type cannot change, including the namespace of the class"*, and it requires Flink > 1.8.0.
FLINK-10896 (extending to composite types) is cited as still open.

**No connector-level error classification and no connector-level health.** Every source-side exception
fails the task/job (*"Exceptions thrown from this method cause JobManager failure/recovery"*). There is
no retriable marker, no `degraded` state, no last-error surface, no DLQ, and no per-record error routing
anywhere in the connector SPI. `CheckpointFailureReason` is a rich internal enum that connectors cannot
see. Kafka Connect — for all its faults — has `RetriableException`, `errors.tolerance`, a DLQ and
`ErrantRecordReporter`; Flink has none of it.

**No `Context`, no deadlines, no cancellation.** `wakeUp()` plus `AutoCloseable` plus hardcoded
timeouts (`READER_CLOSE_TIMEOUT_SECONDS = 30L`; `source.reader.close.timeout` default 30000 ms), with
the reentrancy burden on the connector: *"this method should be reentrant, meaning that the next fetch
call should just resume from where the last fetch call was waken up or interrupted."*

**No config model for DataStream connectors at all.** No option declaration, no validation hook, no
presentation metadata, no HTTP surface. A UI cannot discover anything about a FLIP-27 source. The
Table-layer `Factory` fixes discovery but not validation or presentation — *"It is the responsibility of
each factory to perform validation before returning an instance"* — and encodes connector version as a
**string suffix** (`elasticsearch-7`).

**Deep buffering had to be undone by two major features.** FLIP-76 unaligned checkpoints and buffer
debloating (`taskmanager.network.memory.buffer-debloat.enabled`) both exist because
*"when a Flink job is running under heavy backpressure, the dominant factor in the end-to-end time of a
checkpoint can be the time to propagate checkpoint barriers"*. An architectural cost paid for years.

**Flink CDC: `shouldEmit` was O(chunks) per record.** The original path is a linear scan over every
finished chunk of the table for every change record:

```java
                    for (FinishedSnapshotSplitInfo splitInfo : tableSplits) {
                        if (taskContext.isRecordBetween(sourceRecord, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                                && position.isAfter(splitInfo.getHighWatermark())) {
                            return true;
                        }
                    }
```

with a later `supportsSplitKeyOptimization` branch adding `SplitKeyUtils.findSplitByKeyBinary` +
`SplitKeyUtils::sortFinishedSplitInfos`. A 1 M-row table at 8192 rows/chunk is ~122 chunks scanned per
record. **The retrofit is opt-in and dialect-gated** (`taskContext.supportsSplitKeyOptimization()`), so
some connectors still take the linear path. Design the index in from the start: sorted ranges +
binary search, or a range tree.

**Flink CDC: the handoff metadata outgrew its transport.**

```java
        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
```

which required inventing four extra events (`StreamSplitMetaRequestEvent`, `StreamSplitMetaEvent`,
`LatestFinishedSplitsNumberRequestEvent`, `LatestFinishedSplitsNumberEvent`), a `totalFinishedSplitSize`
field distinct from `finishedSnapshotSplitInfos.size()`, an `isCompletedSplit()` predicate comparing
them, plus a truncation path with a warning log:

```java
        if (totalFinishedSplitSizeOfReader > totalFinishedSplitSizeOfEnumerator) {
            LOG.warn("Total finished split size of subtask {} is {}, while total finished split size of "
                     + "enumerator is only {}. Try to truncate it", ...);
```

**canal should assume from day one that the finished-chunk set is large and must be paged or stored
by reference**, not shipped whole in an assignment message.

**Flink CDC: a concurrency bug in the checkpoint path**, still visible as a defensive-copy comment:

```java
        // FLINK-38061: make defensive copy to avoid potential concurrent modification of the
        // collections.
        this.alreadyProcessedTables = new ArrayList<>(alreadyProcessedTables);
        this.remainingSplits = new ArrayList<>(remainingSplits);
        this.assignedSplits = new LinkedHashMap<>(assignedSplits);
```

and its sibling in `HybridSplitAssigner.open()`:

```java
        // Init enumerator metrics before opening the snapshot assigner. Opening the assigner
        // starts an asynchronous splitting thread that mutates remainingSplits, so metrics must be
        // initialized first to avoid a ConcurrentModificationException while iterating the splits.
```

**The enumerator was supposed to be single-threaded, and an async splitter thread broke that.** Two
comments documenting the same class of bug. In Go: the splitter is a goroutine that *sends* completed
chunks to the enumerator over a channel and never touches enumerator state.

**Flink CDC: hand-rolled sealed types and stringly-typed identity.**

```java
    public final boolean isSnapshotSplit() {
        return getClass() == SnapshotSplit.class || getClass() == SchemalessSnapshotSplit.class;
    }
```
and split identity encoded as a parseable string:
```java
    public static String generateSplitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }
    public static TableId extractTableId(String splitId) {
        return TableId.parse(splitId.substring(0, splitId.lastIndexOf(":")));
    }
```
with a javadoc warning attached to the `@Internal` constructor: *"This constructor should not be used
directly… **If this constructor must be invoked, please use the same format for the splitId as
`generateSplitId(TableId, int)`. Or else the parsing method will fail.**"* And downstream, the metrics
layer *parses split ids* (`SnapshotSplit.extractChunkId(newSplitId)`) to maintain its counters. **Split
id is `FLIP-27`'s only mandated field, so everything got smuggled into it.** canal's split identity
should be a struct, with a separate opaque `ID string` only for logging.

**Flink CDC: non-key chunk columns produce documented data inconsistency.** From `mysql-cdc.md`:

> *"**Warning:** Using a non-primary key column as a chunk key may lead to data inconsistencies."*
> and the worked example: *"An update operation changes `pid` from `2` to `4` for `id=0` while both
> splits are being read. This update occurs between the low and high watermark of both splits."*

Also *"If the actual values for the primary key are not uniformly distributed across its range, this may
lead to unbalanced tasks when incremental snapshot read"*, and the OOM mitigation flag
`scan.incremental.snapshot.unbounded-chunk-first.enabled` — *"This might help reduce the risk of the
TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded
chunk."* **The unbounded first/last chunks are unbounded in size, and that is an OOM waiting to happen**
unless the reader streams rather than buffers.

**Flink CDC: the stream phase is single-parallelism, forever.** *"After all snapshot chunks finished, the
source will continue to read binlog in a single task."* Snapshot scales; stream does not. And **the whole
snapshot phase must complete before the stream starts**: *"binlog reader will start to read data until
there is a complete checkpoint after snapshot chunks finished."* A 10 TB snapshot means the stream is
blocked for its full duration, and if upstream retention is shorter than the snapshot, the pipeline is
unrecoverable. **canal should support interleaved snapshot-and-stream** (Debezium's incremental snapshot
does; Flink CDC's does not) or at minimum surface the retention risk as a pre-flight check.

**Package typo shipped in a public path.** `org.apache.flink.cdc.connectors.base.source.meta.wartermark`
(`WatermarkKind`, `WatermarkEvent`) — unfixable without breaking `SimpleVersionedSerializer` class-name
assumptions and user imports. A reminder that **package and type names in a plugin API are a wire
contract.**

**The upstream doc has a blank enum case.** `understand-flink-cdc-api.md` lists four operation types and
leaves the fourth as *"Replace:"* with no definition. The most important table in the CDC record model
is incomplete in the official docs.

### 13.3 The explicit verdict: what to adopt, what is distributed-dataflow-only

**Adopt for a single-process-first Go tool (L2 + L3):**

| Idea | Why it survives outside a dataflow engine |
|---|---|
| `SplitEnumerator` / `SourceReader` / `SourceSplit` triple | Pure work-decomposition; the "coordinator" can be a goroutine. |
| Reader checkpoint = its split list | No barriers needed; splits are self-describing positions. |
| `addSplitsBack` on worker loss | A work queue property, not a dataflow property. |
| Pull-based `sendSplitRequest()` | Load balancing without a load model; trivially a channel receive. |
| `SourceEvent` bidirectional control channel | Two channels. Removes the need for signalling tables. |
| Idempotent 30s reconciliation sweep | Correct precisely *because* messages are assumed lossy. |
| `InputStatus` + `isAvailable()` | `(status, error)` + a channel. Gives real termination semantics. |
| Bounded hand-off queue, capacity ~2 batches | A buffered channel. Bounds memory, bounds recovery. |
| `flush → prepareCommit → snapshot → [durable] → commit` | Needs a monotonic id and a durable store, nothing more. |
| Checkpoint Subsuming Contract + ready/pending sets | Solves "unreliable confirm signal", which exists in one process too (crash between fsync and commit). |
| `CommitRequest` six-outcome per-item result | Pure API design. |
| Committables/splits/state as `(version, []byte)` | Enables upgrades *and* the future gRPC boundary. |
| `SimpleVersionedSerializer` | Copy verbatim. |
| Four-valued state compatibility verdict | Copy the *shape*. |
| `WithCompatibleState` compatible-state-names | One method, enables connector rewrites. |
| `Supports<Feature>` mixins + context-injected capabilities | The whole extensibility story. |
| `Boundedness` selecting batch vs streaming runtime | One source, two runtimes, declared property. |
| Flink CDC chunk/watermark/backfill algorithm (all 8 steps) | Nothing in it requires distribution — only comparable positions and a splittable key space. |
| `ChunkSplitterState` (resumable splitting) | Copy verbatim. |
| `min(highWatermark)` stream start + self-retiring filter | Copy verbatim. |
| `AssignerStatus`-style FSM with throwing illegal transitions + integer wire codes | Copy the pattern for canal's connector state machine. |
| Snapshot-progress metrics (phase gauges, remaining/processed/finished as id-sets, per-object groups) | This *is* canal's frontend. |
| Connector-supplied `pendingRecords`/`pendingBytes` | The only way to get generic source lag. |
| Five-mode schema-drift policy + per-event-type include/exclude | Copy essentially verbatim. |
| Schema-as-ordered-in-band-event with a "schema before data" ordering rule | Fits canal's canonical envelope (R2). |
| Heartbeat records that advance position without emitting data | Prevents retention-window loss on idle streams. |
| `setIsProcessingBacklog` | Snapshot vs stream latency/throughput trade-off, declared. |
| Transaction timeout on committables, with loud expiry | Honest treatment of an unavoidable failure. |
| Split/split-state pairing (immutable assignment + mutable cursor) | Cleaner than per-record offset maps. |
| Restored-state reconciliation as a pure function (`filterOutdatedSplitInfos`) | Testable, and required for correctness after config change. |

**Do NOT adopt (justified only by a distributed dataflow engine):**

| Idea | Why not |
|---|---|
| **Chandy-Lamport barriers in the data stream** | Exists to cut a *shuffled multi-operator graph* consistently without stopping. canal's pipeline is linear; one durable record containing `{position, committables, state}` gives the same guarantee. |
| **Barrier alignment** | Only meaningful with multiple inputs joining at a stateful operator. |
| **Unaligned checkpoints + in-flight buffer persistence** | Entirely a consequence of deep network buffering. Choose small bounded buffers and the problem — plus its long limitation list, its watermark-semantics divergence, and its data-loss recovery flag — never exists. |
| **`CheckpointBarrierHandler`, `AlignmentType`, `alignedCheckpointTimeout`, buffer debloating** | Machinery serving the above. |
| **Keyed state / key groups / key-group redistribution** | Only for stateful keyed transforms at scale. If canal ever needs dedupe state, scope it to a split. |
| **Operator list state vs union list state redistribution policies** | Needed because parallelism changes and opaque state must be re-sharded. canal should make *all* connector state split-scoped so the question never arises. |
| **Broadcast state** | Requires "all instances store identical elements" as an unverifiable user obligation. |
| **`TypeSerializerSnapshot` managed-state machinery** | Copy the four-valued *verdict*; do not build the framework. `SimpleVersionedSerializer` covers canal's needs with none of the POJO/Avro restrictions. |
| **Savepoint-vs-checkpoint duality (canonical/native formats, `SharingFilesStrategy`, relocatability matrix)** | Two artifact kinds and eight-column compatibility matrices are an operator tax. canal should have **one** snapshot format that is always self-contained and relocatable. |
| **`DataStream`-typed topology mixins** | Unshippable over a wire; express aggregation as `Aggregate(ctx, []WriterResult) ([]Committable, error)`. |
| **Speculative execution / attempt numbers** | But *do* make worker identity `(workerID, epoch)` from the start, so this is addable without a compatibility wart. |
| **Watermarks, event-time, `TimestampAssigner`, watermark alignment, per-split watermark generators, `pauseOrResumeSplits`** | Event-time processing is a stream-analytics concern. A data-movement tool needs *positions*, not watermarks. (Note: CDC's LOW/HIGH/END "watermarks" are **positions**, not event-time watermarks — different concept, same word. canal should call them **positions/bounds** and never reuse the word.) |
| **JobManager/TaskManager split, execution graph, session/application modes, slots** | canal's enterprise mode needs a leader-elected enumerator + stateless workers + a checkpoint-pointer store. That is all. |
| **Java SPI / classpath discovery / `factoryIdentifier()` string-suffix versioning** | canal's init-time registry with structured `(name, version)` is better. |

**The one-sentence summary:** *take Flink's L2 and L3 — the state/confirm contract, the split model, the
two-phase commit, and the CDC snapshot algorithm — and leave L1, the barrier protocol and everything
built to compensate for it, entirely alone.*

---

## 14. Steal this

- **Split the source into an enumerator and a reader**: the enumerator discovers and assigns work, the
  reader reads assigned work, and neither knows the other's algorithm.

- **Make the reader's checkpoint literally its list of splits** (`Snapshot(id) []Split`), so position,
  assignment and progress are one concept instead of three.

- **Return unfinished splits to the enumerator on worker loss** (`AddSplitsBack(splits, workerID)`)
  rather than restarting the worker and re-reading a stored offset.

- **Make split assignment pull-based** — the worker requests work when it has capacity — which gives load
  balancing without a load model and eliminates the entire rebalance-storm problem class.

- **Give the enumerator a single-goroutine managed threading model** (Flink's `callAsync` /
  `runInCoordinatorThread`) so connector authors never write a lock, and never let a splitter goroutine
  touch enumerator state — Flink CDC shipped two `ConcurrentModificationException` fixes for exactly
  that.

- **Add an empty `ControlEvent` interface as a bidirectional core↔connector channel**, so a running
  connector can be commanded without a restart — Kafka Connect's absence of this forced Debezium to
  invent a database signalling table.

- **Assume every control message can be lost and add an idempotent periodic reconciliation sweep**
  (Flink CDC re-requests unacked finished splits every 30 s) instead of relying on acknowledgements.

- **Return a three-valued read status** (`MoreAvailable` / `NothingAvailable` / `EndOfInput`) plus a
  readiness channel, so a bounded pipeline can actually declare itself finished.

- **Bound the fetcher→pipeline hand-off queue at ~2 batches**, keep blocking I/O strictly on the fetcher
  side, and treat "small buffers" as a correctness decision — deep buffering is what forced Flink to
  build unaligned checkpoints and buffer debloating.

- **Order the sink commit as `flush(endOfInput) → PrepareCommit() → Snapshot(id) → [durable] →
  Commit(committables)`**, with the pending committables persisted *inside* the checkpoint.

- **Implement the Checkpoint Subsuming Contract verbatim**: monotonic ids, a transient "ready set", a
  checkpointed `map[id][]Committable` "pending set", and on confirm commit everything up to and including
  that id — so a lost or skipped confirmation is repaired by the next one.

- **Make the commit result a per-item request object with named outcomes**
  (`Committable()`, `Retries()`, `RetryLater()`, `UpdateAndRetryLater(c)`, `AlreadyCommitted()`,
  `FailedTerminal(err)`), never a boolean and never one exception for a batch.

- **Give committables an expiry and fail loudly when one expires** — Flink's
  `transactionTimeout` + `ignoreFailuresAfterTransactionTimeout` + a warning ratio is the honest
  treatment; silent skipping is not.

- **Force everything that crosses a role boundary or hits disk to be `(version, []byte)` from a
  connector-supplied serialiser** — splits, enumerator state, committables, writer state, control
  events. This single rule buys both binary-upgrade compatibility and the future out-of-process
  connector.

- **Copy `SimpleVersionedSerializer` exactly**: `Version() int`, `Serialize(E) ([]byte, error)`,
  `Deserialize(version int, b []byte) (E, error)` — the deserialiser is handed the version it was
  written with.

- **Make compatibility verdicts four-valued** (`CompatibleAsIs`, `CompatibleAfterMigration`,
  `CompatibleWithReconfiguredSerializer`, `Incompatible`) rather than a boolean.

- **Let a connector declare which other connectors' persisted state it can adopt**
  (`CompatibleStateNames() []string`) so a future rewrite is a declaration, not an operator runbook.

- **Put every optional capability behind its own type-assertable interface** (`SupportsChunkedSnapshot`,
  `SupportsCommitter`, `SupportsWriterState`, `SupportsSchemaApply`, `SupportsPendingEstimate`) and never
  add a method to a required interface — Flink's five `default`-throwing `UnsupportedOperationException`
  methods and three Sink API rewrites are the price of doing otherwise.

- **Never put a type parameter on an interface a future feature might need to vary, and never nest an
  interface inside another interface** — FLIP-191 needed a whole new package because
  *"there is not an easy way to replace/remove the `GlobalCommitter` … due to the typed parameters"*, and
  FLIP-372 names inner-interface coupling as what *"prevented the evolution of the
  `TwoPhaseCommittingSink`"*.

- **Grow core capabilities through the context interface, not the connector interface** — the core
  implements the context and passes it in, so adding a capability never breaks a connector.

- **Implement Flink CDC's incremental snapshot algorithm as canal's generic snapshot engine**: chunk the
  key space with explicit `START/MIDDLE/END` bounds, per chunk record `LOW` → read → record `HIGH` →
  replay `[LOW, HIGH)` → upsert into the buffer → emit as inserts → `END`.

- **Model the per-chunk backfill as a bounded instance of the same stream-split type**, so one reader
  implementation serves both the per-chunk replay and the endless stream.

- **Checkpoint the chunk splitter's own cursor** (`{table, nextChunkStart, nextChunkID}`) so slicing a
  huge object resumes mid-way instead of restarting.

- **Encode a chunk's completion as data, not a flag** — `highWatermark != null` *is* "this chunk is
  done".

- **Start the incremental stream at `min(highWatermark)` across all chunks**, and filter each record by
  `key ∈ chunkRange && position > chunkHighWatermark` — no gap, no duplicates.

- **Make the snapshot-dedup filter self-retiring**: once the stream position passes `max(highWatermark)`
  for an object, drop the filter for that object forever, so steady-state cost is zero. A dedup filter
  must have a proof of termination and its state must live in the checkpoint.

- **Index the chunk-range filter from day one** (sorted ranges + binary search) — Flink CDC's original
  O(chunks)-per-record linear scan needed an opt-in, dialect-gated retrofit.

- **Assume the finished-chunk set is too large to ship in one message** and page it by reference from the
  start — Flink CDC had to add four events, a separate size field and a truncation path after the fact.

- **Only start the incremental stream after a complete checkpoint following snapshot completion**, so no
  stream record can precede a snapshot record for the same key.

- **Record *when* each unit of work's completion became durable**
  (`splitFinishedCheckpointIds map[string]int64`), not merely that it completed.

- **Reconcile restored checkpoint state against current discovery as a pure function**
  (`filterOutdatedSplitInfos(state, currentFilter)`), dropping state for objects that no longer match and
  adjusting the expected totals so completeness predicates stay correct.

- **Give the source a heartbeat concept**: a way to advance the durable position with no records
  emitted, so an idle stream's stored position never falls out of the upstream retention window.

- **Give the source a "processing backlog" flag** (`SetIsProcessingBacklog(bool)`) so the core can batch
  aggressively during a snapshot and optimise latency once caught up.

- **Model the connector/assigner state machine as an enum whose illegal transitions throw by
  construction**, persisted by explicit integer code (never `ordinal()` or `name()`), with the transition
  diagram in the doc comment and intent-named predicate helpers at call sites.

- **Make the pipeline type a strategy object selected by a startup mode**
  (`initial | snapshot-only | stream-only | from-position | from-timestamp`) with `Boundedness` derived
  from it — not a parallel hierarchy of pipeline classes.

- **Express the delivery guarantee as which interfaces the connector implements**, compute the
  end-to-end guarantee as the minimum across source, sink and mode, and refuse an impossible pipeline at
  submit time rather than at 3 a.m.

- **Make "chunked snapshot output is keyed upserts, not appends" an explicit capability requirement** —
  Flink CDC's exactly-once claim depends on the sink upserting by key, and canal must check that at
  submit time rather than discovering it in production.

- **Adopt the five-mode schema-drift policy wholesale** (`exception | evolve | try_evolve | lenient |
  ignore`) with never-destructive `lenient` as the default and `drop.*`/`truncate.*` opt-in, plus
  per-event-type `include.schema.changes` / `exclude.schema.changes` with prefix matching.

- **Carry schema as an ordered in-band event with a "schema before data" ordering rule**, tracked into
  checkpointed state whether or not it is emitted downstream, and only applied during the stream phase
  (a chunk's schema is pinned at its start).

- **Split the source into read + discovery facets and the sink into write + apply-schema facets**
  (Flink CDC's `EventSource`/`MetadataAccessor` and `EventSink`/`MetadataApplier`), and quiesce and flush
  in-flight data before applying a schema change downstream.

- **Hand each connector role a typed metric group through its context**, document the expected metrics on
  the interface itself with recommendation levels, and let the core own naming, tagging and export.

- **Let the connector supply `PendingRecords()`/`PendingBytes()`** so the core can compute source lag
  generically — Kafka Connect structurally cannot, because its offsets are opaque.

- **Build the frontend on split-level progress**: `isSnapshotting` / `isStreamReading` phase gauges,
  objects remaining/done, splits remaining/processed/finished maintained as **id-sets** (so progress
  cannot double-count and can correctly go backwards on reassignment), snapshot start/end timestamps, and
  a per-object metric subgroup — this gives a real progress bar and ETA for free.

- **Distinguish `processed` from `finished`** — emitted by a worker vs durably acknowledged by the
  coordinator — and only declare a phase complete when remaining is zero *and* the two are equal.

- **Never show a lag number during a snapshot.** Flink CDC deliberately omits the event-time lag metric
  for snapshot rows because *"Records in snapshot mode have a zero timestamp"*; show percent-complete
  instead.

- **Instrument recovery itself in phases** (state download, state restore, initialise, first-record) — if
  restart-and-resume is a headline feature, restart time is a metric, not an assumption.

- **Separate `status` (health: healthy/degraded/paused/terminal + last error) from `phase` (what work is
  happening: discovering/snapshotting/streaming/completed)** — Flink has phase but no health, Kafka
  Connect has health but no phase, and canal needs both axes.

- **Keep the connector API byte-identical between dev mode and enterprise mode**, swapping only the
  checkpoint store, the leader election and the metrics sink — Flink (MiniCluster vs YARN) and Kafka
  Connect (standalone vs distributed) independently converged on this.

- **Keep the enterprise-mode consensus surface to exactly two facts**: who owns the enumerator, and what
  is the latest committed checkpoint pointer. Everything else goes in ordinary durable storage.

- **Separate "where state lives while running" from "where snapshots are durably written"** — Flink
  needed several releases to untangle state backend from checkpoint storage.

- **Make worker identity `(workerID, epoch)` from the first commit** — Flink's retrofit of attempt
  numbers onto subtask-id-keyed interfaces cost two `default`-throwing methods and a permanent
  compatibility wart.

- **Make split identity a struct, and use the opaque string id only for logging** — FLIP-27 mandates only
  `splitId() string`, so Flink CDC ended up encoding `table:chunkID` into it, parsing it back out in
  three places, and warning implementors that a custom id will break the parser.

- **Ship exactly one snapshot format, always self-contained and relocatable** — Flink's
  canonical-savepoint / native-savepoint / aligned-checkpoint / unaligned-checkpoint matrix is an
  operator tax, and "aligned checkpoints are not relocatable" plus "you cannot upgrade Flink minor
  versions from an unaligned checkpoint" are the sharp edges.

- **Do not implement barrier-based distributed snapshots.** Write `{source positions, sink committables,
  connector state}` as one durable record under one monotonic id, and keep the *interface*
  barrier-shaped (`Snapshot(ctx, id)` on every stage) so a real barrier protocol could be inserted later
  without changing a single connector.

- **Never let a dataflow or transport type into a connector interface.** Flink's three sink topology
  mixins take and return `DataStream`, are `@Experimental` years later, and can never be implemented
  out-of-process; express the same capability as
  `Aggregate(ctx, []WriterResult) ([]Committable, error)` over serialisable values.

- **Never publish an outcome that silently drops data.** Flink's `signalFailedWithKnownReason` is
  documented as *"only logs the error, discards the comittable and continues"* — every terminal failure in
  canal must produce a dead-letter record and a visible status change.

- **Write the honest sentence in the code where a guarantee degrades.** Flink CDC's
  *"this behaviour downgrades the delivery guarantee to at-least-once"* and *"only the intersection of
  both is exactly-once"* are the model; a guarantee that is not written down next to the code that
  weakens it will be assumed by someone.

- **Treat package and type names in the plugin API as a wire contract** — `…source.meta.wartermark` has
  been unfixable in a public Flink CDC path for years.



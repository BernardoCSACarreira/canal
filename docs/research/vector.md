# Prior art: Vector (vectordotdev/vector, Rust)

> ## PROVENANCE WARNING — READ BEFORE TRUSTING ANY SIGNATURE IN THIS FILE
>
> **Every network tool available to this run (WebFetch, WebSearch, and Bash/curl) was hard-failing
> for the entire duration of the research window** with `claude-sonnet-5 is temporarily unavailable,
> so auto mode cannot determine the safety of <tool>`. Roughly a dozen attempts were made across all
> three tools, against `raw.githubusercontent.com`, `github.com` and `vector.dev`. Zero bytes of
> primary source were retrieved.
>
> This dossier is therefore written **from model recall, not from primary source**. It has NOT been
> checked against the repository.
>
> Treat it accordingly:
>
> - **Architectural claims** (what the pieces are, how they compose, why) are what recall is
>   actually good for. Confidence is genuinely high on most of these and they are the useful part.
> - **Literal signatures are RECONSTRUCTIONS.** Method names, argument order, generic parameters,
>   associated-type names and enum variants are plausible-but-unchecked. Every fenced block carries
>   a `RECALL: high|medium|low` tag. Do **not** copy a signature out of here into a canal design
>   document without opening the real file first.
> - Anything I could not pin down at all is stated as such inline and repeated in the
>   `unverified` list of the structured return.
>
> **Re-run this research task when the network classifier is back up.** The specific files worth
> fetching first are listed in the last section, "Verification worklist".

Vector is an observability data router: sources → transforms → sinks, one static Rust binary, config
in TOML/YAML/JSON. It is the closest existing thing to canal in *shape* (generic source/sink,
in-process plugin registry, single binary, pluggable codecs, per-sink buffering) while being
deliberately narrower in *domain* (a fixed telemetry event model, and — importantly — no general
checkpoint abstraction and no snapshot concept at all).

The three things Vector does better than almost anything else, and which are the reason it is on this
list, are:

1. **Buffers as a first-class, per-sink, configurable pipeline stage** with an explicit
   rejection/blocking policy (`when_full`). Directly answers canal's R6 and open decision 3.
2. **End-to-end acknowledgements via event finalizers** — a reference-counted, status-merging
   ack graph that lets a source learn that *its* records were durably written, without the source
   knowing anything about the sink. Directly answers canal's R4.
3. **Separation of `encode + build request + retry policy` from connector logic** via a
   tower-based service stack plus a `RequestBuilder` trait, and **adaptive request concurrency**.

And the two most instructive things it got *wrong* for canal's purposes:

1. **A fixed event enum** (`Log | Metric | Trace`). Adding a telemetry kind is a core edit touching
   every component — precisely the "zero core edits" property canal is optimising for.
2. **No checkpoint abstraction.** Every source that needs durable position invents its own. This is
   the single largest hole in Vector as prior art, and it is canal's core requirement.

---

## 1. Core interfaces

Vector has one config trait per component kind. The trait is on the *config object*, not on the
runtime object — `build()` is the factory. This split (deserializable config struct → built runtime
component) is the single most transplantable structural decision in the codebase.

### 1.1 `SourceConfig`

```rust
// RECALL: medium-high on shape, medium on exact names/arity.
// src/config/source.rs (approx.)
#[async_trait::async_trait]
pub trait SourceConfig: NamedComponent + core::fmt::Debug + Send + Sync {
    /// Build the running source. Returns a future that IS the source's main loop.
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source>;

    /// Declare what this source emits, per named output port.
    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput>;

    /// Exclusive OS/global resources this source needs (ports, files) — used for
    /// config-validation-time conflict detection between components.
    fn resources(&self) -> Vec<Resource> {
        Vec::new()
    }

    /// Whether this source can propagate end-to-end acknowledgements upstream.
    fn can_acknowledge(&self) -> bool;
}
```

The returned `Source` is, from recall, a boxed future rather than an object with `read()`:

```rust
// RECALL: medium-high.
pub type Source = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;
```

**This is the most important single observation in section 1.** Vector's source interface is *not*
`read() -> Batch`. It is "here is a channel, here is a shutdown signal, go run until you're done."
The source owns its own loop, its own batching, its own polling cadence, its own state. The core
does not pull.

Consequences, all of which canal must decide about deliberately:

- The core cannot implement generic retry, generic checkpointing, generic rate limiting, or generic
  progress reporting for sources, because it never sees a source's iteration boundary. Vector
  accordingly has none of those for sources.
- `Result<(), ()>` throws the error away. There is no error *type* crossing the boundary. Errors are
  reported out-of-band via internal telemetry events (`emit!`). The topology cannot distinguish
  "permanent config failure" from "clean shutdown" from "transient upstream outage". See §13.
- A push-model source (HTTP listener, syslog socket) and a pull-model source (file tail, Kafka
  consumer) fit the *same* interface with no adapter, because the interface is just "a task". That
  genuinely is an advantage of the async-task shape over a `read()` shape.

**For canal:** a `Run(ctx) error` shape is strictly more permissive than `Read() (Batch, error)`, but
it buys that permissiveness by making the core blind. Canal wants generic checkpointing, which needs
iteration boundaries. The likely right answer is the opposite of Vector's: a pull interface for
sources that can be pulled, and a `Run`-shaped escape hatch for genuinely push-native sources — but
with the push variant required to go through the same batch/ack plumbing.

### 1.2 `SourceContext`

Everything the core injects into a source. This is the dependency-injection seam.

```rust
// RECALL: medium. Field set has churned across releases; several of these were added later.
pub struct SourceContext {
    pub key: ComponentKey,
    pub globals: GlobalOptions,
    pub shutdown: ShutdownSignal,
    pub out: SourceSender,
    pub proxy: ProxyConfig,
    pub acknowledgements: bool,
    pub schema_definitions: HashMap<Option<String>, schema::Definition>,
    pub schema: schema::Options,
    pub enrichment_tables: enrichment::TableRegistry,
}
```

Worth noting what is in there:

- `shutdown: ShutdownSignal` — cancellation is an explicit injected object, not an ambient
  `context.Context` equivalent. It is awaitable and it hands back a token on drop so the coordinator
  can tell the difference between "source finished draining" and "source is still going". Canal's Go
  equivalent is `context.Context` plus an explicit "drained" signal; `ctx.Done()` alone is not
  enough, because you need to know when the source has *finished flushing*, not when it was told to
  stop.
- `out: SourceSender` — the source's only way to emit. Backpressure is expressed by this send
  blocking. See §8.
- `acknowledgements: bool` — the core tells the source whether end-to-end acks are active, resolved
  from global + per-source config. The source changes behaviour based on it (attach finalizers and
  wait, vs fire-and-forget).
- `proxy: ProxyConfig` — global egress proxy settings, injected rather than read from global state.
  Small thing, right instinct: cross-cutting operational config is injected into connectors.
- `key: ComponentKey` — the component's identity, injected. Every metric the source emits is tagged
  with it. Connectors do not invent their own identity. See §11.

### 1.3 `SinkConfig`

```rust
// RECALL: high on shape and on the (VectorSink, Healthcheck) tuple; medium on bounds.
// src/config/sink.rs (approx.)
#[async_trait::async_trait]
pub trait SinkConfig: DynClone + NamedComponent + core::fmt::Debug + Send + Sync {
    /// Build both the sink and its healthcheck in one call.
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)>;

    /// What this sink accepts — event kinds AND schema requirements.
    /// Used for compile-time-ish topology validation.
    fn input(&self) -> Input;

    fn resources(&self) -> Vec<Resource> {
        Vec::new()
    }

    /// Per-sink acknowledgement configuration.
    fn acknowledgements(&self) -> &AcknowledgementsConfig;
}

pub type Healthcheck = futures::future::BoxFuture<'static, crate::Result<()>>;
```

Three design points here that matter a lot for canal:

1. **`build()` returns the sink *and* a healthcheck as one unit.** The healthcheck is a
   connector-authored future, not a core-implemented probe. The core runs it at boot (and can be
   told to ignore failures via `healthcheck.enabled = false`). Canal wants exactly this: the core
   cannot know how to health-check a generic sink, so the connector supplies the closure — but note
   canal's design-rules line about a UI that cannot distinguish "the endpoint answered" from "your
   data arrived". Vector's healthcheck is explicitly the former only, and Vector's docs are honest
   that it is a boot-time credential/reachability check, not a liveness guarantee.
2. **`input() -> Input` is static edge typing.** `Input` carries a `DataType` bitflag set plus a
   schema `Requirement`. The topology validates source/transform/sink edge compatibility *at config
   load*, before anything runs. Wiring a metric-only source into a log-only sink is a config error
   with a message, not a runtime surprise. This is the mechanism canal needs so that "implement the
   interface and register it" doesn't produce silently-incompatible graphs.
3. **`acknowledgements()` is on the sink config, and the source is told the resolved answer.** The
   two ends negotiate through the core; neither knows the other.

### 1.4 `VectorSink` and `StreamSink`

```rust
// RECALL: medium-high on StreamSink::run; medium on the VectorSink enum's current variants.
pub enum VectorSink {
    Stream(Box<dyn StreamSink<EventArray> + Send>),
    // Historically also an `EventStream`/`Sink`-based variant; the codebase converged on
    // stream sinks and the older futures-`Sink` variant was removed.
}

#[async_trait::async_trait]
pub trait StreamSink<T> {
    /// Consume the input stream until it ends. Returns Result<(), ()> — again, no error type.
    async fn run(self: Box<Self>, input: futures::stream::BoxStream<'_, T>) -> Result<(), ()>;
}
```

Same "you get a stream, you own the loop" shape as sources, mirrored. Note the sink consumes
`EventArray` — **arrays, not single events** — as the unit of transfer. Batching is pushed down to
the transport channel type itself, so the per-event overhead of the topology is amortised. Canal
should carry the same instinct: the internal channel element should be a batch, not a record.

### 1.5 `TransformConfig` and the three transform shapes

```rust
// RECALL: medium.
#[async_trait::async_trait]
pub trait TransformConfig: DynClone + NamedComponent + core::fmt::Debug + Send + Sync {
    async fn build(&self, cx: &TransformContext) -> crate::Result<Transform>;
    fn input(&self) -> Input;
    fn outputs(&self, /* input definitions, log namespace */ ...) -> Vec<TransformOutput>;
}

pub enum Transform {
    /// Synchronous, 1 event in → N events out, no internal state across events.
    Function(Box<dyn FunctionTransform>),
    /// Synchronous but stateful/fallible.
    Synchronous(Box<dyn SyncTransform>),
    /// Owns a stream→stream conversion; can buffer, window, aggregate, reorder.
    Task(Box<dyn TaskTransform<EventArray>>),
}

pub trait FunctionTransform: Send + Sync + DynClone {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event);
}

pub trait TaskTransform<T>: Send + 'static {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn futures::Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn futures::Stream<Item = T> + Send>>;
}
```

**The tiering is the idea worth stealing, not the names.** Vector distinguishes transforms that are
pure per-event functions from transforms that need to own the stream (aggregations, windowed
dedupe, `reduce`). The cheap tier can be *inlined into the same task as its upstream component* —
no channel, no task hop, no scheduling cost — while the expensive tier gets its own task. Vector's
topology builder does exactly that fusion. A canal transform-chain design should have the same two
tiers for the same reason, and should make the cheap one the default.

Note also `FunctionTransform::transform` writes into a caller-provided `&mut OutputBuffer` rather
than returning a `Vec`. That is allocation-avoidance as an interface decision: the buffer is reused
across events. The Go analogue is `Transform(dst []Record, src Record) []Record` with append —
worth doing on the hot path, worth *not* doing anywhere else.

---

## 2. Record model

### 2.1 The fixed event enum

```rust
// RECALL: high. This one is stable and well known.
// lib/vector-core/src/event/mod.rs (approx.)
pub enum Event {
    Log(LogEvent),
    Metric(Metric),
    Trace(TraceEvent),
}

pub enum EventArray {
    Logs(Vec<LogEvent>),
    Metrics(Vec<Metric>),
    Traces(Vec<TraceEvent>),
}
```

`EventArray` being *homogeneous per variant* is deliberate: a batch is all-logs or all-metrics, so
downstream code matches once per batch, not once per event.

### 2.2 `LogEvent`: a structured document, not bytes

```rust
// RECALL: medium-high on the (value, metadata) pairing; medium on Arc placement.
pub struct LogEvent {
    /// The event's data. An `Object` at the top level in practice.
    fields: Arc<Value>,
    metadata: EventMetadata,
}
```

`Value` is VRL's value type — a self-describing dynamic document type:

```rust
// RECALL: high on the variant set; `Regex` variant is medium.
pub enum Value {
    Bytes(Bytes),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Object(BTreeMap<KeyString, Value>),
    Array(Vec<Value>),
    Regex(ValueRegex),
    Null,
}
```

**This is the answer to "how do raw bytes and structured data coexist" and it is a good one:**
`Bytes` is *a variant of the value type*, not a separate representation of the event. An unparsed
log line is a `LogEvent` whose `message` field is `Value::Bytes`. Parsing it with `remap` replaces
that field with `Value::Object`. There is no "raw event" type and no "structured event" type and no
conversion boundary between them — there is one event type whose leaves may be opaque bytes.

Nothing in the pipeline has to know whether an event is parsed. A sink that wants bytes serialises
the value (§5, §2.5); a sink that wants JSON serialises the same value differently. The codec
handles it, not the connector.

Contrast the two rejected alternatives:

- **Arbitrary bytes as the envelope** (Kafka Connect's `byte[]`, or a naked `[]byte` in canal):
  every transform must parse and re-serialise, so a chain of N transforms pays N round trips, and no
  transform can be written generically.
- **A per-connector typed record** (Debezium's generated schemas): transforms become
  connector-specific and the "zero core knowledge of the connector" property dies.

Vector's middle path — one dynamic document type with an opaque-bytes leaf — is the right shape for
canal's canonical record (design-rule R2, open decision 5). It is also the shape that makes a
*generic* UI possible: the frontend can render any event because every event is a JSON-ish tree.

### 2.3 `EventMetadata`: the out-of-band sidecar

```rust
// RECALL: medium. Field set has grown a lot; `datadog_origin_metadata` is a real
// (and instructive) vendor leak, see §13.
pub struct EventMetadata {
    /// A SECOND value namespace, addressable as `%foo` in VRL vs `.foo` for the event body.
    value: Value,
    secrets: Secrets,
    finalizers: EventFinalizers,
    source_id: Option<Arc<ComponentKey>>,
    source_type: Option<Cow<'static, str>>,
    upstream_id: Option<Arc<OutputId>>,
    schema_definition: Arc<schema::Definition>,
    // ...
}
```

Several strong ideas packed in here:

- **Metadata is a separate addressable namespace, not fields mixed into the body.** VRL addresses
  the event body with `.field` and the metadata with `%field`. So provenance, routing hints and
  connector-specific junk never collide with the user's data and never accidentally get serialised
  into the sink payload. Canal must have this split from day one; the alternative (reserved
  `_canal_*` field prefixes in the body) is how you end up with R9-style vocabulary sprawl.
- **`secrets: Secrets`** is its own compartment so credentials pulled from a secret backend ride
  along without being loggable or serialisable by accident.
- **`source_id` / `source_type` / `upstream_id`** — provenance is core, not connector-supplied.
  `upstream_id` is what makes `vector tap` and per-edge metrics possible.
- **`finalizers: EventFinalizers`** — the ack handles live *on the event*, which is what makes
  end-to-end acknowledgement work through arbitrary transform topologies. See §3/§9.
- **`schema_definition`** rides with the event rather than being looked up. See §5.

### 2.4 What is NOT in the model — and this is the big one for canal

There is **no operation type, no key/value split, no before/after image, no transaction id, no LSN,
no primary key** in Vector's event model.

Vector is a telemetry router, not a CDC tool. An event is a fact that happened, not a mutation of a
row. So:

- `op` (`c`/`u`/`d`/`r`) — absent.
- `before`/`after` — absent.
- key vs value — absent as a *model* concept. Partitioning is instead expressed as a
  per-sink templated *string* (`key_field`, or a `Template` like `logs-%Y-%m-%d` /
  `{{ host }}`), evaluated per event by the sink's partitioner (§8.2). So "the key" is a sink-side
  projection of the event, not an envelope field.
- Ordering — no sequence number. Vector explicitly does not promise global ordering.
- Delete/tombstone semantics — absent.

**Canal cannot copy this part.** Canal's end-state includes streaming CDC, which means the envelope
must carry operation type, key, before/after images and a source position. The lesson to take is
*structural*, not literal:

- Keep the **body** as Vector's dynamic document (`Value`), because that is what makes the core
  source/sink-agnostic and the UI generic.
- Put CDC semantics in a **fixed, typed envelope around** that body — `Op`, `Key`, `Before`,
  `After`, `Position`, `Namespace/Collection` — so a source that has no such concept leaves them
  zero and a sink that ignores them costs nothing.
- Put everything else — provenance, finalizers, secrets, connector scratch — in a **metadata
  namespace** that is separately addressable and never serialised by default.

That is: Vector's two-namespace split (body + metadata) plus a third, *typed* layer (the CDC
envelope) that Vector didn't need. Note this is a genuine three-way split, not R9 vocabulary sprawl:
each of the three has a different lifetime and a different serialisation rule.

### 2.5 Native representation

Vector defines `native` and `native_json` codecs — a wire form of the *whole* event including
metadata, used by the `vector` source/sink pair for Vector-to-Vector transport, and by disk buffers.
The protobuf definition (`proto/vector.proto` / `event.proto`, RECALL: medium on path) is the
canonical schema for `Event`.

**Steal:** canal's canonical record needs exactly this — one blessed wire encoding of the full
envelope-plus-metadata, used for (a) worker-to-worker transport, (b) buffer/WAL persistence, and
(c) dead-letter payloads. If those three use three encodings you will get three subtly different
notions of what a record is, which is R2 failing in a new way.

---

## 3. Checkpoint model

**This is Vector's weakest area and the most important "what they got wrong" for canal.**

### 3.1 There is no checkpoint abstraction

There is no `Checkpointer` trait in the core, no offset type in the event model, no
`commit(position)` in `SourceConfig`. Position tracking is entirely **per-connector, ad hoc, and
invisible to the core**. From recall, the actual implementations:

| Source | Position mechanism | Where it lives |
|---|---|---|
| `file` | `checkpoints.json` mapping file fingerprint → byte offset, written periodically | `${data_dir}/file_source/` |
| `kafka` | Kafka consumer-group offset commit, gated on ack | Kafka `__consumer_offsets` |
| `journald` | cursor string in a checkpoint file | `${data_dir}` |
| `aws_sqs` / `aws_s3` | delete the SQS message after ack (the queue *is* the checkpoint) | remote |
| `http_server`, `syslog`, `socket` | none — push sources, no replayable position | n/a |

The only shared infrastructure is the **global `data_dir` option** plus a convention that a
component gets a subdirectory under it. That is it. Vector's own docs describe durability per-source
rather than as a system property.

`file`'s fingerprinting is worth a note because it is the interesting sub-problem: identity of a
"stream" when the underlying handle is unstable. Options are `checksum` (hash of the first N bytes,
`fingerprint.bytes` / `ignored_header_bytes`) or `device_and_inode`. Rotation, truncation and
copy-truncate all break naive inode identity, so the checksum mode exists to survive them. The
generalised lesson: **a checkpoint key must be derived from stable content-addressable properties of
the stream, not from a handle the OS may reuse.** Canal's equivalent decision is what identifies a
"partition" of a source across restarts and across config edits.

### 3.2 Who owns it, when it is committed

Where checkpointing exists, the pattern is consistent and correct:

1. Source reads a record and notes its position.
2. Source attaches a finalizer to the event (§3.3) and sends it downstream.
3. Source **awaits the batch status** from the finalizer.
4. **Only on `BatchStatus::Delivered` does it advance the committed position.**

So the commit is source-owned, downstream-gated, and asynchronous. The source does not block on each
record — it keeps reading and holds a set of in-flight ack futures, advancing a watermark as they
resolve. (`kafka` is the clearest implementation of this.)

This is exactly the discipline canal's R4 demands, and Vector arrives at it *without* the source and
sink knowing about each other. The mechanism is §3.3.

### 3.3 The acknowledgement mechanism (`EventFinalizer` / `BatchNotifier`)

This is the best-engineered thing in the codebase.

```rust
// RECALL: medium-high on the mechanism, medium on exact type/method names.
// lib/vector-common/src/finalization.rs (approx.)

/// Terminal disposition of one event.
pub enum EventStatus {
    /// Not yet finalized.
    Dropped,
    /// Successfully processed / durably handed off.
    Delivered,
    /// Transient failure — the sender may retry.
    Errored,
    /// Permanent failure — retrying will not help (bad data, 4xx).
    Rejected,
}

/// Aggregate disposition of a whole batch, sent once.
pub enum BatchStatus {
    Delivered,
    Errored,
    Rejected,
}

/// One event's handle onto its batch.
pub struct EventFinalizer {
    status: AtomicCell<EventStatus>,
    batch: Arc<OwnedBatchNotifier>,
}

impl EventFinalizer {
    pub fn new(batch: BatchNotifier) -> Self;
    pub fn update_status(&self, status: EventStatus);
}

/// Events carry zero or more finalizers (more than one after a `reduce`/merge).
pub struct EventFinalizers(SmallVec<[Arc<EventFinalizer>; 2]>);

impl BatchNotifier {
    /// Create a notifier plus the oneshot the SOURCE awaits.
    pub fn new_with_receiver() -> (BatchNotifier, BatchStatusReceiver);
}
```

The mechanics, which are the part to internalise:

- The source creates a `BatchNotifier` per read-batch and keeps the `BatchStatusReceiver`.
- Each emitted event gets an `EventFinalizer` pointing at that notifier, stored in
  `EventMetadata.finalizers`.
- **Status is merged pessimistically, worst-wins.** `Delivered` + `Errored` → `Errored`;
  anything + `Rejected` → `Rejected`. So one bad event in a batch cannot be masked by its
  successful siblings.
- **The ack fires on `Drop`, via reference counting.** When the last `EventFinalizer` for a batch is
  dropped, the `OwnedBatchNotifier`'s `Drop` sends the merged `BatchStatus` on the oneshot. Nobody
  has to remember to call `ack()`.
- **Therefore losing an event is not silently un-acked — it is an ack with the wrong status.** If a
  transform drops an event without setting `Delivered`, the finalizer drops in state `Dropped` and
  the batch resolves as not-delivered, so the source does *not* advance. Correct-by-default:
  forgetting to do anything produces "don't checkpoint", which is the safe direction.
- **Fan-out clones the `Arc`,** so an event routed to three sinks has three references to one
  finalizer and the batch resolves only when all three have finished. Fan-out correctness falls out
  of `Arc` semantics rather than being a special case.
- **`reduce`-style transforms merge finalizer sets**, so one output event can hold the finalizers of
  the fifty inputs it consumed, and acking it acks all fifty. `AddBatchNotifier`/`merge` on the
  metadata handles this.

The sink side closes the loop through two traits:

```rust
// RECALL: medium.
pub trait Finalizable {
    /// Take the finalizers out of a request so the driver can settle them on response.
    fn take_finalizers(&mut self) -> EventFinalizers;
}

pub trait DriverResponse {
    /// Map the transport response onto an event disposition.
    fn event_status(&self) -> EventStatus;
    fn events_sent(&self) -> &GroupedCountByteSize;
    fn bytes_sent(&self) -> Option<usize> { None }
}
```

The generic `Driver` takes finalizers off each request before sending, awaits the response, and calls
`update_status(response.event_status())`. **A connector author writes `event_status()` — one function
mapping "what the remote said" to `Delivered | Errored | Rejected` — and gets end-to-end
acknowledgement for free.** That is the whole connector-facing surface of the ack system. This is an
extraordinarily good API/effort ratio and it is the single best thing to copy.

### 3.4 Buffers as the acknowledgement boundary

Critical interaction: **when a sink has a disk buffer, the buffer takes ownership of the finalizers
on write.** Once the record is durably in the disk buffer, the event is `Delivered` from the source's
point of view, and the source may advance its checkpoint — because the buffer will survive restart
and retry the send itself.

With a **memory** buffer the finalizers pass straight through to the sink, so acks only resolve on
actual remote success.

So the durability boundary is *configurable per sink*, and the ack semantics follow the
configuration automatically. This is precisely the discipline canal's R4 states ("if a stage cannot
promise durability, it must not return a success the sender is told to checkpoint on") —
implemented, not just written down.

### 3.5 Restart behaviour and the compatibility hook

- Source position: whatever that connector persisted under `data_dir`. No schema, no versioning
  contract across sources, no central migration story.
- Buffer contents: disk buffers replay on start. The record framing carries a checksum, and the
  `Encodable` trait (§10.4) has a `can_decode(metadata)` hook precisely so a binary can *detect*
  that on-disk records were written by an incompatible version rather than misparse them.

That `can_decode` hook is the answer to canal's open decision 9 (checkpoint format compatibility
across upgrades): **persist a metadata/version word alongside every record, and give the decoder an
explicit "can I read this?" predicate that is checked before decoding, so an upgrade fails loudly
instead of corrupting.**

### 3.6 Honest gaps

- No checkpoint interface means **no generic operator view of progress/lag**. Vector cannot tell you
  "this source is 4 minutes behind" in general; only source-specific metrics can.
- No replay/rewind control surface. You cannot ask Vector to reset a source's position; you delete
  a checkpoint file.
- Push sources have no position at all, so `acknowledgements` for them degrades to "hold the HTTP
  response open until acked" (which the `http_server` source does do, returning non-2xx if the
  batch errored). That is actually a nice generalisation — the ack propagates into the *protocol
  response* — but it only works for request/response sources.

---

## 4. Snapshot handling

**Vector has no snapshot concept.** There is no initial-scan phase, no snapshot-to-stream handoff, no
chunking, no parallel scan, no watermarking. This section is prior art *by absence*, and the absence
is itself informative.

The closest things that exist:

- **`file` source `read_from`**: `"beginning"` or `"end"` (plus `ignore_older_secs`). Reading from
  the beginning of an existing file is the entire "backfill" story. There is no distinction in the
  event model or the metrics between "catching up on history" and "tailing live" — it is one byte
  stream, one offset. The transition is implicit: you reach EOF and start waiting.
- **`aws_s3` + SQS**: object-notification driven. Historical objects are simply objects you were
  notified about. No bulk-list-then-tail mode.
- **`kafka` `auto_offset_reset = "beginning"`**: same pattern — the log *is* both history and live,
  so no handoff is needed.

Why Vector gets away with this: **all its sources are append-only logs.** For an append-only stream,
"snapshot" and "stream" are the same operation at different offsets, so the handoff problem does not
exist. The handoff problem is created by sources with *mutable state* (a database table) where the
snapshot is a point-in-time view and the stream is a change log, and the two must be stitched without
gaps or duplicates.

**So Vector offers canal nothing directly here, but it does offer one transplantable structural
idea:** because Vector's source is `Run(ctx)`-shaped and owns its own loop, a source *could*
implement snapshot-then-stream entirely internally with zero core support, and the core would never
know. That is the cheap path, and it is a trap for canal:

- Progress reporting for a snapshot ("42% of 1.2M rows") is impossible if the core can't see phases.
- Resuming a snapshot mid-way requires the checkpoint model to represent "phase = snapshot, chunk =
  K, key > X", which requires the core to have a phase concept.
- Parallelising a snapshot across workers requires the core to hand out chunk assignments.
- The UI requirement ("a frontend showing relevant metrics") cannot show snapshot progress for a
  generic connector unless phase and progress are core concepts.

**Recommendation for canal, derived from Vector's gap:** make phase a first-class, core-visible part
of the source contract — something like a source declaring the phases it supports and the core
driving the transition, with the checkpoint record carrying `phase` plus a phase-specific opaque
position blob. The handoff (open a stream position *before* the snapshot begins, snapshot, then
replay the stream from the held position, deduplicating by key) is the standard Debezium pattern and
must be modelled in the *core* so every source gets it consistently — otherwise every connector
reimplements it slightly wrong. Vector's design would let each connector reimplement it, and that is
the mistake to avoid.

The one Vector-adjacent detail worth carrying: `read_from` is a **per-source config enum with a
conservative default**. `file` defaults to `"beginning"` for newly discovered files but there is a
`ignore_checkpoints` / rotation story around it, and the operator can always choose. Canal's
snapshot mode should likewise be explicit config (`snapshot: never | initial | always`) rather than
inferred, because "did it backfill?" is the question operators ask most and inference makes it
unanswerable.

---

## 5. Schema handling

Vector has three overlapping schema mechanisms, which is itself a lesson (canal's R9: one concept,
one vocabulary).

### 5.1 The global `log_schema` — the documented wart

```toml
# Global, mutable-at-load, read by components at runtime.
[log_schema]
host_key = "host"
message_key = "message"
timestamp_key = "timestamp"
source_type_key = "source_type"
```

Components read these global keys to decide where to put "the message" or "the timestamp". This is
**global mutable state that connectors implicitly depend on** — exactly the anti-pattern canal's
design rules call out (the DI-of-dedupe-store note). Consequences Vector actually suffered:

- A connector's behaviour depends on config it wasn't given, so it cannot be unit-tested in
  isolation without setting global state.
- Changing `log_schema` changes the behaviour of every component at once.
- Library consumers of `vector-core` fight over the global.

### 5.2 `LogNamespace` — the migration out of that hole

```rust
// RECALL: high on the two variants, medium on method names.
pub enum LogNamespace {
    /// Everything flat in the event root, keys from global `log_schema`.
    Legacy,
    /// Event body under `.`, all connector-supplied metadata under `%source_type` etc.
    Vector,
}
```

The `Vector` namespace puts the payload in the event root and *all* injected metadata in the metadata
namespace, so there is no collision between "the user's field called `host`" and "the host the event
came from". This is unambiguously the right model and Vector arrived at it years late; the migration
is long-running, dual-coded (`outputs(global_log_namespace)` takes the namespace as an argument
precisely so a connector can declare *different* output schemas per namespace), and a recurring
source of user confusion.

**Steal for canal: start with the equivalent of `LogNamespace::Vector` and never build Legacy.**
Payload and framework metadata live in separate namespaces from commit one. This is cheap now and
extremely expensive later.

### 5.3 `schema::Definition` / `Requirement` — out-of-band structural typing

```rust
// RECALL: medium-low on exact API; the concepts are right, the method names are guesses.
// Attached to events via EventMetadata.schema_definition, and declared statically by components.
pub struct Definition {
    /// The structural type of the event body.
    event_kind: Kind,
    metadata_kind: Kind,
    /// Named "semantic meanings" pointing at paths: e.g. "timestamp" -> ".ts", "message" -> ".msg"
    meaning: BTreeMap<String, MeaningPointer>,
    log_namespaces: BTreeSet<LogNamespace>,
}

/// A sink says what it needs.
pub struct Requirement {
    meaning: HashMap<String, SemanticMeaning>,
}
```

The interesting part is **semantic meaning as an indirection layer**. A sink does not say "I need a
field called `timestamp`"; it says "I need a field that *means* timestamp". Sources declare which of
their fields carry which meaning. `remap` can re-point a meaning. So a sink works against any
source's field naming without the sink knowing the source.

Propagation is **out-of-band and static**: definitions flow through the graph at config-load time
(each component's `outputs()` computes its output definition from its input definitions), and are
validated against each sink's `Requirement` before anything runs. The per-event
`metadata.schema_definition` is the runtime carrier so a sink can resolve a meaning to a path at
send time.

Honest assessment: this system is **partially wired, feature-gated (`schema.enabled`), driven
largely by Datadog-shaped requirements, and not something most Vector sinks use.** Discovery is
absent — there is no notion of asking a source what its schema is at runtime. Evolution/drift is
absent — the definition is static config-time metadata, not a runtime observation, so a source whose
records change shape mid-stream is simply unmodelled.

### 5.4 What canal should take

- **Semantic meanings / logical field roles are a genuinely good abstraction** for keeping sinks
  source-agnostic: a sink declares required roles, a source declares which paths fill them, the core
  validates the pairing at config time. This is how canal keeps constraint 1 (source/sink agnostic)
  while still letting a sink do something intelligent with, say, the event timestamp or the primary
  key.
- **Static, config-time schema validation of edges** (Vector's `Input`/`Output` + `Requirement`) is
  what turns "adding a connector is just registration" into something safe. Do this.
- **Runtime schema discovery and drift are unsolved in Vector.** Canal needs them (CDC sources drift:
  a column is added). Prior art for that is Debezium/Kafka-Connect `Schema` in the envelope, not
  Vector. Vector's contribution is only the static-validation half.

---

## 6. Lifecycle

### 6.1 The states, such as they are

Vector's component lifecycle is deliberately thin:

1. **Deserialize** — TOML/YAML → `Box<dyn SourceConfig>` etc. via the registry (§10).
2. **Validate** — `Input`/`outputs()` type compatibility across every edge; `resources()` conflict
   detection (two components binding the same port); schema `Requirement` satisfaction; unknown-field
   rejection. All *before* anything is built.
3. **Build** — `build(cx)` per component. Fallible, returns `crate::Result`. A build failure aborts
   the whole topology start (or aborts a *reload*, leaving the old topology running).
4. **Healthcheck** — run the returned `Healthcheck` future. Failure aborts start unless
   `healthcheck.enabled = false` (or `--require-healthy=false`, which is the default: healthchecks
   are logged but non-fatal at boot by default).
5. **Run** — every component is a spawned task. Sources run their future; sinks consume their
   stream; transforms either get their own task or are fused into the upstream one.
6. **Shutdown** — see §6.3.

There is **no `open`/`close`, no `commit` callback, no per-poll hook, no pause/resume.** The task
*is* the lifecycle: setup happens at the top of the async fn, teardown happens when it returns or is
dropped. Rust's `Drop` does most of the work that an explicit `close()` would do elsewhere.

**For canal in Go this does not translate.** Go has no `Drop`, so canal *must* have explicit
`Close(ctx) error` on both sources and sinks, and must define whether `Close` may flush (it must be
allowed to, and must be given a context with a deadline). Vector's "the destructor handles it" is a
language affordance canal cannot borrow.

Note also the absence canal's design rules already anticipate: Vector has **no connector state
machine** (`healthy → degraded → paused → terminal`). A component is running or the process is
dying. There is no operator-visible per-connector status beyond metrics and logs, and no way to pause
one connector. Canal's open decision 7 is asking for something Vector does not have, and the
`degraded` state in particular (sustained backoff should be *visible*, not just slow) is a real
improvement over Vector.

### 6.2 Cancellation

```rust
// RECALL: medium. Concept is solid; names approximate.
pub struct ShutdownSignal {
    /// Resolves when this source should begin shutting down.
    begin_shutdown: Option<future::Shared<oneshot::Receiver<ShutdownSignalToken>>>,
    /// Dropped when the source has finished — this is how the coordinator learns it's done.
    shutdown_complete: Option<ShutdownSignalToken>,
}
```

`ShutdownSignal` is `Future`-implementing, so a source's `select!` loop awaits it alongside its real
work. The token is the important half: **the coordinator learns a source has finished draining by
the token being dropped**, not by a timer. `SourceShutdownCoordinator` holds one per source and can
report *which* sources are still draining — which it does, in a log line, during shutdown. That
"here is precisely who is holding us up" diagnostic is a small thing that operators love.

The two-signal design (begin-shutdown vs shutdown-complete) is the transplantable bit: `ctx.Done()`
covers only the first. Canal needs both, per connector, and should log the laggards by name.

### 6.3 Graceful shutdown ordering

On SIGINT/SIGTERM:

1. Sources are signalled to stop *producing*.
2. Sources finish in-flight work, flush to their output channel, and drop their tokens.
3. When a source's output is closed, its downstream sees end-of-stream, drains, and exits — the
   shutdown propagates **topologically, by channel closure**, from sources through transforms to
   sinks. There is no shutdown broadcast to sinks; a sink shuts down because its input ended.
4. A global deadline (`--graceful-shutdown-limit-secs`, default 60 from recall) forces exit if
   draining hangs. A second SIGINT forces immediate exit.

**This is the correct design and canal should copy it exactly.** Shutdown propagating by
input-closure means a sink is structurally incapable of exiting before it has drained what was
handed to it, and no ordering logic is required anywhere. Cancel only the sources; let closure do
the rest; keep a hard deadline as the safety valve. In Go: cancel source contexts, close channels on
source return, sinks `for range ch`, `errgroup` with a shutdown timeout.

### 6.4 Error and retry classification

Two distinct classification systems, at different layers, and the split is instructive.

**Request-level (sink transport), via `RetryLogic`:**

```rust
// RECALL: medium-high on RetryAction; medium on the trait.
pub trait RetryLogic: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Response;

    /// Is this transport-level error worth retrying?
    fn is_retriable_error(&self, error: &Self::Error) -> bool;

    /// Did the (successful-transport) response indicate we should retry?
    fn should_retry_response(&self, _response: &Self::Response) -> RetryAction {
        RetryAction::Successful
    }
}

pub enum RetryAction {
    /// Retry, with a reason string for logs/metrics.
    Retry(Cow<'static, str>),
    /// Do not retry, with a reason.
    DontRetry(Cow<'static, str>),
    Successful,
}
```

Note the shape: **the classification is a connector-authored function over the connector's own error
and response types**, and the *policy* (how many times, how long to back off, whether to give up) is
core config. That is the right seam. The reason strings are carried into logs and the
`component_errors_total{error_type=...}` metric, so a retry is never anonymous.

The default HTTP classifier maps: 5xx → retriable, 429 → retriable (honouring `Retry-After`),
408/timeouts → retriable, other 4xx → `DontRetry` (permanent), 2xx → success. Notably some sinks
special-case 400-with-a-particular-body as permanent-reject so it goes to the ack path as
`Rejected` rather than looping forever.

**Event-level (disposition), via `EventStatus`:** `Delivered | Errored | Rejected` (§3.3). The
`Errored`/`Rejected` split is the transient/permanent axis, and it is what the *source* sees. So the
source learns "retry-worthy failure" vs "this data will never be accepted" without learning anything
about HTTP.

**Comparison to canal's inherited taxonomy.** Canal's design rules already carry a richer
failure-mode taxonomy (transient-upstream, transient-internal, permanent-upstream, permanent-mapping,
permanent-contract, duplicate-idempotent-success, clock-skew). Vector's is coarser — essentially
`{transient, permanent}` × `{transport, event}` — but Vector's has two properties canal's list does
not yet:

1. **It is expressed as a connector-implemented function**, not a doc. The connector *must* classify,
   because the trait requires it.
2. **Each classification carries a reason string that reaches telemetry.** No unexplained retries.

Canal should keep the richer taxonomy but express it Vector's way: a `Classify(err) (Class, reason
string)` method the connector implements, with the retention/backoff/dead-letter *policy* living in
the core keyed on `Class`. And per canal's own rules, sustained `transient` must escalate the
connector to a visible `degraded` state — which Vector lacks entirely.

**What Vector got wrong here, and it matters:** the component-level error path is
`Result<(), ()>`. Once a source or sink task returns, its error type is gone. There is no supervision
tree, no per-component restart, no exponential-backoff-then-retry-the-whole-component. If a sink's
task dies, Vector logs and generally tears down the topology. Canal should return a typed error from
`Run`/`Close` and should decide explicitly whether a failed connector restarts in place — Vector's
answer ("restart the process") is not adequate for a multi-pipeline enterprise deployment.

---

## 7. Config model

This is Vector's second-strongest area after acknowledgements, and it is the one that most directly
serves canal's "frontend driven by what connectors declare about themselves" goal.

### 7.1 How a connector declares config

```rust
// RECALL: medium-high on the macro's existence and usage shape; medium on attribute spellings.
/// The macro that does everything: derives Serialize/Deserialize, derives Configurable
/// (JSON-Schema generation), implements NamedComponent with NAME = "http",
/// and registers the type in the component inventory.
#[configurable_component(sink("http", push))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct HttpSinkConfig {
    /// The full URI to make HTTP requests to.
    ///
    /// This should include the protocol and host, but can also include the port, path,
    /// and any other valid part of a URI.
    #[configurable(metadata(docs::examples = "https://10.22.212.22:9000/endpoint"))]
    pub uri: UriSerde,

    /// The HTTP method to use.
    #[serde(default)]
    pub method: HttpMethod,

    #[configurable(derived)]
    #[serde(default)]
    pub compression: Compression,

    #[configurable(derived)]
    pub encoding: EncodingConfigWithFraming,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: RequestConfig,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "crate::serde::bool_or_struct")]
    pub acknowledgements: AcknowledgementsConfig,
}
```

The properties that make this work, each of which canal needs an equivalent for:

- **Doc comments are the documentation.** The first line becomes `title`, the rest becomes
  `description` in the generated JSON Schema, which becomes the website reference docs. There is
  exactly one place the option is documented, adjacent to the code that reads it. Drift is
  structurally impossible (canal's R8: "drift is prevented structurally, not by discipline").
- **`#[configurable(derived)]` means "inherit the schema of the field's type"**, so shared blocks
  (`batch`, `request`, `buffer`, `encoding`, `tls`, `proxy`, `acknowledgements`) are declared once,
  globally, and appear identically on every sink that includes them. **This is the single most
  important config-model idea for canal**: the framework's cross-cutting concerns are *struct fields
  the connector embeds*, not conventions the connector reimplements. A connector that wants batching
  embeds `BatchConfig` and gets the options, the defaults, the docs, the validation and the UI for
  free.
- **`#[serde(deny_unknown_fields)]`** — a typo in a config key is a startup error, not a silently
  ignored option. Vector's config loader also reports unknown top-level keys and suggests near
  matches.
- **Defaults are declarative** (`#[serde(default)]` / `#[serde(default = "path::to::fn")]`) and are
  *reflected into the schema*, so the UI and the docs know the default without it being written
  twice.
- **Generic default-carriers**: `BatchConfig<RealtimeSizeBasedDefaultBatchSettings>` — the type
  parameter supplies sink-appropriate defaults (a realtime sink wants small/timeout-driven batches; a
  bulk sink wants large ones) while the *option set* stays identical. This is a genuinely good use of
  generics: same schema, different defaults, no duplicated struct. Canal should note this as the
  legitimate-generics case (constraint 5).

### 7.2 The schema trait

```rust
// RECALL: medium-low on exact signatures. The crate (lib/vector-config) and the
// generate_schema/metadata concepts are right; names may differ.
pub trait Configurable {
    /// If Some, this type is emitted once as a $ref-able definition rather than inlined.
    fn referenceable_name() -> Option<&'static str> { None }

    fn metadata() -> Metadata { Metadata::default() }

    fn validate_metadata(_metadata: &Metadata) -> Result<(), GenerateError> { Ok(()) }

    fn generate_schema(
        generator: &RefCell<SchemaGenerator>,
    ) -> Result<SchemaObject, GenerateError>;
}
```

Output: a **single JSON Schema document for the entire Vector configuration surface**, generated by a
build-time task and committed/published. That artifact is what drives:

- the website's configuration reference (via a CUE pipeline, RECALL: medium),
- editor autocomplete / validation for `vector.yaml`,
- `vector validate` and the startup config check,
- and it is exactly the artifact a UI needs to render a generic connector config form.

**This is the mechanism for canal's "specialized UI/UX for less-generic connectors comes later and
must NOT require core changes" constraint.** If every connector's config is reflected into a
machine-readable schema with titles, descriptions, defaults, examples, enums and required-ness, then
the frontend renders *any* connector generically from the schema, and a bespoke UI for a particular
connector is a later, purely additive override keyed on the connector's name. The core never learns
about connectors.

The Go equivalent worth considering: struct tags plus `reflect` to emit JSON Schema, or a small
`Describe() ConfigSchema` method on the registry entry. Struct tags + reflection gets you Vector's
single-source-of-truth property; a hand-written `Describe()` does not (it can drift from the struct,
which is R8's failure mode). Doc comments are not available to Go reflection, so canal needs either a
`doc:"..."` struct tag or a code generator that reads comments — a real decision, and the one place
Rust's macro system gives Vector something Go cannot cheaply match.

### 7.3 Validation layers

1. **Deserialization** — types, unknown fields, required fields.
2. **`Configurable::validate_metadata`** plus schema-level constraints (ranges, non-empty strings).
3. **Semantic validation at topology build**: edge type compatibility (`Input` vs `outputs()`),
   `resources()` conflicts, cycle detection, orphan detection (a source wired to nothing, a sink with
   no inputs — both are errors, not warnings), unknown component references in `inputs`.
4. **`vector validate`** — a CLI subcommand that runs 1–3 plus optionally the healthchecks, without
   starting the data path. Also `vector generate 'source/transform/sink'` to scaffold config, and
   `vector list` to enumerate registered components.

`vector validate` as a distinct, cheap, offline-able command is worth copying verbatim as
`canal validate`.

### 7.4 Secrets

Config supports `SECRET[backend.key]` interpolation resolved at load from a configured secret backend
(`exec`-based, file-based, AWS/GCP providers). Resolved secrets go into the typed config and, for
per-event secrets, into `EventMetadata.secrets` — a separate compartment so they are not loggable.
Environment interpolation (`${VAR}`, with `${VAR-default}`) is also supported. Canal needs this early;
retrofitting secret indirection after connectors have `password string` fields is painful.

---

## 8. Backpressure

Vector's backpressure story is the most complete of any system on this list, and it is built from four
composable layers. The governing principle, stated in Vector's own docs: **Vector does not drop data
by default; it propagates backpressure to the source and lets the source decide.**

### 8.1 Layer 1 — bounded channels everywhere

Every edge in the topology is a bounded channel. `SourceSender::send` awaits when the downstream
channel is full. A source that cannot send is a source that is not reading. That is the whole
mechanism, and it is why backpressure reaches the origin without any explicit signalling protocol:
**there is no unbounded queue anywhere in the topology, so slowness is transitive by construction.**

This is canal's R6 generalised to every edge, not just the buffer.

### 8.2 Layer 2 — buffers as an explicit, configurable per-sink stage

**This is the headline feature.** Every sink has a buffer in front of it, and it is operator-visible
config:

```toml
[sinks.my_sink.buffer]
type = "memory"        # or "disk"
max_events = 500       # memory only
when_full = "block"    # or "drop_newest"

# disk variant
[sinks.other_sink.buffer]
type = "disk"
max_size = 268435488   # bytes, not events — includes framing overhead
when_full = "block"
```

`when_full` (RECALL: high on `block`/`drop_newest`, medium on `overflow`):

```rust
pub enum WhenFull {
    /// Apply backpressure: the writer awaits. Default.
    Block,
    /// Drop the incoming event. Data loss, explicitly chosen, and counted.
    DropNewest,
    /// Spill to the next buffer stage in the chain.
    Overflow,
}
```

Several things here are exactly right and should be copied wholesale:

- **The rejection path is named in the type.** Canal's R6 says "a buffer without a rejection path is
  not a buffer" and "backpressure is an expressible outcome — block or reject". Vector's enum *is*
  that rule, encoded. There is no way to configure unbounded growth.
- **`drop_newest` rather than `drop_oldest`.** Dropping the newest preserves the already-acked
  prefix and keeps the ack/checkpoint invariant intact; dropping the oldest would silently discard
  data the source may have been told was accepted. The choice of *which end* to drop is not arbitrary.
- **Dropping is counted**, via `buffer_discarded_events_total`. Silent loss is impossible to
  configure; you can only configure *counted* loss.
- **Buffers can be chained** (`Overflow`): a small memory buffer overflowing into a large disk
  buffer, so the common case never touches the disk but a sustained sink outage still doesn't block.
  That tiering is a natural extension of a one-buffer-interface design, and it is the answer to the
  "local dev store vs durable store" axis in canal's design rules — same interface, different stage.
- **`max_size` for disk is bytes including overhead**, and Vector's docs are explicit about it. This
  caused real operator confusion (§13) but the honest accounting is correct: a byte cap is what
  bounds a disk, an event cap is what bounds RAM predictability.

Disk buffer implementation (`disk_v2`, RECALL: medium on internals): a purpose-built record-oriented
log — segmented data files with a size cap per segment, per-record length prefix and CRC32C checksum,
and an in-memory *ledger* (read/write offsets, segment ids) flushed periodically and on clean
shutdown. Reads and writes are decoupled; fully-read segments are deleted. It is a WAL, not a
key-value store. Notably it replaced a LevelDB-based `disk_v1` — see §13.

Relevant to canal's open decision 1 (WAL vs segment-per-partition, and whether checkpoint state
shares the WAL): Vector's answer is **segmented log + separate ledger**, and the checkpoint state
(source position) is *entirely independent* of the buffer. That independence is only safe because the
buffer takes ownership of finalizers on write (§3.4) — the two stores are decoupled precisely because
the ack protocol makes the ordering explicit. That is the trick: you do not need a shared WAL for
crash consistency if the durability boundary is explicit and the checkpoint is strictly downstream of
it.

### 8.3 Layer 3 — batching

```toml
[sinks.my_sink.batch]
max_events = 1000        # or
max_bytes = 10000000     # or both
timeout_secs = 1.0
```

Batch settings are a shared, embedded config block (§7.1), with the defaults supplied by a type
parameter per sink. Whichever limit trips first closes the batch. The timeout is what bounds latency
when volume is low, and it is the option operators tune most.

Partitioned batching is a first-class combinator: a sink supplies a `Partitioner` (usually evaluating
a `Template` like `logs-%Y-%m-%d` or `{{ host }}` against each event) and the framework maintains one
open batch per partition key, each with its own limits. This is how a generic sink gets
per-tenant/per-table/per-index batching without writing any batching code.

The stream-sink combinator chain (RECALL: medium on exact method names, high on the shape):

```rust
// The canonical modern Vector sink body — this is the whole `run` method for many sinks.
input
    .batched_partitioned(partitioner, || batch_settings.as_byte_size_config())
    .request_builder(request_builder_concurrency_limit, request_builder)
    .filter_map(|request| async move {
        match request {
            Err(error) => { emit!(SinkRequestBuildError { error }); None }
            Ok(req) => Some(req),
        }
    })
    .into_driver(service)
    .protocol("http")
    .run()
    .await
```

**Read that as an interface statement.** Everything generic — batching, partitioning, encoding,
request construction concurrency, the retry/concurrency/rate-limit service stack, the ack settlement,
the telemetry emission — is a framework combinator. The connector supplies three values: a
`partitioner`, a `request_builder`, and a `service`. Canal's sink framework should aim for exactly
this ratio.

### 8.4 Layer 4 — the tower service stack and Adaptive Request Concurrency

The `service` in that chain is a `tower::Service` wrapped in a stack of layers, all driven by shared
config:

```toml
[sinks.my_sink.request]
concurrency = "adaptive"          # "none" | "adaptive" | <integer>
timeout_secs = 60
rate_limit_duration_secs = 1
rate_limit_num = 1000
retry_attempts = 9223372036854775807   # effectively infinite by default
retry_max_duration_secs = 30
retry_initial_backoff_secs = 1
```

Layer order (outermost → innermost, RECALL: medium): buffer → concurrency limit (fixed or adaptive) →
rate limit → retry → timeout → the connector's own `Service::call`.

Two design points:

- **Retries default to effectively unlimited attempts with a capped per-attempt backoff.** Vector's
  position is that giving up = losing data, so it retries forever and lets backpressure propagate.
  That is only tenable *because* the buffer and channel layers make "retry forever" degrade into
  "block the source" rather than "grow memory without bound". The three features are a package: you
  cannot adopt infinite retry without bounded buffers, and you should not adopt bounded buffers
  without a rejection policy. Backoff is fibonacci/exponential with a max, and `Retry-After` is
  honoured on 429.
- **Adaptive Request Concurrency (ARC)** replaces operator-guessed concurrency limits with a
  closed-loop controller. This is the mechanism (RECALL: medium-high on the algorithm, medium on
  parameter names/defaults):

  - Maintain an exponentially-weighted moving average of observed round-trip time
    (`ewma_alpha`, default ~0.4).
  - After each batch: if the response indicates backpressure (429/503, or the connector's
    `RetryLogic` says so) **or** the observed RTT exceeds `past_rtt_mean + rtt_deviation_scale *
    past_rtt_deviation` (default scale ~2.5), **multiplicatively decrease** the limit
    (`decrease_ratio`, default ~0.9).
  - Otherwise, if we are actually saturating the current limit, **additively increase** by 1.
  - Start at `initial_concurrency` (1), optionally capped by `max_concurrency_limit`.

  It is AIMD — TCP congestion control for HTTP sinks — with RTT-deviation as the congestion signal in
  addition to explicit rate-limit responses. There is a design RFC in the repo
  (`rfcs/2020-04-06-1858-automatically-adjust-request-limits.md`, RECALL: medium on filename) and a
  documented test harness comparing it against fixed limits under various server behaviours.

  **Why this matters for canal:** the concurrency limit is the hardest number for an operator to pick
  and the most damaging to pick wrong (too low = throughput loss; too high = you DoS the sink and get
  throttled into a worse state). Making it adaptive-by-default removes the whole class of problem,
  and the RTT-deviation signal means it works even against sinks that degrade without returning
  errors. ARC also exposes its state as metrics (§11) so an operator can *see* the controller working
  — which is what makes it trustworthy rather than magic.

### 8.5 What happens when the sink is slower than the source

The full chain, in order: sink's remote slows → ARC reduces in-flight concurrency → requests queue →
batches stop being consumed → the sink's buffer fills → per `when_full`, either the write blocks
(default) or events are dropped-and-counted → blocking propagates back through the bounded topology
channels → `SourceSender::send` awaits → the source stops reading → **the source stops advancing its
checkpoint, and pressure is applied to the actual origin** (TCP window on a socket source, no offset
commit on Kafka, HTTP 429/held-open response on `http_server`, file simply not read).

That last hop is the one most systems get wrong, and it only works because the source owns its loop
and its checkpoint: there is no core-driven poll that would keep pulling into a full queue.

**Diagnosing it** is the `utilization` metric (§11): a gauge per component in `[0, 1]` measuring the
fraction of wall time the component spent doing work rather than waiting. The bottleneck is the
component whose utilization is near 1 while its upstreams are blocked. Vector added this specifically
because backpressure was previously undiagnosable — everything is slow and nothing says why. **Canal
should ship this from the start**; it is cheap (time spent in the work branch vs the await branch) and
it is the single highest-value operator metric in the whole system.

---

## 9. Delivery guarantees

Vector's documented position, stated plainly:

- **At-least-once**, and only for source/sink pairs where *both* ends support acknowledgements and
  the operator has enabled them.
- **Best-effort** otherwise (memory buffers, non-acking sources, push sources without a replayable
  position).
- **Exactly-once: not offered anywhere.** No transactional sinks, no two-phase commit, no
  distributed transaction coordinator, no dedup store. Vector's docs are explicit that exactly-once
  is out of scope, and the reasoning is that it requires either idempotent writes (a sink-specific
  property Vector cannot assume) or transactions (which most telemetry sinks do not support).

Per-component guarantees are **published as a matrix in the docs** — every source and sink page
carries its delivery guarantee and whether it supports acknowledgements. That honesty-as-documentation
is itself worth copying: canal's registry should expose per-connector capability flags
(`SupportsAck`, `SupportsSnapshot`, `SupportsResume`, `Idempotent`) and the UI should surface the
*pipeline's* guarantee as the weakest link of its components. Canal's design rules already demand a UI
that distinguishes "the endpoint answered" from "your data arrived"; a computed end-to-end guarantee
badge derived from connector capability flags is the mechanism.

### 9.1 The `can_acknowledge` / `acknowledgements` negotiation

```rust
// source side
fn can_acknowledge(&self) -> bool;

// sink side
fn acknowledgements(&self) -> &AcknowledgementsConfig;
```

Resolution: global `acknowledgements.enabled` × per-sink setting → the core computes whether acks are
live and passes the resolved boolean into `SourceContext.acknowledgements`. The source then either
attaches finalizers and waits before committing, or doesn't.

**The known problem with this** (§13): it degrades *silently*. If you enable acknowledgements but wire
an acking source to a sink that doesn't propagate them, or use a source whose `can_acknowledge()` is
false, you get best-effort delivery with no error. The negotiation is not validated end-to-end at
config time even though every input to that validation is known at config time. **Canal should make
this a config-time check**: if a pipeline is declared `at_least_once` and any component in it cannot
support that, fail to start. Declaring the *intended* guarantee in pipeline config and validating it
against component capabilities is a small addition that closes Vector's most dangerous silent
degradation.

### 9.2 Idempotency and dedup

Vector has **neither**. No dedup store, no idempotency keys, no event ids in the envelope. The
`dedupe` transform exists but is an in-memory LRU over configurable field sets for *noise reduction*,
explicitly not a correctness mechanism — and it is exactly the shape canal's R5 warns about (a
process-lifetime RAM cache described in prose as deduplication).

So: at-least-once + no dedup = **duplicates are the operator's problem**, handled by making the sink
idempotent (e.g. Elasticsearch with a document id derived from event content) if it can be. Several
sinks support an `id_key`/document-id template for exactly this, which is the pattern: *idempotency is
delegated to the sink's own primitives via a templated key computed from the event.*

**For canal this is a gap to fill, not a pattern to copy**, and canal's design rules already specify
the better answer: three layers of idempotency (upstream vendor id → canonical record id → in-flight
submit guard), a durable and tenant-scoped dedupe store, commit-after-write ordering, and a
deterministic derivation rule for sources with no natural id. Vector's contribution here is only the
negative result plus the templated-sink-side-key trick, which is genuinely useful for the subset of
sinks that support upserts.

---

## 10. Plugin boundary

### 10.1 In-process, statically linked, compile-time registry

Vector is a **single static binary with every connector compiled in**. There is no dynamic loading, no
`.so`/`.dll` plugins, no subprocess model, no WASM. Connectors are Rust modules behind Cargo
features, so a build can include a subset (`--no-default-features --features sources-file,sinks-http`)
— feature flags are the only "which connectors exist" control, resolved at compile time.

**This is precisely canal's constraint 3, validated at scale**, and it is worth noting Vector chose it
deliberately and has stuck with it across ~150 sources/sinks and years of pressure to add plugins. The
recurring community request for a plugin API has been consistently declined; the counter-argument is
that a stable plugin ABI would freeze the internal event model and the buffer/ack contracts, which
Vector has needed to evolve. That is a real datapoint for canal: **the cost of an out-of-process
boundary is paid in interface ossification, not just in performance.**

### 10.2 Registration and discovery

Two mechanisms, layered (RECALL: medium — this area has churned and I am least confident here):

1. **Polymorphic deserialization.** Config needs `type = "http"` in TOML to produce a
   `Box<dyn SinkConfig>`. Historically this was `typetag` (`#[typetag::serde(name = "http")]`), which
   uses a linker-section registry to map the discriminant string to a deserializer. Later work moved
   toward generated enums / `enum_dispatch` in places to get better error messages and to make the
   full config surface enumerable for schema generation. **I could not verify which mechanism is
   current.**
2. **Component enumeration** via `inventory` — a linker-section collection so the binary can list
   every registered component without a central match. Something like
   `inventory::submit! { SinkDescription::new::<HttpSinkConfig>("http") }`, collected via
   `inventory::collect!(SinkDescription)`. This is what backs `vector list`, `vector generate`, and
   the schema generator. **Names approximate.**

`NamedComponent` ties them together:

```rust
// RECALL: medium.
pub trait NamedComponent {
    const NAME: &'static str;
    fn get_component_name(&self) -> &'static str { Self::NAME }
}
```

**The Go translation is direct and simpler than Vector's**, because Go has no `Drop`/linker-section
problem to work around and Vector's `typetag`/`inventory` machinery exists mainly to fight Rust's lack
of runtime reflection:

```go
// Sketch, for canal — not Vector code.
package registry

type SourceFactory interface {
    Name() string
    ConfigSchema() Schema                          // §7 — drives validation and the UI
    New(cfg json.RawMessage) (Source, error)       // decode + validate + construct
}

func RegisterSource(f SourceFactory) // called from connector package init()
```

Connector packages call `registry.RegisterSource(...)` in `init()`; the binary blank-imports the
connector packages in one `plugins` package (`_ "canal/connectors/postgres"`). **That single import
list is the only file that changes when a connector is added** — and it is arguably not "core", since
it contains no logic. Everything else is registry lookups. Zero switch statements, which is
constraint 4.

Note what the registry entry must carry beyond the constructor, all of it needed by §7 and §11:
name, config schema, capability flags (§9), and a kind. If the registry entry is just
`func(cfg) (Source, error)`, the UI has nothing to introspect and you will end up adding a
core-side table of connector metadata — which is the switch statement wearing a hat.

### 10.3 Designing for a future out-of-process implementation

Canal's constraint 3 requires the interface to be satisfiable later by a gRPC/subprocess
implementation without core changes. Vector is the useful case study here because **it already has
this, twice over, without calling it a plugin API**:

- The **`vector` source and `vector` sink** are a gRPC pair speaking the native protobuf event
  encoding, with acknowledgements propagating across the wire. So a "connector" can already live in
  another process — it just has to be another Vector.
- The **`exec` source** and the **`exec`-based secret backend** run subprocesses and parse their
  output.

The transplantable lesson is about what the in-process interface must avoid so that an out-of-process
implementation can satisfy it later:

- **The record type must have a canonical wire encoding** (§2.5). Vector has one; that is why the
  gRPC pair was possible without redesigning anything.
- **No shared-memory or borrowed-lifetime assumptions in the interface.** Vector's sink takes an owned
  `EventArray`, not a reference into a pool. In Go: pass records by value or as owned slices the
  connector may retain; never hand a connector a buffer the core will reuse. This is the constraint
  most likely to be violated accidentally, by exactly the allocation-avoidance optimisation described
  in §1.5 — so if canal does buffer reuse on the transform hot path, it must not do it at the
  connector boundary.
- **Acks must be explicit values, not destructor side-effects.** This is where Vector's design would
  *not* survive the transition cleanly: `EventFinalizer` acking on `Drop` is a language-local trick
  with no wire representation. The gRPC `vector` sink has to translate it into explicit
  request/response ack ids. **Canal, being in Go, has no `Drop` and is therefore forced into the
  explicit form from day one** — which is the more portable design. Take the *semantics* of Vector's
  finalizers (reference-counted, worst-wins status merge, resolves once) but implement them as an
  explicit handle with an explicit `Ack(status)`/`Nack(class, reason)` call and a leak detector in
  debug builds to catch the "forgot to settle" bug that Rust's `Drop` prevents for free.
- **Batch-oriented calls, not per-record calls.** A per-record interface is fine in-process and
  catastrophic over RPC. Vector's `EventArray`-as-the-unit choice (§1.4) means the same interface
  works both ways. Canal should make the batch the unit of every core↔connector call for this reason
  alone.
- **Context/cancellation must be a parameter, not ambient.** Already true in Go.

### 10.4 Versioning and format compatibility

There is no plugin ABI to version, so the only compatibility surfaces are *data at rest* and *data on
the wire*:

```rust
// RECALL: medium. The `can_decode` predicate is the notable part and I am fairly confident it exists.
pub trait Encodable: Sized {
    type Metadata;
    type EncodeError;
    type DecodeError;

    /// Version/format marker persisted alongside the record.
    fn get_metadata() -> Self::Metadata;

    /// Explicit compatibility predicate, checked BEFORE attempting to decode.
    fn can_decode(metadata: Self::Metadata) -> bool;

    fn encode<B: BufMut>(self, buffer: &mut B) -> Result<(), Self::EncodeError>;
    fn decode<B: Buf + Clone>(metadata: Self::Metadata, buffer: B) -> Result<Self, Self::DecodeError>;
}
```

`can_decode` is the pattern to steal for canal's open decision 9: **persist a format marker with
every record/checkpoint, and require an explicit "can I read this?" check before decode.** An upgrade
that changes the format then fails loudly at startup with an actionable message, rather than
misparsing bytes or silently discarding a buffer. Vector applies this to disk buffers; canal should
apply it to disk buffers *and* checkpoint state *and* dead-letter payloads.

Config compatibility is handled by a **documented per-release upgrade guide** with explicit breaking
changes and, in some cases, deprecation-with-warning for a release or two before removal. Given how
much Vector's config surface has churned (§13), the upgrade guides are load-bearing documentation.

---

## 11. Observability

Vector's strongest non-obvious idea: **instrumentation is a normative specification, not a
convention.** The repo contains `docs/specs/instrumentation.md` and `docs/specs/component.md`
(RECALL: medium-high that these exist at these paths) written with RFC-2119 MUST/SHOULD language,
defining the events and metrics every component is *required* to emit. A component that doesn't
comply is out of spec, and Vector later built a **component validation framework** — a test harness
that runs a component and asserts it emitted the mandated telemetry with the mandated tags.

That is R8 applied to observability: drift prevented structurally, by a test, rather than by review
discipline. **Canal should have this document and this harness before it has ten connectors**, because
retrofitting consistent metric names across connectors is exactly the kind of churn Vector suffered
(§13).

### 11.1 The tagging convention

Every component-emitted metric carries:

- `component_id` — the operator-assigned name from config (`[sinks.my_sink]` → `my_sink`)
- `component_kind` — `source` | `transform` | `sink`
- `component_type` — the registered connector name (`http`, `kafka`, `file`)
- `output` — for sources/transforms with multiple named output ports

The **separation of `component_id` (instance) from `component_type` (implementation)** is the crucial
bit and it is what makes generic dashboards possible: you can aggregate "all kafka sinks" or drill into
"this one pipeline stage" from the same metric. Canal must fix this vocabulary before the first
connector ships (R9), and must inject identity rather than letting connectors self-tag (§1.2).

### 11.2 The core metric set

Named by *position in the pipeline*, not by connector (RECALL: high — these names are stable and
widely used):

| Metric | Meaning |
|---|---|
| `component_received_events_total` | events accepted into the component |
| `component_received_event_bytes_total` | estimated in-memory size of those events |
| `component_received_bytes_total` | raw bytes off the wire (sources) |
| `component_sent_events_total` | events emitted onward |
| `component_sent_event_bytes_total` | estimated size emitted |
| `component_sent_bytes_total` | raw bytes on the wire (sinks) |
| `component_errors_total` | tagged `error_type` and `stage` |
| `component_discarded_events_total` | intentional drops |
| `utilization` | gauge 0–1: fraction of time busy vs waiting |
| `buffer_events` / `buffer_byte_size` | current buffer depth |
| `buffer_received_events_total` / `buffer_sent_events_total` | buffer throughput |
| `buffer_discarded_events_total` | `when_full = drop_newest` losses |
| `adaptive_concurrency_limit` | ARC's current limit (histogram/gauge) |
| `adaptive_concurrency_in_flight` | requests in flight |
| `adaptive_concurrency_observed_rtt` | measured RTT |
| `adaptive_concurrency_averaged_rtt` | the EWMA the controller uses |

Points worth internalising:

- **Three byte counters, distinguished deliberately**: raw wire bytes, estimated in-memory event size,
  and event counts. They answer different questions (network cost, memory pressure, throughput) and
  conflating them makes capacity planning impossible. Note `*_event_bytes_total` is explicitly an
  *estimate* (`ByteSizeOf`), and Vector documents it as such.
- **`component_errors_total{error_type, stage}`** — errors are dimensioned by classification and by
  pipeline stage, which is the telemetry side of §6.4. This is why classification must carry a reason
  string.
- **`utilization`** — already argued in §8.5. The highest-value metric in the system.
- **ARC's internals are exposed**, so an adaptive controller is auditable rather than magic. If canal
  builds any closed-loop controller, its state must be a metric.

Sinks and sources with an inherent lag concept expose their own metrics (e.g. Kafka consumer lag), but
because there is no core checkpoint model (§3.1) there is **no generic lag or progress metric** — a
direct consequence of the missing abstraction, and one of the strongest arguments for canal making
checkpointing core: `canal_source_lag_seconds` and `canal_snapshot_progress_ratio` are only possible
if position and phase are core concepts.

### 11.3 Telemetry is exposed as sources

`internal_metrics` and `internal_logs` are **ordinary Vector sources**. Vector's own telemetry flows
through Vector's own pipeline and out to any sink.

```toml
[sources.internal]
type = "internal_metrics"

[sinks.prom]
type = "prometheus_exporter"
inputs = ["internal"]
```

This is elegant and nearly free: no separate metrics subsystem, no separate exporter matrix, and
operators use the routing/filtering/aggregation tools they already know. A `prometheus_exporter` sink
is how you get a `/metrics` endpoint; a `datadog_metrics` sink is how you ship to Datadog. **Canal
should do exactly this** — internal telemetry as a built-in source — because it makes the observability
matrix a consequence of the sink matrix instead of a second implementation.

### 11.4 The GraphQL API, `vector top`, and `vector tap`

```toml
[api]
enabled = true
address = "127.0.0.1:8686"
playground = true
```

A GraphQL endpoint with **queries for topology/config and subscriptions for live metrics**. Roughly
(RECALL: medium on field names): `health`, `uptime`, `components` (paginated, with each component's
kind, type, and its input/output edges), `componentsAdded`/`componentsRemoved` subscriptions for
reload events, and throughput/total subscriptions such as
`componentReceivedEventsThroughputs`, `componentSentEventsTotals`,
`outputEventsByComponentIdTotals`.

Three things this gets right for canal's frontend requirement:

1. **Subscriptions, not polling.** A live metrics UI over a subscription is far less code than a
   polling loop and gives correct rate calculation server-side. Canal's frontend goal ("live metrics")
   should be a push channel (SSE or websocket) from the start.
2. **The API serves the topology graph, not a fixed stage list.** Components and their edges are
   queried as data. This is canal's R1 (topology is data, never schema) implemented — and the direct
   contrast with the abandoned attempt's `minItems: 8, maxItems: 8`. Vector's API can describe a
   two-component pipeline or a forty-component DAG with the same schema.
3. **`vector top`** — a terminal dashboard consuming that same GraphQL API. Because the API is the
   only interface, the TUI and any web UI see identical data and cannot diverge. **Canal should build
   its CLI status view on the same API the frontend uses**, for exactly this reason: two consumers of
   one API keep the API honest, whereas a CLI reading internal state directly lets the API rot.

**`vector tap` is the feature to steal outright.** It attaches to *any* component's output by
id/glob pattern and streams live sampled events, over a GraphQL subscription:

```
vector tap 'my_transform*' --outputs-of my_source --limit 100 --interval 500
```

This is the single best debugging affordance in the system: an operator can see the actual records
flowing at any edge of a running pipeline, without redeploying, without adding a debug sink, without
log statements. It is only possible because (a) events have a canonical serialisable form (§2.5), (b)
every edge is identified (`component_id` + `output`), and (c) the fanout supports live subscriber
addition (§12.3). **For canal this is a killer feature and the three prerequisites are all things
canal wants anyway** — so it should be designed in, not bolted on. An operator debugging "why is the
wrong data arriving in my warehouse" needs to see the record at each hop, and canal's design rules'
insistence on an honest UI is served much better by "here is the actual record" than by any badge.

---

## 12. Deployment

### 12.1 One binary, two roles, by convention only

Vector ships a single static binary. Deployment topology is expressed as two *documented roles*, not
two modes or two builds:

- **Agent** — one per host/node (or a k8s DaemonSet), collects local data (files, journald, host
  metrics, kubernetes logs), does minimal processing, forwards to an aggregator via the `vector`
  sink.
- **Aggregator** — a horizontally scaled tier (k8s Deployment/StatefulSet) that receives from agents
  via the `vector` source, does the expensive transforms and enrichment, and fans out to the real
  sinks.

The binary is identical; the *config* differs. There is no `--mode` flag and no separate control-plane
component. Local development is the same binary with a small config, plus `vector validate`,
`vector top`, and a `test` framework (`[[tests]]` blocks in config, with `inputs` and
`outputs.conditions`, run by `vector test`) that unit-tests transform logic without any external
systems.

**This directly serves canal's "standalone single-binary for local dev AND enterprise scale"
requirement, and the lesson is that the difference should be configuration, not a build or a mode
flag.** The corollary Vector demonstrates: keep every external dependency optional. Vector needs
nothing but a `data_dir` to run. Canal's dev mode should require no Postgres, no Kafka, no etcd — the
buffer and checkpoint stores must have local-disk implementations behind the same interface (which is
also what canal's design rules mean by keeping remote drivers as conformance targets rather than
shipped dependencies).

### 12.2 The coordination gap — Vector's largest architectural limitation

**Vector has no clustering, no work assignment, no rebalancing, no shared state store, and no leader
election.** Each process is independent and knows only its own config.

Consequences:

- **Horizontal scale = run N independent processes behind a load balancer.** This works for the
  aggregator tier because it is stateless: agents connect to a service address, requests distribute,
  each aggregator buffers and sends independently.
- **It does not work for stateful pull sources.** Two Vector instances with the same `file` source
  config both read the same files and you get duplicates. The workaround is *structural*: partition by
  deployment. `kubernetes_logs` works because it is a DaemonSet and each pod only sees its own node's
  files — the partitioning comes from k8s, not from Vector.
- **The one exception is `kafka`,** where the source delegates partition assignment and rebalancing to
  the Kafka consumer group protocol. So Vector *can* scale a stateful source horizontally, but only by
  borrowing someone else's coordinator.
- **Scaling a stateful tier is therefore manual and error-prone**, and autoscaling it is unsafe. This
  is a recurring, well-known community complaint.
- **Disk buffers pin a process to a volume.** An aggregator with a disk buffer cannot be freely
  rescheduled without either a StatefulSet+PVC or accepting the loss of buffered data. Vector's docs
  address this and it is a genuine operational sharp edge.

Vector's answer for the enterprise control plane was to put it *outside* the OSS binary (Datadog
Observability Pipelines / the short-lived `enterprise` config block, §13). So there is no prior art
here to copy — only a clear statement of the requirement Vector declined to meet.

**For canal this is the most important gap, because canal's end-state explicitly includes
"horizontal, k8s, multi-worker, coordinated".** Canal cannot follow Vector here. What canal must add,
and what Vector's absence tells you about the shape of it:

- **A partition/assignment concept in the source interface.** A source must be able to enumerate its
  work units (files, tables, shards, key ranges) and be *assigned a subset*, rather than assuming it
  owns everything. This is the thing Vector's `Run(ctx)`-shaped source cannot express, and adding it
  later would be a breaking interface change — so it belongs in the first version of the interface
  even if the first coordinator is "one worker, all partitions".
- **A coordination interface with a local implementation.** Leader election, assignment and checkpoint
  storage behind one interface, with a single-process in-memory/local-disk implementation for dev mode
  and a real one (etcd/k8s leases/Postgres) for enterprise mode. Same interface, so the data path
  never knows which is in use.
- **Checkpoints keyed by `(pipeline, source, partition)`**, stored in that shared store rather than in
  a local file, so a partition can move between workers. This is exactly why §3.1's per-connector
  local-file checkpointing is inadequate for canal: a checkpoint in a file on a pod's local disk cannot
  be reassigned.
- **Disk buffers must be understood as worker-affine state** and their interaction with rescheduling
  decided explicitly, not discovered in production.

### 12.3 Topology reload

The one distributed-ish thing Vector does well: **config reload without process restart** (SIGHUP or
`--watch-config`).

The model (RECALL: medium-high on `ConfigDiff`/`Difference`, medium on exact names):

```rust
pub struct ConfigDiff {
    pub sources: Difference,
    pub transforms: Difference,
    pub sinks: Difference,
}

pub struct Difference {
    pub to_remove: HashSet<ComponentKey>,
    pub to_change: HashSet<ComponentKey>,
    pub to_add: HashSet<ComponentKey>,
}
```

The sequence:

1. Load and fully validate the new config (§7.3). **If validation or build fails, the reload is
   abandoned and the existing topology keeps running untouched.** Failure is non-destructive.
2. Diff against the running topology to get add/change/remove sets.
3. Build the new and changed components — *before* tearing down the old ones, except where a
   `resources()` conflict (e.g. the same TCP port) forces the old one to be shut down first. This
   ordering constraint is exactly what `resources()` exists for.
4. Re-wire edges. This is the interesting part: `Fanout` — the object that copies a component's output
   to its consumers — accepts control messages on a channel:

   ```rust
   // RECALL: medium.
   pub enum ControlMessage {
       Add(ComponentKey, BufferSender<EventArray>),
       Remove(ComponentKey),
       /// Temporarily detach a sender while its component is rebuilt, then reattach.
       Replace(ComponentKey, Option<BufferSender<EventArray>>),
   }
   ```

   So **topology edges are mutable at runtime through an in-band control channel**, and a component
   that is not itself changing is never restarted just because its neighbours changed. Unaffected
   components keep running with their buffers intact.
5. Old components are shut down; their buffers drain.

**Two ideas to steal.** First, *validate-and-build-then-swap, with failure leaving the old topology
running* — a bad config edit must never take a running pipeline down. Second, *edges as runtime
control messages*, which is R1 taken to its conclusion: if topology is data, then changing the data
should not require restarting the components. For canal, where a user editing a pipeline in the
frontend is a first-class flow, non-destructive reload is a requirement rather than a nicety, and it
is much easier to build in from the start than to retrofit.

Documented limitations, honestly: not everything is hot-reloadable (some global options require a
restart), reload had a class of bugs where a partially-applied reload left inconsistent state, and
`--watch-config` picking up a half-written file is a real hazard (mitigated by writing atomically).

---

## 13. What they got right / what they got wrong

### 13.1 Got right

1. **Config-object trait with a `build()` factory.** Deserializable config struct → validated →
   built runtime object. Clean separation, testable, and it is what makes config-time validation and
   non-destructive reload possible.
2. **Buffers as a first-class configurable per-sink stage with a named rejection policy.** No
   unbounded queue is expressible. Chainable for tiering. Losses are counted.
3. **End-to-end acknowledgements via reference-counted finalizers with worst-wins status merging.**
   Correct through fan-out, through merging transforms, and through arbitrary graph depth, with a
   connector-facing surface of one function (`event_status()`). Forgetting to settle produces
   "don't checkpoint", which is the safe default.
4. **The durability boundary is configurable and the ack semantics follow it automatically** (disk
   buffer takes ownership of finalizers).
5. **Static edge typing at config load** (`Input`/`outputs()` + `DataType` + schema `Requirement`),
   so a registry-based plugin model can't produce silently-broken graphs.
6. **`resources()` conflict detection** — declaring exclusive resources so the core can order
   teardown/startup during reload and reject conflicting configs.
7. **Cross-cutting concerns as embedded config structs** (`batch`, `request`, `buffer`, `encoding`,
   `tls`, `acknowledgements`) with generic default-carriers. Declared once, identical everywhere,
   documented once.
8. **Config schema generated from the code, with doc comments as the documentation source.** One
   place to change, no drift, and the generated JSON Schema is exactly what a generic UI needs.
9. **Adaptive request concurrency** with its controller state exposed as metrics.
10. **The encoding/framing split** (§14) — serialization is composed, config-driven and connector-
    independent on both the source and sink side.
11. **Shutdown propagating by input-channel closure**, with a two-phase signal (begin / complete) and
    a global deadline. No ordering logic needed anywhere.
12. **Instrumentation as a normative spec plus a validation harness.** Metric naming by pipeline
    position, `component_id` vs `component_type` separation, and the `utilization` gauge.
13. **Internal telemetry exposed as sources**, so observability reuses the data path.
14. **One GraphQL API serving both `vector top` and any web UI**, describing the topology as a graph.
15. **`vector tap`** — live event sampling at any edge of a running pipeline.
16. **`vector validate` / `vector test` / `vector generate`** — offline config checking, in-config
    unit tests for transform logic, and scaffolding.
17. **Non-destructive reload with runtime-mutable edges.**
18. **Honest per-component delivery-guarantee documentation** rather than a blanket claim.

### 13.2 Got wrong — concrete, documented pain

1. **The fixed `Event` enum is a core-edit boundary.** Adding `Trace` alongside `Log` and `Metric`
   required touching the event model, `DataType`, `EventArray`, every codec, the native protobuf
   schema, buffer encoding, and the type-compatibility matrix. It is the exact opposite of "implement
   the interface, register it, done". For canal, whose constraint 4 is precisely this property: **do
   not enumerate record kinds in a closed core enum.** One envelope with an open, dynamic body (§2.4)
   plus capability flags is the extensible shape.
   The secondary cost is combinatorial: every transform and sink must declare which kinds it accepts,
   many combinations are unsupported, and users hit "sink X does not accept metrics" as a config error
   with no alternative.

2. **`Result<(), ()>` at the component boundary throws away all error information.** No supervision
   tree, no per-component restart, no way for the topology to distinguish clean shutdown from
   permanent failure from transient outage. A dying component generally means the process is going
   down. Errors reach operators only via out-of-band telemetry. Canal must return typed errors from
   component entry points and must decide its supervision policy explicitly.

3. **No checkpoint abstraction at all** (§3.1). Every source reinvents position persistence; there is
   no generic lag metric, no generic progress view, no operator control to reset or rewind a position,
   no versioning story for position state, and — fatally for a distributed deployment — checkpoints
   live in local files that cannot be reassigned to another worker (§12.2).

4. **No snapshot / backfill model** (§4). Not a bug for a telemetry router, but a total absence for
   canal's purposes, and the `Run(ctx)`-shaped source interface actively invites every connector to
   solve it privately and inconsistently.

5. **No coordination, clustering, or work assignment** (§12.2). Stateful sources cannot be scaled
   horizontally except by borrowing Kafka's consumer group or by partitioning at the deployment layer.
   Autoscaling a stateful tier is unsafe. Disk buffers pin a process to a volume. This is the most
   frequently raised architectural limitation.

6. **Acknowledgements degrade silently** (§9.1). Enable acks, wire them through a source or sink that
   doesn't support them, and you get best-effort delivery with no warning — even though every input to
   that determination is known at config time. Compounded by the option's history: it was off by
   default, the global vs per-sink resolution changed, and which components support it is a docs
   lookup rather than a validated property. This is the most dangerous wart in the system and it is
   trivially fixable by validating a declared pipeline guarantee against component capabilities.

7. **Memory buffers are the default**, so the out-of-the-box configuration loses buffered data on
   crash. Correct for the telemetry use case, wrong as a default for a data-movement tool. Canal
   should default to the durable option and make the fast/lossy one an explicit opt-in — the
   R4 direction.

8. **Disk buffer v1 → v2 rewrite.** v1 was built on an embedded key-value store (LevelDB), which was a
   poor fit for an append-and-truncate workload; it was replaced by a purpose-built segmented log
   (`disk_v2`), which then went through its own period of data-loss and corruption bugs before
   stabilising, with v1 deprecated and eventually removed. Two lessons: a KV store is the wrong
   primitive for a buffer, and the buffer format needs a version marker and an explicit
   compatibility predicate from day one (§10.4) — which is what `disk_v2` has and `disk_v1` didn't.
   Also, `max_size` accounting in bytes-including-framing-overhead was a persistent source of
   operator confusion.

9. **Metric renaming churn.** The whole telemetry surface was renamed (e.g. `processed_events_total`
   → `component_received_events_total` / `component_sent_events_total`, and the byte counters split
   three ways) in the 0.1x series, breaking every dashboard and alert in the field, with a migration
   guide as the mitigation. The end state is much better than the start; the cost was paid by users.
   **Canal should pay this cost up front by writing the instrumentation spec before the connectors**
   (§11) — the metric names are a public API and renaming them is a breaking change.

10. **A large transform catalog was deprecated in favour of one programmable transform.**
    `add_fields`, `remove_fields`, `rename_fields`, `coercer`, `json_parser`, `regex_parser`,
    `grok_parser`, `logfmt_parser`, `tokenizer`, `split`, `merge`, `concat`, `ansi_stripper` and
    others were all superseded by `remap` (VRL), and `swimlanes` was renamed to `route`. That is a
    large deprecation wave and a clear negative result: **a catalog of narrow, fixed transforms is the
    wrong bet; one well-designed programmable transform plus a few genuinely structural ones (route,
    filter, reduce, aggregate, dedupe) is the right one.** Relevant to canal's "transform chains"
    goal — resist building twenty transforms.

11. **The `encoding` config was reshaped mid-life.** Old sinks had an ad-hoc `encoding` block with
    `codec` plus bolted-on `only_fields`/`except_fields`/`timestamp_format`, and behaviour differed
    per sink. The clean `encoding` + `framing` split (§14) was a breaking migration; both shapes
    coexisted across many releases, and which sinks supported which was inconsistent. The final design
    is excellent — it just should have been the first design.

12. **Global `log_schema` is global mutable state that connectors implicitly depend on** (§5.1), and
    the `LogNamespace` migration out of it (§5.2) is long-running, dual-coded, and a documented source
    of user confusion. Canal should start in the equivalent of the new namespace and never build the
    legacy one.

13. **Schema support is partially wired and vendor-shaped.** `schema.enabled` is effectively
    experimental, semantic meanings are used by few sinks, and there is no runtime discovery or drift
    handling. Related: `EventMetadata` carries a `datadog_origin_metadata`-style field — **a specific
    vendor's concern in the core event type**, which is precisely canal's constraint 1 being violated.
    Also the short-lived `enterprise` config block for Datadog Observability Pipelines, added to the
    OSS binary and later removed. **Lesson: the core record type is where vendor coupling goes to
    hide.** Guard it explicitly; a per-connector metadata namespace (§2.3) is the pressure valve that
    keeps vendor fields out of core types.

14. **Backpressure was undiagnosable before `utilization` existed.** Everything is slow, nothing says
    why. The metric was added in response to real operator pain. Ship it from the start.

15. **Registration/deserialization mechanism churn.** The `typetag`-era approach gave poor error
    messages for unknown component types and did not support enumerating the config surface, which
    was part of the motivation for the `vector-config`/`#[configurable_component]` work. (Low
    confidence on the details — flagged as unverified.)

16. **The `dedupe` transform is an in-memory LRU described in user-facing terms that invite
    correctness assumptions.** It is documented as noise reduction, but it is the same shape as the
    anti-pattern in canal's R5. Canal should not ship a component named `dedupe` that isn't durable
    and scoped.

---

## 14. Serialization: the encoding + framing split

Called out separately because it is the cleanest single abstraction in the codebase and it maps
directly onto canal's pluggable-codec requirement.

### 14.1 The split

Serialization is decomposed into **two independent, independently configurable stages**:

- **Encoding / serializing** — one event → bytes. `encoding.codec`: `json`, `text`, `logfmt`, `avro`,
  `protobuf`, `csv`, `gelf`, `cef`, `native`, `native_json`, `raw_message`.
- **Framing** — bytes → a delimited byte stream. `framing.method`: `newline_delimited`,
  `character_delimited` (with a `delimiter`), `length_delimited`, `bytes` (no framing).

```toml
[sinks.my_sink.encoding]
codec = "json"
only_fields = ["message", "host"]
timestamp_format = "rfc3339"

[sinks.my_sink.framing]
method = "newline_delimited"
```

And symmetrically on the source side, inverted:

```toml
[sources.my_source.framing]
method = "character_delimited"
delimiter = ","

[sources.my_source.decoding]
codec = "json"
```

### 14.2 The traits

```rust
// RECALL: medium on exact signatures; high on the two-stage structure and the
// tokio_util::codec Encoder/Decoder basis.
// lib/codecs/

/// Event -> bytes.
pub trait Serializer: tokio_util::codec::Encoder<Event, Error = BoxedSerializerError> { }

/// Bytes -> framed byte stream. Encodes the *delimiters* only — note the `()` item type.
pub trait Framer: tokio_util::codec::Encoder<(), Error = BoxedFramingError> { }

/// Bytes -> events. NOTE: one frame may yield MANY events.
pub trait Deserializer: DynClone + std::fmt::Debug + Send + Sync {
    fn parse(
        &self,
        bytes: Bytes,
        log_namespace: LogNamespace,
    ) -> vector_common::Result<SmallVec<[Event; 1]>>;
}

/// Byte stream -> frames.
pub trait Framer2: tokio_util::codec::Decoder<Item = Bytes, Error = BoxedFramingError> { }
```

(The last name is invented for exposition; the real decoding-side framer is also called `Framer` in
its own module.)

Composition: `codecs::encoding::Encoder<Framer>` implements
`tokio_util::codec::Encoder<Event>` by running the serializer into a `BytesMut` and then letting the
framer add delimiters. `codecs::Decoder` implements
`tokio_util::codec::Decoder<Item = (SmallVec<[Event; 1]>, usize)>` — frames, then parses, returning
the events plus the byte count consumed (needed for the `component_received_bytes_total` metric).

Each codec also has a config struct with `build()`, plus — importantly — `input_type() -> DataType`
and `schema_requirement() -> Requirement`, so **the codec participates in config-time type
validation**: configuring the `avro` codec on a sink fed metrics is a startup error, not a runtime
one.

### 14.3 Why this is the right decomposition

1. **A connector never writes serialization code.** A sink implements *transport* — how to make an
   HTTP request, how to write to a socket, how to call an SDK — and receives an already-encoded
   payload. A source implements *transport* — how to get bytes from a socket or a file — and hands
   bytes to the framework. `RequestBuilder::encode_events` (§8.3) is supplied by the framework for
   the common case.
2. **N codecs × M connectors with no multiplication.** Add a codec and every connector gains it. Add
   a connector and it gains every codec. This is the property canal needs for constraint 4 to hold
   for serialization as well as for connectors.
3. **Framing is genuinely orthogonal to encoding**, and conflating them (as "the JSONLines codec")
   creates a codec per pairing. Newline-delimited JSON, length-prefixed protobuf, comma-delimited raw
   text are all just two independent choices.
4. **It makes the source side symmetric.** The same two-stage split, reversed, means a TCP source and
   a file source share all of their byte→event logic. A new transport gets every parser for free.
5. **One frame → many events is in the signature** (`SmallVec<[Event; 1]>`), which correctly handles
   a JSON array in one frame or a multiline record, and the `SmallVec` sizing optimises the
   overwhelmingly common one-event case without special-casing it.
6. **Compression is a third, separate stage** (`compression = "gzip" | "zstd" | "none"`), applied
   after encoding and framing, again as a shared config block.

### 14.4 The canal translation

```go
// Sketch for canal — not Vector code.

// Serializer: one record -> bytes. Registered in the codec registry by name.
type Serializer interface {
    Name() string
    Serialize(dst []byte, rec Record) ([]byte, error)  // append-style, reuses dst
    ContentType() string
}

// Deserializer: one frame -> zero or more records.
type Deserializer interface {
    Name() string
    Deserialize(frame []byte, dst []Record) ([]Record, error)
}

// Framer: delimit an encoded payload / split a byte stream.
type Framer interface {
    Name() string
    Frame(dst []byte, payload []byte) ([]byte, error)
}
type Deframer interface {
    Name() string
    // Returns the next frame and bytes consumed, or (nil, 0, nil) if more data is needed.
    NextFrame(buf []byte) (frame []byte, n int, err error)
}
```

Note the Go-specific opportunity: `bufio.Scanner`'s `SplitFunc` is almost exactly `Deframer`, so the
deframing side has an idiomatic precedent (`func(data []byte, atEOF bool) (advance int, token []byte,
err error)`) worth matching rather than inventing.

And the sink-side separation that keeps connectors thin, generalising `RequestBuilder`:

```go
// Sketch for canal — not Vector code.
// The connector supplies partitioning, request construction, and classification.
// The framework supplies batching, encoding, framing, compression, concurrency,
// rate limiting, retry, timeout, ack settlement, and telemetry.

type Partitioner interface {
    Partition(rec Record) (key string, err error)
}

type RequestBuilder[Req any] interface {
    // Build a transport request from an already-encoded, already-framed, already-compressed payload.
    BuildRequest(key string, payload []byte, meta BatchMeta) (Req, error)
}

type Service[Req, Resp any] interface {
    Call(ctx context.Context, req Req) (Resp, error)
}

type Classifier[Resp any] interface {
    // The single function that drives both retry policy and end-to-end acknowledgement.
    Classify(resp Resp, err error) (Class, reason string)
}
```

That is the shape to aim for: a new sink is a partitioner (often trivial), a request builder, a
`Call`, and a `Classify`. Nothing else.

---

## Verification worklist

Nothing in this file was checked against primary source (see the banner). When the network is
available, fetch these in roughly this order — they are where the load-bearing signatures live.

Core interfaces (§1):
- `src/config/source.rs`, `src/config/sink.rs`, `src/config/transform.rs` — the three config traits,
  the `*Context` structs, `VectorSink`, `Healthcheck`, `Input`, `SourceOutput`/`TransformOutput`.
- `src/config/unit_test/`, `src/config/mod.rs` — `Config`, `ConfigDiff`, `Difference`, `Resource`.
- `src/sinks/util/mod.rs` and `src/sinks/util/builder.rs` — `SinkBuilderExt`, the combinator chain.
- `src/sinks/util/service.rs`, `src/sinks/util/retries.rs` — `TowerRequestConfig`, `RetryLogic`,
  `RetryAction`, layer order.
- `src/sinks/util/adaptive_concurrency/` — ARC controller, and
  `rfcs/2020-04-06-1858-automatically-adjust-request-limits.md` for the design rationale and
  parameter defaults.
- `src/sinks/util/request_builder.rs` — `RequestBuilder`, `encode_events`, `split_input`.
- `src/sinks/util/driver.rs` — `Driver`, `DriverResponse`, `Finalizable`, where `update_status` is
  called.

Record model and acks (§2, §3, §9):
- `lib/vector-core/src/event/mod.rs`, `.../log_event.rs`, `.../metadata.rs`, `.../array.rs`.
- `lib/vector-common/src/finalization.rs` — `EventStatus`, `BatchStatus`, `EventFinalizer(s)`,
  `BatchNotifier`, the merge rule, the `Drop` impls.
- `proto/` — the native protobuf event schema.
- `src/sources/kafka.rs` and `src/sources/file.rs` — the two real checkpointing implementations;
  `lib/file-source/` for `Checkpointer` and `FileFingerprint`.

Buffers (§8):
- `lib/vector-buffers/src/lib.rs` — `Bufferable`, `Encodable`, `can_decode`.
- `lib/vector-buffers/src/config.rs` — `BufferType`, `WhenFull`, defaults, and whether `Overflow`
  exists as I claim.
- `lib/vector-buffers/src/variants/disk_v2/` — segment/ledger design.

Config and registry (§7, §10):
- `lib/vector-config/`, `lib/vector-config-macros/` — `Configurable`, `#[configurable_component]`.
- Check specifically **how polymorphic deserialization is done today** (`typetag` vs generated
  enums vs `enum_dispatch`) and how `inventory` is used — this is my lowest-confidence area.

Topology, telemetry, API (§11, §12):
- `src/topology/builder.rs`, `src/topology/running.rs`, `src/topology/fanout.rs` —
  `ControlMessage`, transform fusion, reload sequencing.
- `docs/specs/component.md`, `docs/specs/instrumentation.md` — the normative metric contract.
- `src/api/schema/` — GraphQL queries and subscriptions; `src/top/`, `src/tap/`.
- `lib/codecs/src/encoding/mod.rs`, `lib/codecs/src/decoding/mod.rs` (§14).

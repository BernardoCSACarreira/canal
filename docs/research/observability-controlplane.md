# Prior art: observability, control plane and the enterprise/standalone deployment split

> Systems surveyed: **OpenTelemetry semantic conventions (messaging)**, **Prometheus/OpenMetrics naming
> practice**, **Kafka Connect REST API**, **Conduit** (HTTP + gRPC), **Vector** (GraphQL API +
> instrumentation spec), **Airbyte** (config API, `web_backend` read model, Airbyte Protocol control/trace
> messages, secret coordinates), **Debezium** (JMX metric groups, snapshot progress), **Apache Flink FLIP-33**
> (standardised connector metrics — the best prior art for "what is lag"), **OpenTelemetry Collector**
> (internal metrics), **Benthos / Redpanda Connect** (streams API, metric mapping), **Strimzi** and the
> **Flink Kubernetes Operator** (CRD patterns), **client-go leaderelection / etcd concurrency / Postgres**
> (coordination stores).
>
> Companion document: `docs/research/kafka-connect.md`. Kafka Connect's plugin API, `ConfigDef`, JMX metric
> groups, standalone-vs-distributed storage swap, `KafkaConfigBackingStore` and KIP-415 rebalancing are
> analysed there from source and are **not** repeated here. This document covers the control-plane surface,
> the metric contract, and the deployment seam.

---

## ⚠ Verification status — read this first

**Network tools were unavailable for most of this research pass** — roughly 50 consecutive `WebFetch` / `curl`
attempts over ~25 minutes returned `claude-sonnet-5 is temporarily unavailable, so auto mode cannot determine
the safety of WebFetch right now`. Tooling recovered intermittently near the end, and I used those windows on
the highest-value sources. **Some of this document is verified against primary source and some is recalled, and
the two are marked distinctly:**

| Marker | Meaning |
|---|---|
| **VERIFIED** | Fetched from primary source during this session. Quotes are verbatim from that fetch. |
| **[R]** | *Recalled.* Name, signature or quote written from memory. **Must be confirmed before it is relied on.** Also listed in the `unverified` return field. |
| **[R!]** | Recalled, high confidence in the *substance*; exact spelling/casing unconfirmed. |
| **[S]** | Second-hand: verified from source in the earlier Kafka Connect pass and quoted from `docs/research/kafka-connect.md`, which states every signature in it was read from source. |
| **[C]** | *Canal design recommendation.* My own reasoning. Nothing to verify — judge it on merit. |

**Verified in this pass** (primary source, verbatim quotes in the relevant section):

- OTel messaging **metrics** semconv — all four instruments, attribute requirement levels, stability (§11.1)
- **Prometheus** metric-and-label naming rules and the cardinality warning (§11.3)
- **Vector** `docs/specs/instrumentation.md` — naming template and the bounded-cardinality requirement (§11.3)
- **Flink FLIP-33** — the full source/sink metric table and the verbatim lag definitions (§11.5)
- **Conduit** `proto/api/v1/api.proto` — services, RPCs, `Pipeline.State`, connector state (§1.2)
- **client-go `leaderelection`** — the fencing and clock-skew caveats, `LeaderElectionConfig` (§12.5)
- **Flink Kubernetes Operator** CRD reference — nonces, `lastReconciledSpec`/`lastStableSpec`, states (§12.6)

**Not verified in this pass** (still `[R]`/`[R!]`): OTel messaging *spans* and the attribute registry; the
Airbyte protocol YAML and config/`web_backend` API (concepts corroborated via Airbyte docs search only, exact
enum values not fetched); Debezium's JMX attribute names (debezium.io returned HTTP 403); Strimzi's CRD field
names; Kafka Connect's `ConnectorStateInfo` JSON and KIP-297 details; Vector's GraphQL subscription field names;
Benthos/Redpanda metric names; OTel Collector internal metric names; the Prometheus ~10-cardinality rule of
thumb.

The **`[C]` recommendations do not depend on the network** — they are design argument, and they are the part of
this document that was actually asked for ("concrete, implementable recommendations for canal, not a literature
review").

**Do not copy an `[R]`-marked identifier into canal source code without fetching the upstream page first.**

### Exact URLs to re-verify against (ordered by remaining value)

Note two that failed with hard HTTP errors and need a different access route: the Airbyte protocol YAML 404s at
the path below (it has moved — search `airbyte_protocol.yaml` in `airbytehq/airbyte` under
`airbyte-protocol/models/src/main/resources/airbyte_protocol/`), and `debezium.io` returns 403 to WebFetch (use
the `debezium/debezium` GitHub docs source instead).

```
https://raw.githubusercontent.com/open-telemetry/semantic-conventions/main/docs/messaging/messaging-metrics.md
https://raw.githubusercontent.com/open-telemetry/semantic-conventions/main/docs/messaging/messaging-spans.md
https://raw.githubusercontent.com/open-telemetry/semantic-conventions/main/model/messaging/registry.yaml
https://raw.githubusercontent.com/vectordotdev/vector/master/docs/specs/instrumentation.md
https://raw.githubusercontent.com/vectordotdev/vector/master/lib/vector-api-client/graphql/  (schema + subscriptions)
https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics
https://raw.githubusercontent.com/ConduitIO/conduit/main/proto/api/v1/api.proto
https://raw.githubusercontent.com/ConduitIO/conduit-connector-protocol/main/proto/connector/v1/  (pconnector)
https://raw.githubusercontent.com/airbytehq/airbyte-platform/main/airbyte-api/  (config.yaml / server-api)
https://raw.githubusercontent.com/airbytehq/airbyte-protocol/main/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-monitoring
https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/main/packaging/install/cluster-operator/  (CRDs)
https://raw.githubusercontent.com/apache/flink-kubernetes-operator/main/docs/content/docs/custom-resource/reference.md
https://pkg.go.dev/k8s.io/client-go/tools/leaderelection
https://prometheus.io/docs/practices/naming/
https://prometheus.io/docs/practices/instrumentation/
https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/
```

---
## 1. Core interfaces

For this dossier "core interfaces" means two things: (a) the **control-plane API surface** each system exposes
to a frontend, and (b) the **in-process observability interface a connector sees**. The data-path interfaces
are covered in the sibling dossiers.

### 1.1 Kafka Connect — REST, resource-shaped, polling-only

Endpoint inventory (verified in the earlier pass, `[S]`): `GET /connectors`, `GET /connectors/{n}`,
`GET /connectors/{n}/status`, `GET /connectors/{n}/tasks`, `GET /connectors/{n}/tasks/{id}/status`,
`GET /connectors/{n}/topics`, `GET /connectors/{n}/offsets` (KIP-875), `GET /connector-plugins`,
`GET /connector-plugins/{p}/config`, `PUT /connector-plugins/{p}/config/validate`,
`PUT .../pause|resume|stop`, `POST .../restart`, `PATCH|DELETE .../offsets`. **No HTTP metrics endpoint at
all** — metrics are JMX-only.

The status document (`ConnectorStateInfo`) shape `[R]`:

```json
GET /connectors/local-file-source/status
{
  "name": "local-file-source",
  "connector": { "state": "RUNNING", "worker_id": "10.0.0.1:8083" },
  "tasks": [
    { "id": 0, "state": "FAILED", "worker_id": "10.0.0.1:8083",
      "trace": "org.apache.kafka.connect.errors.ConnectException: ...\n\tat ..." }
  ],
  "type": "source"
}
```

Three things about that document are worth naming precisely, because they are the *entire* reason Connect's
status API is usable despite being minimal:

1. **`trace` carries the stacktrace inline.** The single highest-value field. An operator answers "why is it
   broken" without shell access to a worker. `[S]`
2. **`worker_id` is on every element.** Status is *attributed to a process*. That is what makes a
   multi-worker deployment debuggable.
3. **The document is served by any worker for any connector**, because status is replicated through the
   compacted `connect-status` topic. `[S]` So the read path has no leader dependency.

What it is missing, and each omission is a concrete lesson `[S]`:

- no `observedGeneration` — you cannot tell whether the running task reflects the config you just `PUT`;
- no `COMPLETED` state, so a bounded job cannot report finishing;
- no progress, no lag, no throughput anywhere in the status document (all of that is in JMX);
- eventual consistency with no version marker: `GET /status` immediately after `POST /connectors` commonly
  404s or reports `UNASSIGNED`, and there is nothing in the response that says "this is stale". `[R!]`
- **connector configs are returned verbatim, secrets included**, unless the operator used a
  `${provider:...}` indirection. `[R!]`

### 1.2 Conduit — gRPC as the source of truth, HTTP generated from it

Conduit defines its control plane in protobuf and generates the HTTP/JSON API from it with grpc-gateway, so
there is exactly one API definition. **VERIFIED** against `ConduitIO/conduit/main/proto/api/v1/api.proto`:

```
PipelineService     ListPipelines  CreatePipeline  GetPipeline  UpdatePipeline  DeletePipeline
                    StartPipeline  StopPipeline
                    GetDLQ  UpdateDLQ
                    ExportPipeline  ImportPipeline
                    PlanPipeline  ApplyPipeline           <-- declarative apply, with a plan step
ConnectorService    ListConnectors  GetConnector  CreateConnector  UpdateConnector  DeleteConnector
                    ValidateConnector                     <-- validation as its own RPC
                    InspectConnector                      <-- live record inspection
                    ListConnectorPlugins
ProcessorService    ListProcessors  GetProcessor  CreateProcessor  UpdateProcessor  DeleteProcessor
                    InspectProcessorIn  InspectProcessorOut   <-- tap before AND after a processor
                    ListProcessorPlugins
InformationService  GetInfo
PluginService       ListPlugins  (deprecated)
```

Pipeline status (verified):

```protobuf
Pipeline.State {
  Status status         // STATUS_UNSPECIFIED | STATUS_RUNNING | STATUS_STOPPED
                        // STATUS_DEGRADED    | STATUS_RECOVERING
  string error          // populated for STATUS_DEGRADED
  StoppedReason stopped_reason  // STOPPED_REASON_UNSPECIFIED | _USER | _SYSTEM
}
```

Connector state is a `oneof`: `source_state { bytes position }` or
`destination_state { map<string, bytes> positions }`. **And there are no metrics messages in the API at all** —
Conduit's metrics are Prometheus-scrape-only, which is the right split (§11.7).

Four verified details worth taking:

1. **`PlanPipeline` + `ApplyPipeline`.** A `terraform plan`/`apply` pair on the control plane: submit a desired
   spec, get back the diff *before* committing to it. This is the declarative write model §12.6 argues for,
   with the extra step that makes it safe for humans — and it is far better than Kafka Connect's
   `PUT /connectors/{n}/config` where the only way to learn the impact is to do it.
2. **`STOPPED_REASON_USER` vs `STOPPED_REASON_SYSTEM`.** "Stopped because a human asked" and "stopped because
   we gave up" are the same `status` with different meanings; encoding the difference in a separate enum
   instead of splitting the status is the right call and is exactly the honesty distinction design-rules
   demands. canal's `PhaseStopped` needs the same companion field.
3. **`STATUS_RECOVERING` is a first-class status**, distinct from `RUNNING` and `DEGRADED` — the state canal
   would reach after a lease loss or a connector restart. Worth having.
4. **`InspectConnector` / `InspectProcessorIn` / `InspectProcessorOut`.** Conduit and Vector independently
   converged on **live record tapping as a control-plane API operation**, and Conduit went further by tapping
   *both sides* of a processor. For debugging a transform chain — "what went in, what came out" — this is the
   single most useful operation the API can offer, and it costs a bounded sampling buffer plus a stream.

Notably **absent** in Conduit as in Connect: no `COMPLETED`/terminal-success status, no `observedGeneration`, no
progress or lag anywhere in the status message.

**The transplantable property is also the generation direction, not just the endpoint list**: proto → gRPC +
HTTP + OpenAPI + Go client + TS client, from one file. That is design-rule R8 ("shared constants are generated
from one source") applied to the API surface, and it structurally prevents the four-vocabulary drift R9
describes.

Conduit's connector-facing self-description is the other relevant piece — `Parameters()` on both source and
destination `[R!]`:

```go
// conduit-commons/config  — recalled, verify
type Parameter struct {
    Default     string
    Description string
    Type        ParameterType   // String, Int, Float, Bool, File, Duration
    Validations []Validation    // Required, LessThan, GreaterThan, Inclusion, Exclusion, Regex
}
type Parameters map[string]Parameter
```

Compared with Connect's `ConfigDef.ConfigKey` (14 fields, five of them purely presentational — `[S]`), this is
*deliberately smaller and worse for a UI*: there is no `group`, no `orderInGroup`, no `displayName`, no
`importance`, no `dependents`, no recommender. A frontend can render a flat form and validate types; it cannot
render a grouped, ordered, conditionally-visible form. **Canal should take Connect's field set and Conduit's
proto-generated delivery.**

### 1.3 Vector — GraphQL, read-only, and the CLI is a client of it

Vector exposes `api.enabled` / `api.address` (default `127.0.0.1:8686`) with `/graphql`, a `/playground`, and
`/health`. `[R!]` The schema is component-oriented rather than pipeline-oriented: queries over
`sources`/`transforms`/`sinks`/`components`, and **subscriptions** for live data —
`componentReceivedEventsThroughputs`, `componentSentEventsThroughputs`, `outputEventsByComponentIdPatterns`,
`componentAdded`/`componentRemoved`, `uptime`. `[R]`

Two structural facts matter far more than the field names:

1. **`vector top` and `vector tap` are clients of the same GraphQL API a browser would use.** `[R!]` The
   built-in TUI has no privileged in-process access. This is the strongest single piece of API-design
   discipline in any of these systems: **if your own CLI dashboard can only be built on the public read
   model, the read model cannot rot.** It is also the cheapest possible enforcement of design-rule R8
   ("tests assert real responses") — the CLI *is* the assertion.
2. **The API is read-only.** You cannot create a sink over it. Vector's answer to configuration is a file plus
   `--watch-config`/SIGHUP with a *partial topology diff* — it rebuilds only the components whose config
   changed. `[R!]` So Vector has a data plane and an observability plane, and **no control plane at all**;
   orchestration is delegated entirely to whatever ships the file (k8s ConfigMap, Ansible).

`outputEventsByComponentIdPatterns` deserves separate mention: it is a **live sample of actual records
flowing through a named component**, delivered over the same API as the metrics. That is `vector tap`. For a
connector framework's UI this is arguably the single highest-value debugging feature — "show me what is
actually coming out of this source right now" — and it costs a subscription plus a sampling tap.

### 1.4 Airbyte — RPC-over-POST, plus an explicit read model for the UI

Airbyte's config API is RPC-shaped, not resource-shaped: every call is a `POST` to a verb path —
`/v1/sources/create`, `/v1/sources/get`, `/v1/connections/create`, `/v1/connections/get`, `/v1/jobs/list`,
`/v1/jobs/get_debug_info`, `/v1/source_definition_specifications/get`,
`/v1/scheduler/sources/check_connection`. `[R!]`

And then, separately, `/v1/web_backend/connections/get`, `/v1/web_backend/connections/list`, … — an entire
**second API layer that exists solely to serve the frontend**. `[R!]` It returns denormalised aggregates
(connection + source + destination + latest job + schema, in one response) because the normalised resource API
was too chatty for a page render.

That split is worth taking seriously, in both directions:

- **Right:** a UI needs a *read model*, not a resource graph. One request per screen, denormalised, versioned.
- **Wrong:** it arrived as a bolt-on with its own DTOs, so the same entity now has two wire shapes — exactly
  the "one entity, two identifiers" failure design-rule R1/R9 warns about. Build the read model **first and
  deliberately**, as a distinct, named layer, rather than growing it accidentally.

The verb-path shape (`/get`, `/create`) also has a concrete downstream cost: it is very hard to put a
Kubernetes operator in front of an API that has no idempotent "here is the desired whole state of this
object" write. See §12.

### 1.5 Airbyte Protocol — status and progress as in-band messages

Airbyte's connector boundary is a subprocess speaking newline-delimited JSON `AirbyteMessage`s on stdout, with
a `type` discriminator `[R!]`: `RECORD`, `STATE`, `LOG`, `SPEC`, `CONNECTION_STATUS`, `CATALOG`, `TRACE`,
`CONTROL`.

The last three are the interesting ones for this dossier:

```
AirbyteTraceMessage   type ∈ { ERROR, ESTIMATE, STREAM_STATUS, ANALYTICS }     [R!]
  AirbyteEstimateTraceMessage    { name, namespace, type ∈ {STREAM, SYNC}, row_estimate, byte_estimate }
  AirbyteStreamStatusTraceMessage{ stream_descriptor, status ∈ {STARTED, RUNNING, COMPLETE, INCOMPLETE} }
AirbyteControlMessage type ∈ { CONNECTOR_CONFIG }                              [R!]
  AirbyteControlConnectorConfigMessage { config }
```

Three ideas here, all directly relevant to canal:

1. **Progress travels in-band on the data channel** (`ESTIMATE`) rather than being polled out-of-band. This
   is the *right* answer for an out-of-process connector — one channel, ordered with respect to the records
   it describes — and it is why canal's telemetry must be *data in the response envelope*, not a registry
   handle (§10).
2. **`row_estimate` is an estimate and is named as one.** A progress bar built on it is honest about being
   approximate. Airbyte's actual failure here is that emitting estimates is *optional* and most connectors
   don't, so the UI shows a progress bar that is silently absent or wrong — the exact
   "endpoint answered ≠ your data arrived" dishonesty design-rules calls out.
3. **`CONTROL / CONNECTOR_CONFIG` lets the connector push a config mutation back into the control plane.**
   This exists for OAuth refresh-token rotation. It is a genuinely necessary capability that almost no
   framework has, and it has a sharp security consequence: the control plane must re-run secret
   splitting/persistence on a connector-originated config write, and must never log it.

### 1.6 What a connector-facing observability interface should look like

Connect's answer, retrofitted in 4.1, is `PluginMetrics` reachable from the context, with `connector`/`task`
tags auto-applied and a `converter=key|value|header` tag for converters `[S]`. The shape is right — *the
plugin declares metrics, the core owns naming, tagging and export* — but the delivery is a live in-process
registry handle, which cannot cross a process boundary. Airbyte's shape (telemetry as messages on the data
channel) crosses a process boundary but is unusable in-process without a serialisation round-trip.

**`[C]` canal should take the union: declaration is data, emission is data, and the core owns the registry.**
See §10 for the concrete interface. The load-bearing consequence, stated once here because it constrains the
interface set now: *if out-of-process connectors are to satisfy the same interface later, a connector can
never hold a metrics registry, a logger sink, or a status callback. All three must be values it returns.*

---
## 2. Record model — the observability obligations of the envelope

This dossier's stake in the record model is narrow but non-negotiable, and it has to be settled *now* because
design-rule R2 says the envelope is decided first and because none of it can be added later without a wire
break.

**Four fields on the envelope exist only to make observability possible.** No system surveyed has all four,
and every one of them regrets a missing one.

| Field | Why it must be on the envelope | Prior art |
|---|---|---|
| `EventTime` (tri-state: set / explicitly absent) | The only input to event-time lag. If it is a bare `time.Time`, "no event time" and "epoch" are indistinguishable and every lag metric silently reads 56 years. | FLIP-33's `currentFetchEventTimeLag` / `currentEmitEventTimeLag` are *defined* as `t - eventTime` `[R!]`; Debezium exposes `MilliSecondsBehindSource` `[R!]` |
| `IngestTime` (core-stamped, never connector-stamped) | The clock-skew-immune half of every latency measurement. End-to-end latency = `sinkAckTime − IngestTime` and needs no trust in the source's clock. | — |
| `Stream` identity (the same value the checkpoint keys on) | Per-stream status in the read model, and per-stream progress, without the core parsing the position. | Connect's `sourcePartition` `[S]`; Airbyte's `stream_descriptor` `[R!]` |
| `TraceContext` slot in metadata (W3C `traceparent`) | Distributed tracing across source → transform → sink → external system, and a DLQ entry that can point at a trace. Retrofitting this means changing every codec. | OTel messaging conventions put trace context in message headers `[R!]` |

Plus two derived-but-worth-storing values: **serialised byte size** (so `bytes_read`/`bytes_written` don't
require re-encoding to measure) and the **failure classification** slot (design-rules' taxonomy:
`transient_upstream`, `transient_internal`, `permanent_upstream`, `permanent_mapping`,
`permanent_contract`, `duplicate_idempotent_success`, `clock_skew`) so a DLQ record carries *why* it is there.

`[C]` Concretely:

```go
// Optional[T] — the single most important type in canal's observability surface.
// "Unknown" must be representable everywhere a metric or a status field can be unknown.
type Optional[T any] struct { V T; Valid bool }

type Meta struct {
    Stream      StreamID              // == checkpoint key. Bounded-ish set, safe as a read-model key.
    EventTime   Optional[time.Time]   // connector-supplied; may legitimately be absent
    IngestTime  time.Time             // core-stamped at read, never trusted from a connector
    SizeBytes   int64
    TraceParent string                // W3C traceparent, "" == none. Opaque to the core.
    Attributes  map[string]string     // connector free-form. NEVER used as a metric label.
}
```

That last comment is a rule, not a note. `Attributes` is unbounded data content; the moment it reaches a
metric label you have the cardinality bomb of §11.

This is a legitimate use of generics under constraint #5: `Optional[T]` earns its keep because the
absent-vs-zero distinction recurs in ~15 places (lag, backlog, snapshot totals, checkpoint time, every gauge).
`Optional[float64]` and a `*float64` are equivalent in expressiveness, but the named type makes "you must
handle Unknown" visible in every signature and lets one JSON marshalling rule apply everywhere.

---

## 3. Checkpoint model — what the control plane must be able to say about progress

Checkpoint *mechanics* are the other dossiers' subject. Here: what the control plane and UI need from it.

### 3.1 `checkpoint_age` is the best health metric in the entire system

**`[C]` The single most valuable number canal can publish is `now − wall_clock_time_of_last_successful
checkpoint commit`.** Reasons, in order:

1. **It is always available.** It requires zero cooperation from the source, no comparable positions, no event
   times, no backlog estimate. Every pipeline commits checkpoints or is broken.
2. **It cannot be faked.** Design-rule R4 exists because a `202` from an unsynced in-memory slice looked
   healthy. A checkpoint age can only advance if something durable actually happened.
3. **It detects every stall mode with one alert**: source hung, sink hung, transform deadlocked, coordinator
   lost, disk full, credential expired. All of them show up as a checkpoint that stopped advancing.
4. **It distinguishes "no data" from "stuck" when paired with `source_idle_seconds`.** Idle high + checkpoint
   age high = quiet source, fine. Idle low + checkpoint age high = records flowing, nothing committing =
   emergency.

Nothing in Kafka Connect exposes this. Connect exposes `offset-commit-success-percentage` and
`offset-commit-avg-time-ms` `[S]` — rates and latencies of the commit *operation*, which are all zero and
therefore invisible when commits stop being attempted at all. **A rate is not a liveness signal; an age is.**
This is the same class of error as a `/health` probe returning 200 while no data moves.

### 3.2 Committed vs written must be two different numbers

Design-rule R4 in metric form. `[C]`

- `canal_records_written_total` — handed to the sink, sink returned success.
- `canal_records_committed_total` — the checkpoint has advanced past this record; it survives `kill -9`.

The UI shows **committed** as "your data arrived" and `written − committed` as "in flight". A UI that shows
only one of these is the dishonest health panel design-rules describes. Connect conflates them: the source
metrics are `source-record-poll-*` and `source-record-write-*` `[S]` with no committed counter at all, and
`source-record-active-count` is the only in-flight signal.

### 3.3 Checkpoint edit as a first-class API operation

KIP-875 gave Connect `GET /connectors/{n}/offsets`, `PATCH .../offsets`, `DELETE .../offsets` `[S]`. This is
the operation every operator eventually needs at 3am ("re-read from yesterday", "skip the poison record") and
which every framework lacking it forces people to do with raw access to the state store.

`[C]` canal must have it from the start, with three properties Connect's version has and one it lacks:

- readable while running, editable only while **stopped** (Connect requires the connector be `STOPPED` for
  `PATCH` `[R!]`);
- the payload is the connector's own opaque position, round-tripped verbatim;
- the connector may *validate* a proposed edit (Connect's `alterOffsets(config, offsets)` capability `[S]`);
- **and (the addition) every edit is an audited event in the pipeline's event log** with actor, before, after.
  Silent checkpoint rewrites are the most destructive operation the API exposes.

---

## 4. Snapshot handling — modelling progress for a phase that has a denominator

Snapshot is the one phase where a real percentage is computable, and it is where every UI either shines or
lies. Debezium is the reference implementation. Its snapshot MBean attributes `[R!]`:

```
TotalTableCount            RemainingTableCount
SnapshotRunning            SnapshotPaused        SnapshotAborted     SnapshotCompleted
SnapshotDurationInSeconds  SnapshotPausedDurationInSeconds
RowsScanned                (map: table -> rows scanned so far)
ChunkId  ChunkFrom  ChunkTo      TableFrom  TableTo
```

and, in the streaming group `[R!]`:

```
Connected                  MilliSecondsBehindSource      MilliSecondsSinceLastEvent
TotalNumberOfEventsSeen    NumberOfEventsFiltered        CapturedTables
QueueTotalCapacity         QueueRemainingCapacity
CurrentQueueSizeInBytes    MaxQueueSizeInBytes
NumberOfCommittedTransactions   LastTransactionId        SourceEventPosition
```

Read that list as a design document, because it is a very good one:

- **`Total` and `Remaining` are separate attributes, not a ratio.** The consumer divides. If the total is
  unknown, `Total` is absent/0 and the consumer knows not to divide. Publishing a pre-divided ratio destroys
  that information. Same reason Prometheus tells you to expose `_total` counters and let PromQL do the rate.
- **`ChunkId`/`ChunkFrom`/`ChunkTo` expose the *current unit of work*, not just a count.** That is what makes
  a stuck incremental snapshot debuggable — you can see which key range it is wedged on. Debezium's
  incremental snapshot (DDD-3 / watermark-based) is chunked and resumable precisely because the chunk
  boundary is durable state, and exposing it costs nothing once it exists.
- **`SnapshotPaused` and `SnapshotPausedDurationInSeconds` are distinct from `SnapshotRunning`.** Paused is
  not stopped and not running. A boolean would have forced a lie.
- **Queue capacity and remaining capacity are both exposed** — again, no pre-divided ratio, and both a record
  count and a byte count because either can be the binding constraint.

What Debezium gets wrong here `[R!]`: `RowsScanned` is a **map keyed by table name**, exposed as a JMX
attribute. As a JMX attribute that is fine (it is a structured value pulled on demand). The moment a
JMX-to-Prometheus exporter flattens it, you get one time series per table — the exact cardinality trap of
§11.2. The lesson is not "don't expose per-table progress"; it is **"per-table progress belongs in the status
document, not in the metrics registry"**.

`[C]` canal's model:

```go
type SnapshotProgress struct {
    Phase        SnapshotPhase          // Pending | Running | Paused | Completed | Aborted
    Units        Optional[int64]        // total work units (tables, chunks, rows) — often unknown
    UnitsDone    int64
    UnitsExact   bool                   // is Units a count or an estimate?
    CurrentUnit  Optional[UnitRef]      // {Stream, ChunkID, From, To} — Debezium's best idea
    StartedAt    time.Time
    PausedFor    time.Duration
}
```

`Units` being `Optional` is the whole point: a snapshot of a Postgres table has an exact `COUNT(*)` or a
cheap estimate from `reltuples`; a snapshot of a paginated REST API has **no denominator at all** and the UI
must render "12,481 records so far, total unknown" rather than a progress bar stuck at 0%.

**Handoff to streaming must be a status transition, not an inference.** Connect has no phase concept at all
(`[S]`: no `COMPLETED`, no bounded-task concept, no way for `poll()` to say "exhausted"), which is why
Debezium had to smuggle snapshot state through the offset map and invent a signalling table. `[C]` canal's
read model carries an explicit `Phase ∈ {snapshotting, handing_off, streaming, caught_up, completed}` and the
transition emits an event. `caught_up` as distinct from `streaming` matters: it is the answer to "is the
initial load finished *and* has the incremental stream drained the backlog it accumulated during the
snapshot", which is the question every operator actually asks and no framework answers.

---
## 5. Schema handling — drift is an *event*, not a metric and not a log line

The observability angle on schema: a UI cannot answer "what changed and when" from either metrics (no
narrative) or logs (unqueryable, unbounded, often not retained). It needs a **bounded, typed event log per
pipeline**.

Prior art:

- **Airbyte** detects source schema changes on a schedule, diffs them against the stored catalog, marks the
  connection with a **`breaking_change` flag**, and can auto-propagate non-breaking changes or halt on
  breaking ones (`non_breaking_changes_preference` ∈ `ignore | disable | propagate_columns |
  propagate_fully`). `[R]` The distinction *breaking vs non-breaking, with a per-connection policy* is the
  right shape.
- **Debezium** keeps a durable schema history (`schema.history.internal.*`) and exposes its own metric group
  for it `[R!]`, because schema history corruption is a top support issue — the history topic and the
  connector's offset can disagree after a restart, producing the well-known
  "Encountered change event for table whose schema isn't known" failure. `[R!]`
- **Connect** carries schema in-band per record and compares structurally `[S]`; drift is invisible to the
  framework and shows up as a converter/sink error.

`[C]` canal:

```go
type Event struct {
    At       time.Time
    Pipeline PipelineID
    Stage    StageID          // bounded
    Kind     EventKind        // bounded enum, see below
    Severity Severity         // info | warning | error
    Stream   Optional[StreamID]
    Reason   string           // machine-readable, bounded vocabulary
    Message  string           // human text
    Detail   json.RawMessage  // free-form, never label-ified
}
```

`EventKind` is a closed set, and this is the vocabulary I'd start with:
`pipeline_started`, `pipeline_stopped`, `phase_changed`, `assignment_gained`, `assignment_lost`,
`schema_discovered`, `schema_changed_compatible`, `schema_changed_breaking`, `checkpoint_committed_first`,
`checkpoint_edited`, `config_applied`, `validation_failed`, `degraded`, `recovered`, `dead_lettered`,
`rate_limited`, `backpressure_engaged`, `backpressure_released`, `clock_skew_detected`.

Storage: **a bounded in-memory ring buffer per pipeline (last ~200 events) served in the read model, plus
optional durable persistence.** The ring buffer is a two-hour feature and is the highest
value-per-line-of-code item in the whole frontend story — it turns "the pipeline is red" into a narrative.
Durable persistence can come later without changing the read model.

Note the deliberate consequence: `schema_changed_breaking` being an `EventKind` and `Detail` being opaque JSON
means canal's core needs **no schema model of its own** to report drift. The connector says "breaking change,
here is the detail blob"; the core stores, counts (`canal_schema_changes_total{compatibility}` — two label
values, safe) and serves it. Source-agnostic by construction.

---

## 6. Lifecycle — the status model, and why one enum is never enough

### 6.1 Prior-art state machines

| System | States |
|---|---|
| Kafka Connect | `UNASSIGNED, RUNNING, PAUSED, FAILED, DESTROYED, RESTARTING, STOPPED` `[S]` |
| Airbyte (job) | `pending, running, incomplete, failed, succeeded, cancelled` `[R]` |
| Airbyte (stream, in-band) | `STARTED, RUNNING, COMPLETE, INCOMPLETE` `[R!]` |
| Kubernetes (the pattern) | a `phase` **plus** a `conditions[]` array `[R!]` |

Connect's is missing `COMPLETED` `[S]` — a documented, structural gap that makes bounded jobs
second-class. Airbyte's has `succeeded` but no notion of a long-running pipeline's *health*, because in
Airbyte everything is a job.

canal needs both axes, because it has both kinds of pipeline. **This is where a single enum breaks.** A
streaming pipeline that is running but 40 minutes behind, with a valid config, a live source connection, and a
sink returning 429s, is: running (yes), healthy (no), progressing (barely), caught up (no), degraded (yes),
failed (no). One enum cannot say that, and every framework that tried ended up with an overloaded `DEGRADED`
that means six different things.

### 6.2 `[C]` Phase + conditions

Adopt the Kubernetes shape verbatim, because it is the most battle-tested status model in existence and
because it makes canal operator-ready for free (§12).

```go
type Phase string // exactly one, coarse, for a badge
const (
    PhasePending  Phase = "Pending"      // accepted, not yet assigned
    PhaseStarting Phase = "Starting"     // opening connections, validating
    PhaseRunning  Phase = "Running"
    PhasePaused   Phase = "Paused"       // operator-requested, not an error
    PhaseStopping Phase = "Stopping"
    PhaseStopped  Phase = "Stopped"
    PhaseFailed   Phase = "Failed"       // terminal, needs intervention
    PhaseCompleted Phase = "Completed"   // bounded pipeline finished. Connect's missing state.
)

type ConditionStatus string // three values. "Unknown" is mandatory, not a convenience.
const ( CondTrue ConditionStatus = "True"; CondFalse = "False"; CondUnknown = "Unknown" )

type Condition struct {
    Type               string          // Configured | Connected | Progressing | CaughtUp | Degraded | Assigned
    Status             ConditionStatus
    Reason             string          // CamelCase machine token, bounded vocabulary
    Message            string          // human sentence, may include last error
    LastTransitionTime time.Time
    ObservedGeneration int64
}
```

The condition set I'd fix now, with the exact semantics — this is the honesty contract from design-rules made
machine-checkable:

| Condition | `True` means | `Unknown` means |
|---|---|---|
| `Configured` | config validated against the connector's parameter spec, including I/O validation | validation not yet run |
| `Connected` | the connector's own liveness check succeeded within the last N seconds | never probed, or the connector declares no probe |
| `Progressing` | `records_committed_total` increased in the last window | window not yet elapsed |
| `CaughtUp` | lag/backlog is within the configured objective | lag is not measurable for this source |
| `Degraded` | sustained backoff, partial failures, or rate limiting | — |
| `Assigned` | every planned assignment has a live worker lease | coordinator unreachable |

**`Connected: True` must never be allowed to imply `Progressing: True`.** That separation is the machine-
readable form of the rule that a probe returning 200 must not imply data arrival. Because they are separate
conditions with separate `LastTransitionTime`s, the UI *cannot* collapse them, and a fixture test can assert
that a pipeline with a healthy connection and a stalled commit renders as unhealthy.

`ObservedGeneration` on each condition is the field that answers "did my config change take effect?" — the
question Connect's status API structurally cannot answer.

### 6.3 Cancellation, shutdown, and what the status must say during it

Connect's failure here is documented and severe `[S]`: `stop()` "can actually be called more than once in some
situations for the same SourceTask", and afterwards "any of `poll()`, `commit()` and `commitRecord()` can
still be called"; KIP-419's `stopped()` has been under discussion ~7 years and is absent from trunk.

`[C]` For the control plane this means: **`Stopping` is a real phase with a deadline, and the read model must
expose the deadline.** `PhaseStopping` carries `stoppingSince` and `drainDeadline`; the UI shows "draining, 12s
of 30s". The distinction between *graceful drain completed* and *deadline exceeded, forced* is an event
(`pipeline_stopped` with `Reason: Drained` vs `Reason: DrainTimeout`), because the second one means records may
be replayed on restart and the operator needs to know.

### 6.4 Error and retry classification, surfaced

Design-rules already fixes the taxonomy (transient-upstream, transient-internal, permanent-upstream,
permanent-mapping, permanent-contract, duplicate-idempotent-success, clock-skew). Two observability
requirements follow:

1. **It is a bounded metric label.** `canal_records_failed_total{pipeline,stage,class}` — 7 values, safe.
   This is the rare case where a failure label is legitimate precisely *because* the taxonomy is closed.
   Never label with an error *message* or an upstream error *code*.
2. **Sustained backoff must set `Degraded: True` with a `Reason` and a last-error `Message`**, per
   design-rules' backoff policy. The metric that makes this alertable is
   `canal_backoff_seconds_total{pipeline,stage,class}` — cumulative time spent waiting. A retry *count* tells
   you retries happened; retry *seconds* tells you the pipeline is spending 80% of its life backing off.

---
## 7. Config model — declaration, validation, storage, reload, secrets

### 7.1 The four prior-art approaches, ranked

| System | Declaration | UI-renderable? | Validation |
|---|---|---|---|
| Kafka Connect | `ConfigDef` — Go-equivalent struct with 14 fields per key, 5 purely presentational `[S]` | **Yes, best in class.** `group`, `orderInGroup`, `width`, `displayName`, `importance`, `dependents`, `Recommender` | Two-tier: declarative `Validator` (no I/O) + overridable `Connector.validate(Map)` that may hit the network; returns per-field `ConfigValue{value, recommendedValues, errorMessages, visible}` `[S]` |
| Airbyte | `connectionSpecification`: **JSON Schema draft-07**, with `airbyte_secret: true`, `order`, `title`, `description`, `oneOf` for variants, plus `advanced_auth` for OAuth `[R!]` | Yes — but the UI has to interpret arbitrary JSON Schema, which is a large, permanently-incomplete job | `check_connection` runs the connector as a subprocess and returns `CONNECTION_STATUS` `[R!]` |
| Conduit | `Parameters() map[string]Parameter` — type, default, description, validations `[R!]` | Flat form only: no groups, no ordering, no conditional visibility | Declarative validations only `[R]` |
| Vector | Rust `#[configurable]` derive → generated JSON Schema for docs `[R]` | Docs, not a live form | Startup-time parse + `vector validate` subcommand `[R!]` |

**Connect wins on expressiveness, Airbyte wins on being data-on-the-wire, and neither wins on both.** Connect's
`ConfigDef` is a Java object graph containing `Validator` and `Recommender` *behaviour*, so it cannot be
shipped over a wire — the REST API has to project it into a flattened `ConfigKeyInfo` DTO `[S]`. Airbyte's
JSON Schema is pure data and crosses a process boundary trivially, but pushes an unbounded interpretation
burden onto the frontend.

### 7.2 `[C]` canal's parameter spec: Connect's field set, as data, with an escape hatch

```go
type ParamType string
const (
    ParamString ParamType = "string"; ParamInt = "int"; ParamFloat = "float"
    ParamBool = "bool"; ParamDuration = "duration"; ParamEnum = "enum"
    ParamSecret = "secret"; ParamFile = "file"
    ParamList = "list"     // Elem describes the element
    ParamObject = "object" // Fields describes the sub-spec  <-- Connect cannot express this
)

type Param struct {
    Name        string
    Type        ParamType
    Elem        *Param            // for list
    Fields      []Param           // for object  (fixes Connect's dotted-prefix hack)
    Default     string            // canonical string form; empty + Required=true => must supply
    Required    bool
    Description string
    Validations []Validation      // declarative, no I/O, serialisable
    // Presentation is part of the contract, not an afterthought:
    DisplayName string
    Group       string
    Order       int
    Importance  Importance        // high | medium | low
    Widget      Widget            // text | textarea | password | select | toggle | kv | code | file
    DependsOn   []string          // re-validate / re-recommend these when I change
    Advanced    bool              // collapse by default
    Internal    bool              // hide from UI entirely
    // The constraint-#1 escape hatch:
    Extensions  map[string]json.RawMessage // opaque to the core; the frontend may special-case by plugin name
}
```

Three deliberate departures from Connect:

1. **`Fields []Param` / `Elem *Param` — nested and repeated structure is expressible.** Connect's inability to
   declare "a list of sub-objects" is why `transforms=a,b` + `transforms.a.type=…` needs bespoke `enrich()`
   logic in `ConnectorConfig` that `ConfigDef` itself cannot describe `[S]`. canal will need exactly this for
   transform chains and multi-stream selection, so it must be in the model from day one.
2. **`Validations` are data, not a `Validator` interface.** A closed set (`required`, `range`, `regex`,
   `oneOf`, `notEmpty`, `duration_min`, `len`) that a Go core, a browser, and a JSON-Schema generator can each
   interpret. Behaviour that cannot be expressed declaratively belongs in the I/O validation pass, not in the
   spec. **This is the single change that makes the spec shippable over gRPC.**
3. **`Extensions map[string]json.RawMessage` is how constraint #1 is honoured.** "Specialised UI/UX for
   less-generic connectors comes later and must NOT require core changes." A Mongo connector can put
   `{"canal.ui/collectionPicker": {...}}` in its extensions; the core stores and serves the blob without
   parsing it; the frontend may render a bespoke widget keyed on the plugin name. **Zero core knowledge, zero
   core edits, and the generic renderer still works if the frontend has never heard of that plugin.** This is
   strictly better than Airbyte's approach of growing the platform's JSON-Schema dialect (`airbyte_secret`,
   `advanced_auth`, `always_show`, `group`, …) every time a connector needs something, which is a core change
   every time.

### 7.3 Two-tier validation, and the response shape a form needs

Copy Connect exactly — this is one of its best decisions `[S]`. Cheap declarative validation on every
keystroke, expensive I/O validation on submit, and **all** errors returned at once, keyed by field.

```go
// Declarative tier: pure function of the spec, runs in the core AND in the browser.
func Validate(spec []Param, cfg Config) []FieldDiagnostic

// I/O tier: optional connector capability, may hit the network. Bounded by ctx.
type Validator interface {
    Validate(ctx context.Context, cfg Config) ([]FieldDiagnostic, error)
}
// Live dropdowns: valid values as a function of the WHOLE parsed config (Connect's Recommender).
type Recommender interface {
    Recommend(ctx context.Context, cfg Config) (map[string][]string, error)
}

type FieldDiagnostic struct {
    Field       string    // dotted path into the (possibly nested) config
    Severity    Severity  // error | warning | info
    Message     string
    Suggestions []string  // recommendedValues
    Visible     *bool     // nil = leave as-is; Connect's ConfigValue.visible
}
```

Both `Validator` and `Recommender` are **separate, independently type-assertable interfaces**, not methods on
a fat connector interface — per the sibling dossier's conclusion that Go has no `default` methods and every
optional capability must be its own interface.

Note `Recommend` returning `map[string][]string` rather than being called per-field: one round trip for the
whole form, which matters enormously once this call crosses a process boundary.

### 7.4 `[C]` Config storage — one interface, CAS, watchable

The store is the standalone/enterprise seam (§12), so its interface must be narrow and must not leak the
substrate:

```go
type Revision uint64

type ConfigStore interface {
    Get(ctx context.Context, id PipelineID) (PipelineSpec, Revision, error)
    // Compare-and-swap. ifRevision == 0 means "create, must not exist".
    Put(ctx context.Context, spec PipelineSpec, ifRevision Revision) (Revision, error) // ErrConflict
    Delete(ctx context.Context, id PipelineID, ifRevision Revision) error
    List(ctx context.Context) ([]PipelineSummary, Revision, error)
    // Watch replays from `from` then streams. Closed channel == caller must re-List.
    Watch(ctx context.Context, from Revision) (<-chan ConfigEvent, error)
}
```

`Revision` is the whole design. It gives, in one field: optimistic concurrency for the API, the
`generation` the status document reports as applied, the resume token for `Watch`, and an ETag for HTTP
caching. **This is the direct fix for the failure `KafkaConfigBackingStore`'s own javadoc documents** —
compaction plus a partial multi-record write leaving "an inconsistent state with no obvious way to resolve the
issue" `[S]`. A store with atomic multi-key writes and a monotonic revision cannot enter that state.

Implementations, in the order I'd build them:

| Impl | Mode | Notes |
|---|---|---|
| in-memory | tests | `Watch` from a channel fan-out |
| SQLite / bbolt single file | **standalone dev, default** | `Revision` = a row version or a monotonic counter table; `Watch` = poll `max(revision)` every 250ms. Zero external deps, survives restart. |
| Postgres | **enterprise default** | `Revision` = `xmin`-free explicit `bigint` bumped in the same txn; `Watch` = `LISTEN/NOTIFY` with a poll fallback. Multi-row atomicity for free. |
| etcd | enterprise alt | `Revision` = etcd `ModRevision`; `Watch` is native and exactly this shape. Best fit semantically, extra operational dependency. |
| Kubernetes CRD | operator mode | `Revision` = `metadata.generation` / `resourceVersion`; `Watch` = the informer. |

Note how cleanly etcd and k8s map onto this interface — **that is not a coincidence, it is because `Revision` +
`Watch(from)` is the shape both of them converged on.** Designing to it now costs nothing and buys the operator
integration later.

**File-based config is a projection, not a competing source of truth.** `canal run -f pipelines.yaml` loads the
file into the store at startup (revision 1) and, with `--watch`, re-`Put`s on change. That keeps exactly one
representation per entity (design-rule R1/R9) while still giving the git-ops workflow Vector and Conduit users
expect.

### 7.5 Reload semantics

Vector's approach is the one to copy `[R!]`: on config change, **diff the topology and rebuild only the
components whose configuration actually changed**, keeping the rest running. Kafka Connect's approach —
`reconfigure(props)` defaulting to `stop(); start(props)` `[S]`, and a config change triggering a rebalance —
is strictly worse.

`[C]` Two rules that make this safe:

1. **The checkpoint key is derived from stable identity (`pipelineID` + `StreamID`), never from a config hash
   or an ordinal.** Otherwise any config edit silently orphans the checkpoint and the pipeline re-reads from
   the beginning. This is a one-line decision with catastrophic failure mode if got wrong, and it is the kind
   of thing that is unfixable after the first production deployment.
2. **A spec change that alters stream identity or codec is a `Restart`, not a `Reload`, and the API says
   which.** The validation response tells the operator: `"impact": "restart_required"` with the reason. Never
   silently restart a pipeline that the operator thought they were tweaking.

### 7.6 Secrets — the one place to be maximally conservative

Prior art, all three patterns worth knowing:

- **Kafka Connect / KIP-297 `ConfigProvider`.** Config values may contain indirections
  `${provider:path:key}`, resolved at task start by a pluggable provider (`config.providers=file,vault`;
  built-ins `FileConfigProvider`, `DirectoryConfigProvider`), with a `subscribe`/`ConfigChangeCallback`
  mechanism for rotation. `[R!]` **The stored config never contains the secret.** This is the best of the
  three because it is dead simple and the control plane's own store never becomes a secret store.
- **Airbyte secret coordinates.** Fields marked `airbyte_secret: true` in the connector's JSON Schema are
  stripped by the platform, written to an external manager (Google Secret Manager / AWS Secrets Manager /
  Vault), and replaced in the persisted config by a coordinate object of the form
  `{"_secret": "<coordinate>"}`; the config is "hydrated" immediately before being handed to the connector.
  `[R!]` Strong property: **the connector's declaration drives which fields are secret** — no core knowledge
  of connector-specific field names. Weak property: it makes an external secret manager a hard dependency for
  any serious deployment, and the local/testing persistence has repeatedly been a source of leaks.
- **Vector.** `${ENV_VAR}` / `${ENV:-default}` interpolation plus a `secret` config section with pluggable
  backends and a `SECRET[backend.key]` reference syntax. `[R!]` Same indirection idea, file-config flavoured.

`[C]` canal takes Connect's indirection *plus* Airbyte's declaration-driven marking, and adds the two things
all three do badly:

```go
// A value that cannot be logged, printed, or serialised by accident.
type Secret struct{ v string }              // unexported field
func (Secret) String() string               { return "***" }
func (Secret) GoString() string             { return "***" }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"***"`), nil }
func (s Secret) Reveal() string             { return s.v }   // grep-able, auditable call site
```

1. **`Param.Type == ParamSecret` (or `Secret: true`) makes redaction automatic in every read path.** The API
   never returns the value — it returns `"***"` plus a boolean `isSet`. Connect returning connector configs
   verbatim over REST `[R!]` is a genuine, frequently-exploited hole, and it exists only because redaction was
   not derived from the declaration.
2. **The stored spec holds the indirection, never the resolved value.** `${env:PGPASSWORD}`,
   `${file:/run/secrets/db:password}`, `${vault:kv/data/prod/db:password}`. Resolution happens at connector
   `Open`, in the worker, as late as possible, and the resolved value exists only as a `Secret` in the
   connector's config struct.
3. **A `Secret` never leaves the worker process.** Not in the status document, not in an event `Detail`, not
   in a validation diagnostic, not in a metric label, not in a log field. The type makes accidental violation
   require an explicit `.Reveal()` that shows up in review and in `grep`.
4. Rotation: the resolver interface has `Watch`, and a rotation triggers a connector `Restart` — not a
   silent in-place swap, because a half-rotated connection pool is worse than a restart.
5. `CONTROL`-message-style connector-originated config writes (Airbyte's OAuth refresh case) go through the
   *same* declaration-driven splitting path. Never a side door.

---
## 8. Backpressure — the four numbers that make it diagnosable

Backpressure *mechanism* is elsewhere. Here: what must be measurable, because "the pipeline is slow" is the
single most common operator question and almost every framework answers it badly.

### 8.1 Prior art worth copying, precisely

**OpenTelemetry Collector** has, in my judgement, the best backpressure instrumentation of any of these
systems, and the key idea is a *pair* of counters `[R!]`:

```
otelcol_receiver_accepted_spans_total     otelcol_receiver_refused_spans_total
otelcol_exporter_sent_spans_total         otelcol_exporter_send_failed_spans_total
                                          otelcol_exporter_enqueue_failed_spans_total
otelcol_exporter_queue_size               otelcol_exporter_queue_capacity
```

**`refused` is a first-class counter, separate from `failed`.** "I rejected this because I am full" and "I
tried and it broke" are different events with different fixes, and collapsing them into one error counter — as
almost everyone does — destroys the distinction. Likewise `send_failed` vs `enqueue_failed`: downstream broke
vs my own buffer was full.

`queue_size` and `queue_capacity` are exposed as **two separate gauges**, not a ratio. Same discipline as
Debezium's `QueueTotalCapacity`/`QueueRemainingCapacity`. The consumer divides; a pre-divided ratio cannot be
re-aggregated across workers and hides whether the capacity itself changed.

**Vector** contributes the `utilization` gauge `[R!]` — per component, the fraction of wall time the component
spent doing work rather than waiting. This is the one metric that answers "which stage is the bottleneck?" in a
single glance, and it is *not* derivable from throughput. Kafka Connect has the same idea, coarser, as
`running-ratio` / `pause-ratio` duty-cycle gauges `[S]`.

**Kafka Connect** contributes the negative example: `source-record-active-count`/`-avg`/`-max` `[S]` lets you
*watch* an unbounded in-flight buffer grow while being structurally unable to push back — the javadoc even
describes the pathology (one offline partition, records for it unacknowledged, other partitions still
dispatching). Design-rule R6 is the same lesson from the other codebase.

### 8.2 `[C]` canal's backpressure metric set

```
canal_buffer_depth{pipeline,buffer}                    gauge      # records currently held
canal_buffer_capacity{pipeline,buffer}                 gauge      # never pre-divide
canal_buffer_bytes{pipeline,buffer}                    gauge
canal_buffer_bytes_capacity{pipeline,buffer}           gauge
canal_buffer_refused_total{pipeline,buffer}            counter    # rejection is a first-class event
canal_stage_blocked_seconds_total{pipeline,stage}      counter    # <-- the one everybody omits
canal_stage_utilization_ratio{pipeline,stage}          gauge      # Vector's utilization
canal_inflight_records{pipeline,stage}                 gauge      # Connect's active-count
```

**`canal_stage_blocked_seconds_total` is the recommendation I would defend hardest in this section.** It is
cumulative time a stage spent unable to make progress because the *next* stage would not accept work. With it,
"why is my pipeline slow" is answered by one PromQL query — `topk(1, rate(canal_stage_blocked_seconds_total[5m]))`
— and the answer names the culprit stage. Without it you are inferring bottlenecks from throughput ratios and
buffer depths, which is what everyone currently does and why it takes an afternoon.

It is also nearly free: the instant a bounded channel send or a credit acquisition blocks, you already have the
timestamp.

Two supporting rules:

- **Both a record count and a byte count for every buffer.** Either can be the binding constraint (Debezium
  exposes both for exactly this reason `[R!]`), and a pipeline of 2KB records and one of 2MB records fail
  differently.
- **`refused` must be distinguishable from `failed` in the failure taxonomy too**, i.e. buffer-full maps to a
  distinct class from upstream-error, so `canal_records_failed_total{class}` and
  `canal_buffer_refused_total` never double-count.

---

## 9. Delivery guarantees — what the UI is allowed to claim

This section is short because it is mostly a restatement of design-rule R4, but the metric consequences are
concrete and easy to get wrong.

`[C]` **Three counters, never two:**

```
canal_records_read_total{pipeline,connector}        # source produced it
canal_records_written_total{pipeline,connector}     # sink accepted it (sink's word)
canal_records_committed_total{pipeline}             # checkpoint advanced past it (durable)
```

and the UI's copy is fixed by which one it reads:

| Number | Permitted UI phrasing |
|---|---|
| `read` | "read from source" |
| `written` | "sent to destination" |
| `committed` | **"delivered"** / "your data arrived" |
| `written − committed` | "in flight" |

Nothing may be labelled "delivered" from anything but the committed counter. That is R4 expressed as a
presentation constraint, and it is testable against pinned read-model fixtures.

For transactional / two-phase sinks the read model needs one more field: **the count and age of *prepared but
uncommitted* committables**, because a 2PC sink that is prepared-and-stuck is a state that looks healthy on
every other signal (records written, no errors, connection live) while data is invisible downstream and the
checkpoint cannot advance. `canal_pending_committables{pipeline}` + `canal_oldest_committable_age_seconds{pipeline}`.

Dedup, per design-rule R5: `canal_records_deduplicated_total{pipeline,stage}` must count *only* records
rejected because they were **already durably stored**. If canal ever counts an in-RAM-cache hit here, the metric
is lying in exactly the way R5 describes.

---

## 10. Plugin boundary — telemetry that survives a process boundary

This is where constraint #3 ("the interface must be designed so an out-of-process implementation can later
satisfy the SAME interface") bites hardest, and it is the most consequential design decision in this dossier.

### 10.1 The failure mode

Kafka Connect's `PluginMetrics` gives the connector a **live registry handle** through its context `[S]`. The
tagging/naming discipline is right; the delivery is unshippable. A gRPC connector cannot hold a handle to the
core's Prometheus registry. Neither can it hold a `log.Logger` that writes to the core's output, nor a status
callback the core will invoke.

Airbyte, whose connectors *are* out-of-process, arrived at the only shape that works: **telemetry, status,
progress and log lines are all messages on the data channel** `[R!]` (`LOG`, `TRACE/ESTIMATE`,
`TRACE/STREAM_STATUS`, `TRACE/ERROR`).

Conduit is the interesting middle case: its connectors can be in-process *or* standalone gRPC subprocesses
satisfying the same SDK-level interface `[R!]`, which is possible precisely because its methods are
`(ctx, request) → (response, error)` over plain `opencdc.Record` structs. It is the existence proof that
canal's constraint #3 is achievable — and the reason it is achievable is that nothing in the interface is a
handle.

### 10.2 `[C]` The rule

> **A connector never holds a registry, a logger, a channel, or a callback. Everything it wants to tell the
> core, it returns as a value. Everything the core wants to tell it arrives as a parameter.**

Applied to observability, that gives four things, all data:

```go
// (1) Declaration — static, serialisable, part of the plugin's self-description.
type MetricSpec struct {
    Name    string    // no prefix; the core prepends canal_<role>_ or canal_<plugin>_
    Kind    Kind      // Counter | Gauge | Histogram
    Unit    string    // OTel-style: "s", "By", "{record}", "1"
    Help    string
    Labels  []string  // validated at register time against a bounded allowlist
    Buckets []float64 // histograms only
}
type Instrumented interface { Metrics() []MetricSpec }   // optional capability

// (2) Emission — a value returned with each batch (in-process: near-free; gRPC: rides the same response).
type Telemetry struct {
    Counters map[string]uint64                 // DELTA since last report. Deltas survive a restart cleanly.
    Gauges   map[string]Optional[float64]      // absolute; Optional => "unknown", omit the series
    Samples  map[string][]float64              // histogram observations
    Labels   map[string]string                 // sub-labels, must be declared in the MetricSpec
}

// (3) Status — polled, serialisable. Never a callback.
type StatusReporter interface {
    Status(ctx context.Context) (ConnectorStatus, error)   // must be cheap and non-blocking
}
type ConnectorStatus struct {
    Conditions []Condition
    Streams    []StreamProgress
    Events     []Event                   // drained by the core each poll
}

// (4) Logs — structured records returned or written to a core-provided io.Writer-equivalent that is
//     trivially replaceable by a gRPC stream. NOT a *slog.Logger the connector keeps.
```

Three specific consequences worth stating because they are easy to get wrong:

- **Counters are reported as deltas, gauges as absolutes.** A delta is idempotent-safe to add and survives a
  connector restart without a spurious `rate()` spike; an absolute counter from a restarted connector looks
  like a counter reset the core must detect. (Prometheus handles resets for *its own* counters; it cannot
  handle a sub-counter inside your process resetting.)
- **`Optional[float64]` gauges mean "do not emit the series".** Not zero. This requires a custom
  `prometheus.Collector` whose `Collect` skips invalid values, which is ~30 lines and is the mechanism that
  makes "unknown lag" honest rather than "0 lag".
- **Label allowlisting is enforced at registration, not at emission.** `Metrics()` returns the specs; the core
  validates that every declared label is in the permitted set and that the plugin has not declared something
  data-derived (`table`, `key`, `id`, `user`, …). A connector that tries to open a cardinality hole fails to
  register — loudly, at startup, like KIP-1004 hard-enforcing `tasks.max` after eight years of it being
  advisory `[S]`.

### 10.3 Versioning of the observability contract

`MetricSpec` and `ConnectorStatus` are part of the plugin contract, so they version with it. `[C]` Two rules:

- The core **prefixes and owns** every metric name, so a connector cannot squat `canal_records_committed_total`
  or collide with another plugin. A plugin metric is `canal_plugin_<plugin>_<name>`.
- Adding a field to `Telemetry`/`ConnectorStatus` is backward compatible *only* if they are structs with
  ignore-unknown decoding on both sides. Design them as protobuf-shaped from the start (all fields optional,
  no required, no positional semantics), even while the only implementation is in-process — this is the cheap
  insurance that makes the later gRPC shim a shim rather than a redesign.

---
## 11. Observability

### 11.1 OpenTelemetry messaging semantic conventions — and why canal should *not* adopt them for its own metrics

**VERIFIED** against
`semantic-conventions/main/docs/messaging/messaging-metrics.md` in a late window of this session. The metric
set is exactly four instruments, and **every one is `Stability: Development`**:

| Metric | Instrument | Unit | Required attributes | Conditionally required | Recommended |
|---|---|---|---|---|---|
| `messaging.client.operation.duration` | Histogram | `s` | `messaging.operation.name`, `messaging.system` | `error.type` (on failure), `messaging.consumer.group.name`, **`messaging.destination.name` (only "if low cardinality")**, `messaging.destination.subscription.name`, `messaging.destination.template`, `messaging.operation.type`, `server.address` | `messaging.destination.partition.id`, `server.port` |
| `messaging.client.sent.messages` | Counter | `{message}` | `messaging.operation.name`, `messaging.system` | `error.type`, `messaging.destination.name` (if low cardinality), `messaging.destination.template`, `server.address` | `messaging.destination.partition.id`, `server.port` |
| `messaging.client.consumed.messages` | Counter | `{message}` | `messaging.operation.name`, `messaging.system` | as `operation.duration` | as above |
| `messaging.process.duration` | Histogram | `s` | `messaging.operation.name`, `messaging.system` | as `operation.duration` | as above |

**Note the single most instructive detail in the whole convention: `messaging.destination.name` is only
conditionally required, and the condition is literally "if low cardinality".** OpenTelemetry — designing for
message brokers, where the destination is the most natural dimension imaginable — still refuses to make the
destination a mandatory attribute, because it might be unbounded. That is external, authoritative support for
the rule in §11.3: **the per-stream dimension does not belong in the metric.** `messaging.destination.template`
exists precisely so you can label with the bounded *pattern* rather than the unbounded instance.

Attributes `[R]` (the wider registry was not re-fetched): `messaging.system`, `messaging.operation.name`,
`messaging.operation.type`
(∈ `create | send | receive | process | settle`), `messaging.destination.name`,
`messaging.destination.template`, `messaging.destination.subscription.name`,
`messaging.destination.partition.id`, `messaging.destination.anonymous`, `messaging.destination.temporary`,
`messaging.consumer.group.name`, `messaging.client.id`, `messaging.message.id`,
`messaging.message.conversation_id`, `messaging.message.body.size`, `messaging.message.envelope.size`,
`messaging.batch.message_count`, plus the general `error.type`, `server.address`, `server.port`.

Kafka-specific `[R]`: `messaging.kafka.offset` (renamed from `messaging.kafka.message.offset`),
`messaging.kafka.message.key`, `messaging.kafka.message.tombstone`, and the deprecated
`messaging.kafka.consumer.group` → `messaging.consumer.group.name`.

Spans `[R]`: span name is `{messaging.operation.name} {destination}`; producer-side spans are
`SpanKind.PRODUCER`, consumer receive spans `SpanKind.CONSUMER`; **batch receive uses span *links* rather than
a single parent**, because one consumer span covers messages from many producers; trace context travels in
message headers.

**Assessment for canal — three findings.**

1. **The conventions are broker-shaped and do not fit a generic connector framework.** `messaging.system` is a
   required attribute naming the *broker technology*; `messaging.destination.name` presumes a broker
   destination; `messaging.consumer.group.name` presumes consumer groups. A canal pipeline moving rows from
   Postgres to S3 has no `messaging.system`, no destination topic, and no consumer group. Forcing
   `messaging.system="canal"` would be a lie that pollutes every messaging dashboard in the org.
   **Constraint #1 forbids letting a Kafka-shaped convention into the core metric names.**
2. **The conventions are not Stable.** Verified: all four messaging metrics carry `Stability: Development`.
   Publishing a metric contract your users build dashboards and alerts on, derived from a
   Development-stability convention, exports that churn to them — and the renames listed above (`[R]`) all
   happened within the last two years.
3. **What *is* worth adopting is the surrounding machinery, not the names**:
   - a **central registry of attributes** with declared types and stability levels, so `pipeline` means one
     thing everywhere (this is design-rule R9, and semconv is the best existing implementation of it);
   - **`error.type`** as the universal low-cardinality error discriminator — canal's failure taxonomy fits it
     exactly;
   - **base units and the unit vocabulary** (`s`, `By`, `{record}`, `1`);
   - **explicit stability levels per metric**, so canal can ship experimental metrics without freezing them;
   - **span links for batches** — the correct answer to "one sink write covers records from many sources".

`[C]` **Decision: canal defines its own `canal.*` / `canal_*` metric namespace and does not emit messaging
semconv for its own pipeline metrics.** A connector that genuinely *is* a messaging client may emit the
messaging conventions itself for its own client library spans/metrics — that is the connector's business and
requires no core involvement. This is exactly the source/sink-agnostic split constraint #1 demands.

### 11.2 OTel *or* Prometheus? — Both, and the answer is an SDK choice not a protocol choice

The compatibility path is well specified `[R!]`: OTLP→Prometheus translation lowercases and replaces `.` with
`_`, appends unit suffixes, appends `_total` to monotonic counters, adds `otel_scope_name` /
`otel_scope_version` labels, and maps resource attributes into a `target_info` metric. Prometheus 3.x can
ingest OTLP natively (`/api/v1/otlp/v1/metrics`) and supports UTF-8 metric names so dots can be preserved.

`[C]` **Recommendation, concretely:**

- **Instrument with the OpenTelemetry Go Metric API** (`go.opentelemetry.io/otel/metric`). Instrument
  definitions live in one internal package; no call site knows the export protocol.
- **Standalone mode: expose a Prometheus `/metrics` endpoint** via
  `go.opentelemetry.io/otel/exporters/prometheus`. Zero external dependencies, `curl`-able, and the dev UI can
  scrape itself. This is non-negotiable — Kafka Connect being JMX-only means "every real deployment bolts on a
  JMX exporter" `[S]`, and that is a self-inflicted adoption tax.
- **Enterprise mode: additionally allow OTLP push** to a collector (`--telemetry.otlp.endpoint`). Both
  exporters can be attached to the same meter provider.
- **Do not** hand-write `prometheus/client_golang` collectors for the main metric set. Do write exactly one
  custom collector: the one that implements "omit the series when the value is `Optional` and invalid".
- **Traces: OTLP only**, sampled, off by default. Never a Prometheus concern.
- Emit **exemplars** on the latency histograms (trace ID attached to the bucket) so the UI can jump from a slow
  p99 bucket straight to a trace. This is the highest-leverage tracing feature and costs a config flag.

The one caveat to record: the OTel Go metric SDK's Prometheus exporter output differs cosmetically from
hand-rolled `client_golang` (`_total` suffixing, `otel_scope_*` labels, `target_info`). Decide the naming
strategy **once**, write it into a normative doc, and pin it with a golden-file test asserting the actual
`/metrics` output — precisely design-rule R8 ("tests assert real responses against the schema").

### 11.3 Prometheus naming and cardinality discipline

**VERIFIED** against `prometheus.io/docs/practices/naming/`. A metric name:

> - "MUST comply with the data model for valid characters"
> - "SHOULD have a (single-word) application prefix relevant to the domain"
> - "MUST refer to a single unit and to a single quantity"
> - "SHOULD use base units (e.g. seconds, bytes, meters)"
> - "SHOULD have a suffix describing the unit, in plural form"
> - "SHOULD represent the same logical thing-being-measured across all label dimensions"

with the documented suffix examples `http_request_duration_seconds`, `node_memory_usage_bytes`,
`http_requests_total`, `process_cpu_seconds_total`, and a base-unit table (seconds, bytes, ratio, celsius,
meters, volts, amperes, joules, grams).

Note the fourth and sixth rules together: **"MUST refer to a single unit and a single quantity"** and
**"SHOULD represent the same logical thing-being-measured across all label dimensions"**. The second is the
one people violate — it means summing the metric across its labels must be *meaningful*. This is a direct
argument against, e.g., a single `canal_lag_seconds{kind="event_time|backlog|position"}`: those are not the
same quantity and summing them is nonsense. Hence four separately named lag metrics in §11.5.

And the cardinality warning, verbatim:

> "Remember that every unique combination of key-value label pairs represents a new time series, which can
> dramatically increase the amount of data stored. Do not use labels to store dimensions with high cardinality
> (many different label values), such as user IDs, email addresses, or other unbounded sets of values."

I also recall `[R]` a rule of thumb on the *instrumentation* practices page that a metric's cardinality should
generally be kept below ~10, with only a handful of higher-cardinality metrics across a whole system — not
re-verified this pass.

**Independent corroboration from Vector's own instrumentation specification** (VERIFIED against
`vectordotdev/vector/master/docs/specs/instrumentation.md`), which is a normative internal spec of exactly the
kind canal needs:

> metric names "MUST only contain ASCII alphanumeric, lowercase, and underscore characters" and "MUST be in
> snakecase format", following the template `namespace_name_unit_[total]`

and, on the error metric `<namespace>_errors_total` (required tags `error_code`, `error_type`, `stage`, plus
inherited component properties):

> "error_code … MUST be a bounded set with relatively low cardinality because it will be used as a metric tag"

plus `<namespace>_discarded_events_total`, a counter required to carry an `intentional` property — i.e.
**Vector distinguishes deliberate drops from failures with a boolean tag on the same metric**, which is a
neater encoding than two metrics and worth copying for canal's `canal_records_dropped_total`.

Three things to steal from that spec wholesale: the **`namespace_name_unit_[total]` template as a mechanically
checkable rule**, the **explicit "MUST be a bounded set with relatively low cardinality" requirement written
into the contract for every tag**, and the **`intentional` boolean on drops**.

**The cardinality trap, stated arithmetically for canal.** This is the concrete warning the brief asked for.

Suppose you label per table: `canal_records_read_total{pipeline,connector,stream}`. A single Mongo or Postgres
source with 2,000 collections gives 2,000 series for that one metric. Now:

| Multiplier | Factor | Running total |
|---|---|---|
| streams | 2,000 | 2,000 |
| × metrics that carry `stream` (read, written, committed, failed, bytes_read, bytes_written, lag) | 7 | 14,000 |
| × failure `class` on the failed one (7 values) | +12,000 | 26,000 |
| × 3 pipelines on the same source | 3 | 78,000 |
| × a latency **histogram** per stream (12 buckets + `_sum` + `_count`) | +2,000 × 14 × 3 = 84,000 | **162,000** |
| × 10 workers, if the metric is per-worker and not aggregated | 10 | **1,620,000** |

1.6 million active series from **one** design decision, on a deployment that is not large. At roughly the
commonly-cited few-KB-per-series memory cost, that is single-digit GB of Prometheus RAM for one canal cluster,
and it will be discovered in production.

`[C]` **The rule, and it is a hard one:**

> **A metric label may only be drawn from the pipeline's *topology*. It may never be drawn from the *data*.**
>
> Permitted label vocabulary — closed, enforced at registration:
> `pipeline`, `stage`, `role` (`source|transform|sink`), `connector` (plugin name), `plugin_version`,
> `worker`, `phase`, `class` (the 7-value failure taxonomy), `outcome` (`success|failure`), `buffer`,
> `codec`, `direction`.
>
> Forbidden: `stream`, `table`, `collection`, `topic`, `partition`, `key`, `id`, `tenant_id`, `user`,
> `error_message`, any upstream error code, any URL, any timestamp.

And the corollary that makes it acceptable rather than merely restrictive:

> **Per-stream detail lives in the control-plane read model, not the metrics registry.**

Two systems, two questions:

| Question | Answered by | Cardinality |
|---|---|---|
| "how has throughput/lag/error-rate behaved over time, aggregated?" | Prometheus time series | bounded by topology |
| "which of my 2,000 tables is behind, right now, and by how much?" | `GET /v1/pipelines/{id}/status` → `streams[]` | unbounded, but it is *current state*, computed on demand from in-process counters, retained for zero seconds |

This is not a compromise — it is the correct decomposition, and it is what Debezium accidentally gets right by
exposing `RowsScanned` as a *JMX map attribute* rather than as 2,000 metrics `[R!]`. The mistake is only ever
made by the exporter that flattens it.

Two escape valves for the operator who genuinely needs per-stream time series:

1. `metrics.per_stream.enabled: true` with an **explicit allowlist** of stream names (not a wildcard), a hard
   cap (say 50), and everything outside the allowlist folded into `stream="__other__"`. Off by default.
2. A **metric mapping/relabel stage in config**, which is Benthos/Redpanda Connect's approach — a `mapping`
   field on the metrics config that can rename or drop metrics before export `[R!]`. Cheap to add, lets a
   deployment shed cardinality without a code change.

Also, specifically about histograms: **never emit a histogram at per-stream granularity, under any escape
valve.** A histogram is 12–20 series per label combination; it is the single fastest way to a cardinality
incident. Pipeline-level latency histograms with a fixed, documented bucket set; counters and gauges only
below that.

### 11.4 RED / USE / golden signals, mapped onto a pipeline

The three standard framings and their honest mapping. The point of doing this explicitly is that a pipeline is
*not* a request-serving service, so RED needs translation and the naive translation is wrong.

| Framing | Element | Naive (wrong) mapping | `[C]` Correct mapping for a pipeline |
|---|---|---|---|
| **RED** | Rate | records/sec read | **records/sec committed.** Read rate is demand; committed rate is throughput. A pipeline reading 10k/s and committing 0/s is broken, and the read rate looks perfect. |
| | Errors | connector exceptions | records failed **by class**, + dead-lettered, + `Degraded` condition time. An exception count with no denominator and no class is unactionable. |
| | Duration | time in `poll()` | **end-to-end record latency: sink-ack time − core ingest time.** Per-stage durations are secondary diagnostics. This is the only "duration" a user cares about. |
| **USE** | Utilization | CPU | per-stage `utilization_ratio` (Vector's) — fraction of wall time doing work |
| | Saturation | queue length | `buffer_depth` / `buffer_capacity` **and** `stage_blocked_seconds_total`. Depth alone misses a stage that is blocked with an empty buffer because it cannot even *start*. |
| | Errors | — | same as RED's |
| **Golden signals** | Latency, Traffic, Errors, Saturation | | as above; the four-tile default dashboard |

`[C]` **The default dashboard is six tiles, and this is the whole frontend MVP:**

1. **Committed records/sec** (per pipeline), with read-rate ghosted behind it — the gap *is* the story.
2. **Checkpoint age** (§3.1). The single alerting metric.
3. **Lag** — whichever of the four kinds (§11.5) is available, explicitly labelled with which one, and
   rendering "not available for this source" when none is.
4. **End-to-end latency p50/p99**, with exemplar links to traces.
5. **Errors by class** (7-way stacked), plus DLQ rate.
6. **Bottleneck: `topk(1, rate(stage_blocked_seconds_total))`** — names the slow stage in words.

Plus the phase/conditions badge row and the recent-events feed from §5. Note that every one of the six works
for a fully generic source and sink, and none of them requires the core to know what the source is.

### 11.5 What "lag" means for a generic source — the definitional core of this dossier

This is the question the brief singles out, and it is the one most frameworks get wrong by picking one
definition and pretending it is universal. **There are four orthogonal quantities. They are not
interchangeable, they have different availability, and a UI that shows one labelled "lag" is misleading.**

The best prior art is **Flink's FLIP-33 "Standardize Connector Metrics"**, which is essentially the only place
this has been thought through properly for a *generic* connector API. **VERIFIED** against the FLIP-33 wiki
page.

Source metrics:

| Name | Type | Unit |
|---|---|---|
| `numBytesIn` | Counter | Bytes |
| `numBytesInPerSecond` | Meter | Bytes/Sec |
| `numRecordsIn` | Counter | Records |
| `numRecordsInPerSecond` | Meter | Records/Sec |
| `numRecordsInErrors` | Counter | Records |
| `currentFetchEventTimeLag` | Gauge | ms |
| `currentEmitEventTimeLag` | Gauge | ms |
| `watermarkLag` | Gauge | ms |
| `sourceIdleTime` | Gauge | ms |
| `pendingBytes` | Gauge | Bytes |
| `pendingRecords` | Gauge | Records |

Sink metrics: `numBytesSend` (Counter, Bytes), `numBytesSendPerSecond` (Meter), `numRecordsSend` (Counter),
`numRecordsSendPerSecond` (Meter), `numRecordsSendErrors` (Counter), `currentSendTime` (Gauge, ms).

The definitions, verbatim — and they are worth reading closely because the precision is the point:

> **`currentFetchEventTimeLag`**: "The time in milliseconds from the record event timestamp to the timestamp
> Flink fetched the record… `currentFetchEventTimeLag = FetchTime - EventTime`"
>
> **`currentEmitEventTimeLag`**: "The time in milliseconds from the record event timestamp to the timestamp the
> record is emitted by the source connector… `currentEmitEventTimeLag = EmitTime - EventTime`"
>
> **`watermarkLag`**: "The time in milliseconds that the watermark lags behind the wall clock time.
> `watermarkLag = CurrentTime - Watermark`"
>
> **`sourceIdleTime`**: "The time in milliseconds that the source has not processed any record.
> `sourceIdleTime = CurrentTime - LastRecordProcessTime`"
>
> **`pendingRecords`**: "The number of records that have not been fetched by the source. e.g. the available
> records after the consumer offset in a Kafka partition."
>
> **`pendingBytes`**: "The number of bytes that have not been fetched by the source. e.g. the remaining bytes
> in a file after the file descriptor reading position."

Three observations that directly shape canal's design:

- **Every one of the four "lag" metrics is given as an explicit arithmetic formula.** That is why they cannot
  be confused, and it is the standard canal's own metric documentation must meet: each lag metric's help text
  is a subtraction, not an adjective.
- **`pendingRecords`/`pendingBytes` are documented with two examples from completely different source
  families** — a Kafka partition offset gap and remaining bytes in a file. That is exactly the
  source-agnostic backlog abstraction canal needs, and it confirms that "backlog" generalises where "offset
  lag" does not.
- **`watermarkLag` has no canal analogue and should not get one.** It is meaningful only because Flink has a
  watermark concept in its core. canal's equivalent question ("how far has committed progress fallen behind
  wall clock") is `checkpoint_age_seconds`, which needs no event-time model at all — a strictly cheaper and
  more universally available metric.

Debezium's equivalent is a single `MilliSecondsBehindSource` `[R!]` — documented as the difference between the
source event's timestamp and the connector's processing of it, i.e. FLIP-33's `currentFetchEventTimeLag`. Kafka
Connect has only `sink-record-lag-max`, which exists *solely* because a Kafka consumer has a log-end offset,
and has **no source-side lag at all** — a direct, documented cost of its offsets being opaque maps `[S]`.

`[C]` **canal's four quantities, with availability and the absent rule:**

| # | Quantity | Definition | Available when | If unavailable |
|---|---|---|---|---|
| **1** | **Event-time lag** (freshness) | `t_stage − record.EventTime`, at a named stage (`fetch`, `emit`, `commit`) | the source populates `EventTime` | `Optional` invalid → series omitted, read model `null`, UI "source provides no event time" |
| **2** | **Backlog** (work remaining) | `pending_records` / `pending_bytes` | the source implements the `Backlogger` capability | `null`. **Never 0.** |
| **3** | **Position distance** | a source-computed scalar distance between current position and head | the source implements `PositionComparer` | `null` |
| **4** | **Internal queue depth / in-flight** | `buffer_depth`, `inflight_records` | **always** — core-owned | n/a |

plus the two always-available companions that make the others interpretable:

| **5** | **Idle time** | `now − t_last_record` | always | n/a |
| **6** | **Checkpoint age** | `now − t_last_commit` | always | n/a |

**The interface consequence, and this is the part that must be decided now:**

```go
// Optional capability. A source opts in; the core NEVER requires it and never guesses.
type Backlogger interface {
    // Backlog reports remaining known work for the currently assigned streams.
    // Must be cheap; the core polls it on a schedule (default 30s), never per record.
    Backlog(ctx context.Context) (Backlog, error)
}
type Backlog struct {
    Records Optional[int64]
    Bytes   Optional[int64]
    Exact   bool          // true = counted, false = estimated. The UI must say which.
    AsOf    time.Time     // a 30s-old backlog is fine; a silently-stale one is not
}

// Optional capability. Lets the core compute "% through" without understanding the position.
type PositionComparer interface {
    // Fraction in [0,1] of the way from `from` to `to`, or invalid if incomparable.
    Fraction(from, to Position) Optional[float64]
}
```

Four properties of this design worth defending:

1. **The core never requires positions to be comparable.** That is what keeps it source-agnostic — an LSN, a
   resume token, a page cursor, and a filename+byte-offset are all legitimate positions and only some support
   arithmetic. Connect made positions opaque and therefore *could not* compute lag `[S]`; canal makes them
   opaque *by default* and lets a source volunteer comparability. That is strictly more capable at zero cost
   to genericity.
2. **`Exact bool` is mandatory.** A `SELECT COUNT(*)` backlog and a `reltuples` estimate must not render
   identically. Airbyte's `row_estimate` `[R!]` is honest in its naming and dishonest in practice because
   nothing forces a connector to distinguish.
3. **`AsOf` is mandatory.** A backlog is a poll result. Without `AsOf` the UI shows a number and implies it is
   live.
4. **Each of the four is a *separately named* metric.** There is no metric called `canal_lag_seconds`. There
   is `canal_event_time_lag_seconds{measured_at="fetch"}`, `canal_backlog_records`,
   `canal_position_fraction`, `canal_buffer_depth`. **A UI element labelled "lag" must state which one it is
   showing.** This is the honesty rule applied to the hardest case, and it is where every other framework's
   dashboard lies.

### 11.6 `[C]` The canal metric set, in full

Names are `canal_`-prefixed, snake_case, base units, `_total` on counters. Labels only from the closed
vocabulary of §11.3. This is proposed as normative; it should land in a doc under `docs/` and be pinned by a
golden-file test against real `/metrics` output.

```
# ---- throughput (the three-counter rule of §9) --------------------------------
canal_records_read_total{pipeline,stage,connector}                  counter {record}
canal_records_emitted_total{pipeline,stage}                          counter {record}   # post-transform
canal_records_written_total{pipeline,stage,connector}                counter {record}
canal_records_committed_total{pipeline}                              counter {record}   # durable
canal_records_failed_total{pipeline,stage,class}                     counter {record}
canal_records_dropped_total{pipeline,stage,reason}                   counter {record}   # filtered | dlq
canal_records_deduplicated_total{pipeline,stage}                     counter {record}
canal_bytes_read_total{pipeline,stage,connector}                     counter By
canal_bytes_written_total{pipeline,stage,connector}                  counter By

# ---- latency & freshness ------------------------------------------------------
canal_record_latency_seconds{pipeline}                               histogram s   # ingest -> committed
canal_stage_duration_seconds{pipeline,stage,op}                      histogram s   # op: read|transform|write|commit
canal_batch_size_records{pipeline,stage}                             histogram {record}
canal_event_time_lag_seconds{pipeline,measured_at}                   gauge s       # fetch|emit|commit; OMITTED if unknown
canal_source_idle_seconds{pipeline}                                  gauge s
canal_checkpoint_age_seconds{pipeline}                               gauge s       # <-- primary alert
canal_checkpoint_commits_total{pipeline,outcome}                     counter 1
canal_checkpoint_commit_duration_seconds{pipeline}                   histogram s

# ---- progress (omitted, not zeroed, when unknown) ----------------------------
canal_backlog_records{pipeline}                                      gauge {record}
canal_backlog_bytes{pipeline}                                        gauge By
canal_backlog_exact{pipeline}                                        gauge 1       # 1 = counted, 0 = estimated
canal_position_fraction{pipeline}                                    gauge 1       # 0..1
canal_snapshot_units_total{pipeline}                                 gauge 1
canal_snapshot_units_done{pipeline}                                  gauge 1
canal_snapshot_duration_seconds{pipeline}                            gauge s

# ---- backpressure (§8) -------------------------------------------------------
canal_buffer_depth{pipeline,buffer}                                  gauge {record}
canal_buffer_capacity{pipeline,buffer}                               gauge {record}
canal_buffer_bytes{pipeline,buffer}                                  gauge By
canal_buffer_bytes_capacity{pipeline,buffer}                         gauge By
canal_buffer_refused_total{pipeline,buffer}                          counter {record}
canal_inflight_records{pipeline,stage}                               gauge {record}
canal_stage_blocked_seconds_total{pipeline,stage}                    counter s
canal_stage_utilization_ratio{pipeline,stage}                        gauge 1
canal_backoff_seconds_total{pipeline,stage,class}                    counter s
canal_pending_committables{pipeline}                                 gauge 1
canal_oldest_committable_age_seconds{pipeline}                       gauge s

# ---- state & config ----------------------------------------------------------
canal_pipeline_phase{pipeline,phase}                                 gauge 1  # 1 on the active phase, 0 others
canal_pipeline_condition{pipeline,type,status}                       gauge 1  # 6 types x 3 statuses = bounded
canal_pipeline_phase_transitions_total{pipeline,to}                  counter 1  # `to` only; {from,to} is n^2
canal_pipeline_generation{pipeline}                                  gauge 1  # spec revision
canal_pipeline_observed_generation{pipeline}                          gauge 1  # applied revision
canal_schema_changes_total{pipeline,compatibility}                    counter 1  # compatible|breaking
canal_config_reloads_total{outcome}                                   counter 1

# ---- coordination / deployment ----------------------------------------------
canal_worker_up{worker}                                              gauge 1
canal_worker_is_leader{worker}                                       gauge 1
canal_worker_assignments{worker}                                     gauge 1
canal_assignment_lease_renewals_total{worker,outcome}                 counter 1
canal_leader_elections_total                                         counter 1
canal_rebalance_duration_seconds                                     histogram s
canal_time_since_last_rebalance_seconds                              gauge s
canal_configstore_operations_total{op,outcome}                        counter 1
canal_configstore_conflicts_total                                    counter 1

# ---- meta --------------------------------------------------------------------
canal_build_info{version,commit,go_version}                          gauge 1
```

Notes on specific choices:

- **`canal_pipeline_generation` / `canal_pipeline_observed_generation`** as metrics (not just read-model
  fields) means `generation != observed_generation for 5m` is an *alert*. "My config change silently didn't
  apply" becomes a page instead of a mystery. Nothing in Connect, Vector or Airbyte can express this.
- **`canal_pipeline_condition{type,status}`** is 6 × 3 = 18 series per pipeline. Bounded, and it lets alerting
  rules be written against conditions rather than scraping the JSON read model.
- **`_transitions_total{to}` not `{from,to}`.** `{from,to}` is 8 × 8 = 64 label combinations per pipeline for
  a metric nobody graphs two-dimensionally. The n² label pair is a classic self-inflicted wound.
- **`canal_backlog_exact` as a separate 0/1 gauge** rather than an `exact="true"` label, because a label would
  split the backlog series in two whenever the source switches estimation strategy, breaking every graph.
- Deliberately absent: anything per-stream, anything per-record-key, any error-message label, any histogram
  below pipeline granularity.

### 11.7 `[C]` The read model — one document, three transports

**One canonical struct, one source of truth, three serialisations.** This is the direct answer to design-rule
R13 and to Airbyte's accidental two-API split.

```go
type PipelineStatus struct {
    Pipeline           PipelineID  `json:"pipeline"`
    Generation         Revision    `json:"generation"`          // spec revision in the store
    ObservedGeneration Revision    `json:"observedGeneration"`  // revision actually running
    AsOf               time.Time   `json:"asOf"`
    Version            uint64      `json:"version"`             // monotonic; SSE cursor + ETag
    Complete           bool        `json:"complete"`            // false => some worker did not report

    Phase       Phase              `json:"phase"`
    Conditions  []Condition        `json:"conditions"`
    Progress    Progress           `json:"progress"`
    Assignments []AssignmentStatus `json:"assignments"`         // worker, streams, lease expiry, phase
    Streams     []StreamStatus     `json:"streams,omitempty"`   // UNBOUNDED detail lives here (§11.3)
    RecentEvents []Event           `json:"recentEvents"`        // bounded ring buffer (§5)
    LastError   *ErrorInfo         `json:"lastError"`           // class, message, at, stage, stream
}

type Progress struct {
    CheckpointAt     *time.Time `json:"checkpointAt"`            // nil => never committed
    CheckpointAge    *float64   `json:"checkpointAgeSeconds"`
    RecordsRead      uint64     `json:"recordsRead"`
    RecordsWritten   uint64     `json:"recordsWritten"`
    RecordsCommitted uint64     `json:"recordsCommitted"`
    RecordsInFlight  uint64     `json:"recordsInFlight"`
    EventTimeLag     *float64   `json:"eventTimeLagSeconds"`     // nil => no event time from this source
    IdleSeconds      float64    `json:"idleSeconds"`
    Backlog          *Backlog   `json:"backlog"`                 // nil => unknowable
    Snapshot         *SnapshotProgress `json:"snapshot"`         // nil => not snapshotting
    Throughput       Throughput `json:"throughput"`              // committed/read per sec, short window
}
```

Rules that make this a contract rather than a struct:

1. **Every unknown is `nil`/`Optional`-invalid, never a zero.** And the frontend has a single shared
   `Unknown` renderer. Pinned fixtures (design-rules already asks for "deterministic read-model fixtures")
   include a case where *every* optional field is absent, and a test asserts the UI renders no zeros.
2. **`Complete bool`.** In a multi-worker deployment the aggregator may not have heard from every worker. A
   status document that silently omits a worker's contribution is the "endpoint answered ≠ data arrived"
   failure at the API level. `Complete: false` forces the UI to say "partial".
3. **`Version` is monotonic per pipeline** and doubles as the SSE resume cursor and the HTTP `ETag`.
4. **`Streams` is opt-in via a query parameter** (`?streams=true&limit=100&offset=…`) because it is the
   unbounded part. Default off keeps the default page render cheap.
5. **`ObservedGeneration` is computed from what workers report, not from what the store contains.** Otherwise
   it always equals `Generation` and is useless.

**Transport — polling vs streaming.** `[C]`

| Transport | Use |
|---|---|
| `GET /v1/pipelines/{id}/status` | snapshot; `ETag`/`If-None-Match`; the CLI and the operator use this |
| `GET /v1/pipelines/{id}/status/watch` (**SSE**) | live updates: `id:` = `Version`, event data = a JSON *delta* or a full document; a full document every N events or on reconnect |
| `GET /metrics` | Prometheus scrape; the dashboard's *time series* come from Prometheus/Grafana, not from the API |

**SSE, not WebSocket**, for four concrete reasons: it is one-directional (which is all a read model needs);
it is plain HTTP so every proxy, load balancer and auth middleware already handles it; browsers reconnect
automatically and resend `Last-Event-ID`, which maps exactly onto `Version`; and it needs no extra dependency
in a Go server. Vector chose GraphQL subscriptions over WebSocket `[R!]` and thereby took on a GraphQL server,
a subscription transport, and a schema language — for a read model that is fundamentally "one document per
pipeline".

**The discipline to copy from Vector regardless of transport:** `[R!]` **the built-in CLI status view must be
a client of the public API.** `canal status --watch` consumes the same SSE endpoint the browser does. It costs
almost nothing, it is the best integration test the read model will ever have, and it makes it impossible for
the API to drift behind the UI.

**What the API must *not* do:** serve time series. Do not build a metrics query endpoint. Prometheus (or the
OTLP pipeline) owns history; the API owns *current state* plus the recent-events ring buffer. Airbyte's and
Connect's UIs are both weaker than a Grafana dashboard at the history question and always will be. The
frontend embeds/links Grafana panels or queries Prometheus directly.

---
## 12. Deployment — standalone and enterprise from one binary

### 12.1 What each system actually does

| System | Standalone | Distributed | Coordination | Control plane |
|---|---|---|---|---|
| **Kafka Connect** | `StandaloneHerder` + `MemoryConfigBackingStore` + `FileOffsetBackingStore` `[S]` | `DistributedHerder` + compacted Kafka topics for config/offset/status `[S]` | Kafka consumer-group protocol; leader computes assignment; non-leaders **forward REST writes to the leader** `[S]` | REST on every worker |
| **Vector** | the only mode | k8s Deployment/DaemonSet + `vector-aggregator`; instances are independent | **none** | none (read-only GraphQL) |
| **Conduit** | the only mode; single binary with embedded UI and local store `[R!]` | not supported | none | gRPC + generated HTTP |
| **Airbyte** | `abctl` / docker-compose | k8s; a workload-launcher creating one Job/Pod per sync `[R!]` | Temporal (historically) / workload API | Postgres + external secret manager |
| **Debezium** | Debezium Server (single process) / Embedded Engine | via Kafka Connect distributed, or the Debezium Operator `[R!]` | inherited from Connect | inherited |

**The single most important observation across this table:** the two systems with a real control plane (Connect,
Airbyte) both put the coordinator *in the data path* in ways they later regretted, and the two without one
(Vector, Conduit) simply cannot do multi-worker pipelines at all. Nobody in this list has the property canal
needs, which is why this has to be designed rather than adopted.

Connect gets the *seam* exactly right though, and it is the model to copy: **the connector-facing interfaces
are byte-identical in both modes; what swaps is three storage implementations plus the herder** `[S]`. The
reason that swap is free is that `OffsetBackingStore` is `ByteBuffer`-in/`ByteBuffer`-out — the durability
substrate never sees a domain type `[S]`.

### 12.2 `[C]` The seam: four interfaces, two assemblies

`[C]` canal's standalone/enterprise split is **four interfaces and nothing else**. If a fifth appears, the
abstraction is wrong.

```go
type Runtime struct {
    Config      ConfigStore       // §7.4 — revisioned CAS + Watch
    Checkpoints CheckpointStore   // bytes-in / bytes-out. Connect's best idea.
    Coordinator Coordinator       // membership, assignment leases, leader election
    Status      StatusAggregator  // collect per-worker status into one PipelineStatus
}
```

```go
// Deliberately bytes-only: the durability substrate never sees a domain type.
type CheckpointStore interface {
    Get(ctx context.Context, keys [][]byte) (map[string][]byte, error)
    Set(ctx context.Context, kv map[string][]byte) error   // must be atomic across the whole map
    Delete(ctx context.Context, keys [][]byte) error
    Keys(ctx context.Context, prefix []byte) ([][]byte, error)  // for the offsets API + per-stream listing
}
```

`Set` being atomic across the map is the requirement Connect's compacted-topic store cannot meet and which
produced the documented unrecoverable state `[S]`. Every proposed backend must satisfy it: one SQL
transaction, one bbolt transaction, one etcd `Txn`.

```go
type Coordinator interface {
    // Membership: register this worker, heartbeat, observe the member set.
    Join(ctx context.Context, w WorkerInfo) (Membership, error)

    // Leader election, for PLANNING ONLY. Data flow must not depend on it.
    Campaign(ctx context.Context) (Leadership, error)

    // Assignment leases: a worker claims a planned assignment and renews it.
    Claim(ctx context.Context, a AssignmentID, w WorkerID, ttl time.Duration) (Lease, error)
    Renew(ctx context.Context, l Lease) (Lease, error)
    Release(ctx context.Context, l Lease) error
    Assignments(ctx context.Context, p PipelineID) ([]AssignmentStatus, error)
}
```

**Implementations:**

| Interface | `canal run` (standalone) | `canal serve` (enterprise) |
|---|---|---|
| `ConfigStore` | bbolt/SQLite file, or `-f pipelines.yaml` projected in | Postgres (default), etcd, k8s CRD |
| `CheckpointStore` | bbolt file (single `Set` txn = atomic) | Postgres table, etcd, object store + WAL |
| `Coordinator` | `singleNode{}` — always leader, all assignments local, leases no-ops | Postgres advisory locks + a leases table, or etcd `concurrency.Election`, or k8s `Lease` |
| `StatusAggregator` | direct in-process read | fan-out to workers over gRPC, or workers write status rows |

One binary, one flag set. `canal run` with no arguments must produce a working pipeline with a durable
checkpoint on a laptop with nothing installed — that is design-rule R3's first milestone, and it is also the
single biggest adoption lever canal has over Kafka Connect and Airbyte.

### 12.3 `[C]` Why Postgres first, not etcd

Recommendation: **the first non-embedded implementation should be Postgres, not etcd or Kafka.** One dependency
delivers every primitive this design needs:

| Need | Postgres primitive |
|---|---|
| revisioned CAS on config | `UPDATE … WHERE id=$1 AND revision=$2 RETURNING revision+1` |
| atomic multi-key checkpoint write | one transaction |
| leader election | `pg_try_advisory_lock(key)`, released automatically on session loss |
| assignment leases | a `leases` table + `UPDATE … WHERE holder=$1 OR expires_at < now()` |
| work claiming without a leader | `SELECT … FOR UPDATE SKIP LOCKED` |
| config change fan-out (`Watch`) | `LISTEN` / `NOTIFY`, with a `max(revision)` poll as fallback |
| status aggregation | a `worker_status` table with a TTL column |

etcd is a *better semantic* fit — its `ModRevision` CAS and native `Watch(fromRevision)` are literally the
interface in §7.4, and `concurrency.NewElection` / `Campaign` / `Resign` / `Observe` is literally
`Coordinator` — but it is one more thing to run. Keep etcd as a second implementation and as the conformance
target that stops the interface from acquiring Postgres-isms (the same discipline design-rules praises: remote
drivers as conformance targets rather than shipped dependencies).

**Kafka as the coordination store is explicitly rejected.** Connect's own `KafkaConfigBackingStore` javadoc
documents the unrecoverable compaction-plus-partial-write state, needing `commit-` marker records to fake
set-atomicity `[S]`. Reproducing that in 2026 with a transactional database available would be perverse.

### 12.4 `[C]` Work partitioning, and the property that matters most

Adopt Connect's two-level split (verified in the sibling dossier `[S]`): **the connector plans how work
divides; the core decides where each piece runs.** Neither learns the other's algorithm. Canal's addition is
that the plan is *durable state*, not an in-memory result of a leader's computation:

```
assignments(pipeline_id, assignment_id, spec_revision, plan_json, worker_id, lease_expires_at, generation)
```

- **The leader only plans.** It writes assignment rows. It does not route data, does not proxy status, does not
  hold anything the data path needs.
- **Workers claim rows** by CAS and renew leases. A worker whose lease lapses stops its assignment; another
  worker may claim it after expiry.
- **Therefore: the data plane keeps running with the control plane completely down.** No leader, no config
  store, no API — existing assignments keep flowing and keep checkpointing, because a worker holding a valid
  lease needs nothing from anyone until it expires. This is the single most important deployment property in
  the whole design and it is worth sacrificing elegance for. Connect approximates it (running tasks survive
  between rebalances); Airbyte historically did not (control-plane outage stops syncs).
- **Reassignment is deliberately delayed**, per KIP-415's `scheduled.rebalance.max.delay.ms` and its
  exponential backoff on consecutive revoking rounds `[S]` — a bouncing worker reclaims its own assignments
  instead of triggering a cluster-wide reshuffle. Concretely: lease TTL 30s, reassignment delay 120s,
  configurable.
- **Hard-enforce the declared parallelism cap at plan time** and fail the pipeline loudly if the planner
  exceeds it — KIP-1004 had to retrofit this after eight years of `tasks.max` being advisory `[S]`.

### 12.5 Leader election — the verified caveats that must shape the design

This is the one part of §12 verified against primary source in this pass. From the `k8s.io/client-go/tools/leaderelection` package documentation:

> **"This implementation does not guarantee that only one client is acting as a leader (a.k.a. fencing)."**

and:

> "A client only acts on timestamps captured locally to infer the state of the leader election. The client does
> not consider timestamps in the leader election record to be accurate because these timestamps may not have
> been produced by a local clock."

and on skew tolerance:

> "The tolerance expressed as a maximum tolerated ratio of time passed on the fastest node to time passed on
> the slowest node can be approximately achieved with a configuration that sets the same ratio of
> LeaseDuration to RenewDeadline. For example if a user wanted to tolerate some nodes progressing forward in
> time twice as fast as other nodes, the user could set LeaseDuration to 60 seconds and RenewDeadline to 30
> seconds."

The config struct (verified):

```go
type LeaderElectionConfig struct {
    Lock            rl.Interface      // e.g. resourcelock.LeaseLock over coordination.k8s.io/v1 Lease
    LeaseDuration   time.Duration     // non-leaders wait this long before force-acquiring   (default 15s)
    RenewDeadline   time.Duration     // acting leader retries refreshing for this long      (default 10s)
    RetryPeriod     time.Duration     // between action attempts                             (default  2s)
    Callbacks       LeaderCallbacks   // OnStartedLeading / OnStoppedLeading / OnNewLeader
    WatchDog        *HealthzAdaptor   // wires liveness to "am I still renewing"
    ReleaseOnCancel bool              // release the lock when the run context is cancelled
    Name            string
    Coordinated     bool              // Coordinated Leader Election (ALPHA)
}
```

`[C]` **Four design consequences for canal, and the first one is the load-bearing one:**

1. **"Does not guarantee fencing" means canal must not put anything correctness-critical behind leadership.**
   This is precisely why §12.4 confines the leader to *planning*. Two simultaneous leaders both writing
   assignment rows is survivable (the rows are CAS-guarded and idempotent; the second write conflicts or
   converges). Two simultaneous leaders both *writing checkpoints* or both *driving the same assignment* is
   data corruption. **The fencing token for the data path is the assignment lease, held by the worker,
   CAS-renewed — not leadership.** Design to the weaker guarantee that is actually on offer.
2. **`ReleaseOnCancel: true` plus a real shutdown path**, so a rolling restart hands leadership over in
   milliseconds instead of after a 15s lease expiry.
3. **Wire `WatchDog` into `/healthz`.** A process that believes it is leader but has stopped renewing must
   fail its liveness probe. Publish it as `canal_worker_is_leader` and
   `canal_assignment_lease_renewals_total{outcome}` so a split-brain window is *visible* — the metric set in
   §11.6 exists partly for this.
4. **Clock skew is a first-class configured policy, not an assumption.** Design-rules already lists
   "clock-skew policy: clamp or reject" as an open decision; the `LeaseDuration : RenewDeadline` ratio is the
   same knob applied to coordination, and the docs' own recommendation ("clock synchronization between nodes
   is highly recommended") should be a documented operational requirement, plus a
   `canal_clock_skew_detected_total` counter.

The equivalent primitives in the other stores: etcd `concurrency.NewSession` + `NewElection` + `Campaign` /
`Resign` / `Observe` (lease-TTL based, revision-fenced — genuinely fenced, unlike the k8s Lease helper);
Postgres `pg_try_advisory_lock` (fenced by session lifetime, which is a stronger guarantee than a
timestamp-based lease and is another argument for Postgres-first).

### 12.6 The Kubernetes operator / CRD pattern — and how to be ready without building it

Two prior-art patterns worth transplanting exactly.

**Strimzi splits runtime from job into two CRDs** `[R!]`: `KafkaConnect` (the worker cluster: image, replicas,
`spec.build` for baking plugin images) and `KafkaConnector` (one connector instance: `spec.class`,
`spec.tasksMax`, `spec.config`, `spec.pause`/`spec.state`, `spec.autoRestart`). And the status trick worth
stealing outright: **`KafkaConnector.status.connectorStatus` embeds the raw Connect REST status JSON
verbatim**, alongside `status.conditions`, `status.tasksMax`, `status.topics`, `status.observedGeneration`.

That is exactly right: the operator does not re-model the runtime's status, it **passes it through**. One
representation per entity (design-rule R1/R9), and adding a field to the read model does not require an
operator release.

**The Flink Kubernetes Operator solves "imperative action in a declarative API" with nonces.** **VERIFIED**
against the operator's CRD reference:

- `restartNonce` (`FlinkDeploymentSpec`, `FlinkSessionJobSpec`), type `java.lang.Long`:
  > "Nonce used to manually trigger restart for the cluster/session job. In order to trigger restart, change
  > the number to a different non-null value."
- `savepointTriggerNonce` (`JobSpec`), type `java.lang.Long`:
  > "Nonce used to manually trigger savepoint for the running job. In order to trigger a savepoint, change the
  > number to a different non-null value."

and the reconciliation status, which is better than I remembered and contains the idea worth stealing most:

- `reconciliationStatus.lastReconciledSpec`:
  > "Last reconciled deployment spec. Used to decide whether further reconciliation steps are necessary."
- `reconciliationStatus.lastStableSpec`:
  > "Last stable deployment spec according to the specified stability condition. **If a rollback strategy is
  > defined this will be the target to roll back to.**"
- `reconciliationStatus.state` ∈ `DEPLOYED | UPGRADING | ROLLING_BACK | ROLLED_BACK`

**`lastStableSpec` plus a rollback strategy is the idea to transplant.** The control plane retains not just
"what I last applied" but "what last actually worked", with an explicit stability condition deciding which
is which — so a bad config change can be automatically reverted instead of leaving the pipeline broken until a
human notices. `[C]` For canal that is: keep `lastStableRevision` alongside `observedGeneration`, define
"stable" as *ran for N minutes with `Progressing: True` and `Degraded: False`*, and offer
`spec.rollbackStrategy: none | auto`. This is a genuinely differentiating operator feature and it costs one
extra revision pointer in the status.

Note also that `ROLLING_BACK`/`ROLLED_BACK` are *reconciliation* states, kept in a **separate enum from the
job's own state**. Two independent lifecycles — "is the desired state applied" and "is the workload healthy" —
are never collapsed into one enum. Same lesson as §6.2.

`[C]` **canal's position: do not build the operator now; make it a thin shim later by getting three things
right in the API today.**

1. **The write model is declarative and idempotent.** `PUT /v1/pipelines/{id}` with a whole `PipelineSpec` and
   an `If-Match: <revision>`; the server diffs. **No verb endpoints for state changes** — `pause` is
   `spec.desiredState: Paused`, not `POST /pause`. This is the single decision that makes an operator a
   50-line reconcile loop instead of a state machine, and it is exactly what Airbyte's `/v1/*/create`,
   `/v1/*/update` RPC shape `[R!]` makes painful.
2. **Imperative actions are nonces in the spec**, following Flink: `spec.restartNonce`,
   `spec.snapshotTriggerNonce`, `spec.checkpointResetNonce`. Each is `int64`; the runtime records the last
   nonce it acted on in the status. This works identically over REST and as a CRD field, is naturally
   idempotent, and survives replay — which a `POST /restart` does not.
3. **Status carries `observedGeneration` + k8s-shaped `conditions`** (§6.2), so `status` maps to a CRD status
   subresource with zero translation, and the CRD's own `printcolumns` can surface phase and lag.

And the generation rule, per design-rule R8: **the CRD OpenAPI schema is generated from the same `[]Param`
specs that drive the UI and the validation** — one source, three consumers (Go validation, browser form, CRD
schema). If those ever diverge by hand, R8 has been violated.

### 12.7 `[C]` Concrete deployment surface

```
canal run                        # standalone: embedded store + bbolt + single-node coordinator + UI + /metrics
canal run -f pipelines.yaml --watch
canal serve --config-store=postgres://…  --checkpoint-store=postgres://…  --coordinator=postgres://…
canal serve --coordinator=k8s-lease --namespace=canal        # operator-adjacent
canal worker --api=…             # data plane only, no API listener  (optional split)
canal status [--watch]           # SSE client of the public API (§11.7)
canal validate -f pipelines.yaml # exit non-zero on validation failure; CI-friendly
```

Ports and endpoints, fixed early because everything else depends on them:

```
:8080  /v1/…            control plane (REST + SSE)
       /metrics         Prometheus
       /healthz         liveness  (includes leadership watchdog)
       /readyz          readiness (config store reachable, assignments claimed)
       /debug/pprof     opt-in
:8081  gRPC             worker-to-worker status fan-out (enterprise only)
```

Exactly one server-side runtime, per design-rule R11: this is all one Go binary, and the frontend is static
assets embedded with `embed.FS` so `canal run` serves the UI with no separate process, no dev proxy, and no
second deployment artifact. That directly addresses the abandoned attempt's "path-based routing split that
existed only in the frontend dev proxy".

---
## 13. What they got right / what they got wrong

### 13.1 Got right

- **Kafka Connect: `ConfigDef` is simultaneously a config schema and a UI schema** — `group`, `orderInGroup`,
  `width`, `displayName`, `importance`, `dependents`, `Validator`, `Recommender` — which is why third-party UIs
  can render a form for a connector nobody has seen `[S]`.
- **Kafka Connect: status is `(state, workerId, generation, trace)` with the stacktrace inline**, replicated so
  any worker answers for any connector `[S]`. `trace` is the single highest-value status field in any of these
  systems.
- **Kafka Connect: standalone vs distributed differ only in three storage impls plus the herder; the
  connector-facing API is byte-identical** `[S]`. The correct seam.
- **Kafka Connect: duty-cycle gauges (`running-ratio`, `pause-ratio`), in-flight depth (`*-active-count`), and
  rebalance as its own first-class metric group** `[S]`.
- **Kafka Connect: KIP-875 offsets over the REST API** — read, patch, delete — the 3am operation every other
  framework makes you do with raw store access `[S]`.
- **Kafka Connect / KIP-297: `${provider:path:key}` config indirection**, so the control-plane store never
  contains a secret `[R!]`.
- **Debezium: `Total` and `Remaining` as separate attributes; `QueueTotalCapacity` and
  `QueueRemainingCapacity`; both a record count and a byte count** — never a pre-divided ratio `[R!]`.
- **Debezium: `ChunkId`/`ChunkFrom`/`ChunkTo` exposing the current unit of work**, which is what makes a wedged
  incremental snapshot debuggable `[R!]`.
- **Debezium: `MilliSecondsBehindSource` and `MilliSecondsSinceLastEvent` as separate metrics** — freshness and
  idleness are different questions `[R!]`.
- **Flink FLIP-33: three separately-named lags** (`currentFetchEventTimeLag`, `currentEmitEventTimeLag`,
  `watermarkLag`) plus `sourceIdleTime` and `pendingRecords`/`pendingBytes` `[R!]`. The only serious attempt at
  defining lag for a *generic* connector API, and the naming refuses to conflate them.
- **Vector: `utilization` per component** — the one metric that identifies a bottleneck at a glance, not
  derivable from throughput `[R!]`.
- **Vector: `vector top` and `vector tap` are clients of the public API** `[R!]`. Best API-discipline mechanism
  in the survey.
- **Vector: partial topology diff on config reload** — rebuild only changed components `[R!]`.
- **OTel Collector: `accepted` vs `refused` as separate counters, and `send_failed` vs `enqueue_failed`**;
  `queue_size` and `queue_capacity` as separate gauges `[R!]`.
- **OTel semconv: a central attribute registry with declared types and stability levels** — the best existing
  implementation of "one concept, one vocabulary" `[R!]`.
- **Airbyte: `web_backend` as an explicit read model for the UI** — one denormalised request per screen `[R!]`.
- **Airbyte: connector-declared secret fields (`airbyte_secret`) drive platform-side secret splitting**, so the
  platform needs no per-connector knowledge `[R!]`.
- **Airbyte: `advanced_auth` declares the OAuth flow as data**, so an OAuth-specific UI needs no platform
  change per connector `[R!]`. This is the mechanism constraint #1 asks for.
- **Airbyte: progress and status as in-band messages on the data channel** (`TRACE/ESTIMATE`,
  `TRACE/STREAM_STATUS`) — the only shape that works across a process boundary `[R!]`.
- **Strimzi: `status.connectorStatus` embeds the runtime's own status document verbatim** rather than
  re-modelling it `[R!]`.
- **Flink k8s Operator: nonces for imperative actions in a declarative spec** (VERIFIED), plus
  `lastReconciledSpec` *and* `lastStableSpec` with an auto-rollback target, and reconciliation state
  (`DEPLOYED|UPGRADING|ROLLING_BACK|ROLLED_BACK`) kept in a separate enum from workload health.
- **Conduit: `PlanPipeline` + `ApplyPipeline`** (VERIFIED) — a plan/apply pair on the control plane, so the
  impact of a spec change is knowable before committing to it.
- **Conduit: `stopped_reason ∈ {USER, SYSTEM}` as a companion to `STATUS_STOPPED`, and `STATUS_RECOVERING` as a
  first-class status** (VERIFIED) — honest distinctions that would otherwise have overloaded the status enum.
- **Conduit and Vector independently made live record tapping a control-plane operation** (VERIFIED for
  Conduit: `InspectConnector`, `InspectProcessorIn`, `InspectProcessorOut` — both sides of a transform).
- **Vector's instrumentation spec writes bounded cardinality into the contract**: `error_code` "MUST be a
  bounded set with relatively low cardinality because it will be used as a metric tag" (VERIFIED), and encodes
  deliberate drops with an `intentional` tag rather than a second metric.
- **OTel makes `messaging.destination.name` conditionally required only "if low cardinality"** (VERIFIED) —
  even the convention designed around brokers refuses to mandate the per-destination dimension.
- **FLIP-33 defines every lag as an explicit subtraction** (VERIFIED) — `FetchTime - EventTime`,
  `EmitTime - EventTime`, `CurrentTime - Watermark`, `CurrentTime - LastRecordProcessTime` — which is why its
  four lags are impossible to conflate.
- **Kubernetes: `phase` + `conditions[]` + `observedGeneration`** — the most battle-tested status model there
  is, and the only one that answers "did my change take effect?" `[R!]`.
- **client-go leaderelection: documenting its own lack of fencing** (verified) — an honest limitation
  disclosure that lets callers design correctly around it.

### 13.2 Got wrong — documented pain

- **Kafka Connect: metrics are JMX-only, no HTTP endpoint**, so every real deployment bolts on a JMX exporter
  `[S]`. A permanent adoption tax for a decision that saved nothing.
- **Kafka Connect: no source-side lag metric at all.** `sink-record-lag-max` exists only because Kafka
  consumers have a log-end offset `[S]`. The direct, documented cost of the framework not understanding the
  offset map it stores.
- **Kafka Connect: no `COMPLETED` state** `[S]` — bounded/snapshot jobs are structurally second-class, which is
  why Debezium had to smuggle snapshot state through the offset map and invent an out-of-band signalling table.
- **Kafka Connect: no `observedGeneration`** — nothing in the status document tells you whether the running
  task reflects the config you just submitted, and status is eventually consistent through the status topic, so
  a `GET` right after a `POST` commonly 404s with no staleness marker `[S]`/`[R!]`.
- **Kafka Connect: the REST API returns connector configs verbatim, secrets included** `[R!]`, unless the
  operator remembered a `${provider:…}` indirection. Redaction was not derived from the declaration, so it
  does not happen.
- **Kafka Connect: `KafkaConfigBackingStore`'s own javadoc documents an unrecoverable state** — compaction plus
  a partial write leaving `commit-foo (2 tasks)` whose `task-foo-1-config` was compacted away, "leaving us in
  an inconsistent state with no obvious way to resolve the issue" `[S]`. A control plane on a compacted log.
- **Kafka Connect: rebalance storms.** KIP-415's own motivation: stop-the-world rebalancing "could bring the
  Connect cluster into a state of consecutive rebalances… several minutes to stabilize" `[S]`; the incremental
  replacement then shipped KAFKA-12495, "Unbalanced connectors/tasks distribution … in Connect's incremental
  cooperative assignor" `[S]`.
- **Kafka Connect: `RowsScanned`-style per-table structured attributes become a cardinality bomb the moment a
  JMX exporter flattens them** (Debezium's case) `[R!]` — the per-table data was never wrong, the *destination*
  was.
- **Vector: no control plane at all.** The API is read-only; config is a file you must ship yourself. Fine for
  an agent, disqualifying for a managed connector platform.
- **Vector: internal metric names churned.** The `processed_events_total` → `component_received_events_total` /
  `component_sent_events_total` migration broke dashboards and required a documented upgrade guide `[R]`. The
  lesson: **your metric names are a public API with no deprecation mechanism.**
- **Vector: GraphQL for a read model** — a schema language, a subscription transport and a server, for
  "one document per component". Cost without matching benefit at this shape.
- **Airbyte: the config API is RPC-over-POST (`/get`, `/create`)** `[R!]`, which makes a declarative operator
  awkward and means there is no idempotent whole-object write.
- **Airbyte: three overlapping APIs** (config API, public API, `web_backend`) with distinct DTOs for the same
  entities `[R!]` — the accidental read model, i.e. design-rule R9's failure at API scale.
- **Airbyte: estimates are optional, so progress bars silently lie** `[R]`. A progress indicator that is
  sometimes real and sometimes absent-but-rendered is worse than none.
- **Airbyte: state model migration (LEGACY → GLOBAL → STREAM `AirbyteStateMessage`)** was a long, painful
  transition `[R]` — the cost of not deciding per-stream checkpoint granularity up front. Canal must decide it
  before the first connector.
- **Airbyte: secrets require an external secret manager for any serious deployment**, and the local/testing
  persistence has been a recurring leak source `[R]`.
- **Conduit: single-node only.** No clustering, no HA, no work partitioning — the enterprise half of canal's
  requirement is simply absent `[R!]`.
- **Conduit: `Parameters()` has no presentation metadata** (no group/order/displayName/importance/dependents),
  so a UI can only render a flat form `[R!]` — a strictly worse position than Connect's from 2015.
- **OTel messaging semconv: still not Stable and has churned repeatedly** —
  `messaging.operation` → `messaging.operation.type` + `.name`; `messaging.kafka.message.offset` →
  `messaging.kafka.offset`; `messaging.publish.duration`/`messaging.receive.duration` →
  `messaging.client.operation.duration`; `messaging.kafka.consumer.group` →
  `messaging.consumer.group.name` `[R]`. Building a user-facing metric contract on it exports that churn.
- **OTel messaging semconv is broker-shaped**: `messaging.system` is required, destinations are assumed. It has
  no vocabulary for "a database snapshot into an object store", which is over half of canal's use cases `[R!]`.
- **Prometheus histograms are the cardinality multiplier everyone forgets** — 12–20 series per label
  combination, with buckets chosen up front and native histograms still not the default `[R!]`.
- **client-go leaderelection does not fence** (verified quote in §12.5). Any design that puts
  correctness-critical work behind "am I the leader" is wrong from the start, and this is under-appreciated
  precisely because the API looks like a mutex.

---

## 14. Steal this

- **Publish `canal_checkpoint_age_seconds` as the primary health metric** — always available, cannot be faked,
  and one alert on it catches every stall mode (source, sink, transform, coordinator, credentials, disk).
- **Keep `read`, `written` and `committed` as three separate counters, and permit the word "delivered" only for
  `committed`** — design-rule R4 turned into a testable presentation constraint.
- **Name the four lags separately and never ship a metric called `lag`**: `event_time_lag_seconds{measured_at}`
  (FLIP-33's fetch/emit split), `backlog_records`, `position_fraction`, `buffer_depth` — plus always-available
  `source_idle_seconds` and `checkpoint_age_seconds`.
- **Make "unknown" representable everywhere with `Optional[T]`, and omit the metric series entirely when it is
  invalid** — never emit 0 for an unmeasurable lag or an unknowable backlog.
- **Put backlog and position-comparison behind optional `Backlogger` / `PositionComparer` interfaces**, with
  `Exact bool` and `AsOf time.Time` mandatory on the result, so the core computes progress without ever
  understanding a position.
- **Hard-close the metric label vocabulary to pipeline topology and enforce it at registration**
  (`pipeline`, `stage`, `role`, `connector`, `worker`, `phase`, `class`, `outcome`, `buffer`, `codec`), and
  forbid `stream`/`table`/`key`/`id` outright — a plugin that declares a data-derived label fails to start.
- **Serve unbounded per-stream detail from the control-plane read model, never from the metrics registry** —
  two systems for two questions: Prometheus for bounded history, `GET .../status?streams=true` for current
  state.
- **Never publish a pre-divided ratio: emit `depth` and `capacity`, `units_total` and `units_done` separately**
  (Debezium's `QueueTotalCapacity`/`QueueRemainingCapacity`), so the consumer divides and "denominator unknown"
  stays expressible.
- **Add `canal_stage_blocked_seconds_total`** — cumulative time each stage spent unable to proceed because the
  next stage would not accept work; it turns "why is it slow" into one `topk` query that names the culprit.
- **Copy the OTel Collector's `accepted` / `refused` counter pair** so backpressure rejection is a first-class
  event distinct from failure, and `send_failed` stays distinct from `enqueue_failed`.
- **Copy Vector's `utilization` duty-cycle gauge per stage** (Connect's `running-ratio` generalised) — the
  bottleneck indicator that throughput cannot give you.
- **Model status as Kubernetes does: one coarse `Phase` plus a `Conditions[]` array** with
  `Configured | Connected | Progressing | CaughtUp | Degraded | Assigned`, each with
  `Status ∈ {True,False,Unknown}`, `Reason`, `Message`, `LastTransitionTime`, `ObservedGeneration` — so
  `Connected: True` structurally cannot imply data is moving.
- **Add `PhaseCompleted`** (Connect's missing state) so bounded snapshot and batch pipelines are first-class
  rather than infinite streams that went quiet.
- **Carry `Generation` and `ObservedGeneration` in both the read model and the metrics**, so
  "my config change silently didn't apply" is an alert rather than a mystery.
- **Ship a bounded per-pipeline ring buffer of ~200 typed events** (`schema_changed_breaking`,
  `checkpoint_edited`, `backpressure_engaged`, `assignment_lost`, …) in the read model — the cheapest
  high-value feature in the entire frontend, and the thing that turns "it's red" into a narrative.
- **One `PipelineStatus` document, three transports**: `GET` snapshot with `ETag`, `GET .../watch` as **SSE**
  keyed on a monotonic `Version` (which is also `Last-Event-ID`), and the same struct in the CLI — and a
  `Complete bool` so a partially-reported multi-worker status cannot masquerade as whole.
- **Make the built-in CLI status view a client of the public API** (`canal status --watch` over the same SSE
  endpoint the browser uses) — Vector's discipline, and the best integration test the read model will get.
- **Do not build a metrics-history API.** Current state and recent events from canal; time series from
  Prometheus/OTLP. Every framework that built its own history view lost to Grafana.
- **Instrument with the OTel metric API and export both Prometheus `/metrics` (always) and OTLP (optional)** —
  and define canal's own `canal_*` namespace rather than adopting the broker-shaped, non-Stable messaging
  semantic conventions; let connectors that *are* messaging clients emit those themselves.
- **Pin the metric contract with a golden-file test against real `/metrics` output**, and treat metric names as
  a public API with a deprecation policy — Vector's `processed_events_total` rename is the cautionary tale.
- **Emit exemplars on the latency histograms and carry a W3C `traceparent` slot in the record envelope** —
  decided now, because retrofitting a metadata slot means changing every codec.
- **Never let a connector hold a metrics registry, logger or status callback: declaration is data
  (`[]MetricSpec`), emission is data (`Telemetry` returned with each batch), status is polled
  (`Status(ctx) (ConnectorStatus, error)`)** — this is the concrete requirement that makes a future gRPC
  connector satisfy the identical interface.
- **Report plugin counters as deltas and gauges as absolutes**, so a connector restart cannot look like a
  counter reset the core has to guess about.
- **Take Connect's `ConfigDef` field set (including the presentational fields) but make it pure data**:
  declarative `Validations` instead of a `Validator` interface, `Fields []Param` / `Elem *Param` for nested and
  repeated config (fixing the `transforms.a.type` dotted-prefix hack), and a two-tier `Validate` /
  `Recommend` pair returning per-field diagnostics.
- **Add `Extensions map[string]json.RawMessage` to the parameter spec** — opaque to the core, special-cased by
  plugin name in the frontend only. That is how "specialised UI for less-generic connectors" ships with zero
  core edits, instead of Airbyte's approach of growing the platform's schema dialect every time.
- **Derive secret redaction from the declaration** (`ParamSecret`) and use an unexported-field `Secret` type
  whose `String`/`MarshalJSON` return `"***"` and whose only accessor is a grep-able `Reveal()`; store
  `${provider:path:key}` indirections, resolve at `Open` in the worker, never persist or log the resolved
  value.
- **Route connector-originated config writes (OAuth refresh, Airbyte's `CONTROL/CONNECTOR_CONFIG`) through the
  same secret-splitting path as operator writes** — no side doors.
- **Make the config store a revisioned CAS with `Watch(from Revision)`**: `Revision` simultaneously gives
  optimistic concurrency, the `generation` in the status, the watch resume token and the HTTP `ETag` — and it
  maps 1:1 onto etcd `ModRevision` and k8s `resourceVersion`, which is why the operator becomes a shim.
- **Require `CheckpointStore.Set` to be atomic across the whole map** and keep it bytes-in/bytes-out — this is
  the requirement Connect's compacted topic cannot meet, and the reason its javadoc documents an unrecoverable
  state.
- **Choose Postgres as the first non-embedded store**: revisioned CAS, atomic multi-key writes, advisory-lock
  leader election, `SKIP LOCKED` work claiming and `LISTEN/NOTIFY` watches in one dependency every enterprise
  already runs — with etcd kept as a conformance target so the interface acquires no Postgres-isms.
- **Confine the leader to *planning* and make the worker's assignment lease the fencing token**, because
  client-go's own docs state leader election "does not guarantee that only one client is acting as a leader
  (a.k.a. fencing)" — and therefore make the data plane keep flowing and checkpointing with the entire control
  plane down.
- **Treat coordination as a metered subsystem from day one** (`is_leader`, `assignments`,
  `lease_renewals_total{outcome}`, `rebalance_duration_seconds`, `time_since_last_rebalance_seconds`) and wire
  the leadership watchdog into `/healthz` so a split-brain window is visible rather than inferred.
- **Delay reassignment of orphaned work with backoff** (KIP-415's `scheduled.rebalance.max.delay.ms`) so a
  bouncing worker reclaims its own assignments instead of triggering a cluster-wide reshuffle.
- **Make the write model declarative and idempotent — `PUT` a whole spec with `If-Match`, `desiredState`
  instead of `POST /pause`, and int64 *nonces* for imperative actions** (`restartNonce`,
  `snapshotTriggerNonce`, `checkpointResetNonce`, following the Flink operator's verified
  "change the number to a different non-null value" contract) — the one decision that makes a Kubernetes
  operator a small reconcile loop rather than a state machine.
- **Offer a plan/apply pair on the control plane** (Conduit's verified `PlanPipeline` + `ApplyPipeline`): submit
  a desired spec, get the diff and the restart-vs-reload impact back, *then* commit.
- **Retain `lastStableRevision` next to `observedGeneration`, with an explicit stability condition and an
  optional auto-rollback** (the Flink operator's `lastStableSpec` — "if a rollback strategy is defined this will
  be the target to roll back to"), so a bad config change reverts itself instead of waiting for a human.
- **Keep reconciliation state in a separate enum from workload health** (`DEPLOYED | UPGRADING | ROLLING_BACK |
  ROLLED_BACK` vs the pipeline's own phase) — two independent lifecycles must never collapse into one enum.
- **Add a `stoppedReason` companion to the stopped phase** (Conduit's verified
  `STOPPED_REASON_USER | _SYSTEM`), so "a human stopped it" and "we gave up" are distinguishable without
  splitting the status enum — and add `Recovering` as a real status alongside `Running` and `Degraded`.
- **Expose live record tapping as a control-plane operation, on both sides of a transform** — Vector's
  `vector tap` and Conduit's verified `InspectConnector` / `InspectProcessorIn` / `InspectProcessorOut`
  converged on this independently; "what went in, what came out" is the most useful debugging primitive an API
  can offer and costs a bounded sampling buffer plus a stream.
- **Encode deliberate drops with an `intentional` boolean tag rather than a separate metric** (Vector's
  verified `discarded_events_total` requirement), and write "MUST be a bounded set with relatively low
  cardinality" into canal's own metric spec for every tag, as Vector does for `error_code`.
- **Document every lag metric's help text as an arithmetic formula, not an adjective** — FLIP-33's
  `currentFetchEventTimeLag = FetchTime - EventTime` is the standard to meet, and it is why its four lags cannot
  be confused with each other.
- **When the operator does arrive, embed canal's own status document verbatim in the CR status** (Strimzi's
  `status.connectorStatus`) instead of re-modelling it, and generate the CRD schema from the same `[]Param`
  specs that drive the UI.
- **Expose checkpoint read/edit over the API from the start** (KIP-875's shape: read live, edit only when
  stopped, connector may validate the edit) and **audit every edit as an event** with actor, before and after.
- **Ship the frontend as `embed.FS` static assets inside the one Go binary**, so `canal run` on a laptop serves
  the UI, the API, `/metrics` and the data plane from a single process with no dev proxy and no second
  artifact.

---

## Appendix: open questions this dossier does not close

1. **Per-stream checkpoint granularity** — one checkpoint per pipeline, or per stream? Airbyte's LEGACY →
   GLOBAL → STREAM migration `[R]` is the cautionary tale; this must be decided before the first connector, and
   it determines whether `StreamStatus.checkpointAt` can exist at all.
2. **Status aggregation transport in enterprise mode** — workers pushing status rows into the store (simple,
   stale by the write interval) vs the API fanning out gRPC calls (fresh, fails partially, needs `Complete`).
   I lean push-to-store with a short TTL plus `Complete`, but it is a real trade.
3. **Whether `canal_pipeline_condition{type,status}` should exist at all**, or whether conditions should stay
   read-model-only and alerting should be driven from `checkpoint_age` + `phase`. 18 series per pipeline is
   cheap, but it duplicates state across two systems.
4. **Event log durability** — the ring buffer is in-memory and dies with the worker. Durable events need a
   table and a retention policy, and it is not obvious that they need it before v1.
5. **Multi-tenancy in the label set.** design-rules R13 requires tenancy be decided before the first
   multi-tenant field. A `tenant` metric label is bounded in principle and unbounded in practice; my instinct
   is tenancy scopes the *API* and never appears as a metric label, but that needs a decision record.
6. **Native histograms vs fixed buckets** for `canal_record_latency_seconds`, and the bucket set if fixed.









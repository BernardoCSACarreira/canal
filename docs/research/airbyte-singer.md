# Prior art: the Singer spec and the Airbyte protocol

> **Status: DRAFT — provenance warning, read this first.**
>
> This dossier was written during a **total outage of every network-capable tool** in the session
> (`WebFetch`, `WebSearch`, `Bash`+`curl`, `git clone`, and the browser MCP tools all returned
> *"claude-sonnet-5 is temporarily unavailable, so auto mode cannot determine the safety of …"* for
> the entire run; ~40 retries across ~25 minutes, batched and spaced). **Nothing below was fetched
> from primary source in this run.** By contrast, `docs/research/kafka-connect.md` was written with
> raw sources cached on disk and every signature read from the file.
>
> Everything here is therefore **model knowledge, not verified quotation**. It is written with
> confidence labels so it can be used immediately for design work and re-verified cheaply later:
>
> - **[HC]** high confidence — structural facts I would bet on (message type names, the command set,
>   the state-type trichotomy, field names that are load-bearing in the ecosystem).
> - **[MC]** medium confidence — right in shape, possibly wrong in a name, a default, or a version.
> - **[LC]** low confidence — directionally true, do not quote.
>
> **Anything presented in a fenced code block is a RECONSTRUCTION, not a quote**, unless the fence is
> explicitly labelled `verified`. There are no `verified` fences in this file. Section 15 is the
> re-verify checklist with exact URLs; §16 lists what a re-run must replace.
>
> Design conclusions (§13, §14) are robust to the small errors that may exist above them — they turn
> on architecture, not on spellings.

## Scope

Two message-protocol-first connector frameworks, analysed as **interface design artifacts**:

1. **Singer** (2016–, Stitch Data; spec repo `singer-io/getting-started`, reference library
   `singer-io/singer-python`). A ~3-page specification. Taps and targets are *programs*; the
   interface is a **newline-delimited JSON stream on stdout/stdin**.
2. **The Airbyte protocol** (2020–, `airbytehq/airbyte-protocol`, `airbytehq/airbyte`, Python CDK now
   split out as `airbytehq/airbyte-python-cdk`). A JSON-Schema-defined message protocol plus a
   five-command CLI contract, layered over Docker images; the Python/Java CDKs then supply the
   *in-process* abstractions (`Source`, `Stream`, `Destination`) that connector authors actually
   write against.

Both are the **opposite** of canal's chosen plugin boundary: they put the process boundary *first*
and derived the object model afterwards. That inversion is exactly why they are worth studying —
canal's constraint #3 ("the interface must be designed so an out-of-process implementation can later
satisfy the SAME interface") is asking for the union of Kafka Connect's in-process ergonomics and
Airbyte's wire-shippability. Airbyte and Singer show what the wire-first half costs and what it buys.

The single most important observation, stated up front: **Airbyte's protocol boundary forced every
piece of connector metadata to become data** — config is a JSON Schema, the stream set is a declared
catalog, sync behaviour is catalog-level, errors are typed messages, even config *mutation* is a
message. That is why Airbyte has a genuinely connector-agnostic UI with hundreds of connectors and
zero core knowledge of any of them. **canal should adopt that "everything is data" discipline while
keeping in-process Go calls.** The costs — throughput, expressiveness, state size, no backpressure —
are also documented below and are avoidable.

---

## 1. Core interfaces

### 1.1 Singer: there is no interface, there is a program contract

Singer's "interface" is a CLI + a stdout format. There is no type to implement. **[HC]**

A **tap**:

```
tap-foo --config CONFIG [--state STATE] [--catalog CATALOG] [--discover]
```

- `--config CONFIG` — path to a JSON file. **Required.** Contents are entirely tap-defined; the spec
  says only that it holds whatever the tap needs (credentials, `start_date`, …). **There is no schema
  for config anywhere in Singer.** **[HC]**
- `--state STATE` — path to a JSON file containing the last STATE value the tap emitted (in practice,
  the last STATE the *target* echoed). Optional. **[HC]**
- `--catalog CATALOG` — path to the catalog selecting streams/fields to sync. Optional. Older taps
  used `--properties` for the same thing; both spellings exist in the wild. **[MC on `--properties`]**
- `--discover` — run discovery: print a catalog to stdout and exit. **[HC]**

A **target**:

```
target-foo --config CONFIG
```

reads the tap's stdout on **stdin**, and writes STATE messages to **its own stdout**. **[HC]**

The whole composition model is a shell pipe:

```sh
tap-foo --config tap.json --state state.json --catalog catalog.json \
  | target-bar --config target.json \
  > state_out.jsonl
# then: tail -1 state_out.jsonl > state.json     (the orchestrator's job)
```

**[HC]** — this pipeline, including the `tail -1` idiom, is the canonical Singer usage and is the
thing Meltano/`singer-runner`/Stitch each wrapped.

Three consequences that matter for interface design:

1. **stdout is the interface, so stdout must be pure.** Logging goes to **stderr**. A tap that prints
   a stray line to stdout corrupts the stream. **[HC]** There is no LOG message type in Singer.
2. **There is no `check`/`spec`/`validate` command.** Connection validation is "run the tap and see if
   it exits non-zero". **[HC]**
3. **Errors are `exit != 0` plus whatever went to stderr.** There is no error taxonomy, no structured
   failure, no distinction between a bad credential and a rate limit. **[HC]** This is the single
   largest expressiveness gap versus Airbyte.

The reference library `singer-python` provides emit helpers rather than an interface. Reconstructed
signatures — **[MC]**, names are right, defaults may drift:

```python
# singer/messages.py  — RECONSTRUCTION, not a quote
class Message: ...
class RecordMessage(Message):
    def __init__(self, stream, record, version=None, time_extracted=None): ...
class SchemaMessage(Message):
    def __init__(self, stream, schema, key_properties, bookmark_properties=None): ...
class StateMessage(Message):
    def __init__(self, value): ...
class ActivateVersionMessage(Message):
    def __init__(self, stream, version): ...

def write_message(message): ...
def write_record(stream_name, record, stream_alias=None, time_extracted=None): ...
def write_records(stream_name, records): ...
def write_schema(stream_name, schema, key_properties, bookmark_properties=None, stream_alias=None): ...
def write_state(value): ...
def write_version(stream_name, version): ...
def parse_message(msg): ...
def format_message(message): ...
```

Note the shape: **free functions that print to stdout.** There is no object a framework holds, so
there is nothing to inject a context, a cancellation token, a metrics handle, or a rate limiter into.
Every cross-cutting concern in Singer is therefore either absent or reimplemented per tap. **[HC]**

### 1.2 Airbyte: five commands, one message union

The protocol-level interface is a **five-command CLI**, each command writing newline-delimited
`AirbyteMessage` JSON to stdout. **[HC]**

| Command | Args | Emits |
|---|---|---|
| `spec` | — | `AirbyteMessage{type: SPEC, spec: ConnectorSpecification}` |
| `check` | `--config <path>` | `AirbyteMessage{type: CONNECTION_STATUS, connectionStatus: {status, message}}` |
| `discover` | `--config <path>` | `AirbyteMessage{type: CATALOG, catalog: AirbyteCatalog}` |
| `read` | `--config <path> --catalog <configured_catalog_path> [--state <path>]` | stream of `RECORD` / `STATE` / `LOG` / `TRACE` / `CONTROL` |
| `write` | `--config <path> --catalog <configured_catalog_path>` | reads stdin; emits `STATE` / `LOG` / `TRACE` / `CONTROL` |

Sources implement `spec`, `check`, `discover`, `read`. Destinations implement `spec`, `check`,
`write`. **[HC]**

This is the crisp part worth transplanting to Go: **the connector surface is five pure-ish
operations, four of which are pre-flight metadata and only one of which moves data.** Compare Kafka
Connect, where `config()`, `validate()`, `taskConfigs()` and the data path are spread across two
abstract classes and a context. Airbyte's split is:

- `spec` — *what config do I take?* (no config needed)
- `check` — *is this config usable?* (config only)
- `discover` — *what streams exist under this config?* (config only)
- `read` / `write` — *move data* (config + configured catalog + state)

Note what is **absent** from the command set and never got added: no `plan`/`split` command (so no
connector-declared parallelism — see §12), no `abort`, no `commit`, no `health`, no `metrics`. **[HC]**

The `AirbyteMessage` union — reconstruction of the JSON-Schema definition **[HC on members]**:

```yaml
# airbyte_protocol.yaml — RECONSTRUCTION
AirbyteMessage:
  type: object
  required: [type]
  properties:
    type:
      type: string
      enum: [RECORD, STATE, LOG, SPEC, CONNECTION_STATUS, CATALOG, TRACE, CONTROL]
    log:              { "$ref": "#/definitions/AirbyteLogMessage" }
    spec:             { "$ref": "#/definitions/ConnectorSpecification" }
    connectionStatus: { "$ref": "#/definitions/AirbyteConnectionStatus" }
    catalog:          { "$ref": "#/definitions/AirbyteCatalog" }
    record:           { "$ref": "#/definitions/AirbyteRecordMessage" }
    state:            { "$ref": "#/definitions/AirbyteStateMessage" }
    trace:            { "$ref": "#/definitions/AirbyteTraceMessage" }
    control:          { "$ref": "#/definitions/AirbyteControlMessage" }
```

**A tagged union with one optional payload field per tag** — the classic "protobuf `oneof` written in
JSON Schema" shape. This is important prior art for canal: it means the *control plane* and the *data
plane* share one channel, so a source can interleave a record, a log line, a progress estimate, a
typed error and a config update on the same stream, in order, with no side channel. Kafka Connect has
no equivalent — control there means "restart the task". **[HC]**

### 1.3 Airbyte CDK: the abstractions authors actually implement

The protocol is not what connector authors write. The Python CDK supplies the in-process interface
and the CDK *entrypoint* translates it to the protocol. This two-layer design (a wire protocol plus
a language-level SDK that generates it) is exactly the shape canal wants inverted (a Go interface
plus a future wire shim that satisfies it).

Reconstructed CDK hierarchy — **[MC]** on exact signatures, **[HC]** on the concept split:

```python
# airbyte_cdk/connector.py — RECONSTRUCTION
class BaseConnector(ABC, Generic[TConfig]):
    @abstractmethod
    def configure(self, config: Mapping[str, Any], temp_dir: str) -> TConfig: ...
    @staticmethod
    def read_config(config_path: str) -> Mapping[str, Any]: ...
    @abstractmethod
    def spec(self, logger: logging.Logger) -> ConnectorSpecification: ...
    @abstractmethod
    def check(self, logger: logging.Logger, config: TConfig) -> AirbyteConnectionStatus: ...

# airbyte_cdk/sources/source.py — RECONSTRUCTION
class Source(BaseConnector[Mapping[str, Any]], ABC):
    def read_state(self, state_path: str) -> List[AirbyteStateMessage]: ...
    def read_catalog(self, catalog_path: str) -> ConfiguredAirbyteCatalog: ...
    @abstractmethod
    def read(self, logger, config, catalog: ConfiguredAirbyteCatalog,
             state: Optional[List[AirbyteStateMessage]] = None) -> Iterable[AirbyteMessage]: ...
    @abstractmethod
    def discover(self, logger, config) -> AirbyteCatalog: ...

# airbyte_cdk/sources/abstract_source.py — RECONSTRUCTION
class AbstractSource(Source, ABC):
    @abstractmethod
    def check_connection(self, logger, config) -> Tuple[bool, Optional[Any]]: ...
    @abstractmethod
    def streams(self, config: Mapping[str, Any]) -> List[Stream]: ...
    # provided: discover() builds the catalog from streams(); read() drives each stream,
    # emits records, and interleaves STATE at checkpoint boundaries.
```

**The load-bearing reduction:** an `AbstractSource` author implements exactly **two** methods —
`check_connection` and `streams` — and everything else (catalog construction, state routing,
checkpoint emission, stream status traces, record counts) is framework work. **[HC]** That is the
extensibility property canal's constraint #4 demands, achieved by making the *stream* the unit of
extension rather than the *connector*.

`Stream` is the real interface. Reconstruction — **[MC]** on signatures, **[HC]** on the member set:

```python
# airbyte_cdk/sources/streams/core.py — RECONSTRUCTION
class Stream(ABC):
    @property
    def name(self) -> str: ...                       # defaults to casing-converted class name

    @abstractmethod
    def read_records(self,
                     sync_mode: SyncMode,
                     cursor_field: Optional[List[str]] = None,
                     stream_slice: Optional[Mapping[str, Any]] = None,
                     stream_state: Optional[Mapping[str, Any]] = None,
                     ) -> Iterable[StreamData]: ...

    def get_json_schema(self) -> Mapping[str, Any]: ...     # default: load schemas/<name>.json
    def as_airbyte_stream(self) -> AirbyteStream: ...

    @property
    def supports_incremental(self) -> bool: ...             # == bool(self.cursor_field)
    @property
    def is_resumable(self) -> bool: ...                     # resumable full refresh support
    @property
    def cursor_field(self) -> Union[str, List[str]]: ...     # default: [] == not incremental
    @property
    @abstractmethod
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]: ...
    @property
    def namespace(self) -> Optional[str]: ...
    @property
    def source_defined_cursor(self) -> bool: ...            # default True
    @property
    def state_checkpoint_interval(self) -> Optional[int]: ...# emit STATE every N records

    def stream_slices(self, *, sync_mode: SyncMode,
                      cursor_field: Optional[List[str]] = None,
                      stream_state: Optional[Mapping[str, Any]] = None,
                      ) -> Iterable[Optional[Mapping[str, Any]]]: ...   # default: [None]

    def get_updated_state(self, current_stream_state, latest_record): ...  # DEPRECATED
    def check_availability(self, logger, source=None) -> Tuple[bool, Optional[str]]: ...
    def get_cursor(self) -> Optional[Cursor]: ...
```

Two mixins carry state, and their shape is unusually clean:

```python
# airbyte_cdk/sources/streams/core.py — RECONSTRUCTION  [HC on shape]
class IncrementalMixin(ABC):
    """A stream that manages its own cursor state as a property."""
    @property
    @abstractmethod
    def state(self) -> MutableMapping[str, Any]: ...
    @state.setter
    @abstractmethod
    def state(self, value: MutableMapping[str, Any]) -> None: ...

class CheckpointMixin(ABC):          # newer name/generalisation of the same idea  [MC]
    @property
    @abstractmethod
    def state(self) -> MutableMapping[str, Any]: ...
    @state.setter
    @abstractmethod
    def state(self, value: MutableMapping[str, Any]) -> None: ...
```

**This is the most transplantable single idea in the CDK:** state is a **property with a getter and a
setter**, not a return value threaded through a call chain. The framework *writes* the setter at
startup (restore) and *reads* the getter whenever it decides to checkpoint. The connector never
decides when a checkpoint happens and never has to plumb state through its own call graph. Compare
Airbyte's own deprecated alternative — `get_updated_state(current_stream_state, latest_record)`,
called per record, which forced a fold over records into the connector and made "state after a slice"
inexpressible. **The property replaced the fold, and the deprecation is documented.** **[HC on the
deprecation, MC on exact removal version]**

The destination interface is one method:

```python
# airbyte_cdk/destinations/destination.py — RECONSTRUCTION  [MC]
class Destination(Connector, ABC):
    @abstractmethod
    def write(self,
              config: Mapping[str, Any],
              configured_catalog: ConfiguredAirbyteCatalog,
              input_messages: Iterable[AirbyteMessage],
              ) -> Iterable[AirbyteMessage]: ...
```

Read that signature carefully, because it is the whole delivery-guarantee design in one line:

- input is an **`Iterable[AirbyteMessage]`**, not `Iterable[Record]` — the destination sees STATE
  messages inline with records;
- output is an **`Iterable[AirbyteMessage]`** — and what a destination yields is **the STATE messages
  it has durably persisted past**;
- there is **no return value per record, no batch accounting, no ack** — a generator that yields
  state is the entire acknowledgement channel. **[HC]**

So the destination contract is: *consume the interleaved stream; when you have durably committed
everything up to and including a STATE message you consumed, yield that STATE message.* That single
convention gives Airbyte at-least-once with sink-driven checkpointing — and it is a **strictly better
shape than Kafka Connect's `void put(...)` + `preCommit()` pair**, because the acknowledgement
*carries the position*, so the core needs no separate offset-tracking data structure at all. The
identical convention in Singer ("targets echo STATE") predates it. **[HC]**

### 1.4 The comparison that matters for canal

| | Singer | Airbyte | Kafka Connect | canal (implied) |
|---|---|---|---|---|
| Unit of extension | a program | a **stream** inside a connector | a `Connector`+`Task` pair | TBD — streams are the better unit |
| Config self-description | none | JSON Schema (`connectionSpecification`) | `ConfigDef` | needs one of the two |
| Stream set | discovered catalog | discovered catalog | none (connector-internal) | catalog |
| Who chooses sync mode | metadata in the catalog | **`ConfiguredAirbyteCatalog`**, per stream | connector config strings | configured catalog |
| Checkpoint owner | the target echoes it | the destination yields it | the framework commits it | sink-acked, core-committed |
| Error model | exit code | `AirbyteTraceMessage` w/ `failure_type` | `RetriableException` marker | typed taxonomy (design-rules) |
| Backpressure | OS pipe buffer | OS pipe buffer | consumer pause / none | must be explicit |
| Out-of-process | native | native | impossible | must remain possible |

The row that should shape canal's core: **"who chooses sync mode"**. Airbyte's answer —
*the catalog does, per stream, at configure time* — is the design decision that makes one connector
serve full-refresh, incremental, dedup, and snapshot-then-stream pipelines without the connector
branching on pipeline type. See §7 and §4.

---

## 2. Record model

### 2.1 Singer RECORD

Reconstruction **[HC on fields]**:

```json
{"type": "RECORD",
 "stream": "users",
 "time_extracted": "2017-11-20T16:45:33.000Z",
 "version": 1509135183000,
 "record": {"id": 0, "name": "Chris"}}
```

| Field | Req | Meaning |
|---|---|---|
| `type` | yes | literal `"RECORD"` |
| `stream` | yes | name of the stream this record belongs to; must match a prior SCHEMA `stream` |
| `record` | yes | **a JSON object** — the data |
| `version` | no | integer, table version for full-table replication (pairs with ACTIVATE_VERSION) |
| `time_extracted` | no | RFC3339 string, when the tap pulled it |

That is the entire envelope. **[HC]** Observe what is missing and never got added:

- **no key** — the primary key lives in the *SCHEMA* message (`key_properties`), not the record;
- **no operation type** — no insert/update/delete discriminator. Deletes are unrepresentable in core
  Singer; taps that support them add a `_sdc_deleted_at` column *inside* `record` by convention;
- **no before-image**;
- **no headers/metadata map** — provenance is faked with `_sdc_*` prefixed columns injected into the
  payload (`_sdc_extracted_at`, `_sdc_batched_at`, `_sdc_table_version`, `_sdc_sequence`,
  `_sdc_primary_key`). **[MC on the exact `_sdc_` list]** These are a *target* convention, not spec.
- **no raw-bytes path at all.** `record` must be a JSON object; binary data must be base64 in a
  string field. **[HC]**

**Lesson: with no metadata slot on the envelope, every cross-cutting field ends up polluting the
user's schema.** Singer's `_sdc_*` columns are the exact failure canal's design-rule R2 is about — the
wire DTO became the canonical model, so metadata had nowhere to live but the payload.

### 2.2 Airbyte `AirbyteRecordMessage`

Reconstruction **[HC on the four core fields, MC on `meta`]**:

```yaml
AirbyteRecordMessage:
  type: object
  required: [stream, data, emitted_at]
  properties:
    namespace: { type: string, description: "namespace the data is associated with" }
    stream:    { type: string, description: "stream the data is associated with" }
    data:      { type: object, description: "record data" }
    emitted_at:{ type: integer, description: "when the data was emitted, ms since epoch" }
    meta:      { "$ref": "#/definitions/AirbyteRecordMessageMeta" }

AirbyteRecordMessageMeta:
  properties:
    changes:
      type: array
      items: { "$ref": "#/definitions/AirbyteRecordMessageMetaChange" }

AirbyteRecordMessageMetaChange:
  required: [field, change, reason]
  properties:
    field:  { type: string }
    change: { enum: [NULLED, TRUNCATED] }
    reason: { enum: [SOURCE_RECORD_SIZE_LIMITATION, SOURCE_FIELD_SIZE_LIMITATION,
                     SOURCE_SERIALIZATION_ERROR, SOURCE_RETRIEVAL_ERROR,
                     DESTINATION_RECORD_SIZE_LIMITATION, DESTINATION_FIELD_SIZE_LIMITATION,
                     DESTINATION_SERIALIZATION_ERROR, DESTINATION_TYPECAST_ERROR,
                     PLATFORM_SERIALIZATION_ERROR] }
```

Key/value: **`(namespace, stream)` addresses the stream; `data` is an untyped JSON object; there is no
key field on the record.** The primary key lives in the **catalog** (`primary_key` on
`ConfiguredAirbyteStream`, `source_defined_primary_key` on `AirbyteStream`) — so **record identity is
a pipeline-configuration concern, not a record-envelope concern.** **[HC]**

This is a real, deliberate design position and it is *the* thing to weigh for canal:

- **Airbyte's way:** identity is declared once, per stream, in the catalog. The record stays a bare
  payload. A destination doing dedup reads `primary_key` from its configured catalog and projects it
  out of `data` itself. Advantage: the envelope stays tiny and the same record bytes work for
  append and for dedup. Disadvantage: **the key must be re-extracted from the payload by every
  consumer**, dedup is impossible without the catalog in hand, and a key change is a catalog change
  (which in Airbyte means a reset — see §13).
- **Kafka Connect's way:** `keySchema`+`key` on every record, independent of any config.
- **canal:** wants both properties — a key *slot* on the envelope (so a sink needs no catalog to
  dedup, and no re-parse) *plus* a catalog-declared key *specification* (so the UI can show it and the
  core can validate it). Populating the slot from the spec is the source-adapter's job, done once.

**Operation type: absent from the Airbyte envelope too.** **[HC]** There is no `op` field. CDC
connectors (Airbyte's Postgres/MySQL/Mongo sources, which wrap Debezium) encode deletes as a
`_ab_cdc_deleted_at` field inside `data`, alongside `_ab_cdc_lsn`, `_ab_cdc_updated_at`,
`_ab_cdc_cursor`. **[MC on the exact field names]** — i.e. **exactly the same failure as Singer's
`_sdc_*`, reinvented**, and it is why the `append_dedup` destination sync mode has to special-case
CDC deletions in destination code rather than reading a protocol-level op.

Airbyte also carries a **`generation_id` / `minimum_generation_id` / `sync_id`** triple on the
configured stream (Refreshes era, ~2024) so destinations can distinguish "records from this refresh
generation" from older ones and implement truncate-and-reload safely. **[MC]** That is a *sync-scoped*
label, not a per-record op, and it exists because the envelope could not express "this record replaces
prior state".

**`meta.changes` is the interesting late addition and is worth stealing.** It is a per-record,
per-field **lossiness log**: "this field was NULLED because the source could not serialize it", "this
field was TRUNCATED because the destination has a size limit". **[MC on enum members]** Canal's design
rules demand honesty in the UI (R"honesty as a structural property"); a machine-readable per-record
degradation record is precisely the mechanism that makes "your data arrived, but three fields were
truncated" assertable rather than prose.

### 2.3 Raw bytes vs structured data

Both protocols are **structured-only by construction**: `record` / `data` are JSON objects.
**[HC]** Consequences:

- Binary payloads must be base64-in-a-string. Cost is ~33% inflation plus an encode/decode per record.
- **A byte-passthrough pipeline is impossible.** In Kafka Connect, `ByteArrayConverter` +
  `Schema.OPTIONAL_BYTES_SCHEMA` gives you a zero-transform passthrough; in Airbyte/Singer every
  record is parsed to a JSON object and re-serialized at every hop.
- Airbyte later bolted on **file transfer** for file-based sources/destinations (a mode where the
  record references a file staged out-of-band rather than carrying its bytes) precisely because the
  envelope cannot carry blobs. **[MC on the flag/field names — believed to involve a
  `file`/`file_reference` field on the record and a `supportsFileTransfer` capability]** Flagging
  this as needing verification; the *existence* of an out-of-band file path is **[HC]**.

**For canal:** the envelope must have a **payload representation that admits raw bytes without a
structured decode**, because "generic source, generic sink, pluggable codec" (goal: serialization) is
undermined the moment the core forces a JSON object. Concretely: payload as `[]byte` + a codec
identifier + an optional already-decoded structured view, with the codec being a pipeline-level
plugin (Connect's `key.converter`/`value.converter` model). Airbyte and Singer both prove the
alternative is a dead end — Airbyte's own connector authors ended up needing an out-of-band file
channel to get around it.

### 2.4 What the record model should be, distilled

Airbyte and Singer agree on the good half: **the record names its stream and nothing else about the
topology**; a record does not know its destination, its partition, its offset, or its pipeline. That
is the right level of ignorance and canal should copy it.

They agree on the bad half too: **payload-only envelope ⇒ metadata pollutes the payload**. Both
ecosystems independently converged on prefixed magic columns (`_sdc_*`, `_ab_cdc_*`, `_airbyte_*`).
Canal should pay the one-field cost up front: an explicit metadata map, an explicit op/kind
discriminator, an explicit key slot, and an optional before-image slot.

---

## 3. Checkpoint model

This is the section with the most transplantable material, and also Airbyte's most documented pain.

### 3.1 Singer: STATE is an opaque blob whose meaning is undefined by the spec

```json
{"type": "STATE", "value": {"bookmarks": {"users": {"updated_at": "2017-11-20T16:45:33.000Z"}}}}
```

The spec's position — paraphrase, **[HC on substance]**: a STATE message's `value` is an arbitrary
JSON object; **the semantics are not part of the specification and are determined independently by
each tap**; a tap should emit STATE periodically; the tap will be handed the last STATE back via
`--state` on the next invocation.

That is the whole model. Three properties follow:

1. **Total opacity, total genericity.** The framework never interprets `value`. Any tap can checkpoint
   anything. This is the same choice Kafka Connect made with `Map<String,?> sourceOffset`, but with no
   type restriction at all (Connect restricts to primitives; Singer allows arbitrary nesting).
2. **Zero framework help.** There is no per-stream routing, no merge, no partial commit, no "reset one
   stream". The orchestrator gets one blob and hands one blob back. **[HC]**
3. **The convention had to be invented afterwards, in a library.** `singer-python`'s
   `singer/bookmarks.py` defines the de-facto shape — reconstruction, **[MC on signatures, HC on the
   state shape]**:

```python
# singer/bookmarks.py — RECONSTRUCTION
def ensure_bookmark_path(state, path): ...
def write_bookmark(state, tap_stream_id, key, val): ...
def get_bookmark(state, tap_stream_id, key, default=None): ...
def clear_bookmark(state, tap_stream_id, key): ...
def reset_stream(state, tap_stream_id): ...
def set_offset(state, tap_stream_id, offset_key, offset_value): ...
def get_offset(state, tap_stream_id, offset_key, default=None): ...
def clear_offset(state, tap_stream_id): ...
def set_currently_syncing(state, tap_stream_id): ...
def get_currently_syncing(state, default=None): ...
```

producing:

```json
{"currently_syncing": "users",
 "bookmarks": {
   "users":  {"updated_at": "2017-11-20T16:45:33.000Z"},
   "orders": {"updated_at": "2017-11-19T00:00:00.000Z",
              "offset": {"page": 4}}}}
```

Two things in that shape are genuinely good design and both were **discovered by the ecosystem, not
specified**:

- **`bookmarks` keyed by `tap_stream_id`** — per-stream state, so one stream's failure does not
  invalidate another's progress. Airbyte later made this a first-class protocol concept (`STREAM`
  state, §3.2) *after* shipping the flat version and suffering for it.
- **`offset` nested inside a stream's bookmark** — a *sub-stream-position* for resuming a partially
  completed page/chunk, distinct from the cursor value. This is exactly the two-level
  "stream cursor + intra-batch resume point" that a chunked snapshot needs (§4).
- **`currently_syncing`** — a single-stream "in progress" marker, so a restart knows which stream was
  mid-flight. A crude but real phase indicator.

**[HC]** that all three are the singer-python convention; **[MC]** that none of them appear in SPEC.md.

### 3.2 Airbyte: three state types, and the migration between them is the lesson

Reconstruction **[HC]**:

```yaml
AirbyteStateMessage:
  type: object
  properties:
    type:   { "$ref": "#/definitions/AirbyteStateType" }
    stream: { "$ref": "#/definitions/AirbyteStreamState" }
    global: { "$ref": "#/definitions/AirbyteGlobalState" }
    data:   { type: object, description: "(Deprecated) the state data" }
    sourceStats:      { "$ref": "#/definitions/AirbyteStateStats" }
    destinationStats: { "$ref": "#/definitions/AirbyteStateStats" }

AirbyteStateType:
  type: string
  enum: [GLOBAL, STREAM, LEGACY]
  # GLOBAL: one shared state blob + per-stream states, all committed together
  # STREAM: per-stream state, independently committable
  # LEGACY: opaque blob for the whole connection (the pre-per-stream design)

AirbyteStreamState:
  required: [stream_descriptor]
  properties:
    stream_descriptor: { "$ref": "#/definitions/StreamDescriptor" }
    stream_state:      { type: object }

AirbyteGlobalState:
  required: [stream_states]
  properties:
    shared_state:  { type: object }
    stream_states: { type: array, items: { "$ref": "#/definitions/AirbyteStreamState" } }

StreamDescriptor:
  required: [name]
  properties:
    name:      { type: string }
    namespace: { type: string }

AirbyteStateStats:
  properties:
    recordCount: { type: number }   # records preceding this state message
```

**The trichotomy is the design artifact worth studying.** Reading it as a decision table:

| Type | Shape | Commit granularity | Fits |
|---|---|---|---|
| `LEGACY` | one opaque blob for the whole connection | all-or-nothing | the original naive design; deprecated |
| `STREAM` | `(stream_descriptor, stream_state)` per stream | **per stream, independently** | API sources, table-per-stream JDBC incremental |
| `GLOBAL` | `shared_state` + `[stream_states]` | **atomic across all streams** | CDC — one WAL/oplog/binlog position shared by every table |

**`GLOBAL` exists because CDC broke `STREAM`.** With a single replication log, per-stream positions
are not independently advanceable: you cannot commit "table A up to LSN 500" without also committing
"table B up to LSN 500", because rewinding the log to re-read A would re-deliver B. `shared_state`
holds the log position; `stream_states` holds per-table auxiliary state (e.g. "this table's initial
snapshot is complete"). **[HC on the motivation, MC on Debezium-specific contents.]**

**This is the single most important checkpoint insight in this dossier for canal.** Canal must support
"multiple pipeline types" including CDC and hybrid snapshot-then-stream. A checkpoint model with only
per-stream positions **cannot express a shared log position**, and a model with only a global blob
**cannot express independent per-stream progress** (so one slow stream pins everything, and resetting
one stream resets all). Airbyte needed *both*, arrived at `STREAM` first, and had to add `GLOBAL`
plus a `LEGACY` compatibility path. **Design the two-level shape (optional shared position +
per-stream positions) into canal's checkpoint from commit one**, rather than discovering it later:

```go
// The shape Airbyte converged on, in canal terms (illustrative, not a proposal):
type Checkpoint struct {
    Shared  []byte                    // optional; nil for sources with no shared position
    Streams map[StreamID][]byte        // per-stream opaque positions
}
// Commit atomicity is a property the source declares, not something the core guesses.
```

Note also `sourceStats.recordCount` / `destinationStats.recordCount`: **the state message carries how
many records preceded it**, on both sides. **[MC on field names, HC on existence and purpose]** That
is how the platform computes "records committed" without inspecting records, and how it cross-checks
source-emitted against destination-persisted counts. It is a tiny field that turns a checkpoint into a
reconciliation point. **Steal it.**

### 3.3 Who owns the checkpoint, and when it commits

The Airbyte/Singer answer is the same and it is a good answer:

1. The **source** decides *where* checkpoints may fall — by choosing when to emit a STATE message into
   the record stream. State frequency **is** checkpoint policy. In the CDK this is
   `Stream.state_checkpoint_interval` (emit STATE every N records) plus one STATE at the end of each
   `stream_slices()` slice. **[MC on the exact CDK behaviour, HC on the mechanism]**
2. The **destination** decides *when* a checkpoint becomes durable — by yielding a STATE message from
   `write()` only after everything preceding it is committed downstream. **[HC]**
3. The **platform** decides *what happens to it* — it persists the last state it received from the
   destination and passes it to the next `read` via `--state`. **[HC]**

**Position-carrying acknowledgement is the whole trick.** The ack *is* the position, so there is no
offset-tracking data structure in the core, no `SubmittedRecords`-style low-water-mark computation, no
out-of-order ack problem — because ordering is guaranteed by the stream and the destination only
yields monotonically. This is dramatically simpler than Kafka Connect's machinery and it is achievable
in canal *if* the sink interface returns the position it has committed rather than `void`.

The price, and it is real: **the destination can only acknowledge at the granularity the source
offered.** If a source emits STATE every 10,000 records, the destination cannot commit at 500 even if
it has durably written 500 — there is no position to name. Checkpoint granularity is therefore
min(source's STATE frequency, destination's batch size), and **the source, which knows nothing about
the destination, sets the upper bound.** Airbyte connectors tune `state_checkpoint_interval` blind.

**canal should let the sink request a checkpoint boundary** (a `RequestCheckpoint()`-style backchannel,
like Connect's `SinkTaskContext.requestCommit()`), so the granularity is negotiated rather than
dictated by the uninformed side.

### 3.4 Restart

Singer: the orchestrator persists the last STATE line (`tail -1`) to a file and passes `--state` next
run. **The durability of the checkpoint is the shell's problem.** **[HC]** If you forget the
redirection, you silently get a full re-sync every time — a famous Singer footgun.

Airbyte: the platform persists the last destination-yielded state in its own database, keyed by
connection, and supplies it as `--state`. `Source.read()` receives `state: List[AirbyteStateMessage]`
— a **list**, because per-stream state is a set of independent messages. The CDK then routes each
message to the matching stream by `stream_descriptor`, and (for `IncrementalMixin` streams) assigns it
through the `state` **setter**. **[MC on the routing implementation, HC on the list-typed parameter.]**

Two details worth copying:

- **`--state` is a file path, not a flag value**, so state can be arbitrarily large without hitting
  argv limits — a lesson Airbyte learned the hard way in the LEGACY era. **[MC]**
- The restored state is delivered **through the same setter the connector uses at runtime**, so there
  is exactly one code path for "state in" and no separate `initialize`-time restore hook. Kafka
  Connect has two (constructor-time `offsetStorageReader()` read vs runtime emission); Airbyte's is
  one. **[HC on the setter, MC on there being no other path.]**

### 3.5 Interaction with sink acknowledgement, and the honest failure mode

The contract is exactly canal's design rule R4 ("an acknowledgement means durable"), stated as a
protocol convention rather than enforced by a type: *a destination must not yield a STATE message
until the records before it are durable.* **[HC that this is the documented expectation.]**

And here is the documented cost: **it is a convention, not a mechanism.** A destination that buffers
records in memory and yields STATE eagerly produces exactly the R4 catastrophe — the platform advances
the source position past data that was never written. Airbyte has no way to verify it; the protocol
cannot express "durable". The `destinationStats` counter is a *reconciliation* aid after the fact, not
a guarantee.

Canal should make this checkable rather than conventional: if the sink's `Commit` returns the position
it has durably written, and the core only advances the checkpoint on that return value, then
"acknowledge before durable" requires a sink to *lie about a return value* rather than merely to be
sloppy about when it forwards a message it was handed.

---

## 4. Snapshot handling

### 4.1 The four-way matrix, and why it is catalog-level

Airbyte's answer to "batch vs streaming vs snapshot-then-stream" is **a 2×3 product of enums on the
configured catalog**, not a pipeline type and not a connector capability. **[HC]**

```yaml
SyncMode:              { enum: [full_refresh, incremental] }              # source side
DestinationSyncMode:   { enum: [append, overwrite, append_dedup] }        # destination side
# `overwrite_dedup` was added later (Refreshes era)  [MC]
```

Combinations, with the names the docs use **[HC on the four classic ones]**:

| `sync_mode` | `destination_sync_mode` | Behaviour |
|---|---|---|
| `full_refresh` | `overwrite` | classic snapshot: read everything, replace the target |
| `full_refresh` | `append` | read everything, append — history of snapshots |
| `incremental` | `append` | CDC/log/cursor tail, append-only — the change log |
| `incremental` | `append_dedup` | tail + upsert by `primary_key`, keeping latest per key (SCD-ish) |

**Three properties of this design are the transplantable ones:**

1. **The mode is per stream, not per pipeline.** One connection can full-refresh five small lookup
   tables and incrementally tail two large ones, in the same sync, with no pipeline-type branching
   anywhere. Canal's goal list says "multiple *types* of pipelines"; Airbyte's answer is that pipeline
   type is **not a type at all — it is a per-stream configuration of an otherwise uniform engine.**
   That is a strictly better factoring than N pipeline classes and it satisfies design-rule R1
   ("topology is data, never schema") for the *mode* axis.
2. **Source-side and destination-side modes are orthogonal enums.** The source does not know whether
   the destination will overwrite, append, or dedup; the destination does not know whether the records
   it receives came from a scan or a log tail. That orthogonality is what makes M sources × N sinks
   combinatorially free. **This is the single cleanest thing in the Airbyte protocol.**
3. **Capability is declared, selection is configured.** `AirbyteStream.supported_sync_modes` is what
   the *connector* can do (discovered); `ConfiguredAirbyteStream.sync_mode` is what the *operator*
   chose. The core validates the second against the first. Similarly
   `ConnectorSpecification.supported_destination_sync_modes` for destinations. So an impossible
   pipeline is rejected at configure time. **[HC]**

### 4.2 Snapshot-then-stream handoff: not modelled, and it shows

Here is the gap. **There is no protocol-level concept of "first snapshot, then tail".** **[HC]**
`sync_mode` is a single value; there is no phase, no ordered pair, no transition event.

So how do Airbyte's CDC sources do it? Entirely inside the connector, hidden behind
`sync_mode: incremental`:

- the first sync sees empty state, so the connector internally reads the log position, runs an initial
  consistent snapshot, then switches to reading the log from the saved position;
- the "which phase am I in" flag lives in the **state blob** — per-stream entries in
  `AirbyteGlobalState.stream_states` marking a table's initial load complete, with the log position in
  `shared_state`. **[MC on the exact contents]**
- the platform sees an undifferentiated `incremental` sync and cannot report snapshot progress,
  cannot parallelise the snapshot differently from the tail, and cannot resume a snapshot at a
  different parallelism.

**That is the same workaround Kafka Connect forces (smuggle phase through the offset map), reached by a
different route.** Two independent, mature frameworks both ended up encoding the snapshot phase in the
opaque checkpoint because neither modelled phases. That is strong evidence canal should model them:
a source declares a *sequence of phases* per stream, the core knows which phase a stream is in, and
"snapshot complete → switch to incremental" is a core-visible transition with a core-visible
checkpoint boundary.

### 4.3 Resumability: `is_resumable` and "resumable full refresh"

Airbyte's most instructive retrofit. **[HC on existence, MC on details]**

The original design: a `full_refresh` stream that failed at 90% restarted from 0, because full refresh
had **no state**. Explicitly: state was an incremental-only concept. For a 200M-row table behind a
flaky API this is fatal.

The fix, "resumable full refresh" (RFR, ~2024): allow a `full_refresh` stream to emit and consume state
too, so it can resume mid-scan. The protocol surface added:

- **`AirbyteStream.is_resumable`** — a boolean the connector declares in the catalog, meaning "I can
  resume a full refresh from a checkpoint". **[HC on the field name and meaning]**
- On the CDK side, `Stream.is_resumable` and the generalisation of `IncrementalMixin` into
  `CheckpointMixin` — the *same* `state` getter/setter now used for full-refresh cursors too.
  **[MC on the class name/timing]**

Two lessons, both sharp:

1. **"Snapshot has no state" was a false economy that cost a protocol change.** Snapshot progress *is*
   a checkpoint; there is nothing special about it. A design that says "checkpointing is for
   incremental pipelines" will be retrofitted. Canal should make **every** phase checkpointable,
   including a full scan, from the start.
2. **The retrofit was cheap only because the state channel was already generic.** Because
   `stream_state` is an opaque object and STATE messages already interleaved with records, resuming a
   full refresh required *no new message type* — only a capability flag and a new convention about who
   emits state. **Opaque, uninterpreted checkpoint payloads bought Airbyte the ability to add a whole
   new phase model without a wire break.** That is a strong argument for canal keeping the checkpoint
   payload opaque bytes at the core boundary (while still having *comparable* positions for lag — see
   the kafka-connect dossier's conclusion).

### 4.4 Chunking and parallelism

- **Chunking: yes, but connector-internal.** `Stream.stream_slices()` is the CDK's chunk enumerator:
  it yields a sequence of opaque `Mapping[str, Any]` slices, and `read_records()` is called once per
  slice with `stream_slice=`. **[MC on signature, HC on the concept]** Typical uses: one slice per
  day for a date-partitioned API, one per parent record for a child stream, one per PK range for a
  chunked table scan. State is emitted at slice boundaries, which makes slices the natural checkpoint
  granularity.

  **`stream_slices()` is a split enumerator that the platform cannot see.** It is a generator inside
  the connector process; the protocol has no message for "here are my splits". So slices give
  resumability and checkpoint granularity but **not distribution**.

- **Parallelism across processes: essentially none.** **[HC]** One `read` invocation per connection
  per sync, one process, and the CDK reads streams **sequentially** by default. There is no split
  assignment, no worker fan-out, no way for the platform to run stream A on one worker and stream B on
  another within a sync. Concurrency was later added *inside* the CDK process (a concurrent source /
  thread-pool framework over `Cursor`/partition abstractions) — **[MC on names]** — which is
  in-process parallelism, not distributed parallelism.

**The structural reason is worth naming precisely:** because the protocol's unit of work is
"the whole sync as one subprocess writing one ordered stdout stream", **ordering and checkpointing are
guaranteed by the single stream, and that guarantee is exactly what forbids parallelism.** Airbyte
traded distribution for a trivially correct checkpoint model. Kafka Connect made the opposite trade
(N tasks, so it needed `SubmittedRecords` low-water-mark machinery).

**Canal cannot take either deal wholesale.** It needs Connect's split/assign model *and* Airbyte's
position-carrying acknowledgement. The synthesis: **splits are the unit of ordering.** Within a split,
ordering holds and the sink's returned position is a valid watermark (Airbyte's simplicity, per split);
across splits, the core composes per-split checkpoints into one `Checkpoint{Shared, Streams}` (Airbyte's
`GLOBAL`/`STREAM` shape) and assigns splits to workers (Connect's model). This is the concrete design
recommendation this whole section exists to produce.

---

## 5. Schema handling

### 5.1 Discovery: an explicit command, and this is why the UI works

Both systems have a **discovery step that returns a catalog**, and this is the largest single
capability Kafka Connect lacks. **[HC]**

Singer, `--discover` → stdout catalog. Reconstruction **[HC on the top-level shape, MC on some
metadata keys]**:

```json
{"streams": [
  {"tap_stream_id": "users",
   "stream": "users",
   "key_properties": ["id"],
   "bookmark_properties": ["updated_at"],
   "schema": {"type": "object",
              "properties": {"id": {"type": "integer"},
                             "name": {"type": ["null", "string"]},
                             "updated_at": {"type": ["null", "string"],
                                            "format": "date-time"}}},
   "metadata": [
     {"breadcrumb": [],
      "metadata": {"inclusion": "available",
                   "selected": true,
                   "table-key-properties": ["id"],
                   "valid-replication-keys": ["updated_at"],
                   "forced-replication-method": null,
                   "replication-method": "INCREMENTAL",
                   "replication-key": "updated_at",
                   "schema-name": "public",
                   "database-name": "mydb",
                   "is-view": false,
                   "row-count": 12345}},
     {"breadcrumb": ["properties", "id"],
      "metadata": {"inclusion": "automatic"}},
     {"breadcrumb": ["properties", "name"],
      "metadata": {"inclusion": "available", "selected": true}}]}]}
```

The **`breadcrumb`** mechanism is the clever bit and is genuinely worth stealing: metadata is a **flat
list of `(json-pointer-ish path, key-value map)` pairs**, where `[]` addresses the stream and
`["properties", "<field>"]` addresses a field. **[HC]** Consequences:

- annotations attach to arbitrary depth in a JSON Schema without mutating the schema;
- the *schema* stays a pure, valid JSON Schema that any validator can consume — annotations do not
  pollute it with `x-` extensions;
- the list is trivially diffable and mergeable, which is how "the tap discovered these, the user chose
  those" round-trips through the same document.

And the **discovered vs chosen split** is explicit **[HC on the concept, MC on the exact partition of
keys]**:

| Written by the **tap** (discovered) | Written by the **user/orchestrator** (chosen) |
|---|---|
| `inclusion` (`available` / `automatic` / `unsupported`) | `selected` |
| `selected-by-default` | `replication-method` (`FULL_TABLE` / `INCREMENTAL` / `LOG_BASED`) |
| `valid-replication-keys` | `replication-key` |
| `forced-replication-method` | `view-key-properties` |
| `table-key-properties`, `schema-name`, `database-name`, `is-view`, `row-count` | |

`inclusion: automatic` means "this field is required for replication (it's a key or the cursor) and
will be sent whether or not you selected it" — a **connector-asserted non-negotiable**. That is a
nice, small mechanism: the connector can constrain the operator's choices *in data*, not in code.

Airbyte's equivalent, reconstruction **[HC]**:

```yaml
AirbyteCatalog:
  required: [streams]
  properties:
    streams: { type: array, items: { "$ref": "#/definitions/AirbyteStream" } }

AirbyteStream:
  required: [name, json_schema]
  properties:
    name:                     { type: string }
    json_schema:              { type: object }      # JSON Schema of the record `data`
    supported_sync_modes:     { type: array, items: { "$ref": "#/definitions/SyncMode" } }
    source_defined_cursor:    { type: boolean }     # source dictates the cursor; user cannot pick
    default_cursor_field:     { type: array, items: { type: string } }
    source_defined_primary_key: { type: array, items: { type: array, items: { type: string } } }
    namespace:                { type: string }
    is_resumable:             { type: boolean }

ConfiguredAirbyteCatalog:
  required: [streams]
  properties:
    streams: { type: array, items: { "$ref": "#/definitions/ConfiguredAirbyteStream" } }

ConfiguredAirbyteStream:
  required: [stream]
  properties:
    stream:                { "$ref": "#/definitions/AirbyteStream" }
    sync_mode:             { "$ref": "#/definitions/SyncMode", default: full_refresh }
    cursor_field:          { type: array, items: { type: string } }
    destination_sync_mode: { "$ref": "#/definitions/DestinationSyncMode", default: append }
    primary_key:           { type: array, items: { type: array, items: { type: string } } }
    generation_id:         { type: integer }   # Refreshes era  [MC]
    minimum_generation_id: { type: integer }   # Refreshes era  [MC]
    sync_id:               { type: integer }   # Refreshes era  [MC]
```

**`AirbyteCatalog` vs `ConfiguredAirbyteCatalog` is the cleanest expression of "discovered vs chosen"
in any of the three frameworks studied.** Two distinct types, one embedding the other:

- `AirbyteStream` = what the source *found and can do* (schema, `supported_sync_modes`,
  `source_defined_cursor`, `source_defined_primary_key`, `is_resumable`);
- `ConfiguredAirbyteStream` = what the operator *chose*, **wrapping the discovered stream verbatim**
  (`sync_mode`, `cursor_field`, `destination_sync_mode`, `primary_key`).

Because the configured form *contains* the discovered form, the connector receives both its own
discovery output and the operator's selections in one document at `read` time, and the core can diff
"what discovery says now" against "what the config was built against" to detect drift. **[HC]**

Note the doubled fields: `source_defined_primary_key` (discovered) vs `primary_key` (chosen),
`default_cursor_field` (discovered) vs `cursor_field` (chosen), with `source_defined_cursor: true`
meaning "my choice is not overridable". That is the same connector-asserted-non-negotiable pattern as
Singer's `inclusion: automatic` and `forced-replication-method`. **Both ecosystems independently
invented it, which means canal needs it: a discovered field must be able to say "not your call".**

`primary_key` and `cursor_field` are `array of array of string` — a **list of JSON paths**, each path a
list of segments, so composite keys and nested cursors both work. **[HC]** Small but important: a
scalar `string` key field would have needed a breaking change for composite keys, and a flat
`[]string` would have needed one for nested fields.

### 5.2 Propagation: out-of-band, once per sync

**This is the sharpest contrast with Kafka Connect.** Connect puts the schema **in-band, on every
record**. Airbyte puts it **out-of-band, in the catalog, once**. Singer puts it **in-band but
sparsely** — a SCHEMA message that applies to all subsequent RECORDs of that stream until superseded.
**[HC]**

| | Where | Frequency | Can it change mid-sync? |
|---|---|---|---|
| Kafka Connect | on each record | every record | yes, naturally |
| Singer | SCHEMA message | once per stream, re-emittable | **yes** — emit another SCHEMA |
| Airbyte | `json_schema` in the catalog | once, at configure time | **no** |

Singer's position is the interesting middle and is under-appreciated: **SCHEMA is a stream-scoped,
re-emittable, in-band message.** A tap that hits a DDL change can just emit a new SCHEMA and continue;
targets are expected to handle it (a JDBC target will `ALTER TABLE`). This gets in-band evolution
without per-record schema overhead. **[HC on the mechanism; MC on how many targets actually honour
re-emitted SCHEMA — in practice, poorly.]**

Airbyte's position costs it real expressiveness: because the schema is fixed in the configured catalog
for the duration of the sync, **a mid-sync schema change cannot be expressed in the protocol at all.**
The record `data` is just a JSON object, so a source *can* emit fields not in the schema — and then
the destination's behaviour is undefined/lossy (this is part of what `meta.changes` and the
`_airbyte_data` raw column exist to absorb).

### 5.3 Evolution / drift

- **Singer core: nothing.** Re-emit SCHEMA and hope. **[HC]**
- **Airbyte core: nothing at the protocol level; a *platform* feature above it.** Airbyte's
  platform re-runs `discover` on a schedule, **diffs** the new `AirbyteCatalog` against the stored
  one, and applies a per-connection policy (`Propagate all` / `Propagate columns only` /
  `Detect and pause` / `Ignore`). **[MC on the exact policy names]** Breaking changes (a removed
  cursor field, a removed primary key) **pause the connection and require operator action**.
- **Type widening is the destination's job**, via Airbyte's "Typing and Deduping": records land in a
  raw table (`_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`, `_airbyte_generation_id`,
  `_airbyte_data` as a JSON blob) and are then typed into a final table, with values that fail to cast
  recorded in `_airbyte_meta.changes` rather than dropped. **[MC on the exact column list, HC on the
  raw-then-typed architecture and on `_airbyte_meta` carrying cast failures.]**

**The transplantable insight is the policy-not-mechanism framing.** Drift handling requires (a) a
re-runnable discovery, (b) a stored catalog to diff against, and (c) a declared per-pipeline policy.
Airbyte has all three *because discovery is a first-class command whose output is a persisted
document*. Kafka Connect has none of them because it has no discovery command. **Canal should make
`Discover(ctx, config) (Catalog, error)` a required source method and persist its output**, purely so
that drift becomes a diff. That single decision unlocks: UI stream pickers, drift detection, "you
selected a column that no longer exists" validation, and an auditable record of what the pipeline was
configured against.

### 5.4 JSON Schema as the type system: honest accounting

Airbyte types records with **JSON Schema** (draft-07 in practice). **[MC on the draft version]**

What it buys:
- one language for *both* config and data schemas, so one validator, one UI renderer, one codegen path;
- ubiquitous tooling in every language;
- structural nesting for free (objects, arrays, `oneOf`).

What it costs, and Airbyte hit all of these:
- **no integer/decimal precision.** `{"type": "number"}` cannot express `DECIMAL(38,9)`; JSON numbers
  are IEEE doubles in most parsers, so large integers and exact decimals silently lose precision. This
  is a recurring, documented complaint against both Singer and Airbyte. **[HC that this is a real,
  widely-reported problem; MC on Airbyte's current mitigation.]**
- **no native temporal types.** Dates/timestamps are `{"type": "string", "format": "date-time"}`, and
  the `airbyte_type` extension keyword (`timestamp_with_timezone`, `timestamp_without_timezone`,
  `time_with_timezone`, `time_without_timezone`, `integer`, `big_integer`, `big_number`) was added to
  disambiguate. **[MC on the exact member list, HC on the existence of an `airbyte_type` extension
  keyword for exactly this reason.]**
- **no binary type** (see §2.3).
- `oneOf`/`anyOf` unions are legal JSON Schema but map to nothing in a relational destination.

**Lesson for canal:** JSON Schema is the right choice for **config** (§7) and the wrong-but-tempting
choice for **data**. If canal's record schema is JSON Schema, it will need the same
`airbyte_type`-style escape hatch within a year. A small closed type system with explicit precision
(Connect's `Schema.Type` + logical-type parameters, or Arrow's type list) is the better data-schema
substrate — and it can still be *rendered* as JSON Schema for the UI.

---

## 6. Lifecycle

### 6.1 The lifecycle is the process lifecycle. That is the whole design.

```
platform: docker run <image> spec                     → parse SPEC       → exit
platform: docker run <image> check --config c.json     → parse STATUS    → exit
platform: docker run <image> discover --config c.json   → parse CATALOG   → exit
platform: docker run <image> read --config c.json --catalog cc.json --state s.json
              │
              └── stdout ──▶ platform ──▶ stdin of `docker run <dest> write --config d.json --catalog cc.json`
                                                            │
                                                            └── stdout (STATE) ──▶ platform persists
```

**[HC]** Every command is a fresh process. There is no `configure` → `open` → `read` → `commit` →
`close` sequence, because **process start *is* open and process exit *is* close.**

This has a genuinely underrated benefit: **there is no lifecycle contract to get wrong.** Compare Kafka
Connect, whose `SourceTask.stop()` contract has been documented-as-broken for seven-plus years
(KIP-419: `stop()` may be called more than once, and `poll()`/`commit()` may still be called after it).
Airbyte and Singer have no equivalent bug class *by construction*: there is exactly one entry point,
one exit, and the OS guarantees the teardown. Resource cleanup is `defer`-equivalent inside `main`.

And the corresponding cost, which is large:

- **No warm start.** Every sync pays container pull/create/start, interpreter boot, and connection
  setup. For a 30-second sync of a small API this is dominated by overhead. **[HC]**
- **Nothing can be kept between syncs** — no connection pool, no cached schema, no compiled query, no
  rate-limiter token bucket. All of it must be re-derived from `config` + `state` each run.
- **No pause/resume.** Kafka Connect has `PAUSED` as a target state; Airbyte's equivalent is "kill the
  container" and "start a new one". **[HC]** A paused Airbyte connection is a *scheduling* state, not a
  connector state.
- **No reconfigure.** Config changes take effect on the next sync, always.

### 6.2 Context and cancellation

**There is none at the protocol level.** **[HC]** Cancellation is `SIGTERM` then `SIGKILL` to the
container; a deadline is an external timeout the platform enforces by killing the process. Inside the
CDK, Python generators mean cancellation is "stop iterating", which unwinds `finally` blocks — the
Python-idiomatic equivalent of `defer`.

Notably: **`read` has no deadline parameter and no way for the platform to say "wrap up cleanly at a
checkpoint".** A killed sync loses everything since the last destination-yielded STATE. For a source
with `state_checkpoint_interval = 10000` and a slow destination, that can be a lot. Airbyte's mitigation
is entirely "checkpoint more often".

**For canal this is the clearest instruction in the whole dossier:** `ctx context.Context` on every
blocking method, plus an explicit *graceful* signal distinct from cancellation — "stop at the next
checkpoint boundary" is a different request from "stop now", and neither Airbyte nor Connect can express
the first one. Design-rule R3 demands a checkpoint that survives `kill -9`; the complementary
requirement is a *clean* stop that loses nothing at all.

### 6.3 Error classification: `AirbyteTraceMessage` is the best artifact in the protocol

Singer has nothing: exit code, stderr. **[HC]**

Airbyte has a typed error channel. Reconstruction **[HC on the four trace types and on
`failure_type`; MC on exact enum spellings]**:

```yaml
AirbyteTraceMessage:
  required: [type, emitted_at]
  properties:
    type:       { enum: [ERROR, ESTIMATE, STREAM_STATUS, ANALYTICS] }
    emitted_at: { type: number }          # millis since epoch, double
    error:         { "$ref": "#/definitions/AirbyteErrorTraceMessage" }
    estimate:      { "$ref": "#/definitions/AirbyteEstimateTraceMessage" }
    stream_status: { "$ref": "#/definitions/AirbyteStreamStatusTraceMessage" }
    analytics:     { "$ref": "#/definitions/AirbyteAnalyticsTraceMessage" }

AirbyteErrorTraceMessage:
  required: [message]
  properties:
    message:          { type: string }   # user-facing, safe to display
    internal_message: { type: string }   # developer-facing
    stack_trace:      { type: string }
    failure_type:     { enum: [system_error, config_error, transient_error] }
    stream_descriptor:{ "$ref": "#/definitions/StreamDescriptor" }

AirbyteEstimateTraceMessage:
  required: [name, type]
  properties:
    name:           { type: string }
    type:           { enum: [STREAM, SYNC] }
    namespace:      { type: string }
    row_estimate:   { type: integer }
    byte_estimate:  { type: integer }

AirbyteStreamStatusTraceMessage:
  required: [stream_descriptor, status]
  properties:
    stream_descriptor: { "$ref": "#/definitions/StreamDescriptor" }
    status:            { enum: [STARTED, RUNNING, COMPLETE, INCOMPLETE] }

AirbyteAnalyticsTraceMessage:
  required: [type]
  properties:
    type:  { type: string }
    value: { type: string }
```

Four things here are directly transplantable and canal should take all four.

**(a) The two-message split of error text.** `message` is *user-facing and safe to display*;
`internal_message` + `stack_trace` are *developer-facing*. **[HC]** This single distinction is what
lets a UI show "Your API key does not have permission to read Contacts" instead of a Python traceback,
without the connector having to choose one audience. Canal's design rules require a `degraded`/`paused`
state "carrying a clear last-error string" — this is the shape that string should have: **two strings,
not one.**

**(b) `failure_type` as the retry/ownership classification.** Three values, and the semantics are the
useful part:
- `config_error` — **the user must fix something.** Do not retry. Surface it on the config form.
- `system_error` — the connector or the platform is broken. Do not retry blindly; this is an
  engineering bug.
- `transient_error` — retry with backoff; nothing is wrong with the configuration. **[MC — I believe
  `transient_error` is a later addition than the other two.]**

Compare Kafka Connect's entire vocabulary: `RetriableException` vs everything else. Airbyte's three
values answer a *different and better* question — not "should the framework retry?" but **"whose
problem is this?"** — which is the question an operator UI needs to answer. Canal's design rules
already specify a richer seven-class taxonomy (transient-upstream, transient-internal,
permanent-upstream, permanent-mapping, permanent-contract, duplicate-idempotent-success, clock-skew).
Airbyte validates the *approach* and adds one refinement worth keeping: **the class should determine
both the retry behaviour and the operator-facing presentation, and it should be attached to the error at
the point it is raised, by the connector**, not inferred by the core from an exception type.

**(c) `stream_descriptor` on the error.** An error is attributable to a stream, so one failing stream
does not have to present as "the whole connection failed". **[HC]** Airbyte's platform uses this for
partial success: a sync where 6 of 7 streams completed reports the 7th as `INCOMPLETE` and keeps the 6
committed states. That is the "partial failure shape" canal's design-rule R7 says must be written at the
same time as the success shape — and note that it works because *state is per-stream* (§3.2). **Per-stream
state and per-stream error attribution are the same design decision seen from two sides.**

**(d) `STREAM_STATUS` as an explicit per-stream state machine on the wire.**
`STARTED → RUNNING → COMPLETE | INCOMPLETE`. **[HC]** This is the `COMPLETED` state Kafka Connect
famously lacks, at *stream* granularity rather than task granularity. It gives the UI, for free: which
streams have started, which are still running, which finished cleanly, which died — without inspecting
records or logs. Canal needs exactly this, and the design rules' connector state machine
(`healthy → degraded → paused → terminal`) should be joined by a **per-stream** progress machine, because
they answer different questions.

**And `ESTIMATE` deserves its own note.** A source can emit `row_estimate` / `byte_estimate` for a
stream or the whole sync, *before or during* the read. **[HC]** That is how the UI shows a progress bar
and an ETA for a snapshot. It is a tiny message and it is the only reason "43% of 12.4M rows" is
displayable for a source whose position is otherwise opaque. The kafka-connect dossier flags exactly
this gap ("there is no source-side lag metric"). **Airbyte's answer is better than making positions
comparable: let the connector declare an estimate, because only the connector can compute it cheaply.**

### 6.4 Retry

Retry lives **entirely above the protocol**. **[HC]** The platform reruns the whole `read`/`write`
process pair on failure, with the last committed state, subject to a configured attempt limit. There is
no in-process retry contract, no `RetriableException` equivalent, no per-record retry, and no DLQ in the
protocol. Inside the CDK there are HTTP-level retry/backoff helpers with rate-limit handling
(`should_retry`, `backoff_time`, `max_retries`, and a newer declarative error-handler with
`ResponseAction ∈ {SUCCESS, RETRY, FAIL, IGNORE}` and a `RATE_LIMITED` action). **[MC on names]**

**The "rerun the whole process" retry model is only viable because checkpoints are frequent and
processes are cheap-ish.** It also means: **no partial-batch retry, no per-record error routing, no
dead-letter path.** A single poison record fails the sync, every time, forever, until someone changes
the config. This is a real and frequently-reported operational problem and canal should not copy it —
Connect's `errors.tolerance` + DLQ + `ErrantRecordReporter` is the better prior art there, and
`AirbyteRecordMessageMeta.changes` (§2.2) is the better prior art for *lossy-but-continue*.

---

## 7. Config model

### 7.1 Singer: nothing. Consequences are instructive.

Config is "a JSON file with whatever the tap needs". **No schema, no declaration, no validation, no
introspection.** **[HC]**

Therefore:
- **a generic UI is impossible.** You cannot render a form for a tap you have not hard-coded. Every
  Singer UI (Stitch's, Meltano's) maintains **its own out-of-band registry of per-tap config
  descriptions**. Meltano's `discovery.yml`/plugin definitions exist precisely to fill this hole.
  **[HC]**
- **there is no `check`**, so "are these credentials right?" means running a sync.
- **secrets are undeclared**, so nothing knows which fields to redact in logs or encrypt at rest.

This is the cleanest possible natural experiment for canal's constraint #4 and its frontend goal: two
ecosystems, same wire model, one with a config schema and one without. **The one without it never got a
usable UI and required a per-connector registry maintained outside the connector — i.e. exactly the
"core knowledge of the connector" that canal forbids.** A self-describing config surface is not a
nice-to-have; it is the load-bearing requirement for "implement the interface, register it, done".

### 7.2 Airbyte: JSON Schema in, everything else falls out

Reconstruction **[HC on `connectionSpecification`, `documentationUrl`,
`supported_destination_sync_modes`, `advanced_auth`; MC on the deprecated fields]**:

```yaml
ConnectorSpecification:
  required: [connectionSpecification]
  properties:
    documentationUrl: { type: string, format: uri }
    changelogUrl:     { type: string, format: uri }
    connectionSpecification:
      type: object
      description: "ConnectorDefinition specific blob. Must be a valid JSON schema (draft-07)."
    supportsIncremental:   { type: boolean }   # deprecated — moved to per-stream sync modes
    supportsNormalization: { type: boolean, default: false }   # deprecated with basic normalization
    supportsDBT:           { type: boolean, default: false }
    supported_destination_sync_modes:
      type: array
      items: { "$ref": "#/definitions/DestinationSyncMode" }
    authSpecification: { "$ref": "#/definitions/AuthSpecification" }   # deprecated
    advanced_auth:     { "$ref": "#/definitions/AdvancedAuth" }
    protocol_version:  { type: string }
```

`connectionSpecification` is **a JSON Schema document, supplied by the connector, describing the shape
of its own config.** That one field is the entire config model, and it is the reason Airbyte's UI
renders forms for hundreds of connectors with zero per-connector frontend code. **[HC]**

The UI needs presentation metadata that JSON Schema does not define, so Airbyte added **extension
keywords**, and this list is the most directly stealable thing in this section **[HC on
`airbyte_secret`, `order`, `title`, `description`, `examples`, `default`, `enum`, `oneOf`, `multiline`;
MC on `airbyte_hidden`, `always_show`, `group`, `display_type`, `pattern_descriptor`]**:

| Keyword | Purpose |
|---|---|
| `title` | human label for the field |
| `description` | help text |
| `examples` | placeholder / example values |
| `order` | field ordering within the form |
| `default` | prefilled value |
| `enum` | render a dropdown |
| `pattern` + `pattern_descriptor` | client-side regex validation + a human explanation of it |
| `airbyte_secret: true` | **password input, redacted in logs, encrypted at rest, never returned to the UI** |
| `airbyte_hidden: true` | present in the schema, not shown in the form |
| `multiline: true` | textarea instead of an input |
| `always_show: true` | show even when nested in a collapsed `oneOf` branch |
| `group` | form section grouping |
| `display_type: radio \| dropdown` | how to render a `oneOf` |
| `oneOf` + a `const` discriminator field | **tagged-union config** — e.g. auth method, destination format |

Compare Kafka Connect's `ConfigKey` (14 fields: `group`, `orderInGroup`, `width`, `displayName`,
`importance`, `dependents`, …). **Airbyte's set is nearly the same information expressed as JSON Schema
annotations instead of a bespoke struct.** Two differences matter:

1. **Airbyte can express nested and repeated structure; Connect cannot.** `oneOf` with a `const`
   discriminator gives real tagged unions (this is how "OAuth vs API key vs service account" is a single
   config field with three shapes), and objects/arrays nest arbitrarily. Connect's dotted-prefix
   convention (`transforms=a,b` + `transforms.a.type=…`) is explicitly **not** describable in
   `ConfigDef`. **This is a decisive advantage for JSON Schema and it is the single strongest argument
   for canal using JSON Schema for config.** **[HC]**
2. **Connect can express dynamic validation and dynamic option lists; Airbyte cannot.** There is no
   `Recommender` equivalent — no "populate this dropdown by querying the live database", no
   `dependents` re-validation graph, no server-side "is this field visible given the rest of the
   config" hook. Airbyte's validation is: JSON Schema structural validation, then `check` (a boolean
   plus a message). **[HC]** The result is that "list my tables so I can pick one" is not a config-form
   capability in Airbyte — it is what `discover` is for, which is why Airbyte's setup flow is
   necessarily two-phase (configure → discover → select streams) while Connect's is one form.

**That two-phase flow is itself a finding.** Airbyte's UX sequence is:

```
spec      → render config form
check     → validate credentials, show one pass/fail + message
discover  → render the stream/field selector with per-stream mode pickers
(persist ConfiguredAirbyteCatalog)
read/write
```

**[HC]** Each step is a separate connector invocation whose output is a document the UI renders
generically. Canal's frontend goal is served by exactly this staging, and it works because **each stage
returns data, not a rendered anything.**

### 7.3 Defaults, secrets, and config mutation

- **Defaults** come from JSON Schema `default`; `required` comes from JSON Schema `required`. No
  separate mechanism. **[HC]**
- **Secrets** are declared by `airbyte_secret: true`, and the platform then stores them in a secrets
  manager, substitutes them at invocation, and masks them in logs. **[HC]** The important property:
  **secretness is declared in the connector's own schema**, so the platform needs no per-connector
  knowledge of which fields are sensitive. Canal must have this from day one — a `secret: true`
  annotation on the config schema, honoured by the core's logging, API serialisation, and store.
- **`AirbyteControlMessage` — connector-initiated config mutation.** Reconstruction **[HC]**:

```yaml
AirbyteControlMessage:
  required: [type, emitted_at]
  properties:
    type:       { enum: [CONNECTOR_CONFIG] }
    emitted_at: { type: number }
    connectorConfig: { "$ref": "#/definitions/AirbyteControlConnectorConfigMessage" }

AirbyteControlConnectorConfigMessage:
  required: [config]
  properties:
    config: { type: object, description: "the new configuration blob" }
```

This exists for **OAuth refresh-token rotation**: a source refreshes an access token mid-sync, and must
persist the new token so the *next* sync can use it. There is no callback, no context, no injected
config store — so the connector **emits a message on the data channel telling the platform to rewrite
its own persisted config**, and the platform does. **[HC]**

Read as interface design, this is remarkable and worth thinking about carefully:

- It is the **one** case where the connector writes to the control plane, and it was implemented as
  another member of the same message union rather than as a side channel or a callback. **Because the
  union existed, adding connector→platform control cost one enum value and one struct.** That is the
  payoff of a tagged-union channel (§1.2).
- It is also a **security-relevant capability delivered with no scoping**: a connector can rewrite its
  entire config blob, and the platform must trust it. Canal should keep the capability (rotating
  credentials are unavoidable) but scope it — a `PersistConfigPatch(ctx, patch)` on the context that
  merges specific keys, rather than "here is a whole new config object".
- **Canal's in-process equivalent is a method on the context, not a message** — which is strictly
  better, because it can return an error (Airbyte's connector cannot learn whether its config write
  succeeded). But the *interface* must still be `(ctx, serialisable-request) → (response, error)` so an
  out-of-process shim can carry it as a message, exactly as Airbyte does.

---

## 8. Backpressure

### 8.1 The mechanism is the OS pipe buffer, and that is the entire story

`tap | target`. `docker run src | docker run dst`. When the target reads slower than the tap writes,
the pipe fills (64 KiB on Linux by default), the tap's `write(2)` blocks, and the tap stops producing.
**[HC]**

Assessed honestly, this is **better than Kafka Connect's source-side backpressure**, which is *nothing*
(see the kafka-connect dossier: `poll()` is called again the instant the previous batch reaches the
producer; KIP-731 has been stuck since 2021). A blocking pipe is real, correct, end-to-end flow control
that requires no code, no config, and no cooperation.

It is also profoundly limited:

- **It is binary and invisible.** There is no queue depth, no credit, no "the sink is at 80%". Nothing
  can be metered, and nothing above the pipe can react (e.g. by scaling the destination, or by
  switching to a bulk-load path).
- **The buffer is tiny and not tunable from the protocol.** 64 KiB of JSON is a few hundred records, so
  the coupling is extremely tight: any destination stall stalls the source immediately. Airbyte's
  platform inserts its own buffering/batching between the two containers to decouple them, which means
  **the real backpressure behaviour is a property of the platform, not of the protocol** — and is
  therefore not something a connector author can reason about. **[MC on the current platform
  implementation.]**
- **There is no per-stream backpressure.** One slow destination table throttles every stream, because
  there is one pipe.
- **Batching is invisible to the source.** A destination that wants 10,000-row batches simply reads
  10,000 records and blocks; the source has no idea and cannot align its checkpoint emission to the
  batch boundary. This is the §3.3 granularity mismatch, seen from the other end.

### 8.2 Concurrency limits

Nothing in the protocol. **[HC]** Rate limiting is per-connector, hand-rolled, and re-derived every run
(§6.1: nothing persists between syncs, so a token bucket cannot be carried across sync boundaries —
a connector that must respect "1000 requests/hour" across hourly syncs cannot do it correctly with only
`config` and `state`… unless it stores the budget *in state*, which some do). **[MC on real-world
practice, HC on the structural constraint.]**

The CDK later added in-process concurrency (a thread-pool source framework with partition/cursor
abstractions) with a `num_workers`-style knob. **[MC]** Note the shape: concurrency became a *connector
config field*, not a platform-assigned parallelism — because the platform has no way to assign work
(§12).

### 8.3 What canal must take from this

1. **Have real end-to-end flow control, like the pipe, but make it observable and tunable.** A bounded
   channel with a declared capacity and a depth gauge gives the pipe's correctness plus the metrics the
   frontend needs. Design-rule R6 says exactly this ("bounded by construction; backpressure is an
   expressible outcome — block or reject"). Airbyte's pipe is the "block" half done implicitly; canal
   needs both halves done explicitly.
2. **Make the batch boundary visible to both sides.** The sink should be able to say "give me batches of
   N or B bytes or T milliseconds, whichever first" and the source should be able to align checkpoints
   to those boundaries. Neither Airbyte nor Connect can express this and both pay for it.
3. **Per-stream (per-split) flow control, not per-pipeline.** One pipe for all streams is the reason a
   single slow table throttles a whole Airbyte connection.

---

## 9. Delivery guarantees

### 9.1 At-least-once, by construction, in both

The guarantee follows mechanically from §3.3 **[HC]**:

- the destination yields STATE only after durably persisting everything before it;
- the platform persists only destination-yielded STATE;
- the next run resumes from that STATE;
- therefore **records between the last persisted STATE and the crash are re-delivered.**

No exactly-once mechanism exists in either protocol. **[HC]** There is:
- no transaction concept,
- no `prepare`/`commit` pair,
- no two-phase commit,
- no committable/recoverable-committable notion,
- no fencing/epoch/generation token to stop a zombie writer (Airbyte's `generation_id` is about refresh
  bookkeeping, not fencing). **[MC on `generation_id` never being used for fencing.]**

Compare Kafka Connect, which *does* have exactly-once for sources (KIP-618) — at the cost of an enormous
amount of machinery (transactional producers, `transactional.id` derivation, a zombie-fencing protocol,
an internal fence endpoint, a three-state worker config requiring a two-phase rolling upgrade), and only
because Kafka is the fixed middle. Airbyte, with a *generic* destination, could not have taken that route
even if it wanted to.

### 9.2 Idempotency and dedup: pushed onto the destination, via the catalog

The one place Airbyte engages with duplicates is `destination_sync_mode: append_dedup` **[HC]**:

- the destination receives `primary_key` (from the configured catalog) and a cursor;
- it upserts by primary key, keeping the latest version per key;
- deduplication is therefore **a destination-side, catalog-configured behaviour**, not a framework
  guarantee.

The modern implementation ("Typing and Deduping") is: append everything to a raw table with
`_airbyte_raw_id` (a UUID), `_airbyte_extracted_at`, `_airbyte_meta`, `_airbyte_generation_id`, and
`_airbyte_data`; then a SQL pass types and dedups into the final table using `primary_key` and the
cursor, with `row_number()`-style latest-wins. **[MC on the exact column set, HC on the raw→final
architecture and on dedup being a SQL pass keyed by the catalog's primary key.]**

Three observations that matter for canal:

1. **Dedup is expressible generically because identity is declared in the catalog.** The core needs no
   knowledge of any source or sink to say "this stream dedups on `[["id"]]` using cursor
   `["updated_at"]`". This is a real vindication of catalog-level identity (§2.2) — and it is also why
   canal wants the key *on the envelope* as well, so a *non-SQL* sink can dedup without reimplementing
   path extraction from a JSON object.
2. **At-least-once + declared idempotency key = effectively-once, for the large class of sinks that can
   upsert.** That is a much cheaper deal than exactly-once machinery and it covers most real
   destinations. Canal should make the *identity* a first-class declared thing and let sinks that can
   upsert advertise it — rather than building 2PC first.
3. **But the framework verifies nothing.** A destination claiming `append_dedup` support and getting it
   wrong produces silent duplicates. Canal's design-rule R5 (dedupe keys are scoped, durable, and
   committed after the write) is the mechanism Airbyte lacks: it puts dedup state in the *core*, scoped
   and durable, rather than trusting each sink's SQL.

### 9.3 The transactional-sink gap, stated precisely

Neither protocol can express "the destination has *prepared* a batch and will commit it if told to".
The destination's only expressible states are "I have consumed up to here" (implicit) and "I have
durably committed up to here" (yield STATE). **[HC]**

This is fine for idempotent/upsert destinations and inadequate for destinations where a partial write is
visible and harmful. canal's plan (per the kafka-connect dossier) for a genuine
`Prepare() → committable; Commit([]committable)` with committables persisted in the same checkpoint as
source positions is strictly more expressive than anything in either protocol here, and nothing in
Airbyte's experience argues against it — Airbyte simply never had a place to put a committable, because
its checkpoint is a single opaque blob owned by the *source*.

**That is the design connection to make:** if the checkpoint is `{Shared, Streams}` (§3.2) plus a
`SinkCommittables` slot, then 2PC sinks and Airbyte-style yield-state sinks are the same mechanism at two
levels of ambition, and a sink declares which one it implements.

---

## 10. Plugin boundary

### 10.1 Out-of-process, by definition — and what it actually bought

Singer: a plugin is **an executable on `PATH`** (or a Python entry point). Discovery is "the user typed
the name". Versioning is pip/PyPI. **[HC]**

Airbyte: a plugin is **a Docker image**. **[HC]** The registry is a JSON/YAML catalog in the
`airbytehq/airbyte` repo (source/destination definition files carrying `dockerRepository`, `dockerImageTag`,
`definitionId` (a UUID), `documentationUrl`, `releaseStage` / `supportLevel`, and a
`spec` cache). **[MC on exact field names, HC on the shape: a registry document mapping a connector
identity to an image tag.]**

What the out-of-process boundary genuinely bought them, and this list is the honest case for it:

1. **Polyglot connectors.** Python, Java, and low-code YAML connectors coexist with no shared runtime.
   The majority of Airbyte's ~300+ connectors are Python; the high-throughput ones are Java. **[HC]**
   In-process Go cannot have this, and it is the single largest thing canal gives up. It is a defensible
   trade (canal's design-rule R11 explicitly buys one runtime deliberately), but it should be named as a
   trade, not a win.
2. **Dependency isolation, absolutely.** No classloader tricks, no version conflicts, ever. Contrast
   Connect's child-first `PluginClassLoader` and its still-unfinished `plugin.discovery` migration.
3. **Resource isolation and limits at the process boundary** — exactly what Kafka's KIP-987 wants and
   cannot have.
4. **Independent release cadence.** A connector ships as an image tag; the platform does not rebuild.
   Version pinning per connection is trivial.
5. **Crash containment.** A segfaulting connector cannot take the platform down.

### 10.2 Versioning and compatibility

- **Protocol version** is a field on `ConnectorSpecification` (`protocol_version`), and Airbyte
  maintained a `v0`/`v1` migration path with platform-side message *upgrade/downgrade* shims (the
  `AirbyteMessage` version migration machinery). **[MC on the class names, HC on the existence of
  platform-side version negotiation and message migration.]**
- **Compatibility is structural, not ABI.** Adding an optional field to a message is non-breaking;
  a receiver ignoring unknown fields is the rule. **This is the property canal must preserve if it wants
  the "same interface, later out-of-process" outcome:** every request/response type must tolerate unknown
  fields and absent optional fields. In Go that means struct-plus-`json`/proto-tag types with no required
  positional semantics — and, critically, **no interface-typed or func-typed fields anywhere in a
  request/response**, because those cannot be migrated or serialised.
- **The state blob is the version hazard.** Because `stream_state` is opaque and connector-authored, a
  connector upgrade that changes its state format must handle its own old format. There is no state
  schema, no version tag on state, and no migration hook. **[HC]** Airbyte's practical answer is
  "changing state format requires a reset". Canal's design rules already flag this as an open decision
  ("checkpoint state format compatibility across binary upgrades") — the concrete recommendation from
  Airbyte's experience is: **put a connector-authored version tag inside the checkpoint payload from the
  first commit**, and make the core carry (but never interpret) it. It costs nothing now and is
  impossible to add later without a reset.

### 10.3 Registration and discovery: the anti-lesson

Neither system has connector self-registration into a running process. Both require an **external
registry document** mapping a name to an artifact. **[HC]**

Airbyte's registry is a **curated file in the platform's own repo** — i.e. adding a connector to the
official catalog is a PR against Airbyte, which is exactly the "core edits to add a connector" that
canal's constraint #4 forbids. (Custom connectors can be added by image URL without the PR, so the hard
requirement is only for the *curated* list.) **[MC on the current custom-connector flow.]**

**Canal's init-time Go registry is strictly better on this axis**, and the thing to steal is not the
mechanism but the *document*: Airbyte's registry entry is a small, machine-readable connector identity
record (id, name, version, docs URL, support level, cached `spec`). Canal should have the equivalent as
the registry's public output — `Registry.List() []ConnectorDescriptor` where the descriptor includes the
cached config schema — because **a UI that can list connectors and render their config forms without
instantiating anything is what makes the frontend goal cheap.** Airbyte caches the `spec` in the registry
precisely so the UI need not run a container to draw a form.

### 10.4 The shape that keeps the door open (the constraint-#3 checklist)

Airbyte's protocol is, by construction, a proof that this connector model is wire-shippable. Extracting
the properties that make it so, as a checklist for canal's Go interfaces:

| Property | Airbyte | Required for canal |
|---|---|---|
| Every operation is `(serialisable request) → (stream of serialisable responses)` | yes | **yes** |
| No handles, no callbacks, no closures, no `Class`/type objects in signatures | yes | **yes** (Connect fails this) |
| Config, catalog, state are all **documents** | yes | **yes** |
| Data path is a **stream**, not a `[]Record` return | yes | **yes** — `Read(ctx) → iterator/channel`, not a slice |
| Control, logs, errors, progress travel **in-band on the same ordered stream** | yes | **yes** — one tagged union |
| Cancellation | signal only | **must be `ctx`** (Airbyte's gap) |
| Backpressure | pipe only | **must be explicit** (Airbyte's gap) |
| Optional capabilities discovered by declaration, not by method presence | catalog/spec flags | **yes** — separate Go interfaces, type-asserted |

The last row is the Go-specific translation: Airbyte declares capabilities as **fields**
(`supported_sync_modes`, `is_resumable`, `source_defined_cursor`, `supported_destination_sync_modes`,
`supportsFileTransfer`). Go's idiom is **small optional interfaces discovered by type assertion**. These
are the same design; the field form is the one that survives a wire boundary, so canal should have
**both**: the optional Go interface for the implementation, and a declared capability set in the
descriptor/catalog for the core and the UI to read. Deriving the second from the first at registration
time keeps them from drifting.

---

## 11. Observability

### 11.1 What the connector can say, and what only the platform knows

Singer's observability surface is **stderr**. **[HC]** There are no metrics, no progress, no status. Some
taps emit structured log lines to stderr by convention; nothing consumes them generically.

Airbyte's is entirely message-based, which is the interesting part: **a connector's only observability
channel is the same ordered message stream that carries data.** **[HC]** From the protocol:

| Message | Carries |
|---|---|
| `AirbyteLogMessage` | `{level: FATAL\|ERROR\|WARN\|INFO\|DEBUG\|TRACE, message, stack_trace}` |
| `TRACE/ESTIMATE` | `row_estimate`, `byte_estimate`, scoped `STREAM` or `SYNC` |
| `TRACE/STREAM_STATUS` | `STARTED → RUNNING → COMPLETE \| INCOMPLETE` per stream |
| `TRACE/ERROR` | typed failure (§6.3) |
| `TRACE/ANALYTICS` | connector-defined `{type, value}` telemetry |
| `STATE` w/ `sourceStats.recordCount` | records emitted before this checkpoint |
| `STATE` w/ `destinationStats.recordCount` | records the destination counted before committing it |

**[HC on log/estimate/stream_status/error; MC on `ANALYTICS` and the stats field names.]**

Everything else — throughput, bytes, duration, attempt count, container CPU/memory — is measured **by the
platform**, from outside the connector: it counts the records and bytes crossing the pipe, times the
process, and records the exit status. **[HC]**

**This division is a genuinely good design and canal should copy it deliberately:**

- **The connector reports only what only it can know**: an estimate of total work, per-stream lifecycle,
  typed errors, and semantic logs. All four are things the core cannot derive.
- **The core measures everything it can observe itself**: counts, bytes, rates, latencies, durations,
  attempts, queue depth. No connector cooperation required, so **no connector can fail to be
  instrumented** — the metric coverage is uniform across every connector by construction.

Contrast Kafka Connect's `PluginMetrics` (Kafka 4.1), where the plugin registers metrics and the core owns
naming/tagging/export. That is a fine mechanism, but Airbyte's split is the more important insight: **the
question is not "who names the metric", it is "which facts can only the plugin supply".** For canal, the
answer is a short list — work estimate, per-stream phase/status, typed error, connector-semantic
counters — and everything else should be core-measured so it exists for free.

### 11.2 The record-count reconciliation, which is the best small idea here

`sourceStats.recordCount` on a STATE message says "N records preceded this checkpoint". The destination,
when it yields that STATE back, attaches `destinationStats.recordCount`. **[MC on names, HC on the
purpose.]** The platform can therefore assert, per checkpoint, **emitted == persisted**, and flag a
mismatch.

This is exactly the mechanism canal's design rules demand ("a metrics UI that cannot distinguish *the
endpoint answered* from *your data arrived* is actively misleading"). It is one integer on each side of
the checkpoint, and it converts the checkpoint from a position into an **auditable reconciliation
point**. **Steal this verbatim.**

### 11.3 Health/status model

There is no connector health model — a connector is a process, and its health is its exit code. **[HC]**
Status lives entirely in the platform:

- per-connection: enabled/disabled/paused (scheduling states), last sync status, schema-change status;
- per-sync ("job"/"attempt"): pending / running / succeeded / failed / cancelled / incomplete, with
  attempt-level retries; **[MC on the exact enum]**
- per-stream within a sync: derived from `STREAM_STATUS` traces.

**The per-stream status is the thing to note**, because it means "partial success" is representable: 6 of
7 streams `COMPLETE`, one `INCOMPLETE`, the 6 states committed. Canal's design rules require a connector
state machine (`healthy → degraded → paused → terminal`); Airbyte shows that a **second, per-stream
machine** is also needed, and that the two are not substitutes — one is about the connector's ability to
run, the other about each unit of work's progress.

### 11.4 What a UI can read

Airbyte's UI is built on: the `spec` (a JSON Schema → a form), the `AirbyteCatalog` (→ a stream/field
tree with per-stream mode pickers), the persisted `ConfiguredAirbyteCatalog` (→ current selections), the
`check` result (→ a green/red banner with a message), job/attempt history with per-stream status and
record/byte counts, and the connector logs. **[HC]**

**Every one of those is a document the connector produced, rendered generically.** That is the whole
answer to canal's frontend goal, and the *reason* it works is that **there is not one connector-specific
line of frontend code anywhere** — which is only possible because config, streams, and modes are all
data. Singer, with the same wire model but no config schema and a weaker catalog, never achieved it.

Gaps to avoid: **no metrics endpoint on the connector** (nothing to scrape; all metrics are
platform-side and only visible in the platform's own UI/API), **no live progress within a stream** beyond
record counts against an optional estimate, and **no lag** — a source has no way to say "I am 4 minutes
behind the log head" because the protocol has no notion of a head position. Canal should add exactly
that: let a source optionally report a *head* alongside its position (`Lag` as a declared capability),
since only the source can know it.

---

## 12. Deployment

### 12.1 Two very different answers

**Singer has no deployment model.** It is a shell pipeline. Orchestration is cron, Airflow, Meltano, or
Stitch's hosted platform. **[HC]** This is worth stating because it is the *entire* reason Singer lost
to Airbyte despite a cleaner, smaller spec: **a connector standard with no runtime is a library, not a
product.** Canal's standalone-single-binary goal is the right hedge against this — but note the corollary:
the single binary *is* the reference runtime, and its semantics become the de-facto spec.

**Airbyte's deployment model** is a platform of long-running services that launch short-lived connector
containers per sync. Roughly **[MC on the current service decomposition, which has churned a lot —
`airbyte-server`, `airbyte-worker`, a `workload-launcher`/`workload-api` pair, a temporal cluster, and a
Postgres for config+jobs]**:

- **Postgres** is the config and job store — a real transactional database, not a compacted log. Worth
  noting against Kafka Connect's `KafkaConfigBackingStore` and its documented unrecoverable
  compaction-plus-partial-write state: Airbyte simply does not have that failure mode. Canal's design
  rules point the same way (R13, and the kafka-connect dossier's "make the control-plane store
  transactional and versioned").
- **Temporal** (a durable workflow engine) owns sync orchestration, retries, and timeouts. **[HC that
  Temporal is used; MC on the current scope.]** This is a strong prior-art signal: Airbyte concluded that
  *sync orchestration is a durable-workflow problem*, not a rebalancing problem. Retries, attempt limits,
  timeouts, and cancellation are workflow concerns, and the connector stays a dumb subprocess.
- **Work assignment** is container scheduling: a sync becomes one (or a few) pods, placed by Kubernetes.
  **There is no rebalancing, no consumer group, no assignor, no leader election over connectors.**
  **[HC]** Compare the entire KIP-415 incremental-cooperative-assignor apparatus in Connect. Airbyte gets
  to skip it because a sync is a *job*, not a long-lived assignment.
- **Scaling unit = the sync.** Horizontal scale means more concurrent syncs, not more parallelism within
  a sync (§4.4). **[HC]**

### 12.2 The trade, stated for canal

| | Airbyte (job model) | Kafka Connect (assignment model) |
|---|---|---|
| Unit of scheduling | a sync (finite job) | a task (indefinite assignment) |
| Coordination needed | a job queue + a scheduler | membership, leader, assignor, rebalance protocol |
| Parallelism within a unit | none (1 process) | N tasks, connector-split |
| Failure handling | rerun the job from last checkpoint | restart the task in place |
| Streaming CDC fit | poor — a "sync" that never ends is a job that never completes | native |
| Batch/snapshot fit | native | poor — no `COMPLETED` state |

**Canal needs both, and this table is why.** A batch/snapshot pipeline is a *job*; a streaming CDC
pipeline is an *assignment*. Airbyte's own CDC support is the awkward case in its model — an
`incremental` sync of a CDC source is a job that reads the log for a while and then *stops*, so CDC in
Airbyte is really "micro-batch tailing on a schedule", with latency bounded below by the sync interval
plus container startup. **[MC on there being no continuous mode; I believe syncs remain scheduled and
finite.]**

The synthesis for canal: **model the unit of work as a split with a declared boundedness** (bounded →
job semantics, completion, a terminal `COMPLETED` state; unbounded → assignment semantics, indefinite,
rebalanceable), and let a pipeline contain both — which is exactly what "hybrid snapshot-then-stream"
means. The scheduler then treats a bounded split as a job and an unbounded split as a lease.

### 12.3 Standalone vs distributed

Airbyte's local story is `abctl`/docker-compose running the same services in one node; the connector
contract is byte-identical. **[MC on `abctl`, HC on the local deployment existing and using the same
connector contract.]** Singer's local story is the pipe itself, which is unbeatable for dev ergonomics —
`tap-foo --config c.json | target-jsonl` is a complete, debuggable pipeline with no infrastructure, and
that is genuinely why Singer connectors were pleasant to write.

**Both halves are worth having and canal can have them cheaply:** the single binary should be able to run
`canal run --source X --sink Y --config c.json` as one process with a file-backed checkpoint store, using
**the same registry, the same interfaces, and the same checkpoint format** as the distributed mode — with
only the checkpoint store, the control-plane store, and the scheduler swapped (Kafka Connect's
standalone/distributed seam, which the kafka-connect dossier already identifies as correct). Adding
Singer's other gift — **a trivially inspectable sink** (`--sink jsonl` writing the canonical envelope to
stdout) — makes the data path debuggable from day one and directly serves design-rule R3 (one end-to-end
path before any breadth).

---

## 13. What they got right / what they got wrong

### 13.1 Got right

- **Config as a JSON Schema supplied by the connector (`connectionSpecification`).** One field, and it
  yields a generic form renderer, structural validation, defaults, `required`, nested and tagged-union
  config, and declared secrets. It is the difference between Airbyte's UI and Singer's absence of one.
- **JSON Schema extension keywords for presentation** (`title`, `description`, `examples`, `order`,
  `enum`, `pattern` + `pattern_descriptor`, `multiline`, `airbyte_secret`, `airbyte_hidden`,
  `always_show`, `group`, `display_type`) — presentation metadata living *with* the schema, so the form
  is fully determined by connector-supplied data.
- **`airbyte_secret: true`.** Secretness declared in the connector's own schema, so redaction,
  encryption, and masking need zero per-connector knowledge.
- **`discover` as a first-class command returning a persisted catalog.** This is the capability Kafka
  Connect lacks entirely, and it is what makes stream pickers, drift detection, and
  "you selected a column that no longer exists" possible.
- **`AirbyteCatalog` vs `ConfiguredAirbyteCatalog`** — two types, the configured one *embedding* the
  discovered one. Discovered-vs-chosen made structural rather than conventional.
- **Sync mode as a per-stream catalog value, and the orthogonal `SyncMode` × `DestinationSyncMode`
  product.** Pipeline "type" becomes configuration of a uniform engine rather than N pipeline classes,
  and source-side/destination-side behaviour are independent, so M×N combinations are free.
- **Connector-asserted non-negotiables** — `source_defined_cursor`, `source_defined_primary_key`,
  Singer's `inclusion: automatic` and `forced-replication-method`. The connector constrains the
  operator's choices *in data*.
- **Composite/nested keys as `[][]string`** — `primary_key` and `cursor_field` as lists of paths, so
  composite and nested keys needed no later breaking change.
- **Per-stream state (`AirbyteStateType.STREAM`) plus a shared-position form (`GLOBAL`) with
  `shared_state` + `stream_states`.** The only checkpoint shape in this study that can express both
  independent per-stream progress and a single shared log position.
- **Opaque, uninterpreted checkpoint payloads.** Resumable full refresh — a whole new phase model — was
  added with no new message type, because the state channel never constrained its contents.
- **Position-carrying acknowledgement.** The destination *yields the STATE message* it has committed
  past, so the ack is the position. No low-water-mark data structure, no out-of-order ack problem, and
  the core needs no offset bookkeeping at all.
- **`sourceStats.recordCount` / `destinationStats.recordCount` on the checkpoint.** Turns each
  checkpoint into an auditable emitted-vs-persisted reconciliation point for one integer of cost.
- **A single ordered tagged-union channel for data, logs, progress, typed errors, and control.** Adding
  connector→platform control (OAuth token rotation) cost one enum value and one struct.
- **`AirbyteControlMessage` at all** — acknowledging that connectors must sometimes rewrite their own
  persisted config (credential rotation), and giving it a defined path instead of leaving every connector
  to fail at it.
- **`AirbyteErrorTraceMessage`'s two-audience split**: `message` (user-facing, safe to display) vs
  `internal_message` + `stack_trace` (developer-facing).
- **`failure_type` as an ownership classification** (`config_error` / `system_error` /
  `transient_error`) — answering "whose problem is this?" rather than only "should we retry?".
- **`stream_descriptor` on errors**, so one failing stream does not present as a failed connection, and
  partial success is representable.
- **`TRACE/STREAM_STATUS` (`STARTED → RUNNING → COMPLETE | INCOMPLETE`)** — the per-stream terminal
  state Kafka Connect never got.
- **`TRACE/ESTIMATE` (`row_estimate` / `byte_estimate`)** — let the connector declare total work, since
  only it can compute it cheaply. This is a better answer to progress/ETA than making positions
  comparable.
- **`AirbyteRecordMessageMeta.changes`** — per-record, per-field lossiness log (`NULLED`/`TRUNCATED`
  with a reason). Machine-readable partial-degradation instead of silent data loss.
- **The two-methods-and-done CDK surface** (`check_connection`, `streams`) with `Stream` as the unit of
  extension, so the framework owns catalog construction, state routing, checkpoint emission, and status.
- **`IncrementalMixin`'s `state` getter/setter property**, replacing the per-record
  `get_updated_state(current, latest)` fold. The framework restores through the setter and checkpoints
  through the getter; the connector never plumbs state through its own call graph.
- **`stream_slices()`** — an in-connector chunk enumerator whose slices are the natural checkpoint
  granularity.
- **`is_resumable`** — resumability as a declared capability rather than an assumption.
- **The process boundary as the lifecycle.** No `stop()`-called-twice bug class is possible; teardown is
  the OS's job. Kafka Connect's KIP-419 has been open for seven years for want of this property.
- **A real transactional store (Postgres) for config and jobs**, and a durable workflow engine (Temporal)
  for sync orchestration — retries, timeouts and cancellation as workflow concerns, not connector
  concerns.
- **Jobs, not assignments, as the scheduling unit** — which deletes the entire rebalance-protocol problem
  space for batch and snapshot workloads.
- **Singer's `breadcrumb` metadata** — a flat list of `(path, key→value)` pairs annotating a JSON Schema
  without mutating it, so the schema stays a valid schema and annotations stay diffable and mergeable.
- **Singer's `bookmarks` + nested `offset` + `currently_syncing`** — cursor, intra-chunk resume point,
  and in-flight marker as three distinct things.
- **Singer's re-emittable in-band `SCHEMA` message** — mid-sync schema evolution expressible without
  per-record schema overhead. Airbyte, with an out-of-band catalog, cannot express this at all.
- **Singer's pipe** as a dev experience: `tap | target` is a complete, debuggable pipeline with zero
  infrastructure.

### 13.2 Got wrong — documented pain

- **Reset-on-config-change is the single most complained-about Airbyte behaviour.** Changing a stream's
  sync mode, cursor field, or primary key — and, historically, many schema changes — requires a **full
  reset**: wipe the destination data for that stream and re-sync from zero. For a 500M-row table this is
  days. The root cause is structural: **state is opaque, so the platform cannot know whether an existing
  checkpoint is still meaningful under a new cursor, and must assume it is not.** **[HC that reset is
  required and widely complained about; MC on the current exact trigger list.]** The lesson for canal is
  precise: **checkpoint payloads must carry enough connector-authored metadata (cursor identity, format
  version) for the core to decide compatibility** — not to interpret the position, just to answer "is
  this still valid?".
- **Full refresh originally had no state at all**, so a scan that failed at 90% restarted at 0. Fixed by
  the "resumable full refresh" retrofit (`is_resumable`, `CheckpointMixin`) years later. **[HC]**
- **`get_updated_state(current_stream_state, latest_record)` — deprecated.** A per-record fold that
  forced state derivation into a record-shaped callback and made "state after a slice" inexpressible.
  Replaced by the `state` property. **[HC on the deprecation.]**
- **`AirbyteStateType.LEGACY` — a deprecated state type that must be supported forever**, because
  connectors and stored connection state exist in the wild in that form. A "we got the granularity wrong
  the first time" scar preserved in the protocol enum. **[HC]**
- **Basic normalization (dbt-based) was deprecated and replaced by destination-side
  "Typing and Deduping".** Airbyte's original answer to "JSON blob in, typed table out" was to generate
  and run dbt models per connection — a whole second toolchain (dbt + a Python/Java orchestrator) inside
  the sync path. It was slow, hard to debug, produced surprising table shapes, and was eventually removed
  in favour of SQL run by the destination connector itself. **[HC that basic normalization was deprecated
  in favour of Typing & Deduping; MC on dates and exact scope.]** The lesson: **a "canonical record" that
  is an untyped JSON blob defers the type problem to a second system, and that second system will be the
  worst part of the product.** This is canal's design-rule R2 with an external witness.
- **`supportsNormalization` / `supportsDBT` / `supportsIncremental` on `ConnectorSpecification` are
  deprecated fields** — capability flags that turned out to be at the wrong level (per-connector rather
  than per-stream) or to describe a feature that was removed. **[MC on which are formally deprecated.]**
- **State size.** Because `stream_state` is arbitrary JSON authored by the connector, and there is one
  STATE per checkpoint on the wire, connectors that put large structures in state (per-partition maps,
  slice lists, incremental-snapshot chunk bookkeeping) produce large, frequently-serialised state
  messages. Reported symptoms include very large state blobs in the platform database and state handling
  becoming a throughput cost. **[MC — I could not verify a specific issue number or the current
  mitigation; treat the *existence* of state-size pain as [HC] and any specific number as unverified.]**
- **Throughput is the protocol's core weakness and Airbyte has spent years on it.** Every record is: JSON
  object → serialise → pipe write → pipe read → parse → (platform buffer) → serialise → pipe write →
  parse. That is **at least two full JSON encode/decode round trips per record**, plus per-record
  newline framing, plus 33% inflation for any binary content. Mitigations Airbyte has shipped or
  attempted include platform-side batching between containers, file-based transfer for file-typed
  sources/destinations, and a move toward more efficient transport (protobuf / socket-based record
  passing) for high-volume connectors. **[MC on which of these are current and on their names —
  verify before quoting. The *diagnosis* (double JSON round-trip is the bottleneck) is [HC].]**
- **No backpressure signal beyond a blocking pipe** (§8): binary, invisible, unmeterable, per-pipeline
  rather than per-stream, and the effective behaviour is a platform implementation detail a connector
  author cannot reason about.
- **No parallelism within a sync at the protocol level** (§4.4). `stream_slices()` enumerates splits that
  the platform cannot see, so splits give resumability but never distribution. Concurrency had to be
  added *inside* the connector process, configured by a connector config field, because the platform has
  no work-assignment channel.
- **No streaming/continuous mode.** A sync is a finite job, so CDC latency is bounded below by the
  schedule interval plus container startup. **[MC]**
- **Container-per-sync overhead** dominates short syncs: image pull/create/start, interpreter boot,
  connection setup, and nothing cached between runs — no connection pool, no compiled query, no
  rate-limiter budget (§6.1, §8.2).
- **No poison-record path.** No DLQ, no per-record error routing, no `errors.tolerance` equivalent. One
  bad record fails the sync, and every retry fails identically until a human intervenes. **[HC]**
- **Retry is "rerun the whole process"**, so there is no partial-batch retry and no in-sync recovery.
- **No graceful-stop signal.** `read` cannot be asked to "finish at the next checkpoint"; cancellation is
  SIGTERM/SIGKILL, losing everything since the last destination-yielded STATE.
- **No context, no deadline, no cancellation token** anywhere in the contract.
- **Durability is a convention, not a mechanism.** "Do not yield STATE until it is durable" is prose; a
  destination that buffers in memory and yields eagerly silently produces exactly canal's design-rule R4
  catastrophe, and the protocol cannot detect it.
- **JSON Schema as the *data* type system leaks** (§5.4): no decimal/precision, no native temporal types,
  no binary, and `oneOf` unions that map to nothing relational — forcing the `airbyte_type` extension
  keyword to disambiguate what JSON Schema could not express. **[MC on the keyword's member list.]**
- **Metadata pollutes the payload.** `_ab_cdc_deleted_at` / `_ab_cdc_lsn` / `_ab_cdc_updated_at` in
  Airbyte and `_sdc_*` in Singer — both ecosystems independently reinvented magic prefixed columns
  because the envelope has no operation type, no before-image, and no metadata map. `append_dedup` then
  has to special-case CDC deletion in destination code instead of reading a protocol-level op. **[MC on
  exact field names, HC on the pattern.]**
- **No config-form dynamism.** No `Recommender` equivalent, no `dependents` graph, no server-side
  visibility hook — so "pick a table from a dropdown populated by your live database" is not a
  config-form capability, which is why setup is necessarily configure → discover → select.
- **`check` returns one boolean and one string.** No per-field errors, so a bad config cannot be
  attributed to the field that caused it. Kafka Connect's `List<ConfigValue>` with per-field
  `errorMessages` is materially better for a form.
- **The curated connector registry is a file in the platform's repo**, so appearing in the official
  catalog is a PR against Airbyte — the "core edits to add a connector" shape canal forbids. **[MC on
  the current custom-connector path.]**
- **No state schema and no state version tag.** A connector upgrade that changes its state format has no
  migration hook; the practical answer is a reset (§10.2).
- **Airbyte's service decomposition has churned repeatedly** (worker → workload-launcher/workload-api,
  scheduler → Temporal, normalization in/out of the sync path). **[MC on specifics.]** Read as prior art:
  the *connector contract* stayed remarkably stable across all of it, which is the strongest available
  evidence that a narrow, document-based connector interface survives platform rewrites — canal's
  constraint #4 is validated by Airbyte's own history.
- **Singer specifically:** no `spec`/`check`/`discover`-schema, so no generic UI and a per-tap registry
  maintained outside the connector; no config schema, so no declared secrets; **STATE semantics
  explicitly undefined by the spec**, so every tap invented its own bookmark convention and the
  ecosystem's real contract lives in `singer-python` rather than the spec; errors are exit codes; no
  log/trace channel (stdout must stay pure); no runtime, so the spec never became a product. The
  ecosystem's answer was to fork the tooling into a stronger SDK (Meltano's Singer SDK) that supplies
  the missing abstractions — i.e. **the spec was too small, and the gap was filled by a library that
  became the de facto standard.** **[HC on the direction; MC on Meltano SDK specifics, which I did not
  examine.]**
- **`ACTIVATE_VERSION` — an out-of-spec message type that became load-bearing.** Full-table replication
  needs "everything before this version marker is stale, delete it", implemented via `version` on RECORD
  plus an `ACTIVATE_VERSION` message. **[MC — I am confident this mechanism exists and is used by Stitch
  taps/targets; I could NOT verify whether it appears in SPEC.md or only in Stitch documentation and
  `singer-python`.]** Either way it is the same lesson as §2: the protocol lacked a way to say "this
  batch supersedes prior state", so it was bolted on — exactly the hole Airbyte later filled with
  `generation_id`/`minimum_generation_id`.

---

## 14. Steal this

Each of these is transplantable into an in-process Go interface set without adopting the process boundary.

1. **Make `Discover(ctx, config) (Catalog, error)` a required source method and persist its output** —
   discovery as a first-class operation is what turns drift into a diff and gives the UI a stream picker
   with zero connector-specific code.
2. **Two types, not one: a discovered `Catalog` and a `ConfiguredCatalog` that embeds it verbatim** — so
   "what the source found" and "what the operator chose" can be diffed, validated against each other, and
   audited.
3. **Put sync mode on the configured stream, not on the pipeline** — a per-stream (source-mode ×
   sink-mode) pair makes batch, incremental, dedup and hybrid pipelines configurations of one uniform
   engine rather than N pipeline classes.
4. **Keep source-side and sink-side modes orthogonal enums** — the source must never learn whether the
   sink overwrites, appends or dedups, which is what makes M×N connector combinations free.
5. **Let a discovered field declare itself non-negotiable** (`source_defined_cursor`,
   `inclusion: automatic`) so a connector can constrain operator choices in data rather than in code.
6. **Express keys and cursors as `[][]string` (lists of field paths)** so composite and nested keys need
   no later breaking change.
7. **Declare config as a JSON Schema the connector supplies**, with presentation keywords alongside
   (`title`, `description`, `examples`, `order`, `enum`, `pattern` + human `pattern_descriptor`,
   `multiline`, `group`, `hidden`, `always_show`, `display_type`) — the whole frontend goal follows from
   this one decision.
8. **Use `oneOf` + a `const` discriminator for tagged-union config** (auth method, file format) — the one
   thing JSON Schema does that Kafka Connect's `ConfigDef` provably cannot.
9. **Declare secretness in the connector's own config schema** (`secret: true`) and let the core handle
   redaction, encryption and masking with zero per-connector knowledge.
10. **…but do not copy `check`'s one-boolean-one-string result** — return per-field diagnostics
    (Connect's `List<ConfigValue>`) so a form can show every failure attributed to its field.
11. **Make the checkpoint two-level: an optional shared position plus per-stream positions**
    (`AirbyteGlobalState.shared_state` + `stream_states`) — CDC needs the first, independent stream
    progress needs the second, and Airbyte had to retrofit the shared form after shipping without it.
12. **Keep the checkpoint payload opaque bytes at the core boundary** — it is what let Airbyte add a whole
    new phase model (resumable full refresh) with no wire change.
13. **But require a connector-authored version/identity tag *inside* the checkpoint payload from commit
    one** — the core never interprets the position, only asks "is this still valid under the new config?".
    Its absence is the root cause of Airbyte's reset-on-config-change pain.
14. **Make the acknowledgement carry the position**: the sink returns the checkpoint it has durably
    committed past, and the core advances only on that return value — this deletes all low-water-mark
    bookkeeping and turns design-rule R4 from a convention into a type.
15. **Let the sink request a checkpoint boundary**, so granularity is negotiated instead of dictated by
    the source, which knows nothing about the sink.
16. **Put a record count on each side of every checkpoint** (`sourceStats` / `destinationStats`) so each
    checkpoint is an auditable emitted-vs-persisted reconciliation point for one integer of cost.
17. **Make every phase checkpointable, including a full scan** — "snapshot has no state" cost Airbyte a
    protocol change, and a declared `Resumable` capability is the right shape for it.
18. **Model phases and splits in the core** — both Airbyte and Kafka Connect independently ended up
    smuggling the snapshot phase through the opaque checkpoint, which is conclusive evidence that phases
    belong in the core rather than in connector-authored state.
19. **Give every split a declared boundedness** — bounded splits get job semantics and a terminal
    `COMPLETED`, unbounded splits get lease/assignment semantics, and a hybrid pipeline is simply a
    pipeline containing both.
20. **Carry state through a getter/setter pair the core drives** (`IncrementalMixin.state`) — the core
    restores through the setter and checkpoints through the getter, so the connector never plumbs state
    through its own call graph. Airbyte deprecated the per-record fold that preceded it.
21. **Use one ordered tagged-union channel for records, logs, progress, typed errors and control** — it
    is why adding connector→platform control (credential rotation) cost Airbyte one enum value.
22. **Give the connector a scoped way to persist a config patch** (credential rotation is unavoidable) —
    as a context method returning an error, not a fire-and-forget message, and never as "here is a whole
    new config blob".
23. **Split error text by audience: a user-facing `message` and a developer-facing
    `internal_message` + stack trace** — one string cannot serve both, and the UI needs the first.
24. **Classify errors by ownership, not just retryability** (`config_error` / `system_error` /
    `transient_error`), attached by the connector at the point of raise — this is the question an operator
    UI actually needs answered, and it maps directly onto the seven-class taxonomy in `design-rules.md`.
25. **Attribute errors to a stream** (`stream_descriptor` on the error) so partial success is
    representable — and note this only works because state is per-stream; the two are one decision.
26. **Emit a per-stream status machine on the wire** (`STARTED → RUNNING → COMPLETE | INCOMPLETE`)
    *alongside* the connector health machine — they answer different questions and neither substitutes
    for the other.
27. **Let the connector declare a work estimate** (`row_estimate` / `byte_estimate`, scoped per stream or
    per pipeline) — a better answer to progress and ETA than making opaque positions comparable, because
    only the connector can compute it cheaply.
28. **Let a source optionally report the head position alongside its own** — the one observability field
    Airbyte lacks, and the reason it cannot show lag.
29. **Add a per-record, per-field lossiness log** (`meta.changes`: field, `NULLED`/`TRUNCATED`, reason) so
    partial degradation is machine-readable and assertable in tests, which is exactly the honesty
    mechanism `design-rules.md` demands of the UI.
30. **Split observability by who can know what**: the connector reports only estimates, per-stream status,
    typed errors and semantic counters; the core measures counts, bytes, rates, durations, attempts and
    queue depth itself — so no connector can fail to be instrumented.
31. **Publish a cached connector descriptor from the registry** (id, name, version, docs URL, support
    level, config schema) so the UI can list connectors and render forms without instantiating anything.
32. **Enforce the wire-shippability checklist on every core method**: `(ctx, serialisable request) →
    (stream of serialisable responses, error)`, no handles, no closures, no func- or interface-typed
    fields in any request/response, unknown fields tolerated — Airbyte's protocol is the existence proof
    that this connector model survives a process boundary, and Kafka Connect is the counter-example.
33. **Add the two things a process boundary gave Airbyte for free and an in-process design must build**:
    `ctx` on every blocking call, and an explicit *graceful* stop ("finish at the next checkpoint")
    distinct from cancellation.
34. **Make flow control explicit, bounded, per-split, and metered** — Airbyte's blocking pipe is correct
    but binary, invisible and per-pipeline; keep the correctness, add the depth gauge and the
    block-or-reject choice R6 requires.
35. **Do not make the canonical record an untyped JSON object.** Payload as bytes plus a pluggable
    pipeline-level codec, with a small closed type system (explicit precision, temporal types, binary)
    for the schema — rendered as JSON Schema for the UI if convenient. Airbyte's untyped blob deferred the
    type problem to dbt-based normalization, which became the worst part of the product and was
    eventually deleted.
36. **Pay for the envelope fields both ecosystems lacked**: an operation/kind discriminator, an optional
    before-image, a metadata map, and a key slot. Airbyte and Singer independently reinvented magic
    prefixed columns (`_ab_cdc_*`, `_sdc_*`) because they omitted them.
37. **Ship a trivially inspectable sink and a one-process run mode** (`canal run --source X --sink jsonl`)
    over the same registry, interfaces and checkpoint format as distributed mode — Singer's pipe is the
    best dev experience in this study, and R3 wants one end-to-end path first.
38. **Treat sync orchestration as a durable-workflow problem** (retries, attempt limits, timeouts,
    cancellation) with a transactional store for config and jobs — Airbyte's Temporal + Postgres choice
    sidesteps both Kafka Connect's rebalance-protocol complexity and its documented compacted-log
    inconsistency.

---

## 15. Re-verify checklist (this run had no network)

Fetch these and correct §1–§12 against them. Ordered by how much a wrong detail would cost.

**Airbyte protocol (authoritative for every message/field name):**
- `https://raw.githubusercontent.com/airbytehq/airbyte-protocol/main/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml`
- `https://docs.airbyte.com/understanding-airbyte/airbyte-protocol`
- `https://docs.airbyte.com/understanding-airbyte/airbyte-protocol-docker`
- `https://docs.airbyte.com/understanding-airbyte/connections/` (sync mode matrix)
- `https://docs.airbyte.com/platform/connector-development/connector-specification-reference` (JSON Schema
  extension keywords, `airbyte_secret`, `oneOf` discriminator)
- `https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types` (`airbyte_type`)
- `https://docs.airbyte.com/platform/using-airbyte/schema-change-management` (drift policies)
- `https://docs.airbyte.com/platform/understanding-airbyte/typing-deduping` (raw/final columns)

**Airbyte CDK (authoritative for §1.3 signatures):**
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/sources/streams/core.py`
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/sources/abstract_source.py`
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/sources/source.py`
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/connector.py`
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/destinations/destination.py`
- `https://raw.githubusercontent.com/airbytehq/airbyte-python-cdk/main/airbyte_cdk/entrypoint.py`
- `.../sources/streams/availability_strategy.py`, `.../sources/streams/checkpoint/`

**Singer:**
- `https://raw.githubusercontent.com/singer-io/getting-started/master/docs/SPEC.md`
- `.../docs/DISCOVERY_MODE.md`, `.../docs/SYNC_MODE.md`, `.../docs/CONFIG_AND_STATE.md`
- `https://raw.githubusercontent.com/singer-io/singer-python/master/singer/messages.py`
- `https://raw.githubusercontent.com/singer-io/singer-python/master/singer/bookmarks.py`
- `https://raw.githubusercontent.com/singer-io/singer-python/master/singer/metadata.py`

**Pain claims (§13.2) needing a primary citation:** the reset-on-config-change trigger list; state-size
issues; the throughput/transport work (protobuf/socket, file transfer); basic-normalization deprecation
notice; `ACTIVATE_VERSION`'s canonical home.

---

## 16. Explicitly unverified in this run

Everything in this file is unverified against primary source, because no network tool functioned. The
following are the items where **being wrong is most likely**, and none should be quoted or used as a
literal signature without checking §15:

1. **All Python CDK signatures** (`Stream.read_records`, `stream_slices`, `AbstractSource`, `Destination.write`,
   `IncrementalMixin`/`CheckpointMixin`, `AvailabilityStrategy`) — argument order, defaults, keyword-only
   markers, and current class names. The *member sets* and the getter/setter state design are high
   confidence; the exact text is not.
2. **`singer-python` signatures** in `messages.py` / `bookmarks.py`, and whether the `bookmarks`/`offset`/
   `currently_syncing` state shape appears anywhere normative rather than only in the library.
3. **Whether `ACTIVATE_VERSION` is in SPEC.md** or only in Stitch documentation / `singer-python`.
4. **Which Singer metadata keys are tap-written vs user-written** — the split in §5.1 is the right concept
   but individual keys may be on the wrong side; also `--catalog` vs legacy `--properties`.
5. **`AirbyteRecordMessageMetaChange.reason` enum members** and the exact `meta`/`changes` field names.
6. **`generation_id` / `minimum_generation_id` / `sync_id`** on `ConfiguredAirbyteStream` — existence,
   spelling, and semantics.
7. **`DestinationSyncMode` members** — whether `overwrite_dedup` exists and whether any older spelling
   (e.g. a `dedup_history` variant) is still present.
8. **`failure_type` members** — whether `transient_error` is current, and whether other values exist.
9. **`AirbyteStateStats` field name** (`recordCount`) and whether both `sourceStats` and
   `destinationStats` are on the state message.
10. **`ANALYTICS` trace type** and `AirbyteStreamStatusTraceMessage` extras (e.g. a `reasons` field).
11. **`ConnectorSpecification` deprecated fields** — which of `supportsIncremental`,
    `supportsNormalization`, `supportsDBT`, `authSpecification` are formally deprecated vs removed; and
    the `AdvancedAuth` / `oauth_config_specification` sub-shape, which I did not attempt to reconstruct
    in detail.
12. **File transfer** — the record field / capability flag names for out-of-band file payloads.
13. **The `airbyte_type` keyword's member list** and the JSON Schema draft version in force.
14. **Throughput/transport mitigations** — whether protobuf/socket-based record passing shipped, under
    what name, and for which connectors.
15. **Reset-on-config-change** — the current exact set of changes that force a reset.
16. **State-size pain** — no issue number, no measured figure; only the existence of the problem class.
17. **Schema-change policy names** (`Propagate all` / `Propagate columns only` / `Detect and pause` /
    `Ignore`).
18. **Typing-and-deduping raw column list** (`_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`,
    `_airbyte_generation_id`, `_airbyte_data`) and the CDC field names (`_ab_cdc_*`), plus Singer's
    `_sdc_*` list.
19. **Airbyte platform architecture** — current service decomposition, Temporal's current scope, `abctl`,
    and whether any continuous (non-scheduled) sync mode now exists.
20. **The connector registry document's field names** and the current custom-connector registration path.
21. **CDK concurrency framework** names (`ConcurrentSource`, `Cursor`, partition abstractions) and the
    declarative error-handler `ResponseAction` members.
22. **Protocol version negotiation** — the message-migration machinery's names and whether `v1` shipped.
23. **`AvailabilityStrategy`** — its current status (I believe `HttpAvailabilityStrategy` was removed and
    availability checking was folded into `check`), which affects the §1.3 claim about
    `Stream.check_availability`.
24. **Pipe buffer behaviour in Airbyte specifically** — how much buffering the platform inserts between
    source and destination containers today.

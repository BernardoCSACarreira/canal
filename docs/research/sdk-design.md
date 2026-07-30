# Prior art: modern connector SDKs — Fivetran, Meltano/Singer, Estuary Flow, Segment, dlt

**Status: DRAFT. NOT NORMATIVE. NOT VERIFIED AGAINST PRIMARY SOURCE.**

> Per `docs/design-rules.md` R12 ("normative or draft — pick one") this file is **draft**. Nothing in it may
> be cited as "MUST conform" and nothing in it may be copied into an interface definition until the
> verification pass in §0 has been run.

---

## 0. Provenance, and why this file is degraded

The task for this document was explicit: *"fetch real source files and real specs from GitHub/docs, do not
rely on memory"*, and *"a hallucinated interface signature is the worst possible output here."*

**During this run, every network egress path was unavailable.** `WebFetch`, `WebSearch`, `curl` via `Bash`,
and the browser MCP tools all returned the same infrastructure error for the entire session:

```
claude-sonnet-5 is temporarily unavailable, so auto mode cannot determine the safety of
<tool> right now.
```

Approximately 45 attempts were made, alternating tools, spread across the session. Two `Bash` calls
succeeded early (both plain `ls`/`find`, which are allowlisted and bypass the classifier); no call that
touched the network ever succeeded. `~/.claude/settings.json` contains no `permissions.allow` entry that
would let `curl` bypass the classifier.

**Consequence:** I have zero verified signatures. Therefore this document deliberately contains **no fenced
block presented as a quotation from source**. Section 1, as originally specified ("the literal source and
sink interface signatures, method by method, exact names and parameter/return types, quote real code"),
**could not be written** and is marked as such. Writing recalled approximations inside fenced blocks that
look like quotations is precisely the failure mode the brief warned about, and it would be especially
corrosive in this repo, whose design rules exist because the previous attempt died of documents that
asserted more than the code supported (R8, R12).

What this file **does** contain, and what it is genuinely useful for:

1. **§0.1 A fetch manifest** — the exact URL set to retrieve, already written as a runnable script, so a
   re-run is one command and costs nothing to targeted.
2. **§1–§14** — for each topic, the **design question canal must answer**, each system's answer stated at
   the level of *architectural pattern* rather than signature, an explicit **confidence label**, the
   **exact file to open to verify it**, and the **canal implication**. The canal implications are the real
   deliverable and they do not depend on exact spellings.

### Confidence labels used throughout

| Label | Meaning |
|---|---|
| `[C-HIGH]` | Structural/architectural claim. I hold it strongly. Still **unverified this run** — verify before relying on it. |
| `[C-MED]` | Recalled specifics — identifier names, enum members, message names. Plausible but **must be checked**; do not copy into code. |
| `[C-LOW]` | Genuinely uncertain. Treat as an open question, not a finding. |

Anything not labelled is a canal design inference, i.e. my own reasoning, not a claim about prior art.

### 0.1 Fetch manifest (run this first)

A runnable script exists at
`/private/tmp/claude-501/-Users-bernardocarreira-Documents-personal-canal/9fad647d-7c79-4ad7-b63f-ba664a732b9c/scratchpad/getall.sh`
(scratchpad, may be reaped — the URL set is reproduced here so it survives).

It fetches each URL with `curl -w '%{http_code}'` and only keeps HTTP 200 responses, printing the status
code for anything else — so path drift shows up as a visible `404` line rather than a silently empty file.

**Fivetran Partner SDK** (`github.com/fivetran/fivetran_sdk`, branch `main`) — the gRPC boundary:
`connector_sdk.proto`, `common.proto`, `destination_sdk.proto`, `README.md`, and the same three under a
`v2/` prefix (the repo has been re-versioned at least once — fetch both and diff). Also
`development-guide.md`.

**Fivetran Connector SDK** (`github.com/fivetran/fivetran_connector_sdk`, `main`) — the Python authoring
layer: `fivetran_connector_sdk/__init__.py` (this is where `Connector` and the operations live),
`README.md`.

**Meltano SDK** (`github.com/meltano/sdk`, `main`, all under `singer_sdk/`): `plugin_base.py`,
`tap_base.py`, `target_base.py`, `streams/core.py`, `sinks/core.py`, `sinks/batch.py`, `sinks/sql.py`,
`connector_base.py`, `mapper_base.py`, `typing.py`, `pagination.py`, `metrics.py`, `exceptions.py`,
`about.py`, `helpers/_state.py`, `helpers/_batch.py`, `helpers/capabilities.py`,
`_singerlib/messages.py`, `_singerlib/catalog.py`.
`streams/core.py` and `helpers/_state.py` are the two highest-value files in the entire manifest.

**Singer spec** (`github.com/singer-io/getting-started`, `master`, under `docs/`): `SPEC.md`,
`DISCOVERY_MODE.md`, `SYNC_MODE.md`, `CONFIG_AND_STATE.md`.

**Estuary Flow** (`github.com/estuary/flow`, `master`, under `go/protocols/`): `capture/capture.proto`,
`materialize/materialize.proto`, `flow/flow.proto`, `runtime/runtime.proto`, `ops/ops.proto`, plus
`capture/extensions.go` and `materialize/extensions.go` (validation logic — often more informative than the
proto). Reduction annotations are documented in the `docs/` tree (`docs/concepts/schemas.md` and the
reduction-strategies reference) — locate via the repo tree, the path has moved historically.

**Airbyte protocol** (for the Estuary compatibility claim):
`airbytehq/airbyte-protocol`, `protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml`.

**dlt** (`github.com/dlt-hub/dlt`, branch `devel`): `dlt/extract/incremental/__init__.py`,
`dlt/extract/incremental/typing.py`, `dlt/extract/resource.py`, `dlt/extract/state.py`,
`dlt/common/pipeline.py`, `dlt/common/configuration/specs/base_configuration.py`,
`dlt/common/destination/reference.py`, `dlt/common/schema/schema.py`.

**Segment action-destinations** (`github.com/segmentio/action-destinations`, `main`, under
`packages/core/src/destination-kit/`): `types.ts` (this is the one that matters — `InputField` lives here),
`index.ts`, `action.ts`, `fields.ts`.

**RudderStack**: `rudderlabs/rudder-transformer` — I assessed this as low value for canal before the outage
and did not build out a target list; see §13.

---

## 1. Core interfaces

**NOT WRITTEN — could not be verified.** This section required verbatim signatures and I have none. Do not
substitute recall. The files to open are listed in §0.1; the highest-yield four are:

- `meltano/sdk` → `singer_sdk/streams/core.py` (the `Stream` base class — the single best-documented
  source abstraction in this whole set, because it is a *declarative* class rather than a method-per-hook
  interface)
- `meltano/sdk` → `singer_sdk/sinks/core.py` (`Sink` — batch lifecycle)
- `fivetran/fivetran_sdk` → `connector_sdk.proto` + `destination_sdk.proto` (the only *wire* interface in
  the set, therefore the only one that has already survived the out-of-process constraint canal's
  constraint #3 demands)
- `estuary/flow` → `go/protocols/materialize/materialize.proto` (the only genuine 2PC sink protocol in the
  set)

What I can say without quoting, and which is the load-bearing structural observation:

**Two families of source abstraction appear across these five systems** `[C-HIGH]`:

- **The pull-loop family** (Kafka Connect, Fivetran Partner SDK, Estuary capture) — the runtime drives; the
  connector is called and returns/streams records; progress markers are interleaved with data.
- **The declarative-stream family** (Meltano SDK, dlt) — the connector *declares* a stream as data (name,
  key, replication key, schema, config schema) and supplies one generator function that yields records.
  Everything else — state arithmetic, message emission, schema messages, metrics — is done by the base
  class.

The declarative family is markedly better for canal's requirement #4 ("implement the interface, register
it, done") **and** for the UI requirement, because a declared stream is *inspectable before it runs*. A
`poll()`-shaped interface tells a UI nothing until data flows. Meltano's `Stream` and Fivetran's
config-form RPC both make the connector describe itself as data first, execute second. That ordering is the
single most important thing to copy.

**Canal implication.** Split the source interface in two: a **descriptor half** that is a pure function of
config and returns plain data (streams, their keys, their schemas, their capabilities), and an **execution
half** that moves records. The UI only ever talks to the descriptor half. This is also exactly what makes a
gRPC implementation possible later: the descriptor half is trivially serialisable.

---

## 2. Record model

**Design question for canal:** one envelope, or per-mode envelopes? Where do op-type and before/after
images live? How do opaque bytes and structured values coexist?

**Fivetran** `[C-HIGH]` — the model is an **operation stream, not a record stream**. The connector emits a
sequence of typed operations; record-carrying operations are distinguished by op type (upsert / update /
delete are separate operations, not a field on one record type) `[C-MED on the exact set]`. Values are
carried in a proto `oneof` over primitive types plus a null marker `[C-MED]` — i.e. the wire format is
column-typed, not bytes. There is no before-image; `update` carries only changed columns, which is why
Fivetran's model is *destination-shaped* (it describes a mutation to apply to a table) rather than
*log-shaped*.

**Singer/Meltano** `[C-HIGH]` — records are JSON objects inside a `RECORD` message carrying the stream
name; there is **no op type at all** and no key/value split. Deletes are expressed out-of-band via the
`ACTIVATE_VERSION` message + a version column (full-table replacement semantics), which is a
well-documented awkwardness `[C-MED on message name]`. Structured-only: there is no bytes passthrough,
which is why Singer cannot carry binary payloads without base64.

**Estuary Flow** `[C-HIGH]` — documents are JSON with a declared **key** (a list of JSON pointers), and the
op-type problem is solved *differently and more interestingly*: rather than an op enum, Flow attaches
**reduction annotations** to the schema, so "how do two documents with the same key combine" is a property
of the schema, not of the record. See §5.

**Segment** `[C-HIGH]` — the inbound envelope is a fixed analytics event shape (type, userId/anonymousId,
event, properties, traits, context, timestamp); the destination never sees a generic record. Not a general
data-movement record model, and not directly transplantable — but see §7, which is where Segment is worth
studying.

**dlt** `[C-HIGH]` — Python dicts/objects; schema is inferred from data and accumulated; no op type in the
core path (merge/replace/append is a *write disposition* on the resource, i.e. a property of the pipeline
step, not of the record).

**Cross-cutting finding** `[C-HIGH]`: **none of these five has a CDC-shaped envelope in core.** Fivetran
comes closest and lands on mutation-shaped. Estuary deliberately replaces op-type with schema-level
reduction. Singer and dlt push it to write-disposition. This is the same gap Kafka Connect has
(`docs/research/kafka-connect.md` §Record Model), and the fact that five independent designs all dodge it
is evidence that the choice is genuinely hard rather than merely neglected.

**Canal implication.** Two live options, and they should be decided explicitly (this blocks R2):
- **(a) op-enum envelope** — `Op ∈ {Insert, Update, Delete, Snapshot, ...}` plus optional before-image.
  Familiar; risks being read as CDC-shaped by generic sinks that don't care.
- **(b) reduction-annotation envelope** (Estuary's answer) — records carry key + value; *how to combine*
  lives in the stream's schema. Strictly more general, composes with fan-in, and lets a snapshot record and
  a stream record be the same type. This is the more interesting idea and §14 flags it.

Either way: carry `Key` separately from `Value`, keep an opaque `[]byte` + declared codec path so binary
passthrough costs nothing, and put provenance (stream id, source position) in fields a transform structurally
cannot rewrite — the invariant Kafka Connect gets right via `SourceRecord.newRecord()`.

---

## 3. Checkpoint model

This is where the set differs most sharply, and the differences are the most transplantable material in the
document.

**Fivetran: the explicit in-band checkpoint.** `[C-HIGH]` The connector emits a **checkpoint operation into
the same ordered stream as the data**, carrying an opaque state value. Semantics: everything emitted before
the checkpoint is durable when the checkpoint is acknowledged; on restart the connector is called again with
that state. In the Python SDK this is an explicit call/yield the connector author makes — it is *not* a
timer, *not* a framework callback, and the connector, not the runtime, chooses the commit points
`[C-MED on the exact call name]`.

This is a genuinely different design from Kafka Connect's `offset.flush.interval.ms` timer, and it is
better for canal, for three reasons:
1. **Commit points align with source-meaningful boundaries** (end of a page, end of a table, end of a
   snapshot chunk) rather than with a wall clock. Connect's timer/chunk mismatch is a documented defect
   (see `kafka-connect.md` §Snapshot Handling — "a snapshot chunk can be fully emitted and acked, and still
   be re-emitted after a crash, because the flush timer hadn't fired").
2. **It is expressible over a wire.** A checkpoint that is a message in a stream survives being gRPC'd; a
   checkpoint that is a framework timer reading connector-held state does not.
3. **It gives the UI a real progress signal for free** — checkpoints are countable, timestamped events.

The cost, which canal must handle and Fivetran handles by fiat: a connector that never checkpoints makes no
durable progress, and a connector that checkpoints per record is pathologically slow. The framework must
therefore *also* be able to force/suggest a checkpoint, and must meter checkpoint frequency as a
first-class health signal.

**Singer/Meltano: state as a document the tap emits and the runner persists.** `[C-HIGH]` A `STATE` message
carries the whole state document; the convention is a top-level `bookmarks` map keyed by stream name. The
contract — and this is the part people get wrong — is that **the runner must only persist a `STATE` message
after every record preceding it has been durably written by the target**, which makes `STATE` an in-band
checkpoint exactly like Fivetran's. `[C-HIGH on the semantics; C-MED on the key name]`

Meltano's SDK adds **state partitioning** `[C-HIGH]`: a stream may declare partitioning keys, and state then
holds a *list* of per-partition entries, each carrying the context dict that identifies the partition plus
its own bookmark. This is the same idea as Kafka Connect's `sourcePartition`/`sourceOffset` pair but with
the partition identity being **connector-declared as a named set of keys**, so the framework knows the
*shape* of the partition space and can therefore enumerate, display, and reset individual partitions. That
is strictly more UI-friendly than Connect's fully-opaque map.

Verify in: `singer_sdk/helpers/_state.py` (the state arithmetic) and `singer_sdk/streams/core.py`
(`state_partitioning_keys`, and the increment/finalize helpers) `[C-MED on names]`.

**Estuary Flow: two checkpoints, and the connector chooses who owns which.** `[C-HIGH]` Flow distinguishes
a **runtime checkpoint** (the framework's own consistency point) from a **connector/driver checkpoint**
(opaque connector state). For captures, the connector emits its checkpoint alongside documents and the
runtime acknowledges; crucially the checkpoint can be declared as a **merge patch rather than a full
replacement** `[C-MED on the mechanism name]`, so a connector with many partitions does not have to
re-serialise all of its state on every commit. That is a real scaling property Connect and Singer both
lack — Singer's `STATE` is always the whole document.

For materializations, the connector may declare that **it** will store the runtime checkpoint
transactionally in the destination, which is what makes exactly-once possible without the framework owning
the destination `[C-HIGH]`. See §9.

**dlt: state lives in the destination.** `[C-HIGH]` Pipeline state is a JSON document written to a
`_dlt_pipeline_state` table in the destination alongside the data, and restored from there. Incremental
cursors are stored inside that state, keyed by source/resource `[C-MED on table name]`. The property worth
noting: **state and data share one durability domain**, so "the data landed but the state didn't" is
structurally impossible for destinations with transactional loads. This is the same insight as Estuary's
connector-stored checkpoint and Connect's "offsets in the sink" folk pattern, but it is the *default*
rather than an escape hatch.

**Canal implication (directly answers open decisions 1 and 9 in `design-rules.md`).**
- Checkpoint is an **in-band, ordered message** in the record stream, emitted by the source, meaning "all
  prior records are complete". Never a timer as the primary mechanism.
- Checkpoint payload is **opaque bytes + a declared codec id + a format version**, so the core never parses
  it and binary-upgrade compatibility is the connector's declared problem (decision 9).
- Support **partition-scoped checkpoints with merge semantics**, not one monolithic blob — take Meltano's
  declared partition keys (so the core knows the shape) plus Estuary's patch-not-replace (so it scales).
- The core commits a checkpoint **only after the sink has acknowledged durability** for every record
  preceding it. This is R4 restated, and it is exactly the Singer rule.
- Keep Connect's contiguous-acked-prefix watermark algorithm (`SubmittedRecords`) as the mechanism for
  turning out-of-order sink acks into a monotonic commit point.

---

## 4. Snapshot handling

**Design question:** is "full scan then tail" a first-class core concept, or smuggled into connector state?

**Fivetran** `[C-MED]` — no separate snapshot phase in the protocol. The connector receives state; empty
state means "start from the beginning"; the connector internally decides whether that means a full scan.
Progress is visible only as checkpoints. Same structural gap as Connect, mitigated by the fact that
checkpoints are connector-chosen so chunk boundaries *can* be commit points.

**Singer/Meltano** `[C-HIGH]` — modelled as **replication method per stream**, declared in the catalog
(`FULL_TABLE` vs `INCREMENTAL` vs `LOG_BASED`) `[C-MED on exact spellings]`. This is a genuinely good idea
that Connect lacks entirely: **the sync mode is a declared, per-stream, UI-selectable property**, so the
frontend can render "how should this table be replicated?" as a dropdown with only the modes the connector
said it supports. The weakness: the *handoff* from full-table to incremental is not modelled — there is no
"snapshot complete, now switch" transition in the protocol, so `LOG_BASED` taps hand-roll it inside their
own state, exactly as Debezium does under Connect.

Meltano SDK does model the *resumability* problem partially: a stream can declare whether its records are
**sorted by the replication key**, which determines whether a bookmark may be advanced progressively during
the sync or only finalised at the end `[C-HIGH on the concept; C-MED on the attribute names, likely
`is_sorted` / `check_sorted`]`. That distinction is important and canal needs it: **an unsorted stream
cannot be checkpointed mid-flight**, and if the core doesn't know which kind it has, it will either
checkpoint incorrectly or refuse to checkpoint at all.

**Estuary Flow** `[C-HIGH]` — captures are modelled as continuous; a backfill is expressed by changing the
binding's backfill counter, which restarts that binding's capture from scratch while the collection's
reduction semantics absorb the overlap. This is the cleanest answer in the set *because of* the reduction
model: if same-key documents combine deterministically, then re-emitting a row during a backfill is not a
correctness problem, so "snapshot then stream" needs no handoff protocol at all. That is a strong argument
for §2 option (b).

**dlt** `[C-HIGH]` — the incremental cursor with `initial_value` and `end_value` makes a backfill a
*bounded* incremental run: set both ends and you have a chunk; leave `end_value` unset and it tails. So
snapshot and stream are **the same code path with different bounds** `[C-MED on parameter names]`. That is
an elegant unification and it is trivially chunkable and parallelisable — N workers each get a
`[start, end)` window.

**Canal implication.** Model **splits/partitions and their bounds in the core**, generically:
- A source enumerates units of work, each with an optional upper bound. Bounded = snapshot chunk;
  unbounded = tail. Snapshot-then-stream is then "the split set changes over time", not a phase enum.
- A split declares whether it is **ordered by its cursor** (Meltano's sorted/unsorted distinction), because
  that is what licenses mid-split checkpointing.
- A split can report **exhaustion**, which gives the core the `COMPLETED` state Connect never had, and gives
  the UI a real percent-complete for the snapshot.
- Parallelism is then a core concern (assign splits to workers) rather than a connector concern
  (`taskConfigs(maxTasks)` frozen at start).

---

## 5. Schema handling

**Fivetran** `[C-HIGH]` — schema is **out-of-band and explicit**: a dedicated RPC/function returns the table
set with primary keys and optionally column types; anything not declared is inferred downstream. Schema
*changes* mid-stream are expressible as an operation in the stream `[C-MED]`. The destination side is where
this gets interesting: the destination service has explicit `DescribeTable` / `CreateTable` / `AlterTable`
RPCs `[C-MED on names]`, i.e. **schema evolution is a negotiated protocol between core and sink, not a
policy the sink invents.** Connect has nothing like this and every JDBC-ish sink reinvents "should I ALTER
TABLE".

**Singer/Meltano** `[C-HIGH]` — **JSON Schema, in-band, per stream, emitted before records**. The catalog is
the out-of-band form (discovery), the `SCHEMA` message the in-band form. Both are literally JSON Schema, so
the *same artifact* is used for validation, for the target's DDL decisions, and for a UI's field list. Using
one standard schema language for everything is a big practical win and the reason Singer targets can be
written generically.

Drift: the SDK can validate records against the declared schema and there are documented behaviours for
extra/missing properties `[C-LOW on specifics]`. The known weak spot is that JSON Schema is lossy for
database types — no native decimal/timestamp distinction beyond `format` strings — which is the same class
of problem as Connect's logical-types-as-name-conventions.

**Estuary Flow** `[C-HIGH]` — JSON Schema plus **reduction annotations**, and this is the idea most worth
stealing from this entire research pass. The schema carries annotations declaring, per location, how two
values for the same key combine — last-write-wins, first-write-wins, sum, min/max, merge (recursive
key-ordered merge), append, set operations `[C-HIGH on the concept and on lastWriteWins/sum/merge existing;
C-MED on the full strategy list]`. Consequences:
- The framework can **combine** records before writing, which is a real optimisation and also the mechanism
  by which duplicate delivery becomes harmless.
- Aggregation is declarative and lives with the data, so a sink needs no aggregation logic.
- It gives a principled answer to "what does a second record with the same key mean?" — which is exactly
  the question an op-type enum answers imperatively and less generally.

**dlt** `[C-HIGH]` — schema is **inferred from observed data and accumulated into a schema document** that
is versioned and persisted; evolution is governed by a **schema contract** setting per entity level (tables
/ columns / data types) with modes for evolve / freeze / discard `[C-MED on the exact mode names]`. That
three-axis × several-modes matrix is a much more precise drift policy than anything else in the set, and it
is *declared configuration*, so a UI can render it. Verify in `dlt/common/schema/schema.py`.

**Canal implication.**
- Schema is **data, not behaviour** — a serialisable descriptor, never a Go interface with methods. This is
  non-negotiable for the gRPC-later constraint; it is exactly where Connect's `Schema`/`Struct` fail.
- Ship **both** forms: out-of-band discovery (for the UI, pre-run) and in-band schema messages (for drift
  mid-run). Fivetran and Singer both have both; they are not redundant.
- Adopt dlt's **schema contract** as core config: a per-pipeline (table, column, type) × (evolve, freeze,
  discard, error) policy the core enforces, so no sink invents its own drift behaviour.
- Adopt Fivetran's **negotiated DDL** on the sink interface: an optional `Describe`/`Migrate` capability the
  core calls before writing, rather than letting each sink guess.
- Seriously evaluate **reduction annotations** as canal's answer to §2.

---

## 6. Lifecycle

**Fivetran Partner SDK** `[C-HIGH]` — the lifecycle *is* the RPC set, and it is the most instructive
lifecycle in the set because it separates concerns canal must also separate:
- **describe the config form** (pure, no I/O, no config) — for the UI
- **test the connection** (I/O, given a config; named tests so the UI can show which check failed)
  `[C-MED]`
- **describe the schema** (I/O, given a config)
- **sync** (a streaming RPC given config + state)

Note the shape: *three cheap, pure-ish, pre-flight calls before anything moves data.* That is the same
insight as Connect's "callable before `start()`" capability methods, but cleaner because there is no
long-lived object at all — every call is `(config, state) → response`. **A stateless request/response
lifecycle is what makes an out-of-process implementation trivial**, and canal's constraint #3 should be read
as "design the lifecycle so that no method depends on in-process object identity."

**Meltano SDK** `[C-HIGH]` — CLI-process lifecycle: a tap is invoked as a process with `--config`,
`--catalog`, `--state`, `--discover`, `--about`; it writes messages to stdout and exits. Streams have a
sync lifecycle with pre/post hooks and a child-stream mechanism where a parent record produces context for
child streams `[C-MED on hook names]`. Cancellation is process signals. Error classification exists as an
exception hierarchy (config errors vs retriable request errors vs fatal) `[C-LOW on the taxonomy]` — verify
in `singer_sdk/exceptions.py`.

**Estuary Flow** `[C-HIGH]` — explicit protocol phases: spec → discover → validate → apply → open → a
long-running streaming session. The **validate-then-apply pair** is worth noting: validation returns what
*would* happen (including per-binding constraints saying which fields are required/forbidden/optional in the
destination) and apply performs it. A plan/apply split at the connector boundary is unusual and very
UI-friendly — the frontend can show a diff before committing.

**dlt** `[C-HIGH]` — extract → normalize → load as three separable, independently resumable stages with
their own storage between them. The staged-filesystem-between-phases design means a failed load can be
retried without re-extracting. For canal this is an argument that the buffer between source and sink should
be a *durable, addressable* stage rather than an in-memory queue — which R6 already implies.

**Canal implication.**
- Every method takes `context.Context`. Non-negotiable; Connect's lack of it is a seven-year-open bug
  (KIP-419).
- Separate the lifecycle into **pure/cheap** (`Spec`, `ConfigSchema`), **I/O pre-flight** (`Check`,
  `Discover`, `Validate`), and **streaming** (`Open`/`Read`/`Write`/`Commit`/`Close`). The UI uses only the
  first two groups plus status.
- Adopt Fivetran's **named tests**: `Check` returns a list of `(test name, passed, message)`, not a single
  bool or error. A setup form that says "Credentials: OK / Network reachability: FAILED — timeout to
  host:port" needs no per-connector UI code.
- Adopt Estuary's **validate → apply** split for sinks so the UI can preview DDL/destination changes.
- Error classification: canal already has a better taxonomy than any of these in
  `design-rules.md` ("Ideas worth carrying forward" — transient-upstream / transient-internal /
  permanent-upstream / permanent-mapping / permanent-contract / duplicate-idempotent-success / clock-skew).
  None of the five systems studied has anything that rich. **Keep canal's; do not downgrade to a
  two-valued retriable/fatal split.**

---

## 7. Config model — the section that matters most for the UI requirement

This is the load-bearing section for canal's constraint that specialised UI must not require core changes.
Four genuinely different answers:

### 7.1 Fivetran: the connector returns a form definition

`[C-HIGH]` A dedicated RPC returns a **configuration form**: an ordered list of fields, each with a name, a
human label, a widget/type hint, required-ness, and — for enumerated fields — the allowed values; plus a
list of named connection **tests** the UI can run and display individually. `[C-MED on the field-level
attribute names and on whether conditional visibility is expressible.]`

Why this is the strongest pattern in the set for canal's purposes: **the form definition is a protobuf
message, i.e. plain serialisable data crossing a process boundary.** It has already been forced to survive
the exact constraint canal imposes in requirement #3. Anything expressible in a `ConfigurationForm` message
is expressible in canal's in-process interface *and* over gRPC later, by construction. Connect's `ConfigDef`
by contrast contains a `Recommender` — a live callback — which is why Connect's config surface cannot cross
a process boundary without redesign.

**The thing to check when verifying:** whether Fivetran's form supports *conditional* fields (show Y only
when X = Z) and *dynamic* option lists (populate a dropdown by querying the source). Those two features are
what separate a usable connector form from a flat property list, and they are also the two that are hard to
express as static data. Connect solves them with a live `Recommender` callback + `dependents` edges; Segment
solves them with `depends_on` + a `dynamic` flag naming a fetcher (see 7.4). **Static declaration plus a
named dynamic-fetch RPC is the design that satisfies both the UI and the out-of-process constraint** — the
form declares "this field's choices come from fetcher `list_tables`", and the core calls a separate
`GetChoices(field, partialConfig)` RPC. That is the synthesis canal should adopt.

### 7.2 Meltano SDK: JSON Schema, and a machine-readable `--about`

`[C-HIGH]` A plugin declares its settings as a **JSON Schema document** (built with typing helpers rather
than hand-written, but JSON Schema is the artifact). Secrets are marked as such so they can be redacted.
Capabilities are declared as a list of named capability constants (`catalog`, `discover`, `state`,
`about`, `stream-maps`, `batch`, `schema-flattening`, `activate-version`, …) `[C-MED on the exact set]`.
And `--about --format=json` makes the whole self-description — name, capabilities, settings schema —
**readable without running a sync**.

Two properties worth stealing:
- **A standard schema language, not a bespoke one.** JSON Schema means the frontend can use off-the-shelf
  form generators, validators exist in every language, and the artifact is diffable and versionable. A
  bespoke `ConfigField` struct is a perpetual tax.
- **Capabilities as a declared string set, separate from config.** Config says *how* to run; capabilities
  say *what this connector can do*. Keeping them separate is what lets a UI grey out "incremental" for a
  connector that only supports full refresh, without knowing anything about that connector.

Weakness `[C-MED]`: plain JSON Schema has no presentation layer — no groups, no ordering, no widget hints,
no conditional visibility beyond `if/then` awkwardness. Meltano compensates outside the SDK (in the hub's
plugin definitions, which carry `kind: password`, ordering, etc.). **So the honest lesson is: JSON Schema for
validation, plus a small presentation-annotation layer alongside it.** Connect's `ConfigDef` has the
presentation layer (`group`, `orderInGroup`, `width`, `displayName`, `importance`) and no standard schema
language; Meltano has the standard language and no presentation layer. Canal should have both, and should
put presentation in an `x-`-prefixed annotation block inside the JSON Schema rather than in a parallel
structure — one artifact, one source of truth (R9).

### 7.3 dlt: config derived from the function signature

`[C-HIGH]` Configuration is resolved by *reflection over type hints* — a resource/source function's typed
parameters with `dlt.config.value` / `dlt.secrets.value` sentinels are resolved from a layered provider
chain (env → `secrets.toml` → `config.toml` → defaults), with secrets segregated. Specs can also be
declared as dataclass-like configuration classes.

For canal this is a **cautionary** data point rather than a model. Reflection-derived config is superb
developer ergonomics for a library and actively hostile to a generic UI: there is no artifact to render,
the schema exists only as Python annotations, and required-ness/validation messages are discovered at run
time. Go struct tags + reflection would land in the same place. **The config schema must be an explicit
value the connector returns, not something the core derives from a struct.** (A helper that *generates*
that value from a tagged struct is fine — the point is that the artifact is the interface, not the struct.)

### 7.4 Segment action-destinations: the field spec is simultaneously the UI schema and the mapping DSL

`[C-HIGH]` A destination declares: an `authentication` block (a scheme, its fields, and a
`testAuthentication` function) and a set of named **actions**, each with a `fields` map and a `perform`
function (plus an optional batch variant). Each field entry is an `InputField` carrying `label`,
`description`, `type`, `required`, `default`, `choices`, `multiple`, `dynamic`, `depends_on`
`[C-HIGH on the concept; C-MED on the exact key names — verify in
packages/core/src/destination-kit/types.ts]`.

Three ideas here that nothing else in the set has:

1. **`required` can be conditional.** Required-ness is a predicate over the rest of the config, not a
   boolean. This is the thing that makes real setup forms possible ("api_key required unless auth_mode =
   oauth").
2. **`dynamic` names a fetcher.** The field declares that its choices are fetched at form-render time; the
   platform calls a corresponding function. Static declaration + named dynamic hook — the exact synthesis
   recommended in 7.1.
3. **`default` values are a mapping DSL.** Defaults can be *directives* that reference the inbound event
   (path expressions, conditionals) rather than literals. So the connector ships a **default mapping** from
   the generic record shape to its own fields, and the UI renders that mapping as an editable form.

That third idea deserves emphasis for canal. Segment's actions model is essentially: *a sink declares the
fields it wants, plus a default expression for how to get each one out of a generic record; the UI renders
that as an editable mapping.* This is a complete, working answer to "how does a generic UI configure a
specialised sink without the core knowing anything about it" — and it is the answer canal needs for
requirement #1 + the frontend goal simultaneously. A per-sink field spec with default extraction
expressions means:
- the core stays fully source/sink-agnostic (it only knows "fields" and "expressions");
- the UI is automatically specialised per sink (it renders that sink's fields);
- adding a sink is "declare fields, implement `perform`, register" — requirement #4 exactly.

**Canal implication — the concrete recommendation.** The connector descriptor should contain:
- a **JSON Schema** for config (validation, standard, off-the-shelf tooling);
- **presentation annotations** inside it (group, order, widget, label, secret, importance);
- **conditional required/visible** expressed as declarative predicates over the config (not callbacks);
- **named dynamic-choice hooks** (`choicesFrom: "list_tables"`) resolved by a separate core→connector call;
- a **declared capability set** (string constants) kept separate from config;
- **named connection tests** returned as a list of results, not a bool;
- for sinks, a **field/mapping spec with default extraction expressions** (Segment's model), which is what
  lets specialised sink UX exist with zero core changes.

Every item on that list is plain data. Nothing on it is a callback. That is the property that makes the
gRPC-later constraint free rather than a future migration.

---

## 8. Backpressure

`[C-MED overall — this is the topic I am least able to substantiate without source, and the one where I most
recommend re-running the fetch.]`

- **Fivetran**: the sync RPC is a server-stream from connector to platform. Flow control is therefore
  whatever gRPC's HTTP/2 windowing gives you — real but implicit; the connector's `Send` blocks when the
  consumer is slow. There is no application-level credit or accepted-count. `[C-MED]`
- **Singer/Meltano**: a Unix pipe between tap and target processes. **The pipe is the backpressure** — the
  tap blocks on write when the target is slow. Crude and completely effective, and notably it is the only
  mechanism in the set that requires zero API surface. `[C-HIGH]`
- **Estuary Flow**: transaction-scoped — the materialize protocol's explicit transaction boundaries mean the
  runtime cannot get arbitrarily far ahead of the connector; commit cadence is the flow control. `[C-MED]`
- **dlt**: stage-decoupled — extract writes to disk, load reads from disk, so source and sink speeds are
  decoupled by durable storage rather than by a blocking channel. Parallelism is configured per stage.
  `[C-MED]`
- **Segment**: request-per-event or batch, with platform-level retry/queueing outside the destination SDK.
  `[C-MED]`

**Canal implication.** Two things the sink interface must express that Connect's `void put(...)` cannot:
1. **How much was accepted** — return a count or a per-record result set, so partial acceptance is
   representable (this is R7: write the failure shape with the success shape).
2. **A slow-down signal** — either a blocking bounded writer (credit implicit) or an explicit
   "retry after" / "pause" outcome.
Combined with R6 (bounded by construction, rejection is an expressible outcome), the answer is a bounded
durable stage between source and sink whose `Append` can *reject* or *block*, and a sink `Write` returning
`(accepted int, perRecordErrors, retryAfter)`. Singer's pipe is the proof that blocking-is-enough for the
simple case; canal needs the explicit form because it must also report to a UI.

---

## 9. Delivery guarantees

**Fivetran** `[C-HIGH]` — at-least-once from the connector's perspective, made effectively exactly-once at
the destination by **upsert-with-primary-key semantics**: because the op model is mutation-shaped and every
table declares a primary key, replaying an upsert is idempotent by construction. This is a real design
insight: *if the record model is mutation-shaped and keyed, dedup is free.*

**Singer/Meltano** `[C-HIGH]` — at-least-once. The guarantee rests entirely on the runner's discipline of
persisting `STATE` only after the target confirms durability. If a runner persists state eagerly, you get
silent data loss — which is exactly canal's R4 failure, and it has bitten the Singer ecosystem in practice.

**Estuary Flow** `[C-HIGH]` — the only genuine **two-phase commit** in the set, and the model canal should
copy for sinks. The materialize transaction is an explicit multi-phase exchange (load existing docs → flush
→ store → start-commit → commit acknowledged → acknowledge) `[C-MED on phase names]`, with a **runtime
checkpoint the connector may persist transactionally in the destination**. That last part is the key: the
framework hands the sink its consistency token, the sink stores it atomically with the data, and on recovery
the framework asks the sink what it last committed. That yields exactly-once **without the framework owning
the destination and without assuming anything about what the destination is** — precisely canal's
constraint #1. Contrast Connect, which achieved exactly-once for sources only by putting offsets inside a
*Kafka* producer transaction, i.e. by assuming Kafka.

**dlt** `[C-HIGH]` — state-in-destination gives the same property by a simpler route: if the load is
transactional, data and state commit together. `merge` write disposition with a declared primary key
provides idempotency where loads are not transactional.

**Canal implication.** Design the sink interface with an **optional two-phase capability**, asserted by type
assertion, not required of all sinks:
- Base sink: `Write(ctx, batch) (accepted, errs, error)` — at-least-once, idempotency is the sink's problem.
- `TransactionalSink` (optional): `Prepare(ctx) (committable, error)` / `Commit(ctx, committables) error`,
  and — the Estuary/dlt trick — allow the sink to declare **"I will store the checkpoint token myself"**,
  with a `RecoverCheckpoint(ctx) (token, error)` for restart. This is the only design that reaches
  exactly-once for a *generic* sink.
- Keep canal's three-layer idempotency model from `design-rules.md` (upstream vendor id → canonical record
  id → in-flight submit guard) plus the deterministic-derivation rule. None of the five systems has anything
  as explicit; it is a genuine canal advantage.
- Note R5: dedupe key must carry tenant + source + stream, and be committed *after* the write.

---

## 10. Plugin boundary

| System | Boundary | Discovery | Versioning |
|---|---|---|---|
| Fivetran Partner SDK | **out-of-process gRPC** (connector is a server; platform is the client) `[C-HIGH]` | platform-side registration of an image/endpoint `[C-MED]` | protobuf field-number compatibility; repo appears re-versioned (`v2`) `[C-MED]` |
| Fivetran Connector SDK (Python) | in-process Python, packaged and deployed to Fivetran `[C-HIGH]` | the module exposes a connector object `[C-MED]` | SDK version pin `[C-MED]` |
| Singer / Meltano | **out-of-process subprocess + stdio JSON lines** `[C-HIGH]` | separate executables; a hub/registry maps names to packages `[C-HIGH]` | plugin package version + declared `capabilities` set for feature negotiation `[C-HIGH]` |
| Estuary Flow | **out-of-process** (container image speaking the protocol; Airbyte-protocol connectors accepted via a shim) `[C-HIGH]` | image reference in the catalog `[C-MED]` | protocol version + per-connector image tag `[C-MED]` |
| dlt | **in-process Python** — decorated functions, no plugin protocol `[C-HIGH]` | import + decorator registration `[C-HIGH]` | Python package versioning |
| Segment | in-process TypeScript modules in a monorepo, deployed as one platform `[C-HIGH]` | a destination object per package, aggregated at build `[C-MED]` | monorepo versioning; no external ABI |

**The most useful observation for canal** `[C-HIGH]`: **four of the six boundaries here are already
out-of-process, and the two that are not (dlt, Segment) are the two whose interfaces canal cannot borrow
wholesale** — dlt because config is reflection-derived, Segment because `perform` closes over a live HTTP
client and the field defaults are an interpreted DSL.

The three out-of-process protocols (Fivetran gRPC, Singer stdio, Estuary) converge on the same discipline,
and it is exactly the discipline `kafka-connect.md` derived by negation:

- every operation is `(config, state, records) → (records, state, status)` over **plain serialisable data**;
- **no callbacks, no futures, no live handles, no type handles** cross the boundary;
- capabilities/config/schema are **messages**, not method calls on an object;
- the connector holds no identity the core depends on between calls (or, for streaming, exactly one
  long-lived session with an explicit open/close).

**Canal implication.** Constraint #3 is satisfied *iff* the interface obeys those four rules. Concretely:
- `Record`, `Schema`, `Checkpoint`, `ConfigSchema`, `Capability`, `TestResult`, `Split` must all be
  **structs of bytes and primitives**, never interfaces with behaviour.
- The registry keys connectors by a **stable string id + version**, and the descriptor is retrievable
  without instantiating anything.
- Optional capabilities go behind **separate, independently-assertable Go interfaces** discovered by type
  assertion — never as "returns nil if unsupported" methods on a fat interface. (Connect's `default`-method
  approach breaks linkage in one direction and Go has no equivalent escape hatch at all.)
- The **context** is an interface the *core* implements and passes in, so growing core capabilities never
  breaks a connector.
- Borrow Meltano's **declared capability strings** as the negotiation mechanism, and treat the protocol
  version as a first-class field checked at registration.

---

## 11. Observability

`[C-MED overall — verify against `singer_sdk/metrics.py` and `estuary/flow` `go/protocols/ops/ops.proto`,
both of which are in the manifest and neither of which I could read.]`

- **Meltano SDK** `[C-MED]` emits structured metric log lines during a sync — record counts per stream,
  timer/duration for HTTP requests, sync durations — as a defined log format (a metric name, a value, and
  tags including stream and endpoint). The valuable property: **the SDK emits them, not the connector
  author**, so every tap gets the same metric names for free. This is the same principle as Connect's
  `PluginMetrics` (plugin declares, core names/tags/exports), reached from the other direction.
- **Estuary Flow** `[C-MED]` has a dedicated ops protocol/collections concept — logs and stats are
  themselves data written to collections, so operational telemetry is queryable with the same machinery as
  user data. Interesting and unusually principled; worth reading before designing canal's status surface.
- **Fivetran** — sync history, row counts and errors are surfaced in the product UI; not part of the
  connector SDK contract as far as I know `[C-LOW]`.
- **dlt** — load info / trace objects returned from a pipeline run, containing per-load counts and timings
  `[C-MED]`.

**Canal implication.** Copy the *ownership* rule from all of them: **the core owns metric names, tags and
export; the connector only declares/increments.** Kafka Connect's naming discipline
(`<noun>-<aggregation>`, and duty-cycle gauges like `running-ratio`) is documented in
`kafka-connect.md` §Observability and is a better naming model than anything here. Add the two things
Connect *cannot* have because its offsets are opaque and canal's will not be:
- **source lag** and **snapshot percent-complete**, computable generically because the core understands
  split bounds (§4);
- **checkpoint cadence** — time since last durable checkpoint, per split. This is the single most important
  health number in a checkpoint-driven design and no system in this set exposes it.

And honour the `design-rules.md` "honesty as a structural property of the UI" rule: the status model must
distinguish *the connector answered* from *your data arrived*. Concretely that means the status surface
carries `last_checkpoint_at` and `records_durably_written`, not just `state: running`. Canal's required
connector state machine (`healthy → degraded → paused → terminal` with a last-error surface, open decision
7) is richer than Connect's `State` enum and richer than anything in this set — keep it, and add
`completed` for exhausted bounded splits.

---

## 12. Deployment

- **Fivetran** `[C-HIGH]` — fully managed SaaS; the connector is a deployed unit the platform schedules.
  Partner-SDK connectors run as the platform's own workloads. No user-visible coordination model. The
  relevant transplantable fact is that the *protocol* is deployment-agnostic: because every call is
  `(config, state) → stream`, the platform is free to run it anywhere, any number of times.
- **Singer/Meltano** `[C-HIGH]` — a process invocation. Orchestration is entirely external (Meltano CLI,
  Airflow, Dagster, cron). Scaling is "run more processes with different config/state slices", and there is
  **no coordination or work-assignment layer at all** — which is why Singer is excellent for a single-binary
  local mode and useless for coordinated multi-worker.
- **Estuary Flow** `[C-HIGH]` — built on Gazette; collections are partitioned journals, shards are the unit
  of assignment, and the runtime handles assignment/recovery with recovery logs. This is the only one of the
  five with a real distributed story, and it is a *derived* one (Gazette provides it).
- **dlt** `[C-HIGH]` — a library in your process. No deployment model of its own.
- **Segment** `[C-HIGH]` — destinations run inside Segment's platform; the SDK author never thinks about
  deployment.

**Canal implication.** The Connect two-mode pattern remains the right answer and is confirmed by contrast
here: **keep the connector-facing interfaces byte-identical between standalone and distributed, and swap
only the storage and coordination implementations behind them.** The seam that makes it work is a
bytes-in/bytes-out checkpoint store (Connect's `OffsetBackingStore`), and it is cheap to copy.

What this set adds over Connect: Estuary's split of *assignment unit* (shard) from *data unit* (journal
partition) generalises Connect's connector/task split, and it is the shape canal should have if splits are
core-modelled (§4). Work assignment then reduces to "assign splits to workers", with rebalancing a core
concern, and Connect's fixed-at-start `taskConfigs(maxTasks)` limitation disappears.

---

## 13. What they got right / what they got wrong

### Right

- **Fivetran: checkpoint as an explicit in-band operation.** Connector-chosen commit points, ordered with
  the data, expressible over a wire. Strictly better than a framework timer. `[C-HIGH]`
- **Fivetran: the connector returns its own config form as a protobuf message.** A UI schema that has
  already survived a process boundary. `[C-HIGH]`
- **Fivetran: named connection tests.** Actionable setup diagnostics with no per-connector UI code.
  `[C-MED]`
- **Fivetran: negotiated destination DDL** (`DescribeTable`/`CreateTable`/`AlterTable`), instead of every
  sink inventing its own drift behaviour. `[C-MED]`
- **Meltano: JSON Schema as the one schema language** for config, records, and catalog. Off-the-shelf
  tooling everywhere. `[C-HIGH]`
- **Meltano: capabilities as a declared string set**, separate from config, enabling feature negotiation and
  UI graceful degradation. `[C-HIGH]`
- **Meltano: `--about --format=json`** — full machine-readable self-description without running a sync.
  `[C-HIGH]`
- **Meltano: declared state partitioning keys** — the framework knows the *shape* of the partition space,
  so it can enumerate/display/reset individual partitions. Better than Connect's fully-opaque map.
  `[C-HIGH]`
- **Meltano: the sorted/unsorted declaration** determining whether a bookmark may advance mid-sync. A
  correctness-critical property that most frameworks leave implicit. `[C-MED]`
- **Singer: the pipe as backpressure.** Zero API surface, completely effective. `[C-HIGH]`
- **Estuary: reduction annotations.** Replaces imperative op-types with declarative combine semantics
  attached to the schema; makes duplicate delivery harmless and backfill/stream handoff a non-problem.
  `[C-HIGH]`
- **Estuary: real two-phase commit with a connector-stored runtime checkpoint.** Exactly-once for a generic
  destination without the framework owning it. `[C-HIGH]`
- **Estuary: checkpoint-as-patch** rather than always-full-document. Scales to many partitions. `[C-MED]`
- **Estuary: validate → apply.** The UI can preview destination changes before committing. `[C-HIGH]`
- **dlt: snapshot and stream as one code path with different cursor bounds.** Trivially chunkable and
  parallelisable. `[C-HIGH]`
- **dlt: state stored in the destination**, so data and progress share one durability domain. `[C-HIGH]`
- **dlt: schema contracts** — an explicit (tables/columns/types) × (evolve/freeze/discard) drift policy as
  declared configuration. `[C-MED]`
- **Segment: `InputField` as UI schema and mapping DSL at once** — conditional `required`, `depends_on`,
  `dynamic` choice fetchers, and defaults that are extraction expressions over the inbound record.
  `[C-HIGH]`

### Wrong — documented pain

**This half of section 13 is the most compromised by the fetch outage.** The brief asked for "concrete
documented pain: known issues, community complaints, rewrites, deprecated abstractions, leaky interfaces" —
and that is exactly the material that requires reading issue trackers, changelogs and migration guides,
none of which I could reach. What follows is limited to structural criticisms I can derive from the designs
themselves, which is a weaker class of evidence, plus a list of the specific pain I know exists and could
not substantiate. **All of the following is `[C-MED]` at best and several items are `[C-LOW]`.**

Structural criticisms (derivable, still unverified):

- **Singer's biggest wound is that the spec is underspecified about who persists `STATE` and when.** The
  correctness of the whole ecosystem rests on a convention rather than a protocol obligation, and a runner
  that persists state eagerly silently loses data. This is canal's R4 as an ecosystem-wide defect. Canal
  must make it structural: the core, not the connector or the runner, decides when a checkpoint is durable,
  and it decides after the sink acknowledges. `[C-MED]`
- **Singer has no delete semantics.** `ACTIVATE_VERSION`/full-table-replacement is a workaround, not a
  model, and it forces full reloads for streams that only needed a tombstone. Direct evidence that omitting
  op-type/tombstone from the record model has a real cost. `[C-MED]`
- **Singer catalog/metadata is genuinely awkward** — replication method and selection live in per-stream
  metadata entries addressed by breadcrumb paths rather than as plain typed fields. A UI has to understand
  the breadcrumb convention. Canal should express selection and replication method as **typed fields on a
  stream descriptor**, not as annotations addressed by path. `[C-MED]`
- **JSON Schema is lossy for database types** (decimal precision, timestamp/timezone, unsigned integers).
  Both Singer and Estuary inherit this. Canal will need a type system that maps *onto* JSON Schema for UI
  purposes while carrying a richer logical type for correctness — and must decide this explicitly rather
  than discovering it in a sink. `[C-MED]`
- **Fivetran's model is destination-shaped.** Because operations describe mutations to a keyed table, a
  source with no primary key, or a sink that is not table-shaped (a queue, an object store, a webhook), is
  awkward. **This is the specific trap canal's constraint #1 forbids**: Fivetran's op model is a
  BigQuery/warehouse-shaped assumption in the core, and it is worth studying precisely as the thing not to
  do. Canal's envelope must not assume the sink has tables or keys.
- **dlt's reflection-derived config is UI-hostile** (§7.3) — no renderable artifact. Cautionary, not
  transplantable.
- **Segment's model is event-analytics-shaped** — a fixed inbound envelope. The `fields`/mapping idea
  transplants; the record model does not.
- **Estuary's power is inherited from Gazette.** The reduction/2PC design assumes a runtime that can
  replay and combine by key. Canal must be honest about which parts of the elegance require that substrate
  and which are free. Reduction annotations *as a schema feature* are free; reduction as an *optimisation
  the runtime performs mid-pipeline* is not.

Specific pain I believe exists and **could not substantiate — do not repeat these without checking**
`[C-LOW]`:

- Singer taps in the wild diverging on state shape and on whether bookmarks are inclusive or exclusive
  (producing off-by-one row duplication/loss at resume boundaries).
- Meltano's `BATCH` message extension and its adoption/stability status, and whether it is still considered
  experimental. I could not verify what the batch message actually contains.
- Airbyte-protocol compatibility friction in Estuary (which fields/behaviours are lossy through the shim).
- Fivetran Partner SDK versioning history — the `v2` path in the manifest suggests a breaking protocol
  revision whose migration notes would be highly relevant to canal's own versioning plan.
- RudderStack: assessed as low-value before the outage; not researched. Its destination-transformation model
  is closer to Segment's than to a data-movement SDK.

---

## 14. Steal this

1. **Checkpoint as an explicit, ordered, in-band message emitted by the source** (Fivetran) — connector
   chooses commit points at source-meaningful boundaries; the core commits it only after the sink
   acknowledges durability (Singer's rule, canal's R4).
2. **The connector returns its config surface as plain serialisable data** — JSON Schema for validation plus
   presentation annotations inside the same document, with conditional `required`/`visible` as declarative
   predicates and dynamic choice lists as *named hooks* the core calls separately (Fivetran + Meltano +
   Segment synthesis). No callbacks in the config surface; that is what keeps gRPC-later free.
3. **Segment's `InputField` mapping model for sinks** — each sink declares its fields plus a *default
   extraction expression* over canal's generic record, so specialised sink UX is automatic and the core
   stays sink-agnostic (satisfies constraint #1 and #4 simultaneously).
4. **Declared capability strings, separate from config** (Meltano) — feature negotiation at registration,
   UI graceful degradation, and optional Go interfaces discovered by type assertion rather than fat
   interfaces with nil-returning methods.
5. **Estuary's reduction annotations** as the alternative to an op-type enum — declare per-location combine
   semantics in the stream schema, which makes duplicate delivery harmless and dissolves the
   snapshot→stream handoff problem.
6. **Estuary/dlt's connector-stored checkpoint** — an optional `TransactionalSink` that persists the core's
   consistency token atomically with the data and returns it on recovery: exactly-once for a *generic*
   destination without the core owning it.
7. **dlt's bounded-cursor unification** — snapshot and stream are one code path with `[start, end)` bounds,
   making chunking and parallelisation fall out for free; combine with core-modelled splits that can report
   exhaustion (giving the `completed` state Connect never had).
8. **Meltano's declared state-partitioning keys and sorted/unsorted flag** — the core learns the *shape* of
   the partition space (so it can enumerate, display and reset partitions) and learns whether mid-split
   checkpointing is even legal.
9. **Named connection tests returning a list of results** (Fivetran) — actionable setup diagnostics with
   zero per-connector UI code.
10. **Validate → apply for sinks** (Estuary) plus **negotiated DDL** (Fivetran) — the UI previews
    destination changes, and no sink invents its own drift policy; pair with **dlt's schema contract** as
    core config.
11. **`--about`-style machine-readable self-description** (Meltano) — the full descriptor readable without
    running anything, which is the precondition for a UI that needs no per-connector knowledge.
12. **Checkpoint cadence as a first-class metric** — time since last durable checkpoint, per split. Nothing
    in this set exposes it, and in a checkpoint-driven design it is the most important health number there
    is.

---

## Open items for the verification pass

Run §0.1, then specifically nail down:

1. Every signature in §1 (not written).
2. Whether Fivetran's configuration form supports conditional visibility and dynamic option lists, and how.
   This decides whether canal's §7 synthesis has prior art or is novel.
3. The exact Fivetran operation set and value encoding, and whether a before-image is expressible.
4. Meltano's exact capability constant set, and the `BATCH` message's actual payload.
5. Meltano's state document shape for partitioned streams, verbatim, from `helpers/_state.py`.
6. Estuary's materialize transaction phase names and the full reduction-strategy list.
7. Estuary's driver-checkpoint merge/patch mechanism — exact field and semantics.
8. dlt's schema-contract mode names and the incremental parameter set.
9. Segment's `InputField` key names from `destination-kit/types.ts`, and the directive grammar for defaults.
10. The whole second half of §13 — issue trackers, changelogs, migration guides. This is where the real
    documented pain lives and none of it was reachable.

# Completeness audit — what the whole effort has not addressed

**Status: DRAFT.** Not normative (design rule R12). This document makes no decision; it names gaps and,
for each, the minimum decision needed now to keep a door open. Anything adopted from here must move into
`docs/architecture.md` or a numbered ADR to become binding.

**Scope.** This is not a quality review of the design that exists. Twelve hostile reviews and eight stress
probes have already done that, and §25's defect ledger absorbs their findings. This asks a different
question: across the owner's stated end-state goals, **which received the least real design attention, and
what is still hand-waved?**

**As of.** `pkg/connector/{runtime,source,sink,state,caps,source_optional,lane,lanectl}.go` and
`pkg/record/{ids,batch,record,change}.go` were being edited while this audit ran — the connector surface is
mid-repair against the stress-probe findings (`SourceRuntime` gained `Streams`, `Schemas`, `Declare`,
`Instance`, `Config`; `record.DeriveLaneID` is now exported). Every claim below was re-verified against the
post-edit tree. `pkg/config`, `pkg/spec`, `pkg/registry`, `pkg/store` and `pkg/telemetry` were untouched,
and every gap in this document lives in those five packages, in `docs/`, or in code that does not exist yet.

**Method.** Read `docs/architecture.md` (§1–§29), all 24 ADRs, every file in `pkg/` and `internal/`, and the
eight stress-probe verdicts. Then compared the *stated end-state goals* and the *nine must-close items*
against what exists as (a) a decision, (b) a typed contract, (c) code. A goal with a section but no type is
hand-waved. A goal with a type but no attachment point is half-built.

**Headline.** The data path is the most thoroughly designed part of this effort by a wide margin: the lane,
the ledger, the three-phase commit, the record model, the capability mechanism and the store seam are all
closed with types and reasons. The gaps cluster in three places, in descending order of neglect:

1. **The frontend** — a beautiful read path, no write path, no ADR, no code, and a read model that does not
   survive the effort's own scale targets.
2. **Serialization** — the encode half is decided and attached; **the decode half has no attachment point
   at all**.
3. **The control plane's intent model** — the architecture declares that reconciliation is the only control
   mechanism, which makes every operator command durable desired state, and then gives desired state no
   home.

Plus one testing gap that matters more than its severity suggests: the conformance kit is the only evidence
for constraint #4, and as specified it cannot test the things it lists.

---

## How to read the seriousness / deferability columns

The useful distinction is not "how bad" but **"what does deferring cost"**. Three cases:

- **Free to defer** — the fix is additive to a Go struct or a JSON document and no durable or
  operator-visible data changes meaning.
- **Forces a breaking change** — the fix later must move operator-visible config keys, change a persisted
  format, change a wire contract that has fixtures pinned against it, or change data already written into a
  customer's own system. Deferring buys a migration you cannot perform unilaterally.
- **Unrecoverable** — deferring admits a silent-data-loss path into production, and the loss precedes the
  discovery.

Only the middle and last categories justify spending design budget now.

---

# Critical

## G1. The decode half of serialization has no attachment point

**Serialization is a named end-state goal. Half of it is built.**

What exists: `connector.Encoder`, `Framer`, `Compressor` — and their inverses `Decoder`, `Deframer`,
`Decompressor` — all four/six as interfaces with `Caps` (`pkg/connector/codec.go`). All are registered
kinds: `KindEncoder`, `KindDecoder`, `KindFramer`, `KindDeframer`, `KindCompressor`
(`pkg/registry/kind.go`), each with a `*Def`, an `AddX`, a spec and a descriptor.

What does not exist: **any way to configure a decode chain.**

- `config.CodecRef` (`pkg/config/composites.go:419`) has exactly six fields: `Encoder`, `EncoderConfig`,
  `Framer`, `FramerConfig`, `Compressor`, `CompressorConfig`. There is no `Decoder`, no `Deframer`, no
  `Decompressor`, and no second reference type.
- `config.Fields.Codec` renders one object with `encoder` / `encoder_config` / `framer` / `compressor`, and
  its doc comment says "attached per **SINK NODE**".
- `registry.withStageStandard` (`pkg/registry/stage_standard.go`) appends `codec` under `case KindSink`
  only. `case KindSource` gets `lane_budget` and `heartbeat_interval`. There is no `decode` field anywhere,
  for any kind.
- ADR 0022 is titled *"Serialization is three registered stages attached per sink node"*. It mentions
  `Decoder.Decode` and `Deframer.Split` as signatures and never says where either is attached or
  configured.
- `internal/engine/graph.go:127` lists `KindDecoder` among the kinds that can fail per record, implying a
  decoder is sometimes a *node*; §15.2 and ADR 0022 say codecs are *stage-standard fields, never nodes*.
  No graph rule validates decoder-node arity, and `Kind.Produces()`/`Consumes()` are both false for it, so
  a decoder node is neither refused nor specified. Two half-mechanisms for one concept is the R9 failure
  this design otherwise polices well.

**Why this is critical rather than cosmetic.** The refusal table in §15.3 contains the row
"`Decoder.Accepts` ⊇ the source's produced kinds — prevents a codec pair that cannot compose, discovered
on record 1". That check is unimplementable: nothing names a decoder, so there is nothing to check. And
downstream of a byte source, with no decode stage:

- `config.Mapping`'s `SourcePayloadField` — the mechanism §15.2 offers as constraint #1's answer for
  specialised sink UIs — has no structured payload to read from.
- The `filter`, `route` and `redact` transforms, and `BatchPolicy.FlushOn`'s predicate over a record, all
  operate on fields that do not exist.
- Engine-owned dedupe keyed on a payload field is inexpressible.
- `Payload` "never converts" by design (§4.5, and correctly so), so nothing downstream can rescue it.

The only remaining option is for every byte source to parse internally, with its own private config keys —
which is precisely the N-frontends outcome §15's opening paragraph exists to prevent, and precisely the
defect XE-f1 that §25.1 records as *Fixed* on the encode side.

**Deferring forces a breaking change.** Once `connectors/kafka` ships `value_format: json` and
`connectors/http` ships `parse: ndjson`, centralising decode moves operator-visible config keys in every
stored pipeline document. That is a migration across data you do not own.

**Minimum decision now.** Mirror the encode side exactly:

1. Add `config.DecodeRef{Decoder, DecoderConfig, Deframer, DeframerConfig, Decompressor,
   DecompressorConfig}` and `config.Fields.Decode(name)`, with the same `Choices` hooks
   (`decoders`, `deframers`, `compressors`).
2. Append `decode` in `withStageStandard` under `case KindSource`, gated on a new
   `SourceCaps.Structured` (the exact mirror of `SinkCaps.Structured`): a source that hands the engine
   structured records is not offered a decoder, and attaching one is refused structurally, the same way
   double-encoding is.
3. State the rule in §9.3 and amend ADR 0022's scope: **a source that emits bytes must not decode them
   itself.**
4. Then the `Decoder.Accepts` refusal row becomes implementable, and `SourceCaps.Produces []record.Kind`
   is the data it reads.

Nothing above changes an existing type's meaning. It costs one composite field and one caps bool today.

## G2. There is no durable model of operator intent

§23.4 is unambiguous: *"the reconciler is the only control mechanism and there is no separate command
protocol to lose messages."* `Reconcile(plan, catalog, lanes, now) []Action` is a pure function on a
30-second timer. That is the right architecture — and it has a hard precondition: **every operator command
must be expressible as durable desired state the reconciler converges toward.** Nothing in the effort
holds that state.

- `spec.Spec` (`pkg/spec/spec.go`) has `Tenant, ID, Revision, Title, Graph, Guarantee, Retry, WhenFull,
  LaneBudget, Drift, Clock, Parallelism, Streams`. No `Paused`, no `DesiredState`, no `Downgrades`, no
  offset-reset request, no drain request.
- `store.LaneRow` has `ID, Name, Group, After, Spec, Bounded, Finished, FinishedAt, Weight`. No intent
  fields.
- There are four store interfaces and §17 states flatly that a fifth means the abstraction is wrong.
- `engine.Pipeline` has `Run` and `Close` (`internal/engine/build.go`). No `Apply`, no `Pause`, no
  reconciler type, and nothing in the module consumes `ConfigStore.Watch`.

Consequences, each a documented feature with no mechanism:

| Documented | Where it is claimed | What is missing |
|---|---|---|
| `PhasePaused`, and `paused` in the legacy vocabulary map | `pkg/telemetry/status.go`, §16, ADR 0019 | nothing can record that an operator paused a pipeline; a paused pipeline resumes on restart |
| "sustained backoff must transition the connector to a visible `paused` state" | design-rules "ideas worth carrying forward" | same |
| The operator-signed `Downgrade` | `pkg/telemetry/negotiated.go:59`, ADR 0024, the §15.3 refusal table's *only* escape hatch | `telemetry.Downgrade` is described as "CONFIG-DECLARED" and `spec.Spec` has no field to declare it in. Already flagged as F9 by the fan-out probe and still open. The refusal table reads a field that cannot exist |
| Offset edit "only while the pipeline is stopped", every edit "bumps Generation" | §13.3 | "stopped" is not a durable state; and a `PATCH` that bumps `Generation` conflates an offset edit with a config change, re-planning every lane |
| Drain on demand, force-reassign, resume-after-terminal | §16, §17, §23.3 | no representation |

**Deferring forces a breaking change.** Intent must land either in `spec.Spec` — which is revisioned,
CAS'd, the body of the API's write endpoints, and coupled to `Generation`/`ObservedGeneration`/
`CondSpecApplied` semantics — or in a fifth store, which §17 forbids. Both are changes to durable,
operator-visible contracts, and the second is an architectural reversal.

**Minimum decision now.** Decide *where intent lives* and *what it does to generation*:

```go
// in spec, alongside Graph — the reconciler's target, not the topology
type Intent struct {
    DesiredState DesiredState // running | paused | drained
    Downgrades   []Downgrade  // moved out of telemetry; the field 0024 already reads
    Resets       []LaneReset  // lane + token, consumed once, then cleared
}
```
with one rule that makes it cheap: **an `Intent` change bumps `Revision` but not `Generation`.** A pause is
not a re-plan; a graph edit is. That single sentence is what stops "pause" from re-assigning 900 lanes, and
it is the reason to decide this now rather than after the API ships. `Downgrade` must move from `telemetry`
into `spec` (or `connector`) so the negotiation can read it without `spec` importing `telemetry`.

## G3. No reload/restart-impact classification — and a node rename silently orphans every cursor

`record.LaneID` is *"derived deterministically from (tenant, pipeline, node, LaneSpec.Name)"*
(`pkg/record/ids.go:31`). `spec.Node.ID` is *"assigned by the operator"* (`pkg/record/ids.go:18`).
`store.LaneKey(tenant, pipeline, lane)` is the durable cursor key.

Therefore: **an operator who renames a node id in a running pipeline's spec re-reads every lane from the
beginning, silently.** `ConfigStore.Put` takes `ifRevision` and validates nothing about compatibility with
persisted state. No rule declares `Node.ID` immutable. No diagnostic code covers it (`config.Code` has
`CodeGraphInvalid` but nothing for "this edit orphans state"). Nothing distinguishes it from renaming a
`Label`.

This is not a hypothetical the effort failed to imagine — it is a recommendation the effort's own research
made and then dropped. `docs/research/observability-controlplane.md` §7.5 ("Reload semantics"), marked
`[C]`:

- *"The checkpoint key is derived from stable identity (`pipelineID` + `StreamID`), never from a config
  hash or an ordinal ... This is a one-line decision with catastrophic failure mode if got wrong, and it is
  the kind of thing that is unfixable after the first production deployment."*
- *"A spec change that alters stream identity or codec is a `Restart`, not a `Reload`, and the API says
  which. The validation response tells the operator: `"impact": "restart_required"` with the reason. Never
  silently restart a pipeline that the operator thought they were tweaking."*

Neither sentence appears in `architecture.md`, in any ADR, or in any type. The validate response is
`{diagnostics, negotiated}` — no `impact`. `config.Field` has `Optional`, `Advanced`, `Secret`,
`Deprecated` — no mutability class. `Generation`/`ObservedGeneration`/`CondSpecApplied` tell you *whether*
a change applied and never *what applying it costs*. Vector's diff-the-topology-and-rebuild-only-what-
changed model is cited nowhere.

**Deferring is unrecoverable on the cursor-orphaning half** (the loss precedes the discovery) and
**forces a breaking change on the API half** (`impact` added to a validate response the UI already renders,
and a class added to `config.Field`, which is the pinned form contract).

**Minimum decision now.** Three sentences and one enum:

1. **`Node.ID` is immutable for a pipeline's life.** `ConfigStore.Put` refuses an edit that removes or
   renames a node id carrying persisted lane state, with a diagnostic naming the affected lanes. The escape
   hatch already exists: `StateAdopter.AdoptsStateOf()` (ADR 0020 rule 8) generalised to node ids, or an
   explicit reset through the offsets API.
2. Add `Impact` to the validate/build response — closed set `{none, reload, restart, state_reset}` — and a
   matching `config.Field.Impact` so a form can warn *before* the operator saves. Default `restart`, which
   is the safe answer and forces a per-field decision to be deliberate.
3. Write down that reload is diff-based, not stop-start, even if v1 implements it as stop-start: the
   *response field* is the contract, and the implementation can improve behind it.

## G4. The frontend has a read path and nothing else — no write API, no ADR, no code

The frontend is a stated end-state goal. §21(d) is genuinely excellent on the read path: five
connector-authored data artefacts, one core-owned read model, `Descriptor` without instantiation,
`config.Spec` as the form model, `Choices` hooks for dynamic values, the nil-pointer honesty rule, the
`Complete`/`Missing` partial-document rule, and the per-UI-element derivation table. That work is real and
should be kept as-is.

Everything else about the frontend is absent:

- **No write API.** Grepping every HTTP method in `architecture.md` yields exactly five lines:
  `POST /v1/pipelines:validate`, `PATCH /v1/pipelines/{id}/offsets`, and two offset `DELETE`s. There is no
  create, update, or delete pipeline; no pause/resume/start/stop/drain; no downgrade signing (which G2
  shows has no representation either); no assignment or worker view; no tenant/token/store admin (ADR 0017
  grants an `admin` role three powers with no endpoints); no reset-and-rerun.
- **No error envelope.** `config.Diagnostics` is the validation shape. There is no shape for 401/403/404/
  409-on-CAS-conflict/412/429/500, and CAS conflict is a first-class outcome of every `Put`.
- **No ADR.** ADRs 0001–0024 cover the record model, commit, lanes, capabilities, tenancy, codecs,
  conformance, guarantee tiers — and not one covers the API or the UI. For a stated end-state goal that is
  the single largest asymmetry in the effort.
- **No artefact.** There is no OpenAPI or JSON Schema for the API, though `config.Spec.JSONSchema()` exists
  for connector config. §29 puts `api` at step 11 and `ui/` at step 15 of 16.
- **No secret round-trip.** `Config.Redacted()` returns a marker; there is no `isSet` boolean, so a form
  cannot tell "no secret configured" from "secret configured and hidden". The practical result is that
  operators re-paste every secret on every edit, which is how secrets end up in tickets. See G7.
- **No SSE contract beyond one sentence.** "A FULL document every N events so a reconnect needs no replay"
  — with `Version` as the cursor. But `StatusStore` has `Report` and `Aggregate` and **no `Watch`**, so the
  SSE endpoint must poll `Aggregate`; the poll interval, the coalescing rule, the reconnect semantics and
  the interaction with `Complete: false` are all unstated.
- **No UI delivery.** `ui/` is TypeScript, browser-only (R11). Nothing says how it reaches a browser: no
  `go:embed` of built assets, no static handler, no dev-proxy story. The abandoned attempt's only
  production-ingress definition lived in a frontend dev proxy — R11's own evidence — and this effort has
  not closed that hole, it has just not opened it yet.

**Deferring forces a breaking change** on the parts that touch the read model (below), and is otherwise
free — the write API can be designed at step 11 without invalidating anything. The two things worth
deciding now are the ones that reach backwards into types that already exist: `Impact` (G3), and `isSet`
plus the secret reference format (G7).

## G5. The read model does not survive the effort's own scale targets

`telemetry.PipelineStatus` inlines `Lanes []LaneStatus`, `Nodes`, `Buffers`, `Workers` and `RecentEvents`
as complete slices. `LaneStatus` is ~30 fields. There is no pagination, no filter, no projection, no
per-stream or per-group rollup, and no cap.

Against the effort's own stated targets:

- The multi-stream probe is **900 runtime-discovered streams**; the parallel-scan probe is **32-way
  chunking**. Those compose: a 900-table snapshot at 32 chunks is ~29,000 lanes in one pipeline, hence a
  single JSON document in the tens of megabytes, re-serialised on every SSE tick.
- The enterprise probe is **400 pipelines × 40 pods**. `StatusStore.Report(ctx, WorkerID,
  telemetry.PipelineStatus)` sends a whole document per worker per pipeline, and `Aggregate` merges 40 of
  them per read. That is 16,000 documents in flight, each of unbounded size, and the aggregation is
  O(workers × lanes) per status read.
- `ScanProgress` is the right rollup and is the *only* one: `{LanesTotal, LanesFinished, Fraction, ETA}`.
  There is no equivalent for streams, groups or nodes, so a UI wanting "which of my 900 tables is behind"
  must download every lane.

`Complete`/`Missing` are the honest-partial mechanism and they presuppose the aggregator hears from every
worker — which is the assumption most likely to fail at 40 pods, and there is no bound on how stale a
worker's last report may be before it counts as missing (`WorkerStatus.LastHeard` exists; no threshold is
stated).

**Deferring forces a breaking change.** `PipelineStatus` is a pinned wire contract with golden fixtures
(§16, §21d) and an ETag/SSE protocol built on `Version`. Splitting it later changes the document, the SSE
stream and `StatusStore`'s interface simultaneously.

**Minimum decision now.** Do not build pagination; decide the *shape* so it can be added without breaking:

1. Declare `PipelineStatus` a **summary** document: pipeline-level facts, conditions, `Negotiated`,
   `Throughput`, `Scan`, `LastFault`, node summaries, **and a bounded, explicitly-truncated lane sample**
   with `LanesTotal` plus a `LanesTruncated bool`. Per-lane detail moves to a separate paginated resource
   from day one, even if v1's implementation returns everything.
2. Add the rollup that scales: per-stream and per-group aggregates (`Behind`, `Blocked`, `Finished` counts,
   worst `CheckpointAge`) so the common UI question is answerable without the lane list.
3. Declare the read model **additive-only**, with the same four-part discipline ADR 0020 gives persisted
   state. It is a wire contract with fixtures; it deserves the same rule and does not have one.
4. State the staleness threshold that makes a worker `Missing`, and put it in the document.

---

# Significant

## G6. The conformance kit cannot test what it lists, and nothing can chaos-test a third-party connector

Constraint #4 says extensibility is the primary success criterion and §24.1 says correctly that *"a
framework-owned conformance suite is the only thing that makes the claim checkable."* ADR 0023 accepts it.
There is no code — expected at this stage — but there are two structural problems that code will hit
immediately.

**It cannot import what it needs.** §3 declares `connector/conformance` imports `connector, record, fault,
schema, config` — no `engine`, no `store`. `TestDependencyDirection` is specified to fail any edge not in
that table. But the case list includes `Resume_MidScan`, `Resume_MidStream`,
`Resume_AcrossFormatVersion` ("write with build N, read with N+1, write, read with N"),
`Write_DurableOnCleanReturn` ("`kill -9` after a clean return loses nothing") and `Faults_RetrySafety`
("a fault-injected timeout is reported `Indeterminate`"). None is assertable without running the engine,
a real `StateStore`, and a fault injector. Two ways out, and the effort has chosen neither: give the kit a
miniature private engine — which is R10 scaffolding standing in for the product, the exact failure R10 was
written about — or let it import `internal/engine` and `store/single` and amend §3. The second is correct
and costs one line, but it must be *decided*, because the dependency test is specified to forbid it.

**Nothing lets a third party chaos-test.** The chaos suite is `tests/chaos`, engine-level, in-tree, and
described as a graduation criterion for canal — not as something shipped. So a third-party connector
author, whose crash-resume behaviour is exactly what canal cannot verify by inspection, has no way to
exercise it. Kafka Connect and Airbyte both ship an acceptance suite; neither ships a crash harness, and
canal's whole pitch here is that crash-resume is the property that matters.

**There is not one fuzz target in the effort.** `grep -rn "func Fuzz"` over `pkg/` and `internal/` returns
nothing. The adversarial byte-parsing surface is large and load-bearing: `record.Position` bytes,
`record.Blob` decode for every one of the five blob kinds, the WAL segment reader (length-prefixed +
CRC32C, with `CanDecode` gating), the deframers, and `config` validation of arbitrary operator YAML. ADR
0020's "never reject state whose version is greater" plus "a connector may declare its own blob unreadable"
is precisely a decode contract that wants fuzzing.

**Deferring is free on the fuzz targets and forces rework on the kit's API.** If the kit ships without a
harness, its `Driver` shape becomes the published contract for third parties and adding crash-resume later
changes it.

**Minimum decision now.**
1. Amend §3: `connector/conformance` may import `internal/engine` and `store/single`. Add the exemption to
   the dependency table so the test encodes the decision rather than contradicting it.
2. Specify the harness in the kit's own API before the kit exists: `conformance.Harness` with
   `Kill()` / `Restart()` / `Advance(d)`, and a `FaultInjector` seam the driver can arm. Three method
   names, decided now, keep `Driver` stable later.
3. Name the fuzz targets in §24 so they are owned: position decode, each blob kind, WAL segment, each
   deframer, config validate.

## G7. Secrets: one boolean where the effort's own research specified five rules

What exists is good as far as it goes: `config.Field.Secret` drives redaction in one place, with a comment
correctly diagnosing why per-call-site redaction failed in Conduit; `Config.Secret(path)` is a distinct,
greppable accessor; `Config.Redacted()` is the only form that leaves the process; ADR 0017 adds a
round-trip fixture asserting the value never appears.

What is missing, all of it recommended `[C]` in `docs/research/observability-controlplane.md` §7.6 and
carried into neither the architecture nor an ADR:

- **The stored spec holds the resolved value.** `spec.Node.Config` is `map[string]any` and it is what
  `ConfigStore.Put` persists. Nothing says the stored config must hold an *indirection*
  (`${env:...}`, `${file:...}`, `${vault:...}`) rather than the secret. §20 pushes "a `secret://` reference
  resolver" out of core as "a root-config plugin" and defines no interface, no syntax, and no resolution
  point. The default outcome is plaintext secrets at rest in the control-plane store — the one thing the
  research called out as the property Kafka Connect's `ConfigProvider` gets right.
- **No `Secret` type.** `Config.Secret()` returns `(string, error)`. The research specified a type with
  `String()`, `GoString()` and `MarshalJSON()` all returning `"***"` and a single greppable `Reveal()`.
  Without it, "a secret never reaches a log field, an event `Detail`, a diagnostic message or a metric
  label" is per-call-site discipline again — the failure mode `Field.Secret`'s own comment says discipline
  cannot prevent.
- **No `isSet`.** See G4: a form cannot distinguish unset from hidden.
- **Rotation is now a promise with no mechanism.** The in-flight connector edits added
  `SourceRuntime.Config(ctx) (*config.Config, error)` and `SinkRuntime.Config(ctx)`, documented as
  re-rendering the node's configuration *"with secret references resolved AS OF NOW"*, explicitly to fix
  the enterprise probe's rotating-secret finding. That is the right shape — credential freshness without
  live reconfiguration — and **there is no such thing as a secret reference.** `pkg/config` has no
  reference type, no syntax, no resolver interface and no re-resolution path; `Config.Secret()` returns a
  plain `string` from an already-resolved tree. Two runtime methods now depend on a mechanism that does not
  exist, which makes this the most urgent item in G7 rather than the least. The research's remaining answer —
  the resolver has `Watch`, and a rotation triggers a component **restart** rather than a silent in-place
  swap — is one case of the reload story (G3) and should be decided with it.

**Deferring forces a breaking change**: the reference syntax and the stored-config format are durable,
operator-visible data, and `Secret` as a type changes every extractor signature.

**Minimum decision now.** Adopt three of the five: the `Secret` value type with redacting marshalers; the
rule that the stored spec holds an indirection and resolution happens as late as possible in the worker;
and `isSet` in the API. Declare the resolver interface (`Resolve(ctx, ref) (Secret, error)` plus optional
`Watch`) even if only `env` and `file` ship. Rotation-triggers-restart falls out of G3's `Impact` enum.

## G8. No dead-letter envelope, therefore no replay path, and no poison-record floor

ADR 0012 is a good decision: dead-lettering is an edge to an ordinary sink, so the operator's own warehouse
is the DLQ store and canal does not grow a queryable archive. The consequence the ADR does not follow
through: **the thing written into that warehouse is a durable external data format, and it is defined
nowhere.**

`spec.EdgeFailed`'s doc comment says failed records are carried *"with their fault, provenance, attempt
count and the redacted config revision attached ... in an ENVELOPE, never re-nested inside a payload
field"*. There is no envelope type, no reserved `Meta` namespace for it, no schema, and no test. `Record`
carries an attached fault as a bare `error` behind `Failed() (error, bool)` (§3, deliberately, to break the
`fault`↔`record` cycle) — so the fault's nine wire fields must be projected into *something* before an
encoder can serialise them, and nothing names that projection.

Two consequences:

- **Every DLQ sink invents its own shape**, which is the M×N explosion the codec design exists to prevent,
  one layer down.
- **Replay is impossible.** "Replay the last hour is not a feature ... re-reading is done by editing the
  offset" (§20/ADR 0011) is a defensible answer for *positional* replay. It is not an answer for
  *dead-letter* replay, which is the common operational request: fix the mapping bug, re-inject the 4,000
  rejected rows. Doing that requires reading the DLQ back and reconstructing records — impossible without a
  defined round-trip.

Also unaddressed: **crash-loop poison**. Retry attempts live in memory (`FaultInfo.Attempts` is a read-model
field). A record that panics the sandbox is classified `PermanentInternal` and stops the pipeline; on
restart the same record is re-read and re-panics. Nothing persists a per-record or per-position attempt
count, so the standard poison floor ("after N restarts at the same position, dead-letter and advance") does
not exist.

**Deferring forces a breaking change** on the envelope: it is written into customer tables, and changing
it later breaks their schemas and any tooling built against it. The poison floor is **free to defer** —
ADR 0020's additive-only contract means an attempt counter can be added to the checkpoint later.

**Minimum decision now.** Define the dead-letter envelope as a versioned type with a stated field set
(original payload as bytes plus content type, `Origin`, the fault's nine wire fields, attempt count, node,
lane, position label, config revision, timestamp) and one rule: **it round-trips** — a source reading the
envelope back can reconstruct a record whose `Origin.Key` and payload equal the original. Assert the
round-trip in the conformance kit. That one rule is what makes DLQ replay a later feature rather than a
later impossibility.

## G9. Pipeline-wide policy where fan-out and fan-in need per-node — flagged, still open

Recorded here for completeness because it is a *goal-level* gap, not just a probe finding: "possibly
fan-out/fan-in" is in the end-state goals, and `spec.Spec` cannot express either at policy level.

`Spec.Guarantee`, `Streams`, `Parallelism`, `Drift` and `LaneBudget` are single pipeline-wide values;
`Retry` and `WhenFull` are the only two a node can override (via stage-standard fields).
`telemetry.Negotiated` carries one `Guarantee`, one `AckPoint`, one `DurabilityEdge`. The fan-out probe
recorded this as F1, F2, F10, F11, and the one escape hatch — `Downgrade`, the only type in the design
carrying a `NodeID` — has no field to live in (F9, and G2 above). `StreamConfig.Stream` has no node scope,
so with two source nodes a read mode requested from source A is validated against both.

The in-flight edits make the scoping gap load-bearing rather than latent: `SourceRuntime.Streams()` and
`SinkRuntime.Streams()` now return `[]ConfiguredStream` *per node*, and neither `spec.StreamConfig` nor
`connector.ConfiguredStream` carries a node, so with two source nodes announcing a stream of the same name
the engine has no basis on which to filter one node's selection from the other's. A `Node record.NodeID`
on `StreamConfig` is the whole fix and it is additive today.

**Deferring forces a breaking change**: `spec.Spec` is persisted and `Negotiated` is a pinned wire
contract; moving `Guarantee` to the node changes both.

**Minimum decision now.** Decide the *scope rule* even if v1 refuses multi-sink-tier graphs at submit time:
per-node `Guarantee` override with the pipeline value as default; `StreamConfig` gains a `Node`; and
`Negotiated` becomes per-node with a pipeline-level `min` rollup. Refusing the configuration today is fine.
Shipping a contract that cannot express it is not.

## G10. The metrics SDK, OTLP export and tracing were researched and never decided

`architecture.md` §16.1 pins a hand-rolled Prometheus name set with a golden-file test against real
`/metrics` output. That is R8 done properly. What is missing is the decision *underneath* it, which the
effort's own research made explicitly (`observability-controlplane.md` §11.2, marked `[C]`): instrument
through the OpenTelemetry Go metric API, export Prometheus in standalone mode, additionally allow OTLP push
in enterprise mode, traces OTLP-only and off by default, exemplars on latency histograms — *and decide the
naming strategy once, because the OTel Prometheus exporter's output differs cosmetically (`_total`
suffixing, `otel_scope_*` labels, `target_info`) from hand-rolled `client_golang`.*

Nothing about this appears in the architecture or an ADR. Consequences: `connector.Metrics` is a
canal-shaped `Counter/Gauge/Histogram` facade with no stated relationship to either SDK; there is no
`Tracer()` on any runtime; there is no span or trace-context propagation, which the out-of-process seam
(§19, a first-class future) will need across the boundary and which `Fault`'s wire form does not carry; and
there is no exemplar story, so the read model cannot link a slow p99 to anything.

**Deferring forces a breaking change**, and this is the clearest cheap-now/expensive-later item in the
audit: once `/metrics` is pinned by a golden file and operators build dashboards, swapping the SDK changes
the output. The research says so in as many words.

**Minimum decision now.** Pick the SDK and write the naming strategy into §16.1 as normative, with the
golden file asserting the chosen form. Add `Tracer()` to the `*Runtime` interfaces (they are the declared
growth path, so this is free later only if the *decision* to have tracing at all is made now — a fault's
wire form and the remote seam both need a trace context field, and adding one to a persisted/wire fault
later is not free).

## G11. Store migration, rolling upgrade and the k8s deployment artifact

ADR 0020 covers *payload* compatibility with unusual thoroughness — four rules, upgrade tests, downgrade
survivability, `state_written_by_newer_build` as a degraded condition, `StateAdopter` for renames. Nothing
covers the *stores* or the *deployment*:

- **No store schema versioning or migration.** `store/postgres` needs DDL, a migration mechanism, and a
  version marker; `store/single` (bbolt) needs bucket versioning. Neither is mentioned. "Reproducing that
  in 2026 with a transactional database available would be perverse" is the right instinct about Kafka as a
  coordination store — and a transactional database comes with schema migration as a first-class problem.
- **No mixed-version cluster statement.** Rolling upgrades mean binary N and N+1 both claiming lanes from
  the same Postgres, both writing checkpoints, both reading each other's. ADR 0020 makes the *bytes*
  survivable; nothing states which binary versions may coexist, whether `Caps.APIVersion` gates
  participation, or how a mid-upgrade planner disagreement resolves.
- **No deployment artifact.** No Dockerfile, no Helm chart, no k8s manifest or CRD shape (the CRD is named
  as a "conformance target" and never sketched), no `terminationGracePeriodSeconds` relationship to
  `Deps.GracePeriod` (a mismatch turns every rolling restart into a DRAIN-TIMEOUT, which by canal's own
  definition means records may replay), no PodDisruptionBudget guidance, and **no autoscaling signal** —
  the lease-per-lane design makes adding a pod safe, and nothing says what tells you to.

"Deployable at enterprise scale (horizontal, k8s, multi-worker, coordinated)" is a stated end-state goal.
The coordination *protocol* is designed to an unusually high standard; the deployment *product* is absent.

**Mostly free to defer**, with one exception worth a sentence now: state the grace-period contract
(`GracePeriod` < `terminationGracePeriodSeconds`, and what canal does when it is not), because that one is
a data-duplication path dressed as a config default.

## G12. Nothing hosts more than one pipeline

`engine.Build` returns one `*Pipeline` with `Run` and `Close`. There is no supervisor, manager, or worker
type; nothing consumes `ConfigStore.Watch`; `Reconcile`'s §23.4 signature has no types and no
implementation. At the stated 400-pipelines-per-fleet scale the unaddressed questions are all
placement-and-isolation questions:

- **No placement model.** `store.LaneRow` and `Coordinator.Claim(AssignmentID, WorkerID, ttl)` have no
  pool, selector, or affinity, so every worker is eligible for every lane of every pipeline in the tenant.
  There is no way to say "this pipeline's lanes run on the memory-heavy pool" or "this tenant's lanes never
  share a pod with that tenant's".
- **No blast-radius bound.** `engine.sandbox` contains panics and abandons hangs — genuinely good — and
  cannot contain memory. One connector's unbounded buffer OOMs a pod hosting hundreds of pipelines, and
  every lane on that pod becomes a lease expiry and a reassignment storm-adjacent event (delayed 120s,
  which helps).
- **No fairness, no quotas.** No per-tenant or per-pipeline limits on lanes, goroutines, memory, or
  connection pools; nothing schedules between pipelines competing for one worker.
- **Disk-buffer affinity interacts.** §17 states a node-durable buffer makes a lane non-reassignable until
  it drains. With no placement model, "which pod has that lane's directory" is unanswerable.

**Partly forces a breaking change**: a placement selector belongs on `store.LaneRow` and in the `Claim`
query (`SELECT … FOR UPDATE SKIP LOCKED` with a predicate). Both are durable and protocol-level.

**Minimum decision now.** Add an opaque `Pool string` to `LaneRow` and a pool filter to `Claim`, defaulting
to `"default"`. Nothing else. It is one column and one `WHERE` clause today, and it is the difference
between adding placement later and re-planning the assignment protocol later.

## G13. The audit trail has no home

ADR 0017 rule 7: *"Every mutating call is audited with actor, tenant, generation, and a diff of the
redacted spec. Offset edits are audited and bump `Generation`."* §13.3 repeats it for offset edits. There
is no audit record type, no store (four stores, none of which is one, and §17 forbids a fifth), no
retention policy, and no read API. `telemetry.Event` is the read model's bounded in-process ring, which is
the wrong lifetime for an audit trail by construction.

So the tenancy decision — one of the nine must-close items — currently rests in part on a mechanism that
does not exist anywhere in the effort.

**Free to defer** as a feature; **not free to leave undecided**, because the answer determines whether §17's
"four interfaces" claim survives. **Minimum decision now:** pick one of (a) audit is a structured log stream
with a declared field set and retention is the operator's log pipeline's problem — cheapest, and consistent
with ADR 0012's "your warehouse is the DLQ store" reasoning; (b) audit is a reserved `StateStore` space
with a documented growth bound. Say which, in ADR 0017.

---

# Minor

## G14. i18n is asserted throughout and designed nowhere

R9 and §2 repeat that every closed enum's `String()` token is simultaneously the wire form, the metric
label value and *"the i18n key suffix"*; `config.Code`, `telemetry` reasons and `fault.Class` all carry the
claim. There is no key namespace, no catalogue format, no lookup, no pluralisation rule, and no ownership.

More to the point, the strings an operator actually reads are not enum tokens: `Condition.Message`,
`Fault.User`, `Diagnostic.Message` and `.Hint`, `Position.Label`, `LaneSpec.Label`, `Event.Message` and
`.Detail` are free prose, several of them authored by connectors. Those are structurally untranslatable, so
the honest position is that the *tokens* are localisable and the *messages* are English.

**Free to defer. Minimum decision now:** one sentence in §2 scoping the i18n claim to enum tokens and
diagnostic codes, so nobody later builds a catalogue for prose that connectors write.

## G15. Two schema identity spaces, undeclared

`connector.SchemaLookup` (`Get(ref)`, `Register(schema) → ref`) is canal's own fingerprint-keyed table in
`StateStore` under `SpaceSchema`, committed atomically with the cursor via `Checkpoint.SchemaEpoch`. That
is a good design. An external schema registry — Confluent-style subject/version/global-id, with
compatibility checking and a magic-byte wire prefix — is entirely a codec's private business: its HTTP
client, caching, failure classification and unavailability behaviour sit outside the fault model, the retry
policy and the capability system. A codec therefore maintains a second schema identity space with no seam,
and `CodecCaps.NeedsRuntime` is the only signal that any of this is happening.

Defensible as a deliberate exclusion (§20 excludes secrets backends on the same reasoning). **Free to
defer. Minimum decision now:** add a row to §20's exclusion table naming it, so it is a choice rather than
an oversight.

## G16. Read-model event history dies with the process

`PipelineStatus.RecentEvents` is a bounded ring, reported through `StatusStore` and aggregated. Nothing
persists events. A crash or a rolling restart erases the "what just happened" surface precisely when an
operator needs it — and the drift, downgrade, committable-expiry and drain-timeout events are the ones the
design leans on most for honesty. **Free to defer; disclose it** in §16 so the UI does not present the ring
as a history.

## G17. No performance or capacity engineering anywhere

The design makes strong performance claims — the `Allocator` holding constant fields so only varying ones
are per-record, `[]*Record` to avoid slab reallocation, bytes-first-class so a pass-through never
materialises structure, per-batch settlement to avoid "hundreds of locked map ops", `Origin` at ~150 bytes
accepted explicitly as a cost. There is no benchmark suite (`grep benchmark` finds one passing mention, for
the sandbox goroutine), no throughput or latency target, no allocation budget, and no sizing model (lanes
per worker, memory per lane, goroutines per pipeline) despite very specific scale targets. §29's
implementation order has no performance gate; nothing would catch a 10× regression.

**Free to defer. Minimum decision now:** name the benchmark targets in §29 alongside the chaos gate, so
milestone one has a number attached to it rather than only a correctness test.

---

# The nine must-close items

All nine have an ADR, all are marked `accepted, normative`, and all nine ADRs carry the four required
sections (Context / Decision / Alternatives rejected / Consequences). Spot-checking the one most likely to
be thin — 0020, state format compatibility — found the opposite: eight numbered rules including the
downgrade-survivability rule every reviewed proposal omitted, a mandatory N↔N+1 upgrade test, one snapshot
format, a loud-failure escape hatch, and `StateAdopter` for renames. This is genuinely closed work and the
effort deserves credit for it.

Two carry a caveat that this audit surfaces:

| # | Item | Status |
|---|---|---|
| 1 | Buffer durability model / WAL sharing | Closed. ADR 0001, §18.3 |
| 2 | Dedupe key scope and store | Closed. ADR 0002, §13.4, `store.DedupeKey` in code |
| 3 | Backpressure signalling | Closed. ADR 0003, §18.2 |
| 4 | Partial-failure response shape | Closed. ADR 0004, §8 |
| 5 | Canonical record model | Closed. ADR 0005, §4, and the most complete part of the codebase |
| 6 | Tenancy and authentication | Closed **as a decision**; rule 7 (audit every mutation) has no mechanism anywhere — see G13 |
| 7 | Connector state machine + last-error surface | Closed **on the reporting side** (ADR 0019, `telemetry`). `PhasePaused` and the legacy `paused` state have no durable trigger and no API, so one member of the state machine is unreachable — see G2 |
| 8 | Clock-skew policy | Closed. ADR 0018, `spec.ClockPolicy` in code |
| 9 | Checkpoint format compatibility | Closed, thoroughly. ADR 0020 |

Neither caveat is a missing decision; both are missing mechanisms for a decision already taken. They belong
in G2 and G13 rather than in a re-opened ADR.

---

# What to decide now, in priority order

Everything below is small. That is the point: each is a door that costs one field, one rule or one sentence
today and a migration later.

1. **`config.DecodeRef` + `Fields.Decode` + `SourceCaps.Structured`**, appended to `KindSource`, plus the
   rule that a byte source does not parse its own bytes. (G1)
2. **Where operator intent lives** — `spec.Intent{DesiredState, Downgrades, Resets}` — and the rule that an
   intent change bumps `Revision`, not `Generation`. Move `Downgrade` out of `telemetry`. (G2)
3. **`Node.ID` is immutable**, `ConfigStore.Put` refuses an orphaning edit, and an `Impact` enum
   (`none|reload|restart|state_reset`) on the validate response and on `config.Field`. (G3)
4. **`PipelineStatus` is a summary document**, additive-only, with per-stream/per-group rollups and an
   explicitly truncated lane sample; per-lane detail is a separate resource. (G5)
5. **The dead-letter envelope**, versioned, with a round-trip rule asserted in the conformance kit. (G8)
6. **The conformance kit may import the engine and `store/single`**, and its `Harness`/`FaultInjector`
   surface is named before the kit is written. (G6)
7. **The `Secret` value type, the indirection-at-rest rule, and `isSet`.** (G7)
8. **The metrics SDK and the naming strategy**, plus whether tracing exists at all (it determines a field
   on the fault wire form and on the remote seam). (G10)
9. **`LaneRow.Pool` + a pool filter on `Claim`**, defaulted. One column, one `WHERE`. (G12)
10. **Per-node `Guarantee` scope rule**, even if multi-tier fan-out is refused at submit time in v1. (G9)

Safe to defer with no door closing: the write API surface and its error envelope (once `Impact` and `isSet`
exist), the UI itself and its delivery mechanism, Helm/Dockerfile/CRD, store DDL migration tooling, the
poison-record floor (additive to the checkpoint), audit implementation (once its home is named), i18n
catalogue, external schema-registry seam, event persistence, quotas and fairness, and the benchmark suite.

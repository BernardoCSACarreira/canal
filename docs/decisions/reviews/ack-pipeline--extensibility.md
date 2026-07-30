# Review: "the acknowledgement pipeline" — EXTENSIBILITY lens

**Reviewer role:** hostile expert, single lens. Judged only on canal's primary success criterion:
*adding a source or sink means "implement the interface, register it, done" — zero core edits, zero switch
statements, zero core knowledge of the connector* — extended, per the brief, to adding a **third kind of
thing** (transform, buffer, codec, pipeline type), to absorbing an **out-of-process implementation** later,
to **testing a connector in isolation**, and to **interface versioning** when the core needs more in v2.

**Score: 6 / 10.**

---

## The verdict, and why it is 6 and not 8 or 4

For the two roles constraint #4 literally names — a source and a sink — this proposal delivers. `Source` is
four methods, `Sink` is three, both frozen, both `(ctx, serialisable) → error`, and the walkthrough in §11(a)
is a complete, credible, ~60-line source and ~12-line sink with no core edit anywhere. The optional-interface
+ declared-`Caps` + single-`Resolve`-site triad is the best answer to D9 in any of the surveyed prior art, and
`ResolvedSource`/`ResolvedSink` (§4.12) is a genuinely new idea: collapsing a type-asserted capability set into
a plain struct of nilable handles *once* is simultaneously the fix for Benthos's nine-forwarder tax and the fix
for "a browser cannot type-assert". §10's five-import rule for connector packages, enforced by a dependency
test, is the strongest structural guarantee in the document — the core's types are not reachable from a
connector, so a core `switch` on connector identity has nowhere to attach.

That is the case for a high score, and it is real.

The case against is that the proposal's extensibility claims are load-bearing in **three** places beyond
source-and-sink, and all three fail as written:

1. **Composition.** Every topology feature — fan-out, fan-in, fallback, retry-wrapping, DLQ routing, buffer
   chaining — is delegated to "components containing components, resolved from config" (§8, §8.1). There is no
   accessor by which a component obtains its children, and the package layout makes one impossible without
   inverting `config` ↔ `connector`. Two of the thirteen advertised composite extractors already form that
   cycle literally. The mechanism that carries the entire topology story does not exist and cannot be added
   without moving types between packages.
2. **Versioning.** `SourceCaps`, `SinkCaps` and `BufferCaps` carry no API version and no unknown-capability
   handling, and the registration rule *panics* when a connector satisfies an optional interface it did not
   declare. The document's answer to "the core needs more in v2" is a growth rule — new fields on core structs
   — that is source-compatible but semantically undetectable: a v1 connector silently ignores a new input
   field and the core cannot tell. This is `ErrBackOff`-honoured-only-on-`Connect` reintroduced one level down,
   and it is decision-space trap #9 and #11 landed on simultaneously.
3. **A third kind of thing.** `registry.Kind` (9 members), `config.Kind` (a duplicate of it), a `AddX`/`X()`
   pair per kind, `Walk(..., caps any)`, `With(defs ...any)`, and a **named slot per stage on `engine.Spec`**
   which `ConfigStore` persists and "the API accepts". Adding a stage kind is a seven-site edit across three
   packages plus a persisted-schema change. R1 says a fixed stage count is a design smell; §8's disclaimer that
   the shape is "an IMPLEMENTATION FACT, never an enumerated stage list in any API" is contradicted by
   `engine.Spec`'s own doc comment two paragraphs later.

None of this is fatal to the *angle*. The ack-pipeline thesis is orthogonal to all three failures, and the
ledger's per-lane tracker, `Position.Label`/`Safe`, `Tracker.Budget()`-as-replay-window and `Negotiated` are
transplantable into any of the competing proposals. But as a candidate *interface set*, this one has one
structural blocker (composition), one undischarged obligation (versioning), one accounting hole that makes a
second sink a core-interface change (`Settle` has no branch dimension), and three literal import cycles. 6.

---

## Defects

### D1 — FATAL. A component cannot obtain its child components, and the two most important composite extractors form an import cycle

**At fault:** `config.Config`'s accessor set (§3.1) plus `func (c *Config) Batching(path ...string)
(BatchPolicy, error)` and `func (c *Config) Codec(path ...string) (CodecChain, error)` (§3.2), against §10's
declared direction `config` imports `fault` only, and `connector` imports `config`.

`BatchPolicy` is defined in `connector` (§4.14) and contains a `*config.Predicate`. `CodecChain` is defined in
`connector` (§4.9). Both are returned by methods on `config.Config`. Therefore `config` must import
`connector`, which imports `config`. **This does not compile.** It is not a naming slip: it is the mechanism
the proposal calls "the single most transplantable idea in Benthos's config model" (§3.2), and it means every
future composite field spec whose extracted type is a core type deepens the cycle. Adding a `Fields.RateLimit`
extractor returning a `connector.RateLimiter`, or `Fields.Buffer` returning a `connector.Buffer`, has the same
problem.

The larger consequence is the one that matters for this lens. `config.FieldType` includes `TypeComponent`
("a nested registered component") and `config.Field.ComponentKinds []Kind` says which registries may satisfy
it. §8 states that *all* topological variety comes from components containing components — "a broker sink
whose config holds N sinks", `fallback`, `retry`, `dlq`, and buffer chaining via `WhenFullOverflow`, all
"ordinary registered components... with no privileges" (§8.1). A broker sink is constructed by
`SinkDef.New(ctx, cfg *config.Config)`. **There is no accessor on `*Config` that returns a constructed child
component**, and there cannot be one: it would have to return `connector.Sink`, and `config` cannot see
`connector`. It also could not resolve a name against a `*registry.Registry`, because `registry` imports
`config`.

So the concrete failure: to add fan-out, someone must either (a) move `Sink`/`Buffer`/`Transform` into
`config`, (b) add a `Children()` handle to `SinkRuntime` and give up on children being available at
construction — which breaks `Build`'s promise that capability negotiation is "a PURE FUNCTION OF CONFIG,
before anything starts", because a broker's negotiated guarantee is `min` over children it cannot yet see, or
(c) give brokers a privilege the design says they do not have. Every route is a core edit to a package every
connector imports. Fan-out is in canal's end-state goals; this is not a hypothetical.

### D2 — FATAL. No API-version or unknown-capability handshake, and undeclared-interface-satisfaction panics

**At fault:** `SourceCaps`, `SinkCaps`, `BufferCaps` (§4.11); `Registry.AddSource`'s stated behaviour
(§7): *"Declaring `Caps.Discoverable` without implementing `Discoverer` PANICS at registration. Implementing
`Discoverer` without declaring it panics too."*

The decision space is explicit (D9, and trap #9): *"Include an explicit `(api_version, capabilities)`
handshake at init. Benthos's gRPC boundary has none, so an older plugin cannot know it is missing a required
semantic."* Its illustrative `Capabilities` struct carries `APIVersion int`. **None of the three `Caps` structs
here has a version field, and nothing in the document says what a core does with a capability flag it does not
recognise.** For the in-process case this is masked by compilation. For the out-of-process case the proposal
advertises — the whole point of `Caps` being data rather than assertions — it is unresolved: a v2 plugin
reports `Chunkable: true` to a v1 host over a wire, and the v1 host has no field to put it in and no rule
saying whether to refuse the plugin or ignore the flag. The one place the design *needed* the versioning
discipline it applies everywhere else (`Position.TokenCodec`, `LaneSpec.ResumeCodec`, `Change.Version`,
`SchemaRef.Fingerprint`, the WAL's `CanDecode(version)` predicate) is the capability handshake, and it is
missing there and only there.

Worse is the panic-on-undeclared rule. Go's interface satisfaction is structural, so whether a connector
satisfies an interface that *does not yet exist* is not under its author's control. The moment canal v2 adds
an optional interface whose method set is narrow — `Heartbeater` is one method, `Prober` is one method,
`Nackable` is one method — every already-shipped connector that happens to have a same-shaped method is
retroactively unregistrable. The rule's motivation is sound ("a silent capability is a capability the UI
cannot see"), but the enforcement direction should be a registration *diagnostic*, not a panic, and the
proposal's own defence — "Benthos left one of its optional interfaces unexported, so nobody could implement
it" — argues for exporting, not for panicking.

Net answer to the lens's versioning question: **the growth rule ("new capabilities are fields on core structs,
never methods on connector interfaces", §1) is source-compatible and semantically unsafe.** Adding
`Request.Deadline`, `Ack.Epoch` or `LaneAssignment.Fenced` compiles against every v1 connector and is silently
unhonoured by all of them, with no `Caps` bit and no version by which the core could refuse. That is the exact
failure mode the `fault.Class` closed set was designed to prevent one layer up.

### D3 — MAJOR. `Ledger.Settle` is keyed on `RecordID` alone, so fan-out and durable buffers double-settle; adding a second sink is a core-interface change

**At fault:** `Ledger.Settle(id record.RecordID, d Disposition, f *fault.Fault)` and
`Ledger.Fanout(g record.GroupID, n int) error` (§5.2), against `Record.Copy()` — *"a deep copy carrying the
same Origin and the same RecordID"* (§1.4) — and `record.Ref{ID, Group, Lane, Stream, Key}` (§1.4).

`Fanout` accounts at **group** granularity (`+n-1` outstanding references). `Settle` accounts at **record**
granularity. Fan-out to three sinks produces three copies with the *same* `RecordID`, and each child sink
returns nil, so the engine calls `Settle(1, Delivered, nil)` three times for three distinct deliveries that
are indistinguishable in the only key the API accepts. Two interleavings, both broken:

- If `Settle` is idempotent per `RecordID` (which it must be, given `Resolve` is documented idempotent and
  retries re-deliver the same ids), sink A's success settles the record and decrements the group once. Sinks B
  and C never register. The group's refcount, inflated by `Fanout(g, 3)`, never reaches zero. Every fan-out
  group leaks until `GroupTTL`, and the lane's prefix never advances: **fan-out deadlocks the pipeline.**
- If `Settle` is not idempotent, then a retry that re-delivers records 1..500 after a partial failure settles
  already-settled records a second time, the refcount reaches zero early, `Resolve` fires, and the source
  commits a position whose records are still in flight. **Premature commit — R4's exact violation, by
  arithmetic.**

`record.Ref` — the *only* thing a sink is given to report per-record outcomes with, and the key of
`fault.BatchError.Fail` — has no branch or attempt dimension either, so a `BatchError` from sink B cannot be
told from one from sink A. The fix requires a branch identity on `Settle`, on `Ref`, and on `BatchError`, i.e.
changing three core types that every sink touches. §8.1's claim that "fan-out correctness is refcounting" and
§5.2's "no core code path per topology" are therefore unearned: **the second sink is a core-interface change.**

The durable-buffer path has the same shape. `BufferCaps.Durable` means "the *core* settles the group on a
successful `Put`" (§4.8) — and then the same records continue downstream, the sink returns nil, and the engine
settles them again. And on restart, a WAL holding records for a lane whose committed watermark has already
advanced past them must re-enter the ledger, but `Ledger.Admit` is the source-side entry point that stamps
`Position.Seq` per lane; there is no second admission path, so replaying a durable buffer after a crash has no
defined group/lane identity. Adding the WAL buffer — one of the two buffers §8.1 ships — needs a new ledger
entry point.

### D4 — MAJOR. `engine.Spec` is a fixed, persisted, API-facing stage schema (R1)

**At fault:** `engine.Spec{Source, Decode, Transforms, Buffer, Encode, Sink, DLQ, ...}` (§8), described in its
own doc comment as *"what the ConfigStore holds and what the API accepts"*, against §8's opening claim that the
outer shape is *"an IMPLEMENTATION FACT, never an enumerated stage list in any API"* and against R1.

The abandoned attempt froze eight stages with `minItems: 8, maxItems: 8`. This freezes seven named slots with
cardinality baked into the field types: exactly one `Source`, at most one `Decode`, at most one `Buffer`, at
most one `Encode`, exactly one `Sink`, at most one `DLQ`, N `Transforms`. That is better than an ordinal array,
but it is still a fixed stage count in the persisted contract. Adding a stage kind — a router, a rate limiter,
a scanner (D2's `BatchScannerCreator` role, which this design has no home for), a sampler, a schema-resolution
stage — costs, minimum: a `registry.Kind` constant, a `config.Kind` constant (see D11), a `XDef` struct, a
`Registry.AddX`, a `Registry.X(name)`, an arm in `With(defs ...any)`, an arm in every `Walk` consumer's
`caps any` switch, a field on `engine.Spec`, a `ConfigStore` schema migration, and an arm in `Build`. Nine
sites, three packages, one persisted-format change. The proposal's answer to that cost is D1's
component-in-component escape hatch, which does not exist.

Note also the interaction with `WhenFullOverflow` (§4.8): buffers are documented as chaining ("a small memory
buffer in front of a large disk one"), but `Spec.Buffer` is a single `*ComponentRef`. Chaining is only
expressible through the missing composition mechanism.

### D5 — MAJOR. One `Encode *CodecRef` per pipeline makes per-sink wire formats inexpressible, which breaks the codec/connector orthogonality exactly at the composition point

**At fault:** `engine.Spec.Encode *CodecRef` (§8) and `connector.Request{Body []byte, ContentType,
ContentEncoding}` (§4.5).

The design's strongest structural claim is D2/Chain-D: sinks implement transport only, so "N codecs × M
connectors never multiplies" (§4.5: *"this is the property that makes 'add a sink: three methods, register,
done' literally true"*). Encoding happens once, in the engine, before `Sink.Write`. Correct — for one sink.

Fan-out to two sinks needing different wire formats (NDJSON to an HTTP endpoint, Avro to object storage) is
then inexpressible: the broker sink receives one `*Request` with one `Body` and must hand the identical bytes
to both children. The only workaround is a broker declaring `Caps.StructuredInput = true` and
`RequiresCodec = false`, taking records via `RecordSink`, and re-implementing encode + frame + compress +
batch + split + partition internally per child. `Batcher` and `Splitter` are exported, which softens it, but
the engine's encode stage and the broker's encode stage are then two implementations of the same pipeline
segment — the N-times duplication the whole codec decision exists to prevent — and the broker must resolve
`KindEncoder`/`KindFramer`/`KindCompressor` children, which is D1 again.

The clean fix is a codec ref *per sink slot* rather than per pipeline, which is a `Spec` change (D4) and a
change to where the engine's encode stage sits. It is cheap now and expensive after connectors exist.

### D6 — MAJOR. Codec interfaces have no `context.Context` and no runtime, so a registry-backed codec — the canonical justification for pluggable codecs — cannot be written

**At fault:** `Encoder.Encode(dst []byte, r *record.Record) ([]byte, error)`,
`Decoder.Decode(frame []byte, dst *record.Batch) error`, `Compressor.Compress(dst, src []byte) ([]byte,
error)` (§4.9). No `Open(ctx, rt)` on any of them; no `ctx` on any method.

Part 0 item 7 of the decision space is *"`context.Context` on every method that can block, from the first
commit"*, with Connect's KIP-419 and Benthos's `// TODO V5: Add context here` as the cost of omission. D2's
headline argument for codecs-as-plugins is Connect's `Converter`: the same connector output goes out "as
schemaless JSON, JSON-with-embedded-schema, or **a registry-backed Avro ID** with no connector change". A
schema-registry-backed Avro encoder must do I/O — register or fetch a schema by fingerprint, with retries and
a timeout — on first sight of a new `SchemaRef`. `Encode(dst, r) ([]byte, error)` cannot block cancellably,
cannot log, cannot register a metric (there is no `CodecRuntime`), and cannot classify its failure with a
lane or stream (it can return a `*fault.Fault`, but with no `ctx` it cannot honour `RetryAfter` on a
rate-limited registry). The listed codec set in §10 — `json, ndjson, avro, csv, protobuf, raw` — includes two
formats (avro, protobuf) that in practice need exactly this.

Consequence for the lens: adding the first I/O-bearing codec requires adding `ctx` and a runtime to
`Encoder`/`Decoder`, which breaks every codec already written. The design's own growth rule forbids that move,
so the escape would be a *second* encoder interface — `ContextEncoder` — and a resolution site, which is the
Flink Sink-API-rewritten-three-times pattern (trap #9) arriving in canal at v2 for a reason that was
foreseeable at v1.

### D7 — MAJOR. The schema subsystem is declared, validated and metered, but has no types and nowhere to live

**At fault:** `record.SchemaRef` (§1.4) — *"the schema body itself lives in the pipeline's schema table"* —
plus `connector.Schema` (referenced by `StreamDesc.Schema *Schema`), `SchemaChange`, `SchemaChangeKind`
(referenced by `SchemaApplier`), all **undefined**; `store`'s four interfaces with the stated rule *"If a
fifth appears, the abstraction is wrong"* (§9); `Request.Schema *record.SchemaRef` (§4.5); `engine.DriftPolicy`
with five modes and `Build` refusing `DriftEvolve` against an unsupporting sink (§8).

There is no interface by which a source registers a schema body and receives a fingerprint (`SourceRuntime`
has `Lanes/State/Log/Metrics/Batcher/Emit/Pipeline/Instance` and nothing schema-shaped), no store for the
"schema table", and no definition of the change vocabulary the drift policy operates over. A connector author
who reads this document cannot implement `SourceCaps.ProducesSchema` or `SinkCaps.AppliesSchema`. Wiring it
later touches `record` (or a new package), `connector` (a runtime method — additive, fine), `store` (a fifth
interface, violating §9's own rule, or an overload of `StateStore` which is documented as bytes-in/bytes-out
for source state only), and `engine`. For a proposal whose deliverable *is* the interface set, a declared
capability with no implementable mechanism is the same defect class as R2's `source_canonical_event_serializer`
— a named stage with no definition.

Separately, and cheaply fixable: **`SchemaApplier.SupportedChanges() []SchemaChangeKind` is capability-as-a-
method.** It is the one violation of the design's own stated rule ("behaviour is an optional exported
interface; the *fact* of that behaviour is declarative data in a `Caps` struct", §4). `SinkCaps` has
`AppliesSchema bool` but not the kind list, so `Build` — advertised as a pure function of config — must
construct a sink to validate `DriftEvolve`, and the UI cannot grey out an impossible drift policy without
instantiating a connector. That is decision-space trap #11 exactly. The list belongs in `SinkCaps`.

### D8 — MAJOR. `store` traffics in typed core objects, cycles with `engine`, and persists `LaneSpec` unversioned

**At fault:** `ConfigStore.Get(...) (engine.Spec, uint64, error)` / `Put(ctx, s engine.Spec, ...)`,
`Coordinator.Plan(ctx, id, lanes []connector.LaneSpec, gen uint64)`,
`StatusSink.Report(ctx, w, s telemetry.PipelineStatus)` (§9), against `engine.Deps{Config store.ConfigStore,
...}` (§8) and §10's claim of strictly downward dependencies with exactly one declared cycle-break.

`engine` imports `store` and `store` imports `engine`. **Second literal compile failure.** More importantly
for this lens: three of the four seam interfaces carry typed domain objects, so the seam is not the
bytes-in/bytes-out boundary the document praises Connect's `OffsetBackingStore` for being. Only `StateStore`
is bytes. Consequences:

- Every `Coordinator` implementation (`singlenode`, `postgres`, and the etcd conformance target §9 promises)
  must serialise `connector.LaneSpec` itself, in its own encoding. Three implementations, three
  serialisations, guaranteed drift — the R8 failure mode.
- `LaneSpec` is durable state with **no format version**, while its own `Resume []byte` field is carefully
  paired with `ResumeCodec uint16`. Adding a field to `LaneSpec` — and this design will, because `LaneSpec` is
  where per-lane knobs accumulate: `Budget` and `Weight` are already there — is an unversioned change to
  persisted rows that a mixed-version cluster reads. Decision 9 of "decisions the architecture must close" is
  "checkpoint state format compatibility across binary upgrades"; this closes it for `Token` and `Resume` and
  leaves it open for the envelope.
- `ConfigStore` returning `engine.Spec` means a new stage kind (D4) is a `ConfigStore` contract change, so the
  deployment seam is coupled to the topology schema.

### D9 — MEDIUM. `SourceRuntime` is a concrete struct with unexported state and no exported constructor, so a connector cannot be tested without the engine

**At fault:** `type SourceRuntime struct{ /* ... */ }` with method-only access (§4.13), and
`Source.Open(ctx, rt *SourceRuntime) error` (§4.3). Same for `SinkRuntime`, `TransformRuntime`,
`BufferRuntime`.

Every *individual* dependency is correctly an interface — `LaneCtl`, `StateHandle`, `Metrics` — which is the
right instinct. The aggregate is not. There is no `NewSourceRuntime(...)`, no `connector.TestRuntime`, no
functional-options constructor, and no interface a test could satisfy. `connector/conformance` is a separate
package (§10) so it cannot reach the unexported fields either. A connector author therefore cannot write
`TestOpen` without standing up an engine, which is the opposite of the isolation property this lens asks about
and it undercuts the conformance kit the document leans on in five places (`Caps` cross-checks, `Durable`
verified by `kill -9`, `Examples` parsed in CI, `canal_unclassified_faults_total == 0`,
`canal_ledger_leaks_total == 0`).

The repair is additive and cheap — an exported constructor or a `Runtime` interface — but "cheap to fix" is not
"fixed", and the choice of a struct over an interface at the *only* injection point in the design is a
deliberate-looking decision that is never argued.

### D10 — MEDIUM. `Payload.Bytes()` materialises "using the pipeline's configured encoder" from inside a package that imports stdlib only

**At fault:** `func (p *Payload) Bytes() ([]byte, error)` (§1.4), documented as *"materialising it from the
structured view if necessary using the pipeline's configured encoder"*, in package `record` whose stated
imports are `iter` and `time`.

`record` cannot see `connector.Encoder`. So either (a) the conversion is driven by a package-level settable
hook in `record` — process-global mutable codec state, which is decision-space trap #14 (Vector's
`log_schema`) in a new costume and would make two pipelines with different encoders in one binary incorrect —
or (b) `Bytes()` cannot do what its doc says, and the lazy dual view collapses: a sink calling `Bytes()` on a
structured-only payload gets an error, and every sink must handle a case the type was designed to hide.

This matters for extensibility because the dual-view payload is what lets a transform touch structure and a
sink take bytes without either knowing about the other. If the conversion is not expressible in `record`, it
has to move into the engine's encode stage, and then `Payload.Bytes()` should not exist as a method — which
changes the record model, the type every connector, codec, transform and buffer depends on.

Related and confirming: `Payload.Len() int // encoded length if known, else -1` combined with
`SinkCaps.MaxRequestBytes` and `Splitter` (§4.14) means a sink's declared hard request-size limit is
unenforceable for a structured-payload pipeline. §8's stage order is `... buffer? -> batch -> encode -> sink`,
so batching and splitting run *before* encoding and cannot know encoded size. A declared capability the engine
cannot honour given its own stage order is a capability that will be fixed by reordering stages, which is a
core change.

### D11 — MEDIUM. Two `Kind` enums for one concept, and `any`-typed registry surfaces that push a per-kind switch into every consumer

**At fault:** `config.Field.ComponentKinds []Kind` (§3, in a package that cannot import `registry`) versus
`registry.Kind` with its nine constants (§7); `Registry.Walk(fn func(k Kind, name string, spec *config.Spec,
caps any) bool)` and `Registry.With(defs ...any) (*Registry, error)` (§7).

R9: *"A function mapping between two representations of the same concept is evidence of a modelling error."*
`config` must define its own component-kind enum because `registry` imports `config`, so the two must be kept
in sync by hand and mapped at the boundary — and both grow per kind. This is the same shape as the abandoned
attempt's four tier vocabularies, caught early.

`caps any` is the more practical cost. `GET /v1/connectors` is advertised as having "no per-connector code
because there is nothing per-connector to have code about" (§7) — true, and good. But every consumer of `Walk`
must `switch caps.(type)` to serialise it, and `Build` must do the same to negotiate. That switch grows once
per *kind*, in `api/`, in `engine/`, and in any future CLI or exporter. `With(defs ...any)` is a second such
switch. Both are avoidable with a small `Def` interface (`Kind() Kind`, `CapsJSON() any`) or per-kind
`WalkSources`/`WalkSinks`, and neither is discussed.

Minor vocabulary note in the same family: the document defines `config.Spec`, `engine.Spec`, `config.Config`
and `ledger.Config`, and `registry.SourceDef.Spec` is a `*config.Spec` while `ConfigStore` stores an
`engine.Spec`. Two overloaded names across five types in a codebase whose R9 rule is one concept, one
vocabulary.

### D12 — MEDIUM. Sub-batch retry needs the records, and neither `Request` nor the reused `Batch` is documented as retaining them

**At fault:** `Request{Body []byte, Records []record.Ref, ...}` (§4.5), `record.Batch` — *"a caller-owned,
reusable buffer. The engine allocates one per stage and passes the same pointer every iteration"* (§1.4) —
and §11(c) step 5: *"the engine rebuilds a retry request from exactly the 12 failed RecordIDs."*

A retry request must be re-encoded, because `Body` is opaque framed-and-possibly-compressed bytes that cannot
be sliced by record. Re-encoding requires the twelve `*record.Record` values. `Request` carries only `Ref`s
(deliberately: "identity and provenance, no payload"). So the engine must retain records per unsettled group
until settlement — plausible, since the ledger holds groups, but **nowhere stated**, and it interacts badly
with two other claims: the one-batch-per-stage reuse idiom, and `Caps.MaxConcurrency > 1` (N concurrent
`Write`s cannot share one reused batch). The size of that retention is `budget × lanes` records held live,
which is the same number `Tracker.Budget()` exports as the replay window — a fact worth stating, because it
turns the budget knob into a memory knob as well and an operator raising it to shrink duplicate replay will be
surprised.

Extensibility consequence: `SinkCaps.PartialFailure` is a declared capability whose honouring depends on a
core retention obligation that is not in the interface set. The first sink that sets it will discover the
obligation.

### D13 — MINOR. `ErrSkip` is a retryable-classed sentinel whose validity is a prose list that grows per optional interface

**At fault:** `ErrSkip = New(TransientInternal, OpUnknown, errSkip)` — *"Supported only where explicitly
documented (currently: `RecordSink.WriteRecords`, `Partitioner.Partition`, `BacklogReporter.Backlog`)"*
(§2).

The `fault.Class` set is closed precisely so that *"a connector must not be able to return a hint the framework
ignores"* — Benthos's `ErrBackOff`-only-on-`Connect` is named as the bug being avoided. `ErrSkip` is that bug,
reintroduced with a three-entry allowlist maintained in a comment. Its class is `TransientInternal`, so
`Retryable()` is true: returned from a method not on the list, it does not fail loudly, it enters the retry
loop and burns `MaxAttempts` before its terminal disposition. Every future optional interface with a fast path
extends the prose list. A typed per-method result (`(handled bool, err error)`, or a distinct `Skipped`
sentinel per interface) removes the class of mistake.

---

## What this proposal does better than any plausible alternative

These are the parts I would fight to keep in a synthesis even if the rest loses.

1. **`ResolvedSource` / `ResolvedSink` and the one-assertion rule (§4.12).** Collapsing an optional-interface
   set into a plain struct of nilable handles, at exactly one site, is the only design in the surveyed field
   that gets Flink's `Supports<Feature>` evolution property *and* a UI-consumable capability document *and* no
   forwarding tax. Benthos pays nine hand-written forwarders per capability; this pays one function. Any
   proposal that keeps type-asserted optional interfaces should adopt this verbatim.

2. **`Position` as `{Seq (core), Token+TokenCodec (opaque), Label (connector-authored display), Safe, At}`
   (§1.2).** `Label` is the cheapest good idea in the document: a connector-authored string the core renders
   and never parses solves "show a meaningful position for an arbitrary connector with zero connector-specific
   frontend code" outright. `Safe` is better: it promotes MySQL-connector-specific transaction-boundary lore
   into a core commit invariant ("the last `Safe` position at or before the resolved prefix"), so a connector
   that has no such distinction sets `true` and pays nothing while one that does cannot get it wrong. Together
   they are the answer to D3's opaque-vs-readable tension without a typed header the core has to interpret.

3. **`Tracker.Budget()` doing three jobs at once (§5.1).** One number is the in-flight bound, the source-side
   backpressure trigger (`Admit` blocks), and the exported worst-case replay window
   (`canal_lane_replay_window_records`). Replacing Benthos's five overlapping bounding knobs with one, and
   making that one knob *the answer to "how much will I re-read after `kill -9`"*, is the single most
   operator-legible idea in any of the proposals.

4. **`RetryPolicy.Terminal` with no valid zero value, plus `Tracker.Abandon` (§2.2, §5.1).** Unbounded retry
   is not a badly chosen default — it is unconstructible — and `Abandon` advancing the prefix is what makes a
   poison record unable to livelock a lane. Encoding a policy rule in a Go zero value, so `Validate` catches
   it rather than a runbook, is the right idiom.

5. **`GroupTTL` + `Leaks() []Leak{Group, Lane, Age, Records, Stage}` (§5.2).** Go has no `Drop`, so the
   reaper is necessary; naming the offending stage/lane/group makes it strictly better than Vector's silent-
   safe-direction behaviour and than Benthos's log line. "Someone forgot to settle" becomes an alertable
   condition. Transplant regardless of which ack model wins.

6. **`Ack.Abandoned` surfaced rather than decided (§4.3).** A source whose commit is destructive (SQS delete)
   may legitimately refuse to advance when records were dead-lettered. The core reports the count and lets the
   source choose. Correct division of authority, and no other surveyed system expresses it.

7. **`Negotiated{Guarantee, Reasons, DurabilityEdge, ReplayWindow, SettleAt}` returned by `Build` and rendered
   on the submit screen (§8, §11(d)).** `DurabilityEdge` as a string naming *where an ack is earned*
   ("sink", "buffer:wal") is an honesty primitive nothing in the prior art has, and returning it from a pure
   function of config is the direct fix for Vector's silent acknowledgement degradation.

8. **Snapshot→stream handoff as lane-announcement ordering (§11(b) step 3).** Announcing the tail lane —
   durably, before any scan lane — makes the handoff invariant survive a crash with **no phase concept in the
   core and no core understanding of a log position**. Nine durable rows replace Debezium's and Airbyte's
   phase-smuggled-into-the-opaque-checkpoint, and re-parallelised resume falls out for free because the
   unfinished lanes are independent rows. This is the cleanest treatment of D6/D7 in the three proposals and it
   costs the core almost nothing.

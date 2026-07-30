# Review — `protocol-first, in-process today`, judged on EXTENSIBILITY

**Reviewer lens:** extensibility only. Not correctness-at-scale, not ergonomics-in-general, not
performance. The question is narrow: **is "to add a source or sink, implement the interface and register
it, done" true — and does the design keep being true when the third, fourth and fifth kind of thing
arrives, when a capability is added in v2, and when the out-of-process binding is finally built?**

**Status:** draft. Judgement of `docs/decisions/proposals/protocol-first.md` as written, at 2,587 lines.
Normative constraints: `docs/design-rules.md` R1–R13. Prior-art traps: `docs/research/_decision-space.md`
Part 3 and the coupling chains in Part 2.

**Score: 6 / 10.**

---

## 1. Why 6 and not 8, and why 6 and not 3

**What earns the high half of the score.** For the *specific* act of adding a source or a sink, this
proposal is the best-argued extensibility story I can construct from the available prior art, and the
claim is close to literally true:

- The required surfaces are small and the growth path is the D9 recommendation implemented exactly:
  behaviour is an optional exported interface, the *fact* is `Capabilities` data, cross-checked at
  `Register`, asserted in one function that materialises a plain struct. Trap #9 (adding a method to a
  required interface) and trap #11 (type-asserted capability that cannot cross a wire) are both
  correctly identified and correctly addressed *in intent*.
- There is no core switch statement that grows per connector. I looked for one and did not find one.
  `probe` grows per *capability*, not per connector; `CriticalityOf` grows per *frame kind*; the
  disposition table is closed. None of these is O(connectors).
- The import-direction rules are structural rather than disciplinary: `connectors/*` cannot compile
  against `engine/`, `api/` cannot import `engine/`, enforced by a `go vet` analyser in
  `tools/importcheck` plus a grep test asserting `api/` contains `"postgres"` zero times. This is R8
  ("drift is prevented structurally, not by discipline") applied to the extensibility boundary itself,
  and it is the single most valuable structural commitment in the document.
- `Spec` as pure data with a closed node set, `SourceInfo` served from the registry with no
  instantiation, `Spec.Mappings` + a core-only `Directive` resolver: this is a complete answer to
  constraint #1's "specialised UI/UX later must not require core changes" for the majority of sinks, and
  no surveyed system has it.
- `Batch.Record(split) *Record` lending a slot whose `id` and `origin` are unexported is a genuinely new
  idea and it closes the KIP-793 retrofit *by construction*.

**What holds it down to 6.** The lens asks four questions beyond "can I add a source". The proposal
fails or nearly fails three of them, and the failures are not hand-wave-able — each has a named
interface and a mechanical consequence:

1. **Can the interface absorb an out-of-process implementation later without change?** No. §9.1's
   `probe` resolves capabilities by type assertion *and* panics on disagreement with declared data.
   A gRPC shim is a single Go type; a single Go type either has a method or does not. There is no
   arrangement of one shim, the stated panic rule, and the stated import direction
   (`transport/grpc` "Never imports engine") that works. Absorbing the second binding requires a new
   exported seam in `engine/`. The thesis sentence — "canal never needs a migration" — is the load-bearing
   claim of the entire proposal and it is not earned. (§2.1 below.)
2. **Does adding a THIRD kind of thing require core surgery?** Worse than surgery: it is unspecified.
   `TransformDescriptor` and `CodecDescriptor` are referenced by `Registry` and never defined.
   `proto.ComponentKind` lists `Buffer` and `Registry` has no `WithBuffer`. `Transform.Apply` receives no
   codec and no host, so it cannot read a record's payload through either declared accessor. Three of the
   five declared component kinds have no registration path, no spec, no capability set and no factory
   signature. (§2.3.)
3. **Can a connector be tested in isolation?** Not from the declared types. `proto.Batch` and
   `connector.WriteResult` have entirely unexported state and no exported constructor, and `testkit` —
   which the layout places in `connector/` — has no way to fabricate either. (§4.14.)
4. **Interface versioning when the core needs something new in v2.** Partially answered and partially
   fictional. The frame union has a criticality mechanism whose sole enforcement point is a `Hello`
   handshake — and `Hello` is not in the `Kind` enum, and the in-process binding has no handshake at all.
   Every closed vocabulary that is *not* the frame union (`ComponentKind`, `DirectiveKind`,
   `PredicateKind`, `SchemaChangeKind`, `SinkMode`, `Stage`, `ValueKind`) has no negotiation story
   whatsoever, and one of them — `DirectiveKind` — is on the critical path of the "specialised sink UX
   with zero core change" claim. (§3.5, §3.9.)

**And one finding that is not about extensibility-as-growth but about extensibility-as-safety:**
`proto.Batch.Emit(Frame)` is exported to connectors and constructs a `Record` without the identity
stamping that `Batch.Record` exists to guarantee. The entire per-record error routing, DLQ, dedupe and
contiguous-prefix apparatus is keyed on `RecordID`. A third-party connector can produce colliding
`RecordID{0,0}` values through a documented public method. (§2.2.)

Why not lower than 6: none of these is a *modelling* error of the kind design-rules was written to
catch. The record model, the checkpoint shape, the capability pattern and the deployment seam are all
right, and every defect below is repairable without restructuring. Defect #1 costs one exported
function; #2 costs one line moved; #3 costs writing the three descriptors that were skipped. That is a
proposal with holes, not a proposal with a wrong spine. Six is "the spine is right and the extension
surface is one-third unbuilt".

---

## 2. Fatal defects

### 2.1 `probe`'s assert-and-panic makes the gRPC binding impossible without a new engine seam

**At fault:** `engine/probe.go`'s `func probe(src connector.Source, info proto.SourceInfo) sourceCaps`
(§9.1), the `Register`-time rule "Declaring a capability whose interface the type does not satisfy
PANICS" (§7), probe's own "cross-check against `info.Capabilities` that panics on a mismatch" (§9.1), and
the layout rule "`transport/grpc` … Imports proto only. **Never imports engine**" (§1).

**The interleaving.** A subprocess connector is represented in-process by one shim type in
`transport/grpc`. The engine must learn that remote connector's capabilities. There are exactly three
routes and all three are closed:

- **Mono-shim.** One shim type implements all fifteen optional interfaces and forwards each over the
  wire. Then `probe`'s fifteen assertions all succeed, so `probe` computes `hasChunker = true`,
  `hasHead = true`, … for a remote source that declared none of them. The stated cross-check —
  "panics on a mismatch" — fires. The mono-shim is illegal *by the rule that exists to prevent
  capability drift*.
- **Combination shims.** Generate one shim type per capability subset so assertions and declarations
  agree. Fifteen capabilities is 32,768 types, and it grows by a factor of two per capability added.
- **Data-derived resolution.** `probe` builds `sourceCaps` from `info.Capabilities` rather than from
  assertions, taking the implementations from the shim. This is what §9.1 actually promises — "a gRPC
  binding implements the capability set from declared data with no assertions at all" — and it is
  unreachable: `probe` and `sourceCaps` are unexported, in `engine/`, and `transport/grpc` may not import
  `engine/`. There is no exported type by which a binding hands the engine a pre-resolved capability
  struct.

**Concrete consequence.** Building the gRPC binding requires either exporting `sourceCaps` (and thereby
making the capability struct part of the contract, which is a new versioning surface with no negotiation
story), or relaxing the panic rule to "asserted-but-undeclared ⇒ ignore" — which re-opens exactly the
declaration/implementation drift the panic exists to close for in-process connectors, and does so
silently. Either way the second binding costs a change to the core's most central extension mechanism.
That is a migration. The thesis says there is never one.

**Why this is fatal rather than major.** Every other property the proposal claims — wire-shippability,
the standalone↔cluster transport swap, "one design decision, two payoffs, and the second was free"
(§12e) — is downstream of the assertion that the Go binding and the wire binding satisfy the same
contract without engine changes. This is the one place where the two bindings provably diverge, and it
diverges at the capability mechanism, which the decision space names as constraining *every other
decision's ability to evolve*.

**Repair.** Make capability resolution an exported, data-first seam:
`engine.ResolveSourceCaps(info proto.Capabilities, impl any) (SourceCaps, []proto.Diagnostic)` where
`SourceCaps` is an exported struct of nullable interface fields; assertions supply implementations,
declared data is authoritative for *whether the engine uses them*, and a mismatch is a diagnostic at
build time rather than a panic in `probe`. The gRPC binding then constructs `SourceCaps` directly. Cost:
one exported type and one exported function. Do it before freezing, not after.

### 2.2 `proto.Batch.Emit` bypasses the identity stamping that the whole accounting layer depends on

**At fault:** `func (b *Batch) Emit(f Frame)` (§2.1), exported on `proto.Batch`, documented "Used by
transforms and by the wire bindings; connectors normally use the typed helpers" — against
`Batch.Record`'s claim to be "the only constructor" for a `Record` (§2.1), and `Record.id` /
`Record.origin` being unexported (§2.3).

**The interleaving.** `connectors/*` import `proto`. A reader's `Fetch` may therefore write:

```go
b.Emit(proto.Frame{Kind: proto.KindRecord, Record: &proto.Record{Value: v}})
```

This compiles. `id` and `origin` are unexported so they are zero: every such record is
`RecordID{Split: 0, Seq: 0}` with an empty `Origin`.

**Concrete consequence.** `Resolver.Written(id RecordID, bytes int)` and `Resolver.Failed(id, …)` are
keyed on `RecordID`. Two records emitted this way collide on `{0,0}`. `Resolver.Committed()` therefore
sees the *first* `Written({0,0})` as satisfying both, and returns marks covering a record the sink never
wrote. `CheckpointStore.Set` commits that cursor. On restart the unwritten record is never re-read.
**This is silent data loss with an advanced durable cursor, which is R4's exact failure mode, reachable
by a third-party connector through an exported method whose doc comment is the only guard.** It also
breaks per-record DLQ targeting (`Fault.Record`), dedupe keying (R5) and the `emitted == persisted +
dropped` reconciliation §12c is proud of.

Secondary consequence: `Origin` empty means `Origin.Split` is the zero `SplitID`, so the DLQ record's
provenance — the thing §12c step 3 advertises — is absent, and the "provenance is structurally
immutable" claim in §2.1 and §13's `record-envelope` row is false as stated. It is immutable *if you use
`Batch.Record`*, which is prose, and R12/R8 exist to distrust prose.

**Repair.** `Emit` must not be reachable from `connectors/*`. Two clean options: (a) move `Emit` behind
an unexported-interface accessor obtained only by `engine/` and `transport/*` (`type frameEmitter
interface{ emit(Frame) }` implemented on `*Batch`, plus `proto.InternalEmitter(*Batch) FrameEmitter` in a
`proto/internal` package the importcheck analyser forbids to `connectors/`); or (b) have `Emit` stamp
`KindRecord` frames whose `id` is zero, and panic on a non-zero `id` it did not issue. (b) is one `if`
and preserves the transform use case. Also: the `tools/importcheck` analyser that already exists should
gain a rule banning `proto.Batch.Emit` in `connectors/` — the enforcement machinery is present, it is
simply not pointed at the hole.

### 2.3 Three of five component kinds have no registration surface; `Transform` cannot read a payload

**At fault:** `Registry.WithTransform(d TransformDescriptor)` and `Registry.WithCodec(d CodecDescriptor)`
(§7) with `TransformDescriptor` and `CodecDescriptor` never defined; `proto.ComponentKind` = `Source |
Sink | Transform | Buffer | Codec` (§6, `Field.Component`) against a `Registry` with **no `WithBuffer`**;
`Transform.Apply(ctx, in *proto.Batch, out *proto.Batch) error` (§9); `Buffer.Push/Pop` (§9);
`Payload.Bytes(ctx, enc Serializer)` and `Payload.Structured(ctx, dec Deserializer)` (§2.3).

Four distinct sub-defects, one root cause: the extension surface was designed for sources and sinks and
then asserted for everything else.

**(a) `Transform` cannot access a record's value.** Both payload accessors require a codec handle.
`Apply` is given `ctx`, an in-batch and an out-batch. There is no `Serializer`, no `Deserializer`, no
`Host`, and no `TransformDescriptor.New(OpenRequest)` to have received one at construction. A transform
can therefore move, drop and derive records but cannot read or write what is in them. §13's
`topology-and-transforms` row claims "one `Transform` interface whose out-batch gives the full return-type
vocabulary" and §9 claims "windowing and async lookups are expressible" — a windowing transform that
cannot read a field cannot key a window.

**(b) The codec plugin type has no coherent home.** `proto.Payload.Bytes` takes a `Serializer`
parameter, so `Serializer` must be visible from `proto`. §1 places `Serializer` in `codec/`, which
"Imports proto" — an import cycle. The alternative is that `Serializer`, `Deserializer`, `Framer`,
`Unframer` and `Compressor` all live in `proto`, which contradicts `proto`'s own package doc: "no
interfaces except the two sanctioned extension points documented below" (`Value` and
`Payload.structured`). One of the two most-cited structural invariants in the document is broken by the
signature of the record model's primary accessor.

**(c) `Buffer` is a declared `ComponentKind` that cannot be registered.** `Field.Component` may be
`Buffer`; `Plan.Buffers []proto.BufferPlan` exists; `engine/buffer.go` exists. `Registry` has no
`WithBuffer`, no `BufferDescriptor`, no `Buffers()` listing for the API. A third-party disk buffer is
inexpressible.

**(d) `Buffer` cannot spill a structured payload and cannot declare durability.** `Push(ctx, b *Batch)
(accepted int, err error)` receives frames whose `Payload.structured` is explicitly "never serialised"
(§2.3). A disk buffer must encode, and it has no codec. `WhenFull = FullOverflow` (memory → disk) spills
*after* the memory tier accepted structured payloads, so the encode must happen at spill time with a
codec the buffer does not hold. Worse for extensibility: `Buffer` has no way to say *"I am a durability
boundary"*. D12 item 6 makes this the mechanism by which R4 is implemented rather than written down — "a
durable buffer takes ownership of the ack handles on write, so the checkpoint advances on buffer write; a
memory buffer passes them through". Under the declared interface the engine cannot distinguish the two,
so either every buffer is treated as non-durable (a disk buffer buys nothing) or every buffer is treated
as durable (a memory buffer acknowledges a RAM slice, which is R4's original catastrophe verbatim, and is
the specific failure design-rules calls "the most dangerous gap in the whole design").

**Concrete consequence, all four together.** "Adding a transform / a buffer / a codec" is not
"implement the interface and register it" — there is no descriptor to fill, no `Registry` method to call
for two of them, no `Spec` so no UI, no `Capabilities` so no `Negotiate` participation, no `Host` so no
metrics or logging, and for `Transform` no way to touch the data. Given the brief's explicit test — "does
adding a THIRD kind of thing require core surgery" — the honest answer from the document as written is
that the third kind of thing has not been designed.

**Repair.** Write the three descriptors with the same shape as `SourceDescriptor`
(`proto.XInfo` + `New func(ctx, OpenRequest) (X, error)`), add `Registry.WithBuffer`, put a
`Codecs()`/`Transforms()`/`Buffers()` listing on `Registry` for the API, add `Serializer`/`Deserializer`
to `OpenRequest` (or, better, to a `TransformHost`), and give `Buffer` a declared
`Durability` capability plus an encode hook. This is not a redesign — it is the missing two-thirds of
§7 applied to §9.

---

## 3. Major defects

### 3.1 The sink→engine handle has unfailable methods, so it cannot be implemented over a wire

**At fault:** `connector.WriteResult`'s methods (§5.2) — `Failed(id, f)`, `Duplicate(id)`, `Deferred(id)`,
`Committable(c)`, `RequestMark()`, `Stats(records, bytes)` — none of which returns an `error`, against
`connector.Emitter`'s methods (§5.1) — `Send(ctx, f) error`, `Assign(ctx, r, splits) error`,
`NoMoreSplits(ctx, s) error`, `Estimate(...) error`, `Fault(...) error` — all of which take a `ctx` and
return an `error`. Also `Host.Counter(name, unit, help) Counter` and `Host.Gauge(...) Gauge`, which return
live handles whose increment methods likewise cannot fail.

**Why this is an extensibility defect and not a style nit.** P2 sanctions handles as parameters precisely
because "the in-process binding implements them with method calls; the gRPC binding implements them with
stream sends". `Emitter` honours that: a stream send can fail and the signature can say so. `WriteResult`
does not. The proposal's own frame union declares `KindOutcome` (20), `KindCommittable` (21) and
`KindMarkRequest` (22) as sink→engine frames — so these six method calls *are* frame sends in the wire
binding, and six frame sends are given no way to report that the stream broke.

**Concrete consequence.** A gRPC sink binding whose `Outcome` send fails mid-`Write` has exactly one
channel left: return a non-nil error from `Write`. The engine's interpretation of a non-nil `Write` return
is "the whole batch failed" (§5.2: "Returning nil from `Write` means every record in `b` that is not named
in `res` is DURABLE"). So a transport hiccup while reporting that 3 of 500 records failed is reported to
the engine as *500 failures*. Under `RetainAndBackoff` that is 497 duplicate writes on retry; under a
sink at tier 1 with a non-idempotent destination that is 497 duplicated rows. The in-process binding
cannot exhibit this, so the defect is invisible until the binding the thesis is built around is
implemented — which is the definition of a design that has not absorbed the second binding.

`Host.Counter` is the same shape one level worse: it returns a *stateful remote object identity*, which is
not a frame at all. §5.4 claims "Every method maps onto an uplink frame"; `Counter(name, unit, help)
Counter` maps onto a frame plus an object handle, and the returned handle's `Add` cannot fail either. The
proposal's honest-weakness #3 admits the batching is undesigned; the deeper issue is that the signature
shape forecloses reporting failure at all.

**Repair.** Give every sink→engine and connector→host call the `Emitter` shape:
`(ctx, …) error`. If per-record `Failed` calls are too hot for an error return, make `WriteResult`
accumulate and add one `res.Err() error` the engine checks after `Write` returns — the deferred-error
pattern `database/sql` already uses, which the proposal cites elsewhere as its model.

### 3.2 Meta-components are declared as config and have no runtime contract; composites re-implement the resolver privately

**At fault:** `Field.Component proto.ComponentKind` + `Repeated bool` (§6) as the *entire* topology
mechanism; `Binder.Component(path string, dst *proto.ComponentRef)` (§6.1); the absence of any interface
by which a component declares "I am a stage, not a leaf"; `Fault.Stage` as a closed seven-value enum
(§8); and the hard-closed metric label vocabulary (§11).

**The interleaving.** A fan-out sink is, per §9 and §13, "just a component-valued config field": a `Sink`
whose `Spec` has a repeated `Component` field of kind `Sink`. At build time the parent receives a
`proto.ComponentRef` — a name plus config — and must instantiate its children itself (it can: `connector`
exports `Default()`). From the engine's point of view there is now **one** sink returning **one**
`WriteResult`.

**Concrete consequence 1 — the resolver is bypassed.** Branch-level durability is the parent's private
business. To be correct under at-least-once the parent must merge N children's per-record outcomes with
worst-wins semantics keyed by `RecordID`, decide when a record is durable in *all* branches, bound its own
in-flight set, and propagate backpressure from the slowest branch. That is the algorithm §3.1 puts in core
specifically "so that no connector ever writes it", and §12c's whole argument ("This is why the resolver
is in core and not in every connector") is undone the moment the sanctioned composition mechanism is used.
Benthos's fan-out semantics are honestly documented because Benthos owns the broker; here the semantics
are whatever each third-party composite author implements, unobservably.

**Concrete consequence 2 — composed topologies are unobservable.** `Fault.Stage` is
`Read | Transform | Encode | Buffer | Write | Commit | Checkpoint`, and the metric label vocabulary is
closed to `pipeline, stage, connector, class, reason, buffer, outcome, worker, phase, type, status, op`
with "nothing per-stream, per-key or per-message ever becom[ing] a label". A component path
(`sink.fan_out[1].retry.http`) is unbounded, so there is no label and no field that can carry it. Benthos's
`IntoPath` automatic observability nesting — which the proposal cites as the reason this mechanism works —
is therefore **inexpressible in canal's declared metric and fault model**. A fan-out to three sinks
produces one `stage="write"` series and a `Fault` that cannot name which branch failed.

**Concrete consequence 3 — R1 tension.** `Fault.Stage` is a fixed stage list living in the contract and
surfaced through `/metrics` and `PipelineStatus.LastError`. §9 correctly insists the outer shape "is a
fixed IMPLEMENTATION FACT and never an enumerated stage list in any API" — and then puts an enumerated
stage list in the API by a side door. It is already incomplete: there is no `Decode` and no `Deframe`
stage, so an `Unframer` or `Deserializer` failure on the *source* side — which §8 explicitly says must be
dead-letterable — has no stage to be attributed to. Adding one is a contract edit plus a frontend edit.

**Repair.** D13 item 3 asked for exactly the missing piece and the proposal walks past it: "Give
meta-components a sanctioned declaration that they are a stage rather than a leaf. Do not ship Benthos's
undocumented `Unwrap()` hole." Concretely: a `connector.Composite` optional interface returning its
children plus a path segment, so the engine (a) instantiates children itself, (b) runs one resolver per
leaf, (c) nests metrics by path, and (d) can render the topology in the UI. Replace `Fault.Stage`'s enum
with `Stage` (kind) plus `Path []string`, and let the *read model* carry the path while the metric label
stays closed.

### 3.3 `Reader.Fetch` blocking and `AddSplits`-while-blocked are mutually exclusive as specified

**At fault:** the `Reader.AddSplits` doc comment (§5.1): "A reader must accept splits at any time,
**including while a `Fetch` is blocked** — so a reader that buffers internally serialises these against
its own fetch loop, and **the engine never calls them concurrently with `Fetch`**", against `Fetch`'s
"expected to block until data, ctx expiry, or a short internal deadline".

**The contradiction.** If the engine never calls `AddSplits` concurrently with `Fetch`, then `AddSplits`
cannot be called while `Fetch` is blocked. The sentence asserts both halves in fifty words.

**Concrete consequence.** Under the only self-consistent reading — engine-serialised calls — split
assignment latency is bounded below by `Fetch`'s internal deadline. That is precisely the seam the
pull-based assignment protocol lives on: §12e's headline scaling story is "during `PhaseBackfilling` the
planner hands 64 chunk splits to 20 workers as they ask for work". A worker that has just gone quiet on
its current split sits inside a blocking `Fetch` and cannot receive the chunk it asked for. The fix
available to a connector author is to give the reader an internal goroutine plus a channel and return
promptly from `Fetch` — i.e. the interface's "chosen for performance rather than symmetry" shape pushes a
concurrency structure into **every** reader, which is the cost Flink paid ~400 lines of
`FutureCompletingBlockingQueue` to pay *once, in the framework*. The proposal's honest weakness #7
concedes the blocking semantics are prose; it does not notice that the prose is internally contradictory
and that the resolution determines whether pull-based assignment works.

**Repair.** Pick one and make it a type. Either (a) `Fetch` is non-blocking and readiness is a channel the
reader hands the engine once (`Ready() <-chan struct{}`, Flink's future in idiomatic Go), which makes
`AddSplits` promptly deliverable and removes the spin-detector mitigation entirely; or (b) `Fetch` blocks
and split delivery is in-band as a frame the engine pushes into the same `Batch` — which is more
protocol-consistent, since `SplitDelta` is already a frame, and would make the reader side genuinely
symmetric with the enumerator side.

### 3.4 `Enumerator.Snapshot` must be a pure read while concurrent emission is sanctioned — the Flink CDC trap, re-entered

**At fault:** the `Enumerator` doc (§5.1) — "The engine calls these methods from a single goroutine. An
enumerator that wants concurrency spawns its own and emits from it; `Emitter` is safe for concurrent use"
— against `Snapshot(ctx, m proto.MarkID) (proto.Blob, error)`: "It must be a pure read: the engine may
call it while readers are mid-fetch."

**The interleaving.** A source that discovers work asynchronously (any source with a slow catalog, any
chunker probing key ranges, i.e. every source the chunked-snapshot engine exists for) spawns a goroutine
per the sanctioned pattern. That goroutine mutates the enumerator's unassigned-split set and its chunker
cursor. `Snapshot` reads that state to produce the checkpointed `Blob`. The engine calls `Snapshot` on its
own goroutine at mark time. That is a data race on connector-private state, and the interface doc tells
the author the opposite: "the engine calls these methods from a single goroutine".

**Concrete consequence.** `-race` will find it only if a test happens to interleave a `Snapshot` with
discovery. Without the race, the observable failure is a torn enumerator blob: a `finishedCount` from
before an update with a `chunkerNext` from after it, committed atomically with position. On resume the
enumerator re-slices from the wrong cursor or under-counts finished chunks, so
`totalFinishedSplitSize == len(finished)` never holds and the backfill never declares
`NoMoreSplits`. This is the failure class the decision space names twice — Flink CDC shipped **two**
`ConcurrentModificationException` fixes for touching enumerator state off-thread — and D5's
recommendation was explicitly written to avoid it: "The enumerator is a goroutine owning its state, driven
by one `select` over a command channel. This is strictly nicer in Go than Flink's
`callAsync`/`runInCoordinatorThread` and removes the class of bug Flink CDC hit twice. **No mutexes, no
prose warning about shared state.**" The proposal ships method handlers plus a concurrency-sanctioning
prose warning — Flink's shape, in Go, with the documented scar unfixed.

**Repair.** Either mandate the mutex in the type (give the enumerator no `Snapshot`; instead have it emit
its state as a frame in response to a `MarkNow`-equivalent, so state only ever leaves on the engine's
goroutine), or adopt D5's recommendation and give the `Enumerator` a single `Run(ctx, cmds <-chan Command,
e Emitter) error`. The second is the protocol-consistent option — commands are frames — and it is what the
thesis should have produced.

### 3.5 `Criticality` is enforced by a `Hello` that is neither a frame kind nor present in-process

**At fault:** the `proto` package doc's versioning contract (§2) — "an unknown `Ignorable` kind is counted
and skipped; an unknown `MustUnderstand` kind aborts the session **at Hello time**, never mid-stream" —
plus `CriticalityOf(k Kind)`; against the `Kind` constant block (§2), which contains **no `KindHello`,
`KindControl`, `KindAccept`, `KindAssign`, `KindRevoke`, `KindConfirm` or `KindDrain`**, and against §1's
`proto/control.go  Hello/Accept, Assign/Revoke, Confirm, Drain, ConfigPatch`.

Three compounding problems.

**(a) The thesis's own frame list does not match the union.** The thesis sentence names nine frames
including `Control`. The `Frame` struct has fourteen pointer fields and no `Control`. `KindConfigPatch`
(33) is the only member of the control family that made it into the enum. So the frames the criticality
mechanism, the session handshake, the assignment protocol and graceful drain are all specified in terms of
do not exist in the closed tagged union that is "canal's entire contract".

**(b) In-process there is no handshake, so `MustUnderstand` is unenforced.** In-process connectors compile
against the host's `proto` (one module version wins in a Go build), so version skew cannot produce an
*unknown* kind — but it can and will produce an *unhandled* one. Every sink's `Write` iterates
`b.Frames()` and switches on `Kind`; §12a's own `stdoutSink` writes `if f.Kind != proto.KindRecord {
continue }` and thereby silently skips `KindMark` and `KindSchemaChange`, both of which are declared
`MustUnderstand` because "silently skipping either loses durability or corrupts a sink". When protocol v2
adds a `MustUnderstand` kind that sinks must act on, every existing in-process sink compiles unchanged and
ignores it. There is no compile error (a `switch` with no arm is legal Go), no registration check (the
declared `SinkInfo.Protocol` is a literal the author will most likely write as `proto.Version`, which
auto-bumps and lies), and no handshake. **Protocol-version safety exists only in the binding that has not
been built.**

**(c) The engine already filters by capability, not criticality.** §5.2: "A sink that does not implement
`SchemaApplier` never sees a `SchemaChange` — the engine filters it and applies the drift policy itself."
So the actual mechanism protecting sinks from frames they cannot handle is `Capabilities`, and
`Criticality` is a second vocabulary for the same concern — R9's definition of a modelling error, and dead
weight in-process.

**Repair.** Add the control family to `Kind` (they are frames or the thesis is not true). Then make
criticality enforceable in-process the only way Go allows: derive the set of frame kinds a sink will
receive from its declared `Capabilities` at `Negotiate` time, and make the engine refuse to deliver a
`MustUnderstand` kind to a component that has not declared it. That collapses (a) and (c) into one
mechanism and gives (b) a registration-time failure instead of a silent skip.

### 3.6 Load-bearing mechanisms exist only in prose, with no field, constant or injection point

Four instances, one pattern. Each is a place where the document argues an extensibility property and the
declared type set cannot express it. This matters more than usual here because constraint #4 says the
immediate deliverable *is* the interface set.

**(a) `TokenSink` — the strongest guarantee tier — is unimplementable.** §5.3: "The token to store with
the next `Write`'s transaction is passed via `WriteResult`'s inbound side: `proto.Batch` carries it on the
`Mark` frame." `Mark` is `{ID, Split, Cursor, Records, Bytes, Phase, At}` (§3). **There is no token
field.** Further, a checkpoint token is per-checkpoint while `Mark` is per-split, so even after adding the
field it is undefined which split's `Mark` carries it and what a sink does when it sees three. `CapToken`
is in the capability list, `TokenSink` is in the optional-interface list, and the mechanism connecting
them does not exist. Adding it is a field on a `MustUnderstand` frame in the contract package.

**(b) `RequiresCompleteImages` is not a capability.** §2.3 makes the central argument for
`Completeness`: "A sink that cannot merge partials declares `RequiresCompleteImages` and the pipeline is
refused at submit time rather than corrupting rows at 3am." The `Capability` constant block (§7) has
fifteen members and no such constant, and `Negotiate` has no corresponding check. The submit-time refusal
that makes the `Change` facet safe for a generic sink is asserted and absent — which is trap #16 (silent
capability degradation) reintroduced at the one point the proposal invented a defence against it.

**(c) `Bind` has no home.** §6.1 makes registration-time spec/struct cross-checking the structural
anti-drift mechanism (R8): "every spec field must be bound and every bound field must exist in the spec,
or `Register` panics at init". The binding is a closure, so P1 forbids it from `proto.Spec`.
`SourceDescriptor` is `proto.SourceInfo` + `New`. `proto.SourceInfo` is `… Spec Spec …`. **No declared
type has a field for the binder.** §6.1's example creates `var spec = connector.NewSpec()….Bind(…)` and
nothing consumes it.

**(d) `DeadLetter` is injected nowhere.** §8 declares `type DeadLetter interface { Send(ctx, r *Record, f
*Fault) error }` and argues "DeadLetter works for sources too — an undecodable frame from an unframer, a
record that fails the configured catalog's schema, a cursor that will not parse. Kafka Connect's
per-record reporter is sink-only, which is why source decode failures there are a log line and a lost
record." `Host` (§5.4) has six methods and none is a DLQ. `OpenRequest` has five fields and none is a DLQ.
So a source cannot dead-letter, and the stated improvement over Connect is unavailable to exactly the
component the argument is about.

**Concrete consequence.** Each of these is a small edit — but each edit lands on `proto` (a `Mark` field,
a `Capability` constant), on `connector` (a descriptor field, a `Host` method), i.e. on the contract that
is supposed to be frozen before connectors are written. Discovering them after the first ten connectors
exist means either a breaking change or a second mechanism bolted alongside. Find them now.

### 3.7 `StateMigrator` covers one of three connector-authored blob families

**At fault:** `StateMigrator.MigrateState(ctx, b proto.Blob, from uint32) (proto.Blob, bool, string)`
(§5.3) — a single blob, singular — against the three places a connector authors versioned bytes:
`Checkpoint.Enumerator Blob`, `SplitState.Cursor Cursor` (= `Blob`, and `Split.Start`/`Split.End`/
`CompletedSplit.Cursor` too), and `Checkpoint.Committables []Committable`. Plus `Split.Attrs Blob`.

**The interleaving.** `Blob{Version uint32, Bytes []byte}` is carried everywhere, and §3 makes the
argument for it: "Version is the connector's own, and is what lets a connector change its state format
across an upgrade — the thing whose absence is the root cause of Airbyte's reset-on-config-change pain."
A connector v2 that changes its cursor encoding is restored from a v1 checkpoint. `Enumerator` goes
through `MigrateState`. `Split.Start` — the cursor a reader resumes from — arrives via `AddSplits` with
`Version: 1` and no hook. `Committable`s replayed to `Committer.Commit` after an upgrade likewise.

**Concrete consequence.** The connector's `Reader` receives v1 cursor bytes and either (a) checks
`Cursor.Version` itself and hand-rolls migration inside `AddSplits`, per split, per connector — the
"every source reimplements the same non-trivial algorithm" pathology the design fixes everywhere else — or
(b) does not check, parses v1 bytes as v2, and resumes at a wrong position: silent duplicate delivery or
silent skip, at the one moment (an upgrade) when nobody is watching for it. Decision-space "Decisions the
architecture must close" item 9 is *checkpoint state format compatibility across binary upgrades*; this
answers it for a third of the state.

**Repair.** Make migration total and uniform:
`MigrateBlob(ctx, kind BlobKind, b Blob, from uint32) (Blob, bool, string)` with `BlobKind ∈ {Enumerator,
Cursor, Committable, SplitAttrs}`, called by the engine on every restored blob before it reaches the
connector. It is the same method with one more parameter, and it turns four hazards into one hook.

### 3.8 `RecordID` embeds a session-scoped `SplitRef` and is then persisted

**At fault:** `RecordID{Split SplitRef, Seq uint64}` (§2.2) where `SplitRef` is "a session-scoped integer
alias … valid until the session ends"; against `Fault.Record RecordID` (§8), which lands in the DLQ, in
`PipelineStatus.LastError` and in `RecentEvents`; and against `Origin.Parent RecordID` (§2.3), which sits
inside the `Origin` the design elsewhere insists survives everything ("`Split SplitID` // full id, not the
ref: survives the session").

**The interleaving.** `Origin` is scrupulous about durability: it carries the full `SplitID` precisely
because the ref does not survive. Then `Origin.Parent` — the unframer fan-out link that §9 says is what
makes ack aggregation structural — is a `RecordID`, hence a `SplitRef`, hence session-scoped, inside that
same durable struct. Same for `Fault.Record`, which §12c step 3 advertises as arriving at the DLQ "with its
full `Origin`".

**Concrete consequence.** After a restart, `SplitRef(7)` denotes a different split. A dead-lettered
record's `Fault.Record` and its `Origin.Parent` are therefore uninterpretable: an operator cannot
correlate a DLQ entry back to a record, a DLQ replay tool cannot address one, and the parent/child ack
grouping cannot be reconstructed. The mechanism the design offers as its answer to Benthos's positional-
identity scar ("canal pays twelve bytes instead") is durable in name only.

**Repair.** Two identities, named as such: `RecordRef{Split SplitRef, Seq uint64}` for the hot in-session
path, and `RecordKey{Split SplitID, Seq uint64}` materialised by the engine at the moment a record leaves
the session (DLQ write, `Fault` persisted into the read model, `Origin.Parent`). The engine already owns
the `splitTable` mapping, so the materialisation is a lookup, not a design change.

### 3.9 Every closed vocabulary except the frame union extends only by coordinated core + frontend edits

**At fault:** `proto.ComponentKind`, `DirectiveKind` (10 nodes), `PredicateKind` (9), `SchemaChangeKind`
(11), `SinkMode` (4), `SourceMode` (4), `ValueKind` (16), `Stage` (7), `Widget`, `FieldType` (13),
`Capability` (15) — and `func Register(d any)` (§7).

**The asymmetry.** The frame union has a real versioning story: append-only kinds, a criticality table,
and an explicit statement that adding a kind, field, enum member or capability "is additive and does not
bump [Version]". Every *other* closed set in the contract has none. And several of them are on the
critical path of the extensibility claim:

- **`DirectiveKind`.** §6.3's claim is the strongest in the document: "Adding a sink with a rich bespoke
  form is: declare `Mappings`, implement `Write`, register. No core change, no frontend change." True
  only while the sink's mapping needs fit ten nodes. There is no `lower()`, no `substring`, no date
  format, no unit conversion, no per-destination enum remap, and no escape hatch — deliberately, since
  a sink-provided function could not cross a wire, and `engine.Resolve` is "the only code that reads a
  `Directive`. A sink never evaluates one." So the first sink that needs `lowercase(email)` as an
  editable, previewable mapping requires a node added to `proto` **and** an arm added to the frontend's
  evaluator, shipped together, with an old frontend rendering the mapping as uneditable. The proposal's
  honest weakness #5 states this; what it does not state is that this makes the headline claim conditional,
  and that the condition is not checkable in advance.
- **`ComponentKind` + `Register(d any)`.** Adding a sixth component kind (a partitioner, a secret
  provider, a DLQ backend, a status exporter) costs: a `ComponentKind` member; a descriptor type; a
  `Registry.WithX`; an arm in `Register`'s type switch on `any`; a listing method for `api/`; component
  resolution in `engine/`; and an arm in the frontend's `FieldComponent` renderer. Seven sites. `Register(d
  any)` is itself an untyped switch in core that grows per kind — the exact construct constraint #4
  forbids, wearing `any` as a disguise, and worse Go than four typed `RegisterSource`/`RegisterSink`/… 
  functions would be.
- **`SinkMode` / `SchemaChangeKind` / `ValueKind`.** A sink whose destination semantics are not
  `Append | Upsert | Overwrite | SoftDelete` (an SCD-2 dimension, a merge-with-reduction target) cannot
  declare them; a sink that can apply a DDL outside the eleven `SchemaChangeKind`s cannot declare it; a
  source with a native type outside sixteen `ValueKind`s falls to `KindJSON`, which the comment concedes is
  "an escape hatch, and a countable one" — i.e. the type system's answer to novelty is to record how often
  it fails.

**Concrete consequence.** The proposal's growth mechanism is "capability = data + optional interface", and
that mechanism genuinely handles *behaviour*. It does not handle *vocabulary*, and roughly half the
extension pressure on a connector framework is vocabulary pressure. There is no version negotiation, no
`Ignorable`/`MustUnderstand` equivalent, and — for the two ASTs — a hard release coupling between the Go
core and the TypeScript frontend that the rest of the design is careful to avoid.

**Repair.** Two cheap moves. (1) Apply the frame union's own discipline to every closed enum: append-only,
numbered, with a documented "unknown member ⇒ ignore-and-count" or "⇒ refuse at submit" rule per enum, and
put the rule in one table beside `CriticalityOf`. (2) For `Directive` specifically, either accept the
expression language now (the decision space's open question 9, deferred rather than answered) or add one
node — `DirCallNamed{Name string, Args []Directive}` resolved against a *registered, declared* function
table with its own spec — so that growing the mapping vocabulary becomes registering a component instead
of editing two repositories.

---

## 4. Minor defects, and smaller findings the synthesiser should still see

### 4.11 Three `Phase` vocabularies and two `Estimate` emit paths — R9

**At fault:** `proto.Phase` = `Unknown | Backfilling | Streaming | CatchingUp | Idle | Completed` (§3);
`proto.StreamPhase`, used only in `Host.StreamStatus(ctx, s, st proto.StreamPhase, f *Fault)` (§5.4) and
never defined; and `PipelineStatus.Phase Phase // k8s-shaped` (§11), which per D14 is
`Pending | Starting | Running | Paused | Stopping | Stopped | Failed | Completed`. Three phase
vocabularies, two of them sharing the identifier `Phase`.

R9: "one wire enum plus one i18n key namespace. A function mapping between two representations of the same
concept is evidence of a modelling error." Worse than that here: the same *name* for two different
concepts. `CheckpointHeader.Phase` is data-progress; `PipelineStatus.Phase` is lifecycle. A frontend that
receives both in one status document and renders "Phase" twice is the tier-vocabulary sprawl design-rules
was written about, one layer earlier.

Related, same rule: `Estimate` can be emitted through `Emitter.Estimate(ctx, e, scope)` (§5.1, enumerator)
and through `Host.Estimate(ctx, e, scope)` (§5.4, anyone). One entity, two identifiers — the pattern the
abandoned attempt died of (buffers as stages 3/5/7 *and* as segments keyed by `followsStageOrdinal`). And
the asymmetry runs the other way too: `Emitter` has `Fault(ctx, f) error`, `Host` does not, so a `Reader`'s
background goroutine has no way to report a fault out-of-band and must wait to fail the next `Fetch`.

**Repair.** Rename: `proto.DataPhase` for progress, `runtime.Lifecycle` for the k8s shape, and delete
`StreamPhase` in favour of one of them plus a scope. Pick one `Estimate` path — `Host`, since `Emitter` is
enumerator-only — and delete the other. Give `Host` a `Fault` method for symmetry.

### 4.12 `Meta.Set` accepts `any`, so the "closed" `Value` set is enforced nowhere

**At fault:** `type Value any` documented as "Exactly one of: nil, bool, int64, uint64, float64, string,
[]byte, time.Time, Decimal, map[string]Value, []Value" (§2.3), and `func (m *Meta) Set(key string, v
Value) error` whose documented failure mode is only "errors on a `canal.` key".

The wire-safety harness is reflective over the *type graph*; reflection over static types cannot check
what a runtime `any` holds. So `m.Set("x", myStruct{})` compiles, passes `TestWireSafety`, and fails
either in the codec at run time or silently. The proposal's own framing — "checked by CI rather than by
review" — does not hold for the one type where the closure is doing real work.

**Repair.** Validate in `Set` (a type switch returning a `Fault` of class `PermanentMapping`) and add a
`testkit` conformance case that round-trips every `Meta` value a connector produces through the frame
codec. Cheap, and it converts a run-time surprise into a connector-test failure.

### 4.13 The canonical trivial-source pattern double-closes

**At fault:** `Source.Close` — "It is always called **exactly once**, including after a failed `Streams`,
`Enumerator` or `Reader`, and including on cancellation" (§5.1) — and `Reader.Close` and
`Enumerator.Close`, against §12a's `ticker`, whose `Reader(ctx, cc)` returns `t` itself.

One value satisfies `Source` and `Reader`, so it has one `Close` method reached by two lifecycle owners:
the reader loop closing its reader, and the pipeline closing its source. Both are documented as
"exactly once". `ticker.Close` happens to be idempotent; a real source closing a connection, releasing a
replication slot or deleting a temporary object is not, and the failure is a double-release at shutdown —
the least-tested path in any connector.

This is not a typo in the example; returning `self` as the `Reader` is the obvious way to write a
single-split source and the proposal presents it as such. The interface shape invites it and the contract
forbids it.

**Repair.** State the rule the shape actually implies: `Close` is called once per *interface instance
obtained*, and a connector that returns itself must be idempotent — plus a `testkit` case that calls the
whole lifecycle twice and asserts no error. Better: give `Reader` and `Enumerator` no `Close` at all and
make `Source.Close` the single teardown, since the engine knows what it created.

### 4.14 `proto.Batch` and `connector.WriteResult` cannot be constructed by a test

**At fault:** `Batch`'s fields are all unexported (`frames`, `records`, `maxRecs`, `maxBytes`, `bytes`,
`splits *splitTable`, `stamp stamper`) with no exported constructor; `WriteResult struct{ /* per-record
entries, keyed by RecordID */ }` likewise; `Batch.Reset()` exported and documented "engine-only"; and
`connector/testkit` positioned as "conformance suite every connector runs. Imports connector + proto."

**Concrete consequence.** A connector author cannot write `TestFetch` — there is no way to obtain a
`*proto.Batch` whose `splits` table and `stamp` are populated, so `b.Record(ref)` cannot be called outside
the engine. Nor can they write `TestWrite` without a `*connector.WriteResult`. `testkit` has the same
problem and it lives in the layer that is supposed to solve it. The lens question "can a connector be
tested in isolation" therefore answers *no* against the declared types, for the two types every connector
touches on its hot path. `Batch.Reset()` being exported-but-engine-only is the same problem from the other
side: the one guard is a comment.

**Repair.** `proto.NewBatch(opts BatchOptions) *Batch` and `connector.NewWriteResult() *WriteResult`,
plus a `proto/prototest` package exposing a deterministic stamper. Then move `Reset` behind the
internal-emitter accessor proposed in §2.2. This is fifteen lines and it is the difference between a
testable plugin API and one whose conformance suite cannot exist.

### 4.15 Smaller items, recorded without argument

- **Method-count claim is shaded.** §12a concludes "two required methods for the sink, four plus three
  reader methods for the source". `Reader` has five methods (`AddSplits`, `RemoveSplits`, `Fetch`,
  `MarkNow`, `Close`); `ticker` writes eight distinct methods plus a helper enumerator plus a `Spec` plus a
  `Register`. The real number is fine; stating it as seven is the kind of shading that makes a reviewer
  distrust the rest of the count.
- **`SourceInfo.Icon`: "data URI or well-known id".** The "well-known id" branch is a frontend asset map
  that grows per connector, which contradicts §12d's "Adding a connector adds a row. No core edit, no
  frontend edit." Drop the branch; require the data URI.
- **`proto.Spec.JSONSchema()` and `Spec.Hash()`** are behaviour on a type in a package documented as
  containing "data types, enumerations and their versioned serialisers, and nothing else". Harmless, but
  it is the same erosion that will later be cited as precedent for putting something worse there.
- **`PipelineStatus.Conditions`** is `Ready, Progressing, Degraded, Backpressured, SchemaDrift,
  CheckpointStale`, dropping D14's `Connected` and `Assigned`. D14's load-bearing requirement is that
  "`Connected: True` must never be able to imply `Progressing: True`" — a separation that needs
  `Connected` to exist. Not an extensibility defect, but it is a normative requirement quietly dropped.
- **No `Op` for replace/rename, and `KindJSON` as the `ValueKind` escape hatch.** Both are honest,
  bounded losses inherent to D1 option (c); noted so the synthesiser does not think they went unexamined.

---

## 5. What this proposal does better than any plausible alternative

These are not compliments; they are the parts I would transplant into a winning proposal even if this one
loses, with the reason each is hard to arrive at independently.

1. **`Batch.Record(split SplitRef) *Record` — lending a slot with unexported `id` and `origin`.** Every
   other design in the space achieves immutable provenance by *rule* (Connect's javadoc warnings, the
   `original*` retrofit of KIP-793) or not at all. Making the framework the only party that can construct
   a `Record` makes it structural, and it simultaneously eliminates the per-record allocation that a
   copy-on-admission design would cost. Combined with `Batch.Derive(in)` copying `Origin` verbatim, a
   transform *cannot* corrupt checkpoint identity, which is Part 0 item 9 satisfied in the type system
   rather than in prose. This is the single best idea in the document and it survives every defect above
   (once `Emit` is closed).

2. **`Completeness{Absent, Partial, Complete}` on both change images.** No surveyed system can say "this
   before-image is partial". A generic upsert sink that cannot distinguish partial from complete writes
   nulls over live data, and the failure is invisible until someone reads the table. Making completeness a
   declared, countable field — paired with `Meta.NoteChange(FieldChange{Nulled|Truncated|Rounded|Redacted|
   Unavailable})` — converts a silent corruption class into an assertable fact. This answers the decision
   space's open question 3 better than the question was asked.

3. **`Plan.Why []string` alongside `Plan.Guarantee`.** A negotiated guarantee with a per-factor
   explanation, carried in the status document, so the UI renders "at-least-once — because sink `http`
   declares no commit tier". Trap #16 (silent capability degradation) is the failure this prevents, and
   every prior system either degrades silently or shows a badge with no derivation. One `[]string` is a
   remarkably cheap fix for the most dangerous class of operator misunderstanding.

4. **Sink `Spec.Mappings` with `Directive` defaults resolved only by `engine.Resolve`.** Segment's model,
   correctly identified as the complete answer to "how does a generic UI configure a specialised sink" —
   constraint #1 and the frontend goal closed by one mechanism. The closed-AST ceiling (§3.9) is a real
   limit on *how far* it goes, not an argument against the shape.

5. **Import direction enforced by a custom `go vet` analyser, plus the grep test.** `connectors/*` cannot
   compile against `engine/`; `api/` cannot import `engine/`; `api/` and the frontend contain the string
   `"postgres"` zero times, asserted. This is the only mechanism in any of the surveyed systems that makes
   "the core has no connector-specific knowledge" a build failure rather than a code-review norm, and it
   is R8 applied to the property the whole project is graded on.

6. **`Registry` as a value type with `Clone` / `With` / `Without` over immutable maps, plus a default
   instance mutated by `Register`.** Benthos's shape, correctly identified, and it directly answers
   design-rules' complaint about module-scope global dedupe state that tests had to randomise ids to work
   around. Sandboxing and allow-listing fall out for free.

7. **Every unknown as a nil pointer in `Progress`, with a pinned fixture asserting the UI renders no
   zeros.** `Lag *float64` nil-because-no-`CapHead`, `Backlog *Backlog` nil-because-unknowable,
   `CheckpointAt *time.Time` nil-because-never-committed, `Catalog.Complete = false`,
   `PipelineStatus.Complete = false`. Design-rules' honesty requirement made machine-checkable, and the
   discipline "when a quantity is unmeasurable, omit the series entirely — never emit 0" carried all the
   way from the metric to the pixel.

8. **`Batch.Mark(split, cursor)` with zero records preceding it is a free heartbeat.** D6 item 10 asks for
   "a way to advance the durable position with no records emitted, so an idle stream's stored position
   never falls out of the upstream retention window", and calls it a per-connector hack in Benthos that
   belongs in core. The proposal does not claim this, but its `Mark` frame already provides it, because
   `Mark.Records`/`Bytes` count since the previous mark and zero is legal. Worth naming explicitly so a
   later proposal does not add a `Heartbeat` frame and violate R9.

Two more, briefly: the chunked-snapshot engine pre-fixes **both** documented Flink CDC scars (indexed
range filter from day one, finished-chunk set paged by reference), which is the only place in the three
proposals where a prior system's retrofit is anticipated rather than rediscovered; and
`SchemaID` as a content-addressed hash pinned on the split and deduplicated in enumerator state is the
right answer to the checkpoint-blowup problem Flink CDC solved with a `SchemalessSnapshotSplit` variant.

---

## 6. Verdict for the synthesiser

**Adopt the spine; do not adopt the extension surface as written.**

The parts to take are the record/identity model (§5.1–5.2 above), the capability pattern *as a pattern*,
the import-direction enforcement, `Spec` + `Mappings`, the read model's nil-means-unknown discipline, and
the checkpoint header/blob split. The parts that must be rebuilt before any interface is frozen are, in
priority order:

1. **An exported, data-first capability-resolution seam** replacing unexported `probe` + panic (§2.1).
   Without this the thesis is false and the whole angle's justification evaporates. This is the one defect
   that would change my score materially if repaired.
2. **Close `Batch.Emit` to connectors** (§2.2). One line, prevents silent data loss.
3. **Write the three missing descriptors** — transform, codec, buffer — with specs, capabilities, hosts
   and a `Buffer` durability declaration (§2.3). Until this exists, "extensible" describes two of five
   component kinds.
4. **A `Composite` runtime declaration** so the sanctioned topology mechanism does not push the
   contiguous-prefix resolver into every meta-component author (§3.2).
5. **Resolve `Fetch`/`AddSplits` and `Enumerator`/`Snapshot` concurrency in the type**, not in prose
   (§3.3, §3.4) — both currently push a concurrency algorithm into every connector, which is the exact
   cost this design's competitor angles will not pay.

Cross-proposal note: the proposal's own honest weakness #2 is correct and should be treated as a blocking
condition by the synthesiser regardless of which angle wins. **Conduit is the one system that has shipped
one Go interface satisfied by both an in-process and a gRPC connector, and no primary source was read.**
Defect §2.1 is precisely the kind of thing Conduit will already have hit and either solved or conceded. Do
not freeze a capability-resolution mechanism, a sink result handle, or a `Host` shape until that dossier
exists.

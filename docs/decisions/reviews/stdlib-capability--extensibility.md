# Review: `stdlib-capability` — EXTENSIBILITY lens

**Reviewer role:** hostile expert, single lens. I judge only whether *"to add a source or sink you
implement the interface and register it, done"* survives contact, whether a third kind of thing can be
added without core surgery, whether the interface set can absorb an out-of-process implementation, and
what happens at v2.

**Proposal reviewed:** `docs/decisions/proposals/stdlib-capability.md` (2853 lines).
**Normative reference:** `docs/design-rules.md` (R1–R13).
**Trap catalogue:** `docs/research/_decision-space.md` Part 3 (traps 1–21) and the coupling chains in Part 2.

**Score: 6 / 10.**

---

## 1. The verdict in one paragraph

The narrow claim is **true and better-defended than in any surveyed system**. A source is
`Spec/Open/Reader/Read/Close`; a sink is `Spec/Open/Writer/Write/Close`; a connector package is one
`init()`; there is no `switch cfg.Type`, no per-connector enum, no core import pointing at a connector,
and the registry is a value type. §0.1's honesty about the boundary of "zero core edits" is the most
precise statement of that constraint in any of the three proposals, and the capability-reification
machinery (`CapabilityEntry.Reason`, `CapSource`, `Refusal.Iface`, `Plan.StrategyWhy`) is the best set of
*extensibility affordances* in the document set — it converts the optional-interface model's worst
known property (invisible degradation) into a rendered sentence, which nothing in the prior art does.

The score is 6 rather than 8 because the lens asks four more questions than the constraint does, and
the proposal fails or half-fails all four. **The third kind of thing is not covered by its own
mechanism** — transforms, buffers and the four codec stages have `Spec`s but no `Tier`, no
`CapabilityID`, no `Spec` mask field, no table row and no `bound` field, so adding them is exactly the
core surgery the lens hunts for. **The one third-party extension axis is sealed shut** — `Facet` is
sealed by an unexported method while the const block advertises `FacetExt` for third parties.
**The serialisation subsystem the "add a sink is three methods" claim depends on is not wired to
anything** — `Writer.Write` takes `*RecordBatch`, so no interface in the document consumes an encoded
frame and none produces one, which means every byte-oriented source reimplements framing and
`StructuredWriter` declares a stage-skip that is unobservable because the stage was never in the path.
And **adding a transform to a working pipeline can permanently stall checkpointing**, because `Derive()`
carries exactly one parent `Origin.Seq` and the per-assignment prefix resolver waits forever for the
orphans.

Two of those are structural, not editorial. One is fatal on the lens's own terms: the mechanism that
makes the proposal *the* proposal is defined for two of the seven component kinds it registers.

---

## 2. What actually holds (stated once, so the defect list is not read as a total rejection)

These claims I tried to break and could not:

1. **No core file changes to add a source or sink.** `canal.Register(f any)` + `Registry.With` +
   `Spec.Name` as the only key. `cmd/canal` blank-imports; that is a `main` package. Confirmed: there is
   no name→behaviour map in `internal/api`, no connector string in the engine, and `Kind_` is a
   *component-kind* discriminator, not a connector discriminator. Trap 20 (plugin discovery by scanning)
   is avoided.
2. **Adding a new capability to core does not break existing connectors.** One interface, one
   `CapabilityID`, one table row, one `bound` field. An existing connector that does not implement the
   new interface probes absent and gets a `Reason`. This is the FLIP-372 rule executed correctly, and it
   avoids trap 9 (adding a method to a required interface) by making the growth vehicle explicit and
   stating it as a rule in the `OpenRequest` doc comment.
3. **Type assertions in exactly one file, as data.** `internal/capability/table.go` with `Probe`/`Bind`
   per row. This eliminates Benthos's nine-forwarders-per-capability tax (trap 11) for the in-process
   case, which is a genuine improvement over the prior art and not merely a restatement of it.
4. **Absence is explained.** `CapabilityEntry.Reason` required-when-absent, enforced by a core test
   walking every fixture entry; `Unlocks []string` next to it. This is the single most valuable
   extensibility affordance in the document: a connector author reading a dry-run report is handed a
   prioritised list of interfaces to implement, and `Refusal.Iface` names the exact Go type.
5. **The plan is a document.** `Plan{Strategy, StrategyWhy, Delivery, DeliveryWhy, AckPoint,
   BatchingFrom, Defaults[]}` mechanises R10 (labelled scaffolding) for defaults, and makes DG-2
   auditable rather than aspirational.

Everything below is subtraction from that baseline.

---

## 3. Fatal defects

### F1. The three serialisation stages have neither a producer nor a consumer, so "connectors implement transport only" is false

**At fault:** `Writer.Write(ctx, b *RecordBatch) (*WriteResult, error)` (§5) against
`Encoder.Encode(ctx, r *Record, dst []byte) ([]byte, error)`, `Framer.Scan(ctx, r io.Reader) (frame
[]byte, ack AckFunc, err error)`, `Compressor.Compress(ctx, in []byte) ([]byte, error)` (§13), and
`Reader.Read(ctx, dst *RecordBatch) error` (§4).

The decision-space's Chain D — *"'adding a sink is: implement three methods, register, done' holds
**only if** serialisation is not the sink's job"* — is the chain this proposal's headline claim rides on,
and §16's `serialization-boundary` row commits to it verbatim: *"sinks implement transport only (take an
already-encoded, already-framed, already-compressed payload and make a request)"*.

But the required sink method receives `*RecordBatch` — records, not a payload. Nothing in the document
takes the `[]byte` that `Encoder.Encode` returns. Nothing supplies the `io.Reader` that `Framer.Scan`
consumes; the only source-side output is `Read(ctx, dst *RecordBatch)`, which is already records.
`Compressor` has no caller at all. The three stages are registered, spec'd, capability-adjacent and
completely unreachable.

Three concrete consequences, each of which is the failure the decision was taken to prevent:

- **Every byte-oriented source reimplements framing.** A file source, a TCP source, an HTTP-body source
  and an S3-object source each need newline / length-prefix / CSV-row deframing, and there is no
  interface on which they can hand the engine a stream and receive records. So they each write it. That
  is *precisely* the N×M multiplication §16 claims "never multiplies", and it is the reason D2 rejected
  option (c).
- **`StructuredWriter` is unfalsifiable.** `StructuredWriter interface{ AcceptsStructured() bool }`
  "causes the engine to omit the Encoder stage — and to SAY SO in `Plan.Codec`". But every sink already
  receives structured records; the sink itself chooses whether to call `r.Value.Bytes(ctx)` or
  `r.Value.Structured(ctx)`. So the capability, its `CapabilityID` (`CapStructuredInput`), its table row,
  its admission check and its plan-document note all describe a state change that cannot be observed.
  A capability whose presence and absence are behaviourally identical is dead weight in a vocabulary the
  proposal itself says is too large (§17.2).
- **`Framer.Scan` returns an `AckFunc` that is never defined anywhere in the document** (verified by
  grep: one occurrence, at line 2176). It is also a *second, parallel* progress primitive next to the
  engine's per-assignment prefix resolver, with no stated composition rule. Benthos's
  `AutoAggregateBatchScannerAcks` reduction — which D2 item 2 explicitly says to copy so no connector
  author hand-writes fan-out ack counting — is cited in §16 as adopted and is absent from the interface
  set.

The honest repair is a fourth required-or-optional source shape (`StreamReader { Stream(ctx,
Assignment) (io.ReadCloser, error) }`) plus a sink shape that accepts frames, plus a rule about which
of the two paths a connector picks. That is a real addition to the required surface, and it is the cost
this proposal has not paid. Until it is paid, "add a sink: three methods" is true only because the
encoding work has been left un-homed rather than moved into core.

### F2. `Facet` is sealed, so the one third-party extension axis in the record model is closed, and the const block advertises the impossible

**At fault:** `type Facet interface { facetKind() FacetKind; FacetVersion() int }` and
`const ( FacetChange FacetKind = "change"; FacetExt FacetKind = "ext" // third-party, namespaced by Name )`
(§2.3).

`facetKind()` is unexported. Only types declared inside package `canal` can satisfy `Facet`. Therefore
`FacetExt`, documented as *"third-party, namespaced by Name"*, is unimplementable — and no `Name` field
and no `ExtFacet` carrier type exists anywhere in the document (verified by grep: zero occurrences of
`ExtFacet`).

The Facet mechanism is the proposal's stated answer to constraint #1: *"Facet is an optional typed view
attached to a record. It is how canal gets CDC semantics without putting a relational shape in the core
envelope."* It is the only place a connector can attach typed, non-generic, per-domain semantics that a
matching sink can read. Sealing it means:

- A Mongo source wanting to carry a resume-token facet, a Kafka source wanting a headers facet, an
  analytics sink pairing with a source over a shared typed view — **all require a patch to
  `canal/facet.go`**, i.e. a core edit for the exact category constraint #1's second sentence covers
  ("specialised UI/UX for less-generic connectors comes later and must NOT require core changes").
- `FacetOf[T Facet]` is constrained to a sealed set, so the generics use the proposal defends as
  "good" (type parameter on the accessor, not on `Record`) buys nothing a plain `Change` accessor would
  not: with a closed member set there is nothing to be generic over.
- The alternative reading — core ships an untyped `ExtFacet{Name string; Data Value}` carrier — is
  worse, because then `FacetOf[ExtFacet]` returns a bag and the "optional **typed** view" property is
  gone for every third party while remaining for core's one facet. Two mechanisms for one concept (R9).

`Signal` has the same seal (`interface{ signal() }`) and `MetricID` is closed (§17.11 owns this), which
in aggregate is the design's real shape: **superbly extensible along the one axis the constraint names,
closed to third parties on every other axis** — facets, signals, metrics, capability IDs, requirements,
outcome statuses, error classes. For phase 1 that is defensible. For the stated end-state ("possibly
fan-out/fan-in and transform chains", specialised connector UX) it is the central unresolved tension,
and the proposal does not name it as a weakness.

### F3. The capability system covers two of the seven registered component kinds; adding the third kind of thing is core surgery

**At fault:** `Spec{SourceCaps, ReaderCaps, SinkCaps, WriterCaps CapMask}` (§7), the `CapabilityID`
const block (§6.1), `capability.Probe(v any, tier canal.Tier, …)` (§6.3), `bound` (§6.4).

`Registry` exposes seven typed accessors: `Source`, `Sink`, `Transform`, `Encoder`, `Decoder`, `Framer`,
`Compressor`. §13 defines an eighth kind, `Buffer`. §13 also states *"Each has a Factory with a Spec,
registered exactly like a connector"*, and §13 defines optional transform capabilities: `Validator`,
`Prober`, `Classifier`, and `StatefulTransform`.

Now check the machinery against that:

| Needed for a kind to participate | Source | Sink | Transform | Buffer | Codec stages |
|---|---|---|---|---|---|
| `Tier` value | `TierSource` ✓ | implied | **none** | **none** | **none** |
| `Spec` declared-mask field | ✓ | ✓ | **none** | **none** | **none** |
| `CapabilityID` for its optionals | ✓ | ✓ | **`StatefulTransform` has none** | n/a | n/a |
| capability table rows | ✓ | ✓ | **none** | **none** | **none** |
| `bound` fields | ✓ | ✓ | **none** | **none** | **none** |
| `Kind_` value | ✓ | ✓ | ✓ | **none** | one shared `KindCodec` |

Verified by grep: `TransformCaps` — zero occurrences; `StatefulTransform` — one occurrence (its own
declaration, no `CapabilityID`, no row, no `bound` field); `KindBuffer` — zero occurrences;
`TierReader`/`TierSink` — zero occurrences (only `TierSource` is ever written).

So the answer to the lens's literal question — *does adding a third kind of thing require core
surgery?* — is **yes, and the proposal has not performed that surgery for the kinds it already ships**.
Registering a stateful transform today requires: a `Tier` constant, a `Spec.TransformCaps` field (a
change to the frozen contract struct — additive, but a contract change), a `CapabilityID`, a table row,
a `bound` field, and an admission path that knows a transform's checkpointed state must be written into
`Checkpoint` (which has `Assignments`, `Committables`, `SinkToken`, `SchemaEpoch` — and **no field for
transform state**, so `StatefulTransform.SnapshotState` has nowhere durable to go).

Adding a `Buffer` additionally requires a `Kind_` value (there is none), so a `buffer:` component-valued
config field — which §13 names as an example of the recursive-composition mechanism — cannot be
expressed today because `ComponentRef.Kind Kind_` has no member to filter by. And a single `KindCodec`
cannot distinguish which of four codec registries a `ComponentRef` draws from.

Separately: `Registry` grows one typed accessor per kind, and `Registry.With(f any)` must type-switch on
factory interface to choose a map — a switch that grows one arm per kind. That is not a per-*connector*
switch, so constraint #4 survives; but the proposal's absolute claim ("no switch statement anywhere")
is narrower than it reads, and a `Lookup(kind Kind_, name string) (any, bool)` over
`map[Kind_]map[string]any` would not grow at all.

### F4. `Transform` N→1 orphans positions and permanently stalls the prefix resolver

**At fault:** `Transform.Apply(ctx, in *RecordBatch, out *RecordBatch) ([]RecordOutcome, error)` (§13)
against `func (r *Record) Derive() *Record` (§2) and `AssignmentState.Resume` computed by "the engine's
prefix resolver, per assignment, from acks" keyed on `Origin.Seq` (§3, §14c step 3).

`Derive()` is documented as *"the only way a transform can create a record that the ack graph can
account for… Origin is copied verbatim, including Seq, so N records derived from one source record share
one position and the prefix resolver treats them as a unit."* That handles 1→N and 1→0 correctly.

`Transform.Apply`'s advertised vocabulary explicitly includes **regrouping** — N→M, and therefore N→1.
There is no `DeriveFrom(parents ...*Record)`. A transform that merges records with `Seq` 5, 6 and 7 into
one output record can carry exactly one parent's `Origin`. Seqs 6 and 7 then never reach terminal
disposition, and the longest-contiguous-committed-prefix resolver for that assignment **stops at 4
forever**. The pipeline continues writing records and never advances a checkpoint. Restart replays from
Seq 4 indefinitely; `MCheckpointAge` — the proposal's own "best single health metric" — climbs without
bound while `Connected: True` and `Progressing: True` are both honest.

The escape the type system offers is worse: the transform returns `[]RecordOutcome` keyed by input
`RecordID`, and the closed six-member `OutcomeStatus` set has no member meaning "merged into another
record". Reporting `OutcomeWritten` for a record that was merged away makes `MRecordsWritten` count
records that never reached a sink, which is the exact `written != committed` conflation §9's `MetricID`
list was designed to prevent.

This is the price of replacing Benthos's ack *graph* with a single-parent `Origin` plus a scalar prefix
watermark. The decision space warned about this coupling directly (D13 item 6: *"1→N is 'ack when all
children ack'… this is why Benthos's tiny ack primitive is preserved even though canal adds a position
model: it is what makes arbitrary composition free"*). This proposal added the position model and did
not preserve the composition property. On the extensibility lens that is fatal, because the failure mode
is *introduced by adding the third kind of thing to an otherwise-working pipeline*, silently, with no
admission-time check available (the engine cannot know a transform will regroup).

### F5. `PlanStrategy` selection reads only source capabilities, so `StrategyChunked` is admissible against an append-only, non-idempotent sink

**At fault:** the §11 admission table row *"`Strategy` | strongest supported ≤ pinned"*, `StrategyChunked`
(§11) and `Requirement`'s `ReqParallelBackfill = CapPlan ∧ CapChunk ∧ CapCompare ∧ CapSeek`.

`StrategyChunked` runs the Offset Signal Algorithm: buffer a chunk, replay the log from `LOW` to `HIGH`,
upsert into the buffer, emit. D7 states the precondition without ambiguity, twice:
*"Chunked-snapshot output is keyed upserts, not appends. Flink CDC's exactly-once claim depends on the
sink upserting by key. **Make it an explicit capability requirement checked when the pipeline is
submitted.**"* And in the hard-couplings list: *"Chunked snapshot (D7) ⇒ the sink **must** upsert by
key; that is a submit-time capability check, not a runtime discovery."*

Every `Requirement` in this proposal that touches the sink is delivery-tier (`ReqEffectivelyOnce`,
`ReqExactlyOnce`). `ReqParallelBackfill` and `ReqResumableBackfill` name only source capabilities. And
strategy selection is described as "strongest the capability set supports" with no sink term in the
conjunction. `admit.Request` carries `SinkCaps` and `WriterCaps`, so the check is *possible*; it is not
*specified*, and the table that specifies the four derived values omits it.

Concrete consequence: a Postgres CDC source (`Planner+Chunkable+Comparable+Replayable+Seekable`) paired
with a plain S3/parquet append sink is admitted at `StrategyChunked` with `Delivery: AtLeastOnce` and no
refusal. The `(high, end)` backfill interval "may duplicate snapshot data" — the proposal quotes this
lineage in §16 — and those duplicates land as extra rows in an append-only destination with no way to
reconcile them. The operator was refused nothing, warned about nothing, and the `Plan` document says
`Strategy: Chunked, StrategyWhy: "source implements Planner+Chunkable+Comparable+Replayable"` — a true
sentence about the wrong half of the pipeline.

This is a catalogued trap walked into by omission, and it is on-lens because it is the failure mode of
*combining* two independently-added connectors, which is what extensibility means.

---

## 4. Major defects

### M1. Reader-tier capabilities cannot be probed before admission, and DG-1's no-nil-check rule turns a mis-declaration into a panic inside core

**At fault:** `Source.Reader(ctx, req OpenRequest) (Reader, error)` requiring
`OpenRequest.Assignment`; `admit.Request.ReaderCaps canal.CapabilitySet // probed from a dry-run Reader
on a probe assignment`; the reader-tier `CapabilityID`s (`CapPosition, CapPhases, CapBounded, CapAck,
CapLag`); `CapabilitySet.Resumable = CapPosition && CapSeek`; and §6.3's *"There is no interface, no
assertion, no nil check."*

The dependency is circular: admission needs `ReaderCaps` → `ReaderCaps` needs a `Reader` → a `Reader`
needs an `Assignment` → an `Assignment` comes from the plan → the plan comes from admission. §17.4 owns
the problem and offers two exits, neither committed: a dry-run that "opens a real connection and
constructs a real reader that is then thrown away, which is a side effect at admission time — precisely
the thing I forbade in `Open`", or "declare reader-tier capabilities in the `Spec` only".

The second exit is worse than the proposal admits, because of DG-1. If `Spec.ReaderCaps` is a *bare
declaration*, then for five of the twenty-nine capabilities — including `CapPosition`, one half of
`Resumable`, the property the entire checkpoint story rests on — DG-3 ("declared must equal
implemented") is unverifiable for third-party connectors. The engine then executes, per §6.3's own
example:

```go
if br.caps.Resumable {
    cur := br.position()          // a function field. never nil when caps.Resumable is true.
}
```

`br.position` is nil whenever a connector declared `CapPosition` and returned a `Reader` that does not
implement `Positioner`. Because DG-1 forbids nil checks on connectors in the engine, this is a **nil
function call panic inside `internal/engine`**, attributed to core, from a third-party declaration
error — instead of the `PermanentContract` refusal every other declaration mismatch produces. The
mechanism invented to make nil unreachable (`Bind` → function fields) reintroduces nil precisely where
the probe cannot reach, and the rule that makes it safe (probe is authoritative) does not hold for a
sixth of the vocabulary.

The first exit (dry-run) is also on-lens: it means the frontend's `POST /dryrun` endpoint opens real
connections and constructs and discards real readers, so "cheap, pure, no I/O, no instance" — the
property that makes the UI able to render a connector that has never run — is false for that endpoint.

### M2. Deleting a capability from core is a breaking change for every connector that declared it — the opposite of §17.2's claim

**At fault:** DG-3's `overdeclared = declared \ probed → a PermanentContract refusal`, against §17.2's
mitigation *"deleting a capability is a table row plus a deprecation — much cheaper than deleting a
required method"*.

Trace it. Core removes `CapChunk` and `canal.Chunkable`. A third-party connector's `Spec.SourceCaps`
still has that bit set (it is a `CapMask uint64` literal compiled into the connector). At admission,
`declared` contains a bit with no matching table row; `probed` cannot contain it. `overdeclared` is
non-empty → `PermanentContract` refusal → **the connector does not run at all**, for a capability it no
longer needs.

The connector must be edited and recompiled. That is exactly the cost profile of removing a required
method, which is the thing §17.2 claims it is cheaper than. The fix is small (treat unknown declared
bits as a warning, not a refusal) but it is not what DG-3 says, and DG-3 is stated as one of the three
rules that "are the whole design".

The same asymmetry is a standing ergonomic tax even without deletion: **adding a capability to a
connector requires two edits in the connector** — implement the interface *and* set the bit in
`Spec.SourceCaps` — and forgetting the second produces `CapSrcUndeclared` → a refusal. Adding a
capability makes a working connector stop working until a bitmask is updated. `Spec.*Caps` and the probe
are two representations of one concept reconciled by a function, which R9 names as *"evidence of a
modelling error"*. The information need behind `Spec.*Caps` is real (the UI must know before an instance
exists), but the enforcement direction is backwards: declared-not-probed should be a diagnostic on the
*UI's* promise, not a refusal of the *pipeline*.

### M3. `CapabilityID` is a tier-grouped `iota` that is persisted raw in operator-signed records

**At fault:** the §6.1 const block (`CapValidate CapabilityID = iota + 1`, grouped `// Source tier`,
`// Reader tier`, `// Sink tier`) against `Downgrade{Missing []canal.CapabilityID}` — "persisted with
the pipeline, stamped with who and when" — and `Refusal{Missing []canal.CapabilityID}`, and
`Result.CapabilityReports []canal.CapabilitySet` — "stored verbatim, served to the UI".

Adding a source-tier capability in its natural position (end of the source group, before
`// Reader tier`) renumbers every reader-tier and sink-tier ID. Any persisted `Downgrade` from before
the upgrade then names a different capability: an operator-signed waiver of "exactly-once because the
sink lacks `CapToken`" reads after upgrade as a waiver referencing `CapIdempotent` or whatever now
occupies that ordinal. This is trap 19 ("package/type names as a permanent wire contract") in its
numeric form, and the proposal already knows the remedy — `CapabilityEntry.Name string` is described as
a *"stable machine token"* — but `Missing` carries IDs, not names, and `CapMask` bit positions are the
same ordinals compiled into every third-party `Spec`.

Two additional constraints follow from the same choice and are unstated: `CapMask uint64` caps the
vocabulary at **64 capabilities forever** (29 are spent on day one, and §17.2 anticipates churn), and a
`CapMask` compiled into a third-party connector is a positional wire contract between two independently
versioned binaries the moment out-of-process connectors exist.

### M4. There is no contract/API version anywhere

**At fault:** `Spec{Version string // semver of the connector itself}`. Verified by grep: `APIVersion`
and `api_version` — zero occurrences in the document.

D9 makes this an explicit requirement: *"Include an explicit `(api_version, capabilities)` handshake at
init. Benthos's gRPC boundary has none, so an older plugin cannot know it is missing a required
semantic."* The illustrative appendix carries `Capabilities{APIVersion int; …}`. This proposal has
`Blob.Version`, `Cursor` version via `Seekable.CursorVersion()`, `Facet.FacetVersion()`, `Spec.Version`
(the connector's), `Checkpoint.Generation`/`Epoch`, and `Config.Generation()` — five version concepts,
none of which is the contract's.

The consequence is specific to this design's growth strategy. Because required interfaces are frozen,
canal v2's changes will be *semantic*, not signature-level — and this proposal invites exactly that
class of change by reifying semantics into values. Examples that compile unchanged and behave wrongly:
`Write` returning nil meaning "accepted" rather than "durable" under a new default; `Read` becoming
legal to call concurrently; `Close` becoming callable more than once; `Origin.Cursor` changing from
"position after this record" to "position of this record". A v1 connector linked into a v2 binary
satisfies every interface and violates the new contract, and nothing detects it. In-process this is
merely dangerous; across the future RPC boundary it is undetectable, which is the failure D9 named.

### M5. `KindStream` makes `Record` unserialisable, breaking the durable buffer, the DLQ, and the wire form

**At fault:** `KindStream // a nested, lazily-read sub-stream (database/sql's cursor Value, generalised)`
and `Stream struct{ /* lazily-read nested record stream; closed when the parent is closed */ }` (§2.2).

Four things in this proposal require a `Record` to be a value that can be written to bytes and read back
later, in another process:

- `Buffer.Push(ctx, b *RecordBatch)` where §15/§16 commit to a durable buffer that "takes ownership of
  the ack handles on write" — a disk buffer must persist the batch.
- `DeadLetter{Record *Record; …}` written "durably before the record is considered terminal" (§14c).
- Chain E's wire-shippability: *"the record has a wire form"* is one of the four conjuncts that make
  constraint #3's future possible.
- `Encoder.Encode(ctx, r *Record, dst []byte)` — a codec must encode every `Value`.

A live, lazily-read, parent-lifetime-scoped sub-stream can satisfy none of them. It also collides with
§2.4's ownership contract: the engine "does not recycle a batch until every record in it has reached a
terminal disposition", but a `Stream` value's data is *outside* the batch and is invalidated when the
parent closes, so a sink that returns `OutcomeAcceptedNotDurable` and defers reading a nested stream to
`Flush` reads from a closed handle.

This is one const and one struct, cargo-culted from `database/sql` (where a cursor-as-`Value` exists
because a driver may return one and nothing downstream persists it) into a system whose defining
features are persistence and relocation. It should be deleted; if nested lazy sub-streams are genuinely
needed, they belong as a distinct source-side capability, not as a member of the record's value lattice.

### M6. `Payload`'s lazy views depend on pipeline-scoped codecs, so a connector cannot be tested in isolation and `Encoder` cannot serve them

**At fault:** `func (p *Payload) Bytes(ctx context.Context) ([]byte, error)` — *"encoding the structured
view on demand using **the pipeline's configured Encoder**"* — and `Structured(ctx)` symmetrically,
against `Encoder.Encode(ctx, r *Record, dst []byte)` and `Record.Key *Payload`.

Two problems.

First, **`Payload` is not a self-contained value.** Either it holds a hidden reference to pipeline codec
state — which makes `Record` unserialisable (compounding M5), makes `canalkit.BytesRecord(...)`
insufficient to build a usable record, and means a `Record` cannot cross a process boundary — or the
codec is fetched from `ctx`, which is context-as-dependency-injection. Either way the extensibility
consequence is direct and is one of the lens's explicit questions: **a sink or transform cannot be unit
tested without engine codec plumbing.** A sink's test calls `r.Value.Structured(ctx)` and gets an error
or a panic unless the test constructs whatever the engine puts in `ctx`. `canaltest` advertises
`AssertDeclarations, AssertReaderContract, AssertRetrySafety, AssertCursorRoundTrip,
GoldenCapabilityReport` — no codec harness, no `Runtime` fake, and no constructor for `Config` (which
is `struct{ /* immutable tree */ }` with no exported builder anywhere in the document, so
`Factory.Open(cfg Config)` cannot be called from a connector's own test).

Second, **the signature does not fit.** `Payload.Bytes()` needs `encodePayload(Payload) ([]byte, error)`.
The registered stage is `Encode(ctx, *Record, dst)` — record-shaped. `Record.Key` is also a `*Payload`,
and encoding a key by handing a record-shaped encoder the whole record is meaningless. The lazy dual-view
cannot be implemented against the declared `Encoder` interface as written.

### M7. Seven capability signatures are wire-hostile, so "the engine cannot tell the difference" is false for a quarter of the vocabulary

**At fault:** `CheckpointRequester.CheckpointRequested() <-chan struct{}`;
`Classifier.Classify(err error) Class`; `Comparable.CompareKeys(a, b []Value) (cmp int, ok bool)`;
`Positioner.Position() Cursor`; `Phased.PhaseLabel() string`; `ChunkIter.Cursor() Blob`;
`MetaComponent.Children() []ComponentHandle`; plus `Runtime` as an interface passed *into* the connector.

§6.2 claims the `Bind`-to-function-fields mechanism means *"an out-of-process connector reports its mask
over the wire, the core builds the identical struct, and every downstream line of engine code is
unchanged… The engine cannot tell the difference and has no code path that could."* D9's checklist is
`(ctx, serialisable request) → (serialisable response, error)`, which Connect fails on five counts and
Benthos passes.

Measured against it:

- `CheckpointRequested() <-chan struct{}` returns a **Go channel**. Categorically unshippable; it needs
  a server-streaming RPC and a host-side pump, i.e. a hand-written forwarder — the tax `Bind` exists to
  abolish.
- `Classify(err error) Class` takes a **Go error value**. A remote connector receives a wire error, not
  the host's `error`, so the semantics invert (the host would have to ship the error *out* and get a
  class back — possible, but it is a different signature).
- `CompareKeys` has **no `ctx` and no `error`**, so a transport failure cannot be reported, and it is
  called by the engine's indexed range filter *inside a binary search, per streamed record*. Remotely
  that is O(log n) round trips per record. DG-1 forbids type assertions on hot paths and says nothing
  about capability *calls* on hot paths; `Bind` makes the local and remote cases syntactically
  identical and performance-categorically different, which is the failure mode of a seam that hides too
  much.
- `Position() Cursor`, `PhaseLabel() string`, `ChunkIter.Cursor() Blob` — no ctx, no error.
- `Children() []ComponentHandle` returns handles.
- `Runtime` is an interface the *core* implements and hands *in*: `Logger() *slog.Logger`,
  `Counter(MetricID) Counter`. For a subprocess these are reverse calls needing host-side stubs per
  method, so "the entire cost of satisfying constraint #3's future requirement… is paid in ONE file"
  understates it by the whole `Runtime` surface.

The in-process value of `Bind` is real and I credit it in §2. The claim that it *also* buys the
out-of-process future for free is not supported by the signatures as written, and constraint #3 is a
hard constraint. Every one of the seven is repairable by adding `ctx`/`error` and replacing the channel
with a poll or a `Signal` — but that repair must happen *before* the contract freezes, which is the
whole point of the constraint.

### M8. `StrategyChunked`'s precondition and `ReqParallelBackfill`'s conjunction disagree, and the lattice is admittedly non-total

**At fault:** `StrategyChunked` — *"Planner + Chunkable + Comparable + Replayable"* — against
`ReqParallelBackfill = CapPlan ∧ CapChunk ∧ CapCompare ∧ CapSeek` and `ReqResumableBackfill = CapChunk ∧
CapSeek`, plus §17.12's own admission that the lattice is proven total only for four enumerated
combinations.

Two consequences.

First, `StrategyChunked` without `CapSeek` is reachable by the stated precondition, and §14(b)'s resume
path depends on `Seekable` (*"Because `Seekable` is declared, the reader resumes strictly after the
cursor"*). Without it, the engine has written `AssignmentState.Resume` cursors for in-flight chunks that
it cannot honour, so a crash restarts each in-flight chunk from its range start and re-emits its prefix —
duplicates whose reconciliation depends on the sink capability F5 shows is unchecked.

Second, and more important for the lens: **a capability that is present but unexercised has no
explanation anywhere.** `CapabilityEntry.Reason` is required only when `Present == false`. A source
implementing `Chunkable + Comparable + Replayable` but not `Planner` falls to `StrategyOpaque`; the
capability report shows `✓ chunk probed`, `✓ compare probed`, `✓ replay probed`; the plan says
`Strategy: Opaque`; and `StrategyWhy` is **free text**, not a structural link, so nothing tells the
operator (or a test) that three implemented capabilities are inert. The entire anti-silent-degradation
apparatus is asymmetric: it explains absences and is silent about unused presences. Given that
capability combinations are the extensibility surface, the missing half is the half that matters as the
connector ecosystem grows.

### M9. `CapabilityReporter` needs network I/O at a layer that forbids it, and `admit.Admit` is declared pure

**At fault:** `CapabilityReporter.Capabilities(ctx) (mask CapMask, notes []CapabilityNote, err error)`;
`SourceFactory.Open` — *"must not perform network I/O… It is called by the UI to probe capabilities and
by admission to compute a plan; both must be cheap and side-effect free"*; `admit.Admit(req Request)
Result` — *"It is pure: same inputs, same Result, no I/O"*; and §11 — *"It runs before any connection is
opened."*

The proposal's own worked example of masking (§14d) is *"the configured user lacks `pg_read_all_stats`,
so replication lag cannot be measured"* — a fact that requires a catalog query. So `Capabilities(ctx)`
does network I/O, and it must run before `Admit` (whose `Request` carries already-computed
`CapabilitySet`s). That I/O therefore happens in an unnamed layer between `Open` (I/O forbidden) and
`Admit` (pure), and it is triggered by the UI's dry-run endpoint. The layering claim — impossible
pipelines refused at submit time by a cheap pure function with no connections opened — is true of
`Admit` and false of admission. Combined with M1's dry-run reader, "the UI can render and validate a
connector without touching the remote system" does not hold.

### M10. `ModeSet` — the mechanism that replaces "pipeline types" — is fabricated for any source without `Discoverer`

**At fault:** `Stream.Modes ModeSet` living only on a discovered `Stream`; `Catalog.Synthesised bool`;
`Discoverer` optional (§16's stated disagreement with the distiller).

§16's `snapshot-model` row makes the operator-facing model Airbyte's orthogonal per-stream `ModeSet`,
and that is what discharges the end-state goal "multiple *types* of pipelines" without a pipeline-type
enum. `ModeSet` is a field on `Stream`, and `Stream`s come from `Discoverer.Discover`. A source without
`Discoverer` gets an engine-synthesised catalog — and the engine cannot know whether the synthesised
stream supports `FullRefresh`, `Incremental`, `FullThenIncremental` or `CDC`. Whatever it writes there is
invention.

R10 requires scaffolding to be *labelled* **and** *"exercised by a test asserting it matches the thing it
stands in for"*. `Catalog.Synthesised: true` labels the container; the `ModeSet` inside it is unlabelled
fabrication with nothing to test it against. The design consequence is that the answer to the
"multiple pipeline types" goal is available only to sources that implement the capability the proposal
chose to keep optional in order to protect its five-method headline — and the fallback silently invents
the data the operator model is built on.

---

## 5. Minor defects

- **M-a. Typo in a frozen exported interface.** `PartialTolerant interface{ TolerantesPartial() bool }`.
  Trap 19 is literally *"Flink CDC's `…meta.wartermark` typo has been unfixable in a public path for
  years."* This is that, in the document proposing to freeze the contract package.
- **M-b. R9 vocabulary sprawl inside the contract package.** `Disposition` (retry terminal) and
  `Disposition2` (buffer push result) — a type named `Disposition2` in a package whose stated virtue is
  one concept, one vocabulary. `Kind` (value kind: `KindNull`…`KindStream`) and `Kind_` (component kind:
  `KindSource`…`KindCodec`) — two types both called "Kind", disambiguated by a trailing underscore,
  with constants sharing a prefix. `Availability` references `Change.Omitted`, a field `Change` does not
  have.
- **M-c. `Read` returns a bare `error` while `Write` returns a growable `*WriteResult`.** This asymmetry
  is *why the capability count is 29*: `Positioner`, `Bounded`, `Phased.PhaseLabel` and `LagReporter`
  all exist to convey batch-level facts that a `ReadResult` struct would carry as fields — additively,
  wire-shippably, and without four table rows, four `bound` fields and four `Unlocks` sentences each.
  §17.2 names the count as the design's real cost without identifying this as its cause.
- **M-d. `Config.Get[T]` silently zero-values in production.** *"there is no error return… a missing or
  mistyped path is a PROGRAMMING error… and panics in tests via canaltest"* — which implies it does not
  panic outside tests. `Get[time.Duration](cfg, "intervl")` returns 0 and the connector polls in a tight
  loop. `T` is also unlinked to the declared `FieldType`, so `Get[int](cfg, "interval")` on an
  `FTDuration` field has no check. This is the most-called connector-facing function in the design.
- **M-e. `Field.Default any` plus a parallel `FieldType`.** `Default: 30 * time.Second` round-trips
  through JSON as `30000000000` and returns as `float64`; the browser and Go disagree on the default's
  type with nothing structural preventing it. R8's *"shared constants are generated from one source;
  drift is prevented structurally"* is the rule, and the design-rules' own `duplicateIds: null` incident
  is the same shape of invisible marshalling drift.
- **M-f. `Phased` duplicates the snapshot-handoff concept for an analogy that does not transfer.**
  `RowsNextResultSet` exists in `database/sql` because one SQL batch yields several result sets — an
  unrelated problem. Here it buys a whole `PlanStrategy` arm, three `bound` fields, a `CapabilityID`, a
  distinct checkpoint-boundary semantics, and a genuine "which do I implement?" question for connector
  authors whose only guidance is that the other one is better. A connector implementing both has dead
  code it still had to write, test and document.
- **M-g. `Runtime` is absent from `Factory.Open`,** so `Validator.Validate`, `Discoverer.Discover`,
  `Planner.Plan` and `CapabilityReporter.Capabilities` have no logger, no metrics and no `Signal`.
  Each will need `Runtime` threaded into its own request struct — four more growth events.
- **M-h. `StatefulTransform.SnapshotState` has no home in `Checkpoint`.** `Checkpoint` has
  `Assignments`, `Committables`, `SinkToken`, `SchemaEpoch`. Adding transform state is a new field on the
  core checkpoint struct — additive, but it is a core edit caused by the third kind of thing, and F3
  already shows the rest of that kind's machinery is absent.

---

## 6. What this proposal does better than any plausible alternative

Ranked by how much I would fight to keep them in a synthesis, even if this proposal loses:

1. **`CapabilityEntry.Reason` required-when-absent, with `Unlocks[]`, enforced by a core test.** No
   surveyed system explains an absent capability. This single field converts optional-interfaces-by-type-
   assertion from a silent-degradation hazard into the best page in the UI *and* into a prioritised
   connector-authoring task list. It is the correct answer to D9's "a flag without methods is worthless"
   objection from the opposite direction, and it costs one string.
2. **`Refusal{Requirement, Missing, Connector, Iface, Message, FixHint, Path}`.** Naming the exact Go
   interface a connector author would implement to make the pipeline legal is the highest-leverage
   extensibility affordance in any of the three proposals. Transplant verbatim.
3. **`Bind` collapsing a type assertion into a function field at one named moment.** Even where the
   remote claim overreaches (M7), this is the right in-process mechanism and it is the correct execution
   of "assert in exactly one place" — it structurally eliminates Benthos's nine-forwarders-per-capability
   tax rather than merely warning about it.
4. **`CapSource` as a closed set (`Probed | Absent | Masked | DeclaredRemote | Undeclared`).** Making
   *how the core knows* a first-class value is what makes a declared/implemented cross-check auditable
   instead of aspirational, and `Undeclared` is the diagnostic that catches the most likely connector
   authoring mistake.
5. **`admit.Downgrade` as an operator-signed, durable, config-declared waiver that raises
   `Degraded: True` for the pipeline's whole life.** Trap 16 (Vector's silent acknowledgement
   degradation) has no good fix in the literature; "refuse, or waive loudly and permanently, never
   degrade quietly" is better than refuse-only because it keeps the escape hatch auditable.
6. **`Plan` as an immutable derivation document** with `StrategyWhy`, `DeliveryWhy`, `BatchingFrom` and
   `Defaults[]` each labelled with its source. This mechanises R10 for every default in the system and is
   what makes DG-2 checkable. (Make the "why" fields structural rather than free text — see M8.)
7. **`ErrCapabilityDeclined` legal only during a named negotiation window.** Bounding
   `driver.ErrSkip` to a phase, so a decline becomes an admission-time `Reason` and a post-`Start`
   decline becomes a contract violation, is the right correction to the one part of `database/sql` this
   whole angle should *not* have copied.
8. **Unexported `Record.origin` with an `Origin()` accessor and `Derive()` as the only mint path.**
   Structural KIP-793 immunity: a transform cannot corrupt checkpoint identity because it cannot reach
   the field. (The N→1 gap in F4 is a missing `DeriveFrom(parents…)`, not an argument against the
   sealing.)

---

## 7. Score reasoning

**6 / 10 on extensibility.**

Above 5 because the constraint as literally written is satisfied, and satisfied better than by any
system in the research: five required methods per role, one `init()`, zero core edits, no switch on
anything a connector supplied, additive capability growth, and an absence-explanation mechanism that is
genuinely novel. The `Refusal`/`Reason`/`Plan` triad makes canal's extensibility *legible to connector
authors*, which is a step past merely being possible.

Below 7 because the lens asks five questions and the proposal answers one:

| Lens question | Verdict |
|---|---|
| Core must know about a specific connector? | **No.** Clean. |
| Switches / registries growing per connector? | **No** per connector; **yes** per component *kind* (F3, M-b). |
| Third kind of thing without core surgery? | **No.** Transform/buffer/codec have Specs and nothing else (F3). |
| Third-party capability without core edit? | **No.** `Facet` sealed, `CapabilityID` closed, `MetricID` closed (F2). |
| Interface can absorb out-of-process later? | **Partly.** Seven signatures cannot (M7). |
| Connector testable in isolation? | **Not as written.** No `Config` builder, no `Runtime` fake, `Payload` needs pipeline codecs (M6). |
| Versioning story for v2? | **Absent.** No contract version; capability deletion breaks connectors (M2, M4). |

And below 7 because two failures are triggered by *composition*, which is what extensibility means in
practice: adding a transform can permanently stall checkpointing (F4), and combining an
independently-added source and sink can select a chunked snapshot whose correctness precondition is
never checked (F5). A design whose headline is "adding things is free" must be hardest to break exactly
there.

Not below 6 because every fatal finding is repairable within the proposal's own idiom, and most of the
repairs are additive: unseal `Facet` with an exported marker; give transform/buffer/codec their tiers,
IDs and rows; add `DeriveFrom(parents…)` and make `Origin` a set; add a sink term to strategy selection;
add `StreamReader`/frame-accepting shapes so the codec stages have a path; add `APIVersion`; delete
`KindStream`; add `ctx`/`error` to the seven wire-hostile signatures. None of that requires abandoning
the capability-reification core, which is the part worth keeping.

The honest summary for the synthesiser: **this proposal has the best extensibility *affordances* and an
incomplete extensibility *mechanism*.** Take `Reason`/`Unlocks`/`CapSource`/`Refusal`/`Plan`/`Downgrade`
wholesale. Do not take the claim that the mechanism generalises past sources and sinks, or that `Bind`
buys the process boundary, without doing the work those claims assume is already done.

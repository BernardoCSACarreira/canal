# Rule-compliance audit — the delivered interface set against `docs/design-rules.md`

**Status: DRAFT, and a DATED SNAPSHOT.** Not normative (design rule R12). This document makes no
decision. It reports verdicts and evidence, and proposes a priority order. Anything adopted from here
must move into `docs/architecture.md` or a numbered ADR to become binding.

> **What has changed since this audit ran at commit `a83f88e`.** The verdicts below are left exactly
> as they were found, because an audit rewritten to match later work stops being evidence of
> anything. Four of its findings have since been addressed:
>
> - **R3 (violated)** — `Pipeline.Run` is implemented (`internal/engine/run.go`), `cmd/canal` exists,
>   and `cmd/canal/main_test.go` `SIGKILL`s the real binary three times against a 300,000-line input
>   and asserts no record is skipped.
> - **"no durable store exists"** — `pkg/store/wal` is a real `store.StateStore`: CRC32C framing,
>   fsync before `Set` returns, per-key CAS and epoch fencing, and a torn tail truncated rather than
>   refused.
> - **"no encoder, framer or compressor implementation exists"** — `pkg/codec` ships `raw`, `json`
>   and `newline`, and `internal/engine/codec.go` resolves them from the node's codec block.
> - **The ten delivered-code defects** in the sections below were each fixed with a test first proven
>   to catch the bug; they are listed in the README's before/after table.
>
> Everything else it found — no `store.Coordinator`, no transforms or buffers, no retry routing, the
> thin `pkg/` test coverage — is still open.

**Scope.** `pkg/` (the connector-author-facing interface set), `internal/` (engine, ledger, the two
example connectors, the eight hostile stress connectors), `docs/architecture.md` (declared NORMATIVE),
the 31 ADRs, and `docs/design-rules.md` (R1–R13). Commit `a83f88e`, module
`github.com/BernardoCSACarreira/canal`, Go 1.23.6.

**Baseline, established independently and not re-litigated below.** `gofmt -l .` is clean;
`go build ./...`, `go vet ./...` and `go test ./...` all pass, nine packages green. "It compiles" is
not a finding anywhere in this report.

---

## How this was checked, and how much to trust it

Nineteen independent audits were run: thirteen against the normative rules R1–R13, and six against
load-bearing claims C1–C6 made about the delivered set. Each audit was required to carry, for every
finding, either a `path:line` citation the auditor had actually opened or a throwaway Go program the
auditor had actually compiled and run. Probes were written outside the repository; the working tree
was clean before and after every audit and is clean now.

Every finding was then attacked by a hostile verifier applying three lenses: **is the citation real**
(re-open it), **is it already handled elsewhere** (search for the compensating mechanism), and **does
it actually matter** (what breaks, for whom). Of 213 findings, **100 were killed** and **113
survived**. Three auditor verdicts were overruled by their own verifier — R6, R7 and C2 were all
moved *toward* satisfied, not away from it.

**The methodological limit, stated plainly.** The original design gave every finding three
*independent* skeptics. That fan-out cost 150 agents against a session limit, so verification was
consolidated to one verifier per check carrying all three lenses. **Coverage is complete — every
finding was attacked on all three grounds — but the independence is weaker than three separate
votes.** A single verifier that shares a blind spot with its auditor will preserve a wrong finding,
and a single verifier having a bad day will kill a right one. Read individual minor findings as
"one careful second opinion", not as "three-way consensus". The fatal and major findings in this
report were additionally spot-checked by hand while writing it; those are noted where they occur.

Where the documentation and the code disagree, this report treats the disagreement itself as the
finding. `docs/architecture.md` is 7,001 lines of confident prose written by the same agent that wrote
the code. The code is what ships.

---

## Verdict table

| # | Rule / claim | Verdict | Basis (one line) |
|---|---|---|---|
| R1 | Topology is data, never schema | **partially-violated** | Graph is genuinely data — no ordinals, no fixed count — but dedupe is modelled twice and `Build`'s node switch has no `default`. |
| R2 | Canonical record model decided first, transport-independent | **partially-violated** | The spine is real, complete and imports no transport; its declared invariants (immutable-after-admission, unforgeable provenance) have no enforcement point. |
| R3 | One end-to-end path before any breadth | **violated** | `Pipeline.Run` is a one-line error return, no durable store exists, and `go list` finds no `main`. No record has ever moved. |
| R4 | An acknowledgement means durable | **violated** | `applyWaivers` disables the data-loss guard on any signed waiver; a `PrunesOnCommit` source is accepted against a `DurabilityNone` store; `Ledger.send` races `Close` and panics. |
| R5 | Dedupe keys scoped, committed after the write | **partially-violated** | Scope is genuinely closed (tenant is a struct field, one durable substrate); the submit-time gate has two holes and `Key.String` collides on unescaped `/`. |
| R6 | A buffer without a rejection path is not a buffer | **satisfied-with-caveats** | The buffer contract is clean: one interface, typed refusal, no unbounded `WhenFull`, `AddBuffer` panics on `Bounded:false`. The backpressure *implementation* has two unbounded paths. |
| R7 | If the design says retry the failures, name them | **satisfied-with-caveats** | `Failed []fault.RecordFault` keyed on `RecordID` is exactly what R7 demands. `Reconcile` checks cardinality only, so a malformed naming passes. |
| R8 | Conformance asserted against real responses | **violated** | Eight `[]uint8`-enum capability fields serialise as base64 **today**, in both shipped descriptors; nil slices marshal to `null`; the generator that would catch it does not exist. |
| R9 | One concept, one vocabulary | **partially-violated** | Several concepts are correctly singular (`Negotiated`, `Guarantee`, `Position`, `BatchPolicy`); the capability table is typed twice and has drifted, and `Backlog` exists twice with three divergences. |
| R10 | Scaffolding labelled, tested against what it stands in for | **partially-violated** | `memstore` and the `Run` stub label themselves exemplarily; `pkg/connectortest` claims inertness it does not have and diverges from the contract it doubles. |
| R11 | One server-side runtime | **satisfied-with-caveats** | Zero non-Go files, zero third-party dependencies, browser owns nothing semantic. Only gap: no recorded answer for how `ui/` reaches a browser. |
| R12 | Normative or draft — pick one | **violated** | §3 declares package paths that do not exist, an import table wrong in 5 of 11 rows including one edge that is now a cycle, and a CI test that does not exist. |
| R13 | State that implies persistence is persistent | **partially-violated** | Tenancy is decided and typed everywhere, as the rule demands. `StoreCaps.Supports` never reads `Durability`, and several accessors return pointers into live state. |
| C1 | The mandatory core surface did not grow | **satisfied-with-caveats** | Source 4 methods, Sink 3, Transform 3; growth went to optional interfaces and runtimes as designed. Eight growth-path methods have no caller. |
| C2 | The nine must-close decisions are genuinely closed | **satisfied-with-caveats** | All nine have an accepted ADR and a type. Two have a code gap: ADR 0018's stated clock default is unrepresentable, and ADR 0003's in-flight bound does not exist for discrete lanes. |
| C3 | The simple case is simple | **satisfied-with-caveats** | 34-line source, 30-line sink, registration from a module physically outside the repo, six registration mistakes that panic at init naming the fix. The normative author guide does not compile. |
| C4 | An out-of-process implementation can satisfy the same interfaces later | **partially-violated** | True for the source side — verified by building one. False for anything carrying a record: `Payload`, `Value` and `Meta` have no wire form at all. |
| C5 | The eight hostile connectors are a real regression test | **partially-violated** | Five of eight genuinely catch interface-shape drift; two are never constructed by any test, and three now pin defects that have since been fixed. |
| C6 | Code and documentation agree | **partially-violated** | 24 of 24 metric constants match byte-for-byte and 28 of 29 enums match positionally; the divergence is concentrated in §3, §22 and the fault-class block. |

**Score:** 4 violated (R3, R4, R8, R12), 7 partially-violated, 5 satisfied-with-caveats, 0 clean
satisfied, 0 not-yet-applicable.

---

## The headline

**The interface set is close to sound and the engine is overdue — but four things must be fixed before
more interface work, because each is a defect *in delivered code* that more interface breadth will
make more expensive, not less.**

The pattern that matters is this: **every guard in this codebase that has never executed is a coin
flip, and four of them have already come up wrong.** The only sink returns a non-retry-safe
`Indeterminate` on three of four stdout shapes because its `Sync` guard tolerates only
`os.ErrInvalid`. `memstore` declares `FlushIsDurable: true` for a mutex-guarded map. `applyWaivers`
matches on a signature that does not include the thing being waived. `Build` never resolves a codec
and hands out `at_least_once` to a pipeline configured with an encoder that cannot exist. None of
these was caught by 33,408 lines of code, 42 tests, twelve hostile reviews and a completeness audit,
because none of them has ever run.

That is the R3 argument stated as a measurement rather than a principle, and it is the single most
important sentence in this report.

---

## R3 — the straight answer

R3 has the most at stake, so it gets its own section. The rule:

> **Rule:** the first milestone is one record moving from a real source to a real sink, durably, with a
> checkpoint that survives `kill -9`. UI follows something true to display.

**Verdict: violated.** Not arguable on the operative clause:

- `internal/engine/build.go:321-322` — `Pipeline.Run` is `return fmt.Errorf("engine: Pipeline.Run is
  not implemented yet; the interface set is the deliverable of this stage")`. There are no node loops,
  no commit pump, no shutdown sequence.
- `grep -rn "\.Run(ctx\|\.Run(context" --include='*_test.go' .` → zero hits across all 42 test
  functions. Nothing has ever called it.
- `go list -f '{{.Name}}' ./... | grep -c '^main'` → **0**. There is no binary to `kill -9`.
- The only `store.StateStore` in the module is `internal/example/memstore`, which declares
  `Durability: connector.DurabilityNone` (`store.go:138`). `store/single` (bbolt), step 6 of the
  architecture's own order, does not exist. There is nothing durable to survive a crash.

The R3 auditor's evidence file argues the old defect has reappeared "in a more disciplined and more
honest form, but the same shape". **That is half right, and the half it gets wrong changes what you
should do about it.**

### What the auditor got right

The sequencing failure is real and it is the project's own ordering that was broken, not an ordering
imposed from outside. `docs/architecture.md:6983` says: "The order is not negotiable, because R3
exists to prevent breadth before a working path." `:6989-6990`, step 10: "`canal run` end to end, with
the chaos test that kills it mid-flush. This is milestone one and **nothing below it starts until the
chaos test is green.**" Step 10 does not exist. Artifacts of step 11 (`pkg/telemetry`, 678 lines) and
step 12 (`pkg/connectortest`, 522 lines) do.

And the absence has already cost real defects — four of them, listed in the headline above. That is
the empirical case for R3, and it is much stronger than the theoretical one.

### What the auditor got wrong

Two of its rhetorical pillars were killed under hostile verification, and both are the findings that
would have driven the most churn:

1. **"`pkg/telemetry/readmodel.go` is the new wizard."** Killed as immaterial. The file is 152 code
   lines over ten struct declarations and one constructor — not 292 lines of display logic.
   `PipelineStatus` is the *parameter of* `store.StatusStore.Report`, one of the four store interfaces
   ADR 0021 declares the pluggable seam. A third party implementing a `StatusStore` needs the type.
   R3's historical target was a 792-line three-step wizard, a tier-badge system with frozen copy and an
   accessibility-audited health panel — *implementations of a display*. Ten struct definitions required
   by a delivered interface are not that. `ui/` correctly does not exist; there is no HTTP handler, no
   SSE stream, no CLI, no aggregator.

2. **"54.7% of the repo is breadth."** Killed as immaterial. `internal/stress` is 18,285 lines, but
   `docs/architecture.md:6797-6805` (NORMATIVE) makes the hostile pass "the only validation this
   interface set has had that was not self-assessment". It produced 23 fatal and 35 major breakages and
   ADRs 0025–0031. A validation harness for the artefact under construction is not product breadth.
   The auditor concedes this in its own §9(3) and then rates it major anyway; the two positions cannot
   both hold.

### So: is this the same failure?

**No — it is a different failure with the same first symptom, and the difference is the part you
should act on.**

The abandoned attempt died of *sequencing plus a lie*: the connector RFC told adapters to advance
their upstream checkpoint on a `202` that was emitted after appending to an unbounded, unsynced,
in-memory slice. The documented rule and the actual durability of the acknowledged thing were
unrelated, and the system shipped that gap to an adapter author.

Here the sequencing is intact and the lie, in the R3 surface, is absent. `Pipeline.Run` names itself
unimplemented in its own returned error string. `memstore` names itself scaffolding that survives
nothing while declaring `DurabilityNone`. The example tests are named `TestBuildNegotiates…` and
`TestBuildRefuses…`, honestly. Nothing is disguised, and nothing is being told to a user, because
there is no user.

**But the lie has reappeared, one layer down, in delivered non-stubbed code — and that is what the
R4 and R8 audits found.** `internal/engine/negotiate.go:99` guards a `PrunesOnCommit` source on
`!sc.FlushIsDurable`; `memstore` declares `FlushIsDurable: true`; so a replication slot is pruned
against a store that survives nothing, while `memstore/store.go:130-132` states in its own comment
that `Build` refuses exactly this. `pkg/store/state_store.go:61-72` gates every guarantee tier —
including `exactly_once` — without ever reading `StoreCaps.Durability`. That is the *same shape* as
the `202`: a documented refusal that does not exist, over a durability claim that is false.

The R3 violation is what allows those to survive. They are not stub-shaped; they are wrong-guard
shaped, and only execution finds wrong guards.

### The answer to give the owner

**Build the engine. It is the cheapest remaining thing and it is the only instrument that finds this
defect class.** The auditor's own estimate, which I checked and find plausible: `store/single` on
bbolt (~200 lines), an ndjson encoder (~60), the node loops and commit pump that the `Run` TODO
already specifies precisely (~400), a `main` (~80), and one `kill -9` test. Roughly 750 lines against
33,408 already written — **2%**. That ratio is the finding.

**But do not start it before the four fixes in list (a) below**, because two of them
(`applyWaivers`, the `Ledger.send`/`Close` race) are in code the engine will call on its first
execution, and one of them (enum wire forms) becomes a breaking change the moment a checkpoint or a
spec is persisted.

And do not treat "R3 violated" as a verdict on `pkg/`. `pkg/` at 12,108 lines is the requested
deliverable, the README says so, and the hostile pass validated it. The finding is not "you wrote the
interfaces"; it is "you wrote them past the point where writing more of them tells you anything new."

---

# Per-rule findings

Each rule below carries two lists. The surviving findings are what to fix. **The "got right" list is
equally load-bearing**: these thirteen rules were expensive to learn, and several of them are
genuinely, structurally satisfied in ways that must not be undone by the fixes on the other list.

Severity is the verifier's after-attack rating, not the auditor's initial one.

---

## R1 — Topology is data, never schema

**Verdict: partially-violated.**

### Got right

- **The graph is genuinely data.** `spec.Spec.Graph []Node` with `Inputs []Edge`. A probe at 3, 4, 14
  and 42 nodes yields exactly one `unknown_component` diagnostic per node and never a count or shape
  error. There is no fixed stage count anywhere.
- `grep -rniE 'ordinal|minitems|maxitems|followsstage'` over `pkg/` and `internal/` finds only prose
  *about* the abandoned attempt. The literal historical defect is absent.
- **Fan-out and terminality are derived, not stored** — `Spec.Consumers` (`pkg/spec/spec.go:70-80`)
  and `Spec.Terminals` (`:83-97`) compute from edges, so neither can drift from the graph.
- **One routing mechanism**: `Edge{From, Select, BestEffort}` with a three-member `EdgeSelect`
  (`pkg/spec/node.go:77-116`). Not two.
- `Registry` is a value type with `Clone`/`With`/`Without`; a probe registered an encoder into a clone
  without touching `registry.Default`.
- `telemetry.LaneStatus.Kind` is reporting-only; nothing in the core branches on it
  (`readmodel.go:118-119`). The only fixed-size array in the delivered surface is
  `schema.Ref.Fingerprint [16]byte` — a hash, not a topology.

### Surviving findings

- **(major) `Build`'s node switch has four arms and no `default`.** Encoder, decoder, framer,
  deframer and compressor graph nodes are accepted, never config-validated, never constructed, never
  closed, and survive into the persisted spec. `internal/engine/build.go:144-221`; `graph.go:47`
  (`r.Has` admits any registered kind); `graph.go:127` blesses `KindEncoder`/`KindDecoder` as graph
  vertices. Probe: an encoder node whose config fails validation builds with **0 diagnostics** and
  `New` is never called; the same bad config on a *source* is refused. Note the internal
  inconsistency: an encoder placed *terminally* **is** caught (`graph.go:106-114`), so this is a gap
  in one path rather than a blanket omission.
- **(major) Dedupe has two representations with no precedence rule.** `spec.Streams[].Dedupe`
  (`pkg/spec/spec.go:172-173,182-200`) is read by `negotiate`; a stage-standard dedupe block is
  appended to every sink (`pkg/registry/stage_standard.go:53`, `pkg/config/composites.go:301-317`) and
  is read by nothing — `grep FieldDedupe` yields two hits, both declarations. Contradictory values
  build clean, and the served `Descriptor.JSONSchema` for a sink exposes the dead one, so a generated
  form configures nothing. **This is R1's second clause verbatim: one entity, two identifiers.**
- **(minor) ADR 0009's "a new node kind is a registry entry and a config document" is false.** Nine
  hard-coded maps, nine `Add*` funcs, closed `Descriptor`/`Clone`/`With`/`Without` switches, ~51 lines
  across four packages (`pkg/registry/registry.go:115-125,194-197,200-240,295-391`). An external-module
  probe fails with `undefined: registry.Add` / `registry.Def`. `registry.Kinds` is also an exported
  mutable var that can diverge from the registry.
- **(minor) `config.BufferRef` + `Config.Buffer` export a component-valued config field naming a stage
  kind** (`pkg/config/composites.go:455-474`), which ADR 0009 forbids and `architecture.md:6592` claims
  was removed as pattern AE-f1. A connector may import `registry` and resolve a nested buffer from its
  own config, reinstating it.

---

## R2 — The canonical record model is decided first, transport-independent

**Verdict: partially-violated.** The rule's operative clause is met; what fails is enforcement of the
spine's own declared invariants.

### Got right

- **The historical defect has not recurred, and this deserves saying plainly.** R2 was written because
  a stage named `source_canonical_event_serializer` existed while no document defined the canonical
  form and an HTTP DTO became the internal type by default. Here the model is genuinely decided:
  10 files, no stubs, every declared method has a body.
- **Transport independence is structural, not aspirational.** `go list -deps ./pkg/record` shows no
  `json`, `http`, `url` or `proto`; its only canal dependency is `pkg/schema`, which has none.
  `pkg/fault` imports `record`, not the reverse, and `record.go:43-46` explains the deliberate
  inversion.
- `grep -rn Marshal pkg/record/` returns **zero hits**. The spine defines no marshaller at all, so no
  transport encoding can leak in through one.
- **No competing envelope exists.** The only other `Event` types (`connector/event.go:15`,
  `telemetry/readmodel.go:242`) are operator-visible notes carrying no payload.
- `*record.Record`'s full method set is `[Failed Handle MarkFailed Origin Ref SetHandle SetKey
  SetUpstream]` — no `SetLane`/`SetID`/`SetGroup`/`SetStream`/`SetOrigin`/`SetRefs`, exactly as ADR
  0025 promises.
- `Meta.Set` rejects `NSCanal` and unknown namespaces with a real error (`meta.go:79-84`) — the
  namespace reservation is enforced by a check, not by convention.
- `Payload` holds no codec and its accessors never convert (`payload.go:24-63`).
- **`internal/ledger/ledger.go:216-223` refuses a batch whose records disagree with its `Lane`,
  loudly, naming both lanes.** This is the one place the defect class below was closed properly, and
  every surviving finding is a place that check was not extended.

### Surviving findings

- **(major) The "engine rejects a key/handle set later" rule has no enforcement point anywhere**, and a
  handle set after admission silently suppresses the entire lane's acknowledgement.
  `pkg/record/record.go:61,72-74`; `docs/architecture.md:823-825`; ADR 0025:46-48. `grep` finds no
  `SetKey`/`SetHandle` reference in `internal/engine` or `internal/ledger`. Consequence: an
  `OrderingDiscrete` lane never acknowledges; upstream messages are redelivered forever, with no error
  and no leak report.
- **(major) `internal/ledger/ledger.go:304` overwrites `l.groups[gid]` when a batch reuses an
  already-open group id**, orphaning the first group's tracker ticket. Reachable from a connector via
  the exported `(*record.Batch).SetGroup`; `Admit` validates only lane (`:216-223`). Probe:
  `pendingGroups=0`, `inflight=2`, `Leaks()` empty. **The prefix stalls forever and the TTL leak reaper
  never names it — this is the only failure mode in the ledger with no detector.**
- **(major) `record.Batch` — `connector.Buffer.Put`'s parameter — JSON-marshals and unmarshals with
  `err == nil` while losing every `Origin` field, the whole `Payload`, `Meta` and the handle.** ADR
  0015's promised wire-shippability generator does not exist (`find` for `*.yml`/`*.yaml`/`Makefile`/
  `*.sh` returns zero results). A durable buffer or out-of-process seam silently loses identity and
  body while `before_complete:2` asserts a complete image over `{}`.
- **(major) Five normative texts state provenance is unforgeable**, while `record.NewAllocator`,
  `record.NewBatch` and the exported `Batch.Records` let any third-party module mint records with
  chosen tenant, lane, group and id zero (`pkg/record/batch.go:8-18,36,75,120`; `doc.go:27-28`; ADR
  0005:49-52; `architecture.md:859` vs `:864`). Colliding ids repoint `ledger.byRec` so an honest
  record can never settle and the lane's cursor stalls until reaped.
- **(minor) `OpUpsert` ships** (`pkg/record/change.go:104,114`) but appears in no ADR and is absent
  from the normative `Op` const block in `docs/architecture.md:754-766`, which still ends at
  `OpScanRead`. The same doc *was* updated for ADR 0025 (`:818-833`), so this is drift, not policy.
- **(minor) `meta.go:89`'s claim that unexported `set` is "the unchecked writer the core uses for the
  canal namespace" is false** — the core is a different package and cannot call it. Latent only:
  nothing reads canal-namespace metadata either.

---

## R3 — One end-to-end path before any breadth

**Verdict: violated.** See the dedicated section above for the full answer. The surviving findings, in
severity order:

- **(fatal)** `Pipeline.Run` is a one-line error return; no node loops, no commit pump, no record has
  ever moved, and no test calls it. `internal/engine/build.go:321-322`.
- **(fatal)** The only `StateStore` is `memstore` with `DurabilityNone`; bbolt is absent and `go list`
  reports zero `main` packages, so a `kill -9`-surviving checkpoint is unreachable.
  `internal/example/memstore/store.go:138`.
- **(major)** `Build` never resolves encoder/framer/compressor names and accepts a codec that cannot
  exist, still returning a negotiated `at_least_once`. No `r.Encoder(`/`r.Framer(`/`r.Compressor(` in
  `internal/engine/*.go`; `Build` resolves only Source (`:146`), Sink (`:170`), Buffer (`:203`),
  Transform (`:214`). Contradicts `build.go:106-107` ("An impossible pipeline is REFUSED HERE, at
  submit time") and ADR 0022:75-77. Probe cases B/C/D: `ndiags=0`.
- **(major)** The only sink's `Sync` guard tolerates only `os.ErrInvalid`, so a pipe (EBADF),
  `/dev/null` (ENOTSUP) and a real tty (ENOTTY) all yield `Indeterminate` with `Written=0` **after the
  bytes have already left the process**. `internal/example/stdoutsink/sink.go:66`;
  `pkg/fault/fault.go:136`. Only `canal run > file` works.
- **(minor)** `internal/ledger` — 1,204 lines of concurrency-sensitive settlement accounting — has no
  test file of its own. Its only driver is one stress test that exercises it to *document* ledger
  defects.
- **(minor)** The architecture's own non-negotiable order says nothing below step 10 starts until the
  chaos test is green; step 10 does not exist while step 11 and step 12 artifacts do.
  `architecture.md:6983,6989-6990`.
- **(minor)** All ten `pkg/` packages report `[no test files]`, and the JSON golden-file tests promised
  by `architecture.md:6984` do not exist. More R8-shaped than R3-shaped, but true.

### Got right

- `ui/` genuinely does not exist. Repo root is `.gitignore README.md docs/ go.mod internal/ pkg/`.
- **The stress suite is real, not padding**: 42 test functions, 259 `t.Error*`/`t.Fatal*` calls, zero
  assertion-free tests.
- R10 labelling is met throughout: `Pipeline.Run` names itself unimplemented in its own error string;
  `memstore`'s doc labels itself scaffolding that survives nothing while declaring `DurabilityNone`.
- `Build`'s no-I/O contract (`build.go:102-103`) is documented and honoured.
- `Build`'s refusal machinery genuinely works where wired: it refuses `ExactlyOnce` against a sink
  implementing neither `Committer` nor `TokenSink`, and the diagnostic names `connector.Committer`.
  **That is what makes the absent codec check an omission rather than a missing subsystem.**
- `Pipeline.Close` (`build.go:325-337`) is safe on a pipeline that never ran.

---

## R4 — An acknowledgement means durable

**Verdict: violated.** R4 was the rule the provenance calls "the most dangerous gap in the whole
design". It has the highest count of majors of any rule here, and two of them are live races or silent
acceptances in delivered code.

### Got right

- **`pkg/connector/sink.go:11-18,30-52` — a sink has no ack callback and no progress awareness.** All
  four `(res, err)` quadrants are specified with a safe unnamed-record default, and `:33-37` names its
  own R4 exposure explicitly. The structural cause of the historical defect is designed out.
- `WriteResult.Deferred` (`sink.go:198-218`) gives a coarse-cadence sink the
  accepted-not-durable-do-not-advance answer instead of forcing it to lie.
- `pkg/registry/add.go:41-43` — `RetentionUnknown` **panics at registration**. Enforced in code, not
  prose.
- **`internal/ledger/ledger.go:487-526` with `:454` — on the prefix path an `Ack` can only be produced
  by `Committed`, and `Flushable` emits nothing.** The ordering R4 demands is enforced by API shape.
- `ledger.go:70-76` and `:659-667` — `resolved` (delivered prefix) and `committed` (durable cursor) are
  separate fields with separate metrics, so a watermark cannot be taken from the resolver.
- `internal/stress/parallel-snapshot/proof_test.go:175-193` — a real regression test asserting a
  position-only batch cannot jump the prefix past an unsettled record.
- `pkg/registry/resolve.go:262-276` — `AckPoint()` derives token/commit/flush/write from the **resolved**
  handles, so the disclosed ack point is the one the engine would act on.
- `pkg/connector/source.go:89` — `Commit` returns only `error`, never a position, so the read model
  cannot report unpersisted progress.

### Surviving findings

- **(major) `applyWaivers` downgrades EVERY `SeverityError` with `CodeCapability` or `CodeGuarantee`
  on a match of only `Node`, `AcknowledgedBy` and `Reason` — it never reads `Requested`, `Effective`
  or `Missing`.** `internal/engine/negotiate.go:304-328`, called at `:288`, gated at
  `build.go:236-238`. **I re-read this code by hand while writing this report; the finding is exact.**
  Probe: a waiver declaring `at_least_once` to `at_least_once` turns both errors into warnings and
  `built=true`. One signature, for any stated reason, disables ADR 0006's data-loss guard on every
  node. ADR 0024 scopes waivers to tiers only.
- **(major) `Build` silently accepts a `PrunesOnCommit` source against `memstore`.** The guard tests
  `FlushIsDurable` (`internal/engine/negotiate.go:99`); `memstore` declares it `true` for a
  mutex-guarded map while its own doc (`store.go:127-141`) claims `Build` refuses. Re-run:
  `HasErrors=false`, `built=true`, zero diagnostics. A replication slot is pruned against a store that
  survives nothing. `StoreCaps.Durability` (`pkg/store/state_store.go:51`) is public API read by
  nothing.
- **(major) The commit protocol branches.** `Settle` ends with `emitDiscrete`
  (`internal/ledger/ledger.go:446`, `:539-572`), which builds a full `Ack` carrying the source's
  delivery handles and sends it — with **no `StateStore` write, no `Flushable`, no `Committed`**.
  `doc.go:13-21` numbers settle=4, phase two=6, phase three=8. Probe: `through.IsZero=true handles=1`.
  A discrete lane feeding a `Committer` sink deletes the upstream copy while the committable exists
  only in RAM. ADR 0006 forbids branching by name.
- **(major) `memstore.Set` reads only the batch epoch and never `Versioned.Epoch`** while declaring
  `EpochFencing: true`, which `StoreCaps.Supports` gates every delivery tier on.
  `internal/example/memstore/store.go:85-115` uses `w.Epoch` twice and `v.Epoch` never;
  `pkg/store/key.go:118-134`; `EpochFor` has zero callers. Probe: a stale per-key write is accepted.
  Two workers can both authorise an upstream commit for one lane. **No `StateStore` conformance suite
  exists to have caught it.**
- **(major) `Ledger.send` samples `l.closed` under the mutex, releases it, then blocks on an unguarded
  channel send; `Close` sets the flag, unlocks, and closes that channel.**
  `internal/ledger/ledger.go:574-584` vs `:778-796`, `close(l.acks)` at `:794`. **I re-read both
  functions by hand; the window is exactly as described.** Probe under `-race`: send-on-closed-channel
  panic plus a DATA RACE between `Close:794` and `send:583` via `Settle:446` — during the shutdown
  window `build.go:311-317` explicitly reserves for acks still being delivered.
- **(minor) A buffer can never be named as the durability edge.** `negotiate.go:214,219-229` hardcodes
  `DurabilityEdge` to sink plus node id, so a `DurabilityCluster` buffer leaves no trace in any
  machine-readable field, though `pkg/telemetry/negotiated.go:63` documents `buffer:wal` as a value.
  Latent today (no buffer is registered) but `DurabilityEdge` is a pinned wire field.

---

## R5 — Dedupe keys are scoped, and committed after the write

**Verdict: partially-violated.** The scope half — the half that caused cross-tenant silent discard — is
genuinely closed by construction. The submit-time gate has holes and one key-encoding collision.

### Got right

- **Cross-tenant collision is impossible by type.** `Tenant` is a separate field on `store.Key`
  (`key.go:34-38`), never a `Part`; `Prefix()` compares it structurally. Every collision constructible
  in a probe stays inside one tenant.
- **One durable substrate, atomic with the cursor.** `SpaceDedupe` on the single `StateStore`
  (`key.go:22-24`); `Set` MUST be atomic and MUST NOT return before durable
  (`state_store.go:24-32`); `AtomicMultiKey` gates the higher tiers.
- **Nothing keys on `RecordID`.** `record/ids.go:69-83` — generation-local, never durable. The exact
  defect ADR 0002 rejects is absent from the delivered types.
- A zero dedupe window **is** refused in the ordinary case: probe gives `HasErrors=true` with one
  guarantee-coded error naming `[streams lines dedupe window]`.
- **Connector authors structurally cannot reimplement dedupe**: `SinkRuntime` deliberately has no
  `Lanes` and no `State` (`connector/runtime.go:110-115`), and `ConfiguredStream`
  (`sink.go:110-114`) does not expose the dedupe config.
- Both identity layers are populatable — `SetKey` and `SetUpstream` (`record/record.go:82,92`) with a
  documented source-only window, so ADR 0025 did close the stress pass's blocker.
- `record.Batch.AddFor` (`record/batch.go:171-187`) stamps `Origin.Stream` at allocation and cannot be
  re-targeted by a transform, naming R5's cross-stream collision as its reason.
- `EffectivelyOnce` **is** refused for a source without `StableKeys`: `negotiate.go:92-95` clamps
  `sourceCeiling` to `AtLeastOnce`.

### Surviving findings

- **(major) `Build` accepts a dedupe config against a source declaring `StableKeys=false`, and against
  `layer=upstream` which no capability field can describe, with zero diagnostics** — contradicting ADR
  0002's "cannot use dedupe at all". `negotiate.go:173-184` is the only dedupe check and it tests only
  `Window != 0`. There is no `Origin.Upstream` field in `connector/caps.go`. A config no source can
  satisfy passes the one function whose stated contract is refusing exactly that at submit time.
- **(major) `store.Key.String` joins `Parts` with `/` unescaped**, so two structurally distinct dedupe
  keys collide inside one tenant. `store/key.go:42-52,142-144` — contrast `record/ids.go:46-58`, where
  `DeriveLaneID` **does** escape. `Batch.Writes` and `memstore` key on that string. Probe: `Build`
  takes node id `"s3/bucket"` with 0 diagnostics; `Batch.Len()=1` for 2 keys; `Get(A)` after writing
  only `B` returns found. **This is R5 bug 1 — silent discard as duplicate — reachable through the
  delivered submit path in `pkg/`, not through the stub.**
- **(minor) The window gate is skipped entirely when a stream entry is scoped to a non-sink node**,
  because `StreamsFor(sink)` never returns it and the source loop checks only lane kinds
  (`spec/spec.go:125-142`; `negotiate.go:122-130` vs `:133,173-183`). Probe: `window=0` scoped to
  source node `"in"` gives `HasErrors=false`, 0 diagnostics. An unconditional ADR 0002 point-3 gate
  with a hole; the config becomes an inert, silently-ignored operator setting.
- **(minor) The commit-after-write ordering is asserted as achieved while zero code implements it.**
  `architecture.md:6745` sits under the heading "How it is satisfied structurally" (`:6739`); there are
  0 callers of `store/key.go:87` and 0 emits of `MRecordsDeduped`. Three delivered docs also disagree
  on what dedupe keys on — `record/origin.go:51-54` says it correlates on `Root`, a `RecordID`.
- **(minor) Dedupe has two operator surfaces** — see R1's second major finding, which is the same
  defect seen from the topology side. `architecture.md:4906,4910` says "the engine reads them"; it
  reads only `negotiate.go:179`. **Unlike batching, this survives the engine stub, because two
  representations genuinely exist in shipped code.**

---

## R6 — A buffer without a rejection path is not a buffer

**Verdict: satisfied-with-caveats.** The auditor said partially-violated; its own verifier overruled
it, and I agree with the overrule for the *interface* — but the caveat is not cosmetic and is carried
into the action list below.

### Got right — this is the rule the design handled best

- **Exactly one buffer interface.** `grep '^type .*[Bb]uffer.* interface'` over `pkg/` and `internal/`
  returns `buffer.go:19 Buffer` and `runtime.go:166 BufferRuntime` (a runtime handle, not a record
  store). The historical defect — two mutually incompatible buffer abstractions in one package — is
  absent.
- **Refusal is a typed outcome.** `Accepted{Records int; Refused []record.RecordID}`
  (`pkg/connector/buffer.go:57-63`) makes partial and total refusal representable, and makes
  success-for-records-not-taken *unrepresentable*. This is the direct answer to "its `Append` could
  never fail".
- **`WhenFull` (`buffer.go:115-141`) has no unbounded member**, `ParseWhenFull("unbounded")` returns
  `ok=false`, and `Block` is both the zero value and the spec default (`composites.go:246`).
- **`record.Batch` is bounded by construction**: `batch.go:187-189` `AddFor` returns `nil` at
  `b.cap`, so a caller ignoring the return still cannot exceed it.
- **`registry.AddBuffer` panics loudly on `Bounded: false`** (`add.go:264-266`). Enforced at
  registration, not documented.
- `Tracker.Track`'s weight budget genuinely blocks, **outside the ledger mutex**
  (`tracker.go:99-119`, `ledger.go:291-299`); an over-budget `Track` returns `context canceled` rather
  than admitting.
- The delivered `Buffer` method set matches the normative architecture section with no drift, and the
  non-destructive-`Get`-plus-`Trim` choice is justified at the site (`buffer.go:36-38`).

### Surviving findings

- **(major) `Tracker.TrackResolved` appends unboundedly many zero-weight nodes.** `t.nodes` is counted
  but never compared to any cap (`tracker.go:112` tests only `== 0`; `:140-149`; `:161 nodes++`), and
  `ledger.go:272-278` routes zero-record batches there. Probe: `nodes=5000001`, **419.6 MiB**. This
  contradicts `architecture.md:5534` — "Nothing in a graph can grow without limit". The reaper
  (`ledger.go:742-773`) never resolves stalls. **This is R6's own prohibition, in the backpressure
  mechanism rather than in the buffer.**
- **(minor) `NewBatcher` accepts a trigger-less `config.BatchPolicy` that `config.Batching` refuses**,
  and `Batcher.Add` has no cap and no refusal (`batcher.go:29,37,88`; handed to authors at
  `runtime.go:93-94`). Probe: 3,000,000 pending, 1,426.5 MiB. Inconsistent with `NewTracker`
  (`tracker.go:78-80`), which coerces a zero budget to one for the analogous case. **A one-line
  coercion closes it.**
- **(minor) ADR 0001's run-vs-serve durability threshold has no representation in code**:
  `negotiate.go:219-228` hardcodes `Durability < DurabilityCluster`, so in `canal run` a Node-durable
  buffer is wrongly told it is narrower than the assignment domain. ADR 0001:35-37 says Node suffices.
  `grep serve|Coordinated|ModeRun` over `*.go` = 0.
- **(minor) Three rows of the submit-time refusal table are unimplemented** inside an otherwise
  implemented negotiation subsystem: `MidLaneResume`, `BufferCaps.Chains` and `Decoder.Accepts` — 0
  occurrences each in `internal/engine/`, against `StableKeys` 4, `Replayable` 4, `SchemaChanges` 2.
  `when_full: overflow` with no chaining buffer downstream draws no diagnostic. **The "engine is
  stubbed" excuse does not apply: twelve sibling rows are implemented.**

**Cross-reference.** C2 independently found that **discrete-ordered lanes have no in-flight bound at
all** — the `Tracker` is built only for prefix lanes (`ledger.go:169-171`, which I re-read by hand;
`:269,293-299` gate blocking on `tracker != nil`). Probe: budget 8, **200,000 admitted, 0 refused**,
`InFlight=0` with 50,000 groups open. Taken with the `TrackResolved` finding above, **the shipped
backpressure implementation has two unbounded paths**, which is why R6 appears in list (a) despite the
interface being clean.

---

## R7 — If the design says "retry the failures", the contract must name them

**Verdict: satisfied-with-caveats.** The auditor said partially-violated; its verifier overruled it
because two of the three pillars fell (one is engine-stub, one was refuted outright). I agree: R7 asks
for a shape, and the shape is there.

### Got right — this is the rule most directly answered

- **`pkg/connector/sink.go:199` — `Failed []fault.RecordFault`, keyed on `record.RecordID`, declared 26
  lines below `Write` in the same file.** That is R7's literal demand, met. The historical schema was
  `{accepted: int, duplicateIds: string[]}` with no per-event error array.
- `pkg/fault/record_fault.go:5-16` names R7 explicitly and refuses positional identity ("Never an
  index").
- All four `(res, err)` quadrants are specified with a safe default in each; quadrant 4 claims nothing
  durable (`sink.go:30-52`).
- **The retry subset is derivable from the result alone**: `fault.Class.Terminal()`
  (`pkg/fault/class.go:159-166`) partitions `Failed` with no side table and no positional correlation.
- `fault.Class.Counted()` (`class.go:181-188`) answers "does this spend an attempt" — a question the
  abandoned attempt's shape could not ask at all.
- **The cardinality identity does catch the canonical honest-under-reporting shape**: probe with 500
  records and only 12 named rejects returns `Reconcile(500) → ok=false, want=488`.
- `internal/ledger/disposition.go:15-38` — `{Delivered, Duplicate, Abandoned}` with a documented
  refusal of any non-terminal member; `Deferred` correctly maps to no `Settle` call.
- `internal/engine/graph.go:125-135` gives `SinkCaps.PartialFailure` a **live engine-side reader** that
  refuses an `EdgeFailed` edge from a node that cannot fail per record.

### Surviving findings

- **(major) `Reconcile` is cardinality-only.** `pkg/connector/sink.go:250-253` never checks that ids
  named in `Failed`/`Deferred` are members of the request, distinct, or disjoint — against the set rule
  stated 200 lines above at `:41-42`. `internal/ledger/ledger.go:355-357` `Settle` silently continues
  on an unknown `RecordID`. A foreign or duplicated id in `Failed` passes green **while a record the
  sink counted as unwritten is settled `Delivered`** — the loss shape the design calls AC-f3.
- **(minor) ADR 0004 (accepted, normative) declares a `WriteResult` with no `Deferred` field** and
  states rule 2's identity without it (`0004-partial-failure-shape.md:31-37,61`), and was never revised
  when `Deferred` shipped. Not a live ambiguity — `Deferred` is illegal from `Write`
  (`sink.go:214-217`), so the two forms are correctly scoped — but it is a stale normative doc.
- **(minor) `internal/stress/fanout-pipeline/connector.go:948-949` still asserts in prose that the
  delivered `Deferred` capability "CANNOT BE EXPRESSED"** and still returns the wasteful shape, though
  its own buffer is already `[]record.RecordID` (`:727`). The regression suite kept as the contract's
  evidence base documents the absence of a field the contract now has.
- **(minor) `schema-drift`'s `dlqSink` builds a full per-record `Failed` slice and drops it**
  (`connector.go:1894` builds, `:1908-1910` returns `WriteResult{Written: 0}` without it), under a
  comment claiming the reconciliation is trivially right. Probe: `Reconcile(10)` → `ok=false want=10`.
  Fixture bug, not a contract defect — but nothing catches it and no test reaches it.

---

## R8 — Conformance is asserted against real responses

**Verdict: violated.** This rule was written about a nil slice marshalling to `null` past a test that
asserted `len(...) != 0`. **Both halves of that defect are present today, and one of them is live in
shipping code.**

### Got right

- **`APIVersion` is one constant.** `caps.go:12,15` define it, `crosscheck.go:107` range-enforces it at
  registration, and both registered example descriptors report `"api_version":1` from the constant.
  R8's "hard-coded in five places with nothing checking they agreed" defect is genuinely fixed.
- **Each enum's token set has exactly one Go definition**: 26 `[...]string` arrays plus 5 map variants,
  one per enum, each read by one `String()`. No token list is written twice inside `pkg/`.
- **`ParseWhenFull` (`pkg/connector/buffer.go:162-170`) iterates `whenFullNames` rather than
  hand-writing a reverse switch, so `String` and `Parse` structurally cannot diverge.** This is the
  model to generalise, and the fix in list (a) is "do what `ParseWhenFull` does, everywhere".
- `telemetry.FaultInfo` puts the token on the wire: `readmodel.go:259` declares `Class string` and
  `:282-283` fills it from `f.Class.String()`.
- **45 tests across `internal/example` and the eight stress packages drive real records through the
  real ledger**, 9/9 green, including after a deliberate mutation. Genuine end-to-end assertion —
  never of a serialised form, which is the gap.
- No doc-vs-code gap at the declaration level: `architecture.md:3614` and `:2889/:2894` carry the
  identical Go field declarations for `WhenFull`, `Boundedness` and `LaneKinds`.

### Surviving findings

- **(fatal) Eight `[]uint8`-enum capability fields serialise as base64, and it is live.**
  `registry.descriptor()` marshals caps at every registration (`pkg/registry/add.go:417` into
  `Descriptor.Caps`, `descriptor.go:26-27`), so **both delivered example connectors already ship
  `"lane_kinds":"AQ=="`**. Fields at `caps.go:47,52,279,291`, `spec.go:164`,
  `source_optional.go:117`, `codec.go:85-86`.

  **I reproduced this by hand while writing this report**, from an external module against the real
  `pkg/connector`:

  ```
  {"api_version":1,"default_ordering":0,"boundedness":null,"lane_kinds":"AQ==","max_lanes":0, …}
  guarantee: {"guarantee":1}   String()= at_least_once
  ```

  Three defects in one line of output: the enum slice is base64, the required slice is `null`, and the
  scalar enum is an ordinal while its own documentation calls the token the wire form. The projection
  `resolve.go:12` says the engine, API, frontend and conformance kit all read is **unrenderable by the
  UI and unproducible by any non-Go peer**.
- **(major) The wire-shippability generator ADR 0023 specifies "running from commit one" is absent**
  (`0023-conformance-and-chaos.md:110-111`), step 1's own JSON golden-file tests are absent
  (`architecture.md:6969`), and `pkg/telemetry/metrics.go:3` asserts a golden-file test that does not
  exist. Verified: no `.github`, no `go:generate`, no `testdata/golden`, zero `*_test.go` under `pkg/`.
  **The findings above and below are live instances of exactly the class that commit-one guard names.**
- **(major) Four token sets are hand-written in `composites.go` with no link to the enum, and one has
  already drifted**: config declares `retry.terminal` default `"dead_letter"` while
  `fault.DefaultRetry.Terminal` is `TerminalStop`. `pkg/config/composites.go:68,71-73,78-80,250-253,
  309-310,347-368` vs `pkg/fault/retry.go:60-65,90-94,127-135,141`. Mutation test: renaming
  `retry.go:63` `"drop"` → `"discard"` leaves build, vet and 9/9 tests green.
- **(major) 29 of 31 `uint8` enums have no text marshaller**, so `Guarantee`, `schema.Type` and
  `fault.Class` serialise as integers on JSON-tagged fields while their own doc comments call the token
  "the wire form" (`schema/type.go:32-33` vs `schema.go:23`; `connector/guarantee.go:47` vs
  `spec.go:25` and `negotiated.go:20,60`; `fault/class.go:127-128` vs `event.go:21`). **Inserting an
  `iota` member reinterprets stored values**, and `ParseWhenFull` reverses tokens so it cannot read
  what `spec.Spec:30` writes.
- **(major) `registry.Support` and `CapSource` have `MarshalText` without `UnmarshalText`**
  (`kind.go:65`, `descriptor.go:108`), so a `Descriptor` cannot be decoded from its own encoding by a
  Go client: `"json: cannot unmarshal string into Go struct field Descriptor.support of type
  registry.Support"`. **Two-line fix.**
- **(major) Six slice fields of `PipelineStatus` plus two in `Negotiated` carry JSON tags with no
  `omitempty` and marshal to `null`** (`readmodel.go:37,47,48,49,50,63`; `negotiated.go:32,52`) —
  **R8's named historical defect verbatim**, including that `len(s.Conditions) != 0` is `false` for the
  result. Latent rather than live: nothing constructs a `PipelineStatus` yet.

---

## R9 — One concept, one vocabulary

**Verdict: partially-violated.** The rule's own test — "a function mapping between two representations
of the same concept is evidence of a modelling error" — currently has several true positives.

### Got right

- **`Negotiated` is defined exactly once** (`telemetry/negotiated.go:17`); `engine/build.go:113`
  returns it rather than defining a parallel struct.
- `connector.Event.Severity` **is** `fault.Class`, not a second severity enum
  (`connector/event.go:19-21`, citing R9 in the code).
- `Guarantee` has one Go spelling (`connector/guarantee.go:20`) with its placement argued in R9's terms
  at `:16-19`; `Min` is a fold, not a cross-map.
- **Position/cursor discipline holds**: `grep 'type Offset|type Mark|type Watermark'` across the module
  returns nothing. One `record.Position` (`position.go:15`).
- One batching policy: `grep 'type BatchPolicy'` = 1 hit (`config/composites.go:379`);
  `connector.Batcher` holds it by value.
- `Class.Blames()`/`Terminal()`/`Counted()` (`fault/class.go:139,159,181`) are **computed facts over
  one source of truth**, not cross-maps — exactly the shape R9 asks for.
- The two `Disposition` types are documented as genuinely different concepts
  (`internal/ledger/disposition.go:20-23`) and one is internal. `registry.Kind` is a deliberate open
  string with its rationale at `kind.go:3-9`; `store.Key.String()` is labelled non-canonical at
  `key.go:40-41`.

### Surviving findings

- **(major) The capability vocabulary is typed by hand in two tables that have drifted.**
  `reads_lanes` and `resolves_stale` exist in `resolve.go:76,181` and in **neither** of `add.go`'s
  tables (`:60-84`, `:147-181`), so over-declaring them does not panic at init. Probe: "NO PANIC:
  registration accepted `ReadsLanes` without `connector.LaneReader`" — while the same lie about
  `Nackable` panics. Violates `AddSource`'s documented panic contract **and** ADR 0013:53.
- **(major) The snake_case token is not the wire form**: 43 of 45 enum-typed JSON fields serialise as
  integers or base64, so token, integer and base64 coexist in one shipped document. Probe emits
  `"lane_kinds":"AQA="` beside `"support":"community"`, and `"phase":"running"` beside
  `"guarantee":2`. Same root cause as R8's fatal.
- **(major) The retry-terminal and indeterminacy token sets are hand-typed three times across two
  packages** — `fault/retry.go:60-64,90-93` and `config/composites.go:71-73,78-81,348-354,361-367` —
  in a package that **already imports their owner** (`go list -deps ./pkg/config` shows `fault`), so
  `composites.go:242-244`'s import-cycle rationale does not cover these. Contradicts
  `architecture.md:6749` ("no cross-map function anywhere").
- **(major) `schema.FieldNote` and `record.FieldChange` model one concept twice with divergent `Path`
  and `Kind` types** (`schema/convert.go:21-29` `Path []string, Kind string` vs
  `record/meta.go:180-184` `Path string, Kind FieldChangeKind`), and the converter the code promises is
  **not expressible** — `record.ParseFieldChangeKind` is undefined. `record` already imports `schema`.
- **(major) `connector.Backlog` and `telemetry.Backlog` are one concept as two structs with three
  divergences**: `as_of` vs `asOf`, `omitempty` vs not, and `*time.Duration` vs `*float64`-seconds
  (`source_optional.go:164-181` vs `readmodel.go:185-193,170`). The `omitempty` split breaks
  `readmodel.go:13`'s own nil-pointer rule. **`negotiated.go:13-16` refuses this exact duplication
  citing R9** — so the project already knows the rule and applied it in one place and not the other.
- **(major) The read model re-spells six typed vocabularies as bare strings** although `telemetry`
  already imports `connector` and `fault` (`readmodel.go:87,120,202,244,245,259-261`;
  `negotiated.go:90-91`). `Downgrade.Requested`/`Effective` are unvalidated free text **persisted in
  the spec** (`spec/spec.go:55`) and rendered verbatim into `Negotiated.Why`
  (`engine/negotiate.go:285-286`): a durable operator-signed waiver can name a guarantee outside the
  guarantee vocabulary.
- ~~**(minor) `pkg/telemetry` uses 41 camelCase json tags against 8 snake_case**, while every other
  package in `pkg/` is 78/0 snake_case — and both casings appear inside one document
  (`"observedGeneration"` beside `"ack_point"`).~~ **Fixed** when the read model acquired a producer
  and `GET /status` shipped, which is the moment the half-decision below stopped being free.
  `pkg/telemetry` is now 49/0 camelCase. The rest of `pkg/` stays snake_case deliberately, and the
  split is not aesthetic: those tags are an **operator input format** (`lane_budget`, `when_full` in a
  spec file) and a **persisted format** (`record.Position`, the lane row, the checkpoint — all
  JSON on disk). Renaming either is a migration, and renaming the persisted one is undetectable: no
  test would fail, because every test starts from a fresh store, and a running deployment would
  silently resume from zero. The read model is neither; it is a brand-new output nobody was reading
  yet, and it is the one place the rename was actually free.
- **(minor) `telemetry` exports three `Phase*`-prefixed vocabularies**; the restart and commit ones are
  untyped string constants (`metrics.go:130-135,138-142`) and are silently assignable into the typed
  `Phase` slot (`status.go:15-29`). `go vet` is silent.
- **(minor) The lane concept carries two spellings of one field** (`StartAfter`/`After`) and two
  representations of `Boundedness` (2-member enum vs bool) across `LaneSpec`, `LaneRow` and
  `LaneState` (`connector/lane.go:126,44-50` vs `store/coordinator.go:100,105` and
  `engine/checkpoint.go:129,133`), although `lane.go:97-102` claims no drift. Latent: zero
  constructions of `LaneRow` or `LaneState` exist yet.

---

## R10 — Scaffolding is labelled, and tested against what it stands in for

**Verdict: partially-violated.** Clause 1 (labelled) is met, sometimes exemplarily. Clause 2 (tested
against what it stands in for) has no carrier, and the one double that *could* be tested today
diverges from its contract.

### Got right

- **`internal/example/memstore/store.go:1-11` is the model**: it labels itself scaffolding, cites R10
  by name, names bbolt as the thing it is not, and **enforces it via `Capabilities()`** returning
  `DurabilityNone`, consumed at `build.go:234`. A label backed by a value the engine reads.
- **`pkg/registry/lint.go:97-113` via `add.go:405-412` makes a drifted config `Example` panic at
  init** rather than mislead an operator — stronger than the CI check the auditor assumed was missing.
- `internal/example/example_test.go:32-46` walks every descriptor in `registry.Default` and enforces
  the `CapReport.Reason` invariant in both directions.
- `Negotiated.Defaults` works: a probe returned five notes, each `From "core default"`.
- The `var _` assertions at `pkg/connectortest/runtime.go:98,147,165,199,376,440`, re-asserted in
  `schema-drift/regtest/reg_test.go:39-48,76-80`, genuinely pin interface shape.
- `internal/stress/parallel-snapshot/connector.go:331-333,1159-1160` labels the stubbed upstream **and
  names exactly what it cannot do that the real connector would** — which is clause 2 done properly, in
  prose, in the one place it was attempted.
- Two working fixture-isolation patterns exist: `fanout-pipeline/connector.go:125-127` (private `Reg`)
  and `push-source/connector.go:862` (`Register` takes the registry as a parameter).

### Surviving findings

- **(major) `pkg/connectortest`'s package doc claims every method returns "the SAFE, INERT answer"**
  (`runtime.go:16-18`), but `LaneCtl` keeps a lane table with `MaxLanes` and rollback and `StateHandle`
  enforces CAS with `fault.ErrFenced` (`:174-175,210-227,363-365,383-393,407-428`). A false inertness
  label in the shipped public surface is *why* a reader never checks it for the divergences below.
- **(major) `LaneCtl.Announce` hardcodes `(DefaultTenant,"p","n")`** while the contract derives
  `LaneID` from the runtime's identity (`runtime.go:212` vs `record/ids.go:33-36`), and `LaneCtl` has
  no field able to carry it. Probe: fake `"default/p/n/orders"` vs contract
  `"acme/orders-p9/src1/orders"`. **Correcting the double breaks a currently green test** —
  `internal/stress/push-source/connector_test.go:127`.
- **(major) `registry.Support`, nominated by `architecture.md:6750` as *the* R10 mechanism, has no
  value meaning scaffolding**, defaults to `SupportCommunity`, and is consumed by nothing outside
  `add.go:433`. Probe: `registry.Default` = 12 descriptors, histogram `map[community:12]`, of which 10
  are hostile fixtures. The one mechanism named to stop fixtures reading as product cannot express the
  distinction, and `Descriptor` has no other field for it.
- **(minor) Two shipped godoc sources state in the present tense that a test builds the same spec
  against both deployment assemblies** (`pkg/store/doc.go:16-17`, `internal/engine/build.go:22-24`); no
  second assembly and no such test exist. `ConfigStore`/`Coordinator`/`StatusStore` have zero
  implementations. **The public package's own documentation asserts a conformance test that does not
  exist — the doc-vs-code gap R8 and R12 were written from.**
- **(minor) `Schemas.Register` mints refs with a zero `Fingerprint` and empty `Stream`**
  (`connectortest/runtime.go:451-455`) although `schema.Ref` is content-addressed and
  `schema.Fingerprint` is exported and usable today (`schema/ref.go:18-22,38`). Probe: two different
  schemas both `fp=000…0` vs real `5a2658…`/`55fe0a…`. **A test asserting the double matches
  `schema.Fingerprint` is writable now** — this is clause 2, available immediately, for one method.
- **(minor) A regression test comments "it works end to end"** over a `Declare`/`Get` round trip whose
  only participant is `connectortest.SourceRuntime` appending to and reading its own slice
  (`schema-drift/regtest/reg_test.go:43,50,55-68`). The test's structure is fine; **the fix is one
  comment.**

---

## R11 — One server-side runtime

**Verdict: satisfied-with-caveats.** This is the cleanest rule in the report. One surviving finding,
minor, and it is an unrecorded decision rather than a defect.

### Got right

- **Zero non-Go files.** 171 files = 106 `.go` + 63 `.md` + `go.mod` + `.gitignore`. A 22-pattern
  second-runtime search (`package.json`, `*.ts`, `*.py`, `Dockerfile`, `Makefile`, `*.yaml`,
  `*.proto`, …) returns empty.
- **Zero third-party dependencies.** `go.mod` is 3 lines with no `require` block; `go list -deps`
  yields only `github.com/BernardoCSACarreira/canal` packages.
- A probe served `[]registry.Descriptor` and `telemetry.PipelineStatus` over a real listener at HTTP
  200, with non-stdlib dependencies limited to canal's own `pkg/` packages. **Wire-ready from one
  stdlib-only Go binary.**
- **No browser-authoritative semantic exists.** All 19 browser/frontend references in `pkg/` describe
  the browser *reading* Go-authored data. `ShowIf`/`RequiredIf` are enforced server-side in Go at
  `pkg/config/validate.go:49,62`; the browser copy owns nothing.
- **ADR 0016 rejects an embedded expression language citing R11 by name**: two evaluators "kept
  bit-identical, which is exactly the multi-runtime drift R11 exists to prevent". Not retrofitted —
  the rule was applied at decision time.
- Anti-drift is structural: the `config.UnionTagKey` constant (`field.go:117-120`),
  `Descriptor.JSONSchema` "GENERATED from Config, never hand-maintained" (`descriptor.go:29-30`), one
  `String()` per enum in six packages.

### Surviving finding

- **(minor) No normative document says how the UI reaches a browser.** `architecture.md:168` declares
  `ui/` browser-only and §29 sequences it at step 15, but `grep go:embed|embed.FS|FileServer|static
  asset|assets` over `architecture.md` **and all 31 ADRs** returns zero hits. The question is raised
  and left unclaimed at `research/observability-controlplane.md:1842-1845`. The next agent implements
  from `architecture.md:11` and reaches step 15 with no recorded answer; **the default path is a dev
  proxy, which is R11's own third symptom.** One sentence in an ADR closes it.

---

## R12 — Normative or draft — pick one

**Verdict: violated.** Ten surviving findings. The status discipline is perfect and the *content* of
the normative documents is wrong in specific, load-bearing places — which is worse than a draft
labelled as such, because "normative" is a promise a reader relies on.

### Got right

- **Every decision document picks exactly one status.** All 31 ADRs carry `**Status:** accepted,
  normative` on line 3; all four proposals say draft; `_completeness-audit.md` says DRAFT.
- **No ADR cites a draft document.** `grep -rl 'docs/research|proposals/|_decision-space'` over
  `docs/decisions/0*.md` returns no match, and `architecture.md:3-9` demotes both draft classes by
  name. This is the exact defect R12 was written about ("both RFCs were `Status: draft` while the
  architecture document cited them with MUST conform") and it does not recur.
- **All 31 ADR links in §28 resolve** — every `(decisions/NNNN-*.md)` target checked for existence,
  zero broken. R12's "README linked a design doc that did not exist" is absent.
- Every §N/§N.M cross-reference maps to a real heading except the §19.x family and §21b, both
  internally consistent.
- §30's measurable claims hold: 146 and 58 non-comment lines for `linefile`/`stdoutsink`; `go doc`
  gives Source 4 methods, Sink 3, Transform 3.
- `architecture.md:3886` and `internal/ledger/tracker.go:35` are both `type Ticket struct{ n *node }`
  **character for character**; `registry.Kind`'s nine constants match `pkg/registry/kind.go:12-20`.
- **`internal/` enforces the engine/ledger half of §3's import promise absolutely**: a third-party
  probe gets "use of internal package … not allowed" for `internal/engine`.

### Surviving findings

- **(fatal) §3 declares top-level package paths the module does not have, an import table wrong in 5
  of 11 rows, one edge that is now an import cycle, and a CI enforcement mechanism that does not
  exist.** `architecture.md:125-166,172-178,202`.

  **I recomputed the import graph by hand while writing this report.** Documented vs actual:

  | package | §3 says imports | actually imports |
  |---|---|---|
  | `config` | fault, schema | fault, **record** |
  | `spec` | config, fault | connector, fault, record, **registry**, schema, **telemetry** |
  | `telemetry` | record, fault, **schema** | **connector**, fault, record |
  | `registry` | connector, config, **spec** | connector, config |
  | `store` | record, spec, telemetry | **connector**, record, spec, telemetry |

  The `registry → spec` edge is **reversed in the code** (`pkg/spec/node.go` uses `registry.Kind`), so
  the documented graph is unrealisable: adding it produces `import cycle not allowed`.
  `grep -rn "TestDependencyDirection" --include='*.go' .` → **0 hits**, against `architecture.md:202`
  which promises that test walks the module graph and fails on any edge not in the table. **The only
  section saying what a third party may import gives nonexistent import paths and a false promise of
  enforcement.**
- **(major) ADR 0014 (accepted, normative) states thirteen fault classes and lists thirteen omitting
  `Throttled`, and nineteen `Op`s; the code has fourteen and twenty-one**, and no ADR carries any
  amendment metadata (`0014:31,34-36,83,108` vs `pkg/fault/class.go` and `op.go:8-34`; ADR 0030:79
  admits the 13→14 change). A reader of any accepted-normative ADR cannot tell whether its statements
  still hold.
- **(major) `Guarantee` is declared `spec.Guarantee` at three architecture sites plus ADR 0024, and
  `connector.Guarantee` in ADR 0029 and in §11 itself sixty-eight lines earlier.** `spec.Guarantee`
  does not exist (`go doc ./pkg/spec Guarantee` → no symbol). **Two accepted-normative ADRs disagree on
  one type's home with nothing saying which governs — R12's own historical defect item 2.**
- **(major) §22's two whole-program blocks, declared transcription targets, do not compile**: five
  nonexistent import paths, then eleven unknown-field errors against the delivered embedded-`Meta`
  `*Def` structs (`architecture.md:6137-6313,6322-6394` vs `pkg/registry/def.go:46-53`). An author
  following the normative guide fails at line 16, then at line 29.
- **(minor)** `README.md` never names `docs/architecture.md`; its Layout block and its one-sentence
  Reading order both stop at `docs/design-rules.md`. **The 7,001-line normative spine is unreachable
  from the repository's front door.**
- **(minor)** Ten of twelve files under `docs/decisions/reviews/` carry no status marker, and no
  blanket clause covers that directory — `architecture.md:3-9` demotes `proposals/` and `research/` by
  name and says nothing about `reviews/`. Adversarial reviews with numeric scores sit one directory
  below the ADRs with nothing saying they decide nothing.
- **(minor)** `architecture.md` contradicts itself on `BatchPolicy`'s owner: §3 (`:185`) says
  `config.BatchPolicy`, §10.2 (`:3205`) uses it unqualified inside the `package connector` block, and
  `connector.BatchPolicy` does not exist.
- **(minor)** The normative fault-class go block orders `Throttled` before `TransientInternal`; the
  code orders them the other way (`architecture.md:1013,1025,1029` vs `pkg/fault/class.go:25,29,50`),
  and `Class` has no `MarshalText`, so **the ordinal is today's JSON value** and the two are
  transposed on the wire. See C6 for why this specific pair matters.
- **(minor)** `internal/example/linefile`'s package comment asserts it must live where a third-party
  connector would live and that the core's types are not reachable from it; **both are false under
  `internal/`** (`source.go:7-10`).
- **(minor)** `architecture.md:13` sends the reader to §24 for the defect ledger, which is §25, and §30
  is presented before §29 in the file.

---

## R13 — State that implies persistence is persistent

**Verdict: partially-violated.** The rule has three clauses. **Tenancy — the clause the provenance
says was decided too late last time — is fully satisfied.** The durability clause and the
return-values-not-pointers clause are not.

### Got right

- **Tenancy is genuinely satisfied, and this is the rule's hardest clause.** `store.Key` is a struct
  with `Tenant` **first** (`key.go:33-37`), `Prefix` compares tenant before any part (`:55-57`), and
  all six key constructors take `TenantID` as parameter one. R13's "tenancy is decided before the first
  multi-tenant field, not after" is met exactly.
- **Tenant reaches every durable surface checked**: `config_store.go:16,20,30`, `status_store.go:23`,
  `coordinator.go:78,90,119`, `checkpoint.go:92`, `readmodel.go:16`, and `LabelTenant` heads
  `telemetry.Labels`.
- **`record.DeriveLaneID` (`ids.go:49`) percent-escapes every component**, so a pipeline id containing
  a slash cannot forge another tenant's lane. (Contrast `store.Key.String`, which does not — see R5.)
- `pkg/store` is six files of interfaces with **no implementation and no exported in-memory type**, so
  there is no dict-pretending-to-be-a-database. `ConfigStore.Put` (`config_store.go:24-28`) names R13
  and returns **the store's** revision, not the caller's.
- **`config.Config.Redacted()` (`config.go:224-236`) plus `redactTree` (`:238-266`) rebuilds every map
  and every `[]any`** — the whole recursion was read; it is the only form reaching the read model.
- `registry.Clone()` (`registry.go:293-324`) allocates fresh and copies all nine component maps;
  `record.Blob.Clone` (`ids.go:114-121`) works.
- `ledger.Leaks()` (`ledger.go:734-739`) transfers ownership under the mutex and `Flushable()`
  (`:454`) allocates a fresh map per call.

### Surviving findings

- **(major) `StoreCaps.Supports` gates only on `FlushIsDurable` and never reads `Durability`**, so an
  in-memory store declaring `FlushIsDurable: true` is accepted at **every tier including
  `exactly_once`**. `pkg/store/state_store.go:61-72` — **I re-read this function by hand; it tests
  `FlushIsDurable`, `CAS`, `EpochFencing` and `AtomicMultiKey`, and nothing else.** `StoreCaps{` has 1
  writer and `.Durability` has 0 readers. **The gate that exists so `Build` can refuse a deployment
  rather than trust it, trusts it.**
- **(major) `memstore`'s own comment claims `Build` refuses a pruning-upstream source because of
  `DurabilityNone`**, but the guard reads `!sc.FlushIsDurable`, which `memstore` declares `true`
  (`store.go:130-132,139` vs `negotiate.go:99`); its package doc `:4-5` says `false`. **Three mutually
  inconsistent statements about durability inside one 141-line file.** Same defect as R4's second
  major, seen from the store side.
- **(major) `engine.Build`'s returned `Negotiated` and `Pipeline.Negotiated()` share the `Nodes` map
  and `Why` slice with the pipeline's permanent record** (`build.go:94-95,257,261`), so any caller can
  rewrite the delivery contract. Probe on a repo copy: a later `p.Negotiated()` returned
  `exactly_once`/`commit`/`sink:LIE`. **The read model whose stated purpose is the honest answer to
  "what did I actually get" is rewritable in place** — R13's "read-model stores handed back live
  mutable records", verbatim.
- **(minor) `spec.Spec.Node` returns a literal pointer into the graph** (`spec.go:58-66` `&s.Graph[i]`)
  though all four internal call sites are read-only and the sibling `StreamFor` in the same file
  (`:105`) returns by value. A caching `ConfigStore`, contemplated at `store/doc.go:11`, plus `Node()`
  reproduces the historical defect exactly.
- **(minor) `connector.Persister` and `connectortest.StateHandle` clone durable-progress blobs on write
  but not on read** (`pkg/connector/state.go:141-176`; `connectortest/runtime.go:378-381,395-397`
  vs `:390,403`), so both cache the caller's token and every blob handed back. `record.Blob.Clone`
  exists and is used on the write path. **`AutoPersist` is the helper canal tells the ninety-percent
  source to use**; a reused token buffer silently corrupts its own persisted progress.
- **(minor) `config.Config.Raw()` returns the live configuration tree to any importer**
  (`config.go:51-57`), with a doc comment stating a purpose but never forbidding mutation. A connector
  mutating its config makes `Redacted()` and the read model disagree with what actually runs, unlocked
  and racy. `internal/stress/enterprise-scale/connector.go:541` documents it as an unsynchronised
  undocumented hatch and `:2456` calls it.
- **(minor) `record.Meta.Changes()` and `record.Record.Handle()` return internal slices** behind a
  comment saying the result must not be modified (`meta.go:175-177`, `record.go:94-95`). **A comment is
  not a guarantee.**
- **(minor) The four exported closed vocabularies are mutable package-level slices**
  (`telemetry/status.go:58`, `metrics.go:79,113`, `registry/kind.go:25`); one write to
  `telemetry.Labels` makes `LabelPermitted` reject `tenant` and admit an unbounded label, though
  `metrics.go:87` calls it enforced at registration. Subvertible from a plugin `init()` before any
  sandbox exists. Blast radius currently prospective — `LabelPermitted` has no callers.

**Note on citation quality.** The R13 verifier reopened roughly 45 `path:line` citations and found two
wrong, neither load-bearing. Across all nineteen audits, citation accuracy was the strongest dimension;
the weakest was consequence-reasoning, which is where most of the 100 kills landed.

---

# Verified claims

These six are not rules. They are load-bearing claims made *about* the delivered set, each of which
some decision downstream depends on. They were audited the same way.

---

## C1 — The mandatory core surface did not grow

**Verdict: satisfied-with-caveats.** The claim holds and the asymmetry it rests on was measured in
both directions.

**Confirmed.** Source 4 methods (`source.go:15,26,71,89,97`), Sink 3 (`sink.go:19,26,73,77`),
Transform 3 (`transform.go:24-37`), Buffer 7 (`buffer.go:19-54`). `StatefulTransform` is separate at
`transform.go:51`, not embedded. **The asymmetry is real and both halves were measured**: adding one
method to `Source` breaks 8 packages / 9 types / 10 registration sites; adding one to `SourceRuntime`
breaks **0** non-test packages. A comment-stripping scan of all 76 non-test `.go` files under `pkg/`
against a 38-token vendor list found 13 hits, **all substring false positives** (`orc` in
`Compressor`, `delta` in `ReconcileDelta`) — zero connector-specific knowledge in the core.
`withStageStandard` branches on **kind**, not connector (`stage_standard.go:39-56`). A third-party
source in an external module registers with zero edits to the repo.

**Caveat the owner should know.** `git log --all -- pkg/connector/` returns exactly one commit
(`a83f88e`), so every comparative "did not change" sentence is **unfalsifiable from the artifact**. The
claim is credible on structure, not on history.

**Surviving findings.**

- **(minor) Eight growth-path methods have zero call sites** (`LaneCtl.Table`/`AnnounceMany`/`Seed`/
  `Forget`/`Admission`, `SourceRuntime.Streams`/`Instance`/`Config`), and two stress connectors still
  assert in present tense that they do not exist (`enterprise-scale/connector.go:1463-1465` "l.Seed
  undefined" vs `lanectl.go:134`; `push-source/connector.go:1093` vs `runtime.go:79`). Deleting all
  eight leaves build, vet and 9 test packages green.
- **(minor) `internal/engine/doc.go` states unbuilt runtime behaviour as present fact** with no
  milestone marker (`:1` "assembles and runs pipelines", `:32-39` "Each node runs one goroutine")
  while `build.go:296` marks the identical items TODO.
- **(minor) `architecture.md:6879-6883`'s claim that embeddable bases make every future runtime method
  cost a connector's test suite nothing is not quite true**: adding a 16th `SourceRuntime` method
  builds clean but fails `multi-stream-source/drive_test.go:390`, whose `fakeRT` hand-writes all 15
  methods and embeds neither base. True cost today is 1 core file + 1 connector test package.

---

## C2 — The nine must-close decisions are genuinely closed

**Verdict: satisfied-with-caveats.** The auditor said partially-violated; its verifier overruled it
because seven of thirteen findings died and what survives shows **no decision left open** — one live
ledger bug, one dead telemetry field, one unimplemented default, four doc drifts. I agree with the
overrule, with one item promoted into the "decide now" list.

**Confirmed closed with a type, not just an ADR.** `pkg/record` depends only on `pkg/schema` plus
stdlib, so no transport DTO can become the internal type by omission (item 5).
`WriteResult{Failed, Deferred, Duplicates, Written, Bytes, DestToken}` keyed on `record.RecordID` is
reachable (item 4). `store.Key{Tenant, Space, Parts}` plus `record.DefaultTenant` make tenancy a type
property (item 6). `Checkpoint.Stamp()` and `WrittenByNewerBuild()` implement ADR 0020 rules 4 and 3
exactly as written, rule 3 as a predicate not an error (item 9). `connector.Durability` is a
four-member domain enum with the anti-bool reasoning inline; `Buffer` has no settle method and
`Accepted{Records,Refused}` gives `Put` a refusal path (items 1, 3).

**Surviving findings.**

- **(major) A discrete-ordered lane has no in-flight bound.** The `Tracker` enforcing the lane budget
  is built **only for prefix lanes** (`ledger.go:169-171`, re-read by hand), so `Admit` never blocks
  or refuses for the push-source lane kind. Probe: budget 8, **200,000 admitted, 0 refused**,
  `InFlight=0` with 50,000 groups open. **ADR 0003 point 1 and `architecture.md:5539-5543` are false
  for half the lane kinds.** See R6.
- **(major) `laneState.blocked` is assigned `false` at two sites and `true` at none**;
  `blockedSince` is never assigned (`ledger.go:102-103,281,310,631-634`). So `LaneStats.Blocked` and
  `connector.Admission.Blocked` **can never be true** — probe shows `Blocked=false, BlockedFor=0s`
  while a goroutine is parked in `Admit` at budget. ADR 0028's `LaneCtl.Admission.Blocked` is a
  delivered observable with no reachable source of truth (an ADR 0031 violation inside the ledger).
- **(major) ADR 0018's stated default (`ClockClamp`, `MaxSkew` five minutes) is neither implemented nor
  representable**: zero already means check-disabled (`pkg/spec/policy.go:78-84` "Zero disables the
  check entirely") and nothing defaults the field. `build.go:240-245` defaults `LaneBudget`, not
  `Clock`. **The zero value is spent, so the type cannot carry unset-vs-off** — this is the one C2
  finding that closes a door and it is promoted to list (b).
- **(minor)** Two versions of a check both documents call mandatory: ADR 0004 says
  `Written+len(Failed)==Count`, shipped `Reconcile` also subtracts `Deferred`
  (`sink.go:240-253` vs `0004:61`; `architecture.md:2350` three-term vs `:5801,5939,6596` two-term).
- **(minor) `Config.Secret` is a one-way gate**: it refuses non-secret fields, but `Get[T]` never
  consults `Field.Secret` (`config.go:206-216` vs `:100-121`). Probe: `Get[string](c,"password")`
  returns `"hunter2"` with a nil error. ADR 0017 rule 6's every-read-is-greppable claim is unenforced.
  Redaction itself is sound.
- **(minor) No delivered metric carries a tenant label**: 26 metrics all declare `{pipeline,…}`, and
  `LabelTenant` is declared and used by nothing, against ADR 0017 rules 1 and 8. `MetricNames` pins
  names not labels, so adding tenant later breaks no shipped consumer.
- **(minor) `_completeness-audit.md` is stale in one row**: `:131-141` asserts `spec.Spec` has no
  `Downgrades` field; it ships in the same commit (`spec.go:46-55`).
- **(minor) `record.NewAllocator` and `NewBatch` are exported**, so ADR 0005's
  provenance-unforgeable-by-construction claim is false, and `architecture.md:6717` separately
  *defends* the export. Two normative documents contradict each other on the exact defect ADR 0005
  says a reviewer once found fatal. See R2.

---

## C3 — The simple case is simple

**Verdict: satisfied-with-caveats.** The claim is true in code and false in the document that teaches
it.

**Confirmed, and this is a genuine achievement.** A 34-line source and a 30-line sink, rebuilt and run
with byte-identical output including `warm start replayed 0 record(s)`. Registration into
`registry.Default` via `init()` **from a module physically outside the repo**, with
`git status --porcelain` empty before and after. `go list -deps ./internal/example/linefile` yields
exactly `pkg/{schema,record,fault,config,connector,registry}` — no `internal/` leakage.
`SinkRuntime` has no `Lanes()` and no `State()`, so "a sink cannot get progress wrong" is enforced by
the interface rather than documented (`runtime.go:110-144`). `min_sink` with an empty
`config.NewSpec()` resolves to `[retry when_full codec batching max_in_flight dedupe]`, all appended
by the registry. **All six registration mistakes panic at init naming the field or Go interface that
fixes them** (`pkg/registry/add.go:38-89`).

**Surviving findings.**

- **(major) The NORMATIVE author guide's trivial source and sink do not compile** —
  `architecture.md:6138-6312,6323-6393` import every canal package without the `pkg/` prefix
  (`grep -c 'canal/pkg/'` = 0). Fresh probe: 5 errors and 4 errors. See R12/C6.
- **(major) After fixing the paths the same literals fail again**: they key
  `Name`/`Version`/`Title`/`Summary`/`Notes`/`Support` directly, but `registry.SourceDef` embeds `Meta`
  and Go forbids promoted-field keys (`def.go:47-58,79-86`). 6 and 5 further errors. **Two independent
  compile failures prove the normative example has never been compiled by anyone.** The shipped code
  nests correctly at `linefile/source.go:40-47`.
- **(minor) `architecture.md:6717` dismisses cold/warm-branch duplication as "four lines and the
  conformance kit tests both paths"**; the project's own reference source has **28 code lines**
  (`linefile/source.go:93-126`) and no kit exists. A normative tradeoff table understating an accepted
  cost 7× and citing a nonexistent mitigation.
- **(minor) The doc's `Read` diverges from the shipped reference twice**: a zero-record `EndOfLane`
  batch with no `Position` (`:6280-6284`), and `dst.Lane` retargeting (`:6266`) which
  `pkg/connector/source.go:33-35` forbids. **The normative guide teaches the retargeting pattern that
  caused documented settlement corruption** (`ledger.go:216-223`, the 33350/33500 scar at `:212`).
- **(minor) Checklist item 13 orders "Run the conformance kit. One function call. See §23"**
  (`:6418`); no such package exists and §23 is titled "The engine" (`:6422`). **Thirteen shipped code
  comments cite the kit in the present tense**, including `pkg/fault/class.go:17`,
  `pkg/telemetry/metrics.go:59`, `pkg/record/origin.go:32` — about a dozen obligations the code
  describes as machine-checked are prose only.
- **(minor) Every non-structured sink gets a codec block whose `encoder` field defaults to `"json"`**
  (`composites.go:118`), and **no encoder, framer or compressor is registered anywhere in the tree** —
  the seven `Add*` definitions have zero call sites. A shipped config field advertises a default naming
  a component ADR 0031's own producer rule requires to exist. See R3.

---

## C4 — An out-of-process implementation can satisfy the same interfaces later

**Verdict: partially-violated.** True on the source side — a working out-of-process
`connector.Source` was built against the shipped interfaces with **no core change**. False for
anything carrying a record.

**Confirmed good.** **No `io.Reader`/`Writer`/`Closer` and no `"io"` import anywhere in `pkg/`** —
Kafka Connect's stated wire-shippability failure mode is genuinely designed out. `connector.Ack`
round-trips through `encoding/json` verbatim and decodes back equal. `record.Blob{Version, Bytes}` is
JSON-tagged and is consistently the boundary shape for cursors, lane specs, state and committables.
`ResolveSource`/`ResolveSink` are exported and return structs of **nilable handles**
(`resolve.go:19-40,63`), with the wire reason stated at `:23-27` — the fatal found in a rejected
proposal is really fixed. `record.Position` is fully exported and JSON-tagged, and `position.go:39-44`
argues C4 correctly: comparability is optional **data**, not an optional method, because a method
cannot cross a process boundary. A verifier also built a host-side `connector.Buffer` over a wire
preserving ID/Group/Root/refs exactly, killing the auditor's claim that `Buffer` is unimplementable
remotely.

**Surviving findings.**

- **(major) `Batch.Records` is an exported unchecked slice and `record.NewAllocator` is exported**, so
  any connector can forge `Origin` and bypass the hard cap — and it is the only route `Buffer.Get` can
  use (`batch.go:36,75,188`). ADR 0005 decision 3 calls an exported emitter "fatal". Probe: hard cap 4,
  actual `Len` 21; forged `ID=9000 Group=77 refs=3` admitted.
- **(major) `record.Value`, `Payload` and `Meta` have no wire form.** `json.Marshal(Payload)` is `{}`,
  `Kind` is silently discarded, `Bytes` is indistinguishable from `String`, and decoding is a hard
  error. Zero `MarshalJSON` module-wide — against `value.go:16-18`'s claim that the record is
  "wire-shippable at every instant" and `:62-63`'s claim that `Kind` is "the wire form". **Every
  record-bearing plugin method loses payload silently over a wire, and each adapter must invent its own
  tagged union.**
- **(major) ADR 0015 point 9's CI wire-shippability generator does not exist**, and
  `architecture.md:6659` and `:6746` assert it twice in the present tense as a shipped, satisfied
  mechanism. **The sole named guard for this claim is absent, and three of these findings are live
  instances of the rot it would catch.**
- **(minor) ADR 0015 point 1's bold "no channels" and "(ctx, serialisable) → (serialisable, error)" is
  falsified by five delivered sites** (`lanectl.go:174,183,198,224`; `buffer.go:52`);
  `json.Marshal(connector.Admission{})` is a hard error. Doc-vs-code only: the channels are
  core-implemented and `lanectl.go:221-223` pre-argues the adapter correctly.
- **(minor) Enums on the plugin surface serialise as integers and base64** — `SourceCaps.LaneKinds`
  becomes `"AQI="`. Same root cause as R8's fatal; a non-Go plugin cannot be written against the
  declared wire form.
- **(minor) For a remote `StructuredSink`, `Rows[i].Ref().ID` is plugin-local while `Records[i].ID` is
  the host's** (`sink.go:157,165`), so one record has two identities out of process and one in
  process. ADR 0015 point 10 calls in-process/out-of-process divergence worse than loss.
  Adapter-solvable by index.

---

## C5 — The eight hostile connectors are a real regression test

**Verdict: partially-violated.** The corpus is real and it paid for itself — but it is decaying, and
three of its pins now contradict the interfaces it helped produce.

**Confirmed good.** All eight self-coverage figures reproduce exactly
(0.6/1.0/8.9/14.6/63.0/66.8/71.6/72.3). The interface-implementation half holds:
`registry.SourceDef[S connector.Source]` is a compile-time proof, plus 19 assertions at
`enterprise-scale:2909-2929` and 6 at `txn-sink:1609-1618`. **Five of eight genuinely catch
interface-shape drift** — a probe mutating `BacklogReporter` produced 4 build failures at the exact
cited lines plus `fanout`'s init panic at `pkg/registry/add.go:88`. `parallel-snapshot` is genuinely
load-bearing: `proof_test.go:97-114` drives the real `internal/ledger`, and `connector.go:758` is the
repo's only `ReadLanes` implementation. `schema-drift/regtest`'s locally restated schema channel is
load-bearing: renaming `SourceRuntime.Declare` fails it and breaks `multi-stream-source`'s build.

**Surviving findings.**

- **(major) `Batch.AddFor`'s stream argument can be made a no-op with the whole suite green**, because
  nothing in the repo ever calls `AddFor` with a stream of its own (`batch.go:187,199`; `Add` delegates
  at `:168`). Mutation `Stream: stream` → `a.stream`: 9 packages green. **`AddFor` was added because
  three hostile connectors argued the one-lane-many-streams case; not one adopted it.**
- **(major) `multi-stream-source` emits batches the shipped ledger refuses** with `PermanentContract`,
  while `TestDrive` passes because its only assertion is that `records` is non-zero
  (`drive_test.go:467,470,495,498`). Probe: `records=5650 wrongLane=5500`, ledger `admitted=3
  refused=100`. **The corpus holds both that guard's regression test and a connector violating it,
  green.**
- **(major) `enterprise-scale` is never constructed by any test** — `registration_test.go` is 25 lines
  of registry lookups, 0.6% coverage. Built through the registry it mislabels 296 of 320 records and
  the ledger refuses 37 of 40 batches (cause: `connector.go:1660` `dst.Lane = f.lane`, then `:1663`
  `dst.Add()`). **The corpus's largest connector, 2,930 lines, guards nothing beyond its compile-time
  assertions.**
- **(major) `no-cursor-source`'s `TestNoKeyIsSettable` pins a defect on a false premise**: the two
  lines it says do not compile **do** compile and work (`connector_test.go:409-411`,
  `connector.go:1114-1121` claim `r.SetKey undefined`; `pkg/record/record.go:82,92` ship it and five
  connectors call it). **The test forbids the connector from adopting a shipped fix** and pins
  `Caps.StableKeys` false.
- **(major) `push-source`'s `TestNoPromptRefusal` enforces the broken behaviour after the fix
  shipped**: `connector_test.go:261-263` fails if the refusal becomes prompt, while
  `pkg/connector/lanectl.go:198` declares `Admission` and its doc `:186-197` quotes push-source's own
  601 ms. **A pin that now contradicts the shipped interface.**
- **(major) Five stale-prose citations quote fabricated compiler errors for `pkg/` APIs that shipped**,
  and `enterprise-scale` contradicts itself inside one file (`:55/:1930/:1942` vs its own `:1699`
  REPAIRED). R8/R12-class doc-versus-code drift **living inside the regression corpus itself**.
- **(major) `WriteResult.Reconcile` can be made vacuously true with `go test ./...` green.** Mutated to
  `return true, want`: build OK, 9 packages green. Every call site tests only `!ok`
  (`txn-sink/connector_test.go:92`, `fanout:1314,1556`, `schema-drift:1941`). **The write-accounting
  identity — the one contract `WriteResult` exists for — has no protection anywhere.**
- **(minor) `schema-drift`'s connector body is never executed** at 1.0% coverage, and a trivial
  in-package test file compiles and runs, so there is no import-cycle reason for its absence.
- **(minor) No stress connector guards `Guarantee.Min`** (`guarantee.go:58`); flipping `if h < g` to
  `if h > g` fails only `internal/example/example_test.go:85`. The repo suite catches it — just not the
  eight.

**Note.** All 11 `pkg/` packages report `[no test files]`, so `pkg/` has zero unit tests of any kind.
The stress corpus is the only assertion the interface set has.

---

## C6 — Code and documentation agree

**Verdict: partially-violated.** The census is mostly reassuring; the divergence is concentrated, and
it is concentrated in exactly the sections a newcomer reads first.

**Confirmed good, and worth weighing against the findings.** An independent census reproduced 49 go
blocks, 5,001 lines, 46 interface declarations, 29 iota enums, 24 metric constants. **All 24 `M*`
metric constants match `pkg/telemetry/metrics.go` byte-for-byte, zero mismatches**; the code adds only
two. **An independent iota diff over all 29 enums confirms `fault.Class` is the sole positional
drift.** `Checkpoint` and `Header` in `internal/engine/checkpoint.go` match the doc field-for-field and
tag-for-tag. Only `Dispenser` has no code counterpart, and `:157` labels it "Not built in v1".
`go list -deps ./pkg/... | grep canal/internal` is empty. The §1 Rule 2 safety refusal is really
implemented at `negotiate.go:99,106`. Registry lint is genuinely aggressive — a probe needed
`APIVersion`, `UpstreamRetention`, `Boundedness`, `LaneKinds`, `Modes` and `MaxConcurrency` before it
would register, **which makes the two capability-table gaps an anomaly rather than laxity**.

**Surviving findings.**

- **(fatal) §3's dependency table disagrees with the code on 5 of 11 rows, declares a `registry→spec`
  edge the code has reversed, and names an enforcement test that does not exist.** Same finding as
  R12's fatal, recomputed independently; see the table there. §3's ban on connectors importing
  `spec`/`store`/`telemetry` is also unenforced — a third-party probe builds.
- **(major) §10.1's guarantee that registration panics on a declared-but-unimplemented capability is
  false for `reads_lanes` and `resolves_stale`**, and the doc never mentions `ResolvesStale` at all
  (`add.go:60-84` has 8 `capChecks`; `resolve.go:76,181` has both). **A `ReadsLanes` liar registers
  cleanly and its operator-facing `Descriptor` omits `reads_lanes` entirely**; failure moves to
  `Resolve` time. Same as R9's first major.
- **(major) §22, the connector-author guide the doc calls a transcription target, does not compile.**
  See C3. Plus three-way incoherence on `dst.Lane` (doc `:6266` vs `batch.go:77` vs `linefile:142`).
- **(major) The normative fault-class block transposes `Throttled` and `TransientInternal`** —
  `architecture.md:1003-1120` has `Throttled=2`, `pkg/fault/class.go:29,50` has `TransientInternal=2`.
  **This is the one pair whose entire purpose is that `Counted()` and `Blames()` differ between them**
  (`class.go:140-146,168`). With no `MarshalText` on `Class`, doc ordinal 2 decodes as
  `transient_internal`: `counted=true`, `blames=canal` — **the exact failure ADR 0030 added
  `Throttled` to prevent.**
- **(minor) Nine shipped struct fields and three signatures are absent from or contradicted by the
  normative document**, including `CodecCaps`, which §10.1 says embeds `Caps` twenty-four lines before
  §9.3 declares it without (`codec.go:80,86` vs `:2833,2857-2860`). A codec author transcribing §9.3
  writes a struct that neither compiles nor carries `Produces`; a caller of `Revoke` written from the
  doc discards the revoked epoch.

---

# What to do about it

Three lists. The division is not by severity but by **what deferring costs**, the same axis
`_completeness-audit.md:41-53` uses.

---

## (a) Must be fixed BEFORE any more breadth

Every item here is a defect in delivered, non-stubbed code. Ten items; none is large. Ordered by
consequence.

1. **`applyWaivers` must read `Requested`, `Effective` and `Missing`, not just `Node`,
   `AcknowledgedBy` and `Reason`.** `internal/engine/negotiate.go:304-328`. Today one signed waiver for
   any stated reason downgrades **every** capability and guarantee error on a node. This is the
   data-loss guard ADR 0006 exists to provide, and it is disabled by a struct literal. *Also fix ADR
   0024's scoping statement or the code, whichever is wrong.*

2. **Fix the `Ledger.send` / `Ledger.Close` race.** `internal/ledger/ledger.go:574-584` vs `:778-796`.
   Send-on-closed-channel panic plus a confirmed `-race` report, **in the shutdown window
   `build.go:311-317` explicitly reserves for acks still being delivered.** The engine's first
   execution will hit this. Hold the mutex across the send, or use a done-channel select.

3. **Fix the ledger group-id reuse overwrite.** `internal/ledger/ledger.go:304` unconditionally
   assigns `l.groups[b.Group()] = g`, orphaning the prior tracker ticket; reachable from a connector
   via the exported `(*record.Batch).SetGroup`. **The prefix stalls forever and the TTL leak reaper
   never names it — this is the only failure mode in the ledger with no detector.** Refuse the reuse
   in `Admit`, which already refuses lane disagreement 80 lines earlier.

4. **Make `StoreCaps.Supports` read `Durability`, and fix `memstore`'s three contradictory durability
   statements.** `pkg/store/state_store.go:61-72`; `internal/example/memstore/store.go:4-5,130-132,139`.
   Today a `DurabilityNone` store passes `exactly_once`, and the refusal `memstore`'s own comment
   promises does not exist. **This is the R4 defect shape — a documented refusal that isn't there over
   a durability claim that's false — which is the exact mechanism that killed the last attempt.** Also
   fixes R13's first major and R4's second.

5. **Bound the two unbounded backpressure paths.** (i) `Tracker.TrackResolved` appends zero-weight
   nodes with no cap (`tracker.go:112,140-161`) — 419.6 MiB at 5M nodes, against
   `architecture.md:5534`. (ii) Discrete-ordered lanes get **no tracker at all**
   (`ledger.go:169-171`), so `Admit` never blocks or refuses — 200,000 admitted against a budget of 8.
   ADR 0003 point 1 is currently false for half the lane kinds. While there, assign
   `laneState.blocked`/`blockedSince` so `Admission.Blocked` can be true, and coerce `NewBatcher`'s
   trigger-less policy the way `NewTracker` already coerces a zero budget.

6. **Decide the enum wire form and enforce it structurally.** 29 of 31 `uint8` enums have no text
   marshaller; eight `[]uint8` capability fields serialise as base64 **today, in both shipped
   descriptors**; `registry.Support` and `CapSource` have `MarshalText` without `UnmarshalText`, so a
   `Descriptor` cannot decode its own encoding. **Reproduced by hand in this audit:
   `"lane_kinds":"AQ=="`, `"boundedness":null`, `"guarantee":1`.** Generalise `ParseWhenFull`'s pattern
   (`pkg/connector/buffer.go:162-170`), which already makes `String` and `Parse` structurally
   inseparable. This is (a) because it is live, and it is also (b) — see below.

7. **Add `omitempty` to the eight nil-marshalling slice fields**
   (`readmodel.go:37,47,48,49,50,63`; `negotiated.go:32,52`). This is **R8's named historical defect
   verbatim** — `null` against a required array, past a test asserting `len(...) != 0`. Two-character
   fix per field, and it must land before anything constructs a `PipelineStatus`.

8. **Close `Build`'s validation holes.** Add a `default` arm to the node switch
   (`build.go:144-221`) so encoder/decoder/framer/deframer/compressor nodes are validated,
   constructed and closed rather than silently passed through into the persisted spec; resolve codec
   names (ADR 0022:75-77 requires it and `build.go:106-107` claims it); and either register an encoder
   or stop defaulting `codec.encoder` to `"json"` (`composites.go:118`). Also add `reads_lanes` and
   `resolves_stale` to `add.go`'s capability tables so the documented init-time panic is real.

9. **Fix `stdoutsink`'s `Sync` guard** (`internal/example/stdoutsink/sink.go:66`). Pipe, `/dev/null`
   and a real tty all escape the `os.ErrInvalid` test and yield `Indeterminate`/`Written=0` **after
   the bytes have left the process**. This is the reference sink a third-party author copies. Cheap,
   and it is the single clearest piece of evidence for why the engine must exist.

10. **Fix §3 and §22 of `architecture.md`, or demote the document.** §3's package layout names paths
    that do not exist, its import table is wrong in 5 of 11 rows and declares an edge the code has
    reversed into a cycle, and it promises a `TestDependencyDirection` that has zero hits. §22's two
    whole-program blocks — declared transcription targets — fail to compile twice over. **A document
    whose first line says every statement is binding cannot have a wrong dependency table in the one
    section that says what a third party may import.** Either fix them or write
    `TestDependencyDirection` for real; the second option fixes the first permanently.

**Then build the engine.** Roughly 750 lines by the R3 auditor's estimate: `store/single` on bbolt,
an ndjson encoder, the node loops and commit pump the `Run` TODO already specifies, a `main`, and one
`kill -9` test. **Nothing on this list should be deferred past that point, because items 1–5 are all in
code the engine executes on its first run.**

---

## (b) Must be DECIDED now — deferring forces a breaking change

**Start with `docs/decisions/_completeness-audit.md:630-651`, which already lists ten of these and
argues each one properly.** That list is correct and this report does not duplicate it. In particular
its items 1 (`config.DecodeRef` and the decode half of serialization), 2 (where operator intent lives),
3 (`Node.ID` immutability and the `Impact` enum), 4 (`PipelineStatus` as a summary document), 5 (the
dead-letter envelope) and 6 (the conformance kit's import permissions and `Harness` surface) are
unaffected by anything found here and remain the right priorities. Note only that its `:131-141` row
about `spec.Spec` lacking `Downgrades` is now stale — the field ships.

**Six additions this audit found, each of which closes a door the completeness audit did not see:**

1. **Is the wire form of an enum the token or the ordinal?** Ordinals are already in shipped
   descriptors and will be in the first persisted spec and checkpoint. Deciding "ordinal" is a
   defensible answer — but then ~30 doc comments claiming the token is the wire form must be corrected
   and no `iota` member may ever be inserted. Deciding "token" costs 29 marshaller pairs today and a
   migration later. **The completeness audit's `PipelineStatus` item assumes this is already settled;
   it is not.** (R8 fatal, R9 second major, C4 fifth.)

2. **Which dedupe representation survives?** `spec.Streams[].Dedupe` (read by the negotiator) or the
   stage-standard per-sink block (read by nothing but rendered into every sink's
   `Descriptor.JSONSchema`). Both are operator-visible and one is persisted. **This is R1's literal
   "one entity, two identifiers" defect and it will be a config migration the day after an operator
   writes either one.** (R1, R5.)

3. **`spec.ClockPolicy`'s zero value is already spent.** `policy.go:78-84` makes 0 mean
   check-disabled, so ADR 0018's stated default of `ClockClamp`/5 min is unrepresentable and nothing
   defaults it. Adding unset-vs-off later changes the meaning of a persisted zero. Fix now with a
   sentinel, a pointer, or an explicit `ClockDisabled` member. (C2.)

4. **Is `record.Batch`/`Payload`/`Meta` required to be wire-shippable?** Today `json.Marshal` of a
   `Batch` succeeds with `err == nil` while losing every `Origin` field, the whole `Payload`, `Meta`
   and the handle. Deciding "no, the remote seam uses its own encoding" is fine and cheap — but it must
   be written down, ADR 0015's claims corrected, and `value.go:16-18`'s "wire-shippable at every
   instant" deleted. Deciding "yes" later changes `pkg/record`, the one package everything imports.
   (R2, C4.)

5. **Is provenance unforgeable, or is it not?** ADR 0005:49-52 says an exported emitter is fatal;
   `record.NewAllocator`, `record.NewBatch` and `Batch.Records` are all exported and
   `architecture.md:6717` defends the export. **Two accepted-normative documents contradict each other
   on the point ADR 0005 says a reviewer once found fatal.** Pick one. If unforgeable, the allocator
   moves behind an internal interface, which is a breaking change for any connector already written.
   (R2, C2, C4.)

6. **Can a buffer be the durability edge, and how is that disclosed?** `negotiate.go:214,219-229`
   hardcodes `DurabilityEdge` to `sink:<node>`, while `telemetry/negotiated.go:63` documents
   `buffer:wal` as a legal value. `DurabilityEdge` is a pinned wire field on `Negotiated`, which is
   persisted in `spec.Spec`. Latent today because no buffer is registered — **which is exactly why it
   is cheap now.** (R4, R6.)

**One more, half-decision — now decided.** `pkg/telemetry`'s 41 camelCase JSON tags against the rest
of `pkg/`'s 78 snake_case. "Free today and a wire break the day the API ships" was exactly right, and
the API shipped with `GET /status`, so it was taken then: `Negotiated`'s eight snake_case tags became
camelCase, making the whole read model one convention. See the resolved entry in (b) for why the rest
of `pkg/` deliberately did not follow.

---

## (c) Safely deferred

Real findings, none of which closes a door. Fix them when convenient, or when the surrounding code is
touched.

- **`pkg/` has zero unit tests and no golden files.** Genuinely needed, and genuinely not urgent
  relative to (a): the stress corpus is a real assertion and adding unit tests to a surface that is
  about to be exercised by an engine is lower value than the engine. Revisit immediately after
  milestone one.
- **Stress-corpus decay** (C5): `multi-stream-source` and `enterprise-scale` emit batches the shipped
  ledger refuses while their tests stay green; `no-cursor-source` and `push-source` pin defects that
  have since been fixed; five stale-prose citations quote compiler errors that no longer occur.
  Worth a single cleanup pass, best done *after* the engine exists so the corpus can be re-driven
  rather than re-read.
- **`Reconcile` set-membership checking** (R7). The cardinality identity already catches the canonical
  honest-under-reporting shape; only a malformed naming escapes. Add the membership check when the
  conformance kit is written, where it belongs.
- **`pkg/connectortest`'s false inertness claim** and its two testable-today divergences
  (`LaneCtl.Announce`'s hardcoded lane id, `Schemas.Register`'s zero fingerprint). Note that fixing
  `Announce` **breaks a currently green test** (`push-source/connector_test.go:127`), so schedule it
  with the C5 cleanup.
- **R9's vocabulary duplications**: the two `Backlog` structs, `schema.FieldNote` vs
  `record.FieldChange`, the hand-typed retry/indeterminacy token sets, the three `Phase*` vocabularies.
  Each is a genuine modelling error by R9's own test, and each is additive to fix. The token-set
  duplications should be folded into (b) item 1 when it is decided.
- **Doc hygiene**: `README.md` never names `architecture.md`; `docs/decisions/reviews/` has no status
  markers; §24/§25 and §29/§30 numbering; `internal/engine/doc.go`'s present-tense runtime claims;
  `internal/example/linefile`'s false comment about where it lives; the thirteen code comments citing a
  conformance kit that does not exist; ADR 0014's stale class and Op counts; ADR 0004's missing
  `Deferred`. **Individually trivial. Collectively they are why R12 is violated** — consider one pass
  that adds an "amended by" line to ADRs and a `Status:` line to `reviews/`.
- **How the UI reaches a browser** (R11). One sentence in an ADR, needed before step 15, not before
  step 8.
- **Eight zero-caller growth-path methods** (C1) and `registry.Support` having no scaffolding value
  (R10). Both wait naturally for the conformance kit.

---

# Closing

**Verdict: fix a few things, then build the engine.**

Not "the interfaces are sound, go build" — because ten items in list (a) are defects in shipped code
and five of them are in the exact machinery the engine calls first. Not "there is a structural problem
that more code makes worse" either — because the structure is largely right, and the audit found more
to preserve than to repair.

**What is genuinely good here, and should not be disturbed by any of the fixes above:** the record
model is decided, complete and transport-free, which is R2's whole demand. The topology is a graph
with derived fan-out and no ordinals. The buffer contract is bounded by construction with a typed
refusal path — R6, answered properly. `WriteResult.Failed []fault.RecordFault` keyed on `RecordID` is
R7's literal demand met. Tenancy is a struct field in position one on every durable key, which is R13's
hardest clause. One Go binary, zero dependencies, zero non-Go files, and a browser that owns nothing.
Registration panics at init on six different mistakes, each naming the fix. A 34-line source and a
30-line sink that register from outside the module. Those are expensive properties and most projects
do not have them.

**The one thing that is structurally wrong is a sequencing decision, not a design decision**, and it
is reversible for about 2% of the code already written. The measurable cost of not reversing it is
already visible: four guards that have never executed and are wrong, discovered by this audit rather
than by a test. That number grows with every additional interface and drops to near zero the moment
one record moves.

Build the path. The interfaces will survive it — that is what the hostile-connector pass was for, and
it is the finding this audit is most confident in.

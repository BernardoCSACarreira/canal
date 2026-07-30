# Review: `stdlib-capability` — lens: **Go ergonomics and implementability**

**Reviewer stance:** hostile expert. The only question I am answering is whether a competent Go engineer
would enjoy implementing against this interface set, and whether the signatures as literally written can
be implemented at all. I am not judging whether the architecture is *right* — other lenses own that.

**Score: 6 / 10.**

**Verdict.** This is real Go, not transliterated Java. The required surface is genuinely tiny and
idiomatic, the simple case is genuinely simple (~30 lines for a working source), generics appear only on
accessors, `context.Context` is on every blocking method, and the `bound` struct-of-closures is the best
single Go idea in the whole proposal set — it is the correct answer to "a type assertion cannot cross a
process boundary" and it is *cheaper* than the interface dispatch it replaces. Against that: the document's
only deliverable is signatures, and several load-bearing signatures are wrong in ways that break the
proposal's own headline claims. There is no way for a source to report a per-record position, which makes
the per-record prefix resolver in its own walkthrough unimplementable and, if implemented naively,
data-lossy. Reader-tier capabilities are probed once per pipeline while `Source.Reader` legitimately returns
different concrete types per assignment — which the flagship walkthrough requires — so DG-3 cannot hold
where it matters most. `CapabilitySet`'s derived fields span tiers the type cannot see. Nine of
twenty-nine capabilities have no `bound` field, so the bijection test the whole DG-1 claim rests on cannot
pass. None of these is unfixable; most are a field or a method away. But "the interface set" is the
deliverable, and this interface set does not yet close.

Score reasoning, explicitly: baseline for a Go-idiomatic, small-required-surface design with a working
`canalkit` and a real conformance kit is **8**. Subtract 2 for the two fatal signature holes (§B1, §B2),
which are not stylistic — they invalidate the resumability and honesty guarantees the design is *for*.
Subtract 1 for the cluster of concurrency-contract omissions (§B6, §B9) that a data-movement tool cannot
ship without. Add 1 back for `bound`, `canalkit`, the frozen-required-interface + growth-in-request-struct
rule, and unexported `Origin` — four things that are better here than in any plausible alternative and are
worth transplanting whatever wins. **6.**

---

## A. What this gets right, specifically

These are not generic praise; each is a named mechanism I would keep verbatim.

**A1. `bound`: capability dispatch as a struct of function fields.** §6.3–6.4. After the probe, the engine
holds `bound{position func() Cursor; chunks func(...) (...); ...}`. This is idiomatic Go (closures over
method values), it costs one indirect call instead of an interface method lookup plus a type assertion,
it is trivially fake-able in a test (`bound{position: func() Cursor { return c }}` — no interface to
implement, no mock library), and the RPC case fills the same fields. It is strictly better than Benthos's
nine hand-written forwarders and strictly better than asserting at call sites. Whoever wins, steal this.

**A2. Frozen required interfaces; growth happens in request/response structs.** `OpenRequest`,
`WriteOpen`, `AckRequest`, `PlanRequest` mean adding a parameter is not a breaking change. Stated as a rule
in the doc (§4, "Growth strategy"). This is the single highest-value ergonomic decision in the document and
it is the direct fix for trap 9 in the decision space.

**A3. Interface sizes are correct.** `SourceFactory` 2, `Source` 1, `Reader` 2; `SinkFactory` 2, `Sink` 1,
`Writer` 2. Optional interfaces are 1–3 methods each. `Read(ctx, dst *RecordBatch) error` returning `io.EOF`
is the `io.Reader` shape, not a `poll()`-returning-a-collection shape. `Transform` is one method.

**A4. Unexported `Origin` + `Derive()`.** Provenance immutability enforced by the language rather than by a
javadoc warning. Two lines of API for the whole KIP-793 problem class. Costs a `Record` copy in `Derive`,
which is the right trade.

**A5. `Close` gets a fresh context with the shutdown grace period, never the cancelled read context, and
is called exactly once.** That sentence (§4, `Reader.Close`) is worth copying verbatim into whatever wins.
It is the single most common Go framework bug and almost nobody writes it down.

**A6. Generics are used exactly where they help and nowhere else.** `FacetOf[T Facet]`, `Get[T]`,
`GetOK[T]` — the type parameter is on the accessor, never on `Record`, `Source`, `Sink`, `Registry`. The
rejected-alternatives comment on `Value` (§2.2) states the reason correctly: "a type parameter that must be
erased at the registry boundary buys nothing." That is the right rule and it is stated in the right place.

**A7. `canalkit` / `canaltest` / `canal` split, with `canal` importing stdlib only.** Adapters are not in
the contract; the conformance kit is not in the contract; the contract has no dependencies. A connector's
dependency footprint is `canal` plus its driver. This is how `database/sql` and `io/fs` are shaped.

**A8. `MaxAttempts: 0` is a config error, not "infinite".** One line that kills the livelock class.

**A9. `RetrySafe` documented as the `ErrBadConn` predicate** — "may report `RetrySafe` only when it KNOWS
the effect did not land" — with `canaltest` asserting it. The strongest single sentence in the document.

**A10. `Registry` as a value type with `Clone/With/Without` plus a default global, and `Snapshot()` taken
once at pipeline build.** Testable registry, no init-order coupling in tests. (The `any` parameter is a
defect — §B20 — but the shape is right.)

---

## B. Defects

Ordered by severity. Each names the exact interface, field or interleaving at fault, states the concrete
consequence, and gives the cheapest fix I can see.

### FATAL

#### B1. There is no API by which a source can report a per-record position — and the design's resume story requires one

**At fault:** `Record.origin` (unexported, §2), `func (r *Record) Origin() Origin` (getter only, no setter),
`RecordBatch.Append(r *Record) (ok bool)` (§2.4), `canalkit.BytesRecord(stream, body)` (§14a),
`Positioner.Position() Cursor` (§4.1, Reader tier — *batch* granularity).

`Origin.Cursor` is documented as "the source position AFTER this record". `Origin` is unexported, has no
setter, and §14a step 8 states plainly that **the engine** assigns it. The only channel a Reader has for
positional information is `Positioner.Position()`, whose own doc comment says it returns "the cursor
immediately after the last record appended by the most recent `Read`" — one cursor per `Read` call, not per
record. `canalkit.BytesRecord` takes no cursor. `Append` takes no cursor.

So there is no expressible path from "this row is at LSN 4711" to `Origin.Cursor`.

**Concrete consequence.** Walkthrough (c) is the proposal's proof that it loses no data. Step 3 says: the
committed set is `{1041..1382, 1402}`, so "`AssignmentState.Resume` advances to record 1382's cursor and
**not one record further**." Record 1382's cursor does not exist and cannot exist. The engine's only
available value is the batch-level cursor obtained after `Read` returned all 500 records, i.e. the position
*after record 1540*. Two outcomes, both bad:

- The engine stamps the batch-end cursor onto every record in the batch (the only thing it can do with
  `Positioner`'s output). Then the "longest contiguous committed prefix" through `Seq 1382` resolves to a
  cursor positioned after 1540, and a `kill -9` at that instant resumes past 158 records that were never
  durable at the sink. **Silent data loss, in the exact scenario the design claims to make structurally
  impossible, violating R4 in the one place the proposal advertises R4-as-a-type.**
- Or the engine refuses to advance `Resume` until the entire batch is terminal. Then the resolver is
  batch-granular, not per-record; §3's `AssignmentState.Resume` comment ("the end of the longest contiguous
  committed prefix ... Computed by the engine's prefix resolver, per assignment, from acks") is false; and
  one poison record pins the whole assignment's progress for as long as it retries — the head-of-line
  behaviour the per-record resolver exists to avoid.

**Fix (cheap).** Give the batch a cursor-carrying append, or better, invert it (see §B15):
`rec := dst.Next(); rec.SetCursor(cur)` where `Next()` hands back an engine-owned slot whose `origin` the
package can write because it is in-package. That preserves unexported `Origin` (A4) *and* makes the position
per-record. `Positioner` then degrades to what its comment says it is: the coarse fallback for sources that
genuinely cannot do better — and admission can report which one it got, which is the design's own idiom.

#### B2. Reader-tier capabilities are probed once per pipeline, but reader shape is legitimately per-assignment — and the flagship walkthrough requires it

**At fault:** `Source.Reader(ctx, req OpenRequest) (Reader, error)` returning an **interface** (§4);
`admit.Request{ReaderCaps canal.CapabilitySet}` — exactly one reader-tier set per pipeline (§11); DG-1 ("no
type assertion after `Start`"); reader-tier capabilities `Positioner`, `Phased`, `Bounded`, `Acknowledger`,
`LagReporter` (§4.1).

Go interface satisfaction is a property of the concrete type. `Reader` is returned as an interface, so a
source is free — and in the natural implementation is *encouraged* — to return `*snapshotReader` for a
bounded chunk assignment and `*streamReader` for the unbounded tail. `*snapshotReader` implements
`Bounded` (it has a row count) and not `LagReporter`. `*streamReader` implements `LagReporter` and
`Positioner` and not `Bounded`. Walkthrough (b) is precisely this shape: 400 bounded chunk assignments plus
one unbounded log-tail assignment, on one `Source`.

There is exactly one `ReaderCaps` in `admit.Request`, produced by one probe of one reader, and DG-1 forbids
re-probing later. `CapabilityReporter` masks per *configuration*, not per assignment.
`ErrCapabilityDeclined` is explicitly illegal after `Start`.

**Concrete consequence.** The connector author has two options and both break a design guarantee:

- Return distinct types per assignment. Then `ReaderCaps` describes whichever type the dry-run probe
  happened to construct. The capability report — the artefact the entire proposal is built to make
  trustworthy — is **wrong for 400 of 401 assignments**, and `bound.remaining` is either nil where the mask
  says present (a nil call in the engine, which §6.4 asserts is impossible) or present where the type does
  not implement it. DG-3 ("declared must equal implemented") is dead exactly where the design's value is.
- Collapse everything into one `reader` type implementing all five. Then `Bounded.Remaining` on the log tail
  must return `(0, false)` and `LagReporter.Lag` on a chunk reader must return `Records: -1`. The mask says
  present; the answer is "I cannot tell you". That is *precisely* the invisible degradation DG-2 exists to
  kill, and it is now unexplainable, because `CapabilityEntry.Reason` "MUST be empty when `Present` is true"
  (§6.2). See also §B14.

**Fix.** Make reader-tier capability a property of the `(Source, Assignment)` pair, probed at `Reader`
construction, and let the *plan* carry per-assignment capability rather than the pipeline carrying one set.
This is a real cost — admission then reasons over a per-assignment lattice — but the alternative is a
capability report that is honest about sinks and lying about readers. Alternatively, forbid per-assignment
reader types in the contract (`Source.Reader` must return the same concrete type for every assignment) and
say so in the doc comment plus a `canaltest` assertion. That is the cheap option and it should be chosen
consciously, not by omission.

#### B3. `CapabilitySet` carries one `Tier` but its derived fields span tiers, and four declared masks map onto three documented tier groups

**At fault:** `CapabilitySet{Tier Tier; Mask CapMask; Resumable bool; Chunkable bool; ProgressKnown bool;
AckPoint AckPoint}` (§6.2); `Spec{SourceCaps, ReaderCaps, SinkCaps, WriterCaps CapMask}` (§7); the
`CapabilityID` enum's comment groups: `// Source tier`, `// Reader tier`, `// Sink tier` (§6.1).

Three concrete inconsistencies in one type:

1. `Resumable = CapPosition ∧ CapSeek`. `CapSeek` is source tier; `CapPosition` is reader tier. Neither
   `SourceCaps` nor `ReaderCaps` can compute it — a `CapabilitySet` with `Tier: TierSource` does not contain
   `CapPosition` and therefore cannot fill its own `Resumable` field. The derived field is on the wrong
   type. Same defect for `ProgressKnown` (`CapBounded` is reader tier, "a Chunker with an exact Total" is
   source tier).
2. `AckPoint = Token > Commit > Flush > Write`. `Flusher`'s own doc comment is written about the **Writer**
   ("unless this `Writer` also implements `Flusher`", §5), but `CapFlush` sits under `// Sink tier` in the
   enum. `Spec` has both `SinkCaps` and `WriterCaps`. **A sink author cannot determine which mask to declare
   `CapFlush` in**, and DG-3 makes a wrong guess a `PermanentContract` refusal — the pipeline does not start,
   and the error message is about a declaration mismatch rather than about the real problem.
3. The enum documents three tier groups; `Spec` declares four masks. There is no `// Writer tier` group. So
   for every sink-side capability the tier is genuinely undefined, not merely unstated.

**Concrete consequence.** The one type the UI, admission and the engine all read cannot be populated
consistently, and the most common connector-author mistake (a capability declared in the wrong mask) fails
at admission with a contract error rather than at compile time. Given that DG-3 turns declaration mismatch
into a hard refusal, the tier taxonomy has to be exact data before any connector is written.

**Fix.** Put `Tier` on the capability **table row** (it already is — `row.Tier`) and make it the single
source of truth; delete the enum's comment groups; replace `Spec`'s four masks with one `CapMask` plus the
table's tier lookup (or, better, derive the declaration from the type entirely — §B12). Move the derived
conjunctions (`Resumable`, `ProgressKnown`, `AckPoint`) off `CapabilitySet` and onto the `Plan`, which is
the only object that legitimately sees all four sets.

### MAJOR

#### B4. Nine of twenty-nine capabilities have no `bound` field, so DG-1's bijection test cannot pass

**At fault:** the `bound` struct (§6.4) versus the `CapabilityID` enum (§6.1).

`bound` declares 26 function fields. Cross-referencing against the enum, these capabilities have **no field
to bind into**: `CapSeek` (`Seekable.CursorVersion`), `CapReplay` (`Replayable.ReplayWindow`),
`CapReportCaps` (`CapabilityReporter.Capabilities`), `CapIdempotent`
(`Idempotent.IdempotencyKeyFields`), `CapRequireKey` (`KeyRequirer.RequiresKey`), `CapBatchPolicy`
(`BatchPolicyProvider.BatchPolicy`), `CapStructuredInput` (`StructuredWriter.AcceptsStructured`),
`CapPartialTolerant` (`PartialTolerant.TolerantesPartial`), and half of `CapSchemaApply` — `bound` has
`applySchema` but no `supportedChanges`, though `SchemaSink` has both methods.

§6.3 asserts "a test asserts the bijection between mask bits and non-nil fields for every row in the table."
That test cannot pass: nine rows have a mask bit and no field.

**Concrete consequence.** Those nine capabilities will be reached the only way left — a type assertion at
the call site, in `admit` or `engine`, which is DG-1 violated nine times over in the exact code the rule was
written to protect. Worse, `CapabilityReporter` is one of them, and it is the mechanism by which the probe
learns the *reported* mask, so the probe depends on a capability it cannot bind.

**Fix.** Either bind them (most are `func() T` — trivial) or split the table into "bound capabilities" and
"declared-value capabilities" and state the invariant per class. The second is honest and cheaper: several of
these are pure values read once at admission, not behaviours invoked at runtime, and forcing them through
`bound` would be ceremony. But the invariant must then be written for two classes, not one.

#### B5. Three capabilities are marker interfaces whose bool return the probe never reads

**At fault:** `KeyRequirer interface{ RequiresKey() bool }`, `StructuredWriter interface{ AcceptsStructured()
bool }`, `PartialTolerant interface{ TolerantesPartial() bool }` (§5.1), against
`Probe: func(v any) bool { _, ok := v.(canal.X); return ok }` (§6.3).

The probe is a type assertion. It never calls the method. So `RequiresKey() bool { return false }` yields
`CapRequireKey: present`.

**Concrete consequence.** A sink whose key requirement depends on its configured write mode (`upsert` needs a
key, `append` does not) writes the obvious `func (w *w) RequiresKey() bool { return w.mode == modeUpsert }`
and gets a pipeline **refused at submit time in append mode** — admission "checks it against the source's
catalog and refuses the pipeline" (§5.1) on the strength of a mask bit whose value was never consulted.
Symmetrically, `AcceptsStructured() bool { return false }` causes the engine to "SKIP the encoder stage
entirely" (§5.1), so a sink that wanted bytes gets `Value`s. That is a wrong-data bug reachable by writing
correct-looking Go.

**Fix.** Either make them pure markers (`type KeyRequirer interface{ requiresKey() }` — or better, no method
at all: `interface{ KeyRequired() }`, name-only) or give the table row a `Probe` that calls the method:
`func(v any) bool { k, ok := v.(canal.KeyRequirer); return ok && k.RequiresKey() }`. The table row already
supports this — the row is data and `Probe` is a closure — so this is a two-line fix per row, but it must be
decided, because "implements the interface" and "answers true" are different facts and the design currently
conflates them.

#### B6. `Payload`'s lazy cache is a data race under the fan-out the design ships

**At fault:** `Payload struct{ bytes []byte; structured Value; which uint8 }` with `Bytes(ctx)`/
`Structured(ctx)` documented to convert "lazily and cache" (§2.1); `RecordBatch.Records []*Record`;
§13's fan-out ("a registered `Sink` whose config has a component-valued `sinks: []Sink` field") and
`WriteOpen.Partition 0..N-1`.

Fan-out means two `Writer`s receive the same `*Record`. Multiple write partitions mean the same. Both writers
call `r.Value.Bytes(ctx)`. `Bytes` writes `p.bytes` and `p.which` to populate the cache. There is no mutex,
no `sync.Once`, no atomic, and no documented rule that a record is owned by one goroutine at a time — the
ownership contract (§2.4) constrains *retention*, not *concurrent access*.

**Concrete consequence.** `go test -race` fails on the first fan-out pipeline; in production it is a torn
`which` byte against a half-written `bytes` slice header, which reads as an empty or corrupt payload. This is
not exotic: fan-out is in canal's end-state goals and is listed in §13 as costing zero core code.

**Fix.** State the rule: a `*Record` is owned by exactly one goroutine between engine hand-offs, and fan-out
deep-copies (or the engine pre-materialises both views before splitting). Or make `Payload` immutable with an
engine-side memo table. Either way the concurrency rule has to be in the doc comment on `Payload`, because
this is the one type every connector touches.

#### B7. `Payload.Bytes(ctx)` is unimplementable as documented, and `Record.Key` can never be encoded

**At fault:** `func (p *Payload) Bytes(ctx context.Context) ([]byte, error)` — "encoding the structured view
on demand using the pipeline's configured `Encoder`" (§2.1) — versus `Payload struct{ bytes []byte;
structured Value; which uint8 }` (no codec reference) and `Encoder interface{ Encode(ctx, r *Record, dst
[]byte) ([]byte, error) }` (§13, takes a whole `*Record`).

Two problems compose. First, a `Payload` holds no reference to any `Encoder`, so the only ways to reach the
pipeline's codec are a `context` value (undiscoverable from godoc, allocation on every read, and a landmine
in tests where a bare `context.Background()` is natural) or a package global (which the design forbids
elsewhere). The document picks neither and the struct comment forecloses both.

Second, even given a codec, `Encoder.Encode` takes `*Record`. `Record.Key` is a `*Payload` with no enclosing
record. **There is no interface in the design that can encode a key.** A sink that needs `key.Bytes(ctx)` —
which is every partitioned or upsert sink — cannot get it.

**Fix.** Give `Encoder` a payload-level method (`EncodeValue(ctx, Value, dst []byte)`) and pass the codec into
the `Payload` at construction, or drop the lazy-encode direction entirely and make `Bytes()` return
`(nil, false)` when only the structured view exists, leaving encoding to the explicit encoder stage — which
is where the design says encoding belongs anyway. The current signature promises a conversion the type
cannot perform.

#### B8. There is no way to release source-level resources: `Source` has one method and no `Close`

**At fault:** `type Source interface { Reader(ctx, req OpenRequest) (Reader, error) }` (§4); the 29-member
`CapabilityID` enum, which contains `CapQuiesce` but nothing for close; `Quiescer` documented as "stops
producing and flushes in flight **without closing**" (§4.1).

`SourceFactory.Open` may not do I/O, so a shared client must be created lazily on first `Reader`. Walkthrough
(b) creates **401 readers on one `Source`**, which is exactly the case where you want one shared `*sql.DB` or
one shared HTTP client with a connection pool. Nothing ever tells the `Source` the pipeline is over. The
engine closes each `Reader`; the pool, the background keepalive goroutine and the replication connection all
survive.

**Concrete consequence.** A goroutine and fd leak per pipeline restart in a long-running `canal serve`
process. The author will reach for `runtime.SetFinalizer` or a package-level pool keyed by DSN — both worse
than an interface method. `Sink` has the identical hole (`Sink` is `Writer(...)` only).

**Fix.** `io.Closer` on `Source` and `Sink` as *required* (they are already 1-method interfaces; 2 is still
tiny and the symmetry with `Reader`/`Writer` is what a Go engineer expects), or as capability #30. Required is
better: a lifecycle hook whose absence leaks is not an optional capability.

#### B9. No per-method concurrency contract, and the reader-tier capabilities cannot all be called from the read goroutine

**At fault:** `Reader` — "A `Reader` itself is single-goroutine" (§4) — against `Acknowledger.Ack`,
`LagReporter.Lag`, `Bounded.Remaining`, `Quiescer.Quiesce`, `Positioner.Position` (all §4.1, Reader tier).

`Acknowledger.Ack` is called "after the records are durable at the sink" — that is the commit path, not the
read loop. `LagReporter.Lag` feeds `MSourceLagSeconds`, i.e. a metrics ticker. `Quiescer` is invoked before a
downstream DDL. Each of these will be called while `Read` is blocked on a network read. So either the engine
serialises them behind `Read` (in which case `Lag` cannot be sampled while a source is idle — and idle is
exactly when lag matters — and `Quiesce` cannot interrupt anything, defeating its purpose), or it calls them
concurrently with `Read`, contradicting the stated single-goroutine rule.

**Concrete consequence.** Every connector author guesses. Half will guess "single-goroutine means I need no
mutex" and ship a race between `Ack` mutating a pending-set and `Read` appending to it. `database/sql`
specifies this precisely per type (`driver.Conn` "will not be used concurrently", with `Rows` likewise and an
explicit pinner) and canal's whole premise is `database/sql`'s discipline — this is the one place it does not
copy the thing that makes that package survivable.

**Fix.** Per-method annotation in the doc comment: which methods the engine may call concurrently with
`Read`, and which are serialised with it. `canaltest.AssertReaderContract` should then drive a reader with
concurrent `Ack`/`Lag` calls under `-race`, which is a test the kit can actually write once the rule exists.

#### B10. `Transform.Apply` cannot express "output batch is full, call me again" — expansion silently drops records

**At fault:** `Apply(ctx context.Context, in *RecordBatch, out *RecordBatch) ([]RecordOutcome, error)` (§13)
with `out.Append(r) (ok bool)` and no growth path (§2.4).

A 1→N transform (the design's headline transform capability: "Expanding is appending several via
`Record.Derive`") receives a full 500-record input batch and an output batch whose caps the engine chose.
When `out` fills, `Append` returns false and the signature offers the transform nothing: no continuation, no
"resume from record k", no error that means "give me another output batch". Returning an error fails the
batch. Returning `RecordOutcome`s describes *records*, not remaining work.

**Concrete consequence.** A JSON-array-splitting or row-expanding transform silently drops everything past
the cap. This is the same failure class as the Benthos scars the design cites, arriving through a different
door — and it is data loss in the one stage that has no ack of its own.

**Fix.** `Apply(ctx, in *RecordBatch, emit func(*Record) error) ([]RecordOutcome, error)`, where `emit`
blocks when the downstream is full. That keeps bounded-by-construction (R6), keeps back-pressure transitive,
and removes the drop. It also happens to be the shape `Decoder.Decode(ctx, frame, dst *RecordBatch)` needs
for a 100k-row CSV frame, which has the identical overflow problem.

#### B11. `Write` has two truth channels and the doc contradicts its own walkthrough

**At fault:** `Write(ctx, b *RecordBatch) (*WriteResult, error)` (§5) whose comment says "Returning a nil
error means EVERY record in the batch is DURABLE at the destination", versus walkthrough (c) step 1, which
returns a nil error with `Accepted: 342`, 157 `OutcomeRetriable`, one `OutcomeRejected` — **and
`Durable: true`**.

Also `WriteResult.Durable bool`, documented as "For a `Writer` with no `Flusher` it is always true", i.e. a
field that is a constant function of a capability, placed on the hot path to avoid a capability check on the
hot path.

**Concrete consequence.** The four quadrants of `(result, err)` have two undefined cells: non-nil error with
a non-nil partial result (is the result read? are the 342 acked?), and nil error with failures in
`Outcomes` (the doc says impossible; the walkthrough does it). This is the single most important method in
the sink contract and R4-as-a-type rests on it. A connector author reading the comment will return an error
for a partial failure; a connector author reading the walkthrough will return nil. The engine cannot be
correct for both.

**Fix.** Pick one channel. `error` means "nothing in this batch is accounted for, retry the whole thing";
`*WriteResult` non-nil means "read me, I am authoritative per record". Say that, delete `Durable` (derive it
from `AckPoint`, which admission already reified), and fix the `Write` comment to match.

#### B12. Declared masks must be hand-maintained against the method sets, with a hard refusal on mismatch

**At fault:** `Spec{SourceCaps, ReaderCaps, SinkCaps, WriterCaps CapMask}` (§7) plus DG-3: "`undeclared =
probed \ declared` → `CapSrcUndeclared`, a `PermanentContract` refusal" and "`overdeclared = declared \
probed` → a `PermanentContract` refusal" (§6.3).

Every capability must be written twice: once as a method set on the concrete type, once as a bit in one of
four masks — and the compiler checks neither the bit nor which mask. Adding `LagReporter` to an existing
source and forgetting the bit means the pipeline **stops admitting**, with an error about declaration rather
than about lag.

**Concrete consequence.** This is the largest recurring per-connector tax in the design and it is pure
bookkeeping. For a Debezium-class source it is ~10 bits across 2 masks; for the whole ecosystem it is a
guaranteed recurring support burden, and the failure mode (refusal, not degradation) is maximally
disruptive for a purely clerical mistake.

**Fix.** Derive the declared mask in `Register()` by running the same probe against the factory-constructed
zero instance, or by running the probe against the type at first `Open`. The declaration only genuinely needs
to be authored by hand for the *remote* case, where there is no Go type to probe — which is the case the
`CapSrcDeclaredRemote` enum member already exists for. Keep `canaltest.AssertDeclarations` for the remote
path; delete the hand-authored masks for the in-process path. This removes the tax without weakening DG-3
anywhere it has teeth.

#### B13. `KindStream` makes `Value` non-serialisable, breaking the codec, the buffer, and the DLQ

**At fault:** `KindStream // a nested, lazily-read sub-stream (database/sql's cursor Value, generalised)`
and `Stream struct{ /* lazily-read nested record stream; closed when the parent is closed */ }` (§2.2).

A `Value` may be a live stream with an open cursor and a lifetime tied to a parent. That value can appear
inside `Payload.structured`, therefore inside a `*Record`, therefore inside a `RecordBatch`. But:
`Encoder.Encode` must produce bytes from it; `Buffer.Push` may persist it to disk; `DeadLetter{Record
*Record}` stores it durably; and §2.4 rule 3 says a `Writer` must not retain records after `Write` returns —
while a lazily-read stream by definition is read after the call that produced it.

**Concrete consequence.** Any source using `KindStream` produces records that cannot be encoded, cannot be
buffered, cannot be dead-lettered and cannot survive a fan-out. The failure is at runtime, per codec, and
the codec author has no correct action to take.

**Fix.** Delete `KindStream`, or move it out of `Value` into a facet with an explicit "materialise before any
durable stage" rule enforced by the engine. `database/sql` can afford a cursor-shaped `Value` because rows
are consumed synchronously and never persisted; canal persists everything.

#### B14. "Present but unused" has nowhere to put a sentence, so the design's own honesty invariant has no dual

**At fault:** `CapabilityEntry.Reason` — "REQUIRED when `Present` is false, and MUST be empty when `Present`
is true. A core test walks every entry ... and fails on a violation" (§6.2) — against `PlanStrategy`
selection (§11) and honest weakness 12.

`StrategyChunked` requires `Planner + Chunkable + Comparable + Replayable`. A source implementing
`Chunkable + Comparable + Replayable` but not `Planner` gets `StrategyOpaque` (the strongest supported). The
report then reads `chunk: probed ✓`, `compare: probed ✓`, `replay: probed ✓` — with no reason, because
reasons are forbidden on present entries — while the actual plan is a single unchunked assignment.

**Concrete consequence.** The operator sees three green ticks and an unchunked backfill, and the document
that is supposed to explain *why* is structurally prohibited from saying so. `Plan.StrategyWhy` is one
string on the plan, not a per-capability note, so the connection between "you have chunk" and "it is not
being used" is never rendered. This is DG-2's silent degradation reappearing on the positive side of the
predicate, in the one artefact whose completeness is the proposal's central claim.

**Fix.** `CapabilityEntry` needs three states, not two: `absent(reason)`, `present-and-used`,
`present-and-unused(reason)`. That is a one-field change (`Used bool` plus relaxing the Reason invariant to
"non-empty unless present-and-used") and it makes the report total.

#### B15. `Append(*Record) (ok bool)` puts allocation on the connector and gives recycling nothing to recycle

**At fault:** `RecordBatch{Records []*Record}` with `Append(r *Record) (ok bool)` (§2.4) and the ownership
contract's rule 1 ("A `Reader` appends to the batch it is handed and must not retain any `*Record` it
appended after `Read` returns. (The `io.ReaderAt` 'Implementations must not retain p' rule.)").

The retention rule exists so the engine can recycle. But the engine recycles only the *slice*: the
`*Record`s are allocated by the Reader, one heap object each, and `Reset()` drops them for the GC. A
structured record additionally allocates one interface box per field plus a `Map`. At 100k records/s with 20
columns that is millions of allocations per second, and none of the reader-side restriction buys anything
back.

There is a second, sharper consequence. `Append` returning `ok=false` means the Reader has already
constructed — and usually already consumed from the wire — a record it cannot hand over. It must keep a
pushback slot and check `Full()` before every fetch. **The proposal's own showcase source ignores the return
value entirely** (§14a: `dst.Append(canalkit.BytesRecord(...))`), so the flagship example silently drops a
record whenever the batch is full. If the canonical example gets it wrong, connector authors will.

**Fix.** Invert it, which is both the Go idiom and the fix for §B1: `rec := dst.Next()` returns a
reset, engine-owned `*Record` from the batch's backing array, or nil when full. The Reader fills it in place
(and can set the cursor, since the engine owns the struct), never allocates, and the "must not retain" rule
becomes true by construction rather than by request. This is `sql.Rows.Scan`/`io.ReaderAt` shape;
`Append(x)` is `List.add(x)` shape.

### MINOR (but cheap to fix, and several are compile-time facts)

#### B16. `ErrCapabilityDeclined` is declared twice, and one of the declarations is in an internal package

§6.5 declares `var ErrCapabilityDeclined = errors.New("canal: capability declined for this configuration")`
inside the section whose preceding code block is headed `package capability // internal`. §8 declares the
identical `var` again in the `canal` sentinel block. If both are in `canal`, the package does not compile. If
§6.5's is in `internal/capability`, then **connectors — which are required to return this error — cannot
import it**, because the design's own dependency rule says connectors import `canal` only. Pick `canal`.

#### B17. `*Error` has no `Is` method, so the `NotConnected` reconnect path will not fire

`var ErrNotConnected = &Error{Class: ClassNotConnected}` (§8) with a comment mandating `errors.Is`. But
`errors.Is` on a `*Error` target falls back to pointer equality unless `*Error` implements
`Is(error) bool`, which it does not. The natural connector code —
`return &canal.Error{Class: canal.ClassNotConnected, DevMessage: "dial tcp: …"}` — is **not**
`errors.Is(err, canal.ErrNotConnected)`. Consequence: the engine's reconnect branch never runs for
well-written connectors; the error falls through `ClassOf` to the retry path and the pipeline fails instead
of reconnecting. Fix: `func (e *Error) Is(target error) bool { t, ok := target.(*Error); return ok &&
t.Class == e.Class }`. Two lines, and without them the sentinel is decoration.

#### B18. `type Cursor Blob` silently discards `Blob`'s method set

`Blob` has `IsZero()`. A defined type over a struct type inherits no methods, so `Cursor` has none, and the
"empty means from the beginning" check that appears throughout the design becomes
`c.Version == 0 && len(c.Bytes) == 0` at every site. Either embed (`type Cursor struct{ Blob }`) or redeclare
`IsZero` on `Cursor`. Small, but it is exactly the kind of thing that becomes forty copies of a two-clause
comparison.

#### B19. R9 violated repeatedly by naming: four secrecy vocabularies, `Kind_`, `Disposition2`, `Bounded`×4, `Probe`×3

The design rules exist because "three tiers acquired four string vocabularies". This document reproduces the
pattern inside one Go package:

- **Secrecy, four ways:** `Field.Secret bool` ("core redacts in logs, metrics, status and API responses"),
  `Field.Sensitive bool` ("shown but never persisted in plaintext"), `FTSecret` (a `FieldType`), and
  `Meta.SetSecret`/`IsSecret`. Four spellings of one concept in one package, with `Secret` and `Sensitive`
  adjacent in the same struct and distinguishable only by prose. A connector author will set the wrong one
  and a credential will be persisted or logged. This is the highest-consequence naming defect in the set.
- **`Kind_`** (§6.2, §7) — a trailing underscore is a Go smell that always means an unresolved collision, and
  here it collides with `Kind` (the `Value` kind enum). Both have constants prefixed `Kind`:
  `KindStream`/`KindMap` are `Value` kinds, `KindSource`/`KindSink`/`KindTransform`/`KindCodec` are `Kind_`
  values, and `FieldMapping.AllowedKinds []Kind` means the *value* one. Rename to `ComponentKind` and
  `ValueKind`; nothing else is defensible in godoc.
- **`Disposition2`** (§15, `Buffer.Push`) — a numeric suffix because `Disposition` was taken by the retry
  terminal disposition. Two unrelated concepts, one named with a `2`.
- **`Bounded`** is a source-tier interface (`Remaining`), `Assignment.Bounded bool`, `Stream.Bounded bool`,
  and `Progress.Bounded bool`. `canal.Bounded` (the interface) and `Bounded` (the field) mean different
  things; the field means "terminates", the interface means "can report a denominator".
- **`Probe`** is `Prober.Probe` (liveness), `capability.Probe` (capability collapse), and `CapSrcProbed`. So
  `capability.Probe()` returns a set that may contain `CapProbe`.
- **`Comparable`** shadows the predeclared `comparable` constraint by one capital letter in a package that
  uses generics.
- **`Record.Value Payload`** sits beside `type Value interface` — `r.Value.Structured(ctx)` returns a
  `Value` that is not `r.Value`.
- **`Change.Keys []string` vs `Stream.Keys [][]string`** — one concept ("the fields forming identity"), two
  shapes. `[][]string` is correct (composite/nested); `[]string` on the facet cannot express it.
- **`TolerantesPartial()`** is a typo in an exported method name, and `NewRegistryPtr` (§10) appears once,
  undefined, next to `NewRegistry`.

#### B20. `Registry.With(f any)` and `Register(f any)` discard compile-time checking on the one call every connector makes

`func (r Registry) With(f any) Registry` / `func Register(f any)` (§10). The single line every connector
author writes — `canal.Register(factory{})` — accepts anything and fails at init with a panic if the type
implements none of the factory interfaces. Four typed functions (`RegisterSource(SourceFactory)`, …) give the
compiler the job instead, and remove the need for `Spec.Kind` to restate what the Go type already proves.
As written, `Spec.Kind` and the implemented factory interface are two representations of one fact that can
disagree, and the doc never says which wins.

#### B21. `Config.Component(path) (any, error)` forces type assertions into connector code

§7. Every meta-component (fan-out sink, switch, retry wrapper, DLQ holder — §13 says these are ordinary
registered components) must write `v, err := cfg.Component("sinks"); ss := v.([]canal.Sink)`. So DG-1's "type
assertions exist in exactly one file" is false the moment the composition mechanism is used, and the
assertions are now in *connector* code where nothing checks them. `ComponentRef.Kind` is known statically from
the `Spec`, so `Component[T](cfg, path) (T, error)` was available at no cost.

#### B22. `Framer.Scan` deviates from `bufio.SplitFunc` and allocates per frame; `Compressor` cannot stream

`Scan(ctx, r io.Reader) (frame []byte, ack AckFunc, err error)` (§13) is handed an `io.Reader` on every call
while needing to retain partial-frame state across calls, and returns a fresh `frame []byte` per frame —
allocation per frame on the hottest path in the system, and zero-copy framing is inexpressible. The research
file explicitly recommends matching `bufio.SplitFunc`'s
`func(data []byte, atEOF bool) (advance int, token []byte, err error)`, which solves both. Deviating is
allowed, but the proposal gives no reason and pays real cost. Separately, `Compressor.Compress(ctx, in
[]byte) ([]byte, error)` is whole-buffer, so gzip-over-a-stream — the common case for a file or object-store
sink — cannot be expressed and forces full materialisation of every payload.

#### B23. Two mechanisms for "request a checkpoint boundary", one of which leaks goroutines

`CheckpointRequester.CheckpointRequested() <-chan struct{}` (§5.1, capability `CapRequestCheckpoint`) and
`SignalCheckpointRequest{Reason string}` via `Runtime.Signal` (§9) are the same concept twice — R9, and trap
21 ("modelling one entity twice"). The channel variant is also the harder one: nothing says whether the
method is called once or per loop, who closes the channel, or what happens on `Writer.Close`. With N write
partitions the engine needs `reflect.Select` or one fan-in goroutine per writer — and since nothing mandates
closing the channel, each fan-in goroutine blocks forever when its writer is replaced. Keep
`SignalCheckpointRequest` (it carries a `Reason`, which the channel cannot) and delete the capability.

#### B24. `Runtime.Signal(SignalSchemaChange{})` cannot deliver the in-band ordering it claims

§9's comment: "In-band because it must be ordered with respect to the records around it, and because a schema
epoch committed atomically with a position is the only way a historical event can be decoded with its
historical schema." But `Signal` is a method on `Runtime`, called from the reader goroutine, while records go
into `dst *RecordBatch`. The engine sees a batch of 500 records and, separately, a `Signal` call at some
unobservable point. There is **no positional relation** between the two, so "schema before data" is not
expressible, and the `SchemaEpoch` atomicity story rests on it. The fix is a control record in the batch (or
a sentinel `Value` kind), which is what "in-band" means; a side-channel method call is by definition
out-of-band.

#### B25. The `Read` poll deadline collides with the `(0, nil)` contract

§4: "Returning (0 records, nil) is legal … a `Reader` that has nothing should block on ctx or its own
notification until it does, and the engine bounds the call with a poll deadline." A correctly-written Reader
that blocks therefore returns `context.DeadlineExceeded` on an idle stream. `ClassOf` maps unknown errors to
`ClassTransientInternal`, whose policy is back off and count a retry. So an idle-but-healthy source
accumulates `MRetryAttempts` and eventually trips `Degraded: SustainedBackoff`. Either the doc must mandate
`if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil { return nil }` in every Reader — boilerplate
in the hottest method — or the engine must treat a deadline it imposed as "no data", which it can, and should
say so.

#### B26. Load-bearing types are referenced but never defined

`PlanRequest`, `PlanResult`, `PlanState`, `ChunkRequest`, `SinkPlanRequest`, `SinkPlanResult`, `Tier`,
`DeliveryClass`, `Kind_`, `BatchHint`, `RecordRef`, `AckFunc`, `ComponentHandle`, `Disposition2`, `facetSet`,
`Change.Omitted`, and any accessor at all for `Value`'s `Map` and `Stream` members. Most are mechanical, but
two are not: `PlanResult` is where the low-watermark invariant lives ("admission asserts that a
`StrategyChunked` plan contains exactly one unbounded assignment per stream and that its `Resume` is
non-empty" — §14b), and `Tier` is what §B3 turns on. A `Value` sum type with an unreadable `Map` member is
not yet a type.

---

## C. Boilerplate, measured

The lens asks for concrete line counts for mandatory per-connector boilerplate. Counted as framework-facing
code only — no driver logic, no business logic, `gofmt` lines including braces.

| Connector class | Mandatory framework-facing lines | Notes |
|---|---|---|
| Trivial source, with `canalkit` | **~28** | §14a as written: package+imports 4, `init` 1, factory type 1, `Spec` 12, `Open` 3, `Reader` 7. Genuinely good. |
| Trivial source, without `canalkit` | **~34** | plus a 4-line `Close` and a struct to hang it on. |
| Trivial sink, with `canalkit` | **~26** | `Spec`, `Open`, `Writer`, `return canal.AllWritten(b.Len())`. |
| Median real source (8 config fields; `Positioner`+`Seekable`+`Discoverer`+`Validator`) | **~190–210** | `Spec` ~45; `Open`+`Decode` ~12; `Read` loop with pushback-on-full ~40; cursor encode/decode with versioning ~30; `Discover`→`Catalog` ~35; `Validate`→`[]Diagnostic` ~20; `Position`/`CursorVersion` ~8; declared masks ~4. |
| Debezium-class source (10 optional interfaces) | **~600–750** | adds `Planner`→`PlanResult` ~60; `ChunkIter` (4 methods, own cursor) ~70; `CompareKeys` over 12 `Kind`s ~35 **and every chunkable source rewrites it — `canalkit` offers no `CompareValues`**; `Replanner` ~50; `Acknowledger` ~30; `LagReporter` ~25; `Phased` ~30; `Classifier` ~25; `CapabilityReporter`+notes ~30. |
| Sink at exactly-once tier (`TwoPhaseWriter`) | **~250–300** | `Prepare`/`Commit`/`Abort` with idempotent-per-handle and subsuming-contract tolerance ~120, plus `WriteResult` construction with per-record outcomes ~60. |

**Verdict on boilerplate:** the floor is excellent and the ceiling is honest — a connector that wants ten
capabilities writes ten interfaces, which is the deal. Two specific taxes are avoidable and should be
removed: the hand-authored capability masks (§B12, ~1 line per capability with a hard-refusal failure mode),
and the missing `canalkit.CompareValues` / cursor-`Blob` helpers (`JSONBlob[T]` is listed in the package
layout but never shown, and it is the single highest-leverage helper in the kit — every source needs versioned
cursor marshalling and every one will get the version handling subtly wrong).

The pushback-on-full requirement (§B15) adds ~8 lines and one field to **every** non-trivial reader and is
pure ceremony under the current `Append`.

---

## D. Discoverability from godoc alone

**Mixed, leaning negative.** The required path is excellent: a reader lands on `SourceFactory`, `Source`,
`Reader`, sees five methods, and can write a connector. The `Unlocks`/`Reason` machinery is a genuinely novel
*runtime* discoverability tool and better than any framework surveyed.

But a package with 29 optional interfaces, ~60 supporting structs and 8 closed enums has no godoc-visible
ordering. Nothing in godoc tells you that `Chunkable` is useless without `Comparable` and `Replayable`, or
that `Seekable` without `Positioner` yields `Resumable: false`, or that `Planner` gates `StrategyChunked`. All
of that lives in `admit`'s `Requirement` table — in an `internal/` package the connector author cannot read.
That is backwards: the conjunctions are the connector author's most important information and they are the one
thing hidden from them.

Concrete fix, cheap: put the `Requirement` lattice in `canal` as exported data (it is a table of
`{Requirement, []CapabilityID}` — pure data, no engine dependency) and reference it from each optional
interface's doc comment ("implement together with `Comparable` and `Replayable` to satisfy
`ReqParallelBackfill`"). Then godoc alone answers "what do I implement to get parallel backfill", which today
it cannot.

Secondary: the naming collisions in §B19 do their damage specifically in godoc, where `Kind` and `Kind_`
appear adjacent in the index, `Bounded` appears once as an interface and three times as a field, and `Secret`
and `Sensitive` appear in the same struct three lines apart.

---

## E. What this proposal does better than any plausible alternative

Transplant these regardless of which proposal wins:

1. **`bound` — capability dispatch as a struct of function fields, filled once by a data-driven probe table.**
   This is the load-bearing novelty and it is correct. It converts "type assertion vs. wire" from a dilemma
   into a fill-the-struct problem, costs one indirect call, needs zero forwarders, and is the easiest thing in
   the design to unit-test (no mocks — assign closures).
2. **Frozen required interfaces + growth confined to request/response structs**, stated as a rule with the
   trap it prevents named. `OpenRequest`/`WriteOpen` are the mechanism.
3. **`Reason` on every absent capability, plus `Unlocks`.** Absence as a rendered sentence rather than a
   blank. No surveyed framework has this, and it costs one string per row. (It needs the third state from
   §B14 to be complete.)
4. **Unexported `Origin` + `Derive()`** as the structural, language-enforced answer to provenance mutation.
5. **`Close` is called exactly once, always, with a fresh context carrying the shutdown grace period.**
   Copy the sentence verbatim.
6. **`WriteResult` keyed by `RecordID` with a closed six-member `OutcomeStatus`**, plus `AllWritten` /
   `AllAccepted` / `Mixed` constructors so the common cases are one line. R7 in the type system, with the
   ergonomics handled.
7. **`AckPoint` reified as a value that admission reads and the UI renders**, with the strict reading as the
   default and weakening requiring a visible interface.
8. **The `canal` / `canalkit` / `canaltest` three-package split**, with the contract importing stdlib only.
9. **`RetrySafe` as an explicit `ErrBadConn`-discipline field on the error, asserted by the conformance kit.**
10. **The rule "generics on accessors and concrete helpers, never on a plugin interface or the envelope",**
    with the erasure-at-the-registry-boundary argument written down next to the type it justifies.

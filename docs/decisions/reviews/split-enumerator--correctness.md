# Review: `split-enumerator.md` — lens: **correctness of data-movement semantics**

**Status:** draft, adversarial review. Not normative. One of three lens reviews of
`docs/decisions/proposals/split-enumerator.md`. Judged only on what is guaranteed after a crash at every
point in a record's life, whether a checkpoint can be committed for data not durably in the sink, whether
the claimed delivery tiers are achievable *with the interfaces as written*, and whether the
snapshot→stream handoff can miss or stale-overwrite records.

**Score: 5 / 10.**

---

## 0. Verdict

The proposal has better correctness *instincts* than any of the systems it surveys. It names the right
invariants — completion is data, the handoff starts at `min(HIGH)` gated on durability, abort ≠ discard,
comparability must be portable, a failed commit escalates, drops are always the newest — and several of
them are stated more precisely here than in the shipping prior art.

It nonetheless fails its own lens, and it fails it *at the centre of its argument* rather than at the
edges. Three findings are decisive:

1. **The flagship walkthrough is a data-loss trace.** §19.2 presents mid-chunk resume of a `Scan` split as
   the payoff that repays the whole model's cost. Mid-chunk resume is incompatible with `Completion`
   carrying **one** `Watermark` for the whole `KeyRange`, and the incompatibility silently suppresses real
   changes during the stream phase (§D1 below). Flink CDC does not resume snapshot chunks mid-way, and this
   is why.

2. **Cursor authority is genuinely undefined, and the two candidate answers differ by "loses data" vs
   "correct".** `Reader.Snapshot`'s doc comment says the core "persists the returned splits **verbatim**";
   §13.4 step 8 says "the prefix tracker's resolved cursors are **folded into** the splits". The reader
   cannot know what settled at the sink, so the first reading commits cursors past in-flight data; the
   second reading is silent about the case where the tracker has resolved nothing yet, which is exactly the
   case that loses records (§D2).

3. **The exactly-once tier is not achievable with these interfaces.** The fence protects the checkpoint
   store, not the sink. A partitioned worker keeps `Fetch`ing and `Write`ing for `lease TTL (30s) +
   reassignment delay (120s)` after it has lost ownership, by design, while a second reader is assigned the
   same split. Nothing fences committables (§D4). `min(source, sink, declared)` is validated at submit time
   and then not honoured at runtime.

The rest of the findings are of the same species: the vocabulary to express the right thing exists, and the
*obligation* is missing from the interface, so the guarantee depends on an implementor inferring it. For a
deliverable whose whole point is "the interface set", that is the defect, not an omission to be filled in
later. Nine of the twenty-three findings below are silent-loss or silent-corruption paths that are **not**
connector-author mistakes — they are core-protocol gaps.

Why 5 and not lower: nothing here is unfixable, and the fixes are mostly *additions of stated obligation*
rather than redesigns (the notable exceptions are D1 and D4, which need a mechanism change). Nothing here
is the R4 catastrophe by *design intent* — the intent is right throughout. Why not higher: the proposal
claims correctness is a *consequence* of the split abstraction. It is not. The split abstraction supplies
the right nouns and then leaves the ordering obligations between reader, tracker, plan and store unstated,
and every one of the nine loss paths lives in exactly those unstated orderings.

---

## 1. Fatal defects

### D1. Mid-chunk resume of a `Scan` split invalidates the chunk's single watermark → silent loss during the stream phase

**At fault:** `split.Completion{Keys, Watermark, DurableAt}` (one watermark per whole `KeyRange`) combined
with `Split.Cursor` on a `Scan` split, `SourceCapabilities.MidSplitResume: true`, and the §13.7 step-2
offset-signal sequence.

**The interleaving**, taken verbatim from the proposal's own §19.2:

- Chunk `orders/scan/7` covers `[437_500, 500_000)`.
- Attempt 1: reader records `LOW₁`, begins reading, **emits rows in batches of 2,000 each carrying a
  `Cursor`** (§19.2 Phase 2: "The reader emits the 62,500 rows in batches of 2,000, each carrying a
  `Cursor` (the last key read)"). 24,530 rows are emitted and durably written (Phase 3).
- `kill -9`. Checkpoint 88 holds `Cursor = key 471_030`.
- Attempt 2 (Phase 5): `ReadRange` is called with that cursor, so the query is
  `WHERE key > 471030 AND key < 500000`. "**37,970 rows remain and 24,530 are not re-read.**"
- The reader now records `LOW₂` (necessarily `> HIGH` of nothing — there was no `HIGH₁`), reads the
  remainder, records `HIGH₂`, replays `[LOW₂, HIGH₂)`, upserts into the *remaining* buffer, emits, and
  reports `Watermark = HIGH₂` for the split.
- `Completion{Keys: [437_500, 500_000), Watermark: HIGH₂}`.
- Stream phase filter (§13.7 step 5): emit iff `Change.Key ∈ range && Origin.Pos > HIGH(chunk)`. Every
  change to a key in `[437_500, 471_030]` at a position `≤ HIGH₂` is **suppressed**.

**Consequence.** Rows `[437_500, 471_030]` were read at `LOW₁`-consistency and never patched: the
per-chunk backfill replay `[LOW₂, HIGH₂)` was applied to a buffer that no longer contained them, and the
window `(LOW₁, LOW₂)` was never replayed at all. Every update and delete to those keys in
`(LOW₁, HIGH₂]` is now permanently invisible — absent from the snapshot output *and* suppressed by the
watermark filter. This is silent, unbounded, per-restart data loss on the design's headline capability.
Worse, the wider the crash gap the larger the loss window, so the failure scales with the exact condition
(a long snapshot, a flappy worker) the feature exists to serve.

Note also what the core does with a replay record whose key is outside the remaining buffer: §13.7 step 2
says the core "upserts them into `buf`". Nothing says whether an unmatched replay record is emitted or
dropped. Dropped ⇒ additional loss. Emitted ⇒ a partial repair of `[LOW₂, HIGH₂)` only, leaving
`(LOW₁, LOW₂)` uncovered. Either way the invariant is broken.

**Why this is structural, not an implementation slip.** The chunked-snapshot algorithm's correctness
condition is *"every row in this key range is consistent as of one position, and that position is the
watermark"*. A resumable chunk has **two or more** consistency positions over disjoint sub-ranges of one
`KeyRange`, and `Completion` can represent only one. The proposal's own submit-time check table has no
`Chunkable ⇒ ¬MidSplitResume` rule, and §19.2 declares `MidSplitResume: true` on the chunkable source.

**Fix direction.** Either (a) `Scan` splits under the chunked engine are **not** resumable — a crash
re-reads the chunk, which is why Flink CDC does it that way, and the proposal's advertised advantage over
Benthos then reduces to *chunk*-granularity resume rather than row-granularity (still a genuine win: 320
chunks means you lose ≤1/320 of the work, not all of it); or (b) resuming a chunk **splits** it: the
completed prefix becomes its own `Completion` with `Watermark = HIGH₁`, which requires the reader to
establish `HIGH₁` *before* the crash, i.e. requires the reader to emit no rows until it has a `HIGH` — which
contradicts row-granularity resume anyway. (a) is the honest answer and it should be a
`registry.RegisterSource` panic, not a doc comment.

---

### D2. Cursor authority is undefined; on the reading the interface actually states, the checkpoint commits cursors for data that is not in the sink

**At fault:** `connector.Reader.Snapshot(ctx, id) ([]split.Split, error)` versus §13.4 steps 6 and 8, and
`prefix.Tracker.Resolve() (T, bool)`.

The interface says two mutually exclusive things:

> `Reader.Snapshot` … "THIS IS THE READER'S ENTIRE DURABLE STATE… The core persists the returned splits
> **verbatim** and, on restore, hands them straight back through `Assign`." (§7.3)

> §13.4: "6. `Reader.Snapshot(ctx, id)` on every reader → `[]Split` (with cursors) … 8. **the prefix
> tracker's resolved cursors are folded into the splits**"

A reader has no visibility of the sink. Its notion of "how far I got" necessarily includes (i) records it
handed to the core that have not been written, (ii) records it fetched from upstream and holds in its own
internal buffers, which §12.1 explicitly permits ("A Reader that wants concurrency inside itself — a
fetcher goroutine per split… owns that entirely"), and (iii) under the checkpoint barrier of §13.4 step 1,
records it fetched during the pause. So the reader's cursor is always ≥ the safe cursor, and often strictly
greater.

**The interleaving that loses records even under the charitable reading (step 8 wins):**

- Split `orders/change/0` is assigned at checkpoint 50 with `Cursor` absent.
- The reader emits 5,000 records across 10 `SplitBatch`es, each carrying a `Cursor`.
- The sink is slow (a long `Write` on a large batch, or a 429 backoff). **Nothing has settled.**
  `Tracker.Resolve()` returns `(_, false)` — "did not advance".
- Checkpoint 51 fires (§13.5 point 4: "a maximum interval … so a silent one still refreshes
  `CommittedAt`", so a checkpoint *will* fire).
- Step 6 collects `Split{Cursor: Some(position of record 5000)}` from the reader.
- Step 8 has no resolved cursor to fold. `Resolve` returned false. There is nothing to overwrite with.
- Step 9 commits checkpoint 51 **verbatim**, per `Reader.Snapshot`'s own contract.
- `kill -9`. Restore hands the split back with `Cursor = record 5000`. `poller.Poll(ctx, Some(5000))` /
  `ReadRange(split with cursor 5000)`.

**Consequence: 5,000 records silently lost, with a healthy-looking checkpoint.** This is R4 violated by
the *core*, not by a connector. The proposal's central R4 claim — "violating R4 requires a sink to lie
about a return value" (§21.1) — is false: it requires a reader to answer the question it was asked.

The proposal's own walkthrough gets this wrong in the safe direction by accident. §19.3.1 T4:

> "`Reader.Snapshot` returns the split with its cursor — which is still the **pre-12,001 cursor**, because
> `Resolve` never advanced."

The reader cannot see `Resolve`. It has read to 12,500. This sentence attributes core knowledge to the
connector, and it is the sentence the whole at-least-once claim rests on.

**Secondary consequence.** If step 8 *does* clobber `Split.Cursor`, then `Split.Cursor` — "the reader's
entire durable state" — is not the reader's to write, and a reader with legitimate non-position resume
state (a page token consumed but not emitted, an open cursor handle description, a partially-decoded
frame) has nowhere durable to put it. There is no `SnapshotReader` analogous to
`SupportsWriterState.SnapshotWriter`; the asymmetry is presented as the FLIP-27 payoff ("the reader side of
resumption needs no API at all", §7.1) and it is exactly one field short.

**Fix direction.** Delete `Cursor` from `Reader.Snapshot`'s return, or rename the method to make clear it
reports *read* progress that the core will overwrite. State the invariant on the type: `Split.Cursor` as
persisted is `Tracker.Resolve()`'s output for that split, and when `Resolve` has never advanced for a split
the persisted cursor is the one from the previous checkpoint (or absent). Add the reader-state field if a
reader may need one, and make `SplitBatch.Cursor` the *only* channel by which a reader proposes progress —
which the design already almost does, and then undoes in `Snapshot`.

---

### D3. `PlanView.AllCompletionsDurable` is vacuously true at zero completions and is named as *the* handoff gate

**At fault:** `PlanView.AllCompletionsDurable bool`, documented as:

> "true iff every completion the core knows about has `DurableAt <=` the last durable checkpoint. **The
> enumerator MUST gate the snapshot→stream handoff on this**, and the core makes the check trivial so
> nobody reimplements it wrongly." (§7.2)

**The interleaving.** Cold start of a `ModeScanThenTail` pipeline. `Enumerator.Poll` is called before any
splits exist. `Completions` is empty. `∀ c ∈ ∅ : c.DurableAt ≤ 88` is **true**. An enumerator author
following the stated MUST emits the `Change` split on the first `Poll`, with
`From = min(HIGH over ∅)` — an undefined minimum, most naturally the source's current position.

**Consequence.** The pipeline streams and never snapshots, or snapshots concurrently with a stream that
started at "now", so every row that is not subsequently updated is missing from the destination forever. No
error, no condition, no metric distinguishes it — `PhaseOf` reports `PhaseStreaming` and everything is
green. Universal-quantifier-over-empty-set is the oldest bug in this family and the interface hands it to
the connector author as the *sanctioned* gate.

**The completeness half is also weaker than the available data.** §6.3 offers
`ExpectedCompletions opt.Opt[int]` so "a restored plan can assert `len(Completions) == ExpectedCompletions`
before handing off". Two problems:

- `ExpectedCompletions` is **not a field of `PlanView`**. The enumerator set it via `Enumeration.Expect`,
  so it must carry it in its own blob to check it — and after `RestoreEnumerator` it may or may not have.
  The core computes the *insufficient* half of the gate and withholds the sufficient half.
- `len(Completions) == Expected` is a **count**, where the invariant the handoff actually needs is
  *coverage*: the union of completed `KeyRange`s equals the key space. Given `ord.Order`, coverage is
  computable in core in a dozen lines, and it is immune to the double-completion problem in D8 and to
  `Reconcile`'s `ExpectedCompletions` adjustment races.

**Fix direction.** Replace `AllCompletionsDurable` with a single core-computed
`HandoffReady bool` (or better, have the *core* emit the handoff split and not the enumerator at all, since
every input is core-owned) whose definition is: `NoMoreScanSplitsPlanned ∧ scan splits outstanding = 0 ∧
completions cover the key space ∧ every completion durable`. If the gate is to remain the enumerator's, at
minimum make the empty case false and expose the denominator.

---

### D4. Nothing fences sink writes or committables; the design's own timers give a 150-second double-writer window, so exactly-once is unattainable

**At fault:** `store.Fence`, checked only by `CheckpointStore.Commit`; `store.Coordinator.Renew`;
`SupportsCommitter.PrepareCommit/Commit`; the §16.4 constants (lease TTL 30s, reassignment delayed 120s).

**The interleaving.** Worker `w-3` holds `orders/change/0` and is network-partitioned from Postgres but
*not* from the source or the sink — the common cloud failure, and the one §16.3 explicitly designs for:

> "**The data plane keeps running and keeps checkpointing with the entire control plane down**, because a
> worker holding a valid lease needs nothing from anyone until it expires." (§16.3)
> "with Postgres unreachable a worker: keeps `Fetch`ing its assigned splits, keeps writing to its sink,
> keeps resolving the prefix" (§16.3)

- `t=0` partition begins. `w-3` keeps `Fetch`ing and `Write`ing. This is stated as the design's "single most
  important deployment property".
- `t=30s` lease TTL elapses. The coordinator marks the assignment orphaned.
- `t=150s` reassignment delay elapses. The split is re-placed on `w-5`, which resumes from the last durable
  checkpoint's cursor.
- `t=150s…` **two readers are reading the same split and two writers are writing its records to the same
  destination**, and `w-3` has been writing un-checkpointed records for 150 seconds already.

`CheckpointStore.Commit` will reject `w-3`'s eventual write with `ErrFenced` — that protects the *state*.
Nothing protects the *destination*. The fence is not passed to `Writer.Open`, is not on `Opening`, is not
on `Write`, and is not derivable from `Committable` (whose `ID` is "core-assigned and stable across
restarts", i.e. two incarnations mint the same ids for different artifacts, or different ids for the same
data — neither is stated).

**Consequence.** Under `TierAtLeastOnce` this is duplicates plus reordering (see D16 for why reordering is
worse than duplicates here). Under `TierExactlyOnce2PC` it is a **flat contradiction of the tier**: `w-3`
and `w-5` both `PrepareCommit` artifacts covering overlapping record ranges, both sets get committed by
their respective checkpointers, and no mechanism can detect it. §19.5's own fencing paragraph describes
`w-3` being caught at `CheckpointStore.Commit` — after up to 150s of writes it already made.

The proposal quotes the k8s leaderelection caveat correctly and draws the right conclusion for *state*
("leadership is never trusted for correctness"). It then never applies the same reasoning to the sink,
which is where the data goes. Every real exactly-once system solves this at the destination: Kafka's
producer epoch fencing, Flink's transactional id derived from `(subtask, attempt)`, Iceberg's
optimistic-concurrency manifest commit. None of those is expressible here.

**Fix direction.** Put the fence on `Opening` and require it inside every committable's identity, so a
zombie's transaction id / staged prefix / manifest generation is distinguishable and the destination can
reject or supersede it. Add a `SupportsFencedWrite` capability and make `TierExactlyOnce2PC` require it —
the submit-time refusal table is the right place, and its absence there is the same class of omission as
the `Chunkable ⇒ upsert` rule it *does* have. Separately: require self-fencing on the worker (stop reading
at `TTL − ε` after a failed renewal, not on learning of failure) and state that the reassignment delay must
exceed the worker's self-fence deadline, which is the invariant the 30s/120s pair is presumably chosen to
satisfy but which is nowhere stated.

---

### D5. Revoke and rebalance move a split at the *reader's* cursor while the core's tracker still holds that split's unsettled sequences — the split either wedges forever or loses the records

**At fault:** `Reader.Revoke(ctx, ids) ([]split.Split, error)` — "with `Cursor` set to the last position at
which a resume would be correct" — plus `prefix.Tracker` being per split and `Origin.Seq` being "monotonic
within `Split` across the whole life of the pipeline generation".

**The interleaving.** Routine graceful drain, not a crash:

- `orders/change/0` on reader A. Seqs 900…1,000 emitted; 950…1,000 unsettled at the sink.
- Operator scales down / the placer rebalances. `Revoke([orders/change/0])`.
- Reader A returns the split with the cursor at position of seq 1,000 (its last correct *read* resume
  point; it does not know 950…1,000 are unsettled — same root cause as D2).
- The split goes to `Unassigned`, is placed on reader B, which resumes at seq-1,000's position.
- Reader B produces new records. The core continues `Seq` for that split: 1,001, 1,002, …

Now the tracker for `orders/change/0` contains unsettled entries 950…1,000 (reader A's, whose writes may or
may not eventually land — reader A's `Write` calls were already handed to the sink) and settled entries
1,001+. `Resolve()` returns "the highest cursor whose entire prefix has settled": the prefix stops at 950.

**Consequence, both branches bad and the interface picks neither:**

- If the core keeps A's entries: the split's cursor can never advance past 950 again unless A's writes
  settle — and A has been closed. `Tracker.Stuck()` reports seq 950 outstanding forever, the split is
  permanently un-checkpointable, and since the tracker's capacity *is* the in-flight bound and `Track`
  blocks when full, the split **wedges after `Limit` more records**. Every rebalance with anything in
  flight permanently poisons a split.
- If the core discards A's entries: seqs 950…1,000 vanish from accounting, `Resolve` advances to reader
  B's position, and the checkpoint commits past records that were never durable. **Loss on a routine
  rebalance.**

`Revoke` is presented as the mechanism that makes drain "graceful" (§7.3, §22.8 defends its existence on
correctness grounds). As specified it is the mechanism that makes drain lossy.

**Fix direction.** Revocation must be a three-step protocol, not a call: (1) stop admitting from that
split, (2) wait for `Resolve` to quiesce (or checkpoint), (3) then hand ownership over at the *resolved*
cursor. State it on the interface. Additionally state that `Origin.Seq` restarts, or is namespaced by
assignment epoch, when ownership changes — otherwise one tracker is fed by two producers with a gap in the
middle, which is the state above.

---

### D6. A `Completion` is reported when *one batch* settles, not when the split's prefix resolves, so the handoff can start past unwritten earlier batches

**At fault:** `SplitBatch.Done`/`Watermark`:

> "The core does not report the completion to the enumerator until **every record in this batch** has
> settled downstream. A completion reported early would let the change read start after a watermark whose
> scan rows were never written." (§7.3)

The doc correctly identifies the hazard and then bounds the check to the wrong set. A `Scan` split emits
many batches (§19.2: 62,500 rows in batches of 2,000, so 32 batches). Concurrency across batches is by
design: `SinkCapabilities.MaxInFlight`, per-record retry (§19.3.1), and `SupportsBatchSplit` all permit
batch *n* to settle before batch *m < n*.

**The interleaving.**

- Batches 1…31 of `orders/scan/7` are written; batch 5 hits a transient 429 and enters retry.
- Batch 32 carries `Done: true, Watermark: HIGH`. It is written successfully. **Every record in this batch
  has settled.**
- The core reports `Finish{Split: orders/scan/7, Watermark: HIGH}` → `Completion` appended.
- The next checkpoint sets `DurableAt`. `AllCompletionsDurable` becomes true. The last chunk finishes.
- Enumerator emits the `Change` split at `min(HIGH)`.
- Batch 5's retries exhaust and it is dead-lettered, or the process dies and batch 5's records are
  re-read — except the *split* is now marked complete and `Completions` is what the plan believes.

**Consequence.** 2,000 scan rows are absent from the destination while the stream phase's filter suppresses
every change to them at positions `≤ HIGH`. Silent loss, and it is the loss the doc comment set out to
prevent — the gate is simply on the wrong quantity. Note also that this produces a *self-contradictory
checkpoint*: `Checkpoint.Workers[w].Splits[7]` carries a cursor short of the range end (because `Resolve`
never advanced past batch 5) while `Checkpoint.Plan.Completions` says the split is complete. Nothing
reconciles the two, and `Split.Finished()` reads watermark presence, so both are "true".

**Fix direction.** The gate is `Tracker.Resolve()` for that split having reached the split's end — which is
the quantity the core already computes. State it as: a `Finish` is admitted to `Completions` only when the
split's resolved prefix covers every sequence the split produced *and* the split produced no `Fail`ed
sequence. Also: forbid `Split.Watermark` from being persisted in `WorkerState` before that point, so the
two representations of completion cannot disagree (see D19 on why they are two representations at all).

---

## 2. Major defects

### D7. `Split.Cursor` is `blob.VersionedBlob`, so four documented core behaviours are not expressible — including the entire ordered-split dedupe mechanism

**At fault:** `Split.Cursor opt.Opt[blob.VersionedBlob]`, `SplitBatch.Cursor opt.Opt[blob.VersionedBlob]`
versus `Origin.Pos opt.Opt[ord.Position]`, `Split.Watermark opt.Opt[ord.Position]`.

A `VersionedBlob` is `{Version int, Bytes []byte}`. It carries **no `Order` and no `Scalar`**. The core
therefore cannot compare a cursor with anything, cannot compare two cursors, and cannot project a cursor
onto a number. The following are all stated as core behaviour and none is type-expressible:

| Stated behaviour | Where | Why impossible |
|---|---|---|
| ordered-split dedupe: "`record.Origin.Pos ≤ committed cursor` ⇒ already durable. A position comparison." | §13.8 | comparing `ord.Position` to `blob.VersionedBlob` |
| `Split.Finished()` for a bounded `Change` split = "the cursor having reached `To`" | §6.2 | comparing a blob to `ord.Position` |
| `canal_position_fraction{split}` = `ord.Fraction(from, cursor, to)` | §17.2 | `Fraction` takes three `Ordinal`s |
| checkpoint-edit UI shows "their cursors' `Scalar` projections" | §13.3 | blobs have no `Scalar` |
| `PlanSummary.Fraction` = "each in-progress split's own position fraction" | §17.4 | same |
| `Resolve()` returning "the **highest** cursor whose entire prefix has settled" | §13.6 | "highest" needs an order over cursors |

**Consequence on this lens.** The first row is the important one: §13.8's headline claim is that ordered
splits need **zero** dedupe storage because "duplicate" reduces to a position comparison against the
committed cursor, and §21.1's R5 row says "For ordered splits 'duplicate' *literally* means 'at or before
the committed cursor'". That mechanism cannot be implemented. Combined with D20 (`(Split, Seq)` is not
durable across generations) and the optionality of `Origin.Upstream` and `Change.Key`, canal ends up with
**no mandatory dedupe key at all**, so the honest delivery tier for the default configuration is
at-least-once with *undetectable* duplicates rather than at-least-once with *reported* duplicates. R5's
"duplicate must mean already durably stored" is satisfied in prose only.

**Aggravating:** §18.2's `RegisterPollSource` lifts the poll cursor to `ord.Position{Bytes: s, Order:
[]byte(s)}`, and `PollSource.Poll(ctx, from opt.Opt[ord.Position])` takes a `Position`. So the same concept
has two representations with a conversion function between them — the exact test §21.1's R9 row states it
applies ("a function mapping between two representations of the same concept is evidence of a modelling
error").

**Fix direction.** `Cursor` is an `ord.Position`. The `Ordinal` type already has the optionality built in:
a connector that cannot supply `Order` supplies `Bytes` only and the core degrades, which is the pattern
§4.3 was written for. This is the single cheapest high-value change in the review.

---

### D8. A split can be placed on two readers concurrently, because it stays in `Unassigned` until a reader's next `Snapshot`

**At fault:** `PlanState.Unassigned`:

> "a split stays here until a reader has ACKNOWLEDGED holding it (which the core learns from the reader's
> next `Snapshot`). Splits handed to the placer but not yet acknowledged remain `Unassigned`, so a lost
> assignment message loses nothing and a duplicated one is absorbed by `Reader.Assign` being idempotent on
> `SplitID`. This is deliberately simpler than Flink's `SplitAssignmentTracker`…" (§6.3)

`Reader.Assign` is idempotent **within one reader**. Across readers it is not, and nothing else prevents
double placement: the placer works from `PlanView.UnassignedIDs`, which by construction still contains the
split for a whole checkpoint interval after it was handed out.

**The interleaving.**

- `t=0`: split `orders/scan/7` placed on reader A. Still in `Unassigned`.
- `t=0.2s`: reader B calls `RequestSplits(4)`; `Demand` rises. The placer sees `orders/scan/7` in
  `UnassignedIDs` and places it on B.
- Both read the chunk. Both eventually `Report{Finished: [{Split: orders/scan/7, Watermark: …}]}` with
  **different** watermarks.
- Both return the split in `Snapshot`, so `Checkpoint.Workers` has it under two `WorkerState`s with two
  cursors. Nothing states which wins on restore.
- `Completions` receives two entries for one `SplitID` (`Completions` is a `[]Completion`, not a map, and
  nothing dedupes it).

**Consequence.** `len(Completions) == ExpectedCompletions` — the completeness predicate §6.3 offers — is
satisfiable with 320 entries covering only 319 distinct chunks. **The handoff fires with one chunk never
scanned**, and the filter then suppresses changes to its key range using whichever duplicate watermark
sorted in. Silent loss of an entire chunk. This is precisely why D3's coverage-not-count recommendation
matters: coverage is immune to this, a count is not.

The rejection of Flink's `SplitAssignmentTracker` as "deliberately simpler" is the defect. Flink's tracker
exists to make assignment *at-most-once* per checkpoint epoch; treating assignment as lossy is fine, but
you then need idempotence at the *plan* level, not just at the reader.

**Fix direction.** A third state: `Pending{split, worker, epoch, placedAt}`, excluded from placement,
reverting to `Unassigned` on timeout. That is the tracker, and it is ~20 lines. Or make `Completions` a
`map[SplitID]Completion` and gate the handoff on range coverage, which fixes the fatal consequence even if
double reads remain.

---

### D9. The chunked-snapshot filter fails **open** on three unvalidated preconditions, and the failure mode is stale overwrite, not duplication

**At fault:** `engine.ChunkFilter.shouldEmit` as printed in §19.2 Phase 7:

```go
k, ok := r.Change.Get().Key.Get(); if !ok { return true }     // no key
pos, ok := r.Origin().Pos.Get();    if !ok { return true }     // no per-record position
c, found := f.findByKeyBinary(...); if !found { return true }  // no covering chunk
cmp, ok := pos.Compare(c.Watermark.Ordinal)
return !ok || cmp > 0                                          // not comparable
```

Four `return true` branches. Emitting when you cannot decide is the wrong default for a *suppression*
filter, and the preconditions are not checked anywhere:

- `Origin.Pos` is `opt.Opt[...]` and documented as "the split position AT this record, **when the source
  named one**". No capability declares that a source always populates it. `ComparablePositions` declares
  only that positions *carry `Order`*, not that records carry positions. The §10.3 submit-time table has no
  row for it.
- `Change.Key` presence is likewise undeclared. `SourceCapabilities.ChangeFacet`/`Ops` say the facet
  exists and which ops; nothing says `Key` is always present.
- Cross-version comparability: see D17.

**Consequence, and it is worse than the "duplicates are the price" framing the proposal uses elsewhere.**
A change at position 200 to a key whose chunk has `HIGH = 500` is *not* a duplicate of anything: the
chunk's snapshot row already reflects state as of ≥ 500. Re-emitting it and upserting it **overwrites
fresher state with older state**. The destination ends up with a value that was never correct at any point
after the snapshot. The proposal has no version/sequence field the sink can order on: `Change` carries
`CommitTime` (optional, source's claim, clamp-able) and `TxID` (optional, a grouping string), and
§15.4 concedes the gap — "sinks configured for `upsert` against a source without a monotonic version field
will produce wrong results" — while placing that warning on `transforms.concurrency` only.

So: a source that satisfies every submit-time check for the chunked engine but omits per-record `Origin.Pos`
produces a destination that is **wrong**, not merely duplicated, and no metric distinguishes it.

**Fix direction.** Make the filter fail **closed** with an escalation (`ClassPermanentContract` naming the
missing field) rather than emitting. Add `PerRecordPositions bool` and `AlwaysKeyed bool` to
`SourceCapabilities`, cross-checked at registration and required by `Chunkable` exactly as
`ComparableKeys` already is. Separately: `Change` needs a monotonic `Version ord.Position` (or the core must
expose `Origin.Seq`/`Origin.Pos` as the documented upsert-ordering key and require `DestUpsert` sinks to
honour it) — without one, `DestUpsert` is not a safe destination mode for *any* pipeline that retries.

---

### D10. Is the acknowledgement `Write` returning nil, or `Flush` returning nil? Both are asserted, no capability distinguishes them, and the shipped example sink loses data under one reading

**At fault:** `Writer.Write`'s doc versus `Writer.Flush`'s doc versus §18.4's `fileWriter`.

> `Write`: "`(res, nil)` with `res.Failed` empty → every record in `b` is **DURABLE**. This return value is
> the acknowledgement and the core will advance the checkpoint past these records."

> `Flush`: "makes previously written data durable when the writer buffers internally… A writer that makes
> each `Write` durable implements `Flush` as `return nil`."

> §18.4: "returning nil from `Write` is the acknowledgement, so `Write` must not claim durability that
> `Flush` provides. This writer buffers, so it flushes AND fsyncs here, and the core does not advance the
> checkpoint until `Flush` returns."

The last sentence contradicts itself twice. `fileWriter.Write` returns `(res, nil)` with `Failed` empty
after writing into a `bufio.Writer`. Under `Write`'s stated contract that is a claim of durability for
bytes sitting in a userspace buffer, and `prefix.Tracker.Settle` is driven by it (§13.6, §19.1 T7: "`err ==
nil` and `Failed` empty ⇒ every record durable ⇒ `Tracker.Settle(1)`"). A checkpoint fires (§13.4) — but
step 3 is `Writer.Flush` *before* step 9 `Commit`, so in the *checkpoint* path the fsync does precede the
commit and the example is saved.

It is saved only in that path. `Tracker.Resolve()` advancing is also what `Buffer.Trim` and — under a
`Durable()` buffer — the source-side ack boundary consume, and §13.5's boundaries include "the sink
requests one". More importantly the interface offers **no way for the core to know** which kind of sink it
has: `SinkCapabilities` has `Idempotent`, `PerRecordFailures`, `MaxInFlight` and the four capability flags,
and no `WriteIsDurable` / `RequiresFlush`. So the core must either trust `Write` (and lose data for every
buffering sink whose author read `Flush`'s doc) or ignore `Write` and settle only at `Flush` (making
per-record `Settle` and the whole out-of-order-settlement story dead code, and collapsing granularity back
to the checkpoint interval — trap 3 by another route).

**Consequence.** The one thing R4 exists to prevent — a documented advance rule unrelated to the actual
durability of the thing being acknowledged — is present in the two doc comments that define the
acknowledgement. And it is present in the *example connector the proposal ships as proof*, which §18.4
claims "**cannot** commit a checkpoint against unsynced bytes". It can; whether it does depends on which
sentence the engine author believed.

**Fix direction.** One sentence, one flag. Either `Write` returning nil means durable (and `Flush` exists
only for sinks that batch *inside* `Write`, which is incoherent) or the boundary is `Flush` and the tracker
settles in `Flush`-sized groups (and then `WriteResult.Failed` must be reported at `Flush`, not `Write` —
which is a real interface change and should be made now). Add the capability either way so the core does not
guess.

---

### D11. A `Durable()` buffer takes the ack boundary at `Push`, but `Pop` is destructive and `Buffer` has no replay cursor — acknowledged records are unrecoverable after a crash

**At fault:** `Buffer` (§12.3): `Push(ctx, b) (PushOutcome, error)`, `Pop(ctx) (record.Batch, error)`,
`Trim(ctx, id) error`, `Durable() bool`.

> "If `Durable()` is true the buffer takes ownership of the records' prefix accounting on `Push`, and the
> checkpoint advances on buffer write." (§12.3)

**The interleaving.**

- Durable disk buffer. Reader emits 10,000 records; `Push` returns `PushAccepted`; the source cursor is
  committed at checkpoint 40 because the buffer owns the prefix.
- The engine `Pop`s 5 batches and hands them to the sink. The sink is slow; they are unsettled.
- `kill -9`.
- Restore. The source resumes from checkpoint 40's cursor, which is past all 10,000. The buffer's remaining
  content is `Pop`able. **The 5 popped batches are gone** — `Pop` is destructive, there is no
  `PopFrom(sequence)`, no `Ack(sequence)` and no cursor, so there is no way to re-read them.

**Consequence.** Records acknowledged to the source and lost — R4's abandoned-attempt catastrophe exactly,
now sanctioned by a `Durable() bool` that the core consults to *move the ack boundary earlier*. §12.3 even
cites the abandoned attempt's `202`-after-append and claims `Durable()` makes the two "the same
expression". It does the opposite: it makes the boundary earlier without making the buffer replayable.

**And it is R6's specific observed defect, moved inside the type.** R6 records "two mutually incompatible
buffer abstractions… a non-destructive cursor buffer and a destructive queue". `Pop` is the destructive
queue; `Trim(ctx, id CheckpointID)` — "discards everything durably handled up to a checkpoint" — is
meaningful only for the non-destructive cursor buffer. One interface with both operations is one interface
implementing two models, and the design rule is satisfied by arithmetic (one interface) rather than by
semantics.

**Fix direction.** Pick the cursor model: `Read(ctx, from Sequence) (Batch, Sequence, error)` +
`Trim(ctx, id)`, with `Push` returning the assigned sequence range. Then `Durable()` is sound, `Trim` means
something, and the buffer is a real durability boundary rather than an earlier one. Or drop `Durable()`
entirely for v1 and keep the boundary at the sink, which is the honest simplification.

---

### D12. `DispositionDeadLetter`'s stated core behaviour is unimplementable for 2PC, and the promised dead-letter record cannot contain the records

**At fault:** `Disposition`:

> "`DispositionDeadLetter`: permanent. The core writes a dead-letter record per covered split with full
> provenance, sets `Degraded` with the reason, and **does NOT advance the prefix past the covered
> records**." (§8.3)

In 2PC the ordering is fixed by §13.4: `Write` returns nil (staged) → `Tracker.Settle` → `PrepareCommit(id)`
→ checkpoint durable → `Commit(requests)`. So by the time `Commit` can return `DeadLetter`, the cursor
covering those records **is already durably committed** in the checkpoint that carried the committable.
There is no operation that un-advances it: `CheckpointStore` has `LoadAt` and the API has
`PATCH /checkpoint`, both operator-driven.

**Consequences, compounding:**

- "Does not advance the prefix past the covered records" is false at the moment it is needed.
- `Committable.Splits []SplitID` is the only back-link. There is no sequence range, so the core cannot
  identify *which* records a committable covered. "A dead-letter record **per covered split**" is therefore
  all it can write — a note saying "some records of `orders/change/0` were lost", with
  `DeadLetter.Payload` ("the record as canal last saw it") unfillable because the core discarded those
  records when they settled.
- §19.3.1's flagship claim — "There is no configuration under which a record vanishes: it lands in the
  destination, or in the DLQ, or the pipeline stops" — is false for tier 3. It stops, *with progress past
  the missing data*, and recovery is manual checkpoint surgery.

The same argument applies to `Committable.Expires` ("FAILS LOUDLY when it passes"): loud failure after the
cursor moved is a wedge requiring operator rewind, not a recoverable state.

**Fix direction.** `Committable` must carry the sequence range per split that it covers, so the core can
(a) refuse to advance the prefix past a committable until it is `Committed`/`AlreadyCommitted` — i.e. the
2PC settle point is `Commit`, not `Write` — and (b) reconstruct or at least name the affected records.
Making the 2PC prefix advance at `Commit` rather than `Write` is the correct fix and it is a real change:
the pending set then bounds in-flight, which is what Flink's `map[ckptID][]Committable` plus its in-flight
bound does.

---

### D13. `PrepareCommit` is never required to re-report committables from an aborted checkpoint, so an abort silently orphans staged data

**At fault:** `SupportsCommitter.PrepareCommit(ctx, id) ([]Committable, error)` — "mints the committables
for checkpoint `id`" — versus §13.4's abort semantics and §19.5's "A reader that misses the window
**aborts** the checkpoint".

**The interleaving.** Checkpoint 77: `Flush`, `PrepareCommit(77)` → `[c1..c4]`, `Committables[77] =
[c1..c4]` (in memory), then a reader misses the `Snapshot` window → abort → `CheckpointStore.Commit` never
runs. The pipeline continues. Checkpoint 78 fires: `PrepareCommit(78)`.

If the writer returns only artifacts staged since 77 — which is what "mints the committables for checkpoint
`id`" says — then `c1..c4` are staged in the destination, referenced by nothing durable, and never
committed. Meanwhile the *prefix advanced* at their `Write` (D12), so checkpoint 78 commits a cursor past
their records.

**Consequence.** Silent loss on every aborted checkpoint for a 2PC sink, with no crash involved. §19.5
makes aborts a *normal* event ("A reader that misses the window aborts the checkpoint; the subsuming
contract makes the next one cover a longer span"). The subsuming contract makes the *confirmation* side
tolerant of loss; it says nothing about the *minting* side, and Flink states the writer-side obligation
explicitly. This proposal implements "abort ≠ discard" for the pending set it persisted and omits it for the
pending set it did not.

**Fix direction.** One sentence on `PrepareCommit`: it MUST return every committable not yet recorded in a
durable checkpoint, including those minted for a checkpoint that aborted. Plus: the in-memory
`Committables[id]` for an aborted id must be carried forward and merged, and the conformance suite must
inject an abort between `PrepareCommit` and `Commit`.

---

### D14. Heartbeat batches carry a cursor and no records, so they have no `Origin.Seq` and no place in the tracker — and they fire exactly when unsettled records exist

**At fault:** `SplitBatch{Heartbeat: true, Cursor: Some(...)}` (§7.3) versus
`prefix.Tracker.Track(ctx, seq uint64, cursor opt.Opt[T], n int)` and `Origin.Seq` "assigned by the core as
it drains `SplitBatch`es" — per record.

A heartbeat batch has zero records, hence zero sequences, hence nothing to `Track`. The interface gives no
way to enqueue a cursor *behind* existing unsettled work.

**The interleaving.**

- CDC reader emits 500 records mid-transaction: `SplitBatch{Records: 500, Cursor: absent}` — correct, "not
  a safe resume point".
- The sink stalls (429 backoff, 60s).
- 30s of no new upstream activity for that split. The reader emits `SplitBatch{Heartbeat: true, Cursor:
  Some(current LSN)}` — the whole point of the heartbeat being to keep the stored position inside the
  replication slot's retention window.
- The core has no tracker entry to hang this cursor on. If it takes the cursor as immediately committable
  (the natural reading of "a batch with no records whose only purpose is to advance `Cursor`"), the next
  checkpoint commits a position **past the 500 unsettled records**.
- `kill -9` → 500 records lost.

**Consequence.** Loss on the *idle* path, and the heartbeat is designed to fire when the pipeline is
otherwise not progressing, which is precisely when unsettled work is most likely to be sitting there. This
is a small interface gap with a large blast radius, and it is invisible in testing because heartbeats fire
on a timer that test pipelines rarely reach.

**Fix direction.** State that a heartbeat cursor enters the tracker as a zero-descendant entry at the next
sequence (`Track(seq, cursor, n=0)`, which the interface already supports — `n=0` is documented for filtered
records) so it can only resolve after every prior sequence has settled. That is a one-line fix and it needs
to be written down.

---

### D15. Schema change requires a cross-worker, cross-split quiesce that has no mechanism; and `ApplySchemaChange` is not required to be idempotent although the commit after it can fail

**At fault:** §11.5's six-step quiesce rule, `Writer.Flush(FlushSchemaChange)`,
`SupportsSchemaApply.ApplySchemaChange`, `Split.Schema opt.Opt[schema.Ref]`.

**(a) The quiesce is not expressible.** Step 2 is "the core stops admitting records for that stream at the
transform chain's input". A stream may have many splits (§6.4 makes "one `Change` split per stream" and "one
per shard" both legitimate; the chunked engine makes 320 `Scan` splits on one stream), and those splits are
on **different workers** with independent `Fetch` loops and independent `Writer`s. Quiescing a stream is
therefore a distributed barrier across workers — the thing §0 and §13.4 explicitly refuse to build. There is
no `ReaderEvent` for it in the sanctioned set (events are best-effort by contract, §7.5, so they cannot
carry it), no `Coordinator` primitive, and no ordering rule. The single-process case works; the
`canal serve` case does not, and the design claims the two are the same code path.

**(b) Scan splits pin schema at their own start, so one stream's chunks are read under divergent schemas.**
"applies it ONLY during a `Change` split (a `Scan` split's schema is pinned at its start)" (§11.4). 320
chunks scanned over an hour across a DDL: chunks 1–140 pinned at fp₁, chunks 141–320 at fp₂. Both feed one
destination object. `Opening.Schemas` was computed at `Writer.Open`. The DDL is applied only when the
`Change` split — which does not exist yet, per D3's handoff gate — observes the change. So fp₂ rows are
written against an fp₁ destination for the remainder of the snapshot: `ClassPermanentMapping` per record at
best, silent column drop at worst. Nothing in the plan reconciles divergent per-split schema refs before
the handoff, and `Split.Schema` makes the divergence *representable* without making it *resolvable*.

**(c) `ApplySchemaChange` idempotency is unstated and the sequence requires it.** Step 4 applies the DDL;
step 5 writes the new epoch into the checkpoint. A crash between them replays step 4 after restore. Under
`DriftLenient` — defined as "an `AlterFieldType` becomes `RenameField` + `AddField`" — a second application
adds a second renamed column. Under `ChangeAddField` a second application may error and wedge. The proposal
correctly diagnoses Debezium's two-store divergence and correctly puts epoch and position in one record; it
then leaves the *external* side effect outside that transaction with no idempotency obligation, which is the
same class of bug one layer out.

**Fix direction.** Require `ApplySchemaChange` to be idempotent and say so on the method (it is the only
safe contract for any operation outside the checkpoint transaction). For (a): make schema change a
*plan-level* transition — the enumerator retires and re-mints splits across the boundary — so the barrier is
the existing checkpoint barrier rather than a new one. For (b): pin the *pipeline's* scan schema for the
whole scan phase and treat a mid-scan DDL as a reason to fail or restart the scan, and say which.

---

### D16. Per-record retry reorders records within a split with no knob, no warning and no version field — and the chunked engine *requires* an upsert sink

**At fault:** `WriteResult.Failed []RecordFailure` + `RetryPolicy` + `DestUpsert`, versus §5.5's ordering
contract ("ordering is meaningful only within one `Origin.Split`") and §15.4's placement of the ordering
warning on `transforms.concurrency` alone.

§19.3.1 is the trace: records 12,001…12,500 are written; 12,207 and 12,208 fail with a 429 and are
"re-batched and retried with full-jitter backoff", while 12,209…12,500 are already durable. If 12,207 and,
say, 12,450 are updates to the same entity key, the destination now holds 12,207's older value on top of
12,450's newer one. Under `DestUpsert` — mandatory for the chunked engine, per the submit-time table — that
is a wrong row, not a duplicate row.

The design has nothing for a sink to order on. `Change` has no version. `Origin.Seq` exists and is monotone
per split and would do the job, but it is not named as the upsert-ordering key, no `DestUpsert` contract
requires honouring it, and §15.4 concedes the absence ("a source without a monotonic version field") while
attributing the risk only to a concurrency knob the operator can decline to turn.

**Consequence.** Every pipeline that (a) uses per-record retry (the default: `PerRecordFailures` + a
`RetryPolicy` with `MaxAttempts > 1`) and (b) upserts by key (mandatory under the chunked engine) can write
stale values, silently, in normal operation with no crash. On this lens that is worse than the duplicates
the design honestly owns.

**Fix direction.** Either retry preserves order within a split (hold the split's subsequent batches until
the failed prefix clears — which the tracker's prefix semantics already imply, so this may only need
stating) or `DestUpsert` requires a monotonic version and the core supplies `Origin.Seq`/`Origin.Pos` as it,
checked at submit time against a `SinkCapabilities.HonoursVersion` flag.

---

### D17. `ord.Ordinal.Compare`'s cross-version contract is unimplementable, and its failure mode is the fail-open filter

**At fault:** `Ordinal.Compare(b Ordinal) (int, bool)` — "returns −1, 0 or +1 and **true** when both
ordinals carry `Order` **and were written by mutually comparable versions**" — plus §4.3's obligation:
"a change to `Order` MUST bump `Version` and the connector MUST be able to compare across versions or
declare the old version unreadable."

Comparison happens in **core**, with `bytes.Compare` over `Order`. The connector has no comparator hook
anywhere in the interface set: there is no `PositionComparer` (deliberately dropped, §20/D3), and
`blob.Serializer` deserialises a connector type but is not consulted by `Ordinal.Compare`. So "the connector
MUST be able to compare across versions" is an obligation with no method to discharge it, and the core is
left with two options: compare raw bytes across versions (unsound — the whole premise of bumping `Version`
is that the encoding changed) or return `(0, false)`.

**Consequence.** `(0, false)` feeds the fail-open filter of D9: `return !ok || cmp > 0` emits. A connector
upgrade that changes its key encoding therefore turns the chunk filter off for every persisted completion,
for the whole remaining stream phase, and the observable symptom is stale overwrites in the destination
(D9's mechanism) with no error. The plan's persisted `KeyRange`s were written under the old encoding and
`findByKeyBinary`'s sort is over those bytes, so even the *lookup* is wrong, not just the watermark
comparison.

**Fix direction.** Forbid it: `Order`'s encoding is immutable for the life of a `SplitID` namespace, and a
change requires a generation bump that re-plans from scratch (which `AdoptsStateOf` could gate). Or make the
version part of the sort domain: never compare ordinals of different versions, and refuse the pipeline with
`ClassPermanentContract` at restore when the plan contains mixed versions. Either way it must be a refusal,
never a degradation.

---

### D18. The per-chunk backfill buffers a whole chunk in core memory, outside the "bounded by construction" claim

**At fault:** §13.7 step 2 — "reads the chunk into a **buffer** … the CORE **upserts** them into the buffer
… emits the buffer" — versus §12.1: "**every edge in the pipeline is a bounded Go channel, so unbounded
growth is not expressible anywhere**".

Chunk size is chosen by the connector's `SplitKeySpace`, which returns `[]ord.KeyRange` with no size
declaration and no core-side cap; `SourceCapabilities` bounds `MaxSplitsPerReader` and `MaxSplitsTotal`
(counts) and nothing bounds a chunk's row count or byte size. §19.2 uses 62,500 rows per chunk × 4
concurrent readers = 250,000 records resident, keyed by `Change.Key`, in core, per worker. A connector that
chunks a 200M-row table into 64 chunks puts 3.1M rows in one buffer.

**Consequence on this lens.** OOM is an availability problem, but the *correctness* consequence is that the
buffer is the only place the chunk's rows exist between `HIGH` and emission: an OOM kill there loses the
whole chunk's work and, under D1, may leave a partially-emitted chunk with an inconsistent watermark. R6's
"bounded by construction" is asserted for channel edges and quietly not held for the one place the core
accumulates an unbounded, connector-sized collection.

**Fix direction.** The chunk buffer must be a bounded, spillable structure with a declared cap, and
`SplitKeySpace` must take a target size (rows or bytes) from the core and be refused at registration if it
cannot honour it. Or the replay-and-upsert must stream (read the chunk in key order, replay in key order,
merge) rather than materialise, which is possible when the split is `Ordered`.

---

## 3. Minor defects

### D19. Two representations of "this split is complete", with no authority rule

`Split.Watermark` (whose presence *is* completion: `Finished()`) travels in `Checkpoint.Workers[*].Splits`;
`split.Completion` travels in `Checkpoint.Plan.Completions`. They are written in the same transaction and
can disagree (D6 shows how). Nothing states which the core believes on restore, and `Reader.Assign`'s
contract does not say what a reader should do with a split it is handed whose `Watermark` is already set.
This is R9's "one entity, one representation" with a correctness edge: the wrong choice either re-scans a
completed chunk (duplicates, tolerable) or drops an incomplete one (loss) — and `ExpectedCompletions` can
then never be satisfied, wedging the handoff permanently.

### D20. `(Split, Seq)` is advertised as durable idempotency layer 3 but is not durable

`Origin.Seq` is "monotonic within `Split` across the whole life of the pipeline **generation**", and
`Generation` is "bumped on every pipeline spec change or **full restart**" (§2). So `Seq` restarts after
exactly the event whose duplicates need deduplicating. §13.9 calls `(Split, Seq)` the layer "which exists
for every record from every source unconditionally", and `record.Ref` presents `{Pipeline, Generation,
Split, Seq}` as "the durable, cross-restart identity of a record". Two generations emit different Refs for
the same upstream row, so layer 3 cannot detect a restart-induced duplicate. Only layers 1
(`Origin.Upstream`, optional and often empty) and 2 (`Change.Key`, optional) can, and combined with D7 that
leaves the design with no mandatory dedupe key.

### D21. `prefix.Tracker` has no exit from `Fail`

§13.6 defines `Fail(seq, e)` — "the prefix DOES NOT advance past seq" — and no method to release it.
§19.3.1 T5 calls `Tracker.SettleDeadLettered(12_411)`, which is not on the type. Consequences: the
documented dead-letter-then-advance path is unspecified; `Fail`ed entries presumably occupy tracker
capacity, and since capacity *is* the in-flight bound and `Track` blocks when full, a split accumulating
`Limit` successfully-dead-lettered records deadlocks. Also unstated: whether the DLQ sink's own `Write`
returning nil is sufficient durability, given the DLQ is "an ordinary registered sink" with its own
batching and `Flush` (D10 applies recursively, and the DLQ's own writes are tracked by no tracker).

### D22. `SkewClamp` writes a fabricated timestamp to the destination

Clamping rewrites `Change.CommitTime` — a field the sink writes — and preserves the original "in `Meta`
under a core-owned key". `Meta` is "NEVER serialised into a sink's payload unless a metadata filter
explicitly selects keys" (§5.3). So the default behaviour of the default skew mode is: the destination
receives a timestamp the source never emitted, and the truth is dropped. "Nothing is lost" (§12.5) is true
of canal's process memory and false of the destination. `SkewClamp` should not be the default for a
data-movement tool, or the original must ride in the `Change` facet.

### D23. Two spellings of end-of-input; two knobs for mid-split resume

`FetchDrained` (a `FetchStatus`) and `fail.ClassEndOfInput`/`ErrEndOfInput` both mean "exhausted", with no
statement of whether the core treats them identically. `Split.Ordered` ("what LICENSES mid-split
checkpointing") and `SourceCapabilities.MidSplitResume` ("declares that a split's cursor may be committed at
any batch boundary") are two declarations of one property at two scopes with no precedence rule; §19.2 sets
both true and never says what `Ordered: true, MidSplitResume: false` means. R9, with a correctness edge in
both cases.

### D24. Stringly map keys reintroduce the parsed identifier the design boasts of eliminating

`PlanState.Assigned map[string]Assignment` keyed on `SplitID.String()`; `Checkpoint.Workers
map[string]WorkerState`; `DedupeState.Keys map[string]KeySet`. `String()` renders `"<stream>/<kind>/<seq>"`
and `schema.StreamID` is unconstrained, so a stream id containing `/` collides two splits into one map
entry — one assignment silently lost or overwritten, one dedupe key set shared across splits (an R5
scope violation). §6.1 spends a paragraph on why a stringly split id is a documented Flink CDC scar and
asserts "there is a test asserting that no canal package calls `strings.Split` on a split id"; the id is
nonetheless the durable key of three maps.

---

## 4. Rule checks

### R4 — "an acknowledgement means durable"

**Fails, structurally, in three independent places.** (i) D2: the core commits the *reader's* read-ahead
cursor when the tracker has resolved nothing. (ii) D10: `Write` returning nil is documented as durability
and the shipped example sink returns it for unsynced bytes. (iii) D11: `Buffer.Durable()` moves the ack
boundary earlier without making the buffer replayable, which is the abandoned attempt's exact failure with
a blessing. The proposal's claim that "violating R4 requires a sink to lie about a return value" is the
strongest sentence in it and it is not true: two of the three paths involve no sink at all.

Credit where due: the *direction* is right everywhere, and `Buffer.Durable()` as "one expression decides the
boundary" is the correct idea. The failure is that the expression is not written down.

### R5 — "dedupe keys are scoped, and committed after the write"

**Partially satisfied.** Scoping is genuinely good: the key is `(TenantID, PipelineRef, source, SplitID)` +
record key, constructed only by the core, and `PipelineRef` making an unscoped key unconstructible is the
right mechanism. `DedupeState` inside `Checkpoint` gives "committed with the data it protects" structurally.
Duplicate-as-success (`WriteResult.Duplicates`, `ClassDuplicate`) directly fixes the
permanently-unresubmittable bug. `Window` as configured rather than emergent from a FIFO is right.

**But the mechanism it relies on for the common case does not exist** (D7: `Origin.Pos ≤ committed cursor`
cannot be evaluated because the cursor is a `VersionedBlob`), and the fallback identity layer is not durable
(D20). So the *design* satisfies R5 and the *interface set* does not.

### R6 — "a buffer without a rejection path is not a buffer"

**Rejection path: satisfied, and well.** `PushOutcome{Accepted, Rejected, Overflowed, DroppedNewest}` puts
the outcome in the type, `Depth`/`Bytes` are mandatory, `Trim` is mandatory, drop-newest preserves the
committable-prefix invariant, and "no configuration under which records are silently lost" is enforced by
the enum rather than by prose. This is the best-executed rule in the proposal.

**Bounded by construction: fails twice.** D18 — the chunked engine's in-core chunk buffer is unbounded and
connector-sized, in direct contradiction of §12.1's "unbounded growth is not expressible anywhere". D11 —
`Pop` (destructive) plus `Trim(id)` (non-destructive) is R6's own observed defect, two incompatible buffer
models, relocated from two types into one.

### R7 — "if the design says retry the failures, the contract must name them"

**Satisfied, and it is the proposal's best rule compliance.** `WriteResult{Failed []RecordFailure,
Duplicates, Written, Bytes}` with the four `(res, err)` combinations enumerated in `Write`'s doc comment;
`RecordFailure` correlating by `RecordID` rather than by position (fixing trap 2 at the root);
`CommitOutcome.Disposition` with five values including `RetryUpdated` for partial success and **no**
silent-discard outcome; `Report.Failed []SplitFailure` for planning; `[]cfg.Diagnostic` for config; the
operator/developer audience split at the point of raise. `Class.Retryable()`/`Terminal()` derived in one
place so a class and a flag cannot disagree. This is materially better than any surveyed system.

Two gaps, both minor against the rule: `res.Written` is unconstrained when `err != nil` and `Failed` is
empty (the "nothing durable" case) so it can be a lie; and `Commit`'s failure shape has no record-level
granularity (D12), which is where R7 stops holding for tier 3.

### Traps from `_decision-space.md` Part 3

Avoided well: 1 (provenance immutable, `Derive` only), 2 (stable `RecordID`), 3 (in-band boundaries; note
D14 makes the timer path unsafe rather than the boundary model), 4/5 (phase as a derived function; mid-scan
resume *representable* — though D1 shows it is unsound as specified), 6 (per-phase parallelism, no restart),
7 (pull-based + delayed reassignment), 8 (transactional store, Kafka rejected), 9 (contexts as the growth
path), 10 (no type parameters on plugin interfaces), 11 (capability = data, `ord.Order` instead of a
comparator method — the single best move in the proposal), 12 (bounded retry with named terminal), 13
(capacity-2 channels), 14, 15, 17 (dedupe on `CheckpointStore`, not a cache), 20, 21.

**Walked into: 16 and 18.** Trap 16 is *silent capability degradation*: the submit-time table is excellent
and then the runtime degrades silently in three places the table does not cover — the chunk filter failing
open on absent `Origin.Pos`/`Change.Key` (D9), `Ordinal.Compare` failing open across versions (D17), and
`min(source, sink, declared)` being validated at submit time while the zombie-worker window (D4) makes the
sink tier unattainable at runtime. Trap 18 is *a terminal outcome that silently discards data*: the
proposal deletes `signalFailedWithKnownReason` and is right to, then reproduces the outcome by another route
in `DispositionDeadLetter` (D12), where the prefix has already advanced and the DLQ record cannot contain
the records.

**Partially walked into: 19.** The design correctly makes `SplitID` a struct, then uses `SplitID.String()`
as the durable key of three maps (D24).

---

## 5. What this proposal does better than any plausible alternative — on this lens

These are the parts a synthesiser should carry regardless of which proposal wins. All are correctness
mechanisms, not ergonomics.

1. **Comparability as *data* (`ord.Order`, `ord.Scalar`) rather than as a method.** This is the single most
   important correctness idea in the document. It is what makes a *generic* chunked-snapshot filter possible
   at all: the core can decide "is this key in that range" and "is this position past that watermark"
   without interpreting the connector's semantics and without a call the connector could get wrong
   differently per source. A comparator method (Flink's `Offset.isBefore`) cannot cross a process boundary
   and cannot be served to a browser; an order-preserving byte encoding can do both. The proposal's own
   §22.4 honestly names the cost (a wrong encoding is a silent data bug) and the mitigations — shipped
   encoders for every Go primitive and tuples, plus a property test against a connector-supplied reference
   comparator — are the right shape.

2. **`SplitBatch.Cursor` absent = "not a safe resume point", as a typed property.** Benthos's
   `// This has no offset - it's a snapshot message` nil convention becomes a type. It cleanly separates
   *record position* from *safe resume point* without a second field on every record, and it is how a CDC
   reader says "we are mid-transaction" and a paginated reader says "replay me from the start". The
   companion `Origin.Pos` (fine-grained, never a resume point) is the right complement.

3. **Completion as data (`Watermark` presence) plus `Completion.DurableAt` gating the handoff.** Copying
   Flink CDC's `isSnapshotReadFinished() { return highWatermark != null; }` and then adding the
   durability-of-the-completion gate is correct and is the part most systems omit. `From = min(HIGH)` rather
   than the latest position, with the per-chunk filter suppressing the redundant range, is the right no-gap
   construction and is argued precisely (§19.2 Phase 6, four numbered properties).

4. **The Checkpoint Subsuming Contract implemented with "abort ≠ discard" stated explicitly**, plus
   publishing everything `≤ id` on confirm, plus `Committables` persisted inside the checkpoint so a lost
   confirmation is repaired by the next one rather than by bookkeeping. Also: five dispositions with **no**
   silent-discard outcome, `DispositionAlreadyCommitted` counted separately so a restart replay is visible,
   and `RetryUpdated` for genuine partial success. The gaps (D12, D13) are additions to this, not
   replacements for it.

5. **A per-split contiguous-prefix tracker in core, with `Stuck()`.** Per-split scoping means one stalled
   split cannot hold back another's cursor — the pathology Connect's own `SubmittedRecords` javadoc warns
   about. `Track(seq, cursor, n)` with `n` as the descendant count absorbs fan-out, filtering, 1→N decode
   and N→M rebatching without a closure, which keeps the whole design wire-shippable; that is a genuinely
   better answer than an ack graph *if* the descendant count is always known, and it deletes decision-space
   Chain E's risk rather than mitigating it. `Stuck()` returning `(oldest unsettled seq, duration)` turns
   "the checkpoint is not advancing" into a named diagnosis and should be non-negotiable in any proposal.

6. **Heartbeat batches in core.** The upstream-retention-window failure ("the binlog file or GTID set may
   have been cleaned in its last committed binlog position") is a real production loss mode that every
   surveyed system handles per connector or not at all. Putting it in the batch type costs one bool. Fix
   D14 and keep it.

7. **The submit-time refusal table.** `chunked ⇒ sink upserts and is Idempotent`, `chunked ⇒ ComparableKeys
   + ComparablePositions + Replayable`, `declared guarantee ≤ min(source, sink)`, `MidSplitResume false ⇒
   buffer is not durable-ack`, and crucially **"store supports atomic multi-key `Commit` when tier >
   at-least-once"** — validating the *deployment's stores* as part of the pipeline is an idea no surveyed
   system has and it directly prevents Connect's documented unrecoverable state. The table is incomplete
   (D4, D9) but the mechanism and the discipline of pairing each row with the prior-art failure it prevents
   should be copied verbatim.

8. **`PushOutcome` as R6 in the type system**, with drops always the newest so the committable-prefix
   invariant survives, and a non-zero drop counter forcing `Degraded`.

9. **`Fence` on every checkpoint write, with leadership never trusted for correctness.** The reasoning from
   the verified k8s leaderelection caveat to "the store rejects a stale fence" is exactly right. The defect
   (D4) is failing to extend the same reasoning to the sink, not the reasoning itself.

10. **`Reconcile(plan, catalog, now)` as a pure function.** "Restored state met a changed world" is the case
    naive implementations get wrong and it is unobservable until it is a stuck pipeline; making it a pure,
    table-testable transition is the right call, and the crash-injection conformance harness sketched in
    §22.2 (restore the enumerator at every checkpoint boundary, assert no split lost or duplicated) is the
    test that would have caught D8 and D5.

---

## 6. Summary table

| # | Severity | Defect | At fault |
|---|---|---|---|
| D1 | fatal | Mid-chunk `Scan` resume invalidates the chunk's single watermark → stream-phase filter suppresses real changes | `Completion{Keys, Watermark}` + `MidSplitResume` on a `Chunkable` source |
| D2 | fatal | Cursor authority undefined; reader's read-ahead cursor is committed when `Resolve` has not advanced | `Reader.Snapshot` "verbatim" vs §13.4 step 8 |
| D3 | fatal | `AllCompletionsDurable` is vacuously true at zero completions and is the named handoff gate | `PlanView.AllCompletionsDurable` |
| D4 | fatal | Fence protects the store, not the sink; 150s designed double-writer window; committables unfenced | `store.Fence` + §16.4 timers + `SupportsCommitter` |
| D5 | fatal | Revoke hands over at the reader's cursor while the tracker holds unsettled seqs → wedge or loss | `Reader.Revoke` + per-split `Tracker` + `Origin.Seq` |
| D6 | fatal | Completion gated on "every record in this batch" instead of the split's resolved prefix | `SplitBatch.Done`/`Watermark` |
| D7 | major | `Cursor` is `VersionedBlob`, so ordered-split dedupe, bounded-split completion and progress are inexpressible | `Split.Cursor` / `SplitBatch.Cursor` |
| D8 | major | Split placeable on two readers; duplicate `Completion`s satisfy a count-based completeness check | `PlanState.Unassigned` acknowledgement rule |
| D9 | major | Chunk filter fails open on unvalidated `Origin.Pos`/`Change.Key`; failure mode is stale overwrite | `engine.ChunkFilter.shouldEmit` |
| D10 | major | `Write`-nil vs `Flush`-nil both asserted as the ack; no capability distinguishes; example sink affected | `Writer.Write` / `Writer.Flush` / §18.4 |
| D11 | major | Durable buffer acks at `Push` but `Pop` is destructive with no replay cursor | `Buffer` |
| D12 | major | `DispositionDeadLetter`'s "does not advance the prefix" unimplementable for 2PC; no record linkage | `Disposition` + `Committable.Splits` |
| D13 | major | `PrepareCommit` not required to re-report committables from an aborted checkpoint | `SupportsCommitter.PrepareCommit` |
| D14 | major | Heartbeat cursor has no sequence, so it can commit past unsettled records | `SplitBatch.Heartbeat` + `Tracker.Track` |
| D15 | major | Cross-worker stream quiesce has no mechanism; divergent per-chunk schemas; DDL not required idempotent | §11.5 + `SupportsSchemaApply` |
| D16 | major | Per-record retry reorders within a split; upsert without a version field → stale overwrite | `WriteResult.Failed` + `RetryPolicy` + `DestUpsert` |
| D17 | major | `Ordinal.Compare`'s cross-version obligation has no connector hook; degrades into the fail-open filter | `ord.Ordinal.Compare` |
| D18 | major | Chunk buffer is unbounded and connector-sized, contradicting "bounded by construction" | §13.7 step 2 |
| D19 | minor | Two representations of split completion with no authority rule | `Split.Watermark` vs `split.Completion` |
| D20 | minor | `(Split, Seq)` sold as durable idempotency but `Seq` restarts per generation | `Origin.Seq` / `record.Ref` |
| D21 | minor | No exit from `Tracker.Fail`; `SettleDeadLettered` used but undefined; DLQ durability unstated | `prefix.Tracker` |
| D22 | minor | `SkewClamp` (the default) writes a fabricated `CommitTime` and drops the original by default | `SkewPolicy` + `Meta` |
| D23 | minor | Two spellings of end-of-input; two scopes for mid-split resume with no precedence | `FetchDrained`/`ClassEndOfInput`; `Ordered`/`MidSplitResume` |
| D24 | minor | `SplitID.String()` is the durable key of three maps; a `/` in a stream id collides | `PlanState.Assigned`, `Checkpoint.Workers`, `DedupeState.Keys` |

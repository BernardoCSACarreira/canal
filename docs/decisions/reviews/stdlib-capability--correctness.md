# Review: `database/sql` for data movement — lens: correctness of data-movement semantics

**Reviewer role:** hostile expert, single lens. I judge only what is guaranteed after a crash, whether a
checkpoint can commit for data not durably in the sink, whether the claimed delivery tiers are reachable with
these interfaces, and whether the snapshot→stream handoff is sound. I do not score elegance, ergonomics,
extensibility or prose quality except where they bear on those questions.

**Score: 4 / 10.**

**Verdict in one paragraph.** This proposal contains the best *architecture* for correctness of any shape I
would expect to see for canal: a per-assignment contiguous-prefix resolver, no clock anywhere in the ack path,
`AckPoint` reified as an admission-checked value, delivery tier computed as `min(source, sink, requested)` with
refusal rather than downgrade, and `Assignment.Bounded`/`AssignmentState.Done` as data instead of a phase enum.
Those are the right answers and several of them are better than any shipped system. But the **interface set as
written cannot implement them.** Three of the guarantees the document sells are structurally unreachable, not
because a connector author might err but because the required channel does not exist in the contract:

1. There is **no channel by which a Reader communicates a per-record position**, so the prefix resolver cannot
   produce the `Resume` cursor its own walkthrough claims. The two implementable readings are "advance to the
   batch-end position" (**data loss of every uncommitted record in the batch**) or "advance only on
   fully-committed batches" (walkthrough (c) is fiction, and one poison record pins an assignment forever).
2. The chunked-snapshot engine — the flagship, `StrategyChunked` — **has no per-chunk HIGH watermark**, so the
   Offset Signal Algorithm it claims to implement cannot be implemented, and the concurrent
   snapshot+stream interleaving **permanently corrupts the destination** with stale values.
3. `Committable` carries a single `AssignmentID`, so the staged set of a Writer that receives records from many
   assignments is **unrepresentable**. The exactly-once tier does not typecheck against its own runtime shape.

Each is a *structural* hole, which is the harshest category: a careful connector author cannot avoid it, and no
amount of `canaltest` catches it. Against R4 the design's headline claim — "R4 becomes a type rather than
prose" — is half true: the `Flusher`/no-`Flusher` axis is genuinely reified, and then `WriteResult.Durable` is
handed back to the connector as an unclamped boolean that the engine is *explicitly instructed* not to
cross-check. That is R4's abandoned-attempt catastrophe with a nicer type name.

The score is 4 rather than 2 because the defects are almost all *additive fixes to an otherwise correct
skeleton* — a per-record cursor slot, a watermark field on the finished-chunk record, a set of assignments on
`Committable`, a fencing token on `Set`. The skeleton is worth keeping. The score is not 6 because as written
this design silently loses data in its most ordinary configuration (a source that returns more than one record
per `Read`), and that is the one thing a data-movement tool may never do.

---

## Part 1 — The record's life, crash by crash

I trace one record through the design and name what survives each kill. This is the frame for everything below.

| Point of crash | What the design guarantees | Actual guarantee as written |
|---|---|---|
| After `Read` appends, before batching | Re-read from `AssignmentState.Resume` | **Undefined.** `Read` returns only `error`; whether records appended before an error are valid is unstated (§4). Engine must diff `dst.Len()`; if it discards the batch and re-calls `Read`, the Reader's internal position has already advanced → **silent loss**. |
| In the batcher / buffer | Nothing committed, re-read on restart | Correct, *if* `Cursor` is per-record. It is not (D-1 below). |
| `Write` returned nil, no `Flusher` | Durable at destination | Correct — this is the design's best axis. |
| `Write` returned nil, sink has `Flusher` | "accepted", durability is `Flush`'s promise | **Violated by `WriteResult.Durable`**, which the sink sets and the engine is told to read "rather than a capability check on the hot path" (DG-1). A `Flusher` sink returning `Durable: true` advances the prefix for records in RAM. |
| Between `Flush` partial success and the checkpoint | Prefix advances only over durable records | **Inexpressible.** `FlushResult{Durable int; Outcomes []RecordOutcome}` gives a *count*, and `Flush` spans an unbounded number of prior `Write` calls, so the engine cannot recover *which* records are durable. |
| Between `Prepare` and the checkpoint being durable | Staged artifact aborted on recovery | **Orphaned forever.** The handle was never persisted, and `Abort(ctx, handles)` can only abort handles the engine knows. No `AbortStale`, no deterministic handle derivation. |
| Between the checkpoint and `Commit` | Repaired by the next checkpoint (Subsuming Contract) | Correct *only while `Commit` eventually succeeds.* On a permanent commit failure the position is already committed → **loss with no repair path and no DLQ, because the records are bytes inside an opaque `Blob`.** No committable expiry (D4 item 8, explicitly demanded, absent). |
| Mid-snapshot, `StrategyChunked` | Chunk resumes from its own cursor with the same `Range` | Resumes, but **unsound**: the remainder of the chunk is now read at a later instant with no new LOW/HIGH, mixing two snapshot epochs inside one range. |
| Mid-snapshot, `StrategyPhased` | Engine checkpoints at the phase boundary | Only at the boundary. Mid-phase resume requires the connector to encode phase and snapshot progress **inside the opaque cursor** — the exact trap this document spends four paragraphs condemning Benthos and Airbyte for. |
| Worker fenced out, still writing | Lease token rejects the write | **The token is not in the signature.** `Set(ctx, pipeline, epoch, blobs, expectEpoch)`. Prose claims fencing; the interface has an epoch CAS and no ownership check. |

---

## Part 2 — Defects

### D-1 (fatal). There is no per-record position channel, so the prefix resolver cannot produce a correct `Resume`

**At fault:** `Record.origin` (unexported, engine-assigned) + `Positioner.Position() Cursor` as the only
position-bearing method + `Reader.Read(ctx, dst *RecordBatch) error`.

`Origin.Cursor` is documented as "the source position AFTER this record". `origin` is unexported and assigned
by the engine (§2, and walkthrough (a) step 8 shows the engine writing it). A Reader constructs `*Record`
values in its own package; it has **no exported field, setter, or constructor argument for a cursor.** The only
position-bearing method in the entire contract is `Positioner.Position()`, defined as "the cursor immediately
after the last record appended by the **most recent Read**" — i.e. batch-granular.

Therefore, for the ordinary case of a Reader appending 500 records in one `Read`, the engine possesses exactly
one cursor: the position after record 500.

Walkthrough (c) step 3 is the proposal's own statement of the intended behaviour:

> the longest contiguous committed prefix ends at `Seq 1382`, so `AssignmentState.Resume` advances to
> **record 1382's cursor** and not one record further.

Record 1382's cursor does not exist and cannot be obtained. Only two implementations are available:

**(a) Assign the batch-end cursor to every record in the batch.** Then advancing the prefix to Seq 1382
advances `Resume` to the position after record **1540**. Restart re-opens the Reader at that cursor, which by
`Seekable`'s contract yields "records strictly after that position" — records 1383–1540 are **never re-read.**
1383 was dead-lettered (fine) but 1384–1540 were `OutcomeRetriable` and never durable. **157 records silently
lost behind a healthy-looking checkpoint.** This is R4's abandoned-attempt failure reproduced exactly:
"upstream had committed its position past that data."

**(b) Advance only when an entire batch is terminal.** Then the prefix resolver's per-record granularity is
decoration, walkthrough (c) step 3 is false, and one permanently-retriable record pins the assignment's
position forever while every later record commits — unbounded in-flight retention (R6) and a stalled position
whose `MCheckpointAge` climbs until the `ReplayWindow` refusal (walkthrough (b) step 4) kills the pipeline.

The document never states which. Both are defects; (a) is data loss. Note that this is not a connector-author
mistake — the connector *cannot* supply the missing information, so the framework's own bookkeeping is wrong
by construction.

**Fix shape:** an exported `Cursor` field on `Record` (or a `canalkit.Positioned(rec, cursor)` mint path), and
an explicit statement that a record with no cursor inherits the nearest *preceding* record's cursor, never the
following one. The "engine owns provenance" instinct (correct, and the KIP-793 fix) has to make one exception
for the position, because the position is the one piece of provenance only the connector knows.

### D-2 (fatal). `StrategyChunked` has no per-chunk HIGH watermark, so concurrent snapshot+stream corrupts the destination

**At fault:** `AssignmentState.FinishedChunks ChunkSetRef` (ranges only) + `ChunkSetRef{Pages, Key}` + the
absence of any "current log position for this stream" method.

The document commits to the Offset Signal Algorithm by name: "`StrategyChunked`: … LOW/HIGH/END watermarks,
concurrent streaming during backfill, an INDEXED range filter". The research names the required tuple exactly:
`FinishedSnapshotSplitInfo{keyRange, highWatermark}` is "the entire snapshot→stream contract". Walkthrough (b)
step 5 restates the filter as "for each streamed record, a binary search over the finished-chunk ranges …
decides whether the backfill has already covered that key" — and drops the position comparison, because there
is nowhere to put a position.

`ChunkSetRef` points at "a paged, engine-owned set of finished chunk **ranges**." `AssignmentState` has
`Done bool`, `Resume Cursor` (a key-space cursor for a bounded chunk), `Range`, `ChunkerCursor` — no log
watermark. A bounded snapshot assignment's Reader is reading a `SELECT`; there is no method by which it or the
Source reports the *log* position at which its chunk read began or ended (Flink CDC's `displayCurrentOffset`).
So the engine cannot record LOW or HIGH per chunk. It has exactly one watermark: the plan-time low watermark
on the single unbounded assignment.

**Concrete corrupting interleaving.** Stream `public.events`, chunk `c = [100,200)`, plan-time LOW = LSN 1000.

1. Tail assignment starts reading the log from LSN 1000, concurrently (walkthrough (b) step 5: "The log-tail
   assignment **also runs concurrently**").
2. At LSN 1500 chunk `c`'s Reader `SELECT`s and buffers key 150 with value `V2`.
3. At LSN 1600 key 150 is updated to `V3`. The tail emits it.
4. Both are records in the same pipeline, in *different assignments*, batched independently ("ordered within a
   split, unordered across splits"), and distributed across `WriteParallelism` writers.
5. The tail's `V3` write completes at wall-clock T; the chunk's `V2` write completes at T+ε.

The destination holds `V2` — a value that was already stale when it was read — **permanently**, because no
later event for key 150 is coming. The range filter cannot save it: filtering on range membership alone drops
`V3` (loss), and emitting both without the `position > HIGH(chunk)` predicate produces exactly the reordering
above. Flink CDC's answer is the missing half: buffer the chunk, replay the log LOW→HIGH *into the buffer*
(upsert), emit the reconciled buffer, and only then let the stream filter admit `position > HIGH`. Every step
of that requires a per-chunk position the contract cannot carry.

There is a second consequence: with no HIGH, the filter's **self-retirement** ("once the stream position passes
`max(HIGH)` for an object, drop the filter") is also unimplementable, so the claimed zero steady-state cost is
unreachable and the filter must be carried for the life of the pipeline.

### D-3 (fatal). `Committable` names one `AssignmentID`; the 2PC tier cannot represent what a Writer actually stages

**At fault:** `type Committable struct { Epoch int64; Assignment AssignmentID; Handle Blob; Records int64 }`
against `WriteOpen{Partition int; Total int}` and `TwoPhaseWriter.Prepare(ctx) (Blob, error)`.

Writers are created **per write partition**, not per assignment (`WriteOpen.Partition 0..N-1`, `SinkPlanner`
negotiates the count). In walkthrough (b) there are 401 assignments and `WriteParallelism` writers. Every
Writer therefore receives records from many assignments. `Prepare` "stages everything written since the last
Prepare" — i.e. one handle covering records from many assignments — and returns a single `Blob`. The engine
must wrap it in a `Committable`, which can name **one** `AssignmentID`.

So the engine cannot record which assignments a staged artifact covers. The consequences compound:

- On recovery it cannot decide, per assignment, whether that assignment's committed prefix is backed by a
  staged-and-committed artifact or by nothing.
- `Abort` of one assignment's work is impossible; staging is per-writer, ownership accounting is per-assignment.
- If a worker dies holding leases on 12 assignments and another worker claims 5 of them, the staged artifact
  spanning all 12 is owned by nobody and named by one.

`Committable.Records int64` is likewise a count with no identity, so the engine cannot verify that the staged
set matches the prefix it is about to commit. **`ReqExactlyOnce` is admitted by a lattice over capability bits
whose implementing type cannot express the state it needs.** That is the sharpest possible form of "the claimed
delivery tier is not achievable with the interfaces as written."

**Fix shape:** `Committable{Epoch, Handle, Covers []AssignmentCoverage}` where coverage is
`{AssignmentID, ThroughSeq uint64, Records int64}`, or forbid multi-assignment writers (which costs the
write-parallelism model).

### D-4 (fatal/major). `CheckpointStore.Set` has no fencing token; the whole-map CAS permits zombie commits and lost updates

**At fault:** `CheckpointStore.Set(ctx, pipeline string, epoch int64, blobs map[string][]byte, expectEpoch int64) error`
against `Lease{Token int64}`'s documented role.

The `Lease` comment states the invariant plainly: "Every `CheckpointStore.Set` from a worker carries its lease
token, and the store rejects a write from a superseded lease. That is how two workers cannot silently clobber
each other." **`Set` takes no token.** It takes a pipeline id, an epoch, a whole map, and an epoch CAS. This is
the design rules' R13/R4 pattern — the API shape implies a property the store cannot enforce.

Two distinct failures follow, both reachable with skewed clocks and no partition needed:

**(i) Zombie commit.** Worker A's lease on assignment `x` expires (A is GC-paused, or its clock is slow). B
claims `x` and resumes from `x`'s stored `Resume`. A wakes, finishes its in-flight batch, and calls `Set` with
`expectEpoch = N` before B does. The CAS succeeds — nothing in `Set` knows A no longer owns `x`. `x`'s `Resume`
now points past records B has not written. B's next `Set` fails CAS, B re-reads, and B's own progress for `x`
is *behind* what A wrote, so B either regresses the cursor (duplicates, tolerable) or the merge keeps A's
(loss of B's work) — and A's cursor covers records A wrote to the sink and B will never write. **Committed
progress for data not durably in the sink**, with the fencing mechanism named in the comments and absent from
the type.

**(ii) Lost update at scale.** Twenty workers each own ~20 of 401 assignments and all write `blobs` for the
same pipeline. `Set` is whole-map with a single epoch CAS: each worker must read-modify-write the entire map,
and a worker writing a map it read at epoch N-3 **silently reverts** the rows another worker advanced at N-2.
The comment claims "only the dirty ones are rewritten"; the signature makes that inexpressible. The epoch
also becomes a global serialization point across all workers, so checkpoint frequency degrades with worker
count exactly when checkpoint frequency matters most.

**Fix shape:** `Set(ctx, pipeline string, epoch int64, writes []AssignmentWrite, fence []Lease) error` where
each write is validated against the lease that authorizes that assignment, and the CAS is per assignment row.

### D-5 (major, R4). `WriteResult.Durable` is a connector-authored boolean the engine is told not to cross-check

**At fault:** `WriteResult.Durable bool` — "For a Writer with no Flusher it is always true; the field exists so
the engine's ack path has one uniform thing to read **rather than a capability check on the hot path** (DG-1)."

DG-3 cross-checks *declared vs implemented capabilities* at admission. Nothing cross-checks a *per-call*
`Durable: true` from a sink whose `AckPoint` is `AckOnFlush` or `AckOnCommit`. The stated rationale for the
field forbids the clamp. So the mechanism by which R4 is claimed to become "a type rather than prose" is: the
type asks the sink whether the data is durable, and believes it.

Interleaving: a Parquet/S3 sink implements `Flusher` (rolls files on `Flush`). Its `Write` buffers rows in RAM
and returns `AllWritten(b.Len())` — the helper constructor the proposal supplies, which sets `Durable: true` —
because the author read `AllWritten` as "all records accepted". The engine advances the prefix, writes the
checkpoint, and calls `Acknowledger.Ack(Through: cursor)`, which deletes the SQS messages. `kill -9`. The
unflushed rows are gone from RAM, gone from SQS, and the checkpoint says they are delivered. This is
byte-for-byte the abandoned attempt's `202`-on-an-unsynced-slice, with the extra insult that the framework
supplied the helper that produced the lie.

The safe shape is available at zero cost: the engine computes durability from `CapabilitySet.AckPoint`, which
it already holds in the bound facade, and either drops `Durable` or clamps it
(`res.Durable = res.Durable && caps.AckPoint == AckOnWrite`). One `&&` on the hot path is not a DG-1 violation;
trusting the connector is an R4 violation.

### D-6 (major, R7). `FlushResult` cannot express a partial-durability prefix

**At fault:** `type FlushResult struct { Durable int; Outcomes []RecordOutcome }` + `Flusher.Flush(ctx)`
carrying no epoch, token, or record-set identity.

`WriteResult` works because a convention is stated: "Outcomes holds one entry per record that did NOT plainly
succeed", scoped to the batch just passed. `Flush` has no batch. It spans every `Write` since the last `Flush`
— an unbounded set of batches — and reports an integer. When `Flush` returns `{Durable: 1500, Outcomes: [3
entries]}` after five 500-record `Write` calls, the engine cannot determine which 1500 of 2500 are durable, and
cannot compute a contiguous prefix. Even the charitable reading ("everything not in `Outcomes` is durable")
contradicts `Durable` being a count, and a sink that knows only "the first two files closed" cannot enumerate
1000 `RecordOutcome`s to say so.

R7 says write the failure shape at the same time as the success shape. The success shape here is a count and
the failure shape is a per-record list; they do not compose. Consequence: an engine implementing this either
treats a partial flush as total failure (re-writing durable records — acceptable only if the sink is
`Idempotent`, which `Flusher` does not require) or as total success (**loss**).

**Fix shape:** `Flush(ctx) (*FlushResult, error)` where `FlushResult{DurableThrough map[AssignmentID]uint64,
Outcomes []RecordOutcome}` — a prefix per ordering scope, which is the only thing the resolver can consume.

### D-7 (major). Schema change is delivered out-of-band, so "schema before data" is unimplementable and a batch spanning an epoch is unrepresentable

**At fault:** `Runtime.Signal(ctx, s Signal) error` carrying `SignalSchemaChange`, against
`Reader.Read(ctx, dst *RecordBatch)` and `WriteOpen.Schemas map[StreamRef]*Schema` "pinned as of this writer's
schema epoch".

The document asserts the change is "an ORDERED, in-band schema change. In-band because it must be ordered with
respect to the records around it". It is not in-band. `Signal` is a separate method on `Runtime`; a Reader that
appends records 1–100 under schema S1, calls `Signal(SignalSchemaChange{S1→S2})`, then appends 101–200 under
S2, hands the engine one batch and one asynchronous signal with **no position and no marker**. The engine
cannot cut the batch at the change, cannot order the DDL against the records, and cannot know which records
belong to which epoch.

Two symmetric failures, both real:

- Engine applies the DDL on receiving the signal, then writes records 1–100 (old shape) into the new target
  schema → mapping rejections or, for a widening change, silent default-filling of a column that should not
  exist for those rows.
- Engine writes the batch first, then applies the DDL → records 101–200 (new shape, new column) are written
  into the pre-`ALTER` target → the new field is dropped. **Silent field loss on every schema change**, which
  is the drift failure the five-mode policy exists to prevent.

`Quiescer` exists but is useless here: the engine has no position at which to quiesce. And
`WriteOpen.Schemas` being pinned per-Writer-lifetime means a batch spanning an epoch has no valid schema to be
written under; the contract offers no way to retire and re-open a Writer at an epoch boundary the engine cannot
locate.

`SchemaChange.Epoch` compounds it: the connector fills `SchemaChange` (it constructs the Signal), so the
connector authors the epoch that the engine is supposed to commit atomically with the position. Two streams
changing concurrently produce conflicting epochs with no arbiter.

**Fix shape:** schema change must be a *record* in the batch (a control record, or a `Facet`), so it occupies a
position in the ordered stream and inherits an `Origin`. `Signal` is the right channel for progress and
degradation, and the wrong channel for anything that must be ordered against data.

### D-8 (major). `Reader.Read`'s error semantics are undefined with respect to records already appended

**At fault:** `Read(ctx context.Context, dst *RecordBatch) error`.

The method returns only an error. Go's own convention for this shape (`io.Reader`) is explicit that callers
must process `n > 0` before considering the error, and `io.Reader` returns `n` precisely so they can. `Read`
returns no count, and the contract says nothing about the validity of records appended before a failure, nor
whether `Positioner.Position()` is meaningful after a failed `Read`, nor whether the engine may call `Read`
again on the same Reader after an error (as opposed to closing and re-opening from `Resume`).

The dangerous default is the natural one: on error the engine discards the batch (records may be
half-constructed) and either retries `Read` or re-opens the Reader. If it retries `Read`, the source's internal
cursor has already advanced past the discarded records — **silent loss of everything appended before the
error**, on the most frequently executed method in the system, for the entirely ordinary case of a page fetch
succeeding and the next one timing out. If it re-opens from `Resume`, a source whose `Read` partially succeeded
must be `Seekable` for that to be safe, and `Seekable` is optional.

The same gap exists for `io.EOF` with records appended (legal under `io.Reader` convention, unaddressed here)
and for `Decoder.Decode(ctx, frame, dst)` — one frame → many records, error after partial append.

### D-9 (major, R5). `Origin.Seq` is not persisted, so the one durable record identity the design offers is unstable across restart

**At fault:** `Origin.Seq uint64` ("monotonic within Assignment", engine-assigned) against `AssignmentState`,
which stores `Resume`, `Params`, `Done`, `ChunkerCursor`, `FinishedChunks`, `Records`, `Bytes` — and **no last
Seq**.

Walkthrough (c) step 6 leans on cross-restart stability explicitly: record 1383 "is re-attempted and
re-dead-lettered (idempotently, keyed on `Origin.Assignment + Origin.Seq`, which is stable across restarts
unlike `RecordID`)."

Seq is assigned by the engine in memory. After restart the engine resumes the assignment at `Resume` and begins
numbering from — nothing is specified, and nothing is recoverable. `AssignmentState.Records` is a committed
count, not a Seq high-water mark, and it diverges from Seq the moment a transform expands 1→N (derived records
share a Seq) or a record is dropped. So the DLQ's idempotency key **collides across restarts**: run 2's Seq
1383 is a different record from run 1's Seq 1383.

This is R5's first bug — a dedupe key not carrying enough scope — reproduced in the one place the design does
dedupe. The consequence is the R5 catastrophe verbatim: a genuinely new poison record hits the DLQ, matches an
existing key, is treated as already-dead-lettered, is counted terminal, the prefix advances past it, and the
record exists nowhere. **Loss behind a healthy checkpoint, via the DLQ.**

A second R5 exposure sits next to it: the DLQ is a `Sink` supplied through `ComponentRef` (`dead_letter: Sink`)
and therefore has its own `AckPoint`. Nothing in `admit.Requirement` requires the DLQ sink's `AckPoint` to be
at least as strong as the primary sink's, or its `Delivery` class to be at least as strong. A DLQ sink with a
`Flusher` lets the engine advance the prefix on the strength of an unflushed DLQ write.

### D-10 (major). No safe-resume-point concept: the prefix resolver will commit a position inside a transaction

**At fault:** one `Cursor` per record, with no distinction between "position of this record" and "position at
which resuming is legal".

The research names this requirement directly (D3): "**Model 'safe resume point' distinctly from 'record
position'**, because CDC must resume only at transaction boundaries — Benthos's MySQL input threads a separate
`latestXIDPos` for exactly this." The proposal has `Change.TxID string` on an optional facet and nothing that
consumes it.

The prefix resolver advances to whatever record last committed. For a source whose transaction of 40 rows is
split across a batch boundary, the committed prefix lands at row 17. Restart re-opens at that cursor. Three
outcomes, source-dependent, none of them expressible as a refusal:

- The source cannot resume mid-transaction and restarts the transaction → rows 1–17 re-delivered
  (at-least-once, fine, but the pipeline was admitted as `EffectivelyOnce` on the strength of a sink
  `Idempotent` bool, and rows 1–17 of a re-run transaction may carry different values under a repeatable-read
  snapshot).
- The source resumes at row 18 → the destination holds a **torn transaction** permanently, and any sink
  applying per-transaction semantics (a staged-then-swap, an `OpTruncate` followed by inserts) is left in the
  intermediate state.
- The source rejects the cursor → hard failure on restart, which is at least loud.

`OpTruncate` makes this concrete and unavoidable: a truncate followed by 10M inserts, with the prefix committed
after the truncate and before the inserts, leaves the destination empty and the checkpoint says it is done.

**Fix shape:** either a `SafeResume Cursor` alongside `Cursor` in `Origin`, or a boolean
`ResumableHere` on the record, so the resolver clamps the advance to the last safe point. It must be a typed
property, not a convention.

### D-11 (major). No heartbeat / idle position advance, and the `ReplayWindow` check turns that into a refusal to start

**At fault:** the absence of any way to advance a durable position with zero records emitted, combined with
`Replayable.ReplayWindow()` being checked against `MCheckpointAge` at restart (walkthrough (b) step 4).

`Read` returning `(0 records, nil)` is legal. `Position()` may well have advanced — a CDC reader consuming log
events for tables it does not track advances its LSN without emitting anything. But the prefix resolver derives
`Resume` **from acks**, and there are no acks, so the position cannot advance. The research demanded this
explicitly (D6 item 10: "Give the source a heartbeat concept — a way to advance the durable position with no
records emitted … it belongs in core"). It is absent.

Consequence chain, which is a genuine production incident and not a theoretical one: a low-traffic stream is
quiet over a weekend. `MCheckpointAge` climbs to 60 hours. The source declares `ReplayWindow() = 6h`. On the
next restart — or, per walkthrough (b) step 4, on admission-on-restart — canal raises a `Refusal`: *"the source
guarantees 6h of replay; this checkpoint is 60h old."* **The pipeline refuses to start, and no data was ever
at risk.** The operator's only recourse is to waive a refusal about replay safety, which teaches them to waive
refusals about replay safety.

The `ReplayWindow` check itself is one of the best ideas in the document (see Part 3). It is the missing
heartbeat that turns it into a footgun.

### D-12 (major). `Idempotent`, `KeyRequirer`, `StructuredWriter` and `PartialTolerant` are unverifiable booleans carrying load-bearing guarantees

**At fault:** `type Idempotent interface { IdempotencyKeyFields() []string }`,
`type KeyRequirer interface{ RequiresKey() bool }`,
`type PartialTolerant interface{ TolerantesPartial() bool }` (sic).

`ReqEffectivelyOnce = ReqAtLeastOnce ∧ (CapIdempotent ∨ CapTwoPhase ∨ CapToken)`. So `EffectivelyOnce` — the
tier walkthrough (c) sells as "**No loss, and no duplicates at the destination**" — can rest entirely on a sink
returning a slice of field names. The engine never performs a dedupe operation, never holds dedupe state, never
observes an idempotent no-op except as an `OutcomeDuplicate` the sink volunteers. There is no dedupe store
interface anywhere in the design.

This is the failure mode the decision space names for option (c) in D9: "a capability *flag* still needs
*methods* to be useful — a bool saying 'I support chunked snapshots' is worthless without the chunking
methods." Here it is worse than worthless: it is the sole input to a delivery-class computation that the UI
renders as an effective guarantee. The proposal's own DG-2 — "the engine never substitutes a weaker algorithm
for a requested one" — is satisfied while the *actual* guarantee is whatever the sink author believed when they
wrote a method that returns two strings.

Contrast with `TwoPhaseWriter`, where the guarantee is backed by behaviour the engine drives and can observe
failing. The asymmetry is the defect: three of the four delivery tiers are behavioural, and the most commonly
reachable one is declarative.

Not deciding a dedupe store is a legitimate choice (R5's bugs cannot occur in state you do not keep). Selling
`EffectivelyOnce` on an unverified bool is not.

### D-13 (major). Chunked snapshot is admitted into an append-only sink with no upsert requirement

**At fault:** `ReqParallelBackfill = CapPlan ∧ CapChunk ∧ CapCompare ∧ CapSeek` and
`ReqResumableBackfill = CapChunk ∧ CapSeek` — **neither mentions the sink.**

The research states the requirement as a hard, submit-time check (D7): "**Chunked-snapshot output is keyed
upserts, not appends.** Flink CDC's exactly-once claim depends on the sink upserting by key. Make it an
explicit capability requirement checked when the pipeline is submitted."

The chunked design *necessarily* produces overlapping deliveries: the log is read from LOW while chunks are
read at LOW+δ, so every change between LOW and a chunk's read time is delivered twice (once as a log event,
once inside the snapshot row). The types exist to express the requirement — `ModeSet.Write` includes `Upsert`,
`KeyRequirer` exists, `Stream.Keys [][]string` exists — and the `Requirement` lattice never joins them. So
canal admits `StrategyChunked` → append-only sink with `Refusals: []`, and the destination receives duplicate
rows for every key touched during the backfill window, with a plan document that says
`Strategy: Chunked, Delivery: at_least_once` and no warning.

Combined with D-2 (no HIGH), an append-only destination gets duplicates *and* an unordered mix of stale and
fresh values for the same key.

### D-14 (major). Mid-chunk resume is unsound, and it is advertised as a feature

**At fault:** `Assignment{Bounded: true, Range: *KeyRange, Resume: Cursor}` + the `Chunkable` row's
`Unlocks: []string{"parallel backfill", "mid-backfill resume", "backfill ETA"}`.

Walkthrough (b) step 3 resumes each of 20 partial chunks "strictly after the cursor" with the same `Range`. Two
snapshot instants are now stitched inside one key range: keys below the cursor as of time T1, keys above as of
T2. Under the Offset Signal Algorithm the chunk's reconciliation interval is `(LOW, HIGH)` for a single read;
a two-instant chunk needs two intervals, and the design records zero. Even with D-2 fixed, resuming a chunk
requires establishing a **new LOW/HIGH for the remainder** and re-reconciling — which is why Flink CDC re-reads
a snapshot split from scratch rather than resuming it.

So the honest capability list for `Chunkable` is "parallel backfill, resumable *slicing* (via `ChunkerCursor`),
backfill ETA". `ChunkerCursor` is genuinely excellent and genuinely solves the Benthos/Airbyte
restart-from-zero problem at the *splitter* level. Advertising mid-*chunk* resume on top of it promises
soundness the algorithm does not have, and the promise is rendered in the UI as an `Unlocks` sentence.

### D-15 (major). `WhenFullDropOldest`/`DropNewest` does not degrade the admitted delivery class

**At fault:** `WhenFull ∈ {Block, Reject, DropOldest, DropNewest}` on the `Buffer` stage, with no edge into
`admit.Requirement` or the `Delivery` computation.

`Delivery = min(sourceClass, sinkClass, requested)`. A buffer configured `drop_oldest` makes the pipeline
at-most-once *by configuration*, and the lattice does not know the buffer exists. A drop must also be treated
as terminal for the record (otherwise the prefix stalls at the dropped Seq forever), so the prefix advances
**over data that was never delivered** and the checkpoint records progress past it.

The comment "preserves the acked prefix; counted" is doubly wrong: dropping the *oldest* record in a buffer
drops the head of the unacked prefix, which is the one record whose loss the prefix ordering was protecting
against. It is counted, so not silent in metrics — but the plan document and the UI still render
`Delivery: at_least_once`, which is the silent degradation this design exists to prevent, arriving through the
one component whose config the lattice ignores.

### D-16 (major). The admission-time "dry-run Reader" consumes data from destructive sources

**At fault:** `admit.Request.ReaderCaps` — "reader tier, probed from a dry-run Reader on a probe assignment".
The proposal's own weakness #4 identifies the side effect and stops short of the consequence.

A probe assignment with an empty `Resume` means "from the connector's natural beginning". Constructing a real
Reader against a destructive-read source and throwing it away consumes and drops data: an SQS `ReceiveMessage`
in flight when the reader is closed, a Kafka consumer group whose member join + auto-commit advances the group
offset, a `LISTEN`/replication slot that consumes and discards, a TCP source that reads and closes. **Data loss
at submit time, before the pipeline exists**, in the function whose entire purpose is to refuse unsafe
pipelines.

The proposal leans toward the alternative (declare reader-tier caps in `Spec` only) and has not committed. For
this lens the answer is forced: declaring is the only safe option, and the DG-3 weakening for that tier is the
correct price.

### D-17 (minor–major). Orphaned staged artifacts and no committable expiry

**At fault:** `TwoPhaseWriter.Prepare(ctx) (Blob, error)` / `Abort(ctx, handles []Committable) error`, with no
`AbortStale`, no expiry field on `Committable`, and no deterministic handle derivation.

Two gaps:

- **Crash between `Prepare` returning and the checkpoint being durable.** The handle was never persisted.
  `Abort` can only be given handles the engine knows. The artifact leaks forever (an open transaction holding
  locks, an S3 multipart upload accruing cost, a staging table). For destinations where staged data becomes
  visible on timeout rather than being discarded, it leaks as **duplicates**.
- **Permanent commit failure.** `CommitOutcome.Status` may be `OutcomeRejected`. The prescribed engine action
  for a permanent class is DLQ — but the records are bytes inside an opaque `Blob` the engine may not decode,
  and the batch was recycled at "terminal disposition". The position was committed in the checkpoint that
  contained the handle. So the outcome is either **loss** or a permanently wedged pipeline with no operator
  path. The research demanded expiry with a loud failure (D4 item 8) and warned specifically against Flink's
  `signalFailedWithKnownReason` ("only logs the error, discards the committable and continues") — this design
  reaches the same place by omission.

Relatedly, `CommitOutcome` is keyed by `Handle Blob`. Two byte-equal handles from different writers or epochs
are indistinguishable, and matching outcomes back to `Committable`s requires a `bytes.Equal` scan. The commit
failure shape is not keyed (R7).

### D-18 (minor–major). Unexported `origin` with no mint path makes lossless record serialization impossible, which breaks buffer spill, DLQ replay and worker transport

**At fault:** `Record.origin Origin` unexported, `facets facetSet` unexported, with `Derive()` as the only
provenance-carrying constructor and no `SetOrigin`/`NewRecord`.

Unexporting `origin` is the correct structural fix for KIP-793 and I would keep it. But nothing outside package
`canal` can *write* it — including `internal/engine`, `internal/codecreg` and any `Buffer` implementation. The
design needs exactly that in four places:

- `Buffer` with spill-to-disk (§15 implies durability options) must round-trip a `*Record`.
- `DeadLetter{Record *Record, Origin Origin}` is stored durably and must be **resubmittable** — the whole point
  of a DLQ. Reconstructing the Record loses its Origin, so a replayed DLQ record cannot be accounted for by the
  ack graph or the prefix resolver.
- Worker-to-worker transport (the multi-worker end-state goal).
- A `native` codec, which D1 requires: "Serialisation must be able to encode the whole three-layer record
  losslessly … one blessed wire form used for worker-to-worker transport, buffer/WAL persistence and
  dead-letter payloads — if those three differ you get three notions of what a record is."

`canal` imports only stdlib, so it cannot host the codec; the codec cannot write `origin`. As written the
design has **no lossless record wire form**, and therefore no DLQ replay and no durable buffer. Fix is one
exported constructor in `canal` taking an `Origin` explicitly (still unreachable from a *transform*, which is
the property worth protecting).

### D-19 (minor). `Cursor` uses a nil sentinel for two different meanings

`Assignment.Resume` empty = "from the connector's natural beginning". `Origin.Cursor` empty = "the source has
no position". `AssignmentState.Resume` empty = both "never committed anything" and "committed a position that
serializes to zero bytes". `Blob.IsZero()` cannot distinguish them.

A source whose natural first position encodes to zero bytes cannot express it, and any bug that zeroes a stored
cursor silently means "start over from the beginning" — full re-delivery rather than a loud failure. The
research asked for the opposite discipline: "A record may carry a null position meaning 'not resumable' … Make
it a **typed property, not a nil convention**." The design adopted the nil convention it was warned about.

### D-20 (minor). `KindStream` values can outlive their Reader

`Stream struct{ /* lazily-read nested record stream; closed when the parent is closed */ }` is a member of the
closed `Kind` set, i.e. a legal payload value. Batches are retained until every record reaches terminal
disposition; Readers are closed when their assignment ends or is revoked. A record whose payload contains a
`Stream` and whose batch is still awaiting a retry after its Reader closed reads from a closed handle — panic,
or a silently truncated payload written to the destination. It also cannot be encoded, buffered, spilled, or
sent to another worker, so any pipeline containing one is confined to a single in-memory hop. This kind buys a
`database/sql` symmetry the data path cannot honour.

### D-21 (minor). `Payload.Bytes(ctx)` cannot do what it documents, so the batch byte cap is unenforceable

`Payload.Bytes(ctx) ([]byte, error)` promises to encode "using the pipeline's configured `Encoder`", but
`Payload` holds no encoder and `Encoder.Encode(ctx, r *Record, dst []byte)` takes a whole `*Record`, not a
`Payload`. The only ways to satisfy the doc are a `context` value or a package global, both of which make the
byte view a function of ambient state.

The correctness consequence is in `RecordBatch`: it enforces `maxBytes` and exposes `Bytes()`, while
`Payload.Size() (n int, ok bool)` returns `ok == false` for a structured payload whose size is unknown without
encoding. So `Append` either cannot enforce the byte cap for structured payloads (an R6 hole — the byte bound
exists in the type and not in effect) or must encode every record at batch time, which destroys the laziness
the dual view exists for and moves encode failures into the batcher, where there is no `RecordOutcome` channel
to report them.

### D-22 (minor). Undefined and misspelled members in the frozen contract

`AckRequest.DeadLettered []RecordRef` — `RecordRef` is defined nowhere in the document, and it matters for this
lens: the upstream ack needs a *durable* record identity, and the two candidates are `RecordID` (explicitly not
stable across restarts) and `(Assignment, Seq)` (not persisted, see D-9). `PartialTolerant.TolerantesPartial()`
is misspelled in a package described as frozen forever. Both are small; both are in the contract package, which
is the one place a typo is permanent.

### D-23 (minor). `StrategyPhased` reinstates the trap the document condemns

`Phased` gives `HasNextPhase`/`NextPhase`/`PhaseLabel` and one `Cursor` namespace spanning both phases. Since
the engine checkpoints only "at the boundary", mid-snapshot resume requires the connector to encode phase and
snapshot progress inside its opaque cursor — which is precisely what the document indicts Benthos and Airbyte
for ("two mature frameworks both encoded the snapshot phase in the opaque checkpoint because neither modelled
phases", quoted approvingly three times). Offering it as a supported tier is defensible; the plan document's
`StrategyWhy` should say what the operator is actually getting, which is "unresumable snapshot, phase state
inside the connector's cursor", not merely "source implements canal.Phased but not canal.Chunkable".

Also: `NextPhase` "advances, abandoning any unconsumed records in the current phase." Abandoning records that
the engine has already assigned Seq numbers to leaves a permanent gap in the prefix for that assignment unless
the engine is told the gap is intentional. There is no such signal.

### D-24 (minor). `Acknowledger` ordering is unspecified against `AckOnCommit`

`Acknowledger.Ack` is documented as called "after the records are durable at the sink and never before (R4)".
Under `AckOnCommit` the checkpoint (containing the advanced position and the committables) is durable *before*
`Commit` runs. Walkthrough (c) step 5 calls `Ack` right after the checkpoint. Under 2PC that is **before**
durability, so the upstream deletes/advances for data that is still staged. Combined with D-17's permanent
commit failure, the data is gone from both ends. One sentence fixes it — `Ack` fires after `Commit` returns
success for every handle covering the acked prefix — and that sentence requires D-3's coverage information to
even be expressible.

---

## Part 3 — What this proposal does better than any plausible alternative

I am hostile, not dishonest. On my own lens this design contains several things I would fight to keep, and some
I have not seen stated this well anywhere, including in the research dossiers.

1. **No clock anywhere in the ack path, stated as an invariant with the counter-example attached.**
   `AssignmentState.Resume`'s comment — the position advances "when and only when the records before it are
   durable at the sink", with KAFKA-4942 named as what happens otherwise — is the single most important
   correctness sentence available, and this proposal makes it structural rather than aspirational. Every
   alternative that reaches for a flush interval is worse, and this one closes the door.

2. **`Error.RetrySafe` with the strict predicate.** *"a connector may report `RetrySafe` only when it KNOWS the
   effect did not land. Even if the destination returned an error, if there is any possibility the operation was
   performed, `RetrySafe` must be false."* That is the correct treatment of the sink-timed-out-after-succeeding
   case, it is the `ErrBadConn` discipline correctly generalized, and it is the one clause in the document that
   directly addresses "sinks time out after having actually succeeded". Transplant verbatim, including the
   `canaltest` assertion.

3. **`WriteResult` with six ID-keyed outcomes, `OutcomeDuplicate` as success, in the *required* signature.**
   This is R7 satisfied in the type system on day one, including the partial-acceptance case that KIP-731 has
   not solved since 2021. Keying by `RecordID` rather than position is the right fix for the Benthos
   `WalkMessages` scar. No alternative shape I can construct improves on it.

4. **`AckPoint` reified, with the strict reading as the default and weakening requiring a visible interface.**
   The direction is exactly right: absence of `Flusher` means the strongest promise. The defect (D-5) is that
   `Durable` reopens the door; the mechanism itself is the best available answer to R4.

5. **`Replayable.ReplayWindow()` cross-checked against checkpoint age, producing a refusal.** *"the source
   guarantees 6h of replay; this checkpoint is 9h old"* — refusing to start rather than starting a stream that
   will silently skip records is a correctness idea I have not seen in any surveyed system, and it is worth
   having even at the cost of D-11's missing heartbeat (which is the fix, not a reason to drop it).

6. **`AssignmentState.ChunkerCursor` as a first-class field.** Resumable *slicing* solves the
   restart-a-500M-row-snapshot-from-zero problem that cost Airbyte a protocol change and that Benthos still
   has. Cheap, sound, and independent of every other defect here.

7. **Delivery as `min(source, sink, requested)` with `requested > computed` a submit-time refusal, and
   `Downgrade` as an operator-signed durable record the engine can never mint itself.** The honest-tier problem
   is normally solved by prose; solving it with a lattice plus a refusal plus a persisted waiver that raises
   `Degraded` for the pipeline's life is strictly better than every alternative, and it is the correct home for
   the clock-skew, drop-policy and idempotency edges that D-12/D-13/D-15 currently bypass.

8. **`Assignment.Bounded` + `AssignmentState.Done` as data, with `Phase` reporting-only and a CI grep
   asserting the engine never switches on it.** Snapshot-then-stream as "the assignment set changes over
   time" removes the phase-enum control flow that two mature frameworks encoded into their opaque checkpoints.
   This is the right mechanism; D-2 is a missing field inside it, not a reason to abandon it.

---

## Part 4 — Design-rule scorecard on this lens

| Rule | Verdict |
|---|---|
| **R4** (an acknowledgement means durable) | **Failed structurally.** The intent is the best in the field (`AckPoint` reified, strict default). Three independent paths commit progress for non-durable data: `WriteResult.Durable` unclamped (D-5), the batch-end cursor being the only cursor available (D-1), and the DLQ sink's unchecked `AckPoint` (D-9). Two of the three are unreachable by a careful connector author — i.e. they are the framework's fault, which is the worse category. |
| **R5** (dedupe keys scoped, committed after the write) | **Partially failed.** DLQ writes precede terminal disposition — correct order, credit given. But the only dedupe key offered (`Origin.Assignment + Origin.Seq`) is not persisted and therefore collides across restarts (D-9), which is R5's bare-`id` bug in a new costume. Canal keeps no dedupe state of its own, which is defensible; selling `EffectivelyOnce` on an unverifiable sink bool (D-12) is not. |
| **R6** (bounded by construction, rejection expressible) | **Mostly satisfied, two holes.** `RecordBatch.Append` returning `ok` and `Buffer.Push` returning a disposition are exactly right, and drops are always counted. Holes: the byte cap is unenforceable without encoding (D-21); `Checkpoint.Committables`, `Checkpoint.Assignments` and the engine's retain-until-durable set under `Flusher`/2PC are unbounded in the type (`Set` takes the whole map atomically). |
| **R7** (name the failure shape with the success shape) | **Satisfied for `Write`, failed at three other boundaries.** `WriteResult` is the best answer available. `FlushResult` cannot express a partial-durability prefix (D-6). `CommitOutcome` is keyed by an unkeyable `Blob` (D-17). `Transform.Apply`'s outcomes are keyed by input `RecordID` with derived records' IDs unaddressed. |

Documented traps from `_decision-space.md` that this proposal walks into anyway:

- **D7's hard requirement** "chunked-snapshot output is keyed upserts … checked when the pipeline is submitted"
  — not in the `Requirement` lattice (D-13).
- **D7 step 4's tuple** `{keyRange, highWatermark}`, called "the entire snapshot→stream contract" — only the
  range is representable (D-2).
- **D3's "safe resume point distinct from record position"** — absent (D-10).
- **D3's "typed property, not a nil convention"** for a null position — nil convention adopted (D-19).
- **D4 item 7's `Snapshot(ctx, id)` on every stage** so a barrier protocol is insertable later — no stage has
  it; `Prepare` takes no epoch.
- **D4 item 8's committable expiry with loud failure** — absent (D-17).
- **D6 item 10's heartbeat** — absent (D-11).
- **D9's warning that a capability flag without methods is worthless** — `Idempotent`, `KeyRequirer`,
  `PartialTolerant`, `StructuredWriter` are flags, and one of them carries a delivery tier (D-12).
- **D1's requirement of a lossless record wire form** shared by transport, WAL and DLQ — impossible with
  unexported `origin` and no mint path (D-18).

---

## Part 5 — What would move this to a passing score

None of the fatal defects require abandoning the capability architecture. In dependency order:

1. **A per-record position slot on `Record`, writable by a Reader** (D-1), with the clamp rule stated: a record
   with no cursor inherits the nearest *preceding* cursor, never the following one. Everything else in the
   correctness story is already correct once this exists.
2. **A watermark on the finished-chunk record and a method to read a stream's current position**, so
   `{keyRange, highWatermark}` is representable and the `position > HIGH` filter (plus its self-retirement)
   can be implemented (D-2). Until then `StrategyChunked` must be refused, not shipped.
3. **`Committable` carries a coverage set**, not one `AssignmentID` (D-3), plus an expiry and an
   abort-orphans path (D-17).
4. **A fencing token in `CheckpointStore.Set`, and per-assignment CAS rather than a whole-map epoch** (D-4).
5. **Clamp `Durable` against `AckPoint` in the engine** (D-5) and give `FlushResult` a per-assignment durable
   prefix (D-6).
6. **Move schema change into the ordered record stream** (D-7); leave `Signal` for progress and degradation.
7. **Define `Read`'s error semantics** in the doc comment, with the count-then-error convention made explicit
   (D-8).
8. **Persist a Seq high-water mark per assignment** (D-9) and add a `SafeResume` position (D-10) and a
   heartbeat/idle-advance path (D-11).
9. **Extend the `Requirement` lattice** to cover the sink-side upsert requirement for chunked plans (D-13),
   the buffer's drop policy (D-15), and the DLQ sink's `AckPoint` (D-9).
10. **Declare reader-tier capabilities in `Spec`**; delete the dry-run Reader probe (D-16).

With 1–5 done, the design's claimed at-least-once is honest and its exactly-once is reachable. With 6–10 done I
would score it 8. As written it is a 4: an excellent skeleton whose flagship guarantees cannot be implemented
from the interfaces given, and whose most ordinary configuration loses data.

# Review: `protocol-first, in-process today`

**Lens:** correctness of data-movement semantics.
**Reviewer stance:** hostile expert. Adversarial world assumed: `kill -9` mid-batch, partitions, sinks that
time out after having actually succeeded, sources that reconnect, skewed clocks, late and out-of-order
records.
**Score: 4 / 10.**

---

## 1. Verdict

The **commit spine of this proposal is the best of the three angles' shapes and I want it kept**: durable
progress advances only when a per-split contiguous-prefix resolver in core observes sink durability for
every record preceding an in-band `Mark`, and there is *no other path to a checkpoint* — no
`offset.flush.interval.ms`, no wall clock, no connector-held commit callback. That single decision kills
the whole Kafka Connect failure family (KAFKA-4942, fully-acked-chunk re-emission, three overlapping
commit hooks) and it is stated crisply enough to build.

Everything above that spine is where it fails. The design **infers durability from a `nil` error return at
three separate edges and constrains none of them**; it **writes one pipeline-scoped checkpoint aggregate
from every worker through a store call that carries no fencing token and no CAS**; it makes record
identity **session-scoped** while records outlive sessions; and the two features it advertises most
loudly — the core-owned chunked-snapshot engine and the two-phase commit tier — **cannot be executed by
the interfaces as written**, not because a connector author might get them wrong but because the
operations they need are absent from the type set.

Net: at-least-once is honest for a **single worker, single split, no reconnect, no fan-out, no buffer
spill, no DLQ** pipeline whose sink is fastidious. Every axis canal's end-state goals add — a second
worker, a second split, a fan-out branch, a chunked backfill, a 2PC sink — reintroduces a *silent* loss or
staleness path. Since R4 is the rule this project was founded on (the predecessor's most dangerous gap was
exactly "the documented advance rule and the actual durability of the thing being acknowledged were
unrelated"), inverting the default so that **silence means durable** is not a detail. It is the same
mistake with better prose around it.

Score reasoning: the spine and the per-split resolver are worth 6–7 on their own; the R4 inversion,
the unfenced multi-writer checkpoint, the ID collision and the two unimplementable tiers each remove a
point. Most of the fatal findings have local repairs that do not change the architecture (add an epoch to
`RecordID`, add a lease/expected-revision to `Set`, add a `Resolver.Deferred`, key committables by
`MarkID`, add a key-range predicate). Two do not: **ack composition under fan-out and fan-in**, and the
**cross-split placement of the chunk/stream merge**. Those are model errors, not omissions. 4.

---

## 2. What is actually guaranteed after a crash, point by point

Traced against the interfaces as written, for a tier-1 (`Sink` only) pipeline.

| Crash point | Claimed | Actually guaranteed |
|---|---|---|
| After `Batch.Record` fills a slot, before `Fetch` returns | nothing durable | correct — nothing committed |
| After `Fetch` returns nil, before sink `Write` | replay from last durable mark | correct **only if** the connector's own read position did not advance irrecoverably; nothing in the contract forbids that |
| `Fetch` returns **non-nil** with 200 frames already appended | unspecified | undefined. If the engine discards the batch and the source consumed from a socket/queue, those 200 are gone with progress never committed → loss. See D11. |
| Sink `Write` returned nil, no `res` entries | every record durable | only that the sink *said nothing*. A sink that returns early, or buffers, or forgets a record, claims durability by omission. See D1. |
| Sink actually succeeded but the call timed out | at-least-once replay | correct, and `WriteResult.Duplicate` is the honest re-report path — this part is good |
| After `Resolver.Committed()`, before `CheckpointStore.Set` | replay | correct |
| `Set` succeeded on a worker whose lease had already expired | fenced out | **not fenced.** `Set` takes no lease and no expected revision. Cursor regression or advance. See D2. |
| Sink returned `Deferred`, checkpoint durable, crash before `Commit` | `Commit` replayed from the checkpoint | `Committable` is never defined and `Checkpoint.Committables` is a flat list with no `MarkID`; "publish everything up to and including m" is not expressible. See D7. |
| Crash during a chunked backfill | resume mid-chunk, no re-read of 250M rows | resume works (this is genuinely good); but the chunk↔stream merge that makes the resumed data *correct* has no home. See D5, D6. |
| Reconnect (`ClassNotConnected`) with records still in flight downstream | records unaffected, session re-established | `RecordID.Seq` restarts and `SplitRef` rebinds → the resolver credits a new record's ack against an old record's slot. See D3. |
| Two sinks in a fan-out, one crashed | ack when all branches ack | `Resolver.Written(id, bytes)` has no branch dimension; first ack advances the prefix. See D4. |

The honest one-line summary the design does not print anywhere: **"at-least-once, provided every stage
that returns `nil` really was durable, and provided nothing in this pipeline is duplicated, revoked,
reconnected, chunked, buffered, dead-lettered or run on a second worker."**

---

## 3. Defects

### D1 — FATAL. Durability is inferred from a `nil` error return at three edges, and constrained at none. R4 inverted.

**At fault:** `Sink.Write(ctx, *Batch, *WriteResult) error` with "silence means Written" (§5.2), plus
`DeadLetter.Send(ctx, *Record, *Fault) error` (§8), plus `Buffer.Push(ctx, *Batch) (accepted int, err error)`
(§9).

The proposal's central R4 claim is: *"there is no other way for a sink to say 'accepted', so a sink cannot
accidentally acknowledge a buffer."* This is false in the strongest possible sense — the design chose the
encoding in which **accidental acknowledgement is the cheapest thing a sink can do**, because it is what
happens when the sink does nothing at all.

Its own flagship example proves it. `stdoutSink.Write` (§12a, lines 2149-2158):

```go
for _, f := range b.Frames() {
    if f.Kind != proto.KindRecord { continue }
    if _, err := s.w.Write(f.Record.Value.Raw()); err != nil {
        res.Failed(f.Record.ID(), ...)
        return nil                       // "per-record failure reported, not a batch failure"
    }
}
return s.w.Flush()
```

Two independent losses in eleven lines:

1. **Early return.** Records `k+1 … n` are never written and never named in `res`. Silence means Written.
   The engine credits them. Record `k` is `ClassTransientInternal → RetainAndBackoff`, so the prefix
   freezes at `k`; retry re-sends *only the failed subset* — `{k}` — and on success the prefix sweeps
   straight over `k+1 … n`. `n-k` records are gone with the cursor committed past them.
2. **Unflushed `bufio`.** On the error path `Flush` is never called, so records `0 … k-1` were also only
   ever in a userspace buffer, and they too were reported durable by silence.

This is R4's predecessor catastrophe reproduced exactly: a success the sender is told to checkpoint on,
emitted after appending to an unsynced in-memory buffer. The difference is that in the predecessor it took
an RFC to instruct the adapter to do it; here the *reference implementation in the proposal* does it.

The same encoding is applied at two more edges with even less specification. `DeadLetter.Send` returning
nil is credited as durable (`Resolver.Failed(..., DeadLetter)` "counts as durable for prefix purposes once
the DLQ write is itself durable") — but nothing in the `DeadLetter` interface says when that is, and if the
DLQ is an ordinary canal sink it has its own marks, its own resolver and its own multipart buffering.
Concrete: DLQ is an S3 sink accumulating a 5 MiB part; `Send` returns nil; prefix advances; `Set` commits;
`kill -9`; the part was never uploaded. Record lost, cursor past it, `canal_records_dropped_total`
incremented as if handled. And `Buffer` has **no durability class at all** — nothing declares whether a
`FullOverflow` disk buffer is a durability boundary, so the engine either credits on `Push` (acknowledging
a non-durable buffer — the exact predecessor bug) or credits only at the sink (in which case the disk tier
buys nothing), and the proposal never chooses.

**Why this is not merely an example bug.** The encoding is also **not wire-safe**, which contradicts the
proposal's own thesis. In-process, a crashed connector cannot return nil. Over the gRPC binding the sink's
outcome stream is `KindOutcome` frames, and a **truncated outcome stream is indistinguishable from "no
exceptions to report"**: the connection drops after the sink wrote 40 of 500 records and the engine, seeing
no `Failed` entries, marks 500 durable. The whole bet of this proposal is that the binding does not change
semantics. Here the default-success encoding is safe only in the binding that cannot fail. Any fix — an
explicit end-of-outcomes marker plus a status — is a fix to the protocol, not to the transport.

**Repair:** flip the polarity. `res.Written(id)` explicit, or a `res.WrittenThrough(idx)` prefix marker for
the common bulk case, and treat an unmentioned record as **not durable** — Vector's property that
"forgetting to settle produces *do not checkpoint*", which the decision space names as the best
effort/benefit ratio in the entire study and which this proposal discarded to save an allocation.

---

### D2 — FATAL. `CheckpointStore.Set` carries no fencing token and no CAS, and one pipeline-scoped checkpoint aggregate is written by every worker.

**At fault:** `CheckpointStore.Set(ctx, kv map[string][]byte) error` (§10), against `Checkpoint`
(pipeline-scoped: `Header`, `Shared *Blob`, `Splits []SplitState`, `Enumerator Blob`) written by workers,
plus `ConfigStore.Put(ctx, spec, expect Revision)` which *does* have the CAS the checkpoint store lacks.

The proposal asserts (§10, §13) *"the lease is the fencing token — not leadership, because the verified
Kubernetes caveat is that leader election does not guarantee fencing. A worker whose lease lapses stops
reading before another can claim it."* That is a **timeout argument**, and the decision space calls it out
by name: clients infer leadership from locally captured timestamps, so leadership must never be trusted for
correctness and *"every checkpoint write carries the lease/generation and the store rejects a stale one"*.
The lease exists on `Coordinator`. It does not appear on the only write path that matters.

**Interleaving.** Worker A owns split S, lease TTL 30 s. A is stop-the-world paused (GC, CPU steal, a paused
container) for 45 s. At t=35 s the planner reassigns S; worker B claims it, resumes from durable cursor 100,
reads to 200, and commits. At t=45 s A wakes *inside* `Sink.Write`'s continuation, its resolver reports mark
m durable at cursor 150, and it calls `Set`. The store accepts it. Two outcomes, both bad:

- The durable cursor for S regresses 200 → 150 → records 150–200 are re-read and re-delivered. Tolerable
  under at-least-once, but not under the `Committer`/`TokenSink` tiers this design claims.
- Because `Checkpoint` is **one aggregate for the whole pipeline**, A's write also stamps its stale
  `Splits[]` view over splits T and U that A does not own, its stale `Header.MarkID` and
  `RecordsCommitted`, and — worst — `Shared *Blob`, the single global CDC log position. If A's view of any
  of those is *ahead* of the truth (A had read further on a split it lost, or `Shared` was advanced by A
  before the pause), the committed position **jumps past data never written** → silent loss at scale, not
  duplicates.

There is no ownership rule for `Shared` anywhere in the proposal. A single global log position in a
multi-worker design is a shared mutable cell with concurrent writers and no protocol.

Compounding: **`Set` and `Delete` are separate calls**, so no operation that must atomically add state and
retire state is atomic. That breaks three things the design depends on: trimming the dedupe window
(D12 below), retiring committables after `Commit`, and rotating the paged finished-chunk set. Concretely,
`Enumerator` blob holds `finishedChunksRef: "ck/7/finished/0000"` and `finishedCount: 31`; a crash between
the `Set` that writes page 0001 and the `Delete` that retires 0000 leaves an orphan (harmless); a crash the
other way leaves `finishedCount: 31` pointing at a **deleted page** — and if the completeness predicate
trusts the count, the backfill is declared complete with 31 chunks' worth of rows never emitted. Silent
loss of 31 million rows, reported as a successful snapshot. The decision space demanded
`filterOutdatedSplitInfos` as a pure reconciliation function precisely for this; the proposal mentions
paging and specifies no page lifecycle.

**Repair:** `Set(ctx, kv map[string][]byte, del [][]byte, fence Fence) error` where `Fence` is
`(lease, generation)` or an expected revision, rejected server-side. Shard the checkpoint by owner so no
two writers touch one key; give `Shared` a declared single owner.

---

### D3 — FATAL. `RecordID` and `SplitRef` are session-scoped, and records outlive sessions.

**At fault:** `RecordID{Split SplitRef; Seq uint64}` with `Seq` documented "monotonic within
(Split, **session**)" (§2.2); `SplitRef` "session-scoped … valid until the session ends"; `ClassNotConnected`
= "no session; reconnect, **do not retry the record**" (§8). `Origin` carries the durable `SplitID` but the
resolver and `WriteResult` are keyed by `RecordID`.

`ClassNotConnected` explicitly means the *connection* failed and the *records* did not. So records admitted
in session A are still in the buffer and at the sink when session B begins. Session B rebinds the split
table and restarts `Seq` at 0.

**Interleaving.** Session A emits `{Split: 1, Seq: 0..255}` for split S; the batch is in the sink's
in-flight window. The source's TCP connection drops; the engine reconnects; session B binds `SplitRef(1)`
(possibly to a *different* `SplitID`, since nothing forbids ref reuse after `RemoveSplits`) and emits
`{Split: 1, Seq: 0..}`. The sink's second `Write` returns nil for session-B records. `Resolver.Written({1,0})`
credits **session A's** record 0, which never landed. If A's record 0 was the hole holding the prefix back,
the prefix now sweeps past it and `Set` commits a cursor over undelivered data → loss. If ref 1 was rebound
to a different split, the resolver writes **split B's cursor into split A's `SplitState`** — a resume point
from an unrelated position, which for a key-range chunk cursor means an arbitrary slice of the object is
never read.

This is not connector-author error; the engine owns `Batch.Record`'s stamping and the split table.

**Repair:** put an epoch in the identity — `RecordID{Split SplitRef; Epoch uint32; Seq uint64}` — or forbid
ref reuse and drain all in-flight records before rebinding, and say so in the contract. Note the proposal
already pays 12 bytes per record for `RecordID`; 16 is not the problem.

---

### D4 — FATAL. Fan-out and N→1 have no ack composition. The prefix either advances early or never advances.

**At fault:** `Resolver.Written(id RecordID, bytes int)` — one call, one id, no branch identity and no
reference count; `Batch.Derive(in *Record) *Record` — exactly one input; `Origin.Parent RecordID` — exactly
one parent.

Fan-out and fan-in are both in canal's stated end-state goals, and §9/§13 claim they are free via
`Field.Component` recursive composition: *"fan-out, fan-in, routing, fallback, retry wrappers and DLQ cost
the core nothing."* They cost the resolver everything.

**Fan-out.** One record, two sink branches. Branch A returns nil (`Written`), branch B times out. The engine
calls `Resolver.Written(id, n)` for branch A's completion — there is no way to call it *for a branch*, and
no way to say "one of two settled". The prefix advances; `Set` commits; `kill -9`; on restart the record is
not replayed and **branch B never receives it**. This is a checkpoint committed for data not durably in a
sink, i.e. the exact question this lens exists to ask, answered wrongly. The decision space's requirement is
one sentence: *"fan-out is 'ack when all branches ack'"*. Implementing it needs a refcount or a settle-set
per `RecordID`; neither exists.

**Fan-in / N→1.** §9's `Transform` comment and §13's table both claim *"N-to-1 is Derive once from many"*.
The signature is `Derive(in *Record) *Record`. There is **no way to name the other N−1 inputs**, so they are
neither `Written`, `Failed`, nor `Drop`ped — they stay in-flight forever and the per-split prefix **never
advances again**. A windowed aggregation permanently stalls its own checkpointing. The alternative
implementation (engine auto-drops the un-named inputs) is silent loss. Either way, an advertised topology is
broken by the ack model.

1→N is fine (`Origin.Parent` gives N children one parent, which is a genuinely nice replacement for
Benthos's ack-counting scanner). It is the many-to-one direction, in both the topology and the transform,
that has no representation.

**Repair:** make settlement a set operation — `Resolver.Settle(ids []RecordID, branch BranchID, outcome)`
with a declared branch count per pipeline edge, and `Derive(ins []*Record) *Record` (or an explicit
`Batch.Consume(ids...)`).

---

### D5 — FATAL. The core's chunk range filter needs a key-in-range predicate that no interface provides.

**At fault:** `Split.Attrs Blob` — *"connector-opaque split detail (a key range, a file offset, a shard id).
The engine stores and ships it and **never reads it**"* (§4); `Split.Start/End Cursor` (opaque);
`CursorComparer.CompareCursor(a, b proto.Cursor) (int, error)`; `Record.Key Payload`.

The chunked-snapshot engine's per-record filter is, verbatim from the walkthrough: *"any stream record in
`(LOW_c, HIGH_c]` **whose key falls inside the chunk's key range**"*, evaluated by *"a sorted key-range
interval tree, binary searched"* in `engine/enumloop.go`.

For the core to evaluate that predicate it needs, for a stream record, a comparison between the record's
**key** and the chunk's **key bounds**. What it has:

- the chunk's bounds either in `Attrs`, which it is forbidden to read, or in `Start`/`End` as opaque
  `Cursor` blobs;
- `CompareCursor(a, b)`, which orders two **cursors**;
- the record's key as `Record.Key Payload` — bytes or structured — with **no function mapping a key into
  cursor space**, and `Origin.Cursor` holding the record's *log position*, not its key.

So the predicate is inexpressible. The only ways to make it evaluate are: (a) the engine byte-compares
`Record.Key`'s encoded form against `Cursor.Bytes` lexically, which is a core assumption about the internal
encoding of two independently-opaque blobs and therefore both incorrect in general and a violation of
constraint #1; or (b) `CompareCursor` is passed a synthesised cursor the engine cannot construct. Note also
that `CompareCursor` takes no split or kind argument, so the engine cannot even tell whether it is comparing
two chunk key-cursors or a key-cursor against an LSN-cursor — and it will do both, since `Plan.Lag` also
routes through the same comparator.

Consequence: the largest piece of machinery in `engine/` — the one the proposal calls its biggest complexity
risk — cannot run. And it is gated on a capability *conjunction* the source can legitimately declare
(`CapChunk ∧ CapReplay ∧ CapCompareCursor`), so `Negotiate` will happily set `Plan.Chunked = true` for a
pipeline whose filter is a no-op or a coin flip. A no-op filter means every change in `(LOW_c, HIGH_c]`
is emitted *and* the snapshot row is emitted, in unspecified order across splits → last-writer-wins
staleness on an upsert sink.

**Repair:** the source must own the predicate. `KeyInSplit(s Split, key proto.Payload) (bool, error)` as a
declared capability alongside `Chunker`, or a typed `KeyRange` in `Split` with a connector-supplied
comparator over keys rather than cursors. Flink CDC does this inside the dialect for exactly this reason.

---

### D6 — FATAL. The per-chunk backfill merge has no home; filter-only semantics silently lose updates.

**At fault:** the split model (`Split` per chunk, one unbounded stream split, "ordered within a split,
**unordered across splits**", pull-based per-worker assignment) against the watermark algorithm §12b claims
to run; `Replayer.CanReplay(s proto.Split) bool`.

Flink CDC's algorithm — the one verified source this design rests on — is correct because the per-chunk log
replay happens **inside the chunk's own reader**: `createBackfillStreamSplit(low, high)` is a bounded
stream split, the chunk is buffered, the changes in `(LOW_c, HIGH_c]` are **upserted into that buffer**, and
only then is the merged result emitted. The stream-phase filter (`key ∈ range ∧ pos > HIGH`) is safe *only
because* step 2 already folded everything at or below `HIGH` into the emitted chunk.

This proposal deletes step 2 and keeps step 5:

- There is **one** unbounded stream split, emitted at `Start = LOW` and read *"the whole time"*, on a
  different split from every chunk and — in cluster mode, with 20 workers on chunks and 1 on the stream —
  on a **different worker**. A cross-worker merge of the stream's records into another worker's chunk buffer
  is not expressible: there is no frame, no shuffle, and no shared state.
- `Replayer` is declared as the capability that licenses the replay step and its entire surface is
  `CanReplay(s) bool` — a **predicate**. There is no operation to read changes in a bounded position range.
  The one thing the algorithm needs from the connector, the interface does not ask for.
- The walkthrough's language slides between the two steps: *"replaces the snapshot row"* (step 2's upsert)
  and *"the filter for chunk c self-retires"* (step 5's filter). Only the filter is implementable here.

**Interleaving under filter-only semantics.** Chunk `c` covers keys 1–1000. `LOW_c = LSN 100`. The chunk's
`SELECT` reads row `k=500` at LSN 105, value `v0`. At LSN 120 an `UPDATE` sets `k=500` to `v1`. The chunk
finishes; `HIGH_c = LSN 140`. The stream reader delivers the LSN-120 update; the filter sees
`120 ≤ HIGH_c` and **drops it**. The snapshot row for `k=500` is emitted as `v0`. The sink holds `v0`
forever. This is a **permanent silent lost update inside the flagship feature**, and it is invisible: no
fault, no DLQ entry, no metric, no reconciliation (the `Mark`/`Stats` counts match — one row emitted, one
row persisted).

Even if the merge were somehow located in core, the design contradicts itself: merging requires holding an
entire chunk (`ChunkTarget{Records: 1e6}` in the walkthrough) while consuming another split's live stream,
inside a pipeline whose stated property is *"capacity-2 batch hand-off"* and "bounded by construction".
You cannot hold 10^6 records in a two-batch pipeline.

Separately, the proposal never validates the precondition the decision space marks as a **submit-time
requirement**: *"chunked-snapshot output is keyed upserts, not appends."* `Negotiate` gates `Plan.Chunked`
on three **source** capabilities only. A chunked backfill into `SinkMode = SinkAppend`, or into a sink for a
stream with no `KeyFields` and records with empty `Record.Key`, is accepted at submit time and produces
duplicate-plus-stale rows presented as at-least-once. Documented trap, walked into.

**Repair:** adopt Flink CDC's actual shape — a bounded stream split per chunk, read by the chunk's own
reader, with the merge inside the connector's buffer or inside a core stage that owns *both* splits on one
worker. Give `Replayer` a range-read operation. Refuse `Chunked ∧ ¬(SinkUpsert ∧ key present)`.

---

### D7 — FATAL. The two-phase tier has no resolver state, no defined committable, and no mark association.

**At fault:** `WriteResult.Deferred(id)`; `Checkpoint.Committables []Committable` (flat, unkeyed);
`Committer.Commit(ctx, cs []proto.Committable) ([]proto.CommitOutcome, error)`;
`Committer.Abandon(ctx, cs)`. **`proto.Committable` and `proto.CommitOutcome` are never defined anywhere in
the document** — grep confirms zero definitions.

Three independent breaks:

1. **No resolver transition.** `Resolver` exposes `Written`, `Failed`, `Committed`, `Oldest`. A `Deferred`
   record is neither written nor failed, so the prefix cannot advance, so the checkpoint containing the
   committables is never written, so `Commit` — which the design says happens *"only after the checkpoint
   containing the committables is durable"* — is never called. **The exactly-once tier deadlocks on its
   first batch.** The alternative (the engine silently treats `Deferred` as prefix-advancing) is legal under
   the Flink pattern but is nowhere stated, and it is the difference between exactly-once and a checkpoint
   committed for data sitting in an uncommitted transaction.
2. **No mark association.** The Checkpoint Subsuming Contract, which the proposal adopts *verbatim* and
   quotes three times, is "on confirm publish everything **up to and including** that id". That requires the
   decision space's `map[MarkID][]Committable` pending set. A flat `[]Committable` cannot answer "which
   belong to m?" after recovery, so neither selective publish nor `Abandon` (whose signature also has no
   id) is implementable. On restart you re-commit everything or nothing.
3. **No already-committed outcome and no expiry.** The single most important recovery case for 2PC is *the
   transaction actually committed and the confirmation was lost* — Flink's `signalAlreadyCommitted`. The
   proposal deliberately drops it ("four outcomes, not Flink's six") on the argument that retryability lives
   in `Fault.Class`; but "already committed" is not a retryability property, it is an idempotence fact, and
   `ClassDuplicate` is a *record*-level outcome with no path through `Commit`. Likewise the decision space's
   explicit instruction — *"give committables an expiry and fail loudly when one expires"* — is absent, so a
   committable whose upstream transaction has timed out fails in whatever way the sink's error happens to
   classify, and `RetainAndBackoff` on a commit failure is meaningless: by then the records are out of the
   resolver and the checkpoint that says they are durable is on disk. **A permanently failing `Commit` after
   a durable checkpoint is unrecoverable data loss with no designed escalation.**

`ExactlyOnce` is also computed from a **sink-only** capability. Nothing requires the source to be
deterministically replayable or the transform chain to be deterministic, and `Guarantee` includes
`EffectivelyOnce`, a fourth value that appears exactly once in the document and is never defined. The
headline honesty mechanism (`Plan.Guarantee` + `Why`) therefore prints a tier the interfaces cannot
deliver — which is R4's failure mode relocated to the API.

---

### D8 — FATAL. `MarkID` is pipeline-global and `MarkConfirmer` asserts global subsumption over per-split marks.

**At fault:** `MarkID` — *"monotonically increasing checkpoint identifier, scoped to (pipeline,
generation)"*; `Mark{ID MarkID; Split SplitRef; …}` — per split; `Batch.Mark(split, cursor)` — no id, so the
engine assigns; `Resolver.Committed() (marks []MarkID, splits []SplitState)`;
`MarkConfirmer.MarkConfirmed(ctx, m)` — *"mark ids strictly increase; a confirm of m subsumes every
m' < m"*.

Flink's subsuming contract is safe because a Flink checkpoint is a **global barrier covering every split at
once**. Here marks are per-split, emitted independently by readers at boundaries of their own choosing, and
resolved by **independent per-split prefixes**. The two are imported without reconciliation.

**Interleaving.** Splits A and B on one reader. Mark 41 = A's records 0–99. Mark 42 = B's records 0–99.
B's batch returns nil; A's batch has a hole at record 50 (`TransientUpstream`, retrying).
`Resolver.Committed()` now faces a choice the proposal never makes:

- Return `[42]`. `MarkConfirmed(42)` is called; by the contract the *connector* is entitled to treat every
  `m' < 42` as durable — so it advances the replication slot / deletes the SQS messages / commits the
  consumer group past **A's record 50, which is not durable**. `kill -9`. Record 50 is unrecoverable
  upstream. **Data loss, caused by the design's own quoted contract.**
- Withhold 42 until 41 resolves. Then one stalled split head-of-line-blocks every other split's upstream
  release — reintroducing exactly the coupling the per-split resolver exists to remove, pinning replication
  slots and WAL, which is the tension the decision space explicitly says canal must document (D12 item 13).

In cluster mode it is worse: `MarkID` must be globally monotonic across workers and **no interface allocates
it**. `Coordinator` has no sequence. Two workers independently assigning ids collide, so worker A's
`MarkConfirmed(918)` subsumes worker B's mark 900 covering data B has not written.

**Repair:** either make marks global (an engine-driven quiesce-and-mark across all splits, which §13 says
canal can afford: *"a single-process Go tool can briefly stop admitting records while it snapshots"*), or
make the confirm per-split — `MarkConfirmed(split SplitID, m MarkID)` — and delete the global subsumption
sentence. The current text is the worst of both: per-split mechanics with a global promise.

---

### D9 — FATAL. `Headwatcher.Head` reserves nothing, so the snapshot→stream gap can be silently lost.

**At fault:** `Headwatcher.Head(ctx, s proto.StreamID) (proto.Cursor, time.Time, error)`; the claim in §12b
that *"the core owns this ordering — it is the handoff invariant"*; `Enumerator.ReaderReady` (pull-based
assignment).

`Head()` is a **query**. It returns where the log currently is. It does not create a replication slot, open a
cursor, take a snapshot handle, or otherwise make position `LOW` *readable later*. The proposal's handoff
invariant is "call `Head()` before any backfill read", which is necessary and nowhere near sufficient.

**Interleaving.** Planner calls `Head()` at t=0 → `LSN 100`. It emits the unbounded stream split with
`Start = LSN 100` and 64 chunk splits. Assignment is **pull-based**: 20 workers ask for work and take chunk
splits; no worker has spare capacity for the stream split for five minutes. At t=5 min a worker opens the
source's `Reader` with `Start = LSN 100` and the connector *now* creates its replication slot — which, for
every real log-based source, starts at the **current** position, not a historical one. LSN 100 → 5000 is
unrecoverable and unread.

Now the loss. Row `R` is in chunk 7, snapshotted at t=1 min. `R` is deleted at LSN 150 (t=2 min). The delete
falls in the lost gap. `R` remains in the destination forever, with no fault, no DLQ record, and a clean
checkpoint. The same construction loses every insert/update in the gap for keys outside already-snapshotted
chunks.

The generic-source requirement is that **the stream read position is reserved before the first backfill row
is read**, atomically with respect to the snapshot — which is what Benthos's MySQL input hand-rolls
(`FLUSH TABLES WITH READ LOCK` → consistent snapshot → read coords → unlock) and what Flink CDC gets by
reading `displayCurrentOffset` on an already-established connection. There is no interface here for
"reserve and hold a stream position", and worse, the reservation would have to be taken by the **planner
process** and honoured by a **different worker process** minutes later. Nothing in the design carries a
reservation across that boundary.

A second, smaller instance of the same class: `Head()` is called by the **core** while the chunk's `SELECT`
runs inside the **connector's** `Fetch`. The algorithm requires `LOW_c` to be captured strictly before any
row of the chunk is read. Nothing enforces that ordering across the boundary — a connector that prefetches,
or a reader already mid-chunk when the core evaluates `Head()`, silently violates it, and the resulting lost
updates look identical to D6's.

**Repair:** a `StreamAnchor`-style capability that *reserves* a resumable position and returns a token the
enumerator persists in checkpointed state before emitting any chunk, with an explicit release; and require
the anchor to be taken by whichever component will read the stream, or made durable so any worker can adopt
it.

---

### D10 — MAJOR. `TokenSink` cannot be implemented, and would create two durability domains if it could.

**At fault:**

```go
type TokenSink interface {
	LoadToken(ctx context.Context) (proto.Blob, error)
	// The token to store with the next Write's transaction is passed via
	// WriteResult's inbound side: proto.Batch carries it on the Mark frame.
}
```

The second half of the mechanism is a comment where a method should be, and it is **false**: `Mark` is
`{ID, Split, Cursor, Records, Bytes, Phase, At}` — no token field. The sink has no way to learn what to
store, so the tier's defining operation does not exist.

If it existed, `Blob{Version, Bytes}` still cannot carry a canal checkpoint: `Header` + `Shared` +
`Splits[]` + `Enumerator` + `SchemaEpoch` + `DedupeEntries`, with the finished-chunk set held **by
reference** into the `CheckpointStore`. A token that references the other store is not one durability
domain — it is exactly Debezium's two independently-committed stores, the counter-example this proposal
cites twice. And nothing states which store wins on restart when both hold state, so recovery is a
split-brain coin flip.

Also absent from the tier ladder the decision space specified: `SupportsWriterState`. A sink mid-way
through a multipart upload or an open staging file has nowhere to persist its own recovery state unless it
implements `Committer`, so at-least-once sinks with expensive in-progress work must discard it on every
restart.

---

### D11 — MAJOR. `Fetch` returning an error leaves the disposition of already-appended frames undefined.

**At fault:** `Reader.Fetch(ctx context.Context, b *proto.Batch) error`.

A reader appends 200 records, then its upstream connection drops, and it returns
`Classify(ClassNotConnected, err, …)`. The contract says nothing about `b`. Both readings lose:

- Engine discards `b`: if the source consumed those 200 from a socket, an HTTP long-poll, or a queue receive
  whose visibility timeout has been extended, they are gone with no committed position past them → loss.
- Engine keeps `b`: a partially-filled batch whose trailing `Mark` may or may not be present, from a reader
  whose internal state is now in an error path. If the last frame appended was a `Mark`, the engine may
  commit a cursor for records that were never emitted.

This is the class of question Benthos and Flink both had to pin down explicitly. Leaving it to prose makes
loss connector-dependent, which R4 forbids: *delivery semantics are a property of the implementation, not
of the prose describing it.*

Related and equally unstated: whether `MarkNow` may be called concurrently with a blocked `Fetch`. §5.1
promises non-concurrency for `AddSplits`/`RemoveSplits` and is silent for `MarkNow`. If it is not
concurrent, then a reader blocked for its full context deadline cannot be asked to mark — so
`WriteResult.RequestMark` cannot reach it, a `Committer` sink's transaction times out waiting for a
boundary, and `Pipeline.Drain` ("finish at the next mark, commit, then exit") hangs until something kills
it. The design also has no *drained vs drain-timeout* distinction, which the decision space requires
precisely because the second means records may replay.

---

### D12 — MAJOR. R5: `DedupeEntry` is undefined, unscoped, unbounded and untrimmable.

**At fault:** `Checkpoint.DedupeEntries []DedupeEntry` (§3) — the type is referenced once and **never
defined**; `CheckpointStore.Set` / `Delete` as separate operations.

The proposal claims (§13.7) that putting dedupe entries in the same atomic `Set` *"makes R5's 'committed
after the write' structural rather than a discipline"*. The ordering claim is right and is the good half of
R5. The rest of R5 is missed entirely:

- **Scope.** R5's first bug was a seen-set keyed on a bare event id, so two connectors or two tenants
  discard each other's records. `DedupeEntry` has no fields, so nothing says the key carries
  tenant/connector/stream. The design also has no tenancy concept anywhere, which the design rules require
  to be decided *before* the first multi-tenant field.
- **Retention.** R5's third bug was a 50k process-lifetime FIFO described in docs as a retention window.
  There is no TTL, no window, no eviction policy, and no metric.
- **Trimming.** Entries live *inside* the pipeline's checkpoint record, so every commit re-serialises the
  whole set — the merge-patch scaling problem the decision space explicitly flagged and deferred. Putting
  them in sibling keys instead requires retiring expired keys **in the same transaction** as a checkpoint
  write, and `Set` and `Delete` are separate calls with no combined atomicity. So either the window grows
  without bound (R6's spirit, at the store) or trimming is non-atomic and a crash mid-trim either
  resurrects "already stored" claims for data that is not, or deletes them for data that is.

The design rules' open decision #2 ("dedupe key scope and backing store, satisfying both durability and
tenancy") is therefore left open while being marked closed in §13.

---

### D13 — MAJOR. Schemas referenced by content hash have no durable home, and the cross-worker quiesce before DDL has no primitive.

**At fault:** `Schema.ID SchemaID` ("content-addressed hash"); `Record.SchemaID`; `Split.SchemaID`;
`Checkpoint.SchemaEpoch uint64`; `runtime.Runtime`'s four interfaces ("a fifth means the abstraction is
wrong"); §4.2's *"the engine quiesces and flushes the affected streams before forwarding it to a sink"*.

The proposal's argument against Debezium is that two independently-committed stores produce *"Encountered a
change event whose schema isn't known"*, and its fix is `SchemaEpoch` committed atomically with position.
But `SchemaEpoch` is a **`uint64`**. The `Schema` *values* — the field lists a historical event must be
decoded against — are stored nowhere: not in the checkpoint (which holds a scalar), not in `ConfigStore`
(typed for `PipelineSpec`), not in `CheckpointStore` in any specified layout, and re-deriving them from
`Source.Streams` gives you *today's* schema, which is the whole problem. After a restart, `Record.SchemaID`
and `Split.SchemaID` are dangling content hashes. Debezium's failure mode is reproduced, one level of
indirection later.

Also: a single pipeline-wide `SchemaEpoch` cannot represent per-stream epochs, while `SchemaChange.Epoch` is
per stream. On restore you know "epoch 3" and not which stream was at which schema.

Second half: `Batch.Schema(sc)` takes **no `SplitRef`**, and `SchemaChange` is scoped to a `StreamID`. A
stream can have many splits on many workers (chunks plus stream, or several stream splits). Quiescing "the
affected streams" therefore requires pausing production for stream X on *other workers* — and there is no
primitive for it: `Coordinator` has no barrier, `Emitter` and `Host` have no stream-pause, and the four
runtime interfaces are declared closed. Consequence with `DriftEvolve` and a narrowing or renaming change:
the DDL is applied to the destination while another worker is still writing old-schema records, those
records fail as `ClassPermanentMapping → DeadLetter`, the prefix advances, and the operator sees a
successful schema evolution with n records silently in the DLQ. The decision space names this race
explicitly (*"otherwise records written under the old schema race the ALTER"*).

---

### D14 — MAJOR. The buffer's durability class is undeclared, and two `WhenFull` modes change the delivery tier with no consequence anywhere.

**At fault:** `Buffer{Push, Pop, Depth, Capacity, Close}`; `WhenFull ∈ {FullBlock, FullReject, FullOverflow,
FullDropNewest}`; `Plan.Buffers []proto.BufferPlan`.

R6 is satisfied on the axis R6 is about — `Push` returns `accepted int`, so rejection is in the signature,
and `WhenFull` is in the type. Good. What is missing is the axis R4 is about, which the decision space
states as a requirement: *"the durability boundary is configurable and ack semantics follow it
automatically. A durable buffer takes ownership of the ack handles on write… a memory buffer passes them
through."* Nothing in `Buffer` declares which it is, and the resolver has no `Push`-time transition. So a
`FullOverflow` memory→disk chain either credits durability at a memory buffer (the predecessor's exact
catastrophe) or never credits at the disk tier (making it pointless). Undecided is the one state R4
forbids.

`FullDropNewest` is worse than undecided. Dropping a record already admitted to the resolver leaves it
neither `Written` nor `Failed`, so the per-split prefix **can never advance past it** — a permanent
checkpoint stall triggered by load. The only escapes are (a) the engine synthesises a success, which is
silent data loss in a pipeline whose `Plan.Guarantee` still says `AtLeastOnce`, or (b) it synthesises a
failure, which under the disposition table means DLQ or Terminal. None of the three is specified. And
`Negotiate` — the function whose whole purpose is to refuse impossible pipelines — is never said to
downgrade `Plan.Guarantee` to `AtMostOnce` when a drop-mode buffer is configured, even though
`Plan.Buffers` is right there in the struct.

---

## 4. Smaller correctness findings (not in the receipt list)

- **`CursorComparer` cannot produce the lag it gates.** `Plan.Lag` requires `CapHead ∧ CapCompareCursor` and
  promises `Progress.CursorLag *float64` — *seconds*. A three-way comparator yields an ordering, not a
  distance. The decision space specified `PositionComparer.Fraction(from, to) Optional[float64]`; the
  proposal dropped `Fraction` and kept the promise. Under the design's own honesty rule this must render as
  unknown, so `Plan.Lag = true` is a lie the read model will print.
- **No clock-skew policy.** `ClassClockSkew` exists as a label with no clamp/reject rule, no configuration
  location, and no disposition-table entry. Meanwhile `canal_checkpoint_age_seconds` — "the primary health
  metric" — is `now(api) − Header.At(worker)`, which under skew goes negative or hallucinates a stall, and
  `Coordinator` leases are pure TTL. The design rules list this as open decision #8; §13 does not close it.
- **A zero `Cursor` is the nil convention the decision space forbade.** *"A record may carry a null position
  meaning 'not resumable' … make it a typed property, not a nil convention."* `Cursor` is
  `Blob{Version, Bytes}` and "no position" is the zero value.
- **`Split.Ordered` is unenforced.** Only an ordered split may be marked mid-split, but `Batch.Mark` accepts
  any split and nothing rejects a mid-split mark on an unordered one. The consequence — resume skips unread
  rows — is silent loss enabled by an interface the design rules say should prevent it structurally.
- **Revocation leaves in-flight records unaccounted.** `RemoveSplits` says nothing about records already
  downstream; the old owner's resolver may still `Committed()` a cursor for a split it no longer owns,
  racing the new owner's write (see D2/D3).
- **`res.Stats` reconciliation is decorative.** "emitted == persisted + dropped, asserted, not assumed" is
  true only when the sink volunteers `Stats`; `stdoutSink` never calls it, and example (c) calls it with
  `written` and `bytes` that are never declared. An unenforced convention is not an assertion.
- **`KindOutcome`, `KindCommittable`, `KindMarkRequest` have no producer.** The sink→engine direction is
  `WriteResult` methods, not frames, so the sink half of the protocol is never exercised by the in-process
  binding — the binding whose existence is supposed to validate the wire binding. This is where D1's
  wire-unsafety hides.

## 5. Rule check

| Rule | Verdict |
|---|---|
| **R4 — an acknowledgement means durable** | **Violated, structurally.** Durability is inferred from `nil` at three edges (D1), none constrained; the reference sink loses data twice; the encoding is unsafe over the wire. The one place R4 *is* honoured — no timer, no path to a checkpoint except the resolver — is genuinely excellent and is why this scores 4 and not 2. |
| **R5 — dedupe keys scoped, committed after the write** | Half satisfied. Ordering is right and structural (same atomic `Set`). Scope, tenancy, retention and atomic trimming are all absent; `DedupeEntry` is undefined (D12). |
| **R6 — a buffer without a rejection path is not a buffer** | Satisfied on rejection (`accepted int`, `WhenFull` in the type, one `Buffer` interface, `Depth`/`Capacity` present). Fails on the durability half and on `FullDropNewest`'s interaction with the prefix (D14). |
| **R7 — the contract must name the failures** | **Best satisfaction of the three angles.** `WriteResult.Failed(id, Fault)` per record with a closed `Class`, a single core disposition table, and a worked partial-failure trace. The gap is `CommitOutcome` (undefined) and `Fetch`'s error/frame ambiguity (D11) — failure shapes written for the sink and not for the commit or read paths. |

## 6. Documented traps walked into

- **Chunked-snapshot output must be keyed upserts — checked at submit time.** Not checked (D6).
- **`map[CheckpointID][]Committable` pending set; abort ≠ discard; committable expiry.** Flattened, no ids,
  no expiry (D7).
- **Every checkpoint write carries the lease/generation and the store rejects a stale one.** Absent (D2).
- **A durable buffer takes ownership of the acks; a memory buffer passes them through.** Undecided (D14).
- **`Fraction`, not just `Compare`, is what produces a progress/lag number.** Dropped, promise kept (§4).
- **`filterOutdatedSplitInfos` as a pure reconciliation over restored state.** Not present; the paged
  finished-chunk set has no lifecycle (D2).
- **Fan-out is "ack when all branches ack".** No branch dimension in the resolver (D4).

## 7. What this proposal does better than any plausible alternative

These are not consolation prizes; several are things I would insist on regardless of which angle wins.

1. **In-band `Mark` frames as the *only* path to a committed cursor**, with the sink kept entirely
   progress-unaware and the position mapping owned by core. It gets Benthos's "a new sink cannot get
   checkpointing wrong" together with the position Benthos structurally lacks. No competing shape does both.
2. **The per-split `Resolver` with `Oldest()`.** `Oldest() (RecordID, time.Time, bool)` turns the oldest
   un-durable record into an age metric and a poison detector in one accessor — a small, obvious primitive
   nobody in the prior art exposes.
3. **`Completeness` on both change images, plus a sink declaring `RequiresCompleteImages` and the pipeline
   being refused at submit time.** This is a genuine contribution: an unchanged TOAST value, a `REPLICA
   IDENTITY DEFAULT`, and a Mongo update without a full-document lookup all produce partial images, and no
   surveyed system can distinguish partial from complete. Without it a generic upsert sink writes nulls over
   live data. Transplant verbatim.
4. **`Meta.NoteChange` / `FieldChange{Nulled|Truncated|Rounded|Redacted|Unavailable}`.** Lossy conversion
   becomes a countable, per-field, testable fact instead of a silent difference. This is the honesty rule
   made machine-readable at the record level rather than the badge level.
5. **`Batch.Record` lending framework-stamped slots.** Because `id` and `origin` are unexported and this is
   the only constructor, a connector *cannot* author provenance and a transform *cannot* corrupt it —
   KIP-793's retrofit closed by construction. The mechanism (lend a slot, don't accept a struct) is better
   than a documented rule.
6. **Emitted-vs-persisted reconciliation carried on the `Mark` itself** (`Records`, `Bytes`) against the
   sink's counts. Two integers per checkpoint turning "did it all arrive?" into an assertion. Make it
   mandatory rather than optional and it is excellent.
7. **The chunker's own resume cursor checkpointed in enumerator state, and the finished-chunk set paged by
   reference from day one.** Both of Flink CDC's scars pre-fixed, and mid-slice resume of a 500M-row object
   is the one place this proposal's resume story is strictly better than every shipped system.
8. **`Negotiate` as a pure function of declared data returning `Plan.Guarantee` *and* `Why` sentences, with
   every unknown as `nil`.** Refusing an impossible pipeline at submit time, and rendering
   "at-least-once — because sink `http` declares no commit tier", is the correct answer to Vector's silent
   downgrade. The inputs need fixing (D7); the mechanism is right.

## 8. Minimum repair set, if this angle is adopted

Ordered by how much of the design changes.

1. Flip `WriteResult` to explicit durability (`Written(id)` / `WrittenThrough(idx)`), and give every other
   nil-return edge (`DeadLetter`, `Buffer`) a declared durability class. (D1, D14)
2. Add a fence to `CheckpointStore` — `Set(ctx, put, del, fence)` with server-side rejection — shard the
   checkpoint by owner, and give `Shared` a single declared owner. (D2)
3. Add an epoch to `RecordID`, or forbid `SplitRef` reuse and mandate an in-flight drain before rebinding.
   (D3)
4. Give the resolver a settle-set with branch identity and a `Deferred` transition; key `Committables` by
   `MarkID`; define `Committable` and `CommitOutcome` including already-committed and expiry. (D4, D7)
5. Make marks global (quiesce-and-mark) *or* make `MarkConfirmed` per split and delete the global
   subsumption claim. (D8)
6. Add a source-owned key-in-split predicate and a bounded range-read to `Replayer`, move the per-chunk
   merge inside the chunk's reader, and gate `Chunked` on upsert-plus-key at submit time. (D5, D6)
7. Add a reserve-and-hold stream anchor, persisted in enumerator state before the first chunk is emitted.
   (D9)
8. Give schemas a durable home and per-stream epochs; specify what quiesce means across workers, or forbid
   multi-worker streams under `DriftEvolve`. (D13)
9. Define `DedupeEntry` with tenant/connector/stream scope and a retention window trimmable in the same
   transaction as a checkpoint. (D12)
10. Specify `Fetch`'s error/frame contract, `MarkNow` concurrency, and drained-vs-drain-timeout. (D11)

Items 1–5 are mechanical. Item 6 is a redesign of the largest piece of core machinery. Item 7 needs a new
capability. Until 1, 2, 3 and 6 are done, no delivery tier this proposal names is achievable with the
interfaces as written.

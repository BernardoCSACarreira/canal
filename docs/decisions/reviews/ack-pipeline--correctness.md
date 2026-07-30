# Review: the acknowledgement pipeline — lens: correctness of data-movement semantics

**Reviewer role:** hostile expert, single lens. I judge only what is guaranteed after a crash at every point in
a record's life; whether a checkpoint can commit for data not durably in the sink; whether at-least-once is
honest; whether the snapshot→stream handoff can miss or duplicate and whether it resumes mid-snapshot; whether
progress can advance past a gap; what happens on schema change mid-stream; and whether the delivery tiers named
in the document are reachable with the interfaces as written. I do not score elegance, ergonomics or prose
except where they bear on those questions. I assume processes are killed mid-batch, networks partition, sinks
time out after having actually succeeded, sources restart, clocks skew, and records arrive late.

**Score: 5 / 10.**

**Verdict in one paragraph.** The single-process spine of this design is the most correct ack-based data path in
the three-proposal space and in most of the prior art. `Position.Safe` with the rule *"commit the last Safe
position at or before the resolved prefix"* generalises Benthos's hand-rolled `latestXIDPos` into a core
invariant and is a genuinely new contribution. `Tracker[P]`'s weight budget being *simultaneously* the
backpressure mechanism and the exported replay window is the best operator-facing idea in any of the three
proposals. `Tracker.Abandon` plus a `Terminal` with no valid zero value makes Benthos's documented poison-record
deadlock inexpressible. `BatchError` keyed on a framework-assigned `RecordID` with no positional walk method is
the correct answer to R7. A sink that is never shown progress cannot get progress wrong, and yet the core still
knows the position — that synthesis is real and it is the reason this proposal exists. **But the thesis's proudest
consequence is a specified data-loss path.** §11(e) states, as a feature, that `Source.Commit` does not consult
the cluster, and §11(e) also states that when a lease lapses the source stops producing but *in-flight records
still settle*. Those two sentences together mean a paused worker whose lease has been reclaimed will commit a
position upstream — a replication-slot advance, a consumer-group commit — covering records the new holder has
not yet delivered, with **no fence of any kind**, because the upstream system has never heard of canal's lease.
That is unbounded silent loss produced by ordinary lease churn, and it is behaviour the document prescribes
rather than an interleaving it overlooked. Alongside it: the snapshot→stream ordering invariant is left entirely
to connector convention and is *structurally* unavailable in cluster mode, because `LaneCtl.Assigned` shows a
worker only its own lanes and there is no cross-lane dependency edge — so a worker holding the tail lane and no
scan lanes will tail while another worker is still scanning, and an upsert sink converges to the stale scan
value. Add `BatchError`'s fail-open default (unnamed records are implicitly *durable*), `RecordID` collapsing
under fan-out, and a schema epoch that is never committed with a position, and four of the design's own headline
claims do not hold.

The score is 5 and not 3 because every one of these is a *bounded, additive* fix to a skeleton that is right:
suppress acks for a revoked lane, add a fencing obligation to `Commit`, invert `BatchError`'s default, give
`Derive`-per-branch identity to fan-out, put the schema epoch in the lane row, add a lane dependency edge. The
score is not 7 because as written the design loses data in its headline deployment mode (multi-worker) and
corrupts destinations in its headline pipeline shape (snapshot-then-stream, distributed), and R4 exists to stop
exactly that.

---

## Part 1 — The record's life, crash by crash

| Point of crash | What the document claims | Actual guarantee as written |
|---|---|---|
| Inside `Read`, records appended to `dst`, error returned | Re-read from the committed token | **Undefined.** `Read(ctx, dst) error` — whether the engine admits a partially-filled `dst` when `Read` returns non-nil is never stated. Walkthrough (a) *depends* on it: the reference source sets `dst.EndOfLane` and `dst.Position` and then `return fault.ErrEndOfInput` with `dst.Len() == 0`. If the engine discards `dst` on error, `EndOfLane` never arrives and a bounded lane never finishes; if it admits it, `ErrEndOfInput` and live records coexist. Pick one and write it down. |
| After `Read`, before `Admit` | Nothing committed; re-read | Correct. |
| Blocked inside `Admit` | Backpressure | Correct, and this is the design's best structural property: one mechanism, no separate credit protocol. |
| In a transform / non-durable buffer | Nothing committed; re-read | Correct. |
| In a **`Durable` buffer**, after `Put` returned | "records survive the loss of this process" → core settles → source commits | **Loss.** The durability domain is `BufferRuntime.DataDir`, i.e. one pod's disk. The commit is a *global* act (slot advance, queue delete). Node loss, or lane reassignment to another worker, orphans the WAL after the source has committed past it. D-1. |
| Between `Write` returning nil and `Ledger.Settle` | Nothing committed; re-deliver | Correct, and this is the right direction. |
| `Write` returned a `BatchError` naming ≥1 failure | Unnamed records succeeded | **Loss.** Unnamed = implicitly Delivered (§2.1 and walkthrough (c) step 4). A sink that names the rows the server rejected but not the rows it never reached commits past unwritten data. D-3. |
| `Write` timed out, outcome unknown | Retry-safety obligation forbids a retryable class | **No representable answer.** The taxonomy has no indeterminate member; `PermanentUpstream.Terminates()` is true, so the honest class kills the pipeline. Every sink will violate the rule. D-4. |
| Between `Resolve` firing and `Source.Commit` running | Progress lost, replayed | Correct. |
| `Source.Commit` returns an error | Escalated as `CommitStalled`, retried | Correct in shape — genuinely better than Benthos's `_ = ackFn(...)`. But the ledger's prefix has already advanced and `Resolve` is spent, so `Ledger.Watermark` now reports a position the source refused. The read model lies. D-6. |
| Lease lapse while records are in flight | "the source stops producing for it; in-flight records still settle" | **Unbounded loss** for any source whose commit is upstream-destructive. D-2. |
| Mid-scan `kill -9` | Resume each scan lane from its own token | **Correct, and the best part of the design.** Lane rows are durable, unfinished lanes come back individually, re-parallelisation is free. Beats Benthos (restart from zero) and Airbyte (needed a protocol change). |
| At the scan→tail handoff | "the core owns the handoff invariant" | **False.** The handoff is a connector convention with no core expression, and it is unimplementable in cluster mode. D-5. |
| After a schema change, before restart | Schema epoch committed with the position | **Not committed at all.** `SchemaRef` is a 16-byte reference into a "schema table" whose durability is never specified, and schema changes arrive via `rt.Emit(Event)`, which is not in the record stream. Debezium's "schema isn't known" failure is reachable. D-7. |
| Anywhere, with a long transaction open | "duplicates bounded above by `budget` records" | **False.** The bound is *records since the last `Safe` position*, which is connector-determined and unbounded. `canal_lane_replay_window_records` exports `Tracker.Budget()`, which is then a lower bound presented as the answer. D-8. |

---

## Part 2 — Defects

### D-1 (fatal) — the `Durable` buffer's durability domain is a pod, the commit it authorises is global

§4.8: the buffer "is the ONLY component permitted to shorten the ack chain, and it may do so only by declaring
`Caps.Durable` — at which point the *core*, not the buffer, settles the group on a successful `Put`." §8.1: `wal
buffer: BufferCaps{Durable: true}` — a segmented local log. §4.13: `BufferRuntime` carries `DataDir`. §4.11:
"The conformance kit asserts this with a `kill -9`."

`kill -9` is precisely the wrong test. It proves survival of *process* loss and says nothing about the two
failures that actually occur:

**Interleaving 1 (node loss).** Worker A runs source → wal buffer → sink. 200k records are in the WAL, settled
on `Put`, and the source has committed the corresponding replication-slot position. The pod is evicted /
the node dies / the `emptyDir` is reclaimed. Those 200k records exist nowhere, and the upstream position is past
them. This is R4's abandoned-attempt catastrophe verbatim — acknowledge on local, unrecoverable storage, then
have upstream commit past it — one durability tier up. The proposal's own framing ("A buffer cannot lie its way
into early acknowledgement, because it does not perform the settling") closes the *lying* loophole and leaves
the *durability-domain* loophole wide open. Who performs the settle is irrelevant; where the bytes are is
everything.

**Interleaving 2 (reassignment).** §11(e): a lapsed lease closes `Revoked(lane)`, "another worker claims the row
after expiry." Worker B claims the lane and calls `Assigned`, gets `Committed` = the token A already committed,
and resumes from there — i.e. from *after* the records still sitting in A's WAL. If A never returns, those
records are lost. Nothing in `Coordinator`, `LaneCtl` or `Buffer` expresses "this lane's assignment may not move
while a Durable buffer holds unwritten records for it", and nothing pins a Durable-buffered lane to a node.

**Interleaving 3 (accounting).** Once a group is settled at the buffer, `Tracker.Abandon` can no longer be
called for it and `Ack.Abandoned` can never report it. So if the sink then fails terminally, the DLQ/drop is
invisible to the lane: `LaneStatus.recordsAbandoned` is wrong, `Ack.Abandoned` is zero, and a source that would
have refused to advance on a non-zero `Abandoned` never gets the chance. There is no second settlement domain
downstream of the durability boundary — Vector has one (a second finalizer domain); this design has one
`Ledger`, keyed per lane, and post-buffer records have no lane accountability.

**Consequence:** the design's one sanctioned way to shorten the ack chain silently loses data in k8s and
silently un-accounts terminal dispositions. `Negotiated.DurabilityEdge: "buffer:<label>"` discloses *where* the
edge is but produces no `Negotiated.Reasons` entry (no downgrade was requested), so `Build` never tells the
operator that their AtLeastOnce pipeline now has a node-local durability domain.

**Fix shape:** a Durable buffer must declare its durability *scope* (`ScopeProcess | ScopeNode | ScopeCluster`),
`Build` must refuse `ScopeNode` in cluster mode or force lane-to-node affinity, and reassignment must be gated
on the previous holder's buffer draining. Or: forbid ack-chain shortening entirely and let the WAL be a
non-settling buffer that merely absorbs sink outages.

### D-2 (fatal) — `Commit` has no fencing token, and the document specifies that a revoked lane's in-flight work still commits

This is the thesis's own consequence, and the document is proud of it. §4.2: "a pipeline whose source's progress
lives in the upstream system keeps committing with canal's control plane entirely down, because commit does not
touch canal's storage at all." §11(e): "*Kill the leader, kill the config store, kill the API.* Every worker
holding a valid lease keeps reading, transforming, writing, settling and **committing**, because `Source.Commit`
does not consult the cluster." And: "A worker's lease lapsing closes `Revoked(lane)`; the source stops producing
for it; **in-flight records still settle**."

§9 justifies the absence of a leader check correctly — `k8s.io/client-go` leader election does not guarantee a
single leader — and then asserts "The LEASE is the fencing token, and `StateStore`'s per-key CAS is the second
fence." A lease is a fencing token **only if the thing being written validates it.** `StateStore` does (per-key
CAS). Kafka's `__consumer_offsets`, a Postgres replication slot, an SQS `DeleteMessageBatch` and a Mongo resume
token do not. For exactly the class of source the thesis is built to serve — progress lives upstream, commit
never touches canal's storage — **there is no fence at all.**

**The interleaving, entirely from the document's own text:**

1. Worker A holds the `tail` lane on a logical-replication source. Settled through Seq 100; Seq 101–5000 in
   flight. Lease TTL 30s, renewed at 10s.
2. A enters a 40s stop-the-world GC pause / its network partitions.
3. The lease expires. Worker B claims the assignment row, constructs its own `Source`, calls `Assigned`, gets
   `Committed` = the token for Seq 100, and resumes replication from there. B begins delivering 101+.
4. A wakes. Its sink goroutines complete; `Ledger.Settle` runs for 101–5000 exactly as §11(e) specifies
   in-flight records must. The prefix advances. `Acks()` emits `Ack{Through: token(5000)}`.
5. A's `Source.Commit` advances the replication slot to 5000. `Revoked` being closed does not stop this: the
   ledger emits the ack, the commit pump delivers it, and `Commit`'s contract is "WHAT THIS MEANS IS ENTIRELY
   THE SOURCE'S DECISION. … The core does not care and does not check."
6. B has delivered through, say, 200. B crashes, or is rescheduled. B restarts, reads the slot, resumes from
   5000. **Records 201–5000 are never delivered by anyone.**

Nothing here is exotic: a GC pause longer than a lease TTL is the canonical distributed-systems failure and the
reason fencing tokens exist. The loss is unbounded — it is the whole in-flight window of the paused worker, and
the design's `Parallelism`/`Budget` knobs make that window as large as the operator wants.

Note the asymmetry with `StateHandle`: the CAS fence exists exactly where it is *least* needed (canal's own
store, which the design says is optional) and is absent exactly where it is load-bearing. And where it does
exist it misfires — see D-9.

**Fix shape:** revocation must be a hard barrier: `Ledger` stops emitting acks for a revoked lane, unsettled
groups are discarded (they will be re-read by the new holder), and `Commit` is never called for a lane whose
lease is not currently held. Additionally `Ack` should carry the lease epoch so a source that *can* fence
upstream (Kafka's producer epoch, a Postgres `pg_replication_slot_advance` guarded by a generation check) has
the value in hand. Without one of those two, "commit is independent of the control plane" and "at-least-once" are
mutually exclusive claims.

### D-3 (fatal) — `BatchError`'s default is implicit success: unnamed records are settled as durable

§2.1: "`Fail` records a per-record outcome. … Calling `Fail` at least once switches the batch to *everything not
named here succeeded*." Walkthrough (c) step 4 makes it concrete: 12 named failures out of 500 ⇒ "the other 488
succeeded: `Ledger.Settle(id, Delivered, nil)` × 488."

The design's own stated ideal, quoted approvingly from Vector in the decision space, is that **forgetting to
settle produces "do not checkpoint" — the safe direction.** `BatchError` inverts it. The default is *checkpoint*.
Consider the realistic sink response: a bulk endpoint returns HTTP 207 listing per-row errors for the rows it
validated and then reports a server-side abort. The sink knows 9 rows were rejected; it does not know whether
rows 300–499 were applied. The natural implementation — name the 9, return the headline — settles 491 records as
durably written, and the source commits its position past all of them. **Silent, unbounded, one-line data loss,
in the direction R4 exists to forbid.**

Worse, `Succeeded(id)` and `Fail(id, f)` have no stated precedence. §2.1 says `Succeeded` is "Needed by sinks
whose response is 'rows 0-9 ok, then I died'" — but a sink that calls only `Succeeded(0..9)` has never called
`Fail`, so the "at least once" switch has not flipped, so per the other sentence the headline applies to every
record *including the ten just marked succeeded*. Two documented rules, one contradiction, both readings unsafe
in opposite directions.

**Fix shape:** invert it. The complete outcome vocabulary must be three-valued per record — `Succeeded`,
`Failed(f)`, and *unstated ⇒ unknown ⇒ do not settle, retry* — with the headline applying only to unstated
records **and** only when it is not itself settling. A sink that wants "everything except these landed" says so
explicitly (`AllSucceededExcept()`), which is a declaration, not a default.

### D-4 (major) — the fault taxonomy has no member for "the write may or may not have landed"

§2's normative RETRY-SAFETY OBLIGATION: "A connector may return a retryable Class only when it knows the effect
did not land. If the remote may have applied the write, the class must be `PermanentUpstream` or
`DuplicateIdempotent`, never `TransientUpstream`."

`PermanentUpstream.Terminates()` is `true` — "Stop the pipeline". `DuplicateIdempotent.Settles()` is `true` — a
lie unless the sink *observed* a duplicate. So the design's own rule instructs every sink to **terminate the
pipeline on an HTTP request timeout**, the single most common sink failure that exists. No sink author will do
this; they will all return `TransientUpstream`, and the normative rule will be universally violated with a clean
conscience — which is exactly the "documented advance rule and actual durability were unrelated" pattern that
R4 was written about.

The honest answer needs a fourth axis. Retryability, blame and terminality are three independent properties
fused into one enum: `PermanentUpstream` covers both "403, will never work" and "quota exhausted", and the
latter is the canonical `Retry-After` case — so `Fault.RetryAfter` is *dead code* on the only class where a
throttle would honestly land. `Class` needs an `Indeterminate` member whose engine behaviour is keyed on the
negotiated guarantee: retry under `EffectivelyOnce` (the destination absorbs it), otherwise DLQ-and-disclose.
Then the retry-safety obligation becomes satisfiable rather than aspirational.

### D-5 (fatal) — the snapshot→stream ordering invariant is connector convention, and it is unimplementable in cluster mode

The decision space is unambiguous (D6 items 5 and 6): "**The core owns the handoff invariant**, not each
connector" and "Only start the incremental stream after a complete checkpoint following snapshot completion, so
no stream record can precede a snapshot record for the same key (verbatim from Flink CDC's documented rule)."

What this proposal actually provides:

- **Resumability: excellent.** Nine durable lane rows, mid-scan resume per range, re-parallelisation for free.
  This is the correct mechanism and it beats every surveyed system.
- **Gap-freedom: correct.** `logPos` is captured before any scan lane and its lane row is durable before any
  scan row moves (§11(b) step 3). No change is missed; duplicates are guaranteed and disclosed.
- **Ordering: absent.** §11(b) step 5: "`Read` serves the eight scan lanes round-robin… It does **not** read the
  tail lane yet. Nothing in the core knows or cares." Step 12: "The source now reads the tail lane." *The source
  decides.* There is no lane dependency edge, no `LaneSpec.After []LaneID`, no core gate. The core cannot check
  it, the conformance kit cannot test it (it has no idea what a tail lane is), and every CDC connector author
  must re-derive it. This is bit-for-bit the trap the decision space documents Benthos and Airbyte falling into,
  relocated from the checkpoint blob to the connector's `Read` loop.

And in cluster mode the convention is not merely unenforced, it is **impossible**:

**Interleaving.** Eight scan lanes and one tail lane are nine independently-leasable assignment rows (§11(e):
"in a cluster a different worker may claim each"). Worker C claims only the `tail` row. C constructs the source;
`LaneCtl.Assigned` returns "the subset this worker holds a lease on" — one lane, the tail. C's source sees no
scan lanes and therefore believes the scan is complete. It tails from `logPos` while workers D and E are 40%
through the scan. For key K: the tail emits `update(K, v2)`; later the scan lane covering K emits
`OpScanRead(K, v1)`. The upsert sink converges to **v1, permanently.** No error, no metric, no event. The
destination is silently wrong and stays wrong until K changes again.

There is no workaround available to a connector author: `LaneCtl` exposes only `Open`, `Finish`, `Assigned`
(my subset), `Revoked` and `Budget`. Cluster-wide lane state is not reachable. The source *cannot* know whether
another worker is still scanning.

Compounding it, the offered remedy is inert:

- **`scan_shadow` (§8.1) does not work.** It "drops a record with `Change.Op == OpScanRead` whose `Origin.Key`
  has already been seen from a stream lane **in this run**." (i) In the sequential design of §11(b) the tail lane
  is not read until the scans finish, so the "seen from a stream lane" set is **always empty** and the transform
  is a no-op — it is incoherent with the very handoff it is offered to fix. (ii) "in this run" means
  process-local and non-durable, so a crash mid-handoff loses the set and a scan record can then overwrite a
  newer stream record. (iii) In a cluster the scanning worker and the tailing worker have disjoint sets, so it
  does nothing. (iv) It is an unbounded in-memory hash set of every key observed, in a design whose R6 boast is
  that "unbounded growth is inexpressible."
- **The correct filter is not expressible from the data the core holds.** Flink CDC's predicate is
  `key ∈ range(chunk) && position > HIGH(chunk)`. There is no `HIGH` anywhere in this design: nothing captures
  the log position at which a scan lane *finished*, and `FinishedSnapshotSplitInfo`'s equivalent — the tuple
  `(key range, position from which changes to that range are still needed)` — has no representation.
  `LaneSpec.Resume` holds the range; nothing holds the watermark.
- §11(b) step 13's third bullet claims `Build` "refuses… a mutable-state scan into a non-idempotent sink with no
  `scan_shadow` and `Guarantee: EffectivelyOnce` requested". `Build` cannot know a scan is over *mutable* state
  (nothing declares that) and, per the above, `scan_shadow`'s presence guarantees nothing.

**Fix shape:** `LaneSpec` needs a dependency edge (`StartAfter []LaneName`) that the core enforces before
handing the lane to `Read` and before allowing it to be claimed; a scan lane must record a `HighPosition` on
`Finish`, persisted in its lane row and surviving lane retirement; and the dedup filter must be a core stage
keyed on `(range, HIGH)` from durable state, not an optional process-local transform.

### D-6 (major) — `Source.Commit` cannot express partial acceptance, so the ledger's watermark can be a lie

`Commit(ctx, ack) error` returns only an error. §4.3 documents a legitimate case for a source declining to
advance: "A source may refuse to advance on a non-zero `Abandoned` — for example a source whose commit is
destructive (deleting a queue message) may prefer to leave the message for another consumer. The core surfaces
the number rather than making the choice."

But `Ack.Through` is one scalar position and there is no "I accepted through here instead" return value. A source
that declines has two options, both wrong:

- Return `nil`. The ledger's prefix has already advanced and `Resolve` is spent. `Ledger.Watermark(lane)` now
  reports a position the source did not commit; `LaneStatus.Position`, `PositionSeq`, `CommittedAt` and
  `canal_checkpoint_age_seconds` — the document's designated *primary alert signal* — all report progress that
  does not exist. On restart the source resumes from its own (older) token and re-delivers a window the core
  believed committed, so `replayWindowRecords` is also wrong. The read model is derived from the ledger, not
  from what the source acknowledged persisting, and the two can diverge permanently.
- Return an error. The engine classifies, retries per policy and raises `CommitStalled` — an alarm for a source
  that is behaving exactly as documented.

This is R7 applied to the design's own most important method: the success shape was written and the
partial-success shape was not. `Commit` should return `(accepted record.Position, err error)`, or the ledger
should hold the prefix until the source confirms, so that the watermark means "the source says this is durable"
rather than "the core thinks it told the source".

### D-7 (fatal for the schema criterion) — the schema epoch is never committed with a position, and schema changes are not in the record stream

Three separate holes, and together they reproduce the exact Debezium failure the decision space cites
("Encountered a change event whose schema isn't known"), which D3 names as the reason schema state must live
inside the checkpoint.

1. **Schema changes are out-of-band.** There is no schema-change record, no `Batch`-level change event, no
   `Change.Op` member for DDL. The only channel is `SourceRuntime.Emit(Event)` — "a schema change, a lane note, a
   drift observation" — which is an unordered side channel into a "bounded `recentEvents` ring". So the
   "schema before data" ordering rule (D8 item 3, verified from Flink CDC) is structurally unavailable: the core
   cannot know that `Emit(schemaChange)` precedes the first record of the new epoch, and a ring buffer can
   *drop* the event that a sink needed in order to `ApplySchemaChange` before the data arrives.
2. **`SchemaRef` is a dangling pointer across a restart.** `SchemaRef{Fingerprint [16]byte, Epoch, Stream}` and
   "The schema body itself lives in the pipeline's schema table, deduplicated." That table's durability is never
   specified, it is not in `store` (four interfaces: `ConfigStore`, `StateStore`, `Coordinator`, `StatusSink` —
   §9 says "If a fifth appears, the abstraction is wrong"), and it is not in the lane row. After a restart the
   source replays from the committed token and re-emits records under epoch N+1; if the table did not survive,
   nothing can resolve the fingerprint and no record decodes. If the table *is* in `StateStore` under some
   unnamed key, then it is not committed atomically with the position, which is the same failure with an extra
   step.
3. **No quiesce before DDL.** `SinkCaps.MaxConcurrency` explicitly permits N concurrent `Write` calls, and
   `Request.Schema` only guarantees that *one request* does not mix epochs. So epoch-N and epoch-N+1 requests are
   concurrently in flight and `ApplySchemaChange` can land between, before or after either. D8 item 5 demands
   "quiesce and flush in-flight data before applying a DDL downstream"; nothing in `engine`, `Sink` or
   `SchemaApplier` describes an ordering barrier. The observable result is epoch-N+1 records written against the
   pre-ALTER shape — a mapping fault at best (DLQ), a silently truncated column at worst.

### D-8 (major) — the exported replay window is wrong precisely when `Safe` is doing its job

§5.3(4): "the duplicate window is bounded above by `budget` records — a number the operator configured and that
canal exports as `canal_lane_replay_window_records`. 'How much will I re-read after a `kill -9`?' has an exact
answer, which is not true of any prior-art ack pipeline." `LaneStats.ReplayWindow` is documented as `== budget`,
and `Tracker.Budget()` returns the configured cap.

`Safe` breaks this. The commit rule is *the last `Safe` position at or before the resolved prefix*. Settlement
still releases budget regardless of `Safe`, so `pending` drains normally and `Admit` never blocks — but the
*committed* position stays pinned at the last safe point. The true replay window is therefore
`Seq(head) − Seq(last committed Safe)`, which is bounded by nothing except how long the source goes without
producing a safe position. One 5-million-row transaction, or a source that only marks transaction boundaries
safe on a low-traffic stream, and the real window is orders of magnitude larger than the exported number.

This is the design's flagship operator metric reporting a **lower bound as if it were the answer**, which is the
category of dishonesty the design rules' "honesty as a structural property of the UI" note singles out. The value
is computable — the tracker knows `Seq(head)` and the last committed `Safe` position — so the fix is to export
the measured distance and keep `budget` as a separate ceiling on the *unsafe-free* case. As a second-order
consequence, an operator has no signal at all for "my source has not offered a safe resume point in 40 minutes",
which is the actual pathology.

### D-9 (major) — `StateHandle`'s CAS fence terminates the whole pipeline on ordinary lease churn

§4.2: a version mismatch on `StateHandle.Set` "is `fault.PermanentContract`: another writer holds this lane, and
continuing would clobber it." `PermanentContract.Terminates()` is `true` and `Retryable()` is `false`.

`Persister.Commit` *is* `Source.Commit`. So: worker A's lease on lane 3 lapses; B claims it and writes version
N+1; A's in-flight records settle (as §11(e) requires) and A's `Commit` CASes on version N; it gets
`PermanentContract`; the engine terminates **the entire pipeline on worker A**, including the other eight lanes
whose leases A still holds and which were making perfect progress. The correct response to "I have been fenced
out of one lane" is to drop that lane, not to stop the process. Combined with D-2 this is the worst of both
worlds: where the fence exists it is too blunt, and where it matters it is absent.

### D-10 (major) — `RecordID` is not unique under fan-out, which is the topology the design claims falls out free

`Record.Copy()` "returns a deep copy carrying the same `Origin` and the same `RecordID`: it is the same record,
materialised twice (for fan-out to two sinks)." `Ledger.Fanout(g, n)` bumps the group refcount by `n-1`. §8.1:
"Fan-out correctness is refcounting."

But every per-record outcome in the design is keyed on `RecordID` — "Every per-record outcome in canal — partial
batch failure, retry targeting, dead-lettering, settlement — is keyed on `RecordID` and never on an index." With
three sinks sharing one `RecordID`:

- Three `Settle(sameID, ...)` calls must each decrement once, which contradicts `Settle`'s "records one record's
  terminal disposition" and any idempotency.
- Sink B returning `BatchError{Fail(ID=1, ...)}` is indistinguishable from sink A doing so. The engine cannot
  retry only branch B. There is no branch identity anywhere in `Ref`, `Request` or `Fault`.
- "Worst outcome wins within a group" now means one branch's permanent failure abandons the record for all
  three, so a DLQ entry cannot say which destination failed.

Note the inconsistency: `Derive()` — 1→N expansion — correctly allocates a **fresh** `RecordID` with `Parent`
set. Fan-out, the case the design advertises as free, is the one that reuses identity. `Copy()` should allocate a
fresh `RecordID` per branch, with `Parent` and a branch label, and `Fanout` should register those IDs.

### D-11 (major) — `Nackable` is structurally inert, and discrete lanes have no visibility-extension path

`Nackable.Nack(ctx, lane, fs []fault.Fault)` and `Fault.Record record.RecordID`. A source can never learn any
`RecordID`: the core assigns it inside `Ledger.Admit`, *after* `Read` returns, and `Read`'s contract is "dst is
owned by the caller and reused; the source must not retain it." So a source receives a list of faults naming
identities it has never seen and cannot map to its own delivery handles. For the case `Nackable` exists to serve
— "park a message or notify upstream" on a queue source — the required key is the receipt handle / delivery tag,
i.e. the `Position.Token`, and `Nack` does not carry one. The interface cannot be implemented usefully by any
connector.

Related, for `OrderingDiscrete` lanes: receipt handles and delivery tags **expire**. A group can stay pending
for `MaxAttempts × Backoff` up to `MaxElapsed`, plus `GroupTTL`. There is no interface by which the core tells a
source "these positions are still in flight, refresh your upstream lease", so an SQS lane with a 30s visibility
timeout and a 4-attempt retry policy will have its messages redelivered to another consumer while canal is still
retrying them — duplicates the design cannot count and did not choose. `Heartbeater` is per-lane-idle, not
per-in-flight-group, and does not cover it.

### D-12 (major) — one blocking `Read` serialised against `Commit` means an idle lane starves every other lane's checkpoint

§4.3: `Read` "blocks until at least one record is available"; "`Read` is NEVER called concurrently with itself";
"`Commit` is never called concurrently with itself, and never concurrently with `Read`."

A source with eight scan lanes and one tail lane is a single `Read` goroutine multiplexing nine lanes. If the
tail is idle, `Read` may block inside the tail's wait. Because `Commit` cannot run concurrently with `Read`,
**no lane's position is persisted while any lane's read is blocked.** The scan lanes keep settling (settlement is
on the sink side, so budget is released and throughput is fine) and their positions accumulate in the ledger
unpersisted, so a `kill -9` re-reads all of it. The design's headline resumability property is defeated by an
idle lane, and nothing in the contract requires the engine to cancel `Read`'s ctx to service commits or requires
the source to poll rather than block.

The mirror case is worse for latency: a `Commit` that does a Postgres `StateHandle.Set` or an SQS
`DeleteMessageBatch` blocks reading for its whole round-trip, so commit latency directly caps read throughput
and the pipeline oscillates between reading and committing.

The serialisation guarantee is worth keeping (it is why sources need no locks) but it must be per-lane, or
`Commit` must be exempted from it with the source told that `Commit` and `Read` may overlap for *different*
lanes.

### D-13 (major) — `Ledger.Settle`'s semantics admit both a deadlock reading and a loss reading

`Settle(id, d, f)` "records one record's terminal disposition. Worst outcome wins within a group,
pessimistically: `Delivered + Failed = Failed`." But `Failed` is documented as "not delivered and not terminal;
the group stays pending" — i.e. explicitly **not** terminal, and it is the disposition used for a record that
will be retried (walkthrough (c) has records failing attempt 1 and landing on attempt 2).

Two readings, both broken:

- *Worst-wins is sticky.* A record that fails once and succeeds on retry leaves its group permanently `Failed`.
  The group never resolves, the prefix never advances, pending climbs to the budget, `Admit` blocks, the
  pipeline deadlocks — and `Leaks()` will report it at `GroupTTL` as a bug in the *engine*, which is where the
  operator will be sent.
- *Worst-wins applies to final outcomes only.* Then `Settle(id, Failed, f)` must not decrement the refcount and
  must be re-callable, which no part of the signature or the doc comment says, and there is no way to express
  "this record's outcome is not yet final" other than by overloading a member named `Failed`.

For the central settlement function of a design whose entire thesis is settlement, this must be a state machine
written out per record: `Pending → (Delivered | Duplicated | Abandoned)`, with retries not passing through
`Settle` at all.

### D-14 (minor but load-bearing) — R5's own dedupe rule is unimplementable with these interfaces

§8.1 states the `dedupe` transform's rule exactly right — keyed on `(pipeline, source, stream, Origin.Key)`,
injected store, "the key is marked seen **AFTER** the downstream settle, never before, which is R5's second
bug." That is the correct semantics and the correct diagnosis.

It cannot be built. `Transform.Apply(ctx, in, out) error` returns immediately; `TransformRuntime` exposes
`Context, Log, Metrics, Emit` and nothing else; and §10's dependency rule forbids a connector-side package from
importing `ledger`. A transform therefore has **no channel by which it can observe settlement** and cannot mark
a key seen after the write. Either `TransformRuntime` gains a settlement subscription (a real interface
addition), or dedupe must be an engine stage rather than a registered component. As written, R5 is satisfied in
prose and inexpressible in the type system — which is the precise failure mode R7 was written about.

### D-15 (minor) — clock-skew policy is named as a fault class and configured nowhere; `Position.At` is source-clock

`fault.ClockSkew` says "Behaviour is configured (clamp or reject), never silently chosen." There is no such
configuration: `engine.Spec` carries `Batching, Retry, WhenFull, LaneBudget, Guarantee, Drift, Parallelism` and
no clock policy, and `config.Fields` has no `ClockSkew` composite. Decision-space open question 8 is left open.

Concretely: `Position.At` is set by the source (`At: time.Now()` in the reference connector) and
`canal_checkpoint_age_seconds` — the primary alert signal — is that value differenced against the *core's*
clock. In a multi-container deployment with 30s of skew the primary alert is 30s wrong in either direction, and
a negative age has no defined rendering. `GroupTTL`, `Depth.OldestAge`, `Batcher.MaxAge` and
`Tracker.Pending()`'s `oldest` are all durations whose clock source (wall vs monotonic) is unstated.

### D-16 (minor) — three loss bugs in the reference connector, which is presented as the whole cost of adding a source

§11(a) is offered as proof that the contract is easy to implement correctly. It contains:

```go
dst.EndOfLane = dst.Len() == 0 || s.sc.Err() == nil && !s.sc.Scan()
```

- `s.sc.Scan()` **consumes and discards a line.** `s.off` is then not advanced for it, but the loop has moved
  past it, so on the *next* `Read` that record is gone. `dst.Position` is computed from `s.off`, so the token
  commits over a record that was never emitted — data loss in the exemplar.
- `Open` seeks to `s.off`, an in-memory field, not to the restored token. `Read` advances `s.off` *before*
  admission. So the `ErrNotConnected` → `Open` path (which the contract explicitly requires to be re-callable)
  resumes **after** all the in-flight, unsettled records and skips them permanently.
- `if dst.Len() == 0 { return fault.ErrEndOfInput }` returns an error on the same call that set `EndOfLane` and
  `Position`, with the engine's handling of a partially-populated `dst`-plus-error undefined (see Part 1).

Three loss-direction bugs in forty lines, in the connector the document uses to argue the contract is safe. The
conformance kit cannot catch any of them (it does not know what the file's true end is), which is directly R4's
"delivery semantics are a property of the implementation, not of the prose describing it."

### D-17 (minor) — `EffectivelyOnce` is not achievable, and `Idempotent` conflates two different properties

`Build` grants `EffectivelyOnce` on `SinkCaps.Idempotent && SourceCaps.StableKeys`. Two problems.

- **Idempotent upsert absorbs duplicates only under per-key ordering.** The design's ordering guarantee is
  "ordered within a lane, unordered across lanes" — and the same key routinely appears in two lanes (scan +
  tail, D-5), and `OrderingDiscrete` lanes have no ordering at all. So the older value can land after the newer
  one and the destination is wrong. `EffectivelyOnce` is granted for exactly the pipeline shape where it does
  not hold.
- **`Idempotent` is request-level in its wording** ("re-delivering an identical request is harmless") but the
  engine's retry rebuilds a request from a *subset* of `RecordID`s (walkthrough (c) step 5), so the retried
  request is never identical. A sink whose idempotency is a client-supplied key over the whole body is silently
  broken by sub-batch retry, and `Request.Attempt` increments while `Body` changes, so it cannot even derive a
  stable key. Record-level and request-level idempotency need to be two declarations.

### D-18 (minor) — cross-restart identity hazards in `Position` and the unpopulatable `Committed`

`Position.Seq` is core-assigned by `Ledger.Admit`, and the `Ledger` is a per-process object. So `Seq` resets to 1
on every restart while `Token` does not. `Ack.Through` hands a source a single struct mixing a session-scoped
scalar with a durable token and marks neither, so a connector author who persists the whole `Position` (the
obvious thing to do) silently records a meaningless `Seq`. Say so on the field.

Relatedly, `LaneAssignment.Committed record.Position` is documented as "the last position the core observed being
committed for this lane before the process stopped, **if the source used the state helper**." `Persister` writes
only the token (`Restore(lane) ([]byte, uint16)`), so `Committed` can only be populated if the helper writes a
core-framed envelope — and for the design's celebrated case (progress lives upstream, `StateHandle` untouched)
`Committed` is permanently unpopulatable. Consequence: after a restart the core has **no committed watermark at all** for
exactly the sources the thesis is built around, so `checkpointAgeSeconds`, `position` and `positionSeq` are
unknown until the first fresh ack. "The ledger is the observer" holds only within one process lifetime, which is
a material qualification on the proposal's central answer to Benthos's disqualifying flaw.

### D-19 (minor) — unresolved settlement paths that can leak or drop

- `WhenFullDropNewest` "discards the incoming records and counts them." Nothing says they are settled. If they
  are not, the group leaks until `GroupTTL`; if they are settled `Delivered`, the source commits past dropped
  data with no `Abandoned` count. Say which.
- `WhenFullOverflow` "spills to the next buffer in the chain." If the chain is `memory → wal(Durable)`, which
  `Put` triggers the core's settle? If the first, records are settled on a non-durable buffer.
- `Terminal == TerminalDLQ` where the DLQ is itself a `Sink` with its own retry policy: nothing forbids a DLQ
  whose terminal is a DLQ. `Build` refuses "TerminalDLQ with no DLQ configured" but not a cycle.
- `Negotiated.DurabilityEdge` is documented as `"sink"` or `"buffer:<label>"` — there is no value for
  `AtMostOnce`, whose edge is source hand-over. The honesty mechanism cannot describe the one tier that is
  honestly lossy.

---

## Part 3 — Audit against R4, R5, R6, R7

**R4 — an acknowledgement means durable.** Half-satisfied, and the half that fails is the half that matters at
scale. Satisfied on the direct sink path: `Write` returning nil is the *only* success signal, the sink is never
shown progress, settlement is strictly downstream-driven, and the prefix cannot advance over an unsettled group.
That is R4 as a mechanism and it is better than any of the surveyed systems. Failed in four places: the `Durable`
buffer's durability domain is a pod's disk while the commit it authorises is global (D-1); `BatchError`'s
implicit-success default settles unnamed records as durable (D-3); the taxonomy forces sinks to misreport
indeterminate writes as retryable (D-4); and a revoked worker's in-flight work is specified to settle and
therefore commit, unfenced (D-2). The document's claim that "'acknowledge before durable' requires a sink to
*lie about a return value* rather than merely be sloppy" is true of `Write` and false of `BatchError`.

**R5 — dedupe keys are scoped, and committed after the write.** Diagnosed perfectly and implemented nowhere. The
`dedupe` component's stated key scope and post-settle commit order are exactly right, and the store is injected
rather than module-global (the rule's own "ideas worth carrying forward" item). But a `Transform` has no
settlement channel, so the post-settle ordering is unimplementable (D-14); and `scan_shadow`'s "seen in this
run" set is process-local, non-durable, unbounded and, in the sequential handoff, always empty (D-5) — the exact
"present in a RAM cache" semantics R5 forbids, in the component the correctness story leans on.

**R6 — a buffer without a rejection path is not a buffer.** The best-satisfied rule in the proposal. One
`Buffer` interface; `Put` returns `Accepted{Records, Refused}` so refusal is in the signature; `WhenFull` has no
unbounded member so unbounded growth is inexpressible; `Depth` carries both capacities and `OldestAge`; `Drain`
propagates end-of-input through a stateful stage. Deductions only for the unsettled-record ambiguities in D-19
and for the two unbounded in-memory sets (`scan_shadow`, `dedupe`) that the design ships in its own components.

**R7 — if the design says "retry the failures", the contract must name them.** Partially satisfied, and the
misses are systematic. The sink's failure shape is written properly and is the single best artefact here:
`BatchError` keyed on framework-assigned identity, with no positional walk, degrading to a headline. But four
other failure shapes are missing: `Source.Commit` has no partial-acceptance return (D-6); `Nackable` names a
shape the source cannot consume (D-11); the indeterminate-write outcome has no member (D-4); and
`Settle(Failed)` has no defined refcount contract (D-13). "Write the failure shape at the same time as the
success shape" was applied once, thoroughly, and then not applied to the source side at all.

---

## Part 4 — documented traps this proposal walks into

- **D6's phase trap, in a new location.** The decision space's finding is that Benthos and Airbyte both encoded
  the snapshot phase in connector-private state and both lost resumability *and* the handoff ordering. This
  proposal fixes resumability brilliantly (durable lane rows) and then leaves the ordering in exactly the same
  place: the connector's `Read` loop. The claim "the core owns the handoff invariant" is not delivered by
  `LaneCtl.Open`'s durability — durability of `logPos` gives gap-freedom, not ordering.
- **D7's missing `HIGH`.** No per-chunk high watermark exists, so the verified Flink CDC filter
  (`key ∈ range && position > HIGH`) is inexpressible and the `(range, HIGH)` tuple that the decision space
  calls "the entire snapshot→stream contract" has no representation. `scan_shadow` is a strictly weaker,
  incorrect substitute (D-5).
- **D3's "schema epoch is inside the checkpoint. One record, one commit."** Not done. The cited Debezium failure
  mode is reachable (D-7).
- **D3's "when a quantity is unmeasurable, omit the metric series entirely."** Honoured everywhere in the read
  model (nil pointers, one "unknown" renderer, the pinned all-absent fixture — genuinely excellent) and then
  violated by the one metric the design invented: `canal_lane_replay_window_records` reports a configured
  ceiling as a measurement (D-8).
- **D9's "a type assertion cannot cross a process boundary."** Honoured — `Caps` is data, `Resolve` is the single
  assertion site. But `StateHandle`, `LaneCtl` and `SourceRuntime` are interfaces/structs the connector *calls*,
  and the concrete `*SourceRuntime` and `*Persister` types are not wire-shaped (`Metrics` returns interfaces;
  `Batcher` is a concrete struct). Out of lens, noted only because a gRPC boundary would change `Commit`'s
  timing and therefore its correctness.
- **Correctly avoided:** the wall-clock commit trap (D4(a) / KAFKA-4942) — nothing in this design is on a timer,
  and walkthrough (c) step 4 makes the point precisely. The "failed commit logged and dropped" trap (Benthos) —
  escalated as a condition. The "unbounded retry deadlocks on a poison record" trap — inexpressible. The "opaque
  checkpoint means no lag metric" trap (Connect) — solved by `Position.Label` + core-assigned `Seq`. Those four
  avoidances are real and should be preserved by whatever wins.

---

## Part 5 — what this proposal does better than any plausible alternative

Listed because a synthesiser needs to know what to carry even if the proposal loses.

1. **`Position.Safe` plus the rule "commit the last `Safe` position at or before the resolved prefix."** This is
   the correct generalisation of "a contiguous prefix is not automatically a legal resume point", and no shipped
   system has it as a core invariant — Benthos threads `latestXIDPos` by hand in one connector. It costs a
   connector that has no such distinction exactly nothing (`Safe: true` everywhere). Transplant verbatim, and
   pair it with a *measured* replay-window metric (D-8).
2. **The weight budget as one mechanism serving three purposes:** admission backpressure, in-flight bound, and
   the worst-case replay window. Benthos needed three overlapping knobs (`checkpoint_limit`, `max_in_flight`,
   `Capped`) to re-impose what its ack model gave away; this collapses them into one number an operator sets and
   the core exports. Best single idea in the proposal.
3. **`Tracker.Abandon` + `Terminal` having no valid zero value.** Unbounded retry is not a badly-chosen default,
   it is unconstructible, and the abandon path is what lets the prefix move past a poison record so the source
   unblocks. This structurally kills Benthos's documented deadlock and should be a hard requirement on any
   winning design.
4. **`BatchError` keyed on a framework-assigned `RecordID`, with the positional walk method *absent* rather than
   deprecated.** The right answer to R7's per-record partial failure, and the right diagnosis of why Benthos's
   `WalkMessages` is marked harmful by its own author. Keep the shape; invert the default (D-3).
5. **A sink with zero progress awareness while the core still knows the position.** This is the actual thesis and
   it holds: `Write(ctx, *Request) error` is the complete contract, a new sink *cannot* get checkpointing wrong,
   and the core nonetheless has a watermark because the position rides on the batch rather than in a closure.
   That is the synthesis of Benthos's best property with Connect's, and it is what the whole document earns.
6. **`GroupTTL` + `Leaks() []Leak{Group, Lane, Age, Records, Stage}`.** Turning "someone forgot to settle" from
   a silent stall into a named condition with the offending stage is strictly better than Rust's `Drop` for
   diagnosis, and it is the correct answer to Go's lack of one. Every ack-based design needs this reaper.
7. **Lanes as durable, individually-leasable rows carrying their own `Resume`.** Mid-snapshot resume,
   per-lane re-parallelisation without a restart, and "distribution is restart with a different subset" are all
   consequences of one representation. This is the cleanest solution in the three proposals to the thing Benthos
   (snapshot from zero) and Airbyte (protocol change) both got wrong, and it needs no split/enumerator protocol.
8. **`Ack.Abandoned` as a count the source may act on, with the core refusing to make the choice.** The right
   division: a destructive-commit source gets to decline. It needs a return channel to be usable (D-6), but the
   *idea* — surface the number, do not decide — is correct and absent from every surveyed system.
9. **`Completeness` on the `Change` facet.** A sink can refuse honestly instead of writing a hole for an
   unchanged-TOAST partial after-image. Small, cheap, and nobody else has it.
10. **`Negotiated{Guarantee, Reasons, DurabilityEdge, ReplayWindow, SettleAt}` returned from `Build` and shown on
    the submit screen.** Refusal-or-disclosure instead of Vector's silent acknowledgement downgrade. Needs a
    value for source hand-over and a `Reasons` entry for a node-local durability edge, but the mechanism is
    right.

---

## Part 6 — the minimum set of changes that would make this design correct

Ordered by severity, for the synthesiser.

1. **Revocation is a barrier, not a suggestion.** On lease loss the ledger stops emitting acks for the lane and
   discards unsettled groups; `Commit` is never called for a lane not currently held; `Ack` carries the lease
   epoch so sources that *can* fence upstream have the value. (D-2, D-9)
2. **Invert `BatchError`.** Unnamed records are *unknown* and are not settled. Explicit
   `AllSucceededExcept()` for sinks that genuinely know. (D-3)
3. **A lane dependency edge (`StartAfter`), a `HighPosition` recorded on lane `Finish` and persisted in the lane
   row, and a core dedup filter keyed on `(range, HIGH)` from durable state.** Then the handoff invariant is the
   core's, enforceable, and works in a cluster. Delete `scan_shadow`. (D-5)
4. **A durability *scope* on `BufferCaps.Durable`, with `Build` refusing node-local scope in cluster mode, and a
   second settlement domain downstream of the durability boundary.** (D-1)
5. **An `Indeterminate` fault class whose engine behaviour is keyed on the negotiated guarantee.** (D-4)
6. **The schema epoch in the lane row, committed with the position; schema changes as ordered in-band records,
   not `Emit` events; a quiesce barrier before `ApplySchemaChange`.** (D-7)
7. **`Commit` returns the accepted position.** (D-6)
8. **A written per-record settlement state machine; retries do not pass through `Settle`.** (D-13)
9. **Fresh `RecordID` per fan-out branch.** (D-10)
10. **Per-lane `Read`/`Commit` serialisation instead of per-source.** (D-12)
11. **Export the measured replay window, not the budget.** (D-8)
12. **`Nack` keyed on `Position`, not `RecordID`; an in-flight-lease-refresh hook for discrete lanes.** (D-11)

Items 1–4 are the difference between a design that loses data and one that does not. Everything above item 7 is
a change to a *signature*, not to the architecture — which is the honest reason this scores 5 and not lower: the
spine survives all twelve fixes unchanged.

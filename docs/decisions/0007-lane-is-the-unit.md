# 0007 — The lane is the unit of work, resume, ordering, assignment and accounting

**Status:** accepted, normative.

## Context

Four candidate spines were designed and reviewed. Two of them — "the acknowledgement pipeline" with its
*lanes*, and "the split is the unit" with its *splits* — converged on the same structural idea from opposite
directions, and the reviewers of both named the same property as the strongest thing in either document:

> Lanes as durable, individually-leasable rows carrying their own opaque resume payload, so mid-snapshot
> resume, re-parallelisation without restart, and "distribution is restart with a different subset" all fall
> out of one representation.

The systems that omitted the concept are structurally unable to scale a stateful source. Vector's only
horizontally-scalable stateful source is `kafka`, and only because it borrows someone else's coordinator; two
Vector instances with the same `file` source both read the same files and duplicate. Airbyte traded
distribution for a trivially-correct checkpoint model. Benthos's snapshot phase is `pos == nil`, so there is
no representation for "partially snapshotted" and a crash at 90% of a 500M-row snapshot restarts from zero.
Kafka Connect splits work exactly once at connector start; re-splitting restarts tasks, so "twenty workers
for the snapshot, two for the stream" is inexpressible — and that asymmetry is a first-class fact of CDC.

Adding a partition concept later is a breaking source-interface change, so it belongs in v1 even if the first
coordinator is "one worker, all lanes".

## Decision

**A lane is one independently-ordered, independently-resumable, independently-assignable progress domain
inside a source.** It is simultaneously:

| Job | Mechanism |
|---|---|
| unit of parallelism | one lane is read by one worker at a time |
| unit of resume | `LaneState{Spec, Cursor}` is the entire durable state |
| ordering scope | `Ordering ∈ {Prefix, Discrete}`, declared per lane |
| assignment and lease subject | one durable row, claimed by CAS, fenced by an epoch |
| in-flight accounting scope | one `Tracker` and one budget per lane |
| progress-reporting scope | one `LaneStatus`, one set of counters |

**The source plans; the runtime places.** A source announces lanes through `LaneCtl.Announce`, continuously,
whenever it likes. The runtime decides which worker runs which lane. Neither learns the other's algorithm.
There is **no separate enumerator role** and no assignment protocol in the required interface: a lane is
simply "a progress domain the source told us about". That is what makes "scan with twenty lanes then stream
with one" expressible without restarting anything.

**A lane's durable state is two differently-lifetimed opaque blobs.** `Spec` is write-once (the key range,
the starting LSN, the shard id); `Cursor` is write-many (the position). Flink CDC maintains a parallel
`SplitState` class hierarchy with downcasts because it conflated the two; two fields on one value deletes that
hierarchy.

**Lane identity is derived from `(tenant, pipeline, node, LaneSpec.Name)`, and `Name` must come from stable
content properties.** Vector's file source needs content fingerprinting precisely because inodes get reused,
and a lane whose name changes on restart silently re-reads everything. `LaneID` is never a parsed string:
Flink mandated only `splitId() string`, so Flink CDC encoded `table:chunkID` into it and parses it back out in
three places with a warning that a custom id breaks the parser.

**`Announce` is durable before it returns**, and idempotent on `Name` with an identical spec. Re-announcing
with a *different* spec is `PermanentContract`, because silently rewriting a lane's construction payload is
how a resume lands at the wrong place.

**Boundedness is on the lane, and it is what a "pipeline type" is.** All lanes bounded = a batch pipeline; one
unbounded lane = streaming; both = hybrid. There is no `pipeline.type` field, no phase enum in the data path,
and a CI grep asserts nothing in the core switches on `LaneKind`.

**`UnitAssignment` declares who owns the division of work.** A source whose upstream already solves
assignment — a Kafka consumer group — declares `UnitsExternal`, and canal announces one lane per instance and
does not attempt to place work. Without that, a planning core actively fights the broker. `UnitsStatic` versus
`UnitsDynamic` is the re-enumeration licence: whether the lane set may change at runtime is knowable at config
time and is the difference between a one-shot plan and a reconciliation loop.

**`MaxLanes` is hard-enforced on the first violation.** Kafka Connect's `tasks.max` was advisory for eight
years until KIP-1004 had to enforce it against "buggy or hostile connectors ... threatening cluster
stability", and it shipped a deprecated escape hatch for connectors already violating it.

## Alternatives rejected

- **One-shot task planning (Kafka Connect).** Rejected: re-splitting restarts tasks, and per-phase parallelism
  is inexpressible.
- **A separate enumerator role plus a reader role (FLIP-27).** Rejected as *more machinery for the same
  property*. It adds a second stateful component that itself needs a state machine and its own checkpoint —
  a weakness the split proposal's own author listed — plus an assignment protocol, plus a documented class of
  bug (Flink CDC shipped two `ConcurrentModificationException` fixes for touching enumerator state
  off-thread). Announcing lanes from the source achieves planner/placer separation with one component and no
  protocol. What is genuinely lost is a *pull-based* assignment request; the reconciler covers it.
- **No lane concept at all.** Rejected: it is Vector, Benthos and Airbyte, and retrofitting is a breaking
  interface change.
- **Reserving the vocabulary and building nothing** (Conduit's ADR: ship the partition-claim protocol years
  before a scheduler consumes it). Genuinely cheaper, and rejected because canal's end-state goals include
  coordinated multi-worker deployment as a *shipped* feature, not a future one — and because the lane is doing
  five other jobs here that a reserved vocabulary would not do.
- **An opaque lane-id string with structure encoded into it.** Rejected: an unfixable public path is a
  permanent wire contract, and Flink CDC's `wartermark` typo is the standing example.

## Consequences

- Positive: mid-scan resume, re-parallelisation without restart, per-phase parallelism, ordered-within-unordered-
  across parallelism, and "distribution is restart with a different subset" are all one representation. The
  frontend gets a real progress model from lane rows.
- **Negative, accepted:** a source author must think about lanes even when there is one. Mitigated: a trivial
  source announces one lane in five lines, and the cold/warm branch is four lines that the conformance kit
  tests separately.
- **Negative, accepted:** there is no pull-based "give me more work when I have capacity" protocol, so
  balancing is by claim order plus the reconciler rather than by demand. Acceptable at canal's scale, and
  `LaneSpec.Weight` gives the planner a size hint.
- **Negative, accepted:** a source that announces very many lanes creates very many durable rows and very many
  read-model entries. `MaxLanes` bounds it, per-lane detail is served from the read model rather than as metric
  labels, and the finished-lane set is paged by reference.

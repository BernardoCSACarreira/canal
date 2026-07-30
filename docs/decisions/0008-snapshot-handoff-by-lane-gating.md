# 0008 — Snapshot-to-stream handoff is durable lane gating, not a watermark engine

**Status:** accepted, normative.

## Context

"Full scan, then incremental stream" is a first-class goal, and it is where the reviewed proposals failed
worst. Twelve of the forty-eight fatal defects raised across the four designs are in this one area:

- The proposal rooted in acknowledgements made the handoff **pure connector convention** — announce the tail
  lane before the scan lanes and read them in the right order — which is unimplementable in a cluster: a
  worker holding only the tail lane sees no scan lanes and tails while others scan, and an upsert sink then
  converges to the *stale* scan value.
- The proposal that implemented Flink CDC's watermark engine in core accumulated six fatals in it: mid-chunk
  resume leaves two consistency positions under one watermark and the stream filter then suppresses real
  changes to the already-emitted prefix (silent loss, on its own flagship walkthrough); the
  all-completions-durable gate is universally quantified over an empty set and so is true before any chunk
  finishes; a completion is admitted when the final batch settles rather than when the prefix resolves; and the
  filter's preconditions are neither declared nor validated.
- Two other proposals had **no home at all** for the chunk range filter or the per-chunk backfill merge.

Meanwhile Flink CDC — the only verified implementation — documents its own two scars: the per-record filter
was an O(chunks) linear scan needing a sorted-range binary-search retrofit behind a dialect gate, and the
finished-chunk set turned out too large to ship in one message, requiring four new events and a truncation
path after the fact.

And Conduit, the closest prior art to canal, ships the *other* consistency family entirely: pin a consistent
read snapshot, resume per table from a per-table `last_read`, no watermarks, no core machinery.

## Decision

**The handoff is durable lane gating, enforced by the core from the lane table.**

```go
type LaneSpec struct {
    // ...
    Group      record.LaneGroup   // this lane's membership label, connector-authored, opaque
    StartAfter []record.LaneGroup // groups that must be FINISHED AND DURABLE first
}
```

The rule: **a lane whose `StartAfter` contains group G is not assignable and not readable until every lane in
group G, in this pipeline generation, is finished and that finish is durable** (`LaneState.FinishedAt` is set
in the same atomic write). The **planner** enforces it by reading the durable lane table, so it holds
cluster-wide: a worker that happens to hold only the tail cannot start early. That is the whole fix for the
first fatal above.

The connector's obligation is three lines: capture the log position, announce the tail lane with
`Group: "tail"`, `StartAfter: ["scan"]` and that position as its `Spec`, then announce the scan lanes with
`Group: "scan"`. `Announce` is durable before it returns, so the fact that the tail must resume from that
position survives a crash before one scan row moves — and the core made it durable without knowing what a log
position is.

**The chunked-snapshot watermark engine is NOT in the core.** Concurrent scan-and-stream is not available in
v1.

**But every datum the engine would need is in v1**, so adding it later changes no connector interface:

| Needed by the engine | Already present |
|---|---|
| comparable positions and keys | `Position.Order`, and `Origin.Key` in a canonical encoding |
| progress arithmetic | `Position.Scalar` |
| a chunk's key range | `LaneSpec.Spec`, opaque, connector-authored |
| per-chunk completion **and when it became durable** | `LaneState.Finished` + `FinishedAt` |
| the chunker's own resume cursor | a chunk-planning lane is a lane, with a cursor |
| declared preconditions | `ComparablePositions`, `StableKeys`, `Replayable`, `MidLaneResume` |
| a paged finished-chunk set | lane rows are already paged by reference from the store |

**For the concurrent case today**, two composable pieces exist and neither is core machinery:

- `canal/scan` — a **library** a connector imports to split a key space into ranges with unbounded ends. Not a
  core stage; versioned separately; ignorable.
- the `shadow` **transform** — drops an `OpScanRead` record whose `Origin.Key` has already been seen from a
  stream lane in this run, and self-retires when the last scan lane closes. Its index is a hash set from day
  one, so Flink CDC's first scar is pre-fixed. A pipeline that does not need it does not pay for it.

`Build` refuses a mutable-state scan into an append-only non-idempotent sink with no `shadow` transform when
`EffectivelyOnce` is requested.

## Alternatives rejected

- **The handoff as connector convention.** Rejected: fatal in a cluster.
- **The eight-step watermark engine in core for v1.** Rejected on three grounds. It is the single largest piece
  of machinery in the surveyed field; the one attempt to specify it here accumulated six fatal defects
  including silent loss on its own walkthrough; and its correctness rests on preconditions
  (`Order` agreeing with the source's own collation) that *nothing can check* — a wrong `Order` implementation
  fails silently in exactly the case that loses data. Correctness first: a design that is right and slower
  beats a design that is fast and has a silent-loss path.
- **A phase enum in the checkpoint or in the core.** Rejected: Debezium/Kafka Connect and Airbyte both
  smuggled a phase into their opaque checkpoint, independently, and both lost snapshot progress reporting,
  snapshot-specific parallelism and re-parallelised resume. Two independent occurrences is conclusive. `LaneKind`
  exists for *reporting* and a CI grep asserts nothing branches on it.
- **A dependency edge between individual lanes** rather than between groups. Rejected: it is a DAG over
  thousands of rows, and the only real requirement is "this set before that one".
- **Reduction annotations that make duplicates harmless** (Estuary's approach), which would dissolve the
  handoff entirely. Genuinely the most elegant answer in the evidence base, and rejected because it requires a
  merge model in the core record semantics and pushes canal toward being an opinionated stream processor rather
  than a data-movement tool. Revisit only if the op facet proves inadequate.
- **Requiring write access to the source for a signal table** (Debezium's `execute-snapshot` signalling).
  Rejected: demanding DDL or write access on a source is a deployment tax many operators refuse.

## Consequences

- Positive: the handoff is correct for **every** source, including one with no comparable positions, with no
  core machinery, no filter, no watermark protocol and no silent-loss path. The gate is one field the planner
  reads.
- **Negative, accepted, and this is the largest deliberate limitation in the architecture:** a gated tail lane
  waits for the whole scan, so tail lag during a large initial load equals the scan duration. Mitigations:
  the scan is parallel and resumable, so it is as fast as the lane count allows; `Heartbeater` holds the
  upstream slot; `Build` refuses the combination that would lose data (`PrunesOnCommit` + gated + no
  heartbeat); and the `shadow`-plus-`canal/scan` route is available for operators who need concurrency and can
  accept an ungated tail.
- **Negative, accepted:** a connector wanting Flink-CDC-grade behaviour writes its own key-range planning
  against a helper library rather than getting it from the core. Stated cost of a small core.
- **Negative, accepted:** the `shadow` transform's seen-key set is in-memory and per-run, so it does not
  survive a restart mid-handoff; a restart re-reads the scan lane's uncommitted window and the sink sees
  duplicates. Bounded by the lane budget, counted, and correct for an idempotent sink.

# 0027 — A position may advance without a record, and it goes through the tracker

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

Three hostile connectors independently needed a lane to advance its cursor without producing a record. It is
not an exotic requirement; it is four ordinary situations:

1. **A fully filtered tail.** A CDC tail lane running concurrently with a chunked scan must drop every
   event already covered by a chunk. Thousands of log positions pass, none produces a record, and the
   replication slot must still be released or the upstream retains its WAL for the whole 4 TB scan.
2. **A page of already-seen rows.** A no-cursor REST feed pins ids at a tie-heavy `updated_at` boundary; a
   page can be entirely pinned ids, and the watermark still moved.
3. **A planner lane.** A chunk planner announces work and emits nothing.
4. **A bounded source's final offset.** `internal/example/linefile` — the in-tree example — emits a
   zero-record `EndOfLane` batch on its last `Read`. The architecture document explicitly sanctioned this.

`ledger.Admit` floored both weight and refs at one:

```go
if refs == 0 { refs = 1 }
weight := uint64(b.Len()); if weight == 0 { weight = 1 }
```

So a zero-record batch opened a settlement group with one outstanding reference. `Settle` is keyed on
`record.RecordID` and there were none, so **the group could never settle and every later position in the
lane queued behind it forever.** The example source wedged its own lane on its own final batch.

One connector reported it as major, one as fatal, one as minor — the disagreement being purely about which
of its lanes it happened to notice first.

The proposals split on the fix. Two asked for `Heartbeater.Heartbeat` to carry a position. One asked for the
ledger to treat a zero-record batch as already resolved.

## Decision

**A batch with a `Position` and zero records is legal, meaningful and admitted with ZERO references.** It is
`record.Batch`'s documented contract, `Source.Read`'s documented contract, and `LaneReader.ReadLanes`'s.

**It still goes through the tracker.** `Tracker.TrackResolved(payload) (P, bool)` appends a node that is
already resolved, with zero weight and zero refs, and returns the prefix advance in exactly the shape
`Release` does. It never blocks and takes no `Ticket`, because there is nothing to release later.

The two halves both matter and the second is the one an implementer will get wrong:

- **Zero weight** means an idle lane emitting a position every second costs no budget at all, which is what
  a thousand quiet streams need.
- **Still in the ordered list** means the prefix reaches it only once everything admitted before it has
  resolved. Committing such a position directly — the obvious shortcut — commits past unsettled records,
  which is the one thing the ledger exists to prevent.

`internal/stress/parallel-snapshot/proof_test.go` asserts both: a position-only batch advances the lane
immediately when nothing is outstanding, and a position-only batch admitted *behind* an unsettled record
does **not** advance the lane until that record settles.

**Zero records AND no position is a spin.** The core raises `fault.PermanentContract` on the second
consecutive one rather than burning a core.

## Why not a position on Heartbeat

`Heartbeater.Heartbeat` runs on the **control goroutine**, which by design runs concurrently with a blocking
`Read`. A position arriving there therefore has no defined order against records the read path has already
produced but not yet handed over. Committing it would advance the cursor past unsettled records — the same
failure the zero-reference path is carefully designed to avoid, arriving through a door with no ordering at
all.

The read path already has ordering for free, because that is what a batch *is*. So the answer is a batch,
and `Heartbeat` keeps its narrower job: telling a source that a lane is quiet so it can hold an upstream
session open, and marking `LaneStatus.Idle` so hundreds of healthy quiet streams stop reporting a
forever-rising `CheckpointAge` — this design's own primary alert signal — for the offence of having nothing
to say.

## Consequences

- One fatal, one major and one minor breakage close with no interface change at all: a ledger fix, a tracker
  method and three documented contracts.
- The in-tree example source stops wedging its own lane, which is the clearest possible evidence that this
  was a defect and not a design boundary.
- `LaneStatus.Idle` and `IdleSince` are added to the read model, because "quiet" and "stuck" must not render
  identically.

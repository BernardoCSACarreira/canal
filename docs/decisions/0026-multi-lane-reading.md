# 0026 — Multi-lane reading is one optional interface over a batch of batches

**Status:** accepted, normative. Forced by the hostile-connector stress pass (architecture §30).

## Context

`Source.Read(ctx, dst *record.Batch)` hands the source ONE batch. A batch is bound to one `Allocator`, and
an `Allocator` stamps one `(lane, stream)` pair. `Source` is frozen, so `Read` cannot grow a parameter.

Four of eight hostile connectors hit this and **all four rated it FATAL**:

- a worker holding 32 chunk lanes of a 4 TB scan, plus the CDC tail, in one process;
- a source with 900 runtime-discovered streams multiplexed over one connection;
- an enterprise source scaling 1 → 32 readers per worker;
- a shared-log CDC source whose one cursor interleaves many tables.

The escape hatch that looked available made it worse. `record.Batch.Lane` is an exported, settable field, so
a source could set it to the lane it actually had data for — and the records' `Origin.Lane` still came from
the allocator. The ledger then settled the group under `Batch.Lane` while every `record.Ref`, every
dead-letter row and every fault carried the *other* lane. One connector measured it: **33350 of 33500
records attributed to a lane they did not come from, with no error anywhere.** Silent settlement
corruption.

The alternative the connectors were left with was to honour the batch's lane and starve: one connector
implemented `honour_batch_lane` as a config flag and its own test documents the dilemma — provenance
correct and 31 lanes starving behind one, or throughput reached and every record mislabelled.

## Decision

**One optional interface. `connector.LaneReader`:**

```go
type LaneReader interface {
	ReadLanes(ctx context.Context, dst []*record.Batch) error
}
```

plus **`SourceCaps.ReadsLanes bool`** and **`SourceCaps.ReadConcurrency int`**.

The engine owns one `Allocator` and one `Batch` per lane this instance holds, partitions the held lanes into
at most `ReadConcurrency` **disjoint** groups, and calls `ReadLanes` once per group on its own goroutine.
No lane appears in two live calls; no batch is touched by two goroutines. `ReadConcurrency` is capped by
`spec.Spec.Parallelism`, so the operator's number always wins downward.

`Source.Read` stays frozen and stays what the ninety-percent source implements. A source declaring
`ReadsLanes` still implements `Read` — the required interface is required — and the core never calls it.

**`ledger.Admit` now REFUSES a batch whose records disagree with `Batch.Lane`**, with
`fault.PermanentContract` naming both lanes. Silent settlement corruption becomes a loud contract fault at
the one place every batch passes through.

## Why this shape, of the four proposed

Three different shapes were proposed by the four connectors, and one more by a fifth. This is the
comparison, because the choice is not obvious.

**`ReadLane(ctx, lane, dst)` — one call per lane.** Fits the 32 independent chunk readers perfectly: the
engine runs N goroutines, each blocking on its own connection. **Unusable for the 900 multiplexed
streams**, where one upstream connection decides which lane the next record belongs to. There, 900
goroutines each blocking on one shared connection is not an implementation; and with
`MaxReadConcurrency: 1` the engine picks which lane to ask for, and the source cannot say "I have data for
lane 7 right now" — which is the only thing it knows.

**`ReadLanes(ctx, dst []*Batch)` — one call, many batches.** Fits the multiplexed source perfectly. With
`ReadConcurrency` it also fits the independent one exactly: declaring 32 with 32 lanes produces 32
concurrent calls each carrying a 1-element slice, which *is* the per-lane shape. **Chosen.** One interface
covers both, and the degenerate case of the general form is the specific form.

**`(*Batch).Retarget(lane, stream)` — mutate the batch's allocator.** **Rejected.** It breaks the property
that makes an `Allocator` safe: one allocator per `(lane, generation)` with ids that do not repeat within a
lane. Retargeting means one id space shared across lanes, so `RecordID` stops being unique per lane, and it
makes concurrent multi-lane reading impossible because there is still only one allocator to mutate. It also
preserves the exact failure mode being fixed — a source that forgets to call it mislabels silently.

**`Batch.AddFor(stream)` — per-record stream within one lane.** **Also adopted, for a different problem.**
It is orthogonal: `ReadLanes` gives many lanes, `AddFor` gives many streams within one lane. A shared-log
CDC source needs the second and not the first, because it genuinely has one cursor.

## Consequences

- Four fatal breakages close with one optional interface and one caps int.
- A source that does not need it is unaffected: `Read` unchanged, `ReadsLanes` false, zero new fields set.
- The engine gains real work: an allocator and batch pool per held lane, and a partitioner. That work is
  the core's, which is the correct place for it — every one of the four connectors was writing a worse
  version of it in userland.
- `Batch.Lane` remains exported and settable, because `NewBatch` sets it and the engine's reframing nodes
  read it. It is now documented as core-owned, and misuse is a loud fault instead of silent corruption.

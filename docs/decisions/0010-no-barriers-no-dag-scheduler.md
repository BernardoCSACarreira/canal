# 0010 — No barrier protocol and no general DAG scheduler

**Status:** accepted, normative.

## Context

Flink's checkpointing is the most thoroughly verified body of evidence available, and its lesson is
specifically about *what it is paying for*. The barrier protocol delivers three properties:

1. atomic position-plus-state under one monotonic id;
2. multi-input alignment;
3. consistency across a shuffled graph at 1000-subtask scale.

Properties 2 and 3 are what the entire aligned / unaligned / in-flight-persistence / buffer-debloating
apparatus exists to serve — two major features (FLIP-76 and buffer debloating) were spent undoing an early
deep-buffering default, and the result is an operator-facing matrix whose sharp edges are documented ("aligned
checkpoints are not relocatable"; "you cannot upgrade minor versions from an unaligned checkpoint").

canal needs property 1. It does not have a shuffled graph.

The temptation is real, though: `Committer`, `WriterState` and `StatefulTransform` all imply a coordinated
snapshot, and one reviewed proposal noted that "briefly stop admitting records" is a latency spike whose cost
is proportional to worker count.

## Decision

**No barriers. No alignment. No in-flight-record persistence. No shuffle.**

Property 1 is obtained directly: `{lane cursors, schema epoch, writer state, transform state, committables}`
are written as **one durable record under one monotonic id** through an atomic `store.StateStore.Set`.

**Quiesce-then-snapshot is the mechanism where a coordinated moment is genuinely needed** — before applying a
DDL, and before a checkpoint that includes a `Committer`'s committables. The engine stops admitting, drains
in-flight records for the affected scope, flushes, acts, writes, resumes.

**The interface is kept barrier-shaped without building barriers.** `SnapshotState(ctx, id)` exists on every
stateful node (`WriterState`, `StatefulTransform`) and the checkpoint carries a monotonic id, so a real barrier
protocol could be inserted later with **zero connector changes**. That is the cheap insurance; building the
protocol is not.

**Multi-input consistency is scoped honestly.** Fan-in across lanes is expressible — two source nodes feeding
one sink node — but **fan-in with shared state across lanes is not**, and that is stated rather than implied.
A transform that must aggregate across two lanes' records is refused at submit time, because the ordering scope
is per lane and nothing in the design makes a cross-lane ordering claim.

**Deep buffering is refused at the same time and for the same reason.** Every edge holds two batches. The
more you buffer, the worse your checkpoint latency and recovery time, and Flink's own history is the proof.

## Alternatives rejected

- **A barrier protocol.** Rejected: it buys properties canal does not need and costs a subsystem plus an
  operator-facing matrix. If canal ever grows a shuffle, `SnapshotState(ctx, id)` is already the seam.
- **A general DAG scheduler with operator chaining, slot sharing and backpressure propagation across a
  shuffle.** Rejected: canal's graph is a pipeline. The reviewed evidence's own verdict was "do not pay for
  this".
- **Cross-lane transactional consistency** (one atomic commit spanning independently-ordered lanes). Rejected:
  it would make every lane's progress depend on every other lane's, which is precisely the failure Airbyte hit
  with per-stream state under CDC and had to retrofit a global tier for — except canal's lane model already
  expresses both cases, because a source with one shared log position simply announces one lane.
- **Unaligned checkpointing as a future option.** Rejected pre-emptively: it is the branch of the matrix whose
  sharp edges are documented, and never creating the matrix is cheaper than navigating it.
- **Multiple snapshot formats** (canonical / native, aligned / unaligned). Rejected: exactly one checkpoint
  format, always self-contained and relocatable.

## Consequences

- Positive: the entire aligned/unaligned subsystem, its config surface, its operator documentation and its
  upgrade constraints do not exist. Recovery is "read one record, hand blobs back". One format, always
  relocatable.
- **Negative, accepted:** the quiesce before a DDL or a 2PC checkpoint is a latency spike proportional to
  in-flight depth, and in a multi-worker deployment it is coordinated through the durable lane table rather than
  instantaneously, so it is proportional to worker count as well. Mitigated by shallow buffering (two batches
  per edge) making the drain short by construction, and by DDL being rare.
- **Negative, accepted:** fan-in with shared state is not expressible. Stated at submit time with a diagnostic
  rather than discovered.
- **Negative, accepted:** throughput is more sensitive to per-node latency than a deeply buffered pipeline
  would be. Deliberate: it is the trade Flink made in the other direction and then spent two features undoing.

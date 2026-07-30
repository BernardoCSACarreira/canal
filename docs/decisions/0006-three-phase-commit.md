# 0006 — Commit is three-phase, and upstream retention is a declared capability

**Status:** accepted, normative.

## Context

R4 says an acknowledgement means durable. Every reviewed proposal read that as a two-phase handshake: the
sink confirms durability, then the checkpoint advances. That reading is incomplete, and the gap it leaves is a
confirmed severity-zero data-loss mechanism that survived years in a mature shipping system.

Conduit's engine sent the acknowledgement to the source plugin and *then* enqueued the durable position
write. From its own postmortem: for a plugin whose upstream **prunes** data once told to commit — a Postgres
logical replication slot, where acking advances `confirmed_flush_lsn` and frees WAL for recycling — a crash
inside that window causes a resume from a durably-persisted position that is now *behind* data the upstream
has already discarded. A structural, unrecoverable gap.

Two things make this the most transplantable lesson in the evidence base:

1. **"Advance the checkpoint" and "tell the source" are two actions, and the ordering between them is a
   correctness property.** Every proposal treated them as one.
2. **The guarantee's validity depends on a property of the *upstream* that no interface expressed.** Postgres
   slots prune on commit; Kafka, MySQL binlog and Mongo change streams are retention-based and do not. *The
   same engine code is a data-loss bug for one class of source and completely benign for the other*, and the
   postmortem had to classify each connector by reading its source code.

Separately, the fix for that bug created a deadlock six days later on the same seam, because the deferred
acknowledgement was delivered inline from the persister callback and a snapshot iterator gating its own
completion on every emitted record being acked turned a dropped deferred ack into a permanent handoff
deadlock plus silent post-snapshot loss.

## Decision

**The commit protocol is three-phase.**

```
1  the sink confirms durability                 (Write returns clean, or Flush covers the request)
2  canal's OWN record of the position is written AND FLUSHED  (store.StateStore.Set, atomic, epoch-fenced)
3  ONLY THEN is the source told it may advance  (Source.Commit(ctx, Ack), and only for a lane still held)
```

**`SourceCaps.UpstreamRetention` is a required declaration** with no valid zero value:

```go
type Retention uint8
const (
    RetentionUnknown Retention = iota // rejected at registration
    PrunesOnCommit                    // the upstream DISCARDS on commit
    RetentionWindow                   // bounded retention regardless
    RetentionUnbounded                // never discards
)
```

`Build` **refuses** a `PrunesOnCommit` source against a deployment with no durable `StateStore`, and refuses
the combination `PrunesOnCommit` + a gated lane + no `Heartbeats` (because a gated tail lane is not being
read, so only a heartbeat can hold its slot without pinning the log for the whole scan).

For `RetentionWindow` and `RetentionUnbounded` sources the ordering is a *latency* question rather than a
correctness one, but the protocol does not branch: it is three-phase for everyone, because a protocol with a
correctness-relevant branch is a protocol that will be got wrong.

**Phase three runs on a dedicated per-source goroutine**, never inline from the persister callback, because a
slow connector would otherwise block the process-wide flush cycle. It carries a bounded retry policy and a
teardown flag that suppresses escalation during shutdown. That mechanism is designed in here, not retrofitted
after the deadlock.

**The enforcement site carries the citation:**

```go
// Invariant 1: the source is told to advance only after StateStore.Set has
// flushed. A pruning upstream's own commit must never precede canal's
// crash-recoverable record of that position. See docs/decisions/0006-*.md.
// Do not reorder.
```

**A `Commit` error is escalated, not logged.** The engine classifies it, retries per policy, and raises
`Condition{Degraded, True, reason: commit_failed}`. Benthos's actual behaviour — `_ = ackFn(...)` — silently
loses progress *after* delivery, and it is unreachable here.

**`Commit` returns only `error`, not a position.** There is nothing to negotiate: phase two already persisted
the cursor, so the read model's `Committed` never reports progress that was not persisted, and a source that
declines to advance because `Abandoned > 0` simply does not advance its own upstream.

## Alternatives rejected

- **Two-phase (sink → source, persist later).** Rejected: it is the sev-0.
- **Two-phase (sink → persist, and never tell the source).** Rejected: a source whose progress lives upstream
  — a consumer group, an SQS delete, a slot — must be told, and that is also the property that lets the data
  plane run with canal's control plane down.
- **Branching the protocol on `UpstreamRetention`.** Rejected: a correctness-relevant branch will be got
  wrong, and testing the benign path proves nothing about the dangerous one.
- **Inferring the retention class from the connector name or a heuristic.** Rejected: that is exactly what the
  postmortem had to do by hand, and it is not a mechanism.
- **A wall-clock commit timer.** Rejected: Kafka Connect's `offset.flush.interval.ms=60000` decoupled from
  batches re-emits fully-acked snapshot chunks after a crash and produces the ecosystem's most misdiagnosed
  log line. Flushes here are triggered by the prefix advancing to a new `Safe` position, thresholds, idleness,
  `EndOfLane`, a schema change or a drain — never by a clock alone.
- **Delivering phase three inline from the flush callback.** Rejected: it is the deadlock.

## Consequences

- Positive: the sev-0 is structurally absent; the dangerous connector class is declared rather than inferred;
  the read model's committed watermark cannot lie; a commit failure is loud.
- **Negative, accepted:** phase three lags phase one by up to one flush interval, so a pruning upstream's
  retention release is delayed by that much. Bounded and tunable (`state.flush_interval` 1s,
  `state.flush_records` 10000), and made visible by `canal_state_persist_staleness_seconds`.
- **Negative, accepted:** a `PrunesOnCommit` source cannot run against a deployment with no durable state
  store, so the "zero dependencies" story requires bbolt rather than truly nothing. Acceptable: `canal run`
  ships bbolt.
- **Negative, accepted:** one extra goroutine per source, and a per-source retry policy to configure. Both are
  small and both are the cost of not repeating two postmortems.

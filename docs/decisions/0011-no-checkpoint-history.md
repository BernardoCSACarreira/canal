# 0011 — One cursor per lane; no checkpoint history

**Status:** accepted, normative.

## Context

The decision-space document specified a checkpoint carrying a monotonic `CheckpointID` and a
`Committables map[CheckpointID][]VersionedBlob`, which **implies retained history without arguing for it**.
Its own addendum flagged the gap: Conduit keeps exactly one position per connector and names precisely what
that forbids — no log replay, no WAL of in-flight records, and no "replay from checkpoint N−3" facility of any
kind. Anything read but not durably acked is re-read from upstream.

Retaining history is not free. It implies a retention policy, a compaction policy, a restore UI, a decision
about what a "restore to checkpoint 4" does to a source whose upstream has since pruned, and a second class of
operator mistake.

## Decision

**Exactly one durable state record per pipeline generation, holding one cursor per lane.** There is no
checkpoint history, no `savepoint` concept and no restore-to-a-past-id operation.

The `Committables map[uint64][]Committable` is the one exception, and it is bounded by the Checkpoint
Subsuming Contract rather than by a retention policy: ids strictly increase, a higher id subsumes every lower
one, and on confirm everything up to and including that id is published and removed. Plus
`Committable.Expires`, after which `AbortStale` runs and `Condition{Degraded, True}` is raised — so the map
cannot grow silently.

**What replaces "restore to an earlier checkpoint":**

| Operator need | Mechanism |
|---|---|
| re-read from a known position | `PATCH /v1/pipelines/{id}/offsets` with a connector-supplied token, while the pipeline is stopped. Audited, bumps `Generation` |
| re-read one lane from the start | `DELETE /v1/pipelines/{id}/offsets/{lane}` |
| re-read everything | `DELETE /v1/pipelines/{id}/offsets` |
| "what would we replay after a crash" | `canal_lane_replay_records`, computed and exported continuously |
| roll back a bad config | the config store is revisioned; put the previous revision |

**Restart is: read the one record, hand each lane its `Spec` and `Cursor` back verbatim.** Nothing else. And
at restart the core compares the cursor's age against `SourceCaps.ReplayWindow` when declared, refusing with
"source guarantees 6h; this cursor is 9h old" rather than silently starting a lossy stream.

## Alternatives rejected

- **Retained checkpoint history with a retention policy.** Rejected: it is a feature nobody asked for with a
  policy surface, a compaction path, a restore UI and a new class of operator error. The goals list restart
  and replay, and offset editing plus a revisioned config store covers both.
- **Savepoints as a distinct artefact.** Rejected: it creates exactly the format matrix ADR 0010 refuses.
  There is one format, always relocatable, so "a savepoint" is just a copy of the record.
- **Merge-patch checkpoints** (a source with thousands of lanes re-serialising all state per commit is a real
  scaling problem, and this is Estuary's stated motivation). Rejected for now, on the grounds that the per-lane
  decomposition already avoids most of it: a phase-two write touches only the lanes whose prefix advanced, not
  every lane. Revisit only if measured per-lane blobs prove too large.
- **A WAL of in-flight records so nothing is ever re-read.** Rejected: that is a durable buffer, and ADR 0001
  explains why no shipped buffer is cluster-durable. Re-reading a bounded window is the correct cost.
- **Coupling the cursor into the pipeline's config record** (Conduit stores the position *inside* the
  connector-instance record: one write, atomic, no separate offset store). Genuinely the cheapest possible
  implementation, and rejected because it makes a config write and a position write contend on the same row and
  a corrupt record lose both — a coupling that document itself flags as needing an explicit decision. `ConfigStore`
  and `StateStore` are separate interfaces and may be separate backends.

## Consequences

- Positive: recovery is one read; there is no retention policy, no compaction, no restore UI and no format
  matrix; "how much do I re-read" is a live metric rather than a function of retained history.
- **Negative, accepted:** there is no "undo" for a bad pipeline run. The remedy is offset editing, which is
  audited and requires the pipeline to be stopped.
- **Negative, accepted:** a source whose upstream has pruned past the stored cursor cannot recover, and canal
  can only refuse loudly. That refusal (against declared `ReplayWindow`) is strictly better than the
  alternative of starting a stream with a silent gap.
- **Negative, accepted:** a source with very many lanes writes a large record when many lanes advance together.
  Measured; per-lane keys mean only the advancing lanes are written; merge-patch remains available if
  measurement demands it.

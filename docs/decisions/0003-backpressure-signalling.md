# 0003 — Backpressure is admission blocking, and a full sender is told a typed fault

**Status:** accepted, normative. Closes `design-rules.md` open decision 3.

## Context

The abandoned attempt returned `202` after appending to an unbounded, unsynced in-memory slice, and its
connector RFC instructed adapters to advance their upstream checkpoint on that `202`. That is R4's original
violation, and it is also a backpressure failure: there was no expressible outcome other than success.

The prior art is a warning. Kafka Connect's source path calls `poll()` again the instant the previous batch
reaches the producer, with a hardcoded 100ms as the only pause, and its `SubmittedRecords` deques are
unbounded with the pathology named in its own javadoc. Benthos accumulated **five** bounding mechanisms
(`checkpoint_limit`, output `max_in_flight`, input `max_in_flight`, `Capped`, buffer `limit`) under three
user-facing names, with one documented deadlock between two of them and no single place to observe "why is
this pipeline slow". Flink chose two *batches* of buffering and then spent two major features undoing an
early deep-buffering default.

## Decision

**1. One accounting concept: the lane budget, enforced by `Ledger.Admit` blocking.** It is simultaneously the
in-flight bound, the backpressure trigger, and an input to the exported replay window. There is no credit
protocol, no separate semaphore, and no second knob.

**2. Every edge is bounded**, capacity measured in **batches**, defaulting to 2. Slowness is transitive by
construction. In Go this is a buffered channel plus `select` on `ctx.Done()`; Flink needed ~400 lines to
express it.

**3. What each kind of sender is told:**

| Sender | What happens | What it observes |
|---|---|---|
| **pull source** (file, query, log tail) | blocks inside `Ledger.Admit` | nothing directly. `LaneStats.Blocked`, `BlockedFor`, `canal_node_blocked_seconds_total`, `Condition{Backpressured, True}` naming the blocking node. `LaneCtl.Budget(lane)` is available if it wants to size its reads |
| **push source** (HTTP ingress, socket) | `Admit` returns `fault.New(TransientInternal, OpBuffer, …)` with `RetryAfter` set | a typed, classified fault it must surface. The built-in HTTP source renders it as `503` plus `Retry-After` |
| **buffer** | `Put` returns `Accepted{Records, Refused}` | the engine applies `WhenFull` |

**4. `WhenFull` has four members and no unbounded one:**

- `Block` (default) — apply backpressure upstream.
- `DropNewest` — discard the incoming records and **count** them
  (`canal_buffer_refused_total{reason="drop_newest"}`). **Newest, never oldest**: dropping the oldest would
  discard data whose prefix the source may already have been told is safe, breaking the cursor invariant.
- `Reject` — settle the affected group `Abandoned` so the source sees `Ack.Abandoned > 0` and decides for
  itself.
- `Overflow` — spill to the chained buffer.

**Silent loss is unconfigurable.** Every drop is counted, every rejection is visible in the ack.

**5. The one thing that never happens is a success for data that is not yet durable.** That is the rule R4
exists for, and it is why the push-source path returns a fault rather than a `202`.

**6. Soft and hard backpressure are separate diagnoses and separate series.**
`canal_node_blocked_seconds_total` is hard blocking; `canal_buffer_depth_records` approaching capacity is
soft. Plus `canal_node_utilization_ratio` as the bottleneck finder.

**7. The upstream-retention tension is documented at the knob, not discovered.** A source that stops reading
may cause the source system to accumulate WAL or retain a replication slot. `Heartbeater` is part of the
answer; the other part is that `canal_checkpoint_age_seconds` alarms before the upstream fills, and the
`lane_budget` field's own description says so.

## Alternatives rejected

- **Several bounding knobs, as Benthos has.** Rejected: one documented deadlock between two of them, three
  names for overlapping concepts, and no single observable. One number that is simultaneously the bound, the
  trigger and the disclosed replay window is strictly better and is R9.
- **Adaptive request concurrency in core.** Rejected for v1: it is a control loop with its own failure modes,
  and `MaxConcurrency` plus `Retry-After` covers the cases that matter. It can be added as sink-side policy
  later without touching the interface.
- **Dropping the oldest on a full buffer.** Rejected: it breaks the acked-prefix invariant.
- **An unbounded or "best effort" buffer mode.** Rejected: R6.
- **Telling a pull source it is blocked, via a callback or channel.** Rejected: there is nothing useful it can
  do that `Budget()` does not already allow, and it adds a plugin-surface concept for no behaviour change.
- **Returning `202` from a push ingress and buffering.** Rejected: R4.

## Consequences

- Positive: one knob, one blocking point, one observable set. Unbounded growth is inexpressible. Every drop is
  attributable.
- **Negative, accepted:** a slow sink makes a pull source stop reading, which can pin upstream retention. This
  is a real trade and it is documented at the `lane_budget` field, surfaced by `checkpoint_age`, and mitigated
  by `Heartbeater`.
- **Negative, accepted:** an HTTP ingress under sustained backpressure returns 503s, which some clients handle
  badly. The alternative is lying, so this is the correct failure.
- **Negative, accepted:** with an edge capacity of 2 batches, throughput is more sensitive to per-node latency
  than a deeply-buffered design. Accepted deliberately: deep buffering costs checkpoint latency and recovery
  time, and choosing small bounded buffers now removes the subsystem that would later undo it.

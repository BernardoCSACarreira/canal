# 0012 — Dead-lettering is an edge to an ordinary sink, not a store

**Status:** accepted, normative.

## Context

R7's remedy requires a terminal disposition that is not "drop", and the failure taxonomy canal inherits
prescribes "dead-letter without poison-retrying" for the permanent-mapping class. Kafka Connect has a
framework DLQ with rich provenance headers plus `ErrantRecordReporter.report(rec, err)` so a sink can reject
individual records without failing the pipeline — but **only for sinks**. Conduit's DLQ circuit breaker
defaults to stopping the pipeline on the first nack, and its default "DLQ destination" is a log line; its UI
can show DLQ *configuration* and nothing else, because the store is explicitly deferred.

Conduit also shipped a related mistake worth avoiding: three `unwrap` processors whose only job is to un-nest a
record that arrived as bytes containing an encoded record, caused in part by the DLQ re-wrapping the original
under a payload field.

The open question the addendum raises is whether canal needs a bounded, queryable DLQ store, given that the
frontend goal says "what failed and why".

## Decision

**A dead-letter route is `spec.Edge{From: node, Select: EdgeFailed}` to an ordinary sink node.** There is no
DLQ interface, no DLQ store, no DLQ component kind and no DLQ code path in the engine beyond edge selection.

**It works for sources as well as sinks**, which Kafka Connect's sink-only reporter does not: a record that
fails decode, transform, encode or write carries its fault and routes on the failed edge of the node that
raised it.

**The record is carried in an envelope, never nested inside a payload field.** The envelope carries: the full
`Origin` (tenant, pipeline, node, lane, stream, key, upstream id, read time, root and parents), the fault's
class, blame, op, and both message audiences, the attempt count, the node id, the redacted config revision, and
the original payload as bytes plus the original `Meta`. One explicit shape, so no `unwrap` processor is ever
needed.

**Settlement follows the DLQ write.** The engine settles a dead-lettered record `Abandoned` **only after the
DLQ node's `Write` returns clean**. If the DLQ write fails, nothing is settled: the group stays pending, the
prefix stalls, `canal_oldest_pending_age_seconds` climbs, `GroupTTL` raises `Condition{Degraded, True}` naming
the group, and the lane's budget fills so the source stops reading. **The pipeline stalls loudly instead of
losing data quietly.**

**The frontend answers "what failed and why" from the read model, not from a store:** `LastFault{class, blame,
user, dev}`, `RecentEvents`, and `canal_faults_total{node,op,class,blame}`. Individual records are found in
whatever sink the operator routed them to — which is usually a warehouse they already query.

**`TerminalDeadLetter` with no `EdgeFailed` route is refused at submit time.** A dead letter with nowhere to go
is not a configuration, it is a silent drop.

## Alternatives rejected

- **A `DeadLetter` interface on the runtime** (`rt.DeadLetter(ctx, r, f) error`). Rejected: it is a *third*
  durability edge for the core to reason about, and one reviewed proposal was found fatal partly because
  durability was inferred from a nil return at three unconstrained edges including this one. Making the DLQ an
  ordinary sink means it is subject to exactly the same `WriteResult` contract and the same reconciliation
  check as any other sink.
- **A built-in bounded queryable DLQ store.** Rejected: it is a database, with a retention policy, a query
  language, an index and a UI. The frontend goal is satisfied by the read model, and the records themselves
  belong wherever the operator already looks.
- **A framework DLQ topic or file with a fixed format.** Rejected: it would be a connector the core is
  privileged to know about, which is constraint #1.
- **Defaulting to a log line** (Conduit's default). Rejected: R4's spirit. A record whose only trace is a log
  line is a lost record.
- **Dropping by default.** Rejected: `TerminalDrop` exists and is counted, but it must be *chosen*.
- **Nesting the original record under a payload field.** Rejected on the `unwrap`-processor evidence.

## Consequences

- Positive: no DLQ machinery in the core; the DLQ is subject to the same durability contract as any sink; it
  works for source-side failures; the envelope needs no un-nesting; a failed DLQ write cannot lose data.
- **Negative, accepted:** there is no "browse the dead letters" screen. The operator queries the sink they
  chose. Stated cost of a small core.
- **Negative, accepted:** an operator who forgets to route `EdgeFailed` gets a submit-time refusal rather than a
  working pipeline with a hidden drop. Intentional.
- **Negative, accepted:** dead-lettering costs a full sink write on the failure path, including its retry
  policy, so a burst of mapping failures is a burst of DLQ writes. Bounded by the same batching and concurrency
  policy as any sink node.

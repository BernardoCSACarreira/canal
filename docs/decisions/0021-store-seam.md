# 0021 — Four store interfaces; Postgres first; Kafka rejected

**Status:** accepted, normative.

## Context

canal must run as a single binary on a laptop with nothing installed, and as a coordinated multi-worker
deployment on Kubernetes, with **the connector-facing API byte-identical in both**. Kafka Connect and Flink
converged on that requirement independently, and Kafka Connect's `OffsetBackingStore` — `ByteBuffer` in,
`ByteBuffer` out — is the single idea that makes its standalone/distributed swap free.

Kafka Connect also supplies the coordination substrate to avoid. `KafkaConfigBackingStore` is a single-partition
compacted topic used as a replicated state machine, needing **seven record types and a `commit-{id}` marker to
fake set-atomicity**, and its own javadoc documents an **unrecoverable** state: compaction plus a partial write
can leave a `commit-foo (2 tasks)` whose `task-foo-1-config` has been compacted away, "leaving us in an
inconsistent state with no obvious way to resolve the issue" — so the class has to expose which connectors are
inconsistent for the herder to paper over.

And the verified caveat that governs everything about leadership: `k8s.io/client-go`'s leaderelection package
documents that its implementation *"does not guarantee that only one client is acting as a leader (a.k.a.
fencing)"*, and clients infer leadership from **locally captured timestamps**.

## Decision

**Four interfaces. If a fifth appears, the abstraction is wrong.**

```go
ConfigStore  // revisioned CAS plus Watch, over spec.Spec
StateStore   // bytes in, bytes out; Set atomic across the whole map, per-key CAS, epoch-fenced
Coordinator  // membership, leader election, assignment leases
StatusStore  // per-worker status in, one aggregated document out
```

| Interface | `canal run` | `canal serve` |
|---|---|---|
| `ConfigStore` | bbolt file, or a YAML file projected in read-only | Postgres |
| `StateStore` | bbolt file — one `Set` is one transaction | Postgres table with a version and an epoch column |
| `Coordinator` | `single{}`: always leader, every lane local, leases no-ops with epoch 1 | `pg_try_advisory_lock`, a leases table, `SELECT … FOR UPDATE SKIP LOCKED` |
| `StatusStore` | in-process | `worker_status` rows with a TTL |

**`StateStore` never sees a domain type**, and neither does `Coordinator`: `LaneRow.Spec` is a `record.Blob`, so
no store implementation ever serialises a connector type and a lane spec's encoding stays versioned by the
engine.

**`StateStore.Set` must be atomic across the whole map, with per-key CAS and per-key epoch fencing.** One SQL
transaction, one bbolt transaction, one etcd `Txn`. `StoreCaps` reports it, and `Build` **refuses a guarantee
tier the deployment's store cannot support** — so the deployment's own stores are part of what is validated.

**Kafka as the coordination store is explicitly rejected.** Reproducing a documented unrecoverable state in 2026
with a transactional database available would be perverse.

**Postgres first; etcd second and only as a conformance target.** One dependency delivers every primitive:
revisioned CAS (`UPDATE … WHERE version = $2`), atomic multi-key writes (one transaction), leader election
(`pg_try_advisory_lock`, released on session loss), leases (a table plus a conditional update), work claiming with
no leader (`SKIP LOCKED`), `Watch` (`LISTEN`/`NOTIFY` with a `max(revision)` poll fallback), and status
aggregation (a table with a TTL). etcd is a *better semantic* fit — `ModRevision` CAS and `Watch(fromRevision)`
are literally the interface — so it exists as a second implementation whose purpose is to stop the interface
acquiring Postgres-isms, not as a shipped dependency.

**The leader only plans.** It writes assignment rows. It does not route data, proxy status, hold a checkpoint or
own anything the data path reads. **Therefore the data plane keeps running and keeps checkpointing with the
entire control plane down**, because a worker holding a valid lease needs nothing from anyone until it expires.
This is the single most important deployment property in the design and it is worth sacrificing elegance for.

**Leadership is never trusted for correctness.** The lease **epoch** is the fencing token and `StateStore`'s
per-key CAS is the second fence. That is the only safe use of leader election given the verified caveat.

**Reassignment is deliberately delayed**: lease TTL 30s, reassignment delay 120s, both configurable, with
exponential backoff on consecutive revoking rounds so a bouncing worker reclaims its own lanes. There is **no
rebalance protocol at all**, because there is nothing global to agree on: assignment is per-lane, claimed by
lease, and the plan is durable state.

**`canal run` with no arguments produces a working pipeline with a durable checkpoint on a laptop with nothing
installed.** That is R3's first milestone and canal's biggest adoption lever.

**Root-config / pipeline-config split**: service-wide observability, stores and auth in one file; N pipeline
documents managed as data over HTTP.

## Alternatives rejected

- **Kafka as the coordination substrate.** Rejected on its own documented unrecoverable state.
- **A fifth interface for dedupe.** Rejected: dedupe uses `StateStore` under a reserved key space, which is what
  makes its seen-mark atomic with the cursor (ADR 0002). A separate store would be a separate durability domain
  and would make that atomicity impossible.
- **A fifth interface for a DLQ store.** Rejected — ADR 0012.
- **Dropping `Coordinator` and putting all coordination above the binary**, with only a fencing token on every
  write (Conduit's ADR argues the concept itself is the surface that gets grown). Genuinely attractive, and
  rejected because canal's goals include coordinated multi-worker deployment as a shipped feature: without
  `Coordinator` the lane-claiming protocol would have to be reinvented by whatever runs above, and the
  byte-identical-API property would be lost at exactly the seam it matters most.
- **etcd first.** Rejected: better semantics, worse adoption. Most enterprises already run Postgres; requiring
  etcd for a data-movement tool is a deployment tax.
- **A leader that routes data or aggregates status on the data path.** Rejected: it destroys the
  survives-control-plane-outage property.
- **Trusting leader election for correctness.** Rejected on the verified caveat.
- **A stop-the-world rebalance.** Rejected: it produced documented rebalance storms taking minutes to stabilise,
  and its incremental replacement shipped its own imbalance bug.
- **Sharing one durability domain between config and state.** Rejected: a config write and a position write would
  contend, and a corrupt record would lose both.

## Consequences

- Positive: the standalone/coordinated swap is four constructor arguments; the data plane survives a total control
  plane outage; there is no rebalance protocol to get wrong; the deployment's own stores are validated at submit
  time; `canal run` needs nothing installed.
- **Negative, accepted:** Postgres is a real dependency for coordinated mode. Judged the smallest possible one for
  what it delivers.
- **Negative, accepted:** a lapsed lease costs up to one in-flight window of re-delivery, and reassignment is
  delayed 120s so a genuinely dead worker's lanes idle for that long. Both are named, configurable and disclosed.
- **Negative, accepted:** maintaining an etcd implementation nobody ships is real work. It is the mechanism that
  keeps the interface honest, and it doubles as the conformance target for `StoreCaps`.
- **Negative, accepted:** four interfaces means four fakes for tests and four implementations per backend. The
  hard cap at four is what stops that becoming eight.

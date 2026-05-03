# RFC: Phase 1 — local buffer and checkpoint provider capability matrix

**Status:** draft (Phase 1 scope)  
**Parent:** [CAN-24](/CAN/issues/CAN-24)  
**Ticket:** [CAN-26](/CAN/issues/CAN-26)  
**Related:** [CAN-2](/CAN/issues/CAN-2) phasing · [CAN-5](/CAN/issues/CAN-5) charter stack · [CAN-7](/CAN/issues/CAN-7) bus neutrality

## 1. Purpose

Define a shared vocabulary and **capability matrix** for two ingestion control-plane surfaces:

1. **Buffer** — where records wait between accept and downstream commit (backpressure, batching, retries).
2. **Checkpoint** — where consumer progress is stored for restart, replay, and scale-out coordination.

The matrix lets backend and platform engineers pick implementations (local defaults in Phase 1 vs remote buses later) **without reopening product scope**: rows encode what the product may assume; gaps are explicit non-goals for Phase 1.

## 2. Evaluation dimensions

Each provider (buffer or checkpoint) is scored on the same five dimensions.

| Dimension | Question we answer |
|-----------|-------------------|
| **Ordering** | What ordering guarantees exist per key, partition, or connection? |
| **Retention** | How long data or progress metadata survives; what trims it? |
| **Replay** | Can consumers re-read from an earlier position; what anchors replay? |
| **Tenancy** | Isolation model (single tenant, logical namespaces, IAM-bound). |
| **HA class** | Availability and durability posture (single node, AZ, regional, multi-region). |

**Legend for matrix cells**

- **P1-local** — Phase 1 default we implement in-repo for dev / single-node MVP.
- **Kafka-class** — Apache Kafka–compatible semantics (log segments, consumer groups, offsets).
- **Pub/Sub** — Google Cloud Pub/Sub–style publish–subscribe.
- **Event Hubs** — Azure Event Hubs / AMQP partition consumer model.

## 3. Buffer provider matrix

| Dimension | P1-local default | Kafka-class | Pub/Sub | Event Hubs |
|-----------|------------------|-------------|---------|------------|
| **Ordering** | Best-effort FIFO per logical partition; no cross-partition order | Strong per-partition total order in a single topic partition | Ordering supported only where product enables ordering keys; otherwise best-effort | Per-partition order within a consumer group partition |
| **Retention** | Bounded in-memory and/or spool directory; process restart clears unless spilled to disk | Topic retention time/size; compacted topics optional | Subscription-based retention ack deadline; topic retention policies | Capture / retention by time or size per event hub |
| **Replay** | Replay from earliest buffered sequence still on disk; no multi-consumer history fan-out | Arbitrary offset replay per partition | Replay via seek or snapshot semantics where supported; generally shorter replay windows than Kafka | Offset-based replay per partition; retention window bound |
| **Tenancy** | Single OS user / single cluster namespace in dev | Cluster + ACLs + quotas | Project + topic + IAM | Namespace + consumer groups + RBAC |
| **HA class** | Single process (no HA); optional disk durability | Clustered brokers, rack/zone aware | Managed regional service | Managed PA / geo DR options |

**Phase 1 product stance (buffer):** ship **P1-local** only. Remote rows document **compatibility targets** for Phase 2+ so adapters do not bake in Kafka-only APIs at the buffer edge.

## 4. Checkpoint provider matrix

| Dimension | P1-local default | Kafka-class | Pub/Sub | Event Hubs |
|-----------|------------------|-------------|---------|------------|
| **Ordering** | Checkpoints monotonic per consumer id; concurrent writers resolved last-write-wins unless fenced | Offset commits per partition in consumer group coordinator | Ack ids / lease extensions; ordering tied to subscription | Checkpoint store (blob) + partition ownership epoch |
| **Retention** | Local file or embedded store; compact to latest offset per (stream, group) | Offsets stored in `__consumer_offsets` with compaction | Ack state retained until deadline or ack | Checkpoint blobs retained until overwritten |
| **Replay** | Restart from last successful checkpoint; explicit “reset” is destructive to local state | Seek to committed or arbitrary offset | Time-based seek or replay from backlog snapshot | Offset or sequence-based repositioning |
| **Tenancy** | Single tenant file layout | Cluster-scoped with ACLs | IAM at subscription | Namespace + storage account isolation |
| **HA class** | None; checkpoint loss = reprocess from upstream replay if buffer allows | Coordinator HA via Kafka cluster | Managed | Managed with storage redundancy |

**Phase 1 product stance (checkpoint):** ship **P1-local** with **at-least-once** processing as the baseline; idempotent sinks remain an application concern unless a later phase adds exactly-once contracts.

## 5. Phase 1 implementation contract

Backend MAY assume the following for Phase 1 code paths:

1. **Buffer** exposes: append, read cursor, trim-after-commit, and metrics (depth, oldest age). Ordering matches **P1-local** row unless a feature flag selects a remote driver.
2. **Checkpoint** exposes: load, save, and fence (optional noop in P1-local). Restart always loads the last successful checkpoint before consuming new data.
3. **Adapter boundary:** buffer and checkpoint are interfaces; Kafka / Pub/Sub / Event Hubs columns are **conformance targets** for future `Driver` implementations, not shipped Phase 1 dependencies.

## 6. Open questions (non-blocking for skeleton)

- Exact on-disk layout for P1-local buffer spool (file per partition vs single WAL).
- Whether P1-local checkpoint merges with buffer spill for crash consistency or stays independent.
- Metrics and alert thresholds for buffer saturation in single-node deployments.

These are engineering choices inside Phase 1 and do not change the **dimensions** above.

## 7. Revision history

| Date | Author | Note |
|------|--------|------|
| 2026-05-03 | CTO agent | Initial skeleton + matrix per [CAN-26](/CAN/issues/CAN-26) |

# RFC: Phase 1 — Tier-2 (managed) source adapter slice

**Status:** draft (Phase 1 engineering contract)  
**Parent epic:** [CAN-34](/CAN/issues/CAN-34) · backlog [CAN-36#document-backlog](/CAN/issues/CAN-36#document-backlog)  
**Ticket:** [CAN-50](/CAN/issues/CAN-50)  
**Charter stack:** [CAN-5](/CAN/issues/CAN-5)  
**Related topology:** [CAN-30](/CAN/issues/CAN-30) (stages 1–2: Source → Source Connector) · buffer/checkpoint matrix [`rfc-phase1-buffer-checkpoint-provider-matrix.md`](./rfc-phase1-buffer-checkpoint-provider-matrix.md) · HTTP edge [`architecture/ingestion-v1.md`](./architecture/ingestion-v1.md) · product idempotency rules [`product/data-model-ingestion-requirements.md`](./product/data-model-ingestion-requirements.md) · operator honesty [`product/connector-tier-ux-spec.md`](./product/connector-tier-ux-spec.md) §2.2 `ingestion_path.tier_2_managed`

## 1. Scope and non-goals

**In scope:** the **contract** between a **Tier-2 managed source connector** (out-of-process worker that talks to an upstream system) and the **Canal Phase 1 HTTP ingestion edge** (`POST /v1/events`), including **idempotency key usage**, **retry / backoff policy**, **checkpoint interaction** with P1-local buffer semantics, and a **failure-mode taxonomy** operators and on-call can reason about.

**Out of scope for this slice:** concrete vendor SDKs, sink adapters, exactly-once end-to-end guarantees, and remote buffer/checkpoint drivers (Kafka-class rows in the matrix remain conformance targets only).

## 2. Interface (logical)

A **Tier-2 source adapter** is a **data-plane** component that:

1. **Reads** from an upstream source (API poll, webhook receiver, file tail, etc.) and **maps** records into OpenAPI **`IngestEvent`** shapes (or rejects / dead-letters at the adapter boundary).
2. **Submits** accepted events to the ingestion edge in **batches** using the normative batch envelope (`source` + `events[]`).
3. **Persists progress** via a **checkpoint** abstraction compatible with [`rfc-phase1-buffer-checkpoint-provider-matrix.md`](./rfc-phase1-buffer-checkpoint-provider-matrix.md) §5.2 (load → process → save; optional fence).
4. **Surfaces** health and last-error metadata to the control plane / operator model (exact API fields are follow-on; this RFC defines semantics).

The adapter does **not** own the **Event Buffer** (stage 5 in the board topology); it is upstream of it. It **may** use an internal **Source Event Buffer** (stage 3) for burst absorption; if present, that buffer MUST NOT weaken the idempotency and checkpoint rules in §§3–5 below.

## 3. Idempotency keys

Three layers MUST be distinguished in design and runbooks:

| Layer | Key / identifier | Responsibility |
|-------|-------------------|----------------|
| **Upstream** | Vendor message id, offset, cursor, or content hash | Adapter maps to a stable **`IngestEvent.id`**; if the upstream provides no id, the adapter MUST derive a **deterministic** id from stable fields (document the derivation in the connector family runbook). |
| **HTTP edge** | `IngestEvent.id` | **Normative** idempotency for dedupe within platform retention — see OpenAPI + [`data-model-ingestion-requirements.md`](./data-model-ingestion-requirements.md) §3.2. Duplicate `id` in a batch or across retries → `duplicateIds` in `202` response. |
| **Adapter run** | Internal “in flight” map (optional) | Prevents double-submit **before** ack from edge; MUST be consistent with checkpoint advance rules in §5. |

**Rules**

1. **Retries:** any retry of the same logical event MUST reuse the **same** `IngestEvent.id` so the edge dedupes correctly.
2. **Replays:** if checkpoint regresses (restore from backup, bug), re-submitted events with the same `id` are **correct** duplicates; downstream buffer replay remains governed by the buffer/checkpoint RFC.
3. **Batch envelopes:** `source` MUST identify the connector instance (stable producer key); it is **not** a substitute for per-event `id` idempotency.

## 4. Backoff

**Objective:** bound thundering herds, respect upstream `Retry-After` / rate limits, and avoid masking permanent faults.

| Situation | Backoff shape | Cap | Jitter |
|-----------|---------------|-----|--------|
| Transient HTTP errors to ingestion edge (5xx, connect timeout) | Full jitter exponential | Configurable max interval; default TBD per deployment | **Required** on every retry |
| Upstream rate limit (429) | Honor `Retry-After` when present; else exponential | Same cap as above | Required |
| Permanent client errors (4xx except 429) | **No** blind retry loop | — | Surface as **terminal** (§6) |
| Partial batch acceptance (`202` with per-event errors) | Retry **only** failed subset with same `id`s | Same policy | Required |

**Operator visibility:** sustained backoff (e.g. crossing a threshold of consecutive failures or wall-clock stall) MUST transition the adapter instance to a **degraded** or **paused** state with a **clear last error** string suitable for the operator wizard honesty stack (`ingestion_path.tier_2_managed` — no pilot labeling; still no fabricated SLA numbers).

## 5. Checkpoint interaction

Ordering with the board topology and P1-local stance:

1. **Baseline:** **at-least-once** from adapter → edge is assumed; the edge documents dedupe on `id`.
2. **Advance rule (recommended default):** persist checkpoint **only after** a `202` response whose body indicates **no unresolved failures** for events that share the checkpoint cursor window (full batch success, or successful retry of failed subset). If the process crashes after `202` but before checkpoint save, **duplicate delivery** is acceptable; duplicate `id` handles correctness.
3. **Stricter modes:** “checkpoint before call” is **forbidden** for default Tier-2 unless paired with proven idempotency at the edge **and** documented loss trade-offs — it loses crash safety between checkpoint and HTTP success.
4. **Fence:** when the checkpoint store supports **fence** (see matrix §5.2), leader / epoch changes MUST increment the fence token so stale writers cannot advance cursor after failover.
5. **Interaction with Event Buffer:** the adapter checkpoint tracks **upstream read position** and **last successful edge accept**, not internal Event Buffer consumer offsets; those are separate consumer checkpoints downstream of this RFC.

## 6. Failure modes (taxonomy)

| Class | Examples | Adapter behavior | Operator signal |
|-------|----------|------------------|-----------------|
| **Transient upstream** | Timeout, 5xx, blips | Backoff (§4), retain checkpoint | Degraded if prolonged |
| **Transient edge** | Ingestion `5xx`, network | Backoff (§4) | Degraded if prolonged |
| **Permanent upstream** | Auth revoked, resource gone, contract change | Stop retrying; **terminal** state | Action required (rotate secret, config) |
| **Permanent mapping** | Unparseable payload, missing required fields | **Dead-letter** or skip with audit; do **not** poison-checkpoint infinite retry | Action required (schema / mapping) |
| **Permanent edge** | `400` on envelope | Fix config; no spin | Action required |
| **Duplicate / idempotent success** | Same `id` after retry | Normal; rely on `duplicateIds` | Healthy (informational metrics only) |
| **Clock skew** | `occurredAt` far future/past | Policy: clamp vs reject — document per connector family | Warning if policy is clamp |

## 7. Matrix cross-reference

The **Tier-2 managed source adapter** row in [`rfc-phase1-buffer-checkpoint-provider-matrix.md`](./rfc-phase1-buffer-checkpoint-provider-matrix.md) §7 points here for **idempotency**, **backoff**, and **checkpoint** semantics relative to the charter stack ([CAN-5](/CAN/issues/CAN-5)).

## 8. Revision history

| Date | Author | Note |
|------|--------|------|
| 2026-05-03 | Backend | Initial slice for [CAN-50](/CAN/issues/CAN-50): interface + failure modes + idempotency / backoff / checkpoint |

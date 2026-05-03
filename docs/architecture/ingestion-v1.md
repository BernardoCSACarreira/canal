# Architecture — Phase 1 HTTP ingestion (`ingestion-v1`)

**Scope:** Phase 1 **HTTP-first** ingestion path for the Canal MVP.  
**Normative contract:** [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml) (validated in CI).  
**Product requirements (schema, contracts, honesty):** [`docs/product/data-model-ingestion-requirements.md`](../product/data-model-ingestion-requirements.md).

---

## 1. System boundary

```
Client / connector
    │  HTTPS
    ▼
ingestion-api  —  POST /v1/events  (batch accept, 202 + dedupe metadata)
              —  GET  /health
              —  GET  /v1/stream   (stub / 501 — not Phase 1 dependency)
```

Downstream buffer, checkpoint, and processing implementations MUST conform to Phase 1 assumptions in [`docs/rfc-phase1-buffer-checkpoint-provider-matrix.md`](../rfc-phase1-buffer-checkpoint-provider-matrix.md) unless a feature flag explicitly selects a future driver.

---

## 2. Data model at the edge

- **Batch envelope:** `source` + `events[]` per OpenAPI. `source` is the logical producer key (connector id, app name, etc.).
- **Event:** `id`, `type`, `occurredAt`, optional untyped `payload`. `id` is the idempotency key; `type` is the schema discriminator for product and analytics.
- **Evolution:** additive `payload` fields are preferred; breaking changes require OpenAPI version bump and coordinated client release.

This architecture doc does **not** redefine fields; the OpenAPI schemas are authoritative.

---

## 3. RFC and roadmap boundaries

| Topic | Phase 1 stance |
|-------|----------------|
| Streaming (`/v1/stream`) | Stub only; no client or operator flow may depend on it. |
| Public SLA numbers | Owned by product copy + commercial packaging, not by this API document. |
| Buffer / checkpoint | P1-local interfaces; Kafka-class and cloud rows in the RFC are **conformance targets**, not shipped dependencies. |
| Lineage graph / schema registry | Out of scope for the v1 HTTP surface; correlation uses `source` + `type` only. |

---

## 4. Related UX

Operator surfaces MUST follow tier and honesty rules in [`docs/product/connector-tier-ux-spec.md`](../product/connector-tier-ux-spec.md) and engineering UI notes in [`docs/design/CAN-28-operator-ui-phase1.md`](../design/CAN-28-operator-ui-phase1.md).

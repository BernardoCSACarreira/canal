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
              —  GET  /v1/control/pipeline  (operator read model: stages + adapter placeholders)
              —  GET  /v1/control/canal/segments  (canal buffer segment list)
```

Example JSON for the control read paths: [`control-api-read-models.md`](./control-api-read-models.md).

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

---

## 5. Implementation mapping and language boundaries

- **Normative contract** remains the OpenAPI file referenced in section 1; **implementation language is not** defined by this doc.
- **Company policy:** Python for control plane, Go for data plane services, TypeScript **only** for web frontends — see [`language-platform-policy.md`](./language-platform-policy.md).
- **This repo today:** `services/ingestion-api` is a **Phase 1 TypeScript scaffold** under the time-bounded exception documented in that policy; it exists to validate the HTTP contract and operator UX before a production-grade Go edge is introduced.
- **Merges that expand server-side TypeScript** or change ingestion semantics require the [mainline architecture review](../governance/mainline-merge-architecture-review.md) checklist (CTO + QA + product).

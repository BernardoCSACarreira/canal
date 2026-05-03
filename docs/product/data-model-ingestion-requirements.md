# Data model and ingestion — explicit requirements (Phase 1)

**Status:** Product + engineering alignment (CPO / CTO).  
**Tickets:** [CAN-31](/CAN/issues/CAN-31) (this document) · parent [CAN-29](/CAN/issues/CAN-29).  
**Normative HTTP contract:** [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml) (versioned with the repo; CI validates shape).

---

## 1. Why this exists

Board and program leadership need a single place that states **what the ingestion MVP promises in data terms** (schema, contracts, durability semantics, and honesty about SLOs), without mixing those promises with UX copy alone or with future streaming work.

---

## 2. Coverage inventory (schema · contracts · SLO language · lineage-adjacent)

| Artifact | Schema / typing | Contracts (machine-readable) | SLO / “honesty” language | Lineage-adjacent |
|----------|-----------------|------------------------------|--------------------------|------------------|
| Company goal / charter (connector program + paths) | Implied tiers and paths | Not a substitute for OpenAPI | Charter-level expectations; must not override API truth | Out of scope for Phase 1 MVP API |
| [`docs/architecture/ingestion-v1.md`](../architecture/ingestion-v1.md) | Ingest-edge model + evolution rules | Pointers to OpenAPI + CI | Defers public SLO claims to product spec | Explicit non-goal for v1 HTTP surface |
| [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml) | `IngestEvent` + batch envelope | **Normative** for HTTP accept + health | Describes **delivery class** (batch vs stub stream), not customer SLA | `source` + `type` are correlation hooks only |
| [`docs/product/connector-tier-ux-spec.md`](connector-tier-ux-spec.md) | Distinguishes `ConnectorTier` vs `IngestionPathTier` | Proposed follow-on fields (non-blocking) | **Normative for operator-facing honesty** in Phase 1 | Not a lineage catalog |
| [`docs/design/CAN-28-operator-ui-phase1.md`](../design/CAN-28-operator-ui-phase1.md) | UI tokens and hierarchy | References OpenAPI for “what works” | Reinforces no implied realtime while stream is stub | N/A |
| [`docs/rfc-phase1-buffer-checkpoint-provider-matrix.md`](../rfc-phase1-buffer-checkpoint-provider-matrix.md) | Buffer / checkpoint interfaces (engineering) | Conformance targets for future drivers | Operational posture (P1-local HA class), not a customer SLA | Replay anchors discussed as **engineering** concern |

---

## 3. PRD rules (explicit gaps closed)

1. **Ingest-edge payload** — The only **required** product-wide event shape in Phase 1 is the OpenAPI `IngestEvent`: stable `id` (idempotency), `type` (discriminator / taxonomy), `occurredAt`, optional `payload` JSON. Product and analytics MUST treat `type` as an extensible string registry; breaking changes require contract version bump + migration note.

2. **Idempotency** — Duplicate `id` values within the platform’s retention window are deduplicated per contract (`duplicateIds` in response). PRDs MUST NOT assume exactly-once end-to-end delivery unless a future phase publishes sink idempotency guarantees.

3. **Batch vs stream** — Phase 1 customer-observable ingestion is **HTTP batch** to `POST /v1/events`. `GET /v1/stream` is a **stub** (implementations SHOULD return `501` until streaming ships). No roadmap item may describe stream as committed scope without a separate delivery milestone and contract revision.

4. **SLO language** — No Phase 1 PRD or GTM artifact may claim contractual latency or availability for pilot or standard tiers. Public copy rules live in [`connector-tier-ux-spec.md`](connector-tier-ux-spec.md) §4; engineering supplements MUST NOT weaken those rules.

5. **Two tier namespaces** — Wizard `IngestionPathTier` and commercial `ConnectorTier` MUST remain distinct in PRDs, APIs, and analytics (see connector spec §1). Collapsing them in a single enum is a **product defect**, not a naming preference.

6. **Buffer and checkpoint** — Product assumes **P1-local** buffer and checkpoint semantics per the RFC matrix until a board-approved phase promotes remote buses. PRDs may reference **at-least-once** processing as the baseline; exactly-once remains out of scope unless explicitly approved.

7. **Lineage** — Phase 1 MAY use `source` (producer identity) and `type` for correlation dashboards. A **catalogued lineage graph**, schema registry enforcement, and column-level lineage are **explicit non-goals** for the HTTP MVP unless spun out as child work with board-visible scope.

---

## 4. Joint weekly checkpoint (CPO + CTO)

Until [CAN-29](/CAN/issues/CAN-29) is resolved, CPO and CTO hold a **weekly 30-minute checkpoint** (async summary if either is unavailable) to reconcile:

- charter connector path language vs `contracts/ingestion-v1.openapi.yaml` reality;
- any new PRD field that implies schema or SLA guarantees; and
- buffer/checkpoint RFC rows vs near-term engineering cuts.

**Output:** a short bullet list posted on [CAN-29](/CAN/issues/CAN-29) when material disagreements exist; otherwise “no delta” is acceptable.

---

## 5. Board / CDO decisions (parent thread only)

Scope cuts (for example deferring lineage, stream, or non-local HA) versus staffing or vendor spend MUST be raised on **[CAN-29](/CAN/issues/CAN-29)** with explicit options. This document records engineering and product alignment; it does **not** substitute for board approval of trade-offs.

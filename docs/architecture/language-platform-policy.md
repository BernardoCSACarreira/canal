# Language and platform policy (Canal / data engineering)

**Status:** normative for net-new work.  
**Related:** [ingestion-v1](./ingestion-v1.md) (Phase 1 HTTP surface), product requirements under `docs/product/`.

---

## 1. Canonical stack

| Layer | Language / runtime | Role |
|-------|-------------------|------|
| **Control plane** | **Python** | Orchestration, connector registry, policy, tenancy, operator APIs that are not latency-critical hot paths, internal tooling, batch workflows. |
| **Data plane** | **Go** | Connectors, buffering, checkpointing, high-throughput ingestion paths, workers talking to Kafka/S3/etc. |
| **Web frontends** | **TypeScript** (React or equivalent) | Browser UI only: operator consoles, marketing sites, internal dashboards served to humans. |

Contract-first boundaries (OpenAPI, protobuf, SQL schemas) are authoritative across languages; implementation language is chosen per layer above.

---

## 2. Server-side TypeScript

The former Phase 1 TypeScript combined **ingestion-api** service has been **removed** (CAN-73); edge HTTP is **Go** and operator control is **Python** per section 1.

**Rules:**

- No new long-lived TypeScript **servers** without an ADR and CTO + QA + product sign-off per [mainline merge — architecture review](../governance/mainline-merge-architecture-review.md).
- New **non-frontend** features default to Python (control) or Go (data), not Node.

---

## 3. Code review expectations (CTO)

- Architecture-impacting PRs require explicit reference to an updated architecture or ADR doc and sign-off process in the governance doc above.
- “Looks fine” without contract/schema impact analysis is not sufficient for data-plane or ingestion changes.

---

## 4. When this policy changes

Updates to this file are **architecture changes**. They follow the same review path as other mainline architecture docs (CTO, QA, product).

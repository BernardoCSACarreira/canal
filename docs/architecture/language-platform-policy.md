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

## 2. Current repository exception (Phase 1 velocity)

The Canal MVP repo currently ships a **small TypeScript** service under `services/ingestion-api` that implements the Phase 1 HTTP contract in [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml). That is an **explicit, time-bounded scaffold** so we can validate the contract, UX, and operator flows before investing in a Go rewrite of the edge accept path.

**Rules for this exception:**

- No additional long-lived TypeScript **servers** beyond Phase 1 ingestion + tests without an ADR and CTO + QA + product sign-off per [mainline merge — architecture review](../governance/mainline-merge-architecture-review.md).
- New **non-frontend** features default to Python (control) or Go (data), not Node.
- Exit criteria for the exception: parity tests against the OpenAPI contract pass in Go (or Python where appropriate for the component), operator flows unchanged, then retire or demote the TS service.
- **Python control-plane counterpart** for operator read models and adapter-instance registry lives under `services/ingestion-control-plane/` (see [`control-api-read-models.md`](./control-api-read-models.md)); the TS service may still expose the same routes until routing is split — keep responses aligned.

---

## 3. Code review expectations (CTO)

- Architecture-impacting PRs require explicit reference to an updated architecture or ADR doc and sign-off process in the governance doc above.
- “Looks fine” without contract/schema impact analysis is not sufficient for data-plane or ingestion changes.

---

## 4. When this policy changes

Updates to this file are **architecture changes**. They follow the same review path as other mainline architecture docs (CTO, QA, product).

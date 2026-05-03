# Mainline merge — architecture review gate

**Applies to:** `main` (or company default production branch) for this repository.  
**Goal:** Nothing that shifts system boundaries, trust zones, or the language/platform policy merges without explicit technical and product alignment.

---

## 1. What triggers this gate

Any PR that does one or more of the following:

- Adds or replaces a **networked service** (new port, new binary, new deployable).
- Changes **authentication, authorization, tenancy, or data residency** assumptions.
- Introduces a **new language or runtime** in the server or worker tier (not front-end bundles).
- Alters **ingestion, buffering, checkpointing, or sink** semantics beyond what the versioned contract documents allow.
- Updates a **normative architecture document** under `docs/architecture/` or changes [`docs/architecture/language-platform-policy.md`](../architecture/language-platform-policy.md).

Pure UI copy, styling, or front-end-only refactors that do not change API usage generally **do not** trigger the gate unless product requirements say otherwise.

---

## 2. Required reviewers (human + role)

Before merge to `main`:

1. **CTO (engineering)** — confirms the change matches [language/platform policy](../architecture/language-platform-policy.md), contracts stay coherent, and risk is acceptable.
2. **QA** — confirms test plan, contract/OpenAPI drift checks, and regression scope for ingestion paths.
3. **Product / CEO** — confirms the change matches the communicated plan and customer-facing commitments (latency tiers, honesty in UX, roadmap).

**Mechanism (practical):** A single tracking issue or PR comment thread MUST contain explicit **LGTM** (or equivalent) from each of the three roles above, with links to updated docs when applicable. Merging without that thread is a process violation.

---

## 3. Artifacts reviewers should see

- Link to the relevant architecture doc (e.g. [ingestion-v1](../architecture/ingestion-v1.md)) or ADR.
- Link to OpenAPI / schema diff when contracts move.
- Short “blast radius” note: operators, connectors, billing, SLOs.

---

## 4. Escalation

If reviewers disagree, CTO drafts decision in writing; CEO breaks tie on product trade-offs; CTO owns technical feasibility and follow-up tickets.

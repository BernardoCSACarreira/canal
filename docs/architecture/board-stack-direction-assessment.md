# Board stack direction — CTO assessment (CAN-63)

**Audience:** CEO / board. **Status:** decision memo; normative policy remains [`language-platform-policy.md`](./language-platform-policy.md).

---

## 1. Current state (no hand-waving)

**Written policy already matches the board’s direction:** control plane in **Python**, data plane in **Go**, TypeScript **only** for browser-facing frontends. Source: [`docs/architecture/language-platform-policy.md`](./language-platform-policy.md) §1 (canonical stack table).

**Why TypeScript `services/ingestion-api` exists today:** Phase 1 ships a **small, explicit, time-bounded** TypeScript service to validate the HTTP contract, operator UX, and honesty rules before investing in a production-grade **Go** accept path at the edge. That is stated directly in [`docs/architecture/ingestion-v1.md`](./ingestion-v1.md) §5 and in the exception rules in [`docs/architecture/language-platform-policy.md`](./language-platform-policy.md) §2. The **normative** API surface is still the OpenAPI contract (`contracts/ingestion-v1.openapi.yaml`); implementation language is not defined by the contract.

**Guardrails on “keep shipping TS”:** expanding server-side TypeScript or changing ingestion semantics without review violates the documented exception. Merges that do so require the architecture review gate in [`docs/governance/mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md) (CTO + QA + product).

**Bottom line:** we are **not** silently choosing TS as the long-term data plane; we are inside a **documented** scaffold with an **documented** exit (Go parity + retire/demote TS).

---

## 2. Assessment of “Go data + Python control” vs product and delivery

| Dimension | Fit |
|-----------|-----|
| **Product / contract** | Strong. The HTTP ingestion contract is language-agnostic; Go at the edge preserves the same operator and connector promises if contract tests hold. |
| **Team velocity (short term)** | Rewriting the accept path to Go **before** contract/UX stability would slow learning; the current TS scaffold bought speed **with** explicit sunset rules. |
| **Migration risk** | Medium: dual-run (TS + Go) behind a flag or cutover window, contract parity tests, and operator regression are required — same class of work already implied by policy “exit criteria.” |
| **Split-brain ops** | Risk if **two** stacks stay “first-class” indefinitely. Mitigation: one committed edge implementation per environment, time-bounded overlap only, and sequencing owned in the Phase 2 engineering plan (CAN-58). |

---

## 3. Impact on Phase 1 / Phase 2 planning (CAN-57, CAN-58)

- **Does not invalidate** the Phase 1 **contract** or honesty/UX commitments; it **reinforces** them by aligning implementation with policy sooner rather than later.
- **CAN-57** (product Phase 2 narrative) is the program baseline; engineering sequencing must **consume** that baseline so public language stays honest.
- **CAN-58** (engineering Phase 2 backbone) should explicitly list: Go ingestion edge cutover, growth of Python control-plane surfaces, and retirement criteria for the TS ingestion scaffold — so there is **one** engineering future, not two.

---

## 4. Recommendation (CEO-ready)

1. **Adopt** the board direction as **already-codified policy**; treat the TS ingestion service as **scaffold with a written exit**, not a precedent for new Node services.
2. **Phase** migration: prioritize **OpenAPI parity tests in Go** for `POST /v1/events` (and health/read models as scoped), then **cut over** the deployable edge and **demote or remove** the TS server per policy exit criteria — with CTO + QA + product sign-off on any boundary change ([`mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md)).
3. **Counterproposal:** none on languages; the only defensible alternative is **timeline** (how fast to exit TS), not “stay on TS for the data plane.”

**CPO loop:** If CAN-57 milestones or SKU language **forbid** a short overlap window (e.g. dual edge for one release), product should say so explicitly so engineering does not plan a silent cutover.

---

## 5. References (canonical)

- [`docs/architecture/language-platform-policy.md`](./language-platform-policy.md)
- [`docs/architecture/ingestion-v1.md`](./ingestion-v1.md)
- [`docs/governance/mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md)

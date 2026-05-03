# Board stack direction — CTO assessment (CAN-63)

**Audience:** CEO / board. **Status:** decision memo; normative policy remains [`language-platform-policy.md`](./language-platform-policy.md).

---

## 1. Current state (no hand-waving)

**Written policy already matches the board’s direction:** control plane in **Python**, data plane in **Go**, TypeScript **only** for browser-facing frontends. Source: [`docs/architecture/language-platform-policy.md`](./language-platform-policy.md) §1 (canonical stack table).

**Historical note (CAN-73):** A Phase 1 TypeScript combined **ingestion-api** service validated the contract before the **Go** edge shipped; it has been **removed** from the repo. The **normative** API surface remains the OpenAPI contract (`contracts/ingestion-v1.openapi.yaml`); implementation language is not defined by the contract.

**Guardrails:** expanding server-side TypeScript or changing ingestion semantics without review violates policy. Merges that do so require the architecture review gate in [`docs/governance/mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md) (CTO + QA + product).

**Bottom line:** the **data plane edge is Go**; **control is Python**; TypeScript is **browser UI only**, per [`language-platform-policy.md`](./language-platform-policy.md).

---

## 2. Assessment of “Go data + Python control” vs product and delivery

| Dimension | Fit |
|-----------|-----|
| **Product / contract** | Strong. The HTTP ingestion contract is language-agnostic; Go at the edge preserves the same operator and connector promises if contract tests hold. |
| **Team velocity (short term)** | The TS scaffold is retired; net-new edge work is Go-first. |
| **Migration risk** | Cutover to Go + split stack is **done** for this repo; ongoing risk is normal contract regression if CI or reviews slip. |
| **Split-brain ops** | Mitigation: one edge implementation per environment (Go data plane) and Python control; no second Node ingestion server. |

---

## 3. Impact on Phase 1 / Phase 2 planning (CAN-57, CAN-58)

- **Does not invalidate** the Phase 1 **contract** or honesty/UX commitments; it **reinforces** them by aligning implementation with policy sooner rather than later.
- **CAN-57** (product Phase 2 narrative) is the program baseline; engineering sequencing must **consume** that baseline so public language stays honest.
- **CAN-58** (engineering Phase 2 backbone) should explicitly list growth of Python control-plane surfaces and hardened Go edge paths — the TS ingestion scaffold is **retired** (CAN-73).

---

## 4. Recommendation (CEO-ready)

1. **Adopt** the board direction as **already-codified policy**; the TS ingestion server is **removed** — do not reintroduce Node as a data-plane pattern without ADR + review.
2. **Maintain** OpenAPI contract tests and operator regression coverage on the **Go** edge and **Python** control plane; sign off boundary changes per [`mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md).
3. **Counterproposal:** none on languages for the ingestion edge; it is **Go**.

**CPO loop:** If CAN-57 milestones or SKU language **forbid** a short overlap window (e.g. dual edge for one release), product should say so explicitly so engineering does not plan a silent cutover.

---

## 5. References (canonical)

- [`docs/architecture/language-platform-policy.md`](./language-platform-policy.md)
- [`docs/architecture/ingestion-v1.md`](./ingestion-v1.md)
- [`docs/governance/mainline-merge-architecture-review.md`](../governance/mainline-merge-architecture-review.md)

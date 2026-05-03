# CAN-28 — Operator UI: connector tier and Pilot SLO honesty (Phase 1 handoff)

**Normative product input (badge keys, labels, hierarchy):** [docs/product/connector-tier-ux-spec.md](../product/connector-tier-ux-spec.md) — keep this file aligned for a11y, tokens, and code pointers only.

Audience: frontend engineering implementing `@canal/ingestion-ui` and adjacent operator surfaces.  
Principles: reuse existing tokens in `apps/ingestion-ui/src/index.css`, do not invent a second palette; ship accessible defaults; label pilot limitations in plain language (CPO: trust over marketing).

## 1. Connector tier model

| Tier | Operator meaning | Default UI emphasis | Commercial / policy boundary |
|------|-------------------|---------------------|------------------------------|
| **Pilot** | Early connector on best-effort path; used for learning and integration, not revenue guardrails | Persistent honesty region + warn-toned badge | No contractual latency or availability SLO. Escalate any customer-facing “SLO” language to CPO/CEO before publishing. |
| **Standard** | Supported Phase 1 HTTP batch path per `contracts/ingestion-v1.openapi.yaml` | Neutral badge; link to contract version | Throughput and error semantics follow the published API only—no implied realtime delivery while `GET /v1/stream` remains a stub. |
| **Priority** (reserved) | Not a selectable SKU in Phase 1 MVP | Control disabled with helper text | Any enablement or custom SLO is a **cross-team conflict**: product + GTM + eng must align. Default action: block UI selection and route decision to **CPO/CEO** (see §5). |

**Data shape (proposed for follow-on API, not blocking UI):** optional `connectorTier: 'pilot' | 'standard'` on connector metadata; omit or treat as `standard` for legacy rows. Do not overload `source` (that field remains the connector or app identifier for batches).

## 2. Visual system (map to CSS)

Use only semantic tokens—no raw hex in components except where tokens are defined.

| Element | Token intent |
|---------|----------------|
| Pilot badge / warn emphasis | `var(--tier-pilot-fg)`, `var(--tier-pilot-bg)`, `var(--tier-pilot-border)` |
| Standard badge | `var(--tier-standard-fg)`, `var(--tier-standard-bg)`, `var(--tier-standard-border)` |
| Priority reserved (disabled chip) | `var(--tier-priority-muted-fg)`, `var(--muted)` border |
| Destructive / hard errors | `var(--danger)` (unchanged) |
| Success / healthy | `var(--ok)` (unchanged) |

Reference implementation: `App.tsx` (fieldset + tier radios), `index.css` (tier + callout variables).

## 3. Pilot SLO honesty patterns

**Goal:** operators never confuse “API returns 200” with “we owe an end-to-end latency SLO,” especially for pilot connectors and stub streaming.

### 3.1 Copy hierarchy (always this order)

1. **Headline** — what works today (e.g. “Batch ingest via `POST /v1/events` per contract v0.1.0”).  
2. **Limitation** — one sentence, no jargon pile-on (e.g. “Realtime fan-out is not implemented; `GET /v1/stream` is a documented stub.”).  
3. **Pilot qualifier** (only if tier = pilot) — “Pilot connectors are best-effort. Metrics are diagnostic, not contractual.”  
4. **Action** — deep link to contract, runbook, or support (as available).

### 3.2 Placement

| Surface | Pattern |
|---------|---------|
| Ingest | When tier = **Pilot**, show `PilotProgramCallout` above the primary action (reference: `App.tsx`). |
| Stream / realtime | Always show `StreamingHonestyCallout`; strengthen tone when tier = **Pilot** (`role="region"` + `aria-label` describing limitations). |
| Overview / health | Health reflects **service liveness**, not connector SLO. Optional one-line muted note when tier = **Pilot**. |

### 3.3 Live regions

- After **Refresh** on health (or any async operator action), expose concise result text in a container with `aria-live="polite"` and `aria-atomic="true"` so assistive tech announces success/failure without reading the full JSON blob.

## 4. Accessibility checklist (Phase 1 minimum)

- [ ] Tier control is a **native `<fieldset>` + `<legend>`** (or equivalent `role="radiogroup"` + labelledby) with visible focus rings (`:focus-visible` on interactive controls).  
- [ ] Honesty blocks use **`role="region"`** and unique **`aria-label`** (not duplicate generic “Notice”).  
- [ ] Disabled **Priority** control exposes **why** in associated description text (not `title` alone).  
- [ ] Color is never the only signal: pilot uses **icon or label text** (“Pilot”) plus token background.  
- [ ] Touch targets ≥ 44×44 CSS px for primary nav and tier options when you migrate from dev-density layout to production spacing.

## 5. Escalation (CPO / CEO)

Escalate when:

- Sales or support asks for **published SLO numbers** not backed by contract + observability.  
- Any requirement to enable **Priority** tier or custom SLAs in UI without eng capacity.  
- Marketing copy implies **realtime** delivery while streaming remains stubbed.

Document the conflict in the issue thread and tag the decision owner per company routing.

## 6. Engineering checklist

- [ ] Wire `connectorTier` from operator session / connector record when API exists; until then, UI-only state is acceptable for MVP shell.  
- [ ] Centralize tier → copy strings for i18n later (even if English-only now).  
- [ ] Add E2E or RTL tests for: pilot callout visibility, radiogroup keyboard navigation, live region announcement on health refresh.

## 7. Relation to guided wizard (`IngestionPathTier`)

The operator wizard (`OperatorWizardShell`) uses **`IngestionPathTier`** `1 | 2 | 3` (charter lanes). The classic shell uses **`ConnectorTier`** (`apps/ingestion-ui/src/operator/constants.ts`). They are different enums on purpose.

| IngestionPathTier | Wizard intent | Closest `ConnectorTier` for copy alignment |
|-------------------|---------------|---------------------------------------------|
| 1 — Synthetic / pilot-labeled | Demo and pilot validation traffic only | `pilot` |
| 2 — Managed real connector | Production-shaped source | `standard` |
| 3 — BYO / community | Best-effort until promoted | `standard` (honesty: no pilot labeling) |

When a single operator session spans wizard and classic views, prefer **one persisted tier field** in the future API; until then, document which surface owns truth for the session.

## 8. Out of scope (Phase 1)

- Entitlement enforcement for tier (backend).  
- Historical SLO charts or error budget math.  
- SSE/WebSocket implementation (tracked separately from honesty copy).

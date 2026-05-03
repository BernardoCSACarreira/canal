# Connector tier — UX spec (Phase 1 interim)

**Status:** Interim product spec (CPO). **Spec approved** by Product Design on CAN-28 (2026-05-03) — badge keys §2 frozen for Phase 1; FE may unblock CAN-27.  
**Goal:** Unblock frontend on [CAN-27](/CAN/issues/CAN-27) without external Figma.  
**Related:** [CAN-8](/CAN/issues/CAN-8), [CAN-28](/CAN/issues/CAN-28), charter connector tiers ([CAN-2](/CAN/issues/CAN-2)).  
**Implementation supplement:** [docs/design/CAN-28-operator-ui-phase1.md](../design/CAN-28-operator-ui-phase1.md) (a11y, token map, code pointers).

---

## 1. Two namespaces (do not merge enums)

Operators see two different concepts; engineering must keep them distinct in types, copy tables, and analytics.

| Namespace | Where used | Type (code) | Purpose |
|-----------|------------|-------------|---------|
| **Commercial connector program** | Classic shell, future connector record | `ConnectorTier` | Support / honesty framing for a connector SKU path |
| **Ingestion path (charter)** | Guided wizard only | `IngestionPathTier` `1 \| 2 \| 3` | Which charter lane the operator chose for this session |

**Rule:** Never show wizard `Tier 2` language on a screen that is bound to `ConnectorTier` without an explicit mapping note in the spec or UI.

---

## 2. Canonical badge keys (machine-readable)

Use these **stable string keys** for i18n bundles, analytics events, and feature flags. **Do not rename** without a migration note and FE coordination.

### 2.1 `ConnectorTier` (commercial program)

| Badge key | Enum / value | Selectable in Phase 1 UI |
|-----------|----------------|---------------------------|
| `connector_tier.pilot` | `pilot` | Yes |
| `connector_tier.standard` | `standard` | Yes |
| `connector_tier.priority_reserved` | `priority_reserved` | **No** — disabled control + explanation (CPO/CEO for SKU + SLO claims) |

**Proposed API field (follow-on):** `connectorTier` with values `pilot` | `standard` only; omit or default to `standard` for legacy. Do **not** persist `priority_reserved` until the SKU exists.

### 2.2 `IngestionPathTier` (wizard — charter path)

| Badge key | Honesty-line key (paired copy) | Numeric tier | Charter meaning (short) |
|-----------|-------------------------------|--------------|-------------------------|
| `ingestion_path.tier_1_synthetic` | `ingestion_path.tier_1_synthetic.honesty` | `1` | Synthetic / pilot-**labeled** path only |
| `ingestion_path.tier_2_managed` | `ingestion_path.tier_2_managed.honesty` | `2` | Managed real connector |
| `ingestion_path.tier_3_byo` | `ingestion_path.tier_3_byo.honesty` | `3` | BYO / community connector |

**Backend alignment:** Synthetic path aligns with internal taxonomy `tier-1` (see `services/ingestion-control-plane/src/canal_control_plane/read_models.py` — placeholder adapter `catalogTier`). UI copy must still say **pilot labeling**, not “customer SLO,” on tier 1.

### 2.3 Optional cross-key alias (analytics only)

For dashboards that collapse wizard + classic, use an explicit **`ui_context`** dimension (`classic_shell` | `operator_wizard`) **in addition to** the badge key above — never overload a single key to mean both.

---

## 3. Human-facing strings (short labels)

Use **sentence case** for headings; **title case** acceptable only in compact badges if space-constrained.

### 3.1 Commercial program (`ConnectorTier`)

| Badge key | Badge label (max ~28 chars) | Subtitle / helper (one line) |
|-----------|----------------------------|------------------------------|
| `connector_tier.pilot` | Pilot | Best-effort; pilot-program honesty copy always on. |
| `connector_tier.standard` | Standard | Phase 1 HTTP batch per published OpenAPI; no pilot badge. |
| `connector_tier.priority_reserved` | Priority (reserved) | Not enabled — requires CPO/CEO before UI or GTM use. |

### 3.2 Wizard path (`IngestionPathTier`)

| Badge key | Badge label | Honesty line (always visible near badge) |
|-----------|-------------|------------------------------------------|
| `ingestion_path.tier_1_synthetic` | Tier 1 — Synthetic / pilot-labeled | Pilot program labeling on this path only — diagnostic, not a contractual SLO. |
| `ingestion_path.tier_2_managed` | Tier 2 — Managed real connector | No pilot labeling — reliability follows standard connector operations. |
| `ingestion_path.tier_3_byo` | Tier 3 — BYO / community | No pilot labeling — best-effort; availability and latency are not guaranteed. |

**Source of truth for wizard strings in code:** `TIER_LABEL` and `SLO_SUMMARY` in `apps/ingestion-ui/src/operator/constants.ts` — keep this table and that file in sync when copy changes.

---

## 4. Copy hierarchy (honesty stack)

On any surface that could be mistaken for an SLA dashboard, render **in this order**:

1. **What works** — Concrete capability (e.g. batch `POST /v1/events` per contract version).  
2. **What does not** — One plain sentence (e.g. stream route is a documented stub; not realtime SLO).  
3. **Pilot qualifier** — Only if `connector_tier.pilot` **or** wizard `ingestion_path.tier_1_synthetic`: best-effort; metrics diagnostic; no published customer SLO without CPO/CEO + Legal.  
4. **Next action** — Link to contract, runbook, or support when available.

**Forbidden in Phase 1 public operator copy:** implying contractual **latency** or **availability** SLO for pilot or standard tiers; calling stream probe success a “commitment.”

---

## 5. Placement matrix (Phase 1)

| Surface | `connector_tier.*` | `ingestion_path.*` |
|---------|-------------------|---------------------|
| Classic — Ingest | Tier fieldset + pilot callout when pilot | N/A |
| Classic — Stream | Honesty region (all tiers); stronger visual weight for pilot | N/A |
| Classic — Health / overview | Optional pilot note; `aria-live` summary for refresh | N/A |
| Wizard — path pick | N/A | Badge + honesty line per row |
| Wizard — configure / review | N/A | Strip persists selected path tier |

---

## 6. Visual system

Single palette: semantic CSS variables in `apps/ingestion-ui/src/index.css` (`--tier-pilot-*`, `--tier-standard-*`, `--callout-*`, etc.). **No ad-hoc hex** in React components.

Focus: visible `:focus-visible` on all tier controls.

---

## 7. Approval gate (CAN-27)

- **Badge keys** — §2 frozen for Phase 1 unless CPO approves a rename.  
- **Copy** — §3 and §4 approved for FE implementation; wizard literals live in `constants.ts`.  
- **Hierarchy** — §4 is normative for honesty blocks.

**Spec approved** by Product Design (CAN-28). Frontend may treat this file + `docs/design/CAN-28-operator-ui-phase1.md` as the combined Phase 1 input for [CAN-27](/CAN/issues/CAN-27).

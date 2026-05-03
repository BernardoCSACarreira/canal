/**
 * Commercial / support framing for the classic operator shell (CAN-28).
 * Persist on connector metadata when the API supports it; session-local today.
 */
import type { CatalogTier } from '../api/types'

export type ConnectorTier = 'pilot' | 'standard' | 'priority_reserved'

/** Stable keys — must match `docs/product/connector-tier-ux-spec.md` §2.1. */
export const CONNECTOR_TIER_BADGE_KEY: Record<ConnectorTier, string> = {
  pilot: 'connector_tier.pilot',
  standard: 'connector_tier.standard',
  priority_reserved: 'connector_tier.priority_reserved',
}

/**
 * Guided wizard “ingestion path” lanes — charter UX, not the same enum as
 * `ConnectorTier`. See `docs/design/CAN-28-operator-ui-phase1.md` for mapping.
 */
export type IngestionPathTier = 1 | 2 | 3

/** Maps wizard lane to control API `catalogTier` (OpenAPI). */
export function catalogTierForIngestionPath(tier: IngestionPathTier): CatalogTier {
  if (tier === 1) return 'tier-1'
  if (tier === 2) return 'tier-2'
  return 'tier-3'
}

/**
 * Epic B3 catalog honesty keys (tier2_real vs tier1_synthetic) — align with
 * internal taxonomy naming; not the same strings as OpenAPI `catalogTier`.
 */
export type CatalogTierKindKey = 'tier1_synthetic' | 'tier2_real' | 'tier3_byo'

export function catalogTierKindKey(catalogTier: CatalogTier): CatalogTierKindKey {
  if (catalogTier === 'tier-1') return 'tier1_synthetic'
  if (catalogTier === 'tier-2') return 'tier2_real'
  return 'tier3_byo'
}

/** Stable keys — must match `docs/product/connector-tier-ux-spec.md` §2.2. */
export const INGESTION_PATH_BADGE_KEY: Record<IngestionPathTier, string> = {
  1: 'ingestion_path.tier_1_synthetic',
  2: 'ingestion_path.tier_2_managed',
  3: 'ingestion_path.tier_3_byo',
}

/** Honesty-line keys — must match `docs/product/connector-tier-ux-spec.md` §2.2. */
export const INGESTION_PATH_HONESTY_KEY: Record<IngestionPathTier, string> = {
  1: 'ingestion_path.tier_1_synthetic.honesty',
  2: 'ingestion_path.tier_2_managed.honesty',
  3: 'ingestion_path.tier_3_byo.honesty',
}

/** Must match `docs/product/connector-tier-ux-spec.md` §3.2. */
export const TIER_LABEL: Record<IngestionPathTier, string> = {
  1: 'Tier 1 — Synthetic / pilot-labeled',
  2: 'Tier 2 — Managed real connector',
  3: 'Tier 3 — BYO / community',
}

/** Must match `docs/product/connector-tier-ux-spec.md` §3.2. */
export const SLO_SUMMARY: Record<IngestionPathTier, string> = {
  1:
    'Pilot program labeling on this path only — diagnostic, not a contractual SLO.',
  2: 'No pilot labeling — reliability follows standard connector operations.',
  3: 'No pilot labeling — best-effort; availability and latency are not guaranteed.',
}

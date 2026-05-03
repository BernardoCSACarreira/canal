/**
 * Commercial / support framing for the classic operator shell (CAN-28).
 * Persist on connector metadata when the API supports it; session-local today.
 */
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

export const TIER_LABEL: Record<IngestionPathTier, string> = {
  1: 'Tier 1 — synthetic / pilot-labeled path',
  2: 'Tier 2 — managed real connector',
  3: 'Tier 3 — BYO / community connector',
}

export const SLO_SUMMARY: Record<IngestionPathTier, string> = {
  1:
    'Pilot program labeling on this synthetic path only — diagnostic, not a contractual SLO.',
  2: 'No pilot labeling — reliability follows standard connector operations.',
  3: 'No pilot labeling — best-effort; availability and latency are not guaranteed.',
}

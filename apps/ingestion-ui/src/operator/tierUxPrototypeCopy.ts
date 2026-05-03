import type { IngestionPathTier } from './constants'

/**
 * Human strings for the tier UX prototype flag — aligned to
 * `docs/product/connector-tier-ux-spec.md` §3.2 (interim CPO table).
 */
export const tierUxPrototypeCopy: Record<
  IngestionPathTier,
  { badge: string; slo: string }
> = {
  1: {
    badge: 'Tier 1 — Synthetic / pilot-labeled',
    slo:
      'Pilot program labeling on this path only — diagnostic, not a contractual SLO.',
  },
  2: {
    badge: 'Tier 2 — Managed real connector',
    slo:
      'No pilot labeling — reliability follows standard connector operations.',
  },
  3: {
    badge: 'Tier 3 — BYO / community',
    slo:
      'No pilot labeling — best-effort; availability and latency are not guaranteed.',
  },
}

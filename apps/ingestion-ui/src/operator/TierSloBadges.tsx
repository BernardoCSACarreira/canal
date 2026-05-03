import type { CSSProperties } from 'react'
import type { IngestionPathTier } from './constants'
import {
  INGESTION_PATH_BADGE_KEY,
  INGESTION_PATH_HONESTY_KEY,
  SLO_SUMMARY,
  TIER_LABEL,
} from './constants'
import { tierUxPrototypeCopy } from './tierUxPrototypeCopy'
import { isTierUxPrototypeActive } from './tierUxPrototypeGate'

const badgeBase: CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: '0.35rem',
  padding: '0.25rem 0.55rem',
  borderRadius: 'var(--radius)',
  fontSize: '0.75rem',
  fontWeight: 600,
  letterSpacing: '0.02em',
  border: '1px solid',
  width: 'fit-content',
}

/** Default wizard chips (pre–CAN-28 visual sign-off). */
const tierStylesLegacy: Record<
  IngestionPathTier,
  { border: string; background: string; color: string }
> = {
  1: {
    border: 'var(--ok)',
    background: 'rgba(63, 185, 80, 0.12)',
    color: 'var(--ok)',
  },
  2: {
    border: 'var(--warn)',
    background: 'rgba(210, 153, 34, 0.12)',
    color: 'var(--warn)',
  },
  3: {
    border: 'var(--muted)',
    background: 'rgba(139, 148, 158, 0.12)',
    color: 'var(--muted)',
  },
}

/** CAN-28 semantic tokens: path 1 ↔ pilot, 2 ↔ standard, 3 ↔ standard honesty + muted emphasis. */
const tierStylesPrototype: Record<
  IngestionPathTier,
  { border: string; background: string; color: string }
> = {
  1: {
    border: 'var(--tier-pilot-border)',
    background: 'var(--tier-pilot-bg)',
    color: 'var(--tier-pilot-fg)',
  },
  2: {
    border: 'var(--tier-standard-border)',
    background: 'var(--tier-standard-bg)',
    color: 'var(--tier-standard-fg)',
  },
  3: {
    border: 'var(--border)',
    background: 'var(--tier-standard-bg)',
    color: 'var(--tier-priority-muted-fg)',
  },
}

function tierBadgeLabel(tier: IngestionPathTier): string {
  return isTierUxPrototypeActive()
    ? tierUxPrototypeCopy[tier].badge
    : TIER_LABEL[tier]
}

function tierSloLine(tier: IngestionPathTier): string {
  return isTierUxPrototypeActive()
    ? tierUxPrototypeCopy[tier].slo
    : SLO_SUMMARY[tier]
}

function tierBadgeStyles(tier: IngestionPathTier) {
  return isTierUxPrototypeActive() ? tierStylesPrototype[tier] : tierStylesLegacy[tier]
}

export function TierBadge({ tier }: { tier: IngestionPathTier }) {
  const t = tierBadgeStyles(tier)
  const proto = isTierUxPrototypeActive()
  return (
    <span
      style={{
        ...badgeBase,
        borderColor: t.border,
        background: t.background,
        color: t.color,
      }}
      data-badge-key={INGESTION_PATH_BADGE_KEY[tier]}
      data-tier-ux-prototype={proto ? 'true' : undefined}
    >
      {tierBadgeLabel(tier)}
    </span>
  )
}

export function SloHonestyLine({ tier }: { tier: IngestionPathTier }) {
  const proto = isTierUxPrototypeActive()
  return (
    <p
      style={{ margin: 0, fontSize: '0.85rem', color: 'var(--muted)' }}
      data-honesty-key={INGESTION_PATH_HONESTY_KEY[tier]}
      data-tier-ux-prototype={proto ? 'true' : undefined}
    >
      {tierSloLine(tier)}
    </p>
  )
}

export function SloBadgeStrip({ tier }: { tier: IngestionPathTier }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
      <TierBadge tier={tier} />
      <SloHonestyLine tier={tier} />
    </div>
  )
}

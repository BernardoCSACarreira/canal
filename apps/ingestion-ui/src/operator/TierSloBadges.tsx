import type { CSSProperties } from 'react'
import type { IngestionPathTier } from './constants'
import {
  INGESTION_PATH_BADGE_KEY,
  INGESTION_PATH_HONESTY_KEY,
  SLO_SUMMARY,
  TIER_LABEL,
} from './constants'

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

/** CAN-28 semantic tokens: path 1 ↔ pilot, 2 ↔ standard, 3 ↔ standard + muted emphasis. */
const tierStyles: Record<
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

export function TierBadge({ tier }: { tier: IngestionPathTier }) {
  const t = tierStyles[tier]
  return (
    <span
      style={{
        ...badgeBase,
        borderColor: t.border,
        background: t.background,
        color: t.color,
      }}
      data-badge-key={INGESTION_PATH_BADGE_KEY[tier]}
    >
      {TIER_LABEL[tier]}
    </span>
  )
}

export function SloHonestyLine({ tier }: { tier: IngestionPathTier }) {
  return (
    <p
      style={{ margin: 0, fontSize: '0.85rem', color: 'var(--muted)' }}
      data-honesty-key={INGESTION_PATH_HONESTY_KEY[tier]}
    >
      {SLO_SUMMARY[tier]}
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

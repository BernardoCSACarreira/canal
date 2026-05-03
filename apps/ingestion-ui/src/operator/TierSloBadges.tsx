import type { CSSProperties } from 'react'
import type { IngestionPathTier } from './constants'
import { SLO_SUMMARY, TIER_LABEL } from './constants'

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

const tierStyles: Record<
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
    >
      {TIER_LABEL[tier]}
    </span>
  )
}

export function SloHonestyLine({ tier }: { tier: IngestionPathTier }) {
  return (
    <p style={{ margin: 0, fontSize: '0.85rem', color: 'var(--muted)' }}>
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

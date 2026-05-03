import { useCallback, useMemo, useState, type CSSProperties } from 'react'
import { postIngestBatch } from '../api/client'
import type { IngestBatchRequest } from '../api/types'
import type { IngestionPathTier } from './constants'
import { SloBadgeStrip, TierBadge } from './TierSloBadges'

type WizardStep = 'path' | 'configure' | 'review'

const defaultEventsJson = `[
  {
    "id": "demo-1",
    "type": "page.view",
    "occurredAt": "${new Date().toISOString()}",
    "payload": { "path": "/" }
  }
]`

export function OperatorWizardShell({ onExit }: { onExit: () => void }) {
  const [step, setStep] = useState<WizardStep>('path')
  const [tier, setTier] = useState<IngestionPathTier | null>(null)

  const stepIndex = useMemo(() => {
    if (step === 'path') return 0
    if (step === 'configure') return 1
    return 2
  }, [step])

  const goPath = (t: IngestionPathTier) => {
    setTier(t)
    setStep('configure')
  }

  const resetPath = () => {
    setTier(null)
    setStep('path')
  }

  return (
    <div style={wiz.layout}>
      <header style={wiz.topBar}>
        <div>
          <h1 style={wiz.title}>Guided ingestion</h1>
          <p style={wiz.sub}>
            Operator wizard shell — tier and pilot-program honesty labels are
            explicit per path (CAN-28 design handoff + connector-tier UX spec).
          </p>
        </div>
        <button type="button" style={wiz.ghostBtn} onClick={onExit}>
          Exit to classic UI
        </button>
      </header>

      <StepIndicator index={stepIndex} />

      {step === 'path' && <PathStep onPick={goPath} />}
      {step === 'configure' && tier !== null && (
        <ConfigureStep
          tier={tier}
          onBack={resetPath}
          onContinue={() => setStep('review')}
        />
      )}
      {step === 'review' && tier !== null && (
        <ReviewStep tier={tier} onBack={() => setStep('configure')} />
      )}
    </div>
  )
}

function StepIndicator({ index }: { index: number }) {
  const labels = ['Path', 'Configure', 'Review']
  return (
    <ol style={wiz.steps}>
      {labels.map((label, i) => (
        <li
          key={label}
          style={{
            ...wiz.stepLi,
            opacity: i <= index ? 1 : 0.45,
            fontWeight: i === index ? 600 : 400,
          }}
        >
          <span style={wiz.stepNum}>{i + 1}</span>
          {label}
        </li>
      ))}
    </ol>
  )
}

function PathStep({ onPick }: { onPick: (tier: IngestionPathTier) => void }) {
  return (
    <section style={wiz.section}>
      <h2 style={wiz.h2}>Choose an ingestion path</h2>
      <p style={wiz.muted}>
        Tier 1 is the only path that shows Pilot program labeling. That label
        describes diagnostic expectations, not a contractual latency or
        availability SLO. Tiers 2 and 3 keep standard / best-effort honesty copy.
      </p>
      <div style={wiz.cards}>
        <button type="button" style={wiz.card} onClick={() => onPick(1)}>
          <TierBadge tier={1} />
          <strong style={wiz.cardTitle}>Synthetic / demo events</strong>
          <span style={wiz.cardBody}>
            Use for pilot validation. Pilot labeling applies here only — not for
            production connector traffic.
          </span>
        </button>
        <button type="button" style={wiz.card} onClick={() => onPick(2)}>
          <TierBadge tier={2} />
          <strong style={wiz.cardTitle}>Managed real connector</strong>
          <span style={wiz.cardBody}>
            Production-shaped source with standard operations expectations — no
            Pilot program badge.
          </span>
        </button>
        <button type="button" style={wiz.card} onClick={() => onPick(3)}>
          <TierBadge tier={3} />
          <strong style={wiz.cardTitle}>BYO / community connector</strong>
          <span style={wiz.cardBody}>
            Lowest coupling; availability and latency are best-effort until
            promoted.
          </span>
        </button>
      </div>
    </section>
  )
}

function ConfigureStep({
  tier,
  onBack,
  onContinue,
}: {
  tier: IngestionPathTier
  onBack: () => void
  onContinue: () => void
}) {
  const [source, setSource] = useState('demo-ui')
  const [eventsJson, setEventsJson] = useState(defaultEventsJson)

  if (tier === 1) {
    return (
      <section style={wiz.section}>
        <div style={wiz.rowBetween}>
          <h2 style={wiz.h2}>Configure synthetic batch</h2>
          <SloBadgeStrip tier={1} />
        </div>
        <label style={wiz.label}>
          Source
          <input
            style={wiz.input}
            value={source}
            onChange={(e) => setSource(e.target.value)}
            placeholder="connector or app id"
          />
        </label>
        <label style={wiz.label}>
          Events JSON
          <textarea
            style={wiz.textarea}
            value={eventsJson}
            onChange={(e) => setEventsJson(e.target.value)}
            spellCheck={false}
          />
        </label>
        <WizardStepNav
          onBack={onBack}
          primaryLabel="Continue to review"
          onPrimary={() => {
            sessionStorage.setItem(
              'canal:wizard:draft',
              JSON.stringify({ tier, source, eventsJson }),
            )
            onContinue()
          }}
        />
      </section>
    )
  }

  return (
    <section style={wiz.section}>
      <div style={wiz.rowBetween}>
        <h2 style={wiz.h2}>Connector setup</h2>
        <SloBadgeStrip tier={tier} />
      </div>
      <div style={wiz.empty}>
        <p style={{ margin: 0, fontWeight: 600 }}>Nothing to wire yet</p>
        <p style={wiz.muted}>
          This step is a deliberate empty state: credential pickers, schema
          mapping, and back-pressure controls ship in later slices. Tier labels
          above stay visible so operators never confuse this with the
          pilot-labeled synthetic path.
        </p>
      </div>
      <WizardStepNav
        onBack={onBack}
        primaryLabel="Continue to review"
        onPrimary={() => {
          sessionStorage.setItem(
            'canal:wizard:draft',
            JSON.stringify({ tier, source: null, eventsJson: null }),
          )
          onContinue()
        }}
      />
    </section>
  )
}

function ReviewStep({
  tier,
  onBack,
}: {
  tier: IngestionPathTier
  onBack: () => void
}) {
  const draft = useMemo(() => {
    try {
      const raw = sessionStorage.getItem('canal:wizard:draft')
      if (!raw) return null
      return JSON.parse(raw) as {
        tier: IngestionPathTier
        source: string | null
        eventsJson: string | null
      }
    } catch {
      return null
    }
  }, [])

  const effectiveTier = draft?.tier ?? tier

  const [ackNoSlo, setAckNoSlo] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [result, setResult] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const send = useCallback(async () => {
    if (effectiveTier !== 1) {
      if (!ackNoSlo) return
    }
    setError(null)
    setResult(null)
    if (effectiveTier !== 1) {
      setResult(
        JSON.stringify(
          {
            skipped: true,
            reason:
              'No batch POST for connector tiers in this shell — ship alongside connector runtime.',
          },
          null,
          2,
        ),
      )
      return
    }
    const source = draft?.source?.trim() || 'unknown'
    const eventsJson = draft?.eventsJson
    if (!eventsJson) {
      setError('Missing draft payload — go back to configure.')
      return
    }
    let parsed: unknown
    try {
      parsed = JSON.parse(eventsJson) as unknown
    } catch {
      setError('Events JSON is invalid.')
      return
    }
    if (!Array.isArray(parsed)) {
      setError('Events must be a JSON array.')
      return
    }
    const body: IngestBatchRequest = {
      source: source || 'unknown',
      events: parsed as IngestBatchRequest['events'],
    }
    setSubmitting(true)
    try {
      const res = await postIngestBatch(body)
      setResult(JSON.stringify(res, null, 2))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }, [ackNoSlo, draft, effectiveTier])

  return (
    <section style={wiz.section}>
      <h2 style={wiz.h2}>Review</h2>
      <SloBadgeStrip tier={effectiveTier} />
      {effectiveTier === 1 && (
        <p style={wiz.muted}>
          Sending uses the same Phase 1 <code>/v1/events</code> batch API as the
          classic shell. Only this tier shows Pilot program labeling; that is
          not a published customer SLA until CPO/CEO and Legal explicitly ship
          one.
        </p>
      )}
      {effectiveTier !== 1 && (
        <label style={wiz.checkRow}>
          <input
            type="checkbox"
            checked={ackNoSlo}
            onChange={(e) => setAckNoSlo(e.target.checked)}
          />
          <span>
            I understand this path does not carry pilot program labeling — I am
            not treating results as pilot-tier diagnostics.
          </span>
        </label>
      )}
      {error && <p style={wiz.error}>{error}</p>}
      {result && <pre style={wiz.pre}>{result}</pre>}
      <div style={wiz.navRow}>
        <button type="button" style={wiz.secondaryBtn} onClick={onBack}>
          Back
        </button>
        <button
          type="button"
          style={wiz.primaryBtn}
          onClick={send}
          disabled={submitting || (effectiveTier !== 1 && !ackNoSlo)}
        >
          {effectiveTier === 1
            ? submitting
              ? 'Sending…'
              : 'POST /v1/events'
            : 'Acknowledge (no POST in shell)'}
        </button>
      </div>
    </section>
  )
}

function WizardStepNav({
  onBack,
  onPrimary,
  primaryLabel,
}: {
  onBack: () => void
  onPrimary: () => void
  primaryLabel: string
}) {
  return (
    <div style={wiz.navRow}>
      <button type="button" style={wiz.secondaryBtn} onClick={onBack}>
        Back
      </button>
      <button type="button" style={wiz.primaryBtn} onClick={onPrimary}>
        {primaryLabel}
      </button>
    </div>
  )
}

const wiz: Record<string, CSSProperties> = {
  layout: {
    minHeight: '100vh',
    boxSizing: 'border-box',
    background: 'var(--bg)',
    maxWidth: 920,
    margin: '0 auto',
    padding: '1.75rem 1.5rem 3rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '1.5rem',
  },
  topBar: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: '1rem',
    flexWrap: 'wrap',
  },
  title: { margin: 0, fontSize: '1.5rem', fontWeight: 700 },
  sub: { margin: '0.35rem 0 0', color: 'var(--muted)', fontSize: '0.9rem' },
  ghostBtn: {
    padding: '0.45rem 0.75rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'transparent',
    color: 'var(--text)',
    cursor: 'pointer',
  },
  steps: {
    listStyle: 'none',
    margin: 0,
    padding: 0,
    display: 'flex',
    gap: '1rem',
    flexWrap: 'wrap',
  },
  stepLi: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.4rem',
    fontSize: '0.85rem',
    color: 'var(--muted)',
  },
  stepNum: {
    display: 'inline-flex',
    width: '1.35rem',
    height: '1.35rem',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '999px',
    border: '1px solid var(--border)',
    fontSize: '0.75rem',
  },
  section: {
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    background: 'var(--surface)',
    padding: '1.25rem 1.35rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem',
  },
  h2: { margin: 0, fontSize: '1.15rem' },
  muted: { margin: 0, color: 'var(--muted)', lineHeight: 1.5 },
  cards: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
    gap: '0.85rem',
  },
  card: {
    textAlign: 'left',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'flex-start',
    gap: '0.55rem',
    padding: '1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text)',
    cursor: 'pointer',
  },
  cardTitle: { fontSize: '1rem' },
  cardBody: { fontSize: '0.85rem', color: 'var(--muted)', fontWeight: 400 },
  rowBetween: {
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'space-between',
    gap: '1rem',
    alignItems: 'flex-start',
  },
  label: { display: 'flex', flexDirection: 'column', gap: '0.35rem' },
  input: {
    padding: '0.5rem 0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text)',
  },
  textarea: {
    minHeight: 200,
    padding: '0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text)',
    fontFamily: 'var(--mono)',
    fontSize: '0.85rem',
    resize: 'vertical',
  },
  empty: {
    border: '1px dashed var(--border)',
    borderRadius: 'var(--radius)',
    padding: '1.25rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.5rem',
  },
  navRow: {
    display: 'flex',
    gap: '0.65rem',
    flexWrap: 'wrap',
    marginTop: '0.25rem',
  },
  primaryBtn: {
    padding: '0.45rem 0.9rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--accent)',
    background: 'var(--accent-muted)',
    color: 'var(--text)',
    cursor: 'pointer',
  },
  secondaryBtn: {
    padding: '0.45rem 0.9rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    color: 'var(--text)',
    cursor: 'pointer',
  },
  checkRow: {
    display: 'flex',
    gap: '0.55rem',
    alignItems: 'flex-start',
    fontSize: '0.9rem',
    cursor: 'pointer',
  },
  error: { color: 'var(--danger)', margin: 0 },
  pre: {
    margin: 0,
    padding: '1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--bg)',
    fontFamily: 'var(--mono)',
    fontSize: '0.82rem',
    overflow: 'auto',
  },
}

import { useCallback, useEffect, useState, type CSSProperties } from 'react'
import { getHealth, getStreamProbe, postIngestBatch } from './api/client'
import type { HealthResponse, IngestBatchRequest } from './api/types'
import {
  CONNECTOR_TIER_BADGE_KEY,
  type ConnectorTier,
} from './operator/constants'
import { OperatorWizardShell } from './operator/OperatorWizardShell'
import {
  clearWizardFromUrl,
  isWizardDeepLinkActive,
  openWizardInUrl,
  shouldOfferWizardNav,
} from './operator/wizardGate'

type NavKey = 'overview' | 'ingest' | 'stream'

const defaultEventsJson = `[
  {
    "id": "demo-1",
    "type": "page.view",
    "occurredAt": "${new Date().toISOString()}",
    "payload": { "path": "/" }
  }
]`

export default function App() {
  const [wizardOpen, setWizardOpen] = useState(isWizardDeepLinkActive)
  const [nav, setNav] = useState<NavKey>('overview')
  const [connectorTier, setConnectorTier] = useState<ConnectorTier>('standard')
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [healthError, setHealthError] = useState<string | null>(null)
  const [loadingHealth, setLoadingHealth] = useState(false)

  useEffect(() => {
    const sync = () => setWizardOpen(isWizardDeepLinkActive())
    window.addEventListener('popstate', sync)
    return () => window.removeEventListener('popstate', sync)
  }, [])

  const refreshHealth = useCallback(async () => {
    setLoadingHealth(true)
    setHealthError(null)
    try {
      setHealth(await getHealth())
    } catch (e) {
      setHealth(null)
      setHealthError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoadingHealth(false)
    }
  }, [])

  if (wizardOpen) {
    return (
      <OperatorWizardShell
        onExit={() => {
          clearWizardFromUrl()
          setWizardOpen(false)
        }}
      />
    )
  }

  return (
    <div style={styles.layout}>
      <aside style={styles.aside}>
        <div style={styles.brand}>
          <strong>Canal</strong>
          <span style={styles.brandSub}>Ingestion MVP</span>
        </div>
        <nav style={styles.nav}>
          {(
            [
              ['overview', 'Overview'],
              ['ingest', 'Send batch'],
              ['stream', 'Live stream'],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              type="button"
              style={{
                ...styles.navBtn,
                ...(nav === key ? styles.navBtnActive : {}),
              }}
              onClick={() => setNav(key)}
            >
              {label}
            </button>
          ))}
        </nav>
        {(shouldOfferWizardNav() || isWizardDeepLinkActive()) && (
          <div style={styles.wizardPromo}>
            <button
              type="button"
              style={styles.wizardBtn}
              onClick={() => {
                openWizardInUrl()
                setWizardOpen(true)
              }}
            >
              Operator ingestion wizard
            </button>
            <span style={styles.wizardHint}>
              Guided steps + tier / SLO honesty. Deep link:{' '}
              <code>?operatorWizard=1</code>
            </span>
          </div>
        )}
        <p style={styles.hint}>
          Dev server proxies <code>/health</code> and <code>/v1/*</code> to{' '}
          <code>127.0.0.1:8080</code>. Run{' '}
          <code>@canal/ingestion-api</code> locally for live responses.
        </p>
      </aside>
      <main style={styles.main}>
        {nav === 'overview' && (
          <OverviewPanel
            connectorTier={connectorTier}
            health={health}
            error={healthError}
            loading={loadingHealth}
            onRefresh={refreshHealth}
          />
        )}
        {nav === 'ingest' && (
          <IngestPanel
            connectorTier={connectorTier}
            onConnectorTierChange={setConnectorTier}
          />
        )}
        {nav === 'stream' && <StreamPanel connectorTier={connectorTier} />}
      </main>
    </div>
  )
}

function OverviewPanel({
  connectorTier,
  health,
  error,
  loading,
  onRefresh,
}: {
  connectorTier: ConnectorTier
  health: HealthResponse | null
  error: string | null
  loading: boolean
  onRefresh: () => void
}) {
  const statusAnnouncement = loading
    ? 'Checking service health.'
    : error
      ? `Health check failed: ${error}`
      : health
        ? `Health check succeeded for ${health.service}, status ${health.status}.`
        : 'Press Refresh to load health from the ingestion API.'

  return (
    <section style={styles.panel}>
      <header style={styles.panelHeader}>
        <h1 style={styles.h1}>Service health</h1>
        <button type="button" style={styles.primaryBtn} onClick={onRefresh}>
          {loading ? 'Checking…' : 'Refresh'}
        </button>
      </header>
      <p style={styles.statusLine} aria-live="polite" aria-atomic="true">
        {statusAnnouncement}
      </p>
      {connectorTier === 'pilot' && (
        <p style={styles.muted}>
          Pilot connector context: this panel shows API liveness only, not an
          end-to-end or contractual SLO for your traffic.
        </p>
      )}
      {error && <p style={styles.error}>{error}</p>}
      {health && (
        <pre style={styles.pre}>{JSON.stringify(health, null, 2)}</pre>
      )}
      {!health && !error && !loading && (
        <p style={styles.muted}>Load health from the ingestion API.</p>
      )}
    </section>
  )
}

function IngestPanel({
  connectorTier,
  onConnectorTierChange,
}: {
  connectorTier: ConnectorTier
  onConnectorTierChange: (t: ConnectorTier) => void
}) {
  const [source, setSource] = useState('demo-ui')
  const [eventsJson, setEventsJson] = useState(defaultEventsJson)
  const [result, setResult] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const submit = async () => {
    setError(null)
    setResult(null)
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
      source: source.trim() || 'unknown',
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
  }

  return (
    <section style={styles.panel}>
      <header style={styles.panelHeader}>
        <h1 style={styles.h1}>Send ingestion batch</h1>
        <button
          type="button"
          style={styles.primaryBtn}
          onClick={submit}
          disabled={submitting}
        >
          {submitting ? 'Sending…' : 'POST /v1/events'}
        </button>
      </header>
      <ConnectorTierFieldset
        value={connectorTier}
        onChange={onConnectorTierChange}
      />
      {connectorTier === 'pilot' && <PilotProgramCallout />}
      <label style={styles.label}>
        Source
        <input
          style={styles.input}
          value={source}
          onChange={(e) => setSource(e.target.value)}
          placeholder="connector or app id"
        />
      </label>
      <label style={styles.label}>
        Events JSON
        <textarea
          style={styles.textarea}
          value={eventsJson}
          onChange={(e) => setEventsJson(e.target.value)}
          spellCheck={false}
        />
      </label>
      {error && <p style={styles.error}>{error}</p>}
      {result && <pre style={styles.pre}>{result}</pre>}
    </section>
  )
}

function ConnectorTierFieldset({
  value,
  onChange,
}: {
  value: ConnectorTier
  onChange: (t: ConnectorTier) => void
}) {
  const tierId = 'connector-tier'
  return (
    <fieldset
      style={styles.tierFieldset}
      aria-describedby={`${tierId}-hint`}
    >
      <legend style={styles.tierLegend}>Connector tier</legend>
      <p id={`${tierId}-hint`} style={styles.muted}>
        Labels operator expectations for support and SLO honesty copy. Future:
        persist on connector metadata; today it is session-local in this shell.
      </p>
      <div style={styles.tierOptions} role="presentation">
        {(
          [
            [
              'pilot',
              'Pilot',
              'Best-effort path; show pilot program honesty disclaimers.',
            ],
            [
              'standard',
              'Standard',
              'Phase 1 HTTP batch contract; neutral honesty copy.',
            ],
            [
              'priority_reserved',
              'Priority (reserved)',
              'Not selectable in MVP — requires CPO/CEO alignment before enablement.',
            ],
          ] as const
        ).map(([id, label, description]) => {
          const disabled = id === 'priority_reserved'
          const tier = id as ConnectorTier
          return (
            <label
              key={id}
              data-badge-key={CONNECTOR_TIER_BADGE_KEY[tier]}
              style={{
                ...styles.tierOption,
                ...(disabled ? styles.tierOptionDisabled : {}),
              }}
            >
              <input
                type="radio"
                name={tierId}
                value={id}
                checked={value === id}
                disabled={disabled}
                onChange={() => onChange(tier)}
                style={styles.tierRadio}
              />
              <span style={styles.tierOptionText}>
                <span style={styles.tierOptionLabel}>{label}</span>
                <span style={styles.tierOptionDesc}>{description}</span>
              </span>
            </label>
          )
        })}
      </div>
    </fieldset>
  )
}

function PilotProgramCallout() {
  return (
    <aside
      role="region"
      aria-label="Pilot program limitations"
      style={styles.calloutPilot}
    >
      <p style={styles.calloutTitle}>Pilot program — honesty</p>
      <p style={styles.calloutBody}>
        Throughput, error handling, and latency follow best-effort operations.
        Metrics are diagnostic, not contractual. Do not publish customer-facing
        SLO figures for this connector without CPO/CEO sign-off.
      </p>
    </aside>
  )
}

function StreamingHonestyCallout({
  connectorTier,
}: {
  connectorTier: ConnectorTier
}) {
  const pilot = connectorTier === 'pilot'
  return (
    <aside
      role="region"
      aria-label={
        pilot
          ? 'Realtime streaming limitations for pilot connectors'
          : 'Realtime streaming limitations'
      }
      style={pilot ? styles.calloutPilot : styles.calloutNeutral}
    >
      <p style={styles.calloutTitle}>
        {pilot ? 'Pilot + streaming' : 'Streaming'} — what Phase 1 guarantees
      </p>
      <ol style={styles.calloutList}>
        <li>
          Batch ingest is supported via{' '}
          <code style={styles.codeInherit}>POST /v1/events</code> per{' '}
          <code style={styles.codeInherit}>ingestion-v1.openapi.yaml</code>.
        </li>
        <li>
          <code style={styles.codeInherit}>GET /v1/stream</code> is a documented
          stub (often <code style={styles.codeInherit}>501</code> until SSE or
          WebSocket ships). It is not an availability or latency commitment.
        </li>
        {pilot && (
          <li>
            For pilot connectors, treat any successful probe as non-binding:
            behavior may change without notice during the pilot window.
          </li>
        )}
      </ol>
    </aside>
  )
}

function StreamPanel({ connectorTier }: { connectorTier: ConnectorTier }) {
  const [probe, setProbe] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const runProbe = async () => {
    setLoading(true)
    setError(null)
    setProbe(null)
    try {
      const { status, body } = await getStreamProbe()
      setProbe(JSON.stringify({ status, body }, null, 2))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <section style={styles.panel}>
      <header style={styles.panelHeader}>
        <h1 style={styles.h1}>Live stream</h1>
        <button
          type="button"
          style={styles.secondaryBtn}
          onClick={runProbe}
          disabled={loading}
        >
          {loading ? 'Probing…' : 'Probe GET /v1/stream'}
        </button>
      </header>
      <StreamingHonestyCallout connectorTier={connectorTier} />
      {error && <p style={styles.error}>{error}</p>}
      {probe && <pre style={styles.pre}>{probe}</pre>}
    </section>
  )
}

const styles: Record<string, CSSProperties> = {
  layout: {
    display: 'grid',
    gridTemplateColumns: '240px 1fr',
    minHeight: '100vh',
  },
  aside: {
    borderRight: '1px solid var(--border)',
    background: 'var(--surface)',
    padding: '1.25rem 1rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem',
  },
  brand: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.15rem',
    fontSize: '1rem',
  },
  brandSub: {
    color: 'var(--muted)',
    fontSize: '0.8rem',
    fontWeight: 400,
  },
  nav: { display: 'flex', flexDirection: 'column', gap: '0.35rem' },
  navBtn: {
    textAlign: 'left',
    padding: '0.55rem 0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px solid transparent',
    background: 'transparent',
    color: 'var(--text)',
    cursor: 'pointer',
  },
  navBtnActive: {
    background: 'var(--accent-muted)',
    borderColor: 'var(--accent)',
  },
  wizardPromo: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.35rem',
    paddingTop: '0.5rem',
    borderTop: '1px solid var(--border)',
  },
  wizardBtn: {
    textAlign: 'left',
    padding: '0.55rem 0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px dashed var(--accent)',
    background: 'rgba(88, 166, 255, 0.08)',
    color: 'var(--text)',
    cursor: 'pointer',
    fontSize: '0.85rem',
    fontWeight: 600,
  },
  wizardHint: {
    fontSize: '0.7rem',
    color: 'var(--muted)',
    lineHeight: 1.4,
  },
  hint: {
    marginTop: 'auto',
    fontSize: '0.75rem',
    color: 'var(--muted)',
    lineHeight: 1.45,
  },
  main: { padding: '1.75rem 2rem', maxWidth: 960 },
  panel: { display: 'flex', flexDirection: 'column', gap: '1rem' },
  panelHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: '1rem',
    flexWrap: 'wrap',
  },
  h1: { margin: 0, fontSize: '1.35rem', fontWeight: 600 },
  statusLine: {
    margin: 0,
    fontSize: '0.95rem',
    color: 'var(--text)',
    minHeight: '1.5em',
  },
  tierFieldset: {
    margin: 0,
    padding: '0.75rem 1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.65rem',
  },
  tierLegend: {
    padding: 0,
    fontSize: '0.95rem',
    fontWeight: 600,
  },
  tierOptions: { display: 'flex', flexDirection: 'column', gap: '0.5rem' },
  tierOption: {
    display: 'flex',
    alignItems: 'flex-start',
    gap: '0.5rem',
    padding: '0.5rem 0.45rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    cursor: 'pointer',
  },
  tierOptionDisabled: {
    opacity: 0.55,
    cursor: 'not-allowed',
    background: 'var(--surface-hover)',
  },
  tierRadio: { marginTop: '0.2rem', accentColor: 'var(--accent)' },
  tierOptionText: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.15rem',
  },
  tierOptionLabel: { fontWeight: 500 },
  tierOptionDesc: { fontSize: '0.8rem', color: 'var(--muted)' },
  calloutPilot: {
    margin: 0,
    padding: '0.85rem 1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--callout-pilot-border)',
    background: 'var(--callout-pilot-bg)',
  },
  calloutNeutral: {
    margin: 0,
    padding: '0.85rem 1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--callout-neutral-border)',
    background: 'var(--callout-neutral-bg)',
  },
  calloutTitle: { margin: '0 0 0.35rem', fontWeight: 600, fontSize: '0.9rem' },
  calloutBody: { margin: 0, fontSize: '0.85rem', lineHeight: 1.5 },
  calloutList: {
    margin: 0,
    paddingLeft: '1.2rem',
    fontSize: '0.85rem',
    lineHeight: 1.55,
    display: 'flex',
    flexDirection: 'column',
    gap: '0.35rem',
  },
  codeInherit: {
    fontFamily: 'var(--mono)',
    fontSize: '0.9em',
    background: 'rgba(230, 237, 243, 0.08)',
    padding: '0.05rem 0.25rem',
    borderRadius: 4,
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
  muted: { color: 'var(--muted)', margin: 0 },
  error: { color: 'var(--danger)', margin: 0 },
  pre: {
    margin: 0,
    padding: '1rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    fontFamily: 'var(--mono)',
    fontSize: '0.85rem',
    overflow: 'auto',
  },
  label: { display: 'flex', flexDirection: 'column', gap: '0.35rem' },
  input: {
    padding: '0.5rem 0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    color: 'var(--text)',
  },
  textarea: {
    minHeight: 220,
    padding: '0.65rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    color: 'var(--text)',
    fontFamily: 'var(--mono)',
    fontSize: '0.85rem',
    resize: 'vertical',
  },
}

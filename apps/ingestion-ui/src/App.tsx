import { useCallback, useState, type CSSProperties } from 'react'
import { getHealth, getStreamProbe, postIngestBatch } from './api/client'
import type { HealthResponse, IngestBatchRequest } from './api/types'

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
  const [nav, setNav] = useState<NavKey>('overview')
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [healthError, setHealthError] = useState<string | null>(null)
  const [loadingHealth, setLoadingHealth] = useState(false)

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
        <p style={styles.hint}>
          Dev server proxies <code>/health</code> and <code>/v1/*</code> to{' '}
          <code>127.0.0.1:8080</code>. Run{' '}
          <code>@canal/ingestion-api</code> locally for live responses.
        </p>
      </aside>
      <main style={styles.main}>
        {nav === 'overview' && (
          <OverviewPanel
            health={health}
            error={healthError}
            loading={loadingHealth}
            onRefresh={refreshHealth}
          />
        )}
        {nav === 'ingest' && <IngestPanel />}
        {nav === 'stream' && <StreamPanel />}
      </main>
    </div>
  )
}

function OverviewPanel({
  health,
  error,
  loading,
  onRefresh,
}: {
  health: HealthResponse | null
  error: string | null
  loading: boolean
  onRefresh: () => void
}) {
  return (
    <section style={styles.panel}>
      <header style={styles.panelHeader}>
        <h1 style={styles.h1}>Service health</h1>
        <button type="button" style={styles.primaryBtn} onClick={onRefresh}>
          {loading ? 'Checking…' : 'Refresh'}
        </button>
      </header>
      {error && <p style={styles.error}>{error}</p>}
      {health && (
        <pre style={styles.pre}>{JSON.stringify(health, null, 2)}</pre>
      )}
      {!health && !error && (
        <p style={styles.muted}>Load health from the ingestion API.</p>
      )}
    </section>
  )
}

function IngestPanel() {
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

function StreamPanel() {
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
      <p style={styles.muted}>
        Phase 1 contract documents this route as a stub (typically{' '}
        <code>501</code> until SSE/WebSocket ships). Clients must not depend on
        it for MVP flows.
      </p>
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

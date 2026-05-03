import { useCallback, useEffect, useMemo, useState, type CSSProperties } from 'react'
import {
  getControlCanalSegments,
  getControlPipeline,
  postIngestBatch,
} from '../api/client'
import type {
  AdapterInstancePlaceholder,
  CanalSegmentsRead,
  IngestBatchRequest,
  PipelineSummaryRead,
} from '../api/types'
import {
  catalogTierForIngestionPath,
  catalogTierKindKey,
  type IngestionPathTier,
} from './constants'
import { OperatorControlSummary } from './OperatorControlSummary'
import { SloBadgeStrip, TierBadge } from './TierSloBadges'

type WizardStep = 'path' | 'configure' | 'review'

type WizardDraft = {
  tier: IngestionPathTier
  source: string | null
  eventsJson: string | null
  /** Selected control read-model adapter; session-local until persistence (CAN-51). */
  selectedAdapterInstanceId: string | null
  control?: { pipeline: PipelineSummaryRead; canal: CanalSegmentsRead }
}

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
            Tier and pilot-program honesty labels per path (CAN-28). Connector
            tiers load live Phase 1 control read models on configure (
            <code style={wiz.subCode}>GET /v1/control/pipeline</code>,{' '}
            <code style={wiz.subCode}>GET /v1/control/canal/segments</code>
            ) — if configure still shows the legacy empty-state copy, the browser is
            serving a stale bundle; hard-refresh and restart{' '}
            <code style={wiz.subCode}>npm run dev</code> from current{' '}
            <code style={wiz.subCode}>main</code>.
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

type ControlLoadState =
  | { status: 'loading' }
  | { status: 'ok'; pipeline: PipelineSummaryRead; canal: CanalSegmentsRead }
  | { status: 'error'; message: string }

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
  const [control, setControl] = useState<ControlLoadState>({ status: 'loading' })
  const [reloadToken, setReloadToken] = useState(0)
  const [selectedAdapterId, setSelectedAdapterId] = useState<string | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    ;(async () => {
      try {
        const [pipeline, canal] = await Promise.all([
          getControlPipeline(),
          getControlCanalSegments(),
        ])
        if (ac.signal.aborted) return
        setControl({ status: 'ok', pipeline, canal })
      } catch (e) {
        if (ac.signal.aborted) return
        setControl({
          status: 'error',
          message: e instanceof Error ? e.message : String(e),
        })
      }
    })()
    return () => ac.abort()
  }, [tier, reloadToken])

  const catalogTier = catalogTierForIngestionPath(tier)
  const controlGateOk = tier === 1 || control.status === 'ok'

  const filteredAdapters = useMemo((): AdapterInstancePlaceholder[] => {
    if (control.status !== 'ok') return []
    return control.pipeline.adapterInstances.filter((a) => a.catalogTier === catalogTier)
  }, [control, catalogTier])

  const adapterIdsKey = filteredAdapters.map((a) => a.id).join('|')

  useEffect(() => {
    if (control.status !== 'ok') {
      setSelectedAdapterId(null)
      return
    }
    const list = control.pipeline.adapterInstances.filter((a) => a.catalogTier === catalogTier)
    setSelectedAdapterId((prev) => {
      if (prev && list.some((a) => a.id === prev)) return prev
      return list[0]?.id ?? null
    })
  }, [control, catalogTier, tier, reloadToken, adapterIdsKey])

  const persistDraft = (
    partial: Pick<WizardDraft, 'source' | 'eventsJson' | 'selectedAdapterInstanceId'>,
  ) => {
    const base: WizardDraft = {
      tier,
      source: partial.source ?? null,
      eventsJson: partial.eventsJson ?? null,
      selectedAdapterInstanceId: partial.selectedAdapterInstanceId ?? null,
    }
    if (control.status === 'ok') {
      base.control = { pipeline: control.pipeline, canal: control.canal }
    }
    sessionStorage.setItem('canal:wizard:draft', JSON.stringify(base))
  }

  const configureContinueDisabledTier1 =
    control.status === 'ok' &&
    (filteredAdapters.length === 0 || selectedAdapterId === null)

  const configureContinueDisabledTier23 =
    !controlGateOk ||
    (control.status === 'ok' &&
      (filteredAdapters.length === 0 || selectedAdapterId === null))

  if (tier === 1) {
    return (
      <section style={wiz.section}>
        <div style={wiz.rowBetween}>
          <h2 style={wiz.h2}>Configure synthetic batch</h2>
          <SloBadgeStrip tier={1} />
        </div>
        {control.status === 'loading' && (
          <p style={wiz.muted}>Loading control read models…</p>
        )}
        {control.status === 'error' && (
          <p style={wiz.error}>
            Control API: {control.message} — you can still send a synthetic batch;
            review will not include control snapshots.
          </p>
        )}
        {control.status === 'ok' && (
          <>
            <OperatorControlSummary
              pipeline={control.pipeline}
              canal={control.canal}
              highlightCatalogTier={catalogTier}
            />
            <AdapterCatalogPicker
              adapters={filteredAdapters}
              value={selectedAdapterId}
              onChange={setSelectedAdapterId}
            />
          </>
        )}
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
          primaryDisabled={configureContinueDisabledTier1}
          onPrimary={() => {
            persistDraft({
              source,
              eventsJson,
              selectedAdapterInstanceId: selectedAdapterId,
            })
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
      <p style={wiz.muted}>
        Pick a catalog adapter instance for this path (B3). Credential pickers and
        connector runtime wiring ship in later slices; choice is session-local until
        adapter persistence lands. The control read models below are live from the
        ingestion API.
      </p>
      {control.status === 'loading' && (
        <p style={wiz.muted}>Loading control read models…</p>
      )}
      {control.status === 'error' && (
        <div style={wiz.empty}>
          <p style={{ margin: 0, fontWeight: 600 }}>Could not load control read models</p>
          <p style={wiz.error}>{control.message}</p>
          <button
            type="button"
            style={wiz.secondaryBtn}
            onClick={() => {
              setControl({ status: 'loading' })
              setReloadToken((n) => n + 1)
            }}
          >
            Retry
          </button>
        </div>
      )}
      {control.status === 'ok' && (
        <>
          <OperatorControlSummary
            pipeline={control.pipeline}
            canal={control.canal}
            highlightCatalogTier={catalogTier}
          />
          <AdapterCatalogPicker
            adapters={filteredAdapters}
            value={selectedAdapterId}
            onChange={setSelectedAdapterId}
          />
        </>
      )}
      <WizardStepNav
        onBack={onBack}
        primaryLabel="Continue to review"
        primaryDisabled={configureContinueDisabledTier23}
        onPrimary={() => {
          persistDraft({
            source: null,
            eventsJson: null,
            selectedAdapterInstanceId: selectedAdapterId,
          })
          onContinue()
        }}
      />
    </section>
  )
}

function AdapterCatalogPicker({
  adapters,
  value,
  onChange,
}: {
  adapters: AdapterInstancePlaceholder[]
  value: string | null
  onChange: (id: string) => void
}) {
  if (adapters.length === 0) return null
  return (
    <fieldset style={wiz.fs}>
      <legend style={wiz.fsLegend}>Connector instance (catalog)</legend>
      <p style={wiz.muted}>
        OpenAPI <code style={wiz.subCode}>catalogTier</code> maps to B3 keys{' '}
        <code style={wiz.subCode}>tier1_synthetic</code> vs{' '}
        <code style={wiz.subCode}>tier2_real</code> (see{' '}
        <code style={wiz.subCode}>catalogTierKindKey</code>).
      </p>
      <div style={wiz.pickerCol}>
        {adapters.map((a) => {
          const kind = catalogTierKindKey(a.catalogTier)
          const sel = value === a.id
          return (
            <label
              key={a.id}
              style={{
                ...wiz.pickRow,
                ...(sel ? { borderColor: 'var(--accent)' } : {}),
              }}
            >
              <input
                type="radio"
                name="adapter-catalog"
                value={a.id}
                checked={sel}
                onChange={() => onChange(a.id)}
                data-catalog-kind={kind}
              />
              <span style={wiz.pickBody}>
                <strong>{a.displayName}</strong>
                <span style={wiz.pickMeta}>
                  {a.stageKey} · <code style={wiz.subCode}>{a.catalogTier}</code> · {kind}
                </span>
              </span>
            </label>
          )
        })}
      </div>
    </fieldset>
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
      return JSON.parse(raw) as WizardDraft
    } catch {
      return null
    }
  }, [])

  const effectiveTier = draft?.tier ?? tier

  const selectedCatalogAdapter = useMemo((): AdapterInstancePlaceholder | null => {
    if (!draft?.control?.pipeline || !draft.selectedAdapterInstanceId) return null
    return (
      draft.control.pipeline.adapterInstances.find((a) => a.id === draft.selectedAdapterInstanceId) ??
      null
    )
  }, [draft])

  const tier2RealAdapterSelected = selectedCatalogAdapter?.catalogTier === 'tier-2'

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
      const c = draft?.control
      if (!c) {
        setError('Control read models missing — return to configure and wait for the API.')
        return
      }
      setResult(
        JSON.stringify(
          {
            wizardPathAcknowledged: true,
            batchIngestDeferred:
              'POST /v1/events remains Tier 1 synthetic in this Phase 1 happy path.',
            selectedAdapterInstanceId: draft?.selectedAdapterInstanceId ?? null,
            selectedCatalogAdapter: selectedCatalogAdapter ?? null,
            controlPlane: c,
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
      if (draft?.control) {
        setResult(
          JSON.stringify(
            {
              ingest: res,
              controlPlane: draft.control,
              selectedAdapterInstanceId: draft.selectedAdapterInstanceId ?? null,
              selectedCatalogAdapter: selectedCatalogAdapter ?? null,
            },
            null,
            2,
          ),
        )
      } else {
        setResult(JSON.stringify(res, null, 2))
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }, [ackNoSlo, draft, effectiveTier, selectedCatalogAdapter])

  return (
    <section style={wiz.section}>
      <h2 style={wiz.h2}>Review</h2>
      <SloBadgeStrip tier={effectiveTier} />
      {effectiveTier === 1 && (
        <p style={wiz.muted}>
          Sending uses the same Phase 1 <code>/v1/events</code> batch API as the
          classic shell. When configure loaded control read models, they are
          echoed below after send. Only this tier shows Pilot program labeling;
          that is not a published customer SLA until CPO/CEO and Legal explicitly
          ship one.
        </p>
      )}
      {effectiveTier !== 1 && (
        <>
          <p style={wiz.muted}>
            This path completes the control-plane happy path: acknowledge honesty
            copy, then persist the snapshot from{' '}
            <code style={{ fontFamily: 'var(--mono)', fontSize: '0.85em' }}>
              GET /v1/control/*
            </code>{' '}
            captured in configure. No batch POST here in Phase 1.
          </p>
          {tier2RealAdapterSelected && (
            <p
              style={wiz.tier2Honesty}
              data-honesty-key="wizard.tier2_real.pilot_slo_disclaimer"
            >
              <strong>Pilot SLO does not apply</strong> — the selected connector instance
              is catalog-classified as managed real (<code style={wiz.subCode}>tier2_real</code>
              ). No pilot program labeling; reliability follows standard connector operations,
              not pilot-tier diagnostics.
            </p>
          )}
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
        </>
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
            : 'Acknowledge control snapshot'}
        </button>
      </div>
    </section>
  )
}

function WizardStepNav({
  onBack,
  onPrimary,
  primaryLabel,
  primaryDisabled,
}: {
  onBack: () => void
  onPrimary: () => void
  primaryLabel: string
  primaryDisabled?: boolean
}) {
  return (
    <div style={wiz.navRow}>
      <button type="button" style={wiz.secondaryBtn} onClick={onBack}>
        Back
      </button>
      <button
        type="button"
        style={wiz.primaryBtn}
        onClick={onPrimary}
        disabled={primaryDisabled}
      >
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
  subCode: {
    fontFamily: 'var(--mono)',
    fontSize: '0.82em',
    wordBreak: 'break-word',
  },
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
  tier2Honesty: {
    margin: 0,
    padding: '0.75rem 0.85rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    background: 'var(--bg)',
    fontSize: '0.9rem',
    lineHeight: 1.5,
  },
  fs: {
    margin: 0,
    padding: '0.65rem 0.85rem',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.55rem',
  },
  fsLegend: { fontSize: '0.9rem', fontWeight: 600, padding: '0 0.25rem' },
  pickerCol: { display: 'flex', flexDirection: 'column', gap: '0.45rem' },
  pickRow: {
    display: 'flex',
    gap: '0.55rem',
    alignItems: 'flex-start',
    padding: '0.55rem 0.65rem',
    borderRadius: 'var(--radius)',
    borderWidth: 1,
    borderStyle: 'solid',
    borderColor: 'var(--border)',
    cursor: 'pointer',
    background: 'var(--bg)',
  },
  pickBody: { display: 'flex', flexDirection: 'column', gap: '0.2rem' },
  pickMeta: { fontSize: '0.8rem', color: 'var(--muted)' },
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

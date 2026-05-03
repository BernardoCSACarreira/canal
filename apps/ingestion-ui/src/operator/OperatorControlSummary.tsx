import type { CSSProperties } from 'react'
import type { CanalSegmentsRead, CatalogTier, PipelineSummaryRead } from '../api/types'
import { catalogTierKindKey } from './constants'

const ADAPTER_PANEL_HEADING_ID = 'operator-control-adapters-heading'

export function OperatorControlSummary({
  pipeline,
  canal,
  highlightCatalogTier,
  adapterPicker,
}: {
  pipeline: PipelineSummaryRead
  canal: CanalSegmentsRead
  /** When set, adapter list is filtered to this charter tier (wizard lane). */
  highlightCatalogTier: CatalogTier | null
  /** When set (wizard configure), adapters render as an interactive radio list (B3). */
  adapterPicker?: { value: string | null; onChange: (id: string) => void }
}) {
  const adapters = highlightCatalogTier
    ? pipeline.adapterInstances.filter((a) => a.catalogTier === highlightCatalogTier)
    : pipeline.adapterInstances

  const usePicker = Boolean(adapterPicker && highlightCatalogTier)

  return (
    <div style={cs.wrap}>
      <p style={cs.lead}>
        Contract <code style={cs.code}>{pipeline.contractVersion}</code> —{' '}
        <code style={cs.code}>GET /v1/control/pipeline</code> +{' '}
        <code style={cs.code}>GET /v1/control/canal/segments</code>
      </p>

      <div style={cs.grid}>
        <section style={cs.panel}>
          <h3 style={cs.h3}>Pipeline stages</h3>
          <ol style={cs.ol}>
            {pipeline.stages.map((s) => (
              <li key={s.key} style={cs.stageLi}>
                <span style={cs.ord}>{s.ordinal}</span>
                <span>{s.title}</span>
                <code style={cs.subcode}>{s.key}</code>
              </li>
            ))}
          </ol>
        </section>

        <section style={cs.panel}>
          <h3 style={cs.h3} id={ADAPTER_PANEL_HEADING_ID}>
            {highlightCatalogTier
              ? usePicker
                ? `Adapters (${highlightCatalogTier}) — pick one`
                : `Adapters (${highlightCatalogTier})`
              : 'Adapter placeholders'}
          </h3>
          {usePicker && (
            <p style={cs.pickerHint}>
              B3: choose the catalog instance with the <strong>radio button</strong> on each
              row (not the static list alone).
            </p>
          )}
          {adapters.length === 0 ? (
            <p style={cs.muted}>No adapter placeholders for this tier in the read model.</p>
          ) : (
            <ul
              style={usePicker ? { ...cs.ul, listStyle: 'none', paddingLeft: 0 } : cs.ul}
              role={usePicker ? 'radiogroup' : undefined}
              aria-labelledby={usePicker ? ADAPTER_PANEL_HEADING_ID : undefined}
            >
              {adapters.map((a) => {
                if (usePicker && adapterPicker) {
                  const kind = catalogTierKindKey(a.catalogTier)
                  const sel = adapterPicker.value === a.id
                  return (
                    <li key={a.id} style={{ listStyle: 'none' }}>
                      <label
                        style={{
                          ...cs.pickLabel,
                          borderColor: sel ? 'var(--accent)' : 'var(--border)',
                        }}
                      >
                        <input
                          type="radio"
                          name="wizard-adapter-catalog"
                          value={a.id}
                          checked={sel}
                          onChange={() => adapterPicker.onChange(a.id)}
                          data-catalog-kind={kind}
                          aria-label={`${a.displayName}, catalog ${a.catalogTier}, ${kind}`}
                        />
                        <span style={cs.pickBody}>
                          <strong>{a.displayName}</strong>
                          <span style={cs.meta}>
                            {a.stageKey} · <code style={cs.subcode}>{a.catalogTier}</code> ·{' '}
                            {kind}
                          </span>
                        </span>
                      </label>
                    </li>
                  )
                }
                return (
                  <li key={a.id} style={cs.liFlat}>
                    <strong>{a.displayName}</strong>
                    <span style={cs.meta}>
                      {a.stageKey} · {a.catalogTier}
                    </span>
                  </li>
                )
              })}
            </ul>
          )}
        </section>

        <section style={{ ...cs.panel, gridColumn: '1 / -1' }}>
          <h3 style={cs.h3}>Canal buffers</h3>
          <ul style={cs.ul}>
            {canal.segments.map((s) => (
              <li key={s.id} style={cs.liFlat}>
                <strong>{s.label}</strong>
                <span style={cs.meta}>
                  after stage {s.followsStageOrdinal} · {s.providerProfile}
                </span>
              </li>
            ))}
          </ul>
        </section>
      </div>
    </div>
  )
}

const cs: Record<string, CSSProperties> = {
  wrap: { display: 'flex', flexDirection: 'column', gap: '0.75rem' },
  lead: { margin: 0, fontSize: '0.88rem', color: 'var(--muted)', lineHeight: 1.5 },
  code: { fontFamily: 'var(--mono)', fontSize: '0.82em' },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
    gap: '0.85rem',
  },
  panel: {
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    background: 'var(--bg)',
    padding: '0.85rem 1rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.5rem',
  },
  h3: { margin: 0, fontSize: '0.95rem' },
  ol: { margin: 0, paddingLeft: '1.2rem', display: 'flex', flexDirection: 'column', gap: '0.35rem' },
  ul: { margin: 0, paddingLeft: '1.1rem', display: 'flex', flexDirection: 'column', gap: '0.35rem' },
  stageLi: {
    display: 'flex',
    flexWrap: 'wrap',
    alignItems: 'baseline',
    gap: '0.35rem 0.5rem',
  },
  liFlat: { display: 'flex', flexDirection: 'column', gap: '0.15rem' },
  ord: {
    fontFamily: 'var(--mono)',
    fontSize: '0.8rem',
    color: 'var(--muted)',
  },
  subcode: {
    fontFamily: 'var(--mono)',
    fontSize: '0.75rem',
    color: 'var(--muted)',
  },
  meta: { fontSize: '0.8rem', color: 'var(--muted)' },
  muted: { margin: 0, fontSize: '0.88rem', color: 'var(--muted)' },
  pickerHint: {
    margin: 0,
    fontSize: '0.82rem',
    color: 'var(--muted)',
    lineHeight: 1.45,
  },
  pickLabel: {
    display: 'flex',
    gap: '0.55rem',
    alignItems: 'flex-start',
    padding: '0.55rem 0.65rem',
    borderRadius: 'var(--radius)',
    borderWidth: 1,
    borderStyle: 'solid',
    borderColor: 'var(--border)',
    cursor: 'pointer',
    background: 'var(--surface)',
  },
  pickBody: { display: 'flex', flexDirection: 'column', gap: '0.2rem', minWidth: 0 },
}

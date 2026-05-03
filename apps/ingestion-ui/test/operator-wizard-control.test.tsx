import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from '../src/App.tsx'

const stubPipeline = {
  contractVersion: '0.1.0',
  stages: [
    { ordinal: 1, key: 'source', title: 'Source' },
    { ordinal: 2, key: 'source_connector', title: 'Source Connector' },
  ],
  adapterInstances: [
    {
      id: 'adapter.t2',
      stageKey: 'sink_connector',
      displayName: 'Managed sink (test stub)',
      catalogTier: 'tier-2',
    },
  ],
}

const stubCanal = {
  segments: [
    {
      id: 'canal.segment.test',
      kind: 'buffer' as const,
      label: 'Test buffer',
      followsStageOrdinal: 2,
      providerProfile: 'p1-local' as const,
    },
  ],
}

function stubIngestionFetchWithControl() {
  const impl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
    const path = new URL(raw, 'http://localhost').pathname
    const method = init?.method ?? 'GET'
    if (path === '/v1/control/pipeline' && method === 'GET') {
      return Response.json(stubPipeline)
    }
    if (path === '/v1/control/canal/segments' && method === 'GET') {
      return Response.json(stubCanal)
    }
    if (path === '/v1/adapter-instances' && method === 'POST') {
      const body = JSON.parse(init?.body as string) as { catalogAdapterId: string }
      return Response.json(
        {
          id: '11111111-1111-1111-1111-111111111111',
          catalogAdapterId: body.catalogAdapterId,
          catalogTier: 'tier-2',
          stageKey: 'sink_connector',
          displayName: 'Managed sink (test stub)',
          operatorLabel: null,
          createdAt: '2026-05-03T12:00:00.000Z',
          updatedAt: '2026-05-03T12:00:00.000Z',
        },
        { status: 201 },
      )
    }
    return new Response('not found', { status: 404 })
  })
  vi.stubGlobal('fetch', impl)
  return impl
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('Operator wizard control API', () => {
  beforeEach(() => {
    sessionStorage.clear()
    window.history.replaceState({}, '', '/?operatorWizard=1')
  })

  it('tier 2 configure loads control read models and review acknowledges snapshot', async () => {
    stubIngestionFetchWithControl()
    const user = userEvent.setup()
    render(<App />)

    expect(screen.getByRole('heading', { name: /guided ingestion/i })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /managed real connector/i }))

    await waitFor(() => {
      expect(screen.getByText(/contract/i)).toBeInTheDocument()
      expect(screen.getAllByText(/Managed sink \(test stub\)/i).length).toBeGreaterThanOrEqual(1)
    })
    expect(screen.getByRole('heading', { name: /connector picker \(b3\)/i })).toBeInTheDocument()
    expect(screen.getByRole('radiogroup')).toBeInTheDocument()
    expect(document.querySelector('input[data-catalog-kind="tier2_real"]')).toBeChecked()

    await user.click(screen.getByRole('button', { name: /continue to review/i }))

    expect(screen.getByRole('heading', { name: /^review$/i })).toBeInTheDocument()
    expect(screen.getByText(/Pilot SLO does not apply/i)).toBeInTheDocument()

    await user.click(
      screen.getByRole('checkbox', {
        name: /i understand this path does not carry pilot program labeling/i,
      }),
    )

    await user.click(screen.getByRole('button', { name: /acknowledge control snapshot/i }))

    await waitFor(() => {
      const pre = screen.getByText(/"wizardPathAcknowledged"/i)
      expect(pre).toBeInTheDocument()
    })
    expect(screen.getByText(/"adapterInstance"/i)).toBeInTheDocument()
    expect(screen.getByText(/11111111-1111-1111-1111-111111111111/i)).toBeInTheDocument()
    expect(screen.getAllByText(/Managed sink \(test stub\)/i).length).toBeGreaterThanOrEqual(1)
  })
})

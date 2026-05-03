import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import App from '../src/App.tsx'

function stubIngestionFetch() {
  const impl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
    const path = new URL(raw, 'http://localhost').pathname
    if (path === '/health' && (!init || init.method === undefined || init.method === 'GET')) {
      return Response.json({
        status: 'ok',
        service: 'ingestion-edge-go',
        version: '0.1.0',
      })
    }
    if (path === '/v1/events' && init?.method === 'POST') {
      return Response.json({ accepted: 1, duplicateIds: [] })
    }
    if (path === '/v1/stream' && (!init || init.method === undefined || init.method === 'GET')) {
      return new Response(JSON.stringify({ error: 'not_implemented', message: 'stream stub' }), {
        status: 501,
        headers: { 'Content-Type': 'application/json' },
      })
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

describe('App shell (classic)', () => {
  it('overview: refresh loads health and updates the live region', async () => {
    stubIngestionFetch()
    const user = userEvent.setup()
    render(<App />)

    expect(screen.getByRole('heading', { name: /service health/i })).toBeInTheDocument()
    expect(
      screen.getByText(/press refresh to load health from the data plane edge/i),
    ).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /^refresh$/i }))

    await waitFor(() => {
      expect(
        screen.getByText(/health check succeeded for ingestion-edge-go/i),
      ).toBeInTheDocument()
    })
    expect(screen.getByText(/"service":\s*"ingestion-edge-go"/)).toBeInTheDocument()
  })

  it('ingest: POST batch renders accepted payload', async () => {
    stubIngestionFetch()
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /send batch/i }))
    expect(screen.getByRole('heading', { name: /send ingestion batch/i })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /post \/v1\/events/i }))

    await waitFor(() => {
      expect(screen.getByText(/"accepted":\s*1/)).toBeInTheDocument()
    })
  })

  it('stream: probe shows stub status in the panel', async () => {
    stubIngestionFetch()
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /live stream/i }))
    expect(screen.getByRole('heading', { name: /live stream/i })).toBeInTheDocument()
    const streamHonesty = screen.getByRole('region', {
      name: /realtime streaming limitations/i,
    })
    expect(streamHonesty).toBeInTheDocument()
    expect(streamHonesty).toHaveTextContent(/GET \/v1\/stream/)
    expect(streamHonesty).toHaveTextContent(/501/)
    expect(streamHonesty).toHaveTextContent(/not an availability or latency commitment/i)

    await user.click(
      screen.getByRole('button', { name: /^probe get \/v1\/stream$/i }),
    )

    await waitFor(() => {
      expect(screen.getByText(/"status":\s*501/)).toBeInTheDocument()
    })
  })
})

import type {
  ErrorResponse,
  HealthResponse,
  IngestBatchRequest,
  IngestBatchResponse,
} from './types'

async function readJson<T>(res: Response): Promise<T> {
  const text = await res.text()
  if (!text) {
    return undefined as T
  }
  return JSON.parse(text) as T
}

export async function getHealth(): Promise<HealthResponse> {
  const res = await fetch('/health')
  const body = await readJson<HealthResponse | ErrorResponse>(res)
  if (!res.ok) {
    const err = body as ErrorResponse
    throw new Error(err?.message ?? `GET /health failed (${res.status})`)
  }
  return body as HealthResponse
}

export async function postIngestBatch(
  body: IngestBatchRequest,
): Promise<IngestBatchResponse> {
  const res = await fetch('/v1/events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const parsed = await readJson<IngestBatchResponse | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(err?.message ?? `POST /v1/events failed (${res.status})`)
  }
  return parsed as IngestBatchResponse
}

export async function getStreamProbe(): Promise<{
  status: number
  body: unknown
}> {
  const res = await fetch('/v1/stream')
  let body: unknown
  try {
    body = await readJson(res)
  } catch {
    body = null
  }
  return { status: res.status, body }
}

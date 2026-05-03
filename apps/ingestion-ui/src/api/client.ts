import type {
  AdapterInstanceCreateRequest,
  AdapterInstanceListRead,
  AdapterInstancePatchRequest,
  AdapterInstanceRead,
  CanalSegmentsRead,
  ErrorResponse,
  HealthResponse,
  IngestBatchRequest,
  IngestBatchResponse,
  PipelineSummaryRead,
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

export async function getControlPipeline(): Promise<PipelineSummaryRead> {
  const res = await fetch('/v1/control/pipeline')
  const parsed = await readJson<PipelineSummaryRead | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(err?.message ?? `GET /v1/control/pipeline failed (${res.status})`)
  }
  return parsed as PipelineSummaryRead
}

export async function getControlCanalSegments(): Promise<CanalSegmentsRead> {
  const res = await fetch('/v1/control/canal/segments')
  const parsed = await readJson<CanalSegmentsRead | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(
      err?.message ?? `GET /v1/control/canal/segments failed (${res.status})`,
    )
  }
  return parsed as CanalSegmentsRead
}

export async function listAdapterInstances(): Promise<AdapterInstanceListRead> {
  const res = await fetch('/v1/adapter-instances')
  const parsed = await readJson<AdapterInstanceListRead | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(err?.message ?? `GET /v1/adapter-instances failed (${res.status})`)
  }
  return parsed as AdapterInstanceListRead
}

export async function postAdapterInstance(
  body: AdapterInstanceCreateRequest,
): Promise<AdapterInstanceRead> {
  const res = await fetch('/v1/adapter-instances', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const parsed = await readJson<AdapterInstanceRead | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(err?.message ?? `POST /v1/adapter-instances failed (${res.status})`)
  }
  return parsed as AdapterInstanceRead
}

export async function patchAdapterInstance(
  id: string,
  body: AdapterInstancePatchRequest,
): Promise<AdapterInstanceRead> {
  const res = await fetch(`/v1/adapter-instances/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const parsed = await readJson<AdapterInstanceRead | ErrorResponse>(res)
  if (!res.ok) {
    const err = parsed as ErrorResponse
    throw new Error(err?.message ?? `PATCH /v1/adapter-instances/${id} failed (${res.status})`)
  }
  return parsed as AdapterInstanceRead
}

export async function deleteAdapterInstance(id: string): Promise<void> {
  const res = await fetch(`/v1/adapter-instances/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  })
  if (res.status === 204) return
  const parsed = await readJson<ErrorResponse>(res)
  const err = parsed
  throw new Error(err?.message ?? `DELETE /v1/adapter-instances/${id} failed (${res.status})`)
}

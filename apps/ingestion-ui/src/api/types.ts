/** Mirrors contracts/ingestion-v1.openapi.yaml (subset). */

export type HealthResponse = {
  status: 'ok'
  service: string
  version: string
}

export type IngestEvent = {
  id: string
  type: string
  occurredAt: string
  payload?: unknown
}

export type IngestBatchRequest = {
  source: string
  events: IngestEvent[]
}

export type IngestBatchResponse = {
  accepted: number
  duplicateIds: string[]
}

export type ErrorResponse = {
  error: string
  message: string
}

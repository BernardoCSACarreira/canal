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

/** Control read models — see `paths./v1/control/*` in contracts/ingestion-v1.openapi.yaml */

export type CatalogTier = 'tier-1' | 'tier-2' | 'tier-3'

export type PipelineStageRead = {
  ordinal: number
  key: string
  title: string
}

export type AdapterInstancePlaceholder = {
  id: string
  stageKey: string
  displayName: string
  catalogTier: CatalogTier
}

export type PipelineSummaryRead = {
  contractVersion: string
  stages: PipelineStageRead[]
  adapterInstances: AdapterInstancePlaceholder[]
}

export type CanalSegmentRead = {
  id: string
  kind: 'buffer'
  label: string
  followsStageOrdinal: number
  providerProfile: 'p1-local'
}

export type CanalSegmentsRead = {
  segments: CanalSegmentRead[]
}

/**
 * Phase 1 control-plane read models for operator UI / wizard.
 * Data is deterministic scaffolding (no persistence); shapes align with OpenAPI.
 *
 * Pipeline ordering matches RFC §1.5 (CAN-30 data-architecture rev 3).
 */

export type CatalogTier = "tier-1" | "tier-2" | "tier-3";

export type PipelineStageRead = {
  ordinal: number;
  key: string;
  title: string;
};

export type AdapterInstancePlaceholder = {
  /** Stable id for UI keys until real connector registry exists */
  id: string;
  /** Stage `key` this placeholder is bound to */
  stageKey: string;
  displayName: string;
  /**
   * Internal charter / implementation taxonomy (see connector-tier-ux-spec §2.2).
   * Maps to synthetic vs managed vs BYO wizard lanes; not the commercial `ConnectorTier` SKU.
   */
  catalogTier: CatalogTier;
};

export type PipelineSummaryRead = {
  /** Mirrors `info.version` in contracts/ingestion-v1.openapi.yaml */
  contractVersion: string;
  stages: PipelineStageRead[];
  adapterInstances: AdapterInstancePlaceholder[];
};

export type CanalSegmentRead = {
  id: string;
  kind: "buffer";
  label: string;
  /** Stage ordinal after which this buffer sits in the RFC topology (1-based) */
  followsStageOrdinal: number;
  /** Phase 1 buffer stance per buffer-checkpoint RFC */
  providerProfile: "p1-local";
};

export type CanalSegmentsRead = {
  segments: CanalSegmentRead[];
};

const PIPELINE_STAGES: PipelineStageRead[] = [
  { ordinal: 1, key: "source", title: "Source" },
  { ordinal: 2, key: "source_connector", title: "Source Connector" },
  { ordinal: 3, key: "source_event_buffer", title: "Source Event Buffer" },
  { ordinal: 4, key: "source_canonical_event_serializer", title: "Source Canonical Event Serializer" },
  { ordinal: 5, key: "event_buffer", title: "Event Buffer" },
  { ordinal: 6, key: "sink_event_serializer", title: "Sink Event Serializer" },
  { ordinal: 7, key: "sink_event_buffer", title: "Sink Event Buffer" },
  { ordinal: 8, key: "sink_connector", title: "Sink Connector" },
];

const ADAPTER_PLACEHOLDERS: AdapterInstancePlaceholder[] = [
  {
    id: "adapter.placeholder.source_connector",
    stageKey: "source_connector",
    displayName: "Source connector (placeholder)",
    catalogTier: "tier-1",
  },
  {
    id: "adapter.placeholder.sink_connector",
    stageKey: "sink_connector",
    displayName: "Sink connector (placeholder)",
    catalogTier: "tier-2",
  },
  {
    id: "adapter.placeholder.community_sink",
    stageKey: "sink_connector",
    displayName: "Community sink adapter (placeholder)",
    catalogTier: "tier-3",
  },
];

const CANAL_SEGMENTS: CanalSegmentRead[] = [
  {
    id: "canal.segment.source_event_buffer",
    kind: "buffer",
    label: "Source Event Buffer",
    followsStageOrdinal: 2,
    providerProfile: "p1-local",
  },
  {
    id: "canal.segment.event_buffer",
    kind: "buffer",
    label: "Event Buffer",
    followsStageOrdinal: 4,
    providerProfile: "p1-local",
  },
  {
    id: "canal.segment.sink_event_buffer",
    kind: "buffer",
    label: "Sink Event Buffer",
    followsStageOrdinal: 6,
    providerProfile: "p1-local",
  },
];

export function getPipelineSummaryRead(): PipelineSummaryRead {
  return {
    contractVersion: "0.1.0",
    stages: PIPELINE_STAGES,
    adapterInstances: ADAPTER_PLACEHOLDERS,
  };
}

export function getCanalSegmentsRead(): CanalSegmentsRead {
  return { segments: CANAL_SEGMENTS };
}

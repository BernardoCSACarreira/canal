export { CONNECTOR_TIER_SYNTHETIC } from "./connector-tier.js";
export type { CanalBuffer } from "./buffer-segment.js";
export { InMemoryCanalBuffer } from "./buffer-segment.js";
export type { BufferedIngestEvent, BufferedIngestRecord, EventBufferMetrics } from "./event-buffer.js";
export { P1LocalEventBuffer } from "./event-buffer.js";
export { Tier1SyntheticGenerator } from "./fake-generator.js";
export { Tier1FakeSink } from "./fake-sink.js";
export { runTier1SyntheticPipelineStub } from "./synthetic-pipeline.js";
export type { ConnectorTierSynthetic, SyntheticPipelineEvent } from "./types.js";

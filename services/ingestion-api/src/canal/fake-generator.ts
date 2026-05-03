import type { CONNECTOR_TIER_SYNTHETIC } from "./connector-tier.js";
import type { SyntheticPipelineEvent } from "./types.js";

export class Tier1SyntheticGenerator {
  constructor(private readonly opts: { connectorTier: typeof CONNECTOR_TIER_SYNTHETIC }) {}

  generateBatch(count: number): SyntheticPipelineEvent[] {
    const { connectorTier } = this.opts;
    const base = new Date().toISOString();
    return Array.from({ length: count }, (_, i) => ({
      id: `synthetic-${base}-${i}`,
      type: "synthetic.ping",
      occurredAt: base,
      connectorTier,
      payload: { seq: i },
    }));
  }
}

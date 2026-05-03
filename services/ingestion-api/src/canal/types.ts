import type { CONNECTOR_TIER_SYNTHETIC } from "./connector-tier.js";

export type ConnectorTierSynthetic = typeof CONNECTOR_TIER_SYNTHETIC;

export type SyntheticPipelineEvent = {
  id: string;
  type: string;
  occurredAt: string;
  connectorTier: ConnectorTierSynthetic;
  payload?: Record<string, unknown>;
};

import type { SyntheticPipelineEvent } from "./types.js";

export class Tier1FakeSink {
  readonly received: SyntheticPipelineEvent[] = [];

  async write(batch: readonly SyntheticPipelineEvent[]): Promise<void> {
    this.received.push(...batch);
  }
}

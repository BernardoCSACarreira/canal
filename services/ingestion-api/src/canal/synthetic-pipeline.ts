import { CONNECTOR_TIER_SYNTHETIC } from "./connector-tier.js";
import { InMemoryCanalBuffer } from "./buffer-segment.js";
import { Tier1SyntheticGenerator } from "./fake-generator.js";
import { Tier1FakeSink } from "./fake-sink.js";
import type { SyntheticPipelineEvent } from "./types.js";

export async function runTier1SyntheticPipelineStub(
  batchSize = 5,
): Promise<{ sink: Tier1FakeSink; generated: number; viaBuffer: number }> {
  const gen = new Tier1SyntheticGenerator({ connectorTier: CONNECTOR_TIER_SYNTHETIC });
  const buffer = new InMemoryCanalBuffer<SyntheticPipelineEvent>();
  const sink = new Tier1FakeSink();

  const generated = gen.generateBatch(batchSize);
  await buffer.enqueue(generated);

  const viaBuffer: SyntheticPipelineEvent[] = [];
  for (;;) {
    const chunk = await buffer.dequeueBatch(256);
    if (chunk.length === 0) break;
    viaBuffer.push(...chunk);
  }

  await sink.write(viaBuffer);
  return { sink, generated: generated.length, viaBuffer: viaBuffer.length };
}

import test from "node:test";
import assert from "node:assert/strict";
import {
  CONNECTOR_TIER_SYNTHETIC,
  InMemoryCanalBuffer,
  Tier1FakeSink,
  Tier1SyntheticGenerator,
  runTier1SyntheticPipelineStub,
  type SyntheticPipelineEvent,
} from "../src/canal/index.js";

test("tier-1 synthetic: generator → buffer → fake sink (stub)", async () => {
  const gen = new Tier1SyntheticGenerator({ connectorTier: CONNECTOR_TIER_SYNTHETIC });
  const buffer = new InMemoryCanalBuffer<SyntheticPipelineEvent>();
  const sink = new Tier1FakeSink();

  const produced = gen.generateBatch(4);
  assert.equal(produced.length, 4);
  for (const ev of produced) {
    assert.equal(ev.connectorTier, CONNECTOR_TIER_SYNTHETIC);
  }

  await buffer.enqueue(produced);
  const drained: SyntheticPipelineEvent[] = [];
  for (;;) {
    const chunk = await buffer.dequeueBatch(10);
    if (chunk.length === 0) break;
    drained.push(...chunk);
  }
  assert.deepEqual(drained, produced);

  await sink.write(drained);
  assert.deepEqual(sink.received, produced);
});

test("tier-1 synthetic: orchestrated stub run", async () => {
  const { sink, generated, viaBuffer } = await runTier1SyntheticPipelineStub(6);
  assert.equal(generated, 6);
  assert.equal(viaBuffer, 6);
  assert.equal(sink.received.length, 6);
  for (const ev of sink.received) {
    assert.equal(ev.connectorTier, "tier-1");
  }
});

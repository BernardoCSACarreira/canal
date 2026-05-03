import test from "node:test";
import assert from "node:assert/strict";
import type { FastifyInstance } from "fastify";
import { buildApp } from "../src/app.js";

async function withApp(fn: (app: FastifyInstance) => Promise<void>): Promise<void> {
  const app = await buildApp();
  try {
    await fn(app);
  } finally {
    await app.close();
  }
}

function validEvent(id: string) {
  return {
    id,
    type: "test.event",
    occurredAt: "2026-05-03T12:00:00.000Z",
    payload: { n: 1 },
  };
}

test("GET /health returns Phase-1 contract shape", async () => {
  await withApp(async (app) => {
    const res = await app.inject({ method: "GET", url: "/health" });
    assert.equal(res.statusCode, 200);
    const body = res.json() as Record<string, unknown>;
    assert.equal(body.status, "ok");
    assert.equal(body.service, "ingestion-api");
    assert.equal(typeof body.version, "string");
    assert.match(body.version as string, /^\d+\.\d+\.\d+/);
  });
});

test("GET /v1/stream returns 501 with ErrorResponse shape", async () => {
  await withApp(async (app) => {
    const res = await app.inject({ method: "GET", url: "/v1/stream" });
    assert.equal(res.statusCode, 501);
    const body = res.json() as Record<string, unknown>;
    assert.equal(typeof body.error, "string");
    assert.equal(typeof body.message, "string");
  });
});

test("POST /v1/events accepts batch and returns 202 + IngestBatchResponse", async () => {
  const id = `evt-${crypto.randomUUID()}`;
  await withApp(async (app) => {
    const res = await app.inject({
      method: "POST",
      url: "/v1/events",
      headers: { "content-type": "application/json" },
      payload: {
        source: "integration-test",
        events: [validEvent(id)],
      },
    });
    assert.equal(res.statusCode, 202);
    const body = res.json() as { accepted: number; duplicateIds: string[] };
    assert.equal(body.accepted, 1);
    assert.deepEqual(body.duplicateIds, []);
  });
});

test("POST /v1/events marks duplicate ids on replay (idempotency)", async () => {
  const id = `dup-${crypto.randomUUID()}`;
  await withApp(async (app) => {
    const payload = {
      source: "integration-test",
      events: [validEvent(id)],
    };
    const first = await app.inject({
      method: "POST",
      url: "/v1/events",
      headers: { "content-type": "application/json" },
      payload,
    });
    assert.equal(first.statusCode, 202);
    assert.deepEqual(first.json(), { accepted: 1, duplicateIds: [] });

    const second = await app.inject({
      method: "POST",
      url: "/v1/events",
      headers: { "content-type": "application/json" },
      payload,
    });
    assert.equal(second.statusCode, 202);
    assert.deepEqual(second.json(), { accepted: 0, duplicateIds: [id] });
  });
});

test("GET /v1/control/pipeline returns PipelineSummaryRead shape", async () => {
  await withApp(async (app) => {
    const res = await app.inject({ method: "GET", url: "/v1/control/pipeline" });
    assert.equal(res.statusCode, 200);
    const body = res.json() as {
      contractVersion: string;
      stages: { ordinal: number; key: string; title: string }[];
      adapterInstances: { id: string; stageKey: string; displayName: string; catalogTier: string }[];
    };
    assert.equal(body.contractVersion, "0.1.0");
    assert.equal(body.stages.length, 8);
    assert.equal(body.stages[0].key, "source");
    assert.equal(body.stages[7].key, "sink_connector");
    assert.ok(body.adapterInstances.length >= 1);
    const tiers = new Set(body.adapterInstances.map((a) => a.catalogTier));
    assert.ok(tiers.has("tier-1"));
    assert.ok(tiers.has("tier-2"));
    assert.ok(tiers.has("tier-3"));
  });
});

test("GET /v1/control/canal/segments returns CanalSegmentsRead shape", async () => {
  await withApp(async (app) => {
    const res = await app.inject({ method: "GET", url: "/v1/control/canal/segments" });
    assert.equal(res.statusCode, 200);
    const body = res.json() as {
      segments: { id: string; kind: string; followsStageOrdinal: number; providerProfile: string }[];
    };
    assert.equal(body.segments.length, 3);
    for (const s of body.segments) {
      assert.equal(s.kind, "buffer");
      assert.equal(s.providerProfile, "p1-local");
    }
  });
});

test("POST /v1/events returns 400 for invalid body", async () => {
  await withApp(async (app) => {
    const cases: { name: string; payload: unknown }[] = [
      { name: "non-object body (JSON array)", payload: [] },
      { name: "missing source", payload: { events: [validEvent("x")] } },
      { name: "empty events", payload: { source: "s", events: [] } },
      { name: "bad date-time", payload: { source: "s", events: [{ id: "i", type: "t", occurredAt: "not-a-date" }] } },
    ];
    for (const { name, payload } of cases) {
      const res = await app.inject({
        method: "POST",
        url: "/v1/events",
        headers: { "content-type": "application/json" },
        payload,
      });
      assert.equal(res.statusCode, 400, name);
      const body = res.json() as { error: string; message: string };
      assert.equal(body.error, "validation_error");
      assert.ok(body.message.length > 0, name);
    }
  });
});

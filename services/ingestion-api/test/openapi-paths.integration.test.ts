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

test("adapter instance CRUD: tier comes from catalog; unknown id rejected", async () => {
  await withApp(async (app) => {
    const bad = await app.inject({
      method: "POST",
      url: "/v1/adapter-instances",
      headers: { "content-type": "application/json" },
      payload: { catalogAdapterId: "adapter.unknown" },
    });
    assert.equal(bad.statusCode, 400);
    const badBody = bad.json() as { error: string; message: string };
    assert.equal(badBody.error, "validation_error");
    assert.match(badBody.message, /catalog/i);

    const create = await app.inject({
      method: "POST",
      url: "/v1/adapter-instances",
      headers: { "content-type": "application/json" },
      payload: {
        catalogAdapterId: "adapter.placeholder.sink_connector",
        operatorLabel: " QA binding ",
      },
    });
    assert.equal(create.statusCode, 201);
    const rec = create.json() as {
      id: string;
      catalogAdapterId: string;
      catalogTier: string;
      stageKey: string;
      displayName: string;
      operatorLabel: string | null;
    };
    assert.equal(rec.catalogAdapterId, "adapter.placeholder.sink_connector");
    assert.equal(rec.catalogTier, "tier-2");
    assert.equal(rec.operatorLabel, "QA binding");

    const list = await app.inject({ method: "GET", url: "/v1/adapter-instances" });
    assert.equal(list.statusCode, 200);
    const listBody = list.json() as { items: { id: string; catalogAdapterId: string }[] };
    assert.equal(listBody.items.length, 1);
    assert.equal(listBody.items[0].id, rec.id);

    const patch = await app.inject({
      method: "PATCH",
      url: `/v1/adapter-instances/${rec.id}`,
      headers: { "content-type": "application/json" },
      payload: { catalogAdapterId: "adapter.placeholder.community_sink", operatorLabel: null },
    });
    assert.equal(patch.statusCode, 200);
    const patched = patch.json() as typeof rec;
    assert.equal(patched.catalogAdapterId, "adapter.placeholder.community_sink");
    assert.equal(patched.catalogTier, "tier-3");
    assert.equal(patched.operatorLabel, null);

    const del = await app.inject({ method: "DELETE", url: `/v1/adapter-instances/${rec.id}` });
    assert.equal(del.statusCode, 204);

    const gone = await app.inject({ method: "GET", url: `/v1/adapter-instances/${rec.id}` });
    assert.equal(gone.statusCode, 404);
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

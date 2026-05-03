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

import type { FastifyInstance } from "fastify";

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [k: string]: JsonValue };

type IngestEvent = {
  id: string;
  type: string;
  occurredAt: string;
  payload?: JsonValue;
};

type IngestBatchRequest = {
  source: string;
  events: IngestEvent[];
};

const seenIds = new Set<string>();
const MAX_SEEN = 50_000;

function pruneSeenIfNeeded(): void {
  if (seenIds.size <= MAX_SEEN) return;
  const overflow = seenIds.size - MAX_SEEN;
  const toDrop = [...seenIds].slice(0, overflow);
  for (const id of toDrop) seenIds.delete(id);
}

function validateBatch(body: unknown): { ok: true; value: IngestBatchRequest } | { ok: false; message: string } {
  if (typeof body !== "object" || body === null) return { ok: false, message: "Body must be a JSON object" };
  const b = body as Record<string, unknown>;
  if (typeof b.source !== "string" || b.source.length < 1 || b.source.length > 256) {
    return { ok: false, message: "`source` must be a non-empty string (max 256 chars)" };
  }
  if (!Array.isArray(b.events) || b.events.length < 1 || b.events.length > 1000) {
    return { ok: false, message: "`events` must be an array with 1–1000 items" };
  }
  for (let i = 0; i < b.events.length; i++) {
    const ev = b.events[i];
    if (typeof ev !== "object" || ev === null) return { ok: false, message: `events[${i}] must be an object` };
    const e = ev as Record<string, unknown>;
    if (typeof e.id !== "string" || e.id.length < 1 || e.id.length > 128) {
      return { ok: false, message: `events[${i}].id invalid` };
    }
    if (typeof e.type !== "string" || e.type.length < 1 || e.type.length > 256) {
      return { ok: false, message: `events[${i}].type invalid` };
    }
    if (typeof e.occurredAt !== "string") {
      return { ok: false, message: `events[${i}].occurredAt must be an ISO-8601 string` };
    }
    const t = Date.parse(e.occurredAt);
    if (Number.isNaN(t)) return { ok: false, message: `events[${i}].occurredAt is not a valid date-time` };
  }
  return { ok: true, value: b as IngestBatchRequest };
}

export async function registerIngestionRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: unknown }>("/v1/events", async (req, reply) => {
    const parsed = validateBatch(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: "validation_error", message: parsed.message });
    }
    const { source, events } = parsed.value;
    const duplicateIds: string[] = [];
    let accepted = 0;
    for (const ev of events) {
      if (seenIds.has(ev.id)) {
        duplicateIds.push(ev.id);
        continue;
      }
      seenIds.add(ev.id);
      accepted += 1;
    }
    pruneSeenIfNeeded();
    req.log.info({ accepted, duplicateCount: duplicateIds.length, source }, "ingest_batch");
    return reply.code(202).send({ accepted, duplicateIds });
  });

  app.get("/v1/stream", async (_req, reply) => {
    return reply.code(501).send({
      error: "not_implemented",
      message: "Streaming fan-out is not implemented in the Phase 1 scaffold; use POST /v1/events.",
    });
  });
}

import type { FastifyInstance } from "fastify";
import type { AdapterInstanceStore } from "../control/adapter-instance-store.js";

type CreateBody = { catalogAdapterId?: unknown; operatorLabel?: unknown };
type PatchBody = { catalogAdapterId?: unknown; operatorLabel?: unknown };

function parseCreateBody(body: unknown): { ok: true; catalogAdapterId: string; operatorLabel: string | null } | { ok: false; message: string } {
  if (typeof body !== "object" || body === null) {
    return { ok: false, message: "Body must be a JSON object" };
  }
  const b = body as CreateBody;
  if (typeof b.catalogAdapterId !== "string" || b.catalogAdapterId.length < 1) {
    return { ok: false, message: "`catalogAdapterId` must be a non-empty string" };
  }
  if (b.operatorLabel !== undefined && b.operatorLabel !== null && typeof b.operatorLabel !== "string") {
    return { ok: false, message: "`operatorLabel` must be a string or null" };
  }
  return {
    ok: true,
    catalogAdapterId: b.catalogAdapterId,
    operatorLabel: b.operatorLabel === undefined || b.operatorLabel === null ? null : (b.operatorLabel as string),
  };
}

function parsePatchBody(body: unknown):
  | { ok: true; catalogAdapterId?: string; operatorLabel?: string | null }
  | { ok: false; message: string } {
  if (typeof body !== "object" || body === null) {
    return { ok: false, message: "Body must be a JSON object" };
  }
  const b = body as PatchBody;
  if (b.catalogAdapterId !== undefined && (typeof b.catalogAdapterId !== "string" || b.catalogAdapterId.length < 1)) {
    return { ok: false, message: "`catalogAdapterId` must be a non-empty string when provided" };
  }
  if (b.operatorLabel !== undefined && b.operatorLabel !== null && typeof b.operatorLabel !== "string") {
    return { ok: false, message: "`operatorLabel` must be a string or null" };
  }
  const out: { catalogAdapterId?: string; operatorLabel?: string | null } = {};
  if (b.catalogAdapterId !== undefined) out.catalogAdapterId = b.catalogAdapterId as string;
  if (b.operatorLabel !== undefined) out.operatorLabel = b.operatorLabel === null ? null : (b.operatorLabel as string);
  if (Object.keys(out).length === 0) {
    return { ok: false, message: "Provide at least one of `catalogAdapterId`, `operatorLabel`" };
  }
  return { ok: true, ...out };
}

export async function registerAdapterInstanceRoutes(
  app: FastifyInstance,
  store: AdapterInstanceStore,
): Promise<void> {
  app.get("/v1/adapter-instances", async (_req, reply) => {
    return reply.send({ items: store.list() });
  });

  app.get<{ Params: { id: string } }>("/v1/adapter-instances/:id", async (req, reply) => {
    const rec = store.get(req.params.id);
    if (!rec) {
      return reply.code(404).send({ error: "not_found", message: "Adapter instance not found" });
    }
    return reply.send(rec);
  });

  app.post<{ Body: unknown }>("/v1/adapter-instances", async (req, reply) => {
    const parsed = parseCreateBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: "validation_error", message: parsed.message });
    }
    const created = store.create({
      catalogAdapterId: parsed.catalogAdapterId,
      operatorLabel: parsed.operatorLabel,
    });
    if (!created.ok) {
      return reply.code(400).send({ error: "validation_error", message: created.message });
    }
    return reply.code(201).send(created.record);
  });

  app.patch<{ Params: { id: string }; Body: unknown }>("/v1/adapter-instances/:id", async (req, reply) => {
    const parsed = parsePatchBody(req.body);
    if (!parsed.ok) {
      return reply.code(400).send({ error: "validation_error", message: parsed.message });
    }
    const updated = store.update(req.params.id, parsed);
    if ("notFound" in updated && updated.notFound) {
      return reply.code(404).send({ error: "not_found", message: "Adapter instance not found" });
    }
    if (!updated.ok) {
      return reply.code(400).send({ error: "validation_error", message: updated.message });
    }
    return reply.send(updated.record);
  });

  app.delete<{ Params: { id: string } }>("/v1/adapter-instances/:id", async (req, reply) => {
    const ok = store.delete(req.params.id);
    if (!ok) {
      return reply.code(404).send({ error: "not_found", message: "Adapter instance not found" });
    }
    return reply.code(204).send();
  });
}

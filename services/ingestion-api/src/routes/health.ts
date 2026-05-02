import type { FastifyInstance } from "fastify";

const VERSION = "0.1.0";

export async function registerHealthRoutes(app: FastifyInstance): Promise<void> {
  app.get("/health", async () => ({
    status: "ok" as const,
    service: "ingestion-api",
    version: VERSION,
  }));
}

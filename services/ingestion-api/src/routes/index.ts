import type { FastifyInstance } from "fastify";
import { registerControlRoutes } from "./control.js";
import { registerHealthRoutes } from "./health.js";
import { registerIngestionRoutes } from "./ingestion.js";

export async function registerRoutes(app: FastifyInstance): Promise<void> {
  await registerHealthRoutes(app);
  await registerIngestionRoutes(app);
  await registerControlRoutes(app);
}

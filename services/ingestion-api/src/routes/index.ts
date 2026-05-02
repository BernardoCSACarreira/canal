import type { FastifyInstance } from "fastify";
import { registerHealthRoutes } from "./health.js";
import { registerIngestionRoutes } from "./ingestion.js";

export async function registerRoutes(app: FastifyInstance): Promise<void> {
  await registerHealthRoutes(app);
  await registerIngestionRoutes(app);
}

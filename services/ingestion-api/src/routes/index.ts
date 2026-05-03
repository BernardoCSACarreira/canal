import type { FastifyInstance } from "fastify";
import type { AdapterInstanceStore } from "../control/adapter-instance-store.js";
import { registerAdapterInstanceRoutes } from "./adapter-instances.js";
import { registerControlRoutes } from "./control.js";
import { registerHealthRoutes } from "./health.js";
import { registerIngestionRoutes } from "./ingestion.js";

export type RegisterRouteDeps = {
  adapterInstanceStore: AdapterInstanceStore;
};

export async function registerRoutes(app: FastifyInstance, deps: RegisterRouteDeps): Promise<void> {
  await registerHealthRoutes(app);
  await registerIngestionRoutes(app);
  await registerControlRoutes(app);
  await registerAdapterInstanceRoutes(app, deps.adapterInstanceStore);
}

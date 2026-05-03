import Fastify, { type FastifyInstance } from "fastify";
import { createAdapterInstanceStore, type AdapterInstanceStore } from "./control/adapter-instance-store.js";
import { registerRoutes } from "./routes/index.js";

export type BuildAppOptions = {
  /** Default false so tests stay quiet; server uses `true`. */
  logger?: boolean;
  /** In-memory Phase 1 store; fresh per `buildApp()` unless injected for tests. */
  adapterInstanceStore?: AdapterInstanceStore;
};

export async function buildApp(opts: BuildAppOptions = {}): Promise<FastifyInstance> {
  const app = Fastify({
    logger: opts.logger ?? false,
  });
  const adapterInstanceStore = opts.adapterInstanceStore ?? createAdapterInstanceStore();
  await registerRoutes(app, { adapterInstanceStore });
  return app;
}

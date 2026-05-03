import Fastify, { type FastifyInstance } from "fastify";
import { P1LocalEventBuffer } from "./canal/event-buffer.js";
import { createAdapterInstanceStore, type AdapterInstanceStore } from "./control/adapter-instance-store.js";
import { registerRoutes } from "./routes/index.js";

export type BuildAppOptions = {
  /** Default false so tests stay quiet; server uses `true`. */
  logger?: boolean;
  /** In-memory Phase 1 store; fresh per `buildApp()` unless injected for tests. */
  adapterInstanceStore?: AdapterInstanceStore;
  /** Primary Event Buffer; fresh per `buildApp()` unless injected for tests. */
  eventBuffer?: P1LocalEventBuffer;
};

export async function buildApp(opts: BuildAppOptions = {}): Promise<FastifyInstance> {
  const app = Fastify({
    logger: opts.logger ?? false,
  });
  const adapterInstanceStore = opts.adapterInstanceStore ?? createAdapterInstanceStore();
  const eventBuffer = opts.eventBuffer ?? new P1LocalEventBuffer();
  await registerRoutes(app, { adapterInstanceStore, eventBuffer });
  return app;
}

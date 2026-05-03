import Fastify, { type FastifyInstance } from "fastify";
import { registerRoutes } from "./routes/index.js";

export type BuildAppOptions = {
  /** Default false so tests stay quiet; server uses `true`. */
  logger?: boolean;
};

export async function buildApp(opts: BuildAppOptions = {}): Promise<FastifyInstance> {
  const app = Fastify({
    logger: opts.logger ?? false,
  });
  await registerRoutes(app);
  return app;
}

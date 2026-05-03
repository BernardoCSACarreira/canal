import type { FastifyInstance } from "fastify";
import { getCanalSegmentsRead, getPipelineSummaryRead } from "../control/read-models.js";

export async function registerControlRoutes(app: FastifyInstance): Promise<void> {
  app.get("/v1/control/pipeline", async (_req, reply) => {
    return reply.send(getPipelineSummaryRead());
  });

  app.get("/v1/control/canal/segments", async (_req, reply) => {
    return reply.send(getCanalSegmentsRead());
  });
}

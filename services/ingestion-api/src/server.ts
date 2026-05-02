import Fastify from "fastify";
import { registerRoutes } from "./routes/index.js";

const PORT = Number(process.env.INGESTION_PORT ?? "8080");
const HOST = process.env.HOST ?? "0.0.0.0";

const app = Fastify({
  logger: true,
});

await registerRoutes(app);

try {
  await app.listen({ port: PORT, host: HOST });
} catch (err) {
  app.log.error(err);
  process.exit(1);
}

import { buildApp } from "./app.js";

const PORT = Number(process.env.INGESTION_PORT ?? "8080");
const HOST = process.env.HOST ?? "0.0.0.0";

const app = await buildApp({ logger: true });

try {
  await app.listen({ port: PORT, host: HOST });
} catch (err) {
  app.log.error(err);
  process.exit(1);
}

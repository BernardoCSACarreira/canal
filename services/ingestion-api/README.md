# Ingestion API (Phase 1 scaffold)

Implements the HTTP surface described in [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml).

```bash
npm install
npm run dev
```

Defaults: `HOST=0.0.0.0`, `INGESTION_PORT=8080` (this service does not read generic `PORT` to avoid collisions with other processes).

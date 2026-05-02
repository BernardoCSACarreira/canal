# @canal/ingestion-ui

Minimal shell for Phase 1 ingestion MVP: health check, batch POST to `/v1/events`, and a documented stub probe for `/v1/stream`. Shapes follow `contracts/ingestion-v1.openapi.yaml`.

## Develop

If your npm config sets `omit=dev`, install with dev tools explicitly:

```bash
npm install --include=dev
```

```bash
# from repo root
cd services/ingestion-api && npm install && npm run dev
# separate terminal
cd apps/ingestion-ui && npm install && npm run dev
```

The Vite dev server proxies `/health` and `/v1` to `http://127.0.0.1:8080` (override with `VITE_API_PROXY_TARGET`).

## Build

```bash
npm run build
```

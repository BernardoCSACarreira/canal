# @canal/ingestion-ui

Minimal shell for Phase 1 ingestion MVP: health check, batch POST to `/v1/events`, and a documented stub probe for `/v1/stream`. Shapes follow `contracts/ingestion-v1.openapi.yaml`. Operator UX: **badge keys + copy + hierarchy** — [`docs/product/connector-tier-ux-spec.md`](../../docs/product/connector-tier-ux-spec.md). Engineering supplement (a11y, tokens) — [`docs/design/CAN-28-operator-ui-phase1.md`](../../docs/design/CAN-28-operator-ui-phase1.md).

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

## QA (operator wizard)

With the dev server running, open **`/?operatorWizard=1`** (and `VITE_OPERATOR_WIZARD=true` in production builds if the sidebar promo is hidden). Confirm path badges use pilot / standard semantic tokens, honesty lines match `docs/product/connector-tier-ux-spec.md` §3.2, and `data-badge-key` / `data-honesty-key` match §2.2. Exercise the classic shell tier fieldset on **Send batch** and **Live stream** callouts for pilot vs standard.

## Build

```bash
npm run build
```

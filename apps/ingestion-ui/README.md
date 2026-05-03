# @canal/ingestion-ui

Minimal shell for Phase 1 ingestion MVP: health check, batch POST to `/v1/events`, and a documented stub probe for `/v1/stream`. Shapes follow `contracts/ingestion-v1.openapi.yaml`. Operator UX: **badge keys + copy + hierarchy** — [`docs/product/connector-tier-ux-spec.md`](../../docs/product/connector-tier-ux-spec.md). Engineering supplement (a11y, tokens) — [`docs/design/CAN-28-operator-ui-phase1.md`](../../docs/design/CAN-28-operator-ui-phase1.md). Platform UI planning (design system, IA, prioritized backlog) — [`docs/design/CAN-68-platform-ui-deep-plan.md`](../../docs/design/CAN-68-platform-ui-deep-plan.md).

## Develop

If your npm config sets `omit=dev`, install with dev tools explicitly:

```bash
npm install --include=dev
```

### Split stack (Go data plane + Python control plane)

| Backend | Port (default) | Routes |
|---------|----------------|--------|
| [`services/ingestion-edge-go`](../../services/ingestion-edge-go) | `8080` | `GET /health`, `POST /v1/events`, `GET /v1/stream` |
| [`services/ingestion-control-plane`](../../services/ingestion-control-plane) | `8091` (matches Dockerfile `EXPOSE`) | `GET /v1/control/*`, `/v1/adapter-instances*` |

```bash
# Terminal A — Go edge
cd services/ingestion-edge-go && go run ./cmd/ingestion-edge

# Terminal B — Python control
cd services/ingestion-control-plane && pip install -e '.[dev]' && \
  uvicorn canal_control_plane.app:app --host 127.0.0.1 --port 8091 --reload

# Terminal C — UI (defaults: data :8080, control :8091)
cd apps/ingestion-ui && npm install && npm run dev
```

Proxy env vars (defaults match split stack; override if your ports differ):

| Variable | Default |
|----------|---------|
| `VITE_DATA_PLANE_PROXY_TARGET` | `http://127.0.0.1:8080` |
| `VITE_CONTROL_PLANE_PROXY_TARGET` | `http://127.0.0.1:8091` |

## QA (operator wizard)

With the dev server running, open **`/?operatorWizard=1`** (and `VITE_OPERATOR_WIZARD=true` in production builds if the sidebar promo is hidden). Confirm path badges use pilot / standard semantic tokens, honesty lines match `docs/product/connector-tier-ux-spec.md` §3.2, and `data-badge-key` / `data-honesty-key` match §2.2. Exercise the classic shell tier fieldset on **Send batch** and **Live stream** callouts for pilot vs standard.

## Build

```bash
npm run build
```

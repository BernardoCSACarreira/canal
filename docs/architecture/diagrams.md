# Architecture diagrams (Canal Phase 1)

**Normative detail:** [`ingestion-v1.md`](./ingestion-v1.md), [`control-api-read-models.md`](./control-api-read-models.md), [`language-platform-policy.md`](./language-platform-policy.md).  
**Contract:** [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml).

These diagrams render in GitHub, many IDEs, and static-site generators that support Mermaid. They summarize the **current repo layout** and the **target** language split (Python control, Go data plane, TS browser only).

---

## 1. System context

Actors and systems at the boundary of Phase 1 HTTP ingestion and operator UX.

```mermaid
flowchart LR
  subgraph clients["Clients"]
    Conn["Connectors / apps"]
    Op["Operators (browser)"]
  end

  subgraph edge["Edge & UI (this repo)"]
    UI["ingestion-ui\n(React / Vite)"]
    GoEdge["ingestion-edge-go\n(Go)\ndata plane"]
    CP["ingestion-control-plane\n(Python / FastAPI)\ncontrol plane"]
  end

  Contract["ingestion-v1 OpenAPI\n(single contract)"]

  Conn -->|"HTTPS\nPOST /v1/events\nGET /health"| GoEdge
  Op -->|"dev: path-aware proxy"| UI
  UI -->|"POST /v1/events\nGET /health\nGET /v1/stream"| GoEdge
  UI -->|"GET /v1/control/*\nadapter CRUD"| CP

  GoEdge -.->|"conformance"| Contract
  CP -.->|"conformance"| Contract
```

**Notes:** Default local dev runs **Go + Python** behind the UI proxy (see `apps/ingestion-ui/README.md`).

---

## 2. Repository map and CI touchpoints

How major paths relate to automation (from `.github/workflows/ci.yml`).

```mermaid
flowchart TB
  subgraph repo["Monorepo"]
    OAS["contracts/\ningestion-v1.openapi.yaml"]
    UI["apps/ingestion-ui"]
    PY["services/ingestion-control-plane"]
    GO["services/ingestion-edge-go"]
  end

  subgraph ci["CI (path-filtered on PR)"]
    V_OAS["OpenAPI validate"]
    V_UI["npm test, lint, build"]
    V_PY["ruff, pytest"]
    V_GO["gofmt, vet, test, docker build"]
  end

  OAS --> V_OAS
  UI --> V_UI
  PY --> V_PY
  GO --> V_GO
  OAS --> V_UI
  OAS --> V_PY
  OAS --> V_GO
```

---

## 3. Ingestion batch flow (Go data plane)

Logical path for `POST /v1/events` through in-process Phase 1 components.

```mermaid
sequenceDiagram
  participant C as Client / connector
  participant R as ingestion-edge-go POST /v1/events
  participant V as Batch validator
  participant D as Dedupe (event ids)
  participant B as P1LocalEventBuffer

  C->>R: POST JSON batch (source + events)
  R->>V: validate shape + occurredAt
  alt invalid
    V-->>R: 400 validation_error
    R-->>C: 400 + message
  else valid
    V->>D: per-event id check
    D->>B: append accepted events
    B-->>R: accepted + duplicate metadata
    R-->>C: 202 Accepted + body
  end
```

---

## 4. Operator read paths and adapter instances

What the operator wizard consumes today (**split stack**): control + adapter routes hit **Python**; batch + edge health hit **Go**.

```mermaid
flowchart TB
  UI["ingestion-ui\nOperatorWizardShell, etc."]
  Conn2["External connectors"]

  subgraph GoEdge["ingestion-edge-go"]
    H["GET /health"]
    E["POST /v1/events"]
    ST["GET /v1/stream\nstub"]
  end

  subgraph CP["ingestion-control-plane"]
    P["GET /v1/control/pipeline"]
    S["GET /v1/control/canal/segments"]
    L["GET/POST/PATCH/DELETE\n/v1/adapter-instances…"]
  end

  UI --> H
  UI --> P
  UI --> S
  UI --> L
  UI -. probe .-> ST
  Conn2 --> E
```

---

## 5. Conceptual pipeline (control read model)

Stages and buffer segments are **deterministic scaffolding** for the operator board (not a live streaming topology in Phase 1).

```mermaid
flowchart LR
  S1[1 Source]
  S2[2 Source connector]
  S3[3 Source event buffer]
  S4[4 Canonical serializer]
  S5[5 Event buffer]
  S6[6 Sink serializer]
  S7[7 Sink event buffer]
  S8[8 Sink connector]

  S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8
```

Buffer segments (`providerProfile: p1-local` in read model) attach after selected ordinals; see OpenAPI **Control** schemas and `control-api-read-models.md` for JSON examples.

---

## 6. Target language split (policy)

Canonical placement of new work.

```mermaid
flowchart TB
  subgraph policy["language-platform-policy"]
    CP2["Control plane → Python"]
    DP["Data plane → Go"]
    FE["Web UI → TypeScript only"]
  end

  CP2 --- PY2["ingestion-control-plane"]
  DP --- GO2["ingestion-edge-go\n+ future workers"]
  FE --- UI2["ingestion-ui"]
```

---

## Changelog

| Date       | Change                                      |
| ---------- | ------------------------------------------- |
| 2026-05-03 | Initial diagrams for CAN-67 (CTO heartbeat) |
| 2026-05-03 | System context + §4 updated for CAN-69 split-stack default (Go + Python) |
| 2026-05-03 | Removed legacy TS ingestion-api from diagrams (CAN-73) |

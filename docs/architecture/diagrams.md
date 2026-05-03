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
    API["ingestion-api\n(TypeScript / Fastify)\nPhase 1 scaffold"]
    UI["ingestion-ui\n(React / Vite)"]
    GoEdge["ingestion-edge-go\n(Go)\nparallel implementation"]
    CP["ingestion-control-plane\n(Python / FastAPI)\nread models + adapter store"]
  end

  Contract["ingestion-v1 OpenAPI\n(single contract)"]

  Conn -->|"HTTPS POST /v1/events\nGET /health"| API
  Op -->|"same-origin fetch\n/proxy to API"| UI
  UI --> API

  API -.->|"conformance"| Contract
  GoEdge -.->|"conformance"| Contract
  CP -.->|"aligned responses"| Contract

  Conn -.->|"future / alt edge"| GoEdge
```

**Notes:** Default dev experience often colocates UI and API (Vite proxy). Python control plane can run on its own port for split-stack dev; until a gateway splits traffic, TS and Python may expose overlapping routes—responses must stay aligned per `control-api-read-models.md`.

---

## 2. Repository map and CI touchpoints

How major paths relate to automation (from `.github/workflows/ci.yml`).

```mermaid
flowchart TB
  subgraph repo["Monorepo"]
    OAS["contracts/\ningestion-v1.openapi.yaml"]
    UI["apps/ingestion-ui"]
    TSAPI["services/ingestion-api"]
    PY["services/ingestion-control-plane"]
    GO["services/ingestion-edge-go"]
  end

  subgraph ci["CI (path-filtered on PR)"]
    V_OAS["OpenAPI validate"]
    V_UI["npm test, lint, build"]
    V_TS["npm test, build"]
    V_PY["ruff, pytest"]
    V_GO["gofmt, vet, test, docker build"]
  end

  OAS --> V_OAS
  UI --> V_UI
  TSAPI --> V_TS
  PY --> V_PY
  GO --> V_GO
  OAS --> V_UI
  OAS --> V_TS
  OAS --> V_PY
  OAS --> V_GO
```

---

## 3. Ingestion batch flow (TypeScript edge)

Logical path for `POST /v1/events` through in-process Phase 1 components.

```mermaid
sequenceDiagram
  participant C as Client / connector
  participant R as Fastify /v1/events
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

What the operator wizard consumes today (TS service registers these routes; Python service mirrors read models and adapter CRUD shape).

```mermaid
flowchart TB
  UI["ingestion-ui\nOperatorWizardShell, etc."]
  Conn2["External connectors"]

  subgraph TSAPI["ingestion-api (Fastify)"]
    H["GET /health"]
    P["GET /v1/control/pipeline"]
    S["GET /v1/control/canal/segments"]
    L["GET/POST/PATCH/DELETE\n/v1/adapter-instances…"]
    E["POST /v1/events"]
    ST["GET /v1/stream\nstub / not P1 dependency"]
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

Canonical placement of new work vs the Phase 1 TS exception.

```mermaid
flowchart TB
  subgraph policy["language-platform-policy"]
    CP2["Control plane → Python"]
    DP["Data plane → Go"]
    FE["Web UI → TypeScript only"]
  end

  subgraph exception["Time-bounded exception"]
    TS["ingestion-api TS scaffold\nexit when Go edge + tests reach parity"]
  end

  CP2 --- PY2["ingestion-control-plane"]
  DP --- GO2["ingestion-edge-go\n+ future workers"]
  FE --- UI2["ingestion-ui"]
  exception --- TS
```

---

## Changelog

| Date       | Change                                      |
| ---------- | ------------------------------------------- |
| 2026-05-03 | Initial diagrams for CAN-67 (CTO heartbeat) |

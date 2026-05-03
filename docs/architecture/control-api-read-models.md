# Control API — read models (Phase 1)

**Normative schemas:** [`contracts/ingestion-v1.openapi.yaml`](../../contracts/ingestion-v1.openapi.yaml) — tag **Control**, operations `getControlPipeline` and `getControlCanalSegments`.  
**Implementation:** `services/ingestion-api/src/control/read-models.ts` and `src/routes/control.ts`.

These endpoints exist so the operator wizard can render the board-approved pipeline and canal buffers without scraping RFC prose. Responses are **deterministic scaffolding** until a persisted connector registry ships.

---

## Example: `GET /v1/control/pipeline`

```json
{
  "contractVersion": "0.1.0",
  "stages": [
    { "ordinal": 1, "key": "source", "title": "Source" },
    { "ordinal": 2, "key": "source_connector", "title": "Source Connector" },
    { "ordinal": 3, "key": "source_event_buffer", "title": "Source Event Buffer" },
    {
      "ordinal": 4,
      "key": "source_canonical_event_serializer",
      "title": "Source Canonical Event Serializer"
    },
    { "ordinal": 5, "key": "event_buffer", "title": "Event Buffer" },
    { "ordinal": 6, "key": "sink_event_serializer", "title": "Sink Event Serializer" },
    { "ordinal": 7, "key": "sink_event_buffer", "title": "Sink Event Buffer" },
    { "ordinal": 8, "key": "sink_connector", "title": "Sink Connector" }
  ],
  "adapterInstances": [
    {
      "id": "adapter.placeholder.source_connector",
      "stageKey": "source_connector",
      "displayName": "Source connector (placeholder)",
      "catalogTier": "tier-1"
    },
    {
      "id": "adapter.placeholder.sink_connector",
      "stageKey": "sink_connector",
      "displayName": "Sink connector (placeholder)",
      "catalogTier": "tier-2"
    },
    {
      "id": "adapter.placeholder.community_sink",
      "stageKey": "sink_connector",
      "displayName": "Community sink adapter (placeholder)",
      "catalogTier": "tier-3"
    }
  ]
}
```

`catalogTier` is the **charter / implementation** namespace (`tier-1` | `tier-2` | `tier-3`). It is **not** the commercial `ConnectorTier` (`pilot` | `standard`); see [`docs/product/connector-tier-ux-spec.md`](../product/connector-tier-ux-spec.md) §2.

---

## Example: `GET /v1/control/canal/segments`

```json
{
  "segments": [
    {
      "id": "canal.segment.source_event_buffer",
      "kind": "buffer",
      "label": "Source Event Buffer",
      "followsStageOrdinal": 2,
      "providerProfile": "p1-local"
    },
    {
      "id": "canal.segment.event_buffer",
      "kind": "buffer",
      "label": "Event Buffer",
      "followsStageOrdinal": 4,
      "providerProfile": "p1-local"
    },
    {
      "id": "canal.segment.sink_event_buffer",
      "kind": "buffer",
      "label": "Sink Event Buffer",
      "followsStageOrdinal": 6,
      "providerProfile": "p1-local"
    }
  ]
}
```

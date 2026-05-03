"""
Operator read models — deterministic Phase 1 scaffolding (OpenAPI-aligned).

Pipeline ordering matches RFC §1.5 (CAN-30 data-architecture rev 3).
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class PipelineStageRead(TypedDict):
    ordinal: int
    key: str
    title: str


class AdapterInstancePlaceholder(TypedDict):
    id: str
    stageKey: str
    displayName: str
    catalogTier: Literal["tier-1", "tier-2", "tier-3"]


class PipelineSummaryRead(TypedDict):
    contractVersion: str
    stages: list[PipelineStageRead]
    adapterInstances: list[AdapterInstancePlaceholder]


class CanalSegmentRead(TypedDict):
    id: str
    kind: Literal["buffer"]
    label: str
    followsStageOrdinal: int
    providerProfile: Literal["p1-local"]


class CanalSegmentsRead(TypedDict):
    segments: list[CanalSegmentRead]


PIPELINE_STAGES: list[PipelineStageRead] = [
    {"ordinal": 1, "key": "source", "title": "Source"},
    {"ordinal": 2, "key": "source_connector", "title": "Source Connector"},
    {"ordinal": 3, "key": "source_event_buffer", "title": "Source Event Buffer"},
    {
        "ordinal": 4,
        "key": "source_canonical_event_serializer",
        "title": "Source Canonical Event Serializer",
    },
    {"ordinal": 5, "key": "event_buffer", "title": "Event Buffer"},
    {"ordinal": 6, "key": "sink_event_serializer", "title": "Sink Event Serializer"},
    {"ordinal": 7, "key": "sink_event_buffer", "title": "Sink Event Buffer"},
    {"ordinal": 8, "key": "sink_connector", "title": "Sink Connector"},
]

ADAPTER_PLACEHOLDERS: list[AdapterInstancePlaceholder] = [
    {
        "id": "adapter.placeholder.source_connector",
        "stageKey": "source_connector",
        "displayName": "Source connector (placeholder)",
        "catalogTier": "tier-1",
    },
    {
        "id": "adapter.placeholder.sink_connector",
        "stageKey": "sink_connector",
        "displayName": "Sink connector (placeholder)",
        "catalogTier": "tier-2",
    },
    {
        "id": "adapter.placeholder.community_sink",
        "stageKey": "sink_connector",
        "displayName": "Community sink adapter (placeholder)",
        "catalogTier": "tier-3",
    },
]

CANAL_SEGMENTS: list[CanalSegmentRead] = [
    {
        "id": "canal.segment.source_event_buffer",
        "kind": "buffer",
        "label": "Source Event Buffer",
        "followsStageOrdinal": 2,
        "providerProfile": "p1-local",
    },
    {
        "id": "canal.segment.event_buffer",
        "kind": "buffer",
        "label": "Event Buffer",
        "followsStageOrdinal": 4,
        "providerProfile": "p1-local",
    },
    {
        "id": "canal.segment.sink_event_buffer",
        "kind": "buffer",
        "label": "Sink Event Buffer",
        "followsStageOrdinal": 6,
        "providerProfile": "p1-local",
    },
]


def get_pipeline_summary_read() -> dict[str, Any]:
    """Return PipelineSummaryRead JSON object."""
    body: PipelineSummaryRead = {
        "contractVersion": "0.1.0",
        "stages": list(PIPELINE_STAGES),
        "adapterInstances": list(ADAPTER_PLACEHOLDERS),
    }
    return body


def get_canal_segments_read() -> dict[str, Any]:
    """Return CanalSegmentsRead JSON object."""
    body: CanalSegmentsRead = {"segments": list(CANAL_SEGMENTS)}
    return body


def catalog_placeholder(catalog_adapter_id: str) -> AdapterInstancePlaceholder | None:
    for ph in ADAPTER_PLACEHOLDERS:
        if ph["id"] == catalog_adapter_id:
            return ph
    return None

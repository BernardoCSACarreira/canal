"""Contract-style tests for control-plane OpenAPI paths."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from canal_control_plane.app import SERVICE_VERSION, create_app
from canal_control_plane.read_models import get_canal_segments_read, get_pipeline_summary_read


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app())


def test_health(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "ingestion-control-plane"
    assert body["version"] == SERVICE_VERSION


def test_pipeline_shape_matches_ts_scaffold(client: TestClient) -> None:
    r = client.get("/v1/control/pipeline")
    assert r.status_code == 200
    body = r.json()
    expected = get_pipeline_summary_read()
    assert body == expected
    assert len(body["stages"]) == 8
    assert body["stages"][0]["key"] == "source"
    assert body["stages"][7]["key"] == "sink_connector"
    tiers = {a["catalogTier"] for a in body["adapterInstances"]}
    assert tiers == {"tier-1", "tier-2", "tier-3"}


def test_canal_segments_shape_matches_ts_scaffold(client: TestClient) -> None:
    r = client.get("/v1/control/canal/segments")
    assert r.status_code == 200
    body = r.json()
    assert body == get_canal_segments_read()
    for seg in body["segments"]:
        assert seg["kind"] == "buffer"
        assert seg["providerProfile"] == "p1-local"


def test_pipeline_fixture_matches_committed_example() -> None:
    """Stable JSON checked into repo (from control-api-read-models.md example)."""
    root = Path(__file__).resolve().parents[1]
    fixture = root / "tests" / "fixtures" / "pipeline_summary.json"
    with fixture.open() as f:
        expected = json.load(f)
    assert get_pipeline_summary_read() == expected


def test_adapter_instance_crud(client: TestClient) -> None:
    bad = client.post("/v1/adapter-instances", json={"catalogAdapterId": "adapter.unknown"})
    assert bad.status_code == 400
    assert bad.json()["error"] == "validation_error"
    assert "catalog" in bad.json()["message"].lower()

    create = client.post(
        "/v1/adapter-instances",
        json={
            "catalogAdapterId": "adapter.placeholder.sink_connector",
            "operatorLabel": " QA binding ",
        },
    )
    assert create.status_code == 201
    rec = create.json()
    assert rec["catalogAdapterId"] == "adapter.placeholder.sink_connector"
    assert rec["catalogTier"] == "tier-2"
    assert rec["operatorLabel"] == "QA binding"

    lst = client.get("/v1/adapter-instances")
    assert lst.status_code == 200
    items = lst.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == rec["id"]

    patch = client.patch(
        f"/v1/adapter-instances/{rec['id']}",
        json={"catalogAdapterId": "adapter.placeholder.community_sink", "operatorLabel": None},
    )
    assert patch.status_code == 200
    patched = patch.json()
    assert patched["catalogAdapterId"] == "adapter.placeholder.community_sink"
    assert patched["catalogTier"] == "tier-3"
    assert patched["operatorLabel"] is None

    dele = client.delete(f"/v1/adapter-instances/{rec['id']}")
    assert dele.status_code == 204

    gone = client.get(f"/v1/adapter-instances/{rec['id']}")
    assert gone.status_code == 404

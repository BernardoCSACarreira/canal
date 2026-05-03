"""Split-stack contract: Python control plane must not claim data-plane routes."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from canal_control_plane.app import create_app


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app())


def test_control_plane_returns_404_for_post_v1_events(client: TestClient) -> None:
    r = client.post(
        "/v1/events",
        json={
            "source": "x",
            "events": [{"id": "e1", "type": "t", "occurredAt": "2026-05-03T12:00:00.000Z"}],
        },
    )
    assert r.status_code == 404


def test_control_plane_returns_404_for_get_v1_stream(client: TestClient) -> None:
    r = client.get("/v1/stream")
    assert r.status_code == 404

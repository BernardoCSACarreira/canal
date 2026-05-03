"""In-memory adapter instances — mirrors ingestion-api adapter-instance-store.ts."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from canal_control_plane.read_models import catalog_placeholder

_UNSET = object()


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class AdapterInstanceRecord:
    id: str
    catalogAdapterId: str
    catalogTier: Literal["tier-1", "tier-2", "tier-3"]
    stageKey: str
    displayName: str
    operatorLabel: str | None
    createdAt: str
    updatedAt: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "catalogAdapterId": self.catalogAdapterId,
            "catalogTier": self.catalogTier,
            "stageKey": self.stageKey,
            "displayName": self.displayName,
            "operatorLabel": self.operatorLabel,
            "createdAt": self.createdAt,
            "updatedAt": self.updatedAt,
        }


class AdapterInstanceStore:
    def __init__(self) -> None:
        self._by_id: dict[str, AdapterInstanceRecord] = {}

    def list(self) -> list[AdapterInstanceRecord]:
        return sorted(self._by_id.values(), key=lambda r: r.createdAt)

    def get(self, instance_id: str) -> AdapterInstanceRecord | None:
        return self._by_id.get(instance_id)

    def create(
        self, catalog_adapter_id: str, operator_label: str | None
    ) -> tuple[Literal[True], AdapterInstanceRecord] | tuple[Literal[False], str]:
        ph = catalog_placeholder(catalog_adapter_id)
        if ph is None:
            return (
                False,
                "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is "
                "taken only from the control read model catalog.",
            )
        now = _utc_iso()
        if operator_label is None:
            label = None
        elif isinstance(operator_label, str):
            label = operator_label.strip() if operator_label.strip() else None
        else:
            label = None
        rec = AdapterInstanceRecord(
            id=str(uuid.uuid4()),
            catalogAdapterId=ph["id"],
            catalogTier=ph["catalogTier"],
            stageKey=ph["stageKey"],
            displayName=ph["displayName"],
            operatorLabel=label,
            createdAt=now,
            updatedAt=now,
        )
        self._by_id[rec.id] = rec
        return (True, rec)

    def update(
        self,
        instance_id: str,
        *,
        catalog_adapter_id: Any = _UNSET,
        operator_label: Any = _UNSET,
    ) -> (
        tuple[Literal[True], AdapterInstanceRecord]
        | tuple[Literal[False], Literal["not_found"]]
        | tuple[Literal[False], str]
    ):
        existing = self._by_id.get(instance_id)
        if existing is None:
            return (False, "not_found")

        if catalog_adapter_id is not _UNSET:
            ph = catalog_placeholder(catalog_adapter_id)
            if ph is None:
                return (
                    False,
                    "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is "
                    "taken only from the control read model catalog.",
                )
            existing.catalogAdapterId = ph["id"]
            existing.catalogTier = ph["catalogTier"]
            existing.stageKey = ph["stageKey"]
            existing.displayName = ph["displayName"]

        if operator_label is not _UNSET:
            if operator_label is None:
                existing.operatorLabel = None
            elif isinstance(operator_label, str):
                existing.operatorLabel = (
                    operator_label.strip() if operator_label.strip() else None
                )
            else:
                return (False, "`operatorLabel` must be a string or null")

        existing.updatedAt = _utc_iso()
        return (True, existing)

    def delete(self, instance_id: str) -> bool:
        return self._by_id.pop(instance_id, None) is not None

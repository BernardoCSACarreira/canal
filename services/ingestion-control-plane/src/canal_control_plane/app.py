"""FastAPI application — control-plane routes only."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from canal_control_plane.adapter_store import AdapterInstanceStore
from canal_control_plane.read_models import get_canal_segments_read, get_pipeline_summary_read

SERVICE_VERSION = "0.1.0"


def _parse_create_body(
    body: Any,
) -> tuple[Literal[True], str, str | None] | tuple[Literal[False], str]:
    if not isinstance(body, dict):
        return (False, "Body must be a JSON object")
    cid = body.get("catalogAdapterId")
    if not isinstance(cid, str) or len(cid) < 1:
        return (False, "`catalogAdapterId` must be a non-empty string")
    if "operatorLabel" not in body:
        return (True, cid, None)
    ol = body["operatorLabel"]
    if ol is not None and not isinstance(ol, str):
        return (False, "`operatorLabel` must be a string or null")
    return (True, cid, ol)


def _parse_patch_body(
    body: Any,
) -> tuple[Literal[True], dict[str, Any]] | tuple[Literal[False], str]:
    if not isinstance(body, dict):
        return (False, "Body must be a JSON object")
    patch: dict[str, Any] = {}
    if "catalogAdapterId" in body:
        cid = body["catalogAdapterId"]
        if not isinstance(cid, str) or len(cid) < 1:
            return (False, "`catalogAdapterId` must be a non-empty string when provided")
        patch["catalog_adapter_id"] = cid
    if "operatorLabel" in body:
        ol = body["operatorLabel"]
        if ol is not None and not isinstance(ol, str):
            return (False, "`operatorLabel` must be a string or null")
        patch["operator_label"] = ol
    if not patch:
        return (False, "Provide at least one of `catalogAdapterId`, `operatorLabel`")
    return (True, patch)


def create_app(store: AdapterInstanceStore | None = None) -> FastAPI:
    app = FastAPI(title="Canal ingestion control plane", version=SERVICE_VERSION)
    app.state.adapter_store = store or AdapterInstanceStore()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {
            "status": "ok",
            "service": "ingestion-control-plane",
            "version": SERVICE_VERSION,
        }

    @app.get("/v1/control/pipeline")
    def control_pipeline() -> dict[str, Any]:
        return get_pipeline_summary_read()

    @app.get("/v1/control/canal/segments")
    def control_canal_segments() -> dict[str, Any]:
        return get_canal_segments_read()

    @app.get("/v1/adapter-instances")
    def list_adapter_instances(request: Request) -> dict[str, Any]:
        st: AdapterInstanceStore = request.app.state.adapter_store
        return {"items": [r.as_dict() for r in st.list()]}

    @app.get("/v1/adapter-instances/{instance_id}")
    def get_adapter_instance(instance_id: str, request: Request) -> JSONResponse:
        st: AdapterInstanceStore = request.app.state.adapter_store
        rec = st.get(instance_id)
        if rec is None:
            return JSONResponse(
                status_code=404,
                content={"error": "not_found", "message": "Adapter instance not found"},
            )
        return JSONResponse(content=rec.as_dict())

    @app.post("/v1/adapter-instances")
    async def create_adapter_instance(request: Request) -> JSONResponse:
        st: AdapterInstanceStore = request.app.state.adapter_store
        body = await request.json()
        parsed = _parse_create_body(body)
        if parsed[0] is False:
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error", "message": parsed[1]},
            )
        _, cid, label = parsed
        created = st.create(cid, label)
        if created[0] is False:
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error", "message": created[1]},
            )
        return JSONResponse(status_code=201, content=created[1].as_dict())

    @app.patch("/v1/adapter-instances/{instance_id}")
    async def patch_adapter_instance(instance_id: str, request: Request) -> JSONResponse:
        st: AdapterInstanceStore = request.app.state.adapter_store
        body = await request.json()
        parsed = _parse_patch_body(body)
        if parsed[0] is False:
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error", "message": parsed[1]},
            )
        _, patch = parsed
        updated = st.update(instance_id, **patch)
        if updated[0] is False:
            if updated[1] == "not_found":
                return JSONResponse(
                    status_code=404,
                    content={"error": "not_found", "message": "Adapter instance not found"},
                )
            return JSONResponse(
                status_code=400,
                content={"error": "validation_error", "message": updated[1]},
            )
        return JSONResponse(content=updated[1].as_dict())

    @app.delete("/v1/adapter-instances/{instance_id}")
    def delete_adapter_instance(instance_id: str, request: Request) -> Response:
        st: AdapterInstanceStore = request.app.state.adapter_store
        if not st.delete(instance_id):
            return JSONResponse(
                status_code=404,
                content={"error": "not_found", "message": "Adapter instance not found"},
            )
        return Response(status_code=204)

    return app


app = create_app()

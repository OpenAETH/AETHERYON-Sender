"""api/supervision.py — Torre de control operacional."""
from fastapi import APIRouter, Depends, Request

from backend.api.deps import require_auth
from backend.services import supervision_service

router = APIRouter(tags=["supervision"], dependencies=[Depends(require_auth)])


@router.get("/supervision")
def get_supervision(refresh: bool = True):
    return supervision_service.get_supervision(refresh)


@router.get("/supervision/summary")
def get_supervision_summary():
    """Panorama unificado: enviados, programados, pendientes, fallidos,
    reintentos y estado del sistema."""
    return supervision_service.get_control_tower()


@router.get("/stats")
def get_stats():
    return supervision_service.get_stats()


@router.get("/logs")
def get_logs(limit: int = 100):
    return supervision_service.get_logs(limit)


@router.get("/logs/{log_id}")
def get_log(log_id: int):
    from fastapi import HTTPException
    try:
        return supervision_service.get_log(log_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/memory")
def get_memory(limit: int = 100):
    return supervision_service.get_memory(limit)


@router.post("/memory")
async def add_memory(request: Request):
    data = await request.json()
    supervision_service.add_memory(data)
    return {"success": True}

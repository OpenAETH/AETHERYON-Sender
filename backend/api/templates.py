"""
api/templates.py — Templates: crear, editar, duplicar, activar, archivar y
reutilizar plantillas HTML con variables.
"""
from fastapi import APIRouter, Depends, HTTPException, Request

from backend.api.deps import require_auth
from backend.services import sender_service, template_service

router = APIRouter(prefix="/templates", tags=["templates"], dependencies=[Depends(require_auth)])


@router.get("")
def list_templates(status: str = None):
    return template_service.list_templates(status)


@router.post("")
async def create_template(request: Request):
    data = await request.json()
    try:
        tid = template_service.create_template(data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, "id": tid}


@router.get("/{tid}")
def get_template(tid: int):
    try:
        return template_service.get_template(tid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.put("/{tid}")
async def update_template(tid: int, request: Request):
    data = await request.json()
    template_service.update_template(tid, data)
    return {"success": True}


@router.delete("/{tid}")
def delete_template(tid: int):
    template_service.delete_template(tid)
    return {"success": True}


@router.post("/{tid}/duplicate")
async def duplicate_template(tid: int, request: Request):
    data = await request.json() if request.headers.get("content-length", "0") != "0" else {}
    try:
        new_id = template_service.duplicate_template(tid, data.get("name"))
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"success": True, "id": new_id}


@router.post("/{tid}/archive")
def archive_template(tid: int):
    template_service.set_status(tid, "archived")
    return {"success": True}


@router.post("/{tid}/activate")
def activate_template(tid: int):
    template_service.set_status(tid, "active")
    return {"success": True}


@router.post("/{tid}/preview")
async def preview_template(tid: int, request: Request):
    data = await request.json() if request.headers.get("content-length", "0") != "0" else {}
    try:
        return template_service.preview_template(tid, contact=data.get("contact"), cta_url=data.get("cta_url"))
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/{tid}/use")
async def use_template(tid: int, request: Request):
    """Instancia la plantilla como un envío programado del Cron Sender."""
    data = await request.json()
    try:
        result = sender_service.create_from_template(tid, data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, **result}

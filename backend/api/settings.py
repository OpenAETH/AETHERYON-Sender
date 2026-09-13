"""api/settings.py — Configuración, contexto y estado general."""
from fastapi import APIRouter, Depends, Request

from backend.api.deps import require_auth
from backend.config import cfg
from backend.services import settings_service

router = APIRouter(tags=["settings"], dependencies=[Depends(require_auth)])


@router.get("/api/status")
def api_status():
    c = cfg()
    return {
        "status": "AETHERYON Outreach Sender",
        "smtp_user": c["sender_email"] or "NO CONFIG",
        "imap_host": c["imap_host"] or "NO CONFIG",
        "provider": "resend",
        "resend_ready": bool(c["resend_api_key"] and c["sender_email"]),
    }


@router.get("/config")
def get_config():
    return settings_service.get_sender_config()


@router.get("/settings")
def get_all_settings():
    return settings_service.get_all_settings()


@router.post("/settings")
async def save_settings(request: Request):
    data = await request.json()
    settings_service.save_settings(data)
    return {"success": True}


@router.get("/context")
def get_context():
    return settings_service.get_context()


@router.post("/context")
async def save_context(request: Request):
    data = await request.json()
    settings_service.save_context(data)
    return {"success": True}

"""api/inbox.py — Bandeja de entrada (IMAP), componente secundario."""
from fastapi import APIRouter, Depends, Request

from backend.api.deps import require_auth
from backend.services import inbox_service

router = APIRouter(prefix="/inbox", tags=["inbox"], dependencies=[Depends(require_auth)])


@router.get("")
def list_messages(refresh: bool = False):
    return inbox_service.list_messages(refresh)


@router.post("/mark-replied")
async def mark_replied(request: Request):
    data = await request.json()
    inbox_service.mark_replied(data.get("message_id", ""))
    return {"success": True}


@router.post("/save-suggestion")
async def save_suggestion(request: Request):
    data = await request.json()
    inbox_service.save_suggestion(data.get("message_id", ""), data.get("suggestion", ""))
    return {"success": True}


@router.delete("/{message_id:path}")
def delete_message(message_id: str):
    inbox_service.delete_message(message_id)
    return {"success": True}

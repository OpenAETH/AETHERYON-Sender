"""
api/agenda.py — Agenda (gestión de destinatarios, NO un CRM) + importación
de audiencia externa (LeadForge, Apollo u otra fuente entregada como
CSV/JSON — nunca un módulo interno del Sender).
"""
import csv
import io
import json

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File

from backend.api.deps import require_auth
from backend.services import agenda_service

router = APIRouter(tags=["agenda"], dependencies=[Depends(require_auth)])


@router.get("/contacts")
def list_contacts():
    return agenda_service.list_contacts()


# Rutas literales ANTES de /contacts/{cid} para que FastAPI no las capture
# como parámetro de path.
@router.get("/contacts/sender-status")
def sender_status():
    """Cruza destinatarios con el estado de sus envíos programados (Cron
    Sender), para mostrar un badge informativo en la Agenda."""
    return agenda_service.sender_status_by_email()


@router.post("/contacts")
async def create_contact(request: Request):
    data = await request.json()
    if not data.get("name") or not data.get("email"):
        raise HTTPException(400, "name y email son obligatorios")
    try:
        cid = agenda_service.create_contact(data)
    except ValueError as e:
        raise HTTPException(409, str(e))
    return {"success": True, "id": cid}


@router.put("/contacts/{cid}")
async def update_contact(cid: int, request: Request):
    data = await request.json()
    agenda_service.update_contact(cid, data)
    return {"success": True}


@router.delete("/contacts/{cid}")
def delete_contact(cid: int):
    agenda_service.delete_contact(cid)
    return {"success": True}


@router.patch("/contacts/{cid}/status")
async def update_contact_status(cid: int, request: Request):
    data = await request.json()
    status = (data.get("status") or "").strip()
    if not status:
        raise HTTPException(400, "status es obligatorio")
    agenda_service.update_contact_tag_status(cid, status)
    return {"success": True}


@router.get("/contacts/{cid}/interactions")
def list_interactions(cid: int):
    return agenda_service.list_interactions(cid)


@router.post("/contacts/{cid}/interactions")
async def create_interaction(cid: int, request: Request):
    data = await request.json()
    agenda_service.create_interaction(cid, data)
    return {"success": True}


@router.get("/contacts/{contact_id}/sent-history")
def get_contact_sent_history(contact_id: int):
    try:
        return agenda_service.get_sent_history(contact_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


# ── Importación de audiencia externa (reemplaza el puente SQLite/LeadForge) ──

@router.post("/agenda/import")
async def import_audience(
    file: UploadFile = File(None),
    request: Request = None,
):
    """
    Importa una lista de destinatarios desde:
    - un archivo CSV subido como multipart (`file`), con columnas
      name/nombre, email, company/empresa, role/cargo, phone/telefono, tags; o
    - un cuerpo JSON `{"rows": [{...}, ...], "source": "apollo"}`.

    LeadForge, Apollo o cualquier otra herramienta de scraping/enrichment son
    fuentes externas: le entregan al Sender una lista y el Sender la importa
    por UPSERT (por email), sin conectarse a esas herramientas directamente.
    """
    rows = []
    source = "import-csv"
    if file is not None:
        raw = (await file.read()).decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)
        source = "import-csv"
    else:
        body = await request.json()
        rows = body.get("rows", [])
        source = (body.get("source") or "import-json").strip()

    if not rows:
        raise HTTPException(400, "No se encontraron filas para importar")

    try:
        result = agenda_service.import_audience(rows, source=source)
    except Exception as e:
        raise HTTPException(500, f"Error importando audiencia: {e}")
    return {"success": True, **result}

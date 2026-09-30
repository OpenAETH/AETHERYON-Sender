"""
api/sender.py — Cron Sender: gestiona cola, fecha/hora, frecuencia, límites,
pausa, reanudación, cancelación, reintentos y estados de envío.

Se mantiene el prefijo histórico `/campaigns` (detalle interno de la API,
no visible para quien usa la app) para no romper compatibilidad; el
producto y la navegación se presentan como "Cron Sender".
"""
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File
from fastapi.responses import Response, StreamingResponse

from backend.api.deps import require_auth
from backend.services import ai_service, sender_service

router = APIRouter(prefix="/campaigns", tags=["sender"], dependencies=[Depends(require_auth)])


# ── Rutas literales (antes de /{cid}) ──────────────────────────

@router.get("")
def list_sequences():
    return sender_service.list_sequences()


@router.get("/schedule")
def get_schedule():
    return sender_service.get_schedule()


@router.post("/process-scheduled")
async def process_scheduled(request: Request):
    """Dispara manualmente un ciclo del Cron Sender (además del loop
    automático de fondo). Con `{"email_ids":[...]}` fuerza el envío
    inmediato de piezas puntuales (por lo general demoradas)."""
    data = await request.json() if request.headers.get("content-length", "0") != "0" else {}
    email_ids = data.get("email_ids")
    if email_ids:
        try:
            return sender_service.force_send_emails(email_ids)
        except ValueError as e:
            raise HTTPException(400, str(e))
    return sender_service.process_due()


@router.post("/retry-failed")
async def retry_failed(request: Request):
    data = await request.json() if request.headers.get("content-length", "0") != "0" else {}
    try:
        return sender_service.retry_failed(campaign_id=data.get("campaign_id"), queue_id=data.get("queue_id"))
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/schedule-email/{email_id}")
def get_schedule_email(email_id: int):
    try:
        return sender_service.get_email_detail(email_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/schedule-email/{email_id}/send-now")
def send_schedule_email_now(email_id: int):
    """Envía inmediatamente un email de la agenda (scheduled/retrying/delayed)
    sin esperar a que llegue su horario programado.
    email_id es el id de campaign_emails (mismo que usa get_schedule_email)."""
    try:
        return sender_service.force_send_emails([email_id])
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/schedule-email/{email_id}/reschedule")
async def reschedule_email(email_id: int, request: Request):
    data = await request.json()
    try:
        sender_service.reschedule_email(email_id, data.get("scheduled_at", ""))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True}


@router.post("/schedule-email/{email_id}/cancel")
def cancel_schedule_email(email_id: int):
    try:
        sender_service.cancel_email(email_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True}


@router.post("/import")
async def import_sequence(request: Request):
    body = await request.json()
    try:
        result = sender_service.import_from_yaml(body.get("yaml", ""))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, **result}


@router.post("/init")
async def init_sequence(request: Request):
    data = await request.json()
    try:
        result = sender_service.init_sequence(data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, **result}


@router.post("/generate")
async def generate_sequence(request: Request):
    data = await request.json()
    try:
        result = await sender_service.generate_and_save(data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, **result}


@router.post("/from-templates")
async def create_from_templates(request: Request):
    """Crea una secuencia multi-dia seleccionando plantillas existentes
    (una por dia, en el orden elegido), sin pasar por generacion con IA."""
    data = await request.json()
    try:
        result = sender_service.create_sequence_from_templates(data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, **result}


# ── Rutas con {cid} ─────────────────────────────────────────────

@router.get("/{cid}")
def get_sequence(cid: int):
    try:
        return sender_service.get_sequence(cid)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.delete("/{cid}")
def delete_sequence(cid: int):
    sender_service.delete_sequence(cid)
    return {"success": True}


@router.get("/{cid}/export")
def export_sequence(cid: int):
    try:
        yaml_text, safe_name = sender_service.export_to_yaml(cid)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return Response(content=yaml_text, media_type="application/x-yaml",
                     headers={"Content-Disposition": f'attachment; filename="{safe_name}.yaml"'})


@router.get("/{cid}/attachments")
def list_sequence_attachments(cid: int):
    return sender_service.list_campaign_attachments(cid)


@router.post("/{cid}/attachments")
async def add_sequence_attachments(cid: int, request: Request):
    data = await request.json()
    try:
        n = sender_service.add_campaign_attachments(cid, data.get("attachment_ids", []))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True, "added": n}


@router.delete("/{cid}/attachments/{att_id}")
def remove_sequence_attachment(cid: int, att_id: int):
    sender_service.remove_campaign_attachment(cid, att_id)
    return {"success": True}


@router.post("/{cid}/approve-email")
async def approve_email(cid: int, request: Request):
    data = await request.json()
    try:
        sender_service.approve_email(cid, email_id=data.get("email_id"), day_number=data.get("day_number"), action=data.get("action", "approved"))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True}


@router.post("/{cid}/retry-email")
async def retry_email(cid: int, request: Request):
    """Dos modos: edición manual (subject/body explícitos) o regeneración
    con IA (feedback opcional)."""
    data = await request.json()
    email_id = data.get("email_id")
    if not email_id:
        raise HTTPException(400, "email_id es obligatorio")

    if data.get("subject") is not None or data.get("body") is not None:
        return sender_service.manual_edit_email(cid, email_id=email_id, subject=data.get("subject", ""), body=data.get("body", ""), content_type=data.get("content_type"))

    try:
        return await ai_service.retry_sequence_email(cid, email_id, data.get("feedback", ""))
    except Exception as e:
        raise HTTPException(502, f"Error regenerando con IA: {e}")


@router.post("/{cid}/generate-one")
async def generate_one(cid: int, request: Request):
    data = await request.json()
    day_number = data.get("day_number")
    if not day_number:
        raise HTTPException(400, "day_number es obligatorio")
    return StreamingResponse(
        ai_service.stream_sequence_email(cid, day_number, data.get("feedback", "")),
        media_type="text/event-stream",
    )


@router.post("/{cid}/finalize")
def finalize_sequence(cid: int):
    try:
        return sender_service.finalize_sequence(cid)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/{cid}/send-now")
def send_now(cid: int):
    try:
        return sender_service.send_now(cid)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/{cid}/pause")
def pause_sequence(cid: int):
    try:
        sender_service.pause_sequence(cid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True}


@router.post("/{cid}/resume")
def resume_sequence(cid: int):
    try:
        sender_service.resume_sequence(cid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"success": True}


@router.post("/{cid}/cancel")
def cancel_sequence(cid: int):
    """Cancelación blanda: la comunicación queda visible en Supervisión pero
    deja de enviar lo pendiente. Para borrarla del todo, usar DELETE."""
    sender_service.cancel_sequence(cid)
    return {"success": True}

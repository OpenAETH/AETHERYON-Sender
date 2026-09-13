"""
api/composition.py — Redacción: preparar y enviar una comunicación puntual,
con adjuntos, preview y envío de prueba.
"""
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File

from backend.api.deps import require_auth
from backend.services import composition_service

router = APIRouter(tags=["composition"], dependencies=[Depends(require_auth)])


@router.post("/send-email")
async def send_email(request: Request):
    data = await request.json()
    recipients = data.get("to") or data.get("recipients") or []
    if isinstance(recipients, str):
        recipients = [recipients]
    try:
        result = composition_service.send_direct(
            recipients=recipients,
            subject=data.get("subject", ""),
            body=data.get("body", ""),
            intent=data.get("intent", "general"),
            campaign_id=data.get("campaign_id"),
            reply_to=data.get("reply_to"),
            attachment_ids=data.get("attachment_ids"),
            cta_url=data.get("cta_url", ""),
            content_type=data.get("content_type", "markdown"),
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    if result["all_failed"]:
        raise HTTPException(502, f"Fallaron todos los envíos: {result['results']}")
    return result


@router.post("/send-test")
async def send_test(request: Request):
    """Envío de prueba: manda la comunicación tal cual está redactada a una
    sola dirección (por lo general la del propio usuario), antes de
    programarla para el resto de la Agenda."""
    data = await request.json()
    to = (data.get("to") or "").strip()
    if not to:
        raise HTTPException(400, "to es obligatorio")
    try:
        result = composition_service.send_test(
            to_email=to,
            subject=data.get("subject", ""),
            body=data.get("body", ""),
            attachment_ids=data.get("attachment_ids"),
            cta_url=data.get("cta_url", ""),
            content_type=data.get("content_type", "markdown"),
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    return result


@router.post("/preview-email")
async def preview_email(request: Request):
    data = await request.json()
    return composition_service.preview(
        body=data.get("body", ""),
        subject=data.get("subject", ""),
        style_overrides=data.get("style"),
        contact=data.get("contact"),
        cta_url=data.get("cta_url", ""),
        content_type=data.get("content_type", "markdown"),
    )


@router.get("/smtp-test")
def smtp_test():
    try:
        return composition_service.resend_status()
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    except Exception as e:
        raise HTTPException(502, f"Error consultando Resend: {e}")


@router.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    files_data = [(f.filename, await f.read(), f.content_type) for f in files]
    try:
        uploaded = composition_service.upload_files_sync(files_data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    return {"success": True, "files": uploaded}


@router.get("/attachments")
def list_attachments():
    return composition_service.list_attachments()


@router.delete("/attachments/{att_id}")
def delete_attachment(att_id: int):
    try:
        composition_service.delete_attachment(att_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"success": True}

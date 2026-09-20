"""
services/composition_service.py — Redacción: preparar y enviar una
comunicación puntual (no programada). Cubre: cuerpo HTML + texto alternativo,
variables, adjuntos, preview y envío de prueba.

El envío programado (secuencias, cadencia, reintentos) vive en
sender_service.py — este módulo es el camino corto "escribo y mando ahora".
"""
import logging
import uuid

from sqlalchemy import text

from backend.config import cfg
from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain.email_content import assemble, html_to_text, is_html_mode
from backend.domain.templates import build_context, render
from backend.providers import resend_provider, storage_provider
from backend.services.settings_service import build_style

logger = logging.getLogger(__name__)


def _find_contact_by_email(db, email: str):
    row = dict_from_row(db.execute(text("SELECT * FROM contacts WHERE email=:email"), {"email": email}).fetchone())
    return row


def resolve_attachments(db, attachment_ids: list) -> list:
    if not attachment_ids:
        return []
    rows = rows_to_list(db.execute(text("SELECT * FROM attachments WHERE id = ANY(:ids)"), {"ids": attachment_ids}).fetchall())
    resolved = []
    for row in rows:
        file_bytes = storage_provider.download(cfg()["storage_bucket"], row["storage_path"])
        resolved.append({"filename": row["filename"], "content": file_bytes, "content_type": row["content_type"]})
    return resolved


def _log_sent(db, to, subject, body, body_html, intent, campaign_id, attachment_ids=None, message_id=None):
    row = dict_from_row(db.execute(text("SELECT name FROM contacts WHERE email=:email"), {"email": to}).fetchone())
    cname = row["name"] if row else to.split("@")[0]
    log_result = db.execute(
        text("""INSERT INTO email_logs
                (direction,contact_email,contact_name,subject,body,body_html,intent,status,sent_at,campaign_id,message_id)
                VALUES ('out',:contact_email,:contact_name,:subject,:body,:body_html,:intent,'sent',NOW(),:campaign_id,:message_id)
                RETURNING id"""),
        {"contact_email": to, "contact_name": cname, "subject": subject, "body": body,
         "body_html": body_html, "intent": intent, "campaign_id": campaign_id, "message_id": message_id},
    )
    email_log_id = log_result.fetchone()[0]
    db.execute(
        text("INSERT INTO memory (type,entity,content,importance) VALUES ('email_sent',:entity,:content,2)"),
        {"entity": to, "content": f"Email enviado a {cname}: {subject}"},
    )
    if attachment_ids:
        for aid in attachment_ids:
            db.execute(text("INSERT INTO email_attachments (attachment_id, email_log_id) VALUES (:aid, :eid)"), {"aid": aid, "eid": email_log_id})


def render_for_recipient(subject: str, body: str, to_email: str, db, cta_url: str = "", content_type: str = "markdown") -> dict:
    """Resuelve variables ({{first_name}}, {{company}}, ...) para UN
    destinatario puntual usando su ficha de Agenda si existe, arma el HTML
    final y devuelve todo listo para enviar.

    `content_type` distingue el modo de Redacción:
    - 'markdown': `body` es Markdown liviano, se envuelve en la plantilla
      visual de AETHERYON.
    - 'html': `body` es un template ya diseñado y completo (pegado por el
      usuario) — se respeta tal cual, sin envolver."""
    style = build_style()
    contact = _find_contact_by_email(db, to_email) or {"email": to_email}
    context = build_context(contact, sender_name=style.get("sender_name") or cfg()["sender_name"], cta_url=cta_url)
    rendered_subject, _ = render(subject or "", context)
    rendered_body, _ = render(body or "", context)
    html = assemble(rendered_body, content_type, style)
    plain = html_to_text(html) if is_html_mode(rendered_body, content_type) else rendered_body
    return {"subject": rendered_subject, "body": plain, "html": html}


def send_direct(recipients: list, subject: str, body: str, intent: str = "general",
                 campaign_id: int = None, reply_to: str = None, attachment_ids: list = None,
                 cta_url: str = "", content_type: str = "markdown") -> dict:
    """Envío individual o múltiple (Redacción). Cada destinatario recibe su
    propia versión con variables resueltas."""
    if not recipients:
        raise ValueError("Al menos un destinatario es requerido")
    if not subject.strip():
        raise ValueError("subject es obligatorio")
    if not body.strip():
        raise ValueError("body es obligatorio")
    if not resend_provider.is_configured():
        raise RuntimeError("RESEND_API_KEY / SENDER_EMAIL no configurados")

    results = []
    db = get_db()
    try:
        attachments = resolve_attachments(db, attachment_ids) if attachment_ids else []
        for to in recipients:
            try:
                rendered = render_for_recipient(subject, body, to, db, cta_url, content_type)
                resp = resend_provider.send(to, rendered["subject"], rendered["body"], rendered["html"], reply_to, attachments)
                email_id = resp.id if hasattr(resp, "id") else None
                _log_sent(db, to, rendered["subject"], rendered["body"], rendered["html"], intent, campaign_id, attachment_ids, email_id)
                results.append({"to": to, "ok": True, "resend_id": email_id})
            except Exception as e:
                logger.error(f"Error enviando a {to}: {type(e).__name__}: {e}")
                results.append({"to": to, "ok": False, "error": f"{type(e).__name__}: {e}"})
        db.commit()
    finally:
        db.close()

    sent_ok = [r for r in results if r["ok"]]
    sent_err = [r for r in results if not r["ok"]]
    return {"sent": len(sent_ok), "failed": len(sent_err), "results": results, "all_failed": bool(sent_err and not sent_ok)}


def send_test(to_email: str, subject: str, body: str, attachment_ids: list = None, cta_url: str = "", content_type: str = "markdown") -> dict:
    """Envío de prueba: misma tubería que un envío real, marcado con
    intent='test' para no mezclarse con el historial operativo real, pero
    visible en Supervisión para verificar que todo funciona antes de
    programar una comunicación."""
    return send_direct([to_email], subject, body, intent="test", attachment_ids=attachment_ids, cta_url=cta_url, content_type=content_type)


def preview(body: str, subject: str = "", style_overrides: dict = None, contact: dict = None, cta_url: str = "", content_type: str = "markdown") -> dict:
    style = build_style()
    if style_overrides:
        style.update({k: v for k, v in style_overrides.items() if v is not None})
    context = build_context(contact, sender_name=style.get("sender_name"), cta_url=cta_url)
    rendered_subject, _ = render(subject or "", context)
    rendered_body, _ = render(body or "", context)
    return {"subject": rendered_subject, "html": assemble(rendered_body, content_type, style)}


def upload_files_sync(files_data: list) -> list:
    """`files_data`: lista de (filename, content_bytes, content_type).
    Sube a Supabase Storage y registra metadata."""
    c = cfg()
    if not storage_provider.is_configured():
        raise RuntimeError("Supabase Storage no configurado — revisá SUPABASE_URL y SUPABASE_SERVICE_KEY")

    uploaded = []
    db = get_db()
    try:
        for filename, contents, content_type in files_data:
            size = len(contents)
            if size > 50 * 1024 * 1024:
                raise ValueError(f"Archivo {filename} excede 50 MB")
            ext = filename.rsplit(".", 1)[-1] if "." in filename else ""
            storage_path = f"{uuid.uuid4().hex}.{ext}" if ext else uuid.uuid4().hex
            storage_provider.upload(c["storage_bucket"], storage_path, contents, content_type)
            result = db.execute(text("""
                INSERT INTO attachments (filename, content_type, size, storage_path)
                VALUES (:fn, :ct, :sz, :sp) RETURNING id
            """), {"fn": filename, "ct": content_type or "application/octet-stream", "sz": size, "sp": storage_path})
            att_id = result.fetchone()[0]
            uploaded.append({"id": att_id, "filename": filename, "size": size, "content_type": content_type})
        db.commit()
        return uploaded
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def list_attachments() -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(text(
            "SELECT id, filename, content_type, size, created_at FROM attachments ORDER BY created_at DESC"
        )).fetchall())
    finally:
        db.close()


def delete_attachment(att_id: int):
    c = cfg()
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("SELECT * FROM attachments WHERE id=:id"), {"id": att_id}).fetchone())
        if not row:
            raise ValueError("Attachment no encontrado")
        try:
            storage_provider.delete(c["storage_bucket"], [row["storage_path"]])
        except Exception as e:
            logger.warning(f"Error eliminando archivo de Storage: {e}")
        db.execute(text("DELETE FROM attachments WHERE id=:id"), {"id": att_id})
        db.commit()
    finally:
        db.close()


def resend_status() -> dict:
    c = cfg()
    if not resend_provider.is_configured():
        raise RuntimeError("RESEND_API_KEY / SENDER_EMAIL no configurados")
    domains = resend_provider.list_domains()
    return {"ok": True, "provider": "resend", "sender": c["sender_email"], "sender_name": c["sender_name"], "domains": domains}

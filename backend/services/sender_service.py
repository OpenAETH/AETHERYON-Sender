"""
services/sender_service.py — Cron Sender.

Núcleo operativo del producto: "el usuario prepara la comunicación, programa
la máquina y se va". Gestiona secuencias (`campaigns`/`campaign_emails`),
destinatarios (`campaign_contacts`) y la cola real de envío por destinatario
(`send_queue`), con pausa, reanudación, cancelación, reintentos con backoff
y límites por corrida.
"""
import logging
import re
from datetime import datetime, timedelta

import yaml
from sqlalchemy import text

from backend.config import DEFAULT_RATE_LIMIT_PER_RUN, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_BACKOFF_MINUTES
from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain import scheduling
from backend.domain.email_content import assemble, html_to_text
from backend.domain.templates import build_context, render
from backend.providers import resend_provider
from backend.services import ai_service
from backend.services.composition_service import resolve_attachments
from backend.services.settings_service import build_style

logger = logging.getLogger(__name__)


def _parse_scheduled_at(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        return datetime.utcnow()
    if len(s) == 10:
        s = s + " 09:00"
    try:
        return datetime.strptime(s[:16], "%Y-%m-%d %H:%M")
    except ValueError:
        return datetime.utcnow()


def _log_sent_from_queue(db, to, subject, body, body_html, campaign_id, message_id):
    row = dict_from_row(db.execute(text("SELECT name FROM contacts WHERE email=:email"), {"email": to}).fetchone())
    cname = row["name"] if row else to.split("@")[0]
    db.execute(text("""
        INSERT INTO email_logs (direction,contact_email,contact_name,subject,body,body_html,intent,status,sent_at,campaign_id,message_id)
        VALUES ('out',:contact_email,:contact_name,:subject,:body,:body_html,'sender','sent',NOW(),:campaign_id,:message_id)
    """), {"contact_email": to, "contact_name": cname, "subject": subject, "body": body,
           "body_html": body_html, "campaign_id": campaign_id, "message_id": message_id})
    db.execute(
        text("INSERT INTO memory (type,entity,content,importance) VALUES ('email_sent',:entity,:content,2)"),
        {"entity": to, "content": f"Email enviado a {cname}: {subject}"},
    )


def _populate_queue(campaign_id: int):
    """Puebla `send_queue` a partir de campaign_emails aprobados x contactos
    asignados. Idempotente (ON CONFLICT DO NOTHING) — segura de llamar de
    nuevo al reanudar o reforzar un envío inmediato."""
    db = get_db()
    try:
        emails = rows_to_list(db.execute(text(
            "SELECT id, scheduled_at FROM campaign_emails WHERE campaign_id=:cid AND status='approved'"
        ), {"cid": campaign_id}).fetchall())
        contacts = rows_to_list(db.execute(text("SELECT email FROM campaign_contacts WHERE campaign_id=:cid"), {"cid": campaign_id}).fetchall())
        recipients = [c["email"].strip() for c in contacts if (c.get("email") or "").strip()]
        for em in emails:
            next_at = _parse_scheduled_at(em.get("scheduled_at"))
            for rcpt in recipients:
                db.execute(text("""
                    INSERT INTO send_queue (campaign_id, campaign_email_id, recipient_email, status, max_attempts, next_attempt_at)
                    VALUES (:cid, :ceid, :email, 'pending', :max_att, :next_at)
                    ON CONFLICT (campaign_email_id, recipient_email) DO NOTHING
                """), {"cid": campaign_id, "ceid": em["id"], "email": rcpt, "max_att": DEFAULT_MAX_ATTEMPTS, "next_at": next_at})
        db.commit()
    finally:
        db.close()


# ────────────────────────────────────────────────────────────
# Secuencias — ciclo de vida (crear, redactar/generar, aprobar, finalizar)
# ────────────────────────────────────────────────────────────

def list_sequences() -> list:
    db = get_db()
    try:
        rows = db.execute(text("""
            SELECT c.*,
                   COUNT(e.id) AS total_emails,
                   SUM(CASE WHEN e.status='approved' THEN 1 ELSE 0 END) AS approved_count,
                   SUM(CASE WHEN e.status='pending'  THEN 1 ELSE 0 END) AS pending_count
            FROM campaigns c
            LEFT JOIN campaign_emails e ON e.campaign_id = c.id
            WHERE c.status != 'template'
            GROUP BY c.id ORDER BY c.created_at DESC
        """)).fetchall()
        return rows_to_list(rows)
    except Exception as e:
        logger.error(f"list_sequences: {e}")
        return []
    finally:
        db.close()


def get_sequence(cid: int) -> dict:
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise ValueError("Comunicación no encontrada")
        emails = rows_to_list(db.execute(text("""
            SELECT ce.*,
                COUNT(sq.id) AS queue_total,
                SUM(CASE WHEN sq.status='sent' THEN 1 ELSE 0 END) AS queue_sent,
                SUM(CASE WHEN sq.status='pending' AND sq.attempts=0 THEN 1 ELSE 0 END) AS queue_pending,
                SUM(CASE WHEN sq.status='pending' AND sq.attempts>0 THEN 1 ELSE 0 END) AS queue_retrying,
                SUM(CASE WHEN sq.status='failed' THEN 1 ELSE 0 END) AS queue_failed,
                SUM(CASE WHEN sq.status='cancelled' THEN 1 ELSE 0 END) AS queue_cancelled
            FROM campaign_emails ce
            LEFT JOIN send_queue sq ON sq.campaign_email_id = ce.id
            WHERE ce.campaign_id=:id GROUP BY ce.id ORDER BY ce.day_number
        """), {"id": cid}).fetchall())
        contacts = rows_to_list(db.execute(text("SELECT * FROM campaign_contacts WHERE campaign_id=:id"), {"id": cid}).fetchall())
        attachments = rows_to_list(db.execute(text("""
            SELECT a.id, a.filename, a.content_type, a.size, a.created_at
            FROM attachments a JOIN campaign_attachments ca ON ca.attachment_id = a.id
            WHERE ca.campaign_id = :cid
        """), {"cid": cid}).fetchall())
        camp["emails"] = emails
        camp["contacts"] = contacts
        camp["attachments"] = attachments
        return camp
    finally:
        db.close()


def delete_sequence(cid: int):
    db = get_db()
    try:
        db.execute(text("DELETE FROM send_queue WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaign_emails WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaign_contacts WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaign_attachments WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaigns WHERE id=:id"), {"id": cid})
        db.commit()
    finally:
        db.close()


def _validate_sequence_input(data: dict):
    if not data.get("contacts"):
        raise ValueError("Se requiere al menos un contacto")
    if not data.get("start_date") or not data.get("end_date"):
        raise ValueError("Fechas requeridas")
    if not (data.get("intent") or "").strip():
        raise ValueError("Intención requerida")


def init_sequence(data: dict) -> dict:
    """Crea la estructura vacía; el contenido se redacta pieza por pieza
    (a mano o con asistencia de IA vía generate-one)."""
    _validate_sequence_input(data)
    start_date, end_date = data["start_date"], data["end_date"]
    send_mode = data.get("send_mode", "daily")
    send_time = data.get("send_time", "09:00")
    dates = scheduling.get_dates_in_range(start_date, end_date, send_mode)
    if not dates:
        raise ValueError(f"No hay fechas válidas con modo '{send_mode}'")
    n = len(dates)

    db = get_db()
    try:
        cid = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, send_time, status)
            VALUES (:name,:intent,:start_date,:end_date,:send_mode,:tone,:send_time,'draft') RETURNING id
        """), {
            "name": data.get("name") or f"Comunicación {start_date}", "intent": data["intent"].strip(),
            "start_date": start_date, "end_date": end_date, "send_mode": send_mode,
            "tone": data.get("tone", "informative"), "send_time": send_time,
        }).fetchone()[0]

        for i in range(n):
            db.execute(text("""
                INSERT INTO campaign_emails (campaign_id, day_number, subject, body, status, scheduled_at, version, regenerated_count)
                VALUES (:cid,:day,'','','pending',:sched,1,0)
            """), {"cid": cid, "day": i + 1, "sched": f"{dates[i]} {send_time}"})
        for email_addr in data["contacts"]:
            email_addr = (email_addr or "").strip()
            if email_addr:
                db.execute(text("INSERT INTO campaign_contacts (campaign_id, email) VALUES (:cid,:email)"), {"cid": cid, "email": email_addr})
        db.commit()
        return {"campaign_id": cid, "total_emails": n, "dates": dates}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def generate_and_save(data: dict) -> dict:
    """Genera con IA la secuencia completa de una vez y la guarda como borrador."""
    _validate_sequence_input(data)
    start_date, end_date = data["start_date"], data["end_date"]
    send_mode = data.get("send_mode", "daily")
    send_time = data.get("send_time", "09:00")
    tone = data.get("tone", "informative")
    name = data.get("name") or f"Comunicación {start_date}"
    intent = data["intent"].strip()

    dates = scheduling.get_dates_in_range(start_date, end_date, send_mode)
    if not dates:
        raise ValueError(f"No hay fechas válidas en el rango con modo '{send_mode}'")
    n = len(dates)

    emails_list = await ai_service.generate_sequence(
        data["contacts"], start_date, end_date, intent, send_mode, tone, send_time, name, n, dates
    )

    db = get_db()
    try:
        cid = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, send_time, status)
            VALUES (:name,:intent,:start_date,:end_date,:send_mode,:tone,:send_time,'draft') RETURNING id
        """), {"name": name, "intent": intent, "start_date": start_date, "end_date": end_date,
               "send_mode": send_mode, "tone": tone, "send_time": send_time}).fetchone()[0]

        for i, em in enumerate(emails_list):
            db.execute(text("""
                INSERT INTO campaign_emails (campaign_id, day_number, subject, body, status, scheduled_at, version, regenerated_count)
                VALUES (:cid,:day,:subject,:body,'pending',:sched,1,0)
            """), {
                "cid": cid, "day": em.get("day_number", i + 1), "subject": em.get("subject", ""), "body": em.get("body", ""),
                "sched": f"{dates[i] if i < len(dates) else dates[-1]} {send_time}",
            })
        for email_addr in data["contacts"]:
            email_addr = (email_addr or "").strip()
            if email_addr:
                db.execute(text("INSERT INTO campaign_contacts (campaign_id, email) VALUES (:cid,:email)"), {"cid": cid, "email": email_addr})
        db.commit()
        logger.info(f"Comunicación {cid} creada con {n} piezas")
        return {"campaign_id": cid, "total_emails": n, "dates": dates}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def approve_email(cid: int, email_id: int = None, day_number: int = None, action: str = "approved"):
    db = get_db()
    try:
        if email_id:
            db.execute(text("UPDATE campaign_emails SET status=:s WHERE id=:id AND campaign_id=:cid"), {"s": action, "id": email_id, "cid": cid})
        elif day_number:
            db.execute(text("UPDATE campaign_emails SET status=:s WHERE day_number=:d AND campaign_id=:cid"), {"s": action, "d": day_number, "cid": cid})
        else:
            raise ValueError("Se requiere email_id o day_number")
        db.commit()
    finally:
        db.close()


def manual_edit_email(cid: int, email_id: int = None, day_number: int = None, subject: str = "", body: str = "", content_type: str = None) -> dict:
    ct_clause = ", content_type=:ct" if content_type else ""
    db = get_db()
    try:
        params = {"s": subject, "b": body, "id": email_id, "cid": cid, "d": day_number}
        if content_type:
            params["ct"] = content_type
        if email_id:
            db.execute(text(f"""UPDATE campaign_emails SET subject=:s, body=:b, status='pending', version=version+1{ct_clause}
                               WHERE id=:id AND campaign_id=:cid"""), params)
        elif day_number:
            db.execute(text(f"""UPDATE campaign_emails SET subject=:s, body=:b, status='pending', version=version+1{ct_clause}
                               WHERE day_number=:d AND campaign_id=:cid"""), params)
        else:
            raise ValueError("Se requiere email_id o day_number")
        db.commit()
        return {"subject": subject, "body": body}
    finally:
        db.close()


def finalize_sequence(cid: int) -> dict:
    """Valida que todo esté aprobado, programa la comunicación y puebla la
    cola de envío del Cron Sender."""
    db = get_db()
    try:
        pending = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:cid AND status='pending'"), {"cid": cid}).fetchone())
        if pending["n"] > 0:
            raise ValueError("Hay piezas pendientes de aprobación")
        rejected = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:cid AND status='rejected'"), {"cid": cid}).fetchone())
        if rejected["n"] > 0:
            raise ValueError("Hay piezas rechazadas — regeneralas antes de finalizar")
        db.execute(text("UPDATE campaigns SET status='scheduled' WHERE id=:id"), {"id": cid})
        db.commit()
    finally:
        db.close()
    _populate_queue(cid)
    return {"message": "Comunicación programada exitosamente"}


# ────────────────────────────────────────────────────────────
# Templates → Cron Sender (reutilizar una plantilla como envío programado)
# ────────────────────────────────────────────────────────────

def create_from_template(tid: int, data: dict) -> dict:
    from backend.services import template_service

    tmpl = template_service.get_template(tid)
    contacts_list = [c.strip() for c in (data.get("contacts") or []) if c and c.strip()]
    scheduled_at = (data.get("scheduled_at") or "").strip()
    if not contacts_list:
        raise ValueError("Se requiere al menos un contacto")
    if not scheduled_at:
        raise ValueError("Se requiere fecha/hora de envío (scheduled_at)")
    if len(scheduled_at) == 10:
        scheduled_at = scheduled_at + " 09:00"

    name = (data.get("name") or "").strip() or tmpl["name"]
    cta_url = data.get("cta_url", tmpl.get("default_cta_url", "")) or ""

    db = get_db()
    try:
        cid = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, send_time, status, template_id, cta_url)
            VALUES (:name, :intent, :d, :d, 'daily', 'informative', :t, 'scheduled', :tid, :cta)
            RETURNING id
        """), {
            "name": name, "intent": f"Plantilla: {tmpl['name']}", "d": scheduled_at[:10],
            "t": scheduled_at[11:] or "09:00", "tid": tid, "cta": cta_url,
        }).fetchone()[0]

        db.execute(text("""
            INSERT INTO campaign_emails (campaign_id, day_number, subject, body, content_type, status, scheduled_at, version, regenerated_count)
            VALUES (:cid, 1, :subject, :body, :content_type, 'approved', :sched, 1, 0)
        """), {"cid": cid, "subject": tmpl["subject"], "body": tmpl["body"], "content_type": tmpl.get("content_type", "markdown"), "sched": scheduled_at})

        for email_addr in contacts_list:
            db.execute(text("INSERT INTO campaign_contacts (campaign_id, email) VALUES (:cid, :email)"), {"cid": cid, "email": email_addr})
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    _populate_queue(cid)
    return {"campaign_id": cid}


# ────────────────────────────────────────────────────────────
# Import / export YAML (portabilidad de secuencias)
# ────────────────────────────────────────────────────────────

def import_from_yaml(yaml_text: str) -> dict:
    if not yaml_text or not yaml_text.strip():
        raise ValueError("Archivo YAML vacío")
    try:
        data = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        raise ValueError(f"YAML inválido: {e}")
    if not isinstance(data, dict):
        raise ValueError("YAML inválido: se esperaba un objeto en la raíz")

    camp = data.get("campaign") or {}
    if not isinstance(camp, dict):
        raise ValueError("YAML inválido: 'campaign' debe ser un objeto")

    intent_raw = camp.get("intent", "")
    intent = (", ".join(str(x).strip() for x in intent_raw if str(x).strip())
              if isinstance(intent_raw, (list, tuple)) else str(intent_raw or "").strip())

    send_mode = str(camp.get("send_mode") or camp.get("schedule") or "daily").strip()
    mode_aliases = {"alternate_days": "alternate", "every_day": "daily", "mon_wed_fri": "mon_wed_fri", "tue_thu": "tue_thu"}
    send_mode = mode_aliases.get(send_mode, send_mode)

    name = str(camp.get("name") or "").strip()
    start_date = str(camp.get("start_date") or "").strip()
    end_date = str(camp.get("end_date") or "").strip()
    tone = str(camp.get("tone") or "informative").strip()
    send_time = str(camp.get("send_time") or "09:00").strip()

    emails_raw = data.get("emails") or []
    if not isinstance(emails_raw, list) or not emails_raw:
        raise ValueError("YAML inválido: se requiere al menos un email")
    emails = []
    for i, em in enumerate(emails_raw):
        if not isinstance(em, dict):
            raise ValueError(f"Email #{i + 1} inválido")
        emails.append({
            "day_number": em.get("day_number") or em.get("sequence") or (i + 1),
            "subject": str(em.get("subject") or "").strip(),
            "body": str(em.get("body") or "").strip(),
        })
    emails.sort(key=lambda e: e["day_number"])

    if not name:
        raise ValueError("Falta 'name' en la campaña")
    if not intent:
        raise ValueError("Falta 'intent' en la campaña")
    if not start_date:
        raise ValueError("Falta 'start_date' en la campaña")

    n = len(emails)
    dates = scheduling.get_dates_count(start_date, send_mode, n)
    if not dates or len(dates) < n:
        raise ValueError(f"No se pudieron calcular {n} fechas con modo '{send_mode}'")
    if not end_date:
        end_date = dates[-1]

    db = get_db()
    try:
        cid = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, send_time, status)
            VALUES (:name,:intent,:start_date,:end_date,:send_mode,:tone,:send_time,'draft') RETURNING id
        """), {"name": name, "intent": intent, "start_date": start_date, "end_date": end_date,
               "send_mode": send_mode, "tone": tone, "send_time": send_time}).fetchone()[0]
        for i in range(n):
            db.execute(text("""
                INSERT INTO campaign_emails (campaign_id, day_number, subject, body, status, scheduled_at, version, regenerated_count)
                VALUES (:cid,:day,:subject,:body,'pending',:sched,1,0)
            """), {"cid": cid, "day": i + 1, "subject": emails[i]["subject"], "body": emails[i]["body"], "sched": f"{dates[i]} {send_time}"})
        db.commit()
        return {"campaign_id": cid, "total_emails": n}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def export_to_yaml(cid: int) -> tuple:
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise ValueError("Comunicación no encontrada")
        emails = rows_to_list(db.execute(text(
            "SELECT day_number, subject, body FROM campaign_emails WHERE campaign_id=:id ORDER BY day_number"), {"id": cid}
        ).fetchall())
    finally:
        db.close()

    payload = {
        "version": 1,
        "campaign": {
            "name": camp.get("name") or "", "intent": camp.get("intent") or "",
            "start_date": camp.get("start_date") or "", "end_date": camp.get("end_date") or "",
            "send_mode": camp.get("send_mode") or "daily", "send_time": camp.get("send_time") or "09:00",
            "tone": camp.get("tone") or "informative",
        },
        "emails": [{"day_number": e["day_number"], "subject": e["subject"] or "", "body": e["body"] or ""} for e in emails],
    }
    yaml_text = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, default_flow_style=False)
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", (camp.get("name") or f"campaign_{cid}")).strip("_") or f"campaign_{cid}"
    return yaml_text, safe_name


# ────────────────────────────────────────────────────────────
# Adjuntos de campaña
# ────────────────────────────────────────────────────────────

def list_campaign_attachments(cid: int) -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(text("""
            SELECT a.id, a.filename, a.content_type, a.size, a.created_at
            FROM attachments a JOIN campaign_attachments ca ON ca.attachment_id = a.id
            WHERE ca.campaign_id = :cid ORDER BY a.created_at DESC
        """), {"cid": cid}).fetchall())
    finally:
        db.close()


def add_campaign_attachments(cid: int, attachment_ids: list) -> int:
    if not attachment_ids:
        raise ValueError("Se requiere al menos un attachment_id")
    db = get_db()
    try:
        for aid in attachment_ids:
            db.execute(text("INSERT INTO campaign_attachments (campaign_id, attachment_id) VALUES (:cid, :aid) ON CONFLICT DO NOTHING"), {"cid": cid, "aid": aid})
        db.commit()
        return len(attachment_ids)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def remove_campaign_attachment(cid: int, att_id: int):
    db = get_db()
    try:
        db.execute(text("DELETE FROM campaign_attachments WHERE campaign_id=:cid AND attachment_id=:aid"), {"cid": cid, "aid": att_id})
        db.commit()
    finally:
        db.close()


# ────────────────────────────────────────────────────────────
# Cron Sender — cola, pausa, reanudación, cancelación, reintentos
# ────────────────────────────────────────────────────────────

def get_schedule() -> list:
    """Agenda de envío unificada: cada pieza programada con el desglose de
    su cola (enviados/pendientes/reintentando/fallidos/cancelados)."""
    db = get_db()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    try:
        rows = rows_to_list(db.execute(text("""
            SELECT ce.id, ce.campaign_id, ce.day_number, ce.subject, ce.scheduled_at,
                   ce.status as approval_status,
                   c.name as camp_name, c.status as camp_status,
                   COUNT(sq.id) as total,
                   SUM(CASE WHEN sq.status='sent' THEN 1 ELSE 0 END) as sent,
                   SUM(CASE WHEN sq.status='pending' AND sq.attempts=0 THEN 1 ELSE 0 END) as pending,
                   SUM(CASE WHEN sq.status='pending' AND sq.attempts>0 THEN 1 ELSE 0 END) as retrying,
                   SUM(CASE WHEN sq.status='failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN sq.status='cancelled' THEN 1 ELSE 0 END) as cancelled
            FROM campaign_emails ce
            JOIN campaigns c ON c.id = ce.campaign_id
            LEFT JOIN send_queue sq ON sq.campaign_email_id = ce.id
            WHERE c.status IN ('scheduled','paused','sent','cancelled')
              AND ce.status IN ('approved','rejected')
            GROUP BY ce.id, c.id
            ORDER BY ce.scheduled_at ASC NULLS LAST
        """)).fetchall())

        items = []
        for r in rows:
            total = r["total"] or 0
            sent = r["sent"] or 0
            failed = r["failed"] or 0
            retrying = r["retrying"] or 0
            cancelled = r["cancelled"] or 0

            if r["camp_status"] == "cancelled" or r["approval_status"] == "rejected":
                state = "cancelled"
            elif r["camp_status"] == "paused":
                state = "paused"
            elif total > 0 and sent == total:
                state = "sent"
            elif total > 0 and failed > 0 and (failed + cancelled) == total:
                state = "failed"
            elif retrying > 0:
                state = "retrying"
            else:
                sched = str(r.get("scheduled_at") or "")
                if len(sched) == 10:
                    sched = sched + " 09:00"
                state = "delayed" if sched and sched[:16] <= now_str else "scheduled"

            items.append({**r, "state": state})
        return items
    finally:
        db.close()


def get_email_detail(email_id: int) -> dict:
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("""
            SELECT ce.*, c.name as camp_name, c.intent as camp_intent
            FROM campaign_emails ce JOIN campaigns c ON c.id = ce.campaign_id
            WHERE ce.id = :id
        """), {"id": email_id}).fetchone())
        if not row:
            raise ValueError("Pieza no encontrada")
        contacts = rows_to_list(db.execute(text("SELECT email FROM campaign_contacts WHERE campaign_id=:cid"), {"cid": row["campaign_id"]}).fetchall())
        row["contacts"] = [c["email"] for c in contacts]
        queue = rows_to_list(db.execute(text("SELECT * FROM send_queue WHERE campaign_email_id=:id"), {"id": email_id}).fetchall())
        row["queue"] = queue
        # Rollup de estado real (send_queue es la fuente de verdad; las
        # columnas legacy sent_at/send_status de campaign_emails ya no se
        # escriben directamente, se derivan acá para mantener compatibilidad).
        if queue:
            statuses = {q["status"] for q in queue}
            sent_ats = [q["sent_at"] for q in queue if q.get("sent_at")]
            if statuses == {"sent"}:
                row["send_status"] = "sent"
                row["sent_at"] = max(sent_ats) if sent_ats else None
            elif statuses <= {"cancelled"}:
                row["send_status"] = "cancelled"
            elif "failed" in statuses and not (statuses - {"failed", "cancelled"}):
                row["send_status"] = "failed"
            elif sent_ats:
                row["send_status"] = "sent"
                row["sent_at"] = max(sent_ats)
            else:
                row["send_status"] = "pending"
        return row
    finally:
        db.close()


def reschedule_email(email_id: int, new_date: str):
    if not new_date:
        raise ValueError("scheduled_at es requerido (formato: YYYY-MM-DD HH:MM)")
    db = get_db()
    try:
        em = dict_from_row(db.execute(text("SELECT id, sent_at FROM campaign_emails WHERE id=:id"), {"id": email_id}).fetchone())
        if not em:
            raise ValueError("Pieza no encontrada")
        if em.get("sent_at"):
            raise ValueError("No se puede reprogramar una pieza ya enviada")
        db.execute(text("UPDATE campaign_emails SET scheduled_at=:sched, send_status='pending', sent_at=NULL WHERE id=:id"), {"sched": new_date, "id": email_id})
        next_at = _parse_scheduled_at(new_date)
        db.execute(text("UPDATE send_queue SET next_attempt_at=:next_at, updated_at=NOW() WHERE campaign_email_id=:id AND status='pending'"), {"next_at": next_at, "id": email_id})
        db.commit()
    finally:
        db.close()


def cancel_email(email_id: int):
    db = get_db()
    try:
        em = dict_from_row(db.execute(text("SELECT id, sent_at FROM campaign_emails WHERE id=:id"), {"id": email_id}).fetchone())
        if not em:
            raise ValueError("Pieza no encontrada")
        if em.get("sent_at"):
            raise ValueError("No se puede cancelar una pieza ya enviada")
        db.execute(text("UPDATE campaign_emails SET status='rejected', send_status='cancelled' WHERE id=:id"), {"id": email_id})
        db.execute(text("UPDATE send_queue SET status='cancelled', updated_at=NOW() WHERE campaign_email_id=:id AND status IN ('pending','failed')"), {"id": email_id})
        db.commit()
    finally:
        db.close()


def pause_sequence(cid: int):
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT status FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise ValueError("Comunicación no encontrada")
        if camp["status"] != "scheduled":
            raise ValueError("Solo se puede pausar una comunicación programada")
        db.execute(text("UPDATE campaigns SET status='paused', paused_at=NOW() WHERE id=:id"), {"id": cid})
        db.commit()
    finally:
        db.close()


def resume_sequence(cid: int):
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT status FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise ValueError("Comunicación no encontrada")
        if camp["status"] != "paused":
            raise ValueError("Solo se puede reanudar una comunicación pausada")
        db.execute(text("UPDATE campaigns SET status='scheduled', paused_at=NULL WHERE id=:id"), {"id": cid})
        db.commit()
    finally:
        db.close()


def cancel_sequence(cid: int):
    """Cancelación blanda: conserva el registro en Supervisión pero no
    vuelve a intentar nada pendiente. Para borrar todo, ver delete_sequence."""
    db = get_db()
    try:
        db.execute(text("UPDATE campaigns SET status='cancelled' WHERE id=:id"), {"id": cid})
        db.execute(text("UPDATE send_queue SET status='cancelled', updated_at=NOW() WHERE campaign_id=:id AND status IN ('pending','failed')"), {"id": cid})
        db.commit()
    finally:
        db.close()


def retry_failed(campaign_id: int = None, queue_id: int = None) -> dict:
    """Reencola manualmente ítems que agotaron sus reintentos automáticos."""
    db = get_db()
    try:
        if queue_id:
            result = db.execute(text("UPDATE send_queue SET status='pending', next_attempt_at=NOW(), updated_at=NOW() WHERE id=:id AND status='failed'"), {"id": queue_id})
        elif campaign_id:
            result = db.execute(text("UPDATE send_queue SET status='pending', next_attempt_at=NOW(), updated_at=NOW() WHERE campaign_id=:cid AND status='failed'"), {"cid": campaign_id})
        else:
            raise ValueError("Se requiere campaign_id o queue_id")
        db.commit()
        return {"requeued": result.rowcount}
    finally:
        db.close()


def send_now(cid: int) -> dict:
    """Fuerza el envío inmediato de una comunicación, sin esperar al próximo
    ciclo del Cron Sender."""
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise ValueError("Comunicación no encontrada")
        if camp["status"] not in ("scheduled", "paused", "draft"):
            raise ValueError("Solo se puede forzar el envío de comunicaciones en borrador, programadas o pausadas")
        approved = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:cid AND status='approved'"), {"cid": cid}).fetchone())
        if not approved or approved["n"] == 0:
            raise ValueError("No hay piezas aprobadas para enviar")
        contacts = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM campaign_contacts WHERE campaign_id=:cid"), {"cid": cid}).fetchone())
        if not contacts or contacts["n"] == 0:
            raise ValueError("No hay contactos en la comunicación")
        db.execute(text("UPDATE campaigns SET status='scheduled' WHERE id=:id"), {"id": cid})
        db.commit()
    finally:
        db.close()

    _populate_queue(cid)

    db = get_db()
    try:
        db.execute(text("UPDATE send_queue SET next_attempt_at=NOW() WHERE campaign_id=:cid AND status='pending'"), {"cid": cid})
        db.commit()
    finally:
        db.close()

    return process_due(campaign_id=cid)


def force_send_emails(email_ids: list) -> dict:
    """Fuerza el envío inmediato de piezas puntuales (por lo general
    demoradas): marca sus ítems `pending` de la cola como vencidos YA y
    corre el Cron Sender sobre esas campañas."""
    if not email_ids:
        raise ValueError("Se requiere al menos un email_id")
    db = get_db()
    campaign_ids = set()
    try:
        for eid in email_ids:
            row = dict_from_row(db.execute(text("SELECT campaign_id FROM campaign_emails WHERE id=:id"), {"id": eid}).fetchone())
            if not row:
                continue
            campaign_ids.add(row["campaign_id"])
            db.execute(text("UPDATE send_queue SET next_attempt_at=NOW(), updated_at=NOW() WHERE campaign_email_id=:id AND status='pending'"), {"id": eid})
        db.commit()
    finally:
        db.close()

    total = {"sent": 0, "failed": 0, "retried": 0, "details": []}
    for cid in campaign_ids:
        r = process_due(campaign_id=cid)
        total["sent"] += r["sent"]
        total["failed"] += r["failed"]
        total["retried"] += r["retried"]
        total["details"] += r["details"]
    return total


def process_due(campaign_id: int = None) -> dict:
    """El corazón del Cron Sender: procesa `send_queue`, respetando pausa
    (campañas no 'scheduled' se saltan), límite por corrida
    (`rate_limit_per_run`) y reintentos con backoff creciente."""
    now_dt = datetime.utcnow()
    db = get_db()
    results = {"sent": 0, "failed": 0, "retried": 0, "details": []}
    try:
        camp_filter = "AND c.id = :cid" if campaign_id else ""
        params = {"now": now_dt}
        if campaign_id:
            params["cid"] = campaign_id

        due = rows_to_list(db.execute(text(f"""
            SELECT sq.*, ce.subject, ce.body, ce.content_type, c.cta_url, c.rate_limit_per_run, c.name as camp_name
            FROM send_queue sq
            JOIN campaign_emails ce ON ce.id = sq.campaign_email_id
            JOIN campaigns c ON c.id = sq.campaign_id
            WHERE sq.status = 'pending' AND sq.next_attempt_at <= :now
              AND c.status = 'scheduled'
              {camp_filter}
            ORDER BY sq.next_attempt_at ASC
        """), params).fetchall())

        if not due:
            return results

        per_campaign_count = {}
        batch = []
        for row in due:
            cnt = per_campaign_count.get(row["campaign_id"], 0)
            limit = row.get("rate_limit_per_run") or DEFAULT_RATE_LIMIT_PER_RUN
            if cnt >= limit:
                continue
            per_campaign_count[row["campaign_id"]] = cnt + 1
            batch.append(row)

        style = build_style()
        attachments_cache = {}

        for row in batch:
            cid = row["campaign_id"]
            if cid not in attachments_cache:
                att_rows = rows_to_list(db.execute(text("SELECT attachment_id FROM campaign_attachments WHERE campaign_id=:cid"), {"cid": cid}).fetchall())
                attachments_cache[cid] = resolve_attachments(db, [a["attachment_id"] for a in att_rows])

            to = row["recipient_email"]
            try:
                contact = dict_from_row(db.execute(text("SELECT * FROM contacts WHERE email=:e"), {"e": to}).fetchone())
                context = build_context(contact, sender_name=style.get("sender_name"), cta_url=row.get("cta_url") or "")
                subject, _ = render(row["subject"] or "", context)
                body, _ = render(row["body"] or "", context)
                content_type = row.get("content_type") or "markdown"
                html = assemble(body, content_type, style)
                body_plain = html_to_text(html) if content_type == "html" else body

                resp = resend_provider.send(to, subject, body_plain, html, attachments=attachments_cache[cid])
                email_id = resp.id if hasattr(resp, "id") else None

                db.execute(text("UPDATE send_queue SET status='sent', sent_at=NOW(), attempts=attempts+1, last_error=NULL, updated_at=NOW() WHERE id=:id"), {"id": row["id"]})
                _log_sent_from_queue(db, to, subject, body_plain, html, cid, email_id)
                results["sent"] += 1
                results["details"].append({"to": to, "campaign": row["camp_name"], "ok": True})
            except Exception as e:
                attempts = row["attempts"] + 1
                if attempts >= row["max_attempts"]:
                    db.execute(text("UPDATE send_queue SET status='failed', attempts=:a, last_error=:err, updated_at=NOW() WHERE id=:id"),
                               {"a": attempts, "err": str(e)[:500], "id": row["id"]})
                    results["failed"] += 1
                else:
                    next_at = now_dt + timedelta(minutes=DEFAULT_RETRY_BACKOFF_MINUTES * attempts)
                    db.execute(text("UPDATE send_queue SET attempts=:a, last_error=:err, next_attempt_at=:next_at, updated_at=NOW() WHERE id=:id"),
                               {"a": attempts, "err": str(e)[:500], "next_at": next_at, "id": row["id"]})
                    results["retried"] += 1
                results["details"].append({"to": to, "campaign": row["camp_name"], "ok": False, "error": str(e)})
                logger.error(f"[cron-sender] Error enviando a {to} (comunicación {cid}): {e}")

        db.commit()

        camp_ids = {row["campaign_id"] for row in batch}
        for cid in camp_ids:
            remaining = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM send_queue WHERE campaign_id=:cid AND status='pending'"), {"cid": cid}).fetchone())
            if remaining and remaining["n"] == 0:
                db.execute(text("UPDATE campaigns SET status='sent' WHERE id=:id AND status='scheduled'"), {"id": cid})
        db.commit()
    except Exception as e:
        logger.error(f"[cron-sender] Error en process_due: {e}")
        db.rollback()
    finally:
        db.close()

    logger.info(f"[cron-sender] {results['sent']} enviados, {results['failed']} fallidos definitivos, {results['retried']} reprogramados para reintento")
    return results

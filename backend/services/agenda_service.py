"""
services/agenda_service.py — Agenda de destinatarios.

Reemplaza el puente SQLite (`leadforge.db`) del proyecto original: LeadForge,
Apollo o cualquier otra fuente de audiencia externa entregan una lista
(CSV o JSON) que se importa acá por UPSERT. El Sender no se conecta a esas
herramientas ni lee sus bases directamente — son fuentes externas, no
módulos internos.
"""
import logging
from datetime import datetime

from sqlalchemy import text

from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain.contacts import parse_csv_rows

logger = logging.getLogger(__name__)


def list_contacts() -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(text("SELECT * FROM contacts ORDER BY name")).fetchall())
    finally:
        db.close()


def create_contact(data: dict) -> int:
    db = get_db()
    try:
        result = db.execute(
            text("""INSERT INTO contacts
                    (name,email,company,role,phone,context,tags,
                     status,tipo,medio,last_contact,next_followup,notes,source)
                    VALUES (:name,:email,:company,:role,:phone,:context,:tags,
                            :status,:tipo,:medio,:last_contact,:next_followup,:notes,'manual')
                    RETURNING id"""),
            {
                "name": data["name"], "email": data["email"], "company": data.get("company", ""),
                "role": data.get("role", ""), "phone": data.get("phone", ""),
                "context": data.get("context", ""), "tags": data.get("tags", ""),
                "status": data.get("status", "nuevo"), "tipo": data.get("tipo", "prensa"),
                "medio": data.get("medio", ""), "last_contact": data.get("last_contact", ""),
                "next_followup": data.get("next_followup", ""), "notes": data.get("notes", ""),
            },
        )
        cid = dict_from_row(result.fetchone())["id"]
        db.execute(
            text("INSERT INTO memory (type,entity,content) VALUES (:type,:entity,:content)"),
            {"type": "contact_added", "entity": data["email"], "content": f"Contacto: {data['name']} - {data.get('company','')}"},
        )
        db.commit()
        return cid
    except Exception as e:
        db.rollback()
        if "duplicate key" in str(e).lower():
            raise ValueError("Email ya existe")
        raise
    finally:
        db.close()


def update_contact(cid: int, data: dict):
    db = get_db()
    try:
        db.execute(
            text("""UPDATE contacts SET
                    name=:name,company=:company,role=:role,phone=:phone,
                    context=:context,tags=:tags,
                    status=COALESCE(:status,status),
                    tipo=COALESCE(:tipo,tipo),
                    medio=COALESCE(:medio,medio),
                    last_contact=COALESCE(:last_contact,last_contact),
                    next_followup=COALESCE(:next_followup,next_followup),
                    notes=COALESCE(:notes,notes),
                    updated_at=NOW() WHERE id=:cid"""),
            {
                "name": data.get("name"), "company": data.get("company", ""), "role": data.get("role", ""),
                "phone": data.get("phone", ""), "context": data.get("context", ""), "tags": data.get("tags", ""),
                "status": data.get("status"), "tipo": data.get("tipo"), "medio": data.get("medio"),
                "last_contact": data.get("last_contact"), "next_followup": data.get("next_followup"),
                "notes": data.get("notes"), "cid": cid,
            },
        )
        db.commit()
    finally:
        db.close()


def delete_contact(cid: int):
    db = get_db()
    try:
        db.execute(text("DELETE FROM contacts WHERE id=:cid"), {"cid": cid})
        db.commit()
    finally:
        db.close()


def update_contact_tag_status(cid: int, status: str):
    """Metadato opcional de segmentación (no es un pipeline de ventas)."""
    db = get_db()
    try:
        db.execute(text("UPDATE contacts SET status=:status,updated_at=NOW() WHERE id=:cid"), {"status": status, "cid": cid})
        db.commit()
    finally:
        db.close()


def list_interactions(cid: int) -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(
            text("SELECT * FROM contact_interactions WHERE contact_id=:cid ORDER BY date DESC"), {"cid": cid}
        ).fetchall())
    finally:
        db.close()


def create_interaction(cid: int, data: dict):
    db = get_db()
    try:
        db.execute(
            text("INSERT INTO contact_interactions (contact_id,type,note) VALUES (:cid,:type,:note)"),
            {"cid": cid, "type": data.get("type", ""), "note": data.get("note", "")},
        )
        db.execute(text("UPDATE contacts SET last_contact=NOW()::date::text,updated_at=NOW() WHERE id=:cid"), {"cid": cid})
        db.commit()
    finally:
        db.close()


def get_sent_history(contact_id: int) -> list:
    db = get_db()
    try:
        contact = dict_from_row(db.execute(text("SELECT email FROM contacts WHERE id=:id"), {"id": contact_id}).fetchone())
        if not contact:
            raise ValueError("Contacto no encontrado")
        return rows_to_list(db.execute(text("""
            SELECT subject, body, sent_at, intent FROM email_logs
            WHERE contact_email=:email AND direction='out' ORDER BY sent_at DESC LIMIT 5
        """), {"email": contact["email"]}).fetchall())
    finally:
        db.close()


def sender_status_by_email() -> dict:
    """Cruza destinatarios de campañas (Cron Sender) con su estado, para
    mostrar en la Agenda un badge 'en envío' — sin convertir esto en un
    pipeline de ventas."""
    db = get_db()
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    priority = {"delayed": 0, "scheduled": 1, "sent": 2, "cancelled": 3}
    try:
        rows = rows_to_list(db.execute(text("""
            SELECT c.id AS campaign_id, c.name AS camp_name,
                   ce.scheduled_at, ce.sent_at, ce.send_status, ce.status AS approval_status
            FROM campaigns c
            JOIN campaign_emails ce ON ce.campaign_id = c.id
            WHERE c.status IN ('scheduled', 'sent')
              AND ce.status IN ('approved', 'rejected')
        """)).fetchall())

        camp_state = {}
        for r in rows:
            raw_sched = r.get("scheduled_at") or ""
            sched = str(raw_sched).strip() if raw_sched else ""
            if sched and len(sched) == 10:
                sched = sched + " 09:00"
            sent_at = r.get("sent_at")
            send_status = r.get("send_status") or ""
            approval_status = r.get("approval_status") or ""

            if send_status == "cancelled" or approval_status == "rejected":
                state = "cancelled"
            elif sent_at or send_status == "sent":
                state = "sent"
            elif sched and sched[:16] <= now_str:
                state = "delayed"
            else:
                state = "scheduled"

            cid = r["campaign_id"]
            prev = camp_state.get(cid)
            if prev is None or priority[state] < priority[prev["state"]]:
                camp_state[cid] = {"name": r["camp_name"], "state": state}

        recipients = rows_to_list(db.execute(text("SELECT campaign_id, email FROM campaign_contacts")).fetchall())
        result = {}
        for rec in recipients:
            cid = rec["campaign_id"]
            email = (rec.get("email") or "").strip()
            cs = camp_state.get(cid)
            if not email or cs is None:
                continue
            result.setdefault(email, []).append({"campaign_id": cid, "name": cs["name"], "state": cs["state"]})
        return result
    finally:
        db.close()


def import_audience(rows: list, source: str = "import-csv") -> dict:
    """Importa una lista de contactos (de LeadForge, Apollo, un CSV manual,
    etc.) por UPSERT en email, preservando lo que el usuario ya haya editado
    en la Agenda (company/phone/tags se actualizan, el resto se conserva)."""
    valid, parse_errors = parse_csv_rows(rows)
    if not valid:
        return {"imported": 0, "updated": 0, "total": 0, "errors": parse_errors}

    db = get_db()
    imported = updated = 0
    try:
        for row in valid:
            res = db.execute(
                text("""
                    INSERT INTO contacts (name, email, company, role, phone, tags, status, tipo, source)
                    VALUES (:name, :email, :company, :role, :phone, :tags, 'nuevo', 'lead', :source)
                    ON CONFLICT (email) DO UPDATE SET
                        company = EXCLUDED.company,
                        phone   = COALESCE(NULLIF(EXCLUDED.phone, ''), contacts.phone),
                        tags    = EXCLUDED.tags,
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted
                """),
                {**row, "source": source},
            )
            was_insert = dict_from_row(res.fetchone())["inserted"]
            if was_insert:
                imported += 1
            else:
                updated += 1
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"[agenda] Error importando audiencia: {e}")
        raise
    finally:
        db.close()

    logger.info(f"[agenda] Import ({source}): +{imported} nuevos, {updated} actualizados")
    return {"imported": imported, "updated": updated, "total": imported + updated, "errors": parse_errors}

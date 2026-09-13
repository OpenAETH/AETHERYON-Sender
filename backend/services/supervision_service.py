"""
services/supervision_service.py — Supervisión: torre de control operacional.

Unifica lo enviado a mano (Redacción) y lo enviado por el Cron Sender:
todo pasa por `email_logs`, así que Supervisión muestra un solo historial
con detección de respuestas por hilo. `get_control_tower()` agrega además
el estado en tiempo real de la cola (`send_queue`) y de los proveedores.
"""
import logging
from datetime import datetime

from sqlalchemy import text

from backend.config import cfg
from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain.supervision import build_reply_index, has_reply
from backend.providers import resend_provider, storage_provider
from backend.providers.imap_provider import is_configured as imap_configured
from backend.services.inbox_service import sync as sync_inbox
from backend.services.settings_service import get_setting

logger = logging.getLogger(__name__)


def get_supervision(refresh: bool = True) -> list:
    if refresh:
        try:
            sync_inbox(60)
        except Exception as e:
            logger.error(f"Supervision IMAP sync error: {e}")

    db = get_db()
    try:
        sent = db.execute(text("""
            SELECT l.*, c.company FROM email_logs l
            LEFT JOIN contacts c ON l.contact_email=c.email
            WHERE l.direction='out' ORDER BY l.sent_at DESC LIMIT 200
        """)).fetchall()

        inbox_rows = rows_to_list(db.execute(text("SELECT from_email, subject FROM inbox_cache")).fetchall())
        reply_index = build_reply_index(inbox_rows)

        result = []
        for row in sent:
            d = dict_from_row(row)
            try:
                sent_at_val = d.get("sent_at")
                if sent_at_val:
                    dt = datetime.fromisoformat(sent_at_val) if isinstance(sent_at_val, str) else sent_at_val.replace(tzinfo=None)
                    hrs = round((datetime.utcnow() - dt).total_seconds() / 3600, 1)
                else:
                    hrs = None
            except Exception:
                hrs = None
            d["hours_since_sent"] = hrs
            d["has_reply"] = has_reply(d.get("contact_email"), d.get("subject"), reply_index)
            result.append(d)
        return result
    finally:
        db.close()


def get_stats() -> dict:
    db = get_db()
    try:
        contacts = db.execute(text("SELECT COUNT(*) FROM contacts")).fetchone()[0]
        sent = db.execute(text("SELECT COUNT(*) FROM email_logs WHERE direction='out'")).fetchone()[0]
        inbox = db.execute(text("SELECT COUNT(*) FROM inbox_cache")).fetchone()[0]
        replied = db.execute(text("SELECT COUNT(*) FROM inbox_cache WHERE replied=1")).fetchone()[0]
        memory = db.execute(text("SELECT COUNT(*) FROM memory")).fetchone()[0]
        return {"contacts": contacts, "sent": sent, "inbox": inbox, "replied": replied, "memory": memory}
    finally:
        db.close()


def get_control_tower() -> dict:
    """Panorama operativo del Cron Sender: qué hay enviado, programado,
    pendiente, fallido, reintentando y cancelado ahora mismo, más el estado
    de los proveedores conectados."""
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("""
            SELECT
                SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN status='pending' AND attempts=0 THEN 1 ELSE 0 END) AS scheduled,
                SUM(CASE WHEN status='pending' AND attempts>0 THEN 1 ELSE 0 END) AS retrying,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                COUNT(*) AS total
            FROM send_queue
        """)).fetchone()) or {}
        active_campaigns = dict_from_row(db.execute(text(
            "SELECT COUNT(*) as n FROM campaigns WHERE status='scheduled'"
        )).fetchone())["n"]
        paused_campaigns = dict_from_row(db.execute(text(
            "SELECT COUNT(*) as n FROM campaigns WHERE status='paused'"
        )).fetchone())["n"]
    finally:
        db.close()

    c = cfg()
    return {
        "queue": {
            "sent": row.get("sent") or 0,
            "scheduled": row.get("scheduled") or 0,
            "retrying": row.get("retrying") or 0,
            "failed": row.get("failed") or 0,
            "cancelled": row.get("cancelled") or 0,
            "total": row.get("total") or 0,
        },
        "active_sequences": active_campaigns,
        "paused_sequences": paused_campaigns,
        "system": {
            "resend_configured": resend_provider.is_configured(),
            "storage_configured": storage_provider.is_configured(),
            "imap_configured": imap_configured(),
            "groq_configured": bool(c["groq_api_key"]),
            "scheduler_last_run": get_setting("scheduler_last_run", ""),
        },
    }


def get_logs(limit: int = 100) -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(text("SELECT * FROM email_logs ORDER BY created_at DESC LIMIT :limit"), {"limit": limit}).fetchall())
    finally:
        db.close()


def get_log(log_id: int) -> dict:
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("SELECT * FROM email_logs WHERE id=:id"), {"id": log_id}).fetchone())
        if not row:
            raise ValueError("Log no encontrado")
        return row
    finally:
        db.close()


def get_memory(limit: int = 100) -> list:
    db = get_db()
    try:
        return rows_to_list(db.execute(text("SELECT * FROM memory ORDER BY created_at DESC LIMIT :limit"), {"limit": limit}).fetchall())
    finally:
        db.close()


def add_memory(data: dict):
    db = get_db()
    try:
        db.execute(
            text("INSERT INTO memory (type,entity,content,importance) VALUES (:type,:entity,:content,:importance)"),
            {"type": data.get("type", "manual"), "entity": data.get("entity", ""), "content": data.get("content", ""), "importance": data.get("importance", 1)},
        )
        db.commit()
    finally:
        db.close()

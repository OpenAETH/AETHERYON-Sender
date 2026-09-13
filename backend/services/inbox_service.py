"""
services/inbox_service.py — Bandeja de entrada (IMAP), componente secundario.

Sync delete-aware: agrega mensajes nuevos del servidor y borra de
`inbox_cache` los que ya no están (alguien los borró en el buzón real).
"""
import logging

from sqlalchemy import text

from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.providers.imap_provider import ImapSession, is_configured

logger = logging.getLogger(__name__)


def sync(limit: int = 500) -> dict:
    if not is_configured():
        return {"added": 0, "deleted": 0, "error": "IMAP no configurado"}
    try:
        with ImapSession() as session:
            server_uids = session.list_uids()

            db = get_db()
            try:
                db_rows = rows_to_list(db.execute(text("SELECT message_id, imap_uid FROM inbox_cache")).fetchall())
                db_by_uid = {r["imap_uid"]: r["message_id"] for r in db_rows if r["imap_uid"]}
                db_message_ids = {r["message_id"] for r in db_rows}

                deleted_uids = set(db_by_uid.keys()) - server_uids
                deleted_count = 0
                for uid in deleted_uids:
                    db.execute(text("DELETE FROM inbox_cache WHERE message_id=:mid"), {"mid": db_by_uid[uid]})
                    deleted_count += 1
                if deleted_count:
                    db.commit()
                    logger.info(f"IMAP sync: {deleted_count} mensajes eliminados de DB")

                uids_to_fetch = sorted(server_uids, key=lambda u: int(u))[-limit:]
                added_count = 0
                for uid in reversed(uids_to_fetch):
                    msg = session.fetch_message(uid)
                    if not msg or msg["message_id"] in db_message_ids:
                        continue
                    try:
                        db.execute(text("""
                            INSERT INTO inbox_cache (message_id,imap_uid,from_email,from_name,subject,body,date,in_reply_to)
                            VALUES (:message_id,:imap_uid,:from_email,:from_name,:subject,:body,:date,:in_reply_to)
                            ON CONFLICT (message_id) DO NOTHING
                        """), msg)
                        db.commit()
                        added_count += 1
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Error insertando msg {msg['message_id']}: {e}")
            finally:
                db.close()

        logger.info(f"IMAP sync: +{added_count} nuevos, -{deleted_count} eliminados")
        return {"added": added_count, "deleted": deleted_count, "total": len(server_uids)}
    except Exception as e:
        logger.error(f"IMAP sync error: {e}")
        return {"added": 0, "deleted": 0, "error": str(e)}


def list_messages(refresh: bool = False) -> dict:
    sync_result = sync(500) if refresh else None
    db = get_db()
    try:
        rows = db.execute(text("SELECT * FROM inbox_cache ORDER BY date DESC LIMIT 500")).fetchall()
        return {"messages": rows_to_list(rows), "sync": sync_result}
    finally:
        db.close()


def mark_replied(message_id: str):
    db = get_db()
    try:
        db.execute(text("UPDATE inbox_cache SET replied=1 WHERE message_id=:mid"), {"mid": message_id})
        db.commit()
    finally:
        db.close()


def delete_message(message_id: str):
    db = get_db()
    try:
        db.execute(text("DELETE FROM inbox_cache WHERE message_id=:mid"), {"mid": message_id})
        db.commit()
    finally:
        db.close()


def save_suggestion(message_id: str, suggestion: str):
    db = get_db()
    try:
        db.execute(text("UPDATE inbox_cache SET ai_suggestion=:s WHERE message_id=:mid"), {"s": suggestion, "mid": message_id})
        db.commit()
    finally:
        db.close()

"""
providers/imap_provider.py — Mecánica IMAP pura (sin tocar la base de datos).

services/inbox_service.py usa este provider para traer el estado actual del
servidor y hace el diff contra `inbox_cache` (qué agregar, qué borrar).
"""
import imaplib
import email as email_lib
import logging
from email.header import decode_header

from backend.config import cfg

logger = logging.getLogger(__name__)


def is_configured() -> bool:
    c = cfg()
    return bool(c["imap_host"] and c["imap_user"])


def decode_str(s):
    if not s:
        return ""
    parts = decode_header(s)
    result = []
    for part, enc in parts:
        result.append(part.decode(enc or "utf-8", errors="replace") if isinstance(part, bytes) else str(part))
    return "".join(result)


class ImapSession:
    """Context manager delgado sobre imaplib para agrupar login/logout."""

    def __enter__(self):
        c = cfg()
        if not is_configured():
            raise RuntimeError("IMAP no configurado")
        self.mail = imaplib.IMAP4_SSL(c["imap_host"], c["imap_port"])
        self.mail.login(c["imap_user"], c["imap_pass"])
        self.mail.select("INBOX")
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.mail.logout()
        except Exception:
            pass

    def list_uids(self) -> set:
        _, uid_data = self.mail.uid("SEARCH", None, "ALL")
        return set(uid_data[0].decode().split()) if uid_data[0] else set()

    def fetch_message(self, uid) -> dict | None:
        """Trae y parsea un mensaje por UID. None si no se pudo traer."""
        _, msg_data = self.mail.uid("FETCH", uid, "(RFC822)")
        if not msg_data or not msg_data[0]:
            return None
        raw = msg_data[0][1]
        msg = email_lib.message_from_bytes(raw)
        mid = msg.get("Message-ID", "").strip() or f"uid-{uid}"

        subj = decode_str(msg.get("Subject", ""))
        from_ = decode_str(msg.get("From", ""))
        date_ = msg.get("Date", "")
        in_reply_to = (msg.get("In-Reply-To", "") or "").strip()

        if "<" in from_:
            pts = from_.split("<")
            from_name = pts[0].strip().strip('"')
            from_email = pts[1].replace(">", "").strip()
        else:
            from_email = from_.strip()
            from_name = from_email.split("@")[0]

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                        break
                    except Exception:
                        pass
        else:
            try:
                body = msg.get_payload(decode=True).decode("utf-8", errors="replace")
            except Exception:
                body = ""

        return {
            "message_id": mid,
            "imap_uid": str(uid),
            "from_email": from_email,
            "from_name": from_name,
            "subject": subj,
            "body": body[:3000],
            "date": date_,
            "in_reply_to": in_reply_to,
        }

"""
services/settings_service.py — Configuración persistida en la tabla `settings`
(estilo visual del email, firma HTML, contexto de entidad/misión).
"""
import logging

from sqlalchemy import text

from backend.config import cfg
from backend.db.connection import get_db, dict_from_row

logger = logging.getLogger(__name__)


def get_setting(key: str, default: str = "") -> str:
    try:
        db = get_db()
        try:
            row = dict_from_row(db.execute(text("SELECT value FROM settings WHERE key=:key"), {"key": key}).fetchone())
            return row["value"] if row else default
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error en get_setting: {e}")
        return default


def set_setting(key: str, value: str):
    db = get_db()
    try:
        db.execute(
            text("INSERT INTO settings (key,value) VALUES (:key,:value) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value"),
            {"key": key, "value": value},
        )
        db.commit()
    finally:
        db.close()


def get_all_settings() -> dict:
    db = get_db()
    try:
        rows = db.execute(text("SELECT key,value FROM settings")).fetchall()
        return {dict_from_row(r)["key"]: dict_from_row(r)["value"] for r in rows}
    finally:
        db.close()


def save_settings(data: dict):
    for k, v in data.items():
        set_setting(k, str(v))


def build_style() -> dict:
    """Estilo completo (colores, fuente, firma, remitente) resuelto desde
    settings + env, listo para pasar a domain.email_content.build_html_email."""
    c = cfg()
    return {
        "primary_color": get_setting("style_primary_color", "#178a7e"),
        "bg_color": get_setting("style_bg_color", "#ffffff"),
        "text_color": get_setting("style_text_color", "#1a1a1a"),
        "font_family": get_setting("style_font_family", "Georgia,'Times New Roman',serif"),
        "font_size": get_setting("style_font_size", "16px"),
        "link_color": get_setting("style_link_color", "#2563eb"),
        "header_bg": get_setting("style_header_bg", "#0a0f14"),
        "header_color": get_setting("style_header_color", "#4dd8c4"),
        "sender_name": c["sender_name"],
        "signature_html": get_setting("signature_html", ""),
    }


def get_context() -> dict:
    return {
        "entity": get_setting("ctx_entity"),
        "mission": get_setting("ctx_mission"),
        "extra": get_setting("ctx_extra"),
    }


def save_context(data: dict):
    for k in ("entity", "mission", "extra"):
        if k in data:
            set_setting("ctx_" + k, data[k])


def get_sender_config() -> dict:
    """Estado de configuración de proveedores, para /config y /api/status."""
    c = cfg()
    return {
        "provider": "resend",
        "sender_email": c["sender_email"],
        "sender_name": c["sender_name"],
        "resend_key_set": bool(c["resend_api_key"]),
        "imap_host": c["imap_host"],
        "imap_port": c["imap_port"],
        "imap_user": c["imap_user"],
        "imap_pass_set": bool(c["imap_pass"]),
    }

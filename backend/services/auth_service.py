"""
services/auth_service.py — Sesiones (token HMAC + tabla `sessions`).
Idéntico en comportamiento al Emailer-Agent original.
"""
import hashlib
import hmac
import logging
import secrets
import time

from sqlalchemy import text

from backend.config import SECRET_KEY, TOKEN_TTL, APP_USER, APP_PASSWORD
from backend.db.connection import get_db

logger = logging.getLogger(__name__)


def make_token() -> str:
    raw = secrets.token_hex(32)
    sig = hmac.new(SECRET_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return f"{raw}.{sig}"


def verify_token(token: str) -> bool:
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac.new(SECRET_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        db = get_db()
        try:
            result = db.execute(
                text("SELECT expires_at FROM sessions WHERE token=:token AND expires_at>:now"),
                {"token": token, "now": int(time.time())},
            ).fetchone()
            return result is not None
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error en verify_token: {e}")
        return False


def check_credentials(username: str, password: str) -> bool:
    return username == APP_USER and password == APP_PASSWORD


def create_session() -> tuple:
    """Crea una sesión nueva. Devuelve (token, expires_at_epoch)."""
    token = make_token()
    now = int(time.time())
    db = get_db()
    try:
        db.execute(text("DELETE FROM sessions WHERE expires_at<:now"), {"now": now})
        db.execute(
            text("INSERT INTO sessions (token,created_at,expires_at) VALUES (:token,:created_at,:expires_at)"),
            {"token": token, "created_at": now, "expires_at": now + TOKEN_TTL},
        )
        db.commit()
    finally:
        db.close()
    return token, now + TOKEN_TTL


def destroy_session(token: str):
    if not token:
        return
    db = get_db()
    try:
        db.execute(text("DELETE FROM sessions WHERE token=:token"), {"token": token})
        db.commit()
    finally:
        db.close()

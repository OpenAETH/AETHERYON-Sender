"""
config.py — Configuración centralizada desde variables de entorno (Render Dashboard
o .env local). Único lugar donde se lee os.getenv() para credenciales de
proveedores; providers/ y services/ importan de acá en vez de leer el entorno
directamente.
"""
import os
import secrets
import logging

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Raíz del repo (padre del paquete backend/), para resolver index.html y static/.
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Auth ────────────────────────────────────────────────────
APP_USER = os.getenv("APP_USER", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    logger.warning(
        "SECRET_KEY no configurada en entorno — usando valor aleatorio efímero. "
        "Las sesiones no sobrevivirán reinicios. Configurá SECRET_KEY en Render."
    )
TOKEN_TTL = int(os.getenv("TOKEN_TTL_HOURS", "8")) * 3600

# True en Render (HTTPS) o si se fuerza con COOKIE_SECURE=true
IS_HTTPS = os.getenv("RENDER", "") != "" or os.getenv("COOKIE_SECURE", "").lower() == "true"


def cfg() -> dict:
    """Config de proveedores externos, releída en cada llamada (permite cambios
    de env sin reiniciar en algunos entornos de desarrollo)."""
    return {
        # Resend
        "resend_api_key": os.getenv("RESEND_API_KEY", ""),
        "sender_email": os.getenv("SENDER_EMAIL", os.getenv("SMTP_USER", "")),
        "sender_name": os.getenv("SENDER_NAME", ""),
        # IMAP
        "imap_host": os.getenv("IMAP_HOST", ""),
        "imap_port": int(os.getenv("IMAP_PORT", "993")),
        "imap_user": os.getenv("IMAP_USER", ""),
        "imap_pass": os.getenv("IMAP_PASS", ""),
        # Supabase Storage
        "supabase_url": os.getenv("SUPABASE_URL", ""),
        "supabase_service_key": os.getenv("SUPABASE_SERVICE_KEY", ""),
        "storage_bucket": os.getenv("STORAGE_BUCKET", "email_attachments"),
        # Groq (asistencia de redacción, opcional)
        "groq_api_key": os.getenv("GROQ_API_KEY", ""),
    }


# ── Cron Sender: límites por defecto (ajustables desde Configuración) ──
DEFAULT_RATE_LIMIT_PER_RUN = int(os.getenv("SENDER_RATE_LIMIT_PER_RUN", "50"))
DEFAULT_MAX_ATTEMPTS = int(os.getenv("SENDER_MAX_ATTEMPTS", "3"))
DEFAULT_RETRY_BACKOFF_MINUTES = int(os.getenv("SENDER_RETRY_BACKOFF_MINUTES", "30"))
SCHEDULER_INTERVAL_SECONDS = int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "60"))

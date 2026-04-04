"""
Asistente Ejecutivo — Deploy Render
Auth: JWT session  |  IMAP sync (delete-aware)  |  Envio multiple
Envio: Resend API (resend.com)
Database: SQLAlchemy + IPv4 forced connection for Supabase
"""
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import os, imaplib, email as email_lib
import re as _re
import socket as _socket
import psycopg2
import psycopg2.extras
from email.header import decode_header
from datetime import datetime, timedelta
import uvicorn, logging, re, secrets, hashlib, hmac, json, time
import asyncio
import resend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG desde ENV (Render Dashboard)
# ─────────────────────────────────────────────
def cfg():
    return {
        # Resend
        "resend_api_key": os.getenv("RESEND_API_KEY", ""),
        "sender_email":   os.getenv("SENDER_EMAIL", os.getenv("SMTP_USER", "")),
        "sender_name":    os.getenv("SENDER_NAME", ""),
        # IMAP
        "imap_host":      os.getenv("IMAP_HOST", ""),
        "imap_port":      int(os.getenv("IMAP_PORT", "993")),
        "imap_user":      os.getenv("IMAP_USER", ""),
        "imap_pass":      os.getenv("IMAP_PASS", ""),
    }

APP_USER     = os.getenv("APP_USER", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin")
SECRET_KEY   = os.getenv("SECRET_KEY", "")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    logger.warning("SECRET_KEY no configurada en entorno — usando valor aleatorio efímero. "
                   "Las sesiones no sobrevivirán reinicios. Configura SECRET_KEY en Render.")
TOKEN_TTL    = int(os.getenv("TOKEN_TTL_HOURS", "8")) * 3600

# True cuando corre en Render (HTTPS) o cuando se fuerza con COOKIE_SECURE=true
IS_HTTPS = os.getenv("RENDER", "") != "" or os.getenv("COOKIE_SECURE", "").lower() == "true"

BASE         = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:[YOUR-PASSWORD]@db.nruezthkvlvlochtnqjw.supabase.co:5432/postgres'
)


# ─────────────────────────────────────────────
# DATABASE — SQLAlchemy con IPv4 forzado para Supabase
# ─────────────────────────────────────────────
def _parse_db_url(url: str) -> dict:
    """Descompone DATABASE_URL en sus partes."""
    m = _re.match(
        r'postgresql://([^:]+):([^@]+)@([^:/]+):?(\d+)?/(.+)',
        url
    )
    if not m:
        raise ValueError(f"DATABASE_URL con formato inválido: {url!r}")
    return {
        'user':     m.group(1),
        'password': m.group(2),
        'host':     m.group(3),
        'port':     int(m.group(4) or 5432),
        'dbname':   m.group(5).split('?')[0],
    }

def _make_ipv4_connection():
    """
    Crea una conexión psycopg2 forzando IPv4.
    - 'host' lleva el hostname real → psycopg2/libpq lo usa como SNI en TLS
      (Supabase necesita el SNI para identificar el tenant)
    - 'hostaddr' lleva la IP IPv4 resuelta → libpq conecta a esa IP directamente,
      sin hacer DNS lookup (que podría devolver IPv6)
    Combinando ambos: conexión por IPv4 + SNI correcto = autenticación exitosa.
    NOTA: Sin cursor_factory para compatibilidad con SQLAlchemy.
    """
    p = _parse_db_url(DATABASE_URL)

    try:
        # Resolver forzando AF_INET para obtener la IPv4
        addrinfo = _socket.getaddrinfo(
            p['host'],
            p['port'],
            _socket.AF_INET,
            _socket.SOCK_STREAM
        )

        if not addrinfo:
            raise RuntimeError(f"No se pudo resolver {p['host']} a IPv4")

        ipv4 = addrinfo[0][4][0]

        logger.info(f"Resolviendo {p['host']} a IPv4: {ipv4}")

        # Conectar usando hostname para SNI y hostaddr para la IP real.
        # Sin cursor_factory=RealDictCursor: SQLAlchemy necesita cursores estándar
        # para introspeccionar la versión del servidor correctamente.
        conn = psycopg2.connect(
            host=p['host'],       # hostname real → usado como SNI en TLS
            hostaddr=ipv4,        # IP IPv4 → libpq conecta directo, sin DNS
            port=p['port'],
            user=p['user'],
            password=p['password'],
            dbname=p['dbname'],
            sslmode='require',
            connect_timeout=10,
        )
        logger.info("Conexión a base de datos establecida exitosamente vía IPv4")
        return conn

    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise

def create_db_engine():
    """Crea un engine de SQLAlchemy con conexión forzada a IPv4"""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está configurada en las variables de entorno")

    engine = create_engine(
        DATABASE_URL,
        creator=_make_ipv4_connection,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    return engine

# Crear engine y session factory globales
engine = None
SessionLocal = None

def init_db():
    """Inicializa la conexión a la base de datos (no crea tablas, asume que ya existen)"""
    global engine, SessionLocal
    try:
        engine = create_db_engine()
        SessionLocal = scoped_session(sessionmaker(bind=engine))

        # Verificar conexión ejecutando una consulta simple
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            result.fetchone()
            logger.info("Conexión a base de datos verificada exitosamente")

        logger.info("Base de datos inicializada correctamente")

    except Exception as e:
        logger.error(f"Error en init_db: {e}")
        raise

def get_db():
    """Retorna una sesión de SQLAlchemy"""
    if SessionLocal is None:
        raise RuntimeError("Base de datos no inicializada")
    return SessionLocal()

def dict_from_row(row):
    """Convierte una fila de SQLAlchemy a diccionario. Retorna None si row es None."""
    if row is None:
        return None
    return dict(row._mapping)

def rows_to_list(rows):
    """Convierte una lista de filas SQLAlchemy a lista de diccionarios."""
    return [dict(r._mapping) for r in rows]

# ─────────────────────────────────────────────
# AUTH — token simple HMAC
# ─────────────────────────────────────────────
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
                {"token": token, "now": int(time.time())}
            ).fetchone()
            return result is not None
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Error en verify_token: {e}")
        return False

async def require_auth(request: Request):
    token = request.cookies.get("session") or request.headers.get("X-Session-Token", "")
    if not token or not verify_token(token):
        raise HTTPException(401, "No autorizado")
    return token

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    try:
        db = get_db()
        try:
            result = db.execute(
                text("SELECT value FROM settings WHERE key=:key"),
                {"key": key}
            ).fetchone()
            # FIX: usar dict_from_row para acceso por nombre de columna
            row = dict_from_row(result)
            return row["value"] if row else default
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error en get_setting: {e}")
        return default

def set_setting(key: str, value: str):
    try:
        db = get_db()
        try:
            db.execute(
                text("INSERT INTO settings (key,value) VALUES (:key,:value) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value"),
                {"key": key, "value": value}
            )
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error en set_setting: {e}")
        raise

# ─────────────────────────────────────────────
# MARKDOWN → HTML
# ─────────────────────────────────────────────
def md_to_html(text: str) -> str:
    text = text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    text = re.sub(r'^### (.+)$', r'<h3 style="margin:16px 0 8px;font-size:1.1em">\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$',  r'<h2 style="margin:20px 0 10px;font-size:1.3em">\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$',   r'<h1 style="margin:24px 0 12px;font-size:1.5em">\1</h1>', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*\*(.+?)\*\*\*', r'<strong><em>\1</em></strong>', text)
    text = re.sub(r'\*\*(.+?)\*\*',     r'<strong>\1</strong>', text)
    text = re.sub(r'\*(.+?)\*',         r'<em>\1</em>', text)
    text = re.sub(r'__(.+?)__',         r'<strong>\1</strong>', text)
    text = re.sub(r'_(.+?)_',           r'<em>\1</em>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2" style="color:LINKCOLOR;text-decoration:underline">\1</a>', text)
    lines = text.split('\n'); result, in_ul = [], False
    for line in lines:
        if re.match(r'^[-*•] (.+)', line):
            if not in_ul: result.append('<ul style="margin:10px 0;padding-left:24px">'); in_ul=True
            result.append(f'<li style="margin:5px 0">{re.sub(r"^[-*•] ","",line)}</li>')
        else:
            if in_ul: result.append('</ul>'); in_ul=False
            result.append(line)
    if in_ul: result.append('</ul>')
    text = '\n'.join(result)
    paragraphs = re.split(r'\n\n+', text)
    wrapped = []
    for p in paragraphs:
        p = p.strip()
        if not p: continue
        if p.startswith('<h') or p.startswith('<ul') or p.startswith('<li'):
            wrapped.append(p)
        else:
            wrapped.append(f'<p style="margin:0 0 14px;line-height:1.7">{p.replace(chr(10),"<br>")}</p>')
    return '\n'.join(wrapped)

# ─────────────────────────────────────────────
# HTML EMAIL BUILDER
# ─────────────────────────────────────────────
def build_html_email(body_text: str, style_cfg: dict = None) -> str:
    s         = style_cfg or {}
    primary   = s.get("primary_color",  "#7ec850")
    bg        = s.get("bg_color",       "#ffffff")
    text_col  = s.get("text_color",     "#1a1a1a")
    font      = s.get("font_family",    "Georgia,'Times New Roman',serif")
    fsize     = s.get("font_size",      "16px")
    link_col  = s.get("link_color",     "#2563eb")
    header_bg = s.get("header_bg",      "#0c0f0a")
    header_fc = s.get("header_color",   "#7ec850")
    sname     = s.get("sender_name",    cfg()["sender_name"])
    sig_html  = s.get("signature_html", get_setting("signature_html", ""))
    body_html = md_to_html(body_text).replace("LINKCOLOR", link_col)

    sig_block = ""
    if sig_html:
        sig_block = (
            '<tr><td style="padding:0 36px 24px 36px">'
            '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            '<tr><td style="border-top:2px solid ' + primary + ';padding-top:16px;'
            'font-size:13px;color:#666;font-family:Arial,sans-serif;line-height:1.5">'
            + sig_html +
            '</td>'
            '</tr>'
            '</table>'
            '</td>'
        )

    footer_name = sname or "Asistente Ejecutivo"

    return (
        '<!DOCTYPE html>'
        '<html lang="es" xmlns="http://www.w3.org/1999/xhtml">'
        '<head>'
        '<meta charset="UTF-8"/>'
        '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
        '<meta http-equiv="X-UA-Compatible" content="IE=edge"/>'
        '<title>Email</title>'
        '</head>'
        '<body style="margin:0;padding:0;background-color:#f0f0f0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"'
        ' style="background-color:#f0f0f0;padding:32px 16px">'
        '<tr><td align="center">'
        '<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"'
        ' style="max-width:600px;width:100%">'
        '<tr>'
        '<td style="background-color:' + header_bg + ';border-radius:10px 10px 0 0;padding:24px 36px">'
        '<span style="font-family:Georgia,serif;font-size:21px;font-weight:700;'
        'color:' + header_fc + ';letter-spacing:-0.3px;line-height:1">'
        + (sname or "") +
        '</span>'
        '</td>'
        '</tr>'
        '<tr>'
        '<td style="background-color:' + bg + ';padding:36px 36px 28px 36px;'
        'font-family:' + font + ';font-size:' + fsize + ';color:' + text_col + ';line-height:1.75">'
        + body_html +
        '</td>'
        '</tr>'
        + sig_block +
        '<tr>'
        '<td style="background-color:#f8f8f8;border-radius:0 0 10px 10px;'
        'border-top:1px solid #e4e4e4;padding:16px 36px">'
        '<p style="margin:0;font-size:12px;color:#aaa;font-family:Arial,sans-serif">'
        'Enviado desde <strong style="color:#888">' + footer_name + '</strong>.'
        '</p>'
        '</td>'
        '</tr>'
        '</table>'
        '</td>'
        '</tr>'
        '</table>'
        '</body></html>'
    )

# ─────────────────────────────────────────────
# RESEND SEND
# ─────────────────────────────────────────────
def send_resend(to: str, subject: str, body_plain: str, body_html: str, reply_to_mid: str = None):
    c = cfg()
    if not c["resend_api_key"]:
        raise ValueError("RESEND_API_KEY no configurada en las variables de entorno de Render")
    if not c["sender_email"]:
        raise ValueError("SENDER_EMAIL no configurada en las variables de entorno de Render")

    resend.api_key = c["resend_api_key"]

    from_addr = f"{c['sender_name']} <{c['sender_email']}>" if c["sender_name"] else c["sender_email"]

    params: resend.Emails.SendParams = {
        "from":    from_addr,
        "to":      [to],
        "subject": subject,
        "html":    body_html,
        "text":    body_plain,
    }
    if reply_to_mid:
        params["headers"] = {"In-Reply-To": reply_to_mid, "References": reply_to_mid}

    response = resend.Emails.send(params)
    email_id = response.id if hasattr(response, "id") else str(response)
    logger.info(f"Resend OK → {to} | id={email_id}")
    return response

# ─────────────────────────────────────────────
# IMAP SYNC (delete-aware)
# ─────────────────────────────────────────────
def decode_str(s):
    if not s: return ""
    parts = decode_header(s); result = []
    for part, enc in parts:
        result.append(part.decode(enc or "utf-8", errors="replace") if isinstance(part, bytes) else str(part))
    return "".join(result)

def fetch_inbox_sync(limit=60):
    c = cfg()
    if not c["imap_host"] or not c["imap_user"]: return {"added":0,"deleted":0,"error":"IMAP no configurado"}
    try:
        mail = imaplib.IMAP4_SSL(c["imap_host"], c["imap_port"])
        mail.login(c["imap_user"], c["imap_pass"])
        mail.select("INBOX")

        _, uid_data = mail.uid("SEARCH", None, "ALL")
        server_uids = set(uid_data[0].decode().split()) if uid_data[0] else set()

        db = get_db()
        try:
            # Obtener UIDs actuales en DB
            # FIX: usar dict_from_row para acceso por nombre de columna
            result = db.execute(text("SELECT message_id, imap_uid FROM inbox_cache"))
            db_rows = result.fetchall()
            db_by_uid = {dict_from_row(r)["imap_uid"]: dict_from_row(r)["message_id"] for r in db_rows if dict_from_row(r)["imap_uid"]}
            db_message_ids = {dict_from_row(r)["message_id"] for r in db_rows}

            # Eliminar mensajes que ya no están en el servidor
            deleted_uids = set(db_by_uid.keys()) - server_uids
            deleted_count = 0
            for uid in deleted_uids:
                mid = db_by_uid[uid]
                db.execute(text("DELETE FROM inbox_cache WHERE message_id=:mid"), {"mid": mid})
                deleted_count += 1
            if deleted_count:
                db.commit()
                logger.info(f"IMAP sync: {deleted_count} mensajes eliminados de DB")

            # Agregar nuevos mensajes
            uids_to_fetch = list(server_uids)[-limit:]
            added_count = 0

            for uid in reversed(uids_to_fetch):
                _, msg_data = mail.uid("FETCH", uid, "(RFC822)")
                if not msg_data or not msg_data[0]: continue
                raw = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw)
                mid = msg.get("Message-ID", "").strip()
                if not mid: mid = f"uid-{uid}"

                if mid in db_message_ids: continue

                subj = decode_str(msg.get("Subject", ""))
                from_ = decode_str(msg.get("From", ""))
                date_ = msg.get("Date", "")
                from_email, from_name = "", ""
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
                            try: body = part.get_payload(decode=True).decode("utf-8", errors="replace"); break
                            except: pass
                else:
                    try: body = msg.get_payload(decode=True).decode("utf-8", errors="replace")
                    except: body = ""
                try:
                    db.execute(
                        text("""INSERT INTO inbox_cache (message_id,imap_uid,from_email,from_name,subject,body,date)
                                VALUES (:mid,:uid,:from_email,:from_name,:subj,:body,:date)
                                ON CONFLICT (message_id) DO NOTHING"""),
                        {"mid": mid, "uid": uid, "from_email": from_email, "from_name": from_name,
                         "subj": subj, "body": body[:3000], "date": date_}
                    )
                    db.commit()
                    added_count += 1
                except Exception as e:
                    db.rollback()
                    logger.error(f"Error insertando msg {mid}: {e}")

        finally:
            db.close()

        mail.logout()
        logger.info(f"IMAP sync: +{added_count} nuevos, -{deleted_count} eliminados")
        return {"added": added_count, "deleted": deleted_count, "total": len(server_uids)}
    except Exception as e:
        logger.error(f"IMAP sync error: {e}")
        return {"added":0,"deleted":0,"error":str(e)}

# ─────────────────────────────────────────────
# SCHEDULED EMAIL PROCESSOR
# ─────────────────────────────────────────────
def _build_style() -> dict:
    """Lee los estilos desde settings para construir emails HTML."""
    c = cfg()
    return {
        "primary_color":  get_setting("style_primary_color",  "#7ec850"),
        "bg_color":       get_setting("style_bg_color",       "#ffffff"),
        "text_color":     get_setting("style_text_color",     "#1a1a1a"),
        "font_family":    get_setting("style_font_family",    "Georgia,'Times New Roman',serif"),
        "font_size":      get_setting("style_font_size",      "16px"),
        "link_color":     get_setting("style_link_color",     "#2563eb"),
        "header_bg":      get_setting("style_header_bg",      "#0c0f0a"),
        "header_color":   get_setting("style_header_color",   "#7ec850"),
        "sender_name":    c["sender_name"],
        "signature_html": get_setting("signature_html",       ""),
    }

def process_scheduled_emails(now_dt: datetime = None) -> dict:
    """
    Procesa y envía todos los emails de campaña cuya scheduled_at ya venció.
    Retorna un resumen de lo procesado.
    Marca cada email_contact con sent/failed en campaign_email_sends.
    """
    if now_dt is None:
        now_dt = datetime.utcnow()

    now_str = now_dt.strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] Procesando emails programados — ahora UTC: {now_str}")

    db = get_db()
    results = {"sent": 0, "failed": 0, "skipped": 0, "details": []}
    try:
        # Obtener emails de campañas 'scheduled' cuya hora ya pasó y no fueron enviados aún
        due_emails = rows_to_list(db.execute(text("""
            SELECT ce.id, ce.campaign_id, ce.day_number, ce.subject, ce.body,
                   ce.scheduled_at, ce.status as email_status,
                   c.status as camp_status, c.name as camp_name
            FROM campaign_emails ce
            JOIN campaigns c ON c.id = ce.campaign_id
            WHERE c.status = 'scheduled'
              AND ce.status = 'approved'
              AND ce.sent_at IS NULL
              AND ce.scheduled_at IS NOT NULL
              AND ce.scheduled_at <= :now
            ORDER BY ce.scheduled_at ASC
        """), {"now": now_str}).fetchall())

        if not due_emails:
            logger.info("[scheduler] No hay emails vencidos para enviar.")
            return results

        style_cfg = _build_style()

        for em in due_emails:
            contacts_rows = rows_to_list(db.execute(text(
                "SELECT email FROM campaign_contacts WHERE campaign_id=:cid"
            ), {"cid": em["campaign_id"]}).fetchall())
            contact_emails = [r["email"] for r in contacts_rows]

            if not contact_emails:
                logger.warning(f"[scheduler] Email id={em['id']} sin contactos — omitido.")
                results["skipped"] += 1
                continue

            body_html = build_html_email(em["body"], style_cfg)
            ok_count = 0
            fail_count = 0

            for to in contact_emails:
                try:
                    send_resend(to, em["subject"], em["body"], body_html)
                    ok_count += 1
                    results["sent"] += 1
                    results["details"].append({
                        "camp_name": em["camp_name"],
                        "day": em["day_number"],
                        "to": to,
                        "ok": True,
                        "scheduled_at": em["scheduled_at"],
                    })
                except Exception as e:
                    fail_count += 1
                    results["failed"] += 1
                    results["details"].append({
                        "camp_name": em["camp_name"],
                        "day": em["day_number"],
                        "to": to,
                        "ok": False,
                        "error": str(e),
                        "scheduled_at": em["scheduled_at"],
                    })
                    logger.error(f"[scheduler] Error enviando a {to}: {e}")

            # Marcar el email como enviado si al menos uno salió bien
            if ok_count > 0:
                db.execute(text("""
                    UPDATE campaign_emails
                    SET sent_at = :now, send_status = 'sent'
                    WHERE id = :id
                """), {"now": now_dt.isoformat(), "id": em["id"]})
                logger.info(f"[scheduler] Email id={em['id']} enviado → {ok_count} ok, {fail_count} errores")
            elif fail_count > 0:
                db.execute(text("""
                    UPDATE campaign_emails
                    SET send_status = 'failed'
                    WHERE id = :id
                """), {"id": em["id"]})

        db.commit()

        # Si todos los emails de una campaña fueron enviados, marcarla como 'sent'
        campaigns_done = set(em["campaign_id"] for em in due_emails)
        for cid in campaigns_done:
            pending = dict_from_row(db.execute(text("""
                SELECT COUNT(*) as n FROM campaign_emails
                WHERE campaign_id=:cid AND status='approved' AND sent_at IS NULL
            """), {"cid": cid}).fetchone())
            if pending and pending["n"] == 0:
                db.execute(text("UPDATE campaigns SET status='sent' WHERE id=:id"), {"id": cid})
        db.commit()

    except Exception as e:
        logger.error(f"[scheduler] Error en process_scheduled_emails: {e}")
        db.rollback()
    finally:
        db.close()

    logger.info(f"[scheduler] Resultado: {results['sent']} enviados, {results['failed']} fallidos, {results['skipped']} omitidos")
    return results


async def _scheduler_loop():
    """Loop de background: cada 60 segundos procesa emails programados."""
    await asyncio.sleep(5)  # Pequeño delay al inicio para que la DB esté lista
    while True:
        try:
            process_scheduled_emails()
        except Exception as e:
            logger.error(f"[scheduler_loop] Error inesperado: {e}")
        await asyncio.sleep(60)


# ─────────────────────────────────────────────
# APP LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    task = None
    try:
        init_db()
        logger.info("Aplicación iniciada correctamente")
        # Al arrancar: solo loguear cuántos emails están demorados, NO enviarlos.
        # El usuario decide desde la Agenda de Envío.
        try:
            db = get_db()
            try:
                now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                delayed = db.execute(text("""
                    SELECT COUNT(*) as n FROM campaign_emails ce
                    JOIN campaigns c ON c.id = ce.campaign_id
                    WHERE c.status = 'scheduled' AND ce.status = 'approved'
                      AND ce.sent_at IS NULL AND ce.scheduled_at IS NOT NULL
                      AND ce.scheduled_at <= :now
                """), {"now": now_str}).fetchone()
                n = dict_from_row(delayed)["n"] if delayed else 0
                if n > 0:
                    logger.warning(f"[startup] {n} email(s) demorados detectados. El usuario debe enviarlos desde la Agenda.")
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"[startup] No se pudo verificar emails demorados: {e}")
        # Iniciar loop de scheduler en background (solo envía en horario, no los demorados)
        task = asyncio.create_task(_scheduler_loop())
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {e}")
    yield
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    logger.info("Aplicación cerrada")

app = FastAPI(title="Asistente Ejecutivo API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                   allow_credentials=True)

# ─────────────────────────────────────────────
# FRONTEND SERVING
# ─────────────────────────────────────────────
def find_index_html():
    for loc in [
        os.path.join(BASE, "static", "index.html"),
        os.path.join(BASE, "index.html"),
        "/opt/render/project/src/static/index.html",
        "/app/static/index.html",
    ]:
        if os.path.exists(loc):
            logger.info(f"Frontend: {loc}")
            return loc
    return None

INDEX_PATH = find_index_html()

static_dir = os.path.join(BASE, "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

index_root = os.path.join(BASE, "index.html")
if os.path.exists(index_root) and not os.path.exists(os.path.join(static_dir, "index.html")):
    import shutil
    shutil.copy2(index_root, os.path.join(static_dir, "index.html"))
    INDEX_PATH = os.path.join(static_dir, "index.html")

if os.path.exists(static_dir) and os.listdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

API_PATHS = ["auth","contacts","inbox","send-email","settings","context",
             "preview-email","logs","memory","supervision","stats",
             "smtp-test","config","api","campaigns","groq-key",
             "schedule","process-scheduled"]

@app.post("/auth/login")
async def login(request: Request):
    data = await request.json()
    user = data.get("username","").strip()
    pw   = data.get("password","")
    if user != APP_USER or pw != APP_PASSWORD:
        raise HTTPException(401, "Credenciales incorrectas")
    token = make_token()
    now   = int(time.time())
    db = get_db()
    try:
        db.execute(text("DELETE FROM sessions WHERE expires_at<:now"), {"now": now})
        db.execute(
            text("INSERT INTO sessions (token,created_at,expires_at) VALUES (:token,:created_at,:expires_at)"),
            {"token": token, "created_at": now, "expires_at": now + TOKEN_TTL}
        )
        db.commit()
    finally:
        db.close()
    logger.info(f"Login OK: user={user!r} | IS_HTTPS={IS_HTTPS}")
    resp = JSONResponse({"success": True, "token": token})
    resp.set_cookie(
        "session", token,
        httponly=True,
        samesite="lax",
        max_age=TOKEN_TTL,
        secure=IS_HTTPS,
    )
    return resp

@app.post("/auth/logout")
async def logout(request: Request):
    token = request.cookies.get("session","")
    if token:
        db = get_db()
        try:
            db.execute(text("DELETE FROM sessions WHERE token=:token"), {"token": token})
            db.commit()
        finally:
            db.close()
    resp = JSONResponse({"success": True})
    resp.delete_cookie("session")
    return resp

@app.get("/auth/check")
async def auth_check(token: str = Depends(require_auth)):
    return {"ok": True, "user": APP_USER}

# ─────────────────────────────────────────────
# ROUTES — CONFIG/STATUS (protegidas)
# ─────────────────────────────────────────────
@app.get("/api/status")
def root(_: str = Depends(require_auth)):
    c = cfg()
    return {
        "status":       "Asistente Ejecutivo API",
        "smtp_user":    c["sender_email"] or "NO CONFIG",
        "imap_host":    c["imap_host"]    or "NO CONFIG",
        "provider":     "resend",
        "resend_ready": bool(c["resend_api_key"] and c["sender_email"]),
    }

@app.get("/config")
def get_config(_: str = Depends(require_auth)):
    c = cfg()
    return {
        "provider":          "resend",
        "sender_email":      c["sender_email"],
        "sender_name":       c["sender_name"],
        "resend_key_set":    bool(c["resend_api_key"]),
        "imap_host":         c["imap_host"],
        "imap_port":         c["imap_port"],
        "imap_user":         c["imap_user"],
        "imap_pass_set":     bool(c["imap_pass"]),
    }

# ─────────────────────────────────────────────
# ROUTES — SETTINGS
# ─────────────────────────────────────────────
@app.get("/settings")
def get_all_settings(_: str = Depends(require_auth)):
    db = get_db()
    try:
        result = db.execute(text("SELECT key,value FROM settings"))
        rows = result.fetchall()
        # FIX: usar dict_from_row para acceso por nombre de columna
        return {dict_from_row(r)["key"]: dict_from_row(r)["value"] for r in rows}
    finally:
        db.close()

@app.post("/settings")
async def save_settings(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    for k, v in data.items():
        set_setting(k, str(v))
    return {"success": True}

@app.get("/context")
def get_context(_: str = Depends(require_auth)):
    return {
        "entity": get_setting("ctx_entity"),
        "mission": get_setting("ctx_mission"),
        "extra": get_setting("ctx_extra")
    }

@app.post("/context")
async def save_context(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    for k in ("entity","mission","extra"):
        if k in data: set_setting("ctx_"+k, data[k])
    return {"success": True}

@app.post("/preview-email")
async def preview_email(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    style = {
        "primary_color":  get_setting("style_primary_color","#7ec850"),
        "bg_color":       get_setting("style_bg_color","#ffffff"),
        "text_color":     get_setting("style_text_color","#1a1a1a"),
        "font_family":    get_setting("style_font_family","Georgia,'Times New Roman',serif"),
        "font_size":      get_setting("style_font_size","16px"),
        "link_color":     get_setting("style_link_color","#2563eb"),
        "header_bg":      get_setting("style_header_bg","#0c0f0a"),
        "header_color":   get_setting("style_header_color","#7ec850"),
        "signature_html": get_setting("signature_html",""),
    }
    for k in style: style[k] = data.get(k, style[k])
    style["sender_name"] = data.get("sender_name", cfg()["sender_name"])
    return {"html": build_html_email(data.get("body",""), style)}

# ─────────────────────────────────────────────
# ROUTES — CONTACTS
# ─────────────────────────────────────────────
@app.get("/contacts")
def list_contacts(_: str = Depends(require_auth)):
    db = get_db()
    try:
        result = db.execute(text("SELECT * FROM contacts ORDER BY name"))
        rows = result.fetchall()
        return rows_to_list(rows)
    finally:
        db.close()

@app.post("/contacts")
async def create_contact(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    if not data.get("name") or not data.get("email"):
        raise HTTPException(400, "name y email son obligatorios")

    db = get_db()
    try:
        result = db.execute(
            text("""INSERT INTO contacts (name,email,company,role,phone,context,tags)
                    VALUES (:name,:email,:company,:role,:phone,:context,:tags) RETURNING id"""),
            {"name": data["name"], "email": data["email"], "company": data.get("company",""),
             "role": data.get("role",""), "phone": data.get("phone",""),
             "context": data.get("context",""), "tags": data.get("tags","")}
        )
        # FIX: usar dict_from_row para acceso por nombre de columna
        cid = dict_from_row(result.fetchone())["id"]
        db.execute(
            text("INSERT INTO memory (type,entity,content) VALUES (:type,:entity,:content)"),
            {"type": "contact_added", "entity": data["email"],
             "content": f"Contacto: {data['name']} - {data.get('company','')}"}
        )
        db.commit()
    except Exception as e:
        db.rollback()
        if "duplicate key" in str(e).lower():
            raise HTTPException(409, "Email ya existe")
        raise
    finally:
        db.close()
    return {"success": True, "id": cid}

@app.put("/contacts/{cid}")
async def update_contact(cid: int, request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    db = get_db()
    try:
        db.execute(
            text("""UPDATE contacts SET name=:name,company=:company,role=:role,phone=:phone,
                    context=:context,tags=:tags,updated_at=NOW() WHERE id=:cid"""),
            {"name": data.get("name"), "company": data.get("company",""), "role": data.get("role",""),
             "phone": data.get("phone",""), "context": data.get("context",""),
             "tags": data.get("tags",""), "cid": cid}
        )
        db.commit()
    finally:
        db.close()
    return {"success": True}

@app.delete("/contacts/{cid}")
def delete_contact(cid: int, _: str = Depends(require_auth)):
    db = get_db()
    try:
        db.execute(text("DELETE FROM contacts WHERE id=:cid"), {"cid": cid})
        db.commit()
    finally:
        db.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — SEND (simple + multiple)
# ─────────────────────────────────────────────


def _log_sent(db, to, subject, body, body_html, intent, campaign_id):
    result = db.execute(text("SELECT name FROM contacts WHERE email=:email"), {"email": to})
    row = dict_from_row(result.fetchone())
    # FIX: usar dict_from_row para acceso por nombre de columna
    cname = row["name"] if row else to.split("@")[0]
    db.execute(
        text("""INSERT INTO email_logs (direction,contact_email,contact_name,subject,body,body_html,intent,status,sent_at,campaign_id)
                VALUES (:direction,:contact_email,:contact_name,:subject,:body,:body_html,:intent,:status,NOW(),:campaign_id)"""),
        {"direction": "out", "contact_email": to, "contact_name": cname, "subject": subject,
         "body": body, "body_html": body_html, "intent": intent, "status": "sent", "campaign_id": campaign_id}
    )
    db.execute(
        text("INSERT INTO memory (type,entity,content,importance) VALUES (:type,:entity,:content,:importance)"),
        {"type": "email_sent", "entity": to,
         "content": f"Email enviado a {cname}: {subject}", "importance": 2}
    )

@app.post("/send-email")
async def send_email(request: Request, _: str = Depends(require_auth)):
    data    = await request.json()
    recipients_raw = data.get("recipients") or ([data.get("to","")] if data.get("to") else [])
    recipients = [r.strip() for r in recipients_raw if r.strip()]
    subject     = data.get("subject","").strip()
    body        = data.get("body","").strip()
    intent      = data.get("intent","general")
    campaign_id = data.get("campaign_id")
    reply_to    = data.get("reply_to")

    if not recipients: raise HTTPException(400, "Al menos un destinatario es requerido")
    if not subject:    raise HTTPException(400, "subject es obligatorio")
    if not body:       raise HTTPException(400, "body es obligatorio")

    c = cfg()
    if not c["resend_api_key"]:
        raise HTTPException(500, "RESEND_API_KEY no configurada")
    if not c["sender_email"]:
        raise HTTPException(500, "SENDER_EMAIL no configurada")

    style_cfg = _build_style()
    body_html = build_html_email(body, style_cfg)

    results = []
    db = get_db()
    try:
        for to in recipients:
            try:
                resp = send_resend(to, subject, body, body_html, reply_to)
                email_id = resp.id if hasattr(resp, "id") else "?"
                _log_sent(db, to, subject, body, body_html, intent, campaign_id)
                results.append({"to": to, "ok": True, "resend_id": email_id})
                logger.info(f"Email enviado via Resend a {to} | resend_id={email_id}")
            except resend.exceptions.ResendError as e:
                logger.error(f"Resend API error → {to}: code={e.code} msg={e.message}")
                results.append({"to": to, "ok": False, "error": f"Resend error {e.code}: {e.message}"})
            except Exception as e:
                logger.error(f"Error inesperado → {to}: {type(e).__name__}: {e}")
                results.append({"to": to, "ok": False, "error": f"{type(e).__name__}: {e}"})
        db.commit()
    finally:
        db.close()

    sent_ok  = [r for r in results if r["ok"]]
    sent_err = [r for r in results if not r["ok"]]
    if not sent_ok and sent_err:
        raise HTTPException(500, sent_err[0]["error"])
    return {"success": True, "sent": len(sent_ok), "failed": len(sent_err), "results": results}

@app.get("/smtp-test")
async def smtp_test(_: str = Depends(require_auth)):
    c = cfg()
    if not c["resend_api_key"]:
        raise HTTPException(500, "RESEND_API_KEY no configurada")
    if not c["sender_email"]:
        raise HTTPException(500, "SENDER_EMAIL no configurada")
    try:
        resend.api_key = c["resend_api_key"]
        domains = resend.Domains.list()
        return {
            "ok": True,
            "provider": "resend",
            "sender": c["sender_email"],
            "sender_name": c["sender_name"],
            "domains": [d.get("name") for d in (domains.get("data") or [])],
        }
    except Exception as e:
        raise HTTPException(500, f"{type(e).__name__}: {e}")

# ─────────────────────────────────────────────
# ROUTES — INBOX
# ─────────────────────────────────────────────
@app.get("/inbox")
def get_inbox(refresh: bool = False, _: str = Depends(require_auth)):
    result = None
    if refresh:
        result = fetch_inbox_sync(60)
    db = get_db()
    try:
        rows = db.execute(text("SELECT * FROM inbox_cache ORDER BY date DESC LIMIT 80")).fetchall()
        resp = rows_to_list(rows)
        if result: return {"messages": resp, "sync": result}
        return {"messages": resp, "sync": None}
    finally:
        db.close()

@app.post("/inbox/mark-replied")
async def mark_replied(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    db = get_db()
    try:
        db.execute(text("UPDATE inbox_cache SET replied=1 WHERE message_id=:message_id"),
                   {"message_id": data.get("message_id")})
        db.commit()
    finally:
        db.close()
    return {"success": True}

@app.delete("/inbox/{message_id:path}")
async def delete_inbox_message(message_id: str, _: str = Depends(require_auth)):
    """Elimina un mensaje de inbox_cache (solo vista frontend, no borra del servidor IMAP)."""
    db = get_db()
    try:
        db.execute(text("DELETE FROM inbox_cache WHERE message_id=:mid"), {"mid": message_id})
        db.commit()
        return {"success": True}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()

@app.get("/contacts/{contact_id}/sent-history")
def get_contact_sent_history(contact_id: int, _: str = Depends(require_auth)):
    """Retorna los últimos emails enviados a un contacto para contexto del agente."""
    db = get_db()
    try:
        contact = dict_from_row(db.execute(
            text("SELECT email FROM contacts WHERE id=:id"), {"id": contact_id}
        ).fetchone())
        if not contact:
            raise HTTPException(404, "Contacto no encontrado")
        rows = rows_to_list(db.execute(text("""
            SELECT subject, body, sent_at, intent
            FROM email_logs
            WHERE contact_email=:email AND direction='out'
            ORDER BY sent_at DESC LIMIT 5
        """), {"email": contact["email"]}).fetchall())
        return rows
    finally:
        db.close()

@app.post("/inbox/save-suggestion")
async def save_suggestion(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    db = get_db()
    try:
        db.execute(text("UPDATE inbox_cache SET ai_suggestion=:suggestion WHERE message_id=:message_id"),
                   {"suggestion": data.get("suggestion",""), "message_id": data.get("message_id")})
        db.commit()
    finally:
        db.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — LOGS
# ─────────────────────────────────────────────
@app.get("/logs")
def get_logs(limit: int = 100, _: str = Depends(require_auth)):
    db = get_db()
    try:
        rows = db.execute(text("SELECT * FROM email_logs ORDER BY created_at DESC LIMIT :limit"),
                         {"limit": limit}).fetchall()
        return rows_to_list(rows)
    finally:
        db.close()

@app.get("/logs/{log_id}")
def get_log(log_id: int, _: str = Depends(require_auth)):
    db = get_db()
    try:
        row = db.execute(text("SELECT * FROM email_logs WHERE id=:log_id"), {"log_id": log_id}).fetchone()
        if not row: raise HTTPException(404, "Log no encontrado")
        return dict_from_row(row)
    finally:
        db.close()

# ─────────────────────────────────────────────
# ROUTES — MEMORY
# ─────────────────────────────────────────────
@app.get("/memory")
def get_memory(limit: int = 100, _: str = Depends(require_auth)):
    db = get_db()
    try:
        rows = db.execute(text("SELECT * FROM memory ORDER BY created_at DESC LIMIT :limit"),
                         {"limit": limit}).fetchall()
        return rows_to_list(rows)
    finally:
        db.close()

@app.post("/memory")
async def add_memory(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    db = get_db()
    try:
        db.execute(
            text("INSERT INTO memory (type,entity,content,importance) VALUES (:type,:entity,:content,:importance)"),
            {"type": data.get("type","manual"), "entity": data.get("entity",""),
             "content": data.get("content",""), "importance": data.get("importance",1)}
        )
        db.commit()
    finally:
        db.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — SUPERVISION
# ─────────────────────────────────────────────
@app.get("/supervision")
def get_supervision(_: str = Depends(require_auth)):
    db = get_db()
    try:
        sent = db.execute(
            text("""SELECT l.*, c.company FROM email_logs l
                    LEFT JOIN contacts c ON l.contact_email=c.email
                    WHERE l.direction='out' ORDER BY l.sent_at DESC LIMIT 100""")
        ).fetchall()

        replied_set = set()
        replied = db.execute(text("SELECT from_email FROM inbox_cache WHERE replied=1")).fetchall()
        for r in replied:
            # FIX: usar dict_from_row para acceso por nombre de columna
            replied_set.add(dict_from_row(r)["from_email"])

        result = []
        for row in sent:
            d = dict_from_row(row)
            try:
                sent_at_val = d.get("sent_at")
                if sent_at_val:
                    if isinstance(sent_at_val, str):
                        dt = datetime.fromisoformat(sent_at_val)
                    else:
                        dt = sent_at_val.replace(tzinfo=None)
                    hrs = round((datetime.utcnow() - dt).total_seconds() / 3600, 1)
                else:
                    hrs = None
            except Exception:
                hrs = None
            d["hours_since_sent"] = hrs
            d["has_reply"] = d["contact_email"] in replied_set
            result.append(d)
        return result
    finally:
        db.close()

# ─────────────────────────────────────────────
# ROUTES — STATS
# ─────────────────────────────────────────────
@app.get("/stats")
def get_stats(_: str = Depends(require_auth)):
    db = get_db()
    try:
        # FIX: acceder por índice [0] para queries de una sola columna COUNT(*)
        contacts = db.execute(text("SELECT COUNT(*) FROM contacts")).fetchone()[0]
        sent     = db.execute(text("SELECT COUNT(*) FROM email_logs WHERE direction='out'")).fetchone()[0]
        inbox    = db.execute(text("SELECT COUNT(*) FROM inbox_cache")).fetchone()[0]
        replied  = db.execute(text("SELECT COUNT(*) FROM inbox_cache WHERE replied=1")).fetchone()[0]
        memory   = db.execute(text("SELECT COUNT(*) FROM memory")).fetchone()[0]
        return {"contacts": contacts, "sent": sent, "inbox": inbox, "replied": replied, "memory": memory}
    finally:
        db.close()

# ─────────────────────────────────────────────
# SERVIR FRONTEND (sin autenticación)
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# ROUTES — GROQ KEY (provista por API)
# ─────────────────────────────────────────────
@app.get("/groq-key")
def get_groq_key(_: str = Depends(require_auth)):
    """Devuelve la Groq API Key configurada en env vars (sin exponerla completa)."""
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return {"configured": False, "key": ""}
    # Devuelve la key completa — solo accesible a usuarios autenticados
    return {"configured": True, "key": key}

# ─────────────────────────────────────────────
# ROUTES — CAMPAIGNS
# ─────────────────────────────────────────────

def _get_campaign_dates(start_date: str, end_date: str, send_mode: str) -> list:
    """Genera la lista de fechas válidas según el modo de envío."""
    from datetime import date, timedelta
    try:
        start = date.fromisoformat(start_date)
        end   = date.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(400, "Fechas inválidas. Usar formato YYYY-MM-DD")
    if start > end:
        raise HTTPException(400, "start_date debe ser anterior a end_date")

    dates = []
    current = start
    while current <= end:
        wd = current.weekday()  # 0=lun, 1=mar, 2=mie, 3=jue, 4=vie, 5=sab, 6=dom
        include = False
        if send_mode == "daily":
            include = True
        elif send_mode == "alternate":
            delta = (current - start).days
            include = (delta % 2 == 0)
        elif send_mode == "mon_wed_fri":
            include = wd in (0, 2, 4)
        elif send_mode == "tue_thu":
            include = wd in (1, 3)
        else:
            include = True
        if include:
            dates.append(str(current))
        current += timedelta(days=1)
    return dates

@app.get("/campaigns")
def list_campaigns(_: str = Depends(require_auth)):
    db = get_db()
    try:
        rows = db.execute(text("""
            SELECT c.*,
                   COUNT(e.id) AS total_emails,
                   SUM(CASE WHEN e.status='approved' THEN 1 ELSE 0 END) AS approved_count,
                   SUM(CASE WHEN e.status='pending'  THEN 1 ELSE 0 END) AS pending_count
            FROM campaigns c
            LEFT JOIN campaign_emails e ON e.campaign_id = c.id
            GROUP BY c.id ORDER BY c.created_at DESC
        """)).fetchall()
        return rows_to_list(rows)
    except Exception as e:
        logger.error(f"list_campaigns: {e}")
        return []
    finally:
        db.close()

@app.get("/campaigns/schedule")
def get_campaign_schedule(_: str = Depends(require_auth)):
    """
    Retorna la agenda de envío de todas las campañas programadas o enviadas.
    Cada item incluye el estado: 'sent', 'scheduled', 'delayed', 'cancelled'.
    DEBE estar declarada ANTES de /campaigns/{cid} para evitar conflicto de rutas.
    """
    db = get_db()
    now_dt = datetime.utcnow()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M")
    try:
        rows = rows_to_list(db.execute(text("""
            SELECT
                ce.id, ce.campaign_id, ce.day_number, ce.subject, ce.body,
                ce.scheduled_at, ce.sent_at, ce.send_status, ce.status as approval_status,
                c.name as camp_name, c.status as camp_status
            FROM campaign_emails ce
            JOIN campaigns c ON c.id = ce.campaign_id
            WHERE c.status IN ('scheduled', 'sent')
              AND ce.status IN ('approved', 'rejected')
            ORDER BY ce.scheduled_at ASC NULLS LAST
        """)).fetchall())

        items = []
        for r in rows:
            # Normalizar scheduled_at: puede venir como "2026-03-31", "2026-03-31 09:00", o timestamp
            raw_sched = r.get("scheduled_at") or ""
            sched = str(raw_sched).strip() if raw_sched else ""
            # Asegurar formato comparable YYYY-MM-DD HH:MM
            if sched and len(sched) == 10:  # solo fecha sin hora
                sched = sched + " 09:00"

            sent_at = r.get("sent_at")
            send_status = r.get("send_status") or ""
            approval_status = r.get("approval_status") or ""

            # Determinar estado visual
            if send_status == "cancelled" or approval_status == "rejected":
                state = "cancelled"
            elif sent_at or send_status == "sent":
                state = "sent"
            elif sched and sched[:16] <= now_str:
                state = "delayed"
            else:
                state = "scheduled"

            items.append({
                "id": r["id"],
                "campaign_id": r["campaign_id"],
                "camp_name": r["camp_name"],
                "camp_status": r["camp_status"],
                "day_number": r["day_number"],
                "subject": r["subject"] or "",
                "scheduled_at": sched,
                "sent_at": str(sent_at) if sent_at else None,
                "send_status": send_status,
                "state": state,
            })

        return items
    finally:
        db.close()


@app.post("/campaigns/process-scheduled")
async def api_process_scheduled(request: Request, _: str = Depends(require_auth)):
    """
    Dispara manualmente el procesamiento de emails programados vencidos.
    Acepta body JSON opcional: {"email_ids": [1,2,3]} para enviar IDs específicos.
    DEBE estar declarada ANTES de /campaigns/{cid} para evitar conflicto de rutas.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    email_ids = body.get("email_ids", [])

    if email_ids:
        db = get_db()
        results = {"sent": 0, "failed": 0, "details": []}
        now_dt = datetime.utcnow()
        style_cfg = _build_style()
        try:
            for eid in email_ids:
                em = dict_from_row(db.execute(text("""
                    SELECT ce.*, c.name as camp_name, c.status as camp_status
                    FROM campaign_emails ce
                    JOIN campaigns c ON c.id = ce.campaign_id
                    WHERE ce.id = :id AND ce.status = 'approved'
                """), {"id": eid}).fetchone())
                if not em:
                    continue

                contacts_rows = rows_to_list(db.execute(text(
                    "SELECT email FROM campaign_contacts WHERE campaign_id=:cid"
                ), {"cid": em["campaign_id"]}).fetchall())
                contact_emails = [r["email"] for r in contacts_rows]

                body_html = build_html_email(em["body"], style_cfg)
                ok_count = 0
                for to in contact_emails:
                    try:
                        send_resend(to, em["subject"], em["body"], body_html)
                        ok_count += 1
                        results["sent"] += 1
                        results["details"].append({"id": eid, "to": to, "ok": True})
                    except Exception as e:
                        results["failed"] += 1
                        results["details"].append({"id": eid, "to": to, "ok": False, "error": str(e)})

                if ok_count > 0:
                    db.execute(text("UPDATE campaign_emails SET sent_at=:now, send_status='sent' WHERE id=:id"),
                               {"now": now_dt.isoformat(), "id": eid})

            db.commit()

            camp_ids = set()
            for eid in email_ids:
                row = db.execute(text("SELECT campaign_id FROM campaign_emails WHERE id=:id"), {"id": eid}).fetchone()
                if row:
                    camp_ids.add(dict_from_row(row)["campaign_id"])
            for cid in camp_ids:
                pending = dict_from_row(db.execute(text("""
                    SELECT COUNT(*) as n FROM campaign_emails
                    WHERE campaign_id=:cid AND status='approved' AND sent_at IS NULL
                """), {"cid": cid}).fetchone())
                if pending and pending["n"] == 0:
                    db.execute(text("UPDATE campaigns SET status='sent' WHERE id=:id"), {"id": cid})
            db.commit()

            return {"success": True, **results}
        except Exception as e:
            db.rollback()
            raise HTTPException(500, str(e))
        finally:
            db.close()
    else:
        result = process_scheduled_emails()
        return {"success": True, **result}


@app.get("/campaigns/schedule-email/{email_id}")
def get_schedule_email_detail(email_id: int, _: str = Depends(require_auth)):
    """Retorna detalle completo de un email de campaña para el modal de agenda."""
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("""
            SELECT ce.*, c.name as camp_name, c.intent as camp_intent
            FROM campaign_emails ce
            JOIN campaigns c ON c.id = ce.campaign_id
            WHERE ce.id = :id
        """), {"id": email_id}).fetchone())
        if not row:
            raise HTTPException(404, "Email no encontrado")
        # Contactos de la campaña
        contacts = rows_to_list(db.execute(text(
            "SELECT email FROM campaign_contacts WHERE campaign_id=:cid"
        ), {"cid": row["campaign_id"]}).fetchall())
        row["contacts"] = [c["email"] for c in contacts]
        return row
    finally:
        db.close()


@app.post("/campaigns/schedule-email/{email_id}/reschedule")
async def reschedule_email(email_id: int, request: Request, _: str = Depends(require_auth)):
    """Reprograma un email de campaña a una nueva fecha/hora."""
    data = await request.json()
    new_date = data.get("scheduled_at", "").strip()
    if not new_date:
        raise HTTPException(400, "scheduled_at es requerido (formato: YYYY-MM-DD HH:MM)")
    db = get_db()
    try:
        em = dict_from_row(db.execute(text(
            "SELECT id, sent_at FROM campaign_emails WHERE id=:id"
        ), {"id": email_id}).fetchone())
        if not em:
            raise HTTPException(404, "Email no encontrado")
        if em.get("sent_at"):
            raise HTTPException(400, "No se puede reprogramar un email ya enviado")
        db.execute(text("""
            UPDATE campaign_emails
            SET scheduled_at = :sched, send_status = 'pending', sent_at = NULL
            WHERE id = :id
        """), {"sched": new_date, "id": email_id})
        db.commit()
        return {"success": True, "scheduled_at": new_date}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()


@app.post("/campaigns/schedule-email/{email_id}/cancel")
async def cancel_scheduled_email(email_id: int, _: str = Depends(require_auth)):
    """Cancela el envío programado de un email (lo marca como rejected)."""
    db = get_db()
    try:
        em = dict_from_row(db.execute(text(
            "SELECT id, sent_at FROM campaign_emails WHERE id=:id"
        ), {"id": email_id}).fetchone())
        if not em:
            raise HTTPException(404, "Email no encontrado")
        if em.get("sent_at"):
            raise HTTPException(400, "No se puede cancelar un email ya enviado")
        db.execute(text("""
            UPDATE campaign_emails
            SET status = 'rejected', send_status = 'cancelled'
            WHERE id = :id
        """), {"id": email_id})
        db.commit()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()


@app.get("/campaigns/{cid}")
def get_campaign(cid: int, _: str = Depends(require_auth)):
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise HTTPException(404, "Campaña no encontrada")
        emails = rows_to_list(db.execute(text(
            "SELECT * FROM campaign_emails WHERE campaign_id=:id ORDER BY day_number"), {"id": cid}).fetchall())
        contacts = rows_to_list(db.execute(text(
            "SELECT * FROM campaign_contacts WHERE campaign_id=:id"), {"id": cid}).fetchall())
        camp["emails"]   = emails
        camp["contacts"] = contacts
        return camp
    finally:
        db.close()

@app.delete("/campaigns/{cid}")
def delete_campaign(cid: int, _: str = Depends(require_auth)):
    db = get_db()
    try:
        db.execute(text("DELETE FROM campaign_emails   WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaign_contacts WHERE campaign_id=:id"), {"id": cid})
        db.execute(text("DELETE FROM campaigns WHERE id=:id"), {"id": cid})
        db.commit()
        return {"success": True}
    finally:
        db.close()


@app.post("/campaigns/init")
async def init_campaign(request: Request, _: str = Depends(require_auth)):
    """Crea la estructura de la campaña con placeholders vacíos. El frontend genera emails de a uno."""
    data = await request.json()
    contacts_list = data.get("contacts", [])
    start_date    = data.get("start_date", "")
    end_date      = data.get("end_date", "")
    intent        = data.get("intent", "").strip()
    send_mode     = data.get("send_mode", "daily")
    tone          = data.get("tone", "informative")
    send_time     = data.get("send_time", "09:00")
    campaign_name = data.get("name", f"Campana {start_date}")

    if not contacts_list: raise HTTPException(400, "Se requiere al menos un contacto")
    if not start_date or not end_date: raise HTTPException(400, "Fechas requeridas")
    if not intent: raise HTTPException(400, "Intencion requerida")

    dates = _get_campaign_dates(start_date, end_date, send_mode)
    if not dates:
        raise HTTPException(400, f"No hay fechas validas con modo '{send_mode}'")

    n = len(dates)
    db = get_db()
    try:
        result = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, status)
            VALUES (:name,:intent,:start_date,:end_date,:send_mode,:tone,'draft')
            RETURNING id
        """), {"name": campaign_name, "intent": intent, "start_date": start_date,
               "end_date": end_date, "send_mode": send_mode, "tone": tone})
        camp_id = result.fetchone()[0]

        for i in range(n):
            db.execute(text("""
                INSERT INTO campaign_emails (campaign_id, day_number, subject, body, status, scheduled_at, version, regenerated_count)
                VALUES (:cid,:day,'','','pending',:scheduled_at,1,0)
            """), {"cid": camp_id, "day": i+1, "scheduled_at": f"{dates[i]} {send_time}"})

        for email_addr in contacts_list:
            email_addr = email_addr.strip()
            if email_addr:
                db.execute(text("INSERT INTO campaign_contacts (campaign_id, email) VALUES (:cid,:email)"),
                           {"cid": camp_id, "email": email_addr})
        db.commit()
        return {"success": True, "campaign_id": camp_id, "total_emails": n, "dates": dates}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()


@app.post("/campaigns/{cid}/generate-one")
async def generate_one_email(cid: int, request: Request, _: str = Depends(require_auth)):
    """Genera UN email de la secuencia usando SSE streaming. El frontend llama de a uno."""
    import httpx
    from fastapi.responses import StreamingResponse
    data = await request.json()
    day_number = data.get("day_number", 1)
    feedback   = data.get("feedback", "")

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        raise HTTPException(500, "GROQ_API_KEY no configurada")

    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp: raise HTTPException(404, "Campana no encontrada")
        total = dict_from_row(db.execute(text(
            "SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:id"), {"id": cid}).fetchone())["n"]
        prev_emails = rows_to_list(db.execute(text("""
            SELECT day_number, subject, body FROM campaign_emails
            WHERE campaign_id=:id AND day_number < :day AND subject != ''
            ORDER BY day_number
        """), {"id": cid, "day": day_number}).fetchall())
        em = dict_from_row(db.execute(text("""
            SELECT * FROM campaign_emails WHERE campaign_id=:id AND day_number=:day
        """), {"id": cid, "day": day_number}).fetchone())
    finally:
        db.close()

    tone_desc = {
        "aggressive": "tono persuasivo y directo, CTA fuerte, urgencia, beneficios concretos",
        "informative": "tono educativo, informativo, construccion de confianza",
    }.get(camp.get("tone","informative"), "tono profesional")

    entity_ctx = ""
    db2 = get_db()
    try:
        r1 = dict_from_row(db2.execute(text("SELECT value FROM settings WHERE key='ctx_entity'")).fetchone())
        r2 = dict_from_row(db2.execute(text("SELECT value FROM settings WHERE key='ctx_mission'")).fetchone())
        if r1: entity_ctx += f"\nEntidad: {r1['value']}"
        if r2: entity_ctx += f"\nMision: {r2['value']}"
    except Exception: pass
    finally: db2.close()

    prev_context = ""
    if prev_emails:
        prev_context = "\n\nEMAILS ANTERIORES (para coherencia):\n"
        prev_context += "\n".join([f"- Email {e['day_number']}: [{e['subject']}]" for e in prev_emails[-3:]])

    scheduled = em['scheduled_at'] if em else ''

    system_prompt = f"""Eres experto en email marketing. Genera el email #{day_number} de {total} de una campana.

INTENCION: {camp['intent']}
TONO: {tone_desc}
FECHA PROGRAMADA: {scheduled}
{entity_ctx}
{prev_context}
{"FEEDBACK A INCORPORAR: " + feedback if feedback else ""}

REGLAS:
- Email numero {day_number} de {total}: posicionarlo narrativamente en la secuencia
- {"Primer email: presentacion, gancho inicial, presentar propuesta" if day_number == 1 else ""}
- {"Ultimo email: cierre, urgencia maxima, CTA final definitivo" if day_number == total else ""}
- Formato Markdown: **negrita**, *italica*, ## titulos, listas con -
- NO incluir Para/De/Asunto en el cuerpo
- Cuerpo completo y elaborado (minimo 150 palabras)

Responde UNICAMENTE con JSON valido (sin texto extra, sin backticks):
{{"subject": "Asunto del email", "body": "Cuerpo completo en Markdown..."}}"""

    async def stream_email():
        full_content = ""
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                async with client.stream(
                    "POST",
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model": "groq/compound",
                        "stream": True,
                        "temperature": 0.85,
                        "max_tokens": 2000,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": f"Genera el email #{day_number}."}
                        ]
                    }
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.startswith("data: "):
                            chunk = line[6:]
                            if chunk == "[DONE]":
                                break
                            try:
                                delta = json.loads(chunk)["choices"][0]["delta"].get("content","")
                                if delta:
                                    full_content += delta
                                    yield f"data: {json.dumps({'delta': delta})}\n\n"
                            except Exception:
                                pass

            # Parsear JSON del contenido completo
            clean = re.sub(r'^```(?:json)?\s*', '', full_content.strip())
            clean = re.sub(r'\s*```$', '', clean)
            # Intentar extraer JSON si hay texto alrededor
            json_match = re.search(r'\{[\s\S]*"subject"[\s\S]*"body"[\s\S]*\}', clean)
            if json_match:
                clean = json_match.group(0)
            parsed = json.loads(clean)
            subject = parsed.get("subject","")
            body    = parsed.get("body","")

            db3 = get_db()
            try:
                db3.execute(text("""
                    UPDATE campaign_emails
                    SET subject=:subject, body=:body,
                        regenerated_count=CASE WHEN subject!='' AND subject IS NOT NULL THEN regenerated_count+1 ELSE regenerated_count END
                    WHERE campaign_id=:cid AND day_number=:day
                """), {"subject": subject, "body": body, "cid": cid, "day": day_number})
                db3.commit()
            finally:
                db3.close()

            yield f"data: {json.dumps({'done': True, 'subject': subject, 'body': body})}\n\n"

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error en generate-one: {e} | content: {full_content[:200]}")
            yield f"data: {json.dumps({'error': f'Error parseando respuesta de IA: {e}'})}\n\n"
        except Exception as e:
            logger.error(f"Stream error en generate-one: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        stream_email(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
    )


@app.post("/campaigns/generate")
async def generate_campaign(request: Request, _: str = Depends(require_auth)):
    """
    Genera la secuencia completa de emails para una campaña usando Groq.
    Llama a Groq desde el backend usando GROQ_API_KEY de env vars.
    """
    import httpx
    data = await request.json()

    contacts_list = data.get("contacts", [])
    start_date    = data.get("start_date", "")
    end_date      = data.get("end_date", "")
    intent        = data.get("intent", "").strip()
    send_mode     = data.get("send_mode", "daily")
    tone          = data.get("tone", "informative")
    send_time     = data.get("send_time", "09:00")
    campaign_name = data.get("name", f"Campaña {start_date}")

    if not contacts_list: raise HTTPException(400, "Se requiere al menos un contacto")
    if not start_date or not end_date: raise HTTPException(400, "Fechas requeridas")
    if not intent: raise HTTPException(400, "Intención requerida")

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        raise HTTPException(500, "GROQ_API_KEY no configurada en variables de entorno")

    # Calcular fechas válidas
    dates = _get_campaign_dates(start_date, end_date, send_mode)
    if not dates:
        raise HTTPException(400, f"No hay fechas válidas en el rango con modo '{send_mode}'")

    n = len(dates)

    # Contexto de entidad
    entity_ctx = ""
    db_ctx = get_db()
    try:
        row = dict_from_row(db_ctx.execute(text("SELECT value FROM settings WHERE key='ctx_entity'")).fetchone())
        if row: entity_ctx += f"\nEntidad: {row['value']}"
        row2 = dict_from_row(db_ctx.execute(text("SELECT value FROM settings WHERE key='ctx_mission'")).fetchone())
        if row2: entity_ctx += f"\nMisión: {row2['value']}"
    except Exception:
        pass
    finally:
        db_ctx.close()

    tone_desc = {
        "aggressive": "tono persuasivo y directo, CTA fuerte, urgencia, beneficios concretos",
        "informative": "tono educativo y de construcción de confianza, informativo, suave",
    }.get(tone, "tono profesional")

    mode_desc = {
        "daily": "diariamente",
        "alternate": "día por medio",
        "mon_wed_fri": "lunes, miércoles y viernes",
        "tue_thu": "martes y jueves",
    }.get(send_mode, send_mode)

    system_prompt = f"""Eres un experto en email marketing. Vas a generar una secuencia de {n} emails para una campaña.

INTENCIÓN: {intent}
TONO: {tone_desc}
FRECUENCIA: emails enviados {mode_desc}
FECHAS: {dates[0]} al {dates[-1]}
{entity_ctx}

REGLAS:
- Genera exactamente {n} emails numerados
- Mantén coherencia narrativa progresiva entre emails
- Varía el ángulo y CTA en cada email
- Usa formato Markdown: **negrita**, *italica*, ## títulos, listas con -
- Cada email debe tener asunto y cuerpo distintos
- NO incluyas encabezados como Para:/De:/Asunto: en el cuerpo

Responde ÚNICAMENTE con JSON válido con esta estructura exacta (sin texto extra, sin markdown):
{{
  "emails": [
    {{
      "day_number": 1,
      "subject": "Asunto del email 1",
      "body": "Cuerpo en Markdown..."
    }}
  ]
}}"""

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={
                    "model": "groq/compound",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Genera la secuencia de {n} emails para la campaña. Responde solo con el JSON."}
                    ],
                    "temperature": 0.8,
                    "max_tokens": 8000,
                }
            )
        resp.raise_for_status()
        groq_data = resp.json()
        raw_content = groq_data["choices"][0]["message"]["content"].strip()
        # Limpiar posibles backticks de markdown
        raw_content = re.sub(r'^```(?:json)?\s*', '', raw_content)
        raw_content = re.sub(r'\s*```$', '', raw_content)
        emails_data = json.loads(raw_content)
        emails_list = emails_data.get("emails", [])
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Groq API error: {e.response.status_code}")
    except json.JSONDecodeError as e:
        raise HTTPException(502, f"Error parseando respuesta de Groq: {e}")
    except Exception as e:
        raise HTTPException(502, f"Error generando campaña: {e}")

    if len(emails_list) != n:
        # Rellenar o truncar si Groq no respetó el count exacto
        while len(emails_list) < n:
            emails_list.append({"day_number": len(emails_list)+1, "subject": f"Email {len(emails_list)+1}", "body": "Contenido pendiente de regeneración."})
        emails_list = emails_list[:n]

    # Guardar en DB
    db = get_db()
    try:
        result = db.execute(text("""
            INSERT INTO campaigns (name, intent, start_date, end_date, send_mode, tone, status)
            VALUES (:name,:intent,:start_date,:end_date,:send_mode,:tone,'draft')
            RETURNING id
        """), {"name": campaign_name, "intent": intent, "start_date": start_date,
               "end_date": end_date, "send_mode": send_mode, "tone": tone})
        camp_id = result.fetchone()[0]

        for i, em in enumerate(emails_list):
            db.execute(text("""
                INSERT INTO campaign_emails (campaign_id, day_number, subject, body, status, scheduled_at, version, regenerated_count)
                VALUES (:cid,:day,:subject,:body,'pending',:scheduled_at,1,0)
            """), {
                "cid": camp_id,
                "day": em.get("day_number", i+1),
                "subject": em.get("subject",""),
                "body": em.get("body",""),
                "scheduled_at": f"{dates[i] if i < len(dates) else dates[-1]} {send_time}",
            })

        for email_addr in contacts_list:
            email_addr = email_addr.strip()
            if email_addr:
                db.execute(text("INSERT INTO campaign_contacts (campaign_id, email) VALUES (:cid,:email)"),
                           {"cid": camp_id, "email": email_addr})

        db.commit()
        logger.info(f"Campaña {camp_id} creada con {n} emails")
        return {"success": True, "campaign_id": camp_id, "total_emails": n, "dates": dates}
    except Exception as e:
        db.rollback()
        logger.error(f"Error guardando campaña: {e}")
        raise HTTPException(500, f"Error guardando en DB: {e}")
    finally:
        db.close()

@app.post("/campaigns/{cid}/approve-email")
async def approve_email(cid: int, request: Request, _: str = Depends(require_auth)):
    data     = await request.json()
    email_id = data.get("email_id")
    day_num  = data.get("day_number")
    action   = data.get("action", "approved")
    db = get_db()
    try:
        if email_id:
            db.execute(text("UPDATE campaign_emails SET status=:status WHERE id=:id AND campaign_id=:cid"),
                       {"status": action, "id": email_id, "cid": cid})
        elif day_num:
            db.execute(text("UPDATE campaign_emails SET status=:status WHERE day_number=:day AND campaign_id=:cid"),
                       {"status": action, "day": day_num, "cid": cid})
        else:
            raise HTTPException(400, "Se requiere email_id o day_number")
        db.commit()
        return {"success": True}
    finally:
        db.close()

@app.post("/campaigns/{cid}/retry-email")
async def retry_email(cid: int, request: Request, _: str = Depends(require_auth)):
    """Regenera un email específico de la campaña manteniendo contexto y posición."""
    import httpx
    data     = await request.json()
    email_id = data.get("email_id")
    feedback = data.get("feedback", "")

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        raise HTTPException(500, "GROQ_API_KEY no configurada")

    db = get_db()
    try:
        em = dict_from_row(db.execute(text(
            "SELECT e.*, c.intent, c.tone, c.send_mode, c.start_date, c.end_date FROM campaign_emails e JOIN campaigns c ON c.id=e.campaign_id WHERE e.id=:id AND e.campaign_id=:cid"),
            {"id": email_id, "cid": cid}).fetchone())
        if not em:
            raise HTTPException(404, "Email no encontrado")

        # Contexto de emails ya generados para coherencia
        others = rows_to_list(db.execute(text(
            "SELECT day_number, subject, body FROM campaign_emails WHERE campaign_id=:cid AND id!=:id ORDER BY day_number"),
            {"cid": cid, "id": email_id}).fetchall())

        context_summary = "\n".join([f"- Email {o['day_number']}: {o['subject']}" for o in others[:5]])

        tone_desc = {
            "aggressive": "tono persuasivo y directo, CTA fuerte, urgencia",
            "informative": "tono educativo, informativo, construcción de confianza",
        }.get(em.get("tone","informative"), "tono profesional")

        prompt = f"""Estás regenerando el email #{em['day_number']} de una secuencia de campaña.

INTENCIÓN DE LA CAMPAÑA: {em['intent']}
TONO: {tone_desc}
POSICIÓN: email {em['day_number']} de la secuencia
OTROS EMAILS EN LA CAMPAÑA:
{context_summary}

EMAIL ANTERIOR (a mejorar):
Asunto: {em['subject']}
Cuerpo: {em['body']}

{'FEEDBACK DEL USUARIO: ' + feedback if feedback else ''}

Genera un nuevo email mejorado para esta posición. Responde SOLO con JSON:
{{"subject": "Nuevo asunto", "body": "Nuevo cuerpo en Markdown"}}"""

        # Edicion manual: feedback con formato "EDIT:subject|||body"
        if feedback.startswith("EDIT:") and "|||" in feedback:
            parts = feedback[5:].split("|||", 1)
            subj_edit, body_edit = parts[0], parts[1]
            db_e = get_db()
            try:
                db_e.execute(text("UPDATE campaign_emails SET subject=:s,body=:b WHERE id=:id"),
                    {"s": subj_edit, "b": body_edit, "id": email_id})
                db_e.commit()
            finally:
                db_e.close()
            return {"success": True, "subject": subj_edit, "body": body_edit}

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={"model": "groq/compound",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.9, "max_tokens": 2000}
            )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        new_data = json.loads(raw)

        db.execute(text("""
            UPDATE campaign_emails
            SET subject=:subject, body=:body, status='pending',
                version=version+1, regenerated_count=regenerated_count+1
            WHERE id=:id
        """), {"subject": new_data.get("subject",""), "body": new_data.get("body",""), "id": email_id})
        db.commit()
        return {"success": True, "subject": new_data.get("subject",""), "body": new_data.get("body","")}
    except json.JSONDecodeError:
        raise HTTPException(502, "Error parseando respuesta de Groq")
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()

@app.post("/campaigns/{cid}/finalize")
async def finalize_campaign(cid: int, _: str = Depends(require_auth)):
    """Valida que todos los emails estén aprobados y marca la campaña como scheduled."""
    db = get_db()
    try:
        pending = db.execute(text(
            "SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:cid AND status='pending'"),
            {"cid": cid}).fetchone()
        if dict_from_row(pending)["n"] > 0:
            raise HTTPException(400, "Hay emails pendientes de aprobación")

        rejected = db.execute(text(
            "SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:cid AND status='rejected'"),
            {"cid": cid}).fetchone()
        if dict_from_row(rejected)["n"] > 0:
            raise HTTPException(400, "Hay emails rechazados — regeneralos antes de finalizar")

        db.execute(text("UPDATE campaigns SET status='scheduled' WHERE id=:id"), {"id": cid})
        db.commit()
        return {"success": True, "message": "Campaña programada exitosamente"}
    finally:
        db.close()

@app.post("/campaigns/{cid}/send-now")
async def send_campaign_now(cid: int, _: str = Depends(require_auth)):
    """Envía todos los emails aprobados de la campaña inmediatamente."""
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            raise HTTPException(404, "Campaña no encontrada")

        emails = rows_to_list(db.execute(text(
            "SELECT * FROM campaign_emails WHERE campaign_id=:id AND status='approved' ORDER BY day_number"),
            {"id": cid}).fetchall())
        contacts_rows = rows_to_list(db.execute(text(
            "SELECT email FROM campaign_contacts WHERE campaign_id=:id"), {"id": cid}).fetchall())
        contact_emails = [r["email"] for r in contacts_rows]

        if not emails:
            raise HTTPException(400, "No hay emails aprobados para enviar")
        if not contact_emails:
            raise HTTPException(400, "No hay contactos en la campaña")

        c = cfg()
        style_cfg = _build_style()
        results = []

        now_dt = datetime.utcnow()
        for em in emails:
            body_html = build_html_email(em["body"], style_cfg)
            email_ok = False
            for to in contact_emails:
                try:
                    send_resend(to, em["subject"], em["body"], body_html)
                    results.append({"email_day": em["day_number"], "to": to, "ok": True})
                    email_ok = True
                except Exception as e:
                    results.append({"email_day": em["day_number"], "to": to, "ok": False, "error": str(e)})
            if email_ok:
                db.execute(text("UPDATE campaign_emails SET sent_at=:now, send_status='sent' WHERE id=:id"),
                           {"now": now_dt.isoformat(), "id": em["id"]})

        db.execute(text("UPDATE campaigns SET status='sent' WHERE id=:id"), {"id": cid})
        db.commit()
        sent_ok = sum(1 for r in results if r["ok"])
        return {"success": True, "sent": sent_ok, "failed": len(results)-sent_ok, "results": results}
    finally:
        db.close()

# ─────────────────────────────────────────────
# ROUTES — FRONTEND SPA
# ─────────────────────────────────────────────
@app.head("/")
async def head_frontend():
    return JSONResponse({})

@app.get("/")
async def serve_frontend():
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH, media_type="text/html")
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if any(full_path.startswith(p) for p in API_PATHS):
        raise HTTPException(404, "Not found")
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH, media_type="text/html")
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("api:app", host="0.0.0.0", port=port)

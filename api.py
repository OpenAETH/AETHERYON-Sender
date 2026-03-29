"""
Asistente Ejecutivo — Deploy Render
Auth: JWT session  |  IMAP sync (delete-aware) |  Envio multiple
Envio: Resend API (resend.com)
"""
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import os, imaplib, email as email_lib
import re as _re
import socket as _socket
import psycopg2
import psycopg2.extras
from email.header import decode_header
from datetime import datetime
import uvicorn, logging, re, secrets, hashlib, hmac, json, time
import resend
from urllib.parse import urlparse, unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG desde ENV (Render Dashboard)
# ─────────────────────────────────────────────
def cfg():
    return {
        "resend_api_key": os.getenv("RESEND_API_KEY", ""),
        "sender_email":   os.getenv("SENDER_EMAIL", os.getenv("SMTP_USER", "")),
        "sender_name":    os.getenv("SENDER_NAME", ""),
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
    logger.warning("SECRET_KEY no configurada")
TOKEN_TTL    = int(os.getenv("TOKEN_TTL_HOURS", "8")) * 3600

IS_HTTPS = os.getenv("RENDER", "") != "" or os.getenv("COOKIE_SECURE", "").lower() == "true"
BASE = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─────────────────────────────────────────────
# DATABASE — PostgreSQL / Supabase
# ─────────────────────────────────────────────
def _get_ipv4_address(hostname):
    """Resuelve hostname a IPv4 exclusivamente"""
    try:
        addrinfo = _socket.getaddrinfo(hostname, None, _socket.AF_INET, _socket.SOCK_STREAM)
        if addrinfo:
            ipv4 = addrinfo[0][4][0]
            logger.info(f"IPv4 para {hostname}: {ipv4}")
            return ipv4
        return None
    except Exception as e:
        logger.error(f"Error resolviendo {hostname}: {e}")
        return None

def _make_connection():
    """
    Crea conexión a Supabase con SSL y usuario correcto.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    
    # Parsear URL
    parsed = urlparse(DATABASE_URL)
    raw_user = unquote(parsed.username) if parsed.username else ''
    raw_password = unquote(parsed.password) if parsed.password else ''
    host = parsed.hostname
    port = parsed.port or 5432
    dbname = parsed.path.lstrip('/')
    
    # CORRECCIÓN CRÍTICA: Para Supabase, extraer el usuario base (sin el sufijo .project_ref)
    # Si el usuario tiene formato "usuario.proyecto", usar solo "usuario"
    if '.' in raw_user:
        db_user = raw_user.split('.')[0]
        logger.info(f"Usuario original: {raw_user} → usando usuario base: {db_user}")
    else:
        db_user = raw_user
    
    logger.info(f"Conectando a {host}:{port} como {db_user} a DB {dbname}")
    
    # Obtener IPv4
    ipv4 = _get_ipv4_address(host)
    
    # Intentar conexiones
    # Opción 1: Conexión directa con hostname (mejor para SSL)
    try:
        logger.info(f"Intentando conexión a {host}:{port} con SSL")
        conn = psycopg2.connect(
            host = host,
            port = port,
            user = db_user,
            password = raw_password,
            dbname = dbname,
            sslmode = 'require',
            connect_timeout = 15,
            cursor_factory = psycopg2.extras.RealDictCursor,
        )
        # Verificar SSL
        cur = conn.cursor()
        cur.execute("SELECT ssl_is_used() as ssl_used, version() as version")
        result = cur.fetchone()
        cur.close()
        logger.info(f"✅ Conexión exitosa! SSL activo: {result['ssl_used']}")
        return conn
    except Exception as e:
        logger.warning(f"Conexión directa falló: {e}")
    
    # Opción 2: Con IPv4 forzado
    if ipv4:
        try:
            logger.info(f"Intentando conexión con hostaddr={ipv4}")
            conn = psycopg2.connect(
                host = host,
                hostaddr = ipv4,
                port = port,
                user = db_user,
                password = raw_password,
                dbname = dbname,
                sslmode = 'require',
                connect_timeout = 15,
                cursor_factory = psycopg2.extras.RealDictCursor,
            )
            cur = conn.cursor()
            cur.execute("SELECT ssl_is_used()")
            cur.fetchone()
            cur.close()
            logger.info("✅ Conexión exitosa vía IPv4!")
            return conn
        except Exception as e:
            logger.warning(f"Conexión IPv4 falló: {e}")
    
    # Opción 3: Pooler de Supabase (puerto 6543)
    if 'supabase.co' in host:
        pooler_host = f"aws-0-us-east-1.pooler.supabase.com"
        pooler_ipv4 = _get_ipv4_address(pooler_host)
        if pooler_ipv4:
            try:
                logger.info(f"Intentando pooler {pooler_host}:6543")
                conn = psycopg2.connect(
                    host = pooler_host,
                    hostaddr = pooler_ipv4,
                    port = 6543,
                    user = db_user,
                    password = raw_password,
                    dbname = dbname,
                    sslmode = 'require',
                    connect_timeout = 15,
                    cursor_factory = psycopg2.extras.RealDictCursor,
                )
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
                logger.info("✅ Conexión exitosa vía pooler!")
                return conn
            except Exception as e:
                logger.warning(f"Pooler falló: {e}")
    
    raise RuntimeError(f"No se pudo conectar a {host}:{port} como {db_user}")

def get_db():
    return _make_connection()

def init_db():
    """Inicializa tablas si no existen"""
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Verificar SSL
        cur.execute("SELECT ssl_is_used() as ssl_used, current_database() as db, version() as version")
        info = cur.fetchone()
        logger.info(f"🔒 SSL activo: {info['ssl_used']}, DB: {info['db']}")
        
        # Crear tablas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            email      TEXT NOT NULL UNIQUE,
            company    TEXT,
            role       TEXT,
            phone      TEXT,
            context    TEXT,
            tags       TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS email_logs (
            id            SERIAL PRIMARY KEY,
            direction     TEXT NOT NULL,
            contact_email TEXT,
            contact_name  TEXT,
            subject       TEXT,
            body          TEXT,
            body_html     TEXT,
            intent        TEXT,
            status        TEXT DEFAULT 'sent',
            sent_at       TIMESTAMPTZ,
            received_at   TIMESTAMPTZ,
            replied_at    TIMESTAMPTZ,
            message_id    TEXT,
            thread_id     TEXT,
            campaign_id   TEXT,
            ai_suggestion TEXT,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS memory (
            id         SERIAL PRIMARY KEY,
            type       TEXT NOT NULL,
            entity     TEXT,
            content    TEXT NOT NULL,
            importance INTEGER DEFAULT 1,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inbox_cache (
            id            SERIAL PRIMARY KEY,
            message_id    TEXT UNIQUE NOT NULL,
            imap_uid      TEXT,
            from_email    TEXT,
            from_name     TEXT,
            subject       TEXT,
            body          TEXT,
            date          TEXT,
            read          INTEGER DEFAULT 0,
            replied       INTEGER DEFAULT 0,
            ai_suggestion TEXT,
            fetched_at    TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            created_at BIGINT NOT NULL,
            expires_at BIGINT NOT NULL
        )
        """)
        
        conn.commit()
        logger.info("✅ Tablas creadas/verificadas")
        
        # Datos demo
        cur.execute("SELECT COUNT(*) as c FROM contacts")
        if cur.fetchone()['c'] == 0:
            cur.execute("""
            INSERT INTO contacts (name, email, company) 
            VALUES ('Demo Contact', 'demo@example.com', 'Demo Company')
            ON CONFLICT (email) DO NOTHING
            """)
            conn.commit()
            logger.info("✅ Datos demo insertados")
        
        cur.close()
        conn.close()
        logger.info("✅ Base de datos inicializada correctamente")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error en init_db: {e}")
        raise

# ─────────────────────────────────────────────
# AUTH (con fallback en memoria)
# ─────────────────────────────────────────────
_in_memory_sessions = {}

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
        
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT expires_at FROM sessions WHERE token=%s AND expires_at>%s", (token, int(time.time())))
            row = cur.fetchone()
            cur.close()
            conn.close()
            return row is not None
        except Exception:
            # Fallback a memoria
            session = _in_memory_sessions.get(token)
            return session and session['expires_at'] > int(time.time())
    except Exception:
        return False

async def require_auth(request: Request):
    token = request.cookies.get("session") or request.headers.get("X-Session-Token", "")
    if not token or not verify_token(token):
        raise HTTPException(401, "No autorizado")
    return token

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
_in_memory_settings = {}

def get_setting(key: str, default: str = "") -> str:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row["value"] if row else default
    except Exception:
        return _in_memory_settings.get(key, default)

def set_setting(key: str, value: str):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("INSERT INTO settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        _in_memory_settings[key] = value

# ─────────────────────────────────────────────
# MARKDOWN → HTML
# ─────────────────────────────────────────────
def md_to_html(text: str) -> str:
    text = text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*\*(.+?)\*\*\*', r'<strong><em>\1</em></strong>', text)
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    
    lines = text.split('\n')
    result, in_ul = [], False
    for line in lines:
        if re.match(r'^[-*•] (.+)', line):
            if not in_ul:
                result.append('<ul>')
                in_ul = True
            result.append(f'<li>{re.sub(r"^[-*•] ","",line)}</li>')
        else:
            if in_ul:
                result.append('</ul>')
                in_ul = False
            if line.strip():
                result.append(f'<p>{line}</p>')
    if in_ul:
        result.append('</ul>')
    return '\n'.join(result)

def build_html_email(body_text: str, style_cfg: dict = None) -> str:
    s = style_cfg or {}
    header_bg = s.get("header_bg", "#0c0f0a")
    header_fc = s.get("header_color", "#7ec850")
    bg = s.get("bg_color", "#ffffff")
    text_col = s.get("text_color", "#1a1a1a")
    sname = s.get("sender_name", cfg()["sender_name"])
    sig_html = s.get("signature_html", get_setting("signature_html", ""))
    
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Email</title></head>
<body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f0f0f0">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f0f0;padding:20px">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:{bg};border-radius:10px">
<tr><td style="background:{header_bg};padding:20px 30px;border-radius:10px 10px 0 0">
<span style="color:{header_fc};font-size:20px;font-weight:bold">{sname or ""}</span></td></tr>
<tr><td style="padding:30px;color:{text_col};line-height:1.6">{md_to_html(body_text)}</td></tr>
{f'<tr><td style="padding:20px 30px;border-top:1px solid #eee"><div style="color:#666">{sig_html}</div></td></tr>' if sig_html else ''}
<tr><td style="background:#f8f8f8;padding:15px;text-align:center;color:#999;font-size:11px;border-radius:0 0 10px 10px">
Enviado desde <strong>{sname or "Asistente"}</strong></td></tr>
</table></td></tr></table></body></html>"""

# ─────────────────────────────────────────────
# RESEND SEND
# ─────────────────────────────────────────────
def send_resend(to: str, subject: str, body_plain: str, body_html: str):
    c = cfg()
    if not c["resend_api_key"]:
        raise ValueError("RESEND_API_KEY no configurada")
    if not c["sender_email"]:
        raise ValueError("SENDER_EMAIL no configurada")
    resend.api_key = c["resend_api_key"]
    from_addr = f"{c['sender_name']} <{c['sender_email']}>" if c["sender_name"] else c["sender_email"]
    return resend.Emails.send({"from": from_addr, "to": [to], "subject": subject, "html": body_html, "text": body_plain})

# ─────────────────────────────────────────────
# IMAP SYNC
# ─────────────────────────────────────────────
def decode_str(s):
    if not s: return ""
    parts = decode_header(s)
    result = []
    for part, enc in parts:
        result.append(part.decode(enc or "utf-8", errors="replace") if isinstance(part, bytes) else str(part))
    return "".join(result)

def fetch_inbox_sync(limit=60):
    c = cfg()
    if not c["imap_host"] or not c["imap_user"]:
        return {"error": "IMAP no configurado"}
    try:
        mail = imaplib.IMAP4_SSL(c["imap_host"], c["imap_port"])
        mail.login(c["imap_user"], c["imap_pass"])
        mail.select("INBOX")
        _, uid_data = mail.uid("SEARCH", None, "ALL")
        server_uids = set(uid_data[0].decode().split()) if uid_data[0] else set()
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT message_id, imap_uid FROM inbox_cache")
        db_rows = cur.fetchall()
        db_by_uid = {r["imap_uid"]: r["message_id"] for r in db_rows if r["imap_uid"]}
        
        # Eliminar mensajes ya no existentes
        deleted = 0
        for uid in set(db_by_uid.keys()) - server_uids:
            cur.execute("DELETE FROM inbox_cache WHERE message_id=%s", (db_by_uid[uid],))
            deleted += 1
        if deleted:
            conn.commit()
        
        # Agregar nuevos
        added = 0
        for uid in list(server_uids)[-limit:]:
            _, msg_data = mail.uid("FETCH", uid, "(RFC822)")
            if not msg_data or not msg_data[0]: continue
            msg = email_lib.message_from_bytes(msg_data[0][1])
            mid = msg.get("Message-ID", "").strip() or f"uid-{uid}"
            
            cur.execute("SELECT 1 FROM inbox_cache WHERE message_id=%s", (mid,))
            if cur.fetchone(): continue
            
            from_ = decode_str(msg.get("From", ""))
            from_email = from_.split("<")[-1].replace(">", "").strip() if "<" in from_ else from_.strip()
            cur.execute("INSERT INTO inbox_cache (message_id, imap_uid, from_email, from_name, subject, body, date) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                       (mid, uid, from_email, decode_str(msg.get("From", ""))[:100], decode_str(msg.get("Subject", ""))[:200], "", msg.get("Date", "")))
            conn.commit()
            added += 1
        
        cur.close()
        conn.close()
        mail.logout()
        return {"added": added, "deleted": deleted}
    except Exception as e:
        logger.error(f"IMAP error: {e}")
        return {"error": str(e)}

# ─────────────────────────────────────────────
# APP LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    try:
        init_db()
        logger.info("✅ App iniciada con DB")
    except Exception as e:
        logger.error(f"⚠️ Error DB: {e}")
    yield
    logger.info("App cerrada")

app = FastAPI(title="Asistente Ejecutivo API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

# ─────────────────────────────────────────────
# FRONTEND
# ─────────────────────────────────────────────
def find_index():
    paths = [os.path.join(BASE, "static", "index.html"), os.path.join(BASE, "index.html"), "/opt/render/project/src/static/index.html"]
    for p in paths:
        if os.path.exists(p):
            return p
    return None

INDEX_PATH = find_index()
static_dir = os.path.join(BASE, "static")
os.makedirs(static_dir, exist_ok=True)
if os.path.exists(os.path.join(BASE, "index.html")) and not os.path.exists(os.path.join(static_dir, "index.html")):
    import shutil
    shutil.copy2(os.path.join(BASE, "index.html"), os.path.join(static_dir, "index.html"))
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

API_PATHS = ["auth", "contacts", "inbox", "send-email", "settings", "context", "preview-email", "logs", "memory", "supervision", "stats", "smtp-test", "config", "api", "health"]

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.post("/auth/login")
async def login(request: Request):
    data = await request.json()
    if data.get("username") != APP_USER or data.get("password") != APP_PASSWORD:
        raise HTTPException(401, "Credenciales incorrectas")
    
    token = make_token()
    now = int(time.time())
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE expires_at<%s", (now,))
        cur.execute("INSERT INTO sessions (token, created_at, expires_at) VALUES (%s,%s,%s)", (token, now, now + TOKEN_TTL))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        _in_memory_sessions[token] = {'expires_at': now + TOKEN_TTL}
    
    resp = JSONResponse({"success": True, "token": token})
    resp.set_cookie("session", token, httponly=True, samesite="lax", max_age=TOKEN_TTL, secure=IS_HTTPS)
    return resp

@app.post("/auth/logout")
async def logout(request: Request):
    token = request.cookies.get("session")
    if token:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions WHERE token=%s", (token,))
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            _in_memory_sessions.pop(token, None)
    resp = JSONResponse({"success": True})
    resp.delete_cookie("session")
    return resp

@app.get("/auth/check")
async def auth_check(token: str = Depends(require_auth)):
    return {"ok": True, "user": APP_USER}

@app.get("/health")
async def health():
    db_ok = False
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT ssl_is_used(), current_database()")
        row = cur.fetchone()
        db_ok = True
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Health check: {e}")
    return {"status": "healthy" if db_ok else "degraded", "database": "connected" if db_ok else "disconnected"}

@app.get("/api/status")
def status(_: str = Depends(require_auth)):
    c = cfg()
    return {"status": "OK", "smtp_user": c["sender_email"] or "NO", "imap_host": c["imap_host"] or "NO", "provider": "resend"}

@app.get("/config")
def config(_: str = Depends(require_auth)):
    c = cfg()
    return {"provider": "resend", "sender_email": c["sender_email"], "sender_name": c["sender_name"], "resend_key_set": bool(c["resend_api_key"]), "imap_host": c["imap_host"]}

@app.get("/settings")
def settings(_: str = Depends(require_auth)):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT key, value FROM settings")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return _in_memory_settings

@app.post("/settings")
async def save_settings(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    for k, v in data.items():
        set_setting(k, str(v))
    return {"success": True}

@app.get("/context")
def context(_: str = Depends(require_auth)):
    return {"entity": get_setting("ctx_entity"), "mission": get_setting("ctx_mission")}

@app.post("/context")
async def save_context(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    for k in ("entity", "mission"):
        if k in data:
            set_setting(f"ctx_{k}", data[k])
    return {"success": True}

@app.get("/contacts")
def contacts(_: str = Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM contacts ORDER BY name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/contacts")
async def create_contact(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO contacts (name, email, company, role, phone, context, tags) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                   (data["name"], data["email"], data.get("company",""), data.get("role",""), data.get("phone",""), data.get("context",""), data.get("tags","")))
        cid = cur.fetchone()["id"]
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(409, "Email ya existe")
    finally:
        cur.close()
        conn.close()
    return {"success": True, "id": cid}

@app.put("/contacts/{cid}")
async def update_contact(cid: int, request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE contacts SET name=%s, company=%s, role=%s, phone=%s, context=%s, tags=%s, updated_at=NOW() WHERE id=%s",
               (data.get("name"), data.get("company",""), data.get("role",""), data.get("phone",""), data.get("context",""), data.get("tags",""), cid))
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}

@app.delete("/contacts/{cid}")
def delete_contact(cid: int, _: str = Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM contacts WHERE id=%s", (cid,))
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}

def _build_style():
    return {"header_bg": get_setting("style_header_bg","#0c0f0a"), "header_color": get_setting("style_header_color","#7ec850"), "bg_color": get_setting("style_bg_color","#ffffff"), "text_color": get_setting("style_text_color","#1a1a1a"), "sender_name": cfg()["sender_name"], "signature_html": get_setting("signature_html","")}

@app.post("/send-email")
async def send_email(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    recipients = data.get("recipients") or ([data.get("to")] if data.get("to") else [])
    recipients = [r.strip() for r in recipients if r.strip()]
    if not recipients:
        raise HTTPException(400, "Destinatario requerido")
    if not data.get("subject") or not data.get("body"):
        raise HTTPException(400, "Subject y body requeridos")
    
    style = _build_style()
    body_html = build_html_email(data["body"], style)
    results = []
    
    for to in recipients:
        try:
            send_resend(to, data["subject"], data["body"], body_html)
            results.append({"to": to, "ok": True})
        except Exception as e:
            results.append({"to": to, "ok": False, "error": str(e)})
    
    return {"success": True, "sent": len([r for r in results if r["ok"]]), "failed": len([r for r in results if not r["ok"]]), "results": results}

@app.get("/inbox")
def inbox(refresh: bool = False, _: str = Depends(require_auth)):
    sync = fetch_inbox_sync(60) if refresh else None
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM inbox_cache ORDER BY date DESC LIMIT 80")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"messages": [dict(r) for r in rows], "sync": sync}

@app.get("/stats")
def stats(_: str = Depends(require_auth)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT (SELECT COUNT(*) FROM contacts) as contacts, (SELECT COUNT(*) FROM email_logs WHERE direction='out') as sent, (SELECT COUNT(*) FROM inbox_cache) as inbox, (SELECT COUNT(*) FROM memory) as memory")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)

@app.head("/")
async def head():
    from fastapi.responses import Response
    return Response(status_code=200)

@app.get("/")
async def root():
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH)
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

@app.get("/{full_path:path}")
async def spa(full_path: str):
    if full_path.startswith(tuple(API_PATHS)) or full_path in API_PATHS:
        raise HTTPException(404)
    if any(full_path.endswith(ext) for ext in ['.js', '.css', '.png', '.jpg', '.svg', '.ico']):
        raise HTTPException(404)
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH)
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        logger.error(f"Error DB inicial: {e}")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("api:app", host="0.0.0.0", port=port)

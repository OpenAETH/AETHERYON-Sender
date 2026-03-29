"""
Asistente Ejecutivo — Deploy Render
Auth: JWT session  |  IMAP sync (delete-aware)  |  Envio multiple
Envio: Resend API (resend.com)
"""
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import os, imaplib, email as email_lib
import psycopg2
import psycopg2.extras
from email.header import decode_header
from datetime import datetime, timedelta
import uvicorn, logging, re, secrets, hashlib, hmac, json, time
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
        "sender_email":   os.getenv("SENDER_EMAIL", os.getenv("SMTP_USER", "")),  # fallback a SMTP_USER por compatibilidad
        "sender_name":    os.getenv("SENDER_NAME", ""),
        # IMAP (sin cambios)
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
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─────────────────────────────────────────────
# DATABASE — PostgreSQL / Supabase
# ─────────────────────────────────────────────
def get_db():
    """Retorna una conexión psycopg2 con cursor de tipo RealDictCursor
    (equivalente a sqlite3.Row: acceso por nombre de columna)."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está configurada en las variables de entorno")
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn

def init_db():
    """Crea las tablas si no existen. El schema completo debe aplicarse
    previamente en Supabase ejecutando schema.sql desde el SQL Editor."""
    conn = get_db()
    cur  = conn.cursor()
    try:
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
        logger.info("DB lista (PostgreSQL/Supabase)")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error en init_db: {e}")
        raise
    finally:
        cur.close()
        conn.close()

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
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            "SELECT expires_at FROM sessions WHERE token=%s AND expires_at>%s",
            (token, int(time.time()))
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        return row is not None
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
def get_setting(key: str, default: str = "") -> str:
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        (key, value)
    )
    conn.commit(); cur.close(); conn.close()

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
            '</td></tr></table></td></tr>'
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
        # Wrapper externo
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"'
        ' style="background-color:#f0f0f0;padding:32px 16px">'
        '<tr><td align="center">'
        # Contenedor principal 600px
        '<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"'
        ' style="max-width:600px;width:100%">'

        # ── HEADER ──
        '<tr>'
        '<td style="background-color:' + header_bg + ';border-radius:10px 10px 0 0;padding:24px 36px">'
        '<span style="font-family:Georgia,serif;font-size:21px;font-weight:700;'
        'color:' + header_fc + ';letter-spacing:-0.3px;line-height:1">'
        + (sname or "") +
        '</span>'
        '</td>'
        '</tr>'

        # ── BODY ──
        '<tr>'
        '<td style="background-color:' + bg + ';padding:36px 36px 28px 36px;'
        'font-family:' + font + ';font-size:' + fsize + ';color:' + text_col + ';line-height:1.75">'
        + body_html +
        '</td>'
        '</tr>'

        # ── FIRMA (opcional) ──
        + sig_block +

        # ── FOOTER ──
        '<tr>'
        '<td style="background-color:#f8f8f8;border-radius:0 0 10px 10px;'
        'border-top:1px solid #e4e4e4;padding:16px 36px">'
        '<p style="margin:0;font-size:12px;color:#aaa;font-family:Arial,sans-serif">'
        'Enviado desde <strong style="color:#888">' + footer_name + '</strong>.'
        '</p>'
        '</td>'
        '</tr>'

        '</table>'  # fin contenedor 600px
        '</td></tr>'
        '</table>'  # fin wrapper
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
    # SendResponse es un objeto con atributo .id, no un dict
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
    """
    Sincroniza bandeja con IMAP:
    - Agrega mensajes nuevos
    - Elimina de DB los que ya no existen en IMAP
    - Preserva ai_suggestion y replied si el mensaje sigue existiendo
    """
    c = cfg()
    if not c["imap_host"] or not c["imap_user"]: return {"added":0,"deleted":0,"error":"IMAP no configurado"}
    try:
        mail = imaplib.IMAP4_SSL(c["imap_host"], c["imap_port"])
        mail.login(c["imap_user"], c["imap_pass"])
        mail.select("INBOX")

        # Obtener todos los UID actuales del servidor
        _, uid_data = mail.uid("SEARCH", None, "ALL")
        server_uids = set(uid_data[0].decode().split()) if uid_data[0] else set()

        conn = get_db()
        cur  = conn.cursor()

        # Obtener message_ids actuales en DB con su uid
        cur.execute("SELECT message_id, imap_uid FROM inbox_cache")
        db_rows = cur.fetchall()
        db_by_uid = {r["imap_uid"]: r["message_id"] for r in db_rows if r["imap_uid"]}
        db_message_ids = {r["message_id"] for r in db_rows}

        # Detectar UIDs eliminados del servidor y borrarlos de DB
        deleted_uids = set(db_by_uid.keys()) - server_uids
        deleted_count = 0
        for uid in deleted_uids:
            mid = db_by_uid[uid]
            cur.execute("DELETE FROM inbox_cache WHERE message_id=%s", (mid,))
            deleted_count += 1
        if deleted_count:
            conn.commit()
            logger.info(f"IMAP sync: {deleted_count} mensajes eliminados de DB")

        # Fetch de los ultimos N mensajes del servidor
        uids_to_fetch = list(server_uids)[-limit:]
        added_count = 0

        for uid in reversed(uids_to_fetch):
            _, msg_data = mail.uid("FETCH", uid, "(RFC822)")
            if not msg_data or not msg_data[0]: continue
            raw = msg_data[0][1]
            msg = email_lib.message_from_bytes(raw)
            mid   = msg.get("Message-ID","").strip()
            if not mid: mid = f"uid-{uid}"

            # Ya existe en DB — skip
            if mid in db_message_ids: continue

            subj  = decode_str(msg.get("Subject",""))
            from_ = decode_str(msg.get("From",""))
            date_ = msg.get("Date","")
            from_email, from_name = "", ""
            if "<" in from_:
                pts = from_.split("<")
                from_name  = pts[0].strip().strip('"')
                from_email = pts[1].replace(">","").strip()
            else:
                from_email = from_.strip()
                from_name  = from_email.split("@")[0]
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
                cur.execute(
                    "INSERT INTO inbox_cache (message_id,imap_uid,from_email,from_name,subject,body,date) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (message_id) DO NOTHING",
                    (mid, uid, from_email, from_name, subj, body[:3000], date_))
                conn.commit()
                added_count += 1
            except Exception as e:
                conn.rollback()
                logger.error(f"Error insertando msg {mid}: {e}")

        cur.close(); conn.close()
        mail.logout()
        logger.info(f"IMAP sync: +{added_count} nuevos, -{deleted_count} eliminados")
        return {"added": added_count, "deleted": deleted_count, "total": len(server_uids)}
    except Exception as e:
        logger.error(f"IMAP sync error: {e}")
        return {"added":0,"deleted":0,"error":str(e)}

# ─────────────────────────────────────────────
# APP LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    init_db()
    yield

app = FastAPI(title="Asistente Ejecutivo API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                   allow_credentials=True)

# ─────────────────────────────────────────────
# BUSCAR EL FRONTEND EN MÚLTIPLES UBICACIONES
# ─────────────────────────────────────────────
def find_index_html():
    """Busca index.html en múltiples ubicaciones posibles"""
    posibles_ubicaciones = [
        os.path.join(BASE, "static", "index.html"),
        os.path.join(BASE, "index.html"),
        os.path.join(os.path.dirname(BASE), "static", "index.html"),
        "/opt/render/project/src/static/index.html",  # Ruta típica en Render
        "/app/static/index.html",  # Otra ruta común
    ]
    
    for ubicacion in posibles_ubicaciones:
        if os.path.exists(ubicacion):
            logger.info(f"Frontend encontrado en: {ubicacion}")
            return ubicacion
    
    logger.warning("No se encontró index.html en ninguna ubicación")
    return None

INDEX_PATH = find_index_html()

# Crear directorio static si no existe
static_dir = os.path.join(BASE, "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)
    logger.info(f"Creado directorio static: {static_dir}")

# Si el archivo index.html está en la raíz, copiarlo a static/
index_root = os.path.join(BASE, "index.html")
if os.path.exists(index_root) and not os.path.exists(os.path.join(static_dir, "index.html")):
    import shutil
    shutil.copy2(index_root, os.path.join(static_dir, "index.html"))
    logger.info(f"Copiado {index_root} a {static_dir}/")
    INDEX_PATH = os.path.join(static_dir, "index.html")

# Montar archivos estáticos si el directorio existe
if os.path.exists(static_dir) and os.listdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    logger.info(f"Directorio static montado: {static_dir}")

# Lista de rutas de API que NO deben ser servidas como frontend
API_PATHS = ["auth", "contacts", "inbox", "send-email", "settings", "context", 
             "preview-email", "logs", "memory", "supervision", "stats", 
             "smtp-test", "config", "api"]

# ─────────────────────────────────────────────
# ROUTES — AUTH (sin autenticación para login)
# ─────────────────────────────────────────────
@app.post("/auth/login")
async def login(request: Request):
    data = await request.json()
    user = data.get("username","").strip()
    pw   = data.get("password","")
    if user != APP_USER or pw != APP_PASSWORD:
        raise HTTPException(401, "Credenciales incorrectas")
    token = make_token()
    now   = int(time.time())
    conn  = get_db()
    cur   = conn.cursor()
    # limpiar sesiones expiradas
    cur.execute("DELETE FROM sessions WHERE expires_at<%s", (now,))
    cur.execute("INSERT INTO sessions (token,created_at,expires_at) VALUES (%s,%s,%s)",
                (token, now, now + TOKEN_TTL))
    conn.commit(); cur.close(); conn.close()
    logger.info(f"Login OK: user={user!r} | IS_HTTPS={IS_HTTPS}")
    resp = JSONResponse({"success": True, "token": token})
    resp.set_cookie(
        "session", token,
        httponly=True,
        samesite="lax",
        max_age=TOKEN_TTL,
        secure=IS_HTTPS,   # True en Render (HTTPS), False en local
    )
    return resp

@app.post("/auth/logout")
async def logout(request: Request):
    token = request.cookies.get("session","")
    if token:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE token=%s", (token,))
        conn.commit(); cur.close(); conn.close()
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
        "smtp_user":    c["sender_email"] or "NO CONFIG",   # smtp_user por compatibilidad con el frontend
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
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT key,value FROM settings")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return {r["key"]: r["value"] for r in rows}

@app.post("/settings")
async def save_settings(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    for k, v in data.items():
        set_setting(k, str(v))
    return {"success": True}

@app.get("/context")
def get_context(_: str = Depends(require_auth)):
    return {"entity": get_setting("ctx_entity"), "mission": get_setting("ctx_mission"), "extra": get_setting("ctx_extra")}

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
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM contacts ORDER BY name")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.post("/contacts")
async def create_contact(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    if not data.get("name") or not data.get("email"):
        raise HTTPException(400, "name y email son obligatorios")
    conn = get_db()
    cur  = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO contacts (name,email,company,role,phone,context,tags) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (data["name"],data["email"],data.get("company",""),data.get("role",""),
             data.get("phone",""),data.get("context",""),data.get("tags",""))
        )
        cid = cur.fetchone()["id"]
        cur.execute("INSERT INTO memory (type,entity,content) VALUES (%s,%s,%s)",
            ("contact_added",data["email"],f"Contacto: {data['name']} - {data.get('company','')}"))
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(409, "Email ya existe")
    finally:
        cur.close(); conn.close()
    return {"success": True, "id": cid}

@app.put("/contacts/{cid}")
async def update_contact(cid: int, request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE contacts SET name=%s,company=%s,role=%s,phone=%s,context=%s,tags=%s,updated_at=NOW() WHERE id=%s",
        (data.get("name"),data.get("company",""),data.get("role",""),
         data.get("phone",""),data.get("context",""),data.get("tags",""),cid))
    conn.commit(); cur.close(); conn.close()
    return {"success": True}

@app.delete("/contacts/{cid}")
def delete_contact(cid: int, _: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM contacts WHERE id=%s", (cid,))
    conn.commit(); cur.close(); conn.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — SEND (simple + multiple)
# ─────────────────────────────────────────────
def _build_style():
    c = cfg()
    return {
        "primary_color":  get_setting("style_primary_color","#7ec850"),
        "bg_color":       get_setting("style_bg_color","#ffffff"),
        "text_color":     get_setting("style_text_color","#1a1a1a"),
        "font_family":    get_setting("style_font_family","Georgia,'Times New Roman',serif"),
        "font_size":      get_setting("style_font_size","16px"),
        "link_color":     get_setting("style_link_color","#2563eb"),
        "header_bg":      get_setting("style_header_bg","#0c0f0a"),
        "header_color":   get_setting("style_header_color","#7ec850"),
        "sender_name":    c["sender_name"],
        "signature_html": get_setting("signature_html",""),
    }

def _log_sent(conn, cur, to, subject, body, body_html, intent, campaign_id):
    cur.execute("SELECT name FROM contacts WHERE email=%s", (to,))
    row = cur.fetchone()
    cname = row["name"] if row else to.split("@")[0]
    cur.execute(
        "INSERT INTO email_logs (direction,contact_email,contact_name,subject,body,body_html,intent,status,sent_at,campaign_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)",
        ("out",to,cname,subject,body,body_html,intent,"sent",campaign_id))
    cur.execute("INSERT INTO memory (type,entity,content,importance) VALUES (%s,%s,%s,%s)",
        ("email_sent",to,f"Email enviado a {cname}: {subject}",2))

@app.post("/send-email")
async def send_email(request: Request, _: str = Depends(require_auth)):
    """Envia a uno o multiples destinatarios."""
    data    = await request.json()
    # Acepta "to" (string) o "recipients" (lista)
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
        raise HTTPException(500, "RESEND_API_KEY no configurada — agregala en las variables de entorno de Render")
    if not c["sender_email"]:
        raise HTTPException(500, "SENDER_EMAIL no configurada — agregala en las variables de entorno de Render")

    style_cfg = _build_style()
    body_html = build_html_email(body, style_cfg)

    results = []
    conn = get_db()
    cur  = conn.cursor()
    for to in recipients:
        try:
            resp = send_resend(to, subject, body, body_html, reply_to)
            email_id = resp.id if hasattr(resp, "id") else "?"
            _log_sent(conn, cur, to, subject, body, body_html, intent, campaign_id)
            results.append({"to": to, "ok": True, "resend_id": email_id})
            logger.info(f"Email enviado via Resend a {to} | resend_id={email_id}")
        except resend.exceptions.ResendError as e:
            logger.error(f"Resend API error → {to}: code={e.code} msg={e.message}")
            results.append({"to": to, "ok": False, "error": f"Resend error {e.code}: {e.message}"})
        except Exception as e:
            logger.error(f"Error inesperado → {to}: {type(e).__name__}: {e}")
            results.append({"to": to, "ok": False, "error": f"{type(e).__name__}: {e}"})
    try:
        conn.commit()
    except Exception as e:
        logger.error(f"Error log DB: {e}")
    finally:
        cur.close(); conn.close()

    sent_ok  = [r for r in results if r["ok"]]
    sent_err = [r for r in results if not r["ok"]]
    if not sent_ok and sent_err:
        raise HTTPException(500, sent_err[0]["error"])
    return {"success": True, "sent": len(sent_ok), "failed": len(sent_err), "results": results}

@app.get("/smtp-test")
async def smtp_test(_: str = Depends(require_auth)):
    """Verifica que la API key de Resend sea válida enviando un ping a la API."""
    c = cfg()
    if not c["resend_api_key"]:
        raise HTTPException(500, "RESEND_API_KEY no configurada")
    if not c["sender_email"]:
        raise HTTPException(500, "SENDER_EMAIL no configurada")
    try:
        resend.api_key = c["resend_api_key"]
        # Validar la key listando dominios (no envía nada, solo autentica)
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
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM inbox_cache ORDER BY date DESC LIMIT 80")
    rows = cur.fetchall()
    cur.close(); conn.close()
    resp = [dict(r) for r in rows]
    if result: return {"messages": resp, "sync": result}
    return {"messages": resp, "sync": None}

@app.post("/inbox/mark-replied")
async def mark_replied(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("UPDATE inbox_cache SET replied=1 WHERE message_id=%s", (data.get("message_id"),))
    conn.commit(); cur.close(); conn.close()
    return {"success": True}

@app.post("/inbox/save-suggestion")
async def save_suggestion(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("UPDATE inbox_cache SET ai_suggestion=%s WHERE message_id=%s",
        (data.get("suggestion",""), data.get("message_id")))
    conn.commit(); cur.close(); conn.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — LOGS
# ─────────────────────────────────────────────
@app.get("/logs")
def get_logs(limit: int = 100, _: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM email_logs ORDER BY created_at DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.get("/logs/{log_id}")
def get_log(log_id: int, _: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM email_logs WHERE id=%s", (log_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row: raise HTTPException(404, "Log no encontrado")
    return dict(row)

# ─────────────────────────────────────────────
# ROUTES — MEMORY
# ─────────────────────────────────────────────
@app.get("/memory")
def get_memory(limit: int = 100, _: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM memory ORDER BY created_at DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.post("/memory")
async def add_memory(request: Request, _: str = Depends(require_auth)):
    data = await request.json()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("INSERT INTO memory (type,entity,content,importance) VALUES (%s,%s,%s,%s)",
        (data.get("type","manual"),data.get("entity",""),data.get("content",""),data.get("importance",1)))
    conn.commit(); cur.close(); conn.close()
    return {"success": True}

# ─────────────────────────────────────────────
# ROUTES — SUPERVISION
# ─────────────────────────────────────────────
@app.get("/supervision")
def get_supervision(_: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""SELECT l.*, c.company FROM email_logs l
        LEFT JOIN contacts c ON l.contact_email=c.email
        WHERE l.direction='out' ORDER BY l.sent_at DESC LIMIT 100""")
    sent = cur.fetchall()
    cur.execute("SELECT from_email FROM inbox_cache WHERE replied=1")
    replied_set = {r["from_email"] for r in cur.fetchall()}
    result = []
    for row in sent:
        d = dict(row)
        try:
            sent_at_val = d.get("sent_at")
            if sent_at_val:
                # sent_at es TIMESTAMPTZ → puede ser datetime o string
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
    cur.close(); conn.close()
    return result

# ─────────────────────────────────────────────
# ROUTES — STATS
# ─────────────────────────────────────────────
@app.get("/stats")
def get_stats(_: str = Depends(require_auth)):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM contacts");          contacts = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM email_logs WHERE direction='out'"); sent = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM inbox_cache");       inbox = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM inbox_cache WHERE replied=1"); replied = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM memory");            memory = cur.fetchone()["c"]
    cur.close(); conn.close()
    return {"contacts": contacts, "sent": sent, "inbox": inbox, "replied": replied, "memory": memory}

# ─────────────────────────────────────────────
# SERVIR FRONTEND (sin autenticación)
# ─────────────────────────────────────────────
@app.head("/")
async def head_frontend():
    """Health-check de Render — responde 200 a HEAD /"""
    from fastapi.responses import Response
    return Response(status_code=200)

@app.get("/")
async def serve_frontend():
    """Sirve el frontend SPA sin autenticación"""
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH)
    
    # Intentar servir desde la raíz como fallback
    root_index = os.path.join(BASE, "index.html")
    if os.path.exists(root_index):
        return FileResponse(root_index)
    
    logger.error(f"No se pudo encontrar index.html. INDEX_PATH={INDEX_PATH}, BASE={BASE}")
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    """Sirve el frontend SPA para cualquier ruta no-API"""
    # Excluir rutas de API explícitamente
    if full_path.startswith(tuple(API_PATHS)) or full_path in API_PATHS:
        raise HTTPException(404, "Not found")
    
    # Excluir también rutas con extensión de archivo estático
    if any(full_path.endswith(ext) for ext in ['.js', '.css', '.png', '.jpg', '.svg', '.ico', '.json']):
        raise HTTPException(404, "Not found")
    
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH)
    
    # Intentar servir desde la raíz como fallback
    root_index = os.path.join(BASE, "index.html")
    if os.path.exists(root_index):
        return FileResponse(root_index)
    
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("api:app", host="0.0.0.0", port=port)
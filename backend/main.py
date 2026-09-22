"""
backend/main.py — AETHERYON Outreach Sender.

Punto de entrada único de la app: arma el FastAPI, levanta el Cron Sender
en background (workers/scheduler.py), sirve el frontend (SPA) y monta
todos los routers de api/.

Correr en desarrollo:   uvicorn backend.main:app --reload
Correr en producción:   uvicorn backend.main:app --host 0.0.0.0 --port $PORT
                         (un solo worker — ver Dockerfile: con >1 worker el
                          Cron Sender correría duplicado y mandaría cada
                          email más de una vez)
"""
import asyncio
import logging
import os
import shutil
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from backend.api import agenda, ai, auth, composition, inbox, sender, settings, supervision, templates
from backend.config import BASE
from backend.db.connection import check_schema, get_db, init_db
from backend.workers.scheduler import scheduler_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_scheduler_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler_task
    try:
        init_db()
        check_schema()
        logger.info("Aplicación iniciada correctamente")

        # Al arrancar: solo informar cuántos envíos están demorados, NO
        # dispararlos. El Cron Sender los toma en su próximo ciclo (o el
        # usuario puede forzarlos con "Enviar ahora").
        try:
            db = get_db()
            try:
                delayed = db.execute(text(
                    "SELECT COUNT(*) as n FROM send_queue WHERE status='pending' AND next_attempt_at <= NOW()"
                )).fetchone()
                n = delayed[0] if delayed else 0
                if n > 0:
                    logger.warning(f"[startup] {n} envío(s) demorados en la cola. El Cron Sender los procesará en su próximo ciclo.")
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"[startup] No se pudo verificar la cola de envío: {e}")

        _scheduler_task = asyncio.create_task(scheduler_loop())
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {e}")
    yield
    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    logger.info("Aplicación cerrada")


app = FastAPI(title="AETHERYON Outreach Sender", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc: Exception):
    """Red de seguridad: sin esto, un error no anticipado (tabla/columna
    faltante en la base, caida de conexion, etc.) sale de Starlette como
    texto plano ("Internal Server Error"), y el frontend — que siempre
    espera JSON — revienta con un 'Unexpected token... is not valid JSON'
    que esconde el error real. Acá se loguea el traceback completo
    (visible en los logs de Render) y se devuelve JSON siempre, con el
    tipo de excepcion para poder diagnosticar sin exponer detalles internos.
    Un caso puntual se detecta y explica solo: tabla o columna faltante en
    la base (schema.sql desactualizado en Supabase), el error real que
    disparó esto varias veces."""
    logger.exception(f"Error no manejado en {request.method} {request.url.path}: {exc}")
    exc_text = str(exc)
    if "UndefinedTable" in exc_text or "UndefinedColumn" in exc_text or "does not exist" in exc_text:
        detail = (
            "Falta una tabla o columna en la base de datos — el esquema de tu Supabase "
            "está desactualizado. Corré backend/db/schema.sql completo en el SQL Editor "
            "de Supabase (es idempotente, no borra datos existentes) y reintentá."
        )
    else:
        detail = f"Error interno ({type(exc).__name__}). Revisá los logs del servidor para más detalle."
    return JSONResponse(status_code=500, content={"detail": detail})

app.include_router(auth.router)
app.include_router(settings.router)
app.include_router(agenda.router)
app.include_router(composition.router)
app.include_router(templates.router)
app.include_router(sender.router)
app.include_router(supervision.router)
app.include_router(inbox.router)
app.include_router(ai.router)


@app.get("/__build")
async def build_info():
    """Verificación rápida de deploy — sin login, para confirmar con un
    simple curl/GET que el servidor está corriendo el build esperado
    (no una versión anterior cacheada por Docker o por un deploy viejo)."""
    from backend.config import APP_BUILD
    return JSONResponse({"build": APP_BUILD}, headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

# ─────────────────────────────────────────────
# FRONTEND (SPA) — index.html vive en la raíz del repo, se copia a static/
# ─────────────────────────────────────────────
static_dir = os.path.join(BASE, "static")
os.makedirs(static_dir, exist_ok=True)

index_root = os.path.join(BASE, "index.html")
static_index = os.path.join(static_dir, "index.html")
if os.path.exists(index_root):
    needs_copy = not os.path.exists(static_index) or os.path.getmtime(index_root) > os.path.getmtime(static_index)
    if needs_copy:
        shutil.copy2(index_root, static_index)
        logger.info("index.html copiado a static/ (actualizado)")


def _find_index_html():
    for loc in [static_index, index_root, "/opt/render/project/src/static/index.html", "/app/static/index.html"]:
        if os.path.exists(loc):
            logger.info(f"Frontend: {loc}")
            return loc
    return None


INDEX_PATH = _find_index_html()

if os.path.exists(static_dir) and os.listdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

API_PATHS = [
    "auth", "contacts", "agenda", "inbox", "send-email", "send-test", "settings", "context",
    "preview-email", "logs", "memory", "supervision", "stats", "smtp-test", "config", "api",
    "campaigns", "templates", "ai", "schedule", "process-scheduled", "upload", "attachments",
]


@app.head("/")
async def head_frontend():
    return JSONResponse({})


@app.get("/")
async def serve_frontend():
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if any(full_path.startswith(p) for p in API_PATHS):
        raise HTTPException(404, "Not found")
    if INDEX_PATH and os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})
    return JSONResponse({"error": "Frontend no encontrado"}, status_code=404)


if __name__ == "__main__":
    import uvicorn
    init_db()
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("backend.main:app", host="0.0.0.0", port=port)

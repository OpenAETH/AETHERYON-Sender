"""
db/connection.py — Conexión a PostgreSQL (Supabase) exclusivamente.

Único punto de acceso a la base de datos. AETHERYON Outreach Sender NO usa
SQLite bajo ninguna circunstancia: toda persistencia vive en Postgres.

Preserva la lógica original de conexión IPv4 forzada (`_make_ipv4_connection`),
necesaria porque el pooler de Supabase puede resolver a IPv6 en entornos cloud
sin soporte completo, causando `connection refused` o timeouts.
"""
import re
import socket
import logging

import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

from backend.config import DATABASE_URL

logger = logging.getLogger(__name__)

engine = None
SessionLocal = None


def _parse_db_url(url: str) -> dict:
    """Descompone DATABASE_URL en sus partes."""
    m = re.match(r"postgresql://([^:]+):([^@]+)@([^:/]+):?(\d+)?/(.+)", url)
    if not m:
        raise ValueError(f"DATABASE_URL con formato inválido: {url!r}")
    return {
        "user": m.group(1),
        "password": m.group(2),
        "host": m.group(3),
        "port": int(m.group(4) or 5432),
        "dbname": m.group(5).split("?")[0],
    }


def _make_ipv4_connection():
    """
    Crea una conexión psycopg2 forzando IPv4.
    - 'host' lleva el hostname real → psycopg2/libpq lo usa como SNI en TLS
      (Supabase necesita el SNI para identificar el tenant)
    - 'hostaddr' lleva la IP IPv4 resuelta → libpq conecta a esa IP directamente,
      sin hacer DNS lookup (que podría devolver IPv6)
    """
    p = _parse_db_url(DATABASE_URL)
    try:
        addrinfo = socket.getaddrinfo(p["host"], p["port"], socket.AF_INET, socket.SOCK_STREAM)
        if not addrinfo:
            raise RuntimeError(f"No se pudo resolver {p['host']} a IPv4")
        ipv4 = addrinfo[0][4][0]
        logger.info(f"Resolviendo {p['host']} a IPv4: {ipv4}")
        conn = psycopg2.connect(
            host=p["host"],
            hostaddr=ipv4,
            port=p["port"],
            user=p["user"],
            password=p["password"],
            dbname=p["dbname"],
            sslmode="require",
            connect_timeout=10,
        )
        logger.info("Conexión a base de datos establecida exitosamente vía IPv4")
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise


def create_db_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está configurada en las variables de entorno")
    return create_engine(
        DATABASE_URL,
        creator=_make_ipv4_connection,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def init_db():
    """Inicializa la conexión (no crea tablas — ver db/schema.sql)."""
    global engine, SessionLocal
    try:
        engine = create_db_engine()
        SessionLocal = scoped_session(sessionmaker(bind=engine))
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        logger.info("Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"Error en init_db: {e}")
        raise


def get_db():
    """Retorna una sesión de SQLAlchemy. El llamador es responsable de cerrarla."""
    if SessionLocal is None:
        raise RuntimeError("Base de datos no inicializada")
    return SessionLocal()


def dict_from_row(row):
    """Convierte una fila de SQLAlchemy a diccionario. None si row es None."""
    if row is None:
        return None
    return dict(row._mapping)


def rows_to_list(rows):
    """Convierte una lista de filas de SQLAlchemy a lista de diccionarios."""
    return [dict(r._mapping) for r in rows]


# Tablas que backend/db/schema.sql debe haber creado. Se chequea al arrancar
# para detectar de una un schema desactualizado en Supabase, en vez de
# esperar a que un usuario dispare el error usando la app (ej: "templates"
# o "send_queue" no existen porque nunca se corrió el schema.sql mas
# reciente contra esa base).
EXPECTED_TABLES = [
    "sessions", "settings", "contacts", "contact_interactions", "attachments",
    "email_logs", "email_attachments", "inbox_cache", "memory", "templates",
    "campaigns", "campaign_emails", "campaign_contacts", "campaign_attachments", "send_queue",
]


def check_schema():
    """Loguea una advertencia bien visible si falta alguna tabla esperada.
    No frena el arranque de la app (por si el chequeo mismo falla en algun
    entorno raro) — es un diagnostico, no un gate."""
    try:
        db = get_db()
        try:
            rows = db.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'")).fetchall()
            existing = {r[0] for r in rows}
            missing = [t for t in EXPECTED_TABLES if t not in existing]
            if missing:
                logger.warning(
                    "\n" + "=" * 70 +
                    f"\n⚠️  FALTAN TABLAS EN LA BASE DE DATOS: {', '.join(missing)}\n"
                    "    La app va a fallar (error 500) al intentar usarlas.\n"
                    "    Corré backend/db/schema.sql COMPLETO en el SQL Editor de tu\n"
                    "    Supabase — es idempotente, no borra datos existentes — y\n"
                    "    reiniciá el servicio.\n" + "=" * 70
                )
            else:
                logger.info("Esquema de base de datos OK — todas las tablas esperadas existen.")
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"No se pudo verificar el esquema de la base de datos: {e}")

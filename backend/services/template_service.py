"""
services/template_service.py — Plantillas HTML reutilizables (Redacción +
Templates). Crear, editar, duplicar, activar, archivar y reutilizar, con
variables {{first_name}} {{company}} {{role}} {{sender_name}} {{cta_url}}.

Cada plantilla tiene un `content_type`:
- 'markdown' (por defecto): cuerpo en Markdown liviano, AETHERYON lo
  envuelve en su plantilla visual (header/firma/footer).
- 'html': un template ya diseñado por completo (pegado por el usuario) —
  se respeta tal cual al enviar, sin reprocesar.
"""
import json
import logging

from sqlalchemy import text

from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain.templates import extract_variables, render, build_context
from backend.domain.email_content import assemble
from backend.services.settings_service import build_style

logger = logging.getLogger(__name__)


def _with_variables(row: dict) -> dict:
    if row is None:
        return None
    if isinstance(row.get("variables"), str):
        try:
            row["variables"] = json.loads(row["variables"])
        except Exception:
            row["variables"] = []
    return row


def _norm_content_type(data: dict) -> str:
    ct = (data.get("content_type") or "markdown").strip().lower()
    return ct if ct in ("markdown", "html") else "markdown"


def list_templates(status: str = None) -> list:
    db = get_db()
    try:
        if status:
            rows = db.execute(
                text("SELECT * FROM templates WHERE status=:status ORDER BY updated_at DESC"), {"status": status}
            ).fetchall()
        else:
            rows = db.execute(text("SELECT * FROM templates ORDER BY updated_at DESC")).fetchall()
        return [_with_variables(r) for r in rows_to_list(rows)]
    finally:
        db.close()


def get_template(tid: int) -> dict:
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("SELECT * FROM templates WHERE id=:id"), {"id": tid}).fetchone())
        if not row:
            raise ValueError("Plantilla no encontrada")
        return _with_variables(row)
    finally:
        db.close()


def create_template(data: dict) -> int:
    name = (data.get("name") or "").strip()
    if not name:
        raise ValueError("name es obligatorio")
    subject = data.get("subject", "") or ""
    body = data.get("body", "") or ""
    content_type = _norm_content_type(data)
    variables = extract_variables(subject, body)

    db = get_db()
    try:
        result = db.execute(text("""
            INSERT INTO templates (name, subject, body, content_type, variables, category, status, default_cta_url)
            VALUES (:name, :subject, :body, :content_type, :variables, :category, 'active', :cta)
            RETURNING id
        """), {
            "name": name, "subject": subject, "body": body, "content_type": content_type,
            "variables": json.dumps(variables), "category": data.get("category", ""),
            "cta": data.get("default_cta_url", ""),
        })
        tid = result.fetchone()[0]
        db.commit()
        return tid
    finally:
        db.close()


def update_template(tid: int, data: dict):
    subject = data.get("subject", "") or ""
    body = data.get("body", "") or ""
    content_type = _norm_content_type(data)
    variables = extract_variables(subject, body)
    db = get_db()
    try:
        db.execute(text("""
            UPDATE templates SET
                name=:name, subject=:subject, body=:body, content_type=:content_type, variables=:variables,
                category=:category, default_cta_url=:cta, updated_at=NOW()
            WHERE id=:id
        """), {
            "id": tid, "name": data.get("name"), "subject": subject, "body": body, "content_type": content_type,
            "variables": json.dumps(variables), "category": data.get("category", ""),
            "cta": data.get("default_cta_url", ""),
        })
        db.commit()
    finally:
        db.close()


def set_status(tid: int, status: str):
    if status not in ("active", "archived"):
        raise ValueError("status debe ser 'active' o 'archived'")
    db = get_db()
    try:
        db.execute(text("UPDATE templates SET status=:status, updated_at=NOW() WHERE id=:id"), {"status": status, "id": tid})
        db.commit()
    finally:
        db.close()


def duplicate_template(tid: int, new_name: str = None) -> int:
    db = get_db()
    try:
        row = dict_from_row(db.execute(text("SELECT * FROM templates WHERE id=:id"), {"id": tid}).fetchone())
        if not row:
            raise ValueError("Plantilla no encontrada")
        name = (new_name or f"{row['name']} (copia)").strip()
        result = db.execute(text("""
            INSERT INTO templates (name, subject, body, content_type, variables, category, status, default_cta_url, source_template_id)
            VALUES (:name, :subject, :body, :content_type, :variables, :category, 'active', :cta, :src)
            RETURNING id
        """), {
            "name": name, "subject": row["subject"], "body": row["body"], "content_type": row.get("content_type", "markdown"),
            "variables": row["variables"], "category": row.get("category", ""), "cta": row.get("default_cta_url", ""), "src": tid,
        })
        new_id = result.fetchone()[0]
        db.commit()
        return new_id
    finally:
        db.close()


def delete_template(tid: int):
    db = get_db()
    try:
        db.execute(text("DELETE FROM templates WHERE id=:id"), {"id": tid})
        db.commit()
    finally:
        db.close()


def preview_template(tid: int, contact: dict = None, cta_url: str = None) -> dict:
    """Renderiza una plantilla con variables resueltas, para vista previa."""
    tmpl = get_template(tid)
    return render_content(tmpl["subject"], tmpl["body"], contact, cta_url or tmpl.get("default_cta_url", ""), tmpl.get("content_type", "markdown"))


def render_content(subject: str, body: str, contact: dict = None, cta_url: str = "", content_type: str = "markdown") -> dict:
    """Renderiza asunto + cuerpo con variables resueltas para un contacto
    puntual, y devuelve también el HTML final del email (respetando el modo
    markdown/html)."""
    from backend.config import cfg
    style = build_style()
    sender_name = style.get("sender_name") or cfg()["sender_name"]
    context = build_context(contact, sender_name=sender_name, cta_url=cta_url)

    rendered_subject, missing_subj = render(subject or "", context)
    rendered_body, missing_body = render(body or "", context)
    html = assemble(rendered_body, content_type, style)

    missing = sorted(set(missing_subj) | set(missing_body))
    return {
        "subject": rendered_subject,
        "body": rendered_body,
        "html": html,
        "missing_variables": missing,
    }

"""
domain/templates.py — Variables de plantillas ({{first_name}}, {{company}}, ...).

Funciones puras: no acceden a la base de datos. `services/template_service.py`
y `services/composition_service.py` construyen el contexto (a partir de un
contacto y de la configuración del remitente) y llaman a `render()`.

Variables reconocidas explícitamente por el producto:
  {{first_name}}  {{company}}  {{role}}  {{sender_name}}  {{cta_url}}
Cualquier otra {{clave}} presente en el texto también se sustituye si está
en el contexto provisto; si no, se reemplaza por vacío (nunca se manda una
comunicación con "{{...}}" literal a un destinatario real).
"""
import re

_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")

KNOWN_VARIABLES = ["first_name", "company", "role", "sender_name", "cta_url"]


def extract_variables(*texts: str) -> list:
    """Devuelve las variables {{clave}} usadas (sin duplicados, en orden de
    aparición) a través de uno o más textos (ej: asunto + cuerpo)."""
    seen = []
    for text in texts:
        if not text:
            continue
        for m in _VAR_RE.finditer(text):
            key = m.group(1).lower()
            if key not in seen:
                seen.append(key)
    return seen


def render(text: str, context: dict) -> tuple:
    """Sustituye {{clave}} por su valor en `context` (case-insensitive).
    Devuelve (texto_renderizado, variables_sin_valor)."""
    if not text:
        return text, []
    ctx_lower = {k.lower(): ("" if v is None else str(v)) for k, v in (context or {}).items()}
    missing = []

    def _sub(m):
        key = m.group(1).lower()
        if key in ctx_lower:
            return ctx_lower[key]
        missing.append(key)
        return ""

    rendered = _VAR_RE.sub(_sub, text)
    return rendered, missing


def build_context(contact: dict = None, sender_name: str = "", cta_url: str = "", extra: dict = None) -> dict:
    """Arma el contexto de variables para un destinatario. `contact` es un
    registro de la Agenda (puede ser None si el destinatario no está
    guardado — ej. una dirección suelta cargada a mano)."""
    contact = contact or {}
    name = (contact.get("name") or "").strip()
    email = (contact.get("email") or "").strip()
    first_name = name.split()[0] if name else (email.split("@")[0] if email else "")
    ctx = {
        "first_name": first_name,
        "company": contact.get("company") or "",
        "role": contact.get("role") or "",
        "sender_name": sender_name or "",
        "cta_url": cta_url or "",
    }
    if extra:
        ctx.update(extra)
    return ctx

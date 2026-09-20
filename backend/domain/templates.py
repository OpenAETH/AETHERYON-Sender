"""
domain/templates.py — Variables de plantillas.

Funciones puras: no acceden a la base de datos. `services/template_service.py`,
`services/composition_service.py` y `services/sender_service.py` construyen
el contexto (a partir de un contacto, la configuración del remitente, y
valores personalizados por destinatario) y llaman a `render()`.

Dos categorías de variable:
- "Auto-mapeadas": se resuelven solas desde la Agenda o la configuración
  (nombre, empresa, rol, remitente, link de acción). Se reconocen por
  alias — un template puede usar {{nombre}}, {{first_name}} o {{name}}
  indistintamente, todas terminan resolviendo al mismo dato.
- "Personalizadas": cualquier otra {{clave}} (ej. {{hipotesis}}, {{señal}}).
  No hay forma de derivarlas de un contacto genérico — el valor se carga a
  mano, por destinatario, al programar el envío (ver Cron Sender).
"""
import re
import unicodedata

_VAR_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}", re.UNICODE)

# canonical -> alias (todos en minuscula, sin acentos — la comparacion se
# hace normalizando el texto de entrada, asi {{NOMBRE}}/{{Nombre}} tambien matchean).
FIELD_ALIASES = {
    "first_name": ["first_name", "nombre", "name"],
    "company": ["company", "empresa", "nombre_empresa", "compania"],
    "role": ["role", "cargo", "puesto", "rol"],
    "sender_name": ["sender_name", "remitente", "firma"],
    "cta_url": ["cta_url", "link", "url"],
}
# alias -> canonical, para lookup O(1)
_ALIAS_TO_CANONICAL = {alias: canon for canon, aliases in FIELD_ALIASES.items() for alias in aliases}


def _strip_accents(s: str) -> str:
    """'señal' -> 'senal', 'compañía' -> 'compania' — para que la Ñ/acentos
    no rompan el matcheo de alias (pero SI se preservan tal cual en el
    nombre de variable real, solo se usa para comparar)."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _canonical_for(var_name: str):
    """Devuelve el campo canonico (first_name/company/...) si `var_name` es
    un alias reconocido, o None si es una variable personalizada."""
    key = _strip_accents((var_name or "").strip().lower())
    return _ALIAS_TO_CANONICAL.get(key)


def is_auto_mapped(var_name: str) -> bool:
    return _canonical_for(var_name) is not None


def extract_variables(*texts: str) -> list:
    """Devuelve las variables {{clave}} usadas (sin duplicados, en orden de
    aparición) a través de uno o más textos (ej: asunto + cuerpo). Acepta
    letras con acentos/ñ — {{señal}} se detecta correctamente."""
    seen = []
    for text in texts:
        if not text:
            continue
        for m in _VAR_RE.finditer(text):
            key = m.group(1)
            key_lower = key.lower()
            if key_lower not in [s.lower() for s in seen]:
                seen.append(key_lower)
    return seen


def classify_variables(variables: list) -> list:
    """Para cada variable de un template, indica si se auto-completa desde
    la Agenda/configuración o si necesita carga manual por destinatario.
    Usado por la UI para saber que pedir en el asistente de programación."""
    result = []
    for v in variables or []:
        canon = _canonical_for(v)
        result.append({"name": v, "auto": canon is not None, "maps_to": canon})
    return result


def render(text: str, context: dict) -> tuple:
    """Sustituye {{clave}} por su valor en `context` (case/acento-insensitive).
    Devuelve (texto_renderizado, variables_sin_valor)."""
    if not text:
        return text, []
    ctx_norm = {_strip_accents(k.lower()): ("" if v is None else str(v)) for k, v in (context or {}).items()}
    missing = []

    def _sub(m):
        key = _strip_accents(m.group(1).lower())
        if key in ctx_norm:
            return ctx_norm[key]
        missing.append(m.group(1).lower())
        return ""

    rendered = _VAR_RE.sub(_sub, text)
    return rendered, missing


def build_context(contact: dict = None, sender_name: str = "", cta_url: str = "", extra: dict = None) -> dict:
    """Arma el contexto de variables para un destinatario. `contact` es un
    registro de la Agenda (puede ser None si el destinatario no está
    guardado). Cada campo auto-mapeado se expone bajo TODOS sus alias
    ({{nombre}} y {{first_name}} resuelven al mismo valor), y `extra`
    (valores personalizados cargados a mano para este destinatario en esta
    comunicación, ej. {{hipotesis}}, {{señal}}) tiene prioridad sobre todo
    lo demás."""
    contact = contact or {}
    name = (contact.get("name") or "").strip()
    email = (contact.get("email") or "").strip()
    first_name = name.split()[0] if name else (email.split("@")[0] if email else "")

    resolved = {
        "first_name": first_name,
        "company": contact.get("company") or "",
        "role": contact.get("role") or "",
        "sender_name": sender_name or "",
        "cta_url": cta_url or "",
    }
    ctx = {}
    for canon, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            ctx[alias] = resolved[canon]
    if extra:
        ctx.update({k: v for k, v in extra.items() if v is not None})
    return ctx

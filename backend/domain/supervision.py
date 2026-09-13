"""
domain/supervision.py — Detección de respuestas por hilo (remitente + asunto
normalizado). Funciones puras.
"""
import re

# Prefijos de respuesta/reenvío a quitar para comparar asuntos por hilo.
# Cubre variantes ES/EN/PT: RE, RV, REF, FW, FWD, ENC, etc.
_REPLY_PREFIX_RE = re.compile(
    r"^\s*(re|rv|ref|res|fw|fwd|enc|rep)\s*(\[\d+\])?\s*:\s*",
    re.IGNORECASE,
)


def normalize_subject(subj: str) -> str:
    """Normaliza un asunto para comparar hilos: quita prefijos RE:/RV:/FWD:
    (encadenados, ej. 'RE: RV: Hola'), colapsa espacios y pasa a minúsculas."""
    s = (subj or "").strip()
    while True:
        new_s = _REPLY_PREFIX_RE.sub("", s)
        if new_s == s:
            break
        s = new_s
    return re.sub(r"\s+", " ", s).strip().lower()


def build_reply_index(inbox_rows: list) -> set:
    """A partir de filas de `inbox_cache` (from_email, subject), arma el set
    de pares (remitente_normalizado, asunto_normalizado) usado para marcar
    `has_reply` en cada envío."""
    replied = set()
    for row in inbox_rows:
        fe = row.get("from_email")
        if not fe:
            continue
        replied.add((fe.strip().lower(), normalize_subject(row.get("subject"))))
    return replied


def has_reply(contact_email: str, subject: str, reply_index: set) -> bool:
    ce = (contact_email or "").strip().lower()
    if not ce:
        return False
    return (ce, normalize_subject(subject)) in reply_index

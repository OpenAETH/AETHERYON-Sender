"""
domain/ai_parsing.py — Parseo tolerante de JSON devuelto por un LLM.
Funciones puras, sin I/O.
"""
import json
import re


def _escape_unescaped_control_chars(s: str) -> str:
    """Escapa saltos de línea/tabs/retornos literales que aparezcan DENTRO de
    strings JSON (causa típica de 'Invalid control character'). Respeta los
    caracteres de control fuera de strings."""
    out = []
    in_string = False
    escaped = False
    for ch in s:
        if in_string:
            if escaped:
                out.append(ch)
                escaped = False
                continue
            if ch == "\\":
                out.append(ch)
                escaped = True
                continue
            if ch == '"':
                out.append(ch)
                in_string = False
                continue
            if ch == "\n":
                out.append("\\n")
                continue
            if ch == "\r":
                out.append("\\r")
                continue
            if ch == "\t":
                out.append("\\t")
                continue
            if ord(ch) < 0x20:
                out.append("\\u%04x" % ord(ch))
                continue
            out.append(ch)
        else:
            if ch == '"':
                in_string = True
            out.append(ch)
    return "".join(out)


def parse_ai_json(raw: str) -> dict:
    """Parsea JSON devuelto por un LLM de forma tolerante: quita code fences,
    recorta texto alrededor del objeto, y repara caracteres de control sin
    escapar dentro de strings. Emojis/acentos/apóstrofes se preservan tal cual."""
    if not raw or not raw.strip():
        raise json.JSONDecodeError("respuesta vacía de la IA", raw or "", 0)
    s = raw.strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s).strip()
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end != -1 and end > start:
        s = s[start:end + 1]

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(s, strict=False)
    except json.JSONDecodeError:
        pass
    return json.loads(_escape_unescaped_control_chars(s), strict=False)


def extract_subject_body(raw: str):
    """Último recurso para un email individual: extrae subject/body con
    regex tolerante aunque el body tenga comillas sin escapar."""
    if not raw:
        return None
    m_sub = re.search(r'"subject"\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
    m_body = re.search(r'"body"\s*:\s*"(.*)"\s*\}?\s*$', raw, re.DOTALL)
    if not m_body:
        m_body = re.search(r'"body"\s*:\s*"(.*)', raw, re.DOTALL)
    if not (m_sub or m_body):
        return None

    def _unescape(v: str) -> str:
        return v.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r").replace('\\"', '"').replace("\\\\", "\\")

    subject = _unescape(m_sub.group(1)) if m_sub else ""
    body = m_body.group(1) if m_body else ""
    body = re.sub(r'"\s*\}?\s*$', "", body)
    body = _unescape(body)
    return {"subject": subject.strip(), "body": body.strip()}

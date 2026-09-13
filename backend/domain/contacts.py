"""
domain/contacts.py — Validaciones de la Agenda (destinatarios). Funciones puras.

La Agenda gestiona a quién se le puede escribir, no un pipeline de ventas:
por eso solo valida forma (nombre/email), no estados de negociación.
"""
import re

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def is_valid_email(email: str) -> bool:
    return bool(email and _EMAIL_RE.match(email.strip()))


def validate_contact(data: dict) -> list:
    """Devuelve la lista de errores (vacía si es válido)."""
    errors = []
    if not (data.get("name") or "").strip():
        errors.append("name es obligatorio")
    email = (data.get("email") or "").strip()
    if not email:
        errors.append("email es obligatorio")
    elif not is_valid_email(email):
        errors.append(f"email inválido: {email}")
    return errors


def parse_csv_rows(rows: list) -> tuple:
    """Normaliza filas de un CSV/JSON de audiencia externa (LeadForge,
    Apollo, u otra fuente) a registros de contacto. Espera columnas
    flexibles: name/nombre, email, company/empresa, role/cargo, phone/telefono,
    tags. Devuelve (contactos_validos, errores).
    Fuentes externas de audiencia: el Sender solo importa lo que le entregan,
    no se conecta a LeadForge/Apollo como módulos internos."""
    valid, errors = [], []
    for i, row in enumerate(rows):
        row = {(k or "").strip().lower(): v for k, v in row.items()}
        name = (row.get("name") or row.get("nombre") or row.get("empresa") or row.get("company") or "").strip()
        email = normalize_email(row.get("email") or "")
        if not email or not is_valid_email(email):
            errors.append(f"Fila {i + 1}: email inválido o ausente")
            continue
        valid.append({
            "name": name or email.split("@")[0],
            "email": email,
            "company": (row.get("company") or row.get("empresa") or "").strip(),
            "role": (row.get("role") or row.get("cargo") or row.get("rol") or "").strip(),
            "phone": (row.get("phone") or row.get("telefono") or row.get("teléfono") or "").strip(),
            "tags": (row.get("tags") or row.get("categoria") or row.get("categoría") or "").strip(),
        })
    return valid, errors

"""
providers/resend_provider.py — Envío de emails vía Resend.

Proveedor "puro": no toca la base de datos ni resuelve adjuntos por ID.
Recibe bytes/contenido ya resuelto por la capa de servicio y solo habla
con la API de Resend. Mantiene Resend como proveedor de envío único,
tal como pedía el enunciado.
"""
import logging

import resend

from backend.config import cfg

logger = logging.getLogger(__name__)


def is_configured() -> bool:
    c = cfg()
    return bool(c["resend_api_key"] and c["sender_email"])


def send(
    to: str,
    subject: str,
    body_plain: str,
    body_html: str,
    reply_to_mid: str = None,
    attachments: list = None,
):
    """
    Envía un email vía Resend.

    `attachments`, si se pasa, es una lista de dicts ya resueltos:
    [{"filename": ..., "content": <bytes>, "content_type": ...}, ...]
    (la resolución attachment_id → bytes vive en el servicio que llama,
    típicamente descargando de Supabase Storage).

    Devuelve la respuesta de Resend (con `.id` cuando el envío es exitoso).
    Levanta ValueError si falta configuración, o resend.exceptions.ResendError /
    Exception propagada desde el SDK si falla el envío.
    """
    import base64

    c = cfg()
    if not c["resend_api_key"]:
        raise ValueError("RESEND_API_KEY no configurada en las variables de entorno")
    if not c["sender_email"]:
        raise ValueError("SENDER_EMAIL no configurada en las variables de entorno")

    resend.api_key = c["resend_api_key"]
    from_addr = f"{c['sender_name']} <{c['sender_email']}>" if c["sender_name"] else c["sender_email"]

    params: dict = {
        "from": from_addr,
        "to": [to],
        "subject": subject,
        "html": body_html,
        "text": body_plain,
    }
    if reply_to_mid:
        params["headers"] = {"In-Reply-To": reply_to_mid, "References": reply_to_mid}

    if attachments:
        atts = []
        for att in attachments:
            content = att["content"]
            if isinstance(content, (bytes, bytearray)):
                content = base64.b64encode(content).decode("ascii")
            atts.append({
                "filename": att["filename"],
                "content": content,
                "content_type": att.get("content_type", "application/octet-stream"),
            })
        params["attachments"] = atts

    response = resend.Emails.send(params)
    email_id = response.id if hasattr(response, "id") else str(response)
    logger.info(f"Resend OK → {to} | id={email_id}")
    return response


def list_domains() -> list:
    """Usado por el chequeo de estado del sistema (Supervisión)."""
    c = cfg()
    if not c["resend_api_key"]:
        raise ValueError("RESEND_API_KEY no configurada")
    resend.api_key = c["resend_api_key"]
    domains = resend.Domains.list()
    return [d.get("name") for d in (domains.get("data") or [])]

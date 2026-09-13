"""
services/ai_service.py — Asistencia OPCIONAL de redacción vía Groq.

Genera contenido (asunto + cuerpo en Markdown) para secuencias de
comunicación. Es una ayuda para escribir más rápido dentro de
Redacción + Templates — el producto funciona igual sin esto (podés
escribir y programar todo a mano).
"""
import json
import logging

from sqlalchemy import text

from backend.db.connection import get_db, dict_from_row, rows_to_list
from backend.domain.ai_parsing import parse_ai_json, extract_subject_body
from backend.providers import groq_provider

logger = logging.getLogger(__name__)

_TONE_DESC = {
    "aggressive": "tono persuasivo y directo, CTA fuerte, urgencia, beneficios concretos",
    "informative": "tono educativo, informativo, construcción de confianza",
}


def _entity_context() -> str:
    db = get_db()
    try:
        ctx = ""
        r1 = dict_from_row(db.execute(text("SELECT value FROM settings WHERE key='ctx_entity'")).fetchone())
        r2 = dict_from_row(db.execute(text("SELECT value FROM settings WHERE key='ctx_mission'")).fetchone())
        if r1:
            ctx += f"\nEntidad: {r1['value']}"
        if r2:
            ctx += f"\nMisión: {r2['value']}"
        return ctx
    except Exception:
        return ""
    finally:
        db.close()


async def raw_generate(messages: list, stream: bool = False):
    """Proxy delgado usado por el asistente de Bandeja/Redacción libre."""
    if not stream:
        content = await groq_provider.chat(messages, model="groq/compound")
        return {"content": content}
    return groq_provider.chat_stream(messages, model="groq/compound")


async def stream_sequence_email(cid: int, day_number: int, feedback: str = ""):
    """Genera UNA pieza de la secuencia con streaming SSE. Yields líneas
    `data: {...}` — {"delta": "..."} mientras llega texto, y finalmente
    {"done": true, "subject":..., "body":...} (o {"error": "..."})."""
    db = get_db()
    try:
        camp = dict_from_row(db.execute(text("SELECT * FROM campaigns WHERE id=:id"), {"id": cid}).fetchone())
        if not camp:
            yield f"data: {json.dumps({'error': 'Comunicación no encontrada'})}\n\n"
            return
        total = dict_from_row(db.execute(text("SELECT COUNT(*) as n FROM campaign_emails WHERE campaign_id=:id"), {"id": cid}).fetchone())["n"]
        prev_emails = rows_to_list(db.execute(text("""
            SELECT day_number, subject, body FROM campaign_emails
            WHERE campaign_id=:id AND day_number < :day AND subject != '' ORDER BY day_number
        """), {"id": cid, "day": day_number}).fetchall())
        em = dict_from_row(db.execute(text(
            "SELECT * FROM campaign_emails WHERE campaign_id=:id AND day_number=:day"
        ), {"id": cid, "day": day_number}).fetchone())
    finally:
        db.close()

    tone_desc = _TONE_DESC.get(camp.get("tone", "informative"), "tono profesional")
    entity_ctx = _entity_context()
    prev_context = ""
    if prev_emails:
        prev_context = "\n\nEMAILS ANTERIORES (para coherencia):\n" + "\n".join(
            f"- Email {e['day_number']}: [{e['subject']}]" for e in prev_emails[-3:]
        )
    scheduled = em["scheduled_at"] if em else ""

    system_prompt = f"""Eres experto en redacción de comunicaciones profesionales. Genera la pieza #{day_number} de {total} de una secuencia.

INTENCION: {camp['intent']}
TONO: {tone_desc}
FECHA PROGRAMADA: {scheduled}
{entity_ctx}
{prev_context}
{"FEEDBACK A INCORPORAR: " + feedback if feedback else ""}

REGLAS:
- Pieza numero {day_number} de {total}: posicionarla narrativamente en la secuencia
- {"Primera pieza: presentacion, gancho inicial, presentar propuesta" if day_number == 1 else ""}
- {"Ultima pieza: cierre, urgencia maxima, CTA final definitivo" if day_number == total else ""}
- Formato Markdown: **negrita**, *italica*, ## titulos, listas con -
- Podes usar variables si aplica: {{{{first_name}}}}, {{{{company}}}}, {{{{role}}}}, {{{{sender_name}}}}, {{{{cta_url}}}}
- NO incluir Para/De/Asunto en el cuerpo
- Cuerpo completo y elaborado (minimo 150 palabras)

Responde UNICAMENTE con JSON valido (sin texto extra, sin backticks):
{{"subject": "Asunto", "body": "Cuerpo completo en Markdown..."}}"""

    full_content = ""
    try:
        async for line in groq_provider.chat_stream(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Genera la pieza #{day_number}."},
            ]
        ):
            chunk = line[6:]
            if chunk == "[DONE]":
                break
            try:
                delta = json.loads(chunk)["choices"][0]["delta"].get("content", "")
            except Exception:
                continue
            if delta:
                full_content += delta
                yield f"data: {json.dumps({'delta': delta})}\n\n"

        try:
            parsed = parse_ai_json(full_content)
        except json.JSONDecodeError:
            parsed = extract_subject_body(full_content)
            if parsed is None:
                raise

        subject = parsed.get("subject", "")
        body = parsed.get("body", "")

        db3 = get_db()
        try:
            db3.execute(text("""
                UPDATE campaign_emails
                SET subject=:subject, body=:body,
                    regenerated_count=CASE WHEN subject!='' AND subject IS NOT NULL THEN regenerated_count+1 ELSE regenerated_count END
                WHERE campaign_id=:cid AND day_number=:day
            """), {"subject": subject, "body": body, "cid": cid, "day": day_number})
            db3.commit()
        finally:
            db3.close()

        yield f"data: {json.dumps({'done': True, 'subject': subject, 'body': body})}\n\n"
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error en generate-one: {e} | content: {full_content[:200]}")
        yield f"data: {json.dumps({'error': f'Error parseando respuesta de IA: {e}'})}\n\n"
    except Exception as e:
        logger.error(f"Stream error en generate-one: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n\n"


async def generate_sequence(contacts_list, start_date, end_date, intent, send_mode, tone, send_time, name, n: int, dates: list) -> list:
    """Genera la secuencia completa de N piezas con Groq (una sola llamada,
    sin streaming). Devuelve la lista de {day_number, subject, body}."""
    entity_ctx = _entity_context()
    tone_desc = _TONE_DESC.get(tone, "tono profesional")
    mode_desc = {
        "daily": "diariamente", "alternate": "día por medio",
        "mon_wed_fri": "lunes, miércoles y viernes", "tue_thu": "martes y jueves",
    }.get(send_mode, send_mode)

    system_prompt = f"""Eres un experto en redacción de comunicaciones profesionales. Vas a generar una secuencia de {n} piezas.

INTENCIÓN: {intent}
TONO: {tone_desc}
FRECUENCIA: enviadas {mode_desc}
FECHAS: {dates[0]} al {dates[-1]}
{entity_ctx}

REGLAS:
- Genera exactamente {n} piezas numeradas
- Mantén coherencia narrativa progresiva entre piezas
- Varía el ángulo y CTA en cada una
- Usa formato Markdown: **negrita**, *italica*, ## títulos, listas con -
- Podés usar variables si aplica: {{{{first_name}}}}, {{{{company}}}}, {{{{role}}}}, {{{{sender_name}}}}, {{{{cta_url}}}}
- Cada pieza debe tener asunto y cuerpo distintos
- NO incluyas encabezados como Para:/De:/Asunto: en el cuerpo

Responde ÚNICAMENTE con JSON válido con esta estructura exacta (sin texto extra, sin markdown):
{{
  "emails": [
    {{"day_number": 1, "subject": "Asunto 1", "body": "Cuerpo en Markdown..."}}
  ]
}}"""

    raw_content = await groq_provider.chat(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Genera la secuencia de {n} piezas. Responde solo con el JSON."},
        ],
        model="llama-3.3-70b-versatile", temperature=0.8, max_tokens=8000,
    )
    emails_data = parse_ai_json(raw_content.strip())
    emails_list = emails_data.get("emails", [])

    if len(emails_list) != n:
        while len(emails_list) < n:
            emails_list.append({"day_number": len(emails_list) + 1, "subject": f"Pieza {len(emails_list)+1}", "body": "Contenido pendiente de regeneración."})
        emails_list = emails_list[:n]
    return emails_list


async def retry_sequence_email(cid: int, email_id: int, feedback: str = "") -> dict:
    """Regenera con IA una pieza puntual, manteniendo contexto y posición."""
    db = get_db()
    try:
        em = dict_from_row(db.execute(text(
            "SELECT e.*, c.intent, c.tone FROM campaign_emails e JOIN campaigns c ON c.id=e.campaign_id WHERE e.id=:id"
        ), {"id": email_id}).fetchone())
        if not em:
            raise ValueError("Comunicación no encontrada")

        others = rows_to_list(db.execute(text(
            "SELECT day_number, subject, body FROM campaign_emails WHERE campaign_id=:cid AND id!=:id ORDER BY day_number"
        ), {"cid": cid, "id": email_id}).fetchall())
    finally:
        db.close()

    context_summary = "\n".join(f"- Email {o['day_number']}: {o['subject']}" for o in others[:5])
    tone_desc = _TONE_DESC.get(em.get("tone", "informative"), "tono profesional")

    prompt = f"""Estás regenerando la pieza #{em['day_number']} de una secuencia.

INTENCIÓN: {em['intent']}
TONO: {tone_desc}
POSICIÓN: pieza {em['day_number']} de la secuencia
OTRAS PIEZAS:
{context_summary}

PIEZA ANTERIOR (a mejorar):
Asunto: {em['subject']}
Cuerpo: {em['body']}

{'FEEDBACK DEL USUARIO: ' + feedback if feedback else ''}

Genera una nueva versión mejorada para esta posición. Responde SOLO con JSON:
{{"subject": "Nuevo asunto", "body": "Nuevo cuerpo en Markdown"}}"""

    raw = await groq_provider.chat(messages=[{"role": "user", "content": prompt}], model="llama-3.3-70b-versatile", temperature=0.9, max_tokens=2000)
    try:
        new_data = parse_ai_json(raw.strip())
    except json.JSONDecodeError:
        new_data = extract_subject_body(raw)
        if new_data is None:
            raise

    db = get_db()
    try:
        db.execute(text("""
            UPDATE campaign_emails
            SET subject=:subject, body=:body, status='pending', version=version+1, regenerated_count=regenerated_count+1
            WHERE id=:id
        """), {"subject": new_data.get("subject", ""), "body": new_data.get("body", ""), "id": email_id})
        db.commit()
    finally:
        db.close()
    return {"subject": new_data.get("subject", ""), "body": new_data.get("body", "")}

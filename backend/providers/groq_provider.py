"""
providers/groq_provider.py — Cliente delgado para Groq (LLM), usado como
asistente OPCIONAL de redacción dentro del módulo Redacción + Templates.
No es requisito del producto: si GROQ_API_KEY no está configurada, las
rutas que lo usan devuelven un error claro y el resto de la app sigue
funcionando (preparar/programar/enviar/supervisar no dependen de esto).
"""
import json
import logging

import httpx

from backend.config import cfg

logger = logging.getLogger(__name__)

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "llama-3.3-70b-versatile"


def is_configured() -> bool:
    return bool(cfg()["groq_api_key"])


async def chat(messages: list, model: str = "groq/compound", temperature: float = 0.8, max_tokens: int = 2000) -> str:
    """Llamada no-streaming. Devuelve el texto de la respuesta."""
    key = cfg()["groq_api_key"]
    if not key:
        raise RuntimeError("GROQ_API_KEY no configurada en variables de entorno")
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(GROQ_URL, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return (data.get("choices") or [{}])[0].get("message", {}).get("content", "")


async def chat_stream(messages: list, model: str = DEFAULT_MODEL, temperature: float = 0.85, max_tokens: int = 2000):
    """Generador async que reenvía las líneas SSE `data: ...` de Groq tal cual."""
    key = cfg()["groq_api_key"]
    if not key:
        raise RuntimeError("GROQ_API_KEY no configurada en variables de entorno")
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", GROQ_URL, headers=headers, json=payload) as resp:
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    yield line

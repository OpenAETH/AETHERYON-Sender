"""
api/ai.py — Proxy delgado hacia Groq, usado por el asistente de redacción
libre (por ejemplo, para sugerir una respuesta en Bandeja). Es opcional:
si GROQ_API_KEY no está configurada, devuelve 500 con un mensaje claro y
el resto del producto sigue funcionando igual.
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from backend.api.deps import require_auth
from backend.providers import groq_provider
from backend.services import ai_service

router = APIRouter(prefix="/ai", tags=["ai"], dependencies=[Depends(require_auth)])


@router.post("/generate")
async def ai_generate(request: Request):
    data = await request.json()
    messages = data.get("messages") or []
    stream = bool(data.get("stream", False))
    if not groq_provider.is_configured():
        raise HTTPException(500, "GROQ_API_KEY no configurada en variables de entorno")
    if not messages:
        raise HTTPException(400, "messages es obligatorio")

    if stream:
        async def gen():
            async for line in groq_provider.chat_stream(messages, model="qwen/qwen3.8-27b"):
                yield line + "\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream")

    result = await ai_service.raw_generate(messages, stream=False)
    return result

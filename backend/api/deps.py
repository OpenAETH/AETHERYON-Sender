"""api/deps.py — Dependencias compartidas entre routers."""
from fastapi import HTTPException, Request

from backend.services.auth_service import verify_token


async def require_auth(request: Request) -> str:
    token = request.cookies.get("session") or request.headers.get("X-Session-Token", "")
    if not token or not verify_token(token):
        raise HTTPException(401, "No autorizado")
    return token

"""api/auth.py — Sesión (login/logout/check)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from backend.api.deps import require_auth
from backend.config import APP_USER, IS_HTTPS, TOKEN_TTL
from backend.services import auth_service

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login")
async def login(request: Request):
    data = await request.json()
    user = data.get("username", "").strip()
    pw = data.get("password", "")
    if not auth_service.check_credentials(user, pw):
        raise HTTPException(401, "Credenciales incorrectas")
    token, _ = auth_service.create_session()
    resp = JSONResponse({"success": True, "token": token})
    resp.set_cookie("session", token, httponly=True, samesite="lax", max_age=TOKEN_TTL, secure=IS_HTTPS)
    return resp


@router.post("/logout")
async def logout(request: Request):
    auth_service.destroy_session(request.cookies.get("session", ""))
    resp = JSONResponse({"success": True})
    resp.delete_cookie("session")
    return resp


@router.get("/check")
async def auth_check(_: str = Depends(require_auth)):
    return {"ok": True, "user": APP_USER}

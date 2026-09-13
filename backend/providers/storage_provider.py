"""
providers/storage_provider.py — Adjuntos en Supabase Storage vía REST (httpx).

Usa la service_role key (JWT, formato eyJ...) — NO la publishable key ni la
Secret Key nueva (sb_secret_), que no funcionan con Storage.
"""
import httpx

from backend.config import cfg


def _storage_cfg():
    c = cfg()
    if not c["supabase_url"] or not c["supabase_service_key"]:
        return None
    return c


def is_configured() -> bool:
    return _storage_cfg() is not None


def _headers():
    c = _storage_cfg()
    if not c:
        return None
    return {"Authorization": f"Bearer {c['supabase_service_key']}"}


def upload(bucket: str, path: str, file_bytes: bytes, content_type: str):
    c = _storage_cfg()
    if not c:
        raise RuntimeError("Supabase Storage no configurado")
    url = f"{c['supabase_url']}/storage/v1/object/{bucket}/{path}"
    headers = _headers()
    headers["Content-Type"] = content_type or "application/octet-stream"
    headers["x-upsert"] = "true"
    r = httpx.post(url, content=file_bytes, headers=headers, timeout=60)
    if r.status_code == 403:
        raise RuntimeError("Credenciales de Supabase Storage inválidas — revisá SUPABASE_SERVICE_KEY")
    if r.status_code == 413:
        mb = len(file_bytes) / 1048576
        raise RuntimeError(
            f"El archivo ({mb:.1f} MB) supera el límite del bucket '{bucket}' en Supabase. "
            f"Subí el límite en Supabase → Storage → bucket '{bucket}' → Edit bucket → "
            f"'File size limit' (o el global en Project Settings → Storage)."
        )
    if not r.is_success:
        raise RuntimeError(f"Supabase Storage respondió {r.status_code}: {r.text[:500]}")
    return True


def signed_url(bucket: str, path: str, expires_in: int = 1800) -> str:
    """Crea una signed URL temporal. Se conserva por compatibilidad/uso defensivo;
    el envío de adjuntos usa `download()` + base64, no esta URL."""
    c = _storage_cfg()
    if not c:
        raise RuntimeError("Supabase Storage no configurado")
    url = f"{c['supabase_url']}/storage/v1/object/sign/{bucket}/{path}"
    headers = _headers()
    headers["Content-Type"] = "application/json"
    r = httpx.post(url, json={"expiresIn": str(expires_in)}, headers=headers, timeout=15)
    if r.status_code == 403:
        raise RuntimeError("Credenciales de Supabase Storage inválidas — revisá SUPABASE_SERVICE_KEY")
    r.raise_for_status()
    data = r.json()
    signed = data.get("signedURL") or data.get("signedUrl", "")
    if not signed:
        raise RuntimeError(f"Respuesta inesperada de Storage: {data}")
    if signed.startswith("http"):
        return signed
    base = c["supabase_url"].rstrip("/")
    path_part = signed if signed.startswith("/") else f"/{signed}"
    if not path_part.startswith("/storage/v1"):
        path_part = f"/storage/v1{path_part}"
    return f"{base}{path_part}"


def download(bucket: str, path: str) -> bytes:
    c = _storage_cfg()
    if not c:
        raise RuntimeError("Supabase Storage no configurado")
    url = f"{c['supabase_url']}/storage/v1/object/{bucket}/{path}"
    r = httpx.get(url, headers=_headers(), timeout=30)
    if r.status_code == 403:
        raise RuntimeError("Credenciales de Supabase Storage inválidas — revisá SUPABASE_SERVICE_KEY")
    if not r.is_success:
        raise RuntimeError(f"Supabase Storage respondió {r.status_code}: {r.text[:300]}")
    return r.content


def delete(bucket: str, paths: list):
    c = _storage_cfg()
    if not c:
        raise RuntimeError("Supabase Storage no configurado")
    url = f"{c['supabase_url']}/storage/v1/object/{bucket}"
    r = httpx.delete(url, json={"prefixes": paths}, headers=_headers(), timeout=15)
    if r.status_code == 403:
        raise RuntimeError("Credenciales de Supabase Storage inválidas — revisá SUPABASE_SERVICE_KEY")
    r.raise_for_status()
    return True

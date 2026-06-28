# Recap — Emailer Agent

Bitácora de trabajo para iterar sobre el código. Anotaciones de Claude para retomar contexto rápido entre sesiones.

---

## Arquitectura (resumen)

- **`api.py`** — backend FastAPI (todo el server: rutas, IMAP, envío via Resend, DB con SQLAlchemy/Postgres).
- **`index.html`** — frontend SPA completo (HTML + CSS + JS inline). Navegación por `go('<view>')`.
- **`migrations.sql`** — esquema DB (integración CRM desde CommandCenter).
- **`render.yaml`** — deploy en Render. **`requirements.txt`**, **`.env.example`**.

### Tablas clave
- `email_logs` — envíos/recibidos. `direction` ('out'/'in'), `contact_email`, `subject`, `sent_at`, `message_id`, `thread_id`, `campaign_id`.
- `inbox_cache` — bandeja sincronizada por IMAP. `message_id`, `imap_uid`, `from_email`, `subject`, `body`, `replied` (flag MANUAL: "yo respondí a este msg", NO "el contacto respondió").
- `contacts` — CRM. `email`, `company`, etc.

### Endpoints relevantes
- `GET /supervision` — seguimiento de envíos + detección de respuestas.
- `GET /inbox?refresh=bool` — bandeja; `refresh=true` dispara `fetch_inbox_sync()`.
- `POST /inbox/mark-replied` — setea `inbox_cache.replied=1` (manual).
- `POST /send-email` — envía via Resend; soporta `reply_to` (headers In-Reply-To/References).
- `fetch_inbox_sync(limit)` (`api.py:~520`) — sync IMAP: trae nuevos, borra los que ya no están en server.

---

## Historial de cambios

### 2026-06-19 — Fix envío de emails con adjuntos (Supabase Storage)
**Problema:** al adjuntar un archivo (p. ej. PDF), el email **no se enviaba**.
**Causa raíz:** `send_resend()` (`api.py:~559`) generaba una *signed URL* con `_storage_signed_url()` y se la pasaba a Resend como `attachment.path`. La URL estaba mal construida: Supabase devuelve `signedURL` como ruta relativa a la raíz de Storage (`/object/sign/...`) y el código anteponía solo el host, **omitiendo el prefijo obligatorio `/storage/v1`**. La URL daba 404; Resend fallaba al descargarla en el momento del envío y **reventaba el email completo** (el fallo ocurría dentro de `resend.Emails.send()`, fuera del `try/except` que solo logueaba un warning).
**Fix (`api.py`):**
1. Nuevo helper `_storage_download(bucket, path)` — descarga bytes vía REST `GET /storage/v1/object/{bucket}/{path}`, simétrico a `_storage_upload()`.
2. `send_resend()` ahora **descarga el archivo de Storage y lo envía a Resend como `content` base64** (usa el `base64` ya importado), en vez de pasar una URL externa. Elimina la dependencia del fetch externo y la expiración de 30 min de la signed URL. Límite Resend: 40 MB/email.
3. Se quitó el `try/except` que silenciaba fallos de adjuntos: si un adjunto no se puede descargar, el envío falla con error claro (el llamador `/send-email` ya reporta el error por destinatario).
4. (Defensivo) `_storage_signed_url()` corregido para anteponer `/storage/v1` correctamente por si vuelve a usarse; idempotente y respeta URLs absolutas.
**Reenvío:** garantizado por construcción — el envío lee por `attachment_id → storage_path` y solo **descarga** el objeto existente; nunca re-sube. Un PDF ya en Storage se reutiliza tal cual.

### 2026-06-04 — Fix módulo Supervisión (commit `f53a0e5`)
**Problema:** la columna "Respuesta" siempre mostraba "No" aunque el contacto respondiera.
**Causa raíz:** dependía del flag `inbox_cache.replied=1`, que solo se activa manualmente desde la Bandeja y semánticamente significa "yo respondí", no "me respondieron". El sync IMAP nunca lo activaba.
**Fix (en `GET /supervision`, `api.py:~1396`):**
1. Helper `_normalize_subject()` — quita prefijos RE/RV/RES/REF/FW/FWD/ENC (encadenados y `RE[2]:`), colapsa espacios, minúsculas.
2. Matching POR HILO: set de pares `(from_email_normalizado, subject_normalizado)` desde `inbox_cache`, comparado contra cada envío. Ej: envío `Saludos cordiales` ↔ respuesta `RE: Saludos cordiales` → "Sí".
3. `GET /supervision?refresh=true` (default) dispara `fetch_inbox_sync(60)` al abrir.
**Verificado:** funcionando en UI ("Si" verde en envío de prueba).

---

## Mejoras futuras pendientes (módulo Supervisión)

> Guido: "por ahora alcanza con remitente + asunto". Estas quedan anotadas para iterar.

1. **Filtro por fecha/hora** — exigir que la respuesta sea *posterior* al `sent_at` del envío (evita que correos viejos del mismo contacto/asunto cuenten como respuesta). Cambio chico, solo en la query/comparación de `/supervision`.
2. **Message-ID / In-Reply-To (preciso por hilo real)** — resuelve el caso de mismo asunto repetido en campañas distintas (hoy una respuesta marca "Sí" en ambos envíos). Requiere:
   - Guardar el `Message-ID` saliente en `email_logs` al enviar (Resend lo devuelve / o setear uno propio).
   - Parsear `In-Reply-To`/`References` de los entrantes en `fetch_inbox_sync` y matchear contra ese Message-ID.
   - Es cambio de esquema + lógica de sync.

---

## Notas de entorno / git

- Repo: `https://github.com/OpenAETH/Emailer-Agent.git` (origin, HTTPS).
- Branch principal: `main`. Local y `origin/main` (cacheado) en `f53a0e5` — sincronizados.
- **WSL sin credenciales git ni `gh`:** no se puede `fetch`/`pull`/`push` en vivo desde la sesión. Para sincronizar, el usuario corre `! git pull origin main` en el prompt, o autentica (`gh auth login` / token).
- Working tree con cambios previos sin commitear (no relacionados al fix): `.env.example`, `render.yaml`, `requirements.txt`. `__pycache__/` es untracked (basura) — considerar `.gitignore`.

---

## TODO / ideas sueltas

- [ ] Añadir `.gitignore` con `__pycache__/`, `.env`, etc.
- [ ] (Supervisión) filtro por fecha — ver mejora #1.
- [ ] (Supervisión) Message-ID/In-Reply-To — ver mejora #2.

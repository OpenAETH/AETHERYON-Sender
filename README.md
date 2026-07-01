# Emailer Agent

Asistente ejecutivo de comunicaciones vía email con backend FastAPI, frontend SPA vanilla, campañas de email marketing generadas por IA y pipeline CRM.

<details>
<summary><strong>📑 Tabla de Contenidos</strong></summary>

- [Stack Tecnológico](#stack-tecnológico)
- [Quick Start](#quick-start)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Funcionalidades](#funcionalidades)
- [API Endpoints](#api-endpoints)
- [Base de Datos](#base-de-datos)
- [Variables de Entorno](#variables-de-entorno)
- [Desarrollo Local](#desarrollo-local)
- [Deploy en Render](#deploy-en-render)
- [Notas Técnicas](#notas-técnicas)
</details>

---

## Stack Tecnológico

| Capa | Tecnología |
|------|-----------|
| Backend | Python / FastAPI / Uvicorn |
| Frontend | SPA vanilla JS (un solo `index.html`) |
| Base de datos | PostgreSQL via Supabase (SQLAlchemy 2.0 + psycopg2) |
| Envío de emails | Resend API |
| Bandeja de entrada | IMAP SSL |
| Generación IA | Groq (Llama 3.3 70b) |
| Deploy | Render.com |

---

## Quick Start

### Requisitos previos

1. **Supabase** — crear proyecto y ejecutar [`migrations.sql`](migrations.sql)
2. **Resend** — cuenta con dominio remitente verificado
3. **Groq** — API key gratuita en [console.groq.com](https://console.groq.com)
4. **Servidor IMAP** — para sincronización de bandeja (opcional)

### Local

```bash
pip install -r requirements.txt
cp .env.example .env
# editar .env con tus credenciales (ver Variables de Entorno)
python api.py
# → http://localhost:8000
```

### Deploy en Render (1 click)

1. Subir el repo a GitHub
2. Render: **New → Web Service → conectar repo** (detecta `render.yaml` automáticamente)
3. Configurar en el dashboard las variables marcadas como `sync: false` (ver [Variables de Entorno](#variables-de-entorno))

> `SECRET_KEY` se genera automáticamente en Render (`generateValue: true`). Para desarrollo local:
> ```bash
> python -c "import secrets; print(secrets.token_hex(32))"
> ```

---

## Estructura del Proyecto

```
api.py            Backend FastAPI — sirve API REST y frontend SPA
index.html        Frontend SPA completo
migrations.sql    Schema SQL para Supabase
requirements.txt  Dependencias Python (versiones pinneadas)
render.yaml       Infraestructura como código para Render
.env.example      Variables de entorno de referencia
```

---

## Funcionalidades

### 📋 CRM — Pipeline de contactos
Tablero kanban con 5 estados (`nuevo → contactado → en conversacion → interesado → cerrado`).  
Drag & drop, avance/retroceso por botón, badges de campañas activas, archivado.  
`PATCH /contacts/{id}/status`

### ✉️ Envío individual / múltiple
Cuerpo en Markdown → HTML con template personalizable. Destinatario único o múltiples.  
`POST /send-email`

### 📎 Adjuntos (Supabase Storage)
Los archivos se suben una vez a Supabase Storage (`POST /upload`) y quedan referenciados por `attachment_id`. Al enviar, el backend **descarga el archivo de Storage y lo adjunta a Resend como `content` base64** — los reenvíos reutilizan el mismo objeto sin volver a subirlo. Límite: 50 MB por archivo al subir, 40 MB por email (límite de Resend tras base64). Disponible tanto en envío individual como en campañas.  
`POST /upload` · `GET /attachments` · `DELETE /attachments/{id}` · `POST /campaigns/{id}/attachments`

### 📬 Bandeja de entrada (IMAP)
Sync bidireccional: agrega mensajes nuevos, elimina de DB los borrados del servidor, preserva flags.  
`GET /inbox?refresh=true`

### 🚀 Campañas de email marketing (IA)
Flujo completo con revisión humana:

```
1. Generar   → Groq crea secuencia completa (intención + tono + fechas + contactos)
2. Revisar   → Galería con cada email y preview HTML
3. Aprobar/Rechazar → email por email
4. Regenerar → rewrite con feedback opcional
5. Finalizar → validación y programación
6. Enviar    → automático (scheduler cada 60s) o manual
```

| Modo | Frecuencia |
|------|-----------|
| `daily` | Todos los días |
| `alternate` | Día por medio |
| `mon_wed_fri` | Lun, Mié, Vie |
| `tue_thu` | Mar, Jue |

| Tono | Estilo |
|------|--------|
| `informative` | Educativo, construcción de confianza |
| `aggressive` | Persuasivo, CTA fuerte, urgencia |

### 🔍 Supervisión
Seguimiento de envíos con detección automática de respuestas por hilo (remitente + asunto normalizado).  
`GET /supervision`

### 🔗 Integración LeadForge (leadforge.db → contactos)
LeadForge (scraper) mantiene un **SQLite acumulativo** (`leadforge.db`) cuya **fuente de
verdad vive en `D:\LeadForge\leadforge.db`**. El `.db` de este repo es solo una **copia de
deploy** que LeadForge sube por `git push` para sincronizar Render. La app lo importa a la
tabla `contacts` por **UPSERT por email**, preservando los campos del CRM ya editados
(status, notas, seguimiento).

- **Local (:8000)**: lee la DB canónica **en vivo** vía bind-mount. `docker-compose.yml`
  monta `D:\LeadForge` y setea `LEADFORGE_DB=/leadforge/leadforge.db`, así cada run de
  LeadForge deja la DB fresca sin rebuild del contenedor.
- **Render (remoto)**: sin disco persistente, lee la copia de deploy horneada en la imagen;
  el `git push` de ese `.db` dispara el redeploy.
- **Auto-sync al arrancar**: en cada boot (local) o deploy (Render con `.db` nuevo vía push).
- **Manual**: botón **↧ LeadForge** en la Agenda.
- **Segmentación por categoría**: el formulario de campaña ofrece un selector de rubro
  (lee las categorías de `leadforge.db`) que pre-puebla los destinatarios.

`GET /contacts/leadforge-status` · `POST /contacts/import-leadforge` · `GET /contacts/by-category?categoria=…`

> Override de la ruta con la env `LEADFORGE_DB` (default: `leadforge.db` junto a `api.py`).

### 🧠 Memoria del agente
Registro cronológico de eventos (emails enviados, contactos agregados, notas manuales). Se inyecta como contexto en prompts de IA.

### ⚙️ Contexto configurable
Define identidad (`entity`) y misión activa (`mission`). Se incluye automáticamente en todas las generaciones IA.

### 🎨 Estilo de email personalizable
Colores, fuentes, firma HTML. Vista previa en vivo. Persistido en DB (`settings`).

---

## API Endpoints

<details>
<summary><strong>🔐 Auth</strong> (3 endpoints)</summary>

| Método | Ruta | Auth |
|--------|------|------|
| POST | `/auth/login` | ❌ |
| POST | `/auth/logout` | ✅ |
| GET | `/auth/check` | ✅ |
</details>

<details>
<summary><strong>📧 Email</strong> (2 endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/send-email` | Envío individual o múltiple via Resend |
| POST | `/preview-email` | Renderiza HTML del email con estilos actuales |
</details>

<details>
<summary><strong>📎 Adjuntos</strong> (3 endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/upload` | Sube archivos a Supabase Storage, devuelve `attachment_id` |
| GET | `/attachments` | Lista adjuntos disponibles |
| DELETE | `/attachments/{id}` | Elimina adjunto (Storage + metadata) |
</details>

<details>
<summary><strong>📬 Bandeja</strong> (3 endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/inbox` | Lista mensajes. `?refresh=true` sincroniza IMAP |
| POST | `/inbox/mark-replied` | Marca un mensaje como respondido |
| POST | `/inbox/save-suggestion` | Guarda sugerencia de IA en un mensaje |
</details>

<details>
<summary><strong>👥 Contactos / CRM</strong> (8 endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/contacts` | Lista todos los contactos (incluye campos CRM) |
| POST | `/contacts` | Crear contacto (`name` y `email` requeridos) |
| PUT | `/contacts/{id}` | Actualizar contacto (no modifica `email`) |
| DELETE | `/contacts/{id}` | Eliminar contacto (cascada sobre interacciones) |
| PATCH | `/contacts/{id}/status` | Actualiza estado del pipeline CRM |
| GET | `/contacts/{id}/interactions` | Lista interacciones del CRM |
| POST | `/contacts/{id}/interactions` | Registra una interacción |
| GET | `/contacts/campaign-status` | Estado de campañas por contacto |
</details>

<details>
<summary><strong>🚀 Campañas</strong> (13+ endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/campaigns` | Lista campañas con conteos |
| GET | `/campaigns/schedule` | Agenda de envío con estados |
| GET | `/campaigns/templates` | Lista plantillas pre-aprobadas |
| POST | `/campaigns/generate` | Genera campaña completa con Groq |
| POST | `/campaigns/init` | Crea estructura vacía (generación uno a uno) |
| POST | `/campaigns/process-scheduled` | Procesa emails programados vencidos |
| GET | `/campaigns/{id}` | Detalle con emails y contactos |
| DELETE | `/campaigns/{id}` | Elimina campaña y sus datos |
| POST | `/campaigns/{id}/generate-one` | Genera un email (SSE streaming) |
| POST | `/campaigns/{id}/approve-email` | Aprueba o rechaza un email |
| POST | `/campaigns/{id}/retry-email` | Regenera un email con feedback |
| POST | `/campaigns/{id}/finalize` | Valida y marca como `scheduled` |
| POST | `/campaigns/{id}/send-now` | Envía todos los emails aprobados |
| POST | `/campaigns/{id}/save-as-template` | Guarda como plantilla |
| POST | `/campaigns/templates/{tid}/use` | Instancia una plantilla |
</details>

<details>
<summary><strong>⚙️ Otros</strong> (9 endpoints)</summary>

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/logs` | Historial de envíos (`?limit=N`) |
| GET | `/supervision` | Emails enviados con seguimiento de respuesta |
| GET | `/stats` | Conteos: contactos, enviados, inbox, respondidos, memoria |
| GET / POST | `/settings` | Estilos del template de email y firma HTML |
| GET / POST | `/context` | Contexto de entidad/misión para prompts de IA |
| GET | `/memory` | Registro de eventos internos |
| POST | `/memory` | Agregar nota manual a la memoria |
| GET | `/api/status` | Estado de Resend e IMAP |
| GET | `/config` | Configuración actual del servidor |
</details>

---

## Base de Datos

> El schema SQL completo está en [`migrations.sql`](migrations.sql).  
> Ejecutarlo en Supabase SQL Editor antes del primer deploy. El código no crea tablas automáticamente.

| Tabla | Propósito |
|-------|-----------|
| `contacts` | Contactos + pipeline CRM (status, tipo, medio, notas, seguimiento) |
| `contact_interactions` | Historial de interacciones por contacto (cascada al borrar) |
| `email_logs` | Todos los emails enviados/recibidos |
| `inbox_cache` | Bandeja IMAP cachead localmente |
| `memory` | Registro de eventos del agente |
| `sessions` | Tokens de sesión activos |
| `settings` | Configuración clave/valor (estilos, firma, contexto) |
| `campaigns` | Campañas de email marketing |
| `campaign_emails` | Emails individuales de cada campaña |
| `campaign_contacts` | Contactos asignados a cada campaña |
| `attachments` | Metadata de archivos subidos a Supabase Storage (`storage_path`) |
| `campaign_attachments` | Vínculo N:N entre campañas y adjuntos |
| `email_attachments` | Auditoría: qué adjunto se envió con qué email |

---

## Variables de Entorno

| Variable | Requerida | Descripción |
|----------|-----------|-------------|
| `DATABASE_URL` | ✅ | Connection string de Supabase |
| `RESEND_API_KEY` | ✅ | API key de Resend |
| `GROQ_API_KEY` | ✅ | API key de Groq |
| `SUPABASE_URL` | ❌ (req. para adjuntos) | URL del proyecto Supabase |
| `SUPABASE_SERVICE_KEY` | ❌ (req. para adjuntos) | **service_role JWT** (`eyJ...`) — NO publishable ni `sb_secret_` |
| `STORAGE_BUCKET` | ❌ | Bucket de Storage (default: `email_attachments`) |
| `APP_USER` | ✅ | Usuario del login |
| `APP_PASSWORD` | ✅ | Contraseña del login |
| `SECRET_KEY` | ⚠️ (auto en Render) | Firma de tokens de sesión |
| `SENDER_NAME` | ❌ | Nombre visible del remitente |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS` | ❌ | Legado (Resend reemplaza SMTP) |
| `IMAP_HOST` / `IMAP_USER` / `IMAP_PASS` | ❌ | Servidor IMAP para bandeja |
| `IMAP_PORT` | ❌ | Default: 993 |
| `TOKEN_TTL_HOURS` | ❌ | Duración de sesión (default: 8) |
| `COOKIE_SECURE` | ❌ | Forzar cookie Secure (default: auto según Render) |

---

## Desarrollo Local

```bash
# 1. Clonar e instalar dependencias
git clone <repo>
pip install -r requirements.txt

# 2. Configurar variables de entorno
cp .env.example .env
# editar .env con tus credenciales (ver Variables de Entorno arriba)

# 3. Inicializar base de datos
# Ejecutar migrations.sql en Supabase SQL Editor

# 4. Iniciar servidor
python api.py
# → http://localhost:8000
```

---

## Deploy en Render

Render detecta automáticamente `render.yaml`. Variables sensibles se configuran manualmente en el dashboard:

- `APP_USER`, `APP_PASSWORD` — credenciales de login
- `DATABASE_URL` — connection string de Supabase
- `RESEND_API_KEY` — API key de Resend
- `SENDER_NAME` — nombre del remitente
- `IMAP_HOST`, `IMAP_USER`, `IMAP_PASS` — servidor IMAP
- `GROQ_API_KEY` — API key de Groq

> `SECRET_KEY` se genera automáticamente por Render.  
> Ejemplo completo de `.env` disponible en [`.env.example`](.env.example).

---

## Notas Técnicas

<details>
<summary><strong>🔧 Conexión IPv4 forzada para Supabase</strong></summary>

El pooler de Supabase puede devolver una dirección IPv6 en el DNS lookup. En entornos cloud sin soporte completo de IPv6 esto causa `connection refused` o timeouts.

La función `_make_ipv4_connection()` en `api.py` resuelve el hostname forzando `socket.AF_INET` y usa `hostaddr` para conectar directamente por IPv4, manteniendo `host` para el SNI correcto en TLS:

```python
addrinfo = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
ipv4 = addrinfo[0][4][0]

conn = psycopg2.connect(
    host=hostname,   # hostname real → SNI en TLS (Supabase lo necesita para identificar el tenant)
    hostaddr=ipv4,   # IP IPv4 → libpq conecta directo, sin hacer un nuevo DNS lookup
    sslmode='require',
    ...
)
```
</details>

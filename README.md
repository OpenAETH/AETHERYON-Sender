# AETHERYON Outreach Sender

Producto de AETHERYON para **preparar, programar, enviar y supervisar
comunicaciones profesionales**, con plantillas HTML reutilizables.

Principio rector: el Consultor prepara la comunicación, programa la máquina
y se va — AETHERYON sigue comunicando todos los días, de forma profesional,
controlada, reproducible y escalable.

Refactor de [Emailer-Agent], reorganizado en capas y con persistencia
exclusiva en Supabase/Postgres (sin SQLite). Ver `MIGRATION_NOTES.md` para
el detalle de qué cambió y por qué.

## Arquitectura

```
Agenda  →  Redacción + Templates  →  Cron Sender  →  Supervisión
```

- **Agenda**: gestión de destinatarios (no es un CRM). LeadForge, Apollo o
  cualquier otra fuente externa entregan una lista (CSV/JSON) que se importa
  por upsert — no son módulos internos del Sender.
- **Redacción**: composición puntual — HTML, texto alternativo, variables,
  adjuntos, preview y envío de prueba. Soporta dos modos: **Markdown**
  (se envuelve en la plantilla visual de AETHERYON) y **HTML pegado**
  (un template ya diseñado se envía tal cual, sin reprocesar).
- **Templates**: biblioteca de plantillas reutilizables — crear, editar,
  duplicar, activar, archivar, con variables `{{first_name}}`, `{{company}}`,
  `{{role}}`, `{{sender_name}}`, `{{cta_url}}`.
- **Cron Sender**: motor de programación y cola de envío por destinatario
  (`send_queue`), con pausa, reanudación, cancelación, reintentos con
  backoff y límite de emails por corrida.
- **Supervisión**: torre de control — enviados, programados, pendientes,
  fallidos, reintentos, respuestas (detección por hilo) y estado del sistema.
- **Bandeja**: componente secundario (IMAP), no domina la aplicación.

## Estructura del backend

```
backend/
  main.py         # ensamblado de la app FastAPI, lifespan, estático
  config.py       # configuración centralizada desde variables de entorno
  api/            # routers HTTP (uno por dominio)
  domain/         # lógica de negocio pura, sin I/O
  services/       # orquestación: domain + providers + db
  providers/      # integraciones externas (Resend, Supabase Storage, IMAP, Groq)
  db/             # conexión Postgres + schema.sql
  workers/        # loop de fondo del Cron Sender
```

## Puesta en marcha

### 1. Base de datos (Supabase)

En Supabase → SQL Editor, correr **`backend/db/schema.sql`** completo. Es
idempotente — se puede volver a correr sin duplicar datos. Si venís de
Emailer-Agent con campañas usadas como plantilla (`status='template'`), el
mismo script las migra automáticamente a la tabla `templates`.

### 2. Variables de entorno

Copiá `.env.example` a `.env` y completá:
- `DATABASE_URL` (Supabase, pooler o conexión directa)
- `RESEND_API_KEY`, `SENDER_EMAIL`, `SENDER_NAME`
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` (service_role, no la publishable ni la `sb_secret_`), `STORAGE_BUCKET`
- `IMAP_*` (opcional, para la Bandeja)
- `GROQ_API_KEY` (opcional, asistencia de redacción)
- `APP_USER`, `APP_PASSWORD`, `SECRET_KEY`

### 3. Local con Docker

```bash
docker compose up --build
```

Abrí `http://localhost:8000`.

### 4. Local sin Docker

```bash
pip install -r requirements.txt
uvicorn backend.main:app --reload
```

### 5. Producción (Render)

`render.yaml` ya define el build/start command y las variables necesarias
(quedan marcadas `sync: false` para que las completes en el dashboard).

## Notas operativas

- El Cron Sender corre **dentro del mismo proceso** de la app (ver
  `workers/scheduler.py`). El Dockerfile y `render.yaml` están configurados
  para un único worker/proceso a propósito: con más de uno, cada email se
  enviaría una vez por worker.
- La cola evalúa fechas en **UTC** (`datetime.utcnow()`).
- Los límites de envío por corrida (`SENDER_RATE_LIMIT_PER_RUN`), reintentos
  (`SENDER_MAX_ATTEMPTS`) y backoff (`SENDER_RETRY_BACKOFF_MINUTES`) se
  configuran por variable de entorno.

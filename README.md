# Emailer Agent

Asistente ejecutivo de email con backend FastAPI, frontend SPA vanilla y deploy en Render. Gestión de bandeja de entrada, envío individual, campañas de email marketing generadas por IA y supervisión de comunicaciones.

## Stack

- **Backend** — Python / FastAPI / Uvicorn
- **Frontend** — SPA vanilla JS (un solo `index.html`, sin frameworks)
- **Base de datos** — PostgreSQL via Supabase (SQLAlchemy 2.0 + psycopg2)
- **Envío de email** — Resend API
- **Bandeja de entrada** — IMAP SSL
- **IA / LLM** — Groq (Llama 3.3 70b) para generación de campañas
- **Deploy** — Render.com

---

## Estructura

```
api.py            Backend FastAPI — sirve API REST y frontend SPA
index.html        Frontend SPA completo
requirements.txt  Dependencias Python (versiones pinneadas)
render.yaml       Infraestructura como código para Render
.env.example      Variables de entorno de referencia
```

---

## Requisitos previos

1. **Supabase** — crear un proyecto y ejecutar el schema SQL (ver sección [Base de datos](#base-de-datos))
2. **Resend** — cuenta con dominio remitente verificado
3. **Groq** — API key gratuita en [console.groq.com](https://console.groq.com)
4. **Servidor IMAP** — para sincronización de bandeja de entrada (opcional)

---

## Deploy en Render

1. Subir el repo a GitHub
2. En Render: **New → Web Service → conectar repo**
3. Render detecta el `render.yaml` automáticamente
4. Configurar las variables marcadas como `sync: false` en el dashboard de Render:

| Variable | Descripción |
|---|---|
| `APP_USER` | Usuario para el login de la app |
| `APP_PASSWORD` | Contraseña para el login |
| `DATABASE_URL` | Connection string de Supabase |
| `RESEND_API_KEY` | API key de Resend |
| `SENDER_NAME` | Nombre visible en el campo "De:" |
| `IMAP_HOST` | Hostname del servidor IMAP |
| `IMAP_USER` | Usuario IMAP |
| `IMAP_PASS` | Contraseña IMAP |
| `GROQ_API_KEY` | API key de Groq |

> `SECRET_KEY` se genera automáticamente por Render (`generateValue: true`). Para desarrollo local, generarla con:
> ```bash
> python -c "import secrets; print(secrets.token_hex(32))"
> ```

### Variables de entorno completas

```env
# Auth
APP_USER=admin
APP_PASSWORD=tu_password_seguro
SECRET_KEY=...generado automáticamente en Render...
TOKEN_TTL_HOURS=8

# Base de datos
DATABASE_URL=postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres

# Resend
RESEND_API_KEY=re_xxxxxxxxxxxxxxxxxxxx
SENDER_NAME=Nombre Remitente

# IMAP
IMAP_HOST=mail.tudominio.com
IMAP_PORT=993
IMAP_USER=contacto@tudominio.com
IMAP_PASS=tu_password_imap

# Groq
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx
```

---

## Base de datos

Crear las siguientes tablas en Supabase antes del primer deploy. El código no crea el schema automáticamente.

```sql
CREATE TABLE contacts (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    company    TEXT, role TEXT, phone TEXT, context TEXT, tags TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE email_logs (
    id            SERIAL PRIMARY KEY,
    direction     TEXT NOT NULL,
    contact_email TEXT, contact_name TEXT,
    subject       TEXT, body TEXT, body_html TEXT,
    intent        TEXT, status TEXT DEFAULT 'sent',
    sent_at       TIMESTAMPTZ, received_at TIMESTAMPTZ, replied_at TIMESTAMPTZ,
    message_id    TEXT, thread_id TEXT, campaign_id TEXT,
    ai_suggestion TEXT,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE inbox_cache (
    id            SERIAL PRIMARY KEY,
    message_id    TEXT UNIQUE NOT NULL,
    imap_uid      TEXT,
    from_email    TEXT, from_name TEXT,
    subject       TEXT, body TEXT, date TEXT,
    read          INTEGER DEFAULT 0,
    replied       INTEGER DEFAULT 0,
    ai_suggestion TEXT,
    fetched_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE memory (
    id         SERIAL PRIMARY KEY,
    type       TEXT NOT NULL,
    entity     TEXT, content TEXT NOT NULL,
    importance INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE sessions (
    token      TEXT PRIMARY KEY,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
);

-- Campañas
CREATE TABLE campaigns (
    id         SERIAL PRIMARY KEY,
    name       TEXT, intent TEXT,
    start_date TEXT, end_date TEXT,
    send_mode  TEXT, tone TEXT,
    status     TEXT DEFAULT 'draft',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE campaign_emails (
    id                SERIAL PRIMARY KEY,
    campaign_id       INTEGER REFERENCES campaigns(id),
    day_number        INTEGER,
    subject           TEXT, body TEXT,
    status            TEXT DEFAULT 'pending',
    scheduled_at      TEXT,
    version           INTEGER DEFAULT 1,
    regenerated_count INTEGER DEFAULT 0
);

CREATE TABLE campaign_contacts (
    id          SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaigns(id),
    email       TEXT
);
```

---

## Funcionalidades

### Envío de email individual

`POST /send-email` acepta un destinatario o múltiples:

```json
{ "to": "persona@ejemplo.com", "subject": "Asunto", "body": "Cuerpo en Markdown" }
{ "recipients": ["a@x.com", "b@x.com"], "subject": "Asunto", "body": "..." }
```

Devuelve resultados individuales por destinatario. El cuerpo acepta Markdown y se convierte a HTML con el template configurado en Settings.

### Bandeja de entrada — IMAP sync

`GET /inbox?refresh=true` sincroniza con el servidor IMAP usando UIDs:

- Agrega los mensajes nuevos que no están en la DB local
- Elimina de la DB los mensajes que ya no existen en el servidor (borrados desde otro cliente)
- Preserva `ai_suggestion` y `replied` de mensajes que siguen existiendo

### Campañas de email marketing

Flujo completo con revisión humana antes del envío:

```
1. Generar   →  POST /campaigns/generate
                intent + tone + fechas + contactos + modo de envío
                Groq genera la secuencia completa de emails

2. Revisar   →  Aprobar o rechazar cada email individualmente
                POST /campaigns/{id}/approve-email

3. Regenerar →  Si un email no convence, pedirle a Groq que lo reescriba
                POST /campaigns/{id}/retry-email  { "feedback": "más corto y directo" }

4. Finalizar →  Valida que no queden emails pendientes ni rechazados
                POST /campaigns/{id}/finalize

5. Enviar    →  POST /campaigns/{id}/send-now
                Envía todos los aprobados a todos los contactos vía Resend
```

**Modos de envío:**

| `send_mode` | Frecuencia |
|---|---|
| `daily` | Todos los días |
| `alternate` | Día por medio |
| `mon_wed_fri` | Lunes, miércoles y viernes |
| `tue_thu` | Martes y jueves |

**Tonos:**

| `tone` | Estilo |
|---|---|
| `informative` | Educativo, construcción de confianza |
| `aggressive` | Persuasivo, CTA fuerte, urgencia |

### Supervisión

`GET /supervision` devuelve los emails enviados con tiempo transcurrido desde el envío y si ya recibieron respuesta.

---

## Endpoints

### Auth
| Método | Ruta | Auth requerida |
|---|---|---|
| POST | `/auth/login` | No |
| POST | `/auth/logout` | Sí |
| GET | `/auth/check` | Sí |

### Email
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/send-email` | Envío individual o múltiple |
| POST | `/preview-email` | Renderiza HTML del email con estilos actuales |

### Bandeja
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/inbox` | Lista mensajes. `?refresh=true` sincroniza IMAP. |
| POST | `/inbox/mark-replied` | Marca un mensaje como respondido |
| POST | `/inbox/save-suggestion` | Guarda sugerencia de IA en un mensaje |

### Contactos
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/contacts` | Lista todos los contactos |
| POST | `/contacts` | Crear contacto (`name` y `email` requeridos) |
| PUT | `/contacts/{id}` | Actualizar contacto |
| DELETE | `/contacts/{id}` | Eliminar contacto |

### Campañas
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/campaigns` | Lista campañas con conteos por estado |
| POST | `/campaigns/generate` | Genera campaña completa con Groq |
| GET | `/campaigns/{id}` | Detalle con emails y contactos |
| DELETE | `/campaigns/{id}` | Elimina campaña y sus datos |
| POST | `/campaigns/{id}/approve-email` | Aprueba o rechaza un email |
| POST | `/campaigns/{id}/retry-email` | Regenera un email con feedback opcional |
| POST | `/campaigns/{id}/finalize` | Valida y marca como scheduled |
| POST | `/campaigns/{id}/send-now` | Envía todos los emails aprobados |

### Otros
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/logs` | Historial de envíos. `?limit=N` |
| GET | `/supervision` | Emails enviados con seguimiento de respuesta |
| GET | `/stats` | Conteos: contactos, enviados, inbox, respondidos, memoria |
| GET / POST | `/settings` | Estilos del template de email y firma HTML |
| GET / POST | `/context` | Contexto de entidad/misión usado en prompts de IA |
| GET | `/memory` | Registro de eventos internos |
| GET | `/api/status` | Estado de Resend e IMAP |

---

## Nota técnica — conexión IPv4 forzada para Supabase

El pooler de Supabase puede devolver una dirección IPv6 en el DNS lookup. En entornos cloud sin soporte completo de IPv6 esto causa `connection refused` o timeouts. La función `_make_ipv4_connection()` resuelve el hostname forzando `socket.AF_INET` y usa `hostaddr` para conectar directamente por IPv4, manteniendo `host` para el SNI correcto en TLS.

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

---

## Desarrollo local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env
# editar .env con tus credenciales

# Iniciar
python api.py
# → http://localhost:8000
```

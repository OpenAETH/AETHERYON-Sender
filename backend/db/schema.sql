-- ============================================================
--  AETHERYON Outreach Sender — Esquema de base de datos
--  Motor: PostgreSQL (Supabase). NO se usa SQLite en ningún módulo.
--  Ejecutar en: Supabase → SQL Editor → New Query
--  Idempotente: seguro de correr varias veces (CREATE IF NOT EXISTS /
--  ADD COLUMN IF NOT EXISTS). Reemplaza a migrations.sql del proyecto
--  original (Emailer-Agent) consolidando todo en un único archivo.
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- AUTH / SETTINGS
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    token       TEXT PRIMARY KEY,
    created_at  BIGINT NOT NULL,
    expires_at  BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key    TEXT PRIMARY KEY,
    value  TEXT
);

-- ────────────────────────────────────────────────────────────
-- AGENDA (destinatarios) — gestión simple de contactos, NO es un CRM.
-- Se conservan las columnas históricas (status/tipo/medio/next_followup)
-- por compatibilidad de datos; la UI ya no las expone como pipeline
-- de ventas, solo como metadatos opcionales de segmentación.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contacts (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    email          TEXT NOT NULL UNIQUE,
    company        TEXT DEFAULT '',
    role           TEXT DEFAULT '',
    phone          TEXT DEFAULT '',
    context        TEXT DEFAULT '',
    tags           TEXT DEFAULT '',
    status         TEXT DEFAULT 'nuevo',
    tipo           TEXT DEFAULT 'prensa',
    medio          TEXT,
    last_contact   TEXT,
    next_followup  TEXT,
    notes          TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now()
);

-- Origen del contacto: manual | import-csv | import-json (reemplaza al
-- acople directo con LeadForge/Apollo — ahora son fuentes externas que
-- entregan una lista, no módulos internos del Sender).
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual';

CREATE TABLE IF NOT EXISTS contact_interactions (
    id         SERIAL PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    type       TEXT,
    note       TEXT,
    date       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_contact_interactions_cid ON contact_interactions(contact_id);

-- ────────────────────────────────────────────────────────────
-- ADJUNTOS (Supabase Storage)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS attachments (
    id            SERIAL PRIMARY KEY,
    filename      VARCHAR(255) NOT NULL,
    content_type  VARCHAR(100) NOT NULL DEFAULT 'application/octet-stream',
    size          INTEGER NOT NULL DEFAULT 0,
    storage_path  TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- ────────────────────────────────────────────────────────────
-- REGISTRO DE ENVÍOS / MEMORIA / BANDEJA
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS email_logs (
    id            SERIAL PRIMARY KEY,
    direction     TEXT NOT NULL,              -- 'out' | 'in'
    contact_email TEXT,
    contact_name  TEXT,
    subject       TEXT,
    body          TEXT,
    body_html     TEXT,
    intent        TEXT,
    status        TEXT,
    sent_at       TIMESTAMPTZ,
    campaign_id   INTEGER,
    message_id    TEXT,                       -- Message-ID de Resend, para threading preciso
    created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_email_logs_campaign ON email_logs(campaign_id);
CREATE INDEX IF NOT EXISTS idx_email_logs_contact ON email_logs(contact_email);

CREATE TABLE IF NOT EXISTS email_attachments (
    id                 SERIAL PRIMARY KEY,
    attachment_id      INTEGER NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
    email_log_id       INTEGER REFERENCES email_logs(id) ON DELETE CASCADE,
    campaign_email_id  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_email_attachments_log ON email_attachments(email_log_id);
CREATE INDEX IF NOT EXISTS idx_email_attachments_camp ON email_attachments(campaign_email_id);

CREATE TABLE IF NOT EXISTS inbox_cache (
    message_id     TEXT PRIMARY KEY,
    imap_uid       TEXT,
    from_email     TEXT,
    from_name      TEXT,
    subject        TEXT,
    body           TEXT,
    date           TEXT,
    replied        INTEGER DEFAULT 0,
    ai_suggestion  TEXT,
    in_reply_to    TEXT
);

CREATE TABLE IF NOT EXISTS memory (
    id          SERIAL PRIMARY KEY,
    type        TEXT,
    entity      TEXT,
    content     TEXT,
    importance  INTEGER DEFAULT 1,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ────────────────────────────────────────────────────────────
-- TEMPLATES — biblioteca de plantillas HTML reutilizables (Redacción).
-- Entidad propia, independiente de las campañas ("Cron Sender").
-- Variables soportadas: {{first_name}} {{company}} {{role}}
--                        {{sender_name}} {{cta_url}}  (+ cualquier
--                        otra {{clave}} libre, se resuelve en blanco
--                        si no hay dato disponible al momento de usar).
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS templates (
    id                 SERIAL PRIMARY KEY,
    name               TEXT NOT NULL,
    subject            TEXT NOT NULL DEFAULT '',
    body               TEXT NOT NULL DEFAULT '',       -- Markdown o HTML completo, según content_type
    content_type       TEXT NOT NULL DEFAULT 'markdown', -- markdown | html (HTML ya diseñado, pegado tal cual)
    variables          JSONB NOT NULL DEFAULT '[]'::jsonb,  -- detectadas automáticamente al guardar
    category           TEXT DEFAULT '',
    status             TEXT NOT NULL DEFAULT 'active',  -- active | archived
    default_cta_url    TEXT DEFAULT '',
    source_template_id INTEGER REFERENCES templates(id) ON DELETE SET NULL,  -- lineage al duplicar
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_templates_status ON templates(status);

-- ────────────────────────────────────────────────────────────
-- CRON SENDER — programación y cola de envío.
-- `campaigns` = una comunicación programada (una o varias piezas en
-- secuencia). `campaign_emails` = cada pieza (día N) con su contenido.
-- `campaign_contacts` = destinatarios asignados. `send_queue` = cola
-- real de envío por destinatario, con reintentos y estado individual.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS campaigns (
    id           SERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    intent       TEXT DEFAULT '',
    start_date   TEXT,
    end_date     TEXT,
    send_mode    TEXT DEFAULT 'daily',       -- daily | alternate | mon_wed_fri | tue_thu
    tone         TEXT DEFAULT 'informative',
    send_time    TEXT DEFAULT '09:00',
    status       TEXT NOT NULL DEFAULT 'draft',  -- draft|scheduled|paused|sent|cancelled|template(legacy)
    created_at   TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_time TEXT DEFAULT '09:00';
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS template_id INTEGER REFERENCES templates(id) ON DELETE SET NULL;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS rate_limit_per_run INTEGER NOT NULL DEFAULT 50;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS cta_url TEXT DEFAULT '';

CREATE TABLE IF NOT EXISTS campaign_emails (
    id                  SERIAL PRIMARY KEY,
    campaign_id         INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    day_number          INTEGER NOT NULL,
    subject             TEXT DEFAULT '',
    body                TEXT DEFAULT '',
    content_type        TEXT NOT NULL DEFAULT 'markdown',  -- markdown | html (pegado, ya diseñado)
    status              TEXT NOT NULL DEFAULT 'pending',   -- pending|approved|rejected (aprobación de contenido)
    scheduled_at        TEXT,                              -- 'YYYY-MM-DD HH:MM' (UTC)
    sent_at             TIMESTAMPTZ,
    send_status         TEXT DEFAULT 'pending',            -- pending|sent|failed|cancelled (agregado legacy)
    version             INTEGER DEFAULT 1,
    regenerated_count   INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_campaign_emails_cid ON campaign_emails(campaign_id);
ALTER TABLE campaign_emails ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'markdown';
ALTER TABLE templates ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'markdown';

CREATE TABLE IF NOT EXISTS campaign_contacts (
    id           SERIAL PRIMARY KEY,
    campaign_id  INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    email        TEXT NOT NULL,
    variables    JSONB NOT NULL DEFAULT '{}'::jsonb  -- valores personalizados de este destinatario para ESTA comunicacion (ej. {"hipotesis":"...","señal":"..."})
);
CREATE INDEX IF NOT EXISTS idx_campaign_contacts_cid ON campaign_contacts(campaign_id);
ALTER TABLE campaign_contacts ADD COLUMN IF NOT EXISTS variables JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS campaign_attachments (
    id            SERIAL PRIMARY KEY,
    campaign_id   INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    attachment_id INTEGER NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
    UNIQUE(campaign_id, attachment_id)
);
CREATE INDEX IF NOT EXISTS idx_campaign_attachments_camp ON campaign_attachments(campaign_id);

-- Cola de envío por destinatario: la unidad real que procesa el Cron Sender.
-- Se puebla al programar/finalizar una campaña (fan-out de
-- campaign_contacts × campaign_emails). Permite pausa, reintentos con
-- backoff y cancelación granular sin tocar el contenido ni la lista base.
CREATE TABLE IF NOT EXISTS send_queue (
    id                 SERIAL PRIMARY KEY,
    campaign_id        INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    campaign_email_id  INTEGER NOT NULL REFERENCES campaign_emails(id) ON DELETE CASCADE,
    recipient_email    TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'pending',  -- pending|sent|failed|cancelled
    attempts           INTEGER NOT NULL DEFAULT 0,
    max_attempts       INTEGER NOT NULL DEFAULT 3,
    last_error         TEXT,
    sent_at            TIMESTAMPTZ,
    next_attempt_at    TIMESTAMPTZ,
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE(campaign_email_id, recipient_email)
);
CREATE INDEX IF NOT EXISTS idx_send_queue_due ON send_queue(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_send_queue_campaign ON send_queue(campaign_id);

-- ============================================================
--  Migración de datos opcional (ejecutar UNA sola vez si el proyecto
--  viene de Emailer-Agent y tenía campañas usadas como plantilla,
--  es decir campaigns.status = 'template'). Es idempotente: no
--  duplica si ya existe una plantilla con el mismo nombre origen.
-- ============================================================
INSERT INTO templates (name, subject, body, category, status, created_at)
SELECT c.name,
       COALESCE((SELECT ce.subject FROM campaign_emails ce WHERE ce.campaign_id = c.id ORDER BY ce.day_number LIMIT 1), ''),
       COALESCE((SELECT ce.body    FROM campaign_emails ce WHERE ce.campaign_id = c.id ORDER BY ce.day_number LIMIT 1), ''),
       'migrado-desde-campañas',
       'active',
       c.created_at
FROM campaigns c
WHERE c.status = 'template'
  AND NOT EXISTS (SELECT 1 FROM templates t WHERE t.name = c.name AND t.category = 'migrado-desde-campañas');

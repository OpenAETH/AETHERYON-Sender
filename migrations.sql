
-- ============================================================
--  Emailer Agent — Migración: módulo CRM sobre contactos
--  Ejecutar en: Supabase → SQL Editor → New Query
--  Idempotente: seguro de correr varias veces.
-- ============================================================

-- ── Extender la tabla contacts con los campos del pipeline CRM ──
-- Se reusan company/phone existentes; solo se añaden los campos nuevos.
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS status        TEXT DEFAULT 'nuevo';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS tipo          TEXT DEFAULT 'prensa';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS medio         TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_contact  TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS next_followup TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS notes         TEXT;

-- ── Historial de interacciones del CRM ──
-- ON DELETE CASCADE: al borrar un contacto se borran sus interacciones.
CREATE TABLE IF NOT EXISTS contact_interactions (
    id         SERIAL PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    type       TEXT,
    note       TEXT,
    date       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contact_interactions_cid ON contact_interactions(contact_id);

-- ── Plantillas de campañas pre-aprobadas ──
-- Una plantilla es una campaña con status='template'. Reusa campaigns/campaign_emails.
-- send_time persiste la hora de envío de la cadencia (antes solo iba embebida en scheduled_at).
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_time TEXT DEFAULT '09:00';
-- ============================================================
--  Emailer Agent — Migración: módulo CRM sobre contactos
--  Ejecutar en: Supabase → SQL Editor → New Query
--  Idempotente: seguro de correr varias veces.
-- ============================================================

-- ── Extender la tabla contacts con los campos del pipeline CRM ──
-- Se reusan company/phone existentes; solo se añaden los campos nuevos.
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS status        TEXT DEFAULT 'nuevo';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS tipo          TEXT DEFAULT 'prensa';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS medio         TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_contact  TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS next_followup TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS notes         TEXT;

-- ── Historial de interacciones del CRM ──
-- ON DELETE CASCADE: al borrar un contacto se borran sus interacciones.
CREATE TABLE IF NOT EXISTS contact_interactions (
    id         SERIAL PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    type       TEXT,
    note       TEXT,
    date       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contact_interactions_cid ON contact_interactions(contact_id);

-- ============================================================
--  Migración: attachments (adjuntos para emails y campañas)
--  Ejecutar en: Supabase → SQL Editor → New Query
--  Idempotente: seguro de correr varias veces.
-- ============================================================

-- ── Archivos adjuntos subidos a Supabase Storage ──
CREATE TABLE IF NOT EXISTS attachments (
    id            SERIAL PRIMARY KEY,
    filename      VARCHAR(255) NOT NULL,
    content_type  VARCHAR(100) NOT NULL DEFAULT 'application/octet-stream',
    size          INTEGER NOT NULL DEFAULT 0,
    storage_path  TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- ── Relación attachments → email_logs (envíos individuales) ──
CREATE TABLE IF NOT EXISTS email_attachments (
    id            SERIAL PRIMARY KEY,
    attachment_id INTEGER NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
    email_log_id  INTEGER REFERENCES email_logs(id) ON DELETE CASCADE,
    campaign_email_id INTEGER REFERENCES campaign_emails(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_email_attachments_log ON email_attachments(email_log_id);
CREATE INDEX IF NOT EXISTS idx_email_attachments_camp ON email_attachments(campaign_email_id);

-- ── Relación attachments → campañas (adjuntos globales de campaña) ──
CREATE TABLE IF NOT EXISTS campaign_attachments (
    id            SERIAL PRIMARY KEY,
    campaign_id   INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    attachment_id INTEGER NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
    UNIQUE(campaign_id, attachment_id)
);

CREATE INDEX IF NOT EXISTS idx_campaign_attachments_camp ON campaign_attachments(campaign_id);

-- ============================================================
--  Migración: trazabilidad de envíos de campaña, por destinatario
--  Ejecutar en: Supabase → SQL Editor → New Query
--  Idempotente: seguro de correr varias veces.
-- ============================================================

-- ── Registro por destinatario de cada envío de campaña ──
-- Antes solo se marcaba campaign_emails.sent_at (agregado, a nivel del email
-- completo). Esta tabla responde ¿a quién? ¿cuándo? ¿ok/falló? por envío.
-- ON DELETE CASCADE: al borrar la campaña o el email, se borra su historial.
CREATE TABLE IF NOT EXISTS campaign_email_sends (
    id                SERIAL PRIMARY KEY,
    campaign_email_id INTEGER NOT NULL REFERENCES campaign_emails(id) ON DELETE CASCADE,
    campaign_id       INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    contact_email     TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'sent',   -- 'sent' | 'failed'
    error             TEXT,
    sent_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ces_campaign ON campaign_email_sends(campaign_id);
CREATE INDEX IF NOT EXISTS idx_ces_email    ON campaign_email_sends(campaign_email_id);
CREATE INDEX IF NOT EXISTS idx_ces_contact  ON campaign_email_sends(contact_email);

-- ── Estilos de email nuevos (banner/footer) ──
-- No requieren esquema: se guardan como filas key/value en la tabla `settings`
-- vía set_setting(). Documentados aquí para referencia:
--   style_logo_url    → URL de logo para el banner (fallback: nombre remitente)
--   style_footer_text → texto personalizado del footer (fallback: "Enviado desde …")

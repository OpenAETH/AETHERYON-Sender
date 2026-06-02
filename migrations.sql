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

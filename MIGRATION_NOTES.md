# Notas de migración — Emailer-Agent → AETHERYON Outreach Sender

## Qué se eliminó

- **SQLite / `leadforge.db`**: eliminado por completo. No queda ningún
  import de `sqlite3` ni archivo `.db` en el repo. LeadForge y Apollo pasan
  a ser fuentes externas: entregan una lista (CSV/JSON) que se importa por
  UPSERT vía `POST /agenda/import`. Antes: `/contacts/leadforge-status`,
  `/contacts/import-leadforge`, `/contacts/by-category` (leían el archivo
  `leadforge.db` embebido en el repo).
- **CRM como centro del producto**: se sacó el tablero kanban (`view-crm`,
  columnas nuevo/contactado/en conversación/interesado/cerrado, drag&drop)
  del nav y del código. La Agenda quedó como gestión simple de destinatarios:
  lista, búsqueda, alta/edición/baja, archivado, e importación — sin
  pipeline de ventas. El campo `status` de `contacts` se conserva en la
  base (no se perdió nada), pero ya no es el eje de la navegación.
- **Campañas usadas como plantilla** (`campaigns.status='template'`, y los
  endpoints `/campaigns/templates`, `/campaigns/{id}/save-as-template`,
  `/campaigns/templates/{id}/use`): reemplazado por una entidad `templates`
  dedicada, con su propio ciclo de vida (activar/archivar/duplicar) y
  soporte de variables. El `schema.sql` migra automáticamente, en un único
  `INSERT ... SELECT` idempotente, las campañas-plantilla existentes a la
  tabla nueva.

## Qué se agregó

- **`send_queue`**: cola de envío por destinatario (no por lote). Es la
  pieza que faltaba para que el Cron Sender pudiera cumplir con "cola,
  límites, pausa, reanudación, cancelación, reintentos y estados de envío"
  tal como se pidió — la implementación anterior no distinguía el estado de
  entrega por destinatario individual.
- **Pausa / reanudación / cancelación de una comunicación programada**
  completa (`POST /campaigns/{id}/pause|resume|cancel`), además del
  cancelar/reprogramar por pieza que ya existía.
- **Reintentos con backoff** automáticos (`SENDER_RETRY_BACKOFF_MINUTES *
  intento`) y **límite de envíos por corrida** (`rate_limit_per_run` por
  comunicación, default `SENDER_RATE_LIMIT_PER_RUN`).
- **`content_type` (`markdown` | `html`)** en Redacción, Templates y en cada
  pieza de una comunicación programada. En modo `html` el contenido pegado
  se envía tal cual (sin envolver en la plantilla visual ni reprocesar como
  Markdown) — pensado para pegar templates ya diseñados en otra herramienta.
  Igual soporta variables (`{{first_name}}`, etc.) y genera un texto plano
  de respaldo automáticamente.
- **Envío de prueba** (`POST /send-test`) desde el modal de confirmación de
  Redacción, antes de programar el envío real.
- **Supervisión unificada**: antes, los envíos hechos por una campaña no
  quedaban registrados en `email_logs` (solo los envíos manuales desde
  Redacción). Ahora todo pasa por el mismo registro, así Supervisión
  muestra un único historial con detección de respuestas por hilo,
  independientemente de si el envío fue manual o programado. Se agregó
  `GET /supervision/summary` como panorama operativo agregado (cola +
  estado de proveedores).
- **`templates` con variables detectadas automáticamente**
  (`{{first_name}}`, `{{company}}`, `{{role}}`, `{{sender_name}}`,
  `{{cta_url}}`, o cualquier otra que uses) y lineage al duplicar
  (`source_template_id`).

## Compatibilidad conservada a propósito

- Las rutas `/contacts/*`, `/campaigns/*`, `/send-email`, `/supervision`,
  etc. se mantuvieron donde no había un motivo funcional para cambiarlas —
  es un detalle interno de la API, no algo que el usuario vea, y cambiarlas
  sin necesidad hubiera sido una reescritura innecesaria del frontend.
- El HTML de armado de email (`md_to_html`, `build_html_email`) es el mismo
  algoritmo del proyecto original, solo movido a `domain/email_content.py`
  como función pura.
- Resend como proveedor de envío, Supabase Storage para adjuntos, IMAP para
  la bandeja, Docker para local y Render para producción — todo se mantuvo.

## Bugs reales corregidos de paso

- `python-dotenv` se usaba (`from dotenv import load_dotenv`) pero no
  estaba en `requirements.txt` — agregado.
- `render.yaml` no declaraba `RESEND_API_KEY`, `SENDER_EMAIL`,
  `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `GROQ_API_KEY` ni `DATABASE_URL`
  como variables de entorno del servicio — agregadas (con `sync: false`
  para completarlas a mano en el dashboard, y `SECRET_KEY` autogenerada).
- Un modal de "usar plantilla" viejo (ligado a la campaña-como-plantilla
  legacy) compartía el mismo `id` de DOM y los mismos nombres de función
  (`loadTemplates`, `deleteTemplate`, `openUseTemplate`,
  `id="btnSaveTemplate"`) que la nueva pantalla de Templates. Se eliminó
  por completo esa ruta legacy — de haber quedado, pisaba silenciosamente
  la funcionalidad nueva.

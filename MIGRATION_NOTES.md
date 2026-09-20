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
- `db/connection.py` leía `DATABASE_URL` directo de `os.environ`, sin
  garantizar que `.env` ya estuviera cargado — funcionaba en producción
  solo por el orden accidental de imports de `backend/main.py`. Ahora
  `DATABASE_URL` se centraliza en `config.py` (que sí llama
  `load_dotenv()`), como ya se documentaba que debía ser.
- El fallback "usar el prefijo del email como nombre" para destinatarios
  sin ficha en la Agenda nunca se activaba: `process_due` y
  `render_for_recipient` pasaban `contact=None`/`{}` en vez de
  `{"email": to}`, así que `build_context` no tenía de dónde derivarlo.
- La regex de detección de variables (`{{clave}}`) solo aceptaba
  `[a-zA-Z0-9_]`, así que una variable como `{{señal}}` (con ñ) no se
  reconocía como variable en absoluto y quedaba literal, sin renderizar,
  en el email final. Ahora usa `\w+` con Unicode.

## Variables personalizadas por destinatario (Templates → Cron Sender)

Cada plantilla declara sus variables (`{{clave}}`) y cada una se clasifica
en `domain/templates.classify_variables()`:
- **Auto-mapeadas**: `{{nombre}}`/`{{first_name}}`/`{{name}}`,
  `{{empresa}}`/`{{company}}`/`{{nombre_empresa}}`,
  `{{cargo}}`/`{{role}}`/`{{puesto}}`, `{{remitente}}`/`{{sender_name}}`,
  `{{link}}`/`{{cta_url}}` — se resuelven solas desde la Agenda o la
  configuración, sea cual sea el alias que use la plantilla.
- **Personalizadas**: cualquier otra clave (ej. `{{hipotesis}}`,
  `{{señal}}`) — no hay forma de derivarlas de un contacto genérico, así
  que se cargan a mano por destinatario al programar el envío ("Usar
  plantilla" en el Cron Sender), y se guardan en
  `campaign_contacts.variables` (JSONB), específicas de esa comunicación.

El asistente de "Usar plantilla" detecta qué emails pegados no están en la
Agenda y pide sus datos (nombre/empresa) para registrarlos de una, además
de la tabla de variables personalizadas por fila con vista previa
individual por destinatario antes de programar.

## Verificación de Agenda y Templates (botones y flujos reales)

Instalé Postgres real en el entorno de trabajo y probé cada botón de
ambas vistas contra la API real (login, CRUD completo, casos de error),
no solo lectura de código. Bugs reales encontrados y corregidos:

- **Editar contacto no guardaba el email.** `update_contact()` nunca
  incluía `email` en el `UPDATE` — el formulario lo mostraba editable, el
  toast decía "Actualizado", pero el valor viejo quedaba intacto en la
  base. Se agregó al `SET`, con manejo de conflicto (409 limpio) si el
  nuevo email ya pertenece a otro contacto.
- **"Archivar" no tenía vuelta atrás.** Era una acción de un solo sentido:
  no existía botón "Activar", ni indicación visual de qué contacto estaba
  archivado, ni filtro para volver a encontrarlo. Ahora es un toggle
  (Archivar ⇄ Activar) y los archivados se muestran atenuados con badge.
- **"Duplicar" plantilla tiraba error 500.** `duplicate_template()` leía
  la columna `variables` (jsonb) ya parseada por psycopg2 a una lista de
  Python, y la reinsertaba tal cual sin volver a serializarla con
  `json.dumps()` — Postgres la interpretaba como `text[]` en vez de
  `jsonb` y rechazaba el INSERT. Confirmado con el error real de Postgres,
  no solo por lectura de código.

## Manejador global de excepciones (robustez de errores)

Antes, cualquier excepción no anticipada (columna faltante en la base por
no haber corrido el `schema.sql` más reciente, caída de conexión, etc.)
salía de Starlette como **texto plano** ("Internal Server Error"), no
JSON. El frontend siempre asume JSON en las respuestas de error
(`const e=await r.json()`), así que en vez de mostrar el error real
terminaba mostrando `Unexpected token 'I', "Internal S"... is not valid
JSON` — un mensaje que no dice nada sobre la causa real.

Se agregó `@app.exception_handler(Exception)` en `backend/main.py`: loguea
el traceback completo (visible en los logs de Render) y siempre devuelve
JSON con el tipo de excepción. Se confirmó que esto **no** interfiere con
los `HTTPException` normales (400/404/409 siguen funcionando igual) — solo
atrapa lo que antes rompía sin control. Si ves "Error interno (X)" en un
toast, el detalle completo está en los logs del servidor.

**Si el guardado de templates (u otro endpoint) sigue dando 500 en Render:
lo más probable es que el schema de Supabase esté desactualizado — volvé a
correr `backend/db/schema.sql` completo (es idempotente, no rompe datos
existentes).**

## Nuevo / Editar contacto ahora es un modal real

El formulario de la Agenda era un `<div style="display:none">` que se
mostraba/ocultaba inline dentro de la vista — el único diálogo de toda la
app que no seguía el patrón `.modal-backdrop` usado en Enviar, Vista
previa, Usar plantilla, etc. Se convirtió a `id="modalContact"`, abierto
con `openModal()`/`closeModal()` igual que el resto. El envío de datos
(`saveContact()`) no cambió — mismos campos, mismos endpoints, mismo
comportamiento, solo cambió cómo se muestra.

## Importar archivo .html en Templates

Se agregó un botón "↑ Importar archivo" en el formulario de plantillas:
lee un `.html`/`.htm` en el navegador (`FileReader`, sin pasar por
Storage) y lo carga directo en el campo de contenido existente, pasando a
modo HTML automáticamente. Si el archivo tiene `<title>`, se usa como
asunto sugerido cuando el campo está vacío. No se agregó un storage de
archivos aparte para el contenido de las plantillas: un email HTML tiene
que pesar poco de por sí (Gmail recorta pasado ~102KB), así que la columna
`TEXT` en Postgres ya es la opción correcta — separarlo a Storage solo
agregaría un round-trip extra y el riesgo de archivos huérfanos, sin
ningún beneficio real de tamaño.

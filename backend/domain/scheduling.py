"""
domain/scheduling.py — Reglas de cadencia y estado del Cron Sender.

Funciones puras (sin DB, sin HTTP). Levantan ValueError ante datos
inválidos; la capa api/ los traduce a HTTPException(400, ...).
"""
from datetime import date, timedelta, datetime

SEND_MODES = ("daily", "alternate", "mon_wed_fri", "tue_thu")


def _includes(current: date, start: date, send_mode: str) -> bool:
    wd = current.weekday()  # 0=lun ... 6=dom
    if send_mode == "daily":
        return True
    if send_mode == "alternate":
        return (current - start).days % 2 == 0
    if send_mode == "mon_wed_fri":
        return wd in (0, 2, 4)
    if send_mode == "tue_thu":
        return wd in (1, 3)
    return True


def get_dates_in_range(start_date: str, end_date: str, send_mode: str) -> list:
    """Genera la lista de fechas válidas (YYYY-MM-DD) según la cadencia,
    entre start_date y end_date (inclusive)."""
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError:
        raise ValueError("Fechas inválidas. Usar formato YYYY-MM-DD")
    if start > end:
        raise ValueError("start_date debe ser anterior a end_date")

    dates = []
    current = start
    while current <= end:
        if _includes(current, start, send_mode):
            dates.append(str(current))
        current += timedelta(days=1)
    return dates


def get_dates_count(start_date: str, send_mode: str, count: int) -> list:
    """Genera EXACTAMENTE `count` fechas válidas avanzando desde start_date
    según la cadencia. Usado al instanciar una plantilla o secuencia: la
    duración es consecuencia de N comunicaciones + cadencia, no al revés."""
    try:
        start = date.fromisoformat(start_date)
    except ValueError:
        raise ValueError("Fecha de inicio inválida. Usar formato YYYY-MM-DD")
    if count <= 0:
        return []

    dates = []
    current = start
    max_iter = count * 14 + 366
    iters = 0
    while len(dates) < count and iters < max_iter:
        if _includes(current, start, send_mode):
            dates.append(str(current))
        current += timedelta(days=1)
        iters += 1
    return dates


def compute_queue_state(status: str, attempts: int, max_attempts: int, next_attempt_at, now: datetime = None) -> str:
    """Estado visual de un ítem de `send_queue` para Supervisión:
    sent | cancelled | failed | retrying | scheduled | delayed.

    `failed` es terminal (se agotaron los reintentos). Mientras quedan
    reintentos disponibles, un ítem que falló vuelve a `pending` con
    `next_attempt_at` en el futuro — eso se muestra como `retrying`.
    """
    now = now or datetime.utcnow()
    if status == "sent":
        return "sent"
    if status == "cancelled":
        return "cancelled"
    if status == "failed":
        return "failed"
    # status == "pending"
    if attempts > 0:
        return "retrying"
    if next_attempt_at and next_attempt_at <= now:
        return "delayed"
    return "scheduled"

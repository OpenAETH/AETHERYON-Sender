"""
workers/scheduler.py — Loop de background del Cron Sender.

Corre dentro del mismo proceso de la app (por diseño: con múltiples workers
cada email se enviaría una vez por worker, duplicando envíos — ver Dockerfile).
Cada `SCHEDULER_INTERVAL_SECONDS` procesa la cola de envío (`send_queue`) y
registra la hora de la última corrida para que Supervisión pueda mostrar el
estado del sistema.
"""
import asyncio
import logging
from datetime import datetime

from backend.config import SCHEDULER_INTERVAL_SECONDS
from backend.services import sender_service
from backend.services.settings_service import set_setting

logger = logging.getLogger(__name__)


async def scheduler_loop():
    await asyncio.sleep(5)  # pequeño delay al inicio para que la DB esté lista
    while True:
        try:
            sender_service.process_due()
            set_setting("scheduler_last_run", datetime.utcnow().isoformat(timespec="seconds"))
        except Exception as e:
            logger.error(f"[scheduler_loop] Error inesperado: {e}")
        await asyncio.sleep(SCHEDULER_INTERVAL_SECONDS)

FROM python:3.12.7-slim

# Evita .pyc y fuerza logs sin buffer (necesario para ver el log del Cron Sender en tiempo real)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

# psycopg2-binary trae sus propias libs, pero curl es útil para el healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias primero para aprovechar la cache de capas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la app (incluye backend/ e index.html)
COPY . .

# La app sirve el frontend desde static/ — copiarlo en build (backend/main.py
# también lo hace en runtime, pero así queda disponible aunque cambie el
# orden de arranque)
RUN mkdir -p static && cp -f index.html static/index.html 2>/dev/null || true

EXPOSE 8000

# IMPORTANTE: un solo proceso/worker. El Cron Sender corre dentro del
# proceso de la app; con múltiples workers cada email se enviaría una vez
# por worker (duplicados). Forma shell (no exec) para que $PORT se expanda
# (Render asigna el puerto dinámicamente).
CMD uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000}

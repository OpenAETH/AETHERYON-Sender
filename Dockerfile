FROM python:3.12-slim

# Evita .pyc y fuerza logs sin buffer (necesario para ver el log del scheduler en tiempo real)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

# psycopg2-binary trae sus propias libs, pero curl es util para el healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias primero para aprovechar la cache de capas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la app
COPY . .

# La app sirve el frontend desde static/ — copiarlo en build (api.py tambien lo
# hace en runtime, pero asi queda disponible aunque cambie el orden de arranque)
RUN mkdir -p static && cp -f index.html static/index.html 2>/dev/null || true

EXPOSE 8000

# IMPORTANTE: un solo proceso. El scheduler corre dentro del proceso de la app;
# con multiples workers cada email se enviaria una vez por worker (duplicados).
CMD ["python", "api.py"]

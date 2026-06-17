# syntax=docker/dockerfile:1.7

FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /build

RUN python -m venv /opt/venv

COPY requirements.txt .

RUN /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"
ENV APP_HOST=0.0.0.0
ENV APP_PORT=8000
ENV APP_MODULE=iot_app.main:app
ENV AUTH_TOKEN=local-dev-token
ENV SERVICE_NAME=iot-ingestion
ENV SERVICE_VERSION=0.5.0
ENV AI_SERVICE_URL=http://ai-service:9000
ENV DB_HOST=db
ENV DB_PORT=5432
ENV POSTGRES_USER=lab05
ENV POSTGRES_PASSWORD=lab05pass
ENV POSTGRES_DB=iotdb

WORKDIR /app

RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup --home /app appuser

COPY --from=builder /opt/venv /opt/venv
COPY src/ ./src/
COPY data/ ./data/

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen(f\"http://127.0.0.1:{os.getenv('APP_PORT', '8000')}/health\", timeout=3).read()" || exit 1

CMD ["sh", "-c", "uvicorn ${APP_MODULE} --app-dir src --host ${APP_HOST} --port ${APP_PORT}"]

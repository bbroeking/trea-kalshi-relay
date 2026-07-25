FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    REFRESH_SECONDS=30 \
    MAXIMUM_AGE_SECONDS=120 \
    POLYMARKET_RECONCILE_SECONDS=60 \
    RELAY_MODE=continuous-websocket \
    ARCHIVE_PATH=/data/relay.sqlite \
    REQUIRE_ARCHIVE=1 \
    REQUIRE_CLOCK_QUALITY=1 \
    REQUIRE_FEE_REGIMES=1 \
    MAXIMUM_CLOCK_AGE_SECONDS=120 \
    MAXIMUM_CLOCK_UNCERTAINTY_MS=750

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY archive.py clock_quality.py collector.py service.py ./

RUN useradd --create-home --uid 10001 relay \
    && mkdir /data \
    && chown relay:relay /data
USER relay

EXPOSE 8080
CMD ["python", "service.py"]

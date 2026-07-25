FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    REFRESH_SECONDS=30 \
    MAXIMUM_AGE_SECONDS=120 \
    POLYMARKET_RECONCILE_SECONDS=60 \
    RELAY_MODE=continuous-websocket \
    ARCHIVE_PATH=/data/relay.sqlite \
    REQUIRE_ARCHIVE=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY archive.py collector.py service.py ./

RUN useradd --create-home --uid 10001 relay \
    && mkdir /data \
    && chown relay:relay /data
USER relay

EXPOSE 8080
CMD ["python", "service.py"]

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    REFRESH_SECONDS=30 \
    MAXIMUM_AGE_SECONDS=120 \
    RELAY_MODE=continuous-http

WORKDIR /app
COPY collector.py service.py ./

RUN useradd --create-home --uid 10001 relay
USER relay

EXPOSE 8080
CMD ["python", "service.py"]

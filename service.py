#!/usr/bin/env python3
"""Continuously refresh and serve the public TREA market snapshot."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

from collector import collect


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class CollectorState:
    collect_once: Callable[[], dict[str, Any]] = collect
    lock: threading.Lock = field(default_factory=threading.Lock)
    payload: dict[str, Any] | None = None
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_success_monotonic: float | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    total_refreshes: int = 0

    def refresh(self) -> None:
        attempted_at = utc_now()
        try:
            payload = self.collect_once()
        except Exception as exc:
            with self.lock:
                self.last_attempt_at = attempted_at
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.consecutive_failures += 1
            return
        with self.lock:
            self.payload = payload
            self.last_attempt_at = attempted_at
            self.last_success_at = utc_now()
            self.last_success_monotonic = time.monotonic()
            self.last_error = None
            self.consecutive_failures = 0
            self.total_refreshes += 1

    def snapshot(self, maximum_age_seconds: float) -> tuple[dict[str, Any], bool]:
        with self.lock:
            age = (
                time.monotonic() - self.last_success_monotonic
                if self.last_success_monotonic is not None
                else None
            )
            healthy = age is not None and age <= maximum_age_seconds
            return (
                {
                    "healthy": healthy,
                    "ageSeconds": round(age, 3) if age is not None else None,
                    "lastAttemptAt": self.last_attempt_at,
                    "lastSuccessAt": self.last_success_at,
                    "lastError": self.last_error,
                    "consecutiveFailures": self.consecutive_failures,
                    "totalRefreshes": self.total_refreshes,
                },
                healthy,
            )


def collector_loop(
    state: CollectorState,
    stop: threading.Event,
    interval_seconds: float,
) -> None:
    while not stop.is_set():
        started = time.monotonic()
        state.refresh()
        remaining = max(0.0, interval_seconds - (time.monotonic() - started))
        stop.wait(remaining)


def handler_class(
    state: CollectorState,
    maximum_age_seconds: float,
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(
                payload, separators=(",", ":"), sort_keys=True
            ).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            health, healthy = state.snapshot(maximum_age_seconds)
            if path in {"/health", "/healthz"}:
                self.send_json(200 if healthy else 503, health)
                return
            if path in {"/", "/data/tonight.json"}:
                with state.lock:
                    payload = state.payload
                if payload is None:
                    self.send_json(
                        503,
                        {"error": "no successful collection yet", **health},
                    )
                    return
                self.send_json(
                    200,
                    {
                        **payload,
                        "serviceHealth": health,
                    },
                )
                return
            self.send_json(404, {"error": "not found"})

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("PORT", "8080"))
    )
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=float(os.environ.get("REFRESH_SECONDS", "30")),
    )
    parser.add_argument(
        "--maximum-age-seconds",
        type=float,
        default=float(os.environ.get("MAXIMUM_AGE_SECONDS", "120")),
    )
    args = parser.parse_args()
    if args.refresh_seconds <= 0 or args.maximum_age_seconds <= 0:
        parser.error("refresh and maximum age must be positive")

    state = CollectorState()
    stop = threading.Event()
    worker = threading.Thread(
        target=collector_loop,
        args=(state, stop, args.refresh_seconds),
        name="collector",
        daemon=True,
    )
    worker.start()
    server = ThreadingHTTPServer(
        ("0.0.0.0", args.port),
        handler_class(state, args.maximum_age_seconds),
    )

    def shutdown(*_: object) -> None:
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever()
    finally:
        stop.set()
        worker.join(timeout=5)
        server.server_close()


if __name__ == "__main__":
    main()

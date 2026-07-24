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

POLYMARKET_MARKET_WS = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class CollectorState:
    collect_once: Callable[[], dict[str, Any]] = collect
    lock: Any = field(default_factory=threading.RLock)
    payload: dict[str, Any] | None = None
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_success_monotonic: float | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    total_refreshes: int = 0
    websocket_connected: bool = False
    websocket_tokens: int = 0
    websocket_messages: int = 0
    websocket_reconnects: int = 0
    websocket_last_message_at: str | None = None
    websocket_last_message_monotonic: float | None = None
    websocket_error: str | None = None

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
            source_health = payload.setdefault("sourceHealth", {})
            source_health["relay"] = "continuous-websocket"
            source_health["polymarketTransport"] = "websocket+rest"
            self.payload = payload
            self.last_attempt_at = attempted_at
            self.last_success_at = utc_now()
            self.last_success_monotonic = time.monotonic()
            self.last_error = None
            self.consecutive_failures = 0
            self.total_refreshes += 1

    def token_ids(self) -> tuple[str, ...]:
        with self.lock:
            return tuple(
                str(outcome["tokenId"])
                for market in (self.payload or {}).get("polymarket", [])
                for outcome in market.get("outcomes", [])
                if outcome.get("tokenId")
            )

    def set_websocket_status(
        self,
        *,
        connected: bool,
        tokens: int,
        error: str | None = None,
        reconnect: bool = False,
    ) -> None:
        with self.lock:
            self.websocket_connected = connected
            self.websocket_tokens = tokens
            self.websocket_error = error
            if reconnect:
                self.websocket_reconnects += 1

    def apply_websocket_event(self, event: dict[str, Any]) -> bool:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return False
        event_type = event.get("event_type") or event.get("type")
        changed = False
        with self.lock:
            for market in (self.payload or {}).get("polymarket", []):
                for outcome in market.get("outcomes", []):
                    if str(outcome.get("tokenId")) != asset_id:
                        continue
                    if event_type == "book":
                        bids = event.get("bids") or []
                        asks = event.get("asks") or []
                        if bids:
                            level = max(
                                bids,
                                key=lambda item: float(item["price"]),
                            )
                            outcome["bid"] = {
                                "price": float(level["price"]),
                                "size": float(level["size"]),
                            }
                        if asks:
                            level = min(
                                asks,
                                key=lambda item: float(item["price"]),
                            )
                            outcome["ask"] = {
                                "price": float(level["price"]),
                                "size": float(level["size"]),
                            }
                        changed = bool(bids or asks)
                    elif event_type in {"price_change", "best_bid_ask"}:
                        if event.get("best_bid") not in {None, ""}:
                            previous = outcome.get("bid") or {}
                            outcome["bid"] = {
                                "price": float(event["best_bid"]),
                                "size": previous.get("size"),
                            }
                            changed = True
                        if event.get("best_ask") not in {None, ""}:
                            previous = outcome.get("ask") or {}
                            outcome["ask"] = {
                                "price": float(event["best_ask"]),
                                "size": previous.get("size"),
                            }
                            changed = True
                    elif (
                        event_type == "last_trade_price"
                        and event.get("price") is not None
                    ):
                        outcome["lastTradePrice"] = float(event["price"])
                        changed = True
                    if changed:
                        observed_at = utc_now()
                        outcome["websocketObservedAt"] = observed_at
                        outcome["bookTimestamp"] = event.get("timestamp")
                    break
        if changed:
            with self.lock:
                self.websocket_messages += 1
                self.websocket_last_message_at = utc_now()
                self.websocket_last_message_monotonic = time.monotonic()
                self.websocket_error = None
        return changed

    def snapshot(self, maximum_age_seconds: float) -> tuple[dict[str, Any], bool]:
        with self.lock:
            age = (
                time.monotonic() - self.last_success_monotonic
                if self.last_success_monotonic is not None
                else None
            )
            websocket_age = (
                time.monotonic() - self.websocket_last_message_monotonic
                if self.websocket_last_message_monotonic is not None
                else None
            )
            websocket_required = bool(self.token_ids())
            websocket_healthy = (
                not websocket_required
                or (
                    self.websocket_connected
                    and websocket_age is not None
                )
            )
            healthy = (
                age is not None
                and age <= maximum_age_seconds
                and websocket_healthy
            )
            return (
                {
                    "healthy": healthy,
                    "ageSeconds": round(age, 3) if age is not None else None,
                    "lastAttemptAt": self.last_attempt_at,
                    "lastSuccessAt": self.last_success_at,
                    "lastError": self.last_error,
                    "consecutiveFailures": self.consecutive_failures,
                    "totalRefreshes": self.total_refreshes,
                    "websocket": {
                        "required": websocket_required,
                        "healthy": websocket_healthy,
                        "connected": self.websocket_connected,
                        "tokens": self.websocket_tokens,
                        "messages": self.websocket_messages,
                        "reconnects": self.websocket_reconnects,
                        "lastMessageAt": self.websocket_last_message_at,
                        "ageSeconds": (
                            round(websocket_age, 3)
                            if websocket_age is not None
                            else None
                        ),
                        "error": self.websocket_error,
                    },
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


def message_events(payload: Any) -> list[dict[str, Any]]:
    parents = (
        [payload]
        if isinstance(payload, dict)
        else [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, list)
        else []
    )
    events = []
    for parent in parents:
        changes = parent.get("price_changes")
        if (
            parent.get("event_type") == "price_change"
            and isinstance(changes, list)
        ):
            common = {
                key: value
                for key, value in parent.items()
                if key != "price_changes"
            }
            events.extend(
                {**common, **change}
                for change in changes
                if isinstance(change, dict)
            )
        else:
            events.append(parent)
    return events


def websocket_loop(state: CollectorState, stop: threading.Event) -> None:
    try:
        from websockets.sync.client import connect
    except ImportError as exc:
        state.set_websocket_status(
            connected=False,
            tokens=0,
            error=f"ImportError: {exc}",
        )
        return
    delay = 1.0
    while not stop.is_set():
        tokens = state.token_ids()
        if not tokens:
            state.set_websocket_status(connected=False, tokens=0)
            stop.wait(1)
            continue
        try:
            with connect(
                POLYMARKET_MARKET_WS,
                ping_interval=10,
                ping_timeout=10,
                close_timeout=5,
                max_size=16 * 1024 * 1024,
            ) as websocket:
                websocket.send(
                    json.dumps(
                        {
                            "assets_ids": list(tokens),
                            "type": "market",
                            "custom_feature_enabled": True,
                        },
                        separators=(",", ":"),
                    )
                )
                state.set_websocket_status(
                    connected=True,
                    tokens=len(tokens),
                )
                delay = 1.0
                while not stop.is_set():
                    if state.token_ids() != tokens:
                        break
                    try:
                        raw_message = websocket.recv(timeout=1)
                    except TimeoutError:
                        continue
                    if raw_message in {"PING", "PONG"}:
                        continue
                    payload = json.loads(raw_message)
                    for event in message_events(payload):
                        state.apply_websocket_event(event)
        except Exception as exc:
            state.set_websocket_status(
                connected=False,
                tokens=len(tokens),
                error=f"{type(exc).__name__}: {exc}",
                reconnect=True,
            )
            stop.wait(delay)
            delay = min(30.0, delay * 2)
        else:
            state.set_websocket_status(
                connected=False,
                tokens=len(tokens),
                reconnect=not stop.is_set(),
            )


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
    websocket_worker = threading.Thread(
        target=websocket_loop,
        args=(state, stop),
        name="polymarket-websocket",
        daemon=True,
    )
    websocket_worker.start()
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
        websocket_worker.join(timeout=10)
        server.server_close()


if __name__ == "__main__":
    main()

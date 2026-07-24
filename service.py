#!/usr/bin/env python3
"""Continuously refresh and serve the public TREA market snapshot."""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

from collector import collect

POLYMARKET_MARKET_WS = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
KALSHI_MARKET_WS = (
    "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
)
KALSHI_WS_PATH = "/trade-api/ws/v2"


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
    kalshi_websocket_configured: bool = False
    kalshi_websocket_required: bool = False
    kalshi_websocket_connected: bool = False
    kalshi_websocket_markets: int = 0
    kalshi_websocket_messages: int = 0
    kalshi_websocket_reconnects: int = 0
    kalshi_websocket_sequence_gaps: int = 0
    kalshi_websocket_last_message_at: str | None = None
    kalshi_websocket_last_message_monotonic: float | None = None
    kalshi_websocket_error: str | None = None

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
            source_health["kalshiTransport"] = (
                "websocket+rest"
                if self.kalshi_websocket_configured
                else "rest"
            )
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

    def market_tickers(self) -> tuple[str, ...]:
        with self.lock:
            return tuple(
                str(outcome["ticker"])
                for event in (self.payload or {}).get("parity", [])
                for outcome in event.get("outcomes", [])
                if outcome.get("ticker")
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

    def set_kalshi_websocket_status(
        self,
        *,
        connected: bool,
        markets: int,
        error: str | None = None,
        reconnect: bool = False,
        sequence_gap: bool = False,
    ) -> None:
        with self.lock:
            self.kalshi_websocket_connected = connected
            self.kalshi_websocket_markets = markets
            self.kalshi_websocket_error = error
            if connected:
                # A reconnect cannot reuse a prior session's freshness.
                self.kalshi_websocket_last_message_at = None
                self.kalshi_websocket_last_message_monotonic = None
            if reconnect:
                self.kalshi_websocket_reconnects += 1
            if sequence_gap:
                self.kalshi_websocket_sequence_gaps += 1

    def apply_kalshi_book(
        self,
        ticker: str,
        book: tuple[
            float | None,
            float | None,
            float | None,
            float | None,
        ],
        event: dict[str, Any],
    ) -> bool:
        if book[0] is None or book[1] is None:
            return False
        changed = False
        with self.lock:
            for market in (self.payload or {}).get("parity", []):
                for outcome in market.get("outcomes", []):
                    if str(outcome.get("ticker")) != ticker:
                        continue
                    outcome["bid"] = {
                        "price": book[0],
                        "size": book[2],
                    }
                    outcome["ask"] = {
                        "price": book[1],
                        "size": book[3],
                    }
                    message = event.get("msg") or {}
                    outcome["websocketObservedAt"] = utc_now()
                    outcome["bookTimestamp"] = (
                        message.get("ts_ms") or message.get("ts")
                    )
                    outcome["bookSequence"] = event.get("seq")
                    outcome["bookPricing"] = "unified_yes"
                    changed = True
                    break
            if changed:
                self.kalshi_websocket_messages += 1
                self.kalshi_websocket_last_message_at = utc_now()
                self.kalshi_websocket_last_message_monotonic = (
                    time.monotonic()
                )
                self.kalshi_websocket_error = None
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
            kalshi_websocket_age = (
                time.monotonic()
                - self.kalshi_websocket_last_message_monotonic
                if self.kalshi_websocket_last_message_monotonic is not None
                else None
            )
            kalshi_stream_required = (
                self.kalshi_websocket_required
                and bool(self.market_tickers())
            )
            kalshi_websocket_healthy = (
                not kalshi_stream_required
                or (
                    self.kalshi_websocket_configured
                    and self.kalshi_websocket_connected
                    and kalshi_websocket_age is not None
                )
            )
            healthy = (
                age is not None
                and age <= maximum_age_seconds
                and websocket_healthy
                and kalshi_websocket_healthy
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
                    "kalshiWebsocket": {
                        "configured": self.kalshi_websocket_configured,
                        "required": kalshi_stream_required,
                        "healthy": kalshi_websocket_healthy,
                        "connected": self.kalshi_websocket_connected,
                        "markets": self.kalshi_websocket_markets,
                        "messages": self.kalshi_websocket_messages,
                        "reconnects": self.kalshi_websocket_reconnects,
                        "sequenceGaps": (
                            self.kalshi_websocket_sequence_gaps
                        ),
                        "lastMessageAt": (
                            self.kalshi_websocket_last_message_at
                        ),
                        "ageSeconds": (
                            round(kalshi_websocket_age, 3)
                            if kalshi_websocket_age is not None
                            else None
                        ),
                        "error": self.kalshi_websocket_error,
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


class KalshiSequenceError(RuntimeError):
    pass


class KalshiSequenceTracker:
    def __init__(self) -> None:
        self.last_by_sid: dict[str, int] = {}

    def observe(self, event: dict[str, Any]) -> None:
        if event.get("type") not in {
            "orderbook_snapshot",
            "orderbook_delta",
        }:
            return
        if event.get("sid") is None or event.get("seq") is None:
            raise KalshiSequenceError("order-book message omitted sid or seq")
        sid = str(event["sid"])
        sequence = int(event["seq"])
        previous = self.last_by_sid.get(sid)
        if previous is not None and sequence != previous + 1:
            raise KalshiSequenceError(
                f"sid {sid} expected seq {previous + 1}, got {sequence}"
            )
        self.last_by_sid[sid] = sequence


class KalshiBookState:
    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.initialized = False

    @staticmethod
    def levels(value: Any) -> dict[Decimal, Decimal]:
        return {
            Decimal(str(level[0])): Decimal(str(level[1]))
            for level in (value or [])
            if len(level) >= 2 and Decimal(str(level[1])) > 0
        }

    def apply(
        self,
        event: dict[str, Any],
    ) -> tuple[
        float | None,
        float | None,
        float | None,
        float | None,
    ]:
        message = event.get("msg") or {}
        if event.get("type") == "orderbook_snapshot":
            self.bids = self.levels(
                message.get("yes_dollars_fp")
                or message.get("yes_dollars")
            )
            self.asks = self.levels(
                message.get("no_dollars_fp")
                or message.get("no_dollars")
            )
            self.initialized = True
        elif (
            event.get("type") == "orderbook_delta"
            and self.initialized
        ):
            side = str(message.get("side") or "").lower()
            price_value = message.get(
                "price_dollars", message.get("price")
            )
            delta_value = message.get("delta_fp", message.get("delta"))
            if (
                side in {"yes", "no"}
                and price_value is not None
                and delta_value is not None
            ):
                levels = self.bids if side == "yes" else self.asks
                price = Decimal(str(price_value))
                size = levels.get(price, Decimal(0)) + Decimal(
                    str(delta_value)
                )
                if size > 0:
                    levels[price] = size
                else:
                    levels.pop(price, None)
        if not self.initialized:
            return None, None, None, None
        bid = max(self.bids) if self.bids else None
        ask = min(self.asks) if self.asks else None
        return (
            float(bid) if bid is not None else None,
            float(ask) if ask is not None else None,
            float(self.bids[bid]) if bid is not None else None,
            float(self.asks[ask]) if ask is not None else None,
        )


def kalshi_websocket_headers(
    api_key_id: str,
    private_key_pem: bytes,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    timestamp = str(
        timestamp_ms
        if timestamp_ms is not None
        else int(time.time() * 1000)
    )
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=None,
    )
    signature = private_key.sign(
        f"{timestamp}GET{KALSHI_WS_PATH}".encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


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


def kalshi_websocket_loop(
    state: CollectorState,
    stop: threading.Event,
    api_key_id: str,
    private_key_pem: bytes,
) -> None:
    try:
        from websockets.sync.client import connect
    except ImportError as exc:
        state.set_kalshi_websocket_status(
            connected=False,
            markets=0,
            error=f"ImportError: {exc}",
        )
        return
    delay = 1.0
    while not stop.is_set():
        tickers = state.market_tickers()
        if not tickers:
            state.set_kalshi_websocket_status(
                connected=False,
                markets=0,
            )
            stop.wait(1)
            continue
        sequence = KalshiSequenceTracker()
        books: dict[str, KalshiBookState] = {}
        try:
            with connect(
                KALSHI_MARKET_WS,
                additional_headers=kalshi_websocket_headers(
                    api_key_id,
                    private_key_pem,
                ),
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=16 * 1024 * 1024,
            ) as websocket:
                websocket.send(
                    json.dumps(
                        {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": list(tickers),
                                "use_yes_price": True,
                            },
                        },
                        separators=(",", ":"),
                    )
                )
                state.set_kalshi_websocket_status(
                    connected=True,
                    markets=len(tickers),
                )
                delay = 1.0
                while not stop.is_set():
                    if state.market_tickers() != tickers:
                        break
                    try:
                        raw_message = websocket.recv(timeout=1)
                    except TimeoutError:
                        continue
                    event = json.loads(raw_message)
                    event_type = event.get("type")
                    if event_type == "error":
                        message = event.get("msg") or {}
                        raise RuntimeError(
                            f"Kalshi error {message.get('code')}: "
                            f"{message.get('msg')}"
                        )
                    if event_type not in {
                        "orderbook_snapshot",
                        "orderbook_delta",
                    }:
                        continue
                    try:
                        sequence.observe(event)
                    except KalshiSequenceError as exc:
                        state.set_kalshi_websocket_status(
                            connected=False,
                            markets=len(tickers),
                            error=str(exc),
                            sequence_gap=True,
                        )
                        raise
                    message = event.get("msg") or {}
                    ticker = str(message.get("market_ticker") or "")
                    if not ticker:
                        raise RuntimeError(
                            f"{event_type} omitted market_ticker"
                        )
                    book = books.setdefault(
                        ticker,
                        KalshiBookState(),
                    ).apply(event)
                    state.apply_kalshi_book(ticker, book, event)
        except Exception as exc:
            state.set_kalshi_websocket_status(
                connected=False,
                markets=len(tickers),
                error=f"{type(exc).__name__}: {exc}",
                reconnect=True,
            )
            stop.wait(delay)
            delay = min(30.0, delay * 2)
        else:
            state.set_kalshi_websocket_status(
                connected=False,
                markets=len(tickers),
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


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_kalshi_credentials() -> tuple[str | None, bytes | None]:
    key_id = os.environ.get("KALSHI_API_KEY_ID")
    encoded_key = os.environ.get("KALSHI_PRIVATE_KEY_B64")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if encoded_key:
        private_key = base64.b64decode(encoded_key)
    elif key_path:
        with open(key_path, "rb") as handle:
            private_key = handle.read()
    else:
        private_key = None
    return key_id, private_key


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

    kalshi_key_id, kalshi_private_key = load_kalshi_credentials()
    kalshi_configured = bool(kalshi_key_id and kalshi_private_key)
    kalshi_required = env_flag("REQUIRE_KALSHI_WEBSOCKET")
    state = CollectorState(
        kalshi_websocket_configured=kalshi_configured,
        kalshi_websocket_required=kalshi_required,
    )
    if kalshi_required and not kalshi_configured:
        state.set_kalshi_websocket_status(
            connected=False,
            markets=0,
            error=(
                "Kalshi WebSocket required but KALSHI_API_KEY_ID and "
                "KALSHI_PRIVATE_KEY_B64 or KALSHI_PRIVATE_KEY_PATH "
                "are not both configured"
            ),
        )
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
    kalshi_websocket_worker = None
    if (
        kalshi_configured
        and kalshi_key_id is not None
        and kalshi_private_key is not None
    ):
        kalshi_websocket_worker = threading.Thread(
            target=kalshi_websocket_loop,
            args=(
                state,
                stop,
                kalshi_key_id,
                kalshi_private_key,
            ),
            name="kalshi-websocket",
            daemon=True,
        )
        kalshi_websocket_worker.start()
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
        if kalshi_websocket_worker is not None:
            kalshi_websocket_worker.join(timeout=10)
        server.server_close()


if __name__ == "__main__":
    main()

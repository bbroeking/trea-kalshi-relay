#!/usr/bin/env python3
"""Continuously refresh and serve the public TREA market snapshot."""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

from archive import EventArchive
from clock_quality import ClockSample, sample_clock
from collector import collect, polymarket_books

POLYMARKET_MARKET_WS = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
KALSHI_MARKET_WS = (
    "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
)
KALSHI_WS_PATH = "/trade-api/ws/v2"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PolymarketContinuityError(RuntimeError):
    pass


class PolymarketBookState:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.initialized = False
        self.last_hash: str | None = None
        self.last_source_timestamp: int | None = None

    @staticmethod
    def levels(value: Any) -> dict[float, float]:
        return {
            float(level["price"]): float(level["size"])
            for level in (value or [])
            if float(level["size"]) > 0
        }

    @staticmethod
    def timestamp(value: Any) -> int | None:
        if value in {None, ""}:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def apply(
        self,
        event: dict[str, Any],
    ) -> tuple[
        float | None,
        float | None,
        float | None,
        float | None,
    ]:
        event_type = event.get("event_type") or event.get("type")
        if event_type == "book":
            self.bids = self.levels(event.get("bids"))
            self.asks = self.levels(event.get("asks"))
            self.initialized = True
        elif event_type == "price_change" and self.initialized:
            side = str(event.get("side") or "").upper()
            if (
                side in {"BUY", "SELL"}
                and event.get("price") is not None
                and event.get("size") is not None
            ):
                levels = self.bids if side == "BUY" else self.asks
                price = float(event["price"])
                size = float(event["size"])
                if size > 0:
                    levels[price] = size
                else:
                    levels.pop(price, None)
        if event.get("hash") not in {None, ""}:
            self.last_hash = str(event["hash"])
        source_timestamp = self.timestamp(event.get("timestamp"))
        if source_timestamp is not None:
            self.last_source_timestamp = source_timestamp
        return self.top()

    def top(
        self,
    ) -> tuple[
        float | None,
        float | None,
        float | None,
        float | None,
    ]:
        if not self.initialized:
            return None, None, None, None
        bid = max(self.bids) if self.bids else None
        ask = min(self.asks) if self.asks else None
        return (
            bid,
            ask,
            self.bids.get(bid) if bid is not None else None,
            self.asks.get(ask) if ask is not None else None,
        )

    def reconcile(self, snapshot: dict[str, Any]) -> str:
        if not self.initialized:
            return "uninitialized"
        snapshot_hash = (
            str(snapshot["hash"])
            if snapshot.get("hash") not in {None, ""}
            else None
        )
        snapshot_timestamp = self.timestamp(snapshot.get("timestamp"))
        if (
            snapshot_hash is not None
            and self.last_hash is not None
            and snapshot_hash == self.last_hash
        ):
            return "verified"
        if (
            snapshot_timestamp is not None
            and self.last_source_timestamp is not None
            and snapshot_timestamp < self.last_source_timestamp
        ):
            return "stale"
        if (
            snapshot_timestamp is not None
            and self.last_source_timestamp is not None
            and snapshot_timestamp > self.last_source_timestamp
        ):
            return "advanced"
        return "mismatch"


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
    polymarket_connection_generation: int = 0
    websocket_last_message_at: str | None = None
    websocket_last_message_monotonic: float | None = None
    websocket_error: str | None = None
    websocket_reconciliations: int = 0
    websocket_hash_matches: int = 0
    websocket_stale_reconciliations: int = 0
    websocket_reconciliation_failures: int = 0
    polymarket_books: dict[str, PolymarketBookState] = field(
        default_factory=dict
    )
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
    archive: EventArchive | None = None
    archive_required: bool = False
    clock_required: bool = False
    maximum_clock_age_seconds: float = 120.0
    clock_sample: ClockSample | None = None
    clock_samples: int = 0
    clock_error: str | None = None

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
        if self.archive is not None:
            self.archive.append(
                source="rest",
                market_id=None,
                event_type="snapshot",
                sequence=None,
                payload=payload,
                received_at=attempted_at,
            )

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
        observed_at = utc_now()
        observed_monotonic_ns = time.monotonic_ns()
        with self.lock:
            was_connected = self.websocket_connected
            if connected:
                self.polymarket_connection_generation += 1
                self.websocket_last_message_at = None
                self.websocket_last_message_monotonic = None
                self.polymarket_books = {}
            generation = self.polymarket_connection_generation
            self.websocket_connected = connected
            self.websocket_tokens = tokens
            self.websocket_error = error
            if reconnect:
                self.websocket_reconnects += 1
        event_type = (
            "connection_opened"
            if connected
            else "connection_closed"
            if was_connected
            else None
        )
        if event_type is not None and self.archive is not None:
            self.archive.append(
                source="polymarket-control",
                market_id=None,
                event_type=event_type,
                sequence=generation,
                payload={
                    "connectionGeneration": generation,
                    "tokens": tokens,
                    "error": error,
                    "reconnect": reconnect,
                },
                received_at=observed_at,
                monotonic_ns=observed_monotonic_ns,
            )

    def apply_clock_sample(self, sample: ClockSample) -> None:
        archived = True
        if self.archive is not None:
            archived = self.archive.append(
                source="clock",
                market_id=None,
                event_type="clock_sample",
                sequence=None,
                payload=sample.payload(),
                received_at=sample.response_received_at,
                monotonic_ns=sample.response_received_monotonic_ns,
            )
        with self.lock:
            self.clock_samples += 1
            if archived:
                self.clock_sample = sample
                self.clock_error = sample.error
            else:
                self.clock_sample = None
                self.clock_error = "clock sample was not archived"

    def current_clock_evidence(
        self, observed_monotonic_ns: int
    ) -> tuple[ClockSample | None, float | None]:
        with self.lock:
            sample = self.clock_sample
        if sample is None:
            return None, None
        age_seconds = max(
            0.0,
            (
                observed_monotonic_ns
                - sample.response_received_monotonic_ns
            )
            / 1_000_000_000,
        )
        if not sample.healthy or age_seconds > self.maximum_clock_age_seconds:
            return None, age_seconds
        return sample, age_seconds

    def apply_websocket_event(self, event: dict[str, Any]) -> bool:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return False
        observed_at = utc_now()
        observed_monotonic_ns = time.monotonic_ns()
        clock_sample, clock_age_seconds = self.current_clock_evidence(
            observed_monotonic_ns
        )
        with self.lock:
            connection_generation = self.polymarket_connection_generation
        archived_event = dict(event)
        archived_event["connectionGeneration"] = connection_generation
        if self.archive is not None:
            self.archive.append(
                source="polymarket",
                market_id=asset_id,
                event_type=str(
                    event.get("event_type") or event.get("type") or ""
                ),
                sequence=None,
                payload=archived_event,
                received_at=observed_at,
                monotonic_ns=observed_monotonic_ns,
            )
        event_type = event.get("event_type") or event.get("type")
        book = None
        validated_monotonic_ns = observed_monotonic_ns
        if event_type in {"book", "price_change"}:
            reconstructed = self.polymarket_books.setdefault(
                asset_id,
                PolymarketBookState(),
            )
            book = reconstructed.apply(event)
            validated_monotonic_ns = time.monotonic_ns()
            if not reconstructed.initialized:
                return False
            if (
                self.archive is not None
                and book[0] is not None
                and book[1] is not None
            ):
                self.archive.append(
                    source="polymarket-state",
                    market_id=asset_id,
                    event_type="book_state",
                    sequence=None,
                    payload={
                        "assetId": asset_id,
                        "connectionGeneration": connection_generation,
                        "originEventType": str(event_type),
                        "sourceTs": event.get("timestamp"),
                        "bookHash": reconstructed.last_hash,
                        "bestBid": book[0],
                        "bestAsk": book[1],
                        "bidSize": book[2],
                        "askSize": book[3],
                        "observedMonotonicNs": observed_monotonic_ns,
                        "validatedMonotonicNs": validated_monotonic_ns,
                        "clockSampleKey": (
                            clock_sample.key if clock_sample is not None else None
                        ),
                        "clockSampleAgeSeconds": clock_age_seconds,
                    },
                    received_at=observed_at,
                    monotonic_ns=observed_monotonic_ns,
                )
        elif (
            event_type == "last_trade_price"
            and event.get("price") is not None
            and event.get("size") is not None
            and self.archive is not None
        ):
            validated_monotonic_ns = time.monotonic_ns()
            trade_id = (
                event.get("transaction_hash")
                or event.get("trade_id")
                or (
                    f"receipt:{self.archive.process_id}:"
                    f"{observed_monotonic_ns}"
                )
            )
            self.archive.append(
                source="polymarket-trade",
                market_id=asset_id,
                event_type="observed_trade",
                sequence=None,
                payload={
                    "assetId": asset_id,
                    "connectionGeneration": connection_generation,
                    "tradeId": str(trade_id),
                    "sourceTs": event.get("timestamp"),
                    "price": float(event["price"]),
                    "size": float(event["size"]),
                    "side": None,
                    "rawSide": event.get("side"),
                    "feeRateBps": (
                        float(event["fee_rate_bps"])
                        if event.get("fee_rate_bps") is not None
                        else None
                    ),
                    "observedMonotonicNs": observed_monotonic_ns,
                    "validatedMonotonicNs": validated_monotonic_ns,
                    "clockSampleKey": (
                        clock_sample.key if clock_sample is not None else None
                    ),
                    "clockSampleAgeSeconds": clock_age_seconds,
                },
                received_at=observed_at,
                monotonic_ns=observed_monotonic_ns,
            )
        changed = False
        with self.lock:
            for market in (self.payload or {}).get("polymarket", []):
                for outcome in market.get("outcomes", []):
                    if str(outcome.get("tokenId")) != asset_id:
                        continue
                    if event_type in {"book", "price_change"}:
                        assert book is not None
                        outcome["bid"] = (
                            {"price": book[0], "size": book[2]}
                            if book[0] is not None
                            else None
                        )
                        outcome["ask"] = (
                            {"price": book[1], "size": book[3]}
                            if book[1] is not None
                            else None
                        )
                        outcome["bookHash"] = event.get("hash")
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

    def reconcile_polymarket_snapshot(
        self,
        snapshot: dict[str, Any],
    ) -> str:
        asset_id = str(snapshot.get("asset_id") or "")
        if not asset_id:
            return "invalid"
        observed_at = utc_now()
        observed_monotonic_ns = time.monotonic_ns()
        clock_sample, clock_age_seconds = self.current_clock_evidence(
            observed_monotonic_ns
        )
        with self.lock:
            connection_generation = self.polymarket_connection_generation
        archived_snapshot = dict(snapshot)
        archived_snapshot["connectionGeneration"] = connection_generation
        if self.archive is not None:
            self.archive.append(
                source="polymarket-rest",
                market_id=asset_id,
                event_type="book_reconciliation",
                sequence=None,
                payload=archived_snapshot,
                received_at=observed_at,
                monotonic_ns=observed_monotonic_ns,
            )
        with self.lock:
            state = self.polymarket_books.get(asset_id)
            status = (
                state.reconcile(snapshot)
                if state is not None
                else "uninitialized"
            )
            self.websocket_reconciliations += 1
            if status == "verified":
                self.websocket_hash_matches += 1
            elif status == "stale":
                self.websocket_stale_reconciliations += 1
        if state is not None and state.initialized and self.archive is not None:
            book = state.top()
            if book[0] is not None and book[1] is not None:
                validated_monotonic_ns = time.monotonic_ns()
                self.archive.append(
                    source="polymarket-state",
                    market_id=asset_id,
                    event_type="book_state",
                    sequence=None,
                    payload={
                        "assetId": asset_id,
                        "connectionGeneration": connection_generation,
                        "originEventType": "book_reconciliation",
                        "sourceTs": snapshot.get("timestamp"),
                        "bookHash": state.last_hash,
                        "bestBid": book[0],
                        "bestAsk": book[1],
                        "bidSize": book[2],
                        "askSize": book[3],
                        "observedMonotonicNs": observed_monotonic_ns,
                        "validatedMonotonicNs": validated_monotonic_ns,
                        "clockSampleKey": (
                            clock_sample.key
                            if clock_sample is not None
                            else None
                        ),
                        "clockSampleAgeSeconds": clock_age_seconds,
                    },
                    received_at=observed_at,
                    monotonic_ns=observed_monotonic_ns,
                )
        return status

    def fail_polymarket_reconciliation(self, message: str) -> None:
        observed_at = utc_now()
        observed_monotonic_ns = time.monotonic_ns()
        with self.lock:
            self.websocket_reconciliation_failures += 1
            self.websocket_error = message
            generation = self.polymarket_connection_generation
        if self.archive is not None:
            self.archive.append(
                source="polymarket-control",
                market_id=None,
                event_type="reconciliation_failure",
                sequence=generation,
                payload={
                    "connectionGeneration": generation,
                    "error": message,
                },
                received_at=observed_at,
                monotonic_ns=observed_monotonic_ns,
            )

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
        if book[0] is None and book[1] is None:
            return False
        changed = False
        with self.lock:
            for market in (self.payload or {}).get("parity", []):
                for outcome in market.get("outcomes", []):
                    if str(outcome.get("ticker")) != ticker:
                        continue
                    outcome["bid"] = (
                        {"price": book[0], "size": book[2]}
                        if book[0] is not None
                        else None
                    )
                    outcome["ask"] = (
                        {"price": book[1], "size": book[3]}
                        if book[1] is not None
                        else None
                    )
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
                    and websocket_age <= maximum_age_seconds
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
                    and kalshi_websocket_age <= maximum_age_seconds
                )
            )
            archive_health = (
                self.archive.status()
                if self.archive is not None
                else {
                    "configured": False,
                    "healthy": not self.archive_required,
                }
            )
            current_ns = time.monotonic_ns()
            clock_sample, clock_age = self.current_clock_evidence(current_ns)
            clock_healthy = clock_sample is not None
            healthy = (
                age is not None
                and age <= maximum_age_seconds
                and websocket_healthy
                and kalshi_websocket_healthy
                and archive_health["healthy"]
                and (clock_healthy or not self.clock_required)
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
                        "connectionGeneration": (
                            self.polymarket_connection_generation
                        ),
                        "tokens": self.websocket_tokens,
                        "messages": self.websocket_messages,
                        "reconnects": self.websocket_reconnects,
                        "reconciliations": (
                            self.websocket_reconciliations
                        ),
                        "hashMatches": self.websocket_hash_matches,
                        "staleReconciliations": (
                            self.websocket_stale_reconciliations
                        ),
                        "reconciliationFailures": (
                            self.websocket_reconciliation_failures
                        ),
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
                    "clockQuality": {
                        "required": self.clock_required,
                        "healthy": clock_healthy,
                        "samples": self.clock_samples,
                        "sampleKey": (
                            clock_sample.key
                            if clock_sample is not None
                            else None
                        ),
                        "collectorBootId": (
                            clock_sample.collector_boot_id
                            if clock_sample is not None
                            else None
                        ),
                        "offsetMs": (
                            clock_sample.offset_ms
                            if clock_sample is not None
                            else None
                        ),
                        "uncertaintyMs": (
                            clock_sample.uncertainty_ms
                            if clock_sample is not None
                            else None
                        ),
                        "ageSeconds": (
                            round(clock_age, 3)
                            if clock_age is not None
                            else None
                        ),
                        "error": self.clock_error,
                    },
                    "archive": archive_health,
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


def clock_quality_loop(
    state: CollectorState,
    stop: threading.Event,
    interval_seconds: float,
    maximum_uncertainty_ms: float,
) -> None:
    boot_id = uuid.uuid4().hex
    while not stop.is_set():
        sample = sample_clock(
            collector_boot_id=boot_id,
            maximum_uncertainty_ms=maximum_uncertainty_ms,
        )
        state.apply_clock_sample(sample)
        stop.wait(interval_seconds)


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


def websocket_loop(
    state: CollectorState,
    stop: threading.Event,
    reconcile_seconds: float = 60.0,
) -> None:
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
                pending: dict[str, tuple[int, str, float]] = {}
                next_reconciliation = time.monotonic() + reconcile_seconds
                delay = 1.0
                while not stop.is_set():
                    if state.token_ids() != tokens:
                        break
                    expired = [
                        token
                        for token, (_, _, expires_at) in pending.items()
                        if time.monotonic() >= expires_at
                    ]
                    if expired:
                        message = (
                            "WebSocket did not reach REST hash for "
                            f"{len(expired)} books"
                        )
                        state.fail_polymarket_reconciliation(message)
                        raise PolymarketContinuityError(message)
                    if (
                        time.monotonic() >= next_reconciliation
                        and not pending
                    ):
                        snapshots = polymarket_books(tokens)
                        seen: set[str] = set()
                        for snapshot in snapshots:
                            asset_id = str(
                                snapshot.get("asset_id") or ""
                            )
                            if not asset_id or asset_id not in tokens:
                                continue
                            seen.add(asset_id)
                            status = (
                                state.reconcile_polymarket_snapshot(
                                    snapshot
                                )
                            )
                            if status in {"advanced", "mismatch"}:
                                source_ts = (
                                    PolymarketBookState.timestamp(
                                        snapshot.get("timestamp")
                                    )
                                )
                                book_hash = snapshot.get("hash")
                                if (
                                    source_ts is None
                                    or book_hash in {None, ""}
                                ):
                                    message = (
                                        f"{asset_id} reconciliation "
                                        "omitted timestamp/hash"
                                    )
                                    state.fail_polymarket_reconciliation(
                                        message
                                    )
                                    raise PolymarketContinuityError(
                                        message
                                    )
                                pending[asset_id] = (
                                    source_ts,
                                    str(book_hash),
                                    time.monotonic() + 3.0,
                                )
                        missing = set(tokens) - seen
                        if missing:
                            message = (
                                "REST reconciliation omitted "
                                f"{len(missing)} subscribed books"
                            )
                            state.fail_polymarket_reconciliation(message)
                            raise PolymarketContinuityError(message)
                        next_reconciliation = (
                            time.monotonic() + reconcile_seconds
                        )
                        continue
                    try:
                        raw_message = websocket.recv(
                            timeout=min(
                                1.0,
                                max(
                                    0.01,
                                    next_reconciliation
                                    - time.monotonic(),
                                ),
                            )
                        )
                    except TimeoutError:
                        continue
                    if raw_message in {"PING", "PONG"}:
                        continue
                    payload = json.loads(raw_message)
                    for event in message_events(payload):
                        state.apply_websocket_event(event)
                        asset_id = str(event.get("asset_id") or "")
                        target = pending.get(asset_id)
                        if target is None:
                            continue
                        target_ts, target_hash, _ = target
                        event_ts = PolymarketBookState.timestamp(
                            event.get("timestamp")
                        )
                        event_hash = event.get("hash")
                        if (
                            event_hash not in {None, ""}
                            and str(event_hash) == target_hash
                        ):
                            with state.lock:
                                state.websocket_hash_matches += 1
                            pending.pop(asset_id, None)
                        elif event_ts is not None and event_ts > target_ts:
                            message = (
                                f"{asset_id} stream passed REST "
                                "snapshot without matching hash"
                            )
                            state.fail_polymarket_reconciliation(message)
                            raise PolymarketContinuityError(message)
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
                                "channels": [
                                    "orderbook_delta",
                                    "trade",
                                ],
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
                    message = event.get("msg") or {}
                    ticker = str(message.get("market_ticker") or "")
                    if (
                        state.archive is not None
                        and event_type
                        in {
                            "orderbook_snapshot",
                            "orderbook_delta",
                            "trade",
                        }
                    ):
                        state.archive.append(
                            source="kalshi",
                            market_id=ticker or None,
                            event_type=str(event_type),
                            sequence=(
                                int(event["seq"])
                                if event.get("seq") is not None
                                else None
                            ),
                            payload=event,
                        )
                    if event_type == "trade":
                        continue
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
            if path == "/archive/events":
                if state.archive is None:
                    self.send_json(404, {"error": "archive not configured"})
                    return
                query = urlsplit(self.path).query
                parameters = {}
                for item in query.split("&"):
                    key, _, value = item.partition("=")
                    if key:
                        parameters[key] = value
                try:
                    payload = state.archive.read_events(
                        after_id=int(parameters.get("after_id", "0")),
                        limit=int(parameters.get("limit", "1000")),
                    )
                except (OSError, sqlite3.Error, ValueError) as exc:
                    self.send_json(
                        400,
                        {"error": f"{type(exc).__name__}: {exc}"},
                    )
                    return
                self.send_json(200, payload)
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
    parser.add_argument(
        "--polymarket-reconcile-seconds",
        type=float,
        default=float(
            os.environ.get("POLYMARKET_RECONCILE_SECONDS", "60")
        ),
    )
    parser.add_argument(
        "--archive-path",
        type=Path,
        default=(
            Path(os.environ["ARCHIVE_PATH"])
            if os.environ.get("ARCHIVE_PATH")
            else None
        ),
    )
    parser.add_argument(
        "--clock-sample-seconds",
        type=float,
        default=float(os.environ.get("CLOCK_SAMPLE_SECONDS", "60")),
    )
    parser.add_argument(
        "--maximum-clock-age-seconds",
        type=float,
        default=float(os.environ.get("MAXIMUM_CLOCK_AGE_SECONDS", "120")),
    )
    parser.add_argument(
        "--maximum-clock-uncertainty-ms",
        type=float,
        default=float(
            os.environ.get("MAXIMUM_CLOCK_UNCERTAINTY_MS", "750")
        ),
    )
    args = parser.parse_args()
    if (
        args.refresh_seconds <= 0
        or args.maximum_age_seconds <= 0
        or args.polymarket_reconcile_seconds <= 0
        or args.clock_sample_seconds <= 0
        or args.maximum_clock_age_seconds <= 0
        or args.maximum_clock_uncertainty_ms <= 0
    ):
        parser.error(
            "refresh, maximum age, reconciliation, and clock settings "
            "must be positive"
        )

    kalshi_key_id, kalshi_private_key = load_kalshi_credentials()
    kalshi_configured = bool(kalshi_key_id and kalshi_private_key)
    kalshi_required = env_flag("REQUIRE_KALSHI_WEBSOCKET")
    archive_required = env_flag("REQUIRE_ARCHIVE")
    clock_required = env_flag("REQUIRE_CLOCK_QUALITY")
    if archive_required and args.archive_path is None:
        parser.error("REQUIRE_ARCHIVE=1 requires ARCHIVE_PATH")
    archive = (
        EventArchive(args.archive_path)
        if args.archive_path is not None
        else None
    )
    if archive is not None:
        archive.start()
    state = CollectorState(
        kalshi_websocket_configured=kalshi_configured,
        kalshi_websocket_required=kalshi_required,
        archive=archive,
        archive_required=archive_required,
        clock_required=clock_required,
        maximum_clock_age_seconds=args.maximum_clock_age_seconds,
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
    clock_worker = threading.Thread(
        target=clock_quality_loop,
        args=(
            state,
            stop,
            args.clock_sample_seconds,
            args.maximum_clock_uncertainty_ms,
        ),
        name="clock-quality",
        daemon=True,
    )
    clock_worker.start()
    websocket_worker = threading.Thread(
        target=websocket_loop,
        args=(state, stop, args.polymarket_reconcile_seconds),
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
        clock_worker.join(timeout=10)
        websocket_worker.join(timeout=10)
        if kalshi_websocket_worker is not None:
            kalshi_websocket_worker.join(timeout=10)
        if archive is not None:
            archive.stop()
        server.server_close()


if __name__ == "__main__":
    main()

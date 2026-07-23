#!/usr/bin/env python3
"""Collect tonight's public MLB slate and executable Kalshi game books."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KALSHI = "https://external-api.kalshi.com/trade-api/v2"
MLB = "https://statsapi.mlb.com/api/v1"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB = "https://clob.polymarket.com"
EASTERN = ZoneInfo("America/New_York")
MONTHS = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}


def get_json(url: str, attempts: int = 4) -> dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "TREA-Kalshi-Relay/1.0 (+https://github.com/bbroeking/trea-kalshi-relay)",
    }
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == attempts - 1:
                raise
            retry_after = int(exc.headers.get("Retry-After", "5"))
            time.sleep(min(30, max(retry_after, 2**attempt)))
        except (OSError, TimeoutError):
            if attempt == attempts - 1:
                raise
            time.sleep(2**attempt)
    raise RuntimeError(f"Unable to fetch {url}")


def event_date(event_ticker: str) -> str | None:
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", event_ticker)
    if not match:
        return None
    month = MONTHS.get(match.group(2))
    return f"20{match.group(1)}-{month}-{match.group(3)}" if month else None


def orderbook(market: dict[str, Any]) -> dict[str, Any]:
    ticker = market["ticker"]
    payload = get_json(
        f"{KALSHI}/markets/{urllib.parse.quote(ticker)}/orderbook?depth=20"
    )
    book = payload.get("orderbook_fp") or {}
    yes = sorted(
        book.get("yes_dollars") or [], key=lambda level: float(level[0]), reverse=True
    )
    no = sorted(
        book.get("no_dollars") or [], key=lambda level: float(level[0]), reverse=True
    )
    bid = (
        {"price": float(yes[0][0]), "size": float(yes[0][1])} if yes else None
    )
    ask = (
        {"price": 1 - float(no[0][0]), "size": float(no[0][1])} if no else None
    )
    return {
        "name": market.get("yes_sub_title") or market.get("subtitle") or "Yes",
        "ticker": ticker,
        "bid": bid,
        "ask": ask,
        "volume": float(market.get("volume_fp") or market.get("volume") or 0),
    }


def json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, list) else []
    return []


def polymarket_book(token_id: str) -> dict[str, Any]:
    query = urllib.parse.urlencode({"token_id": token_id})
    payload = get_json(f"{POLYMARKET_CLOB}/book?{query}")
    bids = sorted(
        payload.get("bids") or [],
        key=lambda level: float(level["price"]),
        reverse=True,
    )
    asks = sorted(
        payload.get("asks") or [],
        key=lambda level: float(level["price"]),
    )
    return {
        "bid": (
            {"price": float(bids[0]["price"]), "size": float(bids[0]["size"])}
            if bids
            else None
        ),
        "ask": (
            {"price": float(asks[0]["price"]), "size": float(asks[0]["size"])}
            if asks
            else None
        ),
        "lastTradePrice": (
            float(payload["last_trade_price"])
            if payload.get("last_trade_price") is not None
            else None
        ),
        "tickSize": payload.get("tick_size"),
        "bookTimestamp": payload.get("timestamp"),
        "bookHash": payload.get("hash"),
    }


def collect_polymarket(date: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for offset in range(0, 500, 100):
        query = urllib.parse.urlencode(
            {
                "series_id": 3,
                "closed": "false",
                "limit": 100,
                "offset": offset,
                "order": "id",
                "ascending": "false",
            }
        )
        page = get_json(f"{POLYMARKET_GAMMA}/events?{query}")
        if not isinstance(page, list):
            break
        candidates.extend(page)
        if len(page) < 100:
            break

    result = []
    seen_conditions: set[str] = set()
    for event in candidates:
        markets = event.get("markets") or []
        starts_today = any(
            str(market.get("gameStartTime") or "").startswith(date)
            for market in markets
        )
        if not starts_today:
            continue
        moneyline = next(
            (
                market
                for market in markets
                if market.get("question") == event.get("title")
                and market.get("active")
                and not market.get("closed")
                and market.get("acceptingOrders")
            ),
            None,
        )
        if not moneyline:
            continue
        condition_id = str(moneyline.get("conditionId") or "")
        if not condition_id or condition_id in seen_conditions:
            continue
        outcomes = json_list(moneyline.get("outcomes"))
        token_ids = json_list(moneyline.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            continue
        seen_conditions.add(condition_id)
        outcome_books = []
        for outcome, token_id in zip(outcomes, token_ids):
            outcome_books.append(
                {
                    "name": str(outcome),
                    "tokenId": str(token_id),
                    **polymarket_book(str(token_id)),
                }
            )
        asks = [
            outcome["ask"]["price"]
            for outcome in outcome_books
            if outcome.get("ask") is not None
        ]
        ask_sum = sum(asks) if len(asks) == 2 else None
        gross_edge = 1 - ask_sum if ask_sum is not None else None
        result.append(
            {
                "eventId": str(event.get("id") or ""),
                "conditionId": condition_id,
                "gameId": event.get("gameId"),
                "slug": event.get("slug"),
                "title": event.get("title") or moneyline.get("question"),
                "gameStartTime": moneyline.get("gameStartTime"),
                "status": "active",
                "outcomes": outcome_books,
                "liquidity": float(moneyline.get("liquidity") or 0),
                "volume": float(moneyline.get("volume") or 0),
                "feesEnabled": bool(moneyline.get("feesEnabled")),
                "takerBaseFee": moneyline.get("takerBaseFee"),
                "makerBaseFee": moneyline.get("makerBaseFee"),
                "askSum": ask_sum,
                "grossEdge": gross_edge,
                "signal": (
                    "VERIFY LIVE"
                    if gross_edge is not None and gross_edge > 0
                    else "NO TRADE"
                ),
            }
        )
    return result


def collect() -> dict[str, Any]:
    date = datetime.now(EASTERN).date().isoformat()
    schedule_query = urllib.parse.urlencode({"sportId": 1, "date": date})
    events_query = urllib.parse.urlencode(
        {
            "series_ticker": "KXMLBGAME",
            "limit": 200,
            "status": "open",
            "with_nested_markets": "true",
        }
    )
    schedule = get_json(f"{MLB}/schedule?{schedule_query}")
    events_payload = get_json(f"{KALSHI}/events?{events_query}")
    games = [
        game
        for day in schedule.get("dates", [])
        for game in day.get("games", [])
    ]
    events = [
        event
        for event in events_payload.get("events", [])
        if event_date(event.get("event_ticker", "")) == date
    ]
    polymarket = collect_polymarket(date)

    parity = []
    for event in events:
        outcomes = [orderbook(market) for market in event.get("markets", [])]
        asks = [
            outcome["ask"]["price"]
            for outcome in outcomes
            if outcome.get("ask") is not None
        ]
        ask_sum = sum(asks) if len(asks) == 2 else None
        gross_edge = 1 - ask_sum if ask_sum is not None else None
        net_edge = gross_edge - 0.02 if gross_edge is not None else None
        parity.append(
            {
                "event": event["event_ticker"],
                "title": event.get("title") or event["event_ticker"],
                "status": "active",
                "outcomes": outcomes,
                "askSum": ask_sum,
                "grossEdge": gross_edge,
                "netEdge": net_edge,
                "signal": (
                    "BUY PAIR"
                    if net_edge is not None and net_edge > 0
                    else "NO TRADE"
                ),
            }
        )

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "schemaVersion": 1,
        "date": date,
        "observedAt": now,
        "games": games,
        "parity": parity,
        "polymarket": polymarket,
        "sourceHealth": {
            "mlbStatus": 200,
            "kalshiStatus": 200,
            "kalshiOpenEvents": len(events_payload.get("events", [])),
            "kalshiTodayMarkets": sum(
                len(event.get("markets", [])) for event in events
            ),
            "usingSnapshot": False,
            "marketObservedAt": now,
            "relay": "github-actions",
            "polymarketStatus": 200,
            "polymarketMarkets": len(polymarket),
        },
    }


def write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(encoded)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/tonight.json"))
    args = parser.parse_args()
    payload = collect()
    write_atomic(args.output, payload)
    print(
        json.dumps(
            {
                "observedAt": payload["observedAt"],
                "games": len(payload["games"]),
                "markets": len(payload["parity"]),
                "polymarketMarkets": len(payload["polymarket"]),
                "output": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()

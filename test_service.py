from __future__ import annotations

import base64
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from archive import EventArchive
from service import (
    KALSHI_WS_PATH,
    CollectorState,
    KalshiBookState,
    KalshiSequenceError,
    KalshiSequenceTracker,
    kalshi_websocket_headers,
)


class CollectorStateTests(unittest.TestCase):
    def test_websocket_updates_matching_outcome(self) -> None:
        state = CollectorState(
            payload={
                "polymarket": [
                    {
                        "outcomes": [
                            {
                                "tokenId": "one",
                                "bid": {"price": 0.4, "size": 2},
                                "ask": {"price": 0.5, "size": 3},
                            }
                        ]
                    }
                ]
            }
        )
        changed = state.apply_websocket_event(
            {
                "asset_id": "one",
                "event_type": "price_change",
                "best_bid": "0.44",
                "best_ask": "0.46",
                "timestamp": "1000",
            }
        )
        self.assertTrue(changed)
        outcome = state.payload["polymarket"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"], {"price": 0.44, "size": 2})
        self.assertEqual(outcome["ask"], {"price": 0.46, "size": 3})
        self.assertEqual(state.websocket_messages, 1)

    def test_full_book_clears_an_empty_side(self) -> None:
        state = CollectorState(
            payload={
                "polymarket": [
                    {
                        "outcomes": [
                            {
                                "tokenId": "one",
                                "bid": {"price": 0.4, "size": 2},
                                "ask": {"price": 0.5, "size": 3},
                            }
                        ]
                    }
                ]
            }
        )
        self.assertTrue(
            state.apply_websocket_event(
                {
                    "asset_id": "one",
                    "event_type": "book",
                    "bids": [{"price": "0.41", "size": "2"}],
                    "asks": [],
                }
            )
        )
        outcome = state.payload["polymarket"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"]["price"], 0.41)
        self.assertIsNone(outcome["ask"])

    def test_best_quote_message_clears_explicit_empty_side(self) -> None:
        state = CollectorState(
            payload={
                "polymarket": [
                    {
                        "outcomes": [
                            {
                                "tokenId": "one",
                                "bid": {"price": 0.4, "size": 2},
                                "ask": {"price": 0.5, "size": 3},
                            }
                        ]
                    }
                ]
            }
        )
        self.assertTrue(
            state.apply_websocket_event(
                {
                    "asset_id": "one",
                    "event_type": "best_bid_ask",
                    "best_bid": "0.42",
                    "best_ask": "",
                }
            )
        )
        outcome = state.payload["polymarket"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"]["price"], 0.42)
        self.assertIsNone(outcome["ask"])

    def test_tokens_require_a_fresh_websocket(self) -> None:
        state = CollectorState(
            payload={
                "polymarket": [
                    {"outcomes": [{"tokenId": "one"}]}
                ]
            },
            last_success_monotonic=10.0,
        )
        with patch("service.time.monotonic", return_value=11.0):
            health, healthy = state.snapshot(5)
        self.assertFalse(healthy)
        self.assertTrue(health["websocket"]["required"])
        self.assertFalse(health["websocket"]["healthy"])

    def test_kalshi_book_updates_matching_outcome(self) -> None:
        state = CollectorState(
            payload={
                "parity": [
                    {
                        "outcomes": [
                            {
                                "ticker": "KX-ONE",
                                "bid": {"price": 0.4, "size": 2},
                                "ask": {"price": 0.5, "size": 3},
                            }
                        ]
                    }
                ]
            }
        )
        changed = state.apply_kalshi_book(
            "KX-ONE",
            (0.44, 0.46, 4.0, 5.0),
            {
                "seq": 12,
                "msg": {"ts_ms": 1234},
            },
        )
        self.assertTrue(changed)
        outcome = state.payload["parity"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"], {"price": 0.44, "size": 4.0})
        self.assertEqual(outcome["ask"], {"price": 0.46, "size": 5.0})
        self.assertEqual(outcome["bookSequence"], 12)
        self.assertEqual(outcome["bookPricing"], "unified_yes")

    def test_kalshi_book_clears_missing_ask(self) -> None:
        state = CollectorState(
            payload={
                "parity": [
                    {
                        "outcomes": [
                            {
                                "ticker": "KX-ONE",
                                "bid": {"price": 0.4, "size": 2},
                                "ask": {"price": 0.5, "size": 3},
                            }
                        ]
                    }
                ]
            }
        )
        self.assertTrue(
            state.apply_kalshi_book(
                "KX-ONE",
                (0.44, None, 4.0, None),
                {"seq": 13, "msg": {"ts_ms": 1234}},
            )
        )
        outcome = state.payload["parity"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"]["price"], 0.44)
        self.assertIsNone(outcome["ask"])

    def test_required_kalshi_stream_must_receive_fresh_snapshot(self) -> None:
        state = CollectorState(
            payload={
                "parity": [
                    {"outcomes": [{"ticker": "KX-ONE"}]}
                ]
            },
            last_success_monotonic=10.0,
            kalshi_websocket_configured=True,
            kalshi_websocket_required=True,
            kalshi_websocket_connected=True,
        )
        with patch("service.time.monotonic", return_value=11.0):
            health, healthy = state.snapshot(5)
        self.assertFalse(healthy)
        self.assertTrue(health["kalshiWebsocket"]["required"])
        self.assertFalse(health["kalshiWebsocket"]["healthy"])

    def test_kalshi_unified_book_and_sequence_gap(self) -> None:
        book = KalshiBookState()
        self.assertEqual(
            book.apply(
                {
                    "type": "orderbook_snapshot",
                    "msg": {
                        "yes_dollars_fp": [["0.42", "4"]],
                        "no_dollars_fp": [["0.44", "5"]],
                    },
                }
            ),
            (0.42, 0.44, 4.0, 5.0),
        )
        self.assertEqual(
            book.apply(
                {
                    "type": "orderbook_delta",
                    "msg": {
                        "side": "no",
                        "price_dollars": "0.44",
                        "delta_fp": "-5",
                    },
                }
            ),
            (0.42, None, 4.0, None),
        )
        sequence = KalshiSequenceTracker()
        sequence.observe(
            {"type": "orderbook_snapshot", "sid": 2, "seq": 8}
        )
        with self.assertRaises(KalshiSequenceError):
            sequence.observe(
                {"type": "orderbook_delta", "sid": 2, "seq": 10}
            )

    def test_kalshi_signature_verifies(self) -> None:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding, rsa

        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        headers = kalshi_websocket_headers(
            "key",
            pem,
            timestamp_ms=1_700_000_000_000,
        )
        private_key.public_key().verify(
            base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
            f"1700000000000GET{KALSHI_WS_PATH}".encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

    def test_success_becomes_healthy(self) -> None:
        state = CollectorState(
            collect_once=lambda: {"observedAt": "2026-07-24T00:00:00Z"}
        )
        with patch("service.time.monotonic", side_effect=[10.0, 11.0]):
            state.refresh()
            health, healthy = state.snapshot(5)
        self.assertTrue(healthy)
        self.assertEqual(health["ageSeconds"], 1)
        self.assertEqual(health["totalRefreshes"], 1)
        self.assertIsNone(health["lastError"])

    def test_failure_preserves_last_good_payload(self) -> None:
        calls = iter(
            [
                {"observedAt": "2026-07-24T00:00:00Z"},
                RuntimeError("upstream unavailable"),
            ]
        )

        def collect_once():
            result = next(calls)
            if isinstance(result, Exception):
                raise result
            return result

        state = CollectorState(collect_once=collect_once)
        with patch("service.time.monotonic", side_effect=[10.0, 11.0]):
            state.refresh()
            state.refresh()
            health, healthy = state.snapshot(5)
        self.assertTrue(healthy)
        self.assertEqual(
            state.payload["observedAt"],
            "2026-07-24T00:00:00Z",
        )
        self.assertEqual(health["consecutiveFailures"], 1)
        self.assertIn("upstream unavailable", health["lastError"])

    def test_old_success_is_not_ready(self) -> None:
        state = CollectorState(
            payload={"observedAt": "2026-07-24T00:00:00Z"},
            last_success_monotonic=10.0,
        )
        with patch("service.time.monotonic", return_value=131.0):
            health, healthy = state.snapshot(120)
        self.assertFalse(healthy)
        self.assertEqual(health["ageSeconds"], 121)

    def test_stale_websocket_message_is_not_ready(self) -> None:
        state = CollectorState(
            payload={
                "polymarket": [
                    {"outcomes": [{"tokenId": "one"}]}
                ]
            },
            last_success_monotonic=100.0,
            websocket_connected=True,
            websocket_last_message_monotonic=10.0,
        )
        with patch("service.time.monotonic", return_value=101.0):
            health, healthy = state.snapshot(30)
        self.assertFalse(healthy)
        self.assertFalse(health["websocket"]["healthy"])


class EventArchiveTests(unittest.TestCase):
    def test_archive_is_append_only_and_resumable(self) -> None:
        with TemporaryDirectory() as directory:
            archive = EventArchive(Path(directory) / "relay.sqlite")
            archive.start()
            self.assertTrue(
                archive.append(
                    source="polymarket",
                    market_id="one",
                    event_type="book",
                    sequence=None,
                    payload={"asset_id": "one", "bids": []},
                )
            )
            self.assertTrue(
                archive.append(
                    source="kalshi",
                    market_id="KX-ONE",
                    event_type="orderbook_delta",
                    sequence=2,
                    payload={"seq": 2},
                )
            )
            self.assertTrue(archive.flush())
            first = archive.read_events(after_id=0, limit=1)
            second = archive.read_events(
                after_id=first["nextAfterId"], limit=10
            )
            status = archive.status()
            archive.stop()
        self.assertEqual(first["maximumId"], 2)
        self.assertEqual(first["events"][0]["source"], "polymarket")
        self.assertEqual(second["events"][0]["sequence"], 2)
        self.assertTrue(status["healthy"])
        self.assertEqual(status["written"], 2)
        self.assertEqual(status["dropped"], 0)


if __name__ == "__main__":
    unittest.main()

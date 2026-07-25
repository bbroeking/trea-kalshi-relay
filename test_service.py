from __future__ import annotations

import base64
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from archive import EventArchive
from clock_quality import ClockSample, sample_clock
from service import (
    KALSHI_WS_PATH,
    CollectorState,
    KalshiBookState,
    KalshiSequenceError,
    KalshiSequenceTracker,
    PolymarketBookState,
    kalshi_websocket_headers,
)


class CollectorStateTests(unittest.TestCase):
    @staticmethod
    def healthy_clock() -> ClockSample:
        return ClockSample(
            collector_boot_id="clock-boot",
            reference="polymarket-public-time",
            request_started_at="2026-07-25T00:00:00Z",
            response_received_at="2026-07-25T00:00:01Z",
            request_started_monotonic_ns=900,
            response_received_monotonic_ns=1000,
            offset_ms=10.0,
            uncertainty_ms=200.0,
            rtt_ms=10.0,
            wall_step_ms=0.1,
            healthy=True,
            error=None,
            raw={"probes": []},
        )

    def test_clock_sample_is_archived_before_becoming_current(self) -> None:
        archive = unittest.mock.Mock()
        archive.append.return_value = True
        state = CollectorState(archive=archive, clock_required=True)
        sample = self.healthy_clock()
        state.apply_clock_sample(sample)
        self.assertEqual(state.clock_sample, sample)
        call = archive.append.call_args.kwargs
        self.assertEqual(call["source"], "clock")
        self.assertEqual(call["event_type"], "clock_sample")
        self.assertEqual(call["payload"]["sampleKey"], sample.key)

    def test_polymarket_connection_boundaries_are_archived_and_numbered(
        self,
    ) -> None:
        archive = unittest.mock.Mock()
        archive.append.return_value = True
        state = CollectorState(archive=archive)

        state.set_websocket_status(connected=True, tokens=2)
        state.set_websocket_status(
            connected=False,
            tokens=2,
            error="socket reset",
            reconnect=True,
        )
        state.set_websocket_status(connected=True, tokens=2)

        controls = [
            call.kwargs
            for call in archive.append.call_args_list
            if call.kwargs["source"] == "polymarket-control"
        ]
        self.assertEqual(
            [row["event_type"] for row in controls],
            ["connection_opened", "connection_closed", "connection_opened"],
        )
        self.assertEqual(
            [
                row["payload"]["connectionGeneration"]
                for row in controls
            ],
            [1, 1, 2],
        )
        self.assertEqual(state.polymarket_connection_generation, 2)

    def test_required_clock_quality_fails_closed_when_stale(self) -> None:
        state = CollectorState(
            collect_once=lambda: {"polymarket": [], "parity": []},
            last_success_monotonic=100.0,
            clock_required=True,
            maximum_clock_age_seconds=10.0,
            clock_sample=self.healthy_clock(),
        )
        with (
            patch("service.time.monotonic", return_value=101.0),
            patch("service.time.monotonic_ns", return_value=20_000_000_000),
        ):
            health, healthy = state.snapshot(30)
        self.assertFalse(healthy)
        self.assertFalse(health["clockQuality"]["healthy"])

    def test_websocket_archives_receipt_aligned_book_state(self) -> None:
        archive = unittest.mock.Mock()
        state = CollectorState(
            payload={
                "polymarket": [
                    {"outcomes": [{"tokenId": "one"}]}
                ]
            },
            archive=archive,
        )
        with (
            patch("service.utc_now", return_value="2026-07-25T00:00:00Z"),
            patch(
                "service.time.monotonic_ns",
                side_effect=[1000, 1010],
            ),
        ):
            self.assertTrue(
                state.apply_websocket_event(
                    {
                        "asset_id": "one",
                        "event_type": "book",
                        "bids": [{"price": "0.40", "size": "2"}],
                        "asks": [{"price": "0.50", "size": "3"}],
                        "timestamp": "999",
                        "hash": "book-hash",
                    }
                )
            )
        self.assertEqual(archive.append.call_count, 2)
        raw = archive.append.call_args_list[0].kwargs
        normalized = archive.append.call_args_list[1].kwargs
        self.assertEqual(raw["source"], "polymarket")
        self.assertEqual(normalized["source"], "polymarket-state")
        self.assertEqual(normalized["event_type"], "book_state")
        self.assertEqual(raw["received_at"], normalized["received_at"])
        self.assertEqual(raw["monotonic_ns"], 1000)
        self.assertEqual(normalized["monotonic_ns"], 1000)
        self.assertEqual(
            normalized["payload"]["validatedMonotonicNs"],
            1010,
        )
        self.assertEqual(
            normalized["payload"]["connectionGeneration"],
            0,
        )

    def test_websocket_book_state_references_healthy_clock(self) -> None:
        archive = unittest.mock.Mock()
        archive.append.return_value = True
        state = CollectorState(
            payload={
                "polymarket": [
                    {"outcomes": [{"tokenId": "one"}]}
                ]
            },
            archive=archive,
            clock_sample=self.healthy_clock(),
        )
        with (
            patch("service.utc_now", return_value="2026-07-25T00:00:01Z"),
            patch(
                "service.time.monotonic_ns",
                side_effect=[1100, 1110],
            ),
        ):
            state.apply_websocket_event(
                {
                    "asset_id": "one",
                    "event_type": "book",
                    "bids": [{"price": "0.40", "size": "2"}],
                    "asks": [{"price": "0.50", "size": "3"}],
                }
            )
        normalized = archive.append.call_args_list[1].kwargs["payload"]
        self.assertEqual(
            normalized["clockSampleKey"],
            self.healthy_clock().key,
        )

    def test_websocket_trade_is_archived_with_clock_and_unknown_side(self) -> None:
        archive = unittest.mock.Mock()
        archive.process_id = "process"
        archive.append.return_value = True
        state = CollectorState(
            payload={
                "polymarket": [
                    {"outcomes": [{"tokenId": "one"}]}
                ]
            },
            archive=archive,
            clock_sample=self.healthy_clock(),
        )
        with (
            patch("service.utc_now", return_value="2026-07-25T00:00:01Z"),
            patch(
                "service.time.monotonic_ns",
                side_effect=[1100, 1110],
            ),
        ):
            state.apply_websocket_event(
                {
                    "asset_id": "one",
                    "event_type": "last_trade_price",
                    "transaction_hash": "0xtrade",
                    "timestamp": "1234",
                    "price": "0.48",
                    "size": "7.5",
                    "side": "BUY",
                    "fee_rate_bps": "0",
                }
            )
        normalized = archive.append.call_args_list[1].kwargs
        self.assertEqual(normalized["source"], "polymarket-trade")
        self.assertEqual(normalized["payload"]["tradeId"], "0xtrade")
        self.assertIsNone(normalized["payload"]["side"])
        self.assertEqual(normalized["payload"]["rawSide"], "BUY")
        self.assertEqual(
            normalized["payload"]["clockSampleKey"],
            self.healthy_clock().key,
        )

    def test_rest_reconciliation_archives_clocked_book_heartbeat(self) -> None:
        archive = unittest.mock.Mock()
        archive.append.return_value = True
        state = CollectorState(
            archive=archive,
            clock_sample=self.healthy_clock(),
        )
        state.polymarket_books["one"] = PolymarketBookState()
        state.polymarket_books["one"].apply(
            {
                "event_type": "book",
                "timestamp": "1234",
                "hash": "hash-one",
                "bids": [{"price": "0.4", "size": "2"}],
                "asks": [{"price": "0.5", "size": "3"}],
            }
        )
        with (
            patch("service.utc_now", return_value="2026-07-25T00:00:01Z"),
            patch(
                "service.time.monotonic_ns",
                side_effect=[1100, 1110],
            ),
        ):
            status = state.reconcile_polymarket_snapshot(
                {
                    "asset_id": "one",
                    "timestamp": "1234",
                    "hash": "hash-one",
                }
            )
        self.assertEqual(status, "verified")
        self.assertEqual(archive.append.call_count, 2)
        heartbeat = archive.append.call_args_list[1].kwargs
        self.assertEqual(heartbeat["source"], "polymarket-state")
        self.assertEqual(
            heartbeat["payload"]["originEventType"],
            "book_reconciliation",
        )
        self.assertEqual(
            heartbeat["payload"]["clockSampleKey"],
            self.healthy_clock().key,
        )


class CollectorStateBehaviorTests(unittest.TestCase):
    def test_conservative_probe_intersection(self) -> None:
        wall_values = iter(
            [
                10_000_000_000,
                10_100_000_000,
                10_200_000_000,
                10_300_000_000,
            ]
        )
        mono_values = iter([1_000, 2_000, 3_000, 4_000])
        sample = sample_clock(
            collector_boot_id="boot",
            probes=2,
            fetch_server_time=lambda: (10.0, 1.0, {}),
            wall_time_ns=lambda: next(wall_values),
            monotonic_ns=lambda: next(mono_values),
        )
        self.assertTrue(sample.healthy)
        self.assertGreaterEqual(sample.uncertainty_ms, 0)
        self.assertEqual(
            sample.key,
            f"boot:{sample.response_received_monotonic_ns}",
        )

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
        self.assertTrue(
            state.apply_websocket_event(
                {
                    "asset_id": "one",
                    "event_type": "book",
                    "bids": [{"price": "0.40", "size": "2"}],
                    "asks": [{"price": "0.50", "size": "3"}],
                    "timestamp": "999",
                    "hash": "before",
                }
            )
        )
        changed = state.apply_websocket_event(
            {
                "asset_id": "one",
                "event_type": "price_change",
                "side": "BUY",
                "price": "0.44",
                "size": "2",
                "best_bid": "0.44",
                "best_ask": "0.50",
                "timestamp": "1000",
                "hash": "after",
            }
        )
        self.assertTrue(changed)
        outcome = state.payload["polymarket"][0]["outcomes"][0]
        self.assertEqual(outcome["bid"], {"price": 0.44, "size": 2})
        self.assertEqual(outcome["ask"], {"price": 0.5, "size": 3.0})
        self.assertEqual(state.websocket_messages, 2)

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

    def test_best_quote_message_is_raw_only(self) -> None:
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
        self.assertFalse(
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
        self.assertEqual(outcome["bid"]["price"], 0.4)
        self.assertEqual(outcome["ask"]["price"], 0.5)

    def test_polymarket_hash_reconciliation_status(self) -> None:
        book = PolymarketBookState()
        self.assertEqual(
            book.reconcile({"timestamp": "1", "hash": "h"}),
            "uninitialized",
        )
        book.apply(
            {
                "event_type": "book",
                "timestamp": "10",
                "hash": "h10",
                "bids": [{"price": "0.4", "size": "2"}],
                "asks": [{"price": "0.5", "size": "3"}],
            }
        )
        self.assertEqual(
            book.reconcile({"timestamp": "10", "hash": "h10"}),
            "verified",
        )
        self.assertEqual(
            book.reconcile({"timestamp": "9", "hash": "old"}),
            "stale",
        )
        self.assertEqual(
            book.reconcile({"timestamp": "11", "hash": "new"}),
            "advanced",
        )

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
        self.assertIsInstance(first["events"][0]["monotonicNs"], int)
        self.assertTrue(first["events"][0]["processId"])
        self.assertEqual(second["events"][0]["sequence"], 2)
        self.assertTrue(status["healthy"])
        self.assertEqual(status["written"], 2)
        self.assertEqual(status["dropped"], 0)


if __name__ == "__main__":
    unittest.main()

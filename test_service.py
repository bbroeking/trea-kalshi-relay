from __future__ import annotations

import unittest
from unittest.mock import patch

from service import CollectorState


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


if __name__ == "__main__":
    unittest.main()

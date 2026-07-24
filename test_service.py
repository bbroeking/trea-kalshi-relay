from __future__ import annotations

import unittest
from unittest.mock import patch

from service import CollectorState


class CollectorStateTests(unittest.TestCase):
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
            state.payload, {"observedAt": "2026-07-24T00:00:00Z"}
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

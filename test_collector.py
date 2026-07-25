from __future__ import annotations

import unittest

from unittest.mock import patch

from collector import (
    event_local_date,
    kalshi_fee_regime,
    polymarket_books,
    polymarket_fee_regime,
)


class CollectorTests(unittest.TestCase):
    def test_kalshi_fee_regime_preserves_public_revision_metadata(self) -> None:
        regime = kalshi_fee_regime(
            {
                "series": {
                    "ticker": "KXMLBGAME",
                    "fee_type": "quadratic_with_maker_fees",
                    "fee_multiplier": 1,
                    "last_updated_ts": "2026-07-20T02:42:30.849144Z",
                }
            }
        )
        self.assertEqual(regime["venue"], "kalshi")
        self.assertEqual(regime["feeType"], "quadratic_with_maker_fees")
        self.assertEqual(regime["feeMultiplier"], 1.0)
        self.assertEqual(
            regime["sourceUpdatedAt"],
            "2026-07-20T02:42:30.849144Z",
        )
        self.assertTrue(regime["complete"])

    def test_incomplete_kalshi_fee_regime_fails_closed(self) -> None:
        regime = kalshi_fee_regime(
            {"series": {"ticker": "KXMLBGAME", "fee_multiplier": 1}}
        )
        self.assertFalse(regime["complete"])
        self.assertIsNone(regime["feeType"])

    def test_polymarket_fee_regime_preserves_schedule(self) -> None:
        regime = polymarket_fee_regime(
            {
                "feesEnabled": True,
                "takerBaseFee": 1000,
                "makerBaseFee": 1000,
                "feeSchedule": {
                    "exponent": 1,
                    "rate": 0.05,
                    "takerOnly": True,
                    "rebateRate": 0.15,
                },
                "updatedAt": "2026-07-25T17:54:49.056839Z",
            }
        )
        self.assertEqual(regime["feeSchedule"]["rate"], 0.05)
        self.assertTrue(regime["feeSchedule"]["takerOnly"])
        self.assertTrue(regime["complete"])

    def test_enabled_polymarket_fee_without_schedule_fails_closed(self) -> None:
        self.assertFalse(
            polymarket_fee_regime(
                {
                    "feesEnabled": True,
                    "updatedAt": "2026-07-25T17:54:49.056839Z",
                }
            )["complete"]
        )

    def test_polymarket_fee_regime_requires_revision_timestamp(self) -> None:
        self.assertFalse(
            polymarket_fee_regime(
                {
                    "feesEnabled": True,
                    "feeSchedule": {
                        "exponent": 1,
                        "rate": 0.05,
                        "takerOnly": True,
                    },
                }
            )["complete"]
        )

    def test_polymarket_slug_preserves_local_game_date(self) -> None:
        self.assertEqual(
            event_local_date(
                {"slug": "mlb-sea-tex-2026-07-24"}
            ),
            "2026-07-24",
        )
        self.assertIsNone(event_local_date({"slug": "other-market"}))

    def test_batch_books_posts_all_tokens(self) -> None:
        with patch(
            "collector.post_json",
            return_value=[{"asset_id": "one", "hash": "h"}],
        ) as post:
            rows = polymarket_books(("one", "two"))
        self.assertEqual(rows, [{"asset_id": "one", "hash": "h"}])
        self.assertTrue(post.call_args.args[0].endswith("/books"))
        self.assertEqual(
            post.call_args.args[1],
            [{"token_id": "one"}, {"token_id": "two"}],
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from collector import event_local_date


class CollectorTests(unittest.TestCase):
    def test_polymarket_slug_preserves_local_game_date(self) -> None:
        self.assertEqual(
            event_local_date(
                {"slug": "mlb-sea-tex-2026-07-24"}
            ),
            "2026-07-24",
        )
        self.assertIsNone(event_local_date({"slug": "other-market"}))


if __name__ == "__main__":
    unittest.main()

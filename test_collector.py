from __future__ import annotations

import unittest

from unittest.mock import patch

from collector import event_local_date, polymarket_books


class CollectorTests(unittest.TestCase):
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

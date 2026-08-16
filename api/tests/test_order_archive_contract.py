from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from order_forecast.scripts import pg_utils


class OrderArchiveContractTests(unittest.TestCase):
    def test_archived_summaries_include_canonical_order_id(self):
        with patch.object(pg_utils, "fetch_all", return_value=[{
            "order_id": "order-989262-1777988222651",
            "delivery_date": date(2026, 5, 11),
            "schedule_key": "monday",
            "item_count": 147,
            "order_revision": 0,
            "last_mutation_kind": None,
            "last_mutation_id": None,
            "last_mutation_at": None,
            "reallocation_count": 0,
            "last_reallocated_at": None,
            "last_reallocation_id": None,
        }]):
            summaries = pg_utils.get_archived_dates("989262")

        self.assertEqual(summaries, [{
            "orderId": "order-989262-1777988222651",
            "date": "2026-05-11",
            "scheduleKey": "monday",
            "itemCount": 147,
            "orderRevision": 0,
            "lastMutation": None,
            "storeReallocationSummary": None,
        }])

    def test_route_988200_archived_summary_preserves_reallocation_badge_metadata(self):
        applied_at = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
        with patch.object(pg_utils, "fetch_all", return_value=[{
            "order_id": "order-988200-archived",
            "delivery_date": date(2026, 8, 20),
            "schedule_key": "thursday",
            "item_count": 131,
            "order_revision": 4,
            "last_mutation_kind": "store_reallocation",
            "last_mutation_id": "adjustment-988200",
            "last_mutation_at": applied_at,
            "reallocation_count": 1,
            "last_reallocated_at": applied_at,
            "last_reallocation_id": "adjustment-988200",
        }]):
            [summary] = pg_utils.get_archived_dates("988200")

        self.assertEqual(summary["orderRevision"], 4)
        self.assertEqual(summary["lastMutation"]["kind"], "store_reallocation")
        self.assertEqual(summary["storeReallocationSummary"]["count"], 1)

    def test_exact_id_lookup_is_scoped_to_route(self):
        with patch.object(pg_utils, "fetch_one", return_value={
            "order_id": "order-989262-1777988222651",
            "schedule_key": "monday",
            "order_date": date(2026, 5, 5),
            "delivery_date": date(2026, 5, 11),
            "total_units": 300,
            "store_count": 1,
        }) as fetch_header, patch.object(pg_utils, "fetch_all", return_value=[{
            "store_id": "store-1",
            "store_name": "Store 1",
            "sap": "1001",
            "quantity": 12,
        }]):
            order = pg_utils.get_order_by_id("989262", "order-989262-1777988222651")

        self.assertEqual(fetch_header.call_args.args[1], [
            "989262",
            "order-989262-1777988222651",
        ])
        self.assertIn("route_number = %s AND order_id = %s", fetch_header.call_args.args[0])
        self.assertEqual(order["id"], "order-989262-1777988222651")
        self.assertEqual(order["deliveryDate"], "2026-05-11")

    def test_date_fallback_is_deterministic(self):
        with patch.object(pg_utils, "fetch_one", return_value=None) as fetch_header:
            self.assertIsNone(pg_utils.get_order_by_date("989262", "2026-04-30"))

        sql = fetch_header.call_args.args[0]
        self.assertIn("ORDER BY order_date DESC, order_id DESC", sql)


if __name__ == "__main__":
    unittest.main()

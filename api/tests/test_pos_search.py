import unittest
from unittest.mock import patch

from order_forecast.api.routers import pos


class FakeInvoiceDoc:
    def __init__(self, document_id: str, **data):
        self.id = document_id
        self._data = {
            "id": document_id,
            "status": "active",
            "createdAt": int(document_id),
            "invoiceNumber": f"invoice-{document_id}",
            "store": "Store",
            "date": "2026-09-01",
            **data,
        }

    def to_dict(self):
        return dict(self._data)


class PosSearchTests(unittest.TestCase):
    def test_filtered_search_scans_beyond_first_firestore_batch(self):
        first_batch = [FakeInvoiceDoc(str(value)) for value in range(200, 100, -1)]
        older_match = FakeInvoiceDoc("100", invoiceNumber="TARGET-123")

        with patch.object(
            pos,
            "_list_active_docs",
            side_effect=[first_batch, [older_match]],
        ) as list_active:
            items, next_cursor, has_more = pos._merged_items(
                route="961767",
                db=object(),
                status="active",
                store=None,
                invoice_number="target-123",
                start_date=None,
                end_date=None,
                limit=50,
                cursor=None,
            )

        self.assertEqual([item["id"] for item in items], ["100"])
        self.assertIsNone(next_cursor)
        self.assertFalse(has_more)
        self.assertEqual(list_active.call_count, 2)
        self.assertIsNone(list_active.call_args_list[0].kwargs["cursor"])
        self.assertEqual(list_active.call_args_list[1].kwargs["cursor"], "101")

    def test_unfiltered_browse_keeps_bounded_firestore_read(self):
        newest = [FakeInvoiceDoc(str(value)) for value in range(200, 100, -1)]

        with patch.object(pos, "_list_active_docs", return_value=newest) as list_active:
            items, next_cursor, has_more = pos._merged_items(
                route="961767",
                db=object(),
                status="active",
                store=None,
                invoice_number=None,
                start_date=None,
                end_date=None,
                limit=50,
                cursor=None,
            )

        self.assertEqual(len(items), 50)
        self.assertEqual(next_cursor, "151")
        self.assertTrue(has_more)
        list_active.assert_called_once_with(
            unittest.mock.ANY,
            "961767",
            status="active",
            limit=100,
            cursor=None,
        )


if __name__ == "__main__":
    unittest.main()

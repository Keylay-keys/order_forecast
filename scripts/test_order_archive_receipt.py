from __future__ import annotations

import unittest

from order_forecast.scripts.order_archive_receipt import (
    build_order_archive_projection,
    evaluate_order_archive_receipt,
    write_verified_order_archive_receipt,
)


class _FakeDocument:
    def __init__(self):
        self.writes = []

    def set(self, value):
        self.writes.append(value)


class _FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    def document(self, doc_id):
        return self.documents.setdefault(doc_id, _FakeDocument())


class _FakeDb:
    def __init__(self):
        self.documents = {}

    def collection(self, name):
        if name != "orderArchiveReceipts":
            raise AssertionError(name)
        return _FakeCollection(self.documents)


class TestOrderArchiveReceipt(unittest.TestCase):
    def setUp(self):
        self.order_id = "order-989262-1777988222651"
        self.route_number = "989262"
        self.order = {
            "routeNumber": self.route_number,
            "scheduleKey": "monday",
            "expectedDeliveryDate": "2026-05-11",
            "status": "finalized",
            "stores": [
                {
                    "storeId": "store-b",
                    "storeName": "Store B",
                    "items": [
                        {"sap": "200", "quantity": 4, "cases": 1},
                        {"sap": "ignored-zero", "quantity": 0},
                    ],
                },
                {
                    "storeId": "store-a",
                    "storeName": "Store A",
                    "items": [{"sap": "100", "quantity": 3, "cases": 0}],
                },
            ],
        }

    def test_projection_matches_shared_typescript_vector(self):
        projection = build_order_archive_projection(
            self.order_id,
            self.route_number,
            self.order,
        )
        self.assertEqual(projection["totalUnits"], 7)
        self.assertEqual(projection["storeCount"], 2)
        self.assertEqual(projection["lineItemCount"], 2)
        self.assertEqual(
            projection["sourceFingerprint"],
            "a5f390b8ba72c1772bc31e4f93038672146cb7370a211c2910dafa442851e208",
        )

    def test_unicode_projection_matches_shared_typescript_vector(self):
        projection = build_order_archive_projection(
            "o",
            "1",
            {
                "routeNumber": "1",
                "scheduleKey": "monday",
                "expectedDeliveryDate": "2026-01-01",
                "stores": [
                    {
                        "storeId": "b",
                        "storeName": "Tienda 🥑",
                        "items": [{"sap": "β", "quantity": 2, "cases": 0}],
                    },
                    {
                        "storeId": "a",
                        "storeName": "Éxito",
                        "items": [{"sap": "100", "quantity": 1, "cases": 0}],
                    },
                ],
            },
        )
        self.assertEqual(
            projection["sourceFingerprint"],
            "c42d65e6685c6c987e07b753f79711bbb433e9dc0744cea9784cf5d38f4796eb",
        )

    def test_verified_receipt_is_written_only_for_exact_summary(self):
        db = _FakeDb()
        receipt = write_verified_order_archive_receipt(
            db,
            self.order_id,
            self.route_number,
            self.order,
            archive_total_units=7,
            archive_store_count=2,
            archive_line_item_count=2,
        )
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(len(db.documents[self.order_id].writes), 1)
        projection = build_order_archive_projection(
            self.order_id,
            self.route_number,
            self.order,
        )
        self.assertIsNone(evaluate_order_archive_receipt(projection, receipt))

    def test_missing_and_changed_source_receipts_fail_closed(self):
        projection = build_order_archive_projection(
            self.order_id,
            self.route_number,
            self.order,
        )
        self.assertEqual(
            evaluate_order_archive_receipt(projection, None),
            "receipt_missing",
        )
        receipt = {
            **projection,
            "status": "verified",
            "archiveTotalUnits": projection["totalUnits"],
            "archiveStoreCount": projection["storeCount"],
            "archiveLineItemCount": projection["lineItemCount"],
        }
        changed = {**projection, "sourceFingerprint": "changed"}
        self.assertEqual(
            evaluate_order_archive_receipt(changed, receipt),
            "receipt_source_changed",
        )

    def test_summary_mismatch_fails_without_receipt(self):
        db = _FakeDb()
        with self.assertRaisesRegex(ValueError, "ORDER_ARCHIVE_PROJECTION_MISMATCH"):
            write_verified_order_archive_receipt(
                db,
                self.order_id,
                self.route_number,
                self.order,
                archive_total_units=7,
                archive_store_count=2,
                archive_line_item_count=1,
            )
        self.assertEqual(db.documents, {})


if __name__ == "__main__":
    unittest.main()

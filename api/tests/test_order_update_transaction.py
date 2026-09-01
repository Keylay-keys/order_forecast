import copy
import unittest
from datetime import datetime, timezone

from fastapi import HTTPException

from order_forecast.api.models import OrderItem, OrderUpdateRequest, StoreOrder
from order_forecast.api.order_update import merge_order_store_patches
from order_forecast.api.routers import orders


ROUTE = "988200"
ORDER_ID = "order-988200-update-transaction"


def _payload(*stores: StoreOrder, **metadata) -> OrderUpdateRequest:
    return OrderUpdateRequest(stores=list(stores), **metadata)


def _store(store_id: str, name: str, items, **extra) -> dict:
    return {
        "storeId": store_id,
        "storeName": name,
        "items": items,
        **extra,
    }


class _Snapshot:
    def __init__(self, data):
        self._data = copy.deepcopy(data)
        self.exists = data is not None

    def to_dict(self):
        return copy.deepcopy(self._data)


class _DocumentRef:
    def __init__(self, data):
        self.data = copy.deepcopy(data)
        self.get_transactions = []

    def get(self, transaction=None):
        self.get_transactions.append(transaction)
        return _Snapshot(self.data)


class _Transaction:
    def __init__(self):
        self.updates = []

    def update(self, ref, data):
        self.updates.append((ref, copy.deepcopy(data)))
        ref.data.update(copy.deepcopy(data))


class OrderUpdateMergeTests(unittest.TestCase):
    def test_merge_preserves_itemless_stores_and_existing_metadata(self):
        existing = [
            _store("empty", "Empty Store", [], completed=True, custom="keep"),
            _store(
                "filled",
                "Old Name",
                [{"sap": "100", "quantity": 2, "promoActive": True}],
                completed=False,
            ),
        ]
        incoming = [
            StoreOrder(
                storeId="filled",
                storeName="Current Name",
                items=[OrderItem(sap="100", quantity=7)],
            )
        ]

        merged = merge_order_store_patches(existing, incoming)

        self.assertEqual([store["storeId"] for store in merged], ["empty", "filled"])
        self.assertEqual(merged[0]["items"], [])
        self.assertTrue(merged[0]["completed"])
        self.assertEqual(merged[0]["custom"], "keep")
        self.assertEqual(merged[1]["storeName"], "Current Name")
        self.assertFalse(merged[1]["completed"])
        self.assertEqual(
            merged[1]["items"],
            [{"sap": "100", "quantity": 7, "promoActive": True}],
        )

    def test_zero_quantity_removes_item_without_deleting_store(self):
        existing = [
            _store(
                "filled",
                "Store",
                [{"sap": "100", "quantity": 2, "source": "forecast"}],
            )
        ]
        incoming = [
            StoreOrder(
                storeId="filled",
                storeName="Store",
                items=[OrderItem(sap="100", quantity=0)],
            )
        ]

        merged = merge_order_store_patches(existing, incoming)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["storeId"], "filled")
        self.assertEqual(merged[0]["items"], [])


class OrderUpdateTransactionTests(unittest.TestCase):
    def setUp(self):
        self.apply = orders._update_order_document.to_wrap
        self.now = datetime(2026, 8, 31, 20, 0, tzinfo=timezone.utc)
        self.order = {
            "id": ORDER_ID,
            "routeNumber": ROUTE,
            "status": "draft",
            "notes": "old",
            "sapOrder": ["100", "200"],
            "stores": [_store("empty", "Empty", [], completed=True)],
        }
        self.ref = _DocumentRef(self.order)
        self.transaction = _Transaction()

    def _run(self, payload=None):
        return self.apply(
            self.transaction,
            order_ref=self.ref,
            route_number=ROUTE,
            payload=payload or _payload(),
            now=self.now,
        )

    def test_commits_and_returns_exact_merged_value_without_post_commit_read(self):
        result = self._run(
            _payload(
                StoreOrder(
                    storeId="new",
                    storeName="New Store",
                    items=[OrderItem(sap="100", quantity=3)],
                ),
                notes="new",
            )
        )

        self.assertEqual(self.ref.get_transactions, [self.transaction])
        self.assertEqual(len(self.transaction.updates), 1)
        self.assertEqual(result, self.ref.data)
        self.assertEqual(result["notes"], "new")
        self.assertEqual(result["sapOrder"], ["100", "200"])
        self.assertEqual([store["storeId"] for store in result["stores"]], ["empty", "new"])

    def test_rechecks_draft_status_inside_transaction(self):
        self.ref.data["status"] = "finalized"

        with self.assertRaises(HTTPException) as raised:
            self._run()

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(self.transaction.updates, [])

    def test_rechecks_route_inside_transaction(self):
        self.ref.data["routeNumber"] = "989262"

        with self.assertRaises(HTTPException) as raised:
            self._run()

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(self.transaction.updates, [])


if __name__ == "__main__":
    unittest.main()

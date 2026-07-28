import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import Order
from order_forecast.api.order_attention import build_core_item_issues
from order_forecast.api.routers import orders


class _Snapshot:
    def __init__(self, data, doc_id="doc"):
        self._data = data
        self.id = doc_id
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _OrderRef:
    def __init__(self, data):
        self.data = data

    def get(self, transaction=None):
        del transaction
        return _Snapshot(self.data, str((self.data or {}).get("id") or "order"))


class _StoresRef:
    def __init__(self, stores):
        self.stores = stores
        self.stream_count = 0

    def stream(self, transaction=None):
        del transaction
        self.stream_count += 1
        return [
            _Snapshot(store, str(store.get("id") or f"store-{index}"))
            for index, store in enumerate(self.stores)
        ]


class _Transaction:
    def __init__(self, fail_update=False):
        self.fail_update = fail_update
        self.updates = []

    def update(self, ref, payload):
        if self.fail_update:
            raise RuntimeError("transaction write failed")
        self.updates.append((ref, dict(payload)))


def _order_data(**overrides):
    data = {
        "id": "order-988200-1",
        "routeNumber": "988200",
        "userId": "owner-1",
        "orderDate": "2026-07-28",
        "expectedDeliveryDate": "2026-07-30",
        "status": "draft",
        "stores": [],
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc),
    }
    data.update(overrides)
    return data


class CoreItemSelectorTests(unittest.TestCase):
    def test_zero_quantity_core_item_matches_client_issue_identity(self):
        issues = build_core_item_issues(
            order_data=_order_data(coreItemPolicyVersion=1, coreItemOverrides=[]),
            stores=[{
                "id": "store-1",
                "name": "Test Store",
                "items": ["28934"],
                "coreItemSaps": ["28934"],
            }],
        )

        self.assertEqual(issues, [{
            "kind": "core",
            "storeId": "store-1",
            "storeName": "Test Store",
            "sap": "28934",
            "storeQuantity": 0,
            "assignedInboundQuantity": 0,
            "purchaseQuantity": 0,
            "requiresOverride": True,
        }])

    def test_inbound_units_are_not_added_to_store_quantity_twice(self):
        issues = build_core_item_issues(
            order_data=_order_data(
                stores=[{
                    "storeId": "store-1",
                    "storeName": "Test Store",
                    "items": [{"sap": "28934", "quantity": 12}],
                }],
                inboundTransferStoreAllocations=[{
                    "storeId": "store-1",
                    "sap": "28934",
                    "units": 12,
                }],
            ),
            stores=[{
                "id": "store-1",
                "name": "Test Store",
                "coreItemSaps": ["28934"],
            }],
        )

        self.assertEqual(issues, [])

    def test_allocation_without_store_quantity_does_not_satisfy_core(self):
        issues = build_core_item_issues(
            order_data=_order_data(
                inboundTransferStoreAllocations=[{
                    "storeId": "store-1",
                    "sap": "28934",
                    "units": 12,
                }],
            ),
            stores=[{
                "id": "store-1",
                "name": "Test Store",
                "coreItemSaps": ["28934"],
            }],
        )

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["assignedInboundQuantity"], 12)
        self.assertEqual(issues[0]["storeQuantity"], 0)

    def test_order_override_clears_only_matching_store_sap(self):
        issues = build_core_item_issues(
            order_data=_order_data(coreItemOverrides=[{
                "storeId": "store-1",
                "sap": "28934",
                "overriddenAtMs": 100,
                "overriddenByUserId": "member-1",
            }]),
            stores=[{
                "id": "store-1",
                "name": "Test Store",
                "coreItemSaps": ["28934", "34398"],
            }],
        )

        self.assertEqual([issue["sap"] for issue in issues], ["34398"])


class CoreItemFinalizeTransactionTests(unittest.TestCase):
    def setUp(self):
        self.finalize = orders._finalize_order_document.to_wrap
        self.now = datetime.now(timezone.utc)

    def test_legacy_order_finalizes_without_loading_core_metadata(self):
        order_ref = _OrderRef(_order_data())
        stores_ref = _StoresRef([{
            "id": "store-1",
            "coreItemSaps": ["28934"],
        }])
        transaction = _Transaction()

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", True):
            self.finalize(
                transaction,
                order_ref=order_ref,
                stores_ref=stores_ref,
                route_number="988200",
                now=self.now,
            )

        self.assertEqual(stores_ref.stream_count, 0)
        self.assertEqual(transaction.updates[0][1]["status"], "finalized")

    def test_marker_bearing_order_finalizes_while_switch_is_disabled(self):
        transaction = _Transaction()

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", False):
            self.finalize(
                transaction,
                order_ref=_OrderRef(_order_data(coreItemPolicyVersion=1)),
                stores_ref=_StoresRef([{
                    "id": "store-1",
                    "coreItemSaps": ["28934"],
                }]),
                route_number="988200",
                now=self.now,
            )

        self.assertEqual(transaction.updates[0][1]["status"], "finalized")

    def test_marker_bearing_order_is_blocked_atomically(self):
        transaction = _Transaction()

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", True):
            with self.assertRaises(StructuredApiError) as raised:
                self.finalize(
                    transaction,
                    order_ref=_OrderRef(_order_data(
                        coreItemPolicyVersion=1,
                        coreItemOverrides=[],
                    )),
                    stores_ref=_StoresRef([{
                        "id": "store-1",
                        "name": "Test Store",
                        "coreItemSaps": ["28934"],
                    }]),
                    route_number="988200",
                    now=self.now,
                )

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.code, "CORE_ITEMS_REQUIRED")
        self.assertEqual(
            raised.exception.details["items"][0]["storeId"],
            "store-1",
        )
        self.assertEqual(transaction.updates, [])

    def test_transaction_write_failure_leaves_no_finalize_update(self):
        transaction = _Transaction(fail_update=True)

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", False):
            with self.assertRaisesRegex(RuntimeError, "transaction write failed"):
                self.finalize(
                    transaction,
                    order_ref=_OrderRef(_order_data(coreItemPolicyVersion=1)),
                    stores_ref=_StoresRef([]),
                    route_number="988200",
                    now=self.now,
                )

        self.assertEqual(transaction.updates, [])

    def test_order_model_remains_backward_compatible_without_marker(self):
        parsed = Order(**_order_data())
        self.assertIsNone(parsed.coreItemPolicyVersion)
        self.assertIsNone(parsed.coreItemOverrides)


if __name__ == "__main__":
    unittest.main()

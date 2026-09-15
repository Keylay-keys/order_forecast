import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from order_forecast.api.order_attention import build_next_order_store_updates
from order_forecast.api.routers import orders


class _Reference:
    def __init__(self, doc_id):
        self.id = doc_id


class _Snapshot:
    def __init__(self, data, doc_id):
        self._data = data
        self.id = doc_id
        self.exists = data is not None
        self.reference = _Reference(doc_id)

    def to_dict(self):
        return dict(self._data or {})


class _OrderRef:
    def __init__(self, data):
        self.data = data
        self.id = str(data.get("id") or "order")

    def get(self, transaction=None):
        del transaction
        return _Snapshot(self.data, self.id)


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
    def __init__(self):
        self.updates = []

    def update(self, reference, payload):
        self.updates.append((reference, dict(payload)))


def _order_data(**overrides):
    data = {
        "id": "order-988200-current",
        "routeNumber": "988200",
        "userId": "owner-1",
        "orderDate": "2026-07-28",
        "expectedDeliveryDate": "2026-07-30",
        "status": "draft",
        "stores": [],
        "createdAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc),
        "coreItemPolicyVersion": 1,
        "coreItemOverrides": [],
    }
    data.update(overrides)
    return data


def _reminder(after_order_id):
    return {
        "sap": "28934",
        "afterOrderId": after_order_id,
        "createdAtMs": 100,
        "createdByUserId": "owner-1",
    }


class NextOrderResolutionTests(unittest.TestCase):
    def setUp(self):
        self.finalize = orders._finalize_order_document.to_wrap
        self.now = datetime.now(timezone.utc)

    def test_positive_active_reminder_resolves_in_finalize_transaction(self):
        transaction = _Transaction()
        stores_ref = _StoresRef([{
            "id": "store-1",
            "name": "Test Store",
            "items": ["28934"],
            "nextOrderItems": [_reminder("order-988200-previous")],
        }])

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", False):
            self.finalize(
                transaction,
                order_ref=_OrderRef(_order_data(stores=[{
                    "storeId": "store-1",
                    "storeName": "Test Store",
                    "items": [{"sap": "28934", "quantity": 12}],
                }])),
                stores_ref=stores_ref,
                route_number="988200",
                now=self.now,
            )

        self.assertEqual(stores_ref.stream_count, 1)
        self.assertEqual(transaction.updates[0][1]["status"], "finalized")
        self.assertEqual(transaction.updates[1][0].id, "store-1")
        self.assertEqual(transaction.updates[1][1]["nextOrderItems"], [])

    def test_zero_quantity_remains_non_blocking_and_persisted(self):
        transaction = _Transaction()

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", False):
            self.finalize(
                transaction,
                order_ref=_OrderRef(_order_data()),
                stores_ref=_StoresRef([{
                    "id": "store-1",
                    "items": ["28934"],
                    "nextOrderItems": [_reminder("order-988200-previous")],
                }]),
                route_number="988200",
                now=self.now,
            )

        self.assertEqual(len(transaction.updates), 1)
        self.assertEqual(transaction.updates[0][1]["status"], "finalized")

    def test_next_order_again_for_current_order_is_not_cleared(self):
        updates = build_next_order_store_updates(
            order_data=_order_data(stores=[{
                "storeId": "store-1",
                "items": [{"sap": "28934", "quantity": 12}],
            }]),
            stores=[{
                "id": "store-1",
                "nextOrderItems": [_reminder("order-988200-current")],
            }],
        )

        self.assertEqual(updates, {})

    def test_repeated_resolution_is_idempotent(self):
        updates = build_next_order_store_updates(
            order_data=_order_data(stores=[{
                "storeId": "store-1",
                "items": [{"sap": "28934", "quantity": 12}],
            }]),
            stores=[{
                "id": "store-1",
                "nextOrderItems": [],
            }],
        )

        self.assertEqual(updates, {})

    def test_legacy_order_does_not_load_or_mutate_reminders(self):
        transaction = _Transaction()
        stores_ref = _StoresRef([{
            "id": "store-1",
            "nextOrderItems": [_reminder("order-988200-previous")],
        }])
        legacy_order = _order_data()
        legacy_order.pop("coreItemPolicyVersion")
        legacy_order.pop("coreItemOverrides")

        with patch.object(orders, "CORE_ITEM_ENFORCEMENT_ENABLED", True):
            self.finalize(
                transaction,
                order_ref=_OrderRef(legacy_order),
                stores_ref=stores_ref,
                route_number="988200",
                now=self.now,
            )

        self.assertEqual(stores_ref.stream_count, 0)
        self.assertEqual(len(transaction.updates), 1)
        self.assertEqual(transaction.updates[0][1]["status"], "finalized")

    def test_finalize_stamps_old_client_draft_exactly_once_when_inputs_match(self):
        transaction = _Transaction()
        stamp = {
            "version": 2,
            "date": "2026-07-28",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 1,
            "deliveryOffsetDays": 2,
            "provenance": "cycle",
        }
        result = self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data(scheduleKey="tuesday")),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=self.now,
            resolved_stamp=stamp,
            resolved_delivery_date="2026-07-30",
            resolved_schedule_key="tuesday",
        )

        self.assertEqual(transaction.updates[0][1]["scheduledOrder"], stamp)
        self.assertEqual(result["scheduledOrder"], stamp)

    def test_finalize_never_overwrites_existing_stamp(self):
        transaction = _Transaction()
        existing = {
            "version": 2,
            "date": "2026-07-28",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 1,
            "deliveryOffsetDays": 2,
            "provenance": "cycle",
        }
        replacement = {**existing, "date": "2099-01-01"}
        result = self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data(scheduleKey="tuesday", scheduledOrder=existing)),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=self.now,
            resolved_stamp=replacement,
            resolved_delivery_date="2026-07-30",
            resolved_schedule_key="tuesday",
        )

        self.assertNotIn("scheduledOrder", transaction.updates[0][1])
        self.assertEqual(result["scheduledOrder"], existing)

    def test_finalize_does_not_stamp_if_delivery_or_key_changed_after_resolution(self):
        transaction = _Transaction()
        stamp = {
            "version": 2,
            "date": "2026-07-28",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 1,
            "deliveryOffsetDays": 2,
            "provenance": "cycle",
        }
        self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data(scheduleKey="monday")),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=self.now,
            resolved_stamp=stamp,
            resolved_delivery_date="2026-07-30",
            resolved_schedule_key="tuesday",
        )

        self.assertNotIn("scheduledOrder", transaction.updates[0][1])


    def test_adjustment_anchor_uses_server_instant_and_owner_timezone(self):
        anchor = orders._build_adjustment_anchor(
            datetime(2026, 9, 4, 2, 23, 45, 678000, tzinfo=timezone.utc),
            "America/Denver",
        )
        self.assertEqual(anchor, {
            "version": 1,
            "finalizedAtMs": 1788488625678,
            "finalizedDate": "2026-09-03",
            "timezone": "America/Denver",
        })

    def test_finalize_writes_anchor_without_mutating_existing_schedule(self):
        transaction = _Transaction()
        scheduled_order = {
            "version": 2,
            "date": "2026-09-01",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 1,
            "deliveryOffsetDays": 2,
            "provenance": "cycle",
        }
        original_schedule = dict(scheduled_order)
        anchor = orders._build_adjustment_anchor(
            datetime(2026, 9, 4, 2, 23, tzinfo=timezone.utc),
            "America/Denver",
        )
        result = self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data(scheduledOrder=scheduled_order)),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=datetime(2026, 9, 4, 2, 23, tzinfo=timezone.utc),
            resolved_stamp={**scheduled_order, "date": "2099-01-01"},
            resolved_adjustment_anchor=anchor,
        )

        update = transaction.updates[0][1]
        self.assertNotIn("scheduledOrder", update)
        self.assertEqual(update["adjustmentAnchor"], anchor)
        self.assertEqual(result["scheduledOrder"], original_schedule)
        self.assertEqual(scheduled_order, original_schedule)

    def test_finalize_never_overwrites_existing_anchor(self):
        transaction = _Transaction()
        scheduled_order = {
            "version": 2,
            "date": "2026-09-01",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 1,
            "deliveryOffsetDays": 2,
            "provenance": "cycle",
        }
        existing_anchor = {
            "version": 1,
            "finalizedAtMs": 1788488625678,
            "finalizedDate": "2026-09-03",
            "timezone": "America/Denver",
        }
        result = self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data(
                scheduledOrder=scheduled_order,
                adjustmentAnchor=existing_anchor,
            )),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=self.now,
            resolved_adjustment_anchor={**existing_anchor, "finalizedAtMs": 1},
        )

        self.assertNotIn("adjustmentAnchor", transaction.updates[0][1])
        self.assertEqual(result["adjustmentAnchor"], existing_anchor)

    def test_finalize_without_schedule_authority_does_not_add_anchor(self):
        transaction = _Transaction()
        self.finalize(
            transaction,
            order_ref=_OrderRef(_order_data()),
            stores_ref=_StoresRef([]),
            route_number="988200",
            now=self.now,
            resolved_adjustment_anchor={
                "version": 1,
                "finalizedAtMs": 1,
                "finalizedDate": "1970-01-01",
                "timezone": "UTC",
            },
        )
        self.assertNotIn("adjustmentAnchor", transaction.updates[0][1])


if __name__ == "__main__":
    unittest.main()

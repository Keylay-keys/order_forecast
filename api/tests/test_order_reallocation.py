import unittest
from copy import deepcopy
from datetime import datetime, timezone

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import (
    Order,
    StoreReallocationMoveRequest,
    StoreReallocationRequest,
)
from order_forecast.api.order_reallocation import (
    apply_store_reallocation,
    moves_signature,
)
from order_forecast.api.routers import orders


ROUTE = "988200"
REALLOCATION_ID = "2c52d6d0-04be-4d1b-bb37-2a0f26e8397c"


class _Snapshot:
    def __init__(self, data, doc_id="doc"):
        self._data = data
        self.id = doc_id
        self.exists = data is not None

    def to_dict(self):
        return deepcopy(self._data or {})


class _DocRef:
    def __init__(self, doc_id, data=None):
        self.id = doc_id
        self.data = deepcopy(data)

    def get(self, transaction=None):
        del transaction
        return _Snapshot(self.data, self.id)


class _CollectionRef:
    def __init__(self, docs):
        self.docs = docs

    def stream(self, transaction=None):
        del transaction
        return [_Snapshot(ref.data, ref.id) for ref in self.docs.values()]

    def document(self, doc_id):
        return self.docs.setdefault(doc_id, _DocRef(doc_id))


class _Transaction:
    def __init__(self):
        self.updates = []
        self.sets = []

    def update(self, ref, payload):
        copied = deepcopy(payload)
        self.updates.append((ref, copied))
        ref.data.update(copied)

    def set(self, ref, payload):
        copied = deepcopy(payload)
        self.sets.append((ref, copied))
        ref.data = copied


def _move(**overrides):
    data = {
        "sap": "28934",
        "fromStoreId": "store-a",
        "toStoreId": "store-b",
        "unitQuantity": 4,
    }
    data.update(overrides)
    return StoreReallocationMoveRequest(**data)


def _order(**overrides):
    now = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
    data = {
        "id": "order-988200-reallocation",
        "routeNumber": ROUTE,
        "userId": "owner-988200",
        "orderDate": "2026-08-15",
        "expectedDeliveryDate": "2026-08-18",
        "scheduleKey": "monday",
        "status": "finalized",
        "stores": [
            {
                "storeId": "store-a",
                "storeName": "Alpha",
                "items": [{"sap": "28934", "quantity": 10, "promoActive": True}],
            },
            {
                "storeId": "store-b",
                "storeName": "Bravo",
                "items": [{"sap": "28934", "quantity": 2}],
            },
        ],
        "createdAt": now,
        "updatedAt": now,
        "submittedAt": now,
        "orderRevision": 3,
        "forecastContext": {"schemaVersion": 2, "forecastId": "forecast-988200"},
    }
    data.update(overrides)
    return data


def _route_stores():
    return {
        "store-a": {"name": "Alpha", "isActive": True, "items": ["28934"]},
        "store-b": {"name": "Bravo", "isActive": True, "items": ["28934"]},
    }


class StoreReallocationPureTests(unittest.TestCase):
    def test_route_988200_move_preserves_totals_and_metadata(self):
        source = _order()
        result = apply_store_reallocation(
            order_data=source,
            moves=[_move()],
            route_stores=_route_stores(),
            products={"28934": {"fullName": "Mission Test Item", "casePack": 12}},
            enforce_core_items=False,
        )

        alpha, bravo = result["stores"]
        self.assertEqual(alpha["items"], [{"sap": "28934", "quantity": 6, "promoActive": True}])
        self.assertEqual(bravo["items"], [{"sap": "28934", "quantity": 6}])
        self.assertEqual(result["beforeTotals"], {"28934": 12})
        self.assertEqual(result["afterTotals"], {"28934": 12})
        self.assertEqual(result["auditMoves"][0]["fullName"], "Mission Test Item")
        self.assertEqual(source["stores"][0]["items"][0]["quantity"], 10)

    def test_signature_is_order_independent(self):
        first = _move(unitQuantity=2)
        second = _move(sap="34398", unitQuantity=3)
        self.assertEqual(moves_signature([first, second]), moves_signature([second, first]))

    def test_rejects_source_overdraw(self):
        with self.assertRaises(StructuredApiError) as raised:
            apply_store_reallocation(
                order_data=_order(),
                moves=[_move(unitQuantity=11)],
                route_stores=_route_stores(),
                products={},
                enforce_core_items=False,
            )
        self.assertEqual(raised.exception.code, "REALLOCATION_SOURCE_OVERDRAW")

    def test_rejects_inbound_transfer_floor_violation(self):
        with self.assertRaises(StructuredApiError) as raised:
            apply_store_reallocation(
                order_data=_order(inboundTransferStoreAllocations=[{
                    "storeId": "store-a",
                    "sap": "28934",
                    "units": 8,
                }]),
                moves=[_move(unitQuantity=4)],
                route_stores=_route_stores(),
                products={},
                enforce_core_items=False,
            )
        self.assertEqual(raised.exception.code, "REALLOCATION_INBOUND_CONFLICT")


class StoreReallocationTransactionTests(unittest.TestCase):
    def setUp(self):
        self.apply_transaction = orders._apply_store_reallocation_document.to_wrap
        self.now = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
        self.order_ref = _DocRef("order-988200-reallocation", _order())
        self.adjustment_ref = _DocRef(str(REALLOCATION_ID))
        self.audit_ref = _DocRef(f"store-reallocation-{REALLOCATION_ID}")
        self.stores_ref = _CollectionRef({
            store_id: _DocRef(store_id, data)
            for store_id, data in _route_stores().items()
        })
        self.products_ref = _CollectionRef({
            "28934": _DocRef("28934", {"fullName": "Mission Test Item", "casePack": 12})
        })
        self.payload = StoreReallocationRequest(
            reallocationId=REALLOCATION_ID,
            baseOrderRevision=3,
            moves=[_move()],
        )

    def _apply(self, transaction, payload=None):
        return self.apply_transaction(
            transaction,
            order_ref=self.order_ref,
            adjustment_ref=self.adjustment_ref,
            audit_ref=self.audit_ref,
            stores_ref=self.stores_ref,
            products_ref=self.products_ref,
            route_number=ROUTE,
            payload=payload or self.payload,
            actor_user_id="member-988200",
            requester_is_owner=False,
            local_date="2026-08-16",
            timezone_name="America/Denver",
            now=self.now,
        )

    def test_route_988200_transaction_updates_order_receipt_and_audit(self):
        transaction = _Transaction()
        result = self._apply(transaction)

        self.assertFalse(result["idempotent"])
        self.assertEqual(result["orderRevision"], 4)
        self.assertEqual(self.order_ref.data["lastMutation"]["kind"], "store_reallocation")
        self.assertEqual(self.order_ref.data["storeReallocationSummary"]["count"], 1)
        self.assertEqual(self.adjustment_ref.data["projection"]["status"], "pending")
        self.assertEqual(self.audit_ref.data["action"], "order_store_reallocated")
        self.assertEqual(len(transaction.updates), 1)
        self.assertEqual(len(transaction.sets), 2)

    def test_lost_response_retry_is_idempotent_before_stale_check(self):
        first = self._apply(_Transaction())
        second = self._apply(_Transaction())
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["orderRevision"], 4)
        self.assertEqual(self.order_ref.data["stores"][0]["items"][0]["quantity"], 6)

    def test_client_created_draft_is_atomically_promoted(self):
        created_at = datetime(2026, 8, 16, 11, tzinfo=timezone.utc)
        self.adjustment_ref.data = {
            "id": str(REALLOCATION_ID),
            "routeNumber": ROUTE,
            "userId": "member-988200",
            "status": "draft",
            "mode": "store_reallocation",
            "sourceOrderId": self.order_ref.id,
            "lines": [],
            "email": {},
            "createdAt": created_at,
        }

        result = self._apply(_Transaction())

        self.assertFalse(result["idempotent"])
        self.assertEqual(result["orderRevision"], 4)
        self.assertEqual(self.adjustment_ref.data["status"], "applied")
        self.assertEqual(self.adjustment_ref.data["createdAt"], created_at)
        self.assertEqual(
            self.adjustment_ref.data["storeReallocation"]["appliedByUserId"],
            "member-988200",
        )

    def test_client_draft_owned_by_another_user_is_hidden(self):
        self.adjustment_ref.data = {
            "id": str(REALLOCATION_ID),
            "routeNumber": ROUTE,
            "userId": "other-member-988200",
            "status": "draft",
            "mode": "store_reallocation",
            "sourceOrderId": self.order_ref.id,
        }

        with self.assertRaises(StructuredApiError) as raised:
            self._apply(_Transaction())

        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(raised.exception.code, "ORDER_NOT_FOUND")

    def test_client_draft_for_another_order_is_rejected(self):
        self.adjustment_ref.data = {
            "id": str(REALLOCATION_ID),
            "routeNumber": ROUTE,
            "userId": "member-988200",
            "status": "draft",
            "mode": "store_reallocation",
            "sourceOrderId": "order-988200-other",
        }

        with self.assertRaises(StructuredApiError) as raised:
            self._apply(_Transaction())

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.code, "REALLOCATION_ID_CONFLICT")

    def test_client_draft_with_server_owned_fields_is_rejected(self):
        self.adjustment_ref.data = {
            "id": str(REALLOCATION_ID),
            "routeNumber": ROUTE,
            "userId": "member-988200",
            "status": "draft",
            "mode": "store_reallocation",
            "sourceOrderId": self.order_ref.id,
            "projection": {"status": "pending"},
        }

        with self.assertRaises(StructuredApiError) as raised:
            self._apply(_Transaction())

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.code, "REALLOCATION_ID_CONFLICT")

    def test_stale_new_reallocation_is_rejected(self):
        self._apply(_Transaction())
        new_payload = StoreReallocationRequest(
            reallocationId="7c3dd0dc-d840-4437-8950-fd92db439265",
            baseOrderRevision=3,
            moves=[_move(unitQuantity=1)],
        )
        self.adjustment_ref = _DocRef(str(new_payload.reallocationId))
        with self.assertRaises(StructuredApiError) as raised:
            self._apply(_Transaction(), new_payload)
        self.assertEqual(raised.exception.code, "REALLOCATION_STALE_ORDER")

    def test_order_model_accepts_new_summary_fields(self):
        parsed = Order(**_order(
            lastMutation={
                "kind": "store_reallocation",
                "mutationId": str(REALLOCATION_ID),
                "atMs": 1,
            },
            storeReallocationSummary={
                "count": 1,
                "lastAppliedAtMs": 1,
                "lastAdjustmentId": str(REALLOCATION_ID),
            },
        ))
        self.assertEqual(parsed.orderRevision, 3)
        self.assertEqual(parsed.storeReallocationSummary.count, 1)


if __name__ == "__main__":
    unittest.main()

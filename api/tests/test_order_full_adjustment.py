from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import datetime, timezone

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import FullOrderAdjustmentConfirmRequest
from order_forecast.api.order_full_adjustment import (
    cumulative_lines_signature,
    validate_and_merge_full_adjustment,
    working_copy_signature,
)
from order_forecast.api.routers import orders


ROUTE = "988200"


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
    def __init__(self, docs=None):
        self.docs = docs or {}

    def stream(self, transaction=None):
        del transaction
        return [_Snapshot(ref.data, ref.id) for ref in self.docs.values()]


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


def _order(quantity=12, revision=3):
    return {
        "id": "order-988200-full-adjustment",
        "routeNumber": ROUTE,
        "status": "finalized",
        "orderRevision": revision,
        "stores": [{
            "storeId": "store-a",
            "storeName": "Alpha",
            "items": [{"sap": "41051", "quantity": quantity}],
        }],
        "routeTransfers": [],
        "routeSplittingEnabled": False,
        "inboundTransfersUsed": [],
        "inboundTransferStoreAllocations": [],
    }


def _batch():
    lines = [{
        "sap": "41051",
        "fullName": "Mission Test Item",
        "direction": "add",
        "casePack": 12,
        "caseQuantity": 1,
    }]
    working = _order(quantity=24)
    return {
        "id": "batch-988200",
        "adjustmentId": "adjustment-988200",
        "sourceOrderId": "order-988200-full-adjustment",
        "routeNumber": ROUTE,
        "cumulativeLines": lines,
        "cumulativeSignature": cumulative_lines_signature(lines),
        "workingCopySnapshot": working,
        "workingCopySignature": working_copy_signature(working),
    }


class FullAdjustmentPureTests(unittest.TestCase):
    def test_route_988200_verifies_batch_and_merges_selected_sap(self):
        merged = validate_and_merge_full_adjustment(
            current_order=_order(),
            batch=_batch(),
            accepted_saps=["41051"],
        )
        self.assertEqual(merged["stores"][0]["items"][0]["quantity"], 24)

    def test_route_988200_rejects_changed_canonical_baseline(self):
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order(quantity=18),
                batch=_batch(),
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_BASELINE_CHANGED")

    def test_route_988200_rejects_transfer_changes_even_with_a_valid_signature(self):
        batch = _batch()
        batch["workingCopySnapshot"]["routeSplittingEnabled"] = True
        batch["workingCopySnapshot"]["routeTransfers"] = [{
            "sap": "41051",
            "toRouteNumber": "988201",
            "units": 12,
            "casePack": 12,
        }]
        batch["workingCopySignature"] = working_copy_signature(batch["workingCopySnapshot"])
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order(),
                batch=batch,
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_TRANSFER_STATE_CHANGED")

    def test_route_988200_rejects_fractional_working_quantities(self):
        batch = _batch()
        batch["workingCopySnapshot"]["stores"][0]["items"][0]["quantity"] = 24.5
        batch["workingCopySignature"] = working_copy_signature(batch["workingCopySnapshot"])
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order(),
                batch=batch,
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_WORKING_COPY_INVALID")

    def test_route_988200_rejects_invalid_sent_line_semantics(self):
        batch = _batch()
        batch["cumulativeLines"][0]["direction"] = "replace"
        batch["cumulativeSignature"] = cumulative_lines_signature(batch["cumulativeLines"])
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order(),
                batch=batch,
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_BATCH_INVALID")


class FullAdjustmentTransactionTests(unittest.TestCase):
    def setUp(self):
        self.apply = orders._confirm_full_order_adjustment_document.to_wrap
        self.batch = _batch()
        self.order_ref = _DocRef("order-988200-full-adjustment", _order())
        self.adjustment_ref = _DocRef("adjustment-988200", {
            "id": "adjustment-988200",
            "routeNumber": ROUTE,
            "sourceOrderId": self.order_ref.id,
            "userId": "owner-988200",
            "mode": "full_order",
            "status": "sent",
            "lastSent": {
                "batchId": self.batch["id"],
                "cumulativeSignature": self.batch["cumulativeSignature"],
                "workingCopySignature": self.batch["workingCopySignature"],
            },
        })
        self.batch_ref = _DocRef(self.batch["id"], self.batch)
        self.audit_ref = _DocRef("full-adjustment-adjustment-988200")
        self.payload = FullOrderAdjustmentConfirmRequest(
            adjustmentId="adjustment-988200",
            sentBatchId=self.batch["id"],
            baseOrderRevision=3,
            acceptedSaps=["41051"],
        )

    def _confirm(self):
        return self.apply(
            _Transaction(),
            order_ref=self.order_ref,
            adjustment_ref=self.adjustment_ref,
            batch_ref=self.batch_ref,
            audit_ref=self.audit_ref,
            stores_ref=_CollectionRef({
                "store-a": _DocRef("store-a", {"isActive": True, "items": ["41051"]}),
            }),
            route_number=ROUTE,
            payload=self.payload,
            actor_user_id="owner-988200",
            requester_is_owner=False,
            now=datetime(2026, 8, 16, 12, tzinfo=timezone.utc),
        )

    def test_route_988200_confirmation_revises_order_and_creates_pending_projection(self):
        result = self._confirm()
        self.assertFalse(result["idempotent"])
        self.assertEqual(result["orderRevision"], 4)
        self.assertEqual(self.order_ref.data["stores"][0]["items"][0]["quantity"], 24)
        self.assertEqual(self.order_ref.data["lastMutation"]["kind"], "full_adjustment")
        self.assertEqual(self.adjustment_ref.data["projection"]["status"], "pending")

    def test_route_988200_confirmation_retry_is_idempotent(self):
        self._confirm()
        result = self._confirm()
        self.assertTrue(result["idempotent"])
        self.assertEqual(result["orderRevision"], 4)

    def test_route_988200_unrelated_team_member_cannot_confirm_adjustment(self):
        self.adjustment_ref.data["userId"] = "other-member-988200"
        with self.assertRaises(StructuredApiError) as raised:
            self._confirm()
        self.assertEqual(raised.exception.code, "ADJUSTMENT_BATCH_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from copy import deepcopy
from datetime import datetime, timezone

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import FullOrderAdjustmentConfirmRequest
from order_forecast.api.order_full_adjustment import (
    _store_sap_map,
    cumulative_lines_signature,
    semantic_changes_signature,
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

    def document(self, doc_id):
        return self.docs.get(doc_id, _DocRef(doc_id))


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


def _order_with_disjoint_reallocation(*, reallocated: bool):
    order = _order()
    order["stores"] = [
        {
            "storeId": "store-a",
            "storeName": "Alpha",
            "items": [
                {"sap": "41051", "quantity": 12},
                {"sap": "24521", "quantity": 36 if reallocated else 24},
            ],
        },
        {
            "storeId": "store-b",
            "storeName": "Beta",
            "items": [{"sap": "24521", "quantity": 12 if reallocated else 24}],
        },
    ]
    return order


def _batch_with_disjoint_sap():
    batch = _batch()
    working = _order_with_disjoint_reallocation(reallocated=False)
    working["stores"][0]["items"][0]["quantity"] = 24
    batch["workingCopySnapshot"] = working
    batch["workingCopySignature"] = working_copy_signature(working)
    return batch


def _semantic_batch():
    batch = _batch_with_disjoint_sap()
    batch["schemaVersion"] = 2
    batch["baseOrderRevision"] = 3
    batch["quantityChanges"] = [{
        "sap": "41051",
        "baselinePurchaseUnits": 12,
        "storeDeltas": [{"storeId": "store-a", "unitDelta": 12}],
        "emailOnlyUnitDelta": 0,
    }]
    batch["semanticSignature"] = semantic_changes_signature(
        batch["baseOrderRevision"],
        batch["quantityChanges"],
    )
    return batch


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
        self.assertEqual(raised.exception.code, "ADJUSTMENT_QUANTITY_CONFLICT")
        self.assertEqual(raised.exception.details, {"saps": ["41051"]})

    def test_route_989262_preserves_a_proven_disjoint_reallocation(self):
        merged = validate_and_merge_full_adjustment(
            current_order=_order_with_disjoint_reallocation(reallocated=True),
            batch=_batch_with_disjoint_sap(),
            accepted_saps=["41051"],
            intervening_reallocation_saps={"24521"},
        )
        stores = {store["storeId"]: store for store in merged["stores"]}
        alpha = {item["sap"]: item["quantity"] for item in stores["store-a"]["items"]}
        beta = {item["sap"]: item["quantity"] for item in stores["store-b"]["items"]}
        self.assertEqual(alpha, {"24521": 36, "41051": 24})
        self.assertEqual(beta, {"24521": 12})

    def test_route_989262_rejects_reallocation_of_an_emailed_sap(self):
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order_with_disjoint_reallocation(reallocated=True),
                batch=_batch_with_disjoint_sap(),
                accepted_saps=["41051"],
                intervening_reallocation_saps={"41051"},
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_ALLOCATION_CONFLICT")
        self.assertEqual(raised.exception.details, {"saps": ["41051"]})

    def test_route_989262_requires_server_proof_for_unrelated_store_drift(self):
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order_with_disjoint_reallocation(reallocated=True),
                batch=_batch_with_disjoint_sap(),
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_BASELINE_CHANGED")

    def test_semantic_signature_matches_the_mobile_canonical_contract(self):
        changes = [{
            "sap": "41051",
            "baselinePurchaseUnits": 36,
            "storeDeltas": [
                {"storeId": "a", "unitDelta": -12},
                {"storeId": "b", "unitDelta": 24},
            ],
            "emailOnlyUnitDelta": 0,
        }]
        self.assertEqual(
            semantic_changes_signature(2, changes),
            '{"baseOrderRevision":2,"quantityChanges":[{"baselinePurchaseUnits":36,"emailOnlyUnitDelta":0,"sap":"41051","storeDeltas":[{"storeId":"a","unitDelta":-12},{"storeId":"b","unitDelta":24}]}]}',
        )

    def test_v2_applies_store_deltas_onto_a_same_sap_reallocation(self):
        current = _order(quantity=0)
        current["stores"].append({
            "storeId": "store-c",
            "storeName": "Gamma",
            "items": [{"sap": "41051", "quantity": 12}],
        })
        batch = _semantic_batch()
        batch["workingCopySnapshot"]["stores"][0]["items"][0]["quantity"] = 24
        batch["workingCopySignature"] = working_copy_signature(batch["workingCopySnapshot"])

        merged = validate_and_merge_full_adjustment(
            current_order=current,
            batch=batch,
            accepted_saps=["41051"],
        )

        self.assertEqual(_store_sap_map(merged, "41051"), {"store-a": 12, "store-c": 12})

    def test_v2_email_only_acceptance_does_not_mutate_the_order(self):
        batch = _semantic_batch()
        batch["quantityChanges"][0]["storeDeltas"] = []
        batch["quantityChanges"][0]["emailOnlyUnitDelta"] = 12
        batch["semanticSignature"] = semantic_changes_signature(3, batch["quantityChanges"])
        current = _order(quantity=12)

        merged = validate_and_merge_full_adjustment(
            current_order=current,
            batch=batch,
            accepted_saps=["41051"],
        )

        self.assertEqual(merged, current)

    def test_v2_rejects_a_real_quantity_conflict(self):
        with self.assertRaises(StructuredApiError) as raised:
            validate_and_merge_full_adjustment(
                current_order=_order(quantity=24),
                batch=_semantic_batch(),
                accepted_saps=["41051"],
            )
        self.assertEqual(raised.exception.code, "ADJUSTMENT_QUANTITY_CONFLICT")

    def test_v2_allows_a_conflicted_line_to_be_rejected(self):
        current = _order(quantity=24)
        merged = validate_and_merge_full_adjustment(
            current_order=current,
            batch=_semantic_batch(),
            accepted_saps=[],
        )
        self.assertEqual(merged, current)

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
        self.order_audit_ref = _CollectionRef()
        self.route_adjustments_ref = _CollectionRef()
        self.payload = FullOrderAdjustmentConfirmRequest(
            adjustmentId="adjustment-988200",
            sentBatchId=self.batch["id"],
            acceptedSaps=["41051"],
        )

    def _confirm(self):
        return self.apply(
            _Transaction(),
            order_ref=self.order_ref,
            adjustment_ref=self.adjustment_ref,
            batch_ref=self.batch_ref,
            audit_ref=self.audit_ref,
            order_audit_ref=self.order_audit_ref,
            route_adjustments_ref=self.route_adjustments_ref,
            stores_ref=_CollectionRef({
                "store-a": _DocRef("store-a", {"isActive": True, "items": ["41051", "24521"]}),
                "store-b": _DocRef("store-b", {"isActive": True, "items": ["24521"]}),
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

    def test_route_989262_rebases_over_a_verified_disjoint_reallocation(self):
        self.batch = _batch_with_disjoint_sap()
        self.batch_ref.data = deepcopy(self.batch)
        self.adjustment_ref.data["lastSent"] = {
            "batchId": self.batch["id"],
            "cumulativeSignature": self.batch["cumulativeSignature"],
            "workingCopySignature": self.batch["workingCopySignature"],
        }
        self.order_ref.data = _order_with_disjoint_reallocation(reallocated=True)
        self.order_ref.data["orderRevision"] = 4
        reallocation_id = "reallocation-989262"
        self.order_audit_ref.docs["store-reallocation-989262"] = _DocRef(
            "store-reallocation-989262",
            {
                "action": "order_store_reallocated",
                "meta": {
                    "reallocationId": reallocation_id,
                    "baseOrderRevision": 3,
                    "appliedOrderRevision": 4,
                },
            },
        )
        self.route_adjustments_ref.docs[reallocation_id] = _DocRef(
            reallocation_id,
            {
                "mode": "store_reallocation",
                "sourceOrderId": self.order_ref.id,
                "storeReallocation": {
                    "baseOrderRevision": 3,
                    "appliedOrderRevision": 4,
                    "moves": [{"sap": "24521"}],
                },
            },
        )

        result = self._confirm()

        self.assertEqual(result["orderRevision"], 5)
        stores = {store["storeId"]: store for store in self.order_ref.data["stores"]}
        alpha = {item["sap"]: item["quantity"] for item in stores["store-a"]["items"]}
        beta = {item["sap"]: item["quantity"] for item in stores["store-b"]["items"]}
        self.assertEqual(alpha, {"24521": 36, "41051": 24})
        self.assertEqual(beta, {"24521": 12})
        self.assertEqual(self.adjustment_ref.data["confirmation"]["rebasedFromOrderRevision"], 3)

    def test_route_989262_fails_closed_when_revision_history_is_missing(self):
        self.order_ref.data["orderRevision"] = 4
        with self.assertRaises(StructuredApiError) as raised:
            self._confirm()
        self.assertEqual(raised.exception.code, "ADJUSTMENT_REBASE_UNSAFE")

    def test_v2_does_not_require_legacy_audit_rebase_history(self):
        self.batch = _semantic_batch()
        self.batch_ref.data = deepcopy(self.batch)
        self.adjustment_ref.data["lastSent"] = {
            "batchId": self.batch["id"],
            "cumulativeSignature": self.batch["cumulativeSignature"],
            "workingCopySignature": self.batch["workingCopySignature"],
        }
        self.order_ref.data["orderRevision"] = 4

        result = self._confirm()

        self.assertEqual(result["orderRevision"], 5)


if __name__ == "__main__":
    unittest.main()

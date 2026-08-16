from __future__ import annotations

import unittest

from order_forecast.scripts.legacy_reallocation_repair import (
    allocation_hash,
    classify_legacy_reallocation,
    route_totals,
)


def _legacy_adjustment(**overrides):
    value = {
        "id": "adjustment-988200",
        "status": "applied",
        "mode": "store_reallocation",
        "storeReallocation": {
            "moves": [{
                "sap": "41051",
                "fromStoreId": "a",
                "toStoreId": "b",
                "unitQuantity": 2,
            }],
        },
    }
    value.update(overrides)
    return value


class LegacyReallocationRepairTests(unittest.TestCase):
    def test_route_988200_classifies_only_complete_exact_evidence_as_safe(self):
        classification, reasons = classify_legacy_reallocation(
            order_data={"routeNumber": "988200", "orderRevision": 0, "stores": []},
            adjustments=[_legacy_adjustment()],
            archive_exact=True,
            receipt_exact=True,
            audit_actions=["order_finalized"],
        )
        self.assertEqual(classification, "safe_unapplied")
        self.assertEqual(reasons, [])

    def test_route_988200_refuses_ambiguous_post_finalization_evidence(self):
        classification, reasons = classify_legacy_reallocation(
            order_data={"routeNumber": "988200", "orderRevision": 0, "stores": []},
            adjustments=[_legacy_adjustment()],
            archive_exact=True,
            receipt_exact=True,
            audit_actions=["order_full_adjustment_confirmed"],
        )
        self.assertEqual(classification, "ambiguous")
        self.assertIn("post_finalization_mutation_audit_present", reasons)

    def test_allocation_hash_is_order_independent(self):
        first = {"stores": [
            {"storeId": "b", "items": [{"sap": "2", "quantity": 3}]},
            {"storeId": "a", "items": [{"sap": "1", "quantity": 2}]},
        ]}
        second = {"stores": list(reversed(first["stores"]))}
        self.assertEqual(allocation_hash(first), allocation_hash(second))

    def test_route_988200_refuses_mixed_legacy_markers(self):
        marked = _legacy_adjustment()
        marked["storeReallocation"]["appliedOrderRevision"] = 1
        classification, reasons = classify_legacy_reallocation(
            order_data={"routeNumber": "988200", "orderRevision": 0, "stores": []},
            adjustments=[marked, _legacy_adjustment(id="adjustment-988200-b")],
            archive_exact=True,
            receipt_exact=True,
            audit_actions=["order_finalized"],
        )
        self.assertEqual(classification, "ambiguous")
        self.assertIn("mixed_legacy_markers", reasons)

    def test_route_988200_route_totals_ignore_store_distribution(self):
        first = {"stores": [
            {"storeId": "a", "items": [{"sap": "41051", "quantity": 2}]},
            {"storeId": "b", "items": [{"sap": "41051", "quantity": 10}]},
        ]}
        second = {"stores": [
            {"storeId": "a", "items": [{"sap": "41051", "quantity": 8}]},
            {"storeId": "b", "items": [{"sap": "41051", "quantity": 4}]},
        ]}
        self.assertEqual(route_totals(first), route_totals(second))


if __name__ == "__main__":
    unittest.main()

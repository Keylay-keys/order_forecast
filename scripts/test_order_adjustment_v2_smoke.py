from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from .order_adjustment_v2_smoke import (
        adjustment_identity_census,
        classify_adjustment_settings,
        classify_route_number,
        classify_source_order_id,
        build_worker_audit_evidence,
        global_route_timezone_census,
        stable_hash,
    )
except ImportError:
    from order_adjustment_v2_smoke import (
        adjustment_identity_census,
        classify_adjustment_settings,
        classify_route_number,
        classify_source_order_id,
        build_worker_audit_evidence,
        global_route_timezone_census,
        stable_hash,
    )


class OrderAdjustmentV2SmokeTests(unittest.TestCase):
    def test_route_timezone_census_reports_multi_route_owner_contract(self):
        class Snapshot:
            def __init__(self, path, data):
                self.id = path.rsplit("/", 1)[-1]
                self.exists = data is not None
                self._data = data

            def to_dict(self):
                return self._data

        class Collection:
            def __init__(self, refs):
                self._refs = refs

            def list_documents(self):
                return [Snapshot(path, {}) for path in self._refs]

            def where(self, *_args):
                return self

            def limit(self, _limit):
                return self

            def stream(self):
                return []

        class Document:
            def __init__(self, path, documents):
                self.path = path
                self._documents = documents

            def get(self):
                return Snapshot(self.path, self._documents.get(self.path))

        class Db:
            documents = {
                "routes/100": {"ownerUid": "owner-1"},
                "routes/200": {"ownerUid": "owner-1"},
                "routeEntitlements/100": {"ownerUid": "owner-1"},
                "routeEntitlements/200": {"ownerUid": "owner-1"},
                "routeNumbers/100": {"ownerUid": "owner-1"},
                "routeNumbers/200": {"ownerUid": "owner-1"},
                "users/owner-1": {"profile": {"timezone": "America/Denver"}},
            }

            def collection(self, path):
                if path in {"routes", "routeEntitlements", "routeNumbers"}:
                    return Collection([f"{path}/100", f"{path}/200"])
                return Collection([])

            def document(self, path):
                return Document(path, self.documents)

        census = global_route_timezone_census(Db())
        self.assertTrue(census["ready"])
        self.assertEqual(census["routeCount"], 2)
        self.assertEqual(census["multiRouteOwners"], [{
            "ownerUid": "owner-1",
            "routes": ["100", "200"],
            "timezone": "America/Denver",
        }])

        Db.documents["routes/100"]["userId"] = "owner-2"
        conflicted = global_route_timezone_census(Db())
        self.assertFalse(conflicted["ready"])
        self.assertIn("100", conflicted["ownerConflictRoutes"])
        del Db.documents["routes/100"]["userId"]

    def test_shared_contract_and_evidence_schema_are_loadable(self):
        root = Path(__file__).resolve().parents[2]
        contract = json.loads(
            (root / "contracts/order-adjustment-scheduled-order-day-v2.json").read_text()
        )
        schema = json.loads(
            (root / "contracts/order-adjustment-v2-evidence.schema.json").read_text()
        )
        self.assertEqual(contract["schemaVersion"], 1)
        self.assertEqual(
            [case["name"] for case in contract["cases"]],
            [
                "route-989262-tuesday-six-day",
                "spring-forward-adjustment-day",
                "fall-back-ambiguous-cutoff-compatible-earlier",
                "spring-forward-nonexistent-cutoff-compatible-later",
            ],
        )
        self.assertEqual(contract["cases"][0]["expected"]["cutoffAtMs"], 1787763600000)
        self.assertEqual(schema["properties"]["schemaVersion"]["const"], 1)

    def test_source_identity_classes_match_rules_contract(self):
        self.assertEqual(classify_source_order_id({}), "missing")
        self.assertEqual(classify_source_order_id({"sourceOrderId": None}), "null")
        self.assertEqual(classify_source_order_id({"sourceOrderId": ""}), "empty")
        self.assertEqual(classify_source_order_id({"sourceOrderId": "  "}), "whitespace")
        self.assertEqual(classify_source_order_id({"sourceOrderId": "order-988200-1"}), "canonical")
        self.assertEqual(classify_source_order_id({"sourceOrderId": " order-988200-1"}), "malformed")
        self.assertEqual(classify_source_order_id({"sourceOrderId": "order/988200"}), "malformed")
        self.assertEqual(classify_source_order_id({"sourceOrderId": 123}), "malformed")

    def test_route_number_type_is_not_coerced(self):
        self.assertEqual(classify_route_number({}), "missing")
        self.assertEqual(classify_route_number({"routeNumber": None}), "null")
        self.assertEqual(classify_route_number({"routeNumber": "988200"}), "string")
        self.assertEqual(classify_route_number({"routeNumber": 988200}), "numeric")

    def test_identity_census_preserves_exact_paths_for_disposition(self):
        census = adjustment_identity_census(
            [
                ("routes/988200/orderAdjustments/a", {"sourceOrderId": None, "routeNumber": "988200"}),
                ("routes/988200/orderAdjustments/b", {"sourceOrderId": " bad", "routeNumber": 988200}),
            ]
        )
        self.assertEqual(census["sourceOrderId"]["null"], 1)
        self.assertEqual(census["sourceOrderId"]["malformed"], 1)
        self.assertEqual(census["malformedPaths"], ["routes/988200/orderAdjustments/b"])
        self.assertEqual(census["numericRoutePaths"], ["routes/988200/orderAdjustments/b"])

    def test_cutoff_gate_includes_reminders_disabled_users(self):
        result = classify_adjustment_settings(
            {"customerServiceEmail": "cs@example.com", "reminderEnabled": False}
        )
        self.assertTrue(result["requiresCutoffRemediation"])
        self.assertFalse(result["cutoffValid"])

    def test_cutoff_and_lead_classification(self):
        result = classify_adjustment_settings(
            {
                "customerServiceEmail": "cs@example.com",
                "reminderEnabled": True,
                "cutoffTimeLocal": "11:00",
                "reminderLeadMinutes": 15,
            }
        )
        self.assertTrue(result["cutoffValid"])
        self.assertTrue(result["leadValid"])
        self.assertFalse(result["requiresCutoffRemediation"])

    def test_hash_is_key_order_stable(self):
        self.assertEqual(stable_hash({"a": 1, "b": 2}), stable_hash({"b": 2, "a": 1}))

    def test_worker_gate_does_not_pass_vacuously_without_v2_rows(self):
        state = {"hash": "unchanged"}
        summary = {
            "dryRun": True,
            "routeNumber": "988200",
            "remainingDueIds": [],
            "results": [{"classification": "v1", "stamp": None}],
        }
        module = build_worker_audit_evidence.__module__
        with (
            patch(f"{module}.capture_route_state", side_effect=[state, state]),
            patch(f"{module}.audit_once", return_value=summary),
        ):
            evidence = build_worker_audit_evidence(
                object(), route="988200", run_id="test-no-v2"
            )

        stamp_assertion = next(
            item for item in evidence["assertions"]
            if item["name"] == "worker_v2_rows_expose_stamp_identity"
        )
        self.assertFalse(stamp_assertion["passed"])
        self.assertFalse(evidence["ready"])
        self.assertIn("worker_v2_rows_expose_stamp_identity", evidence["stopReasons"])

    def test_worker_gate_passes_with_explicit_v2_stamp_identity(self):
        state = {"hash": "unchanged"}
        summary = {
            "dryRun": True,
            "routeNumber": "988200",
            "remainingDueIds": [],
            "results": [{
                "classification": "v2",
                "stamp": {
                    "version": 2,
                    "date": "2026-08-25",
                    "timezone": "America/Denver",
                },
            }],
        }
        module = build_worker_audit_evidence.__module__
        with (
            patch(f"{module}.capture_route_state", side_effect=[state, state]),
            patch(f"{module}.audit_once", return_value=summary),
        ):
            evidence = build_worker_audit_evidence(
                object(), route="988200", run_id="test-v2"
            )

        self.assertTrue(evidence["ready"])


if __name__ == "__main__":
    unittest.main()

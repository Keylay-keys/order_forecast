from __future__ import annotations

import json
import unittest
from pathlib import Path

from order_forecast.api.fixture_identity import classify_fixture_document


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "order_forecast/contracts/pcf/fixture-identity-v1.json"


class FixtureIdentityTest(unittest.TestCase):
    def test_contract_vectors(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        for vector in contract["testVectors"]:
            with self.subTest(vector=vector["name"]):
                self.assertEqual(
                    classify_fixture_document(vector["document"]), vector["expected"]
                )

    def test_production_fixture_writes_are_fail_closed(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        execution = contract["execution"]
        self.assertEqual(execution["dedicatedProjectId"], "routespark-pcf-test-20260820")
        self.assertEqual(execution["dedicatedProjectNumber"], "901720710978")
        self.assertTrue(execution["fixtureWritesAllowed"])
        self.assertTrue(execution["fixtureWritesAllowedOnlyWhenRuntimeProjectIdMatchesDedicatedProjectId"])
        self.assertFalse(execution["billingEnabled"])
        self.assertFalse(execution["billingMutationAuthorized"])
        self.assertEqual(
            execution["cloudFunctionsDeployRollbackProof"],
            "blocked_pending_separate_billing_authority",
        )
        self.assertFalse(execution["productionRoute988200FixtureWritesAllowed"])
        self.assertFalse(execution["productionRoute989262FixtureWritesAllowed"])
        self.assertEqual(execution["route900000Use"], "never_fixture_or_canary_observe_only")

    def test_reserved_names_do_not_claim_live_state(self):
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        reserved = contract["reservedFixture"]
        self.assertEqual(reserved["route"], "988200")
        self.assertIn("names only", reserved["reservationMeaning"])
        self.assertIn("create-only", reserved["createPolicy"])


if __name__ == "__main__":
    unittest.main()

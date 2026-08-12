import unittest
from unittest.mock import patch

from forecast_contract import (
    ForecastContractError,
    artifact_semantic_payload,
    build_order_snapshot,
    key_fingerprint,
    stable_fingerprint,
    validate_ready_artifact,
    load_authority_generation_state,
)
from models import Product, StoreConfig


class ForecastContractTest(unittest.TestCase):
    def artifact(self):
        items = [
            {"storeId": "2", "sap": "B", "recommendedUnits": 0, "source": "suppressed"},
            {"storeId": "1", "sap": "A", "recommendedUnits": 5.0, "source": "last_order"},
        ]
        keys = {("1", "A"), ("2", "B")}
        artifact = {
            "schemaVersion": 2,
            "state": "ready",
            "forecastId": "forecast-1",
            "generationMode": "last_order",
            "routeNumber": "988200",
            "deliveryDate": "2026-08-13",
            "scheduleKey": "tuesday",
            "generationInputFingerprint": "input-1",
            "eligibility": {
                "activeCarryItemCount": 2,
                "emittedItemCount": 2,
                "zeroItemCount": 1,
                "activeCarryFingerprint": key_fingerprint(keys),
            },
            "items": items,
        }
        artifact["artifactFingerprint"] = stable_fingerprint(artifact_semantic_payload(artifact))
        return artifact, keys

    def test_dense_mixed_source_last_order_artifact_retains_explicit_mode(self):
        artifact, keys = self.artifact()
        rows = validate_ready_artifact(
            artifact,
            route_number="988200",
            delivery_date="2026-08-13",
            schedule_key="tuesday",
            active_carry_keys=keys,
        )
        self.assertEqual(artifact["generationMode"], "last_order")
        self.assertEqual([row["recommendedUnits"] for row in rows], [5, 0])

    def test_order_only_item_gets_explicit_zero_without_changing_active_artifact(self):
        artifact, keys = self.artifact()
        rows = validate_ready_artifact(
            artifact,
            route_number="988200",
            delivery_date="2026-08-13",
            schedule_key="tuesday",
        )
        snapshot, fingerprint = build_order_snapshot(rows, keys, {("3", "C")})
        self.assertEqual(snapshot[-1], {
            "storeId": "3", "sap": "C", "recommendedUnits": 0, "source": "order_only_zero"
        })
        self.assertEqual(fingerprint, key_fingerprint(keys | {("3", "C")}))

    def test_rejects_missing_zero_row_and_invalid_units(self):
        artifact, keys = self.artifact()
        artifact["items"] = artifact["items"][:1]
        with self.assertRaises(ForecastContractError):
            validate_ready_artifact(
                artifact,
                route_number="988200",
                delivery_date="2026-08-13",
                schedule_key="tuesday",
                active_carry_keys=keys,
            )

    def test_firebase_schedule_authority_rejects_obsolete_draft_schedule(self):
        with patch(
            "firebase_loader.load_master_catalog",
            return_value=[Product(sap="100", name="One", case_pack=12)],
        ), patch(
            "firebase_loader.load_store_configs",
            return_value=[StoreConfig(
                store_id="store-1", store_name="One",
                delivery_days=["thursday"], active_saps=["100"],
            )],
        ), patch(
            "schedule_utils.get_order_cycles_from_firebase",
            return_value=[{"orderDay": 7, "deliveryDay": 4, "loadDay": 3}],
        ):
            with self.assertRaisesRegex(ForecastContractError, "schedule_not_active:monday"):
                load_authority_generation_state(
                    object(), "988200", "2026-08-13", "monday"
                )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from order_forecast.api import pcf_event_contract as contract


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/event-contract-v1.json"
SCHEMA_PATH = ROOT / "order_forecast/contracts/pcf/projection-event-v1.schema.json"
UPLOAD_SCHEMA_PATH = ROOT / "order_forecast/contracts/pcf/pcf-upload-settled-v1.schema.json"


def _ocr_context(*containers: str, loading_date: str = "08/20/2026") -> dict:
    return {
        "validatedLoadingDate": loading_date,
        "containers": [
            {
                "containerCode": container,
                "loadingDate": loading_date,
                "ssccBypassUsed": False,
                "userVerifiedContainerCode": True,
                "containerCodeSource": "",
                "realSsccRecovered": False,
                "ssccRecoveryStatus": "",
            }
            for container in containers
        ],
    }


def _container_event() -> dict:
    update_time = "2026-08-20T12:00:00.123456789Z"
    return {
        "schemaVersion": 1,
        "sourceEventId": "firestore-event-0001",
        "eventType": "projection.pcf_container.changed",
        "mutationKind": "upsert",
        "occurredAt": "2026-08-20T12:00:01Z",
        "sourcePath": "routes/989262/pcfs/1806242572/containers/C01",
        "sourceRevision": contract.firestore_revision(update_time, "upsert"),
        "entity": {
            "route": "989262",
            "delivery": "1806242572",
            "container": "C01",
        },
        "automationExcluded": False,
        "fixture": None,
        "payload": {
            "active": True,
            "loadingDate": "8/20/2026",
            "pageNumbers": [2, 1],
            "items": [
                {
                    "pageNumber": 2,
                    "itemPosition": 0,
                    "product": "200",
                    "description": " Second ",
                    "expiryDate": "08/29/2026",
                    "visibleOnList": True,
                    "isLowQuantity": False,
                    "isShortCoded": False,
                    "guaranteed": None,
                },
                {
                    "pageNumber": 1,
                    "itemPosition": 0,
                    "product": "100",
                    "description": "First",
                    "expiryDate": "2026-08-28",
                    "visibleOnList": True,
                    "isLowQuantity": True,
                    "isShortCoded": True,
                    "guaranteed": {
                        "isGuaranteed": True,
                        "guaranteeExpiresAt": "2026-09-01T23:59:59.000Z",
                    },
                },
            ],
        },
    }


class PcfEventContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frozen = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        cls.upload_schema = json.loads(UPLOAD_SCHEMA_PATH.read_text(encoding="utf-8"))

    def test_frozen_contract_and_schema_match_runtime_versions_and_event_types(self):
        self.assertEqual(self.frozen["envelope"]["schemaVersion"], contract.EVENT_SCHEMA_VERSION)
        self.assertEqual(
            self.frozen["businessHash"]["version"], contract.BUSINESS_HASH_VERSION
        )
        self.assertEqual(
            self.frozen["pcfWorkInputHash"]["version"], contract.WORK_INPUT_HASH_VERSION
        )
        self.assertEqual(set(self.frozen["eventTypes"]), contract.EVENT_TYPES)
        self.assertEqual(
            set(self.schema["properties"]["eventType"]["enum"]), contract.EVENT_TYPES
        )
        self.assertFalse(self.frozen["scope"]["productionResourcesExist"])
        self.assertFalse(self.frozen["scope"]["effectsDefaultEnabled"])

    def test_firestore_timestamp_is_nanosecond_preserving_and_delete_wins_tie(self):
        upsert = contract.firestore_revision("2026-08-20T12:00:00.123Z", "upsert")
        deletion = contract.firestore_revision("2026-08-20T12:00:00.123000000Z", "delete")
        self.assertEqual(upsert, "fsut1|2026-08-20T12:00:00.123000000Z|0")
        self.assertGreater(contract.compare_firestore_revisions(deletion, upsert), 0)

    def test_recreated_document_wins_prior_tombstone(self):
        deletion = contract.firestore_revision("2026-08-20T12:00:00.999999999Z", "delete")
        recreated = contract.firestore_revision("2026-08-20T12:00:01Z", "upsert")
        self.assertGreater(contract.compare_firestore_revisions(recreated, deletion), 0)

    def test_out_of_order_and_duplicate_projection_decisions_are_idempotent(self):
        old_revision = contract.firestore_revision("2026-08-20T12:00:00Z", "upsert")
        new_revision = contract.firestore_revision("2026-08-20T12:01:00Z", "upsert")
        old_hash = contract.business_hash({"value": "old"})
        new_hash = contract.business_hash({"value": "new"})
        self.assertEqual(
            contract.projection_decision(
                current_revision=new_revision,
                current_business_hash=new_hash,
                current_mutation_kind="upsert",
                incoming_revision=old_revision,
                incoming_business_hash=old_hash,
                incoming_mutation_kind="upsert",
            ),
            "stale_noop",
        )
        self.assertEqual(
            contract.projection_decision(
                current_revision=new_revision,
                current_business_hash=new_hash,
                current_mutation_kind="upsert",
                incoming_revision=new_revision,
                incoming_business_hash=new_hash,
                incoming_mutation_kind="upsert",
            ),
            "duplicate_noop",
        )

    def test_effect_only_generation_advances_watermark_without_replanning(self):
        first = contract.firestore_revision("2026-08-20T12:00:00Z", "upsert")
        effect = contract.firestore_revision("2026-08-20T12:00:02Z", "upsert")
        stable_hash = contract.business_hash({"product": "100", "expiry": "2026-08-29"})
        self.assertEqual(
            contract.projection_decision(
                current_revision=first,
                current_business_hash=stable_hash,
                current_mutation_kind="upsert",
                incoming_revision=effect,
                incoming_business_hash=stable_hash,
                incoming_mutation_kind="upsert",
            ),
            "advance_watermark_only",
        )

    def test_aba_revert_after_intermediate_state_is_applied(self):
        intermediate = contract.firestore_revision("2026-08-20T12:00:01Z", "upsert")
        reverted = contract.firestore_revision("2026-08-20T12:00:02Z", "upsert")
        hash_a = contract.business_hash({"value": "A"})
        hash_b = contract.business_hash({"value": "B"})
        self.assertEqual(
            contract.projection_decision(
                current_revision=intermediate,
                current_business_hash=hash_b,
                current_mutation_kind="upsert",
                incoming_revision=reverted,
                incoming_business_hash=hash_a,
                incoming_mutation_kind="upsert",
            ),
            "apply",
        )

    def test_equal_revision_with_different_content_is_rejected(self):
        revision = contract.firestore_revision("2026-08-20T12:00:00Z", "upsert")
        self.assertEqual(
            contract.projection_decision(
                current_revision=revision,
                current_business_hash=contract.business_hash({"value": "A"}),
                current_mutation_kind="upsert",
                incoming_revision=revision,
                incoming_business_hash=contract.business_hash({"value": "B"}),
                incoming_mutation_kind="upsert",
            ),
            "revision_conflict_reject",
        )

    def test_container_hash_normalizes_dates_order_and_ignores_transport_metadata(self):
        first = _container_event()
        second = copy.deepcopy(first)
        second["sourceEventId"] = "firestore-event-9999"
        second["occurredAt"] = "2026-08-20T13:00:00Z"
        second["sourceRevision"] = contract.firestore_revision(
            "2026-08-20T13:00:00Z", "upsert"
        )
        second["payload"]["items"].reverse()
        second["payload"]["loadingDate"] = "2026-08-20"
        second["payload"]["items"][0]["expiryDate"] = "08/28/2026"
        self.assertEqual(
            contract.projection_event_business_hash(first),
            contract.projection_event_business_hash(second),
        )

    def test_delete_is_a_first_class_tombstone_hash(self):
        upsert = _container_event()
        deletion = copy.deepcopy(upsert)
        deletion["sourceEventId"] = "firestore-event-delete"
        deletion["mutationKind"] = "delete"
        deletion["sourceRevision"] = contract.firestore_revision(
            "2026-08-20T12:00:00.123456789Z", "delete"
        )
        deletion["payload"] = None
        self.assertNotEqual(
            contract.projection_event_business_hash(upsert),
            contract.projection_event_business_hash(deletion),
        )

    def test_path_entity_schema_and_fixture_rules_fail_closed(self):
        event = _container_event()
        event["sourcePath"] = "routes/988200/pcfs/1806242572/containers/C01"
        with self.assertRaises(contract.EventContractError):
            contract.validate_projection_event(event)

        event = _container_event()
        event["automationExcluded"] = True
        with self.assertRaises(contract.EventContractError):
            contract.validate_projection_event(event)
        event["fixture"] = {"identity": "fixture-01", "generation": "7"}
        contract.validate_projection_event(event)

    def test_unknown_schema_and_event_type_are_deterministic_rejections(self):
        event = _container_event()
        event["schemaVersion"] = 2
        with self.assertRaisesRegex(contract.EventContractError, "unsupported schemaVersion"):
            contract.validate_projection_event(event)
        event = _container_event()
        event["eventType"] = "projection.unknown.changed"
        with self.assertRaisesRegex(contract.EventContractError, "unsupported eventType"):
            contract.validate_projection_event(event)

    def test_recipient_payload_contains_fingerprint_not_raw_tokens(self):
        update_time = "2026-08-20T12:00:00Z"
        event = {
            "schemaVersion": 1,
            "sourceEventId": "firestore-user-event-01",
            "eventType": "projection.notification_recipient.changed",
            "mutationKind": "upsert",
            "occurredAt": update_time,
            "sourcePath": "users/user-1",
            "sourceRevision": contract.firestore_revision(update_time, "upsert"),
            "entity": {"userId": "user-1"},
            "automationExcluded": False,
            "fixture": None,
            "payload": {
                "enabled": True,
                "expirationAlertDays": 1,
                "deliveryMinuteLocal": 510,
                "timezone": "America/Denver",
                "routeNumbers": ["989262", "988200"],
                "hasPushToken": True,
                "tokenSetHash": "a" * 64,
            },
        }
        normalized = contract.validate_projection_event(event)
        self.assertEqual(normalized["payload"]["routeNumbers"], ["988200", "989262"])
        event["payload"]["fcmTokens"] = ["ExponentPushToken[secret]"]
        with self.assertRaises(contract.EventContractError):
            contract.validate_projection_event(event)

    def test_retention_policy_binds_canonical_control_and_tombstone(self):
        update_time = "2026-08-20T12:00:00Z"
        event = {
            "schemaVersion": 1,
            "sourceEventId": "firestore-retention-event-01",
            "eventType": "projection.pcf_retention.changed",
            "mutationKind": "upsert",
            "occurredAt": update_time,
            "sourcePath": "pcfProjectionRoutePolicies/989262",
            "sourceRevision": contract.firestore_revision(update_time, "upsert"),
            "entity": {"route": "989262"},
            "automationExcluded": False,
            "fixture": None,
            "payload": {"retentionDays": 6, "ownerUserId": "owner-989262"},
        }
        normalized = contract.validate_projection_event(event)
        self.assertEqual(normalized["payload"]["retentionDays"], 6)

        mismatched = copy.deepcopy(event)
        mismatched["sourcePath"] = "routes/989262"
        with self.assertRaisesRegex(contract.EventContractError, "sourcePath"):
            contract.validate_projection_event(mismatched)

        tombstone = copy.deepcopy(event)
        tombstone["sourceEventId"] = "firestore-retention-delete"
        tombstone["mutationKind"] = "delete"
        tombstone["sourceRevision"] = contract.firestore_revision(update_time, "delete")
        tombstone["payload"] = None
        self.assertIsNone(contract.validate_projection_event(tombstone)["payload"])

    def test_expiring_key_collapses_legacy_date_aliases(self):
        first = contract.expiring_semantic_key("user-1", "8/19/2026", 1)
        second = contract.expiring_semantic_key("user-1", "08/19/2026", 1)
        third = contract.expiring_semantic_key("user-1", "2026-08-19", 1)
        self.assertEqual(first, second)
        self.assertEqual(second, third)
        self.assertNotEqual(first, contract.expiring_semantic_key("user-1", "2026-08-19", 0))

    def test_lifecycle_key_uses_monotonic_revision_not_business_hash(self):
        first = contract.lifecycle_semantic_key(
            route="989262",
            delivery="1806242572",
            container="C01",
            transition_kind="archive_delivery",
            effective_business_date="2026-08-20",
            source_revision=contract.firestore_revision("2026-08-20T12:00:00Z", "upsert"),
        )
        second = contract.lifecycle_semantic_key(
            route="989262",
            delivery="1806242572",
            container="C01",
            transition_kind="archive_delivery",
            effective_business_date="2026-08-20",
            source_revision=contract.firestore_revision("2026-08-20T12:01:00Z", "upsert"),
        )
        self.assertNotEqual(first, second)
        self.assertNotIn("businessHash", first)

    def test_work_input_hash_is_order_independent_but_generation_bound(self):
        objects = [
            {
                "containerCode": "003400001234567891",
                "uploadPageNumber": 2,
                "objectPath": "routes/989262/unprocessed/1806242572_003400001234567891_page_2.jpg",
                "generation": "20",
                "size": 200,
                "md5Hash": "MDEyMzQ1Njc4OWFiY2RlZg==",
            },
            {
                "containerCode": "003400001234567890",
                "uploadPageNumber": 1,
                "objectPath": "routes/989262/unprocessed/1806242572_003400001234567890_page_1.jpg",
                "generation": "10",
                "size": 100,
                "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
            },
        ]
        first = contract.pcf_work_input_hash(
            route="989262",
            delivery="1806242572",
            coverage_scope="uploaded_manifest",
            objects=objects,
            ocr_context=_ocr_context(
                "003400001234567890", "003400001234567891"
            ),
        )
        second = contract.pcf_work_input_hash(
            route="989262",
            delivery="1806242572",
            coverage_scope="uploaded_manifest",
            objects=list(reversed(objects)),
            ocr_context=_ocr_context(
                "003400001234567891", "003400001234567890"
            ),
        )
        replacement = copy.deepcopy(objects)
        replacement[0]["generation"] = "21"
        third = contract.pcf_work_input_hash(
            route="989262",
            delivery="1806242572",
            coverage_scope="uploaded_manifest",
            objects=replacement,
            ocr_context=_ocr_context(
                "003400001234567890", "003400001234567891"
            ),
        )
        corrected_date = contract.pcf_work_input_hash(
            route="989262",
            delivery="1806242572",
            coverage_scope="uploaded_manifest",
            objects=objects,
            ocr_context=_ocr_context(
                "003400001234567890",
                "003400001234567891",
                loading_date="08/21/2026",
            ),
        )
        self.assertRegex(first, r"^[0-9a-f]{64}$")
        self.assertEqual(first, second)
        self.assertNotEqual(first, third)
        self.assertNotEqual(first, corrected_date)

    def test_float_and_out_of_scope_object_paths_are_rejected(self):
        with self.assertRaises(contract.EventContractError):
            contract.business_hash({"unsafe": 1.25})
        with self.assertRaises(contract.EventContractError):
            contract.pcf_work_input_hash(
                route="989262",
                delivery="1806242572",
                coverage_scope="uploaded_manifest",
                objects=[
                    {
                        "containerCode": "003400001234567890",
                        "uploadPageNumber": 1,
                        "objectPath": "routes/988200/unprocessed/1806242572_003400001234567890_page_1.jpg",
                        "generation": "1",
                        "size": 100,
                        "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
                    }
                ],
                ocr_context=_ocr_context("003400001234567890"),
            )

    def test_upload_event_validates_counts_scope_and_computes_cluster_hash(self):
        update_time = "2026-08-20T12:00:00Z"
        event = {
            "eventId": "firestore-upload-event-01",
            "eventType": "pcf.upload.settled",
            "schemaVersion": 1,
            "route": "989262",
            "delivery": "1806242572",
            "occurredAt": "2026-08-20T12:00:01Z",
            "sourceRevision": contract.firestore_revision(update_time, "upsert"),
            "quietPeriodElapsed": True,
            "coverageScope": "uploaded_manifest",
            "manifest": {
                "containerCount": 1,
                "pageCount": 1,
                "objects": [
                    {
                        "containerCode": "003400001234567890",
                        "uploadPageNumber": 1,
                        "objectPath": "routes/989262/unprocessed/1806242572_003400001234567890_page_1.jpg",
                        "generation": "10",
                        "size": 4853371,
                        "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
                    }
                ],
            },
            "ocrContext": _ocr_context("003400001234567890"),
            "sourceQueuePath": "pcf_processing_queue/989262/queue/1806242572",
            "automationExcluded": False,
            "fixture": None,
        }
        validated = contract.validate_pcf_upload_event(event)
        self.assertEqual(validated["inputHashVersion"], "pcf-work-input-v1")
        self.assertRegex(validated["inputHash"], r"^[0-9a-f]{64}$")

        wrong_scope = copy.deepcopy(event)
        wrong_scope["manifest"]["objects"][0]["objectPath"] = (
            "routes/989262/unprocessed/other_C01_page_1.jpg"
        )
        with self.assertRaises(contract.EventContractError):
            contract.validate_pcf_upload_event(wrong_scope)

        wrong_count = copy.deepcopy(event)
        wrong_count["manifest"]["pageCount"] = 2
        with self.assertRaises(contract.EventContractError):
            contract.validate_pcf_upload_event(wrong_count)

        conflicting_date = copy.deepcopy(event)
        conflicting_date["ocrContext"]["containers"][0]["loadingDate"] = "08/21/2026"
        with self.assertRaises(contract.EventContractError):
            contract.validate_pcf_upload_event(conflicting_date)

    def test_upload_event_rejects_duplicate_logical_pages_and_oversized_objects(self):
        base_object = {
            "containerCode": "003400001234567890",
            "uploadPageNumber": 1,
            "objectPath": "routes/989262/unprocessed/1806242572_003400001234567890_page_1.jpg",
            "generation": "10",
            "size": 100,
            "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
        }
        duplicate = dict(base_object)
        duplicate["generation"] = "11"
        with self.assertRaises(contract.EventContractError):
            contract.pcf_work_input_hash(
                route="989262",
                delivery="1806242572",
                coverage_scope="uploaded_manifest",
                objects=[base_object, duplicate],
                ocr_context=_ocr_context("003400001234567890"),
            )
        oversized = dict(base_object)
        oversized["size"] = 20_000_001
        with self.assertRaises(contract.EventContractError):
            contract.pcf_work_input_hash(
                route="989262",
                delivery="1806242572",
                coverage_scope="uploaded_manifest",
                objects=[oversized],
                ocr_context=_ocr_context("003400001234567890"),
            )

    def test_contract_forbids_route_wide_fixture_exclusion_and_secret_material(self):
        self.assertFalse(self.frozen["fixtures"]["routeWideExclusion"])
        self.assertIn("real entities", self.frozen["fixtures"]["route988200"])
        self.assertIn("never a fixture", self.frozen["fixtures"]["route900000"])
        source = (
            CONTRACT_PATH.read_text(encoding="utf-8")
            + SCHEMA_PATH.read_text(encoding="utf-8")
            + UPLOAD_SCHEMA_PATH.read_text(encoding="utf-8")
        )
        for forbidden in (
            "BEGIN PRIVATE KEY",
            "STRIPE_SECRET_KEY",
            "STRIPE_WEBHOOK_SECRET",
            "ExponentPushToken[",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from google.api_core.exceptions import PreconditionFailed

from order_forecast.scripts.pcf_lifecycle_due import ClaimedLifecycleWork
from order_forecast.scripts.pcf_lifecycle_effect import (
    ArchiveContractError,
    ExactStorageObject,
)
from order_forecast.scripts.pcf_lifecycle_runtime import (
    FirebaseExactArchiveAdapter,
    LifecycleRuntimeControl,
)


NOW = datetime(2026, 8, 26, 18, 0, tzinfo=timezone.utc)


def claim() -> ClaimedLifecycleWork:
    return ClaimedLifecycleWork(
        id=7,
        semantic_key="semantic-key-988200",
        route_number="988200",
        delivery_id="1806231248",
        container_id="C01",
        effective_business_date=NOW.date(),
        due_at=NOW - timedelta(hours=1),
        deadline_at=NOW + timedelta(hours=1),
        source_revision="fsut1|2026-08-26T17:00:00.000000000Z|0",
        source_business_hash="a" * 64,
        decision_watermark={},
        claim_token="secret-token",
        attempt_count=1,
    )


def control_payload(*, enabled=True, expires_at=None, route="988200", delivery="1806231248"):
    return {
        "schemaVersion": 1,
        "effectsEnabled": enabled,
        "allProvenWork": False,
        "fences": [] if not enabled else [
            {
                "semanticKey": "semantic-key-988200",
                "routeNumber": route,
                "deliveryId": delivery,
                "expiresAt": (expires_at or (NOW + timedelta(minutes=30))).isoformat(),
                "evidenceSha256": "b" * 64,
            }
        ],
    }


class LifecycleRuntimeControlTests(TestCase):
    def test_disabled_empty_control_is_valid_and_admits_nothing(self):
        control = LifecycleRuntimeControl.parse(control_payload(enabled=False))
        self.assertFalse(control.admits(claim(), NOW))
        self.assertEqual(control.allowed_semantic_keys(NOW), ())

    def test_enabled_empty_control_fails_closed(self):
        payload = control_payload(enabled=False)
        payload["effectsEnabled"] = True
        with self.assertRaisesRegex(ArchiveContractError, "require an exact fence"):
            LifecycleRuntimeControl.parse(payload)

    def test_only_exact_live_identity_is_admitted(self):
        control = LifecycleRuntimeControl.parse(control_payload())
        self.assertTrue(control.admits(claim(), NOW))
        self.assertFalse(control.admits(claim(), NOW + timedelta(minutes=31)))
        self.assertFalse(
            LifecycleRuntimeControl.parse(
                control_payload(route="989262")
            ).admits(claim(), NOW)
        )
        self.assertFalse(
            LifecycleRuntimeControl.parse(
                control_payload(expires_at=NOW + timedelta(hours=2))
            ).admits(claim(), NOW)
        )

    def test_unknown_keys_and_bad_evidence_digest_are_rejected(self):
        payload = control_payload()
        payload["surprise"] = True
        with self.assertRaisesRegex(ArchiveContractError, "control keys"):
            LifecycleRuntimeControl.parse(payload)
        payload = control_payload()
        payload["fences"][0]["evidenceSha256"] = "not-a-digest"
        with self.assertRaisesRegex(ArchiveContractError, "evidence digest"):
            LifecycleRuntimeControl.parse(payload)

    def test_adapter_rereads_control_at_each_mutation_fence(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "control.json"
            path.write_text(json.dumps(control_payload()), encoding="utf-8")
            adapter = FirebaseExactArchiveAdapter(
                firestore_client=None,
                bucket=None,
                control_path=path,
                claim_authority=None,
                projection_loader=None,
                clock=lambda: NOW,
            )
            self.assertTrue(adapter.assert_exact_mutation_fence(claim()))
            path.write_text(json.dumps(control_payload(enabled=False)), encoding="utf-8")
            self.assertFalse(adapter.assert_exact_mutation_fence(claim()))


class FakeBlob:
    def __init__(self, name, *, size=20, md5_hash="md5-value", generation="99"):
        self.name = name
        self.size = size
        self.md5_hash = md5_hash
        self.generation = generation

    def reload(self):
        return None


class FakeBucket:
    def __init__(self):
        self.copy_kwargs = None
        self.destination = FakeBlob(
            "routes/988200/archivedPCFs/1806231248/C01/page_1.jpg"
        )
        self.fail_precondition = False

    def blob(self, name, generation=None):
        return FakeBlob(name, generation=str(generation))

    def copy_blob(self, source, destination_bucket, destination_name, **kwargs):
        self.copy_kwargs = kwargs
        if self.fail_precondition:
            raise PreconditionFailed("exists")
        return self.destination

    def get_blob(self, name):
        return self.destination


class StorageGenerationContractTests(TestCase):
    def setUp(self):
        self.bucket = FakeBucket()
        self.adapter = FirebaseExactArchiveAdapter(
            firestore_client=None,
            bucket=self.bucket,
            control_path=Path("unused"),
            claim_authority=None,
            projection_loader=None,
        )
        self.obj = ExactStorageObject(
            source_path="routes/988200/pcfs/1806231248/C01/page_1.jpg",
            archive_path="routes/988200/archivedPCFs/1806231248/C01/page_1.jpg",
            generation="99",
            size=20,
            md5_hash="md5-value",
        )

    def test_copy_is_source_generation_bound_and_destination_create_only(self):
        self.adapter.copy_exact_object(self.obj)
        self.assertEqual(self.bucket.copy_kwargs["source_generation"], 99)
        self.assertEqual(self.bucket.copy_kwargs["if_source_generation_match"], 99)
        self.assertEqual(self.bucket.copy_kwargs["if_generation_match"], 0)

    def test_existing_identical_destination_is_idempotent(self):
        self.bucket.fail_precondition = True
        self.adapter.copy_exact_object(self.obj)

    def test_existing_different_destination_is_a_hard_conflict(self):
        self.bucket.fail_precondition = True
        self.bucket.destination.size = 21
        with self.assertRaisesRegex(ArchiveContractError, "content conflicts"):
            self.adapter.copy_exact_object(self.obj)

    def test_suffix_change_is_rejected_before_copy(self):
        bad = ExactStorageObject(
            source_path=self.obj.source_path,
            archive_path="routes/988200/archivedPCFs/1806231248/C01/renamed.jpg",
            generation=self.obj.generation,
            size=self.obj.size,
            md5_hash=self.obj.md5_hash,
        )
        with self.assertRaisesRegex(ArchiveContractError, "mapping"):
            self.adapter.copy_exact_object(bad)
        self.assertIsNone(self.bucket.copy_kwargs)

from __future__ import annotations

import dataclasses
import inspect
import unittest
from datetime import datetime, timedelta, timezone

from order_forecast.api.pcf_event_contract import business_hash, firestore_revision
from order_forecast.scripts.pcf_lifecycle_due import (
    ClaimedLifecycleWork,
    ProjectedContainer,
    build_cleanup_plan,
)
from order_forecast.scripts.pcf_lifecycle_effect import (
    ArchiveContractError,
    ArchiveManifest,
    ExactContainerSnapshot,
    ExactDeliverySnapshot,
    ExactStorageObject,
    build_archive_manifest,
    execute_exact_archive,
)


NOW = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)


def revision(day: int, hour: int = 12) -> str:
    return firestore_revision(
        f"2026-08-{day:02d}T{hour:02d}:00:00.000000000Z", "upsert"
    )


def projected(container_id: str, expiries: tuple[str | None, ...]) -> ProjectedContainer:
    items = tuple(
        {
            "description": "Item",
            "expiryDate": expiry,
            "isLowQuantity": False,
            "isShortCoded": False,
            "itemPosition": index,
            "pageNumber": 1,
            "product": str(100 + index),
            "visibleOnList": True,
        }
        for index, expiry in enumerate(expiries)
    )
    return ProjectedContainer(
        route_number="989262",
        delivery_id="1806242572",
        container_id=container_id,
        active=True,
        items=items,
        source_revision=revision(20, 12 + len(container_id)),
        source_business_hash=business_hash(
            {"container": container_id, "items": list(items)}
        ),
    )


def storage_object(container_id: str, name: str, generation: str) -> ExactStorageObject:
    return ExactStorageObject(
        source_path=f"routes/989262/pcfs/1806242572/{container_id}/{name}",
        archive_path=f"routes/989262/archivedPCFs/1806242572/{container_id}/{name}",
        generation=generation,
        size=100 + int(generation),
        md5_hash=f"md5-{container_id}-{name}-{generation}",
    )


def snapshot() -> ExactDeliverySnapshot:
    rows = []
    for index, container_id in enumerate(("C01", "C02"), start=1):
        row = projected(container_id, ("2026-08-19", "2026-08-20"))
        rows.append(
            ExactContainerSnapshot(
                projected=row,
                document_revision=row.source_revision,
                document_hash=business_hash(
                    {"container": container_id, "document": "raw"}
                ),
                objects=(
                    storage_object(container_id, "page_1.jpg", str(index)),
                    storage_object(container_id, "page_2.jpg", str(index + 10)),
                ),
            )
        )
    return ExactDeliverySnapshot(
        route_number="989262",
        delivery_id="1806242572",
        parent_revision=revision(21),
        parent_hash=business_hash({"delivery": "1806242572"}),
        containers=tuple(rows),
    )


def claim_for(source: ExactDeliverySnapshot) -> ClaimedLifecycleWork:
    plan = build_cleanup_plan(
        [row.projected for row in source.containers],
        retention_days=4,
        policy_revision=revision(22),
    ).candidate
    return ClaimedLifecycleWork(
        id=1,
        semantic_key=plan.semantic_key,
        route_number=plan.route_number,
        delivery_id=plan.delivery_id,
        container_id=plan.anchor_container_id,
        effective_business_date=plan.effective_business_date,
        due_at=plan.due_at,
        deadline_at=plan.due_at + timedelta(days=30),
        source_revision=plan.source_revision,
        source_business_hash=plan.source_business_hash,
        decision_watermark=plan.decision_watermark,
        claim_token="claim-token",
        attempt_count=1,
    )


class FakeExactArchiveAdapter:
    def __init__(self, source: ExactDeliverySnapshot):
        self.source_template = source
        self.source_documents_present = True
        self.source_objects = {
            obj.source_path: obj
            for row in source.containers
            for obj in row.objects
        }
        self.archive_manifest: ArchiveManifest | None = None
        self.archive_documents_copied = False
        self.archive_objects = {}
        self.fence = True
        self.fence_sequence = None
        self.authority_sequence = None
        self.authority_checks = 0
        self.retention_days = 4
        self.policy_revision = revision(22)
        self.retention_sequence = None
        self.verify_archive_override = None
        self.verify_source_absent_override = None
        self.source_list_override = None
        self.delete_object_noop = False
        self.read_count = 0
        self.copy_count = 0
        self.delete_count = 0
        self.fail_copy_after = None
        self.fail_delete_after = None
        self.fail_after_document_delete = False
        self.advance_document_revision_on_second_read = False
        self.calls = []

    def assert_exact_mutation_fence(self, claimed):
        self.calls.append("fence")
        if self.fence_sequence:
            return self.fence_sequence.pop(0)
        return self.fence

    def refresh_claim_authority(self, claimed, *, begin_effect):
        self.calls.append(f"authority:{'begin' if begin_effect else 'refresh'}")
        self.authority_checks += 1
        if self.authority_sequence is not None:
            return self.authority_sequence.pop(0)
        return NOW + timedelta(seconds=self.authority_checks)

    def read_current_retention(self, route_number):
        self.calls.append("retention")
        if self.retention_sequence:
            return self.retention_sequence.pop(0)
        return self.retention_days, self.policy_revision

    def _current_snapshot(self):
        if not self.source_documents_present:
            return None
        self.read_count += 1
        rows = []
        for row in self.source_template.containers:
            revision_value = row.document_revision
            if self.advance_document_revision_on_second_read and self.read_count >= 2:
                revision_value = revision(24)
            rows.append(
                dataclasses.replace(
                    row,
                    document_revision=revision_value,
                    objects=tuple(
                        self.source_objects[obj.source_path]
                        for obj in row.objects
                        if obj.source_path in self.source_objects
                    ),
                )
            )
        return dataclasses.replace(self.source_template, containers=tuple(rows))

    def read_exact_delivery(self, route_number, delivery_id):
        self.calls.append("read_source")
        return self._current_snapshot()

    def read_archive_manifest(self, route_number, delivery_id):
        self.calls.append("read_manifest")
        return self.archive_manifest

    def create_or_verify_archive_manifest(self, manifest):
        self.calls.append("manifest")
        if self.archive_manifest is None:
            self.archive_manifest = manifest
            return True
        return self.archive_manifest == manifest

    def copy_archive_documents(self, manifest):
        self.calls.append("copy_documents")
        self.archive_documents_copied = True

    def copy_exact_object(self, obj):
        self.calls.append(f"copy:{obj.source_path}")
        existing = self.archive_objects.get(obj.archive_path)
        if existing is not None:
            if existing != obj:
                raise RuntimeError("archive object conflict")
            return
        current = self.source_objects.get(obj.source_path)
        if current != obj:
            raise RuntimeError("source generation precondition failed")
        self.copy_count += 1
        if self.fail_copy_after is not None and self.copy_count > self.fail_copy_after:
            raise RuntimeError("injected copy crash")
        self.archive_objects[obj.archive_path] = obj

    def verify_archive(self, manifest):
        self.calls.append("verify_archive")
        if self.verify_archive_override is not None:
            return self.verify_archive_override
        expected = {
            obj.archive_path: obj
            for row in manifest.containers
            for obj in row.objects
        }
        expected.update(
            {obj.archive_path: obj for obj in manifest.unmapped_objects}
        )
        return (
            self.archive_manifest == manifest
            and self.archive_documents_copied
            and self.archive_objects == expected
        )

    def list_exact_source_objects(self, route_number, delivery_id):
        self.calls.append("list_source_objects")
        if self.source_list_override is not None:
            return list(self.source_list_override)
        return list(self.source_objects.values())

    def delete_exact_source_object(self, obj):
        self.calls.append(f"delete:{obj.source_path}:{obj.generation}")
        if self.source_objects.get(obj.source_path) != obj:
            raise RuntimeError("delete generation precondition failed")
        if self.delete_object_noop:
            return
        del self.source_objects[obj.source_path]
        self.delete_count += 1
        if self.fail_delete_after is not None and self.delete_count >= self.fail_delete_after:
            raise RuntimeError("injected delete crash")

    def delete_source_documents_atomically(self, manifest):
        self.calls.append("delete_documents_atomically")
        if self.source_objects:
            raise RuntimeError("source objects remain")
        current = self._current_snapshot()
        if current is None:
            return
        current_identity = (
            current.parent_revision,
            current.parent_hash,
            tuple(
                (
                    row.projected.container_id,
                    row.document_revision,
                    row.document_hash,
                    row.projected.source_business_hash,
                )
                for row in current.containers
            ),
        )
        manifest_identity = (
            manifest.parent_revision,
            manifest.parent_hash,
            tuple(
                (
                    row.projected.container_id,
                    row.document_revision,
                    row.document_hash,
                    row.projected.source_business_hash,
                )
                for row in manifest.containers
            ),
        )
        if current_identity != manifest_identity:
            raise RuntimeError("atomic document precondition failed")
        self.source_documents_present = False
        if self.fail_after_document_delete:
            raise RuntimeError("injected post-document-delete crash")

    def verify_exact_source_absent(self, route_number, delivery_id):
        self.calls.append("verify_source_absent")
        if self.verify_source_absent_override is not None:
            return self.verify_source_absent_override
        return not self.source_documents_present and not self.source_objects


class ExactArchiveEffectTests(unittest.TestCase):
    def setUp(self):
        self.source = snapshot()
        self.claim = claim_for(self.source)
        self.adapter = FakeExactArchiveAdapter(self.source)

    def execute(self):
        return execute_exact_archive(self.claim, self.adapter)

    def test_happy_path_verifies_archive_before_exact_deletes(self):
        result = self.execute()
        self.assertTrue(result.completed)
        self.assertEqual(result.disposition, "archived_verified")
        self.assertTrue(self.adapter.verify_archive(self.adapter.archive_manifest))
        self.assertTrue(self.adapter.verify_exact_source_absent("989262", "1806242572"))
        verify_index = self.adapter.calls.index("verify_archive")
        first_delete = next(
            index
            for index, call in enumerate(self.adapter.calls)
            if call.startswith("delete:")
        )
        self.assertLess(verify_index, first_delete)
        self.assertLess(
            max(
                index
                for index, call in enumerate(self.adapter.calls)
                if call.startswith("delete:")
            ),
            self.adapter.calls.index("delete_documents_atomically"),
        )
        begin_index = self.adapter.calls.index("authority:begin")
        self.assertLess(begin_index, first_delete)
        self.assertGreater(
            self.adapter.calls.index("authority:refresh", begin_index + 1),
            first_delete,
        )

    def test_unproven_mutation_fence_stops_before_source_read(self):
        self.adapter.fence = False
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "mutation_fence_not_proven")
        self.assertEqual(self.adapter.calls, ["fence"])

    def test_archive_verification_failure_performs_no_delete(self):
        self.adapter.verify_archive_override = False
        result = self.execute()
        self.assertEqual(result.disposition, "archive_verification_failed")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))
        self.assertTrue(self.adapter.source_documents_present)

    def test_existing_different_manifest_is_a_hard_conflict(self):
        manifest = build_archive_manifest(self.claim, self.source)
        self.adapter.archive_manifest = dataclasses.replace(
            manifest, manifest_hash="f" * 64
        )
        result = self.execute()
        self.assertEqual(result.disposition, "archive_manifest_conflict")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))

    def test_object_generation_change_stops_before_delete(self):
        original_create = self.adapter.create_or_verify_archive_manifest

        def mutate_after_manifest(manifest):
            result = original_create(manifest)
            path, original = next(iter(self.adapter.source_objects.items()))
            self.adapter.source_objects[path] = dataclasses.replace(
                original, generation="999"
            )
            return result

        self.adapter.create_or_verify_archive_manifest = mutate_after_manifest
        with self.assertRaisesRegex(RuntimeError, "source generation precondition"):
            self.execute()
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))

    def test_new_object_after_manifest_blocks_document_cleanup(self):
        original_copy = self.adapter.copy_archive_documents

        def inject(manifest):
            original_copy(manifest)
            extra = storage_object("C01", "late_page.jpg", "77")
            self.adapter.source_objects[extra.source_path] = extra

        self.adapter.copy_archive_documents = inject
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "unexpected_source_object")
        self.assertTrue(self.adapter.source_documents_present)

    def test_document_revision_change_after_archive_blocks_all_deletes(self):
        self.adapter.advance_document_revision_on_second_read = True
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "source_documents_changed")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))

    def test_mutation_fence_loss_after_verified_archive_blocks_all_deletes(self):
        self.adapter.fence_sequence = [True, False]
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "mutation_fence_lost")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))
        self.assertTrue(self.adapter.source_documents_present)

    def test_retention_change_after_verified_archive_blocks_all_deletes(self):
        self.adapter.retention_sequence = [
            (4, revision(22)),
            (8, revision(24)),
        ]
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "superseded_schedule")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))
        self.assertTrue(self.adapter.source_documents_present)

    def test_source_object_generation_change_after_archive_blocks_delete(self):
        changed = list(self.adapter.source_objects.values())
        changed[0] = dataclasses.replace(changed[0], generation="999")
        self.adapter.source_list_override = changed
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "source_object_changed")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))
        self.assertTrue(self.adapter.source_documents_present)

    def test_source_object_remaining_after_delete_blocks_document_cleanup(self):
        self.adapter.delete_object_noop = True
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "source_objects_remain")
        self.assertTrue(self.adapter.source_documents_present)
        self.assertNotIn("delete_documents_atomically", self.adapter.calls)

    def test_failed_final_absence_verification_never_reports_complete(self):
        self.adapter.verify_source_absent_override = False
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "source_cleanup_not_verified")

    def test_copy_crash_is_idempotent_on_retry(self):
        self.adapter.fail_copy_after = 2
        with self.assertRaisesRegex(RuntimeError, "injected copy crash"):
            self.execute()
        self.assertEqual(len(self.adapter.archive_objects), 2)
        self.assertTrue(self.adapter.source_documents_present)
        self.adapter.fail_copy_after = None
        result = self.execute()
        self.assertTrue(result.completed)
        self.assertEqual(result.disposition, "archived_verified")

    def test_partial_object_delete_crash_recovers_from_verified_manifest(self):
        self.adapter.fail_delete_after = 2
        with self.assertRaisesRegex(RuntimeError, "injected delete crash"):
            self.execute()
        self.assertEqual(len(self.adapter.source_objects), 2)
        self.assertTrue(self.adapter.source_documents_present)
        self.adapter.fail_delete_after = None
        result = self.execute()
        self.assertTrue(result.completed)
        self.assertEqual(result.disposition, "archived_verified")

    def test_crash_after_atomic_document_delete_replays_as_already_complete(self):
        self.adapter.fail_after_document_delete = True
        with self.assertRaisesRegex(RuntimeError, "post-document-delete"):
            self.execute()
        self.adapter.fail_after_document_delete = False
        result = self.execute()
        self.assertTrue(result.completed)
        self.assertEqual(result.disposition, "already_complete")

    def test_source_missing_without_verified_archive_never_completes(self):
        self.adapter.source_documents_present = False
        self.adapter.source_objects.clear()
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "source_missing_without_verified_archive")

    def test_changed_retention_blocks_before_archive_manifest(self):
        self.adapter.retention_days = 8
        self.adapter.policy_revision = revision(24)
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "superseded_schedule")
        self.assertIsNone(self.adapter.archive_manifest)

    def test_path_escape_is_rejected_before_adapter_writes(self):
        bad_object = dataclasses.replace(
            self.source.containers[0].objects[0],
            source_path="routes/other/pcfs/1806242572/C01/page_1.jpg",
        )
        bad_container = dataclasses.replace(
            self.source.containers[0], objects=(bad_object,)
        )
        bad_snapshot = dataclasses.replace(
            self.source,
            containers=(bad_container, *self.source.containers[1:]),
        )
        with self.assertRaises(ArchiveContractError):
            build_archive_manifest(self.claim, bad_snapshot)

    def test_archive_suffix_mismatch_is_rejected(self):
        bad_object = dataclasses.replace(
            self.source.containers[0].objects[0],
            archive_path=(
                "routes/989262/archivedPCFs/1806242572/C01/renamed.jpg"
            ),
        )
        bad_container = dataclasses.replace(
            self.source.containers[0], objects=(bad_object,)
        )
        bad_snapshot = dataclasses.replace(
            self.source,
            containers=(bad_container, *self.source.containers[1:]),
        )
        with self.assertRaisesRegex(
            ArchiveContractError, "exact source-relative suffix"
        ):
            build_archive_manifest(self.claim, bad_snapshot)

    def test_preexisting_unmapped_object_is_archived_and_deleted_exactly(self):
        orphan = storage_object("ORPHAN", "page_1.jpg", "77")
        self.source = dataclasses.replace(
            self.source,
            unmapped_objects=(orphan,),
        )
        self.claim = claim_for(self.source)
        self.adapter = FakeExactArchiveAdapter(self.source)
        self.adapter.source_objects[orphan.source_path] = orphan
        result = self.execute()
        self.assertTrue(result.completed)
        self.assertEqual(self.adapter.archive_objects[orphan.archive_path], orphan)
        self.assertIn(f"delete:{orphan.source_path}:{orphan.generation}", self.adapter.calls)

    def test_lost_claim_after_archive_performs_no_delete(self):
        self.adapter.authority_sequence = [NOW, None]
        result = self.execute()
        self.assertFalse(result.completed)
        self.assertEqual(result.disposition, "claim_authority_lost")
        self.assertFalse(any(call.startswith("delete:") for call in self.adapter.calls))

    def test_effect_protocol_exposes_no_prefix_recursive_or_bulk_delete(self):
        source = inspect.getsource(
            __import__(
                "order_forecast.scripts.pcf_lifecycle_effect",
                fromlist=["ExactArchiveAdapter"],
            ).ExactArchiveAdapter
        ).lower()
        for forbidden in ("delete_prefix", "recursive", "bulk_delete", "route_wide"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()

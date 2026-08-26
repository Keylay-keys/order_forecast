from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import archive_purge_worker as worker


class _FakeSnapshot:
    def __init__(self, doc_id, data, reference):
        self.id = doc_id
        self._data = dict(data)
        self.reference = reference
        self.exists = True

    def to_dict(self):
        return dict(self._data)


class _FakeDocument:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)
        self.id = self.path[-1]

    def collection(self, name):
        return _FakeCollection(self.db, self.path + (name,))

    def set(self, data, merge=False):
        current = self.db.documents.get(self.path, {}) if merge else {}
        self.db.documents[self.path] = {**current, **dict(data)}

    def update(self, data):
        self.db.documents.setdefault(self.path, {}).update(dict(data))


class _FakeCollection:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)

    def document(self, doc_id):
        return _FakeDocument(self.db, self.path + (doc_id,))

    def stream(self):
        expected_length = len(self.path) + 1
        rows = []
        for path, data in self.db.documents.items():
            if len(path) == expected_length and path[:-1] == self.path:
                ref = _FakeDocument(self.db, path)
                rows.append(_FakeSnapshot(path[-1], data, ref))
        return rows


class _FakeDB:
    def __init__(self):
        self.documents = {}

    def collection(self, name):
        return _FakeCollection(self, (name,))


class TestArchivePurgeWorkerHddDates(unittest.TestCase):
    def test_latest_hdd_run_date_ignores_canonical_dirs_when_timestamp_runs_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            (delivery_dir / "20260416_173753").mkdir(parents=True)
            (delivery_dir / "20260417_101112").mkdir()
            (delivery_dir / "pages").mkdir()
            (delivery_dir / "reports").mkdir()

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 17),
                )

    def test_latest_hdd_run_date_uses_manifest_source_run_ids_for_canonical_only_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            delivery_dir.mkdir(parents=True)
            (delivery_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "generatedAt": "2026-04-20T00:00:00Z",
                        "sourceRunIds": ["20260416_173753", "20260418_091011"],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 18),
                )

    def test_latest_hdd_run_date_falls_back_to_manifest_generated_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            delivery_dir.mkdir(parents=True)
            (delivery_dir / "manifest.json").write_text(
                json.dumps({"generatedAt": "2026-04-20T14:15:16Z"}),
                encoding="utf-8",
            )

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 20),
                )


class TestArchivePurgeCheckpointPolicy(unittest.TestCase):
    def test_route_lock_prevents_candidate_reads_and_effects(self):
        db = _FakeDB()
        with mock.patch.object(
            worker, "_is_route_locked", return_value=True
        ), mock.patch.object(
            worker, "_active_delivery_ids"
        ) as active_ids, mock.patch.object(
            worker, "_delete_storage_prefix"
        ) as delete_storage, mock.patch.object(
            worker, "_delete_firestore_delivery"
        ) as delete_firestore:
            result = worker._process_route_purge(db, "989262", {})

        self.assertEqual(result, {"route": "989262", "skipped": True, "reason": "route_locked"})
        active_ids.assert_not_called()
        delete_storage.assert_not_called()
        delete_firestore.assert_not_called()
        self.assertFalse(any(path[0] == worker.PURGE_EVENT_COLLECTION for path in db.documents))

    def test_active_delivery_is_not_a_purge_candidate(self):
        db = _FakeDB()
        source_path = ("routes", "989262", "archivedPCFs", "1800000000")
        db.documents[source_path] = {"archivedAt": "2020-01-01T00:00:00Z"}

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            worker, "HDD_ARCHIVE_BASE", Path(tmp)
        ), mock.patch.object(
            worker, "_is_route_locked", return_value=False
        ), mock.patch.object(
            worker, "_retention_days_for_route", return_value=90
        ), mock.patch.object(
            worker, "_active_delivery_ids", return_value={"1800000000"}
        ), mock.patch.object(
            worker, "_delete_storage_prefix"
        ) as delete_storage, mock.patch.object(
            worker, "_delete_firestore_delivery"
        ) as delete_firestore:
            result = worker._process_route_purge(db, "989262", {})

        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "no_candidates")
        delete_storage.assert_not_called()
        delete_firestore.assert_not_called()
        self.assertIn(source_path, db.documents)

    def test_retention_cutoff_is_inclusive_and_newer_source_is_preserved(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = cls(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
                return value if tz is not None else value.replace(tzinfo=None)

        db = _FakeDB()
        cutoff = _FixedDatetime.now(timezone.utc).date() - timedelta(days=90)
        eligible_delivery = "1800000000"
        newer_delivery = "1800000001"
        db.documents[("routes", "989262", "archivedPCFs", eligible_delivery)] = {
            "archivedAt": cutoff.isoformat()
        }
        db.documents[("routes", "989262", "archivedPCFs", newer_delivery)] = {
            "archivedAt": (cutoff + timedelta(days=1)).isoformat()
        }

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            worker, "datetime", _FixedDatetime
        ), mock.patch.object(
            worker, "HDD_ARCHIVE_BASE", Path(tmp)
        ), mock.patch.object(
            worker, "_is_route_locked", return_value=False
        ), mock.patch.object(
            worker, "_retention_days_for_route", return_value=90
        ), mock.patch.object(
            worker, "_active_delivery_ids", return_value=set()
        ), mock.patch.object(
            worker, "_delete_storage_prefix", return_value=0
        ), mock.patch.object(
            worker,
            "_delete_firestore_delivery",
            return_value={"firestoreDocsDeleted": 1, "containerDocsDeleted": 0},
        ) as delete_firestore:
            result = worker._process_route_purge(db, "989262", {})

        delete_firestore.assert_called_once_with(db, "989262", eligible_delivery)
        self.assertEqual(result["successCount"], 1)
        self.assertEqual(result["failureCount"], 0)

    def test_invalid_delivery_identity_is_never_used_for_effects(self):
        db = _FakeDB()
        invalid_path = ("routes", "989262", "archivedPCFs", "..")
        db.documents[invalid_path] = {"archivedAt": "2020-01-01T00:00:00Z"}

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            worker, "HDD_ARCHIVE_BASE", Path(tmp)
        ), mock.patch.object(
            worker, "_is_route_locked", return_value=False
        ), mock.patch.object(
            worker, "_retention_days_for_route", return_value=90
        ), mock.patch.object(
            worker, "_active_delivery_ids", return_value=set()
        ), mock.patch.object(
            worker, "_delete_storage_prefix"
        ) as delete_storage, mock.patch.object(
            worker, "_delete_firestore_delivery"
        ) as delete_firestore:
            result = worker._process_route_purge(db, "989262", {})

        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "no_candidates")
        self.assertEqual(result["invalidIdentityCount"], 1)
        delete_storage.assert_not_called()
        delete_firestore.assert_not_called()

    def test_hdd_delete_rejects_traversal_and_symlink_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive_root = Path(tmp) / "archive"
            route_root = archive_root / "989262"
            outside = Path(tmp) / "outside"
            route_root.mkdir(parents=True)
            outside.mkdir()

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", archive_root):
                with self.assertRaisesRegex(ValueError, "Invalid delivery identity"):
                    worker._delete_hdd_delivery("989262", "../outside")

                delivery_link = route_root / "1800000000"
                delivery_link.symlink_to(outside, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "escapes configured root"):
                    worker._delete_hdd_delivery("989262", "1800000000")

            self.assertTrue(outside.exists())

    def test_failed_reprocess_clears_older_purged_timestamp(self):
        db = _FakeDB()
        checkpoint_path = (
            worker.PURGE_CHECKPOINT_COLLECTION,
            worker._checkpoint_doc_id("989262", "1800000000"),
        )
        db.documents[checkpoint_path] = {
            "status": "completed",
            "purgedAt": "older-completion",
        }

        worker._mark_checkpoint(
            db,
            route_number="989262",
            delivery_number="1800000000",
            status="failed",
            event_id="retry-event",
            details={"error": "injected"},
        )

        checkpoint = db.documents[checkpoint_path]
        self.assertEqual(checkpoint["status"], "failed")
        self.assertIsNone(checkpoint["purgedAt"])
        self.assertEqual(checkpoint["checkpointPolicy"], "reprocess_current_source")

    def test_completed_delivery_checkpoint_does_not_suppress_reappeared_source(self):
        db = _FakeDB()
        route_number = "989262"
        delivery_number = "1800000000"
        db.documents[("routes", route_number, "archivedPCFs", delivery_number)] = {
            "archivedAt": "2020-01-01T00:00:00Z"
        }
        checkpoint_path = (
            worker.PURGE_CHECKPOINT_COLLECTION,
            worker._checkpoint_doc_id(route_number, delivery_number),
        )
        db.documents[checkpoint_path] = {
            "status": "completed",
            "eventId": "older-purge",
        }

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            worker, "HDD_ARCHIVE_BASE", Path(tmp)
        ), mock.patch.object(
            worker, "_is_route_locked", return_value=False
        ), mock.patch.object(
            worker, "_retention_days_for_route", return_value=90
        ), mock.patch.object(
            worker, "_active_delivery_ids", return_value=set()
        ), mock.patch.object(
            worker,
            "_delete_firestore_delivery",
            return_value={"firestoreDocsDeleted": 1, "containerDocsDeleted": 2},
        ) as delete_firestore, mock.patch.object(
            worker, "_delete_storage_prefix", return_value=3
        ) as delete_storage:
            result = worker._process_route_purge(db, route_number, {})

        delete_firestore.assert_called_once_with(db, route_number, delivery_number)
        delete_storage.assert_called_once_with(route_number, delivery_number)
        self.assertEqual(result["successCount"], 1)
        self.assertEqual(result["failureCount"], 0)
        self.assertEqual(result["skippedCheckpointCount"], 0)
        self.assertEqual(result["checkpointPolicy"], "reprocess_current_source")
        checkpoint = db.documents[checkpoint_path]
        self.assertEqual(checkpoint["status"], "completed")
        self.assertEqual(checkpoint["checkpointPolicy"], "reprocess_current_source")
        self.assertEqual(checkpoint["details"]["sources"], ["firestore"])
        self.assertEqual(checkpoint["details"]["retentionAnchorDates"], ["2020-01-01"])

    def test_storage_failure_keeps_firestore_source_for_retry(self):
        db = _FakeDB()
        route_number = "989262"
        delivery_number = "1800000000"
        source_path = ("routes", route_number, "archivedPCFs", delivery_number)
        db.documents[source_path] = {"archivedAt": "2020-01-01T00:00:00Z"}

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            worker, "HDD_ARCHIVE_BASE", Path(tmp)
        ), mock.patch.object(
            worker, "_is_route_locked", return_value=False
        ), mock.patch.object(
            worker, "_retention_days_for_route", return_value=90
        ), mock.patch.object(
            worker, "_active_delivery_ids", return_value=set()
        ), mock.patch.object(
            worker,
            "_delete_storage_prefix",
            side_effect=RuntimeError("injected exact-object failure"),
        ), mock.patch.object(
            worker, "_delete_firestore_delivery"
        ) as delete_firestore:
            result = worker._process_route_purge(db, route_number, {})

        delete_firestore.assert_not_called()
        self.assertIn(source_path, db.documents)
        self.assertEqual(result["successCount"], 0)
        self.assertEqual(result["failureCount"], 1)
        checkpoint_path = (
            worker.PURGE_CHECKPOINT_COLLECTION,
            worker._checkpoint_doc_id(route_number, delivery_number),
        )
        checkpoint = db.documents[checkpoint_path]
        self.assertEqual(checkpoint["status"], "failed")
        self.assertIsNone(checkpoint["purgedAt"])

    def test_storage_delete_reports_partial_failure(self):
        class _Blob:
            def __init__(self, name, should_fail=False):
                self.name = name
                self.should_fail = should_fail

            def delete(self):
                if self.should_fail:
                    raise RuntimeError("injected")

        class _Bucket:
            def list_blobs(self, prefix):
                self.prefix = prefix
                return [_Blob(f"{prefix}one"), _Blob(f"{prefix}two", should_fail=True)]

        bucket = _Bucket()
        with mock.patch.object(worker, "_bucket", return_value=bucket):
            with self.assertRaisesRegex(RuntimeError, "1 archived Storage object"):
                worker._delete_storage_prefix("989262", "1800000000")

        self.assertEqual(
            bucket.prefix,
            "routes/989262/archivedPCFs/1800000000/",
        )


if __name__ == "__main__":
    unittest.main()

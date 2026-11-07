from contextlib import contextmanager
from datetime import datetime, timezone
import base64
import hashlib
import unittest
from unittest.mock import patch

from order_forecast.api import accounting_purchase_import_worker as worker


PACKET_ID = "123e4567-e89b-42d3-a456-426614174000"
ROUTE = "989262"
OWNER = "owner-uid"
MEMBER = "member-uid"
BUCKET = "routespark-1f47d.firebasestorage.app"
JPEG = bytes.fromhex("ffd8ffc00008000000000000ffda0002ffd9")
FIREBASE_MANIFEST_KEYS = {
    "bytes", "contentType", "md5Hash", "packetId", "pageId", "pageIndex",
    "routeNumber", "storageBucket", "storageGeneration", "storagePath", "uploadedByUid",
}


def jpeg_for(index):
    return JPEG if index == 0 else JPEG + bytes([index]) + b"SEFT"


def ready_upload(uploader=OWNER, pages=1):
    objects = []
    for index in range(pages):
        page_id = f"page_{index}.jpg"
        content = jpeg_for(index)
        objects.append({
            "routeNumber": ROUTE,
            "packetId": PACKET_ID,
            "pageId": page_id,
            "pageIndex": index,
            "storageBucket": BUCKET,
            "storagePath": f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}/{page_id}",
            "storageGeneration": str(index + 10),
            "bytes": len(content),
            "contentType": "image/jpeg",
            "md5Hash": base64.b64encode(hashlib.md5(content).digest()).decode(),
            "uploadedByUid": uploader,
        })
    return {
        "schemaVersion": 1,
        "id": PACKET_ID,
        "routeNumber": ROUTE,
        "userId": uploader,
        "source": "app_scan",
        "status": "ready",
        "pageCount": pages,
        "expectedPageIds": [f"page_{index}.jpg" for index in range(pages)],
        "uploadComplete": True,
        "pendingClusterImport": True,
        "createdAt": datetime(2026, 11, 4, 18, tzinfo=timezone.utc),
        "storageManifest": objects,
        "storageManifestHash": worker._manifest_hash(objects),
        "clientRelease": {"contractVersion": 1, "appVersion": "1.1.7", "buildNumber": "7", "platform": "android"},
    }


def imported_upload(uploader=OWNER, pages=1):
    value = ready_upload(uploader, pages)
    value.update({
        "pendingClusterImport": False,
        "clusterImportStatus": "imported",
        "clusterImportUploadId": PACKET_ID,
        "clusterImportOwnerUid": OWNER,
        "firebaseStorageCleanupStatus": "pending",
    })
    return value


class FakeStore:
    def __init__(self):
        self.documents = {}
        self.originals = {}
        self.jobs = set()

    def get(self, key):
        return self.documents.get(key)

    def put(self, key, value):
        self.documents[key] = value

    def has_original(self, kind, digest):
        return (kind, digest) in self.originals

    def add_original(self, kind, upload, file, content):
        self.originals[(kind, file["sha256"])] = (upload, file, content)

    def original(self, kind, upload, file_id):
        for stored_kind, _digest in self.originals:
            stored_upload, file, content = self.originals[(stored_kind, _digest)]
            if stored_kind == kind and stored_upload == upload and file["id"] == file_id:
                if hashlib.sha256(content).hexdigest() != file["sha256"]:
                    raise RuntimeError("Original integrity mismatch")
                return {
                    "sha256": file["sha256"],
                    "mediaType": file["mediaType"],
                    "originalName": file["originalName"],
                    "content": content,
                }
        return None

    def queue_receipt(self, upload):
        self.jobs.add(upload)


class FakeBlob:
    def __init__(self, item, content=None):
        self.item = item
        self.content = jpeg_for(item["pageIndex"]) if content is None else content
        self.generation = int(item["storageGeneration"])
        self.size = len(self.content)
        self.content_type = "image/jpeg"
        self.md5_hash = base64.b64encode(hashlib.md5(self.content).digest()).decode()
        self.metadata = {
            "uploadedByUid": item["uploadedByUid"],
            "routeNumber": ROUTE,
            "packetId": PACKET_ID,
            "pageId": item["pageId"],
            "source": "app_scan",
            "schemaVersion": "1",
        }
        self.delete_calls = []

    def reload(self, **kwargs):
        if kwargs.get("if_generation_match") != self.generation:
            raise AssertionError("generation precondition missing from metadata read")
        return None

    def download_as_bytes(self, **kwargs):
        if kwargs.get("if_generation_match") != self.generation:
            raise AssertionError("generation precondition missing")
        return self.content

    def delete(self, **kwargs):
        if kwargs.get("if_generation_match") != self.generation:
            raise AssertionError("generation precondition missing from delete")
        if kwargs.get("timeout") != 30:
            raise AssertionError("bounded delete timeout missing")
        self.delete_calls.append(kwargs)


class FakeBucket:
    def __init__(self, objects):
        self.objects = {item["storagePath"]: item for item in objects}
        self.blobs = {path: FakeBlob(item) for path, item in self.objects.items()}

    def blob(self, path, generation=None):
        item = self.objects[path]
        if generation != int(item["storageGeneration"]):
            raise AssertionError("wrong generation")
        return self.blobs[path]


class FakeReference:
    def __init__(self, db, path):
        self.db = db
        self.path = path

    def get(self, transaction=None):
        return FakeSnapshot(self, self.db.documents.get(self.path))


class FakeSnapshot:
    def __init__(self, reference, data):
        self.reference = reference
        self.exists = data is not None
        self.data = data

    def to_dict(self):
        return self.data


class FakeCollection:
    def __init__(self, db, name):
        self.db = db
        self.name = name

    def document(self, name):
        return FakeReference(self.db, f"{self.name}/{name}")


class FakeFirestore:
    def __init__(self, documents):
        self.documents = documents

    def collection(self, name):
        return FakeCollection(self, name)

    def get_all(self, references):
        # Deliberately reverse the order to enforce path-bound authorization.
        return [reference.get() for reference in reversed(references)]

    def transaction(self):
        return FakeTransaction()


class FakeTransaction:
    def update(self, reference, values):
        document = reference.db.documents[reference.path]
        for key, value in values.items():
            if value is worker.firestore.DELETE_FIELD:
                document.pop(key, None)
            else:
                document[key] = value


class PurchaseImportTests(unittest.TestCase):
    def test_firebase_completion_manifest_contract_has_exact_expected_shape(self):
        data = ready_upload()
        self.assertEqual(set(data["storageManifest"][0]), FIREBASE_MANIFEST_KEYS)
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", data, BUCKET,
        )
        self.assertEqual(set(packet["objects"][0]), FIREBASE_MANIFEST_KEYS)

    def test_completed_packet_becomes_existing_receipt_packet_and_ocr_job(self):
        data = ready_upload(pages=2)
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", data, BUCKET,
        )
        sources = worker.download_sources(packet, FakeBucket(data["storageManifest"]))
        store = FakeStore()

        @contextmanager
        def transaction(uid, route):
            self.assertEqual((uid, route), (OWNER, ROUTE))
            yield store

        manifest, replay = worker.save_packet(OWNER, packet, sources, transaction)
        self.assertFalse(replay)
        self.assertEqual(manifest["source"], "app_scan")
        self.assertEqual(manifest["firebaseUpload"]["uploadedByUid"], OWNER)
        self.assertEqual([file["originalName"] for file in manifest["files"]], ["Scan page 1.jpg", "Scan page 2.jpg"])
        self.assertEqual(len(store.originals), 2)
        self.assertEqual(store.jobs, {PACKET_ID})

        repeated, replay = worker.save_packet(OWNER, packet, sources, transaction)
        self.assertTrue(replay)
        self.assertEqual(repeated, manifest)
        self.assertEqual(len(store.originals), 2)

    def test_saved_packet_is_read_back_before_import_confirmation(self):
        data = ready_upload(pages=2)
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", data, BUCKET,
        )
        sources = worker.download_sources(packet, FakeBucket(data["storageManifest"]))
        store = FakeStore()

        @contextmanager
        def transaction(uid, route):
            self.assertEqual((uid, route), (OWNER, ROUTE))
            yield store

        manifest, _ = worker.save_packet(OWNER, packet, sources, transaction)
        worker.verify_saved_packet(OWNER, packet, manifest, transaction)

        key = next(iter(store.originals))
        upload, file, _content = store.originals[key]
        store.originals[key] = (upload, file, b"damaged after commit")
        with self.assertRaisesRegex(RuntimeError, "Saved purchase original failed verification"):
            worker.verify_saved_packet(OWNER, packet, manifest, transaction)

    def test_failed_database_readback_never_marks_firebase_imported(self):
        data = ready_upload()
        path = f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}"
        db = FakeFirestore({path: data})
        reference = FakeReference(db, path)
        packet = worker.parse_ready_upload(path, data, BUCKET)
        manifest = worker.receipt_manifest(packet, [{
            "pageId": "page_0.jpg",
            "content": JPEG,
            "sha256": hashlib.sha256(JPEG).hexdigest(),
        }])
        finished = []

        def finish(_db, _reference, _lease, values):
            finished.append(values)
            return True

        with (
            patch.object(worker, "_claim", return_value={"lease": "lease", "attempt": 1, "data": data}),
            patch.object(worker, "authorize", return_value=OWNER),
            patch.object(worker, "download_sources", return_value=[{
                "pageId": "page_0.jpg", "content": JPEG,
                "sha256": hashlib.sha256(JPEG).hexdigest(),
            }]),
            patch.object(worker, "save_packet", return_value=(manifest, False)),
            patch.object(worker, "verify_saved_packet", side_effect=RuntimeError("readback failed")),
            patch.object(worker, "_finish", side_effect=finish),
        ):
            self.assertFalse(worker.process_reference(
                db, reference, BUCKET, bucket_factory=lambda _name: object(),
            ))

        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["clusterImportStatus"], "queued")
        self.assertTrue(finished[0]["pendingClusterImport"])
        self.assertNotIn("clusterImportUploadId", finished[0])

    def test_cluster_schedules_cleanup_only_after_import(self):
        data = imported_upload(pages=2)
        path = f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}"
        db = FakeFirestore({path: data})
        reference = FakeReference(db, path)
        with patch.object(worker.firestore, "transactional", side_effect=lambda function: function):
            self.assertTrue(worker.schedule_firebase_cleanup(db, reference, BUCKET))
        saved = db.documents[path]
        self.assertEqual(saved["firebaseStorageCleanupStatus"], "scheduled")
        self.assertIsInstance(saved["firebaseStorageCleanupAfter"], datetime)
        self.assertIsInstance(saved["firebaseStorageCleanupScheduledAt"], datetime)
        self.assertEqual(saved["firebaseStorageCleanupAttempts"], 0)
        self.assertNotIn("firebaseStorageCleanupErrorCode", saved)
        self.assertNotIn("firebaseStorageDeletedObjects", saved)

    def test_scheduled_cleanup_replay_is_idempotent(self):
        data = imported_upload()
        scheduled_at = datetime(2026, 11, 6, 19, tzinfo=timezone.utc)
        data.update({
            "firebaseStorageCleanupStatus": "scheduled",
            "firebaseStorageCleanupAfter": scheduled_at,
            "firebaseStorageCleanupScheduledAt": scheduled_at,
            "firebaseStorageCleanupAttempts": 0,
        })
        path = f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}"
        db = FakeFirestore({path: data})
        reference = FakeReference(db, path)
        with patch.object(worker.firestore, "transactional", side_effect=lambda function: function):
            self.assertTrue(worker.schedule_firebase_cleanup(db, reference, BUCKET))
        saved = db.documents[path]
        self.assertEqual(saved["firebaseStorageCleanupAfter"], scheduled_at)
        self.assertEqual(saved["firebaseStorageCleanupAttempts"], 0)

    def test_cleanup_is_not_scheduled_for_unimported_packet(self):
        data = imported_upload()
        data["clusterImportStatus"] = "needs_attention"
        path = f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}"
        db = FakeFirestore({path: data})
        reference = FakeReference(db, path)
        with patch.object(worker.firestore, "transactional", side_effect=lambda function: function):
            self.assertFalse(worker.schedule_firebase_cleanup(db, reference, BUCKET))
        self.assertEqual(data["firebaseStorageCleanupStatus"], "pending")

    def test_team_member_is_attributed_but_imported_into_owner_workspace(self):
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", ready_upload(MEMBER), BUCKET,
        )
        owner = worker.actor_allowed(
            packet,
            {"ownerUid": OWNER},
            {"userId": OWNER},
            {"profile": {"role": "teamMember"}, "routeAssignments": {
                ROUTE: {"role": "team_member", "verified": True, "assignedTo": OWNER},
            }},
        )
        self.assertEqual(owner, OWNER)
        packet["manifestHash"] = "a" * 64
        manifest = worker.receipt_manifest(packet, [{
            "pageId": "page_0.jpg", "content": JPEG, "sha256": hashlib.sha256(JPEG).hexdigest(),
        }])
        self.assertEqual(manifest["firebaseUpload"]["uploadedByUid"], MEMBER)

    def test_authorization_binds_unordered_reads_to_exact_document_paths(self):
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", ready_upload(MEMBER), BUCKET,
        )
        db = FakeFirestore({
            f"routes/{ROUTE}": {"ownerUid": OWNER},
            f"routeNumbers/{ROUTE}": {"userId": OWNER},
            f"users/{MEMBER}": {"routeAssignments": {ROUTE: {
                "role": "team_member", "verified": True, "assignedTo": OWNER,
            }}},
        })
        self.assertEqual(worker.authorize(db, packet), OWNER)

    def test_revoked_or_cross_account_actor_is_denied(self):
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", ready_upload(MEMBER), BUCKET,
        )
        with self.assertRaisesRegex(worker.PermanentImportError, "ACCOUNTING_ACCESS_REVOKED"):
            worker.actor_allowed(
                packet,
                {"ownerUid": OWNER}, {"userId": OWNER},
                {"routeAssignments": {ROUTE: {
                    "role": "team_member", "verified": True, "assignedTo": "different-owner",
                }}},
            )

    def test_manifest_hash_bucket_and_page_scope_fail_closed(self):
        path = f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}"
        bad_hash = ready_upload()
        bad_hash["storageManifestHash"] = "0" * 64
        with self.assertRaisesRegex(worker.PermanentImportError, "STORAGE_MANIFEST_HASH_MISMATCH"):
            worker.parse_ready_upload(path, bad_hash, BUCKET)

        wrong_bucket = ready_upload()
        wrong_bucket["storageManifest"][0]["storageBucket"] = "attacker-bucket"
        wrong_bucket["storageManifestHash"] = worker._manifest_hash(wrong_bucket["storageManifest"])
        with self.assertRaisesRegex(worker.PermanentImportError, "INVALID_STORAGE_MANIFEST"):
            worker.parse_ready_upload(path, wrong_bucket, BUCKET)

        wrong_page = ready_upload()
        wrong_page["storageManifest"][0]["storagePath"] = f"routes/111111/accounting_purchase_uploads/{PACKET_ID}/page_0.jpg"
        wrong_page["storageManifestHash"] = worker._manifest_hash(wrong_page["storageManifest"])
        with self.assertRaisesRegex(worker.PermanentImportError, "INVALID_STORAGE_MANIFEST"):
            worker.parse_ready_upload(path, wrong_page, BUCKET)

        wrong_route_identity = ready_upload()
        wrong_route_identity["storageManifest"][0]["routeNumber"] = "111111"
        with self.assertRaisesRegex(worker.PermanentImportError, "INVALID_STORAGE_MANIFEST"):
            worker.parse_ready_upload(path, wrong_route_identity, BUCKET)

        wrong_packet_identity = ready_upload()
        wrong_packet_identity["storageManifest"][0]["packetId"] = (
            "223e4567-e89b-42d3-a456-426614174000"
        )
        with self.assertRaisesRegex(worker.PermanentImportError, "INVALID_STORAGE_MANIFEST"):
            worker.parse_ready_upload(path, wrong_packet_identity, BUCKET)

        missing_identity = ready_upload()
        del missing_identity["storageManifest"][0]["packetId"]
        with self.assertRaisesRegex(worker.PermanentImportError, "INVALID_STORAGE_MANIFEST"):
            worker.parse_ready_upload(path, missing_identity, BUCKET)

    def test_storage_metadata_and_jpeg_content_are_reverified(self):
        data = ready_upload()
        packet = worker.parse_ready_upload(
            f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET_ID}", data, BUCKET,
        )
        blob = FakeBlob(data["storageManifest"][0])
        blob.metadata["uploadedByUid"] = "outsider"

        class Bucket:
            def blob(self, *args, **kwargs):
                return blob

        with self.assertRaisesRegex(worker.PermanentImportError, "STORAGE_OBJECT_IDENTITY_MISMATCH"):
            worker.download_sources(packet, Bucket())

        bad = FakeBlob(data["storageManifest"][0], content=b"not-a-jpeg")
        bad.size = data["storageManifest"][0]["bytes"]
        bad.md5_hash = data["storageManifest"][0]["md5Hash"]

        class BadBucket:
            def blob(self, *args, **kwargs):
                return bad

        with self.assertRaisesRegex(worker.PermanentImportError, "STORAGE_OBJECT_CONTENT_INVALID"):
            worker.download_sources(packet, BadBucket())


if __name__ == "__main__":
    unittest.main()

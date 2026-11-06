import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.routers import accounting_purchase


ROUTE = "989262"
PACKET = "123e4567-e89b-42d3-a456-426614174000"
UID = "member-uid"
DIGEST = "a" * 64


class Snapshot:
    def __init__(self, reference):
        self.reference = reference
        self.exists = reference.path in reference.db.documents

    def to_dict(self):
        value = self.reference.db.documents.get(self.reference.path)
        return dict(value) if value is not None else None


class Reference:
    def __init__(self, db, path):
        self.db = db
        self.path = path

    def collection(self, name):
        return Collection(self.db, f"{self.path}/{name}")

    def get(self):
        return Snapshot(self)


class Collection:
    def __init__(self, db, path):
        self.db = db
        self.path = path

    def document(self, name):
        return Reference(self.db, f"{self.path}/{name}")


class Firestore:
    def __init__(self, data):
        self.documents = {f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET}": data}

    def collection(self, name):
        return Collection(self, name)


def upload(**changes):
    value = {
        "schemaVersion": 1,
        "id": PACKET,
        "routeNumber": ROUTE,
        "userId": UID,
        "source": "app_scan",
        "status": "ready",
        "pageCount": 1,
        "uploadComplete": True,
        "pendingClusterImport": True,
        "storageManifestHash": DIGEST,
    }
    value.update(changes)
    return value


class AccountingPurchaseApiTests(unittest.TestCase):
    def client(self, data):
        db = Firestore(data)
        app = FastAPI()
        app.include_router(accounting_purchase.router, prefix="/api")
        app.dependency_overrides[verify_firebase_token] = lambda: {"uid": UID}
        app.dependency_overrides[get_firestore] = lambda: db
        return TestClient(app), db

    def test_exact_ready_packet_is_imported_before_phone_marks_complete(self):
        client, db = self.client(upload())

        def imported(_db, reference, bucket):
            self.assertIs(_db, db)
            self.assertEqual(reference.path, f"routes/{ROUTE}/accounting_purchase_uploads/{PACKET}")
            self.assertEqual(bucket, "routespark-1f47d.firebasestorage.app")
            db.documents[reference.path].update({
                "pendingClusterImport": False,
                "clusterImportStatus": "imported",
                "clusterImportUploadId": PACKET,
                "clusterImportOwnerUid": "owner-uid",
                "firebaseStorageCleanupStatus": "pending",
            })
            return True

        def cleaned(_db, reference, bucket):
            self.assertIs(_db, db)
            self.assertEqual(bucket, "routespark-1f47d.firebasestorage.app")
            db.documents[reference.path].update({
                "firebaseStorageCleanupStatus": "deleted",
                "firebaseStorageDeletedObjects": 1,
            })
            return True

        with (
            patch.object(accounting_purchase, "process_reference", side_effect=imported),
            patch.object(accounting_purchase, "cleanup_imported_reference", side_effect=cleaned),
        ):
            response = client.post(
                f"/api/business/accounting-purchase-import?route={ROUTE}",
                json={"packetId": PACKET, "storageManifestHash": DIGEST},
            )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json(), {
            "status": "imported", "route": ROUTE, "packetId": PACKET, "uploadId": PACKET,
        })

    def test_import_replay_is_idempotent_without_downloading_again(self):
        client, _ = self.client(upload(
            pendingClusterImport=False,
            clusterImportStatus="imported",
            clusterImportUploadId=PACKET,
            clusterImportOwnerUid="owner-uid",
            firebaseStorageCleanupStatus="deleted",
            firebaseStorageDeletedObjects=1,
        ))
        with (
            patch.object(accounting_purchase, "process_reference") as process,
            patch.object(accounting_purchase, "cleanup_imported_reference") as cleanup,
        ):
            response = client.post(
                f"/api/business/accounting-purchase-import?route={ROUTE}",
                json={"packetId": PACKET, "storageManifestHash": DIGEST},
            )
        self.assertEqual(response.status_code, 200)
        process.assert_not_called()
        cleanup.assert_not_called()

    def test_imported_packet_finishes_pending_cleanup_without_reimport(self):
        client, db = self.client(upload(
            pendingClusterImport=False,
            clusterImportStatus="imported",
            clusterImportUploadId=PACKET,
            clusterImportOwnerUid="owner-uid",
            firebaseStorageCleanupStatus="pending",
        ))

        def cleaned(_db, reference, _bucket):
            db.documents[reference.path].update({
                "firebaseStorageCleanupStatus": "deleted",
                "firebaseStorageDeletedObjects": 1,
            })
            return True

        with (
            patch.object(accounting_purchase, "process_reference") as process,
            patch.object(accounting_purchase, "cleanup_imported_reference", side_effect=cleaned) as cleanup,
        ):
            response = client.post(
                f"/api/business/accounting-purchase-import?route={ROUTE}",
                json={"packetId": PACKET, "storageManifestHash": DIGEST},
            )
        self.assertEqual(response.status_code, 200)
        process.assert_not_called()
        cleanup.assert_called_once()

    def test_imported_packet_returns_retryable_error_while_cleanup_is_pending(self):
        client, _ = self.client(upload(
            pendingClusterImport=False,
            clusterImportStatus="imported",
            clusterImportUploadId=PACKET,
            clusterImportOwnerUid="owner-uid",
            firebaseStorageCleanupStatus="pending",
        ))
        with (
            patch.object(accounting_purchase, "process_reference") as process,
            patch.object(accounting_purchase, "cleanup_imported_reference", return_value=False),
        ):
            response = client.post(
                f"/api/business/accounting-purchase-import?route={ROUTE}",
                json={"packetId": PACKET, "storageManifestHash": DIGEST},
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers.get("retry-after"), "5")
        process.assert_not_called()

    def test_cross_account_or_changed_manifest_is_denied_before_import(self):
        for body, data in (
            ({"packetId": PACKET, "storageManifestHash": DIGEST}, upload(userId="other")),
            ({"packetId": PACKET, "storageManifestHash": "b" * 64}, upload()),
        ):
            with self.subTest(body=body, data=data):
                client, _ = self.client(data)
                with patch.object(accounting_purchase, "process_reference") as process:
                    response = client.post(
                        f"/api/business/accounting-purchase-import?route={ROUTE}", json=body,
                    )
                self.assertEqual(response.status_code, 403)
                process.assert_not_called()


if __name__ == "__main__":
    unittest.main()

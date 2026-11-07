"""Import completed Firebase app scans into the hosted Accounting receipt pipeline.

Firebase is a recoverable handoff, not the Accounting system of record. This worker
claims only completed app packets, independently revalidates route access and
Storage identity, then stores the immutable originals in the owner's existing
PostgreSQL Accounting workspace. The existing receipt OCR worker handles the
resulting queue. Once the cluster commit is durable, it schedules Firebase-side
removal of the exact sealed Storage generations while retaining the audit record.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import re
from typing import Any
from uuid import UUID, uuid4, uuid5

from firebase_admin import firestore, storage
from .accounting_evidence import detect_media
from .accounting_store import accounting_transaction
from .dependencies import get_firebase_app

logger = logging.getLogger("accounting.purchase.import")

ROUTE = re.compile(r"^[0-9]{6}$")
PACKET = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
PAGE = re.compile(r"^page_([0-4])\.jpg$")
PATH = re.compile(r"^routes/([0-9]{6})/accounting_purchase_uploads/([0-9a-f-]{36})$")
MAX_PAGE_BYTES = 12 * 1024 * 1024
MAX_PACKET_BYTES = 25 * 1024 * 1024
MAX_ATTEMPTS = 3
LEASE_MINUTES = 10
class PermanentImportError(ValueError):
    """A retained packet needs user or operator attention, not a hot retry."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _fail(code: str) -> None:
    raise PermanentImportError(code)


def _clean(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _expected_pages(count: int) -> list[str]:
    if type(count) is not int or not 1 <= count <= 5:
        _fail("INVALID_PAGE_COUNT")
    return [f"page_{index}.jpg" for index in range(count)]


def _manifest_hash(objects: list[dict[str, Any]]) -> str:
    # This key order is the exact contract used by the Firebase completion
    # function when it hashes the admitted Storage inventory.
    value = [{
        "pageId": item["pageId"],
        "storageBucket": item["storageBucket"],
        "storagePath": item["storagePath"],
        "storageGeneration": item["storageGeneration"],
        "bytes": item["bytes"],
        "contentType": item["contentType"],
        "md5Hash": item["md5Hash"],
    } for item in objects]
    encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def _parse_upload_manifest(document_path: str, data: Any, expected_bucket: str) -> dict[str, Any]:
    """Validate immutable upload identity and the server-sealed Storage manifest."""
    match = PATH.fullmatch(document_path)
    if not match or type(data) is not dict:
        _fail("INVALID_UPLOAD_IDENTITY")
    route, packet_id = match.groups()
    packet_id = packet_id.lower()
    if not PACKET.fullmatch(packet_id):
        _fail("INVALID_UPLOAD_IDENTITY")
    page_ids = _expected_pages(data.get("pageCount"))
    if (
        data.get("schemaVersion") != 1
        or data.get("id") != packet_id
        or data.get("routeNumber") != route
        or not _clean(data.get("userId"))
        or data.get("source") != "app_scan"
        or data.get("status") != "ready"
        or data.get("uploadComplete") is not True
        or data.get("expectedPageIds") != page_ids
        or not isinstance(data.get("createdAt"), datetime)
    ):
        _fail("INVALID_READY_UPLOAD")
    raw_objects = data.get("storageManifest")
    if type(raw_objects) is not list or len(raw_objects) != len(page_ids):
        _fail("INVALID_STORAGE_MANIFEST")
    expected_keys = {
        "routeNumber", "packetId", "pageId", "pageIndex", "storageBucket",
        "storagePath", "storageGeneration", "bytes", "contentType", "md5Hash",
        "uploadedByUid",
    }
    objects: list[dict[str, Any]] = []
    packet_bytes = 0
    for index, raw in enumerate(raw_objects):
        if type(raw) is not dict or set(raw) != expected_keys:
            _fail("INVALID_STORAGE_MANIFEST")
        page_id = page_ids[index]
        expected_path = f"routes/{route}/accounting_purchase_uploads/{packet_id}/{page_id}"
        size = raw.get("bytes")
        if (
            raw.get("routeNumber") != route
            or raw.get("packetId") != packet_id
            or raw.get("pageId") != page_id
            or raw.get("pageIndex") != index
            or raw.get("storageBucket") != expected_bucket
            or raw.get("storagePath") != expected_path
            or not isinstance(raw.get("storageGeneration"), str)
            or not raw["storageGeneration"].isdigit()
            or type(size) is not int
            or not 1 <= size <= MAX_PAGE_BYTES
            or raw.get("contentType") != "image/jpeg"
            or not re.fullmatch(r"[A-Za-z0-9+/]{22}==", _clean(raw.get("md5Hash")))
            or raw.get("uploadedByUid") != data["userId"]
        ):
            _fail("INVALID_STORAGE_MANIFEST")
        packet_bytes += size
        objects.append(dict(raw))
    if packet_bytes > MAX_PACKET_BYTES:
        _fail("UPLOAD_TOO_LARGE")
    digest = _clean(data.get("storageManifestHash"))
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or _manifest_hash(objects) != digest:
        _fail("STORAGE_MANIFEST_HASH_MISMATCH")
    return {
        "route": route,
        "packetId": packet_id,
        "uploaderUid": data["userId"],
        "createdAt": data["createdAt"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "objects": objects,
        "manifestHash": digest,
        "clientRelease": data.get("clientRelease") if type(data.get("clientRelease")) is dict else {},
    }


def parse_ready_upload(document_path: str, data: Any, expected_bucket: str) -> dict[str, Any]:
    """Validate a server-completed packet that is still waiting for cluster import."""
    packet = _parse_upload_manifest(document_path, data, expected_bucket)
    if data.get("pendingClusterImport") is not True:
        _fail("INVALID_READY_UPLOAD")
    return packet


def parse_imported_upload(document_path: str, data: Any, expected_bucket: str) -> dict[str, Any]:
    """Validate an imported packet before removing its exact Firebase generations."""
    packet = _parse_upload_manifest(document_path, data, expected_bucket)
    if (
        data.get("pendingClusterImport") is not False
        or data.get("clusterImportStatus") != "imported"
        or data.get("clusterImportUploadId") != packet["packetId"]
        or not _clean(data.get("clusterImportOwnerUid"))
    ):
        _fail("INVALID_IMPORTED_UPLOAD")
    return packet


def actor_allowed(packet: dict[str, Any], route_data: Any, claim_data: Any,
                  user_data: Any) -> str:
    """Return the canonical owner UID after repeating the Firebase authorization."""
    if not all(type(item) is dict for item in (route_data, claim_data, user_data)):
        _fail("ACCOUNTING_ACCESS_REVOKED")
    owner_uid = _clean(route_data.get("ownerUid"))
    route = packet["route"]
    uploader = packet["uploaderUid"]
    if (
        not owner_uid
        or claim_data.get("userId") != owner_uid
    ):
        _fail("ACCOUNTING_ACCESS_REVOKED")
    profile = user_data.get("profile") if type(user_data.get("profile")) is dict else {}
    assignments = user_data.get("routeAssignments") if type(user_data.get("routeAssignments")) is dict else {}
    assignment = assignments.get(route) if type(assignments.get(route)) is dict else {}
    if uploader == owner_uid:
        owner_role = profile.get("role") in ("owner", "ownerOnly")
        owns_route = (
            profile.get("routeNumber") == route
            or profile.get("currentRoute") == route
            or type(profile.get("additionalRoutes")) is list and route in profile["additionalRoutes"]
            or assignment.get("role") == "owner"
        )
        if not owner_role or not owns_route:
            _fail("ACCOUNTING_ACCESS_REVOKED")
    elif not (
        assignment.get("role") in ("team_member", "teamMember")
        and assignment.get("verified") is True
        and assignment.get("assignedTo") == owner_uid
    ):
        _fail("ACCOUNTING_ACCESS_REVOKED")
    return owner_uid


def receipt_manifest(packet: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    files = []
    packet_uuid = UUID(packet["packetId"])
    for index, source in enumerate(sources):
        file_id = str(uuid5(packet_uuid, source["pageId"]))
        files.append({
            "id": file_id,
            "originalName": f"Scan page {index + 1}.jpg",
            "bytes": len(source["content"]),
            "sha256": source["sha256"],
            "mediaType": "image/jpeg",
            "storedName": file_id + ".jpg",
        })
    return {
        "schemaVersion": 1,
        "id": packet["packetId"],
        "route": packet["route"],
        "source": "app_scan",
        "status": "received",
        "createdAt": packet["createdAt"],
        "files": files,
        "firebaseUpload": {
            "packetId": packet["packetId"],
            "uploadedByUid": packet["uploaderUid"],
            "storageManifestHash": packet["manifestHash"],
            "clientRelease": packet["clientRelease"],
        },
    }


def save_packet(owner_uid: str, packet: dict[str, Any], sources: list[dict[str, Any]],
                transaction_factory=accounting_transaction) -> tuple[dict[str, Any], bool]:
    """Atomically retain originals and queue OCR. Replays are verification-only."""
    if len({source["sha256"] for source in sources}) != len(sources):
        _fail("DUPLICATE_RECEIPT_ORIGINAL")
    manifest = receipt_manifest(packet, sources)
    key = "receipt-upload:" + packet["packetId"]
    with transaction_factory(owner_uid, packet["route"]) as store:
        existing = store.get(key)
        if existing is not None:
            if existing != manifest:
                _fail("CLUSTER_PACKET_IDENTITY_CONFLICT")
            store.queue_receipt(packet["packetId"])
            return manifest, True
        for file in manifest["files"]:
            if store.has_original("receipt", file["sha256"]):
                _fail("DUPLICATE_RECEIPT_ORIGINAL")
        for file, source in zip(manifest["files"], sources):
            store.add_original("receipt", packet["packetId"], file, source["content"])
        store.put(key, manifest)
        store.queue_receipt(packet["packetId"])
    return manifest, False


def verify_saved_packet(owner_uid: str, packet: dict[str, Any], manifest: dict[str, Any],
                        transaction_factory=accounting_transaction) -> None:
    """Read committed originals through the normal integrity-checking path.

    The phone removes its retained photos only after Firebase reports the cluster
    import as complete.  Saving in one transaction and then reading in a new one
    makes that response attest to committed, retrievable bytes rather than only a
    successful INSERT call.
    """
    try:
        with transaction_factory(owner_uid, packet["route"]) as store:
            if store.get("receipt-upload:" + packet["packetId"]) != manifest:
                raise RuntimeError("Saved purchase manifest failed verification")
            for file in manifest["files"]:
                source = store.original("receipt", manifest["id"], file["id"])
                if (
                    source is None
                    or source.get("sha256") != file["sha256"]
                    or source.get("mediaType") != file["mediaType"]
                    or source.get("originalName") != file["originalName"]
                    or len(source.get("content", b"")) != file["bytes"]
                ):
                    raise RuntimeError("Saved purchase original failed verification")
    except Exception as error:
        if isinstance(error, RuntimeError) and str(error).startswith("Saved purchase"):
            raise
        raise RuntimeError("Saved purchase original failed verification") from error


def _snapshot_data(snapshot: Any) -> dict[str, Any]:
    return snapshot.to_dict() if snapshot is not None and snapshot.exists else {}


def authorize(db: Any, packet: dict[str, Any]) -> str:
    route_ref = db.collection("routes").document(packet["route"])
    claim_ref = db.collection("routeNumbers").document(packet["route"])
    user_ref = db.collection("users").document(packet["uploaderUid"])
    # Firestore does not promise get_all result order. Bind every result back to
    # its exact requested path before making an authorization decision.
    snapshots = {snapshot.reference.path: snapshot for snapshot in db.get_all([route_ref, claim_ref, user_ref])}
    route_data = _snapshot_data(snapshots.get(route_ref.path))
    owner_uid = _clean(route_data.get("ownerUid"))
    return actor_allowed(
        packet,
        route_data,
        _snapshot_data(snapshots.get(claim_ref.path)),
        _snapshot_data(snapshots.get(user_ref.path)),
    )


def download_sources(packet: dict[str, Any], bucket: Any) -> list[dict[str, Any]]:
    sources = []
    for item in packet["objects"]:
        generation = int(item["storageGeneration"])
        blob = bucket.blob(item["storagePath"], generation=generation)
        blob.reload(if_generation_match=generation, timeout=30)
        metadata = blob.metadata or {}
        if (
            str(blob.generation) != item["storageGeneration"]
            or blob.size != item["bytes"]
            or blob.content_type != "image/jpeg"
            or blob.md5_hash != item["md5Hash"]
            or metadata.get("uploadedByUid") != packet["uploaderUid"]
            or metadata.get("routeNumber") != packet["route"]
            or metadata.get("packetId") != packet["packetId"]
            or metadata.get("pageId") != item["pageId"]
            or metadata.get("source") != "app_scan"
            or metadata.get("schemaVersion") != "1"
        ):
            _fail("STORAGE_OBJECT_IDENTITY_MISMATCH")
        content = blob.download_as_bytes(if_generation_match=generation, timeout=30)
        if len(content) != item["bytes"] or detect_media(content) != ("image/jpeg", "jpg"):
            _fail("STORAGE_OBJECT_CONTENT_INVALID")
        sources.append({
            "pageId": item["pageId"],
            "content": content,
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    return sources


def schedule_firebase_cleanup(db: Any, reference: Any, expected_bucket: str) -> bool:
    """Delegate exact-generation deletion to the Firebase-side cleanup worker.

    The cluster attests only after its immutable originals are committed. It
    does not receive Storage delete permission; Firebase revalidates the sealed
    manifest and performs the bounded deletion under its own Admin identity.
    """
    data = _snapshot_data(reference.get())
    try:
        packet = parse_imported_upload(reference.path, data, expected_bucket)
    except PermanentImportError:
        logger.warning("Accounting app scan cleanup identity rejected", extra={
            "packet_path": reference.path,
        })
        return False
    transaction = db.transaction()

    @firestore.transactional
    def schedule(current):
        snapshot = reference.get(transaction=current)
        data = _snapshot_data(snapshot)
        if (
            data.get("clusterImportStatus") != "imported"
            or data.get("pendingClusterImport") is not False
            or data.get("clusterImportUploadId") != packet["packetId"]
            or data.get("storageManifestHash") != packet["manifestHash"]
        ):
            return False
        if data.get("firebaseStorageCleanupStatus") == "deleted":
            return data.get("firebaseStorageDeletedObjects") == len(packet["objects"])
        if data.get("firebaseStorageCleanupStatus") == "scheduled":
            return isinstance(data.get("firebaseStorageCleanupAfter"), datetime)
        now = datetime.now(timezone.utc)
        current.update(reference, {
            "firebaseStorageCleanupStatus": "scheduled",
            "firebaseStorageCleanupAfter": now,
            "firebaseStorageCleanupScheduledAt": now,
            "firebaseStorageCleanupAttempts": 0,
            "firebaseStorageCleanupUpdatedAt": now,
            "firebaseStorageCleanupErrorCode": firestore.DELETE_FIELD,
            "firebaseStorageDeletedObjects": firestore.DELETE_FIELD,
            "firebaseStorageDeletedAt": firestore.DELETE_FIELD,
        })
        return True

    return schedule(transaction)


def _claim(db: Any, reference: Any) -> dict[str, Any] | None:
    transaction = db.transaction()

    @firestore.transactional
    def claim(current):
        snapshot = reference.get(transaction=current)
        data = _snapshot_data(snapshot)
        now = datetime.now(timezone.utc)
        lease_expires = data.get("clusterImportLeaseExpiresAt")
        active = data.get("clusterImportStatus") == "importing" and isinstance(lease_expires, datetime) and lease_expires > now
        attempts = data.get("clusterImportAttempts", 0)
        if (
            data.get("pendingClusterImport") is not True
            or data.get("status") != "ready"
            or active
            or type(attempts) is not int
            or attempts >= MAX_ATTEMPTS
        ):
            return None
        lease = str(uuid4())
        current.update(reference, {
            "clusterImportStatus": "importing",
            "clusterImportAttempts": attempts + 1,
            "clusterImportLeaseToken": lease,
            "clusterImportLeaseExpiresAt": now + timedelta(minutes=LEASE_MINUTES),
            "clusterImportUpdatedAt": now,
        })
        return {"lease": lease, "attempt": attempts + 1, "data": data}

    return claim(transaction)


def _finish(db: Any, reference: Any, lease: str, values: dict[str, Any]) -> bool:
    transaction = db.transaction()

    @firestore.transactional
    def finish(current):
        snapshot = reference.get(transaction=current)
        data = _snapshot_data(snapshot)
        if data.get("clusterImportStatus") != "importing" or data.get("clusterImportLeaseToken") != lease:
            return False
        current.update(reference, {
            **values,
            "clusterImportLeaseToken": firestore.DELETE_FIELD,
            "clusterImportLeaseExpiresAt": firestore.DELETE_FIELD,
            "clusterImportUpdatedAt": datetime.now(timezone.utc),
        })
        return True

    return finish(transaction)


def process_reference(db: Any, reference: Any, expected_bucket: str, bucket_factory=None) -> bool:
    claimed = _claim(db, reference)
    if claimed is None:
        return False
    lease, attempt, data = claimed["lease"], claimed["attempt"], claimed["data"]
    try:
        packet = parse_ready_upload(reference.path, data, expected_bucket)
        owner_uid = authorize(db, packet)
        bucket = (bucket_factory or (lambda name: storage.bucket(name=name, app=get_firebase_app())))(expected_bucket)
        sources = download_sources(packet, bucket)
        manifest, replay = save_packet(owner_uid, packet, sources)
        verify_saved_packet(owner_uid, packet, manifest)
        imported_at = datetime.now(timezone.utc)
        if not _finish(db, reference, lease, {
            "pendingClusterImport": False,
            "clusterImportStatus": "imported",
            "clusterImportUploadId": manifest["id"],
            "clusterImportOwnerUid": owner_uid,
            "clusterImportedAt": imported_at,
            "clusterImportReplay": replay,
            "clusterImportErrorCode": firestore.DELETE_FIELD,
            "firebaseStorageCleanupStatus": "scheduled",
            "firebaseStorageCleanupAfter": imported_at,
            "firebaseStorageCleanupScheduledAt": imported_at,
            "firebaseStorageCleanupAttempts": 0,
            "firebaseStorageCleanupErrorCode": firestore.DELETE_FIELD,
        }):
            raise RuntimeError("Import lease changed before completion")
        logger.info("Accounting app scan imported", extra={
            "route_number": packet["route"], "packet_id": packet["packetId"],
            "page_count": len(packet["objects"]), "replay": replay,
        })
        return True
    except PermanentImportError as error:
        _finish(db, reference, lease, {
            "pendingClusterImport": False,
            "clusterImportStatus": "needs_attention",
            "clusterImportErrorCode": error.code,
        })
        logger.warning("Accounting app scan needs attention", extra={
            "packet_path": reference.path, "reason": error.code,
        })
        return False
    except Exception:
        terminal = attempt >= MAX_ATTEMPTS
        _finish(db, reference, lease, {
            "pendingClusterImport": not terminal,
            "clusterImportStatus": "needs_attention" if terminal else "queued",
            "clusterImportErrorCode": "IMPORT_RETRY_EXHAUSTED" if terminal else firestore.DELETE_FIELD,
        })
        logger.warning("Accounting app scan import paused; retained evidence remains available",
                       extra={"packet_path": reference.path, "attempt": attempt})
        return False



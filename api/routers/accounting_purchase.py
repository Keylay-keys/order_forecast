"""Authenticated handoff from Firebase purchase intake to hosted Accounting.

The phone uploads first to its route-scoped Firebase packet. Once the callable
completion function freezes the exact Storage inventory, the same signed-in
user asks the cluster to import that one packet. The cluster repeats route
authorization, pins every Storage generation, and queues the existing receipt
OCR workflow. No client-supplied bytes or Storage paths are trusted here.
"""
from __future__ import annotations

from datetime import datetime
import json
import os
import re

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from ..accounting_domain import fail
from ..accounting_evidence import saved_receipt_integrity
from ..accounting_purchase_import_worker import (
    PermanentImportError,
    actor_allowed,
    process_reference,
    schedule_firebase_cleanup,
)
from ..accounting_store import accounting_transaction
from ..dependencies import get_firestore, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history
from .accounting import HEADERS, bounded_body


router = APIRouter()
PACKET = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
HASH = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_BUCKET = "routespark-1f47d.firebasestorage.app"


def _data(snapshot):
    return snapshot.to_dict() if snapshot is not None and snapshot.exists else {}


def _request_identity(raw: bytes) -> tuple[str, str]:
    try:
        body = json.loads(raw)
    except (UnicodeError, ValueError):
        fail("Invalid purchase import request.")
    if type(body) is not dict or set(body) != {"packetId", "storageManifestHash"}:
        fail("Invalid purchase import request.")
    packet_id = body.get("packetId")
    digest = body.get("storageManifestHash")
    if not isinstance(packet_id, str) or not PACKET.fullmatch(packet_id):
        fail("Invalid purchase import request.")
    if not isinstance(digest, str) or not HASH.fullmatch(digest):
        fail("Invalid purchase import request.")
    return packet_id, digest


def _verified_result(data: dict, *, route: str, packet_id: str, uid: str, digest: str):
    if (
        data.get("schemaVersion") != 1
        or data.get("id") != packet_id
        or data.get("routeNumber") != route
        or data.get("userId") != uid
        or data.get("source") != "app_scan"
        or data.get("storageManifestHash") != digest
    ):
        raise HTTPException(403, "Purchase upload access denied")
    cleanup_scheduled = (
        data.get("firebaseStorageCleanupStatus") == "scheduled"
        and isinstance(data.get("firebaseStorageCleanupAfter"), datetime)
    )
    cleanup_deleted = (
        data.get("firebaseStorageCleanupStatus") == "deleted"
        and type(data.get("pageCount")) is int
        and type(data.get("firebaseStorageDeletedObjects")) is int
        and data["firebaseStorageDeletedObjects"] == data["pageCount"]
    )
    if (
        data.get("clusterImportStatus") == "imported"
        and data.get("pendingClusterImport") is False
        and data.get("clusterImportUploadId") == packet_id
        and isinstance(data.get("clusterImportOwnerUid"), str)
        and data.get("clusterImportOwnerUid")
        and (cleanup_scheduled or cleanup_deleted)
    ):
        return {
            "status": "imported",
            "route": route,
            "packetId": packet_id,
            "uploadId": packet_id,
        }
    return None


def _status_owner(db, data: dict, *, route: str, packet_id: str, uid: str) -> str:
    """Recheck the exact uploader and current route authority for phone polling."""
    if (
        data.get("schemaVersion") != 1
        or data.get("id") != packet_id
        or data.get("routeNumber") != route
        or data.get("userId") != uid
        or data.get("source") != "app_scan"
        or data.get("clusterImportStatus") != "imported"
        or data.get("pendingClusterImport") is not False
        or data.get("clusterImportUploadId") != packet_id
    ):
        raise HTTPException(403, "Purchase upload access denied")
    route_ref = db.collection("routes").document(route)
    claim_ref = db.collection("routeNumbers").document(route)
    user_ref = db.collection("users").document(uid)
    snapshots = {snapshot.reference.path: snapshot for snapshot in db.get_all([
        route_ref, claim_ref, user_ref,
    ])}
    route_data = _data(snapshots.get(route_ref.path))
    try:
        owner_uid = actor_allowed(
            {"route": route, "uploaderUid": uid},
            route_data,
            _data(snapshots.get(claim_ref.path)),
            _data(snapshots.get(user_ref.path)),
        )
    except PermanentImportError:
        raise HTTPException(403, "Purchase upload access denied") from None
    if data.get("clusterImportOwnerUid") != owner_uid:
        raise HTTPException(403, "Purchase upload access denied")
    return owner_uid


def _receipt_source_status(owner_uid: str, route: str, packet_id: str,
                           transaction_factory=accounting_transaction) -> dict:
    with transaction_factory(owner_uid, route) as store:
        manifest = store.get("receipt-upload:" + packet_id)
        if manifest is None:
            return {"status": "needs_rescan", "reason": "stored_original_missing"}
        integrity = saved_receipt_integrity(store, packet_id)
        if integrity:
            return {"status": "needs_rescan", "reason": integrity["reason"]}
        job = store.receipt_jobs().get(packet_id)
        return {"status": "processing" if job in (None, "queued") else "verified"}


@router.get("/business/accounting-purchase-source-status")
@rate_limit_history
async def accounting_purchase_source_status(
    request: Request,
    route: str = Query(..., pattern=r"^[0-9]{6}$"),
    packetId: str = Query(..., pattern=PACKET.pattern),
    decoded_token: dict = Depends(verify_firebase_token),
    db=Depends(get_firestore),
):
    """Let only the original phone uploader wait for cluster source verification."""
    uid = decoded_token.get("uid")
    if not isinstance(uid, str) or not uid:
        raise HTTPException(401, "Authentication required")
    reference = db.collection("routes").document(route).collection(
        "accounting_purchase_uploads"
    ).document(packetId)
    data = _data(await run_in_threadpool(reference.get))
    if not data:
        raise HTTPException(404, "Purchase upload not found")
    try:
        owner_uid = await run_in_threadpool(
            _status_owner, db, data, route=route, packet_id=packetId, uid=uid,
        )
        state = await run_in_threadpool(_receipt_source_status, owner_uid, route, packetId)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            503, "Purchase source verification is still pending.", headers={"Retry-After": "5"},
        ) from None
    result = {"route": route, "packetId": packetId, **state}
    headers = dict(HEADERS)
    if state["status"] == "processing":
        headers["Retry-After"] = "5"
    return Response(json.dumps(result), media_type="application/json", headers=headers)


@router.post("/business/accounting-purchase-import")
@rate_limit_history
async def import_accounting_purchase(
    request: Request,
    route: str = Query(..., pattern=r"^[0-9]{6}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db=Depends(get_firestore),
):
    if not request.headers.get("content-type", "").lower().startswith("application/json"):
        fail("JSON is required.", 415)
    packet_id, digest = _request_identity(await bounded_body(request, 2048))
    uid = decoded_token.get("uid")
    if not isinstance(uid, str) or not uid:
        raise HTTPException(401, "Authentication required")
    reference = db.collection("routes").document(route).collection(
        "accounting_purchase_uploads"
    ).document(packet_id)
    before = await run_in_threadpool(reference.get)
    data = _data(before)
    if not data:
        raise HTTPException(404, "Purchase upload not found")
    replay = _verified_result(data, route=route, packet_id=packet_id, uid=uid, digest=digest)
    if replay:
        return Response(json.dumps(replay), media_type="application/json", headers=HEADERS)
    bucket = os.environ.get("FIREBASE_STORAGE_BUCKET", "").strip() or DEFAULT_BUCKET
    already_imported = (
        data.get("clusterImportStatus") == "imported"
        and data.get("pendingClusterImport") is False
        and data.get("clusterImportUploadId") == packet_id
    )
    if already_imported:
        await run_in_threadpool(schedule_firebase_cleanup, db, reference, bucket)
        after_cleanup = _data(await run_in_threadpool(reference.get))
        result = _verified_result(
            after_cleanup, route=route, packet_id=packet_id, uid=uid, digest=digest,
        )
        if result:
            return Response(json.dumps(result), media_type="application/json", headers=HEADERS)
        raise HTTPException(
            503,
            "Purchase was saved; secure transfer cleanup could not be scheduled.",
            headers={"Retry-After": "5"},
        )
    if (
        data.get("status") != "ready"
        or data.get("uploadComplete") is not True
        or data.get("pendingClusterImport") is not True
    ):
        raise HTTPException(409, "Purchase upload is not ready for import")

    await run_in_threadpool(process_reference, db, reference, bucket)
    after = _data(await run_in_threadpool(reference.get))
    result = _verified_result(after, route=route, packet_id=packet_id, uid=uid, digest=digest)
    if result is None:
        if after.get("clusterImportStatus") == "needs_attention":
            raise HTTPException(422, "This purchase could not be imported. The saved images were retained.")
        raise HTTPException(503, "Purchase import is still pending. Try again shortly.", headers={"Retry-After": "5"})
    return Response(json.dumps(result), media_type="application/json", headers=HEADERS, status_code=201)

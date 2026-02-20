"""Archive export queue router (owner-only).

Implements Phase 2 API surface for PCF archive exports:
- request export job
- list export jobs
- cancel queued job
- generate download link for ready artifact
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from firebase_admin import storage
from google.cloud import firestore
from pydantic import BaseModel, Field

from ..dependencies import get_firebase_app, get_firestore, require_route_access, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse

router = APIRouter()
logger = logging.getLogger("api.archive_exports")

EXPORT_COLLECTION = "archiveExports"
MAX_RANGE_DAYS = 31
MAX_REQUESTS_PER_DAY = 3
MAX_ROUTE_ACTIVE_QUEUE_DEPTH = 3
SIGNED_URL_TTL_SECONDS = 60 * 60


class ArchiveExportRequest(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")
    fromDate: date
    toDate: date
    format: Literal["zip"] = "zip"


class ArchiveExportListResponse(BaseModel):
    ok: bool
    jobs: List[Dict[str, Any]]


class ArchiveExportMutationResponse(BaseModel):
    ok: bool
    reused: bool = False
    job: Dict[str, Any]


class ArchiveExportLinkResponse(BaseModel):
    ok: bool
    exportId: str
    url: str
    expiresAtMs: int


def _normalize_route_number(value: Any) -> str:
    route = str(value or "").strip()
    return route if route.isdigit() and len(route) <= 10 else ""


def _is_owner_for_route(user_data: Dict[str, Any], route_number: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if (
        str(profile.get("role") or "").strip() == "owner"
        and _normalize_route_number(profile.get("routeNumber")) == route_number
    ):
        return True
    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "owner"


def _to_epoch_millis(value: Any) -> Optional[int]:
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, dict):
        seconds = value.get("seconds") or value.get("_seconds")
        nanos = value.get("nanoseconds") or value.get("_nanoseconds") or value.get("nanos") or 0
        if seconds is None:
            return None
        try:
            return int((float(seconds) + float(nanos) / 1_000_000_000) * 1000)
        except Exception:
            return None
    return None


def _date_from_firestore_timestamp(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, dict):
        ms = _to_epoch_millis(value)
        if ms is None:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()
    if hasattr(value, "date"):
        try:
            return value.date()
        except Exception:
            return None
    if isinstance(value, datetime):
        return value.date()
    return None


def _assert_owner_access(user_data: Dict[str, Any], route_number: str) -> None:
    if not _is_owner_for_route(user_data, route_number):
        raise HTTPException(status_code=403, detail="Owner access required")


def _serialize_export_job(
    doc: firestore.DocumentSnapshot,
    *,
    queue_position_by_doc_id: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    artifact = data.get("artifact") if isinstance(data.get("artifact"), dict) else {}
    status = str(data.get("status") or "queued").strip().lower()

    created_at_ms = _to_epoch_millis(data.get("createdAt"))
    updated_at_ms = _to_epoch_millis(data.get("updatedAt"))
    ready_at_ms = _to_epoch_millis(data.get("readyAt"))

    artifact_expires_ms = (
        data.get("artifactExpiresAtMs")
        if isinstance(data.get("artifactExpiresAtMs"), int)
        else _to_epoch_millis(artifact.get("expiresAt"))
    )

    job = {
        "exportId": doc.id,
        "routeNumber": str(data.get("routeNumber") or ""),
        "fromDate": str(data.get("fromDate") or ""),
        "toDate": str(data.get("toDate") or ""),
        "format": str(data.get("format") or "zip"),
        "status": status,
        "attemptCount": int(data.get("attemptCount") or 0),
        "maxAttempts": int(data.get("maxAttempts") or 3),
        "createdAtMs": created_at_ms,
        "updatedAtMs": updated_at_ms,
        "readyAtMs": ready_at_ms,
        "expiresAtMs": artifact_expires_ms,
        "artifactExpiresAtMs": artifact_expires_ms,
        "artifactParts": artifact.get("parts") if isinstance(artifact.get("parts"), list) else [],
        "errorCode": str(data.get("errorCode") or "") or None,
        "errorMessage": str(data.get("errorMessage") or "") or None,
    }

    if queue_position_by_doc_id and status == "queued":
        queue_position = queue_position_by_doc_id.get(doc.id)
        if queue_position is not None:
            job["queuePosition"] = queue_position

    return job


def _build_queue_positions(docs: List[firestore.DocumentSnapshot]) -> Dict[str, int]:
    queued = []
    for doc in docs:
        data = doc.to_dict() or {}
        if str(data.get("status") or "").strip().lower() != "queued":
            continue
        created_at_ms = _to_epoch_millis(data.get("createdAt")) or 0
        queued.append((doc.id, created_at_ms))

    queued.sort(key=lambda row: row[1])
    return {doc_id: idx + 1 for idx, (doc_id, _) in enumerate(queued)}


def _start_of_utc_day_millis(now: datetime) -> int:
    start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000)


def _validate_export_date_range(
    *,
    db: firestore.Client,
    route_number: str,
    from_date: date,
    to_date: date,
) -> None:
    if from_date > to_date:
        raise HTTPException(status_code=422, detail="INVALID_DATE_RANGE")

    inclusive_days = (to_date - from_date).days + 1
    if inclusive_days > MAX_RANGE_DAYS:
        raise HTTPException(status_code=422, detail="EXPORT_RANGE_EXCEEDS_MAX_31_DAYS")

    today = datetime.now(timezone.utc).date()
    if from_date > today or to_date > today:
        raise HTTPException(status_code=422, detail="INVALID_DATE_RANGE_FUTURE")

    route_doc = db.collection("routes").document(route_number).get()
    if route_doc.exists:
        route_data = route_doc.to_dict() or {}
        route_created_date = _date_from_firestore_timestamp(route_data.get("createdAt"))
        if route_created_date and from_date < route_created_date:
            raise HTTPException(status_code=422, detail="DATE_BEFORE_ROUTE_START")


@router.post(
    "/archive/exports/request",
    response_model=ArchiveExportMutationResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def request_archive_export(
    request: Request,
    body: ArchiveExportRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    user_data = await require_route_access(body.route, decoded_token, db)
    _assert_owner_access(user_data, body.route)

    _validate_export_date_range(
        db=db,
        route_number=body.route,
        from_date=body.fromDate,
        to_date=body.toDate,
    )

    route_docs = list(db.collection(EXPORT_COLLECTION).where("routeNumber", "==", body.route).stream())

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)

    # Dedupe by route + date range + format for active and valid-ready jobs.
    for doc in route_docs:
        data = doc.to_dict() or {}
        status = str(data.get("status") or "").strip().lower()
        if status not in {"queued", "processing", "ready", "ready_partial"}:
            continue
        if str(data.get("fromDate") or "") != body.fromDate.isoformat():
            continue
        if str(data.get("toDate") or "") != body.toDate.isoformat():
            continue
        if str(data.get("format") or "zip") != body.format:
            continue

        if status in {"ready", "ready_partial"}:
            artifact = data.get("artifact") if isinstance(data.get("artifact"), dict) else {}
            artifact_expires_ms = (
                data.get("artifactExpiresAtMs")
                if isinstance(data.get("artifactExpiresAtMs"), int)
                else _to_epoch_millis(artifact.get("expiresAt"))
            )
            if artifact_expires_ms is not None and artifact_expires_ms <= now_ms:
                continue

        queue_positions = _build_queue_positions(route_docs)
        return {
            "ok": True,
            "reused": True,
            "job": _serialize_export_job(doc, queue_position_by_doc_id=queue_positions),
        }

    requester_uid = str(decoded_token.get("uid") or "").strip()
    day_start_ms = _start_of_utc_day_millis(now)

    requests_today = 0
    active_queue_depth = 0
    for doc in route_docs:
        data = doc.to_dict() or {}
        status = str(data.get("status") or "").strip().lower()
        created_ms = _to_epoch_millis(data.get("createdAt")) or 0

        if status in {"queued", "processing"}:
            active_queue_depth += 1

        if (
            str(data.get("requestedByUid") or "").strip() == requester_uid
            and created_ms >= day_start_ms
        ):
            requests_today += 1

    if requests_today >= MAX_REQUESTS_PER_DAY:
        raise HTTPException(status_code=429, detail="EXPORT_DAILY_LIMIT_REACHED")

    if active_queue_depth >= MAX_ROUTE_ACTIVE_QUEUE_DEPTH:
        raise HTTPException(status_code=409, detail="ROUTE_EXPORT_QUEUE_FULL")

    queued_count = sum(
        1
        for doc in route_docs
        if str((doc.to_dict() or {}).get("status") or "").strip().lower() == "queued"
    )
    queue_position = queued_count + 1

    user_settings = user_data.get("userSettings", {}) if isinstance(user_data.get("userSettings"), dict) else {}
    database_settings = user_settings.get("database", {}) if isinstance(user_settings.get("database"), dict) else {}

    export_id = f"exp_{uuid4().hex[:24]}"
    export_ref = db.collection(EXPORT_COLLECTION).document(export_id)
    export_ref.set(
        {
            "exportId": export_id,
            "routeNumber": body.route,
            "requestedByUid": requester_uid,
            "requestedByEmail": str((user_data.get("profile", {}) or {}).get("email") or "").strip() or None,
            "fromDate": body.fromDate.isoformat(),
            "toDate": body.toDate.isoformat(),
            "format": body.format,
            "status": "queued",
            "attemptCount": 0,
            "maxAttempts": 3,
            "queue": {
                "key": f"{body.route}:{body.fromDate.isoformat()}:{body.toDate.isoformat()}:{body.format}",
                "positionHint": queue_position,
            },
            "settingsSnapshot": {
                "archivedPcfRetentionDays": int(database_settings.get("archivedPcfRetentionDays") or 90),
                "archivedPcfEndOfLifeAction": str(database_settings.get("archivedPcfEndOfLifeAction") or "allow_export_then_delete"),
            },
            "artifact": {
                "storagePath": None,
                "parts": [],
                "expiresAt": None,
                "sizeBytes": 0,
            },
            "errorCode": None,
            "errorMessage": None,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
    )

    created_doc = export_ref.get()
    queue_positions = _build_queue_positions(route_docs + [created_doc])
    return {
        "ok": True,
        "reused": False,
        "job": _serialize_export_job(created_doc, queue_position_by_doc_id=queue_positions),
    }


@router.get(
    "/archive/exports",
    response_model=ArchiveExportListResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def list_archive_exports(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    limit: int = Query(default=50, ge=1, le=100),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    user_data = await require_route_access(route, decoded_token, db)
    _assert_owner_access(user_data, route)

    docs = list(db.collection(EXPORT_COLLECTION).where("routeNumber", "==", route).stream())
    docs.sort(
        key=lambda d: _to_epoch_millis((d.to_dict() or {}).get("createdAt")) or 0,
        reverse=True,
    )
    queue_positions = _build_queue_positions(docs)
    jobs = [
        _serialize_export_job(doc, queue_position_by_doc_id=queue_positions)
        for doc in docs[:limit]
    ]
    return {"ok": True, "jobs": jobs}


@router.post(
    "/archive/exports/{export_id}/cancel",
    response_model=ArchiveExportMutationResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def cancel_archive_export(
    request: Request,
    export_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    export_ref = db.collection(EXPORT_COLLECTION).document(export_id)
    export_doc = export_ref.get()
    if not export_doc.exists:
        raise HTTPException(status_code=404, detail="EXPORT_NOT_FOUND")

    data = export_doc.to_dict() or {}
    route_number = _normalize_route_number(data.get("routeNumber"))
    if not route_number:
        raise HTTPException(status_code=409, detail="EXPORT_ROUTE_INVALID")

    user_data = await require_route_access(route_number, decoded_token, db)
    _assert_owner_access(user_data, route_number)

    status = str(data.get("status") or "").strip().lower()
    if status != "queued":
        raise HTTPException(status_code=409, detail="EXPORT_CANCEL_ONLY_QUEUED")

    export_ref.update(
        {
            "status": "failed",
            "errorCode": "CANCELED_BY_OWNER",
            "errorMessage": "Export request canceled by route owner",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
    )

    updated_doc = export_ref.get()
    queue_docs = list(db.collection(EXPORT_COLLECTION).where("routeNumber", "==", route_number).stream())
    queue_positions = _build_queue_positions(queue_docs)
    return {
        "ok": True,
        "reused": False,
        "job": _serialize_export_job(updated_doc, queue_position_by_doc_id=queue_positions),
    }


@router.post(
    "/archive/exports/{export_id}/link",
    response_model=ArchiveExportLinkResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def generate_archive_export_link(
    request: Request,
    export_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    export_ref = db.collection(EXPORT_COLLECTION).document(export_id)
    export_doc = export_ref.get()
    if not export_doc.exists:
        raise HTTPException(status_code=404, detail="EXPORT_NOT_FOUND")

    data = export_doc.to_dict() or {}
    route_number = _normalize_route_number(data.get("routeNumber"))
    if not route_number:
        raise HTTPException(status_code=409, detail="EXPORT_ROUTE_INVALID")

    user_data = await require_route_access(route_number, decoded_token, db)
    _assert_owner_access(user_data, route_number)

    status = str(data.get("status") or "").strip().lower()
    if status not in {"ready", "ready_partial"}:
        raise HTTPException(status_code=409, detail="EXPORT_NOT_READY")

    artifact = data.get("artifact") if isinstance(data.get("artifact"), dict) else {}
    artifact_expires_ms = (
        data.get("artifactExpiresAtMs")
        if isinstance(data.get("artifactExpiresAtMs"), int)
        else _to_epoch_millis(artifact.get("expiresAt"))
    )
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if artifact_expires_ms is not None and artifact_expires_ms <= now_ms:
        export_ref.update({"status": "expired", "updatedAt": firestore.SERVER_TIMESTAMP})
        raise HTTPException(status_code=410, detail="EXPORT_ARTIFACT_EXPIRED")

    storage_path = str(
        data.get("artifactStoragePath")
        or artifact.get("storagePath")
        or ""
    ).strip()
    if not storage_path:
        raise HTTPException(status_code=409, detail="EXPORT_ARTIFACT_NOT_READY")

    bucket_name = (
        str(os.environ.get("FIREBASE_STORAGE_BUCKET") or "").strip()
        or str((get_firebase_app().options or {}).get("storageBucket") or "").strip()
    )
    if not bucket_name:
        raise HTTPException(status_code=500, detail="STORAGE_BUCKET_NOT_CONFIGURED")

    try:
        bucket = storage.bucket(name=bucket_name, app=get_firebase_app())
        blob = bucket.blob(storage_path)
        if not blob.exists():
            raise HTTPException(status_code=404, detail="EXPORT_ARTIFACT_MISSING")

        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=SIGNED_URL_TTL_SECONDS),
            method="GET",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to generate signed URL for export %s: %s", export_id, exc)
        raise HTTPException(status_code=500, detail="EXPORT_LINK_GENERATION_FAILED")

    expires_at_ms = now_ms + SIGNED_URL_TTL_SECONDS * 1000
    export_ref.update({
        "lastDownloadLinkGeneratedAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    return {
        "ok": True,
        "exportId": export_id,
        "url": signed_url,
        "expiresAtMs": expires_at_ms,
    }

"""POS invoice router - active + archived access, archive job enqueueing, no hard delete."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import logging
import os
import re
import tempfile
import zipfile

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from starlette.background import BackgroundTask
from firebase_admin import storage
from google.cloud import firestore

from ..dependencies import (
    get_firebase_app,
    verify_firebase_token,
    require_route_access,
    get_firestore,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write

router = APIRouter()
logger = logging.getLogger("api.pos")

ACTIVE_COLLECTION = "pos_invoices"
ARCHIVED_COLLECTION = "archived_pos_invoices"
JOB_COLLECTION = "posArchiveJobs"
STATUS_VALUES = {
    "active",
    "archived",
    "archived_server",
    "archive_requested",
    "all",
}

POS_ARCHIVE_BASE = Path(os.environ.get("POS_ARCHIVE_PATH", "/mnt/archive/pos"))
POS_HOT_TIER_DAYS = int(os.environ.get("POS_ARCHIVE_HOT_TIER_DAYS", "60"))
POS_RETENTION_YEARS = int(os.environ.get("POS_ARCHIVE_RETENTION_YEARS", "3"))
STORAGE_BUCKET = os.environ.get("FIREBASE_STORAGE_BUCKET", "routespark-1f47d.firebasestorage.app")


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value / 1000).isoformat() + "Z"
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "timestamp"):
        try:
            return datetime.utcfromtimestamp(value.timestamp()).isoformat() + "Z"
        except Exception:
            return str(value)
    return str(value)


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    return None


def _normalize_invoice(doc: firestore.DocumentSnapshot, *, collection_name: str) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    data["id"] = data.get("id") or doc.id
    data["storageTier"] = "server" if collection_name == ARCHIVED_COLLECTION else "firebase"
    data["sourceCollection"] = collection_name
    for field in ("createdAt", "archivedAt", "firebaseDeletedAt", "retentionUntil"):
        if field in data:
            data[field] = _to_iso(data.get(field))
    return data


def _validate_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as exc:
        raise HTTPException(400, "Invalid date format (YYYY-MM-DD)") from exc


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _matches_filters(
    invoice: Dict[str, Any],
    store: Optional[str],
    invoice_number: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> bool:
    if store and _normalize_text(store) not in _normalize_text(invoice.get("store")):
        return False
    if invoice_number and (invoice.get("invoiceNumber") or "").lower().find(invoice_number.lower()) == -1:
        return False
    invoice_date = invoice.get("date")
    if invoice_date:
        if start_date and invoice_date < start_date:
            return False
        if end_date and invoice_date > end_date:
            return False
    elif start_date or end_date:
        return False
    return True


def _is_admin(user_data: Dict[str, Any]) -> bool:
    role = (user_data.get("profile") or {}).get("role")
    return role in {"owner", "admin"}


def _route_collections(db: firestore.Client, route_number: str) -> Tuple[firestore.CollectionReference, firestore.CollectionReference]:
    route_doc = db.collection("routes").document(route_number)
    return (
        route_doc.collection(ACTIVE_COLLECTION),
        route_doc.collection(ARCHIVED_COLLECTION),
    )


def _query_collection(
    collection_ref: firestore.CollectionReference,
    *,
    status: Optional[str],
    limit: int,
    cursor: Optional[str],
) -> List[firestore.DocumentSnapshot]:
    query: firestore.Query = collection_ref.order_by("createdAt", direction=firestore.Query.DESCENDING)
    if status and status not in {"all", "archived"}:
        query = query.where("status", "==", status)

    if cursor:
        cursor_doc = collection_ref.document(cursor).get()
        if cursor_doc.exists:
            query = query.start_after(cursor_doc)

    return list(query.limit(limit).stream())


def _list_active_docs(
    db: firestore.Client,
    route: str,
    *,
    status: Optional[str],
    limit: int,
    cursor: Optional[str],
) -> List[firestore.DocumentSnapshot]:
    active_ref, _ = _route_collections(db, route)
    if status in {"archived_server"}:
        return []
    return _query_collection(active_ref, status=status, limit=limit, cursor=cursor)


def _list_archived_docs(
    db: firestore.Client,
    route: str,
    *,
    status: Optional[str],
    limit: int,
) -> List[firestore.DocumentSnapshot]:
    _, archived_ref = _route_collections(db, route)
    if status == "active":
        return []
    archived_status = "archived_server" if status in {None, "archived", "archived_server", "all"} else status
    return _query_collection(archived_ref, status=archived_status, limit=limit, cursor=None)


def _merged_items(
    *,
    route: str,
    db: firestore.Client,
    status: Optional[str],
    store: Optional[str],
    invoice_number: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    limit: int,
    cursor: Optional[str],
) -> List[Dict[str, Any]]:
    per_collection_limit = max(limit * 2, 100)
    docs_with_source: List[Tuple[firestore.DocumentSnapshot, str]] = []

    active_docs = _list_active_docs(db, route, status=status, limit=per_collection_limit, cursor=cursor)
    docs_with_source.extend((doc, ACTIVE_COLLECTION) for doc in active_docs)

    archived_docs = _list_archived_docs(db, route, status=status, limit=per_collection_limit)
    docs_with_source.extend((doc, ARCHIVED_COLLECTION) for doc in archived_docs)

    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for doc, collection_name in docs_with_source:
        invoice = _normalize_invoice(doc, collection_name=collection_name)
        if invoice["id"] in seen_ids:
            continue
        if _matches_filters(invoice, store, invoice_number, start_date, end_date):
            items.append(invoice)
            seen_ids.add(invoice["id"])

    items.sort(key=lambda item: (_to_epoch_ms(item.get("createdAt")) or 0), reverse=True)
    return items[:limit]


def _find_active_invoice_doc(db: firestore.Client, invoice_id: str) -> Optional[firestore.DocumentSnapshot]:
    invoices_group = db.collection_group(ACTIVE_COLLECTION)
    return next(iter(invoices_group.where("id", "==", invoice_id).limit(1).stream()), None)


def _find_archived_invoice_doc(db: firestore.Client, invoice_id: str) -> Optional[firestore.DocumentSnapshot]:
    invoices_group = db.collection_group(ARCHIVED_COLLECTION)
    return next(iter(invoices_group.where("id", "==", invoice_id).limit(1).stream()), None)


def _find_any_invoice_doc(db: firestore.Client, invoice_id: str) -> Tuple[Optional[firestore.DocumentSnapshot], Optional[str]]:
    doc = _find_active_invoice_doc(db, invoice_id)
    if doc:
        return doc, ACTIVE_COLLECTION
    doc = _find_archived_invoice_doc(db, invoice_id)
    if doc:
        return doc, ARCHIVED_COLLECTION
    return None, None


def _active_storage_blob_path(route_number: str, invoice_id: str, page_number: int) -> str:
    return f"routes/{route_number}/pos_invoices/{invoice_id}/page_{page_number}.jpg"


def _active_storage_prefix(route_number: str, invoice_id: str) -> str:
    return f"routes/{route_number}/pos_invoices/{invoice_id}/"


def _archive_path(route_number: str, invoice_id: str) -> Path:
    return POS_ARCHIVE_BASE / route_number / invoice_id


def _archive_file(route_number: str, invoice_id: str, page_number: int) -> Path:
    return _archive_path(route_number, invoice_id) / f"page_{page_number}.jpg"


def _archive_metadata_file(route_number: str, invoice_id: str) -> Path:
    return _archive_path(route_number, invoice_id) / "metadata.json"


def _bucket():
    return storage.bucket(name=STORAGE_BUCKET, app=get_firebase_app())


def _download_active_page(route_number: str, invoice_id: str, page_number: int) -> Optional[bytes]:
    try:
        blob = _bucket().blob(_active_storage_blob_path(route_number, invoice_id, page_number))
        if not blob.exists():
            return None
        return blob.download_as_bytes()
    except Exception as exc:
        logger.warning("Failed fetching active POS page %s/%s/%s: %s", route_number, invoice_id, page_number, exc)
        return None


def _build_archive_job_doc_id(route_number: str, invoice_id: str) -> str:
    return f"{route_number}_{invoice_id}"


def _compute_retention_until(created_at_ms: Optional[int]) -> Optional[int]:
    if not created_at_ms:
        return None
    created = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)
    try:
        return int(created.replace(year=created.year + POS_RETENTION_YEARS).timestamp() * 1000)
    except ValueError:
        # Leap-year safe fallback
        return int((created + timedelta(days=365 * POS_RETENTION_YEARS)).timestamp() * 1000)


def _build_archive_job(invoice: Dict[str, Any], *, user_uid: str, force_archive: bool = False) -> Dict[str, Any]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "invoiceId": invoice["id"],
        "routeNumber": invoice["routeNumber"],
        "status": "queued",
        "createdAt": now_ms,
        "updatedAt": now_ms,
        "requestedBy": user_uid,
        "forceArchive": bool(force_archive),
        "workerId": None,
        "startedAt": None,
        "completedAt": None,
        "error": None,
        "serverArchivePath": str(_archive_path(invoice["routeNumber"], invoice["id"])),
        "pageCountExpected": int(invoice.get("pageCount") or 0),
        "pageCountArchived": 0,
        "firebaseDeleteEligible": False,
        "archiveVerified": False,
    }


def _archive_eligible(invoice: Dict[str, Any]) -> bool:
    created_at_ms = _to_epoch_ms(invoice.get("createdAt"))
    if not created_at_ms:
        return False
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=POS_HOT_TIER_DAYS)).timestamp() * 1000)
    return created_at_ms <= cutoff_ms


def _ensure_archive_job(
    db: firestore.Client,
    invoice: Dict[str, Any],
    *,
    user_uid: str,
    force_archive: bool = False,
) -> Dict[str, Any]:
    job_id = _build_archive_job_doc_id(invoice["routeNumber"], invoice["id"])
    job_ref = db.collection(JOB_COLLECTION).document(job_id)
    existing = job_ref.get()
    if existing.exists:
        data = existing.to_dict() or {}
        data["id"] = existing.id
        return {"reused": True, "job": data}

    payload = _build_archive_job(invoice, user_uid=user_uid, force_archive=force_archive)
    job_ref.set(payload)
    payload["id"] = job_id
    return {"reused": False, "job": payload}


def _delete_file_later(path: str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        logger.warning("Failed removing temp POS download file: %s", path)


def _add_bytes_to_zip(zip_file: zipfile.ZipFile, arcname: str, content: bytes) -> None:
    info = zipfile.ZipInfo(arcname)
    info.date_time = datetime.utcnow().timetuple()[:6]
    zip_file.writestr(info, content)


def _build_download_bundle(invoice: Dict[str, Any]) -> str:
    route_number = str(invoice.get("routeNumber") or "")
    invoice_id = str(invoice.get("id") or "")
    tier = invoice.get("storageTier")
    tmp_file = tempfile.NamedTemporaryFile(prefix=f"pos_{invoice_id}_", suffix=".zip", delete=False)
    tmp_file.close()

    with zipfile.ZipFile(tmp_file.name, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        metadata = json.dumps(invoice, indent=2).encode("utf-8")
        _add_bytes_to_zip(zf, "metadata.json", metadata)

        page_count = int(invoice.get("pageCount") or 0)
        for page_number in range(page_count):
            if tier == "server":
                page_path = _archive_file(route_number, invoice_id, page_number)
                if page_path.exists():
                    zf.write(page_path, arcname=f"page_{page_number}.jpg")
            else:
                page_bytes = _download_active_page(route_number, invoice_id, page_number)
                if page_bytes is not None:
                    _add_bytes_to_zip(zf, f"page_{page_number}.jpg", page_bytes)

    return tmp_file.name


@router.get("/pos")
@rate_limit_history
async def list_pos_invoices(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    status: Optional[str] = Query(default="active", description="active|archived|archived_server|all"),
    store: Optional[str] = Query(default=None),
    invoiceNumber: Optional[str] = Query(default=None),
    startDate: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    endDate: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: Optional[str] = Query(default=None, description="Last document ID (active-only compatibility)"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await require_route_access(route, decoded_token, db)

    if status not in STATUS_VALUES:
        raise HTTPException(400, "Invalid status")

    start_date = _validate_date(startDate)
    end_date = _validate_date(endDate)

    items = _merged_items(
        route=route,
        db=db,
        status=status,
        store=store,
        invoice_number=invoiceNumber,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cursor,
    )

    return {
        "routeNumber": route,
        "items": items,
        "nextCursor": None,
    }


@router.get("/pos/{invoice_id}")
@rate_limit_history
async def get_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    doc, collection_name = _find_any_invoice_doc(db, invoice_id)
    if not doc or not collection_name:
        raise HTTPException(404, "Invoice not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)
    return _normalize_invoice(doc, collection_name=collection_name)


@router.get("/pos/{invoice_id}/page/{page_number}")
@rate_limit_history
async def get_pos_invoice_page(
    request: Request,
    invoice_id: str,
    page_number: int,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    if page_number < 0 or page_number > 500:
        raise HTTPException(400, "Invalid page number")

    doc, collection_name = _find_any_invoice_doc(db, invoice_id)
    if not doc or not collection_name:
        raise HTTPException(404, "Invoice not found")

    invoice = _normalize_invoice(doc, collection_name=collection_name)
    route_number = str(invoice.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)

    page_count = int(invoice.get("pageCount") or 0)
    if page_number >= page_count:
        raise HTTPException(404, "Page not found")

    if collection_name == ARCHIVED_COLLECTION:
        archive_file = _archive_file(route_number, invoice_id, page_number)
        resolved = archive_file.resolve()
        archive_root = POS_ARCHIVE_BASE.resolve()
        if not str(resolved).startswith(str(archive_root)):
            raise HTTPException(403, "Access denied")
        if not resolved.exists():
            raise HTTPException(404, "Page not found")
        return FileResponse(
            str(resolved),
            media_type="image/jpeg",
            filename=f"pos_{invoice_id}_page_{page_number}.jpg",
        )

    page_bytes = _download_active_page(route_number, invoice_id, page_number)
    if page_bytes is None:
        raise HTTPException(404, "Page not found")
    return Response(
        content=page_bytes,
        media_type="image/jpeg",
        headers={
            "Content-Disposition": f'inline; filename="pos_{invoice_id}_page_{page_number}.jpg"'
        },
    )


@router.get("/pos/{invoice_id}/download")
@rate_limit_history
async def download_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    doc, collection_name = _find_any_invoice_doc(db, invoice_id)
    if not doc or not collection_name:
        raise HTTPException(404, "Invoice not found")

    invoice = _normalize_invoice(doc, collection_name=collection_name)
    route_number = str(invoice.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)

    bundle_path = _build_download_bundle(invoice)
    return FileResponse(
        bundle_path,
        media_type="application/zip",
        filename=f"pos_{invoice_id}.zip",
        background=BackgroundTask(_delete_file_later, bundle_path),
    )


@router.put("/pos/{invoice_id}/archive")
@rate_limit_write
async def archive_pos_invoice(
    request: Request,
    invoice_id: str,
    force: bool = Query(default=False, description="Force archive before 60-day threshold"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    doc = _find_active_invoice_doc(db, invoice_id)
    if not doc:
        archived_doc = _find_archived_invoice_doc(db, invoice_id)
        if archived_doc:
            invoice = _normalize_invoice(archived_doc, collection_name=ARCHIVED_COLLECTION)
            route_number = str(invoice.get("routeNumber") or "")
            await require_route_access(route_number, decoded_token, db)
            return {"ok": True, "reused": True, "invoice": invoice, "job": None}
        raise HTTPException(404, "Invoice not found")

    invoice = _normalize_invoice(doc, collection_name=ACTIVE_COLLECTION)
    route_number = str(invoice.get("routeNumber") or "")
    user_data = await require_route_access(route_number, decoded_token, db)

    if force and not _is_admin(user_data):
        raise HTTPException(403, "Only admins can force archive")

    if not force and not _archive_eligible(invoice):
        raise HTTPException(409, "Invoice is still within the Firebase hot tier")

    job_result = _ensure_archive_job(
        db,
        invoice,
        user_uid=str(decoded_token.get("uid") or ""),
        force_archive=force,
    )
    updated_doc = doc.reference.get()
    return {
        "ok": True,
        "reused": job_result["reused"],
        "invoice": _normalize_invoice(updated_doc, collection_name=ACTIVE_COLLECTION),
        "job": job_result["job"],
    }


@router.delete("/pos/{invoice_id}")
@rate_limit_write
async def delete_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    doc, _ = _find_any_invoice_doc(db, invoice_id)
    if not doc:
        raise HTTPException(404, "Invoice not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    user_data = await require_route_access(route_number, decoded_token, db)
    if not _is_admin(user_data):
        raise HTTPException(403, "Admin access required")

    raise HTTPException(
        status_code=409,
        detail="POS hard delete is disabled; invoices are retention-managed",
    )

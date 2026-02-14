"""Archived deliveries router — serves PCF archive data from HDD + Firebase.

Endpoints:
    GET /api/deliveries/archived               — List archived delivery summaries
    GET /api/deliveries/archived/{id}          — Full delivery detail with containers
    GET /api/deliveries/archived/{id}/image    — Serve an archived PCF image

Data sources (merged, deduplicated):
    1. Firebase archivedPCFs/ — structured data awaiting cleanup
    2. Server HDD /mnt/archive/pcf/pcf_archive/{route}/{delivery}/  — cleaned up
"""

from __future__ import annotations

import json
import os
import logging
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response

from ..dependencies import (
    get_firestore,
    require_route_access,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history
from ..models import ErrorResponse

logger = logging.getLogger("api.deliveries")

router = APIRouter()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HDD_ARCHIVE_BASE = os.environ.get(
    "PCF_ARCHIVE_PATH", "/mnt/archive/pcf/pcf_archive"
)


# ---------------------------------------------------------------------------
# HDD helpers
# ---------------------------------------------------------------------------

def _find_latest_run(route: str, delivery: str) -> Optional[str]:
    """Return the path to the newest timestamped run directory, or None."""
    delivery_dir = os.path.join(HDD_ARCHIVE_BASE, route, delivery)
    if not os.path.isdir(delivery_dir):
        return None
    timestamps = sorted(os.listdir(delivery_dir), reverse=True)
    for ts in timestamps:
        run = os.path.join(delivery_dir, ts)
        if os.path.isdir(run):
            return run
    return None


def _read_hdd_summary(route: str, delivery: str) -> Optional[Dict[str, Any]]:
    """Read delivery summary from HDD archive (firestore/ or manifest)."""
    run_path = _find_latest_run(route, delivery)
    if not run_path:
        return None

    result: Dict[str, Any] = {
        "deliveryNumber": delivery,
        "routeNumber": route,
        "source": "hdd",
    }

    # Prefer firestore/delivery.json (has full structured data)
    fs_path = os.path.join(run_path, "firestore", "delivery.json")
    if os.path.isfile(fs_path):
        try:
            with open(fs_path) as f:
                data = json.load(f)
            result.update({
                "containerCount": data.get("containerCount"),
                "totalItems": data.get("totalItems"),
                "status": data.get("status", "completed"),
                "createdAt": str(data.get("createdAt", ""))[:10] or None,
                "loadingDate": None,  # populated from container if needed
                "hasFirestoreData": True,
            })
            return result
        except Exception as exc:
            logger.warning("Failed to read %s: %s", fs_path, exc)

    # Fall back to manifest.json (images-only archive)
    manifest_path = os.path.join(run_path, "manifest.json")
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            result.update({
                "containerCount": None,
                "totalItems": None,
                "status": "completed",
                "createdAt": _ts_dir_to_date(os.path.basename(run_path)),
                "imageCount": len(manifest.get("images", [])),
                "hasFirestoreData": False,
            })
            return result
        except Exception as exc:
            logger.warning("Failed to read %s: %s", manifest_path, exc)

    return None


def _read_hdd_detail(route: str, delivery: str) -> Optional[Dict[str, Any]]:
    """Read full delivery detail from HDD including containers."""
    run_path = _find_latest_run(route, delivery)
    if not run_path:
        return None

    fs_path = os.path.join(run_path, "firestore", "delivery.json")
    if not os.path.isfile(fs_path):
        return None

    try:
        with open(fs_path) as f:
            data = json.load(f)
    except Exception:
        return None

    # Read containers
    containers = []
    containers_dir = os.path.join(run_path, "firestore", "containers")
    if os.path.isdir(containers_dir):
        for fname in sorted(os.listdir(containers_dir)):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(containers_dir, fname)) as f:
                    containers.append(json.load(f))
            except Exception:
                continue

    data["containers"] = containers
    data["source"] = "hdd"
    return data


def _ts_dir_to_date(ts_dir: str) -> Optional[str]:
    """Convert timestamp directory name like '20251030_193238' to '2025-10-30'."""
    try:
        dt = datetime.strptime(ts_dir[:8], "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Firebase helpers
# ---------------------------------------------------------------------------

def _firebase_doc_to_summary(doc) -> Dict[str, Any]:
    """Convert a Firebase archivedPCFs document to a summary dict."""
    data = doc.to_dict() or {}
    created = data.get("createdAt")
    if created is not None:
        created = str(created)[:10]

    return {
        "deliveryNumber": doc.id,
        "routeNumber": data.get("routeNumber", ""),
        "containerCount": data.get("containerCount"),
        "totalItems": data.get("totalItems"),
        "status": data.get("status", "completed"),
        "createdAt": created,
        "loadingDate": None,  # populated from container if needed
        "hasFirestoreData": True,
        "source": "firebase",
    }


def _firebase_doc_to_detail(db, route: str, delivery_id: str) -> Optional[Dict[str, Any]]:
    """Read full delivery detail from Firebase archivedPCFs."""
    ref = (
        db.collection("routes")
        .document(route)
        .collection("archivedPCFs")
        .document(delivery_id)
    )
    doc = ref.get()
    if not doc.exists:
        return None

    data = doc.to_dict() or {}

    # Read containers
    containers = []
    for cdoc in ref.collection("containers").stream():
        cdata = cdoc.to_dict() or {}
        cdata["containerCode"] = cdoc.id
        containers.append(cdata)

    data["deliveryNumber"] = doc.id
    data["containers"] = containers
    data["source"] = "firebase"
    return data


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _serialize(obj: Any) -> Any:
    """Make an object JSON-safe (handle datetime, Firestore timestamps, etc.)."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    # Firestore DatetimeWithNanoseconds, proto timestamps, etc.
    if hasattr(obj, "timestamp"):
        try:
            return datetime.fromtimestamp(obj.timestamp()).isoformat()
        except Exception:
            pass
    return str(obj)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/deliveries/archived",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def list_archived_deliveries(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    from_date: Optional[str] = Query(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Start date (YYYY-MM-DD). If omitted, defaults to 6 months ago.",
    ),
    to_date: Optional[str] = Query(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="End date (YYYY-MM-DD). If omitted, defaults to today.",
    ),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> Dict[str, Any]:
    """List archived delivery summaries from Firebase + HDD.

    Merges both sources, deduplicates by delivery number, and sorts by
    creation date descending.

    Returns ``oldestDate`` so the client can show the user how far back
    data is available.
    """
    await require_route_access(route, decoded_token, db)

    # Default date range
    cutoff_str = from_date or (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%d")
    end_str = to_date or datetime.utcnow().strftime("%Y-%m-%d")

    all_items: Dict[str, Dict[str, Any]] = {}   # all deliveries (unfiltered, for oldestDate)
    seen: Dict[str, Dict[str, Any]] = {}          # filtered by date range

    # --- Build set of active delivery IDs to exclude from archive ---
    # The OCR pipeline writes every delivery to HDD, including active ones.
    # We must exclude deliveries that still exist in the active pcfs/ collection.
    active_ids: set = set()
    try:
        active_ref = db.collection("routes").document(route).collection("pcfs")
        for adoc in active_ref.select([]).stream():   # select([]) = IDs only, no data
            active_ids.add(adoc.id)
    except Exception as exc:
        logger.warning("Error reading active pcfs for route %s: %s", route, exc)

    # --- Source 1: Firebase archivedPCFs ---
    try:
        archived_ref = (
            db.collection("routes")
            .document(route)
            .collection("archivedPCFs")
        )
        for doc in archived_ref.stream():
            summary = _firebase_doc_to_summary(doc)
            all_items[doc.id] = summary
            created = summary.get("createdAt")
            if created and (created < cutoff_str or created > end_str):
                continue
            seen[doc.id] = summary
    except Exception as exc:
        logger.warning("Error reading archivedPCFs for route %s: %s", route, exc)

    # --- Source 2: HDD archive ---
    route_dir = os.path.join(HDD_ARCHIVE_BASE, route)
    if os.path.isdir(route_dir):
        try:
            for delivery in os.listdir(route_dir):
                if delivery in all_items:
                    continue  # Firebase archivedPCFs version preferred
                if delivery in active_ids:
                    continue  # Still an active delivery — not archived
                summary = _read_hdd_summary(route, delivery)
                if summary is None:
                    continue
                all_items[delivery] = summary
                created = summary.get("createdAt")
                if created and (created < cutoff_str or created > end_str):
                    continue
                if delivery not in seen:
                    seen[delivery] = summary
        except Exception as exc:
            logger.warning("Error reading HDD archive for route %s: %s", route, exc)

    # Determine oldest date across ALL deliveries (not just filtered)
    all_dates = [
        v.get("createdAt")
        for v in all_items.values()
        if v.get("createdAt")
    ]
    oldest_date = min(all_dates) if all_dates else None

    # Sort filtered results by createdAt descending (unknowns at the end)
    items = sorted(
        seen.values(),
        key=lambda x: x.get("createdAt") or "0000-00-00",
        reverse=True,
    )

    return _serialize({
        "items": items,
        "total": len(items),
        "totalAvailable": len(all_items),
        "oldestDate": oldest_date,
        "route": route,
        "fromDate": cutoff_str,
        "toDate": end_str,
    })


@router.get(
    "/deliveries/archived/{delivery_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_archived_delivery(
    request: Request,
    delivery_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> Dict[str, Any]:
    """Get full archived delivery detail with containers.

    Checks Firebase archivedPCFs first (richer data), falls back to HDD.
    """
    await require_route_access(route, decoded_token, db)

    # Try Firebase first
    detail = _firebase_doc_to_detail(db, route, delivery_id)

    # Fall back to HDD
    if detail is None:
        detail = _read_hdd_detail(route, delivery_id)

    if detail is None:
        raise HTTPException(404, "Archived delivery not found")

    return _serialize(detail)


# ---------------------------------------------------------------------------
# Image serving
# ---------------------------------------------------------------------------

def _find_hdd_image(route: str, delivery: str, container: str, page: int) -> Optional[str]:
    """Find an archived image on the HDD.

    Image naming convention: {delivery}_{container}_page_{page}.jpg
    Located in: /mnt/archive/pcf/pcf_archive/{route}/{delivery}/{timestamp}/images/
    """
    run_path = _find_latest_run(route, delivery)
    if not run_path:
        return None

    images_dir = os.path.join(run_path, "images")
    if not os.path.isdir(images_dir):
        return None

    # Expected filename pattern
    expected = f"{delivery}_{container}_page_{page}.jpg"
    candidate = os.path.join(images_dir, expected)
    if os.path.isfile(candidate):
        return candidate

    # Fallback: scan images dir for matching container + page
    pattern = re.compile(rf".*{re.escape(container)}_page_{page}\.jpg$", re.IGNORECASE)
    for fname in os.listdir(images_dir):
        if pattern.match(fname):
            return os.path.join(images_dir, fname)

    return None


STORAGE_BUCKET = os.environ.get(
    "FIREBASE_STORAGE_BUCKET", "routespark-1f47d.firebasestorage.app"
)


def _get_firebase_storage_image(route: str, delivery: str, container: str, page: int) -> Optional[bytes]:
    """Download an archived image from Firebase Storage (archivedPCFs path).

    Returns image bytes or None if not found.
    """
    try:
        from firebase_admin import storage as fb_storage

        bucket = fb_storage.bucket(STORAGE_BUCKET)
        blob_path = f"routes/{route}/archivedPCFs/{delivery}/{container}/page_{page}.jpg"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            return None

        return blob.download_as_bytes()
    except Exception as exc:
        logger.warning("Firebase Storage fetch failed for %s/%s/%s page %d: %s",
                        route, delivery, container, page, exc)
        return None


@router.get(
    "/deliveries/archived/{delivery_id}/image",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_archived_image(
    request: Request,
    delivery_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    container: str = Query(..., description="Container code"),
    page: int = Query(default=1, ge=1, le=50, description="Page number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """Serve an archived PCF image.

    Checks HDD first (post-cleanup), falls back to Firebase Storage
    (recently archived, pre-cleanup).
    """
    await require_route_access(route, decoded_token, db)

    # Validate inputs (prevent path traversal)
    if not re.match(r"^\d+$", delivery_id):
        raise HTTPException(400, "Invalid delivery ID")
    if not re.match(r"^\d+$", container):
        raise HTTPException(400, "Invalid container code")

    # Try HDD first (images that have been cleaned up from Firebase)
    hdd_path = _find_hdd_image(route, delivery_id, container, page)
    if hdd_path:
        # Resolve and verify path stays within archive root
        resolved = os.path.realpath(hdd_path)
        if not resolved.startswith(os.path.realpath(HDD_ARCHIVE_BASE)):
            raise HTTPException(403, "Access denied")
        return FileResponse(
            resolved,
            media_type="image/jpeg",
            filename=f"pcf_{delivery_id}_{container}_page{page}.jpg",
        )

    # Fall back to Firebase Storage (recently archived, not yet cleaned up)
    image_bytes = _get_firebase_storage_image(route, delivery_id, container, page)
    if image_bytes:
        return Response(
            content=image_bytes,
            media_type="image/jpeg",
            headers={
                "Content-Disposition": f'inline; filename="pcf_{delivery_id}_{container}_page{page}.jpg"'
            },
        )

    raise HTTPException(404, "Image not found")


# ---------------------------------------------------------------------------
# Active PCF Endpoints
# ---------------------------------------------------------------------------

def _process_active_container(container_doc, delivery_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process an active container document into a summary."""
    cdata = container_doc.to_dict() or {}

    # Extract items from pages for product search support
    products = []
    pages_data = []

    for page in cdata.get("pages", []):
        page_items = []
        for item in page.get("items", []):
            if item.get("product"):
                # Detect short-coded items (days contains *)
                days = item.get("days", "")
                is_short_coded = "*" in days if days else False

                product_info = {
                    "product": item.get("product"),
                    "description": item.get("description", ""),
                    "bestBefore": item.get("bestBefore"),
                    "days": days,
                    "isShortCoded": is_short_coded,
                }

                # Include guarantee info if present
                if item.get("guaranteed"):
                    product_info["guaranteed"] = item.get("guaranteed")

                page_items.append(product_info)
                products.append({
                    **product_info,
                    "containerCode": container_doc.id,
                    "pageNumber": page.get("pageNumber", len(pages_data) + 1),
                })

        pages_data.append({
            "pageNumber": page.get("pageNumber", len(pages_data) + 1),
            "items": page_items,
        })

    return {
        "containerCode": container_doc.id,
        "loadingDate": cdata.get("loadingDate"),
        "pageCount": len(pages_data),
        "totalItems": cdata.get("totalItems", len(products)),
        "expiringItems": cdata.get("expiringItems", 0),
        "expiredItems": cdata.get("expiredItems", 0),
        "pages": pages_data,
        "products": products,
    }


@router.get(
    "/deliveries/active",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def list_active_deliveries(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> Dict[str, Any]:
    """List active (non-archived) deliveries with PCF data.

    Returns summary data for list view. Use /deliveries/active/{id} for full detail.
    """
    await require_route_access(route, decoded_token, db)

    deliveries = []

    try:
        pcfs_ref = db.collection("routes").document(route).collection("pcfs")

        for delivery_doc in pcfs_ref.stream():
            delivery_data = delivery_doc.to_dict() or {}

            # Get containers for this delivery
            containers_ref = delivery_doc.reference.collection("containers")
            containers = []
            total_items = 0
            total_expiring = 0
            total_expired = 0
            loading_date = None
            all_products = []

            for container_doc in containers_ref.stream():
                container_info = _process_active_container(container_doc, delivery_data)
                containers.append({
                    "containerCode": container_info["containerCode"],
                    "pageCount": container_info["pageCount"],
                    "totalItems": container_info["totalItems"],
                })
                total_items += container_info["totalItems"]
                total_expiring += container_info.get("expiringItems", 0)
                total_expired += container_info.get("expiredItems", 0)
                all_products.extend(container_info["products"])

                if not loading_date and container_info.get("loadingDate"):
                    loading_date = container_info["loadingDate"]

            deliveries.append({
                "deliveryNumber": delivery_doc.id,
                "routeNumber": route,
                "status": delivery_data.get("status", "active"),
                "loadingDate": loading_date,
                "containerCount": len(containers),
                "totalItems": total_items,
                "expiringItems": total_expiring,
                "expiredItems": total_expired,
                "containers": containers,
                "products": all_products,  # For client-side product search
                "createdAt": delivery_data.get("createdAt"),
                "updatedAt": delivery_data.get("updatedAt"),
            })

    except Exception as exc:
        logger.error("Error fetching active PCFs for route %s: %s", route, exc)
        raise HTTPException(500, "Failed to fetch active deliveries")

    # Sort by loading date descending (newest first)
    # loadingDate is MM/DD/YYYY — parse to YYYY-MM-DD for correct sort
    def _parse_loading_date(d: dict) -> str:
        ld = d.get("loadingDate") or ""
        if "/" in ld:
            parts = ld.split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        return ld or "0000-00-00"

    deliveries.sort(key=_parse_loading_date, reverse=True)

    return _serialize({
        "items": deliveries,
        "total": len(deliveries),
        "route": route,
    })


@router.get(
    "/deliveries/active/{delivery_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_active_delivery(
    request: Request,
    delivery_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> Dict[str, Any]:
    """Get full detail for an active delivery including all containers and items."""
    await require_route_access(route, decoded_token, db)

    # Validate delivery ID
    if not re.match(r"^\d+$", delivery_id):
        raise HTTPException(400, "Invalid delivery ID")

    try:
        delivery_ref = (
            db.collection("routes")
            .document(route)
            .collection("pcfs")
            .document(delivery_id)
        )
        delivery_doc = delivery_ref.get()

        if not delivery_doc.exists:
            raise HTTPException(404, "Delivery not found")

        delivery_data = delivery_doc.to_dict() or {}

        # Get all containers with full detail
        containers = []
        containers_ref = delivery_ref.collection("containers")

        for container_doc in containers_ref.stream():
            container_info = _process_active_container(container_doc, delivery_data)
            containers.append(container_info)

        return _serialize({
            "deliveryNumber": delivery_id,
            "routeNumber": route,
            "status": delivery_data.get("status", "active"),
            "containers": containers,
            "createdAt": delivery_data.get("createdAt"),
            "updatedAt": delivery_data.get("updatedAt"),
            "source": "firebase",
        })

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching active delivery %s: %s", delivery_id, exc)
        raise HTTPException(500, "Failed to fetch delivery")


@router.get(
    "/deliveries/active/{delivery_id}/image",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_active_image(
    request: Request,
    delivery_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    container: str = Query(..., description="Container code"),
    page: int = Query(default=1, ge=1, le=50, description="Page number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """Serve an active PCF image from Firebase Storage.

    Active images are stored at: routes/{route}/pcfs/{delivery}/{container}/page_{page}.jpg
    """
    await require_route_access(route, decoded_token, db)

    # Validate inputs
    if not re.match(r"^\d+$", delivery_id):
        raise HTTPException(400, "Invalid delivery ID")
    if not re.match(r"^\d+$", container):
        raise HTTPException(400, "Invalid container code")

    try:
        from firebase_admin import storage as fb_storage

        bucket = fb_storage.bucket(STORAGE_BUCKET)
        blob_path = f"routes/{route}/pcfs/{delivery_id}/{container}/page_{page}.jpg"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise HTTPException(404, "Image not found")

        image_bytes = blob.download_as_bytes()

        return Response(
            content=image_bytes,
            media_type="image/jpeg",
            headers={
                "Content-Disposition": f'inline; filename="pcf_{delivery_id}_{container}_page{page}.jpg"',
                "Cache-Control": "private, max-age=300",  # 5 min cache for active images
            },
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Failed to fetch active image %s/%s/%s page %d: %s",
                       route, delivery_id, container, page, exc)
        raise HTTPException(404, "Image not found")

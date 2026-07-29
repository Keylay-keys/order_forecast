"""Reference data router - products, stores, schedule, promos."""

from __future__ import annotations

import logging
import os
from pathlib import Path as FilePath
from typing import Dict, Any, List, NoReturn, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi.responses import FileResponse
from google.cloud import firestore
import psycopg2
from psycopg2.extras import RealDictCursor

from ..dependencies import (
    get_pg_connection,
    return_pg_connection,
    verify_firebase_token,
    require_route_access,
    get_firestore,
)
from ..errors import StructuredApiError, get_request_id
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()
logger = logging.getLogger(__name__)

DEFAULT_REFERENCE_CATALOG_ID = "routespark-starter-catalog"
IMAGE_ROUTE_PREFIX = "/api/catalog/starter/images"
REFERENCE_TAG_SEARCH_ALIASES = {
    "bfy": "better_for_you",
    "better for you": "better_for_you",
}


def _raise_reference_catalog_unavailable(
    request: Request,
    *,
    stage: str,
    error: psycopg2.Error,
) -> NoReturn:
    logger.exception(
        "reference_catalog_db_error request_id=%s stage=%s error_type=%s",
        get_request_id(request),
        stage,
        type(error).__name__,
    )
    raise StructuredApiError(
        status_code=503,
        error="Reference catalog temporarily unavailable",
        code="REFERENCE_CATALOG_UNAVAILABLE",
        details={"stage": stage},
    ) from error


def _public_base_url(request: Request) -> str:
    configured = os.environ.get("REFERENCE_CATALOG_PUBLIC_BASE_URL")
    if configured:
        return configured

    forwarded_proto = _clean_text(request.headers.get("x-forwarded-proto")).split(",", 1)[0].strip()
    forwarded_host = _clean_text(request.headers.get("x-forwarded-host")).split(",", 1)[0].strip()
    proto = forwarded_proto or request.url.scheme
    host = forwarded_host or _clean_text(request.headers.get("host")) or request.url.netloc
    if host:
        return f"{proto}://{host}/"
    return str(request.base_url)


def _image_root() -> FilePath:
    configured = os.environ.get("REFERENCE_CATALOG_IMAGE_ROOT")
    if configured:
        return FilePath(configured)
    return FilePath(__file__).resolve().parents[3] / "data" / "catalogs" / "product_images"


def _clean_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _clean_optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _clean_int(value: Any, fallback: int = 0) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return number if number >= 0 else fallback


def _clean_optional_int(value: Any) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_optional_positive_int(value: Any) -> Optional[int]:
    number = _clean_optional_int(value)
    return number if number is not None and number > 0 else None


def _clean_tags(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_tags = value
    elif isinstance(value, tuple):
        raw_tags = list(value)
    else:
        raw_tags = [value]

    tags: List[str] = []
    seen = set()
    for entry in raw_tags:
        tag = _clean_text(entry)
        if tag and tag not in seen:
            tags.append(tag)
            seen.add(tag)
    return tags


def _normalize_upc(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _tag_alias_like_query(query: str) -> str:
    alias = REFERENCE_TAG_SEARCH_ALIASES.get(query.strip().lower())
    return f"%{_escape_like(alias)}%" if alias else f"%{_escape_like(query)}%"


def _reference_image_url(base_url: Optional[str], sap: str, image_path: Optional[str]) -> Optional[str]:
    if not base_url or not image_path:
        return None
    url = f"{base_url.rstrip('/')}{IMAGE_ROUTE_PREFIX}/{sap}.png"
    image_file = _safe_catalog_image_file(_image_root(), image_path)
    if image_file and image_file.exists():
        try:
            return f"{url}?v={int(image_file.stat().st_mtime)}"
        except OSError:
            pass
    return url


def _normalize_reference_item(row: Dict[str, Any], *, base_url: Optional[str] = None) -> Dict[str, Any]:
    sap = _clean_text(row.get("sap"))
    image_path = _clean_optional_text(row.get("image_path") or row.get("imagePath"))
    image_thumb_path = _clean_optional_text(row.get("image_thumb_path") or row.get("imageThumbPath") or image_path)
    return {
        "catalogId": _clean_text(row.get("catalog_id"), DEFAULT_REFERENCE_CATALOG_ID),
        "sap": sap,
        "upc": _clean_optional_text(row.get("upc")),
        "brand": _clean_text(row.get("brand")),
        "category": _clean_text(row.get("category")),
        "tags": _clean_tags(row.get("tags")),
        "fullName": _clean_text(row.get("full_name") or row.get("fullName"), "Unnamed Product"),
        "casePack": _clean_int(row.get("case_pack") or row.get("casePack"), 0),
        "unitPack": _clean_optional_positive_int(
            row.get("unit_pack") if row.get("unit_pack") is not None else row.get("unitPack")
        ),
        "searchPriority": _clean_optional_int(
            row.get("search_priority")
            if row.get("search_priority") is not None
            else row.get("searchPriority")
        ),
        "displayOrder": _clean_int(row.get("display_order") or row.get("displayOrder"), 0),
        "imageUrl": _reference_image_url(base_url, sap, image_path),
        "imageThumbUrl": _reference_image_url(base_url, sap, image_thumb_path),
        "source": _clean_optional_text(row.get("source")),
        "active": bool(row.get("active", True)),
    }


def _normalize_reference_meta(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not row:
        return {
            "version": None,
            "productCount": None,
            "updatedAt": None,
        }
    updated_at = row.get("updated_at") or row.get("updatedAt")
    return {
        "version": _clean_int(row.get("version"), 0) or None,
        "productCount": _clean_int(row.get("product_count") or row.get("productCount"), 0) or None,
        "updatedAt": updated_at.isoformat() if hasattr(updated_at, "isoformat") else _clean_optional_text(updated_at),
    }


def _fetch_reference_catalog_meta(
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
) -> Dict[str, Any]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT catalog_id, version, product_count, updated_at
                FROM reference_catalog_meta
                WHERE catalog_id = %s
                LIMIT 1
                """,
                [catalog_id],
            )
            row = cur.fetchone()
            return _normalize_reference_meta(dict(row) if row else None)
    finally:
        return_pg_connection(conn)


def _reference_catalog_response(
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    items: Optional[List[Dict[str, Any]]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta = _fetch_reference_catalog_meta(catalog_id=catalog_id)
    response: Dict[str, Any] = {
        "catalogId": catalog_id,
        "version": meta["version"],
        "productCount": meta["productCount"],
        "updatedAt": meta["updatedAt"],
    }
    if items is not None:
        response["items"] = items
    if extra:
        response.update(extra)
    return response


def _safe_catalog_image_file(root: FilePath, image_path: str) -> Optional[FilePath]:
    resolved_root = root.resolve()
    candidate = (resolved_root / image_path).resolve()
    if resolved_root not in candidate.parents:
        return None
    return candidate if candidate.is_file() else None


def _fetch_reference_items_by_search(
    query: str,
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    limit: int = 25,
    base_url: Optional[str] = None,
    include_inactive: bool = False,
) -> List[Dict[str, Any]]:
    normalized_query = query.strip()
    normalized_upc = _normalize_upc(normalized_query)
    like_query = f"%{_escape_like(normalized_query)}%"
    tag_alias_query = _tag_alias_like_query(normalized_query)
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    catalog_id,
                    sap,
                    upc,
                    full_name,
                    brand,
                    category,
                    tags,
                    case_pack,
                    unit_pack,
                    search_priority,
                    display_order,
                    image_path,
                    image_thumb_path,
                    source,
                    active,
                    CASE
                        WHEN sap = %s THEN 0
                        WHEN upc = %s THEN 1
                        WHEN regexp_replace(COALESCE(upc, ''), '[^0-9]', '', 'g') = %s
                             AND %s <> '' THEN 1
                        WHEN full_name ILIKE %s ESCAPE '\\' THEN 2
                        ELSE 3
                    END AS match_rank
                FROM reference_catalog_items
                WHERE catalog_id = %s
                  AND (%s OR active = TRUE)
                  AND (
                    sap = %s
                    OR upc = %s
                    OR (
                        regexp_replace(COALESCE(upc, ''), '[^0-9]', '', 'g') = %s
                        AND %s <> ''
                    )
                    OR full_name ILIKE %s ESCAPE '\\'
                    OR brand ILIKE %s ESCAPE '\\'
                    OR category ILIKE %s ESCAPE '\\'
                    OR array_to_string(COALESCE(tags, ARRAY[]::TEXT[]), ' ') ILIKE %s ESCAPE '\\'
                    OR array_to_string(COALESCE(tags, ARRAY[]::TEXT[]), ' ') ILIKE %s ESCAPE '\\'
                    OR replace(array_to_string(COALESCE(tags, ARRAY[]::TEXT[]), ' '), '_', ' ') ILIKE %s ESCAPE '\\'
                  )
                ORDER BY match_rank, display_order NULLS LAST, sap
                LIMIT %s
                """,
                [
                    normalized_query,
                    normalized_query,
                    normalized_upc,
                    normalized_upc,
                    like_query,
                    catalog_id,
                    include_inactive,
                    normalized_query,
                    normalized_query,
                    normalized_upc,
                    normalized_upc,
                    like_query,
                    like_query,
                    like_query,
                    tag_alias_query,
                    like_query,
                    like_query,
                    limit,
                ],
            )
            return [_normalize_reference_item(dict(row), base_url=base_url) for row in cur.fetchall()]
    finally:
        return_pg_connection(conn)


def _fetch_reference_item_by_sap(
    sap: str,
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    base_url: Optional[str] = None,
    include_inactive: bool = False,
) -> Optional[Dict[str, Any]]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT catalog_id, sap, upc, full_name, brand, category, tags,
                       case_pack, unit_pack, search_priority, display_order, image_path, image_thumb_path,
                       source, active
                FROM reference_catalog_items
                WHERE catalog_id = %s AND sap = %s AND (%s OR active = TRUE)
                LIMIT 1
                """,
                [catalog_id, sap, include_inactive],
            )
            row = cur.fetchone()
            return _normalize_reference_item(dict(row), base_url=base_url) if row else None
    finally:
        return_pg_connection(conn)


def _fetch_reference_catalog_items(
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    limit: int = 250,
    base_url: Optional[str] = None,
    include_inactive: bool = False,
) -> List[Dict[str, Any]]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT catalog_id, sap, upc, full_name, brand, category, tags,
                       case_pack, unit_pack, search_priority, display_order, image_path, image_thumb_path,
                       source, active
                FROM reference_catalog_items
                WHERE catalog_id = %s AND (%s OR active = TRUE)
                ORDER BY display_order NULLS LAST, sap
                LIMIT %s
                """,
                [catalog_id, include_inactive, limit],
            )
            return [_normalize_reference_item(dict(row), base_url=base_url) for row in cur.fetchall()]
    finally:
        return_pg_connection(conn)


def _fetch_reference_image_path(sap: str, *, catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID) -> Optional[FilePath]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT image_path
                FROM reference_catalog_items
                WHERE catalog_id = %s AND sap = %s AND active = TRUE
                LIMIT 1
                """,
                [catalog_id, sap],
            )
            row = cur.fetchone()
            image_path = _clean_optional_text(dict(row).get("image_path")) if row else None
            if not image_path:
                return None
            return _safe_catalog_image_file(_image_root(), image_path)
    finally:
        return_pg_connection(conn)


@router.get("/catalog/starter")
@rate_limit_history
async def get_starter_catalog(
    request: Request,
    limit: int = Query(250, ge=1, le=500),
    includeInactive: bool = Query(False, description="Include inactive reference rows"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Return the shared RouteSpark starter/reference catalog."""
    del decoded_token
    try:
        items = _fetch_reference_catalog_items(
            limit=limit,
            base_url=_public_base_url(request),
            include_inactive=includeInactive,
        )
    except psycopg2.Error as error:
        _raise_reference_catalog_unavailable(request, stage="items", error=error)

    try:
        return _reference_catalog_response(items=items)
    except psycopg2.Error as error:
        _raise_reference_catalog_unavailable(request, stage="metadata", error=error)


@router.get("/catalog/items/search")
@rate_limit_history
async def search_reference_catalog(
    request: Request,
    q: str = Query(..., min_length=1, max_length=120, description="SAP, UPC, or item description"),
    limit: int = Query(25, ge=1, le=50),
    includeInactive: bool = Query(False, description="Include inactive reference rows"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Search the shared RouteSpark reference catalog.

    This data is not route-private, but reads still require Firebase auth so the
    endpoint cannot be scraped anonymously.
    """
    del decoded_token
    query = q.strip()
    items = _fetch_reference_items_by_search(
        query,
        limit=limit,
        base_url=_public_base_url(request),
        include_inactive=includeInactive,
    )
    return _reference_catalog_response(items=items, extra={"query": query})


@router.get("/catalog/starter/items/{sap}")
@rate_limit_history
async def get_reference_catalog_item(
    request: Request,
    sap: str = Path(..., pattern=r"^[A-Za-z0-9_-]{1,20}$"),
    includeInactive: bool = Query(False, description="Include inactive reference rows"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Return one item from the shared RouteSpark reference catalog by SAP."""
    del decoded_token
    item = _fetch_reference_item_by_sap(
        sap.strip(),
        base_url=_public_base_url(request),
        include_inactive=includeInactive,
    )
    if not item:
        raise HTTPException(status_code=404, detail="Reference catalog item not found")
    return _reference_catalog_response(extra={"sap": item["sap"], "item": item})


@router.get("/catalog/starter/images/{sap}.png")
@rate_limit_history
async def get_reference_catalog_image(
    request: Request,
    sap: str = Path(..., pattern=r"^[A-Za-z0-9_-]{1,20}$"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> FileResponse:
    """Return a reviewed reference product image by SAP."""
    del request, decoded_token
    image_path = _fetch_reference_image_path(sap.strip())
    if not image_path:
        raise HTTPException(status_code=404, detail="Reference catalog image not found")
    return FileResponse(
        image_path,
        media_type="image/png",
        headers={"Cache-Control": "private, max-age=300, must-revalidate"},
    )


@router.get("/products")
@rate_limit_history
async def get_products(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return active products for a route."""
    await require_route_access(route, decoded_token, db)

    products_ref = db.collection("masterCatalog").document(route).collection("products")
    q = products_ref.where("active", "==", True).order_by("displayOrder")
    items = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["sap"] = data.get("sap") or doc.id
        if "name" not in data:
            data["name"] = data.get("fullName") or data.get("full_name") or data.get("description")
        if "casePack" not in data and "case_pack" in data:
            data["casePack"] = data.get("case_pack")
        items.append(data)

    logger.debug(f"GET /products route={route} count={len(items)} saps={[i.get('sap') for i in items[:10]]}...")
    return {"routeNumber": route, "products": items}


@router.get("/schedule")
@rate_limit_history
async def get_schedule(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return order cycles and schedule info for a route."""
    await require_route_access(route, decoded_token, db)

    from schedule_utils import get_schedule_info

    schedule_info = get_schedule_info(db, route)
    return {
        "routeNumber": route,
        **schedule_info,
    }


@router.get("/promos")
@rate_limit_history
async def get_promos(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return promos for a route (raw promo docs)."""
    await require_route_access(route, decoded_token, db)

    from firebase_loader import load_promotions

    promos = load_promotions(db, route)
    return {"routeNumber": route, "promos": promos}

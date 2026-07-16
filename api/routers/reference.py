"""Reference data router - products, stores, schedule, promos."""

from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from google.cloud import firestore
from psycopg2.extras import RealDictCursor

from ..dependencies import (
    get_pg_connection,
    return_pg_connection,
    verify_firebase_token,
    require_route_access,
    get_firestore,
)
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()
logger = logging.getLogger(__name__)

DEFAULT_REFERENCE_CATALOG_ID = "routespark-starter-catalog"


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


def _normalize_upc(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _normalize_reference_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "catalogId": _clean_text(row.get("catalog_id"), DEFAULT_REFERENCE_CATALOG_ID),
        "sap": _clean_text(row.get("sap")),
        "upc": _clean_optional_text(row.get("upc")),
        "brand": _clean_text(row.get("brand")),
        "category": _clean_text(row.get("category")),
        "fullName": _clean_text(row.get("full_name") or row.get("fullName"), "Unnamed Product"),
        "casePack": _clean_int(row.get("case_pack") or row.get("casePack"), 0),
        "displayOrder": _clean_int(row.get("display_order") or row.get("displayOrder"), 0),
        "source": _clean_optional_text(row.get("source")),
        "active": bool(row.get("active", True)),
    }


def _fetch_reference_items_by_search(
    query: str,
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    normalized_query = query.strip()
    normalized_upc = _normalize_upc(normalized_query)
    like_query = f"%{_escape_like(normalized_query)}%"
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
                    case_pack,
                    display_order,
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
                  AND active = TRUE
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
                    normalized_query,
                    normalized_query,
                    normalized_upc,
                    normalized_upc,
                    like_query,
                    like_query,
                    like_query,
                    limit,
                ],
            )
            return [_normalize_reference_item(dict(row)) for row in cur.fetchall()]
    finally:
        return_pg_connection(conn)


def _fetch_reference_item_by_sap(
    sap: str,
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
) -> Optional[Dict[str, Any]]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT catalog_id, sap, upc, full_name, brand, category,
                       case_pack, display_order, source, active
                FROM reference_catalog_items
                WHERE catalog_id = %s AND sap = %s AND active = TRUE
                LIMIT 1
                """,
                [catalog_id, sap],
            )
            row = cur.fetchone()
            return _normalize_reference_item(dict(row)) if row else None
    finally:
        return_pg_connection(conn)


def _fetch_reference_catalog_items(
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
    limit: int = 250,
) -> List[Dict[str, Any]]:
    conn = get_pg_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT catalog_id, sap, upc, full_name, brand, category,
                       case_pack, display_order, source, active
                FROM reference_catalog_items
                WHERE catalog_id = %s AND active = TRUE
                ORDER BY display_order NULLS LAST, sap
                LIMIT %s
                """,
                [catalog_id, limit],
            )
            return [_normalize_reference_item(dict(row)) for row in cur.fetchall()]
    finally:
        return_pg_connection(conn)


@router.get("/catalog/starter")
@rate_limit_history
async def get_starter_catalog(
    request: Request,
    limit: int = Query(250, ge=1, le=500),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Return the shared RouteSpark starter/reference catalog."""
    del request, decoded_token
    items = _fetch_reference_catalog_items(limit=limit)
    return {
        "catalogId": DEFAULT_REFERENCE_CATALOG_ID,
        "items": items,
    }


@router.get("/catalog/items/search")
@rate_limit_history
async def search_reference_catalog(
    request: Request,
    q: str = Query(..., min_length=1, max_length=120, description="SAP, UPC, or item description"),
    limit: int = Query(25, ge=1, le=50),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Search the shared RouteSpark reference catalog.

    This data is not route-private, but reads still require Firebase auth so the
    endpoint cannot be scraped anonymously.
    """
    del request, decoded_token
    query = q.strip()
    items = _fetch_reference_items_by_search(query, limit=limit)
    return {
        "catalogId": DEFAULT_REFERENCE_CATALOG_ID,
        "query": query,
        "items": items,
    }


@router.get("/catalog/starter/items/{sap}")
@rate_limit_history
async def get_reference_catalog_item(
    request: Request,
    sap: str = Path(..., pattern=r"^[A-Za-z0-9_-]{1,20}$"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Return one item from the shared RouteSpark reference catalog by SAP."""
    del request, decoded_token
    item = _fetch_reference_item_by_sap(sap.strip())
    if not item:
        raise HTTPException(status_code=404, detail="Reference catalog item not found")
    return {"catalogId": DEFAULT_REFERENCE_CATALOG_ID, "sap": item["sap"], "item": item}


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

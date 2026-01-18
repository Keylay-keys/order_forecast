"""Reference data router - products, stores, schedule, promos."""

from __future__ import annotations

from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
    get_duckdb,
)
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()


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

    print(f"[DEBUG] GET /products route={route} count={len(items)} saps={[i.get('sap') for i in items[:10]]}...")
    return {"routeNumber": route, "products": items}


@router.get("/stores")
@rate_limit_history
async def get_stores(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return stores for a route."""
    await require_route_access(route, decoded_token, db)

    stores_ref = db.collection("routes").document(route).collection("stores")
    q = stores_ref.order_by("name")
    stores = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["id"] = data.get("id") or doc.id
        stores.append(data)

    return {"routeNumber": route, "stores": stores}


@router.get("/schedule")
@rate_limit_history
async def get_schedule(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
    duckdb = Depends(get_duckdb),
) -> Dict[str, Any]:
    """Return order cycles and schedule info for a route."""
    await require_route_access(route, decoded_token, db)

    from schedule_utils import get_schedule_info

    schedule_info = get_schedule_info(db, route, db_client=duckdb._db_client)
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

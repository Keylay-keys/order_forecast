"""Orders router - create, update, finalize, and audit orders."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
    get_route_timezone,
)
from ..middleware.rate_limit import rate_limit_write
from ..models import (
    Order,
    OrderCreateRequest,
    OrderUpdateRequest,
    ErrorResponse,
)


router = APIRouter()


def _get_local_order_date(db: firestore.Client, route_number: str) -> str:
    """Return local date (YYYY-MM-DD) using route owner's timezone when possible."""
    tz_name = get_route_timezone(db, route_number)
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            return datetime.now(ZoneInfo(tz_name)).date().isoformat()
        except Exception:
            pass
    return datetime.utcnow().date().isoformat()


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    return None


def _log_order_audit(
    db: firestore.Client,
    order_id: str,
    route_number: str,
    user_id: str,
    action: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a lightweight audit entry for order changes."""
    db.collection("orders").document(order_id).collection("audit").add({
        "orderId": order_id,
        "routeNumber": route_number,
        "userId": user_id,
        "action": action,
        "meta": meta or {},
        "source": "web_portal",
        "createdAt": firestore.SERVER_TIMESTAMP,
    })


@router.get(
    "/orders/active",
    response_model=Optional[Order],
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def get_active_order(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Optional[Order]:
    """Get the active (draft) order for a route."""
    await require_route_access(route, decoded_token, db)

    orders_ref = db.collection("orders")
    q = (
        orders_ref.where("routeNumber", "==", route)
        .where("status", "==", "draft")
        .order_by("createdAt", direction=firestore.Query.DESCENDING)
        .limit(1)
    )
    docs = list(q.stream())
    if not docs:
        return None
    return Order(**docs[0].to_dict())


@router.get(
    "/orders/{order_id}",
    response_model=Order,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
async def get_order(
    request: Request,
    order_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Get an order by ID."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")
    
    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)
    
    return Order(**order_data)


@router.post(
    "/orders",
    response_model=Order,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_order(
    request: Request,
    payload: OrderCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Create a new draft order."""
    await require_route_access(payload.routeNumber, decoded_token, db)

    order_id = f"order-{payload.routeNumber}-{int(datetime.utcnow().timestamp() * 1000)}"
    now = datetime.utcnow()

    order_doc = {
        "id": order_id,
        "routeNumber": payload.routeNumber,
        "userId": decoded_token["uid"],
        "orderDate": _get_local_order_date(db, payload.routeNumber),
        "expectedDeliveryDate": payload.deliveryDate.isoformat(),
        "scheduleKey": payload.scheduleKey,
        "status": "draft",
        "stores": [],
        "createdAt": now,
        "updatedAt": now,
        "submittedAt": None,
        "orderCycleId": None,
        "notes": payload.notes,
        "isHolidaySchedule": payload.isHolidaySchedule,
    }

    db.collection("orders").document(order_id).set(order_doc)
    _log_order_audit(
        db,
        order_id,
        payload.routeNumber,
        decoded_token["uid"],
        "order_created",
        {"deliveryDate": payload.deliveryDate.isoformat(), "scheduleKey": payload.scheduleKey},
    )

    return Order(**order_doc)


@router.put(
    "/orders/{order_id}",
    response_model=Order,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def update_order(
    request: Request,
    order_id: str,
    payload: OrderUpdateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Update an order's stores/items with optimistic locking."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)

    if order_data.get("status") != "draft":
        raise HTTPException(400, "Order is not editable")

    # Optimistic locking: compare updatedAt if provided
    if payload.updatedAt:
        server_updated_at = _to_datetime(order_data.get("updatedAt"))
        if server_updated_at:
            if abs(server_updated_at.timestamp() - payload.updatedAt.timestamp()) > 1:
                raise HTTPException(409, "Order was modified by another session")

    now = datetime.utcnow()
    update_data = {
        "stores": [store.dict() for store in payload.stores],
        "updatedAt": now,
    }
    if payload.notes is not None:
        update_data["notes"] = payload.notes

    order_ref.update(update_data)

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_updated",
        {"storeCount": len(payload.stores)},
    )

    updated = order_ref.get().to_dict() or {}
    return Order(**updated)


@router.delete(
    "/orders/{order_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def delete_order(
    request: Request,
    order_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Delete a draft order."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)

    if order_data.get("status") != "draft":
        raise HTTPException(409, "Only draft orders can be deleted")

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_deleted",
    )

    order_ref.delete()

    return {"orderId": order_id, "status": "deleted"}


@router.post(
    "/orders/{order_id}/finalize",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def finalize_order(
    request: Request,
    order_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Finalize an order and trigger DuckDB sync via DBClient."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)

    now = datetime.utcnow()
    order_ref.update({
        "status": "finalized",
        "submittedAt": now,
        "updatedAt": now,
    })

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_finalized",
    )

    # Trigger DBManager sync (best-effort)
    sync_result: Dict[str, Any] = {"synced": False}
    try:
        from db_client import DBClient
        db_client = DBClient(db)
        result = db_client.sync_order(order_id, route_number)
        sync_result = {"synced": True, **result}
    except Exception as exc:
        sync_result = {"synced": False, "error": str(exc)}

    return {
        "orderId": order_id,
        "status": "finalized",
        "sync": sync_result,
    }


@router.get(
    "/orders/{order_id}/audit",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def get_order_audit(
    request: Request,
    order_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return audit log entries for an order."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)

    audit_ref = order_ref.collection("audit").order_by(
        "createdAt", direction=firestore.Query.DESCENDING
    ).limit(200)
    entries = []
    for doc in audit_ref.stream():
        data = doc.to_dict() or {}
        data["id"] = doc.id
        entries.append(data)

    return {
        "orderId": order_id,
        "entries": entries,
    }

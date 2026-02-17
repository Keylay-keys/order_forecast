"""Orders router - create, update, finalize, and audit orders."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    require_route_feature_access,
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

def _normalize_route_number(value: Any) -> str:
    v = str(value or "").strip()
    return v if v.isdigit() and len(v) <= 10 else ""


def _resolve_route_group_id(
    db: firestore.Client,
    *,
    requester_user_data: Dict[str, Any],
    order_route_number: str,
) -> str:
    """Resolve the master routeGroupId for transfer ledger operations.

    Transfers are grouped under the *owner's* primary route number (routeGroupId).
    For owners, this is `users/{uid}.profile.routeNumber`.
    For team members, resolve via `routes/{route}.ownerUid` then owner's profile.routeNumber.
    """
    profile = (requester_user_data or {}).get("profile", {}) or {}
    role = str(profile.get("role") or "").strip()
    if role == "owner":
        master = _normalize_route_number(profile.get("routeNumber"))
        if master:
            return master

    # Team member path: route doc -> owner uid -> owner's primary route number.
    route = _normalize_route_number(order_route_number)
    if not route:
        return ""

    try:
        route_doc = db.collection("routes").document(route).get()
        if not route_doc.exists:
            return ""
        route_data = route_doc.to_dict() or {}
        owner_uid = str(route_data.get("ownerUid") or route_data.get("userId") or "").strip()
        if not owner_uid:
            return ""
        owner_doc = db.collection("users").document(owner_uid).get()
        if not owner_doc.exists:
            return ""
        owner_data = owner_doc.to_dict() or {}
        owner_profile = owner_data.get("profile", {}) or {}
        return _normalize_route_number(owner_profile.get("routeNumber"))
    except Exception:
        return ""


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


def _order_ref(db: firestore.Client, route_number: str, order_id: str):
    """Build a route-scoped order document reference."""
    return (
        db.collection("routes")
        .document(route_number)
        .collection("orders")
        .document(order_id)
    )


def _log_order_audit(
    db: firestore.Client,
    order_id: str,
    route_number: str,
    user_id: str,
    action: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a lightweight audit entry for order changes."""
    _order_ref(db, route_number, order_id).collection("audit").add({
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

    orders_ref = db.collection("routes").document(route).collection("orders")
    q = (
        orders_ref.where("status", "==", "draft")
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
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Get an order by ID."""
    await require_route_access(route, decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()

    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    # Verify doc route matches path route
    if str(order_data.get("routeNumber", "")) != route:
        raise HTTPException(403, "Route mismatch")

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
    await require_route_feature_access(payload.routeNumber, "ordering", decoded_token, db)

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

    _order_ref(db, payload.routeNumber, order_id).set(order_doc)
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
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Update an order's stores/items with optimistic locking."""
    await require_route_feature_access(route, "ordering", decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    if route_number != route:
        raise HTTPException(403, "Route mismatch")

    if order_data.get("status") != "draft":
        raise HTTPException(400, "Order is not editable")

    # Optimistic locking: compare updatedAt if provided
    if payload.updatedAt:
        server_updated_at = _to_datetime(order_data.get("updatedAt"))
        if server_updated_at:
            if abs(server_updated_at.timestamp() - payload.updatedAt.timestamp()) > 1:
                raise HTTPException(409, "Order was modified by another session")

    now = datetime.utcnow()

    # IMPORTANT: Clients (notably the web portal) may send a minimal item payload
    # like {sap, quantity} and omit forecast metadata fields. Because `stores` is
    # stored as an ARRAY of MAPs in Firestore, a full `stores` replacement would
    # otherwise drop those omitted keys. We merge against the existing order doc
    # to preserve forecast metadata when the client didn't explicitly change it.
    existing_stores: List[Dict[str, Any]] = order_data.get("stores") or []
    existing_store_by_id: Dict[str, Dict[str, Any]] = {
        str(s.get("storeId")): s for s in existing_stores if s.get("storeId") is not None
    }

    merged_stores: List[Dict[str, Any]] = []
    for store in payload.stores:
        incoming_store = store.dict(exclude_unset=True)
        store_id = str(incoming_store.get("storeId", ""))
        existing_store = existing_store_by_id.get(store_id, {})

        existing_items = existing_store.get("items") or []
        existing_item_by_sap: Dict[str, Dict[str, Any]] = {
            str(it.get("sap")): it for it in existing_items if it.get("sap") is not None
        }

        merged_items: List[Dict[str, Any]] = []
        for item in store.items:
            incoming_item = item.dict(exclude_unset=True, by_alias=False)
            sap = str(incoming_item.get("sap", "")).strip()
            base = existing_item_by_sap.get(sap, {})
            merged_items.append({**base, **incoming_item})

        merged_store = {**existing_store, **incoming_store, "items": merged_items}
        merged_stores.append(merged_store)

    update_data = {
        "stores": merged_stores,
        "updatedAt": now,
    }
    if payload.notes is not None:
        update_data["notes"] = payload.notes

    # Transfer metadata (web portal parity)
    if payload.inboundTransfersUsed is not None:
        update_data["inboundTransfersUsed"] = [t.dict() for t in payload.inboundTransfersUsed]
    if payload.routeTransfers is not None:
        update_data["routeTransfers"] = [t.dict() for t in payload.routeTransfers]
    if payload.routeSplittingEnabled is not None:
        update_data["routeSplittingEnabled"] = payload.routeSplittingEnabled

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
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Delete a draft order."""
    requester_user_data = await require_route_feature_access(route, "ordering", decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    if route_number != route:
        raise HTTPException(403, "Route mismatch")

    if order_data.get("status") != "draft":
        raise HTTPException(409, "Only draft orders can be deleted")

    # Release transfer reservations before deleting (best-effort cleanup)
    route_group_id = _resolve_route_group_id(
        db,
        requester_user_data=requester_user_data,
        order_route_number=route_number,
    ) or route_number

    # 1. Release inbound transfer reservations
    inbound_transfers = order_data.get("inboundTransfersUsed") or []
    for transfer_use in inbound_transfers:
        transfer_key = transfer_use.get("transferKey")
        if not transfer_key:
            continue
        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )
            transfer_ref.update({
                f"reservedBy.{order_id}": firestore.DELETE_FIELD,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            })
        except Exception:
            pass  # Best-effort — continue even if cleanup fails

    # 2. Delete planned outbound transfers (only if no other orders have reserved from them)
    route_transfers = order_data.get("routeTransfers") or []
    for transfer_alloc in route_transfers:
        to_route = transfer_alloc.get("toRouteNumber")
        sap = transfer_alloc.get("sap")
        if not to_route or not sap:
            continue

        # Transfer key format: {orderId}:{fromRoute}:{toRoute}:{sap}
        transfer_key = f"{order_id}:{route_number}:{to_route}:{sap}".replace("/", "_")
        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )
            transfer_snap = transfer_ref.get()
            if transfer_snap.exists:
                transfer_data = transfer_snap.to_dict()
                status = transfer_data.get("status")
                reserved_by = transfer_data.get("reservedBy", {})
                reserved_total = sum(v for v in reserved_by.values() if isinstance(v, (int, float)))

                # Only delete if it's planned and has no reservations
                if status == "planned" and reserved_total == 0:
                    transfer_ref.delete()
        except Exception:
            pass  # Best-effort cleanup

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
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Finalize an order and trigger PostgreSQL sync."""
    requester_user_data = await require_route_feature_access(route, "ordering", decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    if route_number != route:
        raise HTTPException(403, "Route mismatch")

    now = datetime.utcnow()
    order_ref.update({
        "status": "finalized",
        "submittedAt": now,
        "updatedAt": now,
    })

    # Commit outbound transfers (change status from 'planned' to 'committed')
    route_group_id = _resolve_route_group_id(
        db,
        requester_user_data=requester_user_data,
        order_route_number=route_number,
    ) or route_number
    route_transfers = order_data.get("routeTransfers") or []
    uid = decoded_token["uid"]

    for transfer_alloc in route_transfers:
        to_route = transfer_alloc.get("toRouteNumber")
        sap = transfer_alloc.get("sap")
        units = transfer_alloc.get("units", 0)

        if not to_route or not sap or units <= 0:
            continue

        # Transfer key format: {orderId}:{fromRoute}:{toRoute}:{sap}
        transfer_key = f"{order_id}:{route_number}:{to_route}:{sap}".replace("/", "_")

        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )

            # Upsert with status='committed'
            transfer_data = {
                "routeGroupId": route_group_id,
                "purchaseRouteNumber": route_number,
                "fromRouteNumber": route_number,
                "toRouteNumber": to_route,
                "sap": sap,
                "units": units,
                "casePack": transfer_alloc.get("casePack", 1),
                "transferDate": transfer_alloc.get("transferDate") or order_data.get("expectedDeliveryDate") or order_data.get("orderDate"),
                "status": "committed",
                "reason": "pooled_order",
                "sourceOrderId": order_id,
                "createdByUid": uid,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }

            # Set createdAt if new doc
            transfer_snap = transfer_ref.get()
            if not transfer_snap.exists:
                transfer_data["createdAt"] = firestore.SERVER_TIMESTAMP

            # Merge to preserve reservedBy map
            transfer_ref.set(transfer_data, merge=True)
        except Exception as exc:
            # Best-effort — log but don't fail the finalize
            print(f"[finalize_order] Failed to commit transfer {transfer_key}: {exc}")

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_finalized",
    )

    # Trigger direct PostgreSQL sync (best-effort)
    sync_result: Dict[str, Any] = {"synced": False}
    try:
        from ..dependencies import get_pg_connection, return_pg_connection
        from db_manager_pg import handle_sync_order

        conn = get_pg_connection()
        try:
            result = handle_sync_order(conn, db, {
                'orderId': order_id,
                'routeNumber': route_number,
            })
            if 'error' in result:
                sync_result = {"synced": False, "error": result['error']}
            else:
                sync_result = {"synced": True, **result}
        finally:
            return_pg_connection(conn)
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
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return audit log entries for an order."""
    await require_route_access(route, decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    if str(order_data.get("routeNumber", "")) != route:
        raise HTTPException(403, "Route mismatch")

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

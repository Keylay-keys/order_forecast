"""Order history router - Firestore-authoritative finalized orders.

Endpoints:
    GET /api/history - Get paginated order history for a route
    GET /api/history/{order_id} - Get specific order details
    
PostgreSQL remains a derived forecast/training cache, but user-facing history
must read finalized order documents from Firebase to avoid stale sync artifacts.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any, Optional, List, Tuple, Dict
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException, Request

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore
)
from ..models import (
    OrderHistoryItem,
    OrderHistoryResponse,
    ErrorResponse
)
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()


def _encode_history_cursor(delivery_date: str, order_id: str) -> str:
    payload = json.dumps(
        {"deliveryDate": delivery_date, "orderId": order_id},
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_history_cursor(cursor: str) -> Tuple[str, str]:
    try:
        payload = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        decoded = json.loads(payload)
        delivery_date = str(decoded.get("deliveryDate") or "")
        order_id = str(decoded.get("orderId") or "")
        if not delivery_date or not order_id:
            raise ValueError("missing fields")
        # Validate date format to prevent malformed cursor input.
        datetime.strptime(delivery_date, "%Y-%m-%d")
        return delivery_date, order_id
    except (ValueError, json.JSONDecodeError, binascii.Error):
        raise HTTPException(400, "Invalid history cursor")


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        try:
            return value.to_datetime()
        except Exception:
            return None
    if isinstance(value, dict):
        seconds = value.get("seconds") or value.get("_seconds")
        nanos = value.get("nanoseconds") or value.get("_nanoseconds") or value.get("nanos") or 0
        if seconds is not None:
            try:
                return datetime.utcfromtimestamp(float(seconds) + float(nanos) / 1_000_000_000)
            except Exception:
                return None
    return None


def _date_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value or "")[:10]


def _fetch_firestore_products(db, route: str) -> Dict[str, Dict[str, Any]]:
    """Return active Firestore catalog metadata keyed by SAP.

    History is user-facing order truth, so it must use the same Firestore
    catalog source that Order Entry and the History spreadsheet use.
    """
    products: Dict[str, Dict[str, Any]] = {}
    try:
        products_ref = db.collection("masterCatalog").document(route).collection("products")
        for doc in products_ref.where("active", "==", True).stream():
            data = doc.to_dict() or {}
            sap = str(data.get("sap") or doc.id or "").strip()
            if not sap:
                continue
            products[sap] = {
                "casePack": _coerce_int(data.get("casePack") or data.get("case_pack")),
                "name": (
                    data.get("name")
                    or data.get("fullName")
                    or data.get("full_name")
                    or data.get("description")
                    or ""
                ),
            }
    except Exception as e:
        print(f"[history] Warning: Failed to fetch Firestore catalog products: {e}")
    return products


def _compute_firestore_order_totals(
    order_data: Dict[str, Any],
    products_by_sap: Dict[str, Dict[str, Any]],
) -> Tuple[int, int]:
    total_units = 0
    units_by_sap: Dict[str, int] = {}
    explicit_cases_by_sap: Dict[str, int] = {}

    for store in order_data.get("stores") or []:
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            quantity = _coerce_int(item.get("quantity"))
            if not sap or quantity <= 0:
                continue
            total_units += quantity
            units_by_sap[sap] = units_by_sap.get(sap, 0) + quantity
            explicit_cases_by_sap[sap] = explicit_cases_by_sap.get(sap, 0) + _coerce_int(item.get("cases"))

    total_cases = 0
    for sap, quantity in units_by_sap.items():
        case_pack = _coerce_int((products_by_sap.get(sap) or {}).get("casePack"))
        if case_pack > 0:
            total_cases += quantity // case_pack
        else:
            total_cases += explicit_cases_by_sap.get(sap, 0)

    return total_units, total_cases


def _order_sort_key(order_id: str, order_data: Dict[str, Any]) -> Tuple[str, str]:
    delivery_date = _date_string(
        order_data.get("expectedDeliveryDate") or order_data.get("deliveryDate")
    )
    return delivery_date, order_id


@router.get(
    "/history",
    response_model=OrderHistoryResponse,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse}
    }
)
@rate_limit_history
async def get_order_history(
    request: Request,  # Required for slowapi rate limiting
    route: str = Query(..., pattern=r'^\d{1,10}$', description="Route number"),
    weeks: int = Query(default=12, ge=1, le=52, description="Weeks of history"),
    cursor: Optional[str] = Query(default=None, description="Keyset cursor from prior response"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    limit: int = Query(default=50, ge=1, le=200, description="Results per page"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> OrderHistoryResponse:
    """Get paginated order history for a route.
    
    Returns orders from the last N weeks, sorted by delivery date descending.
    Supports keyset pagination via cursor. OFFSET remains for compatibility.
    
    Security:
    - Requires valid Firebase token
    - Verifies user has access to the route
    - All queries use parameterized statements
    
    Performance: Firestore is authoritative for user-facing order history.
    """
    # Verify route access
    await require_route_access(route, decoded_token, db)
    
    # Calculate date range
    cutoff_date = (datetime.utcnow() - timedelta(weeks=weeks)).strftime('%Y-%m-%d')

    try:
        products_by_sap = _fetch_firestore_products(db, route)
        orders_ref = db.collection("routes").document(route).collection("orders")
        snaps = list(orders_ref.where("status", "==", "finalized").stream())

        filtered: List[Tuple[str, Dict[str, Any]]] = []
        for snap in snaps:
            data = snap.to_dict() or {}
            delivery_date = _date_string(data.get("expectedDeliveryDate") or data.get("deliveryDate"))
            if delivery_date and delivery_date < cutoff_date:
                continue
            filtered.append((snap.id, data))

        filtered.sort(key=lambda item: _order_sort_key(item[0], item[1]), reverse=True)

        if cursor:
            cursor_date, cursor_order_id = _decode_history_cursor(cursor)
            filtered = [
                item for item in filtered
                if _order_sort_key(item[0], item[1]) < (cursor_date, cursor_order_id)
            ]
            page_orders = filtered[:limit]
            effective_offset = 0
        else:
            page_orders = filtered[offset:offset + limit]
            effective_offset = offset

        items: List[OrderHistoryItem] = []
        for order_id, data in page_orders:
            total_units, total_cases = _compute_firestore_order_totals(data, products_by_sap)
            finalized_at = _to_datetime(data.get("submittedAt") or data.get("finalizedAt"))
            items.append(OrderHistoryItem(
                orderId=order_id,
                routeNumber=str(data.get("routeNumber") or route),
                scheduleKey=str(data.get("scheduleKey") or ""),
                deliveryDate=_date_string(data.get("expectedDeliveryDate") or data.get("deliveryDate")),
                orderDate=_date_string(data.get("orderDate")) or None,
                finalizedAt=finalized_at,
                totalCases=total_cases,
                totalUnits=total_units,
                storeCount=len(data.get("stores") or []),
                status=str(data.get("status") or "finalized"),
            ))
        
        next_cursor = None
        if page_orders and len(page_orders) == limit:
            next_index = limit if cursor else offset + limit
            if next_index < len(filtered):
                last_order_id, last_data = page_orders[-1]
                next_cursor = _encode_history_cursor(
                    _date_string(last_data.get("expectedDeliveryDate") or last_data.get("deliveryDate")),
                    last_order_id,
                )

        return OrderHistoryResponse(
            items=items,
            total=len(filtered),
            offset=effective_offset,
            limit=limit,
            nextCursor=next_cursor,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"History error: {str(e)}")


@router.get(
    "/history/{order_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse}
    }
)
@rate_limit_history
async def get_order_details(
    request: Request,  # Required for slowapi rate limiting
    order_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> dict:
    """Get detailed order including line items.
    
    Security:
    - Verifies user has access to the order's route
    - Double-checks route ownership on nested data
    
    Firebase is authoritative for user-facing finalized order history.
    """
    try:
        route_number = str(order_id.split("-")[1]) if "-" in order_id else ""
        if not route_number:
            raise HTTPException(404, "Order not found")

        order_ref = db.collection("routes").document(route_number).collection("orders").document(order_id)
        order_doc = order_ref.get()
        if not order_doc.exists:
            raise HTTPException(404, "Order not found")

        order_data = order_doc.to_dict() or {}
        if str(order_data.get("routeNumber") or route_number) != route_number:
            raise HTTPException(403, "Route mismatch")

        await require_route_access(route_number, decoded_token, db)

        products_by_sap = _fetch_firestore_products(db, route_number)
        total_units, total_cases = _compute_firestore_order_totals(order_data, products_by_sap)

        store_numbers: Dict[str, Any] = {}
        try:
            stores_ref = db.collection("routes").document(route_number).collection("stores")
            for store_doc in stores_ref.stream():
                store_data = store_doc.to_dict() or {}
                store_num = store_data.get("number") or store_data.get("storeNumber")
                if store_num:
                    store_numbers[str(store_doc.id)] = store_num
                    alt_id = store_data.get("id") or store_data.get("storeId")
                    if alt_id:
                        store_numbers[str(alt_id)] = store_num
        except Exception as e:
            print(f"[history] Warning: Failed to fetch store numbers from Firestore: {e}")

        stores = []
        for store in order_data.get("stores") or []:
            store_id = str(store.get("storeId") or store.get("id") or "")
            store_name = str(store.get("storeName") or store.get("name") or "")
            items = []
            for item in store.get("items") or []:
                sap = str(item.get("sap") or "").strip()
                quantity = _coerce_int(item.get("quantity"))
                if not sap or quantity <= 0:
                    continue
                product_meta = products_by_sap.get(sap) or {}
                case_pack = _coerce_int(product_meta.get("casePack"))
                cases = _coerce_int(item.get("cases"))
                if cases <= 0 and case_pack > 0:
                    cases = quantity // case_pack
                items.append({
                    "sap": sap,
                    "productName": item.get("productName") or product_meta.get("name") or "",
                    "quantity": quantity,
                    "cases": cases,
                    "promoActive": bool(item.get("promoActive", False)),
                    "priorOrderContext": item.get("priorOrderContext"),
                })

            stores.append({
                "storeId": store_id,
                "storeName": store_name,
                "storeNumber": store.get("storeNumber") or store_numbers.get(store_id),
                "items": items,
            })

        finalized_at = _to_datetime(order_data.get("submittedAt") or order_data.get("finalizedAt"))

        return {
            "orderId": order_id,
            "routeNumber": route_number,
            "scheduleKey": str(order_data.get("scheduleKey") or ""),
            "deliveryDate": _date_string(order_data.get("expectedDeliveryDate") or order_data.get("deliveryDate")),
            "orderDate": _date_string(order_data.get("orderDate")) or None,
            "finalizedAt": finalized_at.isoformat() if finalized_at else None,
            "totalCases": total_cases,
            "totalUnits": total_units,
            "storeCount": len(order_data.get("stores") or []),
            "status": str(order_data.get("status") or "finalized"),
            "orderAdjustmentAppliedAtMs": order_data.get("orderAdjustmentAppliedAtMs"),
            "sapOrder": order_data.get("sapOrder") or [],
            "stores": stores,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"History error: {str(e)}")

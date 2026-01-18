"""Low quantity router - expose low-qty items for web portal."""

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


def _serialize_item(item) -> Dict[str, Any]:
    return {
        "product": item.sap,
        "expiryDate": item.expiry_date,
        "nextOrderDate": item.order_by_date or None,
        "nextDeliveryDate": item.delivery_date or None,
        "daysLeft": item.days_left,
        "willHaveGap": item.will_have_gap,
        "containerCode": item.container_code,
        "deliveryNumber": item.delivery_number,
    }


@router.get("/low-quantity")
@rate_limit_history
async def get_low_quantity(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    orderDate: Optional[str] = Query(default=None, description="Filter to order date YYYY-MM-DD"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
    duckdb = Depends(get_duckdb),
) -> Dict[str, Any]:
    """Return low-quantity items for a route."""
    await require_route_access(route, decoded_token, db)

    from low_quantity_loader import (
        get_items_for_order_date,
        get_gap_items,
        load_pcf_items,
        get_low_quantity_items,
        get_current_datetime,
        get_user_timezone,
    )
    from schedule_utils import get_order_cycles

    if orderDate:
        items = get_items_for_order_date(db, route, orderDate, db_client=duckdb._db_client)
        gap_items = get_gap_items(db, route, db_client=duckdb._db_client)
        # Sort by expiry date ascending (soonest first)
        items_sorted = sorted(items, key=lambda x: x.expiry_date or "9999-12-31")
        gap_items_sorted = sorted(gap_items, key=lambda x: x.expiry_date or "9999-12-31")
        return {
            "routeNumber": route,
            "orderDate": orderDate,
            "items": [_serialize_item(i) for i in items_sorted],
            "gapItems": [_serialize_item(i) for i in gap_items_sorted],
        }

    # No order date filter: return all low-qty items with their order-by dates
    pcf_items = load_pcf_items(db, route)
    order_cycles = get_order_cycles(db, route, db_client=duckdb._db_client)
    user_tz = get_user_timezone(db, route)
    today = get_current_datetime(user_tz)
    all_items = get_low_quantity_items(pcf_items, order_cycles, today=today)

    # Sort by expiry date ascending (soonest first)
    all_items_sorted = sorted(all_items, key=lambda x: x.expiry_date or "9999-12-31")

    return {
        "routeNumber": route,
        "items": [_serialize_item(i) for i in all_items_sorted],
    }

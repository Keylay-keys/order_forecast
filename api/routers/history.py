"""Order history router - DuckDB queries for historical orders.

Endpoints:
    GET /api/history - Get paginated order history for a route
    GET /api/history/{order_id} - Get specific order details
"""

from __future__ import annotations

from typing import Optional, List
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException, Request

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_duckdb,
    get_firestore
)
from ..models import (
    OrderHistoryItem,
    OrderHistoryResponse,
    ErrorResponse
)
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()


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
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    limit: int = Query(default=50, ge=1, le=200, description="Results per page"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
    duckdb = Depends(get_duckdb)
) -> OrderHistoryResponse:
    """Get paginated order history for a route.
    
    Returns orders from the last N weeks, sorted by delivery date descending.
    
    Security:
    - Requires valid Firebase token
    - Verifies user has access to the route
    - All queries use parameterized statements
    """
    # Verify route access
    await require_route_access(route, decoded_token, db)
    
    # Calculate date range
    cutoff_date = (datetime.utcnow() - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
    
    # Count total matching orders
    count_result = duckdb.execute("""
        SELECT COUNT(*) as total
        FROM orders_historical
        WHERE route_number = ?
          AND delivery_date >= ?
    """, [route, cutoff_date]).fetchone()
    
    total = count_result[0] if count_result else 0
    
    # Fetch orders with pagination (derive total_cases when missing)
    rows = duckdb.execute("""
        WITH cases_by_order AS (
            SELECT
                li.order_id,
                SUM(
                    CASE
                        WHEN pc.case_pack > 0 THEN FLOOR(li.quantity / pc.case_pack)
                        ELSE 0
                    END
                ) AS total_cases
            FROM order_line_items li
            LEFT JOIN product_catalog pc
                ON li.sap = pc.sap AND li.route_number = pc.route_number
            WHERE li.route_number = ?
              AND li.delivery_date >= ?
            GROUP BY li.order_id
        )
        SELECT 
            o.order_id,
            o.route_number,
            o.schedule_key,
            o.delivery_date,
            o.order_date,
            o.finalized_at,
            COALESCE(c.total_cases, o.total_cases, 0) AS total_cases,
            o.total_units,
            o.store_count,
            o.status
        FROM orders_historical o
        LEFT JOIN cases_by_order c
            ON c.order_id = o.order_id
        WHERE o.route_number = ?
          AND o.delivery_date >= ?
        ORDER BY o.delivery_date DESC
        LIMIT ?
        OFFSET ?
    """, [route, cutoff_date, route, cutoff_date, limit, offset]).fetchall()
    
    # Convert to response models
    items: List[OrderHistoryItem] = []
    for row in rows:
        items.append(OrderHistoryItem(
            orderId=row[0],
            routeNumber=row[1],
            scheduleKey=row[2] or '',
            deliveryDate=str(row[3]) if row[3] else '',
            orderDate=str(row[4]) if row[4] else None,
            finalizedAt=row[5],
            totalCases=row[6] or 0,
            totalUnits=row[7] or 0,
            storeCount=row[8] or 0,
            status=row[9] or 'finalized'
        ))
    
    return OrderHistoryResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit
    )


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
    duckdb = Depends(get_duckdb)
) -> dict:
    """Get detailed order including line items.
    
    Security:
    - Verifies user has access to the order's route
    - Double-checks route ownership on nested data
    """
    # First get the order to find route
    order_row = duckdb.execute("""
        SELECT 
            order_id, route_number, schedule_key, delivery_date,
            order_date, finalized_at, total_cases, total_units,
            store_count, status
        FROM orders_historical
        WHERE order_id = ?
    """, [order_id]).fetchone()
    
    if not order_row:
        raise HTTPException(404, "Order not found")
    
    route_number = order_row[1]
    
    # Verify route access
    await require_route_access(route_number, decoded_token, db)
    
    # Fetch line items with calculated cases from product catalog
    # Join with stores table to get user-defined store_number
    line_items = duckdb.execute("""
        SELECT 
            oli.store_id, 
            oli.store_name, 
            oli.sap, 
            COALESCE(oli.product_name, pc.full_name) as product_name,
            oli.quantity, 
            CASE 
                WHEN oli.cases > 0 THEN oli.cases
                WHEN pc.case_pack > 0 THEN CAST(FLOOR(oli.quantity / pc.case_pack) AS INTEGER)
                ELSE 0 
            END as cases,
            oli.promo_active,
            COALESCE(pc.case_pack, 0) as case_pack,
            s.store_number
        FROM order_line_items oli
        LEFT JOIN product_catalog pc ON oli.sap = pc.sap AND oli.route_number = pc.route_number
        LEFT JOIN stores s ON oli.store_id = s.store_id
        WHERE oli.order_id = ? AND oli.route_number = ?
        ORDER BY oli.store_name, oli.sap ASC
    """, [order_id, route_number]).fetchall()
    
    # Group by store
    stores_dict = {}
    total_calculated_cases = 0
    stores_needing_number = []  # Track stores where DuckDB didn't have the number
    
    for row in line_items:
        store_id = row[0]
        cases = row[5] or 0
        store_number = row[8] or None  # User-defined store number from DuckDB
        total_calculated_cases += cases
        if store_id not in stores_dict:
            stores_dict[store_id] = {
                "storeId": store_id,
                "storeName": row[1] or '',
                "storeNumber": store_number,  # User-defined number for display
                "items": []
            }
            if store_number is None:
                stores_needing_number.append(store_id)
        stores_dict[store_id]["items"].append({
            "sap": row[2],
            "productName": row[3] or '',
            "quantity": row[4] or 0,
            "cases": cases,
            "promoActive": bool(row[6])
        })
    
    # Fallback: fetch store numbers from Firestore if DuckDB didn't have them
    if stores_needing_number:
        try:
            stores_ref = db.collection('routes').document(route_number).collection('stores')
            for store_id in stores_needing_number:
                store_doc = stores_ref.document(store_id).get()
                if store_doc.exists:
                    store_data = store_doc.to_dict()
                    # Mobile app saves as 'number', not 'storeNumber'
                    store_num = store_data.get('number')
                    if store_num and store_id in stores_dict:
                        stores_dict[store_id]["storeNumber"] = store_num
        except Exception as e:
            # Non-fatal: just log and continue without store numbers
            print(f"[history] Warning: Failed to fetch store numbers from Firestore: {e}")
    
    # Use calculated cases if stored value is 0
    final_total_cases = order_row[6] if order_row[6] else total_calculated_cases
    
    return {
        "orderId": order_row[0],
        "routeNumber": order_row[1],
        "scheduleKey": order_row[2] or '',
        "deliveryDate": str(order_row[3]) if order_row[3] else '',
        "orderDate": str(order_row[4]) if order_row[4] else None,
        "finalizedAt": order_row[5].isoformat() if order_row[5] else None,
        "totalCases": final_total_cases,
        "totalUnits": order_row[7] or 0,
        "storeCount": order_row[8] or 0,
        "status": order_row[9] or 'finalized',
        "stores": list(stores_dict.values())
    }

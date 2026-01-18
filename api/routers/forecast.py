"""Forecast router - retrieve and apply forecasts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
    get_duckdb,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import (
    ForecastResponse,
    ForecastPayload,
    ForecastItem,
    ErrorResponse,
)


router = APIRouter()


def _get_latest_forecast(
    db: firestore.Client,
    route_number: str,
    delivery_date: str,
    schedule_key: str,
) -> Optional[Dict[str, Any]]:
    """Return latest cached forecast doc (exact match, then scheduleKey fallback)."""
    forecasts_ref = db.collection("forecasts").document(route_number).collection("cached")

    exact_query = (
        forecasts_ref.where("deliveryDate", "==", delivery_date)
        .where("scheduleKey", "==", schedule_key)
    )
    docs = list(exact_query.stream())

    if not docs:
        fallback_query = forecasts_ref.where("scheduleKey", "==", schedule_key)
        docs = list(fallback_query.stream())

    if not docs:
        return None

    # Choose newest by createdAt
    docs.sort(
        key=lambda d: (d.to_dict() or {}).get("createdAt") or datetime.min,
        reverse=True,
    )
    data = docs[0].to_dict() or {}

    # Skip expired
    expires_at = _to_naive_utc(data.get("expiresAt"))
    if expires_at and expires_at < _to_naive_utc(datetime.now(timezone.utc)):
        return None

    return data


def _to_naive_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def _get_last_finalized_at(duckdb, route_number: str, schedule_key: Optional[str]) -> Optional[datetime]:
    if schedule_key:
        row = duckdb.execute(
            """
            SELECT MAX(synced_at) AS last_submitted
            FROM orders_historical
            WHERE route_number = ? AND schedule_key = ?
            """,
            [route_number, schedule_key],
        ).fetchone()
    else:
        row = duckdb.execute(
            """
            SELECT MAX(synced_at) AS last_submitted
            FROM orders_historical
            WHERE route_number = ?
            """,
            [route_number],
        ).fetchone()
    if row and row[0]:
        return _to_naive_utc(row[0])
    return None


def _log_order_audit(
    db: firestore.Client,
    order_id: str,
    route_number: str,
    user_id: str,
    action: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
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
    "/forecast",
    response_model=ForecastResponse,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_forecast(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    deliveryDate: str = Query(..., description="Delivery date YYYY-MM-DD"),
    scheduleKey: str = Query(..., description="Schedule key (monday, tuesday, etc.)"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
    duckdb = Depends(get_duckdb),
) -> ForecastResponse:
    """Get cached forecast for a delivery date + schedule."""
    await require_route_access(route, decoded_token, db)

    forecast_doc = _get_latest_forecast(db, route, deliveryDate, scheduleKey)
    if not forecast_doc:
        return ForecastResponse(
            forecastAvailable=False,
            reason="no_data",
        )

    payload = ForecastPayload(
        forecastId=forecast_doc.get("forecastId") or forecast_doc.get("id", ""),
        deliveryDate=forecast_doc.get("deliveryDate") or deliveryDate,
        scheduleKey=forecast_doc.get("scheduleKey") or scheduleKey,
        generatedAt=forecast_doc.get("generatedAt") or forecast_doc.get("createdAt"),
        items=[ForecastItem(**item) for item in forecast_doc.get("items", [])],
    )

    last_finalized = _get_last_finalized_at(duckdb, route, scheduleKey)
    is_stale = False
    stale_reason = None
    generated_at = _to_naive_utc(payload.generatedAt)
    if last_finalized and generated_at and generated_at < last_finalized:
        is_stale = True
        stale_reason = "order_finalized_after_forecast"

    return ForecastResponse(
        forecastAvailable=True,
        forecast=payload,
        isStale=is_stale,
        staleReason=stale_reason,
    )


@router.get(
    "/forecast/status",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_forecast_status(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    scheduleKey: Optional[str] = Query(default=None),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
    duckdb = Depends(get_duckdb),
) -> Dict[str, Any]:
    """Return forecast status for a route (history count + last update)."""
    await require_route_access(route, decoded_token, db)

    status_doc = db.collection("forecasts").document(route).get()
    status_data = status_doc.to_dict() or {}

    last_finalized = _get_last_finalized_at(duckdb, route, scheduleKey)

    return {
        "orderCount": status_data.get("orderCount"),
        "minOrdersRequired": status_data.get("minOrdersRequired"),
        "lastUpdated": status_data.get("lastUpdated"),
        "lastFinalizedAt": last_finalized,
    }


@router.post(
    "/forecast/apply/{order_id}",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def apply_forecast(
    request: Request,
    order_id: str,
    payload: Dict[str, Any],
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Apply forecast items to a draft order."""
    order_ref = db.collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    await require_route_access(route_number, decoded_token, db)

    if order_data.get("status") != "draft":
        raise HTTPException(400, "Order is not editable")

    forecast_id = payload.get("forecastId") or ""
    items: List[Dict[str, Any]] = payload.get("items") or []

    stores = order_data.get("stores") or []
    stores_updated = set()
    items_applied = 0
    now = datetime.utcnow()

    for item in items:
        if item.get("recommendedUnits", 0) <= 0:
            continue
        store_id = item.get("storeId")
        sap = item.get("sap")
        if not store_id or not sap:
            continue

        store_order = next((s for s in stores if s.get("storeId") == store_id), None)
        if not store_order:
            store_order = {
                "storeId": store_id,
                "storeName": item.get("storeName") or f"Store-{store_id[-6:]}",
                "items": [],
            }
            stores.append(store_order)
        elif not store_order.get("storeName") and item.get("storeName"):
            store_order["storeName"] = item.get("storeName")

        existing_index = next((i for i, it in enumerate(store_order["items"]) if it.get("sap") == sap), -1)
        item_data = {
            "sap": sap,
            "quantity": item.get("recommendedUnits", 0),
            "enteredAt": now,
            "forecastSuggestedUnits": item.get("recommendedUnits", 0),
            "forecastSuggestedCases": item.get("recommendedCases"),
            "userAdjusted": False,
            "userDelta": 0,
            "forecastId": forecast_id,
        }

        for key in (
            "promoActive",
            "promoLiftPct",
            "isFirstWeekend",
            "confidence",
            "source",
            "priorOrderContext",
            "expiryReplacement",
        ):
            if key in item:
                item_data[key] = item.get(key)

        if existing_index >= 0:
            store_order["items"][existing_index] = item_data
        else:
            store_order["items"].append(item_data)

        items_applied += 1
        stores_updated.add(store_id)

    order_ref.update({
        "stores": stores,
        "updatedAt": now,
    })

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "forecast_applied",
        {"forecastId": forecast_id, "itemsApplied": items_applied},
    )

    return {
        "orderId": order_id,
        "forecastId": forecast_id,
        "itemsApplied": items_applied,
        "storesUpdated": len(stores_updated),
    }


@router.get(
    "/forecast/insights",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_forecast_insights(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    weeks: int = Query(default=12, ge=1, le=52, description="Weeks of history to analyze"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
    duckdb = Depends(get_duckdb),
) -> Dict[str, Any]:
    """Return forecast accuracy insights based on user corrections."""
    await require_route_access(route, decoded_token, db)

    from datetime import timedelta
    cutoff_date = (datetime.utcnow() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")

    # Overall accuracy metrics
    accuracy_row = duckdb.execute("""
        SELECT
            COUNT(*) as total_predictions,
            SUM(CASE WHEN correction_delta = 0 THEN 1 ELSE 0 END) as exact_matches,
            SUM(CASE WHEN ABS(correction_delta) <= 2 THEN 1 ELSE 0 END) as close_matches,
            AVG(ABS(correction_delta)) as avg_deviation,
            SUM(CASE WHEN correction_delta > 0 THEN 1 ELSE 0 END) as under_forecasted,
            SUM(CASE WHEN correction_delta < 0 THEN 1 ELSE 0 END) as over_forecasted
        FROM forecast_corrections
        WHERE route_number = ?
          AND delivery_date >= ?
          AND predicted_units > 0
    """, [route, cutoff_date]).fetchone()

    total_predictions = accuracy_row[0] or 0
    exact_matches = accuracy_row[1] or 0
    close_matches = accuracy_row[2] or 0
    avg_deviation = round(accuracy_row[3] or 0, 1)
    under_forecasted = accuracy_row[4] or 0
    over_forecasted = accuracy_row[5] or 0

    accuracy_pct = round((exact_matches / total_predictions * 100) if total_predictions > 0 else 0, 1)
    close_pct = round((close_matches / total_predictions * 100) if total_predictions > 0 else 0, 1)

    # Top under-forecasted items (user increased quantities)
    under_rows = duckdb.execute("""
        SELECT
            fc.sap,
            COALESCE(pc.full_name, fc.sap) as product_name,
            COUNT(*) as correction_count,
            ROUND(AVG(fc.correction_delta), 1) as avg_increase
        FROM forecast_corrections fc
        LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
        WHERE fc.route_number = ?
          AND fc.delivery_date >= ?
          AND fc.correction_delta > 0
          AND fc.predicted_units > 0
        GROUP BY fc.sap, pc.full_name
        HAVING COUNT(*) >= 3
        ORDER BY avg_increase DESC
        LIMIT 5
    """, [route, cutoff_date]).fetchall()

    under_forecasted_items = [
        {"sap": row[0], "productName": row[1], "count": row[2], "avgIncrease": row[3]}
        for row in under_rows
    ]

    # Top over-forecasted items (user decreased quantities)
    over_rows = duckdb.execute("""
        SELECT
            fc.sap,
            COALESCE(pc.full_name, fc.sap) as product_name,
            COUNT(*) as correction_count,
            ROUND(AVG(ABS(fc.correction_delta)), 1) as avg_decrease
        FROM forecast_corrections fc
        LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
        WHERE fc.route_number = ?
          AND fc.delivery_date >= ?
          AND fc.correction_delta < 0
          AND fc.predicted_units > 0
        GROUP BY fc.sap, pc.full_name
        HAVING COUNT(*) >= 3
        ORDER BY avg_decrease DESC
        LIMIT 5
    """, [route, cutoff_date]).fetchall()

    over_forecasted_items = [
        {"sap": row[0], "productName": row[1], "count": row[2], "avgDecrease": row[3]}
        for row in over_rows
    ]

    # Most frequently corrected items
    freq_rows = duckdb.execute("""
        SELECT
            fc.sap,
            COALESCE(pc.full_name, fc.sap) as product_name,
            COUNT(*) as correction_count,
            SUM(CASE WHEN fc.correction_delta > 0 THEN 1 ELSE 0 END) as times_increased,
            SUM(CASE WHEN fc.correction_delta < 0 THEN 1 ELSE 0 END) as times_decreased
        FROM forecast_corrections fc
        LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
        WHERE fc.route_number = ?
          AND fc.delivery_date >= ?
          AND fc.correction_delta != 0
          AND fc.predicted_units > 0
        GROUP BY fc.sap, pc.full_name
        ORDER BY correction_count DESC
        LIMIT 5
    """, [route, cutoff_date]).fetchall()

    frequently_corrected = [
        {
            "sap": row[0],
            "productName": row[1],
            "count": row[2],
            "timesIncreased": row[3],
            "timesDecreased": row[4],
        }
        for row in freq_rows
    ]

    trend_rows = duckdb.execute("""
        SELECT
            date_trunc('week', submitted_at) as week_start,
            COUNT(*) as total_predictions,
            SUM(CASE WHEN correction_delta = 0 THEN 1 ELSE 0 END) as exact_matches,
            AVG(ABS(correction_delta)) as avg_deviation
        FROM forecast_corrections
        WHERE route_number = ?
          AND submitted_at >= ?
          AND predicted_units > 0
        GROUP BY week_start
        ORDER BY week_start ASC
    """, [route, cutoff_date]).fetchall()

    trend = []
    for row in trend_rows:
        week_start = row[0]
        total = row[1] or 0
        exact = row[2] or 0
        avg_dev = round(row[3] or 0, 1)
        accuracy = round((exact / total * 100) if total > 0 else 0, 1)
        trend.append({
            "weekStart": str(week_start) if week_start else None,
            "accuracyPct": accuracy,
            "avgDeviation": avg_dev,
            "totalPredictions": total,
        })

    return {
        "totalPredictions": total_predictions,
        "accuracyPct": accuracy_pct,
        "closePct": close_pct,
        "avgDeviation": avg_deviation,
        "underForecastedCount": under_forecasted,
        "overForecastedCount": over_forecasted,
        "underForecastedItems": under_forecasted_items,
        "overForecastedItems": over_forecasted_items,
        "frequentlyCorrected": frequently_corrected,
        "trend": trend,
        "windowWeeks": weeks,
    }

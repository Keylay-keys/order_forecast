"""Forecast router - retrieve and apply forecasts."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    require_route_feature_access,
    get_firestore,
    get_pg_connection,
    return_pg_connection,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import (
    ForecastResponse,
    ForecastPayload,
    ForecastItem,
    ErrorResponse,
    ApplyForecastRequest,
)


router = APIRouter()


def _safe_pct_change(new_value: float, old_value: float) -> float:
    if old_value == 0:
        return 0.0
    return (new_value - old_value) / abs(old_value) * 100.0


def _safe_improvement_pct(model_value: float, naive_value: float) -> float:
    if naive_value == 0:
        return 0.0
    return (naive_value - model_value) / abs(naive_value) * 100.0


def _weighted_mean(df: pd.DataFrame, value_col: str, weight_col: str = "fold_count") -> float:
    if df.empty or value_col not in df.columns:
        return 0.0
    values = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)
    if weight_col in df.columns:
        weights = pd.to_numeric(df[weight_col], errors="coerce").fillna(0.0)
        denom = float(weights.sum())
        if denom > 0:
            return float((values * weights).sum() / denom)
    return float(values.mean()) if len(values) else 0.0


def _list_backtest_files(backtest_dir: Path) -> List[Path]:
    if not backtest_dir.exists():
        return []
    return sorted(backtest_dir.glob("walk_forward_scorecard_*.csv"), key=lambda p: p.name)


def _scorecard_stamp(scorecard_path: Path) -> str:
    # walk_forward_scorecard_YYYYMMDD_HHMMSS.csv
    stem = scorecard_path.stem
    prefix = "walk_forward_scorecard_"
    if stem.startswith(prefix):
        return stem[len(prefix):]
    return ""


def _folds_path_for_scorecard(scorecard_path: Path) -> Optional[Path]:
    stamp = _scorecard_stamp(scorecard_path)
    if not stamp:
        return None
    folds_path = scorecard_path.with_name(f"walk_forward_folds_{stamp}.csv")
    return folds_path if folds_path.exists() else None


def _load_route_scorecard_summary(
    route_number: str,
    window_days: int,
    schedule_key: Optional[str],
) -> Dict[str, Any]:
    backtest_dir = Path(
        os.environ.get("FORECAST_BACKTEST_DIR", "/app/logs/order-forecast/backtests")
    ).resolve()
    allowed_dirs = [Path("/app/logs").resolve(), Path("/srv/routespark/logs").resolve()]
    if not any(str(backtest_dir).startswith(str(d)) for d in allowed_dirs):
        raise HTTPException(status_code=500, detail="invalid_backtest_path")

    scorecards = _list_backtest_files(backtest_dir)
    if not scorecards:
        raise HTTPException(status_code=404, detail="no_scorecard_available")

    latest = scorecards[-1]
    latest_df = pd.read_csv(latest)
    latest_df["route_number"] = latest_df["route_number"].astype(str)
    route_df = latest_df[latest_df["route_number"] == str(route_number)].copy()
    if schedule_key:
        route_df = route_df[
            route_df["schedule_key"].astype(str).str.lower() == str(schedule_key).lower()
        ]
    if route_df.empty:
        raise HTTPException(status_code=404, detail="no_route_scorecard_available")

    folds_path = _folds_path_for_scorecard(latest)
    mode_mix = {"copy_last_order": 0, "schedule_aware": 0, "store_centric": 0}
    orders_evaluated = int(route_df["fold_count"].sum()) if "fold_count" in route_df.columns else 0
    if folds_path and folds_path.exists():
        folds_df = pd.read_csv(folds_path)
        folds_df["route_number"] = folds_df["route_number"].astype(str)
        folds_route = folds_df[folds_df["route_number"] == str(route_number)].copy()
        if schedule_key:
            folds_route = folds_route[
                folds_route["schedule_key"].astype(str).str.lower() == str(schedule_key).lower()
            ]
        if not folds_route.empty:
            if "mode" in folds_route.columns:
                counts = folds_route["mode"].value_counts().to_dict()
                for k in mode_mix.keys():
                    mode_mix[k] = int(counts.get(k, 0))
            if "target_delivery_date" in folds_route.columns:
                orders_evaluated = int(folds_route["target_delivery_date"].nunique())

    line_wape_model = _weighted_mean(route_df, "mean_line_wape_model")
    line_wape_naive = _weighted_mean(route_df, "mean_line_wape_naive")
    order_wape_model = _weighted_mean(route_df, "mean_order_total_wape_model")
    order_wape_naive = _weighted_mean(route_df, "mean_order_total_wape_naive")
    zero_touch_model = _weighted_mean(route_df, "order_zero_touch_rate_model")
    zero_touch_naive = _weighted_mean(route_df, "order_zero_touch_rate_naive")
    coverage = _weighted_mean(route_df, "mean_line_band_coverage_10_90")
    under_rate = _weighted_mean(route_df, "mean_line_band_under_rate_10_90")
    over_rate = _weighted_mean(route_df, "mean_line_band_over_rate_10_90")

    # Prior window trend (previous scorecard file, same route/filter)
    trend = {
        "lineWapeLiftPctDelta": 0.0,
        "zeroTouchLiftPctDelta": 0.0,
        "coverage10_90Delta": 0.0,
    }
    if len(scorecards) >= 2:
        prev = scorecards[-2]
        prev_df = pd.read_csv(prev)
        prev_df["route_number"] = prev_df["route_number"].astype(str)
        prev_route = prev_df[prev_df["route_number"] == str(route_number)].copy()
        if schedule_key:
            prev_route = prev_route[
                prev_route["schedule_key"].astype(str).str.lower() == str(schedule_key).lower()
            ]
        if not prev_route.empty:
            prev_line_wape_model = _weighted_mean(prev_route, "mean_line_wape_model")
            prev_line_wape_naive = _weighted_mean(prev_route, "mean_line_wape_naive")
            prev_zero_touch_model = _weighted_mean(prev_route, "order_zero_touch_rate_model")
            prev_zero_touch_naive = _weighted_mean(prev_route, "order_zero_touch_rate_naive")
            prev_coverage = _weighted_mean(prev_route, "mean_line_band_coverage_10_90")

            line_lift = _safe_improvement_pct(line_wape_model, line_wape_naive)
            prev_line_lift = _safe_improvement_pct(prev_line_wape_model, prev_line_wape_naive)
            zt_lift = _safe_improvement_pct(-zero_touch_model, -zero_touch_naive)  # model should be larger
            prev_zt_lift = _safe_improvement_pct(-prev_zero_touch_model, -prev_zero_touch_naive)
            trend = {
                "lineWapeLiftPctDelta": round(line_lift - prev_line_lift, 2),
                "zeroTouchLiftPctDelta": round(zt_lift - prev_zt_lift, 2),
                "coverage10_90Delta": round(coverage - prev_coverage, 4),
            }

    line_lift_pct = _safe_improvement_pct(line_wape_model, line_wape_naive)
    order_lift_pct = _safe_improvement_pct(order_wape_model, order_wape_naive)
    zero_touch_lift_pct = _safe_pct_change(zero_touch_model, zero_touch_naive)

    if line_lift_pct >= 5.0:
        learning_state = "improving"
    elif line_lift_pct <= -5.0:
        learning_state = "needs_review"
    else:
        learning_state = "stable"

    if coverage >= 0.80:
        confidence_state = "calibrated"
    elif coverage >= 0.65:
        confidence_state = "almost_calibrated"
    else:
        confidence_state = "low_calibration"

    recommended_action = (
        "review_low_confidence_badges_only" if coverage < 0.80 else "review_exceptions_then_submit"
    )

    return {
        "routeNumber": str(route_number),
        "asOf": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "windowDays": int(window_days),
        "ordersEvaluated": int(orders_evaluated),
        "schedulesEvaluated": sorted(route_df["schedule_key"].astype(str).unique().tolist()),
        "modeMix": mode_mix,
        "quality": {
            "lineWapeModel": round(line_wape_model, 4),
            "lineWapeNaive": round(line_wape_naive, 4),
            "lineWapeLiftPct": round(line_lift_pct, 2),
            "orderWapeModel": round(order_wape_model, 4),
            "orderWapeNaive": round(order_wape_naive, 4),
            "orderWapeLiftPct": round(order_lift_pct, 2),
            "zeroTouchRateModel": round(zero_touch_model, 4),
            "zeroTouchRateNaive": round(zero_touch_naive, 4),
            "zeroTouchLiftPct": round(zero_touch_lift_pct, 2),
            "lineBandCoverage10_90": round(coverage, 4),
            "bandUnderRate10_90": round(under_rate, 4),
            "bandOverRate10_90": round(over_rate, 4),
        },
        "trendVsPriorWindow": trend,
        "status": {
            "learningState": learning_state,
            "confidenceState": confidence_state,
            "recommendedAction": recommended_action,
        },
    }


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


def _get_last_finalized_at(route_number: str, schedule_key: Optional[str]) -> Optional[datetime]:
    """Get the last finalized order timestamp using direct PostgreSQL.
    
    Performance: Direct PostgreSQL connection (no Firestore round-trip).
    """
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        if schedule_key:
            cur.execute(
                """
                SELECT MAX(synced_at) AS last_submitted
                FROM orders_historical
                WHERE route_number = %s AND schedule_key = %s
                """,
                [route_number, schedule_key],
            )
        else:
            cur.execute(
                """
                SELECT MAX(synced_at) AS last_submitted
                FROM orders_historical
                WHERE route_number = %s
                """,
                [route_number],
            )
        
        row = cur.fetchone()
        cur.close()
        return_pg_connection(conn)
        conn = None
        
        if row and row[0]:
            return _to_naive_utc(row[0])
        return None
        
    except Exception:
        if conn is not None:
            return_pg_connection(conn)
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
) -> ForecastResponse:
    """Get cached forecast for a delivery date + schedule.
    
    Performance: Direct PostgreSQL connection for staleness check.
    """
    await require_route_feature_access(route, "forecasting", decoded_token, db)

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

    # Cross-cycle dependency: finalizing one schedule can affect upcoming
    # forecasts in the other schedule. Use route-level finalized time.
    last_finalized = _get_last_finalized_at(route, None)
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
    "/forecast/learning/{route_number}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_forecast_learning(
    request: Request,
    route_number: str,
    windowDays: int = Query(default=90, ge=30, le=365),
    scheduleKey: Optional[str] = Query(default=None),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return route-level forecast learning summary for web Forecast UX."""
    if not route_number.isdigit():
        raise HTTPException(status_code=400, detail="invalid_route_number")
    await require_route_feature_access(route_number, "forecasting", decoded_token, db)
    return _load_route_scorecard_summary(
        route_number=route_number,
        window_days=windowDays,
        schedule_key=scheduleKey,
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
) -> Dict[str, Any]:
    """Return forecast status for a route (history count + last update).
    
    Performance: Direct PostgreSQL connection for lastFinalizedAt.
    """
    await require_route_feature_access(route, "forecasting", decoded_token, db)

    status_doc = db.collection("forecasts").document(route).get()
    status_data = status_doc.to_dict() or {}

    last_finalized = _get_last_finalized_at(route, scheduleKey)

    return {
        "orderCount": status_data.get("orderCount"),
        "minOrdersRequired": status_data.get("minOrdersRequired"),
        "hasTrainedModel": status_data.get("hasTrainedModel", False),
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
    payload: ApplyForecastRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Apply forecast items to a draft order."""
    await require_route_feature_access(route, "forecasting", decoded_token, db)

    order_ref = db.collection("routes").document(route).collection("orders").document(order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = route

    if order_data.get("status") != "draft":
        raise HTTPException(400, "Order is not editable")

    forecast_id = payload.forecastId
    items: List[Dict[str, Any]] = [item.model_dump() for item in payload.items]

    stores = order_data.get("stores") or []
    stores_updated = set()
    items_applied = 0
    items_preserved = 0
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
        forecast_units = item.get("recommendedUnits", 0)
        forecast_cases = item.get("recommendedCases")
        item_data = {
            "sap": sap,
            "quantity": forecast_units,
            "enteredAt": now,
            "forecastRecommendedUnits": forecast_units,
            "forecastRecommendedCases": forecast_cases,
            "forecastSuggestedUnits": forecast_units,  # legacy
            "forecastSuggestedCases": forecast_cases,  # legacy
            "userAdjusted": False,
            "userDelta": 0,
            "forecastId": forecast_id,
        }

        for key in (
            "promoActive",
            "promoLiftPct",
            "isFirstWeekend",
            "p10Units",
            "p50Units",
            "p90Units",
            "confidence",
            "source",
            "priorOrderContext",
            "expiryReplacement",
        ):
            if key in item:
                item_data[key] = item.get(key)

        if existing_index >= 0:
            existing_item = store_order["items"][existing_index] or {}
            existing_quantity = existing_item.get("quantity", 0)
            merged_item = {
                **existing_item,
                "sap": sap,
                "quantity": existing_quantity,
                "forecastRecommendedUnits": forecast_units,
                "forecastRecommendedCases": forecast_cases,
                "forecastSuggestedUnits": forecast_units,  # legacy
                "forecastSuggestedCases": forecast_cases,  # legacy
                "forecastId": forecast_id,
                "userAdjusted": existing_quantity != forecast_units,
                "userDelta": existing_quantity - forecast_units,
                "preexistingBeforeForecast": True,
            }
            for key in (
                "promoActive",
                "promoLiftPct",
                "isFirstWeekend",
                "p10Units",
                "p50Units",
                "p90Units",
                "confidence",
                "source",
                "priorOrderContext",
                "expiryReplacement",
            ):
                if key in item:
                    merged_item[key] = item.get(key)
            store_order["items"][existing_index] = merged_item
            items_preserved += 1
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
        {
            "forecastId": forecast_id,
            "itemsApplied": items_applied,
            "itemsPreserved": items_preserved,
            "storesUpdated": len(stores_updated),
        },
    )

    return {
        "orderId": order_id,
        "forecastId": forecast_id,
        "itemsApplied": items_applied,
        "itemsPreserved": items_preserved,
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
) -> Dict[str, Any]:
    """Return forecast accuracy insights based on user corrections.
    
    Performance: Direct PostgreSQL connection (no Firestore round-trip).
    """
    await require_route_feature_access(route, "forecasting", decoded_token, db)

    from datetime import timedelta
    cutoff_date = (datetime.utcnow() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")

    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        # Overall accuracy metrics
        cur.execute("""
            SELECT
                COUNT(*) as total_predictions,
                SUM(CASE WHEN correction_delta = 0 THEN 1 ELSE 0 END) as exact_matches,
                SUM(CASE WHEN ABS(correction_delta) <= 2 THEN 1 ELSE 0 END) as close_matches,
                AVG(ABS(correction_delta)) as avg_deviation,
                SUM(CASE WHEN correction_delta > 0 THEN 1 ELSE 0 END) as under_forecasted,
                SUM(CASE WHEN correction_delta < 0 THEN 1 ELSE 0 END) as over_forecasted
            FROM forecast_corrections
            WHERE route_number = %s
              AND delivery_date >= %s
              AND predicted_units > 0
        """, [route, cutoff_date])
        accuracy_row = cur.fetchone()

        total_predictions = accuracy_row[0] or 0
        exact_matches = accuracy_row[1] or 0
        close_matches = accuracy_row[2] or 0
        avg_deviation = round(float(accuracy_row[3] or 0), 1)
        under_forecasted = accuracy_row[4] or 0
        over_forecasted = accuracy_row[5] or 0

        accuracy_pct = round((exact_matches / total_predictions * 100) if total_predictions > 0 else 0, 1)
        close_pct = round((close_matches / total_predictions * 100) if total_predictions > 0 else 0, 1)

        # Top under-forecasted items (user increased quantities)
        cur.execute("""
            SELECT
                fc.sap,
                COALESCE(pc.full_name, fc.sap) as product_name,
                COUNT(*) as correction_count,
                ROUND(AVG(fc.correction_delta)::numeric, 1) as avg_increase
            FROM forecast_corrections fc
            LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
            WHERE fc.route_number = %s
              AND fc.delivery_date >= %s
              AND fc.correction_delta > 0
              AND fc.predicted_units > 0
            GROUP BY fc.sap, pc.full_name
            HAVING COUNT(*) >= 3
            ORDER BY avg_increase DESC
            LIMIT 5
        """, [route, cutoff_date])
        under_rows = cur.fetchall()

        under_forecasted_items = [
            {"sap": row[0], "productName": row[1], "count": row[2], "avgIncrease": float(row[3])}
            for row in under_rows
        ]

        # Top over-forecasted items (user decreased quantities)
        cur.execute("""
            SELECT
                fc.sap,
                COALESCE(pc.full_name, fc.sap) as product_name,
                COUNT(*) as correction_count,
                ROUND(AVG(ABS(fc.correction_delta))::numeric, 1) as avg_decrease
            FROM forecast_corrections fc
            LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
            WHERE fc.route_number = %s
              AND fc.delivery_date >= %s
              AND fc.correction_delta < 0
              AND fc.predicted_units > 0
            GROUP BY fc.sap, pc.full_name
            HAVING COUNT(*) >= 3
            ORDER BY avg_decrease DESC
            LIMIT 5
        """, [route, cutoff_date])
        over_rows = cur.fetchall()

        over_forecasted_items = [
            {"sap": row[0], "productName": row[1], "count": row[2], "avgDecrease": float(row[3])}
            for row in over_rows
        ]

        # Most frequently corrected items
        cur.execute("""
            SELECT
                fc.sap,
                COALESCE(pc.full_name, fc.sap) as product_name,
                COUNT(*) as correction_count,
                SUM(CASE WHEN fc.correction_delta > 0 THEN 1 ELSE 0 END) as times_increased,
                SUM(CASE WHEN fc.correction_delta < 0 THEN 1 ELSE 0 END) as times_decreased
            FROM forecast_corrections fc
            LEFT JOIN product_catalog pc ON fc.sap = pc.sap AND fc.route_number = pc.route_number
            WHERE fc.route_number = %s
              AND fc.delivery_date >= %s
              AND fc.correction_delta != 0
              AND fc.predicted_units > 0
            GROUP BY fc.sap, pc.full_name
            ORDER BY correction_count DESC
            LIMIT 5
        """, [route, cutoff_date])
        freq_rows = cur.fetchall()

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

        cur.execute("""
            SELECT
                date_trunc('week', submitted_at) as week_start,
                COUNT(*) as total_predictions,
                SUM(CASE WHEN correction_delta = 0 THEN 1 ELSE 0 END) as exact_matches,
                AVG(ABS(correction_delta)) as avg_deviation
            FROM forecast_corrections
            WHERE route_number = %s
              AND submitted_at >= %s
              AND predicted_units > 0
            GROUP BY week_start
            ORDER BY week_start ASC
        """, [route, cutoff_date])
        trend_rows = cur.fetchall()

        cur.close()
        return_pg_connection(conn)
        conn = None

        trend = []
        for row in trend_rows:
            week_start = row[0]
            total = row[1] or 0
            exact = row[2] or 0
            avg_dev = round(float(row[3] or 0), 1)
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
        
    except Exception as e:
        if conn is not None:
            return_pg_connection(conn)
        raise HTTPException(500, f"Database error: {str(e)}")

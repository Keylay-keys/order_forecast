"""Forecast engine: orchestrates loading data, running model, applying business logic, and emitting forecasts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple
import os
import sys
import uuid

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from google.cloud.firestore_v1.base_query import FieldFilter


# =============================================================================
# SCHEDULE-AWARE FEATURE HELPERS
# =============================================================================

def _get_first_saturday_of_month(date: datetime) -> datetime:
    """Get the first Saturday of the given date's month."""
    first_of_month = date.replace(day=1)
    days_until_saturday = (5 - first_of_month.weekday()) % 7
    return first_of_month + timedelta(days=days_until_saturday)


def _days_until_first_weekend(date: datetime) -> int:
    """Days from this date until first Saturday of the month. Can be negative if already passed."""
    first_sat = _get_first_saturday_of_month(date)
    return (first_sat - date).days


def _covers_first_weekend(date: datetime, days_until_next: int = 4) -> bool:
    """Check if delivery on this date will have stock on shelves for first weekend."""
    first_sat = _get_first_saturday_of_month(date)
    first_sun = first_sat + timedelta(days=1)
    coverage_end = date + timedelta(days=days_until_next)
    return date <= first_sat <= coverage_end or date <= first_sun <= coverage_end


def _covers_weekend(delivery_dow: int, days_until_next: int = 4) -> bool:
    """Check if this delivery will have stock on shelves for Sat/Sun.
    
    Args:
        delivery_dow: Day of week of delivery (0=Mon, 6=Sun)
        days_until_next: Days until next delivery
    
    Returns:
        TRUE if the delivery period covers Saturday (5) or Sunday (6)
    """
    # Check if any day from delivery through next delivery is a weekend
    for i in range(days_until_next + 1):
        day = (delivery_dow + i) % 7
        if day in (5, 6):  # Saturday or Sunday
            return True
    return False


def _compute_schedule_features(delivery_date: datetime, days_until_next: int = 4) -> dict:
    """Compute all schedule-aware features for a delivery date.
    
    Args:
        delivery_date: The delivery date
        days_until_next: Days until next scheduled delivery (from user's schedule)
    
    Returns:
        Dict of feature values
    """
    return {
        "days_until_first_weekend": _days_until_first_weekend(delivery_date),
        "covers_first_weekend": int(_covers_first_weekend(delivery_date, days_until_next)),
        "covers_weekend": int(_covers_weekend(delivery_date.weekday(), days_until_next)),
        "days_until_next_delivery": days_until_next,
    }

# =============================================================================
# MINIMUM DATA THRESHOLDS
# =============================================================================
# These thresholds ensure we have enough data for meaningful predictions.
# Training needs: lag features (2+ orders), rolling mean (4+ orders), train/test split

MIN_ORDERS_FOR_FORECAST = int(os.environ.get("MIN_ORDERS_FOR_FORECAST", "4"))  # Minimum orders needed to generate a forecast
MIN_ORDERS_FOR_TRAINING = 8  # Minimum orders needed to train a model (more stringent)
MIN_CORRECTED_ORDERS_FOR_TRAINING = int(
    os.environ.get("MIN_CORRECTED_ORDERS_FOR_TRAINING", "3")
)


def _load_schedule_band_calibration(route_number: str, schedule_key: str) -> Tuple[float, float]:
    """Load schedule-level uncertainty calibration from PostgreSQL.

    Returns:
        (band_scale, band_center_offset_units)
    """
    enabled = os.environ.get("FORECAST_BAND_CALIBRATION_ENABLED", "1").lower() in ("1", "true", "yes")
    if not enabled:
        return 1.0, 0.0

    interval_name = os.environ.get("FORECAST_BAND_INTERVAL_NAME", "p10_p90")
    min_scale = float(os.environ.get("FORECAST_BAND_SCALE_MIN", "0.5"))
    max_scale = float(os.environ.get("FORECAST_BAND_SCALE_MAX", "8.0"))
    max_center_abs = float(os.environ.get("FORECAST_BAND_CENTER_OFFSET_MAX_ABS", "64.0"))

    try:
        scale_row = fetch_one(
            """
            SELECT band_scale
            FROM forecast_band_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            LIMIT 1
            """,
            [route_number, str(schedule_key).lower(), interval_name],
        )
        center_row = fetch_one(
            """
            SELECT center_offset_units
            FROM forecast_band_center_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            LIMIT 1
            """,
            [route_number, str(schedule_key).lower(), interval_name],
        )
        scale = float((scale_row or {}).get("band_scale", 1.0) or 1.0)
        center = float((center_row or {}).get("center_offset_units", 0.0) or 0.0)
        scale = max(min_scale, min(max_scale, scale))
        center = max(-max_center_abs, min(max_center_abs, center))
        return scale, center
    except Exception:
        return 1.0, 0.0


def _load_schedule_source_band_calibration(route_number: str, schedule_key: str) -> Dict[str, Tuple[float, float]]:
    """Load source/branch-level uncertainty calibration from PostgreSQL.

    Returns:
        {source_key: (band_scale_mult, center_offset_units)}
    """
    enabled = os.environ.get("FORECAST_BAND_CALIBRATION_ENABLED", "1").lower() in ("1", "true", "yes")
    if not enabled:
        return {}

    interval_name = os.environ.get("FORECAST_BAND_INTERVAL_NAME", "p10_p90")
    min_scale = float(os.environ.get("FORECAST_BAND_SOURCE_SCALE_MIN", "0.5"))
    max_scale = float(os.environ.get("FORECAST_BAND_SOURCE_SCALE_MAX", "4.0"))
    max_center_abs = float(os.environ.get("FORECAST_BAND_CENTER_OFFSET_MAX_ABS", "64.0"))
    try:
        rows = fetch_all(
            """
            SELECT source, band_scale_mult, center_offset_units
            FROM forecast_band_source_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            """,
            [route_number, str(schedule_key).lower(), interval_name],
        )
        out: Dict[str, Tuple[float, float]] = {}
        for r in rows:
            source = str(r.get("source") or "").strip()
            if not source:
                continue
            scale = float(r.get("band_scale_mult", 1.0) or 1.0)
            center = float(r.get("center_offset_units", 0.0) or 0.0)
            scale = max(min_scale, min(max_scale, scale))
            center = max(-max_center_abs, min(max_center_abs, center))
            out[source] = (scale, center)
        return out
    except Exception:
        return {}

# Support running as a script without package context
try:
    from .models import ForecastItem, ForecastPayload, Product, StoreConfig, PriorOrderContext, ExpiryReplacementInfo
    from .firebase_loader import (
        get_firestore_client,
        load_master_catalog,
        load_store_configs,
        load_orders,
        load_orders_from_postgres,
        load_catalog_from_postgres,
        load_stores_from_postgres,
        load_promotions,
    )
    from .firebase_writer import write_cached_forecast, write_forecast_transfer_suggestions
    from .schedule_utils import (
        normalize_delivery_date,
        weekday_key,
        get_order_day_for_delivery,
        get_days_until_next_delivery,
        get_schedule_info,
        get_legacy_schedule_keys,
    )
    from .case_allocator import allocate_cases_across_stores, get_historical_shares, get_item_case_pattern
    from .low_quantity_loader import get_items_for_order_date, get_stores_for_sap, get_saps_to_suppress
    from .pg_utils import fetch_all, fetch_one
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from models import ForecastItem, ForecastPayload, Product, StoreConfig, PriorOrderContext, ExpiryReplacementInfo
    from firebase_loader import (
        get_firestore_client,
        load_master_catalog,
        load_store_configs,
        load_orders,
        load_orders_from_postgres,
        load_catalog_from_postgres,
        load_stores_from_postgres,
        load_promotions,
    )
    from firebase_writer import write_cached_forecast, write_forecast_transfer_suggestions
    from schedule_utils import (
        normalize_delivery_date,
        weekday_key,
        get_order_day_for_delivery,
        get_days_until_next_delivery,
        get_schedule_info,
        get_legacy_schedule_keys,
    )
    from case_allocator import allocate_cases_across_stores, get_historical_shares, get_item_case_pattern
    from low_quantity_loader import get_items_for_order_date, get_stores_for_sap, get_saps_to_suppress
    from pg_utils import fetch_all, fetch_one


def _parse_promo_date(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Firestore DatetimeWithNanoseconds behaves like datetime
        if hasattr(value, "to_datetime"):
            return value.to_datetime()
    except Exception:
        pass
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def _build_active_promo_lookup(
    db,
    route_number: str,
    delivery_date: str,
    stores_cfg: List,
    valid_saps: Optional[set[str]] = None,
) -> Dict[Tuple[str, str], bool]:
    """Build a lookup of (store_id, sap) -> is_on_promo for a delivery date.
    
    Args:
        db: Firestore client
        route_number: Route number
        delivery_date: ISO date string (YYYY-MM-DD)
        stores_cfg: List of StoreConfig objects
        
    Returns:
        Dict mapping (store_id, sap) to True if on promo
    """
    promos = load_promotions(db, route_number)
    if not promos:
        return {}
    
    delivery_dt = datetime.strptime(delivery_date, "%Y-%m-%d").date()
    default_duration_days = int(os.environ.get("PROMO_DEFAULT_DURATION_DAYS", "7"))
    
    # Build store name -> store_id lookup for fuzzy matching
    store_name_to_ids: Dict[str, List[str]] = {}
    for store in stores_cfg:
        name_key = (store.store_name or "").lower().replace("'", "").replace(" ", "")
        if name_key not in store_name_to_ids:
            store_name_to_ids[name_key] = []
        store_name_to_ids[name_key].append(store.store_id)
    
    active_promos: Dict[Tuple[str, str], bool] = {}
    
    for promo in promos:
        items = promo.get('items') or []
        affected_saps = promo.get('affectedSaps') or []
        
        # Get overall promo dates (fallback if per-item dates not available)
        promo_start = promo.get('startDate')
        promo_end = promo.get('endDate')
        promo_uploaded = promo.get('uploadedAt')
        
        if items:
            # New format: per-item with account and dates
            for item in items:
                sap = str(item.get('sap_code') or item.get('sap') or '')
                if not sap:
                    continue
                if valid_saps is not None and sap not in valid_saps:
                    continue
                
                # Get per-item dates, fallback to promo-level dates
                item_start = item.get('start_date') or promo_start
                item_end = item.get('end_date') or promo_end
                
                # Check if delivery date is within promo period
                start_dt = _parse_promo_date(item_start)
                end_dt = _parse_promo_date(item_end)
                if not start_dt and not end_dt:
                    uploaded_dt = _parse_promo_date(promo_uploaded)
                    if uploaded_dt:
                        start_dt = uploaded_dt
                        end_dt = uploaded_dt + timedelta(days=default_duration_days)
                if start_dt and not end_dt:
                    end_dt = start_dt + timedelta(days=default_duration_days)
                if end_dt and not start_dt:
                    start_dt = end_dt - timedelta(days=default_duration_days)
                if not start_dt or not end_dt:
                    continue
                is_active = start_dt.date() <= delivery_dt <= end_dt.date()
                
                if not is_active:
                    continue
                
                # Match account to store(s)
                account = item.get('account', '')
                if account:
                    acct_key = account.lower().replace("'", "").replace(" ", "")
                    # Find matching stores
                    for name_key, store_ids in store_name_to_ids.items():
                        if acct_key in name_key or name_key in acct_key:
                            for sid in store_ids:
                                active_promos[(sid, sap)] = True
                else:
                    # No account = applies to all stores
                    for store in stores_cfg:
                        active_promos[(store.store_id, sap)] = True
        elif affected_saps:
            # Legacy format: just SAP codes, check promo-level dates
            start_dt = _parse_promo_date(promo_start)
            end_dt = _parse_promo_date(promo_end)
            if not start_dt and not end_dt:
                uploaded_dt = _parse_promo_date(promo_uploaded)
                if uploaded_dt:
                    start_dt = uploaded_dt
                    end_dt = uploaded_dt + timedelta(days=default_duration_days)
            if start_dt and not end_dt:
                end_dt = start_dt + timedelta(days=default_duration_days)
            if end_dt and not start_dt:
                start_dt = end_dt - timedelta(days=default_duration_days)
            if not start_dt or not end_dt:
                continue
            is_active = start_dt.date() <= delivery_dt <= end_dt.date()
            
            if is_active:
                for sap in affected_saps:
                    sap_str = str(sap)
                    if valid_saps is not None and sap_str not in valid_saps:
                        continue
                    for store in stores_cfg:
                        active_promos[(store.store_id, sap_str)] = True
    
    return active_promos


def _get_last_promo_quantities(
    route_number: str,
    schedule_keys: Optional[List[str]],
    delivery_date: str,
    lookback_days: int = 365,
) -> Dict[Tuple[str, str], float]:
    """Get last promo order quantity per store+SAP from PostgreSQL."""
    try:
        min_date = None
        try:
            min_date = datetime.strptime(delivery_date, "%Y-%m-%d").date() - timedelta(days=lookback_days)
        except Exception:
            min_date = None

        params: list = [route_number]
        date_clause = ""
        if min_date:
            date_clause = "AND delivery_date >= %s"
            params.append(min_date)
        schedule_clause = ""
        if schedule_keys:
            schedule_clause = "AND schedule_key = ANY(%s)"
            params.append(schedule_keys)

        rows = fetch_all(
            f"""
            SELECT DISTINCT ON (store_id, sap)
                store_id, sap, quantity, delivery_date
            FROM order_line_items
            WHERE route_number = %s
              AND promo_active = TRUE
              {schedule_clause}
              {date_clause}
            ORDER BY store_id, sap, delivery_date DESC, synced_at DESC
            """,
            params,
        )

        return {(row.get("store_id"), str(row.get("sap"))): float(row.get("quantity") or 0) for row in rows}
    except Exception as e:
        print(f"[forecast] Warning: Could not load promo history: {e}")
        return {}


def _get_prior_order_context(
    db,
    route_number: str,
    current_schedule_key: str,
    target_delivery_date: str,
    stores_cfg: List[StoreConfig],
) -> Dict[tuple, PriorOrderContext]:
    """
    Check for items ordered in a different order cycle that deliver to the same stores.
    
    This handles the case where:
    - Monday order delivers Thursday to most stores, Friday to Bowman's
    - Tuesday order delivers Monday to most stores, Friday to Bowman's
    - Both orders deliver to Bowman's on Friday = overlapping delivery
    
    Only looks at orders from the SAME WEEK (Mon-Sun) with a DIFFERENT schedule.
    This prevents including last week's same-day order as a "prior order".
    
    Returns: Dict[(store_id, sap) -> PriorOrderContext]
    """
    from datetime import datetime, timedelta
    
    prior_context: Dict[tuple, PriorOrderContext] = {}
    
    # Get recent finalized orders from OTHER schedules (not current)
    orders_ref = db.collection('routes').document(route_number).collection('orders')
    
    # Calculate the start of the current week (Monday) based on target delivery date
    # This ensures we only look at orders from THIS week, not last week's same-day order
    try:
        target_dt = datetime.strptime(target_delivery_date, '%Y-%m-%d')
    except ValueError:
        # Try MM/DD/YYYY format
        parts = target_delivery_date.split('/')
        if len(parts) == 3:
            target_dt = datetime(int(parts[2]), int(parts[0]), int(parts[1]))
        else:
            target_dt = datetime.utcnow()
    
    # Find Monday of the week containing the target delivery date
    # We want orders placed Mon-Fri of that week
    days_since_monday = target_dt.weekday()  # 0=Monday, 6=Sunday
    week_start = target_dt - timedelta(days=days_since_monday + 7)  # Start of previous week
    week_start_str = week_start.strftime('%Y-%m-%d')
    
    try:
        query = orders_ref.where(filter=FieldFilter('status', '==', 'finalized'))
        docs = list(query.stream())
        
        for doc in docs:
            order_data = doc.to_dict()
            order_schedule = order_data.get('scheduleKey', '')
            
            # Skip orders from the same schedule
            if order_schedule == current_schedule_key:
                continue
            
            # Check if order was placed in the same week (use orderDate, not createdAt)
            order_date = order_data.get('orderDate', '')
            # Normalize date format for comparison
            if order_date:
                # Handle both YYYY-MM-DD and MM/DD/YYYY formats
                if '/' in order_date:
                    parts = order_date.split('/')
                    if len(parts) == 3:
                        order_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                
                # Only include orders from within the past week (same week or prior week's late orders)
                if order_date < week_start_str:
                    continue
                
                # Also skip if order is newer than target (shouldn't happen, but safety check)
                if order_date > target_delivery_date:
                    continue
            
            order_delivery = order_data.get('expectedDeliveryDate', '')
            order_date_raw = order_data.get('orderDate', '')
            order_id = doc.id
            
            # Parse order date to calculate store-specific delivery dates
            order_date_parsed = None
            if order_date_raw:
                try:
                    if '-' in order_date_raw:
                        order_date_parsed = datetime.strptime(order_date_raw, '%Y-%m-%d')
                    elif '/' in order_date_raw:
                        parts = order_date_raw.split('/')
                        if len(parts) == 3:
                            order_date_parsed = datetime(int(parts[2]), int(parts[0]), int(parts[1]))
                except:
                    pass
            
            # Normalize target delivery for comparison
            target_delivery_normalized = target_delivery_date
            if target_delivery_date and '/' in target_delivery_date:
                parts = target_delivery_date.split('/')
                if len(parts) == 3:
                    target_delivery_normalized = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            
            # Check each store in this order - delivery dates vary by store!
            for store in order_data.get('stores', []):
                store_id = store.get('storeId', '')
                store_name = store.get('storeName', '')
                
                # Find store config to check delivery days
                store_cfg = next((s for s in stores_cfg if s.store_id == store_id), None)
                if not store_cfg:
                    continue
                
                # Calculate the ACTUAL delivery date for this store based on their delivery days
                # e.g., Bowman's only delivers Friday, so their delivery date differs from the order's
                store_delivery_date = order_delivery  # Default to order's delivery
                
                if store_cfg.delivery_days and order_date_parsed:
                    # Map day names to weekday numbers (0=Monday, 6=Sunday)
                    day_map = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 
                               'friday': 4, 'saturday': 5, 'sunday': 6}
                    
                    # Calculate PRIOR order's store-specific delivery
                    order_weekday = order_date_parsed.weekday()
                    for day_name in store_cfg.delivery_days:
                        target_weekday = day_map.get(day_name.lower())
                        if target_weekday is not None:
                            days_until = (target_weekday - order_weekday) % 7
                            if days_until == 0:
                                days_until = 7  # If same day, assume next week
                            store_delivery = order_date_parsed + timedelta(days=days_until)
                            store_delivery_date = store_delivery.strftime('%Y-%m-%d')
                            break
                
                # Calculate the CURRENT order's store-specific delivery for accurate comparison
                # For stores like Bowman's that only deliver Friday, we need to find the Friday
                # in the same delivery window as the current order
                current_store_delivery = target_delivery_normalized  # Default
                
                if store_cfg.delivery_days:
                    day_map = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 
                               'friday': 4, 'saturday': 5, 'sunday': 6}
                    target_weekday_num = target_dt.weekday()
                    target_day_name = list(day_map.keys())[target_weekday_num]
                    
                    # If the store delivers on the target day (e.g., Monday), use that
                    # Otherwise use the closest delivery day before the target
                    if target_day_name in [d.lower() for d in store_cfg.delivery_days]:
                        # Store delivers on the main delivery day - use target date
                        current_store_delivery = target_delivery_normalized
                    else:
                        # Store delivers on a different day (e.g., Bowman's Friday)
                        # Find the closest delivery day BEFORE the target
                        best_delivery = None
                        for day_name in store_cfg.delivery_days:
                            store_weekday = day_map.get(day_name.lower())
                            if store_weekday is not None:
                                days_diff = store_weekday - target_weekday_num
                                if days_diff > 0:
                                    days_diff -= 7  # Previous week
                                candidate_dt = target_dt + timedelta(days=days_diff)
                                candidate_str = candidate_dt.strftime('%Y-%m-%d')
                                if best_delivery is None or candidate_str > best_delivery:
                                    best_delivery = candidate_str
                        if best_delivery:
                            current_store_delivery = best_delivery
                
                # KEY CHECK: Only warn if prior delivery >= current store's delivery
                # e.g., Harmon's Thursday Dec 18 < Monday Dec 22 → no warning (already sold)
                # e.g., Bowman's Friday Dec 19 >= Friday Dec 19 → warning (same day overlap!)
                if store_delivery_date < current_store_delivery:
                    continue  # Prior delivery is before current store's delivery - no overlap
                
                # Check each item in this store
                for item in store.get('items', []):
                    sap = item.get('sap', '')
                    quantity = item.get('quantity', 0)
                    
                    if quantity > 0 and sap:
                        key = (store_id, sap)
                        # Only keep the most recent prior order for each store/sap
                        if key not in prior_context:
                            prior_context[key] = PriorOrderContext(
                                order_id=order_id,
                                order_date=order_date_raw,
                                delivery_date=store_delivery_date,  # Use store-specific date
                                quantity=int(quantity),
                                schedule_key=order_schedule,
                            )
        
        if prior_context:
            print(f"[forecast] Found {len(prior_context)} items with prior orders from other schedules")
            
    except Exception as e:
        print(f"[forecast] Warning: Could not check prior orders: {e}")
    
    return prior_context


@dataclass
class CrossCycleOrder:
    """Context for an item ordered in a different schedule cycle."""
    store_id: str
    sap: str
    schedule_key: str
    delivery_date: str
    quantity: int
    days_ago: int


def _get_shared_case_rates(route_number: str) -> Dict[str, Tuple[float, int]]:
    """
    Compute how often a SAP is ordered across 2+ stores in the same order.
    
    Returns dict sap -> (shared_rate, order_count)
    """
    try:
        rows = fetch_all("""
            WITH per_order AS (
                SELECT o.order_id, oli.sap, COUNT(DISTINCT oli.store_id) as store_count
                FROM order_line_items oli
                JOIN orders_historical o ON oli.order_id = o.order_id
                WHERE o.route_number = %s
                GROUP BY o.order_id, oli.sap
            ),
            sap_totals AS (
                SELECT 
                    sap,
                    COUNT(*) as order_count,
                    SUM(CASE WHEN store_count >= 2 THEN 1 ELSE 0 END) as shared_orders
                FROM per_order
                GROUP BY sap
            )
            SELECT 
                sap,
                order_count,
                shared_orders,
                CASE WHEN order_count = 0 THEN 0 ELSE shared_orders::float / order_count END as shared_rate
            FROM sap_totals
        """, [route_number])
        return {str(r['sap']): (float(r['shared_rate']), int(r['order_count'])) for r in rows}
    except Exception as e:
        print(f"[forecast] Warning: Shared-case rate query exception: {e}")
        return {}


def _get_store_sap_home_cycles(
    route_number: str,
    home_cycle_min_share: float = 0.60,
) -> Dict[Tuple[str, str], Tuple[str, float, int]]:
    """
    Compute home cycle per store+SAP.
    
    Returns dict (store_id, sap) -> (home_cycle, home_share, total_orders)
    Only includes entries that meet home_cycle_min_share.
    """
    try:
        rows = fetch_all("""
            WITH store_sap_counts AS (
                SELECT 
                    oli.store_id,
                    oli.sap,
                    o.schedule_key,
                    COUNT(DISTINCT o.order_id) as times_in_schedule
                FROM order_line_items oli
                JOIN orders_historical o ON oli.order_id = o.order_id
                WHERE o.route_number = %s
                GROUP BY oli.store_id, oli.sap, o.schedule_key
            ),
            store_sap_totals AS (
                SELECT store_id, sap, SUM(times_in_schedule) as total_orders
                FROM store_sap_counts
                GROUP BY store_id, sap
            ),
            store_sap_home AS (
                SELECT 
                    c.store_id,
                    c.sap,
                    c.schedule_key as home_cycle,
                    c.times_in_schedule,
                    t.total_orders,
                    c.times_in_schedule::float / t.total_orders as home_share,
                    ROW_NUMBER() OVER (PARTITION BY c.store_id, c.sap ORDER BY c.times_in_schedule DESC) as rn
                FROM store_sap_counts c
                JOIN store_sap_totals t ON c.store_id = t.store_id AND c.sap = t.sap
            )
            SELECT store_id, sap, home_cycle, total_orders, home_share
            FROM store_sap_home
            WHERE rn = 1
        """, [route_number])
        result: Dict[Tuple[str, str], Tuple[str, float, int]] = {}
        for row in rows:
            home_share = float(row['home_share'])
            if home_share < home_cycle_min_share:
                continue
            key = (row['store_id'], str(row['sap']))
            result[key] = (row['home_cycle'], home_share, int(row['total_orders']))
        return result
    except Exception as e:
        print(f"[forecast] Warning: Store/SAP home cycle query exception: {e}")
        return {}


@dataclass
class SAPTierInfo:
    """Velocity tier and home cycle info for a SAP."""
    sap: str
    order_frequency: float  # 0.0 to 1.0
    tier: str  # 'SLOW', 'MEDIUM', 'FAST'
    home_cycle: Optional[str]  # e.g., 'monday', 'tuesday', or None if no clear home
    home_cycle_share: float  # What % of orders are in home cycle (0.0 to 1.0)
    total_orders: int
    shared_case_rate: float  # % of orders where SAP appears in >=2 stores
    is_shared_case: bool  # True when shared_case_rate passes threshold


def _get_sap_tiers(
    route_number: str,
    slow_threshold: float = 0.25,
    fast_threshold: float = 0.60,
    home_cycle_min_share: float = 0.60,
) -> Dict[str, SAPTierInfo]:
    """
    Compute velocity tier and home cycle for each SAP.
    
    Tiers:
      - SLOW: order_frequency <= slow_threshold (default 25%)
      - MEDIUM: slow_threshold < order_frequency <= fast_threshold (default 25-60%)
      - FAST: order_frequency > fast_threshold (default >60%)
    
    Home cycle: The schedule where the SAP is ordered most often.
    Only assigned if >= home_cycle_min_share of orders are in that schedule.
    
    Args:
        route_number: Route to analyze
        slow_threshold: Max frequency to be considered SLOW (default 0.25)
        fast_threshold: Min frequency to be considered FAST (default 0.60)
        home_cycle_min_share: Min share to assign a home cycle (default 0.60)
        
    Returns:
        Dict mapping sap -> SAPTierInfo
    """
    try:
        shared_rates = _get_shared_case_rates(route_number)
        shared_min_rate = float(os.environ.get("SHARED_CASE_MIN_RATE", "0.60"))
        shared_min_orders = int(os.environ.get("SHARED_CASE_MIN_ORDERS", "4"))

        rows = fetch_all("""
            WITH total_orders AS (
                SELECT COUNT(DISTINCT order_id) as total
                FROM orders_historical
                WHERE route_number = %s
            ),
            sap_schedule_counts AS (
                SELECT 
                    oli.sap,
                    o.schedule_key,
                    COUNT(DISTINCT o.order_id) as times_in_schedule
                FROM order_line_items oli
                JOIN orders_historical o ON oli.order_id = o.order_id
                WHERE o.route_number = %s
                GROUP BY oli.sap, o.schedule_key
            ),
            sap_totals AS (
                SELECT sap, SUM(times_in_schedule) as sap_total_orders
                FROM sap_schedule_counts
                GROUP BY sap
            ),
            sap_home AS (
                SELECT 
                    c.sap,
                    c.schedule_key as home_cycle,
                    c.times_in_schedule,
                    t.sap_total_orders,
                    c.times_in_schedule::float / t.sap_total_orders as schedule_share,
                    ROW_NUMBER() OVER (PARTITION BY c.sap ORDER BY c.times_in_schedule DESC) as rn
                FROM sap_schedule_counts c
                JOIN sap_totals t ON c.sap = t.sap
            )
            SELECT 
                h.sap,
                h.home_cycle,
                h.sap_total_orders as total_orders,
                h.schedule_share as home_share,
                h.sap_total_orders::float / (SELECT total FROM total_orders) as order_freq
            FROM sap_home h
            WHERE h.rn = 1
        """, [route_number, route_number])
        sap_tiers: Dict[str, SAPTierInfo] = {}
        
        tier_counts = {'SLOW': 0, 'MEDIUM': 0, 'FAST': 0}
        
        for row in rows:
            sap = str(row['sap'])
            freq = float(row['order_freq'])
            home_share = float(row['home_share'])
            shared_rate, shared_orders = shared_rates.get(sap, (0.0, 0))
            
            # Determine tier
            if freq <= slow_threshold:
                tier = 'SLOW'
            elif freq <= fast_threshold:
                tier = 'MEDIUM'
            else:
                tier = 'FAST'
            
            tier_counts[tier] += 1
            
            # Only assign home cycle if share is high enough
            home_cycle = row['home_cycle'] if home_share >= home_cycle_min_share else None
            
            is_shared = shared_orders >= shared_min_orders and shared_rate >= shared_min_rate
            sap_tiers[sap] = SAPTierInfo(
                sap=sap,
                order_frequency=freq,
                tier=tier,
                home_cycle=home_cycle,
                home_cycle_share=home_share,
                total_orders=int(row['total_orders']),
                shared_case_rate=shared_rate,
                is_shared_case=is_shared,
            )
        
        print(f"[forecast] SAP tiers: SLOW={tier_counts['SLOW']}, MEDIUM={tier_counts['MEDIUM']}, FAST={tier_counts['FAST']}")
        slow_with_home = sum(1 for s in sap_tiers.values() if s.tier == 'SLOW' and s.home_cycle)
        print(f"[forecast] Slow movers with home cycle: {slow_with_home}/{tier_counts['SLOW']}")
        
        return sap_tiers
        
    except Exception as e:
        print(f"[forecast] Warning: SAP tier query exception: {e}")
        return {}


def _get_cross_cycle_orders(
    route_number: str,
    current_schedule: str,
    target_delivery_date: str,
    lookback_days: int = 7,
) -> Dict[Tuple[str, str], CrossCycleOrder]:
    """
    Find items ordered in OTHER schedule cycles within the lookback window.
    
    This enables cross-cycle suppression: if a slow-mover was ordered 3 days ago
    in the Tuesday cycle, don't recommend it again in the Monday cycle.
    
    Data source: PostgreSQL (orders_historical + order_line_items)
    No Firebase call needed.
    
    Args:
        route_number: Route to query
        current_schedule: The schedule we're forecasting for (e.g., 'monday')
        target_delivery_date: The delivery date we're forecasting for (YYYY-MM-DD)
        lookback_days: How many days back to look for cross-cycle orders
        
    Returns:
        Dict mapping (store_id, sap) -> CrossCycleOrder for items ordered in other cycles
    """
    from datetime import datetime, timedelta
    
    # Calculate lookback window
    try:
        target_dt = datetime.strptime(target_delivery_date, '%Y-%m-%d')
    except ValueError:
        print(f"[forecast] Warning: Invalid target date {target_delivery_date}, skipping cross-cycle check")
        return {}
    
    lookback_dt = target_dt - timedelta(days=lookback_days)
    lookback_date = lookback_dt.strftime('%Y-%m-%d')
    
    try:
        rows = fetch_all("""
            SELECT 
                oli.store_id,
                oli.sap,
                o.schedule_key,
                o.delivery_date,
                SUM(oli.quantity) as quantity
            FROM order_line_items oli
            JOIN orders_historical o ON oli.order_id = o.order_id
            WHERE o.route_number = %s
              AND o.schedule_key != %s
              AND o.delivery_date >= %s
              AND o.delivery_date < %s
            GROUP BY oli.store_id, oli.sap, o.schedule_key, o.delivery_date
            ORDER BY o.delivery_date DESC
        """, [route_number, current_schedule, lookback_date, target_delivery_date])
        if not rows:
            print(f"[forecast] No cross-cycle orders found in {lookback_days}-day window")
            return {}
        
        # Build lookup dict - keep most recent order for each store/sap
        cross_cycle_orders: Dict[Tuple[str, str], CrossCycleOrder] = {}
        for row in rows:
            store_id = row['store_id']
            sap = str(row['sap'])
            key = (store_id, sap)
            
            delivery_date = str(row['delivery_date'])
            try:
                delivery_dt = datetime.strptime(delivery_date[:10], '%Y-%m-%d')
                days_ago = (target_dt - delivery_dt).days
            except:
                days_ago = lookback_days
            
            # Only keep if not already present (first row is most recent due to ORDER BY)
            if key not in cross_cycle_orders:
                cross_cycle_orders[key] = CrossCycleOrder(
                    store_id=store_id,
                    sap=sap,
                    schedule_key=row['schedule_key'],
                    delivery_date=delivery_date[:10],
                    quantity=int(row['quantity']),
                    days_ago=days_ago,
                )
        
        print(f"[forecast] Found {len(cross_cycle_orders)} store/sap combinations ordered in other cycles within {lookback_days} days")
        return cross_cycle_orders
        
    except Exception as e:
        print(f"[forecast] Warning: Cross-cycle query exception: {e}")
        return {}


def _get_same_cycle_recent_orders(
    route_number: str,
    current_schedule: str,
    target_delivery_date: str,
    lookback_days: int = 21,
) -> Dict[str, int]:
    """
    Find slow-mover items ordered in the SAME schedule cycle recently.
    
    This prevents recommending a slow mover that was just ordered last week
    in the same cycle. For example, if SAP 22351 was ordered on Monday 1/29,
    don't recommend it again on Monday 2/5 (only 7 days later).
    
    Args:
        route_number: Route to query
        current_schedule: The schedule we're forecasting for (e.g., 'monday')
        target_delivery_date: The delivery date we're forecasting for (YYYY-MM-DD)
        lookback_days: How many days back to look (default 21 = ~3 weeks)
        
    Returns:
        Dict mapping sap -> days_ago for items ordered in the same cycle recently
    """
    from datetime import datetime, timedelta
    
    try:
        target_dt = datetime.strptime(target_delivery_date, '%Y-%m-%d')
    except ValueError:
        return {}
    
    lookback_dt = target_dt - timedelta(days=lookback_days)
    lookback_date = lookback_dt.strftime('%Y-%m-%d')
    
    try:
        rows = fetch_all("""
            SELECT 
                oli.sap,
                MAX(o.delivery_date) as last_delivery_date
            FROM order_line_items oli
            JOIN orders_historical o ON oli.order_id = o.order_id
            WHERE o.route_number = %s
              AND o.schedule_key = %s
              AND o.delivery_date >= %s
              AND o.delivery_date < %s
              AND oli.quantity > 0
            GROUP BY oli.sap
        """, [route_number, current_schedule, lookback_date, target_delivery_date])
        if not rows:
            return {}
        
        same_cycle_orders: Dict[str, int] = {}
        for row in rows:
            sap = str(row['sap'])
            delivery_date = str(row['last_delivery_date'])
            try:
                delivery_dt = datetime.strptime(delivery_date[:10], '%Y-%m-%d')
                days_ago = (target_dt - delivery_dt).days
            except:
                days_ago = lookback_days
            
            same_cycle_orders[sap] = days_ago
        
        return same_cycle_orders
        
    except Exception as e:
        print(f"[forecast] Warning: Same-cycle query exception: {e}")
        return {}


def _get_same_cycle_recent_orders_by_store(
    route_number: str,
    current_schedule: str,
    target_delivery_date: str,
    lookback_days: int = 21,
) -> Dict[Tuple[str, str], int]:
    """
    Same-cycle recent orders per store+SAP.
    
    Returns dict (store_id, sap) -> days_ago.
    """
    from datetime import datetime, timedelta
    
    try:
        target_dt = datetime.strptime(target_delivery_date, '%Y-%m-%d')
    except ValueError:
        return {}
    
    lookback_dt = target_dt - timedelta(days=lookback_days)
    lookback_date = lookback_dt.strftime('%Y-%m-%d')
    
    try:
        rows = fetch_all("""
            SELECT 
                oli.store_id,
                oli.sap,
                MAX(o.delivery_date) as last_delivery_date
            FROM order_line_items oli
            JOIN orders_historical o ON oli.order_id = o.order_id
            WHERE o.route_number = %s
              AND o.schedule_key = %s
              AND o.delivery_date >= %s
              AND o.delivery_date < %s
              AND oli.quantity > 0
            GROUP BY oli.store_id, oli.sap
        """, [route_number, current_schedule, lookback_date, target_delivery_date])
        if not rows:
            return {}
        
        same_cycle_orders: Dict[Tuple[str, str], int] = {}
        for row in rows:
            store_id = row['store_id']
            sap = str(row['sap'])
            delivery_date = str(row['last_delivery_date'])
            try:
                delivery_dt = datetime.strptime(delivery_date[:10], '%Y-%m-%d')
                days_ago = (target_dt - delivery_dt).days
            except Exception:
                days_ago = lookback_days
            
            same_cycle_orders[(store_id, sap)] = days_ago
        
        return same_cycle_orders
        
    except Exception as e:
        print(f"[forecast] Warning: Same-cycle (by-store) query exception: {e}")
        return {}


@dataclass
class ForecastConfig:
    route_number: str
    delivery_date: str  # ISO or MM/DD/YYYY
    schedule_key: str  # monday / thursday etc
    service_account: Optional[str] = None
    since_days: int = 365
    round_cases: bool = True
    ttl_days: int = 7


def _load_corrections_from_postgres(route_number: str, schedule_key: str) -> Optional[pd.DataFrame]:
    """Load aggregated corrections from PostgreSQL for ML features.
    
    Queries the forecast_corrections table and aggregates by store/sap/schedule
    to compute correction features like avg_delta, avg_ratio, removal_rate.
    
    Args:
        route_number: Route to load corrections for
        schedule_key: Order schedule (e.g., 'monday', 'tuesday')
    
    Returns:
        DataFrame with columns: store_id, sap, schedule_key, samples, avg_delta, 
        avg_ratio, ratio_stddev, removal_rate, promo_rate
        Returns None if query fails.
    """
    
    try:
        # Aggregate corrections by store/sap/schedule using recency weighting.
        # Added-line behavior (predicted=0, final>0) is tracked explicitly via add_rate/avg_add_units.
        half_life_days = float(os.environ.get("FORECAST_CORR_RECENCY_HALF_LIFE_DAYS", "42"))
        rows = fetch_all(
            """
            WITH base AS (
                SELECT
                    store_id,
                    sap,
                    schedule_key,
                    correction_delta,
                    correction_ratio,
                    was_removed,
                    promo_active,
                    predicted_units,
                    final_units,
                    POWER(
                        0.5,
                        GREATEST(
                            0.0,
                            (CURRENT_DATE - COALESCE(delivery_date::date, submitted_at::date, CURRENT_DATE))::float
                        ) / %s
                    ) AS recency_w
                FROM forecast_corrections
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND is_holiday_week = FALSE
            )
            SELECT
                store_id,
                sap,
                schedule_key,
                COUNT(*) as samples,
                SUM(correction_delta * recency_w) / NULLIF(SUM(recency_w), 0) as avg_delta,
                SUM(correction_ratio * recency_w) / NULLIF(SUM(recency_w), 0) as avg_ratio,
                STDDEV(correction_ratio) as ratio_stddev,
                SUM((CASE WHEN was_removed THEN 1 ELSE 0 END) * recency_w) / NULLIF(SUM(recency_w), 0) as removal_rate,
                SUM((CASE WHEN promo_active THEN 1 ELSE 0 END) * recency_w) / NULLIF(SUM(recency_w), 0) as promo_rate,
                SUM((CASE WHEN predicted_units = 0 AND final_units > 0 THEN 1 ELSE 0 END) * recency_w)
                    / NULLIF(SUM(recency_w), 0) as add_rate,
                SUM((CASE WHEN predicted_units = 0 AND final_units > 0 THEN final_units ELSE 0 END) * recency_w)
                    / NULLIF(
                        SUM(CASE WHEN predicted_units = 0 AND final_units > 0 THEN recency_w ELSE 0 END),
                        0
                    ) as avg_add_units
            FROM base
            GROUP BY store_id, sap, schedule_key
            HAVING COUNT(*) >= 1
            """,
            [half_life_days, route_number, schedule_key],
        )
        if not rows:
            print(f"[forecast] No corrections found for route {route_number}, schedule {schedule_key}")
            return None
        
        df = pd.DataFrame(rows)
        print(f"[forecast] Loaded {len(df)} correction aggregates from PostgreSQL")
        return df
        
    except Exception as e:
        print(f"[forecast] Warning: Could not load corrections from PostgreSQL: {e}")
        return None


def _case_pack_for_sap(products: List[Product], sap: str) -> Optional[int]:
    for p in products:
        if p.sap == sap:
            return p.case_pack or p.tray
    return None


def _match_store(store_cfgs: List[StoreConfig], store_id: str, store_name: str) -> Optional[StoreConfig]:
    """Match a store by ID or fuzzy name match.
    
    This handles cases where historical data has store names like "Farmington"
    but Firebase stores are named "Farmington Smith's".
    """
    # First try exact ID match
    for s in store_cfgs:
        if s.store_id == store_id:
            return s
    
    # Then try fuzzy name match (check if historical name is contained in Firebase name)
    store_name_lower = store_name.lower().strip()
    for s in store_cfgs:
        firebase_name_lower = s.store_name.lower().strip()
        # Match if historical name is contained in Firebase name or vice versa
        if store_name_lower in firebase_name_lower or firebase_name_lower in store_name_lower:
            return s
    
    return None


def _store_active(store_cfgs: List[StoreConfig], store_id: str, store_name: str, sap: str) -> bool:
    """Check if a store+sap combination is active."""
    # Strict by default: if we can't prove the line item is carried/active, drop it.
    matched_store = _match_store(store_cfgs, store_id, store_name)
    if not matched_store:
        return False  # Prevent "ghost" items from skewing totals without being visible in clients

    active_saps = matched_store.active_saps or []
    if not active_saps:
        # Empty carry list means "no items configured" (do NOT treat as allow-all).
        return False

    return str(sap) in {str(s) for s in active_saps}


def _store_active_with_reason(
    store_cfgs: List[StoreConfig], store_id: str, store_name: str, sap: str
) -> Tuple[bool, Optional[str]]:
    """Strict store+SAP visibility check with an explicit reason on failure."""
    matched_store = _match_store(store_cfgs, store_id, store_name)
    if not matched_store:
        return False, "store_unmatched"

    active_saps = matched_store.active_saps or []
    if not active_saps:
        return False, "no_carry_list"

    if str(sap) not in {str(s) for s in active_saps}:
        return False, "inactive_store_item"

    return True, None


def _filter_orders_by_schedule(orders, schedule_key: str):
    return [o for o in orders if o.schedule_key == schedule_key]


def _count_corrected_orders(route_number: str, schedule_key: str, since_days: int) -> int:
    """Count distinct finalized orders with captured corrections for this route/schedule."""
    try:
        row = fetch_one(
            """
            SELECT COUNT(DISTINCT order_id) AS corrected_orders
            FROM forecast_corrections
            WHERE route_number = %s
              AND schedule_key = %s
              AND is_holiday_week = FALSE
              AND delivery_date >= (CURRENT_DATE - (%s || ' days')::interval)
            """,
            [route_number, str(schedule_key).lower(), int(since_days)],
        )
        return int((row or {}).get("corrected_orders") or 0)
    except Exception:
        return 0


def _summarize_schedule_shape(order_cycles: List[dict], schedule_key: str) -> Dict[str, object]:
    """Summarize schedule shape and structural issues from user order cycles."""
    cycles = list(order_cycles or [])
    day_name_to_num = {
        "sunday": 1,
        "monday": 2,
        "tuesday": 3,
        "wednesday": 4,
        "thursday": 5,
        "friday": 6,
        "saturday": 7,
    }
    order_day_num = day_name_to_num.get(str(schedule_key or "").lower())

    order_day_counts: Dict[int, int] = {}
    delivery_day_counts: Dict[int, int] = {}
    order_to_delivery: Dict[int, set] = {}
    invalid_cycles = 0
    has_schedule_cycle = False

    for c in cycles:
        if not isinstance(c, dict):
            invalid_cycles += 1
            continue
        od = c.get("orderDay")
        dd = c.get("deliveryDay")
        ld = c.get("loadDay")
        if not isinstance(od, int) or not isinstance(dd, int) or not isinstance(ld, int):
            invalid_cycles += 1
            continue
        if od < 1 or od > 7 or dd < 1 or dd > 7 or ld < 1 or ld > 7:
            invalid_cycles += 1
            continue
        if order_day_num is not None and od == order_day_num:
            has_schedule_cycle = True
        order_day_counts[od] = order_day_counts.get(od, 0) + 1
        delivery_day_counts[dd] = delivery_day_counts.get(dd, 0) + 1
        order_to_delivery.setdefault(od, set()).add(dd)

    duplicate_order_day = any(v > 1 for v in order_day_counts.values())
    duplicate_delivery_day = any(v > 1 for v in delivery_day_counts.values())
    same_order_to_multi_delivery = any(len(v) > 1 for v in order_to_delivery.values())
    is_valid = invalid_cycles == 0

    return {
        "is_valid": is_valid,
        "invalid_cycles": invalid_cycles,
        "total_cycles": len(cycles),
        "duplicate_order_day": duplicate_order_day,
        "duplicate_delivery_day": duplicate_delivery_day,
        "same_order_to_multi_delivery": same_order_to_multi_delivery,
        "has_schedule_cycle": has_schedule_cycle,
    }


@dataclass
class ForecastModeDecision:
    mode: str  # copy_last_order | schedule_aware | store_centric
    training_scope: str  # schedule_only | store_all_cycles
    reason: str
    schedule_order_count: int
    corrected_order_count: int
    schedule_shape: Dict[str, object]


def _resolve_forecast_mode(
    *,
    route_number: str,
    schedule_key: str,
    requested_scope: str,
    all_orders,
    order_cycles: List[dict],
    since_days: int,
) -> ForecastModeDecision:
    """Resolve forecast mode progression for this route/schedule.

    Progression:
      1) copy_last_order (cold start, low correction signal)
      2) schedule_aware
      3) store_centric (only when route-wide depth supports it)
    """
    scope = str(requested_scope or "auto").lower()
    schedule_orders = _filter_orders_by_schedule(all_orders, schedule_key)
    schedule_order_count = len(schedule_orders)
    corrected_order_count = _count_corrected_orders(route_number, schedule_key, since_days)
    schedule_shape = _summarize_schedule_shape(order_cycles, schedule_key)

    min_schedule_orders = int(
        os.environ.get("FORECAST_MIN_SCHEDULE_ORDERS_FOR_ML", str(MIN_ORDERS_FOR_TRAINING))
    )
    min_corrected_orders = int(
        os.environ.get("FORECAST_MIN_CORRECTED_ORDERS_FOR_ML", str(MIN_CORRECTED_ORDERS_FOR_TRAINING))
    )
    strict_schedule_validation = os.environ.get(
        "FORECAST_STRICT_SCHEDULE_VALIDATION", "1"
    ).lower() in ("1", "true", "yes")
    allow_store_on_ambiguous_schedule = os.environ.get(
        "FORECAST_ALLOW_STORE_CONTEXT_ON_AMBIGUOUS_SCHEDULE", "1"
    ).lower() in ("1", "true", "yes")

    # Stage 1 gate: cold start remains anchored to last order until enough signal exists.
    if schedule_order_count < min_schedule_orders or corrected_order_count < min_corrected_orders:
        reason_parts = []
        if schedule_order_count < min_schedule_orders:
            reason_parts.append(
                f"schedule_orders={schedule_order_count}<{min_schedule_orders}"
            )
        if corrected_order_count < min_corrected_orders:
            reason_parts.append(
                f"corrected_orders={corrected_order_count}<{min_corrected_orders}"
            )
        return ForecastModeDecision(
            mode="copy_last_order",
            training_scope="schedule_only",
            reason="cold_start:" + ",".join(reason_parts),
            schedule_order_count=schedule_order_count,
            corrected_order_count=corrected_order_count,
            schedule_shape=schedule_shape,
        )

    # Stage 2/3 candidate from explicit scope or adaptive depth.
    if scope in ("schedule_only", "cycle_only"):
        candidate_mode = "schedule_aware"
        candidate_scope = "schedule_only"
    elif scope in ("store_all_cycles", "store_all", "all_cycles"):
        candidate_mode = "store_centric"
        candidate_scope = "store_all_cycles"
    else:
        adaptive_scope = _resolve_training_scope(all_orders, "auto")
        if adaptive_scope == "store_all_cycles":
            candidate_mode = "store_centric"
            candidate_scope = "store_all_cycles"
        else:
            candidate_mode = "schedule_aware"
            candidate_scope = "schedule_only"

    # Guard malformed schedule payloads (missing / out-of-range day mappings).
    if strict_schedule_validation and not bool(schedule_shape.get("is_valid")):
        return ForecastModeDecision(
            mode="schedule_aware",
            training_scope="schedule_only",
            reason=f"invalid_schedule_config:invalid_cycles={schedule_shape.get('invalid_cycles', 0)}",
            schedule_order_count=schedule_order_count,
            corrected_order_count=corrected_order_count,
            schedule_shape=schedule_shape,
        )

    # Ambiguous mappings are allowed, but we can still force safer schedule-aware mode.
    if (
        candidate_mode == "store_centric"
        and not allow_store_on_ambiguous_schedule
        and bool(schedule_shape.get("same_order_to_multi_delivery"))
    ):
        return ForecastModeDecision(
            mode="schedule_aware",
            training_scope="schedule_only",
            reason="ambiguous_schedule_mapping:same_order_to_multi_delivery",
            schedule_order_count=schedule_order_count,
            corrected_order_count=corrected_order_count,
            schedule_shape=schedule_shape,
        )

    return ForecastModeDecision(
        mode=candidate_mode,
        training_scope=candidate_scope,
        reason=f"resolved_from_scope:{scope}",
        schedule_order_count=schedule_order_count,
        corrected_order_count=corrected_order_count,
        schedule_shape=schedule_shape,
    )


def _resolve_training_scope(orders, requested_scope: str) -> str:
    """Resolve effective training scope with safety fallback for sparse routes.

    Returns:
        "store_all_cycles" or "schedule_only"
    """
    scope = str(requested_scope or "auto").lower()
    if scope in ("schedule_only", "cycle_only"):
        return "schedule_only"
    if scope in ("store_all_cycles", "store_all", "all_cycles"):
        return "store_all_cycles"

    # Auto mode: store-centric only when route history depth is sufficient.
    min_total = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_TOTAL_ORDERS", "24"))
    min_per_schedule = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_PER_SCHEDULE", "6"))
    min_schedules = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_SCHEDULES", "2"))

    if not orders or len(orders) < min_total:
        return "schedule_only"

    schedule_counts: Dict[str, int] = {}
    for o in orders:
        sk = str(getattr(o, "schedule_key", "") or "").lower()
        if not sk:
            continue
        schedule_counts[sk] = schedule_counts.get(sk, 0) + 1
    schedules_meeting_min = sum(1 for c in schedule_counts.values() if c >= min_per_schedule)
    if schedules_meeting_min < min_schedules:
        return "schedule_only"
    return "store_all_cycles"


def _select_last_order_for_schedule(orders, schedule_key: str):
    """Pick the most recent order for a schedule, preferring exact schedule_key."""
    primary = [o for o in orders if o.schedule_key == schedule_key]
    candidates = primary or orders
    if not candidates:
        return None

    def sort_key(order):
        dt = _parse_date(order.expected_delivery_date or "")
        if not dt:
            dt = getattr(order, "created_at", None) or getattr(order, "updated_at", None)
        return dt or datetime.min

    return max(candidates, key=sort_key)


def _build_items_from_last_order(
    last_order,
    stores_cfg: List[StoreConfig],
    products: List[Product],
    valid_store_ids: set,
    active_promos: Optional[Dict[Tuple[str, str], bool]] = None,
) -> List[ForecastItem]:
    """Build forecast items by copying quantities from the last order."""
    last_order_confidence = float(os.environ.get("FORECAST_CONFIDENCE_LAST_ORDER_DEFAULT", "0.72"))
    items: List[ForecastItem] = []
    for store in last_order.stores:
        store_id = store.store_id
        store_name = store.store_name
        matched_store = _match_store(stores_cfg, store_id, store_name)
        if matched_store:
            store_id = matched_store.store_id
            store_name = matched_store.store_name

        if store_id not in valid_store_ids:
            continue

        for item in store.items:
            sap = str(item.sap)
            qty = float(item.quantity or 0)
            if qty <= 0:
                continue
            if not _store_active(stores_cfg, store_id, store_name, sap):
                continue

            case_pack = _case_pack_for_sap(products, sap)
            rec_cases = qty / case_pack if case_pack else None
            if rec_cases is None and item.cases:
                try:
                    rec_cases = float(item.cases)
                except Exception:
                    rec_cases = None

            promo_active = False
            if active_promos:
                promo_active = bool(active_promos.get((store_id, sap), False))

            items.append(
                ForecastItem(
                    store_id=store_id,
                    store_name=store_name,
                    sap=sap,
                    recommended_units=qty,
                    recommended_cases=rec_cases,
                    p10_units=qty,
                    p50_units=qty,
                    p90_units=qty,
                    promo_active=promo_active,
                    confidence=last_order_confidence,
                    source="last_order",
                    last_order_quantity=int(qty),
                )
            )
    return items


def _build_last_order_quantity_map(last_order) -> Dict[Tuple[str, str], float]:
    """Build a (store_id, sap) -> qty map from a single historical order."""
    out: Dict[Tuple[str, str], float] = {}
    if not last_order:
        return out
    for store in getattr(last_order, "stores", []) or []:
        sid = str(getattr(store, "store_id", "") or "")
        if not sid:
            continue
        for item in getattr(store, "items", []) or []:
            sap = str(getattr(item, "sap", "") or "")
            if not sap:
                continue
            try:
                qty = float(getattr(item, "quantity", 0.0) or 0.0)
            except Exception:
                qty = 0.0
            out[(sid, sap)] = max(0.0, qty)
    return out


def _parse_date(date_str: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(date_str)
    except Exception:
        return None


def _orders_to_dataframe(orders):
    rows = []
    for o in orders:
        dt = _parse_date(o.expected_delivery_date)
        for store in o.stores:
            for item in store.items:
                promo_active = 1 if getattr(item, "promo_active", False) else 0
                rows.append({
                    "delivery_date": dt,
                    "store": store.store_name,
                    "store_id": store.store_id,
                    "sap": item.sap,
                    "units": item.quantity,
                    "cases": item.cases,
                    "promo_active": promo_active,
                })
    df = pd.DataFrame(rows)
    return df


def _build_features(df: pd.DataFrame, corrections_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    # Ensure sorted by date
    df = df.sort_values("delivery_date").copy()
    df["delivery_dow"] = df["delivery_date"].dt.weekday
    df["delivery_month"] = df["delivery_date"].dt.month
    df["delivery_quarter"] = df["delivery_date"].dt.quarter
    df["is_monday_delivery"] = (df["delivery_dow"] == 0).astype(int)
    df["is_first_weekend_of_month"] = (
        (df["delivery_date"].dt.day <= 7) & (df["delivery_date"].dt.weekday >= 5)
    ).astype(int)
    if "promo_active" not in df.columns:
        df["promo_active"] = 0
    else:
        df["promo_active"] = df["promo_active"].fillna(0).astype(int)

    # ==========================================================================
    # SLOW-MOVER / ORDER FREQUENCY FEATURES
    # These help the ML understand items that are ordered infrequently
    # ==========================================================================
    
    # Get global counts for normalization
    total_order_cycles = df["delivery_date"].nunique()
    total_stores = df["store_id"].nunique()
    
    # Per-SAP metrics (across all stores)
    sap_stats = df.groupby("sap").agg(
        sap_store_count=("store_id", "nunique"),  # How many stores order this SAP
        sap_total_orders=("delivery_date", "nunique"),  # How many cycles has this SAP been ordered
    ).reset_index()
    sap_stats["store_coverage"] = sap_stats["sap_store_count"] / max(total_stores, 1)
    sap_stats["sap_order_frequency"] = sap_stats["sap_total_orders"] / max(total_order_cycles, 1)
    
    # Per store/sap metrics
    store_sap_stats = df.groupby(["store_id", "sap"]).agg(
        times_ordered=("delivery_date", "nunique"),  # How many times this store ordered this SAP
        avg_qty_when_ordered=("units", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
        max_qty_ordered=("units", "max"),
        last_order_date=("delivery_date", "max"),
    ).reset_index()
    store_sap_stats["order_frequency"] = store_sap_stats["times_ordered"] / max(total_order_cycles, 1)
    # Slow mover: ordered 25% of cycles or less (e.g., 1 in 4 cycles or less)
    store_sap_stats["is_slow_mover"] = (store_sap_stats["order_frequency"] <= 0.25).astype(int)
    
    # ==========================================================================
    # LAG FEATURES (per store/sap)
    # ==========================================================================
    out_frames = []
    for (store_id, sap), grp in df.groupby(["store_id", "sap"]):
        g = grp.sort_values("delivery_date").copy()
        g["prev_delivery_date"] = g["delivery_date"].shift(1)
        g["days_since_last_order"] = (
            g["delivery_date"] - g["prev_delivery_date"]
        ).dt.days
        g["lag_1"] = g["units"].shift(1)
        g["lag_2"] = g["units"].shift(2)
        g["rolling_mean_4"] = g["units"].rolling(window=4, min_periods=1).mean().shift(1)
        g["rolling_mean_13"] = g["units"].rolling(window=13, min_periods=1).mean().shift(1)
        out_frames.append(g)
    feat = pd.concat(out_frames, ignore_index=True)

    # Fill lag defaults
    feat["lag_1"] = feat["lag_1"].fillna(0.0)
    feat["lag_2"] = feat["lag_2"].fillna(0.0)
    feat["rolling_mean_4"] = feat["rolling_mean_4"].fillna(feat["lag_1"])
    feat["rolling_mean_13"] = feat["rolling_mean_13"].fillna(feat["rolling_mean_4"])
    feat["days_since_last_order"] = feat["days_since_last_order"].fillna(999.0)
    feat["trend_delta_4_13"] = feat["rolling_mean_4"] - feat["rolling_mean_13"]
    feat["trend_ratio_4_13"] = (
        feat["rolling_mean_4"]
        / feat["rolling_mean_13"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=5.0)
    
    # ==========================================================================
    # MERGE SLOW-MOVER FEATURES
    # ==========================================================================
    # Merge SAP-level stats
    feat = feat.merge(
        sap_stats[["sap", "store_coverage", "sap_order_frequency"]],
        on="sap",
        how="left"
    )
    
    # Merge store/sap-level stats
    feat = feat.merge(
        store_sap_stats[["store_id", "sap", "order_frequency", "is_slow_mover", "avg_qty_when_ordered"]],
        on=["store_id", "sap"],
        how="left"
    )
    
    # Fill any missing slow-mover features with defaults
    feat["store_coverage"] = feat["store_coverage"].fillna(1.0)
    feat["sap_order_frequency"] = feat["sap_order_frequency"].fillna(1.0)
    feat["order_frequency"] = feat["order_frequency"].fillna(1.0)
    feat["is_slow_mover"] = feat["is_slow_mover"].fillna(0)
    feat["avg_qty_when_ordered"] = feat["avg_qty_when_ordered"].fillna(0.0)
    
    # Fill any missing schedule-aware features with defaults
    if "days_until_first_weekend" not in feat.columns:
        feat["days_until_first_weekend"] = 0
    else:
        feat["days_until_first_weekend"] = feat["days_until_first_weekend"].fillna(0)
    
    if "covers_first_weekend" not in feat.columns:
        feat["covers_first_weekend"] = 0
    else:
        feat["covers_first_weekend"] = feat["covers_first_weekend"].fillna(0)
    
    if "covers_weekend" not in feat.columns:
        feat["covers_weekend"] = 0
    else:
        feat["covers_weekend"] = feat["covers_weekend"].fillna(0)
    
    if "days_until_next_delivery" not in feat.columns:
        feat["days_until_next_delivery"] = 4  # Default for 2x/week
    else:
        feat["days_until_next_delivery"] = feat["days_until_next_delivery"].fillna(4)

    # Merge corrections if provided
    # Note: corrections are already filtered by schedule_key in _load_corrections_from_postgres,
    # so we only need to merge on store_id and sap
    if corrections_df is not None and not corrections_df.empty:
        corr = corrections_df.copy()
        rename = {
            "samples": "corr_samples",
            "avg_delta": "corr_avg_delta",
            "avg_ratio": "corr_avg_ratio",
            "ratio_stddev": "corr_ratio_stddev",
            "removal_rate": "corr_removal_rate",
            "promo_rate": "corr_promo_rate",
            "add_rate": "corr_add_rate",
            "avg_add_units": "corr_avg_add_units",
        }
        corr = corr.rename(columns=rename)
        # Drop schedule_key before merge since it was used for filtering, not joining
        corr = corr.drop(columns=["schedule_key"], errors="ignore")
        feat = feat.merge(
            corr,
            on=["store_id", "sap"],
            how="left",
        )
        for col in [
            "corr_samples",
            "corr_avg_delta",
            "corr_avg_ratio",
            "corr_ratio_stddev",
            "corr_removal_rate",
            "corr_promo_rate",
            "corr_add_rate",
            "corr_avg_add_units",
        ]:
            feat[col] = feat.get(col, 0.0).fillna(0.0)
    else:
        for col in [
            "corr_samples",
            "corr_avg_delta",
            "corr_avg_ratio",
            "corr_ratio_stddev",
            "corr_removal_rate",
            "corr_promo_rate",
            "corr_add_rate",
            "corr_avg_add_units",
        ]:
            feat[col] = 0.0

    return feat


def _predict_slow_intermittent_units(row: pd.Series) -> float:
    """Intermittent-demand estimate for slow movers.

    Formula:
      P(order_this_cycle) * E(size | ordered)

    Where:
      P(order_this_cycle) ~= order_frequency (with fallback to sap_order_frequency)
      E(size | ordered)  ~= avg_qty_when_ordered (fallback to lag/rolling history)
    """
    try:
        slow_threshold = float(os.environ.get("SLOW_MOVER_ORDER_FREQ_THRESHOLD", "0.25"))
    except Exception:
        slow_threshold = 0.25

    order_frequency = float(row.get("order_frequency", 0.0) or 0.0)
    sap_order_frequency = float(row.get("sap_order_frequency", 0.0) or 0.0)
    order_prob = order_frequency if order_frequency > 0 else sap_order_frequency
    order_prob = max(0.0, min(1.0, order_prob))

    # Keep expected demand conservative for slow movers.
    if order_prob > slow_threshold:
        order_prob = slow_threshold

    avg_qty_when_ordered = float(row.get("avg_qty_when_ordered", 0.0) or 0.0)
    lag_1 = float(row.get("lag_1", 0.0) or 0.0)
    rolling_mean_4 = float(row.get("rolling_mean_4", 0.0) or 0.0)
    lag_2 = float(row.get("lag_2", 0.0) or 0.0)

    expected_size = avg_qty_when_ordered
    if expected_size <= 0:
        expected_size = max(lag_1, rolling_mean_4, lag_2, 0.0)

    return max(0.0, order_prob * expected_size)


def _compute_slow_suppression_days(order_frequency: float) -> int:
    """Dynamic suppression window for slow-mover repeat recommendations."""
    min_days = int(os.environ.get("SLOW_MOVER_SUPPRESSION_MIN_DAYS", "14"))
    max_days = int(os.environ.get("SLOW_MOVER_SUPPRESSION_MAX_DAYS", "42"))
    base_cycle_days = float(os.environ.get("SLOW_MOVER_BASE_CYCLE_DAYS", "7.0"))
    factor = float(os.environ.get("SLOW_MOVER_SUPPRESSION_FACTOR", "0.80"))
    freq_floor = float(os.environ.get("SLOW_MOVER_SUPPRESSION_FREQ_FLOOR", "0.05"))

    freq = max(freq_floor, float(order_frequency or 0.0))
    expected_gap = base_cycle_days / freq
    dynamic_days = int(round(expected_gap * factor))
    return max(min_days, min(max_days, dynamic_days))


def _apply_correction_damping(row: pd.Series, pred_units: float) -> float:
    """Apply deterministic correction feedback to the point estimate.

    Captures both sides of feedback:
      - predicted>0 then user zeroed (removals): damp down aggressively
      - predicted=0 then user added lines: modest upward nudge
    """
    pred = max(0.0, float(pred_units or 0.0))
    if pred <= 0:
        return 0.0

    corr_samples = float(row.get("corr_samples", 0.0) or 0.0)
    if corr_samples <= 0:
        return pred

    corr_avg_ratio = float(row.get("corr_avg_ratio", 1.0) or 1.0)
    corr_avg_delta = float(row.get("corr_avg_delta", 0.0) or 0.0)
    corr_removal_rate = max(0.0, min(1.0, float(row.get("corr_removal_rate", 0.0) or 0.0)))
    corr_add_rate = max(0.0, min(1.0, float(row.get("corr_add_rate", 0.0) or 0.0)))
    corr_avg_add_units = max(0.0, float(row.get("corr_avg_add_units", 0.0) or 0.0))
    days_since_last = float(row.get("days_since_last_order", 999.0) or 999.0)

    full_effect_samples = float(os.environ.get("FORECAST_CORR_FULL_EFFECT_SAMPLES", "6"))
    sample_strength = max(0.0, min(1.0, corr_samples / max(1.0, full_effect_samples)))

    removal_mult = 1.0 - min(
        0.92,
        corr_removal_rate * float(os.environ.get("FORECAST_CORR_REMOVAL_MULT_SCALE", "0.95")),
    )
    ratio_mult = min(1.0, max(0.10, corr_avg_ratio))
    downward_target = min(removal_mult, ratio_mult)

    adjusted = pred * ((1.0 - sample_strength) + sample_strength * downward_target)

    # If user frequently adds after zero forecast, nudge up cautiously.
    if corr_add_rate > 0.0 and corr_avg_add_units > 0.0:
        add_weight = float(os.environ.get("FORECAST_CORR_ADD_DELTA_WEIGHT", "0.25"))
        add_strength = sample_strength * max(0.0, 1.0 - corr_removal_rate) * corr_add_rate
        add_cap = min(corr_avg_add_units, max(2.0, pred * 0.60))
        adjusted += add_cap * add_strength * add_weight

    # Hard-zero guardrail for repeated removals on stale lines.
    hard_zero_min_samples = float(os.environ.get("FORECAST_CORR_HARD_ZERO_MIN_SAMPLES", "3"))
    hard_zero_removal = float(os.environ.get("FORECAST_CORR_HARD_ZERO_REMOVAL_RATE", "0.80"))
    hard_zero_stale_days = float(os.environ.get("FORECAST_CORR_HARD_ZERO_MIN_STALE_DAYS", "14"))
    if (
        corr_samples >= hard_zero_min_samples
        and corr_removal_rate >= hard_zero_removal
        and days_since_last >= hard_zero_stale_days
    ):
        return 0.0

    # Small negative correction bias should still reduce output slightly.
    if corr_avg_delta < 0:
        adjusted += max(corr_avg_delta * 0.15, -max(2.0, pred * 0.35))

    return max(0.0, adjusted)


def _should_zero_low_signal_slow_line(row: pd.Series, case_pack: Optional[int]) -> bool:
    """Pre-allocation gate to prevent tiny slow-mover predictions from becoming full cases."""
    if not case_pack or case_pack <= 0:
        return False

    pred_units = float(row.get("pred_units", 0.0) or 0.0)
    confidence = float(row.get("confidence", 0.0) or 0.0)
    corr_removal_rate = float(row.get("corr_removal_rate", 0.0) or 0.0)
    corr_samples = float(row.get("corr_samples", 0.0) or 0.0)
    days_since_last = float(row.get("days_since_last_order", 999.0) or 999.0)

    min_case_fraction = float(os.environ.get("SLOW_MOVER_MIN_CASE_FRACTION_GATE", "0.60"))
    low_conf_threshold = float(os.environ.get("SLOW_MOVER_LOW_CONFIDENCE_GATE", "0.55"))
    removal_threshold = float(os.environ.get("SLOW_MOVER_REMOVAL_RATE_GATE", "0.50"))
    min_samples = float(os.environ.get("SLOW_MOVER_REMOVAL_MIN_SAMPLES_GATE", "2"))
    stale_days = float(os.environ.get("SLOW_MOVER_STALE_DAYS_GATE", "14"))

    return (
        pred_units > 0.0
        and pred_units < (float(case_pack) * min_case_fraction)
        and confidence < low_conf_threshold
        and corr_samples >= min_samples
        and corr_removal_rate >= removal_threshold
        and days_since_last >= stale_days
    )


def _compute_prediction_confidence(row: pd.Series, branch: str) -> float:
    """Compute calibrated confidence score [0.05, 0.98] for a forecast row.

    Confidence is based on:
    - observed ordering frequency / data sufficiency
    - correction history depth
    - correction volatility (ratio stddev)
    - correction removal rate

    Branch effect:
    - gbr: higher base confidence
    - slow_intermittent: lower base confidence due sparse/intermittent demand
    """
    order_frequency = float(row.get("order_frequency", 0.0) or 0.0)
    sap_order_frequency = float(row.get("sap_order_frequency", 0.0) or 0.0)
    corr_samples = float(row.get("corr_samples", 0.0) or 0.0)
    corr_ratio_stddev = float(row.get("corr_ratio_stddev", 0.0) or 0.0)
    corr_removal_rate = float(row.get("corr_removal_rate", 0.0) or 0.0)
    trend_ratio = float(row.get("trend_ratio_4_13", 1.0) or 1.0)
    is_slow = int(row.get("is_slow_mover", 0) or 0) == 1

    data_strength = max(0.0, min(1.0, max(order_frequency, sap_order_frequency)))
    sample_strength = max(0.0, min(1.0, corr_samples / 6.0))
    volatility_penalty = max(0.0, min(1.0, abs(corr_ratio_stddev) / 0.60))
    removal_penalty = max(0.0, min(1.0, corr_removal_rate))

    base = 0.58 if branch == "gbr" else 0.44
    confidence = (
        base
        + 0.24 * data_strength
        + 0.10 * sample_strength
        - 0.18 * volatility_penalty
        - 0.08 * removal_penalty
    )

    if is_slow:
        confidence -= 0.05

    # Sparse recency signal should reduce confidence.
    lag_1 = float(row.get("lag_1", 0.0) or 0.0)
    lag_2 = float(row.get("lag_2", 0.0) or 0.0)
    rolling_mean_4 = float(row.get("rolling_mean_4", 0.0) or 0.0)
    if lag_1 <= 0 and lag_2 <= 0 and rolling_mean_4 <= 0:
        confidence -= 0.08

    # Demand regime drift penalty: widen caution when short-term trend diverges
    # from medium-term baseline (4-cycle vs 13-cycle ratio).
    trend_shift = abs(trend_ratio - 1.0)
    confidence -= min(0.12, trend_shift * 0.10)

    return float(max(0.05, min(0.98, confidence)))


def _compute_prediction_bands(
    row: pd.Series,
    pred_units: float,
    confidence: float,
    branch: str,
    band_scale: float = 1.0,
) -> Tuple[float, float, float]:
    """Compute pragmatic uncertainty bands (p10/p50/p90) around a point prediction.

    This is a calibrated heuristic band model for operational use. It uses
    recent signal strength and correction volatility to widen/narrow the interval.
    """
    p50 = max(0.0, float(pred_units))

    lag_1 = float(row.get("lag_1", 0.0) or 0.0)
    lag_2 = float(row.get("lag_2", 0.0) or 0.0)
    rolling_mean_4 = float(row.get("rolling_mean_4", 0.0) or 0.0)
    avg_qty_when_ordered = float(row.get("avg_qty_when_ordered", 0.0) or 0.0)
    corr_avg_delta = abs(float(row.get("corr_avg_delta", 0.0) or 0.0))
    corr_ratio_stddev = abs(float(row.get("corr_ratio_stddev", 0.0) or 0.0))
    trend_ratio = float(row.get("trend_ratio_4_13", 1.0) or 1.0)

    signal_scale = max(p50, lag_1, lag_2, rolling_mean_4, avg_qty_when_ordered, 1.0)
    confidence_penalty = max(0.15, (1.0 - float(confidence)) + 0.10)
    branch_scale = 0.22 if branch == "gbr" else 0.35

    # Approximate sigma with a blend of model-scale and correction volatility.
    sigma = max(
        1.0,
        signal_scale * branch_scale * confidence_penalty,
        corr_avg_delta * 0.80,
        corr_ratio_stddev * signal_scale,
        signal_scale * min(0.60, abs(trend_ratio - 1.0) * 0.35),
    )
    sigma *= max(0.5, min(8.0, float(band_scale or 1.0)))

    z90 = 1.2815515655446004  # 10th/90th percentile z-score
    p10 = max(0.0, p50 - z90 * sigma)
    p90 = max(p50, p50 + z90 * sigma)
    return float(p10), float(p50), float(p90)


def _apply_uncertainty_safe_policy(
    recommended_units: float,
    p10_units: float,
    p50_units: float,
    p90_units: float,
    confidence: float,
    last_order_units: Optional[float],
    case_pack: Optional[int],
) -> Tuple[float, float, float, float, bool]:
    """Clamp low-confidence changes toward last order to avoid aggressive swings."""
    threshold = float(os.environ.get("FORECAST_UNCERTAINTY_SAFE_THRESHOLD", "0.42"))
    if last_order_units is None or confidence >= threshold:
        return recommended_units, p10_units, p50_units, p90_units, False

    max_delta_pct = float(os.environ.get("FORECAST_UNCERTAINTY_SAFE_MAX_DELTA_PCT", "0.35"))
    min_delta_units_env = float(os.environ.get("FORECAST_UNCERTAINTY_SAFE_MIN_DELTA_UNITS", "1"))

    cp = float(case_pack) if case_pack and case_pack > 0 else 1.0
    max_delta_units = max(min_delta_units_env, cp, float(last_order_units) * max_delta_pct)
    lower = max(0.0, float(last_order_units) - max_delta_units)
    upper = max(lower, float(last_order_units) + max_delta_units)
    clamped = min(max(float(recommended_units), lower), upper)

    if abs(clamped - float(recommended_units)) < 1e-9:
        return recommended_units, p10_units, p50_units, p90_units, False

    shift = clamped - float(p50_units)
    next_p50 = max(0.0, clamped)
    next_p10 = max(0.0, float(p10_units) + shift)
    next_p90 = max(next_p50, float(p90_units) + shift)
    return clamped, next_p10, next_p50, next_p90, True


def _apply_band_center_offset(
    p10_units: float,
    p50_units: float,
    p90_units: float,
    center_offset_units: float,
) -> Tuple[float, float, float]:
    """Shift the uncertainty interval center by schedule-level bias offset."""
    offset = float(center_offset_units or 0.0)
    if abs(offset) < 1e-9:
        return float(p10_units), float(p50_units), float(p90_units)

    next_p10 = max(0.0, float(p10_units) + offset)
    next_p50 = max(0.0, float(p50_units) + offset)
    next_p90 = max(next_p50, float(p90_units) + offset)
    return next_p10, next_p50, next_p90


def _apply_band_scale_and_center(
    p10_units: float,
    p50_units: float,
    p90_units: float,
    scale_mult: float = 1.0,
    center_offset_units: float = 0.0,
) -> Tuple[float, float, float]:
    """Scale uncertainty width around p50, then shift interval center."""
    p10 = float(p10_units or 0.0)
    p50 = float(p50_units or 0.0)
    p90 = float(p90_units or 0.0)

    scale = max(0.1, float(scale_mult or 1.0))
    lo_span = max(0.0, p50 - p10)
    hi_span = max(0.0, p90 - p50)
    p10 = p50 - (lo_span * scale)
    p90 = p50 + (hi_span * scale)

    return _apply_band_center_offset(
        p10_units=p10,
        p50_units=p50,
        p90_units=p90,
        center_offset_units=center_offset_units,
    )


def _confidence_bucket(confidence: Optional[float]) -> str:
    """Map confidence score to stable UI buckets."""
    c = float(confidence or 0.0)
    if c >= 0.75:
        return "high"
    if c >= 0.50:
        return "medium"
    return "low"


def _train_and_predict(
    feat: pd.DataFrame,
    target_date: datetime,
    days_until_next: int = 4,
    active_promos: Optional[Dict[Tuple[str, str], bool]] = None,
    band_scale: float = 1.0,
) -> pd.DataFrame:
    """Train GBR on historical rows and predict next for each store/sap using last known lags.
    
    Args:
        feat: Feature dataframe with historical data
        target_date: Date to predict for
        days_until_next: Days until next delivery (from user's schedule)
    """
    if feat.empty:
        return pd.DataFrame(columns=[
            "store", "store_id", "sap", "pred_units",
            "p10_units", "p50_units", "p90_units",
            "confidence", "confidence_bucket",
        ])

    feat = feat.sort_values("delivery_date")
    feature_cols = [
        # Lag features
        "lag_1",
        "lag_2",
        "rolling_mean_4",
        "rolling_mean_13",
        "trend_delta_4_13",
        "trend_ratio_4_13",
        "days_since_last_order",
        # Calendar features
        "delivery_dow",
        "delivery_month",
        "delivery_quarter",
        "is_monday_delivery",
        "is_first_weekend_of_month",
        # Schedule-aware features (learns from user's delivery pattern)
        "days_until_first_weekend",  # Days from delivery to first weekend of month
        "covers_first_weekend",      # TRUE if this delivery covers first weekend
        "covers_weekend",            # TRUE if this delivery covers Sat/Sun
        "days_until_next_delivery",  # Days until next scheduled delivery
        # Slow-mover / order frequency features
        "store_coverage",          # How many stores carry this SAP (0-1)
        "sap_order_frequency",     # How often this SAP is ordered overall (0-1)
        "order_frequency",         # How often this store orders this SAP (0-1)
        "is_slow_mover",           # Binary: order_frequency < 0.25
        "avg_qty_when_ordered",    # Average quantity when this store orders this SAP
        # Promo features
        "promo_active",
        # User correction features
        "corr_samples",
        "corr_avg_delta",
        "corr_avg_ratio",
        "corr_ratio_stddev",
        "corr_removal_rate",
        "corr_promo_rate",
        "corr_add_rate",
        "corr_avg_add_units",
    ]

    # Training data: all rows with lag_1 available
    train_df = feat.dropna(subset=["lag_1"]).copy()
    if train_df.empty:
        return pd.DataFrame(columns=[
            "store", "store_id", "sap", "pred_units",
            "p10_units", "p50_units", "p90_units",
            "confidence", "confidence_bucket",
        ])

    X_train = train_df[feature_cols]
    y_train = train_df["units"]
    model = GradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Build inference rows: last observation per store/sap, reuse latest lags
    preds = []
    
    # Compute schedule-aware features for target date using actual schedule
    target_schedule_feats = _compute_schedule_features(target_date, days_until_next)
    
    slow_branch_enabled = os.environ.get("FORECAST_ENABLE_INTERMITTENT_SLOW", "1").lower() in ("1", "true", "yes")
    slow_threshold = float(os.environ.get("SLOW_MOVER_ORDER_FREQ_THRESHOLD", "0.25"))
    slow_branch_count = 0
    gbr_branch_count = 0

    for (store_id, sap), grp in feat.groupby(["store_id", "sap"]):
        last = grp.iloc[-1]
        row = last[feature_cols].copy()
        # update date features to target_date
        row = row.copy()
        row["delivery_dow"] = target_date.weekday()
        row["delivery_month"] = target_date.month
        row["delivery_quarter"] = (target_date.month - 1) // 3 + 1
        row["is_monday_delivery"] = 1 if target_date.weekday() == 0 else 0
        row["is_first_weekend_of_month"] = 1 if (target_date.day <= 7 and target_date.weekday() >= 5) else 0
        try:
            last_delivery_dt = pd.to_datetime(last.get("delivery_date"))
            row["days_since_last_order"] = float(max(0, (target_date - last_delivery_dt.to_pydatetime()).days))
        except Exception:
            row["days_since_last_order"] = float(row.get("days_since_last_order", 999.0) or 999.0)
        
        # NEW: Schedule-aware features for target date
        row["days_until_first_weekend"] = target_schedule_feats["days_until_first_weekend"]
        row["covers_first_weekend"] = target_schedule_feats["covers_first_weekend"]
        row["covers_weekend"] = target_schedule_feats["covers_weekend"]
        row["days_until_next_delivery"] = target_schedule_feats["days_until_next_delivery"]
        # Promo lift is handled via promo-history override later.
        # Keep model inference neutral unless explicitly enabled.
        promo_model_lift = os.environ.get("PROMO_MODEL_LIFT", "0") == "1"
        if promo_model_lift and active_promos:
            row["promo_active"] = 1 if active_promos.get((store_id, str(sap)), False) else 0
        else:
            row["promo_active"] = 0

        # Explicit intermittent-demand branch for SLOW movers.
        # MEDIUM/FAST stay on existing GBR path unchanged.
        is_slow_flag = int(row.get("is_slow_mover", 0) or 0) == 1
        order_frequency = float(row.get("order_frequency", 1.0) or 1.0)
        use_slow_branch = slow_branch_enabled and (is_slow_flag or order_frequency <= slow_threshold)

        if use_slow_branch:
            raw_pred_units = _predict_slow_intermittent_units(row)
            pred_units = _apply_correction_damping(row, raw_pred_units)
            confidence = _compute_prediction_confidence(row, branch="slow_intermittent")
            p10_units, p50_units, p90_units = _compute_prediction_bands(
                row=row,
                pred_units=pred_units,
                confidence=confidence,
                branch="slow_intermittent",
                band_scale=band_scale,
            )
            slow_branch_count += 1
        else:
            model_input = pd.DataFrame([row], columns=feature_cols)
            raw_pred_units = float(model.predict(model_input)[0])
            pred_units = _apply_correction_damping(row, raw_pred_units)
            confidence = _compute_prediction_confidence(row, branch="gbr")
            p10_units, p50_units, p90_units = _compute_prediction_bands(
                row=row,
                pred_units=pred_units,
                confidence=confidence,
                branch="gbr",
                band_scale=band_scale,
            )
            gbr_branch_count += 1

        preds.append(
            {
                "store": last["store"],
                "store_id": store_id,
                "sap": str(sap),
                "pred_units": max(0.0, pred_units),
                "p10_units": p10_units,
                "p50_units": p50_units,
                "p90_units": p90_units,
                "model_branch": "slow_intermittent" if use_slow_branch else "gbr",
                "confidence": confidence,
                "confidence_bucket": _confidence_bucket(confidence),
                "order_frequency": float(row.get("order_frequency", 0.0) or 0.0),
                "is_slow_mover": int(row.get("is_slow_mover", 0) or 0),
                "days_since_last_order": float(row.get("days_since_last_order", 999.0) or 999.0),
                "corr_samples": float(row.get("corr_samples", 0.0) or 0.0),
                "corr_removal_rate": float(row.get("corr_removal_rate", 0.0) or 0.0),
                "corr_avg_ratio": float(row.get("corr_avg_ratio", 1.0) or 1.0),
                "corr_avg_delta": float(row.get("corr_avg_delta", 0.0) or 0.0),
                "corr_add_rate": float(row.get("corr_add_rate", 0.0) or 0.0),
                "corr_avg_add_units": float(row.get("corr_avg_add_units", 0.0) or 0.0),
            }
        )

    if slow_branch_enabled:
        print(
            f"[forecast] Intermittent branch coverage: slow={slow_branch_count}, "
            f"gbr={gbr_branch_count}, threshold={slow_threshold}"
        )

    return pd.DataFrame(preds)


def _get_stores_for_delivery_day(stores_cfg: List[StoreConfig], delivery_weekday: str) -> set:
    """Return set of store IDs that receive deliveries on the given weekday."""
    # Normalize weekday name (e.g., 'Thursday' -> 'thursday')
    delivery_weekday_lower = delivery_weekday.lower()
    
    valid_stores = set()
    for store in stores_cfg:
        # Check if store delivers on this day
        store_delivery_days = [d.lower() for d in (store.delivery_days or [])]
        if delivery_weekday_lower in store_delivery_days:
            valid_stores.add(store.store_id)
    
    return valid_stores


def _get_stores_for_order_cycle(stores_cfg: List[StoreConfig], order_cycles: List[dict], schedule_key: str) -> set:
    """Return set of store IDs that are part of an order cycle.
    
    DATA-DRIVEN: Reads store.deliveryDays and matches against cycle's deliveryDay/loadDay.
    No hardcoding.
    
    Data sources:
        - order_cycles: from users/{uid}/userSettings.notifications.scheduling.orderCycles
        - stores_cfg[].delivery_days: from routes/{route}/stores/{storeId}.deliveryDays
    
    Args:
        stores_cfg: List of store configurations (with delivery_days attribute)
        order_cycles: List of order cycle dicts with orderDay, loadDay, deliveryDay
        schedule_key: Order day key ('monday', 'tuesday', etc.)
        
    Returns:
        Set of store IDs in this order cycle
    """
    from schedule_utils import day_name_to_num, get_cycle_delivery_days
    
    # Find the cycle for this schedule_key
    order_day_num = day_name_to_num(schedule_key)
    target_cycle = next((c for c in order_cycles if c.get('orderDay') == order_day_num), None)
    
    if not target_cycle:
        print(f"[forecast] Warning: No cycle found for schedule_key '{schedule_key}'. Including all stores.")
        return {s.store_id for s in stores_cfg}
    
    # Get the days this cycle delivers on (deliveryDay + loadDay)
    cycle_days = get_cycle_delivery_days(target_cycle)
    
    # Match stores whose deliveryDays intersect with cycle_days
    valid_store_ids = set()
    for store in stores_cfg:
        store_days = {d.lower() for d in (store.delivery_days or [])}
        if store_days & cycle_days:
            valid_store_ids.add(store.store_id)
    
    return valid_store_ids


def generate_forecast(config: ForecastConfig) -> ForecastPayload:
    db = get_firestore_client(config.service_account)
    use_postgres = os.environ.get("FORECAST_USE_POSTGRES", "1").lower() in ("1", "true", "yes")
    
    delivery_iso = normalize_delivery_date(config.delivery_date)
    target_dt = _parse_date(delivery_iso) or datetime.utcnow()
    
    # Get the delivery weekday name (e.g., 'Thursday', 'Friday')
    WEEKDAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    delivery_weekday = WEEKDAY_NAMES[target_dt.weekday()]

    # Load data
    if use_postgres:
        try:
            products = load_catalog_from_postgres(config.route_number)
            stores_cfg = load_stores_from_postgres(config.route_number)
        except Exception as e:
            print(f"[forecast] Warning: PostgreSQL loaders failed, falling back to Firestore: {e}")
            products = load_master_catalog(db, config.route_number)
            stores_cfg = load_store_configs(db, config.route_number)
    else:
        products = load_master_catalog(db, config.route_number)
        stores_cfg = load_store_configs(db, config.route_number)
    
    # Load order cycles from PostgreSQL/Firebase (DATA-DRIVEN - no hardcoding)
    from schedule_utils import get_order_cycles
    order_cycles = get_order_cycles(db, config.route_number)
    if not order_cycles:
        raise ValueError(
            f"[forecast] No order cycles found for route {config.route_number}. "
            f"Check: users/{{uid}}/userSettings.notifications.scheduling.orderCycles"
        )
    print(f"[forecast] Order cycles loaded: {order_cycles}")
    
    # schedule_key is based on ORDER day, not delivery day
    # e.g., Thursday delivery → 'monday' (ordered Monday)
    schedule_key = get_order_day_for_delivery(db, config.route_number, delivery_iso)

    # Ensure case-allocation shares are current (PostgreSQL only, no Firebase reads)
    try:
        try:
            from .compute_shares_pg import ensure_shares_fresh  # type: ignore
        except Exception:
            from compute_shares_pg import ensure_shares_fresh  # type: ignore

        window_orders = int(os.environ.get("SHARE_WINDOW_ORDERS", "20"))
        recent_count = int(os.environ.get("SHARE_RECENT_COUNT", "4"))
        recent_weight = float(os.environ.get("SHARE_RECENT_WEIGHT", "0.6"))

        refreshed, reason = ensure_shares_fresh(
            config.route_number,
            schedule_key,
            window_orders=window_orders,
            recent_count=recent_count,
            recent_weight=recent_weight,
        )
        if refreshed:
            print(f"[forecast] Refreshed case shares ({reason}).")
    except Exception as e:
        print(f"[forecast] Warning: could not refresh case shares: {e}")
    
    # Check for prior orders from other schedules that might overlap (e.g., both deliver to Bowman's)
    prior_order_context = _get_prior_order_context(
        db=db,
        route_number=config.route_number,
        current_schedule_key=schedule_key,
        target_delivery_date=delivery_iso,
        stores_cfg=stores_cfg,
    )
    
    # Build active promo lookup for this delivery date
    valid_saps = {p.sap for p in products if getattr(p, "sap", None)}
    active_promos = _build_active_promo_lookup(
        db=db,
        route_number=config.route_number,
        delivery_date=delivery_iso,
        stores_cfg=stores_cfg,
        valid_saps=valid_saps,
    )
    if active_promos:
        print(f"[forecast] Active promos: {len(active_promos)} store/sap combinations on promo for {delivery_iso}")
    
    # Cross-cycle suppression: find items ordered in OTHER schedules recently
    # This prevents double-ordering slow movers (e.g., ordered Tuesday, now Monday forecast)
    cross_cycle_lookback_days = int(os.environ.get("SLOW_MOVER_CROSS_CYCLE_LOOKBACK_DAYS", "7"))
    cross_cycle_orders = _get_cross_cycle_orders(
        route_number=config.route_number,
        current_schedule=schedule_key,
        target_delivery_date=delivery_iso,
        lookback_days=cross_cycle_lookback_days,
    )
    
    # Same-cycle recent orders: find slow movers ordered in THIS schedule recently
    # Prevents recommending a slow mover that was just ordered 7 days ago in the same cycle
    same_cycle_lookback_days = int(os.environ.get("SLOW_MOVER_SAME_CYCLE_LOOKBACK_DAYS", "42"))
    same_cycle_recent = _get_same_cycle_recent_orders(
        route_number=config.route_number,
        current_schedule=schedule_key,
        target_delivery_date=delivery_iso,
        lookback_days=same_cycle_lookback_days,
    )
    if same_cycle_recent:
        print(
            f"[forecast] Found {len(same_cycle_recent)} SAPs ordered in same cycle "
            f"within {same_cycle_lookback_days} days"
        )
    
    same_cycle_recent_by_store = _get_same_cycle_recent_orders_by_store(
        route_number=config.route_number,
        current_schedule=schedule_key,
        target_delivery_date=delivery_iso,
        lookback_days=same_cycle_lookback_days,
    )
    if same_cycle_recent_by_store:
        print(
            f"[forecast] Found {len(same_cycle_recent_by_store)} store/SAPs ordered in same cycle "
            f"within {same_cycle_lookback_days} days"
        )
    
    # SAP velocity tiers: SLOW (≤25%), MEDIUM (25-60%), FAST (>60%)
    # Used for tiered suppression logic - slow movers get rule-based handling
    sap_tiers = _get_sap_tiers(
        route_number=config.route_number,
        slow_threshold=0.25,
        fast_threshold=0.60,
        home_cycle_min_share=0.60,  # Need 60% of orders in one cycle to assign home
    )
    store_sap_home_cycles = _get_store_sap_home_cycles(
        route_number=config.route_number,
        home_cycle_min_share=0.60,
    )
    
    # DATA-DRIVEN: Match stores to cycle based on store.deliveryDays vs cycle.deliveryDay/loadDay
    # No hardcoding - reads from Firebase data
    valid_store_ids = _get_stores_for_order_cycle(stores_cfg, order_cycles, schedule_key)
    print(f"[forecast] Order cycle: {schedule_key} (primary delivery: {delivery_iso} {delivery_weekday})")
    print(f"[forecast] All stores in {schedule_key} order: {[s.store_name for s in stores_cfg if s.store_id in valid_store_ids]}")
    
    # Forecast mode/scope:
    # - copy_last_order: cold-start fallback until schedule + correction thresholds are met
    # - schedule_aware: same-schedule model context
    # - store_centric: cross-schedule/store context when route depth supports it
    requested_scope = str(os.environ.get("FORECAST_TRAINING_SCOPE", "auto") or "auto").lower()
    schedule_keys_to_load = None if requested_scope not in ("schedule_only", "cycle_only") else [schedule_key]

    if use_postgres:
        try:
            orders = load_orders_from_postgres(
                route_number=config.route_number,
                since_days=config.since_days,
                schedule_keys=schedule_keys_to_load,
            )
        except Exception as e:
            print(f"[forecast] Warning: PostgreSQL orders load failed, falling back to Firestore: {e}")
            orders = load_orders(
                db,
                route_number=config.route_number,
                since_days=config.since_days,
                schedule_keys=schedule_keys_to_load,
            )
    else:
            orders = load_orders(
                db,
                route_number=config.route_number,
                since_days=config.since_days,
                schedule_keys=schedule_keys_to_load,
            )
    # Keep only orders with valid schedule key for model context.
    orders = [o for o in orders if str(getattr(o, "schedule_key", "") or "").strip()]
    all_orders = list(orders)

    mode_decision = _resolve_forecast_mode(
        route_number=config.route_number,
        schedule_key=schedule_key,
        requested_scope=requested_scope,
        all_orders=all_orders,
        order_cycles=order_cycles,
        since_days=config.since_days,
    )
    training_scope = mode_decision.training_scope
    schedule_only_scope = training_scope == "schedule_only"
    if schedule_only_scope:
        orders = [
            o
            for o in all_orders
            if str(getattr(o, "schedule_key", "") or "").lower() == str(schedule_key).lower()
        ]
    else:
        orders = all_orders
    schedule_counts: Dict[str, int] = {}
    for o in orders:
        sk = str(getattr(o, "schedule_key", "") or "").lower()
        if not sk:
            continue
        schedule_counts[sk] = schedule_counts.get(sk, 0) + 1
    print(
        f"[forecast] Mode={mode_decision.mode} scope={training_scope} "
        f"schedule_orders={mode_decision.schedule_order_count} "
        f"corrected_orders={mode_decision.corrected_order_count} "
        f"reason={mode_decision.reason} loaded_orders={len(orders)} "
        f"schedule_counts={schedule_counts} schedule_shape={mode_decision.schedule_shape}"
    )

    # Load corrections from PostgreSQL (replaces CSV-based approach)
    corrections_df = _load_corrections_from_postgres(config.route_number, schedule_key)
    correction_samples = int(corrections_df["samples"].sum()) if corrections_df is not None else 0

    # Fallback for early forecasts: copy last order for this cycle until we have
    # enough schedule history to qualify for training. This keeps cold-start
    # forecasts anchored to real user behavior and captures user deltas early.
    fallback_enabled = os.environ.get("FORECAST_FALLBACK_LAST_ORDER", "1").lower() in ("1", "true", "yes")
    schedule_orders = _filter_orders_by_schedule(all_orders, schedule_key)
    schedule_order_count = len(schedule_orders)
    last_order_for_schedule = _select_last_order_for_schedule(all_orders, schedule_key) if all_orders else None
    if fallback_enabled and mode_decision.mode == "copy_last_order" and all_orders:
        if last_order_for_schedule:
            fallback_items = _build_items_from_last_order(
                last_order=last_order_for_schedule,
                stores_cfg=stores_cfg,
                products=products,
                valid_store_ids=valid_store_ids,
                active_promos=active_promos,
            )
            if fallback_items:
                print(
                    f"[forecast] Fallback to last order (schedule={schedule_key}) "
                    f"mode={mode_decision.mode} reason={mode_decision.reason} "
                    f"(schedule_orders={schedule_order_count}, "
                    f"corrected_orders={mode_decision.corrected_order_count}); "
                    f"corrections={correction_samples}."
                )
                fallback_forecast = ForecastPayload(
                    forecast_id=str(uuid.uuid4()),
                    route_number=config.route_number,
                    delivery_date=delivery_iso,
                    schedule_key=schedule_key,
                    generated_at=datetime.utcnow(),
                    items=fallback_items,
                )
                # Write to Firebase cache (same as ML path)
                write_cached_forecast(
                    db=db,
                    route_number=config.route_number,
                    forecast=fallback_forecast,
                    ttl_days=config.ttl_days,
                )
                try:
                    write_forecast_transfer_suggestions(
                        db=db,
                        route_number=config.route_number,
                        forecast=fallback_forecast,
                    )
                except Exception as e:
                    print(f"[forecast] Warning: transfer suggestion generation failed (fallback path): {e}")
                return fallback_forecast

    # Check minimum data threshold
    if len(orders) < MIN_ORDERS_FOR_FORECAST:
        raise ValueError(
            f"insufficient_history: Only {len(orders)} orders found, "
            f"need at least {MIN_ORDERS_FOR_FORECAST} for forecasting"
        )
    last_order_qty_map = _build_last_order_quantity_map(last_order_for_schedule)

    # Load promo history (last promo quantities per store+SAP)
    promo_history = _get_last_promo_quantities(
        route_number=config.route_number,
        schedule_keys=[schedule_key] if schedule_only_scope else None,
        delivery_date=delivery_iso,
        lookback_days=int(os.environ.get("PROMO_HISTORY_DAYS", "365")),
    )
    if promo_history:
        print(f"[forecast] Loaded promo history for {len(promo_history)} store/SAP pairs")

    # Build dataframe
    df_orders = _orders_to_dataframe(orders)

    # Get user's schedule info for schedule-aware features
    # Uses PostgreSQL first, falls back to Firebase if needed
    schedule_info = get_schedule_info(db, config.route_number)
    days_between = schedule_info.get('days_between_deliveries', {})
    print(f"[forecast] User schedule: {schedule_info.get('cycles', [])}")
    print(f"[forecast] Days between deliveries: {days_between}")
    
    def get_days_until_next_for_dow(py_dow: int) -> int:
        """Get days until next delivery based on user's schedule."""
        # Convert Python DOW (0=Mon) to Firebase DOW (0=Sun, 1=Mon)
        fb_dow = (py_dow + 1) % 7
        return days_between.get(fb_dow, 4)  # Default 4 if not found
    
    # Get days_until_next for target date
    target_days_until_next = get_days_until_next_for_dow(target_dt.weekday())
    print(f"[forecast] Target {target_dt.strftime('%A')} → {target_days_until_next} days until next delivery")
    
    # Add simple calendar flags to df_orders before feature build
    if not df_orders.empty:
        df_orders["delivery_month"] = df_orders["delivery_date"].dt.month
        df_orders["delivery_quarter"] = df_orders["delivery_date"].dt.quarter
        # Legacy feature (keep for backwards compatibility)
        df_orders["is_first_weekend_of_month"] = (
            (df_orders["delivery_date"].dt.day <= 7)
            & (df_orders["delivery_date"].dt.weekday >= 5)
        ).astype(int)
        
        # NEW: Schedule-aware features using ACTUAL user schedule
        df_orders["days_until_next_delivery"] = df_orders["delivery_date"].apply(
            lambda d: get_days_until_next_for_dow(d.weekday())
        )
        df_orders["days_until_first_weekend"] = df_orders["delivery_date"].apply(
            lambda d: _days_until_first_weekend(d.to_pydatetime())
        )
        df_orders["covers_first_weekend"] = df_orders.apply(
            lambda row: int(_covers_first_weekend(
                row["delivery_date"].to_pydatetime(), 
                row["days_until_next_delivery"]
            )), axis=1
        )
        df_orders["covers_weekend"] = df_orders.apply(
            lambda row: int(_covers_weekend(
                row["delivery_date"].weekday(), 
                row["days_until_next_delivery"]
            )), axis=1
        )
    feat = _build_features(df_orders, corrections_df)
    band_scale, band_center_offset = _load_schedule_band_calibration(config.route_number, schedule_key)
    source_band_adjustments = _load_schedule_source_band_calibration(config.route_number, schedule_key)
    if abs(band_scale - 1.0) > 1e-6 or abs(band_center_offset) > 1e-6:
        print(
            f"[forecast] Applying calibrated bands for {schedule_key}: "
            f"scale={band_scale:.3f} center_offset={band_center_offset:+.2f}u"
        )
    if source_band_adjustments:
        print(
            f"[forecast] Source-level band calibration loaded for {schedule_key}: "
            f"{sorted(source_band_adjustments.keys())}"
        )
    preds = _train_and_predict(
        feat=feat,
        target_date=target_dt,
        days_until_next=target_days_until_next,
        active_promos=active_promos,
        band_scale=band_scale,
    )

    # Segment observability: surface cohorts that were previously masked in route-level aggregates.
    try:
        if not preds.empty:
            slow_mask = (
                (preds.get("model_branch", "") == "slow_intermittent")
                | (preds.get("is_slow_mover", 0).astype(float) >= 1.0)
            )
            stale14_mask = preds.get("days_since_last_order", 0).astype(float) >= 14.0
            stale21_mask = preds.get("days_since_last_order", 0).astype(float) >= 21.0
            high_removal_mask = preds.get("corr_removal_rate", 0).astype(float) >= 0.50

            def _seg(mask: pd.Series, label: str):
                subset = preds[mask] if label else preds
                count = int(len(subset))
                units = float(subset.get("pred_units", pd.Series(dtype=float)).sum()) if count else 0.0
                positive = int((subset.get("pred_units", pd.Series(dtype=float)) > 0).sum()) if count else 0
                return count, units, positive

            slow_count, slow_units, slow_positive = _seg(slow_mask, "slow")
            stale14_count, stale14_units, stale14_positive = _seg(stale14_mask, "stale14")
            stale21_count, stale21_units, stale21_positive = _seg(stale21_mask, "stale21")
            hi_rm_count, hi_rm_units, hi_rm_positive = _seg(high_removal_mask, "hi_rm")

            print(
                "[forecast] Segment snapshot: "
                f"slow(lines={slow_count}, units={slow_units:.1f}, positive={slow_positive}) "
                f"stale14(lines={stale14_count}, units={stale14_units:.1f}, positive={stale14_positive}) "
                f"stale21(lines={stale21_count}, units={stale21_units:.1f}, positive={stale21_positive}) "
                f"high_removal(lines={hi_rm_count}, units={hi_rm_units:.1f}, positive={hi_rm_positive})"
            )
    except Exception as e:
        print(f"[forecast] Warning: segment observability failed: {e}")

    # Allocate per SAP using total-case rounding + share distribution
    items: List[ForecastItem] = []
    suppressed_slow_movers: List[str] = []  # Track what we suppressed
    promo_history_hits = 0
    promo_history_misses = 0
    promo_non_promo = 0
    promo_miss_samples: List[str] = []
    uncertainty_safe_applied = 0
    uncertainty_safe_samples: List[str] = []
    branch_by_line: Dict[Tuple[str, str], str] = {}
    debug_saps = ["31032"]  # Debug specific items
    
    for sap in preds["sap"].unique():
        sap_preds = preds[preds["sap"] == sap]
        case_pack = _case_pack_for_sap(products, sap)
        sap_str = str(sap)
        
        if sap_str in debug_saps:
            print(f"[DEBUG] SAP {sap}: case_pack={case_pack}, products count={len(products)}")
        
        # === TIERED SUPPRESSION LOGIC ===
        # SLOW movers: Rule-based with home cycle assignment
        # MEDIUM/FAST movers: ML prediction (no suppression)
        
        tier_info = sap_tiers.get(sap_str)
        is_slow = bool(tier_info and tier_info.tier == 'SLOW')
        is_shared_case = bool(tier_info and tier_info.is_shared_case)
        
        if is_slow and is_shared_case:
            # Shared-case slow movers: route-level home cycle suppression (single restock event)
            home_cycle = tier_info.home_cycle if tier_info else None
            slow_suppression_days = _compute_slow_suppression_days(
                float(tier_info.order_frequency if tier_info else 0.25)
            )
            
            if home_cycle:
                if schedule_key != home_cycle:
                    cross_match = None
                    for (sid, s), cco in cross_cycle_orders.items():
                        if s == sap_str:
                            cross_match = cco
                            break
                    
                    if cross_match:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW shared, home={home_cycle}, ordered {cross_match.days_ago}d ago)"
                        )
                    else:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW shared, home={home_cycle}, current={schedule_key})"
                        )
                    continue
                else:
                    days_ago = same_cycle_recent.get(sap_str)
                    if days_ago is not None and days_ago < slow_suppression_days:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW shared, home={home_cycle}, same-cycle {days_ago}d ago, "
                            f"window={slow_suppression_days}d)"
                        )
                        continue
            else:
                cross_match = None
                for (sid, s), cco in cross_cycle_orders.items():
                    if s == sap_str:
                        cross_match = cco
                        break
                
                if cross_match:
                    suppressed_slow_movers.append(
                        f"{sap_str} (SLOW shared, no home, ordered {cross_match.days_ago}d ago in {cross_match.schedule_key})"
                    )
                    continue
                
                days_ago = same_cycle_recent.get(sap_str)
                if days_ago is not None and days_ago < slow_suppression_days:
                    suppressed_slow_movers.append(
                        f"{sap_str} (SLOW shared, no home, same-cycle {days_ago}d ago, "
                        f"window={slow_suppression_days}d)"
                    )
                    continue
        
        # MEDIUM and FAST movers: No suppression - ML handles them

        # Build active store predictions and matched names/ids
        store_pred_map: Dict[str, float] = {}
        store_p10_map: Dict[str, float] = {}
        store_p50_map: Dict[str, float] = {}
        store_p90_map: Dict[str, float] = {}
        store_name_map: Dict[str, str] = {}
        store_confidence_map: Dict[str, float] = {}
        for _, row in sap_preds.iterrows():
            historical_store_id = row.get("store_id") or ""
            historical_store_name = row.get("store") or ""
            matched_store = _match_store(stores_cfg, historical_store_id, historical_store_name)
            store_id = matched_store.store_id if matched_store else historical_store_id
            store_name = matched_store.store_name if matched_store else historical_store_name
            
            # Skip stores that don't receive deliveries on this day
            if store_id not in valid_store_ids:
                continue
            
            if not _store_active(stores_cfg, store_id, store_name, sap):
                continue

            # Store-specific slow-mover suppression (non-shared case)
            if is_slow and not is_shared_case:
                store_order_freq = float(row.get("order_frequency", 0.0) or 0.0)
                slow_suppression_days = _compute_slow_suppression_days(store_order_freq)
                store_home = store_sap_home_cycles.get((store_id, sap_str))
                if store_home:
                    home_cycle = store_home[0]
                    if schedule_key != home_cycle:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW store={store_id}, home={home_cycle}, current={schedule_key})"
                        )
                        continue
                    days_ago = same_cycle_recent_by_store.get((store_id, sap_str))
                    if days_ago is not None and days_ago < slow_suppression_days:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW store={store_id}, home={home_cycle}, same-cycle {days_ago}d ago, "
                            f"window={slow_suppression_days}d)"
                        )
                        continue
                else:
                    cross_match = cross_cycle_orders.get((store_id, sap_str))
                    if cross_match:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW store={store_id}, no home, ordered {cross_match.days_ago}d ago in {cross_match.schedule_key})"
                        )
                        continue
                    days_ago = same_cycle_recent_by_store.get((store_id, sap_str))
                    if days_ago is not None and days_ago < slow_suppression_days:
                        suppressed_slow_movers.append(
                            f"{sap_str} (SLOW store={store_id}, no home, same-cycle {days_ago}d ago, "
                            f"window={slow_suppression_days}d)"
                        )
                        continue

            # Prevent tiny low-signal slow-mover recommendations from being promoted to full cases.
            if is_slow and _should_zero_low_signal_slow_line(row, case_pack):
                suppressed_slow_movers.append(
                    f"{sap_str} (SLOW gate, store={store_id}, pred={float(row.get('pred_units', 0.0) or 0.0):.2f}, "
                    f"conf={float(row.get('confidence', 0.0) or 0.0):.2f}, "
                    f"removal={float(row.get('corr_removal_rate', 0.0) or 0.0):.2f})"
                )
                continue
            store_pred_map[store_id] = float(row["pred_units"])
            p50_units = float(row.get("p50_units", row.get("pred_units", 0.0)) or 0.0)
            store_p10_map[store_id] = float(row.get("p10_units", p50_units) or p50_units)
            store_p50_map[store_id] = p50_units
            store_p90_map[store_id] = float(row.get("p90_units", p50_units) or p50_units)
            store_name_map[store_id] = store_name
            store_confidence_map[store_id] = float(row.get("confidence", 0.0) or 0.0)
            branch_by_line[(store_id, sap_str)] = str(row.get("model_branch") or "gbr")

        if not store_pred_map:
            continue

        # Historical shares from PostgreSQL (falls back to equal shares)
        store_shares = get_historical_shares(
            route_number=config.route_number,
            sap=str(sap),
            schedule_key=schedule_key,
        )
        if not store_shares:
            equal_share = 1.0 / len(store_pred_map)
            store_shares = {sid: equal_share for sid in store_pred_map}

        if str(sap) in debug_saps:
            print(f"[DEBUG] SAP {sap}: round_cases={config.round_cases}, case_pack={case_pack}, will_allocate={config.round_cases and case_pack and case_pack > 0}")
        
        if config.round_cases and case_pack and case_pack > 0:
            allocations = allocate_cases_across_stores(
                sap=str(sap),
                store_predictions=store_pred_map,
                case_pack=case_pack,
                historical_shares=store_shares,
                route_number=config.route_number,
                schedule_key=schedule_key,
            )
            for sid, units_alloc in allocations.items():
                sname = store_name_map.get(sid, "")
                rec_cases = units_alloc / case_pack if case_pack else None
                # Check if this store/sap was ordered in a prior order cycle
                prior_ctx = prior_order_context.get((sid, str(sap)))
                # Check if item is on promo for this store and apply promo-history override
                is_promo = active_promos.get((sid, str(sap)), False)
                override_units = promo_history.get((sid, str(sap))) if is_promo else None
                model_conf = float(store_confidence_map.get(sid, 0.0) or 0.0)
                promo_history_conf_floor = float(os.environ.get("FORECAST_CONFIDENCE_PROMO_HISTORY_FLOOR", "0.70"))
                base_p10 = float(store_p10_map.get(sid, units_alloc) or 0.0)
                base_p50 = float(store_p50_map.get(sid, units_alloc) or 0.0)
                base_p90 = float(store_p90_map.get(sid, units_alloc) or 0.0)
                alloc_shift = float(units_alloc) - base_p50
                aligned_p10 = max(0.0, base_p10 + alloc_shift)
                aligned_p50 = float(units_alloc)
                aligned_p90 = max(aligned_p50, base_p90 + alloc_shift)

                final_units = float(override_units) if override_units is not None else float(units_alloc)
                if override_units is not None:
                    final_p10 = final_units
                    final_p50 = final_units
                    final_p90 = final_units
                else:
                    final_p10 = aligned_p10
                    final_p50 = aligned_p50
                    final_p90 = aligned_p90

                source = "promo_history" if override_units is not None else "baseline"
                final_confidence = max(model_conf, promo_history_conf_floor) if override_units is not None else model_conf
                last_order_units = last_order_qty_map.get((sid, str(sap)))
                safe_units, safe_p10, safe_p50, safe_p90, safe_applied = _apply_uncertainty_safe_policy(
                    recommended_units=final_units,
                    p10_units=final_p10,
                    p50_units=final_p50,
                    p90_units=final_p90,
                    confidence=final_confidence,
                    last_order_units=last_order_units,
                    case_pack=case_pack,
                )
                final_units = safe_units
                final_p10 = safe_p10
                final_p50 = safe_p50
                final_p90 = safe_p90
                if safe_applied:
                    uncertainty_safe_applied += 1
                    if len(uncertainty_safe_samples) < 8:
                        uncertainty_safe_samples.append(
                            f"store_id={sid} sap={sap} conf={final_confidence:.2f} last={last_order_units} adjusted={safe_units:.2f}"
                        )
                final_cases = final_units / case_pack if case_pack else None
                if is_promo:
                    if override_units is not None:
                        promo_history_hits += 1
                    else:
                        promo_history_misses += 1
                        if len(promo_miss_samples) < 8:
                            promo_miss_samples.append(
                                f"store_id={sid} sap={sap} source=baseline reason=promo_history_missing"
                            )
                else:
                    promo_non_promo += 1
                items.append(
                    ForecastItem(
                        store_id=sid,
                        store_name=sname,
                        sap=str(sap),
                        recommended_units=final_units,
                        recommended_cases=final_cases,
                        p10_units=final_p10,
                        p50_units=final_p50,
                        p90_units=final_p90,
                        confidence=final_confidence,
                        source=source,
                        prior_order_context=prior_ctx,
                        promo_active=is_promo,
                    )
                )
        else:
            for sid, units in store_pred_map.items():
                sname = store_name_map.get(sid, "")
                rec_cases = units / case_pack if case_pack else None
                # Check if this store/sap was ordered in a prior order cycle
                prior_ctx = prior_order_context.get((sid, str(sap)))
                # Check if item is on promo for this store
                is_promo = active_promos.get((sid, str(sap)), False)
                override_units = promo_history.get((sid, str(sap))) if is_promo else None
                model_conf = float(store_confidence_map.get(sid, 0.0) or 0.0)
                promo_history_conf_floor = float(os.environ.get("FORECAST_CONFIDENCE_PROMO_HISTORY_FLOOR", "0.70"))
                final_units = float(override_units) if override_units is not None else float(units)
                if override_units is not None:
                    final_p10 = final_units
                    final_p50 = final_units
                    final_p90 = final_units
                else:
                    final_p10 = float(store_p10_map.get(sid, final_units) or final_units)
                    final_p50 = float(store_p50_map.get(sid, final_units) or final_units)
                    final_p90 = float(store_p90_map.get(sid, final_units) or final_units)
                source = "promo_history" if override_units is not None else "baseline"
                final_confidence = max(model_conf, promo_history_conf_floor) if override_units is not None else model_conf
                last_order_units = last_order_qty_map.get((sid, str(sap)))
                safe_units, safe_p10, safe_p50, safe_p90, safe_applied = _apply_uncertainty_safe_policy(
                    recommended_units=final_units,
                    p10_units=final_p10,
                    p50_units=final_p50,
                    p90_units=final_p90,
                    confidence=final_confidence,
                    last_order_units=last_order_units,
                    case_pack=case_pack,
                )
                final_units = safe_units
                final_p10 = safe_p10
                final_p50 = safe_p50
                final_p90 = safe_p90
                if safe_applied:
                    uncertainty_safe_applied += 1
                    if len(uncertainty_safe_samples) < 8:
                        uncertainty_safe_samples.append(
                            f"store_id={sid} sap={sap} conf={final_confidence:.2f} last={last_order_units} adjusted={safe_units:.2f}"
                        )
                final_cases = final_units / case_pack if case_pack else None
                if is_promo:
                    if override_units is not None:
                        promo_history_hits += 1
                    else:
                        promo_history_misses += 1
                        if len(promo_miss_samples) < 8:
                            promo_miss_samples.append(
                                f"store_id={sid} sap={sap} source=baseline reason=promo_history_missing"
                            )
                else:
                    promo_non_promo += 1
                items.append(
                    ForecastItem(
                        store_id=sid,
                        store_name=sname,
                        sap=str(sap),
                        recommended_units=final_units,
                        recommended_cases=final_cases,
                        p10_units=final_p10,
                        p50_units=final_p50,
                        p90_units=final_p90,
                        confidence=final_confidence,
                        source=source,
                        prior_order_context=prior_ctx,
                        promo_active=is_promo,
                    )
                )

    # Log cross-cycle suppression results
    if suppressed_slow_movers:
        print(f"[forecast] Cross-cycle suppression: {len(suppressed_slow_movers)} slow-mover SAPs suppressed")
        for msg in suppressed_slow_movers[:10]:  # Show first 10
            print(f"  - {msg}")
        if len(suppressed_slow_movers) > 10:
            print(f"  ... and {len(suppressed_slow_movers) - 10} more")

    print(
        "[forecast] Promo decisions: "
        f"history_hit={promo_history_hits} "
        f"history_miss={promo_history_misses} "
        f"not_promo={promo_non_promo}"
    )
    if promo_miss_samples:
        print("[forecast] Promo history miss samples (first 8):")
        for msg in promo_miss_samples:
            print(f"  - {msg}")
    if uncertainty_safe_applied:
        print(f"[forecast] Uncertainty-safe policy applied: {uncertainty_safe_applied} line(s)")
        print("[forecast] Uncertainty-safe samples (first 8):")
        for msg in uncertainty_safe_samples:
            print(f"  - {msg}")

    # ==========================================================================
    # LOW QUANTITY HANDLING
    # ==========================================================================
    # Two-step process:
    # 1. SUPPRESSION: Zero out predictions for low-qty items on wrong order dates
    # 2. FLOOR INJECTION: Apply minimum (1 case) for low-qty items on correct date

    # Calculate order date from delivery date and schedule
    # schedule_key is 'monday', 'tuesday', etc. (the ORDER day)
    order_day_num = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }.get(schedule_key.lower(), 0)

    # Find the order date that corresponds to this delivery
    # Order date is before delivery date, on the schedule_key day
    days_back = (target_dt.weekday() - order_day_num) % 7
    if days_back == 0:
        days_back = 7  # If same day, go back a week
    order_date_dt = target_dt - timedelta(days=days_back)
    order_date_iso = order_date_dt.strftime('%Y-%m-%d')

    # ==========================================================================
    # STEP 1: LOW QUANTITY SUPPRESSION
    # ==========================================================================
    # Zero out predictions for low-qty items where today is NOT their order_by_date.
    # This prevents over-ordering low-qty items on the wrong day.
    try:
        suppress_saps = get_saps_to_suppress(
            db=db,
            route_number=config.route_number,
            order_date=order_date_iso,
        )
        
        if suppress_saps:
            suppressed_count = 0
            for item in items:
                if item.sap in suppress_saps:
                    item.recommended_units = 0.0
                    item.recommended_cases = 0.0
                    item.p10_units = 0.0
                    item.p50_units = 0.0
                    item.p90_units = 0.0
                    suppressed_count += 1
            if suppressed_count > 0:
                print(f"[forecast] Suppressed {suppressed_count} predictions for low-qty items (wrong order date)")
    except Exception as e:
        print(f"[forecast] Warning: Low-qty suppression failed: {e}")
        # Continue without suppression

    # ==========================================================================
    # STEP 2: LOW QUANTITY FLOOR INJECTION
    # ==========================================================================
    # Apply floor for items with single expiry date (low quantity = 1 case left)
    # This ensures we order replacement stock before the expiry is pulled.

    try:
        low_qty_items = get_items_for_order_date(
            db=db,
            route_number=config.route_number,
            order_date=order_date_iso,
        )
        low_qty_confidence_floor = float(os.environ.get("FORECAST_CONFIDENCE_LOW_QTY_FLOOR", "0.90"))

        if low_qty_items:
            print(f"[forecast] Processing {len(low_qty_items)} low-qty items for order date {order_date_iso}")

            # Build lookup of existing predictions: (store_id, sap) -> index in items list
            existing_items: Dict[Tuple[str, str], int] = {}
            for idx, item in enumerate(items):
                existing_items[(item.store_id, item.sap)] = idx

            # Track store name lookup from stores_cfg
            store_name_lookup = {s.store_id: s.store_name for s in stores_cfg}

            for lq_item in low_qty_items:
                # Get stores that stock this SAP
                store_ids = get_stores_for_sap(config.route_number, lq_item.sap)

                if not store_ids:
                    continue  # Warning already logged by get_stores_for_sap

                # Get case pack for floor quantity
                case_pack = _case_pack_for_sap(products, lq_item.sap)
                floor_units = case_pack if case_pack and case_pack > 0 else 1

                for store_id in store_ids:
                    # Skip stores not in this order cycle
                    if store_id not in valid_store_ids:
                        continue
                    
                    # Skip stores that don't actually stock this SAP (validate against Firebase config)
                    store_name = store_name_lookup.get(store_id, '')
                    if not _store_active(stores_cfg, store_id, store_name, lq_item.sap):
                        continue

                    key = (store_id, lq_item.sap)
                    expiry_info = ExpiryReplacementInfo(
                        expiry_date=lq_item.expiry_date,
                        min_units_required=floor_units,
                        reason="low_qty_expiry",
                    )

                    if key in existing_items:
                        # Apply floor to existing prediction
                        idx = existing_items[key]
                        if items[idx].recommended_units < floor_units:
                            items[idx].recommended_units = float(floor_units)
                            items[idx].recommended_cases = floor_units / case_pack if case_pack else None
                            items[idx].p10_units = float(floor_units)
                            items[idx].p50_units = float(floor_units)
                            items[idx].p90_units = float(floor_units)
                            items[idx].expiry_replacement = expiry_info
                            items[idx].confidence = max(float(items[idx].confidence or 0.0), low_qty_confidence_floor)
                            print(f"[forecast] Applied floor to {lq_item.sap} @ {store_id}: {floor_units} units (expires {lq_item.expiry_date})")
                        else:
                            # ML already predicted enough, just attach metadata
                            items[idx].expiry_replacement = expiry_info
                            items[idx].confidence = max(float(items[idx].confidence or 0.0), low_qty_confidence_floor)
                    else:
                        # No prediction exists - add new item
                        store_name = store_name_lookup.get(store_id, '')
                        items.append(ForecastItem(
                            store_id=store_id,
                            store_name=store_name,
                            sap=lq_item.sap,
                            recommended_units=float(floor_units),
                            recommended_cases=floor_units / case_pack if case_pack else None,
                            p10_units=float(floor_units),
                            p50_units=float(floor_units),
                            p90_units=float(floor_units),
                            source="expiry_replacement",
                            expiry_replacement=expiry_info,
                            confidence=low_qty_confidence_floor,
                            promo_active=active_promos.get(key, False),
                        ))
                        print(f"[forecast] Added {lq_item.sap} @ {store_id}: {floor_units} units (expiry replacement, expires {lq_item.expiry_date})")

            print(f"[forecast] Low-qty floor injection complete")
    except Exception as e:
        print(f"[forecast] Warning: Low-qty floor injection failed: {e}")
        # Continue without low-qty injection - forecast still valid

    # ==========================================================================
    # VISIBILITY FILTER (NO GHOST LINE ITEMS)
    # ==========================================================================
    # Prevent emitting forecast lines that the user cannot see/edit in app/web.
    # Concrete rule: every (store_id, sap) must be active per store carry list AND exist in active catalog.
    try:
        valid_saps = {p.sap for p in products if getattr(p, "sap", None)}
        filtered: List[ForecastItem] = []
        drops: List[Tuple[str, str, str]] = []  # (store_id, sap, reason)

        for it in items:
            sid = str(getattr(it, "store_id", "") or "")
            sname = str(getattr(it, "store_name", "") or "")
            sap = str(getattr(it, "sap", "") or "")

            if not sid or not sap:
                drops.append((sid, sap, "missing_ids"))
                continue

            if sid not in valid_store_ids:
                drops.append((sid, sap, "store_not_in_cycle"))
                continue

            if sap not in valid_saps:
                drops.append((sid, sap, "missing_product"))
                continue

            ok, reason = _store_active_with_reason(stores_cfg, sid, sname, sap)
            if not ok:
                drops.append((sid, sap, reason or "inactive_store_item"))
                continue

            filtered.append(it)

        if drops:
            # Summarize by reason, and print a deterministic sample for debugging.
            reason_counts: Dict[str, int] = {}
            for _, _, r in drops:
                reason_counts[r] = reason_counts.get(r, 0) + 1
            parts = ", ".join([f"{k}={reason_counts[k]}" for k in sorted(reason_counts.keys())])
            print(f"[forecast] Visibility filter dropped {len(drops)} item(s): {parts}")
            for sid, sap, reason in drops[:25]:
                print(
                    f"[forecast] Dropped invisible item: route={config.route_number} "
                    f"delivery={delivery_iso} schedule={schedule_key} store_id={sid} sap={sap} reason={reason}"
                )

        items = filtered
    except Exception as e:
        print(f"[forecast] Warning: Visibility filter failed: {e}")
        # Continue - do not block forecast generation on logging/filtering issues

    # ==========================================================================
    # WHOLE-CASE ENFORCEMENT (ORDER-LEVEL)
    # ==========================================================================
    # Invariant: each SAP must be ordered in full cases at the ORDER level
    # (sum across stores divisible by case_pack).
    #
    # We do this AFTER all post-processing (promo history overrides, low-qty floors,
    # suppression) because those steps can break the case-allocation invariant that
    # allocate_cases_across_stores tries to guarantee.
    try:
        from collections import defaultdict

        by_sap = defaultdict(list)
        for it in items:
            if getattr(it, "sap", None):
                by_sap[str(it.sap)].append(it)

        adjusted = 0
        for sap, sap_items in by_sap.items():
            case_pack = _case_pack_for_sap(products, sap)
            if not case_pack or case_pack <= 0:
                continue

            # Sum current units as ints (recommended_units should be integer-ish already).
            total_units = 0
            for it in sap_items:
                u = getattr(it, "recommended_units", 0.0) or 0.0
                try:
                    total_units += int(round(float(u)))
                except Exception:
                    continue

            if total_units <= 0:
                continue

            remainder = total_units % int(case_pack)
            if remainder == 0:
                continue

            # Round UP to the next full case (never under-forecast).
            add_units = int(case_pack) - remainder
            if add_units <= 0:
                continue

            trigger = "allocation_remainder"
            try:
                if any(getattr(it, "source", "") == "promo_history" for it in sap_items):
                    trigger = "promo_history"
                elif any(
                    getattr(it, "source", "") == "expiry_replacement" or getattr(it, "expiry_replacement", None) is not None
                    for it in sap_items
                ):
                    trigger = "low_qty_expiry"
            except Exception:
                trigger = "allocation_remainder"

            # Prefer to add to a baseline (non-override) store so we don't stomp promo-history
            # or explicit expiry-replacement injections.
            candidates = [
                it for it in sap_items
                if getattr(it, "source", "") not in ("promo_history", "expiry_replacement")
            ]
            if not candidates:
                candidates = sap_items

            target = max(candidates, key=lambda it: float(getattr(it, "recommended_units", 0.0) or 0.0))
            pre_units = int(round(float(getattr(target, "recommended_units", 0.0) or 0.0)))
            try:
                target.whole_case_adjustment = {
                    "adjusted_units": int(add_units),
                    "pre_enforcement_units": int(pre_units),
                    "trigger": trigger,
                    "target_store_id": str(getattr(target, "store_id", "") or ""),
                }
            except Exception:
                # Metadata is best-effort; invariant gate below is the real guardrail.
                pass
            try:
                # Adjustment introduces allocator-side uncertainty.
                target.confidence = max(0.05, float(getattr(target, "confidence", 0.5) or 0.5) - 0.03)
            except Exception:
                pass
            target.recommended_units = float((getattr(target, "recommended_units", 0.0) or 0.0) + add_units)
            target.recommended_cases = float(target.recommended_units) / float(case_pack)
            try:
                p10 = float(getattr(target, "p10_units", target.recommended_units) or target.recommended_units)
                p50 = float(getattr(target, "p50_units", target.recommended_units) or target.recommended_units)
                p90 = float(getattr(target, "p90_units", target.recommended_units) or target.recommended_units)
                shift = float(target.recommended_units) - p50
                target.p10_units = max(0.0, p10 + shift)
                target.p50_units = float(target.recommended_units)
                target.p90_units = max(float(target.p50_units), p90 + shift)
            except Exception:
                pass
            adjusted += 1

            print(
                f"[forecast] Whole-case adjustment for SAP {sap}: "
                f"total={total_units} case_pack={case_pack} remainder={remainder} "
                f"-> +{add_units} units @ {getattr(target, 'store_id', '')}"
            )

        if adjusted:
            print(f"[forecast] Whole-case enforcement complete: adjusted {adjusted} SAP(s)")
    except Exception as e:
        print(f"[forecast] Warning: Whole-case enforcement failed: {e}")
        # Continue - forecast still usable

    # ==========================================================================
    # WHOLE-CASE INVARIANT GATE (HARD FAIL: DO NOT EMIT FORECAST)
    # ==========================================================================
    from collections import defaultdict

    grouped_by_sap = defaultdict(list)
    for it in items:
        if getattr(it, "sap", None):
            grouped_by_sap[str(it.sap)].append(it)

    violations: List[Tuple[str, int, int]] = []  # (sap, total_units, case_pack)
    for sap, sap_items in grouped_by_sap.items():
        case_pack = _case_pack_for_sap(products, sap)
        if not case_pack or int(case_pack) <= 0:
            continue
        total_units = 0
        for it in sap_items:
            u = getattr(it, "recommended_units", 0.0) or 0.0
            try:
                total_units += int(round(float(u)))
            except Exception:
                continue
        if total_units <= 0:
            continue
        if total_units % int(case_pack) != 0:
            violations.append((sap, total_units, int(case_pack)))

    if violations:
        # Log a deterministic sample for debugging, then hard-fail.
        for sap, total, case_pack in violations[:25]:
            print(
                f"[forecast] ERROR: Whole-case invariant violated: "
                f"route={config.route_number} delivery={delivery_iso} schedule={schedule_key} "
                f"sap={sap} total={total} case_pack={case_pack}"
            )
        raise RuntimeError(
            f"whole_case_invariant_violated: route={config.route_number} "
            f"delivery={delivery_iso} schedule={schedule_key} violations={len(violations)}"
        )

    # Apply schedule + source/branch uncertainty calibration to bands (not quantities).
    # Keep recommended_units unchanged; this only adjusts interval width/placement.
    if abs(float(band_center_offset or 0.0)) > 1e-9 or source_band_adjustments:
        adjusted = 0
        for it in items:
            source = str(getattr(it, "source", "") or "")
            if source not in ("baseline", "last_order"):
                continue
            line_key = (str(getattr(it, "store_id", "") or ""), str(getattr(it, "sap", "") or ""))
            source_key = "last_order" if source == "last_order" else branch_by_line.get(line_key, "gbr")
            src_scale, src_center = source_band_adjustments.get(source_key, (1.0, 0.0))
            total_center = float(band_center_offset or 0.0) + float(src_center or 0.0)

            p10 = float(getattr(it, "p10_units", 0.0) or 0.0)
            p50 = float(getattr(it, "p50_units", 0.0) or 0.0)
            p90 = float(getattr(it, "p90_units", 0.0) or 0.0)
            n10, n50, n90 = _apply_band_scale_and_center(
                p10_units=p10,
                p50_units=p50,
                p90_units=p90,
                scale_mult=src_scale,
                center_offset_units=total_center,
            )
            it.p10_units = n10
            it.p50_units = n50
            it.p90_units = n90
            adjusted += 1
        if adjusted:
            print(
                f"[forecast] Applied schedule/source band calibration to {adjusted} line(s); "
                f"schedule_center={band_center_offset:+.2f}u"
            )

    # Confidence distribution for observability
    try:
        bucket_counts = {"high": 0, "medium": 0, "low": 0}
        for it in items:
            b = _confidence_bucket(getattr(it, "confidence", None))
            bucket_counts[b] = bucket_counts.get(b, 0) + 1
        print(
            f"[forecast] Confidence distribution: "
            f"high={bucket_counts.get('high', 0)} "
            f"medium={bucket_counts.get('medium', 0)} "
            f"low={bucket_counts.get('low', 0)}"
        )
    except Exception:
        pass

    forecast = ForecastPayload(
        forecast_id=str(uuid.uuid4()),
        route_number=config.route_number,
        delivery_date=delivery_iso,
        schedule_key=schedule_key,
        generated_at=datetime.utcnow(),
        items=items,
    )

    # Write to Firestore cache
    write_cached_forecast(
        db=db,
        route_number=config.route_number,
        forecast=forecast,
        ttl_days=config.ttl_days,
    )
    try:
        write_forecast_transfer_suggestions(
            db=db,
            route_number=config.route_number,
            forecast=forecast,
        )
    except Exception as e:
        print(f"[forecast] Warning: transfer suggestion generation failed: {e}")

    return forecast

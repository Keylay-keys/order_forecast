"""Forecast engine: orchestrates loading data, running model, applying business logic, and emitting forecasts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple
import sys
import uuid

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor


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

MIN_ORDERS_FOR_FORECAST = 4  # Minimum orders needed to generate a forecast
MIN_ORDERS_FOR_TRAINING = 8  # Minimum orders needed to train a model (more stringent)

# Support running as a script without package context
try:
    from .models import ForecastItem, ForecastPayload, Product, StoreConfig, PriorOrderContext, ExpiryReplacementInfo
    from .firebase_loader import (
        get_firestore_client,
        load_master_catalog,
        load_store_configs,
        load_orders,
        load_promotions,
    )
    from .firebase_writer import write_cached_forecast
    from .schedule_utils import normalize_delivery_date, weekday_key, get_order_day_for_delivery, get_days_until_next_delivery, get_schedule_info
    from .case_allocator import allocate_cases_across_stores, get_historical_shares, get_item_case_pattern, set_db_client
    from .low_quantity_loader import get_items_for_order_date, get_stores_for_sap, get_saps_to_suppress
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from models import ForecastItem, ForecastPayload, Product, StoreConfig, PriorOrderContext, ExpiryReplacementInfo
    from firebase_loader import (
        get_firestore_client,
        load_master_catalog,
        load_store_configs,
        load_orders,
        load_promotions,
    )
    from firebase_writer import write_cached_forecast
    from schedule_utils import normalize_delivery_date, weekday_key, get_order_day_for_delivery, get_days_until_next_delivery, get_schedule_info
    from case_allocator import allocate_cases_across_stores, get_historical_shares, get_item_case_pattern, set_db_client
    from low_quantity_loader import get_items_for_order_date, get_stores_for_sap, get_saps_to_suppress


def _build_active_promo_lookup(
    db,
    route_number: str,
    delivery_date: str,
    stores_cfg: List,
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
        
        if items:
            # New format: per-item with account and dates
            for item in items:
                sap = str(item.get('sap_code') or item.get('sap') or '')
                if not sap:
                    continue
                
                # Get per-item dates, fallback to promo-level dates
                item_start = item.get('start_date') or promo_start
                item_end = item.get('end_date') or promo_end
                
                # Check if delivery date is within promo period
                is_active = True
                if item_start:
                    try:
                        start_dt = datetime.strptime(item_start, "%Y-%m-%d").date()
                        if delivery_dt < start_dt:
                            is_active = False
                    except ValueError:
                        pass
                if item_end:
                    try:
                        end_dt = datetime.strptime(item_end, "%Y-%m-%d").date()
                        if delivery_dt > end_dt:
                            is_active = False
                    except ValueError:
                        pass
                
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
            is_active = True
            if promo_start:
                try:
                    start_dt = datetime.strptime(promo_start, "%Y-%m-%d").date()
                    if delivery_dt < start_dt:
                        is_active = False
                except ValueError:
                    pass
            if promo_end:
                try:
                    end_dt = datetime.strptime(promo_end, "%Y-%m-%d").date()
                    if delivery_dt > end_dt:
                        is_active = False
                except ValueError:
                    pass
            
            if is_active:
                for sap in affected_saps:
                    sap_str = str(sap)
                    for store in stores_cfg:
                        active_promos[(store.store_id, sap_str)] = True
    
    return active_promos


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
    orders_ref = db.collection('orders')
    
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
        query = orders_ref.where('routeNumber', '==', route_number).where('status', '==', 'finalized')
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
class ForecastConfig:
    route_number: str
    delivery_date: str  # ISO or MM/DD/YYYY
    schedule_key: str  # monday / thursday etc
    service_account: Optional[str] = None
    since_days: int = 365
    round_cases: bool = True
    ttl_days: int = 7


def _load_corrections_from_duckdb(db_client, route_number: str, schedule_key: str) -> Optional[pd.DataFrame]:
    """Load aggregated corrections from DuckDB for ML features.
    
    Queries the forecast_corrections table and aggregates by store/sap/schedule
    to compute correction features like avg_delta, avg_ratio, removal_rate.
    
    Args:
        db_client: DBClient instance for DuckDB queries
        route_number: Route to load corrections for
        schedule_key: Order schedule (e.g., 'monday', 'tuesday')
    
    Returns:
        DataFrame with columns: store_id, sap, schedule_key, samples, avg_delta, 
        avg_ratio, ratio_stddev, removal_rate, promo_rate
        Returns None if db_client is unavailable or query fails.
    """
    if db_client is None:
        return None
    
    try:
        # Aggregate corrections by store/sap/schedule
        # Only use non-holiday corrections for training features
        result = db_client.query("""
            SELECT 
                store_id,
                sap,
                schedule_key,
                COUNT(*) as samples,
                AVG(correction_delta) as avg_delta,
                AVG(correction_ratio) as avg_ratio,
                STDDEV(correction_ratio) as ratio_stddev,
                SUM(CASE WHEN was_removed THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as removal_rate,
                SUM(CASE WHEN promo_active THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as promo_rate
            FROM forecast_corrections
            WHERE route_number = ?
              AND schedule_key = ?
              AND is_holiday_week = FALSE
            GROUP BY store_id, sap, schedule_key
            HAVING COUNT(*) >= 1
        """, [route_number, schedule_key])
        
        if 'error' in result:
            print(f"[forecast] Warning: Corrections query failed: {result['error']}")
            return None
        
        rows = result.get('rows', [])
        if not rows:
            print(f"[forecast] No corrections found for route {route_number}, schedule {schedule_key}")
            return None
        
        df = pd.DataFrame(rows)
        print(f"[forecast] Loaded {len(df)} correction aggregates from DuckDB")
        return df
        
    except Exception as e:
        print(f"[forecast] Warning: Could not load corrections from DuckDB: {e}")
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
    matched_store = _match_store(store_cfgs, store_id, store_name)
    if matched_store:
        return sap in matched_store.active_saps if matched_store.active_saps else True
    return True  # Default to active if store not found


def _filter_orders_by_schedule(orders, schedule_key: str):
    return [o for o in orders if o.schedule_key == schedule_key]


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
                rows.append({
                    "delivery_date": dt,
                    "store": store.store_name,
                    "store_id": store.store_id,
                    "sap": item.sap,
                    "units": item.quantity,
                    "cases": item.cases,
                })
    df = pd.DataFrame(rows)
    return df


def _build_features(df: pd.DataFrame, corrections_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    # Ensure sorted by date
    df = df.sort_values("delivery_date").copy()
    df["delivery_dow"] = df["delivery_date"].dt.weekday
    df["delivery_month"] = df["delivery_date"].dt.month
    df["is_monday_delivery"] = (df["delivery_dow"] == 0).astype(int)

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
        g["lag_1"] = g["units"].shift(1)
        g["lag_2"] = g["units"].shift(2)
        g["rolling_mean_4"] = g["units"].rolling(window=4, min_periods=1).mean().shift(1)
        out_frames.append(g)
    feat = pd.concat(out_frames, ignore_index=True)

    # Fill lag defaults
    feat["lag_1"] = feat["lag_1"].fillna(0.0)
    feat["lag_2"] = feat["lag_2"].fillna(0.0)
    feat["rolling_mean_4"] = feat["rolling_mean_4"].fillna(feat["lag_1"])
    
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
    # Note: corrections are already filtered by schedule_key in _load_corrections_from_duckdb,
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
        ]:
            feat[col] = 0.0

    return feat


def _train_and_predict(feat: pd.DataFrame, target_date: datetime, days_until_next: int = 4) -> pd.DataFrame:
    """Train GBR on historical rows and predict next for each store/sap using last known lags.
    
    Args:
        feat: Feature dataframe with historical data
        target_date: Date to predict for
        days_until_next: Days until next delivery (from user's schedule)
    """
    if feat.empty:
        return pd.DataFrame(columns=["store", "store_id", "sap", "pred_units"])

    feat = feat.sort_values("delivery_date")
    feature_cols = [
        # Lag features
        "lag_1",
        "lag_2",
        "rolling_mean_4",
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
        # User correction features
        "corr_samples",
        "corr_avg_delta",
        "corr_avg_ratio",
        "corr_ratio_stddev",
        "corr_removal_rate",
        "corr_promo_rate",
    ]

    # Training data: all rows with lag_1 available
    train_df = feat.dropna(subset=["lag_1"]).copy()
    if train_df.empty:
        return pd.DataFrame(columns=["store", "store_id", "sap", "pred_units"])

    X_train = train_df[feature_cols]
    y_train = train_df["units"]
    model = GradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Build inference rows: last observation per store/sap, reuse latest lags
    preds = []
    
    # Compute schedule-aware features for target date using actual schedule
    target_schedule_feats = _compute_schedule_features(target_date, days_until_next)
    
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
        
        # NEW: Schedule-aware features for target date
        row["days_until_first_weekend"] = target_schedule_feats["days_until_first_weekend"]
        row["covers_first_weekend"] = target_schedule_feats["covers_first_weekend"]
        row["covers_weekend"] = target_schedule_feats["covers_weekend"]
        row["days_until_next_delivery"] = target_schedule_feats["days_until_next_delivery"]
        
        pred_units = float(model.predict(row.values.reshape(1, -1))[0])
        preds.append(
            {
                "store": last["store"],
                "store_id": store_id,
                "sap": str(sap),
                "pred_units": max(0.0, pred_units),
            }
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


def _get_stores_for_order_cycle(stores_cfg: List[StoreConfig], schedule_key: str) -> set:
    """Return set of store IDs that are part of an order cycle.
    
    This returns ALL stores that receive product from a given order day,
    regardless of their specific delivery day. This is crucial for proper
    case allocation - we need to consider ALL stores when splitting cases.
    
    Example:
        - Tuesday order: Walmart/Harmon's/Sam's/Smith's (Monday delivery) + Bowman's (Friday delivery)
        - Monday order: Walmart/Harmon's/Sam's/Smith's (Thursday delivery) - no Bowman's
    
    Args:
        stores_cfg: List of store configurations
        schedule_key: Order day key ('monday', 'tuesday', etc.)
        
    Returns:
        Set of store IDs in this order cycle
    """
    # Define which delivery days are part of each order cycle
    # This maps order_day -> list of delivery days that are fulfilled from that order
    ORDER_CYCLE_DELIVERY_DAYS = {
        'monday': ['thursday'],           # Monday order → Thursday delivery
        'tuesday': ['monday', 'friday'],  # Tuesday order → Monday delivery (most stores) + Friday (Bowman's)
    }
    
    delivery_days = ORDER_CYCLE_DELIVERY_DAYS.get(schedule_key.lower(), [])
    
    if not delivery_days:
        # Fallback: include all active stores
        return {s.store_id for s in stores_cfg if s.is_active}
    
    valid_stores = set()
    for store in stores_cfg:
        store_delivery_days = [d.lower() for d in (store.delivery_days or [])]
        # Include store if ANY of its delivery days match the order cycle
        for delivery_day in delivery_days:
            if delivery_day in store_delivery_days:
                valid_stores.add(store.store_id)
                break
    
    return valid_stores


def generate_forecast(config: ForecastConfig) -> ForecastPayload:
    db = get_firestore_client(config.service_account)
    
    # Initialize DBClient for case_allocator queries and local DB lookups
    db_client = None
    try:
        from db_client import DBClient
        db_client = DBClient(db, timeout=30)
        set_db_client(db_client)
    except Exception as e:
        print(f"[forecast] Warning: Could not initialize DBClient: {e}")
    
    delivery_iso = normalize_delivery_date(config.delivery_date)
    # schedule_key is based on ORDER day, not delivery day
    # e.g., Thursday delivery → 'monday' (ordered Monday)
    schedule_key = get_order_day_for_delivery(db, config.route_number, delivery_iso)
    target_dt = _parse_date(delivery_iso) or datetime.utcnow()
    
    # Get the delivery weekday name (e.g., 'Thursday', 'Friday')
    WEEKDAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    delivery_weekday = WEEKDAY_NAMES[target_dt.weekday()]

    # Load data
    products = load_master_catalog(db, config.route_number)
    stores_cfg = load_store_configs(db, config.route_number)
    
    # Check for prior orders from other schedules that might overlap (e.g., both deliver to Bowman's)
    prior_order_context = _get_prior_order_context(
        db=db,
        route_number=config.route_number,
        current_schedule_key=schedule_key,
        target_delivery_date=delivery_iso,
        stores_cfg=stores_cfg,
    )
    
    # Build active promo lookup for this delivery date
    active_promos = _build_active_promo_lookup(
        db=db,
        route_number=config.route_number,
        delivery_date=delivery_iso,
        stores_cfg=stores_cfg,
    )
    if active_promos:
        print(f"[forecast] Active promos: {len(active_promos)} store/sap combinations on promo for {delivery_iso}")
    
    # IMPORTANT: Include ALL stores in this order cycle, not just those delivering on this specific day.
    # Example: Tuesday orders include stores delivering Monday AND Bowman's delivering Friday.
    # The case allocation needs to consider ALL stores to properly distribute cases.
    # We use the schedule_key to determine which stores are part of this order cycle.
    valid_store_ids = _get_stores_for_order_cycle(stores_cfg, schedule_key)
    print(f"[forecast] Order cycle: {schedule_key} (primary delivery: {delivery_iso} {delivery_weekday})")
    print(f"[forecast] All stores in {schedule_key} order: {[s.store_name for s in stores_cfg if s.store_id in valid_store_ids]}")
    
    # SCHEDULE KEY MIGRATION: Old orders used delivery-day as schedule_key
    # New orders use order-day. Map both to handle mixed data:
    # - "tuesday" orders deliver on Monday, old key was "monday"
    # - "monday" orders deliver on Thursday, old key was "thursday"
    legacy_schedule_map = {
        'tuesday': ['tuesday', 'monday'],    # Tuesday orders had old key "monday" (delivery day)
        'monday': ['monday', 'thursday'],    # Monday orders had old key "thursday" (delivery day)
    }
    schedule_keys_to_load = legacy_schedule_map.get(schedule_key, [schedule_key])
    
    orders = load_orders(
        db,
        route_number=config.route_number,
        since_days=config.since_days,
        schedule_keys=schedule_keys_to_load,
    )
    # Filter to just the ones that match our target schedule OR the legacy key
    orders = [o for o in orders if o.schedule_key in schedule_keys_to_load]

    # Check minimum data threshold
    if len(orders) < MIN_ORDERS_FOR_FORECAST:
        raise ValueError(
            f"insufficient_history: Only {len(orders)} orders found, "
            f"need at least {MIN_ORDERS_FOR_FORECAST} for forecasting"
        )

    # Build dataframe
    df_orders = _orders_to_dataframe(orders)

    # Load corrections from DuckDB (replaces CSV-based approach)
    corrections_df = _load_corrections_from_duckdb(db_client, config.route_number, schedule_key)

    # Get user's schedule info for schedule-aware features
    # Uses DuckDB (local) via db_client, falls back to Firebase if needed
    schedule_info = get_schedule_info(db, config.route_number, db_client=db_client)
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
    preds = _train_and_predict(feat, target_dt, target_days_until_next)

    # Allocate per SAP using total-case rounding + share distribution
    items: List[ForecastItem] = []
    debug_saps = ["31032"]  # Debug specific items
    for sap in preds["sap"].unique():
        sap_preds = preds[preds["sap"] == sap]
        case_pack = _case_pack_for_sap(products, sap)
        
        if str(sap) in debug_saps:
            print(f"[DEBUG] SAP {sap}: case_pack={case_pack}, products count={len(products)}")

        # Build active store predictions and matched names/ids
        store_pred_map: Dict[str, float] = {}
        store_name_map: Dict[str, str] = {}
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
            store_pred_map[store_id] = float(row["pred_units"])
            store_name_map[store_id] = store_name

        if not store_pred_map:
            continue

        # Historical shares from DuckDB (falls back to equal shares)
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
                # Check if item is on promo for this store
                is_promo = active_promos.get((sid, str(sap)), False)
                items.append(
                    ForecastItem(
                        store_id=sid,
                        store_name=sname,
                        sap=str(sap),
                        recommended_units=units_alloc,
                        recommended_cases=rec_cases,
                        source="baseline",
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
                items.append(
                    ForecastItem(
                        store_id=sid,
                        store_name=sname,
                        sap=str(sap),
                        recommended_units=units,
                        recommended_cases=rec_cases,
                        source="baseline",
                        prior_order_context=prior_ctx,
                        promo_active=is_promo,
                    )
                )

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
            db_client=db_client,
        )
        
        if suppress_saps:
            suppressed_count = 0
            for item in items:
                if item.sap in suppress_saps:
                    item.recommended_units = 0.0
                    item.recommended_cases = 0.0
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
            db_client=db_client,
        )

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
                store_ids = get_stores_for_sap(db_client, config.route_number, lq_item.sap)

                if not store_ids:
                    continue  # Warning already logged by get_stores_for_sap

                # Get case pack for floor quantity
                case_pack = _case_pack_for_sap(products, lq_item.sap)
                floor_units = case_pack if case_pack and case_pack > 0 else 1

                for store_id in store_ids:
                    # Skip stores not in this order cycle
                    if store_id not in valid_store_ids:
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
                            items[idx].expiry_replacement = expiry_info
                            print(f"[forecast] Applied floor to {lq_item.sap} @ {store_id}: {floor_units} units (expires {lq_item.expiry_date})")
                        else:
                            # ML already predicted enough, just attach metadata
                            items[idx].expiry_replacement = expiry_info
                    else:
                        # No prediction exists - add new item
                        store_name = store_name_lookup.get(store_id, '')
                        items.append(ForecastItem(
                            store_id=store_id,
                            store_name=store_name,
                            sap=lq_item.sap,
                            recommended_units=float(floor_units),
                            recommended_cases=floor_units / case_pack if case_pack else None,
                            source="expiry_replacement",
                            expiry_replacement=expiry_info,
                            promo_active=active_promos.get(key, False),
                        ))
                        print(f"[forecast] Added {lq_item.sap} @ {store_id}: {floor_units} units (expiry replacement, expires {lq_item.expiry_date})")

            print(f"[forecast] Low-qty floor injection complete")
    except Exception as e:
        print(f"[forecast] Warning: Low-qty floor injection failed: {e}")
        # Continue without low-qty injection - forecast still valid

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

    return forecast

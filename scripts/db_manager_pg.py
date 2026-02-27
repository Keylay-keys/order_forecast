#!/usr/bin/env python3
"""PostgreSQL utility functions for order sync operations.

⚠️  SERVICE DEPRECATED: The DB Manager service (listener loop) has been removed.
    All services now use direct PostgreSQL connections via pg_utils.

This module is KEPT as a utility library for functions like handle_sync_order()
which are still used by order_sync_listener.py and api/routers/orders.py.

Active utility functions:
- handle_sync_order(): Syncs an order from Firebase to PostgreSQL (used by listeners/API)
- handle_query(): Execute a read query
- handle_write(): Execute a write operation
- handle_get_historical_shares(): Get case allocation shares
- handle_get_archived_dates(): Get list of archived order dates
- handle_get_order(): Get a specific archived order
- handle_check_route_synced(): Check if a route is synced
- handle_get_delivery_manifest(): Get delivery manifest for a date

For new code, prefer using pg_utils.py directly:
    from pg_utils import fetch_all, fetch_one, execute

The main() function and run_listener() are no longer used but kept for reference.

Environment Variables:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from google.cloud import firestore

# Global flag for graceful shutdown
_shutdown_requested = False

# Worker ID for this instance
WORKER_ID = f"dbmgr-pg-{socket.gethostname()}-{os.getpid()}"

# Request timeout (seconds)
REQUEST_TIMEOUT = 60

# =============================================================================
# HOLIDAY WEEK DETECTION
# =============================================================================

HOLIDAY_WEEKS = [
    ('2024-11-25', '2024-12-01', 'Thanksgiving'),
    ('2024-12-23', '2024-12-29', 'Christmas'),
    ('2025-11-24', '2025-11-30', 'Thanksgiving'),
    ('2025-12-22', '2025-12-28', 'Christmas'),
    ('2026-11-23', '2026-11-29', 'Thanksgiving'),
    ('2026-12-21', '2026-12-27', 'Christmas'),
]


def is_holiday_week(order_date_str: str) -> tuple:
    """Check if an order date falls within a holiday week."""
    if not order_date_str:
        return False, ''
    
    try:
        if '/' in order_date_str:
            dt = datetime.strptime(order_date_str, '%m/%d/%Y')
        else:
            dt = datetime.fromisoformat(order_date_str.replace('Z', '+00:00').split('T')[0])
        
        date_str = dt.strftime('%Y-%m-%d')
        
        for start, end, name in HOLIDAY_WEEKS:
            if start <= date_str <= end:
                return True, name
        
        return False, ''
    except (ValueError, TypeError):
        return False, ''


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


def get_pg_conn(host: str, port: int, database: str, user: str, password: str) -> psycopg2.extensions.connection:
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


# =============================================================================
# Request Handlers
# =============================================================================

def handle_query(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Execute a read query and return results."""
    sql = payload.get('sql', '')
    params = payload.get('params', [])
    
    if not sql:
        return {'error': 'No SQL provided'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Convert ? placeholders to %s for PostgreSQL
        pg_sql = sql.replace('?', '%s')
        
        cur.execute(pg_sql, params if params else None)
        
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            rows = []
            for row in cur.fetchall():
                row_dict = dict(row)
                # Handle special types
                for k, v in row_dict.items():
                    if hasattr(v, 'isoformat'):
                        row_dict[k] = v.isoformat()
                    elif isinstance(v, bytes):
                        row_dict[k] = v.decode('utf-8', errors='replace')
                rows.append(row_dict)
            
            cur.close()
            return {
                'columns': columns,
                'rows': rows,
                'row_count': len(rows),
            }
        else:
            cur.close()
            return {'columns': [], 'rows': [], 'row_count': 0}
            
    except Exception as e:
        return {'error': str(e)}


def handle_write(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Execute a write operation."""
    sql = payload.get('sql', '')
    params = payload.get('params', [])
    
    if not sql:
        return {'error': 'No SQL provided'}
    
    try:
        cur = conn.cursor()
        
        # Convert ? placeholders to %s for PostgreSQL
        pg_sql = sql.replace('?', '%s')
        
        cur.execute(pg_sql, params if params else None)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        
        return {'success': True, 'affected_rows': affected}
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}


def resolve_store_id_from_db(conn: psycopg2.extensions.connection, route_number: str, store_id: str) -> str:
    """Resolve a store_id to its canonical form using the alias table."""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT canonical_id FROM store_id_aliases 
            WHERE route_number = %s AND alias_id = %s
        """, [route_number, store_id])
        result = cur.fetchone()
        cur.close()
        if result:
            return result[0]
    except Exception:
        pass
    return store_id


def _load_archived_forecast(
    route_number: str,
    delivery_date: str,
    schedule_key: str,
    forecast_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Best-effort: load a forecast JSON from the server's HDD archive.

    Expected layout:
      {ROUTESPARK_FORECAST_ARCHIVE_DIR}/{route}/{deliveryDate}/{scheduleKey}/{forecastId}.json

    If forecast_id is not provided, loads the most recently generated forecast in that folder.
    """
    root = os.environ.get("ROUTESPARK_FORECAST_ARCHIVE_DIR")
    if not root:
        return None, None

    base_dir = Path(root) / str(route_number) / str(delivery_date) / str(schedule_key)
    if not base_dir.exists() or not base_dir.is_dir():
        return None, None

    def _read(p: Path) -> Optional[Dict[str, Any]]:
        try:
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    if forecast_id:
        direct = base_dir / f"{forecast_id}.json"
        d = _read(direct)
        return (forecast_id, d) if d else (None, None)

    candidates: List[Tuple[float, Path]] = []
    try:
        for p in base_dir.iterdir():
            if not p.is_file() or p.suffix.lower() != ".json":
                continue
            d = _read(p)
            if not d:
                continue
            ts = 0.0
            ga = d.get("generatedAt")
            try:
                if isinstance(ga, str) and ga:
                    ts = datetime.fromisoformat(ga.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts = 0.0
            if ts <= 0:
                try:
                    ts = p.stat().st_mtime
                except Exception:
                    ts = 0.0
            candidates.append((ts, p))
    except Exception:
        return None, None

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0], reverse=True)
    best = _read(candidates[0][1])
    if not best:
        return None, None
    fid = best.get("forecastId") or candidates[0][1].stem
    return str(fid), best


def handle_sync_order(conn: psycopg2.extensions.connection, db: firestore.Client, payload: Dict) -> Dict:
    """Sync an order from Firebase to PostgreSQL."""
    order_id = payload.get('orderId')
    route_number = payload.get('routeNumber')
    
    if not order_id:
        return {'error': 'No orderId provided'}
    
    try:
        # Fetch order from Firebase
        order_ref = db.collection('routes').document(str(route_number)).collection('orders').document(order_id)
        order_doc = order_ref.get()
        
        if not order_doc.exists:
            return {'error': f'Order {order_id} not found'}
        
        data = order_doc.to_dict()
        now = datetime.now(timezone.utc)
        
        delivery_date = data.get('expectedDeliveryDate', '')
        order_date = data.get('orderDate', '')
        schedule_key = data.get('scheduleKey', 'unknown')
        stores = data.get('stores', [])

        # Best-effort: load the cached forecast used for this delivery/schedule so we can
        # extract corrections even if the app didn't store forecastRecommendedUnits on items.
        # This is critical for capturing removals (forecasted > 0 but not ordered).
        cached_forecast_id: Optional[str] = None
        forecast_lookup: Dict[str, Dict[str, Any]] = {}  # "{storeId}::{sap}" -> cached forecast item
        if route_number and delivery_date and schedule_key:
            try:
                cached_ref = (
                    db.collection('forecasts')
                    .document(str(route_number))
                    .collection('cached')
                )

                candidates = []
                for doc in cached_ref.stream():
                    d = doc.to_dict() or {}
                    if d.get('deliveryDate') != delivery_date:
                        continue
                    if d.get('scheduleKey') != schedule_key:
                        continue
                    candidates.append((doc.id, d))

                def _ts(v: Any) -> float:
                    try:
                        if hasattr(v, "timestamp"):
                            return float(v.timestamp())
                    except Exception:
                        pass
                    return 0.0

                if candidates:
                    candidates.sort(key=lambda x: _ts((x[1] or {}).get("generatedAt")), reverse=True)
                    cached_forecast_id, cached_forecast = candidates[0]
                    for it in (cached_forecast.get('items') or []):
                        sid_raw = it.get('storeId') or ''
                        sap_raw = it.get('sap') or ''
                        sap = str(sap_raw).strip()
                        if not sid_raw or not sap:
                            continue
                        sid_resolved = resolve_store_id_from_db(conn, route_number, sid_raw) or sid_raw
                        forecast_lookup[f"{sid_resolved}::{sap}"] = it
            except Exception as e:
                print(f"     ⚠️  Warning: Could not load cached forecast for corrections: {e}")

            # Fallback: cached forecasts are TTL-based; if expired, load from server archive.
            if not forecast_lookup:
                try:
                    fid, archived = _load_archived_forecast(
                        route_number=str(route_number),
                        delivery_date=str(delivery_date),
                        schedule_key=str(schedule_key),
                        forecast_id=None,
                    )
                    if archived and (archived.get("items") or []):
                        cached_forecast_id = fid or cached_forecast_id
                        for it in (archived.get("items") or []):
                            sid_raw = it.get('storeId') or ''
                            sap_raw = it.get('sap') or ''
                            sap = str(sap_raw).strip()
                            if not sid_raw or not sap:
                                continue
                            sid_resolved = resolve_store_id_from_db(conn, route_number, sid_raw) or sid_raw
                            forecast_lookup[f"{sid_resolved}::{sap}"] = it
                except Exception as e:
                    print(f"     ⚠️  Warning: Could not load archived forecast for corrections: {e}")
        
        # Holiday week check
        user_marked_holiday = data.get('isHolidaySchedule', False)
        if user_marked_holiday:
            holiday_week = True
            holiday_name = 'User-marked Holiday'
            print(f"     🦃 Order {order_id} marked by user as Holiday Schedule")
        else:
            holiday_week, holiday_name = is_holiday_week(order_date)
            if holiday_week:
                print(f"     🦃 Order {order_id} is in {holiday_name} week")
        
        # Count totals
        total_units = 0
        for store in stores:
            for item in store.get('items', []):
                total_units += item.get('quantity', 0)
        
        cur = conn.cursor()
        
        # Insert/update order header using PostgreSQL ON CONFLICT
        cur.execute("""
            INSERT INTO orders_historical (
                order_id, route_number, user_id, schedule_key,
                delivery_date, order_date, status, total_units, store_count, is_holiday_week, synced_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'finalized', %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                total_units = EXCLUDED.total_units,
                store_count = EXCLUDED.store_count,
                is_holiday_week = EXCLUDED.is_holiday_week,
                synced_at = EXCLUDED.synced_at
        """, [
            order_id,
            data.get('routeNumber', route_number),
            data.get('userId', ''),
            schedule_key,
            delivery_date if delivery_date else None,
            order_date if order_date else None,
            total_units,
            len(stores),
            holiday_week,
            now
        ])
        
        # Get forecast ID
        forecast_id = (
            data.get('forecastId')
            or cached_forecast_id
            or f'order-derived-{order_id}'
        )

        # Did this order actually carry forecast context?
        # We only infer "added-not-forecasted" corrections when forecast context exists.
        has_forecast_context = bool(data.get('forecastId'))
        if not has_forecast_context:
            for store in stores:
                for item in store.get('items', []):
                    if (
                        item.get('forecastId')
                        or item.get('forecastRecommendedUnits') is not None
                        or item.get('forecastSuggestedUnits') is not None
                        or item.get('forecastedQuantity') is not None
                    ):
                        has_forecast_context = True
                        break
                if has_forecast_context:
                    break
        
        # Collect line items and corrections for batch insert
        line_item_rows = []
        correction_rows = []
        ordered_map: Dict[str, int] = {}  # "{storeId}::{sap}" -> final units synced to Postgres (>0 only)
        
        for store in stores:
            raw_store_id = store.get('id', store.get('storeId', ''))
            store_name = store.get('name', store.get('storeName', ''))
            store_id = resolve_store_id_from_db(conn, route_number, raw_store_id) if raw_store_id else ''
            store_key = store_id or raw_store_id or ''
            
            for item in store.get('items', []):
                sap = item.get('sap', '')
                quantity = item.get('quantity', 0)
                
                if not sap or quantity == 0:
                    continue

                sap = str(sap).strip()
                ordered_key = f"{store_key}::{sap}"
                ordered_map[ordered_key] = int(quantity)
                
                line_id = f"{order_id}-{store_id}-{sap}"
                item_cases = item.get('cases')
                promo_active = item.get('promoActive', False)
                promo_id = item.get('promoId')

                line_item_rows.append((
                    line_id, order_id, data.get('routeNumber', route_number),
                    schedule_key, delivery_date if delivery_date else None,
                    store_id, store_name, sap, quantity,
                    int(item_cases) if item_cases is not None else 0,
                    promo_id, bool(promo_active),
                    now
                ))
                
                # Extract corrections
                forecasted_qty = item.get('forecastRecommendedUnits')
                if forecasted_qty is None:
                    forecasted_qty = item.get('forecastSuggestedUnits')
                if forecasted_qty is None:
                    forecasted_qty = item.get('forecastedQuantity')
                # If the order item doesn't carry forecast metadata (e.g. older web portal writes),
                # fall back to the archived/cached forecast for this delivery+schedule.
                if forecasted_qty is None:
                    fit = forecast_lookup.get(ordered_key)
                    if fit is not None:
                        forecasted_qty = fit.get('recommendedUnits')

                # If forecast context exists and this ordered line was not in forecast_lookup,
                # record it explicitly as an added line (predicted=0, final>0).
                added_not_forecasted = (
                    has_forecast_context
                    and bool(forecast_lookup)
                    and forecasted_qty is None
                    and ordered_key not in forecast_lookup
                )

                user_adjusted = item.get('userAdjusted', None)
                if user_adjusted is None:
                    user_adjusted = (forecasted_qty is not None and float(forecasted_qty) != float(quantity))

                if added_not_forecasted:
                    final = float(quantity)
                    final_cases = item_cases if item_cases is not None else 0
                    correction_id = f"{order_id}-{store_id}-{sap}-add"
                    correction_rows.append((
                        correction_id, forecast_id, order_id,
                        data.get('routeNumber', route_number), schedule_key,
                        delivery_date if delivery_date else None, store_id, store_name, sap,
                        0,  # predicted_units
                        0,  # predicted_cases
                        int(final),
                        int(final_cases) if final_cases is not None else 0,
                        int(final),  # delta = final - 0
                        1.0,  # ratio is undefined at predicted=0; keep neutral
                        False,  # was_removed
                        promo_id, bool(promo_active),
                        now
                    ))
                elif user_adjusted or (forecasted_qty is not None and forecasted_qty != quantity):
                    predicted = float(forecasted_qty) if forecasted_qty is not None else 0.0
                    final = float(quantity)
                    delta = int(final - predicted)
                    ratio = final / predicted if predicted > 0 else 1.0
                    was_removed = final == 0 and predicted > 0
                    predicted_cases = item.get('forecastRecommendedCases')
                    if predicted_cases is None:
                        predicted_cases = item.get('forecastSuggestedCases')
                    if predicted_cases is None:
                        predicted_cases = item.get('forecastedCases')
                    if predicted_cases is None:
                        fit = forecast_lookup.get(ordered_key)
                        if fit is not None:
                            predicted_cases = fit.get('recommendedCases')
                    final_cases = item_cases if item_cases is not None else 0

                    correction_id = f"{order_id}-{store_id}-{sap}-corr"
                    correction_rows.append((
                        correction_id, forecast_id, order_id,
                        data.get('routeNumber', route_number), schedule_key,
                        delivery_date if delivery_date else None, store_id, store_name, sap,
                        int(predicted),
                        int(predicted_cases) if predicted_cases is not None else 0,
                        int(final),
                        int(final_cases) if final_cases is not None else 0,
                        delta, ratio,
                        was_removed,
                        promo_id, bool(promo_active),
                        now
                    ))

        # Add "removal corrections": forecasted > 0 but not ordered (missing from order doc).
        # Without this, the model never learns from explicit rejections of recommendations.
        if forecast_lookup:
            for key, fit in forecast_lookup.items():
                if key in ordered_map:
                    continue
                try:
                    rec_units = fit.get("recommendedUnits")
                    if rec_units is None:
                        continue
                    rec_units = int(rec_units)
                    if rec_units <= 0:
                        continue

                    store_id, sap = key.split("::", 1)
                    store_name = fit.get("storeName") or ""
                    rec_cases = fit.get("recommendedCases")
                    rec_cases = int(rec_cases) if rec_cases is not None else 0
                    promo_active = bool(fit.get("promoActive", False))
                    promo_id = fit.get("promoId")

                    correction_id = f"{order_id}-{store_id}-{sap}-rm"
                    predicted = float(rec_units)
                    final = 0.0
                    delta = int(final - predicted)
                    ratio = 0.0
                    was_removed = True

                    correction_rows.append((
                        correction_id, forecast_id, order_id,
                        data.get('routeNumber', route_number), schedule_key,
                        delivery_date if delivery_date else None, store_id, store_name, sap,
                        int(predicted),
                        int(rec_cases),
                        0,
                        0,
                        delta, ratio,
                        was_removed,
                        promo_id, promo_active,
                        now
                    ))
                except Exception:
                    continue
        
        # Batch insert line items using execute_values (much faster than individual inserts)
        if line_item_rows:
            execute_values(cur, """
                INSERT INTO order_line_items (
                    line_item_id, order_id, route_number, schedule_key,
                    delivery_date, store_id, store_name, sap, quantity, cases,
                    promo_id, promo_active, synced_at
                ) VALUES %s
                ON CONFLICT (line_item_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    cases = EXCLUDED.cases,
                    promo_id = EXCLUDED.promo_id,
                    promo_active = EXCLUDED.promo_active,
                    synced_at = EXCLUDED.synced_at
            """, line_item_rows, page_size=100)
        
        # Batch insert corrections
        if correction_rows:
            execute_values(cur, """
                INSERT INTO forecast_corrections (
                    correction_id, forecast_id, order_id, route_number, schedule_key,
                    delivery_date, store_id, store_name, sap,
                    predicted_units, predicted_cases, final_units, final_cases,
                    correction_delta, correction_ratio, was_removed,
                    promo_id, promo_active,
                    submitted_at
                ) VALUES %s
                ON CONFLICT (correction_id) DO UPDATE SET
                    final_units = EXCLUDED.final_units,
                    final_cases = EXCLUDED.final_cases,
                    predicted_units = EXCLUDED.predicted_units,
                    predicted_cases = EXCLUDED.predicted_cases,
                    correction_delta = EXCLUDED.correction_delta,
                    correction_ratio = EXCLUDED.correction_ratio,
                    was_removed = EXCLUDED.was_removed,
                    promo_id = EXCLUDED.promo_id,
                    promo_active = EXCLUDED.promo_active
            """, correction_rows, page_size=100)
        
        conn.commit()
        cur.close()
        
        return {
            'success': True,
            'orderId': order_id,
            'totalUnits': total_units,
            'storeCount': len(stores),
            'correctionsExtracted': len(correction_rows),
            'isHolidayWeek': holiday_week,
        }
        
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        return {'error': str(e)}


def handle_get_historical_shares(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Get case allocation shares for a SAP."""
    route_number = payload.get('routeNumber')
    sap = payload.get('sap')
    schedule_key = payload.get('scheduleKey')
    
    if not route_number or not sap or not schedule_key:
        return {'error': 'routeNumber, sap, and scheduleKey are required'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT store_id, store_name, share, avg_quantity, order_count, recent_share, share_trend
            FROM store_item_shares
            WHERE route_number = %s AND sap = %s AND schedule_key = %s
            ORDER BY share DESC
        """, [route_number, sap, schedule_key])
        
        shares = {}
        for row in cur.fetchall():
            shares[row['store_id']] = {
                'storeName': row['store_name'],
                'share': row['share'],
                'avgQuantity': row['avg_quantity'],
                'orderCount': row['order_count'],
                'recentShare': row['recent_share'],
                'shareTrend': row['share_trend'],
            }
        
        cur.close()
        return {'shares': shares}
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_archived_dates(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Get list of archived order dates with summary info.
    
    Returns ArchivedDateSummary objects matching the app's expected format:
    { date: string, scheduleKey: string, itemCount: number }
    """
    route_number = payload.get('routeNumber')
    
    if not route_number:
        return {'error': 'routeNumber is required'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Match original DuckDB query structure - group by order_id to get one row per order
        cur.execute("""
            SELECT 
                o.delivery_date,
                o.schedule_key,
                COUNT(DISTINCT li.line_item_id) as item_count
            FROM orders_historical o
            LEFT JOIN order_line_items li ON o.order_id = li.order_id
            WHERE o.route_number = %s
            GROUP BY o.order_id, o.delivery_date, o.schedule_key
            ORDER BY o.delivery_date DESC
            LIMIT 200
        """, [route_number])
        
        dates = []
        for row in cur.fetchall():
            delivery_date = row['delivery_date']
            date_str = delivery_date.strftime('%Y-%m-%d') if hasattr(delivery_date, 'strftime') else str(delivery_date)
            dates.append({
                'date': date_str,
                'scheduleKey': row['schedule_key'] or 'unknown',
                'itemCount': row['item_count'] or 0,
            })
        cur.close()
        
        return {'dates': dates}
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_order(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Get a specific archived order.
    
    Returns order in the exact format the app expects (matching original DuckDB version).
    """
    route_number = payload.get('routeNumber')
    delivery_date = payload.get('deliveryDate')
    
    if not route_number or not delivery_date:
        return {'error': 'Missing routeNumber or deliveryDate'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get order header
        cur.execute("""
            SELECT order_id, schedule_key, order_date, total_units, store_count
            FROM orders_historical
            WHERE route_number = %s AND delivery_date = %s
            LIMIT 1
        """, [route_number, delivery_date])
        
        order_row = cur.fetchone()
        if not order_row:
            return {'error': f'No order found for {delivery_date}'}
        
        order_id = order_row['order_id']
        
        # Get line items grouped by store
        cur.execute("""
            SELECT store_id, store_name, sap, quantity
            FROM order_line_items
            WHERE order_id = %s
            ORDER BY store_id, sap
        """, [order_id])
        
        # Group by store - use keys that match app expectations
        stores = {}
        for row in cur.fetchall():
            store_id = row['store_id']
            if store_id not in stores:
                stores[store_id] = {
                    'storeId': store_id,
                    'storeName': row['store_name'],
                    'items': []
                }
            stores[store_id]['items'].append({
                'sap': row['sap'],
                'quantity': row['quantity'],
            })
        
        # Convert date to string for Firestore
        order_date = order_row['order_date']
        if hasattr(order_date, 'isoformat'):
            order_date = order_date.isoformat()
        
        cur.close()
        
        return {
            'order': {
                'id': order_id,
                'routeNumber': route_number,
                'scheduleKey': order_row['schedule_key'],
                'deliveryDate': delivery_date,
                'orderDate': order_date,
                'totalUnits': order_row['total_units'],
                'storeCount': order_row['store_count'],
                'stores': list(stores.values()),
            }
        }
        
    except Exception as e:
        return {'error': str(e)}


def handle_check_route_synced(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Check if a route is synced in PostgreSQL."""
    route_number = payload.get('routeNumber')
    
    if not route_number:
        return {'error': 'routeNumber is required'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM routes_synced WHERE route_number = %s
        """, [route_number])
        
        row = cur.fetchone()
        cur.close()
        
        if not row:
            return {'synced': False, 'status': 'not_synced'}
        
        result = dict(row)
        for k, v in result.items():
            if hasattr(v, 'isoformat'):
                result[k] = v.isoformat()
        
        result['synced'] = True
        return result
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_delivery_manifest(conn: psycopg2.extensions.connection, payload: Dict) -> Dict:
    """Get delivery manifest for a date."""
    route_number = payload.get('routeNumber')
    delivery_date = payload.get('deliveryDate')
    store_id = payload.get('storeId')
    
    if not route_number or not delivery_date:
        return {'error': 'routeNumber and deliveryDate are required'}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query allocations
        if store_id:
            cur.execute("""
                SELECT store_id, store_name, sap, quantity
                FROM delivery_allocations
                WHERE route_number = %s AND delivery_date = %s AND store_id = %s
                ORDER BY store_name, sap
            """, [route_number, delivery_date, store_id])
        else:
            cur.execute("""
                SELECT store_id, store_name, sap, quantity
                FROM delivery_allocations
                WHERE route_number = %s AND delivery_date = %s
                ORDER BY store_name, sap
            """, [route_number, delivery_date])
        
        rows = cur.fetchall()
        cur.close()
        
        # Group by store
        stores_map = {}
        total_items = 0
        total_quantity = 0
        
        for row in rows:
            sid = row['store_id']
            if sid not in stores_map:
                stores_map[sid] = {
                    'storeId': sid,
                    'storeName': row['store_name'],
                    'items': [],
                    'itemCount': 0,
                    'totalQuantity': 0,
                }
            stores_map[sid]['items'].append({
                'sap': row['sap'],
                'quantity': row['quantity'],
            })
            stores_map[sid]['itemCount'] += 1
            stores_map[sid]['totalQuantity'] += row['quantity']
            total_items += 1
            total_quantity += row['quantity']
        
        return {
            'manifest': {
                'routeNumber': route_number,
                'deliveryDate': delivery_date,
                'stores': list(stores_map.values()),
                'totalStores': len(stores_map),
                'totalItems': total_items,
                'totalQuantity': total_quantity,
            }
        }
        
    except Exception as e:
        return {'error': str(e)}


# =============================================================================
# Main Request Handler
# =============================================================================

HANDLERS = {
    'query': lambda conn, db, p: handle_query(conn, p),
    'write': lambda conn, db, p: handle_write(conn, p),
    'sync_order': handle_sync_order,
    'get_historical_shares': lambda conn, db, p: handle_get_historical_shares(conn, p),
    'get_archived_dates': lambda conn, db, p: handle_get_archived_dates(conn, p),
    'get_order': lambda conn, db, p: handle_get_order(conn, p),
    'check_route_synced': lambda conn, db, p: handle_check_route_synced(conn, p),
    'get_delivery_manifest': lambda conn, db, p: handle_get_delivery_manifest(conn, p),
}


def process_request(conn: psycopg2.extensions.connection, db: firestore.Client, request_doc) -> Dict:
    """Process a single request."""
    data = request_doc.to_dict()
    request_type = data.get('type', 'unknown')
    payload = data.get('payload', {})
    
    handler = HANDLERS.get(request_type)
    if not handler:
        return {'error': f'Unknown request type: {request_type}'}
    
    # Check handler signature
    import inspect
    sig = inspect.signature(handler)
    if len(sig.parameters) == 3:
        return handler(conn, db, payload)
    else:
        return handler(conn, payload)


def run_listener(conn: psycopg2.extensions.connection, db: firestore.Client):
    """Main listener - uses Firestore on_snapshot for real-time updates.
    
    Performance: Zero polling - only triggers when a new request arrives.
    This eliminates ~170k daily Firestore reads from polling.
    """
    global _shutdown_requested
    
    requests_ref = db.collection('dbRequests')
    
    # Metrics for monitoring
    total_requests = 0
    last_stats_time = time.time()
    
    print(f"[db_manager_pg] Listening for requests as {WORKER_ID}")
    print(f"[db_manager_pg] PostgreSQL connected: {conn.info.host}:{conn.info.port}/{conn.info.dbname}")
    print(f"[db_manager_pg] Mode: Real-time (on_snapshot) - zero polling")
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle collection changes in real-time."""
        nonlocal total_requests, last_stats_time
        
        for change in changes:
            if _shutdown_requested:
                return
            
            # Only process new/modified pending requests
            if change.type.name not in ('ADDED', 'MODIFIED'):
                continue
            
            doc = change.document
            data = doc.to_dict() or {}
            
            # Only process pending requests
            if data.get('status') != 'pending':
                continue
            
            request_id = doc.id
            request_type = data.get('type', 'unknown')
            
            print(f"[db_manager_pg] Processing {request_type} request: {request_id}")
            
            try:
                # Mark as processing (atomic claim)
                doc.reference.update({
                    'status': 'processing',
                    'workerId': WORKER_ID,
                    'processingStartedAt': firestore.SERVER_TIMESTAMP,
                })
                
                # Process the request
                result = process_request(conn, db, doc)
                
                # Update with result
                if 'error' in result:
                    doc.reference.update({
                        'status': 'error',
                        'error': result['error'],
                        'completedAt': firestore.SERVER_TIMESTAMP,
                    })
                    print(f"[db_manager_pg] Request {request_id} failed: {result['error']}")
                else:
                    doc.reference.update({
                        'status': 'completed',
                        'result': result,
                        'completedAt': firestore.SERVER_TIMESTAMP,
                    })
                    print(f"[db_manager_pg] Request {request_id} completed")
                
                total_requests += 1
                    
            except Exception as e:
                traceback.print_exc()
                try:
                    doc.reference.update({
                        'status': 'error',
                        'error': str(e),
                        'completedAt': firestore.SERVER_TIMESTAMP,
                    })
                except:
                    pass
        
        # Log stats every 5 minutes
        now = time.time()
        if now - last_stats_time >= 300:
            print(f"[db_manager_pg] Stats: {total_requests} requests processed in last 5 min")
            last_stats_time = now
    
    # Start real-time listener for pending requests
    query = requests_ref.where('status', '==', 'pending')
    watcher = query.on_snapshot(on_snapshot)
    
    print(f"[db_manager_pg] Real-time listener started")
    
    # Keep alive until shutdown requested
    try:
        while not _shutdown_requested:
            time.sleep(1)
    finally:
        # Clean up the watcher
        watcher.unsubscribe()
        print("[db_manager_pg] Shutting down...")


def handle_shutdown(signum, frame):
    """Handle shutdown signal."""
    global _shutdown_requested
    _shutdown_requested = True
    print("\n[db_manager_pg] Shutdown requested...")


def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Database Manager Service')
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    # PostgreSQL connection - prefer environment variables for security
    # CLI args are fallback only (avoid passing password on command line)
    parser.add_argument('--host', default=None, help='PostgreSQL host (default: POSTGRES_HOST env)')
    parser.add_argument('--port', type=int, default=None, help='PostgreSQL port (default: POSTGRES_PORT env)')
    parser.add_argument('--database', default=None, help='Database name (default: POSTGRES_DB env)')
    parser.add_argument('--user', default=None, help='Username (default: POSTGRES_USER env)')
    parser.add_argument('--password', default=None, help='Password (default: POSTGRES_PASSWORD env)')
    args = parser.parse_args()
    
    # Resolve from environment, with CLI override
    pg_host = args.host or os.environ.get('POSTGRES_HOST', 'localhost')
    pg_port = args.port or int(os.environ.get('POSTGRES_PORT', 5432))
    pg_database = args.database or os.environ.get('POSTGRES_DB', 'routespark')
    pg_user = args.user or os.environ.get('POSTGRES_USER', 'routespark')
    pg_password = args.password or os.environ.get('POSTGRES_PASSWORD', '')
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    print(f"[db_manager_pg] Starting PostgreSQL Database Manager")
    print(f"[db_manager_pg] Worker ID: {WORKER_ID}")
    print(f"[db_manager_pg] PostgreSQL: {pg_user}@{pg_host}:{pg_port}/{pg_database}")
    
    # Initialize connections
    try:
        db = get_firestore_client(args.serviceAccount)
        print(f"[db_manager_pg] Firebase connected")
        
        conn = get_pg_conn(pg_host, pg_port, pg_database, pg_user, pg_password)
        print(f"[db_manager_pg] PostgreSQL connected")
        
    except Exception as e:
        print(f"[db_manager_pg] Connection failed: {e}")
        return 1
    
    # Run the listener
    try:
        run_listener(conn, db)
    finally:
        conn.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

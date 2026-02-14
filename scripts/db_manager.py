#!/usr/bin/env python3
"""Centralized Database Manager Service.

This service owns the single DuckDB connection and handles all database operations.
Other services communicate via Firebase `dbRequests` collection.

Request Types:
- query: Execute a read query, return results
- write: Execute a write operation
- sync_order: Sync an order from Firebase to DuckDB
- sync_route: Full sync of a route's data
- get_historical_shares: Get case allocation shares for a SAP

Usage:
    python scripts/db_manager.py \
        --serviceAccount /path/to/sa.json \
        --duckdb data/analytics.duckdb
"""

from __future__ import annotations

import argparse
import json
import signal
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
from google.cloud import firestore

# Global flag for graceful shutdown
_shutdown_requested = False

# Worker ID for this instance
WORKER_ID = f"dbmgr-{socket.gethostname()}-{__import__('os').getpid()}"

# Request timeout (seconds)
REQUEST_TIMEOUT = 60

# =============================================================================
# HOLIDAY WEEK DETECTION
# =============================================================================
# Holiday weeks (start_date, end_date, name) - weeks containing major holidays
# Orders during these weeks are marked is_holiday_week=TRUE to exclude from training

HOLIDAY_WEEKS = [
    # 2024
    ('2024-11-25', '2024-12-01', 'Thanksgiving'),
    ('2024-12-23', '2024-12-29', 'Christmas'),
    # 2025
    ('2025-11-24', '2025-11-30', 'Thanksgiving'),
    ('2025-12-22', '2025-12-28', 'Christmas'),
    # 2026
    ('2026-11-23', '2026-11-29', 'Thanksgiving'),
    ('2026-12-21', '2026-12-27', 'Christmas'),
]


def is_holiday_week(order_date_str: str) -> tuple:
    """Check if an order date falls within a holiday week.
    
    Args:
        order_date_str: Order date in YYYY-MM-DD or MM/DD/YYYY format
    
    Returns:
        (is_holiday: bool, holiday_name: str)
    """
    if not order_date_str:
        return False, ''
    
    try:
        if '/' in order_date_str:
            dt = datetime.strptime(order_date_str, '%m/%d/%Y')
        else:
            # Handle ISO format, possibly with time
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


def get_duckdb_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection with write access.
    
    Uses single thread to minimize GIL contention with gRPC threads.
    db_manager processes requests sequentially, so parallelism isn't needed.
    """
    return duckdb.connect(db_path, read_only=False, config={'threads': 1})


# =============================================================================
# Request Handlers
# =============================================================================

def handle_query(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Execute a read query and return results."""
    sql = payload.get('sql', '')
    params = payload.get('params', [])
    
    if not sql:
        return {'error': 'No SQL provided'}
    
    try:
        result = conn.execute(sql, params).fetchall()
        columns = [desc[0] for desc in conn.description] if conn.description else []
        
        # Convert to list of dicts for JSON serialization
        rows = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Handle special types that Firestore can't serialize
                if val is None:
                    pass  # Keep as None
                elif hasattr(val, 'isoformat'):
                    # datetime, date, time objects
                    val = val.isoformat()
                elif isinstance(val, bytes):
                    val = val.decode('utf-8', errors='replace')
                row_dict[col] = val
            rows.append(row_dict)
        
        return {
            'columns': columns,
            'rows': rows,
            'row_count': len(rows),
        }
    except Exception as e:
        return {'error': str(e)}


def handle_write(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Execute a write operation."""
    sql = payload.get('sql', '')
    params = payload.get('params', [])
    
    if not sql:
        return {'error': 'No SQL provided'}
    
    try:
        conn.execute(sql, params)
        conn.commit()  # Ensure write is persisted to disk
        return {'success': True, 'affected_rows': conn.rowcount if hasattr(conn, 'rowcount') else -1}
    except Exception as e:
        return {'error': str(e)}


def resolve_store_id_from_db(conn: duckdb.DuckDBPyConnection, route_number: str, store_id: str) -> str:
    """Resolve a store_id to its canonical form using the alias table."""
    try:
        result = conn.execute("""
            SELECT canonical_id FROM store_id_aliases 
            WHERE route_number = ? AND alias_id = ?
        """, [route_number, store_id]).fetchone()
        if result:
            return result[0]
    except Exception:
        pass
    return store_id


def handle_sync_order(conn: duckdb.DuckDBPyConnection, db: firestore.Client, payload: Dict) -> Dict:
    """Sync an order from Firebase to DuckDB."""
    order_id = payload.get('orderId')
    route_number = payload.get('routeNumber')
    
    if not order_id:
        return {'error': 'No orderId provided'}
    
    try:
        # Fetch order from Firebase
        order_ref = db.collection('orders').document(order_id)
        order_doc = order_ref.get()
        
        if not order_doc.exists:
            return {'error': f'Order {order_id} not found'}
        
        data = order_doc.to_dict()
        now = datetime.now(timezone.utc)
        
        delivery_date = data.get('expectedDeliveryDate', '')
        order_date = data.get('orderDate', '')
        schedule_key = data.get('scheduleKey', 'unknown')
        stores = data.get('stores', [])
        
        # Check if this is a holiday week
        # Priority: 1. User's explicit flag from Firebase, 2. Calendar-based detection
        user_marked_holiday = data.get('isHolidaySchedule', False)
        if user_marked_holiday:
            holiday_week = True
            holiday_name = 'User-marked Holiday'
            print(f"     🦃 Order {order_id} marked by user as Holiday Schedule - excluding from training")
        else:
            holiday_week, holiday_name = is_holiday_week(order_date)
            if holiday_week:
                print(f"     🦃 Order {order_id} is in {holiday_name} week - marking as holiday")
        
        # Count totals
        total_units = 0
        item_count = 0
        for store in stores:
            for item in store.get('items', []):
                total_units += item.get('quantity', 0)
                item_count += 1
        
        # Insert order header (with holiday week flag)
        conn.execute("""
            INSERT INTO orders_historical (
                order_id, route_number, user_id, schedule_key,
                delivery_date, order_date, status, total_units, store_count, is_holiday_week, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'finalized', ?, ?, ?, ?)
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
            delivery_date,
            order_date,
            total_units,
            len(stores),
            holiday_week,
            now
        ])
        
        # Get forecast ID from order (for correction tracking)
        forecast_id = data.get('forecastId', f'order-derived-{order_id}')
        
        # Insert line items AND extract corrections
        corrections_count = 0
        for store in stores:
            raw_store_id = store.get('id', store.get('storeId', ''))
            store_name = store.get('name', store.get('storeName', ''))
            
            # Resolve store ID to canonical form (handles old/alias IDs)
            store_id = resolve_store_id_from_db(conn, route_number, raw_store_id) if raw_store_id else ''
            
            for item in store.get('items', []):
                sap = item.get('sap', '')
                quantity = item.get('quantity', 0)
                
                if not sap or quantity == 0:
                    continue
                
                line_id = f"{order_id}-{store_id}-{sap}"
                
                conn.execute("""
                    INSERT INTO order_line_items (
                        line_item_id, order_id, route_number, schedule_key,
                        delivery_date, store_id, store_name, sap, quantity, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (line_item_id) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        synced_at = EXCLUDED.synced_at
                """, [
                    line_id,
                    order_id,
                    data.get('routeNumber', route_number),
                    schedule_key,
                    delivery_date,
                    store_id,
                    store_name,
                    sap,
                    quantity,
                    now
                ])
                
                # Extract correction if forecast data exists
                # Canonical field: forecastRecommendedUnits (legacy: forecastSuggestedUnits)
                forecasted_qty = (
                    item.get('forecastRecommendedUnits')
                    or item.get('forecastSuggestedUnits')
                    or item.get('forecastedQuantity')
                )
                user_adjusted = item.get('userAdjusted', False)
                
                # Create correction if:
                # 1. User explicitly adjusted the quantity, OR
                # 2. There was a forecast and the final quantity differs
                if user_adjusted or (forecasted_qty is not None and forecasted_qty != quantity):
                    predicted = float(forecasted_qty) if forecasted_qty is not None else 0.0
                    final = float(quantity)
                    delta = final - predicted
                    ratio = (final / predicted) if predicted > 0 else (1.0 if final > 0 else 0.0)
                    was_removed = (final == 0 and predicted > 0)
                    
                    correction_id = f"corr-{order_id}-{store_id}-{sap}"
                    
                    conn.execute("""
                        INSERT INTO forecast_corrections (
                            correction_id, forecast_id, order_id, route_number,
                            schedule_key, delivery_date, store_id, store_name, sap,
                            predicted_units, predicted_cases, final_units, final_cases,
                            correction_delta, correction_ratio, was_removed,
                            promo_id, promo_active, is_first_weekend_of_month, is_holiday_week,
                            submitted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (correction_id) DO UPDATE SET
                            predicted_units = EXCLUDED.predicted_units,
                            final_units = EXCLUDED.final_units,
                            correction_delta = EXCLUDED.correction_delta,
                            correction_ratio = EXCLUDED.correction_ratio,
                            was_removed = EXCLUDED.was_removed,
                            submitted_at = EXCLUDED.submitted_at
                    """, [
                        correction_id,
                        forecast_id,
                        order_id,
                        data.get('routeNumber', route_number),
                        schedule_key,
                        delivery_date,
                        store_id,
                        store_name,
                        sap,
                        predicted,
                        item.get('forecastRecommendedCases')
                        or item.get('forecastSuggestedCases')
                        or item.get('forecastedCases', 0)
                        or 0,
                        final,
                        item.get('cases', 0) or 0,
                        delta,
                        ratio,
                        was_removed,
                        item.get('promoId'),
                        item.get('promoActive', False),
                        False,  # is_first_weekend - would need calendar lookup
                        holiday_week,
                        now
                    ])
                    corrections_count += 1
        
        if corrections_count > 0:
            print(f"     📊 Extracted {corrections_count} corrections for ML training")
        
        return {
            'success': True,
            'orderId': order_id,
            'totalUnits': total_units,
            'storeCount': len(stores),
            'itemCount': item_count,
            'correctionsExtracted': corrections_count,
        }
        
    except Exception as e:
        return {'error': str(e), 'traceback': traceback.format_exc()}


def handle_get_historical_shares(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Get historical case allocation shares for a SAP."""
    route_number = payload.get('routeNumber')
    sap = payload.get('sap')
    schedule_key = payload.get('scheduleKey')
    
    if not all([route_number, sap, schedule_key]):
        return {'error': 'Missing routeNumber, sap, or scheduleKey'}
    
    try:
        result = conn.execute("""
            SELECT store_id, share, avg_quantity, order_count
            FROM store_item_shares
            WHERE route_number = ? AND sap = ? AND schedule_key = ?
        """, [route_number, sap, schedule_key]).fetchall()
        
        shares = {}
        for row in result:
            shares[row[0]] = {
                'share': row[1],
                'avgQuantity': row[2],
                'orderCount': row[3],
            }
        
        return {'shares': shares}
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_archived_dates(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Get list of archived order dates for a route."""
    route_number = payload.get('routeNumber')
    
    if not route_number:
        return {'error': 'Missing routeNumber'}
    
    try:
        result = conn.execute("""
            SELECT 
                o.delivery_date,
                o.schedule_key,
                COUNT(DISTINCT li.line_item_id) as item_count
            FROM orders_historical o
            LEFT JOIN order_line_items li ON o.order_id = li.order_id
            WHERE o.route_number = ?
            GROUP BY o.order_id, o.delivery_date, o.schedule_key
            ORDER BY o.delivery_date DESC
            LIMIT 200
        """, [route_number]).fetchall()
        
        dates = []
        for row in result:
            dates.append({
                'date': row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]),
                'scheduleKey': row[1] or 'unknown',
                'itemCount': row[2] or 0,
            })
        
        return {'dates': dates}
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_order(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Get a specific archived order by date."""
    route_number = payload.get('routeNumber')
    delivery_date = payload.get('deliveryDate')
    
    if not route_number or not delivery_date:
        return {'error': 'Missing routeNumber or deliveryDate'}
    
    try:
        # Get order header
        order_row = conn.execute("""
            SELECT order_id, schedule_key, order_date, total_units, store_count
            FROM orders_historical
            WHERE route_number = ? AND delivery_date = ?
            LIMIT 1
        """, [route_number, delivery_date]).fetchone()
        
        if not order_row:
            return {'error': f'No order found for {delivery_date}'}
        
        order_id = order_row[0]
        
        # Get line items grouped by store
        items = conn.execute("""
            SELECT store_id, store_name, sap, quantity
            FROM order_line_items
            WHERE order_id = ?
            ORDER BY store_id, sap
        """, [order_id]).fetchall()
        
        # Group by store
        stores = {}
        for row in items:
            store_id = row[0]
            if store_id not in stores:
                stores[store_id] = {
                    'storeId': store_id,
                    'storeName': row[1],
                    'items': []
                }
            stores[store_id]['items'].append({
                'sap': row[2],
                'quantity': row[3],
            })
        
        # Convert dates to strings for Firestore
        order_date = order_row[2]
        if hasattr(order_date, 'isoformat'):
            order_date = order_date.isoformat()
        
        return {
            'order': {
                'id': order_id,
                'routeNumber': route_number,
                'scheduleKey': order_row[1],
                'deliveryDate': delivery_date,
                'orderDate': order_date,
                'totalUnits': order_row[3],
                'storeCount': order_row[4],
                'stores': list(stores.values()),
            }
        }
        
    except Exception as e:
        return {'error': str(e)}


def handle_check_route_synced(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Check if a route is synced in DuckDB."""
    route_number = payload.get('routeNumber')
    
    if not route_number:
        return {'error': 'Missing routeNumber'}
    
    try:
        result = conn.execute("""
            SELECT sync_status, first_synced_at, orders_count
            FROM routes_synced
            WHERE route_number = ?
        """, [route_number]).fetchone()
        
        if result:
            return {
                'synced': result[0] == 'ready',
                'status': result[0],
                'firstSyncedAt': result[1].isoformat() if result[1] else None,
                'ordersCount': result[2],
            }
        else:
            return {'synced': False, 'status': 'not_found'}
            
    except Exception as e:
        return {'error': str(e)}


def handle_get_delivery_manifest(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Get delivery manifest for a specific date.
    
    Returns all items being delivered on a date, grouped by store,
    including case splits from different orders.
    """
    route_number = payload.get('routeNumber')
    delivery_date = payload.get('deliveryDate')
    store_id = payload.get('storeId')  # Optional - filter to single store
    
    if not route_number or not delivery_date:
        return {'error': 'Missing routeNumber or deliveryDate'}
    
    try:
        # Build query
        if store_id:
            # Single store manifest
            result = conn.execute("""
                SELECT 
                    da.store_id,
                    da.store_name,
                    da.sap,
                    da.quantity,
                    da.source_order_id,
                    da.source_order_date,
                    da.is_case_split,
                    pc.full_name as product_name,
                    pc.case_pack
                FROM delivery_allocations da
                LEFT JOIN product_catalog pc 
                    ON da.sap = pc.sap AND da.route_number = pc.route_number
                WHERE da.route_number = ?
                  AND da.delivery_date = ?
                  AND da.store_id = ?
                ORDER BY pc.full_name, da.sap
            """, [route_number, delivery_date, store_id]).fetchall()
        else:
            # Full manifest for the date
            result = conn.execute("""
                SELECT 
                    da.store_id,
                    da.store_name,
                    da.sap,
                    da.quantity,
                    da.source_order_id,
                    da.source_order_date,
                    da.is_case_split,
                    pc.full_name as product_name,
                    pc.case_pack
                FROM delivery_allocations da
                LEFT JOIN product_catalog pc 
                    ON da.sap = pc.sap AND da.route_number = pc.route_number
                WHERE da.route_number = ?
                  AND da.delivery_date = ?
                ORDER BY da.store_name, pc.full_name, da.sap
            """, [route_number, delivery_date]).fetchall()
        
        # Group by store
        stores = {}
        for row in result:
            sid = row[0]
            if sid not in stores:
                stores[sid] = {
                    'storeId': sid,
                    'storeName': row[1],
                    'items': [],
                    'totalUnits': 0,
                    'caseSplitCount': 0,
                }
            
            # Convert dates to strings
            source_order_date = row[5]
            if hasattr(source_order_date, 'isoformat'):
                source_order_date = source_order_date.isoformat()
            
            item = {
                'sap': row[2],
                'productName': row[7] or row[2],
                'quantity': row[3],
                'casePack': row[8] or 1,
                'sourceOrderId': row[4],
                'sourceOrderDate': source_order_date,
                'isCaseSplit': row[6] or False,
            }
            
            stores[sid]['items'].append(item)
            stores[sid]['totalUnits'] += row[3]
            if row[6]:  # is_case_split
                stores[sid]['caseSplitCount'] += 1
        
        # Add computed fields
        for store in stores.values():
            store['totalItems'] = len(store['items'])
            store['hasCaseSplits'] = store['caseSplitCount'] > 0
        
        return {
            'manifest': {
                'routeNumber': route_number,
                'deliveryDate': delivery_date,
                'stores': list(stores.values()),
                'totalStores': len(stores),
                'totalUnits': sum(s['totalUnits'] for s in stores.values()),
                'totalItems': sum(s['totalItems'] for s in stores.values()),
            }
        }
        
    except Exception as e:
        return {'error': str(e)}


def handle_get_store_delivery(conn: duckdb.DuckDBPyConnection, payload: Dict) -> Dict:
    """Get delivery for a specific store on a date (for invoice pre-fill)."""
    route_number = payload.get('routeNumber')
    delivery_date = payload.get('deliveryDate')
    store_id = payload.get('storeId')
    
    if not all([route_number, delivery_date, store_id]):
        return {'error': 'Missing routeNumber, deliveryDate, or storeId'}
    
    # Reuse manifest handler with store filter
    result = handle_get_delivery_manifest(conn, {
        'routeNumber': route_number,
        'deliveryDate': delivery_date,
        'storeId': store_id,
    })
    
    if 'error' in result:
        return result
    
    # Extract single store from manifest
    stores = result.get('manifest', {}).get('stores', [])
    if stores:
        return {'storeDelivery': stores[0]}
    else:
        return {'storeDelivery': None, 'message': f'No delivery found for store {store_id} on {delivery_date}'}


# =============================================================================
# Request Processor
# =============================================================================

def process_request(conn: duckdb.DuckDBPyConnection, db: firestore.Client, 
                   request_type: str, payload: Dict) -> Dict:
    """Route request to appropriate handler."""
    handlers = {
        'query': lambda: handle_query(conn, payload),
        'write': lambda: handle_write(conn, payload),
        'sync_order': lambda: handle_sync_order(conn, db, payload),
        'get_historical_shares': lambda: handle_get_historical_shares(conn, payload),
        'get_archived_dates': lambda: handle_get_archived_dates(conn, payload),
        'get_order': lambda: handle_get_order(conn, payload),
        'check_route_synced': lambda: handle_check_route_synced(conn, payload),
        'get_delivery_manifest': lambda: handle_get_delivery_manifest(conn, payload),
        'get_store_delivery': lambda: handle_get_store_delivery(conn, payload),
    }
    
    handler = handlers.get(request_type)
    if not handler:
        return {'error': f'Unknown request type: {request_type}'}
    
    return handler()


# =============================================================================
# Main Listener
# =============================================================================

def watch_requests(sa_path: str, duckdb_path: str):
    """Watch dbRequests collection and process requests.
    
    Uses connect-per-request pattern to avoid holding DuckDB lock permanently.
    This allows other processes to read the database between requests.
    """
    print(f"\n🗄️  Database Manager Service")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Database: {duckdb_path}")
    print(f"   Mode: Connect-per-request (allows concurrent reads)")
    print(f"   Watching: dbRequests/*")
    print(f"\n   Press Ctrl+C to stop\n")
    
    db = get_firestore_client(sa_path)
    
    # Ensure schema exists (open, create, close)
    print("   Initializing schema...")
    init_conn = get_duckdb_conn(duckdb_path)
    from db_schema import create_schema
    create_schema(init_conn)
    init_conn.close()
    print("   ✅ Schema ready, connection released")
    
    requests_col = db.collection("dbRequests")
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle collection changes."""
        for change in changes:
            if change.type.name not in ('ADDED', 'MODIFIED'):
                continue
            
            doc = change.document
            data = doc.to_dict() or {}
            
            # Skip if not pending
            if data.get("status") != "pending":
                continue
            
            request_id = data.get("requestId", doc.id)
            request_type = data.get("type", "")
            payload = data.get("payload", {})
            
            print(f"  📥 Request: {request_type} ({request_id[:20]}...)")
            
            # Claim the request
            try:
                doc.reference.update({
                    "status": "processing",
                    "workerId": WORKER_ID,
                    "processingAt": firestore.SERVER_TIMESTAMP,
                })
            except Exception as e:
                print(f"     ⚠️  Could not claim: {e}")
                continue
            
            # Process the request with a fresh connection
            start_time = time.time()
            conn = None
            try:
                # Open connection for this request only
                conn = get_duckdb_conn(duckdb_path)
                result = process_request(conn, db, request_type, payload)
                elapsed = time.time() - start_time
                
                # Write result
                doc.reference.update({
                    "status": "completed",
                    "result": result,
                    "completedAt": firestore.SERVER_TIMESTAMP,
                    "elapsedMs": int(elapsed * 1000),
                })
                
                if 'error' in result:
                    print(f"     ❌ Error: {result['error'][:100]}")
                else:
                    print(f"     ✅ Done in {elapsed*1000:.0f}ms")
                    
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"     ❌ Exception: {e}")
                
                doc.reference.update({
                    "status": "error",
                    "error": str(e),
                    "completedAt": firestore.SERVER_TIMESTAMP,
                    "elapsedMs": int(elapsed * 1000),
                })
            finally:
                # Always close connection after request
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    # Start real-time listener
    watcher = requests_col.on_snapshot(on_snapshot)
    
    # Setup graceful shutdown handlers
    def shutdown_handler(signum, frame):
        global _shutdown_requested
        _shutdown_requested = True
        signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
        print(f"\n\n👋 Received {signal_name} - stopping DB Manager gracefully...")
    
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    try:
        while not _shutdown_requested:
            time.sleep(1)  # Check more frequently for shutdown signal
    except KeyboardInterrupt:
        pass
    
    # Graceful cleanup
    print("   Unsubscribing from Firebase...")
    try:
        watcher.unsubscribe()
    except Exception as e:
        print(f"   Warning: unsubscribe error: {e}")
    
    print("   DB Manager stopped.\n")


# =============================================================================
# Polling Mode (Low CPU Alternative)
# =============================================================================

def watch_requests_polling(sa_path: str, duckdb_path: str, poll_interval: float = 0.5):
    """Poll for pending requests instead of using on_snapshot.
    
    This avoids gRPC streaming overhead and reduces idle CPU significantly.
    Trade-off: slightly higher latency (up to poll_interval).
    
    Args:
        sa_path: Path to Firebase service account JSON
        duckdb_path: Path to DuckDB database file
        poll_interval: Seconds between polls (default 0.5s)
    """
    print(f"\n🗄️  Database Manager Service (Polling Mode)")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Database: {duckdb_path}")
    print(f"   Poll interval: {poll_interval}s")
    print(f"   Watching: dbRequests/* (polling)")
    print(f"\n   Press Ctrl+C to stop\n")
    
    db = get_firestore_client(sa_path)
    
    # Ensure schema exists
    print("   Initializing schema...")
    init_conn = get_duckdb_conn(duckdb_path)
    from db_schema import create_schema
    create_schema(init_conn)
    init_conn.close()
    print("   ✅ Schema ready")
    
    requests_col = db.collection("dbRequests")
    
    # Setup graceful shutdown
    def shutdown_handler(signum, frame):
        global _shutdown_requested
        _shutdown_requested = True
        signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
        print(f"\n\n👋 Received {signal_name} - stopping DB Manager gracefully...")
    
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    try:
        while not _shutdown_requested:
            try:
                # Query for pending requests
                pending_docs = requests_col.where("status", "==", "pending").limit(10).stream()
                
                for doc in pending_docs:
                    if _shutdown_requested:
                        break
                    
                    data = doc.to_dict() or {}
                    request_id = data.get("requestId", doc.id)
                    request_type = data.get("type", "")
                    payload = data.get("payload", {})
                    
                    print(f"  📥 Request: {request_type} ({request_id[:20]}...)")
                    
                    # Claim the request
                    try:
                        doc.reference.update({
                            "status": "processing",
                            "workerId": WORKER_ID,
                            "processingAt": firestore.SERVER_TIMESTAMP,
                        })
                    except Exception as e:
                        print(f"     ⚠️  Could not claim: {e}")
                        continue
                    
                    # Process with fresh connection
                    start_time = time.time()
                    conn = None
                    try:
                        conn = get_duckdb_conn(duckdb_path)
                        result = process_request(conn, db, request_type, payload)
                        elapsed = time.time() - start_time
                        
                        doc.reference.update({
                            "status": "completed",
                            "result": result,
                            "completedAt": firestore.SERVER_TIMESTAMP,
                            "elapsedMs": int(elapsed * 1000),
                        })
                        
                        if 'error' in result:
                            print(f"     ❌ Error: {result['error'][:100]}")
                        else:
                            print(f"     ✅ Done in {elapsed*1000:.0f}ms")
                            
                    except Exception as e:
                        elapsed = time.time() - start_time
                        print(f"     ❌ Exception: {e}")
                        
                        doc.reference.update({
                            "status": "error",
                            "error": str(e),
                            "completedAt": firestore.SERVER_TIMESTAMP,
                            "elapsedMs": int(elapsed * 1000),
                        })
                    finally:
                        if conn:
                            try:
                                conn.close()
                            except Exception:
                                pass
                
            except Exception as e:
                print(f"  ⚠️  Poll error: {e}")
            
            # Sleep between polls
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        pass
    
    print("   DB Manager stopped.\n")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Database Manager Service - centralized DuckDB access",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    parser.add_argument('--duckdb', required=True, help='Path to DuckDB database')
    parser.add_argument('--mode', choices=['listener', 'polling'], default='polling',
                       help='Mode: listener (real-time, high CPU) or polling (low CPU, default)')
    parser.add_argument('--poll-interval', type=float, default=0.5,
                       help='Poll interval in seconds (only for polling mode)')
    
    args = parser.parse_args()
    
    if args.mode == 'polling':
        watch_requests_polling(args.serviceAccount, args.duckdb, args.poll_interval)
    else:
        watch_requests(args.serviceAccount, args.duckdb)

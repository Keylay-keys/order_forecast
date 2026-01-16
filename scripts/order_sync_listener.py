"""Unified order sync listener - watches ALL orders across all routes.

Multi-user support: Automatically syncs new routes when their first order appears.
Uses DBClient to communicate with DB Manager (no direct DuckDB access).

Flow:
1. Watches `/orders` collection for all users
2. On new order: sends sync request to DB Manager
3. DB Manager handles all database operations
4. On finalized order: syncs order data for retraining

Usage:
    python scripts/order_sync_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import socket
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List

from google.cloud import firestore  # type: ignore

# Import DBClient for database operations
try:
    from .db_client import DBClient
except ImportError:
    from db_client import DBClient

# US Holiday weeks (start_date, end_date, name) - weeks containing major holidays
# Orders placed during these weeks should be marked as is_holiday_week=TRUE
# This prevents them from being counted toward training minimums
HOLIDAY_WEEKS_2024 = [
    ('2024-11-25', '2024-12-01', 'Thanksgiving'),  # Thanksgiving Nov 28
    ('2024-12-23', '2024-12-29', 'Christmas'),      # Christmas Dec 25
]

HOLIDAY_WEEKS_2025 = [
    ('2025-11-24', '2025-11-30', 'Thanksgiving'),  # Thanksgiving Nov 27
    ('2025-12-22', '2025-12-28', 'Christmas'),      # Christmas Dec 25
]

HOLIDAY_WEEKS_2026 = [
    ('2025-11-23', '2025-11-29', 'Thanksgiving'),  # Thanksgiving Nov 26
    ('2025-12-21', '2025-12-27', 'Christmas'),      # Christmas Dec 25
]

ALL_HOLIDAY_WEEKS = HOLIDAY_WEEKS_2024 + HOLIDAY_WEEKS_2025 + HOLIDAY_WEEKS_2026


def is_holiday_week(order_date_str: str) -> tuple[bool, str]:
    """Check if an order date falls within a holiday week.
    
    Args:
        order_date_str: Order date in YYYY-MM-DD or MM/DD/YYYY format
    
    Returns:
        (is_holiday, holiday_name) - True if in holiday week, with the holiday name
    """
    if not order_date_str:
        return False, ''
    
    try:
        if '/' in order_date_str:
            order_date = datetime.strptime(order_date_str, '%m/%d/%Y')
        else:
            order_date = datetime.fromisoformat(order_date_str.replace('Z', '+00:00'))
            if hasattr(order_date, 'date'):
                order_date = datetime.combine(order_date.date(), datetime.min.time())
        
        order_str = order_date.strftime('%Y-%m-%d')
        
        for start, end, name in ALL_HOLIDAY_WEEKS:
            if start <= order_str <= end:
                return True, name
        
        return False, ''
    except (ValueError, TypeError):
        return False, ''

# Worker ID for this instance
WORKER_ID = f"order-sync-{socket.gethostname()}-{__import__('os').getpid()}"

# Cache for store ID aliases (loaded once per route)
_store_alias_cache: Dict[str, Dict[str, str]] = {}  # route_number -> {alias_id: canonical_id}


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


# =============================================================================
# Store ID Alias Resolution
# =============================================================================

def load_store_aliases(db_client: DBClient, route_number: str) -> Dict[str, str]:
    """Load store ID aliases for a route from DuckDB.
    
    Returns:
        Dict mapping alias_id -> canonical_id
    """
    global _store_alias_cache
    
    # Check cache first
    if route_number in _store_alias_cache:
        return _store_alias_cache[route_number]
    
    try:
        result = db_client.query("""
            SELECT alias_id, canonical_id 
            FROM store_id_aliases 
            WHERE route_number = ?
        """, [route_number])
        
        aliases = {}
        for row in result.get('rows', []):
            aliases[row['alias_id']] = row['canonical_id']
        
        # Cache it
        _store_alias_cache[route_number] = aliases
        if aliases:
            print(f"  📋 Loaded {len(aliases)} store ID aliases for route {route_number}")
        return aliases
        
    except Exception as e:
        print(f"  ⚠️ Error loading store aliases: {e}")
        return {}


def resolve_store_id(db_client: DBClient, route_number: str, store_id: str) -> str:
    """Resolve a store_id to its canonical form using the alias table.
    
    If the store_id is found in the alias table, returns the canonical_id.
    Otherwise, returns the original store_id unchanged.
    """
    aliases = load_store_aliases(db_client, route_number)
    canonical = aliases.get(store_id)
    if canonical:
        return canonical
    return store_id


def clear_alias_cache(route_number: str = None):
    """Clear the store alias cache. Call when aliases are updated."""
    global _store_alias_cache
    if route_number:
        _store_alias_cache.pop(route_number, None)
    else:
        _store_alias_cache.clear()


# =============================================================================
# Route Sync Status (via DBClient)
# =============================================================================

def is_route_synced(client: DBClient, route_number: str) -> bool:
    """Check if a route has been synced to DuckDB."""
    try:
        result = client.check_route_synced(route_number)
        return result.get('synced', False)
    except Exception as e:
        print(f"     ⚠️  Error checking sync status: {e}")
        return False


def mark_route_syncing(client: DBClient, route_number: str, user_id: str, trigger_order_id: str):
    """Mark a route as currently syncing."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.write("""
            INSERT INTO routes_synced (
                route_number, user_id, first_synced_at, last_synced_at,
                worker_id, triggered_by, trigger_order_id, sync_status
            ) VALUES (?, ?, ?, ?, ?, 'first_order', ?, 'syncing')
            ON CONFLICT (route_number) DO UPDATE SET
                last_synced_at = EXCLUDED.last_synced_at,
                worker_id = EXCLUDED.worker_id,
                sync_status = 'syncing'
        """, [route_number, user_id, now, now, WORKER_ID, trigger_order_id])
    except Exception as e:
        print(f"     ⚠️  Error marking route syncing: {e}")


def mark_route_ready(client: DBClient, route_number: str, stores: int, products: int, orders: int):
    """Mark a route as synced and ready."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.write("""
            UPDATE routes_synced SET
                sync_status = 'ready',
                last_synced_at = ?,
                stores_count = ?,
                products_count = ?,
                orders_count = ?,
                schedules_synced = TRUE
            WHERE route_number = ?
        """, [now, stores, products, orders, route_number])
    except Exception as e:
        print(f"     ⚠️  Error marking route ready: {e}")


# =============================================================================
# Firebase Sync Functions (via DBClient)
# =============================================================================

def sync_stores_from_firebase(fb_client: firestore.Client, db_client: DBClient, route_number: str) -> int:
    """Sync stores from Firebase to DuckDB via DB Manager."""
    stores_ref = fb_client.collection('routes').document(route_number).collection('stores')
    stores = stores_ref.stream()
    
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    
    for store_doc in stores:
        data = store_doc.to_dict() or {}
        store_id = store_doc.id
        
        try:
            db_client.write("""
                INSERT INTO stores (store_id, route_number, store_name, store_number, is_active, synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_id) DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    store_number = EXCLUDED.store_number,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
            """, [
                store_id,
                route_number,
                data.get('name', ''),
                data.get('storeNumber', ''),
                data.get('isActive', True),
                now
            ])
            count += 1
        except Exception as e:
            print(f"     ⚠️  Error syncing store {store_id}: {e}")
    
    return count


def sync_products_from_firebase(fb_client: firestore.Client, db_client: DBClient, route_number: str) -> int:
    """Sync products from Firebase to DuckDB via DB Manager."""
    products_ref = fb_client.collection('routes').document(route_number).collection('products')
    products = products_ref.stream()
    
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    
    for prod_doc in products:
        data = prod_doc.to_dict() or {}
        sap = data.get('sap', prod_doc.id)
        
        try:
            db_client.write("""
                INSERT INTO product_catalog (sap, route_number, full_name, short_name, brand, category, case_pack, is_active, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (sap, route_number) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    short_name = EXCLUDED.short_name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    case_pack = EXCLUDED.case_pack,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
            """, [
                sap,
                route_number,
                data.get('name', data.get('fullName', '')),
                data.get('shortName', ''),
                data.get('brand', ''),
                data.get('category', ''),
                data.get('casePack', 1),
                data.get('isActive', True),
                now
            ])
            count += 1
        except Exception as e:
            print(f"     ⚠️  Error syncing product {sap}: {e}")
    
    return count


def sync_schedules_from_firebase(fb_client: firestore.Client, db_client: DBClient, route_number: str, user_id: str) -> int:
    """Sync order schedules from Firebase to DuckDB via DB Manager."""
    user_ref = fb_client.collection('users').document(user_id)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        return 0
    
    user_data = user_doc.to_dict() or {}
    
    # Try the correct nested path first (userSettings.notifications.scheduling.orderCycles)
    cycles = (
        user_data.get('userSettings', {})
        .get('notifications', {})
        .get('scheduling', {})
        .get('orderCycles', [])
    )
    
    # Fallback to old path for backwards compatibility
    if not cycles:
        settings = user_data.get('settings', {})
        cycles = settings.get('orderCycles', [])
    
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    day_names = {1: 'monday', 2: 'tuesday', 3: 'wednesday', 4: 'thursday', 
                 5: 'friday', 6: 'saturday', 0: 'sunday'}
    
    for i, cycle in enumerate(cycles):
        order_day = cycle.get('orderDay', 1)
        load_day = cycle.get('loadDay', 3)
        delivery_day = cycle.get('deliveryDay', 4)
        # schedule_key based on ORDER day (user's mental model)
        schedule_key = day_names.get(order_day, 'unknown')
        
        schedule_id = f"{route_number}-cycle-{i}"
        
        try:
            db_client.write("""
                INSERT INTO user_schedules (id, route_number, user_id, order_day, load_day, delivery_day, schedule_key, is_active, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    order_day = EXCLUDED.order_day,
                    load_day = EXCLUDED.load_day,
                    delivery_day = EXCLUDED.delivery_day,
                    schedule_key = EXCLUDED.schedule_key,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
            """, [
                schedule_id,
                route_number,
                user_id,
                order_day,
                load_day,
                delivery_day,
                schedule_key,
                True,
                now
            ])
            count += 1
        except Exception as e:
            print(f"     ⚠️  Error syncing schedule {schedule_id}: {e}")
    
    return count


def sync_full_route(fb_client: firestore.Client, db_client: DBClient, 
                   route_number: str, user_id: str, trigger_order_id: str) -> bool:
    """Perform a full sync of a route from Firebase to DuckDB."""
    print(f"  🔄 Syncing route {route_number}...")
    
    try:
        # Mark as syncing
        mark_route_syncing(db_client, route_number, user_id, trigger_order_id)
        
        # Sync each entity type
        stores_count = sync_stores_from_firebase(fb_client, db_client, route_number)
        print(f"     ✓ Synced {stores_count} stores")
        
        products_count = sync_products_from_firebase(fb_client, db_client, route_number)
        print(f"     ✓ Synced {products_count} products")
        
        schedules_count = sync_schedules_from_firebase(fb_client, db_client, route_number, user_id)
        print(f"     ✓ Synced {schedules_count} schedules")
        
        # Mark as ready
        mark_route_ready(db_client, route_number, stores_count, products_count, 0)
        
        print(f"  ✅ Route {route_number} synced and ready!")
        return True
        
    except Exception as e:
        print(f"  ❌ Sync failed: {e}")
        return False


def update_firebase_sync_status(fb_client: firestore.Client, route_number: str, synced: bool):
    """Update sync status in Firebase for the app to see."""
    try:
        fb_client.collection('routes').document(route_number).collection('backend_status').document('sync').set({
            'synced': synced,
            'syncedAt': firestore.SERVER_TIMESTAMP,
            'workerId': WORKER_ID,
        }, merge=True)
    except Exception as e:
        print(f"     ⚠️  Failed to update Firebase status: {e}")


# =============================================================================
# Order Event Handlers
# =============================================================================

def handle_new_order(fb_client: firestore.Client, db_client: DBClient, order_id: str, data: dict):
    """Handle a new order being created."""
    route_number = data.get('routeNumber')
    user_id = data.get('userId')
    status = data.get('status')
    
    if not route_number:
        return
    
    print(f"  📦 New order: {order_id} (route: {route_number}, status: {status})")
    
    # Check if this route is already synced
    if not is_route_synced(db_client, route_number):
        print(f"  🆕 First order for route {route_number} - syncing...")
        if sync_full_route(fb_client, db_client, route_number, user_id, order_id):
            update_firebase_sync_status(fb_client, route_number, True)
    else:
        print(f"     Route {route_number} already synced")


def handle_finalized_order(fb_client: firestore.Client, db_client: DBClient, order_id: str, data: dict):
    """Handle an order being finalized - sync it to DuckDB and regenerate forecasts."""
    route_number = data.get('routeNumber')
    user_id = data.get('userId')
    schedule_key = data.get('scheduleKey')
    
    if not route_number:
        return
    
    print(f"  ✅ Order finalized: {order_id}")
    
    # Use DB Manager's sync_order handler
    try:
        result = db_client.sync_order(order_id, route_number)
        if 'error' in result:
            print(f"     ⚠️  Sync error: {result['error']}")
        else:
            print(f"     Synced {result.get('totalUnits', 0)} units across {result.get('storeCount', 0)} stores")
            corrections_count = result.get('correctionsExtracted', 0)
            if corrections_count > 0:
                print(f"     📊 Extracted {corrections_count} corrections for ML training")
            create_delivery_allocations(db_client, fb_client, order_id, route_number, data)
            link_promos_for_order(db_client, order_id, route_number, data)
            
            # Regenerate forecasts for all upcoming deliveries (incorporates new corrections)
            regenerate_forecasts_after_finalization(fb_client, db_client, route_number, user_id)
    except Exception as e:
        print(f"     ❌ Error syncing order: {e}")


def regenerate_forecasts_after_finalization(
    fb_client: firestore.Client,
    db_client: DBClient,
    route_number: str,
    user_id: str,
) -> None:
    """Regenerate forecasts for all upcoming deliveries after an order is finalized.
    
    This ensures the latest corrections are incorporated into upcoming forecasts.
    Deletes existing cached forecasts and regenerates them.
    """
    print(f"  🔄 Regenerating forecasts with latest corrections...")
    
    # First, clean up old forecasts (past delivery dates)
    cleaned = cleanup_old_forecasts(fb_client, route_number)
    if cleaned > 0:
        print(f"     🧹 Cleaned up {cleaned} past forecasts")
    
    try:
        # Get user's order schedules (tries DuckDB first, then Firebase)
        schedules = get_user_schedules(fb_client, db_client, route_number)
        if not schedules:
            print(f"     ⚠️  No schedules found for route {route_number}")
            return
        
        # Calculate next delivery dates for each schedule
        upcoming_deliveries = []
        today = datetime.now(timezone.utc).date()
        
        for schedule in schedules:
            order_day = schedule.get('orderDay', 1)  # 0=Sun, 1=Mon, etc.
            delivery_day = schedule.get('deliveryDay', 4)
            
            # Convert to schedule_key (based on order day)
            day_names = {0: 'sunday', 1: 'monday', 2: 'tuesday', 3: 'wednesday',
                        4: 'thursday', 5: 'friday', 6: 'saturday'}
            schedule_key = day_names.get(order_day, 'unknown')
            
            # Find next delivery date
            next_delivery = get_next_delivery_date(today, delivery_day)
            upcoming_deliveries.append({
                'delivery_date': next_delivery.isoformat(),
                'schedule_key': schedule_key,
            })
        
        if not upcoming_deliveries:
            print(f"     No upcoming deliveries to regenerate")
            return
        
        # Delete existing forecasts for these dates
        deleted = delete_forecasts_for_dates(fb_client, route_number, upcoming_deliveries)
        if deleted > 0:
            print(f"     🗑️  Deleted {deleted} stale forecasts")
        
        # Import and run forecast generation
        try:
            from forecast_engine import ForecastConfig, generate_forecast
            
            for delivery in upcoming_deliveries:
                delivery_date = delivery['delivery_date']
                schedule_key = delivery['schedule_key']
                
                print(f"     🔮 Generating forecast for {delivery_date} ({schedule_key})...")
                
                # Find service account path (from environment or default)
                import os
                sa_path = os.environ.get(
                    'FIREBASE_SA_PATH',
                    '/Users/kylemacmini/Desktop/routespark/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'
                )
                
                config = ForecastConfig(
                    route_number=route_number,
                    delivery_date=delivery_date,
                    schedule_key=schedule_key,
                    service_account=sa_path,
                    since_days=365,
                    round_cases=True,
                    ttl_days=7,
                )
                
                forecast = generate_forecast(config)
                print(f"     ✅ Forecast {forecast.forecast_id}: {len(forecast.items)} items")
                
        except ImportError as e:
            print(f"     ⚠️  Could not import forecast_engine: {e}")
        except Exception as e:
            print(f"     ❌ Forecast generation error: {e}")
            
    except Exception as e:
        print(f"     ❌ Error regenerating forecasts: {e}")


def get_user_schedules(fb_client: firestore.Client, db_client: DBClient, route_number: str) -> List[Dict]:
    """Get user's order schedules - tries DuckDB first, falls back to Firebase.
    
    Uses schedule_utils.get_order_cycles which handles the correct Firebase path
    (userSettings.notifications.scheduling.orderCycles) with fallback.
    """
    try:
        from schedule_utils import get_order_cycles
        return get_order_cycles(fb_client, route_number, db_client=db_client)
    except ImportError:
        pass
    
    # Fallback: try to get from DuckDB directly
    if db_client:
        try:
            result = db_client.query("""
                SELECT order_day, load_day, delivery_day
                FROM user_schedules
                WHERE route_number = ? AND is_active = TRUE
            """, [route_number])
            
            cycles = []
            for row in result.get('rows', []):
                cycles.append({
                    'orderDay': row['order_day'],
                    'loadDay': row['load_day'],
                    'deliveryDay': row['delivery_day'],
                })
            if cycles:
                return cycles
        except Exception as e:
            print(f"     ⚠️  Error getting schedules from DuckDB: {e}")
    
    return []


def get_next_delivery_date(from_date, delivery_weekday: int) -> datetime:
    """Get the next occurrence of a delivery weekday.
    
    Args:
        from_date: Date to start searching from
        delivery_weekday: Firebase weekday (0=Sun, 1=Mon, ..., 6=Sat)
    
    Returns:
        datetime of next delivery
    """
    # Convert Firebase weekday (0=Sun) to Python weekday (0=Mon)
    python_weekday = (delivery_weekday - 1) % 7
    
    # Calculate days until next occurrence
    days_ahead = python_weekday - from_date.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    
    next_date = from_date + timedelta(days=days_ahead)
    return datetime.combine(next_date, datetime.min.time(), tzinfo=timezone.utc)


def delete_forecasts_for_dates(
    fb_client: firestore.Client,
    route_number: str,
    deliveries: List[Dict],
) -> int:
    """Delete cached forecasts for specific delivery dates.
    
    Returns number of forecasts deleted.
    """
    deleted = 0
    try:
        forecasts_ref = fb_client.collection('forecasts').document(route_number).collection('cached')
        
        for doc in forecasts_ref.stream():
            data = doc.to_dict()
            doc_delivery = data.get('deliveryDate', '')
            
            # Check if this forecast matches any of our target deliveries
            for target in deliveries:
                if doc_delivery == target['delivery_date']:
                    doc.reference.delete()
                    deleted += 1
                    break
                    
    except Exception as e:
        print(f"     ⚠️  Error deleting forecasts: {e}")
    
    return deleted


def cleanup_old_forecasts(fb_client: firestore.Client, route_number: str) -> int:
    """Delete forecasts for past delivery dates.
    
    Called periodically or after order finalization to keep Firebase clean.
    
    Returns number of forecasts deleted.
    """
    deleted = 0
    today = datetime.now(timezone.utc).date()
    
    try:
        forecasts_ref = fb_client.collection('forecasts').document(route_number).collection('cached')
        
        for doc in forecasts_ref.stream():
            data = doc.to_dict()
            delivery_str = data.get('deliveryDate', '')
            
            if not delivery_str:
                continue
            
            try:
                # Parse delivery date
                if isinstance(delivery_str, str):
                    delivery_date = datetime.fromisoformat(delivery_str.replace('Z', '+00:00')).date()
                else:
                    delivery_date = delivery_str.date() if hasattr(delivery_str, 'date') else None
                
                # Delete if in the past
                if delivery_date and delivery_date < today:
                    doc.reference.delete()
                    deleted += 1
                    
            except (ValueError, AttributeError):
                continue
                
    except Exception as e:
        print(f"  ⚠️  Error cleaning up forecasts: {e}")
    
    return deleted


# =============================================================================
# Delivery Allocation Helpers
# =============================================================================

# Day name to weekday number mapping
DAY_TO_WEEKDAY = {
    'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
    'friday': 4, 'saturday': 5, 'sunday': 6,
    'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6,
}


def get_store_delivery_days(fb_client: firestore.Client, route_number: str) -> Dict[str, List[int]]:
    """Load delivery days for all stores in a route from Firebase.
    
    Returns: {store_id: [weekday_numbers]} where 0=Monday, 6=Sunday
    """
    store_delivery_days = {}
    try:
        stores_ref = fb_client.collection('routes').document(route_number).collection('stores')
        for store_doc in stores_ref.stream():
            store_data = store_doc.to_dict() or {}
            delivery_days = store_data.get('deliveryDays', [])
            
            # Convert day names to weekday numbers
            weekdays = []
            for day in delivery_days:
                day_lower = day.lower().strip()
                if day_lower in DAY_TO_WEEKDAY:
                    weekdays.append(DAY_TO_WEEKDAY[day_lower])
            
            store_delivery_days[store_doc.id] = weekdays
    except Exception as e:
        print(f"     ⚠️  Error loading store delivery days: {e}")
    
    return store_delivery_days


def calculate_store_delivery_date(
    order_date_str: str,
    primary_delivery_date_str: str,
    store_delivery_weekdays: List[int],
) -> tuple[str, bool]:
    """Calculate the actual delivery date for a store based on their delivery schedule.
    
    Args:
        order_date_str: When the order was placed (YYYY-MM-DD)
        primary_delivery_date_str: The order's primary delivery date (YYYY-MM-DD)
        store_delivery_weekdays: List of weekday numbers (0=Mon, 6=Sun) when store gets deliveries
    
    Returns:
        (delivery_date, is_case_split) - the actual delivery date and whether it's a case split
    """
    # Parse dates
    try:
        if '/' in primary_delivery_date_str:
            # Handle MM/DD/YYYY format
            primary_delivery = datetime.strptime(primary_delivery_date_str, '%m/%d/%Y')
        else:
            primary_delivery = datetime.fromisoformat(primary_delivery_date_str)
        
        if order_date_str:
            if '/' in order_date_str:
                order_date = datetime.strptime(order_date_str, '%m/%d/%Y')
            else:
                order_date = datetime.fromisoformat(order_date_str)
        else:
            order_date = primary_delivery - timedelta(days=3)  # Assume 3 days lead time
    except (ValueError, TypeError):
        # Can't parse dates, use primary delivery date
        return primary_delivery_date_str, False
    
    primary_weekday = primary_delivery.weekday()
    
    # If store has no configured delivery days, use primary
    if not store_delivery_weekdays:
        return primary_delivery.strftime('%Y-%m-%d'), False
    
    # Check if primary delivery day matches store's delivery days
    if primary_weekday in store_delivery_weekdays:
        # Store can receive on the primary delivery day - no case split
        return primary_delivery.strftime('%Y-%m-%d'), False
    
    # Store can't receive on primary day - find next valid delivery day after order date
    # Look for the next occurrence of a valid delivery day
    # NOTE: is_case_split = FALSE here because this item is part of THIS order's normal flow.
    # Case splits are only marked when items from PREVIOUS orders are allocated to this delivery.
    for days_ahead in range(1, 8):
        check_date = order_date + timedelta(days=days_ahead)
        if check_date.weekday() in store_delivery_weekdays:
            return check_date.strftime('%Y-%m-%d'), False  # Not a case split - part of this order
    
    # Shouldn't happen, but fall back to primary
    return primary_delivery.strftime('%Y-%m-%d'), False


def create_delivery_allocations(
    db_client: DBClient,
    fb_client: firestore.Client,
    order_id: str,
    route_number: str,
    data: dict,
) -> None:
    """Create delivery allocation rows for a finalized order.
    
    Calculates the correct delivery date per store based on their configured
    delivery days. Marks items as case splits when delivery differs from primary.
    """
    delivery_date = data.get('expectedDeliveryDate') or data.get('deliveryDate')
    order_date = data.get('orderDate')
    stores = data.get('stores', []) or []

    if not delivery_date:
        print(f"     ⚠️  Skipping allocations: missing delivery date for order {order_id}")
        return

    # Load store delivery days from Firebase
    store_delivery_days = get_store_delivery_days(fb_client, route_number)
    
    inserted = 0
    case_splits = 0
    
    for store in stores:
        raw_store_id = store.get('id') or store.get('storeId') or ''
        store_name = store.get('name') or store.get('storeName') or ''
        items = store.get('items', []) or []

        if not raw_store_id:
            continue

        # Resolve store ID to canonical form (handles old/alias IDs)
        store_id = resolve_store_id(db_client, route_number, raw_store_id)
        if store_id != raw_store_id:
            print(f"     📍 Resolved store alias: {raw_store_id} -> {store_id}")

        # Calculate the actual delivery date for this store
        store_weekdays = store_delivery_days.get(store_id, []) or store_delivery_days.get(raw_store_id, [])
        actual_delivery_date, is_case_split = calculate_store_delivery_date(
            order_date,
            delivery_date,
            store_weekdays,
        )
        
        if is_case_split:
            case_splits += 1

        for item in items:
            sap = item.get('sap')
            qty = item.get('quantity', 0)
            if not sap or qty == 0:
                continue

            allocation_id = f"{order_id}-{store_id}-{sap}"
            try:
                db_client.write(
                    """
                    INSERT INTO delivery_allocations (
                        allocation_id, route_number, source_order_id, source_order_date,
                        sap, store_id, store_name, quantity, delivery_date, is_case_split
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (allocation_id) DO UPDATE SET
                        route_number = EXCLUDED.route_number,
                        source_order_date = EXCLUDED.source_order_date,
                        store_name = EXCLUDED.store_name,
                        quantity = EXCLUDED.quantity,
                        delivery_date = EXCLUDED.delivery_date,
                        is_case_split = EXCLUDED.is_case_split
                    """,
                    [
                        allocation_id,
                        route_number,
                        order_id,
                        order_date or delivery_date,
                        sap,
                        store_id,
                        store_name,
                        qty,
                        actual_delivery_date,
                        is_case_split,
                    ],
                )
                inserted += 1
            except Exception as e:
                print(f"     ⚠️  Failed to write allocation {allocation_id}: {e}")

    if inserted:
        msg = f"     ✓ Created/updated {inserted} delivery allocation rows"
        if case_splits > 0:
            msg += f" ({case_splits} stores with case splits)"
        print(msg)
    
    # Post-process: Mark case splits based on comparing source order dates
    # For each store+delivery_date, the LATEST source_order_date is the "primary"
    # Earlier source_order_dates are case splits
    mark_case_splits_for_route(db_client, route_number)


def mark_case_splits_for_route(db_client: DBClient, route_number: str) -> None:
    """Mark case splits by comparing source order dates per store+delivery_date.
    
    For each store+delivery_date combination:
    - The LATEST source_order_date is the "primary" order (is_case_split = FALSE)
    - Earlier source_order_dates are case splits (is_case_split = TRUE)
    """
    try:
        # Find all cases where a store+delivery_date has multiple source_order_dates
        result = db_client.query("""
            WITH order_dates_per_delivery AS (
                SELECT 
                    store_id,
                    delivery_date,
                    MAX(source_order_date) as primary_order_date
                FROM delivery_allocations
                WHERE route_number = ?
                GROUP BY store_id, delivery_date
            )
            SELECT store_id, delivery_date, primary_order_date
            FROM order_dates_per_delivery
        """, [route_number])
        
        if not result.get('rows'):
            return
        
        # For each store+delivery_date, set is_case_split based on source_order_date
        updates = 0
        for row in result['rows']:
            store_id = row['store_id']
            delivery_date = row['delivery_date']
            primary_order_date = row['primary_order_date']
            
            # Set is_case_split = FALSE for items from primary order date
            db_client.write("""
                UPDATE delivery_allocations
                SET is_case_split = FALSE
                WHERE route_number = ?
                  AND store_id = ?
                  AND delivery_date = ?
                  AND source_order_date = ?
            """, [route_number, store_id, delivery_date, primary_order_date])
            
            # Set is_case_split = TRUE for items from earlier order dates
            db_client.write("""
                UPDATE delivery_allocations
                SET is_case_split = TRUE
                WHERE route_number = ?
                  AND store_id = ?
                  AND delivery_date = ?
                  AND source_order_date < ?
            """, [route_number, store_id, delivery_date, primary_order_date])
            
            updates += 1
        
        if updates > 0:
            print(f"     ✓ Updated case split flags for {updates} store+delivery combinations")
    except Exception as e:
        print(f"     ⚠️  Error marking case splits: {e}")


# =============================================================================
# Promo linkage
# =============================================================================

def link_promos_for_order(db_client: DBClient, order_id: str, route_number: str, data: dict) -> None:
    """Link finalized order items to active promos (date-window + SAP) and compute ML features."""
    order_date = data.get('orderDate') or data.get('expectedDeliveryDate')
    stores = data.get('stores', []) or []

    if not order_date:
        print(f"     ⚠️  Skipping promo linkage: missing order date for {order_id}")
        return

    promo_items = []
    for store in stores:
        raw_store_id = store.get('id') or store.get('storeId') or ''
        # Resolve store ID to canonical form
        store_id = resolve_store_id(db_client, route_number, raw_store_id) if raw_store_id else ''
        
        for item in store.get('items', []) or []:
            sap = item.get('sap')
            qty = item.get('quantity', 0)
            cases = item.get('cases', 0)
            if not sap or qty == 0:
                continue
            promo_info = _find_promo_info(db_client, route_number, sap, order_date)
            if promo_info:
                # Get baseline quantity (avg non-promo orders for this store/sap)
                baseline = _get_baseline_quantity(db_client, route_number, store_id, sap)
                quantity_lift = qty / baseline if baseline and baseline > 0 else None
                
                promo_items.append({
                    'promo_id': promo_info['promo_id'],
                    'store_id': store_id,
                    'sap': sap,
                    'quantity': qty,
                    'cases': cases,
                    'promo_price': promo_info.get('special_price'),
                    'discount_percent': promo_info.get('discount_percent'),
                    'baseline_quantity': baseline,
                    'quantity_lift': quantity_lift,
                    'weeks_into_promo': promo_info.get('weeks_into_promo', 1),
                    'is_first_occurrence': promo_info.get('is_first_occurrence', True),
                })

    if not promo_items:
        return

    inserted = 0
    for entry in promo_items:
        try:
            db_client.write(
                """
                INSERT INTO promo_order_history (
                    id, route_number, promo_id, order_id, store_id, sap,
                    promo_price, discount_percent,
                    quantity_ordered, cases_ordered,
                    baseline_quantity, quantity_lift,
                    weeks_into_promo, is_first_promo_occurrence,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET
                    quantity_ordered = EXCLUDED.quantity_ordered,
                    cases_ordered = EXCLUDED.cases_ordered,
                    baseline_quantity = EXCLUDED.baseline_quantity,
                    quantity_lift = EXCLUDED.quantity_lift
                """,
                [
                    f"{order_id}-{entry['store_id']}-{entry['sap']}-{entry['promo_id']}",
                    route_number,
                    entry['promo_id'],
                    order_id,
                    entry['store_id'],
                    entry['sap'],
                    entry['promo_price'],
                    entry['discount_percent'],
                    entry['quantity'],
                    entry['cases'],
                    entry['baseline_quantity'],
                    entry['quantity_lift'],
                    entry['weeks_into_promo'],
                    entry['is_first_occurrence'],
                ],
            )
            inserted += 1
        except Exception as e:
            print(f"     ⚠️  Failed to write promo_order_history: {e}")

    if inserted:
        print(f"     ✓ Linked {inserted} promo items for order {order_id}")


def _find_promo_info(db_client: DBClient, route_number: str, sap: str, order_date: str) -> dict | None:
    """Lookup promo info for sap/date window including ML features."""
    try:
        result = db_client.query(
            """
            SELECT 
                pi.promo_id,
                pi.special_price,
                pi.discount_percent,
                ph.start_date,
                ph.times_seen
            FROM promo_items pi
            JOIN promo_history ph ON pi.promo_id = ph.promo_id
            WHERE pi.sap = ?
              AND ph.route_number = ?
              AND DATE(?) BETWEEN ph.start_date AND ph.end_date
            LIMIT 1
            """,
            [sap, route_number, order_date],
        )
        rows = result.get('rows') if result else []
        if rows:
            row = rows[0]
            promo_id = row.get('promo_id')
            start_date = row.get('start_date')
            times_seen = row.get('times_seen', 0)
            
            # Calculate weeks into promo
            weeks_into = 1
            if start_date and order_date:
                try:
                    from datetime import datetime
                    if isinstance(start_date, str):
                        start_dt = datetime.fromisoformat(start_date.replace('/', '-'))
                    else:
                        start_dt = start_date
                    if isinstance(order_date, str):
                        if '/' in order_date:
                            order_dt = datetime.strptime(order_date, '%m/%d/%Y')
                        else:
                            order_dt = datetime.fromisoformat(order_date)
                    else:
                        order_dt = order_date
                    weeks_into = max(1, ((order_dt - start_dt).days // 7) + 1)
                except:
                    pass
            
            return {
                'promo_id': promo_id,
                'special_price': row.get('special_price'),
                'discount_percent': row.get('discount_percent'),
                'weeks_into_promo': weeks_into,
                'is_first_occurrence': times_seen == 0,
            }
    except Exception as e:
        print(f"     ⚠️  Promo lookup failed for SAP {sap}: {e}")
    return None


def _get_baseline_quantity(db_client: DBClient, route_number: str, store_id: str, sap: str) -> float | None:
    """Get baseline (non-promo) average quantity for this store/sap."""
    try:
        result = db_client.query(
            """
            SELECT AVG(oli.quantity) as avg_qty
            FROM order_line_items oli
            LEFT JOIN promo_order_history poh 
                ON oli.order_id = poh.order_id 
                AND oli.store_id = poh.store_id 
                AND oli.sap = poh.sap
            WHERE oli.route_number = ?
              AND oli.store_id = ?
              AND oli.sap = ?
              AND poh.id IS NULL  -- Only non-promo orders
            """,
            [route_number, store_id, sap],
        )
        rows = result.get('rows') if result else []
        if rows and rows[0].get('avg_qty'):
            return float(rows[0]['avg_qty'])
    except Exception as e:
        print(f"     ⚠️  Baseline lookup failed for {store_id}/{sap}: {e}")
    return None


# =============================================================================
# Real-time Listener
# =============================================================================

def watch_all_orders(sa_path: str):
    """Watch all orders across all routes using real-time on_snapshot."""
    print(f"\n🎧 Order Sync Listener (Multi-User)")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Using: DB Manager (via dbRequests)")
    print(f"   Watching: /orders (all routes)")
    print(f"\n   Press Ctrl+C to stop\n")
    
    fb_client = get_firestore_client(sa_path)
    db_client = DBClient(fb_client, timeout=30)
    
    # Watch all orders
    orders_col = fb_client.collection('orders')
    
    # Track seen orders to avoid reprocessing
    seen_orders = set()
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle order collection changes."""
        for change in changes:
            doc = change.document
            order_id = doc.id
            data = doc.to_dict() or {}
            
            status = data.get('status', '')
            
            if change.type.name == 'ADDED':
                if order_id not in seen_orders:
                    seen_orders.add(order_id)
                    handle_new_order(fb_client, db_client, order_id, data)
                    
                    # If it was already finalized, sync it
                    if status == 'finalized':
                        handle_finalized_order(fb_client, db_client, order_id, data)
            
            elif change.type.name == 'MODIFIED':
                # Check if status changed to finalized
                if status == 'finalized':
                    if order_id not in seen_orders:
                        seen_orders.add(order_id)
                    handle_finalized_order(fb_client, db_client, order_id, data)
    
    # Start real-time listener
    watcher = orders_col.on_snapshot(on_snapshot)
    
    try:
        while True:
            time.sleep(60)  # Keep process alive
    except KeyboardInterrupt:
        print("\n\n👋 Stopping listener...")
        watcher.unsubscribe()


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Order sync listener - watches all orders, syncs new routes via DB Manager",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    # --duckdb is no longer needed since we use DBClient
    parser.add_argument('--duckdb', help='(deprecated) Not used - DB Manager handles database')
    
    args = parser.parse_args()
    
    watch_all_orders(args.serviceAccount)

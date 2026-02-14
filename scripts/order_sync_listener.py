"""Unified order sync listener - watches ALL orders across all routes.

Multi-user support: Automatically syncs new routes when their first order appears.
Uses direct PostgreSQL connections (no DB Manager / DuckDB dependency).

Flow:
1. Watches `/orders` collection for all users
2. On new order: syncs route metadata into PostgreSQL
3. On finalized order: syncs order data directly to PostgreSQL
4. On finalized order: syncs order data for retraining

Usage:
    python scripts/order_sync_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import os
import socket
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from google.cloud import firestore  # type: ignore

try:
    from .db_manager_pg import handle_sync_order
except ImportError:
    from db_manager_pg import handle_sync_order

try:
    from .forecast_engine import ForecastConfig, generate_forecast
except ImportError:
    from forecast_engine import ForecastConfig, generate_forecast

try:
    from google.cloud.firestore_v1.base_query import FieldFilter
except Exception:
    FieldFilter = None  # type: ignore

# =============================================================================
# Direct PostgreSQL connection (for high-volume operations)
# =============================================================================

_pg_conn: Optional[psycopg2.extensions.connection] = None


def get_pg_connection() -> psycopg2.extensions.connection:
    """Get or create a PostgreSQL connection for direct DB access."""
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            port=int(os.environ.get('POSTGRES_PORT', 5432)),
            database=os.environ.get('POSTGRES_DB', 'routespark'),
            user=os.environ.get('POSTGRES_USER', 'routespark'),
            password=os.environ.get('POSTGRES_PASSWORD', ''),
        )
        _pg_conn.autocommit = True
    return _pg_conn

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
    ('2026-11-23', '2026-11-29', 'Thanksgiving'),  # Thanksgiving Nov 26
    ('2026-12-21', '2026-12-27', 'Christmas'),      # Christmas Dec 25
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

def _forecast_exists(
    fb_client: firestore.Client,
    route_number: str,
    delivery_date: str,
    schedule_key: str,
) -> bool:
    """Return True if a non-expired cached forecast exists for delivery_date + schedule_key."""
    try:
        cached_ref = fb_client.collection('forecasts').document(str(route_number)).collection('cached')
        if FieldFilter is not None:
            query = (
                cached_ref
                .where(filter=FieldFilter('deliveryDate', '==', delivery_date))
                .where(filter=FieldFilter('scheduleKey', '==', schedule_key))
            )
            docs = query.stream()
        else:
            # Fallback for older client: scan.
            docs = cached_ref.stream()

        now = datetime.now(timezone.utc)
        for doc in docs:
            data = doc.to_dict() or {}
            if FieldFilter is None:
                if data.get('deliveryDate') != delivery_date or data.get('scheduleKey') != schedule_key:
                    continue
            expires_at = data.get('expiresAt')
            if not expires_at:
                return True
            try:
                if hasattr(expires_at, 'timestamp'):
                    if expires_at.timestamp() > now.timestamp():
                        return True
                else:
                    if expires_at > now:
                        return True
            except Exception:
                # If expiry is malformed, err on "exists" to avoid regenerating repeatedly.
                return True
        return False
    except Exception:
        return False


def _get_next_unordered_delivery(route_number: str) -> Optional[Dict[str, str]]:
    """Pick the single next delivery across all active schedules that doesn't already have a finalized order."""
    conn = get_pg_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT schedule_key, delivery_day
            FROM user_schedules
            WHERE route_number = %s AND is_active = TRUE
            """,
            [str(route_number)],
        )
        schedules = cur.fetchall() or []
        if not schedules:
            return None

        candidates: List[Dict[str, str]] = []
        today = datetime.now(timezone.utc).date()
        for sched in schedules:
            schedule_key = (sched.get('schedule_key') or '').lower()
            delivery_dow = int(sched.get('delivery_day') or 0)  # 1=Mon ... 7=Sun
            if not schedule_key or delivery_dow <= 0:
                continue

            for days in range(1, 15):
                check_date = today + timedelta(days=days)
                check_dow = check_date.weekday() + 1
                if check_dow != delivery_dow:
                    continue

                delivery_date_str = check_date.strftime('%Y-%m-%d')
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM orders_historical
                    WHERE route_number = %s
                      AND schedule_key = %s
                      AND delivery_date = %s
                    """,
                    [str(route_number), schedule_key, delivery_date_str],
                )
                row = cur.fetchone() or {}
                if int(row.get('cnt') or 0) > 0:
                    continue

                candidates.append(
                    {
                        'delivery_date': delivery_date_str,
                        'schedule_key': schedule_key,
                    }
                )
                break

        if not candidates:
            return None
        candidates.sort(key=lambda x: x['delivery_date'])
        return candidates[0]


def _maybe_generate_next_forecast_after_finalization(fb_client: firestore.Client, route_number: str) -> None:
    """Generate exactly one next forecast (if missing) right after an order finalizes.

    This avoids waiting up to 24h for retrain_daemon, and is guarded by existence checks so
    we don't regenerate repeatedly on duplicate finalize events.
    """
    enabled = os.environ.get('FORECAST_ON_FINALIZE', '0').lower() in ('1', 'true', 'yes')
    if not enabled:
        return

    nxt = _get_next_unordered_delivery(route_number)
    if not nxt:
        return

    delivery_date = nxt['delivery_date']
    schedule_key = nxt['schedule_key']

    if _forecast_exists(fb_client, str(route_number), delivery_date, schedule_key):
        return

    sa_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or '/app/config/serviceAccountKey.json'
    print(f"  🔮 Generating next forecast after finalization: {delivery_date} ({schedule_key})")
    cfg = ForecastConfig(
        route_number=str(route_number),
        delivery_date=delivery_date,
        schedule_key=schedule_key,
        service_account=sa_path,
        since_days=365,
        round_cases=True,
        ttl_days=7,
    )
    try:
        forecast = generate_forecast(cfg)
        print(f"     ✅ Forecast {forecast.forecast_id}: {len(forecast.items)} items")
    except RuntimeError as e:
        # Whole-case invariant or other hard gate: skip emission and keep listener running.
        print(f"     ❌ Forecast skipped (hard gate): {e}")
        return


# =============================================================================
# Store ID Alias Resolution
# =============================================================================

def load_store_aliases(route_number: str) -> Dict[str, str]:
    """Load store ID aliases for a route from PostgreSQL.
    
    Returns:
        Dict mapping alias_id -> canonical_id
    """
    global _store_alias_cache
    
    # Check cache first
    if route_number in _store_alias_cache:
        return _store_alias_cache[route_number]
    
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT alias_id, canonical_id 
                FROM store_id_aliases 
                WHERE route_number = %s
                """,
                [route_number],
            )
            rows = cur.fetchall()
        aliases = {row['alias_id']: row['canonical_id'] for row in rows}
        
        # Cache it
        _store_alias_cache[route_number] = aliases
        if aliases:
            print(f"  📋 Loaded {len(aliases)} store ID aliases for route {route_number}")
        return aliases
        
    except Exception as e:
        print(f"  ⚠️ Error loading store aliases: {e}")
        return {}


def resolve_store_id(route_number: str, store_id: str) -> str:
    """Resolve a store_id to its canonical form using the alias table.
    
    If the store_id is found in the alias table, returns the canonical_id.
    Otherwise, returns the original store_id unchanged.
    """
    aliases = load_store_aliases(route_number)
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
# Route Sync Status (PostgreSQL)
# =============================================================================

def is_route_synced(route_number: str) -> bool:
    """Check if a route has been synced to PostgreSQL."""
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT sync_status
                FROM routes_synced
                WHERE route_number = %s
                """,
                [route_number],
            )
            row = cur.fetchone()
        return bool(row and row.get('sync_status') == 'ready')
    except Exception as e:
        print(f"     ⚠️  Error checking sync status: {e}")
        return False


def mark_route_syncing(route_number: str, user_id: str, trigger_order_id: str):
    """Mark a route as currently syncing."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO routes_synced (
                route_number, user_id, first_synced_at, last_synced_at,
                worker_id, triggered_by, trigger_order_id, sync_status
            ) VALUES (%s, %s, %s, %s, %s, 'first_order', %s, 'syncing')
            ON CONFLICT (route_number) DO UPDATE SET
                last_synced_at = EXCLUDED.last_synced_at,
                worker_id = EXCLUDED.worker_id,
                sync_status = 'syncing'
            """, [route_number, user_id, now, now, WORKER_ID, trigger_order_id])
    except Exception as e:
        print(f"     ⚠️  Error marking route syncing: {e}")


def mark_route_ready(route_number: str, stores: int, products: int, orders: int):
    """Mark a route as synced and ready."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE routes_synced SET
                sync_status = 'ready',
                last_synced_at = %s,
                stores_count = %s,
                products_count = %s,
                orders_count = %s,
                schedules_synced = TRUE
            WHERE route_number = %s
            """, [now, stores, products, orders, route_number])
    except Exception as e:
        print(f"     ⚠️  Error marking route ready: {e}")


# =============================================================================
# Firebase Sync Functions (PostgreSQL writes)
# =============================================================================

def sync_stores_from_firebase(fb_client: firestore.Client, route_number: str) -> int:
    """Sync stores from Firebase to PostgreSQL."""
    stores_ref = fb_client.collection('routes').document(route_number).collection('stores')
    stores = stores_ref.stream()
    
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    
    rows = []
    for store_doc in stores:
        data = store_doc.to_dict() or {}
        store_id = store_doc.id

        # Note: Mobile app stores field as 'number', not 'storeNumber'
        rows.append((
            store_id,
            route_number,
            data.get('name', ''),
            data.get('number', ''),
            data.get('isActive', True),
            now,
        ))

    if not rows:
        return 0

    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO stores (store_id, route_number, store_name, store_number, is_active, synced_at)
                VALUES %s
                ON CONFLICT (store_id) DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    store_number = EXCLUDED.store_number,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        count = len(rows)
    except Exception as e:
        print(f"     ⚠️  Error syncing stores: {e}")
    
    return count


def sync_products_from_firebase(fb_client: firestore.Client, route_number: str) -> int:
    """Sync products from Firebase to PostgreSQL.

    Source of truth is masterCatalog/{route}/products (casePack/tray live here).
    Falls back to legacy routes/{route}/products if masterCatalog is empty.
    """
    products_ref = fb_client.collection('masterCatalog').document(route_number).collection('products')
    products = list(products_ref.stream())
    if not products:
        legacy_ref = fb_client.collection('routes').document(route_number).collection('products')
        products = list(legacy_ref.stream())
    
    count = 0
    now = datetime.now(timezone.utc).isoformat()
    
    rows = []
    for prod_doc in products:
        data = prod_doc.to_dict() or {}
        sap = data.get('sap', prod_doc.id)

        rows.append((
            sap,
            route_number,
            data.get('fullName') or data.get('name') or data.get('product') or '',
            data.get('shortName', ''),
            data.get('brand', ''),
            data.get('category', ''),
            data.get('casePack') or data.get('tray') or 1,
            data.get('active', data.get('isActive', True)),
            now,
        ))

    if not rows:
        return 0

    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO product_catalog (sap, route_number, full_name, short_name, brand, category, case_pack, is_active, synced_at)
                VALUES %s
                ON CONFLICT (sap, route_number) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    short_name = EXCLUDED.short_name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    case_pack = EXCLUDED.case_pack,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        count = len(rows)
    except Exception as e:
        print(f"     ⚠️  Error syncing products: {e}")
    
    return count


def sync_schedules_from_firebase(fb_client: firestore.Client, route_number: str, user_id: str) -> int:
    """Sync order schedules from Firebase to PostgreSQL."""
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
    
    rows = []
    for i, cycle in enumerate(cycles):
        order_day = cycle.get('orderDay', 1)
        load_day = cycle.get('loadDay', 3)
        delivery_day = cycle.get('deliveryDay', 4)
        # schedule_key based on ORDER day (user's mental model)
        schedule_key = day_names.get(order_day, 'unknown')
        
        schedule_id = f"{route_number}-cycle-{i}"

        rows.append((
            schedule_id,
            route_number,
            user_id,
            order_day,
            load_day,
            delivery_day,
            schedule_key,
            True,
            now,
        ))

    if not rows:
        return 0

    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO user_schedules (id, route_number, user_id, order_day, load_day, delivery_day, schedule_key, is_active, synced_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    order_day = EXCLUDED.order_day,
                    load_day = EXCLUDED.load_day,
                    delivery_day = EXCLUDED.delivery_day,
                    schedule_key = EXCLUDED.schedule_key,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )
        count = len(rows)
    except Exception as e:
        print(f"     ⚠️  Error syncing schedules: {e}")
    
    return count


def sync_full_route(fb_client: firestore.Client, 
                   route_number: str, user_id: str, trigger_order_id: str) -> bool:
    """Perform a full sync of a route from Firebase to PostgreSQL."""
    print(f"  🔄 Syncing route {route_number}...")
    
    try:
        # Mark as syncing
        mark_route_syncing(route_number, user_id, trigger_order_id)
        
        # Sync each entity type
        stores_count = sync_stores_from_firebase(fb_client, route_number)
        print(f"     ✓ Synced {stores_count} stores")
        
        products_count = sync_products_from_firebase(fb_client, route_number)
        print(f"     ✓ Synced {products_count} products")
        
        schedules_count = sync_schedules_from_firebase(fb_client, route_number, user_id)
        print(f"     ✓ Synced {schedules_count} schedules")
        
        # Mark as ready
        mark_route_ready(route_number, stores_count, products_count, 0)
        
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

def handle_new_order(fb_client: firestore.Client, order_id: str, data: dict):
    """Handle a new order being created."""
    route_number = data.get('routeNumber')
    user_id = data.get('userId')
    status = data.get('status')
    
    if not route_number:
        return
    
    print(f"  📦 New order: {order_id} (route: {route_number}, status: {status})")
    
    # Check if this route is already synced
    if not is_route_synced(route_number):
        print(f"  🆕 First order for route {route_number} - syncing...")
        if sync_full_route(fb_client, route_number, user_id, order_id):
            update_firebase_sync_status(fb_client, route_number, True)
    else:
        print(f"     Route {route_number} already synced")


def handle_finalized_order(fb_client: firestore.Client, order_id: str, data: dict):
    """Handle an order being finalized - sync it to PostgreSQL and regenerate forecasts."""
    route_number = data.get('routeNumber')
    user_id = data.get('userId')
    schedule_key = data.get('scheduleKey')
    
    if not route_number:
        return
    
    print(f"  ✅ Order finalized: {order_id}")
    
    # Use db_manager_pg's sync_order handler
    try:
        result = handle_sync_order(get_pg_connection(), fb_client, {
            'orderId': order_id,
            'routeNumber': route_number,
        })
        if 'error' in result:
            print(f"     ⚠️  Sync error: {result['error']}")
        else:
            print(f"     Synced {result.get('totalUnits', 0)} units across {result.get('storeCount', 0)} stores")
            corrections_count = result.get('correctionsExtracted', 0)
            if corrections_count > 0:
                print(f"     📊 Extracted {corrections_count} corrections for ML training")
            create_delivery_allocations(fb_client, order_id, route_number, data)
            link_promos_for_order(order_id, route_number, data)
            
            # Update Firebase sync status so app knows data is current
            update_firebase_sync_status(fb_client, route_number, True)
            
            # NOTE: Removed auto-regeneration - was causing duplicate forecasts on every order sync.
            # Forecasts should only be generated:
            #   1. Once per complete order cycle (by retrain_daemon.py)
            #   2. Manually via run_forecast.py when needed
            # The daemon already incorporates corrections when generating forecasts.
            # regenerate_forecasts_after_finalization(fb_client, route_number, user_id)
            _maybe_generate_next_forecast_after_finalization(fb_client, route_number)
    except Exception as e:
        print(f"     ❌ Error syncing order: {e}")


def regenerate_forecasts_after_finalization(
    fb_client: firestore.Client,
    route_number: str,
    user_id: str,
) -> None:
    """Generate a forecast for ONLY the next upcoming delivery after finalization.
    
    Cross-cycle dependency: what is ordered in Cycle A affects what should be
    recommended in Cycle B.  So we generate ONE forecast at a time:
    
        Order(CycleA) → Forecast(CycleB) → Order(CycleB) → Retrain → Forecast(CycleA) → …
    
    This ensures each forecast incorporates the most recent order data.
    """
    print(f"  🔄 Generating next forecast after finalization...")
    
    # First, clean up old forecasts (past delivery dates)
    cleaned = cleanup_old_forecasts(fb_client, route_number)
    if cleaned > 0:
        print(f"     🧹 Cleaned up {cleaned} past forecasts")
    
    try:
        # Get user's order schedules (tries PostgreSQL first, then Firebase)
        schedules = get_user_schedules(fb_client, route_number)
        if not schedules:
            print(f"     ⚠️  No schedules found for route {route_number}")
            return
        
        # Find the SINGLE soonest unordered delivery across all schedules
        candidates = []
        today = datetime.now(timezone.utc).date()
        
        for schedule in schedules:
            order_day = schedule.get('orderDay', 1)  # 0=Sun, 1=Mon, etc.
            delivery_day = schedule.get('deliveryDay', 4)
            
            # Convert to schedule_key (based on order day)
            day_names = {0: 'sunday', 1: 'monday', 2: 'tuesday', 3: 'wednesday',
                        4: 'thursday', 5: 'friday', 6: 'saturday'}
            schedule_key = day_names.get(order_day, 'unknown')
            
            # Find next delivery date for this schedule
            next_delivery = get_next_delivery_date(today, delivery_day)
            next_delivery_str = next_delivery.strftime('%Y-%m-%d') if hasattr(next_delivery, 'strftime') else next_delivery.isoformat()[:10]
            
            # Check if this delivery already has a finalized order (PostgreSQL)
            try:
                from db_manager_pg import fetch_one
                order_check = fetch_one("""
                    SELECT COUNT(*) as cnt
                    FROM orders_historical
                    WHERE route_number = %s
                      AND schedule_key = %s
                      AND delivery_date = %s
                """, [route_number, schedule_key, next_delivery_str])
                if (order_check.get('cnt', 0) if order_check else 0) > 0:
                    print(f"     ⏭️  Skipping {next_delivery_str} ({schedule_key}) - already ordered")
                    continue
            except Exception:
                pass  # If PG check fails, include this candidate
            
            candidates.append({
                'delivery_date': next_delivery_str,
                'schedule_key': schedule_key,
            })
        
        if not candidates:
            print(f"     No upcoming unordered deliveries to forecast")
            return
        
        # Pick the SOONEST delivery (serial chain)
        candidates.sort(key=lambda x: x['delivery_date'])
        target = candidates[0]
        delivery_date = target['delivery_date']
        schedule_key = target['schedule_key']
        
        print(f"     📋 Next forecast target: {delivery_date} ({schedule_key})")
        
        # Delete any existing forecast for this date (will be replaced)
        deleted = delete_forecasts_for_dates(fb_client, route_number, [target])
        if deleted > 0:
            print(f"     🗑️  Deleted {deleted} stale forecast(s) for {delivery_date}")
        
        # Generate fresh forecast
        try:
            from forecast_engine import ForecastConfig, generate_forecast
            
            import os
            sa_path = os.environ.get(
                'FIREBASE_SA_PATH',
                '/Users/kylemacmini/Desktop/routespark/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'
            )
            
            print(f"     🔮 Generating forecast for {delivery_date} ({schedule_key})...")
            
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


def get_user_schedules(fb_client: firestore.Client, route_number: str) -> List[Dict]:
    """Get user's order schedules - uses PostgreSQL, falls back to Firebase.
    
    Uses schedule_utils.get_order_cycles which handles the correct Firebase path
    (userSettings.notifications.scheduling.orderCycles) with fallback.
    """
    try:
        from schedule_utils import get_order_cycles
        return get_order_cycles(fb_client, route_number)
    except ImportError:
        pass
    
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
    fb_client: firestore.Client,
    order_id: str,
    route_number: str,
    data: dict,
) -> None:
    """Create delivery allocation rows for a finalized order.
    
    Uses direct PostgreSQL with batch inserts for efficiency (no Firebase round-trips).
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
    
    # Collect all rows for batch insert
    allocation_rows = []
    case_splits = 0
    
    for store in stores:
        raw_store_id = store.get('id') or store.get('storeId') or ''
        store_name = store.get('name') or store.get('storeName') or ''
        items = store.get('items', []) or []

        if not raw_store_id:
            continue

        # Resolve store ID to canonical form (handles old/alias IDs)
        store_id = resolve_store_id(route_number, raw_store_id)
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
            allocation_rows.append((
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
            ))

    # Batch insert using direct PostgreSQL (no Firebase round-trips)
    if allocation_rows:
        try:
            conn = get_pg_connection()
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO delivery_allocations (
                        allocation_id, route_number, source_order_id, source_order_date,
                        sap, store_id, store_name, quantity, delivery_date, is_case_split
                    ) VALUES %s
                    ON CONFLICT (allocation_id) DO UPDATE SET
                        route_number = EXCLUDED.route_number,
                        source_order_date = EXCLUDED.source_order_date,
                        store_name = EXCLUDED.store_name,
                        quantity = EXCLUDED.quantity,
                        delivery_date = EXCLUDED.delivery_date,
                        is_case_split = EXCLUDED.is_case_split
                    """,
                    allocation_rows,
                )
            
            msg = f"     ✓ Created/updated {len(allocation_rows)} delivery allocation rows"
            if case_splits > 0:
                msg += f" ({case_splits} stores with case splits)"
            print(msg)
        except Exception as e:
            print(f"     ⚠️  Failed to batch insert allocations: {e}")
    
    # Post-process: Mark case splits based on comparing source order dates
    # For each store+delivery_date, the LATEST source_order_date is the "primary"
    # Earlier source_order_dates are case splits
    mark_case_splits_for_route(route_number)


def mark_case_splits_for_route(route_number: str) -> None:
    """Mark case splits by comparing source order dates per store+delivery_date.
    
    Uses direct PostgreSQL for efficiency (no Firebase round-trips).
    
    For each store+delivery_date combination:
    - The LATEST source_order_date is the "primary" order (is_case_split = FALSE)
    - Earlier source_order_dates are case splits (is_case_split = TRUE)
    """
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find all store+delivery_date combinations
            cur.execute("""
                WITH order_dates_per_delivery AS (
                    SELECT 
                        store_id,
                        delivery_date,
                        MAX(source_order_date) as primary_order_date
                    FROM delivery_allocations
                    WHERE route_number = %s
                    GROUP BY store_id, delivery_date
                )
                SELECT store_id, delivery_date, primary_order_date
                FROM order_dates_per_delivery
            """, [route_number])
            
            rows = cur.fetchall()
            if not rows:
                return
            
            # For each store+delivery_date, set is_case_split based on source_order_date
            updates = 0
            for row in rows:
                store_id = row['store_id']
                delivery_date = row['delivery_date']
                primary_order_date = row['primary_order_date']
                
                # Set is_case_split = FALSE for items from primary order date
                cur.execute("""
                    UPDATE delivery_allocations
                    SET is_case_split = FALSE
                    WHERE route_number = %s
                      AND store_id = %s
                      AND delivery_date = %s
                      AND source_order_date = %s
                """, [route_number, store_id, delivery_date, primary_order_date])
                
                # Set is_case_split = TRUE for items from earlier order dates
                cur.execute("""
                    UPDATE delivery_allocations
                    SET is_case_split = TRUE
                    WHERE route_number = %s
                      AND store_id = %s
                      AND delivery_date = %s
                      AND source_order_date < %s
                """, [route_number, store_id, delivery_date, primary_order_date])
                
                updates += 1
            
            if updates > 0:
                print(f"     ✓ Updated case split flags for {updates} store+delivery combinations")
    except Exception as e:
        print(f"     ⚠️  Error marking case splits: {e}")


# =============================================================================
# Promo linkage
# =============================================================================

def link_promos_for_order(order_id: str, route_number: str, data: dict) -> None:
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
        store_id = resolve_store_id(route_number, raw_store_id) if raw_store_id else ''
        
        for item in store.get('items', []) or []:
            sap = item.get('sap')
            qty = item.get('quantity', 0)
            cases = item.get('cases', 0)
            if not sap or qty == 0:
                continue
            promo_info = _find_promo_info(route_number, sap, order_date)
            if promo_info:
                # Get baseline quantity (avg non-promo orders for this store/sap)
                baseline = _get_baseline_quantity(route_number, store_id, sap)
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

    # Batch insert using direct PostgreSQL
    try:
        rows = []
        for entry in promo_items:
            rows.append((
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
            ))
        
        conn = get_pg_connection()
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO promo_order_history (
                    id, route_number, promo_id, order_id, store_id, sap,
                    promo_price, discount_percent,
                    quantity_ordered, cases_ordered,
                    baseline_quantity, quantity_lift,
                    weeks_into_promo, is_first_promo_occurrence,
                    created_at
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    quantity_ordered = EXCLUDED.quantity_ordered,
                    cases_ordered = EXCLUDED.cases_ordered,
                    baseline_quantity = EXCLUDED.baseline_quantity,
                    quantity_lift = EXCLUDED.quantity_lift
                """,
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
            )
        
        print(f"     ✓ Linked {len(promo_items)} promo items for order {order_id}")
    except Exception as e:
        print(f"     ⚠️  Failed to write promo_order_history: {e}")


def _find_promo_info(route_number: str, sap: str, order_date: str) -> dict | None:
    """Lookup promo info for sap/date window including ML features.
    
    Uses direct PostgreSQL for efficiency.
    """
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    pi.promo_id,
                    pi.special_price,
                    pi.discount_percent,
                    ph.start_date,
                    ph.times_seen
                FROM promo_items pi
                JOIN promo_history ph ON pi.promo_id = ph.promo_id
                WHERE pi.sap = %s
                  AND ph.route_number = %s
                  AND DATE(%s) BETWEEN ph.start_date AND ph.end_date
                LIMIT 1
                """,
                [sap, route_number, order_date],
            )
            row = cur.fetchone()
            if row:
                promo_id = row.get('promo_id')
                start_date = row.get('start_date')
                times_seen = row.get('times_seen', 0)
                
                # Calculate weeks into promo
                weeks_into = 1
                if start_date and order_date:
                    try:
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


def _get_baseline_quantity(route_number: str, store_id: str, sap: str) -> float | None:
    """Get baseline (non-promo) average quantity for this store/sap.
    
    Uses direct PostgreSQL for efficiency.
    """
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT AVG(oli.quantity) as avg_qty
                FROM order_line_items oli
                LEFT JOIN promo_order_history poh 
                    ON oli.order_id = poh.order_id 
                    AND oli.store_id = poh.store_id 
                    AND oli.sap = poh.sap
                WHERE oli.route_number = %s
                  AND oli.store_id = %s
                  AND oli.sap = %s
                  AND poh.id IS NULL  -- Only non-promo orders
                """,
                [route_number, store_id, sap],
            )
            row = cur.fetchone()
            if row and row.get('avg_qty'):
                return float(row['avg_qty'])
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
    print(f"   Using: Direct PostgreSQL")
    print(f"   Watching: /routes/*/orders (collection group)")
    print(f"\n   Press Ctrl+C to stop\n")
    
    fb_client = get_firestore_client(sa_path)
    # Watch all orders
    orders_col = fb_client.collection_group('orders')
    
    # Track seen orders to avoid reprocessing
    seen_orders = set()
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle order collection changes."""
        for change in changes:
            doc = change.document
            order_id = doc.id
            data = doc.to_dict() or {}

            # Guard: only process route-scoped orders
            path_parts = doc.reference.path.split('/')
            if len(path_parts) != 4 or path_parts[0] != 'routes' or path_parts[2] != 'orders':
                continue
            
            status = data.get('status', '')
            
            if change.type.name == 'ADDED':
                if order_id not in seen_orders:
                    seen_orders.add(order_id)
                    handle_new_order(fb_client, order_id, data)
                    
                    # If it was already finalized, sync it
                    if status == 'finalized':
                        handle_finalized_order(fb_client, order_id, data)
            
            elif change.type.name == 'MODIFIED':
                # Check if status changed to finalized
                if status == 'finalized':
                    if order_id not in seen_orders:
                        seen_orders.add(order_id)
                    handle_finalized_order(fb_client, order_id, data)
    
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
        description="Order sync listener - watches all orders, syncs new routes via PostgreSQL",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    # --duckdb is no longer needed since we use PostgreSQL directly
    parser.add_argument('--duckdb', help='(deprecated) Not used - direct PostgreSQL writes')
    
    args = parser.parse_args()
    
    watch_all_orders(args.serviceAccount)

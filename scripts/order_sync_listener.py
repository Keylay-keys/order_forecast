"""Unified order sync listener - watches ALL orders across all routes.

Multi-user support: Automatically syncs new routes when their first order appears.
Uses direct PostgreSQL connections.

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
from pathlib import Path
from typing import Any, Optional, Dict, List

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from google.cloud import firestore  # type: ignore

try:
    from .db_manager_pg import handle_sync_order
except ImportError:
    from db_manager_pg import handle_sync_order

try:
    from .schedule_cycle import add_days, normalize_order_cycle, schedule_key_for_day
except ImportError:
    from schedule_cycle import add_days, normalize_order_cycle, schedule_key_for_day

try:
    from .forecast_engine import ForecastConfig, generate_forecast
except ImportError:
    from forecast_engine import ForecastConfig, generate_forecast

try:
    from .forecast_generation_queue import (
        coerce_finalized_at,
        enqueue_finalize_jobs,
        ensure_forecast_queue_tables,
        mark_finalize_event_error,
        process_generation_jobs_for_route,
        reconcile_finalize_event,
        register_finalize_event,
    )
except ImportError:
    from forecast_generation_queue import (
        coerce_finalized_at,
        enqueue_finalize_jobs,
        ensure_forecast_queue_tables,
        mark_finalize_event_error,
        process_generation_jobs_for_route,
        reconcile_finalize_event,
        register_finalize_event,
    )

try:
    from .finalize_rollout import api_finalize_rollout_enabled_for_route
except ImportError:
    from finalize_rollout import api_finalize_rollout_enabled_for_route

try:
    from google.cloud.firestore_v1.base_query import FieldFilter
except Exception:
    FieldFilter = None  # type: ignore

# =============================================================================
# Direct PostgreSQL connection (for high-volume operations)
# =============================================================================

_pg_conn: Optional[psycopg2.extensions.connection] = None
LOCAL_FIREBASE_SA_FALLBACK = (
    Path.home() / "Desktop" / "dev" / "firebase-tools" / "routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json"
)
SERVER_FIREBASE_SA_FALLBACK = Path("/srv/routespark/config/serviceAccountKey.json")


def resolve_firebase_sa_path() -> str:
    """Resolve a usable Firebase service account path for forecast generation."""
    from_env = os.environ.get("FIREBASE_SA_PATH", "").strip()
    if from_env:
        return from_env

    for candidate in (SERVER_FIREBASE_SA_FALLBACK, LOCAL_FIREBASE_SA_FALLBACK):
        if candidate.exists():
            return str(candidate)

    raise RuntimeError(
        "FIREBASE_SA_PATH is not set and no default service account path exists. "
        "Set FIREBASE_SA_PATH explicitly for this runtime."
    )


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

def _allowed_routes() -> Optional[set[str]]:
    raw = os.environ.get("ROUTESPARK_ALLOWED_ROUTES", "").strip()
    if not raw:
        return None
    values = {item.strip() for item in raw.split(",") if item.strip()}
    return values or None


def _route_allowed(route_number: Optional[str]) -> bool:
    if not route_number:
        return False
    allowed = _allowed_routes()
    return True if allowed is None else str(route_number) in allowed


def _skip_initial_snapshot() -> bool:
    return os.environ.get('ROUTESPARK_SKIP_INITIAL_ORDER_SNAPSHOT', '0').lower() in ('1', 'true', 'yes')


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


def _delivery_date_matches_schedule(delivery_date, schedule: Dict) -> bool:
    cycle = normalize_order_cycle({
        'orderDay': schedule.get('orderDay', schedule.get('order_day')),
        'loadDay': schedule.get('loadDay', schedule.get('load_day')),
        'deliveryDay': schedule.get('deliveryDay', schedule.get('delivery_day')),
        'loadOffsetDays': schedule.get('loadOffsetDays', schedule.get('load_offset_days')),
        'deliveryOffsetDays': schedule.get('deliveryOffsetDays', schedule.get('delivery_offset_days')),
        'scheduleVersion': schedule.get('scheduleVersion', schedule.get('schedule_version')),
        'needsScheduleReview': schedule.get('needsScheduleReview', schedule.get('needs_schedule_review')),
    })
    order_date = add_days(delivery_date, -cycle['deliveryOffsetDays'])
    return order_date.isoweekday() == cycle['orderDay']


def _get_next_unordered_delivery(route_number: str) -> Optional[Dict[str, str]]:
    """Pick the single next delivery across all active schedules that doesn't already have a finalized order."""
    conn = get_pg_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                schedule_key,
                order_day,
                load_day,
                delivery_day,
                load_offset_days,
                delivery_offset_days,
                schedule_version,
                needs_schedule_review
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
            if not schedule_key:
                continue

            for days in range(1, 15):
                check_date = today + timedelta(days=days)
                if not _delivery_date_matches_schedule(check_date, sched):
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


def _forecast_on_finalize_enabled() -> bool:
    return os.environ.get('FORECAST_ON_FINALIZE', '0').lower() in ('1', 'true', 'yes')


def _extract_finalized_at(data: dict) -> datetime:
    timestamps = data.get('timestamps') if isinstance(data.get('timestamps'), dict) else {}
    candidates = [
        data.get('finalizedAt'),
        data.get('statusFinalizedAt'),
        data.get('statusUpdatedAt'),
        timestamps.get('finalizedAt'),
        data.get('updatedAt'),
        data.get('createdAt'),
    ]
    for value in candidates:
        parsed = coerce_finalized_at(value, fallback_now=False)
        if parsed is not None:
            return parsed
    return datetime.now(timezone.utc)


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
            data.get('upc') or data.get('sku'),
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
                INSERT INTO product_catalog (sap, route_number, full_name, short_name, upc, brand, category, case_pack, is_active, synced_at)
                VALUES %s
                ON CONFLICT (sap, route_number) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    short_name = EXCLUDED.short_name,
                    upc = EXCLUDED.upc,
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
                 5: 'friday', 6: 'saturday', 7: 'sunday'}
    
    rows = []
    for i, cycle in enumerate(cycles):
        normalized_cycle = normalize_order_cycle(cycle)
        order_day = normalized_cycle['orderDay']
        load_day = normalized_cycle['loadDay']
        delivery_day = normalized_cycle['deliveryDay']
        load_offset_days = normalized_cycle['loadOffsetDays']
        delivery_offset_days = normalized_cycle['deliveryOffsetDays']
        schedule_version = normalized_cycle['scheduleVersion']
        needs_schedule_review = normalized_cycle['needsScheduleReview']
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
            load_offset_days,
            delivery_offset_days,
            schedule_version,
            needs_schedule_review,
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
                INSERT INTO user_schedules (
                    id, route_number, user_id, order_day, load_day, delivery_day,
                    load_offset_days, delivery_offset_days, schedule_version, needs_schedule_review,
                    schedule_key, is_active, synced_at
                )
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    order_day = EXCLUDED.order_day,
                    load_day = EXCLUDED.load_day,
                    delivery_day = EXCLUDED.delivery_day,
                    load_offset_days = EXCLUDED.load_offset_days,
                    delivery_offset_days = EXCLUDED.delivery_offset_days,
                    schedule_version = EXCLUDED.schedule_version,
                    needs_schedule_review = EXCLUDED.needs_schedule_review,
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
    
    if not _route_allowed(route_number):
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
    finalized_at = _extract_finalized_at(data)
    finalize_event_key = None
    
    if not _route_allowed(route_number):
        return

    mutation_kind = str((data.get('lastMutation') or {}).get('kind') or '').strip()
    if mutation_kind in {'store_reallocation', 'full_adjustment'}:
        # The durable adjustment receipt owns projection retries. An order MODIFIED
        # event must never be mistaken for another finalization/forecast event.
        print(f"  ↪️  Order revision awaiting adjustment projection: {order_id} ({mutation_kind})")
        return
    
    print(f"  ✅ Order finalized: {order_id}")

    if _forecast_on_finalize_enabled():
        try:
            event_row = register_finalize_event(
                route_number=str(route_number),
                order_id=str(order_id),
                schedule_key=schedule_key,
                finalized_at_raw=finalized_at,
                worker_id=WORKER_ID,
            )
            finalize_event_key = event_row.get('finalize_key')
            if event_row.get('status') == 'processed':
                print(f"     ⏭️  Finalize event already processed: {finalize_event_key}")
        except Exception as e:
            print(f"     ⚠️  Failed to register finalize event: {e}")
    
    # Use db_manager_pg's sync_order handler
    try:
        result = handle_sync_order(get_pg_connection(), fb_client, {
            'orderId': order_id,
            'routeNumber': route_number,
        })
        if 'error' in result:
            print(f"     ⚠️  Sync error: {result['error']}")
            if finalize_event_key:
                try:
                    mark_finalize_event_error(str(finalize_event_key), f"sync_error:{result['error']}")
                except Exception as e:
                    print(f"     ⚠️  Failed to mark finalize event error: {e}")
        else:
            print(f"     Synced {result.get('totalUnits', 0)} units across {result.get('storeCount', 0)} stores")
            corrections_count = result.get('correctionsExtracted', 0)
            if corrections_count > 0:
                print(f"     📊 Extracted {corrections_count} corrections for ML training")
            # Update Firebase sync status so app knows data is current
            update_firebase_sync_status(fb_client, route_number, True)

            if api_finalize_rollout_enabled_for_route(str(route_number)):
                print(f"     ℹ️  API finalize rollout owns forecast enqueue for route {route_number}; listener standing down")
                return
            
            # NOTE: Removed auto-regeneration - was causing duplicate forecasts on every order sync.
            # Forecasts should only be generated:
            #   1. Once per complete order cycle (by retrain_daemon.py)
            #   2. Manually via run_forecast.py when needed
            # The daemon already incorporates corrections when generating forecasts.
            # regenerate_forecasts_after_finalization(fb_client, route_number, user_id)
            if _forecast_on_finalize_enabled():
                queue_result = enqueue_finalize_jobs(
                    route_number=str(route_number),
                    order_id=str(order_id),
                    schedule_key=schedule_key,
                    finalized_at_raw=finalized_at,
                    worker_id=WORKER_ID,
                )
                queue_status = queue_result.get('status')
                if queue_status == 'already_processed':
                    print(f"     ⏭️  Forecast queue skipped (already processed): {queue_result.get('finalize_key')}")
                elif queue_status == 'queued':
                    sa_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or '/app/config/serviceAccountKey.json'
                    stats = process_generation_jobs_for_route(
                        fb_client=fb_client,
                        route_number=str(route_number),
                        worker_id=WORKER_ID,
                        sa_path=sa_path,
                        max_jobs=int(os.environ.get('FORECAST_FINALIZE_MAX_JOBS_PER_EVENT', '4')),
                    )
                    reconciliation = reconcile_finalize_event(str(queue_result.get('finalize_key')))
                    print(
                        "     🧠 Forecast queue:"
                        f" claimed={stats.get('claimed', 0)} done={stats.get('done', 0)}"
                        f" skipped={stats.get('skipped_fresh', 0)}"
                        f" retry_or_error={stats.get('retry_or_error', 0)}"
                        f" finalize_status={reconciliation.get('status')}"
                    )
                else:
                    print(f"     ℹ️  Forecast queue result: {queue_status}")
            else:
                _maybe_generate_next_forecast_after_finalization(fb_client, route_number)
    except Exception as e:
        print(f"     ❌ Error syncing order: {e}")
        if finalize_event_key:
            try:
                mark_finalize_event_error(str(finalize_event_key), f"sync_exception:{e}")
            except Exception:
                pass


def handle_pending_adjustment_projection(
    fb_client: firestore.Client,
    adjustment_ref: Any,
    adjustment_data: dict,
) -> Dict[str, Any]:
    """Project the current canonical order for one durable adjustment receipt."""
    route_number = str(adjustment_data.get('routeNumber') or '').strip()
    order_id = str(adjustment_data.get('sourceOrderId') or '').strip()
    projection = adjustment_data.get('projection') or {}
    target_revision = int(projection.get('targetOrderRevision') or 0)
    if not route_number or not order_id or target_revision < 1:
        adjustment_ref.update({
            'projection.status': 'failed',
            'projection.lastErrorCode': 'INVALID_PROJECTION_RECEIPT',
            'projection.lastAttemptAt': firestore.SERVER_TIMESTAMP,
            'projection.attemptCount': firestore.Increment(1),
            'updatedAt': firestore.SERVER_TIMESTAMP,
        })
        return {'error': 'INVALID_PROJECTION_RECEIPT'}

    result = handle_sync_order(get_pg_connection(), fb_client, {
        'orderId': order_id,
        'routeNumber': route_number,
    })
    projected_revision = int(result.get('projectedRevision') or 0) if 'error' not in result else 0
    if 'error' in result or projected_revision < target_revision:
        print(
            f"     ❌ Adjustment projection failed: route={route_number} "
            f"order={order_id} target_revision={target_revision} code=ORDER_PROJECTION_FAILED"
        )
        adjustment_ref.update({
            'projection.status': 'failed',
            'projection.lastErrorCode': 'ORDER_PROJECTION_FAILED',
            'projection.lastAttemptAt': firestore.SERVER_TIMESTAMP,
            'projection.attemptCount': firestore.Increment(1),
            'updatedAt': firestore.SERVER_TIMESTAMP,
        })
        return {'error': 'ORDER_PROJECTION_FAILED'}

    adjustment_ref.update({
        'projection.status': 'succeeded',
        'projection.projectedOrderRevision': projected_revision,
        'projection.completedAt': firestore.SERVER_TIMESTAMP,
        'projection.lastAttemptAt': firestore.SERVER_TIMESTAMP,
        'projection.lastErrorCode': firestore.DELETE_FIELD,
        'projection.attemptCount': firestore.Increment(1),
        'updatedAt': firestore.SERVER_TIMESTAMP,
    })
    update_firebase_sync_status(fb_client, route_number, True)
    print(
        f"     ✓ Adjustment projected: route={route_number} order={order_id} "
        f"revision={projected_revision}"
    )
    return result


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
            normalized_schedule = normalize_order_cycle(schedule)
            schedule_key = schedule_key_for_day(normalized_schedule['orderDay'])

            next_delivery_str = None
            for days in range(1, 15):
                check_date = today + timedelta(days=days)
                if _delivery_date_matches_schedule(check_date, normalized_schedule):
                    next_delivery_str = check_date.strftime('%Y-%m-%d')
                    break

            if not next_delivery_str:
                continue
            
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
        
        # The publisher writes and verifies the replacement before removing any
        # superseded exact artifact; callers must never create an availability gap.
        try:
            from forecast_engine import ForecastConfig, generate_forecast
            
            sa_path = resolve_firebase_sa_path()
            
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
    if _forecast_on_finalize_enabled():
        try:
            ensure_forecast_queue_tables()
        except Exception as e:
            print(f"⚠️  Failed to ensure forecast queue tables: {e}")
    # Watch all orders
    orders_col = fb_client.collection_group('orders')
    adjustments_col = fb_client.collection_group('orderAdjustments')
    if FieldFilter is not None:
        pending_adjustments = adjustments_col.where(
            filter=FieldFilter('projection.status', '==', 'pending')
        )
    else:
        pending_adjustments = adjustments_col.where('projection.status', '==', 'pending')
    
    # Track seen orders to avoid reprocessing
    seen_orders = set()
    initial_snapshot_seen = False
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle order collection changes."""
        nonlocal initial_snapshot_seen

        if not initial_snapshot_seen:
            initial_snapshot_seen = True
            if _skip_initial_snapshot():
                primed = 0
                for change in changes:
                    doc = change.document
                    path_parts = doc.reference.path.split('/')
                    if len(path_parts) != 4 or path_parts[0] != 'routes' or path_parts[2] != 'orders':
                        continue
                    seen_orders.add(doc.id)
                    primed += 1
                print(f"   Initial snapshot skipped; primed {primed} existing order(s)")
                return

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

    def on_adjustment_snapshot(col_snapshot, changes, read_time):
        """Process durable projection receipts, including pending docs on startup."""
        for change in changes:
            if change.type.name not in {'ADDED', 'MODIFIED'}:
                continue
            doc = change.document
            data = doc.to_dict() or {}
            path_parts = doc.reference.path.split('/')
            if (
                len(path_parts) != 4
                or path_parts[0] != 'routes'
                or path_parts[2] != 'orderAdjustments'
                or (data.get('projection') or {}).get('status') != 'pending'
            ):
                continue
            try:
                handle_pending_adjustment_projection(fb_client, doc.reference, data)
            except Exception as exc:
                print(
                    "     ❌ Unhandled adjustment projection error:"
                    f" receipt={doc.id} code=UNHANDLED_PROJECTION_EXCEPTION"
                    f" type={type(exc).__name__}"
                )
    
    # Start real-time listener
    watcher = orders_col.on_snapshot(on_snapshot)
    adjustment_watcher = pending_adjustments.on_snapshot(on_adjustment_snapshot)
    
    try:
        while True:
            time.sleep(60)  # Keep process alive
    except KeyboardInterrupt:
        print("\n\n👋 Stopping listener...")
        watcher.unsubscribe()
        adjustment_watcher.unsubscribe()


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Order sync listener - watches all orders, syncs new routes via PostgreSQL",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    
    args = parser.parse_args()
    
    watch_all_orders(args.serviceAccount)

#!/usr/bin/env python3
"""Real-time config sync listener - syncs stores, products, and schedules from Firestore to PostgreSQL.

Provides event-driven sync for user configuration data:
- stores (from routes/{route}/stores) -> stores + store_items tables
- products (from masterCatalog/{route}/products) -> product_catalog table
- schedules (from users/{ownerId}) -> user_schedules table

This replaces polling-based sync with real-time Firestore listeners.

Usage:
    python scripts/config_sync_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List, Set

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from google.cloud import firestore  # type: ignore

try:
    from .schedule_cycle import normalize_order_cycle
except ImportError:
    from schedule_cycle import normalize_order_cycle

# Worker ID for this instance
WORKER_ID = f"config-sync-{socket.gethostname()}-{os.getpid()}"
_pg_connection_slots = threading.BoundedSemaphore(
    max(1, int(os.environ.get("CONFIG_SYNC_MAX_DB_CONNECTIONS", "8")))
)

# =============================================================================
# PostgreSQL Connection
# =============================================================================

def _new_pg_connection() -> psycopg2.extensions.connection:
    if not _pg_connection_slots.acquire(timeout=30):
        raise TimeoutError("config_sync_db_connection_slot_timeout")
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            port=int(os.environ.get('POSTGRES_PORT', 5432)),
            database=os.environ.get('POSTGRES_DB', 'routespark'),
            user=os.environ.get('POSTGRES_USER', 'routespark'),
            password=os.environ.get('POSTGRES_PASSWORD', ''),
        )
    except Exception:
        _pg_connection_slots.release()
        raise
    # Keep autocommit disabled so each sync unit can commit/rollback atomically.
    conn.autocommit = False
    return conn


def get_pg_connection() -> psycopg2.extensions.connection:
    """Open a short-lived PostgreSQL connection for one sync unit."""
    return _new_pg_connection()


def close_pg_connection(conn: Optional[psycopg2.extensions.connection]) -> None:
    """Close a sync unit's PostgreSQL connection."""
    if conn is None:
        return
    try:
        if not conn.closed:
            conn.close()
    finally:
        _pg_connection_slots.release()


def get_firestore_client(sa_path: str) -> firestore.Client:
    """Create a Firestore client from service account."""
    return firestore.Client.from_service_account_json(sa_path)


def _store_forecast_signature(data: dict) -> str:
    active_items = []
    for item in (data.get("items") or data.get("activeItems") or []):
        if isinstance(item, dict):
            if item.get("isActive") is False or item.get("is_active") is False or item.get("active") is False:
                continue
            sap = item.get("sap") or item.get("SAP") or item.get("id") or ""
        else:
            sap = item
        if str(sap).strip():
            active_items.append(str(sap).strip())
    return json.dumps({
        "deliveryDays": sorted(
            str(day).lower() for day in (data.get("deliveryDays") or data.get("delivery_days") or [])
        ),
        "items": sorted(active_items),
        "active": data.get("isActive", True),
    }, sort_keys=True, separators=(",", ":"))


def _product_forecast_signature(data: dict) -> str:
    return json.dumps({
        "sap": str(data.get("sap") or ""),
        "effectiveCasePack": data.get("casePack") or data.get("tray") or 0,
        "active": data.get("active", data.get("isActive", data.get("is_active", True))),
    }, sort_keys=True, separators=(",", ":"))


def _schedule_forecast_signature(order_cycles: List[dict]) -> str:
    normalized = [normalize_order_cycle(cycle) for cycle in order_cycles]
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


# =============================================================================
# Route Discovery
# =============================================================================

def get_known_routes() -> Set[str]:
    """Get all known route numbers from routes_synced table."""
    conn = None
    try:
        conn = get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT route_number FROM routes_synced")
                return {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"  [!] Error fetching known routes: {e}")
        return set()
    finally:
        close_pg_connection(conn)


# =============================================================================
# Store Sync
# =============================================================================

def sync_store_to_pg(route_number: str, store_id: str, data: dict, deleted: bool = False) -> bool:
    """Sync a single store document to PostgreSQL.

    Args:
        route_number: The route this store belongs to
        store_id: The Firestore document ID
        data: The store document data
        deleted: If True, mark the store as inactive
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    try:
        with conn:
            with conn.cursor() as cur:
                if deleted:
                    # Mark store as inactive
                    cur.execute("""
                        UPDATE stores SET is_active = FALSE, synced_at = %s
                        WHERE store_id = %s
                    """, [now, store_id])

                    # Mark all store items as inactive
                    cur.execute("""
                        UPDATE store_items SET is_active = FALSE, removed_at = %s, synced_at = %s
                        WHERE store_id = %s
                    """, [now, now, store_id])

                    print(f"  [Store] Marked inactive: {store_id}")
                    return True

                # Extract delivery_days - can be array or string
                delivery_days = data.get('deliveryDays') or data.get('delivery_days') or []
                if isinstance(delivery_days, list):
                    delivery_days_str = json.dumps(delivery_days)
                else:
                    delivery_days_str = str(delivery_days) if delivery_days else '[]'

                # Upsert store
                cur.execute("""
                    INSERT INTO stores (store_id, route_number, store_name, store_number, address, delivery_days, is_active, synced_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (store_id) DO UPDATE SET
                        store_name = EXCLUDED.store_name,
                        store_number = EXCLUDED.store_number,
                        address = EXCLUDED.address,
                        delivery_days = EXCLUDED.delivery_days,
                        is_active = EXCLUDED.is_active,
                        synced_at = EXCLUDED.synced_at
                """, [
                    store_id,
                    route_number,
                    data.get('name', ''),
                    data.get('number', ''),
                    data.get('address', ''),
                    delivery_days_str,
                    data.get('isActive', True),
                    now,
                ])

                # Sync store items (items is an array field on the store doc, not a subcollection)
                items = data.get('items') or data.get('activeItems') or []
                sync_store_items(route_number, store_id, items, conn=conn)

                print(f"  [Store] Synced: {store_id} ({data.get('name', 'unnamed')}) - {len(items)} items")
                return True

    except Exception as e:
        print(f"  [!] Error syncing store {store_id}: {e}")
        return False
    finally:
        close_pg_connection(conn)


def _sync_store_items_inner(
    conn: psycopg2.extensions.connection,
    route_number: str,
    store_id: str,
    items: List[dict],
    now: str,
) -> None:
    # Get current SAPs from the items array
    current_saps = set()
    for item in items:
        if isinstance(item, dict):
            if item.get('isActive') is False or item.get('is_active') is False or item.get('active') is False:
                continue
            sap = item.get('sap') or item.get('SAP') or item.get('id')
        else:
            sap = str(item)
        if sap:
            current_saps.add(str(sap))

    with conn.cursor() as cur:
        # Get existing SAPs for this store
        cur.execute("""
            SELECT sap FROM store_items
            WHERE store_id = %s AND is_active = TRUE
        """, [store_id])
        existing_saps = {row[0] for row in cur.fetchall()}

        # Find new items (in current but not in existing)
        new_saps = current_saps - existing_saps

        # Find removed items (in existing but not in current)
        removed_saps = existing_saps - current_saps

        # Insert new items
        if new_saps:
            rows = []
            for sap in new_saps:
                item_id = f"{store_id}-{sap}"
                rows.append((item_id, store_id, route_number, sap, True, now, now))

            execute_values(
                cur,
                """
                INSERT INTO store_items (id, store_id, route_number, sap, is_active, added_at, synced_at)
                VALUES %s
                ON CONFLICT (store_id, sap) DO UPDATE SET
                    is_active = TRUE,
                    removed_at = NULL,
                    synced_at = EXCLUDED.synced_at
                """,
                rows,
            )

        # Mark removed items as inactive
        if removed_saps:
            cur.execute("""
                UPDATE store_items
                SET is_active = FALSE, removed_at = %s, synced_at = %s
                WHERE store_id = %s AND sap = ANY(%s)
            """, [now, now, store_id, list(removed_saps)])

        if new_saps or removed_saps:
            print(f"    [Items] +{len(new_saps)} -{len(removed_saps)} items for {store_id}")


def sync_store_items(
    route_number: str,
    store_id: str,
    items: List[dict],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """Sync store items array to store_items table.

    The items array contains objects with 'sap' field.
    We need to:
    1. Add new items
    2. Mark removed items as inactive
    """
    now = datetime.now(timezone.utc).isoformat()
    active_conn = conn or get_pg_connection()

    try:
        if conn is None:
            with active_conn:
                _sync_store_items_inner(active_conn, route_number, store_id, items, now)
        else:
            _sync_store_items_inner(active_conn, route_number, store_id, items, now)

    except Exception as e:
        print(f"  [!] Error syncing store items for {store_id}: {e}")
    finally:
        if conn is None:
            close_pg_connection(active_conn)


# =============================================================================
# Product Catalog Sync
# =============================================================================

def sync_product_to_pg(route_number: str, sap: str, data: dict, deleted: bool = False) -> bool:
    """Sync a single product document to PostgreSQL.

    Args:
        route_number: The route this product belongs to
        sap: The SAP code (document ID or sap field)
        data: The product document data
        deleted: If True, mark the product as inactive
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    try:
        with conn:
            with conn.cursor() as cur:
                if deleted:
                    cur.execute("""
                        UPDATE product_catalog SET is_active = FALSE, synced_at = %s
                        WHERE sap = %s AND route_number = %s
                    """, [now, sap, route_number])
                    print(f"  [Product] Marked inactive: {sap}")
                    return True

                # Get SAP from data if available (some docs use doc ID, some have sap field)
                actual_sap = data.get('sap', sap)

                cur.execute("""
                    INSERT INTO product_catalog (
                        sap, route_number, full_name, short_name, upc, brand, category,
                        sub_category, case_pack, tray, is_active, synced_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sap, route_number) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        short_name = EXCLUDED.short_name,
                        upc = EXCLUDED.upc,
                        brand = EXCLUDED.brand,
                        category = EXCLUDED.category,
                        sub_category = EXCLUDED.sub_category,
                        case_pack = EXCLUDED.case_pack,
                        tray = EXCLUDED.tray,
                        is_active = EXCLUDED.is_active,
                        synced_at = EXCLUDED.synced_at
                """, [
                    actual_sap,
                    route_number,
                    data.get('fullName', data.get('name', '')),
                    data.get('shortName', ''),
                    data.get('upc') or data.get('sku'),
                    data.get('brand', ''),
                    data.get('category', ''),
                    data.get('subCategory', ''),
                    data.get('casePack') or data.get('tray') or 0,
                    data.get('tray'),  # Can be None
                    data.get('active', data.get('isActive', data.get('is_active', True))),
                    now,
                ])

                print(f"  [Product] Synced: {actual_sap} ({data.get('fullName', data.get('name', 'unnamed'))})")
                return True

    except Exception as e:
        print(f"  [!] Error syncing product {sap}: {e}")
        return False
    finally:
        close_pg_connection(conn)


# =============================================================================
# User Schedule Sync
# =============================================================================

def sync_user_schedules_to_pg(route_number: str, user_id: str, order_cycles: List[dict]) -> bool:
    """Sync user order cycles to user_schedules table.

    Args:
        route_number: The route number (extracted from user doc)
        user_id: The Firestore user ID
        order_cycles: List of order cycle objects with orderDay, loadDay, deliveryDay
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    day_names = {
        1: 'monday', 2: 'tuesday', 3: 'wednesday', 4: 'thursday',
        5: 'friday', 6: 'saturday', 7: 'sunday',
    }

    try:
        with conn:
            with conn.cursor() as cur:
                # First mark all existing schedules for this route as inactive
                cur.execute("""
                    UPDATE user_schedules SET is_active = FALSE, synced_at = %s
                    WHERE route_number = %s
                """, [now, route_number])

                # Insert/update each cycle
                for i, cycle in enumerate(order_cycles):
                    normalized_cycle = normalize_order_cycle(cycle)
                    order_day = normalized_cycle['orderDay']
                    load_day = normalized_cycle['loadDay']
                    delivery_day = normalized_cycle['deliveryDay']
                    load_offset_days = normalized_cycle['loadOffsetDays']
                    delivery_offset_days = normalized_cycle['deliveryOffsetDays']
                    schedule_version = normalized_cycle['scheduleVersion']
                    needs_schedule_review = normalized_cycle['needsScheduleReview']
                    schedule_key = day_names.get(order_day, 'unknown')

                    schedule_id = f"{route_number}-cycle-{i}"

                    cur.execute("""
                        INSERT INTO user_schedules (
                            id, route_number, user_id, order_day, load_day, delivery_day,
                            load_offset_days, delivery_offset_days, schedule_version, needs_schedule_review,
                            schedule_key, is_active, synced_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
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
                    """, [
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
                    ])

                print(f"  [Schedule] Synced {len(order_cycles)} cycles for route {route_number}")
                return True

    except Exception as e:
        print(f"  [!] Error syncing schedules for route {route_number}: {e}")
        return False
    finally:
        close_pg_connection(conn)


def resolve_route_owner_user_id(
    fb_client: firestore.Client,
    route_number: str,
    fallback_user_id: str,
) -> str:
    """Resolve the same Firebase schedule authority used by forecast attachment.

    `routes_synced.user_id` is a discovery hint, not route ownership authority.
    Test/migrated routes can have multiple user profiles claiming the same route,
    so following that stale projection can sync one user's cycles while the
    generator reads `routes/{route}.ownerUid`. Prefer the route document pointer
    and retain the PostgreSQL user only as a compatibility fallback.
    """
    try:
        snapshot = fb_client.collection('routes').document(str(route_number)).get()
        if snapshot.exists:
            data = snapshot.to_dict() or {}
            owner_user_id = str(data.get('ownerUid') or data.get('userId') or '').strip()
            if owner_user_id:
                return owner_user_id
    except Exception as exc:
        print(
            f"  [Schedule] ⚠️ Could not resolve route owner for {route_number};"
            f" using discovery fallback: {exc}"
        )
    return str(fallback_user_id or '').strip()


# =============================================================================
# Firestore Listeners
# =============================================================================

class ConfigSyncManager:
    """Manages all config sync listeners for multiple routes."""

    def __init__(self, fb_client: firestore.Client):
        self.fb_client = fb_client
        self.watchers: List = []
        self.known_routes: Set[str] = set()
        self._store_signatures: Dict[tuple, str] = {}
        self._product_signatures: Dict[tuple, str] = {}
        self._schedule_signatures: Dict[str, str] = {}
        self._stores_initialized: Set[str] = set()
        self._products_initialized: Set[str] = set()
        self._schedules_initialized: Set[str] = set()
        self._refresh_timers: Dict[str, threading.Timer] = {}

    def _schedule_forecast_refresh(self, route_number: str, reason: str) -> None:
        previous = self._refresh_timers.pop(route_number, None)
        if previous:
            previous.cancel()

        def flush() -> None:
            self._refresh_timers.pop(route_number, None)
            try:
                from forecast_contract import load_authority_generation_state
                from forecast_generation_queue import (
                    derive_upcoming_generation_targets,
                    enqueue_generation_job,
                )
                for target in derive_upcoming_generation_targets(route_number):
                    _keys, revision = load_authority_generation_state(
                        self.fb_client,
                        route_number,
                        target["delivery_date"],
                        target["schedule_key"],
                    )
                    enqueue_generation_job(
                        route_number,
                        target["schedule_key"],
                        target["delivery_date"],
                        source="config_sync_listener",
                        desired_revision=revision,
                        refresh_reason="config_change",
                    )
            except Exception as exc:
                print(f"  [Forecast] Could not enqueue config refresh for {route_number}: {exc}")

        timer = threading.Timer(2.0, flush)
        timer.daemon = True
        self._refresh_timers[route_number] = timer
        timer.start()
        print(f"  [Forecast] Debounced refresh scheduled for {route_number}: {reason}")

    def start_stores_listener(self, route_number: str) -> None:
        """Start listening to stores collection for a route."""
        stores_ref = self.fb_client.collection('routes').document(route_number).collection('stores')

        def on_stores_snapshot(col_snapshot, changes, read_time):
            initial_batch = route_number not in self._stores_initialized
            changed = False
            for change in changes:
                doc = change.document
                store_id = doc.id
                data = doc.to_dict() or {}

                key = (route_number, store_id)
                previous_signature = self._store_signatures.get(key)
                next_signature = _store_forecast_signature(data)
                if change.type.name == 'ADDED' or change.type.name == 'MODIFIED':
                    synced = sync_store_to_pg(route_number, store_id, data, deleted=False)
                    self._store_signatures[key] = next_signature
                elif change.type.name == 'REMOVED':
                    synced = sync_store_to_pg(route_number, store_id, data, deleted=True)
                    self._store_signatures.pop(key, None)
                else:
                    synced = False
                changed = changed or bool(
                    synced and not initial_batch and (
                        change.type.name == 'REMOVED' or previous_signature != next_signature
                    )
                )
            self._stores_initialized.add(route_number)
            if changed:
                self._schedule_forecast_refresh(route_number, "store_or_carry_change")

        watcher = stores_ref.on_snapshot(on_stores_snapshot)
        self.watchers.append(watcher)
        print(f"  [Listener] Stores for route {route_number}")

    def start_products_listener(self, route_number: str) -> None:
        """Start listening to masterCatalog products for a route.

        NOTE: Uses masterCatalog/{route}/products which is the source of truth
        for casePack and tray fields.
        """
        products_ref = self.fb_client.collection('masterCatalog').document(route_number).collection('products')

        def on_products_snapshot(col_snapshot, changes, read_time):
            initial_batch = route_number not in self._products_initialized
            changed = False
            for change in changes:
                doc = change.document
                sap = doc.id
                data = doc.to_dict() or {}

                key = (route_number, sap)
                previous_signature = self._product_signatures.get(key)
                next_signature = _product_forecast_signature(data)
                if change.type.name == 'ADDED' or change.type.name == 'MODIFIED':
                    synced = sync_product_to_pg(route_number, sap, data, deleted=False)
                    self._product_signatures[key] = next_signature
                elif change.type.name == 'REMOVED':
                    synced = sync_product_to_pg(route_number, sap, data, deleted=True)
                    self._product_signatures.pop(key, None)
                else:
                    synced = False
                changed = changed or bool(
                    synced and not initial_batch and (
                        change.type.name == 'REMOVED' or previous_signature != next_signature
                    )
                )
            self._products_initialized.add(route_number)
            if changed:
                self._schedule_forecast_refresh(route_number, "product_or_case_pack_change")

        watcher = products_ref.on_snapshot(on_products_snapshot)
        self.watchers.append(watcher)
        print(f"  [Listener] Products for route {route_number}")

    def start_schedules_listener(self, user_id: str, route_number: str) -> None:
        """Start listening to user document for schedule changes.

        The orderCycles are at: userSettings.notifications.scheduling.orderCycles
        """
        user_ref = self.fb_client.collection('users').document(user_id)

        def on_user_snapshot(doc_snapshots, changes, read_time):
            try:
                print(f"  [Schedule] 📥 Snapshot received for user {user_id} (route {route_number}) at {read_time}")
                
                # on_snapshot passes a list of snapshots
                if not doc_snapshots:
                    print(f"  [Schedule] ⚠️ No snapshots received for user {user_id}")
                    return
                
                doc_snapshot = doc_snapshots[0] if isinstance(doc_snapshots, list) else doc_snapshots
                
                if not doc_snapshot.exists:
                    print(f"  [Schedule] ⚠️ User {user_id} no longer exists")
                    return

                data = doc_snapshot.to_dict() or {}

                # Try the correct nested path first
                order_cycles = (
                    data.get('userSettings', {})
                    .get('notifications', {})
                    .get('scheduling', {})
                    .get('orderCycles', [])
                )

                # Fallback to old path
                if not order_cycles:
                    order_cycles = data.get('settings', {}).get('orderCycles', [])

                if order_cycles:
                    print(f"  [Schedule] 🔄 Syncing {len(order_cycles)} order cycle(s) for route {route_number}")
                    # Log the cycles for debugging
                    days = {0: 'Sun', 1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat'}
                    for i, c in enumerate(order_cycles):
                        od = days.get(c.get('orderDay'), '?')
                        dd = days.get(c.get('deliveryDay'), '?')
                        print(f"    Cycle {i}: Order={od} -> Delivery={dd}")
                else:
                    print(f"  [Schedule] ⚠️ No orderCycles found for user {user_id}")
                signature = _schedule_forecast_signature(order_cycles)
                previous_signature = self._schedule_signatures.get(route_number)
                synced = sync_user_schedules_to_pg(route_number, user_id, order_cycles)
                self._schedule_signatures[route_number] = signature
                initialized = route_number in self._schedules_initialized
                self._schedules_initialized.add(route_number)
                if synced and initialized and previous_signature != signature:
                    self._schedule_forecast_refresh(route_number, "schedule_change")
            except Exception as e:
                print(f"  [Schedule] ❌ Error processing snapshot for {user_id}: {e}")
                import traceback
                traceback.print_exc()

        watcher = user_ref.on_snapshot(on_user_snapshot)
        self.watchers.append(watcher)
        print(f"  [Listener] Schedules for user {user_id} (route {route_number})")

    def start_route_listeners(self, route_number: str, user_id: str) -> None:
        """Start all listeners for a single route."""
        if route_number in self.known_routes:
            return  # Already listening

        authority_user_id = resolve_route_owner_user_id(
            self.fb_client,
            route_number,
            user_id,
        )
        if not authority_user_id:
            print(f"  [Schedule] ⚠️ No owner authority found for route {route_number}")
            return
        if authority_user_id != user_id:
            print(
                f"  [Schedule] Route {route_number} owner authority overrides"
                f" routes_synced user {user_id} -> {authority_user_id}"
            )

        print(f"\n[+] Starting listeners for route {route_number}")
        self.start_stores_listener(route_number)
        self.start_products_listener(route_number)
        self.start_schedules_listener(authority_user_id, route_number)
        self.known_routes.add(route_number)

    def discover_and_start_listeners(self) -> None:
        """Discover existing routes from PG and start listeners for each."""
        print("\n[*] Discovering existing routes from PostgreSQL...")

        conn = None
        try:
            conn = get_pg_connection()
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT route_number, user_id
                        FROM routes_synced
                        WHERE sync_status = 'ready'
                    """)
                    routes = cur.fetchall()

            if not routes:
                print("  [!] No synced routes found. Listeners will start when first order arrives.")
                return

            print(f"  Found {len(routes)} synced routes")

            for row in routes:
                route_number = row['route_number']
                user_id = row['user_id']
                if route_number and user_id:
                    self.start_route_listeners(route_number, user_id)

        except Exception as e:
            print(f"  [!] Error discovering routes: {e}")
        finally:
            close_pg_connection(conn)

    def watch_for_new_routes(self) -> None:
        """Watch routes_synced in PostgreSQL for new routes (via polling).

        This is a fallback mechanism - new routes are typically discovered
        when orders are finalized.
        """
        # We poll routes_synced periodically to catch any routes we might have missed
        # This is a low-frequency poll (every 5 minutes) as a safety net
        pass

    def stop_all(self) -> None:
        """Stop all watchers."""
        for watcher in self.watchers:
            try:
                watcher.unsubscribe()
            except:
                pass
        self.watchers.clear()
        for timer in self._refresh_timers.values():
            timer.cancel()
        self._refresh_timers.clear()
        self.known_routes.clear()
        print("\n[*] All listeners stopped")


# =============================================================================
# Main Entry Point
# =============================================================================

def main(sa_path: str):
    """Main entry point for config sync listener."""
    print(f"\n{'='*60}")
    print("Config Sync Listener (Firestore -> PostgreSQL)")
    print(f"{'='*60}")
    print(f"  Worker ID: {WORKER_ID}")
    print(f"  Service Account: {sa_path}")
    print(f"  PostgreSQL: {os.environ.get('POSTGRES_HOST', 'localhost')}/{os.environ.get('POSTGRES_DB', 'routespark')}")
    print(f"\n  Press Ctrl+C to stop\n")

    # Initialize clients
    fb_client = get_firestore_client(sa_path)

    # Test PostgreSQL connection
    conn = None
    try:
        conn = get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        print("  [OK] PostgreSQL connection verified")
    except Exception as e:
        print(f"  [!] PostgreSQL connection failed: {e}")
        return 1
    finally:
        close_pg_connection(conn)

    # Create sync manager and start listeners
    manager = ConfigSyncManager(fb_client)

    try:
        # Discover existing routes and start listeners
        manager.discover_and_start_listeners()

        # Keep process alive and periodically check for new routes
        check_interval = 300  # 5 minutes
        last_check = time.time()

        print(f"\n[*] Listening for config changes... (checking for new routes every {check_interval}s)")

        while True:
            time.sleep(60)

            # Periodically check for new routes
            current_time = time.time()
            if current_time - last_check >= check_interval:
                last_check = current_time

                # Check for newly synced routes
                conn = None
                try:
                    conn = get_pg_connection()
                    with conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT route_number, user_id
                                FROM routes_synced
                                WHERE sync_status = 'ready'
                            """)
                            routes = cur.fetchall()

                    for row in routes:
                        route_number = row['route_number']
                        user_id = row['user_id']
                        if route_number and user_id and route_number not in manager.known_routes:
                            print(f"\n[+] New route discovered: {route_number}")
                            manager.start_route_listeners(route_number, user_id)

                except Exception as e:
                    print(f"  [!] Error checking for new routes: {e}")
                finally:
                    close_pg_connection(conn)

    except KeyboardInterrupt:
        print("\n\n[*] Shutdown requested...")
    finally:
        manager.stop_all()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Config sync listener - real-time Firestore to PostgreSQL sync",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')

    args = parser.parse_args()

    exit(main(args.serviceAccount))

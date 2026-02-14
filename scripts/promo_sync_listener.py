#!/usr/bin/env python3
"""Real-time promo sync listener - syncs promo data from Firestore to PostgreSQL.

Provides event-driven sync for promotion data:
- promos/{route}/active/* -> promo_history + promo_items tables
- promos/{route}/history/* -> promo_history + promo_items tables (historical promos for training)

This replaces polling-based sync with real-time Firestore listeners.

Usage:
    python scripts/promo_sync_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import os
import socket
import time
from datetime import datetime, timezone
from typing import Optional, List, Set

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from google.cloud import firestore  # type: ignore

# Worker ID for this instance
WORKER_ID = f"promo-sync-{socket.gethostname()}-{os.getpid()}"

# =============================================================================
# PostgreSQL Connection
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


def get_firestore_client(sa_path: str) -> firestore.Client:
    """Create a Firestore client from service account."""
    return firestore.Client.from_service_account_json(sa_path)


# =============================================================================
# Promo History Sync
# =============================================================================

def sync_promo_to_pg(
    route_number: str,
    promo_id: str,
    data: dict,
    subcollection: str,
    deleted: bool = False
) -> None:
    """Sync a single promo document to PostgreSQL.

    Args:
        route_number: The route this promo belongs to
        promo_id: The Firestore document ID
        data: The promo document data
        subcollection: The subcollection name (active, history, seasonal)
        deleted: If True, mark the promo as inactive
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    try:
        with conn.cursor() as cur:
            if deleted:
                # Delete promo and its items
                cur.execute("""
                    DELETE FROM promo_items WHERE promo_id = %s
                """, [promo_id])
                cur.execute("""
                    DELETE FROM promo_history WHERE promo_id = %s
                """, [promo_id])
                print(f"  [Promo] Deleted: {promo_id}")
                return

            # Parse dates
            start_date = data.get('startDate') or data.get('start_date')
            end_date = data.get('endDate') or data.get('end_date')

            # Handle Firestore timestamps
            if hasattr(start_date, 'isoformat'):
                start_date = start_date.date().isoformat() if hasattr(start_date, 'date') else str(start_date)
            if hasattr(end_date, 'isoformat'):
                end_date = end_date.date().isoformat() if hasattr(end_date, 'date') else str(end_date)

            # Determine promo type from subcollection
            promo_type = data.get('promoType') or data.get('promo_type') or subcollection

            # Upsert promo_history
            cur.execute("""
                INSERT INTO promo_history (
                    promo_id, route_number, promo_name, promo_type,
                    start_date, end_date, discount_percent, discount_amount,
                    source_file, uploaded_by, uploaded_at, synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (promo_id) DO UPDATE SET
                    promo_name = EXCLUDED.promo_name,
                    promo_type = EXCLUDED.promo_type,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    discount_percent = EXCLUDED.discount_percent,
                    discount_amount = EXCLUDED.discount_amount,
                    source_file = EXCLUDED.source_file,
                    uploaded_by = EXCLUDED.uploaded_by,
                    uploaded_at = EXCLUDED.uploaded_at,
                    synced_at = EXCLUDED.synced_at
            """, [
                promo_id,
                route_number,
                data.get('promoName') or data.get('promo_name') or data.get('name', ''),
                promo_type,
                start_date,
                end_date,
                data.get('discountPercent') or data.get('discount_percent'),
                data.get('discountAmount') or data.get('discount_amount'),
                data.get('sourceFile') or data.get('source_file'),
                data.get('uploadedBy') or data.get('uploaded_by'),
                data.get('uploadedAt') or data.get('uploaded_at'),
                now,
            ])

            # Sync promo items
            items = data.get('items', [])
            affected_saps = data.get('affectedSaps', [])

            # If we have items with details, use those
            if items:
                sync_promo_items(promo_id, items)
            # Otherwise, create basic items from affectedSaps
            elif affected_saps:
                sync_promo_items_from_saps(promo_id, affected_saps, start_date, end_date)

            item_count = len(items) if items else len(affected_saps)
            print(f"  [Promo] Synced: {promo_id} ({data.get('promoName', 'unnamed')}) - {item_count} items")

    except Exception as e:
        print(f"  [!] Error syncing promo {promo_id}: {e}")


def sync_promo_items(promo_id: str, items: List[dict]) -> None:
    """Sync promo items array to promo_items table.

    Args:
        promo_id: The parent promo ID
        items: List of item dicts with sap, special_price, discount_percent, etc.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    try:
        with conn.cursor() as cur:
            # Delete existing items for this promo
            cur.execute("DELETE FROM promo_items WHERE promo_id = %s", [promo_id])

            # Insert new items
            if items:
                rows = []
                for item in items:
                    sap = item.get('sap') or item.get('sap_code') or item.get('sap_raw')
                    if not sap:
                        continue

                    # Parse dates from item if available
                    start_date = item.get('startDate') or item.get('start_date')
                    end_date = item.get('endDate') or item.get('end_date')

                    if hasattr(start_date, 'isoformat'):
                        start_date = start_date.date().isoformat() if hasattr(start_date, 'date') else str(start_date)
                    if hasattr(end_date, 'isoformat'):
                        end_date = end_date.date().isoformat() if hasattr(end_date, 'date') else str(end_date)

                    item_id = f"{promo_id}-{sap}"
                    account = item.get('account') or item.get('customer_account')

                    rows.append((
                        item_id,
                        promo_id,
                        sap,
                        account,
                        start_date,
                        end_date,
                        item.get('specialPrice') or item.get('special_price'),
                        item.get('discountPercent') or item.get('discount_percent'),
                        now,
                    ))

                if rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO promo_items (
                            id, promo_id, sap, account, start_date, end_date,
                            special_price, discount_percent, synced_at
                        )
                        VALUES %s
                        ON CONFLICT (promo_id, sap, account) DO UPDATE SET
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            special_price = EXCLUDED.special_price,
                            discount_percent = EXCLUDED.discount_percent,
                            synced_at = EXCLUDED.synced_at
                        """,
                        rows,
                    )

    except Exception as e:
        print(f"  [!] Error syncing promo items for {promo_id}: {e}")


def sync_promo_items_from_saps(
    promo_id: str,
    sap_codes: List[str],
    start_date: Optional[str],
    end_date: Optional[str]
) -> None:
    """Create basic promo_items from a list of SAP codes.

    Used when the promo doc only has affectedSaps without detailed item info.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = get_pg_connection()

    try:
        with conn.cursor() as cur:
            # Delete existing items for this promo
            cur.execute("DELETE FROM promo_items WHERE promo_id = %s", [promo_id])

            # Insert basic items
            if sap_codes:
                rows = []
                for sap in sap_codes:
                    if not sap:
                        continue
                    item_id = f"{promo_id}-{sap}"
                    rows.append((
                        item_id,
                        promo_id,
                        sap,
                        None,  # account
                        start_date,
                        end_date,
                        None,  # special_price
                        None,  # discount_percent
                        now,
                    ))

                if rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO promo_items (
                            id, promo_id, sap, account, start_date, end_date,
                            special_price, discount_percent, synced_at
                        )
                        VALUES %s
                        ON CONFLICT (promo_id, sap, account) DO UPDATE SET
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            synced_at = EXCLUDED.synced_at
                        """,
                        rows,
                    )

    except Exception as e:
        print(f"  [!] Error syncing promo items from SAPs for {promo_id}: {e}")


# =============================================================================
# Firestore Listeners
# =============================================================================

class PromoSyncManager:
    """Manages promo sync listeners for multiple routes."""

    def __init__(self, fb_client: firestore.Client):
        self.fb_client = fb_client
        self.watchers: List = []
        self.known_routes: Set[str] = set()

    def start_promo_subcollection_listener(self, route_number: str, subcollection: str) -> None:
        """Start listening to a promo subcollection for a route.

        Args:
            route_number: The route number
            subcollection: The subcollection name (active, history, seasonal)
        """
        promo_ref = self.fb_client.collection('promos').document(route_number).collection(subcollection)

        def on_promo_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                doc = change.document
                promo_id = doc.id
                data = doc.to_dict() or {}

                if change.type.name == 'ADDED' or change.type.name == 'MODIFIED':
                    sync_promo_to_pg(route_number, promo_id, data, subcollection, deleted=False)
                elif change.type.name == 'REMOVED':
                    sync_promo_to_pg(route_number, promo_id, data, subcollection, deleted=True)

        watcher = promo_ref.on_snapshot(on_promo_snapshot)
        self.watchers.append(watcher)
        print(f"  [Listener] Promos/{subcollection} for route {route_number}")

    def start_route_listeners(self, route_number: str) -> None:
        """Start all promo listeners for a single route."""
        if route_number in self.known_routes:
            return  # Already listening

        print(f"\n[+] Starting promo listeners for route {route_number}")

        # Listen to known subcollections
        # Note: Some routes may not have all subcollections
        subcollections = ['active', 'history', 'seasonal']
        for subcol in subcollections:
            try:
                self.start_promo_subcollection_listener(route_number, subcol)
            except Exception as e:
                print(f"  [!] Could not start listener for {subcol}: {e}")

        self.known_routes.add(route_number)

    def discover_promo_subcollections(self, route_number: str) -> List[str]:
        """Discover which promo subcollections exist for a route."""
        try:
            promo_doc_ref = self.fb_client.collection('promos').document(route_number)
            collections = promo_doc_ref.collections()
            return [col.id for col in collections]
        except Exception as e:
            print(f"  [!] Error discovering promo subcollections: {e}")
            return []

    def discover_and_start_listeners(self) -> None:
        """Discover existing routes from PG and start listeners for each."""
        print("\n[*] Discovering existing routes from PostgreSQL...")

        try:
            conn = get_pg_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT route_number
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
                if route_number:
                    # First discover which subcollections exist
                    subcols = self.discover_promo_subcollections(route_number)
                    if subcols:
                        print(f"  [Route {route_number}] Found promo subcollections: {subcols}")
                    self.start_route_listeners(route_number)

        except Exception as e:
            print(f"  [!] Error discovering routes: {e}")

    def stop_all(self) -> None:
        """Stop all watchers."""
        for watcher in self.watchers:
            try:
                watcher.unsubscribe()
            except:
                pass
        self.watchers.clear()
        self.known_routes.clear()
        print("\n[*] All promo listeners stopped")


# =============================================================================
# Main Entry Point
# =============================================================================

def main(sa_path: str):
    """Main entry point for promo sync listener."""
    print(f"\n{'='*60}")
    print("Promo Sync Listener (Firestore -> PostgreSQL)")
    print(f"{'='*60}")
    print(f"  Worker ID: {WORKER_ID}")
    print(f"  Service Account: {sa_path}")
    print(f"  PostgreSQL: {os.environ.get('POSTGRES_HOST', 'localhost')}/{os.environ.get('POSTGRES_DB', 'routespark')}")
    print(f"\n  Press Ctrl+C to stop\n")

    # Initialize clients
    fb_client = get_firestore_client(sa_path)

    # Test PostgreSQL connection
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("  [OK] PostgreSQL connection verified")
    except Exception as e:
        print(f"  [!] PostgreSQL connection failed: {e}")
        return 1

    # Create sync manager and start listeners
    manager = PromoSyncManager(fb_client)

    try:
        # Discover existing routes and start listeners
        manager.discover_and_start_listeners()

        # Keep process alive and periodically check for new routes
        check_interval = 300  # 5 minutes
        last_check = time.time()

        print(f"\n[*] Listening for promo changes... (checking for new routes every {check_interval}s)")

        while True:
            time.sleep(60)

            # Periodically check for new routes
            current_time = time.time()
            if current_time - last_check >= check_interval:
                last_check = current_time

                # Check for newly synced routes
                try:
                    conn = get_pg_connection()
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT route_number
                            FROM routes_synced
                            WHERE sync_status = 'ready'
                        """)
                        routes = cur.fetchall()

                    for row in routes:
                        route_number = row['route_number']
                        if route_number and route_number not in manager.known_routes:
                            print(f"\n[+] New route discovered: {route_number}")
                            manager.start_route_listeners(route_number)

                except Exception as e:
                    print(f"  [!] Error checking for new routes: {e}")

    except KeyboardInterrupt:
        print("\n\n[*] Shutdown requested...")
    finally:
        manager.stop_all()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Promo sync listener - real-time Firestore to PostgreSQL sync",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')

    args = parser.parse_args()

    exit(main(args.serviceAccount))

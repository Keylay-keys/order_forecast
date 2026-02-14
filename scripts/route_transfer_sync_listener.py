#!/usr/bin/env python3
"""Real-time route transfer sync listener - syncs cross-route transfer entries from Firestore to PostgreSQL.

Phase 1: Transfers are an accounting/audit layer (do not contaminate demand targets).

Firestore path (authoritative):
  routeTransfers/{routeGroupId}/transfers/{transferId}

Postgres table (analytical/audit):
  route_transfers

Usage:
  python scripts/route_transfer_sync_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import os
import socket
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from google.cloud import firestore  # type: ignore

# Worker ID for this instance
WORKER_ID = f"route-transfer-sync-{socket.gethostname()}-{os.getpid()}"

_pg_conn: Optional[psycopg2.extensions.connection] = None


def get_pg_connection() -> psycopg2.extensions.connection:
    """Get or create a PostgreSQL connection for direct DB access."""
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", 5432)),
            database=os.environ.get("POSTGRES_DB", "routespark"),
            user=os.environ.get("POSTGRES_USER", "routespark"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
        )
        _pg_conn.autocommit = True
    return _pg_conn


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


def _to_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    return None


def ensure_schema() -> None:
    """Create route_transfers table if missing.

    We keep this local (instead of importing pg_schema) so this listener is self-contained and safe to deploy.
    """
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS route_transfers (
                fs_doc_path TEXT PRIMARY KEY,
                route_group_id VARCHAR(20) NOT NULL,
                transfer_id VARCHAR(255) NOT NULL,

                transfer_date DATE NOT NULL,

                purchase_route_number VARCHAR(20) NOT NULL,
                from_route_number VARCHAR(20) NOT NULL,
                to_route_number VARCHAR(20) NOT NULL,

                sap VARCHAR(20) NOT NULL,
                units INTEGER NOT NULL,
                case_pack INTEGER NOT NULL,

                status VARCHAR(20) NOT NULL,
                reason VARCHAR(50),
                source_order_id VARCHAR(255),
                created_by VARCHAR(255),

                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_route_transfers_group_date ON route_transfers(route_group_id, transfer_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_route_transfers_from ON route_transfers(from_route_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_route_transfers_to ON route_transfers(to_route_number)")


def upsert_transfer(doc) -> None:
    data = doc.to_dict() or {}
    path = doc.reference.path  # routeTransfers/{routeGroupId}/transfers/{transferId}
    parts = path.split("/")
    if len(parts) < 4:
        print(f"  [!] Unexpected transfer path: {path}")
        return

    route_group_id = parts[1]
    transfer_id = parts[3]

    transfer_date = data.get("transferDate")
    purchase_route_number = data.get("purchaseRouteNumber") or data.get("fromRouteNumber") or ""
    from_route_number = data.get("fromRouteNumber") or ""
    to_route_number = data.get("toRouteNumber") or ""
    sap = str(data.get("sap") or "").strip()
    units = int(data.get("units") or 0)
    case_pack = int(data.get("casePack") or 0)
    status = str(data.get("status") or "committed")
    reason = data.get("reason")
    source_order_id = data.get("sourceOrderId")
    created_by = data.get("createdByUid")

    created_at = _to_dt(data.get("createdAt"))
    updated_at = _to_dt(data.get("updatedAt"))

    # Minimal sanity checks (don't crash the listener; just log and skip bad rows)
    if not route_group_id or not transfer_id:
        print(f"  [!] Missing identifiers for path: {path}")
        return
    if not transfer_date or not isinstance(transfer_date, str):
        print(f"  [!] Missing transferDate for {path}")
        return
    if not from_route_number or not to_route_number:
        print(f"  [!] Missing from/to routes for {path}")
        return
    if not sap:
        print(f"  [!] Missing sap for {path}")
        return
    if units <= 0 or case_pack <= 0:
        print(f"  [!] Invalid units/casePack for {path}: units={units} casePack={case_pack}")
        return

    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO route_transfers (
                fs_doc_path,
                route_group_id,
                transfer_id,
                transfer_date,
                purchase_route_number,
                from_route_number,
                to_route_number,
                sap,
                units,
                case_pack,
                status,
                reason,
                source_order_id,
                created_by,
                created_at,
                updated_at,
                synced_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s
            )
            ON CONFLICT (fs_doc_path) DO UPDATE SET
                route_group_id = EXCLUDED.route_group_id,
                transfer_id = EXCLUDED.transfer_id,
                transfer_date = EXCLUDED.transfer_date,
                purchase_route_number = EXCLUDED.purchase_route_number,
                from_route_number = EXCLUDED.from_route_number,
                to_route_number = EXCLUDED.to_route_number,
                sap = EXCLUDED.sap,
                units = EXCLUDED.units,
                case_pack = EXCLUDED.case_pack,
                status = EXCLUDED.status,
                reason = EXCLUDED.reason,
                source_order_id = EXCLUDED.source_order_id,
                created_by = EXCLUDED.created_by,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                synced_at = EXCLUDED.synced_at
            """,
            [
                path,
                route_group_id,
                transfer_id,
                transfer_date,
                purchase_route_number,
                from_route_number,
                to_route_number,
                sap,
                units,
                case_pack,
                status,
                reason,
                source_order_id,
                created_by,
                created_at,
                updated_at,
                datetime.now(timezone.utc),
            ],
        )

    print(f"  [Transfer] Upserted {route_group_id}/{transfer_id} {sap} {units}u ({from_route_number}->{to_route_number})")


def watch_all_transfers(sa_path: str) -> None:
    print("\n" + "=" * 70)
    print("🔁 Route Transfer Sync Listener (Firestore → PostgreSQL)")
    print(f"Worker: {WORKER_ID}")
    print("=" * 70)

    ensure_schema()
    db = get_firestore_client(sa_path)

    transfers_col = db.collection_group("transfers")

    def on_snapshot(col_snapshot, changes, read_time):
        try:
            for change in changes:
                if change.type.name not in ("ADDED", "MODIFIED"):
                    continue
                upsert_transfer(change.document)
        except Exception as e:
            print(f"  [!] Error processing transfer snapshot: {e}")

    watcher = transfers_col.on_snapshot(on_snapshot)
    print("[+] Listening for route transfer changes...")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n\n👋 Stopping transfer listener...")
        watcher.unsubscribe()


def main():
    parser = argparse.ArgumentParser(
        description="Route transfer sync listener - real-time Firestore to PostgreSQL sync",
    )
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    args = parser.parse_args()

    watch_all_transfers(args.serviceAccount)


if __name__ == "__main__":
    main()


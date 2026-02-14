"""Delivery manifest request listener.

Watches `deliveryRequests` collection and responds with delivery manifest data.
Uses direct PostgreSQL access via pg_utils (no message bus).

Usage:
    python scripts/delivery_manifest_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import socket
import time
from datetime import datetime

from google.cloud import firestore

try:
    from .pg_utils import get_delivery_manifest, get_store_delivery, fetch_all
except ImportError:
    from pg_utils import get_delivery_manifest, get_store_delivery, fetch_all

WORKER_ID = f"delivery-{socket.gethostname()}-{__import__('os').getpid()}"


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


def handle_request(doc_ref, data: dict) -> bool:
    """Process a delivery manifest request."""
    request_id = data.get("requestId", doc_ref.id)
    request_type = data.get("type")
    route_number = data.get("routeNumber")

    if not route_number or not request_type:
        doc_ref.update({
            "status": "error",
            "error": "Missing routeNumber or type",
            "workerId": WORKER_ID,
            "completedAt": firestore.SERVER_TIMESTAMP,
        })
        return False

    # Claim this request
    try:
        doc_ref.update({
            "status": "processing",
            "workerId": WORKER_ID,
            "processingAt": firestore.SERVER_TIMESTAMP,
        })
    except Exception as e:
        print(f"     ⚠️  Could not claim request: {e}")
        return False

    print(f"  📥 {request_type}: route={route_number} ({request_id[:20]}...)")

    start_time = time.time()
    result = None

    try:
        if request_type == "get_manifest":
            delivery_date = data.get("deliveryDate")
            if not delivery_date:
                result = {'error': 'Missing deliveryDate'}
            else:
                result = get_delivery_manifest(route_number, delivery_date)

        elif request_type == "get_store_delivery":
            delivery_date = data.get("deliveryDate")
            store_id = data.get("storeId")
            if not delivery_date or not store_id:
                result = {'error': 'Missing deliveryDate or storeId'}
            else:
                result = get_store_delivery(route_number, store_id, delivery_date)

        elif request_type in ("list_dates", "list_delivery_dates"):
            # Get distinct dates with deliveries
            rows = fetch_all("""
                SELECT DISTINCT delivery_date::text, COUNT(*) as item_count
                FROM delivery_allocations
                WHERE route_number = %s
                GROUP BY delivery_date
                ORDER BY delivery_date DESC
                LIMIT 60
            """, [route_number])

            dates = []
            for row in rows:
                dates.append({
                    'date': row['delivery_date'],
                    'itemCount': row['item_count'],
                })
            result = {'dates': dates}
        
        else:
            result = {'error': f'Unknown request type: {request_type}'}
        
        elapsed = time.time() - start_time
        
        # Write result
        if 'error' in result:
            doc_ref.update({
                "status": "error",
                "error": result['error'],
                "workerId": WORKER_ID,
                "completedAt": firestore.SERVER_TIMESTAMP,
            })
            print(f"     ❌ Error: {result['error'][:100]}")
            return False
        else:
            doc_ref.update({
                "status": "completed",
                "result": result,
                "workerId": WORKER_ID,
                "completedAt": firestore.SERVER_TIMESTAMP,
            })
            print(f"     ✅ Done in {elapsed*1000:.0f}ms")
            return True
            
    except Exception as e:
        print(f"     ❌ Exception: {e}")
        doc_ref.update({
            "status": "error",
            "error": str(e),
            "workerId": WORKER_ID,
            "completedAt": firestore.SERVER_TIMESTAMP,
        })
        return False


def watch_requests(sa_path: str):
    """Watch deliveryRequests collection using real-time on_snapshot."""
    print(f"\n📦 Delivery Manifest Listener")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Using: Direct PostgreSQL (pg_utils)")
    print(f"   Watching: deliveryRequests/*")
    print(f"\n   Press Ctrl+C to stop\n")

    fb_client = get_firestore_client(sa_path)
    requests_col = fb_client.collection("deliveryRequests")

    def on_snapshot(col_snapshot, changes, read_time):
        """Handle collection changes."""
        for change in changes:
            if change.type.name not in ('ADDED', 'MODIFIED'):
                continue

            doc = change.document
            data = doc.to_dict() or {}

            if data.get("status") != "pending":
                continue

            try:
                handle_request(doc.reference, data)
            except Exception as e:
                print(f"     ❌ Unexpected error: {e}")
    
    watcher = requests_col.on_snapshot(on_snapshot)
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n👋 Stopping listener...")
        watcher.unsubscribe()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delivery manifest listener - handles manifest requests via DB Manager",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    
    args = parser.parse_args()
    
    watch_requests(args.serviceAccount)


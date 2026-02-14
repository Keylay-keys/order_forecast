"""Archive request listener - handles app requests for historical orders.

Uses direct PostgreSQL access via pg_utils (no message bus).
Watches `orderRequests` collection for archive requests from the app.

Usage:
    python scripts/order_archive_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import socket
import time
from datetime import datetime, timezone

from google.cloud import firestore  # type: ignore

# Import pg_utils for direct PostgreSQL access
try:
    from .pg_utils import get_archived_dates, get_order_by_date
except ImportError:
    from pg_utils import get_archived_dates, get_order_by_date

# Worker ID for this instance
WORKER_ID = f"archive-{socket.gethostname()}-{__import__('os').getpid()}"


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


# =============================================================================
# Request Handlers
# =============================================================================

def handle_list_dates(route_number: str) -> dict:
    """Handle request to list archived order dates."""
    try:
        dates = get_archived_dates(route_number)
        return {'dates': dates}
    except Exception as e:
        return {'error': str(e)}


def handle_get_order(route_number: str, delivery_date: str) -> dict:
    """Handle request to get a specific archived order."""
    try:
        order = get_order_by_date(route_number, delivery_date)
        if not order:
            return {'error': f'No order found for {delivery_date}'}
        return {'order': order}
    except Exception as e:
        return {'error': str(e)}


def handle_request(doc_ref, data: dict) -> bool:
    """Process a single archive request."""
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
    
    # Process based on type
    start_time = time.time()
    result = None
    
    try:
        if request_type == "list_dates":
            result = handle_list_dates(route_number)

        elif request_type == "get_order":
            delivery_date = data.get("deliveryDate")
            if not delivery_date:
                result = {'error': 'Missing deliveryDate'}
            else:
                result = handle_get_order(route_number, delivery_date)
        
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
        elapsed = time.time() - start_time
        print(f"     ❌ Exception: {e}")
        doc_ref.update({
            "status": "error",
            "error": str(e),
            "workerId": WORKER_ID,
            "completedAt": firestore.SERVER_TIMESTAMP,
        })
        return False


# =============================================================================
# Real-time Listener
# =============================================================================

def watch_requests(sa_path: str):
    """Watch orderRequests collection using real-time on_snapshot."""
    print(f"\n🎧 Order Archive Listener")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Using: Direct PostgreSQL (pg_utils)")
    print(f"   Watching: orderRequests/*")
    print(f"\n   Press Ctrl+C to stop\n")

    fb_client = get_firestore_client(sa_path)
    requests_col = fb_client.collection("orderRequests")
    
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
            
            # Process the request
            try:
                handle_request(doc.reference, data)
            except Exception as e:
                print(f"     ❌ Unexpected error: {e}")
    
    # Start real-time listener
    watcher = requests_col.on_snapshot(on_snapshot)
    
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
        description="Archive listener - handles order history requests via DB Manager",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    # --duckdb is no longer needed since we use direct PostgreSQL
    parser.add_argument('--duckdb', help='(deprecated) Not used - DB Manager handles database')
    
    args = parser.parse_args()
    
    watch_requests(args.serviceAccount)

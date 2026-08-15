"""Archive request listener - handles app requests for historical orders.

Uses direct PostgreSQL access via pg_utils (no message bus).
Watches `orderRequests` collection for archive requests from the app.

Usage:
    python scripts/order_archive_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import os
import socket
import time

from google.cloud import firestore  # type: ignore

# Import pg_utils for direct PostgreSQL access
try:
    from .pg_utils import get_archived_dates, get_order_by_date, get_order_by_id
except ImportError:
    from pg_utils import get_archived_dates, get_order_by_date, get_order_by_id

# Worker ID for this instance
WORKER_ID = f"archive-{socket.gethostname()}-{__import__('os').getpid()}"


def _allowed_routes() -> set[str] | None:
    raw = os.environ.get("ROUTESPARK_ALLOWED_ROUTES", "").strip()
    if not raw:
        return None
    values = {item.strip() for item in raw.split(",") if item.strip()}
    return values or None


def _route_allowed(route_number: str | None) -> bool:
    if not route_number:
        return False
    allowed = _allowed_routes()
    return True if allowed is None else str(route_number) in allowed


def _user_has_route_access(user_data: dict, route_number: str) -> bool:
    """Mirror Firestore hasAccessToRoute for listener defense in depth."""
    if not isinstance(user_data, dict) or not route_number:
        return False

    profile = user_data.get("profile")
    profile = profile if isinstance(profile, dict) else {}
    assignments = user_data.get("routeAssignments")
    assignments = assignments if isinstance(assignments, dict) else {}

    role = profile.get("role")
    owner_scoped = role in ("owner", "ownerOnly") and (
        profile.get("routeNumber") == route_number
        or profile.get("currentRoute") == route_number
        or (
            isinstance(profile.get("additionalRoutes"), list)
            and route_number in profile["additionalRoutes"]
        )
    )
    explicitly_assigned = route_number in assignments and assignments.get(route_number) is not None
    return owner_scoped or explicitly_assigned


def _requester_has_route_access(fb_client: firestore.Client, user_id: object, route_number: str) -> bool:
    if not isinstance(user_id, str) or not user_id:
        return False
    user_doc = fb_client.collection("users").document(user_id).get()
    if not user_doc.exists:
        return False
    return _user_has_route_access(user_doc.to_dict() or {}, route_number)


def _reject_unauthorized_request(doc_ref) -> None:
    doc_ref.update({
        "status": "error",
        "error": "Archive request is not authorized",
        "workerId": WORKER_ID,
        "completedAt": firestore.SERVER_TIMESTAMP,
    })


def _skip_initial_snapshot() -> bool:
    return os.environ.get("ROUTESPARK_SKIP_INITIAL_ORDER_ARCHIVE_SNAPSHOT", "0").lower() in ("1", "true", "yes")


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


# =============================================================================
# Request Handlers
# =============================================================================

def handle_list_dates(route_number: str) -> dict:
    """Handle request to list archived order dates."""
    try:
        return {'dates': get_archived_dates(route_number)}
    except Exception as e:
        return {'error': str(e)}


def handle_get_order(
    route_number: str,
    order_id: str | None = None,
    delivery_date: str | None = None,
) -> dict:
    """Handle request to get a specific archived order."""
    try:
        order = (
            get_order_by_id(route_number, order_id)
            if order_id
            else get_order_by_date(route_number, str(delivery_date))
        )
        if not order:
            return {'error': 'No archived order found'}
        return {'order': order}
    except Exception as e:
        return {'error': str(e)}


def handle_request(doc_ref, data: dict, fb_client: firestore.Client) -> bool:
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

    if not _route_allowed(route_number):
        return False

    try:
        if not _requester_has_route_access(fb_client, data.get("userId"), str(route_number)):
            _reject_unauthorized_request(doc_ref)
            print(f"     ⛔ Unauthorized archive request rejected ({request_id[:20]}...)")
            return False
    except Exception as e:
        # Fail closed if the user membership check cannot be completed.
        print(f"     ⚠️  Archive authorization check failed: {type(e).__name__}")
        _reject_unauthorized_request(doc_ref)
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
            order_id = data.get("orderId")
            delivery_date = data.get("deliveryDate")
            if not order_id and not delivery_date:
                result = {'error': 'Missing orderId'}
            else:
                result = handle_get_order(
                    route_number,
                    order_id=str(order_id) if order_id else None,
                    delivery_date=str(delivery_date) if delivery_date else None,
                )
        
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
    initial_snapshot_seen = False
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle collection changes."""
        nonlocal initial_snapshot_seen

        if not initial_snapshot_seen:
            initial_snapshot_seen = True
            if _skip_initial_snapshot():
                primed = sum(1 for change in changes if change.type.name in ("ADDED", "MODIFIED"))
                print(f"   Initial snapshot skipped; primed {primed} existing request(s)")
                return

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
                handle_request(doc.reference, data, fb_client)
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
        description="Archive listener - handles order history requests via PostgreSQL",
    )
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase service account JSON')
    
    args = parser.parse_args()
    
    watch_requests(args.serviceAccount)

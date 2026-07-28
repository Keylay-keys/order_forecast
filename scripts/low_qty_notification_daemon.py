"""Low-Quantity Notification Daemon

Sends push notifications for low-qty items that need ordering today.
Uses snapshot listener to track users with reminders enabled.
At each user's reminder time, computes low-qty items and sends notification.

Usage:
    python scripts/low_qty_notification_daemon.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

import pytz
import requests
from google.cloud import firestore
from google.cloud.firestore_v1.watch import Watch

try:
    from .pg_utils import fetch_one, execute
    from .low_quantity_loader import get_items_for_order_date, get_user_timezone
except ImportError:
    from pg_utils import fetch_one, execute
    from low_quantity_loader import get_items_for_order_date, get_user_timezone

WORKER_ID = f"low-qty-notif-{socket.gethostname()}-{os.getpid()}"
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
LOW_QTY_NOTIFICATIONS_ENABLED = os.environ.get("LOW_QTY_NOTIFICATIONS_ENABLED", "true").lower() == "true"
LOW_QTY_NOTIFICATION_DRY_RUN = os.environ.get("LOW_QTY_NOTIFICATION_DRY_RUN", "false").lower() == "true"
CHECK_INTERVAL_SECONDS = int(os.environ.get("LOW_QTY_NOTIFICATION_CHECK_INTERVAL_SECONDS", "60"))
DEFAULT_REMINDER_TOLERANCE_MINUTES = int(os.environ.get("LOW_QTY_NOTIFICATION_TOLERANCE_MINUTES", "2"))
DEFAULT_ONCE_LATE_TOLERANCE_MINUTES = int(
    os.environ.get("LOW_QTY_NOTIFICATION_ONCE_LATE_TOLERANCE_MINUTES", "20")
)

# In-memory cache of users with reminders enabled
# Updated by snapshot listener
reminder_cache: Dict[str, Dict] = {}
# {
#     "user_abc123": {
#         "route_number": "989262",
#         "reminder_time": {"hour": 8, "minute": 0, "period": "AM"},
#         "timezone": "America/Denver",
#     }
# }

# Keep reference to watcher to prevent garbage collection
_users_watcher: Optional[Watch] = None


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


def get_firestore_client(sa_path: str) -> firestore.Client:
    """Initialize Firestore client."""
    return firestore.Client.from_service_account_json(sa_path)


def get_route_owner(db: firestore.Client, route_number: str) -> Optional[str]:
    """Get the owner user_id for a route.
    
    Returns:
        The owner UID from the best available route metadata source, or None if not found.
    """
    try:
        route_doc = db.collection("routes").document(route_number).get()
        if route_doc.exists:
            data = route_doc.to_dict()
            # Check both field names (ownerUid is current, userId is legacy)
            owner_uid = data.get("ownerUid") or data.get("userId")
            if owner_uid:
                return owner_uid

        entitlement_doc = db.collection("routeEntitlements").document(route_number).get()
        if entitlement_doc.exists:
            data = entitlement_doc.to_dict() or {}
            owner_uid = data.get("ownerUid")
            if owner_uid:
                return owner_uid

        route_number_doc = db.collection("routeNumbers").document(route_number).get()
        if route_number_doc.exists:
            data = route_number_doc.to_dict() or {}
            owner_uid = data.get("userId") or data.get("userID")
            if owner_uid:
                return owner_uid
    except Exception as e:
        print(f"    [route] Error getting owner for route {route_number}: {e}")
    return None


def setup_reminder_listener(db: firestore.Client) -> Watch:
    """Set up snapshot listener on users with reminders enabled.
    
    Updates reminder_cache when user settings change.
    
    Returns:
        Watch handle (must be kept to prevent garbage collection)
    """
    global _users_watcher
    
    users_ref = db.collection("users")
    
    def on_snapshot(col_snapshot, changes, read_time):
        """Handle collection changes."""
        for change in changes:
            doc = change.document
            user_id = doc.id
            data = doc.to_dict() or {}
            
            # Get reminder settings
            user_settings = data.get("userSettings", {})
            notifications = user_settings.get("notifications", {})
            order_reminders = notifications.get("orderReminders", {})
            
            enabled = order_reminders.get("enabled", False)
            reminder_time = order_reminders.get("time", {})
            
            # Get route number from profile
            # Prefer currentRoute (active route), fall back to routeNumber (legacy)
            profile = data.get("profile", {})
            route_number = profile.get("currentRoute") or profile.get("routeNumber")
            timezone = profile.get("timezone", "America/Denver")

            if not _route_allowed(route_number):
                if user_id in reminder_cache:
                    del reminder_cache[user_id]
                continue
            
            if change.type.name == "REMOVED":
                if user_id in reminder_cache:
                    del reminder_cache[user_id]
                    print(f"  [cache] Removed user {user_id}")
            elif enabled and route_number and reminder_time:
                reminder_cache[user_id] = {
                    "route_number": route_number,
                    "reminder_time": reminder_time,
                    "timezone": timezone,
                }
                print(f"  [cache] Updated user {user_id}: route={route_number}, time={reminder_time}")
            elif user_id in reminder_cache:
                # User disabled reminders
                del reminder_cache[user_id]
                print(f"  [cache] Removed user {user_id} (reminders disabled)")
    
    # Start listening - IMPORTANT: keep reference to prevent GC
    _users_watcher = users_ref.on_snapshot(on_snapshot)
    print(f"  [listener] Watching users collection for reminder changes")
    
    return _users_watcher


def load_reminder_cache_once(db: firestore.Client) -> int:
    """Populate reminder cache from a one-time users scan."""
    reminder_cache.clear()
    users_ref = db.collection("users")
    count = 0

    for doc in users_ref.stream():
        user_id = doc.id
        data = doc.to_dict() or {}

        user_settings = data.get("userSettings", {})
        notifications = user_settings.get("notifications", {})
        order_reminders = notifications.get("orderReminders", {})

        enabled = order_reminders.get("enabled", False)
        reminder_time = order_reminders.get("time", {})

        profile = data.get("profile", {})
        route_number = profile.get("currentRoute") or profile.get("routeNumber")
        timezone = profile.get("timezone", "America/Denver")

        if not _route_allowed(route_number):
            continue

        if enabled and route_number and reminder_time:
            reminder_cache[user_id] = {
                "route_number": route_number,
                "reminder_time": reminder_time,
                "timezone": timezone,
            }
            count += 1

    return count


def reminder_time_to_minutes(reminder_time: Dict) -> int:
    """Convert reminder time to minutes since midnight.
    
    Args:
        reminder_time: {"hour": 8, "minute": 0, "period": "AM"}
    
    Returns:
        Minutes since midnight (0-1439)
    """
    hour = reminder_time.get("hour", 8)
    minute = reminder_time.get("minute", 0)
    period = reminder_time.get("period", "AM")
    
    # Convert to 24h
    if period == "PM" and hour != 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0
    
    return hour * 60 + minute


def is_reminder_time_now(reminder_time: Dict, timezone: str, tolerance_minutes: int = 2) -> bool:
    """Check if current time matches user's reminder time.
    
    Args:
        reminder_time: {"hour": 8, "minute": 0, "period": "AM"}
        timezone: e.g., "America/Denver"
        tolerance_minutes: How many minutes before/after to match
    
    Returns:
        True if within tolerance of reminder time
    """
    try:
        tz = pytz.timezone(timezone)
    except pytz.UnknownTimeZoneError:
        tz = pytz.timezone("America/Denver")
    
    now = datetime.now(tz)
    current_minutes = now.hour * 60 + now.minute
    target_minutes = reminder_time_to_minutes(reminder_time)
    
    return abs(current_minutes - target_minutes) <= tolerance_minutes


def is_reminder_time_due(
    reminder_time: Dict,
    timezone: str,
    *,
    early_tolerance_minutes: int = 2,
    late_tolerance_minutes: int = 2,
) -> bool:
    """Check whether the reminder is due in the user's local day.

    CronJob mode uses a larger late tolerance so a delayed or skipped run can
    still send the reminder once. The sent-table dedupe prevents repeat sends.
    """
    try:
        tz = pytz.timezone(timezone)
    except pytz.UnknownTimeZoneError:
        tz = pytz.timezone("America/Denver")

    now = datetime.now(tz)
    current_minutes = now.hour * 60 + now.minute
    target_minutes = reminder_time_to_minutes(reminder_time)
    delta = current_minutes - target_minutes

    return -early_tolerance_minutes <= delta <= late_tolerance_minutes


def get_fcm_tokens(db: firestore.Client, user_id: str) -> List[str]:
    """Get FCM tokens for a user."""
    user_doc = db.collection("users").document(user_id).get()
    if not user_doc.exists:
        return []
    return user_doc.to_dict().get("fcmTokens", [])


def check_already_sent(route_number: str, user_id: str,
                       order_date: str, saps_hash: str) -> bool:
    """Check if this notification was already sent today for this user.

    Dedup includes user_id to allow multiple users on same route to get notifications.
    """
    row = fetch_one("""
        SELECT 1 FROM low_qty_notifications_sent
        WHERE route_number = %s AND user_id = %s AND order_by_date = %s AND saps_hash = %s
        LIMIT 1
    """, [route_number, user_id, order_date, saps_hash])

    return row is not None


def mark_as_sent(route_number: str, user_id: str,
                 order_date: str, saps: List[str], saps_hash: str) -> None:
    """Record that notification was sent."""
    execute("""
        INSERT INTO low_qty_notifications_sent
        (route_number, user_id, order_by_date, saps, saps_hash, items_count, sent_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (route_number, user_id, order_by_date, saps_hash) DO NOTHING
    """, [
        route_number, user_id, order_date,
        json.dumps(saps), saps_hash, len(saps),
        datetime.utcnow().isoformat()
    ])


def is_valid_expo_token(token: str) -> bool:
    """Check if token is a valid Expo push token.
    
    Accepts both ExponentPushToken[...] and ExpoPushToken[...] formats.
    """
    if not token:
        return False
    return token.startswith("ExponentPushToken[") or token.startswith("ExpoPushToken[")


def send_push_notification(fcm_tokens: List[str], title: str, body: str, data: Dict) -> bool:
    """Send push notification via Expo Push API.
    
    Args:
        fcm_tokens: List of Expo push tokens
        title: Notification title
        body: Notification body
        data: Data payload for deep linking
    
    Returns:
        True if at least one notification was sent successfully
    """
    if not fcm_tokens:
        return False
    
    messages = []
    for token in fcm_tokens:
        if not is_valid_expo_token(token):
            continue
        messages.append({
            "to": token,
            "title": title,
            "body": body,
            "data": data,
            "sound": "default",
            "priority": "high",
        })
    
    if not messages:
        return False
    
    # Expo Push API limits batches to 100 messages
    BATCH_SIZE = 100
    total_success = 0
    total_error = 0
    
    for i in range(0, len(messages), BATCH_SIZE):
        batch = messages[i:i + BATCH_SIZE]
        try:
            response = requests.post(
                EXPO_PUSH_URL,
                json=batch,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            result = response.json()
            
            # Check for errors
            if "data" in result:
                success_count = sum(1 for r in result["data"] if r.get("status") == "ok")
                error_count = len(result["data"]) - success_count
                total_success += success_count
                total_error += error_count
            elif response.status_code == 200:
                total_success += len(batch)
                
        except Exception as e:
            print(f"    [push] Batch error: {e}")
            total_error += len(batch)
    
    if total_error > 0:
        print(f"    [push] {total_success} sent, {total_error} failed")
    
    return total_success > 0


def check_and_notify(
    db: firestore.Client,
    *,
    early_tolerance_minutes: int = DEFAULT_REMINDER_TOLERANCE_MINUTES,
    late_tolerance_minutes: int = DEFAULT_REMINDER_TOLERANCE_MINUTES,
) -> int:
    """Check all users and send notifications if it's their reminder time.

    Called every 60 seconds by the main loop.
    Only notifies route owners (userId field on route document).
    """
    if not reminder_cache:
        return 0

    failure_count = 0
    
    for user_id, user_data in list(reminder_cache.items()):
        reminder_time = user_data["reminder_time"]
        timezone = user_data["timezone"]
        route_number = user_data["route_number"]

        if not _route_allowed(route_number):
            continue
        
        # Check if it's reminder time for this user
        if not is_reminder_time_due(
            reminder_time,
            timezone,
            early_tolerance_minutes=early_tolerance_minutes,
            late_tolerance_minutes=late_tolerance_minutes,
        ):
            continue
        
        print(f"  [check] Reminder time for user {user_id} (route {route_number})")
        
        # Only notify route owner to prevent duplicate notifications
        # Fail closed: if owner is unknown, skip to avoid notifying wrong user
        route_owner = get_route_owner(db, route_number)
        if not route_owner:
            print(f"    Skipping: route {route_number} has no owner (userId field missing)")
            continue
        if route_owner != user_id:
            print(f"    Skipping: user {user_id} is not route owner (owner: {route_owner})")
            continue
        
        try:
            # Get today in user's timezone
            # Note: get_items_for_order_date uses route-owner timezone internally,
            # which should match since we're only notifying owners
            try:
                tz = pytz.timezone(timezone)
            except pytz.UnknownTimeZoneError:
                tz = pytz.timezone("America/Denver")
            today = datetime.now(tz).strftime("%Y-%m-%d")
            
            # Get low-qty items for today
            items = get_items_for_order_date(db, route_number, today)
            
            if not items:
                print(f"    No low-qty items for route {route_number}")
                continue
            
            # Compute dedup hash
            saps = sorted([item.sap for item in items])
            saps_hash = hashlib.md5(json.dumps(saps).encode()).hexdigest()
            
            # Check if already sent (per user to allow team members if added later)
            if check_already_sent(route_number, user_id, today, saps_hash):
                print(f"    Already sent notification for route {route_number} today")
                continue
            
            # Get FCM tokens (re-read to get latest)
            tokens = get_fcm_tokens(db, user_id)
            if not tokens:
                print(f"    No FCM tokens for user {user_id}")
                continue

            # Build and send notification
            item_count = len(items)
            title = "Low Stock Alert"
            body = f"{item_count} item{'s' if item_count != 1 else ''} need to be ordered today"
            data = {
                "type": "low_quantity",
                "routeNumber": route_number,
                "orderDate": today,
                "saps": saps,
            }

            if LOW_QTY_NOTIFICATION_DRY_RUN:
                print(
                    f"    [dry-run] Would send notification: {item_count} items "
                    f"to {len(tokens)} token(s) for route {route_number}"
                )
                continue

            if send_push_notification(tokens, title, body, data):
                mark_as_sent(route_number, user_id, today, saps, saps_hash)
                print(f"    ✅ Sent notification: {item_count} items")
            else:
                print(f"    ❌ Failed to send notification")
                failure_count += 1
                
        except Exception as e:
            print(f"    ❌ Error processing user {user_id}: {e}")
            failure_count += 1

    return failure_count


def run_daemon(
    sa_path: str,
    *,
    run_once: bool = False,
    once_late_tolerance_minutes: int = DEFAULT_ONCE_LATE_TOLERANCE_MINUTES,
) -> None:
    """Main daemon loop."""
    global _users_watcher

    print(f"\n📦 Low-Quantity Notification Daemon")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Enabled: {LOW_QTY_NOTIFICATIONS_ENABLED}")
    print(f"   Dry Run: {LOW_QTY_NOTIFICATION_DRY_RUN}")
    print(f"   Using: Direct PostgreSQL (pg_utils)")
    print(f"   Checking every {CHECK_INTERVAL_SECONDS} seconds")
    print(f"\n   Press Ctrl+C to stop\n")

    db = get_firestore_client(sa_path)

    if run_once:
        loaded = load_reminder_cache_once(db)
        print(f"  [cache] {loaded} users with reminders enabled (one-time scan)")
        if LOW_QTY_NOTIFICATIONS_ENABLED:
            failure_count = check_and_notify(
                db,
                early_tolerance_minutes=DEFAULT_REMINDER_TOLERANCE_MINUTES,
                late_tolerance_minutes=once_late_tolerance_minutes,
            )
            if failure_count:
                raise RuntimeError(
                    f"Low-quantity notification cycle failed for {failure_count} user(s)"
                )
        else:
            print("  [skip] LOW_QTY_NOTIFICATIONS_ENABLED=false; notification cycle skipped")
        return

    # Set up snapshot listener for users with reminders
    # Keep reference to prevent garbage collection
    _users_watcher = setup_reminder_listener(db)

    # Give listener time to populate cache
    time.sleep(3)
    print(f"  [cache] {len(reminder_cache)} users with reminders enabled")

    try:
        while True:
            if not LOW_QTY_NOTIFICATIONS_ENABLED:
                print("  [skip] LOW_QTY_NOTIFICATIONS_ENABLED=false; notification cycle skipped")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue
            check_and_notify(db)
            time.sleep(CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n\n👋 Stopping daemon...")
        if _users_watcher:
            _users_watcher.unsubscribe()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Low-quantity notification daemon",
    )
    parser.add_argument(
        '--serviceAccount', 
        required=True, 
        help='Path to Firebase service account JSON'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run a single reminder scan/check cycle and exit'
    )
    parser.add_argument(
        '--once-late-tolerance-minutes',
        type=int,
        default=DEFAULT_ONCE_LATE_TOLERANCE_MINUTES,
        help='Late catch-up window for --once CronJob mode'
    )
    
    args = parser.parse_args()
    run_daemon(
        args.serviceAccount,
        run_once=bool(args.once),
        once_late_tolerance_minutes=args.once_late_tolerance_minutes,
    )

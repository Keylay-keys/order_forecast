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

WORKER_ID = f"low-qty-notif-{socket.gethostname()}-{__import__('os').getpid()}"
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"

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


def get_firestore_client(sa_path: str) -> firestore.Client:
    """Initialize Firestore client."""
    return firestore.Client.from_service_account_json(sa_path)


def get_route_owner(db: firestore.Client, route_number: str) -> Optional[str]:
    """Get the owner user_id for a route.
    
    Returns:
        The ownerUid (or userId) field from routes/{route_number}, or None if not found.
    """
    try:
        route_doc = db.collection("routes").document(route_number).get()
        if route_doc.exists:
            data = route_doc.to_dict()
            # Check both field names (ownerUid is current, userId is legacy)
            return data.get("ownerUid") or data.get("userId")
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


def check_and_notify(db: firestore.Client) -> None:
    """Check all users and send notifications if it's their reminder time.

    Called every 60 seconds by the main loop.
    Only notifies route owners (userId field on route document).
    """
    if not reminder_cache:
        return
    
    for user_id, user_data in list(reminder_cache.items()):
        reminder_time = user_data["reminder_time"]
        timezone = user_data["timezone"]
        route_number = user_data["route_number"]
        
        # Check if it's reminder time for this user
        if not is_reminder_time_now(reminder_time, timezone):
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
            
            if send_push_notification(tokens, title, body, data):
                mark_as_sent(route_number, user_id, today, saps, saps_hash)
                print(f"    ✅ Sent notification: {item_count} items")
            else:
                print(f"    ❌ Failed to send notification")
                
        except Exception as e:
            print(f"    ❌ Error processing user {user_id}: {e}")


def run_daemon(sa_path: str) -> None:
    """Main daemon loop."""
    global _users_watcher

    print(f"\n📦 Low-Quantity Notification Daemon")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Using: Direct PostgreSQL (pg_utils)")
    print(f"   Checking every 60 seconds")
    print(f"\n   Press Ctrl+C to stop\n")

    db = get_firestore_client(sa_path)

    # Set up snapshot listener for users with reminders
    # Keep reference to prevent garbage collection
    _users_watcher = setup_reminder_listener(db)

    # Give listener time to populate cache
    time.sleep(3)
    print(f"  [cache] {len(reminder_cache)} users with reminders enabled")

    try:
        while True:
            check_and_notify(db)
            time.sleep(60)
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
    
    args = parser.parse_args()
    run_daemon(args.serviceAccount)

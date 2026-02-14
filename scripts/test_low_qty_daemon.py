#!/usr/bin/env python3
"""Test script for low-qty notification daemon.

Tests each component without sending real notifications.
Only tests route 989262.

Usage:
    cd /Users/kylemacmini/Desktop/routespark/restore-2025-09-24/order_forecast
    source venv/bin/activate
    python scripts/test_low_qty_daemon.py
"""

import json
import hashlib
from datetime import datetime
from pathlib import Path

import pytz

# Setup paths
SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'
TEST_ROUTE = '989262'

print("=" * 60)
print("Low-Qty Notification Daemon Test Suite")
print("=" * 60)
print(f"Route: {TEST_ROUTE}")
print(f"SA Path: {SA_PATH}")
print()

# Test 1: Firebase connection
print("Test 1: Firebase Connection")
print("-" * 40)
try:
    from google.cloud import firestore
    db = firestore.Client.from_service_account_json(SA_PATH)
    print("  ✅ Firebase connected")
except Exception as e:
    print(f"  ❌ Firebase connection failed: {e}")
    exit(1)

# Test 2: Get route owner
print("\nTest 2: Get Route Owner")
print("-" * 40)
try:
    route_doc = db.collection("routes").document(TEST_ROUTE).get()
    if route_doc.exists:
        route_data = route_doc.to_dict()
        # Check both field names (ownerUid is current, userId is legacy)
        owner_id = route_data.get("ownerUid") or route_data.get("userId")
        print(f"  ✅ Route exists")
        print(f"     Owner (ownerUid): {owner_id}")
    else:
        print(f"  ❌ Route {TEST_ROUTE} not found")
        exit(1)
except Exception as e:
    print(f"  ❌ Error: {e}")
    exit(1)

# Test 3: Get user profile and reminder settings
print("\nTest 3: User Profile & Reminder Settings")
print("-" * 40)
try:
    user_doc = db.collection("users").document(owner_id).get()
    if user_doc.exists:
        user_data = user_doc.to_dict()
        profile = user_data.get("profile", {})
        current_route = profile.get("currentRoute") or profile.get("routeNumber")
        timezone = profile.get("timezone", "America/Denver")
        
        user_settings = user_data.get("userSettings", {})
        notifications = user_settings.get("notifications", {})
        order_reminders = notifications.get("orderReminders", {})
        
        enabled = order_reminders.get("enabled", False)
        reminder_time = order_reminders.get("time", {})
        
        print(f"  ✅ User found: {owner_id}")
        print(f"     currentRoute: {current_route}")
        print(f"     timezone: {timezone}")
        print(f"     orderReminders.enabled: {enabled}")
        print(f"     orderReminders.time: {reminder_time}")
        
        if not enabled:
            print("\n  ⚠️  Reminders are DISABLED - daemon would skip this user")
            print("     Enable in app: Settings > Notifications > Order Reminders")
    else:
        print(f"  ❌ User {owner_id} not found")
        exit(1)
except Exception as e:
    print(f"  ❌ Error: {e}")
    exit(1)

# Test 4: Get FCM tokens
print("\nTest 4: FCM Tokens")
print("-" * 40)
try:
    fcm_tokens = user_data.get("fcmTokens", [])
    print(f"  Found {len(fcm_tokens)} token(s)")
    for i, token in enumerate(fcm_tokens):
        if token:
            prefix = token[:30] if len(token) > 30 else token
            valid = token.startswith("ExponentPushToken[") or token.startswith("ExpoPushToken[")
            status = "✅ valid" if valid else "⚠️ invalid format"
            print(f"     [{i}] {prefix}... {status}")
    
    if not fcm_tokens:
        print("  ⚠️  No FCM tokens - notifications can't be sent")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 5: Low-qty items loader
print("\nTest 5: Low-Qty Items")
print("-" * 40)
try:
    from low_quantity_loader import get_items_for_order_date
    
    tz = pytz.timezone(timezone)
    today = datetime.now(tz).strftime("%Y-%m-%d")
    print(f"  Today (user tz): {today}")
    
    items = get_items_for_order_date(db, TEST_ROUTE, today)
    
    if items:
        print(f"  ✅ Found {len(items)} low-qty items for today")
        for i, item in enumerate(items[:5]):  # Show first 5
            print(f"     [{i}] SAP: {item.sap}, expires: {item.expiry_date}")
        if len(items) > 5:
            print(f"     ... and {len(items) - 5} more")
    else:
        print("  ℹ️  No low-qty items for today")
        print("     This is normal if no items are expiring soon")
except Exception as e:
    print(f"  ❌ Error loading low-qty items: {e}")
    import traceback
    traceback.print_exc()

# Test 6: Reminder time check
print("\nTest 6: Reminder Time Check")
print("-" * 40)
try:
    from low_qty_notification_daemon import is_reminder_time_now, reminder_time_to_minutes
    
    if reminder_time:
        target_minutes = reminder_time_to_minutes(reminder_time)
        target_hour = target_minutes // 60
        target_min = target_minutes % 60
        
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)
        current_minutes = now.hour * 60 + now.minute
        
        is_time = is_reminder_time_now(reminder_time, timezone)
        diff = abs(current_minutes - target_minutes)
        
        print(f"  Current time (user tz): {now.strftime('%H:%M')}")
        print(f"  Reminder time: {target_hour:02d}:{target_min:02d}")
        print(f"  Difference: {diff} minutes")
        print(f"  Is reminder time now (±2 min): {'✅ YES' if is_time else '❌ NO'}")
    else:
        print("  ⚠️  No reminder time configured")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 7: PostgreSQL connection
print("\nTest 7: PostgreSQL")
print("-" * 40)
try:
    from pg_utils import fetch_one
    result = fetch_one("SELECT 1 as test")
    if result:
        print("  ✅ PostgreSQL connected")
    else:
        print("  ⚠️  PostgreSQL query returned no rows")
except Exception as e:
    print(f"  ⚠️  PostgreSQL not available: {e}")
    print("     This is OK for testing - dedup will be skipped")
    fetch_one = None

# Test 8: Dedup check
print("\nTest 8: Dedup Check")
print("-" * 40)
if fetch_one and items:
    try:
        saps = sorted([item.sap for item in items])
        saps_hash = hashlib.md5(json.dumps(saps).encode()).hexdigest()

        from pg_utils import fetch_all
        rows = fetch_all("""
            SELECT * FROM low_qty_notifications_sent
            WHERE route_number = %s AND user_id = %s AND order_by_date = %s
            LIMIT 5
        """, [TEST_ROUTE, owner_id, today])

        if rows:
            print(f"  Found {len(rows)} previous notification(s) for today:")
            for row in rows:
                print(f"     sent_at: {row.get('sent_at')}, items: {row.get('items_count')}")
        else:
            print("  ✅ No previous notifications for today")
            print(f"     New notification would use hash: {saps_hash[:16]}...")
    except Exception as e:
        print(f"  ⚠️  Dedup check failed: {e}")
else:
    print("  ⏭️  Skipped (no PostgreSQL or no items)")

# Test 9: Notification payload (dry run)
print("\nTest 9: Notification Payload (DRY RUN)")
print("-" * 40)
if items:
    saps = sorted([item.sap for item in items])
    payload = {
        "to": "<token>",
        "title": "Low Stock Alert",
        "body": f"{len(items)} item{'s' if len(items) != 1 else ''} need to be ordered today",
        "data": {
            "type": "low_quantity",
            "routeNumber": TEST_ROUTE,
            "orderDate": today,
            "saps": saps[:10],  # Truncate for display
        },
        "sound": "default",
        "priority": "high",
    }
    print("  Payload that would be sent:")
    print(f"     title: {payload['title']}")
    print(f"     body: {payload['body']}")
    print(f"     type: {payload['data']['type']}")
    print(f"     saps: {len(saps)} items")
else:
    print("  ⏭️  No items to notify about")

# Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)

checks = [
    ("Firebase connection", True),
    ("Route owner found", owner_id is not None),
    ("User profile loaded", True),
    ("Reminders enabled", enabled),
    ("FCM tokens present", len(fcm_tokens) > 0),
    ("Low-qty items found", items is not None and len(items) > 0 if 'items' in dir() else False),
]

all_pass = True
for name, passed in checks:
    status = "✅" if passed else "❌"
    print(f"  {status} {name}")
    if not passed:
        all_pass = False

print()
if all_pass:
    print("🎉 All checks passed! Daemon should work correctly.")
else:
    print("⚠️  Some checks failed. Review above for details.")

print()
print("To run the actual daemon:")
print(f"  python scripts/low_qty_notification_daemon.py --serviceAccount {SA_PATH}")

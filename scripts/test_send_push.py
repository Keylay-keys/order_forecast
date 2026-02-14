#!/usr/bin/env python3
"""Send a test push notification to route 989262 owner.

Bypasses time check and low-qty check - just tests Expo Push API.

Usage:
    python scripts/test_send_push.py
"""

import requests
from google.cloud import firestore

SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'
TEST_ROUTE = '989262'
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"

print("=" * 50)
print("Test Push Notification")
print("=" * 50)

# Connect to Firebase
db = firestore.Client.from_service_account_json(SA_PATH)

# Get route owner
route_doc = db.collection("routes").document(TEST_ROUTE).get()
owner_id = route_doc.to_dict().get("ownerUid")
print(f"Route owner: {owner_id}")

# Get FCM tokens (just use the last one - most recent device)
user_doc = db.collection("users").document(owner_id).get()
fcm_tokens = user_doc.to_dict().get("fcmTokens", [])
print(f"Found {len(fcm_tokens)} tokens")

# Use only the last token (most recent)
if fcm_tokens:
    token = fcm_tokens[-1]
    print(f"Using token: {token[:40]}...")
else:
    print("No tokens found!")
    exit(1)

# Build test notification
payload = {
    "to": token,
    "title": "🧪 Test: Low Stock Alert",
    "body": "This is a test notification from the daemon",
    "data": {
        "type": "low_quantity",
        "routeNumber": TEST_ROUTE,
        "orderDate": "2026-01-16",
        "saps": ["TEST-SAP-001"],
    },
    "sound": "default",
    "priority": "high",
}

print(f"\nSending notification...")
print(f"  Title: {payload['title']}")
print(f"  Body: {payload['body']}")

# Send
response = requests.post(
    EXPO_PUSH_URL,
    json=[payload],
    headers={"Content-Type": "application/json"},
    timeout=10,
)

result = response.json()
print(f"\nResponse: {response.status_code}")
print(f"Result: {result}")

if "data" in result and result["data"]:
    status = result["data"][0].get("status")
    if status == "ok":
        print("\n✅ Push notification sent successfully!")
        print("   Check your device for the notification.")
    else:
        print(f"\n❌ Push failed: {result['data'][0]}")
else:
    print(f"\n⚠️ Unexpected response: {result}")

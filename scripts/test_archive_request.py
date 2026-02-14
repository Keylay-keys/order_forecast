"""Test script to verify archive listener works end-to-end.

Creates a test request in Firebase, waits for response.

Usage:
    python scripts/test_archive_request.py --serviceAccount /path/to/sa.json
"""

import argparse
import time
import uuid
from google.cloud import firestore


def test_list_dates(db, route_number: str = "989262"):
    """Test listing archived dates."""
    print("\n📋 Test: list_dates")
    print("-" * 40)
    
    request_id = f"test-{uuid.uuid4().hex[:8]}"
    
    # Create request
    doc_ref = db.collection("orderRequests").document(request_id)
    doc_ref.set({
        "requestId": request_id,
        "type": "list_dates",
        "routeNumber": route_number,
        "userId": "test-user",
        "status": "pending",
        "createdAt": firestore.SERVER_TIMESTAMP,
    })
    print(f"   Created request: {request_id}")
    
    # Wait for response
    for i in range(15):  # 15 second timeout
        time.sleep(1)
        doc = doc_ref.get()
        data = doc.to_dict()
        status = data.get("status")
        print(f"   Poll {i+1}: status={status}")
        
        if status == "completed":
            result = data.get("result", {})
            dates = result.get("dates", [])
            print(f"\n   ✅ SUCCESS! Got {len(dates)} dates")
            if dates:
                print(f"   First 3 dates:")
                for d in dates[:3]:
                    print(f"      {d}")
            return True
        elif status == "error":
            print(f"\n   ❌ ERROR: {data.get('error')}")
            return False
    
    print("\n   ⏱️ TIMEOUT - no response after 15 seconds")
    return False


def test_get_order(db, route_number: str = "989262", delivery_date: str = "2025-12-15"):
    """Test getting a specific order."""
    print("\n📦 Test: get_order")
    print("-" * 40)
    
    request_id = f"test-{uuid.uuid4().hex[:8]}"
    
    # Create request
    doc_ref = db.collection("orderRequests").document(request_id)
    doc_ref.set({
        "requestId": request_id,
        "type": "get_order",
        "routeNumber": route_number,
        "deliveryDate": delivery_date,
        "userId": "test-user",
        "status": "pending",
        "createdAt": firestore.SERVER_TIMESTAMP,
    })
    print(f"   Created request: {request_id}")
    print(f"   Requesting order for: {delivery_date}")
    
    # Wait for response
    for i in range(15):
        time.sleep(1)
        doc = doc_ref.get()
        data = doc.to_dict()
        status = data.get("status")
        print(f"   Poll {i+1}: status={status}")
        
        if status == "completed":
            result = data.get("result", {})
            order = result.get("order", {})
            stores = order.get("stores", [])
            print(f"\n   ✅ SUCCESS!")
            print(f"   Order ID: {order.get('id')}")
            print(f"   Schedule: {order.get('scheduleKey')}")
            print(f"   Stores: {len(stores)}")
            if stores:
                print(f"   First store: {stores[0].get('storeName')} ({len(stores[0].get('items', []))} items)")
            return True
        elif status == "error":
            print(f"\n   ❌ ERROR: {data.get('error')}")
            return False
    
    print("\n   ⏱️ TIMEOUT - no response after 15 seconds")
    return False


def cleanup_test_requests(db):
    """Clean up any test requests."""
    print("\n🧹 Cleaning up test requests...")
    docs = db.collection("orderRequests").where("userId", "==", "test-user").stream()
    count = 0
    for doc in docs:
        doc.reference.delete()
        count += 1
    print(f"   Deleted {count} test request(s)")


def main():
    parser = argparse.ArgumentParser(description="Test archive listener")
    parser.add_argument("--serviceAccount", required=True, help="Path to service account JSON")
    parser.add_argument("--route", default="989262", help="Route number to test")
    parser.add_argument("--cleanup", action="store_true", help="Only cleanup test requests")
    args = parser.parse_args()
    
    db = firestore.Client.from_service_account_json(args.serviceAccount)
    
    if args.cleanup:
        cleanup_test_requests(db)
        return
    
    print("=" * 50)
    print("🧪 Archive Listener Test Suite")
    print("=" * 50)
    print(f"Route: {args.route}")
    print("\n⚠️  Make sure the listener is running!")
    
    # Run tests
    results = []
    
    results.append(("list_dates", test_list_dates(db, args.route)))
    results.append(("get_order", test_get_order(db, args.route)))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, r in results if r)
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status}: {name}")
    
    print(f"\n   Total: {passed}/{len(results)} passed")
    
    # Cleanup
    cleanup_test_requests(db)


if __name__ == "__main__":
    main()


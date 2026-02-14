#!/usr/bin/env python3
"""Quick test to generate a forecast and check item 31032 allocation."""

import sys
import warnings
import os
from pathlib import Path

# Suppress ALL warnings before importing anything else
warnings.filterwarnings('ignore')
os.environ['PYTHONWARNINGS'] = 'ignore'

sys.path.insert(0, str(Path(__file__).resolve().parent / 'scripts'))

from forecast_engine import ForecastConfig, generate_forecast
from google.cloud import firestore

SA_PATH = "/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json"

def main():
    print("=" * 60)
    print("CASE ALLOCATION TEST")
    print("=" * 60)
    
    # First check if product catalog has case pack info
    print("\n0. Checking product catalog for 31032...")
    db_check = firestore.Client.from_service_account_json(SA_PATH)
    doc = db_check.collection('masterCatalog').document('989262').collection('products').document('31032').get()
    if doc.exists:
        data = doc.to_dict()
        print(f"   Found in catalog!")
        print(f"   Name: {data.get('name') or data.get('fullName')}")
        print(f"   casePack: {data.get('casePack')}")
        print(f"   tray: {data.get('tray')}")
    else:
        print("   ⚠️  NOT in catalog! Case rounding won't work.")
    
    # Generate forecast
    print("\n1. Generating forecast for Dec 15 (Monday)...")
    cfg = ForecastConfig(
        route_number="989262",
        delivery_date="2025-12-15",
        schedule_key="",  # auto-deduced
        service_account=SA_PATH,
        since_days=365,
        round_cases=True,
        ttl_days=7,
    )
    
    try:
        forecast = generate_forecast(cfg)
        print(f"   ✅ Generated forecast: {forecast.forecast_id}")
        print(f"   Total items: {len(forecast.items)}")
    except Exception as e:
        print(f"   ❌ Error generating forecast: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Check item 31032
    print("\n2. Checking item 31032 allocation...")
    items_31032 = [i for i in forecast.items if i.sap == "31032"]
    
    if not items_31032:
        print("   ⚠️  Item 31032 not in forecast!")
    else:
        print(f"   Found {len(items_31032)} stores with item 31032:")
        total_units = 0
        for item in sorted(items_31032, key=lambda x: x.recommended_units, reverse=True):
            print(f"     {item.store_name}: {item.recommended_units} units ({item.recommended_cases} cases)")
            total_units += item.recommended_units
        print(f"   TOTAL: {total_units} units")
        
        # Check if case rounding worked
        if items_31032[0].recommended_cases:
            implied_pack = items_31032[0].recommended_units / items_31032[0].recommended_cases
            print(f"\n   Implied casePack: {implied_pack:.0f}")
            expected_total = int(implied_pack) * int((total_units + implied_pack - 1) // implied_pack)
            print(f"   Expected total (rounded): {expected_total} units")
            if total_units != expected_total:
                print(f"   ⚠️  Case rounding NOT working! Got {total_units}, expected {expected_total}")
        
        # Compare to historical
        print("\n   Historical shares (from DuckDB):")
        print("     Walmart: 30.3%")
        print("     Kaysville Smith's: 29.3%")
        print("     Farmington Smith's: 21.2%")
        print("     Bowman's: 12.1%")
        print("     Harmon's: 7.1%")
    
    # Now verify it's in Firebase
    print("\n3. Verifying forecast in Firebase...")
    db = firestore.Client.from_service_account_json(SA_PATH)
    doc = db.collection('forecasts').document('989262').collection('cached').document(forecast.forecast_id).get()
    
    if doc.exists:
        data = doc.to_dict()
        print(f"   ✅ Forecast found in Firebase!")
        print(f"   Delivery: {data.get('deliveryDate')}")
        print(f"   Schedule: {data.get('scheduleKey')}")
        print(f"   Items: {len(data.get('items', []))}")
    else:
        print("   ❌ Forecast NOT found in Firebase!")

if __name__ == "__main__":
    main()


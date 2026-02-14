"""Load existing JSON order files into DuckDB.

This script imports the historical order JSON files from parsed_orders_batch_no_catalog/
into the DuckDB analytical database.

Usage:
    python scripts/load_json_orders.py --db data/analytics.duckdb
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import Optional
import argparse

# Import our schema module
from db_schema import get_connection, create_schema, print_schema_summary


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string in various formats."""
    if not date_str:
        return None
    
    # Try common formats
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    print(f"  ⚠️  Could not parse date: {date_str}")
    return None


def get_schedule_key(delivery_date: datetime) -> str:
    """Derive schedule_key from delivery day of week.
    
    Convention:
    - Monday (0) → 'monday'
    - Thursday (3) → 'thursday'
    - Friday (4) → 'friday'
    - Other days → lowercase day name
    """
    day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return day_names[delivery_date.weekday()]


def get_calendar_features(date: datetime) -> dict:
    """Compute calendar features for a given date."""
    # Day of week (0=Mon, 6=Sun)
    dow = date.weekday()
    
    # Week of year
    week = date.isocalendar()[1]
    
    # Month
    month = date.month
    
    # Is first weekend of month?
    # First Saturday is day 1-7, so first full weekend is when day <= 7
    is_first_weekend = (dow >= 5) and (date.day <= 7)
    
    # Is month end? (last 3 days)
    # Simple approximation: day >= 28
    is_month_end = date.day >= 28
    
    return {
        'day_of_week': dow,
        'week_of_year': week,
        'month': month,
        'is_first_weekend_of_month': is_first_weekend,
        'is_month_end': is_month_end,
        'is_holiday_week': False,  # Would need holiday calendar
    }


def load_json_order(conn, json_path: Path) -> dict:
    """Load a single JSON order file into the database.
    
    Returns:
        Dict with stats: {'order_id': str, 'stores': int, 'items': int}
    """
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    order_id = data.get('id', json_path.stem)
    route_number = data.get('routeNumber', '')
    user_id = data.get('userId', '')
    status = data.get('status', 'finalized')
    
    # Parse dates
    delivery_date = parse_date(data.get('expectedDeliveryDate', ''))
    order_date = parse_date(data.get('orderDate', ''))
    
    if not delivery_date:
        print(f"  ⚠️  Skipping {order_id} - no valid delivery date")
        return {'order_id': order_id, 'stores': 0, 'items': 0, 'skipped': True}
    
    # Derive schedule key from delivery day
    schedule_key = get_schedule_key(delivery_date)
    calendar = get_calendar_features(delivery_date)
    
    stores = data.get('stores', [])
    total_units = 0
    total_items = 0
    
    # Insert line items first to calculate totals
    for store in stores:
        store_id = store.get('storeId', '')
        store_name = store.get('storeName', '')
        items = store.get('items', [])
        
        for item in items:
            sap = str(item.get('sap', ''))
            quantity = int(item.get('quantity', 0))
            
            # Skip zero-quantity items (they clutter the data)
            if quantity == 0:
                continue
            
            total_units += quantity
            total_items += 1
            
            line_item_id = f"{order_id}-{store_id}-{sap}"
            
            # Insert line item
            conn.execute("""
                INSERT INTO order_line_items (
                    line_item_id, order_id, route_number, schedule_key,
                    delivery_date, store_id, store_name, sap, quantity, cases,
                    promo_active, promo_id,
                    is_first_weekend_of_month, is_holiday_week, is_month_end,
                    day_of_week, week_of_year, month
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (line_item_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity
            """, [
                line_item_id,
                order_id,
                route_number,
                schedule_key,
                delivery_date.strftime('%Y-%m-%d'),
                store_id,
                store_name,
                sap,
                quantity,
                0,  # cases - would need catalog to calculate
                item.get('promoActive', False),
                item.get('promoId'),
                calendar['is_first_weekend_of_month'],
                calendar['is_holiday_week'],
                calendar['is_month_end'],
                calendar['day_of_week'],
                calendar['week_of_year'],
                calendar['month']
            ])
    
    # Insert order header
    conn.execute("""
        INSERT INTO orders_historical (
            order_id, route_number, user_id, schedule_key,
            delivery_date, order_date, status,
            total_units, store_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            total_units = EXCLUDED.total_units,
            store_count = EXCLUDED.store_count
    """, [
        order_id,
        route_number,
        user_id,
        schedule_key,
        delivery_date.strftime('%Y-%m-%d'),
        order_date.strftime('%Y-%m-%d') if order_date else None,
        status,
        total_units,
        len(stores)
    ])
    
    # Also extract unique stores and items for reference tables
    for store in stores:
        store_id = store.get('storeId', '')
        store_name = store.get('storeName', '')
        
        # Insert/update store
        conn.execute("""
            INSERT INTO stores (store_id, route_number, store_name, is_active)
            VALUES (?, ?, ?, TRUE)
            ON CONFLICT (store_id) DO UPDATE SET
                store_name = EXCLUDED.store_name
        """, [store_id, route_number, store_name])
        
        # NOTE: Do NOT populate store_items from order data!
        # store_items should only be synced from Firebase store configs via db_sync.py.
        # Populating from orders causes stores to incorrectly appear to stock items
        # they ordered historically but no longer carry.
    
    return {
        'order_id': order_id,
        'stores': len(stores),
        'items': total_items,
        'schedule_key': schedule_key,
        'delivery_date': delivery_date.strftime('%Y-%m-%d')
    }


def load_all_json_orders(conn, json_dir: Path) -> list:
    """Load all JSON order files from a directory.
    
    Returns:
        List of stats dicts for each order.
    """
    json_files = sorted(json_dir.glob('*.json'))
    
    if not json_files:
        print(f"⚠️  No JSON files found in {json_dir}")
        return []
    
    print(f"\n📂 Found {len(json_files)} JSON files in {json_dir}")
    print("=" * 60)
    
    results = []
    for json_file in json_files:
        print(f"\n📄 Loading: {json_file.name}")
        try:
            result = load_json_order(conn, json_file)
            results.append(result)
            
            if result.get('skipped'):
                print(f"   ⏭️  Skipped")
            else:
                print(f"   ✅ {result['schedule_key']} delivery on {result['delivery_date']}")
                print(f"      {result['stores']} stores, {result['items']} line items")
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({'order_id': json_file.name, 'error': str(e)})
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Load JSON orders into DuckDB')
    parser.add_argument('--db', default='data/analytics.duckdb', help='Path to database')
    parser.add_argument('--json-dir', default='parsed_orders_batch_no_catalog', 
                        help='Directory containing JSON order files')
    parser.add_argument('--create-schema', action='store_true', 
                        help='Create schema if not exists')
    args = parser.parse_args()
    
    # Resolve paths relative to order_forecast directory
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / args.db
    json_dir = base_dir / args.json_dir
    
    print(f"📁 Database: {db_path}")
    print(f"📂 JSON Dir: {json_dir}")
    
    # Connect and optionally create schema
    conn = get_connection(db_path)
    
    if args.create_schema:
        create_schema(conn)
    
    # Load all orders
    results = load_all_json_orders(conn, json_dir)
    
    # Summary
    successful = [r for r in results if not r.get('skipped') and not r.get('error')]
    skipped = [r for r in results if r.get('skipped')]
    errors = [r for r in results if r.get('error')]
    
    print("\n" + "=" * 60)
    print("📊 IMPORT SUMMARY")
    print("=" * 60)
    print(f"   Total files:    {len(results)}")
    print(f"   ✅ Successful:   {len(successful)}")
    print(f"   ⏭️  Skipped:      {len(skipped)}")
    print(f"   ❌ Errors:       {len(errors)}")
    
    if successful:
        total_items = sum(r.get('items', 0) for r in successful)
        print(f"\n   Total line items imported: {total_items:,}")
        
        # Schedule key breakdown
        schedules = {}
        for r in successful:
            key = r.get('schedule_key', 'unknown')
            schedules[key] = schedules.get(key, 0) + 1
        
        print("\n   Orders by schedule:")
        for key, count in sorted(schedules.items()):
            print(f"      {key}: {count}")
    
    # Print database summary
    print_schema_summary(conn)
    
    conn.close()


if __name__ == '__main__':
    main()


#!/usr/bin/env python3
"""Backfill delivery_allocations from existing order_line_items.

One-time script to populate allocations for historical orders.

Usage:
    python scripts/backfill_allocations.py
"""

try:
    from .pg_utils import fetch_all, fetch_one, execute
except ImportError:
    from pg_utils import fetch_all, fetch_one, execute


def backfill():
    print("Backfilling delivery_allocations from order_line_items...")
    
    # Get all line items that don't have allocations yet
    rows = fetch_all("""
        SELECT 
            li.order_id,
            li.route_number,
            li.delivery_date,
            oh.order_date as source_order_date,
            li.store_id,
            li.store_name,
            li.sap,
            li.quantity
        FROM order_line_items li
        JOIN orders_historical oh ON li.order_id = oh.order_id
        WHERE NOT EXISTS (
            SELECT 1 FROM delivery_allocations da 
            WHERE da.source_order_id = li.order_id 
              AND da.store_id = li.store_id 
              AND da.sap = li.sap
        )
        ORDER BY li.delivery_date, li.store_id, li.sap
    """)

    print(f"Found {len(rows)} line items to backfill")
    
    if not rows:
        print("Nothing to backfill!")
        return
    
    # Insert allocations
    inserted = 0
    for row in rows:
        allocation_id = f"{row['order_id']}-{row['store_id']}-{row['sap']}"
        
        try:
            execute("""
                INSERT INTO delivery_allocations (
                    allocation_id, route_number, source_order_id, source_order_date,
                    sap, store_id, store_name, quantity, delivery_date, is_case_split
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (allocation_id) DO NOTHING
            """, [
                allocation_id,
                row['route_number'],
                row['order_id'],
                row['source_order_date'],
                row['sap'],
                row['store_id'],
                row['store_name'],
                row['quantity'],
                row['delivery_date'],
                False,  # is_case_split - TODO: detect from store delivery days
            ])
            inserted += 1
        except Exception as e:
            print(f"  Error inserting {allocation_id}: {e}")
    
    print(f"✅ Inserted {inserted} allocations")
    
    # Verify
    count_row = fetch_one("SELECT COUNT(*) as cnt FROM delivery_allocations")
    print(f"Total allocations now: {count_row.get('cnt', 0) if count_row else 0}")


if __name__ == "__main__":
    backfill()

#!/usr/bin/env python3
"""Fix inconsistent store IDs in DuckDB to match Firebase."""

import duckdb

# Mapping of wrong store_id -> correct Firebase store_id
# Get these from Firebase: /routes/989262/stores/
STORE_ID_FIXES = {
    "kaysville": "store_1763150611812",      # Kaysville Smith's
    "farmington": "store_1763150601142",     # Farmington Smith's
}

conn = duckdb.connect('data/analytics.duckdb')

print("Fixing store IDs to match Firebase...")

for old_id, new_id in STORE_ID_FIXES.items():
    print(f"\n  {old_id} -> {new_id}")
    
    # 1. Update order_line_items
    result = conn.execute("""
        UPDATE order_line_items 
        SET store_id = ?
        WHERE store_id = ?
    """, [new_id, old_id])
    print(f"    order_line_items: {result.rowcount} rows updated")
    
    # 2. Update stores table
    result = conn.execute("""
        UPDATE stores 
        SET store_id = ?
        WHERE store_id = ?
    """, [new_id, old_id])
    print(f"    stores: {result.rowcount} rows updated")
    
    # 3. Update store_items
    result = conn.execute("""
        UPDATE store_items 
        SET store_id = ?, id = REPLACE(id, ?, ?)
        WHERE store_id = ?
    """, [new_id, old_id, new_id, old_id])
    print(f"    store_items: {result.rowcount} rows updated")
    
    # 4. Update store_item_shares
    result = conn.execute("""
        UPDATE store_item_shares 
        SET store_id = ?
        WHERE store_id = ?
    """, [new_id, old_id])
    print(f"    store_item_shares: {result.rowcount} rows updated")

conn.commit()
conn.close()

print("\n✅ Done! Store IDs fixed.")


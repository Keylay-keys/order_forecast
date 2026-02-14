#!/usr/bin/env python3
"""Check DuckDB data for store ID inconsistencies."""

import duckdb

conn = duckdb.connect('data/analytics.duckdb', read_only=True)

print("=== store_item_shares for 31032 (monday) ===")
rows = conn.execute("""
    SELECT store_id, store_name, share, total_quantity
    FROM store_item_shares
    WHERE sap = '31032' AND schedule_key = 'monday'
    ORDER BY share DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} ({r[2]*100:.1f}%, qty={r[3]})")

print("\n=== order_line_items store_id values for 31032 ===")
rows = conn.execute("""
    SELECT DISTINCT store_id, store_name
    FROM order_line_items
    WHERE sap = '31032'
    ORDER BY store_id
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print("\n=== stores table ===")
rows = conn.execute("SELECT store_id, store_name FROM stores ORDER BY store_name").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

conn.close()



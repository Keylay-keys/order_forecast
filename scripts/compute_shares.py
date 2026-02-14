"""Compute historical shares for case allocation.

This script analyzes historical order data and computes:
1. Each store's share of each item's total demand
2. Splitting patterns (single_store, even_split, skewed, varies)
3. Caches results for fast forecast generation

Usage:
    python scripts/compute_shares.py --route 989262
    python scripts/compute_shares.py --route 989262 --schedule monday
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import duckdb

# Import from same package
try:
    from .db_schema import get_connection, DEFAULT_DB_PATH
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from scripts.db_schema import get_connection, DEFAULT_DB_PATH


def compute_store_item_shares(
    conn: duckdb.DuckDBPyConnection,
    route_number: str,
    schedule_key: str | None = None,
) -> int:
    """Compute and cache each store's share of each item.
    
    Args:
        conn: DuckDB connection
        route_number: Route to compute shares for
        schedule_key: Optional filter for specific schedule (monday, thursday)
    
    Returns:
        Number of rows inserted/updated
    """
    schedule_filter = ""
    params = [route_number]
    if schedule_key:
        schedule_filter = "AND li.schedule_key = ?"
        params.append(schedule_key)
    
    # First, compute raw totals per store/item
    query = f"""
    WITH store_item_totals AS (
        SELECT 
            li.route_number,
            li.store_id,
            s.store_name,
            li.sap,
            li.schedule_key,
            SUM(li.quantity) as total_qty,
            COUNT(DISTINCT li.order_id) as order_count,
            AVG(li.quantity) as avg_qty,
            MAX(li.delivery_date) as last_ordered_date
        FROM order_line_items li
        LEFT JOIN stores s ON li.store_id = s.store_id
        WHERE li.route_number = ?
          {schedule_filter}
        GROUP BY li.route_number, li.store_id, s.store_name, li.sap, li.schedule_key
    ),
    item_totals AS (
        SELECT 
            route_number,
            sap,
            schedule_key,
            SUM(total_qty) as grand_total
        FROM store_item_totals
        GROUP BY route_number, sap, schedule_key
    ),
    with_shares AS (
        SELECT 
            sit.*,
            sit.total_qty * 1.0 / NULLIF(it.grand_total, 0) as share
        FROM store_item_totals sit
        JOIN item_totals it 
            ON sit.route_number = it.route_number 
            AND sit.sap = it.sap 
            AND sit.schedule_key = it.schedule_key
    ),
    recent_orders AS (
        -- Get last 4 orders per schedule
        SELECT DISTINCT 
            o.order_id,
            o.schedule_key,
            ROW_NUMBER() OVER (PARTITION BY o.schedule_key ORDER BY o.delivery_date DESC) as rn
        FROM orders_historical o
        WHERE o.route_number = ?
    ),
    recent_totals AS (
        SELECT 
            li.store_id,
            li.sap,
            li.schedule_key,
            SUM(li.quantity) as recent_qty
        FROM order_line_items li
        JOIN recent_orders ro ON li.order_id = ro.order_id AND li.schedule_key = ro.schedule_key
        WHERE ro.rn <= 4
        GROUP BY li.store_id, li.sap, li.schedule_key
    ),
    recent_item_totals AS (
        SELECT 
            sap,
            schedule_key,
            SUM(recent_qty) as recent_grand_total
        FROM recent_totals
        GROUP BY sap, schedule_key
    )
    SELECT 
        ws.route_number || '-' || ws.store_id || '-' || ws.sap || '-' || ws.schedule_key as id,
        ws.route_number,
        ws.store_id,
        ws.store_name,
        ws.sap,
        ws.schedule_key,
        ws.share,
        ws.total_qty,
        ws.order_count,
        ws.avg_qty,
        COALESCE(rt.recent_qty * 1.0 / NULLIF(rit.recent_grand_total, 0), ws.share) as recent_share,
        ws.last_ordered_date
    FROM with_shares ws
    LEFT JOIN recent_totals rt 
        ON ws.store_id = rt.store_id 
        AND ws.sap = rt.sap 
        AND ws.schedule_key = rt.schedule_key
    LEFT JOIN recent_item_totals rit 
        ON ws.sap = rit.sap 
        AND ws.schedule_key = rit.schedule_key
    """
    
    # Add route_number again for the recent_orders subquery
    params.append(route_number)
    
    rows = conn.execute(query, params).fetchall()
    
    if not rows:
        print(f"  No order data found for route {route_number}")
        return 0
    
    # Insert/update store_item_shares
    inserted = 0
    for row in rows:
        share = row[6] or 0
        recent_share = row[10] or share
        share_trend = recent_share - share if share else 0
        
        now = datetime.now()
        conn.execute("""
            INSERT INTO store_item_shares (
                id, route_number, store_id, store_name, sap, schedule_key,
                share, total_quantity, order_count, avg_quantity,
                recent_share, share_trend, last_ordered_date, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                share = EXCLUDED.share,
                total_quantity = EXCLUDED.total_quantity,
                order_count = EXCLUDED.order_count,
                avg_quantity = EXCLUDED.avg_quantity,
                recent_share = EXCLUDED.recent_share,
                share_trend = EXCLUDED.share_trend,
                last_ordered_date = EXCLUDED.last_ordered_date,
                updated_at = EXCLUDED.updated_at
        """, [
            row[0],   # id
            row[1],   # route_number
            row[2],   # store_id
            row[3],   # store_name
            row[4],   # sap
            row[5],   # schedule_key
            share,
            row[7],   # total_quantity
            row[8],   # order_count
            row[9],   # avg_quantity
            recent_share,
            share_trend,
            row[11],  # last_ordered_date
            now,      # updated_at
        ])
        inserted += 1
    
    return inserted


def compute_item_allocation_cache(
    conn: duckdb.DuckDBPyConnection,
    route_number: str,
    schedule_key: str | None = None,
) -> int:
    """Compute and cache item-level allocation data.
    
    Args:
        conn: DuckDB connection
        route_number: Route to compute for
        schedule_key: Optional filter for specific schedule
    
    Returns:
        Number of rows inserted/updated
    """
    schedule_filter = ""
    params = [route_number]
    if schedule_key:
        schedule_filter = "AND schedule_key = ?"
        params.append(schedule_key)
    
    # Get unique SAP/schedule combinations
    items_query = f"""
    SELECT DISTINCT sap, schedule_key
    FROM store_item_shares
    WHERE route_number = ?
      {schedule_filter}
    """
    items = conn.execute(items_query, params).fetchall()
    
    inserted = 0
    for sap, sched in items:
        # Get all store shares for this item
        shares_query = """
        SELECT store_id, share, total_quantity, order_count, avg_quantity
        FROM store_item_shares
        WHERE route_number = ? AND sap = ? AND schedule_key = ?
        ORDER BY share DESC
        """
        shares = conn.execute(shares_query, [route_number, sap, sched]).fetchall()
        
        if not shares:
            continue
        
        # Compute aggregates
        store_shares = {row[0]: row[1] for row in shares}
        total_qty = sum(row[2] for row in shares)
        total_orders = max(row[3] for row in shares) if shares else 0
        avg_total_qty = total_qty / total_orders if total_orders > 0 else 0
        avg_stores = len([s for s in shares if s[1] > 0])
        
        # Get case pack from catalog (default to 1)
        case_pack_query = """
        SELECT case_pack FROM product_catalog WHERE sap = ? LIMIT 1
        """
        case_pack_result = conn.execute(case_pack_query, [sap]).fetchone()
        case_pack = case_pack_result[0] if case_pack_result else 1
        typical_cases = avg_total_qty / case_pack if case_pack > 0 else avg_total_qty
        
        # Determine split pattern
        top_share = shares[0][1] if shares else 0
        dominant_store = shares[0][0] if shares else None
        
        if len(shares) == 1 or top_share >= 0.95:
            pattern = 'single_store'
        elif top_share >= 0.6:
            pattern = 'skewed'
        elif max(s[1] for s in shares) - min(s[1] for s in shares) < 0.15:
            pattern = 'even_split'
        else:
            pattern = 'varies'
        
        # Build the ID
        item_id = f"{route_number}-{sap}-{sched}"
        
        now = datetime.now()
        conn.execute("""
            INSERT INTO item_allocation_cache (
                id, route_number, sap, schedule_key,
                total_avg_quantity, total_orders_seen, store_shares,
                avg_stores_per_order, typical_case_count,
                split_pattern, dominant_store_id, dominant_store_share,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                total_avg_quantity = EXCLUDED.total_avg_quantity,
                total_orders_seen = EXCLUDED.total_orders_seen,
                store_shares = EXCLUDED.store_shares,
                avg_stores_per_order = EXCLUDED.avg_stores_per_order,
                typical_case_count = EXCLUDED.typical_case_count,
                split_pattern = EXCLUDED.split_pattern,
                dominant_store_id = EXCLUDED.dominant_store_id,
                dominant_store_share = EXCLUDED.dominant_store_share,
                updated_at = EXCLUDED.updated_at
        """, [
            item_id,
            route_number,
            sap,
            sched,
            avg_total_qty,
            total_orders,
            json.dumps(store_shares),
            avg_stores,
            typical_cases,
            pattern,
            dominant_store,
            top_share,
            now,
        ])
        inserted += 1
    
    return inserted


def compute_all_shares(
    conn: duckdb.DuckDBPyConnection,
    route_number: str,
    schedule_key: str | None = None,
) -> dict:
    """Compute all share data for a route.
    
    Args:
        conn: DuckDB connection
        route_number: Route to compute for
        schedule_key: Optional filter for specific schedule
    
    Returns:
        Dict with counts of rows inserted
    """
    print(f"\n📊 Computing historical shares for route {route_number}")
    if schedule_key:
        print(f"   Schedule: {schedule_key}")
    
    # Step 1: Compute store-item shares
    print("\n1️⃣  Computing store-item shares...")
    store_shares = compute_store_item_shares(conn, route_number, schedule_key)
    print(f"   ✓ {store_shares} store-item share records")
    
    # Step 2: Compute item allocation cache
    print("\n2️⃣  Computing item allocation cache...")
    item_alloc = compute_item_allocation_cache(conn, route_number, schedule_key)
    print(f"   ✓ {item_alloc} item allocation records")
    
    return {
        'store_item_shares': store_shares,
        'item_allocation_cache': item_alloc,
    }


def print_share_summary(conn: duckdb.DuckDBPyConnection, route_number: str) -> None:
    """Print a summary of computed shares."""
    print("\n" + "=" * 60)
    print("📈 SHARE SUMMARY")
    print("=" * 60)
    
    # Pattern distribution
    pattern_query = """
    SELECT 
        split_pattern,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
    FROM item_allocation_cache
    WHERE route_number = ?
    GROUP BY split_pattern
    ORDER BY count DESC
    """
    patterns = conn.execute(pattern_query, [route_number]).fetchall()
    
    print("\nSplit Pattern Distribution:")
    for pattern, count, pct in patterns:
        print(f"  {pattern}: {count} items ({pct}%)")
    
    # Top skewed items
    skewed_query = """
    SELECT 
        sap,
        schedule_key,
        dominant_store_id,
        ROUND(dominant_store_share * 100, 1) as pct
    FROM item_allocation_cache
    WHERE route_number = ?
      AND split_pattern = 'skewed'
    ORDER BY dominant_store_share DESC
    LIMIT 10
    """
    skewed = conn.execute(skewed_query, [route_number]).fetchall()
    
    if skewed:
        print("\nTop Skewed Items (one store dominates):")
        for sap, sched, store, pct in skewed:
            print(f"  {sap} ({sched}): {store} gets {pct}%")
    
    # Even split items
    even_query = """
    SELECT 
        sap,
        schedule_key,
        avg_stores_per_order
    FROM item_allocation_cache
    WHERE route_number = ?
      AND split_pattern = 'even_split'
    ORDER BY avg_stores_per_order DESC
    LIMIT 10
    """
    even = conn.execute(even_query, [route_number]).fetchall()
    
    if even:
        print("\nEven Split Items (distributed across stores):")
        for sap, sched, stores in even:
            print(f"  {sap} ({sched}): ~{stores:.1f} stores")


def main():
    parser = argparse.ArgumentParser(description='Compute historical shares for case allocation')
    parser.add_argument('--route', required=True, help='Route number')
    parser.add_argument('--schedule', help='Schedule key (monday, thursday, etc.)')
    parser.add_argument('--db', default=str(DEFAULT_DB_PATH), help='Path to DuckDB')
    parser.add_argument('--summary', action='store_true', help='Print summary after computing')
    args = parser.parse_args()
    
    conn = get_connection(args.db)
    
    try:
        results = compute_all_shares(conn, args.route, args.schedule)
        
        print("\n✅ Share computation complete!")
        print(f"   Store-item shares: {results['store_item_shares']}")
        print(f"   Item allocations:  {results['item_allocation_cache']}")
        
        if args.summary:
            print_share_summary(conn, args.route)
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()


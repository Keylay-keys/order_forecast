"""Compute historical shares for case allocation (PostgreSQL).

This mirrors scripts/compute_shares.py but targets the production PostgreSQL
schema used by the server containers.

Usage:
    python scripts/compute_shares_pg.py --route 989262
    python scripts/compute_shares_pg.py --route 989262 --schedule tuesday
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from .pg_utils import get_pg_connection
except ImportError:
    from pg_utils import get_pg_connection


def _fetch_all(conn: psycopg2.extensions.connection, sql: str, params: Optional[list] = None) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params if params else None)
        return [dict(row) for row in cur.fetchall()]


def _fetch_scalar(conn: psycopg2.extensions.connection, sql: str, params: Optional[list] = None):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params if params else None)
        row = cur.fetchone()
        if not row:
            return None
        return list(row.values())[0]


def get_latest_order_sync(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
):
    sql = "SELECT MAX(synced_at) as last_synced FROM order_line_items WHERE route_number = %s"
    params: list = [route_number]
    if schedule_key:
        sql += " AND schedule_key = %s"
        params.append(schedule_key)
    return _fetch_scalar(conn, sql, params)


def get_latest_share_update(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
):
    sql = "SELECT MAX(updated_at) as last_updated FROM store_item_shares WHERE route_number = %s"
    params: list = [route_number]
    if schedule_key:
        sql += " AND schedule_key = %s"
        params.append(schedule_key)
    return _fetch_scalar(conn, sql, params)


def shares_need_refresh(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
) -> Tuple[bool, str]:
    last_synced = get_latest_order_sync(conn, route_number, schedule_key)
    last_share = get_latest_share_update(conn, route_number, schedule_key)

    if last_share is None:
        return True, "no share cache"
    if last_synced is None:
        return False, "no orders synced"
    if last_synced > last_share:
        return True, "new orders synced"
    return False, "up to date"


def ensure_shares_fresh(
    route_number: str,
    schedule_key: Optional[str] = None,
    window_orders: int = 20,
    recent_count: int = 4,
    recent_weight: float = 0.6,
    force: bool = False,
) -> Tuple[bool, str]:
    conn = get_pg_connection()
    try:
        needs, reason = shares_need_refresh(conn, route_number, schedule_key)
        if force or needs:
            compute_all_shares(
                conn,
                route_number,
                schedule_key,
                window_orders=window_orders,
                recent_count=recent_count,
                recent_weight=recent_weight,
            )
            return True, reason
        return False, reason
    finally:
        conn.close()


def compute_store_item_shares(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
    window_orders: int = 20,
    recent_count: int = 4,
    recent_weight: float = 0.6,
) -> int:
    if window_orders < 1:
        window_orders = 1
    if recent_count < 1:
        recent_count = 1
    if recent_count > window_orders:
        recent_count = window_orders
    if recent_weight < 0:
        recent_weight = 0.0
    if recent_weight > 1:
        recent_weight = 1.0
    base_weight = 1.0 - recent_weight

    schedule_filter = ""
    params: list = [route_number]
    if schedule_key:
        schedule_filter = "AND o.schedule_key = %s"
        params.append(schedule_key)

    query = f"""
    WITH recent_orders AS (
        SELECT
            o.order_id,
            o.schedule_key,
            o.delivery_date,
            ROW_NUMBER() OVER (PARTITION BY o.schedule_key ORDER BY o.delivery_date DESC) as rn
        FROM orders_historical o
        WHERE o.route_number = %s
          {schedule_filter}
    ),
    window_orders AS (
        SELECT order_id, schedule_key, delivery_date, rn
        FROM recent_orders
        WHERE rn <= %s
    ),
    store_item_totals AS (
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
        JOIN window_orders wo ON li.order_id = wo.order_id AND li.schedule_key = wo.schedule_key
        LEFT JOIN stores s ON li.store_id = s.store_id
        WHERE li.route_number = %s
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
            sit.total_qty * 1.0 / NULLIF(it.grand_total, 0) as base_share
        FROM store_item_totals sit
        JOIN item_totals it
            ON sit.route_number = it.route_number
            AND sit.sap = it.sap
            AND sit.schedule_key = it.schedule_key
    ),
    recent_totals AS (
        SELECT
            li.store_id,
            li.sap,
            li.schedule_key,
            SUM(li.quantity) as recent_qty
        FROM order_line_items li
        JOIN window_orders wo ON li.order_id = wo.order_id AND li.schedule_key = wo.schedule_key
        WHERE wo.rn <= %s
          AND li.route_number = %s
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
        (%s * COALESCE(rt.recent_qty * 1.0 / NULLIF(rit.recent_grand_total, 0), ws.base_share)
         + %s * ws.base_share) as share,
        ws.total_qty,
        ws.order_count,
        ws.avg_qty,
        COALESCE(rt.recent_qty * 1.0 / NULLIF(rit.recent_grand_total, 0), ws.base_share) as recent_share,
        ws.last_ordered_date,
        ws.base_share
    FROM with_shares ws
    LEFT JOIN recent_totals rt
        ON ws.store_id = rt.store_id
        AND ws.sap = rt.sap
        AND ws.schedule_key = rt.schedule_key
    LEFT JOIN recent_item_totals rit
        ON ws.sap = rit.sap
        AND ws.schedule_key = rit.schedule_key
    """

    rows = _fetch_all(
        conn,
        query,
        params + [
            window_orders,
            route_number,
            recent_count,
            route_number,
            recent_weight,
            base_weight,
        ],
    )
    if not rows:
        print(f"  No order data found for route {route_number}")
        return 0

    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for row in rows:
            share = row.get('share') or 0
            recent_share = row.get('recent_share') or share
            base_share = row.get('base_share') or share
            share_trend = recent_share - base_share if base_share else 0

            cur.execute("""
                INSERT INTO store_item_shares (
                    id, route_number, store_id, store_name, sap, schedule_key,
                    share, total_quantity, order_count, avg_quantity,
                    recent_share, share_trend, last_ordered_date, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                row.get('id'),
                row.get('route_number'),
                row.get('store_id'),
                row.get('store_name'),
                row.get('sap'),
                row.get('schedule_key'),
                share,
                row.get('total_qty') or 0,
                row.get('order_count') or 0,
                row.get('avg_qty'),
                recent_share,
                share_trend,
                row.get('last_ordered_date'),
                now,
            ])

    conn.commit()
    return len(rows)


def compute_item_allocation_cache(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
) -> int:
    schedule_filter = ""
    params: list = [route_number]
    if schedule_key:
        schedule_filter = "AND sis.schedule_key = %s"
        params.append(schedule_key)

    share_rows = _fetch_all(
        conn,
        f"""
    SELECT
        sis.sap,
        sis.schedule_key,
        sis.store_id,
        sis.share,
        sis.total_quantity,
        sis.order_count,
        sis.avg_quantity,
        COALESCE(pc.case_pack, 1) as case_pack
    FROM store_item_shares sis
    LEFT JOIN product_catalog pc
        ON pc.route_number = sis.route_number
        AND pc.sap = sis.sap
    WHERE sis.route_number = %s
      {schedule_filter}
    ORDER BY sis.sap, sis.schedule_key, sis.share DESC
    """,
        params,
    )
    if not share_rows:
        return 0

    grouped_shares: dict[tuple[str, Optional[str]], list[dict]] = defaultdict(list)
    case_pack_by_item: dict[tuple[str, Optional[str]], float] = {}
    for row in share_rows:
        sap = row.get('sap')
        sched = row.get('schedule_key')
        key = (sap, sched)
        grouped_shares[key].append(row)
        if key not in case_pack_by_item:
            case_pack_by_item[key] = row.get('case_pack') or 1

    inserted = 0
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for (sap, sched), shares in grouped_shares.items():
            if not shares:
                continue

            store_shares = {row['store_id']: row['share'] for row in shares}
            total_qty = sum(row.get('total_quantity') or 0 for row in shares)
            total_orders = max((row.get('order_count') or 0) for row in shares) if shares else 0
            avg_total_qty = total_qty / total_orders if total_orders > 0 else 0
            avg_stores = len([s for s in shares if (s.get('share') or 0) > 0])

            case_pack = case_pack_by_item.get((sap, sched), 1)
            typical_cases = avg_total_qty / case_pack if case_pack and case_pack > 0 else avg_total_qty

            top_share = shares[0].get('share') if shares else 0
            dominant_store = shares[0].get('store_id') if shares else None

            if len(shares) == 1 or (top_share or 0) >= 0.95:
                pattern = 'single_store'
            elif (top_share or 0) >= 0.6:
                pattern = 'skewed'
            elif (max(s.get('share') or 0 for s in shares) - min(s.get('share') or 0 for s in shares)) < 0.15:
                pattern = 'even_split'
            else:
                pattern = 'varies'

            item_id = f"{route_number}-{sap}-{sched}"

            cur.execute("""
                INSERT INTO item_allocation_cache (
                    id, route_number, sap, schedule_key,
                    total_avg_quantity, total_orders_seen, store_shares,
                    avg_stores_per_order, typical_case_count,
                    split_pattern, dominant_store_id, dominant_store_share,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                top_share or 0,
                now,
            ])
            inserted += 1

    conn.commit()
    return inserted


def compute_all_shares(
    conn: psycopg2.extensions.connection,
    route_number: str,
    schedule_key: Optional[str] = None,
    window_orders: int = 20,
    recent_count: int = 4,
    recent_weight: float = 0.6,
) -> dict:
    print(f"\nComputing historical shares for route {route_number}")
    if schedule_key:
        print(f"   Schedule: {schedule_key}")

    with conn.cursor() as cur:
        if schedule_key:
            cur.execute(
                "DELETE FROM store_item_shares WHERE route_number = %s AND schedule_key = %s",
                [route_number, schedule_key],
            )
            cur.execute(
                "DELETE FROM item_allocation_cache WHERE route_number = %s AND schedule_key = %s",
                [route_number, schedule_key],
            )
        else:
            cur.execute("DELETE FROM store_item_shares WHERE route_number = %s", [route_number])
            cur.execute("DELETE FROM item_allocation_cache WHERE route_number = %s", [route_number])
    conn.commit()

    print("\n1) Computing store-item shares...")
    store_shares = compute_store_item_shares(
        conn,
        route_number,
        schedule_key,
        window_orders=window_orders,
        recent_count=recent_count,
        recent_weight=recent_weight,
    )
    print(f"   OK: {store_shares} store-item share records")

    print("\n2) Computing item allocation cache...")
    item_alloc = compute_item_allocation_cache(conn, route_number, schedule_key)
    print(f"   OK: {item_alloc} item allocation records")

    return {
        'store_item_shares': store_shares,
        'item_allocation_cache': item_alloc,
    }


def print_share_summary(conn: psycopg2.extensions.connection, route_number: str) -> None:
    print("\n" + "=" * 60)
    print("SHARE SUMMARY")
    print("=" * 60)

    patterns = _fetch_all(
        conn,
        """
        SELECT
            split_pattern,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
        FROM item_allocation_cache
        WHERE route_number = %s
        GROUP BY split_pattern
        ORDER BY count DESC
        """,
        [route_number],
    )

    print("\nSplit Pattern Distribution:")
    for row in patterns:
        print(f"  {row.get('split_pattern')}: {row.get('count')} items ({row.get('pct')}%)")

    skewed = _fetch_all(
        conn,
        """
        SELECT
            sap,
            schedule_key,
            dominant_store_id,
            ROUND((dominant_store_share * 100)::numeric, 1) as pct
        FROM item_allocation_cache
        WHERE route_number = %s
          AND split_pattern = 'skewed'
        ORDER BY dominant_store_share DESC
        LIMIT 10
        """,
        [route_number],
    )

    if skewed:
        print("\nTop skewed items (one store dominates):")
        for row in skewed:
            print(f"  {row.get('sap')} ({row.get('schedule_key')}): {row.get('dominant_store_id')} gets {row.get('pct')}%") 

    even = _fetch_all(
        conn,
        """
        SELECT
            sap,
            schedule_key,
            avg_stores_per_order
        FROM item_allocation_cache
        WHERE route_number = %s
          AND split_pattern = 'even_split'
        ORDER BY avg_stores_per_order DESC
        LIMIT 10
        """,
        [route_number],
    )

    if even:
        print("\nEven split items (distributed across stores):")
        for row in even:
            print(f"  {row.get('sap')} ({row.get('schedule_key')}): ~{row.get('avg_stores_per_order'):.1f} stores")


def main() -> None:
    parser = argparse.ArgumentParser(description='Compute historical shares for case allocation (PostgreSQL)')
    parser.add_argument('--route', required=True, help='Route number')
    parser.add_argument('--schedule', help='Schedule key (monday, tuesday, etc.)')
    parser.add_argument('--windowOrders', type=int, default=20, help='Number of recent orders per schedule')
    parser.add_argument('--recentCount', type=int, default=4, help='Number of most recent orders to weight')
    parser.add_argument('--recentWeight', type=float, default=0.6, help='Weight (0-1) for recent shares')
    parser.add_argument('--summary', action='store_true', help='Print summary after computing')
    args = parser.parse_args()

    conn = get_pg_connection()
    try:
        results = compute_all_shares(
            conn,
            args.route,
            args.schedule,
            window_orders=args.windowOrders,
            recent_count=args.recentCount,
            recent_weight=args.recentWeight,
        )

        print("\nShare computation complete.")
        print(f"   Store-item shares: {results['store_item_shares']}")
        print(f"   Item allocations:  {results['item_allocation_cache']}")

        if args.summary:
            print_share_summary(conn, args.route)
    finally:
        conn.close()


if __name__ == '__main__':
    main()

"""PostgreSQL helpers for scripts/daemons.

Uses environment variables:
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

import os
from typing import Any, Iterable, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

_pg_conn: Optional[psycopg2.extensions.connection] = None


def get_pg_connection() -> psycopg2.extensions.connection:
    """Get a cached PostgreSQL connection (reconnects if closed)."""
    global _pg_conn
    if _pg_conn is not None and _pg_conn.closed == 0:
        return _pg_conn

    _pg_conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DB", "routespark"),
        user=os.environ.get("POSTGRES_USER", "routespark"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    return _pg_conn


def fetch_all(sql: str, params: Optional[Iterable[Any]] = None) -> list[dict]:
    """Run a SELECT and return rows as list of dicts."""
    conn = get_pg_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, list(params) if params else None)
        return [dict(row) for row in cur.fetchall()]


def fetch_one(sql: str, params: Optional[Iterable[Any]] = None) -> Optional[dict]:
    """Run a SELECT and return a single row as dict."""
    conn = get_pg_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, list(params) if params else None)
        row = cur.fetchone()
        return dict(row) if row else None


def execute(sql: str, params: Optional[Iterable[Any]] = None) -> int:
    """Run a write query and return affected rows."""
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(sql, list(params) if params else None)
        affected = cur.rowcount
    conn.commit()
    return affected


# =============================================================================
# High-level helpers (replacements for DBClient methods)
# =============================================================================

def get_archived_dates(route_number: str) -> list[dict]:
    """Get list of archived order dates for a route.

    Replacement for DBClient.get_archived_dates().
    Returns: [{date, scheduleKey, itemCount}]
    """
    rows = fetch_all("""
        SELECT 
            o.delivery_date,
            o.schedule_key,
            COUNT(DISTINCT li.line_item_id) as item_count
        FROM orders_historical o
        LEFT JOIN order_line_items li ON o.order_id = li.order_id
        WHERE o.route_number = %s
        GROUP BY o.order_id, o.delivery_date, o.schedule_key
        ORDER BY o.delivery_date DESC
        LIMIT 200
    """, [route_number])
    dates = []
    for row in rows:
        delivery_date = row.get('delivery_date')
        date_str = delivery_date.strftime('%Y-%m-%d') if hasattr(delivery_date, 'strftime') else str(delivery_date)
        dates.append({
            'date': date_str,
            'scheduleKey': row.get('schedule_key') or 'unknown',
            'itemCount': row.get('item_count') or 0,
        })
    return dates


def get_order_by_date(route_number: str, delivery_date: str) -> Optional[dict]:
    """Get order data for a specific delivery date.

    Replacement for DBClient.get_order().
    Returns order header with nested stores/items.
    """
    order_row = fetch_one("""
        SELECT order_id, schedule_key, order_date, total_units, store_count
        FROM orders_historical
        WHERE route_number = %s AND delivery_date = %s
        LIMIT 1
    """, [route_number, delivery_date])

    if not order_row:
        return None

    order_id = order_row.get('order_id')
    items = fetch_all("""
        SELECT store_id, store_name, sap, quantity
        FROM order_line_items
        WHERE order_id = %s
        ORDER BY store_id, sap
    """, [order_id])

    stores: dict[str, dict] = {}
    for row in items:
        store_id = row.get('store_id')
        if store_id not in stores:
            stores[store_id] = {
                'storeId': store_id,
                'storeName': row.get('store_name'),
                'items': [],
            }
        stores[store_id]['items'].append({
            'sap': row.get('sap'),
            'quantity': row.get('quantity'),
        })

    order_date = order_row.get('order_date')
    if hasattr(order_date, 'isoformat'):
        order_date = order_date.isoformat()

    return {
        'id': order_id,
        'routeNumber': route_number,
        'scheduleKey': order_row.get('schedule_key'),
        'deliveryDate': delivery_date,
        'orderDate': order_date,
        'totalUnits': order_row.get('total_units'),
        'storeCount': order_row.get('store_count'),
        'stores': list(stores.values()),
    }


def check_route_synced(route_number: str) -> dict:
    """Check if a route is synced in PostgreSQL.

    Replacement for DBClient.check_route_synced().
    """
    row = fetch_one("""
        SELECT route_number, sync_status, last_synced_at, stores_count, products_count
        FROM routes_synced
        WHERE route_number = %s
    """, [route_number])

    if row and row.get('sync_status') == 'ready':
        return {'synced': True, 'status': 'ready', **row}
    return {'synced': False, 'status': row.get('sync_status') if row else 'not_found'}


def get_delivery_manifest(
    route_number: str,
    delivery_date: str,
    store_id: Optional[str] = None
) -> dict:
    """Get delivery manifest for a date.

    Replacement for DBClient.get_delivery_manifest().
    """
    params: list[Any] = [route_number, delivery_date]
    store_filter = ""
    if store_id:
        store_filter = "AND da.store_id = %s"
        params.append(store_id)

    rows = fetch_all(f"""
        SELECT 
            da.store_id,
            da.store_name,
            da.sap,
            da.quantity,
            da.source_order_id,
            da.source_order_date,
            da.is_case_split,
            pc.full_name as product_name,
            pc.case_pack
        FROM delivery_allocations da
        LEFT JOIN product_catalog pc 
            ON da.sap = pc.sap AND da.route_number = pc.route_number
        WHERE da.route_number = %s
          AND da.delivery_date = %s
          {store_filter}
        ORDER BY da.store_name, pc.full_name, da.sap
    """, params)

    stores: dict[str, dict] = {}
    total_units = 0
    for row in rows:
        sid = row.get('store_id')
        if sid not in stores:
            stores[sid] = {
                'storeId': sid,
                'storeName': row.get('store_name'),
                'items': [],
                'totalUnits': 0,
                'caseSplitCount': 0,
            }

        source_order_date = row.get('source_order_date')
        if hasattr(source_order_date, 'isoformat'):
            source_order_date = source_order_date.isoformat()

        item = {
            'sap': row.get('sap'),
            'productName': row.get('product_name') or row.get('sap'),
            'quantity': row.get('quantity'),
            'casePack': row.get('case_pack') or 1,
            'sourceOrderId': row.get('source_order_id'),
            'sourceOrderDate': source_order_date,
            'isCaseSplit': row.get('is_case_split') or False,
        }
        stores[sid]['items'].append(item)
        stores[sid]['totalUnits'] += row.get('quantity') or 0
        if row.get('is_case_split'):
            stores[sid]['caseSplitCount'] += 1
        total_units += row.get('quantity') or 0

    for store in stores.values():
        store['totalItems'] = len(store['items'])
        store['hasCaseSplits'] = store['caseSplitCount'] > 0

    return {
        'manifest': {
            'routeNumber': route_number,
            'deliveryDate': delivery_date,
            'stores': list(stores.values()),
            'totalStores': len(stores),
            'totalUnits': total_units,
            'totalItems': sum(s['totalItems'] for s in stores.values()),
        }
    }


def get_store_delivery(
    route_number: str,
    store_id: str,
    delivery_date: str
) -> dict:
    """Get delivery items for a specific store on a date.

    Replacement for DBClient.get_store_delivery().
    """
    result = get_delivery_manifest(route_number, delivery_date, store_id=store_id)
    stores = result.get('manifest', {}).get('stores', [])
    if stores:
        return {'storeDelivery': stores[0]}
    return {'storeDelivery': None, 'message': f'No delivery found for store {store_id} on {delivery_date}'}


def get_historical_shares(
    route_number: str,
    sap: str,
    schedule_key: str
) -> dict:
    """Get case allocation shares for a SAP.

    Replacement for DBClient.get_historical_shares().
    """
    rows = fetch_all("""
        SELECT store_id, store_name, share, avg_quantity, order_count, last_ordered_date
        FROM store_item_shares
        WHERE route_number = %s AND sap = %s AND schedule_key = %s
        ORDER BY share DESC
    """, [route_number, sap, schedule_key])

    shares = {}
    for row in rows:
        shares[row['store_id']] = {
            'store_name': row['store_name'],
            'share': row['share'],
            'avg_quantity': row['avg_quantity'],
            'order_count': row['order_count'],
            'last_ordered_date': str(row['last_ordered_date']) if row['last_ordered_date'] else None,
        }

    return {'shares': shares}

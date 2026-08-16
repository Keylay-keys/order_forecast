"""PostgreSQL helpers for scripts/daemons.

Uses environment variables:
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

import os
import sys
from typing import Any, Iterable, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

_pg_conn: Optional[psycopg2.extensions.connection] = None


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _postgres_application_name() -> str:
    configured = os.environ.get("POSTGRES_APPLICATION_NAME", "").strip()
    if configured:
        return configured
    script_name = os.path.basename(sys.argv[0] or "runtime")
    return f"routespark-{script_name[:48]}"


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
        connect_timeout=_positive_int_env("POSTGRES_CONNECT_TIMEOUT_SECONDS", 10),
        application_name=_postgres_application_name(),
        options=(
            f"-c idle_in_transaction_session_timeout="
            f"{_positive_int_env('POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS', 60000)} "
            f"-c lock_timeout={_positive_int_env('POSTGRES_LOCK_TIMEOUT_MS', 15000)}"
        ),
    )
    # This module keeps a cached connection in long-running daemons. Without
    # autocommit, even plain SELECT helpers leave the session "idle in
    # transaction" and can hold AccessShareLock until the process exits.
    _pg_conn.autocommit = True
    return _pg_conn


def _discard_pg_connection(connection: psycopg2.extensions.connection) -> None:
    """Close a failed cached connection so the next read creates a fresh one."""
    global _pg_conn
    try:
        connection.close()
    except Exception:
        pass
    if _pg_conn is connection:
        _pg_conn = None


def _run_read(operation):
    """Run an idempotent read, reconnecting exactly once on connection loss."""
    for attempt in range(2):
        connection = get_pg_connection()
        try:
            return operation(connection)
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            _discard_pg_connection(connection)
            if attempt == 1:
                raise
    raise AssertionError("unreachable")


def fetch_all(sql: str, params: Optional[Iterable[Any]] = None) -> list[dict]:
    """Run a SELECT and return rows as list of dicts."""
    bound_params = list(params) if params else None

    def read(connection):
        with connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, bound_params)
            return [dict(row) for row in cur.fetchall()]

    return _run_read(read)


def fetch_one(sql: str, params: Optional[Iterable[Any]] = None) -> Optional[dict]:
    """Run a SELECT and return a single row as dict."""
    bound_params = list(params) if params else None

    def read(connection):
        with connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, bound_params)
            row = cur.fetchone()
            return dict(row) if row else None

    return _run_read(read)


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
            o.order_id,
            o.delivery_date,
            o.schedule_key,
            o.order_revision,
            o.last_mutation_kind,
            o.last_mutation_id,
            o.last_mutation_at,
            o.reallocation_count,
            o.last_reallocated_at,
            o.last_reallocation_id,
            COUNT(DISTINCT li.line_item_id) as item_count
        FROM orders_historical o
        LEFT JOIN order_line_items li ON o.order_id = li.order_id
        WHERE o.route_number = %s
        GROUP BY o.order_id, o.delivery_date, o.schedule_key, o.order_revision,
                 o.last_mutation_kind, o.last_mutation_id, o.last_mutation_at,
                 o.reallocation_count, o.last_reallocated_at, o.last_reallocation_id
        ORDER BY o.delivery_date DESC
        LIMIT 200
    """, [route_number])
    dates = []
    for row in rows:
        delivery_date = row.get('delivery_date')
        date_str = delivery_date.strftime('%Y-%m-%d') if hasattr(delivery_date, 'strftime') else str(delivery_date)
        dates.append({
            'orderId': row.get('order_id'),
            'date': date_str,
            'scheduleKey': row.get('schedule_key') or 'unknown',
            'itemCount': row.get('item_count') or 0,
            'orderRevision': row.get('order_revision') or 0,
            'lastMutation': {
                'kind': row.get('last_mutation_kind'),
                'mutationId': row.get('last_mutation_id'),
                'atMs': int(row['last_mutation_at'].timestamp() * 1000),
            } if row.get('last_mutation_kind') and row.get('last_mutation_at') else None,
            'storeReallocationSummary': {
                'count': row.get('reallocation_count') or 0,
                'lastAppliedAtMs': int(row['last_reallocated_at'].timestamp() * 1000),
                'lastAdjustmentId': row.get('last_reallocation_id') or '',
            } if (row.get('reallocation_count') or 0) > 0 else None,
        })
    return dates


def _build_archived_order(route_number: str, order_row: dict) -> dict:
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
    delivery_date = order_row.get('delivery_date')
    if hasattr(delivery_date, 'isoformat'):
        delivery_date = delivery_date.isoformat()
    finalized_at = order_row.get('finalized_at')
    if hasattr(finalized_at, 'isoformat'):
        finalized_at = finalized_at.isoformat()
    synced_at = order_row.get('synced_at')
    if hasattr(synced_at, 'isoformat'):
        synced_at = synced_at.isoformat()

    return {
        'id': order_id,
        'routeNumber': route_number,
        'userId': order_row.get('user_id') or '',
        'scheduleKey': order_row.get('schedule_key'),
        'deliveryDate': delivery_date,
        'expectedDeliveryDate': delivery_date,
        'orderDate': order_date,
        'status': 'finalized',
        'createdAt': finalized_at or synced_at,
        'updatedAt': synced_at or finalized_at,
        'totalUnits': order_row.get('total_units'),
        'storeCount': order_row.get('store_count'),
        'orderRevision': order_row.get('order_revision') or 0,
        'lastMutation': {
            'kind': order_row.get('last_mutation_kind'),
            'mutationId': order_row.get('last_mutation_id'),
            'atMs': int(order_row['last_mutation_at'].timestamp() * 1000),
        } if order_row.get('last_mutation_kind') and order_row.get('last_mutation_at') else None,
        'storeReallocationSummary': {
            'count': order_row.get('reallocation_count') or 0,
            'lastAppliedAtMs': int(order_row['last_reallocated_at'].timestamp() * 1000),
            'lastAdjustmentId': order_row.get('last_reallocation_id') or '',
        } if (order_row.get('reallocation_count') or 0) > 0 else None,
        'stores': list(stores.values()),
    }


def get_order_by_id(route_number: str, order_id: str) -> Optional[dict]:
    """Get an archived order by its canonical ID, scoped to its route."""
    order_row = fetch_one("""
        SELECT order_id, user_id, schedule_key, order_date, delivery_date,
               finalized_at, synced_at, total_units, store_count,
               order_revision, last_mutation_kind, last_mutation_id,
               last_mutation_at, reallocation_count, last_reallocated_at,
               last_reallocation_id
        FROM orders_historical
        WHERE route_number = %s AND order_id = %s
        LIMIT 1
    """, [route_number, order_id])
    return _build_archived_order(route_number, order_row) if order_row else None


def get_order_by_date(route_number: str, delivery_date: str) -> Optional[dict]:
    """Compatibility lookup for older clients; deterministic across duplicates."""
    order_row = fetch_one("""
        SELECT order_id, user_id, schedule_key, order_date, delivery_date,
               finalized_at, synced_at, total_units, store_count,
               order_revision, last_mutation_kind, last_mutation_id,
               last_mutation_at, reallocation_count, last_reallocated_at,
               last_reallocation_id
        FROM orders_historical
        WHERE route_number = %s AND delivery_date = %s
        ORDER BY order_date DESC, order_id DESC
        LIMIT 1
    """, [route_number, delivery_date])
    return _build_archived_order(route_number, order_row) if order_row else None


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

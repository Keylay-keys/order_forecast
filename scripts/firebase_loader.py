"""Firebase loader for model-friendly structures.

This module pulls data from Firestore and maps into the shared dataclasses in
`models.py`. It is intentionally read-only. Writers live in firebase_writer.py.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional
import json
import sys

from google.cloud import firestore  # type: ignore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False

# Support running as a script without package context
try:
    from .models import Product, StoreConfig, OrderItem, StoreOrder, Order, ForecastPayload
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from models import Product, StoreConfig, OrderItem, StoreOrder, Order, ForecastPayload


# ----------------------------
# Firestore client helpers
# ----------------------------

def get_firestore_client(service_account_path: Optional[str] = None) -> firestore.Client:
    """Return a Firestore client using an optional service account JSON."""
    if service_account_path:
        return firestore.Client.from_service_account_json(service_account_path)
    return firestore.Client()


def _get_pg_connection():
    """Get PostgreSQL connection using environment variables."""
    if not PG_AVAILABLE:
        raise RuntimeError("psycopg2 not available; cannot use PostgreSQL loaders.")
    import os
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        database=os.environ.get('POSTGRES_DB', 'routespark'),
        user=os.environ.get('POSTGRES_USER', 'routespark'),
        password=os.environ.get('POSTGRES_PASSWORD', ''),
    )


# ----------------------------
# Loaders
# ----------------------------

def load_master_catalog(
    db: firestore.Client, route_number: str
) -> List[Product]:
    """Load product catalog for a route."""
    col = db.collection("masterCatalog").document(route_number).collection("products")
    docs = col.stream()
    products: List[Product] = []
    for doc in docs:
        data = doc.to_dict() or {}
        # If catalog items have an explicit inactive flag, respect it.
        # Missing flag defaults to active for backward compatibility.
        if data.get("isActive") is False or data.get("is_active") is False or data.get("active") is False:
            continue
        products.append(
            Product(
                sap=str(data.get("sap") or doc.id),
                name=data.get("fullName") or data.get("name") or data.get("product") or "",
                brand=data.get("brand"),
                category=data.get("category"),
                case_pack=data.get("casePack") or data.get("tray"),
                tray=data.get("tray"),
                additional={k: v for k, v in data.items() if k not in {"sap", "name", "product", "brand", "category", "casePack", "tray"}},
            )
        )
    return products


def load_store_configs(
    db: firestore.Client, route_number: str
) -> List[StoreConfig]:
    """Load store configs (delivery days + active items) for a route."""
    col = db.collection("routes").document(route_number).collection("stores")
    docs = col.stream()
    stores: List[StoreConfig] = []
    for doc in docs:
        data = doc.to_dict() or {}
        delivery_days = data.get("deliveryDays") or data.get("delivery_days") or []
        active_items = data.get("items") or data.get("activeItems") or []
        active_saps: List[str] = []
        for item in active_items:
            if isinstance(item, dict):
                if item.get("isActive") is False or item.get("is_active") is False or item.get("active") is False:
                    continue
                sap_val = item.get("sap") or item.get("SAP") or item.get("id")
                if sap_val is None:
                    continue
                active_saps.append(str(sap_val))
            else:
                active_saps.append(str(item))
        stores.append(
            StoreConfig(
                store_id=doc.id,
                store_name=data.get("name") or doc.id,
                delivery_days=[d.lower() for d in delivery_days],
                active_saps=active_saps,
                additional={k: v for k, v in data.items() if k not in {"name", "deliveryDays", "delivery_days", "items", "activeItems"}},
            )
        )
    return stores


def _parse_timestamp(ts) -> Optional[datetime]:
    if ts is None:
        return None
    if hasattr(ts, "to_datetime"):
        return ts.to_datetime()
    if hasattr(ts, "to_pydatetime"):
        return ts.to_pydatetime()
    if isinstance(ts, datetime):
        return ts
    return None


def _decode_order(doc) -> Order:
    data = doc.to_dict() or {}
    stores: List[StoreOrder] = []
    for s in data.get("stores", []):
        items: List[OrderItem] = []
        for it in s.get("items", []):
            # Extract forecast feedback fields for ML correction learning
            # Canonical field: forecastRecommendedUnits (legacy: forecastSuggestedUnits)
            forecasted_qty = (
                it.get("forecastRecommendedUnits")
                or it.get("forecastSuggestedUnits")
                or it.get("forecastedQuantity")
            )
            if forecasted_qty is not None:
                forecasted_qty = float(forecasted_qty)
            forecasted_cases = (
                it.get("forecastRecommendedCases")
                or it.get("forecastSuggestedCases")
                or it.get("forecastedCases")
            )
            if forecasted_cases is not None:
                forecasted_cases = float(forecasted_cases)
            
            items.append(
                OrderItem(
                  sap=str(it.get("sap")),
                  quantity=float(it.get("quantity", 0) or 0),
                  cases=it.get("cases"),
                  promo_active=bool(it.get("promoActive", False)),
                  promo_id=it.get("promoId"),
                  user_adjusted=bool(it.get("userAdjusted", False)),
                  forecasted_quantity=forecasted_qty,
                  forecasted_cases=forecasted_cases,
                  meta={k: v for k, v in it.items() if k not in {
                      "sap", "quantity", "cases", "promoActive", "promoId",
                      "userAdjusted", "forecastedQuantity", "forecastedCases",
                      "forecastRecommendedUnits", "forecastRecommendedCases",
                      "forecastSuggestedUnits", "forecastSuggestedCases"
                  }},
                )
            )
        stores.append(
            StoreOrder(
                store_id=s.get("storeId") or s.get("id") or "",
                store_name=s.get("storeName") or "",
                items=items,
                entered_at=_parse_timestamp(s.get("enteredAt")),
            )
        )
    return Order(
        id=data.get("id") or doc.id,
        route_number=str(data.get("routeNumber") or data.get("route_number") or ""),
        schedule_key=str(data.get("scheduleKey") or data.get("schedule_key") or "").lower(),
        expected_delivery_date=data.get("expectedDeliveryDate") or data.get("deliveryDate") or "",
        order_date=data.get("orderDate"),
        status=data.get("status", "finalized"),
        stores=stores,
        user_id=data.get("userId"),
        created_at=_parse_timestamp(data.get("createdAt")),
        updated_at=_parse_timestamp(data.get("updatedAt")),
        meta={k: v for k, v in data.items() if k not in {"id","routeNumber","route_number","scheduleKey","schedule_key","expectedDeliveryDate","deliveryDate","orderDate","status","stores","userId","createdAt","updatedAt"}},
    )


def load_orders(
    db: firestore.Client,
    route_number: str,
    since_days: int = 365,
    schedule_keys: Optional[Iterable[str]] = None,
    status: str = "finalized",
    limit: Optional[int] = None,
) -> List[Order]:
    """Load orders for a route with optional time window and schedule filtering."""
    orders_ref = db.collection("routes").document(route_number).collection("orders")
    q = orders_ref.where(filter=FieldFilter("status", "==", status))

    if since_days:
        since = datetime.utcnow() - timedelta(days=since_days)
        q = q.where(filter=FieldFilter("createdAt", ">=", since))

    if limit:
        q = q.limit(limit)

    docs = list(q.stream())
    result: List[Order] = []
    schedule_set = {s.lower() for s in schedule_keys} if schedule_keys else None

    for doc in docs:
        order = _decode_order(doc)
        if schedule_set and order.schedule_key not in schedule_set:
            continue
        result.append(order)

    return result


def load_orders_from_postgres(
    route_number: str,
    since_days: int = 365,
    schedule_keys: Optional[Iterable[str]] = None,
    status: str = "finalized",
    limit: Optional[int] = None,
) -> List[Order]:
    """Load orders for a route from PostgreSQL."""
    conn = _get_pg_connection()
    created_conn = True
    try:
        params: List[object] = [route_number, status]
        sql = """
            SELECT order_id, route_number, schedule_key, delivery_date, order_date, status, user_id
            FROM orders_historical
            WHERE route_number = %s AND status = %s
        """

        if since_days:
            since_date = (datetime.utcnow() - timedelta(days=since_days)).date()
            sql += " AND delivery_date >= %s"
            params.append(since_date)

        if schedule_keys:
            schedule_list = [s.lower() for s in schedule_keys]
            placeholders = ",".join(["%s"] * len(schedule_list))
            sql += f" AND schedule_key IN ({placeholders})"
            params.extend(schedule_list)

        sql += " ORDER BY delivery_date DESC"
        if limit:
            sql += " LIMIT %s"
            params.append(limit)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            order_rows = cur.fetchall()

        if not order_rows:
            return []

        order_ids = [row["order_id"] for row in order_rows]
        placeholders = ",".join(["%s"] * len(order_ids))
        item_sql = f"""
            SELECT order_id, store_id, store_name, sap, quantity, cases, promo_active, promo_id
            FROM order_line_items
            WHERE order_id IN ({placeholders})
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(item_sql, order_ids)
            item_rows = cur.fetchall()

        order_map: dict[str, Order] = {}
        store_maps: dict[str, dict[str, StoreOrder]] = {}

        for row in order_rows:
            delivery_date = row.get("delivery_date")
            order_date = row.get("order_date")
            order_id = row["order_id"]
            order = Order(
                id=order_id,
                route_number=str(row.get("route_number") or route_number),
                schedule_key=str(row.get("schedule_key") or "").lower(),
                expected_delivery_date=delivery_date.strftime("%Y-%m-%d") if delivery_date else "",
                order_date=order_date.strftime("%Y-%m-%d") if order_date else None,
                status=row.get("status") or "finalized",
                stores=[],
                user_id=row.get("user_id"),
                meta={},
            )
            order_map[order_id] = order
            store_maps[order_id] = {}

        for row in item_rows:
            order_id = row["order_id"]
            order = order_map.get(order_id)
            if not order:
                continue
            store_id = str(row.get("store_id") or "")
            store_name = row.get("store_name") or ""
            store_map = store_maps[order_id]
            store = store_map.get(store_id)
            if store is None:
                store = StoreOrder(store_id=store_id, store_name=store_name, items=[])
                store_map[store_id] = store
                order.stores.append(store)

            cases_val = row.get("cases")
            item = OrderItem(
                sap=str(row.get("sap") or ""),
                quantity=float(row.get("quantity") or 0),
                cases=float(cases_val) if cases_val is not None else None,
                promo_active=bool(row.get("promo_active") or False),
                promo_id=row.get("promo_id"),
                meta={},
            )
            store.items.append(item)

        return list(order_map.values())
    finally:
        if created_conn:
            conn.close()


def load_catalog_from_postgres(route_number: str) -> List[Product]:
    """Load product catalog for a route from PostgreSQL."""
    conn = _get_pg_connection()
    created_conn = True
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT sap, full_name, brand, category, case_pack, tray, sub_category, unit_weight, is_active
                FROM product_catalog
                WHERE route_number = %s AND is_active = TRUE
                """,
                [route_number],
            )
            rows = cur.fetchall()

        products: List[Product] = []
        for row in rows:
            additional = {}
            if row.get("sub_category") is not None:
                additional["sub_category"] = row.get("sub_category")
            if row.get("unit_weight") is not None:
                additional["unit_weight"] = row.get("unit_weight")
            products.append(
                Product(
                    sap=str(row.get("sap")),
                    name=row.get("full_name") or "",
                    brand=row.get("brand"),
                    category=row.get("category"),
                    case_pack=row.get("case_pack"),
                    tray=row.get("tray"),
                    additional=additional,
                )
            )
        return products
    finally:
        if created_conn:
            conn.close()


def load_stores_from_postgres(route_number: str) -> List[StoreConfig]:
    """Load store configs (delivery days + active items) for a route from PostgreSQL."""
    conn = _get_pg_connection()
    created_conn = True
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT store_id, store_name, store_number, delivery_days, is_active
                FROM stores
                WHERE route_number = %s AND is_active = TRUE
                """,
                [route_number],
            )
            store_rows = cur.fetchall()

            cur.execute(
                """
                SELECT store_id, sap
                FROM store_items
                WHERE route_number = %s AND is_active = TRUE
                """,
                [route_number],
            )
            item_rows = cur.fetchall()

        items_by_store: dict[str, List[str]] = {}
        for row in item_rows:
            store_id = str(row.get("store_id") or "")
            sap = row.get("sap")
            if store_id not in items_by_store:
                items_by_store[store_id] = []
            if sap is not None:
                items_by_store[store_id].append(str(sap))

        stores: List[StoreConfig] = []
        for row in store_rows:
            delivery_days_raw = row.get("delivery_days") or []
            delivery_days: List[str] = []
            if isinstance(delivery_days_raw, list):
                delivery_days = [str(d).lower() for d in delivery_days_raw]
            elif isinstance(delivery_days_raw, str):
                try:
                    parsed = json.loads(delivery_days_raw)
                    if isinstance(parsed, list):
                        delivery_days = [str(d).lower() for d in parsed]
                    else:
                        delivery_days = [str(delivery_days_raw).lower()]
                except json.JSONDecodeError:
                    delivery_days = [d.strip().lower() for d in delivery_days_raw.split(",") if d.strip()]

            store_id = str(row.get("store_id") or "")
            additional = {}
            if row.get("store_number"):
                additional["store_number"] = row.get("store_number")

            stores.append(
                StoreConfig(
                    store_id=store_id,
                    store_name=row.get("store_name") or store_id,
                    delivery_days=delivery_days,
                    active_saps=items_by_store.get(store_id, []),
                    additional=additional,
                )
            )

        return stores
    finally:
        if created_conn:
            conn.close()


def load_promotions(
    db: firestore.Client, route_number: str
) -> List[dict]:
    """Load promo docs for a route (raw dicts).
    
    Reads from promos/{route}/... subcollections.
    """
    col = db.collection("promos").document(route_number).collections()
    promos: List[dict] = []
    for sub in col:
        for doc in sub.stream():
            data = doc.to_dict() or {}
            data["id"] = doc.id
            promos.append(data)
    return promos

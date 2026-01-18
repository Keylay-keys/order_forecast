"""Firebase loader for model-friendly structures.

This module pulls data from Firestore and maps into the shared dataclasses in
`models.py`. It is intentionally read-only. Writers live in firebase_writer.py.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional
import sys

from google.cloud import firestore  # type: ignore

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
        stores.append(
            StoreConfig(
                store_id=doc.id,
                store_name=data.get("name") or doc.id,
                delivery_days=[d.lower() for d in delivery_days],
                active_saps=[str(item.get("sap") if isinstance(item, dict) else item) for item in active_items],
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
            # App uses "forecastSuggestedUnits" as the field name
            forecasted_qty = it.get("forecastSuggestedUnits") or it.get("forecastedQuantity")
            if forecasted_qty is not None:
                forecasted_qty = float(forecasted_qty)
            forecasted_cases = it.get("forecastSuggestedCases") or it.get("forecastedCases")
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
    orders_ref = db.collection("orders")
    q = orders_ref.where("routeNumber", "==", route_number).where("status", "==", status)

    if since_days:
        since = datetime.utcnow() - timedelta(days=since_days)
        q = q.where("createdAt", ">=", since)

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


def load_forecast_feedback(
    db: firestore.Client,
    route_number: str,
    since_days: Optional[int] = None,
) -> List[dict]:
    """Load forecast feedback/corrections for a route.
    
    Firebase path: forecast_feedback/{route}/entries/{autoId}
    
    Args:
        db: Firestore client.
        route_number: Route to load feedback for.
        since_days: Optional limit to recent feedback.
        
    Returns:
        List of feedback dicts with correction data.
    """
    col = db.collection("forecast_feedback").document(route_number).collection("entries")
    
    q = col.order_by("createdAt", direction=firestore.Query.DESCENDING)
    
    if since_days:
        since = datetime.utcnow() - timedelta(days=since_days)
        q = q.where("createdAt", ">=", since)
    
    feedback: List[dict] = []
    for doc in q.stream():
        data = doc.to_dict() or {}
        data["id"] = doc.id
        feedback.append(data)
    
    return feedback

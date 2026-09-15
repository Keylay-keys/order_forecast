"""Durable Firestore receipts for verified PostgreSQL order projections."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from google.cloud import firestore  # type: ignore
try:
    from .order_archive_schedule import is_send_archive
except ImportError:
    from order_archive_schedule import is_send_archive


ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION = 2
LEGACY_ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION = 1


def evaluate_order_archive_receipt(projection: dict, receipt: dict | None) -> str | None:
    """Return None only when a receipt is eligible under the cleanup gate."""
    if is_send_archive(projection.get('orderId')) or projection.get('schemaVersion') not in (1, 2):
        return 'unsupported_schedule_evidence'
    if not receipt:
        return "receipt_missing"
    if receipt.get("status") != "verified":
        return "receipt_not_verified"
    if receipt.get("schemaVersion") != projection.get("schemaVersion"):
        return "receipt_schema_mismatch"
    if (
        receipt.get("orderId") != projection.get("orderId")
        or receipt.get("routeNumber") != projection.get("routeNumber")
    ):
        return "receipt_identity_mismatch"
    if any((
        receipt.get("scheduleKey") != projection.get("scheduleKey"),
        receipt.get("deliveryDate") != projection.get("deliveryDate"),
        receipt.get("totalUnits") != projection.get("totalUnits"),
        receipt.get("storeCount") != projection.get("storeCount"),
        receipt.get("lineItemCount") != projection.get("lineItemCount"),
        receipt.get("archiveTotalUnits") != projection.get("totalUnits"),
        receipt.get("archiveStoreCount") != projection.get("storeCount"),
        receipt.get("archiveLineItemCount") != projection.get("lineItemCount"),
    )):
        return "receipt_projection_mismatch"
    if receipt.get("sourceFingerprint") != projection.get("sourceFingerprint"):
        return "receipt_source_changed"
    if receipt.get("scheduledOrder") != projection.get("scheduledOrder"):
        return "receipt_source_changed"
    return None


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _integer(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _canonical_value(value: Any) -> Any:
    """Normalize map ordering so Python and TypeScript hash the same stamp."""
    if isinstance(value, dict):
        return {
            key: _canonical_value(value[key])
            for key in sorted(value)
        }
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value


def build_order_archive_projection(
    order_id: str,
    route_number: str,
    order_data: dict,
) -> dict:
    """Build the same immutable source projection used by the cleanup gate."""
    if is_send_archive(_text(order_id)) or is_send_archive(order_data.get('id')):
        raise ValueError('unsupported_schedule_evidence')
    stores = order_data.get("stores") if isinstance(order_data.get("stores"), list) else []
    lines = []
    for raw_store in stores:
        store = raw_store if isinstance(raw_store, dict) else {}
        store_id = _text(store.get("id") if store.get("id") is not None else store.get("storeId"))
        store_name = _text(store.get("name") if store.get("name") is not None else store.get("storeName"))
        items = store.get("items") if isinstance(store.get("items"), list) else []
        for raw_item in items:
            item = raw_item if isinstance(raw_item, dict) else {}
            sap = _text(item.get("sap"))
            quantity = _integer(item.get("quantity"))
            if not sap or quantity == 0:
                continue
            lines.append({
                "storeId": store_id,
                "storeName": store_name,
                "sap": sap,
                "quantity": quantity,
                "cases": _integer(item.get("cases")),
            })
    lines.sort(
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )

    scheduled_order = order_data.get("scheduledOrder")
    if scheduled_order is not None and (not isinstance(scheduled_order, dict) or scheduled_order.get('version') != 2):
        # Never certify a new-format order as an unstamped legacy projection.
        # Full V3 archive/anchor round-trip is a separate activation gate.
        raise ValueError('unsupported_schedule_evidence')
    if not (
        isinstance(scheduled_order, dict)
        and scheduled_order.get("version") == 2
    ):
        scheduled_order = None
    elif scheduled_order is not None:
        scheduled_order = _canonical_value(scheduled_order)
    source = {
        "schemaVersion": (
            ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION
            if scheduled_order is not None
            else LEGACY_ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION
        ),
        "orderId": _text(order_id),
        "routeNumber": _text(
            order_data.get("routeNumber")
            if order_data.get("routeNumber") is not None
            else route_number
        ),
        "scheduleKey": _text(order_data.get("scheduleKey") or "unknown"),
        "deliveryDate": _text(
            order_data.get("expectedDeliveryDate")
            if order_data.get("expectedDeliveryDate") is not None
            else order_data.get("deliveryDate")
        ),
        "status": "finalized",
        "totalUnits": sum(item["quantity"] for item in lines),
        "storeCount": len(stores),
        "lineItemCount": len(lines),
        "lines": lines,
        **({"scheduledOrder": scheduled_order} if scheduled_order is not None else {}),
    }
    canonical = json.dumps(source, ensure_ascii=False, separators=(",", ":"))
    return {
        **source,
        "sourceFingerprint": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def write_verified_order_archive_receipt(
    db: firestore.Client,
    order_id: str,
    route_number: str,
    order_data: dict,
    *,
    archive_total_units: int,
    archive_store_count: int,
    archive_line_item_count: int,
) -> dict:
    """Write a receipt only when the committed archive summary matches its source."""
    projection = build_order_archive_projection(order_id, route_number, order_data)
    expected = (
        projection["totalUnits"],
        projection["storeCount"],
        projection["lineItemCount"],
    )
    archived = (
        int(archive_total_units),
        int(archive_store_count),
        int(archive_line_item_count),
    )
    if archived != expected:
        raise ValueError(
            "ORDER_ARCHIVE_PROJECTION_MISMATCH "
            f"expected={expected} archived={archived}"
        )

    receipt = {
        "schemaVersion": projection["schemaVersion"],
        "status": "verified",
        "orderId": projection["orderId"],
        "routeNumber": projection["routeNumber"],
        "scheduleKey": projection["scheduleKey"],
        "deliveryDate": projection["deliveryDate"],
        "totalUnits": projection["totalUnits"],
        "storeCount": projection["storeCount"],
        "lineItemCount": projection["lineItemCount"],
        "archiveTotalUnits": archived[0],
        "archiveStoreCount": archived[1],
        "archiveLineItemCount": archived[2],
        "sourceFingerprint": projection["sourceFingerprint"],
        **(
            {"scheduledOrder": projection["scheduledOrder"]}
            if "scheduledOrder" in projection
            else {}
        ),
        "source": "postgres_order_sync",
        "verifiedAt": firestore.SERVER_TIMESTAMP,
    }
    db.collection("orderArchiveReceipts").document(projection["orderId"]).set(receipt)
    return receipt

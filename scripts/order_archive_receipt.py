"""Durable Firestore receipts for verified PostgreSQL order projections."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from google.cloud import firestore  # type: ignore


ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION = 1


def evaluate_order_archive_receipt(projection: dict, receipt: dict | None) -> str | None:
    """Return None only when a receipt is eligible under the cleanup gate."""
    if not receipt:
        return "receipt_missing"
    if receipt.get("status") != "verified":
        return "receipt_not_verified"
    if receipt.get("schemaVersion") != ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION:
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
    return None


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _integer(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def build_order_archive_projection(
    order_id: str,
    route_number: str,
    order_data: dict,
) -> dict:
    """Build the same immutable source projection used by the cleanup gate."""
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

    source = {
        "schemaVersion": ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION,
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
        "schemaVersion": ORDER_ARCHIVE_RECEIPT_SCHEMA_VERSION,
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
        "source": "postgres_order_sync",
        "verifiedAt": firestore.SERVER_TIMESTAMP,
    }
    db.collection("orderArchiveReceipts").document(projection["orderId"]).set(receipt)
    return receipt

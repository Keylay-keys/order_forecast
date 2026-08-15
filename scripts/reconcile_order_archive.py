#!/usr/bin/env python3
"""Read-only reconciliation of Firestore cleanup candidates against PostgreSQL.

This intentionally mirrors cleanupOldOrders eligibility, but performs no writes.
It compares each exact order ID with the archived header and derived line totals.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from .pg_utils import fetch_all
    from .order_archive_receipt import (
        build_order_archive_projection,
        evaluate_order_archive_receipt,
    )
except ImportError:
    from pg_utils import fetch_all
    from order_archive_receipt import (
        build_order_archive_projection,
        evaluate_order_archive_receipt,
    )


def _number(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _date_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None


def _firestore_projection(order_id: str, route_number: str, data: dict[str, Any]) -> dict[str, Any]:
    receipt_projection = build_order_archive_projection(order_id, route_number, data)
    stores = data.get("stores")
    stores = stores if isinstance(stores, list) else []
    line_count = 0
    total_units = 0
    raw_line_keys: set[tuple[str, str]] = set()
    duplicate_line_keys: list[str] = []

    for store in stores:
        if not isinstance(store, dict):
            continue
        store_id = str(store.get("id") or store.get("storeId") or "")
        items = store.get("items")
        items = items if isinstance(items, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            quantity = _number(item.get("quantity"))
            total_units += quantity
            if quantity == 0:
                continue
            line_count += 1
            key = (store_id, str(item.get("sap") or "").strip())
            if key in raw_line_keys:
                duplicate_line_keys.append(f"{key[0]}::{key[1]}")
            raw_line_keys.add(key)

    return {
        "orderId": order_id,
        "routeNumber": route_number,
        "scheduleKey": data.get("scheduleKey") or "unknown",
        "deliveryDate": _date_text(data.get("expectedDeliveryDate") or data.get("deliveryDate")),
        "lineCount": line_count,
        "totalUnits": total_units,
        "storeCount": len(stores),
        "duplicateLineKeys": sorted(set(duplicate_line_keys)),
        "sourceFingerprint": receipt_projection["sourceFingerprint"],
        "lines": receipt_projection["lines"],
    }


def _receipt_gate_summary(
    db: firestore.Client,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    eligible = []
    blocked_by_reason: dict[str, int] = {}
    for source in candidates:
        receipt_doc = db.collection("orderArchiveReceipts").document(source["orderId"]).get()
        projection = {
            "schemaVersion": 1,
            "orderId": source["orderId"],
            "routeNumber": source["routeNumber"],
            "scheduleKey": str(source["scheduleKey"]),
            "deliveryDate": source["deliveryDate"] or "",
            "status": "finalized",
            "totalUnits": source["totalUnits"],
            "storeCount": source["storeCount"],
            "lineItemCount": source["lineCount"],
            "sourceFingerprint": source["sourceFingerprint"],
        }
        reason = evaluate_order_archive_receipt(
            projection,
            receipt_doc.to_dict() if receipt_doc.exists else None,
        )
        if reason is None:
            eligible.append(source["orderId"])
        else:
            blocked_by_reason[reason] = blocked_by_reason.get(reason, 0) + 1
    return {
        "eligibleReceiptCount": len(eligible),
        "blockedReceiptCount": len(candidates) - len(eligible),
        "receiptBlockedByReason": blocked_by_reason,
        "eligibleReceiptOrderIds": eligible,
    }


def _load_cleanup_candidates(
    db: firestore.Client,
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], int]:
    candidates: list[dict[str, Any]] = []
    route_count = 0
    for route_ref in db.collection("routes").list_documents():
        route_count += 1
        query = (
            route_ref.collection("orders")
            .where(filter=FieldFilter("status", "==", "finalized"))
            .where(filter=FieldFilter("createdAt", "<", cutoff))
        )
        for order_doc in query.stream():
            candidates.append(
                _firestore_projection(order_doc.id, route_ref.id, order_doc.to_dict() or {})
            )
    candidates.sort(key=lambda row: (row["routeNumber"], row["orderId"]))
    return candidates, route_count


def _load_archive_rows(order_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not order_ids:
        return {}
    rows = fetch_all(
        """
        SELECT
            o.order_id,
            o.route_number,
            o.schedule_key,
            o.delivery_date,
            o.status,
            o.total_units,
            o.store_count,
            COUNT(DISTINCT li.line_item_id)::int AS line_count,
            COALESCE(SUM(li.quantity), 0)::int AS line_units
        FROM orders_historical o
        LEFT JOIN order_line_items li ON li.order_id = o.order_id
        WHERE o.order_id = ANY(%s)
        GROUP BY o.order_id, o.route_number, o.schedule_key, o.delivery_date,
                 o.status, o.total_units, o.store_count
        """,
        [order_ids],
    )
    archived = {str(row["order_id"]): row for row in rows}
    line_rows = fetch_all(
        """
        SELECT order_id, store_id, store_name, sap, quantity, cases
        FROM order_line_items
        WHERE order_id = ANY(%s)
        ORDER BY order_id, store_id, sap
        """,
        [order_ids],
    )
    for row in line_rows:
        order_id = str(row["order_id"])
        if order_id not in archived:
            continue
        archived[order_id].setdefault("lines", []).append({
            "storeId": str(row.get("store_id") or "").strip(),
            "storeName": str(row.get("store_name") or "").strip(),
            "sap": str(row.get("sap") or "").strip(),
            "quantity": _number(row.get("quantity")),
            "cases": _number(row.get("cases")),
        })
    for row in archived.values():
        row.setdefault("lines", [])
        row["lines"].sort(
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    return archived


def _compare_sources(
    candidates: list[dict[str, Any]],
    archived: dict[str, dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    verified: list[str] = []
    anomalies: list[dict[str, Any]] = []
    for source in candidates:
        order_id = source["orderId"]
        archive = archived.get(order_id)
        reasons: list[str] = []
        if archive is None:
            reasons.append("missing_archive_header")
        else:
            comparisons = {
                "route_mismatch": str(archive["route_number"]) != source["routeNumber"],
                "schedule_mismatch": str(archive["schedule_key"]) != str(source["scheduleKey"]),
                "delivery_date_mismatch": _date_text(archive["delivery_date"]) != source["deliveryDate"],
                "header_total_units_mismatch": _number(archive["total_units"]) != source["totalUnits"],
                "line_total_units_mismatch": _number(archive["line_units"]) != source["totalUnits"],
                "store_count_mismatch": _number(archive["store_count"]) != source["storeCount"],
                "line_count_mismatch": _number(archive["line_count"]) != source["lineCount"],
                "archive_status_mismatch": str(archive["status"]) != "finalized",
            }
            reasons.extend(name for name, failed in comparisons.items() if failed)
            if "lines" in source and archive.get("lines") != source["lines"]:
                reasons.append("line_projection_mismatch")
        if source.get("duplicateLineKeys"):
            reasons.append("duplicate_firestore_line_keys")

        if reasons:
            anomalies.append({
                "orderId": order_id,
                "routeNumber": source["routeNumber"],
                "reasons": reasons,
                "firestore": {
                    "deliveryDate": source["deliveryDate"],
                    "lineCount": source["lineCount"],
                    "totalUnits": source["totalUnits"],
                    "storeCount": source["storeCount"],
                },
                "postgres": None if archive is None else {
                    "deliveryDate": _date_text(archive["delivery_date"]),
                    "lineCount": _number(archive["line_count"]),
                    "lineUnits": _number(archive["line_units"]),
                    "totalUnits": _number(archive["total_units"]),
                    "storeCount": _number(archive["store_count"]),
                },
            })
        else:
            verified.append(order_id)
    return verified, anomalies


def reconcile(service_account: str, retention_days: int) -> dict[str, Any]:
    captured_at = datetime.now(timezone.utc)
    cutoff = captured_at - timedelta(days=retention_days)
    db = firestore.Client.from_service_account_json(service_account)
    candidates, route_count = _load_cleanup_candidates(db, cutoff)
    archived = _load_archive_rows([row["orderId"] for row in candidates])
    verified, anomalies = _compare_sources(candidates, archived)
    receipt_gate = _receipt_gate_summary(db, candidates)

    return {
        "mode": "read_only",
        "capturedAt": captured_at.isoformat(),
        "retentionDays": retention_days,
        "cutoff": cutoff.isoformat(),
        "routesScanned": route_count,
        "cleanupCandidates": len(candidates),
        "archiveHeadersFound": len(archived),
        "verifiedExactMatches": len(verified),
        "anomalyCount": len(anomalies),
        "verifiedOrderIds": verified,
        "anomalies": anomalies,
        **receipt_gate,
    }


def reconcile_managed_export(source_summary: str) -> dict[str, Any]:
    captured_at = datetime.now(timezone.utc)
    with open(source_summary, encoding="utf-8") as source_file:
        payload = json.load(source_file)
    candidates = [row for row in payload.get("orders", []) if row.get("status") == "finalized"]
    archived = _load_archive_rows([row["orderId"] for row in candidates])
    verified, anomalies = _compare_sources(candidates, archived)
    return {
        "mode": "read_only_managed_export_reconciliation",
        "capturedAt": captured_at.isoformat(),
        "sourceExport": payload.get("exportDirectory"),
        "sourceFinalizedOrders": len(candidates),
        "archiveHeadersFound": len(archived),
        "verifiedExactMatches": len(verified),
        "anomalyCount": len(anomalies),
        "verifiedOrderIds": verified,
        "anomalies": anomalies,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--serviceAccount")
    source.add_argument("--managed-export-summary")
    parser.add_argument("--retention-days", type=int, default=90)
    args = parser.parse_args()
    if args.retention_days <= 0:
        parser.error("--retention-days must be positive")
    result = (
        reconcile_managed_export(args.managed_export_summary)
        if args.managed_export_summary
        else reconcile(args.serviceAccount, args.retention_days)
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Bounded, dry-run-first repair for legacy store-reallocation overlays.

The command requires one exact route and order. It never scans or mutates a
route broadly, and it refuses any record whose Firestore/PostgreSQL/archive
evidence does not prove the overlay is still unapplied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from google.cloud import firestore

try:
    from ..api.models import StoreReallocationMoveRequest
    from ..api.order_reallocation import apply_store_reallocation, moves_signature
    from .db_manager_pg import handle_sync_order
    from .order_archive_receipt import build_order_archive_projection, evaluate_order_archive_receipt
    from .pg_utils import get_pg_connection
    from .reconcile_order_archive import _compare_sources, _firestore_projection, _load_archive_rows
except ImportError:
    try:
        from order_forecast.api.models import StoreReallocationMoveRequest
        from order_forecast.api.order_reallocation import apply_store_reallocation, moves_signature
        from order_forecast.scripts.db_manager_pg import handle_sync_order
        from order_forecast.scripts.order_archive_receipt import build_order_archive_projection, evaluate_order_archive_receipt
        from order_forecast.scripts.pg_utils import get_pg_connection
        from order_forecast.scripts.reconcile_order_archive import _compare_sources, _firestore_projection, _load_archive_rows
    except ImportError:
        from api.models import StoreReallocationMoveRequest
        from api.order_reallocation import apply_store_reallocation, moves_signature
        from db_manager_pg import handle_sync_order
        from order_archive_receipt import build_order_archive_projection, evaluate_order_archive_receipt
        from pg_utils import get_pg_connection
        from reconcile_order_archive import _compare_sources, _firestore_projection, _load_archive_rows


MUTATION_AUDIT_ACTIONS = {
    "order_store_reallocated",
    "order_full_adjustment_confirmed",
    "order_adjustment_applied",
}


def allocation_hash(order_data: Dict[str, Any]) -> str:
    rows = []
    for store in order_data.get("stores") or []:
        store_id = str(store.get("storeId") or store.get("id") or "")
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            quantity = int(item.get("quantity") or 0)
            if sap and quantity:
                rows.append({"storeId": store_id, "sap": sap, "quantity": quantity})
    rows.sort(key=lambda row: (row["storeId"], row["sap"]))
    encoded = json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def route_totals(order_data: Dict[str, Any]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for store in order_data.get("stores") or []:
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            quantity = item.get("quantity")
            if sap and isinstance(quantity, int) and not isinstance(quantity, bool) and quantity > 0:
                totals[sap] = totals.get(sap, 0) + quantity
    return dict(sorted(totals.items()))


def _legacy_applied_at(row: Dict[str, Any]) -> int:
    value = (row.get("storeReallocation") or {}).get("appliedAtMs")
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def classify_legacy_reallocation(
    *,
    order_data: Dict[str, Any],
    adjustments: List[Dict[str, Any]],
    archive_exact: bool,
    receipt_exact: bool,
    audit_actions: Iterable[str],
) -> tuple[str, List[str]]:
    relevant = [
        row for row in adjustments
        if row.get("status") == "applied" and row.get("mode") == "store_reallocation"
    ]
    if not relevant:
        return "ambiguous", ["no_legacy_applied_reallocation"]
    marked = [
        (row.get("storeReallocation") or {}).get("appliedOrderRevision") is not None
        for row in relevant
    ]
    if all(marked):
        return "already_marked", []
    summary = order_data.get("storeReallocationSummary") or {}
    if summary.get("count") or (order_data.get("lastMutation") or {}).get("kind") == "store_reallocation":
        return "already_marked", []

    reasons = []
    if any(marked):
        reasons.append("mixed_legacy_markers")
    if int(order_data.get("orderRevision") or 0) != 0:
        reasons.append("order_already_revisioned")
    if order_data.get("orderAdjustmentAppliedAtMs"):
        reasons.append("other_adjustment_marker_present")
    if any(action in MUTATION_AUDIT_ACTIONS for action in audit_actions):
        reasons.append("post_finalization_mutation_audit_present")
    if any(
        row.get("status") == "applied" and row.get("mode") != "store_reallocation"
        for row in adjustments
    ):
        reasons.append("other_applied_adjustment_present")
    if not archive_exact:
        reasons.append("postgres_projection_not_exact")
    if not receipt_exact:
        reasons.append("archive_receipt_not_exact")
    if any(not (row.get("storeReallocation") or {}).get("moves") for row in relevant):
        reasons.append("legacy_moves_missing")
    return ("ambiguous", reasons) if reasons else ("safe_unapplied", [])


@firestore.transactional
def _apply_safe_legacy_reallocations(
    transaction,
    *,
    order_ref,
    adjustment_refs,
    audit_refs,
    stores_ref,
    products_ref,
    route_number: str,
    now: datetime,
) -> Dict[str, Any]:
    order_doc = order_ref.get(transaction=transaction)
    adjustment_docs = [ref.get(transaction=transaction) for ref in adjustment_refs]
    if not order_doc.exists or any(not doc.exists for doc in adjustment_docs):
        raise RuntimeError("LEGACY_REPAIR_SOURCE_MISSING")
    order_data = order_doc.to_dict() or {}
    if int(order_data.get("orderRevision") or 0) != 0 or order_data.get("storeReallocationSummary"):
        raise RuntimeError("LEGACY_REPAIR_ORDER_ALREADY_MUTATED")

    adjustment_rows = [doc.to_dict() or {} for doc in adjustment_docs]
    raw_moves = [
        move
        for row in adjustment_rows
        for move in ((row.get("storeReallocation") or {}).get("moves") or [])
    ]
    saps = sorted({str(move.get("sap") or "") for move in raw_moves if move.get("sap")})
    store_docs = list(stores_ref.stream(transaction=transaction))
    route_stores = {doc.id: (doc.to_dict() or {}) for doc in store_docs}
    products = {}
    for sap in saps:
        product_doc = products_ref.document(sap).get(transaction=transaction)
        if product_doc.exists:
            products[sap] = product_doc.to_dict() or {}

    current = order_data
    applied = []
    base_revision = 0
    at_ms = int(now.timestamp() * 1000)
    for ref, audit_ref, row in zip(adjustment_refs, audit_refs, adjustment_rows):
        snapshot = row.get("storeReallocation") or {}
        if row.get("status") != "applied" or row.get("mode") != "store_reallocation":
            raise RuntimeError("LEGACY_REPAIR_ADJUSTMENT_CHANGED")
        if snapshot.get("appliedOrderRevision") is not None:
            raise RuntimeError("LEGACY_REPAIR_ALREADY_MARKED")
        moves = [
            StoreReallocationMoveRequest(
                sap=str(move.get("sap") or ""),
                fromStoreId=str(move.get("fromStoreId") or ""),
                toStoreId=str(move.get("toStoreId") or ""),
                unitQuantity=int(
                    move.get("unitQuantity")
                    or (int(move.get("caseQuantity") or 0) * int(move.get("casePack") or 0))
                ),
            )
            for move in snapshot.get("moves") or []
        ]
        result = apply_store_reallocation(
            order_data=current,
            moves=moves,
            route_stores=route_stores,
            products=products,
            enforce_core_items=os.environ.get("CORE_ITEM_ENFORCEMENT_ENABLED", "false").lower() == "true",
        )
        next_revision = base_revision + 1
        adjustment_id = str(ref.id)
        updated_snapshot = {
            **snapshot,
            "movesSignature": moves_signature(moves),
            "baseOrderRevision": base_revision,
            "appliedOrderRevision": next_revision,
            "reallocationCount": next_revision,
            "legacyRepairAppliedAtMs": at_ms,
        }
        transaction.update(ref, {
            "storeReallocation": updated_snapshot,
            "projection": {
                "status": "pending",
                "targetOrderRevision": next_revision,
                "attemptCount": 0,
            },
            "updatedAt": now,
        })
        transaction.set(audit_ref, {
            "orderId": str(order_ref.id),
            "routeNumber": route_number,
            "action": "order_store_reallocated",
            "source": "legacy_repair",
            "meta": {
                "reallocationId": adjustment_id,
                "baseOrderRevision": base_revision,
                "appliedOrderRevision": next_revision,
                "movesSignature": moves_signature(moves),
            },
            "createdAt": now,
        })
        current = {**current, "stores": result["stores"]}
        base_revision = next_revision
        applied.append(adjustment_id)

    last_id = applied[-1]
    transaction.update(order_ref, {
        "stores": current["stores"],
        "updatedAt": now,
        "orderRevision": base_revision,
        "lastMutation": {"kind": "store_reallocation", "mutationId": last_id, "atMs": at_ms},
        "storeReallocationSummary": {
            "count": len(applied),
            "lastAppliedAtMs": at_ms,
            "lastAdjustmentId": last_id,
        },
    })
    return {"orderRevision": base_revision, "adjustmentIds": applied, "stores": current["stores"]}


def run(args) -> Dict[str, Any]:
    db = firestore.Client.from_service_account_json(args.service_account)
    route_ref = db.collection("routes").document(args.route)
    order_ref = route_ref.collection("orders").document(args.order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise RuntimeError("ORDER_NOT_FOUND")
    order_data = order_doc.to_dict() or {}
    adjustment_docs = list(
        route_ref.collection("orderAdjustments").where("sourceOrderId", "==", args.order_id).stream()
    )
    adjustment_docs.sort(key=lambda doc: (_legacy_applied_at(doc.to_dict() or {}), doc.id))
    adjustments = [doc.to_dict() or {} for doc in adjustment_docs]
    source = _firestore_projection(args.order_id, args.route, order_data)
    archive = _load_archive_rows([args.order_id])
    verified, _ = _compare_sources([source], archive)
    receipt_doc = db.collection("orderArchiveReceipts").document(args.order_id).get()
    receipt = receipt_doc.to_dict() if receipt_doc.exists else None
    projection = build_order_archive_projection(args.order_id, args.route, order_data)
    receipt_exact = evaluate_order_archive_receipt(projection, receipt) is None
    audit_actions = [
        (doc.to_dict() or {}).get("action")
        for doc in order_ref.collection("audit").stream()
    ]
    classification, reasons = classify_legacy_reallocation(
        order_data=order_data,
        adjustments=adjustments,
        archive_exact=args.order_id in verified,
        receipt_exact=receipt_exact,
        audit_actions=audit_actions,
    )
    report = {
        "mode": "apply" if args.apply_safe else "dry_run",
        "routeNumber": args.route,
        "orderId": args.order_id,
        "classification": classification,
        "reasons": reasons,
        "beforeAllocationHash": allocation_hash(order_data),
        "beforeRouteTotals": route_totals(order_data),
        "legacyAdjustmentIds": [doc.id for doc in adjustment_docs],
        "archiveReceiptId": args.order_id,
        "archiveExact": args.order_id in verified,
        "receiptExact": receipt_exact,
    }
    if not args.apply_safe:
        return report
    if args.confirm_order_id != args.order_id:
        raise RuntimeError("--confirm-order-id must exactly match --order-id")
    if classification != "safe_unapplied":
        raise RuntimeError(f"LEGACY_REPAIR_REFUSED_{classification.upper()}")

    legacy_docs = [
        doc for doc in adjustment_docs
        if (doc.to_dict() or {}).get("status") == "applied"
        and (doc.to_dict() or {}).get("mode") == "store_reallocation"
        and (doc.to_dict() or {}).get("storeReallocation", {}).get("appliedOrderRevision") is None
    ]
    result = _apply_safe_legacy_reallocations(
        db.transaction(),
        order_ref=order_ref,
        adjustment_refs=[doc.reference for doc in legacy_docs],
        audit_refs=[order_ref.collection("audit").document(f"store-reallocation-{doc.id}") for doc in legacy_docs],
        stores_ref=route_ref.collection("stores"),
        products_ref=db.collection("masterCatalog").document(args.route).collection("products"),
        route_number=args.route,
        now=datetime.now(timezone.utc),
    )
    sync = handle_sync_order(get_pg_connection(), db, {"orderId": args.order_id, "routeNumber": args.route})
    verified_after = False
    receipt_after_exact = False
    if "error" not in sync:
        repaired_doc = order_ref.get()
        repaired_data = repaired_doc.to_dict() or {}
        repaired_source = _firestore_projection(args.order_id, args.route, repaired_data)
        repaired_archive = _load_archive_rows([args.order_id])
        repaired_verified, _ = _compare_sources([repaired_source], repaired_archive)
        verified_after = args.order_id in repaired_verified
        repaired_receipt_doc = db.collection("orderArchiveReceipts").document(args.order_id).get()
        repaired_receipt = repaired_receipt_doc.to_dict() if repaired_receipt_doc.exists else None
        receipt_after_exact = evaluate_order_archive_receipt(
            build_order_archive_projection(args.order_id, args.route, repaired_data),
            repaired_receipt,
        ) is None
        if verified_after and receipt_after_exact:
            for doc in legacy_docs:
                doc.reference.update({
                    "projection.status": "succeeded",
                    "projection.projectedOrderRevision": result["orderRevision"],
                    "projection.completedAt": firestore.SERVER_TIMESTAMP,
                    "projection.attemptCount": firestore.Increment(1),
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                })
    report.update({
        "applied": True,
        "orderRevision": result["orderRevision"],
        "afterAllocationHash": allocation_hash({"stores": result["stores"]}),
        "afterRouteTotals": route_totals({"stores": result["stores"]}),
        "projection": "succeeded" if verified_after and receipt_after_exact else "failed",
        "postgresArchiveVerified": verified_after,
        "archiveReceiptVerified": receipt_after_exact,
    })
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-account", required=True)
    parser.add_argument("--route", required=True)
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--apply-safe", action="store_true")
    parser.add_argument("--confirm-order-id")
    args = parser.parse_args()
    if not args.route.isdigit() or len(args.route) > 10:
        parser.error("--route must be 1-10 digits")
    print(json.dumps(run(args), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

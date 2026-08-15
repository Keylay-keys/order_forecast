#!/usr/bin/env python3
"""Verify finalized Firestore orders against PostgreSQL and mint archive receipts.

The default mode is read-only. Production writes require both --apply and the
exact confirmation phrase. This tool never deletes orders or archive rows.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from .order_archive_receipt import (
        build_order_archive_projection,
        evaluate_order_archive_receipt,
        write_verified_order_archive_receipt,
    )
    from .reconcile_order_archive import (
        _compare_sources,
        _firestore_projection,
        _load_archive_rows,
    )
except ImportError:
    from order_archive_receipt import (
        build_order_archive_projection,
        evaluate_order_archive_receipt,
        write_verified_order_archive_receipt,
    )
    from reconcile_order_archive import (
        _compare_sources,
        _firestore_projection,
        _load_archive_rows,
    )


APPLY_CONFIRMATION = "WRITE_VERIFIED_ARCHIVE_RECEIPTS"


def _load_finalized_orders(
    db: firestore.Client,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], int]:
    summaries = []
    source_by_id = {}
    route_count = 0
    for route_ref in db.collection("routes").list_documents():
        route_count += 1
        query = route_ref.collection("orders").where(
            filter=FieldFilter("status", "==", "finalized")
        )
        for order_doc in query.stream():
            data = order_doc.to_dict() or {}
            summaries.append(_firestore_projection(order_doc.id, route_ref.id, data))
            source_by_id[order_doc.id] = {
                "routeNumber": route_ref.id,
                "data": data,
            }
    summaries.sort(key=lambda row: (row["routeNumber"], row["orderId"]))
    return summaries, source_by_id, route_count


def run_backfill(service_account: str, apply: bool) -> dict[str, Any]:
    captured_at = datetime.now(timezone.utc)
    db = firestore.Client.from_service_account_json(service_account)
    summaries, source_by_id, route_count = _load_finalized_orders(db)
    archived = _load_archive_rows([row["orderId"] for row in summaries])
    verified_ids, anomalies = _compare_sources(summaries, archived)

    already_eligible = []
    needs_receipt = []
    for order_id in verified_ids:
        source = source_by_id[order_id]
        projection = build_order_archive_projection(
            order_id,
            source["routeNumber"],
            source["data"],
        )
        receipt_doc = db.collection("orderArchiveReceipts").document(order_id).get()
        receipt = receipt_doc.to_dict() if receipt_doc.exists else None
        if evaluate_order_archive_receipt(projection, receipt) is None:
            already_eligible.append(order_id)
        else:
            needs_receipt.append(order_id)

    written = []
    if apply:
        for order_id in needs_receipt:
            source = source_by_id[order_id]
            archive = archived[order_id]
            write_verified_order_archive_receipt(
                db,
                order_id,
                source["routeNumber"],
                source["data"],
                archive_total_units=int(archive["line_units"] or 0),
                archive_store_count=int(archive["store_count"] or 0),
                archive_line_item_count=int(archive["line_count"] or 0),
            )
            written.append(order_id)

    return {
        "mode": "apply" if apply else "dry_run",
        "capturedAt": captured_at.isoformat(),
        "routesScanned": route_count,
        "finalizedFirestoreOrders": len(summaries),
        "archiveHeadersFound": len(archived),
        "verifiedExactMatches": len(verified_ids),
        "anomalyCount": len(anomalies),
        "alreadyEligibleReceiptCount": len(already_eligible),
        "wouldWriteReceiptCount": len(needs_receipt),
        "writtenReceiptCount": len(written),
        "writtenOrderIds": written,
        "anomalies": anomalies,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serviceAccount", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirmation", default="")
    args = parser.parse_args()
    if args.apply and args.confirmation != APPLY_CONFIRMATION:
        parser.error(f"--apply requires --confirmation {APPLY_CONFIRMATION}")
    print(json.dumps(run_backfill(args.serviceAccount, args.apply), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

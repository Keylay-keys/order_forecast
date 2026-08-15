#!/usr/bin/env python3
"""Download selected preserved exports, decode orders, and retain summaries only."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

try:
    from .read_firestore_managed_order_export import extract
except ImportError:
    from read_firestore_managed_order_export import extract


COMPARE_FIELDS = (
    "routeNumber",
    "status",
    "scheduleKey",
    "deliveryDate",
    "lineCount",
    "totalUnits",
    "storeCount",
)


def collect(
    source_root: str,
    dates: list[str],
    temp_root: Path,
    gcloud_sdk_lib: Path,
) -> dict[str, Any]:
    temp_root.mkdir(parents=True, exist_ok=True)
    orders: dict[str, dict[str, Any]] = {}
    first_seen: dict[str, str] = {}
    mutations: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []

    for export_date in dates:
        destination = temp_root / export_date
        if destination.exists():
            shutil.rmtree(destination)
        source = f"{source_root.rstrip('/')}/{export_date}"
        print(f"[{export_date}] downloading preserved export", flush=True)
        completed = subprocess.run(
            ["gcloud", "storage", "cp", "--recursive", source, str(temp_root)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"gcloud copy failed for {export_date}: {completed.stderr.strip()[-1000:]}"
            )

        print(f"[{export_date}] CRC-verifying and decoding", flush=True)
        decoded = extract(destination, gcloud_sdk_lib)
        snapshot_orders = decoded.pop("orders")
        finalized = [row for row in snapshot_orders if row.get("status") == "finalized"]
        snapshots.append({"date": export_date, **decoded})
        for row in finalized:
            order_id = row["orderId"]
            previous = orders.get(order_id)
            if previous is not None:
                changed = [field for field in COMPARE_FIELDS if previous.get(field) != row.get(field)]
                if changed:
                    mutations.append({
                        "orderId": order_id,
                        "previousSnapshot": previous["lastSeenSnapshot"],
                        "currentSnapshot": export_date,
                        "changedFields": changed,
                    })
            else:
                first_seen[order_id] = export_date
            orders[order_id] = {
                **row,
                "firstSeenSnapshot": first_seen[order_id],
                "lastSeenSnapshot": export_date,
            }

        shutil.rmtree(destination)
        print(
            f"[{export_date}] finalized={len(finalized)} union={len(orders)} temp-removed",
            flush=True,
        )

    merged_orders = sorted(orders.values(), key=lambda row: (row["routeNumber"], row["orderId"]))
    return {
        "mode": "read_only_preserved_export_collection",
        "sourceRoot": source_root,
        "selectedSnapshotDates": dates,
        "snapshotsProcessed": len(snapshots),
        "uniqueFinalizedOrders": len(merged_orders),
        "mutatedFinalizedOrderCount": len(mutations),
        "mutatedFinalizedOrders": mutations,
        "snapshots": snapshots,
        "orders": merged_orders,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--temp-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--gcloud-sdk-lib",
        type=Path,
        default=Path("/opt/homebrew/share/google-cloud-sdk/lib"),
    )
    args = parser.parse_args()
    result = collect(args.source_root, args.dates, args.temp_root, args.gcloud_sdk_lib)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key not in ("orders", "snapshots")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

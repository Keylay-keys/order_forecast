#!/usr/bin/env python3
"""Force a dry-run low-qty notification evaluation for a single route right now.

This does not modify the live user doc or PostgreSQL notification ledger and
cannot call Expo. It resolves the current owner and timezone, evaluates the
route's low-quantity inventory for today's local date, and prints the result.

Usage:
    cd /Users/kylemacmini/projects/routespark/restore-2025-09-24/order_forecast
    source venv/bin/activate
    python scripts/test_low_qty_send_now.py --serviceAccount /path/to/sa.json --route 989262
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo


def _load_owner_context(daemon_module, db, route_number: str) -> tuple[str, str]:
    owner_uid = daemon_module.get_route_owner(db, route_number)
    if not owner_uid:
        raise RuntimeError(f"Could not resolve route owner for {route_number}")

    user_doc = db.collection("users").document(owner_uid).get()
    if not user_doc.exists:
        raise RuntimeError(f"Owner user doc {owner_uid} not found")

    data = user_doc.to_dict() or {}
    profile = data.get("profile", {}) if isinstance(data.get("profile"), dict) else {}
    timezone = daemon_module.validate_timezone(profile.get("timezone"))
    return owner_uid, timezone


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run low-qty notification test for one route, forced to now")
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    parser.add_argument("--route", default="989262", help="Route number to test")
    parser.add_argument("--pg-host", default=os.environ.get("POSTGRES_HOST", "192.168.1.40"), help="PostgreSQL host")
    parser.add_argument("--pg-port", default=os.environ.get("POSTGRES_PORT", "5432"), help="PostgreSQL port")
    parser.add_argument("--pg-db", default=os.environ.get("POSTGRES_DB", "routespark"), help="PostgreSQL database")
    parser.add_argument("--pg-user", default=os.environ.get("POSTGRES_USER", "routespark"), help="PostgreSQL user")
    parser.add_argument("--pg-password", default=os.environ.get("POSTGRES_PASSWORD", ""), help="PostgreSQL password")
    args = parser.parse_args()

    os.environ["ROUTESPARK_ALLOWED_ROUTES"] = str(args.route)
    os.environ["POSTGRES_HOST"] = str(args.pg_host)
    os.environ["POSTGRES_PORT"] = str(args.pg_port)
    os.environ["POSTGRES_DB"] = str(args.pg_db)
    os.environ["POSTGRES_USER"] = str(args.pg_user)
    os.environ["POSTGRES_PASSWORD"] = str(args.pg_password)

    import low_qty_notification_daemon as daemon

    db = daemon.get_firestore_client(args.serviceAccount)
    owner_uid, timezone_name = _load_owner_context(daemon, db, str(args.route))
    order_date = datetime.now(ZoneInfo(timezone_name)).date().isoformat()
    items = daemon.get_items_for_order_date(
        db,
        str(args.route),
        order_date,
        resolved_timezone=timezone_name,
    )
    saps = sorted({str(item.sap) for item in items})

    print("=" * 60)
    print("Low-Qty Inventory Preview")
    print("=" * 60)
    print(f"route: {args.route}")
    print(f"owner: {owner_uid}")
    print(f"timezone: {timezone_name}")
    print(f"order date: {order_date}")
    print(f"postgres: {args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_db}")
    print("send capability: disabled (production sends require the ledger)")
    print(f"low-quantity items: {len(items)}")
    print(f"SAPs: {', '.join(saps) if saps else '(none)'}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

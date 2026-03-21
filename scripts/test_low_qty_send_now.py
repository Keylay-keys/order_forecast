#!/usr/bin/env python3
"""Force a dry-run low-qty notification evaluation for a single route right now.

This does not modify the live user doc. It builds an in-memory reminder-cache
entry using the owner's current timezone and the current clock time, then runs
the daemon's normal `check_and_notify()` path in dry-run mode.

Usage:
    cd /Users/kylemacmini/Desktop/routespark/restore-2025-09-24/order_forecast
    source venv/bin/activate
    python scripts/test_low_qty_send_now.py --serviceAccount /path/to/sa.json --route 989262
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

import pytz


def _load_owner_context(daemon_module, db, route_number: str) -> tuple[str, str]:
    owner_uid = daemon_module.get_route_owner(db, route_number)
    if not owner_uid:
        raise RuntimeError(f"Could not resolve route owner for {route_number}")

    user_doc = db.collection("users").document(owner_uid).get()
    if not user_doc.exists:
        raise RuntimeError(f"Owner user doc {owner_uid} not found")

    data = user_doc.to_dict() or {}
    profile = data.get("profile", {}) if isinstance(data.get("profile"), dict) else {}
    timezone = profile.get("timezone") or "America/Denver"
    return owner_uid, timezone


def _current_reminder_time(timezone_name: str) -> dict:
    try:
        tz = pytz.timezone(timezone_name)
    except pytz.UnknownTimeZoneError:
        tz = pytz.timezone("America/Denver")

    now = datetime.now(tz)
    hour_24 = now.hour
    minute = now.minute
    period = "AM" if hour_24 < 12 else "PM"
    hour_12 = hour_24 % 12
    if hour_12 == 0:
        hour_12 = 12

    return {
        "hour": hour_12,
        "minute": minute,
        "period": period,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run low-qty notification test for one route, forced to now")
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    parser.add_argument("--route", default="989262", help="Route number to test")
    parser.add_argument("--real-send", action="store_true", help="Actually send the notification instead of dry-run")
    parser.add_argument("--pg-host", default=os.environ.get("POSTGRES_HOST", "192.168.1.40"), help="PostgreSQL host")
    parser.add_argument("--pg-port", default=os.environ.get("POSTGRES_PORT", "5432"), help="PostgreSQL port")
    parser.add_argument("--pg-db", default=os.environ.get("POSTGRES_DB", "routespark"), help="PostgreSQL database")
    parser.add_argument("--pg-user", default=os.environ.get("POSTGRES_USER", "routespark"), help="PostgreSQL user")
    parser.add_argument("--pg-password", default=os.environ.get("POSTGRES_PASSWORD", ""), help="PostgreSQL password")
    args = parser.parse_args()

    dry_run = not args.real_send
    os.environ["LOW_QTY_NOTIFICATION_DRY_RUN"] = "true" if dry_run else "false"
    os.environ["ROUTESPARK_ALLOWED_ROUTES"] = str(args.route)
    os.environ["POSTGRES_HOST"] = str(args.pg_host)
    os.environ["POSTGRES_PORT"] = str(args.pg_port)
    os.environ["POSTGRES_DB"] = str(args.pg_db)
    os.environ["POSTGRES_USER"] = str(args.pg_user)
    os.environ["POSTGRES_PASSWORD"] = str(args.pg_password)

    import low_qty_notification_daemon as daemon

    # Update module globals to match the forced dry-run env in this process.
    daemon.LOW_QTY_NOTIFICATION_DRY_RUN = dry_run

    db = daemon.get_firestore_client(args.serviceAccount)
    owner_uid, timezone_name = _load_owner_context(daemon, db, str(args.route))
    reminder_time = _current_reminder_time(timezone_name)

    daemon.reminder_cache.clear()
    daemon.reminder_cache[owner_uid] = {
        "route_number": str(args.route),
        "reminder_time": reminder_time,
        "timezone": timezone_name,
    }

    print("=" * 60)
    print("Low-Qty Send-Now Dry-Run")
    print("=" * 60)
    print(f"route: {args.route}")
    print(f"owner: {owner_uid}")
    print(f"timezone: {timezone_name}")
    print(f"forced reminder time: {reminder_time}")
    print(f"postgres: {args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_db}")
    print(f"dry run: {str(dry_run).lower()}")
    print()

    daemon.check_and_notify(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

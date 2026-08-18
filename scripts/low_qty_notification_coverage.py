#!/usr/bin/env python3
"""Read-only Firebase/PostgreSQL low-quantity preference coverage check.

Run this only at the bounded cutover gate. It performs the enabled-users
Firestore query and authoritative owner point reads but never writes either
system. Output omits user IDs and reports only route-level mismatch fields.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from .low_qty_notification_daemon import get_firestore_client, get_route_owner_strict
    from .low_qty_notification_store import load_enabled_preference_snapshot
    from .low_qty_schedule import parse_reminder_minute, validate_timezone
except ImportError:
    from low_qty_notification_daemon import get_firestore_client, get_route_owner_strict
    from low_qty_notification_store import load_enabled_preference_snapshot
    from low_qty_schedule import parse_reminder_minute, validate_timezone


FOCUS_ROUTES = ("961825", "985957")


def _reminder_settings(user_data: dict[str, Any]) -> dict[str, Any]:
    settings = user_data.get("userSettings")
    if not isinstance(settings, dict):
        return {}
    notifications = settings.get("notifications")
    if not isinstance(notifications, dict):
        return {}
    reminder = notifications.get("orderReminders")
    return reminder if isinstance(reminder, dict) else {}


def load_firebase_snapshot(db) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    query = db.collection("users").where(
        filter=FieldFilter(
            "userSettings.notifications.orderReminders.enabled",
            "==",
            True,
        )
    )
    documents = list(query.stream())
    authoritative: dict[str, dict[str, Any]] = {}
    invalid_routes: set[str] = set()
    non_owner_count = 0

    for snapshot in documents:
        data = snapshot.to_dict() or {}
        profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
        route_number = str(profile.get("currentRoute") or profile.get("routeNumber") or "").strip()
        if not route_number:
            invalid_routes.add("missing_route")
            continue

        owner_uid = get_route_owner_strict(db, route_number)
        if not owner_uid:
            invalid_routes.add(route_number)
            continue
        if owner_uid != snapshot.id:
            non_owner_count += 1
            continue

        reminder = _reminder_settings(data)
        if reminder.get("enabled") is not True:
            invalid_routes.add(route_number)
            continue
        try:
            normalized = {
                "owner_uid": owner_uid,
                "reminder_minute_local": parse_reminder_minute(reminder.get("time")),
                "timezone": validate_timezone(profile.get("timezone")),
            }
        except ValueError:
            invalid_routes.add(route_number)
            continue
        if route_number in authoritative:
            invalid_routes.add(route_number)
            continue
        authoritative[route_number] = normalized

    diagnostics = {
        "enabled_user_documents": len(documents),
        "authoritative_preferences": len(authoritative),
        "non_owner_documents": non_owner_count,
        "invalid_routes": sorted(invalid_routes),
    }
    return authoritative, diagnostics


def compare_snapshots(
    firebase_rows: dict[str, dict[str, Any]],
    postgres_rows: list[dict[str, Any]],
    *,
    focus_routes: tuple[str, ...] = FOCUS_ROUTES,
) -> dict[str, Any]:
    postgres_by_route = {str(row["route_number"]): row for row in postgres_rows}
    firebase_routes = set(firebase_rows)
    postgres_routes = set(postgres_by_route)
    mismatches: dict[str, list[str]] = {}
    for route_number in sorted(firebase_routes & postgres_routes):
        fields = []
        expected = firebase_rows[route_number]
        actual = postgres_by_route[route_number]
        for field_name in ("owner_uid", "reminder_minute_local", "timezone"):
            if actual.get(field_name) != expected.get(field_name):
                fields.append(field_name)
        if fields:
            mismatches[route_number] = fields

    missing = sorted(firebase_routes - postgres_routes)
    unexpected = sorted(postgres_routes - firebase_routes)
    focus = {}
    for route_number in focus_routes:
        if route_number in missing:
            focus[route_number] = "missing_postgres"
        elif route_number in unexpected:
            focus[route_number] = "unexpected_postgres"
        elif route_number in mismatches:
            focus[route_number] = "field_mismatch"
        elif route_number in firebase_routes:
            focus[route_number] = "match"
        else:
            focus[route_number] = "not_currently_enabled_owner"

    return {
        "ready": not missing and not unexpected and not mismatches,
        "firebase_count": len(firebase_rows),
        "postgres_count": len(postgres_rows),
        "missing_postgres": missing,
        "unexpected_postgres": unexpected,
        "field_mismatches": mismatches,
        "focus_routes": focus,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only low-quantity Firebase/PostgreSQL coverage comparison",
    )
    parser.add_argument("--serviceAccount", required=True)
    args = parser.parse_args()

    db = get_firestore_client(args.serviceAccount)
    firebase_rows, diagnostics = load_firebase_snapshot(db)
    postgres_rows = load_enabled_preference_snapshot()
    result = compare_snapshots(firebase_rows, postgres_rows)
    result["firebase_diagnostics"] = diagnostics
    result["ready"] = bool(result["ready"] and not diagnostics["invalid_routes"])
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

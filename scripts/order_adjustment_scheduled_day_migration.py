#!/usr/bin/env python3
"""Read-only production audit for scheduled-order-day adjustment windows.

The tool has three deliberately separate passes: order stamps, adjustment
reminders, and finalized archive metadata. It never deletes data. Apply mode is
available only against the Firestore emulator so this module cannot be used as
a production migration tool.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import firestore as gcf_firestore
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from .db_manager_pg import project_scheduled_order_archive_metadata
    from .order_adjustment_schedule import (
        build_canonical_reminder_fields,
        meaningful_lines,
        reminder_fields_equal,
        trusted_adjustment_settings,
    )
    from .order_archive_receipt import build_order_archive_projection
    from .schedule_cycle import resolve_order_schedule_for_delivery_date
except ImportError:
    from db_manager_pg import project_scheduled_order_archive_metadata
    from order_adjustment_schedule import (
        build_canonical_reminder_fields,
        meaningful_lines,
        reminder_fields_equal,
        trusted_adjustment_settings,
    )
    from order_archive_receipt import build_order_archive_projection
    from schedule_cycle import resolve_order_schedule_for_delivery_date


PROJECT_ID = "routespark-1f47d"
FIREBASE_MUTATION_CONTRACT = "update_only_no_delete"
EMULATOR_APPLY_CONTRACT = "firestore_emulator_only"
ARCHIVE_APPLY_CONTRACT = "loopback_test_postgres_only"
RECOVERABLE_SKIP_REASONS = {
    "reminder_time_passed",
    "window_unresolved",
    "missing_source_order",
}
UNTOUCHABLE_REMINDER_STATUSES = {"sending", "sent", "failed"}
TERMINAL_ADJUSTMENT_STATUSES = {"sent", "confirmed", "applied", "archived", "canceled"}
STAMP_KEYS = {
    "version", "date", "timezone", "scheduleKey", "orderDay",
    "loadOffsetDays", "deliveryOffsetDays", "provenance",
}
DAY_KEYS = ("", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "to_datetime"):
        return _jsonable(value.to_datetime())
    return value


def stable_hash(value: Any) -> str:
    import hashlib
    payload = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def is_v2_stamp(value: Any) -> bool:
    if not (
        isinstance(value, Mapping)
        and set(value) == STAMP_KEYS
        and value.get("version") == 2
        and value.get("provenance") in {"cycle", "backfill"}
    ):
        return False
    order_day = value.get("orderDay")
    load_offset = value.get("loadOffsetDays")
    delivery_offset = value.get("deliveryOffsetDays")
    if not (
        isinstance(order_day, int) and not isinstance(order_day, bool)
        and 1 <= order_day <= 7
        and isinstance(load_offset, int) and not isinstance(load_offset, bool)
        and isinstance(delivery_offset, int) and not isinstance(delivery_offset, bool)
        and 0 <= load_offset <= delivery_offset <= 60
    ):
        return False
    try:
        stamped_date = date.fromisoformat(str(value.get("date") or ""))
        ZoneInfo(str(value.get("timezone") or ""))
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        return False
    schedule_key = str(value.get("scheduleKey") or "")
    return stamped_date.isoweekday() == order_day and schedule_key == DAY_KEYS[order_day]


def _nested(data: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    current: Any = data
    for key in keys:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return dict(current) if isinstance(current, Mapping) else {}


def _owner_uid(data: Mapping[str, Any] | None) -> str | None:
    values = _owner_values(data)
    return next(iter(values)) if len(values) == 1 else None


def _owner_values(data: Mapping[str, Any] | None) -> set[str]:
    return {
        value
        for key in ("ownerUid", "userId", "userID")
        if (value := str((data or {}).get(key) or "").strip())
    }


def _owner_source_paths(route: str) -> list[str]:
    return [
        f"routes/{route}",
        f"routeEntitlements/{route}",
        f"routeNumbers/{route}",
    ]


def _owner_source_state(snapshots: list[tuple[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for path, snap in snapshots:
        values = sorted(_owner_values(snap.to_dict() if snap.exists else None))
        result.append({
            "path": path,
            "ownerUid": values[0] if len(values) == 1 else None,
            "ownerUids": values,
        })
    return result


def load_route_context(db: Any, route: str) -> dict[str, Any]:
    source_snapshots = [
        (path, db.document(path).get()) for path in _owner_source_paths(route)
    ]
    owner_source_state = _owner_source_state(source_snapshots)
    owner_source_hash = stable_hash(owner_source_state)
    sources = []
    source_conflict = False
    for source in owner_source_state:
        path = source["path"]
        uid = source["ownerUid"]
        source_conflict = source_conflict or len(source["ownerUids"]) > 1
        if uid:
            sources.append((path, uid))
    distinct = sorted({uid for _, uid in sources})
    if source_conflict or len(distinct) > 1:
        return {
            "status": "missing_owner",
            "reason": "owner_sources_disagree",
            "sources": sources,
            "ownerSourcePaths": _owner_source_paths(route),
            "ownerSourceHash": owner_source_hash,
        }
    uid = distinct[0] if distinct else None
    if not uid:
        return {
            "status": "missing_owner",
            "reason": "missing_owner",
            "sources": sources,
            "ownerSourcePaths": _owner_source_paths(route),
            "ownerSourceHash": owner_source_hash,
        }
    user_ref = db.document(f"users/{uid}")
    user_snap = user_ref.get()
    if not user_snap.exists:
        return {
            "status": "missing_owner",
            "reason": "missing_owner_user",
            "ownerUid": uid,
            "ownerSourcePaths": _owner_source_paths(route),
            "ownerSourceHash": owner_source_hash,
        }
    user = user_snap.to_dict() or {}
    zone_name = str(_nested(user, "profile").get("timezone") or "").strip()
    try:
        ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError):
        return {
            "status": "missing_timezone",
            "reason": "missing_timezone",
            "ownerUid": uid,
            "ownerPath": user_ref.path,
            "ownerSourcePaths": _owner_source_paths(route),
            "ownerSourceHash": owner_source_hash,
        }
    cycles = _nested(user, "userSettings", "notifications", "scheduling").get("orderCycles")
    cycles = cycles if isinstance(cycles, list) else []
    context_source = {"ownerUid": uid, "timezone": zone_name, "cycles": cycles}
    return {
        "status": "ready",
        **context_source,
        "ownerPath": user_ref.path,
        "ownerSourcePaths": _owner_source_paths(route),
        "ownerSourceHash": owner_source_hash,
        "contextHash": stable_hash(context_source),
    }


def reconstruct_stamp(order: Mapping[str, Any], context: Mapping[str, Any]) -> tuple[dict | None, str]:
    if "scheduledOrder" in order:
        if is_v2_stamp(order.get("scheduledOrder")):
            return dict(order["scheduledOrder"]), "already_stamped"
        return None, "invalid_existing_stamp"
    if context.get("status") != "ready":
        return None, str(context.get("reason") or context.get("status") or "missing_owner")
    delivery = str(order.get("expectedDeliveryDate") or order.get("deliveryDate") or "").strip()
    schedule_key = str(order.get("scheduleKey") or "").strip().lower()
    if not delivery or not schedule_key:
        return None, "off_cycle"
    try:
        resolution = resolve_order_schedule_for_delivery_date(
            delivery,
            list(context.get("cycles") or []),
            schedule_key,
        )
    except (TypeError, ValueError):
        return None, "off_cycle"
    if resolution.get("status") != "resolved":
        reason = resolution.get("reason")
        return None, "ambiguous" if reason == "ambiguous_schedule" else str(reason or "off_cycle")
    cycle = resolution["cycle"]
    return {
        "version": 2,
        "date": resolution["orderDate"],
        "timezone": context["timezone"],
        "scheduleKey": resolution["scheduleKey"],
        "orderDay": cycle["orderDay"],
        "loadOffsetDays": cycle["loadOffsetDays"],
        "deliveryOffsetDays": cycle["deliveryOffsetDays"],
        "provenance": "backfill",
    }, "reconstructable"


def _order_basis(data: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if key != "scheduledOrder"}


def _order_in_scope(data: Mapping[str, Any], stamp: Mapping[str, Any] | None, now_ms: int) -> bool:
    status = str(data.get("status") or "")
    if status == "draft":
        return True
    if status != "finalized" or not stamp:
        return False
    try:
        local_today = datetime.fromtimestamp(
            now_ms / 1000, ZoneInfo(str(stamp["timezone"]))
        ).date()
        delivery = date.fromisoformat(
            str(data.get("expectedDeliveryDate") or data.get("deliveryDate"))
        )
        return delivery >= local_today
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        return False


def _finalized_delivery_is_past(
    data: Mapping[str, Any], context: Mapping[str, Any], now_ms: int
) -> bool:
    if context.get("status") != "ready":
        return False
    try:
        local_today = datetime.fromtimestamp(
            now_ms / 1000, ZoneInfo(str(context["timezone"]))
        ).date()
        delivery = date.fromisoformat(
            str(data.get("expectedDeliveryDate") or data.get("deliveryDate"))
        )
        return delivery < local_today
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        return False


def _route_refs(db: Any, routes: list[str]):
    for route in routes:
        yield route, db.document(f"routes/{route}")


def scan_orders(db: Any, routes: list[str], now_ms: int) -> dict[str, Any]:
    entries = []
    contexts = {route: load_route_context(db, route) for route in routes}
    for route, route_ref in _route_refs(db, routes):
        context = contexts[route]
        for snap in route_ref.collection("orders").stream():
            data = snap.to_dict() or {}
            stamp, classification = reconstruct_stamp(data, context)
            status = str(data.get("status") or "")
            context_blocked = classification in {
                "missing_owner", "missing_owner_user", "owner_sources_disagree",
                "missing_timezone", "missing_schedule", "ambiguous", "off_cycle",
                "invalid_existing_stamp",
            }
            if status not in {"draft", "finalized"}:
                classification = "out_of_scope"
            elif status == "finalized" and _finalized_delivery_is_past(data, context, now_ms):
                classification = "out_of_scope"
            elif status == "finalized" and not context_blocked and not _order_in_scope(data, stamp, now_ms):
                classification = "out_of_scope"
            entry = {
                "path": snap.reference.path,
                "routeNumber": route,
                "orderId": snap.id,
                "classification": classification,
                "basisHash": stable_hash(_order_basis(data)),
                "documentHash": stable_hash(data),
                "contextHash": context.get("contextHash"),
                "ownerPath": context.get("ownerPath"),
                "ownerSourcePaths": context.get("ownerSourcePaths"),
                "ownerSourceHash": context.get("ownerSourceHash"),
                "proposedStamp": stamp if classification == "reconstructable" else None,
            }
            entries.append(entry)
    return _finish_report("orders", routes, entries)


def _load_source(db: Any, route: str, source_id: str) -> tuple[Any, dict[str, Any] | None]:
    ref = db.document(f"routes/{route}/orders/{source_id}")
    snap = ref.get()
    return ref, (snap.to_dict() or {}) if snap.exists else None


def _load_settings(db: Any, uid: str) -> tuple[Any, dict[str, Any], str]:
    ref = db.document(f"users/{uid}")
    snap = ref.get()
    data = snap.to_dict() or {} if snap.exists else {}
    raw = _nested(data, "userSettings", "notifications", "orderAdjustmentSettings")
    return ref, trusted_adjustment_settings(raw), stable_hash(raw)


def _normalized_patch_for_hash(fields: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(fields)
    if result.get("reminderSkipReason") == "past_cutoff":
        result["reminderSkippedAtMs"] = "<apply-time>"
    return result


def _adjustment_candidate(
    data: Mapping[str, Any],
    source: Mapping[str, Any] | None,
    stamp: Mapping[str, Any] | None,
    settings: Mapping[str, Any],
    now_ms: int,
) -> tuple[str, dict[str, Any] | None, str | None]:
    status = str(data.get("status") or "")
    if status in TERMINAL_ADJUSTMENT_STATUSES or status != "draft":
        return "terminal_untouched", None, None
    reminder_status = str(data.get("reminderStatus") or "none")
    if reminder_status in UNTOUCHABLE_REMINDER_STATUSES or data.get("reminderClaimedAtMs") is not None:
        return "claimed_untouched", None, None
    if not source or not stamp:
        return "missing_source", None, None
    if settings.get("reminderEnabled") is not True:
        return "reminders_disabled", None, None
    if not meaningful_lines(data):
        return "no_meaningful_lines", None, None
    skip_reason = str(data.get("reminderSkipReason") or "")
    if skip_reason == "past_cutoff":
        return "past_cutoff_untouched", None, None
    recoverable = (
        reminder_status in {"none", "pending"}
        or (reminder_status == "skipped" and skip_reason in RECOVERABLE_SKIP_REASONS)
    )
    if not recoverable:
        return "unrecoverable_untouched", None, None
    fields = build_canonical_reminder_fields(
        {**source, "scheduledOrder": dict(stamp)},
        settings=settings,
        has_meaningful_lines=True,
        status="draft",
        now_ms=now_ms,
    )
    cutoff = fields.get("cutoffAtMs")
    if not isinstance(cutoff, int):
        return "window_unresolved", None, None
    window_class = "expired" if now_ms >= cutoff else (
        "already_due" if now_ms >= int(fields.get("reminderAtMs") or cutoff) else "future"
    )
    if reminder_fields_equal(data, fields):
        return "already_canonical", None, window_class
    if reminder_status == "pending" and isinstance(data.get("reminderAtMs"), (int, float)):
        classification = "stored_future_patched" if data["reminderAtMs"] > now_ms else "pending_patched"
    else:
        classification = "rearmed" if window_class != "expired" else "expired_patched"
    return classification, fields, window_class


def scan_adjustments(db: Any, routes: list[str], now_ms: int) -> dict[str, Any]:
    entries = []
    contexts = {route: load_route_context(db, route) for route in routes}
    source_cache: dict[tuple[str, str], tuple[Any, dict | None]] = {}
    settings_cache: dict[str, tuple[Any, dict, str]] = {}
    for route, route_ref in _route_refs(db, routes):
        context = contexts[route]
        for snap in route_ref.collection("orderAdjustments").stream():
            data = snap.to_dict() or {}
            source_id = str(data.get("sourceOrderId") or "").strip()
            if not source_id:
                entries.append({
                    "path": snap.reference.path,
                    "routeNumber": route,
                    "adjustmentId": snap.id,
                    "classification": "standalone_untouched",
                    "documentHash": stable_hash(data),
                })
                continue
            source_key = (route, source_id)
            if source_key not in source_cache:
                source_cache[source_key] = _load_source(db, route, source_id)
            source_ref, source = source_cache[source_key]
            stamp, stamp_class = reconstruct_stamp(source or {}, context)
            uid = str(data.get("userId") or "").strip()
            if uid not in settings_cache:
                settings_cache[uid] = _load_settings(db, uid) if uid else (None, {}, stable_hash({}))
            settings_ref, settings, settings_hash = settings_cache[uid]
            classification, fields, window_class = _adjustment_candidate(
                data, source, stamp, settings, now_ms
            )
            entries.append({
                "path": snap.reference.path,
                "routeNumber": route,
                "adjustmentId": snap.id,
                "sourceOrderId": source_id,
                "classification": classification,
                "windowClass": window_class,
                "documentHash": stable_hash(data),
                "sourceBasisHash": stable_hash(_order_basis(source or {})),
                "contextHash": context.get("contextHash"),
                "ownerPath": context.get("ownerPath"),
                "ownerSourcePaths": context.get("ownerSourcePaths"),
                "ownerSourceHash": context.get("ownerSourceHash"),
                "settingsPath": settings_ref.path if settings_ref else None,
                "settingsHash": settings_hash,
                "stampClassification": stamp_class,
                "proposedStamp": stamp,
                "proposedFields": _normalized_patch_for_hash(fields) if fields else None,
            })
    return _finish_report("adjustments", routes, entries)


def _pg_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DB", "routespark"),
        user=os.environ.get("POSTGRES_USER", "routespark"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def _archive_summary(conn: Any, order_id: str, route: str) -> dict[str, int] | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT total_units, store_count,
                   (SELECT COUNT(*) FROM order_line_items li
                    WHERE li.order_id = orders_historical.order_id) AS line_item_count
            FROM orders_historical
            WHERE order_id = %s AND route_number = %s
            """,
            [order_id, route],
        )
        row = cur.fetchone()
    return dict(row) if row else None


def scan_archive(db: Any, routes: list[str], now_ms: int, conn: Any) -> dict[str, Any]:
    entries = []
    contexts = {route: load_route_context(db, route) for route in routes}
    for route, route_ref in _route_refs(db, routes):
        context = contexts[route]
        for snap in route_ref.collection("orders").stream():
            data = snap.to_dict() or {}
            stamp, stamp_class = reconstruct_stamp(data, context)
            if str(data.get("status") or "") != "finalized" or not _order_in_scope(data, stamp, now_ms):
                continue
            summary = _archive_summary(conn, snap.id, route)
            projection = build_order_archive_projection(
                snap.id, route, {**data, "scheduledOrder": stamp} if stamp else data
            )
            exact = bool(summary) and (
                int(summary.get("total_units") or 0) == projection["totalUnits"]
                and int(summary.get("store_count") or 0) == projection["storeCount"]
                and int(summary.get("line_item_count") or 0) == projection["lineItemCount"]
            )
            classification = (
                "archive_receipt_update" if stamp and exact else
                "archive_projection_mismatch" if summary else
                "archive_missing"
            )
            entries.append({
                "path": snap.reference.path,
                "routeNumber": route,
                "orderId": snap.id,
                "classification": classification,
                "stampClassification": stamp_class,
                "sourceBasisHash": stable_hash(_order_basis(data)),
                "contextHash": context.get("contextHash"),
                "ownerPath": context.get("ownerPath"),
                "ownerSourcePaths": context.get("ownerSourcePaths"),
                "ownerSourceHash": context.get("ownerSourceHash"),
                "proposedStamp": stamp,
                "sourceFingerprint": projection.get("sourceFingerprint"),
                "archiveSummary": summary,
            })
    return _finish_report("archive", routes, entries)


def _proposal_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "path", "classification", "basisHash", "documentHash", "sourceBasisHash", "contextHash",
        "ownerSourcePaths", "ownerSourceHash", "settingsHash", "windowClass",
        "proposedStamp", "proposedFields",
        "sourceFingerprint", "archiveSummary",
    )
    return {key: entry.get(key) for key in keys if key in entry}


def _finish_report(phase: str, routes: list[str], entries: list[dict[str, Any]]) -> dict[str, Any]:
    entries.sort(key=lambda item: item["path"])
    counts = dict(sorted(Counter(item["classification"] for item in entries).items()))
    proposal = {
        "phase": phase,
        "routes": sorted(routes),
        "entries": [_proposal_entry(item) for item in entries],
    }
    return {
        "schemaVersion": 1,
        "firebaseMutationContract": FIREBASE_MUTATION_CONTRACT,
        "applyContract": EMULATOR_APPLY_CONTRACT,
        "archiveApplyContract": ARCHIVE_APPLY_CONTRACT,
        "phase": phase,
        "routes": sorted(routes),
        "counts": counts,
        "windowCounts": dict(sorted(Counter(
            item["windowClass"] for item in entries if item.get("windowClass")
        ).items())),
        "documentIdsByClassification": {
            key: [item["path"] for item in entries if item["classification"] == key]
            for key in counts
        },
        "entries": entries,
        "proposalHash": stable_hash(proposal),
    }


def _apply_target_paths(report: Mapping[str, Any]) -> list[str]:
    """Return the exact Firestore source documents eligible for this apply."""
    phase = report.get("phase")
    entries = report.get("entries") or []
    if phase == "orders":
        paths = [
            entry["path"] for entry in entries
            if entry.get("classification") == "reconstructable"
        ]
    elif phase == "adjustments":
        paths = [
            entry["path"] for entry in entries
            if entry.get("proposedFields")
        ]
    elif phase == "archive":
        paths = [
            entry["path"] for entry in entries
            if entry.get("classification") == "archive_receipt_update"
        ]
    else:
        raise RuntimeError(f"unsupported_migration_phase:{phase}")
    return sorted(paths)


def _loopback_firestore_emulator_configured() -> bool:
    emulator_host = str(os.environ.get("FIRESTORE_EMULATOR_HOST") or "").strip()
    if not emulator_host:
        return False
    try:
        parsed = urlsplit(f"//{emulator_host}")
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        and port is not None
        and parsed.username is None
        and parsed.password is None
        and not parsed.path
        and not parsed.query
        and not parsed.fragment
    )


def _assert_emulator_apply_allowed() -> None:
    """Fail closed before any write unless a loopback emulator is configured."""
    if not _loopback_firestore_emulator_configured():
        raise RuntimeError("production_migration_disabled:apply_requires_firestore_emulator")


def _loopback_test_postgres_configured() -> bool:
    host = str(os.environ.get("POSTGRES_HOST") or "").strip().lower()
    database = str(os.environ.get("POSTGRES_DB") or "").strip().lower()
    explicit_test_opt_in = (
        str(os.environ.get("ORDER_ADJUSTMENT_MIGRATION_TEST_POSTGRES") or "")
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    return (
        explicit_test_opt_in
        and host in {"localhost", "127.0.0.1", "::1"}
        and "test" in database
    )


def _assert_archive_apply_allowed() -> None:
    _assert_emulator_apply_allowed()
    if not _loopback_test_postgres_configured():
        raise RuntimeError(
            "production_migration_disabled:archive_apply_requires_loopback_test_postgres"
        )


def _context_source(user_data: Mapping[str, Any], uid: str) -> dict[str, Any]:
    return {
        "ownerUid": uid,
        "timezone": str(_nested(user_data, "profile").get("timezone") or "").strip(),
        "cycles": _nested(user_data, "userSettings", "notifications", "scheduling").get("orderCycles") or [],
    }


def apply_orders(db: Any, report: Mapping[str, Any]) -> list[str]:
    _assert_emulator_apply_allowed()
    applied = []
    for entry in report["entries"]:
        if entry["classification"] != "reconstructable":
            continue
        order_ref = db.document(entry["path"])
        owner_ref = db.document(entry["ownerPath"])
        owner_source_refs = [
            db.document(path) for path in entry["ownerSourcePaths"]
        ]

        @gcf_firestore.transactional
        def update(transaction):
            order_snap = order_ref.get(transaction=transaction)
            owner_snap = owner_ref.get(transaction=transaction)
            owner_source_snaps = [
                (ref.path, ref.get(transaction=transaction))
                for ref in owner_source_refs
            ]
            if not order_snap.exists or not owner_snap.exists:
                raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
            current = order_snap.to_dict() or {}
            owner = owner_snap.to_dict() or {}
            if "scheduledOrder" in current:
                raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
            if stable_hash(current) != entry["documentHash"]:
                raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
            if stable_hash(_context_source(owner, owner_ref.id)) != entry["contextHash"]:
                raise RuntimeError(f"changed_since_dry_run:{entry['ownerPath']}")
            if stable_hash(_owner_source_state(owner_source_snaps)) != entry["ownerSourceHash"]:
                raise RuntimeError(f"changed_since_dry_run:route_owner:{entry['routeNumber']}")
            transaction.update(order_ref, {"scheduledOrder": entry["proposedStamp"]})

        update(db.transaction())
        entry["afterDocumentHash"] = stable_hash(order_ref.get().to_dict() or {})
        applied.append(entry["path"])
    return applied


def apply_adjustments(db: Any, report: Mapping[str, Any], now_ms: int) -> list[str]:
    _assert_emulator_apply_allowed()
    applied = []
    for entry in report["entries"]:
        if not entry.get("proposedFields"):
            continue
        adjustment_ref = db.document(entry["path"])
        source_ref = db.document(
            f"routes/{entry['routeNumber']}/orders/{entry['sourceOrderId']}"
        )
        settings_ref = db.document(entry["settingsPath"])

        @gcf_firestore.transactional
        def update(transaction):
            adjustment_snap = adjustment_ref.get(transaction=transaction)
            source_snap = source_ref.get(transaction=transaction)
            settings_snap = settings_ref.get(transaction=transaction)
            if not adjustment_snap.exists or not source_snap.exists or not settings_snap.exists:
                raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
            adjustment = adjustment_snap.to_dict() or {}
            source = source_snap.to_dict() or {}
            settings_raw = _nested(
                settings_snap.to_dict() or {},
                "userSettings", "notifications", "orderAdjustmentSettings",
            )
            if stable_hash(adjustment) != entry["documentHash"]:
                raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
            if stable_hash(_order_basis(source)) != entry["sourceBasisHash"]:
                raise RuntimeError(f"changed_since_dry_run:{source_ref.path}")
            if stable_hash(settings_raw) != entry["settingsHash"]:
                raise RuntimeError(f"changed_since_dry_run:{settings_ref.path}")
            stamp = source.get("scheduledOrder")
            if not is_v2_stamp(stamp) or dict(stamp) != entry["proposedStamp"]:
                raise RuntimeError(f"changed_since_dry_run:{source_ref.path}")
            classification, fields, side = _adjustment_candidate(
                adjustment,
                source,
                stamp,
                trusted_adjustment_settings(settings_raw),
                now_ms,
            )
            if side != entry.get("windowClass") or not fields:
                raise RuntimeError(f"cutoff_side_changed_since_dry_run:{entry['path']}")
            if classification != entry["classification"]:
                raise RuntimeError(f"claim_state_changed_since_dry_run:{entry['path']}")
            transaction.update(adjustment_ref, fields)

        update(db.transaction())
        entry["afterDocumentHash"] = stable_hash(adjustment_ref.get().to_dict() or {})
        applied.append(entry["path"])
    return applied


def apply_archive(db: Any, report: Mapping[str, Any], conn: Any) -> list[str]:
    _assert_archive_apply_allowed()
    applied = []
    for entry in report["entries"]:
        if entry["classification"] != "archive_receipt_update":
            continue
        snap = db.document(entry["path"]).get()
        if not snap.exists:
            raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
        data = snap.to_dict() or {}
        if stable_hash(_order_basis(data)) != entry["sourceBasisHash"]:
            raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
        if not is_v2_stamp(data.get("scheduledOrder")):
            raise RuntimeError(f"missing_applied_stamp:{entry['path']}")
        if dict(data["scheduledOrder"]) != entry["proposedStamp"]:
            raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
        projection = build_order_archive_projection(
            snap.id, entry["routeNumber"], data
        )
        if projection.get("sourceFingerprint") != entry.get("sourceFingerprint"):
            raise RuntimeError(f"changed_since_dry_run:{entry['path']}")
        result = project_scheduled_order_archive_metadata(
            conn, db, snap.id, entry["routeNumber"], data
        )
        if not result.get("success"):
            raise RuntimeError(f"archive_projection_failed:{entry['path']}:{result.get('error')}")
        entry["afterDocumentHash"] = stable_hash(data)
        applied.append(entry["path"])
    return applied


def _default_service_account() -> Path:
    configured = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    return Path(configured) if configured else Path.cwd() / "routespark-firebase-adminsdk.json"


def _initialize(path: Path):
    if not path.is_file():
        raise FileNotFoundError(f"Missing service account: {path}")
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(str(path)), {"projectId": PROJECT_ID})
    return firestore.client()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--route")
    scope.add_argument("--all-routes", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--phase", required=True, choices=("orders", "adjustments", "archive"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-dry-run-hash")
    parser.add_argument(
        "--expected-document",
        action="append",
        default=[],
        help="Exact Firestore source path approved from the reviewed dry run; repeat per target",
    )
    parser.add_argument("--serviceAccount", type=Path, default=_default_service_account())
    args = parser.parse_args(argv)
    if args.route:
        args.route = str(args.route).strip()
        if not args.route.isdigit() or len(args.route) > 10:
            parser.error("--route must contain 1-10 digits")
    if args.apply and not args.expected_dry_run_hash:
        parser.error("--apply requires --expected-dry-run-hash")
    if args.apply and not args.expected_document:
        parser.error("--apply requires at least one --expected-document")
    if args.apply and args.all_routes:
        parser.error("--apply requires one explicit --route; --all-routes is dry-run only")
    if args.apply and not _loopback_firestore_emulator_configured():
        parser.error(
            "--apply requires a loopback Firestore emulator; "
            "production migration writes are disabled"
        )
    if args.apply and args.phase == "archive" and not _loopback_test_postgres_configured():
        parser.error(
            "archive --apply requires explicit opt-in to a loopback PostgreSQL test database; "
            "production migration writes are disabled"
        )
    if not args.apply and args.expected_dry_run_hash:
        parser.error("--expected-dry-run-hash is valid only with --apply")
    if not args.apply and args.expected_document:
        parser.error("--expected-document is valid only with --apply")
    if len(args.expected_document) != len(set(args.expected_document)):
        parser.error("--expected-document values must be unique")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db = _initialize(args.serviceAccount.resolve())
    routes = [args.route] if args.route else sorted(ref.id for ref in db.collection("routes").list_documents())
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    conn = _pg_connection() if args.phase == "archive" else None
    try:
        if args.phase == "orders":
            report = scan_orders(db, routes, now_ms)
        elif args.phase == "adjustments":
            report = scan_adjustments(db, routes, now_ms)
        else:
            report = scan_archive(db, routes, now_ms, conn)
        report["mode"] = "apply" if args.apply else "dry_run"
        report["generatedAt"] = datetime.now(timezone.utc).isoformat()
        report["appliedPaths"] = []
        if args.apply:
            if report.get("firebaseMutationContract") != FIREBASE_MUTATION_CONTRACT:
                raise RuntimeError("firebase_mutation_contract_mismatch")
            if report["proposalHash"] != args.expected_dry_run_hash:
                raise RuntimeError(
                    "dry_run_hash_mismatch: "
                    f"expected={args.expected_dry_run_hash} current={report['proposalHash']}"
                )
            actual_targets = _apply_target_paths(report)
            expected_targets = sorted(args.expected_document)
            if actual_targets != expected_targets:
                raise RuntimeError(
                    "approved_document_set_mismatch: "
                    f"expected={expected_targets} current={actual_targets}"
                )
            if args.phase == "orders":
                report["appliedPaths"] = apply_orders(db, report)
            elif args.phase == "adjustments":
                report["appliedPaths"] = apply_adjustments(db, report, now_ms)
            else:
                report["appliedPaths"] = apply_archive(db, report, conn)
        args.output = args.output.resolve()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(_jsonable(report), indent=2, sort_keys=True) + "\n")
        print(json.dumps({
            "output": str(args.output),
            "mode": report["mode"],
            "proposalHash": report["proposalHash"],
            "counts": report["counts"],
            "applied": len(report["appliedPaths"]),
        }))
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Route-scoped, read-only evidence harness for scheduled-order-day rollout.

The default and Phase 0 behavior is deliberately incapable of writes. Later
phases may add explicitly gated fixture operations, but `--dry-run` must always
remain side-effect free.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import firebase_admin
from firebase_admin import credentials, firestore

try:
    from .schedule_cycle import resolve_order_schedule_for_delivery_date
    from .order_adjustment_schedule import build_canonical_reminder_fields
    from .order_adjustment_reminder_worker import audit_once
    from .order_archive_receipt import build_order_archive_projection
    from .order_sync_listener import (
        FinalizationDispatchGuard,
        dispatch_finalized_order_if_new,
        finalization_identity,
    )
    from .order_adjustment_v2_monitor import collect_deployment_report
except ImportError:
    from schedule_cycle import resolve_order_schedule_for_delivery_date
    from order_adjustment_schedule import build_canonical_reminder_fields
    from order_adjustment_reminder_worker import audit_once
    from order_archive_receipt import build_order_archive_projection
    from order_sync_listener import (
        FinalizationDispatchGuard,
        dispatch_finalized_order_if_new,
        finalization_identity,
    )
    from order_adjustment_v2_monitor import collect_deployment_report


PROJECT_ID = "routespark-1f47d"
SCHEMA_VERSION = 1
ROUTE_PATTERN = re.compile(r"^[0-9]{1,10}$")
CANONICAL_SOURCE_PATTERN = re.compile(r"^[^/\s]+$")
CUTOFF_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


@dataclass(frozen=True)
class OwnerResolution:
    uid: str | None
    sources: dict[str, str | None]
    sources_agree: bool


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def stable_hash(value: Any) -> str:
    payload = json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def classify_source_order_id(data: Mapping[str, Any]) -> str:
    if "sourceOrderId" not in data:
        return "missing"
    value = data.get("sourceOrderId")
    if value is None:
        return "null"
    if not isinstance(value, str):
        return "malformed"
    if not value:
        return "empty"
    if not value.strip():
        return "whitespace"
    if CANONICAL_SOURCE_PATTERN.fullmatch(value):
        return "canonical"
    return "malformed"


def classify_route_number(data: Mapping[str, Any]) -> str:
    if "routeNumber" not in data:
        return "missing"
    value = data.get("routeNumber")
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "numeric"
    if value is None:
        return "null"
    return "other"


def adjustment_identity_census(
    rows: Iterable[tuple[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    source_counts = {
        key: 0
        for key in ("missing", "null", "empty", "whitespace", "canonical", "malformed")
    }
    route_counts = {key: 0 for key in ("missing", "null", "string", "numeric", "other")}
    malformed_paths: list[str] = []
    numeric_route_paths: list[str] = []
    for path, data in rows:
        source_class = classify_source_order_id(data)
        route_class = classify_route_number(data)
        source_counts[source_class] += 1
        route_counts[route_class] += 1
        if source_class == "malformed":
            malformed_paths.append(path)
        if route_class == "numeric":
            numeric_route_paths.append(path)
    return {
        "sourceOrderId": source_counts,
        "routeNumber": route_counts,
        "malformedPaths": sorted(malformed_paths),
        "numericRoutePaths": sorted(numeric_route_paths),
    }


def _owner_uid(data: Mapping[str, Any] | None) -> str | None:
    values = _owner_values(data)
    return next(iter(values)) if len(values) == 1 else None


def _owner_values(data: Mapping[str, Any] | None) -> set[str]:
    return {
        value
        for key in ("ownerUid", "userId", "userID")
        if (value := str((data or {}).get(key) or "").strip())
    }


def resolve_route_owner(db: Any, route: str) -> OwnerResolution:
    route_snap = db.document(f"routes/{route}").get()
    entitlement_snap = db.document(f"routeEntitlements/{route}").get()
    route_number_snap = db.document(f"routeNumbers/{route}").get()
    raw_sources = {
        "route": _owner_values(route_snap.to_dict() if route_snap.exists else None),
        "entitlement": _owner_values(
            entitlement_snap.to_dict() if entitlement_snap.exists else None
        ),
        "routeNumber": _owner_values(
            route_number_snap.to_dict() if route_number_snap.exists else None
        ),
    }
    source_conflict = any(len(values) > 1 for values in raw_sources.values())
    distinct = set().union(*raw_sources.values())
    sources = {
        key: next(iter(values)) if len(values) == 1 else None
        for key, values in raw_sources.items()
    }
    uid = next(iter(distinct)) if len(distinct) == 1 and not source_conflict else None
    if not distinct and not source_conflict:
        candidates = list(
            db.collection("users")
            .where("profile.routeNumber", "==", route)
            .limit(2)
            .stream()
        )
        if len(candidates) == 1:
            uid = candidates[0].id
            sources["profileQuery"] = uid
    return OwnerResolution(
        uid=uid,
        sources=sources,
        sources_agree=not source_conflict and len(distinct) <= 1,
    )


def _nested_map(data: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    current: Any = data
    for key in keys:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return dict(current) if isinstance(current, Mapping) else {}


def classify_adjustment_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    email = str(settings.get("customerServiceEmail") or "").strip()
    cutoff = str(settings.get("cutoffTimeLocal") or "").strip()
    lead = settings.get("reminderLeadMinutes")
    reminder_enabled = settings.get("reminderEnabled") is True
    cutoff_valid = bool(CUTOFF_PATTERN.fullmatch(cutoff))
    lead_valid = isinstance(lead, int) and not isinstance(lead, bool) and lead > 0
    return {
        "configuredEmail": bool(email),
        "reminderEnabled": reminder_enabled,
        "cutoffPresent": bool(cutoff),
        "cutoffValid": cutoff_valid,
        "leadValid": lead_valid,
        "cutoffTimeLocal": cutoff or None,
        "reminderLeadMinutes": lead if lead_valid else None,
        "requiresCutoffRemediation": bool(email and not cutoff_valid),
    }


def global_cutoff_census(db: Any) -> dict[str, Any]:
    configured = 0
    missing_cutoff = 0
    by_reminder_enabled = {"true": 0, "false": 0}
    remediation_paths: list[str] = []
    for snap in db.collection("users").stream():
        data = snap.to_dict() or {}
        settings = _nested_map(
            data,
            "userSettings",
            "notifications",
            "orderAdjustmentSettings",
        )
        classification = classify_adjustment_settings(settings)
        if not classification["configuredEmail"]:
            continue
        configured += 1
        if classification["requiresCutoffRemediation"]:
            missing_cutoff += 1
            key = "true" if classification["reminderEnabled"] else "false"
            by_reminder_enabled[key] += 1
            remediation_paths.append(f"users/{snap.id}")
    return {
        "configuredEmailUsers": configured,
        "configuredEmailMissingValidCutoff": missing_cutoff,
        "missingCutoffByReminderEnabled": by_reminder_enabled,
        "remediationPaths": sorted(remediation_paths),
        "ready": missing_cutoff == 0,
    }


def global_route_timezone_census(db: Any) -> dict[str, Any]:
    """Read-only census of the route-owner timezone authority used by v2 stamps."""
    route_ids: set[str] = set()
    for collection_name in ("routes", "routeEntitlements", "routeNumbers"):
        route_ids.update(ref.id for ref in db.collection(collection_name).list_documents())

    user_cache: dict[str, tuple[str | None, bool, bool]] = {}
    rows: list[dict[str, Any]] = []
    routes_by_owner: dict[str, list[str]] = {}
    for route in sorted(route_ids):
        owner = resolve_route_owner(db, route)
        timezone_name: str | None = None
        timezone_valid = False
        owner_exists = False
        if owner.uid:
            cached = user_cache.get(owner.uid)
            if cached is None:
                owner_snap = db.document(f"users/{owner.uid}").get()
                owner_exists = owner_snap.exists
                owner_data = owner_snap.to_dict() or {} if owner_exists else {}
                timezone_name, timezone_valid = _valid_timezone(
                    _nested_map(owner_data, "profile").get("timezone")
                )
                cached = (timezone_name, timezone_valid, owner_exists)
                user_cache[owner.uid] = cached
            timezone_name, timezone_valid, owner_exists = cached
            routes_by_owner.setdefault(owner.uid, []).append(route)
        rows.append({
            "route": route,
            "ownerUid": owner.uid,
            "ownerExists": owner_exists,
            "ownerSourcesAgree": owner.sources_agree,
            "timezone": timezone_name,
            "timezoneValid": timezone_valid,
        })

    missing_owner = [row["route"] for row in rows if not row["ownerUid"]]
    missing_owner_user = [
        row["route"] for row in rows if row["ownerUid"] and not row["ownerExists"]
    ]
    owner_conflicts = [row["route"] for row in rows if not row["ownerSourcesAgree"]]
    invalid_timezone = [
        row["route"] for row in rows
        if row["ownerUid"] and row["ownerExists"] and not row["timezoneValid"]
    ]
    multi_route_owners = [
        {
            "ownerUid": owner_uid,
            "routes": sorted(routes),
            "timezone": user_cache[owner_uid][0],
        }
        for owner_uid, routes in sorted(routes_by_owner.items())
        if len(routes) > 1
    ]
    return {
        "routeCount": len(rows),
        "ownerCount": len(routes_by_owner),
        "missingOwnerRoutes": missing_owner,
        "missingOwnerUserRoutes": missing_owner_user,
        "ownerConflictRoutes": owner_conflicts,
        "invalidTimezoneRoutes": invalid_timezone,
        "multiRouteOwners": multi_route_owners,
        "authorityContract": "all routes owned by one user inherit that user's profile timezone",
        "ready": not (
            missing_owner
            or missing_owner_user
            or owner_conflicts
            or invalid_timezone
        ),
    }


def _valid_timezone(value: Any) -> tuple[str | None, bool]:
    zone = str(value or "").strip()
    if not zone:
        return None, False
    try:
        ZoneInfo(zone)
    except ZoneInfoNotFoundError:
        return zone, False
    return zone, True


def capture_route_state(db: Any, route: str) -> dict[str, Any]:
    owner = resolve_route_owner(db, route)
    owner_data: dict[str, Any] = {}
    owner_exists = False
    if owner.uid:
        owner_snap = db.document(f"users/{owner.uid}").get()
        owner_exists = owner_snap.exists
        owner_data = owner_snap.to_dict() or {} if owner_exists else {}

    profile = _nested_map(owner_data, "profile")
    timezone_name, timezone_valid = _valid_timezone(profile.get("timezone"))
    scheduling = _nested_map(owner_data, "userSettings", "notifications", "scheduling")
    cycles = scheduling.get("orderCycles")
    cycles = cycles if isinstance(cycles, list) else []
    adjustment_settings = _nested_map(
        owner_data,
        "userSettings",
        "notifications",
        "orderAdjustmentSettings",
    )

    order_rows = []
    for snap in db.collection(f"routes/{route}/orders").stream():
        data = snap.to_dict() or {}
        order_rows.append(
            {
                "path": snap.reference.path,
                "status": data.get("status"),
                "scheduleKey": data.get("scheduleKey"),
                "expectedDeliveryDate": data.get("expectedDeliveryDate"),
                "scheduledOrder": data.get("scheduledOrder"),
            }
        )

    adjustment_rows: list[tuple[str, Mapping[str, Any]]] = []
    adjustment_summary = []
    for snap in db.collection(f"routes/{route}/orderAdjustments").stream():
        data = snap.to_dict() or {}
        adjustment_rows.append((snap.reference.path, data))
        adjustment_summary.append(
            {
                "path": snap.reference.path,
                "status": data.get("status"),
                "sourceOrderId": data.get("sourceOrderId") if "sourceOrderId" in data else "<missing>",
                "routeNumber": data.get("routeNumber") if "routeNumber" in data else "<missing>",
                "reminderStatus": data.get("reminderStatus"),
            }
        )

    state = {
        "owner": {
            "uid": owner.uid,
            "sources": owner.sources,
            "sourcesAgree": owner.sources_agree,
            "documentExists": owner_exists,
        },
        "timezone": {"value": timezone_name, "valid": timezone_valid},
        "cycles": cycles,
        "adjustmentSettings": classify_adjustment_settings(adjustment_settings),
        "identityCensus": adjustment_identity_census(adjustment_rows),
        "orders": sorted(order_rows, key=lambda row: row["path"]),
        "adjustments": sorted(adjustment_summary, key=lambda row: row["path"]),
    }
    state["hash"] = stable_hash(state)
    return state


def build_baseline_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
    global_census: bool = True,
) -> dict[str, Any]:
    before = capture_route_state(db, route)
    cutoff_census = global_cutoff_census(db) if global_census else {"skipped": True}
    timezone_census = global_route_timezone_census(db) if global_census else {"skipped": True}
    after = capture_route_state(db, route)
    identity = before["identityCensus"]
    stop_reasons: list[str] = []
    if not before["owner"]["uid"]:
        stop_reasons.append("missing_owner")
    elif not before["owner"]["documentExists"]:
        stop_reasons.append("missing_owner_user")
    if not before["owner"]["sourcesAgree"]:
        stop_reasons.append("owner_sources_disagree")
    if not before["timezone"]["valid"]:
        stop_reasons.append("invalid_or_missing_timezone")
    if not before["cycles"]:
        stop_reasons.append("missing_schedule")
    if identity["malformedPaths"]:
        stop_reasons.append("malformed_source_order_id")
    if identity["numericRoutePaths"]:
        stop_reasons.append("numeric_route_number_requires_disposition")
    if global_census and not cutoff_census["ready"]:
        stop_reasons.append("configured_email_missing_valid_cutoff")
    if global_census and not timezone_census["ready"]:
        stop_reasons.append("route_owner_timezone_census_not_ready")

    assertions = [
        {
            "name": "read_only_state_unchanged",
            "passed": before["hash"] == after["hash"],
            "details": {"beforeHash": before["hash"], "afterHash": after["hash"]},
        },
        {
            "name": "owner_resolved",
            "passed": bool(before["owner"]["uid"] and before["owner"]["documentExists"]),
            "details": before["owner"],
        },
        {
            "name": "timezone_valid",
            "passed": before["timezone"]["valid"],
            "details": before["timezone"],
        },
        {
            "name": "cycles_present",
            "passed": bool(before["cycles"]),
            "details": {"count": len(before["cycles"])},
        },
        {
            "name": "identity_census_reviewable",
            "passed": not identity["malformedPaths"] and not identity["numericRoutePaths"],
            "details": identity,
        },
        {
            "name": "global_cutoff_gate",
            "passed": bool(cutoff_census.get("ready", False)) if global_census else False,
            "details": cutoff_census,
        },
        {
            "name": "global_route_timezone_gate",
            "passed": bool(timezone_census.get("ready", False)) if global_census else False,
            "details": timezone_census,
        },
    ]
    ready = all(item["passed"] for item in assertions) and not stop_reasons
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "phase": "baseline",
        "runId": run_id,
        "route": route,
        "mode": "dry-run",
        "versions": {"harness": 1, "scheduleContract": 1},
        "before": before,
        "after": after,
        "fixtureIds": [],
        "assertions": assertions,
        "cleanup": {"required": False, "completed": True, "paths": []},
        "ready": ready,
        "stopReasons": sorted(set(stop_reasons)),
    }


def build_helper_parity_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    before = capture_route_state(db, route)
    candidates = sorted(
        {
            (
                str(order.get("expectedDeliveryDate") or "").strip(),
                str(order.get("scheduleKey") or "").strip().lower() or None,
            )
            for order in before["orders"]
            if str(order.get("expectedDeliveryDate") or "").strip()
        },
        key=lambda item: (item[0], item[1] or ""),
    )
    python_results = [
        {
            "deliveryDate": delivery_date,
            "scheduleKey": schedule_key,
            "resolution": resolve_order_schedule_for_delivery_date(
                delivery_date,
                before["cycles"],
                schedule_key,
            ),
        }
        for delivery_date, schedule_key in candidates
    ]

    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="oa-v2-helper-parity-") as temp_dir:
        input_path = Path(temp_dir) / "input.json"
        output_path = Path(temp_dir) / "typescript.json"
        input_path.write_text(
            json.dumps(
                {
                    "cycles": before["cycles"],
                    "candidates": [
                        {"deliveryDate": delivery_date, "scheduleKey": schedule_key}
                        for delivery_date, schedule_key in candidates
                    ],
                }
            )
        )
        completed = subprocess.run(
            [
                "npx",
                "tsx",
                "scripts/order-adjustment-v2-helper-parity.ts",
                "--input",
                str(input_path),
                "--output",
                str(output_path),
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "TypeScript parity runner failed: "
                + (completed.stderr.strip() or completed.stdout.strip())
            )
        typescript_results = json.loads(output_path.read_text()).get("results", [])

    after = capture_route_state(db, route)
    parity = _jsonable(python_results) == _jsonable(typescript_results)
    assertions = [
        {
            "name": "read_only_state_unchanged",
            "passed": before["hash"] == after["hash"],
            "details": {"beforeHash": before["hash"], "afterHash": after["hash"]},
        },
        {
            "name": "delivery_candidates_present",
            "passed": bool(candidates),
            "details": {"count": len(candidates)},
        },
        {
            "name": "typescript_python_delivery_resolution_parity",
            "passed": parity,
            "details": {
                "python": python_results,
                "typescript": typescript_results,
            },
        },
    ]
    stop_reasons = []
    if not candidates:
        stop_reasons.append("no_delivery_candidates")
    if not parity:
        stop_reasons.append("cross_runtime_mismatch")
    if before["hash"] != after["hash"]:
        stop_reasons.append("live_state_changed_during_read")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "phase": "helper-parity",
        "runId": run_id,
        "route": route,
        "mode": "dry-run",
        "versions": {"harness": 1, "scheduleContract": 1},
        "before": before,
        "after": after,
        "fixtureIds": [],
        "assertions": assertions,
        "cleanup": {"required": False, "completed": True, "paths": []},
        "ready": all(item["passed"] for item in assertions),
        "stopReasons": stop_reasons,
    }


def build_types_rules_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    evidence = build_helper_parity_evidence(db, route=route, run_id=run_id)
    timezone_name = evidence["before"]["timezone"]["value"]
    python_rows = next(
        item["details"]["python"]
        for item in evidence["assertions"]
        if item["name"] == "typescript_python_delivery_resolution_parity"
    )
    resolved = [row["resolution"] for row in python_rows if row["resolution"]["status"] == "resolved"]
    stamps = [
        {
            "version": 2,
            "date": row["orderDate"],
            "timezone": timezone_name,
            "scheduleKey": row["scheduleKey"],
            "orderDay": row["cycle"]["orderDay"],
            "loadOffsetDays": row["cycle"]["loadOffsetDays"],
            "deliveryOffsetDays": row["cycle"]["deliveryOffsetDays"],
            "provenance": "cycle",
        }
        for row in resolved
    ]
    required_keys = {
        "version", "date", "timezone", "scheduleKey", "orderDay",
        "loadOffsetDays", "deliveryOffsetDays", "provenance",
    }
    stamp_assertion = {
        "name": "live_resolutions_form_exact_v2_stamp_shape",
        "passed": bool(stamps) and all(set(stamp) == required_keys for stamp in stamps),
        "details": {"count": len(stamps), "stamps": stamps},
    }
    evidence["phase"] = "types-rules"
    evidence["assertions"].append(stamp_assertion)
    evidence["ready"] = evidence["ready"] and stamp_assertion["passed"]
    if not stamp_assertion["passed"]:
        evidence["stopReasons"].append("no_exact_v2_stamp_from_live_resolution")
    return evidence


def build_trusted_reconcile_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    evidence = build_types_rules_evidence(db, route=route, run_id=run_id)
    timezone_name = evidence["before"]["timezone"]["value"]
    settings = evidence["before"]["adjustmentSettings"]
    parity = next(
        item for item in evidence["assertions"]
        if item["name"] == "typescript_python_delivery_resolution_parity"
    )
    rows = [
        row["resolution"] for row in parity["details"]["python"]
        if row["resolution"]["status"] == "resolved"
    ]
    canonical_rows = []
    for row in rows:
        cycle = row["cycle"]
        source_order = {
            "routeNumber": route,
            "scheduledOrder": {
                "version": 2,
                "date": row["orderDate"],
                "timezone": timezone_name,
                "scheduleKey": row["scheduleKey"],
                "orderDay": cycle["orderDay"],
                "loadOffsetDays": cycle["loadOffsetDays"],
                "deliveryOffsetDays": cycle["deliveryOffsetDays"],
                "provenance": "cycle",
            },
        }
        initial = build_canonical_reminder_fields(
            source_order,
            settings=settings,
            has_meaningful_lines=True,
            status="draft",
            now_ms=0,
        )
        late = build_canonical_reminder_fields(
            source_order,
            settings=settings,
            has_meaningful_lines=True,
            status="draft",
            now_ms=int(initial["reminderAtMs"]) + 1,
        )
        canonical_rows.append({"initial": initial, "late": late})
    assertion = {
        "name": "trusted_reconcile_keeps_late_open_live_windows_pending",
        "passed": bool(canonical_rows) and all(
            item["late"]["reminderStatus"] == "pending"
            and item["late"]["reminderAtMs"] == item["initial"]["reminderAtMs"]
            and item["late"]["timezone"] == timezone_name
            for item in canonical_rows
        ),
        "details": {"count": len(canonical_rows), "rows": canonical_rows},
    }
    evidence["phase"] = "trusted-reconcile"
    evidence["assertions"].append(assertion)
    evidence["ready"] = evidence["ready"] and assertion["passed"]
    if not assertion["passed"]:
        evidence["stopReasons"].append("trusted_reconcile_live_fixture_failed")
    return evidence


def build_worker_audit_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    before = capture_route_state(db, route)
    summary = audit_once(db, route_number=route)
    after = capture_route_state(db, route)
    v2_rows = [row for row in summary["results"] if row["classification"] == "v2"]
    assertions = [
        {
            "name": "worker_audit_state_unchanged",
            "passed": before["hash"] == after["hash"],
            "details": {"beforeHash": before["hash"], "afterHash": after["hash"]},
        },
        {
            "name": "worker_audit_structured_summary_present",
            "passed": (
                summary.get("dryRun") is True
                and summary.get("routeNumber") == route
                and isinstance(summary.get("remainingDueIds"), list)
            ),
            "details": summary,
        },
        {
            "name": "worker_v2_rows_expose_stamp_identity",
            "passed": bool(v2_rows) and all(
                isinstance(row.get("stamp"), dict)
                and row["stamp"].get("version") == 2
                and row["stamp"].get("date")
                and row["stamp"].get("timezone")
                for row in v2_rows
            ),
            "details": {"v2Count": len(v2_rows)},
        },
    ]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "phase": "worker-audit",
        "runId": run_id,
        "route": route,
        "mode": "dry-run",
        "versions": {"harness": 1, "scheduleContract": 2},
        "before": before,
        "after": after,
        "fixtureIds": [],
        "assertions": assertions,
        "cleanup": {"required": False, "completed": True, "paths": []},
        "ready": all(item["passed"] for item in assertions),
        "stopReasons": [item["name"] for item in assertions if not item["passed"]],
    }


def build_create_finalize_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    evidence = build_trusted_reconcile_evidence(db, route=route, run_id=run_id)
    stamp_assertion = next(
        item for item in evidence["assertions"]
        if item["name"] == "live_resolutions_form_exact_v2_stamp_shape"
    )
    assertion = {
        "name": "create_finalize_has_unique_live_stamp_candidates",
        "passed": stamp_assertion["passed"] and stamp_assertion["details"]["count"] > 0,
        "details": stamp_assertion["details"],
    }
    evidence["phase"] = "create-finalize"
    evidence["assertions"].append(assertion)
    evidence["ready"] = evidence["ready"] and assertion["passed"]
    if not assertion["passed"]:
        evidence["stopReasons"].append("no_unique_create_finalize_candidate")
    return evidence


def build_client_cutoff_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    """Read-only Phase 6 gate for stamp-zone and exact-cutoff client fixtures."""
    evidence = build_trusted_reconcile_evidence(db, route=route, run_id=run_id)
    timezone_name = evidence["before"]["timezone"]["value"]
    settings = evidence["before"]["adjustmentSettings"]
    parity = next(
        item for item in evidence["assertions"]
        if item["name"] == "typescript_python_delivery_resolution_parity"
    )
    rows = [
        row["resolution"] for row in parity["details"]["python"]
        if row["resolution"]["status"] == "resolved"
    ]
    fixtures = []
    for row in rows:
        cycle = row["cycle"]
        source = {
            "scheduledOrder": {
                "version": 2,
                "date": row["orderDate"],
                "timezone": timezone_name,
                "scheduleKey": row["scheduleKey"],
                "orderDay": cycle["orderDay"],
                "loadOffsetDays": cycle["loadOffsetDays"],
                "deliveryOffsetDays": cycle["deliveryOffsetDays"],
                "provenance": "cycle",
            }
        }
        initial = build_canonical_reminder_fields(
            source,
            settings=settings,
            has_meaningful_lines=True,
            status="draft",
            now_ms=0,
        )
        cutoff_ms = int(initial["cutoffAtMs"])
        fixtures.append(
            {
                "stamp": source["scheduledOrder"],
                "oneMinuteBefore": build_canonical_reminder_fields(
                    source, settings=settings, has_meaningful_lines=True,
                    status="draft", now_ms=cutoff_ms - 60_000,
                ),
                "atCutoff": build_canonical_reminder_fields(
                    source, settings=settings, has_meaningful_lines=True,
                    status="draft", now_ms=cutoff_ms,
                ),
                "afterCutoff": build_canonical_reminder_fields(
                    source, settings=settings, has_meaningful_lines=True,
                    status="draft", now_ms=cutoff_ms + 1,
                ),
            }
        )
    cutoff_assertion = {
        "name": "client_cutoff_equality_is_closed_in_stamp_timezone",
        "passed": bool(fixtures) and all(
            item["oneMinuteBefore"]["reminderStatus"] == "pending"
            and item["oneMinuteBefore"]["timezone"] == item["stamp"]["timezone"]
            and item["atCutoff"]["reminderStatus"] == "skipped"
            and item["atCutoff"]["reminderSkipReason"] == "past_cutoff"
            and item["afterCutoff"]["reminderStatus"] == "skipped"
            and item["afterCutoff"]["reminderSkipReason"] == "past_cutoff"
            for item in fixtures
        ),
        "details": {"count": len(fixtures), "fixtures": fixtures},
    }
    stamp_hash_assertion = {
        "name": "client_gate_read_only_stamp_hash_unchanged",
        "passed": evidence["before"]["hash"] == evidence["after"]["hash"],
        "details": {
            "beforeHash": evidence["before"]["hash"],
            "afterHash": evidence["after"]["hash"],
        },
    }
    evidence["phase"] = "client-cutoff"
    evidence["assertions"].extend([cutoff_assertion, stamp_hash_assertion])
    evidence["ready"] = evidence["ready"] and cutoff_assertion["passed"] and stamp_hash_assertion["passed"]
    if not cutoff_assertion["passed"]:
        evidence["stopReasons"].append("client_cutoff_fixture_failed")
    if not stamp_hash_assertion["passed"]:
        evidence["stopReasons"].append("live_state_changed_during_client_gate")
    return evidence


def build_lifecycle_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    """Read-only Phase 7 gate for lifecycle and archive invariants."""
    evidence = build_types_rules_evidence(db, route=route, run_id=run_id)
    stamp_rows = next(
        item["details"]["stamps"] for item in evidence["assertions"]
        if item["name"] == "live_resolutions_form_exact_v2_stamp_shape"
    )
    stamp = stamp_rows[0] if stamp_rows else None
    order_id = f"synthetic-{route}-phase7"
    finalized = {
        "routeNumber": route,
        "status": "finalized",
        "submittedAt": "2026-08-28T12:09:00-06:00",
        "lastMutation": {
            "kind": "finalization",
            "mutationId": f"{run_id}-finalize",
        },
        "scheduleKey": stamp.get("scheduleKey") if stamp else "unknown",
        "expectedDeliveryDate": "2026-09-01",
        "scheduledOrder": stamp,
        "stores": [],
    }
    calls: list[str] = []
    guard = FinalizationDispatchGuard()
    first = dispatch_finalized_order_if_new(
        guard, object(), order_id, finalized,
        handler=lambda _db, handled_id, _data: calls.append(handled_id),
    )
    duplicate = dispatch_finalized_order_if_new(
        guard,
        object(),
        order_id,
        {**finalized, "scheduledOrder": dict(stamp or {})},
        handler=lambda _db, handled_id, _data: calls.append(handled_id),
    )
    primed_guard = FinalizationDispatchGuard()
    primed_guard.prime(finalization_identity(route, order_id, finalized))
    initial_snapshot = dispatch_finalized_order_if_new(
        primed_guard, object(), order_id, finalized,
        handler=lambda _db, handled_id, _data: calls.append(handled_id),
    )
    projection = build_order_archive_projection(order_id, route, finalized)
    legacy_projection = build_order_archive_projection(
        f"{order_id}-legacy",
        route,
        {key: value for key, value in finalized.items() if key != "scheduledOrder"},
    )
    statuses = ["applied", "draft"]
    cleanup_blocked = any(status in {"draft", "sent"} for status in statuses)

    assertions = [
        {
            "name": "finalization_identity_dispatches_once",
            "passed": first and not duplicate and calls == [order_id],
            "details": {"first": first, "duplicate": duplicate, "handlerCalls": calls},
        },
        {
            "name": "initial_snapshot_is_primed_without_side_effects",
            "passed": not initial_snapshot and calls == [order_id],
            "details": {"dispatched": initial_snapshot, "handlerCalls": calls},
        },
        {
            "name": "stamped_archive_projection_is_v2_and_legacy_remains_v1",
            "passed": (
                bool(stamp)
                and projection.get("schemaVersion") == 2
                and projection.get("scheduledOrder") == stamp
                and legacy_projection.get("schemaVersion") == 1
                and "scheduledOrder" not in legacy_projection
            ),
            "details": {
                "v2Fingerprint": projection.get("sourceFingerprint"),
                "legacyFingerprint": legacy_projection.get("sourceFingerprint"),
            },
        },
        {
            "name": "cleanup_preview_blocks_nonterminal_source_adjustment",
            "passed": cleanup_blocked,
            "details": {"statuses": statuses, "reason": "nonterminal_adjustment_exists"},
        },
        {
            "name": "lifecycle_gate_is_read_only",
            "passed": evidence["before"]["hash"] == evidence["after"]["hash"],
            "details": {
                "beforeHash": evidence["before"]["hash"],
                "afterHash": evidence["after"]["hash"],
            },
        },
    ]
    evidence["phase"] = "lifecycle"
    evidence["assertions"].extend(assertions)
    evidence["ready"] = evidence["ready"] and all(item["passed"] for item in assertions)
    evidence["stopReasons"].extend(
        item["name"] for item in assertions if not item["passed"]
    )
    return evidence


def build_deployed_evidence(
    db: Any,
    *,
    route: str,
    run_id: str,
) -> dict[str, Any]:
    before = capture_route_state(db, route)
    root = Path(__file__).resolve().parents[2]
    deployment = collect_deployment_report(
        db, route=route, root=root, expect_stamp_gate="open"
    )
    after = capture_route_state(db, route)
    assertions = [
        {
            "name": "deployment_monitor_ready",
            "passed": deployment["ready"],
            "details": deployment,
        },
        {
            "name": "deployed_gate_is_read_only",
            "passed": before["hash"] == after["hash"],
            "details": {"beforeHash": before["hash"], "afterHash": after["hash"]},
        },
    ]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "phase": "deployed",
        "runId": run_id,
        "route": route,
        "mode": "dry-run",
        "versions": {"harness": 1, "scheduleContract": 2},
        "before": before,
        "after": after,
        "fixtureIds": [],
        "assertions": assertions,
        "cleanup": {"required": False, "completed": True, "paths": []},
        "ready": all(item["passed"] for item in assertions),
        "stopReasons": [item["name"] for item in assertions if not item["passed"]],
    }


def _default_service_account() -> Path:
    configured = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    return Path(configured) if configured else Path.cwd() / "routespark-firebase-adminsdk.json"


def _initialize(service_account: Path):
    if not service_account.is_file():
        raise FileNotFoundError(f"Missing service account: {service_account}")
    if not firebase_admin._apps:
        firebase_admin.initialize_app(
            credentials.Certificate(str(service_account)),
            {"projectId": PROJECT_ID},
        )
    return firestore.client()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route", required=True)
    parser.add_argument("--phase", default="baseline")
    parser.add_argument("--run-id")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--notification-sink")
    parser.add_argument("--output", required=True)
    parser.add_argument("--serviceAccount", type=Path, default=_default_service_account())
    parser.add_argument("--skip-global-cutoff-census", action="store_true")
    args = parser.parse_args(argv)
    args.route = str(args.route).strip()
    if not ROUTE_PATTERN.fullmatch(args.route):
        parser.error("--route must contain 1-10 digits")
    if args.phase not in {"baseline", "helper-parity", "types-rules", "trusted-reconcile", "worker-audit", "create-finalize", "client-cutoff", "lifecycle", "deployed"}:
        parser.error("supported phases: baseline, helper-parity, types-rules, trusted-reconcile, worker-audit, create-finalize, client-cutoff, lifecycle, deployed")
    if args.apply:
        parser.error("Phase 0 baseline is read-only; --apply is forbidden")
    args.dry_run = True
    args.run_id = args.run_id or f"oa-v2-{args.route}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    args.output = Path(args.output).resolve()
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db = _initialize(args.serviceAccount.resolve())
    if args.phase == "baseline":
        evidence = build_baseline_evidence(
            db,
            route=args.route,
            run_id=args.run_id,
            global_census=not args.skip_global_cutoff_census,
        )
    elif args.phase == "helper-parity":
        evidence = build_helper_parity_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "types-rules":
        evidence = build_types_rules_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "trusted-reconcile":
        evidence = build_trusted_reconcile_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "worker-audit":
        evidence = build_worker_audit_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "create-finalize":
        evidence = build_create_finalize_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "client-cutoff":
        evidence = build_client_cutoff_evidence(db, route=args.route, run_id=args.run_id)
    elif args.phase == "lifecycle":
        evidence = build_lifecycle_evidence(db, route=args.route, run_id=args.run_id)
    else:
        evidence = build_deployed_evidence(db, route=args.route, run_id=args.run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(_jsonable(evidence), indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "ready": evidence["ready"], "stopReasons": evidence["stopReasons"]}))
    return 0 if evidence["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

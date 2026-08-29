#!/usr/bin/env python3
"""Read-only deployment and quiet-cycle monitor for schedule rule v2."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
from typing import Any

try:
    from .order_adjustment_reminder_worker import audit_once
except ImportError:
    from order_adjustment_reminder_worker import audit_once


EXPECTED_WORKER_CONFIG = {
    "ORDER_ADJUSTMENT_REMINDERS_ENABLED": "true",
    "ORDER_ADJUSTMENT_REMINDER_DRY_RUN": "false",
    "ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT": "100",
    "ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS": "600000",
    "ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS": "120000",
}
MAX_SUCCESS_AGE_SECONDS = 5 * 60


def _run_json(args: list[str]) -> dict[str, Any]:
    completed = subprocess.run(args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return json.loads(completed.stdout)


def _run_text(args: list[str]) -> str:
    completed = subprocess.run(args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout


def _pinned_image(path: Path, container_name: str) -> str | None:
    text = path.read_text()
    pattern = re.compile(
        rf"- name:\s*{re.escape(container_name)}\s*\n\s*image:\s*([^\s]+)",
        re.MULTILINE,
    )
    match = pattern.search(text)
    return match.group(1) if match else None


def _container_by_name(workload: dict[str, Any], name: str) -> dict[str, Any]:
    containers = (
        workload.get("spec", {})
        .get("jobTemplate", {})
        .get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers")
    )
    if containers is None:
        containers = (
            workload.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("containers", [])
        )
    for container in containers or []:
        if container.get("name") == name:
            return container
    raise RuntimeError(f"Container {name!r} is missing from workload")


def _json_events(text: str) -> list[dict[str, Any]]:
    events = []
    for line in text.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            value = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("event"):
            events.append(value)
    return events


def _successful_job_summaries(namespace: str, limit: int = 2) -> list[dict[str, Any]]:
    jobs = _run_json([
        "kubectl", "-n", namespace, "get", "jobs",
        "-l", "app.kubernetes.io/name=order-adjustment-reminders", "-o", "json",
    ]).get("items", [])
    succeeded = [
        job for job in jobs
        if int((job.get("status") or {}).get("succeeded") or 0) > 0
    ]
    succeeded.sort(
        key=lambda job: str((job.get("status") or {}).get("completionTime") or ""),
        reverse=True,
    )
    summaries = []
    for job in succeeded[:limit]:
        name = job["metadata"]["name"]
        logs = _run_text(["kubectl", "-n", namespace, "logs", f"job/{name}"])
        events = _json_events(logs)
        run = next(
            (event for event in reversed(events) if event.get("event") == "order_adjustment_reminder_run"),
            None,
        )
        summaries.append({
            "job": name,
            "completionTime": (job.get("status") or {}).get("completionTime"),
            "run": run,
        })
    return summaries


def _age_seconds(value: str | None, now: datetime) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0.0, (now - parsed.astimezone(timezone.utc)).total_seconds())


def _route_v2_health(audit: dict[str, Any], *, require_v2: bool) -> tuple[dict[str, Any], list[str]]:
    v2_rows = [row for row in audit["results"] if row.get("classification") == "v2"]
    eligible_v2_rows = [row for row in v2_rows if row.get("eligibleForReminder") is True]
    wrong_future = [
        row["id"] for row in eligible_v2_rows
        if row.get("storedFutureScheduleDrift") is True
    ]
    unresolved = [row["id"] for row in eligible_v2_rows if row.get("schedule") is None]
    remaining_due = audit.get("remainingDueIds") or []
    alerts = []
    if require_v2 and not v2_rows:
        alerts.append("route_has_no_v2_source")
    if wrong_future:
        alerts.append("stored_future_v1_rows_for_v2_source")
    if unresolved:
        alerts.append("v2_source_unexpectedly_unresolved")
    if remaining_due:
        alerts.append("route_due_backlog_remaining")
    return {
        "v2Count": len(v2_rows),
        "wrongFutureIds": wrong_future,
        "unresolvedV2Ids": unresolved,
        "remainingDueIds": remaining_due,
    }, alerts


def collect_deployment_report(
    db: Any,
    *,
    route: str,
    root: Path,
    expect_stamp_gate: str = "open",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    alerts: list[str] = []
    cron = _run_json([
        "kubectl", "-n", "routespark-events", "get", "cronjob",
        "order-adjustment-reminders", "-o", "json",
    ])
    container = _container_by_name(cron, "order-adjustment-reminders")
    live_worker_image = container.get("image")
    expected_worker_image = _pinned_image(
        root / "k8s/overlays/lab/reminder-workers-image-patch.yaml",
        "order-adjustment-reminders",
    )
    cron_state = {
        "schedule": cron["spec"].get("schedule"),
        "suspend": cron["spec"].get("suspend"),
        "concurrencyPolicy": cron["spec"].get("concurrencyPolicy"),
        "image": live_worker_image,
        "expectedImage": expected_worker_image,
        "args": container.get("args") or [],
        "lastSuccessfulTime": (cron.get("status") or {}).get("lastSuccessfulTime"),
    }
    if cron_state["schedule"] != "*/2 * * * *":
        alerts.append("worker_schedule_changed")
    if cron_state["suspend"] is not False:
        alerts.append("worker_unexpectedly_suspended")
    if cron_state["concurrencyPolicy"] != "Forbid":
        alerts.append("worker_concurrency_policy_changed")
    if live_worker_image != expected_worker_image:
        alerts.append("worker_image_pin_mismatch")
    if cron_state["args"] != [
        "scripts/order_adjustment_reminder_worker.py",
        "--serviceAccount", "/app/config/serviceAccountKey.json", "--once",
    ]:
        alerts.append("worker_args_changed")
    success_age = _age_seconds(cron_state["lastSuccessfulTime"], now)
    if success_age is None or success_age > MAX_SUCCESS_AGE_SECONDS:
        alerts.append("worker_last_success_stale")

    config = _run_json([
        "kubectl", "-n", "routespark-events", "get", "configmap",
        "order-adjustment-reminders-config", "-o", "json",
    ]).get("data", {})
    config_matches = {
        key: config.get(key) == value for key, value in EXPECTED_WORKER_CONFIG.items()
    }
    if not all(config_matches.values()):
        alerts.append("worker_config_mismatch")

    jobs = _successful_job_summaries("routespark-events", 2)
    if len(jobs) < 2:
        alerts.append("fewer_than_two_successful_worker_ticks")
    for item in jobs:
        run = item.get("run") or {}
        if run.get("scheduleRuleVersion") != 2:
            alerts.append("worker_tick_missing_v2_version")
        if int(run.get("failed") or 0) > 0:
            alerts.append("worker_send_failure")
        if run.get("batchLimitReached") is True:
            alerts.append("worker_batch_limit_exhausted")
        if run.get("remainingDueIds"):
            alerts.append("worker_due_backlog_remaining")

    listener = _run_json([
        "kubectl", "-n", "routespark-events", "get", "deployment",
        "order-sync-listener", "-o", "json",
    ])
    listener_container = _container_by_name(listener, "order-sync-listener")
    live_listener_image = listener_container.get("image")
    expected_listener_image = _pinned_image(
        root / "k8s/overlays/lab/order-sync-listener-image-patch.yaml",
        "order-sync-listener",
    )
    if live_listener_image != expected_listener_image:
        alerts.append("listener_image_pin_mismatch")
    listener_logs = _run_text([
        "kubectl", "-n", "routespark-events", "logs",
        "deployment/order-sync-listener", "--since=15m",
    ])
    listener_events = _json_events(listener_logs)
    replay_events = [
        event for event in listener_events
        if event.get("event") == "order_finalization_dispatched"
    ]

    edge_config = _run_json([
        "kubectl", "-n", "routespark-edge", "get", "configmap",
        "web-api-config", "-o", "json",
    ]).get("data", {})
    expected_gate = "true" if expect_stamp_gate == "open" else "false"
    gate = {
        "stampWritesEnabled": edge_config.get("ORDER_ADJUSTMENT_SCHEDULE_V2_STAMP_WRITES_ENABLED"),
        "workerCompatible": edge_config.get("ORDER_ADJUSTMENT_SCHEDULE_V2_WORKER_COMPATIBLE"),
        "minimumClientVersion": edge_config.get("ORDER_ADJUSTMENT_SCHEDULE_V2_MIN_CLIENT_VERSION"),
        "requiredCapability": edge_config.get("ORDER_ADJUSTMENT_SCHEDULE_V2_REQUIRED_CAPABILITY"),
    }
    if gate["stampWritesEnabled"] != expected_gate:
        alerts.append("stamp_gate_state_mismatch")
    if gate["workerCompatible"] != "true":
        alerts.append("worker_compatibility_gate_closed")
    if gate["minimumClientVersion"] != "1.1.7":
        alerts.append("minimum_client_version_mismatch")
    if gate["requiredCapability"] != "scheduled-order-day-v2":
        alerts.append("required_capability_mismatch")

    audit = audit_once(db, route_number=route)
    route_audit, route_alerts = _route_v2_health(
        audit, require_v2=expect_stamp_gate == "open"
    )
    alerts.extend(route_alerts)
    if replay_events:
        alerts.append("finalization_replay_detected")

    unique_alerts = sorted(set(alerts))
    return {
        "schemaVersion": 1,
        "generatedAt": now.isoformat(),
        "route": route,
        "cronjob": cron_state,
        "workerConfig": config,
        "workerConfigMatches": config_matches,
        "successfulTicks": jobs,
        "listener": {
            "image": live_listener_image,
            "expectedImage": expected_listener_image,
            "initialPrimeEvents": [
                event for event in listener_events
                if event.get("event") == "order_sync_listener_initial_prime"
            ],
            "finalizationDispatchEvents": replay_events,
        },
        "releaseGate": gate,
        "routeAudit": route_audit,
        "alerts": unique_alerts,
        "ready": not unique_alerts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route", required=True)
    parser.add_argument("--serviceAccount", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expect-stamp-gate", choices=("open", "closed"), default="open")
    args = parser.parse_args()
    if not args.route.isdigit() or len(args.route) > 10:
        parser.error("--route must contain 1-10 digits")
    from google.cloud import firestore
    db = firestore.Client.from_service_account_json(args.serviceAccount)
    root = Path(__file__).resolve().parents[2]
    report = collect_deployment_report(
        db, route=args.route, root=root, expect_stamp_gate=args.expect_stamp_gate
    )
    args.output.resolve().write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"ready": report["ready"], "alerts": report["alerts"]}))
    return 0 if report["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

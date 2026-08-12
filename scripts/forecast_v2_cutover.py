#!/usr/bin/env python3
"""Inventory, enqueue, and verify route-scoped schema-v2 forecast targets."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone

from firebase_writer import get_firestore_client
from forecast_contract import (
    ForecastContractError,
    load_authority_generation_state,
    validate_ready_artifact,
)
from forecast_generation_queue import enqueue_generation_job
from forecast_generation_queue import derive_upcoming_generation_targets
from schedule_cycle import normalize_order_cycle, schedule_key_for_day
from schedule_utils import get_order_cycles


def firebase_upcoming_generation_targets(db, route: str, lookahead_days: int = 21):
    """Firestore-authority fallback for route-scoped cutover checkpoints."""
    cycles = [normalize_order_cycle(cycle) for cycle in get_order_cycles(db, route)]
    finalized = {
        (
            str((snapshot.to_dict() or {}).get("expectedDeliveryDate") or ""),
            str((snapshot.to_dict() or {}).get("scheduleKey") or "").lower(),
        )
        for snapshot in db.collection("routes").document(route).collection("orders")
        .where("status", "==", "finalized").stream()
    }
    today = datetime.now(timezone.utc).date()
    targets = []
    for cycle in cycles:
        schedule_key = schedule_key_for_day(cycle["orderDay"])
        for offset in range(1, max(2, int(lookahead_days)) + 1):
            delivery = today + timedelta(days=offset)
            order_date = delivery - timedelta(days=cycle["deliveryOffsetDays"])
            if order_date.isoweekday() != cycle["orderDay"]:
                continue
            delivery_date = delivery.isoformat()
            if (delivery_date, schedule_key) in finalized:
                continue
            targets.append({
                "delivery_date": delivery_date,
                "schedule_key": schedule_key,
            })
            break
    return sorted(targets, key=lambda target: (target["delivery_date"], target["schedule_key"]))


def active_targets(db, route: str, include_upcoming: bool = True):
    docs = db.collection("routes").document(route).collection("orders").where("status", "==", "draft").stream()
    today = datetime.now(timezone.utc).date().isoformat()
    targets = {
        (str(data.get("expectedDeliveryDate") or ""), str(data.get("scheduleKey") or "").lower())
        for snapshot in docs
        if (data := (snapshot.to_dict() or {}))
        and data.get("expectedDeliveryDate") and data.get("scheduleKey")
        and str(data.get("expectedDeliveryDate")) >= today
    }
    if include_upcoming:
        try:
            upcoming = derive_upcoming_generation_targets(route)
        except Exception:
            upcoming = firebase_upcoming_generation_targets(db, route)
        for target in upcoming:
            targets.add((target["delivery_date"], target["schedule_key"]))
    return sorted(targets)


def candidates(db, route: str, delivery_date: str, schedule_key: str):
    ref = db.collection("forecasts").document(route).collection("cached")
    return [
        snapshot.to_dict() or {}
        for snapshot in ref.where("deliveryDate", "==", delivery_date)
        .where("scheduleKey", "==", schedule_key).stream()
    ]


def inspect_target(db, route: str, delivery_date: str, schedule_key: str):
    active_keys, revision = load_authority_generation_state(db, route, delivery_date, schedule_key)
    if not active_keys:
        return {
            "routeNumber": route,
            "deliveryDate": delivery_date,
            "scheduleKey": schedule_key,
            "artifactCount": 0,
            "readyForecastId": None,
            "verified": False,
            "required": False,
            "skippedReason": "no_active_carry_items",
            "desiredRevision": revision,
            "errors": [],
        }
    docs = candidates(db, route, delivery_date, schedule_key)
    errors = []
    ready_id = None
    for artifact in docs:
        try:
            validate_ready_artifact(
                artifact,
                route_number=route,
                delivery_date=delivery_date,
                schedule_key=schedule_key,
                active_carry_keys=active_keys,
            )
            if artifact.get("generationInputFingerprint") != revision:
                raise ForecastContractError("generation_input_stale")
            ready_id = artifact.get("forecastId")
            break
        except ForecastContractError as exc:
            errors.append(str(exc))
    return {
        "routeNumber": route,
        "deliveryDate": delivery_date,
        "scheduleKey": schedule_key,
        "artifactCount": len(docs),
        "readyForecastId": ready_id,
        "verified": bool(ready_id),
        "required": True,
        "desiredRevision": revision,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serviceAccount", required=True)
    parser.add_argument("--routes", required=True, help="Comma-separated route numbers")
    parser.add_argument("--mode", choices=("dry-run", "enqueue", "verify"), default="dry-run")
    parser.add_argument(
        "--draft-targets-only",
        action="store_true",
        help="Checkpoint mode: inventory active draft targets without claiming rollout coverage",
    )
    parser.add_argument("--output")
    args = parser.parse_args()
    db = get_firestore_client(args.serviceAccount)
    report = {
        "mode": args.mode,
        "targetScope": "active_drafts_only" if args.draft_targets_only else "drafts_and_upcoming_schedules",
        "rolloutCoverageComplete": False if args.draft_targets_only else None,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "targets": [],
    }
    routes = sorted({value.strip() for value in args.routes.split(",") if value.strip()})
    if not routes or any(not re.fullmatch(r"\d{1,10}", route) for route in routes):
        parser.error("--routes must contain only comma-separated numeric route numbers")
    for route in routes:
        for delivery_date, schedule_key in active_targets(
            db, route, include_upcoming=not args.draft_targets_only
        ):
            row = inspect_target(db, route, delivery_date, schedule_key)
            if args.mode == "enqueue" and row["required"] and not row["verified"]:
                job = enqueue_generation_job(
                    route, schedule_key, delivery_date,
                    source="forecast_v2_cutover",
                    desired_revision=row["desiredRevision"],
                    refresh_reason="v2_cutover",
                )
                row["jobId"] = (job or {}).get("job_key")
            report["targets"].append(row)
    report["unverifiedCount"] = sum(
        row["required"] and not row["verified"] for row in report["targets"]
    )
    if not args.draft_targets_only:
        report["rolloutCoverageComplete"] = report["unverifiedCount"] == 0
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        parent = os.path.dirname(os.path.abspath(args.output))
        os.makedirs(parent, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(rendered + "\n")
    print(rendered)
    return 1 if args.mode == "verify" and report["unverifiedCount"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

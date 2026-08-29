#!/usr/bin/env python3
"""One-shot, fail-closed PCF lifecycle effect worker."""

from __future__ import annotations

import argparse
import json
import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore, storage

from order_forecast.api.pcf_database import (
    get_pcf_pg_connection,
    return_pcf_pg_connection,
)
from order_forecast.api.pcf_projection_reconciliation_runtime import (
    ProjectionReconciliationRuntime,
    record_reconciliation_failure,
    reserve_due_reconciliation_route,
)
from order_forecast.api.pcf_projection_source import (
    FirestoreProjectionSourceAdapter,
    load_production_fixture_registry,
)
from order_forecast.scripts.pcf_lifecycle_due import (
    block_lifecycle_claim,
    claim_one_lifecycle_work,
    complete_lifecycle_claim,
    recover_expired_lifecycle_leases,
)
from order_forecast.scripts.pcf_lifecycle_effect import execute_exact_archive
from order_forecast.scripts.pcf_lifecycle_runtime import (
    FirebaseExactArchiveAdapter,
    LifecycleRuntimeControl,
    PostgresClaimAuthority,
    PostgresProjectionLoader,
)


WORKER_ID = f"pcf-lifecycle-{socket.gethostname()}-{os.getpid()}"
RECONCILIATION_MODES = {"off", "bounded"}
DEFAULT_FIXTURE_REGISTRY = Path(
    "/app/order_forecast/contracts/pcf/fixture-identity-v1.json"
)


def _positive_int(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if not 1 <= value <= maximum:
        raise RuntimeError(f"{name} must be between 1 and {maximum}")
    return value


def _release(conn) -> None:
    return_pcf_pg_connection(conn)


def _reconciliation_mode() -> str:
    mode = os.environ.get("PCF_RECONCILIATION_MODE", "off").strip()
    if mode not in RECONCILIATION_MODES:
        raise RuntimeError(
            "PCF_RECONCILIATION_MODE must be exactly 'off' or 'bounded'"
        )
    return mode


def _service_account_project(service_account: Path) -> str:
    if not service_account.is_file():
        raise RuntimeError("Firebase service account is required")
    try:
        payload = json.loads(service_account.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Firebase service account is unreadable") from exc
    project_id = payload.get("project_id") if isinstance(payload, dict) else None
    if not isinstance(project_id, str) or not project_id.strip():
        raise RuntimeError("Firebase service account project_id is required")
    return project_id.strip()


def _reserve_reconciliation_route(now: datetime) -> str | None:
    conn = get_pcf_pg_connection()
    try:
        route = reserve_due_reconciliation_route(conn, observed_at=now)
        conn.commit()
        return route
    except Exception:
        conn.rollback()
        raise
    finally:
        _release(conn)


def _record_failed_reconciliation(route: str) -> None:
    conn = get_pcf_pg_connection()
    try:
        record_reconciliation_failure(conn, route)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _release(conn)


def _reconciliation_result(report) -> dict[str, object]:
    return {
        "routeNumber": report.route_number,
        "repairMode": report.repair_mode,
        "status": report.status,
        "reason": report.reason,
        "sourceSnapshotCount": report.source_snapshot_count,
        "upsertRepairs": report.upsert_repairs,
        "tombstoneRepairs": report.tombstone_repairs,
        "unresolvedExtras": report.unresolved_extras,
        "missingInProjection": len(report.final_result.missing_in_projection),
        "changedInProjection": len(report.final_result.changed_in_projection),
        "extraInProjection": len(report.final_result.extra_in_projection),
        "minimumFirestoreReads": report.minimum_firestore_reads,
    }


def _block_live_claim(claim, reason: str, error: str | None = None) -> bool:
    now = datetime.now(timezone.utc)
    conn = get_pcf_pg_connection()
    try:
        changed = block_lifecycle_claim(
            conn,
            work_id=claim.id,
            claim_token=claim.claim_token,
            now_utc=now,
            next_attempt_at=now + timedelta(minutes=5),
            reason=reason[:64],
            error=error,
        )
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        _release(conn)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service-account",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
    )
    parser.add_argument(
        "--control-file",
        default=os.environ.get(
            "PCF_LIFECYCLE_CONTROL_FILE", "/app/config/pcf-lifecycle-control.json"
        ),
    )
    args = parser.parse_args()
    control_path = Path(args.control_file)
    now = datetime.now(timezone.utc)
    control = LifecycleRuntimeControl.read(control_path)
    reconciliation_mode = _reconciliation_mode()
    if not control.effects_enabled and reconciliation_mode == "off":
        print(json.dumps({"event": "pcf_lifecycle_worker_disabled"}, sort_keys=True))
        return 0
    if control.effects_enabled and control.all_proven_work:
        raise RuntimeError(
            "allProvenWork remains disabled until a fleet-wide mutation fence is implemented"
        )
    allowed_keys: tuple[str, ...] = ()
    if control.effects_enabled:
        allowed_keys = control.allowed_semantic_keys(now)
        if not allowed_keys:
            raise RuntimeError("no live exact lifecycle fence is available")

    service_account = Path(args.service_account)
    project_id = _service_account_project(service_account)
    bucket_name = os.environ.get("FIREBASE_STORAGE_BUCKET", "").strip()
    if control.effects_enabled and not bucket_name:
        raise RuntimeError("Firebase storage bucket is required for lifecycle effects")

    reconciliation_route = None
    fixture_registry = None
    if reconciliation_mode == "bounded":
        expected_project = os.environ.get("PCF_RECONCILIATION_PROJECT_ID", "").strip()
        if not expected_project or project_id != expected_project:
            raise RuntimeError("reconciliation Firebase project identity mismatch")
        registry_path = Path(
            os.environ.get(
                "PCF_RECONCILIATION_FIXTURE_REGISTRY", str(DEFAULT_FIXTURE_REGISTRY)
            )
        )
        fixture_registry = load_production_fixture_registry(
            registry_path,
            production_project=expected_project,
        )
        reconciliation_route = _reserve_reconciliation_route(now)

    app = None
    db = None
    bucket = None
    reconciliation = {"status": "not_due"}
    if reconciliation_route is not None or control.effects_enabled:
        options = {"projectId": project_id}
        if control.effects_enabled:
            options["storageBucket"] = bucket_name
        try:
            app = firebase_admin.initialize_app(
                credentials.Certificate(str(service_account)),
                options,
                name=f"pcf-lifecycle-{os.getpid()}",
            )
            db = firestore.client(app=app)
            if reconciliation_route is not None:
                runtime = ProjectionReconciliationRuntime(
                    connection_provider=get_pcf_pg_connection,
                    connection_releaser=_release,
                    source_adapter=FirestoreProjectionSourceAdapter(
                        db,
                        fixture_registry=fixture_registry,
                    ),
                )
                reconciliation = _reconciliation_result(
                    runtime.run_route(reconciliation_route, observed_at=now)
                )
            if control.effects_enabled:
                bucket = storage.bucket(app=app)
        except Exception:
            if reconciliation_route is not None:
                try:
                    _record_failed_reconciliation(reconciliation_route)
                except Exception as record_exc:
                    raise RuntimeError(
                        "reconciliation failed and failure health could not be recorded"
                    ) from record_exc
            raise

    if not control.effects_enabled:
        print(
            json.dumps(
                {
                    "event": "pcf_lifecycle_worker_complete",
                    "effectsEnabled": False,
                    "reconciliation": reconciliation,
                },
                sort_keys=True,
            )
        )
        return 0

    lease_seconds = _positive_int("PCF_LIFECYCLE_LEASE_SECONDS", 300, 3600)
    max_claims = _positive_int("PCF_LIFECYCLE_MAX_CLAIMS", 10, 20)
    authority = PostgresClaimAuthority(
        get_pcf_pg_connection, _release, lease_seconds=lease_seconds
    )
    projection_loader = PostgresProjectionLoader(get_pcf_pg_connection, _release)

    recovery_conn = get_pcf_pg_connection()
    try:
        recovered = recover_expired_lifecycle_leases(
            recovery_conn, now_utc=datetime.now(timezone.utc), limit=100
        )
        recovery_conn.commit()
    finally:
        _release(recovery_conn)

    counters = {"claimed": 0, "completed": 0, "blocked": 0, "lost": 0}
    for _ in range(max_claims):
        claim_conn = get_pcf_pg_connection()
        try:
            claim = claim_one_lifecycle_work(
                claim_conn,
                worker_id=WORKER_ID,
                now_utc=datetime.now(timezone.utc),
                lease_seconds=lease_seconds,
                allowed_semantic_keys=allowed_keys,
                all_proven_work=False,
            )
            claim_conn.commit()
        finally:
            _release(claim_conn)
        if claim is None:
            break
        counters["claimed"] += 1
        adapter = FirebaseExactArchiveAdapter(
            firestore_client=db,
            bucket=bucket,
            control_path=control_path,
            claim_authority=authority,
            projection_loader=projection_loader,
        )
        try:
            result = execute_exact_archive(claim, adapter)
        except Exception as exc:
            if _block_live_claim(claim, "effect_error", type(exc).__name__):
                counters["blocked"] += 1
            else:
                counters["lost"] += 1
            continue
        if result.completed:
            complete_conn = get_pcf_pg_connection()
            try:
                completed = complete_lifecycle_claim(
                    complete_conn,
                    work_id=claim.id,
                    claim_token=claim.claim_token,
                    now_utc=datetime.now(timezone.utc),
                    outcome=result.disposition,
                )
                complete_conn.commit()
            finally:
                _release(complete_conn)
            counters["completed" if completed else "lost"] += 1
        elif result.disposition in {"claim_authority_not_active", "claim_authority_lost"}:
            counters["lost"] += 1
        elif _block_live_claim(claim, result.disposition):
            counters["blocked"] += 1
        else:
            counters["lost"] += 1

    print(
        json.dumps(
            {
                "event": "pcf_lifecycle_worker_complete",
                "effectsEnabled": True,
                "reconciliation": reconciliation,
                "recovered": recovered,
                **counters,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

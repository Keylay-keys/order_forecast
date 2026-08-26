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
    if not control.effects_enabled:
        print(json.dumps({"event": "pcf_lifecycle_worker_disabled"}, sort_keys=True))
        return 0
    if control.all_proven_work:
        raise RuntimeError(
            "allProvenWork remains disabled until a fleet-wide mutation fence is implemented"
        )
    allowed_keys = control.allowed_semantic_keys(now)
    if not allowed_keys:
        raise RuntimeError("no live exact lifecycle fence is available")

    service_account = Path(args.service_account)
    bucket_name = os.environ.get("FIREBASE_STORAGE_BUCKET", "").strip()
    if not service_account.is_file() or not bucket_name:
        raise RuntimeError("Firebase service account and storage bucket are required")
    app = firebase_admin.initialize_app(
        credentials.Certificate(str(service_account)),
        {"storageBucket": bucket_name},
        name=f"pcf-lifecycle-{os.getpid()}",
    )
    db = firestore.client(app=app)
    bucket = storage.bucket(app=app)
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
                "recovered": recovered,
                **counters,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

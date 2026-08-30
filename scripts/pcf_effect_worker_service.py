#!/usr/bin/env python3
"""Long-running, exact-allowlist PCF fixed-effect worker (disabled by default)."""

from __future__ import annotations

import argparse
import json
import os
import select
import signal
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore, storage as firebase_storage
from google.auth import impersonated_credentials
from google.cloud import storage as google_storage
from google.oauth2 import service_account

from order_forecast.api.pcf_database import (
    get_pcf_pg_connection,
    return_pcf_pg_connection,
)
from order_forecast.api.pcf_effect_control import load_effect_control
from order_forecast.api.pcf_effect_firebase import ExactFirestoreProjectionStore
from order_forecast.api.pcf_effect_handlers import FixedPcfEffectHandlers
from order_forecast.api.pcf_effect_notification import (
    ExactReviewAlertStore,
    PcfNotificationDispatcher,
)
from order_forecast.api.pcf_effect_runtime import EFFECT_STEPS
from order_forecast.api.pcf_effect_service import (
    EffectServiceError,
    PostgresSuccessorGuard,
    run_controlled_effect_cycle,
    validate_service_account_separation,
)
from order_forecast.api.pcf_effect_storage import ExactPcfObjectStore
from order_forecast.api.pcf_effect_summary import (
    PcfEffectSummaryStore,
    PcfEffectSummaryVerifier,
)
from order_forecast.api.pcf_notification_provider import TelegramNotificationProvider
from order_forecast.api.pcf_runtime_control import load_runtime_control
from order_forecast.api.pcf_work_runtime import WORK_NOTIFY_CHANNEL


DEFAULT_RUNTIME_CONTROL = Path("/app/config/pcf-runtime-control.json")
DEFAULT_EFFECT_CONTROL = Path("/app/config/pcf-effect-control.json")
DEFAULT_EVIDENCE_MOUNT = Path("/mnt/archive/pcf/pcf_effect_evidence")
ACTOR_ID = f"pcf-effect-{socket.gethostname()}-{os.getpid()}"
_stop_requested = False


def _signal_stop(_signum: int, _frame: Any) -> None:
    global _stop_requested
    _stop_requested = True


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EffectServiceError(f"{name} is required")
    return value


def _enabled() -> bool:
    value = os.environ.get("PCF_EFFECT_WORKER_ENABLED", "false")
    if value not in {"true", "false"}:
        raise EffectServiceError("PCF_EFFECT_WORKER_ENABLED must be true or false")
    return value == "true"


def _validate_preflight(runtime_path: Path, effect_path: Path, evidence: Path) -> None:
    runtime = load_runtime_control(runtime_path)
    if not runtime.is_fresh(datetime.now(timezone.utc)) or not runtime.allows_effects:
        raise EffectServiceError("pcf_effect_runtime_authority_denied")
    # Parsing is itself the empty/malformed/broad-scope fence. An effect-off
    # document is valid and leaves a deliberately started process idle.
    load_effect_control(effect_path)
    if not evidence.is_dir():
        raise EffectServiceError("pcf_effect_evidence_mount_unavailable")
    try:
        pool_max = int(os.environ.get("PCF_POSTGRES_POOL_MAX", "0"))
    except ValueError as exc:
        raise EffectServiceError("PCF_POSTGRES_POOL_MAX must be an integer") from exc
    if pool_max < 3:
        raise EffectServiceError("PCF_POSTGRES_POOL_MAX must be at least 3")


def _impersonated_bucket(
    source_service_account: Path,
    *,
    target_principal: str,
    project_id: str,
    bucket_name: str,
) -> Any:
    source = service_account.Credentials.from_service_account_file(
        str(source_service_account),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    delegated = impersonated_credentials.Credentials(
        source_credentials=source,
        target_principal=target_principal,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=900,
    )
    return google_storage.Client(
        project=project_id,
        credentials=delegated,
    ).bucket(bucket_name)


def _handlers(
    main_conn: Any,
    verifier_conn: Any,
    guard_conn: Any,
    *,
    writer_db: Any,
    verifier_db: Any,
    writer_bucket: Any,
    verifier_bucket: Any,
    cleaner_bucket: Any,
    evidence_mount: Path,
) -> dict[str, Any]:
    provider = TelegramNotificationProvider(
        bot_token=_required("PCF_TELEGRAM_BOT_TOKEN"),
        chat_id=_required("PCF_TELEGRAM_CHAT_ID"),
    )
    fixed = FixedPcfEffectHandlers(
        firestore_writer=ExactFirestoreProjectionStore(writer_db),
        firestore_verifier=ExactFirestoreProjectionStore(verifier_db),
        object_projector=ExactPcfObjectStore(writer_bucket),
        object_verifier=ExactPcfObjectStore(verifier_bucket),
        object_cleaner=ExactPcfObjectStore(cleaner_bucket),
        review_alert_writer=ExactReviewAlertStore(writer_db),
        review_alert_verifier=ExactReviewAlertStore(verifier_db),
        notification_dispatcher=PcfNotificationDispatcher(
            main_conn,
            provider,
            actor_id=ACTOR_ID,
        ),
        summary_writer=PcfEffectSummaryStore(main_conn, actor_id=ACTOR_ID),
        summary_verifier=PcfEffectSummaryVerifier(verifier_conn),
        successor_exists=PostgresSuccessorGuard(guard_conn),
        evidence_mount=evidence_mount,
    )
    return {step: getattr(fixed, step) for step in EFFECT_STEPS}


def _listen(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {WORK_NOTIFY_CHANNEL}")
    conn.commit()


def _unlisten(conn: Any) -> None:
    try:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"UNLISTEN {WORK_NOTIFY_CHANNEL}")
        conn.commit()
    except Exception:
        conn.rollback()


def _wait(conn: Any, seconds: int) -> None:
    ready, _, _ = select.select([conn], [], [], seconds)
    if ready:
        conn.poll()
        if hasattr(conn, "notifies"):
            conn.notifies.clear()


def _event(report: Any) -> dict[str, Any]:
    effect = report.controller.effect
    return {
        "event": "pcf_effect_worker_cycle",
        "runtimeRevision": report.runtime_revision,
        "effectControlEnabled": report.effect_control_enabled,
        "admittedWorkCount": report.admitted_work_count,
        "disposition": effect.disposition,
        "workId": effect.work_id,
        "step": effect.step,
        "nextStep": effect.next_step,
        "status": effect.status,
        "retryAt": effect.retry_at.isoformat() if effect.retry_at else None,
        "errorCode": effect.error_code,
        "queueTtlDisposition": report.controller.queue_ttl_disposition,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-control", type=Path, default=DEFAULT_RUNTIME_CONTROL)
    parser.add_argument("--effect-control", type=Path, default=DEFAULT_EFFECT_CONTROL)
    parser.add_argument("--evidence-mount", type=Path, default=DEFAULT_EVIDENCE_MOUNT)
    parser.add_argument(
        "--writer-service-account",
        type=Path,
        default=Path("/app/config/firebase-writer.json"),
    )
    parser.add_argument(
        "--verifier-service-account",
        type=Path,
        default=Path("/app/config/firebase-verifier.json"),
    )
    args = parser.parse_args()

    if not _enabled():
        print(json.dumps({"event": "pcf_effect_worker_disabled"}, sort_keys=True))
        return 0

    _validate_preflight(args.runtime_control, args.effect_control, args.evidence_mount)
    project_id = _required("PCF_FIREBASE_PROJECT_ID")
    bucket_name = _required("FIREBASE_STORAGE_BUCKET")
    writer_principal = _required("PCF_FIRESTORE_WRITER_SERVICE_ACCOUNT")
    verifier_principal = _required("PCF_VERIFIER_SERVICE_ACCOUNT")
    projector_principal = _required("PCF_OBJECT_PROJECTOR_SERVICE_ACCOUNT")
    cleaner_principal = _required("PCF_OBJECT_CLEANER_SERVICE_ACCOUNT")
    identities = validate_service_account_separation(
        (
            args.writer_service_account,
            args.verifier_service_account,
        ),
        expected_project=project_id,
        expected_direct_identities=(writer_principal, verifier_principal),
        delegated_identities=(projector_principal, cleaner_principal),
    )

    apps = []
    connections = []
    main_conn = None
    try:
        for role, identity in zip(("writer", "verifier"), identities):
            apps.append(
                firebase_admin.initialize_app(
                    credentials.Certificate(str(identity.path)),
                    {"projectId": project_id, "storageBucket": bucket_name},
                    name=f"pcf-effect-{role}-{os.getpid()}",
                )
            )
        writer_app, verifier_app = apps
        main_conn = get_pcf_pg_connection()
        verifier_conn = get_pcf_pg_connection()
        guard_conn = get_pcf_pg_connection()
        connections.extend((main_conn, verifier_conn, guard_conn))
        writer_db = firestore.client(app=writer_app)
        verifier_db = firestore.client(app=verifier_app)
        handlers = _handlers(
            main_conn,
            verifier_conn,
            guard_conn,
            writer_db=writer_db,
            verifier_db=verifier_db,
            writer_bucket=_impersonated_bucket(
                args.writer_service_account,
                target_principal=projector_principal,
                project_id=project_id,
                bucket_name=bucket_name,
            ),
            verifier_bucket=firebase_storage.bucket(app=verifier_app),
            cleaner_bucket=_impersonated_bucket(
                args.writer_service_account,
                target_principal=cleaner_principal,
                project_id=project_id,
                bucket_name=bucket_name,
            ),
            evidence_mount=args.evidence_mount,
        )
        _listen(main_conn)
        signal.signal(signal.SIGTERM, _signal_stop)
        signal.signal(signal.SIGINT, _signal_stop)
        print(
            json.dumps(
                {"event": "pcf_effect_worker_started", "actorId": ACTOR_ID},
                sort_keys=True,
            )
        )
        while not _stop_requested:
            report = run_controlled_effect_cycle(
                main_conn,
                writer_db,
                handlers,
                actor_id=ACTOR_ID,
                runtime_control_path=args.runtime_control,
                effect_control_path=args.effect_control,
            )
            print(json.dumps(_event(report), sort_keys=True))
            if report.controller.effect.disposition == "idle":
                _wait(main_conn, 30)
        return 0
    finally:
        if main_conn is not None:
            _unlisten(main_conn)
        for conn in reversed(connections):
            return_pcf_pg_connection(conn, discard=bool(getattr(conn, "closed", False)))
        for app in reversed(apps):
            firebase_admin.delete_app(app)


if __name__ == "__main__":
    raise SystemExit(main())

"""PostgreSQL-backed archive export queue helpers.

This module defines the cluster-native archive export queue model while keeping
the current API contract stable:

- export IDs stay text values like ``exp_<id>``
- user-facing statuses stay ``queued`` / ``processing`` / ``ready`` /
  ``ready_partial`` / ``failed`` / ``expired``
- dedupe stays scoped to route + date range + format

It is intentionally aligned to the existing FastAPI archive export router and
the current Firestore worker semantics so migration can be incremental instead
of speculative.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor


ACTIVE_REUSABLE_STATUSES = {"queued", "processing", "ready", "ready_partial"}
QUEUE_ACTIVE_STATUSES = {"queued", "processing"}
READY_STATUSES = {"ready", "ready_partial"}
TERMINAL_STATUSES = {"ready", "ready_partial", "failed", "expired"}


def _pg_connect(autocommit: bool = True) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DB", "routespark"),
        user=os.environ.get("POSTGRES_USER", "routespark"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    conn.autocommit = autocommit
    return conn


def ensure_archive_export_queue_tables() -> None:
    """Create archive export queue tables and indexes if missing."""
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_export_jobs (
                    export_id TEXT PRIMARY KEY,
                    job_key TEXT NOT NULL,
                    route_number VARCHAR(20) NOT NULL,
                    requested_by_uid VARCHAR(255) NOT NULL,
                    requested_by_email TEXT,
                    from_date DATE NOT NULL,
                    to_date DATE NOT NULL,
                    format VARCHAR(16) NOT NULL DEFAULT 'zip',
                    status VARCHAR(20) NOT NULL DEFAULT 'queued',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    retry_after_at TIMESTAMP WITH TIME ZONE,
                    claimed_by VARCHAR(100),
                    started_at TIMESTAMP WITH TIME ZONE,
                    worker_heartbeat_at TIMESTAMP WITH TIME ZONE,
                    ready_at TIMESTAMP WITH TIME ZONE,
                    artifact_storage_path TEXT,
                    artifact_expires_at TIMESTAMP WITH TIME ZONE,
                    artifact_parts JSONB NOT NULL DEFAULT '[]'::jsonb,
                    artifact_size_bytes BIGINT NOT NULL DEFAULT 0,
                    archived_pcf_retention_days INTEGER NOT NULL DEFAULT 90,
                    archived_pcf_end_of_life_action VARCHAR(64) NOT NULL DEFAULT 'allow_export_then_delete',
                    result_warning_count INTEGER NOT NULL DEFAULT 0,
                    result_total_deliveries_requested INTEGER NOT NULL DEFAULT 0,
                    result_total_deliveries_exported INTEGER NOT NULL DEFAULT 0,
                    result_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    last_download_link_generated_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_export_attempts (
                    id BIGSERIAL PRIMARY KEY,
                    export_id TEXT NOT NULL REFERENCES archive_export_jobs(export_id) ON DELETE CASCADE,
                    attempt_number INTEGER NOT NULL,
                    worker_id VARCHAR(100),
                    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMP WITH TIME ZONE,
                    outcome VARCHAR(24) NOT NULL,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    artifact_size_bytes BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_archive_export_jobs_route_status_created
                ON archive_export_jobs(route_number, status, created_at)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_archive_export_jobs_status_retry_created
                ON archive_export_jobs(status, retry_after_at, created_at)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_archive_export_jobs_requester_created
                ON archive_export_jobs(requested_by_uid, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_archive_export_jobs_job_key
                ON archive_export_jobs(job_key)
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_archive_export_one_processing_per_route
                ON archive_export_jobs(route_number)
                WHERE status = 'processing'
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_archive_export_attempts_export_id_started
                ON archive_export_attempts(export_id, started_at DESC)
                """
            )
    finally:
        conn.close()


def build_export_job_key(route_number: str, from_date: Any, to_date: Any, export_format: str = "zip") -> str:
    """Build the dedupe key used by the current API contract."""
    return f"{str(route_number).strip()}::{_date_to_iso(from_date)}::{_date_to_iso(to_date)}::{str(export_format or 'zip').strip()}"


def build_queue_positions(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """Compute queue positions for currently queued jobs."""
    queued: List[tuple[str, int]] = []
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if status != "queued":
            continue
        created_at = row.get("created_at")
        created_ms = _to_epoch_millis(created_at) or 0
        export_id = str(row.get("export_id") or "").strip()
        if not export_id:
            continue
        queued.append((export_id, created_ms))
    queued.sort(key=lambda item: item[1])
    return {export_id: idx + 1 for idx, (export_id, _) in enumerate(queued)}


def serialize_archive_export_row(
    row: Dict[str, Any],
    *,
    queue_position_by_export_id: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Serialize a PostgreSQL row into the current web/API job shape."""
    status = str(row.get("status") or "queued").strip().lower()
    expires_at_ms = _to_epoch_millis(row.get("artifact_expires_at"))
    job = {
        "exportId": str(row.get("export_id") or ""),
        "routeNumber": str(row.get("route_number") or ""),
        "fromDate": _date_to_iso(row.get("from_date")),
        "toDate": _date_to_iso(row.get("to_date")),
        "format": str(row.get("format") or "zip"),
        "status": status,
        "attemptCount": int(row.get("attempt_count") or 0),
        "maxAttempts": int(row.get("max_attempts") or 3),
        "createdAtMs": _to_epoch_millis(row.get("created_at")),
        "updatedAtMs": _to_epoch_millis(row.get("updated_at")),
        "readyAtMs": _to_epoch_millis(row.get("ready_at")),
        "expiresAtMs": expires_at_ms,
        "artifactExpiresAtMs": expires_at_ms,
        "artifactParts": row.get("artifact_parts") if isinstance(row.get("artifact_parts"), list) else [],
        "errorCode": str(row.get("error_code") or "") or None,
        "errorMessage": str(row.get("error_message") or "") or None,
    }
    if queue_position_by_export_id and status == "queued":
        queue_position = queue_position_by_export_id.get(job["exportId"])
        if queue_position is not None:
            job["queuePosition"] = queue_position
    return job


def notify_archive_export_job(export_id: str) -> None:
    """Wake a LISTENing export worker after enqueue or retry release."""
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_notify('archive_export_jobs', %s)", [str(export_id)])
    finally:
        conn.close()


def fetch_route_jobs(route_number: str, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT *
                FROM archive_export_jobs
                WHERE route_number = %s
                ORDER BY created_at DESC
            """
            params: List[Any] = [str(route_number)]
            if limit is not None:
                sql += " LIMIT %s"
                params.append(int(limit))
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def fetch_export_job(export_id: str) -> Optional[Dict[str, Any]]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM archive_export_jobs WHERE export_id = %s",
                [str(export_id)],
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def find_reusable_job(
    route_number: str,
    from_date: Any,
    to_date: Any,
    export_format: str,
    *,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM archive_export_jobs
                WHERE route_number = %s
                  AND from_date = %s
                  AND to_date = %s
                  AND format = %s
                  AND status = ANY(%s)
                ORDER BY created_at ASC
                """,
                [
                    str(route_number),
                    _date_to_iso(from_date),
                    _date_to_iso(to_date),
                    str(export_format or "zip"),
                    list(ACTIVE_REUSABLE_STATUSES),
                ],
            )
            rows = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    now_ms = _to_epoch_millis(now)
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if status in READY_STATUSES:
            expires_ms = _to_epoch_millis(row.get("artifact_expires_at"))
            if expires_ms is not None and now_ms is not None and expires_ms <= now_ms:
                continue
        return row
    return None


def count_requests_today(requested_by_uid: str, day_start: datetime) -> int:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM archive_export_jobs
                WHERE requested_by_uid = %s
                  AND created_at >= %s
                """,
                [str(requested_by_uid), day_start],
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
    finally:
        conn.close()


def count_active_queue_depth(route_number: str) -> int:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM archive_export_jobs
                WHERE route_number = %s
                  AND status = ANY(%s)
                """,
                [str(route_number), list(QUEUE_ACTIVE_STATUSES)],
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
    finally:
        conn.close()


def count_queued_jobs(route_number: str) -> int:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM archive_export_jobs
                WHERE route_number = %s
                  AND status = 'queued'
                """,
                [str(route_number)],
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)
    finally:
        conn.close()


def create_export_job(
    *,
    export_id: str,
    route_number: str,
    requested_by_uid: str,
    requested_by_email: Optional[str],
    from_date: Any,
    to_date: Any,
    export_format: str,
    max_attempts: int,
    archived_pcf_retention_days: int,
    archived_pcf_end_of_life_action: str,
) -> Dict[str, Any]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO archive_export_jobs (
                    export_id,
                    job_key,
                    route_number,
                    requested_by_uid,
                    requested_by_email,
                    from_date,
                    to_date,
                    format,
                    status,
                    attempt_count,
                    max_attempts,
                    archived_pcf_retention_days,
                    archived_pcf_end_of_life_action
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'queued', 0, %s, %s, %s)
                RETURNING *
                """,
                [
                    str(export_id),
                    build_export_job_key(route_number, from_date, to_date, export_format),
                    str(route_number),
                    str(requested_by_uid),
                    str(requested_by_email or "").strip() or None,
                    _date_to_iso(from_date),
                    _date_to_iso(to_date),
                    str(export_format or "zip"),
                    int(max_attempts),
                    int(archived_pcf_retention_days),
                    str(archived_pcf_end_of_life_action or "allow_export_then_delete"),
                ],
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def cancel_export_job(export_id: str) -> Optional[Dict[str, Any]]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE archive_export_jobs
                SET status = 'failed',
                    error_code = 'CANCELED_BY_OWNER',
                    error_message = 'Export request canceled by route owner',
                    updated_at = NOW()
                WHERE export_id = %s
                  AND status = 'queued'
                RETURNING *
                """,
                [str(export_id)],
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def mark_export_expired(export_id: str) -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE archive_export_jobs
                SET status = 'expired',
                    updated_at = NOW()
                WHERE export_id = %s
                """,
                [str(export_id)],
            )
    finally:
        conn.close()


def touch_download_link(export_id: str) -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE archive_export_jobs
                SET last_download_link_generated_at = NOW(),
                    updated_at = NOW()
                WHERE export_id = %s
                """,
                [str(export_id)],
            )
    finally:
        conn.close()


def count_processing_jobs() -> int:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archive_export_jobs WHERE status = 'processing'")
            row = cur.fetchone()
            return int(row[0] if row else 0)
    finally:
        conn.close()


def claim_next_job(*, worker_id: str, max_global_concurrency: int) -> Optional[Dict[str, Any]]:
    conn = _pg_connect(autocommit=False)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM archive_export_jobs WHERE status = 'processing'")
            row = cur.fetchone()
            if int(row["count"] if row else 0) >= int(max_global_concurrency):
                conn.rollback()
                return None

            cur.execute(
                """
                WITH next_job AS (
                    SELECT j.export_id
                    FROM archive_export_jobs j
                    WHERE j.status = 'queued'
                      AND (j.retry_after_at IS NULL OR j.retry_after_at <= NOW())
                      AND NOT EXISTS (
                        SELECT 1
                        FROM archive_export_jobs p
                        WHERE p.route_number = j.route_number
                          AND p.status = 'processing'
                      )
                    ORDER BY j.created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE archive_export_jobs j
                SET status = 'processing',
                    claimed_by = %s,
                    started_at = COALESCE(j.started_at, NOW()),
                    worker_heartbeat_at = NOW(),
                    updated_at = NOW()
                FROM next_job
                WHERE j.export_id = next_job.export_id
                RETURNING j.*
                """,
                [str(worker_id)],
            )
            claimed = cur.fetchone()
        conn.commit()
        return dict(claimed) if claimed else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_job_heartbeat(export_id: str) -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE archive_export_jobs
                SET worker_heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE export_id = %s
                """,
                [str(export_id)],
            )
    finally:
        conn.close()


def finalize_job_success(export_id: str, result: Dict[str, Any], *, artifact_ttl_days: int) -> None:
    expires_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=int(artifact_ttl_days))
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE archive_export_jobs
                SET status = %s,
                    ready_at = NOW(),
                    worker_heartbeat_at = NOW(),
                    artifact_storage_path = %s,
                    artifact_expires_at = %s,
                    artifact_parts = %s,
                    artifact_size_bytes = %s,
                    result_warning_count = %s,
                    result_total_deliveries_requested = %s,
                    result_total_deliveries_exported = %s,
                    result_warnings = %s,
                    error_code = NULL,
                    error_message = NULL,
                    retry_after_at = NULL,
                    updated_at = NOW()
                WHERE export_id = %s
                """,
                [
                    str(result["status"]),
                    str(result["blobPath"]),
                    expires_at,
                    Json(result.get("parts") or []),
                    int(result.get("sizeBytes") or 0),
                    len(result.get("warnings") or []),
                    int(result.get("requested") or 0),
                    int(result.get("exported") or 0),
                    Json(result.get("warnings") or []),
                    str(export_id),
                ],
            )
    finally:
        conn.close()


def finalize_job_failure(
    export_id: str,
    *,
    current_attempt_count: int,
    max_attempts: int,
    error_code: str,
    error_message: str,
    retryable: bool,
    retry_delay_seconds: int,
) -> Dict[str, Any]:
    next_attempt = int(current_attempt_count) + 1
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if retryable and next_attempt < int(max_attempts):
                cur.execute(
                    """
                    UPDATE archive_export_jobs
                    SET status = 'queued',
                        attempt_count = %s,
                        retry_after_at = NOW() + (%s || ' seconds')::interval,
                        worker_heartbeat_at = NOW(),
                        error_code = %s,
                        error_message = %s,
                        updated_at = NOW()
                    WHERE export_id = %s
                    RETURNING *
                    """,
                    [
                        next_attempt,
                        int(retry_delay_seconds),
                        str(error_code),
                        str(error_message)[:1000],
                        str(export_id),
                    ],
                )
                row = cur.fetchone()
                return dict(row) if row else {}

            cur.execute(
                """
                UPDATE archive_export_jobs
                SET status = 'failed',
                    attempt_count = %s,
                    worker_heartbeat_at = NOW(),
                    error_code = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE export_id = %s
                RETURNING *
                """,
                [
                    next_attempt,
                    str(error_code),
                    str(error_message)[:1000],
                    str(export_id),
                ],
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def recover_stale_processing_jobs(*, worker_timeout_seconds: int, stale_threshold_seconds: int) -> List[Dict[str, Any]]:
    conn = _pg_connect(autocommit=True)
    recovered: List[Dict[str, Any]] = []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM archive_export_jobs
                WHERE status = 'processing'
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    now_ms = _to_epoch_millis(datetime.utcnow())
    stale_ms = int(stale_threshold_seconds) * 1000
    timeout_ms = int(worker_timeout_seconds) * 1000
    for row in rows:
        started_ms = _to_epoch_millis(row.get("started_at")) or _to_epoch_millis(row.get("updated_at"))
        heartbeat_ms = _to_epoch_millis(row.get("worker_heartbeat_at")) or started_ms
        if started_ms is None:
            started_ms = now_ms or 0
        if heartbeat_ms is None:
            heartbeat_ms = now_ms or 0
        is_stale = (now_ms or 0) - heartbeat_ms > stale_ms
        timed_out = (now_ms or 0) - started_ms > timeout_ms
        if not is_stale and not timed_out:
            continue
        recovered.append(
            finalize_job_failure(
                str(row.get("export_id") or ""),
                current_attempt_count=int(row.get("attempt_count") or 0),
                max_attempts=int(row.get("max_attempts") or 3),
                error_code="WORKER_TIMEOUT" if timed_out else "STALE_PROCESSING_JOB",
                error_message="Processing job timed out" if timed_out else "Processing job became stale",
                retryable=True,
                retry_delay_seconds=60,
            )
        )
    return recovered


def _to_epoch_millis(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    return None


def _date_to_iso(value: Any) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value or "")

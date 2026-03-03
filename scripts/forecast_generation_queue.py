"""Forecast generation queue helpers shared by listener and daemon.

This module introduces durable finalize-event dedupe and a PostgreSQL-backed
generation job queue so finalize-triggered and daemon-triggered work use the
same claim/retry/processing flow.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from google.cloud import firestore  # type: ignore

try:
    from google.cloud.firestore_v1.base_query import FieldFilter
except Exception:
    FieldFilter = None  # type: ignore

try:
    from .forecast_engine import ForecastConfig, generate_forecast
except ImportError:
    from forecast_engine import ForecastConfig, generate_forecast


TERMINAL_JOB_STATUSES = {"done", "skipped_fresh", "error"}
SUCCESS_JOB_STATUSES = {"done", "skipped_fresh"}


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


def ensure_forecast_queue_tables() -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_finalize_events (
                    finalize_key TEXT PRIMARY KEY,
                    route_number VARCHAR(20) NOT NULL,
                    order_id VARCHAR(255) NOT NULL,
                    schedule_key VARCHAR(20),
                    finalized_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    job_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
                    last_error TEXT,
                    source_worker_id VARCHAR(100),
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    processed_at TIMESTAMP WITH TIME ZONE
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_generation_jobs (
                    job_key TEXT PRIMARY KEY,
                    route_number VARCHAR(20) NOT NULL,
                    schedule_key VARCHAR(20) NOT NULL,
                    delivery_date DATE NOT NULL,
                    source VARCHAR(32) NOT NULL,
                    finalize_key TEXT REFERENCES forecast_finalize_events(finalize_key) ON DELETE SET NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    trigger_count INTEGER NOT NULL DEFAULT 1,
                    available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    claimed_by VARCHAR(100),
                    claimed_at TIMESTAMP WITH TIME ZONE,
                    started_at TIMESTAMP WITH TIME ZONE,
                    finished_at TIMESTAMP WITH TIME ZONE,
                    skipped_reason TEXT,
                    last_error TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    last_triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_forecast_jobs_route_status_available
                ON forecast_generation_jobs(route_number, status, available_at)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_forecast_jobs_status_available
                ON forecast_generation_jobs(status, available_at)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_forecast_finalize_route_status
                ON forecast_finalize_events(route_number, status)
                """
            )
    finally:
        conn.close()


def _to_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if hasattr(value, "to_datetime"):
        try:
            value = value.to_datetime()
        except Exception:
            return None
    elif hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            return None

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(s)
        except Exception:
            return None

    if not isinstance(value, datetime):
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_iso_seconds(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_schedule_key(schedule_key: Optional[str]) -> Optional[str]:
    if schedule_key is None:
        return None
    sk = str(schedule_key).strip().lower()
    return sk or None


def normalize_delivery_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date().strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        return None
    try:
        if len(s) >= 10:
            return datetime.fromisoformat(s[:10]).strftime("%Y-%m-%d")
    except Exception:
        return None
    return None


def build_finalize_key(route_number: str, order_id: str, finalized_at: datetime) -> str:
    return f"{str(route_number).strip()}::{str(order_id).strip()}::{_utc_iso_seconds(finalized_at)}"


def build_job_key(route_number: str, schedule_key: str, delivery_date: str) -> str:
    return f"{str(route_number).strip()}::{str(schedule_key).strip().lower()}::{str(delivery_date).strip()}"


def _register_finalize_event(
    route_number: str,
    order_id: str,
    schedule_key: Optional[str],
    finalized_at: datetime,
    worker_id: str,
) -> Dict[str, Any]:
    finalize_key = build_finalize_key(route_number, order_id, finalized_at)
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO forecast_finalize_events (
                    finalize_key, route_number, order_id, schedule_key,
                    finalized_at, status, source_worker_id
                )
                VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                ON CONFLICT (finalize_key) DO NOTHING
                RETURNING finalize_key, route_number, order_id, schedule_key, status, job_keys
                """,
                [finalize_key, str(route_number), str(order_id), schedule_key, finalized_at, worker_id],
            )
            inserted = cur.fetchone()
            if inserted:
                row = dict(inserted)
                row["inserted"] = True
                return row

            cur.execute(
                """
                SELECT finalize_key, route_number, order_id, schedule_key, status, job_keys, last_error
                FROM forecast_finalize_events
                WHERE finalize_key = %s
                """,
                [finalize_key],
            )
            row = cur.fetchone() or {}
            out = dict(row)
            out["inserted"] = False
            return out
    finally:
        conn.close()


def mark_finalize_event_error(finalize_key: str, error_text: str) -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE forecast_finalize_events
                SET status = 'error',
                    last_error = %s,
                    updated_at = NOW(),
                    processed_at = NULL
                WHERE finalize_key = %s
                """,
                [str(error_text)[:4000], finalize_key],
            )
    finally:
        conn.close()


def register_finalize_event(
    route_number: str,
    order_id: str,
    schedule_key: Optional[str],
    finalized_at_raw: Any,
    worker_id: str,
) -> Dict[str, Any]:
    ensure_forecast_queue_tables()
    finalized_at = _to_utc_datetime(finalized_at_raw) or datetime.now(timezone.utc)
    return _register_finalize_event(
        route_number=str(route_number),
        order_id=str(order_id),
        schedule_key=normalize_schedule_key(schedule_key),
        finalized_at=finalized_at,
        worker_id=worker_id,
    )


def coerce_finalized_at(value: Any, fallback_now: bool = True) -> Optional[datetime]:
    parsed = _to_utc_datetime(value)
    if parsed:
        return parsed
    if fallback_now:
        return datetime.now(timezone.utc)
    return None


def _append_finalize_event_job_keys(finalize_key: str, job_keys: Iterable[str]) -> None:
    keys = sorted({str(k) for k in job_keys if str(k).strip()})
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT job_keys FROM forecast_finalize_events WHERE finalize_key = %s",
                [finalize_key],
            )
            row = cur.fetchone() or {}
            existing = row.get("job_keys") if isinstance(row.get("job_keys"), list) else []
            merged = sorted({str(k) for k in (existing or []) + keys})
            cur.execute(
                """
                UPDATE forecast_finalize_events
                SET job_keys = %s::jsonb,
                    updated_at = NOW()
                WHERE finalize_key = %s
                """,
                [Json(merged), finalize_key],
            )
    finally:
        conn.close()


def _infer_schedule_key_for_order(order_id: str) -> Optional[str]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT schedule_key
                FROM orders_historical
                WHERE order_id = %s
                LIMIT 1
                """,
                [str(order_id)],
            )
            row = cur.fetchone() or {}
            return normalize_schedule_key(row.get("schedule_key"))
    finally:
        conn.close()


def _get_route_last_finalized_at(route_number: str) -> Optional[datetime]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT MAX(finalized_at) AS last_finalized_at
                FROM orders_historical
                WHERE route_number = %s
                """,
                [str(route_number)],
            )
            row = cur.fetchone() or {}
            return _to_utc_datetime(row.get("last_finalized_at"))
    finally:
        conn.close()


def _get_next_unordered_delivery_global(route_number: str, lookahead_days: int = 21) -> Optional[Dict[str, str]]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT schedule_key, delivery_day
                FROM user_schedules
                WHERE route_number = %s AND is_active = TRUE
                """,
                [str(route_number)],
            )
            schedules = cur.fetchall() or []

            candidates: List[Dict[str, str]] = []
            today = datetime.now(timezone.utc).date()
            for sched in schedules:
                sk = normalize_schedule_key(sched.get("schedule_key"))
                delivery_dow = int(sched.get("delivery_day") or 0)  # 1=Mon ... 7=Sun
                if not sk or delivery_dow <= 0:
                    continue
                for days in range(1, max(2, int(lookahead_days)) + 1):
                    check_date = today + timedelta(days=days)
                    if (check_date.weekday() + 1) != delivery_dow:
                        continue
                    delivery_str = check_date.strftime("%Y-%m-%d")
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM orders_historical
                        WHERE route_number = %s
                          AND schedule_key = %s
                          AND delivery_date = %s
                        """,
                        [str(route_number), sk, delivery_str],
                    )
                    row = cur.fetchone() or {}
                    if int(row.get("cnt") or 0) > 0:
                        continue
                    candidates.append({"delivery_date": delivery_str, "schedule_key": sk})
                    break

            if not candidates:
                return None
            candidates.sort(key=lambda x: x["delivery_date"])
            return candidates[0]
    finally:
        conn.close()


def _get_next_unordered_delivery_for_schedule(
    route_number: str,
    schedule_key: str,
    lookahead_days: int = 21,
) -> Optional[Dict[str, str]]:
    sk = normalize_schedule_key(schedule_key)
    if not sk:
        return None
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT delivery_day
                FROM user_schedules
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND is_active = TRUE
                """,
                [str(route_number), sk],
            )
            rows = cur.fetchall() or []
            if not rows:
                return None

            candidates: List[str] = []
            today = datetime.now(timezone.utc).date()
            for row in rows:
                delivery_dow = int(row.get("delivery_day") or 0)
                if delivery_dow <= 0:
                    continue
                for days in range(1, max(2, int(lookahead_days)) + 1):
                    check_date = today + timedelta(days=days)
                    if (check_date.weekday() + 1) != delivery_dow:
                        continue
                    delivery_str = check_date.strftime("%Y-%m-%d")
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM orders_historical
                        WHERE route_number = %s
                          AND schedule_key = %s
                          AND delivery_date = %s
                        """,
                        [str(route_number), sk, delivery_str],
                    )
                    cnt_row = cur.fetchone() or {}
                    if int(cnt_row.get("cnt") or 0) > 0:
                        continue
                    candidates.append(delivery_str)
                    break
            if not candidates:
                return None
            candidates.sort()
            return {"delivery_date": candidates[0], "schedule_key": sk}
    finally:
        conn.close()


def _derive_finalize_targets(route_number: str, finalized_schedule_key: Optional[str]) -> List[Dict[str, str]]:
    lookahead_days = int(os.environ.get("FORECAST_QUEUE_LOOKAHEAD_DAYS", "21"))
    primary: Optional[Dict[str, str]] = None
    if finalized_schedule_key:
        primary = _get_next_unordered_delivery_for_schedule(route_number, finalized_schedule_key, lookahead_days)
    if not primary:
        primary = _get_next_unordered_delivery_global(route_number, lookahead_days)
    if not primary:
        return []

    out = [primary]
    mode = str(os.environ.get("FORECAST_FINALIZE_SECONDARY_MODE", "never")).strip().lower()
    if mode not in {"always", "if_primary_far"}:
        return out

    secondary = _get_next_unordered_delivery_global(route_number, lookahead_days)
    if not secondary:
        return out
    if build_job_key(route_number, secondary["schedule_key"], secondary["delivery_date"]) == build_job_key(
        route_number, primary["schedule_key"], primary["delivery_date"]
    ):
        return out

    if mode == "if_primary_far":
        min_gap_days = int(os.environ.get("FORECAST_FINALIZE_SECONDARY_MIN_GAP_DAYS", "4"))
        primary_date = datetime.strptime(primary["delivery_date"], "%Y-%m-%d").date()
        if (primary_date - datetime.now(timezone.utc).date()).days < min_gap_days:
            return out

    out.append(secondary)
    return out


def enqueue_finalize_jobs(
    route_number: str,
    order_id: str,
    schedule_key: Optional[str],
    finalized_at_raw: Any,
    worker_id: str,
) -> Dict[str, Any]:
    ensure_forecast_queue_tables()

    finalized_at = _to_utc_datetime(finalized_at_raw) or datetime.now(timezone.utc)
    event = _register_finalize_event(route_number, order_id, normalize_schedule_key(schedule_key), finalized_at, worker_id)
    if event.get("status") == "processed":
        return {"status": "already_processed", "finalize_key": event.get("finalize_key"), "job_keys": event.get("job_keys") or []}

    effective_schedule = normalize_schedule_key(schedule_key) or _infer_schedule_key_for_order(order_id)
    targets = _derive_finalize_targets(str(route_number), effective_schedule)
    if not targets:
        mark_finalize_event_error(str(event.get("finalize_key")), "no_targets")
        return {"status": "no_targets", "finalize_key": event.get("finalize_key"), "job_keys": []}

    inserted_keys: List[str] = []
    for target in targets:
        delivery_date = normalize_delivery_date(target.get("delivery_date"))
        target_schedule = normalize_schedule_key(target.get("schedule_key"))
        if not delivery_date or not target_schedule:
            continue
        row = enqueue_generation_job(
            route_number=str(route_number),
            schedule_key=target_schedule,
            delivery_date=delivery_date,
            source="finalize_trigger",
            finalize_key=str(event.get("finalize_key")),
        )
        if row:
            inserted_keys.append(str(row.get("job_key")))

    _append_finalize_event_job_keys(str(event.get("finalize_key")), inserted_keys)
    return {
        "status": "queued",
        "finalize_key": event.get("finalize_key"),
        "job_keys": inserted_keys,
        "schedule_key": effective_schedule,
    }


def enqueue_generation_job(
    route_number: str,
    schedule_key: str,
    delivery_date: str,
    source: str,
    finalize_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    ensure_forecast_queue_tables()
    sk = normalize_schedule_key(schedule_key)
    dd = normalize_delivery_date(delivery_date)
    if not sk or not dd:
        return None
    job_key = build_job_key(route_number, sk, dd)
    max_attempts = int(os.environ.get("FORECAST_JOB_MAX_ATTEMPTS", "5"))
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO forecast_generation_jobs (
                    job_key, route_number, schedule_key, delivery_date,
                    source, finalize_key, status, attempts, max_attempts,
                    trigger_count, available_at, updated_at, last_triggered_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'queued', 0, %s, 1, NOW(), NOW(), NOW())
                ON CONFLICT (job_key) DO UPDATE SET
                    source = EXCLUDED.source,
                    finalize_key = COALESCE(forecast_generation_jobs.finalize_key, EXCLUDED.finalize_key),
                    trigger_count = forecast_generation_jobs.trigger_count + 1,
                    last_triggered_at = NOW(),
                    updated_at = NOW(),
                    status = CASE
                        WHEN forecast_generation_jobs.status = 'error' THEN 'queued'
                        ELSE forecast_generation_jobs.status
                    END,
                    attempts = CASE
                        WHEN forecast_generation_jobs.status = 'error' THEN 0
                        ELSE forecast_generation_jobs.attempts
                    END,
                    available_at = CASE
                        WHEN forecast_generation_jobs.status = 'error' THEN NOW()
                        ELSE forecast_generation_jobs.available_at
                    END,
                    last_error = CASE
                        WHEN forecast_generation_jobs.status = 'error' THEN NULL
                        ELSE forecast_generation_jobs.last_error
                    END
                RETURNING job_key, route_number, schedule_key, delivery_date, status, attempts, source
                """,
                [job_key, str(route_number), sk, dd, str(source), finalize_key, max_attempts],
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _claim_next_job(route_number: str, worker_id: str) -> Optional[Dict[str, Any]]:
    conn = _pg_connect(autocommit=False)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT job_key
                FROM forecast_generation_jobs
                WHERE route_number = %s
                  AND status = 'queued'
                  AND available_at <= NOW()
                ORDER BY available_at ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                [str(route_number)],
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            job_key = row.get("job_key")
            cur.execute(
                """
                UPDATE forecast_generation_jobs
                SET status = 'running',
                    claimed_by = %s,
                    claimed_at = NOW(),
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW()
                WHERE job_key = %s
                RETURNING *
                """,
                [worker_id, job_key],
            )
            claimed = cur.fetchone()
        conn.commit()
        return dict(claimed) if claimed else None
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def _select_latest_cached_forecast_meta(
    fb_client: firestore.Client, route_number: str, delivery_date: str, schedule_key: str
) -> Optional[Dict[str, Any]]:
    cached_ref = fb_client.collection("forecasts").document(str(route_number)).collection("cached")
    docs = []
    if FieldFilter is not None:
        query = (
            cached_ref.where(filter=FieldFilter("deliveryDate", "==", delivery_date))
            .where(filter=FieldFilter("scheduleKey", "==", schedule_key))
        )
        docs = list(query.stream())
    else:
        docs = [doc for doc in cached_ref.stream() if (doc.to_dict() or {}).get("deliveryDate") == delivery_date and (doc.to_dict() or {}).get("scheduleKey") == schedule_key]
    if not docs:
        return None
    docs.sort(key=lambda d: _to_utc_datetime((d.to_dict() or {}).get("createdAt")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return docs[0].to_dict() or {}


def _evaluate_job_freshness(
    fb_client: firestore.Client, route_number: str, delivery_date: str, schedule_key: str
) -> Tuple[bool, str]:
    meta = _select_latest_cached_forecast_meta(fb_client, route_number, delivery_date, schedule_key)
    if not meta:
        return False, "missing"
    now = datetime.now(timezone.utc)
    expires_at = _to_utc_datetime(meta.get("expiresAt"))
    if expires_at and expires_at <= now:
        return False, "expired"

    generated_at = _to_utc_datetime(meta.get("generatedAt")) or _to_utc_datetime(meta.get("createdAt"))
    if not generated_at:
        return True, "non_expired_no_generated_at"

    last_finalized_at = _get_route_last_finalized_at(route_number)
    if last_finalized_at and generated_at < last_finalized_at:
        return False, "order_finalized_after_forecast"
    return True, "fresh"


def _finish_job(job_key: str, status: str, skipped_reason: Optional[str] = None, error_text: Optional[str] = None) -> None:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE forecast_generation_jobs
                SET status = %s,
                    skipped_reason = %s,
                    last_error = %s,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE job_key = %s
                """,
                [status, skipped_reason, error_text, job_key],
            )
    finally:
        conn.close()


def _retry_or_fail_job(job: Dict[str, Any], error_text: str) -> None:
    attempts = int(job.get("attempts") or 0) + 1
    max_attempts = int(job.get("max_attempts") or int(os.environ.get("FORECAST_JOB_MAX_ATTEMPTS", "5")))
    base_seconds = int(os.environ.get("FORECAST_JOB_RETRY_BASE_SECONDS", "30"))
    max_backoff_seconds = int(os.environ.get("FORECAST_JOB_RETRY_MAX_SECONDS", "3600"))
    backoff_seconds = min(max_backoff_seconds, base_seconds * (2 ** max(0, attempts - 1)))

    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            if attempts >= max_attempts:
                cur.execute(
                    """
                    UPDATE forecast_generation_jobs
                    SET status = 'error',
                        attempts = %s,
                        last_error = %s,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE job_key = %s
                    """,
                    [attempts, str(error_text)[:4000], str(job.get("job_key"))],
                )
            else:
                cur.execute(
                    """
                    UPDATE forecast_generation_jobs
                    SET status = 'queued',
                        attempts = %s,
                        last_error = %s,
                        available_at = NOW() + (%s || ' seconds')::interval,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        updated_at = NOW()
                    WHERE job_key = %s
                    """,
                    [attempts, str(error_text)[:4000], backoff_seconds, str(job.get("job_key"))],
                )
    finally:
        conn.close()


def _process_claimed_job(
    fb_client: firestore.Client,
    sa_path: str,
    job: Dict[str, Any],
) -> str:
    route_number = str(job.get("route_number"))
    schedule_key = normalize_schedule_key(job.get("schedule_key")) or ""
    delivery_date = normalize_delivery_date(job.get("delivery_date")) or ""
    job_key = str(job.get("job_key"))

    if not route_number or not schedule_key or not delivery_date:
        _finish_job(job_key, "error", error_text="invalid_job_payload")
        return "error"

    is_fresh, reason = _evaluate_job_freshness(fb_client, route_number, delivery_date, schedule_key)
    if is_fresh:
        _finish_job(job_key, "skipped_fresh", skipped_reason=reason)
        return "skipped_fresh"

    cfg = ForecastConfig(
        route_number=route_number,
        delivery_date=delivery_date,
        schedule_key=schedule_key,
        service_account=sa_path,
        since_days=365,
        round_cases=True,
        ttl_days=7,
    )
    try:
        generate_forecast(cfg)
        _finish_job(job_key, "done")
        return "done"
    except Exception as exc:
        _retry_or_fail_job(job, str(exc))
        return "retry_or_error"


def process_generation_jobs_for_route(
    fb_client: firestore.Client,
    route_number: str,
    worker_id: str,
    max_jobs: int = 8,
    sa_path: Optional[str] = None,
) -> Dict[str, int]:
    ensure_forecast_queue_tables()
    lock_conn = _pg_connect(autocommit=True)
    stats = {"claimed": 0, "done": 0, "skipped_fresh": 0, "retry_or_error": 0}
    lock_key = f"forecast-route::{str(route_number)}"

    try:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS ok", [lock_key])
            lock_row = cur.fetchone()
            if not lock_row or not bool(lock_row[0]):
                return stats

        resolved_sa_path = sa_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "/app/config/serviceAccountKey.json"
        for _ in range(max(1, int(max_jobs))):
            claimed = _claim_next_job(str(route_number), worker_id)
            if not claimed:
                break
            stats["claimed"] += 1
            result = _process_claimed_job(fb_client, resolved_sa_path, claimed)
            if result in stats:
                stats[result] += 1
        return stats
    finally:
        try:
            with lock_conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", [lock_key])
        except Exception:
            pass
        lock_conn.close()


def reconcile_finalize_event(finalize_key: str) -> Dict[str, Any]:
    conn = _pg_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT status, job_keys
                FROM forecast_finalize_events
                WHERE finalize_key = %s
                """,
                [str(finalize_key)],
            )
            event = cur.fetchone() or {}
            job_keys = event.get("job_keys") if isinstance(event.get("job_keys"), list) else []
            if not job_keys:
                cur.execute(
                    """
                    UPDATE forecast_finalize_events
                    SET status = 'error',
                        last_error = 'no_jobs',
                        updated_at = NOW(),
                        processed_at = NULL
                    WHERE finalize_key = %s
                    """,
                    [str(finalize_key)],
                )
                return {"status": "error", "reason": "no_jobs"}

            cur.execute(
                """
                SELECT job_key, status
                FROM forecast_generation_jobs
                WHERE job_key = ANY(%s)
                """,
                [job_keys],
            )
            rows = cur.fetchall() or []
            statuses = {str(r.get("job_key")): str(r.get("status") or "") for r in rows}
            missing = [jk for jk in job_keys if jk not in statuses]
            if missing:
                return {"status": "pending", "reason": "missing_jobs"}

            status_set = set(statuses.values())
            if status_set.issubset(SUCCESS_JOB_STATUSES):
                cur.execute(
                    """
                    UPDATE forecast_finalize_events
                    SET status = 'processed',
                        last_error = NULL,
                        updated_at = NOW(),
                        processed_at = NOW()
                    WHERE finalize_key = %s
                    """,
                    [str(finalize_key)],
                )
                return {"status": "processed"}

            if status_set.issubset(TERMINAL_JOB_STATUSES) and "error" in status_set:
                cur.execute(
                    """
                    UPDATE forecast_finalize_events
                    SET status = 'error',
                        last_error = 'job_error',
                        updated_at = NOW(),
                        processed_at = NULL
                    WHERE finalize_key = %s
                    """,
                    [str(finalize_key)],
                )
                return {"status": "error", "reason": "job_error"}

            return {"status": "pending"}
    finally:
        conn.close()

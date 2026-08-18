"""Transactional PostgreSQL storage for low-quantity notification state."""

from __future__ import annotations

import os
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json, RealDictCursor

try:
    from .low_qty_notification_schema_migration import inspect as inspect_schema
    from .low_qty_schedule import next_scheduled_instant, validate_timezone
except ImportError:
    from low_qty_notification_schema_migration import inspect as inspect_schema
    from low_qty_schedule import next_scheduled_instant, validate_timezone


ROUTE_NUMBER_PATTERN = re.compile(r"^[0-9]{1,10}$")
DISABLED_REASON_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")
COMPLETION_REASONS = {
    "attempts_exhausted",
    "delivery_unknown",
    "no_items",
    "no_valid_token",
    "owner_changed",
    "policy_excluded",
    "window_expired",
}
_CLAIM_PROGRESS = object()


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _pg_connect() -> psycopg2.extensions.connection:
    """Open one bounded, non-autocommit connection for a short state transition."""
    application_name = os.environ.get(
        "POSTGRES_APPLICATION_NAME",
        "routespark-low-qty-notifications",
    ).strip()[:64]
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=_positive_int_env("POSTGRES_PORT", 5432),
        database=os.environ.get("POSTGRES_DB", "routespark"),
        user=os.environ.get("POSTGRES_USER", "routespark"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        connect_timeout=_positive_int_env("POSTGRES_CONNECT_TIMEOUT_SECONDS", 10),
        application_name=application_name or "routespark-low-qty-notifications",
        options=(
            f"-c statement_timeout={_positive_int_env('POSTGRES_STATEMENT_TIMEOUT_MS', 15000)} "
            f"-c lock_timeout={_positive_int_env('POSTGRES_LOCK_TIMEOUT_MS', 5000)} "
            f"-c idle_in_transaction_session_timeout="
            f"{_positive_int_env('POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS', 30000)}"
        ),
    )
    conn.autocommit = False
    return conn


@dataclass(frozen=True)
class EnabledPreference:
    route_number: str
    owner_uid: str
    reminder_minute_local: int
    timezone_name: str
    next_due_at: datetime

    def validated(self) -> "EnabledPreference":
        route_number = str(self.route_number).strip()
        if not isinstance(self.owner_uid, str):
            raise ValueError("owner_uid must be a string")
        owner_uid = self.owner_uid.strip()
        if not ROUTE_NUMBER_PATTERN.fullmatch(route_number):
            raise ValueError("route_number must contain 1 through 10 digits")
        if not owner_uid or len(owner_uid) > 512:
            raise ValueError("owner_uid must contain 1 through 512 characters")
        if (
            isinstance(self.reminder_minute_local, bool)
            or not isinstance(self.reminder_minute_local, int)
            or self.reminder_minute_local not in range(24 * 60)
        ):
            raise ValueError("reminder_minute_local must be in 0 through 1439")
        timezone_name = validate_timezone(self.timezone_name)
        if not isinstance(self.next_due_at, datetime):
            raise ValueError("next_due_at must be a datetime")
        if self.next_due_at.tzinfo is None or self.next_due_at.utcoffset() is None:
            raise ValueError("next_due_at must be timezone-aware")
        return EnabledPreference(
            route_number=route_number,
            owner_uid=owner_uid,
            reminder_minute_local=self.reminder_minute_local,
            timezone_name=timezone_name,
            next_due_at=self.next_due_at.astimezone(timezone.utc),
        )


@dataclass(frozen=True)
class ClaimedExecution:
    route_number: str
    scheduled_local_date: date
    scheduled_for_utc: datetime
    claimed_preference_version: int
    owner_uid: str
    reminder_minute_local: int
    timezone_name: str
    claim_token: str
    attempt_count: int
    computed_payload: Optional[dict[str, Any]] = None
    computed_saps: Optional[list[str]] = None


def _claim_from_rows(preference: dict[str, Any], execution: dict[str, Any]) -> ClaimedExecution:
    return ClaimedExecution(
        route_number=str(execution["route_number"]),
        scheduled_local_date=execution["scheduled_local_date"],
        scheduled_for_utc=execution["scheduled_for_utc"],
        claimed_preference_version=int(execution["claimed_preference_version"]),
        owner_uid=str(execution["owner_uid"]),
        reminder_minute_local=int(preference["reminder_minute_local"]),
        timezone_name=str(preference["timezone"]),
        claim_token=str(execution["claim_token"]),
        attempt_count=int(execution["attempt_count"]),
        computed_payload=execution.get("computed_payload"),
        computed_saps=execution.get("computed_saps"),
    )


UPSERT_ENABLED_PREFERENCE_SQL = """
    INSERT INTO low_qty_notification_preferences (
        route_number,
        owner_uid,
        enabled,
        reminder_minute_local,
        timezone,
        next_due_at,
        preference_version,
        disabled_reason,
        created_at,
        updated_at
    )
    VALUES (%s, %s, TRUE, %s, %s, %s, 1, NULL, NOW(), NOW())
    ON CONFLICT (route_number) DO UPDATE
    SET owner_uid = EXCLUDED.owner_uid,
        enabled = TRUE,
        reminder_minute_local = EXCLUDED.reminder_minute_local,
        timezone = EXCLUDED.timezone,
        next_due_at = CASE
            WHEN low_qty_notification_preferences.owner_uid IS DISTINCT FROM EXCLUDED.owner_uid
              OR low_qty_notification_preferences.enabled IS DISTINCT FROM TRUE
              OR low_qty_notification_preferences.reminder_minute_local IS DISTINCT FROM EXCLUDED.reminder_minute_local
              OR low_qty_notification_preferences.timezone IS DISTINCT FROM EXCLUDED.timezone
            THEN EXCLUDED.next_due_at
            ELSE low_qty_notification_preferences.next_due_at
        END,
        disabled_reason = NULL,
        preference_version = CASE
            WHEN low_qty_notification_preferences.owner_uid IS DISTINCT FROM EXCLUDED.owner_uid
              OR low_qty_notification_preferences.enabled IS DISTINCT FROM TRUE
              OR low_qty_notification_preferences.reminder_minute_local IS DISTINCT FROM EXCLUDED.reminder_minute_local
              OR low_qty_notification_preferences.timezone IS DISTINCT FROM EXCLUDED.timezone
            THEN low_qty_notification_preferences.preference_version + 1
            ELSE low_qty_notification_preferences.preference_version
        END,
        updated_at = NOW()
"""


def _preference_params(preference: EnabledPreference) -> tuple[object, ...]:
    return (
        preference.route_number,
        preference.owner_uid,
        preference.reminder_minute_local,
        preference.timezone_name,
        preference.next_due_at,
    )


def schema_ready(*, connect: Callable[[], object] = _pg_connect) -> bool:
    conn = connect()
    try:
        return bool(inspect_schema(conn).get("ready"))
    finally:
        conn.close()


def load_preference_run_counts(
    *,
    now_utc: datetime,
    connect: Callable[[], object] = _pg_connect,
) -> dict[str, int]:
    """Return bounded run-level scheduling counts without locking due rows."""
    if now_utc.tzinfo is None or now_utc.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FILTER (WHERE enabled = TRUE) AS enabled_count,
                       COUNT(*) FILTER (
                           WHERE enabled = TRUE AND next_due_at <= %s
                       ) AS due_count
                FROM low_qty_notification_preferences
                """,
                (now_utc.astimezone(timezone.utc),),
            )
            row = cur.fetchone()
        conn.rollback()
        return {
            "enabled": int(row[0] if row else 0),
            "due": int(row[1] if row else 0),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def load_enabled_preference_snapshot(
    *,
    connect: Callable[[], object] = _pg_connect,
) -> list[dict[str, Any]]:
    """Read the normalized enabled mirror for an operator coverage check."""
    conn = connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT route_number,
                       owner_uid,
                       reminder_minute_local,
                       timezone
                FROM low_qty_notification_preferences
                WHERE enabled = TRUE
                ORDER BY route_number
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
        conn.rollback()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_enabled_preference(
    preference: EnabledPreference,
    *,
    connect: Callable[[], object] = _pg_connect,
) -> None:
    validated = preference.validated()
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(UPSERT_ENABLED_PREFERENCE_SQL, _preference_params(validated))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def disable_preference(
    route_number: str,
    reason: str,
    *,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    normalized_route = str(route_number).strip()
    if not isinstance(reason, str):
        raise ValueError("disabled reason must be a string")
    normalized_reason = reason.strip().lower()
    if not ROUTE_NUMBER_PATTERN.fullmatch(normalized_route):
        raise ValueError("route_number must contain 1 through 10 digits")
    if not DISABLED_REASON_PATTERN.fullmatch(normalized_reason):
        raise ValueError("disabled reason must be a lowercase diagnostic key")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_preferences
                SET enabled = FALSE,
                    reminder_minute_local = NULL,
                    timezone = NULL,
                    next_due_at = NULL,
                    disabled_reason = %s,
                    preference_version = preference_version + 1,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND (
                      enabled
                      OR reminder_minute_local IS NOT NULL
                      OR timezone IS NOT NULL
                      OR next_due_at IS NOT NULL
                      OR disabled_reason IS DISTINCT FROM %s
                  )
                """,
                (normalized_reason, normalized_route, normalized_reason),
            )
            changed = cur.rowcount > 0
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def disable_claimed_preference(
    claim: ClaimedExecution,
    reason: str,
    *,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    """Disable only the exact preference version and slot owned by a claim."""
    if not isinstance(reason, str):
        raise ValueError("disabled reason must be a string")
    normalized_reason = reason.strip().lower()
    if not DISABLED_REASON_PATTERN.fullmatch(normalized_reason):
        raise ValueError("disabled reason must be a lowercase diagnostic key")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_preferences
                SET enabled = FALSE,
                    reminder_minute_local = NULL,
                    timezone = NULL,
                    next_due_at = NULL,
                    disabled_reason = %s,
                    preference_version = preference_version + 1,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND enabled = TRUE
                  AND preference_version = %s
                  AND next_due_at = %s
                """,
                (
                    normalized_reason,
                    claim.route_number,
                    claim.claimed_preference_version,
                    claim.scheduled_for_utc,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reconcile_complete_enabled_snapshot(
    preferences: Iterable[EnabledPreference],
    *,
    connect: Callable[[], object] = _pg_connect,
) -> dict[str, int]:
    """Atomically upsert one complete snapshot and disable missing enabled rows.

    Firestore iteration and ownership resolution must finish before this
    function is called. It performs no network work while holding SQL locks.
    """
    validated_by_route: dict[str, EnabledPreference] = {}
    for raw_preference in preferences:
        preference = raw_preference.validated()
        if preference.route_number in validated_by_route:
            raise ValueError(f"duplicate authoritative route: {preference.route_number}")
        validated_by_route[preference.route_number] = preference

    ordered_preferences = [validated_by_route[key] for key in sorted(validated_by_route)]
    route_numbers = [preference.route_number for preference in ordered_preferences]
    conn = connect()
    try:
        with conn.cursor() as cur:
            for preference in ordered_preferences:
                cur.execute(UPSERT_ENABLED_PREFERENCE_SQL, _preference_params(preference))

            if route_numbers:
                cur.execute(
                    """
                    UPDATE low_qty_notification_preferences
                    SET enabled = FALSE,
                        reminder_minute_local = NULL,
                        timezone = NULL,
                        next_due_at = NULL,
                        disabled_reason = 'not_in_enabled_snapshot',
                        preference_version = preference_version + 1,
                        updated_at = NOW()
                    WHERE enabled = TRUE
                      AND NOT (route_number = ANY(%s))
                    """,
                    (route_numbers,),
                )
            else:
                cur.execute(
                    """
                    UPDATE low_qty_notification_preferences
                    SET enabled = FALSE,
                        reminder_minute_local = NULL,
                        timezone = NULL,
                        next_due_at = NULL,
                        disabled_reason = 'not_in_enabled_snapshot',
                        preference_version = preference_version + 1,
                        updated_at = NOW()
                    WHERE enabled = TRUE
                    """
                )
            disabled_count = max(cur.rowcount, 0)
        conn.commit()
        return {
            "enabled": len(ordered_preferences),
            "disabled": disabled_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _next_due_for_preference(preference: dict[str, Any], now_utc: datetime) -> datetime:
    _local_date, next_due_at = next_scheduled_instant(
        int(preference["reminder_minute_local"]),
        str(preference["timezone"]),
        after_utc=now_utc,
    )
    return next_due_at


def _advance_preference(
    cur,
    preference: dict[str, Any],
    *,
    now_utc: datetime,
) -> bool:
    next_due_at = _next_due_for_preference(preference, now_utc)
    cur.execute(
        """
        UPDATE low_qty_notification_preferences
        SET next_due_at = %s,
            updated_at = NOW()
        WHERE route_number = %s
          AND enabled = TRUE
          AND preference_version = %s
          AND next_due_at = %s
        """,
        (
            next_due_at,
            preference["route_number"],
            preference["preference_version"],
            preference["next_due_at"],
        ),
    )
    return cur.rowcount == 1


def _close_existing_execution(
    cur,
    *,
    route_number: str,
    scheduled_local_date: date,
    reason: str,
    now_utc: datetime,
) -> None:
    cur.execute(
        """
        UPDATE low_qty_notification_executions
        SET status = 'closed',
            completion_reason = %s,
            completed_at = %s,
            updated_at = NOW()
        WHERE route_number = %s
          AND scheduled_local_date = %s
          AND status NOT IN ('sent', 'closed')
        """,
        (reason, now_utc, route_number, scheduled_local_date),
    )


def _claim_one_due(
    *,
    now_utc: datetime,
    lease_seconds: int = 300,
    late_tolerance_minutes: int = 20,
    max_attempts: int = 3,
    connect: Callable[[], object] = _pg_connect,
) -> object:
    """Select and claim one due route without holding locks across external work."""
    if now_utc.tzinfo is None or now_utc.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")
    if lease_seconds < 300 or lease_seconds > 3600:
        raise ValueError("lease_seconds must be between 300 and 3600")
    if late_tolerance_minutes < 0 or late_tolerance_minutes > 60:
        raise ValueError("late_tolerance_minutes must be between 0 and 60")
    if max_attempts < 1 or max_attempts > 20:
        raise ValueError("max_attempts must be between 1 and 20")

    now_utc = now_utc.astimezone(timezone.utc)
    lease_expires_at = now_utc + timedelta(seconds=lease_seconds)
    conn = connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT route_number,
                       owner_uid,
                       reminder_minute_local,
                       timezone,
                       next_due_at,
                       preference_version
                FROM low_qty_notification_preferences
                WHERE enabled = TRUE
                  AND next_due_at <= %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM low_qty_notification_executions active_execution
                      WHERE active_execution.route_number = low_qty_notification_preferences.route_number
                        AND active_execution.status IN ('processing', 'retryable', 'dispatching')
                        AND active_execution.lease_expires_at > %s
                  )
                ORDER BY next_due_at, route_number
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (now_utc, now_utc),
            )
            preference = cur.fetchone()
            if not preference:
                conn.rollback()
                return None

            scheduled_for_utc = preference["next_due_at"].astimezone(timezone.utc)
            scheduled_local_date = scheduled_for_utc.astimezone(
                ZoneInfo(str(preference["timezone"]))
            ).date()
            age = now_utc - scheduled_for_utc
            within_window = age <= timedelta(minutes=late_tolerance_minutes)
            claim_token = secrets.token_urlsafe(24)

            cur.execute(
                """
                INSERT INTO low_qty_notification_executions (
                    route_number,
                    scheduled_local_date,
                    scheduled_for_utc,
                    claimed_preference_version,
                    owner_uid,
                    status,
                    claim_token,
                    claimed_at,
                    lease_expires_at,
                    attempt_count,
                    completion_reason,
                    completed_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, 1, %s, %s, NOW()
                )
                ON CONFLICT (route_number, scheduled_local_date) DO NOTHING
                RETURNING *
                """,
                (
                    preference["route_number"],
                    scheduled_local_date,
                    scheduled_for_utc,
                    preference["preference_version"],
                    preference["owner_uid"],
                    "processing" if within_window else "closed",
                    claim_token,
                    now_utc,
                    lease_expires_at,
                    None if within_window else "window_expired",
                    None if within_window else now_utc,
                ),
            )
            execution = cur.fetchone()
            if execution:
                if not within_window:
                    _advance_preference(cur, preference, now_utc=now_utc)
                    conn.commit()
                    return _CLAIM_PROGRESS
                conn.commit()
                return _claim_from_rows(preference, execution)

            cur.execute(
                """
                SELECT *
                FROM low_qty_notification_executions
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                FOR UPDATE
                """,
                (preference["route_number"], scheduled_local_date),
            )
            existing = cur.fetchone()
            if not existing:
                raise RuntimeError("execution conflict disappeared during claim")

            existing_status = str(existing["status"])
            if existing_status in {"sent", "closed"}:
                _advance_preference(cur, preference, now_utc=now_utc)
                conn.commit()
                return _CLAIM_PROGRESS

            if existing_status == "dispatching":
                if existing["lease_expires_at"] <= now_utc:
                    _close_existing_execution(
                        cur,
                        route_number=str(preference["route_number"]),
                        scheduled_local_date=scheduled_local_date,
                        reason="delivery_unknown",
                        now_utc=now_utc,
                    )
                    _advance_preference(cur, preference, now_utc=now_utc)
                conn.commit()
                return _CLAIM_PROGRESS

            if not within_window:
                _close_existing_execution(
                    cur,
                    route_number=str(preference["route_number"]),
                    scheduled_local_date=scheduled_local_date,
                    reason="window_expired",
                    now_utc=now_utc,
                )
                _advance_preference(cur, preference, now_utc=now_utc)
                conn.commit()
                return _CLAIM_PROGRESS

            if existing_status == "processing" and existing["lease_expires_at"] > now_utc:
                conn.commit()
                return _CLAIM_PROGRESS

            attempt_count = int(existing["attempt_count"]) + 1
            if attempt_count > max_attempts:
                _close_existing_execution(
                    cur,
                    route_number=str(preference["route_number"]),
                    scheduled_local_date=scheduled_local_date,
                    reason="attempts_exhausted",
                    now_utc=now_utc,
                )
                _advance_preference(cur, preference, now_utc=now_utc)
                conn.commit()
                return _CLAIM_PROGRESS

            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET scheduled_for_utc = %s,
                    claimed_preference_version = %s,
                    owner_uid = %s,
                    status = 'processing',
                    claim_token = %s,
                    claimed_at = %s,
                    lease_expires_at = %s,
                    attempt_count = %s,
                    completion_reason = NULL,
                    completed_at = NULL,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND status IN ('processing', 'retryable')
                RETURNING *
                """,
                (
                    scheduled_for_utc,
                    preference["preference_version"],
                    preference["owner_uid"],
                    claim_token,
                    now_utc,
                    lease_expires_at,
                    attempt_count,
                    preference["route_number"],
                    scheduled_local_date,
                ),
            )
            reclaimed = cur.fetchone()
            if not reclaimed:
                conn.rollback()
                return _CLAIM_PROGRESS
        conn.commit()
        return _claim_from_rows(preference, reclaimed)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def claim_next_due(
    *,
    now_utc: datetime,
    lease_seconds: int = 300,
    late_tolerance_minutes: int = 20,
    max_attempts: int = 3,
    connect: Callable[[], object] = _pg_connect,
    max_terminal_drain: int = 100,
) -> Optional[ClaimedExecution]:
    """Drain terminal due rows and return the next executable claim, if any."""
    if max_terminal_drain < 1 or max_terminal_drain > 1000:
        raise ValueError("max_terminal_drain must be between 1 and 1000")
    for _ in range(max_terminal_drain):
        result = _claim_one_due(
            now_utc=now_utc,
            lease_seconds=lease_seconds,
            late_tolerance_minutes=late_tolerance_minutes,
            max_attempts=max_attempts,
            connect=connect,
        )
        if result is _CLAIM_PROGRESS:
            continue
        return result if isinstance(result, ClaimedExecution) else None
    return None


def store_claim_payload(
    claim: ClaimedExecution,
    *,
    payload: dict[str, Any],
    saps: list[str],
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    normalized_saps = sorted({str(sap).strip() for sap in saps if str(sap).strip()})
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET computed_payload = %s,
                    computed_saps = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = 'processing'
                """,
                (
                    Json(payload),
                    Json(normalized_saps),
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def begin_dispatch(
    claim: ClaimedExecution,
    *,
    now_utc: datetime,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    if now_utc.tzinfo is None or now_utc.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET status = 'dispatching',
                    dispatch_started_at = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = 'processing'
                """,
                (
                    now_utc.astimezone(timezone.utc),
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def record_accepted_tickets(
    claim: ClaimedExecution,
    ticket_ids: list[str],
    *,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    normalized_ticket_ids = sorted(
        {str(ticket_id).strip() for ticket_id in ticket_ids if str(ticket_id).strip()}
    )
    if not normalized_ticket_ids:
        return True
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET accepted_expo_ticket_ids = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = 'dispatching'
                """,
                (
                    Json(normalized_ticket_ids),
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_retryable(
    claim: ClaimedExecution,
    *,
    error: str,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    sanitized_error = str(error).replace("\x00", "")[:500]
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET status = 'retryable',
                    last_error = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = 'processing'
                """,
                (
                    sanitized_error,
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_zero_ticket_retryable(
    claim: ClaimedExecution,
    *,
    error: str,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    """Retry only after a complete Expo response definitively accepted nothing."""
    sanitized_error = str(error).replace("\x00", "")[:500]
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET status = 'retryable',
                    last_error = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = 'dispatching'
                  AND accepted_expo_ticket_ids = '[]'::jsonb
                """,
                (
                    sanitized_error,
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                ),
            )
            changed = cur.rowcount == 1
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def complete_claim(
    claim: ClaimedExecution,
    *,
    status: str,
    reason: str,
    now_utc: datetime,
    accepted_ticket_ids: Optional[list[str]] = None,
    error: Optional[str] = None,
    connect: Callable[[], object] = _pg_connect,
) -> bool:
    """Guard-complete one owned claim and CAS-advance its preference."""
    if status not in {"sent", "closed"}:
        raise ValueError("terminal status must be sent or closed")
    normalized_reason = str(reason).strip().lower()
    if status == "closed" and normalized_reason not in COMPLETION_REASONS:
        raise ValueError("unsupported closed completion reason")
    if status == "sent" and normalized_reason != "accepted":
        raise ValueError("sent completion reason must be accepted")
    if now_utc.tzinfo is None or now_utc.utcoffset() is None:
        raise ValueError("now_utc must be timezone-aware")

    allowed_source_statuses = ("dispatching",) if status == "sent" else (
        "processing",
        "retryable",
        "dispatching",
    )
    ticket_ids = sorted(
        {
            str(ticket_id).strip()
            for ticket_id in (accepted_ticket_ids or [])
            if str(ticket_id).strip()
        }
    )
    if status == "sent" and not ticket_ids:
        raise ValueError("sent completion requires at least one accepted ticket id")
    sanitized_error = None if error is None else str(error).replace("\x00", "")[:500]
    conn = connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE low_qty_notification_executions
                SET status = %s,
                    accepted_expo_ticket_ids = %s,
                    completion_reason = %s,
                    last_error = %s,
                    completed_at = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND scheduled_local_date = %s
                  AND claim_token = %s
                  AND status = ANY(%s)
                RETURNING *
                """,
                (
                    status,
                    Json(ticket_ids),
                    normalized_reason,
                    sanitized_error,
                    now_utc.astimezone(timezone.utc),
                    claim.route_number,
                    claim.scheduled_local_date,
                    claim.claim_token,
                    list(allowed_source_statuses),
                ),
            )
            completed = cur.fetchone()
            if not completed:
                conn.rollback()
                return False

            next_due_at = _next_due_for_preference(
                {
                    "reminder_minute_local": claim.reminder_minute_local,
                    "timezone": claim.timezone_name,
                },
                now_utc.astimezone(timezone.utc),
            )
            cur.execute(
                """
                UPDATE low_qty_notification_preferences
                SET next_due_at = %s,
                    updated_at = NOW()
                WHERE route_number = %s
                  AND enabled = TRUE
                  AND preference_version = %s
                  AND next_due_at = %s
                """,
                (
                    next_due_at,
                    claim.route_number,
                    claim.claimed_preference_version,
                    claim.scheduled_for_utc,
                ),
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

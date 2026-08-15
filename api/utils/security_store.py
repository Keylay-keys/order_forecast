"""Cluster-wide persistence for API abuse controls and security evidence.

PostgreSQL is the authority for IP blocks because the web API runs with
multiple workers on multiple nodes. Process memory is never authoritative.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import logging
import os
from queue import Empty, Full, Queue
import socket
from threading import Lock, Thread
import time
from typing import Any, Callable, Dict, Optional

from psycopg2.extras import RealDictCursor, Json


logger = logging.getLogger("api.security_store")

MAX_SECURITY_EVENT_QUEUE = 5000
SECURITY_EVENT_RETENTION_DAYS = 90
SECURITY_MAINTENANCE_INTERVAL = timedelta(hours=1)
SECURITY_MAINTENANCE_POLL_SECONDS = 300
STORE_SOURCE_INSTANCE = f"{socket.gethostname()}:{os.getpid()}"
_EVENT_QUEUE: Queue["SecurityEvent"] = Queue(maxsize=MAX_SECURITY_EVENT_QUEUE)
_EVENT_WORKER_LOCK = Lock()
_EVENT_WORKER_STARTED = False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validated_ip(value: str) -> str:
    return str(ipaddress.ip_address(str(value or "").strip()))


def _safe_text(value: Any, maximum: int) -> str:
    return str(value or "").replace("\x00", "")[:maximum]


def _json_safe(value: Any) -> Any:
    """Return a bounded JSON-compatible value for JSONB storage."""
    try:
        encoded = json.dumps(value, default=str, ensure_ascii=True)
    except (TypeError, ValueError):
        return {}
    if len(encoded) > 16_384:
        return {"truncated": True, "original_size": len(encoded)}
    return json.loads(encoded)


@dataclass(frozen=True)
class SecurityEvent:
    occurred_at: datetime
    event_type: str
    severity: str
    details: Dict[str, Any]
    ip: Optional[str] = None
    path: Optional[str] = None
    source_instance: str = ""


class PostgresSecurityStore:
    """PostgreSQL-backed, cluster-wide block state."""

    MAX_BAN_DURATION = timedelta(hours=48)
    AUTH_LOCKOUT_THRESHOLDS = (
        (20, timedelta(hours=24), timedelta(hours=24)),
        (10, timedelta(minutes=30), timedelta(minutes=30)),
        (5, timedelta(minutes=5), timedelta(minutes=5)),
        (3, timedelta(minutes=1), timedelta(minutes=1)),
    )

    def __init__(
        self,
        connection_factory: Optional[Callable[[], Any]] = None,
        connection_returner: Optional[Callable[[Any], None]] = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._connection_returner = connection_returner

    def _connection(self):
        if self._connection_factory is not None:
            return self._connection_factory()
        from ..dependencies import get_pg_connection

        return get_pg_connection()

    def _return_connection(self, conn) -> None:
        if self._connection_returner is not None:
            self._connection_returner(conn)
            return
        from ..dependencies import return_pg_connection

        return_pg_connection(conn)

    def verify_schema(self) -> None:
        """Fail deployment startup when the required migration is absent."""
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        to_regclass('public.security_ip_blocks'),
                        to_regclass('public.security_events')
                    """
                )
                row = cur.fetchone()
            conn.rollback()
            if not row or not row[0] or not row[1]:
                raise RuntimeError(
                    "Security schema is not ready; apply the PostgreSQL schema migration before deploying web-api"
                )
        finally:
            self._return_connection(conn)

    def add_block(
        self,
        ip: str,
        reason: str,
        duration: timedelta,
        *,
        permanent: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Atomically add or escalate a block for one canonical IP."""
        canonical_ip = _validated_ip(ip)
        base_duration = min(max(duration, timedelta(seconds=1)), self.MAX_BAN_DURATION)
        now = _utc_now()
        conn = self._connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Serialize all updates for this IP across every pod and worker.
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (canonical_ip,),
                )
                cur.execute(
                    """
                    SELECT hit_count, permanent, blocked_until, first_seen_at
                    FROM security_ip_blocks
                    WHERE ip_address = %s::inet
                    FOR UPDATE
                    """,
                    (canonical_ip,),
                )
                existing = cur.fetchone()
                existing_active = bool(
                    existing
                    and (
                        existing["permanent"]
                        or (
                            existing["blocked_until"] is not None
                            and existing["blocked_until"] > now
                        )
                    )
                )
                hits = int(existing["hit_count"]) + 1 if existing_active else 1
                permanent = bool(permanent or (existing and existing["permanent"]))
                multiplier = 4 if hits >= 3 else 2 if hits == 2 else 1
                effective_duration = min(base_duration * multiplier, self.MAX_BAN_DURATION)
                blocked_until = None if permanent else now + effective_duration
                first_seen_at = existing["first_seen_at"] if existing_active else now

                cur.execute(
                    """
                    INSERT INTO security_ip_blocks (
                        ip_address, reason, hit_count, permanent, blocked_until,
                        first_seen_at, last_seen_at, last_metadata
                    ) VALUES (%s::inet, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ip_address) DO UPDATE SET
                        reason = EXCLUDED.reason,
                        hit_count = EXCLUDED.hit_count,
                        permanent = EXCLUDED.permanent,
                        blocked_until = EXCLUDED.blocked_until,
                        first_seen_at = EXCLUDED.first_seen_at,
                        last_seen_at = EXCLUDED.last_seen_at,
                        last_metadata = EXCLUDED.last_metadata
                    RETURNING ip_address::text AS ip_address, reason, hit_count,
                              permanent, blocked_until, first_seen_at,
                              last_seen_at, last_metadata
                    """,
                    (
                        canonical_ip,
                        _safe_text(reason, 128),
                        hits,
                        permanent,
                        blocked_until,
                        first_seen_at,
                        now,
                        Json(_json_safe(metadata or {})),
                    ),
                )
                row = dict(cur.fetchone())
            conn.commit()
            return self._serialize_block(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            self._return_connection(conn)

    def record_auth_failure(
        self,
        ip: str,
        *,
        reason: str,
        path: str,
        source_instance: str,
    ) -> Optional[Dict[str, Any]]:
        """Atomically count auth failures across the whole worker fleet."""
        canonical_ip = _validated_ip(ip)
        now = _utc_now()
        conn = self._connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"auth:{canonical_ip}",),
                )
                cur.execute(
                    """
                    INSERT INTO security_events (
                        occurred_at, event_type, severity, ip_address,
                        request_path, source_instance, details
                    ) VALUES (%s, 'auth_failure_observed', 'medium', %s::inet, %s, %s, %s)
                    """,
                    (
                        now,
                        canonical_ip,
                        _safe_text(path, 512) or None,
                        _safe_text(source_instance, 255),
                        Json({"reason": _safe_text(reason, 128)}),
                    ),
                )
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE occurred_at > %s - INTERVAL '1 minute') AS one_minute,
                        COUNT(*) FILTER (WHERE occurred_at > %s - INTERVAL '5 minutes') AS five_minutes,
                        COUNT(*) FILTER (WHERE occurred_at > %s - INTERVAL '30 minutes') AS thirty_minutes,
                        COUNT(*) FILTER (WHERE occurred_at > %s - INTERVAL '24 hours') AS one_day
                    FROM security_events
                    WHERE ip_address = %s::inet
                      AND event_type = 'auth_failure_observed'
                      AND occurred_at > %s - INTERVAL '24 hours'
                    """,
                    (now, now, now, now, canonical_ip, now),
                )
                counts = dict(cur.fetchone() or {})
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._return_connection(conn)

        window_counts = {
            timedelta(minutes=1): int(counts.get("one_minute") or 0),
            timedelta(minutes=5): int(counts.get("five_minutes") or 0),
            timedelta(minutes=30): int(counts.get("thirty_minutes") or 0),
            timedelta(hours=24): int(counts.get("one_day") or 0),
        }
        for threshold, window, duration in self.AUTH_LOCKOUT_THRESHOLDS:
            failures = window_counts[window]
            if failures >= threshold:
                return {
                    "failures": failures,
                    "lockout_duration": duration,
                    "window": window,
                }
        return None

    def run_maintenance(self, *, source_instance: str = STORE_SOURCE_INSTANCE) -> Dict[str, Any]:
        """Run at most one cluster-wide retention pass per hour."""
        now = _utc_now()
        conn = self._connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) AS acquired",
                    ("routespark-security-maintenance",),
                )
                lock_row = cur.fetchone() or {}
                if not lock_row.get("acquired"):
                    conn.rollback()
                    return {"status": "locked"}

                cur.execute(
                    """
                    SELECT MAX(occurred_at) AS last_completed_at
                    FROM security_events
                    WHERE event_type = 'security_maintenance_completed'
                    """
                )
                marker = cur.fetchone() or {}
                last_completed_at = marker.get("last_completed_at")
                if (
                    last_completed_at is not None
                    and last_completed_at > now - SECURITY_MAINTENANCE_INTERVAL
                ):
                    conn.rollback()
                    return {
                        "status": "not_due",
                        "last_completed_at": last_completed_at.isoformat(),
                    }

                cur.execute(
                    """
                    DELETE FROM security_ip_blocks
                    WHERE NOT permanent AND blocked_until <= %s
                    """,
                    (now,),
                )
                expired_blocks = cur.rowcount
                cur.execute(
                    """
                    DELETE FROM security_events
                    WHERE occurred_at < %s - (%s * INTERVAL '1 day')
                    """,
                    (now, SECURITY_EVENT_RETENTION_DAYS),
                )
                expired_events = cur.rowcount
                cur.execute(
                    """
                    INSERT INTO security_events (
                        occurred_at, event_type, severity, request_path,
                        source_instance, details
                    ) VALUES (%s, 'security_maintenance_completed', 'low', NULL, %s, %s)
                    """,
                    (
                        now,
                        _safe_text(source_instance, 255),
                        Json(
                            {
                                "expired_blocks": expired_blocks,
                                "expired_events": expired_events,
                            }
                        ),
                    ),
                )
            conn.commit()
            return {
                "status": "completed",
                "expired_blocks": expired_blocks,
                "expired_events": expired_events,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            self._return_connection(conn)

    def get_block(self, ip: str) -> Optional[Dict[str, Any]]:
        canonical_ip = _validated_ip(ip)
        conn = self._connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT ip_address::text AS ip_address, reason, hit_count,
                           permanent, blocked_until, first_seen_at,
                           last_seen_at, last_metadata
                    FROM security_ip_blocks
                    WHERE ip_address = %s::inet
                      AND (permanent OR blocked_until > CURRENT_TIMESTAMP)
                    """,
                    (canonical_ip,),
                )
                row = cur.fetchone()
            conn.rollback()
            return self._serialize_block(dict(row)) if row else None
        finally:
            self._return_connection(conn)

    def remove_block(self, ip: str) -> bool:
        canonical_ip = _validated_ip(ip)
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM security_ip_blocks WHERE ip_address = %s::inet",
                    (canonical_ip,),
                )
                removed = cur.rowcount > 0
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise
        finally:
            self._return_connection(conn)

    def cleanup_expired(self) -> int:
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM security_ip_blocks
                    WHERE NOT permanent AND blocked_until <= CURRENT_TIMESTAMP
                    """
                )
                removed = cur.rowcount
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise
        finally:
            self._return_connection(conn)

    def get_stats(self) -> Dict[str, Any]:
        conn = self._connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT ip_address::text AS ip_address, reason, hit_count,
                           permanent, blocked_until, first_seen_at,
                           last_seen_at, last_metadata
                    FROM security_ip_blocks
                    WHERE permanent OR blocked_until > CURRENT_TIMESTAMP
                    ORDER BY hit_count DESC, last_seen_at DESC
                    LIMIT 10
                    """
                )
                rows = [self._serialize_block(dict(row)) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM security_ip_blocks
                    WHERE permanent OR blocked_until > CURRENT_TIMESTAMP
                    """
                )
                count_row = cur.fetchone() or {}
                active_count = int(count_row.get("count") or 0)
            conn.rollback()
            return {
                "active_blocks": active_count,
                "top_offenders": [
                    {
                        "ip": row["ip"],
                        "hits": row["hits"],
                        "reason": row["reason"],
                        "expires": row["until"],
                        "permanent": row["permanent"],
                        "last_seen_at": row["last_seen_at"],
                    }
                    for row in rows
                ],
            }
        finally:
            self._return_connection(conn)

    @staticmethod
    def _serialize_block(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ip": str(row.get("ip_address") or ""),
            "until": row["blocked_until"].isoformat() if row.get("blocked_until") else None,
            "reason": str(row.get("reason") or ""),
            "hits": int(row.get("hit_count") or 0),
            "permanent": bool(row.get("permanent")),
            "first_seen_at": row["first_seen_at"].isoformat() if row.get("first_seen_at") else None,
            "last_seen_at": row["last_seen_at"].isoformat() if row.get("last_seen_at") else None,
            "last_metadata": row.get("last_metadata") or {},
        }


def record_security_event(conn, event: SecurityEvent) -> None:
    """Persist one security event; periodic maintenance owns retention."""
    canonical_ip = _validated_ip(event.ip) if event.ip else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO security_events (
                occurred_at, event_type, severity, ip_address, request_path,
                source_instance, details
            ) VALUES (%s, %s, %s, %s::inet, %s, %s, %s)
            """,
            (
                event.occurred_at,
                _safe_text(event.event_type, 64),
                _safe_text(event.severity, 16),
                canonical_ip,
                _safe_text(event.path, 512) or None,
                _safe_text(event.source_instance, 255),
                Json(_json_safe(event.details)),
            ),
        )
    conn.commit()


def _record_queued_event(event: SecurityEvent) -> None:
    from ..dependencies import get_pg_connection, return_pg_connection

    conn = None
    try:
        conn = get_pg_connection()
        record_security_event(conn, event)
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            return_pg_connection(conn)


def _event_worker() -> None:
    next_maintenance_at = 0.0
    while True:
        if time.monotonic() >= next_maintenance_at:
            try:
                result = security_store.run_maintenance()
                if result.get("status") == "completed":
                    logger.info("Security maintenance completed: %s", result)
            except Exception:
                logger.exception("Security maintenance failed")
            next_maintenance_at = time.monotonic() + SECURITY_MAINTENANCE_POLL_SECONDS
        try:
            event = _EVENT_QUEUE.get(timeout=1)
        except Empty:
            continue
        try:
            _record_queued_event(event)
        except Exception:
            logger.exception("Failed to persist security event; JSON stdout/file evidence remains available")
        finally:
            _EVENT_QUEUE.task_done()


def _ensure_event_worker_started() -> None:
    global _EVENT_WORKER_STARTED
    if _EVENT_WORKER_STARTED:
        return
    with _EVENT_WORKER_LOCK:
        if _EVENT_WORKER_STARTED:
            return
        Thread(target=_event_worker, name="security-event-writer", daemon=True).start()
        _EVENT_WORKER_STARTED = True


def enqueue_security_event(event: SecurityEvent) -> bool:
    """Queue durable evidence without delaying or failing the request."""
    _ensure_event_worker_started()
    try:
        _EVENT_QUEUE.put_nowait(event)
        return True
    except Full:
        logger.critical("Security event queue full; PostgreSQL copy was not queued")
        return False


def start_security_maintenance() -> None:
    """Start the security event/maintenance worker during application startup."""
    _ensure_event_worker_started()


def flush_security_events(timeout_seconds: float = 5.0) -> bool:
    """Wait briefly for queued evidence during graceful shutdown."""
    deadline = time.monotonic() + max(timeout_seconds, 0)
    while _EVENT_QUEUE.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.05)
    return _EVENT_QUEUE.unfinished_tasks == 0


security_store = PostgresSecurityStore()

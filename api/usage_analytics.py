"""Server-side API usage rollups for the internal activity dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import logging
import os
from queue import Empty, Full, Queue
import re
from threading import Lock, Thread
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from uuid import UUID

from psycopg2.extras import RealDictCursor


logger = logging.getLogger("api.usage_analytics")

_ROUTE_PATTERN = re.compile(r"^\d{1,10}$")
_ROUTE_PATH_PATTERN = re.compile(r"/routes/(\d{1,10})(?:/|$)")
_SAFE_ENDPOINT_PATTERN = re.compile(r"^/api/[A-Za-z0-9_./{}:-]{0,195}$")
_SAFE_ERROR_CODE_PATTERN = re.compile(r"^[A-Z0-9_.:-]{1,64}$")
_SAFE_METHODS = {"DELETE", "GET", "HEAD", "PATCH", "POST", "PUT"}
_IDENTITY_CACHE_TTL_SECONDS = 600
_REQUEST_QUEUE: Queue["ApiUsageRequest"] = Queue(maxsize=2000)
_WORKER_LOCK = Lock()
_WORKER_STARTED = False
_IDENTITY_CACHE: Dict[Tuple[str, str], Tuple[float, "ActorContext"]] = {}


_FEATURE_PREFIXES: Tuple[Tuple[str, str], ...] = (
    ("/api/routes/", "dashboard"),
    ("/api/catalog/starter", "reference_catalog"),
    ("/api/catalog/items/search", "reference_catalog"),
    ("/api/orders", "orders"),
    ("/api/history", "history"),
    ("/api/stores", "stores"),
    ("/api/catalog", "catalog"),
    ("/api/low-quantity", "low_quantity"),
    ("/api/pos", "pos"),
    ("/api/credits", "credits"),
    ("/api/deliveries", "deliveries"),
    ("/api/schedule", "schedule"),
    ("/api/settings", "settings"),
    ("/api/team-tasks", "team_tasks"),
    ("/api/team", "team"),
    ("/api/billing", "billing"),
    ("/api/archive-exports", "archive_exports"),
    ("/api/forecast", "forecast"),
    # Keep specific transfer paths before the base history endpoint. Matching
    # stops at the first prefix, and the split is how rollout proves that idle
    # clients no longer generate ledger reads.
    ("/api/transfers/ledger", "transfer_ledger_read"),
    ("/api/transfers/reserve", "transfer_reserve"),
    ("/api/transfers/create", "transfer_create"),
    ("/api/transfers", "transfer_history_read"),
    ("/api/auth", "auth"),
)


@dataclass(frozen=True)
class ApiUsageRequest:
    uid: str
    path: str
    status_code: int
    route_hint: str = ""
    method: str = "GET"
    endpoint: str = ""
    error_code: str = ""
    request_id: str = ""


@dataclass(frozen=True)
class ActorContext:
    actor_hash: str
    route_number: str
    actor_role: str


def build_actor_hash(uid: str, secret: Optional[str] = None) -> str:
    """Return a stable pseudonymous key without persisting Firebase UID."""

    key = secret if secret is not None else os.environ.get("USAGE_ANALYTICS_HASH_KEY", "")
    if len(key) < 32:
        raise RuntimeError("USAGE_ANALYTICS_HASH_KEY must contain at least 32 characters")
    return hmac.new(key.encode("utf-8"), uid.encode("utf-8"), hashlib.sha256).hexdigest()


def classify_api_feature(path: str) -> Optional[str]:
    """Map an API path to a stable reporting bucket."""

    normalized = path.rstrip("/") or "/"
    if normalized in {"/", "/api", "/api/health"}:
        return None
    if normalized.startswith("/api/admin/usage"):
        return None
    for prefix, feature_key in _FEATURE_PREFIXES:
        if normalized == prefix.rstrip("/") or normalized.startswith(prefix):
            return feature_key
    return None


def extract_route_hint(path: str, query: Mapping[str, str]) -> str:
    """Extract only explicit, validated route identifiers from a request."""

    for key in ("route", "route_number", "routeNumber"):
        candidate = str(query.get(key) or "").strip()
        if _ROUTE_PATTERN.fullmatch(candidate):
            return candidate
    match = _ROUTE_PATH_PATTERN.search(path)
    return match.group(1) if match else ""


def normalize_endpoint_path(path: str, route_template: str = "") -> str:
    """Return a route template without retaining resource identifiers."""

    candidate = str(route_template or "").strip()
    if _SAFE_ENDPOINT_PATTERN.fullmatch(candidate):
        return candidate

    normalized = str(path or "").strip().rstrip("/") or "/"
    for prefix, _feature_key in _FEATURE_PREFIXES:
        if normalized == prefix.rstrip("/") or normalized.startswith(prefix):
            return f"{prefix.rstrip('/')}/*"
    return "/api/unmatched"


def normalize_error_code(value: str, status_code: int) -> str:
    candidate = str(value or "").strip().upper()
    if _SAFE_ERROR_CODE_PATTERN.fullmatch(candidate):
        return candidate
    return f"HTTP_{int(status_code)}"


def normalize_request_id(value: str) -> Optional[str]:
    try:
        return str(UUID(str(value or "").strip()))
    except (TypeError, ValueError):
        return None


def record_api_request(
    conn,
    *,
    actor_hash: str,
    route_number: str,
    actor_role: str,
    feature_key: str,
    status_code: int,
    method: str = "GET",
    endpoint: str = "",
    error_code: str = "",
    request_id: str = "",
    now: Optional[datetime] = None,
) -> None:
    """Increment a daily rollup and retain safe error metadata for 30 days."""

    observed_at = now or datetime.now(timezone.utc)
    error_count = 1 if status_code >= 400 else 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_usage_daily (
                    activity_date,
                    actor_hash,
                    route_number,
                    actor_role,
                    feature_key,
                    request_count,
                    error_count,
                    last_status,
                    last_seen_at
                ) VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s)
                ON CONFLICT (activity_date, actor_hash, route_number, feature_key)
                DO UPDATE SET
                    actor_role = EXCLUDED.actor_role,
                    request_count = api_usage_daily.request_count + 1,
                    error_count = api_usage_daily.error_count + EXCLUDED.error_count,
                    last_status = EXCLUDED.last_status,
                    last_seen_at = GREATEST(api_usage_daily.last_seen_at, EXCLUDED.last_seen_at)
                """,
                (
                    observed_at.date(),
                    actor_hash,
                    route_number,
                    actor_role,
                    feature_key,
                    error_count,
                    status_code,
                    observed_at,
                ),
            )
            normalized_request_id = normalize_request_id(request_id)
            if status_code >= 400 and normalized_request_id:
                normalized_method = str(method or "").strip().upper()
                if normalized_method not in _SAFE_METHODS:
                    normalized_method = "GET"
                cur.execute(
                    """
                    INSERT INTO api_usage_errors (
                        occurred_at,
                        route_number,
                        actor_role,
                        feature_key,
                        method,
                        endpoint,
                        status_code,
                        error_code,
                        request_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::uuid)
                    ON CONFLICT (request_id) DO NOTHING
                    """,
                    (
                        observed_at,
                        route_number,
                        actor_role,
                        feature_key,
                        normalized_method,
                        normalize_endpoint_path("", endpoint),
                        status_code,
                        normalize_error_code(error_code, status_code),
                        normalized_request_id,
                    ),
                )
                cur.execute(
                    "DELETE FROM api_usage_errors WHERE occurred_at < %s - INTERVAL '30 days'",
                    (observed_at,),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _resolve_actor_context(uid: str, route_hint: str) -> Optional[ActorContext]:
    """Resolve route and owner/member role from authoritative Firebase data."""

    from .dependencies import _active_route_for_user, _is_owner_for_route, get_firestore, has_access_to_route

    cache_key = (uid, route_hint)
    cached = _IDENTITY_CACHE.get(cache_key)
    now = time.monotonic()
    if cached and cached[0] > now:
        return cached[1]

    db = get_firestore()
    snapshot = db.collection("users").document(uid).get()
    if not snapshot.exists:
        return None
    user_data = snapshot.to_dict() or {}
    route_number = route_hint if route_hint and has_access_to_route(user_data, route_hint) else _active_route_for_user(user_data)
    if not route_number:
        return None

    context = ActorContext(
        actor_hash=build_actor_hash(uid),
        route_number=route_number,
        actor_role="owner" if _is_owner_for_route(user_data, route_number) else "team_member",
    )
    _IDENTITY_CACHE[cache_key] = (now + _IDENTITY_CACHE_TTL_SECONDS, context)
    return context


def _record_queued_request(item: ApiUsageRequest) -> None:
    feature_key = classify_api_feature(item.path)
    if feature_key is None:
        return
    context = _resolve_actor_context(item.uid, item.route_hint)
    if context is None:
        return

    from .dependencies import get_pg_connection, return_pg_connection

    conn = None
    try:
        conn = get_pg_connection()
        record_api_request(
            conn,
            actor_hash=context.actor_hash,
            route_number=context.route_number,
            actor_role=context.actor_role,
            feature_key=feature_key,
            status_code=item.status_code,
            method=item.method,
            endpoint=item.endpoint,
            error_code=item.error_code,
            request_id=item.request_id,
        )
    finally:
        if conn is not None:
            return_pg_connection(conn)


def _usage_worker() -> None:
    while True:
        try:
            item = _REQUEST_QUEUE.get(timeout=1)
        except Empty:
            continue
        try:
            _record_queued_request(item)
        except Exception:
            logger.exception("Failed to record API usage")
        finally:
            _REQUEST_QUEUE.task_done()


def _ensure_worker_started() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    with _WORKER_LOCK:
        if _WORKER_STARTED:
            return
        Thread(target=_usage_worker, name="api-usage-writer", daemon=True).start()
        _WORKER_STARTED = True


def enqueue_api_request(item: ApiUsageRequest) -> bool:
    """Queue analytics without delaying or failing the user request."""

    if not item.uid or classify_api_feature(item.path) is None:
        return False
    _ensure_worker_started()
    try:
        _REQUEST_QUEUE.put_nowait(item)
        return True
    except Full:
        logger.warning("API usage queue full; dropping one analytics record")
        return False


def _rows(cur) -> List[Dict[str, Any]]:
    return [dict(row) for row in cur.fetchall()]


def _serialize(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _serialize_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{key: _serialize(value) for key, value in row.items()} for row in rows]


def get_usage_summary(
    conn,
    *,
    days: int,
    route_number: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return route/role aggregates without exposing actor identifiers."""

    end_date = (now or datetime.now(timezone.utc)).date()
    start_date = end_date - timedelta(days=days - 1)
    route_clause = " AND route_number = %s" if route_number else ""
    params: List[Any] = [start_date, end_date]
    if route_number:
        params.append(route_number)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(SUM(request_count), 0)::BIGINT AS "requestCount",
                COALESCE(SUM(error_count), 0)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            """,
            params,
        )
        totals = dict(cur.fetchone() or {})

        cur.execute(
            f"""
            SELECT
                feature_key AS "featureKey",
                SUM(request_count)::BIGINT AS "requestCount",
                SUM(error_count)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY feature_key
            ORDER BY "uniqueUsers" DESC, "requestCount" DESC, feature_key
            """,
            params,
        )
        features = _rows(cur)

        cur.execute(
            f"""
            SELECT
                COALESCE(SUM(request_count), 0)::BIGINT AS "requestCount",
                COALESCE(SUM(error_count), 0)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
              AND (feature_key = 'transfers' OR feature_key LIKE 'transfer_%%')
            """,
            params,
        )
        transfer_rollup = dict(cur.fetchone() or {})

        cur.execute(
            f"""
            SELECT
                activity_date AS "date",
                SUM(request_count)::BIGINT AS "requestCount",
                SUM(error_count)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY activity_date
            ORDER BY activity_date
            """,
            params,
        )
        trend = _rows(cur)

        cur.execute(
            f"""
            SELECT
                route_number AS "routeNumber",
                SUM(request_count)::BIGINT AS "requestCount",
                SUM(error_count)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers",
                COUNT(DISTINCT feature_key)::INTEGER AS "featureCount",
                COUNT(DISTINCT activity_date)::INTEGER AS "activeDays",
                MIN(activity_date) AS "firstSeenDate",
                MAX(last_seen_at) AS "lastSeenAt"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY route_number
            ORDER BY "errorCount" DESC, "requestCount" DESC, route_number
            LIMIT 500
            """,
            params,
        )
        route_summaries = _rows(cur)

        cur.execute(
            f"""
            SELECT
                route_number AS "routeNumber",
                feature_key AS "featureKey",
                SUM(request_count)::BIGINT AS "requestCount",
                SUM(error_count)::BIGINT AS "errorCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM api_usage_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY route_number, feature_key
            ORDER BY "uniqueUsers" DESC, "requestCount" DESC, route_number, feature_key
            LIMIT 500
            """,
            params,
        )
        route_features = _rows(cur)

        error_params: List[Any] = [start_date, end_date]
        error_route_clause = ""
        if route_number:
            error_route_clause = " AND route_number = %s"
            error_params.append(route_number)
        cur.execute(
            f"""
            SELECT
                occurred_at AS "occurredAt",
                route_number AS "routeNumber",
                actor_role AS "actorRole",
                feature_key AS "featureKey",
                method,
                endpoint,
                status_code AS "statusCode",
                error_code AS "errorCode",
                request_id::TEXT AS "requestId"
            FROM api_usage_errors
            WHERE occurred_at >= %s::DATE
              AND occurred_at < (%s::DATE + INTERVAL '1 day'){error_route_clause}
            ORDER BY occurred_at DESC
            LIMIT 201
            """,
            error_params,
        )
        recent_errors = _rows(cur)

    return {
        "range": {"days": days, "startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
        "totals": {key: int(value or 0) for key, value in totals.items()},
        "features": _serialize_rows(features),
        "transferRollup": {
            key: int(value or 0)
            for key, value in transfer_rollup.items()
        },
        "trend": _serialize_rows(trend),
        "routeSummaries": _serialize_rows(route_summaries),
        "routeFeatures": _serialize_rows(route_features),
        "recentErrors": _serialize_rows(recent_errors[:200]),
        "errorsTruncated": len(recent_errors) > 200,
    }

"""Compact Postgres-backed product usage analytics."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import os
from typing import Any, Dict, Iterable, List, Optional

from psycopg2.extras import RealDictCursor


ALLOWED_USAGE_FEATURES = frozenset(
    {
        "app_session",
        "dashboard",
        "item_lookup",
        "orders",
        "catalog",
        "stores",
        "scanner_load",
        "scanner_pos",
        "low_quantity",
        "order_adjustments",
        "calendar",
        "team_tasks",
        "reminders",
        "notes",
    }
)


def build_actor_hash(uid: str, secret: Optional[str] = None) -> str:
    """Return a stable pseudonymous actor key without persisting Firebase UID."""

    key = secret if secret is not None else os.environ.get("USAGE_ANALYTICS_HASH_KEY", "")
    if len(key) < 32:
        raise RuntimeError("USAGE_ANALYTICS_HASH_KEY must contain at least 32 characters")
    return hmac.new(key.encode("utf-8"), uid.encode("utf-8"), hashlib.sha256).hexdigest()


def record_usage_batch(
    conn,
    *,
    batch_id: str,
    actor_hash: str,
    route_number: str,
    actor_role: str,
    access_tier: str,
    platform: str,
    app_version: Optional[str],
    events: Iterable[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> bool:
    """Atomically apply one idempotent batch to the daily rollup."""

    observed_at = now or datetime.now(timezone.utc)
    activity_date = observed_at.date()
    grouped: Dict[str, int] = {}
    for event in events:
        feature_key = str(event.get("feature") or "").strip()
        if feature_key not in ALLOWED_USAGE_FEATURES:
            raise ValueError(f"Unsupported usage feature: {feature_key}")
        count = int(event.get("count") or 1)
        if count < 1 or count > 100:
            raise ValueError("Usage event count must be between 1 and 100")
        grouped[feature_key] = grouped.get(feature_key, 0) + count

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO usage_event_batches (batch_id, actor_hash, received_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (batch_id) DO NOTHING
                RETURNING batch_id
                """,
                (batch_id, actor_hash, observed_at),
            )
            if cur.fetchone() is None:
                conn.rollback()
                return False

            cur.execute(
                "DELETE FROM usage_event_batches WHERE received_at < %s",
                (observed_at - timedelta(days=120),),
            )

            for feature_key, count in grouped.items():
                cur.execute(
                    """
                    INSERT INTO usage_activity_daily (
                        activity_date,
                        actor_hash,
                        route_number,
                        actor_role,
                        access_tier,
                        feature_key,
                        event_count,
                        platform,
                        app_version,
                        last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (activity_date, actor_hash, route_number, feature_key)
                    DO UPDATE SET
                        actor_role = EXCLUDED.actor_role,
                        access_tier = EXCLUDED.access_tier,
                        event_count = usage_activity_daily.event_count + EXCLUDED.event_count,
                        platform = EXCLUDED.platform,
                        app_version = EXCLUDED.app_version,
                        last_seen_at = GREATEST(usage_activity_daily.last_seen_at, EXCLUDED.last_seen_at)
                    """,
                    (
                        activity_date,
                        actor_hash,
                        route_number,
                        actor_role,
                        access_tier,
                        feature_key,
                        count,
                        platform,
                        app_version,
                        observed_at,
                    ),
                )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


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
    """Return aggregate activity for the admin widget."""

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
                COALESCE(SUM(event_count), 0)::BIGINT AS "eventCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM usage_activity_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            """,
            params,
        )
        totals = dict(cur.fetchone() or {})

        cur.execute(
            f"""
            SELECT
                feature_key AS "featureKey",
                SUM(event_count)::BIGINT AS "eventCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE access_tier = 'paid')::INTEGER AS "paidUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE access_tier = 'trial')::INTEGER AS "trialUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE access_tier = 'free')::INTEGER AS "freeUsers"
            FROM usage_activity_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY feature_key
            ORDER BY "uniqueUsers" DESC, "eventCount" DESC, feature_key
            """,
            params,
        )
        features = _rows(cur)

        cur.execute(
            f"""
            SELECT
                activity_date AS "date",
                SUM(event_count)::BIGINT AS "eventCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT route_number)::INTEGER AS "uniqueRoutes"
            FROM usage_activity_daily
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
                feature_key AS "featureKey",
                SUM(event_count)::BIGINT AS "eventCount",
                COUNT(DISTINCT actor_hash)::INTEGER AS "uniqueUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'owner')::INTEGER AS "ownerUsers",
                COUNT(DISTINCT actor_hash) FILTER (WHERE actor_role = 'team_member')::INTEGER AS "teamMemberUsers"
            FROM usage_activity_daily
            WHERE activity_date BETWEEN %s AND %s{route_clause}
            GROUP BY route_number, feature_key
            ORDER BY "uniqueUsers" DESC, "eventCount" DESC, route_number, feature_key
            LIMIT 500
            """,
            params,
        )
        route_features = _rows(cur)

    return {
        "range": {"days": days, "startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
        "totals": {key: int(value or 0) for key, value in totals.items()},
        "features": _serialize_rows(features),
        "trend": _serialize_rows(trend),
        "routeFeatures": _serialize_rows(route_features),
    }

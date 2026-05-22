"""Shared retrain-readiness helpers for finalize and daemon paths.

This module is the single source of truth for:
- cycle completeness in the trailing 7-day window
- per-schedule non-holiday order minimums
- route-level readiness for retraining
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from .pg_utils import fetch_all as _fetch_all_global
    from .pg_utils import fetch_one as _fetch_one_global
except ImportError:
    from pg_utils import fetch_all as _fetch_all_global
    from pg_utils import fetch_one as _fetch_one_global


DEFAULT_MIN_NON_HOLIDAY_ORDERS_FOR_RETRAIN = int(
    os.environ.get("FORECAST_MIN_SCHEDULE_ORDERS_FOR_RETRAIN", "7")
)


def _utc_now(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _fetch_all(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> list[dict]:
    if conn is None:
        return _fetch_all_global(sql, params)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, list(params) if params else None)
        return [dict(row) for row in cur.fetchall()]


def _fetch_one(
    sql: str,
    params: Optional[Iterable[Any]] = None,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> Optional[dict]:
    if conn is None:
        return _fetch_one_global(sql, params)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, list(params) if params else None)
        row = cur.fetchone()
        return dict(row) if row else None


def check_cycle_complete(
    route_number: str,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
    now: Optional[datetime] = None,
) -> dict:
    """Check if the current trailing 7-day order cycle is complete for a route."""
    try:
        schedules = _fetch_all(
            """
            SELECT schedule_key, order_day, delivery_day
            FROM user_schedules
            WHERE route_number = %s AND is_active = TRUE
            """,
            [route_number],
            conn=conn,
        )
        if not schedules:
            return {"status": "no_schedules", "schedules": [], "completed": [], "missing": []}

        one_week_ago = (_utc_now(now) - timedelta(days=7)).date().isoformat()

        completed = []
        missing = []

        for sched in schedules:
            schedule_key = sched["schedule_key"]
            order_result = _fetch_one(
                """
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND order_date >= %s
                """,
                [route_number, schedule_key, one_week_ago],
                conn=conn,
            )
            cnt = order_result.get("cnt", 0) if order_result else 0
            if cnt > 0:
                completed.append(schedule_key)
            else:
                missing.append(schedule_key)

        all_schedule_keys = [s["schedule_key"] for s in schedules]
        return {
            "status": "complete" if not missing else "incomplete",
            "schedules": all_schedule_keys,
            "completed": completed,
            "missing": missing,
            "window_start": one_week_ago,
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def get_total_order_count(
    route_number: str,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
    exclude_holidays: bool = True,
) -> int:
    try:
        if exclude_holidays:
            result = _fetch_one(
                """
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND COALESCE(is_holiday_week, FALSE) = FALSE
                """,
                [route_number],
                conn=conn,
            )
        else:
            result = _fetch_one(
                """
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                """,
                [route_number],
                conn=conn,
            )
        return result.get("cnt", 0) if result else 0
    except Exception:
        return 0


def get_order_count(
    route_number: str,
    schedule_key: str,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
    exclude_holidays: bool = True,
) -> int:
    try:
        if exclude_holidays:
            result = _fetch_one(
                """
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND COALESCE(is_holiday_week, FALSE) = FALSE
                """,
                [route_number, schedule_key],
                conn=conn,
            )
        else:
            result = _fetch_one(
                """
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
                """,
                [route_number, schedule_key],
                conn=conn,
            )
        return result.get("cnt", 0) if result else 0
    except Exception:
        return 0


def evaluate_retrain_readiness(
    route_number: str,
    *,
    conn: Optional[psycopg2.extensions.connection] = None,
    now: Optional[datetime] = None,
    min_non_holiday_orders_for_retrain: int = DEFAULT_MIN_NON_HOLIDAY_ORDERS_FOR_RETRAIN,
) -> Dict[str, Any]:
    """Evaluate whether a route is ready for retraining."""
    cycle = check_cycle_complete(route_number, conn=conn, now=now)
    total_orders = get_total_order_count(route_number, conn=conn, exclude_holidays=True)

    result: Dict[str, Any] = {
        "route_number": str(route_number),
        "min_non_holiday_orders_for_retrain": int(min_non_holiday_orders_for_retrain),
        "total_non_holiday_orders": int(total_orders),
        "cycle": cycle,
        "schedule_counts": {},
        "has_enough_data": False,
        "ready_for_retrain": False,
    }

    if cycle.get("status") in {"no_schedules", "error"}:
        return result

    has_enough_data = True
    schedule_counts: Dict[str, Dict[str, Any]] = {}
    for schedule_key in cycle.get("schedules", []):
        non_holiday_count = get_order_count(
            route_number,
            schedule_key,
            conn=conn,
            exclude_holidays=True,
        )
        total_count = get_order_count(
            route_number,
            schedule_key,
            conn=conn,
            exclude_holidays=False,
        )
        holiday_excluded = max(0, total_count - non_holiday_count)
        meets_minimum = non_holiday_count >= int(min_non_holiday_orders_for_retrain)
        if not meets_minimum:
            has_enough_data = False

        schedule_counts[str(schedule_key)] = {
            "non_holiday_orders": int(non_holiday_count),
            "total_orders": int(total_count),
            "holiday_excluded_orders": int(holiday_excluded),
            "meets_minimum": bool(meets_minimum),
        }

    result["schedule_counts"] = schedule_counts
    result["has_enough_data"] = has_enough_data
    result["ready_for_retrain"] = (
        cycle.get("status") == "complete" and has_enough_data
    )
    return result

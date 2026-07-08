#!/usr/bin/env python3
"""Dry-run/apply migration for schedule offset columns.

Default mode is read-only: it simulates the additive user_schedules migration in
a transaction-local temp table and reports derived offsets/mismatches. Use
--apply only in staging or an approved rollout window.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from schedule_cycle import normalize_order_cycle  # noqa: E402


SCHEDULE_COLUMNS = {
    "load_offset_days": "INTEGER",
    "delivery_offset_days": "INTEGER",
    "schedule_version": "INTEGER DEFAULT 2",
    "needs_schedule_review": "BOOLEAN DEFAULT FALSE",
}


def get_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "routespark"),
        user=os.environ.get("POSTGRES_USER", "routespark"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    conn.autocommit = False
    return conn


def get_columns(cur, table_name: str = "user_schedules") -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        [table_name],
    )
    return {
        str(row["column_name"] if isinstance(row, dict) else row[0])
        for row in cur.fetchall()
    }


def add_columns_sql(table_name: str) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column} {definition}"
        for column, definition in SCHEDULE_COLUMNS.items()
    ]


def backfill_sql(table_name: str, route_filter_sql: str = "") -> str:
    return f"""
        WITH normalized AS (
            SELECT
                id,
                CASE
                    WHEN load_day - order_day < 0 THEN load_day - order_day + 7
                    ELSE load_day - order_day
                END AS load_offset,
                CASE
                    WHEN delivery_day - order_day <= 0 THEN delivery_day - order_day + 7
                    ELSE delivery_day - order_day
                END AS delivery_base
            FROM {table_name}
            WHERE (
                    load_offset_days IS NULL
                 OR delivery_offset_days IS NULL
                 OR schedule_version IS NULL
                 OR needs_schedule_review IS NULL
            )
            {route_filter_sql}
        ),
        final_offsets AS (
            SELECT
                id,
                load_offset,
                CASE
                    WHEN delivery_base < load_offset THEN delivery_base + 7
                    ELSE delivery_base
                END AS delivery_offset
            FROM normalized
        )
        UPDATE {table_name} AS us
        SET
            load_offset_days = COALESCE(us.load_offset_days, fo.load_offset),
            delivery_offset_days = COALESCE(us.delivery_offset_days, fo.delivery_offset),
            schedule_version = COALESCE(us.schedule_version, 2),
            needs_schedule_review = COALESCE(us.needs_schedule_review, FALSE)
        FROM final_offsets fo
        WHERE us.id = fo.id
    """


def mismatch_sql(table_name: str, route_filter_sql: str = "", *, parameterized: bool = False) -> str:
    modulo = "%%" if parameterized else "%"
    return f"""
        SELECT
            id,
            route_number,
            user_id,
            order_day,
            load_day,
            delivery_day,
            load_offset_days,
            delivery_offset_days
        FROM {table_name}
        WHERE is_active = TRUE
          AND load_offset_days IS NOT NULL
          AND delivery_offset_days IS NOT NULL
          AND (
              ((((order_day - 1 + load_offset_days) {modulo} 7) + 1) <> load_day)
              OR ((((order_day - 1 + delivery_offset_days) {modulo} 7) + 1) <> delivery_day)
          )
          {route_filter_sql}
        ORDER BY route_number, user_id, order_day
    """


def select_cycles_sql(columns: set[str]) -> str:
    optional = {
        "load_offset_days": "load_offset_days",
        "delivery_offset_days": "delivery_offset_days",
        "schedule_version": "schedule_version",
        "needs_schedule_review": "needs_schedule_review",
    }
    selected = [
        "id",
        "route_number",
        "user_id",
        "order_day",
        "load_day",
        "delivery_day",
        "schedule_key",
        "is_active",
    ]
    for column, alias in optional.items():
        selected.append(column if column in columns else f"NULL AS {alias}")
    return ", ".join(selected)


def fetch_pg_cycles(cur, route: str) -> List[Dict[str, Any]]:
    columns = get_columns(cur)
    cur.execute(
        f"""
        SELECT {select_cycles_sql(columns)}
        FROM user_schedules
        WHERE route_number = %s AND is_active = TRUE
        ORDER BY order_day, load_day, delivery_day
        """,
        [route],
    )
    return [dict(row) for row in cur.fetchall()]


def normalize_pg_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_order_cycle(
        {
            "orderDay": row.get("order_day"),
            "loadDay": row.get("load_day"),
            "deliveryDay": row.get("delivery_day"),
            "loadOffsetDays": row.get("load_offset_days"),
            "deliveryOffsetDays": row.get("delivery_offset_days"),
            "scheduleVersion": row.get("schedule_version"),
            "needsScheduleReview": row.get("needs_schedule_review"),
        }
    )


def fetch_firestore_cycles(service_account: str, route: str) -> Dict[str, Any]:
    from google.cloud import firestore
    from google.cloud.firestore_v1.base_query import FieldFilter

    db = firestore.Client.from_service_account_json(service_account)
    user_id = None
    route_doc = db.collection("routes").document(route).get()
    if route_doc.exists:
        route_data = route_doc.to_dict() or {}
        user_id = route_data.get("ownerUid") or route_data.get("userId")

    if not user_id:
        docs = list(
            db.collection("users")
            .where(filter=FieldFilter("profile.routeNumber", "==", route))
            .where(filter=FieldFilter("profile.role", "==", "owner"))
            .limit(1)
            .stream()
        )
        user_id = docs[0].id if docs else None

    cycles: List[Dict[str, Any]] = []
    if user_id:
        user_doc = db.collection("users").document(user_id).get()
        user_data = user_doc.to_dict() or {}
        cycles = (
            ((user_data.get("userSettings") or {}).get("notifications") or {})
            .get("scheduling", {})
            .get("orderCycles", [])
        )

    return {
        "userId": user_id,
        "cycles": cycles,
        "normalizedCycles": [normalize_order_cycle(cycle) for cycle in cycles],
    }


def run_dry_run(conn, route: Optional[str]) -> Dict[str, Any]:
    route_params: List[Any] = [route] if route else []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        existing_columns = sorted(get_columns(cur))
        where_clause = "WHERE route_number = %s" if route else ""
        cur.execute(f"CREATE TEMP TABLE tmp_user_schedules AS SELECT * FROM user_schedules {where_clause}", route_params)
        for stmt in add_columns_sql("tmp_user_schedules"):
            cur.execute(stmt)
        cur.execute(backfill_sql("tmp_user_schedules"))
        updated = cur.rowcount
        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE is_active = TRUE) AS active,
                   COUNT(*) FILTER (WHERE needs_schedule_review = TRUE) AS needs_review
            FROM tmp_user_schedules
            """
        )
        counts = dict(cur.fetchone() or {})
        cur.execute(mismatch_sql("tmp_user_schedules"))
        mismatches = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT id, route_number, user_id, is_active, order_day, load_day, delivery_day,
                   load_offset_days, delivery_offset_days, schedule_version, needs_schedule_review
            FROM tmp_user_schedules
            ORDER BY is_active DESC, route_number, user_id, order_day
            LIMIT 20
            """
        )
        sample = [dict(row) for row in cur.fetchall()]
    conn.rollback()
    return {
        "mode": "dry-run",
        "route": route,
        "existingColumns": existing_columns,
        "simulatedUpdatedRows": updated,
        "counts": counts,
        "mirrorMismatches": mismatches,
        "sampleRows": sample,
    }


def run_apply(conn, route: Optional[str]) -> Dict[str, Any]:
    route_clause = "AND route_number = %s" if route else ""
    route_params: List[Any] = [route] if route else []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for stmt in add_columns_sql("user_schedules"):
            cur.execute(stmt)
        cur.execute(backfill_sql("user_schedules", route_clause), route_params)
        updated = cur.rowcount
        cur.execute(mismatch_sql("user_schedules", route_clause, parameterized=bool(route_params)), route_params)
        mismatches = [dict(row) for row in cur.fetchall()]
    conn.commit()
    return {
        "mode": "apply",
        "route": route,
        "updatedRows": updated,
        "mirrorMismatches": mismatches,
    }


def compare_route(conn, route: str, service_account: Optional[str]) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        pg_rows = fetch_pg_cycles(cur, route)
    pg_normalized = [normalize_pg_row(row) for row in pg_rows]

    result: Dict[str, Any] = {
        "route": route,
        "postgresRows": pg_rows,
        "postgresNormalizedCycles": pg_normalized,
    }

    if service_account:
        fs = fetch_firestore_cycles(service_account, route)
        result["firestoreUserId"] = fs["userId"]
        result["firestoreCycles"] = fs["cycles"]
        result["firestoreNormalizedCycles"] = fs["normalizedCycles"]
        result["normalizedMatch"] = fs["normalizedCycles"] == pg_normalized

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate and verify schedule offset columns")
    parser.add_argument("--route", help="Optional route number to scope dry-run/apply/compare")
    parser.add_argument("--serviceAccount", help="Optional Firebase service account for Firestore-vs-Postgres compare")
    parser.add_argument("--apply", action="store_true", help="Apply the additive Postgres migration. Omit for dry-run.")
    parser.add_argument("--compare", action="store_true", help="Compare normalized Firestore and Postgres cycles for --route")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = get_connection()
    try:
        result = run_apply(conn, args.route) if args.apply else run_dry_run(conn, args.route)
        if args.compare:
            if not args.route:
                raise SystemExit("--compare requires --route")
            result["comparison"] = compare_route(conn, args.route, args.serviceAccount)
        print(json.dumps(result, default=str, indent=2))
        return 0 if not result.get("mirrorMismatches") else 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

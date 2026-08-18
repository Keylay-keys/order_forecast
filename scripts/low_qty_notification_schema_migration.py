#!/usr/bin/env python3
"""Inspect or apply the additive low-quantity notification schema.

Inspection is the default and never mutates the database. Pass ``--apply``
only from the versioned, one-shot schema Job before listener or worker cutover.
"""

from __future__ import annotations

import argparse
import json

try:
    from .pg_schema import _create_low_qty_notification_tables, get_connection
except ImportError:
    from pg_schema import _create_low_qty_notification_tables, get_connection


TABLES = (
    "low_qty_notification_preferences",
    "low_qty_notification_executions",
)

INDEXES = (
    "idx_low_qty_preferences_due",
    "idx_low_qty_executions_predispatch_lease",
    "idx_low_qty_executions_dispatching_lease",
)

CONSTRAINTS = (
    "low_qty_notification_preferences_pkey",
    "low_qty_preferences_route_number_valid",
    "low_qty_preferences_minute_valid",
    "low_qty_preferences_version_positive",
    "low_qty_preferences_enabled_complete",
    "low_qty_preferences_disabled_reason_consistent",
    "low_qty_notification_executions_pkey",
    "low_qty_executions_route_number_valid",
    "low_qty_executions_owner_uid_present",
    "low_qty_executions_preference_version_positive",
    "low_qty_executions_status_valid",
    "low_qty_executions_claim_token_present",
    "low_qty_executions_attempt_positive",
    "low_qty_executions_lease_after_claim",
    "low_qty_executions_payload_object",
    "low_qty_executions_saps_array",
    "low_qty_executions_tickets_array",
    "low_qty_executions_dispatch_timestamp_consistent",
    "low_qty_executions_completion_consistent",
)


def inspect(conn) -> dict:
    """Return a read-only schema readiness result."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('public.low_qty_notification_preferences'),
                   to_regclass('public.low_qty_notification_executions')
            """
        )
        table_rows = cur.fetchone()
        present_tables = [
            table
            for table, regclass_value in zip(TABLES, table_rows)
            if regclass_value is not None
        ]

        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname = ANY(%s)
            ORDER BY indexname
            """,
            (list(INDEXES),),
        )
        present_indexes = [row[0] for row in cur.fetchall()]

        cur.execute(
            """
            SELECT conname
            FROM pg_constraint constraint_row
            JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
            JOIN pg_namespace schema_row ON schema_row.oid = table_row.relnamespace
            WHERE schema_row.nspname = 'public'
              AND table_row.relname = ANY(%s)
              AND conname = ANY(%s)
            ORDER BY conname
            """,
            (list(TABLES), list(CONSTRAINTS)),
        )
        present_constraints = [row[0] for row in cur.fetchall()]

    conn.rollback()
    ready = (
        set(present_tables) == set(TABLES)
        and set(present_indexes) == set(INDEXES)
        and set(present_constraints) == set(CONSTRAINTS)
    )
    return {
        "mode": "read-only",
        "tables": sorted(present_tables),
        "indexes": sorted(present_indexes),
        "constraints": sorted(present_constraints),
        "ready": ready,
    }


def apply(conn) -> dict:
    """Apply the additive schema in one transaction, then re-inspect it."""
    try:
        with conn.cursor() as cur:
            _create_low_qty_notification_tables(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    result = inspect(conn)
    result["mode"] = "apply"
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the additive migration. Omit for a read-only readiness check.",
    )
    args = parser.parse_args()

    conn = get_connection(autocommit=False)
    try:
        result = apply(conn) if args.apply else inspect(conn)
    finally:
        conn.close()

    print(json.dumps(result, sort_keys=True))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

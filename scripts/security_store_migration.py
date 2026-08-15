#!/usr/bin/env python3
"""Inspect or apply the additive cluster security-store migration.

Default mode is read-only. Pass --apply only in the schema-first production
rollout gate, before deploying the web-api image that requires these tables.
"""

from __future__ import annotations

import argparse
import json

from pg_schema import _create_security_tables, get_connection


SECURITY_INDEXES = (
    ("idx_security_blocks_active", "security_ip_blocks", "permanent, blocked_until"),
    ("idx_security_blocks_last_seen", "security_ip_blocks", "last_seen_at DESC"),
    ("idx_security_events_occurred", "security_events", "occurred_at DESC"),
    ("idx_security_events_type_occurred", "security_events", "event_type, occurred_at DESC"),
    ("idx_security_events_ip_occurred", "security_events", "ip_address, occurred_at DESC"),
    ("idx_security_events_ip_type_occurred", "security_events", "ip_address, event_type, occurred_at DESC"),
)


def inspect(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                to_regclass('public.security_ip_blocks'),
                to_regclass('public.security_events')
            """
        )
        block_table, event_table = cur.fetchone()
        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname = ANY(%s)
            ORDER BY indexname
            """,
            ([name for name, _table, _columns in SECURITY_INDEXES],),
        )
        indexes = [row[0] for row in cur.fetchall()]
    conn.rollback()
    return {
        "mode": "read-only",
        "securityIpBlocks": bool(block_table),
        "securityEvents": bool(event_table),
        "indexes": indexes,
        "ready": bool(
            block_table
            and event_table
            and len(indexes) == len(SECURITY_INDEXES)
        ),
    }


def apply(conn) -> dict:
    try:
        with conn.cursor() as cur:
            _create_security_tables(cur)
            for name, table, columns in SECURITY_INDEXES:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({columns})")
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
    conn = get_connection()
    try:
        result = apply(conn) if args.apply else inspect(conn)
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

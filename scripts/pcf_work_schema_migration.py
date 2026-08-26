#!/usr/bin/env python3
"""Inspect or explicitly apply the checksum-pinned PCF work-ledger migration."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

try:
    from .pg_schema import get_connection
except ImportError:
    from pg_schema import get_connection


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "migrations/pcf/0002_pcf_work_ledger.sql"
CHECKSUM_PATH = ROOT / "migrations/pcf/0002_pcf_work_ledger.sha256"

TABLES = ("pcf_work_items", "pcf_work_events")
INDEXES = (
    "pcf_work_claim_ready_idx",
    "pcf_work_effect_due_idx",
    "pcf_work_one_effect_authority_idx",
    "pcf_work_events_source_event_id_idx",
    "pcf_work_events_work_created_idx",
)
ROLES = ("routespark_pcf_owner", "routespark_pcf_app")


def migration_sha256() -> str:
    return hashlib.sha256(MIGRATION_PATH.read_bytes()).hexdigest()


def expected_sha256() -> str:
    value = CHECKSUM_PATH.read_text(encoding="utf-8").strip()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise RuntimeError("migration checksum file is malformed")
    return value


def verify_checksum() -> str:
    actual = migration_sha256()
    expected = expected_sha256()
    if actual != expected:
        raise RuntimeError(
            f"migration checksum mismatch: expected {expected}, got {actual}"
        )
    return actual


def inspect(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.pcf_work_items'), "
            "to_regclass('public.pcf_work_events')"
        )
        table_values = cur.fetchone()
        present_tables = [
            name for name, value in zip(TABLES, table_values) if value is not None
        ]
        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = ANY(%s)
            ORDER BY indexname
            """,
            (list(INDEXES),),
        )
        present_indexes = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole
            FROM pg_roles
            WHERE rolname = ANY(%s)
            ORDER BY rolname
            """,
            (list(ROLES),),
        )
        role_rows = cur.fetchall()
        present_roles = [row[0] for row in role_rows]
        cur.execute(
            """
            SELECT tablename, tableowner
            FROM pg_tables
            WHERE schemaname = 'public' AND tablename = ANY(%s)
            ORDER BY tablename
            """,
            (list(TABLES),),
        )
        owners = cur.fetchall()
        runtime_is_owner_member = None
        if set(present_roles) == set(ROLES):
            cur.execute(
                "SELECT pg_has_role("
                "'routespark_pcf_app', 'routespark_pcf_owner', 'MEMBER'"
                ")"
            )
            runtime_is_owner_member = bool(cur.fetchone()[0])
        default_public_grant_count = None
        if "routespark_pcf_owner" in present_roles:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_default_acl AS defaults
                JOIN pg_roles AS owner ON owner.oid = defaults.defaclrole
                LEFT JOIN pg_namespace AS namespace
                  ON namespace.oid = defaults.defaclnamespace
                CROSS JOIN LATERAL aclexplode(defaults.defaclacl) AS privilege
                WHERE owner.rolname = 'routespark_pcf_owner'
                  AND defaults.defaclobjtype IN ('r', 'S')
                  AND (
                      defaults.defaclnamespace = 0
                      OR namespace.nspname = 'public'
                  )
                  AND privilege.grantee = 0
                """
            )
            default_public_grant_count = int(cur.fetchone()[0])
        restrictive_event_foreign_keys = 0
        if set(present_tables) == set(TABLES):
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_constraint
                WHERE conrelid = 'public.pcf_work_events'::regclass
                  AND confrelid = 'public.pcf_work_items'::regclass
                  AND contype = 'f'
                  AND confdeltype = 'r'
                """
            )
            restrictive_event_foreign_keys = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT table_name, privilege_type
            FROM information_schema.role_table_grants
            WHERE grantee = 'routespark_pcf_app'
              AND table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name, privilege_type
            """,
            (list(TABLES),),
        )
        grants = [(row[0], row[1]) for row in cur.fetchall()]
    conn.rollback()
    expected_grants = {
        ("pcf_work_items", "INSERT"),
        ("pcf_work_items", "SELECT"),
        ("pcf_work_items", "UPDATE"),
        ("pcf_work_events", "INSERT"),
        ("pcf_work_events", "SELECT"),
    }
    ready = (
        set(present_tables) == set(TABLES)
        and set(present_indexes) == set(INDEXES)
        and set(present_roles) == set(ROLES)
        and role_rows
        == [
            ("routespark_pcf_app", True, False, False, False),
            ("routespark_pcf_owner", False, False, False, False),
        ]
        and owners
        == [
            ("pcf_work_events", "routespark_pcf_owner"),
            ("pcf_work_items", "routespark_pcf_owner"),
        ]
        and runtime_is_owner_member is False
        and default_public_grant_count == 0
        and restrictive_event_foreign_keys == 1
        and set(grants) == expected_grants
    )
    return {
        "mode": "read-only",
        "checksum": verify_checksum(),
        "tables": sorted(present_tables),
        "indexes": sorted(present_indexes),
        "roles": sorted(present_roles),
        "roleAttributes": role_rows,
        "tableOwners": owners,
        "runtimeIsOwnerMember": runtime_is_owner_member,
        "defaultPublicGrantCount": default_public_grant_count,
        "restrictiveEventForeignKeys": restrictive_event_foreign_keys,
        "runtimeTableGrants": grants,
        "ready": ready,
    }


def apply(conn) -> dict:
    checksum = verify_checksum()
    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    result = inspect(conn)
    result["mode"] = "apply"
    result["checksum"] = checksum
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the checked migration. Omit for read-only inspection.",
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

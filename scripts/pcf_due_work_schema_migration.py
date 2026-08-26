#!/usr/bin/env python3
"""Inspect or explicitly apply the PCF shared-projection migration.

Inspection is the default and performs no mutation.  ``--apply`` verifies the
checked-in SHA-256 before executing the one-shot SQL in one transaction.
"""

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
MIGRATION_PATH = ROOT / "migrations/pcf/0001_items_current_due_work.sql"
CHECKSUM_PATH = ROOT / "migrations/pcf/0001_items_current_due_work.sha256"

TABLES = ("items_current", "pcf_projection_routes", "due_work")
INDEXES = (
    "items_current_active_route_delivery_idx",
    "items_current_active_serving_idx",
    "items_current_tombstone_prune_idx",
    "pcf_projection_routes_reconcile_idx",
    "due_work_claim_ready_idx",
    "due_work_lifecycle_active_entity_idx",
    "due_work_terminal_prune_idx",
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
            "SELECT to_regclass('public.items_current'), "
            "to_regclass('public.pcf_projection_routes'), "
            "to_regclass('public.due_work')"
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
        == [(table, "routespark_pcf_owner") for table in sorted(TABLES)]
        and runtime_is_owner_member is False
        and default_public_grant_count == 0
        and set(grants)
        == {
            (table, privilege)
            for table in TABLES
            for privilege in ("INSERT", "SELECT", "UPDATE")
        }
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

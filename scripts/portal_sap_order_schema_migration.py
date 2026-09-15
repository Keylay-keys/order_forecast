#!/usr/bin/env python3
"""Inspect or explicitly apply the Portal SAP-order preference schema.

Inspection is the default.  The versioned Kubernetes Job supplies a privileged
migration login only for this process and separately names the existing web-api
runtime role that receives the narrow table grants.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from psycopg2 import sql

try:
    from .pg_schema import get_connection
except ImportError:
    from pg_schema import get_connection


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "migrations/portal_preferences/0001_portal_sap_order_preferences.sql"
CHECKSUM_PATH = ROOT / "migrations/portal_preferences/0001_portal_sap_order_preferences.sha256"
TABLE = "portal_sap_order_preferences"
CONSTRAINTS = {
    "portal_sap_order_preferences_pkey",
    "portal_sap_order_uid_present",
    "portal_sap_order_route_valid",
    "portal_sap_order_array",
    "portal_sap_order_string_elements",
    "portal_sap_order_bounded",
    "portal_sap_order_revision_positive",
}
REQUIRED_PRIVILEGES = {"SELECT", "INSERT", "UPDATE"}


def migration_sha256() -> str:
    return hashlib.sha256(MIGRATION_PATH.read_bytes()).hexdigest()


def expected_sha256() -> str:
    value = CHECKSUM_PATH.read_text(encoding="utf-8").strip()
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RuntimeError("migration checksum file is malformed")
    return value


def verify_checksum() -> str:
    actual = migration_sha256()
    expected = expected_sha256()
    if actual != expected:
        raise RuntimeError(f"migration checksum mismatch: expected {expected}, got {actual}")
    return actual


def runtime_role() -> str:
    value = os.environ.get("PORTAL_SAP_ORDER_RUNTIME_ROLE", "").strip()
    if not value or len(value) > 63:
        raise RuntimeError("PORTAL_SAP_ORDER_RUNTIME_ROLE is missing or invalid")
    return value


def verify_target() -> dict:
    """Fail closed if the Job target differs from the web-api target."""
    target = {
        "host": os.environ.get("POSTGRES_HOST", "localhost").strip(),
        "port": str(os.environ.get("POSTGRES_PORT", "5432")).strip(),
        "database": os.environ.get("POSTGRES_DB", "routespark").strip(),
    }
    expected = {
        "host": os.environ.get("WEB_API_POSTGRES_HOST", "").strip(),
        "port": os.environ.get("WEB_API_POSTGRES_PORT", "").strip(),
        "database": os.environ.get("WEB_API_POSTGRES_DB", "").strip(),
    }
    if not all(expected.values()) or target != expected:
        raise RuntimeError("migration target does not match the declared web-api database target")
    return target


def inspect(conn, *, runtime: str | None = None) -> dict:
    checksum = verify_checksum()
    target = verify_target()
    runtime = runtime or runtime_role()
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.portal_sap_order_preferences')")
        table_present = cur.fetchone()[0] is not None
        owner = None
        constraints: list[str] = []
        public_privileges: list[str] = []
        direct_grants: list[str] = []
        effective_privileges: dict[str, bool] = {}
        if table_present:
            cur.execute(
                """
                SELECT tableowner
                FROM pg_tables
                WHERE schemaname = 'public' AND tablename = %s
                """,
                (TABLE,),
            )
            owner_row = cur.fetchone()
            owner = owner_row[0] if owner_row else None
            cur.execute(
                """
                SELECT constraint_row.conname
                FROM pg_constraint AS constraint_row
                JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
                JOIN pg_namespace AS schema_row ON schema_row.oid = table_row.relnamespace
                WHERE schema_row.nspname = 'public' AND table_row.relname = %s
                ORDER BY constraint_row.conname
                """,
                (TABLE,),
            )
            constraints = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = 'PUBLIC'
                  AND table_schema = 'public'
                  AND table_name = %s
                ORDER BY privilege_type
                """,
                (TABLE,),
            )
            public_privileges = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = %s
                  AND table_schema = 'public'
                  AND table_name = %s
                ORDER BY privilege_type
                """,
                (runtime, TABLE),
            )
            direct_grants = [row[0] for row in cur.fetchall()]
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                cur.execute(
                    "SELECT has_table_privilege(%s, %s, %s)",
                    (runtime, f"public.{TABLE}", privilege),
                )
                effective_privileges[privilege] = bool(cur.fetchone()[0])
    conn.rollback()
    ready = (
        table_present
        and set(constraints) == CONSTRAINTS
        and not public_privileges
        and REQUIRED_PRIVILEGES.issubset(set(direct_grants))
        and all(effective_privileges.get(privilege, False) for privilege in REQUIRED_PRIVILEGES)
    )
    return {
        "mode": "read-only",
        "checksum": checksum,
        "target": target,
        "tablePresent": table_present,
        "tableOwner": owner,
        "constraints": constraints,
        "publicPrivileges": public_privileges,
        "runtimeRole": runtime,
        "runtimeDirectGrants": direct_grants,
        "runtimeEffectivePrivileges": effective_privileges,
        "runtimeOwnsTable": owner == runtime if owner is not None else None,
        "ready": ready,
    }


def apply(conn, *, runtime: str | None = None) -> dict:
    checksum = verify_checksum()
    verify_target()
    runtime = runtime or runtime_role()
    migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    try:
        with conn.cursor() as cur:
            cur.execute(migration_sql)
            cur.execute(
                sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {} TO {}").format(
                    sql.Identifier(TABLE),
                    sql.Identifier(runtime),
                )
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    result = inspect(conn, runtime=runtime)
    result["mode"] = "apply"
    result["checksum"] = checksum
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the checksummed additive migration. Omit for read-only inspection.",
    )
    args = parser.parse_args()
    verify_checksum()
    verify_target()
    conn = get_connection(autocommit=False)
    try:
        result = apply(conn) if args.apply else inspect(conn)
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Inspect/apply the checksummed, additive Accounting schema on the declared DB."""
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
MIGRATION = ROOT / "migrations/accounting/0001_accounting.sql"
TABLES = {"accounting_documents": {"SELECT", "INSERT", "UPDATE"},
          "accounting_originals": {"SELECT", "INSERT"},
          "accounting_history": {"SELECT", "INSERT"},
          "accounting_attempts": {"SELECT", "INSERT"},
          "accounting_receipt_jobs": {"SELECT", "INSERT", "UPDATE"}}


def verify():
    checksum = hashlib.sha256(MIGRATION.read_bytes()).hexdigest()
    if checksum != MIGRATION.with_suffix(".sha256").read_text().strip():
        raise RuntimeError("Accounting migration checksum mismatch")
    target = {name: os.environ.get("POSTGRES_" + name, "") for name in ("HOST", "PORT", "DB")}
    expected = {name: os.environ.get("WEB_API_POSTGRES_" + name, "") for name in target}
    if not all(target.values()) or target != expected:
        raise RuntimeError("Accounting migration target differs from the declared web API database")
    runtime = os.environ.get("ACCOUNTING_RUNTIME_ROLE", "")
    if not runtime or len(runtime) > 63:
        raise RuntimeError("Accounting runtime role is required")
    return checksum, target, runtime


def inspect(conn, runtime):
    results = {}
    with conn.cursor() as cur:
        for table, required in TABLES.items():
            cur.execute("SELECT to_regclass(%s)", ("public." + table,))
            if cur.fetchone()[0] is None:
                results[table] = {"present": False, "ready": False}
                continue
            cur.execute("SELECT tableowner FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,))
            owner = cur.fetchone()[0]
            privileges = {}
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                cur.execute("SELECT has_table_privilege(%s,%s,%s)", (runtime, "public." + table, privilege))
                privileges[privilege] = bool(cur.fetchone()[0])
            cur.execute("SELECT privilege_type FROM information_schema.role_table_grants WHERE grantee='PUBLIC' AND table_schema='public' AND table_name=%s", (table,))
            public = [r[0] for r in cur.fetchall()]
            effective = {p for p, granted in privileges.items() if granted}
            results[table] = dict(present=True, owner=owner, privileges=privileges, public=public,
                                  ready=owner != runtime and effective == required and not public)
    conn.rollback()
    return {"tables": results, "ready": all(t["ready"] for t in results.values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    checksum, target, runtime = verify()
    conn = get_connection(autocommit=False)
    try:
        if args.apply:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout='5s'")
                cur.execute(MIGRATION.read_text())
                for table, privileges in TABLES.items():
                    cur.execute(sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                        sql.SQL(", ").join(sql.SQL(p) for p in sorted(privileges)), sql.Identifier(table), sql.Identifier(runtime)))
                # Validate effective grants inside the same transaction before commit.
                for table, required in TABLES.items():
                    for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                        cur.execute("SELECT has_table_privilege(%s,%s,%s)", (runtime, "public." + table, privilege))
                        if bool(cur.fetchone()[0]) != (privilege in required):
                            raise RuntimeError("Accounting runtime privileges are not narrowly scoped")
            conn.commit()
        result = inspect(conn, runtime)
        print(json.dumps(dict(mode="apply" if args.apply else "inspect", checksum=checksum, target=target, runtime=runtime, **result), sort_keys=True))
        return 0 if result["ready"] else 1
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

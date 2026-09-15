#!/usr/bin/env python3
"""Apply only the additive email migration using the existing guarded runner."""
try:
    from . import accounting_schema_migration as runner
except ImportError:
    import accounting_schema_migration as runner

runner.MIGRATION = runner.ROOT / "migrations/accounting/0002_settlement_email.sql"
runner.TABLES = {
    "accounting_email_addresses": {"SELECT", "INSERT", "UPDATE"},
    "accounting_email_originals": {"SELECT", "INSERT"},
    "accounting_email_jobs": {"SELECT", "INSERT", "UPDATE"},
}

if __name__ == "__main__":
    raise SystemExit(runner.main())

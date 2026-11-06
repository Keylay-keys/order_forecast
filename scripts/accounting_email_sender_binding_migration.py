#!/usr/bin/env python3
"""Apply only the additive, exact email-sender binding migration."""
try:
    from . import accounting_schema_migration as runner
except ImportError:
    import accounting_schema_migration as runner

runner.MIGRATION = runner.ROOT / "migrations/accounting/0005_email_sender_bindings.sql"
runner.TABLES = {
    "accounting_email_sender_bindings": {"SELECT", "INSERT"},
}

if __name__ == "__main__":
    raise SystemExit(runner.main())

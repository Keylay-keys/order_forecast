#!/usr/bin/env python3
"""Inspect/apply the additive Accounting receipt-agent queue schema."""
try:
    from . import accounting_schema_migration as runner
except ImportError:
    import accounting_schema_migration as runner

runner.MIGRATION = runner.ROOT / "migrations/accounting/0004_receipt_agent_review.sql"
runner.TABLES = {"accounting_receipt_agent_jobs": {"SELECT", "INSERT", "UPDATE"}}

if __name__ == "__main__":
    raise SystemExit(runner.main())

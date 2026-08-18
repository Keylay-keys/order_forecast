import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from order_forecast.scripts import low_qty_notification_schema_migration as migration


class LowQtyNotificationSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[3]
        cls.schema = (root / "order_forecast" / "scripts" / "pg_schema.py").read_text()
        cls.job = (
            root / "k8s" / "base" / "low-qty-notifications" / "schema-job.yaml"
        ).read_text()

    def test_preference_table_has_due_index_and_validation(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS low_qty_notification_preferences", self.schema)
        self.assertIn("PRIMARY KEY", self.schema)
        self.assertIn("reminder_minute_local BETWEEN 0 AND 1439", self.schema)
        self.assertIn("low_qty_preferences_enabled_complete", self.schema)
        self.assertIn("idx_low_qty_preferences_due", self.schema)
        self.assertIn("WHERE enabled = TRUE", self.schema)

    def test_execution_ledger_uses_route_local_date_identity(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS low_qty_notification_executions", self.schema)
        self.assertIn("PRIMARY KEY (route_number, scheduled_local_date)", self.schema)
        self.assertIn("'processing', 'retryable', 'dispatching', 'sent', 'closed'", self.schema)
        self.assertIn("accepted_expo_ticket_ids JSONB", self.schema)
        self.assertIn("low_qty_executions_completion_consistent", self.schema)

    def test_schema_job_is_bounded_and_uses_apply_mode(self):
        self.assertIn("name: low-qty-notifications-schema-v1", self.job)
        self.assertIn("activeDeadlineSeconds: 120", self.job)
        self.assertIn("backoffLimit: 0", self.job)
        self.assertIn("suspend: true", self.job)
        self.assertIn("low_qty_notification_schema_migration.py", self.job)
        self.assertIn("--apply", self.job)
        self.assertIn("automountServiceAccountToken: false", self.job)

    def test_apply_commits_then_reinspects(self):
        conn = MagicMock()
        expected = {"ready": True, "mode": "read-only"}

        with patch.object(migration, "_create_low_qty_notification_tables") as create_tables, patch.object(
            migration,
            "inspect",
            return_value=expected.copy(),
        ) as inspect:
            result = migration.apply(conn)

        create_tables.assert_called_once_with(conn.cursor.return_value.__enter__.return_value)
        conn.commit.assert_called_once_with()
        inspect.assert_called_once_with(conn)
        self.assertEqual(result["mode"], "apply")
        self.assertTrue(result["ready"])

    def test_inspect_is_read_only_and_requires_complete_contract(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = tuple(migration.TABLES)
        cursor.fetchall.side_effect = [
            [(name,) for name in migration.INDEXES],
            [(name,) for name in migration.CONSTRAINTS],
        ]

        result = migration.inspect(conn)

        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()
        self.assertTrue(result["ready"])
        self.assertEqual(result["mode"], "read-only")
        self.assertEqual(set(result["tables"]), set(migration.TABLES))

    def test_apply_rolls_back_on_schema_error(self):
        conn = MagicMock()
        with patch.object(
            migration,
            "_create_low_qty_notification_tables",
            side_effect=RuntimeError("schema failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "schema failure"):
                migration.apply(conn)

        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()

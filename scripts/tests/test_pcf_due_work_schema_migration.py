from __future__ import annotations

import unittest

from order_forecast.scripts import pcf_due_work_schema_migration as migration


class PcfDueWorkSchemaMigrationTests(unittest.TestCase):
    def test_checksum_is_pinned_to_current_schema(self):
        self.assertEqual(migration.verify_checksum(), migration.expected_sha256())

    def test_inspection_inventory_includes_serving_control_plane(self):
        self.assertEqual(
            set(migration.TABLES),
            {
                "items_current",
                "pcf_projection_routes",
                "route_dashboard_summaries",
                "due_work",
            },
        )
        self.assertIn("items_current_active_serving_idx", migration.INDEXES)
        self.assertIn("pcf_projection_routes_reconcile_idx", migration.INDEXES)
        self.assertEqual(
            set(migration.RETENTION_COLUMNS),
            {
                "lifecycle_policy_active",
                "lifecycle_retention_days",
                "lifecycle_policy_revision",
                "lifecycle_policy_synced_at",
            },
        )
        self.assertEqual(
            set(migration.RECONCILIATION_COLUMNS),
            {"last_reconciliation_attempt_at", "last_reconciled_at"},
        )
        self.assertNotIn("eligible", migration.ROUTE_RUNTIME_INSERT_COLUMNS)
        self.assertNotIn("eligible", migration.ROUTE_RUNTIME_UPDATE_COLUMNS)
        self.assertNotIn("serving_enabled", migration.ROUTE_RUNTIME_INSERT_COLUMNS)
        self.assertNotIn("serving_enabled", migration.ROUTE_RUNTIME_UPDATE_COLUMNS)
        self.assertIn(
            "last_reconciliation_attempt_at",
            migration.ROUTE_RUNTIME_UPDATE_COLUMNS,
        )
        self.assertEqual(
            [(table, "routespark_pcf_owner") for table in sorted(migration.TABLES)],
            [
                ("due_work", "routespark_pcf_owner"),
                ("items_current", "routespark_pcf_owner"),
                ("pcf_projection_routes", "routespark_pcf_owner"),
                ("route_dashboard_summaries", "routespark_pcf_owner"),
            ],
        )

    def test_schema_keeps_serving_fail_closed_by_default(self):
        sql = migration.MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn("eligible BOOLEAN NOT NULL DEFAULT FALSE", sql)
        self.assertIn("serving_enabled BOOLEAN NOT NULL DEFAULT FALSE", sql)
        self.assertIn("healthy_until TIMESTAMPTZ", sql)
        self.assertIn("NOT serving_enabled OR eligible", sql)
        self.assertIn("lifecycle_retention_days BETWEEN 1 AND 3650", sql)
        self.assertIn("lifecycle_policy_active BOOLEAN NOT NULL DEFAULT FALSE", sql)
        self.assertIn("lifecycle_policy_revision", sql)
        self.assertIn("last_reconciliation_attempt_at TIMESTAMPTZ", sql)
        self.assertIn(
            "ON pcf_projection_routes (last_reconciliation_attempt_at, route_number)",
            sql,
        )
        self.assertIn(
            "REVOKE INSERT, UPDATE ON TABLE pcf_projection_routes",
            sql,
        )


if __name__ == "__main__":
    unittest.main()

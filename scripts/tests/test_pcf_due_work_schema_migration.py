from __future__ import annotations

import unittest

from order_forecast.scripts import pcf_due_work_schema_migration as migration


class PcfDueWorkSchemaMigrationTests(unittest.TestCase):
    def test_checksum_is_pinned_to_current_schema(self):
        self.assertEqual(migration.verify_checksum(), migration.expected_sha256())

    def test_inspection_inventory_includes_serving_control_plane(self):
        self.assertEqual(
            set(migration.TABLES),
            {"items_current", "pcf_projection_routes", "due_work"},
        )
        self.assertIn("items_current_active_serving_idx", migration.INDEXES)
        self.assertIn("pcf_projection_routes_reconcile_idx", migration.INDEXES)
        self.assertEqual(
            [(table, "routespark_pcf_owner") for table in sorted(migration.TABLES)],
            [
                ("due_work", "routespark_pcf_owner"),
                ("items_current", "routespark_pcf_owner"),
                ("pcf_projection_routes", "routespark_pcf_owner"),
            ],
        )

    def test_schema_keeps_serving_fail_closed_by_default(self):
        sql = migration.MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn("eligible BOOLEAN NOT NULL DEFAULT FALSE", sql)
        self.assertIn("serving_enabled BOOLEAN NOT NULL DEFAULT FALSE", sql)
        self.assertIn("healthy_until TIMESTAMPTZ", sql)
        self.assertIn("NOT serving_enabled OR eligible", sql)


if __name__ == "__main__":
    unittest.main()

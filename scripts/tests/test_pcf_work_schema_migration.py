import re
import unittest

from order_forecast.scripts import pcf_work_schema_migration


class PcfWorkSchemaMigrationTests(unittest.TestCase):
    def test_checksum_is_pinned(self):
        self.assertEqual(
            pcf_work_schema_migration.verify_checksum(),
            pcf_work_schema_migration.expected_sha256(),
        )

    def test_schema_keeps_event_receipts_append_only_for_runtime(self):
        sql = pcf_work_schema_migration.MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn("ALTER ROLE routespark_pcf_app LOGIN", sql)
        self.assertIn("ALTER ROLE routespark_pcf_owner NOLOGIN", sql)
        self.assertIn(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC",
            sql,
        )
        self.assertIn(
            "GRANT SELECT, INSERT ON TABLE pcf_work_events TO routespark_pcf_app",
            sql,
        )
        self.assertNotRegex(
            sql,
            re.compile(r"GRANT[^;]*UPDATE[^;]*pcf_work_events", re.IGNORECASE),
        )
        self.assertIn("ON DELETE RESTRICT", sql)
        self.assertIn("REVOKE ALL ON FUNCTION pcf_notify_work_available() FROM PUBLIC", sql)
        self.assertIn("AFTER INSERT OR UPDATE OF status, not_before", sql)
        self.assertIn("pg_notify('pcf_work_available'", sql)

    def test_schema_has_exact_hot_and_idempotency_indexes(self):
        self.assertEqual(
            set(pcf_work_schema_migration.INDEXES),
            {
                "pcf_work_claim_ready_idx",
                "pcf_work_effect_due_idx",
                "pcf_work_one_effect_authority_idx",
                "pcf_work_events_source_event_id_idx",
                "pcf_work_events_work_created_idx",
                "pcf_work_events_offbox_backup_idx",
            },
        )


if __name__ == "__main__":
    unittest.main()

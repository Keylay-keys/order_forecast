import unittest
from pathlib import Path


class SecuritySchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = (
            Path(__file__).resolve().parents[2] / "scripts" / "pg_schema.py"
        ).read_text()

    def test_cluster_wide_block_table_is_in_canonical_schema(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS security_ip_blocks", self.schema)
        self.assertIn("ip_address INET PRIMARY KEY", self.schema)
        self.assertIn("last_metadata JSONB", self.schema)

    def test_durable_security_event_table_is_in_canonical_schema(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS security_events", self.schema)
        self.assertIn("source_instance VARCHAR(255) NOT NULL", self.schema)
        self.assertIn("idx_security_events_ip_occurred", self.schema)
        self.assertIn("idx_security_events_ip_type_occurred", self.schema)


if __name__ == "__main__":
    unittest.main()

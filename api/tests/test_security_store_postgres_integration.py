"""Opt-in integration tests for the real PostgreSQL security contract.

Set TEST_SECURITY_POSTGRES_DSN only to an isolated test database. The suite
creates the two security tables and deletes only reserved RFC 5737 test data.
"""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg2

from order_forecast.api.utils.security_store import (
    PostgresSecurityStore,
    SecurityEvent,
    record_security_event,
)
from order_forecast.scripts.pg_schema import _create_security_tables


TEST_DSN = os.environ.get("TEST_SECURITY_POSTGRES_DSN", "").strip()
TEST_IPS = ("203.0.113.40", "203.0.113.41")


@unittest.skipUnless(TEST_DSN, "TEST_SECURITY_POSTGRES_DSN is not set")
class PostgresSecurityStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = psycopg2.connect(TEST_DSN)
        try:
            with conn.cursor() as cur:
                _create_security_tables(cur)
            conn.commit()
        finally:
            conn.close()

    def setUp(self):
        self._delete_test_rows()

    def tearDown(self):
        self._delete_test_rows()

    @staticmethod
    def _connect():
        return psycopg2.connect(TEST_DSN)

    @staticmethod
    def _return(conn):
        conn.close()

    def _store(self):
        return PostgresSecurityStore(self._connect, self._return)

    def _delete_test_rows(self):
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM security_events WHERE event_type = 'security_maintenance_completed'"
                )
                cur.execute(
                    "DELETE FROM security_events WHERE ip_address = ANY(%s::inet[])",
                    (list(TEST_IPS),),
                )
                cur.execute(
                    "DELETE FROM security_ip_blocks WHERE ip_address = ANY(%s::inet[])",
                    (list(TEST_IPS),),
                )
            conn.commit()
        finally:
            conn.close()

    def test_block_is_visible_across_independent_connections(self):
        writer = self._store()
        reader = self._store()

        writer.add_block(TEST_IPS[0], "honeypot", timedelta(hours=24))

        observed = reader.get_block(TEST_IPS[0])
        self.assertIsNotNone(observed)
        self.assertEqual(observed["reason"], "honeypot")
        self.assertEqual(observed["hits"], 1)

    def test_concurrent_hits_are_atomically_escalated(self):
        first = self._store()
        second = self._store()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(
                executor.map(
                    lambda store: store.add_block(
                        TEST_IPS[0], "honeypot", timedelta(hours=12)
                    ),
                    (first, second),
                )
            )

        observed = self._store().get_block(TEST_IPS[0])
        self.assertEqual(sorted(result["hits"] for result in results), [1, 2])
        self.assertEqual(observed["hits"], 2)
        expires = datetime.fromisoformat(observed["until"])
        remaining = expires - datetime.now(timezone.utc)
        self.assertGreater(remaining, timedelta(hours=23, minutes=55))

    def test_security_event_survives_a_new_connection(self):
        conn = self._connect()
        try:
            record_security_event(
                conn,
                SecurityEvent(
                    occurred_at=datetime.now(timezone.utc),
                    event_type="honeypot_triggered",
                    severity="high",
                    details={"cf_ray": "integration-test"},
                    ip=TEST_IPS[1],
                    path="/graphql",
                    source_instance="integration:1",
                ),
            )
        finally:
            conn.close()

        check = self._connect()
        try:
            with check.cursor() as cur:
                cur.execute(
                    """
                    SELECT event_type, severity, request_path, details->>'cf_ray'
                    FROM security_events
                    WHERE ip_address = %s::inet
                    """,
                    (TEST_IPS[1],),
                )
                row = cur.fetchone()
        finally:
            check.close()
        self.assertEqual(row, ("honeypot_triggered", "high", "/graphql", "integration-test"))

    def test_auth_failure_threshold_is_shared_across_connections(self):
        decisions = []
        for worker_number in range(3):
            decision = self._store().record_auth_failure(
                TEST_IPS[0],
                reason=f"worker_{worker_number}",
                path="/api/auth/verify",
                source_instance=f"pod:{worker_number}",
            )
            decisions.append(decision)

        self.assertIsNone(decisions[0])
        self.assertIsNone(decisions[1])
        self.assertEqual(decisions[2]["failures"], 3)
        self.assertEqual(decisions[2]["lockout_duration"], timedelta(minutes=1))

    def test_retention_is_periodic_and_cluster_coordinated(self):
        first = self._store().run_maintenance(source_instance="integration:one")
        second = self._store().run_maintenance(source_instance="integration:two")

        self.assertEqual(first["status"], "completed")
        self.assertEqual(second["status"], "not_due")


if __name__ == "__main__":
    unittest.main()

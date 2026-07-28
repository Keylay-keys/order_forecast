import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from order_forecast.scripts import pg_utils


class PgUtilsRuntimeSafetyTests(unittest.TestCase):
    def tearDown(self):
        pg_utils._pg_conn = None

    def test_cached_connection_uses_autocommit_and_bounded_waits(self):
        connection = MagicMock()
        connection.closed = 0

        with patch.dict(
            os.environ,
            {
                "POSTGRES_APPLICATION_NAME": "routespark-test-listener",
                "POSTGRES_CONNECT_TIMEOUT_SECONDS": "7",
                "POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS": "45000",
                "POSTGRES_LOCK_TIMEOUT_MS": "9000",
            },
            clear=False,
        ), patch.object(pg_utils.psycopg2, "connect", return_value=connection) as connect:
            result = pg_utils.get_pg_connection()

        self.assertIs(result, connection)
        self.assertTrue(connection.autocommit)
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["connect_timeout"], 7)
        self.assertEqual(kwargs["application_name"], "routespark-test-listener")
        self.assertIn("idle_in_transaction_session_timeout=45000", kwargs["options"])
        self.assertIn("lock_timeout=9000", kwargs["options"])

    def test_invalid_timeout_environment_uses_safe_defaults(self):
        connection = MagicMock()
        connection.closed = 0

        with patch.dict(
            os.environ,
            {
                "POSTGRES_CONNECT_TIMEOUT_SECONDS": "invalid",
                "POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS": "0",
                "POSTGRES_LOCK_TIMEOUT_MS": "-1",
            },
            clear=False,
        ), patch.object(pg_utils.psycopg2, "connect", return_value=connection) as connect:
            pg_utils.get_pg_connection()

        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["connect_timeout"], 10)
        self.assertIn("idle_in_transaction_session_timeout=60000", kwargs["options"])
        self.assertIn("lock_timeout=15000", kwargs["options"])

    def test_application_name_falls_back_to_script_name(self):
        with patch.dict(os.environ, {"POSTGRES_APPLICATION_NAME": ""}, clear=False), patch.object(
            sys,
            "argv",
            ["/app/order_forecast/scripts/delivery_manifest_listener.py"],
        ):
            application_name = pg_utils._postgres_application_name()

        self.assertEqual(application_name, "routespark-delivery_manifest_listener.py")

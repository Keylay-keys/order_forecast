import unittest
from unittest.mock import patch

from order_forecast.api import pcf_database


class PcfDatabaseConfigurationTests(unittest.TestCase):
    def setUp(self):
        pcf_database._pool = None

    def tearDown(self):
        pcf_database._pool = None

    def test_missing_dedicated_user_fails_closed(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "PCF_POSTGRES_USER"):
                pcf_database.get_pcf_pg_pool()

    def test_general_application_role_is_rejected(self):
        with patch.dict(
            "os.environ",
            {"PCF_POSTGRES_USER": "routespark", "PCF_POSTGRES_PASSWORD": "test"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "routespark_pcf_app"):
                pcf_database.get_pcf_pg_pool()

    def test_exact_runtime_role_builds_separate_bounded_pool(self):
        fake_pool = object()
        with patch.dict(
            "os.environ",
            {
                "PCF_POSTGRES_USER": "routespark_pcf_app",
                "PCF_POSTGRES_PASSWORD": "test-only",
                "PCF_POSTGRES_POOL_MAX": "4",
            },
            clear=True,
        ), patch.object(
            pcf_database.pool,
            "ThreadedConnectionPool",
            return_value=fake_pool,
        ) as constructor:
            result = pcf_database.get_pcf_pg_pool()

        self.assertIs(result, fake_pool)
        kwargs = constructor.call_args.kwargs
        self.assertEqual(kwargs["user"], "routespark_pcf_app")
        self.assertEqual(kwargs["maxconn"], 4)
        self.assertIn("statement_timeout=10000", kwargs["options"])

    def test_pool_bound_rejects_invalid_or_excessive_values(self):
        for value in ("not-a-number", "0", "21"):
            with self.subTest(value=value), patch.dict(
                "os.environ",
                {
                    "PCF_POSTGRES_USER": "routespark_pcf_app",
                    "PCF_POSTGRES_PASSWORD": "test-only",
                    "PCF_POSTGRES_POOL_MAX": value,
                },
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "PCF_POSTGRES_POOL_MAX"):
                    pcf_database.get_pcf_pg_pool()


if __name__ == "__main__":
    unittest.main()

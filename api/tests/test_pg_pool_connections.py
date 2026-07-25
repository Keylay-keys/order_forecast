import unittest
from unittest.mock import patch

import psycopg2
from psycopg2 import extensions

from order_forecast.api import dependencies


class _FakeCursor:
    def __init__(self, *, fail: bool = False):
        self.fail = fail

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql):
        if self.fail:
            raise psycopg2.OperationalError("SSL connection has been closed unexpectedly")


class _FakeConnection:
    def __init__(self, *, fail_ping: bool = False, closed: int = 0):
        self.fail_ping = fail_ping
        self.closed = closed
        self.rollback_count = 0

    def get_transaction_status(self):
        return extensions.TRANSACTION_STATUS_IDLE

    def cursor(self):
        return _FakeCursor(fail=self.fail_ping)

    def rollback(self):
        self.rollback_count += 1


class _FakePool:
    def __init__(self, connections):
        self.connections = list(connections)
        self.returned = []

    def getconn(self):
        return self.connections.pop(0)

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


class PostgresPoolConnectionTests(unittest.TestCase):
    def test_get_pg_connection_discards_stale_connection_and_retries(self):
        stale = _FakeConnection(fail_ping=True)
        fresh = _FakeConnection()
        fake_pool = _FakePool([stale, fresh])

        with patch.object(dependencies, "get_pg_pool", return_value=fake_pool):
            conn = dependencies.get_pg_connection()

        self.assertIs(conn, fresh)
        self.assertEqual(fake_pool.returned, [(stale, True)])
        self.assertEqual(fresh.rollback_count, 1)


if __name__ == "__main__":
    unittest.main()

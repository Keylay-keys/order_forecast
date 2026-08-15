import unittest
from datetime import datetime, timedelta, timezone
import importlib
from unittest.mock import patch

from order_forecast.api.utils.blocklist import IPBlocklist
from order_forecast.api.utils.security_logger import SecurityLogger
from order_forecast.api.utils.security_store import PostgresSecurityStore, SecurityEvent, record_security_event


blocklist_module = importlib.import_module("order_forecast.api.utils.blocklist")


class _SharedMemoryStore:
    """One authority shared by multiple simulated pods/workers."""

    def __init__(self):
        self.blocks = {}
        self.fail_reads = False
        self.fail_writes = False

    def add_block(self, ip, reason, duration, *, permanent=False, metadata=None):
        if self.fail_writes:
            raise RuntimeError("database unavailable")
        now = datetime.now(timezone.utc)
        existing = self.blocks.get(ip)
        hits = int(existing["hits"]) + 1 if existing else 1
        until = None if permanent else now + duration
        entry = {
            "ip": ip,
            "until": until.isoformat() if until else None,
            "reason": reason,
            "hits": hits,
            "permanent": permanent,
            "first_seen_at": existing["first_seen_at"] if existing else now.isoformat(),
            "last_seen_at": now.isoformat(),
            "last_metadata": metadata or {},
        }
        self.blocks[ip] = entry
        return dict(entry)

    def get_block(self, ip):
        if self.fail_reads:
            raise RuntimeError("database unavailable")
        entry = self.blocks.get(ip)
        return dict(entry) if entry else None

    def remove_block(self, ip):
        return self.blocks.pop(ip, None) is not None

    def get_stats(self):
        return {"active_blocks": len(self.blocks), "top_offenders": []}

    def cleanup_expired(self):
        return 0


class _RecordingCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executions = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executions.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


class _RecordingConnection:
    def __init__(self, cursor):
        self.test_cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs):
        return self.test_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class SecurityBlocklistTests(unittest.TestCase):
    def test_block_created_by_one_worker_is_enforced_by_another(self):
        shared_store = _SharedMemoryStore()
        worker_a = IPBlocklist(shared_store)
        worker_b = IPBlocklist(shared_store)

        with patch("order_forecast.api.utils.blocklist.security_logger.ip_blocked"):
            worker_a.add("203.0.113.10", "honeypot", timedelta(hours=24))

        self.assertTrue(worker_b.is_blocked("203.0.113.10"))
        self.assertEqual(worker_b.get_block_info("203.0.113.10")["reason"], "honeypot")

    def test_database_outage_only_preserves_previously_observed_blocks(self):
        shared_store = _SharedMemoryStore()
        observer = IPBlocklist(shared_store)
        clean_worker = IPBlocklist(shared_store)

        with patch("order_forecast.api.utils.blocklist.security_logger.ip_blocked"):
            observer.add("203.0.113.11", "honeypot", timedelta(hours=24))
        self.assertTrue(observer.is_blocked("203.0.113.11"))

        shared_store.fail_reads = True
        with patch("order_forecast.api.utils.blocklist.logger.exception"):
            self.assertTrue(observer.is_blocked("203.0.113.11"))
            self.assertFalse(clean_worker.is_blocked("203.0.113.12"))

    def test_whitelist_never_reaches_shared_store(self):
        shared_store = _SharedMemoryStore()
        worker = IPBlocklist(shared_store)

        with patch.object(blocklist_module, "WHITELISTED_IPS", {"127.0.0.1"}):
            self.assertFalse(worker.add("127.0.0.1", "test"))
            self.assertFalse(worker.is_blocked("127.0.0.1"))
        self.assertEqual(shared_store.blocks, {})

    def test_invalid_client_ip_cannot_turn_honeypot_into_server_error(self):
        shared_store = _SharedMemoryStore()
        worker = IPBlocklist(shared_store)

        with patch("order_forecast.api.utils.blocklist.logger.warning"):
            self.assertFalse(worker.add("unknown", "honeypot"))
            self.assertFalse(worker.is_blocked("not-an-ip"))
        self.assertEqual(shared_store.blocks, {})

    def test_database_write_outage_uses_local_block_without_server_error(self):
        shared_store = _SharedMemoryStore()
        shared_store.fail_writes = True
        worker = IPBlocklist(shared_store)

        with (
            patch("order_forecast.api.utils.blocklist.security_logger.ip_blocked"),
            patch("order_forecast.api.utils.blocklist.logger.exception"),
        ):
            self.assertTrue(worker.add("203.0.113.13", "honeypot"))
        shared_store.fail_reads = True
        with patch("order_forecast.api.utils.blocklist.logger.exception"):
            self.assertTrue(worker.is_blocked("203.0.113.13"))


class PostgresSecurityStoreContractTests(unittest.TestCase):
    def test_add_serializes_updates_with_transaction_advisory_lock(self):
        now = datetime.now(timezone.utc)
        cursor = _RecordingCursor(
            rows=[
                None,
                {
                    "ip_address": "203.0.113.20",
                    "reason": "honeypot",
                    "hit_count": 1,
                    "permanent": False,
                    "blocked_until": now + timedelta(hours=24),
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "last_metadata": {},
                },
            ]
        )
        conn = _RecordingConnection(cursor)
        returned = []
        store = PostgresSecurityStore(lambda: conn, returned.append)

        result = store.add_block(
            "203.0.113.20",
            "honeypot",
            timedelta(hours=24),
        )

        statements = [sql for sql, _params in cursor.executions]
        self.assertIn("pg_advisory_xact_lock", statements[0])
        self.assertIn("FOR UPDATE", statements[1])
        self.assertIn("ON CONFLICT (ip_address) DO UPDATE", statements[2])
        self.assertEqual(result["ip"], "203.0.113.20")
        self.assertEqual(conn.commits, 1)
        self.assertEqual(returned, [conn])

    def test_security_event_excludes_uid_and_uses_bounded_jsonb_payload(self):
        cursor = _RecordingCursor()
        conn = _RecordingConnection(cursor)
        event = SecurityEvent(
            occurred_at=datetime.now(timezone.utc),
            event_type="honeypot_triggered",
            severity="high",
            ip="203.0.113.30",
            path="/graphql",
            source_instance="pod:123",
            details={"cf_ray": "abc", "authenticated_actor_present": False},
        )

        record_security_event(conn, event)

        insert_params = cursor.executions[0][1]
        self.assertEqual(insert_params[3], "203.0.113.30")
        self.assertEqual(insert_params[4], "/graphql")
        self.assertNotIn("firebase-uid", repr(insert_params))
        self.assertEqual(conn.commits, 1)

    def test_security_logger_replaces_uid_with_presence_boolean_for_database(self):
        logger = SecurityLogger()
        with (
            patch.object(logger.logger, "warning"),
            patch(
                "order_forecast.api.utils.security_store.enqueue_security_event",
                return_value=True,
            ) as enqueue,
        ):
            logger.log_event(
                event_type="authorization_failure",
                severity="high",
                details={"reason": "test"},
                ip="203.0.113.31",
                uid="firebase-uid-must-not-persist",
                path="/api/orders",
            )

        event = enqueue.call_args.args[0]
        self.assertNotIn("uid", event.details)
        self.assertNotIn("firebase-uid-must-not-persist", repr(event))
        self.assertTrue(event.details["authenticated_actor_present"])


if __name__ == "__main__":
    unittest.main()

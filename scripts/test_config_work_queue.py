import threading
import unittest
from unittest.mock import MagicMock, patch
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from google.api_core.exceptions import DeadlineExceeded, RetryError
from order_forecast.scripts.config_sync_retry import LatestConfigWork
from order_forecast.scripts import config_sync_listener as listener


class WorkQueueTests(unittest.TestCase):
    def test_failed_group_does_not_starve_another_route(self):
        work, calls = LatestConfigWork(), []
        for key in range(30):
            work.put(('product', '988200'), key, lambda: False)
        work.put(('store', '908820'), 'one', lambda: calls.append('other') or True)
        self.assertEqual(work.drain(limit=2), 1)
        self.assertEqual(calls, ['other'])
        self.assertEqual(len(work), 30)

    def test_failure_attempted_once_per_drain_and_latest_payload_coalesces(self):
        work = LatestConfigWork(retry_delay=0)
        old, latest = MagicMock(return_value=False), MagicMock(return_value=True)
        work.put('route', 'one', old)
        work.drain()
        self.assertEqual(old.call_count, 1)
        work.put('route', 'one', latest)
        work.drain()
        self.assertEqual(latest.call_count, 1)
        self.assertEqual(len(work), 0)

    def test_replacement_during_success_is_not_acknowledged_by_old_action(self):
        work = LatestConfigWork()
        latest = MagicMock(return_value=True)
        def old():
            work.put('route', 'one', latest)
            return True
        work.put('route', 'one', old)
        work.drain()
        self.assertEqual(len(work), 1)
        work.drain()
        latest.assert_called_once()

    def test_second_drainer_does_not_block_or_run_concurrently(self):
        work, entered, release = LatestConfigWork(), threading.Event(), threading.Event()
        def blocked():
            entered.set()
            self.assertTrue(release.wait(2))
            return True
        work.put('route', 'one', blocked)
        thread = threading.Thread(target=work.drain)
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertEqual(work.drain(), 0)
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(work), 0)

    def test_sdk_begin_timeout_masked_by_rollback_still_retries_actor(self):
        # Real SDK transaction lifecycle, RPC stubbed before any network access.
        client = firestore.Client(project='demo-routespark-send-device-test', credentials=AnonymousCredentials())
        transaction = client.transaction(max_attempts=1)
        deadline = DeadlineExceeded('deadline')
        timeout = RetryError('timeout', deadline)
        # google.api_core raises RetryError from the last RPC exception.
        timeout.__cause__ = deadline
        def masked_failure(*args):
            @firestore.transactional
            def body(tx):
                raise AssertionError('Must not enter callback after failed BeginTransaction')
            with patch.object(transaction, '_begin', side_effect=timeout):
                body(transaction)
        manager = listener.ConfigSyncManager(MagicMock())
        manager._send_worker.work._retry_delay = 0
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
             patch.object(listener, 'enqueue_send_reminder_refresh', side_effect=masked_failure), \
             patch('builtins.print') as output:
            manager.enqueue_send_reminder_settings('synthetic-member', '988200')
            manager._send_worker.drain()
            self.assertIn('ValueError>RetryError>DeadlineExceeded', str(output.call_args))
        self.assertEqual(len(manager._send_worker.work), 1)
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
             patch.object(listener, 'enqueue_send_reminder_refresh', return_value='queued') as retry:
            manager._send_worker.drain()
            retry.assert_called_once_with(manager.fb_client, 'synthetic-member', '988200')
        self.assertEqual(len(manager._send_worker.work), 0)

    def test_failure_cooldown_survives_new_payload_and_uses_monotonic_clock(self):
        now = [100.0]
        work = LatestConfigWork(clock=lambda: now[0])
        old, latest = MagicMock(return_value=False), MagicMock(return_value=True)
        work.put('route', 'one', old)
        work.drain()
        work.put('route', 'one', latest)
        for _ in range(10):
            work.drain()
        latest.assert_not_called()
        now[0] += 5
        work.drain()
        latest.assert_called_once()
        self.assertEqual(len(work), 0)

    def test_drain_stops_starting_calls_after_elapsed_budget(self):
        now = [100.0]
        work = LatestConfigWork(clock=lambda: now[0])
        def slow():
            now[0] += 30
            return True
        latest = MagicMock(return_value=True)
        work.put('route', 'one', slow)
        work.put('route', 'two', latest)
        self.assertEqual(work.drain(), 1)
        latest.assert_not_called()
        self.assertEqual(work.drain(), 1)


if __name__ == '__main__':
    unittest.main()

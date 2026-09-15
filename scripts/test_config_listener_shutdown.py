"""Cooperative startup boundaries and signal idempotency, without live I/O."""
import signal
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch
from order_forecast.scripts import config_sync_listener as listener
from order_forecast.scripts.config_listener_process import ShutdownRequest, shutdown_signals


class ShutdownTests(unittest.TestCase):
    def test_launcher_filename_starts_without_pythonpath(self):
        filename = Path(listener.__file__).with_name('config_listener_process.py')
        result = subprocess.run([sys.executable, '-u', str(filename), '--help'],
                                cwd='/tmp', env={k: v for k, v in os.environ.items() if k != 'PYTHONPATH'},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--serviceAccount', result.stdout)

    def test_signals_only_set_flag_and_restore_handlers(self):
        before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
        with shutdown_signals() as shutdown:
            for _ in range(3):
                signal.raise_signal(signal.SIGTERM)
                signal.raise_signal(signal.SIGINT)
            self.assertTrue(shutdown.requested)
        self.assertEqual({s: signal.getsignal(s) for s in before}, before)

    def test_stopped_before_initialization_does_not_open_resources(self):
        stop = ShutdownRequest()
        stop.requested = True
        with patch.object(listener, 'from_service_account') as factory:
            self.assertEqual(listener.main('not-read', shutdown=stop), 0)
        factory.assert_not_called()

    def test_stop_during_runtime_creation_closes_before_opening_database(self):
        stop = ShutdownRequest()
        runtime = MagicMock()
        def create(*args):
            stop.requested = True
            return runtime
        with patch.object(listener, 'from_service_account', side_effect=create), \
             patch.object(listener, 'get_pg_connection') as database:
            self.assertEqual(listener.main('not-read', shutdown=stop), 0)
        runtime.close.assert_called_once()
        database.assert_not_called()

    def test_stop_during_database_connection_releases_both_resources(self):
        stop = ShutdownRequest()
        runtime, connection = MagicMock(), MagicMock()
        def connect():
            stop.requested = True
            return connection
        with patch.object(listener, 'from_service_account', return_value=runtime), \
             patch.object(listener, 'get_pg_connection', side_effect=connect), \
             patch.object(listener, 'close_pg_connection') as close:
            self.assertEqual(listener.main('not-read', shutdown=stop), 0)
        close.assert_called_once_with(connection)
        connection.cursor.assert_not_called()
        runtime.close.assert_called_once()

    def test_stop_between_routes_does_not_register_remaining_routes(self):
        stop = ShutdownRequest()
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            {'route_number': str(880000 + i), 'user_id': 'synthetic-owner'} for i in range(60)]
        manager = listener.ConfigSyncManager(MagicMock())
        def start(*args):
            stop.requested = True
        with patch.object(listener, 'get_pg_connection', return_value=connection), \
             patch.object(listener, 'close_pg_connection'), \
             patch.object(manager, 'start_route_listeners', side_effect=start) as register:
            manager.discover_and_start_listeners(shutdown=stop)
        self.assertEqual(register.call_count, 1)


if __name__ == '__main__':
    unittest.main()

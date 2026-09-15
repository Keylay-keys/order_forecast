"""Integrated listener lifecycle tests. No credentials or network."""
import threading
import unittest
from unittest.mock import MagicMock, patch
from order_forecast.scripts.config_firestore_runtime import FirestoreRuntime, IsolatedTransport, from_service_account
from order_forecast.scripts.config_refresh_worker import RefreshWorker
from order_forecast.scripts import config_sync_listener as listener


class RuntimeTests(unittest.TestCase):
    def test_main_starts_worker_registers_routes_and_cleans_up_on_interrupt(self):
        runtime = FirestoreRuntime(lambda: MagicMock())
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [{'route_number': str(880000 + i), 'user_id': 'synthetic-owner'} for i in range(60)]
        managers, before_close = [], []
        original = listener.ConfigSyncManager
        def create(*args, **kwargs):
            manager = original(*args, **kwargs)
            managers.append(manager)
            return manager
        close = runtime.close
        def record_close():
            before_close.append(len(runtime._leases))
            close()
        with patch.object(listener, 'from_service_account', return_value=runtime), \
             patch.object(listener, 'ConfigSyncManager', side_effect=create), \
             patch.object(listener, 'get_pg_connection', return_value=connection), \
             patch.object(listener, 'close_pg_connection'), \
             patch.object(listener, 'resolve_route_owner_user_id', return_value='synthetic-owner'), \
             patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
             patch.object(original, '_low_qty_sync_enabled', return_value=False), \
             patch.object(runtime, 'close', side_effect=record_close), \
             patch.object(listener.time, 'sleep', side_effect=KeyboardInterrupt), patch('builtins.print'):
            self.assertEqual(listener.main('synthetic-key-not-read.json'), 0)
        self.assertEqual(before_close, [300])
        self.assertEqual(len(managers), 1)
        self.assertIsNotNone(managers[0]._send_worker._thread)
        self.assertTrue(managers[0]._send_worker.join(.1))
        self.assertEqual(len(runtime._leases), 0)
        self.assertTrue(runtime._closed)

    def test_sixty_route_registrations_have_bounded_channels_and_all_release(self):
        runtime = FirestoreRuntime(lambda: MagicMock())
        manager = listener.ConfigSyncManager(runtime.rpc, firestore_runtime=runtime)
        with patch.object(listener, 'resolve_route_owner_user_id', return_value='synthetic-owner'), \
             patch.object(listener, 'send_reminder_route_enabled', return_value=True), patch('builtins.print'):
            for i in range(60):
                manager.start_route_listeners(str(880000 + i), 'synthetic-owner')
            self.assertEqual(len(manager.known_routes), 60)
            self.assertEqual(len(runtime._leases), 300)
            self.assertEqual(len(runtime._slots), 13)
            self.assertTrue(all(slot[1] <= 24 for slot in runtime._slots))
            manager.stop_all()
            self.assertEqual(len(runtime._leases), 0)
            self.assertTrue(all(slot[1] == 0 for slot in runtime._slots))

    def test_partial_route_registration_releases_watch_capacity_for_retry(self):
        runtime = FirestoreRuntime(lambda: MagicMock(), watches_per_channel=2, max_watch_channels=2)
        manager = listener.ConfigSyncManager(runtime.rpc, firestore_runtime=runtime)
        with patch.object(listener, 'resolve_route_owner_user_id', return_value='synthetic-owner'), \
             patch.object(listener, 'send_reminder_route_enabled', return_value=True):
            for _ in range(2):
                with self.assertRaisesRegex(RuntimeError, 'capacity'):
                    manager.start_route_listeners('988200', 'synthetic-owner')
                self.assertEqual(len(runtime._leases), 0)
                self.assertEqual(manager.watchers, [])
                self.assertNotIn('988200', manager.known_routes)
                self.assertTrue(all(slot[1] == 0 for slot in runtime._slots))
        manager.stop_all()

    def test_every_listener_kind_uses_watch_pool_not_rpc_client(self):
        runtime = FirestoreRuntime(lambda: MagicMock(), watches_per_channel=2)
        manager = listener.ConfigSyncManager(runtime.rpc, firestore_runtime=runtime)
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
             patch.object(manager, '_low_qty_sync_enabled', return_value=True), \
             patch.object(listener, 'low_qty_schema_ready', return_value=True):
            manager.start_stores_listener('988200')
            manager.start_products_listener('988200')
            manager.start_schedules_listener('synthetic-owner', '988200')
            manager.start_low_qty_preference_listener()
            manager._reconcile_low_qty_owner_watchers({'988200'})
        self.assertEqual(len(runtime._leases), 7)
        self.assertEqual(len(runtime._slots), 4)
        self.assertTrue(all(slot[1] <= 2 for slot in runtime._slots))
        runtime.rpc.collection.assert_not_called()
        manager.stop_all()
        self.assertTrue(all(slot[1] == 0 for slot in runtime._slots))

    def test_close_racing_registration_releases_late_lease(self):
        runtime = FirestoreRuntime(lambda: MagicMock())
        entered, release = threading.Event(), threading.Event()
        reference, watcher = MagicMock(), MagicMock()
        failures = []
        def register(callback):
            entered.set()
            if not release.wait(2):
                raise TimeoutError('test release failed')
            return watcher
        reference.on_snapshot.side_effect = register
        def add():
            try:
                runtime.watch(lambda db: reference, MagicMock())
            except RuntimeError as error:
                failures.append(str(error))
        thread = threading.Thread(target=add, daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            runtime.close()
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, ['Firestore runtime stopped during registration'])
        watcher.unsubscribe.assert_called_once()
        self.assertEqual(len(runtime._leases), 0)
        self.assertEqual(runtime._slots[0][1], 0)

    def test_watch_budget_release_failure_and_close(self):
        clients = []
        def factory():
            client = MagicMock()
            clients.append(client)
            return client
        runtime = FirestoreRuntime(factory, watches_per_channel=2, max_watch_channels=2)
        leases = [runtime.watch(lambda db: db.document('routes/988200'), MagicMock()) for _ in range(4)]
        self.assertEqual(len(clients), 3)
        clients[0].document.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, 'capacity'):
            runtime.watch(lambda db: db.document('routes/988200'), MagicMock())
        leases[0].unsubscribe()
        leases[0].unsubscribe()
        failed = MagicMock(side_effect=RuntimeError('registration failed'))
        with self.assertRaises(RuntimeError):
            runtime.watch(failed, MagicMock())
        runtime.watch(lambda db: db.document('routes/988200'), MagicMock())
        self.assertEqual(len(clients), 3)
        runtime.close()
        runtime.close()
        for client in clients:
            client.close.assert_called_once()
        with self.assertRaisesRegex(RuntimeError, 'stopped'):
            runtime.watch(MagicMock(), MagicMock())

    def test_production_factory_preserves_identity_and_refuses_emulator(self):
        credentials = MagicMock(project_id='synthetic-project')
        with patch.dict('os.environ', {}, clear=True), \
             patch('google.oauth2.service_account.Credentials.from_service_account_file', return_value=credentials), \
             patch('order_forecast.scripts.config_firestore_runtime.IsolatedFirestoreClient') as client:
            runtime = from_service_account('synthetic-key-not-read.json')
            runtime.watch(lambda db: db.document('synthetic/doc'), MagicMock())
            self.assertEqual(client.call_count, 2)
            for call in client.call_args_list:
                self.assertEqual(call.kwargs, dict(project='synthetic-project', database='(default)', credentials=credentials))
            runtime.close()
        with patch.dict('os.environ', {'FIRESTORE_EMULATOR_HOST': '127.0.0.1:12345'}):
            with self.assertRaises(RuntimeError):
                from_service_account('not-read.json')

    def test_channel_isolation_keeps_credentials_and_keepalive(self):
        credentials = MagicMock()
        with patch('google.cloud.firestore_v1.services.firestore.transports.grpc.FirestoreGrpcTransport.create_channel') as create:
            IsolatedTransport.create_channel('firestore.googleapis.com', credentials=credentials,
                options=[('grpc.keepalive_time_ms', 30000)])
            self.assertIs(create.call_args.kwargs['credentials'], credentials)
            self.assertEqual(dict(create.call_args.kwargs['options']), {
                'grpc.keepalive_time_ms': 30000, 'grpc.use_local_subchannel_pool': 1})

    def test_reentrant_registration_does_not_hold_pool_lock_over_callback(self):
        runtime = FirestoreRuntime(lambda: MagicMock(), watches_per_channel=2)
        nested = []
        reference = MagicMock()
        reference.on_snapshot.side_effect = lambda callback: (nested.append(runtime.watch(lambda db: db.document('b/c'), callback)) or MagicMock())
        result = []
        thread = threading.Thread(target=lambda: result.append(runtime.watch(lambda db: reference, MagicMock())), daemon=True)
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive(), 'Registration callback deadlocked pool')
        self.assertEqual(len(result), 1)
        runtime.close()

    def test_actual_schedule_continues_while_refresh_worker_waits_and_shutdown_cancels(self):
        runtime = MagicMock()
        runtime.rpc = MagicMock()
        runtime.watch.side_effect = lambda ref, cb: ref(runtime.rpc).on_snapshot(cb)
        manager = listener.ConfigSyncManager(runtime.rpc, firestore_runtime=runtime)
        entered, canceled, schedule_done = threading.Event(), threading.Event(), threading.Event()
        runtime.close.side_effect = canceled.set
        def slow(*args):
            entered.set()
            if not canceled.wait(3):
                raise TimeoutError('test cancellation failed')
            raise RuntimeError('RPC canceled')
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
             patch.object(listener, 'enqueue_send_reminder_refresh', side_effect=slow), \
             patch.object(listener, 'read_current_schedule', return_value=('owner-b', {}, None)) as read:
            manager._send_worker.start()
            manager._send_worker.start()  # Idempotent, no second worker.
            manager.enqueue_send_reminder_settings('owner-a', '988200')
            self.assertTrue(entered.wait(1))
            manager.start_schedules_listener('owner-b', '908820')
            callback = runtime.rpc.collection.return_value.document.return_value.on_snapshot.call_args.args[0]
            def schedule():
                callback([], [], None)
                schedule_done.set()
            thread = threading.Thread(target=schedule, daemon=True)
            thread.start()
            try:
                self.assertTrue(schedule_done.wait(1))
                read.assert_called_once()
            finally:
                manager.stop_all()
                thread.join(1)
            self.assertTrue(manager._send_worker.join(.1))
            self.assertFalse(manager._send_worker.submit('owner-c', '908821'))
            runtime.close.assert_called_once()

    def test_pending_hints_coalesce_recheck_gate_and_revocation(self):
        enabled = MagicMock(return_value=True)
        denied = RuntimeError('revoked')
        denied.status_code = 403
        enqueue = MagicMock(side_effect=[TimeoutError(), denied])
        worker = RefreshWorker(enqueue, enabled, retry_delay=0)
        for _ in range(100):
            worker.submit('member', '988200')
        self.assertEqual(len(worker.work), 1)
        worker.drain()
        self.assertEqual(len(worker.work), 1)
        worker.drain()
        self.assertEqual(len(worker.work), 0)
        self.assertEqual(enqueue.call_count, 2)
        worker.submit('member', '988200')
        enabled.return_value = False
        worker.drain()
        self.assertEqual(enqueue.call_count, 2)
        self.assertEqual(len(worker.work), 0)
        worker.request_stop()
        self.assertTrue(worker.join())
        with self.assertRaises(RuntimeError):
            worker.start()


if __name__ == '__main__':
    unittest.main()

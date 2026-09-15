"""No credentials, network or production mutations."""
import os
import threading
import unittest
from unittest.mock import MagicMock, patch

from order_forecast.scripts import order_send_reminder_refresh as refresh
from order_forecast.scripts import order_adjustment_reminder_worker as worker
from order_forecast.scripts import config_sync_listener as listener


class RefreshTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(listener, 'read_route_send_view', return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_default_off_and_malformed_gate_do_not_touch_database(self):
        db = MagicMock()
        for flag, routes in [('false', '989262'), ('TRUE', '989262'), ('true', '*'), ('true', ''),
                             ('true', ' 989262'), ('true', '989262,'), ('true', '989262,989262'),
                             ('true', ','.join(str(i) for i in range(11)))]:
            with patch.dict(os.environ, {refresh.ENABLED_ENV: flag, refresh.ROUTES_ENV: routes}):
                self.assertEqual(refresh.pilot_routes(), ())
                self.assertEqual(refresh.run_pending(db, now_ms=1), {})
                self.assertEqual(refresh.enqueue_current(db, 'owner', '989262'), 'disabled')
        self.assertEqual(db.mock_calls, [])

    def test_one_failed_route_does_not_starve_other_pending_work(self):
        db = MagicMock()
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value.exists = False
        with patch.dict(os.environ, {refresh.ENABLED_ENV: 'true', refresh.ROUTES_ENV: '989262,989263'}), \
                patch.object(refresh, 'process_page', side_effect=[RuntimeError('offline'), {'status': 'complete'}]):
            result = refresh.run_pending(db, now_ms=1)
        self.assertEqual(result, {'989262': {'status': 'retry'}, '989263': {'status': 'complete'}})

    def test_two_failed_member_events_on_same_route_are_both_retried(self):
        manager = listener.ConfigSyncManager(MagicMock())
        manager._send_worker.work._retry_delay = 0
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
                patch.object(listener, 'enqueue_send_reminder_refresh', side_effect=[RuntimeError(), RuntimeError(), 'queued', 'queued']) as enqueue:
            manager.enqueue_send_reminder_settings('member-a', '989262')
            manager.enqueue_send_reminder_settings('member-b', '989262')
            enqueue.assert_not_called()
            manager._send_worker.drain()
            manager._send_worker.drain()
            self.assertEqual(len(manager._send_worker.work), 2)
            manager._send_worker.drain()
            manager._send_worker.drain()
            self.assertEqual(len(manager._send_worker.work), 0)
            self.assertEqual([call.args[1] for call in enqueue.call_args_list], ['member-a', 'member-b', 'member-a', 'member-b'])

    def test_member_query_event_uses_requested_route_and_live_actor_enqueue(self):
        db = MagicMock()
        manager = listener.ConfigSyncManager(db)
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
                patch.object(listener, 'enqueue_send_reminder_refresh') as enqueue:
            manager.start_send_member_listener('989262')
            query = db.collection.return_value.where
            self.assertEqual(query.call_args.kwargs['filter'].field_path, 'routeAssignments.`989262`.role')
            callback = query.return_value.on_snapshot.call_args.args[0]
            snapshot = MagicMock(id='member-a')
            callback([snapshot], [], None)
            enqueue.assert_not_called()
            manager._send_worker.drain()
            enqueue.assert_called_once_with(db, 'member-a', '989262')

    def test_listener_enqueue_failure_retries_without_new_settings_event(self):
        manager = listener.ConfigSyncManager(MagicMock())
        manager._send_worker.work._retry_delay = 0
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
                patch.object(listener, 'enqueue_send_reminder_refresh', side_effect=[RuntimeError('offline'), 'queued']) as enqueue:
            manager.enqueue_send_reminder_settings('owner', '989262')
            enqueue.assert_not_called()
            manager._send_worker.drain()
            self.assertEqual(len(manager._send_worker.work), 1)
            manager._send_worker.drain()
            self.assertEqual(len(manager._send_worker.work), 0)
            self.assertEqual(enqueue.call_count, 2)

    def test_dry_run_cannot_enter_refresh_or_mutation_path(self):
        with patch.object(worker, 'ORDER_ADJUSTMENT_REMINDER_DRY_RUN', True), \
                patch.object(worker, 'refresh_send_reminders') as process:
            with self.assertRaisesRegex(RuntimeError, 'Dry-run'):
                worker.run_once(MagicMock(), now_ms=1)
            process.assert_not_called()

    def test_existing_user_snapshot_queues_even_when_only_reminder_settings_changed(self):
        reference = MagicMock()
        manager = listener.ConfigSyncManager(reference)
        data = {'userSettings': {'notifications': {'scheduling': {'orderCycles': []},
            'orderAdjustmentSettings': {'cutoffTimeLocal': '09:30'}}}}
        snapshot = MagicMock(exists=True)
        snapshot.to_dict.return_value = {'stale': 'event is a hint, not authority'}
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
                patch.object(listener, 'enqueue_send_reminder_refresh', return_value='queued') as enqueue, \
                patch.object(listener, 'read_current_schedule', return_value=('owner', data, None)), \
                patch.object(listener, 'sync_user_schedules_to_pg', return_value=True):
            manager.start_schedules_listener('owner', '989262')
            callback = reference.collection.return_value.document.return_value.on_snapshot.call_args.args[0]
            callback([snapshot], [], None)
            enqueue.assert_not_called()
            manager._send_worker.drain()
            enqueue.assert_called_once_with(reference, 'owner', '989262')


if __name__ == '__main__': unittest.main()

"""Real listener functions with injected I/O. No daemon startup or live access."""
import os
import unittest
from unittest.mock import MagicMock, patch

from order_forecast.scripts import order_sync_listener as orders
from order_forecast.scripts import schedule_forecast_refresh as refresh
from order_forecast.scripts import forecast_generation_queue as queue
from order_forecast.scripts import forecast_contract
from order_forecast.scripts.schedule_sync import ScheduleSyncResult


CYCLE = {'scheduleVersion': 3, 'orderDay': 2, 'sendOffsetDays': 2, 'loadOffsetDays': 6,
         'deliveryOffsetDays': 6, 'loadDay': 1, 'deliveryDay': 1, 'needsScheduleReview': False}
SOURCE = {'control': {'version': 3, 'revision': 2, 'routeNumber': '988200',
                      'ownerUid': 'test-owner', 'timezone': 'America/Denver'}, 'cycles': [CYCLE]}


class ListenerIntegrationTests(unittest.TestCase):
    def test_pending_and_blocked_schedule_cannot_mark_route_ready(self):
        for outcome in ('pending', 'blocked'):
            with self.subTest(outcome=outcome), patch.object(orders, 'mark_route_syncing'), patch.object(
                orders, 'sync_stores_from_firebase', return_value=1
            ), patch.object(orders, 'sync_products_from_firebase', return_value=1), patch.object(
                orders, 'sync_schedules_from_firebase', return_value=ScheduleSyncResult(outcome)
            ), patch.object(orders, 'mark_route_ready') as ready:
                self.assertFalse(orders.sync_full_route(MagicMock(), '988200', 'test-owner', 'order'))
                ready.assert_not_called()

    def test_valid_empty_schedule_can_mark_route_ready(self):
        with patch.object(orders, 'mark_route_syncing'), patch.object(orders, 'sync_stores_from_firebase', return_value=1), patch.object(
            orders, 'sync_products_from_firebase', return_value=1
        ), patch.object(orders, 'sync_schedules_from_firebase', return_value=ScheduleSyncResult('legacy_synced', 0)), patch.object(
            orders, 'mark_route_ready'
        ) as ready:
            self.assertTrue(orders.sync_full_route(MagicMock(), '988200', 'test-owner', 'order'))
            ready.assert_called_once()

    def test_order_listener_dispatches_v3_without_opening_legacy_connection(self):
        db = MagicMock()
        with patch.object(orders, 'read_current_schedule', return_value=('test-owner', {'profile': {}}, SOURCE)), patch.object(
            orders, 'sync_current_v3', return_value=ScheduleSyncResult('already_projected', 1, 2)
        ) as sync, patch.object(orders, '_new_schedule_connection') as legacy:
            result = orders.sync_schedules_from_firebase(db, '988200', 'stale-owner')
            self.assertTrue(result.ready)
            sync.assert_called_once_with(db, '988200', SOURCE)
            legacy.assert_not_called()

    def test_missing_source_is_not_a_successful_empty_schedule(self):
        with patch.object(orders, 'read_current_schedule', return_value=('test-owner', {}, None)), patch.object(
            orders, '_new_schedule_connection'
        ) as legacy:
            self.assertFalse(orders.sync_schedules_from_firebase(MagicMock(), '988200', 'test-owner').ready)
            legacy.assert_not_called()

    def test_legacy_sql_failure_is_explicit(self):
        with patch.object(orders, 'read_current_schedule', return_value=('test-owner', {'profile': {}}, None)), patch.object(
            orders, '_new_schedule_connection', side_effect=RuntimeError('offline')
        ):
            self.assertEqual(orders.sync_schedules_from_firebase(MagicMock(), '988200', 'test-owner').status, 'pending')

    def test_forecast_connection_requires_explicit_configuration(self):
        with patch.dict(os.environ, {'ORDER_SCHEDULE_FORECAST_DSN': ''}), patch.object(refresh.psycopg2, 'connect') as connect:
            with self.assertRaisesRegex(RuntimeError, 'not_configured'):
                refresh.connect_forecast_queue()
            connect.assert_not_called()

    def test_refresh_enqueues_exact_target_without_schema_initialization(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (False,)
        connect = MagicMock(return_value=connection)
        with patch.object(refresh, 'read_current_schedule', return_value=('test-owner', {}, SOURCE)), patch.object(
            forecast_contract, 'load_authority_generation_state', return_value=(set(), 'fingerprint')
        ), patch.object(queue, 'enqueue_generation_job', return_value={'job_key': 'durable'}) as enqueue:
            self.assertEqual(refresh.enqueue_schedule_refresh(MagicMock(), SOURCE, connect), 1)
            args, kwargs = enqueue.call_args
            self.assertEqual(args[:2], ('988200', 'tuesday'))
            self.assertFalse(kwargs['ensure_schema'])
            self.assertIs(kwargs['connect'], connect)
            self.assertEqual(kwargs['desired_revision'], 'fingerprint')
        connection.close.assert_called_once()

    def test_forecast_connection_rejects_superuser_and_closes(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (True,)
        with patch.dict(os.environ, {'ORDER_SCHEDULE_FORECAST_DSN': 'synthetic'}), patch.object(
            refresh.psycopg2, 'connect', return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, 'non_superuser'):
                refresh.connect_forecast_queue()
        connection.close.assert_called_once()

    def test_unconfirmed_enqueue_fails_instead_of_consuming_intent(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (False,)
        with patch.object(refresh, 'read_current_schedule', return_value=('test-owner', {}, SOURCE)), patch.object(
            forecast_contract, 'load_authority_generation_state', return_value=(set(), 'fingerprint')
        ), patch.object(queue, 'enqueue_generation_job', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'not_confirmed'):
                refresh.enqueue_schedule_refresh(MagicMock(), SOURCE, lambda: connection)

    def test_queue_injected_connection_never_uses_default_or_schema_creation(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.fetchone.return_value = {'job_key': 'durable'}
        with patch.object(queue, '_pg_connect') as default, patch.object(queue, 'ensure_forecast_queue_tables') as schema:
            self.assertEqual(queue.enqueue_generation_job('988200', 'tuesday', '2026-09-14', 'test',
                             ensure_schema=False, connect=lambda: connection), {'job_key': 'durable'})
            default.assert_not_called()
            schema.assert_not_called()


if __name__ == '__main__':
    unittest.main()

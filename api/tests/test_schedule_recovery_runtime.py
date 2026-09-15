import os
from threading import Event
import unittest
from unittest.mock import Mock, patch

from order_forecast.api import schedule_recovery
from order_forecast.scripts import schedule_recovery_worker as worker


class RecoveryRuntimeTests(unittest.TestCase):
    def test_disabled_runtime_does_not_initialize_cloud_or_database(self):
        with patch.dict(os.environ, {worker.ENABLE_ENV: ''}), patch.object(worker.firestore.Client, 'from_service_account_json') as cloud, patch.object(worker.psycopg2, 'connect') as connect:
            self.assertEqual(worker.main([]), 0)
            cloud.assert_not_called()
            connect.assert_not_called()

    def test_database_configuration_is_explicit(self):
        with patch.dict(os.environ, {worker.DSN_ENV: ''}), patch.object(worker.psycopg2, 'connect') as connect:
            with self.assertRaises(RuntimeError):
                worker.connect_projector()
            connect.assert_not_called()

    def test_one_shot_reports_query_failure(self):
        with patch.object(worker, 'recover_due', side_effect=RuntimeError('synthetic')):
            self.assertEqual(worker.run(object(), Mock(), Event(), once=True), 1)

    def test_write_pause_does_not_disable_existing_recovery(self):
        with patch.dict(os.environ, {'ORDER_SCHEDULE_V3_SOURCE_WRITES_ENABLED': ''}), patch.object(worker, 'recover_due', return_value=['projected']) as recover:
            self.assertEqual(worker.run(object(), Mock(), Event(), once=True), 0)
            recover.assert_called_once()

    def test_batch_uses_fresh_claim_clock_not_query_clock(self):
        db = Mock()
        query = db.collection.return_value
        query.where.return_value = query
        query.order_by.return_value = query
        query.limit.return_value = query
        snapshot = Mock()
        snapshot.reference.path = 'orderScheduleProjectionWork/000001'
        query.stream.return_value = [snapshot, snapshot]
        with patch.object(schedule_recovery, 'process_one', return_value='projected') as process:
            self.assertEqual(schedule_recovery.recover_due(db, Mock()), ['projected', 'projected'])
            self.assertTrue(all(call.args[3] is None for call in process.call_args_list))
            db.collection.assert_called_once_with('orderScheduleProjectionWork')
            db.collection_group.assert_not_called()

    def test_stop_event_prevents_another_batch(self):
        stopped = Event()
        stopped.set()
        with patch.object(worker, 'recover_due') as recover:
            self.assertEqual(worker.run(object(), Mock(), stopped), 0)
            recover.assert_not_called()

    def test_claim_failure_is_retryable_and_does_not_stop_other_due_work(self):
        db = Mock()
        query = db.collection.return_value
        query.where.return_value = query
        query.order_by.return_value = query
        query.limit.return_value = query
        snapshot = Mock()
        snapshot.reference.path = 'orderScheduleProjectionWork/000001'
        query.stream.return_value = [snapshot, snapshot]
        with patch.object(schedule_recovery, 'process_one', side_effect=[ValueError('transaction attempts exhausted'), 'projected']):
            self.assertEqual(schedule_recovery.recover_due(db, Mock()), ['retryable_failure', 'projected'])

    def test_one_shot_reports_per_record_failures(self):
        with patch.object(worker, 'recover_due', return_value=['projected', 'pending']):
            self.assertEqual(worker.run(object(), Mock(), Event(), once=True), 1)


if __name__ == '__main__':
    unittest.main()

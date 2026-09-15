"""Failure injection only: no credentials, Firestore or PostgreSQL connections."""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from order_forecast.scripts import config_sync_listener as listener
from order_forecast.scripts.config_sync_retry import LatestConfigWork


def change(identifier, value, kind='MODIFIED'):
    return SimpleNamespace(type=SimpleNamespace(name=kind),
        document=SimpleNamespace(id=identifier, to_dict=lambda: value))


class ConfigRetryTests(unittest.TestCase):
    def manager(self, kind):
        db = MagicMock()
        manager = listener.ConfigSyncManager(db)
        manager._config_work = LatestConfigWork(retry_delay=0)
        manager._schedule_forecast_refresh = MagicMock()
        if kind == 'product':
            manager.start_products_listener('988200')
            callback = db.collection.return_value.document.return_value.collection.return_value.on_snapshot.call_args.args[0]
        else:
            manager.start_stores_listener('988200')
            callback = db.collection.return_value.document.return_value.collection.return_value.on_snapshot.call_args.args[0]
        return manager, callback

    def test_timeout_keeps_watch_alive_and_retries_without_another_event(self):
        for kind in ('product', 'store'):
            manager, callback = self.manager(kind)
            with patch.object(listener, f'sync_{kind}_to_pg', side_effect=[TimeoutError('slots'), True]) as sync:
                callback([], [change('one', {'casePack': 12}, 'ADDED')], None)
                signatures = manager._product_signatures if kind == 'product' else manager._store_signatures
                self.assertNotIn(('988200', 'one'), signatures)
                manager.retry_config_documents()
                self.assertEqual(sync.call_count, 2)
                self.assertIn(('988200', 'one'), signatures)
            manager._schedule_forecast_refresh.assert_not_called()

    def test_real_sync_connection_acquisition_failure_is_contained(self):
        for kind in ('product', 'store'):
            manager, callback = self.manager(kind)
            with patch.object(listener, 'get_pg_connection', side_effect=TimeoutError('slots')) as connection:
                callback([], [change('one', {'casePack': 12}, 'ADDED')], None)
                connection.assert_called_once()
                self.assertEqual(len(manager._config_work), 1)
            with patch.object(listener, f'sync_{kind}_to_pg', return_value=True):
                manager.retry_config_documents()
            self.assertEqual(len(manager._config_work), 0)

    def test_false_result_retries_latest_value_not_stale_payload(self):
        manager, callback = self.manager('product')
        with patch.object(listener, 'sync_product_to_pg', side_effect=[False, True]) as sync:
            callback([], [change('one', {'casePack': 12}, 'ADDED')], None)
            callback([], [change('one', {'casePack': 24})], None)
            manager.retry_config_documents()
        self.assertEqual(sync.call_count, 2)
        self.assertEqual(sync.call_args.args[2]['casePack'], 24)
        manager._schedule_forecast_refresh.assert_called_once()

    def test_large_snapshot_drains_without_new_event_and_copies_payload(self):
        manager, callback = self.manager('product')
        value = {'casePack': 12}
        with patch.object(listener, 'sync_product_to_pg', return_value=True) as sync:
            callback([], [change(str(i), value, 'ADDED') for i in range(60)], None)
            self.assertEqual(sync.call_count, 25)
            value['casePack'] = 99
            manager.retry_config_documents()
            manager.retry_config_documents()
        self.assertEqual(sync.call_count, 60)
        self.assertTrue(all(call.args[2]['casePack'] == 12 for call in sync.call_args_list))
        self.assertEqual(len(manager._config_work), 0)
        manager._schedule_forecast_refresh.assert_not_called()

    def test_failed_first_document_does_not_drop_remaining_snapshot(self):
        manager, callback = self.manager('store')
        with patch.object(listener, 'sync_store_to_pg', side_effect=[TimeoutError('slots'), True, True]) as sync:
            callback([], [change('one', {}, 'ADDED'), change('two', {}, 'ADDED')], None)
            self.assertIn(('988200', 'two'), manager._store_signatures)
            self.assertNotIn(('988200', 'one'), manager._store_signatures)
            manager.retry_config_documents()
        self.assertEqual(sync.call_count, 3)
        self.assertEqual(len(manager._config_work), 0)

    def test_removal_is_retained_until_success(self):
        manager, callback = self.manager('store')
        with patch.object(listener, 'sync_store_to_pg', side_effect=[True, False, True]) as sync:
            callback([], [change('one', {}, 'ADDED')], None)
            callback([], [change('one', {}, 'REMOVED')], None)
            self.assertIn(('988200', 'one'), manager._store_signatures)
            manager.retry_config_documents()
        self.assertTrue(sync.call_args.kwargs['deleted'])
        self.assertNotIn(('988200', 'one'), manager._store_signatures)
        manager._schedule_forecast_refresh.assert_called_once()


if __name__ == '__main__':
    unittest.main()

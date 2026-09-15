import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import config_sync_listener as listener


class _Document:
    def __init__(self, document_id, data):
        self.id = document_id
        self._data = data
        self.exists = True

    def to_dict(self):
        return dict(self._data)


class _Change:
    def __init__(self, change_type, document_id, data):
        self.type = SimpleNamespace(name=change_type)
        self.document = _Document(document_id, data)


class _WatchReference:
    def __init__(self):
        self.callbacks = []

    def collection(self, _name):
        return self

    def document(self, _name):
        return self

    def on_snapshot(self, callback):
        self.callbacks.append(callback)
        return MagicMock()


class _RouteAuthorityReference(_WatchReference):
    def __init__(self, route_owner):
        super().__init__()
        self.route_owner = route_owner
        self.document_ids = []

    def document(self, document_id):
        self.document_ids.append(document_id)
        return self

    def get(self):
        return _Document(
            self.document_ids[-1] if self.document_ids else 'route',
            {'ownerUid': self.route_owner},
        )


class ConfigSyncForecastEnqueueTests(unittest.TestCase):
    def test_member_watch_quotes_numeric_route_segment_in_wire_query(self):
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import firestore
        from google.cloud.firestore_v1.query import Query
        db = firestore.Client(project='demo-routespark-send-device-test', credentials=AnonymousCredentials())
        manager = listener.ConfigSyncManager(db)
        captured = []
        def watch(query, callback):
            captured.append(query._to_protobuf().where.field_filter.field.field_path)
            return MagicMock()
        with patch.object(listener, 'send_reminder_route_enabled', return_value=True), \
                patch.object(Query, 'on_snapshot', watch):
            manager.start_send_member_listener('988200')
        self.assertEqual(captured, ['routeAssignments.`988200`.role'])

    def setUp(self):
        patcher = patch.object(listener, 'read_route_send_view', return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def _schedule_data(cycles):
        return {
            "userSettings": {
                "notifications": {"scheduling": {"orderCycles": cycles}}
            }
        }

    def test_store_bootstrap_is_suppressed_and_only_authority_changes_refresh(self):
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        manager._schedule_forecast_refresh = MagicMock()
        with patch.object(listener, "sync_store_to_pg", return_value=True):
            manager.start_stores_listener("988200")
            callback = reference.callbacks[-1]
            baseline = {
                "name": "Store One",
                "deliveryDays": ["thursday"],
                "activeItems": [{"sap": "100"}, {"sap": "200", "active": False}],
            }
            callback(None, [_Change("ADDED", "store-1", baseline)], None)
            manager._schedule_forecast_refresh.assert_not_called()

            renamed = {**baseline, "name": "Renamed Store"}
            callback(None, [_Change("MODIFIED", "store-1", renamed)], None)
            manager._schedule_forecast_refresh.assert_not_called()

            changed = {**renamed, "activeItems": [{"sap": "100"}, {"sap": "300"}]}
            callback(None, [_Change("MODIFIED", "store-1", changed)], None)

        manager._schedule_forecast_refresh.assert_called_once_with(
            "988200", "store_or_carry_change"
        )

    def test_route_document_owner_overrides_stale_routes_synced_user(self):
        reference = _RouteAuthorityReference('firebase-owner')
        manager = listener.ConfigSyncManager(reference)
        manager.start_stores_listener = MagicMock()
        manager.start_products_listener = MagicMock()
        manager.start_schedules_listener = MagicMock()

        manager.start_route_listeners('988200', 'stale-pg-user')

        manager.start_schedules_listener.assert_called_once_with(
            'firebase-owner', '988200'
        )
        self.assertIn('988200', manager.known_routes)

    def test_route_owner_resolution_falls_back_when_route_document_is_unavailable(self):
        reference = MagicMock()
        reference.collection.side_effect = RuntimeError('firestore unavailable')

        resolved = listener.resolve_route_owner_user_id(
            reference,
            '988200',
            'fallback-user',
        )

        self.assertEqual(resolved, 'fallback-user')

    def test_product_name_change_coalesces_but_case_pack_change_refreshes(self):
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        manager._schedule_forecast_refresh = MagicMock()
        with patch.object(listener, "sync_product_to_pg", return_value=True):
            manager.start_products_listener("988200")
            callback = reference.callbacks[-1]
            baseline = {"sap": "100", "fullName": "Old", "casePack": 12, "active": True}
            callback(None, [_Change("ADDED", "100", baseline)], None)
            callback(None, [_Change("MODIFIED", "100", {**baseline, "fullName": "New"})], None)
            manager._schedule_forecast_refresh.assert_not_called()
            callback(None, [_Change("MODIFIED", "100", {**baseline, "casePack": 18})], None)

        manager._schedule_forecast_refresh.assert_called_once_with(
            "988200", "product_or_case_pack_change"
        )

    def test_debounce_cancels_previous_timer_and_enqueues_each_exact_target(self):
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        timers = []

        class _Timer:
            def __init__(self, _delay, callback):
                self.callback = callback
                self.cancelled = False
                self.daemon = False
                timers.append(self)

            def start(self):
                return None

            def cancel(self):
                self.cancelled = True

        targets = [
            {"delivery_date": "2026-08-13", "schedule_key": "tuesday"},
            {"delivery_date": "2026-08-17", "schedule_key": "friday"},
        ]
        with patch.object(listener.threading, "Timer", _Timer), patch(
            "forecast_generation_queue.derive_upcoming_generation_targets",
            return_value=targets,
        ), patch(
            "forecast_generation_queue.enqueue_generation_job"
        ) as enqueue, patch(
            "forecast_contract.load_authority_generation_state",
            side_effect=[(set(), "rev-1"), (set(), "rev-2")],
        ):
            manager._schedule_forecast_refresh("988200", "first")
            manager._schedule_forecast_refresh("988200", "second")
            self.assertTrue(timers[0].cancelled)
            timers[1].callback()

        self.assertEqual(enqueue.call_count, 2)
        self.assertEqual(enqueue.call_args_list[0].kwargs["desired_revision"], "rev-1")
        self.assertEqual(enqueue.call_args_list[1].kwargs["desired_revision"], "rev-2")

    def test_user_schedule_changes_and_removal_sync_then_refresh(self):
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        manager._schedule_forecast_refresh = MagicMock()
        monday = [{"orderDay": 1, "loadDay": 3, "deliveryDay": 4}]
        tuesday = [{"orderDay": 2, "loadDay": 4, "deliveryDay": 5}]
        with patch.object(
            listener, "sync_user_schedules_to_pg", return_value=True
        ) as sync, patch.object(listener, 'read_current_schedule', side_effect=[
            ('owner-1', self._schedule_data(cycles), None) for cycles in (monday, tuesday, [])
        ]):
            manager.start_schedules_listener("owner-1", "988200")
            callback = reference.callbacks[-1]
            callback([_Document("owner-1", self._schedule_data(monday))], None, None)
            manager._schedule_forecast_refresh.assert_not_called()

            callback([_Document("owner-1", self._schedule_data(tuesday))], None, None)
            manager._schedule_forecast_refresh.assert_called_once_with(
                "988200", "schedule_change"
            )

            callback([_Document("owner-1", self._schedule_data([]))], None, None)

        self.assertEqual(manager._schedule_forecast_refresh.call_count, 2)
        self.assertEqual(sync.call_args_list[-1].args, ("988200", "owner-1", []))

    def test_failed_schedule_does_not_advance_checkpoint_and_identical_retry_refreshes(self):
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        manager._schedule_forecast_refresh = MagicMock()
        cycles = [{"orderDay": 1, "loadDay": 3, "deliveryDay": 4}]
        data = self._schedule_data(cycles)
        with patch.object(listener, 'read_current_schedule', return_value=('owner-1', data, None)), patch.object(
            listener, 'sync_user_schedules_to_pg', side_effect=[False, True]
        ):
            manager.start_schedules_listener('owner-1', '988200')
            callback = reference.callbacks[-1]
            callback([_Document('owner-1', data)], None, None)
            self.assertNotIn('988200', manager._schedule_signatures)
            self.assertNotIn('988200', manager._schedules_initialized)
            callback([_Document('owner-1', data)], None, None)
        manager._schedule_forecast_refresh.assert_called_once_with('988200', 'schedule_change')

    def test_v3_uses_current_source_and_never_legacy_writer_or_timer(self):
        from schedule_sync import ScheduleSyncResult
        reference = _WatchReference()
        manager = listener.ConfigSyncManager(reference)
        manager._schedule_forecast_refresh = MagicMock()
        current = {'control': {'revision': 4}, 'cycles': []}
        with patch.object(listener, 'read_current_schedule', return_value=('new-owner', {'orderScheduleControl': {}}, current)), patch.object(
            listener, 'sync_current_v3', side_effect=[ScheduleSyncResult('pending'), ScheduleSyncResult('already_projected', 0, 4)]
        ) as v3, patch.object(listener, 'sync_user_schedules_to_pg') as legacy:
            manager.start_schedules_listener('old-owner', '988200')
            callback = reference.callbacks[-1]
            callback([_Document('old-owner', self._schedule_data([{'orderDay': 1}]))], None, None)
            self.assertNotIn('988200', manager._schedules_initialized)
            callback([_Document('old-owner', {})], None, None)
            self.assertIn('988200', manager._schedules_initialized)
            self.assertEqual(v3.call_args.args, (reference, '988200', current))
            legacy.assert_not_called()
        manager._schedule_forecast_refresh.assert_not_called()

    def test_product_sync_uses_generator_case_pack_default_and_closes_connection(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(listener, "get_pg_connection", return_value=conn), patch.object(
            listener, "close_pg_connection"
        ) as close:
            synced = listener.sync_product_to_pg(
                "988200", "54304", {"sap": "54304", "fullName": "No pack"}
            )

        self.assertTrue(synced)
        values = cursor.execute.call_args.args[1]
        self.assertEqual(values[8], 0)
        close.assert_called_once_with(conn)

    def test_schedule_sync_releases_its_connection_after_each_snapshot(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(listener, "get_pg_connection", return_value=conn), patch.object(
            listener, "close_pg_connection"
        ) as close:
            synced = listener.sync_user_schedules_to_pg(
                "988200",
                "owner-1",
                [{"orderDay": 1, "loadDay": 3, "deliveryDay": 4}],
            )

        self.assertTrue(synced)
        schedule_upsert_sql = cursor.execute.call_args_list[-1].args[0]
        self.assertIn("user_id = EXCLUDED.user_id", schedule_upsert_sql)
        close.assert_called_once_with(conn)


if __name__ == "__main__":
    unittest.main()

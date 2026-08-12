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


class ConfigSyncForecastEnqueueTests(unittest.TestCase):
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
        ) as sync:
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


if __name__ == "__main__":
    unittest.main()

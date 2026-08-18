import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from order_forecast.scripts import config_sync_listener as listener


def _user_snapshot(
    uid: str,
    route: str,
    *,
    enabled: bool = True,
    reminder_time=None,
    timezone_name: str = "America/Denver",
):
    snapshot = MagicMock()
    snapshot.id = uid
    snapshot.exists = True
    snapshot.to_dict.return_value = {
        "profile": {
            "currentRoute": route,
            "timezone": timezone_name,
        },
        "userSettings": {
            "notifications": {
                "orderReminders": {
                    "enabled": enabled,
                    "time": reminder_time
                    if reminder_time is not None
                    else {"hour": 8, "minute": 0, "period": "AM"},
                }
            }
        },
    }
    return snapshot


class LowQtyConfigSyncTests(unittest.TestCase):
    def test_listener_is_disabled_by_default_before_schema_or_firestore_query(self):
        manager = listener.ConfigSyncManager(MagicMock())
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LOW_QTY_PREFERENCE_SYNC_ENABLED", None)
            with patch.object(listener, "low_qty_schema_ready") as schema_ready:
                started = manager.start_low_qty_preference_listener()

        self.assertFalse(started)
        schema_ready.assert_not_called()
        manager.fb_client.collection.assert_not_called()

    def test_missing_schema_is_isolated_from_existing_config_sync(self):
        manager = listener.ConfigSyncManager(MagicMock())
        with patch.dict(os.environ, {"LOW_QTY_PREFERENCE_SYNC_ENABLED": "true"}), patch.object(
            listener,
            "low_qty_schema_ready",
            return_value=False,
        ):
            started = manager.start_low_qty_preference_listener()

        self.assertFalse(started)
        self.assertFalse(manager._low_qty_sync_ready)
        manager.fb_client.collection.assert_not_called()

    def test_complete_filtered_snapshot_keeps_only_authoritative_owner(self):
        fb_client = MagicMock()
        query = MagicMock()
        fb_client.collection.return_value.where.return_value = query
        captured = {}

        def capture_callback(callback):
            captured["callback"] = callback
            return MagicMock()

        query.on_snapshot.side_effect = capture_callback
        manager = listener.ConfigSyncManager(fb_client)
        owner_doc = _user_snapshot("owner-989262", "989262")
        non_owner_doc = _user_snapshot("team-989262", "989262")

        with patch.dict(os.environ, {"LOW_QTY_PREFERENCE_SYNC_ENABLED": "true"}), patch.object(
            listener,
            "low_qty_schema_ready",
            return_value=True,
        ), patch.object(
            listener,
            "resolve_authoritative_route_owner",
            return_value="owner-989262",
        ), patch.object(
            listener,
            "reconcile_complete_enabled_snapshot",
            return_value={"enabled": 1, "disabled": 0},
        ) as reconcile, patch.object(
            manager,
            "_reconcile_low_qty_owner_watchers",
        ) as reconcile_watches:
            self.assertTrue(manager.start_low_qty_preference_listener())
            captured["callback"](
                [owner_doc, non_owner_doc],
                [],
                datetime(2026, 8, 18, 13, 0, tzinfo=timezone.utc),
            )

        preferences = reconcile.call_args.args[0]
        self.assertEqual(len(preferences), 1)
        self.assertEqual(preferences[0].route_number, "989262")
        self.assertEqual(preferences[0].owner_uid, "owner-989262")
        reconcile_watches.assert_called_once_with({"989262"})

        field_filter = fb_client.collection.return_value.where.call_args.kwargs["filter"]
        self.assertEqual(field_filter.field_path, "userSettings.notifications.orderReminders.enabled")
        self.assertEqual(field_filter.op_string, "==")
        self.assertIs(field_filter.value, True)

    def test_invalid_owner_setting_is_omitted_so_old_row_is_disabled(self):
        manager = listener.ConfigSyncManager(MagicMock())
        invalid_owner = _user_snapshot(
            "owner-989262",
            "989262",
            reminder_time={"hour": 8, "minute": 0},
        )
        with patch.object(
            listener,
            "resolve_authoritative_route_owner",
            return_value="owner-989262",
        ), patch.object(
            listener,
            "reconcile_complete_enabled_snapshot",
            return_value={"enabled": 0, "disabled": 1},
        ) as reconcile, patch.object(manager, "_reconcile_low_qty_owner_watchers"):
            manager._sync_low_qty_snapshot(
                [invalid_owner],
                datetime(2026, 8, 18, 13, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(reconcile.call_args.args[0], [])

    def test_owner_resolution_error_does_not_run_disable_sweep(self):
        fb_client = MagicMock()
        query = MagicMock()
        fb_client.collection.return_value.where.return_value = query
        captured = {}
        query.on_snapshot.side_effect = lambda callback: captured.setdefault("callback", callback) or MagicMock()
        manager = listener.ConfigSyncManager(fb_client)

        with patch.dict(os.environ, {"LOW_QTY_PREFERENCE_SYNC_ENABLED": "true"}), patch.object(
            listener,
            "low_qty_schema_ready",
            return_value=True,
        ), patch.object(
            listener,
            "resolve_authoritative_route_owner",
            side_effect=RuntimeError("firestore unavailable"),
        ), patch.object(listener, "reconcile_complete_enabled_snapshot") as reconcile:
            self.assertTrue(manager.start_low_qty_preference_listener())
            captured["callback"](
                [_user_snapshot("owner-989262", "989262")],
                [],
                datetime(2026, 8, 18, 13, 0, tzinfo=timezone.utc),
            )

        reconcile.assert_not_called()

    def test_malformed_nested_settings_disable_stale_rows_without_crashing_snapshot(self):
        malformed = MagicMock()
        malformed.id = "owner-989262"
        malformed.exists = True
        malformed.to_dict.return_value = {
            "profile": {"currentRoute": "989262", "timezone": "America/Denver"},
            "userSettings": ["not", "an", "object"],
        }
        manager = listener.ConfigSyncManager(MagicMock())
        with patch.object(
            listener,
            "reconcile_complete_enabled_snapshot",
            return_value={"enabled": 0, "disabled": 1},
        ) as reconcile, patch.object(manager, "_reconcile_low_qty_owner_watchers"):
            manager._sync_low_qty_snapshot(
                [malformed],
                datetime(2026, 8, 18, 13, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(reconcile.call_args.args[0], [])


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import MagicMock, patch

from order_forecast.scripts import low_qty_notification_daemon as daemon


class LowQtyNotificationReliabilityTests(unittest.TestCase):
    def tearDown(self):
        daemon.reminder_cache.clear()

    def test_once_run_exits_with_error_when_user_processing_fails(self):
        with patch.object(daemon, "get_firestore_client", return_value=MagicMock()), patch.object(
            daemon,
            "load_reminder_cache_once",
            return_value=1,
        ), patch.object(daemon, "check_and_notify", return_value=1), patch.object(
            daemon,
            "LOW_QTY_NOTIFICATIONS_ENABLED",
            True,
        ):
            with self.assertRaisesRegex(RuntimeError, "failed for 1 user"):
                daemon.run_daemon("/tmp/service-account.json", run_once=True)

    def test_processing_exception_is_reported_to_once_runner(self):
        user_id = "owner-1"
        route_number = "989262"
        daemon.reminder_cache[user_id] = {
            "route_number": route_number,
            "reminder_time": {"hour": 8, "minute": 0, "period": "AM"},
            "timezone": "America/Denver",
        }

        with patch.object(daemon, "is_reminder_time_due", return_value=True), patch.object(
            daemon,
            "get_route_owner",
            return_value=user_id,
        ), patch.object(
            daemon,
            "get_items_for_order_date",
            side_effect=RuntimeError("database unavailable"),
        ):
            failure_count = daemon.check_and_notify(MagicMock())

        self.assertEqual(failure_count, 1)

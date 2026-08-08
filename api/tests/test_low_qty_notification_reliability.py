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

    def test_push_delivery_uses_receipts_and_identifies_dead_tokens(self):
        stale_token = "ExponentPushToken[stale]"
        live_token = "ExponentPushToken[live]"
        response = MagicMock()
        response.json.return_value = {
            "data": [
                {"status": "ok", "id": "ticket-stale"},
                {"status": "ok", "id": "ticket-live"},
            ]
        }

        with patch.object(daemon.requests, "post", return_value=response), patch.object(
            daemon,
            "_wait_for_push_receipts",
            return_value={
                "ticket-stale": {
                    "status": "error",
                    "details": {"error": "DeviceNotRegistered"},
                },
                "ticket-live": {"status": "ok"},
            },
        ):
            result = daemon.send_push_notification(
                [stale_token, live_token],
                "Low Stock Alert",
                "2 items need to be ordered today",
                {"type": "low_quantity"},
            )

        self.assertTrue(result.successful)
        self.assertEqual(result.delivered_count, 1)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.invalid_tokens, [stale_token])

    def test_push_delivery_does_not_succeed_when_all_receipts_fail(self):
        token = "ExponentPushToken[stale]"
        response = MagicMock()
        response.json.return_value = {
            "data": [{"status": "ok", "id": "ticket-stale"}]
        }

        with patch.object(daemon.requests, "post", return_value=response), patch.object(
            daemon,
            "_wait_for_push_receipts",
            return_value={
                "ticket-stale": {
                    "status": "error",
                    "details": {
                        "error": "DeveloperError",
                        "apns": {"reason": "BadDeviceToken"},
                    },
                }
            },
        ):
            result = daemon.send_push_notification(
                [token],
                "Low Stock Alert",
                "1 item needs to be ordered today",
                {"type": "low_quantity"},
            )

        self.assertFalse(result.successful)
        self.assertEqual(result.invalid_tokens, [token])

    def test_push_delivery_keeps_pending_ticket_deduplicated(self):
        token = "ExponentPushToken[pending]"
        response = MagicMock()
        response.json.return_value = {
            "data": [{"status": "ok", "id": "ticket-pending"}]
        }

        with patch.object(daemon.requests, "post", return_value=response), patch.object(
            daemon,
            "_wait_for_push_receipts",
            return_value={},
        ):
            result = daemon.send_push_notification(
                [token],
                "Low Stock Alert",
                "1 item needs to be ordered today",
                {"type": "low_quantity"},
            )

        self.assertTrue(result.successful)
        self.assertEqual(result.pending_count, 1)

    def test_invalid_push_tokens_are_removed_with_array_transform(self):
        db = MagicMock()

        daemon.remove_invalid_push_tokens(
            db,
            "owner-1",
            ["ExponentPushToken[stale]", "ExponentPushToken[stale]"],
        )

        update = db.collection.return_value.document.return_value.update
        update.assert_called_once()
        payload = update.call_args.args[0]
        self.assertIn("fcmTokens", payload)
        self.assertEqual(
            payload["fcmTokens"]._values,
            ["ExponentPushToken[stale]"],
        )

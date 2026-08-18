import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from order_forecast.scripts import low_qty_notification_daemon as daemon


class LowQtyNotificationReliabilityTests(unittest.TestCase):
    def test_once_run_exits_with_error_when_user_processing_fails(self):
        with patch.object(daemon, "get_firestore_client", return_value=MagicMock()), patch.object(
            daemon,
            "check_and_notify_postgres",
            return_value=1,
        ), patch.dict(
            os.environ,
            {
                "LOW_QTY_NOTIFICATIONS_ENABLED": "true",
                "LOW_QTY_RECIPIENT_SOURCE": "postgres",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "failed for 1 claim"):
                daemon.run_daemon("/tmp/service-account.json", run_once=True)

    def test_disabled_by_default_exits_before_firestore_initialization(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LOW_QTY_NOTIFICATIONS_ENABLED", None)
            with patch.object(daemon, "get_firestore_client") as get_firestore_client:
                daemon.run_daemon("/tmp/service-account.json", run_once=True)

        get_firestore_client.assert_not_called()

    def test_legacy_supervisors_do_not_launch_low_qty_daemon(self):
        root = Path(__file__).resolve().parents[3]
        runtime_supervisor = (root / "order_forecast" / "runtime_supervisor.py").read_text()
        supervisor = (root / "order_forecast" / "supervisor.py").read_text()
        supervisor_docker = (root / "order_forecast" / "supervisor_docker.py").read_text()
        deploy_supervisor_docker = (
            root / "deploy" / "order-forecast" / "supervisor_docker.py"
        ).read_text()

        self.assertNotIn("low_qty_notification_daemon.py", runtime_supervisor)
        # The remaining references are termination cleanup patterns, not launch entries.
        self.assertEqual(supervisor.count("low_qty_notification_daemon.py"), 1)
        self.assertEqual(supervisor_docker.count("low_qty_notification_daemon.py"), 1)
        self.assertEqual(deploy_supervisor_docker.count("low_qty_notification_daemon.py"), 1)

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
            "_fetch_push_receipts",
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

    def test_expo_token_validation_rejects_malformed_values(self):
        self.assertTrue(daemon.is_valid_expo_token("ExpoPushToken[valid-123]"))
        self.assertTrue(daemon.is_valid_expo_token("ExponentPushToken[valid-123]"))
        for value in (None, 123, "ExpoPushToken[]", "ExpoPushToken[has space]", "ExpoPushToken[open"):
            with self.subTest(value=value):
                self.assertFalse(daemon.is_valid_expo_token(value))

    def test_push_delivery_does_not_succeed_when_all_receipts_fail(self):
        token = "ExponentPushToken[stale]"
        response = MagicMock()
        response.json.return_value = {
            "data": [{"status": "ok", "id": "ticket-stale"}]
        }

        with patch.object(daemon.requests, "post", return_value=response), patch.object(
            daemon,
            "_fetch_push_receipts",
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
            "_fetch_push_receipts",
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
        self.assertEqual(result.accepted_ticket_ids, ["ticket-pending"])

    def test_missing_source_fails_before_firestore_initialization(self):
        with patch.dict(
            os.environ,
            {"LOW_QTY_NOTIFICATIONS_ENABLED": "true"},
            clear=False,
        ):
            os.environ.pop("LOW_QTY_RECIPIENT_SOURCE", None)
            with patch.object(daemon, "get_firestore_client") as get_firestore_client:
                with self.assertRaisesRegex(RuntimeError, "LOW_QTY_RECIPIENT_SOURCE"):
                    daemon.run_daemon("/tmp/service-account.json", run_once=True)
        get_firestore_client.assert_not_called()

    def test_postgres_source_never_scans_firebase_users(self):
        db = MagicMock()
        with patch.dict(
            os.environ,
            {
                "LOW_QTY_NOTIFICATIONS_ENABLED": "true",
                "LOW_QTY_RECIPIENT_SOURCE": "postgres",
            },
        ), patch.object(daemon, "get_firestore_client", return_value=db), patch.object(
            daemon,
            "sync_firebase_recipient_snapshot",
        ) as firebase_sync, patch.object(
            daemon,
            "check_and_notify_postgres",
            return_value=0,
        ) as check_due:
            daemon.run_daemon("/tmp/service-account.json", run_once=True)

        firebase_sync.assert_not_called()
        check_due.assert_called_once_with(db, late_tolerance_minutes=20)

    def test_firebase_rollback_source_materializes_before_shared_claim_path(self):
        db = MagicMock()
        call_order = []
        with patch.dict(
            os.environ,
            {
                "LOW_QTY_NOTIFICATIONS_ENABLED": "true",
                "LOW_QTY_RECIPIENT_SOURCE": "firebase",
            },
        ), patch.object(daemon, "get_firestore_client", return_value=db), patch.object(
            daemon,
            "sync_firebase_recipient_snapshot",
            side_effect=lambda *_args, **_kwargs: call_order.append("sync"),
        ), patch.object(
            daemon,
            "check_and_notify_postgres",
            side_effect=lambda *_args, **_kwargs: call_order.append("claim") or 0,
        ):
            daemon.run_daemon("/tmp/service-account.json", run_once=True)

        self.assertEqual(call_order, ["sync", "claim"])

    def test_firebase_rollback_uses_only_the_enabled_users_query(self):
        db = MagicMock()
        db.collection.return_value.where.return_value.stream.return_value = []
        with patch.object(
            daemon,
            "reconcile_complete_enabled_snapshot",
            return_value={"enabled": 0, "disabled": 0},
        ):
            daemon.sync_firebase_recipient_snapshot(
                db,
                now_utc=daemon.datetime.now(daemon.timezone.utc),
            )

        field_filter = db.collection.return_value.where.call_args.kwargs["filter"]
        self.assertEqual(
            field_filter.field_path,
            "userSettings.notifications.orderReminders.enabled",
        )
        self.assertEqual(field_filter.op_string, "==")
        self.assertIs(field_filter.value, True)

    def test_manifests_preserve_migration_and_deadline_invariants(self):
        root = Path(__file__).resolve().parents[3]
        cronjob = (root / "k8s" / "base" / "low-qty-notifications" / "cronjob.yaml").read_text()
        schema_job = (root / "k8s" / "base" / "low-qty-notifications" / "schema-job.yaml").read_text()
        config = (root / "k8s" / "overlays" / "lab" / "low-qty-notifications-configmap.yaml").read_text()

        self.assertIn('schedule: "*/5 * * * *"', cronjob)
        self.assertIn("activeDeadlineSeconds: 240", cronjob)
        self.assertIn("suspend: true", schema_job)
        self.assertIn('LOW_QTY_NOTIFICATION_DRY_RUN: "false"', config)
        self.assertIn('LOW_QTY_RECIPIENT_SOURCE: "firebase"', config)
        self.assertIn('LOW_QTY_CLAIM_LEASE_SECONDS: "300"', config)

    def test_scheduled_worker_rejects_dry_run_before_firestore(self):
        with patch.dict(
            os.environ,
            {
                "LOW_QTY_NOTIFICATIONS_ENABLED": "true",
                "LOW_QTY_RECIPIENT_SOURCE": "postgres",
            },
        ), patch.object(daemon, "LOW_QTY_NOTIFICATION_DRY_RUN", True), patch.object(
            daemon,
            "get_firestore_client",
        ) as get_firestore_client:
            with self.assertRaisesRegex(RuntimeError, "cannot run in dry-run"):
                daemon.run_daemon("/tmp/service-account.json", run_once=True)
        get_firestore_client.assert_not_called()

    def test_non_once_worker_path_is_rejected_before_firestore(self):
        with patch.dict(
            os.environ,
            {"LOW_QTY_NOTIFICATIONS_ENABLED": "true"},
        ), patch.object(daemon, "get_firestore_client") as get_firestore_client:
            with self.assertRaisesRegex(RuntimeError, "only the --once CronJob"):
                daemon.run_daemon("/tmp/service-account.json", run_once=False)
        get_firestore_client.assert_not_called()

    def test_operator_preview_has_no_real_send_option(self):
        root = Path(__file__).resolve().parents[3]
        preview_source = (root / "order_forecast" / "scripts" / "test_low_qty_send_now.py").read_text()
        self.assertNotIn("--real-send", preview_source)
        self.assertNotIn("send_push_notification", preview_source)
        self.assertNotIn("check_and_notify(", preview_source)
        self.assertIn("resolved_timezone=timezone_name", preview_source)

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

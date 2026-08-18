import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from order_forecast.scripts import low_qty_notification_daemon as daemon
from order_forecast.scripts.low_qty_notification_store import ClaimedExecution


def _claim(*, payload=None, saps=None) -> ClaimedExecution:
    return ClaimedExecution(
        route_number="989262",
        scheduled_local_date=date(2026, 8, 18),
        scheduled_for_utc=datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        claimed_preference_version=4,
        owner_uid="owner-989262",
        reminder_minute_local=8 * 60,
        timezone_name="America/Denver",
        claim_token="claim-token",
        attempt_count=1,
        computed_payload=payload,
        computed_saps=saps,
    )


def _payload() -> dict:
    return {
        "title": "Low Stock Alert",
        "body": "1 item needs to be ordered today",
        "data": {
            "type": "low_quantity",
            "routeNumber": "989262",
            "orderDate": "2026-08-18",
            "saps": ["24521"],
        },
    }


class LowQtyNotificationWorkerTests(unittest.TestCase):
    def test_policy_exclusion_closes_before_firestore_or_pcf(self):
        claim = _claim()
        with patch.object(daemon, "_route_allowed", return_value=False), patch.object(
            daemon,
            "complete_claim",
            return_value=True,
        ) as complete, patch.object(
            daemon,
            "_current_claim_owner",
        ) as current_owner, patch.object(
            daemon,
            "get_items_for_order_date",
        ) as load_items:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        self.assertEqual(complete.call_args.kwargs["reason"], "policy_excluded")
        current_owner.assert_not_called()
        load_items.assert_not_called()

    def test_empty_inventory_closes_after_one_resolved_timezone_load(self):
        claim = _claim()
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(
            daemon,
            "get_items_for_order_date",
            return_value=[],
        ) as load_items, patch.object(
            daemon,
            "complete_claim",
            return_value=True,
        ) as complete, patch.object(daemon, "get_fcm_tokens") as get_tokens:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        load_items.assert_called_once_with(
            unittest.mock.ANY,
            "989262",
            "2026-08-18",
            resolved_timezone="America/Denver",
        )
        self.assertEqual(complete.call_args.kwargs["reason"], "no_items")
        get_tokens.assert_not_called()

    def test_safe_retry_reuses_stored_payload_without_loading_pcf(self):
        claim = _claim(payload=_payload(), saps=["24521"])
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(daemon, "get_items_for_order_date") as load_items, patch.object(
            daemon,
            "get_fcm_tokens",
            return_value=[],
        ), patch.object(
            daemon,
            "complete_claim",
            return_value=True,
        ) as complete:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        load_items.assert_not_called()
        self.assertEqual(complete.call_args.kwargs["reason"], "no_valid_token")

    def test_dispatch_persists_ticket_before_terminal_send(self):
        claim = _claim(payload=_payload(), saps=["24521"])
        events = []

        def send(*_args, **kwargs):
            events.append("http")
            self.assertTrue(kwargs["accepted_ticket_callback"](["ticket-1"]))
            return daemon.PushDeliveryResult(
                valid_token_count=1,
                pending_count=1,
                accepted_ticket_ids=["ticket-1"],
            )

        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(
            daemon,
            "get_fcm_tokens",
            return_value=["ExponentPushToken[live]"],
        ), patch.object(
            daemon,
            "begin_dispatch",
            side_effect=lambda *_args, **_kwargs: events.append("dispatching") or True,
        ), patch.object(
            daemon,
            "record_accepted_tickets",
            side_effect=lambda *_args, **_kwargs: events.append("ticket") or True,
        ), patch.object(
            daemon,
            "send_push_notification",
            side_effect=send,
        ), patch.object(
            daemon,
            "complete_claim",
            side_effect=lambda *_args, **_kwargs: events.append("sent") or True,
        ) as complete:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        self.assertEqual(events, ["dispatching", "http", "ticket", "sent"])
        self.assertEqual(complete.call_args.kwargs["status"], "sent")
        self.assertEqual(complete.call_args.kwargs["accepted_ticket_ids"], ["ticket-1"])

    def test_all_device_not_registered_closes_without_retry(self):
        claim = _claim(payload=_payload(), saps=["24521"])
        result = daemon.PushDeliveryResult(
            valid_token_count=1,
            failed_count=1,
            invalid_tokens=["ExponentPushToken[stale]"],
            accepted_ticket_ids=["ticket-stale"],
        )
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(
            daemon,
            "get_fcm_tokens",
            return_value=["ExponentPushToken[stale]"],
        ), patch.object(daemon, "begin_dispatch", return_value=True), patch.object(
            daemon,
            "send_push_notification",
            return_value=result,
        ), patch.object(daemon, "remove_invalid_push_tokens") as remove_tokens, patch.object(
            daemon,
            "complete_claim",
            return_value=True,
        ) as complete, patch.object(daemon, "mark_zero_ticket_retryable") as retry:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        remove_tokens.assert_called_once()
        self.assertEqual(complete.call_args.kwargs["reason"], "no_valid_token")
        retry.assert_not_called()

    def test_ambiguous_dispatch_closes_unknown_and_never_retries(self):
        claim = _claim(payload=_payload(), saps=["24521"])
        result = daemon.PushDeliveryResult(valid_token_count=1, ambiguous=True)
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(
            daemon,
            "get_fcm_tokens",
            return_value=["ExponentPushToken[live]"],
        ), patch.object(daemon, "begin_dispatch", return_value=True), patch.object(
            daemon,
            "send_push_notification",
            return_value=result,
        ), patch.object(
            daemon,
            "complete_claim",
            return_value=True,
        ) as complete, patch.object(daemon, "mark_zero_ticket_retryable") as retry:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 1)
        self.assertEqual(complete.call_args.kwargs["reason"], "delivery_unknown")
        retry.assert_not_called()

    def test_explicit_zero_ticket_response_is_the_only_dispatch_retry(self):
        claim = _claim(payload=_payload(), saps=["24521"])
        result = daemon.PushDeliveryResult(valid_token_count=1, failed_count=1)
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value=claim.owner_uid,
        ), patch.object(
            daemon,
            "get_fcm_tokens",
            return_value=["ExponentPushToken[live]"],
        ), patch.object(daemon, "begin_dispatch", return_value=True), patch.object(
            daemon,
            "send_push_notification",
            return_value=result,
        ), patch.object(
            daemon,
            "mark_zero_ticket_retryable",
            return_value=True,
        ) as retry, patch.object(daemon, "complete_claim") as complete:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 1)
        retry.assert_called_once_with(claim, error="expo_accepted_zero_tickets")
        complete.assert_not_called()

    def test_owner_change_reconciles_before_closing_stale_claim(self):
        claim = _claim()
        events = []
        with patch.object(daemon, "_route_allowed", return_value=True), patch.object(
            daemon,
            "_current_claim_owner",
            return_value="new-owner",
        ), patch.object(
            daemon,
            "_reconcile_changed_claim_owner",
            side_effect=lambda *_args, **_kwargs: events.append("reconcile"),
        ), patch.object(
            daemon,
            "complete_claim",
            side_effect=lambda *_args, **_kwargs: events.append("close") or True,
        ), patch.object(daemon, "get_items_for_order_date") as load_items:
            failures = daemon._process_claim(MagicMock(), claim)

        self.assertEqual(failures, 0)
        self.assertEqual(events, ["reconcile", "close"])
        load_items.assert_not_called()

    def test_postgres_loop_claims_before_processing(self):
        claim = _claim()
        with patch.object(
            daemon,
            "load_preference_run_counts",
            return_value={"enabled": 7, "due": 1},
        ), patch.object(
            daemon,
            "claim_next_due",
            side_effect=[claim, None],
        ) as claim_due, patch.object(
            daemon,
            "_process_claim",
            return_value=0,
        ) as process:
            failures = daemon.check_and_notify_postgres(
                MagicMock(),
                late_tolerance_minutes=20,
            )

        self.assertEqual(failures, 0)
        self.assertEqual(claim_due.call_count, 2)
        process.assert_called_once()
        self.assertEqual(process.call_args.args[1], claim)
        self.assertIsInstance(process.call_args.args[2], daemon.NotificationRunCounters)
        self.assertEqual(claim_due.call_args_list[0].kwargs["lease_seconds"], 300)


if __name__ == "__main__":
    unittest.main()

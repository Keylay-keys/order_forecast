from __future__ import annotations

import json
import socket
import unittest
from pathlib import Path
from unittest.mock import patch

from order_forecast.api import notification_sink as sink


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = ROOT / "order_forecast/contracts/pcf/notification-sink-v1.json"
FIXTURE_PATH = ROOT / "order_forecast/contracts/pcf/notification-sink-provider-fixtures-v1.json"


class NotificationSinkContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.provider = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def redacted_envelope(self, *, token_count=2):
        key = b"evidence-key-not-persisted-32bytes!"
        fingerprints = tuple(
            sink.fingerprint_push_token(token=f"redacted-runtime-token-{index}", hmac_key=key)
            for index in range(token_count)
        )
        return sink.RedactedNotificationEnvelope(
            semantic_key='{"alertDays":1,"expiryDate":"2026-08-20","userId":"user-1"}',
            source_revision="fsut1|2026-08-20T12:00:00.000000000Z|0",
            due_at_utc="2026-08-20T12:01:00Z",
            attempt_id="attempt-1",
            recipient_aggregate_hash="c" * 64,
            title="Item expiration reminder",
            body="An item expires soon.",
            data={"kind": "expiring"},
            token_fingerprints=fingerprints,
            token_count=token_count,
            payload_hash="a" * 64,
        )

    def test_effect_disabled_sink_has_no_network_or_token_surface(self):
        disabled = sink.EffectDisabledNotificationSink()
        scripted = sink.classify_expo_tickets(
            expected_count=2,
            response_payload={
                "data": [
                    {"status": "ok", "id": "ticket-1"},
                    {"status": "ok", "id": "ticket-2"},
                ]
            },
        )
        with patch.object(socket, "create_connection") as create_connection:
            result = disabled.record(envelope=self.redacted_envelope(), scripted_result=scripted)
        create_connection.assert_not_called()
        self.assertEqual(result.disposition, "effect_disabled")
        self.assertEqual(result.outbound_calls, 0)
        self.assertEqual(result.scripted_disposition, "accepted")
        self.assertFalse(hasattr(result, "tokens"))
        self.assertFalse(hasattr(result, "message"))

    def test_effect_disabled_sink_count_is_not_an_expo_request_batch(self):
        result = sink.EffectDisabledNotificationSink().record(
            envelope=self.redacted_envelope(token_count=101),
            scripted_result=sink.TicketClassification(
                disposition="ambiguous",
                accepted_ticket_ids=(),
                accepted_indexes=(),
                rejected_indexes=(),
                invalid_token_indexes=(),
                provider_error_codes=(),
            ),
        )
        self.assertEqual(result.recipient_count, 101)
        with self.assertRaises(sink.NotificationSinkContractError):
            sink.canonicalize_redacted_envelope(
                sink.RedactedNotificationEnvelope(
                    **{
                        **self.redacted_envelope().__dict__,
                        "token_count": -1,
                    }
                )
            )

    def test_token_fingerprints_are_keyed_stable_and_do_not_reveal_tokens(self):
        token = "ExpoPushToken[redacted-runtime-token]"
        first = sink.fingerprint_push_token(token=token, hmac_key=b"a" * 32)
        self.assertEqual(first, sink.fingerprint_push_token(token=token, hmac_key=b"a" * 32))
        self.assertNotEqual(first, sink.fingerprint_push_token(token=token, hmac_key=b"b" * 32))
        self.assertNotIn("redacted-runtime-token", first)

    def test_envelope_is_canonical_and_rejects_raw_token_shape(self):
        envelope = self.redacted_envelope()
        left = sink.canonicalize_redacted_envelope(envelope)
        right = sink.canonicalize_redacted_envelope(
            sink.RedactedNotificationEnvelope(
                **{**envelope.__dict__, "data": {"kind": "expiring"}}
            )
        )
        self.assertEqual(left, right)
        with self.assertRaises(sink.NotificationSinkContractError):
            sink.canonicalize_redacted_envelope(
                sink.RedactedNotificationEnvelope(
                    **{
                        **envelope.__dict__,
                        "token_fingerprints": ("ExpoPushToken[raw]", "d" * 64),
                    }
                )
            )

    def test_sink_module_imports_no_network_client_or_expo_url(self):
        source = (ROOT / "order_forecast/api/notification_sink.py").read_text(encoding="utf-8")
        for forbidden in (
            "import requests",
            "import httpx",
            "import urllib",
            "import socket",
            "exp.host",
            "ExponentPushToken[",
            "ExpoPushToken[",
        ):
            self.assertNotIn(forbidden, source)

    def test_official_provider_ticket_fixtures_match_frozen_taxonomy(self):
        for name in ("ticketSuccess", "ticketPartialDeviceNotRegistered"):
            fixture = self.provider["fixtures"][name]
            result = sink.classify_expo_tickets(
                expected_count=fixture["expectedCount"],
                response_payload=fixture["response"],
            )
            self.assertEqual(result.disposition, fixture["expectedDisposition"])
        partial = sink.classify_expo_tickets(
            expected_count=2,
            response_payload=self.provider["fixtures"]["ticketPartialDeviceNotRegistered"]["response"],
        )
        self.assertEqual(partial.accepted_indexes, (1,))
        self.assertEqual(partial.rejected_indexes, (0,))
        self.assertEqual(partial.invalid_token_indexes, (0,))

    def test_complete_all_error_tickets_prove_zero_acceptance(self):
        result = sink.classify_expo_tickets(
            expected_count=2,
            response_payload={
                "data": [
                    {
                        "status": "error",
                        "message": "redacted",
                        "details": {"error": "DeviceNotRegistered"},
                    },
                    {
                        "status": "error",
                        "message": "redacted",
                        "details": {"error": "MessageTooBig"},
                    },
                ]
            },
        )
        self.assertEqual(result.disposition, "proven_zero_accepted")
        self.assertEqual(result.accepted_ticket_ids, ())
        self.assertEqual(result.invalid_token_indexes, (0,))

    def test_complete_top_level_provider_error_proves_request_rejection(self):
        result = sink.classify_expo_tickets(
            expected_count=1,
            response_payload={
                "errors": [
                    {"code": "PUSH_TOO_MANY_EXPERIENCE_IDS", "message": "redacted"}
                ]
            },
        )
        self.assertEqual(result.disposition, "request_rejected_zero_accepted")

    def test_injected_timeout_malformed_and_count_mismatch_are_ambiguous(self):
        fixtures = (
            None,
            "not-json-object",
            {},
            {"data": [{"status": "ok", "id": "one"}]},
            {"data": [{"status": "ok"}, {"status": "error"}]},
        )
        for response in fixtures:
            with self.subTest(response=response):
                result = sink.classify_expo_tickets(
                    expected_count=2,
                    response_payload=response,
                )
                self.assertEqual(result.disposition, "ambiguous")

    def test_partial_batch_acceptance_then_timeout_is_terminal_ambiguous(self):
        first = sink.classify_expo_tickets(
            expected_count=2,
            response_payload={
                "data": [
                    {"status": "ok", "id": "ticket-1"},
                    {
                        "status": "error",
                        "message": "redacted",
                        "details": {"error": "DeviceNotRegistered"},
                    },
                ]
            },
        )
        combined = sink.combine_expo_ticket_batches(
            batches=[first],
            batch_sizes=[2],
            transport_failure_after_batches=True,
        )
        self.assertEqual(combined.disposition, "ambiguous")
        self.assertEqual(combined.accepted_ticket_ids, ("ticket-1",))
        self.assertEqual(combined.accepted_indexes, (0,))
        self.assertEqual(combined.invalid_token_indexes, (1,))

    def test_complete_batches_rebase_local_indexes(self):
        first = sink.classify_expo_tickets(
            expected_count=1,
            response_payload={"data": [{"status": "ok", "id": "ticket-1"}]},
        )
        second = sink.classify_expo_tickets(
            expected_count=2,
            response_payload={
                "data": [
                    {
                        "status": "error",
                        "message": "redacted",
                        "details": {"error": "DeviceNotRegistered"},
                    },
                    {"status": "ok", "id": "ticket-2"},
                ]
            },
        )
        combined = sink.combine_expo_ticket_batches(
            batches=[first, second], batch_sizes=[1, 2]
        )
        self.assertEqual(combined.disposition, "accepted")
        self.assertEqual(combined.accepted_indexes, (0, 2))
        self.assertEqual(combined.rejected_indexes, (1,))
        self.assertEqual(combined.invalid_token_indexes, (1,))

    def test_official_receipt_fixture_uses_id_mapping_and_omission_is_pending(self):
        fixture = self.provider["fixtures"]["receiptSuccessWithPendingOmission"]
        result = sink.classify_expo_receipts(
            accepted_ticket_ids=fixture["acceptedTicketIds"],
            response_payload=fixture["response"],
        )
        self.assertEqual(list(result.delivered_ticket_ids), fixture["expectedDelivered"])
        self.assertEqual(list(result.pending_ticket_ids), fixture["expectedPending"])
        self.assertFalse(result.malformed)

    def test_device_not_registered_receipt_marks_only_its_ticket_invalid(self):
        fixture = self.provider["fixtures"]["receiptDeviceNotRegistered"]
        result = sink.classify_expo_receipts(
            accepted_ticket_ids=fixture["acceptedTicketIds"],
            response_payload=fixture["response"],
        )
        self.assertEqual(list(result.invalid_ticket_ids), fixture["expectedInvalid"])
        self.assertEqual(list(result.failed_ticket_ids), fixture["expectedInvalid"])

    def test_malformed_receipt_never_changes_accepted_delivery_to_retryable(self):
        result = sink.classify_expo_receipts(
            accepted_ticket_ids=["ticket-1"],
            response_payload={"data": {"ticket-1": {"status": "unknown"}}},
        )
        self.assertTrue(result.malformed)
        self.assertEqual(result.delivered_ticket_ids, ())
        self.assertEqual(result.failed_ticket_ids, ())

    def test_provider_and_sdk_provenance_are_pinned_without_user_data(self):
        provenance = self.provider["source"]
        self.assertEqual(provenance["kind"], "official_provider_contract")
        self.assertEqual(provenance["observedAt"], "2026-08-20")
        self.assertEqual(provenance["checkedInNodeSdk"], "expo-server-sdk@4.0.0")
        package_lock = json.loads((ROOT / "functions/package-lock.json").read_text())
        self.assertEqual(
            package_lock["packages"]["node_modules/expo-server-sdk"]["version"],
            "4.0.0",
        )
        fixture_source = FIXTURE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("ExponentPushToken[", fixture_source)
        self.assertNotIn("ExpoPushToken[", fixture_source)
        self.assertNotIn("routeNumber", fixture_source)

    def test_contract_preserves_partial_acceptance_and_no_automatic_retry(self):
        boundary = self.contract["stateMachineBoundary"]
        self.assertTrue(boundary["acceptedTicketMeansTerminalSend"])
        self.assertTrue(boundary["partialAcceptanceMeansTerminalSend"])
        self.assertFalse(boundary["transportAmbiguityAutomaticallyRetries"])
        self.assertFalse(boundary["receiptFailureAutomaticallyRetries"])
        self.assertFalse(self.contract["p1RuntimeExpoTransportExists"])
        self.assertFalse(self.contract["rollout"]["productionEffectEnabled"])

    def test_contract_and_module_contain_no_stripe_or_secret_material(self):
        source = (
            CONTRACT_PATH.read_text(encoding="utf-8")
            + FIXTURE_PATH.read_text(encoding="utf-8")
            + (ROOT / "order_forecast/api/notification_sink.py").read_text(encoding="utf-8")
        )
        for forbidden in (
            "STRIPE_SECRET_KEY",
            "STRIPE_WEBHOOK_SECRET",
            "BEGIN PRIVATE KEY",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()

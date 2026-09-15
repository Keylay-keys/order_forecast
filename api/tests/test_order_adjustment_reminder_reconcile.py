from __future__ import annotations

import copy
import asyncio
import unittest

from pydantic import ValidationError

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import AdjustmentReminderReconcileRequest
from order_forecast.api.routers.orders import (
    _reconcile_adjustment_reminder_document,
    reconcile_order_adjustment_reminder,
)


STAMP = {
    "version": 2,
    "date": "2026-08-25",
    "timezone": "America/Denver",
    "scheduleKey": "tuesday",
    "orderDay": 2,
    "loadOffsetDays": 2,
    "deliveryOffsetDays": 3,
    "provenance": "cycle",
}


class Snapshot:
    def __init__(self, data=None, exists=True):
        self._data = data
        self.exists = exists

    def to_dict(self):
        return copy.deepcopy(self._data)


class Ref:
    def __init__(self, name, transaction, data=None, exists=True):
        self.name = name
        self.transaction = transaction
        self.snapshot = Snapshot(data, exists)

    def get(self, transaction=None):
        self.transaction.events.append(("read", self.name))
        return self.snapshot


class Transaction:
    def __init__(self):
        self.events = []
        self.updates = []

    def update(self, ref, fields):
        self.events.append(("write", ref.name))
        self.updates.append((ref, fields))


def source():
    return {
        "id": "order-v2",
        "routeNumber": "988200",
        "scheduledOrder": copy.deepcopy(STAMP),
        "submittedAt": "2099-01-01T12:00:00Z",
        "adjustmentAnchor": {
            "version": 1,
            "finalizedAtMs": 1787921220000,
            "finalizedDate": "2026-08-28",
            "timezone": "America/Denver",
        },
    }


def adjustment(**overrides):
    return {
        "id": "adjustment-v2",
        "routeNumber": "988200",
        "userId": "draft-author",
        "sourceOrderId": "order-v2",
        "status": "draft",
        "reminderStatus": "none",
        "timezone": "Pacific/Honolulu",
        "sourceOrderSnapshot": {"scheduledOrder": {**STAMP, "date": "2099-01-01"}},
        "lines": [{"sap": "100", "direction": "add", "caseQuantity": 1}],
        **overrides,
    }


def requester(updated_at=None, **settings):
    data = {
        "userSettings": {
            "notifications": {
                "orderAdjustmentSettings": {
                    "reminderEnabled": True,
                    "cutoffTimeLocal": "11:00",
                    "reminderLeadMinutes": 15,
                    **settings,
                },
            },
            "timestamps": {},
        }
    }
    if updated_at is not None:
        data["userSettings"]["timestamps"]["updatedAt"] = updated_at
    return data


class ReconcileTests(unittest.TestCase):
    def call(
        self,
        *,
        adjustment_data=None,
        source_data=None,
        settings_data=None,
        payload=None,
        adjustment_exists=True,
        source_exists=True,
        settings_exists=True,
        now_ms=1788022200000,
    ):
        transaction = Transaction()
        adjustment_ref = Ref(
            "adjustment",
            transaction,
            adjustment() if adjustment_data is None else adjustment_data,
            adjustment_exists,
        )
        source_ref = Ref(
            "source",
            transaction,
            source() if source_data is None else source_data,
            source_exists,
        )
        settings_ref = Ref(
            "requester-settings",
            transaction,
            requester() if settings_data is None else settings_data,
            settings_exists,
        )
        result = _reconcile_adjustment_reminder_document.to_wrap(
            transaction,
            adjustment_ref=adjustment_ref,
            source_ref=source_ref,
            settings_ref=settings_ref,
            route_number="988200",
            order_id="order-v2",
            adjustment_id="adjustment-v2",
            authenticated_uid="viewer-not-draft-author",
            payload=payload or AdjustmentReminderReconcileRequest(),
            now_ms=now_ms,
        )
        return transaction, result

    def test_reads_all_authorities_before_write_and_uses_fresh_source_zone(self):
        transaction, result = self.call()
        self.assertEqual(transaction.events[:3], [
            ("read", "adjustment"), ("read", "source"), ("read", "requester-settings"),
        ])
        self.assertEqual(transaction.events[3], ("write", "adjustment"))
        self.assertTrue(result["changed"])
        self.assertEqual(result["fields"]["timezone"], "America/Denver")
        self.assertEqual(result["fields"]["adjustmentDate"], "2026-08-29")
        self.assertEqual(result["fields"]["reminderAtMs"], 1788021900000)

    def test_canonical_input_is_true_noop(self):
        first_transaction, first = self.call()
        canonical = {**adjustment(), **first["fields"]}
        second_transaction, second = self.call(adjustment_data=canonical)
        self.assertTrue(first_transaction.updates)
        self.assertFalse(second["changed"])
        self.assertEqual(second_transaction.updates, [])

    def test_sending_state_is_preserved(self):
        transaction, result = self.call(adjustment_data=adjustment(
            reminderStatus="sending", reminderWorkerId="worker-1", reminderClaimedAtMs=100,
        ))
        self.assertFalse(result["changed"])
        self.assertEqual(result["fields"]["reminderWorkerId"], "worker-1")
        self.assertEqual(transaction.updates, [])

    def test_sent_state_is_preserved(self):
        transaction, result = self.call(adjustment_data=adjustment(
            reminderStatus="sent", reminderSentAtMs=123, reminderPushStats={"sent": 1},
        ))
        self.assertFalse(result["changed"])
        self.assertEqual(result["fields"]["reminderSentAtMs"], 123)
        self.assertEqual(result["fields"]["reminderPushStats"], {"sent": 1})
        self.assertEqual(transaction.updates, [])

    def test_every_nondraft_terminal_state_is_preserved(self):
        for status in ("sent", "confirmed", "applied", "archived", "canceled"):
            with self.subTest(status=status):
                transaction, result = self.call(adjustment_data=adjustment(
                    status=status,
                    reminderStatus="skipped",
                    reminderSkipReason=f"adjustment_{status}",
                    reminderSkippedAtMs=123,
                ))
                self.assertFalse(result["changed"])
                self.assertEqual(result["fields"]["reminderSkipReason"], f"adjustment_{status}")
                self.assertEqual(result["fields"]["reminderSkippedAtMs"], 123)
                self.assertEqual(transaction.updates, [])

    def test_unknown_nondraft_state_fails_closed_without_write(self):
        transaction, result = self.call(adjustment_data=adjustment(
            status="processing",
            reminderStatus="pending",
            reminderAtMs=1788021900000,
        ))
        self.assertFalse(result["changed"])
        self.assertEqual(result["fields"]["reminderAtMs"], 1788021900000)
        self.assertEqual(transaction.updates, [])

    def test_no_meaningful_lines_cannot_be_armed_by_client_context(self):
        _, result = self.call(adjustment_data=adjustment(
            lines=[
                {"sap": "100", "direction": "add", "caseQuantity": 2},
                {"sap": "100", "direction": "remove", "caseQuantity": 2},
            ],
        ))
        self.assertFalse(result["fields"]["reminderEnabled"])
        self.assertEqual(result["fields"]["reminderStatus"], "none")
        self.assertIsNone(result["fields"]["reminderAtMs"])

    def test_at_cutoff_is_closed_and_never_late_armed(self):
        _, result = self.call(now_ms=1788022800000)
        self.assertEqual(result["fields"]["reminderStatus"], "skipped")
        self.assertEqual(result["fields"]["reminderSkipReason"], "past_cutoff")
        self.assertIsNone(result["fields"]["reminderScheduledForMs"])

    def test_repeated_past_cutoff_reconcile_preserves_first_skip_time_and_is_noop(self):
        first_transaction, first = self.call(now_ms=1788022800000)
        canonical = {**adjustment(), **first["fields"]}
        second_transaction, second = self.call(
            adjustment_data=canonical,
            now_ms=1788022860000,
        )

        self.assertTrue(first_transaction.updates)
        self.assertFalse(second["changed"])
        self.assertEqual(second["fields"]["reminderSkippedAtMs"], 1788022800000)
        self.assertEqual(second_transaction.updates, [])

    def test_persisted_requester_settings_override_client_and_draft_author(self):
        _, result = self.call(settings_data=requester(
            cutoffTimeLocal="10:00", reminderLeadMinutes=30,
        ))
        self.assertEqual(result["fields"]["cutoffTimeLocal"], "10:00")
        self.assertEqual(result["fields"]["reminderLeadMinutes"], 30)

    def test_settings_revision_conflict_writes_nothing(self):
        with self.assertRaises(StructuredApiError) as raised:
            self.call(
                settings_data=requester({"seconds": 10, "nanoseconds": 0}),
                payload=AdjustmentReminderReconcileRequest(expectedSettingsUpdatedAtMs=999),
            )
        self.assertEqual(raised.exception.code, "SETTINGS_REVISION_CHANGED")

    def test_source_identity_change_is_not_treated_as_standalone(self):
        with self.assertRaises(StructuredApiError) as raised:
            self.call(adjustment_data=adjustment(sourceOrderId="missing-order"))
        self.assertEqual(raised.exception.code, "ADJUSTMENT_SOURCE_CHANGED")

    def test_missing_adjustment_source_and_requester_settings_fail_closed(self):
        cases = [
            ({"adjustment_exists": False}, "ADJUSTMENT_NOT_FOUND"),
            ({"source_exists": False}, "MISSING_SOURCE_ORDER"),
            ({"settings_exists": False}, "MISSING_ADJUSTMENT_SETTINGS"),
        ]
        for kwargs, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                with self.assertRaises(StructuredApiError) as raised:
                    self.call(**kwargs)
                self.assertEqual(raised.exception.code, expected_code)

    def test_route_mismatch_in_adjustment_or_source_fails_closed(self):
        with self.assertRaises(StructuredApiError) as raised:
            self.call(adjustment_data=adjustment(routeNumber="989262"))
        self.assertEqual(raised.exception.code, "ADJUSTMENT_ROUTE_CHANGED")

        with self.assertRaises(StructuredApiError) as raised:
            self.call(source_data={**source(), "routeNumber": "989262"})
        self.assertEqual(raised.exception.code, "MISSING_SOURCE_ORDER")

    def test_padded_source_identity_fails_closed(self):
        with self.assertRaises(StructuredApiError) as raised:
            self.call(adjustment_data=adjustment(sourceOrderId=" order-v2 "))
        self.assertEqual(raised.exception.code, "ADJUSTMENT_SOURCE_CHANGED")

    def test_malformed_nested_settings_fail_closed(self):
        with self.assertRaises(StructuredApiError) as raised:
            self.call(settings_data={"userSettings": {"notifications": "bad"}})
        self.assertEqual(raised.exception.code, "MISSING_ADJUSTMENT_SETTINGS")

    def test_request_model_forbids_client_schedule_and_line_hints(self):
        for field, value in [
            ("cutoffTimeLocal", "23:59"),
            ("reminderLeadMinutes", 1),
            ("hasMeaningfulLines", True),
            ("timezone", "Pacific/Honolulu"),
        ]:
            with self.subTest(field=field):
                with self.assertRaises(ValidationError):
                    AdjustmentReminderReconcileRequest(**{field: value})

    def test_blank_auth_is_rejected_before_database_access(self):
        class BombDatabase:
            def __getattr__(self, name):
                raise AssertionError(f"database accessed before auth validation: {name}")

        with self.assertRaises(StructuredApiError) as raised:
            asyncio.run(reconcile_order_adjustment_reminder.__wrapped__(
                request=None,
                payload=AdjustmentReminderReconcileRequest(),
                order_id="order-v2",
                adjustment_id="adjustment-v2",
                route="988200",
                decoded_token={"uid": "   "},
                db=BombDatabase(),
            ))
        self.assertEqual(raised.exception.code, "AUTH_REQUIRED")


if __name__ == "__main__":
    unittest.main()

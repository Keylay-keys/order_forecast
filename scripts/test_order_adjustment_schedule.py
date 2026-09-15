from __future__ import annotations

import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

try:
    from .order_adjustment_schedule import (
        WindowResolutionError,
        build_canonical_reminder_fields,
        canonical_source_window,
        meaningful_lines,
    )
except ImportError:
    from order_adjustment_schedule import (
        WindowResolutionError,
        build_canonical_reminder_fields,
        canonical_source_window,
        meaningful_lines,
    )


def source(
    scheduled_date="2026-08-25",
    *,
    submitted_at="2026-08-28T12:47:00Z",
    timezone_name="America/Denver",
    **stamp_overrides,
):
    weekday = date_weekday(scheduled_date)
    keys = ["", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    stamp = {
        "version": 2,
        "date": scheduled_date,
        "timezone": timezone_name,
        "scheduleKey": keys[weekday],
        "orderDay": weekday,
        "loadOffsetDays": 2,
        "deliveryOffsetDays": 3,
        "provenance": "cycle",
        **stamp_overrides,
    }
    data = {"scheduledOrder": stamp}
    if submitted_at is not None:
        data["submittedAt"] = submitted_at
    return data


def date_weekday(value):
    return datetime.fromisoformat(value).isoweekday()


def anchor_source(finalized_at, timezone_name="America/Denver", **anchor_overrides):
    finalized_at_ms = int(finalized_at.timestamp() * 1000)
    finalized_date = finalized_at.astimezone(ZoneInfo(timezone_name)).date().isoformat()
    return {
        "scheduledOrder": source()["scheduledOrder"],
        "submittedAt": "2099-01-01T12:00:00Z",
        "adjustmentAnchor": {
            "version": 1,
            "finalizedAtMs": finalized_at_ms,
            "finalizedDate": finalized_date,
            "timezone": timezone_name,
            **anchor_overrides,
        },
    }


class OrderAdjustmentScheduleTests(unittest.TestCase):
    def test_legacy_stamped_order_uses_server_submission_not_cycle_date(self):
        window = canonical_source_window(
            source(), cutoff_time_local="11:00", reminder_lead_minutes=15
        )
        self.assertEqual(window.adjustment_date, "2026-08-29")
        self.assertEqual(window.cutoff_at_ms, 1788022800000)
        self.assertEqual(window.reminder_at_ms, 1788021900000)

    def test_anchor_is_authoritative_over_conflicting_legacy_submission(self):
        finalized = datetime(2026, 9, 4, 2, 23, tzinfo=timezone.utc)
        window = canonical_source_window(
            anchor_source(finalized),
            cutoff_time_local="11:00",
            reminder_lead_minutes=15,
        )
        self.assertEqual(window.adjustment_date, "2026-09-04")
        self.assertEqual(window.timezone, "America/Denver")

    def test_present_invalid_anchor_fails_closed_without_legacy_fallback(self):
        with self.assertRaises(WindowResolutionError) as raised:
            canonical_source_window(
                anchor_source(
                    datetime(2026, 9, 4, 2, 23, tzinfo=timezone.utc),
                    finalizedDate="2026-09-04",
                ),
                cutoff_time_local="11:00",
                reminder_lead_minutes=15,
            )
        self.assertEqual(raised.exception.code, "invalid_adjustment_anchor")

    def test_missing_submission_never_falls_back_to_created_or_order_date(self):
        data = source(submitted_at=None)
        data.update({"createdAt": "2026-08-28T12:47:00Z", "orderDate": "2026-08-28"})
        with self.assertRaises(WindowResolutionError) as raised:
            canonical_source_window(data, cutoff_time_local="11:00", reminder_lead_minutes=15)
        self.assertEqual(raised.exception.code, "window_unresolved")

    def test_dst_contract_epochs_use_the_adjustment_day_offset(self):
        spring = canonical_source_window(
            source("2027-03-13", submitted_at="2027-03-13T19:00:00Z"),
            cutoff_time_local="11:00",
            reminder_lead_minutes=15,
        )
        overlap = canonical_source_window(
            source("2027-11-06", submitted_at="2027-11-06T18:00:00Z"),
            cutoff_time_local="01:30",
            reminder_lead_minutes=15,
        )
        gap = canonical_source_window(
            source("2027-03-13", submitted_at="2027-03-13T19:00:00Z"),
            cutoff_time_local="02:30",
            reminder_lead_minutes=15,
        )
        self.assertEqual(spring.cutoff_at_ms, 1805043600000)
        self.assertEqual(overlap.cutoff_at_ms, 1825572600000)
        self.assertEqual(gap.cutoff_at_ms, 1805016600000)

    def test_legacy_timezone_stamp_rejects_extra_keys_and_weekday_mismatch(self):
        with self.assertRaises(WindowResolutionError):
            canonical_source_window(
                source(extra="bad"), cutoff_time_local="11:00", reminder_lead_minutes=15
            )
        with self.assertRaises(WindowResolutionError):
            canonical_source_window(
                source(orderDay=3), cutoff_time_local="11:00", reminder_lead_minutes=15
            )

    def test_late_before_cutoff_stays_pending_with_original_due_time(self):
        fields = build_canonical_reminder_fields(
            source(),
            settings={"reminderEnabled": True, "cutoffTimeLocal": "11:00", "reminderLeadMinutes": 15},
            has_meaningful_lines=True,
            status="draft",
            now_ms=1788022200000,
        )
        self.assertEqual(fields["reminderStatus"], "pending")
        self.assertEqual(fields["reminderAtMs"], 1788021900000)
        self.assertEqual(fields["reminderScheduledForMs"], 1788021900000)

    def test_at_cutoff_is_closed(self):
        fields = build_canonical_reminder_fields(
            source(),
            settings={"reminderEnabled": True, "cutoffTimeLocal": "11:00", "reminderLeadMinutes": 15},
            has_meaningful_lines=True,
            status="draft",
            now_ms=1788022800000,
        )
        self.assertEqual(fields["reminderStatus"], "skipped")
        self.assertEqual(fields["reminderSkipReason"], "past_cutoff")

    def test_meaningful_lines_nets_opposite_changes(self):
        self.assertFalse(meaningful_lines({"lines": [
            {"sap": "100", "direction": "add", "caseQuantity": 2},
            {"sap": "100", "direction": "remove", "caseQuantity": 2},
        ]}))
        self.assertTrue(meaningful_lines({"fullOrderWorkingCopy": {"generatedLines": [
            {"sap": "100", "direction": "add", "caseQuantity": 1},
        ]}}))


if __name__ == "__main__":
    unittest.main()

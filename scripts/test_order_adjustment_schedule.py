from __future__ import annotations

import unittest

try:
    from .order_adjustment_schedule import (
        WindowResolutionError,
        build_canonical_reminder_fields,
        canonical_source_v2_window,
        meaningful_lines,
    )
except ImportError:
    from order_adjustment_schedule import (
        WindowResolutionError,
        build_canonical_reminder_fields,
        canonical_source_v2_window,
        meaningful_lines,
    )


def source(date="2026-08-25", timezone="America/Denver", **overrides):
    stamp = {
        "version": 2,
        "date": date,
        "timezone": timezone,
        "scheduleKey": "tuesday" if date == "2026-08-25" else "saturday",
        "orderDay": 2 if date == "2026-08-25" else 6,
        "loadOffsetDays": 2,
        "deliveryOffsetDays": 3,
        "provenance": "cycle",
        **overrides,
    }
    return {"scheduledOrder": stamp, "submittedAt": "2099-01-01T12:00:00Z"}


class OrderAdjustmentScheduleTests(unittest.TestCase):
    def test_989262_window_uses_stamp_and_ignores_submitted_at(self):
        window = canonical_source_v2_window(
            source(), cutoff_time_local="11:00", reminder_lead_minutes=15
        )
        self.assertEqual(window.adjustment_date, "2026-08-26")
        self.assertEqual(window.cutoff_at_ms, 1787763600000)
        self.assertEqual(window.reminder_at_ms, 1787762700000)

    def test_dst_contract_epochs(self):
        spring = canonical_source_v2_window(
            source("2027-03-13"), cutoff_time_local="11:00", reminder_lead_minutes=15
        )
        overlap = canonical_source_v2_window(
            source("2027-11-06"), cutoff_time_local="01:30", reminder_lead_minutes=15
        )
        gap = canonical_source_v2_window(
            source("2027-03-13"), cutoff_time_local="02:30", reminder_lead_minutes=15
        )
        self.assertEqual(spring.cutoff_at_ms, 1805043600000)
        self.assertEqual(overlap.cutoff_at_ms, 1825572600000)
        self.assertEqual(gap.cutoff_at_ms, 1805016600000)

    def test_strict_stamp_rejects_extra_keys_and_weekday_mismatch(self):
        with self.assertRaises(WindowResolutionError):
            canonical_source_v2_window(
                source(extra="bad"), cutoff_time_local="11:00", reminder_lead_minutes=15
            )
        with self.assertRaises(WindowResolutionError):
            canonical_source_v2_window(
                source(orderDay=3), cutoff_time_local="11:00", reminder_lead_minutes=15
            )

    def test_late_before_cutoff_stays_pending_with_original_due_time(self):
        fields = build_canonical_reminder_fields(
            source(),
            settings={"reminderEnabled": True, "cutoffTimeLocal": "11:00", "reminderLeadMinutes": 15},
            has_meaningful_lines=True,
            status="draft",
            now_ms=1787763000000,
        )
        self.assertEqual(fields["reminderStatus"], "pending")
        self.assertEqual(fields["reminderAtMs"], 1787762700000)
        self.assertEqual(fields["reminderScheduledForMs"], 1787762700000)

    def test_at_cutoff_is_closed(self):
        fields = build_canonical_reminder_fields(
            source(),
            settings={"reminderEnabled": True, "cutoffTimeLocal": "11:00", "reminderLeadMinutes": 15},
            has_meaningful_lines=True,
            status="draft",
            now_ms=1787763600000,
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

import unittest
from datetime import date, datetime, timezone

from order_forecast.scripts.low_qty_schedule import (
    InvalidReminderSetting,
    next_scheduled_instant,
    parse_reminder_minute,
    scheduled_instant_for_local_date,
    validate_timezone,
)


class LowQtyScheduleTests(unittest.TestCase):
    def test_strict_reminder_parser_handles_noon_and_midnight(self):
        self.assertEqual(parse_reminder_minute({"hour": 12, "minute": 0, "period": "AM"}), 0)
        self.assertEqual(parse_reminder_minute({"hour": 12, "minute": 0, "period": "PM"}), 720)
        self.assertEqual(parse_reminder_minute({"hour": 8, "minute": 7, "period": "AM"}), 487)

    def test_strict_reminder_parser_rejects_defaults_and_wrong_types(self):
        invalid_values = (
            {},
            {"hour": 8, "minute": 0},
            {"hour": True, "minute": 0, "period": "AM"},
            {"hour": 0, "minute": 0, "period": "AM"},
            {"hour": 8, "minute": 60, "period": "AM"},
            {"hour": 8, "minute": 0, "period": "am"},
        )
        for value in invalid_values:
            with self.subTest(value=value), self.assertRaises(InvalidReminderSetting):
                parse_reminder_minute(value)

    def test_invalid_timezone_has_no_default_fallback(self):
        with self.assertRaisesRegex(InvalidReminderSetting, "unknown timezone"):
            validate_timezone("Mars/Olympus")

    def test_spring_forward_gap_runs_at_first_valid_local_minute(self):
        scheduled = scheduled_instant_for_local_date(
            date(2026, 3, 8),
            2 * 60 + 30,
            "America/Denver",
        )
        self.assertEqual(scheduled, datetime(2026, 3, 8, 9, 0, tzinfo=timezone.utc))

    def test_fall_back_uses_first_occurrence(self):
        scheduled = scheduled_instant_for_local_date(
            date(2026, 11, 1),
            1 * 60 + 30,
            "America/Denver",
        )
        self.assertEqual(scheduled, datetime(2026, 11, 1, 7, 30, tzinfo=timezone.utc))

    def test_fall_back_does_not_create_a_second_same_day_slot(self):
        local_date, scheduled = next_scheduled_instant(
            1 * 60 + 30,
            "America/Denver",
            after_utc=datetime(2026, 11, 1, 7, 45, tzinfo=timezone.utc),
        )
        self.assertEqual(local_date, date(2026, 11, 2))
        self.assertEqual(scheduled, datetime(2026, 11, 2, 8, 30, tzinfo=timezone.utc))

    def test_next_slot_is_strictly_future(self):
        local_date, scheduled = next_scheduled_instant(
            8 * 60,
            "America/Denver",
            after_utc=datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(local_date, date(2026, 8, 19))
        self.assertEqual(scheduled, datetime(2026, 8, 19, 14, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()

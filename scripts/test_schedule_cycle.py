import unittest

from schedule_cycle import (
    add_days,
    get_cycle_dates,
    get_schedule_key_for_delivery_date,
    normalize_order_cycle,
    weekday_after,
)


class ScheduleCycleTest(unittest.TestCase):
    def test_normalizes_long_monday_order_cycle(self):
        cycle = normalize_order_cycle(
            {
                "orderDay": 1,
                "loadDay": 5,
                "deliveryDay": 3,
                "loadOffsetDays": 4,
                "deliveryOffsetDays": 9,
            }
        )

        self.assertEqual(
            cycle,
            {
                "orderDay": 1,
                "loadDay": 5,
                "deliveryDay": 3,
                "loadOffsetDays": 4,
                "deliveryOffsetDays": 9,
                "needsScheduleReview": False,
                "scheduleVersion": 2,
            },
        )
        self.assertEqual(weekday_after(cycle["orderDay"], cycle["deliveryOffsetDays"]), 3)

    def test_preserves_same_weekday_delivery_as_next_occurrence(self):
        cycle = normalize_order_cycle({"orderDay": 5, "loadDay": 5, "deliveryDay": 5})

        self.assertEqual(cycle["loadOffsetDays"], 0)
        self.assertEqual(cycle["deliveryOffsetDays"], 7)
        self.assertEqual(cycle["loadDay"], 5)
        self.assertEqual(cycle["deliveryDay"], 5)

    def test_marks_suspicious_migrated_cycles_for_review(self):
        self.assertFalse(
            normalize_order_cycle({"orderDay": 1, "loadDay": 2, "deliveryDay": 3})[
                "needsScheduleReview"
            ]
        )
        self.assertTrue(
            normalize_order_cycle({"orderDay": 1, "loadDay": 6, "deliveryDay": 3})[
                "needsScheduleReview"
            ]
        )

    def test_computes_long_cycle_dates_with_date_only_arithmetic(self):
        dates = get_cycle_dates(
            {
                "orderDay": 1,
                "loadDay": 5,
                "deliveryDay": 3,
                "loadOffsetDays": 4,
                "deliveryOffsetDays": 9,
            },
            "2026-03-01",
        )

        self.assertEqual(dates["orderDateString"], "2026-03-02")
        self.assertEqual(dates["loadDateString"], "2026-03-06")
        self.assertEqual(dates["deliveryDateString"], "2026-03-11")
        self.assertEqual(dates["scheduleKey"], "monday")

    def test_keeps_add_days_stable_across_dst_boundary(self):
        self.assertEqual(add_days("2026-03-07", 2).isoformat(), "2026-03-09")

    def test_reverse_maps_delivery_dates_by_delivery_offset(self):
        resolution = get_schedule_key_for_delivery_date(
            "2026-03-11",
            [
                {
                    "orderDay": 1,
                    "loadDay": 5,
                    "deliveryDay": 3,
                    "loadOffsetDays": 4,
                    "deliveryOffsetDays": 9,
                }
            ],
        )

        self.assertEqual(resolution["scheduleKey"], "monday")
        self.assertEqual(resolution["matchedBy"], "delivery")
        self.assertFalse(resolution["ambiguous"])

    def test_reverse_maps_load_day_by_load_offset(self):
        resolution = get_schedule_key_for_delivery_date(
            "2026-03-06",
            [
                {
                    "orderDay": 1,
                    "loadDay": 5,
                    "deliveryDay": 3,
                    "loadOffsetDays": 4,
                    "deliveryOffsetDays": 9,
                }
            ],
        )

        self.assertEqual(resolution["scheduleKey"], "monday")
        self.assertEqual(resolution["matchedBy"], "load")

    def test_returns_deterministic_fallback_for_shared_delivery_weekdays(self):
        resolution = get_schedule_key_for_delivery_date(
            "2026-03-11",
            [
                {
                    "orderDay": 1,
                    "loadDay": 5,
                    "deliveryDay": 3,
                    "loadOffsetDays": 4,
                    "deliveryOffsetDays": 9,
                },
                {
                    "orderDay": 2,
                    "loadDay": 5,
                    "deliveryDay": 3,
                    "loadOffsetDays": 3,
                    "deliveryOffsetDays": 8,
                },
            ],
        )

        self.assertTrue(resolution["ambiguous"])
        self.assertEqual(resolution["scheduleKey"], "tuesday")
        self.assertEqual(resolution["matchedBy"], "delivery")
        self.assertEqual(
            [match["scheduleKey"] for match in resolution["matches"]],
            ["tuesday", "monday"],
        )

    def test_returns_none_when_no_cycle_matches_delivery_date(self):
        self.assertIsNone(
            get_schedule_key_for_delivery_date(
                "2026-03-08",
                [
                    {
                        "orderDay": 1,
                        "loadDay": 5,
                        "deliveryDay": 3,
                        "loadOffsetDays": 4,
                        "deliveryOffsetDays": 9,
                    }
                ],
            )
        )


if __name__ == "__main__":
    unittest.main()

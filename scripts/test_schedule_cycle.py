import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from schedule_cycle import (
    add_days,
    get_cycle_dates,
    get_schedule_key_for_delivery_date,
    is_valid_offset,
    is_valid_weekday,
    normalize_order_cycle,
    parse_civil_date,
    resolve_order_schedule_for_delivery_date,
    weekday_after,
)


class ScheduleCycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[2]
        cls.shared_contract = json.loads(
            (root / "contracts/order-adjustment-scheduled-order-day-v2.json").read_text()
        )
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

    def test_boolean_values_are_not_valid_integer_schedule_fields(self):
        self.assertFalse(is_valid_weekday(True))
        self.assertFalse(is_valid_offset(False))
        normalized = normalize_order_cycle({
            "orderDay": True,
            "loadOffsetDays": False,
            "deliveryOffsetDays": True,
        })
        self.assertEqual(normalized["orderDay"], 1)
        self.assertEqual(normalized["loadOffsetDays"], 0)
        self.assertEqual(normalized["deliveryOffsetDays"], 7)

    def test_datetime_input_is_normalized_to_a_civil_date(self):
        self.assertEqual(
            parse_civil_date(datetime(2026, 8, 25, 23, 30, tzinfo=timezone.utc)).isoformat(),
            "2026-08-25",
        )

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

    def test_deduplicates_equal_offset_load_delivery_hit(self):
        resolution = get_schedule_key_for_delivery_date(
            "2026-08-31",
            [
                {
                    "orderDay": 2,
                    "loadDay": 1,
                    "deliveryDay": 1,
                    "loadOffsetDays": 6,
                    "deliveryOffsetDays": 6,
                }
            ],
        )
        self.assertFalse(resolution["ambiguous"])
        self.assertEqual(resolution["matchedBy"], "delivery")
        self.assertEqual(len(resolution["matches"]), 1)

    def test_deduplicates_semantically_identical_duplicate_cycle_rows(self):
        cycle = {
            "orderDay": 2,
            "loadDay": 1,
            "deliveryDay": 1,
            "loadOffsetDays": 6,
            "deliveryOffsetDays": 6,
        }
        resolution = get_schedule_key_for_delivery_date(
            "2026-08-31",
            [cycle, dict(cycle)],
        )
        self.assertFalse(resolution["ambiguous"])
        self.assertEqual(resolution["matchedBy"], "delivery")
        self.assertEqual(len(resolution["matches"]), 1)

    def test_distinct_equal_offset_cycles_stay_ambiguous(self):
        resolution = get_schedule_key_for_delivery_date(
            "2026-08-31",
            [
                {"orderDay": 2, "loadOffsetDays": 6, "deliveryOffsetDays": 6},
                {"orderDay": 1, "loadOffsetDays": 7, "deliveryOffsetDays": 7},
            ],
        )
        self.assertTrue(resolution["ambiguous"])
        self.assertEqual(len(resolution["matches"]), 2)

    def test_delivery_only_resolver_rejects_load_only_hit(self):
        resolution = resolve_order_schedule_for_delivery_date(
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
        self.assertEqual(resolution, {"status": "unresolved", "reason": "off_cycle"})

    def test_delivery_only_resolver_preserves_distinct_reasons(self):
        self.assertEqual(
            resolve_order_schedule_for_delivery_date("2026-03-11", []),
            {"status": "unresolved", "reason": "missing_schedule"},
        )
        cycles = [
            {"orderDay": 1, "loadOffsetDays": 4, "deliveryOffsetDays": 9},
            {"orderDay": 2, "loadOffsetDays": 3, "deliveryOffsetDays": 8},
        ]
        self.assertEqual(
            resolve_order_schedule_for_delivery_date("2026-03-11", cycles),
            {"status": "unresolved", "reason": "ambiguous_schedule"},
        )

    def test_shared_989262_fixture(self):
        fixture = next(
            item
            for item in self.shared_contract["cases"]
            if item["name"] == "route-989262-tuesday-six-day"
        )
        resolution = resolve_order_schedule_for_delivery_date(
            fixture["deliveryDate"], fixture["cycles"], fixture["scheduleKey"]
        )
        self.assertEqual(resolution["status"], "resolved")
        self.assertEqual(resolution["orderDate"], fixture["expected"]["orderDate"])
        self.assertEqual(resolution["loadDate"], fixture["expected"]["loadDate"])


if __name__ == "__main__":
    unittest.main()

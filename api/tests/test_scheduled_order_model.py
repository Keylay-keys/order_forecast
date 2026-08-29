from __future__ import annotations

import unittest

from pydantic import ValidationError

from order_forecast.api.models import ScheduledOrder


VALID = {
    "version": 2,
    "date": "2026-08-25",
    "timezone": "America/Denver",
    "scheduleKey": "tuesday",
    "orderDay": 2,
    "loadOffsetDays": 2,
    "deliveryOffsetDays": 3,
    "provenance": "cycle",
}


class ScheduledOrderModelTests(unittest.TestCase):
    def test_exact_valid_stamp_round_trips(self):
        self.assertEqual(ScheduledOrder(**VALID).model_dump(), VALID)

    def test_malformed_or_inconsistent_stamp_fails_closed(self):
        cases = [
            {**VALID, "extra": True},
            {**VALID, "date": "2026-08-26"},
            {**VALID, "timezone": "Mars/Olympus"},
            {**VALID, "scheduleKey": "monday"},
            {**VALID, "deliveryOffsetDays": 1},
            {
                **VALID,
                "date": "2026-08-24",
                "scheduleKey": "monday",
                "orderDay": True,
                "loadOffsetDays": False,
            },
        ]
        for stamp in cases:
            with self.subTest(stamp=stamp):
                with self.assertRaises(ValidationError):
                    ScheduledOrder(**stamp)


if __name__ == "__main__":
    unittest.main()

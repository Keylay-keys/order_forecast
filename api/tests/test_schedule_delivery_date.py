import unittest
from unittest.mock import AsyncMock, patch

from order_forecast.api.routers import schedule


class ScheduleDeliveryDateTests(unittest.IsolatedAsyncioTestCase):
    async def test_matched_cycle_returns_boolean_matched_field(self):
        resolution = {"scheduleKey": "monday", "orderDay": 1}
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule, "get_order_cycles_from_firestore", return_value=[]
        ), patch.object(schedule, "get_schedule_key_for_delivery_date", return_value=resolution):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-10",
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertIsInstance(result, schedule.ScheduleKeyResponse)
        self.assertTrue(result.matched)
        self.assertEqual(result.scheduleKey, "monday")

    async def test_unmatched_date_returns_typed_fallback(self):
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule, "get_order_cycles_from_firestore", return_value=[]
        ), patch.object(schedule, "get_schedule_key_for_delivery_date", return_value=None):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-10",
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertIsInstance(result, schedule.ScheduleKeyResponse)
        self.assertFalse(result.matched)
        self.assertEqual(result.scheduleKey, "monday")


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import date, timedelta
from unittest.mock import patch

from fastapi import HTTPException

from order_forecast.api.models import OrderCreateRequest
from order_forecast.api.routers import orders


def _payload(schedule_key="thursday", *, holiday=False):
    return OrderCreateRequest(
        routeNumber="989262",
        deliveryDate=date.today() + timedelta(days=7),
        scheduleKey=schedule_key,
        isHolidaySchedule=holiday,
    )


class OrderScheduleGuardTests(unittest.TestCase):
    def test_rejects_non_holiday_schedule_key_that_matches_no_cycle(self):
        resolution = {
            "scheduleKey": "monday",
            "orderDay": 1,
            "ambiguous": False,
            "matches": [{"scheduleKey": "monday"}],
        }

        with patch.object(orders, "get_order_cycles_from_firestore", return_value=[{"orderDay": 1}]), patch.object(
            orders, "get_schedule_key_for_delivery_date", return_value=resolution
        ):
            with self.assertRaises(HTTPException) as ctx:
                orders._validate_non_holiday_schedule_key(object(), _payload("thursday"))

        self.assertEqual(ctx.exception.status_code, 400)

    def test_accepts_schedule_key_that_matches_any_ambiguous_cycle(self):
        resolution = {
            "scheduleKey": "monday",
            "orderDay": 1,
            "ambiguous": True,
            "matches": [{"scheduleKey": "monday"}, {"scheduleKey": "thursday"}],
        }

        with patch.object(orders, "get_order_cycles_from_firestore", return_value=[{"orderDay": 1}]), patch.object(
            orders, "get_schedule_key_for_delivery_date", return_value=resolution
        ):
            orders._validate_non_holiday_schedule_key(object(), _payload("thursday"))

    def test_holiday_orders_bypass_schedule_key_validation(self):
        with patch.object(orders, "get_order_cycles_from_firestore") as get_cycles:
            orders._validate_non_holiday_schedule_key(object(), _payload("thursday", holiday=True))

        get_cycles.assert_not_called()


if __name__ == "__main__":
    unittest.main()

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

    def test_derives_load_date_only_from_one_consistent_delivery_match(self):
        payload = _payload("thursday")
        delivery_offset = 3
        load_offset = 2
        order_date = payload.deliveryDate - timedelta(days=delivery_offset)
        match = {
            "scheduleKey": "thursday",
            "matchedBy": "delivery",
            "cycle": {
                "orderDay": order_date.isoweekday(),
                "loadOffsetDays": load_offset,
                "deliveryOffsetDays": delivery_offset,
            },
        }

        self.assertEqual(
            orders._derive_expected_load_date(payload, {"matches": [match]}),
            (order_date + timedelta(days=load_offset)).isoformat(),
        )
        self.assertIsNone(
            orders._derive_expected_load_date(payload, {"matches": [match, dict(match)]})
        )

    def test_order_model_remains_compatible_when_load_date_is_absent(self):
        order = orders.Order(
            id="order-1",
            routeNumber="989262",
            userId="user-1",
            orderDate="2026-07-27",
            expectedDeliveryDate="2026-07-30",
            scheduleKey="monday",
            status="draft",
            stores=[],
            createdAt="2026-07-27T12:00:00Z",
            updatedAt="2026-07-27T12:00:00Z",
        )

        self.assertIsNone(order.expectedLoadDate)


if __name__ == "__main__":
    unittest.main()

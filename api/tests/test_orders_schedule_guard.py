import unittest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import OrderCreateRequest
from order_forecast.api.routers import orders


def _payload(schedule_key="thursday", *, holiday=False, acknowledged=False):
    return OrderCreateRequest(
        routeNumber="989262",
        deliveryDate=date.today() + timedelta(days=7),
        scheduleKey=schedule_key,
        isHolidaySchedule=holiday,
        allowUnresolvedHolidaySchedule=acknowledged,
    )


def _matching_cycle(payload, *, load_offset=2, delivery_offset=3):
    order_day = (payload.deliveryDate - timedelta(days=delivery_offset)).isoweekday()
    return {
        "orderDay": order_day,
        "loadDay": ((order_day - 1 + load_offset) % 7) + 1,
        "deliveryDay": ((order_day - 1 + delivery_offset) % 7) + 1,
        "loadOffsetDays": load_offset,
        "deliveryOffsetDays": delivery_offset,
    }


def _schedule_context(cycles, timezone="America/Denver", status="ready"):
    return {
        "status": status,
        "ownerUid": "owner-1",
        "cycles": cycles,
        "timezone": timezone,
    }


class _Snapshot:
    def __init__(self, data):
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return self._data


class _Document:
    def __init__(self, database, collection, doc_id):
        self.database = database
        self.collection_name = collection
        self.doc_id = doc_id

    def get(self):
        return _Snapshot(self.database.data.get((self.collection_name, self.doc_id)))


class _Collection:
    def __init__(self, database, name):
        self.database = database
        self.name = name

    def document(self, doc_id):
        return _Document(self.database, self.name, doc_id)


class _Database:
    def __init__(self, data):
        self.data = data

    def collection(self, name):
        return _Collection(self, name)


class OrderScheduleGuardTests(unittest.TestCase):
    def test_unique_delivery_result_builds_one_exact_stamp_and_load_date(self):
        payload = _payload("thursday")
        cycle = _matching_cycle(payload)
        payload.scheduleKey = orders.resolve_order_schedule_for_delivery_date(
            payload.deliveryDate, [cycle]
        )["scheduleKey"]
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([cycle]),
        ):
            resolution, stamp = orders._resolve_create_schedule(object(), payload)

        self.assertEqual(stamp["date"], resolution["orderDate"])
        self.assertEqual(stamp["scheduleKey"], payload.scheduleKey)
        self.assertEqual(stamp["timezone"], "America/Denver")
        self.assertEqual(resolution["loadDate"], (
            payload.deliveryDate - timedelta(days=1)
        ).isoformat())

    def test_normal_off_cycle_and_load_only_dates_fail_closed(self):
        payload = _payload("monday")
        cycle = {
            "orderDay": payload.deliveryDate.isoweekday(),
            "loadDay": payload.deliveryDate.isoweekday(),
            "deliveryDay": ((payload.deliveryDate.isoweekday() + 2) % 7) + 1,
            "loadOffsetDays": 0,
            "deliveryOffsetDays": 3,
        }
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([cycle]),
        ):
            with self.assertRaises(StructuredApiError) as raised:
                orders._resolve_create_schedule(object(), payload)
        self.assertEqual(raised.exception.code, "ORDER_SCHEDULE_OFF_CYCLE")

    def test_distinct_delivery_cycles_are_ambiguous_and_rejected(self):
        payload = _payload("monday")
        target_weekday = payload.deliveryDate.isoweekday()
        delivery_offset = (target_weekday - 1) % 7 or 7
        cycle = {
            "orderDay": 1,
            "loadDay": 1,
            "deliveryDay": target_weekday,
            "loadOffsetDays": 0,
            "deliveryOffsetDays": delivery_offset,
        }
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([cycle, dict(cycle)]),
        ):
            with self.assertRaises(StructuredApiError) as raised:
                orders._resolve_create_schedule(object(), payload)
        self.assertEqual(raised.exception.code, "ORDER_SCHEDULE_AMBIGUOUS")

    def test_unique_holiday_stamps_but_unresolved_holiday_requires_acknowledgement(self):
        payload = _payload(holiday=True)
        cycle = _matching_cycle(payload)
        payload.scheduleKey = orders.resolve_order_schedule_for_delivery_date(
            payload.deliveryDate, [cycle]
        )["scheduleKey"]
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([cycle]),
        ):
            resolution, stamp = orders._resolve_create_schedule(object(), payload)
        self.assertIsNotNone(resolution)
        self.assertEqual(stamp["version"], 2)

        unresolved = _payload("monday", holiday=True)
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([]),
        ):
            with self.assertRaises(StructuredApiError) as raised:
                orders._resolve_create_schedule(object(), unresolved)
        self.assertEqual(raised.exception.code, "HOLIDAY_SCHEDULE_ACK_REQUIRED")

        acknowledged = _payload("monday", holiday=True, acknowledged=True)
        with patch.object(
            orders,
            "get_order_schedule_context_from_firestore",
            return_value=_schedule_context([]),
        ):
            self.assertEqual(orders._resolve_create_schedule(object(), acknowledged), (None, None))

    def test_timezone_errors_are_distinct_and_fail_closed(self):
        missing_owner = _Database({("routes", "989262"): {}})
        with self.assertRaises(StructuredApiError) as raised:
            orders._require_route_owner_timezone(missing_owner, "989262")
        self.assertEqual(raised.exception.code, "ROUTE_OWNER_MISSING")

        invalid_zone = _Database({
            ("routes", "989262"): {"ownerUid": "owner-1"},
            ("users", "owner-1"): {"profile": {"timezone": "Not/AZone"}},
        })
        with self.assertRaises(StructuredApiError) as raised:
            orders._require_route_owner_timezone(invalid_zone, "989262")
        self.assertEqual(raised.exception.code, "ROUTE_TIMEZONE_INVALID")

        conflicting_owner = _Database({
            ("routes", "989262"): {"ownerUid": "owner-1", "userId": "owner-2"},
        })
        with self.assertRaises(StructuredApiError) as raised:
            orders._require_route_owner_timezone(conflicting_owner, "989262")
        self.assertEqual(raised.exception.code, "ROUTE_OWNER_CONFLICT")

    def test_order_model_remains_compatible_when_stamp_and_load_date_are_absent(self):
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
        self.assertIsNone(order.scheduledOrder)


class _WriteRef:
    def __init__(self):
        self.written = None

    def set(self, data):
        self.written = data


class OrderCreateResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_response_round_trips_exact_stamp(self):
        payload = _payload()
        cycle = _matching_cycle(payload)
        resolution = orders.resolve_order_schedule_for_delivery_date(payload.deliveryDate, [cycle])
        payload.scheduleKey = resolution["scheduleKey"]
        stamp = orders._build_scheduled_order(resolution, "America/Denver")
        write_ref = _WriteRef()

        with patch.object(
            orders, "require_route_feature_access", new=AsyncMock(return_value={})
        ), patch.object(
            orders, "_resolve_create_schedule", return_value=(resolution, stamp)
        ), patch.object(
            orders, "_get_local_order_date", return_value="2026-08-28"
        ), patch.object(
            orders, "_order_ref", return_value=write_ref
        ), patch.object(
            orders, "stamp_write_allowed_for_headers", return_value=True
        ), patch.object(orders, "_log_order_audit"):
            created = await orders.create_order.__wrapped__(
                request=None,
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=object(),
            )

        self.assertEqual(created.scheduledOrder.model_dump(), stamp)
        self.assertEqual(created.expectedLoadDate, resolution["loadDate"])
        self.assertEqual(write_ref.written["scheduledOrder"], stamp)
        self.assertEqual(write_ref.written["orderDate"], "2026-08-28")

    async def test_closed_release_gate_creates_legacy_compatible_unstamped_draft(self):
        payload = _payload()
        cycle = _matching_cycle(payload)
        resolution = orders.resolve_order_schedule_for_delivery_date(payload.deliveryDate, [cycle])
        payload.scheduleKey = resolution["scheduleKey"]
        stamp = orders._build_scheduled_order(resolution, "America/Denver")
        write_ref = _WriteRef()

        with patch.object(
            orders, "require_route_feature_access", new=AsyncMock(return_value={})
        ), patch.object(
            orders, "_resolve_create_schedule", return_value=(resolution, stamp)
        ), patch.object(
            orders, "_get_local_order_date", return_value="2026-08-28"
        ), patch.object(
            orders, "_order_ref", return_value=write_ref
        ), patch.object(
            orders, "stamp_write_allowed_for_headers", return_value=False
        ), patch.object(orders, "_log_order_audit"):
            created = await orders.create_order.__wrapped__(
                request=None,
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=object(),
            )

        self.assertIsNone(created.scheduledOrder)
        self.assertNotIn("scheduledOrder", write_ref.written)
        self.assertEqual(created.expectedLoadDate, resolution["loadDate"])


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import AsyncMock, patch

from order_forecast.api.routers import schedule


class _Snapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return dict(self._data or {})


class _Document:
    def __init__(self, database, collection_name, doc_id):
        self.database = database
        self.collection_name = collection_name
        self.doc_id = doc_id

    def get(self):
        return _Snapshot(
            self.doc_id,
            self.database.data.get((self.collection_name, self.doc_id)),
        )


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


class ScheduleDeliveryDateTests(unittest.IsolatedAsyncioTestCase):
    async def test_v3_preview_preserves_send_day_and_requires_compatible_client(self):
        from types import SimpleNamespace
        from order_forecast.api.errors import StructuredApiError
        cycle = dict(scheduleVersion=3, orderDay=2, sendOffsetDays=2, loadOffsetDays=6,
                     deliveryOffsetDays=6, loadDay=1, deliveryDay=1, needsScheduleReview=False)
        context = dict(cycles=[cycle], timezone='America/Denver', status='ready', sourceVersion=3,
                       sourceRevision=4, ownerUid='owner-a')
        with patch.object(schedule, 'require_route_access', new=AsyncMock()), \
                patch.object(schedule, 'get_order_schedule_context_from_firestore', return_value=context), \
                patch.dict('os.environ', {'ORDER_SCHEDULE_V3_CREATION_ENABLED': 'true',
                    'ORDER_SCHEDULE_V3_PILOT_ROUTES': '988200',
                    'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMERS_ENABLED': 'true',
                    'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMER_ROUTES': '988200'}):
            args = dict(route='988200', deliveryDate='2026-09-14', scheduleKey='tuesday',
                        decoded_token={'uid': 'owner-a'}, db=object())
            result = await schedule.get_schedule_key_for_date(
                request=SimpleNamespace(headers={'x-routespark-capabilities': 'order-schedule-v3'}), **args)
            self.assertTrue(result.matched)
            self.assertEqual(result.scheduledOrder.version, 3)
            self.assertEqual(result.scheduledOrder.sendDate, '2026-09-10')
            self.assertEqual(result.scheduledOrder.sourceRevision, 4)
            self.assertEqual(result.loadDate, '2026-09-14')
            with self.assertRaises(StructuredApiError):
                await schedule.get_schedule_key_for_date(request=SimpleNamespace(headers={}), **args)

    def test_schedule_owner_authority_fails_closed_on_legacy_field_conflict(self):
        db = _Database({
            ("routes", "988200"): {"ownerUid": "owner-a", "userId": "owner-b"},
        })
        self.assertEqual(
            schedule.resolve_schedule_owner_uid(db, "988200"),
            (None, "owner_conflict"),
        )

        aligned = _Database({
            ("routes", "988200"): {"ownerUid": "owner-a"},
            ("routeEntitlements", "988200"): {"ownerUid": "owner-a"},
        })
        self.assertEqual(
            schedule.resolve_schedule_owner_uid(aligned, "988200"),
            ("owner-a", "ready"),
        )

    def test_one_owner_snapshot_supplies_both_cycles_and_timezone(self):
        cycle = {
            "orderDay": 2,
            "loadDay": 4,
            "deliveryDay": 4,
            "loadOffsetDays": 2,
            "deliveryOffsetDays": 2,
        }
        db = _Database({
            ("routes", "988200"): {"ownerUid": "owner-a"},
            ("users", "owner-a"): {
                "profile": {"timezone": "America/Denver"},
                "userSettings": {
                    "notifications": {"scheduling": {"orderCycles": [cycle]}}
                },
            },
        })
        context = schedule.get_order_schedule_context_from_firestore(db, "988200")
        self.assertEqual(context["status"], "ready")
        self.assertEqual(context["ownerUid"], "owner-a")
        self.assertEqual(context["timezone"], "America/Denver")
        self.assertEqual(len(context["cycles"]), 1)
        self.assertEqual(context["cycles"][0]["orderDay"], 2)

    async def test_unique_delivery_cycle_returns_v2_snapshot(self):
        resolution = {
            "status": "resolved",
            "orderDate": "2026-08-10",
            "loadDate": "2026-08-12",
            "deliveryDate": "2026-08-13",
            "scheduleKey": "monday",
            "cycle": {
                "orderDay": 1,
                "loadDay": 3,
                "deliveryDay": 4,
                "loadOffsetDays": 2,
                "deliveryOffsetDays": 3,
                "needsScheduleReview": False,
                "scheduleVersion": 2,
            },
        }
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule,
            "get_order_schedule_context_from_firestore",
            return_value={"cycles": [], "timezone": "America/Denver", "status": "ready"},
        ), patch.object(
            schedule, "resolve_order_schedule_for_delivery_date", return_value=resolution
        ), patch.object(
            schedule, "stamp_write_allowed_for_headers", return_value=True
        ):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-13",
                scheduleKey=None,
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertIsInstance(result, schedule.ScheduleKeyResponse)
        self.assertTrue(result.matched)
        self.assertEqual(result.scheduleKey, "monday")
        self.assertEqual(result.orderDate, "2026-08-10")
        self.assertEqual(result.loadDate, "2026-08-12")
        self.assertEqual(result.timezone, "America/Denver")
        self.assertEqual(result.scheduledOrder.version, 2)
        self.assertEqual(result.scheduledOrder.date, "2026-08-10")

    async def test_unmatched_date_fails_closed_without_fabricated_key(self):
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule,
            "get_order_schedule_context_from_firestore",
            return_value={"cycles": [], "timezone": "America/Denver", "status": "ready"},
        ), patch.object(
            schedule,
            "resolve_order_schedule_for_delivery_date",
            return_value={"status": "unresolved", "reason": "off_cycle"},
        ):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-10",
                scheduleKey=None,
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertIsInstance(result, schedule.ScheduleKeyResponse)
        self.assertFalse(result.matched)
        self.assertIsNone(result.scheduleKey)
        self.assertEqual(result.reason, "off_cycle")

    async def test_resolved_cycle_withholds_stamp_when_release_gate_is_closed(self):
        resolution = {
            "status": "resolved",
            "orderDate": "2026-08-10",
            "loadDate": "2026-08-12",
            "deliveryDate": "2026-08-13",
            "scheduleKey": "monday",
            "cycle": {
                "orderDay": 1,
                "loadDay": 3,
                "deliveryDay": 4,
                "loadOffsetDays": 2,
                "deliveryOffsetDays": 3,
                "needsScheduleReview": False,
                "scheduleVersion": 2,
            },
        }
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule,
            "get_order_schedule_context_from_firestore",
            return_value={"cycles": [], "timezone": "America/Denver", "status": "ready"},
        ), patch.object(
            schedule, "resolve_order_schedule_for_delivery_date", return_value=resolution
        ), patch.object(
            schedule, "stamp_write_allowed_for_headers", return_value=False
        ):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-13",
                scheduleKey=None,
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertTrue(result.matched)
        self.assertEqual(result.orderDate, "2026-08-10")
        self.assertIsNone(result.scheduledOrder)

    async def test_valid_cycle_without_valid_timezone_is_unresolved(self):
        resolution = {
            "status": "resolved",
            "orderDate": "2026-08-10",
            "loadDate": "2026-08-12",
            "deliveryDate": "2026-08-13",
            "scheduleKey": "monday",
            "cycle": {
                "orderDay": 1,
                "loadOffsetDays": 2,
                "deliveryOffsetDays": 3,
            },
        }
        with patch.object(schedule, "require_route_access", new=AsyncMock()), patch.object(
            schedule,
            "get_order_schedule_context_from_firestore",
            return_value={"cycles": [], "timezone": None, "status": "invalid_timezone"},
        ), patch.object(
            schedule, "resolve_order_schedule_for_delivery_date", return_value=resolution
        ):
            result = await schedule.get_schedule_key_for_date(
                request=None,
                route="988200",
                deliveryDate="2026-08-13",
                scheduleKey=None,
                decoded_token={"uid": "test"},
                db=object(),
            )

        self.assertFalse(result.matched)
        self.assertEqual(result.reason, "invalid_timezone")


if __name__ == "__main__":
    unittest.main()

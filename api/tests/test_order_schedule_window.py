from copy import deepcopy
from datetime import datetime, timezone
import unittest

from order_forecast.api.models import Order
from order_forecast.api.order_schedule_window import (
    ScheduleWindowError, resolve_scheduled_adjustment_window,
)
from order_forecast.api.routers import orders
from order_forecast.api.tests.test_order_next_order_finalize import _OrderRef, _StoresRef, _Transaction
from order_forecast.api.tests.test_order_schedule_evidence import draft
from order_forecast.scripts.order_adjustment_schedule import (
    WindowResolutionError, canonical_source_window,
)


class ScheduledAdjustmentWindowTests(unittest.TestCase):
    def finalized(self, now):
        return orders._finalize_order_document.to_wrap(
            _Transaction(), order_ref=_OrderRef(draft()), stores_ref=_StoresRef([]),
            route_number='900001', now=now, resolved_adjustment_anchor=None,
        )

    def test_actual_finalizer_and_response_model_feed_dormant_reader(self):
        now = datetime(2026, 9, 11, 13, 0, 0, 123456, tzinfo=timezone.utc)
        source = Order(**self.finalized(now)).model_dump()
        before = deepcopy(source)
        result = resolve_scheduled_adjustment_window(source, '11:00', 1789131600123)
        self.assertEqual(result['status'], 'open')
        self.assertEqual(result['adjustmentDate'], '2026-09-11')
        self.assertEqual(result['availableAtMs'], 1789131600123)
        self.assertEqual(result['cutoffAtMs'], 1789146000000)
        self.assertEqual(source, before)

    def test_finalizing_at_cutoff_or_after_never_grants_next_day(self):
        for day, hour in [(11, 17), (11, 18), (12, 13)]:
            now = datetime(2026, 9, day, hour, tzinfo=timezone.utc)
            with self.subTest(now=now):
                result = resolve_scheduled_adjustment_window(self.finalized(now), '11:00', int(now.timestamp()) * 1000)
                self.assertEqual(result['status'], 'closed')
                self.assertEqual(result['adjustmentDate'], '2026-09-11')
                self.assertIsNone(result['availableAtMs'])

    def test_finalized_source_requires_timezone_aware_submitted_timestamp(self):
        now = datetime(2026, 9, 11, 13, tzinfo=timezone.utc)
        source = self.finalized(now)
        for bad in [now.replace(tzinfo=None), now.isoformat(), True, 1789131600000]:
            with self.subTest(bad=bad), self.assertRaisesRegex(ScheduleWindowError, 'invalid_finalization'):
                resolve_scheduled_adjustment_window({**source, 'submittedAt': bad}, '11:00', 1789131600000)

    def test_repeated_finalization_does_not_rewrite_source(self):
        from fastapi import HTTPException
        source = self.finalized(datetime(2026, 9, 11, 13, tzinfo=timezone.utc))
        before = deepcopy(source)
        transaction = _Transaction()
        with self.assertRaises(HTTPException) as error:
            orders._finalize_order_document.to_wrap(
                transaction, order_ref=_OrderRef(source), stores_ref=_StoresRef([]),
                route_number='900001', now=datetime(2026, 9, 12, 13, tzinfo=timezone.utc),
            )
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(transaction.updates, [])
        self.assertEqual(source, before)

    def test_existing_reminder_entry_point_still_refuses_v3(self):
        source = self.finalized(datetime(2026, 9, 11, 13, tzinfo=timezone.utc))
        with self.assertRaises(WindowResolutionError) as error:
            canonical_source_window(source, cutoff_time_local='11:00', reminder_lead_minutes=30)
        self.assertEqual(error.exception.code, 'unsupported_schedule_version')


if __name__ == '__main__':
    unittest.main()

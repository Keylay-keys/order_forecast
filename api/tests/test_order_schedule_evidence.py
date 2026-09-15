from copy import deepcopy
from datetime import datetime, timezone
import unittest
from fastapi import HTTPException

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.models import Order
from order_forecast.api.order_schedule_evidence import build_evidence, read_evidence, validate_order_evidence
from order_forecast.api.routers import orders
from order_forecast.api.tests.test_order_next_order_finalize import _OrderRef, _StoresRef, _Transaction
from order_forecast.scripts.order_adjustment_schedule import canonical_source_window, WindowResolutionError

CONTROL = {'version': 3, 'revision': 7, 'ownerUid': 'owner-test', 'routeNumber': '900001', 'timezone': 'America/Denver'}
CYCLE = {'scheduleVersion': 3, 'orderDay': 2, 'sendOffsetDays': 2, 'loadOffsetDays': 6,
         'deliveryOffsetDays': 6, 'loadDay': 1, 'deliveryDay': 1, 'needsScheduleReview': False}
STAMP = build_evidence(CONTROL, [CYCLE], '2026-09-14', 'tuesday')


def draft():
    now = datetime(2026, 9, 11, 13, tzinfo=timezone.utc)
    return {'id': 'test-order', 'routeNumber': '900001', 'userId': 'owner-test',
            'orderDate': '2026-09-08', 'expectedDeliveryDate': '2026-09-14', 'expectedLoadDate': '2026-09-14',
            'scheduleKey': 'tuesday', 'scheduledOrder': deepcopy(STAMP), 'status': 'draft',
            'stores': [], 'createdAt': now, 'updatedAt': now}


class OrderEvidenceTests(unittest.TestCase):
    def test_archive_receipt_cannot_certify_unknown_evidence_as_legacy(self):
        from order_forecast.scripts.order_archive_receipt import build_order_archive_projection, evaluate_order_archive_receipt
        self.assertEqual(evaluate_order_archive_receipt({'schemaVersion': 0}, {'schemaVersion': 0}), 'unsupported_schedule_evidence')
        for version in (3, 4):
            with self.assertRaisesRegex(ValueError, 'unsupported_schedule_evidence'):
                build_order_archive_projection('test-order', '900001', {**draft(), 'scheduledOrder': {**STAMP, 'version': version}})
    def test_exact_source_link_round_trips_through_real_response_model(self):
        self.assertEqual(STAMP['date'], '2026-09-08')
        self.assertEqual(STAMP['sendDate'], '2026-09-10')
        self.assertEqual(STAMP['sourceRevision'], 7)
        self.assertEqual(Order(**draft()).model_dump()['scheduledOrder'], STAMP)
        self.assertEqual(read_evidence(STAMP), STAMP)

    def test_new_source_never_changes_prior_evidence(self):
        prior = deepcopy(STAMP)
        newer = build_evidence({**CONTROL, 'revision': 8}, [{**CYCLE, 'sendOffsetDays': 3}], '2026-09-14', 'tuesday')
        self.assertEqual(newer['sendDate'], '2026-09-11')
        self.assertEqual(STAMP, prior)

    def test_rejects_incomplete_tampered_or_unknown_evidence(self):
        cases = [{**STAMP, **change} for change in [
            {'version': 4}, {'version': True}, {'sourceRevision': True}, {'sourceRevision': 0},
            {'sourceRevision': 9007199254740992}, {'sourceOwnerUid': '../owner'}, {'sourceRouteNumber': '9/1'},
            {'sendOffsetDays': True}, {'sendOffsetDays': 7}, {'sendDate': '2026-09-11'},
            {'date': '2026-09-09'}, {'timezone': 'Mars/Olympus'}, {'extra': 1}, {'provenance': 'backfill'},
        ]]
        cases += [{key: value for key, value in STAMP.items() if key != omitted} for omitted in STAMP]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                read_evidence(case)

    def test_cross_route_or_dates_cannot_relabel_evidence(self):
        for change in [{'routeNumber': '900002'}, {'expectedLoadDate': '2026-09-13'},
                       {'expectedDeliveryDate': '2026-09-21'}, {'scheduleKey': 'friday'}]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                Order(**{**draft(), **change})

    def test_legacy_reader_keeps_exact_v2_shape(self):
        from order_forecast.api.tests.test_scheduled_order_model import VALID
        self.assertEqual(read_evidence(VALID), VALID)
        self.assertEqual(Order(**{**draft(), 'scheduledOrder': VALID}).scheduledOrder.version, 2)

    def test_finalization_uses_saved_timezone_and_keeps_revision(self):
        transaction = _Transaction()
        now = datetime(2026, 9, 11, 13, tzinfo=timezone.utc)
        result = orders._finalize_order_document.to_wrap(transaction, order_ref=_OrderRef(draft()),
                  stores_ref=_StoresRef([]), route_number='900001', now=now,
                  resolved_adjustment_anchor={'wrong': 'pre-read anchor'})
        self.assertEqual(result['scheduledOrder'], STAMP)
        self.assertEqual(result['submittedAt'], now)
        self.assertEqual(result['adjustmentAnchor']['timezone'], 'America/Denver')
        self.assertEqual(result['adjustmentAnchor']['finalizedDate'], '2026-09-11')
        self.assertNotIn('scheduledOrder', transaction.updates[0][1])
        self.assertEqual(Order(**result).scheduledOrder.sourceRevision, 7)
        for change in [{'submittedAt': datetime(2026, 9, 12, 13, tzinfo=timezone.utc)},
                       {'adjustmentAnchor': {**result['adjustmentAnchor'], 'timezone': 'UTC'}}]:
            with self.assertRaises(ValueError):
                Order(**{**result, **change})
        with self.assertRaises(HTTPException) as repeated:
            orders._finalize_order_document.to_wrap(_Transaction(), order_ref=_OrderRef(result),
                stores_ref=_StoresRef([]), route_number='900001', now=now)
        self.assertEqual(repeated.exception.status_code, 409)

    def test_finalization_rejects_bad_links_before_any_write(self):
        for change in [{'expectedDeliveryDate': '2026-09-21'}, {'adjustmentAnchor': {}},
                       {'scheduledOrder': {**STAMP, 'version': 4}}]:
            transaction = _Transaction()
            with self.subTest(change=change), self.assertRaises(StructuredApiError) as error:
                orders._finalize_order_document.to_wrap(transaction, order_ref=_OrderRef({**draft(), **change}),
                    stores_ref=_StoresRef([]), route_number='900001', now=datetime.now(timezone.utc))
            self.assertEqual(error.exception.code, 'ORDER_SCHEDULE_EVIDENCE_INVALID')
            self.assertEqual(transaction.updates, [])

    def test_anchor_cannot_send_v3_or_unknown_into_legacy_window(self):
        order = draft()
        order['adjustmentAnchor'] = orders._build_adjustment_anchor(datetime(2026, 9, 11, 13, tzinfo=timezone.utc), 'America/Denver')
        for version in (3, 4):
            order['scheduledOrder']['version'] = version
            with self.assertRaises(WindowResolutionError) as error:
                canonical_source_window(order, cutoff_time_local='11:00', reminder_lead_minutes=30)
            self.assertEqual(error.exception.code, 'unsupported_schedule_version')


if __name__ == '__main__':
    unittest.main()

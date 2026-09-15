from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from order_forecast.api.tests import test_order_adjustment_reminder_reconcile as api_fakes
from order_forecast.scripts import test_order_adjustment_reminder_worker as worker_fakes
from order_forecast.scripts.order_schedule_v3_release import ENABLED_ENV, ROUTES_ENV, v3_consumers_allowed
from order_forecast.scripts.order_adjustment_schedule import canonical_source_window, WindowResolutionError

worker = worker_fakes.worker
OPEN = 1789106400000  # Friday midnight Denver
FINALIZE = 1789131600000  # Friday 7 AM
CUTOFF = 1789146000000  # Friday 11 AM
DUE = CUTOFF - 15 * 60000


def source(route='989567', order_id='order-a', finalized=FINALIZE):
    result = {
        'id': order_id, 'routeNumber': route, 'scheduleKey': 'tuesday',
        'expectedLoadDate': '2026-09-14', 'expectedDeliveryDate': '2026-09-14',
        'status': 'draft', 'scheduledOrder': {
            'version': 3, 'date': '2026-09-08', 'sendDate': '2026-09-10', 'timezone': 'America/Denver',
            'scheduleKey': 'tuesday', 'orderDay': 2, 'sendOffsetDays': 2,
            'loadOffsetDays': 6, 'deliveryOffsetDays': 6, 'provenance': 'cycle',
            'sourceRevision': 7, 'sourceOwnerUid': 'owner-test', 'sourceRouteNumber': route,
        },
    }
    if finalized is not None:
        from zoneinfo import ZoneInfo
        stamp = datetime.fromtimestamp(finalized / 1000, timezone.utc)
        result.update(status='finalized', submittedAt=stamp, adjustmentAnchor={
            'version': 1, 'finalizedAtMs': finalized, 'timezone': 'America/Denver',
            'finalizedDate': stamp.astimezone(ZoneInfo('America/Denver')).date().isoformat(),
        })
    return result


class ScheduleConsumerTests(unittest.TestCase):
    def setUp(self):
        self.gate = patch.dict(os.environ, {ENABLED_ENV: 'true', ROUTES_ENV: '988200,989567'})
        self.gate.start()
        self.addCleanup(self.gate.stop)

    def test_release_gate_requires_exact_enabled_flag_and_bounded_valid_allowlist(self):
        self.assertFalse(v3_consumers_allowed('989567', {}))
        for flag in ('TRUE', '1', 'yes', '', False):
            self.assertFalse(v3_consumers_allowed('989567', {ENABLED_ENV: flag, ROUTES_ENV: '989567'}))
        for routes in ('', '*', '989567,', ' 989567', '989567,invalid', ',989567', '9/89567',
                       '989567,' + ','.join(['1'] * 100), None):
            self.assertFalse(v3_consumers_allowed('989567', {ENABLED_ENV: 'true', ROUTES_ENV: routes}))
        self.assertFalse(v3_consumers_allowed('989568'))
        self.assertTrue(v3_consumers_allowed('989567'))

    def api_call(self, **kwargs):
        return api_fakes.ReconcileTests().call(
            source_data=kwargs.pop('source_data', source('988200', 'order-v2')),
            now_ms=kwargs.pop('now_ms', FINALIZE), **kwargs)

    def test_api_reads_source_and_settings_not_forged_adjustment_snapshot(self):
        tx, result = self.api_call(adjustment_data=api_fakes.adjustment(
            sourceOrderSnapshot={'scheduledOrder': {'sendDate': '2099-01-01'}},
            adjustmentDate='2099-01-02', timezone='Asia/Tokyo', cutoffAtMs=1,
        ))
        self.assertEqual(tx.events[:3], [('read', 'adjustment'), ('read', 'source'), ('read', 'requester-settings')])
        self.assertEqual(result['fields']['adjustmentDate'], '2026-09-11')
        self.assertEqual(result['fields']['cutoffAtMs'], CUTOFF)
        self.assertEqual(result['fields']['timezone'], 'America/Denver')
        canonical = {**api_fakes.adjustment(), **result['fields']}
        tx2, result2 = self.api_call(adjustment_data=canonical)
        self.assertFalse(result2['changed'])
        self.assertEqual(tx2.updates, [])

    def test_api_off_gate_cannot_be_enabled_by_adjustment_fields(self):
        for env in ({ENABLED_ENV: 'false'}, {ROUTES_ENV: '111111'}):
            with patch.dict(os.environ, env):
                _, result = self.api_call(adjustment_data=api_fakes.adjustment(allowV3=True))
                self.assertEqual(result['fields']['reminderStatus'], 'skipped')
                self.assertIsNone(result['fields']['reminderAtMs'])

    def test_api_draft_requires_finalize_and_exact_cutoff_is_closed(self):
        _, draft_result = self.api_call(source_data=source('988200', 'order-v2', None))
        self.assertEqual(draft_result['fields']['reminderSkipReason'], 'needs_finalization')
        self.assertIsNone(draft_result['fields']['reminderAtMs'])
        _, finalized_result = self.api_call(adjustment_data={**api_fakes.adjustment(), **draft_result['fields']})
        self.assertEqual(finalized_result['fields']['reminderStatus'], 'pending')
        for stamp in (None, FINALIZE, CUTOFF):
            _, expired = self.api_call(source_data=source('988200', 'order-v2', stamp), now_ms=CUTOFF)
            self.assertEqual(expired['fields']['reminderSkipReason'], 'past_cutoff')

    def test_api_rejects_source_identity_mismatch(self):
        for bad in (source('111111', 'order-v2'), source('988200', 'another-order')):
            with self.assertRaises(Exception) as error:
                self.api_call(source_data=bad)
            self.assertIn(error.exception.code, ('MISSING_SOURCE_ORDER', 'ADJUSTMENT_SOURCE_CHANGED'))

    def test_reminder_clamped_to_opening_or_late_finalization(self):
        for finalized, lead, now, due in [(OPEN - 60000, 1440, OPEN - 1, OPEN),
                                          (CUTOFF - 60000, 15, CUTOFF - 60000, CUTOFF - 60000)]:
            window = canonical_source_window(source(finalized=finalized), cutoff_time_local='11:00',
                reminder_lead_minutes=lead, now_ms=now, allow_v3=True)
            self.assertEqual(window.reminder_at_ms, due)
        with self.assertRaises(WindowResolutionError):
            canonical_source_window(source(), cutoff_time_local='11:00', reminder_lead_minutes=15, now_ms=FINALIZE)

    def db(self, order=None, **adjustment):
        db = worker_fakes._build_db()
        worker_fakes._add_source_order(db, **(source() if order is None else order))
        worker_fakes._add_adjustment(db, sourceOrderId='order-a', cutoffTimeLocal='11:00',
                                     reminderLeadMinutes=15, reminderAtMs=1, **adjustment)
        return db

    def tick(self, db, now):
        with patch.object(worker.firestore, 'transactional', side_effect=lambda fn: fn), patch.object(
            worker, '_send_expo_push', return_value={'sent': 1, 'failed': 0}
        ) as push:
            stats = worker.run_once(db, now_ms=now, limit=10)
        return stats, push.call_count

    def test_worker_due_once_no_duplicate_and_ignores_copied_snapshot(self):
        db = self.db(sourceOrderSnapshot={'scheduledOrder': {'sendDate': '2099-01-01'}})
        first, count = self.tick(db, DUE)
        second, second_count = self.tick(db, DUE + 1)
        self.assertEqual((first['claimed'], first['sent'], count), (1, 1, 1))
        self.assertEqual((second['claimed'], second_count), (0, 0))

    def test_worker_off_gate_unfinalized_bad_source_and_before_window_never_send(self):
        for bad in (source(finalized=None), {**source(), 'routeNumber': '111111'},
                    {**source(), 'id': 'different'}, {**source(), 'submittedAt': None}):
            stats, count = self.tick(self.db(bad), DUE)
            self.assertEqual((stats['claimed'], count), (0, 0))
        stats, count = self.tick(self.db(source(finalized=OPEN - 60000)), OPEN - 1)
        self.assertEqual((stats['claimed'], count), (0, 0))
        with patch.dict(os.environ, {ENABLED_ENV: 'false'}):
            stats, count = self.tick(self.db(), DUE)
            self.assertEqual((stats['claimed'], count), (0, 0))

    def test_v3_stale_claim_has_no_cutoff_grace(self):
        db = self.db(reminderStatus='sending', reminderClaimedAtMs=1)
        with patch.object(worker, 'ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS', 120000):
            stats, count = self.tick(db, CUTOFF)
        self.assertEqual((stats['claimed'], count), (0, 0))

    def test_send_rechecks_gate_source_and_cutoff_after_claim(self):
        for reason in ('gate', 'source', 'cutoff'):
            db = self.db()
            doc = next(iter(worker._pending_query(db, DUE, 10)))
            with patch.object(worker.firestore, 'transactional', side_effect=lambda fn: fn):
                claimed = worker.claim_adjustment_reminder(db, doc, now_ms=DUE)
            self.assertIsNotNone(claimed)
            if reason == 'source':
                worker_fakes._add_source_order(db, **source(finalized=None), submittedAt=None, adjustmentAnchor=None)
            with patch.dict(os.environ, {ENABLED_ENV: 'false' if reason == 'gate' else 'true'}), patch.object(
                worker, '_send_expo_push'
            ) as push:
                self.assertFalse(worker.send_claimed_reminder(db, claimed, now_ms=CUTOFF if reason == 'cutoff' else DUE))
                push.assert_not_called()

    def test_production_tick_refreshes_clock_between_claim_and_send(self):
        db = self.db()
        with patch.object(worker, '_now_ms', side_effect=[DUE, DUE, CUTOFF]), patch.object(
            worker.firestore, 'transactional', side_effect=lambda fn: fn
        ), patch.object(worker, '_send_expo_push') as push:
            stats = worker.run_once(db, limit=10)
        self.assertEqual(stats['claimed'], 1)
        self.assertEqual(stats['sent'], 0)
        push.assert_not_called()

    def test_container_layout_can_execute_enabled_consumer(self):
        import json
        data = source()
        data['submittedAt'] = {'seconds': FINALIZE // 1000, 'nanoseconds': 0}
        code = ('import json; from scripts.order_adjustment_schedule import canonical_source_window; '
                'v=json.loads(input()); w=canonical_source_window(v, cutoff_time_local="11:00", '
                f'reminder_lead_minutes=15, now_ms={FINALIZE}, allow_v3=True); assert w.cutoff_at_ms == {CUTOFF}')
        completed = subprocess.run([sys.executable, '-c', code], input=json.dumps(data), text=True,
            capture_output=True, timeout=30, cwd=Path(__file__).resolve().parents[2],
            env={'PATH': '/usr/bin:/bin', 'PYTHONPATH': '.', 'PYTHONDONTWRITEBYTECODE': '1'})
        self.assertEqual(completed.returncode, 0, completed.stderr)

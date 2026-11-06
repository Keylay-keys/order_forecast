"""Full-year email jobs retain their lease and resume saved reads safely."""
import asyncio
from contextlib import contextmanager
from hashlib import sha256
from unittest import TestCase
from unittest.mock import AsyncMock, Mock, patch

from order_forecast.api import accounting_email_worker as worker
from order_forecast.api.tests.test_accounting import MemoryStore
from order_forecast.api.tests.test_accounting_payment_advice import TEXT
from order_forecast.api.tests.test_accounting_email import message, PDF


class WorkerProgressTests(TestCase):
    def test_unapproved_rewritten_sender_is_held_with_a_machine_readable_reason(self):
        sender = 'outlook_0123456789ABCDEF@outlook.com'
        raw = message(sender)
        job = dict(id='message', lease='lease', enabled=True, uid='owner', route='123456',
            raw=raw, digest=sha256(raw).hexdigest(), result={}, address='private@example.test',
            forwarder='owner@example.com', authorized_senders=[])
        with patch.object(worker, 'require_business_access', new_callable=AsyncMock), \
             patch.object(worker, 'finish') as finish:
            asyncio.run(worker.process(job, object()))
        status, result = finish.call_args.args[1:]
        self.assertEqual(status, 'needs_attention')
        self.assertEqual(result['reason'], 'sender_not_allowed')
        self.assertEqual(result['senderProvider'], 'outlook')
        self.assertEqual(result['detail'], worker.intake.SENDER_MISMATCH_DETAIL)

    def test_advice_records_once_outside_pdf_transaction_and_never_becomes_a_statement(self):
        raw = message('PaymentAdvices@gruma.com', [(PDF, 'Payment Advice Note from 09_15_2026.PDF')])
        store = MemoryStore()
        store.cursor = Mock()
        accounting = worker.Accounting(store, '123456', '2026-09-21')
        ledger = accounting.load('money-in')
        ledger['settlements'] = [dict(periodStart='2026-08-27', periodEnd='2026-09-02', revision='final',
            source='sha256:' + 'a' * 64, amountToPayCents=125025, statementReview={'fileId':'verified'})]
        store.put('money-in', ledger)
        transaction_active = [False]
        @contextmanager
        def transaction(*_):
            transaction_active[0] = True
            try: yield store
            finally: transaction_active[0] = False
        def read(*_):
            self.assertFalse(transaction_active[0])
            return worker.payment_advice.parse_text(TEXT, '123456')
        job = dict(id='message', lease='lease', enabled=True, uid='owner', route='123456',
            raw=raw, digest=sha256(raw).hexdigest(), result={}, address='private@example.test', forwarder='owner@gmail.com')
        with patch.object(worker, 'accounting_transaction', transaction), \
             patch.object(worker, 'require_business_access', new_callable=AsyncMock), \
             patch.object(worker, 'lease'), patch.object(worker, 'finish') as finish, \
             patch.object(worker, 'now', return_value='2026-09-21T12:00:00Z'), \
             patch.object(worker.payment_advice, 'read_bytes', side_effect=read) as reader, \
             patch.object(worker.evidence, 'sync_statement_updates', side_effect=lambda domain: domain.load('money-in')):
            asyncio.run(worker.process(job, object()))
            asyncio.run(worker.process(job, object()))
        self.assertEqual(reader.call_count, 1)
        self.assertEqual(finish.call_args.args[1], 'ready')
        self.assertEqual(len(accounting.load('money-in')['remittances']), 1)
        self.assertEqual(store.documents('statement-upload:'), [])
        self.assertEqual(finish.call_args.args[2]['paymentAdviceRecorded'], 1)

    def test_progress_renews_only_the_current_live_lease(self):
        cursor = Mock()
        cursor.fetchone.return_value = (1,)
        @contextmanager
        def transaction():
            yield cursor
        job = dict(id='message', lease='lease')
        result = dict(statements=[{}, {}])
        with patch.object(worker.intake, 'transaction', transaction):
            worker.progress(job, result, 1)
            self.assertEqual((result['processed'], result['total']), (1, 2))
            self.assertIn("lease_expires_at>now()", cursor.execute.call_args_list[0].args[0])
            self.assertIn("interval '10 minutes'", cursor.execute.call_args_list[1].args[0])
            self.assertEqual(cursor.execute.call_args_list[1].args[1][1:], ('message', 'lease'))
            cursor.reset_mock()
            cursor.fetchone.return_value = None
            with self.assertRaisesRegex(RuntimeError, 'lease changed'):
                worker.progress(job, result, 1)
            self.assertEqual(cursor.execute.call_count, 1)

    def test_resumes_106_saved_originals_without_rereading_completed_files(self):
        raw = b'saved email'
        refs = [dict(uploadId='upload', fileId=str(i)) for i in range(106)]
        job = dict(id='message', lease='lease', enabled=True, uid='owner', route='123456',
                   raw=raw, digest=sha256(raw).hexdigest(), result=dict(saved=True, statements=refs, duplicates=2))
        store = Mock()
        in_transaction = [False]
        @contextmanager
        def transaction(*args):
            in_transaction[0] = True
            try: yield store
            finally: in_transaction[0] = False
        def read(*args):
            self.assertFalse(in_transaction[0])
            return {'analysis': True}
        with patch.object(worker, 'accounting_transaction', transaction), \
             patch.object(worker, 'require_business_access', new_callable=AsyncMock) as access, \
             patch.object(worker, 'lease'), patch.object(worker, 'progress') as progress, \
             patch.object(worker, 'finish') as finish, patch.object(worker.intake, 'parse_message') as parse, \
             patch.object(worker.evidence, 'latest_statement', side_effect=lambda _, upload, file: {'analysis': True} if int(file) < 103 else None), \
             patch.object(worker.evidence, 'original', return_value={'content': b'PDF'}), \
             patch.object(worker.evidence, 'analyze_bytes', side_effect=read) as analyze, \
             patch.object(worker.evidence, 'analyze_statement', return_value={'analysis': True}), \
             patch.object(worker.evidence, 'sync_statement_updates', return_value={'settlements': []}) as sync, \
             patch.object(worker.payment_advice, 'reconcile', return_value={'settlements': []}):
            asyncio.run(worker.process(job, object()))
        parse.assert_not_called()
        self.assertEqual(analyze.call_count, 3)
        self.assertEqual(progress.call_count, 106)
        self.assertEqual(access.call_count, 108)
        sync.assert_called_once()
        self.assertEqual(finish.call_args.args[1], 'ready')
        self.assertEqual(job['result']['processed'], 106)
        self.assertEqual(job['result']['duplicates'], 2)

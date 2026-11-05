"""Attention recovery preserves evidence and binds every action to its workspace."""
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi import HTTPException
from order_forecast.api import accounting_email as intake
from order_forecast.api.routers.accounting import dispatch
from order_forecast.api.tests.test_accounting import MemoryStore


class RecoveryTests(TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.store.scope = ('owner', '123456')
        self.store.cursor = MagicMock()
        self.store.cursor.fetchone.return_value = ('needs_attention', 1, None)
        self.mid = str(uuid4())

    def test_handle_reopen_audit_and_idempotent_state(self):
        first = intake.handle_arrival(self.store, {'id': self.mid, 'handled': True})
        self.assertTrue(first['handledAt'])
        self.assertEqual(intake.handle_arrival(self.store, {'id': self.mid, 'handled': True}), first)
        self.assertEqual(len(self.store.history), 1)
        self.assertIsNone(intake.handle_arrival(self.store, {'id': self.mid, 'handled': False})['handledAt'])
        self.assertEqual(len(self.store.history), 2)
        self.assertEqual(set(self.store.docs), {'email-handled:' + self.mid})
        self.assertFalse(self.store.originals)
        sql, scope = self.store.cursor.execute.call_args.args
        self.assertIn('FOR UPDATE OF j', sql)
        self.assertEqual(scope, ('owner', '123456', self.mid))

    def test_unknown_scope_and_nonterminal_messages_cannot_be_handled(self):
        for row, code in ((None, 404), (('queued', 0, None), 409), (('ready', 1, None), 409), (('reading', 3, False), 409)):
            self.store.cursor.fetchone.return_value = row
            with self.assertRaises(HTTPException) as error:
                intake.handle_arrival(self.store, {'id': self.mid, 'handled': True})
            self.assertEqual(error.exception.status_code, code)
        self.assertFalse(self.store.docs)
        self.store.cursor.fetchone.return_value = ('reading', 3, True)
        self.assertTrue(intake.handle_arrival(self.store, {'id': self.mid, 'handled': True})['handledAt'])

    def test_invalid_identity_types_and_extra_fields_rejected_before_sql(self):
        for body in ({'id': '../other', 'handled': True}, {'id': self.mid, 'handled': 'true'}, {'id': self.mid, 'handled': True, 'uid': 'other'}):
            with self.assertRaises(HTTPException):
                intake.handle_arrival(self.store, body)
        self.store.cursor.execute.assert_not_called()
        with self.assertRaises(HTTPException):
            dispatch(self.store, '123456', '2026-09-19', 'GET', 'email-intake/handle', {}, [], None)

    def test_headers_are_bounded_plain_text_and_never_include_body(self):
        raw = b'From: "SAP ADMIN" <SAPADMIN@GRUMA.COM>\r\nSubject: Mission Foods Order Reminder\r\n\r\nPRIVATE BODY'
        self.assertEqual(intake.arrival_headers(raw), {'sender': 'SAPADMIN@GRUMA.COM', 'subject': 'Mission Foods Order Reminder'})
        self.assertEqual(len(intake.arrival_headers(b'Subject: ' + b'a'*1000 + b'\n\n')['subject']), 200)

    @patch.object(intake.inbox, 'pending', return_value=None)
    @patch.object(intake, 'available', return_value=True)
    def test_refresh_projects_handled_and_metadata_without_changing_worker_result(self, *_):
        stamp = datetime.now(timezone.utc)
        result = {'detail': 'Sender not allowed'}
        self.store.cursor.fetchone.return_value = ('private@example.com', 'owner@example.com', True)
        self.store.cursor.fetchall.return_value = [(self.mid, stamp, 'needs_attention', result, 1, None,
            b'From: other@example.com\nSubject: Reminder\n\nPRIVATE BODY', {'handledAt': stamp.isoformat()})]
        response = intake.setup(self.store, '123456', 'GET', {})
        arrival = response['messages'][0]
        self.assertEqual(arrival['status'], 'needs_attention')
        self.assertEqual(arrival['handledAt'], stamp.isoformat())
        self.assertEqual(arrival['subject'], 'Reminder')
        self.assertEqual(result, {'detail': 'Sender not allowed'})
        self.assertNotIn('PRIVATE BODY', str(response))
        self.assertFalse(self.store.history)

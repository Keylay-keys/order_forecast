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

    def test_owner_approves_only_the_exact_sender_from_the_held_arrival(self):
        sender = 'outlook_0123456789ABCDEF@outlook.com'
        self.store.cursor.fetchone.side_effect = [('needs_attention', {'reason': 'sender_not_allowed'},
            'private@example.com', 'owner@example.com', f'From: Person <{sender}>\r\nSubject: Test\r\n\r\n'.encode()), None]
        result = intake.approve_sender(self.store, {'id': self.mid})
        self.assertEqual(result, {'id': self.mid, 'senderApproved': True, 'queued': True})
        calls = self.store.cursor.execute.call_args_list
        self.assertIn('FOR UPDATE OF j', calls[0].args[0])
        self.assertEqual(calls[0].args[1], ('owner', '123456', self.mid))
        self.assertIn('SELECT 1 FROM accounting_email_sender_bindings', calls[1].args[0])
        self.assertEqual(calls[1].args[1], ('private@example.com', 'owner@example.com', sender.lower()))
        self.assertIn('INSERT INTO accounting_email_sender_bindings', calls[2].args[0])
        self.assertEqual(calls[2].args[1], ('private@example.com', 'owner@example.com', sender.lower(), 'outlook', self.mid))
        self.assertIn("status='queued'", calls[3].args[0])
        self.assertEqual(calls[3].args[1], (self.mid,))

    def test_sender_approval_retry_is_idempotent_after_a_lost_response(self):
        sender = 'outlook_0123456789abcdef@outlook.com'
        self.store.cursor.fetchone.side_effect = [('queued', {}, 'private@example.com', 'owner@example.com',
            f'From: Person <{sender}>\r\n\r\n'.encode()), (1,)]
        self.assertEqual(intake.approve_sender(self.store, {'id': self.mid}),
            {'id': self.mid, 'senderApproved': True, 'queued': True})
        self.assertEqual(self.store.cursor.execute.call_count, 2)
        self.assertNotIn('INSERT', self.store.cursor.execute.call_args_list[1].args[0])

    def test_sender_approval_fails_closed_for_wrong_reason_status_shape_or_provider(self):
        with self.assertRaises(HTTPException):
            intake.approve_sender(self.store, {'id': self.mid, 'sender': 'outlook_0123456789abcdef@outlook.com'})
        self.store.cursor.execute.assert_not_called()
        cases = [
            ('ready', {'reason': 'sender_not_allowed'}, 'outlook_0123456789abcdef@outlook.com'),
            ('needs_attention', {'detail': 'No PDF'}, 'outlook_0123456789abcdef@outlook.com'),
            ('needs_attention', {'reason': 'sender_not_allowed'}, 'not-an-email'),
        ]
        for status, result, sender in cases:
            self.store.cursor.reset_mock()
            self.store.cursor.fetchone.side_effect = [(status, result, 'private@example.com', 'owner@example.com',
                f'From: Person <{sender}>\r\n\r\n'.encode()), None]
            with self.assertRaises(HTTPException) as error:
                intake.approve_sender(self.store, {'id': self.mid})
            self.assertEqual(error.exception.status_code, 409)
            self.assertLessEqual(self.store.cursor.execute.call_count, 2)

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

    @patch.object(intake.inbox, 'pending', return_value=None)
    @patch.object(intake, 'available', return_value=True)
    def test_refresh_offers_exact_outlook_approval_for_new_and_legacy_sender_errors(self, *_):
        stamp = datetime.now(timezone.utc)
        header = b'From: Person <outlook_0123456789ABCDEF@outlook.com>\r\nSubject: Test\r\n\r\n'
        self.store.cursor.fetchone.return_value = ('private@example.com', 'owner@example.com', True)
        for result in ({'reason': 'sender_not_allowed', 'detail': intake.SENDER_MISMATCH_DETAIL},
                       {'detail': intake.LEGACY_SENDER_MISMATCH_DETAIL}):
            self.store.cursor.fetchall.return_value = [(self.mid, stamp, 'needs_attention', result, 1, None, header, None)]
            arrival = intake.setup(self.store, '123456', 'GET', {})['messages'][0]
            self.assertTrue(arrival['senderApprovalAvailable'])
            self.assertEqual(arrival['senderProvider'], 'outlook')
            self.assertEqual(arrival['sender'], 'outlook_0123456789ABCDEF@outlook.com')

    @patch.object(intake.inbox, 'pending', return_value=None)
    @patch.object(intake, 'available', return_value=True)
    def test_refresh_offers_exact_icloud_and_generic_forwarding_approval(self, *_):
        stamp = datetime.now(timezone.utc)
        self.store.cursor.fetchone.return_value = ('private@example.com', 'owner@icloud.com', True)
        for sender, provider in (('relay@icloud.com', 'icloud'), ('rewrite@relay.example', 'icloud')):
            header = f'From: Person <{sender}>\r\nSubject: Test\r\n\r\n'.encode()
            self.store.cursor.fetchall.return_value = [(self.mid, stamp, 'needs_attention',
                {'reason': 'sender_not_allowed'}, 1, None, header, None)]
            arrival = intake.setup(self.store, '123456', 'GET', {})['messages'][0]
            self.assertTrue(arrival['senderApprovalAvailable'])
            self.assertEqual(arrival['senderProvider'], provider)

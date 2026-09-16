"""Inbox possession, replay, durable attempt limits and tenant separation."""
from contextlib import contextmanager
from copy import deepcopy
import json
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from order_forecast.api import accounting_email_sender as inbox
from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.routers import accounting
from order_forecast.api.tests.test_accounting import MemoryStore
from order_forecast.api.tests.test_business_access import fixture

ENV = {'ACCOUNTING_EMAIL_DOMAIN': 'settlements.routespark.pro', 'ACCOUNTING_EMAIL_SECRET': 'x'*64,
       'MAILJET_API_KEY': 'test', 'MAILJET_SECRET_KEY': 'test'}


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.stores = {}
        self.in_transaction = False
        @contextmanager
        def transaction(uid, route):
            key = uid, route
            original = self.stores.setdefault(key, MemoryStore())
            working = deepcopy(original)
            working.scope = key
            self.in_transaction = True
            try:
                yield working
                self.stores[key] = working
            finally:
                self.in_transaction = False
        for target, value in [('accounting_transaction', transaction), ('bind', MagicMock()), ('send_code', MagicMock(return_value=True))]:
            patcher = patch.object(inbox, target, value); patcher.start(); self.addCleanup(patcher.stop)
        env = patch.dict('os.environ', ENV); env.start(); self.addCleanup(env.stop)
        clock = patch.object(inbox.time, 'time', return_value=1000000); self.clock = clock.start(); self.addCleanup(clock.stop)
        self.uid, self.route = 'owner', '989262'

    def call(self, action, body, rid=None, fingerprint=None):
        return inbox.configure(self.uid, self.route, action, body, rid or str(uuid4()), fingerprint or json.dumps(body, sort_keys=True))

    def start(self):
        return self.call('sender-code', {'email': 'inbox@yahoo.com', '_verifiedEmail': 'owner@gmail.com'})['pendingInbox']

    def verify(self, pending, code):
        return self.call('sender-verify', {'challengeId': pending['challengeId'], 'code': code})

    def test_verified_login_shortcut_never_sends_and_does_not_trust_other_address(self):
        self.assertTrue(self.call('sender-code', {'email': ' Owner@Gmail.com ', '_verifiedEmail': 'owner@gmail.com'})['verified'])
        inbox.send_code.assert_not_called()
        inbox.bind.assert_called_once()
        self.start()
        self.assertEqual(inbox.bind.call_count, 1)

    def test_code_required_bound_to_scope_one_use_and_never_exposed(self):
        pending = self.start()
        code = inbox.send_code.call_args.args[1]
        self.assertNotIn(code, json.dumps(self.stores[self.uid, self.route].docs))
        self.assertNotIn('hash', pending)
        inbox.bind.assert_not_called()
        self.uid = 'another-owner'
        self.assertFalse(self.verify(pending, code)['verified'])
        self.uid = 'owner'; self.route = '123456'
        self.assertFalse(self.verify(pending, code)['verified'])
        self.route = '989262'
        self.assertTrue(self.verify(pending, code)['verified'])
        inbox.bind.assert_called_once()
        self.assertFalse(self.verify(pending, code)['verified'])

    def test_failed_attempts_persist_and_correct_code_cannot_bypass_limit(self):
        pending = self.start(); code = inbox.send_code.call_args.args[1]
        for _ in range(5):
            self.assertFalse(self.verify(pending, 'bad-code')['verified'])
        self.assertEqual(self.stores[self.uid, self.route].get(inbox.KEY)['attempts'], 5)
        self.assertFalse(self.verify(pending, code)['verified'])
        inbox.bind.assert_not_called()

    def test_expiry_replacement_and_rate_limits(self):
        old = self.start(); old_code = inbox.send_code.call_args.args[1]
        with self.assertRaises(HTTPException) as error: self.start()
        self.assertEqual(error.exception.status_code, 429)
        self.clock.return_value += 61
        new = self.start()
        self.assertFalse(self.verify(old, old_code)['verified'])
        code = inbox.send_code.call_args.args[1]
        self.clock.return_value += 601
        self.assertFalse(self.verify(new, code)['verified'])
        inbox.bind.assert_not_called()

    def test_send_outside_transaction_failure_consumes_allowance_and_replay_cannot_resend(self):
        def failed_send(*_):
            self.assertFalse(self.in_transaction)
            return False
        inbox.send_code.side_effect = failed_send
        rid = str(uuid4()); body = {'email': 'inbox@yahoo.com'}
        first = self.call('sender-code', body, rid)
        self.assertIn('verificationError', first)
        self.assertEqual(self.call('sender-code', body, rid), first)
        self.assertEqual(inbox.send_code.call_count, 1)
        with self.assertRaises(HTTPException): self.call('sender-code', {'email': 'different@yahoo.com'}, rid)
        with self.assertRaises(HTTPException): self.start()

    def test_daily_limit_survives_successful_verification(self):
        for _ in range(10):
            pending = self.start()
            self.assertTrue(self.verify(pending, inbox.send_code.call_args.args[1])['verified'])
            self.clock.return_value += 61
        with self.assertRaises(HTTPException): self.start()

    def test_validation_blocks_header_injection_lists_and_apple_relays(self):
        for email in ('one@example.com\nBcc: other@example.com', 'one@example.com,two@example.com', 'Name <one@example.com>', 'a@privaterelay.appleid.com', 'a@private.icloud.com', None, 'a'*65+'@example.com', 'a@-bad.example'):
            with self.assertRaises(HTTPException): inbox.normalize_email(email)
        self.assertEqual(inbox.normalize_email(' User+route@Business.example '), 'user+route@business.example')


class InboxHttpTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI(); app.include_router(accounting.router, prefix='/api')
        self.db = fixture()
        app.dependency_overrides[get_firestore] = lambda: self.db
        app.dependency_overrides[verify_firebase_token] = lambda: {'uid':'owner', 'email':'owner@gmail.com', 'email_verified':True}
        self.client = TestClient(app)

    def post(self, path, body):
        return self.client.post('/api/business/accounting/email-intake/' + path + '?route=989262', json=body, headers={'Idempotency-Key':str(uuid4())})

    def test_token_identity_is_injected_and_client_identity_rejected(self):
        with patch.object(inbox, 'configure', return_value={'verified':True}) as configure:
            self.assertEqual(self.post('sender-code', {'email':'other@yahoo.com'}).status_code, 200)
            self.assertEqual(configure.call_args.args[3], {'email':'other@yahoo.com', '_verifiedEmail':'owner@gmail.com'})
            configure.reset_mock()
            self.assertEqual(self.post('sender-code', {'email':'other@yahoo.com', '_verifiedEmail':'other@yahoo.com'}).status_code, 400)
            configure.assert_not_called()

    def test_access_revocation_prevents_sending(self):
        self.db.set_document('routes/989262', {'ownerUid':'another-owner'})
        with patch.object(inbox, 'configure') as configure:
            self.assertEqual(self.post('sender-code', {'email':'other@yahoo.com'}).status_code, 403)
            configure.assert_not_called()


if __name__ == '__main__': unittest.main()

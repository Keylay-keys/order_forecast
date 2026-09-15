"""Trust boundaries, MIME handling, and duplicate-safe review-only ingestion."""
from email.message import EmailMessage
import hashlib
import hmac
import time
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api import accounting_email as intake
from order_forecast.api.routers import accounting_email as relay
from order_forecast.api.dependencies import get_firestore
from order_forecast.api.tests.test_accounting import MemoryStore

ADDRESS = 's-' + 'a' * 32 + '@settlements.routespark.pro'
ENV = {'ACCOUNTING_EMAIL_DOMAIN': 'settlements.routespark.pro', 'ACCOUNTING_EMAIL_SECRET': 'x' * 64}
PDF = b'%PDF-1.4\nSample settlement\n%%EOF'


def message(sender='owner@gmail.com', attachments=None):
    msg = EmailMessage()
    msg['From'], msg['To'], msg['Subject'] = sender, ADDRESS, 'Fwd: Route settlement statement 123456'
    msg.set_content('Forwarded settlement')
    for data, filename in (attachments if attachments is not None else [(PDF, 'Statement.pdf')]):
        msg.add_attachment(data, maintype='application', subtype='pdf', filename=filename)
    return msg.as_bytes()


def signature(raw, address=ADDRESS, sender='owner@gmail.com', timestamp=None):
    timestamp = timestamp or str(int(time.time()))
    digest = hashlib.sha256(raw).hexdigest()
    sig = hmac.new(ENV['ACCOUNTING_EMAIL_SECRET'].encode(), f'{timestamp}\n{address}\n{sender}\n{digest}'.encode(), hashlib.sha256).hexdigest()
    return {'X-Settlement-Time': timestamp, 'X-Settlement-Recipient': address, 'X-Settlement-Sender': sender,
            'X-Settlement-SHA256': digest, 'X-Settlement-Signature': sig, 'Content-Type': 'message/rfc822'}


class MimeTests(unittest.TestCase):
    def test_manual_and_automatic_forwarding(self):
        for sender in ('owner@gmail.com', 'Notification@gruma.com'):
            _, files = intake.parse_message(message(sender), ADDRESS, 'owner@gmail.com')
            self.assertEqual(files, [('Statement.pdf', 'application/pdf', PDF)])

    def test_sender_restricted_and_fake_pdf_rejected(self):
        for raw in (message('stranger@example.com'), message(attachments=[(b'<html>bad</html>', 'x.pdf')]), message(attachments=[])):
            with self.assertRaises(ValueError):
                intake.parse_message(raw, ADDRESS, 'owner@gmail.com')

    def test_duplicate_attachments_and_filename_path(self):
        _, files = intake.parse_message(message(attachments=[(PDF, '../../Statement.pdf'), (PDF, 'copy.pdf')]), ADDRESS, 'owner@gmail.com')
        self.assertEqual(len(files), 1)
        self.assertNotIn('/', files[0][0])

    def test_forwarded_message_attachment(self):
        inner = EmailMessage()
        inner.set_content('Inner')
        inner.add_attachment(PDF, maintype='application', subtype='pdf', filename='nested.pdf')
        outer = EmailMessage()
        outer['From'] = 'owner@gmail.com'
        outer.set_content('Attached forward')
        outer.add_attachment(inner)
        self.assertEqual(len(intake.parse_message(outer.as_bytes(), ADDRESS, 'owner@gmail.com')[1]), 1)

    def test_gmail_code_requires_matching_address_and_owner(self):
        msg = EmailMessage()
        msg['From'], msg['Subject'] = intake.VERIFIER, 'Gmail Forwarding Confirmation'
        msg.set_content(f'Confirmation code: 123456789\n{ADDRESS}\nowner@gmail.com')
        self.assertEqual(intake.parse_message(msg.as_bytes(), ADDRESS, 'owner@gmail.com'), ({'verificationCode':'123456789'}, []))
        with self.assertRaises(ValueError):
            intake.parse_message(msg.as_bytes(), ADDRESS, 'other@gmail.com')

    def test_original_deduplication_does_not_post_money(self):
        store = MemoryStore()
        refs, duplicates = intake.save_files(store, '123456', [('one.pdf', 'application/pdf', PDF)])
        self.assertEqual((len(refs), duplicates), (1, 0))
        again, duplicates = intake.save_files(store, '123456', [('renamed.pdf', 'application/pdf', PDF)])
        self.assertEqual((again, duplicates), ([], 1))
        self.assertTrue(all(key.startswith('statement-upload:') for key in store.docs))


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict('os.environ', ENV)
        self.env.start()
        self.addCleanup(self.env.stop)
        app = FastAPI()
        app.include_router(relay.router)
        app.dependency_overrides[get_firestore] = lambda: object()
        self.client = TestClient(app)

    def test_signature_binds_every_envelope_field_and_timestamp(self):
        raw = message()
        headers = signature(raw)
        args = [headers[k] for k in ('X-Settlement-Time', 'X-Settlement-Recipient', 'X-Settlement-Sender', 'X-Settlement-SHA256', 'X-Settlement-Signature')]
        intake.verify_signature(*args)
        for index, value in ((0, str(int(time.time()) - 600)), (1, ADDRESS.replace('s-a', 's-b')), (2,'intruder@example.com'), (3,'0'*64), (4,'0'*64)):
            invalid = args.copy(); invalid[index] = value
            with self.assertRaises(HTTPException): intake.verify_signature(*invalid)

    def test_authentication_precedes_storage_and_body_hash_is_checked(self):
        raw = message()
        with patch.object(intake, 'address_scope', return_value=('uid','123456','owner@gmail.com')), patch.object(relay, 'require_business_access') as guard, patch.object(intake, 'accept') as accept:
            self.assertEqual(self.client.post('/business/settlement-email/receive', content=raw).status_code, 401)
            accept.assert_not_called()
            response = self.client.post('/business/settlement-email/receive', content=raw+b'tampered', headers=signature(raw))
            self.assertEqual(response.status_code, 400)
            accept.assert_not_called()
            guard.assert_called_once()

    def test_ack_only_after_accept_and_eligibility(self):
        raw = message()
        with patch.object(intake, 'address_scope', return_value=('uid','123456','owner@gmail.com')), patch.object(relay, 'require_business_access'), patch.object(intake, 'accept', return_value={'accepted':True}) as accept:
            self.assertEqual(self.client.post('/business/settlement-email/receive', content=raw, headers=signature(raw)).status_code, 202)
            accept.assert_called_once_with(ADDRESS,'uid','123456','owner@gmail.com',raw)
        with patch.object(intake, 'address_scope', return_value=('uid','123456','owner@gmail.com')), patch.object(relay, 'require_business_access', side_effect=HTTPException(403,'Denied')), patch.object(intake, 'accept') as accept:
            self.assertEqual(self.client.post('/business/settlement-email/receive', content=raw, headers=signature(raw)).status_code, 403)
            accept.assert_not_called()

if __name__ == '__main__': unittest.main()

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
    def test_payment_advice_is_classified_separately_and_sapadmin_stays_blocked(self):
        digest = hashlib.sha256(PDF).hexdigest()
        for sender in ('PaymentAdvices@gruma.com', 'owner@gmail.com'):
            metadata, files = intake.parse_message(message(sender, [(PDF, 'Payment Advice Note from 09_15_2026.PDF')]), ADDRESS, 'owner@gmail.com')
            self.assertEqual(metadata['paymentAdviceDigests'], [digest])
            self.assertEqual(len(files), 1)
        with self.assertRaises(ValueError):
            intake.parse_message(message('SAPADMIN@gruma.com'), ADDRESS, 'owner@gmail.com')

    def test_mixed_attached_advice_and_statements_are_not_both_classified_as_advice(self):
        metadata, files = intake.parse_message(message(attachments=[(PDF, 'Payment Advice Note from 09_15_2026.PDF'),
            (PDF.replace(b'Sample', b'Distinct'), 'Settlement statement.pdf')]), ADDRESS, 'owner@gmail.com')
        self.assertEqual(len(files), 2)
        self.assertEqual(metadata['paymentAdviceDigests'], [hashlib.sha256(PDF).hexdigest()])

    def test_manual_and_automatic_forwarding(self):
        for sender in ('owner@gmail.com', 'Notification@gruma.com'):
            _, files = intake.parse_message(message(sender), ADDRESS, 'owner@gmail.com')
            self.assertEqual(files, [('Statement.pdf', 'application/pdf', PDF)])

    def test_outlook_rewritten_sender_requires_one_exact_owner_approved_binding(self):
        sender = 'outlook_0123456789ABCDEF@outlook.com'
        with self.assertRaises(intake.SenderApprovalRequired) as error:
            intake.parse_message(message(sender), ADDRESS, 'owner@example.com')
        self.assertEqual(error.exception.provider, 'outlook')
        _, files = intake.parse_message(message(sender), ADDRESS, 'owner@example.com', [sender.lower()])
        self.assertEqual(files, [('Statement.pdf', 'application/pdf', PDF)])
        with self.assertRaises(intake.SenderApprovalRequired):
            intake.parse_message(message('outlook_FEDCBA9876543210@outlook.com'), ADDRESS,
                                 'owner@example.com', [sender.lower()])

    def test_sender_approval_classifies_one_exact_address_without_trusting_a_domain(self):
        self.assertEqual(intake.sender_binding_provider('outlook_0123456789abcdef@outlook.com'), 'outlook')
        self.assertEqual(intake.sender_binding_provider('relay@icloud.com'), 'icloud')
        self.assertEqual(intake.sender_binding_provider('rewritten@relay.example', 'owner@icloud.com'), 'icloud')
        self.assertEqual(intake.sender_binding_provider('person@outlook.com'), 'forwarding')
        for sender in ('outlook_bad', 'two@@example.com', 'person@example', 'sapadmin@gruma.com',
                       'outlook_0123456789abcdef@outlook.com.evil.test\nBcc: bad@example.com'):
            self.assertIsNone(intake.sender_binding_provider(sender))
        with self.assertRaisesRegex(intake.SenderApprovalRequired, intake.SENDER_MISMATCH_DETAIL):
            intake.parse_message(message('person@outlook.com'), ADDRESS, 'owner@example.com')

    def test_icloud_and_unknown_forwarders_require_exact_approval_then_parse(self):
        for sender, forwarder, provider in (
            ('relay@icloud.com', 'owner@icloud.com', 'icloud'),
            ('rewrite@provider.example', 'owner@custom.example', 'forwarding'),
        ):
            with self.assertRaises(intake.SenderApprovalRequired) as error:
                intake.parse_message(message(sender), ADDRESS, forwarder)
            self.assertEqual(error.exception.provider, provider)
            _, files = intake.parse_message(message(sender), ADDRESS, forwarder, [sender])
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

    def test_full_year_of_forwarded_preliminary_and_final_emails(self):
        outer = EmailMessage()
        outer['From'] = 'owner@gmail.com'
        outer.set_content('All statements since January 1')
        for index in range(106):
            inner = EmailMessage()
            inner['Subject'] = 'Settlement 123456'
            inner.set_content('Original body')
            inner.add_alternative('<p>Original body</p>', subtype='html')
            inner.add_attachment(b'%PDF-1.4\n' + str(index).encode() + b'\n%%EOF', maintype='application', subtype='pdf', filename='Settlement statement.pdf')
            outer.add_attachment(inner)
        _, files = intake.parse_message(outer.as_bytes(), ADDRESS, 'owner@gmail.com')
        self.assertEqual(len(files), 106)
        store = MemoryStore()
        refs, duplicates = intake.save_files(store, '123456', files)
        self.assertEqual((len(refs), duplicates), (106, 0))
        self.assertEqual(intake.save_files(store, '123456', files), ([], 106))
        self.assertTrue(all(key.startswith('statement-upload:') for key in store.docs))

    def test_bulk_intake_keeps_bounded_size_count_and_depth(self):
        files = [(b'%PDF-1.4\n' + str(i).encode() + b'\n%%EOF', str(i)+'.pdf') for i in range(121)]
        with self.assertRaisesRegex(ValueError, '120'):
            intake.parse_message(message(attachments=files), ADDRESS, 'owner@gmail.com')
        with self.assertRaisesRegex(ValueError, '25 MB'):
            intake.parse_message(b'0' * (intake.MAX_EMAIL + 1), ADDRESS, 'owner@gmail.com')
        inner = EmailMessage(); inner.set_content('nested')
        for _ in range(14):
            outer = EmailMessage(); outer.set_content('body'); outer.add_attachment(inner); inner = outer
        inner['From'] = 'owner@gmail.com'
        with self.assertRaisesRegex(ValueError, 'too complex'):
            intake.parse_message(inner.as_bytes(), ADDRESS, 'owner@gmail.com')

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

    def test_gmail_link_only_confirmation_is_restricted_to_google_verification(self):
        msg = EmailMessage()
        msg['From'], msg['Subject'] = intake.VERIFIER, '(Gmail Forwarding Confirmation'
        link = 'https://mail.google.com/mail/vf-%5Btest-token%5D-proof'
        msg.set_content(f'{ADDRESS}\nowner@gmail.com\n{link}\nhttps://mail.google.com/mail/uf-cancel')
        self.assertEqual(intake.parse_message(msg.as_bytes(), ADDRESS, 'owner@gmail.com'), ({'verificationUrl':link}, []))
        with self.assertRaises(ValueError):
            intake.parse_message(msg.as_bytes(), ADDRESS, 'other@gmail.com')
        for unsafe in ('https://mail.google.com.evil.test/mail/vf-token', 'https://mail.google.com/mail/uf-cancel', 'https://mail.google.com/mail/vf-token?redirect=https://evil.test'):
            msg.set_content(f'{ADDRESS}\nowner@gmail.com\n{unsafe}')
            with self.assertRaises(ValueError):
                intake.parse_message(msg.as_bytes(), ADDRESS, 'owner@gmail.com')


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

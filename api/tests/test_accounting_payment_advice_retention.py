"""Retention operates only on synthetic MIME data and exact server-owned matches."""
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
import hashlib
from unittest import TestCase

from order_forecast.api.accounting_payment_advice_retention import discard_matched

ADVICE = b'%PDF-1.4\n' + b'Synthetic advice only\n' * 50
STATEMENT = b'%PDF-1.4\nUnrelated settlement must remain\n%%EOF'
UNMATCHED = b'%PDF-1.4\nUnmatched advice must remain\n%%EOF'
digest = lambda value: hashlib.sha256(value).hexdigest()


def fixture(nested=False):
    message = EmailMessage()
    message['From'] = 'owner@example.test'
    message['Subject'] = 'Synthetic batch'
    message.set_content('Keep this email body.')
    for content, name in [(ADVICE,'Payment Advice Note from date.pdf'),(STATEMENT,'statement.pdf'),(UNMATCHED,'unmatched.pdf')]:
        message.add_attachment(content, maintype='application', subtype='pdf', filename=name)
    message.add_attachment(b'keep image bytes', maintype='image', subtype='png', filename='photo.png')
    if nested:
        outer = EmailMessage()
        outer['From'] = 'owner@example.test'
        outer.set_content('Keep outer body.')
        outer.add_attachment(message)
        message = outer
    result = dict(saved=True,paymentAdviceDocuments=['matched','unmatched'],paymentAdviceMatchedDocuments=['matched'],
        paymentAdviceFiles=[dict(documentNumber='matched',sha256=digest(ADVICE)),dict(documentNumber='unmatched',sha256=digest(UNMATCHED))])
    return message.as_bytes(), result


class RetentionTests(TestCase):
    def test_exact_pdf_only_is_discarded_including_attached_email_and_retry(self):
        for nested in (False,True):
            raw, result = fixture(nested)
            reduced, updated = discard_matched(raw,result)
            self.assertLess(len(reduced),len(raw))
            message = BytesParser(policy=policy.default).parsebytes(reduced)
            attachments = [part.get_payload(decode=True) for part in message.walk() if not part.is_multipart()]
            self.assertNotIn(ADVICE,attachments)
            for value in (STATEMENT,UNMATCHED,b'keep image bytes'):
                self.assertIn(value,attachments)
            self.assertIn(b'Keep this email body.',reduced)
            self.assertEqual(updated['paymentAdviceRemovedDocuments'],['matched'])
            self.assertEqual(updated['retainedContentSha256'],digest(reduced))
            self.assertEqual(discard_matched(reduced,updated),(None,updated))
            updated['paymentAdviceMatchedDocuments'].append('unmatched')
            final, metadata = discard_matched(reduced,updated)
            self.assertIsNotNone(final)
            self.assertEqual(metadata['paymentAdviceRemovedDocuments'],['matched','unmatched'])

    def test_unmatched_and_unknown_digest_preserve_original(self):
        raw,result=fixture()
        result['paymentAdviceMatchedDocuments']=[]
        self.assertEqual(discard_matched(raw,result),(None,result))
        result['paymentAdviceMatchedDocuments']=['matched']
        result['paymentAdviceFiles'][0]['sha256']='f'*64
        with self.assertRaises(ValueError):discard_matched(raw,result)

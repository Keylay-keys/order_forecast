"""Discard only exactly matched advice attachments from the retained MIME copy.

Run outside database transactions. The original delivery digest remains the
deduplication identity; the rewritten copy has a separate integrity digest.
"""
from email import policy
from email.parser import BytesParser
import hashlib

from .accounting_domain import now


def discard_matched(raw, result):
    matched = set(result.get('paymentAdviceMatchedDocuments', []))
    previous = set(result.get('paymentAdviceRemovedDocuments', []))
    files = [f for f in result.get('paymentAdviceFiles', []) if f['documentNumber'] in matched - previous]
    if not files:
        return None, result
    desired = {f['sha256'] for f in files}
    if not 0 < len(raw) <= 25 * 1024 * 1024:
        raise ValueError('Payment advice retention could not complete. Its PDF was kept for review.')
    message = BytesParser(policy=policy.default).parsebytes(raw)
    stack, count, removed = [(message, 0)], 0, set()
    while stack:
        part, depth = stack.pop()
        count += 1
        if count > 2048 or depth > 12:
            raise ValueError('Payment advice retention could not complete. Its PDF was kept for review.')
        if part.is_multipart():
            stack.extend((child, depth + 1) for child in part.iter_parts())
            continue
        if part.get_content_type() != 'application/pdf' and not (part.get_filename() or '').lower().endswith('.pdf'):
            continue
        content = part.get_payload(decode=True) or b''
        digest = hashlib.sha256(content).hexdigest()
        if digest in desired:
            part.clear_content()
            part.set_content('Payment advice PDF removed after an exact settlement match. Payment details are retained in RouteSpark.')
            removed.add(digest)
    if removed != desired:
        raise ValueError('Payment advice retention could not complete. Its PDF was kept for review.')
    reduced = message.as_bytes(policy=policy.SMTP)
    if len(reduced) > 25 * 1024 * 1024:
        raise ValueError('Payment advice retention could not complete. Its PDF was kept for review.')
    return reduced, {**result,
        'originalContentBytes': result.get('originalContentBytes', len(raw)),
        'paymentAdviceRemovedDocuments': sorted(previous | {f['documentNumber'] for f in files}),
        'paymentAdviceRemovedAt': now(),
        'retainedContentSha256': hashlib.sha256(reduced).hexdigest()}

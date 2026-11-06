"""Private settlement addresses and durable MIME intake; financial matching is worker-only."""
from contextlib import contextmanager
from email import policy
from email.parser import BytesHeaderParser, BytesParser
from email.utils import getaddresses
import hashlib
import hmac
import os
import re
import secrets
import time
from uuid import uuid4

from psycopg2.extras import Json

from .accounting_domain import fail, identifier, now
from .accounting_store import accounting_transaction
from .dependencies import get_pg_connection, return_pg_connection
from . import accounting_evidence as evidence
from . import accounting_email_sender as inbox

MAX_EMAIL = 25 * 1024 * 1024
MAX_STATEMENT_PDFS = 120  # Includes both preliminary and final for a full year.
MAX_MIME_PARTS = 2048  # Attached .eml messages contain their own MIME trees.
SUPPLIER = "notification@gruma.com"
PAYMENT_SUPPLIER = "paymentadvices@gruma.com"
VERIFIER = "forwarding-noreply@google.com"
SENDER_MISMATCH_DETAIL = "This email used a forwarding address RouteSpark has not approved yet. Review and approve that exact address, or upload the PDF instead."
LEGACY_SENDER_MISMATCH_DETAIL = "Forward from your verified settlement inbox, or change your inbox in Email settlements."
UNCONNECTED_SENDER_DETAIL = "This email came from an address that is not connected to this route. Forward the PDF from your connected inbox, or upload it instead."
OUTLOOK_REWRITTEN_SENDER = re.compile(r"^outlook_[0-9a-f]{16,64}@outlook[.]com$", re.I)
APPROVABLE_SENDER = re.compile(
    r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    r"@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+",
    re.I,
)
ICLOUD_DOMAINS = {"icloud.com", "me.com", "mac.com"}
BLOCKED_APPROVAL_DOMAINS = {"gruma.com"}


class SenderApprovalRequired(ValueError):
    """A held message used one exact provider-rewritten sender identity."""

    def __init__(self, sender, provider):
        super().__init__(SENDER_MISMATCH_DETAIL if provider else UNCONNECTED_SENDER_DETAIL)
        self.sender = sender
        self.provider = provider


def sender_binding_provider(sender, forwarder=""):
    """Classify one exact approvable sender; the label never grants domain trust."""
    sender = (sender or "").lower()
    forwarder = (forwarder or "").lower()
    if not APPROVABLE_SENDER.fullmatch(sender) or len(sender) > 254 or len(sender.split("@", 1)[0]) > 64:
        return None
    if sender.rsplit("@", 1)[-1] in BLOCKED_APPROVAL_DOMAINS:
        return None
    if OUTLOOK_REWRITTEN_SENDER.fullmatch(sender):
        return "outlook"
    domains = {value.rsplit("@", 1)[-1] for value in (sender, forwarder) if "@" in value}
    return "icloud" if domains & ICLOUD_DOMAINS else "forwarding"


def domain():
    value = os.environ.get("ACCOUNTING_EMAIL_DOMAIN", "")
    return value if re.fullmatch(r"[a-z0-9]+(?:[.-][a-z0-9]+)+", value) else ""


def available():
    return bool(domain() and len(os.environ.get("ACCOUNTING_EMAIL_SECRET", "")) >= 48)


@contextmanager
def transaction():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute("SET LOCAL statement_timeout = '15s'")
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_pg_connection(conn)


def setup(store, route, method, body):
    if not available():
        if method != "GET":
            fail("Settlement email receiving is not available yet.", 503)
        return {"route": route, "available": False, "address": None, "messages": []}
    cur = store.cursor
    if method == "POST":
        # This field is supplied exclusively by the verified Firebase token.
        email = inbox.normalize_email(body.get("_verifiedEmail", ""))
        cur.execute("""INSERT INTO accounting_email_addresses (address,firebase_uid,route_number,forwarder_email)
            VALUES (%s,%s,%s,%s) ON CONFLICT (firebase_uid,route_number) DO NOTHING""",
            ("s-" + secrets.token_hex(16) + "@" + domain(), *store.scope, email))
    elif method != "GET":
        fail("Email intake endpoint not found.", 404)
    cur.execute("SELECT address,forwarder_email,enabled FROM accounting_email_addresses WHERE firebase_uid=%s AND route_number=%s", store.scope)
    row = cur.fetchone()
    cur.execute("""SELECT o.message_id,o.received_at,j.status,j.result,j.attempts,j.lease_expires_at<now(),
        CASE WHEN j.status IN ('needs_attention','blocked') OR (j.status='reading' AND j.attempts>=3 AND j.lease_expires_at<now())
            THEN substring(o.content FROM 1 FOR 65536) END, d.payload
        FROM accounting_email_originals o JOIN accounting_email_jobs j USING(message_id)
        LEFT JOIN accounting_documents d ON d.firebase_uid=o.firebase_uid AND d.route_number=o.route_number
            AND d.document_key='email-handled:' || o.message_id::text
        WHERE o.firebase_uid=%s AND o.route_number=%s ORDER BY o.received_at DESC LIMIT 30""", store.scope)
    message_rows = cur.fetchall()
    advice_records = {}
    if any("paymentAdviceDocuments" in result for _, _, _, result, *_ in message_rows):
        from . import accounting_payment_advice as payment_advice
        advice_records = {record["documentNumber"]: record for record in store.documents(payment_advice.PREFIX)}
    messages = []
    verified_at = (store.get(inbox.KEY) or {}).get("verifiedAt", 0)
    for mid, received, status, result, attempts, expired, header, handled in message_rows:
        if status == "reading" and attempts >= 3 and expired:
            status, result = "needs_attention", {"detail": "Processing stopped. Upload the PDF to continue."}
        # Verification codes expire from the UI, while the original evidence stays intact.
        result = dict(result)
        if "paymentAdviceDocuments" in result and status not in ("queued", "reading", "blocked"):
            from .accounting_payment_advice import arrival_status
            status, result = arrival_status(result, advice_records)
        if status in ("needs_attention", "blocked"):
            result.update(arrival_headers(header))
            provider = sender_binding_provider(result.get("sender", ""), row[1] if row else "")
            result["senderApprovalAvailable"] = bool(
                provider and (result.get("reason") == "sender_not_allowed"
                              or result.get("detail") == LEGACY_SENDER_MISMATCH_DETAIL)
            )
            if result["senderApprovalAvailable"]:
                result["senderProvider"] = provider
            result["handledAt"] = (handled or {}).get("handledAt")
        if time.time() - received.timestamp() > 3600 or received.timestamp() < verified_at:
            result.pop("verificationCode", None)
            result.pop("verificationUrl", None)
        messages.append(dict(id=str(mid), receivedAt=received.isoformat(), status=status, **result))
    return dict(route=route, available=True, address=row[0] if row and row[2] else None,
                forwarderEmail=row[1] if row else None, messages=messages,
                loginEmail=body.get("_loginEmail", body.get("_verifiedEmail", "")),
                inboxVerificationAvailable=inbox.available(), pendingInbox=inbox.pending(store),
                gmailFilter=f"from:({SUPPLIER}) subject:(settlement {route}) has:attachment filename:pdf",
                paymentAdviceGmailFilter=f'from:({PAYMENT_SUPPLIER}) subject:("Payment Advice Note from") has:attachment filename:pdf')


def arrival_headers(raw):
    """Bounded plain-text labels only. Never expose message bodies or HTML."""
    if not raw:
        return {}
    try:
        headers = BytesHeaderParser(policy=policy.default).parsebytes(bytes(raw)[:65536])
        clean = lambda value, limit: re.sub(r"[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]", " ", str(value or ""))[:limit].strip()
        senders = getaddresses(headers.get_all("From", []))
        return {"subject": clean(headers.get("Subject"), 200),
                "sender": clean(senders[0][1], 254) if len(senders) == 1 else ""}
    except (ValueError, IndexError):
        return {}


def handle_arrival(store, body):
    if set(body) != {"id", "handled"} or type(body.get("handled")) is not bool:
        fail("Choose whether this email still needs attention.")
    mid = identifier(body.get("id"))
    store.cursor.execute("""SELECT j.status,j.attempts,j.lease_expires_at<now()
        FROM accounting_email_originals o JOIN accounting_email_jobs j USING(message_id)
        WHERE o.firebase_uid=%s AND o.route_number=%s AND o.message_id=%s FOR UPDATE OF j""", (*store.scope, mid))
    row = store.cursor.fetchone()
    if not row:
        fail("Email arrival not found.", 404)
    status, attempts, expired = row
    if status not in ("needs_attention", "blocked") and not (status == "reading" and attempts >= 3 and expired):
        fail("This email no longer needs attention. Refresh its status.", 409)
    key = "email-handled:" + mid
    previous = store.get(key) or {}
    handled_at = (previous.get("handledAt") or now()) if body["handled"] else None
    result = {"id": mid, "handledAt": handled_at}
    if previous != result:
        store.put(key, result)
    return result


def approve_sender(store, body):
    """Bind only the exact rewritten sender from one owner-scoped held arrival."""
    if set(body) != {"id"}:
        fail("Choose the forwarded email to approve.")
    mid = identifier(body.get("id"))
    store.cursor.execute("""SELECT j.status,j.result,o.address,a.forwarder_email,substring(o.content FROM 1 FOR 65536)
        FROM accounting_email_originals o JOIN accounting_email_jobs j USING(message_id)
        JOIN accounting_email_addresses a USING(address)
        WHERE o.firebase_uid=%s AND o.route_number=%s AND o.message_id=%s FOR UPDATE OF j""", (*store.scope, mid))
    row = store.cursor.fetchone()
    if not row:
        fail("Email arrival not found.", 404)
    status, result, address, forwarder, header = row
    labels = arrival_headers(header)
    sender = labels.get("sender", "").lower()
    provider = sender_binding_provider(sender, forwarder)
    if not provider:
        fail("This forwarding address cannot be approved automatically.", 409)
    approval_pending = status == "needs_attention" and isinstance(result, dict) and (
        result.get("reason") == "sender_not_allowed" or result.get("detail") == LEGACY_SENDER_MISMATCH_DETAIL
    )
    store.cursor.execute("""SELECT 1 FROM accounting_email_sender_bindings
        WHERE address=%s AND verified_forwarder_email=%s AND sender_email=%s""", (address, forwarder, sender))
    already_approved = store.cursor.fetchone() is not None
    if already_approved and not approval_pending:
        return {"id": mid, "senderApproved": True, "queued": status in ("queued", "reading")}
    if not approval_pending:
        fail("This email is not waiting for sender approval. Refresh its status.", 409)
    if already_approved:
        store.cursor.execute("""UPDATE accounting_email_jobs SET status='queued',attempts=0,lease_token=NULL,
            lease_expires_at=NULL,result='{}'::jsonb,updated_at=now() WHERE message_id=%s""", (mid,))
        return {"id": mid, "senderApproved": True, "queued": True}
    store.cursor.execute("""INSERT INTO accounting_email_sender_bindings
        (address,verified_forwarder_email,sender_email,provider,source_message_id) VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (address,verified_forwarder_email,sender_email) DO NOTHING""",
        (address, forwarder, sender, provider, mid))
    store.cursor.execute("""UPDATE accounting_email_jobs SET status='queued',attempts=0,lease_token=NULL,
        lease_expires_at=NULL,result='{}'::jsonb,updated_at=now() WHERE message_id=%s""", (mid,))
    return {"id": mid, "senderApproved": True, "queued": True}


def verify_signature(timestamp, recipient, sender, digest, signature):
    key = os.environ.get("ACCOUNTING_EMAIL_SECRET", "")
    if not available() or not re.fullmatch(r"[0-9]{10}", timestamp or "") or abs(time.time() - int(timestamp)) > 300:
        fail("Email relay authentication required.", 401)
    if not re.fullmatch(r"s-[0-9a-f]{32}@" + re.escape(domain()), recipient or ""):
        fail("Unknown intake address.", 404)
    if len(sender) > 254 or any(c in sender for c in "\r\n"):
        fail("Invalid envelope.")
    expected = hmac.new(key.encode(), f"{timestamp}\n{recipient}\n{sender}\n{digest}".encode(), hashlib.sha256).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", signature or "") or not hmac.compare_digest(expected, signature):
        fail("Email relay authentication required.", 401)


def address_scope(address):
    with transaction() as cur:
        cur.execute("SELECT firebase_uid,route_number,forwarder_email FROM accounting_email_addresses WHERE address=%s AND enabled", (address,))
        row = cur.fetchone()
        if not row:
            fail("Unknown intake address.", 404)
        return row


def accept(address, uid, route, sender, raw):
    digest = hashlib.sha256(raw).hexdigest()
    # The same workspace lock serializes replay, rate checks and manual uploads.
    with accounting_transaction(uid, route) as store:
        cur = store.cursor
        cur.execute("SELECT message_id FROM accounting_email_originals WHERE address=%s AND sha256=%s", (address, digest))
        prior = cur.fetchone()
        if prior:
            return {"accepted": True, "duplicate": True, "id": str(prior[0])}
        cur.execute("""SELECT count(*),coalesce(sum(coalesce((j.result->>'originalContentBytes')::bigint,octet_length(o.content))),0)
            FROM accounting_email_originals o JOIN accounting_email_jobs j USING(message_id)
            WHERE o.firebase_uid=%s AND o.route_number=%s AND o.received_at>now()-interval '1 day'""", store.scope)
        count, size = cur.fetchone()
        if count >= 100 or size + len(raw) > 250 * 1024 * 1024:
            fail("Today's email intake limit was reached. Upload the PDF or try again tomorrow.", 429)
        mid = str(uuid4())
        cur.execute("""INSERT INTO accounting_email_originals (message_id,address,firebase_uid,route_number,sha256,envelope_from,content)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""", (mid, address, uid, route, digest, sender, raw))
        cur.execute("INSERT INTO accounting_email_jobs (message_id) VALUES (%s)", (mid,))
        return {"accepted": True, "duplicate": False, "id": mid}


def parse_message(raw, address, forwarder, authorized_senders=()):
    if not 0 < len(raw) <= MAX_EMAIL:
        raise ValueError("The email exceeds the 25 MB limit. Upload the PDFs separately.")
    message = BytesParser(policy=policy.default).parsebytes(raw)
    if len(message.get_all("From", [])) != 1:
        raise ValueError("The email sender could not be identified.")
    senders = getaddresses(message.get_all("From", []))
    sender = senders[0][1].lower() if len(senders) == 1 else ""
    allowed = {forwarder.lower(), SUPPLIER, PAYMENT_SUPPLIER, VERIFIER,
               *(value.lower() for value in authorized_senders)}
    if not sender or sender not in allowed:
        raise SenderApprovalRequired(sender, sender_binding_provider(sender, forwarder))
    # From is a routing restriction, not proof of sender identity. The private,
    # unguessable alias and signed provider relay are the authorization boundary.
    stack, parts, files, texts, hashes = [(message, 0)], 0, [], [], set()
    while stack:
        part, depth = stack.pop()
        parts += 1
        if parts > MAX_MIME_PARTS or depth > 12:
            raise ValueError("The email is too complex. Forward the PDF attachment directly.")
        if part.is_multipart():
            stack.extend((child, depth + 1) for child in part.iter_parts())
            continue
        data = part.get_payload(decode=True) or b""
        name = part.get_filename() or ""
        if part.get_content_type() == "application/pdf" or name.lower().endswith(".pdf"):
            if not 0 < len(data) <= 15 * 1024 * 1024 or evidence.detect_media(data) != ("application/pdf", "pdf"):
                raise ValueError("An attachment is not a supported PDF. Upload the original PDF separately.")
            digest = hashlib.sha256(data).hexdigest()
            if digest in hashes:
                continue
            hashes.add(digest)
            # Never treat an email filename as a path; preserve a safe display label.
            name = re.sub(r"[\\/\x00-\x1f\x7f]", "_", name)[:180] or "Settlement statement.pdf"
            if not name.lower().endswith(".pdf"):
                name += ".pdf"
            files.append((name, "application/pdf", data))
            if len(files) > MAX_STATEMENT_PDFS:
                raise ValueError("This email contains more than 120 different PDFs. Send the remaining statements in another email.")
        elif part.get_content_type() == "text/plain" and len(data) <= 100_000:
            texts.append(data.decode(part.get_content_charset() or "utf-8", errors="replace"))
    if sender == VERIFIER:
        body = "\n".join(texts)
        match = re.search(r"confirmation code\s*:\s*([0-9]{6,12})\b", body, re.I)
        if "gmail forwarding confirmation" in str(message.get("Subject", "")).lower() and address in body and forwarder.lower() in body.lower():
            if match:
                return {"verificationCode": match[1]}, []
            # Gmail also sends link-only confirmations. Never expose arbitrary
            # email links, cancellation links, alternate hosts or redirects.
            links = re.findall(r"https://[^\s<>]+", body)
            for link in links:
                if len(link) <= 1000 and re.fullmatch(r"https://mail\.google\.com/mail/vf-[A-Za-z0-9_%=-]+", link):
                    return {"verificationUrl": link}, []
        raise ValueError("A Gmail setup message arrived, but its confirmation could not be read. Add the forwarding address again in Gmail.")
    if not files:
        raise ValueError("No PDF attachment found. Forward the settlement email with its PDF attached.")
    advice_subject = re.match(r"^(?:(?:re|fwd?):\s*)*Payment Advice Note from\b", str(message.get("Subject", "")), re.I)
    advice_hashes = [hashlib.sha256(data).hexdigest() for name, _, data in files
                     if sender == PAYMENT_SUPPLIER or advice_subject or re.match(r"^Payment Advice Note from\b", name, re.I)]
    return ({"paymentAdviceDigests": advice_hashes} if advice_hashes else {}), files


def save_files(store, route, files):
    refs, duplicates = [], 0
    for file in files:
        digest = hashlib.sha256(file[2]).hexdigest()
        if store.has_original("statement", digest):
            duplicates += 1
            continue
        manifest = evidence.upload(store, route, "statement", [file])
        refs.append({"uploadId": manifest["id"], "fileId": manifest["files"][0]["id"]})
    return refs, duplicates

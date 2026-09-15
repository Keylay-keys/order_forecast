"""Private settlement addresses and durable MIME intake; never posts accounting money."""
from contextlib import contextmanager
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses
import hashlib
import hmac
import os
import re
import secrets
import time
from uuid import uuid4

from psycopg2.extras import Json

from .accounting_domain import fail
from .accounting_store import accounting_transaction
from .dependencies import get_pg_connection, return_pg_connection
from . import accounting_evidence as evidence

MAX_EMAIL = 25 * 1024 * 1024
SUPPLIER = "notification@gruma.com"
VERIFIER = "forwarding-noreply@google.com"


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
        email = body.get("_verifiedEmail", "").lower()
        if not re.fullmatch(r"[^\s@]{1,64}@[^\s@]{1,189}", email) or len(email) > 254:
            fail("Verify your sign-in email before setting up forwarding.", 403)
        cur.execute("""INSERT INTO accounting_email_addresses (address,firebase_uid,route_number,forwarder_email)
            VALUES (%s,%s,%s,%s) ON CONFLICT (firebase_uid,route_number) DO NOTHING""",
            ("s-" + secrets.token_hex(16) + "@" + domain(), *store.scope, email))
    elif method != "GET":
        fail("Email intake endpoint not found.", 404)
    cur.execute("SELECT address,forwarder_email,enabled FROM accounting_email_addresses WHERE firebase_uid=%s AND route_number=%s", store.scope)
    row = cur.fetchone()
    cur.execute("""SELECT o.message_id,o.received_at,j.status,j.result,j.attempts,j.lease_expires_at<now()
        FROM accounting_email_originals o JOIN accounting_email_jobs j USING(message_id)
        WHERE o.firebase_uid=%s AND o.route_number=%s ORDER BY o.received_at DESC LIMIT 30""", store.scope)
    messages = []
    for mid, received, status, result, attempts, expired in cur.fetchall():
        if status == "reading" and attempts >= 3 and expired:
            status, result = "needs_attention", {"detail": "Processing stopped. Upload the PDF to continue."}
        # Verification codes expire from the UI, while the original evidence stays intact.
        result = dict(result)
        if time.time() - received.timestamp() > 3600:
            result.pop("verificationCode", None)
        messages.append(dict(id=str(mid), receivedAt=received.isoformat(), status=status, **result))
    return dict(route=route, available=True, address=row[0] if row and row[2] else None,
                forwarderEmail=row[1] if row else None, messages=messages,
                gmailFilter=f"from:({SUPPLIER}) subject:(settlement {route}) has:attachment filename:pdf")


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
        cur.execute("""SELECT count(*),coalesce(sum(octet_length(content)),0) FROM accounting_email_originals
            WHERE firebase_uid=%s AND route_number=%s AND received_at>now()-interval '1 day'""", store.scope)
        count, size = cur.fetchone()
        if count >= 100 or size + len(raw) > 250 * 1024 * 1024:
            fail("Today's email intake limit was reached. Upload the PDF or try again tomorrow.", 429)
        mid = str(uuid4())
        cur.execute("""INSERT INTO accounting_email_originals (message_id,address,firebase_uid,route_number,sha256,envelope_from,content)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""", (mid, address, uid, route, digest, sender, raw))
        cur.execute("INSERT INTO accounting_email_jobs (message_id) VALUES (%s)", (mid,))
        return {"accepted": True, "duplicate": False, "id": mid}


def parse_message(raw, address, forwarder):
    if not 0 < len(raw) <= MAX_EMAIL:
        raise ValueError("The email exceeds the 25 MB limit. Upload the PDFs separately.")
    message = BytesParser(policy=policy.default).parsebytes(raw)
    if len(message.get_all("From", [])) != 1:
        raise ValueError("The email sender could not be identified.")
    senders = getaddresses(message.get_all("From", []))
    if len(senders) != 1 or senders[0][1].lower() not in (forwarder.lower(), SUPPLIER, VERIFIER):
        raise ValueError("Forward from your sign-in email or use your settlement forwarding rule.")
    # From is a routing restriction, not proof of sender identity. The private,
    # unguessable alias and signed provider relay are the authorization boundary.
    sender = senders[0][1].lower()
    stack, parts, files, texts, hashes = [(message, 0)], 0, [], [], set()
    while stack:
        part, depth = stack.pop()
        parts += 1
        if parts > 80 or depth > 8:
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
            if len(files) > 10:
                raise ValueError("Forward up to 10 PDFs at a time.")
        elif part.get_content_type() == "text/plain" and len(data) <= 100_000:
            texts.append(data.decode(part.get_content_charset() or "utf-8", errors="replace"))
    if sender == VERIFIER:
        body = "\n".join(texts)
        match = re.search(r"confirmation code\s*:\s*([0-9]{6,12})\b", body, re.I)
        if "gmail forwarding confirmation" in str(message.get("Subject", "")).lower() and address in body and forwarder.lower() in body.lower() and match:
            return {"verificationCode": match[1]}, []
        raise ValueError("A Gmail setup message arrived, but its code could not be read. Add the forwarding address again in Gmail.")
    if not files:
        raise ValueError("No PDF attachment found. Forward the settlement email with its PDF attached.")
    return {}, files


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

"""Verify a settlement inbox without changing the Firebase sign-in identity."""
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from email.utils import parseaddr
from urllib import request

from .accounting_domain import fail
from .accounting_store import accounting_transaction

KEY = "settlement-inbox-verification"


def normalize_email(value):
    if not isinstance(value, str):
        fail("Enter the email address where you receive settlements.")
    value = value.strip().lower()
    if len(value) > 254 or not re.fullmatch(r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+", value) or len(value.split('@')[0]) > 64:
        fail("Enter a valid email address.")
    if value.endswith(("@privaterelay.appleid.com", "@private.icloud.com")):
        fail("Enter your actual settlement inbox, rather than your private Apple sign-in address.")
    return value


def available():
    return bool(os.environ.get("MAILJET_API_KEY") and os.environ.get("MAILJET_SECRET_KEY"))


def digest(scope, email, challenge, code):
    key = os.environ.get("ACCOUNTING_EMAIL_SECRET", "")
    if len(key) < 48:
        fail("Email verification is not available yet.", 503)
    return hmac.new(key.encode(), "\0".join((*scope, email, challenge, code)).encode(), hashlib.sha256).hexdigest()


def pending(store):
    state = store.get(KEY) or {}
    if not state.get("challenge") or state.get("expires", 0) <= time.time() or state.get("attempts", 0) >= 5:
        return None
    return {"email": state["email"], "challengeId": state["challenge"], "expiresAt": state["expires"], "delivery": state["delivery"]}


def bind(store, email):
    from .accounting_email import domain
    store.cursor.execute("""INSERT INTO accounting_email_addresses (address,firebase_uid,route_number,forwarder_email)
        VALUES (%s,%s,%s,%s) ON CONFLICT (firebase_uid,route_number)
        DO UPDATE SET forwarder_email=EXCLUDED.forwarder_email""",
        ("s-" + secrets.token_hex(16) + "@" + domain(), *store.scope, email))


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def send_code(email, code):
    """Fixed provider endpoint; no credentials, recipient or response body in logs."""
    _, sender = parseaddr(os.environ.get("MAILJET_FROM", "RouteSpark <support@routespark.pro>"))
    payload = {"Messages": [{"From": {"Email": sender, "Name": "RouteSpark"},
        "To": [{"Email": email}], "Subject": "Verify your settlement email for RouteSpark",
        "TextPart": f"Your RouteSpark code is {code}.\n\nEnter it in Email settlements within 10 minutes. This connects your settlement inbox; it does not change your sign-in or give RouteSpark mailbox access.\n\nIf you did not request this code, ignore this email."}]}
    auth = base64.b64encode((os.environ["MAILJET_API_KEY"] + ":" + os.environ["MAILJET_SECRET_KEY"]).encode()).decode()
    req = request.Request("https://api.mailjet.com/v3.1/send", data=json.dumps(payload).encode(),
                          headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"}, method="POST")
    try:
        with request.build_opener(NoRedirect()).open(req, timeout=10) as response:
            data = json.loads(response.read(65536))
            return response.status == 200 and len(data.get("Messages", [])) == 1 and data["Messages"][0].get("Status") == "success"
    except Exception:
        return False


def configure(uid, route, action, body, request_id, fingerprint):
    from .accounting_email import available as intake_available
    if not intake_available():
        fail("Settlement email receiving is not available yet.", 503)
    now = time.time()
    code = None
    with accounting_transaction(uid, route) as store:
        prior = store.get("request:" + request_id)
        if prior:
            if prior["fingerprint"] != fingerprint:
                fail("A request identifier was reused for different work.", 409)
            return prior["response"]
        state = store.get(KEY) or {}
        if action == "sender-code":
            email = normalize_email(body.get("email"))
            if email == body.get("_verifiedEmail", "").lower():
                bind(store, email)
                store.put(KEY, {**state, "challenge": None, "email": email, "verifiedAt": now})
                result = {"verified": True}
            else:
                if not available():
                    fail("Inbox verification is temporarily unavailable. You can still upload your settlement PDF.", 503)
                sends = [at for at in state.get("sends", []) if now - at < 86400]
                if (sends and now - sends[-1] < 60) or len(sends) >= 10:
                    fail("Please wait before requesting another code. Up to 10 codes can be sent per day.", 429)
                code = f"{secrets.randbelow(100_000_000):08d}"
                challenge = secrets.token_hex(16)
                state = {"email": email, "challenge": challenge, "hash": digest(store.scope, email, challenge, code),
                         "expires": now + 600, "attempts": 0, "sends": [*sends, now], "delivery": "sending"}
                store.put(KEY, state)
                result = {"verified": False, "pendingInbox": pending(store)}
        elif action == "sender-verify":
            supplied = body.get("code")
            if not pending(store) or body.get("challengeId") != state.get("challenge"):
                result = {"verified": False, "verificationError": "This code has expired or was replaced. Request a new code."}
            else:
                state["attempts"] += 1
                valid = isinstance(supplied, str) and bool(re.fullmatch(r"[0-9]{8}", supplied))
                if valid and hmac.compare_digest(state["hash"], digest(store.scope, state["email"], state["challenge"], supplied)):
                    bind(store, state["email"])
                    state["challenge"] = None
                    state["verifiedAt"] = now
                    result = {"verified": True}
                else:
                    result = {"verified": False, "verificationError": "That code did not match. Check the latest email." if state["attempts"] < 5 else "Too many attempts. Request a new code."}
                # Failed attempts must commit, not roll back through an HTTP error.
                store.put(KEY, state)
        else:
            fail("Email intake endpoint not found.", 404)
        store.put("request:" + request_id, {"fingerprint": fingerprint, "response": result})
    if code is not None:
        # Reserve rate allowance and hash before sending. No network call while
        # holding the workspace lock, and retries cannot resend the same request.
        sent = send_code(email, code)
        with accounting_transaction(uid, route) as store:
            current = store.get(KEY) or {}
            if current.get("challenge") == challenge:
                current["delivery"] = "sent" if sent else "failed"
                store.put(KEY, current)
            result = {"verified": False, "pendingInbox": pending(store)}
            if not sent:
                result["verificationError"] = "We could not confirm code delivery. Check your inbox, or request a new code in a minute."
            store.put("request:" + request_id, {"fingerprint": fingerprint, "response": result})
    return result

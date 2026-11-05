"""All hosted Accounting workflows, behind the existing owner/pilot guard."""
import hashlib
import asyncio
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile

from ..business_access import require_business_access
from ..dependencies import get_firestore, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history
from ..accounting_domain import Accounting, fail, identifier, now
from ..accounting_store import accounting_transaction
from ..accounting_pdf_capacity import request_capacity
from .. import accounting_evidence as evidence
from .. import accounting_email as email_intake
from .. import accounting_email_sender as email_sender

router = APIRouter()
logger = logging.getLogger("api.accounting")
HEADERS = {"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"}
UPLOAD_SLOTS = asyncio.Semaphore(1)


def upload_hashes(files):
    return [[name, media, files.digest(content) if isinstance(files, evidence.UploadFiles)
             else hashlib.sha256(content).hexdigest()] for name, media, content in files]


async def upload_capacity(request: Request):
    if request.method != "POST" or not request.url.path.endswith(("/receipts", "/statements")):
        yield
        return
    if UPLOAD_SLOTS.locked():
        raise HTTPException(429, "Another upload is being saved. Retry shortly.", headers={"Retry-After": "5"})
    await UPLOAD_SLOTS.acquire()
    try:
        yield
    finally:
        UPLOAD_SLOTS.release()


async def bounded_body(request, limit):
    chunks, total = [], 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            fail("Accounting request is too large.", 413)
        chunks.append(chunk)
    return b"".join(chunks)


def dispatch(store, route, today, method, path, body, files, refresh):
    domain, _, suffix = path.partition("/")
    accounting = Accounting(store, route, today)
    if domain == "email-intake" and not suffix:
        return email_intake.setup(store, route, method, body)
    if domain == "email-intake" and suffix == "handle" and method == "POST":
        return email_intake.handle_arrival(store, body)
    if domain == "expenses":
        return accounting.expenses(method, suffix, body)
    if domain == "bills":
        return accounting.bills(method, suffix, body)
    if domain == "payees":
        return accounting.payees(method, suffix, body)
    if domain == "money-in":
        if method == "POST" and suffix == "sync-statements":
            if body:
                fail("Statement updates take no client-supplied facts.")
            from ..accounting_payment_advice import reconcile
            return reconcile(accounting, evidence.sync_statement_updates(accounting))
        if method == "POST" and suffix == "confirm-statement":
            from ..accounting_payment_advice import reconcile
            return reconcile(accounting, evidence.confirm_statement(accounting, body))
        return accounting.remittances(method, suffix, body)
    if domain == "cash-accounts":
        return accounting.cash(method, suffix, body)
    if domain == "route-stores" and not suffix:
        return accounting.route_stores(method, body, refresh)
    if domain in ("receipts", "statements") and method == "POST" and not suffix:
        return evidence.upload(store, route, "receipt" if domain == "receipts" else "statement", files)
    if domain == "statement-reviews":
        if method == "GET" and not suffix:
            return evidence.statements(store, route)
        parts = suffix.split("/")
        if len(parts) == 3:
            upload, file, action = parts
            if method == "GET" and action == "original":
                return evidence.original(store, "statement", upload, file)
            if method == "POST" and action == "analyze":
                return evidence.analyze_statement(store, route, upload, file)
    if domain == "receipt-reviews":
        if method == "GET" and not suffix:
            return evidence.receipt_inbox(store, route)
        parts = suffix.split("/")
        if len(parts) == 3 and parts[1] == "files" and method == "GET":
            return evidence.original(store, "receipt", parts[0], parts[2])
        if len(parts) == 1 and method == "PATCH":
            return evidence.change_receipt_review(accounting, suffix, body)
        if len(parts) == 2 and parts[1] == "manifest" and method == "GET":
            identifier(parts[0])
            manifest = store.get("receipt-upload:" + parts[0])
            if manifest:
                return manifest
        if len(parts) == 2 and parts[1] == "ocr" and method == "POST":
            return evidence.accept_receipt_attempt(store, route, parts[0], body)
        if len(parts) == 2 and parts[1] == "read" and method == "POST":
            identifier(parts[0])
            if store.get("receipt-upload:" + parts[0]) is None:
                fail("Receipt packet not found.", 404)
            store.queue_receipt(parts[0], retry=True)
            return {"queued": True, "route": route}
    fail("Accounting endpoint not found.", 404)


def execute(uid, route, today, method, path, body, files, refresh, request_id, fingerprint):
    parts = path.split("/")
    if method == "POST" and len(parts) == 4 and parts[0] == "statement-reviews" and parts[3] == "analyze":
        identifier(parts[1])
        identifier(parts[2])
        started = now()
        # Bound requests and serialize only duplicate originals through commit.
        # Native reader slots belong to parsing, not database lock waits.
        with request_capacity(route, parts[1] + ':' + parts[2]):
            return _execute(uid, route, today, method, path, body, files, refresh, request_id, fingerprint, started)
    return _execute(uid, route, today, method, path, body, files, refresh, request_id, fingerprint)


def _execute(uid, route, today, method, path, body, files, refresh, request_id, fingerprint, analysis_started=None):
    if method == "POST" and path in ("email-intake/sender-code", "email-intake/sender-verify"):
        return email_sender.configure(uid, route, path.split("/")[1], body, request_id, fingerprint)
    # PDF parsing can take seconds. Release the workspace lock while the bounded
    # reader runs so receipts, bills and other tabs can continue to save.
    prepared_analysis = None
    parts = path.split("/")
    is_analysis = method == "POST" and len(parts) == 4 and parts[0] == "statement-reviews" and parts[3] == "analyze"
    if is_analysis:
        with accounting_transaction(uid, route) as store:
            saved = store.get("request:" + request_id)
            if saved:
                if saved["fingerprint"] != fingerprint:
                    fail("A request identifier was reused for different work.", 409)
                return saved["response"]
            source = evidence.original(store, "statement", parts[1], parts[2])
            prior = evidence.latest_statement(store, parts[1], parts[2], verified_source=source)
            if prior and analysis_started and prior["analyzedAt"] >= analysis_started:
                store.put("request:" + request_id, {"fingerprint": fingerprint, "response": prior})
                return prior
        try:
            prepared_analysis = evidence.analyze_bytes(source["content"], route)
        except (OSError, ValueError, evidence.subprocess.SubprocessError):
            pass
    with accounting_transaction(uid, route) as store:
        if request_id:
            saved = store.get("request:" + request_id)
            if saved:
                if saved["fingerprint"] != fingerprint:
                    fail("A request identifier was reused for different work.", 409)
                return saved["response"]
        if is_analysis:
            result = evidence.analyze_statement(store, route, parts[1], parts[2], reader=lambda *_: prepared_analysis)
        else:
            result = dispatch(store, route, today, method, path, body, files, refresh)
        if request_id:
            store.put("request:" + request_id, {"fingerprint": fingerprint, "response": result})
        return result


@router.api_route("/business/accounting/{path:path}", methods=["GET", "POST", "PATCH", "DELETE"], dependencies=[Depends(upload_capacity)])
@rate_limit_history
async def accounting_request(
    request: Request,
    path: str,
    route: str = Query(..., pattern=r"^[0-9]{6}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db=Depends(get_firestore),
):
    await require_business_access(route, decoded_token, db)
    if len(path) > 180 or path.endswith("/") or ".." in path:
        fail("Accounting endpoint not found.", 404)
    try:
        zone = ZoneInfo(request.headers.get("X-Accounting-Timezone", "UTC"))
    except (ZoneInfoNotFoundError, ValueError):
        fail("Invalid accounting timezone.")
    today = datetime.now(zone).date().isoformat()
    method = request.method
    body, files, fingerprint, request_id = {}, evidence.UploadFiles(), "", None
    if method != "GET":
        request_id = identifier(request.headers.get("Idempotency-Key"))
        upload_kind = path in ("receipts", "statements") and method == "POST"
        if upload_kind:
            if not request.headers.get("content-type", "").lower().startswith("multipart/form-data;"):
                fail("Multipart files are required.", 415)
            limit = (26 if path == "receipts" else 201) * 1024 * 1024
            # Starlette spools individual files. Count the ASGI stream as well as
            # per-file sizes, so missing/forged Content-Length cannot bypass limits.
            receive = request._receive
            total = 0
            async def bounded_receive():
                nonlocal total
                message = await receive()
                total += len(message.get("body", b""))
                if total > limit:
                    fail("The upload exceeds the batch limit.", 413)
                return message
            request._receive = bounded_receive
            async with request.form(max_files=5 if path == "receipts" else 120, max_fields=0) as form:
                for name, file in form.multi_items():
                    if name != "files" or not isinstance(file, UploadFile):
                        fail("Only files are accepted.")
                    content = await file.read((16 * 1024 * 1024) + 1)
                    if len(content) > 15 * 1024 * 1024:
                        fail("The file exceeds the size limit.", 413)
                    files.append((file.filename or "", file.content_type or "", content))
            # Keep small uploads direct; a catch-up batch must not block screen reads.
            hash_input = (await run_in_threadpool(upload_hashes, files)
                          if sum(len(content) for _, _, content in files) > 1024 * 1024
                          else upload_hashes(files))
        else:
            raw = await bounded_body(request, 5 * 1024 * 1024 if path.endswith("/ocr") else 8192)
            if raw and not request.headers.get("content-type", "").lower().startswith("application/json"):
                fail("JSON is required.", 415)
            try:
                def invalid_constant(_):
                    raise ValueError("Non-finite JSON")
                body = json.loads(raw, parse_constant=invalid_constant) if raw else {}
            except (ValueError, UnicodeError):
                fail("Invalid JSON request.")
            if type(body) is not dict:
                fail("An accounting request must be an object.")
            hash_input = body
        if path == "email-intake":
            if body:
                fail("Email setup takes no client-supplied identity fields.")
            if not decoded_token.get("email_verified"):
                fail("Verify your sign-in email before setting up forwarding.", 403)
            body = {"_verifiedEmail": decoded_token.get("email", "")}
            hash_input = body
        elif path in ("email-intake/sender-code", "email-intake/sender-verify"):
            allowed = {"email"} if path.endswith("sender-code") else {"code", "challengeId"}
            if method != "POST" or set(body) != allowed:
                fail("Invalid inbox verification request.")
            body = {**body, "_verifiedEmail": decoded_token.get("email", "") if decoded_token.get("email_verified") else ""}
            hash_input = body
        fingerprint = hashlib.sha256(json.dumps([method, path, hash_input], sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    if method == "GET" and path == "email-intake":
        body = {"_loginEmail": decoded_token.get("email", "")}

    def refresh():
        snapshot = db.collection("routes").document(route).collection("stores").select(["name", "number"]).limit(1001).stream()
        stores = []
        for doc in snapshot:
            value = doc.to_dict()
            stores.append(dict(id=doc.id, name=value.get("name"), **({"number": value["number"]} if value.get("number") else {})))
        return stores

    try:
        result = await run_in_threadpool(execute, decoded_token["uid"], route, today, method, path, body, files, refresh, request_id, fingerprint)
    except HTTPException:
        raise
    except Exception:
        logger.warning("Accounting request could not complete", extra={"accounting_domain": path.split("/")[0]})
        raise HTTPException(503, "Accounting could not be loaded or saved. Reload to verify the latest records before trying again.") from None
    if isinstance(result, dict) and isinstance(result.get("content"), bytes):
        extension = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}[result["mediaType"]]
        return Response(result["content"], media_type=result["mediaType"], headers={**HEADERS,
            "Content-Disposition": f'inline; filename="original.{extension}"', "Content-Security-Policy": "sandbox; default-src 'none'"})
    if path.split("/", 1)[0] == "money-in":
        from ..accounting_payment_advice import public_payment_notes
        result = public_payment_notes(result)
    return Response(json.dumps(result, allow_nan=False), media_type="application/json", headers=HEADERS,
                    status_code=201 if method == "POST" and path in ("receipts", "statements", "bills") else 200)

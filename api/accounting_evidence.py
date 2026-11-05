"""Immutable receipt/statement intake and explicit review commands."""
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4
from fastapi import HTTPException

from .accounting_domain import CATEGORIES, PAYMENT_METHODS, amount, day, evidence_identifier, fail, fields, identifier, now, text
from .accounting_performance import gross_sales_cents
from .accounting_statement_timestamp import statement_issued_at
from .accounting_pdf_capacity import capacity as pdf_capacity
from .accounting_pdf_process import run_bounded


class UploadFiles(list):
    """Reuse server-computed hashes only for the exact immutable byte object.

    No digest is accepted from request fields. Holding the byte reference also
    prevents Python object-id reuse from matching a different file.
    """
    def __init__(self, files=()):
        super().__init__(files)
        self._digests = {}

    def digest(self, content):
        cached = self._digests.get(id(content))
        if cached is not None and cached[0] is content:
            return cached[1]
        digest = hashlib.sha256(content).hexdigest()
        if isinstance(content, bytes):
            self._digests[id(content)] = (content, digest)
        return digest


def detect_media(content):
    if content.startswith(b"%PDF-") and b"%%EOF" in content[-1024:]:
        return "application/pdf", "pdf"
    if content.startswith(b"\x89PNG\r\n\x1a\n") and b"IEND" in content[-32:]:
        return "image/png", "png"
    # JPEG frame + scan markers and bounded Samsung motion-photo trailer support.
    if not content.startswith(b"\xff\xd8"):
        return None
    offset, frame, scan = 2, False, False
    while offset < len(content):
        if content[offset] != 255:
            return None
        while offset < len(content) and content[offset] == 255:
            offset += 1
        if offset >= len(content):
            return None
        marker = content[offset]
        offset += 1
        if marker == 217:
            trailer = len(content) - offset
            return ("image/jpeg", "jpg") if frame and scan and (trailer == 0 or trailer <= 256 * 1024 and content.endswith(b"SEFT")) else None
        if marker in (216, 0) or offset + 2 > len(content):
            return None
        if marker == 1 or 208 <= marker <= 215:
            continue
        size = int.from_bytes(content[offset:offset + 2], "big")
        if size < 2 or offset + size > len(content):
            return None
        frame = frame or 192 <= marker <= 207 and marker not in (196, 200, 204)
        offset += size
        if marker == 218:
            scan = True
            while offset < len(content):
                if content[offset] != 255:
                    offset += 1
                    continue
                start = offset
                while offset < len(content) and content[offset] == 255:
                    offset += 1
                if offset >= len(content):
                    return None
                if content[offset] == 0 or 208 <= content[offset] <= 215:
                    offset += 1
                    continue
                offset = start
                break
    return None


def upload(store, route, kind, files):
    if not 1 <= len(files) <= (5 if kind == "receipt" else 120):
        fail("Choose a supported number of files.")
    prepared, digests, total, duplicates = [], set(), 0, 0
    for name, declared, content in files:
        name = text(name, 160 if kind == "receipt" else 200, True)
        if "/" in name or "\\" in name:
            fail("Invalid upload filename.")
        total += len(content)
        file_limit = (12 if kind == "receipt" else 15) * 1024 * 1024
        batch_limit = (25 if kind == "receipt" else 200) * 1024 * 1024
        if not 0 < len(content) <= file_limit or total > batch_limit:
            fail("The upload exceeds the file or batch size limit.", 413)
        detected = detect_media(content)
        if detected is None or kind == "statement" and detected[0] != "application/pdf":
            fail("Upload a valid supported image or PDF.", 415)
        media, extension = detected
        if Path(name).suffix.lower() not in ((".jpg", ".jpeg") if extension == "jpg" else ("." + extension,)) or kind == "receipt" and declared != media:
            fail("The filename, contents and file type must agree.", 415)
        digest = files.digest(content) if isinstance(files, UploadFiles) else hashlib.sha256(content).hexdigest()
        if digest in digests or store.has_original(kind, digest):
            if kind == "statement":
                duplicates += 1
                continue
            fail("This exact file has already been uploaded.", 409)
        digests.add(digest)
        item = dict(id=str(uuid4()), originalName=name, bytes=len(content), sha256=digest)
        if kind == "receipt":
            item.update(mediaType=media, storedName=item["id"] + "." + extension)
        prepared.append((item, content))
    upload_id = str(uuid4())
    manifest = dict(schemaVersion=1, id=upload_id, route=route, files=[item for item, _ in prepared])
    if kind == "receipt":
        manifest.update(source="computer_upload", status="received", createdAt=now())
    else:
        manifest["uploadedAt"] = now()
        manifest["duplicateCount"] = duplicates
        if not prepared:
            # Successful no-op: do not create an empty stored upload manifest.
            return manifest
    for item, content in prepared:
        store.add_original(kind, upload_id, item, content)
    store.put(f"{kind}-upload:{upload_id}", manifest)
    if kind == "receipt":
        store.queue_receipt(upload_id)
    return manifest


def original(store, kind, upload_id, file_id):
    identifier(upload_id)
    evidence_identifier(file_id)
    manifest = store.get(f"{kind}-upload:{upload_id}")
    file = next((f for f in (manifest or {}).get("files", []) if f["id"] == file_id), None)
    source = store.original(kind, upload_id, file_id) if file else None
    if source is None:
        fail("Original not found.", 404)
    if source["sha256"] != file["sha256"] or len(source["content"]) != file["bytes"]:
        fail("Original evidence could not be verified.", 503)
    return source


def latest_statement(store, upload_id, file_id, *, verified_source=None):
    pointer = store.get(f"statement-latest:{upload_id}:{file_id}")
    if pointer is None:
        return None
    result = store.attempt("statement", upload_id, file_id, pointer["attemptId"])
    source = verified_source if verified_source is not None else original(store, "statement", upload_id, file_id)
    if result is None or result["sha256"] != source["sha256"]:
        fail("Statement reading could not be verified.", 503)
    public = {key: value for key, value in result.items() if key != "_evidence"}
    if public.get("analysis") is not None:
        public["analysis"] = dict(public["analysis"], grossSalesCents=gross_sales_cents(result), printedIssuedAt=statement_issued_at(result))
    return public


def statements(store, route):
    items = []
    for manifest in store.documents("statement-upload:"):
        for file in manifest["files"]:
            items.append(dict(uploadId=manifest["id"], route=route, uploadedAt=manifest["uploadedAt"], file=file, attempt=latest_statement(store, manifest["id"], file["id"])))
    return sorted(items, key=lambda item: item["uploadedAt"], reverse=True)


def analyze_bytes(content, route):
    # Only this bounded reader uses scratch files. PostgreSQL retains the original.
    with pdf_capacity(route), tempfile.TemporaryDirectory(prefix="accounting-pdf-") as directory:
        source = Path(directory) / "source.pdf"
        source.write_bytes(content)
        result = run_bounded(["/usr/local/bin/routespark-pdf-sandbox", "analyze", str(source), route, str(os.getpid())],
                             stdout_limit=6 * 1024 * 1024, timeout=45, new_session=True)
        return json.loads(result)


def valid_analysis(value, route):
    try:
        if type(value) is not dict or value.get("schemaVersion") != 1 or value.get("route") != route:
            return False
        if day(value["periodStart"]) >= day(value["periodEnd"]) or type(value["pageCount"]) is not int or not 1 <= value["pageCount"] <= 100:
            return False
        for name in ("marginCents", "newSettlementCents", "hhcRentalFeeCents"):
            amount(value[name], signed=True)
        for number in [value[n] for n in ("inventoryCents", "totalAdjustmentsCents", "cashSalesCents")] + [value["account"][n] for n in ("previousBalance", "payment", "totalNewSettlement", "amountToPay")]:
            if number is not None:
                amount(number, signed=True)
        if not isinstance(value["adjustments"], list) or len(value["adjustments"]) > 10 or not isinstance(value["checks"], list) or len(value["checks"]) != 8:
            return False
        for row in value["adjustments"]:
            text(row["key"], 100, True)
            text(row["label"], 100, True)
            amount(row["cents"], signed=True)
        for check in value["checks"]:
            text(check["label"], 100, True)
            for name in ("calculatedCents", "printedCents"):
                if check[name] is not None:
                    amount(check[name], signed=True)
        if value["cashState"] not in ("present", "none", "unknown") or not isinstance(value["issues"], list):
            return False
        for issue in value["issues"]:
            text(issue, 300)
        return True
    except (KeyError, TypeError, ValueError):
        return False
    except Exception as error:
        from fastapi import HTTPException
        if isinstance(error, HTTPException):
            return False
        raise


def analyze_statement(store, route, upload_id, file_id, reader=analyze_bytes):
    source = original(store, "statement", upload_id, file_id)
    analysis = None
    source_evidence = None
    try:
        value = reader(source["content"], route)
        if valid_analysis(value, route):
            source_evidence = value.pop("_evidence", None)
            analysis = value
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    attempt = dict(id=str(uuid4()), uploadId=upload_id, fileId=file_id, route=route, sha256=source["sha256"], analyzedAt=now(), analysis=analysis,
                   error="" if analysis else "This PDF could not be fully read and reconciled for this route. Check the original; nothing was recorded.")
    store.add_attempt("statement", upload_id, file_id, attempt["id"], dict(attempt, _evidence=source_evidence))
    store.put(f"statement-latest:{upload_id}:{file_id}", {"attemptId": attempt["id"]})
    return attempt


def sync_statement_updates(accounting, period_end=None):
    """Advance an already-reviewed period to its uniquely newer, verified issue."""
    ledger = accounting.load("money-in")
    periods = [p for p in ledger["settlements"] if period_end is None or p["periodEnd"] == period_end]
    if not periods:
        return ledger
    # Inspect saved reading metadata first; only retrieve and hash PDFs for periods
    # being updated. Bulk first-time confirmation need not reread the whole year.
    ends = {p["periodEnd"] for p in periods}
    items = []
    for manifest in accounting.store.documents("statement-upload:"):
        for file in manifest["files"]:
            pointer = accounting.store.get(f"statement-latest:{manifest['id']}:{file['id']}")
            saved = accounting.store.attempt("statement", manifest["id"], file["id"], pointer["attemptId"]) if pointer else None
            if ((saved or {}).get("analysis") or {}).get("periodEnd") in ends:
                items.append({"attempt": latest_statement(accounting.store, manifest["id"], file["id"])})
    for existing in periods:
        review = existing.get("statementReview") or {}
        if not all(review.get(k) for k in ("uploadId", "fileId", "attemptId")):
            continue
        saved = accounting.store.attempt("statement", review["uploadId"], review["fileId"], review["attemptId"])
        old_analysis = (saved or {}).get("analysis") or {}
        old_issue = statement_issued_at(saved or {})
        if not old_issue or old_analysis.get("route") != accounting.route or any(old_analysis.get(k) != existing[k] for k in ("periodStart", "periodEnd")):
            continue
        source = original(accounting.store, "statement", review["uploadId"], review["fileId"])
        if saved["sha256"] != source["sha256"] or existing["source"] != "sha256:" + source["sha256"]:
            continue
        group = [item for item in items if item.get("attempt") and item["attempt"].get("analysis")
                 and all(item["attempt"]["analysis"].get(k) == existing[k] for k in ("periodStart", "periodEnd"))]
        if not group or any(not item["attempt"]["analysis"].get("printedIssuedAt") for item in group):
            continue
        group.sort(key=lambda item: item["attempt"]["analysis"]["printedIssuedAt"], reverse=True)
        latest = group[0]["attempt"]
        issued = latest["analysis"]["printedIssuedAt"]
        if issued <= old_issue or (len(group) > 1 and group[1]["attempt"]["analysis"]["printedIssuedAt"] == issued):
            continue
        try:
            ledger = confirm_statement(accounting, dict(revision=ledger["revision"], uploadId=latest["uploadId"],
                fileId=latest["fileId"], attemptId=latest["id"], version="final", replaceSource=existing["source"]),
                automatic_update=dict(updatedAt=now(), previousIssuedAt=old_issue, issuedAt=issued,
                    previousAmountToPayCents=existing.get("amountToPayCents", existing["newSettlementCents"])))
        except HTTPException as error:
            if error.status_code != 422:
                raise
            # An unclear or unreconciled new issue remains in the review queue.
    return ledger


def confirm_statement(accounting, body, *, automatic_update=None):
    fields(body, ("revision", "uploadId", "fileId", "attemptId", "version", "replaceSource"), ("revision", "uploadId", "fileId", "attemptId", "version"))
    for name in ("uploadId", "fileId", "attemptId"):
        identifier(body[name])
    if body["version"] not in ("preliminary", "final"):
        fail()
    attempt = latest_statement(accounting.store, body["uploadId"], body["fileId"])
    if attempt is None or attempt["id"] != body["attemptId"]:
        fail("A newer reading exists. Reload before confirming.", 409)
    a = attempt["analysis"]
    if not a or not valid_analysis(a, accounting.route) or a["issues"] or a["cashState"] == "unknown" or any(v is None for v in (a["inventoryCents"], a["totalAdjustmentsCents"], a["cashSalesCents"], *a["account"].values())):
        fail("The statement must reconcile exactly before confirmation.", 422)
    adjustments = {item["key"]: item["cents"] for item in a["adjustments"]}
    account = a["account"]
    checks = [all(c["calculatedCents"] is not None and c["calculatedCents"] == c["printedCents"] for c in a["checks"]),
              a["marginCents"] + a["inventoryCents"] + a["totalAdjustmentsCents"] == a["newSettlementCents"],
              sum(item["cents"] for item in a["adjustments"]) == a["totalAdjustmentsCents"],
              len(adjustments) == len(a["adjustments"]),
              a["cashSalesCents"] == -adjustments.get("netCashSales", 0),
              a["hhcRentalFeeCents"] == -adjustments.get("hhcRentalFee", 0),
              (a["cashState"] == "present") == (a["cashSalesCents"] != 0),
              account["previousBalance"] + account["payment"] + a["newSettlementCents"] == account["totalNewSettlement"],
              account["totalNewSettlement"] == account["amountToPay"]]
    if not all(checks):
        fail("The statement must reconcile exactly before confirmation.", 422)
    ledger = accounting.load("money-in")
    accounting.revision(ledger, body)
    existing = next((p for p in ledger["settlements"] if p["periodEnd"] == a["periodEnd"]), None)
    source = "sha256:" + attempt["sha256"]
    if existing:
        if existing["source"] == source and existing["revision"] == body["version"]:
            return ledger
        if (existing["revision"] != "preliminary" and automatic_update is None) or body["version"] != "final" or existing["periodStart"] != a["periodStart"] or body.get("replaceSource") != existing["source"]:
            fail("Confirm replacement of the exact preliminary statement with its final version.", 409)
    elif len(ledger["settlements"]) >= 520:
        fail("Statement record limit reached.", 409)
    period = {k: a[k] for k in ("periodStart", "periodEnd", "newSettlementCents", "hhcRentalFeeCents", "cashSalesCents")}
    period.update(amountToPayCents=account["amountToPay"], previousBalanceCents=account["previousBalance"], statementPaymentCents=account["payment"], source=source, revision=body["version"],
                  statementReview=dict(uploadId=attempt["uploadId"], fileId=attempt["fileId"], attemptId=attempt["id"], confirmedAt=now()))
    if automatic_update is not None:
        period["automaticUpdate"] = automatic_update
    if existing:
        ledger["settlements"][ledger["settlements"].index(existing)] = period
    else:
        ledger["settlements"].append(period)
    result = accounting.save("money-in", ledger)
    return result if automatic_update is not None else sync_statement_updates(accounting, a["periodEnd"])


def receipt_review(store, route, manifest):
    return store.get("receipt-review:" + manifest["id"]) or dict(schemaVersion=1, route=route, packetId=manifest["id"], revision=0,
        groupedFileIds=[f["id"] for f in manifest["files"]], includeInAccountantHandoff=False, status="reviewing")


def receipt_inbox(store, route):
    items = []
    jobs = store.receipt_jobs()
    agent_jobs = store.receipt_agent_reviews()
    for manifest in store.documents("receipt-upload:"):
        pointer = store.get("receipt-latest:" + manifest["id"])
        report = store.attempt("receipt", manifest["id"], manifest["id"], pointer["attemptId"]) if pointer else None
        if pointer and not report:
            fail("Receipt reading could not be verified.", 503)
        agent = None
        if report:
            key = (manifest["id"], report["attemptId"])
            agent = agent_jobs.get(key)
            if agent is None:
                # One-time backfill for accepted OCR attempts created before
                # the agent-review stage was introduced.
                store.queue_receipt_agent(*key)
                agent = {"status": "queued", "review": None}
        agent_report = (agent or {}).get("review")
        file_reviews = {item["fileId"]: item for item in (agent_report or {}).get("fileReviews", [])}
        review_files = []
        for file in manifest["files"]:
            document = next((d for d in report["documents"] if d["fileId"] == file["id"] and d["sha256"] == file["sha256"]), None) if report else None
            extraction = (document or {}).get("extraction", {})
            agent_file = file_reviews.get(file["id"])
            review_files.append(dict(id=file["id"], originalName=file["originalName"], mediaType=file["mediaType"], bytes=file["bytes"],
                documentType=(agent_file or {}).get("documentType", extraction.get("documentType", "unknown")),
                ocrStatus=(document or {}).get("status", "unsupported"), fields=extraction.get("fields", {})))
        item = dict(packetId=manifest["id"], createdAt=manifest["createdAt"], ocrStatus=report["status"] if report else "not_run",
                    files=review_files, proposal=(agent_report or {}).get("proposal"), review=receipt_review(store, route, manifest),
                    processingStatus=(("agent_" + agent["status"]) if agent and agent["status"] != "complete"
                                      else "complete" if agent_report else jobs.get(manifest["id"], "needs_attention")))
        if report:
            item.update(ocrAttemptId=report["attemptId"], ocrCreatedAt=report["createdAt"], pipelineVersion=report["pipelineVersion"])
        if agent_report:
            item.update(agentReviewId=agent_report["reviewId"], agentReviewedAt=agent_report["createdAt"],
                        agentConfidence=agent_report["confidence"], agentConfidenceTier=agent_report["confidenceTier"])
        items.append(item)
    return dict(schemaVersion=1, route=route, items=sorted(items, key=lambda i: i["createdAt"], reverse=True))


def change_receipt_review(accounting, packet_id, body):
    identifier(packet_id)
    manifest = accounting.store.get("receipt-upload:" + packet_id)
    if manifest is None:
        fail("Receipt packet not found.", 404)
    action = body.get("action")
    allowed = ("revision", "action", "groupedFileIds", "includeInAccountantHandoff") if action == "save" else ("revision", "action") if action == "unlink" else ("revision", "action", "expenseId")
    fields(body, allowed, ("revision", "action"))
    review = receipt_review(accounting.store, accounting.route, manifest)
    accounting.revision(review, body)
    if action == "save":
        grouped = body.get("groupedFileIds")
        if type(grouped) is not list or not grouped or any(not isinstance(i, str) for i in grouped) or len(set(grouped)) != len(grouped) or set(grouped) - {f["id"] for f in manifest["files"]} or type(body.get("includeInAccountantHandoff")) is not bool:
            fail("Choose the supporting images for this purchase.")
        review.update(groupedFileIds=grouped, includeInAccountantHandoff=body["includeInAccountantHandoff"])
    elif action == "record":
        if review["status"] == "recorded":
            fail("This receipt group is already linked to an expense.", 409)
        accounting.find(accounting.load("expenses")["items"], body.get("expenseId"))
        review.update(status="recorded", linkedExpenseId=body["expenseId"])
    elif action == "unlink" and review["status"] == "recorded":
        review["status"] = "reviewing"
        review.pop("linkedExpenseId", None)
    else:
        fail("Invalid receipt review change.")
    review.update(revision=review["revision"] + 1, updatedAt=now())
    accounting.store.put("receipt-review:" + packet_id, review)
    return receipt_inbox(accounting.store, accounting.route)


def accept_receipt_attempt(store, route, packet_id, report):
    """Worker candidates are untrusted evidence, never a payment authorization."""
    identifier(packet_id)
    if type(report) is not dict or report.get("schemaVersion") != 1 or report.get("route") != route or report.get("packetId") != packet_id:
        fail("OCR result does not match this receipt packet.")
    identifier(report.get("attemptId"))
    text(report.get("pipelineVersion"), 80, True)
    text(report.get("createdAt"), 80, True)
    from datetime import datetime
    try:
        datetime.fromisoformat(report["createdAt"].replace("Z", "+00:00"))
    except ValueError:
        fail("Invalid OCR timestamp.")
    if report.get("status") not in ("ocr_complete", "needs_attention"):
        fail("Invalid OCR result status.")
    manifest = store.get("receipt-upload:" + packet_id)
    if manifest is None:
        fail("Receipt packet not found.", 404)
    documents = report.get("documents")
    if type(documents) is not list or len(documents) != len(manifest["files"]):
        fail("OCR must identify every source image.")
    seen = set()
    for document in documents:
        if not isinstance(document, dict):
            fail()
        fid = evidence_identifier(document.get("fileId"))
        source = original(store, "receipt", packet_id, fid)
        if fid in seen or source["sha256"] != document.get("sha256") or document.get("status") not in ("ocr_complete", "no_text", "failed", "timeout", "unsupported"):
            fail("OCR evidence identity or status mismatch.")
        seen.add(fid)
        extraction = document.get("extraction", {})
        if type(extraction) is not dict or extraction.get("documentType", "unknown") not in ("invoice", "payment_receipt", "combined", "credit_or_refund", "unknown"):
            fail("Invalid receipt classification.")
        candidates = extraction.get("fields", {})
        if type(candidates) is not dict or len(candidates) > 40:
            fail("Invalid OCR fields.")
        for name, values in candidates.items():
            text(name, 80, True)
            if type(values) is not list or len(values) > 8:
                fail("Invalid OCR candidates.")
            for value in values:
                if type(value) is not dict or not (isinstance(value.get("value"), str) or type(value.get("value")) is int):
                    fail("Invalid OCR candidate.")
                if isinstance(value["value"], str):
                    text(value["value"], 500)
                for name, limit, count in (("rawText", 500, 8), ("alternatives", 160, 5)):
                    lines = value.get(name, [])
                    if type(lines) is not list or len(lines) > count:
                        fail("Invalid OCR evidence lines.")
                    for line in lines:
                        text(line, limit)
                value.setdefault("rawText", [])
    proposal = report.get("proposal")
    if proposal is not None:
        if type(proposal) is not dict or not isinstance(proposal.get("issues"), list) or len(proposal["issues"]) > 30:
            fail("Invalid OCR proposal.")
        for issue in proposal["issues"]:
            text(issue, 500)
        if proposal.get("amountCents") is not None:
            amount(proposal["amountCents"], signed=True)
    previous = store.attempt("receipt", packet_id, packet_id, report["attemptId"])
    if previous:
        if previous != report:
            fail("An OCR attempt cannot be overwritten.", 409)
        return receipt_inbox(store, route)
    store.add_attempt("receipt", packet_id, packet_id, report["attemptId"], report)
    store.put("receipt-latest:" + packet_id, {"attemptId": report["attemptId"]})
    store.queue_receipt_agent(packet_id, report["attemptId"])
    return receipt_inbox(store, route)


def accept_receipt_agent_attempt(store, route, packet_id, ocr_attempt_id, review):
    """Validate and retain a model review as advisory, immutable evidence.

    The result can improve the user-facing draft but cannot create an expense,
    select an account, or authorize a ledger write.
    """
    identifier(packet_id)
    identifier(ocr_attempt_id)
    if type(review) is not dict or set(review) != {
        "schemaVersion", "reviewId", "route", "packetId", "ocrAttemptId", "createdAt", "model",
        "confidence", "confidenceTier", "confidenceReasons", "fileReviews", "proposal"
    }:
        fail("Invalid receipt agent review.")
    if review["schemaVersion"] != 1 or review["route"] != route or review["packetId"] != packet_id or review["ocrAttemptId"] != ocr_attempt_id:
        fail("Receipt agent review identity mismatch.")
    identifier(review["reviewId"])
    text(review["model"], 100, True)
    text(review["createdAt"], 80, True)
    from datetime import datetime
    try:
        datetime.fromisoformat(review["createdAt"].replace("Z", "+00:00"))
    except ValueError:
        fail("Invalid receipt agent timestamp.")
    if type(review["confidence"]) is not int or not 0 <= review["confidence"] <= 100:
        fail("Invalid receipt agent confidence.")
    tier = "high" if review["confidence"] >= 92 else "medium" if review["confidence"] >= 70 else "low"
    if review["confidenceTier"] != tier:
        fail("Receipt agent confidence tier mismatch.")
    reasons = review["confidenceReasons"]
    if type(reasons) is not list or not 1 <= len(reasons) <= 20:
        fail("Invalid receipt agent confidence reasons.")
    for reason in reasons:
        text(reason, 500, True)

    manifest = store.get("receipt-upload:" + packet_id)
    ocr = store.attempt("receipt", packet_id, packet_id, ocr_attempt_id)
    if manifest is None or ocr is None:
        fail("Receipt review evidence is missing.", 409)
    expected = {item["id"]: item for item in manifest["files"]}
    file_reviews = review["fileReviews"]
    if type(file_reviews) is not list or len(file_reviews) != len(expected):
        fail("Receipt agent must review every source image.")
    seen = set()
    for item in file_reviews:
        if type(item) is not dict or set(item) != {"fileId", "sha256", "documentType", "evidence", "issues"}:
            fail("Invalid receipt agent file review.")
        file_id = evidence_identifier(item["fileId"])
        source = expected.get(file_id)
        if source is None or file_id in seen or item["sha256"] != source["sha256"]:
            fail("Receipt agent source identity mismatch.")
        seen.add(file_id)
        if item["documentType"] not in ("invoice", "payment_receipt", "combined", "credit_or_refund", "unknown"):
            fail("Invalid receipt agent classification.")
        for name, limit in (("evidence", 12), ("issues", 12)):
            values = item[name]
            if type(values) is not list or len(values) > limit:
                fail("Invalid receipt agent file evidence.")
            for value in values:
                text(value, 500, name == "evidence")

    proposal = review["proposal"]
    required = {"status", "purchaseCount", "relationship", "vendor", "amountCents", "currency", "paymentDate",
                "category", "paymentMethod", "invoiceNumber", "checks", "issues", "agentReview"}
    if type(proposal) is not dict or set(proposal) != required or proposal["agentReview"] != "complete":
        fail("Invalid receipt agent proposal.")
    if proposal["status"] not in ("one_purchase_proposed_for_review", "needs_review"):
        fail("Invalid receipt agent proposal status.")
    if proposal["purchaseCount"] is not None and (type(proposal["purchaseCount"]) is not int or not 0 <= proposal["purchaseCount"] <= 5):
        fail("Invalid receipt agent purchase count.")
    if proposal["relationship"] not in ("invoice_and_payment_receipt", "single_paid_receipt", "invoice_only", "multiple_purchases", "credit_or_refund", "unclear"):
        fail("Invalid receipt document relationship.")
    for name, limit in (("vendor", 120), ("currency", 3), ("invoiceNumber", 120)):
        if proposal[name] is not None:
            text(proposal[name], limit, True)
    if proposal["currency"] not in (None, "USD"):
        fail("Only USD receipt candidates are supported.")
    if proposal["amountCents"] is not None:
        amount(proposal["amountCents"], signed=True)
    if proposal["paymentDate"] is not None:
        day(proposal["paymentDate"])
    if proposal["category"] is not None and proposal["category"] not in CATEGORIES:
        fail("Invalid receipt category candidate.")
    if proposal["paymentMethod"] is not None and proposal["paymentMethod"] not in PAYMENT_METHODS:
        fail("Invalid receipt payment method candidate.")
    if type(proposal["checks"]) is not dict or len(proposal["checks"]) > 20 or any(type(k) is not str or len(k) > 80 or v not in (True, False, None) for k, v in proposal["checks"].items()):
        fail("Invalid receipt agent checks.")
    if type(proposal["issues"]) is not list or len(proposal["issues"]) > 30:
        fail("Invalid receipt agent issues.")
    for issue in proposal["issues"]:
        text(issue, 500, True)
    if proposal["status"] == "one_purchase_proposed_for_review" and not (
        proposal["purchaseCount"] == 1 and proposal["vendor"] and type(proposal["amountCents"]) is int
        and proposal["amountCents"] > 0 and proposal["paymentDate"] and proposal["relationship"] in ("invoice_and_payment_receipt", "single_paid_receipt")
    ):
        fail("A ready receipt proposal must identify one paid purchase.")
    previous = store.attempt("receipt", packet_id, packet_id, review["reviewId"])
    if previous:
        if previous != review:
            fail("A receipt agent review cannot be overwritten.", 409)
        return review
    store.add_attempt("receipt", packet_id, packet_id, review["reviewId"], review)
    return review

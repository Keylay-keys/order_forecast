"""Immutable receipt/statement intake and explicit review commands."""
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from .accounting_domain import amount, day, fail, fields, identifier, now, text


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
    prepared, digests, total = [], set(), 0
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
        digest = hashlib.sha256(content).hexdigest()
        if digest in digests or store.has_original(kind, digest):
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
    for item, content in prepared:
        store.add_original(kind, upload_id, item, content)
    store.put(f"{kind}-upload:{upload_id}", manifest)
    if kind == "receipt":
        store.queue_receipt(upload_id)
    return manifest


def original(store, kind, upload_id, file_id):
    identifier(upload_id)
    identifier(file_id)
    manifest = store.get(f"{kind}-upload:{upload_id}")
    file = next((f for f in (manifest or {}).get("files", []) if f["id"] == file_id), None)
    source = store.original(kind, upload_id, file_id) if file else None
    if source is None:
        fail("Original not found.", 404)
    if source["sha256"] != file["sha256"] or len(source["content"]) != file["bytes"]:
        fail("Original evidence could not be verified.", 503)
    return source


def latest_statement(store, upload_id, file_id):
    pointer = store.get(f"statement-latest:{upload_id}:{file_id}")
    if pointer is None:
        return None
    result = store.attempt("statement", upload_id, file_id, pointer["attemptId"])
    source = original(store, "statement", upload_id, file_id)
    if result is None or result["sha256"] != source["sha256"]:
        fail("Statement reading could not be verified.", 503)
    return {key: value for key, value in result.items() if key != "_evidence"}


def statements(store, route):
    items = []
    for manifest in store.documents("statement-upload:"):
        for file in manifest["files"]:
            items.append(dict(uploadId=manifest["id"], route=route, uploadedAt=manifest["uploadedAt"], file=file, attempt=latest_statement(store, manifest["id"], file["id"])))
    return sorted(items, key=lambda item: item["uploadedAt"], reverse=True)


def analyze_bytes(content, route):
    # Only this bounded reader uses scratch files. PostgreSQL retains the original.
    with tempfile.TemporaryDirectory(prefix="accounting-pdf-") as directory:
        source = Path(directory) / "source.pdf"
        source.write_bytes(content)
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("accounting_statement_analysis.py")), str(source), route], capture_output=True, timeout=45, check=True)
        if len(result.stdout) > 6 * 1024 * 1024:
            raise ValueError("Reader response limit")
        return json.loads(result.stdout)


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


def confirm_statement(accounting, body):
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
        if existing["revision"] != "preliminary" or body["version"] != "final" or existing["periodStart"] != a["periodStart"] or body.get("replaceSource") != existing["source"]:
            fail("Confirm replacement of the exact preliminary statement with its final version.", 409)
    elif len(ledger["settlements"]) >= 520:
        fail("Statement record limit reached.", 409)
    period = {k: a[k] for k in ("periodStart", "periodEnd", "newSettlementCents", "hhcRentalFeeCents", "cashSalesCents")}
    period.update(amountToPayCents=account["amountToPay"], previousBalanceCents=account["previousBalance"], statementPaymentCents=account["payment"], source=source, revision=body["version"],
                  statementReview=dict(uploadId=attempt["uploadId"], fileId=attempt["fileId"], attemptId=attempt["id"], confirmedAt=now()))
    if existing:
        ledger["settlements"][ledger["settlements"].index(existing)] = period
    else:
        ledger["settlements"].append(period)
    return accounting.save("money-in", ledger)


def receipt_review(store, route, manifest):
    return store.get("receipt-review:" + manifest["id"]) or dict(schemaVersion=1, route=route, packetId=manifest["id"], revision=0,
        groupedFileIds=[f["id"] for f in manifest["files"]], includeInAccountantHandoff=False, status="reviewing")


def receipt_inbox(store, route):
    items = []
    jobs = store.receipt_jobs()
    for manifest in store.documents("receipt-upload:"):
        pointer = store.get("receipt-latest:" + manifest["id"])
        report = store.attempt("receipt", manifest["id"], manifest["id"], pointer["attemptId"]) if pointer else None
        if pointer and not report:
            fail("Receipt reading could not be verified.", 503)
        review_files = []
        for file in manifest["files"]:
            document = next((d for d in report["documents"] if d["fileId"] == file["id"] and d["sha256"] == file["sha256"]), None) if report else None
            extraction = (document or {}).get("extraction", {})
            review_files.append(dict(id=file["id"], originalName=file["originalName"], mediaType=file["mediaType"], bytes=file["bytes"],
                documentType=extraction.get("documentType", "unknown"), ocrStatus=(document or {}).get("status", "unsupported"), fields=extraction.get("fields", {})))
        item = dict(packetId=manifest["id"], createdAt=manifest["createdAt"], ocrStatus=report["status"] if report else "not_run",
                    files=review_files, proposal=report.get("proposal") if report else None, review=receipt_review(store, route, manifest), processingStatus=jobs.get(manifest["id"], "needs_attention"))
        if report:
            item.update(ocrAttemptId=report["attemptId"], ocrCreatedAt=report["createdAt"], pipelineVersion=report["pipelineVersion"])
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
        fid = identifier(document.get("fileId"))
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
    return receipt_inbox(store, route)

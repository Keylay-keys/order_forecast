"""Durable statement intake and exact payment-advice reconciliation."""
import asyncio
import hashlib
import logging
from uuid import uuid4

from fastapi import HTTPException
from psycopg2.extras import Json

from . import accounting_email as intake, accounting_evidence as evidence
from . import accounting_payment_advice as payment_advice
from .accounting_payment_advice_retention import discard_matched
from .accounting_store import accounting_transaction
from .accounting_domain import Accounting, now
from .business_access import require_business_access
from .dependencies import get_firestore

logger = logging.getLogger("accounting.email.worker")


def claim():
    with intake.transaction() as cur:
        cur.execute("""SELECT message_id,result FROM accounting_email_jobs
            WHERE attempts<3 AND (status='queued' OR status='reading' AND lease_expires_at<now())
            ORDER BY updated_at LIMIT 1 FOR UPDATE SKIP LOCKED""")
        row = cur.fetchone()
        if not row:
            return None
        mid, result = row
        lease = str(uuid4())
        cur.execute("""UPDATE accounting_email_jobs SET status='reading',attempts=attempts+1,
            lease_token=%s,lease_expires_at=now()+interval '10 minutes',updated_at=now() WHERE message_id=%s""", (lease, mid))
        cur.execute("""SELECT o.firebase_uid,o.route_number,o.address,a.forwarder_email,o.sha256,o.content,a.enabled
            FROM accounting_email_originals o JOIN accounting_email_addresses a USING(address) WHERE o.message_id=%s""", (mid,))
        uid, route, address, forwarder, digest, raw, enabled = cur.fetchone()
        return dict(id=str(mid), lease=lease, uid=uid, route=route, address=address,
                    forwarder=forwarder, digest=result.get('retainedContentSha256', digest), raw=bytes(raw), enabled=enabled, result=result)


def lease(cur, job):
    cur.execute("""SELECT 1 FROM accounting_email_jobs WHERE message_id=%s AND lease_token=%s
        AND status='reading' AND lease_expires_at>now() FOR UPDATE""", (job["id"], job["lease"]))
    if cur.fetchone() is None:
        raise RuntimeError("Processing lease changed")


def finish(job, status, result):
    # User-approved retention: reduce only matched advice PDFs, preserving all
    # other MIME content. No PDF/MIME processing while a database lock is held.
    reduced, result = discard_matched(job['raw'], result)
    with intake.transaction() as cur:
        lease(cur, job)
        if reduced is not None:
            cur.execute("SELECT public.accounting_discard_matched_advice(%s,%s,%s,%s,%s,%s)",
                (job['id'], job['uid'], job['route'], job['lease'], reduced, Json(result)))
        cur.execute("UPDATE accounting_email_jobs SET status=%s,result=%s,updated_at=now() WHERE message_id=%s", (status, Json(result), job["id"]))


def progress(job, result, processed):
    # Renew only a still-owned lease, between bounded PDF reads. No PDF work
    # occurs while this short transaction holds the job row lock.
    result.update(processed=processed, total=len(result["statements"]),
                  detail=f"Reading {processed} of {len(result['statements'])} new statements. Originals are saved.")
    with intake.transaction() as cur:
        lease(cur, job)
        cur.execute("""UPDATE accounting_email_jobs SET result=%s,
            lease_expires_at=now()+interval '10 minutes',updated_at=now()
            WHERE message_id=%s AND lease_token=%s""", (Json(result), job["id"], job["lease"]))


async def process(job, db):
    try:
        if not job["enabled"]:
            finish(job, "blocked", {"detail": "This intake address is disabled."})
            return
        await require_business_access(job["route"], {"uid": job["uid"]}, db)
        if hashlib.sha256(job["raw"]).hexdigest() != job["digest"]:
            raise ValueError("Stored email integrity check failed. Upload the PDF to continue.")
        result = job["result"]
        if not result.get("saved"):
            metadata, files = intake.parse_message(job["raw"], job["address"], job["forwarder"])
            if metadata.get("verificationCode") or metadata.get("verificationUrl"):
                finish(job, "verification", metadata)
                return
            advice_digests = set(metadata.get("paymentAdviceDigests", []))
            advice_numbers, advice_errors, advice_files = [], [], []
            for _, _, content in files:
                if hashlib.sha256(content).hexdigest() not in advice_digests:
                    continue
                # Parsing is outside the database transaction. Renew the owned
                # lease between bounded reads, including attached-email batches.
                with accounting_transaction(job["uid"], job["route"]) as store:
                    lease(store.cursor, job)
                    store.cursor.execute("UPDATE accounting_email_jobs SET lease_expires_at=now()+interval '10 minutes' WHERE message_id=%s", (job["id"],))
                try:
                    advice = payment_advice.read_bytes(content, job["route"])
                except (OSError, ValueError, evidence.subprocess.SubprocessError):
                    advice_errors.append(payment_advice.ERROR)
                    continue
                with accounting_transaction(job["uid"], job["route"]) as store:
                    lease(store.cursor, job)
                    # Catch inside the transaction so a conflicting document
                    # marker is retained, while preserving every prior payment.
                    try:
                        payment_advice.save(store, advice, job["id"], content)
                        advice_numbers.append(advice["documentNumber"])
                        advice_files.append(dict(documentNumber=advice['documentNumber'], sha256=hashlib.sha256(content).hexdigest()))
                    except ValueError as error:
                        advice_errors.append(str(error))
            statement_files = [file for file in files if hashlib.sha256(file[2]).hexdigest() not in advice_digests]
            with accounting_transaction(job["uid"], job["route"]) as store:
                lease(store.cursor, job)
                refs, duplicates = intake.save_files(store, job["route"], statement_files)
                result = dict(saved=True, statements=refs, duplicates=duplicates)
                if advice_digests:
                    result.update(paymentAdviceDocuments=sorted(set(advice_numbers)), paymentAdviceErrors=advice_errors, paymentAdviceFiles=advice_files)
                store.cursor.execute("UPDATE accounting_email_jobs SET result=%s WHERE message_id=%s", (Json(result), job["id"]))
        job["result"] = result
        errors = 0
        for index, ref in enumerate(result["statements"]):
            progress(job, result, index)
            await require_business_access(job["route"], {"uid": job["uid"]}, db)
            with accounting_transaction(job["uid"], job["route"]) as store:
                lease(store.cursor, job)
                prior = evidence.latest_statement(store, ref["uploadId"], ref["fileId"])
                if prior:
                    errors += int(not prior.get("analysis"))
                    continue
                source = evidence.original(store, "statement", ref["uploadId"], ref["fileId"])
            analysis = None
            try:
                analysis = evidence.analyze_bytes(source["content"], job["route"])
            except (OSError, ValueError, evidence.subprocess.SubprocessError):
                pass
            with accounting_transaction(job["uid"], job["route"]) as store:
                lease(store.cursor, job)
                attempt = evidence.analyze_statement(store, job["route"], ref["uploadId"], ref["fileId"], reader=lambda *_: analysis)
                if not attempt.get("analysis"):
                    errors += 1
        await require_business_access(job["route"], {"uid": job["uid"]}, db)
        with accounting_transaction(job["uid"], job["route"]) as store:
            lease(store.cursor, job)
            ledger = evidence.sync_statement_updates(Accounting(store, job["route"], now()[:10]))
            ledger = payment_advice.reconcile(Accounting(store, job["route"], now()[:10]), ledger)
            advice_records = [store.get(payment_advice.PREFIX + number) for number in result.get("paymentAdviceDocuments", [])]
            received_files = {ref["fileId"] for ref in result["statements"]}
            result["automaticUpdates"] = sum(bool(period.get("automaticUpdate")) and (period.get("statementReview") or {}).get("fileId") in received_files
                for period in ledger["settlements"])
        result.update(processed=len(result["statements"]), total=len(result["statements"]))
        result["detail"] = f"{len(result['statements'])} new statements saved; {result['duplicates']} already saved. " + (
            "Open Statements to review in Money in." if not errors else
            f"{errors} readings need attention. Open Statements to review in Money in.")
        if result["automaticUpdates"]:
            result["detail"] = f"{result['automaticUpdates']} billing periods updated automatically. Open Money in to see the updates and any statements still awaiting review."
        if "paymentAdviceDocuments" in result:
            result["statementReadErrors"] = errors
            advice_status, result = payment_advice.arrival_status(result, {record["documentNumber"]: record for record in advice_records if record})
            errors += int(advice_status == "needs_attention")
        finish(job, "needs_attention" if errors else "ready" if result["statements"] or result.get("paymentAdviceDocuments") else "duplicate", result)
    except ValueError as error:
        finish(job, "needs_attention", {**job["result"], "detail": str(error)})
    except HTTPException as error:
        if error.status_code == 403:
            finish(job, "blocked", {"detail": "Accounting access for this route is no longer active."})
        else:
            raise


async def main():
    logging.basicConfig(level=logging.INFO)
    db = get_firestore()
    while True:
        try:
            job = claim()
            if job:
                await process(job, db)
                continue
        except Exception:
            # No MIME, private addresses, credentials or financial fields in logs.
            # Lease expiry retries durable evidence at most three times.
            logger.warning("Settlement email processing paused; saved evidence remains queued")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())

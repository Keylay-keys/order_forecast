"""Restart-safe cluster consumer for email originals. No financial confirmation."""
import asyncio
import hashlib
import logging
from uuid import uuid4

from fastapi import HTTPException
from psycopg2.extras import Json

from . import accounting_email as intake, accounting_evidence as evidence
from .accounting_store import accounting_transaction
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
                    forwarder=forwarder, digest=digest, raw=bytes(raw), enabled=enabled, result=result)


def lease(cur, job):
    cur.execute("""SELECT 1 FROM accounting_email_jobs WHERE message_id=%s AND lease_token=%s
        AND status='reading' AND lease_expires_at>now() FOR UPDATE""", (job["id"], job["lease"]))
    if cur.fetchone() is None:
        raise RuntimeError("Processing lease changed")


def finish(job, status, result):
    with intake.transaction() as cur:
        lease(cur, job)
        cur.execute("UPDATE accounting_email_jobs SET status=%s,result=%s,updated_at=now() WHERE message_id=%s", (status, Json(result), job["id"]))


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
            with accounting_transaction(job["uid"], job["route"]) as store:
                lease(store.cursor, job)
                refs, duplicates = intake.save_files(store, job["route"], files)
                result = dict(saved=True, statements=refs, duplicates=duplicates)
                store.cursor.execute("UPDATE accounting_email_jobs SET result=%s WHERE message_id=%s", (Json(result), job["id"]))
        errors = 0
        for ref in result["statements"]:
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
        result["detail"] = ("PDFs saved. Open Saved statements to review." if not errors else
                            "PDFs saved, but a reading needs attention. Open Saved statements to review the original.")
        finish(job, "needs_attention" if errors else "ready" if result["statements"] else "duplicate", result)
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

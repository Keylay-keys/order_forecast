"""Dedicated Mac OCR service boundary. No bill/payment/ledger-write commands.

The service credential can claim queued receipt packets, read only the sources
covered by its live lease, and submit derived candidates. Owner/pilot eligibility
is rechecked on every operation. Candidates still require user review to post.
"""
from contextlib import contextmanager
import json
import os
import secrets
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from ..accounting_domain import fail, identifier
from ..accounting_store import AccountingStore
from ..accounting_evidence import accept_receipt_agent_attempt, accept_receipt_attempt, original
from ..business_access import require_business_access
from ..dependencies import get_firestore, get_pg_connection, return_pg_connection
from .accounting import HEADERS, bounded_body

router = APIRouter()


def worker_auth(request: Request):
    expected = os.environ.get("ACCOUNTING_RECEIPT_WORKER_TOKEN", "")
    supplied = request.headers.get("Authorization", "")
    if len(expected) < 48 or not secrets.compare_digest(supplied, "Bearer " + expected):
        fail("Worker authentication required.", 401)


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


def claim():
    with transaction() as cur:
        cur.execute("""SELECT j.job_id,j.firebase_uid,j.route_number,j.upload_id FROM accounting_receipt_jobs j
            WHERE j.attempts < 3 AND (j.status='queued' OR j.status='running' AND j.lease_expires_at < now())
            AND NOT EXISTS (
                SELECT 1 FROM accounting_documents d
                WHERE d.firebase_uid=j.firebase_uid AND d.route_number=j.route_number
                AND d.document_key='receipt-review:' || j.upload_id::text
                AND d.payload ? 'archivedAt'
            )
            ORDER BY j.created_at LIMIT 1 FOR UPDATE OF j SKIP LOCKED""")
        row = cur.fetchone()
        if row is None:
            return None
        job, uid, route, upload = map(str, row)
        lease = str(uuid4())
        cur.execute("""UPDATE accounting_receipt_jobs SET status='running',attempts=attempts+1,
            lease_token=%s,lease_expires_at=now()+interval '10 minutes',updated_at=now() WHERE job_id=%s""", (lease, job))
        return dict(jobId=job, uid=uid, route=route, uploadId=upload, lease=lease)


def lease_scope(job_id, lease):
    identifier(job_id)
    identifier(lease)
    with transaction() as cur:
        cur.execute("""SELECT firebase_uid,route_number,upload_id,status FROM accounting_receipt_jobs
            WHERE job_id=%s AND lease_token=%s AND (status='complete' OR status='running' AND lease_expires_at>now())""", (job_id, lease))
        row = cur.fetchone()
        if row is None:
            fail("Receipt processing lease expired or changed.", 409)
        return tuple(map(str, row))


def blocked(job_id, lease):
    with transaction() as cur:
        cur.execute("UPDATE accounting_receipt_jobs SET status='blocked',updated_at=now() WHERE job_id=%s AND lease_token=%s", (job_id, lease))


def read_manifest(uid, route, upload):
    with transaction() as cur:
        manifest = AccountingStore(cur, uid, route).get("receipt-upload:" + upload)
        if manifest is None:
            fail("Receipt evidence missing.", 503)
        return manifest


def claim_agent():
    with transaction() as cur:
        cur.execute("""SELECT j.job_id,j.firebase_uid,j.route_number,j.upload_id,j.ocr_attempt_id
            FROM accounting_receipt_agent_jobs j
            WHERE j.attempts < 3 AND (j.status='queued' OR j.status='running' AND j.lease_expires_at < now())
            AND NOT EXISTS (
                SELECT 1 FROM accounting_documents d
                WHERE d.firebase_uid=j.firebase_uid AND d.route_number=j.route_number
                AND d.document_key='receipt-review:' || j.upload_id::text
                AND d.payload ? 'archivedAt'
            )
            ORDER BY j.created_at LIMIT 1 FOR UPDATE OF j SKIP LOCKED""")
        row = cur.fetchone()
        if row is None:
            return None
        job, uid, route, upload, ocr_attempt = map(str, row)
        lease = str(uuid4())
        cur.execute("""UPDATE accounting_receipt_agent_jobs SET status='running',attempts=attempts+1,
            lease_token=%s,lease_expires_at=now()+interval '30 minutes',updated_at=now() WHERE job_id=%s""", (lease, job))
        store = AccountingStore(cur, uid, route)
        manifest = store.get("receipt-upload:" + upload)
        ocr = store.attempt("receipt", upload, upload, ocr_attempt)
        if manifest is None or ocr is None:
            cur.execute("UPDATE accounting_receipt_agent_jobs SET status='blocked',updated_at=now() WHERE job_id=%s", (job,))
            return None
        return dict(jobId=job, uid=uid, route=route, uploadId=upload, ocrAttemptId=ocr_attempt,
                    lease=lease, manifest=manifest, ocrReport=ocr)


def agent_lease_scope(job_id, lease):
    identifier(job_id)
    identifier(lease)
    with transaction() as cur:
        cur.execute("""SELECT firebase_uid,route_number,upload_id,ocr_attempt_id,status
            FROM accounting_receipt_agent_jobs
            WHERE job_id=%s AND lease_token=%s
            AND (status='complete' OR status='running' AND lease_expires_at>now())""", (job_id, lease))
        row = cur.fetchone()
        if row is None:
            fail("Receipt review lease expired or changed.", 409)
        return tuple(map(str, row))


def block_agent(job_id, lease):
    with transaction() as cur:
        cur.execute("""UPDATE accounting_receipt_agent_jobs SET status='blocked',updated_at=now()
            WHERE job_id=%s AND lease_token=%s""", (job_id, lease))


@router.post("/business/receipt-worker/claim", dependencies=[Depends(worker_auth)])
async def claim_receipt(request: Request, db=Depends(get_firestore)):
    job = await run_in_threadpool(claim)
    if job:
        try:
            await require_business_access(job["route"], {"uid": job["uid"]}, db)
        except HTTPException as error:
            if error.status_code == 403:
                await run_in_threadpool(blocked, job["jobId"], job["lease"])
                return Response('{"job":null}', media_type="application/json", headers=HEADERS)
            raise
        job["manifest"] = await run_in_threadpool(read_manifest, job["uid"], job["route"], job["uploadId"])
    return Response(json.dumps({"job": job}), media_type="application/json", headers=HEADERS)


@router.get("/business/receipt-worker/{job_id}/files/{file_id}", dependencies=[Depends(worker_auth)])
async def read_receipt_source(request: Request, job_id: str, file_id: str, db=Depends(get_firestore)):
    uid, route, upload, status = await run_in_threadpool(lease_scope, job_id, request.headers.get("X-Receipt-Lease"))
    if status != "running":
        fail("Receipt source lease has completed.", 409)
    await require_business_access(route, {"uid": uid}, db)
    def read():
        with transaction() as cur:
            return original(AccountingStore(cur, uid, route), "receipt", upload, file_id)
    source = await run_in_threadpool(read)
    return Response(source["content"], media_type=source["mediaType"], headers=HEADERS)


@router.post("/business/receipt-worker/{job_id}/complete", dependencies=[Depends(worker_auth)])
async def complete_receipt(request: Request, job_id: str, db=Depends(get_firestore)):
    lease = request.headers.get("X-Receipt-Lease")
    uid, route, upload, status = await run_in_threadpool(lease_scope, job_id, lease)
    await require_business_access(route, {"uid": uid}, db)
    raw = await bounded_body(request, 5 * 1024 * 1024)
    try:
        report = json.loads(raw)
    except (ValueError, UnicodeError):
        fail("Invalid OCR result JSON.")
    if type(report) is not dict:
        fail("Invalid OCR result JSON.")
    # Reject nonfinite numbers anywhere, including worker debug evidence.
    try:
        json.dumps(report, allow_nan=False)
    except ValueError:
        fail("Invalid OCR result numbers.")
    def complete():
        with transaction() as cur:
            cur.execute("""SELECT status,completed_attempt_id FROM accounting_receipt_jobs WHERE job_id=%s
                AND lease_token=%s AND (status='complete' OR status='running' AND lease_expires_at>now()) FOR UPDATE""", (job_id, lease))
            row = cur.fetchone()
            if row is None:
                fail("Receipt processing lease expired or changed.", 409)
            if row[0] == "complete" and str(row[1]) != (report or {}).get("attemptId"):
                fail("This receipt job already completed.", 409)
            store = AccountingStore(cur, uid, route)
            accept_receipt_attempt(store, route, upload, report)
            cur.execute("""UPDATE accounting_receipt_jobs SET status='complete',completed_attempt_id=%s,updated_at=now()
                WHERE job_id=%s AND lease_token=%s""", (report["attemptId"], job_id, lease))
            return {"accepted": True, "attemptId": report["attemptId"]}
    result = await run_in_threadpool(complete)
    return Response(json.dumps(result), media_type="application/json", headers=HEADERS)


@router.post("/business/receipt-agent/claim", dependencies=[Depends(worker_auth)])
async def claim_receipt_agent(request: Request, db=Depends(get_firestore)):
    job = await run_in_threadpool(claim_agent)
    if job:
        try:
            await require_business_access(job["route"], {"uid": job["uid"]}, db)
        except HTTPException as error:
            if error.status_code == 403:
                await run_in_threadpool(block_agent, job["jobId"], job["lease"])
                return Response('{"job":null}', media_type="application/json", headers=HEADERS)
            raise
    return Response(json.dumps({"job": job}), media_type="application/json", headers=HEADERS)


@router.get("/business/receipt-agent/{job_id}/files/{file_id}", dependencies=[Depends(worker_auth)])
async def read_receipt_agent_source(request: Request, job_id: str, file_id: str, db=Depends(get_firestore)):
    uid, route, upload, ocr_attempt, status = await run_in_threadpool(
        agent_lease_scope, job_id, request.headers.get("X-Receipt-Lease"))
    if status != "running":
        fail("Receipt review source lease has completed.", 409)
    await require_business_access(route, {"uid": uid}, db)
    def read():
        with transaction() as cur:
            return original(AccountingStore(cur, uid, route), "receipt", upload, file_id)
    source = await run_in_threadpool(read)
    return Response(source["content"], media_type=source["mediaType"], headers=HEADERS)


@router.post("/business/receipt-agent/{job_id}/complete", dependencies=[Depends(worker_auth)])
async def complete_receipt_agent(request: Request, job_id: str, db=Depends(get_firestore)):
    lease = request.headers.get("X-Receipt-Lease")
    uid, route, upload, ocr_attempt, status = await run_in_threadpool(agent_lease_scope, job_id, lease)
    await require_business_access(route, {"uid": uid}, db)
    raw = await bounded_body(request, 2 * 1024 * 1024)
    try:
        review = json.loads(raw)
        json.dumps(review, allow_nan=False)
    except (ValueError, UnicodeError):
        fail("Invalid receipt agent result JSON.")
    if type(review) is not dict:
        fail("Invalid receipt agent result JSON.")
    def complete():
        with transaction() as cur:
            cur.execute("""SELECT status,completed_review_id FROM accounting_receipt_agent_jobs
                WHERE job_id=%s AND lease_token=%s
                AND (status='complete' OR status='running' AND lease_expires_at>now()) FOR UPDATE""", (job_id, lease))
            row = cur.fetchone()
            if row is None:
                fail("Receipt review lease expired or changed.", 409)
            if row[0] == "complete" and str(row[1]) != review.get("reviewId"):
                fail("This receipt review job already completed.", 409)
            store = AccountingStore(cur, uid, route)
            accept_receipt_agent_attempt(store, route, upload, ocr_attempt, review)
            cur.execute("""UPDATE accounting_receipt_agent_jobs SET status='complete',completed_review_id=%s,
                updated_at=now() WHERE job_id=%s AND lease_token=%s""", (review["reviewId"], job_id, lease))
            return {"accepted": True, "reviewId": review["reviewId"]}
    result = await run_in_threadpool(complete)
    return Response(json.dumps(result), media_type="application/json", headers=HEADERS)

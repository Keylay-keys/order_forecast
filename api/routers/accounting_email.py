"""Signed Cloudflare-to-cluster boundary; acknowledge only committed MIME evidence."""
import asyncio
import hashlib

from fastapi import APIRouter, Depends, Request
from starlette.concurrency import run_in_threadpool

from .. import accounting_email as intake
from ..accounting_domain import fail
from ..business_access import require_business_access
from ..dependencies import get_firestore
from .accounting import bounded_body

router = APIRouter()
SLOTS = asyncio.Semaphore(1)


@router.post("/business/settlement-email/receive", status_code=202)
async def receive(request: Request, db=Depends(get_firestore)):
    recipient = request.headers.get("X-Settlement-Recipient", "")
    sender = request.headers.get("X-Settlement-Sender", "")
    digest = request.headers.get("X-Settlement-SHA256", "")
    intake.verify_signature(request.headers.get("X-Settlement-Time", ""), recipient, sender,
                            digest, request.headers.get("X-Settlement-Signature", ""))
    if SLOTS.locked():
        fail("Email intake is busy. Try forwarding again shortly.", 503)
    async with SLOTS:
        uid, route, _ = await run_in_threadpool(intake.address_scope, recipient)
        await require_business_access(route, {"uid": uid}, db)
        raw = await bounded_body(request, intake.MAX_EMAIL)
        if not raw or hashlib.sha256(raw).hexdigest() != digest:
            fail("Email content does not match its signature.", 400)
        return await run_in_threadpool(intake.accept, recipient, uid, route, sender, raw)

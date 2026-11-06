"""Accounting access and the authenticated cluster workflow API."""

from fastapi import APIRouter, Depends, Query, Request
from google.cloud import firestore

from ..business_access import require_business_access
from ..dependencies import get_firestore, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history
from .accounting import router as accounting_router
from .accounting_purchase import router as accounting_purchase_router
from .accounting_worker import router as accounting_worker_router

from .accounting_email import router as accounting_email_router

router = APIRouter()
router.include_router(accounting_purchase_router)
router.include_router(accounting_router)
router.include_router(accounting_worker_router)
router.include_router(accounting_email_router)


@router.get("/business/access")
@rate_limit_history
async def business_access(
    request: Request,
    route: str = Query(..., pattern=r"^[0-9]{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> dict:
    await require_business_access(route, decoded_token, db)
    return {"uid": decoded_token["uid"], "routeNumber": route, "enabled": True}

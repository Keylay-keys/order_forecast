"""Business pilot shell only. No financial records or writes in this slice."""

from fastapi import APIRouter, Depends, Query, Request
from google.cloud import firestore

from ..business_access import require_business_access
from ..dependencies import get_firestore, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history

router = APIRouter()


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

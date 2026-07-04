"""Dashboard summary API backed by the cluster mirror."""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, Path, Request
from google.cloud import firestore

from ..dashboard_summary import get_dashboard_summary_payload
from ..dependencies import (
    get_firestore,
    get_pg_connection,
    require_route_feature_access,
    return_pg_connection,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history
from ..models import ErrorResponse

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/routes/{route_number}/dashboard-summary",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_dashboard_summary(
    request: Request,
    route_number: str = Path(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return route dashboard data from the cluster mirror.

    The API owns freshness validation. The client should rely on the returned
    freshness metadata and must not read Firebase only to verify the mirror.
    """
    _ = request
    await require_route_feature_access(route_number, "managementDashboard", decoded_token, db)

    conn = None
    try:
        conn = get_pg_connection()
        payload = get_dashboard_summary_payload(db=db, conn=conn, route_number=route_number)
        payload["routeNumber"] = route_number
        return payload
    finally:
        if conn is not None:
            return_pg_connection(conn)

"""Admin reporting for automatic server-side API usage analytics."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Request

from ..dependencies import get_pg_connection, return_pg_connection, verify_firebase_token
from ..errors import StructuredApiError
from ..middleware.rate_limit import rate_limit_history
from ..models import ErrorResponse
from ..usage_analytics import get_usage_summary


router = APIRouter()


def _admin_uids() -> set[str]:
    return {
        value.strip()
        for value in os.environ.get("USAGE_ANALYTICS_ADMIN_UIDS", "").split(",")
        if value.strip()
    }


def _require_usage_admin(decoded_token: Dict[str, Any]) -> None:
    uid = str(decoded_token.get("uid") or "").strip()
    has_admin_claim = bool(
        decoded_token.get("usageAnalyticsAdmin") is True
        or decoded_token.get("admin") is True
    )
    if not has_admin_claim and uid not in _admin_uids():
        raise StructuredApiError(
            status_code=403,
            error="Usage analytics access denied",
            code="USAGE_ANALYTICS_ADMIN_REQUIRED",
        )


@router.get(
    "/admin/usage/summary",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def usage_summary(
    request: Request,
    days: int = Query(default=30, ge=1, le=90),
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
) -> Dict[str, Any]:
    """Return route/role aggregates for the internal admin widget."""

    _ = request
    _require_usage_admin(decoded_token)
    conn = None
    try:
        conn = get_pg_connection()
        return get_usage_summary(conn, days=days, route_number=route)
    finally:
        if conn is not None:
            return_pg_connection(conn)

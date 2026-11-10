"""Dashboard summary API backed by the cluster mirror."""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from google.cloud import firestore
from psycopg2.errors import UndefinedTable

from ..dependencies import (
    get_firestore,
    get_pg_connection,
    require_route_access,
    return_pg_connection,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history
from ..models import ErrorResponse
from ..dashboard_summary import get_dashboard_summary_payload
from ..pcf_active_serving import (
    ProjectionContractError,
    ProjectionUnavailable,
    get_dashboard_summary_from_postgres,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _dashboard_fallback(db: firestore.Client, conn: Any, route_number: str) -> Dict[str, Any]:
    """Serve the existing compact Firebase summary when projection serving is unavailable."""
    try:
        conn.rollback()
    except Exception:
        logger.exception("dashboard_projection_fallback_rollback_failed")
        raise HTTPException(
            503,
            {"code": "dashboard_unavailable", "reason": "postgres_rollback_failed"},
        )

    try:
        return get_dashboard_summary_payload(
            db=db,
            conn=conn,
            route_number=route_number,
        )
    except Exception as exc:
        logger.exception("dashboard_projection_fallback_failed")
        raise HTTPException(
            503,
            {"code": "dashboard_unavailable", "reason": "fallback_failed"},
        ) from exc


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
    # Dashboard/PCF data is route-visible operational data. Subscription and
    # trial entitlements protect premium actions, not read access to a route the
    # caller is already authorized to use.
    await require_route_access(route_number, decoded_token, db)

    conn = None
    try:
        conn = get_pg_connection()
        return get_dashboard_summary_from_postgres(conn, route_number)
    except ProjectionUnavailable as exc:
        logger.warning("dashboard_projection_fallback reason=%s", exc.reason)
        return _dashboard_fallback(db, conn, route_number)
    except ProjectionContractError:
        logger.exception("dashboard_projection_contract_error_fallback")
        return _dashboard_fallback(db, conn, route_number)
    except UndefinedTable:
        logger.exception("dashboard_projection_schema_missing_fallback")
        return _dashboard_fallback(db, conn, route_number)
    finally:
        if conn is not None:
            return_pg_connection(conn)

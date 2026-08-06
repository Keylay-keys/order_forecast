"""Authenticated product usage ingestion and admin reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field, field_validator

from ..dependencies import (
    _has_legacy_subscription_feature,
    _has_trial_feature,
    _is_owner_for_route,
    _resolve_owner_uid_for_route,
    get_firestore,
    get_pg_connection,
    require_route_access,
    return_pg_connection,
    verify_firebase_token,
)
from ..errors import StructuredApiError
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse
from ..usage_analytics import (
    ALLOWED_USAGE_FEATURES,
    build_actor_hash,
    get_usage_summary,
    record_usage_batch,
)


router = APIRouter()


class UsageEvent(BaseModel):
    feature: str = Field(..., min_length=1, max_length=64)
    count: int = Field(default=1, ge=1, le=100)

    @field_validator("feature")
    @classmethod
    def validate_feature(cls, value: str) -> str:
        feature = value.strip()
        if feature not in ALLOWED_USAGE_FEATURES:
            raise ValueError("Unsupported usage feature")
        return feature


class UsageBatchRequest(BaseModel):
    batchId: str = Field(..., min_length=16, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    routeNumber: str = Field(..., min_length=1, max_length=10, pattern=r"^\d{1,10}$")
    platform: Literal["android", "ios", "web"]
    appVersion: Optional[str] = Field(None, max_length=32)
    events: List[UsageEvent] = Field(..., min_length=1, max_length=25)

    @field_validator("appVersion", mode="before")
    @classmethod
    def clean_app_version(cls, value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None


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


def _active_entitlement_tier(
    *,
    db: firestore.Client,
    route_number: str,
    requester_uid: str,
    requester_data: Dict[str, Any],
) -> str:
    entitlement_doc = db.collection("routeEntitlements").document(route_number).get()
    if entitlement_doc.exists:
        entitlement = entitlement_doc.to_dict() or {}
        provider = str(entitlement.get("provider") or entitlement.get("source") or "").strip().lower()
        apple_environment = str(entitlement.get("appleEnvironment") or "").strip().lower()
        if bool(entitlement.get("active")) and not (
            provider in {"apple", "app_store", "appstore", "ios"}
            and apple_environment == "sandbox"
        ):
            return "paid"

    owner_data = requester_data
    if not _is_owner_for_route(requester_data, route_number):
        owner_uid = _resolve_owner_uid_for_route(
            db=db,
            route_number=route_number,
            requester_uid=requester_uid,
            requester_data=requester_data,
        )
        if owner_uid:
            owner_doc = db.collection("users").document(owner_uid).get()
            if owner_doc.exists:
                owner_data = owner_doc.to_dict() or {}

    if _has_legacy_subscription_feature(
        route_number=route_number,
        owner_data=owner_data,
        feature_key="scanner",
    ):
        return "paid"
    if _has_trial_feature(
        route_number=route_number,
        owner_data=owner_data,
        feature_key="scanner",
    ):
        return "trial"
    return "free"


@router.post(
    "/usage/events",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_write
async def ingest_usage_events(
    request: Request,
    payload: UsageBatchRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Record one idempotent, content-free usage batch."""

    _ = request
    user_data = await require_route_access(payload.routeNumber, decoded_token, db)
    actor_role = "owner" if _is_owner_for_route(user_data, payload.routeNumber) else "team_member"
    access_tier = _active_entitlement_tier(
        db=db,
        route_number=payload.routeNumber,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )

    try:
        actor_hash = build_actor_hash(decoded_token["uid"])
    except RuntimeError as exc:
        raise StructuredApiError(
            status_code=503,
            error="Usage analytics is not configured",
            code="USAGE_ANALYTICS_UNAVAILABLE",
        ) from exc

    conn = None
    try:
        conn = get_pg_connection()
        accepted = record_usage_batch(
            conn,
            batch_id=payload.batchId,
            actor_hash=actor_hash,
            route_number=payload.routeNumber,
            actor_role=actor_role,
            access_tier=access_tier,
            platform=payload.platform,
            app_version=payload.appVersion,
            events=[event.model_dump() for event in payload.events],
            now=datetime.now(timezone.utc),
        )
        return {"ok": True, "accepted": accepted}
    finally:
        if conn is not None:
            return_pg_connection(conn)


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

"""Billing router - subscription catalog and entitlement reads.

This is the backend contract for mobile IAP wiring. Verification endpoints are
intentionally explicit when Apple server-side verification is not yet enabled.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from google.cloud import firestore
from pydantic import BaseModel, Field

from ..dependencies import get_firestore, require_route_access, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse

router = APIRouter()
logger = logging.getLogger("api.billing")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


APPLE_BILLING_VERIFICATION_ENABLED = _bool_env("APPLE_BILLING_VERIFICATION_ENABLED", False)


def _normalize_route_number(value: Any) -> str:
    route = str(value or "").strip()
    return route if route.isdigit() and len(route) <= 10 else ""


def _to_epoch_millis(value: Any) -> Optional[int]:
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, dict):
        seconds = value.get("seconds") or value.get("_seconds")
        nanos = value.get("nanoseconds") or value.get("_nanoseconds") or value.get("nanos") or 0
        if seconds is None:
            return None
        try:
            return int((float(seconds) + float(nanos) / 1_000_000_000) * 1000)
        except Exception:
            return None
    return None


def _normalize_plan(value: Any) -> Optional[Literal["solo", "pro"]]:
    v = str(value or "").strip().lower()
    if v in ("solo", "pro"):
        return v  # type: ignore[return-value]
    return None


def _normalize_interval(value: Any) -> Optional[Literal["monthly", "yearly"]]:
    v = str(value or "").strip().lower()
    if v in ("monthly", "month"):
        return "monthly"
    if v in ("yearly", "year", "annual", "annually"):
        return "yearly"
    return None


def _is_owner_for_route(user_data: Dict[str, Any], route_number: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if (
        str(profile.get("role") or "").strip() == "owner"
        and _normalize_route_number(profile.get("routeNumber")) == route_number
    ):
        return True
    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "owner"


def _resolve_owner_uid_for_route(
    *,
    db: firestore.Client,
    route_number: str,
    requester_uid: str,
    requester_data: Dict[str, Any],
) -> str:
    route_doc = db.collection("routes").document(route_number).get()
    if route_doc.exists:
        route_data = route_doc.to_dict() or {}
        owner_uid = str(route_data.get("ownerUid") or route_data.get("userId") or "").strip()
        if owner_uid:
            return owner_uid
    if _is_owner_for_route(requester_data, route_number):
        return requester_uid
    assignments = requester_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    if isinstance(assignment, dict):
        assigned_to = str(assignment.get("assignedTo") or "").strip()
        if assigned_to:
            return assigned_to
    return ""


def _feature_payload_for_plan(plan: Optional[str]) -> Dict[str, bool]:
    is_pro = str(plan or "").strip().lower() == "pro"
    return {
        "scanner": True,
        "managementDashboard": True,
        "multiRoute": is_pro,
        "ordering": True,
        "forecasting": True,
        "pcfEmailImport": True,
    }


def _error_response(
    status_code: int,
    *,
    error: str,
    code: str,
    details: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    payload: Dict[str, Any] = {"error": error, "code": code}
    if details:
        payload["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


# ---------------------------------------------------------------------------
# Product catalog
# ---------------------------------------------------------------------------

IAP_PRODUCT_MAP: Dict[str, Dict[str, Any]] = {
    "com.keylay.routespark.solo.monthly": {
        "plan": "solo",
        "interval": "monthly",
        "displayName": "Solo Monthly",
        "description": "Adds monthly Solo subscription access",
        "platforms": ["ios", "android"],
    },
    "com.keylay.routespark.solo.yearly": {
        "plan": "solo",
        "interval": "yearly",
        "displayName": "Solo Yearly",
        "description": "Adds yearly Solo subscription access",
        "platforms": ["ios", "android"],
    },
    "com.keylay.routespark.pro.monthly": {
        "plan": "pro",
        "interval": "monthly",
        "displayName": "Pro Monthly",
        "description": "Adds monthly Pro subscription access",
        "platforms": ["ios", "android"],
    },
    "com.keylay.routespark.pro.yearly": {
        "plan": "pro",
        "interval": "yearly",
        "displayName": "Pro Yearly",
        "description": "Adds yearly Pro subscription access",
        "platforms": ["ios", "android"],
    },
}


class BillingProduct(BaseModel):
    productId: str
    platform: Literal["ios", "android"]
    plan: Literal["solo", "pro"]
    interval: Literal["monthly", "yearly"]
    displayName: str
    description: str
    features: Dict[str, bool]


class BillingProductsResponse(BaseModel):
    ok: bool
    products: List[BillingProduct]


class BillingEntitlement(BaseModel):
    routeNumber: str
    active: bool
    plan: Optional[Literal["solo", "pro"]] = None
    provider: Optional[str] = None
    interval: Optional[Literal["monthly", "yearly"]] = None
    currentPeriodEndMs: Optional[int] = None
    source: Literal["route_entitlements", "legacy_user_subscription", "trial", "none"]
    updatedAtMs: Optional[int] = None
    features: Dict[str, bool]


class BillingEntitlementResponse(BaseModel):
    ok: bool
    entitlement: BillingEntitlement


class AppleVerifyRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    productId: str = Field(..., min_length=3, max_length=255)
    transactionId: Optional[str] = Field(default=None, max_length=255)
    originalTransactionId: Optional[str] = Field(default=None, max_length=255)
    signedTransactionInfo: Optional[str] = None
    signedRenewalInfo: Optional[str] = None
    appAccountToken: Optional[str] = Field(default=None, max_length=255)
    environment: Optional[Literal["Sandbox", "Production"]] = None


class AppleRestoreRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    appAccountToken: Optional[str] = Field(default=None, max_length=255)
    originalTransactionId: Optional[str] = Field(default=None, max_length=255)


def _coerce_entitlement_from_route_doc(
    route_number: str,
    doc_data: Dict[str, Any],
) -> Optional[BillingEntitlement]:
    plan = _normalize_plan(doc_data.get("plan"))
    interval = _normalize_interval(doc_data.get("interval"))
    active = bool(doc_data.get("active"))
    provider = str(doc_data.get("provider") or "").strip() or None
    features = doc_data.get("features") if isinstance(doc_data.get("features"), dict) else {}
    if not features and plan:
        features = _feature_payload_for_plan(plan)
    return BillingEntitlement(
        routeNumber=route_number,
        active=active,
        plan=plan,
        provider=provider,
        interval=interval,
        currentPeriodEndMs=_to_epoch_millis(doc_data.get("currentPeriodEnd")),
        source="route_entitlements",
        updatedAtMs=_to_epoch_millis(doc_data.get("updatedAt")),
        features={k: bool(v) for k, v in features.items()},
    )


def _coerce_legacy_subscription_entitlement(
    route_number: str,
    owner_data: Dict[str, Any],
) -> Optional[BillingEntitlement]:
    route_sub = (
        owner_data.get("subscriptions", {})
        .get("routes", {})
        .get(route_number)
        if isinstance(owner_data.get("subscriptions", {}), dict)
        else None
    )
    if not isinstance(route_sub, dict):
        return None

    plan = _normalize_plan(route_sub.get("plan")) or (
        "pro" if bool((owner_data.get("trialStatus", {}).get("features", {}) or {}).get("multiRoute")) else "solo"
    )
    interval = _normalize_interval(route_sub.get("interval"))
    provider = str(route_sub.get("provider") or "stripe").strip() or "stripe"
    active = bool(route_sub.get("active"))
    features = _feature_payload_for_plan(plan)

    return BillingEntitlement(
        routeNumber=route_number,
        active=active,
        plan=plan,
        provider=provider,
        interval=interval,
        currentPeriodEndMs=_to_epoch_millis(route_sub.get("currentPeriodEnd")),
        source="legacy_user_subscription",
        updatedAtMs=_to_epoch_millis(owner_data.get("timestamps", {}).get("updatedAt")),
        features=features,
    )


def _coerce_trial_entitlement(route_number: str, owner_data: Dict[str, Any]) -> Optional[BillingEntitlement]:
    profile = owner_data.get("profile", {}) or {}
    primary_route = _normalize_route_number(profile.get("routeNumber"))
    if primary_route != route_number:
        return None

    trial_status = owner_data.get("trialStatus", {}) or {}
    trial_ends_at = trial_status.get("endsAt")
    ends_ms = _to_epoch_millis(trial_ends_at)
    if not ends_ms:
        return None

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if ends_ms <= now_ms:
        return None

    features = trial_status.get("features", {}) if isinstance(trial_status.get("features"), dict) else {}
    scanner_enabled = bool(features.get("scanner"))
    if not scanner_enabled:
        return None

    plan = "pro" if bool(features.get("multiRoute")) else "solo"
    return BillingEntitlement(
        routeNumber=route_number,
        active=True,
        plan=plan,
        provider="trial",
        interval=None,
        currentPeriodEndMs=ends_ms,
        source="trial",
        updatedAtMs=_to_epoch_millis(trial_status.get("startedAt")),
        features={
            "scanner": True,
            "managementDashboard": bool(features.get("managementDashboard")),
            "multiRoute": bool(features.get("multiRoute")),
            "ordering": True,
            "forecasting": True,
            "pcfEmailImport": True,
        },
    )


def _write_route_entitlement_from_legacy(
    *,
    db: firestore.Client,
    route_number: str,
    entitlement: BillingEntitlement,
    owner_uid: str,
) -> None:
    route_ref = db.collection("routeEntitlements").document(route_number)
    route_ref.set(
        {
            "active": entitlement.active,
            "plan": entitlement.plan,
            "provider": entitlement.provider,
            "interval": entitlement.interval,
            "currentPeriodEnd": (
                datetime.fromtimestamp(entitlement.currentPeriodEndMs / 1000, tz=timezone.utc)
                if entitlement.currentPeriodEndMs
                else None
            ),
            "features": entitlement.features,
            "ownerUid": owner_uid,
            "source": "legacy_backfill",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _build_correlation_id(request: Request) -> str:
    existing = request.headers.get("X-Correlation-ID")
    if existing:
        return existing
    return f"billing-{uuid4()}"


@router.get(
    "/billing/products",
    response_model=BillingProductsResponse,
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def get_billing_products(
    request: Request,
    platform: Literal["ios", "android"] = Query(default="ios"),
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> BillingProductsResponse:
    """Return IAP catalog products for the requested platform."""
    if route:
        await require_route_access(route, decoded_token, db)

    products: List[BillingProduct] = []
    for product_id, spec in IAP_PRODUCT_MAP.items():
        platforms = spec.get("platforms") or []
        if platform not in platforms:
            continue
        plan = str(spec["plan"])
        products.append(
            BillingProduct(
                productId=product_id,
                platform=platform,
                plan=plan,  # type: ignore[arg-type]
                interval=spec["interval"],
                displayName=spec["displayName"],
                description=spec["description"],
                features=_feature_payload_for_plan(plan),
            )
        )

    return BillingProductsResponse(ok=True, products=products)


@router.get(
    "/billing/entitlement",
    response_model=BillingEntitlementResponse,
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def get_billing_entitlement(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> BillingEntitlementResponse:
    """Resolve effective entitlement state for a route."""
    user_data = await require_route_access(route, decoded_token, db)

    # Route-level source of truth when available.
    ent_doc = db.collection("routeEntitlements").document(route).get()
    if ent_doc.exists:
        ent_data = ent_doc.to_dict() or {}
        entitlement = _coerce_entitlement_from_route_doc(route, ent_data)
        if entitlement:
            return BillingEntitlementResponse(ok=True, entitlement=entitlement)

    owner_uid = _resolve_owner_uid_for_route(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )
    owner_data: Dict[str, Any] = {}
    if owner_uid:
        owner_doc = db.collection("users").document(owner_uid).get()
        if owner_doc.exists:
            owner_data = owner_doc.to_dict() or {}

    if not owner_data:
        owner_data = user_data
        owner_uid = decoded_token["uid"]

    legacy_entitlement = _coerce_legacy_subscription_entitlement(route, owner_data)
    if legacy_entitlement:
        if legacy_entitlement.active:
            try:
                _write_route_entitlement_from_legacy(
                    db=db,
                    route_number=route,
                    entitlement=legacy_entitlement,
                    owner_uid=owner_uid,
                )
            except Exception as exc:
                logger.warning("Legacy entitlement backfill failed for route=%s: %s", route, exc)
        return BillingEntitlementResponse(ok=True, entitlement=legacy_entitlement)

    trial_entitlement = _coerce_trial_entitlement(route, owner_data)
    if trial_entitlement:
        return BillingEntitlementResponse(ok=True, entitlement=trial_entitlement)

    return BillingEntitlementResponse(
        ok=True,
        entitlement=BillingEntitlement(
            routeNumber=route,
            active=False,
            plan=None,
            provider=None,
            interval=None,
            currentPeriodEndMs=None,
            source="none",
            updatedAtMs=None,
            features={
                "scanner": False,
                "managementDashboard": False,
                "multiRoute": False,
                "ordering": False,
                "forecasting": False,
                "pcfEmailImport": False,
            },
        ),
    )


@router.post(
    "/billing/verify/apple",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        501: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def verify_apple_subscription(
    request: Request,
    payload: AppleVerifyRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    """Verify Apple purchase and update entitlements.

    This endpoint is intentionally explicit until server-side Apple verification
    credentials are configured in production.
    """
    correlation_id = _build_correlation_id(request)
    route = payload.routeNumber
    user_data = await require_route_access(route, decoded_token, db)

    if not _is_owner_for_route(user_data, route):
        return _error_response(
            403,
            error="Owner access required for purchase verification",
            code="OWNER_REQUIRED",
            details={"correlationId": correlation_id, "route": route},
        )

    if payload.productId not in IAP_PRODUCT_MAP:
        return _error_response(
            422,
            error="Unknown subscription product ID",
            code="UNKNOWN_PRODUCT_ID",
            details={"correlationId": correlation_id, "productId": payload.productId},
        )

    if not APPLE_BILLING_VERIFICATION_ENABLED:
        logger.warning(
            "Apple verify called while disabled: uid=%s route=%s product=%s corr=%s",
            decoded_token["uid"],
            route,
            payload.productId,
            correlation_id,
        )
        return _error_response(
            501,
            error="Apple verification is not configured on the server",
            code="APPLE_VERIFICATION_NOT_CONFIGURED",
            details={"correlationId": correlation_id},
        )

    return _error_response(
        501,
        error="Apple verification implementation pending server credentials integration",
        code="APPLE_VERIFICATION_NOT_IMPLEMENTED",
        details={"correlationId": correlation_id},
    )


@router.post(
    "/billing/restore/apple",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        501: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def restore_apple_subscription(
    request: Request,
    payload: AppleRestoreRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    """Restore Apple purchases for the route owner."""
    correlation_id = _build_correlation_id(request)
    route = payload.routeNumber
    user_data = await require_route_access(route, decoded_token, db)

    if not _is_owner_for_route(user_data, route):
        return _error_response(
            403,
            error="Owner access required for restore",
            code="OWNER_REQUIRED",
            details={"correlationId": correlation_id, "route": route},
        )

    if not APPLE_BILLING_VERIFICATION_ENABLED:
        logger.warning(
            "Apple restore called while disabled: uid=%s route=%s corr=%s",
            decoded_token["uid"],
            route,
            correlation_id,
        )
        return _error_response(
            501,
            error="Apple restore is not configured on the server",
            code="APPLE_RESTORE_NOT_CONFIGURED",
            details={"correlationId": correlation_id},
        )

    return _error_response(
        501,
        error="Apple restore implementation pending server credentials integration",
        code="APPLE_RESTORE_NOT_IMPLEMENTED",
        details={"correlationId": correlation_id},
    )

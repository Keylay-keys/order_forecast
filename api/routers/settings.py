"""
Settings API Router

User settings including store column order preferences.
"""
from datetime import datetime
import hashlib
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from google.cloud import firestore
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from typing import Any, Dict, List
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..dependencies import verify_firebase_token, get_firestore, require_route_access
from ..errors import StructuredApiError
from ..middleware.rate_limit import (
    rate_limit_portal_sap_order_read,
    rate_limit_portal_sap_order_write,
)
from ..models import normalize_sap_order
from ..portal_sap_order_store import (
    PortalSapOrderConflict,
    PortalSapOrderPreference,
    read_portal_sap_order,
    save_portal_sap_order,
)

router = APIRouter(prefix="/settings", tags=["settings"])
logger = logging.getLogger("api.portal_sap_order")
PORTAL_SAP_ORDER_CACHE_CONTROL = "private, no-store"
PORTAL_SAP_ORDER_MAX_ITEMS = 2000


class StoreOrderConfig(BaseModel):
    """Store order configuration per route."""
    config: Dict[str, List[str]]  # routeNumber -> list of storeIds


class StoreOrderResponse(BaseModel):
    """Response with store order config."""
    config: Dict[str, List[str]]
    updatedAt: str | None = None


class ProfileTimezoneRequest(BaseModel):
    timezone: str = Field(min_length=1, max_length=64)


class ProfileTimezoneResponse(BaseModel):
    timezone: str


class PortalSapOrderWriteRequest(BaseModel):
    sapOrder: List[str]
    expectedRevision: int = Field(strict=True, ge=0)

    @field_validator("sapOrder", mode="before")
    @classmethod
    def validate_sap_order(cls, value: Any) -> List[str]:
        normalized = normalize_sap_order(value, allow_none=False)
        assert normalized is not None
        return normalized

    model_config = ConfigDict(extra="forbid")


class PortalSapOrderResponse(BaseModel):
    routeNumber: str
    exists: bool
    sapOrder: List[str]
    revision: int = Field(ge=0)
    updatedAt: datetime | None = None


def _privacy_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _emit_portal_sap_order_event(
    request: Request,
    *,
    action: str,
    firebase_uid: str,
    route: str,
    found: bool | None = None,
    item_count: int | None = None,
    revision: int | None = None,
) -> None:
    event = {
        "event": "portal_sap_order",
        "action": action,
        "requestId": getattr(request.state, "request_id", "unassigned"),
        "actorHash": _privacy_hash(firebase_uid),
        "routeHash": _privacy_hash(route),
    }
    if found is not None:
        event["found"] = found
    if item_count is not None:
        event["itemCount"] = max(0, min(PORTAL_SAP_ORDER_MAX_ITEMS, item_count))
    if revision is not None:
        event["revision"] = max(0, revision)
    logger.info("portal_sap_order %s", json.dumps(event, sort_keys=True))


def _portal_response(
    route: str,
    preference: PortalSapOrderPreference | None,
    *,
    exists: bool,
) -> PortalSapOrderResponse:
    return PortalSapOrderResponse(
        routeNumber=route,
        exists=exists,
        sapOrder=preference.sap_order if preference else [],
        revision=preference.revision if preference else 0,
        updatedAt=preference.updated_at if preference else None,
    )


async def _parse_portal_write_request(request: Request) -> PortalSapOrderWriteRequest:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise StructuredApiError(
            415,
            "Content-Type must be application/json.",
            "PORTAL_SAP_ORDER_INVALID",
        )
    try:
        raw = await request.json()
    except Exception as exc:
        raise StructuredApiError(
            400,
            "Request body must be valid JSON.",
            "PORTAL_SAP_ORDER_INVALID",
        ) from exc

    if isinstance(raw, dict) and isinstance(raw.get("sapOrder"), list):
        received_items = len(raw["sapOrder"])
        if received_items > PORTAL_SAP_ORDER_MAX_ITEMS:
            raise StructuredApiError(
                422,
                "Saved SAP list limit exceeded.",
                "PORTAL_SAP_ORDER_INVALID",
                {
                    "maxItems": PORTAL_SAP_ORDER_MAX_ITEMS,
                    "receivedItems": received_items,
                },
            )
    try:
        return PortalSapOrderWriteRequest.model_validate(raw)
    except ValidationError as exc:
        raise StructuredApiError(
            422,
            "SAP order is invalid.",
            "PORTAL_SAP_ORDER_INVALID",
        ) from exc


def validate_iana_timezone(value: str) -> str:
    normalized = str(value or "").strip()
    try:
        ZoneInfo(normalized)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise StructuredApiError(
            422,
            "Select a valid business timezone.",
            "TIMEZONE_INVALID",
        ) from exc
    return normalized


@router.put("/profile/timezone", response_model=ProfileTimezoneResponse)
async def set_profile_timezone(
    body: ProfileTimezoneRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> ProfileTimezoneResponse:
    """Persist a server-validated IANA timezone on the caller's profile."""
    uid = str(decoded_token.get("uid") or "").strip()
    if not uid:
        raise StructuredApiError(401, "Authentication required.", "AUTH_REQUIRED")

    user_ref = db.collection("users").document(uid)
    user_snapshot = user_ref.get()
    if not user_snapshot.exists:
        raise StructuredApiError(404, "User profile not found.", "USER_PROFILE_NOT_FOUND")
    user_data = user_snapshot.to_dict() or {}
    if not isinstance(user_data.get("profile"), dict):
        raise StructuredApiError(409, "User profile is incomplete.", "USER_PROFILE_INCOMPLETE")

    timezone_name = validate_iana_timezone(body.timezone)
    user_ref.update({
        "profile.timezone": timezone_name,
        "timestamps.updatedAt": firestore.SERVER_TIMESTAMP,
    })
    return ProfileTimezoneResponse(timezone=timezone_name)


@router.get(
    "/sap-orders/portal",
    response_model=PortalSapOrderResponse,
    responses={
        401: {"description": "Authentication required"},
        403: {"description": "Route access denied"},
        503: {"description": "Preference store unavailable"},
    },
)
@rate_limit_portal_sap_order_read
async def get_portal_sap_order(
    request: Request,
    response: Response,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> PortalSapOrderResponse:
    """Read the caller's current Portal row order for one authorized route."""
    await require_route_access(route, decoded_token, db)
    firebase_uid = str(decoded_token.get("uid") or "").strip()
    if not firebase_uid:
        raise StructuredApiError(401, "Authentication required.", "AUTH_REQUIRED")
    try:
        stored = read_portal_sap_order(firebase_uid, route)
    except Exception as exc:
        logger.warning(
            "portal_sap_order_unavailable request_id=%s action=read error_type=%s",
            getattr(request.state, "request_id", "unassigned"),
            type(exc).__name__,
        )
        raise StructuredApiError(
            503,
            "Portal SAP order is temporarily unavailable.",
            "PORTAL_SAP_ORDER_UNAVAILABLE",
        ) from exc

    response.headers["Cache-Control"] = PORTAL_SAP_ORDER_CACHE_CONTROL
    preference = stored.preference
    _emit_portal_sap_order_event(
        request,
        action="read",
        firebase_uid=firebase_uid,
        route=route,
        found=stored.exists,
        item_count=len(preference.sap_order) if preference else 0,
        revision=preference.revision if preference else 0,
    )
    return _portal_response(route, preference, exists=stored.exists)


@router.put(
    "/sap-orders/portal",
    response_model=PortalSapOrderResponse,
    responses={
        400: {"description": "Malformed JSON"},
        401: {"description": "Authentication required"},
        403: {"description": "Route access denied"},
        409: {"description": "Revision conflict"},
        415: {"description": "Unsupported content type"},
        422: {"description": "Invalid SAP order"},
        503: {"description": "Preference store unavailable"},
    },
)
@rate_limit_portal_sap_order_write
async def put_portal_sap_order(
    request: Request,
    response: Response,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
) -> PortalSapOrderResponse:
    """CAS-replace or reset the caller's Portal row order for one route."""
    await require_route_access(route, decoded_token, db)
    firebase_uid = str(decoded_token.get("uid") or "").strip()
    if not firebase_uid:
        raise StructuredApiError(401, "Authentication required.", "AUTH_REQUIRED")
    body = await _parse_portal_write_request(request)

    try:
        committed = save_portal_sap_order(
            firebase_uid,
            route,
            body.sapOrder,
            body.expectedRevision,
        )
    except PortalSapOrderConflict as exc:
        current = exc.current
        _emit_portal_sap_order_event(
            request,
            action="conflict",
            firebase_uid=firebase_uid,
            route=route,
            found=current.revision > 0,
            item_count=len(current.sap_order),
            revision=current.revision,
        )
        raise StructuredApiError(
            409,
            "The saved SAP order changed in another session.",
            "PORTAL_SAP_ORDER_CONFLICT",
            {
                "routeNumber": route,
                "sapOrder": current.sap_order,
                "revision": current.revision,
                "updatedAt": current.updated_at.isoformat().replace("+00:00", "Z"),
            },
        ) from exc
    except Exception as exc:
        logger.warning(
            "portal_sap_order_unavailable request_id=%s action=write error_type=%s",
            getattr(request.state, "request_id", "unassigned"),
            type(exc).__name__,
        )
        raise StructuredApiError(
            503,
            "Portal SAP order is temporarily unavailable.",
            "PORTAL_SAP_ORDER_UNAVAILABLE",
        ) from exc

    response.headers["Cache-Control"] = PORTAL_SAP_ORDER_CACHE_CONTROL
    _emit_portal_sap_order_event(
        request,
        action="reset" if len(body.sapOrder) == 0 else "write",
        firebase_uid=firebase_uid,
        route=route,
        found=True,
        item_count=len(committed.sap_order),
        revision=committed.revision,
    )
    return _portal_response(route, committed, exists=True)


@router.get("/store-order", response_model=StoreOrderResponse)
async def get_store_order(
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> StoreOrderResponse:
    """Get the user's store column order preferences.
    
    Returns the order of store columns for spreadsheet views,
    keyed by route number.
    
    Falls back to empty config if user hasn't configured store order yet.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Get from Firestore: users/{uid} document, userSettings.storeOrder field
    user_doc = db.collection('users').document(uid).get()
    
    if not user_doc.exists:
        # User document doesn't exist - return empty config (fallback to default order)
        return StoreOrderResponse(config={})
    
    data = user_doc.to_dict()
    user_settings = data.get('userSettings', {})
    store_order = user_settings.get('storeOrder', {}) if user_settings else {}
    
    return StoreOrderResponse(
        config=store_order,
        updatedAt=None
    )


@router.put("/store-order", response_model=StoreOrderResponse)
async def set_store_order(
    body: StoreOrderConfig,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> StoreOrderResponse:
    """Set the user's store column order preferences.
    
    Stores the order of store columns for spreadsheet views,
    keyed by route number.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Save to Firestore: users/{uid} document, userSettings.storeOrder field
    user_ref = db.collection('users').document(uid)
    user_ref.update({
        'userSettings.storeOrder': body.config
    })
    
    return StoreOrderResponse(
        config=body.config,
        updatedAt=None
    )


@router.delete("/store-order/{route_number}")
async def clear_store_order(
    route_number: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> dict:
    """Clear the store order for a specific route.
    
    Reverts to default (alphabetical) order for that route.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Get current config from user document
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        return {"message": "No store order configured"}
    
    data = user_doc.to_dict()
    user_settings = data.get('userSettings', {})
    config = user_settings.get('storeOrder', {}) if user_settings else {}
    
    if route_number in config:
        del config[route_number]
        user_ref.update({
            'userSettings.storeOrder': config
        })
    
    return {"message": f"Store order cleared for route {route_number}"}

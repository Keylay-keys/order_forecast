"""Billing router - subscription catalog and entitlement reads.

This is the backend contract for mobile IAP wiring. Verification endpoints are
intentionally explicit when Apple server-side verification is not yet enabled.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from urllib import error as url_error
from urllib import parse as url_parse
from urllib import request as url_request
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
APPLE_ISSUER_ID = str(os.environ.get("APPLE_ISSUER_ID", "")).strip()
APPLE_KEY_ID = str(os.environ.get("APPLE_KEY_ID", "")).strip()
APPLE_BUNDLE_ID = str(os.environ.get("APPLE_BUNDLE_ID", "")).strip()
APPLE_PRIVATE_KEY = str(os.environ.get("APPLE_PRIVATE_KEY", "")).strip()
APPLE_API_TIMEOUT_SEC = float(os.environ.get("APPLE_API_TIMEOUT_SEC", "8"))
GOOGLE_BILLING_VERIFICATION_ENABLED = _bool_env("GOOGLE_BILLING_VERIFICATION_ENABLED", False)
GOOGLE_PLAY_PACKAGE_NAME = str(os.environ.get("GOOGLE_PLAY_PACKAGE_NAME", "")).strip()
GOOGLE_API_TIMEOUT_SEC = float(os.environ.get("GOOGLE_API_TIMEOUT_SEC", "8"))
ENTITLEMENT_PROVIDER_OVERRIDE = _bool_env("ENTITLEMENT_PROVIDER_OVERRIDE", False)
STRIPE_SECRET_KEY = str(os.environ.get("STRIPE_SECRET_KEY", "")).strip()
STRIPE_API_TIMEOUT_SEC = float(os.environ.get("STRIPE_API_TIMEOUT_SEC", "8"))
STRIPE_BILLING_PORTAL_RETURN_URL = str(
    os.environ.get("STRIPE_BILLING_PORTAL_RETURN_URL", "https://routespark.pro/subscription")
).strip()
STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS = tuple(
    origin.strip().rstrip("/")
    for origin in str(
        os.environ.get(
            "STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS",
            "https://routespark.pro,https://routespark-1f47d.web.app,http://localhost:3000",
        )
    ).split(",")
    if origin.strip()
)


def _normalize_route_number(value: Any) -> str:
    route = str(value or "").strip()
    return route if route.isdigit() and len(route) <= 10 else ""


def _parse_epoch_millis(*values: Any) -> Optional[int]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            as_int = int(value)
            return as_int if as_int > 1_000_000_000 else as_int * 1000
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            try:
                as_float = float(stripped)
                as_int = int(as_float)
                return as_int if as_int > 1_000_000_000 else as_int * 1000
            except Exception:
                continue
    return None


def _parse_rfc3339_millis(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    return int(dt.timestamp() * 1000)


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


def _decode_unverified_jws_payload(compact_jws: Optional[str]) -> Dict[str, Any]:
    token = str(compact_jws or "").strip()
    if not token:
        return {}
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload_segment = parts[1]
    pad_len = (-len(payload_segment)) % 4
    payload_padded = payload_segment + ("=" * pad_len)
    try:
        decoded = base64.urlsafe_b64decode(payload_padded.encode("utf-8"))
        parsed = json.loads(decoded.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


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


def _primary_route_for_user(user_data: Dict[str, Any]) -> str:
    profile = user_data.get("profile", {}) or {}
    return _normalize_route_number(profile.get("routeNumber"))


def _is_owner_role(user_data: Dict[str, Any]) -> bool:
    profile = user_data.get("profile", {}) or {}
    role = str(profile.get("role") or "").strip()
    return role in ("owner", "ownerOnly")


def _require_primary_owner_billing_route(
    *,
    user_data: Dict[str, Any],
    route_number: str,
    correlation_id: str,
) -> Optional[JSONResponse]:
    if not _is_owner_role(user_data):
        return _error_response(
            403,
            error="Only route owners can manage subscriptions",
            code="BILLING_OWNER_REQUIRED",
            details={"correlationId": correlation_id, "route": route_number},
        )

    primary_route = _primary_route_for_user(user_data)
    if not primary_route:
        return _error_response(
            403,
            error="Primary route is required for subscription management",
            code="BILLING_PRIMARY_ROUTE_MISSING",
            details={"correlationId": correlation_id, "route": route_number},
        )

    if route_number != primary_route:
        return _error_response(
            403,
            error="Use your primary route to manage subscriptions",
            code="BILLING_PRIMARY_ROUTE_REQUIRED",
            details={
                "correlationId": correlation_id,
                "route": route_number,
                "primaryRoute": primary_route,
            },
        )

    return None


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


class AppleVerificationError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        error: str,
        code: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(error)
        self.status_code = status_code
        self.error = error
        self.code = code
        self.details = details or {}


def _raise_apple_error(
    *,
    status_code: int,
    error: str,
    code: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    raise AppleVerificationError(
        status_code=status_code,
        error=error,
        code=code,
        details=details,
    )


def _apple_credentials_configured() -> bool:
    return bool(APPLE_ISSUER_ID and APPLE_KEY_ID and APPLE_BUNDLE_ID and APPLE_PRIVATE_KEY)


def _apple_private_key_material() -> str:
    return APPLE_PRIVATE_KEY.replace("\\n", "\n").strip()


def _build_apple_server_api_jwt() -> str:
    try:
        import jwt  # type: ignore
    except Exception as exc:
        _raise_apple_error(
            status_code=500,
            error="Apple verification runtime missing dependency",
            code="APPLE_VERIFICATION_RUNTIME_MISSING",
            details={"dependency": "PyJWT", "reason": str(exc)},
        )

    now = int(time.time())
    payload = {
        "iss": APPLE_ISSUER_ID,
        "iat": now,
        "exp": now + 300,
        "aud": "appstoreconnect-v1",
        "bid": APPLE_BUNDLE_ID,
    }
    headers = {"alg": "ES256", "kid": APPLE_KEY_ID, "typ": "JWT"}
    token = jwt.encode(payload, _apple_private_key_material(), algorithm="ES256", headers=headers)
    if isinstance(token, bytes):
        return token.decode("utf-8")
    return token


def _apple_api_base_url(environment: Literal["Sandbox", "Production"]) -> str:
    if environment == "Sandbox":
        return "https://api.storekit-sandbox.itunes.apple.com"
    return "https://api.storekit.itunes.apple.com"


def _apple_environment_order(environment_hint: Optional[Literal["Sandbox", "Production"]]) -> List[Literal["Sandbox", "Production"]]:
    if environment_hint == "Sandbox":
        return ["Sandbox", "Production"]
    if environment_hint == "Production":
        return ["Production", "Sandbox"]
    return ["Production", "Sandbox"]


def _apple_request_json(
    *,
    path: str,
    environment: Literal["Sandbox", "Production"],
    bearer_token: str,
) -> Dict[str, Any]:
    url = f"{_apple_api_base_url(environment)}{path}"
    req = url_request.Request(
        url=url,
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Accept": "application/json",
            "User-Agent": "routespark-web-api/1.0",
        },
    )
    try:
        with url_request.urlopen(req, timeout=APPLE_API_TIMEOUT_SEC) as resp:
            body = resp.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            if not isinstance(parsed, dict):
                _raise_apple_error(
                    status_code=502,
                    error="Invalid response from Apple verification API",
                    code="APPLE_INVALID_RESPONSE",
                    details={"environment": environment},
                )
            return parsed
    except url_error.HTTPError as http_exc:
        body_text = ""
        try:
            body_text = http_exc.read().decode("utf-8")
        except Exception:
            body_text = ""
        parsed_body: Dict[str, Any] = {}
        if body_text:
            try:
                parsed = json.loads(body_text)
                if isinstance(parsed, dict):
                    parsed_body = parsed
            except Exception:
                parsed_body = {}
        raise AppleVerificationError(
            status_code=http_exc.code if http_exc.code in (400, 401, 403, 404, 409, 422) else 502,
            error="Apple verification API request failed",
            code="APPLE_API_HTTP_ERROR",
            details={
                "environment": environment,
                "httpStatus": http_exc.code,
                "appleErrorCode": parsed_body.get("errorCode"),
                "appleErrorMessage": parsed_body.get("errorMessage"),
            },
        ) from http_exc
    except url_error.URLError as url_exc:
        _raise_apple_error(
            status_code=502,
            error="Unable to reach Apple verification API",
            code="APPLE_API_UNREACHABLE",
            details={"environment": environment, "reason": str(url_exc)},
        )


def _resolve_apple_transaction(
    *,
    transaction_id: str,
    environment_hint: Optional[Literal["Sandbox", "Production"]],
) -> Dict[str, Any]:
    bearer = _build_apple_server_api_jwt()
    env_order = _apple_environment_order(environment_hint)
    attempts: List[Dict[str, Any]] = []
    terminal_error: Optional[AppleVerificationError] = None
    for idx, env in enumerate(env_order):
        try:
            response = _apple_request_json(
                path=f"/inApps/v1/transactions/{url_parse.quote(transaction_id, safe='')}",
                environment=env,
                bearer_token=bearer,
            )
            return {"environment": env, "response": response}
        except AppleVerificationError as exc:
            attempts.append(
                {
                    "environment": env,
                    "statusCode": exc.status_code,
                    "code": exc.code,
                    "details": exc.details,
                }
            )
            # Keep trying cross-environment for lookup misses. Also retry 401
            # across environments when caller did not provide an explicit
            # environment hint. Apple often returns 401 in the wrong
            # environment for sandbox transactions.
            can_retry_cross_env = (
                idx < len(env_order) - 1
                and (
                    exc.status_code == 404
                    or (environment_hint is None and exc.status_code == 401)
                )
            )
            if can_retry_cross_env:
                continue
            terminal_error = exc
            break
    if terminal_error:
        status_codes = {int(a.get("statusCode") or 0) for a in attempts}
        # Mixed 401/404 across environments is treated as "not found" from the
        # caller perspective (credentials worked in at least one environment).
        if 404 in status_codes and status_codes.issubset({401, 404}):
            _raise_apple_error(
                status_code=422,
                error="Unable to resolve Apple transaction",
                code="APPLE_TRANSACTION_NOT_FOUND",
                details={"transactionId": transaction_id, "attempts": attempts},
            )
        _raise_apple_error(
            status_code=terminal_error.status_code,
            error=terminal_error.error,
            code=terminal_error.code,
            details={**(terminal_error.details or {}), "transactionId": transaction_id, "attempts": attempts},
        )
    _raise_apple_error(
        status_code=422,
        error="Unable to resolve Apple transaction",
        code="APPLE_TRANSACTION_NOT_FOUND",
        details={"transactionId": transaction_id, "attempts": attempts},
    )


def _build_entitlement_from_apple_transaction(
    *,
    route_number: str,
    expected_product_id: Optional[str],
    apple_payload: Dict[str, Any],
) -> Dict[str, Any]:
    signed_tx = str(apple_payload.get("signedTransactionInfo") or "").strip()
    signed_renewal = str(apple_payload.get("signedRenewalInfo") or "").strip()
    tx_data = _decode_unverified_jws_payload(signed_tx)
    renewal_data = _decode_unverified_jws_payload(signed_renewal)

    product_id = str(tx_data.get("productId") or expected_product_id or "").strip()
    if not product_id:
        _raise_apple_error(
            status_code=422,
            error="Apple transaction is missing productId",
            code="APPLE_PRODUCT_ID_MISSING",
        )
    if expected_product_id and product_id != expected_product_id:
        _raise_apple_error(
            status_code=422,
            error="Apple transaction product does not match requested product",
            code="APPLE_PRODUCT_ID_MISMATCH",
            details={"expectedProductId": expected_product_id, "receivedProductId": product_id},
        )
    if product_id not in IAP_PRODUCT_MAP:
        _raise_apple_error(
            status_code=422,
            error="Unknown subscription product ID from Apple transaction",
            code="UNKNOWN_PRODUCT_ID",
            details={"productId": product_id},
        )

    bundle_id = str(tx_data.get("bundleId") or "").strip()
    if APPLE_BUNDLE_ID and bundle_id and bundle_id != APPLE_BUNDLE_ID:
        _raise_apple_error(
            status_code=422,
            error="Apple transaction bundle ID mismatch",
            code="APPLE_BUNDLE_ID_MISMATCH",
            details={"expectedBundleId": APPLE_BUNDLE_ID, "receivedBundleId": bundle_id},
        )

    spec = IAP_PRODUCT_MAP[product_id]
    plan = str(spec["plan"])
    interval = str(spec["interval"])
    expires_ms = _parse_epoch_millis(
        tx_data.get("expiresDate"),
        tx_data.get("expiresDateMs"),
        tx_data.get("expires_date_ms"),
        renewal_data.get("expiresDate"),
    )
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    is_active = expires_ms is None or expires_ms > now_ms

    entitlement = BillingEntitlement(
        routeNumber=route_number,
        active=is_active,
        plan=plan,  # type: ignore[arg-type]
        provider="apple",
        interval=interval,  # type: ignore[arg-type]
        currentPeriodEndMs=expires_ms,
        source="route_entitlements",
        updatedAtMs=now_ms,
        features=_feature_payload_for_plan(plan),
    )
    meta = {
        "appStoreTransactionId": str(tx_data.get("transactionId") or "").strip() or None,
        "appleOriginalTransactionId": str(tx_data.get("originalTransactionId") or "").strip() or None,
        "signedTransactionInfo": signed_tx or None,
        "signedRenewalInfo": signed_renewal or None,
        "environment": str(apple_payload.get("environment") or "").strip() or None,
    }
    return {"entitlement": entitlement, "meta": meta}


# ---------------------------------------------------------------------------
# Google Play verification helpers
# ---------------------------------------------------------------------------


class GoogleVerificationError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        error: str,
        code: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(error)
        self.status_code = status_code
        self.error = error
        self.code = code
        self.details = details or {}


def _raise_google_error(
    *,
    status_code: int,
    error: str,
    code: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    raise GoogleVerificationError(
        status_code=status_code,
        error=error,
        code=code,
        details=details,
    )


def _google_package_name(explicit_package_name: Optional[str]) -> str:
    package_name = str(explicit_package_name or "").strip() or GOOGLE_PLAY_PACKAGE_NAME
    return package_name.strip()


def _build_google_access_token() -> str:
    try:
        import google.auth  # type: ignore
        from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
    except Exception as exc:
        _raise_google_error(
            status_code=500,
            error="Google verification runtime missing dependency",
            code="GOOGLE_VERIFICATION_RUNTIME_MISSING",
            details={"reason": str(exc)},
        )

    try:
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/androidpublisher"])
        if not credentials:
            _raise_google_error(
                status_code=501,
                error="Google verification credentials not configured",
                code="GOOGLE_VERIFICATION_CREDENTIALS_MISSING",
            )
        credentials.refresh(GoogleAuthRequest())
        token = str(getattr(credentials, "token", "") or "").strip()
        if not token:
            _raise_google_error(
                status_code=502,
                error="Failed to obtain Google access token",
                code="GOOGLE_ACCESS_TOKEN_EMPTY",
            )
        return token
    except GoogleVerificationError:
        raise
    except Exception as exc:
        _raise_google_error(
            status_code=501,
            error="Google verification credentials are unavailable",
            code="GOOGLE_VERIFICATION_CREDENTIALS_MISSING",
            details={"reason": str(exc)},
        )


def _google_request_json(
    *,
    path: str,
    bearer_token: str,
) -> Dict[str, Any]:
    url = f"https://androidpublisher.googleapis.com/androidpublisher/v3/{path}"
    req = url_request.Request(
        url=url,
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Accept": "application/json",
            "User-Agent": "routespark-web-api/1.0",
        },
    )
    try:
        with url_request.urlopen(req, timeout=GOOGLE_API_TIMEOUT_SEC) as resp:
            body = resp.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            if not isinstance(parsed, dict):
                _raise_google_error(
                    status_code=502,
                    error="Invalid response from Google Play verification API",
                    code="GOOGLE_INVALID_RESPONSE",
                )
            return parsed
    except url_error.HTTPError as http_exc:
        body_text = ""
        try:
            body_text = http_exc.read().decode("utf-8")
        except Exception:
            body_text = ""
        parsed_body: Dict[str, Any] = {}
        if body_text:
            try:
                parsed = json.loads(body_text)
                if isinstance(parsed, dict):
                    parsed_body = parsed
            except Exception:
                parsed_body = {}
        parsed_error = parsed_body.get("error") if isinstance(parsed_body.get("error"), dict) else {}
        status_code = http_exc.code
        if status_code == 404:
            status_code = 422
        elif status_code not in (400, 401, 403, 409, 422):
            status_code = 502
        raise GoogleVerificationError(
            status_code=status_code,
            error="Google Play verification API request failed",
            code="GOOGLE_API_HTTP_ERROR",
            details={
                "httpStatus": http_exc.code,
                "googleStatus": parsed_error.get("status"),
                "googleMessage": parsed_error.get("message"),
            },
        ) from http_exc
    except url_error.URLError as url_exc:
        _raise_google_error(
            status_code=502,
            error="Unable to reach Google Play verification API",
            code="GOOGLE_API_UNREACHABLE",
            details={"reason": str(url_exc)},
        )


def _resolve_google_subscription_purchase(
    *,
    package_name: str,
    purchase_token: str,
) -> Dict[str, Any]:
    bearer = _build_google_access_token()
    encoded_package = url_parse.quote(package_name, safe="")
    encoded_token = url_parse.quote(purchase_token, safe="")
    response = _google_request_json(
        path=f"applications/{encoded_package}/purchases/subscriptionsv2/tokens/{encoded_token}",
        bearer_token=bearer,
    )
    return {"packageName": package_name, "response": response}


def _build_entitlement_from_google_subscription(
    *,
    route_number: str,
    expected_product_id: Optional[str],
    purchase_token: str,
    google_payload: Dict[str, Any],
) -> Dict[str, Any]:
    line_items = google_payload.get("lineItems") if isinstance(google_payload.get("lineItems"), list) else []
    first_line = line_items[0] if line_items and isinstance(line_items[0], dict) else {}
    product_id = str(first_line.get("productId") or expected_product_id or "").strip()
    if not product_id:
        _raise_google_error(
            status_code=422,
            error="Google subscription payload is missing productId",
            code="GOOGLE_PRODUCT_ID_MISSING",
        )
    if expected_product_id and product_id != expected_product_id:
        _raise_google_error(
            status_code=422,
            error="Google subscription product does not match requested product",
            code="GOOGLE_PRODUCT_ID_MISMATCH",
            details={"expectedProductId": expected_product_id, "receivedProductId": product_id},
        )
    if product_id not in IAP_PRODUCT_MAP:
        _raise_google_error(
            status_code=422,
            error="Unknown subscription product ID from Google purchase",
            code="UNKNOWN_PRODUCT_ID",
            details={"productId": product_id},
        )

    spec = IAP_PRODUCT_MAP[product_id]
    plan = str(spec["plan"])
    interval = str(spec["interval"])

    subscription_state = str(google_payload.get("subscriptionState") or "").strip()
    expiry_ms = _parse_rfc3339_millis(first_line.get("expiryTime"))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    active_states = {"SUBSCRIPTION_STATE_ACTIVE", "SUBSCRIPTION_STATE_IN_GRACE_PERIOD"}
    is_active = subscription_state in active_states and (expiry_ms is None or expiry_ms > now_ms)

    entitlement = BillingEntitlement(
        routeNumber=route_number,
        active=is_active,
        plan=plan,  # type: ignore[arg-type]
        provider="google",
        interval=interval,  # type: ignore[arg-type]
        currentPeriodEndMs=expiry_ms,
        source="route_entitlements",
        updatedAtMs=now_ms,
        features=_feature_payload_for_plan(plan),
    )

    latest_order_id = str(google_payload.get("latestOrderId") or "").strip() or None
    linked_purchase_token = str(google_payload.get("linkedPurchaseToken") or "").strip() or None
    package_name = str(google_payload.get("packageName") or "").strip() or None
    meta = {
        "googlePurchaseToken": purchase_token,
        "googlePackageName": package_name,
        "googleOrderId": latest_order_id,
        "googleLinkedPurchaseToken": linked_purchase_token,
        "googleSubscriptionState": subscription_state or None,
    }
    return {"entitlement": entitlement, "meta": meta}


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
    resolvedFrom: Literal["route_entitlements", "legacy_subscription", "trial", "none"] = "none"
    isTrial: bool = False
    isPaidSubscription: bool = False
    displayState: Literal["trial_active", "subscribed", "none"] = "none"
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


class GoogleVerifyRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    productId: str = Field(..., min_length=3, max_length=255)
    purchaseToken: str = Field(..., min_length=10, max_length=4096)
    packageName: Optional[str] = Field(default=None, min_length=3, max_length=255)
    orderId: Optional[str] = Field(default=None, max_length=255)
    obfuscatedExternalAccountId: Optional[str] = Field(default=None, max_length=255)


class GoogleRestoreRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    purchaseToken: Optional[str] = Field(default=None, min_length=10, max_length=4096)
    packageName: Optional[str] = Field(default=None, min_length=3, max_length=255)


class StripePortalSessionRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    returnUrl: Optional[str] = Field(default=None, max_length=2048)


class StripePortalSessionResponse(BaseModel):
    ok: bool
    url: str


def _normalize_provider(value: Any) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    provider_aliases = {
        "stripe_webhook": "stripe",
        "legacy_backfill": "stripe",
        "web_stripe": "stripe",
        "web": "stripe",
        "app_store": "apple",
        "appstore": "apple",
        "ios": "apple",
        "google_play": "google",
        "play_store": "google",
        "play": "google",
        "android": "google",
    }
    return provider_aliases.get(normalized, normalized)


def _check_entitlement_provider_conflict(
    *,
    db: firestore.Client,
    route_number: str,
    incoming_provider: str,
) -> Dict[str, Any]:
    ent_doc = db.collection("routeEntitlements").document(route_number).get()
    if not ent_doc.exists:
        return {"conflict": False, "existingProvider": None, "existingActive": False, "reason": "missing"}

    ent_data = ent_doc.to_dict() or {}
    existing_active = bool(ent_data.get("active"))
    existing_provider = _normalize_provider(ent_data.get("provider")) or _normalize_provider(ent_data.get("source"))
    normalized_incoming = _normalize_provider(incoming_provider) or incoming_provider

    if not existing_active or not existing_provider or existing_provider == normalized_incoming:
        return {
            "conflict": False,
            "existingProvider": existing_provider,
            "existingActive": existing_active,
            "reason": "compatible",
        }

    if ENTITLEMENT_PROVIDER_OVERRIDE:
        return {
            "conflict": False,
            "existingProvider": existing_provider,
            "existingActive": existing_active,
            "reason": "override_enabled",
        }

    return {
        "conflict": True,
        "existingProvider": existing_provider,
        "existingActive": existing_active,
        "reason": "provider_conflict",
    }


def _log_entitlement_write_event(
    *,
    provider: str,
    route_number: str,
    owner_uid: str,
    action: str,
    entitlement: BillingEntitlement,
    correlation_id: Optional[str],
    source: str,
) -> None:
    logger.info(
        "EntitlementWrite provider=%s action=%s route=%s ownerUid=%s active=%s plan=%s interval=%s source=%s correlationId=%s",
        provider,
        action,
        route_number,
        owner_uid,
        entitlement.active,
        entitlement.plan,
        entitlement.interval,
        source,
        correlation_id,
    )


def _finalize_entitlement(
    entitlement: BillingEntitlement,
    *,
    resolved_from: Literal["route_entitlements", "legacy_subscription", "trial", "none"],
) -> BillingEntitlement:
    provider = _normalize_provider(entitlement.provider)
    source = entitlement.source
    is_trial = bool(entitlement.active and (source == "trial" or provider == "trial"))
    is_paid_subscription = bool(
        entitlement.active
        and not is_trial
        and (
            source in ("route_entitlements", "legacy_user_subscription")
            or provider in ("stripe", "apple", "google")
        )
    )

    entitlement.provider = provider
    entitlement.resolvedFrom = resolved_from
    entitlement.isTrial = is_trial
    entitlement.isPaidSubscription = is_paid_subscription
    entitlement.displayState = (
        "trial_active" if is_trial else "subscribed" if is_paid_subscription else "none"
    )
    logger.debug(
        "Resolved entitlement route=%s source=%s resolvedFrom=%s provider=%s displayState=%s",
        entitlement.routeNumber,
        entitlement.source,
        entitlement.resolvedFrom,
        entitlement.provider,
        entitlement.displayState,
    )
    return entitlement


def _coerce_entitlement_from_route_doc(
    route_number: str,
    doc_data: Dict[str, Any],
) -> Optional[BillingEntitlement]:
    plan = _normalize_plan(doc_data.get("plan"))
    interval = _normalize_interval(doc_data.get("interval"))
    active = bool(doc_data.get("active"))
    provider = _normalize_provider(doc_data.get("provider")) or _normalize_provider(doc_data.get("source"))
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
    provider = _normalize_provider(route_sub.get("provider")) or "stripe"
    current_period_end_ms = _to_epoch_millis(route_sub.get("currentPeriodEnd"))
    active = bool(route_sub.get("active"))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if active and current_period_end_ms is not None and current_period_end_ms <= now_ms:
        active = False
    if not active:
        return None
    features = _feature_payload_for_plan(plan)

    return BillingEntitlement(
        routeNumber=route_number,
        active=active,
        plan=plan,
        provider=provider,
        interval=interval,
        currentPeriodEndMs=current_period_end_ms,
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
            "source": _normalize_provider(entitlement.provider) or "legacy",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _write_route_entitlement_from_apple(
    *,
    db: firestore.Client,
    route_number: str,
    entitlement: BillingEntitlement,
    owner_uid: str,
    meta: Dict[str, Any],
) -> None:
    route_ref = db.collection("routeEntitlements").document(route_number)
    route_ref.set(
        {
            "active": entitlement.active,
            "plan": entitlement.plan,
            "provider": "apple",
            "interval": entitlement.interval,
            "currentPeriodEnd": (
                datetime.fromtimestamp(entitlement.currentPeriodEndMs / 1000, tz=timezone.utc)
                if entitlement.currentPeriodEndMs
                else None
            ),
            "features": entitlement.features,
            "ownerUid": owner_uid,
            "source": "apple_server_api",
            "appStoreTransactionId": meta.get("appStoreTransactionId"),
            "appleOriginalTransactionId": meta.get("appleOriginalTransactionId"),
            "appleEnvironment": meta.get("environment"),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _write_legacy_subscription_shadow_from_apple(
    *,
    db: firestore.Client,
    owner_uid: str,
    route_number: str,
    entitlement: BillingEntitlement,
    meta: Dict[str, Any],
) -> None:
    user_ref = db.collection("users").document(owner_uid)
    current_period_end = (
        datetime.fromtimestamp(entitlement.currentPeriodEndMs / 1000, tz=timezone.utc)
        if entitlement.currentPeriodEndMs
        else None
    )
    user_ref.set(
        {
            "subscriptions": {
                "routes": {
                    route_number: {
                        "active": entitlement.active,
                        "plan": entitlement.plan,
                        "provider": "apple",
                        "interval": entitlement.interval,
                        "status": "active" if entitlement.active else "inactive",
                        "currentPeriodEnd": current_period_end,
                        "appStoreTransactionId": meta.get("appStoreTransactionId"),
                        "appleOriginalTransactionId": meta.get("appleOriginalTransactionId"),
                        "updatedAt": firestore.SERVER_TIMESTAMP,
                    }
                }
            },
            "timestamps": {"updatedAt": firestore.SERVER_TIMESTAMP},
        },
        merge=True,
    )


def _write_route_entitlement_from_google(
    *,
    db: firestore.Client,
    route_number: str,
    entitlement: BillingEntitlement,
    owner_uid: str,
    meta: Dict[str, Any],
) -> None:
    route_ref = db.collection("routeEntitlements").document(route_number)
    route_ref.set(
        {
            "active": entitlement.active,
            "plan": entitlement.plan,
            "provider": "google",
            "interval": entitlement.interval,
            "currentPeriodEnd": (
                datetime.fromtimestamp(entitlement.currentPeriodEndMs / 1000, tz=timezone.utc)
                if entitlement.currentPeriodEndMs
                else None
            ),
            "features": entitlement.features,
            "ownerUid": owner_uid,
            "source": "google_play_api",
            "googlePurchaseToken": meta.get("googlePurchaseToken"),
            "googlePackageName": meta.get("googlePackageName"),
            "googleOrderId": meta.get("googleOrderId"),
            "googleLinkedPurchaseToken": meta.get("googleLinkedPurchaseToken"),
            "googleSubscriptionState": meta.get("googleSubscriptionState"),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _write_legacy_subscription_shadow_from_google(
    *,
    db: firestore.Client,
    owner_uid: str,
    route_number: str,
    entitlement: BillingEntitlement,
    meta: Dict[str, Any],
) -> None:
    user_ref = db.collection("users").document(owner_uid)
    current_period_end = (
        datetime.fromtimestamp(entitlement.currentPeriodEndMs / 1000, tz=timezone.utc)
        if entitlement.currentPeriodEndMs
        else None
    )
    user_ref.set(
        {
            "subscriptions": {
                "routes": {
                    route_number: {
                        "active": entitlement.active,
                        "plan": entitlement.plan,
                        "provider": "google",
                        "interval": entitlement.interval,
                        "status": "active" if entitlement.active else "inactive",
                        "currentPeriodEnd": current_period_end,
                        "googlePurchaseToken": meta.get("googlePurchaseToken"),
                        "googleOrderId": meta.get("googleOrderId"),
                        "updatedAt": firestore.SERVER_TIMESTAMP,
                    }
                }
            },
            "timestamps": {"updatedAt": firestore.SERVER_TIMESTAMP},
        },
        merge=True,
    )


def _resolve_owner_uid_for_billing_write(
    *,
    db: firestore.Client,
    route_number: str,
    requester_uid: str,
    requester_data: Dict[str, Any],
) -> str:
    owner_uid = _resolve_owner_uid_for_route(
        db=db,
        route_number=route_number,
        requester_uid=requester_uid,
        requester_data=requester_data,
    )
    return owner_uid or requester_uid


def _pick_restore_transaction_id(
    *,
    route_number: str,
    payload: AppleRestoreRequest,
    requester_data: Dict[str, Any],
    db: firestore.Client,
) -> Optional[str]:
    if payload.originalTransactionId:
        return payload.originalTransactionId.strip()

    ent_doc = db.collection("routeEntitlements").document(route_number).get()
    if ent_doc.exists:
        ent_data = ent_doc.to_dict() or {}
        ent_tx = str(
            ent_data.get("appStoreTransactionId")
            or ent_data.get("appleOriginalTransactionId")
            or ""
        ).strip()
        if ent_tx:
            return ent_tx

    route_sub = (
        requester_data.get("subscriptions", {})
        .get("routes", {})
        .get(route_number)
        if isinstance(requester_data.get("subscriptions", {}), dict)
        else None
    )
    if isinstance(route_sub, dict):
        legacy_tx = str(
            route_sub.get("appStoreTransactionId")
            or route_sub.get("appleOriginalTransactionId")
            or route_sub.get("originalTransactionId")
            or ""
        ).strip()
        if legacy_tx:
            return legacy_tx

    return None


def _pick_google_restore_purchase_token(
    *,
    route_number: str,
    payload: GoogleRestoreRequest,
    requester_uid: str,
    requester_data: Dict[str, Any],
    db: firestore.Client,
) -> Optional[str]:
    if payload.purchaseToken:
        return payload.purchaseToken.strip()

    ent_doc = db.collection("routeEntitlements").document(route_number).get()
    if ent_doc.exists:
        ent_data = ent_doc.to_dict() or {}
        ent_token = str(ent_data.get("googlePurchaseToken") or ent_data.get("purchaseToken") or "").strip()
        if ent_token:
            return ent_token

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
            route_sub = (
                owner_data.get("subscriptions", {})
                .get("routes", {})
                .get(route_number)
                if isinstance(owner_data.get("subscriptions", {}), dict)
                else None
            )
            if isinstance(route_sub, dict):
                token = str(route_sub.get("googlePurchaseToken") or route_sub.get("purchaseToken") or "").strip()
                if token:
                    return token

    requester_sub = (
        requester_data.get("subscriptions", {})
        .get("routes", {})
        .get(route_number)
        if isinstance(requester_data.get("subscriptions", {}), dict)
        else None
    )
    if isinstance(requester_sub, dict):
        token = str(requester_sub.get("googlePurchaseToken") or requester_sub.get("purchaseToken") or "").strip()
        if token:
            return token

    return None


def _build_correlation_id(request: Request) -> str:
    existing = request.headers.get("X-Correlation-ID")
    if existing:
        return existing
    return f"billing-{uuid4()}"


def _is_allowed_return_url(url: str) -> bool:
    try:
        parsed = url_parse.urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("https", "http"):
        return False
    if not parsed.netloc:
        return False
    origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return origin in STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS


def _build_default_stripe_return_url() -> str:
    configured = STRIPE_BILLING_PORTAL_RETURN_URL.strip()
    if configured and _is_allowed_return_url(configured):
        return configured
    if STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS:
        return f"{STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS[0]}/subscription"
    return "https://routespark.pro/subscription"


def _resolve_stripe_portal_return_url(*, request: Request, payload_return_url: Optional[str]) -> str:
    candidate = str(payload_return_url or "").strip()
    if candidate and _is_allowed_return_url(candidate):
        return candidate

    origin = str(request.headers.get("origin") or "").strip().rstrip("/")
    if origin and origin in STRIPE_BILLING_PORTAL_ALLOWED_RETURN_ORIGINS:
        return f"{origin}/subscription"

    return _build_default_stripe_return_url()


def _stripe_api_request_form(
    *,
    method: str,
    path: str,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    if not STRIPE_SECRET_KEY:
        _raise_apple_error(
            status_code=501,
            error="Stripe billing portal is not configured",
            code="STRIPE_PORTAL_NOT_CONFIGURED",
            details={"required": ["STRIPE_SECRET_KEY"]},
        )

    body = url_parse.urlencode(
        {k: str(v) for k, v in data.items() if v is not None},
        quote_via=url_parse.quote,
    ).encode("utf-8")
    req = url_request.Request(
        url=f"https://api.stripe.com/v1/{path.lstrip('/')}",
        method=method.upper(),
        data=body if method.upper() in ("POST", "PUT", "PATCH") else None,
        headers={
            "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": "routespark-web-api/1.0",
        },
    )
    try:
        with url_request.urlopen(req, timeout=STRIPE_API_TIMEOUT_SEC) as resp:
            body_text = resp.read().decode("utf-8")
            parsed = json.loads(body_text) if body_text else {}
            if not isinstance(parsed, dict):
                return {}
            return parsed
    except url_error.HTTPError as http_exc:
        response_body = ""
        try:
            response_body = http_exc.read().decode("utf-8")
        except Exception:
            response_body = ""
        details: Dict[str, Any] = {"httpStatus": http_exc.code}
        if response_body:
            try:
                parsed_error = json.loads(response_body)
                if isinstance(parsed_error, dict):
                    stripe_error = parsed_error.get("error") if isinstance(parsed_error.get("error"), dict) else {}
                    if stripe_error:
                        details["stripeCode"] = stripe_error.get("code")
                        details["stripeMessage"] = stripe_error.get("message")
                        details["stripeType"] = stripe_error.get("type")
            except Exception:
                pass
        raise AppleVerificationError(
            status_code=502 if http_exc.code >= 500 else 422,
            error="Stripe API request failed",
            code="STRIPE_API_HTTP_ERROR",
            details=details,
        ) from http_exc
    except url_error.URLError as url_exc:
        raise AppleVerificationError(
            status_code=502,
            error="Unable to reach Stripe API",
            code="STRIPE_API_UNREACHABLE",
            details={"reason": str(url_exc)},
        ) from url_exc


def _resolve_stripe_customer_id(route_sub: Dict[str, Any]) -> Optional[str]:
    customer_id = str(route_sub.get("stripeCustomerId") or "").strip()
    if customer_id:
        return customer_id

    subscription_id = str(route_sub.get("stripeSubscriptionId") or "").strip()
    if not subscription_id:
        return None

    stripe_sub = _stripe_api_request_form(
        method="GET",
        path=f"subscriptions/{url_parse.quote(subscription_id, safe='')}",
        data={},
    )
    sub_customer = str(stripe_sub.get("customer") or "").strip()
    return sub_customer or None


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
        user_data = await require_route_access(route, decoded_token, db)
        gate_error = _require_primary_owner_billing_route(
            user_data=user_data,
            route_number=route,
            correlation_id=_build_correlation_id(request),
        )
        if gate_error:
            return gate_error

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
    # Priority chain (explicit): active routeEntitlements -> active legacy subscription -> active trial -> none.
    user_data = await require_route_access(route, decoded_token, db)

    # Route-level source of truth when available.
    ent_doc = db.collection("routeEntitlements").document(route).get()
    route_doc_entitlement: Optional[BillingEntitlement] = None
    if ent_doc.exists:
        ent_data = ent_doc.to_dict() or {}
        route_doc_entitlement = _coerce_entitlement_from_route_doc(route, ent_data)
        if route_doc_entitlement and route_doc_entitlement.active:
            return BillingEntitlementResponse(
                ok=True,
                entitlement=_finalize_entitlement(route_doc_entitlement, resolved_from="route_entitlements"),
            )

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
        try:
            _write_route_entitlement_from_legacy(
                db=db,
                route_number=route,
                entitlement=legacy_entitlement,
                owner_uid=owner_uid,
            )
        except Exception as exc:
            logger.warning("Legacy entitlement backfill failed for route=%s: %s", route, exc)
        return BillingEntitlementResponse(
            ok=True,
            entitlement=_finalize_entitlement(legacy_entitlement, resolved_from="legacy_subscription"),
        )

    trial_entitlement = _coerce_trial_entitlement(route, owner_data)
    if trial_entitlement:
        return BillingEntitlementResponse(
            ok=True,
            entitlement=_finalize_entitlement(trial_entitlement, resolved_from="trial"),
        )

    if route_doc_entitlement:
        return BillingEntitlementResponse(
            ok=True,
            entitlement=_finalize_entitlement(route_doc_entitlement, resolved_from="route_entitlements"),
        )

    return BillingEntitlementResponse(
        ok=True,
        entitlement=_finalize_entitlement(
            BillingEntitlement(
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
            resolved_from="none",
        ),
    )


@router.post(
    "/billing/stripe/portal",
    response_model=StripePortalSessionResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        501: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_stripe_billing_portal_session(
    request: Request,
    payload: StripePortalSessionRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> StripePortalSessionResponse:
    correlation_id = _build_correlation_id(request)
    route = payload.routeNumber
    requester_data = await require_route_access(route, decoded_token, db)
    gate_error = _require_primary_owner_billing_route(
        user_data=requester_data,
        route_number=route,
        correlation_id=correlation_id,
    )
    if gate_error:
        return gate_error

    owner_uid = _resolve_owner_uid_for_billing_write(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=requester_data,
    )
    owner_doc = db.collection("users").document(owner_uid).get()
    if not owner_doc.exists:
        return _error_response(
            404,
            error="Owner record not found",
            code="OWNER_RECORD_NOT_FOUND",
            details={"routeNumber": route, "ownerUid": owner_uid, "correlationId": correlation_id},
        )

    owner_data = owner_doc.to_dict() or {}
    route_sub = (
        owner_data.get("subscriptions", {})
        .get("routes", {})
        .get(route)
        if isinstance(owner_data.get("subscriptions", {}), dict)
        else None
    )
    if not isinstance(route_sub, dict):
        return _error_response(
            409,
            error="No Stripe subscription found for this route",
            code="STRIPE_SUBSCRIPTION_NOT_FOUND",
            details={"routeNumber": route, "correlationId": correlation_id},
        )

    provider = _normalize_provider(route_sub.get("provider")) or "stripe"
    if provider != "stripe":
        return _error_response(
            409,
            error="Active entitlement is managed by another provider",
            code="ENTITLEMENT_PROVIDER_CONFLICT",
            details={
                "routeNumber": route,
                "existingProvider": provider,
                "incomingProvider": "stripe",
                "correlationId": correlation_id,
            },
        )

    if not bool(route_sub.get("active")):
        return _error_response(
            409,
            error="Stripe subscription is not active for this route",
            code="STRIPE_SUBSCRIPTION_INACTIVE",
            details={"routeNumber": route, "correlationId": correlation_id},
        )

    try:
        customer_id = _resolve_stripe_customer_id(route_sub)
        if not customer_id:
            return _error_response(
                409,
                error="Stripe customer ID missing for this route",
                code="STRIPE_CUSTOMER_ID_MISSING",
                details={"routeNumber": route, "correlationId": correlation_id},
            )

        return_url = _resolve_stripe_portal_return_url(
            request=request,
            payload_return_url=payload.returnUrl,
        )
        session = _stripe_api_request_form(
            method="POST",
            path="billing_portal/sessions",
            data={"customer": customer_id, "return_url": return_url},
        )
        portal_url = str(session.get("url") or "").strip()
        if not portal_url:
            return _error_response(
                502,
                error="Stripe portal session did not return a URL",
                code="STRIPE_PORTAL_URL_MISSING",
                details={"routeNumber": route, "correlationId": correlation_id},
            )
        return StripePortalSessionResponse(ok=True, url=portal_url)
    except AppleVerificationError as exc:
        return _error_response(
            exc.status_code,
            error=exc.error,
            code=exc.code,
            details={**(exc.details or {}), "routeNumber": route, "correlationId": correlation_id},
        )
    except Exception as exc:
        logger.exception("Stripe portal session creation failed route=%s corr=%s", route, correlation_id)
        return _error_response(
            500,
            error="Failed to create Stripe portal session",
            code="STRIPE_PORTAL_CREATE_FAILED",
            details={"reason": str(exc), "routeNumber": route, "correlationId": correlation_id},
        )


@router.post(
    "/billing/verify/apple",
    response_model=BillingEntitlementResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
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
    gate_error = _require_primary_owner_billing_route(
        user_data=user_data,
        route_number=route,
        correlation_id=correlation_id,
    )
    if gate_error:
        return gate_error

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

    if not _apple_credentials_configured():
        return _error_response(
            501,
            error="Apple verification is enabled but credentials are incomplete",
            code="APPLE_VERIFICATION_CREDENTIALS_MISSING",
            details={
                "correlationId": correlation_id,
                "required": ["APPLE_ISSUER_ID", "APPLE_KEY_ID", "APPLE_BUNDLE_ID", "APPLE_PRIVATE_KEY"],
            },
        )

    request_tx_data = _decode_unverified_jws_payload(payload.signedTransactionInfo)
    tx_id = str(
        payload.transactionId
        or request_tx_data.get("transactionId")
        or payload.originalTransactionId
        or request_tx_data.get("originalTransactionId")
        or ""
    ).strip()
    if not tx_id:
        return _error_response(
            422,
            error="Apple verification requires transactionId (or signedTransactionInfo with transactionId)",
            code="APPLE_TRANSACTION_ID_REQUIRED",
            details={"correlationId": correlation_id},
        )

    try:
        apple_lookup = _resolve_apple_transaction(
            transaction_id=tx_id,
            environment_hint=payload.environment,
        )
        apple_response = apple_lookup.get("response") or {}
        if not isinstance(apple_response, dict):
            apple_response = {}
        apple_response["environment"] = apple_lookup.get("environment")
        built = _build_entitlement_from_apple_transaction(
            route_number=route,
            expected_product_id=payload.productId,
            apple_payload=apple_response,
        )
        entitlement: BillingEntitlement = built["entitlement"]
        meta: Dict[str, Any] = built["meta"]
    except AppleVerificationError as exc:
        return _error_response(
            exc.status_code,
            error=exc.error,
            code=exc.code,
            details={"correlationId": correlation_id, **(exc.details or {})},
        )

    owner_uid = _resolve_owner_uid_for_billing_write(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )
    provider_gate = _check_entitlement_provider_conflict(
        db=db,
        route_number=route,
        incoming_provider="apple",
    )
    if provider_gate.get("conflict"):
        logger.warning(
            "Entitlement provider conflict route=%s incoming=%s existing=%s corr=%s",
            route,
            "apple",
            provider_gate.get("existingProvider"),
            correlation_id,
        )
        return _error_response(
            409,
            error="Active entitlement is managed by another provider",
            code="ENTITLEMENT_PROVIDER_CONFLICT",
            details={
                "correlationId": correlation_id,
                "route": route,
                "incomingProvider": "apple",
                "existingProvider": provider_gate.get("existingProvider"),
                "overrideEnabled": ENTITLEMENT_PROVIDER_OVERRIDE,
            },
        )
    try:
        _write_route_entitlement_from_apple(
            db=db,
            route_number=route,
            entitlement=entitlement,
            owner_uid=owner_uid,
            meta=meta,
        )
        _write_legacy_subscription_shadow_from_apple(
            db=db,
            owner_uid=owner_uid,
            route_number=route,
            entitlement=entitlement,
            meta=meta,
        )
    except Exception as exc:
        logger.exception(
            "Apple verification write failure route=%s uid=%s corr=%s: %s",
            route,
            owner_uid,
            correlation_id,
            exc,
        )
        return _error_response(
            500,
            error="Failed to persist Apple entitlement",
            code="APPLE_ENTITLEMENT_WRITE_FAILED",
            details={"correlationId": correlation_id},
        )
    _log_entitlement_write_event(
        provider="apple",
        route_number=route,
        owner_uid=owner_uid,
        action="verify",
        entitlement=entitlement,
        correlation_id=correlation_id,
        source="apple_server_api",
    )

    return BillingEntitlementResponse(
        ok=True,
        entitlement=_finalize_entitlement(entitlement, resolved_from="route_entitlements"),
    )


@router.post(
    "/billing/restore/apple",
    response_model=BillingEntitlementResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
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
    gate_error = _require_primary_owner_billing_route(
        user_data=user_data,
        route_number=route,
        correlation_id=correlation_id,
    )
    if gate_error:
        return gate_error

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

    if not _apple_credentials_configured():
        return _error_response(
            501,
            error="Apple restore is enabled but credentials are incomplete",
            code="APPLE_RESTORE_CREDENTIALS_MISSING",
            details={
                "correlationId": correlation_id,
                "required": ["APPLE_ISSUER_ID", "APPLE_KEY_ID", "APPLE_BUNDLE_ID", "APPLE_PRIVATE_KEY"],
            },
        )

    tx_id = _pick_restore_transaction_id(
        route_number=route,
        payload=payload,
        requester_data=user_data,
        db=db,
    )
    if not tx_id:
        return _error_response(
            422,
            error="No Apple transaction reference available for restore",
            code="APPLE_RESTORE_TRANSACTION_ID_MISSING",
            details={"correlationId": correlation_id},
        )

    try:
        apple_lookup = _resolve_apple_transaction(
            transaction_id=tx_id,
            environment_hint=None,
        )
        apple_response = apple_lookup.get("response") or {}
        if not isinstance(apple_response, dict):
            apple_response = {}
        apple_response["environment"] = apple_lookup.get("environment")
        built = _build_entitlement_from_apple_transaction(
            route_number=route,
            expected_product_id=None,
            apple_payload=apple_response,
        )
        entitlement: BillingEntitlement = built["entitlement"]
        meta: Dict[str, Any] = built["meta"]
    except AppleVerificationError as exc:
        return _error_response(
            exc.status_code,
            error=exc.error,
            code=exc.code,
            details={"correlationId": correlation_id, **(exc.details or {})},
        )

    owner_uid = _resolve_owner_uid_for_billing_write(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )
    provider_gate = _check_entitlement_provider_conflict(
        db=db,
        route_number=route,
        incoming_provider="apple",
    )
    if provider_gate.get("conflict"):
        logger.warning(
            "Entitlement provider conflict route=%s incoming=%s existing=%s corr=%s",
            route,
            "apple",
            provider_gate.get("existingProvider"),
            correlation_id,
        )
        return _error_response(
            409,
            error="Active entitlement is managed by another provider",
            code="ENTITLEMENT_PROVIDER_CONFLICT",
            details={
                "correlationId": correlation_id,
                "route": route,
                "incomingProvider": "apple",
                "existingProvider": provider_gate.get("existingProvider"),
                "overrideEnabled": ENTITLEMENT_PROVIDER_OVERRIDE,
            },
        )
    try:
        _write_route_entitlement_from_apple(
            db=db,
            route_number=route,
            entitlement=entitlement,
            owner_uid=owner_uid,
            meta=meta,
        )
        _write_legacy_subscription_shadow_from_apple(
            db=db,
            owner_uid=owner_uid,
            route_number=route,
            entitlement=entitlement,
            meta=meta,
        )
    except Exception as exc:
        logger.exception(
            "Apple restore write failure route=%s uid=%s corr=%s: %s",
            route,
            owner_uid,
            correlation_id,
            exc,
        )
        return _error_response(
            500,
            error="Failed to persist restored Apple entitlement",
            code="APPLE_RESTORE_WRITE_FAILED",
            details={"correlationId": correlation_id},
        )
    _log_entitlement_write_event(
        provider="apple",
        route_number=route,
        owner_uid=owner_uid,
        action="restore",
        entitlement=entitlement,
        correlation_id=correlation_id,
        source="apple_server_api",
    )

    return BillingEntitlementResponse(
        ok=True,
        entitlement=_finalize_entitlement(entitlement, resolved_from="route_entitlements"),
    )


@router.post(
    "/billing/verify/google",
    response_model=BillingEntitlementResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
        501: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def verify_google_subscription(
    request: Request,
    payload: GoogleVerifyRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    """Verify Google Play subscription purchase and update entitlements."""
    correlation_id = _build_correlation_id(request)
    route = payload.routeNumber
    user_data = await require_route_access(route, decoded_token, db)
    gate_error = _require_primary_owner_billing_route(
        user_data=user_data,
        route_number=route,
        correlation_id=correlation_id,
    )
    if gate_error:
        return gate_error

    if payload.productId not in IAP_PRODUCT_MAP:
        return _error_response(
            422,
            error="Unknown subscription product ID",
            code="UNKNOWN_PRODUCT_ID",
            details={"correlationId": correlation_id, "productId": payload.productId},
        )

    if not GOOGLE_BILLING_VERIFICATION_ENABLED:
        logger.warning(
            "Google verify called while disabled: uid=%s route=%s product=%s corr=%s",
            decoded_token["uid"],
            route,
            payload.productId,
            correlation_id,
        )
        return _error_response(
            501,
            error="Google verification is not configured on the server",
            code="GOOGLE_VERIFICATION_NOT_CONFIGURED",
            details={"correlationId": correlation_id},
        )

    package_name = _google_package_name(payload.packageName)
    if not package_name:
        return _error_response(
            501,
            error="Google verification package is not configured on the server",
            code="GOOGLE_PACKAGE_MISSING",
            details={"correlationId": correlation_id, "required": ["GOOGLE_PLAY_PACKAGE_NAME"]},
        )

    purchase_token = payload.purchaseToken.strip()
    if not purchase_token:
        return _error_response(
            422,
            error="Google verification requires purchaseToken",
            code="GOOGLE_PURCHASE_TOKEN_REQUIRED",
            details={"correlationId": correlation_id},
        )

    try:
        google_lookup = _resolve_google_subscription_purchase(
            package_name=package_name,
            purchase_token=purchase_token,
        )
        google_response = google_lookup.get("response") or {}
        if not isinstance(google_response, dict):
            google_response = {}
        google_response["packageName"] = google_lookup.get("packageName")
        built = _build_entitlement_from_google_subscription(
            route_number=route,
            expected_product_id=payload.productId,
            purchase_token=purchase_token,
            google_payload=google_response,
        )
        entitlement: BillingEntitlement = built["entitlement"]
        meta: Dict[str, Any] = built["meta"]
    except GoogleVerificationError as exc:
        return _error_response(
            exc.status_code,
            error=exc.error,
            code=exc.code,
            details={"correlationId": correlation_id, **(exc.details or {})},
        )

    owner_uid = _resolve_owner_uid_for_billing_write(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )
    provider_gate = _check_entitlement_provider_conflict(
        db=db,
        route_number=route,
        incoming_provider="google",
    )
    if provider_gate.get("conflict"):
        logger.warning(
            "Entitlement provider conflict route=%s incoming=%s existing=%s corr=%s",
            route,
            "google",
            provider_gate.get("existingProvider"),
            correlation_id,
        )
        return _error_response(
            409,
            error="Active entitlement is managed by another provider",
            code="ENTITLEMENT_PROVIDER_CONFLICT",
            details={
                "correlationId": correlation_id,
                "route": route,
                "incomingProvider": "google",
                "existingProvider": provider_gate.get("existingProvider"),
                "overrideEnabled": ENTITLEMENT_PROVIDER_OVERRIDE,
            },
        )
    try:
        _write_route_entitlement_from_google(
            db=db,
            route_number=route,
            entitlement=entitlement,
            owner_uid=owner_uid,
            meta=meta,
        )
        _write_legacy_subscription_shadow_from_google(
            db=db,
            owner_uid=owner_uid,
            route_number=route,
            entitlement=entitlement,
            meta=meta,
        )
    except Exception as exc:
        logger.exception(
            "Google verification write failure route=%s uid=%s corr=%s: %s",
            route,
            owner_uid,
            correlation_id,
            exc,
        )
        return _error_response(
            500,
            error="Failed to persist Google entitlement",
            code="GOOGLE_ENTITLEMENT_WRITE_FAILED",
            details={"correlationId": correlation_id},
        )
    _log_entitlement_write_event(
        provider="google",
        route_number=route,
        owner_uid=owner_uid,
        action="verify",
        entitlement=entitlement,
        correlation_id=correlation_id,
        source="google_play_api",
    )

    return BillingEntitlementResponse(
        ok=True,
        entitlement=_finalize_entitlement(entitlement, resolved_from="route_entitlements"),
    )


@router.post(
    "/billing/restore/google",
    response_model=BillingEntitlementResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
        501: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def restore_google_subscription(
    request: Request,
    payload: GoogleRestoreRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
):
    """Restore Google Play subscription entitlement for the route owner."""
    correlation_id = _build_correlation_id(request)
    route = payload.routeNumber
    user_data = await require_route_access(route, decoded_token, db)
    gate_error = _require_primary_owner_billing_route(
        user_data=user_data,
        route_number=route,
        correlation_id=correlation_id,
    )
    if gate_error:
        return gate_error

    if not GOOGLE_BILLING_VERIFICATION_ENABLED:
        logger.warning(
            "Google restore called while disabled: uid=%s route=%s corr=%s",
            decoded_token["uid"],
            route,
            correlation_id,
        )
        return _error_response(
            501,
            error="Google restore is not configured on the server",
            code="GOOGLE_RESTORE_NOT_CONFIGURED",
            details={"correlationId": correlation_id},
        )

    package_name = _google_package_name(payload.packageName)
    if not package_name:
        return _error_response(
            501,
            error="Google restore package is not configured on the server",
            code="GOOGLE_PACKAGE_MISSING",
            details={"correlationId": correlation_id, "required": ["GOOGLE_PLAY_PACKAGE_NAME"]},
        )

    purchase_token = _pick_google_restore_purchase_token(
        route_number=route,
        payload=payload,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
        db=db,
    )
    if not purchase_token:
        return _error_response(
            422,
            error="No Google purchase token reference available for restore",
            code="GOOGLE_RESTORE_PURCHASE_TOKEN_MISSING",
            details={"correlationId": correlation_id},
        )

    try:
        google_lookup = _resolve_google_subscription_purchase(
            package_name=package_name,
            purchase_token=purchase_token,
        )
        google_response = google_lookup.get("response") or {}
        if not isinstance(google_response, dict):
            google_response = {}
        google_response["packageName"] = google_lookup.get("packageName")
        built = _build_entitlement_from_google_subscription(
            route_number=route,
            expected_product_id=None,
            purchase_token=purchase_token,
            google_payload=google_response,
        )
        entitlement: BillingEntitlement = built["entitlement"]
        meta: Dict[str, Any] = built["meta"]
    except GoogleVerificationError as exc:
        return _error_response(
            exc.status_code,
            error=exc.error,
            code=exc.code,
            details={"correlationId": correlation_id, **(exc.details or {})},
        )

    owner_uid = _resolve_owner_uid_for_billing_write(
        db=db,
        route_number=route,
        requester_uid=decoded_token["uid"],
        requester_data=user_data,
    )
    provider_gate = _check_entitlement_provider_conflict(
        db=db,
        route_number=route,
        incoming_provider="google",
    )
    if provider_gate.get("conflict"):
        logger.warning(
            "Entitlement provider conflict route=%s incoming=%s existing=%s corr=%s",
            route,
            "google",
            provider_gate.get("existingProvider"),
            correlation_id,
        )
        return _error_response(
            409,
            error="Active entitlement is managed by another provider",
            code="ENTITLEMENT_PROVIDER_CONFLICT",
            details={
                "correlationId": correlation_id,
                "route": route,
                "incomingProvider": "google",
                "existingProvider": provider_gate.get("existingProvider"),
                "overrideEnabled": ENTITLEMENT_PROVIDER_OVERRIDE,
            },
        )
    try:
        _write_route_entitlement_from_google(
            db=db,
            route_number=route,
            entitlement=entitlement,
            owner_uid=owner_uid,
            meta=meta,
        )
        _write_legacy_subscription_shadow_from_google(
            db=db,
            owner_uid=owner_uid,
            route_number=route,
            entitlement=entitlement,
            meta=meta,
        )
    except Exception as exc:
        logger.exception(
            "Google restore write failure route=%s uid=%s corr=%s: %s",
            route,
            owner_uid,
            correlation_id,
            exc,
        )
        return _error_response(
            500,
            error="Failed to persist restored Google entitlement",
            code="GOOGLE_RESTORE_WRITE_FAILED",
            details={"correlationId": correlation_id},
        )
    _log_entitlement_write_event(
        provider="google",
        route_number=route,
        owner_uid=owner_uid,
        action="restore",
        entitlement=entitlement,
        correlation_id=correlation_id,
        source="google_play_api",
    )

    return BillingEntitlementResponse(
        ok=True,
        entitlement=_finalize_entitlement(entitlement, resolved_from="route_entitlements"),
    )

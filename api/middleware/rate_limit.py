"""Rate limiting middleware using slowapi.

Provides per-endpoint rate limits to prevent abuse.

Default limits:
- Undecorated endpoint default: 100 req/min per IP
- History endpoints: 30 req/min (database-backed queries)
- Write endpoints: 10 req/min (order creation/updates)
- Auth endpoints: 60 req/min
"""

from __future__ import annotations

import logging
import hashlib
import re

from fastapi import Request, Response
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIASGIMiddleware

logger = logging.getLogger("api.rate_limit")
security_logger = logging.getLogger("security")

from ..utils.client_ip import get_client_ip
from ..errors import structured_error_response


# Create limiter with custom key function
limiter = Limiter(
    key_func=get_client_ip,
    default_limits=["100/minute"],  # Per-endpoint default; not one shared all-route bucket.
    storage_uri="memory://",  # In-memory storage (simple, no Redis needed)
)


def authenticated_or_ip_key(request: Request) -> str:
    """Return a non-empty, privacy-safe authenticated limiter key.

    FastAPI dependencies populate ``usage_uid`` before decorated endpoint
    wrappers run. Middleware-level default limits execute before authentication,
    so they intentionally fall back to the trusted client-IP resolver.
    """

    uid = str(getattr(request.state, "usage_uid", "") or "").strip()
    if uid:
        digest = hashlib.sha256(uid.encode("utf-8")).hexdigest()
        return f"uid:{digest}"

    client_ip = str(get_client_ip(request) or "").strip()
    if client_ip:
        digest = hashlib.sha256(client_ip.encode("utf-8")).hexdigest()
        return f"ip:{digest}"
    return "ip:unknown"


# Rate limit decorators for different endpoint types
# Usage: @rate_limit_history on history endpoints
rate_limit_history = limiter.limit("30/minute")
rate_limit_write = limiter.limit("10/minute")
rate_limit_auth = limiter.limit("60/minute")
rate_limit_health = limiter.limit("120/minute")  # Health checks can be more frequent
rate_limit_order_update = limiter.limit(
    "60/minute",
    key_func=authenticated_or_ip_key,
    override_defaults=True,
)
rate_limit_order_read = limiter.limit(
    "30/minute",
    key_func=authenticated_or_ip_key,
    override_defaults=True,
)
rate_limit_transfer_reserve = limiter.limit(
    "60/minute",
    key_func=authenticated_or_ip_key,
    override_defaults=True,
)
rate_limit_transfer_create = limiter.limit(
    "60/minute",
    key_func=authenticated_or_ip_key,
    override_defaults=True,
)


def setup_rate_limiting(app):
    """Configure rate limiting on the FastAPI app.
    
    Call this in main.py after creating the app.
    """
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _custom_rate_limit_handler)
    app.add_middleware(SlowAPIASGIMiddleware)
    logger.info("Rate limiting enabled")


async def _custom_rate_limit_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Custom handler for rate limit exceeded.
    
    Logs the event and returns 429 with Retry-After header.
    """
    ip = get_client_ip(request)
    
    # Log for security monitoring
    security_logger.warning({
        "event": "rate_limit_exceeded",
        "ip": ip,
        "path": request.url.path,
        "method": request.method,
        "limit": str(exc.detail),
    })
    
    retry_after = 60
    is_order_update = (
        request.method == "PUT"
        and re.fullmatch(r"/api/orders/[^/]+", request.url.path) is not None
    )
    code = "RATE_LIMIT_ORDER_UPDATE" if is_order_update else "RATE_LIMIT_EXCEEDED"
    request.state.api_error_code = code

    response = structured_error_response(
        request,
        status_code=429,
        error="Too many requests",
        code=code,
        details={"retryAfterSeconds": retry_after},
    )
    response.headers["Retry-After"] = str(retry_after)
    return response

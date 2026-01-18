"""Rate limiting middleware using slowapi.

Provides per-endpoint rate limits to prevent abuse.

Default limits:
- Global: 100 req/min per IP
- History endpoints: 30 req/min (expensive DuckDB queries)
- Write endpoints: 10 req/min (order creation/updates)
- Auth endpoints: 60 req/min
"""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import Request, Response
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

logger = logging.getLogger("api.rate_limit")
security_logger = logging.getLogger("security")

from ..utils.client_ip import get_client_ip


# Create limiter with custom key function
limiter = Limiter(
    key_func=get_client_ip,
    default_limits=["100/minute"],  # Global default
    storage_uri="memory://",  # In-memory storage (simple, no Redis needed)
)


# Rate limit decorators for different endpoint types
# Usage: @rate_limit_history on history endpoints
rate_limit_history = limiter.limit("30/minute")
rate_limit_write = limiter.limit("10/minute")
rate_limit_auth = limiter.limit("60/minute")
rate_limit_health = limiter.limit("120/minute")  # Health checks can be more frequent


def setup_rate_limiting(app):
    """Configure rate limiting on the FastAPI app.
    
    Call this in main.py after creating the app.
    """
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _custom_rate_limit_handler)
    app.add_middleware(SlowAPIMiddleware)
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
    
    # Calculate retry-after (extract seconds from limit string)
    retry_after = 60  # Default 1 minute
    
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=429,
        content={
            "error": "Too many requests",
            "code": "RATE_LIMIT_EXCEEDED",
            "detail": str(exc.detail)
        },
        headers={"Retry-After": str(retry_after)}
    )

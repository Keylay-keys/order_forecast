"""Code extraction prevention middleware.

Prevents attackers from:
- Discovering API structure via docs endpoints
- Getting stack traces from errors
- Enumerating paths with common patterns
- Fingerprinting the server

Security headers are added in main.py; this module handles
enumeration detection and blocklist enforcement.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.concurrency import run_in_threadpool

from ..utils.security_logger import security_logger
from ..utils.blocklist import blocklist, WHITELISTED_IPS
from ..utils.client_ip import get_client_ip

UNBLOCKABLE_HEALTH_PATHS = {
    "/api/health",
}


# Patterns that indicate enumeration attempts
# These are NOT honeypots (which are fake paths), but suspicious patterns
# in otherwise valid requests that suggest scanning
ENUMERATION_PATTERNS = [
    r"\.\.\/",                    # Path traversal
    r"\.\.\\",                    # Windows path traversal
    r"%2e%2e",                    # URL-encoded traversal
    r"etc/passwd",                # Unix password file
    r"etc/shadow",                # Unix shadow file
    r"windows/system32",          # Windows system
    r"boot\.ini",                 # Windows boot config
    r"\$\{.*\}",                  # Template injection attempt
    r"<script",                   # XSS attempt
    r"javascript:",               # XSS attempt
    r"onerror=",                  # XSS attempt
    r"onload=",                   # XSS attempt
    r"UNION.*SELECT",             # SQL injection
    r"SELECT.*FROM",              # SQL injection
    r"INSERT.*INTO",              # SQL injection
    r"DELETE.*FROM",              # SQL injection
    r"DROP.*TABLE",               # SQL injection
    r"--.*$",                     # SQL comment
    r"#.*$",                      # Comment injection
    r"sleep\(\d+\)",              # Time-based injection
    r"benchmark\(",               # MySQL benchmark
    r"pg_sleep",                  # PostgreSQL sleep
    r"waitfor.*delay",            # MSSQL wait
]

# Compile patterns for efficiency
COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in ENUMERATION_PATTERNS]


def detect_enumeration(request: Request) -> str | None:
    """Check if request contains enumeration/attack patterns.
    
    Returns:
        Pattern name if detected, None if clean
    """
    # Check path
    path = request.url.path
    query = str(request.url.query) if request.url.query else ""
    
    # Combine path and query for checking
    full_url = path + "?" + query if query else path
    
    for i, pattern in enumerate(COMPILED_PATTERNS):
        if pattern.search(full_url):
            return ENUMERATION_PATTERNS[i]
    
    return None


class BlocklistMiddleware(BaseHTTPMiddleware):
    """Middleware that checks blocklist before processing requests."""
    
    async def dispatch(self, request: Request, call_next):
        # Keep the primary uptime/readiness probe available even if a client IP
        # was previously blocklisted by honeypot or enumeration controls.
        if request.url.path in UNBLOCKABLE_HEALTH_PATHS:
            return await call_next(request)

        ip = get_client_ip(request)
        
        # Skip whitelist
        if ip in WHITELISTED_IPS:
            return await call_next(request)
        
        # The authoritative lookup is synchronous PostgreSQL work, so keep it
        # off the event loop. Fetch once to avoid a second, inconsistent read.
        block_info = await run_in_threadpool(blocklist.get_block_info, ip)
        if block_info:
            current_request = {
                "current_method": request.method,
                "current_cf_ray": request.headers.get("cf-ray", ""),
                "current_user_agent": request.headers.get("user-agent", ""),
            }
            security_logger.blocked_ip_attempt(
                ip,
                request.url.path,
                details={**block_info, **current_request},
            )
            if str(block_info.get("reason") or "").startswith("brute_force_"):
                retry_after = 1
                if block_info.get("until"):
                    until = datetime.fromisoformat(
                        str(block_info["until"]).replace("Z", "+00:00")
                    )
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    retry_after = max(
                        1,
                        int((until - datetime.now(timezone.utc)).total_seconds()),
                    )
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too many requests", "code": "RATE_LIMITED"},
                    headers={"Retry-After": str(retry_after)},
                )
            return JSONResponse(status_code=403, content={"error": "Access denied"})
        
        return await call_next(request)


class EnumerationProtectionMiddleware(BaseHTTPMiddleware):
    """Middleware that detects and blocks enumeration attempts."""
    
    async def dispatch(self, request: Request, call_next):
        # Check for enumeration patterns
        pattern = detect_enumeration(request)
        
        if pattern:
            ip = get_client_ip(request)
            
            # Log the attempt
            security_logger.enumeration_attempt(
                ip=ip,
                path=request.url.path,
                pattern=pattern
            )
            
            # Add to blocklist
            await run_in_threadpool(
                blocklist.add,
                ip,
                "enumeration",
                timedelta(hours=12),
            )
            
            # Return generic 400 (don't reveal what we detected)
            return JSONResponse(
                status_code=400,
                content={"error": "Bad request"}
            )
        
        return await call_next(request)


def setup_code_protection(app):
    """Add code protection middleware to the app.
    
    Starlette executes middleware in reverse order (last added runs first).
    We want: blocklist → enumeration
    So add: enumeration first, then blocklist
    """
    # Added first, runs second: enumeration detection
    app.add_middleware(EnumerationProtectionMiddleware)
    
    # Added last, runs first: blocklist check
    app.add_middleware(BlocklistMiddleware)

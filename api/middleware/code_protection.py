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
from datetime import timedelta
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..utils.security_logger import security_logger
from ..utils.blocklist import blocklist, WHITELISTED_IPS
from ..utils.client_ip import get_client_ip


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
        ip = get_client_ip(request)
        
        # Skip whitelist
        if ip in WHITELISTED_IPS:
            return await call_next(request)
        
        # Check blocklist
        if blocklist.is_blocked(ip):
            security_logger.blocked_ip_attempt(ip, request.url.path)
            return JSONResponse(
                status_code=403,
                content={"error": "Access denied"}
            )
        
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
            blocklist.add(ip, reason="enumeration", duration=timedelta(hours=12))
            
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

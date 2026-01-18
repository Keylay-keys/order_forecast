"""Client IP extraction with trusted proxy support.

Only trusts X-Forwarded-For and CF-Connecting-IP headers when
TRUST_PROXY environment variable is set to "1" or "true".

This prevents IP spoofing when the API is exposed directly.
"""

from __future__ import annotations

import os
from fastapi import Request

# Environment flag for trusted proxy mode
# Set to "1" or "true" when behind Cloudflare Tunnel or trusted reverse proxy
TRUST_PROXY = os.environ.get("TRUST_PROXY", "").lower() in ("1", "true")


def get_client_ip(request: Request) -> str:
    """Get client IP address from request.
    
    If TRUST_PROXY is enabled, checks proxy headers.
    Otherwise, only uses direct connection IP.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Client IP address string
    """
    if TRUST_PROXY:
        # Check Cloudflare header first
        cf_ip = request.headers.get("CF-Connecting-IP")
        if cf_ip:
            return cf_ip.strip()
        
        # Check X-Forwarded-For (first IP in chain is original client)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    
    # Direct connection or untrusted proxy
    if request.client:
        return request.client.host
    
    return "unknown"

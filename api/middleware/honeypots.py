"""Honeypot middleware - detects and blocks attackers/scanners.

Fake endpoints that real users would never hit. Any request = attacker probing.
Triggers auto-ban on the IP blocklist.

Common honeypot paths:
- /admin, /wp-admin - WordPress/admin panel probes
- /.env, /.git - Configuration/source code leaks
- /phpMyAdmin - Database admin probes
- /api/debug, /api/internal - Internal API probes
"""

from __future__ import annotations

import os
import re
from datetime import timedelta
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..utils.security_logger import security_logger
from ..utils.blocklist import blocklist
from ..utils.client_ip import TRUST_PROXY, get_client_ip

PERMANENT_HONEYPOT_IPS = {
    ip.strip()
    for ip in os.environ.get("PERMANENT_HONEYPOT_IPS", "").split(",")
    if ip.strip()
}

# Honeypot paths that trigger immediate blocking
HONEYPOT_PATHS = [
    # Admin panels
    "/admin",
    "/administrator",
    "/wp-admin",
    "/wp-login.php",
    "/wp-content",
    "/wp-includes",
    
    # Configuration files
    "/.env",
    "/.env.local",
    "/.env.production",
    "/config.json",
    "/config.yml",
    "/config.yaml",
    "/settings.json",
    
    # Source control
    "/.git",
    "/.git/config",
    "/.git/HEAD",
    "/.svn",
    "/.hg",
    
    # Database
    "/phpmyadmin",
    "/pma",
    "/adminer",
    "/mysql",
    "/backup.sql",
    "/dump.sql",
    "/db.sql",
    
    # API probes
    "/api/v1/users",
    "/api/v1/admin",
    "/api/debug",
    "/api/internal",
    "/api/private",
    "/graphql",
    "/graphiql",
    
    # Common CMS/framework probes
    "/drupal",
    "/joomla",
    "/magento",
    "/laravel",
    "/symfony",
    "/rails",
    "/actuator",
    "/actuator/health",
    "/actuator/env",
    
    # Sensitive files
    "/id_rsa",
    "/id_rsa.pub",
    "/.ssh",
    "/credentials",
    "/secrets",
    "/passwords.txt",
    
    # Server probes
    "/server-status",
    "/server-info",
    "/nginx.conf",
    "/apache",
    "/cgi-bin",
]

# Patterns that indicate scanning/probing
HONEYPOT_PATTERNS = [
    r"\.php$",           # PHP file requests
    r"\.asp$",           # ASP file requests
    r"\.aspx$",          # ASPX file requests
    r"\.jsp$",           # JSP file requests
    r"\.cgi$",           # CGI scripts
    r"etc/passwd",       # Unix password file
    r"etc/shadow",       # Unix shadow file
    r"windows/system32", # Windows system files
    r"\.\.\/",           # Path traversal
    r"\.\.\\",           # Windows path traversal
]


def is_honeypot_path(path: str) -> bool:
    """Check if path matches a honeypot."""
    path_lower = path.lower()
    
    # Check exact path matches
    for honeypot in HONEYPOT_PATHS:
        if path_lower.startswith(honeypot) or path_lower.endswith(honeypot):
            return True
    
    # Check pattern matches
    for pattern in HONEYPOT_PATTERNS:
        if re.search(pattern, path_lower, re.IGNORECASE):
            return True
    
    return False


def build_honeypot_request_details(request: Request) -> dict[str, str | bool]:
    """Capture enough request context to investigate scanners later."""
    query_string = request.url.query or ""
    forwarded_for = request.headers.get("x-forwarded-for")
    cf_connecting_ip = request.headers.get("cf-connecting-ip")
    x_real_ip = request.headers.get("x-real-ip")

    ip_source = "direct"
    if TRUST_PROXY and cf_connecting_ip:
        ip_source = "cf-connecting-ip"
    elif TRUST_PROXY and forwarded_for:
        ip_source = "x-forwarded-for"
    elif x_real_ip:
        ip_source = "x-real-ip"

    return {
        "query_string": query_string,
        "full_path": f"{request.url.path}?{query_string}" if query_string else request.url.path,
        "host": request.headers.get("host", ""),
        "referer": request.headers.get("referer", ""),
        "accept": request.headers.get("accept", ""),
        "accept_language": request.headers.get("accept-language", ""),
        "cf_ray": request.headers.get("cf-ray", ""),
        "cf_connecting_ip": cf_connecting_ip or "",
        "x_forwarded_for": forwarded_for or "",
        "x_real_ip": x_real_ip or "",
        "forwarded_proto": request.headers.get("x-forwarded-proto", ""),
        "ip_source": ip_source,
        "trust_proxy": TRUST_PROXY,
    }


class HoneypotMiddleware(BaseHTTPMiddleware):
    """Middleware to detect and block attackers hitting honeypot paths."""
    
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        
        if is_honeypot_path(path):
            ip = get_client_ip(request)
            details = build_honeypot_request_details(request)
            permanent = ip in PERMANENT_HONEYPOT_IPS
            
            # Log the honeypot trigger
            security_logger.honeypot_triggered(
                ip=ip,
                path=path,
                user_agent=request.headers.get("user-agent"),
                method=request.method,
                details=details,
            )
            
            # Add to blocklist. Known-bad IPs can be pinned permanently.
            if permanent:
                blocklist.permaban(ip, reason="honeypot", metadata=details)
            else:
                blocklist.add(ip, reason="honeypot", duration=timedelta(hours=24), metadata=details)
            
            # Return believable error (don't reveal it's a trap)
            return JSONResponse(
                status_code=404,
                content={"error": "Not found"}
            )
        
        return await call_next(request)


def setup_honeypots(app):
    """Add honeypot middleware to the app."""
    app.add_middleware(HoneypotMiddleware)

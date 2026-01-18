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

import re
from datetime import timedelta
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..utils.security_logger import security_logger
from ..utils.blocklist import blocklist
from ..utils.client_ip import get_client_ip

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


class HoneypotMiddleware(BaseHTTPMiddleware):
    """Middleware to detect and block attackers hitting honeypot paths."""
    
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        
        if is_honeypot_path(path):
            ip = get_client_ip(request)
            
            # Log the honeypot trigger
            security_logger.honeypot_triggered(
                ip=ip,
                path=path,
                user_agent=request.headers.get("user-agent"),
                method=request.method
            )
            
            # Add to blocklist (24 hour ban, escalates for repeat offenders)
            blocklist.add(ip, reason="honeypot", duration=timedelta(hours=24))
            
            # Return believable error (don't reveal it's a trap)
            return JSONResponse(
                status_code=404,
                content={"error": "Not found"}
            )
        
        return await call_next(request)


def setup_honeypots(app):
    """Add honeypot middleware to the app."""
    app.add_middleware(HoneypotMiddleware)

"""Security audit logging for the Web Portal API.

Provides structured logging for security-relevant events:
- Authentication failures
- Authorization failures
- Rate limit violations
- Honeypot triggers
- Brute force lockouts
- Suspicious activity

Logs are structured JSON for easy parsing and alerting.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any, Optional

# Log directory
LOG_DIR = Path(__file__).parent.parent.parent / "logs"
SECURITY_LOG_FILE = LOG_DIR / "security.log"

# Log rotation settings
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB per file
BACKUP_COUNT = 5  # Keep 5 rotated files (50MB total)


class SecurityLogger:
    """Structured security event logger with rotation."""
    
    def __init__(self):
        self.logger = logging.getLogger("security")
        self._setup_handler()
    
    def _setup_handler(self):
        """Set up rotating file handler for security logs."""
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Rotating file handler - prevents unbounded growth
        handler = RotatingFileHandler(
            SECURITY_LOG_FILE,
            maxBytes=MAX_LOG_SIZE,
            backupCount=BACKUP_COUNT
        )
        handler.setLevel(logging.WARNING)
        
        # JSON formatter
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.WARNING)
    
    def log_event(
        self,
        event_type: str,
        severity: str,  # 'low', 'medium', 'high', 'critical'
        details: Dict[str, Any],
        ip: Optional[str] = None,
        uid: Optional[str] = None,
        path: Optional[str] = None
    ):
        """Log a security event.
        
        Args:
            event_type: Type of event (auth_failure, rate_limit, honeypot, etc.)
            severity: low, medium, high, critical
            details: Event-specific details
            ip: Client IP address
            uid: User ID if known
            path: Request path
        """
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "event": event_type,
            "severity": severity,
            "ip": ip,
            "uid": uid,
            "path": path,
            **details
        }
        
        # Remove None values
        event = {k: v for k, v in event.items() if v is not None}
        
        self.logger.warning(json.dumps(event))
    
    # Convenience methods for common events
    
    def auth_failure(
        self,
        ip: str,
        reason: str,
        path: str,
        user_agent: Optional[str] = None,
        uid: Optional[str] = None
    ):
        """Log authentication failure."""
        self.log_event(
            event_type="auth_failure",
            severity="medium",
            details={
                "reason": reason,
                "user_agent": user_agent
            },
            ip=ip,
            uid=uid,
            path=path
        )
    
    def unauthorized_access(
        self,
        ip: str,
        uid: str,
        route_number: str,
        path: str
    ):
        """Log unauthorized route access attempt."""
        self.log_event(
            event_type="unauthorized_route_access",
            severity="high",
            details={
                "route_number": route_number
            },
            ip=ip,
            uid=uid,
            path=path
        )
    
    def rate_limit_exceeded(
        self,
        ip: str,
        path: str,
        limit: str
    ):
        """Log rate limit violation."""
        self.log_event(
            event_type="rate_limit_exceeded",
            severity="medium",
            details={
                "limit": limit
            },
            ip=ip,
            path=path
        )
    
    def honeypot_triggered(
        self,
        ip: str,
        path: str,
        user_agent: Optional[str] = None,
        method: Optional[str] = None
    ):
        """Log honeypot trigger (scanner/attacker detected)."""
        self.log_event(
            event_type="honeypot_triggered",
            severity="high",
            details={
                "user_agent": user_agent,
                "method": method
            },
            ip=ip,
            path=path
        )
    
    def brute_force_lockout(
        self,
        ip: str,
        failures: int,
        lockout_minutes: int,
        reason: str
    ):
        """Log brute force lockout."""
        self.log_event(
            event_type="brute_force_lockout",
            severity="high",
            details={
                "failures": failures,
                "lockout_minutes": lockout_minutes,
                "reason": reason
            },
            ip=ip
        )
    
    def ip_blocked(
        self,
        ip: str,
        reason: str,
        duration_hours: float
    ):
        """Log IP being added to blocklist."""
        self.log_event(
            event_type="ip_blocked",
            severity="high",
            details={
                "reason": reason,
                "duration_hours": duration_hours
            },
            ip=ip
        )
    
    def enumeration_attempt(
        self,
        ip: str,
        path: str,
        pattern: str
    ):
        """Log enumeration/probing attempt."""
        self.log_event(
            event_type="enumeration_attempt",
            severity="medium",
            details={
                "pattern": pattern
            },
            ip=ip,
            path=path
        )
    
    def blocked_ip_attempt(self, ip: str, path: str):
        """Log request from blocked IP."""
        self.log_event(
            event_type="blocked_ip_attempt",
            severity="low",
            details={},
            ip=ip,
            path=path
        )


# Singleton instance
security_logger = SecurityLogger()


def log_security_event(event_type: str, details: Dict[str, Any]):
    """Convenience function for logging security events.
    
    This matches the interface expected by the security model in the plan.
    """
    ip = details.pop("ip", None)
    uid = details.pop("uid", None)
    path = details.pop("path", None)
    severity = details.pop("severity", "medium")
    
    security_logger.log_event(
        event_type=event_type,
        severity=severity,
        details=details,
        ip=ip,
        uid=uid,
        path=path
    )

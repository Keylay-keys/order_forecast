"""Brute force protection with progressive lockout.

Tracks failed authentication attempts and implements escalating lockouts:
- 3 failures in 1 min → 1 min lockout
- 5 failures in 5 min → 5 min lockout
- 10 failures in 30 min → 30 min lockout
- 20 failures in 24h → 24h lockout

Beyond basic rate limiting - specifically targets auth abuse patterns.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import threading

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..utils.security_logger import security_logger
from ..utils.blocklist import blocklist, WHITELISTED_IPS
from ..utils.client_ip import get_client_ip


class BruteForceProtection:
    """Tracks failed attempts and implements progressive lockout."""
    
    # Lockout thresholds: (failures, time_window, lockout_duration)
    LOCKOUT_THRESHOLDS: List[Tuple[int, timedelta, timedelta]] = [
        (3, timedelta(minutes=1), timedelta(minutes=1)),      # 3 in 1 min → 1 min lockout
        (5, timedelta(minutes=5), timedelta(minutes=5)),      # 5 in 5 min → 5 min lockout
        (10, timedelta(minutes=30), timedelta(minutes=30)),   # 10 in 30 min → 30 min lockout
        (20, timedelta(hours=24), timedelta(hours=24)),       # 20 in 24h → 24h lockout
    ]
    
    def __init__(self):
        self.failed_attempts: Dict[str, List[datetime]] = defaultdict(list)
        self.locked_out: Dict[str, datetime] = {}  # IP -> unlock_time
        self._lock = threading.Lock()
    
    def record_failure(self, ip: str, reason: str = "auth_failure"):
        """Record a failed attempt for an IP.
        
        Checks if lockout threshold is reached and applies it.
        """
        if ip in WHITELISTED_IPS:
            return
        
        with self._lock:
            now = datetime.utcnow()
            
            # Add new failure
            self.failed_attempts[ip].append(now)
            
            # Clean old attempts (keep last 24h)
            cutoff = now - timedelta(hours=24)
            self.failed_attempts[ip] = [
                t for t in self.failed_attempts[ip] if t > cutoff
            ]
            
            # Check thresholds (highest first for escalation)
            for threshold_count, window, lockout_duration in reversed(self.LOCKOUT_THRESHOLDS):
                window_start = now - window
                recent_fails = len([
                    t for t in self.failed_attempts[ip] if t > window_start
                ])
                
                if recent_fails >= threshold_count:
                    unlock_time = now + lockout_duration
                    self.locked_out[ip] = unlock_time
                    
                    # Log the lockout
                    security_logger.brute_force_lockout(
                        ip=ip,
                        failures=recent_fails,
                        lockout_minutes=int(lockout_duration.total_seconds() / 60),
                        reason=reason
                    )
                    
                    # For severe cases (24h lockout), also add to persistent blocklist
                    if lockout_duration >= timedelta(hours=24):
                        blocklist.add(ip, reason=f"brute_force_{reason}", duration=lockout_duration)
                    
                    break
    
    def is_locked_out(self, ip: str) -> bool:
        """Check if IP is currently locked out."""
        if ip in WHITELISTED_IPS:
            return False
        
        with self._lock:
            if ip not in self.locked_out:
                return False
            
            if datetime.utcnow() > self.locked_out[ip]:
                # Lockout expired
                del self.locked_out[ip]
                return False
            
            return True
    
    def get_unlock_time(self, ip: str) -> Optional[datetime]:
        """Get when the IP will be unlocked."""
        with self._lock:
            return self.locked_out.get(ip)
    
    def get_retry_after_seconds(self, ip: str) -> int:
        """Get seconds until unlock for Retry-After header."""
        unlock_time = self.get_unlock_time(ip)
        if not unlock_time:
            return 0
        
        remaining = (unlock_time - datetime.utcnow()).total_seconds()
        return max(1, int(remaining))
    
    def clear_failures(self, ip: str):
        """Clear failure history for an IP (e.g., after successful auth)."""
        with self._lock:
            if ip in self.failed_attempts:
                del self.failed_attempts[ip]
            if ip in self.locked_out:
                del self.locked_out[ip]
    
    def get_stats(self) -> Dict:
        """Get brute force protection statistics."""
        now = datetime.utcnow()
        
        with self._lock:
            active_lockouts = {
                ip: unlock_time
                for ip, unlock_time in self.locked_out.items()
                if unlock_time > now
            }
            
            # IPs with high failure counts
            high_risk = [
                (ip, len(attempts))
                for ip, attempts in self.failed_attempts.items()
                if len(attempts) >= 3
            ]
            high_risk.sort(key=lambda x: x[1], reverse=True)
        
        return {
            "active_lockouts": len(active_lockouts),
            "locked_ips": list(active_lockouts.keys())[:10],
            "high_risk_ips": [
                {"ip": ip, "failures": count}
                for ip, count in high_risk[:10]
            ]
        }


# Singleton instance
brute_force = BruteForceProtection()


class BruteForceMiddleware(BaseHTTPMiddleware):
    """Middleware that checks for lockouts and tracks auth failures."""
    
    async def dispatch(self, request: Request, call_next):
        ip = get_client_ip(request)
        
        # Check if IP is locked out
        if brute_force.is_locked_out(ip):
            security_logger.log_event(
                event_type="locked_ip_attempt",
                severity="low",
                details={},
                ip=ip,
                path=request.url.path
            )
            
            retry_after = brute_force.get_retry_after_seconds(ip)
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too many failed attempts. Try again later.",
                    "code": "LOCKED_OUT"
                },
                headers={"Retry-After": str(retry_after)}
            )
        
        # Process request
        response = await call_next(request)
        
        # Only track 401s (authentication failures), NOT 403s (authorization)
        # 403 can be legitimate (user accessing wrong route) and shouldn't
        # contribute to lockout. This prevents DoS via route enumeration.
        if response.status_code == 401:
            brute_force.record_failure(ip, "auth_failure")
        elif response.status_code == 200:
            # Successful auth - clear failure history to prevent accumulation
            # Only clear on auth endpoints to avoid clearing on every request
            if request.url.path.startswith("/api/auth"):
                brute_force.clear_failures(ip)
        
        return response


def setup_brute_force_protection(app):
    """Add brute force protection middleware to the app."""
    app.add_middleware(BruteForceMiddleware)

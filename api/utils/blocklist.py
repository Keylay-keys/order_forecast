"""IP Blocklist with persistence.

Manages blocked IPs with:
- Persistent storage (survives restarts)
- Auto-expiry
- Escalating bans for repeat offenders (capped at 48h max)
- Whitelist for known good IPs
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Set
import threading

from .security_logger import security_logger

# Paths
DATA_DIR = Path(__file__).parent.parent.parent / "data"
BLOCKLIST_FILE = DATA_DIR / "ip_blocklist.json"

# Whitelisted IPs (from environment or hardcoded)
WHITELISTED_IPS: Set[str] = set(
    ip.strip() 
    for ip in os.environ.get("WHITELISTED_IPS", "127.0.0.1").split(",")
    if ip.strip()
)


class IPBlocklist:
    """Persistent IP blocklist with auto-expiry and escalation."""
    
    # Maximum ban duration (protects shared NAT IPs)
    MAX_BAN_DURATION = timedelta(hours=48)
    
    def __init__(self):
        self.blocklist: Dict[str, Dict] = {}  # IP -> {until, reason, hits}
        self._lock = threading.Lock()
        self._load()
    
    def _load(self):
        """Load blocklist from disk."""
        if BLOCKLIST_FILE.exists():
            try:
                data = json.loads(BLOCKLIST_FILE.read_text())
                for ip, entry in data.items():
                    self.blocklist[ip] = {
                        "until": datetime.fromisoformat(entry["until"]),
                        "reason": entry["reason"],
                        "hits": entry.get("hits", 1)
                    }
            except Exception as e:
                print(f"[blocklist] Error loading blocklist: {e}")
    
    def _save(self):
        """Persist blocklist to disk."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # Only save active blocks
        now = datetime.utcnow()
        data = {
            ip: {
                "until": entry["until"].isoformat(),
                "reason": entry["reason"],
                "hits": entry["hits"]
            }
            for ip, entry in self.blocklist.items()
            if entry["until"] > now
        }
        
        try:
            BLOCKLIST_FILE.write_text(json.dumps(data, indent=2))
        except Exception as e:
            print(f"[blocklist] Error saving blocklist: {e}")
    
    def add(
        self,
        ip: str,
        reason: str,
        duration: timedelta = timedelta(hours=24)
    ) -> bool:
        """Add IP to blocklist.
        
        Duration escalates for repeat offenders:
        - 1st offense: specified duration (default 24h)
        - 2nd offense: 2x duration
        - 3rd+ offense: 4x duration
        
        Returns:
            True if newly blocked, False if already blocked
        """
        if ip in WHITELISTED_IPS:
            return False
        
        with self._lock:
            now = datetime.utcnow()
            
            if ip in self.blocklist:
                existing = self.blocklist[ip]
                hits = existing["hits"] + 1
                
                # Escalate duration for repeat offenders
                if hits >= 3:
                    duration = duration * 4
                elif hits >= 2:
                    duration = duration * 2
                
                # Cap at max duration to protect shared NAT IPs
                if duration > self.MAX_BAN_DURATION:
                    duration = self.MAX_BAN_DURATION
            else:
                hits = 1
            
            self.blocklist[ip] = {
                "until": now + duration,
                "reason": reason,
                "hits": hits
            }
            
            self._save()
        
        # Log the block
        security_logger.ip_blocked(
            ip=ip,
            reason=reason,
            duration_hours=duration.total_seconds() / 3600
        )
        
        return True
    
    def is_blocked(self, ip: str) -> bool:
        """Check if IP is currently blocked.
        
        Cleans up expired entries.
        """
        if ip in WHITELISTED_IPS:
            return False
        
        with self._lock:
            if ip not in self.blocklist:
                return False
            
            entry = self.blocklist[ip]
            if datetime.utcnow() > entry["until"]:
                # Expired, clean up
                del self.blocklist[ip]
                self._save()
                return False
            
            return True
    
    def get_block_info(self, ip: str) -> Optional[Dict]:
        """Get block details for an IP."""
        if ip not in self.blocklist:
            return None
        
        entry = self.blocklist[ip]
        if datetime.utcnow() > entry["until"]:
            return None
        
        return {
            "until": entry["until"].isoformat(),
            "reason": entry["reason"],
            "hits": entry["hits"]
        }
    
    def remove(self, ip: str) -> bool:
        """Manually unblock an IP.
        
        Returns:
            True if IP was blocked and removed, False otherwise
        """
        with self._lock:
            if ip in self.blocklist:
                del self.blocklist[ip]
                self._save()
                return True
            return False
    
    def get_stats(self) -> Dict:
        """Get blocklist statistics for admin dashboard."""
        now = datetime.utcnow()
        
        with self._lock:
            active = {
                ip: entry 
                for ip, entry in self.blocklist.items()
                if entry["until"] > now
            }
        
        # Sort by hits (most offensive first)
        top_offenders = sorted(
            active.items(),
            key=lambda x: x[1]["hits"],
            reverse=True
        )[:10]
        
        return {
            "active_blocks": len(active),
            "top_offenders": [
                {
                    "ip": ip,
                    "hits": entry["hits"],
                    "reason": entry["reason"],
                    "expires": entry["until"].isoformat()
                }
                for ip, entry in top_offenders
            ]
        }
    
    def cleanup(self):
        """Remove all expired entries."""
        now = datetime.utcnow()
        
        with self._lock:
            expired = [
                ip for ip, entry in self.blocklist.items()
                if entry["until"] <= now
            ]
            
            for ip in expired:
                del self.blocklist[ip]
            
            if expired:
                self._save()
        
        return len(expired)


# Singleton instance
blocklist = IPBlocklist()


async def add_to_blocklist(ip: str, reason: str, duration: timedelta = timedelta(hours=24)):
    """Async wrapper for adding to blocklist."""
    blocklist.add(ip, reason, duration)

"""Cluster-wide IP blocklist with a bounded local outage fallback."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import ipaddress
import logging
import os
from threading import Lock
from typing import Any, Dict, Optional, Set

from .security_logger import security_logger
from .security_store import PostgresSecurityStore, security_store


logger = logging.getLogger("api.blocklist")

WHITELISTED_IPS: Set[str] = {
    ip.strip()
    for ip in os.environ.get("WHITELISTED_IPS", "127.0.0.1").split(",")
    if ip.strip()
}


class IPBlocklist:
    """Use PostgreSQL as authority; retain known blocks during brief outages."""

    MAX_BAN_DURATION = timedelta(hours=48)

    def __init__(self, store: Optional[PostgresSecurityStore] = None) -> None:
        self._store = store or security_store
        self._fallback_blocks: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    @staticmethod
    def _canonical_ip(ip: str) -> Optional[str]:
        try:
            return str(ipaddress.ip_address(str(ip or "").strip()))
        except ValueError:
            logger.warning("Ignoring invalid client IP supplied to blocklist")
            return None

    @staticmethod
    def _active(entry: Optional[Dict[str, Any]]) -> bool:
        if not entry:
            return False
        if entry.get("permanent"):
            return True
        until_raw = entry.get("until")
        if not until_raw:
            return False
        until = datetime.fromisoformat(str(until_raw).replace("Z", "+00:00"))
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        return until > datetime.now(timezone.utc)

    def _remember(self, ip: str, entry: Optional[Dict[str, Any]]) -> None:
        with self._lock:
            if self._active(entry):
                self._fallback_blocks[ip] = dict(entry or {})
            else:
                self._fallback_blocks.pop(ip, None)

    def _fallback(self, ip: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._fallback_blocks.get(ip)
            if self._active(entry):
                return dict(entry or {})
            self._fallback_blocks.pop(ip, None)
            return None

    def add(
        self,
        ip: str,
        reason: str,
        duration: timedelta = timedelta(hours=24),
        *,
        permanent: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        canonical_ip = self._canonical_ip(ip)
        if canonical_ip is None or canonical_ip in WHITELISTED_IPS:
            return False
        try:
            entry = self._store.add_block(
                canonical_ip,
                reason,
                duration,
                permanent=permanent,
                metadata=metadata,
            )
            enforcement_scope = "cluster"
        except Exception:
            now = datetime.now(timezone.utc)
            existing = self._fallback(canonical_ip)
            hits = int(existing.get("hits") or 0) + 1 if existing else 1
            base_duration = min(
                max(duration, timedelta(seconds=1)),
                self.MAX_BAN_DURATION,
            )
            effective_duration = min(
                base_duration * (4 if hits >= 3 else 2 if hits == 2 else 1),
                self.MAX_BAN_DURATION,
            )
            entry = {
                "ip": canonical_ip,
                "until": None if permanent else (now + effective_duration).isoformat(),
                "reason": reason,
                "hits": hits,
                "permanent": permanent,
                "first_seen_at": existing.get("first_seen_at") if existing else now.isoformat(),
                "last_seen_at": now.isoformat(),
                "last_metadata": metadata or {},
            }
            enforcement_scope = "local_outage_fallback"
            logger.exception(
                "Shared blocklist write failed; enforcing a local outage fallback for ip=%s",
                canonical_ip,
            )
        self._remember(canonical_ip, entry)
        until = datetime.fromisoformat(entry["until"]) if entry.get("until") else None
        effective_hours = (
            max(0.0, (until - datetime.now(timezone.utc)).total_seconds() / 3600)
            if until
            else None
        )
        security_logger.ip_blocked(
            ip=canonical_ip,
            reason=reason,
            duration_hours=effective_hours,
            permanent=entry["permanent"],
            hits=entry["hits"],
            expires_at=entry["until"],
            details={**(metadata or {}), "enforcement_scope": enforcement_scope},
        )
        return True

    def get_block_info(self, ip: str) -> Optional[Dict[str, Any]]:
        canonical_ip = self._canonical_ip(ip)
        if canonical_ip is None or canonical_ip in WHITELISTED_IPS:
            return None
        try:
            entry = self._store.get_block(canonical_ip)
            self._remember(canonical_ip, entry)
            return entry
        except Exception:
            # Availability-safe degradation: never block a previously clean IP
            # merely because PostgreSQL is unavailable, but continue enforcing a
            # still-active block this worker has already observed.
            fallback = self._fallback(canonical_ip)
            logger.exception(
                "Shared blocklist unavailable; using local known-block fallback for ip=%s active=%s",
                canonical_ip,
                bool(fallback),
            )
            return fallback

    def is_blocked(self, ip: str) -> bool:
        return self.get_block_info(ip) is not None

    def remove(self, ip: str) -> bool:
        canonical_ip = self._canonical_ip(ip)
        if canonical_ip is None:
            return False
        removed = self._store.remove_block(canonical_ip)
        self._remember(canonical_ip, None)
        return removed

    def get_stats(self) -> Dict[str, Any]:
        return self._store.get_stats()

    def cleanup(self) -> int:
        return self._store.cleanup_expired()

    def permaban(self, ip: str, reason: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        return self.add(ip, reason=reason, permanent=True, metadata=metadata)


blocklist = IPBlocklist()


async def add_to_blocklist(ip: str, reason: str, duration: timedelta = timedelta(hours=24)):
    """Compatibility wrapper for callers that already await block additions."""
    from starlette.concurrency import run_in_threadpool

    return await run_in_threadpool(blocklist.add, ip, reason, duration)

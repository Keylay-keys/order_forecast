"""Shared rollout controls for finalize-triggered forecast orchestration."""

from __future__ import annotations

import os
from typing import Optional, Set


def _env_enabled(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes")


def _route_set_from_env(name: str) -> Optional[Set[str]]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    values = {item.strip() for item in raw.split(",") if item.strip()}
    return values or None


def api_finalize_rollout_enabled_for_route(route_number: str | None) -> bool:
    """Return True when API finalize enqueue owns this route."""
    if not route_number:
        return False
    if not _env_enabled("FORECAST_API_FINALIZE_ENABLED", "0"):
        return False
    allowed = _route_set_from_env("FORECAST_API_FINALIZE_ROUTES")
    return True if allowed is None else str(route_number) in allowed

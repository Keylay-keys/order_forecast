"""Fail-closed server-side gate for the currently disabled Promo feature."""

from __future__ import annotations

import os


def promos_enabled() -> bool:
    """Return True only when Promo is explicitly enabled for this runtime."""
    return os.environ.get("PROMOS_ENABLED", "false").strip().lower() == "true"

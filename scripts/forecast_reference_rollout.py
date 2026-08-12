"""Route-scoped rollout control for attach-without-prefill forecast references."""

from __future__ import annotations

import os


def _enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def forecast_reference_enabled_for_route(route_number: str | None) -> bool:
    """Require an explicit global switch and, when supplied, route allowlist."""
    if not route_number or not _enabled(os.environ.get("FORECAST_REFERENCE_ATTACH_ENABLED")):
        return False
    raw_routes = os.environ.get("FORECAST_REFERENCE_ATTACH_ROUTES", "").strip()
    if not raw_routes:
        return True
    return str(route_number) in {
        value.strip() for value in raw_routes.split(",") if value.strip()
    }

"""Pure rollout decisions for staged team-member route-transfer access."""

from __future__ import annotations

import re
from collections.abc import Iterable


ROUTE_PATTERN = re.compile(r"^\d{1,10}$")


def parse_route_allowlist(value: str | Iterable[str] | None) -> frozenset[str]:
    """Normalize a comma-delimited or iterable route allowlist."""
    entries = value.split(",") if isinstance(value, str) else (value or ())
    normalized = {
        str(route).strip()
        for route in entries
        if str(route).strip() == "*" or ROUTE_PATTERN.fullmatch(str(route).strip())
    }
    return frozenset(normalized)


def team_member_transfers_enabled_for(
    route_group_id: str,
    *,
    enabled: bool,
    allowlist: str | Iterable[str] | None,
) -> bool:
    """Return true only when both rollout controls permit the owner route group."""
    route = str(route_group_id).strip()
    if not enabled or not ROUTE_PATTERN.fullmatch(route):
        return False
    allowed_routes = parse_route_allowlist(allowlist)
    return "*" in allowed_routes or route in allowed_routes

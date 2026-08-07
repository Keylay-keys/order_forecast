"""Canonical owner-route extraction for auth and route transfers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


ROUTE_PATTERN = re.compile(r"^\d{1,10}$")


def normalize_route(value: Any) -> str:
    route = str(value or "").strip()
    return route if ROUTE_PATTERN.fullmatch(route) else ""


def extract_owned_routes_for_owner(user_data: Mapping[str, Any]) -> list[str]:
    profile = user_data.get("profile") or {}
    if str(profile.get("role") or "").strip() != "owner":
        return []

    owned: set[str] = set()

    def add_route(value: Any) -> None:
        route = normalize_route(value)
        if route:
            owned.add(route)

    add_route(profile.get("routeNumber"))
    for route in profile.get("additionalRoutes") or []:
        add_route(route)

    assignments = user_data.get("routeAssignments") or {}
    if isinstance(assignments, Mapping):
        for route, assignment in assignments.items():
            if isinstance(assignment, Mapping) and str(assignment.get("role") or "").strip() == "owner":
                add_route(route)

    return sorted(owned, key=int)

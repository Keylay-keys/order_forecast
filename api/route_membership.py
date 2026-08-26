"""Bounded, route-scoped membership reads for the cluster API."""

from __future__ import annotations

from typing import Any, List

from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.field_path import render_field_path


def team_member_role_field_path(route_number: str) -> str:
    """Return a safely quoted dynamic route-assignment field path."""

    route = str(route_number or "").strip()
    if not route.isdigit() or len(route) > 10:
        raise ValueError("invalid route number")
    return render_field_path(["routeAssignments", route, "role"])


def query_team_member_documents(db: Any, route_number: str) -> List[Any]:
    """Read only user documents assigned as team members to one exact route.

    Do not add a full-users fallback. An unavailable index or malformed route
    must fail the request rather than restoring an unbounded scan.
    """

    role_path = team_member_role_field_path(route_number)
    query = db.collection("users").where(
        filter=FieldFilter(role_path, "==", "team_member")
    )
    return list(query.stream())

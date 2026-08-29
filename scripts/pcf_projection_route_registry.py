#!/usr/bin/env python3
"""Checksum-pinned, owner-only PCF eligible-route initialization.

Dry-run is the default. The signed census authorizes only the ``eligible``
control bit. It does not synthesize lifecycle policy revisions, enable serving,
or create projection health.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

from psycopg2.extras import RealDictCursor, execute_values

try:
    from .pg_schema import get_connection
except ImportError:
    from pg_schema import get_connection


EXPECTED_CENSUS_SHA256 = (
    "23d6acf5dcbede928ff6f4d6077e1e164ee965af912e67508db9deeae04ecfdd"
)
EXPECTED_PROJECT = "routespark-1f47d"
EXPECTED_MODE = "read_only_exact_pcf_retention_authority_census"
EXPECTED_COUNTS = {
    "activeRoutes": 64,
    "resolvedActivePolicies": 61,
    "inactivePolicies": 3,
}
EXPECTED_UNRESOLVED = {
    "960462": "owner_unresolved",
    "961636": "retention_invalid",
    "988580": "owner_unresolved",
}
EXPECTED_AUTHORITY_CHAIN = (
    "routes.ownerUid",
    "routeEntitlements.ownerUid",
    "routeNumbers.userId",
)
REQUIRED_ACTIVE_ROUTES = frozenset({"900000", "988200", "989262"})
CONFIRMATION = "INITIALIZE PCF ELIGIBLE ROUTES FROM SIGNED CENSUS"

_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TOP_KEYS = {
    "schemaVersion",
    "observedAt",
    "project",
    "mode",
    "frozenAuthorityChain",
    "codeIdentity",
    "counts",
    "routes",
    "unresolvedRoutes",
    "measurementOperationAccounting",
    "privacy",
}
_ROUTE_KEYS = {
    "route",
    "active",
    "retentionDays",
    "authoritySource",
    "inactiveReason",
    "ownerUserIdSha256",
    "ownerDocumentRead",
    "ownerDocumentExists",
    "authorityDocumentPresence",
}


@dataclass(frozen=True)
class RegistryRoute:
    route_number: str
    eligible: bool


@dataclass(frozen=True)
class RegistryEvidence:
    census_sha256: str
    observed_at: str
    routes: tuple[RegistryRoute, ...]

    @property
    def eligible_routes(self) -> tuple[str, ...]:
        return tuple(row.route_number for row in self.routes if row.eligible)


def _parse_observed_at(value: Any) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RuntimeError("signed census observedAt is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError("signed census observedAt is invalid") from exc
    if parsed.tzinfo is None:
        raise RuntimeError("signed census observedAt is invalid")
    return value


def _validate_payload(payload: Any, *, census_sha256: str) -> RegistryEvidence:
    if not isinstance(payload, Mapping) or set(payload) != _TOP_KEYS:
        raise RuntimeError("signed census top-level contract is invalid")
    if (
        payload.get("schemaVersion") != 1
        or payload.get("project") != EXPECTED_PROJECT
        or payload.get("mode") != EXPECTED_MODE
        or tuple(payload.get("frozenAuthorityChain") or ()) != EXPECTED_AUTHORITY_CHAIN
    ):
        raise RuntimeError("signed census identity is invalid")
    observed_at = _parse_observed_at(payload.get("observedAt"))

    counts = payload.get("counts")
    if not isinstance(counts, Mapping) or any(
        counts.get(name) != expected for name, expected in EXPECTED_COUNTS.items()
    ):
        raise RuntimeError("signed census counts are invalid")
    code_identity = payload.get("codeIdentity")
    if (
        not isinstance(code_identity, Mapping)
        or not code_identity
        or any(_SHA256_RE.fullmatch(str(value)) is None for value in code_identity.values())
    ):
        raise RuntimeError("signed census code identity is invalid")
    operations = payload.get("measurementOperationAccounting")
    if (
        not isinstance(operations, Mapping)
        or operations.get("firestoreWrites") != 0
        or operations.get("firestoreDeletes") != 0
        or operations.get("storageReads") != 0
    ):
        raise RuntimeError("signed census is not read-only evidence")

    raw_routes = payload.get("routes")
    if not isinstance(raw_routes, list) or len(raw_routes) != 64:
        raise RuntimeError("signed census route rows are invalid")
    routes: list[RegistryRoute] = []
    seen: set[str] = set()
    retention_counts: dict[int, int] = {}
    authority_counts: dict[str, int] = {}
    inactive: dict[str, str] = {}
    for raw in raw_routes:
        if not isinstance(raw, Mapping) or set(raw) != _ROUTE_KEYS:
            raise RuntimeError("signed census route row contract is invalid")
        route = raw.get("route")
        if (
            not isinstance(route, str)
            or _ROUTE_RE.fullmatch(route) is None
            or route in seen
        ):
            raise RuntimeError("signed census route identity is invalid")
        seen.add(route)
        active = raw.get("active")
        if not isinstance(active, bool):
            raise RuntimeError("signed census route eligibility is invalid")
        if active:
            retention = raw.get("retentionDays")
            authority = raw.get("authoritySource")
            owner_hash = raw.get("ownerUserIdSha256")
            if (
                isinstance(retention, bool)
                or not isinstance(retention, int)
                or not 1 <= retention <= 3650
                or authority not in {"routes", "routeEntitlements", "routeNumbers"}
                or raw.get("inactiveReason") is not None
                or _SHA256_RE.fullmatch(str(owner_hash)) is None
                or raw.get("ownerDocumentRead") is not True
                or raw.get("ownerDocumentExists") is not True
            ):
                raise RuntimeError("signed census active route is incomplete")
            retention_counts[retention] = retention_counts.get(retention, 0) + 1
            authority_counts[str(authority)] = authority_counts.get(str(authority), 0) + 1
        else:
            reason = raw.get("inactiveReason")
            if (
                route not in EXPECTED_UNRESOLVED
                or reason != EXPECTED_UNRESOLVED[route]
                or raw.get("retentionDays") is not None
            ):
                raise RuntimeError("signed census inactive route is invalid")
            presence = raw.get("authorityDocumentPresence")
            if reason == "owner_unresolved":
                if (
                    raw.get("authoritySource") is not None
                    or raw.get("ownerUserIdSha256") is not None
                    or raw.get("ownerDocumentRead") is not False
                    or raw.get("ownerDocumentExists") is not None
                    or presence
                    != {
                        "routes": False,
                        "routeEntitlements": False,
                        "routeNumbers": False,
                    }
                ):
                    raise RuntimeError("signed census unresolved owner evidence is invalid")
            elif (
                raw.get("authoritySource") != "routes"
                or _SHA256_RE.fullmatch(str(raw.get("ownerUserIdSha256"))) is None
                or raw.get("ownerDocumentRead") is not True
                or raw.get("ownerDocumentExists") is not True
                or presence
                != {
                    "routes": True,
                    "routeEntitlements": False,
                    "routeNumbers": True,
                }
            ):
                raise RuntimeError("signed census invalid-retention evidence is invalid")
            inactive[route] = str(reason)
        routes.append(RegistryRoute(route_number=route, eligible=active))

    if (
        sum(row.eligible for row in routes) != 61
        or inactive != EXPECTED_UNRESOLVED
        or retention_counts != {4: 51, 5: 1, 6: 9}
        or authority_counts != {"routes": 35, "routeNumbers": 22, "routeEntitlements": 4}
        or not REQUIRED_ACTIVE_ROUTES.issubset(
            {row.route_number for row in routes if row.eligible}
        )
    ):
        raise RuntimeError("signed census derived totals are invalid")
    raw_unresolved = payload.get("unresolvedRoutes")
    if not isinstance(raw_unresolved, list):
        raise RuntimeError("signed census unresolved rows are invalid")
    unresolved = {
        str(row.get("route")): str(row.get("inactiveReason"))
        for row in raw_unresolved
        if isinstance(row, Mapping)
    }
    if len(raw_unresolved) != 3 or unresolved != EXPECTED_UNRESOLVED:
        raise RuntimeError("signed census unresolved rows do not match route rows")
    return RegistryEvidence(
        census_sha256=census_sha256,
        observed_at=observed_at,
        routes=tuple(sorted(routes, key=lambda row: row.route_number)),
    )


def load_signed_census(path: Path) -> RegistryEvidence:
    try:
        raw = path.read_bytes()
        signature = path.with_suffix(path.suffix + ".sha256").read_text(
            encoding="utf-8"
        ).strip()
    except OSError as exc:
        raise RuntimeError("signed census or checksum is unavailable") from exc
    digest = hashlib.sha256(raw).hexdigest()
    expected_signature = f"{digest}  {path.name}"
    if (
        digest != EXPECTED_CENSUS_SHA256
        or signature != expected_signature
        or _SHA256_RE.fullmatch(digest) is None
    ):
        raise RuntimeError("signed census checksum is invalid")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("signed census JSON is invalid") from exc
    return _validate_payload(payload, census_sha256=digest)


def _current_registry(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT route_number, eligible, serving_enabled
        FROM pcf_projection_routes
        ORDER BY route_number
        """
    )
    return [dict(row) for row in cur.fetchall()]


def plan_registry_initialization(conn: Any, evidence: RegistryEvidence) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        current = _current_registry(cur)
    conn.rollback()
    desired = {row.route_number: row.eligible for row in evidence.routes}
    serving = [row["route_number"] for row in current if row["serving_enabled"]]
    unexpected_eligible = [
        row["route_number"]
        for row in current
        if row["eligible"] and not desired.get(row["route_number"], False)
    ]
    current_by_route = {row["route_number"]: bool(row["eligible"]) for row in current}
    changes = sum(
        current_by_route.get(route) != eligible
        for route, eligible in desired.items()
    )
    return {
        "mode": "read-only",
        "censusSha256": evidence.census_sha256,
        "censusObservedAt": evidence.observed_at,
        "desiredRouteCount": len(desired),
        "desiredEligibleCount": sum(desired.values()),
        "desiredIneligibleCount": len(desired) - sum(desired.values()),
        "existingRouteCount": len(current),
        "wouldChangeCount": changes,
        "servingEnabledRoutes": serving,
        "unexpectedEligibleRoutes": unexpected_eligible,
        "readyToApply": not serving and not unexpected_eligible,
    }


def initialize_registry(conn: Any, evidence: RegistryEvidence) -> dict[str, Any]:
    desired = {row.route_number: row.eligible for row in evidence.routes}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '5s'")
            cur.execute("SET LOCAL lock_timeout = '3s'")
            cur.execute("SET LOCAL ROLE routespark_pcf_owner")
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("pcf-projection-route-registry",),
            )
            current = _current_registry(cur)
            if any(row["serving_enabled"] for row in current):
                raise RuntimeError("registry initialization refuses serving-enabled routes")
            unexpected = [
                row["route_number"]
                for row in current
                if row["eligible"] and not desired.get(row["route_number"], False)
            ]
            if unexpected:
                raise RuntimeError("registry initialization found unproven eligible routes")
            execute_values(
                cur,
                """
                INSERT INTO pcf_projection_routes (
                    route_number, eligible, serving_enabled,
                    reconciliation_status, healthy_until, created_at, updated_at
                ) VALUES %s
                ON CONFLICT (route_number) DO UPDATE
                SET eligible = EXCLUDED.eligible,
                    serving_enabled = FALSE,
                    reconciliation_status = 'unknown',
                    parity_source_revision = NULL,
                    parity_projection_revision = NULL,
                    source_entity_count = NULL,
                    projection_entity_count = NULL,
                    source_business_hash = NULL,
                    projection_business_hash = NULL,
                    last_reconciliation_attempt_at = NULL,
                    last_reconciled_at = NULL,
                    healthy_until = NULL,
                    updated_at = NOW()
                """,
                [
                    (row.route_number, row.eligible, False, "unknown", None)
                    for row in evidence.routes
                ],
                template="(%s, %s, %s, %s, %s, NOW(), NOW())",
                page_size=100,
            )
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE eligible) AS eligible_count,
                    COUNT(*) FILTER (WHERE serving_enabled) AS serving_count
                FROM pcf_projection_routes
                """
            )
            verified = cur.fetchone()
            if int(verified["eligible_count"]) != 61 or int(verified["serving_count"]) != 0:
                raise RuntimeError("registry initialization verification failed")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "mode": "apply",
        "censusSha256": evidence.census_sha256,
        "censusObservedAt": evidence.observed_at,
        "routeCount": len(evidence.routes),
        "eligibleCount": len(evidence.eligible_routes),
        "ineligibleCount": len(evidence.routes) - len(evidence.eligible_routes),
        "servingEnabledCount": 0,
        "lifecyclePoliciesInitialized": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    evidence = load_signed_census(Path(args.census))
    if args.apply and args.confirm != CONFIRMATION:
        raise RuntimeError(f"--apply requires --confirm '{CONFIRMATION}'")
    conn = get_connection(autocommit=False)
    try:
        result = (
            initialize_registry(conn, evidence)
            if args.apply
            else plan_registry_initialization(conn, evidence)
        )
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("readyToApply", True) else 2


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from order_forecast.scripts import pcf_projection_route_registry as registry


def _payload() -> dict:
    unresolved = {
        "960462": "owner_unresolved",
        "961636": "retention_invalid",
        "988580": "owner_unresolved",
    }
    active_routes = ["900000", "988200", "989262"] + [
        str(970000 + index) for index in range(58)
    ]
    rows = []
    for index, route in enumerate(active_routes):
        retention = 4 if index < 51 else (5 if index == 51 else 6)
        authority = "routes" if index < 35 else (
            "routeNumbers" if index < 57 else "routeEntitlements"
        )
        rows.append(
            {
                "route": route,
                "active": True,
                "retentionDays": retention,
                "authoritySource": authority,
                "inactiveReason": None,
                "ownerUserIdSha256": hashlib.sha256(route.encode()).hexdigest(),
                "ownerDocumentRead": True,
                "ownerDocumentExists": True,
                "authorityDocumentPresence": {
                    "routes": authority == "routes",
                    "routeEntitlements": authority == "routeEntitlements",
                    "routeNumbers": authority == "routeNumbers",
                },
            }
        )
    for route, reason in unresolved.items():
        rows.append(
            {
                "route": route,
                "active": False,
                "retentionDays": None,
                "authoritySource": "routes" if route == "961636" else None,
                "inactiveReason": reason,
                "ownerUserIdSha256": (
                    hashlib.sha256(route.encode()).hexdigest()
                    if route == "961636"
                    else None
                ),
                "ownerDocumentRead": route == "961636",
                "ownerDocumentExists": True if route == "961636" else None,
                "authorityDocumentPresence": {
                    "routes": route == "961636",
                    "routeEntitlements": False,
                    "routeNumbers": route == "961636",
                },
            }
        )
    return {
        "schemaVersion": 1,
        "observedAt": "2026-08-29T16:53:46.297Z",
        "project": "routespark-1f47d",
        "mode": "read_only_exact_pcf_retention_authority_census",
        "frozenAuthorityChain": list(registry.EXPECTED_AUTHORITY_CHAIN),
        "codeIdentity": {"producerSha256": "a" * 64},
        "counts": {
            "activeRoutes": 64,
            "resolvedActivePolicies": 61,
            "inactivePolicies": 3,
        },
        "routes": rows,
        "unresolvedRoutes": [
            {"route": route, "inactiveReason": reason}
            for route, reason in unresolved.items()
        ],
        "measurementOperationAccounting": {
            "firestoreWrites": 0,
            "firestoreDeletes": 0,
            "storageReads": 0,
        },
        "privacy": {"userIdsEmitted": False},
    }


class SignedCensusTests(unittest.TestCase):
    def _write(self, root: Path, payload: dict) -> tuple[Path, str]:
        path = root / "census.json"
        raw = (json.dumps(payload, sort_keys=True) + "\n").encode()
        path.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        path.with_suffix(".json.sha256").write_text(
            f"{digest}  census.json\n", encoding="utf-8"
        )
        return path, digest

    def test_exact_signed_census_yields_61_eligible_and_three_ineligible(self):
        with TemporaryDirectory() as temp:
            path, digest = self._write(Path(temp), _payload())
            with patch.object(registry, "EXPECTED_CENSUS_SHA256", digest):
                evidence = registry.load_signed_census(path)
        self.assertEqual(len(evidence.routes), 64)
        self.assertEqual(len(evidence.eligible_routes), 61)
        self.assertTrue(registry.REQUIRED_ACTIVE_ROUTES.issubset(evidence.eligible_routes))

    def test_content_change_without_matching_signature_is_rejected(self):
        with TemporaryDirectory() as temp:
            path, digest = self._write(Path(temp), _payload())
            changed = copy.deepcopy(_payload())
            changed["routes"][0]["active"] = False
            path.write_text(json.dumps(changed), encoding="utf-8")
            with patch.object(registry, "EXPECTED_CENSUS_SHA256", digest):
                with self.assertRaisesRegex(RuntimeError, "checksum"):
                    registry.load_signed_census(path)

    def test_unresolved_route_cannot_be_promoted_by_shape_alone(self):
        payload = _payload()
        row = next(row for row in payload["routes"] if row["route"] == "961636")
        row.update(
            active=True,
            retentionDays=4,
            inactiveReason=None,
            ownerUserIdSha256="b" * 64,
            ownerDocumentRead=True,
            ownerDocumentExists=True,
        )
        with self.assertRaises(RuntimeError):
            registry._validate_payload(payload, census_sha256="c" * 64)

    def test_required_review_and_rollout_routes_cannot_be_omitted(self):
        payload = _payload()
        payload["routes"] = [
            row for row in payload["routes"] if row["route"] != "900000"
        ]
        with self.assertRaises(RuntimeError):
            registry._validate_payload(payload, census_sha256="c" * 64)


if __name__ == "__main__":
    unittest.main()

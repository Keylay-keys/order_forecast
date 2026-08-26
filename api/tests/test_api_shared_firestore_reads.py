import unittest
import json
import os
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import HTTPException

from order_forecast.api import dependencies
from order_forecast.api import firestore_read_telemetry


class _Snapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _Document:
    def __init__(self, db, collection, key):
        self._db = db
        self._collection = collection
        self._key = key

    def get(self):
        self._db.reads.append((self._collection, self._key))
        return _Snapshot(self._db.data.get(self._collection, {}).get(self._key))


class _Collection:
    def __init__(self, db, name):
        self._db = db
        self._name = name

    def document(self, key):
        return _Document(self._db, self._name, key)


class _CountingDB:
    def __init__(self, data):
        self.data = data
        self.reads = []

    def collection(self, name):
        return _Collection(self, name)


class ApiSharedFirestoreReadTests(unittest.IsolatedAsyncioTestCase):
    def test_access_failure_timestamp_is_explicit_utc(self):
        with patch.object(dependencies.security_logger, "warning") as warning:
            dependencies._log_access_failure("user-1", "989262", "no_route_access")

        event = warning.call_args.args[0]
        timestamp = datetime.fromisoformat(event["timestamp"])
        self.assertEqual(timestamp.utcoffset(), timezone.utc.utcoffset(timestamp))

    async def test_route_access_reads_exactly_one_user_document(self):
        db = _CountingDB({
            "users": {
                "owner": {"profile": {"routeNumber": "989262"}},
            },
        })

        with patch.object(firestore_read_telemetry.logger, "info") as metric:
            result = await dependencies.require_route_access(
                "989262",
                {"uid": "owner"},
                db,
            )

        self.assertEqual(result["profile"]["routeNumber"], "989262")
        self.assertEqual(db.reads, [("users", "owner")])
        event = json.loads(metric.call_args.args[0])
        self.assertEqual(event, {
            "event": "api_firestore_shared_read_tax",
            "source": "route_access",
            "outcome": "route_access_granted",
            "documentGetCalls": 1,
        })
        self.assertNotIn("owner", str(event))
        self.assertNotIn("989262", str(event))

    async def test_analytics_seed_never_caches_route_authorization(self):
        db = _CountingDB({
            "users": {
                "owner": {"profile": {"role": "owner", "routeNumber": "989262"}},
            },
        })

        with patch.dict(os.environ, {"USAGE_ANALYTICS_HASH_KEY": "x" * 32}, clear=False):
            await dependencies.require_route_access("989262", {"uid": "owner"}, db)
            db.data["users"]["owner"]["profile"]["routeNumber"] = "111111"
            with self.assertRaises(HTTPException) as raised:
                await dependencies.require_route_access("989262", {"uid": "owner"}, db)

        self.assertEqual(raised.exception.detail, "Access denied")
        self.assertEqual(db.reads, [
            ("users", "owner"),
            ("users", "owner"),
        ])

    async def test_feature_access_shortest_success_path_reads_two_documents(self):
        db = _CountingDB({
            "users": {
                "owner": {"profile": {"role": "owner", "routeNumber": "989262"}},
            },
            "routeEntitlements": {
                "989262": {
                    "active": True,
                    "provider": "stripe",
                    "features": {"ordering": True},
                },
            },
        })

        with patch.object(dependencies, "ENFORCE_ROUTE_ENTITLEMENTS", True), patch.object(
            firestore_read_telemetry.logger, "info"
        ) as metric:
            await dependencies.require_route_feature_access(
                "989262",
                "ordering",
                {"uid": "owner"},
                db,
            )

        self.assertEqual(db.reads, [
            ("users", "owner"),
            ("routeEntitlements", "989262"),
        ])
        self.assertEqual(json.loads(metric.call_args_list[-1].args[0]), {
            "event": "api_firestore_shared_read_tax",
            "source": "feature_access",
            "outcome": "entitlement_granted",
            "documentGetCalls": 1,
            "featureKey": "ordering",
        })

    async def test_feature_access_worst_denial_path_reads_each_authority_document_once(self):
        requester = {
            "profile": {"role": "teamMember", "currentRoute": "989262"},
            "routeAssignments": {
                "989262": {"role": "team_member", "assignedTo": "owner"},
            },
        }
        owner = {
            "profile": {"role": "owner", "routeNumber": "111111"},
        }
        db = _CountingDB({
            "users": {"member": requester, "owner": owner},
            "routes": {},
            "routeEntitlements": {
                "111111": {
                    "active": True,
                    "provider": "stripe",
                    "features": {"ordering": True, "multiRoute": False},
                },
            },
            "routeNumbers": {},
        })

        with patch.object(dependencies, "ENFORCE_ROUTE_ENTITLEMENTS", True), patch.object(
            firestore_read_telemetry.logger, "info"
        ) as metric:
            with self.assertRaises(HTTPException) as raised:
                await dependencies.require_route_feature_access(
                    "989262",
                    "ordering",
                    {"uid": "member"},
                    db,
                )

        self.assertEqual(raised.exception.detail, "plan_downgraded_route_locked")
        self.assertEqual(len(db.reads), 6)
        self.assertEqual(db.reads.count(("users", "member")), 1)
        self.assertEqual(db.reads.count(("routes", "989262")), 1)
        self.assertEqual(db.reads.count(("routeEntitlements", "989262")), 1)
        self.assertEqual(db.reads.count(("routeNumbers", "989262")), 1)
        self.assertEqual(db.reads.count(("users", "owner")), 1)
        self.assertEqual(db.reads.count(("routeEntitlements", "111111")), 1)
        self.assertEqual(json.loads(metric.call_args_list[-1].args[0]), {
            "event": "api_firestore_shared_read_tax",
            "source": "feature_access",
            "outcome": "plan_downgraded_route_locked",
            "documentGetCalls": 5,
            "featureKey": "ordering",
        })

    async def test_feature_authority_snapshot_is_never_reused_across_requests(self):
        db = _CountingDB({
            "users": {
                "owner": {"profile": {"role": "owner", "routeNumber": "989262"}},
            },
            "routes": {},
            "routeEntitlements": {
                "989262": {
                    "active": True,
                    "provider": "stripe",
                    "features": {"ordering": True},
                },
            },
            "routeNumbers": {},
        })

        with patch.object(dependencies, "ENFORCE_ROUTE_ENTITLEMENTS", True):
            await dependencies.require_route_feature_access(
                "989262",
                "ordering",
                {"uid": "owner"},
                db,
            )
            db.data["routeEntitlements"]["989262"]["features"]["ordering"] = False
            with self.assertRaises(HTTPException) as raised:
                await dependencies.require_route_feature_access(
                    "989262",
                    "ordering",
                    {"uid": "owner"},
                    db,
                )

        self.assertEqual(raised.exception.detail, "ordering_entitlement_required")
        self.assertEqual(db.reads.count(("users", "owner")), 2)
        self.assertEqual(db.reads.count(("routeEntitlements", "989262")), 2)

    def test_telemetry_bounds_unknown_values_and_never_accepts_identifiers(self):
        with patch.object(firestore_read_telemetry.logger, "info") as metric:
            firestore_read_telemetry.emit_firestore_read_tax(
                source="unsafe-user-value",
                outcome="unsafe-route-value",
                document_get_calls=1000,
                feature_key="unsafe-feature-value",
            )

        self.assertEqual(json.loads(metric.call_args.args[0]), {
            "event": "api_firestore_shared_read_tax",
            "source": "unknown",
            "outcome": "unknown",
            "documentGetCalls": 100,
            "featureKey": "unknown",
        })


if __name__ == "__main__":
    unittest.main()

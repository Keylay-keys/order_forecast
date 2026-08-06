import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from order_forecast.api import usage_analytics
from order_forecast.api.errors import StructuredApiError
from order_forecast.api.routers import usage


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.connection.statements.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result or []


class _FakeConnection:
    def __init__(self):
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _SummaryCursor(_FakeCursor):
    def execute(self, sql, params=None):
        self.connection.statements.append((" ".join(sql.split()), params))
        self.result = self.connection.results.pop(0)


class _SummaryConnection(_FakeConnection):
    def __init__(self, results):
        super().__init__()
        self.results = list(results)

    def cursor(self, *args, **kwargs):
        return _SummaryCursor(self)


class _Snapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _Document:
    def __init__(self, data):
        self._data = data

    def get(self):
        return _Snapshot(self._data)


class _Collection:
    def __init__(self, documents):
        self._documents = documents

    def document(self, document_id):
        return _Document(self._documents.get(document_id))


class _FirestoreDB:
    def __init__(self, users):
        self._users = users

    def collection(self, name):
        return _Collection(self._users if name == "users" else {})


class UsageAnalyticsTests(unittest.TestCase):
    def setUp(self):
        usage_analytics._IDENTITY_CACHE.clear()

    def test_actor_hash_is_stable_and_does_not_contain_uid(self):
        first = usage_analytics.build_actor_hash("firebase-user-123", "x" * 32)
        second = usage_analytics.build_actor_hash("firebase-user-123", "x" * 32)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)
        self.assertNotIn("firebase-user-123", first)

    def test_classifies_only_known_api_features(self):
        cases = {
            "/api/catalog/starter": "reference_catalog",
            "/api/catalog/items/search": "reference_catalog",
            "/api/catalog/starter/images/31032.png": "reference_catalog",
            "/api/orders": "orders",
            "/api/stores/123": "stores",
            "/api/low-quantity": "low_quantity",
            "/api/team-tasks": "team_tasks",
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(usage_analytics.classify_api_feature(path), expected)

        self.assertIsNone(usage_analytics.classify_api_feature("/api/health"))
        self.assertIsNone(usage_analytics.classify_api_feature("/api/admin/usage/summary"))
        self.assertIsNone(usage_analytics.classify_api_feature("/unknown"))

    def test_extract_route_hint_accepts_only_known_locations(self):
        self.assertEqual(
            usage_analytics.extract_route_hint("/api/orders", {"route": "989262"}),
            "989262",
        )
        self.assertEqual(
            usage_analytics.extract_route_hint("/api/routes/988200/dashboard-summary", {}),
            "988200",
        )
        self.assertEqual(usage_analytics.extract_route_hint("/api/orders/123", {}), "")
        self.assertEqual(usage_analytics.extract_route_hint("/api/orders", {"route": "bad"}), "")

    def test_resolves_owner_and_team_member_from_server_data(self):
        db = _FirestoreDB(
            {
                "owner": {"profile": {"role": "owner", "routeNumber": "989262"}},
                "member": {
                    "profile": {"currentRoute": "989262", "role": "team_member"},
                    "routeAssignments": {"989262": {"role": "team_member"}},
                },
            }
        )
        with patch("order_forecast.api.dependencies.get_firestore", return_value=db), patch.dict(
            os.environ, {"USAGE_ANALYTICS_HASH_KEY": "x" * 32}, clear=False
        ):
            owner = usage_analytics._resolve_actor_context("owner", "989262")
            member = usage_analytics._resolve_actor_context("member", "989262")

        self.assertEqual(owner.route_number, "989262")
        self.assertEqual(owner.actor_role, "owner")
        self.assertEqual(member.actor_role, "team_member")
        self.assertNotEqual(owner.actor_hash, member.actor_hash)

    def test_records_request_and_error_counts(self):
        connection = _FakeConnection()
        usage_analytics.record_api_request(
            connection,
            actor_hash="a" * 64,
            route_number="989262",
            actor_role="owner",
            feature_key="reference_catalog",
            status_code=404,
            now=datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        sql, params = connection.statements[0]
        self.assertIn("INSERT INTO api_usage_daily", sql)
        self.assertEqual(params[1], "a" * 64)
        self.assertEqual(params[2], "989262")
        self.assertEqual(params[3], "owner")
        self.assertEqual(params[4], "reference_catalog")
        self.assertEqual(params[5], 1)
        self.assertEqual(params[6], 404)

    def test_summary_exposes_route_and_role_aggregates_without_actor_hashes(self):
        connection = _SummaryConnection(
            [
                {"requestCount": 9, "errorCount": 1, "uniqueUsers": 2, "uniqueRoutes": 1, "ownerUsers": 1, "teamMemberUsers": 1},
                [{"featureKey": "reference_catalog", "requestCount": 9, "uniqueUsers": 2}],
                [{"date": datetime(2026, 8, 6, tzinfo=timezone.utc).date(), "requestCount": 9, "uniqueUsers": 2}],
                [{"routeNumber": "989262", "featureKey": "reference_catalog", "requestCount": 9, "uniqueUsers": 2}],
            ]
        )

        result = usage_analytics.get_usage_summary(
            connection,
            days=7,
            route_number="989262",
            now=datetime(2026, 8, 6, tzinfo=timezone.utc),
        )

        self.assertEqual(result["totals"]["uniqueUsers"], 2)
        self.assertEqual(result["totals"]["errorCount"], 1)
        self.assertEqual(result["features"][0]["featureKey"], "reference_catalog")
        self.assertEqual(result["trend"][0]["date"], "2026-08-06")
        self.assertEqual(result["routeFeatures"][0]["routeNumber"], "989262")
        self.assertNotIn("actor_hash", str(result))
        self.assertNotIn("firebase", str(result))

    def test_admin_access_requires_claim_or_allowlisted_uid(self):
        with patch.dict(os.environ, {"USAGE_ANALYTICS_ADMIN_UIDS": "allowed-uid"}, clear=False):
            usage._require_usage_admin({"uid": "allowed-uid"})
            usage._require_usage_admin({"uid": "claim-uid", "usageAnalyticsAdmin": True})
            with self.assertRaises(StructuredApiError) as context:
                usage._require_usage_admin({"uid": "ordinary-uid"})

        self.assertEqual(context.exception.status_code, 403)
        self.assertEqual(context.exception.code, "USAGE_ANALYTICS_ADMIN_REQUIRED")


if __name__ == "__main__":
    unittest.main()

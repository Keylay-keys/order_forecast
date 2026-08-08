import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import APIRouter, FastAPI, Request
from fastapi.testclient import TestClient

from order_forecast.api import usage_analytics
from order_forecast.api.errors import StructuredApiError, install_api_error_handlers
from order_forecast.api.middleware.request_context import setup_request_context
from order_forecast.api.middleware.usage_analytics import _resolve_route_template, setup_usage_analytics
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
            "/api/transfers": "transfer_history_read",
            "/api/transfers/ledger": "transfer_ledger_read",
            "/api/transfers/reserve": "transfer_reserve",
            "/api/transfers/create": "transfer_create",
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

    def test_normalizes_error_metadata_without_resource_identifiers(self):
        self.assertEqual(
            usage_analytics.normalize_endpoint_path(
                "/api/orders/customer-order-123",
                "/api/orders/{order_id}",
            ),
            "/api/orders/{order_id}",
        )
        self.assertEqual(
            usage_analytics.normalize_endpoint_path("/api/orders/customer-order-123"),
            "/api/orders/*",
        )
        self.assertEqual(
            usage_analytics.normalize_endpoint_path("", "/api/orders/*"),
            "/api/orders/*",
        )
        self.assertEqual(
            usage_analytics.normalize_error_code("core_items_required", 409),
            "CORE_ITEMS_REQUIRED",
        )
        self.assertEqual(
            usage_analytics.normalize_error_code("unsafe error text", 422),
            "HTTP_422",
        )

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

    def test_records_safe_error_event_and_prunes_old_events(self):
        connection = _FakeConnection()
        request_id = "77ee4c94-0265-43bc-9d66-288573534bb9"
        usage_analytics.record_api_request(
            connection,
            actor_hash="a" * 64,
            route_number="989262",
            actor_role="team_member",
            feature_key="orders",
            status_code=409,
            method="POST",
            endpoint="/api/orders/{order_id}",
            error_code="ORDER_ALREADY_FINALIZED",
            request_id=request_id,
            now=datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(connection.commits, 1)
        self.assertEqual(len(connection.statements), 3)
        event_sql, event_params = connection.statements[1]
        self.assertIn("INSERT INTO api_usage_errors", event_sql)
        self.assertEqual(event_params[1:8], (
            "989262",
            "team_member",
            "orders",
            "POST",
            "/api/orders/{order_id}",
            409,
            "ORDER_ALREADY_FINALIZED",
        ))
        self.assertEqual(event_params[8], request_id)
        self.assertNotIn("a" * 64, event_params)
        self.assertIn("INTERVAL '30 days'", connection.statements[2][0])

    def test_middleware_captures_route_template_code_and_request_id(self):
        app = FastAPI()

        @app.get("/api/orders/{order_id}")
        async def order_error(order_id: str, request: Request):
            _ = order_id
            request.state.usage_uid = "firebase-user-123"
            raise StructuredApiError(409, "Order already finalized", "ORDER_ALREADY_FINALIZED")

        setup_request_context(app, usage_analytics.logger)
        setup_usage_analytics(app, usage_analytics.logger)
        install_api_error_handlers(app, debug_mode=False, app_logger=usage_analytics.logger)

        with patch("order_forecast.api.middleware.usage_analytics.enqueue_api_request") as enqueue:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get("/api/orders/customer-order-123")

        self.assertEqual(response.status_code, 409)
        item = enqueue.call_args.args[0]
        self.assertEqual(item.endpoint, "/api/orders/{order_id}")
        self.assertEqual(item.error_code, "ORDER_ALREADY_FINALIZED")
        self.assertEqual(item.method, "GET")
        self.assertEqual(item.request_id, response.headers["x-request-id"])
        self.assertNotIn("customer-order-123", item.endpoint)

    def test_route_template_fallback_matches_without_scope_route(self):
        app = FastAPI()
        router = APIRouter()

        @router.get("/catalog/starter/items/{sap}")
        async def catalog_item(sap: str):
            return {"sap": sap}

        app.include_router(router, prefix="/api")

        scope = {
            "type": "http",
            "path": "/api/catalog/starter/items/DO_NOT_STORE_988200",
            "root_path": "",
            "method": "GET",
        }
        self.assertEqual(
            _resolve_route_template(app, scope),
            "/api/catalog/starter/items/{sap}",
        )

    def test_summary_exposes_route_and_role_aggregates_without_actor_hashes(self):
        connection = _SummaryConnection(
            [
                {"requestCount": 9, "errorCount": 1, "uniqueUsers": 2, "uniqueRoutes": 1, "ownerUsers": 1, "teamMemberUsers": 1},
                [{"featureKey": "reference_catalog", "requestCount": 9, "uniqueUsers": 2}],
                {"requestCount": 4, "errorCount": 0, "uniqueUsers": 1, "uniqueRoutes": 1, "ownerUsers": 1, "teamMemberUsers": 0},
                [{"date": datetime(2026, 8, 6, tzinfo=timezone.utc).date(), "requestCount": 9, "uniqueUsers": 2}],
                [{
                    "routeNumber": "989262",
                    "requestCount": 9,
                    "errorCount": 1,
                    "uniqueUsers": 2,
                    "ownerUsers": 1,
                    "teamMemberUsers": 1,
                    "featureCount": 1,
                    "activeDays": 1,
                    "firstSeenDate": datetime(2026, 8, 6, tzinfo=timezone.utc).date(),
                    "lastSeenAt": datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
                }],
                [{"routeNumber": "989262", "featureKey": "reference_catalog", "requestCount": 9, "uniqueUsers": 2}],
                [{
                    "occurredAt": datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
                    "routeNumber": "989262",
                    "actorRole": "owner",
                    "featureKey": "reference_catalog",
                    "method": "GET",
                    "endpoint": "/api/catalog/starter/items/{sap}",
                    "statusCode": 404,
                    "errorCode": "HTTP_404",
                    "requestId": "77ee4c94-0265-43bc-9d66-288573534bb9",
                }],
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
        self.assertEqual(result["transferRollup"]["requestCount"], 4)
        self.assertEqual(result["transferRollup"]["uniqueUsers"], 1)
        self.assertEqual(result["trend"][0]["date"], "2026-08-06")
        self.assertEqual(result["routeSummaries"][0]["uniqueUsers"], 2)
        self.assertEqual(result["routeSummaries"][0]["firstSeenDate"], "2026-08-06")
        self.assertEqual(result["routeSummaries"][0]["lastSeenAt"], "2026-08-06T12:00:00+00:00")
        self.assertEqual(result["routeFeatures"][0]["routeNumber"], "989262")
        self.assertEqual(result["recentErrors"][0]["endpoint"], "/api/catalog/starter/items/{sap}")
        self.assertEqual(result["recentErrors"][0]["statusCode"], 404)
        self.assertFalse(result["errorsTruncated"])
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

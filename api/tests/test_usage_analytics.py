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
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, params))
        if "INSERT INTO usage_event_batches" in normalized:
            self.result = None if self.connection.duplicate else (params[0],)

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result or []


class _FakeConnection:
    def __init__(self, duplicate=False):
        self.duplicate = duplicate
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


class _FirestoreSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _FirestoreDocument:
    def __init__(self, data):
        self._data = data

    def get(self):
        return _FirestoreSnapshot(self._data)


class _FirestoreCollection:
    def __init__(self, documents):
        self._documents = documents

    def document(self, document_id):
        return _FirestoreDocument(self._documents.get(document_id))


class _FirestoreDB:
    def __init__(self, collections):
        self._collections = collections

    def collection(self, name):
        return _FirestoreCollection(self._collections.get(name, {}))


class UsageAnalyticsTests(unittest.TestCase):
    def test_actor_hash_is_stable_and_does_not_contain_uid(self):
        first = usage_analytics.build_actor_hash("firebase-user-123", "x" * 32)
        second = usage_analytics.build_actor_hash("firebase-user-123", "x" * 32)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)
        self.assertNotIn("firebase-user-123", first)

    def test_record_batch_groups_events_and_is_idempotent(self):
        now = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)
        connection = _FakeConnection()

        accepted = usage_analytics.record_usage_batch(
            connection,
            batch_id="batch_1234567890123456",
            actor_hash="a" * 64,
            route_number="989262",
            actor_role="owner",
            access_tier="paid",
            platform="ios",
            app_version="1.1.7",
            events=[
                {"feature": "item_lookup", "count": 1},
                {"feature": "item_lookup", "count": 2},
                {"feature": "dashboard", "count": 1},
            ],
            now=now,
        )

        self.assertTrue(accepted)
        self.assertEqual(connection.commits, 1)
        rollup_writes = [row for row in connection.statements if "INSERT INTO usage_activity_daily" in row[0]]
        self.assertEqual(len(rollup_writes), 2)
        counts = sorted(row[1][6] for row in rollup_writes)
        self.assertEqual(counts, [1, 3])

        duplicate = _FakeConnection(duplicate=True)
        accepted_again = usage_analytics.record_usage_batch(
            duplicate,
            batch_id="batch_1234567890123456",
            actor_hash="a" * 64,
            route_number="989262",
            actor_role="owner",
            access_tier="paid",
            platform="ios",
            app_version="1.1.7",
            events=[{"feature": "item_lookup", "count": 3}],
            now=now,
        )
        self.assertFalse(accepted_again)
        self.assertEqual(duplicate.rollbacks, 1)
        self.assertFalse(any("INSERT INTO usage_activity_daily" in sql for sql, _ in duplicate.statements))

    def test_request_rejects_unknown_feature(self):
        with self.assertRaises(ValueError):
            usage.UsageBatchRequest(
                batchId="batch_1234567890123456",
                routeNumber="989262",
                platform="ios",
                events=[{"feature": "note_text", "count": 1}],
            )

    def test_summary_exposes_route_and_role_aggregates_without_actor_hashes(self):
        connection = _SummaryConnection(
            [
                {"eventCount": 9, "uniqueUsers": 2, "uniqueRoutes": 1, "ownerUsers": 1, "teamMemberUsers": 1},
                [{"featureKey": "item_lookup", "eventCount": 9, "uniqueUsers": 2}],
                [{"date": datetime(2026, 8, 6, tzinfo=timezone.utc).date(), "eventCount": 9, "uniqueUsers": 2}],
                [{"routeNumber": "989262", "featureKey": "item_lookup", "eventCount": 9, "uniqueUsers": 2}],
            ]
        )

        result = usage_analytics.get_usage_summary(
            connection,
            days=7,
            route_number="989262",
            now=datetime(2026, 8, 6, tzinfo=timezone.utc),
        )

        self.assertEqual(result["totals"]["uniqueUsers"], 2)
        self.assertEqual(result["features"][0]["featureKey"], "item_lookup")
        self.assertEqual(result["trend"][0]["date"], "2026-08-06")
        self.assertEqual(result["routeFeatures"][0]["routeNumber"], "989262")
        self.assertNotIn("actor_hash", str(result))

    def test_admin_access_requires_claim_or_allowlisted_uid(self):
        with patch.dict(os.environ, {"USAGE_ANALYTICS_ADMIN_UIDS": "allowed-uid"}, clear=False):
            usage._require_usage_admin({"uid": "allowed-uid"})
            usage._require_usage_admin({"uid": "claim-uid", "usageAnalyticsAdmin": True})
            with self.assertRaises(StructuredApiError) as context:
                usage._require_usage_admin({"uid": "ordinary-uid"})

        self.assertEqual(context.exception.status_code, 403)
        self.assertEqual(context.exception.code, "USAGE_ANALYTICS_ADMIN_REQUIRED")

    def test_access_tier_uses_server_entitlement_state(self):
        paid_db = _FirestoreDB(
            {"routeEntitlements": {"989262": {"active": True, "provider": "stripe"}}}
        )
        requester = {"profile": {"role": "owner", "routeNumber": "989262"}}
        self.assertEqual(
            usage._active_entitlement_tier(
                db=paid_db,
                route_number="989262",
                requester_uid="owner-uid",
                requester_data=requester,
            ),
            "paid",
        )

        free_db = _FirestoreDB({})
        self.assertEqual(
            usage._active_entitlement_tier(
                db=free_db,
                route_number="989262",
                requester_uid="owner-uid",
                requester_data=requester,
            ),
            "free",
        )


if __name__ == "__main__":
    unittest.main()

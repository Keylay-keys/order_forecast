import unittest
from datetime import datetime, timezone

from order_forecast.api import dashboard_summary


class _FakeSnapshot:
    def __init__(self, data, doc_id="doc"):
        self._data = data
        self.exists = data is not None
        self.id = doc_id

    def to_dict(self):
        return self._data


class _FakeContainerQuery:
    def __init__(self, docs, fail=False):
        self._docs = docs
        self._fail = fail
        self.stream_called = False

    def where(self, field, op, value):
        assert (field, op, value) == ("active", "==", True)
        return self

    def stream(self):
        self.stream_called = True
        if self._fail:
            raise RuntimeError("firebase read failed")
        return [
            _FakeSnapshot(doc, doc.get("rowId") or doc.get("containerCode") or "container")
            for doc in self._docs
            if doc.get("active") is True
        ]


class _FakeSummaryDocument:
    def __init__(self, summary, containers, fail_containers=False):
        self._summary = summary
        self.containers_query = _FakeContainerQuery(containers, fail_containers)

    def get(self):
        return _FakeSnapshot(self._summary, "summary")

    def collection(self, name):
        assert name == "containers"
        return self.containers_query


class _FakeSummaryCollection:
    def __init__(self, routes):
        self._routes = routes

    def document(self, route_number):
        route = self._routes.get(route_number) or {}
        return _FakeSummaryDocument(
            route.get("summary"),
            route.get("containers", []),
            route.get("fail_containers", False),
        )


class _FakeDB:
    def __init__(self, routes):
        self._routes = routes

    def collection(self, name):
        assert name == "routeDashboardSummaries"
        return _FakeSummaryCollection(self._routes)


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("create table"):
            self.conn.ensure_count += 1
            return
        if normalized.startswith("select"):
            row = self.conn.rows.get(params[0])
            self._result = dict(row) if row else None
            return
        if normalized.startswith("insert"):
            route_number = params[0]
            payload = getattr(params[1], "adapted", params[1])
            now = params[5]
            row = {
                "route_number": route_number,
                "payload": payload,
                "source_revision": params[2],
                "source_updated_at": params[3],
                "mirrored_revision": params[4],
                "mirrored_at": now,
                "updated_at": params[6],
            }
            self.conn.rows[route_number] = row
            self._result = dict(row)
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self._result


class _FakeConn:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.commits = 0
        self.ensure_count = 0

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def _source_doc(revision):
    return {
        "updatedAtMs": revision,
        "updatedAt": datetime.fromtimestamp(revision / 1000, timezone.utc),
    }


def _container_doc():
    return {
        "active": True,
        "rowId": "1806000001_100737310000000000",
        "routeNumber": "989567",
        "deliveryNumber": "1806000001",
        "containerCode": "100737310000000000",
        "itemCount": 2,
        "expiringCount": 1,
        "expiredCount": 0,
        "expiringItems": [
            {
                "description": "Whole Milk",
                "product": "1001",
                "expiryDate": "03/20/2026",
                "daysLeft": 3,
                "isShortCoded": True,
                "deliveryNumber": "1806000001",
                "containerCode": "100737310000000000",
                "visibleOnList": False,
                "isLowQuantity": True,
                "pageNumber": 3,
                "guaranteed": {
                    "isGuaranteed": True,
                    "guaranteeExpiresAt": "2026-03-25T12:00:00.000Z",
                },
            }
        ],
    }


class DashboardSummaryMirrorTests(unittest.TestCase):
    def test_fresh_postgres_mirror_returns_without_container_readthrough(self):
        db = _FakeDB({
            "989567": {
                "summary": _source_doc(1773777600000),
                "containers": [_container_doc()],
            }
        })
        conn = _FakeConn({
            "989567": {
                "route_number": "989567",
                "payload": {
                    "routeNumber": "989567",
                    "counts": {"totalPCFs": 1},
                    "activePcfs": [{"containerCode": "100737310000000000"}],
                    "expiringItems": [],
                },
                "source_revision": "1773777600000",
                "source_updated_at": datetime.fromtimestamp(1773777600, timezone.utc),
                "mirrored_revision": "1773777600000",
                "mirrored_at": datetime.fromtimestamp(1773777601, timezone.utc),
                "updated_at": datetime.fromtimestamp(1773777601, timezone.utc),
            }
        })

        result = dashboard_summary.get_dashboard_summary_payload(
            db=db,
            conn=conn,
            route_number="989567",
        )

        self.assertTrue(result["freshness"]["fresh"])
        self.assertEqual(result["freshness"]["source"], "postgres")
        self.assertEqual(result["counts"]["totalPCFs"], 1)
        self.assertFalse(db.collection("routeDashboardSummaries").document("989567").containers_query.stream_called)

    def test_missing_mirror_reads_firebase_summary_rows_and_upserts_postgres(self):
        db = _FakeDB({
            "989567": {
                "summary": _source_doc(1773777600000),
                "containers": [_container_doc()],
            }
        })
        conn = _FakeConn()

        result = dashboard_summary.get_dashboard_summary_payload(
            db=db,
            conn=conn,
            route_number="989567",
        )

        self.assertTrue(result["freshness"]["fresh"])
        self.assertEqual(result["freshness"]["source"], "firebase_readthrough")
        self.assertEqual(result["freshness"]["sourceRevision"], "1773777600000")
        self.assertEqual(result["freshness"]["mirroredRevision"], "1773777600000")
        self.assertEqual(result["counts"]["totalPCFs"], 1)
        self.assertEqual(result["activePcfs"][0]["items"][0]["guaranteed"]["isGuaranteed"], True)
        self.assertNotIn("pages", result["activePcfs"][0])
        self.assertIn("989567", conn.rows)

    def test_rebuild_failure_returns_cached_payload_as_stale(self):
        db = _FakeDB({
            "989567": {
                "summary": _source_doc(1773777600000),
                "containers": [_container_doc()],
                "fail_containers": True,
            }
        })
        conn = _FakeConn({
            "989567": {
                "route_number": "989567",
                "payload": {
                    "routeNumber": "989567",
                    "counts": {"totalPCFs": 1},
                    "activePcfs": [],
                    "expiringItems": [],
                },
                "source_revision": "1773777500000",
                "source_updated_at": datetime.fromtimestamp(1773777500, timezone.utc),
                "mirrored_revision": "1773777500000",
                "mirrored_at": datetime.fromtimestamp(1773777501, timezone.utc),
                "updated_at": datetime.fromtimestamp(1773777501, timezone.utc),
            }
        })

        with self.assertLogs("order_forecast.api.dashboard_summary", level="ERROR"):
            result = dashboard_summary.get_dashboard_summary_payload(
                db=db,
                conn=conn,
                route_number="989567",
            )

        self.assertFalse(result["freshness"]["fresh"])
        self.assertEqual(result["freshness"]["source"], "cache")
        self.assertEqual(result["freshness"]["staleReason"], "rebuild_failed")
        self.assertEqual(result["freshness"]["sourceRevision"], "1773777600000")
        self.assertEqual(result["freshness"]["mirroredRevision"], "1773777500000")

    def test_missing_source_watermark_does_not_mark_empty_data_fresh(self):
        db = _FakeDB({"989567": {"summary": None, "containers": []}})
        conn = _FakeConn()

        result = dashboard_summary.get_dashboard_summary_payload(
            db=db,
            conn=conn,
            route_number="989567",
        )

        self.assertEqual(result["routeNumber"], "989567")
        self.assertFalse(result["freshness"]["fresh"])
        self.assertEqual(result["freshness"]["source"], "cache")
        self.assertEqual(result["freshness"]["staleReason"], "mirror_missing")


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from order_forecast.api.routers import health


class _FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _FakeDocument:
    def __init__(self, data, key):
        self._data = data
        self._key = key

    def get(self):
        return _FakeSnapshot(self._data.get(self._key))


class _FakeOrdersCollection:
    def __init__(self, order_docs):
        self._order_docs = order_docs

    def document(self, key):
        return _FakeDocument(self._order_docs, key)


class _FakeRouteDocument:
    def __init__(self, routes_data, route_key):
        self._routes_data = routes_data
        self._route_key = route_key

    def collection(self, name):
        if name != "orders":
            raise KeyError(name)
        route_data = self._routes_data.get(self._route_key) or {}
        return _FakeOrdersCollection(route_data.get("orders", {}))

    def get(self):
        return _FakeSnapshot(self._routes_data.get(self._route_key))


class _FakeCollection:
    def __init__(self, data, name):
        self._data = data
        self._name = name

    def document(self, key):
        if self._name == "routes":
            return _FakeRouteDocument(self._data, key)
        return _FakeDocument(self._data, key)


class _FakeDB:
    def __init__(self, collections):
        self._collections = collections

    def collection(self, name):
        return _FakeCollection(self._collections.setdefault(name, {}), name)


class HealthSyncReplayTests(unittest.TestCase):
    def test_resolve_route_for_manual_sync_prefers_explicit_route(self):
        db = _FakeDB({"users": {}})
        decoded = {"uid": "user-1"}

        self.assertEqual(
            health._resolve_route_for_manual_sync(db, decoded, "961767"),
            "961767",
        )

    def test_resolve_route_for_manual_sync_uses_current_route_fallback(self):
        db = _FakeDB(
            {
                "users": {
                    "user-1": {
                        "profile": {
                            "currentRoute": "961767",
                            "routeNumber": "989262",
                        }
                    }
                }
            }
        )

        self.assertEqual(
            health._resolve_route_for_manual_sync(db, {"uid": "user-1"}, None),
            "961767",
        )

    def test_sync_single_finalized_order_rejects_draft_orders(self):
        db = _FakeDB(
            {
                "routes": {
                    "961767": {
                        "orders": {
                            "order-961767-1": {
                                "status": "draft",
                            }
                        }
                    }
                }
            }
        )

        with self.assertRaises(HTTPException) as ctx:
            health._sync_single_finalized_order(
                conn=object(),
                db=db,
                route="961767",
                order_id="order-961767-1",
            )

        self.assertEqual(ctx.exception.status_code, 409)

    def test_sync_single_finalized_order_calls_handle_sync_order(self):
        db = _FakeDB(
            {
                "routes": {
                    "961767": {
                        "orders": {
                            "order-961767-1": {
                                "status": "finalized",
                            }
                        }
                    }
                }
            }
        )

        with patch(
            "db_manager_pg.handle_sync_order",
            return_value={"success": True, "orderId": "order-961767-1"},
        ) as sync_mock:
            result = health._sync_single_finalized_order(
                conn="fake-conn",
                db=db,
                route="961767",
                order_id="order-961767-1",
            )

        self.assertTrue(result["success"])
        sync_mock.assert_called_once_with(
            "fake-conn",
            db,
            {"orderId": "order-961767-1", "routeNumber": "961767"},
        )


if __name__ == "__main__":
    unittest.main()

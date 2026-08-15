from __future__ import annotations

import unittest
from unittest import mock

from order_forecast.scripts import db_manager_pg as worker


class _FakeDocSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _FakeOrderDocument:
    def __init__(self, store, route_number: str, order_id: str):
        self._store = store
        self._route_number = route_number
        self._order_id = order_id

    def get(self):
        return _FakeDocSnapshot(self._store.get((self._route_number, self._order_id)))


class _FakeOrdersCollection:
    def __init__(self, store, route_number: str):
        self._store = store
        self._route_number = route_number

    def document(self, order_id: str):
        return _FakeOrderDocument(self._store, self._route_number, order_id)


class _FakeForecastDoc:
    def __init__(self, doc_id: str, data):
        self.id = doc_id
        self._data = data

    def to_dict(self):
        return self._data


class _FakeForecastsCollection:
    def __init__(self, docs):
        self._docs = docs

    def stream(self):
        return [_FakeForecastDoc(doc_id, data) for doc_id, data in self._docs]


class _FakeRouteDocument:
    def __init__(self, orders_store, forecast_store, route_number: str, parent_collection: str):
        self._orders_store = orders_store
        self._forecast_store = forecast_store
        self._route_number = route_number
        self._parent_collection = parent_collection

    def collection(self, name: str):
        if self._parent_collection == "routes" and name == "orders":
            return _FakeOrdersCollection(self._orders_store, self._route_number)
        if self._parent_collection == "forecasts" and name == "cached":
            return _FakeForecastsCollection(self._forecast_store.get(self._route_number, []))
        raise KeyError((self._parent_collection, self._route_number, name))


class _FakeTopCollection:
    def __init__(self, orders_store, forecast_store, name: str):
        self._orders_store = orders_store
        self._forecast_store = forecast_store
        self._name = name

    def document(self, route_number: str):
        return _FakeRouteDocument(
            self._orders_store,
            self._forecast_store,
            route_number,
            self._name,
        )


class _FakeFirestoreDB:
    def __init__(self, orders_store, forecast_store):
        self.orders_store = orders_store
        self.forecast_store = forecast_store

    def collection(self, name: str):
        if name not in {"routes", "forecasts"}:
            raise KeyError(name)
        return _FakeTopCollection(self.orders_store, self.forecast_store, name)


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._fetchone = None

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split()).lower()
        params = params or []

        if normalized.startswith("select canonical_id from store_id_aliases"):
            route_number, store_id = params
            canonical = self.conn.store_aliases.get((route_number, store_id))
            self._fetchone = (canonical,) if canonical else None
            return

        if normalized.startswith("insert into orders_historical"):
            order_id = params[0]
            self.conn.orders_historical[order_id] = {
                "order_id": order_id,
                "route_number": params[1],
                "user_id": params[2],
                "schedule_key": params[3],
                "delivery_date": params[4],
                "order_date": params[5],
                "total_units": params[6],
                "store_count": params[7],
                "is_holiday_week": params[8],
                "synced_at": params[9],
            }
            return

        if normalized.startswith("delete from order_line_items where order_id = %s"):
            order_id = params[0]
            self.conn.order_line_items = {
                key: row for key, row in self.conn.order_line_items.items() if row[1] != order_id
            }
            return

        if normalized.startswith("delete from forecast_corrections where order_id = %s"):
            order_id = params[0]
            self.conn.forecast_corrections = {
                key: row for key, row in self.conn.forecast_corrections.items() if row[2] != order_id
            }
            return

        raise AssertionError(f"Unexpected SQL in fake cursor: {sql}")

    def fetchone(self):
        return self._fetchone

    def close(self):
        return None


class _FakeConnection:
    def __init__(self):
        self.orders_historical = {}
        self.order_line_items = {}
        self.forecast_corrections = {}
        self.store_aliases = {}
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _fake_execute_values(cur, sql, rows, page_size=100):
    normalized = " ".join(sql.split()).lower()
    if "insert into order_line_items" in normalized:
        for row in rows:
            cur.conn.order_line_items[row[0]] = row
        return
    if "insert into forecast_corrections" in normalized:
        for row in rows:
            cur.conn.forecast_corrections[row[0]] = row
        return
    raise AssertionError(f"Unexpected execute_values SQL: {sql}")


class TestHandleSyncOrderProjectionReplacement(unittest.TestCase):
    def setUp(self):
        self.receipt_patcher = mock.patch.object(
            worker,
            "write_verified_order_archive_receipt",
            return_value={"status": "verified"},
        )
        self.receipt_mock = self.receipt_patcher.start()

    def tearDown(self):
        self.receipt_patcher.stop()

    def test_schema_v2_snapshot_records_omitted_zero_add_and_changed_amounts(self):
        route_number = "988200"
        order_id = "order-988200-v2"
        order = {
            "id": order_id,
            "routeNumber": route_number,
            "userId": "user-1",
            "expectedDeliveryDate": "2026-08-13",
            "orderDate": "2026-08-11",
            "scheduleKey": "tuesday",
            "forecastContext": {
                "schemaVersion": 2,
                "forecastId": "forecast-v2",
                "items": [
                    {"storeId": "store-1", "sap": "omitted", "recommendedUnits": 5, "source": "last_order"},
                    {"storeId": "store-1", "sap": "zero-add", "recommendedUnits": 0, "source": "order_only_zero"},
                    {"storeId": "store-1", "sap": "changed", "recommendedUnits": 6, "source": "baseline"},
                    {"storeId": "store-1", "sap": "exact", "recommendedUnits": 2, "source": "dense_zero"},
                ],
            },
            "stores": [{
                "storeId": "store-1",
                "storeName": "Store One",
                "items": [
                    {"sap": "zero-add", "quantity": 3, "cases": 0},
                    {"sap": "changed", "quantity": 4, "cases": 0},
                    {"sap": "exact", "quantity": 2, "cases": 0},
                ],
            }],
        }
        db = _FakeFirestoreDB({(route_number, order_id): order}, {})
        conn = _FakeConnection()

        with (
            mock.patch.object(worker, "execute_values", side_effect=_fake_execute_values),
            mock.patch.object(worker, "resolve_store_id_from_db", side_effect=lambda conn, route, store: store),
            mock.patch.object(worker, "is_holiday_week", return_value=(False, "")),
            mock.patch.object(worker.traceback, "print_exc"),
        ):
            result = worker.handle_sync_order(conn, db, {"orderId": order_id, "routeNumber": route_number})

        self.assertTrue(result["success"])
        self.assertTrue(result["archiveReceiptWritten"])
        self.receipt_mock.assert_called_once()
        self.assertEqual(result["correctionsExtracted"], 3)
        self.assertEqual(set(conn.forecast_corrections), {
            f"{order_id}-store-1-omitted-rm",
            f"{order_id}-store-1-zero-add-corr",
            f"{order_id}-store-1-changed-corr",
        })
        zero_add = conn.forecast_corrections[f"{order_id}-store-1-zero-add-corr"]
        changed = conn.forecast_corrections[f"{order_id}-store-1-changed-corr"]
        omitted = conn.forecast_corrections[f"{order_id}-store-1-omitted-rm"]
        self.assertEqual((zero_add[9], zero_add[11], zero_add[12], zero_add[14]), (0, "order_only_zero", 3, 3))
        self.assertEqual(changed[11], "baseline")
        self.assertEqual(omitted[11], "last_order")

    def test_receipt_write_failure_surfaces_after_postgres_commit(self):
        route_number = "989262"
        order_id = "order-receipt-failure"
        order = {
            "routeNumber": route_number,
            "userId": "user-1",
            "expectedDeliveryDate": "2026-08-17",
            "orderDate": "2026-08-14",
            "scheduleKey": "monday",
            "stores": [{
                "storeId": "store-1",
                "storeName": "Store One",
                "items": [{"sap": "100", "quantity": 2, "cases": 0}],
            }],
        }
        db = _FakeFirestoreDB({(route_number, order_id): order}, {})
        conn = _FakeConnection()
        self.receipt_mock.side_effect = RuntimeError("firestore receipt unavailable")

        with (
            mock.patch.object(worker, "execute_values", side_effect=_fake_execute_values),
            mock.patch.object(worker, "resolve_store_id_from_db", side_effect=lambda conn, route, store: store),
            mock.patch.object(worker, "is_holiday_week", return_value=(False, "")),
            mock.patch.object(worker.traceback, "print_exc"),
        ):
            result = worker.handle_sync_order(
                conn,
                db,
                {"orderId": order_id, "routeNumber": route_number},
            )

        self.assertIn("firestore receipt unavailable", result["error"])
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 1)
        self.assertIn(order_id, conn.orders_historical)

    def test_sync_order_replaces_removed_line_items_and_stale_corrections(self):
        route_number = "989262"
        order_id = "order-989262-1776772064245"

        first_order = {
            "id": order_id,
            "routeNumber": route_number,
            "userId": "user-1",
            "expectedDeliveryDate": "2026-04-27",
            "expectedLoadDate": "2026-04-26",
            "orderDate": "2026-04-21",
            "scheduleKey": "tuesday",
            "coreItemPolicyVersion": 1,
            "coreItemOverrides": [{"storeId": "store-1", "sap": "11111"}],
            "forecastId": "fc-1",
            "stores": [
                {
                    "storeId": "store-1",
                    "storeName": "Store One",
                    "items": [
                        {"sap": "11111", "quantity": 5, "cases": 0},
                        {"sap": "22222", "quantity": 4, "cases": 0},
                    ],
                }
            ],
        }
        second_order = {
            "id": order_id,
            "routeNumber": route_number,
            "userId": "user-1",
            "expectedDeliveryDate": "2026-04-27",
            "expectedLoadDate": "2026-04-26",
            "orderDate": "2026-04-21",
            "scheduleKey": "tuesday",
            "coreItemPolicyVersion": 1,
            "coreItemOverrides": [{"storeId": "store-1", "sap": "11111"}],
            "forecastId": "fc-1",
            "stores": [
                {
                    "storeId": "store-1",
                    "storeName": "Store One",
                    "items": [
                        {"sap": "11111", "quantity": 2, "cases": 0},
                    ],
                }
            ],
        }
        cached_forecasts = {
            route_number: [
                (
                    "forecast-doc-1",
                    {
                        "deliveryDate": "2026-04-27",
                        "scheduleKey": "tuesday",
                        "generatedAt": "2026-04-20T10:00:00Z",
                        "items": [
                            {
                                "storeId": "store-1",
                                "sap": "11111",
                                "recommendedUnits": 2,
                                "recommendedCases": 1,
                            }
                        ],
                    },
                )
            ]
        }
        orders_store = {(route_number, order_id): first_order}
        db = _FakeFirestoreDB(orders_store, cached_forecasts)
        conn = _FakeConnection()

        with (
            mock.patch.object(worker, "execute_values", side_effect=_fake_execute_values),
            mock.patch.object(worker, "resolve_store_id_from_db", side_effect=lambda conn, route, store: store),
            mock.patch.object(worker, "is_holiday_week", return_value=(False, "")),
            mock.patch.object(worker, "_load_archived_forecast", return_value=(None, None)),
        ):
            first_result = worker.handle_sync_order(conn, db, {"orderId": order_id, "routeNumber": route_number})
            self.assertTrue(first_result["success"])
            self.assertEqual(set(conn.order_line_items.keys()), {
                f"{order_id}-store-1-11111",
                f"{order_id}-store-1-22222",
            })
            self.assertEqual(set(conn.forecast_corrections.keys()), {
                f"{order_id}-store-1-11111-corr",
                f"{order_id}-store-1-22222-add",
            })

            orders_store[(route_number, order_id)] = second_order
            second_result = worker.handle_sync_order(conn, db, {"orderId": order_id, "routeNumber": route_number})
            self.assertTrue(second_result["success"])

        self.assertEqual(set(conn.order_line_items.keys()), {
            f"{order_id}-store-1-11111",
        })
        self.assertEqual(conn.order_line_items[f"{order_id}-store-1-11111"][8], 2)
        self.assertEqual(conn.forecast_corrections, {})
        self.assertEqual(conn.rollbacks, 0)
        self.assertEqual(conn.commits, 2)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from order_forecast.scripts.forecast_engine import (
    _get_prior_order_context,
    _resolve_store_delivery_date,
)
from order_forecast.scripts.models import StoreConfig


class _FakeOrderDocument:
    def __init__(self, order_id: str, data: dict):
        self.id = order_id
        self._data = data

    def to_dict(self):
        return self._data


class _FakeOrdersCollection:
    def __init__(self, documents):
        self._documents = documents

    def where(self, **_kwargs):
        return self

    def stream(self):
        return self._documents


class _FakeRouteDocument:
    def __init__(self, documents):
        self._documents = documents

    def collection(self, name: str):
        assert name == "orders"
        return _FakeOrdersCollection(self._documents)


class _FakeRoutesCollection:
    def __init__(self, documents):
        self._documents = documents

    def document(self, _route_number: str):
        return _FakeRouteDocument(self._documents)


class _FakeFirestore:
    def __init__(self, documents):
        self._documents = documents

    def collection(self, name: str):
        assert name == "routes"
        return _FakeRoutesCollection(self._documents)


def _finalized_order(*, expected_delivery_date: str) -> dict:
    return {
        "status": "finalized",
        "orderDate": "2026-07-27",
        "scheduleKey": "monday",
        "expectedDeliveryDate": expected_delivery_date,
        "stores": [
            {
                "storeId": "walmart",
                "storeName": "Walmart",
                "items": [{"sap": "28934", "quantity": 105}],
            }
        ],
    }


def test_saved_delivery_date_wins_when_store_has_multiple_delivery_days():
    resolved = _resolve_store_delivery_date(
        "2026-07-30",
        "2026-07-27",
        ["Monday", "Thursday"],
    )

    assert resolved == "2026-07-30"


def test_exception_store_uses_first_delivery_after_order_day():
    resolved = _resolve_store_delivery_date(
        "2026-07-30",
        "2026-07-27",
        ["Friday"],
    )

    assert resolved == "2026-07-31"


def test_delivered_monday_order_does_not_warn_on_tuesday_order():
    db = _FakeFirestore(
        [_FakeOrderDocument("order-monday", _finalized_order(expected_delivery_date="2026-07-30"))]
    )
    stores = [
        StoreConfig(
            store_id="walmart",
            store_name="Walmart",
            delivery_days=["Monday", "Thursday"],
        )
    ]

    context = _get_prior_order_context(
        db,
        route_number="989262",
        current_schedule_key="tuesday",
        current_order_date="2026-07-28",
        target_delivery_date="2026-08-03",
        stores_cfg=stores,
    )

    assert context == {}


def test_same_delivery_window_still_reports_prior_order():
    db = _FakeFirestore(
        [_FakeOrderDocument("order-monday", _finalized_order(expected_delivery_date="2026-07-30"))]
    )
    stores = [
        StoreConfig(
            store_id="walmart",
            store_name="Walmart",
            delivery_days=["Friday"],
        )
    ]

    context = _get_prior_order_context(
        db,
        route_number="989262",
        current_schedule_key="tuesday",
        current_order_date="2026-07-28",
        target_delivery_date="2026-08-03",
        stores_cfg=stores,
    )

    assert context[("walmart", "28934")].delivery_date == "2026-07-31"


def test_exception_store_delivered_before_current_cycle_does_not_warn():
    db = _FakeFirestore(
        [_FakeOrderDocument("order-monday", _finalized_order(expected_delivery_date="2026-07-30"))]
    )
    stores = [
        StoreConfig(
            store_id="walmart",
            store_name="Walmart",
            delivery_days=["Tuesday"],
        )
    ]

    context = _get_prior_order_context(
        db,
        route_number="989262",
        current_schedule_key="tuesday",
        current_order_date="2026-07-28",
        target_delivery_date="2026-08-03",
        stores_cfg=stores,
    )

    assert context == {}

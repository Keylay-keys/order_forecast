from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from order_forecast.api import pcf_active_serving as serving


NOW = datetime(2026, 8, 25, 18, 0, tzinfo=timezone.utc)
REVISION = "fsut1|2026-08-25T17:59:00.000000000Z|0"


def _row(
    *,
    delivery_id: str = "1806249027",
    container_id: str = "C01",
    loading_date: date = date(2026, 8, 25),
    page_numbers: list[int] | None = None,
    items: list[dict] | None = None,
) -> dict:
    return {
        "route_number": "988200",
        "delivery_id": delivery_id,
        "container_id": container_id,
        "loading_date": loading_date,
        "page_numbers": [1] if page_numbers is None else page_numbers,
        "items": [] if items is None else items,
        "source_revision": REVISION,
        "updated_at": NOW - timedelta(seconds=10),
    }


def _item(
    *,
    position: int = 0,
    product: str = "1001",
    description: str = "Projected item",
    expiry: str | None = "2026-08-30",
    visible: bool = True,
    low_quantity: bool = False,
    short_coded: bool = False,
    guaranteed: dict | None = None,
) -> dict:
    return {
        "pageNumber": 1,
        "itemPosition": position,
        "product": product,
        "description": description,
        "expiryDate": expiry,
        "visibleOnList": visible,
        "isLowQuantity": low_quantity,
        "isShortCoded": short_coded,
        "guaranteed": guaranteed,
    }


def _state() -> serving.RouteProjectionState:
    return serving.RouteProjectionState(
        route_number="988200",
        last_projected_revision=REVISION,
        healthy_until=NOW + timedelta(minutes=5),
    )


class PcfActiveServingTests(unittest.TestCase):
    def test_firestore_controlled_item_fields_and_empty_page_are_served(self):
        controlled = _item(
            product="CORRECTED-1001",
            description="Corrected description",
            visible=False,
            low_quantity=True,
            short_coded=True,
            guaranteed={
                "isGuaranteed": True,
                "guaranteeExpiresAt": "2026-09-30T23:59:59.000000000Z",
            },
        )
        container = serving._container_from_row(
            _row(page_numbers=[1, 2], items=[controlled]),
            business_date=date(2026, 8, 25),
        )

        self.assertEqual(container["pageCount"], 2)
        self.assertEqual(container["pages"][1], {"pageNumber": 2, "items": []})
        projected = container["pages"][0]["items"][0]
        self.assertEqual(projected["product"], "CORRECTED-1001")
        self.assertEqual(projected["description"], "Corrected description")
        self.assertEqual(projected["days"], "5*")
        self.assertTrue(projected["isShortCoded"])
        self.assertFalse(projected["visibleOnList"])
        self.assertTrue(projected["isLowQuantity"])
        self.assertEqual(
            projected["guaranteed"],
            {
                "isGuaranteed": True,
                "guaranteeExpiresAt": "2026-09-30T23:59:59.000000000Z",
            },
        )

    def test_removed_guarantee_is_absent_without_changing_other_controls(self):
        container = serving._container_from_row(
            _row(items=[_item(visible=False, guaranteed=None)]),
            business_date=date(2026, 8, 25),
        )
        projected = container["pages"][0]["items"][0]
        self.assertNotIn("guaranteed", projected)
        self.assertFalse(projected["visibleOnList"])

    def test_expiry_counters_are_recomputed_at_denver_business_boundaries(self):
        items = [
            _item(position=0, expiry="2026-08-25"),
            _item(position=1, expiry="2026-09-01"),
            _item(position=2, expiry="2026-09-02"),
            _item(position=3, expiry=None),
        ]
        container = serving._container_from_row(
            _row(items=items), business_date=date(2026, 8, 25)
        )

        self.assertEqual(container["expiredItems"], 1)
        self.assertEqual(container["expiringItems"], 1)
        self.assertEqual(
            [item["days"] for item in container["pages"][0]["items"]],
            ["0", "7", "8", ""],
        )

    def test_invalid_expiry_and_missing_page_identity_fail_closed(self):
        with self.assertRaisesRegex(serving.ProjectionContractError, "invalid expiryDate"):
            serving._container_from_row(
                _row(items=[_item(expiry="08/30/2026")]),
                business_date=date(2026, 8, 25),
            )
        with self.assertRaisesRegex(serving.ProjectionContractError, "absent page"):
            serving._container_from_row(
                _row(page_numbers=[2], items=[_item()]),
                business_date=date(2026, 8, 25),
            )

    def test_list_is_chronological_across_year_boundary(self):
        rows = [
            _row(delivery_id="2025", loading_date=date(2025, 12, 31)),
            _row(delivery_id="2026", loading_date=date(2026, 1, 1)),
        ]
        with (
            patch.object(serving, "_load_route_state", return_value=_state()),
            patch.object(serving, "_load_rows", return_value=rows),
        ):
            result = serving.list_active_deliveries_from_postgres(
                object(), "988200", now_utc=NOW
            )

        self.assertEqual(
            [delivery["deliveryNumber"] for delivery in result["items"]],
            ["2026", "2025"],
        )
        self.assertEqual(result["source"], "postgres")

    def test_detail_returns_none_only_after_projection_gate_passes(self):
        with (
            patch.object(serving, "_load_route_state", return_value=_state()),
            patch.object(serving, "_load_rows", return_value=[]),
        ):
            self.assertIsNone(
                serving.get_active_delivery_from_postgres(
                    object(), "988200", "missing", now_utc=NOW
                )
            )

    def test_route_state_is_fail_closed_for_each_required_proof(self):
        complete = {
            "route_number": "988200",
            "eligible": True,
            "serving_enabled": True,
            "reconciliation_status": "matched",
            "last_projected_revision": REVISION,
            "parity_source_revision": REVISION,
            "parity_projection_revision": REVISION,
            "source_entity_count": 1,
            "projection_entity_count": 1,
            "source_business_hash": "a" * 64,
            "projection_business_hash": "a" * 64,
            "healthy_until": NOW + timedelta(minutes=5),
        }
        cases = (
            (None, "route_state_missing"),
            ({**complete, "serving_enabled": False}, "route_not_enabled"),
            ({**complete, "reconciliation_status": "mismatched"}, "projection_not_reconciled"),
            ({**complete, "parity_projection_revision": "other"}, "projection_revision_mismatch"),
            ({**complete, "projection_entity_count": 2}, "projection_count_mismatch"),
            ({**complete, "projection_business_hash": "b" * 64}, "projection_hash_mismatch"),
            ({**complete, "healthy_until": NOW}, "projection_health_expired"),
        )
        for row, reason in cases:
            with self.subTest(reason=reason):
                conn = _StateConnection(row)
                with self.assertRaises(serving.ProjectionUnavailable) as raised:
                    serving._load_route_state(conn, "988200", now_utc=NOW)
                self.assertEqual(raised.exception.reason, reason)

    def test_module_has_no_firebase_fallback_dependency(self):
        source = serving.__loader__.get_source(serving.__name__)
        self.assertIsNotNone(source)
        self.assertNotIn("firebase_admin", source)
        self.assertNotIn("get_firestore", source)


class _StateCursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return self.row


class _StateConnection:
    def __init__(self, row):
        self.row = row

    def cursor(self, *args, **kwargs):
        return _StateCursor(self.row)


if __name__ == "__main__":
    unittest.main()

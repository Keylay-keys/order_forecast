#!/usr/bin/env python3
"""Tests for the test-lab-only hierarchical hurdle candidate."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

try:
    from .experimental_hierarchical_hurdle import (
        build_dense_hurdle_panel,
        predict_hierarchical_hurdle,
        predict_store_sap_reconciled,
        predict_store_first_hierarchy,
        predict_store_first_case_hierarchy,
    )
    from .models import Order, OrderItem, StoreOrder
except ImportError:
    from experimental_hierarchical_hurdle import (
        build_dense_hurdle_panel,
        predict_hierarchical_hurdle,
        predict_store_sap_reconciled,
        predict_store_first_hierarchy,
        predict_store_first_case_hierarchy,
    )
    from models import Order, OrderItem, StoreOrder


def _order(index: int, quantities: dict[str, int], schedule_key: str = "monday") -> Order:
    delivery = datetime(2026, 1, 1) + timedelta(days=index * 7)
    return Order(
        id=f"{schedule_key}-{index}",
        route_number="989262",
        schedule_key=schedule_key,
        expected_delivery_date=delivery.strftime("%Y-%m-%d"),
        order_date=(delivery - timedelta(days=3)).strftime("%Y-%m-%d"),
        stores=[
            StoreOrder(
                store_id="store-1",
                store_name="Store 1",
                items=[OrderItem(sap=sap, quantity=units) for sap, units in quantities.items()],
            )
        ],
    )


class HierarchicalHurdleTests(unittest.TestCase):
    def test_dense_panel_materializes_absent_known_lines_as_zero(self) -> None:
        orders = [
            _order(0, {"A": 10}),
            _order(1, {}),
            _order(2, {"A": 12}),
        ]

        panel, state = build_dense_hurdle_panel(orders, "monday")

        self.assertEqual(panel["y_occurrence"].tolist(), [0.0, 1.0])
        self.assertEqual(panel["y_units"].tolist(), [0.0, 12.0])
        self.assertEqual(state["known_keys"], {("store-1", "A")})

    def test_panel_uses_only_requested_schedule(self) -> None:
        monday = [_order(index, {"A": 10}) for index in range(3)]
        tuesday = [_order(index, {"B": 500}, "tuesday") for index in range(3)]

        panel, state = build_dense_hurdle_panel(monday + tuesday, "monday")

        self.assertEqual(state["known_keys"], {("store-1", "A")})
        self.assertEqual(set(panel["sap"]), {"A"})

    def test_prediction_contains_occurrence_and_conditional_quantity(self) -> None:
        orders = []
        for index in range(12):
            quantities = {"A": 10 + (index % 2)}
            if index % 3 == 0:
                quantities["B"] = 6
            orders.append(_order(index, quantities))

        predictions, stats = predict_hierarchical_hurdle(
            orders,
            "monday",
            datetime(2026, 3, 26),
        )

        self.assertEqual(set(predictions["sap"]), {"A", "B"})
        self.assertGreater(stats["dense_zero_rows"], 0)
        self.assertGreater(stats["positive_rows"], 0)
        self.assertTrue((predictions["occurrence_probability"] >= 0).all())
        self.assertTrue((predictions["occurrence_probability"] <= 1).all())
        self.assertTrue((predictions["conditional_positive_units"] >= 0).all())

    def test_store_sap_reconciliation_places_more_on_dominant_cycle(self) -> None:
        orders = []
        for index in range(12):
            orders.append(_order(index * 2, {"A": 20}, "monday"))
            orders.append(_order(index * 2 + 1, {"A": 5}, "tuesday"))
        target = datetime(2026, 7, 1)

        monday, monday_stats = predict_store_sap_reconciled(orders, "monday", target)
        tuesday, _ = predict_store_sap_reconciled(orders, "tuesday", target)

        self.assertEqual(monday_stats["target_keys"], 1)
        self.assertGreater(float(monday.iloc[0]["allocation_share"]), 0.5)
        self.assertGreater(
            float(monday.iloc[0]["pred_units"]),
            float(tuesday.iloc[0]["pred_units"]),
        )

    def test_store_sap_reconciliation_uses_zero_orders_in_recent_signal(self) -> None:
        steady = [_order(index, {"A": 20}) for index in range(8)]
        with_zeros = steady + [_order(index, {}) for index in range(8, 12)]
        target = datetime(2026, 3, 26)

        steady_prediction, _ = predict_store_sap_reconciled(steady, "monday", target)
        zero_prediction, _ = predict_store_sap_reconciled(with_zeros, "monday", target)

        self.assertGreater(
            float(steady_prediction.iloc[0]["pred_units"]),
            float(zero_prediction.iloc[0]["pred_units"]),
        )

    def test_store_first_hierarchy_reconciles_saps_to_store_total(self) -> None:
        orders = []
        for index in range(12):
            orders.append(_order(index, {"A": 20, "B": 10}))

        predictions, stats = predict_store_first_hierarchy(
            orders,
            "monday",
            datetime(2026, 3, 26),
        )

        self.assertEqual(stats["stores_reconciled"], 1)
        self.assertAlmostEqual(float(predictions["pred_units"].sum()), 30.0, places=6)
        self.assertAlmostEqual(
            float(predictions["store_total_target"].iloc[0]), 30.0, places=6
        )
        self.assertGreater(
            float(predictions[predictions["sap"] == "A"]["pred_units"].iloc[0]),
            float(predictions[predictions["sap"] == "B"]["pred_units"].iloc[0]),
        )

    def test_case_hierarchy_makes_one_whole_case_decision_per_sap(self) -> None:
        orders = [_order(index, {"A": 10, "B": 3}) for index in range(12)]

        predictions, stats = predict_store_first_case_hierarchy(
            orders,
            "monday",
            datetime(2026, 3, 26),
            {"A": 12, "B": 12},
        )

        sap_totals = predictions.groupby("sap")["pred_units"].sum().to_dict()
        self.assertEqual(sap_totals["A"] % 12, 0)
        self.assertEqual(sap_totals["B"] % 12, 0)
        self.assertEqual(sap_totals["A"], 12)
        self.assertEqual(sap_totals["B"], 0)
        self.assertEqual(stats["sap_case_decisions"], 2)
        self.assertEqual(stats["sap_case_zeroes"], 1)


if __name__ == "__main__":
    unittest.main()

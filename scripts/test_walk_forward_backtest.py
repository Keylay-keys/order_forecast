#!/usr/bin/env python3
"""Correctness regressions for the walk-forward forecast backtester."""

from __future__ import annotations

import math
import os
import unittest
from datetime import datetime
from unittest.mock import patch

import numpy as np

try:
    from . import walk_forward_backtest as backtest
    from .models import Order, OrderItem, StoreOrder
except ImportError:
    import walk_forward_backtest as backtest
    from models import Order, OrderItem, StoreOrder


def _order(
    order_id: str,
    delivery_date: str,
    order_date: str | None,
    quantities: dict[str, int],
    schedule_key: str = "monday",
) -> Order:
    return Order(
        id=order_id,
        route_number="989262",
        schedule_key=schedule_key,
        expected_delivery_date=delivery_date,
        order_date=order_date,
        stores=[
            StoreOrder(
                store_id="store-1",
                store_name="Store 1",
                items=[OrderItem(sap=sap, quantity=qty) for sap, qty in quantities.items()],
            )
        ],
    )


class WalkForwardBacktestCorrectnessTests(unittest.TestCase):
    def test_ordered_units_train_without_forecast_metadata(self) -> None:
        actual_only = _order("actual-only", "2026-01-08", "2026-01-05", {"A": 17})

        self.assertIsNone(actual_only.stores[0].items[0].forecasted_quantity)
        frame = backtest._orders_to_dataframe([actual_only])

        self.assertEqual(frame["units"].tolist(), [17])
        self.assertEqual(frame["sap"].tolist(), ["A"])

    def test_continuous_actual_learning_bypasses_only_correction_gate(self) -> None:
        orders = [
            _order(
                f"order-{index}",
                f"2026-01-{index + 1:02d}",
                f"2025-12-{index + 1:02d}",
                {"A": index + 1},
            )
            for index in range(8)
        ]
        environment = {
            "FORECAST_MIN_SCHEDULE_ORDERS_FOR_ML": "8",
            "FORECAST_MIN_CORRECTED_ORDERS_FOR_ML": "3",
            "FORECAST_STRICT_SCHEDULE_VALIDATION": "0",
        }
        with (
            patch.dict(os.environ, environment, clear=False),
            patch.object(backtest, "_temporal_corrected_orders_count", return_value=0),
        ):
            baseline = backtest._resolve_mode_for_fold(
                route_number="989262",
                schedule_key="monday",
                train_orders_all=orders,
                order_cycles=[],
                cutoff_dt=datetime(2026, 2, 1),
                since_days=365,
                allow_store_context=False,
                held_out_order_id="held-out",
                target_delivery_dt=datetime(2026, 2, 2),
            )
            candidate = backtest._resolve_mode_for_fold(
                route_number="989262",
                schedule_key="monday",
                train_orders_all=orders,
                order_cycles=[],
                cutoff_dt=datetime(2026, 2, 1),
                since_days=365,
                allow_store_context=False,
                held_out_order_id="held-out",
                target_delivery_dt=datetime(2026, 2, 2),
                continuous_actual_learning=True,
            )

        self.assertEqual(baseline[0], "copy_last_order")
        self.assertEqual(candidate[0], "schedule_aware")
        self.assertEqual(candidate[3:5], (8, 0))
        self.assertTrue(candidate[5])
        self.assertIn("learning_target=finalized_order_quantities", candidate[2])
        self.assertIn("correction_gate_bypassed=true", candidate[2])

    def test_continuous_actual_learning_keeps_history_gate(self) -> None:
        orders = [
            _order(
                f"order-{index}",
                f"2026-01-{index + 1:02d}",
                f"2025-12-{index + 1:02d}",
                {"A": index + 1},
            )
            for index in range(7)
        ]
        with (
            patch.dict(
                os.environ,
                {
                    "FORECAST_MIN_SCHEDULE_ORDERS_FOR_ML": "8",
                    "FORECAST_MIN_CORRECTED_ORDERS_FOR_ML": "3",
                },
                clear=False,
            ),
            patch.object(backtest, "_temporal_corrected_orders_count", return_value=0),
        ):
            decision = backtest._resolve_mode_for_fold(
                route_number="989262",
                schedule_key="monday",
                train_orders_all=orders,
                order_cycles=[],
                cutoff_dt=datetime(2026, 2, 1),
                since_days=365,
                allow_store_context=False,
                held_out_order_id="held-out",
                target_delivery_dt=datetime(2026, 2, 2),
                continuous_actual_learning=True,
            )

        self.assertEqual(decision[0], "copy_last_order")
        self.assertFalse(decision[5])
        self.assertIn("schedule_orders=7<8", decision[2])

    def test_last_order_baseline_is_exact_and_input_order_independent(self) -> None:
        older = _order("older", "2026-01-01", "2025-12-29", {"A": 10})
        latest = _order("latest", "2026-01-08", "2026-01-05", {"B": 5})

        expected = {("store-1", "B"): 5}
        self.assertEqual(backtest._build_last_value_map([older, latest], "monday"), expected)
        self.assertEqual(backtest._build_last_value_map([latest, older], "monday"), expected)

    def test_last_order_baseline_uses_same_schedule(self) -> None:
        monday = _order("monday", "2026-01-05", "2026-01-02", {"A": 10})
        later_tuesday = _order(
            "tuesday", "2026-01-06", "2026-01-03", {"B": 20}, schedule_key="tuesday"
        )

        self.assertEqual(
            backtest._build_last_value_map([monday, later_tuesday], "monday"),
            {("store-1", "A"): 10},
        )

    def test_as_of_selection_uses_order_date_and_excludes_same_day(self) -> None:
        available = _order("available", "2026-01-08", "2026-01-05", {"A": 10})
        same_day = _order("same-day", "2026-01-14", "2026-01-12", {"A": 20})
        held_out = _order("held-out", "2026-01-15", "2026-01-12", {"A": 30})

        selected, cutoff = backtest._select_as_of_train_orders(
            [held_out, same_day, available], held_out
        )

        self.assertEqual([order.id for order in selected], ["available"])
        self.assertEqual(cutoff, datetime(2026, 1, 12))

    def test_zero_actual_wape_is_undefined(self) -> None:
        value = backtest._wape(np.array([0.0]), np.array([100.0]))
        self.assertTrue(math.isnan(value))

    def test_temporal_corrections_exclude_held_out_and_target_delivery(self) -> None:
        with patch.object(backtest, "fetch_all", return_value=[]) as fetch:
            result = backtest._load_temporal_corrections(
                "989262",
                "monday",
                datetime(2026, 1, 12),
                held_out_order_id="held-out",
                target_delivery_dt=datetime(2026, 1, 15),
            )

        self.assertIsNone(result)
        sql, params = fetch.call_args.args
        self.assertIn("delivery_date < %s", sql)
        self.assertIn("order_id <> %s", sql)
        self.assertEqual(params[3], datetime(2026, 1, 15).date())
        self.assertEqual(params[4], "held-out")

    def test_one_cycle_removals_use_immediately_prior_same_schedule_order(self) -> None:
        monday_old = _order("monday-old", "2026-01-01", "2025-12-29", {"A": 10})
        monday_latest = _order("monday-latest", "2026-01-08", "2026-01-05", {"B": 5})
        tuesday_later = _order(
            "tuesday-later", "2026-01-09", "2026-01-06", {"C": 8}, schedule_key="tuesday"
        )
        with patch.object(
            backtest,
            "fetch_all",
            return_value=[{"store_id": "store-1", "sap": "B"}],
        ) as fetch:
            removed = backtest._load_test_one_cycle_removed_keys(
                route_number="989262",
                schedule_key="monday",
                train_orders=[monday_old, tuesday_later, monday_latest],
                cutoff_dt=datetime(2026, 1, 12),
                target_delivery_dt=datetime(2026, 1, 15),
            )

        self.assertEqual(removed, {("store-1", "B")})
        sql, params = fetch.call_args.args
        self.assertIn("ROW_NUMBER() OVER", sql)
        self.assertIn("submitted_at < %s", sql)
        self.assertIn("delivery_date < %s", sql)
        self.assertEqual(params[2], "monday-latest")
        self.assertEqual(params[3], datetime(2026, 1, 12))
        self.assertEqual(params[4], datetime(2026, 1, 15).date())

    def test_one_cycle_slow_cooldown_uses_actual_absence_and_corrections(self) -> None:
        prior = _order("prior", "2026-01-08", "2026-01-05", {"present": 5, "removed": 3})
        pred = {
            ("store-1", "present"): 7,
            ("store-1", "absent"): 6,
            ("store-1", "removed"): 4,
            ("store-1", "gbr-absent"): 9,
        }
        sources = {
            ("store-1", "present"): "slow_intermittent",
            ("store-1", "absent"): "slow_intermittent",
            ("store-1", "removed"): "slow_intermittent",
            ("store-1", "gbr-absent"): "gbr",
        }

        keys, stats = backtest._build_test_one_cycle_slow_cooldown_keys(
            train_orders=[prior],
            schedule_key="monday",
            pred_line_map=pred,
            key_source_map=sources,
            key_meta_map={},
            removed_keys={("store-1", "removed")},
        )

        self.assertEqual(keys, {("store-1", "absent"), ("store-1", "removed")})
        self.assertEqual(stats["slow_positive_predictions"], 3)
        self.assertEqual(stats["prior_actual_absence_keys"], 1)
        self.assertEqual(stats["explicit_removal_keys"], 1)
        self.assertEqual(stats["both_signal_keys"], 0)

    def test_strict_slow_cooldown_excludes_branch_only_moderate_frequency_lines(self) -> None:
        prior = _order("prior", "2026-01-08", "2026-01-05", {"present": 5})
        pred = {
            ("store-1", "true-slow"): 6,
            ("store-1", "branch-only"): 9,
            ("store-1", "flagged-gbr"): 4,
        }
        sources = {
            ("store-1", "true-slow"): "slow_intermittent",
            ("store-1", "branch-only"): "slow_intermittent",
            ("store-1", "flagged-gbr"): "gbr",
        }
        metadata = {
            ("store-1", "true-slow"): {"is_slow_mover": 1.0},
            ("store-1", "branch-only"): {"is_slow_mover": 0.0},
            ("store-1", "flagged-gbr"): {"is_slow_mover": 1.0},
        }

        keys, stats = backtest._build_test_one_cycle_slow_cooldown_keys(
            train_orders=[prior],
            schedule_key="monday",
            pred_line_map=pred,
            key_source_map=sources,
            key_meta_map=metadata,
            removed_keys=set(),
            strict_slow_only=True,
        )

        self.assertEqual(
            keys,
            {("store-1", "true-slow"), ("store-1", "flagged-gbr")},
        )
        self.assertEqual(stats["slow_positive_predictions"], 2)
        self.assertEqual(stats["prior_actual_absence_keys"], 2)

    def test_one_cycle_slow_cooldown_zeroes_only_positive_matching_lines(self) -> None:
        pred = {("store-1", "A"): 12, ("store-1", "B"): 4}
        bands = {
            ("store-1", "A"): (8.0, 12.0, 16.0, 0.7),
            ("store-1", "B"): (2.0, 4.0, 6.0, 0.6),
        }
        sources = {("store-1", "A"): "slow_intermittent", ("store-1", "B"): "gbr"}

        stats = backtest._apply_test_one_cycle_slow_cooldown(
            pred_line_map=pred,
            pred_band_map=bands,
            key_source_map=sources,
            removed_keys={("store-1", "A"), ("store-1", "missing")},
            actual_line_map={("store-1", "A"): 0, ("store-1", "B"): 5},
        )

        self.assertEqual(pred[("store-1", "A")], 0)
        self.assertEqual(pred[("store-1", "B")], 4)
        self.assertEqual(bands[("store-1", "A")], (0.0, 0.0, 0.0, 1.0))
        self.assertEqual(sources[("store-1", "A")], "test_slow_mover_cooldown")
        self.assertEqual(stats["eligible_cooldown_keys"], 2)
        self.assertEqual(stats["lines_zeroed"], 1)
        self.assertEqual(stats["units_suppressed"], 12)
        self.assertEqual(stats["lines_helped"], 1)
        self.assertEqual(stats["lines_harmed"], 0)

    def test_transient_spike_quarantine_is_past_only_and_does_not_mutate_actuals(self) -> None:
        orders = [
            _order(f"prior-{index}", f"2026-01-{index + 1:02d}", "2025-12-20", {"A": 10})
            for index in range(4)
        ]
        orders.append(_order("spike", "2026-01-05", "2025-12-21", {"A": 100}))

        model_orders, stats = backtest._apply_test_transient_spike_quarantine(orders)

        self.assertEqual(orders[-1].stores[0].items[0].quantity, 100)
        self.assertEqual(model_orders[-1].stores[0].items[0].quantity, 30.0)
        self.assertEqual(stats["eligible_rows"], 1)
        self.assertEqual(stats["rows_quarantined"], 1)
        self.assertEqual(stats["units_quarantined"], 70)

    def test_transient_spike_quarantine_requires_same_schedule_history(self) -> None:
        other_cycle = [
            _order(
                f"tuesday-{index}",
                f"2026-01-{index + 1:02d}",
                "2025-12-20",
                {"A": 10},
                schedule_key="tuesday",
            )
            for index in range(4)
        ]
        monday = _order("monday-spike", "2026-01-05", "2025-12-21", {"A": 100})

        model_orders, stats = backtest._apply_test_transient_spike_quarantine(
            other_cycle + [monday]
        )

        self.assertEqual(model_orders[-1].stores[0].items[0].quantity, 100)
        self.assertEqual(stats["rows_quarantined"], 0)

    def test_inferred_lift_starts_holds_and_returns_to_user_normal(self) -> None:
        history = [
            _order(f"normal-{index}", f"2026-01-{index + 1:02d}", "2025-12-20", {"A": 10})
            for index in range(4)
        ]
        spike = _order("lift-1", "2026-01-05", "2025-12-21", {"A": 100})

        model_orders, lift_map, stats = backtest._apply_test_inferred_lift_regime(
            history + [spike], "monday"
        )

        self.assertEqual(spike.stores[0].items[0].quantity, 100)
        self.assertEqual(model_orders[-1].stores[0].items[0].quantity, 10.0)
        self.assertEqual(lift_map, {("store-1", "A"): 30})
        self.assertEqual(stats["regimes_started"], 1)

        elevated = _order("lift-2", "2026-01-06", "2025-12-22", {"A": 25})
        _, held_lift_map, held_stats = backtest._apply_test_inferred_lift_regime(
            history + [spike, elevated], "monday"
        )
        self.assertEqual(held_lift_map, {("store-1", "A"): 30})
        self.assertEqual(held_stats["active_lift_keys"], 1)

        returned = _order("normal-return", "2026-01-07", "2025-12-23", {"A": 14})
        returned_orders, returned_lift_map, returned_stats = (
            backtest._apply_test_inferred_lift_regime(
                history + [spike, elevated, returned], "monday"
            )
        )
        self.assertEqual(returned_lift_map, {})
        self.assertEqual(returned_orders[-1].stores[0].items[0].quantity, 14)
        self.assertEqual(returned_stats["regimes_ended_by_normal"], 1)

    def test_inferred_lift_override_reports_held_out_effect(self) -> None:
        predictions = {("store-1", "A"): 10}
        bands = {("store-1", "A"): (7.0, 10.0, 13.0, 0.7)}
        sources = {("store-1", "A"): "gbr"}

        stats = backtest._apply_test_inferred_lift_overrides(
            pred_line_map=predictions,
            pred_band_map=bands,
            key_source_map=sources,
            lift_map={("store-1", "A"): 30},
            actual_line_map={("store-1", "A"): 25},
        )

        self.assertEqual(predictions[("store-1", "A")], 30)
        self.assertEqual(sources[("store-1", "A")], "test_inferred_transient_lift")
        self.assertEqual(stats["lines_helped"], 1)
        self.assertEqual(stats["lines_harmed"], 0)


if __name__ == "__main__":
    unittest.main()

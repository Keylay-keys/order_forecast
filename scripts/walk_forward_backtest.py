#!/usr/bin/env python3
"""Walk-forward backtest runner for RouteSpark forecasting.

Generates two CSVs:
1) Fold-level metrics (per route/schedule/test delivery)
2) Route scorecard summary (aggregated by route/schedule)

Default output:
  order_forecast/logs/backtests/
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import subprocess
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

# Support running as a script without package context
try:
    from .firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from .experimental_hierarchical_hurdle import (
        predict_hierarchical_hurdle,
        predict_store_first_case_hierarchy,
        predict_store_first_hierarchy,
        predict_store_sap_reconciled,
    )
    from .experimental_settlement_truth import apply_settlement_store_share
    from .forecast_engine import (
        MIN_CORRECTED_ORDERS_FOR_TRAINING,
        MIN_ORDERS_FOR_TRAINING,
        _build_features,
        _compute_schedule_features,
        _load_corrections_from_postgres,
        _orders_to_dataframe,
        _parse_date,
        _should_zero_low_signal_slow_line,
        _summarize_schedule_shape,
        _train_and_predict,
    )
    from .model_validator import validate_predictions
    from .pg_utils import fetch_all, fetch_one
except ImportError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from experimental_hierarchical_hurdle import (
        predict_hierarchical_hurdle,
        predict_store_first_case_hierarchy,
        predict_store_first_hierarchy,
        predict_store_sap_reconciled,
    )
    from experimental_settlement_truth import apply_settlement_store_share
    from forecast_engine import (
        MIN_CORRECTED_ORDERS_FOR_TRAINING,
        MIN_ORDERS_FOR_TRAINING,
        _build_features,
        _compute_schedule_features,
        _load_corrections_from_postgres,
        _orders_to_dataframe,
        _parse_date,
        _should_zero_low_signal_slow_line,
        _summarize_schedule_shape,
        _train_and_predict,
    )
    from model_validator import validate_predictions
    from pg_utils import fetch_all, fetch_one


LineKey = Tuple[str, str]  # (store_id, sap)
BandTuple = Tuple[float, float, float, float]  # (p10, p50, p90, confidence)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_revision() -> Optional[str]:
    repo_dir = Path(__file__).resolve().parents[1]
    try:
        return subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip() or None
    except Exception:
        return None


def _to_int_units(value: float | int | None) -> int:
    if value is None:
        return 0
    try:
        return int(round(float(value)))
    except Exception:
        return 0


def _wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = float(np.sum(np.abs(y_true)))
    if denom <= 0:
        # WAPE is undefined when the actual volume is zero. Returning zero makes
        # an arbitrarily large false-positive forecast look perfect.
        return float("nan")
    return float(np.sum(np.abs(y_true - y_pred)) / denom)


def _order_delivery_dt(order) -> Optional[datetime]:
    return _parse_date(order.expected_delivery_date or "")


def _order_to_line_map(order) -> Dict[LineKey, int]:
    out: Dict[LineKey, int] = {}
    for store in order.stores:
        sid = str(store.store_id or "")
        if not sid:
            continue
        for item in store.items:
            sap = str(item.sap or "")
            if not sap:
                continue
            qty = _to_int_units(item.quantity)
            if qty <= 0:
                continue
            out[(sid, sap)] = out.get((sid, sap), 0) + qty
    return out


def _build_last_value_map(train_orders, schedule_key: Optional[str] = None) -> Dict[LineKey, int]:
    """Return the exact latest eligible order, not a union of historical lines.

    A line absent from the latest order must be predicted as zero by the
    last-order baseline. The prior implementation accumulated keys across every
    order and was also sensitive to input ordering.
    """
    eligible = list(train_orders)
    if schedule_key:
        wanted = str(schedule_key).lower()
        eligible = [
            order
            for order in eligible
            if str(getattr(order, "schedule_key", "") or "").lower() == wanted
        ]
    if not eligible:
        return {}
    latest = max(eligible, key=lambda order: _order_delivery_dt(order) or datetime.min)
    return _order_to_line_map(latest)


def _order_prediction_cutoff(order) -> Optional[datetime]:
    """Return a conservative as-of boundary for a historical forecast fold.

    PostgreSQL history currently preserves ``order_date`` but not a populated
    immutable forecast-generation timestamp. Midnight on the order date is a
    deliberately conservative boundary: it excludes all same-day corrections
    and avoids using the held-out order's finalized feedback.
    """
    return _parse_date(str(getattr(order, "order_date", "") or ""))


def _select_as_of_train_orders(all_orders, test_order) -> Tuple[List, Optional[datetime]]:
    cutoff_dt = _order_prediction_cutoff(test_order)
    if cutoff_dt is None:
        return [], None

    test_id = str(getattr(test_order, "id", "") or "")
    selected = []
    for order in all_orders:
        if str(getattr(order, "id", "") or "") == test_id:
            continue
        available_dt = _order_prediction_cutoff(order)
        if available_dt is None or available_dt >= cutoff_dt:
            continue
        selected.append(order)
    return selected, cutoff_dt


def _latest_same_schedule_order(train_orders, schedule_key: str):
    wanted = str(schedule_key or "").lower()
    eligible = [
        order
        for order in train_orders
        if str(getattr(order, "schedule_key", "") or "").lower() == wanted
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda order: _order_delivery_dt(order) or datetime.min)


def _load_test_one_cycle_removed_keys(
    route_number: str,
    schedule_key: str,
    train_orders,
    cutoff_dt: datetime,
    target_delivery_dt: datetime,
) -> set[LineKey]:
    """Return latest removals from the immediately prior same-schedule order.

    This is an opt-in backtest candidate, not production forecast behavior.
    The query is intentionally bounded to one prior order and a past-only
    submission cutoff. Ranking first ensures a later correction that restores
    a line overrides an earlier removal for the same order/store/SAP.
    """
    prior_order = _latest_same_schedule_order(train_orders, schedule_key)
    prior_order_id = str(getattr(prior_order, "id", "") or "")
    if not prior_order_id:
        return set()

    rows = fetch_all(
        """
        WITH ranked AS (
            SELECT
                store_id,
                sap,
                was_removed,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id, store_id, sap
                    ORDER BY submitted_at DESC, correction_id DESC
                ) AS correction_rank
            FROM forecast_corrections
            WHERE route_number = %s
              AND schedule_key = %s
              AND order_id = %s
              AND is_holiday_week = FALSE
              AND submitted_at < %s
              AND delivery_date < %s
        )
        SELECT store_id, sap
        FROM ranked
        WHERE correction_rank = 1
          AND was_removed = TRUE
        """,
        [
            route_number,
            str(schedule_key).lower(),
            prior_order_id,
            cutoff_dt,
            target_delivery_dt.date(),
        ],
    )
    return {
        (str(row.get("store_id") or ""), str(row.get("sap") or ""))
        for row in rows
        if row.get("store_id") and row.get("sap")
    }


def _build_test_one_cycle_slow_cooldown_keys(
    train_orders,
    schedule_key: str,
    pred_line_map: Dict[LineKey, int],
    key_source_map: Dict[LineKey, str],
    key_meta_map: Dict[LineKey, Dict[str, float]],
    removed_keys: set[LineKey],
    strict_slow_only: bool = False,
) -> Tuple[set[LineKey], Dict[str, int]]:
    """Select slow predictions absent from the prior actual order or removed.

    ``strict_slow_only`` is the R2 lab refinement. R1 treats the full
    intermittent branch as slow even though that branch currently extends to
    70% order frequency. R2 limits cooldown eligibility to the engine's true
    slow-mover flag (25% frequency or less).
    """
    prior_order = _latest_same_schedule_order(train_orders, schedule_key)
    if prior_order is None:
        return set(), {
            "slow_positive_predictions": 0,
            "prior_actual_absence_keys": 0,
            "explicit_removal_keys": 0,
            "both_signal_keys": 0,
        }

    prior_actual_keys = {
        key for key, quantity in _order_to_line_map(prior_order).items() if int(quantity or 0) > 0
    }
    slow_positive_keys = {
        key
        for key, quantity in pred_line_map.items()
        if int(quantity or 0) > 0
        and (
            float((key_meta_map.get(key, {}) or {}).get("is_slow_mover", 0.0)) >= 1.0
            if strict_slow_only
            else (
                key_source_map.get(key) == "slow_intermittent"
                or float((key_meta_map.get(key, {}) or {}).get("is_slow_mover", 0.0)) >= 1.0
            )
        )
    }
    absent_keys = slow_positive_keys - prior_actual_keys
    explicit_keys = slow_positive_keys & removed_keys
    cooldown_keys = absent_keys | explicit_keys
    return cooldown_keys, {
        "slow_positive_predictions": len(slow_positive_keys),
        "prior_actual_absence_keys": len(absent_keys),
        "explicit_removal_keys": len(explicit_keys),
        "both_signal_keys": len(absent_keys & explicit_keys),
    }


def _apply_test_one_cycle_slow_cooldown(
    pred_line_map: Dict[LineKey, int],
    pred_band_map: Dict[LineKey, BandTuple],
    key_source_map: Dict[LineKey, str],
    removed_keys: set[LineKey],
    actual_line_map: Optional[Dict[LineKey, int]] = None,
) -> Dict[str, int]:
    """Zero selected slow predictions and return held-out diagnostics."""
    actual_line_map = actual_line_map or {}
    stats = {
        "eligible_cooldown_keys": len(removed_keys),
        "lines_zeroed": 0,
        "units_suppressed": 0,
        "actual_units_on_zeroed_lines": 0,
        "lines_helped": 0,
        "lines_harmed": 0,
        "lines_unchanged_error": 0,
    }
    for key in removed_keys:
        prior_prediction = int(pred_line_map.get(key, 0) or 0)
        if prior_prediction <= 0:
            continue
        actual = int(actual_line_map.get(key, 0) or 0)
        prior_error = abs(prior_prediction - actual)
        cooldown_error = abs(actual)
        pred_line_map[key] = 0
        pred_band_map[key] = (0.0, 0.0, 0.0, 1.0)
        key_source_map[key] = "test_slow_mover_cooldown"
        stats["lines_zeroed"] += 1
        stats["units_suppressed"] += prior_prediction
        stats["actual_units_on_zeroed_lines"] += actual
        if cooldown_error < prior_error:
            stats["lines_helped"] += 1
        elif cooldown_error > prior_error:
            stats["lines_harmed"] += 1
        else:
            stats["lines_unchanged_error"] += 1
    return stats


def _apply_test_transient_spike_quarantine(
    train_orders,
    *,
    history_window: int = 6,
    min_history: int = 4,
    median_multiple: float = 3.0,
    mad_multiple: float = 3.0,
    min_excess_units: float = 6.0,
):
    """Cap extreme training observations using only earlier local history.

    This is the P1 test-lab candidate. Finalized order quantities remain the
    scoring truth; only the copy supplied to feature construction is changed.
    History is isolated by schedule/store/SAP so a high-volume delivery cycle
    cannot redefine another cycle's normal level.
    """
    model_orders = copy.deepcopy(list(train_orders))
    history: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)
    stats = {
        "eligible_rows": 0,
        "rows_quarantined": 0,
        "raw_units_on_quarantined_rows": 0,
        "model_units_on_quarantined_rows": 0,
        "units_quarantined": 0,
        "promo_tagged_rows_quarantined": 0,
    }

    ordered = sorted(
        model_orders,
        key=lambda order: (
            _order_delivery_dt(order) or datetime.min,
            str(getattr(order, "id", "") or ""),
        ),
    )
    for order in ordered:
        schedule_key = str(getattr(order, "schedule_key", "") or "").lower()
        for store in getattr(order, "stores", []) or []:
            store_id = str(getattr(store, "store_id", "") or "")
            for item in getattr(store, "items", []) or []:
                sap = str(getattr(item, "sap", "") or "")
                if not schedule_key or not store_id or not sap:
                    continue
                key = (schedule_key, store_id, sap)
                raw_units = max(0.0, float(getattr(item, "quantity", 0.0) or 0.0))
                prior = history[key][-max(1, int(history_window)):]
                model_units = raw_units
                if len(prior) >= max(1, int(min_history)):
                    stats["eligible_rows"] += 1
                    prior_values = np.asarray(prior, dtype=float)
                    median = float(np.median(prior_values))
                    mad = float(np.median(np.abs(prior_values - median)))
                    robust_ceiling = max(
                        median * float(median_multiple),
                        median + float(mad_multiple) * mad,
                    )
                    if (
                        raw_units > robust_ceiling
                        and raw_units - median >= float(min_excess_units)
                    ):
                        model_units = robust_ceiling
                        item.quantity = model_units
                        stats["rows_quarantined"] += 1
                        stats["raw_units_on_quarantined_rows"] += int(round(raw_units))
                        stats["model_units_on_quarantined_rows"] += int(round(model_units))
                        stats["units_quarantined"] += int(round(raw_units - model_units))
                        if bool(getattr(item, "promo_active", False)):
                            stats["promo_tagged_rows_quarantined"] += 1

                if model_units > 0:
                    history[key].append(model_units)

    return model_orders, stats


def _apply_test_inferred_lift_regime(
    train_orders,
    target_schedule_key: str,
    *,
    history_window: int = 6,
    min_history: int = 4,
    trigger_median_multiple: float = 3.0,
    trigger_mad_multiple: float = 3.0,
    return_median_multiple: float = 1.5,
    return_mad_multiple: float = 2.0,
    min_excess_units: float = 6.0,
):
    """Separate inferred lift from normal demand using finalized user signals.

    An extreme same-schedule observation starts a lift regime. Elevated rows
    are replaced by the pre-lift median only in the model-training copy, while
    their raw values feed a bounded lift suggestion. A near-normal or absent
    value from the user ends the regime immediately. No future row is consulted.
    """
    model_orders = copy.deepcopy(list(train_orders))
    states: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    stats = {
        "eligible_rows": 0,
        "regimes_started": 0,
        "regimes_ended_by_normal": 0,
        "regimes_ended_by_absence": 0,
        "rows_quarantined": 0,
        "raw_units_on_quarantined_rows": 0,
        "model_units_on_quarantined_rows": 0,
        "units_quarantined": 0,
        "active_lift_keys": 0,
    }

    grouped_orders: Dict[str, List] = defaultdict(list)
    for order in model_orders:
        schedule_key = str(getattr(order, "schedule_key", "") or "").lower()
        if schedule_key:
            grouped_orders[schedule_key].append(order)

    for schedule_key, schedule_orders in grouped_orders.items():
        known_keys: set[Tuple[str, str]] = set()
        for order in sorted(
            schedule_orders,
            key=lambda value: (
                _order_delivery_dt(value) or datetime.min,
                str(getattr(value, "id", "") or ""),
            ),
        ):
            item_by_key = {}
            for store in getattr(order, "stores", []) or []:
                store_id = str(getattr(store, "store_id", "") or "")
                for item in getattr(store, "items", []) or []:
                    sap = str(getattr(item, "sap", "") or "")
                    if store_id and sap:
                        item_by_key[(store_id, sap)] = item

            for store_sap_key in known_keys - set(item_by_key):
                state_key = (schedule_key, *store_sap_key)
                state = states[state_key]
                if bool(state["active"]):
                    state["active"] = False
                    state["elevated"] = []
                    stats["regimes_ended_by_absence"] += 1

            for store_sap_key, item in item_by_key.items():
                known_keys.add(store_sap_key)
                state_key = (schedule_key, *store_sap_key)
                state = states.setdefault(
                    state_key,
                    {"normal": [], "active": False, "elevated": [], "lift_cap": 0.0},
                )
                raw_units = max(0.0, float(getattr(item, "quantity", 0.0) or 0.0))
                normal = list(state["normal"])[-max(1, int(history_window)):]
                if len(normal) < max(1, int(min_history)):
                    if raw_units > 0:
                        state["normal"].append(raw_units)
                    continue

                stats["eligible_rows"] += 1
                normal_values = np.asarray(normal, dtype=float)
                median = float(np.median(normal_values))
                mad = float(np.median(np.abs(normal_values - median)))
                trigger_ceiling = max(
                    median * float(trigger_median_multiple),
                    median + float(trigger_mad_multiple) * mad,
                )
                return_ceiling = max(
                    median * float(return_median_multiple),
                    median + float(return_mad_multiple) * mad,
                )

                if bool(state["active"]):
                    if raw_units <= return_ceiling:
                        state["active"] = False
                        state["elevated"] = []
                        stats["regimes_ended_by_normal"] += 1
                        if raw_units > 0:
                            state["normal"].append(raw_units)
                        continue
                elif not (
                    raw_units > trigger_ceiling
                    and raw_units - median >= float(min_excess_units)
                ):
                    if raw_units > 0:
                        state["normal"].append(raw_units)
                    continue
                else:
                    state["active"] = True
                    state["elevated"] = []
                    state["lift_cap"] = trigger_ceiling
                    stats["regimes_started"] += 1

                state["elevated"].append(raw_units)
                item.quantity = median
                stats["rows_quarantined"] += 1
                stats["raw_units_on_quarantined_rows"] += int(round(raw_units))
                stats["model_units_on_quarantined_rows"] += int(round(median))
                stats["units_quarantined"] += int(round(raw_units - median))

    wanted_schedule = str(target_schedule_key or "").lower()
    lift_map: Dict[LineKey, int] = {}
    for (schedule_key, store_id, sap), state in states.items():
        if schedule_key != wanted_schedule or not bool(state["active"]):
            continue
        elevated = list(state["elevated"])[-2:]
        if not elevated:
            continue
        lift_units = min(float(np.median(elevated)), float(state["lift_cap"]))
        if lift_units > 0:
            lift_map[(store_id, sap)] = max(1, int(round(lift_units)))
    stats["active_lift_keys"] = len(lift_map)
    return model_orders, lift_map, stats


def _apply_test_inferred_lift_overrides(
    pred_line_map: Dict[LineKey, int],
    pred_band_map: Dict[LineKey, BandTuple],
    key_source_map: Dict[LineKey, str],
    lift_map: Dict[LineKey, int],
    actual_line_map: Optional[Dict[LineKey, int]] = None,
) -> Dict[str, int]:
    """Apply inferred-lift suggestions and report held-out diagnostics."""
    actual_line_map = actual_line_map or {}
    stats = {
        "lines_overridden": 0,
        "units_delta": 0,
        "lines_helped": 0,
        "lines_harmed": 0,
        "lines_unchanged_error": 0,
    }
    for key, lift_units in lift_map.items():
        prior_prediction = int(pred_line_map.get(key, 0) or 0)
        next_prediction = max(0, int(lift_units or 0))
        if next_prediction == prior_prediction:
            continue
        actual = int(actual_line_map.get(key, 0) or 0)
        prior_error = abs(prior_prediction - actual)
        next_error = abs(next_prediction - actual)
        pred_line_map[key] = next_prediction
        pred_band_map[key] = (
            max(0.0, float(next_prediction) * 0.7),
            float(next_prediction),
            float(next_prediction) * 1.3,
            0.55,
        )
        key_source_map[key] = "test_inferred_transient_lift"
        stats["lines_overridden"] += 1
        stats["units_delta"] += next_prediction - prior_prediction
        if next_error < prior_error:
            stats["lines_helped"] += 1
        elif next_error > prior_error:
            stats["lines_harmed"] += 1
        else:
            stats["lines_unchanged_error"] += 1
    return stats


def _build_pred_line_map(preds_df: pd.DataFrame) -> Dict[LineKey, int]:
    out: Dict[LineKey, int] = {}
    if preds_df.empty:
        return out
    for _, row in preds_df.iterrows():
        sid = str(row.get("store_id") or "")
        sap = str(row.get("sap") or "")
        if not sid or not sap:
            continue
        qty = _to_int_units(row.get("pred_units"))
        if qty <= 0:
            continue
        out[(sid, sap)] = out.get((sid, sap), 0) + qty
    return out


def _build_key_source_map(preds_df: pd.DataFrame) -> Dict[LineKey, str]:
    out: Dict[LineKey, str] = {}
    if preds_df.empty:
        return out
    for _, row in preds_df.iterrows():
        sid = str(row.get("store_id") or "")
        sap = str(row.get("sap") or "")
        if not sid or not sap:
            continue
        source = str(
            row.get("source")
            or row.get("model_branch")
            or "unknown"
        ).strip() or "unknown"
        out[(sid, sap)] = source
    return out


def _build_key_meta_map(preds_df: pd.DataFrame) -> Dict[LineKey, Dict[str, float]]:
    out: Dict[LineKey, Dict[str, float]] = {}
    if preds_df.empty:
        return out
    for _, row in preds_df.iterrows():
        sid = str(row.get("store_id") or "")
        sap = str(row.get("sap") or "")
        if not sid or not sap:
            continue
        out[(sid, sap)] = {
            "is_slow_mover": float(row.get("is_slow_mover", 0.0) or 0.0),
            "days_since_last_order": float(row.get("days_since_last_order", 0.0) or 0.0),
            "corr_removal_rate": float(row.get("corr_removal_rate", 0.0) or 0.0),
            "corr_samples": float(row.get("corr_samples", 0.0) or 0.0),
        }
    return out


def _apply_live_slow_gate(preds_df: pd.DataFrame, case_pack_by_sap: Dict[str, int]) -> pd.DataFrame:
    """Mirror the production low-signal slow-mover gate before scoring."""
    if preds_df.empty:
        return preds_df
    gated = preds_df.copy()
    for idx, row in gated.iterrows():
        case_pack = case_pack_by_sap.get(str(row.get("sap") or ""))
        if _should_zero_low_signal_slow_line(row, case_pack):
            gated.at[idx, "pred_units"] = 0.0
            gated.at[idx, "p10_units"] = 0.0
            gated.at[idx, "p50_units"] = 0.0
            gated.at[idx, "p90_units"] = 0.0
    return gated


def _apply_band_scale_and_center(
    p10_units: float,
    p50_units: float,
    p90_units: float,
    scale_mult: float = 1.0,
    center_offset_units: float = 0.0,
) -> Tuple[float, float, float]:
    p10 = float(p10_units or 0.0)
    p50 = float(p50_units or 0.0)
    p90 = float(p90_units or 0.0)

    scale = max(0.1, float(scale_mult or 1.0))
    lo_span = max(0.0, p50 - p10)
    hi_span = max(0.0, p90 - p50)
    p10 = p50 - (lo_span * scale)
    p90 = p50 + (hi_span * scale)

    offset = float(center_offset_units or 0.0)
    if abs(offset) > 1e-9:
        p10 = max(0.0, p10 + offset)
        p50 = max(0.0, p50 + offset)
        p90 = max(p50, p90 + offset)

    if p10 > p90:
        p10, p90 = p90, p10
    return max(0.0, p10), max(0.0, p50), max(0.0, p90)


def _build_pred_band_map(
    preds_df: pd.DataFrame,
    center_offset_units: float = 0.0,
    source_band_adjustments: Optional[Dict[str, Tuple[float, float]]] = None,
) -> Dict[LineKey, BandTuple]:
    out: Dict[LineKey, BandTuple] = {}
    if preds_df.empty:
        return out
    for _, row in preds_df.iterrows():
        sid = str(row.get("store_id") or "")
        sap = str(row.get("sap") or "")
        if not sid or not sap:
            continue
        p50 = float(row.get("p50_units", row.get("pred_units", 0.0)) or 0.0)
        p10 = float(row.get("p10_units", p50) or p50)
        p90 = float(row.get("p90_units", p50) or p50)
        source = str(
            row.get("source")
            or row.get("model_branch")
            or "unknown"
        ).strip() or "unknown"
        src_scale, src_center = (1.0, 0.0)
        if source_band_adjustments:
            src_scale, src_center = source_band_adjustments.get(source, (1.0, 0.0))
        p10, p50, p90 = _apply_band_scale_and_center(
            p10_units=p10,
            p50_units=p50,
            p90_units=p90,
            scale_mult=src_scale,
            center_offset_units=(float(center_offset_units or 0.0) + float(src_center or 0.0)),
        )
        conf = float(row.get("confidence", 0.0) or 0.0)
        out[(sid, sap)] = (max(0.0, p10), max(0.0, p50), max(0.0, p90), conf)
    return out


def _compute_source_breakdown(
    route_number: str,
    schedule_key: str,
    target_dt: datetime,
    fold_index: int,
    train_orders_count: int,
    key_source_map: Dict[LineKey, str],
    pred_line_map: Dict[LineKey, int],
    pred_band_map: Dict[LineKey, BandTuple],
    actual_line_map: Dict[LineKey, int],
    cold_start_p90_by_sap: Optional[Dict[str, int]] = None,
) -> List[Dict[str, object]]:
    keys = sorted(set(actual_line_map) | set(pred_line_map))
    if not keys:
        return []

    grouped: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "line_count": 0.0,
            "sum_abs_error_units": 0.0,
            "sum_actual_units": 0.0,
            "sum_pred_units": 0.0,
            "sum_width_units": 0.0,
            "hits": 0.0,
            "under": 0.0,
            "over": 0.0,
        }
    )

    for key in keys:
        source = key_source_map.get(key, "missing_pred")
        actual = float(actual_line_map.get(key, 0) or 0.0)
        pred = float(pred_line_map.get(key, 0) or 0.0)
        if key in pred_band_map:
            p10, _p50, p90, _conf = pred_band_map[key]
        else:
            sap = str(key[1] or "")
            cold_p90 = float((cold_start_p90_by_sap or {}).get(sap, 0) or 0.0)
            p10, _p50, p90 = (0.0, 0.0, cold_p90) if cold_p90 > 0 else (pred, pred, pred)
        lo = min(float(p10), float(p90))
        hi = max(float(p10), float(p90))
        width = max(0.0, hi - lo)

        g = grouped[source]
        g["line_count"] += 1.0
        g["sum_abs_error_units"] += abs(pred - actual)
        g["sum_actual_units"] += abs(actual)
        g["sum_pred_units"] += abs(pred)
        g["sum_width_units"] += width
        g["hits"] += 1.0 if (actual >= lo and actual <= hi) else 0.0
        g["under"] += 1.0 if actual < lo else 0.0
        g["over"] += 1.0 if actual > hi else 0.0

    rows: List[Dict[str, object]] = []
    for source, g in sorted(grouped.items(), key=lambda kv: kv[0]):
        line_count = float(g["line_count"])
        if line_count <= 0:
            continue
        denom = float(g["sum_actual_units"])
        rows.append(
            {
                "route_number": route_number,
                "schedule_key": schedule_key,
                "target_delivery_date": target_dt.strftime("%Y-%m-%d"),
                "fold_index": int(fold_index),
                "train_orders": int(train_orders_count),
                "source": source,
                "line_count": int(line_count),
                "line_mae_units": float(g["sum_abs_error_units"] / line_count),
                "line_wape": float(g["sum_abs_error_units"] / denom) if denom > 0 else 0.0,
                "line_band_coverage_10_90": float(g["hits"] / line_count),
                "line_band_under_rate_10_90": float(g["under"] / line_count),
                "line_band_over_rate_10_90": float(g["over"] / line_count),
                "line_band_avg_width_units_10_90": float(g["sum_width_units"] / line_count),
            }
        )
    return rows


def _round_up_to_case(units: int, case_pack: Optional[int]) -> int:
    if units <= 0:
        return 0
    if not case_pack or case_pack <= 0:
        return units
    remainder = units % case_pack
    if remainder == 0:
        return units
    return units + (case_pack - remainder)


def _inject_training_schedule_features(df_orders: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Mirror schedule feature shape used by generate_forecast.

    For backtests we derive days-until-next-delivery from historical cadence.
    """
    if df_orders.empty:
        return df_orders, 4

    out = df_orders.copy()
    unique_dates = sorted(pd.to_datetime(out["delivery_date"].dropna().unique()).tolist())
    if len(unique_dates) < 2:
        default_gap = 4
        gap_by_date = {d.date(): default_gap for d in unique_dates}
    else:
        gaps: List[int] = []
        gap_by_date: Dict[datetime.date, int] = {}
        for idx, d in enumerate(unique_dates[:-1]):
            nxt = unique_dates[idx + 1]
            gap = max(1, int((nxt - d).days))
            gaps.append(gap)
            gap_by_date[d.date()] = gap
        default_gap = int(round(float(np.median(gaps)))) if gaps else 4
        gap_by_date[unique_dates[-1].date()] = default_gap

    def _gap_for_date(ts) -> int:
        try:
            return int(gap_by_date.get(pd.Timestamp(ts).date(), default_gap))
        except Exception:
            return default_gap

    out["days_until_next_delivery"] = out["delivery_date"].apply(_gap_for_date)
    out["days_until_first_weekend"] = out.apply(
        lambda r: _compute_schedule_features(
            pd.Timestamp(r["delivery_date"]).to_pydatetime(),
            int(r["days_until_next_delivery"] or default_gap),
        )["days_until_first_weekend"],
        axis=1,
    )
    out["covers_first_weekend"] = out.apply(
        lambda r: _compute_schedule_features(
            pd.Timestamp(r["delivery_date"]).to_pydatetime(),
            int(r["days_until_next_delivery"] or default_gap),
        )["covers_first_weekend"],
        axis=1,
    )
    out["covers_weekend"] = out.apply(
        lambda r: _compute_schedule_features(
            pd.Timestamp(r["delivery_date"]).to_pydatetime(),
            int(r["days_until_next_delivery"] or default_gap),
        )["covers_weekend"],
        axis=1,
    )
    return out, default_gap


def _load_temporal_corrections(
    route_number: str,
    schedule_key: str,
    cutoff_dt: datetime,
    *,
    held_out_order_id: str,
    target_delivery_dt: datetime,
) -> Optional[pd.DataFrame]:
    """Load correction aggregates only from history strictly before test cutoff."""
    half_life_days = float(os.environ.get("FORECAST_CORR_RECENCY_HALF_LIFE_DAYS", "42"))
    cutoff_date = cutoff_dt.date()
    rows = fetch_all(
        """
        WITH ranked AS (
            SELECT
                correction_id,
                order_id,
                store_id,
                sap,
                schedule_key,
                correction_delta,
                correction_ratio,
                was_removed,
                promo_active,
                predicted_units,
                final_units,
                delivery_date,
                submitted_at,
                ROW_NUMBER() OVER (
                    PARTITION BY route_number, schedule_key, order_id, store_id, sap
                    ORDER BY submitted_at DESC, correction_id DESC
                ) AS dedupe_rn
            FROM forecast_corrections
            WHERE route_number = %s
              AND schedule_key = %s
              AND is_holiday_week = FALSE
              AND submitted_at < %s
              AND delivery_date < %s
              AND order_id <> %s
        ),
        base AS (
            SELECT
                store_id,
                sap,
                schedule_key,
                correction_delta,
                correction_ratio,
                was_removed,
                promo_active,
                predicted_units,
                final_units,
                delivery_date,
                submitted_at,
                POWER(
                    0.5,
                    GREATEST(
                        0.0,
                        (%s::date - COALESCE(delivery_date::date, submitted_at::date, %s::date))::float
                    ) / %s
                ) AS recency_w
            FROM ranked
            WHERE dedupe_rn = 1
        ),
        recent_streak AS (
            SELECT
                store_id,
                sap,
                schedule_key,
                CASE
                    WHEN MIN(CASE WHEN was_removed THEN NULL ELSE rn END) IS NULL THEN MAX(rn)
                    ELSE GREATEST(MIN(CASE WHEN was_removed THEN NULL ELSE rn END) - 1, 0)
                END AS recent_removal_streak
            FROM (
                SELECT
                    store_id,
                    sap,
                    schedule_key,
                    was_removed,
                    ROW_NUMBER() OVER (
                        PARTITION BY store_id, sap, schedule_key
                        ORDER BY COALESCE(delivery_date::date, submitted_at::date, %s::date) DESC, submitted_at DESC
                    ) AS rn
                FROM base
            ) ordered
            GROUP BY store_id, sap, schedule_key
        ),
        agg AS (
            SELECT
                store_id,
                sap,
                schedule_key,
                COUNT(*) AS samples,
                SUM(correction_delta * recency_w) / NULLIF(SUM(recency_w), 0) AS avg_delta,
                SUM(correction_ratio * recency_w) / NULLIF(SUM(recency_w), 0) AS avg_ratio,
                STDDEV(correction_ratio) AS ratio_stddev,
                SUM((CASE WHEN was_removed THEN 1 ELSE 0 END) * recency_w) / NULLIF(SUM(recency_w), 0) AS removal_rate,
                SUM((CASE WHEN promo_active THEN 1 ELSE 0 END) * recency_w) / NULLIF(SUM(recency_w), 0) AS promo_rate,
                SUM((CASE WHEN predicted_units = 0 AND final_units > 0 THEN 1 ELSE 0 END) * recency_w)
                    / NULLIF(SUM(recency_w), 0) AS add_rate,
                SUM((CASE WHEN predicted_units = 0 AND final_units > 0 THEN final_units ELSE 0 END) * recency_w)
                    / NULLIF(
                        SUM(CASE WHEN predicted_units = 0 AND final_units > 0 THEN recency_w ELSE 0 END),
                        0
                    ) AS avg_add_units
            FROM base
            GROUP BY store_id, sap, schedule_key
            HAVING COUNT(*) >= 1
        )
        SELECT
            agg.*,
            COALESCE(rs.recent_removal_streak, 0) AS recent_removal_streak
        FROM agg
        LEFT JOIN recent_streak rs
            ON rs.store_id = agg.store_id
           AND rs.sap = agg.sap
           AND rs.schedule_key = agg.schedule_key
        """,
        [
            route_number,
            schedule_key,
            cutoff_dt,
            target_delivery_dt.date(),
            held_out_order_id,
            cutoff_date,
            cutoff_date,
            half_life_days,
            cutoff_date,
        ],
    )
    if not rows:
        return None
    return pd.DataFrame(rows)


def _aggregate_sap_totals(line_map: Dict[LineKey, int]) -> Dict[str, int]:
    out: Dict[str, int] = defaultdict(int)
    for (_, sap), qty in line_map.items():
        out[sap] += int(qty)
    return dict(out)


def _compute_cold_start_p90_by_sap(df_orders: pd.DataFrame, case_pack_by_sap: Dict[str, int]) -> Dict[str, int]:
    """Conservative fallback p90 for unseen store/sap lines with route SAP history."""
    out: Dict[str, int] = {}
    if df_orders.empty or "sap" not in df_orders.columns or "units" not in df_orders.columns:
        return out

    work = df_orders.copy()
    work["sap"] = work["sap"].astype(str)
    work["units"] = pd.to_numeric(work["units"], errors="coerce").fillna(0.0)
    work = work[work["units"] > 0]
    if work.empty:
        return out

    for sap, g in work.groupby("sap"):
        cp = int(case_pack_by_sap.get(str(sap), 1) or 1)
        q90 = float(g["units"].quantile(0.90))
        mean_u = float(g["units"].mean())
        std_u = float(g["units"].std(ddof=0) or 0.0)
        fallback = max(float(cp) * 2.0, q90, mean_u + std_u)
        out[str(sap)] = max(1, int(round(fallback)))
    return out


def _case_pack_lookup(products) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for p in products:
        sap = str(getattr(p, "sap", "") or "")
        if not sap:
            continue
        cp = getattr(p, "case_pack", None) or getattr(p, "tray", None)
        if cp is not None:
            try:
                out[sap] = int(cp)
            except Exception:
                pass
    return out


def _load_band_calibration(
    route_number: str,
    schedule_key: str,
    ignore_band_calibration: bool = False,
) -> Tuple[float, float]:
    if ignore_band_calibration:
        return 1.0, 0.0
    enabled = str(
        (os.environ.get("FORECAST_BAND_CALIBRATION_ENABLED", "1"))
    ).lower() in ("1", "true", "yes")
    if not enabled:
        return 1.0, 0.0

    interval_name = os.environ.get("FORECAST_BAND_INTERVAL_NAME", "p10_p90")
    min_scale = float(os.environ.get("FORECAST_BAND_SCALE_MIN", "0.5"))
    max_scale = float(os.environ.get("FORECAST_BAND_SCALE_MAX", "8.0"))
    max_center_abs = float(os.environ.get("FORECAST_BAND_CENTER_OFFSET_MAX_ABS", "64.0"))
    try:
        scale_row = fetch_one(
            """
            SELECT band_scale
            FROM forecast_band_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            LIMIT 1
            """,
            [route_number, schedule_key, interval_name],
        )
        center_row = fetch_one(
            """
            SELECT center_offset_units
            FROM forecast_band_center_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            LIMIT 1
            """,
            [route_number, schedule_key, interval_name],
        )
        scale = float((scale_row or {}).get("band_scale", 1.0) or 1.0)
        center = float((center_row or {}).get("center_offset_units", 0.0) or 0.0)
        scale = max(min_scale, min(max_scale, scale))
        center = max(-max_center_abs, min(max_center_abs, center))
        return scale, center
    except Exception:
        return 1.0, 0.0


def _load_source_band_calibration(
    route_number: str,
    schedule_key: str,
    ignore_band_calibration: bool = False,
) -> Dict[str, Tuple[float, float]]:
    if ignore_band_calibration:
        return {}
    enabled = str(
        (os.environ.get("FORECAST_BAND_CALIBRATION_ENABLED", "1"))
    ).lower() in ("1", "true", "yes")
    if not enabled:
        return {}

    interval_name = os.environ.get("FORECAST_BAND_INTERVAL_NAME", "p10_p90")
    min_scale = float(os.environ.get("FORECAST_BAND_SOURCE_SCALE_MIN", "0.5"))
    max_scale = float(os.environ.get("FORECAST_BAND_SOURCE_SCALE_MAX", "4.0"))
    max_center_abs = float(os.environ.get("FORECAST_BAND_CENTER_OFFSET_MAX_ABS", "64.0"))
    try:
        rows = fetch_all(
            """
            SELECT source, band_scale_mult, center_offset_units
            FROM forecast_band_source_calibration
            WHERE route_number = %s
              AND schedule_key = %s
              AND interval_name = %s
            """,
            [route_number, schedule_key, interval_name],
        )
        out: Dict[str, Tuple[float, float]] = {}
        for r in rows:
            source = str(r.get("source") or "").strip()
            if not source:
                continue
            scale = float(r.get("band_scale_mult", 1.0) or 1.0)
            center = float(r.get("center_offset_units", 0.0) or 0.0)
            scale = max(min_scale, min(max_scale, scale))
            center = max(-max_center_abs, min(max_center_abs, center))
            out[source] = (scale, center)
        return out
    except Exception:
        return {}


def _compute_fold_metrics(
    route_number: str,
    schedule_key: str,
    target_dt: datetime,
    fold_index: int,
    train_orders_count: int,
    pred_line_map: Dict[LineKey, int],
    pred_band_map: Dict[LineKey, BandTuple],
    key_source_map: Optional[Dict[LineKey, str]],
    key_meta_map: Optional[Dict[LineKey, Dict[str, float]]],
    naive_line_map: Dict[LineKey, int],
    actual_line_map: Dict[LineKey, int],
    case_pack_by_sap: Dict[str, int],
    cold_start_p90_by_sap: Optional[Dict[str, int]] = None,
) -> Dict[str, object]:
    keys = sorted(set(actual_line_map) | set(pred_line_map) | set(naive_line_map))
    if not keys:
        return {}

    y_true = np.array([actual_line_map.get(k, 0) for k in keys], dtype=float)
    y_pred = np.array([pred_line_map.get(k, 0) for k in keys], dtype=float)
    y_naive = np.array([naive_line_map.get(k, 0) for k in keys], dtype=float)

    band_hits: List[float] = []
    band_under: List[float] = []
    band_over: List[float] = []
    band_widths: List[float] = []
    for k in keys:
        actual = float(actual_line_map.get(k, 0))
        fallback = float(pred_line_map.get(k, 0))
        if k in pred_band_map:
            p10, _p50, p90, _conf = pred_band_map[k]
        else:
            sap = str(k[1] or "")
            cold_p90 = float((cold_start_p90_by_sap or {}).get(sap, 0) or 0.0)
            p10, _p50, p90 = (0.0, 0.0, cold_p90) if cold_p90 > 0 else (fallback, fallback, fallback)
        lo = min(float(p10), float(p90))
        hi = max(float(p10), float(p90))
        band_hits.append(1.0 if (actual >= lo and actual <= hi) else 0.0)
        band_under.append(1.0 if actual < lo else 0.0)
        band_over.append(1.0 if actual > hi else 0.0)
        band_widths.append(max(0.0, hi - lo))

    model_metrics = validate_predictions(
        y_true=y_true,
        y_pred=y_pred,
        y_naive=y_naive,
        mae_threshold=float("inf"),
        rmse_threshold=float("inf"),
    )

    line_exact_model = float(np.mean(y_pred == y_true))
    line_exact_naive = float(np.mean(y_naive == y_true))
    line_edit_rate_model = float(np.mean(y_pred != y_true))
    line_edit_rate_naive = float(np.mean(y_naive != y_true))

    edited_model = np.abs(y_pred - y_true)
    edited_naive = np.abs(y_naive - y_true)
    line_edit_mag_model = float(np.mean(edited_model[edited_model > 0])) if np.any(edited_model > 0) else 0.0
    line_edit_mag_naive = float(np.mean(edited_naive[edited_naive > 0])) if np.any(edited_naive > 0) else 0.0

    actual_sap = _aggregate_sap_totals(actual_line_map)
    pred_sap_raw = _aggregate_sap_totals(pred_line_map)
    naive_sap_raw = _aggregate_sap_totals(naive_line_map)

    all_saps = sorted(set(actual_sap) | set(pred_sap_raw) | set(naive_sap_raw))
    sap_true: List[int] = []
    sap_pred: List[int] = []
    sap_naive: List[int] = []
    case_match_model = 0
    case_match_naive = 0

    for sap in all_saps:
        cp = case_pack_by_sap.get(sap)
        actual_units = int(actual_sap.get(sap, 0))
        pred_units = _round_up_to_case(int(pred_sap_raw.get(sap, 0)), cp)
        naive_units = _round_up_to_case(int(naive_sap_raw.get(sap, 0)), cp)
        sap_true.append(actual_units)
        sap_pred.append(pred_units)
        sap_naive.append(naive_units)
        if pred_units == actual_units:
            case_match_model += 1
        if naive_units == actual_units:
            case_match_naive += 1

    sap_true_arr = np.array(sap_true, dtype=float)
    sap_pred_arr = np.array(sap_pred, dtype=float)
    sap_naive_arr = np.array(sap_naive, dtype=float)

    sap_mae_model = float(np.mean(np.abs(sap_pred_arr - sap_true_arr))) if len(sap_true_arr) else 0.0
    sap_mae_naive = float(np.mean(np.abs(sap_naive_arr - sap_true_arr))) if len(sap_true_arr) else 0.0
    sap_wape_model = _wape(sap_true_arr, sap_pred_arr)
    sap_wape_naive = _wape(sap_true_arr, sap_naive_arr)

    order_actual_total = int(np.sum(sap_true_arr))
    order_pred_total = int(np.sum(sap_pred_arr))
    order_naive_total = int(np.sum(sap_naive_arr))
    order_abs_err_model = abs(order_pred_total - order_actual_total)
    order_abs_err_naive = abs(order_naive_total - order_actual_total)
    order_wape_model = (order_abs_err_model / order_actual_total) if order_actual_total > 0 else float("nan")
    order_wape_naive = (order_abs_err_naive / order_actual_total) if order_actual_total > 0 else float("nan")

    key_source_map = key_source_map or {}
    key_meta_map = key_meta_map or {}

    def _segment_stats(selector) -> Dict[str, float]:
        seg_keys = [k for k in keys if selector(k)]
        if not seg_keys:
            return {
                "line_count": 0.0,
                "wape": 0.0,
                "over_rate": 0.0,
            }
        a = np.array([actual_line_map.get(k, 0) for k in seg_keys], dtype=float)
        p = np.array([pred_line_map.get(k, 0) for k in seg_keys], dtype=float)
        denom = float(np.sum(np.abs(a)))
        over = float(np.mean(p > a)) if len(seg_keys) else 0.0
        return {
            "line_count": float(len(seg_keys)),
            "wape": float(np.sum(np.abs(p - a)) / denom) if denom > 0 else 0.0,
            "over_rate": over,
        }

    slow_stats = _segment_stats(
        lambda k: key_source_map.get(k) == "slow_intermittent"
        or float((key_meta_map.get(k, {}) or {}).get("is_slow_mover", 0.0)) >= 1.0
    )
    stale14_stats = _segment_stats(
        lambda k: float((key_meta_map.get(k, {}) or {}).get("days_since_last_order", 0.0)) >= 14.0
    )
    stale21_stats = _segment_stats(
        lambda k: float((key_meta_map.get(k, {}) or {}).get("days_since_last_order", 0.0)) >= 21.0
    )
    high_removal_stats = _segment_stats(
        lambda k: float((key_meta_map.get(k, {}) or {}).get("corr_removal_rate", 0.0)) >= 0.50
    )

    return {
        "route_number": route_number,
        "schedule_key": schedule_key,
        "target_delivery_date": target_dt.strftime("%Y-%m-%d"),
        "fold_index": int(fold_index),
        "train_orders": int(train_orders_count),
        "line_items_eval_count": int(len(keys)),
        "saps_eval_count": int(len(all_saps)),
        "line_mae_model": float(model_metrics["mae"]),
        "line_mae_naive": float(model_metrics["naive_mae"]),
        "line_rmse_model": float(model_metrics["rmse"]),
        "line_rmse_naive": float(model_metrics["naive_rmse"]),
        "line_wape_model": _wape(y_true, y_pred),
        "line_wape_naive": _wape(y_true, y_naive),
        "line_abs_error_units_model": float(np.sum(np.abs(y_pred - y_true))),
        "line_abs_error_units_naive": float(np.sum(np.abs(y_naive - y_true))),
        "line_actual_units": float(np.sum(np.abs(y_true))),
        "line_exact_match_rate_model": line_exact_model,
        "line_exact_match_rate_naive": line_exact_naive,
        "line_edit_rate_proxy_model": line_edit_rate_model,
        "line_edit_rate_proxy_naive": line_edit_rate_naive,
        "line_edit_magnitude_proxy_model": line_edit_mag_model,
        "line_edit_magnitude_proxy_naive": line_edit_mag_naive,
        "line_band_coverage_10_90": float(np.mean(band_hits)) if band_hits else 0.0,
        "line_band_under_rate_10_90": float(np.mean(band_under)) if band_under else 0.0,
        "line_band_over_rate_10_90": float(np.mean(band_over)) if band_over else 0.0,
        "line_band_avg_width_units_10_90": float(np.mean(band_widths)) if band_widths else 0.0,
        "line_band_median_width_units_10_90": float(np.median(band_widths)) if band_widths else 0.0,
        "order_zero_touch_model": 1 if np.all(y_pred == y_true) else 0,
        "order_zero_touch_naive": 1 if np.all(y_naive == y_true) else 0,
        "sap_case_match_rate_model": (case_match_model / len(all_saps)) if all_saps else 0.0,
        "sap_case_match_rate_naive": (case_match_naive / len(all_saps)) if all_saps else 0.0,
        "sap_mae_model": sap_mae_model,
        "sap_mae_naive": sap_mae_naive,
        "sap_wape_model": sap_wape_model,
        "sap_wape_naive": sap_wape_naive,
        "sap_abs_error_units_model": float(np.sum(np.abs(sap_pred_arr - sap_true_arr))),
        "sap_abs_error_units_naive": float(np.sum(np.abs(sap_naive_arr - sap_true_arr))),
        "sap_actual_units": float(np.sum(np.abs(sap_true_arr))),
        "order_total_units_actual": order_actual_total,
        "order_total_units_model": order_pred_total,
        "order_total_units_naive": order_naive_total,
        "order_total_signed_bias_model": float(order_pred_total - order_actual_total),
        "order_total_signed_bias_naive": float(order_naive_total - order_actual_total),
        "order_total_abs_error_model": float(order_abs_err_model),
        "order_total_abs_error_naive": float(order_abs_err_naive),
        "order_total_wape_model": float(order_wape_model),
        "order_total_wape_naive": float(order_wape_naive),
        "segment_slow_line_count": int(slow_stats["line_count"]),
        "segment_slow_line_wape_model": float(slow_stats["wape"]),
        "segment_slow_over_rate_model": float(slow_stats["over_rate"]),
        "segment_stale14_line_count": int(stale14_stats["line_count"]),
        "segment_stale14_line_wape_model": float(stale14_stats["wape"]),
        "segment_stale14_over_rate_model": float(stale14_stats["over_rate"]),
        "segment_stale21_line_count": int(stale21_stats["line_count"]),
        "segment_stale21_line_wape_model": float(stale21_stats["wape"]),
        "segment_stale21_over_rate_model": float(stale21_stats["over_rate"]),
        "segment_high_removal_line_count": int(high_removal_stats["line_count"]),
        "segment_high_removal_line_wape_model": float(high_removal_stats["wape"]),
        "segment_high_removal_over_rate_model": float(high_removal_stats["over_rate"]),
    }


def _observed_correction_proxies(route_number: str, since_days: int) -> Dict[str, Dict[str, float]]:
    try:
        corr_rows = fetch_all(
            """
            SELECT
                schedule_key,
                COUNT(*)::FLOAT AS correction_rows,
                COUNT(DISTINCT order_id)::FLOAT AS orders_with_corrections,
                AVG(ABS(correction_delta))::FLOAT AS avg_abs_correction_delta
            FROM forecast_corrections
            WHERE route_number = %s
              AND submitted_at >= (NOW() - (%s || ' days')::interval)
            GROUP BY schedule_key
            """,
            [route_number, since_days],
        )
        line_rows = fetch_all(
            """
            SELECT
                o.schedule_key,
                COUNT(li.line_item_id)::FLOAT AS total_lines,
                COUNT(DISTINCT o.order_id)::FLOAT AS total_orders
            FROM orders_historical o
            LEFT JOIN order_line_items li ON li.order_id = o.order_id
            WHERE o.route_number = %s
              AND o.delivery_date >= (CURRENT_DATE - (%s || ' days')::interval)
            GROUP BY o.schedule_key
            """,
            [route_number, since_days],
        )
    except Exception as e:
        print(f"[backtest] Warning: could not load observed correction proxies for route {route_number}: {e}")
        return {}
    corr_by_sched = {str(r["schedule_key"]): r for r in corr_rows}
    line_by_sched = {str(r["schedule_key"]): r for r in line_rows}
    out: Dict[str, Dict[str, float]] = {}

    for sched in sorted(set(corr_by_sched) | set(line_by_sched)):
        c = corr_by_sched.get(sched, {})
        l = line_by_sched.get(sched, {})
        total_lines = float(l.get("total_lines") or 0.0)
        total_orders = float(l.get("total_orders") or 0.0)
        corr_rows_count = float(c.get("correction_rows") or 0.0)
        corr_orders = float(c.get("orders_with_corrections") or 0.0)
        out[sched] = {
            "observed_correction_line_rate_proxy": (corr_rows_count / total_lines) if total_lines > 0 else 0.0,
            "observed_corrected_order_rate_proxy": (corr_orders / total_orders) if total_orders > 0 else 0.0,
            "observed_avg_abs_correction_delta": float(c.get("avg_abs_correction_delta") or 0.0),
        }
    return out


def _summarize_scorecard(folds_df: pd.DataFrame, since_days: int) -> pd.DataFrame:
    if folds_df.empty:
        return folds_df

    rows: List[Dict[str, object]] = []
    for (route, schedule), g in folds_df.groupby(["route_number", "schedule_key"]):
        obs = _observed_correction_proxies(str(route), since_days).get(str(schedule), {})
        line_mae_model = float(g["line_mae_model"].mean())
        line_mae_naive = float(g["line_mae_naive"].mean())
        sap_wape_model = float(g["sap_wape_model"].mean())
        sap_wape_naive = float(g["sap_wape_naive"].mean())
        order_wape_model = float(g["order_total_wape_model"].mean())
        order_wape_naive = float(g["order_total_wape_naive"].mean())
        line_actual = float(g["line_actual_units"].sum())
        sap_actual = float(g["sap_actual_units"].sum())
        order_actual = float(g["order_total_units_actual"].sum())

        rows.append(
            {
                "route_number": route,
                "schedule_key": schedule,
                "fold_count": int(len(g)),
                "mean_line_mae_model": line_mae_model,
                "mean_line_mae_naive": line_mae_naive,
                "line_mae_improvement_pct": ((line_mae_naive - line_mae_model) / line_mae_naive * 100.0)
                if line_mae_naive > 0
                else 0.0,
                "mean_line_wape_model": float(g["line_wape_model"].mean()),
                "mean_line_wape_naive": float(g["line_wape_naive"].mean()),
                "micro_line_wape_model": (
                    float(g["line_abs_error_units_model"].sum()) / line_actual
                    if line_actual > 0 else float("nan")
                ),
                "micro_line_wape_naive": (
                    float(g["line_abs_error_units_naive"].sum()) / line_actual
                    if line_actual > 0 else float("nan")
                ),
                "mean_sap_case_match_rate_model": float(g["sap_case_match_rate_model"].mean()),
                "mean_sap_case_match_rate_naive": float(g["sap_case_match_rate_naive"].mean()),
                "mean_sap_wape_model": sap_wape_model,
                "mean_sap_wape_naive": sap_wape_naive,
                "micro_sap_wape_model": (
                    float(g["sap_abs_error_units_model"].sum()) / sap_actual
                    if sap_actual > 0 else float("nan")
                ),
                "micro_sap_wape_naive": (
                    float(g["sap_abs_error_units_naive"].sum()) / sap_actual
                    if sap_actual > 0 else float("nan")
                ),
                "sap_wape_improvement_pct": ((sap_wape_naive - sap_wape_model) / sap_wape_naive * 100.0)
                if sap_wape_naive > 0
                else 0.0,
                "mean_order_total_wape_model": order_wape_model,
                "mean_order_total_wape_naive": order_wape_naive,
                "micro_order_total_wape_model": (
                    float(g["order_total_abs_error_model"].sum()) / order_actual
                    if order_actual > 0 else float("nan")
                ),
                "micro_order_total_wape_naive": (
                    float(g["order_total_abs_error_naive"].sum()) / order_actual
                    if order_actual > 0 else float("nan")
                ),
                "aggregate_order_signed_bias_model": float(g["order_total_signed_bias_model"].sum()),
                "aggregate_order_signed_bias_naive": float(g["order_total_signed_bias_naive"].sum()),
                "order_wape_improvement_pct": ((order_wape_naive - order_wape_model) / order_wape_naive * 100.0)
                if order_wape_naive > 0
                else 0.0,
                "mean_line_edit_rate_proxy_model": float(g["line_edit_rate_proxy_model"].mean()),
                "mean_line_edit_rate_proxy_naive": float(g["line_edit_rate_proxy_naive"].mean()),
                "mean_line_edit_magnitude_proxy_model": float(g["line_edit_magnitude_proxy_model"].mean()),
                "mean_line_edit_magnitude_proxy_naive": float(g["line_edit_magnitude_proxy_naive"].mean()),
                "mean_line_band_coverage_10_90": float(g["line_band_coverage_10_90"].mean()),
                "mean_line_band_under_rate_10_90": float(g["line_band_under_rate_10_90"].mean()),
                "mean_line_band_over_rate_10_90": float(g["line_band_over_rate_10_90"].mean()),
                "mean_line_band_avg_width_units_10_90": float(g["line_band_avg_width_units_10_90"].mean()),
                "mean_line_band_median_width_units_10_90": float(g["line_band_median_width_units_10_90"].mean()),
                "order_zero_touch_rate_model": float(g["order_zero_touch_model"].mean()),
                "order_zero_touch_rate_naive": float(g["order_zero_touch_naive"].mean()),
                "mean_segment_slow_line_wape_model": float(g["segment_slow_line_wape_model"].mean()),
                "mean_segment_slow_over_rate_model": float(g["segment_slow_over_rate_model"].mean()),
                "mean_segment_stale14_line_wape_model": float(g["segment_stale14_line_wape_model"].mean()),
                "mean_segment_stale14_over_rate_model": float(g["segment_stale14_over_rate_model"].mean()),
                "mean_segment_stale21_line_wape_model": float(g["segment_stale21_line_wape_model"].mean()),
                "mean_segment_stale21_over_rate_model": float(g["segment_stale21_over_rate_model"].mean()),
                "mean_segment_high_removal_line_wape_model": float(g["segment_high_removal_line_wape_model"].mean()),
                "mean_segment_high_removal_over_rate_model": float(g["segment_high_removal_over_rate_model"].mean()),
                "observed_correction_line_rate_proxy": float(obs.get("observed_correction_line_rate_proxy", 0.0)),
                "observed_corrected_order_rate_proxy": float(obs.get("observed_corrected_order_rate_proxy", 0.0)),
                "observed_avg_abs_correction_delta": float(obs.get("observed_avg_abs_correction_delta", 0.0)),
            }
        )
    return pd.DataFrame(rows).sort_values(["route_number", "schedule_key"])


def _distinct_routes_from_history(since_days: int, as_of_date: date) -> List[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT route_number
        FROM orders_historical
        WHERE delivery_date >= (%s::date - (%s || ' days')::interval)
        ORDER BY route_number
        """,
        [as_of_date, since_days],
    )
    return [str(r["route_number"]) for r in rows if r.get("route_number")]


def _resolve_store_context_for_fold(train_orders, allow_store_context: bool) -> bool:
    if not allow_store_context:
        return False
    min_total = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_TOTAL_ORDERS", "24"))
    min_per_schedule = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_PER_SCHEDULE", "6"))
    min_schedules = int(os.environ.get("FORECAST_STORE_CONTEXT_MIN_SCHEDULES", "2"))
    if len(train_orders) < min_total:
        return False
    schedule_counts: Dict[str, int] = {}
    for o in train_orders:
        sk = str(getattr(o, "schedule_key", "") or "").lower()
        if not sk:
            continue
        schedule_counts[sk] = schedule_counts.get(sk, 0) + 1
    schedules_meeting_min = sum(1 for c in schedule_counts.values() if c >= min_per_schedule)
    return schedules_meeting_min >= min_schedules


def _load_order_cycles_for_route(route_number: str) -> List[dict]:
    """Load order cycle configuration from PostgreSQL user_schedules."""
    try:
        from schedule_cycle import normalize_order_cycle
    except ImportError:
        from .schedule_cycle import normalize_order_cycle

    try:
        rows = fetch_all(
            """
            SELECT
                order_day,
                delivery_day,
                load_day,
                load_offset_days,
                delivery_offset_days,
                schedule_version,
                needs_schedule_review
            FROM user_schedules
            WHERE route_number = %s
              AND is_active = TRUE
            ORDER BY order_day, delivery_day, load_day
            """,
            [route_number],
        )
        out: List[dict] = []
        for r in rows:
            od = r.get("order_day")
            dd = r.get("delivery_day")
            ld = r.get("load_day")
            if od is None or dd is None or ld is None:
                continue
            out.append(normalize_order_cycle({
                "orderDay": int(od),
                "deliveryDay": int(dd),
                "loadDay": int(ld),
                "loadOffsetDays": r.get("load_offset_days"),
                "deliveryOffsetDays": r.get("delivery_offset_days"),
                "scheduleVersion": r.get("schedule_version"),
                "needsScheduleReview": r.get("needs_schedule_review"),
            }))
        return out
    except Exception:
        return []


def _temporal_corrected_orders_count(
    route_number: str,
    schedule_key: str,
    cutoff_dt: datetime,
    since_days: int,
    *,
    held_out_order_id: str,
    target_delivery_dt: datetime,
) -> int:
    """Count distinct corrected orders up to a fold cutoff datetime."""
    try:
        row = fetch_one(
            """
            SELECT COUNT(DISTINCT order_id) AS corrected_orders
            FROM forecast_corrections
            WHERE route_number = %s
              AND schedule_key = %s
              AND is_holiday_week = FALSE
              AND submitted_at < %s
              AND delivery_date < %s
              AND order_id <> %s
              AND delivery_date >= (%s::date - (%s || ' days')::interval)
            """,
            [
                route_number,
                str(schedule_key).lower(),
                cutoff_dt,
                target_delivery_dt.date(),
                held_out_order_id,
                cutoff_dt.date(),
                int(since_days),
            ],
        )
        return int((row or {}).get("corrected_orders") or 0)
    except Exception:
        return 0


def _resolve_mode_for_fold(
    *,
    route_number: str,
    schedule_key: str,
    train_orders_all,
    order_cycles: List[dict],
    cutoff_dt: datetime,
    since_days: int,
    allow_store_context: bool,
    held_out_order_id: str,
    target_delivery_dt: datetime,
    continuous_actual_learning: bool = False,
) -> Tuple[str, str, str, int, int, bool]:
    """Resolve mode per fold to mirror production progression logic.

    Returns:
        (mode, training_scope, reason, schedule_order_count,
         corrected_order_count, correction_gate_bypassed)

    ``continuous_actual_learning`` is an opt-in test-lab candidate. It removes
    only the correction-count requirement: finalized same-schedule orders
    remain the supervised signal and the schedule-history/safety gates remain.
    """
    schedule_orders = [
        o for o in train_orders_all if str(getattr(o, "schedule_key", "") or "").lower() == schedule_key
    ]
    schedule_order_count = len(schedule_orders)
    corrected_order_count = _temporal_corrected_orders_count(
        route_number=route_number,
        schedule_key=schedule_key,
        cutoff_dt=cutoff_dt,
        since_days=since_days,
        held_out_order_id=held_out_order_id,
        target_delivery_dt=target_delivery_dt,
    )

    min_schedule_orders = int(
        os.environ.get("FORECAST_MIN_SCHEDULE_ORDERS_FOR_ML", str(MIN_ORDERS_FOR_TRAINING))
    )
    min_corrected_orders = int(
        os.environ.get("FORECAST_MIN_CORRECTED_ORDERS_FOR_ML", str(MIN_CORRECTED_ORDERS_FOR_TRAINING))
    )
    strict_schedule_validation = os.environ.get(
        "FORECAST_STRICT_SCHEDULE_VALIDATION", "1"
    ).lower() in ("1", "true", "yes")
    allow_store_on_ambiguous_schedule = os.environ.get(
        "FORECAST_ALLOW_STORE_CONTEXT_ON_AMBIGUOUS_SCHEDULE", "1"
    ).lower() in ("1", "true", "yes")

    correction_gate_bypassed = bool(
        continuous_actual_learning
        and schedule_order_count >= min_schedule_orders
        and corrected_order_count < min_corrected_orders
    )

    def _reason(base: str) -> str:
        if not continuous_actual_learning:
            return base
        return (
            f"{base};learning_target=finalized_order_quantities,"
            f"schedule_orders={schedule_order_count},"
            f"corrected_orders={corrected_order_count},"
            f"correction_gate_bypassed={str(correction_gate_bypassed).lower()}"
        )

    if (
        schedule_order_count < min_schedule_orders
        or (
            not continuous_actual_learning
            and corrected_order_count < min_corrected_orders
        )
    ):
        reason_parts = []
        if schedule_order_count < min_schedule_orders:
            reason_parts.append(
                f"schedule_orders={schedule_order_count}<{min_schedule_orders}"
            )
        if (
            not continuous_actual_learning
            and corrected_order_count < min_corrected_orders
        ):
            reason_parts.append(
                f"corrected_orders={corrected_order_count}<{min_corrected_orders}"
            )
        return (
            "copy_last_order",
            "schedule_only",
            _reason("cold_start:" + ",".join(reason_parts)),
            schedule_order_count,
            corrected_order_count,
            correction_gate_bypassed,
        )

    schedule_shape = _summarize_schedule_shape(order_cycles, schedule_key)
    if strict_schedule_validation and not bool(schedule_shape.get("is_valid")):
        return (
            "schedule_aware",
            "schedule_only",
            _reason(
                f"invalid_schedule_config:invalid_cycles={schedule_shape.get('invalid_cycles', 0)}"
            ),
            schedule_order_count,
            corrected_order_count,
            correction_gate_bypassed,
        )
    if (
        not allow_store_on_ambiguous_schedule
        and bool(schedule_shape.get("same_order_to_multi_delivery"))
    ):
        return (
            "schedule_aware",
            "schedule_only",
            _reason("ambiguous_schedule_mapping:same_order_to_multi_delivery"),
            schedule_order_count,
            corrected_order_count,
            correction_gate_bypassed,
        )

    if allow_store_context and _resolve_store_context_for_fold(train_orders_all, allow_store_context=True):
        return (
            "store_centric",
            "store_all_cycles",
            _reason("adaptive_depth_ok"),
            schedule_order_count,
            corrected_order_count,
            correction_gate_bypassed,
        )
    return (
        "schedule_aware",
        "schedule_only",
        _reason("adaptive_depth_insufficient"),
        schedule_order_count,
        corrected_order_count,
        correction_gate_bypassed,
    )


def run_backtest(
    routes: Iterable[str],
    since_days: int,
    min_train_orders: int,
    max_folds: int,
    schedule_keys: Optional[set[str]],
    temporal_corrections: bool,
    ignore_band_calibration: bool = False,
    store_centric_context: bool = True,
    as_of_date: Optional[date] = None,
    test_one_cycle_slow_mover_cooldown: bool = False,
    test_strict_one_cycle_slow_mover_cooldown: bool = False,
    test_continuous_actual_learning: bool = False,
    test_transient_spike_quarantine: bool = False,
    test_inferred_transient_lift: bool = False,
    test_hierarchical_hurdle_v1: bool = False,
    test_store_sap_reconciled_v1: bool = False,
    test_store_first_hierarchy_v1: bool = False,
    test_store_first_case_hierarchy_v1: bool = False,
    test_settlement_store_share_v1: bool = False,
    settlement_truth_csv: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: List[Dict[str, object]] = []
    source_rows: List[Dict[str, object]] = []
    settlement_daily = pd.DataFrame()
    if test_settlement_store_share_v1:
        if not settlement_truth_csv:
            raise ValueError("settlement_truth_csv is required for the S1 settlement experiment")
        settlement_daily = pd.read_csv(
            settlement_truth_csv,
            dtype={"route_number": str, "store_number": str, "sap": str},
        )
        required = {
            "route_number",
            "store_number",
            "sap",
            "settlement_week_ending",
            "simulated_available_at",
            "sale_units",
        }
        missing = required - set(settlement_daily.columns)
        if missing:
            raise ValueError(f"settlement truth CSV missing columns: {sorted(missing)}")
        settlement_daily["store_number"] = settlement_daily["store_number"].str.zfill(4)

    for route_number in routes:
        print(f"[backtest] Route {route_number}: loading catalog + orders...")
        try:
            products = load_catalog_from_postgres(route_number)
            case_pack_by_sap = _case_pack_lookup(products)
            orders = load_orders_from_postgres(
                route_number=route_number,
                since_days=since_days,
                schedule_keys=schedule_keys if schedule_keys else None,
                status="finalized",
                as_of_date=as_of_date,
            )
        except Exception as e:
            print(f"[backtest] Route {route_number}: load failure ({e})")
            continue
        if not orders:
            print(f"[backtest] Route {route_number}: no orders in lookback window.")
            continue
        order_cycles = _load_order_cycles_for_route(route_number)

        grouped: Dict[str, List] = defaultdict(list)
        for o in orders:
            sk = str(o.schedule_key or "").lower()
            if not sk:
                continue
            grouped[sk].append(o)

        for schedule_key, sched_orders in grouped.items():
            if schedule_keys and schedule_key not in schedule_keys:
                continue
            band_scale, band_center_offset = _load_band_calibration(
                route_number=route_number,
                schedule_key=schedule_key,
                ignore_band_calibration=ignore_band_calibration,
            )
            source_band_adjustments = _load_source_band_calibration(
                route_number=route_number,
                schedule_key=schedule_key,
                ignore_band_calibration=ignore_band_calibration,
            )
            sched_orders = sorted(
                sched_orders,
                key=lambda o: _order_delivery_dt(o) or datetime.min,
            )
            if len(sched_orders) <= min_train_orders:
                print(
                    f"[backtest] Route {route_number} schedule {schedule_key}: "
                    f"insufficient orders ({len(sched_orders)} <= {min_train_orders})."
                )
                continue

            fold_counter = 0
            fold_candidates = list(range(min_train_orders, len(sched_orders)))
            if max_folds > 0:
                # A bounded run should represent current behavior, not silently
                # keep only the oldest eligible folds.
                fold_candidates = fold_candidates[-max_folds:]
            for i in fold_candidates:

                test_order = sched_orders[i]
                target_dt = _order_delivery_dt(test_order)
                if not target_dt:
                    continue

                candidate_train_orders, forecast_as_of = _select_as_of_train_orders(orders, test_order)
                if forecast_as_of is None:
                    print(
                        f"[backtest] Route {route_number} schedule {schedule_key}: "
                        f"skipping {getattr(test_order, 'id', '?')} (missing order_date cutoff)."
                    )
                    continue
                (
                    mode,
                    training_scope,
                    mode_reason,
                    schedule_actual_order_count,
                    corrected_order_count,
                    correction_gate_bypassed,
                ) = _resolve_mode_for_fold(
                    route_number=route_number,
                    schedule_key=schedule_key,
                    train_orders_all=candidate_train_orders,
                    order_cycles=order_cycles,
                    cutoff_dt=forecast_as_of,
                    since_days=since_days,
                    allow_store_context=bool(store_centric_context),
                    held_out_order_id=str(getattr(test_order, "id", "") or ""),
                    target_delivery_dt=target_dt,
                    continuous_actual_learning=bool(test_continuous_actual_learning),
                )
                if training_scope == "schedule_only":
                    train_orders = [
                        o for o in candidate_train_orders
                        if str(getattr(o, "schedule_key", "") or "").lower() == schedule_key
                    ]
                else:
                    train_orders = candidate_train_orders
                if not train_orders:
                    continue

                df_orders = _orders_to_dataframe(train_orders)
                if df_orders.empty:
                    continue
                cold_start_p90_by_sap = _compute_cold_start_p90_by_sap(df_orders, case_pack_by_sap)
                naive_line_map = _build_last_value_map(train_orders, schedule_key=schedule_key)
                actual_line_map = _order_to_line_map(test_order)
                spike_stats = {
                    "eligible_rows": 0,
                    "rows_quarantined": 0,
                    "raw_units_on_quarantined_rows": 0,
                    "model_units_on_quarantined_rows": 0,
                    "units_quarantined": 0,
                    "promo_tagged_rows_quarantined": 0,
                }
                lift_map: Dict[LineKey, int] = {}
                lift_stats = {
                    "eligible_rows": 0,
                    "regimes_started": 0,
                    "regimes_ended_by_normal": 0,
                    "regimes_ended_by_absence": 0,
                    "rows_quarantined": 0,
                    "raw_units_on_quarantined_rows": 0,
                    "model_units_on_quarantined_rows": 0,
                    "units_quarantined": 0,
                    "active_lift_keys": 0,
                    "lines_overridden": 0,
                    "units_delta": 0,
                    "lines_helped": 0,
                    "lines_harmed": 0,
                    "lines_unchanged_error": 0,
                }
                hurdle_stats = {
                    "schedule_cycles": 0,
                    "training_rows": 0,
                    "dense_zero_rows": 0,
                    "positive_rows": 0,
                    "target_keys": 0,
                    "classifier_fitted": 0,
                    "quantity_model_fitted": 0,
                    "positive_predictions": 0,
                }
                reconciled_stats = {
                    "route_cycles": 0,
                    "target_keys": 0,
                    "positive_predictions": 0,
                    "stale_keys_zeroed": 0,
                }
                settlement_stats = {
                    "eligible_saps": 0,
                    "reallocated_saps": 0,
                    "units_moved_between_stores": 0,
                    "settlement_weeks_used": 0,
                }

                if mode == "copy_last_order":
                    last_same_schedule = max(
                        train_orders,
                        key=lambda o: _order_delivery_dt(o) or datetime.min,
                    )
                    pred_line_map = _order_to_line_map(last_same_schedule)
                    key_source_map = {k: "last_order_anchor" for k in pred_line_map.keys()}
                    key_meta_map: Dict[LineKey, Dict[str, float]] = {}
                    pred_band_map = {
                        k: (
                            max(0.0, float(v) * 0.7),
                            float(v),
                            float(v) * 1.3,
                            0.72,
                        )
                        for k, v in pred_line_map.items()
                    }
                else:
                    if test_settlement_store_share_v1:
                        preds_df, reconciled_stats = predict_store_first_case_hierarchy(
                            candidate_train_orders,
                            schedule_key,
                            target_dt,
                            case_pack_by_sap,
                        )
                        route_settlement = settlement_daily[
                            settlement_daily["route_number"].astype(str) == str(route_number)
                        ]
                        preds_df, settlement_stats = apply_settlement_store_share(
                            preds_df,
                            route_settlement,
                            forecast_as_of,
                        )
                        training_scope = "store_first_case_plus_settlement_share_all_cycles"
                        mode_reason = (
                            "S1_H4_plus_past_available_settlement_store_share;"
                            f"settlement_weeks_used={settlement_stats.get('settlement_weeks_used', 0)}"
                        )
                    elif test_store_first_case_hierarchy_v1:
                        preds_df, reconciled_stats = predict_store_first_case_hierarchy(
                            candidate_train_orders,
                            schedule_key,
                            target_dt,
                            case_pack_by_sap,
                        )
                        training_scope = "store_first_case_reconciled_all_cycles"
                        mode_reason = (
                            f"H4_store_total_then_sap_cases_then_store_allocation;"
                            f"past_only_route_cycles={reconciled_stats.get('route_cycles', 0)}"
                        )
                    elif test_store_first_hierarchy_v1:
                        preds_df, reconciled_stats = predict_store_first_hierarchy(
                            candidate_train_orders,
                            schedule_key,
                            target_dt,
                        )
                        training_scope = "store_first_reconciled_all_cycles"
                        mode_reason = (
                            f"H3_store_total_then_sap_then_cycle;past_only_route_cycles="
                            f"{reconciled_stats.get('route_cycles', 0)}"
                        )
                    elif test_store_sap_reconciled_v1:
                        preds_df, reconciled_stats = predict_store_sap_reconciled(
                            candidate_train_orders,
                            schedule_key,
                            target_dt,
                        )
                        training_scope = "store_sap_reconciled_all_cycles"
                        mode_reason = (
                            f"H2_store_sap_total_then_cycle_allocation;past_only_route_cycles="
                            f"{reconciled_stats.get('route_cycles', 0)}"
                        )
                    elif test_hierarchical_hurdle_v1:
                        preds_df, hurdle_stats = predict_hierarchical_hurdle(
                            train_orders,
                            schedule_key,
                            target_dt,
                        )
                        training_scope = "schedule_dense_hurdle"
                        mode_reason = (
                            f"H1_dense_hurdle;past_only_schedule_cycles="
                            f"{hurdle_stats.get('schedule_cycles', 0)}"
                        )
                    else:
                        model_orders = train_orders
                        if test_transient_spike_quarantine:
                            model_orders, spike_stats = _apply_test_transient_spike_quarantine(
                                train_orders
                            )
                        elif test_inferred_transient_lift:
                            model_orders, lift_map, inferred_stats = (
                                _apply_test_inferred_lift_regime(train_orders, schedule_key)
                            )
                            lift_stats.update(inferred_stats)
                        model_df_orders = _orders_to_dataframe(model_orders)
                        model_df_orders, default_gap = _inject_training_schedule_features(
                            model_df_orders
                        )
                        if temporal_corrections:
                            corrections_df = _load_temporal_corrections(
                                route_number,
                                schedule_key,
                                forecast_as_of,
                                held_out_order_id=str(getattr(test_order, "id", "") or ""),
                                target_delivery_dt=target_dt,
                            )
                        else:
                            corrections_df = _load_corrections_from_postgres(route_number, schedule_key)

                        feat = _build_features(model_df_orders, corrections_df)
                        preds_df = _train_and_predict(
                            feat=feat,
                            target_date=target_dt,
                            schedule_key=schedule_key,
                            days_until_next=default_gap,
                            active_promos=None,
                            band_scale=band_scale,
                        )
                        preds_df = _apply_live_slow_gate(preds_df, case_pack_by_sap)

                    pred_line_map = _build_pred_line_map(preds_df)
                    key_source_map = _build_key_source_map(preds_df)
                    key_meta_map = _build_key_meta_map(preds_df)
                    pred_band_map = _build_pred_band_map(
                        preds_df,
                        center_offset_units=band_center_offset,
                        source_band_adjustments=source_band_adjustments,
                    )
                    if test_inferred_transient_lift:
                        lift_stats.update(_apply_test_inferred_lift_overrides(
                            pred_line_map=pred_line_map,
                            pred_band_map=pred_band_map,
                            key_source_map=key_source_map,
                            lift_map=lift_map,
                            actual_line_map=actual_line_map,
                        ))

                cooldown_stats = {
                    "slow_positive_predictions": 0,
                    "prior_actual_absence_keys": 0,
                    "explicit_removal_keys": 0,
                    "both_signal_keys": 0,
                    "eligible_cooldown_keys": 0,
                    "lines_zeroed": 0,
                    "units_suppressed": 0,
                    "actual_units_on_zeroed_lines": 0,
                    "lines_helped": 0,
                    "lines_harmed": 0,
                    "lines_unchanged_error": 0,
                }
                if (
                    test_one_cycle_slow_mover_cooldown
                    or test_strict_one_cycle_slow_mover_cooldown
                ):
                    removed_keys = _load_test_one_cycle_removed_keys(
                        route_number=route_number,
                        schedule_key=schedule_key,
                        train_orders=train_orders,
                        cutoff_dt=forecast_as_of,
                        target_delivery_dt=target_dt,
                    )
                    cooldown_keys, trigger_stats = _build_test_one_cycle_slow_cooldown_keys(
                        train_orders=train_orders,
                        schedule_key=schedule_key,
                        pred_line_map=pred_line_map,
                        key_source_map=key_source_map,
                        key_meta_map=key_meta_map,
                        removed_keys=removed_keys,
                        strict_slow_only=bool(test_strict_one_cycle_slow_mover_cooldown),
                    )
                    cooldown_stats.update(trigger_stats)
                    cooldown_stats.update(_apply_test_one_cycle_slow_cooldown(
                        pred_line_map=pred_line_map,
                        pred_band_map=pred_band_map,
                        key_source_map=key_source_map,
                        removed_keys=cooldown_keys,
                        actual_line_map=actual_line_map,
                    ))

                fold_metrics = _compute_fold_metrics(
                    route_number=route_number,
                    schedule_key=schedule_key,
                    target_dt=target_dt,
                    fold_index=fold_counter + 1,
                    train_orders_count=len(train_orders),
                    pred_line_map=pred_line_map,
                    pred_band_map=pred_band_map,
                    key_source_map=key_source_map,
                    key_meta_map=key_meta_map,
                    naive_line_map=naive_line_map,
                    actual_line_map=actual_line_map,
                    case_pack_by_sap=case_pack_by_sap,
                    cold_start_p90_by_sap=cold_start_p90_by_sap,
                )
                if fold_metrics:
                    fold_metrics["mode"] = mode
                    fold_metrics["training_scope"] = training_scope
                    fold_metrics["mode_reason"] = mode_reason
                    fold_metrics["forecast_as_of"] = forecast_as_of.isoformat()
                    fold_metrics["learning_target"] = "finalized_order_quantities"
                    fold_metrics["training_same_schedule_finalized_orders"] = int(
                        schedule_actual_order_count
                    )
                    fold_metrics["training_correction_orders"] = int(corrected_order_count)
                    fold_metrics["test_continuous_actual_learning_bypassed_correction_gate"] = int(
                        correction_gate_bypassed
                    )
                    test_candidates = []
                    if test_continuous_actual_learning:
                        test_candidates.append("A1_continuous_finalized_order_learning")
                    if test_one_cycle_slow_mover_cooldown:
                        test_candidates.append(
                            "R1_one_cycle_slow_mover_cooldown_actual_plus_corrections"
                        )
                    if test_strict_one_cycle_slow_mover_cooldown:
                        test_candidates.append(
                            "R2_strict_true_slow_mover_cooldown_actual_plus_corrections"
                        )
                    if test_transient_spike_quarantine:
                        test_candidates.append(
                            "P1_past_only_transient_spike_quarantine"
                        )
                    if test_inferred_transient_lift:
                        test_candidates.append(
                            "P2_user_signaled_transient_lift_regime"
                        )
                    if test_hierarchical_hurdle_v1:
                        test_candidates.append(
                            "H1_dense_hierarchical_hurdle_v1"
                        )
                    if test_store_sap_reconciled_v1:
                        test_candidates.append(
                            "H2_store_sap_total_then_cycle_allocation_v1"
                        )
                    if test_store_first_hierarchy_v1:
                        test_candidates.append(
                            "H3_store_total_then_sap_then_cycle_v1"
                        )
                    if test_store_first_case_hierarchy_v1:
                        test_candidates.append(
                            "H4_store_total_then_sap_cases_then_store_allocation_v1"
                        )
                    if test_settlement_store_share_v1:
                        test_candidates.append(
                            "S1_H4_plus_settlement_store_share_v1"
                        )
                    fold_metrics["test_candidate"] = "+".join(test_candidates) or "baseline"
                    for name, value in cooldown_stats.items():
                        fold_metrics[f"test_cooldown_{name}"] = int(value)
                    for name, value in spike_stats.items():
                        fold_metrics[f"test_spike_{name}"] = int(value)
                    for name, value in lift_stats.items():
                        fold_metrics[f"test_lift_{name}"] = int(value)
                    for name, value in hurdle_stats.items():
                        fold_metrics[f"test_hurdle_{name}"] = int(value)
                    for name, value in reconciled_stats.items():
                        fold_metrics[f"test_reconciled_{name}"] = int(value)
                    for name, value in settlement_stats.items():
                        fold_metrics[f"test_settlement_{name}"] = int(value)
                    fold_rows.append(fold_metrics)
                    source_rows.extend(
                        _compute_source_breakdown(
                            route_number=route_number,
                            schedule_key=schedule_key,
                            target_dt=target_dt,
                            fold_index=fold_counter + 1,
                            train_orders_count=len(train_orders),
                            key_source_map=key_source_map,
                            pred_line_map=pred_line_map,
                            pred_band_map=pred_band_map,
                            actual_line_map=actual_line_map,
                            cold_start_p90_by_sap=cold_start_p90_by_sap,
                        )
                    )
                    fold_counter += 1

            print(
                f"[backtest] Route {route_number} schedule {schedule_key}: "
                f"{fold_counter} fold(s) evaluated."
            )

    if not fold_rows:
        return pd.DataFrame(), pd.DataFrame()
    folds_df = pd.DataFrame(fold_rows).sort_values(
        ["route_number", "schedule_key", "target_delivery_date"]
    )
    sources_df = pd.DataFrame(source_rows).sort_values(
        ["route_number", "schedule_key", "target_delivery_date", "source"]
    ) if source_rows else pd.DataFrame()
    return folds_df, sources_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk-forward backtest for forecasting engine")
    parser.add_argument(
        "--routes",
        help="Comma-separated route numbers. If omitted, auto-discovers routes from orders_historical.",
    )
    parser.add_argument("--sinceDays", type=int, default=365, help="Lookback window for source orders")
    parser.add_argument(
        "--asOfDate",
        help="Fixed YYYY-MM-DD anchor for the lookback window (default: current UTC date)",
    )
    parser.add_argument(
        "--minTrainOrders",
        type=int,
        default=1,
        help="Minimum prior same-schedule orders required before first fold",
    )
    parser.add_argument(
        "--maxFolds",
        type=int,
        default=0,
        help="Max folds per route/schedule (0 = all)",
    )
    parser.add_argument(
        "--scheduleKeys",
        help="Optional comma-separated schedule keys (e.g., monday,thursday)",
    )
    parser.add_argument(
        "--outputDir",
        default="logs/backtests",
        help="Directory for CSV outputs",
    )
    parser.add_argument(
        "--disableTemporalCorrections",
        action="store_true",
        help="Use route/schedule aggregate corrections without cutoff-date filtering",
    )
    parser.add_argument(
        "--ignoreBandCalibration",
        action="store_true",
        help="Ignore persisted band calibration and use default band scale",
    )
    parser.add_argument(
        "--disableStoreCentricContext",
        action="store_true",
        help="Train folds using same-schedule history only (legacy behavior).",
    )
    parser.add_argument(
        "--testOneCycleSlowMoverCooldown",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: zero a slow/intermittent line when it was absent "
            "from the immediately prior same-schedule actual order or explicitly removed"
        ),
    )
    parser.add_argument(
        "--testStrictOneCycleSlowMoverCooldown",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: apply the one-cycle absence/removal cooldown "
            "only to true slow movers (25%% order frequency or less)"
        ),
    )
    parser.add_argument(
        "--testContinuousActualLearning",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: let finalized order quantities qualify the model "
            "without requiring forecast-button correction rows"
        ),
    )
    parser.add_argument(
        "--testTransientSpikeQuarantine",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: cap extreme training quantities using only earlier "
            "same-schedule store/SAP history; held-out actuals remain unchanged"
        ),
    )
    parser.add_argument(
        "--testInferredTransientLift",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: infer an active lift from finalized same-schedule "
            "user quantities, preserve a separate normal baseline, and end lift "
            "when the user's quantity returns near normal or disappears"
        ),
    )
    parser.add_argument(
        "--testHierarchicalHurdleV1",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: replace the positive-line quantity regressor with "
            "a schedule-specific dense occurrence-plus-conditional-quantity model"
        ),
    )
    parser.add_argument(
        "--testStoreSapReconciledV1",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: forecast each store/SAP's rolling weekly total "
            "from monthly, weekly, and per-order signals, then allocate it to "
            "the target schedule"
        ),
    )
    parser.add_argument(
        "--testStoreFirstHierarchyV1",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: forecast each store's target-cycle total first, "
            "then reconcile store/SAP and cycle allocations to that fixed total"
        ),
    )
    parser.add_argument(
        "--testStoreFirstCaseHierarchyV1",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: forecast store totals, choose one whole-case "
            "total per SAP, then allocate those fixed units among stores"
        ),
    )
    parser.add_argument(
        "--testSettlementStoreShareV1",
        action="store_true",
        help=(
            "TEST HARNESS ONLY: run H4, then blend past-available SBT settlement "
            "sell-through into the allocation between the two Smith's stores"
        ),
    )
    parser.add_argument(
        "--settlementTruthCsv",
        help=(
            "Normalized settlement daily truth used only by "
            "--testSettlementStoreShareV1"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.testOneCycleSlowMoverCooldown and args.testStrictOneCycleSlowMoverCooldown:
        raise SystemExit(
            "[backtest] choose either --testOneCycleSlowMoverCooldown (R1) or "
            "--testStrictOneCycleSlowMoverCooldown (R2), not both"
        )
    if args.testTransientSpikeQuarantine and args.testInferredTransientLift:
        raise SystemExit(
            "[backtest] choose either --testTransientSpikeQuarantine (P1) or "
            "--testInferredTransientLift (P2), not both"
        )
    replacement_architectures = int(bool(args.testHierarchicalHurdleV1)) + int(
        bool(args.testStoreSapReconciledV1)
    ) + int(bool(args.testStoreFirstHierarchyV1)) + int(
        bool(args.testStoreFirstCaseHierarchyV1)
    ) + int(bool(args.testSettlementStoreShareV1))
    if replacement_architectures > 1:
        raise SystemExit(
            "[backtest] choose exactly one replacement architecture: H1, H2, H3, H4, or S1"
        )
    if replacement_architectures and (
        args.testOneCycleSlowMoverCooldown
        or args.testStrictOneCycleSlowMoverCooldown
        or args.testTransientSpikeQuarantine
        or args.testInferredTransientLift
    ):
        raise SystemExit(
            "[backtest] H1/H2/H3/H4/S1 are replacement architectures; "
            "do not combine it with R1, R2, P1, or P2"
        )
    if args.testSettlementStoreShareV1 and not args.settlementTruthCsv:
        raise SystemExit(
            "[backtest] --settlementTruthCsv is required with --testSettlementStoreShareV1"
        )

    if args.asOfDate:
        try:
            as_of_date = datetime.strptime(str(args.asOfDate), "%Y-%m-%d").date()
        except ValueError as exc:
            raise SystemExit("[backtest] --asOfDate must use YYYY-MM-DD") from exc
    else:
        as_of_date = datetime.now(timezone.utc).date()

    if args.routes:
        routes = [r.strip() for r in str(args.routes).split(",") if r.strip()]
    else:
        routes = _distinct_routes_from_history(args.sinceDays, as_of_date)
    if not routes:
        raise SystemExit("[backtest] No routes found to evaluate.")

    schedule_keys = None
    if args.scheduleKeys:
        schedule_keys = {s.strip().lower() for s in str(args.scheduleKeys).split(",") if s.strip()}

    folds_df, source_df = run_backtest(
        routes=routes,
        since_days=int(args.sinceDays),
        min_train_orders=int(args.minTrainOrders),
        max_folds=int(args.maxFolds),
        schedule_keys=schedule_keys,
        temporal_corrections=not bool(args.disableTemporalCorrections),
        ignore_band_calibration=bool(args.ignoreBandCalibration),
        store_centric_context=not bool(args.disableStoreCentricContext),
        as_of_date=as_of_date,
        test_one_cycle_slow_mover_cooldown=bool(args.testOneCycleSlowMoverCooldown),
        test_strict_one_cycle_slow_mover_cooldown=bool(
            args.testStrictOneCycleSlowMoverCooldown
        ),
        test_continuous_actual_learning=bool(args.testContinuousActualLearning),
        test_transient_spike_quarantine=bool(args.testTransientSpikeQuarantine),
        test_inferred_transient_lift=bool(args.testInferredTransientLift),
        test_hierarchical_hurdle_v1=bool(args.testHierarchicalHurdleV1),
        test_store_sap_reconciled_v1=bool(args.testStoreSapReconciledV1),
        test_store_first_hierarchy_v1=bool(args.testStoreFirstHierarchyV1),
        test_store_first_case_hierarchy_v1=bool(args.testStoreFirstCaseHierarchyV1),
        test_settlement_store_share_v1=bool(args.testSettlementStoreShareV1),
        settlement_truth_csv=args.settlementTruthCsv,
    )
    if folds_df.empty:
        raise SystemExit("[backtest] Completed with no folds evaluated.")

    scorecard_df = _summarize_scorecard(folds_df, since_days=int(args.sinceDays))

    out_dir = Path(args.outputDir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    folds_path = out_dir / f"walk_forward_folds_{stamp}.csv"
    scorecard_path = out_dir / f"walk_forward_scorecard_{stamp}.csv"
    sources_path = out_dir / f"walk_forward_sources_{stamp}.csv"
    folds_df.to_csv(folds_path, index=False)
    scorecard_df.to_csv(scorecard_path, index=False)
    if not source_df.empty:
        source_df.to_csv(sources_path, index=False)

    artifact_paths = [folds_path, scorecard_path]
    if not source_df.empty:
        artifact_paths.append(sources_path)
    recorded_env_names = sorted(
        name
        for name in os.environ
        if name.startswith("FORECAST_")
        or name in {"MIN_CORRECTED_ORDERS_FOR_TRAINING", "SLOW_MOVER_ORDER_FREQ_THRESHOLD"}
    )
    manifest = {
        "schema_version": "routespark-forecast-backtest-manifest-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "backtester": str(Path(__file__).resolve()),
        "backtester_sha256": _sha256_file(Path(__file__).resolve()),
        "git_revision": _git_revision(),
        "python_version": platform.python_version(),
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "arguments": {
            "routes": routes,
            "since_days": int(args.sinceDays),
            "as_of_date": as_of_date.isoformat(),
            "min_train_orders": int(args.minTrainOrders),
            "max_folds": int(args.maxFolds),
            "schedule_keys": sorted(schedule_keys) if schedule_keys else None,
            "temporal_corrections": not bool(args.disableTemporalCorrections),
            "ignore_band_calibration": bool(args.ignoreBandCalibration),
            "store_centric_context": not bool(args.disableStoreCentricContext),
            "test_one_cycle_slow_mover_cooldown": bool(args.testOneCycleSlowMoverCooldown),
            "test_strict_one_cycle_slow_mover_cooldown": bool(
                args.testStrictOneCycleSlowMoverCooldown
            ),
            "test_continuous_actual_learning": bool(args.testContinuousActualLearning),
            "test_transient_spike_quarantine": bool(args.testTransientSpikeQuarantine),
            "test_inferred_transient_lift": bool(args.testInferredTransientLift),
            "test_hierarchical_hurdle_v1": bool(args.testHierarchicalHurdleV1),
            "test_store_sap_reconciled_v1": bool(args.testStoreSapReconciledV1),
            "test_store_first_hierarchy_v1": bool(args.testStoreFirstHierarchyV1),
            "test_store_first_case_hierarchy_v1": bool(
                args.testStoreFirstCaseHierarchyV1
            ),
            "test_settlement_store_share_v1": bool(args.testSettlementStoreShareV1),
            "settlement_truth_csv": args.settlementTruthCsv,
        },
        "recorded_forecast_environment": {
            name: os.environ[name] for name in recorded_env_names
        },
        "result": {
            "fold_count": int(len(folds_df)),
            "routes_evaluated": sorted({str(value) for value in folds_df["route_number"]}),
            "first_target_delivery_date": str(folds_df["target_delivery_date"].min()),
            "last_target_delivery_date": str(folds_df["target_delivery_date"].max()),
        },
        "artifacts": {
            path.name: {"sha256": _sha256_file(path), "bytes": path.stat().st_size}
            for path in artifact_paths
        },
        "input_artifacts": {
            "settlement_truth_csv": {
                "path": str(Path(args.settlementTruthCsv).resolve()),
                "sha256": _sha256_file(Path(args.settlementTruthCsv).resolve()),
                "availability": "simulated Monday receipt; not observed historical upload",
            }
            if args.settlementTruthCsv
            else None
        },
        "known_limitations": [
            "model-core scoring does not yet replay all production post-processing",
            "catalog, schedule, carry, promo, and calibration inputs are not fully versioned as-of each fold",
            "order_date midnight is a conservative substitute for immutable forecast generated_at",
            "training-row aggregate features are not yet expanding and shifted",
            "finalized positive order lines are learned, but absent eligible lines are not yet a dense zero target panel",
        ],
    }
    manifest_path = out_dir / f"walk_forward_manifest_{stamp}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print(f"[backtest] Wrote folds CSV: {folds_path}")
    print(f"[backtest] Wrote scorecard CSV: {scorecard_path}")
    if not source_df.empty:
        print(f"[backtest] Wrote source breakdown CSV: {sources_path}")
    print(f"[backtest] Wrote manifest JSON: {manifest_path}")
    print(f"[backtest] Routes evaluated: {sorted(set(folds_df['route_number']))}")
    print(f"[backtest] Total folds: {len(folds_df)}")


if __name__ == "__main__":
    main()

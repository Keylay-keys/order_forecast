#!/usr/bin/env python3
"""Test-lab-only dense hierarchical hurdle forecaster.

This module is deliberately separate from ``forecast_engine.py``. It consumes
only already-available finalized orders supplied by a walk-forward fold and
never reads or writes production state.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor


LineKey = Tuple[str, str]

FEATURE_COLUMNS = [
    "history_depth",
    "key_frequency",
    "sap_frequency",
    "store_frequency",
    "global_frequency",
    "hit_lag_1",
    "hit_rate_3",
    "hit_rate_6",
    "cycles_since_positive",
    "last_units",
    "positive_median_4",
    "positive_median_all",
    "positive_ewma",
    "sap_positive_median",
    "store_positive_median",
    "days_since_cycle",
    "delivery_dow",
    "month_sin",
    "month_cos",
]


def _parse_order_date(order) -> Optional[datetime]:
    raw = str(getattr(order, "expected_delivery_date", "") or "")
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None


def _line_map(order) -> Tuple[Dict[LineKey, float], Dict[LineKey, str]]:
    quantities: Dict[LineKey, float] = defaultdict(float)
    names: Dict[LineKey, str] = {}
    for store in getattr(order, "stores", []) or []:
        store_id = str(getattr(store, "store_id", "") or "")
        store_name = str(getattr(store, "store_name", "") or store_id)
        if not store_id:
            continue
        for item in getattr(store, "items", []) or []:
            sap = str(getattr(item, "sap", "") or "")
            if not sap:
                continue
            try:
                units = max(0.0, float(getattr(item, "quantity", 0.0) or 0.0))
            except (TypeError, ValueError):
                units = 0.0
            if units <= 0:
                continue
            key = (store_id, sap)
            quantities[key] += units
            names[key] = store_name
    return dict(quantities), names


def _smoothed_rate(hits: float, opportunities: float) -> float:
    return float((hits + 1.0) / (opportunities + 2.0))


def _median(values: Iterable[float], default: float = 0.0) -> float:
    cleaned = [float(value) for value in values if float(value) > 0]
    return float(np.median(cleaned)) if cleaned else float(default)


def _ewma(values: Iterable[float], alpha: float = 0.45) -> float:
    cleaned = [float(value) for value in values if float(value) > 0]
    if not cleaned:
        return 0.0
    result = cleaned[0]
    for value in cleaned[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return float(result)


def _ewma_including_zeros(values: Iterable[float], alpha: float = 0.55) -> float:
    cleaned = [max(0.0, float(value)) for value in values]
    if not cleaned:
        return 0.0
    result = cleaned[0]
    for value in cleaned[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return float(result)


def _calendar_features(target_date: datetime) -> Dict[str, float]:
    angle = 2.0 * np.pi * (float(target_date.month) - 1.0) / 12.0
    return {
        "delivery_dow": float(target_date.weekday()),
        "month_sin": float(np.sin(angle)),
        "month_cos": float(np.cos(angle)),
    }


def _feature_row(
    key: LineKey,
    target_date: datetime,
    last_cycle_date: Optional[datetime],
    key_state: Dict[LineKey, Dict[str, List[float]]],
    sap_hits: Dict[str, int],
    sap_opportunities: Dict[str, int],
    sap_positive: Dict[str, List[float]],
    store_hits: Dict[str, int],
    store_opportunities: Dict[str, int],
    store_positive: Dict[str, List[float]],
    global_hits: int,
    global_opportunities: int,
) -> Dict[str, float]:
    store_id, sap = key
    state = key_state[key]
    occurrence = list(state["occurrence"])
    quantities = list(state["quantities"])
    positive = list(state["positive"])
    opportunities = len(occurrence)
    hits = int(sum(occurrence))
    since_positive = 99
    for offset, hit in enumerate(reversed(occurrence), start=1):
        if hit > 0:
            since_positive = offset - 1
            break
    if last_cycle_date is None:
        days_since_cycle = 7.0
    else:
        days_since_cycle = float(max(0, (target_date - last_cycle_date).days))

    row = {
        "history_depth": float(opportunities),
        "key_frequency": _smoothed_rate(hits, opportunities),
        "sap_frequency": _smoothed_rate(
            sap_hits.get(sap, 0), sap_opportunities.get(sap, 0)
        ),
        "store_frequency": _smoothed_rate(
            store_hits.get(store_id, 0), store_opportunities.get(store_id, 0)
        ),
        "global_frequency": _smoothed_rate(global_hits, global_opportunities),
        "hit_lag_1": float(occurrence[-1]) if occurrence else 0.0,
        "hit_rate_3": float(np.mean(occurrence[-3:])) if occurrence else 0.0,
        "hit_rate_6": float(np.mean(occurrence[-6:])) if occurrence else 0.0,
        "cycles_since_positive": float(since_positive),
        "last_units": float(quantities[-1]) if quantities else 0.0,
        "positive_median_4": _median(positive[-4:]),
        "positive_median_all": _median(positive),
        "positive_ewma": _ewma(positive[-6:]),
        "sap_positive_median": _median(sap_positive.get(sap, [])),
        "store_positive_median": _median(store_positive.get(store_id, [])),
        "days_since_cycle": days_since_cycle,
    }
    row.update(_calendar_features(target_date))
    return row


def build_dense_hurdle_panel(train_orders, schedule_key: str):
    """Build past-only dense rows and target features for one schedule."""
    wanted = str(schedule_key or "").lower()
    cycles = [
        order
        for order in train_orders
        if str(getattr(order, "schedule_key", "") or "").lower() == wanted
        and _parse_order_date(order) is not None
    ]
    cycles.sort(key=lambda order: (_parse_order_date(order), str(getattr(order, "id", ""))))

    known_keys: set[LineKey] = set()
    key_state: Dict[LineKey, Dict[str, List[float]]] = {}
    store_names: Dict[LineKey, str] = {}
    sap_hits: Dict[str, int] = defaultdict(int)
    sap_opportunities: Dict[str, int] = defaultdict(int)
    sap_positive: Dict[str, List[float]] = defaultdict(list)
    store_hits: Dict[str, int] = defaultdict(int)
    store_opportunities: Dict[str, int] = defaultdict(int)
    store_positive: Dict[str, List[float]] = defaultdict(list)
    global_hits = 0
    global_opportunities = 0
    training_rows: List[Dict[str, float]] = []
    last_cycle_date: Optional[datetime] = None

    for order in cycles:
        cycle_date = _parse_order_date(order)
        current, names = _line_map(order)
        store_names.update(names)

        # Only keys already known before this cycle enter its training panel.
        # A product's first appearance cannot be predicted without a catalog or
        # carry-list snapshot, so it becomes eligible starting next cycle.
        for key in sorted(known_keys):
            row = _feature_row(
                key,
                cycle_date,
                last_cycle_date,
                key_state,
                sap_hits,
                sap_opportunities,
                sap_positive,
                store_hits,
                store_opportunities,
                store_positive,
                global_hits,
                global_opportunities,
            )
            actual_units = float(current.get(key, 0.0))
            row["store_id"] = key[0]
            row["sap"] = key[1]
            row["y_occurrence"] = 1.0 if actual_units > 0 else 0.0
            row["y_units"] = actual_units
            training_rows.append(row)

        all_keys = known_keys | set(current)
        for key in sorted(all_keys):
            if key not in key_state:
                key_state[key] = {"occurrence": [], "quantities": [], "positive": []}
            units = float(current.get(key, 0.0))
            hit = 1 if units > 0 else 0
            state = key_state[key]
            state["occurrence"].append(float(hit))
            state["quantities"].append(units)
            if hit:
                state["positive"].append(units)
            store_id, sap = key
            sap_opportunities[sap] += 1
            store_opportunities[store_id] += 1
            global_opportunities += 1
            if hit:
                sap_hits[sap] += 1
                store_hits[store_id] += 1
                global_hits += 1
                sap_positive[sap].append(units)
                store_positive[store_id].append(units)
        known_keys = all_keys
        last_cycle_date = cycle_date

    panel = pd.DataFrame(training_rows)
    state = {
        "cycles": cycles,
        "known_keys": known_keys,
        "key_state": key_state,
        "store_names": store_names,
        "sap_hits": sap_hits,
        "sap_opportunities": sap_opportunities,
        "sap_positive": sap_positive,
        "store_hits": store_hits,
        "store_opportunities": store_opportunities,
        "store_positive": store_positive,
        "global_hits": global_hits,
        "global_opportunities": global_opportunities,
        "last_cycle_date": last_cycle_date,
    }
    return panel, state


def _empirical_occurrence_probability(row: pd.Series) -> float:
    depth = max(0.0, float(row["history_depth"]))
    local_weight = min(0.75, depth / 16.0)
    pooled = (
        0.55 * float(row["sap_frequency"])
        + 0.25 * float(row["store_frequency"])
        + 0.20 * float(row["global_frequency"])
    )
    return float(local_weight * float(row["key_frequency"]) + (1.0 - local_weight) * pooled)


def _empirical_positive_quantity(row: pd.Series) -> float:
    depth = max(0.0, float(row["history_depth"]))
    local_weight = min(0.80, depth / 12.0)
    local = max(float(row["positive_median_4"]), float(row["positive_ewma"]), 0.0)
    pooled = max(
        0.70 * float(row["sap_positive_median"])
        + 0.30 * float(row["store_positive_median"]),
        0.0,
    )
    return float(local_weight * local + (1.0 - local_weight) * pooled)


def predict_hierarchical_hurdle(
    train_orders,
    schedule_key: str,
    target_date: datetime,
):
    """Fit the experimental dense hurdle and return production-shaped rows."""
    panel, state = build_dense_hurdle_panel(train_orders, schedule_key)
    target_rows = []
    for key in sorted(state["known_keys"]):
        row = _feature_row(
            key,
            target_date,
            state["last_cycle_date"],
            state["key_state"],
            state["sap_hits"],
            state["sap_opportunities"],
            state["sap_positive"],
            state["store_hits"],
            state["store_opportunities"],
            state["store_positive"],
            state["global_hits"],
            state["global_opportunities"],
        )
        row["store_id"] = key[0]
        row["sap"] = key[1]
        target_rows.append(row)
    target = pd.DataFrame(target_rows)
    if target.empty:
        return pd.DataFrame(), {
            "schedule_cycles": len(state["cycles"]),
            "training_rows": len(panel),
            "dense_zero_rows": 0,
            "positive_rows": 0,
            "target_keys": 0,
            "classifier_fitted": 0,
            "quantity_model_fitted": 0,
            "positive_predictions": 0,
        }

    classifier = None
    quantity_model = None
    if not panel.empty and panel["y_occurrence"].nunique() == 2 and len(panel) >= 100:
        classifier = HistGradientBoostingClassifier(
            max_iter=80,
            max_leaf_nodes=15,
            min_samples_leaf=20,
            l2_regularization=2.0,
            class_weight="balanced",
            early_stopping=False,
            random_state=42,
        )
        classifier.fit(panel[FEATURE_COLUMNS], panel["y_occurrence"])

    positive_panel = panel[panel["y_units"] > 0].copy() if not panel.empty else panel
    if len(positive_panel) >= 40:
        quantity_model = HistGradientBoostingRegressor(
            loss="absolute_error",
            max_iter=80,
            max_leaf_nodes=15,
            min_samples_leaf=15,
            l2_regularization=2.0,
            early_stopping=False,
            random_state=42,
        )
        quantity_model.fit(
            positive_panel[FEATURE_COLUMNS],
            np.log1p(positive_panel["y_units"].to_numpy(dtype=float)),
        )

    if classifier is not None:
        model_probability = classifier.predict_proba(target[FEATURE_COLUMNS])[:, 1]
    else:
        model_probability = np.asarray(
            [_empirical_occurrence_probability(row) for _, row in target.iterrows()]
        )
    empirical_probability = np.asarray(
        [_empirical_occurrence_probability(row) for _, row in target.iterrows()]
    )
    probability = np.clip(0.75 * model_probability + 0.25 * empirical_probability, 0.0, 1.0)

    if quantity_model is not None:
        model_positive_units = np.expm1(quantity_model.predict(target[FEATURE_COLUMNS]))
    else:
        model_positive_units = np.asarray(
            [_empirical_positive_quantity(row) for _, row in target.iterrows()]
        )
    empirical_positive_units = np.asarray(
        [_empirical_positive_quantity(row) for _, row in target.iterrows()]
    )
    conditional_units = np.maximum(
        0.0,
        0.70 * model_positive_units + 0.30 * empirical_positive_units,
    )
    expected_units = probability * conditional_units
    expected_units = np.where(probability >= 0.20, expected_units, 0.0)

    predictions = []
    for index, row in target.iterrows():
        units = max(0.0, float(expected_units[index]))
        p = float(probability[index])
        conditional = max(0.0, float(conditional_units[index]))
        confidence = float(
            np.clip(0.35 + min(float(row["history_depth"]), 20.0) / 50.0 + abs(p - 0.5) * 0.30, 0.0, 0.90)
        )
        predictions.append(
            {
                "store": state["store_names"].get((row["store_id"], row["sap"]), row["store_id"]),
                "store_id": str(row["store_id"]),
                "sap": str(row["sap"]),
                "pred_units": units,
                "p10_units": 0.0 if p < 0.90 else max(0.0, conditional * 0.70),
                "p50_units": 0.0 if p < 0.50 else conditional,
                "p90_units": max(units, conditional * 1.30),
                "model_branch": "hierarchical_hurdle_v1",
                "confidence": confidence,
                "confidence_bucket": "high" if confidence >= 0.75 else "medium" if confidence >= 0.50 else "low",
                "order_frequency": float(row["key_frequency"]),
                "is_slow_mover": int(float(row["key_frequency"]) <= 0.25),
                "days_since_last_order": float(row["cycles_since_positive"] * max(row["days_since_cycle"], 1.0)),
                "days_since_last_order_same_schedule": float(row["cycles_since_positive"] * max(row["days_since_cycle"], 1.0)),
                "corr_samples": 0.0,
                "corr_removal_rate": 0.0,
                "corr_avg_ratio": 1.0,
                "corr_avg_delta": 0.0,
                "corr_add_rate": 0.0,
                "corr_avg_add_units": 0.0,
                "corr_recent_removal_streak": 0.0,
                "occurrence_probability": p,
                "conditional_positive_units": conditional,
            }
        )

    stats = {
        "schedule_cycles": len(state["cycles"]),
        "training_rows": len(panel),
        "dense_zero_rows": int((panel["y_occurrence"] == 0).sum()) if not panel.empty else 0,
        "positive_rows": int((panel["y_occurrence"] == 1).sum()) if not panel.empty else 0,
        "target_keys": len(target),
        "classifier_fitted": int(classifier is not None),
        "quantity_model_fitted": int(quantity_model is not None),
        "positive_predictions": int(sum(float(row["pred_units"]) > 0 for row in predictions)),
    }
    return pd.DataFrame(predictions), stats


def predict_store_sap_reconciled(
    train_orders,
    schedule_key: str,
    target_date: datetime,
):
    """Forecast store/SAP totals first, then allocate to an order cycle.

    H2 is a deterministic top-down candidate. It blends rolling 4/8/12-week
    store/SAP totals with recent per-order demand, then uses a locally observed
    schedule share shrunk toward the route-wide SAP schedule share. All inputs
    are supplied by the fold and precede its as-of cutoff.
    """
    wanted = str(schedule_key or "").lower()
    ordered = [order for order in train_orders if _parse_order_date(order) is not None]
    ordered.sort(key=lambda order: (_parse_order_date(order), str(getattr(order, "id", ""))))
    if not ordered:
        return pd.DataFrame(), {
            "route_cycles": 0,
            "target_keys": 0,
            "positive_predictions": 0,
            "stale_keys_zeroed": 0,
        }

    observations: Dict[LineKey, List[Tuple[datetime, str, float]]] = defaultdict(list)
    store_names: Dict[LineKey, str] = {}
    known_keys: set[LineKey] = set()
    schedule_cycle_dates: Dict[str, List[datetime]] = defaultdict(list)

    for order in ordered:
        delivery_date = _parse_order_date(order)
        order_schedule = str(getattr(order, "schedule_key", "") or "").lower()
        schedule_cycle_dates[order_schedule].append(delivery_date)
        current, names = _line_map(order)
        store_names.update(names)
        for key in known_keys | set(current):
            observations[key].append(
                (delivery_date, order_schedule, float(current.get(key, 0.0)))
            )
        known_keys |= set(current)

    recent_start = target_date - timedelta(days=84)
    sap_schedule_units: Dict[Tuple[str, str], float] = defaultdict(float)
    sap_total_units: Dict[str, float] = defaultdict(float)
    for key, rows in observations.items():
        sap = key[1]
        for delivery_date, row_schedule, units in rows:
            if recent_start <= delivery_date < target_date and units > 0:
                sap_schedule_units[(sap, row_schedule)] += units
                sap_total_units[sap] += units

    predictions = []
    stale_keys_zeroed = 0
    for key in sorted(known_keys):
        rows = [row for row in observations[key] if row[0] < target_date]
        if not rows:
            continue
        first_date = rows[0][0]
        exposure_days = max(1, (target_date - first_date).days)
        exposure_weeks = min(12, max(1, int(np.ceil(exposure_days / 7.0))))
        weekly_totals = []
        for offset in reversed(range(exposure_weeks)):
            end = target_date - timedelta(days=7 * offset)
            start = end - timedelta(days=7)
            weekly_totals.append(
                float(sum(units for dt, _, units in rows if start <= dt < end))
            )

        monthly_weekly_pace = float(sum(weekly_totals[-4:])) / max(
            1, min(4, len(weekly_totals))
        )
        eight_week_pace = float(sum(weekly_totals[-8:])) / max(
            1, min(8, len(weekly_totals))
        )
        twelve_week_pace = float(sum(weekly_totals)) / max(1, len(weekly_totals))
        recent_weekly_ewma = _ewma_including_zeros(weekly_totals, alpha=0.55)
        store_sap_weekly_total = max(
            0.0,
            0.40 * recent_weekly_ewma
            + 0.30 * monthly_weekly_pace
            + 0.20 * eight_week_pace
            + 0.10 * twelve_week_pace,
        )

        recent_rows = [row for row in rows if row[0] >= recent_start]
        local_total = float(sum(units for _, _, units in recent_rows))
        local_schedule_units = float(
            sum(units for _, row_schedule, units in recent_rows if row_schedule == wanted)
        )
        sap = key[1]
        sap_total = float(sap_total_units.get(sap, 0.0))
        if local_total > 0:
            local_share = local_schedule_units / local_total
        else:
            local_share = 0.0
        if sap_total > 0:
            sap_share = float(sap_schedule_units.get((sap, wanted), 0.0)) / sap_total
        else:
            all_cycles = sum(len(values) for values in schedule_cycle_dates.values())
            sap_share = len(schedule_cycle_dates.get(wanted, [])) / max(1, all_cycles)
        local_weight = min(0.85, len(recent_rows) / 12.0)
        allocation_share = float(
            np.clip(local_weight * local_share + (1.0 - local_weight) * sap_share, 0.0, 1.0)
        )

        recent_schedule_dates = [
            dt for dt in schedule_cycle_dates.get(wanted, []) if recent_start <= dt < target_date
        ]
        schedule_cycles_per_week = max(1.0, len(recent_schedule_dates) / 12.0)
        allocated_cycle_units = (
            store_sap_weekly_total * allocation_share / schedule_cycles_per_week
        )

        schedule_rows = [units for _, row_schedule, units in rows if row_schedule == wanted]
        recent_per_order = schedule_rows[-12:]
        if recent_per_order:
            per_order_expected = (
                0.50 * float(np.mean(recent_per_order[-4:]))
                + 0.30 * float(np.median(recent_per_order[-6:]))
                + 0.20 * float(np.mean(recent_per_order))
            )
            occurrence_probability = _smoothed_rate(
                sum(units > 0 for units in recent_per_order), len(recent_per_order)
            )
        else:
            per_order_expected = 0.0
            occurrence_probability = 0.0

        predicted_units = max(
            0.0,
            0.70 * allocated_cycle_units + 0.30 * per_order_expected,
        )
        days_since_positive = 999.0
        for delivery_date, _, units in reversed(rows):
            if units > 0:
                days_since_positive = float(max(0, (target_date - delivery_date).days))
                break
        if store_sap_weekly_total < 0.5 or predicted_units < 0.5:
            if predicted_units > 0:
                stale_keys_zeroed += 1
            predicted_units = 0.0

        conditional_positive = (
            predicted_units / occurrence_probability
            if occurrence_probability > 0
            else 0.0
        )
        confidence = float(
            np.clip(0.35 + min(len(recent_rows), 12) / 30.0 + abs(occurrence_probability - 0.5) * 0.20, 0.0, 0.90)
        )
        predictions.append(
            {
                "store": store_names.get(key, key[0]),
                "store_id": key[0],
                "sap": key[1],
                "pred_units": predicted_units,
                "p10_units": max(0.0, predicted_units * 0.55),
                "p50_units": predicted_units,
                "p90_units": predicted_units * 1.55,
                "model_branch": "store_sap_reconciled_v1",
                "confidence": confidence,
                "confidence_bucket": "high" if confidence >= 0.75 else "medium" if confidence >= 0.50 else "low",
                "order_frequency": occurrence_probability,
                "is_slow_mover": int(occurrence_probability <= 0.25),
                "days_since_last_order": days_since_positive,
                "days_since_last_order_same_schedule": days_since_positive,
                "corr_samples": 0.0,
                "corr_removal_rate": 0.0,
                "corr_avg_ratio": 1.0,
                "corr_avg_delta": 0.0,
                "corr_add_rate": 0.0,
                "corr_avg_add_units": 0.0,
                "corr_recent_removal_streak": 0.0,
                "occurrence_probability": occurrence_probability,
                "conditional_positive_units": conditional_positive,
                "store_sap_weekly_total": store_sap_weekly_total,
                "allocation_share": allocation_share,
                "allocated_cycle_units": allocated_cycle_units,
                "per_order_expected": per_order_expected,
            }
        )

    stats = {
        "route_cycles": len(ordered),
        "target_keys": len(predictions),
        "positive_predictions": int(sum(row["pred_units"] > 0 for row in predictions)),
        "stale_keys_zeroed": stale_keys_zeroed,
    }
    return pd.DataFrame(predictions), stats


def predict_store_first_hierarchy(
    train_orders,
    schedule_key: str,
    target_date: datetime,
):
    """Reconcile store/SAP allocations to a forecasted store-order total.

    H3 uses H2 only to estimate the within-store SAP mix. The authoritative
    top level is a robust forecast of each store's total units on the target
    schedule. Every SAP prediction is scaled so its store sums exactly to that
    total before integer rounding in the backtest scorer.
    """
    predictions, h2_stats = predict_store_sap_reconciled(
        train_orders,
        schedule_key,
        target_date,
    )
    empty_stats = {
        **h2_stats,
        "stores_reconciled": 0,
        "raw_predicted_units": 0,
        "reconciled_predicted_units": 0,
        "store_total_history_min": 0,
        "store_total_history_max": 0,
    }
    if predictions.empty:
        return predictions, empty_stats

    wanted = str(schedule_key or "").lower()
    cycles = [
        order
        for order in train_orders
        if str(getattr(order, "schedule_key", "") or "").lower() == wanted
        and _parse_order_date(order) is not None
        and _parse_order_date(order) < target_date
    ]
    cycles.sort(key=lambda order: (_parse_order_date(order), str(getattr(order, "id", ""))))
    target_stores = set(predictions["store_id"].astype(str))
    totals_by_store: Dict[str, List[float]] = {store_id: [] for store_id in target_stores}
    for order in cycles:
        current, _ = _line_map(order)
        for store_id in target_stores:
            totals_by_store[store_id].append(
                float(sum(units for (sid, _), units in current.items() if sid == store_id))
            )

    predictions = predictions.copy()
    predictions["store_total_target"] = 0.0
    predictions["store_reconciliation_scale"] = 0.0
    raw_total = float(predictions["pred_units"].sum())
    histories = []
    stores_reconciled = 0
    for store_id, indexes in predictions.groupby("store_id").groups.items():
        history = totals_by_store.get(str(store_id), [])[-12:]
        if not history:
            continue
        histories.append(len(history))
        recent4 = history[-4:]
        recent8 = history[-8:]
        store_total_target = max(
            0.0,
            0.50 * float(np.median(recent4))
            + 0.30 * _ewma_including_zeros(recent8, alpha=0.55)
            + 0.20 * float(np.mean(history)),
        )
        raw_store_total = float(predictions.loc[indexes, "pred_units"].sum())
        if raw_store_total <= 0:
            continue
        scale = store_total_target / raw_store_total
        predictions.loc[indexes, "pred_units"] *= scale
        predictions.loc[indexes, "p10_units"] *= scale
        predictions.loc[indexes, "p50_units"] *= scale
        predictions.loc[indexes, "p90_units"] *= scale
        predictions.loc[indexes, "conditional_positive_units"] *= scale
        predictions.loc[indexes, "allocated_cycle_units"] *= scale
        predictions.loc[indexes, "per_order_expected"] *= scale
        predictions.loc[indexes, "store_total_target"] = store_total_target
        predictions.loc[indexes, "store_reconciliation_scale"] = scale
        predictions.loc[indexes, "model_branch"] = "store_first_hierarchy_v1"
        stores_reconciled += 1

    reconciled_total = float(predictions["pred_units"].sum())
    stats = {
        **h2_stats,
        "stores_reconciled": stores_reconciled,
        "raw_predicted_units": int(round(raw_total)),
        "reconciled_predicted_units": int(round(reconciled_total)),
        "store_total_history_min": min(histories) if histories else 0,
        "store_total_history_max": max(histories) if histories else 0,
    }
    return predictions, stats


def predict_store_first_case_hierarchy(
    train_orders,
    schedule_key: str,
    target_date: datetime,
    case_pack_by_sap: Dict[str, int],
):
    """Make one whole-case decision per SAP, then allocate units to stores.

    H4 prevents post-hoc case rounding from independently inflating many small
    store predictions. The nearest whole-case SAP total is chosen from H3's
    reconciled expected units and distributed by largest remainder.
    """
    predictions, h3_stats = predict_store_first_hierarchy(
        train_orders,
        schedule_key,
        target_date,
    )
    stats = {
        **h3_stats,
        "sap_case_decisions": 0,
        "sap_case_zeroes": 0,
        "pre_case_units": 0,
        "post_case_units": 0,
    }
    if predictions.empty:
        return predictions, stats

    predictions = predictions.copy()
    pre_case_units = float(predictions["pred_units"].sum())
    sap_case_decisions = 0
    sap_case_zeroes = 0
    for sap, indexes in predictions.groupby("sap").groups.items():
        raw = predictions.loc[indexes, "pred_units"].astype(float).clip(lower=0.0)
        raw_total = float(raw.sum())
        case_pack = int(case_pack_by_sap.get(str(sap), 0) or 0)
        if case_pack > 0:
            target_total = int(round(raw_total / case_pack)) * case_pack
            sap_case_decisions += 1
        else:
            target_total = int(round(raw_total))
        target_total = max(0, target_total)
        if raw_total > 0 and target_total == 0:
            sap_case_zeroes += 1

        if raw_total <= 0 or target_total <= 0:
            allocated = {index: 0 for index in indexes}
        else:
            exact = {index: target_total * float(raw.loc[index]) / raw_total for index in indexes}
            allocated = {index: int(np.floor(value)) for index, value in exact.items()}
            remainder = target_total - sum(allocated.values())
            ranked = sorted(
                indexes,
                key=lambda index: (exact[index] - allocated[index], str(index)),
                reverse=True,
            )
            for index in ranked[:remainder]:
                allocated[index] += 1

        for index in indexes:
            prior = float(predictions.at[index, "pred_units"])
            next_units = float(allocated[index])
            scale = next_units / prior if prior > 0 else 0.0
            predictions.at[index, "pred_units"] = next_units
            predictions.at[index, "p10_units"] = max(
                0.0, float(predictions.at[index, "p10_units"]) * scale
            )
            predictions.at[index, "p50_units"] = next_units
            predictions.at[index, "p90_units"] = max(
                next_units, float(predictions.at[index, "p90_units"]) * scale
            )
            predictions.at[index, "conditional_positive_units"] = (
                float(predictions.at[index, "conditional_positive_units"]) * scale
            )
            predictions.at[index, "model_branch"] = "store_first_case_hierarchy_v1"

    stats.update(
        {
            "sap_case_decisions": sap_case_decisions,
            "sap_case_zeroes": sap_case_zeroes,
            "pre_case_units": int(round(pre_case_units)),
            "post_case_units": int(round(float(predictions["pred_units"].sum()))),
        }
    )
    return predictions, stats

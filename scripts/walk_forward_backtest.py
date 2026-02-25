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
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

# Support running as a script without package context
try:
    from .firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from .forecast_engine import (
        MIN_CORRECTED_ORDERS_FOR_TRAINING,
        MIN_ORDERS_FOR_TRAINING,
        _build_features,
        _compute_schedule_features,
        _load_corrections_from_postgres,
        _orders_to_dataframe,
        _parse_date,
        _summarize_schedule_shape,
        _train_and_predict,
    )
    from .model_validator import validate_predictions
    from .pg_utils import fetch_all, fetch_one
except ImportError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from forecast_engine import (
        MIN_CORRECTED_ORDERS_FOR_TRAINING,
        MIN_ORDERS_FOR_TRAINING,
        _build_features,
        _compute_schedule_features,
        _load_corrections_from_postgres,
        _orders_to_dataframe,
        _parse_date,
        _summarize_schedule_shape,
        _train_and_predict,
    )
    from model_validator import validate_predictions
    from pg_utils import fetch_all, fetch_one


LineKey = Tuple[str, str]  # (store_id, sap)
BandTuple = Tuple[float, float, float, float]  # (p10, p50, p90, confidence)


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
        return 0.0
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


def _build_last_value_map(train_orders) -> Dict[LineKey, int]:
    out: Dict[LineKey, int] = {}
    for order in train_orders:
        for key, qty in _order_to_line_map(order).items():
            out[key] = qty
    return out


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


def _load_temporal_corrections(route_number: str, schedule_key: str, cutoff_dt: datetime) -> Optional[pd.DataFrame]:
    """Load correction aggregates only from history strictly before test cutoff."""
    rows = fetch_all(
        """
        SELECT
            store_id,
            sap,
            schedule_key,
            COUNT(*) AS samples,
            AVG(correction_delta) AS avg_delta,
            AVG(correction_ratio) AS avg_ratio,
            STDDEV(correction_ratio) AS ratio_stddev,
            SUM(CASE WHEN was_removed THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS removal_rate,
            SUM(CASE WHEN promo_active THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS promo_rate
        FROM forecast_corrections
        WHERE route_number = %s
          AND schedule_key = %s
          AND is_holiday_week = FALSE
          AND submitted_at < %s
        GROUP BY store_id, sap, schedule_key
        HAVING COUNT(*) >= 1
        """,
        [route_number, schedule_key, cutoff_dt],
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
    order_wape_model = (order_abs_err_model / order_actual_total) if order_actual_total > 0 else 0.0
    order_wape_naive = (order_abs_err_naive / order_actual_total) if order_actual_total > 0 else 0.0

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
        "order_total_units_actual": order_actual_total,
        "order_total_units_model": order_pred_total,
        "order_total_units_naive": order_naive_total,
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
                "mean_sap_case_match_rate_model": float(g["sap_case_match_rate_model"].mean()),
                "mean_sap_case_match_rate_naive": float(g["sap_case_match_rate_naive"].mean()),
                "mean_sap_wape_model": sap_wape_model,
                "mean_sap_wape_naive": sap_wape_naive,
                "sap_wape_improvement_pct": ((sap_wape_naive - sap_wape_model) / sap_wape_naive * 100.0)
                if sap_wape_naive > 0
                else 0.0,
                "mean_order_total_wape_model": order_wape_model,
                "mean_order_total_wape_naive": order_wape_naive,
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


def _distinct_routes_from_history(since_days: int) -> List[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT route_number
        FROM orders_historical
        WHERE delivery_date >= (CURRENT_DATE - (%s || ' days')::interval)
        ORDER BY route_number
        """,
        [since_days],
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
        rows = fetch_all(
            """
            SELECT order_day, delivery_day, load_day
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
            out.append(
                {
                    "orderDay": int(od),
                    "deliveryDay": int(dd),
                    "loadDay": int(ld),
                }
            )
        return out
    except Exception:
        return []


def _temporal_corrected_orders_count(
    route_number: str,
    schedule_key: str,
    cutoff_dt: datetime,
    since_days: int,
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
              AND submitted_at <= %s
              AND delivery_date >= (%s::date - (%s || ' days')::interval)
            """,
            [route_number, str(schedule_key).lower(), cutoff_dt, cutoff_dt.date(), int(since_days)],
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
) -> Tuple[str, str, str]:
    """Resolve mode per fold to mirror production progression logic.

    Returns:
        (mode, training_scope, reason)
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

    if schedule_order_count < min_schedule_orders or corrected_order_count < min_corrected_orders:
        return (
            "copy_last_order",
            "schedule_only",
            (
                f"cold_start:schedule_orders={schedule_order_count}<{min_schedule_orders},"
                f"corrected_orders={corrected_order_count}<{min_corrected_orders}"
            ),
        )

    schedule_shape = _summarize_schedule_shape(order_cycles, schedule_key)
    if strict_schedule_validation and not bool(schedule_shape.get("is_valid")):
        return (
            "schedule_aware",
            "schedule_only",
            f"invalid_schedule_config:invalid_cycles={schedule_shape.get('invalid_cycles', 0)}",
        )
    if (
        not allow_store_on_ambiguous_schedule
        and bool(schedule_shape.get("same_order_to_multi_delivery"))
    ):
        return (
            "schedule_aware",
            "schedule_only",
            "ambiguous_schedule_mapping:same_order_to_multi_delivery",
        )

    if allow_store_context and _resolve_store_context_for_fold(train_orders_all, allow_store_context=True):
        return ("store_centric", "store_all_cycles", "adaptive_depth_ok")
    return ("schedule_aware", "schedule_only", "adaptive_depth_insufficient")


def run_backtest(
    routes: Iterable[str],
    since_days: int,
    min_train_orders: int,
    max_folds: int,
    schedule_keys: Optional[set[str]],
    temporal_corrections: bool,
    ignore_band_calibration: bool = False,
    store_centric_context: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: List[Dict[str, object]] = []
    source_rows: List[Dict[str, object]] = []

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
            for i in range(min_train_orders, len(sched_orders)):
                if max_folds > 0 and fold_counter >= max_folds:
                    break

                test_order = sched_orders[i]
                target_dt = _order_delivery_dt(test_order)
                if not target_dt:
                    continue

                candidate_train_orders = [
                    o for o in orders if (_order_delivery_dt(o) or datetime.min) < target_dt
                ]
                mode, training_scope, mode_reason = _resolve_mode_for_fold(
                    route_number=route_number,
                    schedule_key=schedule_key,
                    train_orders_all=candidate_train_orders,
                    order_cycles=order_cycles,
                    cutoff_dt=target_dt,
                    since_days=since_days,
                    allow_store_context=bool(store_centric_context),
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
                naive_line_map = _build_last_value_map(train_orders)
                actual_line_map = _order_to_line_map(test_order)

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
                    df_orders, default_gap = _inject_training_schedule_features(df_orders)
                    if temporal_corrections:
                        corrections_df = _load_temporal_corrections(route_number, schedule_key, target_dt)
                    else:
                        corrections_df = _load_corrections_from_postgres(route_number, schedule_key)

                    feat = _build_features(df_orders, corrections_df)
                    preds_df = _train_and_predict(
                        feat=feat,
                        target_date=target_dt,
                        days_until_next=default_gap,
                        active_promos=None,
                        band_scale=band_scale,
                    )

                    pred_line_map = _build_pred_line_map(preds_df)
                    key_source_map = _build_key_source_map(preds_df)
                    key_meta_map = _build_key_meta_map(preds_df)
                    pred_band_map = _build_pred_band_map(
                        preds_df,
                        center_offset_units=band_center_offset,
                        source_band_adjustments=source_band_adjustments,
                    )

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
        "--minTrainOrders",
        type=int,
        default=8,
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.routes:
        routes = [r.strip() for r in str(args.routes).split(",") if r.strip()]
    else:
        routes = _distinct_routes_from_history(args.sinceDays)
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
    )
    if folds_df.empty:
        raise SystemExit("[backtest] Completed with no folds evaluated.")

    scorecard_df = _summarize_scorecard(folds_df, since_days=int(args.sinceDays))

    out_dir = Path(args.outputDir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    folds_path = out_dir / f"walk_forward_folds_{stamp}.csv"
    scorecard_path = out_dir / f"walk_forward_scorecard_{stamp}.csv"
    sources_path = out_dir / f"walk_forward_sources_{stamp}.csv"
    folds_df.to_csv(folds_path, index=False)
    scorecard_df.to_csv(scorecard_path, index=False)
    if not source_df.empty:
        source_df.to_csv(sources_path, index=False)

    print(f"[backtest] Wrote folds CSV: {folds_path}")
    print(f"[backtest] Wrote scorecard CSV: {scorecard_path}")
    if not source_df.empty:
        print(f"[backtest] Wrote source breakdown CSV: {sources_path}")
    print(f"[backtest] Routes evaluated: {sorted(set(folds_df['route_number']))}")
    print(f"[backtest] Total folds: {len(folds_df)}")


if __name__ == "__main__":
    main()

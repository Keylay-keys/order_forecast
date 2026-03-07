#!/usr/bin/env python3
"""Local forecast-tuning harness for one route/delivery fold.

Purpose:
- Reuse the existing walk-forward/backtest logic
- Compare one generated forecast against the actual finalized order
- Make forecast-engine tuning repeatable before any production deploy

This script does not write forecasts to Firebase. It only reads PostgreSQL
history and emits local artifacts under logs/forecast_tuning/.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from .firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from .forecast_engine import _build_features, _should_zero_low_signal_slow_line
    from .walk_forward_backtest import (
        _aggregate_sap_totals,
        _build_key_meta_map,
        _build_key_source_map,
        _build_last_value_map,
        _build_pred_band_map,
        _build_pred_line_map,
        _compute_cold_start_p90_by_sap,
        _compute_fold_metrics,
        _inject_training_schedule_features,
        _load_band_calibration,
        _load_order_cycles_for_route,
        _load_source_band_calibration,
        _load_temporal_corrections,
        _order_delivery_dt,
        _order_to_line_map,
        _orders_to_dataframe,
        _resolve_mode_for_fold,
        _round_up_to_case,
        _train_and_predict,
    )
except ImportError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from firebase_loader import load_catalog_from_postgres, load_orders_from_postgres
    from forecast_engine import _build_features, _should_zero_low_signal_slow_line
    from walk_forward_backtest import (
        _aggregate_sap_totals,
        _build_key_meta_map,
        _build_key_source_map,
        _build_last_value_map,
        _build_pred_band_map,
        _build_pred_line_map,
        _compute_cold_start_p90_by_sap,
        _compute_fold_metrics,
        _inject_training_schedule_features,
        _load_band_calibration,
        _load_order_cycles_for_route,
        _load_source_band_calibration,
        _load_temporal_corrections,
        _order_delivery_dt,
        _order_to_line_map,
        _orders_to_dataframe,
        _resolve_mode_for_fold,
        _round_up_to_case,
        _train_and_predict,
    )


CompareRow = Dict[str, object]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one walk-forward-style forecast comparison")
    parser.add_argument("--route", required=True, help="Route number")
    parser.add_argument("--deliveryDate", required=True, help="Delivery date YYYY-MM-DD")
    parser.add_argument("--sinceDays", type=int, default=365, help="History lookback window")
    parser.add_argument(
        "--modeProfile",
        choices=["official", "schedule_only", "store_all_cycles"],
        default="official",
        help="How to choose training scope for the comparison fold",
    )
    parser.add_argument(
        "--outputDir",
        default="logs/forecast_tuning",
        help="Output directory root for summary artifacts",
    )
    return parser.parse_args()


def _serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value


def _find_test_order(route: str, delivery_date: str, since_days: int):
    orders = load_orders_from_postgres(route_number=route, since_days=since_days, status="finalized")
    orders = sorted(orders, key=lambda o: _order_delivery_dt(o) or datetime.min)
    for order in orders:
        if (order.expected_delivery_date or "") == delivery_date:
            return order, orders
    raise SystemExit(f"No finalized order found for route {route} delivery {delivery_date}")


def _choose_train_orders(
    route: str,
    schedule_key: str,
    delivery_dt: datetime,
    all_orders,
    mode_profile: str,
) -> Tuple[str, str, str, List]:
    candidate_train_orders = [o for o in all_orders if (_order_delivery_dt(o) or datetime.min) < delivery_dt]
    order_cycles = _load_order_cycles_for_route(route)

    if mode_profile == "official":
        mode, training_scope, mode_reason = _resolve_mode_for_fold(
            route_number=route,
            schedule_key=schedule_key,
            train_orders_all=candidate_train_orders,
            order_cycles=order_cycles,
            cutoff_dt=delivery_dt,
            since_days=365,
            allow_store_context=True,
        )
    elif mode_profile == "schedule_only":
        mode, training_scope, mode_reason = ("schedule_aware", "schedule_only", "forced_schedule_only")
    else:
        mode, training_scope, mode_reason = ("store_centric", "store_all_cycles", "forced_store_all_cycles")

    if training_scope == "schedule_only":
        train_orders = [
            o
            for o in candidate_train_orders
            if str(getattr(o, "schedule_key", "") or "").lower() == schedule_key
        ]
    else:
        train_orders = candidate_train_orders
    return mode, training_scope, mode_reason, train_orders


def _build_compare_rows(
    pred_line_map: Dict[Tuple[str, str], int],
    actual_line_map: Dict[Tuple[str, str], int],
    case_pack_by_sap: Dict[str, int],
) -> List[CompareRow]:
    pred_sap = _aggregate_sap_totals(pred_line_map)
    actual_sap = _aggregate_sap_totals(actual_line_map)
    rows: List[CompareRow] = []
    for sap in sorted(set(pred_sap) | set(actual_sap)):
        case_pack = case_pack_by_sap.get(sap)
        forecast_units = _round_up_to_case(int(pred_sap.get(sap, 0)), case_pack)
        actual_units = int(actual_sap.get(sap, 0))
        delta = forecast_units - actual_units
        if forecast_units == 0 and actual_units == 0:
            continue
        rows.append(
            {
                "sap": sap,
                "actual_units": actual_units,
                "forecast_units": forecast_units,
                "delta_units": delta,
                "case_pack": case_pack,
                "direction": "over" if delta > 0 else ("under" if delta < 0 else "match"),
                "zero_actual_overforecast": bool(actual_units == 0 and forecast_units > 0),
            }
        )
    rows.sort(key=lambda row: abs(int(row["delta_units"])), reverse=True)
    return rows


def _apply_live_slow_gate(preds_df, case_pack_by_sap):
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


def _write_csv(path: Path, rows: List[CompareRow]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = _parse_args()
    route = str(args.route).strip()
    delivery_date = str(args.deliveryDate).strip()

    test_order, all_orders = _find_test_order(route, delivery_date, int(args.sinceDays))
    delivery_dt = _order_delivery_dt(test_order)
    if not delivery_dt:
        raise SystemExit(f"Order {test_order.id} is missing a delivery date")
    schedule_key = str(test_order.schedule_key or "").lower()
    if not schedule_key:
        raise SystemExit(f"Order {test_order.id} is missing schedule_key")

    mode, training_scope, mode_reason, train_orders = _choose_train_orders(
        route=route,
        schedule_key=schedule_key,
        delivery_dt=delivery_dt,
        all_orders=all_orders,
        mode_profile=str(args.modeProfile),
    )
    if not train_orders:
        raise SystemExit("No training orders available for this comparison fold")

    case_pack_by_sap = {
        str(product.sap): int(product.case_pack or product.tray or 1)
        for product in load_catalog_from_postgres(route)
    }
    band_scale, band_center_offset = _load_band_calibration(
        route_number=route,
        schedule_key=schedule_key,
        ignore_band_calibration=False,
    )
    source_band_adjustments = _load_source_band_calibration(
        route_number=route,
        schedule_key=schedule_key,
        ignore_band_calibration=False,
    )

    df_orders = _orders_to_dataframe(train_orders)
    df_orders, default_gap = _inject_training_schedule_features(df_orders)
    corrections_df = _load_temporal_corrections(route, schedule_key, delivery_dt)
    feat = _build_features(df_orders, corrections_df)
    preds_df = _train_and_predict(
        feat=feat,
        target_date=delivery_dt,
        schedule_key=schedule_key,
        days_until_next=default_gap,
        active_promos=None,
        band_scale=band_scale,
    )
    preds_df = _apply_live_slow_gate(preds_df, case_pack_by_sap)

    pred_line_map = _build_pred_line_map(preds_df)
    actual_line_map = _order_to_line_map(test_order)
    naive_line_map = _build_last_value_map(train_orders)
    pred_band_map = _build_pred_band_map(
        preds_df,
        center_offset_units=band_center_offset,
        source_band_adjustments=source_band_adjustments,
    )
    key_source_map = _build_key_source_map(preds_df)
    key_meta_map = _build_key_meta_map(preds_df)
    cold_start_p90_by_sap = _compute_cold_start_p90_by_sap(df_orders, case_pack_by_sap)
    metrics = _compute_fold_metrics(
        route_number=route,
        schedule_key=schedule_key,
        target_dt=delivery_dt,
        fold_index=0,
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

    compare_rows = _build_compare_rows(pred_line_map, actual_line_map, case_pack_by_sap)
    over_rows = [row for row in compare_rows if int(row["delta_units"]) > 0]
    under_rows = [row for row in compare_rows if int(row["delta_units"]) < 0]
    zero_actual_over_rows = [row for row in compare_rows if bool(row["zero_actual_overforecast"])]

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.outputDir) / route / delivery_date / stamp
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "route_number": route,
        "delivery_date": delivery_date,
        "order_id": test_order.id,
        "order_date": test_order.order_date,
        "schedule_key": schedule_key,
        "mode_profile": args.modeProfile,
        "mode": mode,
        "training_scope": training_scope,
        "mode_reason": mode_reason,
        "train_orders": len(train_orders),
        "metrics": metrics,
        "top_over_forecasts": over_rows[:15],
        "top_under_forecasts": under_rows[:15],
        "top_zero_actual_over_forecasts": zero_actual_over_rows[:15],
    }
    with (out_dir / "summary.json").open("w") as handle:
        json.dump(summary, handle, indent=2, default=_serialize)
    _write_csv(out_dir / "compare.csv", compare_rows)

    print(json.dumps(
        {
            "route_number": route,
            "delivery_date": delivery_date,
            "order_id": test_order.id,
            "schedule_key": schedule_key,
            "mode_profile": args.modeProfile,
            "mode": mode,
            "training_scope": training_scope,
            "train_orders": len(train_orders),
            "order_total_units_actual": metrics.get("order_total_units_actual"),
            "order_total_units_model": metrics.get("order_total_units_model"),
            "order_total_abs_error_model": metrics.get("order_total_abs_error_model"),
            "order_total_wape_model": metrics.get("order_total_wape_model"),
            "sap_wape_model": metrics.get("sap_wape_model"),
            "segment_slow_line_wape_model": metrics.get("segment_slow_line_wape_model"),
            "segment_slow_over_rate_model": metrics.get("segment_slow_over_rate_model"),
            "output_dir": str(out_dir),
        },
        indent=2,
        default=_serialize,
    ))
    print("\nTop over-forecast lines:")
    for row in over_rows[:10]:
        print(
            f"  {row['sap']}: actual={row['actual_units']} "
            f"forecast={row['forecast_units']} delta=+{row['delta_units']}"
        )
    print("\nTop zero-actual over-forecasts:")
    for row in zero_actual_over_rows[:10]:
        print(
            f"  {row['sap']}: actual=0 forecast={row['forecast_units']} "
            f"delta=+{row['delta_units']}"
        )


if __name__ == "__main__":
    main()

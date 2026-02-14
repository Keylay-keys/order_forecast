#!/usr/bin/env python3
"""Weekly uncertainty-band calibration.

Goal:
- Keep p10/p90 coverage close to target (default 0.80) per route/schedule.
- Persist schedule-level band_scale used by forecast_engine.
- Log drift (coverage gap + under/over skew) for operational visibility.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Dict, Iterable, List, Optional

try:
    from .pg_utils import execute, fetch_all, fetch_one
    from .walk_forward_backtest import run_backtest
except ImportError:
    from pg_utils import execute, fetch_all, fetch_one
    from walk_forward_backtest import run_backtest


def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_band_calibration_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_band_calibration (
            route_number TEXT NOT NULL,
            schedule_key TEXT NOT NULL,
            interval_name TEXT NOT NULL DEFAULT 'p10_p90',
            band_scale REAL NOT NULL DEFAULT 1.0,
            target_coverage REAL NOT NULL DEFAULT 0.80,
            observed_coverage REAL,
            under_rate REAL,
            over_rate REAL,
            sample_lines INTEGER,
            fold_count INTEGER,
            notes TEXT,
            last_backtest_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (route_number, schedule_key, interval_name)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_band_center_calibration (
            route_number TEXT NOT NULL,
            schedule_key TEXT NOT NULL,
            interval_name TEXT NOT NULL DEFAULT 'p10_p90',
            center_offset_units REAL NOT NULL DEFAULT 0.0,
            observed_under_rate REAL,
            observed_over_rate REAL,
            sample_lines INTEGER,
            fold_count INTEGER,
            notes TEXT,
            last_backtest_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (route_number, schedule_key, interval_name)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_band_source_calibration (
            route_number TEXT NOT NULL,
            schedule_key TEXT NOT NULL,
            source TEXT NOT NULL,
            interval_name TEXT NOT NULL DEFAULT 'p10_p90',
            band_scale_mult REAL NOT NULL DEFAULT 1.0,
            center_offset_units REAL NOT NULL DEFAULT 0.0,
            target_coverage REAL NOT NULL DEFAULT 0.80,
            observed_coverage REAL,
            observed_under_rate REAL,
            observed_over_rate REAL,
            sample_lines INTEGER,
            fold_count INTEGER,
            notes TEXT,
            last_backtest_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (route_number, schedule_key, source, interval_name)
        )
        """
    )


def get_band_scale(route_number: str, schedule_key: str, interval_name: str = "p10_p90") -> float:
    row = fetch_one(
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
    if not row:
        return 1.0
    return float(row.get("band_scale", 1.0) or 1.0)


def get_band_center_offset(route_number: str, schedule_key: str, interval_name: str = "p10_p90") -> float:
    row = fetch_one(
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
    if not row:
        return 0.0
    return float(row.get("center_offset_units", 0.0) or 0.0)


def get_source_band_calibration(
    route_number: str,
    schedule_key: str,
    source: str,
    interval_name: str = "p10_p90",
) -> tuple[float, float]:
    row = fetch_one(
        """
        SELECT band_scale_mult, center_offset_units
        FROM forecast_band_source_calibration
        WHERE route_number = %s
          AND schedule_key = %s
          AND source = %s
          AND interval_name = %s
        LIMIT 1
        """,
        [route_number, schedule_key, source, interval_name],
    )
    if not row:
        return 1.0, 0.0
    return (
        float(row.get("band_scale_mult", 1.0) or 1.0),
        float(row.get("center_offset_units", 0.0) or 0.0),
    )


def _compute_scale_update(
    old_scale: float,
    observed_coverage: float,
    target_coverage: float,
    min_scale: float,
    max_scale: float,
    damping: float,
) -> float:
    obs = max(0.01, min(0.99, float(observed_coverage)))
    tgt = max(0.01, min(0.99, float(target_coverage)))

    z_obs = NormalDist().inv_cdf((1.0 + obs) / 2.0)
    z_tgt = NormalDist().inv_cdf((1.0 + tgt) / 2.0)
    z_obs = max(0.05, z_obs)

    raw_factor = z_tgt / z_obs
    damp = max(0.1, min(1.0, float(damping)))
    factor = raw_factor ** damp

    next_scale = float(old_scale) * factor
    return max(min_scale, min(max_scale, next_scale))


def _compute_center_offset_update(
    old_center_offset: float,
    under_rate: float,
    over_rate: float,
    avg_width_units: float,
    center_damping: float,
    max_step_units: float,
    max_abs_center: float,
) -> float:
    # Positive skew (over > under) means actuals sit above forecast interval; shift up.
    skew = float(over_rate) - float(under_rate)
    half_width = max(1.0, float(avg_width_units) / 2.0)
    damp = max(0.1, min(1.0, float(center_damping)))
    raw_step = skew * half_width * damp
    step = max(-float(max_step_units), min(float(max_step_units), raw_step))
    next_center = float(old_center_offset) + step
    return max(-float(max_abs_center), min(float(max_abs_center), next_center))


def _upsert_schedule_calibration(
    route_number: str,
    schedule_key: str,
    interval_name: str,
    band_scale: float,
    target_coverage: float,
    observed_coverage: float,
    under_rate: float,
    over_rate: float,
    sample_lines: int,
    fold_count: int,
    notes: str,
) -> None:
    execute(
        """
        INSERT INTO forecast_band_calibration (
            route_number, schedule_key, interval_name, band_scale,
            target_coverage, observed_coverage, under_rate, over_rate,
            sample_lines, fold_count, notes, last_backtest_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (route_number, schedule_key, interval_name)
        DO UPDATE SET
            band_scale = EXCLUDED.band_scale,
            target_coverage = EXCLUDED.target_coverage,
            observed_coverage = EXCLUDED.observed_coverage,
            under_rate = EXCLUDED.under_rate,
            over_rate = EXCLUDED.over_rate,
            sample_lines = EXCLUDED.sample_lines,
            fold_count = EXCLUDED.fold_count,
            notes = EXCLUDED.notes,
            last_backtest_at = EXCLUDED.last_backtest_at,
            updated_at = EXCLUDED.updated_at
        """,
        [
            route_number,
            schedule_key,
            interval_name,
            float(band_scale),
            float(target_coverage),
            float(observed_coverage),
            float(under_rate),
            float(over_rate),
            int(sample_lines),
            int(fold_count),
            notes,
        ],
    )


def _upsert_schedule_center_calibration(
    route_number: str,
    schedule_key: str,
    interval_name: str,
    center_offset_units: float,
    under_rate: float,
    over_rate: float,
    sample_lines: int,
    fold_count: int,
    notes: str,
) -> None:
    execute(
        """
        INSERT INTO forecast_band_center_calibration (
            route_number, schedule_key, interval_name, center_offset_units,
            observed_under_rate, observed_over_rate, sample_lines, fold_count,
            notes, last_backtest_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (route_number, schedule_key, interval_name)
        DO UPDATE SET
            center_offset_units = EXCLUDED.center_offset_units,
            observed_under_rate = EXCLUDED.observed_under_rate,
            observed_over_rate = EXCLUDED.observed_over_rate,
            sample_lines = EXCLUDED.sample_lines,
            fold_count = EXCLUDED.fold_count,
            notes = EXCLUDED.notes,
            last_backtest_at = EXCLUDED.last_backtest_at,
            updated_at = EXCLUDED.updated_at
        """,
        [
            route_number,
            schedule_key,
            interval_name,
            float(center_offset_units),
            float(under_rate),
            float(over_rate),
            int(sample_lines),
            int(fold_count),
            notes,
        ],
    )


def _upsert_source_calibration(
    route_number: str,
    schedule_key: str,
    source: str,
    interval_name: str,
    band_scale_mult: float,
    center_offset_units: float,
    target_coverage: float,
    observed_coverage: float,
    under_rate: float,
    over_rate: float,
    sample_lines: int,
    fold_count: int,
    notes: str,
) -> None:
    execute(
        """
        INSERT INTO forecast_band_source_calibration (
            route_number, schedule_key, source, interval_name,
            band_scale_mult, center_offset_units,
            target_coverage, observed_coverage, observed_under_rate, observed_over_rate,
            sample_lines, fold_count, notes, last_backtest_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (route_number, schedule_key, source, interval_name)
        DO UPDATE SET
            band_scale_mult = EXCLUDED.band_scale_mult,
            center_offset_units = EXCLUDED.center_offset_units,
            target_coverage = EXCLUDED.target_coverage,
            observed_coverage = EXCLUDED.observed_coverage,
            observed_under_rate = EXCLUDED.observed_under_rate,
            observed_over_rate = EXCLUDED.observed_over_rate,
            sample_lines = EXCLUDED.sample_lines,
            fold_count = EXCLUDED.fold_count,
            notes = EXCLUDED.notes,
            last_backtest_at = EXCLUDED.last_backtest_at,
            updated_at = EXCLUDED.updated_at
        """,
        [
            route_number,
            schedule_key,
            source,
            interval_name,
            float(band_scale_mult),
            float(center_offset_units),
            float(target_coverage),
            float(observed_coverage),
            float(under_rate),
            float(over_rate),
            int(sample_lines),
            int(fold_count),
            notes,
        ],
    )


def calibrate_route_band_scales(
    route_number: str,
    since_days: int = 365,
    min_train_orders: int = 8,
    max_folds: int = 0,
    target_coverage: float = 0.80,
    min_lines: int = 200,
    interval_name: str = "p10_p90",
    min_scale: float = 0.5,
    max_scale: float = 8.0,
    damping: float = 1.0,
    center_damping: float = 1.0,
    max_center_step_units: float = 12.0,
    max_center_abs_units: float = 64.0,
    source_min_lines: int = 100,
    source_min_scale: float = 0.5,
    source_max_scale: float = 4.0,
    source_damping: float = 1.0,
    source_center_damping: float = 1.0,
    source_max_center_step_units: float = 8.0,
) -> dict:
    ensure_band_calibration_table()

    backtest_result = run_backtest(
        routes=[route_number],
        since_days=since_days,
        min_train_orders=min_train_orders,
        max_folds=max_folds,
        schedule_keys=None,
        temporal_corrections=True,
        ignore_band_calibration=False,
    )
    # walk_forward_backtest.run_backtest now returns (folds_df, source_df).
    # Keep compatibility with older single-DataFrame return shape.
    if isinstance(backtest_result, tuple):
        folds_df = backtest_result[0]
        source_df = backtest_result[1] if len(backtest_result) > 1 else None
    else:
        folds_df = backtest_result
        source_df = None
    if folds_df.empty:
        return {"status": "no_data", "route": route_number, "updated": 0, "schedules": []}

    updated = 0
    schedules: List[dict] = []

    for schedule_key, g in folds_df.groupby("schedule_key"):
        lines = int(g["line_items_eval_count"].sum())
        folds = int(len(g))
        observed = float(g["line_band_coverage_10_90"].mean())
        under_rate = float(g["line_band_under_rate_10_90"].mean())
        over_rate = float(g["line_band_over_rate_10_90"].mean())
        drift = float(target_coverage - observed)
        skew = float(over_rate - under_rate)
        avg_width = float(g["line_band_avg_width_units_10_90"].mean())
        old_scale = get_band_scale(route_number, schedule_key, interval_name=interval_name)
        old_center = get_band_center_offset(route_number, schedule_key, interval_name=interval_name)

        if lines < int(min_lines):
            schedules.append(
                {
                    "schedule_key": schedule_key,
                    "status": "insufficient_lines",
                    "sample_lines": lines,
                    "fold_count": folds,
                    "observed_coverage": observed,
                    "old_scale": old_scale,
                    "new_scale": old_scale,
                    "old_center_offset_units": old_center,
                    "new_center_offset_units": old_center,
                    "drift": drift,
                    "skew": skew,
                }
            )
            continue

        new_scale = _compute_scale_update(
            old_scale=old_scale,
            observed_coverage=observed,
            target_coverage=target_coverage,
            min_scale=min_scale,
            max_scale=max_scale,
            damping=damping,
        )
        new_center = _compute_center_offset_update(
            old_center_offset=old_center,
            under_rate=under_rate,
            over_rate=over_rate,
            avg_width_units=avg_width,
            center_damping=center_damping,
            max_step_units=max_center_step_units,
            max_abs_center=max_center_abs_units,
        )
        notes = (
            f"coverage_drift={drift:+.4f}; "
            f"under_rate={under_rate:.4f}; over_rate={over_rate:.4f}; skew(over-under)={skew:+.4f}; "
            f"avg_width={avg_width:.4f}; center_offset={new_center:+.4f}"
        )
        _upsert_schedule_calibration(
            route_number=route_number,
            schedule_key=str(schedule_key),
            interval_name=interval_name,
            band_scale=new_scale,
            target_coverage=target_coverage,
            observed_coverage=observed,
            under_rate=under_rate,
            over_rate=over_rate,
            sample_lines=lines,
            fold_count=folds,
            notes=notes,
        )
        _upsert_schedule_center_calibration(
            route_number=route_number,
            schedule_key=str(schedule_key),
            interval_name=interval_name,
            center_offset_units=new_center,
            under_rate=under_rate,
            over_rate=over_rate,
            sample_lines=lines,
            fold_count=folds,
            notes=notes,
        )
        updated += 1
        schedules.append(
            {
                "schedule_key": schedule_key,
                "status": "updated",
                "sample_lines": lines,
                "fold_count": folds,
                "observed_coverage": observed,
                "target_coverage": target_coverage,
                "old_scale": old_scale,
                "new_scale": new_scale,
                "old_center_offset_units": old_center,
                "new_center_offset_units": new_center,
                "drift": drift,
                "skew": skew,
                "avg_width_units": avg_width,
            }
        )

    # Source-aware calibration (branch/source specific): gbr vs slow_intermittent.
    if source_df is not None and not source_df.empty:
        for (schedule_key, source), sg in source_df.groupby(["schedule_key", "source"]):
            source_key = str(source or "").strip()
            if not source_key or source_key == "missing_pred":
                continue
            sample_lines = int(sg["line_count"].sum())
            if sample_lines < int(source_min_lines):
                continue

            # Weighted means across folds by line_count.
            w = sg["line_count"].astype(float)
            wsum = float(w.sum()) if float(w.sum()) > 0 else 1.0
            observed = float((sg["line_band_coverage_10_90"] * w).sum() / wsum)
            under_rate = float((sg["line_band_under_rate_10_90"] * w).sum() / wsum)
            over_rate = float((sg["line_band_over_rate_10_90"] * w).sum() / wsum)
            avg_width = float((sg["line_band_avg_width_units_10_90"] * w).sum() / wsum)
            folds = int(sg["fold_index"].nunique()) if "fold_index" in sg.columns else 0
            drift = float(target_coverage - observed)
            skew = float(over_rate - under_rate)

            old_scale, old_center = get_source_band_calibration(
                route_number=route_number,
                schedule_key=str(schedule_key),
                source=source_key,
                interval_name=interval_name,
            )
            new_scale = _compute_scale_update(
                old_scale=old_scale,
                observed_coverage=observed,
                target_coverage=target_coverage,
                min_scale=source_min_scale,
                max_scale=source_max_scale,
                damping=source_damping,
            )
            new_center = _compute_center_offset_update(
                old_center_offset=old_center,
                under_rate=under_rate,
                over_rate=over_rate,
                avg_width_units=avg_width,
                center_damping=source_center_damping,
                max_step_units=source_max_center_step_units,
                max_abs_center=max_center_abs_units,
            )
            notes = (
                f"source={source_key}; coverage_drift={drift:+.4f}; "
                f"under_rate={under_rate:.4f}; over_rate={over_rate:.4f}; skew(over-under)={skew:+.4f}; "
                f"avg_width={avg_width:.4f}; center_offset={new_center:+.4f}"
            )
            _upsert_source_calibration(
                route_number=route_number,
                schedule_key=str(schedule_key),
                source=source_key,
                interval_name=interval_name,
                band_scale_mult=new_scale,
                center_offset_units=new_center,
                target_coverage=target_coverage,
                observed_coverage=observed,
                under_rate=under_rate,
                over_rate=over_rate,
                sample_lines=sample_lines,
                fold_count=folds,
                notes=notes,
            )

    return {"status": "updated", "route": route_number, "updated": updated, "schedules": schedules}


def calibrate_route_if_due(
    route_number: str,
    min_days_between_runs: int = 7,
    **kwargs,
) -> dict:
    ensure_band_calibration_table()
    interval_name = str(kwargs.get("interval_name", "p10_p90"))
    row = fetch_one(
        """
        SELECT MAX(updated_at) AS last_updated
        FROM forecast_band_calibration
        WHERE route_number = %s
          AND interval_name = %s
        """,
        [route_number, interval_name],
    )

    last_updated = row.get("last_updated") if row else None
    now_utc = datetime.now(timezone.utc)
    if last_updated:
        try:
            delta = now_utc - last_updated.astimezone(timezone.utc)
            if delta < timedelta(days=int(min_days_between_runs)):
                return {
                    "status": "skipped_recent",
                    "route": route_number,
                    "last_updated": last_updated.isoformat(),
                    "days_since_last": float(delta.total_seconds() / 86400.0),
                }
        except Exception:
            pass

    return calibrate_route_band_scales(route_number=route_number, **kwargs)


def _parse_routes_arg(routes_arg: Optional[str]) -> List[str]:
    if not routes_arg:
        rows = fetch_all(
            """
            SELECT route_number
            FROM routes_synced
            WHERE sync_status = 'ready'
            ORDER BY route_number
            """
        )
        return [str(r["route_number"]) for r in rows if r.get("route_number")]
    return [r.strip() for r in str(routes_arg).split(",") if r.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly band calibration")
    parser.add_argument("--routes", help="Comma-separated route list (default: routes_synced ready)")
    parser.add_argument("--sinceDays", type=int, default=365)
    parser.add_argument("--minTrainOrders", type=int, default=8)
    parser.add_argument("--maxFolds", type=int, default=0)
    parser.add_argument("--targetCoverage", type=float, default=0.80)
    parser.add_argument("--minLines", type=int, default=200)
    parser.add_argument("--intervalName", default="p10_p90")
    parser.add_argument("--minScale", type=float, default=0.5)
    parser.add_argument("--maxScale", type=float, default=8.0)
    parser.add_argument("--damping", type=float, default=1.0)
    parser.add_argument("--centerDamping", type=float, default=1.0)
    parser.add_argument("--maxCenterStepUnits", type=float, default=12.0)
    parser.add_argument("--maxCenterAbsUnits", type=float, default=64.0)
    parser.add_argument("--sourceMinLines", type=int, default=100)
    parser.add_argument("--sourceMinScale", type=float, default=0.5)
    parser.add_argument("--sourceMaxScale", type=float, default=4.0)
    parser.add_argument("--sourceDamping", type=float, default=1.0)
    parser.add_argument("--sourceCenterDamping", type=float, default=1.0)
    parser.add_argument("--sourceMaxCenterStepUnits", type=float, default=8.0)
    parser.add_argument("--weeklyDays", type=int, default=7)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    routes = _parse_routes_arg(args.routes)
    if not routes:
        raise SystemExit("No routes to calibrate.")

    _log(f"Band calibration starting for {len(routes)} route(s)")
    total_updated = 0
    for route in routes:
        if args.force:
            result = calibrate_route_band_scales(
                route_number=route,
                since_days=args.sinceDays,
                min_train_orders=args.minTrainOrders,
                max_folds=args.maxFolds,
                target_coverage=args.targetCoverage,
                min_lines=args.minLines,
                interval_name=args.intervalName,
                min_scale=args.minScale,
                max_scale=args.maxScale,
                damping=args.damping,
                center_damping=args.centerDamping,
                max_center_step_units=args.maxCenterStepUnits,
                max_center_abs_units=args.maxCenterAbsUnits,
                source_min_lines=args.sourceMinLines,
                source_min_scale=args.sourceMinScale,
                source_max_scale=args.sourceMaxScale,
                source_damping=args.sourceDamping,
                source_center_damping=args.sourceCenterDamping,
                source_max_center_step_units=args.sourceMaxCenterStepUnits,
            )
        else:
            result = calibrate_route_if_due(
                route_number=route,
                min_days_between_runs=args.weeklyDays,
                since_days=args.sinceDays,
                min_train_orders=args.minTrainOrders,
                max_folds=args.maxFolds,
                target_coverage=args.targetCoverage,
                min_lines=args.minLines,
                interval_name=args.intervalName,
                min_scale=args.minScale,
                max_scale=args.maxScale,
                damping=args.damping,
                center_damping=args.centerDamping,
                max_center_step_units=args.maxCenterStepUnits,
                max_center_abs_units=args.maxCenterAbsUnits,
                source_min_lines=args.sourceMinLines,
                source_min_scale=args.sourceMinScale,
                source_max_scale=args.sourceMaxScale,
                source_damping=args.sourceDamping,
                source_center_damping=args.sourceCenterDamping,
                source_max_center_step_units=args.sourceMaxCenterStepUnits,
            )

        status = result.get("status")
        if status == "skipped_recent":
            _log(
                f"Route {route}: skipped (recent run {result.get('days_since_last', 0):.2f} days ago)"
            )
            continue
        if status == "no_data":
            _log(f"Route {route}: no backtest folds available; skipped")
            continue

        updated = int(result.get("updated", 0) or 0)
        total_updated += updated
        _log(f"Route {route}: updated {updated} schedule calibration row(s)")
        for sched in result.get("schedules", []):
            _log(
                "  "
                f"{sched.get('schedule_key')}: "
                f"coverage={sched.get('observed_coverage', 0.0):.4f} "
                f"target={sched.get('target_coverage', args.targetCoverage):.4f} "
                f"scale {sched.get('old_scale', 1.0):.3f} -> {sched.get('new_scale', 1.0):.3f} "
                f"center {sched.get('old_center_offset_units', 0.0):+.2f}u -> {sched.get('new_center_offset_units', 0.0):+.2f}u "
                f"drift={sched.get('drift', 0.0):+.4f} "
                f"skew={sched.get('skew', 0.0):+.4f} "
                f"lines={sched.get('sample_lines', 0)}"
            )

    _log(f"Band calibration complete. Total updated schedules: {total_updated}")


if __name__ == "__main__":
    main()

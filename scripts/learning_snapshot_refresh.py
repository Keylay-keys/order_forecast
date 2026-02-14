#!/usr/bin/env python3
"""Learning snapshot refresh for web forecast card.

Purpose:
- Run walk-forward backtest for due routes on a weekly cadence.
- Persist fresh CSV snapshots (folds + scorecard + source breakdown).
- Track per-route refresh status in PostgreSQL so cadence is deterministic.

This script is safe to call frequently; it skips routes that are not due unless forced.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

try:
    from .pg_utils import execute, fetch_all, fetch_one
    from .walk_forward_backtest import run_backtest, _summarize_scorecard
except ImportError:
    from pg_utils import execute, fetch_all, fetch_one
    from walk_forward_backtest import run_backtest, _summarize_scorecard


def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_learning_refresh_state_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_learning_refresh_state (
            route_number TEXT PRIMARY KEY,
            last_refreshed_at TIMESTAMPTZ,
            last_status TEXT NOT NULL DEFAULT 'never',
            last_scorecard_file TEXT,
            last_folds_file TEXT,
            last_sources_file TEXT,
            last_fold_count INTEGER,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _to_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    return None


def _discover_ready_routes() -> list[str]:
    rows = fetch_all(
        """
        SELECT route_number
        FROM routes_synced
        WHERE sync_status = 'ready'
        ORDER BY route_number
        """
    )
    return [str(r.get("route_number")) for r in rows if r.get("route_number")]


def _upsert_refresh_state(
    route_number: str,
    *,
    last_refreshed_at: Optional[datetime],
    last_status: str,
    last_scorecard_file: Optional[str],
    last_folds_file: Optional[str],
    last_sources_file: Optional[str],
    last_fold_count: int,
    last_error: Optional[str],
) -> None:
    execute(
        """
        INSERT INTO forecast_learning_refresh_state (
            route_number,
            last_refreshed_at,
            last_status,
            last_scorecard_file,
            last_folds_file,
            last_sources_file,
            last_fold_count,
            last_error,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (route_number)
        DO UPDATE SET
            last_refreshed_at = EXCLUDED.last_refreshed_at,
            last_status = EXCLUDED.last_status,
            last_scorecard_file = EXCLUDED.last_scorecard_file,
            last_folds_file = EXCLUDED.last_folds_file,
            last_sources_file = EXCLUDED.last_sources_file,
            last_fold_count = EXCLUDED.last_fold_count,
            last_error = EXCLUDED.last_error,
            updated_at = EXCLUDED.updated_at
        """,
        [
            str(route_number),
            last_refreshed_at,
            str(last_status),
            last_scorecard_file,
            last_folds_file,
            last_sources_file,
            int(last_fold_count),
            last_error,
        ],
    )


def _resolve_due_routes(
    routes: Sequence[str],
    *,
    force_routes: set[str],
    min_days_between_runs: int,
) -> list[str]:
    if not routes:
        return []

    rows = fetch_all(
        """
        SELECT route_number, last_refreshed_at
        FROM forecast_learning_refresh_state
        WHERE route_number = ANY(%s)
        """,
        [list(routes)],
    )
    by_route = {str(r.get("route_number")): _to_dt(r.get("last_refreshed_at")) for r in rows}

    now = datetime.now(timezone.utc)
    due: list[str] = []
    for route in routes:
        r = str(route)
        if r in force_routes:
            due.append(r)
            continue
        last = by_route.get(r)
        if last is None:
            due.append(r)
            continue
        age = now - last.astimezone(timezone.utc)
        if age >= timedelta(days=max(1, int(min_days_between_runs))):
            due.append(r)
    return sorted(set(due))


def refresh_learning_snapshots(
    *,
    routes: Optional[Sequence[str]] = None,
    force_routes: Optional[Iterable[str]] = None,
    since_days: int = 365,
    min_train_orders: int = 8,
    max_folds: int = 24,
    min_days_between_runs: int = 7,
    output_dir: str = "logs/backtests",
    temporal_corrections: bool = True,
    ignore_band_calibration: bool = False,
    store_centric_context: bool = True,
) -> dict:
    """Refresh learning snapshots for due routes.

    Returns a status dict with route-level results and output paths.
    """
    ensure_learning_refresh_state_table()

    route_list = [str(r) for r in (routes or _discover_ready_routes()) if str(r).strip()]
    if not route_list:
        return {"status": "no_routes", "routes_due": [], "routes_refreshed": [], "routes_no_data": []}

    force_set = {str(r) for r in (force_routes or [])}
    due_routes = _resolve_due_routes(
        route_list,
        force_routes=force_set,
        min_days_between_runs=int(min_days_between_runs),
    )
    if not due_routes:
        return {
            "status": "skipped_not_due",
            "routes_due": [],
            "routes_refreshed": [],
            "routes_no_data": [],
        }

    _log(f"[learning-refresh] Due routes: {', '.join(due_routes)}")

    now = datetime.now(timezone.utc)
    try:
        folds_df, source_df = run_backtest(
            routes=due_routes,
            since_days=int(since_days),
            min_train_orders=int(min_train_orders),
            max_folds=int(max_folds),
            schedule_keys=None,
            temporal_corrections=bool(temporal_corrections),
            ignore_band_calibration=bool(ignore_band_calibration),
            store_centric_context=bool(store_centric_context),
        )
    except Exception as e:
        err = str(e)
        for route in due_routes:
            _upsert_refresh_state(
                route,
                last_refreshed_at=now,
                last_status="error",
                last_scorecard_file=None,
                last_folds_file=None,
                last_sources_file=None,
                last_fold_count=0,
                last_error=err,
            )
        return {
            "status": "error",
            "error": err,
            "routes_due": due_routes,
            "routes_refreshed": [],
            "routes_no_data": due_routes,
        }

    if folds_df.empty:
        for route in due_routes:
            _upsert_refresh_state(
                route,
                last_refreshed_at=now,
                last_status="no_data",
                last_scorecard_file=None,
                last_folds_file=None,
                last_sources_file=None,
                last_fold_count=0,
                last_error=None,
            )
        return {
            "status": "no_data",
            "routes_due": due_routes,
            "routes_refreshed": [],
            "routes_no_data": due_routes,
        }

    scorecard_df = _summarize_scorecard(folds_df, since_days=int(since_days))

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    folds_path = out_dir / f"walk_forward_folds_{stamp}.csv"
    scorecard_path = out_dir / f"walk_forward_scorecard_{stamp}.csv"
    sources_path = out_dir / f"walk_forward_sources_{stamp}.csv"

    folds_df.to_csv(folds_path, index=False)
    scorecard_df.to_csv(scorecard_path, index=False)
    if not source_df.empty:
        source_df.to_csv(sources_path, index=False)

    route_counts = (
        folds_df.groupby("route_number").size().to_dict()
        if "route_number" in folds_df.columns
        else {}
    )

    refreshed: list[str] = []
    no_data: list[str] = []
    for route in due_routes:
        fold_count = int(route_counts.get(str(route), 0) or 0)
        if fold_count > 0:
            refreshed.append(str(route))
            _upsert_refresh_state(
                str(route),
                last_refreshed_at=now,
                last_status="refreshed",
                last_scorecard_file=str(scorecard_path),
                last_folds_file=str(folds_path),
                last_sources_file=str(sources_path) if not source_df.empty else None,
                last_fold_count=fold_count,
                last_error=None,
            )
        else:
            no_data.append(str(route))
            _upsert_refresh_state(
                str(route),
                last_refreshed_at=now,
                last_status="no_data",
                last_scorecard_file=str(scorecard_path),
                last_folds_file=str(folds_path),
                last_sources_file=str(sources_path) if not source_df.empty else None,
                last_fold_count=0,
                last_error=None,
            )

    return {
        "status": "ok",
        "routes_due": due_routes,
        "routes_refreshed": refreshed,
        "routes_no_data": no_data,
        "scorecard_path": str(scorecard_path),
        "folds_path": str(folds_path),
        "sources_path": str(sources_path) if not source_df.empty else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh weekly learning snapshots")
    parser.add_argument("--routes", help="Optional comma-separated route numbers")
    parser.add_argument("--forceRoutes", help="Optional comma-separated routes to force refresh")
    parser.add_argument("--forceAll", action="store_true", help="Force all selected routes")
    parser.add_argument("--sinceDays", type=int, default=365)
    parser.add_argument("--minTrainOrders", type=int, default=8)
    parser.add_argument("--maxFolds", type=int, default=24)
    parser.add_argument("--minDaysBetweenRuns", type=int, default=7)
    parser.add_argument("--outputDir", default="logs/backtests")
    parser.add_argument("--disableTemporalCorrections", action="store_true")
    parser.add_argument("--ignoreBandCalibration", action="store_true")
    parser.add_argument("--disableStoreCentricContext", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    routes = None
    if args.routes:
        routes = [r.strip() for r in str(args.routes).split(",") if r.strip()]

    force_routes: set[str] = set()
    if args.forceAll and routes:
        force_routes.update(routes)
    if args.forceRoutes:
        force_routes.update({r.strip() for r in str(args.forceRoutes).split(",") if r.strip()})

    result = refresh_learning_snapshots(
        routes=routes,
        force_routes=force_routes,
        since_days=int(args.sinceDays),
        min_train_orders=int(args.minTrainOrders),
        max_folds=int(args.maxFolds),
        min_days_between_runs=int(args.minDaysBetweenRuns),
        output_dir=str(args.outputDir),
        temporal_corrections=not bool(args.disableTemporalCorrections),
        ignore_band_calibration=bool(args.ignoreBandCalibration),
        store_centric_context=not bool(args.disableStoreCentricContext),
    )
    _log(f"[learning-refresh] Result: {result}")
    if result.get("status") == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""Release gate using latest walk-forward scorecard CSV.

Blocks deployment when route/schedule quality falls below minimum thresholds.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd


def _latest_scorecard(backtest_dir: Path) -> Path | None:
    files = sorted(backtest_dir.glob("walk_forward_scorecard_*.csv"), key=lambda p: p.name)
    return files[-1] if files else None


def _check_gate(
    df: pd.DataFrame,
    *,
    min_folds: int,
    min_coverage: float,
    min_order_wape_improvement_pct: float,
    max_edit_rate_regression_pct: float,
) -> pd.DataFrame:
    checks = df.copy()
    checks["fail_reasons"] = ""

    def append_reason(mask: pd.Series, reason: str) -> None:
        checks.loc[mask, "fail_reasons"] = checks.loc[mask, "fail_reasons"].apply(
            lambda s: f"{s},{reason}" if s else reason
        )

    append_reason(checks["fold_count"] < min_folds, f"fold_count<{min_folds}")
    append_reason(
        checks["mean_line_band_coverage_10_90"] < min_coverage,
        f"coverage<{min_coverage:.2f}",
    )
    append_reason(
        checks["order_wape_improvement_pct"] < min_order_wape_improvement_pct,
        f"order_wape_improvement<{min_order_wape_improvement_pct:.2f}%",
    )

    allowed_edit_rate = checks["mean_line_edit_rate_proxy_naive"] * (
        1.0 + max_edit_rate_regression_pct / 100.0
    )
    append_reason(
        checks["mean_line_edit_rate_proxy_model"] > allowed_edit_rate,
        f"edit_rate_regression>{max_edit_rate_regression_pct:.1f}%",
    )

    return checks[checks["fail_reasons"] != ""].copy()


def main() -> None:
    parser = argparse.ArgumentParser(description="Release gate from walk-forward scorecard")
    parser.add_argument(
        "--backtestDir",
        default="logs/backtests",
        help="Directory containing walk_forward_scorecard_*.csv",
    )
    parser.add_argument("--route", help="Optional route filter")
    parser.add_argument("--scheduleKey", help="Optional schedule filter")
    parser.add_argument("--minFolds", type=int, default=3)
    parser.add_argument("--minCoverage10_90", type=float, default=0.65)
    parser.add_argument("--minOrderWapeImprovementPct", type=float, default=0.0)
    parser.add_argument("--maxEditRateRegressionPct", type=float, default=20.0)
    parser.add_argument(
        "--allowNoScorecard",
        action="store_true",
        help="Do not fail when scorecard file is missing",
    )
    args = parser.parse_args()

    backtest_dir = Path(args.backtestDir)
    scorecard_path = _latest_scorecard(backtest_dir)
    if scorecard_path is None:
        msg = f"[release-gate] No scorecard found in {backtest_dir}"
        if args.allowNoScorecard:
            print(f"{msg} (allowed)")
            return
        raise SystemExit(msg)

    df = pd.read_csv(scorecard_path)
    if "route_number" not in df.columns:
        raise SystemExit(f"[release-gate] Invalid scorecard format: {scorecard_path}")
    df["route_number"] = df["route_number"].astype(str)
    if args.route:
        df = df[df["route_number"] == str(args.route)]
    if args.scheduleKey:
        df = df[df["schedule_key"].astype(str).str.lower() == str(args.scheduleKey).lower()]
    if df.empty:
        msg = "[release-gate] No scorecard rows after filters"
        if args.allowNoScorecard:
            print(f"{msg} (allowed)")
            return
        raise SystemExit(msg)

    failed = _check_gate(
        df,
        min_folds=int(args.minFolds),
        min_coverage=float(args.minCoverage10_90),
        min_order_wape_improvement_pct=float(args.minOrderWapeImprovementPct),
        max_edit_rate_regression_pct=float(args.maxEditRateRegressionPct),
    )

    print(f"[release-gate] Scorecard file: {scorecard_path}")
    print(f"[release-gate] Rows evaluated: {len(df)}")

    if failed.empty:
        print("[release-gate] PASS")
        return

    cols = [
        "route_number",
        "schedule_key",
        "fold_count",
        "mean_line_band_coverage_10_90",
        "order_wape_improvement_pct",
        "mean_line_edit_rate_proxy_model",
        "mean_line_edit_rate_proxy_naive",
        "fail_reasons",
    ]
    print("[release-gate] FAIL")
    print(failed[cols].to_string(index=False))
    raise SystemExit(2)


if __name__ == "__main__":
    main()


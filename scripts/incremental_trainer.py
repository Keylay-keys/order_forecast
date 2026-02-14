"""Incremental trainer: warm-start retraining after each finalized order.

Inputs:
  - Fresh orders CSV (2-row header format) and stock CSV (same shape) for the route
  - Optional corrections CSV from feedback_collector aggregates
  - Optional promo PDFs

Outputs:
  - Trained GradientBoostingRegressor model (in-memory)
  - Metrics (MAE/RMSE vs. naive baseline)

This is a thin wrapper around baseline_model.train_baseline with warm-start
support scaffolded (sklearn GB does not truly warm-start; in practice we retrain).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from . import baseline_model as bm


def train_incremental(
    orders_csv: Path,
    stock_csv: Path,
    promo_globs: list[str] | None = None,
    corrections_csv: Path | None = None,
) -> None:
    # Resolve promo paths
    promo_paths = []
    if promo_globs:
        for pattern in promo_globs:
            promo_paths.extend(Path.cwd().glob(pattern))
    else:
        promo_paths = list(Path("promos/raw").glob("*.pdf"))

    df = bm.build_modeling_dataframe(
        orders_csv,
        stock_csv,
        promo_paths,
        corrections_csv,
    )
    print("Modeling rows:", len(df))
    bm.train_baseline(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incremental trainer (warm-start retrain after new order)")
    parser.add_argument("--orders", required=True, help="Path to orders CSV (2-row header)")
    parser.add_argument("--stock", required=True, help="Path to stock CSV (2-row header)")
    parser.add_argument("--corrections", help="Path to corrections aggregate CSV (optional)")
    parser.add_argument("--promos", nargs="*", help="Glob pattern(s) for promo PDFs")
    args = parser.parse_args()

    train_incremental(
        orders_csv=Path(args.orders),
        stock_csv=Path(args.stock),
        promo_globs=args.promos,
        corrections_csv=Path(args.corrections) if args.corrections else None,
    )

"""Baseline forecasting pipeline for Mission order data."""

from __future__ import annotations

from pathlib import Path
import argparse
from typing import Iterable, List

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

import mission_orders as mo
import promotion_parser as pp


def _add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("delivery_date").copy()
    df["lag_1"] = df["cases"].shift(1)
    df["lag_2"] = df["cases"].shift(2)
    df["rolling_mean_4"] = df["cases"].rolling(window=4, min_periods=1).mean().shift(1)
    return df


def _collect_promotions(paths: Iterable[Path]) -> pd.DataFrame:
    pdf_paths: List[Path] = [path for path in paths if path.suffix.lower() == ".pdf"]
    if not pdf_paths:
        return pd.DataFrame()
    return pp.extract_promotions_from_pdfs([str(path) for path in pdf_paths])


def build_modeling_dataframe(
    order_csv: Path,
    stock_csv: Path,
    promo_paths: Iterable[Path],
) -> pd.DataFrame:
    orders = mo.load_order_history(str(order_csv))
    stock = mo.load_store_stock(str(stock_csv))
    orders = mo.annotate_with_stock(orders, stock)

    promotions = _collect_promotions(promo_paths)
    orders = mo.annotate_with_promotions(orders, promotions)

    orders = orders[orders["store"] != "Order"].copy()
    orders = orders[orders["active"] == 1].copy()

    orders["promo_active"] = orders["promo_active"].fillna(False).astype(int)
    orders["delivery_date"] = pd.to_datetime(orders["delivery_date"], errors="coerce")
    orders = orders.dropna(subset=["delivery_date"])  # guard against unexpected NaT

    orders["delivery_dow"] = orders["delivery_date"].dt.weekday
    orders["delivery_month"] = orders["delivery_date"].dt.month
    orders["is_monday_delivery"] = (orders["delivery_dow"] == 0).astype(int)

    orders = (
        orders.groupby(["store", "sap"], group_keys=False)
        .apply(_add_lag_features)
        .reset_index(drop=True)
    )

    orders = orders.dropna(subset=["lag_1"]).copy()

    numeric_cols = ["case_count", "tray", "lead_time_days"]
    for col in numeric_cols:
        orders[col] = pd.to_numeric(orders[col], errors="coerce")

    orders = orders.fillna({"lag_2": 0.0, "rolling_mean_4": orders["lag_1"]})

    orders["case_count"] = orders["case_count"].fillna(0)
    orders["tray"] = orders["tray"].fillna(0)
    orders["lead_time_days"] = orders["lead_time_days"].fillna(orders["lead_time_days"].median())


    return orders



def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train baseline demand model")
    parser.add_argument("--orders", default="data/daily/current_orders.csv", help="Path to orders CSV")
    parser.add_argument("--stock", default="data/daily/storeStock.csv", help="Path to store stock CSV")
    parser.add_argument("--promos", nargs='*', help="Glob pattern(s) for promo PDFs")
    return parser.parse_args()


def train_baseline(df: pd.DataFrame) -> None:
    feature_cols = [
        "lag_1",
        "lag_2",
        "rolling_mean_4",
        "promo_active",
        "delivery_dow",
        "delivery_month",
        "is_monday_delivery",
        "lead_time_days",
        "case_count",
        "tray",
    ]

    df = df.sort_values("delivery_date")
    unique_dates = df["delivery_date"].dropna().unique()
    unique_dates = np.sort(unique_dates)

    if len(unique_dates) < 6:
        split_idx = max(1, len(unique_dates) - 2)
    else:
        split_idx = len(unique_dates) - 4

    cutoff_date = unique_dates[split_idx]

    train_mask = df["delivery_date"] < cutoff_date
    train_df = df[train_mask].copy()
    test_df = df[~train_mask].copy()

    if train_df.empty or test_df.empty:
        raise ValueError("Not enough data for time-based split. Provide more history.")

    X_train = train_df[feature_cols]
    y_train = train_df["cases"]

    X_test = test_df[feature_cols]
    y_test = test_df["cases"]

    model = GradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)

    naive_pred = X_test["lag_1"].values
    naive_mae = mean_absolute_error(y_test, naive_pred)
    naive_rmse = mean_squared_error(y_test, naive_pred, squared=False)

    print("Test rows:", len(test_df))
    print("GradientBoosting MAE:", round(mae, 3))
    print("GradientBoosting RMSE:", round(rmse, 3))
    print("Naive (lag1) MAE:", round(naive_mae, 3))
    print("Naive (lag1) RMSE:", round(naive_rmse, 3))

    feature_importance = pd.Series(
        model.feature_importances_, index=feature_cols
    ).sort_values(ascending=False)
    print("\nFeature importance:")
    print(feature_importance)


if __name__ == "__main__":
    args = _parse_args()

    order_path = Path(args.orders)
    if not order_path.is_absolute():
        order_path = Path.cwd() / args.orders
    if not order_path.exists():
        raise FileNotFoundError(f"Orders file not found: {order_path}")

    stock_path = Path(args.stock)
    if not stock_path.is_absolute():
        stock_path = Path.cwd() / args.stock
    if not stock_path.exists():
        raise FileNotFoundError(f"Stock file not found: {stock_path}")

    if args.promos:
        promo_paths = []
        for pattern in args.promos:
            promo_paths.extend(Path.cwd().glob(pattern))
        # remove duplicates while preserving order
        seen = set()
        promo_paths = [p for p in promo_paths if not (p in seen or seen.add(p))]
    else:
        promo_paths = list(Path("promos/raw").glob("*.pdf"))

    df = build_modeling_dataframe(order_path, stock_path, promo_paths)
    print("Modeling rows:", len(df))
    train_baseline(df)

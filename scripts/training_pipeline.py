"""Training pipeline orchestrator.

Stages:
  1) Load data (orders, stock, promos, corrections)
  2) Train baseline model (with correction features)
  3) Validate vs naive; gate on thresholds
  4) (Future) Save model artifact for inference
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import os
from typing import Optional

# Support running as a script (python training_pipeline.py) without package context
try:
    from . import baseline_model as bm  # type: ignore
    from . import model_validator as mv  # type: ignore
    from . import firebase_loader as fl  # type: ignore
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    import baseline_model as bm  # type: ignore
    import model_validator as mv  # type: ignore
    import firebase_loader as fl  # type: ignore

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False


def _build_promo_paths(promos: list[str] | None) -> list[Path]:
    promo_paths = []
    if promos:
        for pattern in promos:
            promo_paths.extend(Path.cwd().glob(pattern))
    return promo_paths


def _load_corrections_from_postgres(route_number: str):
    if not PG_AVAILABLE:
        return None
    import pandas as pd
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            port=int(os.environ.get('POSTGRES_PORT', 5432)),
            database=os.environ.get('POSTGRES_DB', 'routespark'),
            user=os.environ.get('POSTGRES_USER', 'routespark'),
            password=os.environ.get('POSTGRES_PASSWORD', ''),
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    store_id,
                    sap,
                    schedule_key,
                    COUNT(*) as samples,
                    AVG(correction_delta) as avg_delta,
                    AVG(correction_ratio) as avg_ratio,
                    STDDEV(correction_ratio) as ratio_stddev,
                    AVG(CASE WHEN was_removed THEN 1.0 ELSE 0.0 END) as removal_rate,
                    AVG(CASE WHEN promo_active THEN 1.0 ELSE 0.0 END) as promo_rate
                FROM forecast_corrections
                WHERE route_number = %s
                  AND is_holiday_week = FALSE
                GROUP BY store_id, sap, schedule_key
                """,
                [route_number],
            )
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[training_pipeline] Warning: Could not load corrections from PostgreSQL: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_orders_dataframe_from_postgres(route_number: str, since_days: int):
    import pandas as pd
    orders = fl.load_orders_from_postgres(route_number=route_number, since_days=since_days)
    catalog = fl.load_catalog_from_postgres(route_number)
    catalog_map = {str(p.sap): p for p in catalog}

    rows = []
    for order in orders:
        for store in order.stores:
            for item in store.items:
                prod = catalog_map.get(str(item.sap))
                rows.append({
                    "store": store.store_name or store.store_id,
                    "store_id": store.store_id,
                    "sap": str(item.sap),
                    "delivery_date": order.expected_delivery_date,
                    "order_date": order.order_date,
                    "schedule_key": order.schedule_key,
                    "units": item.quantity,
                    "cases": item.cases,
                    "promo_active": item.promo_active,
                    "case_count": getattr(prod, "case_pack", None) if prod else None,
                    "tray": getattr(prod, "tray", None) if prod else None,
                })

    return pd.DataFrame(rows)


def run_pipeline(
    orders_csv: Optional[Path],
    stock_csv: Optional[Path],
    promos: list[str] | None,
    corrections_csv: Path | None,
    calendar_csv: Path | None,
    mae_threshold: float,
    rmse_threshold: float,
    save_preds_csv: Path | None = None,
    use_postgres: bool = False,
    route_number: Optional[str] = None,
    since_days: int = 365,
) -> dict:
    # Build training DataFrame
    if use_postgres:
        if not route_number:
            raise ValueError("route_number is required when use_postgres=True")
        orders_df = _load_orders_dataframe_from_postgres(route_number, since_days)
        corrections_df = _load_corrections_from_postgres(route_number)
        df = bm.build_modeling_dataframe_from_orders_df(
            orders=orders_df,
            corrections_df=corrections_df,
            calendar_csv=calendar_csv,
        )
    else:
        if not orders_csv or not stock_csv:
            raise ValueError("orders_csv and stock_csv are required when not using PostgreSQL")
        promo_paths = _build_promo_paths(promos)
        df = bm.build_modeling_dataframe(orders_csv, stock_csv, promo_paths, corrections_csv, calendar_csv)

    # Base features (always available)
    base_features = [
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
    
    # Optional correction features (only if corrections provided)
    correction_features = [
        "corr_samples",
        "corr_avg_delta",
        "corr_avg_ratio",
        "corr_ratio_stddev",
        "corr_removal_rate",
        "corr_promo_rate",
    ]
    
    # Optional calendar features (only if calendar provided)
    calendar_features = [
        "is_first_weekend_of_month",
        "is_last_weekend_of_month",
        "is_holiday",
        "is_holiday_week",
    ]
    
    # Build feature list dynamically based on what's available
    feature_cols = base_features.copy()
    for feat in correction_features + calendar_features:
        if feat in df.columns:
            feature_cols.append(feat)
    
    print(f"Using {len(feature_cols)} features: {feature_cols}")

    df = df.sort_values("delivery_date")
    unique_dates = df["delivery_date"].dropna().unique()
    unique_dates = sorted(unique_dates)

    if len(unique_dates) < 6:
        split_idx = max(1, len(unique_dates) - 2)
    else:
        split_idx = len(unique_dates) - 4

    cutoff_date = unique_dates[split_idx]

    train_df = df[df["delivery_date"] < cutoff_date].copy()
    test_df = df[df["delivery_date"] >= cutoff_date].copy()

    if train_df.empty or test_df.empty:
        raise ValueError("Not enough data for time-based split. Provide more history.")

    X_train = train_df[feature_cols]
    y_train = train_df["cases"]
    X_test = test_df[feature_cols]
    y_test = test_df["cases"]

    model = bm.GradientBoostingRegressor(random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_naive = X_test["lag_1"].values

    # Optionally save preds
    if save_preds_csv:
        import pandas as pd

        out = test_df[["store", "sap", "delivery_date"]].copy()
        out["true"] = y_test.values
        out["pred"] = y_pred
        out["naive"] = y_naive
        out.to_csv(save_preds_csv, index=False)

    metrics = mv.validate_predictions(
        y_true=y_test.values,
        y_pred=y_pred,
        y_naive=y_naive,
        mae_threshold=mae_threshold,
        rmse_threshold=rmse_threshold,
    )
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Training pipeline runner")
    parser.add_argument("--orders", help="Orders CSV (2-row header)")
    parser.add_argument("--stock", help="Stock CSV")
    parser.add_argument("--corrections", help="Corrections aggregate CSV")
    parser.add_argument("--calendar", help="Calendar features CSV")
    parser.add_argument("--promos", nargs="*", help="Promo PDF globs")
    parser.add_argument("--maeThreshold", type=float, default=5.0)
    parser.add_argument("--rmseThreshold", type=float, default=8.0)
    parser.add_argument("--savePreds", help="Optional CSV to save predictions for inspection")
    parser.add_argument("--usePostgres", action="store_true", help="Load training data from PostgreSQL")
    parser.add_argument("--route", help="Route number (required if --usePostgres)")
    parser.add_argument("--sinceDays", type=int, default=365)
    args = parser.parse_args()

    metrics = run_pipeline(
        orders_csv=Path(args.orders) if args.orders else None,
        stock_csv=Path(args.stock) if args.stock else None,
        promos=args.promos,
        corrections_csv=Path(args.corrections) if args.corrections else None,
        calendar_csv=Path(args.calendar) if args.calendar else None,
        mae_threshold=args.maeThreshold,
        rmse_threshold=args.rmseThreshold,
        save_preds_csv=Path(args.savePreds) if args.savePreds else None,
        use_postgres=args.usePostgres,
        route_number=args.route,
        since_days=args.sinceDays,
    )
    print(metrics)

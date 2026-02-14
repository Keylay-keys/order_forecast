"""Feedback collector: pull user corrections from DuckDB and compute aggregates.

Inputs:
  - DuckDB file with table `forecast_corrections` as defined in db_schema.py
    (expected columns: route_number, schedule_key, delivery_date, store_id,
     sap, predicted_units, final_units, promo_active, submitted_at, was_removed)

Outputs:
  - Aggregated features per (store_id, sap, schedule_key) suitable for feature_cache
"""

from __future__ import annotations

import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Iterable, Optional


def get_connection(db_path: str):
    return duckdb.connect(db_path)


def fetch_corrections(
    db_path: str,
    route_number: str,
    schedule_keys: Optional[Iterable[str]] = None,
    since_days: int = 180,
) -> pd.DataFrame:
    """Fetch corrections for a route and optional schedule keys."""
    schedule_keys = [s.lower() for s in schedule_keys] if schedule_keys else None
    since_date = datetime.utcnow() - timedelta(days=since_days)

    clauses = ["route_number = ?"]
    params = [route_number]

    clauses.append("submitted_at >= ?")
    params.append(since_date)

    if schedule_keys:
        clauses.append("lower(schedule_key) IN (%s)" % ",".join(["?"] * len(schedule_keys)))
        params.extend(schedule_keys)

    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT
          route_number,
          lower(schedule_key) AS schedule_key,
          delivery_date,
          store_id,
          sap,
          CAST(predicted_units AS DOUBLE) AS predicted_units,
          CAST(final_units AS DOUBLE) AS final_units,
          (CASE WHEN predicted_units = 0 THEN 0 ELSE final_units - predicted_units END) AS correction_delta,
          (CASE WHEN predicted_units = 0 THEN 0 ELSE final_units / predicted_units END) AS correction_ratio,
          COALESCE(promo_active, FALSE) AS promo_active,
          COALESCE(was_removed, FALSE) AS was_removed,
          submitted_at
        FROM forecast_corrections
        WHERE {where_sql}
    """
    with get_connection(db_path) as con:
        df = con.execute(sql, params).fetchdf()
    return df


def aggregate_corrections(df: pd.DataFrame) -> pd.DataFrame:
    """Compute aggregate correction features per (store_id, sap, schedule_key)."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "store_id",
                "sap",
                "schedule_key",
                "samples",
                "avg_delta",
                "avg_ratio",
                "ratio_stddev",
                "removal_rate",
                "promo_rate",
                "last_seen",
            ]
        )

    grouped = df.groupby(["store_id", "sap", "schedule_key"], as_index=False).agg(
        samples=("correction_ratio", "count"),
        avg_delta=("correction_delta", "mean"),
        avg_ratio=("correction_ratio", "mean"),
        ratio_stddev=("correction_ratio", "std"),
        removal_rate=("was_removed", "mean"),
        promo_rate=("promo_active", "mean"),
        last_seen=("submitted_at", "max"),
    )

    # Fill NaNs for stddev with 0 when only one sample
    grouped["ratio_stddev"] = grouped["ratio_stddev"].fillna(0.0)
    return grouped


def collect_feedback_features(
    db_path: str,
    route_number: str,
    schedule_keys: Optional[Iterable[str]] = None,
    since_days: int = 180,
) -> pd.DataFrame:
    """Convenience wrapper: fetch + aggregate."""
    df = fetch_corrections(
        db_path=db_path,
        route_number=route_number,
        schedule_keys=schedule_keys,
        since_days=since_days,
    )
    return aggregate_corrections(df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Collect feedback aggregates from DuckDB")
    parser.add_argument("--db", required=True, help="Path to analytics.duckdb")
    parser.add_argument("--route", required=True, help="Route number")
    parser.add_argument("--schedule", nargs="*", help="Schedule keys to include (e.g., monday thursday)")
    parser.add_argument("--sinceDays", type=int, default=180, help="Lookback window in days")
    args = parser.parse_args()

    df = collect_feedback_features(
        db_path=args.db,
        route_number=args.route,
        schedule_keys=args.schedule,
        since_days=args.sinceDays,
    )
    print(df.head())

"""Shared retrain execution helper for daemon and queue workers."""

from __future__ import annotations

from typing import Any, Dict


def run_retrain_for_route(route_number: str) -> Dict[str, Any]:
    """Run the ML retrain pipeline for a single route using PostgreSQL data."""
    try:
        from .training_pipeline import run_pipeline  # type: ignore
    except Exception:
        from training_pipeline import run_pipeline  # type: ignore

    metrics = run_pipeline(
        orders_csv=None,
        stock_csv=None,
        promos=None,
        corrections_csv=None,
        calendar_csv=None,
        mae_threshold=5.0,
        rmse_threshold=8.0,
        use_postgres=True,
        route_number=route_number,
        since_days=365,
    )
    return metrics if isinstance(metrics, dict) else {"result": metrics}

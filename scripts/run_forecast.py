"""CLI entry point to generate and cache a forecast for a given route/date."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Support running as a script without package context
try:
    from .forecast_engine import ForecastConfig, generate_forecast
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from forecast_engine import ForecastConfig, generate_forecast


def main():
    parser = argparse.ArgumentParser(description="Run forecast and cache to Firestore")
    parser.add_argument("--route", required=True, help="Route number")
    parser.add_argument("--deliveryDate", required=True, help="Delivery date (YYYY-MM-DD or MM/DD/YYYY)")
    parser.add_argument("--serviceAccount", help="Path to service account JSON")
    parser.add_argument("--sinceDays", type=int, default=365)
    parser.add_argument("--roundCases", action="store_true", default=True)
    parser.add_argument("--ttlDays", type=int, default=7)
    args = parser.parse_args()

    cfg = ForecastConfig(
        route_number=args.route,
        delivery_date=args.deliveryDate,
        schedule_key="",  # auto-deduced inside engine
        service_account=args.serviceAccount,
        since_days=args.sinceDays,
        round_cases=args.roundCases,
        ttl_days=args.ttlDays,
    )
    forecast = generate_forecast(cfg)
    print(f"Wrote forecast {forecast.forecast_id} with {len(forecast.items)} items")


if __name__ == "__main__":
    main()

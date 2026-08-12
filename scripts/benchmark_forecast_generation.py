#!/usr/bin/env python3
"""Run one route-scoped generation checkpoint and persist direct phase timings."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from forecast_engine import ForecastConfig, generate_forecast


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--route", required=True)
    parser.add_argument("--delivery-date", required=True)
    parser.add_argument("--schedule-key", required=True)
    parser.add_argument("--serviceAccount", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--firebase-loaders", action="store_true")
    parser.add_argument("--archive-dir")
    args = parser.parse_args()
    if not args.route.isdigit() or len(args.route) > 10:
        parser.error("--route must be numeric")
    if args.firebase_loaders:
        os.environ["FORECAST_USE_POSTGRES"] = "0"
    if args.archive_dir:
        os.environ["ROUTESPARK_FORECAST_ARCHIVE_DIR"] = args.archive_dir

    forecast = generate_forecast(ForecastConfig(
        route_number=args.route,
        delivery_date=args.delivery_date,
        schedule_key=args.schedule_key,
        service_account=args.serviceAccount,
        since_days=365,
        round_cases=True,
        ttl_days=7,
    ))
    report = {
        "routeNumber": args.route,
        "deliveryDate": forecast.delivery_date,
        "scheduleKey": forecast.schedule_key,
        "forecastId": forecast.forecast_id,
        "generationMode": forecast.generation_mode,
        "itemCount": len(forecast.items),
        "measuredAt": datetime.now(timezone.utc).isoformat(),
        "queueWaitMs": None,
        "queueMeasurement": "not_applicable_direct_checkpoint",
        "timingsMs": forecast.generation_timings_ms,
    }
    parent = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(parent, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

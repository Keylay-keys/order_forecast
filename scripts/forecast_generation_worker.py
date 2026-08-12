#!/usr/bin/env python3
"""Dedicated durable forecast-generation queue worker."""

from __future__ import annotations

import argparse
import os
import socket
import time

from firebase_writer import get_firestore_client
from forecast_generation_queue import list_queued_generation_routes, process_generation_jobs_for_route


def drain_once(
    client,
    worker_id: str,
    *,
    route_limit: int,
    max_jobs_per_route: int,
    sa_path: str | None,
) -> bool:
    """Drain one bounded batch; report transient failures without killing the worker."""
    try:
        routes = list_queued_generation_routes(limit=route_limit)
    except Exception as exc:
        print(f"[forecast_generation_worker] queue_list_error={exc}", flush=True)
        return False

    succeeded = True
    for route in routes:
        try:
            result = process_generation_jobs_for_route(
                client,
                route,
                worker_id,
                max_jobs=max_jobs_per_route,
                sa_path=sa_path,
            )
        except Exception as exc:
            succeeded = False
            print(
                f"[forecast_generation_worker] route={route} drain_error={exc}",
                flush=True,
            )
            continue
        if result.get("claimed"):
            print(f"[forecast_generation_worker] route={route} stats={result}", flush=True)
    return succeeded


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serviceAccount", default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument(
        "--route-limit",
        type=int,
        default=int(os.environ.get("FORECAST_WORKER_ROUTE_LIMIT", "20")),
    )
    parser.add_argument(
        "--max-jobs-per-route",
        type=int,
        default=int(os.environ.get("FORECAST_WORKER_MAX_JOBS_PER_ROUTE", "2")),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    client = get_firestore_client(args.serviceAccount)
    worker_id = f"forecast-worker:{socket.gethostname()}:{os.getpid()}"
    failure_streak = 0
    while True:
        succeeded = drain_once(
            client,
            worker_id,
            route_limit=max(1, args.route_limit),
            max_jobs_per_route=max(1, args.max_jobs_per_route),
            sa_path=args.serviceAccount,
        )
        if args.once:
            return 0 if succeeded else 1
        failure_streak = 0 if succeeded else min(failure_streak + 1, 6)
        delay = max(0.25, args.poll_seconds) * (2 ** failure_streak)
        time.sleep(min(60.0, delay))


if __name__ == "__main__":
    raise SystemExit(main())

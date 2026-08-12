#!/usr/bin/env python3
"""Dedicated durable forecast-generation queue worker."""

from __future__ import annotations

import argparse
import os
import socket
import time

from firebase_writer import get_firestore_client
from forecast_generation_queue import list_queued_generation_routes, process_generation_jobs_for_route


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serviceAccount", default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    client = get_firestore_client(args.serviceAccount)
    worker_id = f"forecast-worker:{socket.gethostname()}:{os.getpid()}"
    while True:
        routes = list_queued_generation_routes()
        for route in routes:
            result = process_generation_jobs_for_route(
                client,
                route,
                worker_id,
                max_jobs=8,
                sa_path=args.serviceAccount,
            )
            if result.get("claimed"):
                print(f"[forecast_generation_worker] route={route} stats={result}", flush=True)
        if args.once:
            return 0
        time.sleep(max(0.25, args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())

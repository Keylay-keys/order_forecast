#!/usr/bin/env python3
"""Tiny internal HTTP server for order-forecast supervisor status.

This runs alongside the order-forecast supervisor on the data node and exposes
the contents of the local service status file over a cluster-internal HTTP
endpoint. It replaces the incorrect cross-node log-file mount that the edge
web-api was using.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("status_api")

HOST = os.environ.get("ORDER_FORECAST_STATUS_BIND", "0.0.0.0")
PORT = int(os.environ.get("ORDER_FORECAST_STATUS_PORT", "8080"))
STATUS_FILE = Path(
    os.environ.get("ORDER_FORECAST_STATUS_FILE", "/app/logs/service_status.json")
).resolve()
STALE_AFTER_SECONDS = int(os.environ.get("SERVICE_STATUS_STALE_SECONDS", "180"))


def _payload(data: dict, status_code: HTTPStatus) -> bytes:
    return json.dumps(data).encode("utf-8")


def _read_status() -> tuple[dict, HTTPStatus]:
    if not STATUS_FILE.exists():
        return (
            {
                "status": "unavailable",
                "error": "status_file_missing",
                "timestamp": datetime.utcnow().isoformat(),
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )

    try:
        data = json.loads(STATUS_FILE.read_text())
        ts = data.get("timestamp")
        age_seconds = None
        if ts:
            try:
                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_seconds = int((datetime.utcnow() - ts_dt.replace(tzinfo=None)).total_seconds())
            except Exception:
                age_seconds = None
        stale = age_seconds is not None and age_seconds > STALE_AFTER_SECONDS
        return (
            {
                "status": "stale" if stale else "healthy",
                "timestamp": ts,
                "ageSeconds": age_seconds,
                "services": data.get("services", []),
            },
            HTTPStatus.OK,
        )
    except Exception as exc:
        logger.error("Failed to read %s: %s", STATUS_FILE, exc)
        return (
            {
                "status": "unavailable",
                "error": "Failed to read status",
                "timestamp": datetime.utcnow().isoformat(),
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            body = _payload({"status": "healthy", "timestamp": datetime.utcnow().isoformat()}, HTTPStatus.OK)
            self.send_response(HTTPStatus.OK)
        elif self.path == "/health/services":
            payload, status = _read_status()
            body = _payload(payload, status)
            self.send_response(status)
        else:
            body = _payload({"status": "not_found"}, HTTPStatus.NOT_FOUND)
            self.send_response(HTTPStatus.NOT_FOUND)

        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    logger.info("Serving order-forecast status API on %s:%s", HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()

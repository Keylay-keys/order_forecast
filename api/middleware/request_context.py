"""Per-request correlation and structured request diagnostics."""

from __future__ import annotations

import logging
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request

from ..errors import REQUEST_ID_HEADER


def setup_request_context(app: FastAPI, app_logger: logging.Logger) -> None:
    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = str(uuid4())
        request.state.request_id = request_id
        started_at = perf_counter()

        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = request_id

        duration_ms = (perf_counter() - started_at) * 1000
        log = app_logger.info
        if response.status_code >= 500:
            log = app_logger.error
        elif response.status_code >= 400:
            log = app_logger.warning

        log(
            "api_request request_id=%s method=%s path=%s status=%s duration_ms=%.0f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response

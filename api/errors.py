"""Shared API error contract and secure exception handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

REQUEST_ID_HEADER = "X-Request-ID"


class StructuredApiError(Exception):
    """Intentional API error using RouteSpark's standard response envelope."""

    def __init__(
        self,
        status_code: int,
        error: str,
        code: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(error)
        self.status_code = status_code
        self.error = error
        self.code = code
        self.details = details


def get_request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "unassigned")


def structured_error_response(
    request: Request,
    *,
    status_code: int,
    error: str,
    code: str,
    details: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    content: Dict[str, Any] = {
        "error": error,
        "code": code,
    }
    if details is not None:
        content["details"] = details

    return JSONResponse(
        status_code=status_code,
        content=content,
        headers={REQUEST_ID_HEADER: get_request_id(request)},
    )


def install_api_error_handlers(
    app: FastAPI,
    *,
    debug_mode: bool,
    app_logger: logging.Logger,
) -> None:
    async def structured_api_error_handler(
        request: Request,
        exc: StructuredApiError,
    ) -> JSONResponse:
        request.state.api_error_code = exc.code
        request_id = get_request_id(request)
        app_logger.warning(
            "api_error request_id=%s method=%s path=%s status=%s code=%s",
            request_id,
            request.method,
            request.url.path,
            exc.status_code,
            exc.code,
        )
        return structured_error_response(
            request,
            status_code=exc.status_code,
            error=exc.error,
            code=exc.code,
            details=exc.details,
        )

    async def generic_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        request.state.api_error_code = "INTERNAL_ERROR"
        request_id = get_request_id(request)
        app_logger.exception(
            "api_unhandled request_id=%s method=%s path=%s error_type=%s",
            request_id,
            request.method,
            request.url.path,
            type(exc).__name__,
        )

        if debug_mode:
            return structured_error_response(
                request,
                status_code=500,
                error=str(exc),
                code="INTERNAL_ERROR",
                details={"type": type(exc).__name__},
            )

        return structured_error_response(
            request,
            status_code=500,
            error="Internal server error",
            code="INTERNAL_ERROR",
        )

    app.add_exception_handler(StructuredApiError, structured_api_error_handler)
    app.add_exception_handler(Exception, generic_exception_handler)

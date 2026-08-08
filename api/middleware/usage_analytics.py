"""Best-effort request accounting for authenticated API traffic."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from starlette.routing import Match

from ..usage_analytics import (
    ApiUsageRequest,
    enqueue_api_request,
    extract_route_hint,
    normalize_endpoint_path,
)


def _resolve_route_template(app: FastAPI, scope) -> str:
    route = scope.get("route")
    route_template = str(getattr(route, "path", "") or "")
    if route_template:
        return route_template
    for candidate in app.routes:
        match, _child_scope = candidate.matches(scope)
        if match == Match.FULL:
            candidate_path = str(getattr(candidate, "path", "") or "")
            if candidate_path:
                return candidate_path
            effective_candidates = getattr(candidate, "effective_candidates", None)
            if callable(effective_candidates):
                for effective in effective_candidates():
                    effective_match, _effective_scope = effective.matches(scope)
                    if effective_match == Match.FULL:
                        return str(getattr(effective, "path", "") or "")
    return ""


def setup_usage_analytics(app: FastAPI, app_logger: logging.Logger) -> None:
    @app.middleware("http")
    async def usage_analytics(request: Request, call_next):
        request_path = request.url.path
        request_method = request.method
        request_root_path = str(request.scope.get("root_path") or "")
        response = await call_next(request)
        try:
            uid = str(getattr(request.state, "usage_uid", "") or "").strip()
            if uid and request_method != "OPTIONS":
                route_template = ""
                if response.status_code >= 400:
                    route_template = _resolve_route_template(
                        app,
                        {
                            "type": "http",
                            "path": request_path,
                            "root_path": request_root_path,
                            "method": request_method,
                        },
                    )
                enqueue_api_request(
                    ApiUsageRequest(
                        uid=uid,
                        path=request_path,
                        status_code=response.status_code,
                        route_hint=extract_route_hint(request_path, request.query_params),
                        method=request_method,
                        endpoint=normalize_endpoint_path(request_path, route_template),
                        error_code=str(getattr(request.state, "api_error_code", "") or ""),
                        request_id=str(getattr(request.state, "request_id", "") or ""),
                    )
                )
        except Exception:
            # Analytics must never alter an API response or user workflow.
            app_logger.exception("Could not enqueue API usage")
        return response

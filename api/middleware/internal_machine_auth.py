"""ASGI authentication boundary for ``/internal/v1/pcf`` endpoints."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Awaitable, Callable

from ..machine_auth import (
    INTERNAL_PREFIX,
    MachineAuthError,
    authenticate_internal_headers,
)


logger = logging.getLogger("security.internal_machine_auth")


class InternalMachineAuthMiddleware:
    """Authenticate from the ASGI scope without reading the request body."""

    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        enabled: bool | None = None,
    ) -> None:
        self.app = app
        self.enabled = (
            os.environ.get("PCF_INTERNAL_API_ENABLED", "false").strip().lower() == "true"
            if enabled is None
            else enabled
        )

    async def __call__(self, scope: dict[str, Any], receive: Callable, send: Callable) -> None:
        if scope.get("type") != "http" or not scope.get("path", "").startswith(INTERNAL_PREFIX):
            await self.app(scope, receive, send)
            return

        if not self.enabled:
            await self._reject(send, 404, "internal_endpoint_not_found")
            return

        path = str(scope.get("path", ""))
        method = str(scope.get("method", ""))
        if method.upper() == "OPTIONS":
            await self._reject(send, 404, "internal_endpoint_not_found")
            return

        raw_headers = scope.get("headers") or []
        headers: dict[str, str] = {}
        for raw_name, raw_value in raw_headers:
            name = raw_name.decode("latin-1").lower()
            if name in headers:
                await self._reject(send, 401, "machine_auth_invalid")
                return
            headers[name] = raw_value.decode("latin-1")

        try:
            principal = authenticate_internal_headers(method, path, headers)
        except MachineAuthError as exc:
            logger.warning(
                {"event": "internal_machine_auth_rejected", "path": path, "code": exc.code}
            )
            await self._reject(send, exc.status_code, exc.code)
            return

        state = scope.setdefault("state", {})
        state["machine_principal"] = principal
        await self.app(scope, receive, send)

    @staticmethod
    async def _reject(send: Callable, status: int, code: str) -> None:
        body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"cache-control", b"no-store"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def setup_internal_machine_auth(app: Any) -> None:
    app.add_middleware(InternalMachineAuthMiddleware)

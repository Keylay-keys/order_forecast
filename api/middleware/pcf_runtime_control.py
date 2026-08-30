"""ASGI runtime-stage authorization for authenticated internal PCF requests."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from ..machine_auth import INTERNAL_PREFIX, endpoint_contract
from ..pcf_runtime_control import PcfRuntimeControl, RuntimeControlError, load_runtime_control


logger = logging.getLogger("security.pcf_runtime_control")
_CAPABILITY_STAGES = {
    "pcf.event.ingest": "ingress",
    "pcf.backup.verify": "ingress",
    "pcf.claim": "shadow",
    "pcf.heartbeat": "shadow",
    "pcf.result": "shadow",
}


def required_stage(method: str, path: str) -> str | None:
    contract = endpoint_contract(method, path)
    return _CAPABILITY_STAGES.get(contract.capability) if contract is not None else None


class PcfRuntimeControlMiddleware:
    """Read one complete control document before any internal request body."""

    def __init__(
        self,
        app: Callable[..., Awaitable[None]],
        *,
        reader: Callable[[], PcfRuntimeControl] = load_runtime_control,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.app = app
        self.reader = reader
        self.clock = clock

    async def __call__(self, scope: dict[str, Any], receive: Callable, send: Callable) -> None:
        path = str(scope.get("path", ""))
        if scope.get("type") != "http" or not path.startswith(INTERNAL_PREFIX):
            await self.app(scope, receive, send)
            return
        minimum = required_stage(str(scope.get("method", "")), path)
        if minimum is None:
            await self._reject(send, 404, "internal_endpoint_not_found")
            return
        try:
            control = self.reader()
        except RuntimeControlError:
            logger.error({"event": "pcf_runtime_control_unavailable", "path": path})
            await self._reject(send, 503, "pcf_runtime_control_unavailable")
            return
        if not control.is_fresh(self.clock()) or not control.allows_stage(minimum):
            logger.warning(
                {
                    "event": "pcf_runtime_control_denied",
                    "path": path,
                    "revision": control.revision,
                }
            )
            await self._reject(
                send,
                503,
                "pcf_runtime_control_denied",
                revision=control.revision,
            )
            return
        state = scope.setdefault("state", {})
        state["pcf_runtime_control"] = control

        async def send_with_revision(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                headers = [
                    header
                    for header in headers
                    if header[0].lower() != b"x-routespark-pcf-control-revision"
                ]
                headers.append(
                    (b"x-routespark-pcf-control-revision", control.revision.encode("ascii"))
                )
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_revision)

    @staticmethod
    async def _reject(
        send: Callable,
        status: int,
        code: str,
        *,
        revision: str | None = None,
    ) -> None:
        body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode("utf-8")
        headers = [
            (b"content-type", b"application/json"),
            (b"cache-control", b"no-store"),
            (b"content-length", str(len(body)).encode("ascii")),
        ]
        if status == 503:
            headers.append((b"retry-after", b"10"))
        if revision is not None:
            headers.append((b"x-routespark-pcf-control-revision", revision.encode("ascii")))
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": body})


def setup_pcf_runtime_control(app: Any) -> None:
    app.add_middleware(PcfRuntimeControlMiddleware)

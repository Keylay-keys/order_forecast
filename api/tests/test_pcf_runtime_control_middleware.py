from __future__ import annotations

from unittest.mock import AsyncMock, Mock
from datetime import datetime, timezone

import pytest

from order_forecast.api.middleware.pcf_runtime_control import (
    PcfRuntimeControlMiddleware,
    required_stage,
)
from order_forecast.api.middleware.internal_machine_auth import InternalMachineAuthMiddleware
from order_forecast.api.pcf_runtime_control import RuntimeControlError
from order_forecast.api.tests.test_pcf_runtime_control import control_payload
from order_forecast.api.pcf_runtime_control import parse_runtime_control


def _control(stage="off", discovery="legacy", authority="legacy_mac"):
    return parse_runtime_control(
        control_payload(stage=stage, discovery=discovery, authority=authority)
    )


CLOCK = lambda: datetime(2026, 8, 29, 18, 0, tzinfo=timezone.utc)


async def _capture(messages, message):
    messages.append(message)


@pytest.mark.anyio
async def test_missing_or_denied_control_rejects_before_body_or_downstream():
    for reader, expected_code in (
        (lambda: (_ for _ in ()).throw(RuntimeControlError("bad")), b"unavailable"),
        (lambda: _control("off"), b"denied"),
    ):
        downstream = AsyncMock()
        receive = AsyncMock(side_effect=AssertionError("body must not be read"))
        messages = []
        middleware = PcfRuntimeControlMiddleware(downstream, reader=reader, clock=CLOCK)
        await middleware(
            {"type": "http", "method": "POST", "path": "/internal/v1/pcf/events"},
            receive,
            lambda message: _capture(messages, message),
        )
        downstream.assert_not_called()
        receive.assert_not_called()
        assert messages[0]["status"] == 503
        assert expected_code in messages[1]["body"]
        assert dict(messages[0]["headers"])[b"cache-control"] == b"no-store"


@pytest.mark.anyio
async def test_allowed_request_attaches_one_complete_observed_revision():
    control = _control("shadow")
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/internal/v1/pcf/claims",
        "state": {"machine_principal": object()},
    }
    messages = []

    async def responding_downstream(_scope, _receive, send):
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = PcfRuntimeControlMiddleware(
        responding_downstream, reader=lambda: control, clock=CLOCK
    )
    await middleware(scope, AsyncMock(), lambda message: _capture(messages, message))
    assert scope["state"]["pcf_runtime_control"] is control
    assert dict(messages[0]["headers"])[b"x-routespark-pcf-control-revision"] == (
        control.revision.encode("ascii")
    )


@pytest.mark.anyio
async def test_stale_control_rejects_before_downstream():
    control = _control("shadow")
    downstream = AsyncMock()
    messages = []
    middleware = PcfRuntimeControlMiddleware(
        downstream,
        reader=lambda: control,
        clock=lambda: datetime(2026, 8, 29, 18, 5, 1, tzinfo=timezone.utc),
    )
    await middleware(
        {"type": "http", "method": "POST", "path": "/internal/v1/pcf/claims"},
        AsyncMock(side_effect=AssertionError("body must not be read")),
        lambda message: _capture(messages, message),
    )
    downstream.assert_not_called()
    assert messages[0]["status"] == 503
    assert b"denied" in messages[1]["body"]


@pytest.mark.anyio
async def test_authentication_remains_outermost_and_rejects_before_control_lookup():
    reader = Mock(side_effect=AssertionError("control must follow authentication"))
    downstream = AsyncMock()
    controlled = PcfRuntimeControlMiddleware(downstream, reader=reader, clock=CLOCK)
    middleware = InternalMachineAuthMiddleware(controlled, enabled=True)
    receive = AsyncMock(side_effect=AssertionError("body must not be read"))
    messages = []
    await middleware(
        {
            "type": "http",
            "method": "POST",
            "path": "/internal/v1/pcf/events",
            "headers": [],
        },
        receive,
        lambda message: _capture(messages, message),
    )
    assert messages[0]["status"] == 401
    reader.assert_not_called()
    receive.assert_not_called()
    downstream.assert_not_called()


def test_endpoint_stage_matrix_is_exact():
    work_id = "123e4567-e89b-42d3-a456-426614174000"
    assert required_stage("POST", "/internal/v1/pcf/events") == "ingress"
    assert required_stage("POST", "/internal/v1/pcf/backups/offbox-verifications") == "ingress"
    assert required_stage("POST", "/internal/v1/pcf/claims") == "shadow"
    assert required_stage("POST", f"/internal/v1/pcf/work/{work_id}/heartbeat") == "shadow"
    assert required_stage("PUT", f"/internal/v1/pcf/work/{work_id}/result") == "shadow"
    assert required_stage("PUT", f"/internal/v1/pcf/work/{work_id}/heartbeat") is None
    assert required_stage("POST", f"/internal/v1/pcf/work/{work_id}/result") is None
    assert required_stage("POST", "/internal/v1/pcf/not-real") is None


def test_application_stack_keeps_authentication_outside_runtime_control():
    from order_forecast.api.main import app

    middleware = [entry.cls.__name__ for entry in app.user_middleware]
    assert middleware[:2] == [
        "InternalMachineAuthMiddleware",
        "PcfRuntimeControlMiddleware",
    ]

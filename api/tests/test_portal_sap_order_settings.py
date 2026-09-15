import json
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, patch

import pytest
from fastapi import Response
from starlette.requests import Request

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.portal_sap_order_store import (
    PortalSapOrderConflict,
    PortalSapOrderPreference,
    PortalSapOrderRead,
)
from order_forecast.api.routers import settings


@pytest.fixture
def anyio_backend():
    return "asyncio"


def request(method: str, body=None, content_type: str = "application/json") -> Request:
    payload = b"" if body is None else json.dumps(body).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    scope = {
        "type": "http",
        "method": method,
        "scheme": "https",
        "server": ("api.routespark.pro", 443),
        "client": ("192.0.2.1", 1234),
        "path": "/api/settings/sap-orders/portal",
        "raw_path": b"/api/settings/sap-orders/portal",
        "query_string": b"route=988200",
        "headers": [(b"content-type", content_type.encode("ascii"))],
    }
    result = Request(scope, receive)
    result.state.request_id = "request-test"
    return result


def endpoint(function):
    while hasattr(function, "__wrapped__"):
        function = function.__wrapped__
    return function


@pytest.mark.anyio
async def test_get_uses_only_verified_uid_and_authorized_route():
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    stored = PortalSapOrderRead(
        exists=True,
        preference=PortalSapOrderPreference(["200", "100"], 3, now),
    )
    response = Response()
    with patch.object(settings, "require_route_access", new=AsyncMock()) as access, patch.object(
        settings, "read_portal_sap_order", return_value=stored,
    ) as read:
        result = await endpoint(settings.get_portal_sap_order)(
            request("GET"), response, "988200", {"uid": "verified-uid"}, object(),
        )

    access.assert_awaited_once_with("988200", {"uid": "verified-uid"}, ANY)
    read.assert_called_once_with("verified-uid", "988200")
    assert result.sapOrder == ["200", "100"]
    assert result.revision == 3
    assert response.headers["Cache-Control"] == "private, no-store"


@pytest.mark.anyio
async def test_put_returns_committed_state_without_follow_up_read():
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    committed = PortalSapOrderPreference(["300", "100"], 8, now)
    response = Response()
    with patch.object(settings, "require_route_access", new=AsyncMock()), patch.object(
        settings, "save_portal_sap_order", return_value=committed,
    ) as save, patch.object(settings, "read_portal_sap_order") as read:
        result = await endpoint(settings.put_portal_sap_order)(
            request("PUT", {"sapOrder": ["300", "100"], "expectedRevision": 7}),
            response,
            "988200",
            {"uid": "verified-uid"},
            object(),
        )

    save.assert_called_once_with("verified-uid", "988200", ["300", "100"], 7)
    read.assert_not_called()
    assert result.sapOrder == ["300", "100"]
    assert result.revision == 8


@pytest.mark.anyio
async def test_conflict_contains_current_state_but_no_database_details():
    current = PortalSapOrderPreference(
        ["400"], 4, datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    with patch.object(settings, "require_route_access", new=AsyncMock()), patch.object(
        settings, "save_portal_sap_order", side_effect=PortalSapOrderConflict(current),
    ):
        with pytest.raises(StructuredApiError) as raised:
            await endpoint(settings.put_portal_sap_order)(
                request("PUT", {"sapOrder": ["300"], "expectedRevision": 3}),
                Response(), "988200", {"uid": "verified-uid"}, object(),
            )

    assert raised.value.status_code == 409
    assert raised.value.code == "PORTAL_SAP_ORDER_CONFLICT"
    assert raised.value.details == {
        "routeNumber": "988200",
        "sapOrder": ["400"],
        "revision": 4,
        "updatedAt": "2026-09-02T00:00:00Z",
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sap_order",
    [["duplicate", "duplicate"], ["bad value"], [123], ["valid\n"]],
)
async def test_invalid_lists_return_structured_422(sap_order):
    with patch.object(settings, "require_route_access", new=AsyncMock()), patch.object(
        settings, "save_portal_sap_order",
    ) as save:
        with pytest.raises(StructuredApiError) as raised:
            await endpoint(settings.put_portal_sap_order)(
                request("PUT", {"sapOrder": sap_order, "expectedRevision": 0}),
                Response(), "988200", {"uid": "verified-uid"}, object(),
            )
    assert raised.value.status_code == 422
    assert raised.value.code == "PORTAL_SAP_ORDER_INVALID"
    save.assert_not_called()


@pytest.mark.anyio
async def test_oversize_error_exposes_counts_only():
    proposed = [f"SAP-{index}" for index in range(2001)]
    with patch.object(settings, "require_route_access", new=AsyncMock()):
        with pytest.raises(StructuredApiError) as raised:
            await endpoint(settings.put_portal_sap_order)(
                request("PUT", {"sapOrder": proposed, "expectedRevision": 0}),
                Response(), "988200", {"uid": "verified-uid"}, object(),
            )
    assert raised.value.details == {"maxItems": 2000, "receivedItems": 2001}
    assert "SAP-1" not in str(raised.value.details)


@pytest.mark.anyio
async def test_database_outage_is_scoped_to_preference_request():
    with patch.object(settings, "require_route_access", new=AsyncMock()), patch.object(
        settings, "read_portal_sap_order", side_effect=RuntimeError("secret database detail"),
    ):
        with pytest.raises(StructuredApiError) as raised:
            await endpoint(settings.get_portal_sap_order)(
                request("GET"), Response(), "988200", {"uid": "verified-uid"}, object(),
            )
    assert raised.value.status_code == 503
    assert raised.value.code == "PORTAL_SAP_ORDER_UNAVAILABLE"
    assert "secret database detail" not in raised.value.error

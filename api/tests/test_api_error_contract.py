import json
import logging
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

try:
    from api.errors import StructuredApiError, install_api_error_handlers
    from api.middleware.request_context import setup_request_context
except ModuleNotFoundError:
    from order_forecast.api.errors import StructuredApiError, install_api_error_handlers
    from order_forecast.api.middleware.request_context import setup_request_context


def build_test_app() -> FastAPI:
    app = FastAPI()
    test_logger = logging.getLogger("test.api.error_contract")

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    @app.get("/structured")
    async def structured():
        raise StructuredApiError(
            status_code=409,
            error="Core items require attention",
            code="CORE_ITEMS_REQUIRED",
            details={"items": [{"storeId": "store-1", "sap": "28934"}]},
        )

    @app.get("/legacy-string")
    async def legacy_string():
        raise HTTPException(status_code=400, detail="Legacy message")

    @app.get("/legacy-object")
    async def legacy_object():
        raise HTTPException(status_code=422, detail={"field": "deliveryDate"})

    @app.get("/unexpected")
    async def unexpected():
        raise RuntimeError("database password should not reach the client")

    setup_request_context(app, test_logger)
    install_api_error_handlers(app, debug_mode=False, app_logger=test_logger)
    return app


def assert_request_id(response) -> str:
    request_id = response.headers.get("x-request-id")
    assert request_id
    assert str(UUID(request_id)) == request_id
    return request_id


def test_success_response_has_server_generated_request_id(caplog):
    app = build_test_app()
    with caplog.at_level(logging.INFO, logger="test.api.error_contract"):
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/ok", headers={"X-Request-ID": "untrusted-client-value"})

    request_id = assert_request_id(response)
    assert response.json() == {"ok": True}
    assert request_id != "untrusted-client-value"
    assert f"request_id={request_id}" in caplog.text
    assert "method=GET" in caplog.text
    assert "path=/ok" in caplog.text
    assert "status=200" in caplog.text


def test_structured_error_uses_standard_envelope_and_matching_log_id(caplog):
    app = build_test_app()
    with caplog.at_level(logging.INFO, logger="test.api.error_contract"):
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/structured")

    request_id = assert_request_id(response)
    assert response.status_code == 409
    assert response.json() == {
        "error": "Core items require attention",
        "code": "CORE_ITEMS_REQUIRED",
        "details": {"items": [{"storeId": "store-1", "sap": "28934"}]},
    }
    assert f"request_id={request_id}" in caplog.text
    assert "code=CORE_ITEMS_REQUIRED" in caplog.text


def test_legacy_fastapi_detail_payloads_remain_unchanged():
    app = build_test_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        string_response = client.get("/legacy-string")
        object_response = client.get("/legacy-object")

    assert_request_id(string_response)
    assert_request_id(object_response)
    assert string_response.json() == {"detail": "Legacy message"}
    assert object_response.json() == {"detail": {"field": "deliveryDate"}}


def test_unexpected_error_is_generic_and_correlated(caplog):
    app = build_test_app()
    with caplog.at_level(logging.INFO, logger="test.api.error_contract"):
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/unexpected")

    request_id = assert_request_id(response)
    assert response.status_code == 500
    assert response.json() == {
        "error": "Internal server error",
        "code": "INTERNAL_ERROR",
    }
    assert "database password" not in json.dumps(response.json())
    assert f"request_id={request_id}" in caplog.text
    assert "error_type=RuntimeError" in caplog.text

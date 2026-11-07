import logging
import unittest
from unittest.mock import patch

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from order_forecast.api.errors import install_api_error_handlers
from order_forecast.api.middleware.rate_limit import (
    limiter,
    rate_limit_accounting_export,
    rate_limit_order_read,
    rate_limit_order_update,
    setup_rate_limiting,
)
from order_forecast.api.middleware.request_context import setup_request_context
from order_forecast.api.middleware.usage_analytics import setup_usage_analytics


PRODUCTION_ORIGIN = "https://routespark.pro"


async def _test_auth(request: Request) -> dict:
    uid = str(request.headers.get("x-test-uid") or "").strip()
    request.state.usage_uid = uid
    return {"uid": uid}


def _build_app() -> FastAPI:
    app = FastAPI()

    @app.put("/api/orders/{order_id}")
    @rate_limit_order_update
    async def update_order(
        request: Request,
        order_id: str,
        _token: dict = Depends(_test_auth),
    ):
        return {"ok": True, "orderId": order_id}

    @app.get("/api/default-limited")
    async def default_limited():
        return {"ok": True}

    @app.get("/api/orders/active")
    @rate_limit_order_read
    async def active_orders(
        request: Request,
        _token: dict = Depends(_test_auth),
    ):
        return {"ok": True}

    @app.get("/api/business/accounting/handoff/export")
    @rate_limit_accounting_export
    async def accounting_export_rate_limit_contract(
        request: Request,
        _token: dict = Depends(_test_auth),
    ):
        return {"ok": True}

    # Match production's effective order: CORS must wrap SlowAPI so even a
    # middleware-level rejection is browser-readable. Request context and
    # analytics remain outside both.
    setup_rate_limiting(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[PRODUCTION_ORIGIN],
        allow_credentials=True,
        allow_methods=["GET", "PUT"],
        allow_headers=["Authorization", "Content-Type", "X-Test-Uid"],
        expose_headers=["X-Request-ID", "Retry-After"],
    )
    setup_request_context(app, logging.getLogger("test.order_save_rate_limit"))
    setup_usage_analytics(app, logging.getLogger("test.order_save_rate_limit"))
    install_api_error_handlers(
        app,
        debug_mode=False,
        app_logger=logging.getLogger("test.order_save_rate_limit"),
    )
    return app


class OrderSaveRateLimitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = _build_app()

    def setUp(self) -> None:
        limiter.reset()

    def tearDown(self) -> None:
        limiter.reset()

    def test_authenticated_order_limit_is_uid_scoped_and_records_specific_error(self):
        app = self.app
        with patch(
            "order_forecast.api.middleware.usage_analytics.enqueue_api_request"
        ) as enqueue:
            with TestClient(app, raise_server_exceptions=False) as client:
                headers = {"Origin": PRODUCTION_ORIGIN, "X-Test-Uid": "owner-a"}
                for _index in range(60):
                    response = client.put("/api/orders/order-a", headers=headers)
                    self.assertEqual(response.status_code, 200)

                rejected = client.put("/api/orders/order-a", headers=headers)
                independent = client.put(
                    "/api/orders/order-a",
                    headers={"Origin": PRODUCTION_ORIGIN, "X-Test-Uid": "owner-b"},
                )

        self.assertEqual(rejected.status_code, 429)
        self.assertEqual(rejected.json()["code"], "RATE_LIMIT_ORDER_UPDATE")
        self.assertEqual(rejected.json()["details"]["retryAfterSeconds"], 60)
        self.assertEqual(rejected.headers["retry-after"], "60")
        self.assertIn("Retry-After", rejected.headers["access-control-expose-headers"])
        self.assertTrue(rejected.headers.get("x-request-id"))
        self.assertEqual(independent.status_code, 200)

        rejected_activity = [
            call.args[0]
            for call in enqueue.call_args_list
            if call.args[0].status_code == 429
        ]
        self.assertEqual(len(rejected_activity), 1)
        self.assertEqual(rejected_activity[0].error_code, "RATE_LIMIT_ORDER_UPDATE")
        self.assertEqual(rejected_activity[0].uid, "owner-a")

    def test_middleware_default_limit_uses_async_handler_and_cors_contract(self):
        app = self.app
        with patch(
            "order_forecast.api.middleware.usage_analytics.enqueue_api_request"
        ):
            with TestClient(app, raise_server_exceptions=False) as client:
                headers = {"Origin": PRODUCTION_ORIGIN}
                for _index in range(100):
                    response = client.get("/api/default-limited", headers=headers)
                    self.assertEqual(response.status_code, 200)
                rejected = client.get("/api/default-limited", headers=headers)

        self.assertEqual(rejected.status_code, 429)
        self.assertEqual(rejected.json()["code"], "RATE_LIMIT_EXCEEDED")
        self.assertEqual(rejected.json()["details"]["retryAfterSeconds"], 60)
        self.assertEqual(rejected.headers["retry-after"], "60")
        self.assertEqual(
            rejected.headers["access-control-allow-origin"],
            PRODUCTION_ORIGIN,
        )
        self.assertIn("Retry-After", rejected.headers["access-control-expose-headers"])
        self.assertTrue(rejected.headers.get("x-request-id"))

    def test_read_bucket_does_not_consume_order_update_allowance(self):
        app = self.app
        with patch("order_forecast.api.middleware.usage_analytics.enqueue_api_request"):
            with TestClient(app, raise_server_exceptions=False) as client:
                headers = {"Origin": PRODUCTION_ORIGIN, "X-Test-Uid": "owner-read-write"}
                for _index in range(30):
                    self.assertEqual(client.get("/api/orders/active", headers=headers).status_code, 200)
                self.assertEqual(client.get("/api/orders/active", headers=headers).status_code, 429)
                for _index in range(60):
                    self.assertEqual(client.put("/api/orders/order-a", headers=headers).status_code, 200)
                self.assertEqual(client.put("/api/orders/order-a", headers=headers).status_code, 429)

    def test_accounting_export_limit_uses_contract_error_code(self):
        app = self.app
        with patch("order_forecast.api.middleware.usage_analytics.enqueue_api_request"):
            with TestClient(app, raise_server_exceptions=False) as client:
                headers = {"Origin": PRODUCTION_ORIGIN, "X-Test-Uid": "handoff-owner"}
                for _index in range(2):
                    response = client.get(
                        "/api/business/accounting/handoff/export",
                        headers=headers,
                    )
                    self.assertEqual(response.status_code, 200)
                rejected = client.get(
                    "/api/business/accounting/handoff/export",
                    headers=headers,
                )

        self.assertEqual(rejected.status_code, 429)
        self.assertEqual(rejected.json()["code"], "HANDOFF_EXPORT_RATE_LIMITED")
        self.assertEqual(rejected.json()["details"]["retryAfterSeconds"], 60)
        self.assertEqual(rejected.headers["retry-after"], "60")

    def test_blank_authenticated_identity_falls_back_to_a_real_bucket(self):
        app = self.app
        with patch("order_forecast.api.middleware.usage_analytics.enqueue_api_request"):
            with TestClient(app, raise_server_exceptions=False) as client:
                headers = {"Origin": PRODUCTION_ORIGIN}
                for _index in range(60):
                    self.assertEqual(client.put("/api/orders/order-a", headers=headers).status_code, 200)
                rejected = client.put("/api/orders/order-a", headers=headers)

        self.assertEqual(rejected.status_code, 429)
        self.assertEqual(rejected.json()["code"], "RATE_LIMIT_ORDER_UPDATE")


if __name__ == "__main__":
    unittest.main()

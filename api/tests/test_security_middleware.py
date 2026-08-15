import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta, timezone

from order_forecast.api.middleware.code_protection import BlocklistMiddleware


class _Headers(dict):
    def get(self, key, default=None):
        return super().get(key.lower(), default)


class BlocklistMiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_attempt_records_current_request_metadata(self):
        request = SimpleNamespace(
            url=SimpleNamespace(path="/api/orders"),
            method="GET",
            headers=_Headers(
                {
                    "cf-ray": "current-ray",
                    "user-agent": "current-agent",
                }
            ),
        )
        middleware = BlocklistMiddleware(app=AsyncMock())
        call_next = AsyncMock()
        original_block = {
            "reason": "honeypot",
            "last_metadata": {"cf_ray": "original-ray"},
        }

        with (
            patch(
                "order_forecast.api.middleware.code_protection.get_client_ip",
                return_value="203.0.113.50",
            ),
            patch(
                "order_forecast.api.middleware.code_protection.blocklist.get_block_info",
                return_value=original_block,
            ),
            patch(
                "order_forecast.api.middleware.code_protection.security_logger.blocked_ip_attempt"
            ) as logged,
        ):
            response = await middleware.dispatch(request, call_next)

        self.assertEqual(response.status_code, 403)
        call_next.assert_not_awaited()
        details = logged.call_args.kwargs["details"]
        self.assertEqual(details["last_metadata"]["cf_ray"], "original-ray")
        self.assertEqual(details["current_cf_ray"], "current-ray")
        self.assertEqual(details["current_user_agent"], "current-agent")

    async def test_brute_force_block_uses_cluster_wide_rate_limit_contract(self):
        request = SimpleNamespace(
            url=SimpleNamespace(path="/api/auth/verify"),
            method="POST",
            headers=_Headers({"cf-ray": "current-ray"}),
        )
        middleware = BlocklistMiddleware(app=AsyncMock())
        call_next = AsyncMock()
        until = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()

        with (
            patch(
                "order_forecast.api.middleware.code_protection.get_client_ip",
                return_value="203.0.113.51",
            ),
            patch(
                "order_forecast.api.middleware.code_protection.blocklist.get_block_info",
                return_value={
                    "reason": "brute_force_auth_failure",
                    "until": until,
                },
            ),
            patch(
                "order_forecast.api.middleware.code_protection.security_logger.blocked_ip_attempt"
            ),
        ):
            response = await middleware.dispatch(request, call_next)

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["Retry-After"], response.headers["retry-after"])
        self.assertGreaterEqual(int(response.headers["Retry-After"]), 1)
        self.assertIn(b'"code":"RATE_LIMITED"', response.body)


if __name__ == "__main__":
    unittest.main()

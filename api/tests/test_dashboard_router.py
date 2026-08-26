import inspect
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException

from order_forecast.api.routers import dashboard


class DashboardRouterAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_member_read_does_not_require_subscription_feature(self):
        handler = inspect.unwrap(dashboard.get_dashboard_summary)
        db = object()
        conn = object()
        payload = {
            "activePcfs": [],
            "expiringItems": [],
            "freshness": {"fresh": True, "source": "postgres"},
        }

        with patch.object(
            dashboard,
            "require_route_access",
            new=AsyncMock(return_value={"profile": {"routeNumber": "989262"}}),
        ) as route_access, patch.object(
            dashboard, "get_pg_connection", return_value=conn
        ), patch.object(
            dashboard, "get_dashboard_summary_payload", return_value=payload
        ) as load_payload, patch.object(
            dashboard, "return_pg_connection"
        ) as return_connection:
            result = await handler(
                request=Mock(),
                route_number="989262",
                decoded_token={"uid": "route-member"},
                db=db,
            )

        route_access.assert_awaited_once_with(
            "989262", {"uid": "route-member"}, db
        )
        load_payload.assert_called_once_with(
            db=db, conn=conn, route_number="989262"
        )
        return_connection.assert_called_once_with(conn)
        self.assertEqual(result["routeNumber"], "989262")

    async def test_route_denial_is_terminal_before_postgres_read(self):
        handler = inspect.unwrap(dashboard.get_dashboard_summary)

        with patch.object(
            dashboard,
            "require_route_access",
            new=AsyncMock(side_effect=HTTPException(403, "Access denied")),
        ), patch.object(dashboard, "get_pg_connection") as get_connection:
            with self.assertRaises(HTTPException) as raised:
                await handler(
                    request=Mock(),
                    route_number="989262",
                    decoded_token={"uid": "unrelated-user"},
                    db=object(),
                )

        self.assertEqual(raised.exception.status_code, 403)
        get_connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()

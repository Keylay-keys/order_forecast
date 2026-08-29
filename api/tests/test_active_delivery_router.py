from __future__ import annotations

import inspect
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException

from order_forecast.api.pcf_active_serving import ProjectionUnavailable
from order_forecast.api.routers import deliveries


class ActiveDeliveryRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_uses_postgres_after_route_access_without_pcf_firestore_read(self):
        handler = inspect.unwrap(deliveries.list_active_deliveries)
        db = object()
        conn = object()
        payload = {
            "items": [],
            "total": 0,
            "route": "988200",
            "source": "postgres",
        }

        with patch.object(
            deliveries,
            "require_route_access",
            new=AsyncMock(return_value={"profile": {"routeNumber": "988200"}}),
        ) as route_access, patch.object(
            deliveries, "get_pg_connection", return_value=conn
        ), patch.object(
            deliveries, "list_active_deliveries_from_postgres", return_value=payload
        ) as load_list, patch.object(
            deliveries, "return_pg_connection"
        ) as return_connection:
            result = await handler(
                request=Mock(),
                route="988200",
                decoded_token={"uid": "route-member"},
                db=db,
            )

        route_access.assert_awaited_once_with(
            "988200", {"uid": "route-member"}, db
        )
        load_list.assert_called_once_with(conn, "988200")
        return_connection.assert_called_once_with(conn)
        self.assertEqual(result["source"], "postgres")

    async def test_detail_uses_postgres_and_preserves_healthy_not_found(self):
        handler = inspect.unwrap(deliveries.get_active_delivery)
        conn = object()

        with patch.object(
            deliveries,
            "require_route_access",
            new=AsyncMock(return_value={"profile": {"routeNumber": "988200"}}),
        ), patch.object(
            deliveries, "get_pg_connection", return_value=conn
        ), patch.object(
            deliveries, "get_active_delivery_from_postgres", return_value=None
        ) as load_detail, patch.object(
            deliveries, "return_pg_connection"
        ) as return_connection:
            with self.assertRaises(HTTPException) as raised:
                await handler(
                    request=Mock(),
                    delivery_id="1806249027",
                    route="988200",
                    decoded_token={"uid": "route-member"},
                    db=object(),
                )

        self.assertEqual(raised.exception.status_code, 404)
        load_detail.assert_called_once_with(conn, "988200", "1806249027")
        return_connection.assert_called_once_with(conn)

    async def test_unavailable_projection_is_explicit_503_for_bounded_fallback(self):
        handler = inspect.unwrap(deliveries.get_active_delivery)
        conn = object()

        with patch.object(
            deliveries,
            "require_route_access",
            new=AsyncMock(return_value={"profile": {"routeNumber": "988200"}}),
        ), patch.object(
            deliveries, "get_pg_connection", return_value=conn
        ), patch.object(
            deliveries,
            "get_active_delivery_from_postgres",
            side_effect=ProjectionUnavailable("projection_health_expired"),
        ), patch.object(
            deliveries, "return_pg_connection"
        ) as return_connection:
            with self.assertRaises(HTTPException) as raised:
                await handler(
                    request=Mock(),
                    delivery_id="1806249027",
                    route="988200",
                    decoded_token={"uid": "route-member"},
                    db=object(),
                )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail,
            {
                "code": "projection_unavailable",
                "reason": "projection_health_expired",
            },
        )
        return_connection.assert_called_once_with(conn)

    async def test_route_denial_is_terminal_before_postgres(self):
        handler = inspect.unwrap(deliveries.list_active_deliveries)

        with patch.object(
            deliveries,
            "require_route_access",
            new=AsyncMock(side_effect=HTTPException(403, "Access denied")),
        ), patch.object(deliveries, "get_pg_connection") as get_connection:
            with self.assertRaises(HTTPException) as raised:
                await handler(
                    request=Mock(),
                    route="988200",
                    decoded_token={"uid": "unrelated-user"},
                    db=object(),
                )

        self.assertEqual(raised.exception.status_code, 403)
        get_connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()

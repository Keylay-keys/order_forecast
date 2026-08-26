from __future__ import annotations

import inspect
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from order_forecast.api.routers import reference


class PromoApiGateTest(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_endpoint_fails_before_route_or_promo_reads(self):
        endpoint = inspect.unwrap(reference.get_promos)

        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "false"}, clear=True),
            patch.object(reference, "require_route_access", new_callable=AsyncMock) as route_access,
        ):
            with self.assertRaises(HTTPException) as raised:
                await endpoint(
                    request=object(),
                    route="989262",
                    decoded_token={"uid": "test-user"},
                    db=object(),
                )

        self.assertEqual(raised.exception.status_code, 404)
        route_access.assert_not_awaited()

    async def test_only_literal_true_preserves_existing_endpoint(self):
        endpoint = inspect.unwrap(reference.get_promos)
        fake_db = object()
        firebase_loader = sys.modules["firebase_loader"]

        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "true"}, clear=True),
            patch.object(reference, "require_route_access", new_callable=AsyncMock) as route_access,
            patch.object(firebase_loader, "load_promotions", return_value=[{"id": "p1"}]) as load,
        ):
            response = await endpoint(
                request=object(),
                route="989262",
                decoded_token={"uid": "test-user"},
                db=fake_db,
            )

        route_access.assert_awaited_once_with("989262", {"uid": "test-user"}, fake_db)
        load.assert_called_once_with(fake_db, "989262")
        self.assertEqual(response, {"routeNumber": "989262", "promos": [{"id": "p1"}]})


if __name__ == "__main__":
    unittest.main()

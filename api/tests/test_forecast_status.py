import inspect
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from order_forecast.api.routers import forecast


class _Snapshot:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return dict(self._data or {})


def _forecast_db(status, cached):
    db = MagicMock()
    forecast_collection = MagicMock()
    forecast_document = MagicMock()
    cached_collection = MagicMock()
    db.collection.return_value = forecast_collection
    forecast_collection.document.return_value = forecast_document
    forecast_document.get.return_value = _Snapshot(status)
    forecast_document.collection.return_value = cached_collection
    cached_collection.stream.return_value = [_Snapshot(item) for item in cached]
    return db


class ForecastStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_last_order_fallback_is_available_before_training(self):
        now = datetime.now(timezone.utc)
        db = _forecast_db(
            status={
                "orderCount": 2,
                "minOrdersRequired": 7,
                "hasTrainedModel": False,
                "lastUpdated": now,
            },
            cached=[{
                "generatedAt": now,
                "expiresAt": now + timedelta(days=2),
                "items": [{"source": "last_order"}],
            }],
        )

        endpoint = inspect.unwrap(forecast.get_forecast_status)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(forecast, "_get_last_finalized_at", return_value=None):
            result = await endpoint(
                request=None,
                route="989567",
                scheduleKey=None,
                decoded_token={"uid": "owner"},
                db=db,
            )

        self.assertEqual(result.orderCount, 2)
        self.assertFalse(result.hasTrainedModel)
        self.assertTrue(result.forecastAvailable)
        self.assertEqual(result.forecastMode, "last_order")

    def test_expired_cache_is_not_available(self):
        now = datetime.now(timezone.utc)
        db = _forecast_db(
            status={},
            cached=[{
                "generatedAt": now - timedelta(days=8),
                "expiresAt": now - timedelta(days=1),
                "items": [{"source": "last_order"}],
            }],
        )

        result = forecast._get_cached_forecast_readiness(db, "989567")

        self.assertEqual(result, {"forecastAvailable": False, "forecastMode": None})


if __name__ == "__main__":
    unittest.main()

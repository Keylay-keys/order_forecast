import inspect
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from order_forecast.api.routers import forecast
from order_forecast.scripts.forecast_contract import (
    artifact_semantic_payload,
    key_fingerprint,
    stable_fingerprint,
)


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
    cached_collection.where.return_value = cached_collection
    cached_collection.stream.return_value = [_Snapshot(item) for item in cached]
    return db


class ForecastStatusTests(unittest.IsolatedAsyncioTestCase):
    def artifact(self, *, delivery_date="2026-08-13", expires_at=None):
        now = datetime.now(timezone.utc)
        keys = {("store-1", "sap-1")}
        artifact = {
            "schemaVersion": 2,
            "state": "ready",
            "forecastId": "forecast-1",
            "generationMode": "last_order",
            "routeNumber": "989567",
            "deliveryDate": delivery_date,
            "scheduleKey": "tuesday",
            "generatedAt": now,
            "publishedAt": now,
            "expiresAt": expires_at or now + timedelta(days=2),
            "generationInputFingerprint": "revision-1",
            "eligibility": {
                "activeCarryItemCount": 1,
                "emittedItemCount": 1,
                "zeroItemCount": 1,
                "activeCarryFingerprint": key_fingerprint(keys),
            },
            "items": [{
                "storeId": "store-1",
                "sap": "sap-1",
                "recommendedUnits": 0,
                "source": "dense_zero",
            }],
        }
        artifact["artifactFingerprint"] = stable_fingerprint(
            artifact_semantic_payload(artifact)
        )
        return artifact, keys

    async def test_last_order_fallback_is_available_before_training(self):
        now = datetime.now(timezone.utc)
        artifact, keys = self.artifact()
        db = _forecast_db(
            status={
                "orderCount": 2,
                "minOrdersRequired": 7,
                "hasTrainedModel": False,
                "lastUpdated": now,
            },
            cached=[artifact],
        )

        endpoint = inspect.unwrap(forecast.get_forecast_status)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(
            forecast, "forecast_reference_enabled_for_route", return_value=True
        ), patch.object(
            forecast, "_get_last_finalized_at", return_value=None
        ), patch.object(
            forecast, "load_authority_generation_state", return_value=(keys, "revision-1")
        ), patch.object(
            forecast, "get_generation_job_status", return_value=None
        ):
            result = await endpoint(
                request=None,
                route="989567",
                scheduleKey="tuesday",
                deliveryDate="2026-08-13",
                orderId=None,
                decoded_token={"uid": "owner"},
                db=db,
            )

        self.assertEqual(result.orderCount, 2)
        self.assertFalse(result.hasTrainedModel)
        self.assertTrue(result.forecastAvailable)
        self.assertEqual(result.forecastMode, "last_order")

    def test_insufficient_history_job_is_a_typed_terminal_preparation_failure(self):
        with patch.object(
            forecast,
            "get_generation_job_status",
            return_value={
                "status": "queued",
                "last_error": "insufficient_history: Only 1 orders found, need at least 4",
            },
        ):
            result = forecast._get_exact_preparation_state(
                "988200", "tuesday", "2026-08-17", False
            )

        self.assertEqual(result, {
            "status": "failed",
            "failureReason": "insufficient_history",
        })

    def test_expired_cache_is_not_available(self):
        now = datetime.now(timezone.utc)
        artifact, keys = self.artifact(expires_at=now - timedelta(days=1))
        db = _forecast_db(
            status={},
            cached=[artifact],
        )

        with patch.object(
            forecast, "load_authority_generation_state", return_value=(keys, "revision-1")
        ):
            result = forecast._get_cached_forecast_readiness(
                db, "989567", "tuesday", "2026-08-13"
            )

        self.assertEqual(result, {"forecastAvailable": False, "forecastMode": None})

    def test_schedule_only_status_is_never_attachable(self):
        artifact, _keys = self.artifact()
        db = _forecast_db(status={}, cached=[artifact])
        result = forecast._get_cached_forecast_readiness(db, "989567", "tuesday")
        self.assertEqual(result, {"forecastAvailable": False, "forecastMode": None})

    def test_same_schedule_other_date_is_not_attachable(self):
        artifact, keys = self.artifact(delivery_date="2026-08-20")
        db = _forecast_db(status={}, cached=[artifact])
        with patch.object(
            forecast, "load_authority_generation_state", return_value=(keys, "revision-1")
        ):
            result = forecast._get_cached_forecast_readiness(
                db, "989567", "tuesday", "2026-08-13"
            )
        self.assertEqual(result, {"forecastAvailable": False, "forecastMode": None})


if __name__ == "__main__":
    unittest.main()

import inspect
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from order_forecast.api.routers import forecast
from order_forecast.scripts.forecast_contract import (
    artifact_semantic_payload,
    key_fingerprint,
    stable_fingerprint,
)


class _Snapshot:
    def __init__(self, data, exists=True):
        self._data = data
        self.exists = exists

    def to_dict(self):
        return deepcopy(self._data)


class _Query:
    def __init__(self, docs):
        self.docs = docs

    def where(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def stream(self):
        return [_Snapshot(doc) for doc in self.docs]


class _Transaction:
    def __init__(self):
        self.updates = []

    def update(self, reference, fields):
        self.updates.append((reference, deepcopy(fields)))
        reference.data.update(deepcopy(fields))


class _OrderReference:
    def __init__(self, data):
        self.data = deepcopy(data)

    def get(self, transaction=None):
        return _Snapshot(self.data)


def _db(order, artifacts):
    db = MagicMock()
    order_ref = _OrderReference(order)
    route_collection = MagicMock()
    route_doc = MagicMock()
    orders_collection = MagicMock()
    forecast_collection = MagicMock()
    forecast_doc = MagicMock()
    cached = _Query(artifacts)
    audit_collection = MagicMock()
    audit_doc = MagicMock()
    transaction = _Transaction()

    route_collection.document.return_value = route_doc
    route_doc.collection.return_value = orders_collection
    orders_collection.document.return_value = order_ref
    forecast_collection.document.return_value = forecast_doc
    forecast_doc.collection.return_value = cached
    audit_collection.document.return_value = audit_doc

    def collection(name):
        if name == "routes":
            return route_collection
        if name == "forecasts":
            return forecast_collection
        if name == "orders":
            return audit_collection
        raise AssertionError(f"unexpected collection: {name}")

    db.collection.side_effect = collection
    db.transaction.return_value = transaction
    return db, order_ref, transaction


class ForecastAttachTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.keys = {("store-1", "sap-1")}
        now = datetime.now(timezone.utc)
        self.artifact = {
            "schemaVersion": 2,
            "state": "ready",
            "forecastId": "forecast-1",
            "generationMode": "model",
            "routeNumber": "988200",
            "deliveryDate": "2026-08-13",
            "scheduleKey": "tuesday",
            "generatedAt": now,
            "publishedAt": now,
            "expiresAt": now + timedelta(days=2),
            "generationInputFingerprint": "revision-1",
            "eligibility": {
                "activeCarryItemCount": 1,
                "emittedItemCount": 1,
                "zeroItemCount": 0,
                "activeCarryFingerprint": key_fingerprint(self.keys),
            },
            "items": [{
                "storeId": "store-1",
                "sap": "sap-1",
                "recommendedUnits": 8,
                "source": "model",
            }],
        }
        self.artifact["artifactFingerprint"] = stable_fingerprint(
            artifact_semantic_payload(self.artifact)
        )
        self.order = {
            "status": "draft",
            "expectedDeliveryDate": "2026-08-13",
            "scheduleKey": "tuesday",
            "stores": [{
                "storeId": "store-1",
                "storeName": "Store 1",
                "items": [{"sap": "sap-1", "quantity": 3}],
            }],
        }

    async def test_attach_persists_only_context_and_keeps_actuals_deep_equal(self):
        db, order_ref, transaction = _db(self.order, [self.artifact])
        stores_before = deepcopy(order_ref.data["stores"])
        endpoint = inspect.unwrap(forecast.attach_forecast)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(
            forecast, "forecast_reference_enabled_for_route", return_value=True
        ), patch.object(
            forecast, "load_authority_generation_state", return_value=(self.keys, "revision-1")
        ), patch.object(
            forecast.firestore, "transactional", side_effect=lambda fn: fn
        ), patch.object(forecast, "_log_order_audit"):
            result = await endpoint(
                request=None,
                order_id="draft-1",
                route="988200",
                decoded_token={"uid": "owner"},
                db=db,
            )

        self.assertEqual(order_ref.data["stores"], stores_before)
        self.assertEqual(result.forecastContext.items[0].recommendedUnits, 8)
        self.assertEqual(len(transaction.updates), 1)
        self.assertEqual(set(transaction.updates[0][1]), {"forecastContext", "updatedAt"})

    async def test_missing_exact_artifact_returns_typed_preparing_without_mutation(self):
        db, order_ref, transaction = _db(self.order, [])
        endpoint = inspect.unwrap(forecast.attach_forecast)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(
            forecast, "forecast_reference_enabled_for_route", return_value=True
        ), patch.object(
            forecast, "load_authority_generation_state", return_value=(self.keys, "revision-1")
        ), patch(
            "forecast_generation_queue.enqueue_generation_job",
            return_value={"job_key": "job-1"},
        ):
            result = await endpoint(
                request=None,
                order_id="draft-1",
                route="988200",
                decoded_token={"uid": "owner"},
                db=db,
            )

        self.assertEqual(result.status_code, 202)
        self.assertIn(b'"status":"preparing"', result.body)
        self.assertEqual(order_ref.data, self.order)
        self.assertEqual(transaction.updates, [])

    async def test_obsolete_draft_schedule_is_rejected_instead_of_preparing_forever(self):
        db, _order_ref, transaction = _db(self.order, [])
        endpoint = inspect.unwrap(forecast.attach_forecast)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(
            forecast, "forecast_reference_enabled_for_route", return_value=True
        ), patch.object(
            forecast,
            "load_authority_generation_state",
            side_effect=forecast.ForecastContractError("schedule_not_active:tuesday"),
        ):
            with self.assertRaisesRegex(Exception, "order_schedule_no_longer_active") as raised:
                await endpoint(
                    request=None,
                    order_id="draft-1",
                    route="988200",
                    decoded_token={"uid": "owner"},
                    db=db,
                )

        self.assertEqual(getattr(raised.exception, "status_code", None), 409)
        self.assertEqual(transaction.updates, [])

    async def test_cycle_without_eligible_store_items_is_rejected_before_enqueue(self):
        db, _order_ref, transaction = _db(self.order, [])
        endpoint = inspect.unwrap(forecast.attach_forecast)
        with patch.object(
            forecast, "require_route_feature_access", new=AsyncMock()
        ), patch.object(
            forecast, "forecast_reference_enabled_for_route", return_value=True
        ), patch.object(
            forecast, "load_authority_generation_state", return_value=(set(), "revision-empty")
        ), patch(
            "forecast_generation_queue.enqueue_generation_job"
        ) as enqueue:
            with self.assertRaisesRegex(Exception, "forecast_no_eligible_items") as raised:
                await endpoint(
                    request=None,
                    order_id="draft-1",
                    route="988200",
                    decoded_token={"uid": "owner"},
                    db=db,
                )

        self.assertEqual(getattr(raised.exception, "status_code", None), 409)
        enqueue.assert_not_called()
        self.assertEqual(transaction.updates, [])


if __name__ == "__main__":
    unittest.main()

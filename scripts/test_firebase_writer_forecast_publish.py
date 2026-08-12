import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from firebase_writer import write_cached_forecast
from models import ForecastItem, ForecastPayload


class _OldReference:
    def __init__(self):
        self.deleted = False

    def delete(self):
        self.deleted = True


class _OldSnapshot:
    id = "old-forecast"

    def __init__(self):
        self.reference = _OldReference()

    def to_dict(self):
        return {"schemaVersion": 2, "state": "ready"}


class _LegacySnapshot(_OldSnapshot):
    id = "legacy-v1"

    def to_dict(self):
        return {"items": [{"storeId": "store-1", "sap": "100"}]}


class _DocumentReference:
    def __init__(self, cached, document_id):
        self.cached = cached
        self.id = document_id

    def set(self, payload):
        if self.cached.fail_publish:
            raise RuntimeError("publish failed")
        self.cached.published[self.id] = payload


class _CachedCollection:
    def __init__(self, fail_publish=False):
        self.fail_publish = fail_publish
        self.published = {}
        self.old = _OldSnapshot()

    def document(self, document_id):
        return _DocumentReference(self, document_id)

    def where(self, *_args, **_kwargs):
        return self

    def stream(self):
        return [self.old]


class _DbPath:
    def __init__(self, cached):
        self.cached = cached

    def collection(self, name):
        return self.cached if name == "cached" else self

    def document(self, _name):
        return self


def _forecast():
    return ForecastPayload(
        forecast_id="forecast-1",
        route_number="988200",
        delivery_date="2026-08-13",
        schedule_key="tuesday",
        generated_at=datetime(2026, 8, 11, 12, 0, 0),
        generation_mode="last_order",
        generation_input_fingerprint="revision-1",
        items=[ForecastItem(
            store_id="store-1",
            store_name="Store One",
            sap="100",
            recommended_units=0,
            recommended_cases=0,
            source="dense_zero",
        )],
    )


class FirebaseWriterForecastPublishTests(unittest.TestCase):
    def test_archive_and_cache_use_one_canonical_payload_before_cleanup(self):
        cached = _CachedCollection()
        db = _DbPath(cached)
        with tempfile.TemporaryDirectory() as archive_root, patch.dict(
            os.environ, {"ROUTESPARK_FORECAST_ARCHIVE_DIR": archive_root}
        ):
            timings = write_cached_forecast(db, "988200", _forecast())
            archive_path = os.path.join(
                archive_root, "988200", "2026-08-13", "tuesday", "forecast-1.json"
            )
            with open(archive_path, "r", encoding="utf-8") as handle:
                archived = json.load(handle)

        published = cached.published["forecast-1"]
        self.assertEqual(archived["artifactFingerprint"], published["artifactFingerprint"])
        self.assertEqual(archived["generationMode"], "last_order")
        self.assertEqual(archived["items"], published["items"])
        self.assertTrue(cached.old.reference.deleted)
        self.assertEqual(set(timings), {
            "denseValidationMs",
            "archiveWriteVerificationMs",
            "firestorePublicationMs",
            "publisherTotalMs",
        })

    def test_failed_publication_never_deletes_previous_exact_artifact(self):
        cached = _CachedCollection(fail_publish=True)
        db = _DbPath(cached)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROUTESPARK_FORECAST_ARCHIVE_DIR", None)
            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                write_cached_forecast(db, "988200", _forecast())
        self.assertFalse(cached.old.reference.deleted)

    def test_safe_replacement_never_deletes_sparse_v1_artifact(self):
        cached = _CachedCollection()
        cached.old = _LegacySnapshot()
        db = _DbPath(cached)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROUTESPARK_FORECAST_ARCHIVE_DIR", None)
            write_cached_forecast(db, "988200", _forecast())
        self.assertFalse(cached.old.reference.deleted)


if __name__ == "__main__":
    unittest.main()

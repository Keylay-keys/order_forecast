import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import forecast_generation_queue as queue


class _Doc:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return dict(self._data)


class _Query:
    def __init__(self, docs):
        self.docs = docs

    def where(self, **_kwargs):
        return self

    def stream(self):
        return self.docs


class _Db:
    def __init__(self, docs):
        self.query = _Query([_Doc(doc) for doc in docs])

    def collection(self, _name):
        return self

    def document(self, _name):
        return self

    def where(self, **_kwargs):
        return self.query


class ForecastGenerationFreshnessV2Test(unittest.TestCase):
    def evaluate(self, artifact, desired_revision=None):
        with patch.object(queue, "_get_route_last_finalized_at", return_value=None):
            return queue._evaluate_job_freshness(
                _Db([artifact]), "988200", "2026-08-13", "tuesday",
                desired_revision=desired_revision,
            )

    def test_schema_less_artifact_is_never_fresh(self):
        fresh, reason = self.evaluate({
            "generatedAt": datetime.now(timezone.utc),
            "expiresAt": datetime.now(timezone.utc) + timedelta(days=1),
        })
        self.assertFalse(fresh)
        self.assertEqual(reason, "unsupported_or_unready")

    def test_revision_mismatch_is_not_fresh(self):
        fresh, reason = self.evaluate({
            "schemaVersion": 2,
            "state": "ready",
            "generationInputFingerprint": "old",
            "publishedAt": datetime.now(timezone.utc),
            "expiresAt": datetime.now(timezone.utc) + timedelta(days=1),
        }, desired_revision="new")
        self.assertFalse(fresh)
        self.assertEqual(reason, "generation_input_changed")


if __name__ == "__main__":
    unittest.main()

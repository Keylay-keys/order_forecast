import unittest
from unittest.mock import MagicMock, patch

import forecast_v2_cutover as cutover


class _Snapshot:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return dict(self._data)


class _Query:
    def __init__(self, docs):
        self.docs = docs

    def where(self, *_args, **_kwargs):
        return self

    def stream(self):
        return [_Snapshot(doc) for doc in self.docs]


class ForecastV2CutoverTests(unittest.TestCase):
    def test_active_targets_union_drafts_with_upcoming_schedules(self):
        db = MagicMock()
        query = _Query([{
            "status": "draft",
            "expectedDeliveryDate": "2026-08-13",
            "scheduleKey": "tuesday",
        }])
        db.collection.return_value.document.return_value.collection.return_value = query
        with patch.object(cutover, "derive_upcoming_generation_targets", return_value=[{
            "delivery_date": "2026-08-17",
            "schedule_key": "friday",
        }]):
            self.assertEqual(cutover.active_targets(db, "988200"), [
                ("2026-08-13", "tuesday"),
                ("2026-08-17", "friday"),
            ])

    def test_inspect_never_relabels_sparse_v1_and_reports_unverified(self):
        db = MagicMock()
        query = _Query([{
            "schemaVersion": 1,
            "deliveryDate": "2026-08-13",
            "scheduleKey": "tuesday",
            "items": [],
        }])
        db.collection.return_value.document.return_value.collection.return_value = query
        with patch.object(
            cutover, "load_authority_generation_state", return_value=({("s", "p")}, "rev")
        ):
            result = cutover.inspect_target(db, "988200", "2026-08-13", "tuesday")

        self.assertFalse(result["verified"])
        self.assertIsNone(result["readyForecastId"])
        self.assertIn("unsupported_or_unready_forecast", result["errors"])
        db.set.assert_not_called()
        db.delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()

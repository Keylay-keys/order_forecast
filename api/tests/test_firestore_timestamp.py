from __future__ import annotations

from datetime import datetime, timezone
import unittest

from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.protobuf.timestamp_pb2 import Timestamp

from order_forecast.api.firestore_timestamp import timestamp_ms


class MillisValue:
    def to_millis(self):
        return 1234


class FirestoreTimestampTests(unittest.TestCase):
    def test_method_and_mapping_shapes(self):
        self.assertEqual(timestamp_ms(MillisValue()), 1234)
        self.assertEqual(timestamp_ms({"seconds": 1, "nanoseconds": 234_999_999}), 1234)

    def test_microsecond_datetime_does_not_drop_milliseconds(self):
        value = datetime(2026, 8, 28, 12, 0, 0, 987654, tzinfo=timezone.utc)
        self.assertEqual(timestamp_ms(value) % 1000, 987)

    def test_full_nanoseconds_match_firestore_to_millis_semantics(self):
        proto = Timestamp(seconds=1_800_000_000, nanos=987_654_321)
        value = DatetimeWithNanoseconds.from_timestamp_pb(proto)
        self.assertEqual(timestamp_ms(value), 1_800_000_000_987)


if __name__ == "__main__":
    unittest.main()

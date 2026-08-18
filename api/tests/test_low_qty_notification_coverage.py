import unittest

from order_forecast.scripts.low_qty_notification_coverage import compare_snapshots


class LowQtyNotificationCoverageTests(unittest.TestCase):
    def test_exact_normalized_coverage_is_ready(self):
        firebase = {
            "961825": {
                "owner_uid": "owner-a",
                "reminder_minute_local": 480,
                "timezone": "America/Denver",
            },
            "985957": {
                "owner_uid": "owner-b",
                "reminder_minute_local": 555,
                "timezone": "America/Chicago",
            },
        }
        postgres = [
            {"route_number": route_number, **values}
            for route_number, values in reversed(list(firebase.items()))
        ]

        result = compare_snapshots(firebase, postgres)

        self.assertTrue(result["ready"])
        self.assertEqual(result["focus_routes"], {"961825": "match", "985957": "match"})

    def test_mismatch_output_omits_owner_values(self):
        firebase = {
            "989262": {
                "owner_uid": "firebase-owner-secret",
                "reminder_minute_local": 480,
                "timezone": "America/Denver",
            }
        }
        postgres = [
            {
                "route_number": "989262",
                "owner_uid": "postgres-owner-secret",
                "reminder_minute_local": 540,
                "timezone": "America/Denver",
            }
        ]

        result = compare_snapshots(firebase, postgres, focus_routes=())

        self.assertFalse(result["ready"])
        self.assertEqual(
            result["field_mismatches"],
            {"989262": ["owner_uid", "reminder_minute_local"]},
        )
        self.assertNotIn("firebase-owner-secret", str(result))
        self.assertNotIn("postgres-owner-secret", str(result))


if __name__ == "__main__":
    unittest.main()

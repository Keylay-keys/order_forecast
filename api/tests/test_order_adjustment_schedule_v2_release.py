from __future__ import annotations

import unittest

from order_forecast.api.order_adjustment_schedule_v2_release import (
    MIN_CLIENT_VERSION_ENV,
    REQUIRED_CAPABILITY_ENV,
    STAMP_WRITES_ENV,
    WORKER_COMPATIBLE_ENV,
    OrderAdjustmentScheduleV2ReleaseControls,
    request_client_version,
    request_capabilities,
    stamp_write_allowed_for_headers,
)


class OrderAdjustmentScheduleV2ReleaseTests(unittest.TestCase):
    def controls(self, **overrides: str) -> OrderAdjustmentScheduleV2ReleaseControls:
        environment = {
            STAMP_WRITES_ENV: "false",
            WORKER_COMPATIBLE_ENV: "false",
            MIN_CLIENT_VERSION_ENV: "1.1.7",
            REQUIRED_CAPABILITY_ENV: "scheduled-order-day-v2",
            **overrides,
        }
        return OrderAdjustmentScheduleV2ReleaseControls.from_environment(environment)

    def test_defaults_fail_closed(self):
        controls = OrderAdjustmentScheduleV2ReleaseControls.from_environment({})
        self.assertFalse(controls.stamp_write_allowed("1.1.7", {"scheduled-order-day-v2"}))
        self.assertFalse(controls.diagnostics()["minimumClientVersionValid"])

    def test_worker_compatibility_is_required_even_when_gate_is_open(self):
        controls = self.controls(**{STAMP_WRITES_ENV: "true"})
        self.assertFalse(controls.stamp_write_allowed("1.1.7", {"scheduled-order-day-v2"}))

    def test_unknown_and_old_clients_fail_closed(self):
        controls = self.controls(
            **{STAMP_WRITES_ENV: "true", WORKER_COMPATIBLE_ENV: "true"}
        )
        capability = {"scheduled-order-day-v2"}
        self.assertFalse(controls.stamp_write_allowed(None, capability))
        self.assertFalse(controls.stamp_write_allowed("unknown", capability))
        self.assertFalse(controls.stamp_write_allowed("1.1.6", capability))

    def test_equal_and_newer_numeric_versions_pass(self):
        controls = self.controls(
            **{STAMP_WRITES_ENV: "true", WORKER_COMPATIBLE_ENV: "true"}
        )
        capability = {"scheduled-order-day-v2"}
        self.assertTrue(controls.stamp_write_allowed("1.1.7", capability))
        self.assertTrue(controls.stamp_write_allowed("1.2", capability))

    def test_same_version_without_ota_capability_fails_closed(self):
        controls = self.controls(
            **{STAMP_WRITES_ENV: "true", WORKER_COMPATIBLE_ENV: "true"}
        )
        self.assertFalse(controls.stamp_write_allowed("1.1.7", set()))
        self.assertFalse(controls.stamp_write_allowed("1.1.7", {"some-other-capability"}))

    def test_invalid_configured_minimum_fails_closed(self):
        controls = self.controls(
            **{
                STAMP_WRITES_ENV: "true",
                WORKER_COMPATIBLE_ENV: "true",
                MIN_CLIENT_VERSION_ENV: "latest",
            }
        )
        self.assertFalse(
            controls.stamp_write_allowed("999.0.0", {"scheduled-order-day-v2"})
        )

    def test_missing_or_invalid_required_capability_fails_closed(self):
        for configured in ("", "Scheduled Order Day V2", "bad/capability"):
            controls = self.controls(
                **{
                    STAMP_WRITES_ENV: "true",
                    WORKER_COMPATIBLE_ENV: "true",
                    REQUIRED_CAPABILITY_ENV: configured,
                }
            )
            self.assertFalse(
                controls.stamp_write_allowed("1.1.7", {"scheduled-order-day-v2"})
            )

    def test_request_header_gate_uses_canonical_release_header(self):
        environment = {
            STAMP_WRITES_ENV: "true",
            WORKER_COMPATIBLE_ENV: "true",
            MIN_CLIENT_VERSION_ENV: "1.1.7",
            REQUIRED_CAPABILITY_ENV: "scheduled-order-day-v2",
        }
        headers = {
            "x-routespark-app-version": "1.1.7",
            "x-routespark-capabilities": "other, scheduled-order-day-v2",
        }
        self.assertEqual(request_client_version(headers), "1.1.7")
        self.assertEqual(
            request_capabilities(headers),
            {"other", "scheduled-order-day-v2"},
        )
        self.assertTrue(stamp_write_allowed_for_headers(headers, environment))
        self.assertFalse(stamp_write_allowed_for_headers({}, environment))

    def test_malformed_request_capabilities_are_ignored(self):
        environment = {
            STAMP_WRITES_ENV: "true",
            WORKER_COMPATIBLE_ENV: "true",
            MIN_CLIENT_VERSION_ENV: "1.1.7",
            REQUIRED_CAPABILITY_ENV: "scheduled-order-day-v2",
        }
        headers = {
            "x-routespark-app-version": "1.1.7",
            "x-routespark-capabilities": "scheduled order day v2,bad/slash",
        }
        self.assertEqual(request_capabilities(headers), set())
        self.assertFalse(stamp_write_allowed_for_headers(headers, environment))


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest.mock import patch

from forecast_reference_rollout import forecast_reference_enabled_for_route


class ForecastReferenceRolloutTests(unittest.TestCase):
    def test_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(forecast_reference_enabled_for_route("988200"))

    def test_global_enable_can_be_scoped_to_routes(self):
        with patch.dict(os.environ, {
            "FORECAST_REFERENCE_ATTACH_ENABLED": "true",
            "FORECAST_REFERENCE_ATTACH_ROUTES": "988200,989262",
        }, clear=True):
            self.assertTrue(forecast_reference_enabled_for_route("988200"))
            self.assertFalse(forecast_reference_enabled_for_route("999999"))


if __name__ == "__main__":
    unittest.main()

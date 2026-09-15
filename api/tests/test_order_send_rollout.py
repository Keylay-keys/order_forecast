import os
import unittest
from unittest.mock import patch

from order_forecast.api import order_send_rollout as rollout


class RolloutTests(unittest.TestCase):
    def test_default_off_and_independent_kill_switches(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(rollout.route_enabled('989262', 'FEATURE_ENABLED', 'FEATURE_ROUTES'))
        with patch.dict(os.environ, {rollout.MODE_ENV: 'all_authorized', 'FIRST_ENABLED': 'true'}, clear=True):
            self.assertTrue(rollout.route_enabled('989262', 'FIRST_ENABLED', 'FIRST_ROUTES'))
            self.assertFalse(rollout.route_enabled('989262', 'SECOND_ENABLED', 'SECOND_ROUTES'))

    def test_all_authorized_has_no_frozen_route_list(self):
        with patch.dict(os.environ, {rollout.MODE_ENV: 'all_authorized', 'FEATURE_ENABLED': 'true'}, clear=True):
            for route in ('989262', '961767', '989567', '1234567890'):
                self.assertTrue(rollout.route_enabled(route, 'FEATURE_ENABLED', 'FEATURE_ROUTES'))
            self.assertEqual(rollout.pilot_routes('FEATURE_ENABLED', 'FEATURE_ROUTES', 10), ())
            for route in ('*', '', '１２３', '../989262', 989262, True):
                self.assertFalse(rollout.route_enabled(route, 'FEATURE_ENABLED', 'FEATURE_ROUTES'))

    def test_ambiguous_or_malformed_config_never_enables(self):
        for mode, enabled, routes in [('ALL', 'true', ''), ('', 'true', ''),
                                     ('all_authorized', 'TRUE', ''), ('all_authorized', 'true', '989262'),
                                     ('pilot', 'true', '*'), ('pilot', 'true', '989262,989262'),
                                     ('pilot', 'true', '989262, 989263')]:
            with patch.dict(os.environ, {rollout.MODE_ENV: mode, 'FEATURE_ENABLED': enabled,
                                         'FEATURE_ROUTES': routes}, clear=True):
                self.assertFalse(rollout.route_enabled('989262', 'FEATURE_ENABLED', 'FEATURE_ROUTES'))

    def test_existing_pilot_is_still_bounded(self):
        with patch.dict(os.environ, {'FEATURE_ENABLED': 'true', 'FEATURE_ROUTES': '989262,989263'}, clear=True):
            self.assertTrue(rollout.route_enabled('989262', 'FEATURE_ENABLED', 'FEATURE_ROUTES'))
            self.assertFalse(rollout.route_enabled('989264', 'FEATURE_ENABLED', 'FEATURE_ROUTES'))
            self.assertFalse(rollout.route_enabled('989262', 'FEATURE_ENABLED', 'FEATURE_ROUTES', 1))


if __name__ == '__main__':
    unittest.main()

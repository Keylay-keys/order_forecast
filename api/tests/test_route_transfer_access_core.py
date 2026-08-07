import unittest

from order_forecast.api.route_transfer_access import (
    parse_route_allowlist,
    team_member_transfers_enabled_for,
)


class RouteTransferAccessCoreTests(unittest.TestCase):
    def test_normalizes_valid_routes_and_wildcard(self):
        self.assertEqual(
            parse_route_allowlist(" 988200,not-a-route,*,988200 "),
            frozenset({"988200", "*"}),
        )

    def test_disabled_flag_denies_even_allowlisted_route(self):
        self.assertFalse(
            team_member_transfers_enabled_for(
                "988200", enabled=False, allowlist="988200"
            )
        )

    def test_empty_allowlist_denies_when_enabled(self):
        self.assertFalse(
            team_member_transfers_enabled_for("988200", enabled=True, allowlist="")
        )

    def test_exact_route_and_wildcard_can_enable(self):
        self.assertTrue(
            team_member_transfers_enabled_for(
                "988200", enabled=True, allowlist="988200"
            )
        )
        self.assertTrue(
            team_member_transfers_enabled_for(
                "989262", enabled=True, allowlist="*"
            )
        )

    def test_other_or_invalid_route_remains_denied(self):
        self.assertFalse(
            team_member_transfers_enabled_for(
                "989262", enabled=True, allowlist="988200"
            )
        )
        self.assertFalse(
            team_member_transfers_enabled_for(
                "invalid", enabled=True, allowlist="*"
            )
        )


if __name__ == "__main__":
    unittest.main()

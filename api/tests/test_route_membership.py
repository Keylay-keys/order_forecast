import unittest

from order_forecast.api.route_membership import (
    query_team_member_documents,
    team_member_role_field_path,
)


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def stream(self):
        return iter(self.rows)


class _Users:
    def __init__(self, rows):
        self.rows = rows
        self.filter = None

    def where(self, *, filter):
        self.filter = filter
        return _Query(self.rows)


class _DB:
    def __init__(self, rows):
        self.users = _Users(rows)

    def collection(self, name):
        if name != "users":
            raise AssertionError(name)
        return self.users


class RouteMembershipTests(unittest.TestCase):
    def test_numeric_route_is_safely_quoted_as_a_field_segment(self):
        self.assertEqual(
            team_member_role_field_path("989262"),
            "routeAssignments.`989262`.role",
        )

    def test_invalid_route_fails_before_query(self):
        for route in ("", "989262.extra", "-1", "12345678901"):
            with self.subTest(route=route):
                with self.assertRaises(ValueError):
                    team_member_role_field_path(route)

    def test_query_uses_one_exact_team_member_filter(self):
        rows = [object()]
        db = _DB(rows)

        self.assertEqual(query_team_member_documents(db, "989262"), rows)
        self.assertEqual(db.users.filter.field_path, "routeAssignments.`989262`.role")
        self.assertEqual(db.users.filter.op_string, "==")
        self.assertEqual(db.users.filter.value, "team_member")


if __name__ == "__main__":
    unittest.main()

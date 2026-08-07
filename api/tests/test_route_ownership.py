import inspect
import unittest

from order_forecast.api.route_ownership import (
    extract_owned_routes_for_owner,
    normalize_route,
)
from order_forecast.api.routers import auth
from order_forecast.api.tests.route_transfer_fakes import FakeFirestore


class CountingFirestore(FakeFirestore):
    def __init__(self, documents=None):
        super().__init__(documents)
        self.document_reads = 0

    def get_document(self, path):
        self.document_reads += 1
        return super().get_document(path)


class RouteOwnershipTests(unittest.IsolatedAsyncioTestCase):
    def test_extracts_only_owned_routes_and_keeps_primary_separate(self):
        user_data = {
            "profile": {
                "role": "owner",
                "routeNumber": "989567",
                "additionalRoutes": ["988200", "989567", "bad-route"],
            },
            "routeAssignments": {
                "961825": {"role": "owner"},
                "777777": {"role": "team_member"},
            },
        }

        self.assertEqual(normalize_route(user_data["profile"]["routeNumber"]), "989567")
        self.assertEqual(
            extract_owned_routes_for_owner(user_data),
            ["961825", "988200", "989567"],
        )

    def test_team_member_assignments_are_never_owned_routes(self):
        user_data = {
            "profile": {"role": "team_member", "routeNumber": "988200"},
            "routeAssignments": {
                "988200": {"role": "team_member"},
                "989567": {"role": "owner"},
            },
        }
        self.assertEqual(extract_owned_routes_for_owner(user_data), [])

    async def test_auth_verify_adds_owner_metadata_without_an_extra_read(self):
        uid = "owner-user"
        db = CountingFirestore({
            f"users/{uid}": {
                "profile": {
                    "role": "owner",
                    "routeNumber": "989567",
                    "currentRoute": "988200",
                    "additionalRoutes": ["988200"],
                },
                "routeAssignments": {
                    "961825": {"role": "owner"},
                    "777777": {"role": "team_member"},
                },
            }
        })

        result = await inspect.unwrap(auth.verify_token)(
            request=None,
            decoded_token={"uid": uid, "email": "owner@example.test"},
            db=db,
        )

        self.assertEqual(db.document_reads, 1)
        self.assertEqual(result.ownedPrimaryRoute, "989567")
        self.assertEqual(result.ownedRoutes, ["961825", "988200", "989567"])
        self.assertEqual(result.currentRoute, "989567")
        self.assertIn("777777", result.routes)

    async def test_auth_verify_keeps_team_member_owner_metadata_empty(self):
        uid = "member-user"
        db = CountingFirestore({
            f"users/{uid}": {
                "profile": {
                    "role": "team_member",
                    "routeNumber": "988200",
                },
                "routeAssignments": {
                    "988200": {"role": "team_member"},
                    "989567": {"role": "team_member"},
                },
            }
        })

        result = await inspect.unwrap(auth.verify_token)(
            request=None,
            decoded_token={"uid": uid},
            db=db,
        )

        self.assertEqual(db.document_reads, 1)
        self.assertIsNone(result.ownedPrimaryRoute)
        self.assertEqual(result.ownedRoutes, [])


if __name__ == "__main__":
    unittest.main()

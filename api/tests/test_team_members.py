import inspect
import unittest
from unittest.mock import patch

from order_forecast.api.routers import team


class _Snapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _Document:
    def __init__(self, collection, doc_id):
        self.collection = collection
        self.doc_id = doc_id

    def get(self):
        return _Snapshot(self.doc_id, self.collection.data.get(self.doc_id))


class _Collection:
    def __init__(self, data):
        self.data = data

    def document(self, doc_id):
        return _Document(self, doc_id)

    def stream(self):
        return [_Snapshot(doc_id, data) for doc_id, data in self.data.items()]


class _DB:
    def __init__(self, users):
        self.users = users

    def collection(self, name):
        if name != "users":
            raise AssertionError(f"Unexpected collection: {name}")
        return _Collection(self.users)


def _owner(roster):
    return {
        "profile": {"role": "owner", "routeNumber": "989262"},
        "business": {"team": {"members": roster, "hasTeam": bool(roster)}},
        "routeAssignments": {"989262": {"role": "owner", "verified": True}},
    }


def _member(assignments):
    return {
        "profile": {
            "role": "teamMember",
            "email": "member@example.com",
            "personalName": "Test Member",
        },
        "routeAssignments": assignments,
        "trialStatus": {
            "features": {"scanner": True, "managementDashboard": False},
        },
    }


class TeamMemberListTests(unittest.IsolatedAsyncioTestCase):
    async def test_roster_member_with_no_routes_remains_visible(self):
        owner = _owner([
            {
                "uid": "member-1",
                "email": "legacy@example.com",
                "name": "Legacy Name",
                "verified": True,
            }
        ])
        db = _DB({"owner-1": owner, "member-1": _member({})})

        with patch.object(team, "require_route_access", return_value=owner):
            result = await inspect.unwrap(team.list_team_members_for_route)(
                request=None,
                route="989262",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertEqual(result["pending"], [])
        self.assertEqual(len(result["approved"]), 1)
        self.assertEqual(result["approved"][0]["uid"], "member-1")
        self.assertEqual(result["approved"][0]["assignedRoutes"], [])
        self.assertEqual(result["approved"][0]["email"], "member@example.com")

    async def test_canonical_uid_roster_and_legacy_route_fallback_are_deduplicated(self):
        owner = _owner(["member-1"])
        assignment = {
            "989262": {
                "role": "team_member",
                "verified": True,
                "assignedTo": "owner-1",
            }
        }
        db = _DB({"owner-1": owner, "member-1": _member(assignment)})

        with patch.object(team, "require_route_access", return_value=owner):
            result = await inspect.unwrap(team.list_team_members_for_route)(
                request=None,
                route="989262",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertEqual(len(result["approved"]), 1)
        self.assertEqual(result["approved"][0]["assignedRoutes"], ["989262"])

    async def test_assignment_owned_by_someone_else_is_not_exposed(self):
        owner = _owner(["member-1"])
        assignment = {
            "989262": {
                "role": "team_member",
                "verified": True,
                "assignedTo": "other-owner",
            }
        }
        db = _DB({"owner-1": owner, "member-1": _member(assignment)})

        with patch.object(team, "require_route_access", return_value=owner):
            result = await inspect.unwrap(team.list_team_members_for_route)(
                request=None,
                route="989262",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertEqual(len(result["approved"]), 1)
        self.assertEqual(result["approved"][0]["assignedRoutes"], [])


if __name__ == "__main__":
    unittest.main()

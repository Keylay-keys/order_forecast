import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from order_forecast.api import dependencies


class _FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _FakeDocument:
    def __init__(self, data, key):
        self._data = data
        self._key = key

    def get(self):
        return _FakeSnapshot(self._data.get(self._key))


class _FakeCollection:
    def __init__(self, data):
        self._data = data

    def document(self, key):
        return _FakeDocument(self._data, key)


class _FakeDB:
    def __init__(self, collections):
        self._collections = collections

    def collection(self, name):
        return _FakeCollection(self._collections.setdefault(name, {}))


class EntitlementAlignmentTests(unittest.TestCase):
    def test_allowlisted_apple_sandbox_route_can_use_entitled_feature(self):
        db = _FakeDB(
            {
                "routeEntitlements": {
                    "900000": {
                        "active": True,
                        "provider": "apple",
                        "appleEnvironment": "Sandbox",
                        "ownerUid": "apple-review-owner",
                        "features": {"managementDashboard": True},
                    }
                }
            }
        )

        with patch.object(dependencies, "APPLE_SANDBOX_BILLING_ALLOWED_ROUTES", {"900000"}):
            self.assertTrue(
                dependencies._has_route_entitlement_feature(
                    db=db,
                    route_number="900000",
                    feature_key="managementDashboard",
                )
            )

    def test_unlisted_apple_sandbox_route_remains_blocked(self):
        db = _FakeDB(
            {
                "routeEntitlements": {
                    "961767": {
                        "active": True,
                        "provider": "apple",
                        "appleEnvironment": "Sandbox",
                        "ownerUid": "sandbox-owner",
                        "features": {"managementDashboard": True},
                    }
                }
            }
        )

        with patch.object(dependencies, "APPLE_SANDBOX_BILLING_ALLOWED_ROUTES", {"900000"}), patch.object(
            dependencies,
            "APPLE_SANDBOX_BILLING_ALLOWED_UIDS",
            set(),
        ):
            self.assertFalse(
                dependencies._has_route_entitlement_feature(
                    db=db,
                    route_number="961767",
                    feature_key="managementDashboard",
                )
            )

    def test_trial_feature_uses_current_route_fallback(self):
        owner_data = {
            "profile": {
                "routeNumber": "111111",
                "currentRoute": "222222",
            },
            "trialStatus": {
                "endsAt": datetime.now(timezone.utc) + timedelta(days=7),
                "features": {
                    "scanner": True,
                    "managementDashboard": True,
                    "multiRoute": True,
                },
            },
        }

        self.assertTrue(
            dependencies._has_trial_feature(
                route_number="222222",
                owner_data=owner_data,
                feature_key="scanner",
            )
        )
        self.assertTrue(
            dependencies._has_trial_feature(
                route_number="222222",
                owner_data=owner_data,
                feature_key="ordering",
            )
        )
        self.assertFalse(
            dependencies._has_trial_feature(
                route_number="111111",
                owner_data=owner_data,
                feature_key="scanner",
            )
        )

    def test_requester_direct_grant_preserves_team_member_schema(self):
        requester_data = {
            "profile": {
                "role": "teamMember",
                "currentRoute": "989262",
            },
            "routeAssignments": {
                "989262": {
                    "role": "team_member",
                    "assignedTo": "owner-1",
                }
            },
            "trialStatus": {
                "features": {
                    "scanner": True,
                    "managementDashboard": True,
                }
            },
        }

        self.assertTrue(
            dependencies._has_requester_direct_feature_grant(
                route_number="989262",
                requester_data=requester_data,
                feature_key="scanner",
            )
        )
        self.assertTrue(
            dependencies._has_requester_direct_feature_grant(
                route_number="989262",
                requester_data=requester_data,
                feature_key="ordering",
            )
        )
        self.assertTrue(
            dependencies._has_requester_direct_feature_grant(
                route_number="989262",
                requester_data=requester_data,
                feature_key="managementDashboard",
            )
        )
        self.assertFalse(
            dependencies._has_requester_direct_feature_grant(
                route_number="989262",
                requester_data=requester_data,
                feature_key="multiRoute",
            )
        )

    def test_route_owner_resolution_falls_back_to_route_numbers_variants(self):
        db = _FakeDB(
            {
                "routes": {},
                "routeEntitlements": {},
                "routeNumbers": {
                    "960581": {"userID": "owner-from-route-number"},
                },
            }
        )

        owner_uid = dependencies._resolve_owner_uid_for_route(
            db=db,
            route_number="960581",
            requester_uid="requester-1",
            requester_data={},
        )

        self.assertEqual(owner_uid, "owner-from-route-number")

    def test_route_feature_entitlement_allows_team_member_direct_grant_without_entitlement_doc(self):
        db = _FakeDB(
            {
                "routes": {
                    "989262": {"ownerUid": "owner-1"},
                },
                "routeEntitlements": {},
                "routeNumbers": {},
                "users": {
                    "owner-1": {},
                },
            }
        )
        requester_data = {
            "profile": {
                "role": "teamMember",
                "currentRoute": "989262",
            },
            "routeAssignments": {
                "989262": {
                    "role": "team_member",
                    "assignedTo": "owner-1",
                }
            },
            "trialStatus": {
                "features": {
                    "scanner": True,
                }
            },
        }

        self.assertTrue(
            dependencies._has_route_feature_entitlement(
                db=db,
                route_number="989262",
                feature_key="ordering",
                requester_uid="member-1",
                requester_data=requester_data,
            )
        )


if __name__ == "__main__":
    unittest.main()

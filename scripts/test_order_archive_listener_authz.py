from __future__ import annotations

import unittest
from unittest.mock import patch

try:
    from . import order_archive_listener as listener
except ImportError:
    import order_archive_listener as listener


class _Snapshot:
    def __init__(self, data: dict | None):
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return self._data


class _UserDocument:
    def __init__(self, data: dict | None):
        self._data = data

    def get(self):
        return _Snapshot(self._data)


class _UsersCollection:
    def __init__(self, users: dict[str, dict]):
        self._users = users

    def document(self, user_id: str):
        return _UserDocument(self._users.get(user_id))


class _FirestoreClient:
    def __init__(self, users: dict[str, dict]):
        self._users = users

    def collection(self, name: str):
        if name != "users":
            raise AssertionError(f"unexpected collection: {name}")
        return _UsersCollection(self._users)


class _RequestDocument:
    id = "archive-test-request"

    def __init__(self):
        self.updates: list[dict] = []

    def update(self, values: dict):
        self.updates.append(values)


class OrderArchiveListenerAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.owner = {
            "profile": {
                "role": "owner",
                "routeNumber": "988200",
                "currentRoute": "988200",
                "additionalRoutes": ["989262"],
            },
            "routeAssignments": {},
        }
        self.member = {
            "profile": {"role": "teamMember"},
            "routeAssignments": {"961038": {"role": "team_member"}},
        }

    def test_owner_and_team_membership_are_accepted(self):
        self.assertTrue(listener._user_has_route_access(self.owner, "988200"))
        self.assertTrue(listener._user_has_route_access(self.owner, "989262"))
        self.assertTrue(listener._user_has_route_access(self.member, "961038"))

    def test_cross_route_access_is_rejected(self):
        self.assertFalse(listener._user_has_route_access(self.owner, "999999"))
        self.assertFalse(listener._user_has_route_access(self.member, "988200"))
        self.assertFalse(listener._user_has_route_access({}, "988200"))

    @patch.object(listener, "handle_list_dates")
    def test_listener_rejects_cross_route_request_before_postgres(self, list_dates):
        client = _FirestoreClient({"owner-1": self.owner})
        request_doc = _RequestDocument()

        accepted = listener.handle_request(
            request_doc,
            {
                "requestId": request_doc.id,
                "type": "list_dates",
                "routeNumber": "999999",
                "userId": "owner-1",
                "status": "pending",
            },
            client,
        )

        self.assertFalse(accepted)
        list_dates.assert_not_called()
        self.assertEqual(request_doc.updates[-1]["status"], "error")
        self.assertEqual(request_doc.updates[-1]["error"], "Archive request is not authorized")

    @patch.object(listener, "handle_list_dates")
    def test_listener_rejects_missing_user_before_postgres(self, list_dates):
        client = _FirestoreClient({})
        request_doc = _RequestDocument()

        accepted = listener.handle_request(
            request_doc,
            {
                "requestId": request_doc.id,
                "type": "list_dates",
                "routeNumber": "988200",
                "userId": "missing-user",
                "status": "pending",
            },
            client,
        )

        self.assertFalse(accepted)
        list_dates.assert_not_called()
        self.assertEqual(request_doc.updates[-1]["status"], "error")

    @patch.object(listener, "handle_get_order", return_value={"order": {"id": "order-1"}})
    def test_authorized_listener_forwards_canonical_order_id(self, get_order):
        client = _FirestoreClient({"owner-1": self.owner})
        request_doc = _RequestDocument()

        accepted = listener.handle_request(
            request_doc,
            {
                "requestId": request_doc.id,
                "type": "get_order",
                "routeNumber": "988200",
                "userId": "owner-1",
                "orderId": "order-1",
                "status": "pending",
            },
            client,
        )

        self.assertTrue(accepted)
        get_order.assert_called_once_with(
            "988200",
            order_id="order-1",
            delivery_date=None,
        )
        self.assertEqual(request_doc.updates[-1]["status"], "completed")


if __name__ == "__main__":
    unittest.main()

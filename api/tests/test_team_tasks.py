import inspect
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from order_forecast.api.routers import team_tasks


class _FakeSnapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _FakeQuery:
    def __init__(self, collection, limit_value=None):
        self.collection = collection
        self.limit_value = limit_value

    def stream(self):
        docs = self.collection.stream()
        return docs[: self.limit_value] if self.limit_value else docs

    def get(self):
        return self.stream()


class _FakeCollection:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)

    def document(self, doc_id=None):
        if doc_id is None:
            doc_id = self.db.next_id()
        return _FakeDocument(self.db, self.path + (doc_id,))

    def add(self, data):
        ref = self.document()
        ref.set(data, merge=False)
        return None, ref

    def limit(self, limit_value):
        return _FakeQuery(self, limit_value)

    def stream(self):
        data = self.db.collection_data(self.path)
        return [_FakeSnapshot(doc_id, doc_data) for doc_id, doc_data in data.items()]

    def get(self):
        return self.stream()


class _FakeDocument:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)
        self.id = self.path[-1]

    def collection(self, name):
        return _FakeCollection(self.db, self.path + (name,))

    def get(self, transaction=None):
        return _FakeSnapshot(self.id, self.db.document_data(self.path))

    def set(self, data, merge=False):
        self.db.set_document(self.path, data, merge=merge)

    def update(self, data):
        self.db.update_document(self.path, data)

    def delete(self):
        self.db.delete_document(self.path)


class _FakeTransaction:
    def update(self, doc_ref, data):
        doc_ref.update(data)

    def set(self, doc_ref, data, merge=False):
        doc_ref.set(data, merge=merge)


class _FakeDB:
    def __init__(self):
        self._id = 0
        self.root = {"users": {}, "routes": {}, "routeEntitlements": {}, "routeNumbers": {}}

    def next_id(self):
        self._id += 1
        return f"task-{self._id}"

    def collection(self, name):
        self.root.setdefault(name, {})
        return _FakeCollection(self, (name,))

    def transaction(self):
        return _FakeTransaction()

    def collection_data(self, path):
        if len(path) == 1:
            return self.root.setdefault(path[0], {})
        if len(path) == 3:
            parent = self.document_data(path[:2])
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        raise AssertionError(f"Unsupported collection path: {path}")

    def document_data(self, path):
        if len(path) == 2:
            return self.root.setdefault(path[0], {}).get(path[1])
        if len(path) == 4:
            parent = self.document_data(path[:2])
            if parent is None:
                return None
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {}).get(path[3])
        raise AssertionError(f"Unsupported document path: {path}")

    def set_document(self, path, data, merge=False):
        if len(path) == 2:
            collection = self.root.setdefault(path[0], {})
        elif len(path) == 4:
            parent = self.root.setdefault(path[0], {}).setdefault(path[1], {})
            collection = parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        else:
            raise AssertionError(f"Unsupported document path: {path}")
        current = collection.get(path[-1]) if merge else None
        collection[path[-1]] = {**(current or {}), **data}

    def update_document(self, path, data):
        current = self.document_data(path)
        if current is None:
            raise AssertionError(f"Missing document for update: {path}")
        current.update(data)

    def delete_document(self, path):
        if len(path) == 4:
            parent = self.document_data(path[:2])
            if parent is not None:
                parent.setdefault("_subcollections", {}).setdefault(path[2], {}).pop(path[3], None)
            return
        self.root.setdefault(path[0], {}).pop(path[1], None)


def _unwrap(endpoint):
    return inspect.unwrap(endpoint)


def _owner_user():
    return {
        "profile": {
            "role": "owner",
            "routeNumber": "989567",
            "email": "owner@example.com",
            "personalName": "Owner Name",
        },
        "fcmTokens": ["ExponentPushToken[owner]"],
    }


def _member_user(*, needs_approval=False, verified=True):
    return {
        "profile": {"role": "team_member", "email": "member@example.com", "personalName": "Member Name"},
        "routeAssignments": {
            "989567": {
                "role": "team_member",
                "needsApproval": needs_approval,
                "verified": verified,
                "assignedTo": "owner-1",
            }
        },
        "fcmTokens": ["ExponentPushToken[member]"],
    }


def _build_db():
    db = _FakeDB()
    db.root["routes"]["989567"] = {"ownerUid": "owner-1"}
    db.root["users"]["owner-1"] = _owner_user()
    db.root["users"]["member-1"] = _member_user()
    db.root["users"]["pending-1"] = _member_user(needs_approval=True)
    db.root["users"]["outsider-1"] = {
        "profile": {"role": "team_member", "email": "outsider@example.com"},
        "routeAssignments": {},
    }
    return db


class TeamTasksApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_can_create_task_for_approved_member_and_notifies_assignee(self):
        db = _build_db()
        payload = team_tasks.TeamTaskCreateRequest(
            routeNumber="989567",
            teamMemberUid="member-1",
            task="Check promo case count",
        )

        with patch.object(team_tasks, "require_route_access", return_value=_owner_user()), patch.object(
            team_tasks, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            response = await _unwrap(team_tasks.create_team_task)(
                request=None,
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertTrue(response["ok"])
        task_id = response["taskId"]
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"][task_id]
        self.assertEqual(task["teamMemberUid"], "member-1")
        self.assertEqual(task["task"], "Check promo case count")
        notifications = db.root["users"]["member-1"]["_subcollections"]["notifications"]
        notification = next(iter(notifications.values()))
        self.assertEqual(notification["type"], "team_task_assigned")
        self.assertIn("Route: 989567", notification["body"])
        self.assertIn("Assigned by: Owner Name", notification["body"])
        send_push.assert_called_once()
        self.assertEqual(send_push.call_args.kwargs["data"]["type"], "team_task_assigned")

    async def test_non_owner_cannot_create_task(self):
        db = _build_db()
        payload = team_tasks.TeamTaskCreateRequest(routeNumber="989567", teamMemberUid="member-1", task="Count bins")

        with patch.object(team_tasks, "require_route_access", return_value=_member_user()):
            with self.assertRaises(HTTPException) as ctx:
                await _unwrap(team_tasks.create_team_task)(
                    request=None,
                    payload=payload,
                    decoded_token={"uid": "member-1"},
                    db=db,
                )

        self.assertEqual(ctx.exception.status_code, 403)

    async def test_owner_cannot_create_task_for_pending_member(self):
        db = _build_db()
        payload = team_tasks.TeamTaskCreateRequest(routeNumber="989567", teamMemberUid="pending-1", task="Count bins")

        with patch.object(team_tasks, "require_route_access", return_value=_owner_user()):
            with self.assertRaises(HTTPException) as ctx:
                await _unwrap(team_tasks.create_team_task)(
                    request=None,
                    payload=payload,
                    decoded_token={"uid": "owner-1"},
                    db=db,
                )

        self.assertEqual(ctx.exception.status_code, 403)

    async def test_capabilities_defaults_team_for_owner_with_members(self):
        db = _build_db()

        with patch.object(team_tasks, "require_route_access", return_value=_owner_user()):
            response = await _unwrap(team_tasks.get_team_task_capabilities)(
                request=None,
                route="989567",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertTrue(response["canAssign"])
        self.assertEqual(response["defaultThirdSlot"], "teamTasks")

    async def test_capabilities_detects_member_assigned_open_task(self):
        db = _build_db()
        db.collection("routes").document("989567").collection("teamTasks").document("task-a").set(
            {
                "routeNumber": "989567",
                "ownerUid": "owner-1",
                "teamMemberUid": "member-1",
                "teamMemberDisplay": "Member Name",
                "task": "Check display",
                "status": "open",
            }
        )

        with patch.object(team_tasks, "require_route_access", return_value=_member_user()):
            response = await _unwrap(team_tasks.get_team_task_capabilities)(
                request=None,
                route="989567",
                decoded_token={"uid": "member-1"},
                db=db,
            )

        self.assertTrue(response["hasAssignedTeamTasks"])
        self.assertEqual(response["defaultThirdSlot"], "teamTasks")

    async def test_list_filters_member_to_own_tasks(self):
        db = _build_db()
        tasks = db.collection("routes").document("989567").collection("teamTasks")
        tasks.document("task-a").set({"routeNumber": "989567", "teamMemberUid": "member-1", "task": "Mine", "status": "open"})
        tasks.document("task-b").set({"routeNumber": "989567", "teamMemberUid": "other-1", "task": "Other", "status": "open"})

        with patch.object(team_tasks, "require_route_access", return_value=_member_user()):
            response = await _unwrap(team_tasks.list_team_tasks)(
                request=None,
                route="989567",
                limit=100,
                decoded_token={"uid": "member-1"},
                db=db,
            )

        self.assertEqual([task["id"] for task in response["tasks"]], ["task-a"])

    async def test_assigned_member_can_complete_once_and_owner_is_notified_once(self):
        db = _build_db()
        db.collection("routes").document("989567").collection("teamTasks").document("task-a").set(
            {
                "routeNumber": "989567",
                "ownerUid": "owner-1",
                "teamMemberUid": "member-1",
                "teamMemberDisplay": "Member Name",
                "task": "Check display",
                "status": "open",
            }
        )
        payload = team_tasks.TeamTaskCompleteRequest(routeNumber="989567")

        with patch.object(team_tasks.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            team_tasks, "require_route_access", return_value=_member_user()
        ), patch.object(team_tasks, "_send_expo_push", return_value={"sent": 1, "failed": 0}) as send_push:
            first = await _unwrap(team_tasks.complete_team_task)(
                request=None,
                task_id="task-a",
                payload=payload,
                decoded_token={"uid": "member-1"},
                db=db,
            )
            second = await _unwrap(team_tasks.complete_team_task)(
                request=None,
                task_id="task-a",
                payload=payload,
                decoded_token={"uid": "member-1"},
                db=db,
            )

        self.assertTrue(first["ok"])
        self.assertTrue(second["alreadyCompleted"])
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["status"], "completed")
        notifications = db.root["users"]["owner-1"]["_subcollections"]["notifications"]
        self.assertEqual(len(notifications), 1)
        notification = next(iter(notifications.values()))
        self.assertEqual(notification["type"], "team_task_completed")
        self.assertIn("Team Member: Member Name", notification["body"])
        self.assertIn("Marked Complete.", notification["body"])
        send_push.assert_called_once()

    async def test_unassigned_member_cannot_complete_task(self):
        db = _build_db()
        db.collection("routes").document("989567").collection("teamTasks").document("task-a").set(
            {"routeNumber": "989567", "ownerUid": "owner-1", "teamMemberUid": "other-1", "task": "Other", "status": "open"}
        )
        payload = team_tasks.TeamTaskCompleteRequest(routeNumber="989567")

        with patch.object(team_tasks.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            team_tasks, "require_route_access", return_value=_member_user()
        ):
            with self.assertRaises(HTTPException) as ctx:
                await _unwrap(team_tasks.complete_team_task)(
                    request=None,
                    task_id="task-a",
                    payload=payload,
                    decoded_token={"uid": "member-1"},
                    db=db,
                )

        self.assertEqual(ctx.exception.status_code, 403)

    async def test_member_cannot_delete_owner_can_delete(self):
        db = _build_db()
        db.collection("routes").document("989567").collection("teamTasks").document("task-a").set(
            {"routeNumber": "989567", "ownerUid": "owner-1", "teamMemberUid": "member-1", "task": "Mine", "status": "open"}
        )

        with patch.object(team_tasks, "require_route_access", return_value=_member_user()):
            with self.assertRaises(HTTPException) as ctx:
                await _unwrap(team_tasks.delete_team_task)(
                    request=None,
                    task_id="task-a",
                    route="989567",
                    decoded_token={"uid": "member-1"},
                    db=db,
                )
        self.assertEqual(ctx.exception.status_code, 403)

        with patch.object(team_tasks, "require_route_access", return_value=_owner_user()):
            response = await _unwrap(team_tasks.delete_team_task)(
                request=None,
                task_id="task-a",
                route="989567",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertTrue(response["ok"])
        self.assertNotIn("task-a", db.root["routes"]["989567"]["_subcollections"]["teamTasks"])

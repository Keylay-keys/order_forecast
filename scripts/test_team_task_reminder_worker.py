from __future__ import annotations

import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import team_task_reminder_worker as worker


class _FakeSnapshot:
    def __init__(self, doc_id, data, reference=None):
        self.id = doc_id
        self._data = data
        self.exists = data is not None
        self.reference = reference

    def to_dict(self):
        return dict(self._data or {})


class _FakeDocument:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)
        self.id = self.path[-1]

    def collection(self, name):
        return _FakeCollection(self.db, self.path + (name,))

    def get(self, transaction=None):
        return _FakeSnapshot(self.id, self.db.document_data(self.path), self)

    def set(self, data, merge=False):
        self.db.set_document(self.path, data, merge=merge)

    def update(self, data):
        self.db.update_document(self.path, data)


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


class _FakeQuery:
    def __init__(self, db, filters=None, order_field=None, limit_value=None):
        self.db = db
        self.filters = filters or []
        self.order_field = order_field
        self.limit_value = limit_value

    def where(self, field, op, value):
        return _FakeQuery(self.db, self.filters + [(field, op, value)], self.order_field, self.limit_value)

    def order_by(self, field):
        return _FakeQuery(self.db, self.filters, field, self.limit_value)

    def limit(self, value):
        return _FakeQuery(self.db, self.filters, self.order_field, value)

    def stream(self):
        docs = []
        for route, route_data in self.db.root["routes"].items():
            tasks = route_data.get("_subcollections", {}).get("teamTasks", {})
            for task_id, data in tasks.items():
                if self._matches(data):
                    ref = _FakeDocument(self.db, ("routes", route, "teamTasks", task_id))
                    docs.append(_FakeSnapshot(task_id, data, ref))
        if self.order_field:
            docs.sort(key=lambda snap: (snap.to_dict() or {}).get(self.order_field) or 0)
        return docs[: self.limit_value] if self.limit_value else docs

    def _matches(self, data):
        for field, op, value in self.filters:
            current = data.get(field)
            if op == "==" and current != value:
                return False
            if op == "<=" and not (current is not None and current <= value):
                return False
        return True


class _FakeTransaction:
    def update(self, doc_ref, data):
        doc_ref.update(data)


class _FakeDB:
    def __init__(self):
        self._id = 0
        self.root = {"users": {}, "routes": {}}

    def next_id(self):
        self._id += 1
        return f"doc-{self._id}"

    def collection(self, name):
        self.root.setdefault(name, {})
        return _FakeCollection(self, (name,))

    def collection_group(self, name):
        if name != "teamTasks":
            raise AssertionError(name)
        return _FakeQuery(self)

    def transaction(self):
        return _FakeTransaction()

    def document_data(self, path):
        if len(path) == 2:
            return self.root.setdefault(path[0], {}).get(path[1])
        if len(path) == 4:
            parent = self.document_data(path[:2])
            if parent is None:
                return None
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {}).get(path[3])
        raise AssertionError(path)

    def set_document(self, path, data, merge=False):
        if len(path) == 2:
            collection = self.root.setdefault(path[0], {})
        elif len(path) == 4:
            parent = self.root.setdefault(path[0], {}).setdefault(path[1], {})
            collection = parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        else:
            raise AssertionError(path)
        current = collection.get(path[-1]) if merge else None
        collection[path[-1]] = {**(current or {}), **data}

    def update_document(self, path, data):
        current = self.document_data(path)
        if current is None:
            raise AssertionError(f"Missing document: {path}")
        current.update(data)


def _build_db():
    db = _FakeDB()
    db.root["routes"]["989567"] = {"ownerUid": "owner-1"}
    db.root["users"]["member-1"] = {
        "fcmTokens": ["ExponentPushToken[member]"],
        "profile": {"personalName": "Member Name"},
    }
    return db


def _add_task(db, task_id="task-a", **overrides):
    data = {
        "routeNumber": "989567",
        "ownerUid": "owner-1",
        "teamMemberUid": "member-1",
        "teamMemberDisplay": "Member Name",
        "task": "Check display",
        "status": "open",
        "dueDate": "2026-07-06",
        "dueTime": {"hour": 4, "minute": 15, "period": "PM"},
        "timezone": "America/Denver",
        "dueAtMs": 1_000_000,
        "reminderEnabled": True,
        "reminderOffsetMinutes": 15,
        "reminderAtMs": 900_000,
        "reminderStatus": "pending",
    }
    data.update(overrides)
    db.collection("routes").document("989567").collection("teamTasks").document(task_id).set(data)
    return data


class TeamTaskReminderWorkerTests(unittest.TestCase):
    def test_pending_reminder_sends_once_and_repeat_run_does_not_resend(self):
        db = _build_db()
        _add_task(db)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            first = worker.run_once(db, now_ms=900_000, limit=10)
            second = worker.run_once(db, now_ms=901_000, limit=10)

        self.assertEqual(first["claimed"], 1)
        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["claimed"], 0)
        send_push.assert_called_once()
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["reminderStatus"], "sent")
        notifications = db.root["users"]["member-1"]["_subcollections"]["notifications"]
        self.assertEqual(len(notifications), 1)

    def test_stale_sending_claim_is_reclaimed(self):
        db = _build_db()
        _add_task(db, reminderStatus="sending", reminderClaimedAtMs=100)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push, patch.object(worker, "TEAM_TASK_REMINDER_CLAIM_TTL_MS", 1_000):
            stats = worker.run_once(db, now_ms=2_000, limit=10)

        self.assertEqual(stats["claimed"], 1)
        send_push.assert_called_once()
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["reminderStatus"], "sent")

    def test_claim_transaction_skips_task_completed_after_query_snapshot(self):
        db = _build_db()
        _add_task(db)
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]["status"] = "completed"

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_task_reminder(db, doc, now_ms=900_000)

        self.assertIsNone(claimed)
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["reminderStatus"], "pending")

    def test_past_due_reminder_is_skipped(self):
        db = _build_db()
        _add_task(db, dueAtMs=1_000, reminderAtMs=900)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push, patch.object(worker, "TEAM_TASK_REMINDER_DUE_GRACE_MS", 100):
            stats = worker.run_once(db, now_ms=2_000, limit=10)

        self.assertEqual(stats["claimed"], 0)
        self.assertEqual(stats["skipped"], 1)
        send_push.assert_not_called()
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["reminderStatus"], "skipped")
        self.assertEqual(task["reminderSkipReason"], "past_due")

    def test_failed_push_marks_failed(self):
        db = _build_db()
        _add_task(db)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 0, "failed": 1}
        ):
            stats = worker.run_once(db, now_ms=900_000, limit=10)

        self.assertEqual(stats["claimed"], 1)
        self.assertEqual(stats["failed"], 1)
        task = db.root["routes"]["989567"]["_subcollections"]["teamTasks"]["task-a"]
        self.assertEqual(task["reminderStatus"], "failed")


if __name__ == "__main__":
    unittest.main()

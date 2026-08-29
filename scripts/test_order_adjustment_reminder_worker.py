from __future__ import annotations

import pathlib
import copy
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import order_adjustment_reminder_worker as worker


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
        self.db.events.append(("read", "/".join(self.path)))
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

    def stream(self):
        collection = self.db.collection_data(self.path)
        return [
            _FakeSnapshot(doc_id, data, _FakeDocument(self.db, self.path + (doc_id,)))
            for doc_id, data in collection.items()
        ]


class _FakeQuery:
    def __init__(self, db, filters=None, order_field=None, limit_value=None):
        self.db = db
        self.filters = filters or []
        self.order_field = order_field
        self.limit_value = limit_value

    def where(self, field=None, op=None, value=None, *, filter=None):
        if filter is not None:
            field = filter.field_path
            op = filter.op_string
            value = filter.value
        return _FakeQuery(self.db, self.filters + [(field, op, value)], self.order_field, self.limit_value)

    def order_by(self, field):
        return _FakeQuery(self.db, self.filters, field, self.limit_value)

    def limit(self, value):
        return _FakeQuery(self.db, self.filters, self.order_field, value)

    def stream(self):
        docs = []
        for route, route_data in self.db.root["routes"].items():
            adjustments = route_data.get("_subcollections", {}).get("orderAdjustments", {})
            for adjustment_id, data in adjustments.items():
                if self._matches(data):
                    ref = _FakeDocument(self.db, ("routes", route, "orderAdjustments", adjustment_id))
                    docs.append(_FakeSnapshot(adjustment_id, data, ref))
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
    def __init__(self, db):
        self.db = db

    def update(self, doc_ref, data):
        self.db.events.append(("write", "/".join(doc_ref.path)))
        doc_ref.update(data)


class _FakeDB:
    def __init__(self):
        self._id = 0
        self.root = {"users": {}, "routes": {}}
        self.events = []

    def next_id(self):
        self._id += 1
        return f"doc-{self._id}"

    def collection(self, name):
        self.root.setdefault(name, {})
        return _FakeCollection(self, (name,))

    def collection_group(self, name):
        if name != "orderAdjustments":
            raise AssertionError(name)
        return _FakeQuery(self)

    def transaction(self):
        return _FakeTransaction(self)

    def collection_data(self, path):
        if len(path) == 1:
            return self.root.setdefault(path[0], {})
        if len(path) == 3:
            parent = self.document_data(path[:2])
            if parent is None:
                return {}
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        raise AssertionError(path)

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
    db.root["users"]["user-1"] = {
        "fcmTokens": ["ExponentPushToken[user]"],
        "profile": {"personalName": "Route User"},
    }
    return db


def _add_adjustment(db, adjustment_id="adjustment-a", **overrides):
    data = {
        "id": adjustment_id,
        "routeNumber": "989567",
        "userId": "user-1",
        "status": "draft",
        "targetDeliveryDate": "2026-07-09",
        "adjustmentDate": "1970-01-01",
        "timezone": "UTC",
        "cutoffTimeLocal": "00:20",
        "reminderLeadMinutes": 5,
        "cutoffAtMs": 1_200_000,
        "reminderEnabled": True,
        "reminderAtMs": 900_000,
        "reminderStatus": "pending",
        "reminderAttemptCount": 0,
        "lines": [
            {
                "sap": "28934",
                "direction": "add",
                "caseQuantity": 1,
            }
        ],
    }
    data.update(overrides)
    db.collection("routes").document("989567").collection("orderAdjustments").document(adjustment_id).set(data)
    return data


def _add_source_order(db, order_id="order-a", **overrides):
    data = {
        "id": order_id,
        "routeNumber": "989567",
        "status": "finalized",
        "orderDate": "2026-07-30",
        "submittedAt": datetime(2026, 7, 30, 18, 0, tzinfo=timezone.utc),
        "createdAt": datetime(2026, 7, 30, 17, 0, tzinfo=timezone.utc),
    }
    data.update(overrides)
    db.collection("routes").document("989567").collection("orders").document(order_id).set(data)
    return data


def _v2_stamp(
    *,
    date="2026-07-30",
    timezone_name="UTC",
    order_day=4,
    schedule_key="thursday",
):
    return {
        "version": 2,
        "date": date,
        "timezone": timezone_name,
        "scheduleKey": schedule_key,
        "orderDay": order_day,
        "loadOffsetDays": 0,
        "deliveryOffsetDays": 1,
        "provenance": "cycle",
    }


class OrderAdjustmentReminderWorkerTests(unittest.TestCase):
    def test_pending_reminder_sends_once_and_repeat_run_does_not_resend(self):
        db = _build_db()
        _add_adjustment(db)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            first = worker.run_once(db, now_ms=900_000, limit=10)
            second = worker.run_once(db, now_ms=901_000, limit=10)

        self.assertEqual(first["claimed"], 1)
        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["claimed"], 0)
        send_push.assert_called_once()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "sent")
        notifications = db.root["users"]["user-1"]["_subcollections"]["notifications"]
        self.assertEqual(len(notifications), 1)

    def test_stale_sending_claim_is_reclaimed(self):
        db = _build_db()
        _add_adjustment(db, reminderStatus="sending", reminderClaimedAtMs=100)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push, patch.object(worker, "ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS", 1_000):
            stats = worker.run_once(db, now_ms=901_000, limit=10)

        self.assertEqual(stats["claimed"], 1)
        send_push.assert_called_once()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "sent")

    def test_finish_does_not_overwrite_terminalized_adjustment(self):
        db = _build_db()
        _add_adjustment(db)
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=900_000)
            self.assertIsNotNone(claimed)
            adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
            adjustment.update({"status": "sent", "reminderStatus": "skipped"})
            finished = worker.finish_adjustment_reminder(
                db,
                claimed["ref"],
                claim=claimed["adjustment"],
                now_ms=901_000,
                sent=True,
                push_stats={"sent": 1, "failed": 0},
            )

        self.assertFalse(finished)
        self.assertEqual(adjustment["reminderStatus"], "skipped")

    def test_finish_does_not_overwrite_newer_claim(self):
        db = _build_db()
        _add_adjustment(db)
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=900_000)
            self.assertIsNotNone(claimed)
            adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
            adjustment.update({
                "reminderClaimedAtMs": 901_000,
                "reminderAttemptCount": 2,
            })
            finished = worker.finish_adjustment_reminder(
                db,
                claimed["ref"],
                claim=claimed["adjustment"],
                now_ms=902_000,
                sent=False,
                push_stats={"sent": 0, "failed": 1},
            )

        self.assertFalse(finished)
        self.assertEqual(adjustment["reminderStatus"], "sending")

    def test_claim_transaction_skips_sent_adjustment_after_query_snapshot(self):
        db = _build_db()
        _add_adjustment(db)
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]["status"] = "sent"

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=900_000)

        self.assertIsNone(claimed)
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "pending")

    def test_past_cutoff_reminder_is_skipped(self):
        db = _build_db()
        _add_adjustment(db)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push, patch.object(worker, "ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS", 100):
            stats = worker.run_once(db, now_ms=2_000_000, limit=10)

        self.assertEqual(stats["claimed"], 0)
        self.assertEqual(stats["skipped"], 1)
        send_push.assert_not_called()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "skipped")
        self.assertEqual(adjustment["reminderSkipReason"], "past_cutoff")

    def test_failed_push_marks_failed(self):
        db = _build_db()
        _add_adjustment(db)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 0, "failed": 1}
        ):
            stats = worker.run_once(db, now_ms=900_000, limit=10)

        self.assertEqual(stats["claimed"], 1)
        self.assertEqual(stats["failed"], 1)
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "failed")

    def test_empty_or_net_zero_adjustment_is_skipped_before_claim(self):
        db = _build_db()
        _add_adjustment(
            db,
            lines=[
                {"sap": "28934", "direction": "add", "caseQuantity": 2},
                {"sap": "28934", "direction": "remove", "caseQuantity": 2},
            ],
        )

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            stats = worker.run_once(db, now_ms=900_000, limit=10)

        self.assertEqual(stats["claimed"], 0)
        send_push.assert_not_called()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderStatus"], "skipped")
        self.assertEqual(adjustment["reminderSkipReason"], "no_meaningful_lines")

    def test_source_order_window_corrects_dormant_wrong_day_schedule_without_sending(self):
        db = _build_db()
        _add_source_order(db)
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            adjustmentDate="2026-08-02",
            cutoffTimeLocal="14:00",
            reminderLeadMinutes=15,
            cutoffAtMs=1,
            reminderAtMs=1,
        )
        expected_cutoff = int(datetime(2026, 7, 31, 14, 0, tzinfo=timezone.utc).timestamp() * 1000)
        now_ms = expected_cutoff - 60 * 60_000

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            stats = worker.run_once(db, now_ms=now_ms, limit=10)

        self.assertEqual(stats["claimed"], 0)
        send_push.assert_not_called()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["adjustmentDate"], "2026-07-31")
        self.assertEqual(adjustment["cutoffAtMs"], expected_cutoff)
        self.assertEqual(adjustment["reminderStatus"], "pending")

    def test_missing_source_order_is_skipped_instead_of_using_stale_schedule(self):
        db = _build_db()
        _add_adjustment(db, sourceOrderId="missing-order")

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            stats = worker.run_once(db, now_ms=900_000, limit=10)

        self.assertEqual(stats["claimed"], 0)
        send_push.assert_not_called()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment["reminderSkipReason"], "missing_source_order")

    def test_route_field_mismatch_never_reads_or_sends_from_another_route(self):
        db = _build_db()
        _add_adjustment(db, routeNumber="989262", sourceOrderId="order-a")
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        before = copy.deepcopy(
            db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        )
        db.events.clear()

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send_push:
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=900_000)

        self.assertIsNone(claimed)
        send_push.assert_not_called()
        adjustment = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment, before)
        self.assertNotIn(("read", "routes/989262/orders/order-a"), db.events)
        self.assertFalse(any(event[0] == "write" for event in db.events))
        audit = worker.classify_without_writes(db, doc, now_ms=900_000)
        self.assertEqual(audit["classification"], "route_mismatch")
        self.assertEqual(audit["pathRouteNumber"], "989567")

    def test_v2_claim_uses_fresh_transaction_source_and_returns_canonical_payload(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderAtMs=1,
            cutoffAtMs=2,
            timezone="Pacific/Honolulu",
        )
        canonical_reminder = int(datetime(2026, 7, 31, 10, 45, tzinfo=timezone.utc).timestamp() * 1000)
        doc = next(iter(worker._pending_query(db, canonical_reminder, 10)))
        # Simulate a stale query-era/source snapshot: the authoritative stamp is
        # changed before the transaction reads it.
        source_data = db.root["routes"]["989567"]["_subcollections"]["orders"]["order-a"]
        source_data["scheduledOrder"] = _v2_stamp(date="2026-08-06")
        future_reminder = int(datetime(2026, 8, 7, 10, 45, tzinfo=timezone.utc).timestamp() * 1000)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=canonical_reminder)

        self.assertIsNone(claimed)
        adjustment_data = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(adjustment_data["adjustmentDate"], "2026-08-07")
        self.assertEqual(adjustment_data["reminderAtMs"], future_reminder)
        self.assertEqual(adjustment_data["timezone"], "UTC")

        # Put the source back on the due cycle and prove patch+claim uses one
        # canonical payload in the same transaction.
        source_data["scheduledOrder"] = _v2_stamp()
        adjustment_data["reminderAtMs"] = 1
        adjustment_data["reminderStatus"] = "pending"
        doc = next(iter(worker._pending_query(db, canonical_reminder + 5 * 60_000, 10)))
        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=canonical_reminder + 5 * 60_000)

        self.assertIsNotNone(claimed)
        payload = claimed["adjustment"]
        self.assertEqual(payload["sourceScheduleClassification"], "v2")
        self.assertEqual(payload["adjustmentDate"], "2026-07-31")
        self.assertEqual(payload["reminderAtMs"], canonical_reminder)
        self.assertEqual(payload["timezone"], "UTC")
        self.assertEqual(payload["reminderStatus"], "sending")
        self.assertEqual(payload["claimAgeMs"], 5 * 60_000)

    def test_1050_worker_tick_sends_original_1045_v2_reminder_once(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        reminder = int(datetime(2026, 7, 31, 10, 45, tzinfo=timezone.utc).timestamp() * 1000)
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderAtMs=reminder,
            cutoffAtMs=reminder + 15 * 60_000,
        )
        now_ms = reminder + 5 * 60_000
        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "_send_expo_push", return_value={"sent": 1, "failed": 0}
        ) as send:
            first = worker.run_once(db, now_ms=now_ms, limit=10)
            second = worker.run_once(db, now_ms=now_ms + 1, limit=10)

        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["sent"], 0)
        send.assert_called_once()
        data = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(data["reminderAtMs"], reminder)
        self.assertEqual(data["reminderStatus"], "sent")

    def test_missing_and_unstamped_sources_are_distinct(self):
        adjustment_data = _add_adjustment(_build_db(), sourceOrderId="order-a")
        missing_schedule, missing_classification, missing_error = worker.expected_schedule_from_fresh_source(
            adjustment_data, None
        )
        v1_schedule, v1_classification, v1_error = worker.expected_schedule_from_fresh_source(
            adjustment_data,
            {"orderDate": "2026-07-30", "submittedAt": datetime(2026, 7, 30, tzinfo=timezone.utc)},
        )
        self.assertIsNone(missing_schedule)
        self.assertEqual(missing_classification, "missing_source_order")
        self.assertEqual(missing_error, "missing_source_order")
        self.assertIsNotNone(v1_schedule)
        self.assertEqual(v1_classification, "v1_source")
        self.assertIsNone(v1_error)

    def test_present_malformed_stamp_never_falls_back_to_v1_timestamps(self):
        adjustment_data = _add_adjustment(_build_db(), sourceOrderId="order-a")
        for malformed_stamp in [None, {}, {"version": 1}, {"version": 2, "date": "bad"}]:
            with self.subTest(stamp=malformed_stamp):
                schedule, classification, error = worker.expected_schedule_from_fresh_source(
                    adjustment_data,
                    {
                        "scheduledOrder": malformed_stamp,
                        "submittedAt": datetime(2099, 1, 1, tzinfo=timezone.utc),
                        "createdAt": datetime(2099, 1, 1, tzinfo=timezone.utc),
                        "orderDate": "2099-01-01",
                    },
                )
                self.assertIsNone(schedule)
                self.assertEqual(classification, "v2")
                self.assertEqual(error, "window_unresolved")

    def test_no_lines_path_reads_fresh_source_before_first_write(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        _add_adjustment(db, sourceOrderId="order-a", lines=[])
        doc = next(iter(worker._pending_query(db, 900_000, 10)))
        db.events.clear()

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn):
            worker.claim_adjustment_reminder(db, doc, now_ms=900_000)

        self.assertEqual(db.events[:3], [
            ("read", "routes/989567/orderAdjustments/adjustment-a"),
            ("read", "routes/989567/orders/order-a"),
            ("write", "routes/989567/orderAdjustments/adjustment-a"),
        ])

    def test_pending_closes_at_cutoff_without_grace(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderAtMs=1,
        )
        cutoff = int(datetime(2026, 7, 31, 11, 0, tzinfo=timezone.utc).timestamp() * 1000)

        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS", 120_000
        ):
            stats = worker.run_once(db, now_ms=cutoff, limit=10)

        self.assertEqual(stats["claimed"], 0)
        data = db.root["routes"]["989567"]["_subcollections"]["orderAdjustments"]["adjustment-a"]
        self.assertEqual(data["reminderSkipReason"], "past_cutoff")

    def test_stale_sending_can_recover_inside_cutoff_grace(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderStatus="sending",
            reminderClaimedAtMs=1,
        )
        cutoff = int(datetime(2026, 7, 31, 11, 0, tzinfo=timezone.utc).timestamp() * 1000)
        with patch.object(worker.firestore, "transactional", side_effect=lambda fn: fn), patch.object(
            worker, "ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS", 1_000
        ), patch.object(worker, "ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS", 120_000):
            doc = next(iter(worker._stale_sending_query(db, cutoff, 10)))
            claimed = worker.claim_adjustment_reminder(db, doc, now_ms=cutoff + 60_000)

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["adjustment"]["reminderStatus"], "sending")

    def test_route_scoped_dry_run_is_byte_identical_and_never_sends(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderAtMs=1,
        )
        before = copy.deepcopy(db.root)
        now_ms = int(datetime(2026, 7, 31, 10, 50, tzinfo=timezone.utc).timestamp() * 1000)
        with patch.object(worker, "claim_adjustment_reminder") as claim, patch.object(
            worker, "finish_adjustment_reminder"
        ) as finish, patch.object(worker, "_send_expo_push") as send:
            summary = worker.audit_once(db, route_number="989567", now_ms=now_ms)

        self.assertEqual(before, db.root)
        claim.assert_not_called()
        finish.assert_not_called()
        send.assert_not_called()
        self.assertTrue(summary["dryRun"])
        self.assertEqual(summary["actions"]["claim_and_send"], 1)
        self.assertEqual(summary["classifications"]["v2"], 1)
        self.assertEqual(summary["results"][0]["stamp"], {
            "version": 2, "date": "2026-07-30", "timezone": "UTC",
        })

    def test_dry_run_flags_incident_shape_even_after_canonical_cutoff_expired(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder=_v2_stamp())
        canonical_cutoff = int(datetime(2026, 7, 31, 11, 0, tzinfo=timezone.utc).timestamp() * 1000)
        stored_saturday = int(datetime(2026, 8, 2, 10, 45, tzinfo=timezone.utc).timestamp() * 1000)
        _add_adjustment(
            db,
            sourceOrderId="order-a",
            cutoffTimeLocal="11:00",
            reminderLeadMinutes=15,
            reminderAtMs=stored_saturday,
            cutoffAtMs=stored_saturday + 15 * 60_000,
        )
        now_ms = canonical_cutoff + 24 * 60 * 60_000
        doc = next(iter(db.collection("routes").document("989567").collection("orderAdjustments").stream()))

        result = worker.classify_without_writes(db, doc, now_ms=now_ms)

        self.assertEqual(result["action"], "skip_past_cutoff")
        self.assertTrue(result["eligibleForReminder"])
        self.assertTrue(result["storedFutureScheduleDrift"])
        self.assertIn("reminderAtMs", result["scheduleDriftFields"])

    def test_terminal_v2_row_is_not_a_wrong_future_or_unresolved_candidate(self):
        db = _build_db()
        _add_source_order(db, scheduledOrder={"version": 2, "date": "bad"})
        _add_adjustment(db, sourceOrderId="order-a", status="sent", reminderAtMs=9_999_999_999_999)
        doc = next(iter(db.collection("routes").document("989567").collection("orderAdjustments").stream()))

        result = worker.classify_without_writes(db, doc, now_ms=900_000)

        self.assertFalse(result["eligibleForReminder"])
        self.assertFalse(result["storedFutureScheduleDrift"])
        self.assertEqual(result["scheduleError"], "window_unresolved")

    def test_cli_route_filter_requires_dry_run_and_dry_run_requires_route(self):
        with patch.object(sys, "argv", ["worker", "--serviceAccount", "sa.json", "--once", "--route", "988200"]):
            with self.assertRaises(SystemExit):
                worker.parse_args()
        with patch.object(sys, "argv", ["worker", "--serviceAccount", "sa.json", "--once", "--dry-run"]):
            with self.assertRaises(SystemExit):
                worker.parse_args()
        with patch.object(sys, "argv", [
            "worker", "--serviceAccount", "sa.json", "--once", "--dry-run", "--route", "988200",
        ]):
            args = worker.parse_args()
        self.assertTrue(args.dry_run)
        self.assertEqual(args.route, "988200")


if __name__ == "__main__":
    unittest.main()

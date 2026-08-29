"""Hermetic alert and direct-notification crash-boundary tests."""

from __future__ import annotations

import copy
import dataclasses
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path

from order_forecast.api.pcf_effect_inputs import load_effect_input
from order_forecast.api.pcf_effect_notification import (
    ExactReviewAlertStore,
    NotificationDispatchError,
    NotificationProviderResult,
    PcfNotificationDispatcher,
    review_alert_path,
)
from order_forecast.api.tests.test_pcf_effect_firebase import _Db
from order_forecast.api.tests.test_pcf_effect_inputs import (
    NOW,
    materialized_claim,
    stored_result,
)


def _low_claim(root: Path, *, attempt: int = 1):
    base = materialized_claim(root)
    request = copy.deepcopy(base.result["request"])
    request["proposal"]["confidence"] = 86
    request["proposal"]["confidenceTier"] = "medium"
    return dataclasses.replace(
        base,
        step="notifying",
        effect_attempt=attempt,
        confidence=86,
        result=stored_result(request),
    )


def _alert_store(db):
    return ExactReviewAlertStore(
        db,
        transactional=lambda function: function,
        clock=lambda: NOW,
    )


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self._one = None
        self._all = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT event_id, event_type"):
            self._all = [copy.deepcopy(item) for item in self.conn.events]
        elif normalized.startswith("SELECT status, effect_step"):
            self._one = copy.deepcopy(self.conn.work)
        elif normalized.startswith("INSERT INTO pcf_work_events"):
            payload = params[-1].adapted
            source_id = params[1]
            if any(item.get("source_event_id") == source_id for item in self.conn.events):
                raise AssertionError("duplicate source event id")
            self.conn.events.append(
                {
                    "event_id": params[0],
                    "source_event_id": source_id,
                    "event_type": params[4],
                    "payload": copy.deepcopy(payload),
                    "created_at": NOW + timedelta(microseconds=len(self.conn.events)),
                }
            )
        else:  # pragma: no cover - new SQL must be deliberately modeled
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _Connection:
    def __init__(self, *, effect_attempt=1):
        self.work = {
            "status": "effects_pending",
            "effect_step": "notifying",
            "effect_attempt_count": effect_attempt,
        }
        self.events = []
        self.commits = 0

    def cursor(self, **_kwargs):
        return _Cursor(self)

    def commit(self):
        self.commits += 1


class PcfEffectNotificationTests(unittest.TestCase):
    def test_exact_alert_is_create_once_and_preserves_human_status(self):
        with tempfile.TemporaryDirectory() as raw:
            effect = load_effect_input(_low_claim(Path(raw)))
            writer_db = _Db({})
            reader_db = _Db({})
            reader_db.docs = writer_db.docs
            first = _alert_store(writer_db).ensure(effect)
            second = _alert_store(writer_db).ensure(effect)
            self.assertEqual((first.disposition, second.disposition), ("applied", "already_correct"))
            self.assertEqual(len(writer_db.writes), 1)
            writer_db.docs[first.path]["status"] = "resolved"
            verified = _alert_store(reader_db).verify(effect)
            self.assertEqual(verified.status, "resolved")
            replay = _alert_store(writer_db).ensure(effect)
            self.assertEqual(replay.status, "resolved")

    def test_alert_identity_conflict_fails_without_overwrite(self):
        with tempfile.TemporaryDirectory() as raw:
            effect = load_effect_input(_low_claim(Path(raw)))
            db = _Db({})
            proof = _alert_store(db).ensure(effect)
            db.docs[proof.path]["inputHash"] = "f" * 64
            writes = len(db.writes)
            with self.assertRaisesRegex(Exception, "alert_conflict"):
                _alert_store(db).ensure(effect)
            self.assertEqual(len(db.writes), writes)

    def test_dispatch_commits_start_before_provider_and_replay_never_resends(self):
        with tempfile.TemporaryDirectory() as raw:
            claim = _low_claim(Path(raw))
            effect = load_effect_input(claim)
            conn = _Connection()
            calls = []

            def provider(envelope):
                self.assertEqual(conn.commits, 1)
                calls.append(envelope)
                return NotificationProviderResult("delivered", "telegram-ticket-1")

            dispatcher = PcfNotificationDispatcher(
                conn, provider, actor_id="pcf-effect-worker-01"
            )
            first = dispatcher.dispatch(claim, effect)
            replay = dispatcher.dispatch(claim, effect)
            self.assertEqual(first.disposition, "delivered")
            self.assertTrue(replay.idempotent_replay)
            self.assertEqual(len(calls), 1)
            self.assertEqual(conn.commits, 2)
            self.assertEqual(
                [event["event_type"] for event in conn.events],
                ["notification_dispatch_started", "notification_dispatch_terminal"],
            )

    def test_provider_exception_is_terminal_ambiguous_and_never_retried(self):
        with tempfile.TemporaryDirectory() as raw:
            claim = _low_claim(Path(raw))
            effect = load_effect_input(claim)
            conn = _Connection()
            calls = []

            def provider(_envelope):
                calls.append(True)
                raise TimeoutError("ambiguous transport timeout")

            dispatcher = PcfNotificationDispatcher(
                conn, provider, actor_id="pcf-effect-worker-01"
            )
            self.assertEqual(dispatcher.dispatch(claim, effect).disposition, "outcome_unknown")
            self.assertEqual(dispatcher.dispatch(claim, effect).disposition, "outcome_unknown")
            self.assertEqual(len(calls), 1)

    def test_prior_unfinished_start_becomes_unknown_without_a_second_send(self):
        with tempfile.TemporaryDirectory() as raw:
            first_claim = _low_claim(Path(raw), attempt=1)
            effect = load_effect_input(first_claim)
            conn = _Connection(effect_attempt=1)
            first = PcfNotificationDispatcher(
                conn,
                lambda _envelope: NotificationProviderResult("delivered"),
                actor_id="pcf-effect-worker-01",
            )
            first._append_start(first_claim, effect)
            conn.work["effect_attempt_count"] = 2
            second_claim = dataclasses.replace(first_claim, effect_attempt=2)
            calls = []
            second = PcfNotificationDispatcher(
                conn,
                lambda _envelope: calls.append(True),
                actor_id="pcf-effect-worker-02",
            )
            proof = second.dispatch(second_claim, effect)
            self.assertEqual(proof.disposition, "outcome_unknown")
            self.assertEqual(proof.effect_attempt, 1)
            self.assertEqual(calls, [])

    def test_proven_pre_acceptance_failure_allows_only_next_attempt_retry(self):
        with tempfile.TemporaryDirectory() as raw:
            first_claim = _low_claim(Path(raw), attempt=1)
            effect = load_effect_input(first_claim)
            conn = _Connection(effect_attempt=1)
            outcomes = [
                NotificationProviderResult("failed_before_acceptance"),
                NotificationProviderResult("delivered", "telegram-ticket-2"),
            ]
            calls = []

            def provider(_envelope):
                calls.append(True)
                return outcomes.pop(0)

            dispatcher = PcfNotificationDispatcher(
                conn, provider, actor_id="pcf-effect-worker-01"
            )
            failed = dispatcher.dispatch(first_claim, effect)
            self.assertEqual(failed.disposition, "failed_before_acceptance")
            same = dispatcher.dispatch(first_claim, effect)
            self.assertEqual(same.disposition, "failed_before_acceptance")
            self.assertEqual(len(calls), 1)
            conn.work["effect_attempt_count"] = 2
            second_claim = dataclasses.replace(first_claim, effect_attempt=2)
            delivered = dispatcher.dispatch(second_claim, effect)
            self.assertEqual(delivered.disposition, "delivered")
            self.assertEqual(len(calls), 2)

    def test_stale_effect_attempt_fails_before_provider(self):
        with tempfile.TemporaryDirectory() as raw:
            claim = _low_claim(Path(raw), attempt=1)
            effect = load_effect_input(claim)
            conn = _Connection(effect_attempt=2)
            calls = []
            dispatcher = PcfNotificationDispatcher(
                conn,
                lambda _envelope: calls.append(True),
                actor_id="pcf-effect-worker-01",
            )
            with self.assertRaisesRegex(NotificationDispatchError, "attempt_stale"):
                dispatcher.dispatch(claim, effect)
            self.assertEqual(calls, [])
            self.assertEqual(conn.commits, 0)


if __name__ == "__main__":
    unittest.main()

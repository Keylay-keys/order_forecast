"""Fail-closed tests for exact verified-closure queue TTL arming."""

from __future__ import annotations

import copy
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from order_forecast.api.pcf_queue_ttl_arming import (
    QueueTtlArmError,
    QueueTtlArmCandidate,
    QueueTtlArmCycleReport,
    QueueTtlArmIntent,
    QueueTtlArmProof,
    QueueTtlArmReceipt,
    QueueTtlHoldSnapshot,
    arm_exact_queue_ttl,
    record_queue_ttl_armed,
    run_queue_ttl_arm_cycle,
    run_queue_ttl_arm_sweep,
)
from order_forecast.api.pcf_queue_ttl_candidates import WORK_TTL_HOLD_STATUSES


NOW = datetime(2026, 8, 29, 22, 0, tzinfo=timezone.utc)
PATH = "pcf_processing_queue/988200/queue/1806248060"
HOLD_PATH = "routeEntitlementHolds/988200/features/scanner"
ALERT_PATH = "admin_alerts/ocr-review-988200-1806248060"


def intent(*, status: str = "complete") -> QueueTtlArmIntent:
    return QueueTtlArmIntent(
        work_id=uuid.UUID("11111111-2222-4333-8444-555555555555"),
        route="988200",
        delivery="1806248060",
        source_queue_path=PATH,
        source_revision="fsut1|2026-08-29T20:00:00.000000000Z|0",
        input_hash="a" * 64,
        work_attempt=1,
        work_status=status,
        work_completed_at=NOW - timedelta(hours=2),
        backup_event_id=uuid.UUID("22222222-3333-4444-8555-666666666666"),
        backup_covers_through=NOW - timedelta(hours=1),
        effect_event_ids=tuple(uuid.uuid4() for _ in range(5)),
        review_alert_path=ALERT_PATH if status == "review_scheduled" else None,
        eligible_at=NOW,
        expires_at=NOW + timedelta(days=7),
        intent_event_id=uuid.UUID("33333333-4444-4555-8666-777777777777"),
        evidence_sha256="b" * 64,
    )


def terminal_queue(**overrides: Any) -> dict[str, Any]:
    value = {
        "pendingBackendProcessing": False,
        "backendProcessingStatus": "done",
        "processingStatus": "processed",
        "status": "processed",
        "completedAt": "2026-08-29T20:00:00Z",
    }
    value.update(overrides)
    return value


class _Snapshot:
    def __init__(self, value: dict[str, Any] | None) -> None:
        self.exists = value is not None
        self._value = copy.deepcopy(value)

    def to_dict(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._value)


class _Reference:
    def __init__(self, db: "_Firestore", path: str) -> None:
        self.db = db
        self.path = path

    def get(self, *, transaction: Any = None) -> _Snapshot:
        if transaction is None and self.path == PATH and self.db.readback_override is not None:
            return _Snapshot(self.db.readback_override)
        return _Snapshot(self.db.values.get(self.path))


class _Transaction:
    def __init__(self, db: "_Firestore") -> None:
        self.db = db

    def update(self, reference: _Reference, patch: dict[str, Any]) -> None:
        self.db.updates.append((reference.path, copy.deepcopy(patch)))
        self.db.values[reference.path].update(copy.deepcopy(patch))


class _Firestore:
    def __init__(
        self,
        *,
        queue: dict[str, Any] | None = None,
        hold: dict[str, Any] | None = None,
        alert: dict[str, Any] | None = None,
    ) -> None:
        self.values: dict[str, dict[str, Any]] = {}
        if queue is not None:
            self.values[PATH] = copy.deepcopy(queue)
        if hold is not None:
            self.values[HOLD_PATH] = copy.deepcopy(hold)
        if alert is not None:
            self.values[ALERT_PATH] = copy.deepcopy(alert)
        self.updates: list[tuple[str, dict[str, Any]]] = []
        self.readback_override: dict[str, Any] | None = None

    def document(self, path: str) -> _Reference:
        return _Reference(self, path)

    def transaction(self) -> _Transaction:
        return _Transaction(self)


class _CycleConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _transactional(function):
    return function


class PcfQueueTtlArmingTests(unittest.TestCase):
    def test_review_scheduled_is_terminal_and_not_a_permanent_work_hold(self) -> None:
        self.assertNotIn("review_scheduled", WORK_TTL_HOLD_STATUSES)
        self.assertIn("dead_letter", WORK_TTL_HOLD_STATUSES)

    def test_exact_terminal_queue_arms_at_frozen_intent_expiry_and_reads_back(self) -> None:
        db = _Firestore(queue=terminal_queue())
        proof = arm_exact_queue_ttl(
            db,
            intent(),
            QueueTtlHoldSnapshot(frozenset(), 0, 0),
            now_fn=lambda: NOW + timedelta(hours=4),
            transactional=_transactional,
        )
        self.assertEqual(db.updates, [(PATH, {"pcfQueueExpiresAt": NOW + timedelta(days=7)})])
        self.assertEqual(proof.expires_at, NOW + timedelta(days=7))
        self.assertFalse(proof.idempotent_replay)

    def test_retry_is_idempotent_and_does_not_extend_retention(self) -> None:
        queue = terminal_queue(pcfQueueExpiresAt=NOW + timedelta(days=7))
        db = _Firestore(queue=queue)
        proof = arm_exact_queue_ttl(
            db,
            intent(),
            QueueTtlHoldSnapshot(frozenset(), 0, 0),
            now_fn=lambda: NOW + timedelta(days=1),
            transactional=_transactional,
        )
        self.assertEqual(db.updates, [])
        self.assertTrue(proof.idempotent_replay)
        self.assertEqual(proof.expires_at, NOW + timedelta(days=7))

    def test_expired_frozen_intent_never_arms_an_immediately_deletable_timestamp(self) -> None:
        db = _Firestore(queue=terminal_queue())
        with self.assertRaisesRegex(QueueTtlArmError, "intent_expired"):
            arm_exact_queue_ttl(
                db,
                intent(),
                QueueTtlHoldSnapshot(frozenset(), 0, 0),
                now_fn=lambda: NOW + timedelta(days=7),
                transactional=_transactional,
            )
        self.assertEqual(db.updates, [])

    def test_preloaded_alert_or_transaction_fresh_scanner_hold_blocks(self) -> None:
        held = QueueTtlHoldSnapshot(frozenset({PATH}), 0, 1)
        with self.assertRaisesRegex(QueueTtlArmError, "unresolved_firestore_hold"):
            arm_exact_queue_ttl(
                _Firestore(queue=terminal_queue()),
                intent(),
                held,
                now_fn=lambda: NOW,
                transactional=_transactional,
            )

        scanner = {
            "status": "entitlement_hold",
            "featureKey": "scanner",
            "heldDeliveryNumber": "1806248060",
        }
        with self.assertRaisesRegex(QueueTtlArmError, "unresolved_firestore_hold"):
            arm_exact_queue_ttl(
                _Firestore(queue=terminal_queue(), hold=scanner),
                intent(),
                QueueTtlHoldSnapshot(frozenset(), 0, 0),
                now_fn=lambda: NOW,
                transactional=_transactional,
            )

    def test_every_nonterminal_queue_shape_fails_without_write(self) -> None:
        cases = (
            {"pendingBackendProcessing": True},
            {"backendProcessingStatus": "in_progress"},
            {"processingStatus": "processing", "status": "processing"},
            {"status": "completed_with_errors"},
            {"maintenanceOperation": {"operationId": "active"}},
        )
        for patch in cases:
            with self.subTest(patch=patch):
                db = _Firestore(queue=terminal_queue(**patch))
                with self.assertRaisesRegex(QueueTtlArmError, "queue_not_closed"):
                    arm_exact_queue_ttl(
                        db,
                        intent(),
                        QueueTtlHoldSnapshot(frozenset(), 0, 0),
                        now_fn=lambda: NOW,
                        transactional=_transactional,
                    )
                self.assertEqual(db.updates, [])

    def test_low_confidence_requires_exact_resolved_review_alert(self) -> None:
        valid_alert = {
            "type": "ocr_post_send_review",
            "status": "resolved",
            "routeNumber": "988200",
            "deliveryNumber": "1806248060",
        }
        cases = (
            (None, "review_alert_missing"),
            ({**valid_alert, "status": "pending"}, "review_alert_unresolved"),
            ({**valid_alert, "routeNumber": "989262"}, "review_alert_unresolved"),
            ({**valid_alert, "type": "ocr_failure"}, "review_alert_unresolved"),
        )
        for alert, message in cases:
            with self.subTest(alert=alert):
                db = _Firestore(queue=terminal_queue(), alert=alert)
                with self.assertRaisesRegex(QueueTtlArmError, message):
                    arm_exact_queue_ttl(
                        db,
                        intent(status="review_scheduled"),
                        QueueTtlHoldSnapshot(frozenset(), 0, 0),
                        now_fn=lambda: NOW,
                        transactional=_transactional,
                    )
                self.assertEqual(db.updates, [])

        accepted = _Firestore(queue=terminal_queue(), alert=valid_alert)
        arm_exact_queue_ttl(
            accepted,
            intent(status="review_scheduled"),
            QueueTtlHoldSnapshot(frozenset(), 0, 0),
            now_fn=lambda: NOW,
            transactional=_transactional,
        )
        self.assertEqual(len(accepted.updates), 1)

    def test_independent_readback_mismatch_fails(self) -> None:
        db = _Firestore(queue=terminal_queue())
        db.readback_override = terminal_queue(pcfQueueExpiresAt=NOW + timedelta(days=6))
        with self.assertRaisesRegex(QueueTtlArmError, "arm_unverified"):
            arm_exact_queue_ttl(
                db,
                intent(),
                QueueTtlHoldSnapshot(frozenset(), 0, 0),
                now_fn=lambda: NOW,
                transactional=_transactional,
            )

    def test_cycle_commits_intent_before_firestore_and_receipt_after_readback(self) -> None:
        conn = _CycleConnection()
        expected_intent = intent()
        expected_proof = QueueTtlArmProof(
            source_queue_path=PATH,
            intent_event_id=expected_intent.intent_event_id,
            expires_at=expected_intent.expires_at,
            verified_at=NOW + timedelta(minutes=1),
            idempotent_replay=False,
        )
        expected_receipt = QueueTtlArmReceipt(uuid.uuid4(), NOW + timedelta(minutes=2), False)
        calls: list[str] = []

        def prepare(conn_arg, work_id, **kwargs):
            self.assertIs(conn_arg, conn)
            self.assertEqual(work_id, expected_intent.work_id)
            calls.append("prepare")
            return expected_intent

        def load_holds(db_arg, **kwargs):
            self.assertEqual(conn.commits, 1)
            calls.append("holds")
            return QueueTtlHoldSnapshot(frozenset(), 0, 0)

        def arm(db_arg, intent_arg, holds_arg, **kwargs):
            self.assertIs(intent_arg, expected_intent)
            calls.append("arm")
            return expected_proof

        def record(conn_arg, intent_arg, proof_arg, **kwargs):
            self.assertIs(proof_arg, expected_proof)
            calls.append("record")
            return expected_receipt

        report = run_queue_ttl_arm_cycle(
            conn,
            object(),
            expected_intent.work_id,
            actor_id="pcf-queue-ttl-worker",
            now_fn=lambda: NOW + timedelta(minutes=2),
            prepare_fn=prepare,
            load_holds_fn=load_holds,
            arm_fn=arm,
            record_fn=record,
        )
        self.assertEqual(calls, ["prepare", "holds", "arm", "record"])
        self.assertEqual(conn.commits, 2)
        self.assertEqual(conn.rollbacks, 0)
        self.assertIs(report.intent, expected_intent)
        self.assertIs(report.proof, expected_proof)
        self.assertIs(report.receipt, expected_receipt)

    def test_cycle_preserves_committed_intent_when_firestore_or_receipt_fails(self) -> None:
        conn = _CycleConnection()

        def fail_arm(*args, **kwargs):
            raise QueueTtlArmError("pcf_queue_ttl_arm_unverified")

        with self.assertRaisesRegex(QueueTtlArmError, "arm_unverified"):
            run_queue_ttl_arm_cycle(
                conn,
                object(),
                intent().work_id,
                actor_id="pcf-queue-ttl-worker",
                now_fn=lambda: NOW,
                prepare_fn=lambda *args, **kwargs: intent(),
                load_holds_fn=lambda *args, **kwargs: QueueTtlHoldSnapshot(
                    frozenset(), 0, 0
                ),
                arm_fn=fail_arm,
            )
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 1)

    def test_receipt_rejects_a_proof_timestamp_from_before_the_intent(self) -> None:
        frozen = intent()
        proof = QueueTtlArmProof(
            source_queue_path=frozen.source_queue_path,
            intent_event_id=frozen.intent_event_id,
            expires_at=frozen.expires_at,
            verified_at=frozen.eligible_at - timedelta(minutes=1),
            idempotent_replay=False,
        )
        with self.assertRaisesRegex(QueueTtlArmError, "receipt_proof_mismatch"):
            record_queue_ttl_armed(
                object(),
                frozen,
                proof,
                actor_id="pcf-queue-ttl-worker",
                now=NOW,
            )

    def test_sweep_loads_holds_once_and_never_intents_a_held_candidate(self) -> None:
        conn = _CycleConnection()
        first = QueueTtlArmCandidate(intent().work_id, PATH, "988200", "1806248060", "complete")
        second = QueueTtlArmCandidate(
            uuid.uuid4(),
            "pcf_processing_queue/989262/queue/1806248061",
            "989262",
            "1806248061",
            "complete",
        )
        hold_calls = 0
        cycle_calls = []

        def load_holds(*_args, **_kwargs):
            nonlocal hold_calls
            hold_calls += 1
            return QueueTtlHoldSnapshot(frozenset({first.source_queue_path}), 0, 1)

        def cycle(_conn, _db, work_id, **kwargs):
            cycle_calls.append(work_id)
            shared = kwargs["load_holds_fn"](_db, maximum=100)
            self.assertIn(first.source_queue_path, shared.paths)
            frozen = intent()
            proof = QueueTtlArmProof(
                frozen.source_queue_path,
                frozen.intent_event_id,
                frozen.expires_at,
                NOW,
                False,
            )
            receipt = QueueTtlArmReceipt(uuid.uuid4(), NOW, False)
            return QueueTtlArmCycleReport(frozen, proof, receipt)

        report = run_queue_ttl_arm_sweep(
            conn,
            object(),
            actor_id="pcf-queue-ttl-worker",
            load_candidates_fn=lambda *_args, **_kwargs: (first, second),
            load_holds_fn=load_holds,
            cycle_fn=cycle,
            now_fn=lambda: NOW,
        )

        self.assertEqual(hold_calls, 1)
        self.assertEqual(cycle_calls, [second.work_id])
        self.assertEqual(report.candidate_count, 2)
        self.assertEqual(report.held_count, 1)
        self.assertEqual(report.armed_count, 1)

    def test_sweep_defers_missing_backup_without_hiding_later_candidate(self) -> None:
        conn = _CycleConnection()
        candidates = (
            QueueTtlArmCandidate(uuid.uuid4(), PATH, "988200", "1806248060", "complete"),
            QueueTtlArmCandidate(
                uuid.uuid4(),
                "pcf_processing_queue/989262/queue/1806248061",
                "989262",
                "1806248061",
                "complete",
            ),
        )
        calls = 0

        def cycle(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise QueueTtlArmError("pcf_queue_ttl_current_backup_missing")
            frozen = intent()
            return QueueTtlArmCycleReport(
                frozen,
                QueueTtlArmProof(
                    frozen.source_queue_path,
                    frozen.intent_event_id,
                    frozen.expires_at,
                    NOW,
                    True,
                ),
                QueueTtlArmReceipt(uuid.uuid4(), NOW, False),
            )

        report = run_queue_ttl_arm_sweep(
            conn,
            object(),
            actor_id="pcf-queue-ttl-worker",
            load_candidates_fn=lambda *_args, **_kwargs: candidates,
            load_holds_fn=lambda *_args, **_kwargs: QueueTtlHoldSnapshot(
                frozenset(), 0, 0
            ),
            cycle_fn=cycle,
            now_fn=lambda: NOW,
        )

        self.assertEqual(calls, 2)
        self.assertEqual(report.deferred_count, 1)
        self.assertEqual(report.armed_count, 1)
        self.assertEqual(report.replay_count, 1)


if __name__ == "__main__":
    unittest.main()

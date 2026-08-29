"""Adversarial unit coverage for the fixed PCF effect-step authority."""

from __future__ import annotations

import copy
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from order_forecast.api.pcf_effect_runtime import (
    EFFECT_STEPS,
    EffectClaim,
    EffectProof,
    advance_effect_step,
    claim_due_effect,
    schedule_effect_retry,
)
from order_forecast.api.pcf_work_runtime import WorkApiError


NOW = datetime(2026, 8, 29, 21, 0, tzinfo=timezone.utc)
WORK_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")


def effect_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "work_id": WORK_ID,
        "route_number": "988200",
        "delivery_number": "1806248060",
        "source_queue_path": "pcf_processing_queue/988200/queue/1806248060",
        "source_revision": "fsut1|2026-08-29T20:00:00.000000000Z|0",
        "input_hash_version": "pcf-work-input-v1",
        "input_hash": "a" * 64,
        "effect_step": "projecting",
        "effect_attempt_count": 0,
        "attempt_count": 1,
        "max_attempts": 3,
        "confidence": 96,
        "manifest": {"objects": [{"generation": "123"}]},
        "ocr_context": {"validatedLoadingDate": "08/01/2026"},
        "result": {"request": {"proposal": {"confidence": 96}}},
    }
    row.update(overrides)
    return row


def effect_claim(**overrides: Any) -> EffectClaim:
    values: dict[str, Any] = {
        "work_id": WORK_ID,
        "route": "988200",
        "delivery": "1806248060",
        "source_queue_path": "pcf_processing_queue/988200/queue/1806248060",
        "source_revision": "fsut1|2026-08-29T20:00:00.000000000Z|0",
        "input_hash_version": "pcf-work-input-v1",
        "input_hash": "a" * 64,
        "step": "projecting",
        "effect_attempt": 1,
        "work_attempt": 1,
        "confidence": 96,
        "manifest": {"objects": [{"generation": "123"}]},
        "ocr_context": {"validatedLoadingDate": "08/01/2026"},
        "result": {"request": {"proposal": {"confidence": 96}}},
        "claimed_at": NOW,
        "visible_again_at": NOW + timedelta(minutes=2),
    }
    values.update(overrides)
    return EffectClaim(**values)


def effect_proof(claim: EffectClaim, *, verified_at: datetime = NOW) -> EffectProof:
    return EffectProof(
        work_id=claim.work_id,
        step=claim.step,
        effect_attempt=claim.effect_attempt,
        disposition="applied",
        receipt_id=f"effect-proof:{claim.step}:{claim.effect_attempt}",
        evidence_sha256="f" * 64,
        verified_at=verified_at,
    )


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection
        self.rowcount = -1
        self._fetchone: Any = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, params))
        if normalized.startswith("SELECT work_id"):
            self._fetchone = copy.deepcopy(self.connection.selected_row)
            self.rowcount = 1 if self._fetchone is not None else 0
        elif normalized.startswith("UPDATE pcf_work_items"):
            self.rowcount = (
                self.connection.update_rowcounts.pop(0)
                if self.connection.update_rowcounts
                else 1
            )
        elif normalized.startswith("INSERT INTO pcf_work_events"):
            self.rowcount = 1
        else:  # pragma: no cover - a new query must be modeled deliberately
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self) -> Any:
        return self._fetchone


class _Connection:
    def __init__(
        self,
        selected_row: dict[str, Any] | None = None,
        *,
        update_rowcounts: list[int] | None = None,
    ) -> None:
        self.selected_row = selected_row
        self.update_rowcounts = list(update_rowcounts or [])
        self.statements: list[tuple[str, Any]] = []

    def cursor(self, **_kwargs: Any) -> _Cursor:
        return _Cursor(self)


class PcfEffectRuntimeTests(unittest.TestCase):
    def test_claim_is_bounded_due_skip_locked_and_moves_visibility(self) -> None:
        connection = _Connection(effect_row())

        claimed = claim_due_effect(
            connection,
            actor_id="pcf-effect-worker-01",
            now=NOW,
            visibility_seconds=120,
        )

        self.assertIsNotNone(claimed)
        assert claimed is not None
        self.assertEqual(claimed.effect_attempt, 1)
        self.assertEqual(claimed.visible_again_at, NOW + timedelta(seconds=120))
        select_sql, select_params = connection.statements[0]
        self.assertIn("WHERE status = 'effects_pending' AND effect_not_before <= %s", select_sql)
        self.assertIn("ORDER BY effect_not_before, updated_at, work_id", select_sql)
        self.assertIn("FOR UPDATE SKIP LOCKED LIMIT 1", select_sql)
        self.assertEqual(select_params, (NOW,))
        update_sql, update_params = connection.statements[1]
        self.assertIn("SET effect_attempt_count = %s, effect_not_before = %s", update_sql)
        self.assertEqual(update_params[0:3], (1, NOW + timedelta(seconds=120), NOW))
        self.assertEqual(update_params[-2:], ("projecting", 0))
        self.assertTrue(connection.statements[2][0].startswith("INSERT INTO pcf_work_events"))

    def test_empty_due_set_performs_no_mutation(self) -> None:
        connection = _Connection()
        self.assertIsNone(
            claim_due_effect(
                connection,
                actor_id="pcf-effect-worker-01",
                now=NOW,
                visibility_seconds=120,
            )
        )
        self.assertEqual(len(connection.statements), 1)

    def test_malformed_durable_payload_fails_before_update(self) -> None:
        for change in (
            {"result": None},
            {"manifest": []},
            {"source_queue_path": "pcf_processing_queue/988200/queue/other"},
            {"confidence": None},
            {"effect_attempt_count": True},
        ):
            with self.subTest(change=change):
                connection = _Connection(effect_row(**change))
                with self.assertRaises(WorkApiError) as raised:
                    claim_due_effect(
                        connection,
                        actor_id="pcf-effect-worker-01",
                        now=NOW,
                        visibility_seconds=120,
                    )
                self.assertEqual(raised.exception.code, "invalid_work_state")
                self.assertEqual(len(connection.statements), 1)

    def test_claim_rejects_naive_clock_and_unbounded_visibility(self) -> None:
        for observed, seconds in (
            (NOW.replace(tzinfo=None), 120),
            (NOW, 29),
            (NOW, 901),
            (NOW, True),
        ):
            with self.subTest(observed=observed, seconds=seconds):
                connection = _Connection(effect_row())
                with self.assertRaises(ValueError):
                    claim_due_effect(
                        connection,
                        actor_id="pcf-effect-worker-01",
                        now=observed,
                        visibility_seconds=seconds,
                    )
                self.assertEqual(connection.statements, [])

    def test_exhausted_current_step_dead_letters_without_external_claim(self) -> None:
        connection = _Connection(effect_row(effect_attempt_count=3, max_attempts=3))

        claimed = claim_due_effect(
            connection,
            actor_id="pcf-effect-worker-01",
            now=NOW,
            visibility_seconds=120,
        )

        self.assertIsNone(claimed)
        update_sql, update_params = connection.statements[1]
        self.assertIn("SET status = 'dead_letter', effect_step = NULL", update_sql)
        self.assertEqual(update_params[-2:], ("projecting", 3))
        self.assertTrue(connection.statements[2][0].startswith("INSERT INTO pcf_work_events"))

    def test_claim_compare_and_set_conflict_fails_closed(self) -> None:
        connection = _Connection(effect_row(), update_rowcounts=[0])
        with self.assertRaises(WorkApiError) as raised:
            claim_due_effect(
                connection,
                actor_id="pcf-effect-worker-01",
                now=NOW,
                visibility_seconds=120,
            )
        self.assertEqual(raised.exception.code, "invalid_work_state")
        self.assertEqual(len(connection.statements), 2)

    def test_successful_advance_is_monotonic_and_resets_current_step_attempts(self) -> None:
        for current, expected_next in zip(EFFECT_STEPS[:-1], EFFECT_STEPS[1:]):
            with self.subTest(current=current):
                connection = _Connection()
                claim = effect_claim(step=current, effect_attempt=2)
                result = advance_effect_step(
                    connection,
                    claim,
                    actor_id="pcf-effect-worker-01",
                    now=NOW,
                    proof=effect_proof(claim),
                )
                self.assertEqual(result.next_step, expected_next)
                self.assertEqual(result.status, "effects_pending")
                update_sql, params = connection.statements[0]
                self.assertIn("SET effect_step = %s, effect_attempt_count = 0", update_sql)
                self.assertEqual(params, (expected_next, NOW, NOW, str(WORK_ID), current, 2))

    def test_only_plan_authorized_skips_are_accepted(self) -> None:
        accepted = (
            ("cleaning", "superseded_input"),
            ("notifying", "notification_not_required"),
        )
        for step, reason in accepted:
            with self.subTest(step=step, reason=reason):
                connection = _Connection()
                result = advance_effect_step(
                    connection,
                    effect_claim(step=step),
                    actor_id="pcf-effect-worker-01",
                    now=NOW,
                    outcome="skipped",
                    reason=reason,
                )
                self.assertEqual(result.completed_step, step)
                self.assertTrue(connection.statements[1][0].startswith("INSERT INTO pcf_work_events"))

        for step, reason in (
            ("projecting", "notification_not_required"),
            ("verifying", "superseded_input"),
            ("cleaning", "arbitrary_reason"),
        ):
            with self.subTest(rejected_step=step, reason=reason):
                connection = _Connection()
                with self.assertRaises(ValueError):
                    advance_effect_step(
                        connection,
                        effect_claim(step=step),
                        actor_id="pcf-effect-worker-01",
                        now=NOW,
                        outcome="skipped",
                        reason=reason,
                    )
                self.assertEqual(connection.statements, [])

    def test_unknown_outcome_never_defaults_to_completed(self) -> None:
        connection = _Connection()
        with self.assertRaises(ValueError):
            advance_effect_step(
                connection,
                effect_claim(),
                actor_id="pcf-effect-worker-01",
                now=NOW,
                outcome="ignored",  # type: ignore[arg-type]
            )
        self.assertEqual(connection.statements, [])

    def test_summarizing_terminal_state_is_confidence_gated(self) -> None:
        for confidence, expected in ((92, "complete"), (91, "review_scheduled")):
            with self.subTest(confidence=confidence):
                connection = _Connection()
                claim = effect_claim(step="summarizing", confidence=confidence)
                result = advance_effect_step(
                    connection,
                    claim,
                    actor_id="pcf-effect-worker-01",
                    now=NOW,
                    proof=effect_proof(claim),
                )
                self.assertEqual(result.next_step, None)
                self.assertEqual(result.status, expected)
                update_sql, params = connection.statements[0]
                self.assertIn("SET status = %s, effect_step = NULL", update_sql)
                self.assertEqual(params[0], expected)

    def test_stale_worker_cannot_advance_or_schedule_retry(self) -> None:
        for operation in ("advance", "retry"):
            with self.subTest(operation=operation):
                connection = _Connection(update_rowcounts=[0])
                with self.assertRaises(WorkApiError) as raised:
                    if operation == "advance":
                        claim = effect_claim(effect_attempt=1)
                        advance_effect_step(
                            connection,
                            claim,
                            actor_id="pcf-effect-worker-01",
                            now=NOW,
                            proof=effect_proof(claim),
                        )
                    else:
                        schedule_effect_retry(
                            connection,
                            effect_claim(effect_attempt=1),
                            actor_id="pcf-effect-worker-01",
                            now=NOW,
                            delay_seconds=30,
                            error_code="firebase_unavailable",
                        )
                self.assertEqual(raised.exception.code, "stale_effect_attempt")
                self.assertEqual(len(connection.statements), 1)

    def test_completed_step_requires_identity_bound_proof_before_sql(self) -> None:
        claim = effect_claim()
        invalid = (
            None,
            effect_proof(claim, verified_at=NOW + timedelta(seconds=31)),
            effect_proof(claim, verified_at=NOW - timedelta(seconds=31)),
            EffectProof(
                work_id=uuid.uuid4(),
                step=claim.step,
                effect_attempt=claim.effect_attempt,
                disposition="applied",
                receipt_id="effect-proof:wrong-work",
                evidence_sha256="f" * 64,
                verified_at=NOW,
            ),
            EffectProof(
                work_id=claim.work_id,
                step="verifying",
                effect_attempt=claim.effect_attempt,
                disposition="applied",
                receipt_id="effect-proof:wrong-step",
                evidence_sha256="f" * 64,
                verified_at=NOW,
            ),
        )
        for proof in invalid:
            with self.subTest(proof=proof):
                connection = _Connection()
                with self.assertRaises(ValueError):
                    advance_effect_step(
                        connection,
                        claim,
                        actor_id="pcf-effect-worker-01",
                        now=NOW,
                        proof=proof,
                    )
                self.assertEqual(connection.statements, [])

    def test_retry_keeps_step_and_moves_only_bounded_visibility(self) -> None:
        connection = _Connection()
        retry_at = schedule_effect_retry(
            connection,
            effect_claim(step="verifying", effect_attempt=2),
            actor_id="pcf-effect-worker-01",
            now=NOW,
            delay_seconds=30,
            error_code="firebase_unavailable",
        )
        self.assertEqual(retry_at, NOW + timedelta(seconds=30))
        update_sql, params = connection.statements[0]
        self.assertNotIn("SET effect_step =", update_sql)
        self.assertEqual(
            params,
            (
                NOW + timedelta(seconds=30),
                NOW,
                "firebase_unavailable",
                str(WORK_ID),
                "verifying",
                2,
            ),
        )

        for seconds in (4, 3601, True):
            with self.subTest(seconds=seconds):
                bounded = _Connection()
                with self.assertRaises(ValueError):
                    schedule_effect_retry(
                        bounded,
                        effect_claim(),
                        actor_id="pcf-effect-worker-01",
                        now=NOW,
                        delay_seconds=seconds,
                        error_code="firebase_unavailable",
                    )
                self.assertEqual(bounded.statements, [])


if __name__ == "__main__":
    unittest.main()

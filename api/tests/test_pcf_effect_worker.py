"""Fail-closed transaction tests for the fixed PCF effect worker loop."""

from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timedelta, timezone

from order_forecast.api.pcf_effect_runtime import (
    EFFECT_STEPS,
    EffectAdvance,
    EffectClaim,
    EffectProof,
)
from order_forecast.api.pcf_effect_worker import (
    EffectSkip,
    RetryableEffectError,
    run_one_effect,
)


NOW = datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)
WORK_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")


class _Connection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def claim(*, step: str = "projecting") -> EffectClaim:
    return EffectClaim(
        work_id=WORK_ID,
        route="988200",
        delivery="1806248060",
        source_queue_path="pcf_processing_queue/988200/queue/1806248060",
        source_revision="fsut1|2026-08-29T20:00:00.000000000Z|0",
        input_hash_version="pcf-work-input-v1",
        input_hash="a" * 64,
        step=step,
        effect_attempt=1,
        work_attempt=1,
        confidence=96,
        manifest={"objects": [{"generation": "123"}]},
        ocr_context={"validatedLoadingDate": "08/01/2026"},
        result={"request": {"proposal": {"confidence": 96}}},
        claimed_at=NOW,
        visible_again_at=NOW + timedelta(minutes=2),
    )


def proof(value: EffectClaim) -> EffectProof:
    return EffectProof(
        work_id=value.work_id,
        step=value.step,
        effect_attempt=value.effect_attempt,
        disposition="applied",
        receipt_id=f"effect-proof:{value.step}:attempt-1",
        evidence_sha256="a" * 64,
        verified_at=NOW + timedelta(seconds=1),
    )


def handlers(handler):
    return {step: handler for step in EFFECT_STEPS}


class PcfEffectWorkerTests(unittest.TestCase):
    def test_handler_set_is_exact_and_checked_before_claim(self) -> None:
        for value in (
            {},
            {
                **handlers(lambda item: proof(item)),
                "extra": lambda item: proof(item),
            },
        ):
            with self.subTest(keys=value.keys()):
                conn = _Connection()
                with self.assertRaisesRegex(ValueError, "fixed step set"):
                    run_one_effect(conn, value, actor_id="pcf-effect-worker-01")
                self.assertEqual((conn.commits, conn.rollbacks), (0, 0))

    def test_idle_lookup_commits_and_never_calls_a_handler(self) -> None:
        conn = _Connection()
        called = []
        report = run_one_effect(
            conn,
            handlers(lambda _item: called.append(True)),
            actor_id="pcf-effect-worker-01",
            now_fn=lambda: NOW,
            claim_fn=lambda *args, **kwargs: None,
        )
        self.assertEqual(report.disposition, "idle")
        self.assertEqual(called, [])
        self.assertEqual((conn.commits, conn.rollbacks), (1, 0))

    def test_claim_commits_before_handler_and_proof_advances_once(self) -> None:
        conn = _Connection()
        current = claim()
        advances = []

        def handler(item):
            self.assertEqual(conn.commits, 1)
            return proof(item)

        def advance(conn_arg, item, **kwargs):
            advances.append(kwargs)
            return EffectAdvance(item.work_id, item.step, "verifying", "effects_pending")

        report = run_one_effect(
            conn,
            handlers(handler),
            actor_id="pcf-effect-worker-01",
            now_fn=lambda: NOW + timedelta(seconds=2),
            claim_fn=lambda *args, **kwargs: current,
            advance_fn=advance,
        )
        self.assertEqual(report.disposition, "completed")
        self.assertIs(advances[0]["proof"].work_id, current.work_id)
        self.assertEqual((conn.commits, conn.rollbacks), (2, 0))

    def test_deterministic_skip_uses_advance_without_a_proof(self) -> None:
        conn = _Connection()
        current = claim(step="notifying")
        observed = {}

        def advance(_conn, item, **kwargs):
            observed.update(kwargs)
            return EffectAdvance(item.work_id, item.step, "summarizing", "effects_pending")

        report = run_one_effect(
            conn,
            handlers(lambda _item: EffectSkip("notification_not_required")),
            actor_id="pcf-effect-worker-01",
            now_fn=lambda: NOW,
            claim_fn=lambda *args, **kwargs: current,
            advance_fn=advance,
        )
        self.assertEqual(report.disposition, "skipped")
        self.assertEqual(observed["outcome"], "skipped")
        self.assertEqual(observed["reason"], "notification_not_required")

    def test_typed_and_unexpected_failures_schedule_bounded_retry(self) -> None:
        for raised, expected_code, expected_delay in (
            (
                RetryableEffectError("firebase_unavailable", delay_seconds=30),
                "firebase_unavailable",
                30,
            ),
            (RuntimeError("secret must not persist"), "effect_handler_failed", 60),
        ):
            with self.subTest(code=expected_code):
                conn = _Connection()
                observed = {}

                def handler(_item):
                    raise raised

                def retry(_conn, _claim, **kwargs):
                    observed.update(kwargs)
                    return kwargs["now"] + timedelta(seconds=kwargs["delay_seconds"])

                report = run_one_effect(
                    conn,
                    handlers(handler),
                    actor_id="pcf-effect-worker-01",
                    now_fn=lambda: NOW,
                    claim_fn=lambda *args, **kwargs: claim(),
                    retry_fn=retry,
                )
                self.assertEqual(report.disposition, "retry_scheduled")
                self.assertEqual(
                    (observed["error_code"], observed["delay_seconds"]),
                    (expected_code, expected_delay),
                )
                self.assertEqual((conn.commits, conn.rollbacks), (2, 0))

    def test_invalid_handler_result_cannot_advance(self) -> None:
        conn = _Connection()
        advances = []
        retries = []

        def retry(_conn, _claim, **kwargs):
            retries.append(kwargs)
            return NOW + timedelta(seconds=60)

        report = run_one_effect(
            conn,
            handlers(lambda _item: object()),
            actor_id="pcf-effect-worker-01",
            now_fn=lambda: NOW,
            claim_fn=lambda *args, **kwargs: claim(),
            advance_fn=lambda *args, **kwargs: advances.append(kwargs),
            retry_fn=retry,
        )
        self.assertEqual(report.error_code, "effect_result_invalid")
        self.assertEqual(advances, [])
        self.assertEqual(len(retries), 1)

    def test_failed_cas_rolls_back_the_second_transaction(self) -> None:
        conn = _Connection()

        def fail_advance(*args, **kwargs):
            raise RuntimeError("stale")

        with self.assertRaisesRegex(RuntimeError, "stale"):
            run_one_effect(
                conn,
                handlers(lambda item: proof(item)),
                actor_id="pcf-effect-worker-01",
                now_fn=lambda: NOW + timedelta(seconds=2),
                claim_fn=lambda *args, **kwargs: claim(),
                advance_fn=fail_advance,
            )
        self.assertEqual((conn.commits, conn.rollbacks), (1, 1))


if __name__ == "__main__":
    unittest.main()

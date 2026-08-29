from __future__ import annotations

import uuid
from datetime import datetime, timezone

from order_forecast.api.pcf_effect_controller import run_effect_controller_cycle
from order_forecast.api.pcf_effect_worker import EffectCycleReport
from order_forecast.api.pcf_queue_ttl_arming import QueueTtlArmError


NOW = datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)
WORK_ID = str(uuid.uuid4())


def effect(*, status="complete", next_step=None):
    return EffectCycleReport(
        "completed",
        WORK_ID,
        "summarizing",
        next_step,
        status,
        None,
        None,
    )


def test_nonterminal_and_review_work_never_call_ttl():
    ttl_calls = []
    for report, expected in (
        (effect(next_step="verifying", status="effects_pending"), "not_terminal"),
        (effect(status="review_scheduled"), "review_pending"),
    ):
        result = run_effect_controller_cycle(
            object(),
            object(),
            {},
            actor_id="pcf-effect-worker",
            now_fn=lambda: NOW,
            effect_fn=lambda *_args, report=report, **_kwargs: report,
            ttl_fn=lambda *_args, **_kwargs: ttl_calls.append(True),
        )
        assert result.queue_ttl_disposition == expected
    assert ttl_calls == []


def test_exact_admission_is_forwarded_to_effect_claim():
    observed = []
    admitted = (uuid.UUID(WORK_ID),)
    result = run_effect_controller_cycle(
        object(),
        object(),
        {},
        actor_id="pcf-effect-worker",
        admitted_work_ids=admitted,
        now_fn=lambda: NOW,
        effect_fn=lambda *_args, **kwargs: (
            observed.append(kwargs["admitted_work_ids"]) or effect(next_step="verifying")
        ),
    )
    assert result.queue_ttl_disposition == "not_terminal"
    assert observed == [admitted]


def test_complete_work_defers_cleanly_until_covering_backup():
    result = run_effect_controller_cycle(
        object(),
        object(),
        {},
        actor_id="pcf-effect-worker",
        now_fn=lambda: NOW,
        effect_fn=lambda *_args, **_kwargs: effect(),
        ttl_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            QueueTtlArmError("pcf_queue_ttl_current_backup_missing")
        ),
    )

    assert result.effect.status == "complete"
    assert result.queue_ttl_disposition == "deferred_until_backup"
    assert result.queue_ttl is None


def test_unexpected_ttl_failure_is_not_hidden():
    try:
        run_effect_controller_cycle(
            object(),
            object(),
            {},
            actor_id="pcf-effect-worker",
            now_fn=lambda: NOW,
            effect_fn=lambda *_args, **_kwargs: effect(),
            ttl_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                QueueTtlArmError("pcf_queue_ttl_arm_unverified")
            ),
        )
    except QueueTtlArmError as exc:
        assert str(exc) == "pcf_queue_ttl_arm_unverified"
    else:
        raise AssertionError("unexpected TTL failure was hidden")

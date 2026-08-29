"""One disabled cluster effect cycle plus terminal queue-TTL handoff."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Mapping
from uuid import UUID

from .pcf_effect_worker import EffectCycleReport, EffectHandler, run_one_effect
from .pcf_queue_ttl_arming import (
    QueueTtlArmCycleReport,
    QueueTtlArmError,
    run_queue_ttl_arm_cycle,
)


@dataclass(frozen=True)
class EffectControllerReport:
    effect: EffectCycleReport
    queue_ttl_disposition: Literal[
        "not_terminal",
        "review_pending",
        "deferred_until_backup",
        "armed",
        "replayed",
    ]
    queue_ttl: QueueTtlArmCycleReport | None


def run_effect_controller_cycle(
    conn: Any,
    db: Any,
    handlers: Mapping[str, EffectHandler],
    *,
    actor_id: str,
    admitted_work_ids: tuple[UUID, ...] | None = None,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    effect_fn: Callable[..., EffectCycleReport] = run_one_effect,
    claim_fn: Callable[..., Any] | None = None,
    ttl_fn: Callable[..., QueueTtlArmCycleReport] = run_queue_ttl_arm_cycle,
) -> EffectControllerReport:
    """Run one effect and arm only a newly complete high-confidence workflow.

    The effect transition commits inside ``effect_fn``. A missing covering
    backup is expected and leaves the completed work for the daily receipt
    sweep; it never turns a successful finalization back into an effect retry.
    Low-confidence work remains held until its durable review alert resolves.
    """
    effect_kwargs: dict[str, Any] = {
        "actor_id": actor_id,
        "admitted_work_ids": admitted_work_ids,
        "now_fn": now_fn,
    }
    if claim_fn is not None:
        effect_kwargs["claim_fn"] = claim_fn
    effect = effect_fn(conn, handlers, **effect_kwargs)
    if effect.next_step is not None or effect.work_id is None:
        return EffectControllerReport(effect, "not_terminal", None)
    if effect.status == "review_scheduled":
        return EffectControllerReport(effect, "review_pending", None)
    if effect.status != "complete":
        return EffectControllerReport(effect, "not_terminal", None)
    try:
        report = ttl_fn(
            conn,
            db,
            UUID(effect.work_id),
            actor_id=actor_id,
            now_fn=now_fn,
        )
    except QueueTtlArmError as exc:
        if str(exc) in {
            "pcf_queue_ttl_current_backup_missing",
            "pcf_queue_ttl_successor_or_work_hold",
        }:
            return EffectControllerReport(effect, "deferred_until_backup", None)
        raise
    disposition = (
        "replayed"
        if report.proof.idempotent_replay or report.receipt.idempotent_replay
        else "armed"
    )
    return EffectControllerReport(effect, disposition, report)

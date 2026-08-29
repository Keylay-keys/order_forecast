"""Transaction-safe runner for one fixed PCF cluster effect."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from .pcf_effect_runtime import (
    EFFECT_STEPS,
    EffectAdvance,
    EffectClaim,
    EffectProof,
    advance_effect_step,
    claim_due_effect,
    schedule_effect_retry,
)


_ERROR_RE = re.compile(r"^[a-z][a-z0-9_]{2,95}$")


@dataclass(frozen=True)
class EffectSkip:
    reason: str


class RetryableEffectError(RuntimeError):
    """A typed pre-completion failure that may consume one bounded retry."""

    def __init__(self, code: str, *, delay_seconds: int) -> None:
        if not isinstance(code, str) or _ERROR_RE.fullmatch(code) is None:
            raise ValueError("invalid PCF effect retry code")
        if (
            isinstance(delay_seconds, bool)
            or not isinstance(delay_seconds, int)
            or not 5 <= delay_seconds <= 3600
        ):
            raise ValueError("invalid PCF effect retry delay")
        super().__init__(code)
        self.code = code
        self.delay_seconds = delay_seconds


@dataclass(frozen=True)
class EffectCycleReport:
    disposition: Literal["idle", "completed", "skipped", "retry_scheduled"]
    work_id: str | None
    step: str | None
    next_step: str | None
    status: str | None
    retry_at: datetime | None
    error_code: str | None


EffectHandler = Callable[[EffectClaim], EffectProof | EffectSkip]


def _handlers(value: Mapping[str, EffectHandler]) -> dict[str, EffectHandler]:
    if not isinstance(value, Mapping) or frozenset(value) != frozenset(EFFECT_STEPS):
        raise ValueError("PCF effect handlers must match the fixed step set")
    copied = dict(value)
    if any(not callable(handler) for handler in copied.values()):
        raise ValueError("PCF effect handler is not callable")
    return copied


def _retry(
    conn: Any,
    claim: EffectClaim,
    *,
    actor_id: str,
    now: datetime,
    delay_seconds: int,
    error_code: str,
    retry_fn: Callable[..., datetime],
) -> EffectCycleReport:
    retry_at = retry_fn(
        conn,
        claim,
        actor_id=actor_id,
        now=now,
        delay_seconds=delay_seconds,
        error_code=error_code,
    )
    conn.commit()
    return EffectCycleReport(
        "retry_scheduled",
        str(claim.work_id),
        claim.step,
        claim.step,
        "effects_pending",
        retry_at,
        error_code,
    )


def run_one_effect(
    conn: Any,
    handlers: Mapping[str, EffectHandler],
    *,
    actor_id: str,
    visibility_seconds: int = 120,
    unexpected_retry_seconds: int = 60,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    claim_fn: Callable[..., EffectClaim | None] = claim_due_effect,
    advance_fn: Callable[..., EffectAdvance] = advance_effect_step,
    retry_fn: Callable[..., datetime] = schedule_effect_retry,
) -> EffectCycleReport:
    """Claim, commit, execute, then CAS one effect without holding a DB lock.

    Handlers own their exact external idempotency/read-back contract and may
    return only a proof or an allowlisted deterministic skip. Any other result
    is retained on the current step and retried; it can never advance work.
    """
    fixed_handlers = _handlers(handlers)
    if (
        isinstance(unexpected_retry_seconds, bool)
        or not isinstance(unexpected_retry_seconds, int)
        or not 5 <= unexpected_retry_seconds <= 3600
    ):
        raise ValueError("invalid unexpected PCF effect retry delay")

    try:
        claim = claim_fn(
            conn,
            actor_id=actor_id,
            now=now_fn(),
            visibility_seconds=visibility_seconds,
        )
        # This commits either the visibility claim, an exhausted-attempt dead
        # letter, or the bounded empty lookup before any handler can run.
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if claim is None:
        return EffectCycleReport("idle", None, None, None, None, None, None)

    try:
        result = fixed_handlers[claim.step](claim)
    except RetryableEffectError as exc:
        try:
            return _retry(
                conn,
                claim,
                actor_id=actor_id,
                now=now_fn(),
                delay_seconds=exc.delay_seconds,
                error_code=exc.code,
                retry_fn=retry_fn,
            )
        except Exception:
            conn.rollback()
            raise
    except Exception:
        try:
            return _retry(
                conn,
                claim,
                actor_id=actor_id,
                now=now_fn(),
                delay_seconds=unexpected_retry_seconds,
                error_code="effect_handler_failed",
                retry_fn=retry_fn,
            )
        except Exception:
            conn.rollback()
            raise

    try:
        if isinstance(result, EffectProof):
            advance = advance_fn(
                conn,
                claim,
                actor_id=actor_id,
                now=now_fn(),
                proof=result,
            )
            disposition: Literal["completed", "skipped"] = "completed"
        elif isinstance(result, EffectSkip):
            advance = advance_fn(
                conn,
                claim,
                actor_id=actor_id,
                now=now_fn(),
                outcome="skipped",
                reason=result.reason,
            )
            disposition = "skipped"
        else:
            return _retry(
                conn,
                claim,
                actor_id=actor_id,
                now=now_fn(),
                delay_seconds=unexpected_retry_seconds,
                error_code="effect_result_invalid",
                retry_fn=retry_fn,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return EffectCycleReport(
        disposition,
        str(claim.work_id),
        claim.step,
        advance.next_step,
        advance.status,
        None,
        None,
    )

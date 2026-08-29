"""Bounded PostgreSQL authority for the fixed PCF effect-step sequence."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from psycopg2.extras import RealDictCursor

from .pcf_work_runtime import WorkApiError, _event


EFFECT_STEPS = ("projecting", "verifying", "cleaning", "notifying", "summarizing")
_ACTOR_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{2,127}$")
_ERROR_RE = re.compile(r"^[a-z][a-z0-9_]{2,95}$")
_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]{7,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_DELIVERY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SOURCE_REVISION_RE = re.compile(
    r"^fsut1\|[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
    r"[0-9]{2}\.[0-9]{9}Z\|0$"
)
_SKIPPABLE_EFFECT_REASONS = {
    "cleaning": frozenset({"superseded_input"}),
    "notifying": frozenset({"notification_not_required"}),
}


@dataclass(frozen=True)
class EffectClaim:
    work_id: uuid.UUID
    route: str
    delivery: str
    source_queue_path: str
    source_revision: str
    input_hash_version: str
    input_hash: str
    step: str
    effect_attempt: int
    work_attempt: int
    confidence: int
    manifest: dict[str, Any]
    ocr_context: dict[str, Any]
    result: dict[str, Any]
    claimed_at: datetime
    visible_again_at: datetime


@dataclass(frozen=True)
class EffectAdvance:
    work_id: uuid.UUID
    completed_step: str
    next_step: str | None
    status: str


@dataclass(frozen=True)
class EffectProof:
    work_id: uuid.UUID
    step: str
    effect_attempt: int
    disposition: Literal["applied", "already_correct"]
    receipt_id: str
    evidence_sha256: str
    verified_at: datetime


def _bounded_actor(actor_id: str) -> str:
    if not isinstance(actor_id, str) or _ACTOR_RE.fullmatch(actor_id) is None:
        raise ValueError("invalid PCF effect actor")
    return actor_id


def _bounded_seconds(value: int, *, minimum: int, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"invalid {name}")
    return value


def _utc_datetime(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"invalid {name}")
    return value.astimezone(timezone.utc)


def _database_integer(value: Any, *, minimum: int, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise WorkApiError(409, "invalid_work_state")
    return value


def _validated_effect_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate every claimed value before changing durable state."""
    try:
        work_id = uuid.UUID(str(row["work_id"]))
        route = str(row["route_number"])
        delivery = str(row["delivery_number"])
        source_queue_path = str(row["source_queue_path"])
        source_revision = str(row["source_revision"])
        input_hash_version = str(row["input_hash_version"])
        input_hash = str(row["input_hash"])
        step = str(row["effect_step"])
        manifest = row["manifest"]
        ocr_context = row["ocr_context"]
        result = row["result"]
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkApiError(409, "invalid_work_state") from exc
    if (
        _ROUTE_RE.fullmatch(route) is None
        or _DELIVERY_RE.fullmatch(delivery) is None
        or source_queue_path
        != f"pcf_processing_queue/{route}/queue/{delivery}"
        or _SOURCE_REVISION_RE.fullmatch(source_revision) is None
        or input_hash_version != "pcf-work-input-v1"
        or _SHA256_RE.fullmatch(input_hash) is None
        or step not in EFFECT_STEPS
        or not isinstance(manifest, Mapping)
        or not isinstance(ocr_context, Mapping)
        or not isinstance(result, Mapping)
    ):
        raise WorkApiError(409, "invalid_work_state")
    prior_attempts = _database_integer(
        row.get("effect_attempt_count"),
        minimum=0,
        maximum=100,
        name="effect attempts",
    )
    max_attempts = _database_integer(
        row.get("max_attempts"), minimum=1, maximum=100, name="max attempts"
    )
    work_attempt = _database_integer(
        row.get("attempt_count"), minimum=1, maximum=100, name="work attempt"
    )
    confidence = _database_integer(
        row.get("confidence"), minimum=0, maximum=100, name="confidence"
    )
    return {
        "work_id": work_id,
        "route": route,
        "delivery": delivery,
        "source_queue_path": source_queue_path,
        "source_revision": source_revision,
        "input_hash_version": input_hash_version,
        "input_hash": input_hash,
        "step": step,
        "prior_attempts": prior_attempts,
        "max_attempts": max_attempts,
        "work_attempt": work_attempt,
        "confidence": confidence,
        "manifest": dict(manifest),
        "ocr_context": dict(ocr_context),
        "result": dict(result),
    }


def _validated_claim(claim: EffectClaim) -> None:
    claimed_at = (
        _utc_datetime(claim.claimed_at, name="effect claimed time")
        if isinstance(claim, EffectClaim)
        else None
    )
    visible_again_at = (
        _utc_datetime(claim.visible_again_at, name="effect visibility time")
        if isinstance(claim, EffectClaim)
        else None
    )
    if (
        not isinstance(claim, EffectClaim)
        or not isinstance(claim.work_id, uuid.UUID)
        or _ROUTE_RE.fullmatch(claim.route) is None
        or _DELIVERY_RE.fullmatch(claim.delivery) is None
        or claim.source_queue_path
        != f"pcf_processing_queue/{claim.route}/queue/{claim.delivery}"
        or _SOURCE_REVISION_RE.fullmatch(claim.source_revision) is None
        or claim.input_hash_version != "pcf-work-input-v1"
        or _SHA256_RE.fullmatch(claim.input_hash) is None
        or claim.step not in EFFECT_STEPS
        or isinstance(claim.effect_attempt, bool)
        or not isinstance(claim.effect_attempt, int)
        or claim.effect_attempt < 1
        or isinstance(claim.work_attempt, bool)
        or not isinstance(claim.work_attempt, int)
        or claim.work_attempt < 1
        or isinstance(claim.confidence, bool)
        or not isinstance(claim.confidence, int)
        or not 0 <= claim.confidence <= 100
        or not isinstance(claim.manifest, Mapping)
        or not isinstance(claim.ocr_context, Mapping)
        or not isinstance(claim.result, Mapping)
        or claimed_at is None
        or visible_again_at is None
        or visible_again_at <= claimed_at
    ):
        raise ValueError("invalid PCF effect claim")


def _validated_effect_proof(
    proof: EffectProof | None,
    claim: EffectClaim,
    *,
    now: datetime,
) -> dict[str, Any]:
    if (
        not isinstance(proof, EffectProof)
        or proof.work_id != claim.work_id
        or proof.step != claim.step
        or isinstance(proof.effect_attempt, bool)
        or proof.effect_attempt != claim.effect_attempt
        or proof.disposition not in {"applied", "already_correct"}
        or not isinstance(proof.receipt_id, str)
        or _RECEIPT_RE.fullmatch(proof.receipt_id) is None
        or not isinstance(proof.evidence_sha256, str)
        or _SHA256_RE.fullmatch(proof.evidence_sha256) is None
    ):
        raise ValueError("completed PCF effect requires an exact verification proof")
    verified_at = _utc_datetime(proof.verified_at, name="effect proof time")
    if (
        verified_at < claim.claimed_at - timedelta(seconds=30)
        or verified_at > now + timedelta(seconds=30)
    ):
        raise ValueError("completed PCF effect proof is outside the claimed attempt")
    return {
        "disposition": proof.disposition,
        "receiptId": proof.receipt_id,
        "evidenceSha256": proof.evidence_sha256,
        "verifiedAt": verified_at.isoformat().replace("+00:00", "Z"),
    }


def claim_due_effect(
    conn: Any,
    *,
    actor_id: str,
    now: datetime,
    visibility_seconds: int,
) -> EffectClaim | None:
    """Claim at most one due effect without holding a lock over external I/O."""
    actor_id = _bounded_actor(actor_id)
    now = _utc_datetime(now, name="effect claim time")
    visibility_seconds = _bounded_seconds(
        visibility_seconds,
        minimum=30,
        maximum=900,
        name="effect visibility",
    )
    visible_again_at = now + timedelta(seconds=visibility_seconds)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT work_id, route_number, delivery_number, source_queue_path,
                   source_revision, input_hash_version, input_hash,
                   effect_step, effect_attempt_count, attempt_count, max_attempts,
                   confidence, manifest, ocr_context, result
            FROM pcf_work_items
            WHERE status = 'effects_pending' AND effect_not_before <= %s
            ORDER BY effect_not_before, updated_at, work_id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            (now,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        claimed = _validated_effect_row(row)
        step = claimed["step"]
        prior_attempts = claimed["prior_attempts"]
        if prior_attempts >= claimed["max_attempts"]:
            cur.execute(
                """
                UPDATE pcf_work_items
                SET status = 'dead_letter', effect_step = NULL,
                    effect_attempt_count = 0, effect_not_before = NULL,
                    completed_at = %s, updated_at = %s,
                    last_error_code = 'effect_attempts_exhausted',
                    last_error_message = NULL
                WHERE work_id = %s AND status = 'effects_pending'
                  AND effect_step = %s AND effect_attempt_count = %s
                """,
                (now, now, str(claimed["work_id"]), step, prior_attempts),
            )
            if cur.rowcount != 1:
                raise WorkApiError(409, "invalid_work_state")
            _event(
                cur,
                event_type="effect_dead_lettered",
                actor_id=actor_id,
                work_id=claimed["work_id"],
                attempt_number=claimed["work_attempt"],
                actor_type="cluster",
                payload={"effectStep": step, "effectAttempt": prior_attempts},
            )
            return None
        effect_attempt = prior_attempts + 1
        cur.execute(
            """
            UPDATE pcf_work_items
            SET effect_attempt_count = %s, effect_not_before = %s, updated_at = %s
            WHERE work_id = %s AND status = 'effects_pending'
              AND effect_step = %s AND effect_attempt_count = %s
            """,
            (
                effect_attempt,
                visible_again_at,
                now,
                str(claimed["work_id"]),
                step,
                prior_attempts,
            ),
        )
        if cur.rowcount != 1:
            raise WorkApiError(409, "invalid_work_state")
        _event(
            cur,
            event_type="effect_claimed",
            actor_id=actor_id,
            work_id=claimed["work_id"],
            attempt_number=claimed["work_attempt"],
            actor_type="cluster",
            payload={
                "effectStep": step,
                "effectAttempt": effect_attempt,
                "visibleAgainAt": visible_again_at.isoformat(),
            },
        )
    return EffectClaim(
        work_id=claimed["work_id"],
        route=claimed["route"],
        delivery=claimed["delivery"],
        source_queue_path=claimed["source_queue_path"],
        source_revision=claimed["source_revision"],
        input_hash_version=claimed["input_hash_version"],
        input_hash=claimed["input_hash"],
        step=step,
        effect_attempt=effect_attempt,
        work_attempt=claimed["work_attempt"],
        confidence=claimed["confidence"],
        manifest=claimed["manifest"],
        ocr_context=claimed["ocr_context"],
        result=claimed["result"],
        claimed_at=now,
        visible_again_at=visible_again_at,
    )


def advance_effect_step(
    conn: Any,
    claim: EffectClaim,
    *,
    actor_id: str,
    now: datetime,
    outcome: Literal["completed", "skipped"] = "completed",
    reason: str | None = None,
    proof: EffectProof | None = None,
) -> EffectAdvance:
    """CAS one claimed effect step forward; stale workers cannot advance."""
    actor_id = _bounded_actor(actor_id)
    now = _utc_datetime(now, name="effect completion time")
    _validated_claim(claim)
    if outcome not in {"completed", "skipped"}:
        raise ValueError("invalid PCF effect outcome")
    if outcome == "skipped":
        if (
            not isinstance(reason, str)
            or _ERROR_RE.fullmatch(reason) is None
            or reason not in _SKIPPABLE_EFFECT_REASONS.get(claim.step, frozenset())
        ):
            raise ValueError("skipped PCF effect requires a deterministic reason")
        if proof is not None:
            raise ValueError("skipped PCF effect cannot carry an external proof")
        proof_payload = None
    else:
        if reason is not None:
            raise ValueError("completed PCF effect cannot carry a skip reason")
        proof_payload = _validated_effect_proof(proof, claim, now=now)
    index = EFFECT_STEPS.index(claim.step)
    next_step = EFFECT_STEPS[index + 1] if index + 1 < len(EFFECT_STEPS) else None
    status = "effects_pending"
    terminal_status = "complete" if claim.confidence >= 92 else "review_scheduled"
    with conn.cursor() as cur:
        if next_step is None:
            status = terminal_status
            cur.execute(
                """
                UPDATE pcf_work_items
                SET status = %s, effect_step = NULL, effect_attempt_count = 0,
                    effect_not_before = NULL, completed_at = %s, updated_at = %s,
                    last_error_code = NULL, last_error_message = NULL
                WHERE work_id = %s AND status = 'effects_pending'
                  AND effect_step = %s AND effect_attempt_count = %s
                """,
                (
                    status,
                    now,
                    now,
                    str(claim.work_id),
                    claim.step,
                    claim.effect_attempt,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE pcf_work_items
                SET effect_step = %s, effect_attempt_count = 0,
                    effect_not_before = %s, updated_at = %s,
                    last_error_code = NULL, last_error_message = NULL
                WHERE work_id = %s AND status = 'effects_pending'
                  AND effect_step = %s AND effect_attempt_count = %s
                """,
                (
                    next_step,
                    now,
                    now,
                    str(claim.work_id),
                    claim.step,
                    claim.effect_attempt,
                ),
            )
        if cur.rowcount != 1:
            raise WorkApiError(409, "stale_effect_attempt")
        _event(
            cur,
            event_type="effect_skipped" if outcome == "skipped" else "effect_completed",
            actor_id=actor_id,
            work_id=claim.work_id,
            attempt_number=claim.work_attempt,
            actor_type="cluster",
            payload={
                "effectStep": claim.step,
                "effectAttempt": claim.effect_attempt,
                "nextEffectStep": next_step,
                "nextStatus": status,
                **({"reason": reason} if reason is not None else {}),
                **({"proof": proof_payload} if proof_payload is not None else {}),
            },
        )
    return EffectAdvance(claim.work_id, claim.step, next_step, status)


def schedule_effect_retry(
    conn: Any,
    claim: EffectClaim,
    *,
    actor_id: str,
    now: datetime,
    delay_seconds: int,
    error_code: str,
) -> datetime:
    """Release one claimed step for bounded retry using the same CAS fence."""
    actor_id = _bounded_actor(actor_id)
    now = _utc_datetime(now, name="effect retry time")
    _validated_claim(claim)
    delay_seconds = _bounded_seconds(
        delay_seconds,
        minimum=5,
        maximum=3600,
        name="effect retry delay",
    )
    if not isinstance(error_code, str) or _ERROR_RE.fullmatch(error_code) is None:
        raise ValueError("invalid PCF effect error code")
    retry_at = now + timedelta(seconds=delay_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pcf_work_items
            SET effect_not_before = %s, updated_at = %s,
                last_error_code = %s, last_error_message = NULL
            WHERE work_id = %s AND status = 'effects_pending'
              AND effect_step = %s AND effect_attempt_count = %s
            """,
            (
                retry_at,
                now,
                error_code,
                str(claim.work_id),
                claim.step,
                claim.effect_attempt,
            ),
        )
        if cur.rowcount != 1:
            raise WorkApiError(409, "stale_effect_attempt")
        _event(
            cur,
            event_type="effect_retry_scheduled",
            actor_id=actor_id,
            work_id=claim.work_id,
            attempt_number=claim.work_attempt,
            actor_type="cluster",
            payload={
                "effectStep": claim.step,
                "effectAttempt": claim.effect_attempt,
                "retryAt": retry_at.isoformat(),
                "errorCode": error_code,
            },
        )
    return retry_at

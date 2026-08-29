"""Exact, fail-closed PCF queue TTL disarming before external effects."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Iterable
from typing import Any, Callable

from firebase_admin import firestore


_QUEUE_PATH = re.compile(
    r"^pcf_processing_queue/(?P<route>[0-9]{6})/queue/(?P<delivery>[0-9]{8,12})$"
)
PCF_QUEUE_TTL_FIELD = "pcfQueueExpiresAt"


class QueueTtlDisarmError(RuntimeError):
    """The exact queue TTL could not be proven absent."""


@dataclass(frozen=True)
class QueueTtlDisarmProof:
    source_queue_path: str
    verified_at: datetime


@dataclass(frozen=True)
class QueueTtlCandidate:
    source_queue_path: str
    route: str
    delivery: str


def validate_source_queue_path(path: str, *, route: str, delivery: str) -> str:
    if not all(isinstance(value, str) for value in (path, route, delivery)):
        raise QueueTtlDisarmError("pcf_queue_ttl_path_invalid")
    value = path.strip()
    match = _QUEUE_PATH.fullmatch(value)
    if (
        match is None
        or match.group("route") != route
        or match.group("delivery") != delivery
    ):
        raise QueueTtlDisarmError("pcf_queue_ttl_path_invalid")
    return value


def disarm_exact_queue_ttl(
    db: Any,
    *,
    source_queue_path: str,
    route: str,
    delivery: str,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    transactional: Callable[[Callable[..., Any]], Callable[..., Any]] = firestore.transactional,
    delete_value: Any = firestore.DELETE_FIELD,
) -> QueueTtlDisarmProof:
    """Delete one exact TTL field atomically, then independently read it back."""
    path = validate_source_queue_path(
        source_queue_path,
        route=route,
        delivery=delivery,
    )
    ref = db.document(path)

    @transactional
    def _clear(transaction: Any) -> None:
        snapshot = ref.get(transaction=transaction)
        if not snapshot.exists:
            raise QueueTtlDisarmError("pcf_queue_ttl_source_missing")
        transaction.update(ref, {PCF_QUEUE_TTL_FIELD: delete_value})

    _clear(db.transaction())
    verified = ref.get()
    if not verified.exists:
        raise QueueTtlDisarmError("pcf_queue_ttl_source_missing")
    if PCF_QUEUE_TTL_FIELD in (verified.to_dict() or {}):
        raise QueueTtlDisarmError("pcf_queue_ttl_disarm_unverified")
    return QueueTtlDisarmProof(source_queue_path=path, verified_at=now_fn())


def repair_bounded_exact_queue_ttls(
    db: Any,
    candidates: Iterable[QueueTtlCandidate],
    *,
    maximum: int = 100,
    disarm_fn: Callable[..., QueueTtlDisarmProof] | None = None,
) -> tuple[QueueTtlDisarmProof, ...]:
    """Disarm only a pre-enumerated bounded set; never discover or scan queues."""
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 1 <= maximum <= 100:
        raise QueueTtlDisarmError("pcf_queue_ttl_repair_limit_invalid")
    rows: list[QueueTtlCandidate] = []
    for candidate in candidates:
        if len(rows) >= maximum:
            raise QueueTtlDisarmError("pcf_queue_ttl_repair_limit_exceeded")
        rows.append(candidate)

    unique: list[QueueTtlCandidate] = []
    seen: set[str] = set()
    for candidate in rows:
        if not isinstance(candidate, QueueTtlCandidate):
            raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
        path = validate_source_queue_path(
            candidate.source_queue_path,
            route=candidate.route,
            delivery=candidate.delivery,
        )
        if path not in seen:
            seen.add(path)
            unique.append(candidate)

    clear = disarm_fn or disarm_exact_queue_ttl
    return tuple(
        clear(
            db,
            source_queue_path=candidate.source_queue_path,
            route=candidate.route,
            delivery=candidate.delivery,
        )
        for candidate in unique
    )

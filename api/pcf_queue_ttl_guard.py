"""One bounded PCF queue TTL safety cycle for a controlled scheduler hook."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .pcf_queue_ttl import (
    QueueTtlDisarmProof,
    repair_bounded_exact_queue_ttls,
)
from .pcf_queue_ttl_candidates import (
    MAX_QUEUE_TTL_CANDIDATES,
    QueueTtlCandidateUnion,
    load_queue_ttl_candidate_union,
)


@dataclass(frozen=True)
class QueueTtlGuardReport:
    candidate_count: int
    work_rows: int
    hold_rows: int
    alert_rows: int
    verified_disarms: int


def run_queue_ttl_guard_cycle(
    conn: Any,
    db: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
    load_fn: Callable[..., QueueTtlCandidateUnion] = load_queue_ttl_candidate_union,
    repair_fn: Callable[..., tuple[QueueTtlDisarmProof, ...]] = (
        repair_bounded_exact_queue_ttls
    ),
) -> QueueTtlGuardReport:
    """Run one exact repair cycle; the caller owns schedule and runtime authority."""
    union = load_fn(conn, db, maximum=maximum)
    proofs = repair_fn(db, union.candidates, maximum=maximum)
    expected_paths = tuple(candidate.source_queue_path for candidate in union.candidates)
    proof_paths = tuple(proof.source_queue_path for proof in proofs)
    if proof_paths != expected_paths:
        raise RuntimeError("pcf_queue_ttl_guard_proof_mismatch")
    return QueueTtlGuardReport(
        candidate_count=len(union.candidates),
        work_rows=union.work_rows,
        hold_rows=union.hold_rows,
        alert_rows=union.alert_rows,
        verified_disarms=len(proofs),
    )

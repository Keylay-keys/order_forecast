from __future__ import annotations

from datetime import datetime, timezone

import pytest

from order_forecast.api.pcf_queue_ttl import QueueTtlCandidate, QueueTtlDisarmProof
from order_forecast.api.pcf_queue_ttl_candidates import QueueTtlCandidateUnion
from order_forecast.api.pcf_queue_ttl_guard import run_queue_ttl_guard_cycle


def test_guard_cycle_repairs_the_complete_deduplicated_union_and_reports_counts():
    candidate = QueueTtlCandidate(
        "pcf_processing_queue/988200/queue/1806000000",
        "988200",
        "1806000000",
    )
    union = QueueTtlCandidateUnion((candidate,), work_rows=1, hold_rows=1, alert_rows=2)
    calls = []

    def load(conn, db, *, maximum):
        calls.append(("load", conn, db, maximum))
        return union

    def repair(db, candidates, *, maximum):
        calls.append(("repair", db, tuple(candidates), maximum))
        return (
            QueueTtlDisarmProof(
                candidate.source_queue_path,
                datetime(2026, 8, 29, tzinfo=timezone.utc),
            ),
        )

    conn = object()
    db = object()
    report = run_queue_ttl_guard_cycle(
        conn,
        db,
        maximum=20,
        load_fn=load,
        repair_fn=repair,
    )
    assert report.candidate_count == 1
    assert report.verified_disarms == 1
    assert (report.work_rows, report.hold_rows, report.alert_rows) == (1, 1, 2)
    assert calls == [
        ("load", conn, db, 20),
        ("repair", db, (candidate,), 20),
    ]


def test_guard_cycle_fails_if_independent_proofs_do_not_cover_the_exact_union():
    candidate = QueueTtlCandidate(
        "pcf_processing_queue/988200/queue/1806000000",
        "988200",
        "1806000000",
    )
    union = QueueTtlCandidateUnion((candidate,), work_rows=1, hold_rows=0, alert_rows=0)
    with pytest.raises(RuntimeError, match="proof_mismatch"):
        run_queue_ttl_guard_cycle(
            object(),
            object(),
            load_fn=lambda *_args, **_kwargs: union,
            repair_fn=lambda *_args, **_kwargs: (),
        )

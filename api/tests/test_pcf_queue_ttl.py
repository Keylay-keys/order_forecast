from __future__ import annotations

from datetime import datetime, timezone

import pytest

from order_forecast.api.pcf_queue_ttl import (
    QueueTtlCandidate,
    QueueTtlDisarmError,
    QueueTtlDisarmProof,
    disarm_exact_queue_ttl,
    repair_bounded_exact_queue_ttls,
    validate_source_queue_path,
)


ROUTE = "988200"
DELIVERY = "1806000000"
PATH = f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}"


class _Snapshot:
    def __init__(self, value):
        self._value = value
        self.exists = value is not None

    def to_dict(self):
        return dict(self._value or {})


class _Ref:
    def __init__(self, store):
        self.store = store

    def get(self, transaction=None):
        return _Snapshot(self.store.get("value"))


class _Transaction:
    def __init__(self, store):
        self.store = store

    def update(self, _ref, patch):
        value = dict(self.store["value"])
        if patch.get("pcfQueueExpiresAt") == "__DELETE__":
            value.pop("pcfQueueExpiresAt", None)
        self.store["value"] = value


class _Db:
    def __init__(self, value):
        self.store = {"value": value}
        self.ref = _Ref(self.store)

    def document(self, path):
        assert path == PATH
        return self.ref

    def transaction(self):
        return _Transaction(self.store)


def test_exact_path_is_bound_to_claim_identity():
    assert validate_source_queue_path(PATH, route=ROUTE, delivery=DELIVERY) == PATH
    for path, route, delivery in (
        (f"pcf_processing_queue/989262/queue/{DELIVERY}", ROUTE, DELIVERY),
        (f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}/server/admission", ROUTE, DELIVERY),
        ("promoRequests/988200/queue/1806000000", ROUTE, DELIVERY),
        (PATH, 988200, DELIVERY),
    ):
        with pytest.raises(QueueTtlDisarmError, match="path_invalid"):
            validate_source_queue_path(path, route=route, delivery=delivery)


def test_disarm_deletes_and_independently_verifies_exact_field():
    db = _Db({"pcfQueueExpiresAt": "armed", "status": "unprocessed"})
    proof = disarm_exact_queue_ttl(
        db,
        source_queue_path=PATH,
        route=ROUTE,
        delivery=DELIVERY,
        now_fn=lambda: datetime(2026, 8, 29, tzinfo=timezone.utc),
        transactional=lambda fn: fn,
        delete_value="__DELETE__",
    )
    assert proof.source_queue_path == PATH
    assert proof.verified_at == datetime(2026, 8, 29, tzinfo=timezone.utc)
    assert db.store["value"] == {"status": "unprocessed"}


def test_disarm_fails_closed_for_missing_or_unverified_queue():
    with pytest.raises(QueueTtlDisarmError, match="source_missing"):
        disarm_exact_queue_ttl(
            _Db(None),
            source_queue_path=PATH,
            route=ROUTE,
            delivery=DELIVERY,
            transactional=lambda fn: fn,
            delete_value="__DELETE__",
        )

    class _NoDeleteTransaction(_Transaction):
        def update(self, _ref, patch):
            return None

    db = _Db({"pcfQueueExpiresAt": "armed"})
    db.transaction = lambda: _NoDeleteTransaction(db.store)
    with pytest.raises(QueueTtlDisarmError, match="disarm_unverified"):
        disarm_exact_queue_ttl(
            db,
            source_queue_path=PATH,
            route=ROUTE,
            delivery=DELIVERY,
            transactional=lambda fn: fn,
            delete_value="__DELETE__",
        )


def test_bounded_repair_validates_all_exact_paths_before_any_clear_and_deduplicates():
    calls = []

    def clear(db, *, source_queue_path, route, delivery):
        calls.append((db, source_queue_path, route, delivery))
        return QueueTtlDisarmProof(
            source_queue_path=source_queue_path,
            verified_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )

    db = object()
    candidate = QueueTtlCandidate(PATH, ROUTE, DELIVERY)
    proofs = repair_bounded_exact_queue_ttls(
        db,
        [candidate, candidate],
        disarm_fn=clear,
    )
    assert len(proofs) == 1
    assert calls == [(db, PATH, ROUTE, DELIVERY)]

    with pytest.raises(QueueTtlDisarmError, match="path_invalid"):
        repair_bounded_exact_queue_ttls(
            db,
            [
                candidate,
                QueueTtlCandidate(PATH, "989262", DELIVERY),
            ],
            disarm_fn=clear,
        )
    assert calls == [(db, PATH, ROUTE, DELIVERY)]


def test_bounded_repair_refuses_oversized_or_invalid_limits_before_reads():
    candidate = QueueTtlCandidate(PATH, ROUTE, DELIVERY)
    for maximum in (0, 101, True):
        with pytest.raises(QueueTtlDisarmError, match="limit_invalid"):
            repair_bounded_exact_queue_ttls(object(), [candidate], maximum=maximum)
    with pytest.raises(QueueTtlDisarmError, match="limit_exceeded"):
        repair_bounded_exact_queue_ttls(object(), [candidate, candidate], maximum=1)

from __future__ import annotations

from dataclasses import dataclass

import pytest

from order_forecast.api.pcf_queue_ttl import QueueTtlCandidate, QueueTtlDisarmError
from order_forecast.api.pcf_queue_ttl_candidates import (
    PCF_ALERT_TYPE_GROUPS,
    UNRESOLVED_ALERT_STATUSES,
    WORK_TTL_HOLD_STATUSES,
    load_alert_queue_ttl_candidates,
    load_scanner_hold_queue_ttl_candidates,
    load_work_queue_ttl_candidates,
    merge_queue_ttl_candidates,
)


ROUTE = "988200"
DELIVERY = "1806000000"
PATH = f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}"


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.execute_args = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, statement, params):
        self.execute_args = (statement, params)

    def fetchall(self):
        return list(self.rows)


class _Connection:
    def __init__(self, rows):
        self.cursor_value = _Cursor(rows)

    def cursor(self, **_kwargs):
        return self.cursor_value


@dataclass
class _Reference:
    path: str


class _Snapshot:
    def __init__(self, path, data):
        self.reference = _Reference(path)
        self._data = data

    def to_dict(self):
        return self._data


class _Query:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.filters = []
        self.bound = None

    def where(self, *, filter):
        self.filters.append((filter.field_path, filter.op_string, filter.value))
        return self

    def limit(self, value):
        self.bound = value
        return self

    def stream(self):
        self.db.calls.append((self.name, tuple(self.filters), self.bound))
        return iter(self.db.results.pop(0))


class _Firestore:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def collection_group(self, name):
        return _Query(self, f"group:{name}")

    def collection(self, name):
        return _Query(self, f"collection:{name}")


def test_work_candidates_are_distinct_bounded_nonclosed_exact_paths():
    conn = _Connection(
        [
            {
                "route_number": ROUTE,
                "delivery_number": DELIVERY,
                "source_queue_path": PATH,
                "status": "blocked",
            }
        ]
    )
    assert load_work_queue_ttl_candidates(conn) == (
        QueueTtlCandidate(PATH, ROUTE, DELIVERY),
    )
    statement, params = conn.cursor_value.execute_args
    assert "DISTINCT ON (route_number, delivery_number)" in statement
    assert "COLLATE \"C\"" in statement
    assert params == (list(WORK_TTL_HOLD_STATUSES), 101)


def test_work_candidate_overflow_or_invalid_identity_fails_closed():
    row = {
        "route_number": ROUTE,
        "delivery_number": DELIVERY,
        "source_queue_path": PATH,
        "status": "queued",
    }
    with pytest.raises(QueueTtlDisarmError, match="work_candidates_overflow"):
        load_work_queue_ttl_candidates(_Connection([row, row]), maximum=1)
    with pytest.raises(QueueTtlDisarmError, match="candidate_invalid"):
        load_work_queue_ttl_candidates(
            _Connection([{**row, "route_number": int(ROUTE)}])
        )
    with pytest.raises(QueueTtlDisarmError, match="work_identity_invalid"):
        load_work_queue_ttl_candidates(
            _Connection([{**row, "status": "succeeded"}])
        )


def test_scanner_hold_query_is_bounded_and_requires_exact_hold_and_queue_identity():
    data = {
        "status": "entitlement_hold",
        "featureKey": "scanner",
        "routeNumber": ROUTE,
        "heldDeliveryNumber": DELIVERY,
        "queueDocPath": PATH,
    }
    db = _Firestore([_Snapshot(f"routeEntitlementHolds/{ROUTE}/features/scanner", data)])
    assert load_scanner_hold_queue_ttl_candidates(db) == (
        QueueTtlCandidate(PATH, ROUTE, DELIVERY),
    )
    assert db.calls == [
        (
            "group:features",
            (
                ("status", "==", "entitlement_hold"),
                ("featureKey", "==", "scanner"),
            ),
            101,
        )
    ]

    wrong = _Firestore([_Snapshot(f"other/{ROUTE}/features/scanner", data)])
    with pytest.raises(QueueTtlDisarmError, match="hold_identity_invalid"):
        load_scanner_hold_queue_ttl_candidates(wrong)

    stale_query_result = _Firestore(
        [
            _Snapshot(
                f"routeEntitlementHolds/{ROUTE}/features/scanner",
                {**data, "status": "released"},
            )
        ]
    )
    with pytest.raises(QueueTtlDisarmError, match="hold_identity_invalid"):
        load_scanner_hold_queue_ttl_candidates(stale_query_result)


def test_alert_queries_stay_below_dnf_limit_ignore_pos_and_require_exact_pcf_identity():
    pcf = _Snapshot(
        "admin_alerts/pcf",
        {
            "type": "ocr_failure",
            "status": "pending",
            "routeNumber": ROUTE,
            "deliveryNumber": DELIVERY,
            "queueDocPath": PATH,
        },
    )
    pos = _Snapshot(
        "admin_alerts/pos",
        {
            "type": "grace_capture_hold",
            "status": "pending",
            "feature": "pos",
            "routeNumber": ROUTE,
            "workItemId": "invoice-1",
            "invoiceDocPath": f"routes/{ROUTE}/pos_invoices/invoice-1",
        },
    )
    scanner = _Snapshot(
        "admin_alerts/scanner",
        {
            "type": "grace_capture_cleanup",
            "status": "reviewed",
            "feature": "scanner",
            "routeNumber": ROUTE,
            "workItemId": DELIVERY,
            "queueDocPath": PATH,
        },
    )
    legacy_post_review = _Snapshot(
        "admin_alerts/legacy-post-review",
        {
            "type": "ocr_post_send_review",
            "status": "pending",
            "routeNumber": ROUTE,
            "deliveryNumber": DELIVERY,
        },
    )
    db = _Firestore([pcf, legacy_post_review], [pos, scanner])
    assert load_alert_queue_ttl_candidates(db) == (
        QueueTtlCandidate(PATH, ROUTE, DELIVERY),
        QueueTtlCandidate(PATH, ROUTE, DELIVERY),
        QueueTtlCandidate(PATH, ROUTE, DELIVERY),
    )
    assert len(db.calls) == 2
    for call, types in zip(db.calls, PCF_ALERT_TYPE_GROUPS):
        assert call == (
            "collection:admin_alerts",
            (
                ("type", "in", list(types)),
                ("status", "in", list(UNRESOLVED_ALERT_STATUSES)),
            ),
            101,
        )
        assert len(types) * len(UNRESOLVED_ALERT_STATUSES) <= 30

    invalid = _Firestore(
        [
            _Snapshot(
                "admin_alerts/bad",
                {
                    "type": "ocr_failure",
                    "status": "pending",
                    "routeNumber": ROUTE,
                    "deliveryNumber": DELIVERY,
                    "queueDocPath": "promoRequests/988200/queue/1806000000",
                },
            )
        ],
        [],
    )
    with pytest.raises(QueueTtlDisarmError, match="path_invalid"):
        load_alert_queue_ttl_candidates(invalid)

    stale_query_result = _Firestore(
        [
            _Snapshot(
                "admin_alerts/stale",
                {
                    "type": "grace_capture_hold",
                    "status": "resolved",
                    "feature": "scanner",
                    "routeNumber": ROUTE,
                    "workItemId": DELIVERY,
                    "queueDocPath": PATH,
                },
            )
        ],
        [],
    )
    with pytest.raises(QueueTtlDisarmError, match="alert_identity_invalid"):
        load_alert_queue_ttl_candidates(stale_query_result)


def test_merge_deduplicates_sources_sorts_paths_and_rejects_global_overflow():
    first = QueueTtlCandidate(PATH, ROUTE, DELIVERY)
    second = QueueTtlCandidate(
        "pcf_processing_queue/989262/queue/1806000001",
        "989262",
        "1806000001",
    )
    union = merge_queue_ttl_candidates(
        work=[second],
        holds=[first],
        alerts=[first],
    )
    assert union.candidates == (first, second)
    assert (union.work_rows, union.hold_rows, union.alert_rows) == (1, 1, 1)
    with pytest.raises(QueueTtlDisarmError, match="union_overflow"):
        merge_queue_ttl_candidates(
            work=[first],
            holds=[second],
            alerts=[],
            maximum=1,
        )

"""Bounded exact-path sources for the PCF queue TTL safety check."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from google.cloud.firestore_v1.base_query import FieldFilter
from psycopg2.extras import RealDictCursor

from .pcf_queue_ttl import (
    QueueTtlCandidate,
    QueueTtlDisarmError,
    validate_source_queue_path,
)


MAX_QUEUE_TTL_CANDIDATES = 100
WORK_TTL_HOLD_STATUSES = (
    "queued",
    "leased",
    "processing",
    "effects_pending",
    "blocked",
    "dead_letter",
)
UNRESOLVED_ALERT_STATUSES = (
    "cooling",
    "pending",
    "in_progress",
    "reviewed",
)
PCF_ALERT_TYPE_GROUPS = (
    (
        "pcf_upload",
        "ocr_failure",
        "ocr_agent_failure",
        "ocr_post_send_review",
        "container_mismatch",
        "loading_date_warning",
    ),
    (
        "grace_capture_hold",
        "grace_capture_blocked",
        "grace_capture_cleanup",
    ),
)


@dataclass(frozen=True)
class QueueTtlCandidateUnion:
    candidates: tuple[QueueTtlCandidate, ...]
    work_rows: int
    hold_rows: int
    alert_rows: int


def _limit(value: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_QUEUE_TTL_CANDIDATES
    ):
        raise QueueTtlDisarmError("pcf_queue_ttl_candidate_limit_invalid")
    return value


def _strict_text(data: Mapping[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
    return value


def _candidate(data: Mapping[str, Any], *, delivery_field: str) -> QueueTtlCandidate:
    route = _strict_text(data, "routeNumber")
    delivery = _strict_text(data, delivery_field)
    path = _strict_text(data, "queueDocPath")
    validate_source_queue_path(path, route=route, delivery=delivery)
    return QueueTtlCandidate(path, route, delivery)


def _alert_candidate(data: Mapping[str, Any]) -> QueueTtlCandidate:
    """Derive the canonical PCF queue path and cross-check any stored copy."""
    route = _strict_text(data, "routeNumber")
    delivery = _strict_text(data, "deliveryNumber")
    path = f"pcf_processing_queue/{route}/queue/{delivery}"
    validate_source_queue_path(path, route=route, delivery=delivery)
    stored_path = data.get("queueDocPath")
    if stored_path is not None:
        if not isinstance(stored_path, str) or stored_path != stored_path.strip():
            raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
        validate_source_queue_path(stored_path, route=route, delivery=delivery)
    return QueueTtlCandidate(path, route, delivery)


def _snapshot_data(snapshot: Any) -> Mapping[str, Any]:
    value = snapshot.to_dict()
    if not isinstance(value, Mapping):
        raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
    return value


def load_work_queue_ttl_candidates(
    conn: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> tuple[QueueTtlCandidate, ...]:
    """Read distinct nonclosed work identities from PostgreSQL only."""
    maximum = _limit(maximum)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (route_number, delivery_number)
                   route_number, delivery_number, source_queue_path, status
            FROM pcf_work_items
            WHERE status = ANY(%s)
            ORDER BY route_number COLLATE "C", delivery_number COLLATE "C",
                     updated_at DESC, work_id
            LIMIT %s
            """,
            (list(WORK_TTL_HOLD_STATUSES), maximum + 1),
        )
        rows = tuple(cur.fetchall())
    if len(rows) > maximum:
        raise QueueTtlDisarmError("pcf_queue_ttl_work_candidates_overflow")
    candidates = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
        if row.get("status") not in WORK_TTL_HOLD_STATUSES:
            raise QueueTtlDisarmError("pcf_queue_ttl_work_identity_invalid")
        candidates.append(
            _candidate(
                {
                    "routeNumber": row.get("route_number"),
                    "deliveryNumber": row.get("delivery_number"),
                    "queueDocPath": row.get("source_queue_path"),
                },
                delivery_field="deliveryNumber",
            )
        )
    return tuple(candidates)


def load_scanner_hold_queue_ttl_candidates(
    db: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> tuple[QueueTtlCandidate, ...]:
    """Read only active scanner holds; never inspect the PCF queue collection."""
    maximum = _limit(maximum)
    snapshots = tuple(
        db.collection_group("features")
        .where(filter=FieldFilter("status", "==", "entitlement_hold"))
        .where(filter=FieldFilter("featureKey", "==", "scanner"))
        .limit(maximum + 1)
        .stream()
    )
    if len(snapshots) > maximum:
        raise QueueTtlDisarmError("pcf_queue_ttl_hold_candidates_overflow")
    candidates = []
    for snapshot in snapshots:
        data = _snapshot_data(snapshot)
        if data.get("status") != "entitlement_hold" or data.get("featureKey") != "scanner":
            raise QueueTtlDisarmError("pcf_queue_ttl_hold_identity_invalid")
        route = _strict_text(data, "routeNumber")
        expected_hold_path = f"routeEntitlementHolds/{route}/features/scanner"
        reference_path = getattr(getattr(snapshot, "reference", None), "path", None)
        if reference_path != expected_hold_path:
            raise QueueTtlDisarmError("pcf_queue_ttl_hold_identity_invalid")
        candidates.append(_candidate(data, delivery_field="heldDeliveryNumber"))
    return tuple(candidates)


def load_alert_queue_ttl_candidates(
    db: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> tuple[QueueTtlCandidate, ...]:
    """Read unresolved named PCF alert classes in two bounded indexed queries."""
    maximum = _limit(maximum)
    candidates: list[QueueTtlCandidate] = []
    rows_seen = 0
    for alert_types in PCF_ALERT_TYPE_GROUPS:
        snapshots = tuple(
            db.collection("admin_alerts")
            .where(filter=FieldFilter("type", "in", list(alert_types)))
            .where(
                filter=FieldFilter(
                    "status",
                    "in",
                    list(UNRESOLVED_ALERT_STATUSES),
                )
            )
            .limit(maximum + 1)
            .stream()
        )
        rows_seen += len(snapshots)
        if len(snapshots) > maximum or rows_seen > maximum:
            raise QueueTtlDisarmError("pcf_queue_ttl_alert_candidates_overflow")
        for snapshot in snapshots:
            data = _snapshot_data(snapshot)
            alert_type = _strict_text(data, "type")
            if (
                alert_type not in alert_types
                or data.get("status") not in UNRESOLVED_ALERT_STATUSES
            ):
                raise QueueTtlDisarmError("pcf_queue_ttl_alert_identity_invalid")
            if alert_type.startswith("grace_capture_"):
                feature = data.get("feature")
                if feature == "pos":
                    continue
                if feature != "scanner":
                    raise QueueTtlDisarmError("pcf_queue_ttl_alert_identity_invalid")
                candidates.append(_candidate(data, delivery_field="workItemId"))
            else:
                candidates.append(_alert_candidate(data))
    return tuple(candidates)


def merge_queue_ttl_candidates(
    *,
    work: Iterable[QueueTtlCandidate],
    holds: Iterable[QueueTtlCandidate],
    alerts: Iterable[QueueTtlCandidate],
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> QueueTtlCandidateUnion:
    maximum = _limit(maximum)
    sources = (tuple(work), tuple(holds), tuple(alerts))
    unique: dict[str, QueueTtlCandidate] = {}
    for rows in sources:
        for candidate in rows:
            if not isinstance(candidate, QueueTtlCandidate):
                raise QueueTtlDisarmError("pcf_queue_ttl_candidate_invalid")
            path = validate_source_queue_path(
                candidate.source_queue_path,
                route=candidate.route,
                delivery=candidate.delivery,
            )
            unique.setdefault(path, candidate)
            if len(unique) > maximum:
                raise QueueTtlDisarmError("pcf_queue_ttl_candidate_union_overflow")
    return QueueTtlCandidateUnion(
        candidates=tuple(unique[path] for path in sorted(unique)),
        work_rows=len(sources[0]),
        hold_rows=len(sources[1]),
        alert_rows=len(sources[2]),
    )


def load_queue_ttl_candidate_union(
    conn: Any,
    db: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> QueueTtlCandidateUnion:
    maximum = _limit(maximum)
    work = load_work_queue_ttl_candidates(conn, maximum=maximum)
    holds = load_scanner_hold_queue_ttl_candidates(db, maximum=maximum)
    alerts = load_alert_queue_ttl_candidates(db, maximum=maximum)
    return merge_queue_ttl_candidates(
        work=work,
        holds=holds,
        alerts=alerts,
        maximum=maximum,
    )

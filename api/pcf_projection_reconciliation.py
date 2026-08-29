"""Bounded, effect-free route parity and targeted PCF projection repair plans."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
from typing import Any, Mapping, Sequence

from psycopg2.extras import RealDictCursor

from .pcf_event_contract import (
    EventContractError,
    business_hash,
    compare_firestore_revisions,
    firestore_revision,
    normalize_google_update_time,
    projection_event_business_hash,
    validate_projection_event,
)
from ..scripts.pcf_lifecycle_due import lock_lifecycle_route


MAX_ROUTE_ENTITIES = 500
_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")


class ProjectionReconciliationError(RuntimeError):
    """The supplied source/projection proof is invalid or unsafe to apply."""


@dataclass(frozen=True, order=True)
class RouteEntityKey:
    delivery_id: str
    container_id: str


@dataclass(frozen=True)
class SourceRouteEntity:
    key: RouteEntityKey
    source_revision: str
    business_hash: str
    event: Mapping[str, Any]


@dataclass(frozen=True)
class ExactFirestoreAbsenceProof:
    """One exact document read proving a named source path was absent."""

    source_path: str
    read_time: str
    exists: bool


@dataclass(frozen=True)
class RouteChecksum:
    route_number: str
    entity_count: int
    business_hash: str
    entities: tuple[tuple[RouteEntityKey, str], ...]
    snapshot_revision: str | None = None
    source_entities: tuple[SourceRouteEntity, ...] = ()
    integrity_mismatches: tuple[RouteEntityKey, ...] = ()


@dataclass(frozen=True)
class RouteReconciliationResult:
    route_number: str
    status: str
    reason: str
    source: RouteChecksum
    projection: RouteChecksum
    missing_in_projection: tuple[RouteEntityKey, ...]
    changed_in_projection: tuple[RouteEntityKey, ...]
    extra_in_projection: tuple[RouteEntityKey, ...]


def _validate_route(route_number: str) -> str:
    route = str(route_number)
    if _ROUTE_RE.fullmatch(route) is None:
        raise ProjectionReconciliationError("route_invalid")
    return route


def _validate_snapshot_revision(value: str) -> str:
    try:
        compare_firestore_revisions(value, value)
    except EventContractError as exc:
        raise ProjectionReconciliationError("snapshot_revision_invalid") from exc
    if not value.endswith("|0"):
        raise ProjectionReconciliationError("snapshot_revision_must_be_read_boundary")
    return value


def _checksum(
    route_number: str,
    entities: Sequence[tuple[RouteEntityKey, str]],
) -> str:
    return business_hash(
        {
            "contractVersion": "pcf-route-checksum-v1",
            "entities": [
                {
                    "businessHash": entity_hash,
                    "container": key.container_id,
                    "delivery": key.delivery_id,
                }
                for key, entity_hash in entities
            ],
            "route": route_number,
        }
    )


def summarize_source_route(
    route_number: str,
    snapshot_revision: str,
    current_events: Sequence[Mapping[str, Any]],
) -> RouteChecksum:
    """Summarize one bounded current-source snapshot.

    ``snapshot_revision`` is the route query/read boundary, encoded with the
    existing sortable Firestore timestamp grammar and rank zero. It is not an
    entity mutation revision. Every included entity revision must be at or
    before that boundary.
    """
    route = _validate_route(route_number)
    boundary = _validate_snapshot_revision(snapshot_revision)
    if len(current_events) > MAX_ROUTE_ENTITIES:
        raise ProjectionReconciliationError("source_route_entity_limit_exceeded")

    source_entities: list[SourceRouteEntity] = []
    seen: set[RouteEntityKey] = set()
    for raw_event in current_events:
        try:
            event = validate_projection_event(raw_event)
        except EventContractError as exc:
            raise ProjectionReconciliationError("source_event_invalid") from exc
        if event["eventType"] != "projection.pcf_container.changed":
            raise ProjectionReconciliationError("source_event_type_invalid")
        if str(event["entity"]["route"]) != route:
            raise ProjectionReconciliationError("source_event_route_mismatch")
        if event["automationExcluded"]:
            continue
        if event["mutationKind"] != "upsert":
            raise ProjectionReconciliationError("source_snapshot_contains_tombstone")
        if compare_firestore_revisions(event["sourceRevision"], boundary) > 0:
            raise ProjectionReconciliationError("source_event_after_snapshot")
        key = RouteEntityKey(
            delivery_id=str(event["entity"]["delivery"]),
            container_id=str(event["entity"]["container"]),
        )
        if key in seen:
            raise ProjectionReconciliationError("source_entity_duplicate")
        seen.add(key)
        source_entities.append(
            SourceRouteEntity(
                key=key,
                source_revision=str(event["sourceRevision"]),
                business_hash=projection_event_business_hash(event),
                event=event,
            )
        )

    source_entities.sort(key=lambda entity: entity.key)
    entities = tuple((entity.key, entity.business_hash) for entity in source_entities)
    return RouteChecksum(
        route_number=route,
        entity_count=len(entities),
        business_hash=_checksum(route, entities),
        entities=entities,
        snapshot_revision=boundary,
        source_entities=tuple(source_entities),
    )


def summarize_projection_rows(
    route_number: str,
    rows: Sequence[Mapping[str, Any]],
) -> RouteChecksum:
    route = _validate_route(route_number)
    if len(rows) > MAX_ROUTE_ENTITIES:
        raise ProjectionReconciliationError("projection_route_entity_limit_exceeded")
    entities: list[tuple[RouteEntityKey, str]] = []
    integrity_mismatches: list[RouteEntityKey] = []
    seen: set[RouteEntityKey] = set()
    for row in rows:
        if str(row.get("route_number") or "") != route:
            raise ProjectionReconciliationError("projection_row_route_mismatch")
        key = RouteEntityKey(
            delivery_id=str(row.get("delivery_id") or ""),
            container_id=str(row.get("container_id") or ""),
        )
        if not key.delivery_id or not key.container_id:
            raise ProjectionReconciliationError("projection_entity_invalid")
        if key in seen:
            raise ProjectionReconciliationError("projection_entity_duplicate")
        stored_hash = str(row.get("source_business_hash") or "")
        if re.fullmatch(r"[0-9a-f]{64}", stored_hash) is None:
            raise ProjectionReconciliationError("projection_business_hash_invalid")
        loading_date = row.get("loading_date")
        if isinstance(loading_date, datetime):
            loading_date = loading_date.date().isoformat()
        elif isinstance(loading_date, date):
            loading_date = loading_date.isoformat()
        elif loading_date is not None and not isinstance(loading_date, str):
            raise ProjectionReconciliationError("projection_loading_date_invalid")
        page_numbers = row.get("page_numbers")
        items = row.get("items")
        if not isinstance(page_numbers, list) or not isinstance(items, list):
            raise ProjectionReconciliationError("projection_payload_invalid")
        actual_hash = business_hash(
            {
                "entity": {
                    "container": key.container_id,
                    "delivery": key.delivery_id,
                    "route": route,
                },
                "payload": {
                    "active": bool(row.get("active")),
                    "items": items,
                    "loadingDate": loading_date,
                    "pageNumbers": page_numbers,
                },
            }
        )
        if stored_hash != actual_hash:
            integrity_mismatches.append(key)
        seen.add(key)
        entities.append((key, actual_hash))
    entities.sort(key=lambda entity: entity[0])
    frozen = tuple(entities)
    return RouteChecksum(
        route_number=route,
        entity_count=len(frozen),
        business_hash=_checksum(route, frozen),
        entities=frozen,
        integrity_mismatches=tuple(sorted(integrity_mismatches)),
    )


def compare_route_checksums(
    source: RouteChecksum,
    projection: RouteChecksum,
    *,
    projection_last_revision: str | None = None,
) -> RouteReconciliationResult:
    if source.route_number != projection.route_number or source.snapshot_revision is None:
        raise ProjectionReconciliationError("route_checksum_scope_mismatch")
    if projection_last_revision is not None:
        try:
            projection_ahead = compare_firestore_revisions(
                projection_last_revision,
                source.snapshot_revision,
            ) > 0
        except EventContractError as exc:
            raise ProjectionReconciliationError("projection_revision_invalid") from exc
        if projection_ahead:
            return RouteReconciliationResult(
                route_number=source.route_number,
                status="mismatched",
                reason="projection_ahead_of_snapshot",
                source=source,
                projection=projection,
                missing_in_projection=(),
                changed_in_projection=(),
                extra_in_projection=(),
            )

    source_map = dict(source.entities)
    projection_map = dict(projection.entities)
    source_keys = set(source_map)
    projection_keys = set(projection_map)
    missing = tuple(sorted(source_keys - projection_keys))
    extra = tuple(sorted(projection_keys - source_keys))
    changed = tuple(
        sorted(
            {
                key
                for key in source_keys & projection_keys
                if source_map[key] != projection_map[key]
            }
            | set(projection.integrity_mismatches)
        )
    )
    matched = not missing and not extra and not changed and (
        source.entity_count == projection.entity_count
        and source.business_hash == projection.business_hash
    )
    return RouteReconciliationResult(
        route_number=source.route_number,
        status="matched" if matched else "mismatched",
        reason=(
            "matched"
            if matched
            else (
                "projection_integrity_mismatch"
                if projection.integrity_mismatches
                else "route_checksum_mismatch"
            )
        ),
        source=source,
        projection=projection,
        missing_in_projection=missing,
        changed_in_projection=changed,
        extra_in_projection=extra,
    )


def targeted_repair_events(
    result: RouteReconciliationResult,
) -> tuple[Mapping[str, Any], ...]:
    """Return exact source upserts; extras require separate absence proof.

    No tombstone is synthesized here. A projection-only key must first receive
    an exact Firestore missing-document read whose read time fences a later
    reconciliation tombstone.
    """
    if result.reason == "projection_ahead_of_snapshot":
        return ()
    wanted = set(result.missing_in_projection) | set(result.changed_in_projection)
    return tuple(
        entity.event
        for entity in result.source.source_entities
        if entity.key in wanted
    )


def build_absence_tombstone(
    route_number: str,
    key: RouteEntityKey,
    proof: ExactFirestoreAbsenceProof,
) -> Mapping[str, Any]:
    """Build one deterministic tombstone from an exact missing-document read.

    A route checksum alone is never deletion authority. The read time fences
    this tombstone below any later recreation, whose Firestore update time wins
    through the normal per-entity projection ordering.
    """
    route = _validate_route(route_number)
    expected_path = (
        f"routes/{route}/pcfs/{key.delivery_id}/containers/{key.container_id}"
    )
    if proof.source_path != expected_path:
        raise ProjectionReconciliationError("absence_proof_path_mismatch")
    if proof.exists is not False:
        raise ProjectionReconciliationError("absence_not_proven")
    try:
        read_time = normalize_google_update_time(proof.read_time)
        source_revision = firestore_revision(read_time, "delete")
    except EventContractError as exc:
        raise ProjectionReconciliationError("absence_proof_read_time_invalid") from exc
    identity_hash = business_hash(
        {
            "purpose": "pcf-reconciliation-absence-v1",
            "readTime": read_time,
            "sourcePath": expected_path,
        }
    )
    event = {
        "schemaVersion": 1,
        "sourceEventId": f"reconcile-absence:{identity_hash}",
        "eventType": "projection.pcf_container.changed",
        "mutationKind": "delete",
        "occurredAt": read_time,
        "sourcePath": expected_path,
        "sourceRevision": source_revision,
        "entity": {
            "route": route,
            "delivery": key.delivery_id,
            "container": key.container_id,
        },
        "automationExcluded": False,
        "fixture": None,
        "payload": None,
    }
    try:
        return validate_projection_event(event)
    except EventContractError as exc:
        raise ProjectionReconciliationError("absence_tombstone_invalid") from exc


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ProjectionReconciliationError(f"{name}_invalid")
    return value.astimezone(timezone.utc)


def reconcile_route_projection(
    conn: Any,
    source: RouteChecksum,
    *,
    observed_at: datetime,
    healthy_until: datetime | None = None,
) -> RouteReconciliationResult:
    """Record bounded parity proof under the existing route transaction lock.

    This function never marks a route eligible, never enables serving, and
    never repairs source or projection data. ``healthy_until`` remains optional
    until R10 freezes the freshness duration; omitting it fails serving closed.
    """
    route = _validate_route(source.route_number)
    if source.snapshot_revision is None:
        raise ProjectionReconciliationError("source_snapshot_revision_missing")
    observed = _aware_utc(observed_at, "observed_at")
    healthy = None
    if healthy_until is not None:
        healthy = _aware_utc(healthy_until, "healthy_until")
        if healthy <= observed:
            raise ProjectionReconciliationError("healthy_until_not_future")

    lock_lifecycle_route(conn, route)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT eligible, last_projected_revision
            FROM pcf_projection_routes
            WHERE route_number = %s
            FOR UPDATE
            """,
            (route,),
        )
        control = cur.fetchone()
        if control is None:
            raise ProjectionReconciliationError("route_control_missing")
        if not bool(control.get("eligible")):
            raise ProjectionReconciliationError("route_not_eligible")
        cur.execute(
            """
            SELECT route_number, delivery_id, container_id,
                   active, loading_date, page_numbers, items,
                   source_business_hash
            FROM items_current
            WHERE route_number = %s AND deleted_at IS NULL
            ORDER BY delivery_id, container_id
            LIMIT %s
            """,
            (route, MAX_ROUTE_ENTITIES + 1),
        )
        projection = summarize_projection_rows(route, cur.fetchall())
        last_revision = control.get("last_projected_revision")
        result = compare_route_checksums(
            source,
            projection,
            projection_last_revision=(str(last_revision) if last_revision else None),
        )
        applied_health = healthy if result.status == "matched" else None
        projection_revision = (
            source.snapshot_revision
            if result.status == "matched"
            else (str(last_revision) if last_revision else None)
        )
        cur.execute(
            """
            UPDATE pcf_projection_routes
            SET reconciliation_status = %s,
                parity_source_revision = %s,
                parity_projection_revision = %s,
                source_entity_count = %s,
                projection_entity_count = %s,
                source_business_hash = %s,
                projection_business_hash = %s,
                last_reconciled_at = %s,
                healthy_until = %s,
                updated_at = NOW()
            WHERE route_number = %s
            """,
            (
                result.status,
                source.snapshot_revision,
                projection_revision,
                source.entity_count,
                projection.entity_count,
                source.business_hash,
                projection.business_hash,
                observed,
                applied_health,
                route,
            ),
        )
    return result

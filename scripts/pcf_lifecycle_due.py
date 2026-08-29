"""Bounded PCF lifecycle projection and due-work primitives.

This module is deliberately effect-free.  It can project normalized container
events, plan one delivery from PostgreSQL, and lease known due work.  It cannot
read or write Firebase, copy Storage objects, archive a PCF, or delete data.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from psycopg2.extras import Json, RealDictCursor

try:
    from order_forecast.api.pcf_event_contract import (
        EventContractError,
        business_hash,
        compare_firestore_revisions,
        firestore_revision_instant,
        lifecycle_semantic_key,
        projection_decision,
        projection_event_business_hash,
        validate_projection_event,
    )
except ImportError:
    from api.pcf_event_contract import (  # type: ignore
        EventContractError,
        business_hash,
        compare_firestore_revisions,
        firestore_revision_instant,
        lifecycle_semantic_key,
        projection_decision,
        projection_event_business_hash,
        validate_projection_event,
    )


DEFAULT_TIMEZONE = "America/Denver"
LIFECYCLE_KIND = "lifecycle"
ARCHIVE_TRANSITION = "archive_delivery"
TERMINAL_RETENTION_DAYS = 90
LIFECYCLE_RECOVERY_DAYS = 30
MAX_ROUTE_REPLAN_DELIVERIES = 500
CLAIMABLE_STATUSES = ("scheduled", "retryable", "blocked")
SUPERSEDABLE_STATUSES = ("scheduled", "retryable", "blocked", "dead_letter")


class LifecycleContractError(ValueError):
    """A deterministic lifecycle input or state error."""


class LifecycleConflictError(RuntimeError):
    """A persisted row conflicts with the immutable lifecycle identity."""


@dataclass(frozen=True)
class ProjectedContainer:
    route_number: str
    delivery_id: str
    container_id: str
    active: bool
    items: tuple[Mapping[str, Any], ...]
    source_revision: str
    source_business_hash: str


@dataclass(frozen=True)
class CleanupCandidate:
    route_number: str
    delivery_id: str
    anchor_container_id: str
    retention_days: int
    effective_business_date: date
    due_at: datetime
    source_revision: str
    source_business_hash: str
    semantic_key: str
    earliest_expiry_date: date
    latest_expiry_date: date
    item_count: int
    decision_watermark: Mapping[str, Any]


@dataclass(frozen=True)
class CleanupPlan:
    candidate: Optional[CleanupCandidate]
    reason: str
    container_count: int
    item_count: int
    missing_expiry_count: int


@dataclass(frozen=True)
class ClaimedLifecycleWork:
    id: int
    semantic_key: str
    route_number: str
    delivery_id: str
    container_id: str
    effective_business_date: date
    due_at: datetime
    deadline_at: datetime
    source_revision: str
    source_business_hash: str
    decision_watermark: Mapping[str, Any]
    claim_token: str
    attempt_count: int


@dataclass(frozen=True)
class CleanupAuthorityDecision:
    authorized: bool
    disposition: str
    days_left_by_container: Mapping[str, tuple[int, ...]]
    current_candidate: Optional[CleanupCandidate]


def _require_aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise LifecycleContractError(f"{name} must be timezone-aware")
    return value


def _require_retention_days(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 3650:
        raise LifecycleContractError("retention_days must be an integer from 1 through 3650")
    return value


def _require_route_number(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value.isdigit()
        or not 1 <= len(value) <= 10
    ):
        raise LifecycleContractError("route_number must contain 1 through 10 digits")
    return value


def lock_lifecycle_route(conn, route_number: str) -> str:
    """Serialize policy and container planning for one route transaction."""
    route_number = _require_route_number(route_number)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"pcf-lifecycle-route:{route_number}",),
        )
    return route_number


def apply_route_retention_policy(
    conn,
    *,
    route_number: str,
    retention_days: int | None,
    policy_revision: str,
) -> str:
    """Store one effective route policy behind monotonic Firestore fencing.

    The policy lives on the existing compact route-control row.  This function
    neither scans projected deliveries nor creates ``due_work``; the planner
    owns those actions after the policy and container projection are durable.
    """
    route_number = lock_lifecycle_route(conn, route_number)
    compare_firestore_revisions(policy_revision, policy_revision)
    active = retention_days is not None
    if active:
        retention_days = _require_retention_days(retention_days)
        if not policy_revision.endswith("|0"):
            raise LifecycleContractError("active retention policy revision must be an upsert")
    elif not policy_revision.endswith("|1"):
        raise LifecycleContractError("inactive retention policy revision must be a delete")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT lifecycle_policy_active, lifecycle_retention_days,
                   lifecycle_policy_revision
            FROM pcf_projection_routes
            WHERE route_number = %s
            FOR UPDATE
            """,
            (route_number,),
        )
        existing = cur.fetchone()
        if existing is not None and existing["lifecycle_policy_revision"] is not None:
            ordering = compare_firestore_revisions(
                policy_revision, existing["lifecycle_policy_revision"]
            )
            if ordering < 0:
                return "stale_noop"
            if ordering == 0:
                if (
                    bool(existing["lifecycle_policy_active"]) != active
                    or existing["lifecycle_retention_days"] != retention_days
                ):
                    raise LifecycleConflictError(
                        "equal retention policy revision has different content"
                    )
                return "duplicate_noop"
            watermark_only = (
                bool(existing["lifecycle_policy_active"]) == active
                and existing["lifecycle_retention_days"] == retention_days
            )
        else:
            watermark_only = False

        cur.execute(
            """
            INSERT INTO pcf_projection_routes (
                route_number, lifecycle_policy_active, lifecycle_retention_days,
                lifecycle_policy_revision, lifecycle_policy_synced_at,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (route_number) DO UPDATE
            SET lifecycle_policy_active = EXCLUDED.lifecycle_policy_active,
                lifecycle_retention_days = EXCLUDED.lifecycle_retention_days,
                lifecycle_policy_revision = EXCLUDED.lifecycle_policy_revision,
                lifecycle_policy_synced_at = EXCLUDED.lifecycle_policy_synced_at,
                updated_at = NOW()
            """,
            (route_number, active, retention_days, policy_revision),
        )
    if existing is None:
        return "inserted"
    return "advance_watermark_only" if watermark_only else "updated"


def load_route_retention_policy(conn, route_number: str) -> tuple[int, str] | None:
    """Read the projected effective policy, failing closed when it is absent."""
    route_number = _require_route_number(route_number)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT lifecycle_policy_active, lifecycle_retention_days,
                   lifecycle_policy_revision
            FROM pcf_projection_routes
            WHERE route_number = %s
            """,
            (route_number,),
        )
        row = cur.fetchone()
    if (
        row is None
        or row["lifecycle_policy_revision"] is None
        or not row["lifecycle_policy_active"]
    ):
        return None
    return int(row["lifecycle_retention_days"]), str(row["lifecycle_policy_revision"])


def _zone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(str(timezone_name))
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise LifecycleContractError("timezone_name must be a known IANA zone") from exc


def _valid_wall_time_candidates(local_naive: datetime, zone: ZoneInfo) -> list[datetime]:
    candidates: list[datetime] = []
    for fold in (0, 1):
        candidate = local_naive.replace(tzinfo=zone, fold=fold)
        round_trip = candidate.astimezone(timezone.utc).astimezone(zone)
        if round_trip.replace(tzinfo=None) == local_naive:
            candidates.append(candidate)
    unique: dict[datetime, datetime] = {
        candidate.astimezone(timezone.utc): candidate for candidate in candidates
    }
    return [unique[key] for key in sorted(unique)]


def resolve_cleanup_due_at(
    effective_business_date: date,
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
    local_hour: int = 2,
) -> datetime:
    """Resolve the legacy 02:00 wall clock without scheduling inside a DST gap.

    Ambiguous wall times choose the first occurrence.  A nonexistent wall time
    advances minute-by-minute to the first valid instant, so cleanup never runs
    earlier than the configured retention boundary.
    """
    if not isinstance(effective_business_date, date):
        raise LifecycleContractError("effective_business_date must be a date")
    if isinstance(local_hour, bool) or not isinstance(local_hour, int) or not 0 <= local_hour <= 23:
        raise LifecycleContractError("local_hour must be an integer from 0 through 23")
    zone = _zone(timezone_name)
    local_naive = datetime.combine(effective_business_date, time(local_hour, 0))
    for minute_offset in range(181):
        candidates = _valid_wall_time_candidates(
            local_naive + timedelta(minutes=minute_offset), zone
        )
        if candidates:
            return candidates[0].astimezone(timezone.utc)
    raise LifecycleContractError("could not resolve cleanup wall time within three hours")


def business_date_at(value: datetime, timezone_name: str = DEFAULT_TIMEZONE) -> date:
    return _require_aware(value, "value").astimezone(_zone(timezone_name)).date()


def _parse_expiry(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _aggregate_business_hash(containers: Sequence[ProjectedContainer]) -> str:
    return business_hash(
        [
            {
                "containerId": container.container_id,
                "sourceBusinessHash": container.source_business_hash,
            }
            for container in sorted(containers, key=lambda row: row.container_id)
        ]
    )


def _max_revision(values: Iterable[str]) -> str:
    result: Optional[str] = None
    for value in values:
        if result is None or compare_firestore_revisions(value, result) > 0:
            result = value
    if result is None:
        raise LifecycleContractError("at least one source revision is required")
    return result


def build_cleanup_plan(
    containers: Sequence[ProjectedContainer],
    *,
    retention_days: int,
    policy_revision: str,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> CleanupPlan:
    """Compute one future archive check from current container contents.

    The strict legacy predicate is preserved: every item expiry date must be
    earlier than ``business_date - retention_days``.  Therefore the scheduled
    business date is ``latest_expiry + retention_days + 1 day``.
    """
    retention_days = _require_retention_days(retention_days)
    compare_firestore_revisions(policy_revision, policy_revision)
    current = [container for container in containers]
    if not current:
        return CleanupPlan(None, "no_containers", 0, 0, 0)

    route_delivery = {(row.route_number, row.delivery_id) for row in current}
    if len(route_delivery) != 1:
        raise LifecycleContractError("all containers must belong to one route and delivery")
    if len({row.container_id for row in current}) != len(current):
        raise LifecycleContractError("container_id values must be unique within a delivery")
    expiries: list[tuple[date, str]] = []
    missing_expiry_count = 0
    item_count = 0
    for container in current:
        if not container.items:
            return CleanupPlan(
                None,
                "empty_container",
                len(current),
                item_count,
                missing_expiry_count,
            )
        for item in container.items:
            item_count += 1
            expiry = _parse_expiry(item.get("expiryDate"))
            if expiry is None:
                missing_expiry_count += 1
            else:
                expiries.append((expiry, container.container_id))

    if missing_expiry_count:
        return CleanupPlan(
            None,
            "missing_or_invalid_expiry",
            len(current),
            item_count,
            missing_expiry_count,
        )
    if not expiries:
        return CleanupPlan(None, "no_valid_expiry", len(current), item_count, 0)

    earliest_expiry = min(expiry for expiry, _ in expiries)
    latest_expiry = max(expiry for expiry, _ in expiries)
    anchor_container = min(
        container_id for expiry, container_id in expiries if expiry == latest_expiry
    )
    effective_date = latest_expiry + timedelta(days=retention_days + 1)
    due_at = resolve_cleanup_due_at(
        effective_date,
        timezone_name=timezone_name,
        local_hour=2,
    )
    source_revision = _max_revision(
        [policy_revision, *(container.source_revision for container in current)]
    )
    aggregate_hash = _aggregate_business_hash(current)
    route_number, delivery_id = next(iter(route_delivery))
    semantic_key = lifecycle_semantic_key(
        route=route_number,
        delivery=delivery_id,
        container=anchor_container,
        transition_kind=ARCHIVE_TRANSITION,
        effective_business_date=effective_date.isoformat(),
        source_revision=source_revision,
    )
    watermark = {
        "containers": {
            row.container_id: {
                "businessHash": row.source_business_hash,
                "sourceRevision": row.source_revision,
            }
            for row in sorted(current, key=lambda row: row.container_id)
        },
        "policyRevision": policy_revision,
        "retentionDays": retention_days,
        "timezone": timezone_name,
    }
    return CleanupPlan(
        CleanupCandidate(
            route_number=route_number,
            delivery_id=delivery_id,
            anchor_container_id=anchor_container,
            retention_days=retention_days,
            effective_business_date=effective_date,
            due_at=due_at,
            source_revision=source_revision,
            source_business_hash=aggregate_hash,
            semantic_key=semantic_key,
            earliest_expiry_date=earliest_expiry,
            latest_expiry_date=latest_expiry,
            item_count=item_count,
            decision_watermark=watermark,
        ),
        "scheduled",
        len(current),
        item_count,
        0,
    )


def cleanup_authority_decision(
    claimed: ClaimedLifecycleWork,
    observed_containers: Sequence[ProjectedContainer],
    *,
    now_utc: datetime,
    retention_days: int,
    policy_revision: str,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> CleanupAuthorityDecision:
    """Recompute exact current eligibility before an effect adapter may run."""
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    plan = build_cleanup_plan(
        observed_containers,
        retention_days=retention_days,
        policy_revision=policy_revision,
        timezone_name=timezone_name,
    )
    local_date = business_date_at(now_utc, timezone_name)
    days_left = {
        container.container_id: tuple(
            (expiry - local_date).days
            for expiry in (
                _parse_expiry(item.get("expiryDate")) for item in container.items
            )
            if expiry is not None
        )
        for container in observed_containers
    }
    if plan.candidate is None:
        return CleanupAuthorityDecision(False, plan.reason, days_left, None)
    candidate = plan.candidate
    if candidate.source_business_hash != claimed.source_business_hash:
        return CleanupAuthorityDecision(False, "superseded_input", days_left, candidate)
    # A newer Firestore revision with the same stable business hash is only a
    # watermark advance and must not invalidate already-planned work.  Compare
    # the material schedule instead of requiring the old semantic key's source
    # revision to equal the current no-op revision.
    if (
        candidate.effective_business_date != claimed.effective_business_date
        or candidate.anchor_container_id != claimed.container_id
    ):
        return CleanupAuthorityDecision(False, "superseded_schedule", days_left, candidate)
    if now_utc < candidate.due_at:
        return CleanupAuthorityDecision(False, "not_due", days_left, candidate)
    if now_utc > claimed.deadline_at:
        return CleanupAuthorityDecision(False, "deadline_expired", days_left, candidate)
    if any(
        value >= -retention_days
        for container_values in days_left.values()
        for value in container_values
    ):
        return CleanupAuthorityDecision(False, "retention_not_elapsed", days_left, candidate)
    return CleanupAuthorityDecision(True, "authorized", days_left, candidate)


def _container_from_row(row: Mapping[str, Any]) -> ProjectedContainer:
    items = row.get("items")
    if not isinstance(items, list):
        raise LifecycleContractError("projected items must decode to a JSON array")
    return ProjectedContainer(
        route_number=str(row["route_number"]),
        delivery_id=str(row["delivery_id"]),
        container_id=str(row["container_id"]),
        active=bool(row["active"]),
        items=tuple(items),
        source_revision=str(row["source_revision"]),
        source_business_hash=str(row["source_business_hash"]),
    )


def load_projected_delivery(conn, route_number: str, delivery_id: str) -> list[ProjectedContainer]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT route_number, delivery_id, container_id, active, items,
                   source_revision, source_business_hash
            FROM items_current
            WHERE route_number = %s
              AND delivery_id = %s
              AND deleted_at IS NULL
            ORDER BY container_id
            """,
            (str(route_number), str(delivery_id)),
        )
        return [_container_from_row(row) for row in cur.fetchall()]


def apply_container_projection(conn, event: Mapping[str, Any]) -> str:
    """Apply one normalized container event with monotonic revision fencing."""
    normalized = validate_projection_event(event)
    if normalized["eventType"] != "projection.pcf_container.changed":
        raise EventContractError("only pcf_container events can enter items_current")
    if normalized["automationExcluded"]:
        return "fixture_suppressed"

    entity = normalized["entity"]
    incoming_hash = projection_event_business_hash(normalized)
    mutation_kind = normalized["mutationKind"]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT active, loading_date, page_numbers, items, deleted_at,
                   source_revision, source_business_hash, source_mutation_kind
            FROM items_current
            WHERE route_number = %s AND delivery_id = %s AND container_id = %s
            FOR UPDATE
            """,
            (entity["route"], entity["delivery"], entity["container"]),
        )
        existing = cur.fetchone()
        persisted_hash = None
        if existing is not None:
            persisted_payload = None
            if existing["deleted_at"] is None:
                persisted_payload = {
                    "active": bool(existing["active"]),
                    "items": existing["items"],
                    "loadingDate": (
                        existing["loading_date"].isoformat()
                        if isinstance(existing["loading_date"], date)
                        else existing["loading_date"]
                    ),
                    "pageNumbers": existing["page_numbers"],
                }
            persisted_hash = business_hash(
                {"entity": dict(entity), "payload": persisted_payload}
            )
        decision = projection_decision(
            current_revision=existing["source_revision"] if existing else None,
            current_business_hash=existing["source_business_hash"] if existing else None,
            current_mutation_kind=existing["source_mutation_kind"] if existing else None,
            incoming_revision=normalized["sourceRevision"],
            incoming_business_hash=incoming_hash,
            incoming_mutation_kind=mutation_kind,
        )
        integrity_repair = bool(
            existing is not None
            and persisted_hash != existing["source_business_hash"]
            and compare_firestore_revisions(
                normalized["sourceRevision"], existing["source_revision"]
            ) == 0
        )
        if integrity_repair:
            decision = "apply"
        if decision in {"stale_noop", "duplicate_noop"}:
            return decision
        if decision == "revision_conflict_reject":
            raise LifecycleConflictError("equal source revision has different business content")
        if decision == "advance_watermark_only":
            cur.execute(
                """
                UPDATE items_current
                SET source_revision = %s,
                    source_path = %s,
                    updated_at = NOW()
                WHERE route_number = %s AND delivery_id = %s AND container_id = %s
                """,
                (
                    normalized["sourceRevision"],
                    normalized["sourcePath"],
                    entity["route"],
                    entity["delivery"],
                    entity["container"],
                ),
            )
            return decision

        payload = normalized["payload"]
        if mutation_kind == "delete":
            active = False
            loading_date = None
            page_numbers: list[int] = []
            items: list[Mapping[str, Any]] = []
            valid_expiries: list[date] = []
            deleted_at: Optional[datetime] = datetime.now(timezone.utc)
        else:
            assert isinstance(payload, Mapping)
            active = bool(payload["active"])
            loading_date = payload["loadingDate"]
            page_numbers = list(payload["pageNumbers"])
            items = list(payload["items"])
            valid_expiries = [
                expiry
                for expiry in (_parse_expiry(item.get("expiryDate")) for item in items)
                if expiry is not None
            ]
            deleted_at = None
        cur.execute(
            """
            INSERT INTO items_current (
                route_number, delivery_id, container_id, active, loading_date,
                page_numbers, items, item_count, valid_expiry_count, earliest_expiry_date,
                latest_expiry_date, source_path, source_revision,
                source_business_hash, source_mutation_kind, deleted_at,
                created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (route_number, delivery_id, container_id) DO UPDATE
            SET active = EXCLUDED.active,
                loading_date = EXCLUDED.loading_date,
                page_numbers = EXCLUDED.page_numbers,
                items = EXCLUDED.items,
                item_count = EXCLUDED.item_count,
                valid_expiry_count = EXCLUDED.valid_expiry_count,
                earliest_expiry_date = EXCLUDED.earliest_expiry_date,
                latest_expiry_date = EXCLUDED.latest_expiry_date,
                source_path = EXCLUDED.source_path,
                source_revision = EXCLUDED.source_revision,
                source_business_hash = EXCLUDED.source_business_hash,
                source_mutation_kind = EXCLUDED.source_mutation_kind,
                deleted_at = EXCLUDED.deleted_at,
                updated_at = NOW()
            """,
            (
                entity["route"],
                entity["delivery"],
                entity["container"],
                active,
                loading_date,
                Json(page_numbers),
                Json(items),
                len(items),
                len(valid_expiries),
                min(valid_expiries) if valid_expiries else None,
                max(valid_expiries) if valid_expiries else None,
                normalized["sourcePath"],
                normalized["sourceRevision"],
                incoming_hash,
                mutation_kind,
                deleted_at,
            ),
        )
    return "integrity_repaired" if integrity_repair else "apply"


def _terminal_retain_until(now_utc: datetime) -> datetime:
    return now_utc + timedelta(days=TERMINAL_RETENTION_DAYS)


def schedule_cleanup_candidate(
    conn,
    candidate: CleanupCandidate,
    *,
    deadline_at: datetime,
    now_utc: datetime,
) -> str:
    """Upsert one immutable lifecycle key and supersede older unleased work."""
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    deadline_at = _require_aware(deadline_at, "deadline_at").astimezone(timezone.utc)
    if deadline_at < candidate.due_at:
        raise LifecycleContractError("deadline_at cannot precede due_at")
    if deadline_at < now_utc:
        return "outside_lateness_window"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE due_work
            SET status = 'cancelled',
                terminal_outcome = 'superseded',
                terminal_at = %s,
                retain_until = %s,
                block_reason = NULL,
                last_error = NULL,
                next_attempt_at = LEAST(next_attempt_at, %s),
                updated_at = NOW()
            WHERE kind = 'lifecycle'
              AND route_number = %s
              AND delivery_id = %s
              AND transition_kind = %s
              AND semantic_key <> %s
              AND status = ANY(%s)
            """,
            (
                now_utc,
                _terminal_retain_until(now_utc),
                now_utc,
                candidate.route_number,
                candidate.delivery_id,
                ARCHIVE_TRANSITION,
                candidate.semantic_key,
                list(SUPERSEDABLE_STATUSES),
            ),
        )
        cur.execute(
            """
            INSERT INTO due_work (
                kind, semantic_key, route_number, delivery_id, container_id,
                transition_kind, effective_business_date, source_revision,
                source_business_hash, decision_watermark, due_at,
                next_attempt_at, deadline_at, priority, status,
                attempt_count, max_attempts, created_at, updated_at
            )
            VALUES (
                'lifecycle', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, 0, 'scheduled', 0, 5, NOW(), NOW()
            )
            ON CONFLICT (kind, semantic_key) DO NOTHING
            RETURNING id
            """,
            (
                candidate.semantic_key,
                candidate.route_number,
                candidate.delivery_id,
                candidate.anchor_container_id,
                ARCHIVE_TRANSITION,
                candidate.effective_business_date,
                candidate.source_revision,
                candidate.source_business_hash,
                Json(dict(candidate.decision_watermark)),
                candidate.due_at,
                candidate.due_at,
                deadline_at,
            ),
        )
        if cur.fetchone():
            return "inserted"
        cur.execute(
            """
            SELECT route_number, delivery_id, container_id, transition_kind,
                   effective_business_date, source_revision,
                   source_business_hash, decision_watermark, due_at, deadline_at
            FROM due_work
            WHERE kind = 'lifecycle' AND semantic_key = %s
            """,
            (candidate.semantic_key,),
        )
        existing = cur.fetchone()
    if existing is None:
        raise LifecycleConflictError("due-work conflict disappeared")
    immutable = {
        "route_number": candidate.route_number,
        "delivery_id": candidate.delivery_id,
        "container_id": candidate.anchor_container_id,
        "transition_kind": ARCHIVE_TRANSITION,
        "effective_business_date": candidate.effective_business_date,
        "source_revision": candidate.source_revision,
        "source_business_hash": candidate.source_business_hash,
        "decision_watermark": dict(candidate.decision_watermark),
        "due_at": candidate.due_at,
        "deadline_at": deadline_at,
    }
    for key, expected in immutable.items():
        if existing[key] != expected:
            raise LifecycleConflictError(f"immutable due-work conflict for {key}")
    return "duplicate"


def plan_projected_delivery(
    conn,
    *,
    route_number: str,
    delivery_id: str,
    retention_days: int,
    policy_revision: str,
    deadline_at: datetime,
    now_utc: datetime,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> CleanupPlan:
    containers = load_projected_delivery(conn, route_number, delivery_id)
    plan = build_cleanup_plan(
        containers,
        retention_days=retention_days,
        policy_revision=policy_revision,
        timezone_name=timezone_name,
    )
    if plan.candidate is not None:
        schedule_cleanup_candidate(
            conn,
            plan.candidate,
            deadline_at=deadline_at,
            now_utc=now_utc,
        )
    return plan


def cancel_projected_delivery_plan(
    conn,
    *,
    route_number: str,
    delivery_id: str,
    reason: str,
    now_utc: datetime,
) -> int:
    """Cancel only unleased lifecycle rows invalidated by current projection."""
    route_number = _require_route_number(route_number)
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    normalized_reason = str(reason).strip().lower().replace(" ", "_")
    if not normalized_reason or len(normalized_reason) > 64:
        raise LifecycleContractError("cancellation reason must contain 1 through 64 characters")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE due_work
            SET status = 'cancelled',
                terminal_outcome = %s,
                terminal_at = %s,
                retain_until = %s,
                block_reason = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE kind = 'lifecycle'
              AND route_number = %s
              AND delivery_id = %s
              AND transition_kind = %s
              AND status = ANY(%s)
            """,
            (
                f"projection_{normalized_reason}",
                now_utc,
                _terminal_retain_until(now_utc),
                route_number,
                str(delivery_id),
                ARCHIVE_TRANSITION,
                list(SUPERSEDABLE_STATUSES),
            ),
        )
        return int(cur.rowcount)


def plan_projected_delivery_from_current_policy(
    conn,
    *,
    route_number: str,
    delivery_id: str,
    now_utc: datetime,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> CleanupPlan:
    """Replan one delivery from durable projection and effective route policy."""
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    policy = load_route_retention_policy(conn, route_number)
    if policy is None:
        containers = load_projected_delivery(conn, route_number, delivery_id)
        cancel_projected_delivery_plan(
            conn,
            route_number=route_number,
            delivery_id=delivery_id,
            reason="policy_missing",
            now_utc=now_utc,
        )
        return CleanupPlan(None, "policy_missing", len(containers), 0, 0)

    retention_days, policy_revision = policy
    containers = load_projected_delivery(conn, route_number, delivery_id)
    plan = build_cleanup_plan(
        containers,
        retention_days=retention_days,
        policy_revision=policy_revision,
        timezone_name=timezone_name,
    )
    if plan.candidate is None:
        cancel_projected_delivery_plan(
            conn,
            route_number=route_number,
            delivery_id=delivery_id,
            reason=plan.reason,
            now_utc=now_utc,
        )
        return plan

    deadline_anchor = max(
        plan.candidate.due_at,
        firestore_revision_instant(plan.candidate.source_revision),
    )
    deadline_at = deadline_anchor + timedelta(
        days=LIFECYCLE_RECOVERY_DAYS
    )
    schedule_cleanup_candidate(
        conn,
        plan.candidate,
        deadline_at=deadline_at,
        now_utc=now_utc,
    )
    return plan


def plan_projected_route_from_current_policy(
    conn,
    *,
    route_number: str,
    now_utc: datetime,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> list[tuple[str, CleanupPlan]]:
    """Replan only projected deliveries belonging to one policy-changed route."""
    route_number = _require_route_number(route_number)
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT delivery_id
            FROM items_current
            WHERE route_number = %s
              AND deleted_at IS NULL
            GROUP BY delivery_id
            HAVING BOOL_OR(active)
            ORDER BY delivery_id
            LIMIT %s
            """,
            (route_number, MAX_ROUTE_REPLAN_DELIVERIES + 1),
        )
        delivery_ids = [str(row[0]) for row in cur.fetchall()]
    if len(delivery_ids) > MAX_ROUTE_REPLAN_DELIVERIES:
        raise LifecycleContractError("route_active_delivery_limit_exceeded")

    if load_route_retention_policy(conn, route_number) is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE due_work
                SET status = 'cancelled',
                    terminal_outcome = 'projection_policy_missing',
                    terminal_at = %s,
                    retain_until = %s,
                    block_reason = NULL,
                    last_error = NULL,
                    updated_at = NOW()
                WHERE kind = 'lifecycle'
                  AND route_number = %s
                  AND transition_kind = %s
                  AND status = ANY(%s)
                """,
                (
                    now_utc,
                    _terminal_retain_until(now_utc),
                    route_number,
                    ARCHIVE_TRANSITION,
                    list(SUPERSEDABLE_STATUSES),
                ),
            )
        return []

    return [
        (
            delivery_id,
            plan_projected_delivery_from_current_policy(
                conn,
                route_number=route_number,
                delivery_id=delivery_id,
                now_utc=now_utc,
                timezone_name=timezone_name,
            ),
        )
        for delivery_id in delivery_ids
    ]


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def claim_one_lifecycle_work(
    conn,
    *,
    worker_id: str,
    now_utc: datetime,
    lease_seconds: int = 120,
    allowed_semantic_keys: Optional[Sequence[str]] = None,
    all_proven_work: bool = False,
) -> Optional[ClaimedLifecycleWork]:
    """Claim one due lifecycle row with fail-closed canary admission."""
    if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 128:
        raise LifecycleContractError("worker_id must contain 1 through 128 characters")
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or not 30 <= lease_seconds <= 3600:
        raise LifecycleContractError("lease_seconds must be an integer from 30 through 3600")
    allowlist = sorted({str(value) for value in (allowed_semantic_keys or []) if str(value)})
    if not all_proven_work and not allowlist:
        return None

    claim_token = secrets.token_urlsafe(32)
    claim_hash = _token_hash(claim_token)
    lease_expires_at = now_utc + timedelta(seconds=lease_seconds)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH candidate AS (
                SELECT work.id
                FROM due_work work
                WHERE work.kind = 'lifecycle'
                  AND work.status = ANY(%s)
                  AND work.next_attempt_at <= %s
                  AND work.deadline_at >= %s
                  AND work.attempt_count < work.max_attempts
                  AND (%s OR work.semantic_key = ANY(%s))
                  AND pg_try_advisory_xact_lock(
                      hashtextextended(
                          'routespark:pcf-lifecycle:' || work.route_number
                              || ':' || work.delivery_id,
                          0
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM due_work active
                      WHERE active.kind = 'lifecycle'
                        AND active.route_number = work.route_number
                        AND active.delivery_id = work.delivery_id
                        AND active.status IN ('leased', 'dispatching')
                        AND active.lease_expires_at > %s
                  )
                ORDER BY work.next_attempt_at, work.priority DESC, work.created_at, work.id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE due_work work
            SET status = 'leased',
                claim_token_hash = %s,
                claimed_by = %s,
                claimed_at = %s,
                lease_expires_at = %s,
                attempt_count = work.attempt_count + 1,
                block_reason = NULL,
                last_error = NULL,
                updated_at = NOW()
            FROM candidate
            WHERE work.id = candidate.id
            RETURNING work.*
            """,
            (
                list(CLAIMABLE_STATUSES),
                now_utc,
                now_utc,
                all_proven_work,
                allowlist,
                now_utc,
                claim_hash,
                worker_id.strip(),
                now_utc,
                lease_expires_at,
            ),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ClaimedLifecycleWork(
        id=int(row["id"]),
        semantic_key=str(row["semantic_key"]),
        route_number=str(row["route_number"]),
        delivery_id=str(row["delivery_id"]),
        container_id=str(row["container_id"]),
        effective_business_date=row["effective_business_date"],
        due_at=row["due_at"],
        deadline_at=row["deadline_at"],
        source_revision=str(row["source_revision"]),
        source_business_hash=str(row["source_business_hash"]),
        decision_watermark=dict(row["decision_watermark"]),
        claim_token=claim_token,
        attempt_count=int(row["attempt_count"]),
    )


def refresh_lifecycle_claim_authority(
    conn,
    *,
    work_id: int,
    claim_token: str,
    lease_seconds: int = 300,
    begin_effect: bool = False,
) -> Optional[datetime]:
    """CAS-refresh one live claim using PostgreSQL's server clock.

    ``begin_effect`` marks the first irreversible boundary. The update fails if
    the token, lease, or hard deadline is no longer live; callers must perform
    no deletion after a failed refresh.
    """
    if isinstance(work_id, bool) or not isinstance(work_id, int) or work_id < 1:
        raise LifecycleContractError("work_id must be a positive integer")
    if not isinstance(claim_token, str) or not claim_token:
        raise LifecycleContractError("claim_token is required")
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, int)
        or not 30 <= lease_seconds <= 3600
    ):
        raise LifecycleContractError(
            "lease_seconds must be an integer from 30 through 3600"
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH effect_boundary AS MATERIALIZED (
                SELECT clock_timestamp() AS checked_at
            )
            UPDATE due_work
            SET status = CASE WHEN %s THEN 'dispatching' ELSE status END,
                lease_expires_at = effect_boundary.checked_at
                    + (%s * INTERVAL '1 second'),
                dispatch_deadline_at = CASE
                    WHEN %s OR status = 'dispatching'
                    THEN effect_boundary.checked_at + (%s * INTERVAL '1 second')
                    ELSE NULL
                END,
                updated_at = effect_boundary.checked_at
            FROM effect_boundary
            WHERE id = %s
              AND kind = 'lifecycle'
              AND status = ANY(%s)
              AND claim_token_hash = %s
              AND lease_expires_at > effect_boundary.checked_at
              AND deadline_at >= effect_boundary.checked_at
                    + (%s * INTERVAL '1 second')
            RETURNING effect_boundary.checked_at
            """,
            (
                begin_effect,
                lease_seconds,
                begin_effect,
                lease_seconds,
                work_id,
                ["leased", "dispatching"],
                _token_hash(claim_token),
                lease_seconds,
            ),
        )
        row = cur.fetchone()
    return row[0] if row is not None else None


def complete_lifecycle_claim(
    conn,
    *,
    work_id: int,
    claim_token: str,
    now_utc: datetime,
    outcome: str = "archived_verified",
) -> bool:
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE due_work
            SET status = 'complete',
                terminal_outcome = %s,
                terminal_at = %s,
                retain_until = %s,
                claim_token_hash = NULL,
                claimed_by = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                dispatch_deadline_at = NULL,
                block_reason = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND kind = 'lifecycle'
              AND status = ANY(%s)
              AND claim_token_hash = %s
              AND lease_expires_at > %s
            """,
            (
                outcome,
                now_utc,
                _terminal_retain_until(now_utc),
                work_id,
                ["leased", "dispatching"],
                _token_hash(claim_token),
                now_utc,
            ),
        )
        return cur.rowcount == 1


def block_lifecycle_claim(
    conn,
    *,
    work_id: int,
    claim_token: str,
    now_utc: datetime,
    next_attempt_at: datetime,
    reason: str,
    error: Optional[str] = None,
) -> bool:
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    next_attempt_at = _require_aware(next_attempt_at, "next_attempt_at").astimezone(timezone.utc)
    if next_attempt_at <= now_utc:
        raise LifecycleContractError("next_attempt_at must be later than now_utc")
    if not isinstance(reason, str) or not reason or len(reason) > 64:
        raise LifecycleContractError("reason must contain 1 through 64 characters")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE due_work
            SET status = 'blocked',
                next_attempt_at = %s,
                block_reason = %s,
                last_error = %s,
                claim_token_hash = NULL,
                claimed_by = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                dispatch_deadline_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND kind = 'lifecycle'
              AND status = ANY(%s)
              AND claim_token_hash = %s
              AND lease_expires_at > %s
              AND deadline_at >= %s
            """,
            (
                next_attempt_at,
                reason,
                str(error)[:4000] if error else None,
                work_id,
                ["leased", "dispatching"],
                _token_hash(claim_token),
                now_utc,
                next_attempt_at,
            ),
        )
        return cur.rowcount == 1


def recover_expired_lifecycle_leases(
    conn,
    *,
    now_utc: datetime,
    limit: int = 100,
) -> int:
    now_utc = _require_aware(now_utc, "now_utc").astimezone(timezone.utc)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise LifecycleContractError("limit must be an integer from 1 through 500")
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH expired AS (
                SELECT id
                FROM due_work
                WHERE kind = 'lifecycle'
                  AND status = ANY(%s)
                  AND lease_expires_at <= %s
                  AND attempt_count < max_attempts
                  AND deadline_at > %s
                ORDER BY lease_expires_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE due_work work
            SET status = 'retryable',
                next_attempt_at = %s,
                block_reason = 'lease_expired',
                claim_token_hash = NULL,
                claimed_by = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                dispatch_deadline_at = NULL,
                updated_at = NOW()
            FROM expired
            WHERE work.id = expired.id
            """,
            (["leased", "dispatching"], now_utc, now_utc, limit, now_utc),
        )
        return cur.rowcount

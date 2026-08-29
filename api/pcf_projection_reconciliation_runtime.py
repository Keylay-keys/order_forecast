"""Bounded orchestration and fair route reservation for PCF reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Callable

from .pcf_projection_ingress import commit_projection_event
from .pcf_projection_reconciliation import (
    ProjectionReconciliationError,
    RouteReconciliationResult,
    build_absence_tombstone,
    reconcile_route_projection,
    targeted_repair_events,
)
from .pcf_projection_source import FirestoreProjectionSourceAdapter


_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
DEFAULT_RECONCILIATION_CADENCE = timedelta(days=1)
OBSERVE_ONLY_RECONCILIATION_ROUTES = frozenset({"900000"})


def _observed_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ProjectionReconciliationError("observed_at_invalid")
    return value.astimezone(timezone.utc)


def reserve_due_reconciliation_route(
    conn: Any,
    *,
    observed_at: datetime,
    cadence: timedelta = DEFAULT_RECONCILIATION_CADENCE,
) -> str | None:
    """Atomically reserve one eligible route without holding a lock over I/O."""
    observed = _observed_utc(observed_at)
    if not isinstance(cadence, timedelta) or not timedelta(hours=1) <= cadence <= timedelta(days=7):
        raise ProjectionReconciliationError("reconciliation_cadence_invalid")
    cutoff = observed - cadence
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT route_number
            FROM pcf_projection_routes
            WHERE eligible = TRUE
              AND (
                  last_reconciliation_attempt_at IS NULL
                  OR last_reconciliation_attempt_at <= %s
              )
            ORDER BY last_reconciliation_attempt_at ASC NULLS FIRST, route_number
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            (cutoff,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        route = str(row[0])
        cur.execute(
            """
            UPDATE pcf_projection_routes
            SET last_reconciliation_attempt_at = %s,
                updated_at = NOW()
            WHERE route_number = %s AND eligible = TRUE
            """,
            (observed, route),
        )
        if cur.rowcount != 1:
            raise ProjectionReconciliationError("reconciliation_reservation_lost")
    return route


def record_reconciliation_failure(
    conn: Any,
    route_number: str,
) -> None:
    """Fail serving health closed after a selected route cannot be compared."""
    route = str(route_number)
    if _ROUTE_RE.fullmatch(route) is None:
        raise ProjectionReconciliationError("route_invalid")
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pcf_projection_routes
            SET reconciliation_status = 'unknown',
                healthy_until = NULL,
                updated_at = NOW()
            WHERE route_number = %s AND eligible = TRUE
            """,
            (route,),
        )
        if cur.rowcount != 1:
            raise ProjectionReconciliationError("reconciliation_failure_record_lost")


@dataclass(frozen=True)
class ReconciliationRunReport:
    route_number: str
    repair_mode: str
    status: str
    reason: str
    source_snapshot_count: int
    upsert_repairs: int
    tombstone_repairs: int
    unresolved_extras: int
    minimum_firestore_reads: int
    final_result: RouteReconciliationResult


class ProjectionReconciliationRuntime:
    """Run at most two source snapshots with isolated exact-entity repairs."""

    def __init__(
        self,
        *,
        connection_provider: Callable[[], Any],
        connection_releaser: Callable[[Any], None],
        source_adapter: FirestoreProjectionSourceAdapter,
        actor_id: str = "pcf-projection-reconciler",
    ) -> None:
        if not actor_id or len(actor_id) > 255:
            raise ProjectionReconciliationError("reconciliation_actor_invalid")
        self._connection_provider = connection_provider
        self._connection_releaser = connection_releaser
        self._source_adapter = source_adapter
        self._actor_id = actor_id

    def _eligible_preflight(self, route: str) -> None:
        conn = self._connection_provider()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT eligible
                    FROM pcf_projection_routes
                    WHERE route_number = %s
                    """,
                    (route,),
                )
                row = cur.fetchone()
            conn.rollback()
            if row is None:
                raise ProjectionReconciliationError("route_control_missing")
            if not bool(row[0]):
                raise ProjectionReconciliationError("route_not_eligible")
        except Exception:
            conn.rollback()
            raise
        finally:
            self._connection_releaser(conn)

    def _record(
        self,
        source,
        observed_at: datetime,
    ) -> RouteReconciliationResult:
        conn = self._connection_provider()
        try:
            result = reconcile_route_projection(
                conn,
                source,
                observed_at=observed_at,
                healthy_until=None,
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            self._connection_releaser(conn)

    def _commit_repairs(
        self,
        result: RouteReconciliationResult,
        observed_at: datetime,
    ) -> tuple[int, int, int]:
        upserts = list(targeted_repair_events(result))
        tombstones = []
        unresolved_extras = 0
        for key in result.extra_in_projection:
            proof = self._source_adapter.read_exact_absence(
                result.route_number,
                key,
            )
            if proof.exists:
                unresolved_extras += 1
                continue
            tombstones.append(
                build_absence_tombstone(result.route_number, key, proof)
            )
        repairs = [*upserts, *tombstones]
        repair_failures: list[Exception] = []
        for event in repairs:
            conn = self._connection_provider()
            try:
                commit_projection_event(
                    conn,
                    event,
                    actor_id=self._actor_id,
                    now_utc=observed_at,
                )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                repair_failures.append(exc)
            finally:
                self._connection_releaser(conn)
        if repair_failures:
            raise ProjectionReconciliationError("projection_repair_failed") from repair_failures[0]
        return len(upserts), len(tombstones), unresolved_extras

    def run_route(
        self,
        route_number: str,
        *,
        observed_at: datetime | None = None,
    ) -> ReconciliationRunReport:
        route = str(route_number)
        if _ROUTE_RE.fullmatch(route) is None:
            raise ProjectionReconciliationError("route_invalid")
        observed = _observed_utc(observed_at or datetime.now(timezone.utc))

        self._eligible_preflight(route)
        first_snapshot = self._source_adapter.read_route(route)
        first_source = first_snapshot.source
        first = self._record(first_source, observed)
        if first.status == "matched":
            return ReconciliationRunReport(
                route_number=route,
                repair_mode=(
                    "observe_only"
                    if route in OBSERVE_ONLY_RECONCILIATION_ROUTES
                    else "exact_repair"
                ),
                status="matched",
                reason=first.reason,
                source_snapshot_count=1,
                upsert_repairs=0,
                tombstone_repairs=0,
                unresolved_extras=len(first.extra_in_projection),
                minimum_firestore_reads=first_snapshot.minimum_billed_document_reads,
                final_result=first,
            )

        if (
            route in OBSERVE_ONLY_RECONCILIATION_ROUTES
            and first.reason != "projection_ahead_of_snapshot"
        ):
            return ReconciliationRunReport(
                route_number=route,
                repair_mode="observe_only",
                status=first.status,
                reason=first.reason,
                source_snapshot_count=1,
                upsert_repairs=0,
                tombstone_repairs=0,
                unresolved_extras=len(first.extra_in_projection),
                minimum_firestore_reads=first_snapshot.minimum_billed_document_reads,
                final_result=first,
            )

        upsert_repairs = 0
        tombstone_repairs = 0
        unresolved_extras = 0
        if first.reason != "projection_ahead_of_snapshot":
            (
                upsert_repairs,
                tombstone_repairs,
                unresolved_extras,
            ) = self._commit_repairs(first, observed)

        second_snapshot = self._source_adapter.read_route(route)
        second_source = second_snapshot.source
        second = self._record(second_source, observed)
        return ReconciliationRunReport(
            route_number=route,
            repair_mode=(
                "observe_only"
                if route in OBSERVE_ONLY_RECONCILIATION_ROUTES
                else "exact_repair"
            ),
            status=second.status,
            reason=second.reason,
            source_snapshot_count=2,
            upsert_repairs=upsert_repairs,
            tombstone_repairs=tombstone_repairs,
            unresolved_extras=(
                len(second.extra_in_projection)
                if route in OBSERVE_ONLY_RECONCILIATION_ROUTES
                else unresolved_extras
            ),
            minimum_firestore_reads=(
                first_snapshot.minimum_billed_document_reads
                + len(first.extra_in_projection)
                + second_snapshot.minimum_billed_document_reads
            ),
            final_result=second,
        )

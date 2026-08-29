"""Transactional Postgres projection and dashboard finalization for PCF effects."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Mapping, Sequence

from psycopg2.extras import Json, RealDictCursor

from .pcf_active_serving import (
    MAX_ACTIVE_CONTAINERS_PER_ROUTE,
    build_dashboard_summary_payload,
)
from .pcf_effect_inputs import EffectInput
from .pcf_effect_projection import ProjectionSnapshot
from .pcf_event_contract import (
    compare_firestore_revisions,
    firestore_revision_instant,
    projection_event_business_hash,
)
from .pcf_projection_ingress import commit_projection_event
from .pcf_projection_source import build_reconciliation_source_event
from ..scripts.pcf_lifecycle_due import lock_lifecycle_route


_ACTOR_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{2,254}$")


class PcfSummaryError(RuntimeError):
    """Verified PCF state could not be finalized without weakening a fence."""


@dataclass(frozen=True)
class SummaryProof:
    disposition: Literal["applied", "already_correct"]
    route_revision: str
    target_count: int
    summary_sha256: str
    evidence_sha256: str
    verified_at: datetime


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _targets(effect: EffectInput) -> dict[str, str]:
    result: dict[str, str] = {}
    for assignment in effect.proposal["deliveryAssignments"]:
        delivery = str(assignment["deliveryNumber"])
        for raw_container in assignment["containerCodes"]:
            container = str(raw_container)
            if container in result:
                raise PcfSummaryError("pcf_summary_target_duplicate")
            result[container] = delivery
    return result


def _events(
    effect: EffectInput,
    snapshot: ProjectionSnapshot,
) -> tuple[Mapping[str, Any], ...]:
    events: list[Mapping[str, Any]] = []
    for container, delivery in sorted(_targets(effect).items()):
        parent_path = f"routes/{effect.route}/pcfs/{delivery}"
        container_path = f"{parent_path}/containers/{container}"
        parent = snapshot.parents.get(parent_path)
        document = snapshot.containers.get(container_path)
        revision = snapshot.revisions.get(container_path)
        if not isinstance(parent, Mapping) or not isinstance(document, Mapping):
            raise PcfSummaryError("pcf_summary_firestore_target_missing")
        if not isinstance(revision, str) or not revision.endswith("|0"):
            raise PcfSummaryError("pcf_summary_firestore_revision_missing")
        events.append(
            build_reconciliation_source_event(
                route_number=effect.route,
                delivery_id=delivery,
                container_id=container,
                parent_data=parent,
                container_data=document,
                container_update_time=firestore_revision_instant(revision),
                snapshot_read_time=effect.accepted_at,
            )
        )
    if not events:
        raise PcfSummaryError("pcf_summary_target_missing")
    return tuple(events)


def _load_route_rows(conn: Any, route: str) -> list[Mapping[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT route_number, delivery_id, container_id, active,
                   loading_date, page_numbers, items, source_revision,
                   source_business_hash, updated_at
            FROM items_current
            WHERE route_number = %s
              AND active = TRUE
              AND deleted_at IS NULL
            ORDER BY loading_date DESC NULLS LAST, delivery_id DESC, container_id
            LIMIT %s
            """,
            (route, MAX_ACTIVE_CONTAINERS_PER_ROUTE + 1),
        )
        rows = list(cur.fetchall())
    if len(rows) > MAX_ACTIVE_CONTAINERS_PER_ROUTE:
        raise PcfSummaryError("pcf_summary_route_limit_exceeded")
    return rows


def _route_revision(conn: Any, route: str) -> str:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT last_projected_revision
            FROM pcf_projection_routes
            WHERE route_number = %s
            """,
            (route,),
        )
        row = cur.fetchone()
    revision = str((row or {}).get("last_projected_revision") or "")
    if not revision:
        raise PcfSummaryError("pcf_summary_route_revision_missing")
    firestore_revision_instant(revision)
    return revision


def _summary_payload(
    route: str,
    rows: Sequence[Mapping[str, Any]],
    route_revision: str,
) -> dict[str, Any]:
    generated_at = firestore_revision_instant(route_revision)
    payload = build_dashboard_summary_payload(
        route,
        rows,
        now_utc=generated_at,
    )
    payload["generatedAt"] = generated_at.isoformat()
    return payload


def _load_summary(conn: Any, route: str, *, for_update: bool) -> Mapping[str, Any] | None:
    suffix = " FOR UPDATE" if for_update else ""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT route_number, payload, source_revision, source_updated_at,
                   mirrored_revision, mirrored_at, updated_at
            FROM route_dashboard_summaries
            WHERE route_number = %s
            """ + suffix,
            (route,),
        )
        return cur.fetchone()


def _matches_summary(
    row: Mapping[str, Any] | None,
    *,
    payload: Mapping[str, Any],
    route_revision: str,
) -> bool:
    return bool(
        row is not None
        and row.get("payload") == payload
        and row.get("source_revision") == route_revision
        and row.get("mirrored_revision") == route_revision
    )


def _upsert_summary(
    conn: Any,
    *,
    route: str,
    payload: Mapping[str, Any],
    route_revision: str,
    mirrored_at: datetime,
) -> None:
    source_updated_at = firestore_revision_instant(route_revision)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO route_dashboard_summaries (
                route_number, payload, source_revision, source_updated_at,
                mirrored_revision, mirrored_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (route_number) DO UPDATE
            SET payload = EXCLUDED.payload,
                source_revision = EXCLUDED.source_revision,
                source_updated_at = EXCLUDED.source_updated_at,
                mirrored_revision = EXCLUDED.mirrored_revision,
                mirrored_at = EXCLUDED.mirrored_at,
                updated_at = EXCLUDED.updated_at
            """,
            (
                route,
                Json(dict(payload)),
                route_revision,
                source_updated_at,
                route_revision,
                mirrored_at,
                mirrored_at,
            ),
        )


class PcfEffectSummaryStore:
    """Commit verified containers and the route summary in one transaction."""

    def __init__(
        self,
        conn: Any,
        *,
        actor_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if not isinstance(actor_id, str) or _ACTOR_RE.fullmatch(actor_id) is None:
            raise ValueError("invalid PCF summary actor")
        if not callable(clock):
            raise ValueError("invalid PCF summary clock")
        self._conn = conn
        self._actor_id = actor_id
        self._clock = clock

    @property
    def connection(self) -> Any:
        return self._conn

    def apply(self, effect: EffectInput, snapshot: ProjectionSnapshot) -> SummaryProof:
        if not isinstance(effect, EffectInput) or not isinstance(snapshot, ProjectionSnapshot):
            raise ValueError("invalid PCF summary input")
        events = _events(effect, snapshot)
        try:
            results = [
                commit_projection_event(
                    self._conn,
                    event,
                    actor_id=self._actor_id,
                    now_utc=self._clock(),
                )
                for event in events
            ]
            lock_lifecycle_route(self._conn, effect.route)
            route_revision = _route_revision(self._conn, effect.route)
            rows = _load_route_rows(self._conn, effect.route)
            payload = _summary_payload(effect.route, rows, route_revision)
            existing = _load_summary(self._conn, effect.route, for_update=True)
            summary_matches = _matches_summary(
                existing,
                payload=payload,
                route_revision=route_revision,
            )
            verified_at = self._clock().astimezone(timezone.utc)
            if not summary_matches:
                _upsert_summary(
                    self._conn,
                    route=effect.route,
                    payload=payload,
                    route_revision=route_revision,
                    mirrored_at=verified_at,
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        summary_sha256 = _digest(payload)
        disposition = (
            "already_correct"
            if summary_matches
            and all(
                result.duplicate_transport_event
                or result.disposition in {"stale_noop", "duplicate_noop"}
                for result in results
            )
            else "applied"
        )
        return SummaryProof(
            disposition,
            route_revision,
            len(events),
            summary_sha256,
            _digest(
                {
                    "projectionResults": [
                        {
                            "disposition": result.disposition,
                            "duplicateTransportEvent": result.duplicate_transport_event,
                        }
                        for result in results
                    ],
                    "routeRevision": route_revision,
                    "summarySha256": summary_sha256,
                    "targets": sorted(event["sourcePath"] for event in events),
                    "workId": str(effect.work_id),
                }
            ),
            verified_at,
        )


class PcfEffectSummaryVerifier:
    """Independently verify current projection rows and their stored summary."""

    def __init__(
        self,
        conn: Any,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if not callable(clock):
            raise ValueError("invalid PCF summary verifier clock")
        self._conn = conn
        self._clock = clock

    @property
    def connection(self) -> Any:
        return self._conn

    def verify(
        self,
        effect: EffectInput,
        snapshot: ProjectionSnapshot,
        expected: SummaryProof,
    ) -> SummaryProof:
        if not isinstance(expected, SummaryProof):
            raise ValueError("invalid PCF summary proof")
        events = _events(effect, snapshot)
        expected_targets = {
            (
                str(event["entity"]["delivery"]),
                str(event["entity"]["container"]),
            ): (
                str(event["sourceRevision"]),
                projection_event_business_hash(event),
            )
            for event in events
        }
        try:
            lock_lifecycle_route(self._conn, effect.route)
            route_revision = _route_revision(self._conn, effect.route)
            rows = _load_route_rows(self._conn, effect.route)
            row_map = {
                (str(row["delivery_id"]), str(row["container_id"])): row
                for row in rows
            }
            for key, (expected_revision, expected_hash) in expected_targets.items():
                row = row_map.get(key)
                if row is None:
                    raise PcfSummaryError("pcf_summary_projection_target_missing")
                actual_revision = str(row.get("source_revision") or "")
                ordering = compare_firestore_revisions(
                    actual_revision,
                    expected_revision,
                )
                if ordering < 0 or (
                    ordering == 0
                    and str(row.get("source_business_hash") or "") != expected_hash
                ):
                    raise PcfSummaryError("pcf_summary_projection_target_mismatch")
            payload = _summary_payload(effect.route, rows, route_revision)
            summary = _load_summary(self._conn, effect.route, for_update=False)
            if not _matches_summary(
                summary,
                payload=payload,
                route_revision=route_revision,
            ):
                raise PcfSummaryError("pcf_summary_readback_mismatch")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        summary_sha256 = _digest(payload)
        if route_revision == expected.route_revision and summary_sha256 != expected.summary_sha256:
            raise PcfSummaryError("pcf_summary_proof_mismatch")
        verified_at = self._clock().astimezone(timezone.utc)
        return SummaryProof(
            "already_correct",
            route_revision,
            len(events),
            summary_sha256,
            _digest(
                {
                    "routeRevision": route_revision,
                    "summarySha256": summary_sha256,
                    "targets": sorted(event["sourcePath"] for event in events),
                    "workId": str(effect.work_id),
                }
            ),
            verified_at,
        )

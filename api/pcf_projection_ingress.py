"""Transactional, effect-free ingress for normalized PCF projection events."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

from psycopg2.extras import Json, RealDictCursor

from .pcf_event_contract import (
    canonical_json_bytes,
    validate_projection_event,
    validate_source_event_id,
)
from ..scripts.pcf_lifecycle_due import apply_container_projection


RECEIPT_VERSION = "pcf-projection-receipt-v1"
SUPPORTED_EVENT_TYPE = "projection.pcf_container.changed"


class ProjectionIngressConflict(RuntimeError):
    """A source event ID was reused for different immutable content."""


class ProjectionEventNotImplemented(RuntimeError):
    """A valid event must remain retryable until its projection exists."""


@dataclass(frozen=True)
class ProjectionCommitResult:
    disposition: str
    duplicate_transport_event: bool


def _event_hash(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(event)).hexdigest()


def _receipt_payload(
    event: Mapping[str, Any], event_hash: str, disposition: str
) -> dict[str, Any]:
    return {
        "contractVersion": RECEIPT_VERSION,
        "disposition": disposition,
        "entity": dict(event["entity"]),
        "eventHash": event_hash,
        "eventType": event["eventType"],
        "mutationKind": event["mutationKind"],
        "sourcePath": event["sourcePath"],
        "sourceRevision": event["sourceRevision"],
    }


def _advance_route_projection(
    conn: Any,
    *,
    route_number: str,
    source_revision: str,
) -> None:
    """Advance compact route progress without arming serving or freshness.

    The row update shares the item projection transaction.  Eligibility,
    reconciliation proof, ``serving_enabled``, and ``healthy_until`` are owned
    by their explicit control/reconciliation paths and are never inferred from
    the arrival of one container event.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO pcf_projection_routes (
                route_number, last_projected_revision, last_projected_at,
                created_at, updated_at
            ) VALUES (%s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (route_number) DO UPDATE
            SET last_projected_revision = CASE
                    WHEN pcf_projection_routes.last_projected_revision IS NULL
                      OR EXCLUDED.last_projected_revision
                         > pcf_projection_routes.last_projected_revision
                    THEN EXCLUDED.last_projected_revision
                    ELSE pcf_projection_routes.last_projected_revision
                END,
                last_projected_at = CASE
                    WHEN pcf_projection_routes.last_projected_revision IS NULL
                      OR EXCLUDED.last_projected_revision
                         >= pcf_projection_routes.last_projected_revision
                    THEN EXCLUDED.last_projected_at
                    ELSE pcf_projection_routes.last_projected_at
                END,
                updated_at = NOW()
            """,
            (route_number, source_revision),
        )


def record_projection_rejection(
    conn: Any,
    *,
    body_sha256: str,
    reason: str,
    actor_id: str,
    source_event_id: str | None = None,
) -> bool:
    """Persist a bounded rejection without retaining malformed request data."""
    if len(body_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in body_sha256
    ):
        raise ValueError("body_sha256 must be lowercase SHA-256")
    allowed_reasons = {
        "body_too_large",
        "content_type_invalid",
        "contract_validation_failed",
        "json_invalid",
        "source_event_conflict",
    }
    if reason not in allowed_reasons:
        raise ValueError("unsupported rejection reason")
    if source_event_id is not None:
        source_event_id = validate_source_event_id(source_event_id)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if source_event_id is not None:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (source_event_id,),
            )
            cur.execute(
                "SELECT payload FROM pcf_work_events WHERE source_event_id = %s",
                (source_event_id,),
            )
            existing = cur.fetchone()
            if existing is not None:
                payload = existing.get("payload") or {}
                if (
                    payload.get("bodySha256") == body_sha256
                    and payload.get("reason") == reason
                ):
                    return True
                source_event_id = None
                reason = "source_event_conflict"
        cur.execute(
            """
            INSERT INTO pcf_work_events (
                event_id, source_event_id, work_id, attempt_number, event_type,
                actor_type, actor_id, payload
            ) VALUES (%s, %s, NULL, NULL, %s, 'cloud_tasks', %s, %s)
            """,
            (
                str(uuid.uuid4()),
                source_event_id,
                "projection_event_rejected",
                actor_id,
                Json(
                    {
                        "bodySha256": body_sha256,
                        "contractVersion": RECEIPT_VERSION,
                        "reason": reason,
                    }
                ),
            ),
        )
    return False


def commit_projection_event(
    conn: Any,
    event: Mapping[str, Any],
    *,
    actor_id: str,
) -> ProjectionCommitResult:
    """Validate and atomically commit one container projection plus receipt.

    The caller owns commit/rollback. A transaction-scoped advisory lock
    serializes only an identical sourceEventId; hash collisions merely cause
    harmless extra serialization and do not alter correctness.
    """
    normalized = validate_projection_event(event)
    if normalized["eventType"] != SUPPORTED_EVENT_TYPE:
        raise ProjectionEventNotImplemented(normalized["eventType"])
    event_hash = _event_hash(normalized)
    source_event_id = normalized["sourceEventId"]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (source_event_id,),
        )
        cur.execute(
            """
            SELECT payload
            FROM pcf_work_events
            WHERE source_event_id = %s
            """,
            (source_event_id,),
        )
        existing = cur.fetchone()
        if existing is not None:
            payload = existing.get("payload") or {}
            if payload.get("eventHash") != event_hash:
                raise ProjectionIngressConflict("source_event_id_reused")
            return ProjectionCommitResult(
                disposition=str(payload.get("disposition") or "duplicate_noop"),
                duplicate_transport_event=True,
            )

        disposition = apply_container_projection(conn, normalized)
        if disposition != "fixture_suppressed":
            _advance_route_projection(
                conn,
                route_number=str(normalized["entity"]["route"]),
                source_revision=str(normalized["sourceRevision"]),
            )
        cur.execute(
            """
            INSERT INTO pcf_work_events (
                event_id, source_event_id, work_id, attempt_number, event_type,
                actor_type, actor_id, payload
            ) VALUES (%s, %s, NULL, NULL, %s, 'cloud_tasks', %s, %s)
            """,
            (
                str(uuid.uuid4()),
                source_event_id,
                "projection_event_committed",
                actor_id,
                Json(_receipt_payload(normalized, event_hash, disposition)),
            ),
        )
    return ProjectionCommitResult(
        disposition=disposition,
        duplicate_transport_event=False,
    )

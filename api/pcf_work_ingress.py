"""Atomic ingestion of settled PCF uploads into the two-table work ledger."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from psycopg2.extras import Json, RealDictCursor

from .pcf_event_contract import (
    EventContractError,
    canonical_json_bytes,
    validate_pcf_upload_event,
)
from .pcf_worker_contract import ContractValidationError, manifest_pages
from .pcf_work_runtime import EXPECTED_BUCKET


WORK_INGRESS_RECEIPT_VERSION = "pcf-work-ingress-receipt-v1"


class WorkIngressConflict(RuntimeError):
    """An immutable transport or source-revision identity was reused."""


@dataclass(frozen=True)
class WorkIngressResult:
    disposition: str
    duplicate_transport_event: bool
    work_id: uuid.UUID | None
    input_hash_version: str
    input_hash: str


def _event_hash(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(event)).hexdigest()


def _receipt_payload(
    *,
    event: Mapping[str, Any],
    event_hash: str,
    disposition: str,
    input_hash_version: str,
    input_hash: str,
) -> dict[str, Any]:
    fixture = event.get("fixture")
    return {
        "contractVersion": WORK_INGRESS_RECEIPT_VERSION,
        "delivery": event["delivery"],
        "disposition": disposition,
        "eventHash": event_hash,
        "inputHash": input_hash,
        "inputHashVersion": input_hash_version,
        "route": event["route"],
        "sourceQueuePath": event["sourceQueuePath"],
        "sourceRevision": event["sourceRevision"],
        **(
            {
                "fixtureIdentitySha256": hashlib.sha256(
                    str(fixture["identity"]).encode("utf-8")
                ).hexdigest()
            }
            if isinstance(fixture, Mapping)
            else {}
        ),
    }


def _result_from_existing(existing: Mapping[str, Any], event_hash: str) -> WorkIngressResult:
    payload = existing.get("payload") or {}
    if (
        payload.get("contractVersion") != WORK_INGRESS_RECEIPT_VERSION
        or payload.get("eventHash") != event_hash
    ):
        raise WorkIngressConflict("source_event_id_reused")
    work_id = existing.get("work_id")
    return WorkIngressResult(
        disposition=str(payload["disposition"]),
        duplicate_transport_event=True,
        work_id=uuid.UUID(str(work_id)) if work_id is not None else None,
        input_hash_version=str(payload["inputHashVersion"]),
        input_hash=str(payload["inputHash"]),
    )


def commit_pcf_upload_event(
    conn: Any,
    event: Mapping[str, Any],
    *,
    actor_id: str,
    now_utc: datetime | None = None,
) -> WorkIngressResult:
    """Validate and atomically commit one settled-upload receipt and work identity.

    Firestore trigger delivery order is not authoritative. Canonical queue
    ``sourceRevision`` orders successor work, while arrival time remains audit
    metadata only. The caller owns commit/rollback.
    """
    validated = validate_pcf_upload_event(event)
    normalized = validated["event"]
    input_hash = str(validated["inputHash"])
    input_hash_version = str(validated["inputHashVersion"])
    route = str(normalized["route"])
    delivery = str(normalized["delivery"])
    source_revision = str(normalized["sourceRevision"])
    source_event_id = str(normalized["eventId"])
    now_utc = now_utc or datetime.now(timezone.utc)
    event_hash = _event_hash(normalized)

    try:
        manifest_pages(
            normalized["manifest"],
            route=route,
            delivery=delivery,
            expected_bucket=EXPECTED_BUCKET,
        )
    except ContractValidationError as exc:
        raise EventContractError(str(exc)) from exc

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (source_event_id,),
        )
        cur.execute(
            "SELECT work_id, payload FROM pcf_work_events WHERE source_event_id = %s",
            (source_event_id,),
        )
        existing_event = cur.fetchone()
        if existing_event is not None:
            return _result_from_existing(existing_event, event_hash)

        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"{route}:{delivery}",),
        )

        if normalized["automationExcluded"]:
            disposition = "fixture_suppressed"
            work_id = None
        else:
            cur.execute(
                """
                SELECT work_id, input_hash_version, input_hash
                FROM pcf_work_items
                WHERE route_number = %s AND delivery_number = %s
                  AND source_revision = %s
                FOR UPDATE
                """,
                (route, delivery, source_revision),
            )
            same_revision = cur.fetchone()
            if same_revision is not None and (
                same_revision["input_hash_version"] != input_hash_version
                or same_revision["input_hash"] != input_hash
            ):
                raise WorkIngressConflict("source_revision_reused")

            cur.execute(
                """
                SELECT work_id, source_revision, status
                FROM pcf_work_items
                WHERE route_number = %s AND delivery_number = %s
                  AND input_hash_version = %s AND input_hash = %s
                FOR UPDATE
                """,
                (route, delivery, input_hash_version, input_hash),
            )
            existing_work = cur.fetchone()
            if existing_work is not None:
                work_id = uuid.UUID(str(existing_work["work_id"]))
                disposition = "duplicate_work"
                if source_revision > str(existing_work["source_revision"]):
                    cur.execute(
                        """
                        UPDATE pcf_work_items
                        SET source_revision = %s, updated_at = %s
                        WHERE work_id = %s
                        """,
                        (source_revision, now_utc, str(work_id)),
                    )
            else:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pcf_work_items
                        WHERE route_number = %s AND delivery_number = %s
                          AND source_revision COLLATE "C" > %s COLLATE "C"
                          AND status NOT IN ('superseded', 'cancelled')
                    ) AS newer_exists
                    """,
                    (route, delivery, source_revision),
                )
                stale_on_arrival = bool(cur.fetchone()["newer_exists"])
                work_id = uuid.uuid4()
                disposition = "stale_superseded" if stale_on_arrival else "queued"
                initial_status = "superseded" if stale_on_arrival else "queued"
                cur.execute(
                    """
                    INSERT INTO pcf_work_items (
                        work_id, route_number, delivery_number, source_revision,
                        input_hash, input_hash_version, status,
                        required_capability, manifest, ocr_context, source_queue_path,
                        not_before, completed_at, last_error_code,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, 'macos_vision', %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        str(work_id), route, delivery, source_revision,
                        input_hash, input_hash_version, initial_status,
                        Json(normalized["manifest"]), Json(normalized["ocrContext"]),
                        normalized["sourceQueuePath"],
                        now_utc, now_utc if stale_on_arrival else None,
                        "newer_source_revision_exists" if stale_on_arrival else None,
                        now_utc, now_utc,
                    ),
                )

            if disposition != "stale_superseded":
                cur.execute(
                    """
                    UPDATE pcf_work_items
                    SET status = 'superseded', completed_at = %s,
                        last_error_code = 'successor_exists',
                        last_error_message = NULL, updated_at = %s
                    WHERE route_number = %s AND delivery_number = %s
                      AND source_revision COLLATE "C" < %s COLLATE "C"
                      AND status = 'queued' AND work_id <> %s
                    RETURNING work_id, attempt_count
                    """,
                    (now_utc, now_utc, route, delivery, source_revision, str(work_id)),
                )
                superseded_rows = cur.fetchall()
                for row in superseded_rows:
                    cur.execute(
                        """
                        INSERT INTO pcf_work_events (
                            event_id, work_id, attempt_number, event_type,
                            actor_type, actor_id, payload
                        ) VALUES (%s, %s, %s, 'work_superseded',
                                  'cloud_tasks', %s, %s)
                        """,
                        (
                            str(uuid.uuid4()), str(row["work_id"]),
                            int(row["attempt_count"]), actor_id,
                            Json({
                                "reason": "newer_source_revision_ingested",
                                "successorWorkId": str(work_id),
                                "successorSourceRevision": source_revision,
                            }),
                        ),
                    )

        payload = _receipt_payload(
            event=normalized,
            event_hash=event_hash,
            disposition=disposition,
            input_hash_version=input_hash_version,
            input_hash=input_hash,
        )
        cur.execute(
            """
            INSERT INTO pcf_work_events (
                event_id, source_event_id, work_id, attempt_number, event_type,
                actor_type, actor_id, payload
            ) VALUES (%s, %s, %s, %s, 'work_ingress_committed',
                      'cloud_tasks', %s, %s)
            """,
            (
                str(uuid.uuid4()), source_event_id,
                str(work_id) if work_id is not None else None,
                0 if work_id is not None else None,
                actor_id, Json(payload),
            ),
        )

    return WorkIngressResult(
        disposition=disposition,
        duplicate_transport_event=False,
        work_id=work_id,
        input_hash_version=input_hash_version,
        input_hash=input_hash,
    )

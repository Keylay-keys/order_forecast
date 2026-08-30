"""Proof-bearing, exact-path PCF queue TTL arming after verified closure."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from firebase_admin import firestore
from psycopg2.extras import RealDictCursor

from .pcf_effect_runtime import EFFECT_STEPS
from .pcf_queue_ttl import PCF_QUEUE_TTL_FIELD, validate_source_queue_path
from .pcf_queue_ttl_candidates import (
    MAX_QUEUE_TTL_CANDIDATES,
    load_alert_queue_ttl_candidates,
    load_scanner_hold_queue_ttl_candidates,
)
from .pcf_work_runtime import OFFBOX_TARGETS, _event


QUEUE_TTL_RETENTION = timedelta(days=7)
_TERMINAL_QUEUE_STATES = frozenset({"processed", "completed_with_errors"})
_ACTOR_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{2,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]{7,255}$")
_INTENT_SOURCE_PREFIX = "pcf-queue-ttl-intent:"
_ALERT_PATH_RE = re.compile(r"^admin_alerts/[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


class QueueTtlArmError(RuntimeError):
    """A verified-closure or exact Firestore arming prerequisite failed."""


@dataclass(frozen=True)
class QueueTtlHoldSnapshot:
    paths: frozenset[str]
    scanner_hold_rows: int
    alert_rows: int


@dataclass(frozen=True)
class QueueTtlArmIntent:
    work_id: uuid.UUID
    route: str
    delivery: str
    source_queue_path: str
    source_revision: str
    input_hash: str
    work_attempt: int
    work_status: str
    work_completed_at: datetime
    backup_event_id: uuid.UUID
    backup_covers_through: datetime
    effect_event_ids: tuple[uuid.UUID, ...]
    review_alert_path: str | None
    eligible_at: datetime
    expires_at: datetime
    intent_event_id: uuid.UUID
    evidence_sha256: str


@dataclass(frozen=True)
class QueueTtlArmProof:
    source_queue_path: str
    intent_event_id: uuid.UUID
    expires_at: datetime
    verified_at: datetime
    idempotent_replay: bool


@dataclass(frozen=True)
class QueueTtlArmReceipt:
    event_id: uuid.UUID
    recorded_at: datetime
    idempotent_replay: bool


@dataclass(frozen=True)
class QueueTtlArmCycleReport:
    """One committed intent, exact Firestore proof, and committed receipt."""

    intent: QueueTtlArmIntent
    proof: QueueTtlArmProof
    receipt: QueueTtlArmReceipt


@dataclass(frozen=True)
class QueueTtlArmCandidate:
    work_id: uuid.UUID
    source_queue_path: str
    route: str
    delivery: str
    work_status: str


@dataclass(frozen=True)
class QueueTtlArmSweepReport:
    candidate_count: int
    armed_count: int
    replay_count: int
    held_count: int
    deferred_count: int


def _aware_utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise QueueTtlArmError(f"pcf_queue_ttl_{name}_invalid")
    return value.astimezone(timezone.utc)


def _parse_rfc3339(value: Any, *, name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise QueueTtlArmError(f"pcf_queue_ttl_{name}_invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise QueueTtlArmError(f"pcf_queue_ttl_{name}_invalid") from exc
    return _aware_utc(parsed, name=name)


def _actor(value: str) -> str:
    if not isinstance(value, str) or _ACTOR_RE.fullmatch(value) is None:
        raise ValueError("invalid PCF queue TTL actor")
    return value


def _canonical_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_queue_ttl_hold_snapshot(
    db: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> QueueTtlHoldSnapshot:
    """Load the bounded unresolved Firestore hold set without a queue scan."""
    scanner = load_scanner_hold_queue_ttl_candidates(db, maximum=maximum)
    alerts = load_alert_queue_ttl_candidates(db, maximum=maximum)
    paths = frozenset(
        candidate.source_queue_path for candidate in (*scanner, *alerts)
    )
    if len(paths) > maximum:
        raise QueueTtlArmError("pcf_queue_ttl_hold_union_overflow")
    return QueueTtlHoldSnapshot(paths, len(scanner), len(alerts))


def load_queue_ttl_arm_candidates(
    conn: Any,
    *,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
) -> tuple[QueueTtlArmCandidate, ...]:
    """Load only terminal Postgres work lacking a committed TTL receipt."""
    if type(maximum) is not int or not 1 <= maximum <= MAX_QUEUE_TTL_CANDIDATES:
        raise ValueError("invalid PCF queue TTL candidate limit")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT work.work_id, work.route_number, work.delivery_number,
                   work.source_queue_path, work.status
            FROM pcf_work_items AS work
            WHERE work.status IN ('complete', 'review_scheduled')
              AND work.completed_at IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM pcf_work_events AS receipt
                  WHERE receipt.work_id = work.work_id
                    AND receipt.event_type = 'queue_ttl_armed'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM pcf_work_items AS successor
                  WHERE successor.route_number = work.route_number
                    AND successor.delivery_number = work.delivery_number
                    AND successor.source_revision COLLATE "C"
                        > work.source_revision COLLATE "C"
                    AND successor.status NOT IN ('superseded', 'cancelled')
              )
            ORDER BY work.completed_at, work.work_id
            LIMIT %s
            """,
            (maximum + 1,),
        )
        rows = tuple(cur.fetchall())
    if len(rows) > maximum:
        raise QueueTtlArmError("pcf_queue_ttl_arm_candidates_overflow")
    candidates: list[QueueTtlArmCandidate] = []
    for row in rows:
        try:
            work_id = uuid.UUID(str(row["work_id"]))
            route = str(row["route_number"])
            delivery = str(row["delivery_number"])
            path = str(row["source_queue_path"])
            status = str(row["status"])
        except (KeyError, TypeError, ValueError) as exc:
            raise QueueTtlArmError("pcf_queue_ttl_arm_candidate_invalid") from exc
        validate_source_queue_path(path, route=route, delivery=delivery)
        if status not in {"complete", "review_scheduled"}:
            raise QueueTtlArmError("pcf_queue_ttl_arm_candidate_invalid")
        candidates.append(
            QueueTtlArmCandidate(work_id, path, route, delivery, status)
        )
    return tuple(candidates)


def _validate_stored_result(result: Any, *, row: Mapping[str, Any]) -> None:
    if not isinstance(result, Mapping) or set(result) != {
        "schemaVersion",
        "resultHash",
        "acceptanceEventId",
        "acceptedAt",
        "claimTokenHash",
        "request",
    }:
        raise QueueTtlArmError("pcf_queue_ttl_result_invalid")
    request = result.get("request")
    evidence = request.get("evidencePackage") if isinstance(request, Mapping) else None
    if (
        result.get("schemaVersion") != 1
        or not isinstance(result.get("resultHash"), str)
        or _SHA256_RE.fullmatch(str(result.get("resultHash"))) is None
        or not isinstance(evidence, Mapping)
        or evidence.get("route") != row["route_number"]
        or evidence.get("delivery") != row["delivery_number"]
        or evidence.get("inputHash") != row["input_hash"]
        or evidence.get("inputHashVersion") != row["input_hash_version"]
        or not isinstance(evidence.get("artifacts"), list)
        or len(evidence["artifacts"]) < 4
    ):
        raise QueueTtlArmError("pcf_queue_ttl_result_invalid")


def _validate_effect_history(
    events: tuple[Mapping[str, Any], ...],
    *,
    work_id: uuid.UUID,
    work_status: str,
    confidence: int,
) -> tuple[tuple[uuid.UUID, ...], str | None]:
    if len(events) != len(EFFECT_STEPS):
        raise QueueTtlArmError("pcf_queue_ttl_effect_history_incomplete")
    identities: list[uuid.UUID] = []
    review_alert_path: str | None = None
    for index, (event, step) in enumerate(zip(events, EFFECT_STEPS)):
        try:
            event_id = uuid.UUID(str(event["event_id"]))
            event_work_id = uuid.UUID(str(event["work_id"]))
            event_type = str(event["event_type"])
            payload = event["payload"]
            created_at = _aware_utc(event["created_at"], name="effect_event_time")
        except (KeyError, TypeError, ValueError) as exc:
            raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid") from exc
        expected_next = EFFECT_STEPS[index + 1] if index + 1 < len(EFFECT_STEPS) else None
        expected_status = "effects_pending" if expected_next else work_status
        common = {
            "effectStep",
            "effectAttempt",
            "nextEffectStep",
            "nextStatus",
        }
        if (
            event_work_id != work_id
            or event_type not in {"effect_completed", "effect_skipped"}
            or not isinstance(payload, Mapping)
            or payload.get("effectStep") != step
            or type(payload.get("effectAttempt")) is not int
            or payload["effectAttempt"] < 1
            or payload.get("nextEffectStep") != expected_next
            or payload.get("nextStatus") != expected_status
        ):
            raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid")
        if event_type == "effect_skipped":
            if (
                set(payload) != common | {"reason"}
                or step != "notifying"
                or confidence < 92
                or payload.get("reason") != "notification_not_required"
            ):
                raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid")
        else:
            proof = payload.get("proof")
            if set(payload) != common | {"proof"} or not isinstance(proof, Mapping):
                raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid")
            if (
                set(proof)
                != {"disposition", "receiptId", "evidenceSha256", "verifiedAt"}
                or proof.get("disposition") not in {"applied", "already_correct"}
                or not isinstance(proof.get("receiptId"), str)
                or _RECEIPT_RE.fullmatch(proof["receiptId"]) is None
                or not isinstance(proof.get("evidenceSha256"), str)
                or _SHA256_RE.fullmatch(proof["evidenceSha256"]) is None
            ):
                raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid")
            verified_at = _parse_rfc3339(proof.get("verifiedAt"), name="effect_proof_time")
            if verified_at > created_at + timedelta(seconds=30):
                raise QueueTtlArmError("pcf_queue_ttl_effect_history_invalid")
            if step == "notifying" and confidence < 92:
                receipt_id = str(proof["receiptId"])
                if _ALERT_PATH_RE.fullmatch(receipt_id) is None:
                    raise QueueTtlArmError("pcf_queue_ttl_review_alert_proof_invalid")
                review_alert_path = receipt_id
        identities.append(event_id)
    if confidence < 92 and events[3]["event_type"] != "effect_completed":
        raise QueueTtlArmError("pcf_queue_ttl_notification_proof_missing")
    return tuple(identities), review_alert_path


def _select_current_backup(
    rows: tuple[Mapping[str, Any], ...],
    *,
    completed_at: datetime,
    now: datetime,
) -> tuple[uuid.UUID, datetime]:
    for row in rows:
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            continue
        try:
            event_id = uuid.UUID(str(row.get("event_id")))
            created_at = _aware_utc(row.get("created_at"), name="backup_event_time")
            covers = _parse_rfc3339(payload.get("coversThrough"), name="backup_coverage")
            verified = _parse_rfc3339(payload.get("verifiedAt"), name="backup_verified_time")
        except (QueueTtlArmError, TypeError, ValueError):
            continue
        if (
            payload.get("restoreInventoryVerified") is True
            and tuple(payload.get("offBoxTargets") or ()) == OFFBOX_TARGETS
            and covers >= completed_at
            and verified <= now + timedelta(seconds=30)
            and verified >= now - timedelta(hours=24)
            and created_at >= now - timedelta(hours=24)
        ):
            return event_id, covers
    raise QueueTtlArmError("pcf_queue_ttl_current_backup_missing")


def prepare_queue_ttl_arm_intent(
    conn: Any,
    work_id: uuid.UUID,
    *,
    actor_id: str,
    now: datetime,
) -> QueueTtlArmIntent:
    """Freeze one idempotent TTL intent after proving durable workflow closure."""
    if not isinstance(work_id, uuid.UUID):
        raise ValueError("invalid PCF work ID")
    actor_id = _actor(actor_id)
    now = _aware_utc(now, name="intent_time")
    source_event_id = f"{_INTENT_SOURCE_PREFIX}{work_id}"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT work_id, route_number, delivery_number, source_revision,
                   input_hash, input_hash_version, source_queue_path, status,
                   confidence, result, attempt_count, completed_at
            FROM pcf_work_items
            WHERE work_id = %s
            """,
            (str(work_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise QueueTtlArmError("pcf_queue_ttl_work_missing")
        if (
            row.get("status") not in {"complete", "review_scheduled"}
            or type(row.get("confidence")) is not int
            or not 0 <= row["confidence"] <= 100
            or row.get("completed_at") is None
        ):
            raise QueueTtlArmError("pcf_queue_ttl_work_not_closed")
        completed_at = _aware_utc(row["completed_at"], name="work_completed_time")
        validate_source_queue_path(
            str(row["source_queue_path"]),
            route=str(row["route_number"]),
            delivery=str(row["delivery_number"]),
        )
        _validate_stored_result(row.get("result"), row=row)
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pcf_work_items AS blocker
                WHERE blocker.route_number = %s
                  AND blocker.delivery_number = %s
                  AND blocker.work_id <> %s
                  AND (
                    blocker.status IN (
                      'queued', 'leased', 'processing', 'effects_pending', 'blocked'
                    )
                    OR (
                      blocker.source_revision COLLATE "C" > %s COLLATE "C"
                      AND blocker.status NOT IN ('superseded', 'cancelled')
                    )
                  )
            ) AS blocked
            """,
            (
                row["route_number"],
                row["delivery_number"],
                str(work_id),
                row["source_revision"],
            ),
        )
        if bool(cur.fetchone()["blocked"]):
            raise QueueTtlArmError("pcf_queue_ttl_successor_or_work_hold")
        # A committed intent is the durable recovery boundary. Re-check the
        # current work/successor state above, then reuse its frozen expiry even
        # after the receipt that originally authorized it ages past 24 hours.
        # Requiring a new backup here would strand an already-armed Firebase
        # write whose Postgres receipt commit had an ambiguous outcome.
        cur.execute(
            """
            SELECT event_id, payload FROM pcf_work_events
            WHERE source_event_id = %s AND event_type = 'queue_ttl_arm_intent'
            """,
            (source_event_id,),
        )
        existing = cur.fetchone()
        if existing is not None:
            payload = existing.get("payload")
            if not isinstance(payload, Mapping):
                raise QueueTtlArmError("pcf_queue_ttl_intent_conflict")
            return _intent_from_payload(
                payload,
                intent_event_id=uuid.UUID(str(existing["event_id"])),
                expected_work_id=work_id,
            )
        cur.execute(
            """
            SELECT event_id, work_id, event_type, payload, created_at
            FROM pcf_work_events
            WHERE work_id = %s
              AND event_type IN ('effect_completed', 'effect_skipped')
            ORDER BY created_at, event_id
            """,
            (str(work_id),),
        )
        effect_rows = tuple(cur.fetchall())
        effect_event_ids, review_alert_path = _validate_effect_history(
            effect_rows,
            work_id=work_id,
            work_status=str(row["status"]),
            confidence=int(row["confidence"]),
        )
        cur.execute(
            """
            SELECT event_id, payload, created_at
            FROM pcf_work_events
            WHERE event_type = 'offbox_backup_verified'
            ORDER BY created_at DESC, event_id DESC
            LIMIT 32
            """
        )
        backup_event_id, backup_covers = _select_current_backup(
            tuple(cur.fetchall()), completed_at=completed_at, now=now
        )
        expires_at = now + QUEUE_TTL_RETENTION
        core = {
            "workId": str(work_id),
            "route": str(row["route_number"]),
            "delivery": str(row["delivery_number"]),
            "sourceQueuePath": str(row["source_queue_path"]),
            "sourceRevision": str(row["source_revision"]),
            "inputHash": str(row["input_hash"]),
            "workAttempt": int(row["attempt_count"]),
            "workStatus": str(row["status"]),
            "workCompletedAt": completed_at.isoformat().replace("+00:00", "Z"),
            "backupEventId": str(backup_event_id),
            "backupCoversThrough": backup_covers.isoformat().replace("+00:00", "Z"),
            "effectEventIds": [str(value) for value in effect_event_ids],
            "reviewAlertPath": review_alert_path,
            "eligibleAt": now.isoformat().replace("+00:00", "Z"),
            "expiresAt": expires_at.isoformat().replace("+00:00", "Z"),
        }
        payload = {**core, "evidenceSha256": _canonical_digest(core)}
        intent_event_id = _event(
            cur,
            event_type="queue_ttl_arm_intent",
            actor_id=actor_id,
            actor_type="cluster",
            work_id=work_id,
            attempt_number=int(row["attempt_count"]),
            source_event_id=source_event_id,
            payload=payload,
        )
    return _intent_from_payload(
        payload,
        intent_event_id=intent_event_id,
        expected_work_id=work_id,
    )


def _intent_from_payload(
    payload: Mapping[str, Any],
    *,
    intent_event_id: uuid.UUID,
    expected_work_id: uuid.UUID,
) -> QueueTtlArmIntent:
    expected_keys = {
        "workId", "route", "delivery", "sourceQueuePath", "sourceRevision",
        "inputHash", "workAttempt", "workStatus", "workCompletedAt", "backupEventId",
        "backupCoversThrough", "effectEventIds", "reviewAlertPath", "eligibleAt",
        "expiresAt", "evidenceSha256",
    }
    try:
        work_id = uuid.UUID(str(payload.get("workId")))
        backup_event_id = uuid.UUID(str(payload.get("backupEventId")))
        effect_event_ids = tuple(uuid.UUID(str(value)) for value in payload.get("effectEventIds", ()))
        completed_at = _parse_rfc3339(payload.get("workCompletedAt"), name="intent_completed")
        backup_covers = _parse_rfc3339(payload.get("backupCoversThrough"), name="intent_backup")
        eligible_at = _parse_rfc3339(payload.get("eligibleAt"), name="intent_eligible")
        expires_at = _parse_rfc3339(payload.get("expiresAt"), name="intent_expiry")
    except (TypeError, ValueError) as exc:
        raise QueueTtlArmError("pcf_queue_ttl_intent_conflict") from exc
    core = {key: payload[key] for key in expected_keys - {"evidenceSha256"}} if set(payload) == expected_keys else {}
    if (
        work_id != expected_work_id
        or len(effect_event_ids) != len(EFFECT_STEPS)
        or expires_at != eligible_at + QUEUE_TTL_RETENTION
        or not isinstance(payload.get("evidenceSha256"), str)
        or payload.get("evidenceSha256") != _canonical_digest(core)
        or payload.get("workStatus") not in {"complete", "review_scheduled"}
        or type(payload.get("workAttempt")) is not int
        or payload["workAttempt"] < 1
        or (
            payload.get("workStatus") == "review_scheduled"
            and (
                not isinstance(payload.get("reviewAlertPath"), str)
                or _ALERT_PATH_RE.fullmatch(payload["reviewAlertPath"]) is None
            )
        )
        or (
            payload.get("workStatus") == "complete"
            and payload.get("reviewAlertPath") is not None
        )
    ):
        raise QueueTtlArmError("pcf_queue_ttl_intent_conflict")
    route = str(payload.get("route"))
    delivery = str(payload.get("delivery"))
    source_path = validate_source_queue_path(
        str(payload.get("sourceQueuePath")), route=route, delivery=delivery
    )
    return QueueTtlArmIntent(
        work_id=work_id,
        route=route,
        delivery=delivery,
        source_queue_path=source_path,
        source_revision=str(payload.get("sourceRevision")),
        input_hash=str(payload.get("inputHash")),
        work_attempt=int(payload["workAttempt"]),
        work_status=str(payload.get("workStatus")),
        work_completed_at=completed_at,
        backup_event_id=backup_event_id,
        backup_covers_through=backup_covers,
        effect_event_ids=effect_event_ids,
        review_alert_path=(
            str(payload["reviewAlertPath"])
            if payload.get("reviewAlertPath") is not None
            else None
        ),
        eligible_at=eligible_at,
        expires_at=expires_at,
        intent_event_id=intent_event_id,
        evidence_sha256=str(payload["evidenceSha256"]),
    )


def _queue_terminal(data: Mapping[str, Any]) -> bool:
    processing = str(data.get("processingStatus") or "").strip().lower()
    status = str(data.get("status") or "").strip().lower()
    backend = str(data.get("backendProcessingStatus") or "").strip().lower()
    return (
        data.get("pendingBackendProcessing") is False
        and backend == "done"
        and processing in _TERMINAL_QUEUE_STATES
        and status == processing
        and data.get("maintenanceOperation") is None
    )


def arm_exact_queue_ttl(
    db: Any,
    intent: QueueTtlArmIntent,
    holds: QueueTtlHoldSnapshot,
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    transactional: Callable[[Callable[..., Any]], Callable[..., Any]] = firestore.transactional,
) -> QueueTtlArmProof:
    """Arm one exact terminal queue and independently prove the timestamp."""
    if not isinstance(intent, QueueTtlArmIntent) or not isinstance(holds, QueueTtlHoldSnapshot):
        raise ValueError("invalid PCF queue TTL arm input")
    if intent.source_queue_path in holds.paths:
        raise QueueTtlArmError("pcf_queue_ttl_unresolved_firestore_hold")
    now = _aware_utc(now_fn(), name="arm_time")
    if now < intent.eligible_at - timedelta(seconds=30):
        raise QueueTtlArmError("pcf_queue_ttl_intent_from_future")
    if now >= intent.expires_at:
        raise QueueTtlArmError("pcf_queue_ttl_intent_expired")
    queue_ref = db.document(intent.source_queue_path)
    scanner_hold_ref = db.document(
        f"routeEntitlementHolds/{intent.route}/features/scanner"
    )
    review_alert_ref = (
        db.document(intent.review_alert_path) if intent.review_alert_path else None
    )

    @transactional
    def _arm(transaction: Any) -> bool:
        queue_snapshot = queue_ref.get(transaction=transaction)
        scanner_snapshot = scanner_hold_ref.get(transaction=transaction)
        review_alert_snapshot = (
            review_alert_ref.get(transaction=transaction)
            if review_alert_ref is not None
            else None
        )
        if not queue_snapshot.exists:
            raise QueueTtlArmError("pcf_queue_ttl_source_missing")
        queue_data = queue_snapshot.to_dict() or {}
        if not isinstance(queue_data, Mapping) or not _queue_terminal(queue_data):
            raise QueueTtlArmError("pcf_queue_ttl_queue_not_closed")
        if scanner_snapshot.exists:
            scanner = scanner_snapshot.to_dict() or {}
            if (
                scanner.get("status") == "entitlement_hold"
                and str(scanner.get("featureKey") or "") == "scanner"
                and str(scanner.get("heldDeliveryNumber") or "") == intent.delivery
            ):
                raise QueueTtlArmError("pcf_queue_ttl_unresolved_firestore_hold")
        if intent.work_status == "review_scheduled":
            if review_alert_snapshot is None or not review_alert_snapshot.exists:
                raise QueueTtlArmError("pcf_queue_ttl_review_alert_missing")
            alert = review_alert_snapshot.to_dict() or {}
            if (
                alert.get("type") != "ocr_post_send_review"
                or alert.get("status") != "resolved"
                or str(alert.get("routeNumber") or "") != intent.route
                or str(alert.get("deliveryNumber") or "") != intent.delivery
            ):
                raise QueueTtlArmError("pcf_queue_ttl_review_alert_unresolved")
        existing = queue_data.get(PCF_QUEUE_TTL_FIELD)
        if isinstance(existing, datetime):
            existing = _aware_utc(existing, name="existing_expiry")
        if existing == intent.expires_at:
            return True
        transaction.update(queue_ref, {PCF_QUEUE_TTL_FIELD: intent.expires_at})
        return False

    replay = bool(_arm(db.transaction()))
    verified = queue_ref.get()
    if not verified.exists:
        raise QueueTtlArmError("pcf_queue_ttl_source_missing")
    value = (verified.to_dict() or {}).get(PCF_QUEUE_TTL_FIELD)
    if not isinstance(value, datetime) or _aware_utc(value, name="readback_expiry") != intent.expires_at:
        raise QueueTtlArmError("pcf_queue_ttl_arm_unverified")
    return QueueTtlArmProof(
        source_queue_path=intent.source_queue_path,
        intent_event_id=intent.intent_event_id,
        expires_at=intent.expires_at,
        verified_at=now,
        idempotent_replay=replay,
    )


def record_queue_ttl_armed(
    conn: Any,
    intent: QueueTtlArmIntent,
    proof: QueueTtlArmProof,
    *,
    actor_id: str,
    now: datetime,
) -> QueueTtlArmReceipt:
    """Append the post-Firestore receipt; an intent alone never claims arming."""
    if not isinstance(intent, QueueTtlArmIntent) or not isinstance(proof, QueueTtlArmProof):
        raise ValueError("invalid PCF queue TTL receipt input")
    actor_id = _actor(actor_id)
    now = _aware_utc(now, name="receipt_time")
    verified_at = _aware_utc(proof.verified_at, name="receipt_verified_time")
    if (
        proof.source_queue_path != intent.source_queue_path
        or proof.intent_event_id != intent.intent_event_id
        or proof.expires_at != intent.expires_at
        or verified_at < intent.eligible_at - timedelta(seconds=30)
        or verified_at > now + timedelta(seconds=30)
    ):
        raise QueueTtlArmError("pcf_queue_ttl_receipt_proof_mismatch")
    source_event_id = f"pcf-queue-ttl-armed:{intent.intent_event_id}"
    payload = {
        "workId": str(intent.work_id),
        "intentEventId": str(intent.intent_event_id),
        "sourceQueuePath": intent.source_queue_path,
        "expiresAt": intent.expires_at.isoformat().replace("+00:00", "Z"),
        "verifiedAt": verified_at.isoformat().replace("+00:00", "Z"),
        "intentEvidenceSha256": intent.evidence_sha256,
    }
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT event_id, payload, created_at FROM pcf_work_events
            WHERE source_event_id = %s AND event_type = 'queue_ttl_armed'
            """,
            (source_event_id,),
        )
        existing = cur.fetchone()
        if existing is not None:
            existing_payload = existing.get("payload")
            static_keys = frozenset(payload) - {"verifiedAt"}
            try:
                existing_verified_at = _parse_rfc3339(
                    existing_payload.get("verifiedAt"),
                    name="receipt_verified_time",
                )
            except (AttributeError, QueueTtlArmError) as exc:
                raise QueueTtlArmError("pcf_queue_ttl_receipt_conflict") from exc
            if (
                not isinstance(existing_payload, Mapping)
                or frozenset(existing_payload) != frozenset(payload)
                or any(existing_payload.get(key) != payload[key] for key in static_keys)
                or existing_verified_at < intent.eligible_at - timedelta(seconds=30)
                or existing_verified_at > now + timedelta(seconds=30)
            ):
                raise QueueTtlArmError("pcf_queue_ttl_receipt_conflict")
            return QueueTtlArmReceipt(
                uuid.UUID(str(existing["event_id"])),
                _aware_utc(existing["created_at"], name="receipt_created_time"),
                True,
            )
        event_id = _event(
            cur,
            event_type="queue_ttl_armed",
            actor_id=actor_id,
            actor_type="cluster",
            work_id=intent.work_id,
            attempt_number=intent.work_attempt,
            source_event_id=source_event_id,
            payload=payload,
        )
    return QueueTtlArmReceipt(event_id, now, False)


def run_queue_ttl_arm_cycle(
    conn: Any,
    db: Any,
    work_id: uuid.UUID,
    *,
    actor_id: str,
    maximum_holds: int = MAX_QUEUE_TTL_CANDIDATES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    prepare_fn: Callable[..., QueueTtlArmIntent] = prepare_queue_ttl_arm_intent,
    load_holds_fn: Callable[..., QueueTtlHoldSnapshot] = load_queue_ttl_hold_snapshot,
    arm_fn: Callable[..., QueueTtlArmProof] = arm_exact_queue_ttl,
    record_fn: Callable[..., QueueTtlArmReceipt] = record_queue_ttl_armed,
) -> QueueTtlArmCycleReport:
    """Run the retry-safe cross-store sequence for one already-known work ID.

    The intent commits before Firebase I/O. If Firebase succeeds but receipt
    persistence fails, replay reuses the frozen expiry, independently reads it
    back, and appends (or recognizes) the idempotent receipt. This deliberately
    performs no work or queue discovery.
    """
    if not isinstance(work_id, uuid.UUID):
        raise ValueError("invalid PCF work ID")
    actor_id = _actor(actor_id)
    if type(maximum_holds) is not int or not 1 <= maximum_holds <= MAX_QUEUE_TTL_CANDIDATES:
        raise ValueError("invalid PCF queue TTL hold limit")

    try:
        intent = prepare_fn(conn, work_id, actor_id=actor_id, now=now_fn())
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    try:
        holds = load_holds_fn(db, maximum=maximum_holds)
        proof = arm_fn(db, intent, holds, now_fn=now_fn)
        receipt = record_fn(
            conn,
            intent,
            proof,
            actor_id=actor_id,
            now=now_fn(),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return QueueTtlArmCycleReport(intent, proof, receipt)


def run_queue_ttl_arm_sweep(
    conn: Any,
    db: Any,
    *,
    actor_id: str,
    maximum: int = MAX_QUEUE_TTL_CANDIDATES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    load_candidates_fn: Callable[..., tuple[QueueTtlArmCandidate, ...]] = (
        load_queue_ttl_arm_candidates
    ),
    load_holds_fn: Callable[..., QueueTtlHoldSnapshot] = load_queue_ttl_hold_snapshot,
    cycle_fn: Callable[..., QueueTtlArmCycleReport] = run_queue_ttl_arm_cycle,
) -> QueueTtlArmSweepReport:
    """Arm bounded terminal work after one shared unresolved-hold read.

    The caller must commit the backup receipt before invoking this sweep. Held
    candidates are filtered before an intent can freeze their seven-day clock.
    Every actual arm still repeats the exact hold checks transactionally.
    """
    actor_id = _actor(actor_id)
    if type(maximum) is not int or not 1 <= maximum <= MAX_QUEUE_TTL_CANDIDATES:
        raise ValueError("invalid PCF queue TTL candidate limit")
    try:
        candidates = load_candidates_fn(conn, maximum=maximum)
        holds = load_holds_fn(db, maximum=maximum)
        # Close the read-only candidate transaction before per-work intent CAS.
        conn.rollback()
    except Exception:
        conn.rollback()
        raise

    armed = 0
    replays = 0
    held = 0
    deferred = 0
    for candidate in candidates:
        if not isinstance(candidate, QueueTtlArmCandidate):
            raise QueueTtlArmError("pcf_queue_ttl_arm_candidate_invalid")
        if candidate.source_queue_path in holds.paths:
            held += 1
            continue
        try:
            report = cycle_fn(
                conn,
                db,
                candidate.work_id,
                actor_id=actor_id,
                maximum_holds=maximum,
                now_fn=now_fn,
                load_holds_fn=lambda _db, *, maximum: holds,
            )
        except QueueTtlArmError as exc:
            code = str(exc)
            if code in {
                "pcf_queue_ttl_current_backup_missing",
                "pcf_queue_ttl_successor_or_work_hold",
                "pcf_queue_ttl_work_not_closed",
            }:
                deferred += 1
                continue
            if code in {
                "pcf_queue_ttl_unresolved_firestore_hold",
                "pcf_queue_ttl_review_alert_missing",
                "pcf_queue_ttl_review_alert_unresolved",
            }:
                held += 1
                continue
            raise
        armed += 1
        replays += int(report.proof.idempotent_replay or report.receipt.idempotent_replay)
    return QueueTtlArmSweepReport(
        len(candidates),
        armed,
        replays,
        held,
        deferred,
    )

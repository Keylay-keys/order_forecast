"""Authenticated internal PCF ingress endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ..machine_auth import MachinePrincipal
from ..dependencies import get_firestore
from ..pcf_database import get_pcf_pg_connection, return_pcf_pg_connection
from ..pcf_event_contract import EventContractError, validate_source_event_id
from ..pcf_projection_ingress import (
    ProjectionEventNotImplemented,
    ProjectionIngressConflict,
    commit_projection_event,
    projection_rejection_receipt_source_id,
    record_projection_rejection,
)
from ..pcf_projection_alerts import publish_projection_rejection_alert
from ..pcf_queue_ttl import disarm_exact_queue_ttl
from ..pcf_queue_ttl_arming import QueueTtlArmSweepReport, run_queue_ttl_arm_sweep
from ..pcf_queue_ttl_guard import QueueTtlGuardReport, run_queue_ttl_guard_cycle
from ..pcf_runtime_control import PcfRuntimeControl
from ..pcf_worker_contract import (
    AttemptRequest,
    BackupReceiptRequest,
    ClaimRequest,
    ResultRequest,
)
from ..pcf_work_runtime import (
    WorkApiError,
    IamV4ClaimUrlSigner,
    accept_backup_receipt,
    accept_result,
    mint_signed_objects,
    release_unmaterialized_claim,
    renew_lease,
    utc_now,
    wait_for_claim,
)
from ..pcf_work_ingress import WorkIngressConflict, commit_pcf_upload_event
from ...scripts.pcf_lifecycle_due import LifecycleConflictError


router = APIRouter()
logger = logging.getLogger(__name__)
MAX_EVENT_BODY_BYTES = 65_536
MAX_SMALL_WORKER_BODY_BYTES = 16_384
MAX_RESULT_BODY_BYTES = 4 * 1024 * 1024


class _DuplicateJsonKey(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKey("duplicate JSON key")
        result[key] = value
    return result


def _machine_principal(request: Request) -> MachinePrincipal:
    principal = getattr(request.state, "machine_principal", None)
    if not isinstance(principal, MachinePrincipal):
        raise HTTPException(401, "machine_auth_invalid")
    if (
        principal.provider != "google_cloud_tasks_oidc"
        or principal.capability != "pcf.event.ingest"
    ):
        raise HTTPException(403, "capability_forbidden")
    return principal


def _worker_principal(request: Request, capability: str) -> MachinePrincipal:
    principal = getattr(request.state, "machine_principal", None)
    if not isinstance(principal, MachinePrincipal):
        raise WorkApiError(401, "machine_auth_invalid")
    if principal.provider != "routespark_machine_jwt" or principal.capability != capability:
        raise WorkApiError(403, "capability_forbidden")
    return principal


def _runtime_control(request: Request) -> PcfRuntimeControl:
    control = getattr(request.state, "pcf_runtime_control", None)
    if not isinstance(control, PcfRuntimeControl):
        raise WorkApiError(503, "pcf_runtime_control_unavailable", retry_after_seconds=10)
    return control


def _rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _work_error(exc: WorkApiError) -> JSONResponse:
    body: dict[str, Any] = {"error": {"code": exc.code}}
    headers = {"Cache-Control": "no-store"}
    if exc.retry_after_seconds is not None:
        body["error"]["retryAfterSeconds"] = exc.retry_after_seconds
        headers["Retry-After"] = str(exc.retry_after_seconds)
    return JSONResponse(status_code=exc.status_code, content=body, headers=headers)


async def _read_strict_json(request: Request, *, maximum: int) -> dict[str, Any]:
    body = bytearray()
    too_large = False
    async for chunk in request.stream():
        remaining = maximum - len(body)
        if remaining > 0:
            body.extend(chunk[:remaining])
        if len(chunk) > remaining:
            too_large = True
    if too_large:
        raise WorkApiError(422, "result_validation_failed")
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise WorkApiError(422, "result_validation_failed")
    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise WorkApiError(422, "result_validation_failed") from exc
    if not isinstance(value, dict):
        raise WorkApiError(422, "result_validation_failed")
    return value


def build_claim_url_signer() -> IamV4ClaimUrlSigner:
    return IamV4ClaimUrlSigner()


async def _read_bounded_body(request: Request) -> tuple[bytes, str, bool]:
    """Hash the full request while retaining no more than the contract limit."""
    body = bytearray()
    digest = hashlib.sha256()
    too_large = False
    async for chunk in request.stream():
        digest.update(chunk)
        remaining = MAX_EVENT_BODY_BYTES - len(body)
        if remaining > 0:
            body.extend(chunk[:remaining])
        if len(chunk) > remaining:
            too_large = True
    return bytes(body), digest.hexdigest(), too_large


def _persist_rejection(*, body_sha256: str, reason: str, actor_id: str) -> Response:
    conn = None
    try:
        conn = get_pcf_pg_connection()
        record_projection_rejection(
            conn,
            body_sha256=body_sha256,
            reason=reason,
            actor_id=actor_id,
        )
        conn.commit()
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf projection rejection receipt failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)


def _source_event_id_or_none(event: dict[str, Any]) -> str | None:
    for key in ("sourceEventId", "eventId"):
        try:
            return validate_source_event_id(event.get(key))
        except EventContractError:
            continue
    return None


def _contract_rejection_reason(exc: EventContractError) -> str:
    message = str(exc)
    if message in {"unsupported eventType", "unsupported schemaVersion"}:
        return "unknown_schema_or_event_type"
    return "contract_validation_failed"


def _publish_severe_rejection_alert(
    *,
    body_sha256: str,
    reason: str,
    actor_id: str,
    source_event_id: str | None,
) -> str:
    receipt_source_event_id = projection_rejection_receipt_source_id(
        body_sha256=body_sha256,
        reason=reason,
        source_event_id=source_event_id,
    )
    return publish_projection_rejection_alert(
        get_firestore(),
        receipt_source_event_id=receipt_source_event_id,
        reason=reason,
        actor_id=actor_id,
        observed_at=datetime.now(timezone.utc),
    )


@router.post("/internal/v1/pcf/claims")
async def claim_work(request: Request) -> Response:
    """Return one committed exact-generation OCR claim or bounded idle."""
    try:
        principal = _worker_principal(request, "pcf.claim")
        raw = await _read_strict_json(request, maximum=MAX_SMALL_WORKER_BODY_BYTES)
        try:
            claim_request = ClaimRequest.model_validate(raw)
        except ValidationError as exc:
            code = (
                "schema_version_unsupported"
                if isinstance(raw.get("supportedResultSchemaVersions"), list)
                and 1 not in raw["supportedResultSchemaVersions"]
                else "result_validation_failed"
            )
            raise WorkApiError(422, code) from exc
        conn = None
        try:
            conn = get_pcf_pg_connection()
            grant = wait_for_claim(conn, claim_request, actor_id=principal.principal)
            if grant is None:
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            try:
                disarm_exact_queue_ttl(
                    get_firestore(),
                    source_queue_path=grant.source_queue_path,
                    route=grant.route,
                    delivery=grant.delivery,
                )
            except Exception as exc:
                logger.error("pcf queue TTL disarm failed: %s", type(exc).__name__)
                if not release_unmaterialized_claim(
                    conn,
                    grant,
                    actor_id=principal.principal,
                    now=utc_now(),
                    reason="queue_ttl_disarm_failed",
                ):
                    conn.rollback()
                    raise WorkApiError(
                        503,
                        "pcf_claim_unavailable",
                        retry_after_seconds=10,
                    ) from exc
                conn.commit()
                raise WorkApiError(
                    503,
                    "pcf_claim_unavailable",
                    retry_after_seconds=10,
                ) from exc
            try:
                signed_objects = mint_signed_objects(grant, build_claim_url_signer())
            except Exception as exc:
                logger.error("pcf claim materialization failed: %s", type(exc).__name__)
                if not release_unmaterialized_claim(
                    conn,
                    grant,
                    actor_id=principal.principal,
                    now=utc_now(),
                    reason="signed_url_unavailable",
                ):
                    conn.rollback()
                    raise WorkApiError(503, "pcf_claim_unavailable", retry_after_seconds=10) from exc
                conn.commit()
                raise WorkApiError(503, "pcf_claim_unavailable", retry_after_seconds=10) from exc
            return JSONResponse(
                status_code=200,
                content={
                    "schemaVersion": 1,
                    "workId": str(grant.work_id),
                    "attemptNumber": grant.attempt_number,
                    "claimToken": grant.claim_token,
                    "leaseExpiresAt": _rfc3339(grant.lease_expires_at),
                    "hardDeadlineAt": _rfc3339(grant.hard_deadline_at),
                    "route": grant.route,
                    "delivery": grant.delivery,
                    "inputHashVersion": grant.input_hash_version,
                    "inputHash": grant.input_hash,
                    "sourceQueuePath": grant.source_queue_path,
                    "manifest": grant.manifest,
                    "ocrContext": grant.ocr_context,
                    "signedObjects": [
                        {
                            "objectPath": item.object_path,
                            "generation": item.generation,
                            "url": item.url,
                            "expiresAt": _rfc3339(item.expires_at),
                        }
                        for item in signed_objects
                    ],
                },
                headers={"Cache-Control": "no-store"},
            )
        except WorkApiError:
            raise
        except Exception as exc:
            if conn is not None:
                conn.rollback()
            logger.error("pcf claim failed: %s", type(exc).__name__)
            raise WorkApiError(503, "pcf_claim_unavailable", retry_after_seconds=10) from exc
        finally:
            if conn is not None:
                return_pcf_pg_connection(conn)
    except WorkApiError as exc:
        return _work_error(exc)


@router.post("/internal/v1/pcf/work/{work_id}/heartbeat")
async def heartbeat_work(work_id: UUID, request: Request) -> Response:
    """Renew one live attempt without extending its immutable hard deadline."""
    conn = None
    try:
        principal = _worker_principal(request, "pcf.heartbeat")
        raw = await _read_strict_json(request, maximum=MAX_SMALL_WORKER_BODY_BYTES)
        try:
            attempt = AttemptRequest.model_validate(raw)
        except ValidationError as exc:
            raise WorkApiError(422, "result_validation_failed") from exc
        conn = get_pcf_pg_connection()
        renewal = renew_lease(
            conn,
            work_id,
            attempt_number=attempt.attempt_number,
            claim_token=attempt.claim_token,
            actor_id=principal.principal,
            now=utc_now(),
        )
        conn.commit()
        return JSONResponse(
            status_code=200,
            content={
                "leaseExpiresAt": _rfc3339(renewal.lease_expires_at),
                "hardDeadlineAt": _rfc3339(renewal.hard_deadline_at),
            },
            headers={"Cache-Control": "no-store"},
        )
    except WorkApiError as exc:
        if conn is not None:
            conn.commit() if exc.commit else conn.rollback()
        return _work_error(exc)
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf heartbeat failed: %s", type(exc).__name__)
        return _work_error(WorkApiError(503, "pcf_worker_api_unavailable", retry_after_seconds=10))
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)


@router.put("/internal/v1/pcf/work/{work_id}/result")
async def submit_work_result(work_id: UUID, request: Request) -> Response:
    """Accept one immutable, fully validated OCR proposal or its exact replay."""
    conn = None
    try:
        principal = _worker_principal(request, "pcf.result")
        raw = await _read_strict_json(request, maximum=MAX_RESULT_BODY_BYTES)
        try:
            result_request = ResultRequest.model_validate(raw)
        except ValidationError as exc:
            code = "schema_version_unsupported" if raw.get("schemaVersion") != 1 else "result_validation_failed"
            raise WorkApiError(422, code) from exc
        conn = get_pcf_pg_connection()
        acceptance = accept_result(
            conn,
            work_id,
            result_request,
            raw,
            actor_id=principal.principal,
            now=utc_now(),
        )
        conn.commit()
        return JSONResponse(
            status_code=200,
            content={
                "acceptanceEventId": str(acceptance.acceptance_event_id),
                "acceptedAt": _rfc3339(acceptance.accepted_at),
                "status": acceptance.status,
                "effectStep": acceptance.effect_step,
                "idempotentReplay": acceptance.idempotent_replay,
            },
            headers={"Cache-Control": "no-store"},
        )
    except WorkApiError as exc:
        if conn is not None:
            conn.commit() if exc.commit else conn.rollback()
        return _work_error(exc)
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf result failed: %s", type(exc).__name__)
        return _work_error(WorkApiError(503, "pcf_worker_api_unavailable", retry_after_seconds=10))
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)


@router.post("/internal/v1/pcf/backups/offbox-verifications")
async def verify_offbox_backup(request: Request) -> Response:
    """Append an exact receipt for a cluster-recorded immutable backup manifest."""
    conn = None
    try:
        principal = _worker_principal(request, "pcf.backup.verify")
        control = _runtime_control(request)
        raw = await _read_strict_json(request, maximum=MAX_SMALL_WORKER_BODY_BYTES)
        try:
            receipt = BackupReceiptRequest.model_validate(raw)
        except ValidationError as exc:
            raise WorkApiError(422, "backup_receipt_invalid") from exc
        conn = get_pcf_pg_connection()
        acceptance = accept_backup_receipt(
            conn,
            receipt,
            raw,
            actor_id=principal.principal,
            now=utc_now(),
        )
        conn.commit()
        ttl_report: QueueTtlGuardReport | None = None
        arm_report: QueueTtlArmSweepReport | None = None
        if control.allows_ttl_repair:
            try:
                db = get_firestore()
                ttl_report = run_queue_ttl_guard_cycle(conn, db)
                # End the read-only PostgreSQL transaction opened by candidate
                # discovery. The receipt was committed before Firebase I/O.
                conn.rollback()
                arm_report = run_queue_ttl_arm_sweep(
                    conn,
                    db,
                    actor_id=principal.principal,
                    now_fn=utc_now,
                )
            except Exception as exc:
                logger.error(
                    "pcf queue TTL guard failed after backup receipt: %s",
                    type(exc).__name__,
                )
                raise WorkApiError(
                    503,
                    "pcf_queue_ttl_guard_unavailable",
                    retry_after_seconds=10,
                ) from exc
        return JSONResponse(
            status_code=200,
            content={
                "eventId": str(acceptance.event_id),
                "acceptedAt": _rfc3339(acceptance.accepted_at),
                "idempotentReplay": acceptance.idempotent_replay,
                "queueTtlGuard": (
                    {
                        "executed": True,
                        "candidateCount": ttl_report.candidate_count,
                        "verifiedDisarms": ttl_report.verified_disarms,
                    }
                    if ttl_report is not None
                    else {"executed": False}
                ),
                "queueTtlArming": (
                    {
                        "executed": True,
                        "candidateCount": arm_report.candidate_count,
                        "armedCount": arm_report.armed_count,
                        "replayCount": arm_report.replay_count,
                        "heldCount": arm_report.held_count,
                        "deferredCount": arm_report.deferred_count,
                    }
                    if arm_report is not None
                    else {"executed": False}
                ),
            },
            headers={"Cache-Control": "no-store"},
        )
    except WorkApiError as exc:
        if conn is not None:
            conn.commit() if exc.commit else conn.rollback()
        return _work_error(exc)
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf backup receipt failed: %s", type(exc).__name__)
        return _work_error(WorkApiError(503, "pcf_worker_api_unavailable", retry_after_seconds=10))
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)


@router.post("/internal/v1/pcf/events")
async def ingest_projection_event(request: Request) -> Response:
    """Commit one authenticated Cloud Tasks projection event or durable rejection."""
    principal = _machine_principal(request)
    body, body_sha256, too_large = await _read_bounded_body(request)
    if too_large:
        return _persist_rejection(
            body_sha256=body_sha256,
            reason="body_too_large",
            actor_id=principal.principal,
        )
    content_type = (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    )
    if content_type != "application/json":
        return _persist_rejection(
            body_sha256=body_sha256,
            reason="content_type_invalid",
            actor_id=principal.principal,
        )
    try:
        event = json.loads(body.decode("utf-8"), object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey):
        return _persist_rejection(
            body_sha256=body_sha256,
            reason="json_invalid",
            actor_id=principal.principal,
        )
    if not isinstance(event, dict):
        return _persist_rejection(
            body_sha256=body_sha256,
            reason="contract_validation_failed",
            actor_id=principal.principal,
        )

    conn = None
    try:
        conn = get_pcf_pg_connection()
        if event.get("eventType") == "pcf.upload.settled":
            work_result = commit_pcf_upload_event(
                conn,
                event,
                actor_id=principal.principal,
            )
            conn.commit()
            return JSONResponse(
                status_code=200,
                content={
                    "status": (
                        "duplicate"
                        if work_result.duplicate_transport_event
                        else "accepted"
                    ),
                    "disposition": work_result.disposition,
                    "workId": (
                        str(work_result.work_id)
                        if work_result.work_id is not None
                        else None
                    ),
                    "inputHashVersion": work_result.input_hash_version,
                    "inputHash": work_result.input_hash,
                },
                headers={"Cache-Control": "no-store"},
            )
        result = commit_projection_event(conn, event, actor_id=principal.principal)
        conn.commit()
        return JSONResponse(
            status_code=200,
            content={
                "status": "duplicate" if result.duplicate_transport_event else "accepted",
                "disposition": result.disposition,
                "planningDisposition": result.planning_disposition,
                "plannedDeliveryCount": result.planned_delivery_count,
            },
            headers={"Cache-Control": "no-store"},
        )
    except ProjectionEventNotImplemented:
        if conn is not None:
            conn.rollback()
        raise HTTPException(503, "projection_event_not_implemented") from None
    except EventContractError as exc:
        if conn is not None:
            conn.rollback()
            try:
                reason = _contract_rejection_reason(exc)
                record_projection_rejection(
                    conn,
                    body_sha256=body_sha256,
                    reason=reason,
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                conn.commit()
                if reason == "unknown_schema_or_event_type":
                    _publish_severe_rejection_alert(
                        body_sha256=body_sha256,
                        reason=reason,
                        actor_id=principal.principal,
                        source_event_id=_source_event_id_or_none(event),
                    )
                    logger.error(
                        "pcf_projection_alert_required reason=%s body_sha256=%s",
                        reason,
                        body_sha256,
                    )
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            except Exception as exc:
                conn.rollback()
                logger.error("pcf contract rejection handling failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    except (ProjectionIngressConflict, WorkIngressConflict):
        if conn is not None:
            conn.rollback()
            try:
                record_projection_rejection(
                    conn,
                    body_sha256=body_sha256,
                    reason="source_event_conflict",
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                conn.commit()
                _publish_severe_rejection_alert(
                    body_sha256=body_sha256,
                    reason="source_event_conflict",
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                logger.error(
                    "pcf_projection_alert_required reason=source_event_conflict body_sha256=%s",
                    body_sha256,
                )
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            except Exception as exc:
                conn.rollback()
                logger.error("pcf conflict rejection handling failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    except LifecycleConflictError:
        if conn is not None:
            conn.rollback()
            try:
                record_projection_rejection(
                    conn,
                    body_sha256=body_sha256,
                    reason="revision_conflict",
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                conn.commit()
                _publish_severe_rejection_alert(
                    body_sha256=body_sha256,
                    reason="revision_conflict",
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                logger.error(
                    "pcf_projection_alert_required reason=revision_conflict body_sha256=%s",
                    body_sha256,
                )
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            except Exception as exc:
                conn.rollback()
                logger.error("pcf revision rejection handling failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf projection ingress failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)

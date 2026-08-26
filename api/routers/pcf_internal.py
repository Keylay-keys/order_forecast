"""Authenticated internal PCF ingress endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from ..machine_auth import MachinePrincipal
from ..pcf_database import get_pcf_pg_connection, return_pcf_pg_connection
from ..pcf_event_contract import EventContractError, validate_source_event_id
from ..pcf_projection_ingress import (
    ProjectionEventNotImplemented,
    ProjectionIngressConflict,
    commit_projection_event,
    record_projection_rejection,
)
from ...scripts.pcf_lifecycle_due import LifecycleConflictError


router = APIRouter()
logger = logging.getLogger(__name__)
MAX_EVENT_BODY_BYTES = 65_536


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
    try:
        return validate_source_event_id(event.get("sourceEventId"))
    except EventContractError:
        return None


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
        result = commit_projection_event(
            conn,
            event,
            actor_id=principal.principal,
        )
        conn.commit()
        return JSONResponse(
            status_code=200,
            content={
                "status": "duplicate" if result.duplicate_transport_event else "accepted",
                "disposition": result.disposition,
            },
            headers={"Cache-Control": "no-store"},
        )
    except ProjectionEventNotImplemented:
        if conn is not None:
            conn.rollback()
        raise HTTPException(503, "projection_event_not_implemented") from None
    except EventContractError:
        if conn is not None:
            conn.rollback()
            try:
                record_projection_rejection(
                    conn,
                    body_sha256=body_sha256,
                    reason="contract_validation_failed",
                    actor_id=principal.principal,
                    source_event_id=_source_event_id_or_none(event),
                )
                conn.commit()
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            except Exception as exc:
                conn.rollback()
                logger.error("pcf contract rejection receipt failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    except (ProjectionIngressConflict, LifecycleConflictError):
        if conn is not None:
            conn.rollback()
            try:
                record_projection_rejection(
                    conn,
                    body_sha256=body_sha256,
                    reason="source_event_conflict",
                    actor_id=principal.principal,
                )
                conn.commit()
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            except Exception as exc:
                conn.rollback()
                logger.error("pcf conflict rejection receipt failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.error("pcf projection ingress failed: %s", type(exc).__name__)
        raise HTTPException(503, "pcf_ingress_unavailable") from None
    finally:
        if conn is not None:
            return_pcf_pg_connection(conn)

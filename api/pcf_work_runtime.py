"""Transactional PCF claim, lease, result, and backup-receipt state machine."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import select
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, unquote, urlsplit

import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from psycopg2.extras import Json, RealDictCursor

from .pcf_event_contract import validate_pcf_ocr_context
from .pcf_worker_contract import (
    BackupReceiptRequest,
    ClaimRequest,
    ContractValidationError,
    ResultRequest,
    canonical_result_identity,
    manifest_pages,
    validate_result_for_work,
)


LEASE_SECONDS = 120
HEARTBEAT_SECONDS = 30
SIGNED_URL_MARGIN_SECONDS = 300
EXPECTED_BUCKET = "routespark-1f47d.firebasestorage.app"
EXPECTED_SIGNER = "routespark-pcf-object-signer@routespark-1f47d.iam.gserviceaccount.com"
EXPECTED_SIGNER_CALLER = "routespark-pcf-projection-fb@routespark-1f47d.iam.gserviceaccount.com"
EXPECTED_MAC_PRINCIPAL = "pcf-macos-vision-01"
EXPECTED_BACKUP_PRINCIPAL = "pcf-dell-backup-01"
OFFBOX_TARGETS = (
    "dell:/srv/routespark/backups/postgres",
    "dell:/mnt/archive/backups/postgres",
)
WORK_NOTIFY_CHANNEL = "pcf_work_available"
D5_CONTRACT_PATH = Path(__file__).resolve().parents[1] / "contracts/pcf/d5-deadline-v1.json"


class WorkApiError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        code: str,
        *,
        retry_after_seconds: int | None = None,
        commit: bool = False,
    ):
        super().__init__(code)
        self.status_code = status_code
        self.code = code
        self.retry_after_seconds = retry_after_seconds
        self.commit = commit


@dataclass(frozen=True)
class ClaimGrant:
    work_id: uuid.UUID
    attempt_number: int
    claim_token: str
    lease_expires_at: datetime
    hard_deadline_at: datetime
    route: str
    delivery: str
    input_hash_version: str
    input_hash: str
    source_queue_path: str
    manifest: dict[str, Any]
    ocr_context: dict[str, Any]


@dataclass(frozen=True)
class LeaseRenewal:
    lease_expires_at: datetime
    hard_deadline_at: datetime


@dataclass(frozen=True)
class ResultAcceptance:
    acceptance_event_id: uuid.UUID
    accepted_at: datetime
    status: str
    effect_step: str | None
    idempotent_replay: bool


@dataclass(frozen=True)
class BackupAcceptance:
    event_id: uuid.UUID
    accepted_at: datetime
    idempotent_replay: bool


@dataclass(frozen=True)
class SignedObject:
    object_path: str
    generation: str
    url: str
    expires_at: datetime


class ClaimUrlSigner(Protocol):
    def sign_get(
        self,
        *,
        bucket: str,
        object_path: str,
        generation: str,
        expires_at: datetime,
    ) -> str: ...


class IamV4ClaimUrlSigner:
    """V4 signer using IAM signBlob and no private signer-key file."""

    def __init__(
        self,
        *,
        signer_email: str = EXPECTED_SIGNER,
        caller_email: str = EXPECTED_SIGNER_CALLER,
    ) -> None:
        configured_signer = os.environ.get("PCF_CLAIM_SIGNER_SERVICE_ACCOUNT", "").strip()
        if configured_signer != signer_email:
            raise RuntimeError("dedicated PCF claim signer is not configured")
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credential_email = str(getattr(credentials, "service_account_email", "") or "")
        if credential_email != caller_email:
            raise RuntimeError("claim signer caller is not the dedicated projection identity")
        if project_id not in {None, "routespark-1f47d"}:
            raise RuntimeError("claim signer caller belongs to the wrong project")
        self._credentials = credentials
        self._signer_email = signer_email
        self._client = storage.Client(project="routespark-1f47d", credentials=credentials)

    def sign_get(
        self,
        *,
        bucket: str,
        object_path: str,
        generation: str,
        expires_at: datetime,
    ) -> str:
        if bucket != EXPECTED_BUCKET or not generation.isdigit() or int(generation) < 1:
            raise RuntimeError("claim object escaped the signer boundary")
        if not self._credentials.valid or not self._credentials.token:
            self._credentials.refresh(GoogleAuthRequest())
        blob = self._client.bucket(bucket).blob(object_path, generation=int(generation))
        return blob.generate_signed_url(
            version="v4",
            expiration=expires_at,
            method="GET",
            generation=generation,
            service_account_email=self._signer_email,
            access_token=self._credentials.token,
            scheme="https",
        )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _matches_token(token: str, expected_hash: str | None) -> bool:
    return bool(expected_hash) and hmac.compare_digest(_token_hash(token), expected_hash)


@lru_cache(maxsize=1)
def _d5_contract() -> dict[str, Any]:
    raw = json.loads(D5_CONTRACT_PATH.read_text(encoding="utf-8"))
    deadlines = raw.get("hardDeadlineSeconds")
    lease = raw.get("lease")
    signed_url_lifetime = (
        lease.get("signedUrlMinimumLifetimeAfterClaimSeconds")
        if isinstance(lease, dict)
        else None
    )
    if (
        raw.get("contractVersion") != "pcf-d5-deadline-v1"
        or raw.get("evidence", {}).get("evidenceReady") is not True
        or not isinstance(deadlines, dict)
        or set(deadlines) != {"1", "2", "3-4", "5-8", "9+"}
        or any(type(value) is not int or not 600 <= value <= 2700 for value in deadlines.values())
        or not isinstance(lease, dict)
        or lease.get("heartbeatSeconds") != HEARTBEAT_SECONDS
        or lease.get("renewableLeaseSeconds") != LEASE_SECONDS
        or type(signed_url_lifetime) is not int
        or signed_url_lifetime < max(deadlines.values()) + SIGNED_URL_MARGIN_SECONDS
    ):
        raise RuntimeError("invalid checked-in D5 deadline contract")
    return raw


def _deadline_seconds(manifest: dict[str, Any]) -> int:
    pages = manifest.get("objects") if isinstance(manifest, dict) else None
    count = len(pages) if isinstance(pages, list) else 0
    if count < 1:
        raise ContractValidationError("manifest has no pages")
    if count == 1:
        band = "1"
    elif count == 2:
        band = "2"
    elif count <= 4:
        band = "3-4"
    elif count <= 8:
        band = "5-8"
    else:
        band = "9+"
    return int(_d5_contract()["hardDeadlineSeconds"][band])


def _event(
    cur: Any,
    *,
    event_type: str,
    actor_id: str,
    payload: dict[str, Any],
    work_id: uuid.UUID | None = None,
    attempt_number: int | None = None,
    source_event_id: str | None = None,
    event_id: uuid.UUID | None = None,
    actor_type: str = "machine",
) -> uuid.UUID:
    identity = event_id or uuid.uuid4()
    cur.execute(
        """
        INSERT INTO pcf_work_events (
            event_id, source_event_id, work_id, attempt_number, event_type,
            actor_type, actor_id, payload
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(identity),
            source_event_id,
            str(work_id) if work_id is not None else None,
            attempt_number,
            event_type,
            actor_type,
            actor_id,
            Json(payload),
        ),
    )
    return identity


def recover_expired_claims(
    conn: Any,
    *,
    now: datetime,
    actor_id: str = "pcf-lease-recovery",
    limit: int = 20,
) -> int:
    """Boundedly requeue or dead-letter expired claims before admitting more."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT candidate.work_id, candidate.route_number,
                   candidate.delivery_number, candidate.source_revision,
                   candidate.attempt_count, candidate.max_attempts,
                   EXISTS (
                       SELECT 1
                       FROM pcf_work_items AS successor
                       WHERE successor.route_number = candidate.route_number
                         AND successor.delivery_number = candidate.delivery_number
                         AND successor.source_revision COLLATE "C"
                             > candidate.source_revision COLLATE "C"
                         AND successor.status NOT IN ('superseded', 'cancelled')
                   ) AS successor_exists
            FROM pcf_work_items AS candidate
            WHERE candidate.status IN ('leased', 'processing')
              AND (lease_expires_at <= %s OR hard_deadline_at <= %s)
            ORDER BY LEAST(lease_expires_at, hard_deadline_at), candidate.work_id
            FOR UPDATE OF candidate SKIP LOCKED
            LIMIT %s
            """,
            (now, now, limit),
        )
        rows = cur.fetchall()
        for row in rows:
            superseded = bool(row["successor_exists"])
            exhausted = int(row["attempt_count"]) >= int(row["max_attempts"])
            next_status = "superseded" if superseded else ("dead_letter" if exhausted else "queued")
            cur.execute(
                """
                UPDATE pcf_work_items
                SET status = %s,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    worker_heartbeat_at = NULL,
                    claim_token_hash = NULL,
                    hard_deadline_at = NULL,
                    not_before = CASE WHEN %s THEN not_before ELSE %s END,
                    completed_at = CASE WHEN %s THEN %s ELSE NULL END,
                    last_error_code = %s,
                    last_error_message = NULL,
                    updated_at = %s
                WHERE work_id = %s
                """,
                (
                    next_status,
                    superseded or exhausted,
                    now,
                    superseded or exhausted,
                    now,
                    "successor_exists" if superseded else "lease_expired",
                    now,
                    str(row["work_id"]),
                ),
            )
            _event(
                cur,
                event_type=(
                    "work_superseded"
                    if superseded
                    else ("claim_dead_lettered" if exhausted else "claim_requeued")
                ),
                actor_id=actor_id,
                work_id=row["work_id"],
                attempt_number=int(row["attempt_count"]),
                payload={
                    "reason": "successor_exists" if superseded else "lease_expired",
                    "nextStatus": next_status,
                },
                actor_type="cluster",
            )
    return len(rows)


def claim_one(
    conn: Any,
    request: ClaimRequest,
    *,
    actor_id: str,
    now: datetime,
) -> ClaimGrant | None:
    if request.worker_id != actor_id or actor_id != EXPECTED_MAC_PRINCIPAL:
        raise WorkApiError(403, "capability_forbidden")
    if 1 not in request.supported_result_schema_versions:
        raise WorkApiError(422, "schema_version_unsupported")
    recover_expired_claims(conn, now=now)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT work_id, route_number, delivery_number, source_revision, input_hash,
                   input_hash_version, manifest, ocr_context, source_queue_path,
                   attempt_count
            FROM pcf_work_items AS candidate
            WHERE candidate.status = 'queued'
              AND candidate.required_capability = %s
              AND candidate.not_before <= %s
              AND candidate.attempt_count < candidate.max_attempts
              AND NOT EXISTS (
                  SELECT 1
                  FROM pcf_work_items AS successor
                  WHERE successor.route_number = candidate.route_number
                    AND successor.delivery_number = candidate.delivery_number
                    AND successor.source_revision COLLATE "C"
                        > candidate.source_revision COLLATE "C"
                    AND successor.status NOT IN ('superseded', 'cancelled')
              )
            ORDER BY candidate.priority DESC, candidate.not_before,
                     candidate.created_at, candidate.work_id
            FOR UPDATE OF candidate SKIP LOCKED
            LIMIT 1
            """,
            (request.required_capability, now),
        )
        row = cur.fetchone()
        if row is None:
            return None
        manifest = row["manifest"]
        pages = manifest_pages(
            manifest,
            route=str(row["route_number"]),
            delivery=str(row["delivery_number"]),
            expected_bucket=EXPECTED_BUCKET,
        )
        ocr_context = validate_pcf_ocr_context(
            row["ocr_context"],
            manifest_containers={page["containerCode"] for page in pages},
        )
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f'{row["route_number"]}:{row["delivery_number"]}',),
        )
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pcf_work_items
                WHERE route_number = %s AND delivery_number = %s
                  AND status IN ('leased', 'processing', 'effects_pending')
            ) AS active
            """,
            (row["route_number"], row["delivery_number"]),
        )
        if cur.fetchone()["active"]:
            return None
        attempt = int(row["attempt_count"]) + 1
        raw_token = secrets.token_urlsafe(32)
        deadline = now + timedelta(seconds=_deadline_seconds(manifest))
        lease = min(now + timedelta(seconds=LEASE_SECONDS), deadline)
        cur.execute(
            """
            UPDATE pcf_work_items
            SET status = 'leased', attempt_count = %s, claimed_by = %s,
                claimed_at = %s, lease_expires_at = %s,
                worker_heartbeat_at = %s, claim_token_hash = %s,
                hard_deadline_at = %s, last_error_code = NULL,
                last_error_message = NULL, updated_at = %s
            WHERE work_id = %s AND status = 'queued'
            """,
            (
                attempt,
                actor_id,
                now,
                lease,
                now,
                _token_hash(raw_token),
                deadline,
                now,
                str(row["work_id"]),
            ),
        )
        if cur.rowcount != 1:
            raise WorkApiError(409, "invalid_work_state")
        _event(
            cur,
            event_type="claim_granted",
            actor_id=actor_id,
            work_id=uuid.UUID(str(row["work_id"])),
            attempt_number=attempt,
            payload={
                "workerId": actor_id,
                "leaseExpiresAt": lease.isoformat(),
                "hardDeadlineAt": deadline.isoformat(),
            },
        )
        return ClaimGrant(
            work_id=uuid.UUID(str(row["work_id"])),
            attempt_number=attempt,
            claim_token=raw_token,
            lease_expires_at=lease,
            hard_deadline_at=deadline,
            route=str(row["route_number"]),
            delivery=str(row["delivery_number"]),
            input_hash_version=str(row["input_hash_version"]),
            input_hash=str(row["input_hash"]),
            source_queue_path=str(row["source_queue_path"]),
            manifest=manifest,
            ocr_context=ocr_context,
        )


def wait_for_claim(
    conn: Any,
    request: ClaimRequest,
    *,
    actor_id: str,
    now_fn=utc_now,
    monotonic_fn=time.monotonic,
) -> ClaimGrant | None:
    """Close the LISTEN race and keep durable rows—not notifications—authoritative."""
    grant = claim_one(conn, request, actor_id=actor_id, now=now_fn())
    if grant is not None:
        conn.commit()
        return grant
    if request.wait_seconds == 0:
        conn.commit()
        return None
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {WORK_NOTIFY_CHANNEL}")
    conn.commit()
    deadline = monotonic_fn() + request.wait_seconds
    claimed = False
    try:
        while True:
            grant = claim_one(conn, request, actor_id=actor_id, now=now_fn())
            if grant is not None:
                conn.commit()
                claimed = True
                return grant
            conn.rollback()
            remaining = deadline - monotonic_fn()
            if remaining <= 0:
                return None
            ready, _, _ = select.select([conn], [], [], remaining)
            if not ready:
                return None
            conn.poll()
            if hasattr(conn, "notifies"):
                conn.notifies.clear()
    finally:
        if not claimed:
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"UNLISTEN {WORK_NOTIFY_CHANNEL}")
        conn.commit()


def release_unmaterialized_claim(
    conn: Any,
    grant: ClaimGrant,
    *,
    actor_id: str,
    now: datetime,
    reason: str = "signed_url_unavailable",
) -> bool:
    """Revoke an unreturned claim without consuming an OCR attempt."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pcf_work_items
            SET status = 'queued', attempt_count = attempt_count - 1,
                claimed_by = NULL, claimed_at = NULL, lease_expires_at = NULL,
                worker_heartbeat_at = NULL, claim_token_hash = NULL,
                hard_deadline_at = NULL, not_before = %s,
                last_error_code = 'claim_materialization_failed',
                last_error_message = NULL, updated_at = %s
            WHERE work_id = %s AND status = 'leased'
              AND attempt_count = %s AND claim_token_hash = %s
            """,
            (
                now + timedelta(seconds=10),
                now,
                str(grant.work_id),
                grant.attempt_number,
                _token_hash(grant.claim_token),
            ),
        )
        if cur.rowcount != 1:
            return False
        _event(
            cur,
            event_type="claim_materialization_failed",
            actor_id=actor_id,
            work_id=grant.work_id,
            attempt_number=grant.attempt_number,
            payload={"reason": reason, "claimReturned": False},
        )
        return True


def mint_signed_objects(grant: ClaimGrant, signer: ClaimUrlSigner) -> tuple[SignedObject, ...]:
    pages = manifest_pages(
        grant.manifest,
        route=grant.route,
        delivery=grant.delivery,
        expected_bucket=EXPECTED_BUCKET,
    )
    expires_at = grant.hard_deadline_at + timedelta(seconds=SIGNED_URL_MARGIN_SECONDS)
    signed: list[SignedObject] = []
    for page in pages:
        object_path = str(page["storagePath"])
        generation = str(page["storageGeneration"])
        url = signer.sign_get(
            bucket=str(page["storageBucket"]),
            object_path=object_path,
            generation=generation,
            expires_at=expires_at,
        )
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "storage.googleapis.com"
            or unquote(parsed.path) != f'/{page["storageBucket"]}/{object_path}'
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or query.get("generation") != [generation]
            or "X-Goog-Signature" not in query
        ):
            raise RuntimeError("signer returned a URL outside the exact-generation contract")
        signed.append(SignedObject(object_path, generation, url, expires_at))
    return tuple(signed)


def _locked_work(conn: Any, work_id: uuid.UUID) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM pcf_work_items WHERE work_id = %s FOR UPDATE",
            (str(work_id),),
        )
        row = cur.fetchone()
    if row is None:
        raise WorkApiError(404, "work_not_found")
    return row


def _validate_attempt(row: dict[str, Any], *, attempt: int, token: str) -> None:
    if int(row["attempt_count"]) != attempt:
        raise WorkApiError(409, "stale_attempt")
    if not _matches_token(token, row.get("claim_token_hash")):
        raise WorkApiError(409, "claim_token_mismatch")


def renew_lease(
    conn: Any,
    work_id: uuid.UUID,
    *,
    attempt_number: int,
    claim_token: str,
    actor_id: str,
    now: datetime,
) -> LeaseRenewal:
    row = _locked_work(conn, work_id)
    if row.get("claimed_by") != actor_id:
        raise WorkApiError(409, "claim_token_mismatch")
    _validate_attempt(row, attempt=attempt_number, token=claim_token)
    if row["status"] not in {"leased", "processing"}:
        raise WorkApiError(409, "invalid_work_state")
    if now >= row["hard_deadline_at"]:
        raise WorkApiError(409, "hard_deadline_exceeded")
    if now >= row["lease_expires_at"]:
        raise WorkApiError(409, "lease_expired")
    lease = min(now + timedelta(seconds=LEASE_SECONDS), row["hard_deadline_at"])
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pcf_work_items
            SET status = 'processing', lease_expires_at = %s,
                worker_heartbeat_at = %s, updated_at = %s
            WHERE work_id = %s AND attempt_count = %s
              AND claim_token_hash = %s AND status IN ('leased', 'processing')
            """,
            (
                lease,
                now,
                now,
                str(work_id),
                attempt_number,
                _token_hash(claim_token),
            ),
        )
        if cur.rowcount != 1:
            raise WorkApiError(409, "invalid_work_state")
    return LeaseRenewal(lease, row["hard_deadline_at"])


def accept_result(
    conn: Any,
    work_id: uuid.UUID,
    request: ResultRequest,
    raw_request: dict[str, Any],
    *,
    actor_id: str,
    now: datetime,
) -> ResultAcceptance:
    row = _locked_work(conn, work_id)
    if request.work_id != work_id:
        raise WorkApiError(422, "result_validation_failed")
    existing = row.get("result")
    if existing is not None:
        if int(row["attempt_count"]) != request.attempt_number:
            raise WorkApiError(409, "stale_attempt")
        if not _matches_token(request.claim_token, existing.get("claimTokenHash")):
            raise WorkApiError(409, "claim_token_mismatch")
    else:
        _validate_attempt(row, attempt=request.attempt_number, token=request.claim_token)
    if request.input_hash_version != row["input_hash_version"] or request.input_hash != row["input_hash"]:
        raise WorkApiError(409, "input_hash_mismatch")
    result_hash, retained_request = canonical_result_identity(raw_request)
    if existing is not None:
        if not isinstance(existing, dict) or existing.get("resultHash") != result_hash:
            raise WorkApiError(409, "result_conflict")
        return ResultAcceptance(
            acceptance_event_id=uuid.UUID(str(existing["acceptanceEventId"])),
            accepted_at=datetime.fromisoformat(str(existing["acceptedAt"]).replace("Z", "+00:00")),
            status=str(row["status"]),
            effect_step=row.get("effect_step"),
            idempotent_replay=True,
        )
    if row["status"] not in {"leased", "processing"}:
        raise WorkApiError(409, "invalid_work_state")
    if now >= row["hard_deadline_at"]:
        raise WorkApiError(409, "hard_deadline_exceeded")
    if now >= row["lease_expires_at"]:
        raise WorkApiError(409, "lease_expired")
    completed_at = request.completed_at.astimezone(timezone.utc)
    if completed_at < row["claimed_at"] - timedelta(seconds=30) or completed_at > now + timedelta(seconds=30):
        raise WorkApiError(422, "result_validation_failed")
    if request.worker_id != actor_id or actor_id != EXPECTED_MAC_PRINCIPAL:
        raise WorkApiError(403, "capability_forbidden")
    try:
        confidence = validate_result_for_work(
            request,
            route=str(row["route_number"]),
            delivery=str(row["delivery_number"]),
            input_hash=str(row["input_hash"]),
            manifest=row["manifest"],
            ocr_context=row["ocr_context"],
            expected_bucket=EXPECTED_BUCKET,
        )
    except ContractValidationError as exc:
        raise WorkApiError(422, "result_validation_failed") from exc
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pcf_work_items
                WHERE route_number = %s AND delivery_number = %s
                  AND source_revision COLLATE "C" > %s COLLATE "C"
                  AND status NOT IN ('superseded', 'cancelled')
            )
            """,
            (row["route_number"], row["delivery_number"], row["source_revision"]),
        )
        if bool(cur.fetchone()[0]):
            cur.execute(
                """
                UPDATE pcf_work_items
                SET status = 'superseded', claimed_by = NULL, claimed_at = NULL,
                    lease_expires_at = NULL, worker_heartbeat_at = NULL,
                    claim_token_hash = NULL, hard_deadline_at = NULL,
                    completed_at = %s, updated_at = %s,
                    last_error_code = 'successor_exists', last_error_message = NULL
                WHERE work_id = %s
                """,
                (now, now, str(work_id)),
            )
            _event(
                cur,
                event_type="work_superseded",
                actor_id=actor_id,
                work_id=work_id,
                attempt_number=request.attempt_number,
                payload={"reason": "successor_exists", "resultAccepted": False},
            )
            raise WorkApiError(409, "invalid_work_state", commit=True)
        acceptance_event_id = uuid.uuid4()
        accepted_at = now
        stored_result = {
            "schemaVersion": 1,
            "resultHash": result_hash,
            "acceptanceEventId": str(acceptance_event_id),
            "acceptedAt": accepted_at.isoformat(),
            "claimTokenHash": _token_hash(request.claim_token),
            "request": retained_request,
        }
        cur.execute(
            """
            UPDATE pcf_work_items
            SET status = 'effects_pending', result = %s, confidence = %s,
                claimed_by = NULL, claimed_at = NULL, lease_expires_at = NULL,
                worker_heartbeat_at = NULL, claim_token_hash = NULL,
                hard_deadline_at = NULL, effect_step = 'projecting',
                effect_attempt_count = 0, effect_not_before = %s,
                updated_at = %s
            WHERE work_id = %s AND status IN ('leased', 'processing')
            """,
            (Json(stored_result), confidence, now, now, str(work_id)),
        )
        if cur.rowcount != 1:
            raise WorkApiError(409, "invalid_work_state")
        _event(
            cur,
            event_type="result_accepted",
            actor_id=actor_id,
            work_id=work_id,
            attempt_number=request.attempt_number,
            event_id=acceptance_event_id,
            payload={
                "resultHash": result_hash,
                "inputHashVersion": request.input_hash_version,
                "inputHash": request.input_hash,
                "confidence": confidence,
                "effectStep": "projecting",
            },
        )
    return ResultAcceptance(
        acceptance_event_id=acceptance_event_id,
        accepted_at=accepted_at,
        status="effects_pending",
        effect_step="projecting",
        idempotent_replay=False,
    )


def _backup_source_id(prefix: str, backup_id: str, sha256: str | None = None) -> str:
    suffix = f":{sha256}" if sha256 else ""
    return f"pcf-{prefix}:{backup_id}{suffix}"


def accept_backup_receipt(
    conn: Any,
    request: BackupReceiptRequest,
    raw_request: dict[str, Any],
    *,
    actor_id: str,
    now: datetime,
) -> BackupAcceptance:
    if actor_id != EXPECTED_BACKUP_PRINCIPAL:
        raise WorkApiError(403, "capability_forbidden")
    if not request.restore_inventory_verified or tuple(request.off_box_targets) != OFFBOX_TARGETS:
        raise WorkApiError(422, "backup_receipt_invalid")
    if request.verified_at > now + timedelta(seconds=30) or request.verified_at < now - timedelta(hours=24):
        raise WorkApiError(409, "backup_receipt_stale")
    if request.covers_through > request.verified_at:
        raise WorkApiError(422, "backup_receipt_invalid")
    source_id = _backup_source_id("offbox", request.backup_id, request.sha256)
    canonical = canonical_result_identity(raw_request)[1]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT event_id, payload, created_at FROM pcf_work_events WHERE source_event_id = %s",
            (source_id,),
        )
        existing = cur.fetchone()
        if existing is not None:
            if existing["payload"] != canonical:
                raise WorkApiError(409, "backup_receipt_conflict")
            return BackupAcceptance(
                uuid.UUID(str(existing["event_id"])), existing["created_at"], True
            )
        manifest_source_id = _backup_source_id("backup-manifest", request.backup_id)
        cur.execute(
            """
            SELECT payload FROM pcf_work_events
            WHERE source_event_id = %s AND event_type = 'backup_manifest_recorded'
            """,
            (manifest_source_id,),
        )
        manifest_row = cur.fetchone()
        if manifest_row is None:
            raise WorkApiError(404, "backup_manifest_not_found")
        manifest = manifest_row["payload"]
        expected = {
            "backupId": request.backup_id,
            "dumpName": request.dump_name,
            "sha256": request.sha256,
            "sizeBytes": request.size_bytes,
            "coversThrough": request.covers_through.isoformat().replace("+00:00", "Z"),
        }
        observed = {key: manifest.get(key) for key in expected} if isinstance(manifest, dict) else {}
        normalized_covers = str(observed.get("coversThrough", "")).replace("+00:00", "Z")
        observed["coversThrough"] = normalized_covers
        if observed != expected:
            raise WorkApiError(409, "backup_manifest_mismatch")
        event_id = uuid.uuid4()
        _event(
            cur,
            event_type="offbox_backup_verified",
            actor_id=actor_id,
            payload=canonical,
            source_event_id=source_id,
            event_id=event_id,
        )
    return BackupAcceptance(event_id, now, False)

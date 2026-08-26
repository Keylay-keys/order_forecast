"""Effect-disabled notification sink and pure Expo response classification.

No network client is imported here. P1/P3 use ``EffectDisabledNotificationSink``;
the eventual P5 transport must be injected behind the frozen result taxonomy.
"""

from __future__ import annotations

import re
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


SINK_CONTRACT_VERSION = "notification-sink-v1"
MAX_EXPO_MESSAGES = 100
MAX_EXPO_RECEIPT_IDS = 1000
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")


class NotificationSinkContractError(ValueError):
    """A deterministic sink input/response contract failure."""


@dataclass(frozen=True)
class DisabledSinkResult:
    disposition: str
    semantic_key: str
    payload_hash: str
    recipient_count: int
    envelope_hash: str
    scripted_disposition: str
    outbound_calls: int = 0


@dataclass(frozen=True)
class RedactedNotificationEnvelope:
    semantic_key: str
    source_revision: str
    due_at_utc: str
    attempt_id: str
    recipient_aggregate_hash: str
    title: str
    body: str
    data: Mapping[str, str]
    token_fingerprints: tuple[str, ...]
    token_count: int
    payload_hash: str


@dataclass(frozen=True)
class TicketClassification:
    disposition: str
    accepted_ticket_ids: tuple[str, ...]
    accepted_indexes: tuple[int, ...]
    rejected_indexes: tuple[int, ...]
    invalid_token_indexes: tuple[int, ...]
    provider_error_codes: tuple[str, ...]


@dataclass(frozen=True)
class ReceiptClassification:
    delivered_ticket_ids: tuple[str, ...]
    pending_ticket_ids: tuple[str, ...]
    failed_ticket_ids: tuple[str, ...]
    invalid_ticket_ids: tuple[str, ...]
    malformed: bool


@dataclass(frozen=True)
class DispatchClassification:
    disposition: str
    accepted_ticket_ids: tuple[str, ...]
    accepted_indexes: tuple[int, ...]
    rejected_indexes: tuple[int, ...]
    invalid_token_indexes: tuple[int, ...]
    provider_error_codes: tuple[str, ...]


class EffectDisabledNotificationSink:
    """Record-only sink whose API cannot carry raw push tokens."""

    mode = "effect_disabled"

    def record(
        self,
        *,
        envelope: RedactedNotificationEnvelope,
        scripted_result: TicketClassification,
    ) -> DisabledSinkResult:
        canonical = canonicalize_redacted_envelope(envelope)
        if not isinstance(scripted_result, TicketClassification):
            raise NotificationSinkContractError("scriptedResult must be a TicketClassification")
        if scripted_result.disposition == "ambiguous":
            classified_count = 0
        else:
            classified_count = len(scripted_result.accepted_indexes) + len(
                scripted_result.rejected_indexes
            )
        if classified_count not in (0, envelope.token_count):
            raise NotificationSinkContractError("scriptedResult count must match tokenCount")
        return DisabledSinkResult(
            disposition="effect_disabled",
            semantic_key=envelope.semantic_key,
            payload_hash=envelope.payload_hash,
            recipient_count=envelope.token_count,
            envelope_hash=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            scripted_disposition=scripted_result.disposition,
        )


def fingerprint_push_token(*, token: str, hmac_key: bytes) -> str:
    """Create an evidence-safe token identity without returning key/token data."""
    if not isinstance(token, str) or not token:
        raise NotificationSinkContractError("token must be non-empty")
    if not isinstance(hmac_key, bytes) or len(hmac_key) < 32:
        raise NotificationSinkContractError("hmacKey must contain at least 32 bytes")
    return hmac.new(hmac_key, token.encode("utf-8"), hashlib.sha256).hexdigest()


def canonicalize_redacted_envelope(envelope: RedactedNotificationEnvelope) -> str:
    """Validate and serialize the exact evidence-safe adapter envelope."""
    if not isinstance(envelope, RedactedNotificationEnvelope):
        raise NotificationSinkContractError("envelope must be RedactedNotificationEnvelope")
    for name, value in (
        ("semanticKey", envelope.semantic_key),
        ("sourceRevision", envelope.source_revision),
        ("attemptId", envelope.attempt_id),
        ("title", envelope.title),
        ("body", envelope.body),
    ):
        if not isinstance(value, str) or not value:
            raise NotificationSinkContractError(f"{name} must be non-empty")
    if not isinstance(envelope.due_at_utc, str) or _UTC_RE.fullmatch(envelope.due_at_utc) is None:
        raise NotificationSinkContractError("dueAtUtc must be an RFC3339 UTC timestamp")
    for name, value in (
        ("recipientAggregateHash", envelope.recipient_aggregate_hash),
        ("payloadHash", envelope.payload_hash),
    ):
        if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
            raise NotificationSinkContractError(f"{name} must be lowercase SHA-256")
    if not isinstance(envelope.data, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in envelope.data.items()
    ):
        raise NotificationSinkContractError("data must contain only string keys and values")
    if (
        isinstance(envelope.token_count, bool)
        or not isinstance(envelope.token_count, int)
        or envelope.token_count < 0
    ):
        raise NotificationSinkContractError("tokenCount must be a non-negative integer")
    if not isinstance(envelope.token_fingerprints, tuple):
        raise NotificationSinkContractError("tokenFingerprints must be an immutable tuple")
    if len(envelope.token_fingerprints) != envelope.token_count:
        raise NotificationSinkContractError("tokenFingerprints must match tokenCount")
    if any(
        not isinstance(value, str) or _HASH_RE.fullmatch(value) is None
        for value in envelope.token_fingerprints
    ) or len(set(envelope.token_fingerprints)) != len(envelope.token_fingerprints):
        raise NotificationSinkContractError("tokenFingerprints must be unique lowercase HMAC-SHA-256")
    return json.dumps(
        {
            "attemptId": envelope.attempt_id,
            "body": envelope.body,
            "data": dict(envelope.data),
            "dueAtUtc": envelope.due_at_utc,
            "payloadHash": envelope.payload_hash,
            "recipientAggregateHash": envelope.recipient_aggregate_hash,
            "semanticKey": envelope.semantic_key,
            "sourceRevision": envelope.source_revision,
            "title": envelope.title,
            "tokenCount": envelope.token_count,
            "tokenFingerprints": list(envelope.token_fingerprints),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def classify_expo_tickets(
    *, expected_count: int, response_payload: Any
) -> TicketClassification:
    """Classify a complete Expo ticket response without token material.

    A caller invokes this only after receiving and parsing a complete HTTP
    response. Timeout/reset/invalid JSON never reaches this function and is
    ``ambiguous`` at the state-machine boundary.
    """
    if isinstance(expected_count, bool) or not isinstance(expected_count, int):
        raise NotificationSinkContractError("expectedCount must be an integer")
    if not 1 <= expected_count <= MAX_EXPO_MESSAGES:
        raise NotificationSinkContractError("expectedCount must be 1 through 100")
    if not isinstance(response_payload, Mapping):
        return _ambiguous_tickets()

    data = response_payload.get("data")
    errors = response_payload.get("errors")
    if errors is not None:
        if data is not None or not isinstance(errors, Sequence) or isinstance(
            errors, (str, bytes, bytearray)
        ) or not errors:
            return _ambiguous_tickets()
        codes: list[str] = []
        for error in errors:
            if not isinstance(error, Mapping):
                return _ambiguous_tickets()
            code = error.get("code")
            message = error.get("message")
            if not isinstance(code, str) or not code or not isinstance(message, str):
                return _ambiguous_tickets()
            codes.append(code)
        return TicketClassification(
            disposition="request_rejected_zero_accepted",
            accepted_ticket_ids=(),
            accepted_indexes=(),
            rejected_indexes=tuple(range(expected_count)),
            invalid_token_indexes=(),
            provider_error_codes=tuple(codes),
        )

    if not isinstance(data, Sequence) or isinstance(data, (str, bytes, bytearray)):
        return _ambiguous_tickets()
    if len(data) != expected_count:
        return _ambiguous_tickets()

    accepted_ids: list[str] = []
    accepted_indexes: list[int] = []
    rejected_indexes: list[int] = []
    invalid_indexes: list[int] = []
    error_codes: list[str] = []
    for index, ticket in enumerate(data):
        if not isinstance(ticket, Mapping):
            return _ambiguous_tickets()
        status = ticket.get("status")
        if status == "ok":
            ticket_id = ticket.get("id")
            if not isinstance(ticket_id, str) or not ticket_id:
                return _ambiguous_tickets()
            accepted_ids.append(ticket_id)
            accepted_indexes.append(index)
            continue
        if status != "error":
            return _ambiguous_tickets()
        details = ticket.get("details")
        message = ticket.get("message")
        if not isinstance(details, Mapping) or not isinstance(message, str):
            return _ambiguous_tickets()
        error_code = details.get("error")
        if not isinstance(error_code, str) or not error_code:
            return _ambiguous_tickets()
        rejected_indexes.append(index)
        error_codes.append(error_code)
        if error_code == "DeviceNotRegistered":
            invalid_indexes.append(index)

    return TicketClassification(
        disposition="accepted" if accepted_ids else "proven_zero_accepted",
        accepted_ticket_ids=tuple(accepted_ids),
        accepted_indexes=tuple(accepted_indexes),
        rejected_indexes=tuple(rejected_indexes),
        invalid_token_indexes=tuple(invalid_indexes),
        provider_error_codes=tuple(error_codes),
    )


def _ambiguous_tickets() -> TicketClassification:
    return TicketClassification(
        disposition="ambiguous",
        accepted_ticket_ids=(),
        accepted_indexes=(),
        rejected_indexes=(),
        invalid_token_indexes=(),
        provider_error_codes=(),
    )


def combine_expo_ticket_batches(
    *,
    batches: Sequence[TicketClassification],
    batch_sizes: Sequence[int],
    transport_failure_after_batches: bool = False,
) -> DispatchClassification:
    """Combine ordered request batches into one semantic dispatch result.

    Indexes in ``TicketClassification`` are local to each request. Any unknown
    outcome after an earlier complete response makes the semantic dispatch
    terminal-ambiguous while retaining every proven ticket ID and index.
    """
    if (
        not isinstance(batches, Sequence)
        or isinstance(batches, (str, bytes, bytearray))
        or not isinstance(batch_sizes, Sequence)
        or isinstance(batch_sizes, (str, bytes, bytearray))
        or len(batches) != len(batch_sizes)
        or not batches
    ):
        raise NotificationSinkContractError("batches and batchSizes must be equal non-empty arrays")

    accepted_ids: list[str] = []
    accepted_indexes: list[int] = []
    rejected_indexes: list[int] = []
    invalid_indexes: list[int] = []
    error_codes: list[str] = []
    offset = 0
    ambiguous = bool(transport_failure_after_batches)
    request_rejected = False

    for result, batch_size in zip(batches, batch_sizes):
        if not isinstance(result, TicketClassification):
            raise NotificationSinkContractError("each batch must be a TicketClassification")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_EXPO_MESSAGES
        ):
            raise NotificationSinkContractError("each batchSize must be 1 through 100")
        indexes = result.accepted_indexes + result.rejected_indexes
        if any(index < 0 or index >= batch_size for index in indexes):
            raise NotificationSinkContractError("batch result index is outside its batch")
        if len(set(indexes)) != len(indexes):
            raise NotificationSinkContractError("batch result indexes must be unique")
        if result.disposition != "ambiguous" and set(indexes) != set(range(batch_size)):
            raise NotificationSinkContractError("complete batch result must classify every index")

        accepted_ids.extend(result.accepted_ticket_ids)
        accepted_indexes.extend(offset + index for index in result.accepted_indexes)
        rejected_indexes.extend(offset + index for index in result.rejected_indexes)
        invalid_indexes.extend(offset + index for index in result.invalid_token_indexes)
        error_codes.extend(result.provider_error_codes)
        ambiguous = ambiguous or result.disposition == "ambiguous"
        request_rejected = request_rejected or result.disposition == "request_rejected_zero_accepted"
        offset += batch_size

    if len(set(accepted_ids)) != len(accepted_ids):
        raise NotificationSinkContractError("accepted ticket IDs must be unique across batches")
    if ambiguous:
        disposition = "ambiguous"
    elif accepted_ids:
        disposition = "accepted"
    elif request_rejected:
        disposition = "request_rejected_zero_accepted"
    else:
        disposition = "proven_zero_accepted"
    return DispatchClassification(
        disposition=disposition,
        accepted_ticket_ids=tuple(accepted_ids),
        accepted_indexes=tuple(accepted_indexes),
        rejected_indexes=tuple(rejected_indexes),
        invalid_token_indexes=tuple(invalid_indexes),
        provider_error_codes=tuple(error_codes),
    )


def classify_expo_receipts(
    *, accepted_ticket_ids: Sequence[str], response_payload: Any
) -> ReceiptClassification:
    """Classify receipts by ticket ID; receipt status never authorizes resend."""
    if not isinstance(accepted_ticket_ids, Sequence) or isinstance(
        accepted_ticket_ids, (str, bytes, bytearray)
    ):
        raise NotificationSinkContractError("acceptedTicketIds must be an array")
    if not 1 <= len(accepted_ticket_ids) <= MAX_EXPO_RECEIPT_IDS:
        raise NotificationSinkContractError("acceptedTicketIds must contain 1 through 1000 IDs")
    ids: list[str] = []
    for ticket_id in accepted_ticket_ids:
        if not isinstance(ticket_id, str) or not ticket_id or ticket_id in ids:
            raise NotificationSinkContractError("acceptedTicketIds must be unique non-empty strings")
        ids.append(ticket_id)
    if not isinstance(response_payload, Mapping):
        return ReceiptClassification((), tuple(ids), (), (), True)
    data = response_payload.get("data")
    if not isinstance(data, Mapping) or response_payload.get("errors") is not None:
        return ReceiptClassification((), tuple(ids), (), (), True)

    delivered: list[str] = []
    pending: list[str] = []
    failed: list[str] = []
    invalid: list[str] = []
    malformed = bool(set(data) - set(ids))
    for ticket_id in ids:
        receipt = data.get(ticket_id)
        if receipt is None:
            pending.append(ticket_id)
            continue
        if not isinstance(receipt, Mapping):
            malformed = True
            continue
        status = receipt.get("status")
        if status == "ok":
            delivered.append(ticket_id)
            continue
        if status != "error":
            malformed = True
            continue
        details = receipt.get("details")
        if not isinstance(details, Mapping) or not isinstance(details.get("error"), str):
            malformed = True
            continue
        failed.append(ticket_id)
        if details.get("error") == "DeviceNotRegistered":
            invalid.append(ticket_id)
    return ReceiptClassification(
        delivered_ticket_ids=tuple(delivered),
        pending_ticket_ids=tuple(pending),
        failed_ticket_ids=tuple(failed),
        invalid_ticket_ids=tuple(invalid),
        malformed=malformed,
    )

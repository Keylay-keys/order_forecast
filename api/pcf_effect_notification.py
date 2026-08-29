"""Exact review alert and at-most-once PCF notification primitives.

The durable Admin alert is the human work item.  Direct Telegram delivery is a
best-effort signal with an unavoidable provider ambiguity window, so a dispatch
is recorded in PostgreSQL before the provider is called and is never repeated
after an ambiguous outcome.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from firebase_admin import firestore
from psycopg2.extras import RealDictCursor

from .pcf_effect_inputs import EffectInput
from .pcf_effect_runtime import EffectClaim
from .pcf_work_runtime import _event


NOTIFICATION_TYPE = "ocr_post_send_review"
NOTIFICATION_CHANNEL = "telegram"
NOTIFICATION_RECIPIENT = "routespark-admin"
_TERMINAL_DISPOSITIONS = frozenset(
    {"delivered", "failed_before_acceptance", "outcome_unknown"}
)
_ALERT_STATUS = frozenset({"pending", "in_progress", "resolved"})
_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]{0,255}$")


class ReviewAlertError(RuntimeError):
    """The deterministic compatibility alert is missing or conflicting."""


class NotificationDispatchError(RuntimeError):
    """Durable notification state is malformed or conflicting."""


@dataclass(frozen=True)
class ReviewAlertProof:
    path: str
    disposition: Literal["applied", "already_correct"]
    status: str
    evidence_sha256: str
    verified_at: datetime


@dataclass(frozen=True)
class NotificationEnvelope:
    notification_key: str
    channel: Literal["telegram"]
    notification_type: Literal["ocr_post_send_review"]
    recipient: Literal["routespark-admin"]
    title: str
    body: str


@dataclass(frozen=True)
class NotificationProviderResult:
    disposition: Literal[
        "delivered", "failed_before_acceptance", "outcome_unknown"
    ]
    receipt_id: str | None = None


@dataclass(frozen=True)
class NotificationDispatchProof:
    disposition: Literal[
        "delivered", "failed_before_acceptance", "outcome_unknown"
    ]
    effect_attempt: int
    provider_receipt_id: str | None
    evidence_sha256: str
    idempotent_replay: bool


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: (
            item.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(item, datetime)
            else str(item)
        ),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_alert_id(effect: EffectInput) -> str:
    if not isinstance(effect, EffectInput):
        raise ValueError("invalid PCF review alert input")
    return (
        f"ocr_post_send_review_{effect.route}_{effect.delivery}_"
        f"{effect.input_hash[:12]}"
    )


def review_alert_path(effect: EffectInput) -> str:
    return f"admin_alerts/{review_alert_id(effect)}"


def notification_key(effect: EffectInput) -> str:
    return _digest(
        {
            "channel": NOTIFICATION_CHANNEL,
            "notificationType": NOTIFICATION_TYPE,
            "recipient": NOTIFICATION_RECIPIENT,
            "workId": str(effect.work_id),
        }
    )


def notification_envelope(effect: EffectInput) -> NotificationEnvelope:
    if effect.confidence >= 92:
        raise ValueError("high-confidence PCF work does not require notification")
    return NotificationEnvelope(
        notification_key(effect),
        NOTIFICATION_CHANNEL,
        NOTIFICATION_TYPE,
        NOTIFICATION_RECIPIENT,
        "RouteSpark OCR review required",
        (
            f"Route {effect.route}, delivery {effect.delivery} was verified at "
            f"confidence {effect.confidence} and requires visual review."
        ),
    )


def _artifact_path(effect: EffectInput, role: str) -> str:
    matches = [item.relative_path for item in effect.artifacts if item.role == role]
    if len(matches) != 1:
        raise ReviewAlertError("pcf_review_alert_evidence_invalid")
    return f"{effect.evidence_run_relative_path}/{matches[0]}"


def _alert_payload(effect: EffectInput) -> dict[str, Any]:
    proposal = effect.proposal
    return {
        "type": NOTIFICATION_TYPE,
        "status": "pending",
        "routeNumber": effect.route,
        "deliveryNumber": effect.delivery,
        "loadingDate": proposal["loadingDate"],
        "identityVerification": proposal["identityVerification"],
        "confidence": effect.confidence,
        "confidenceTier": proposal["confidenceTier"],
        "dueAt": effect.accepted_at,
        "uncertainRows": list(proposal.get("uncertainRows") or ()),
        "confidenceReasons": list(proposal.get("confidenceReasons") or ()),
        "evidenceManifestPath": _artifact_path(effect, "package_manifest"),
        "beforePayloadPath": _artifact_path(effect, "before_payload"),
        "afterPayloadPath": _artifact_path(effect, "proposed_result"),
        "verificationPath": (
            f"{effect.evidence_run_relative_path}/verification/report.json"
        ),
        "inputHash": effect.input_hash,
        "agentReviewWorkId": str(effect.work_id),
        "notificationChannel": NOTIFICATION_CHANNEL,
        "notificationStatus": "pending",
        "createdAt": effect.accepted_at,
        "updatedAt": effect.accepted_at,
    }


_ALERT_IDENTITY_FIELDS = (
    "type",
    "routeNumber",
    "deliveryNumber",
    "loadingDate",
    "identityVerification",
    "confidence",
    "confidenceTier",
    "uncertainRows",
    "confidenceReasons",
    "evidenceManifestPath",
    "beforePayloadPath",
    "afterPayloadPath",
    "verificationPath",
    "inputHash",
    "agentReviewWorkId",
)


class ExactReviewAlertStore:
    """Create/read/update one deterministic Admin alert without a query."""

    def __init__(
        self,
        db: Any,
        *,
        transactional: Callable[[Callable[..., Any]], Callable[..., Any]] = firestore.transactional,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._db = db
        self._transactional = transactional
        self._clock = clock

    @staticmethod
    def _data(snapshot: Any) -> dict[str, Any]:
        if not bool(getattr(snapshot, "exists", False)):
            return {}
        value = snapshot.to_dict()
        if not isinstance(value, Mapping):
            raise ReviewAlertError("pcf_review_alert_invalid")
        return dict(value)

    @staticmethod
    def _validate(effect: EffectInput, observed: Mapping[str, Any]) -> str:
        expected = _alert_payload(effect)
        if (
            any(observed.get(field) != expected[field] for field in _ALERT_IDENTITY_FIELDS)
            or observed.get("status") not in _ALERT_STATUS
        ):
            raise ReviewAlertError("pcf_review_alert_conflict")
        return str(observed["status"])

    def ensure(self, effect: EffectInput) -> ReviewAlertProof:
        if effect.confidence >= 92:
            raise ValueError("high-confidence PCF work does not require an alert")
        path = review_alert_path(effect)
        reference = self._db.document(path)
        transaction = self._db.transaction()

        @self._transactional
        def ensure_transaction(current: Any) -> str:
            snapshot = reference.get(transaction=current)
            if snapshot.exists:
                self._validate(effect, self._data(snapshot))
                return "already_correct"
            current.set(reference, _alert_payload(effect))
            return "applied"

        disposition = ensure_transaction(transaction)
        proof = self.verify(effect)
        return ReviewAlertProof(
            path,
            disposition,
            proof.status,
            proof.evidence_sha256,
            proof.verified_at,
        )

    def verify(self, effect: EffectInput) -> ReviewAlertProof:
        path = review_alert_path(effect)
        observed = self._data(self._db.document(path).get())
        if not observed:
            raise ReviewAlertError("pcf_review_alert_missing")
        status = self._validate(effect, observed)
        verified_at = self._clock().astimezone(timezone.utc)
        evidence = {
            "identity": {field: observed.get(field) for field in _ALERT_IDENTITY_FIELDS},
            "path": path,
            "status": status,
        }
        return ReviewAlertProof(
            path,
            "already_correct",
            status,
            _digest(evidence),
            verified_at,
        )

    def record_notification(
        self,
        effect: EffectInput,
        dispatch: NotificationDispatchProof,
    ) -> ReviewAlertProof:
        if not isinstance(dispatch, NotificationDispatchProof):
            raise ValueError("invalid PCF notification proof")
        path = review_alert_path(effect)
        reference = self._db.document(path)
        observed_at = self._clock().astimezone(timezone.utc)
        transaction = self._db.transaction()

        @self._transactional
        def update_transaction(current: Any) -> None:
            snapshot = reference.get(transaction=current)
            observed = self._data(snapshot)
            self._validate(effect, observed)
            merged = dict(observed)
            merged.update(
                {
                    "notificationChannel": NOTIFICATION_CHANNEL,
                    "notificationStatus": dispatch.disposition,
                    "notificationAttemptedAt": observed_at,
                    "notificationDispatchEvidenceSha256": dispatch.evidence_sha256,
                    "updatedAt": observed_at,
                }
            )
            if dispatch.disposition == "delivered":
                merged["notificationDeliveredAt"] = observed_at
                merged.pop("notificationError", None)
            elif dispatch.disposition == "failed_before_acceptance":
                merged["notificationError"] = "provider_rejected_before_acceptance"
            else:
                merged["notificationError"] = "provider_outcome_unknown_manual_adjudication"
            if dispatch.provider_receipt_id is not None:
                merged["notificationProviderReceiptId"] = dispatch.provider_receipt_id
            current.set(reference, merged)

        update_transaction(transaction)
        return self.verify(effect)


class PcfNotificationDispatcher:
    """Append-before-dispatch ledger using the existing PCF event table."""

    def __init__(
        self,
        conn: Any,
        provider: Callable[[NotificationEnvelope], NotificationProviderResult],
        *,
        actor_id: str,
    ) -> None:
        if not callable(provider) or not isinstance(actor_id, str) or not actor_id:
            raise ValueError("invalid PCF notification dispatcher dependencies")
        self._conn = conn
        self._provider = provider
        self._actor_id = actor_id

    @staticmethod
    def _source_id(kind: str, claim: EffectClaim, effect_attempt: int) -> str:
        return f"pcf-notify-{kind}:{claim.work_id}:{effect_attempt}"

    @staticmethod
    def _common(claim: EffectClaim, effect: EffectInput) -> dict[str, Any]:
        return {
            "channel": NOTIFICATION_CHANNEL,
            "effectAttempt": claim.effect_attempt,
            "notificationKey": notification_key(effect),
            "notificationType": NOTIFICATION_TYPE,
            "recipient": NOTIFICATION_RECIPIENT,
            "workId": str(claim.work_id),
        }

    def _events(self, claim: EffectClaim) -> tuple[Mapping[str, Any], ...]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT event_id, event_type, payload, created_at
                FROM pcf_work_events
                WHERE work_id = %s
                  AND event_type IN (
                    'notification_dispatch_started',
                    'notification_dispatch_terminal'
                  )
                ORDER BY created_at, event_id
                """,
                (str(claim.work_id),),
            )
            return tuple(cur.fetchall())

    def _terminal_from_event(
        self,
        event: Mapping[str, Any],
        *,
        claim: EffectClaim,
        effect: EffectInput,
        replay: bool,
    ) -> NotificationDispatchProof:
        payload = event.get("payload")
        common = self._common(claim, effect)
        if not isinstance(payload, Mapping):
            raise NotificationDispatchError("pcf_notification_event_invalid")
        attempt = payload.get("effectAttempt")
        disposition = payload.get("disposition")
        receipt = payload.get("providerReceiptId")
        expected_common = {**common, "effectAttempt": attempt}
        if (
            type(attempt) is not int
            or attempt < 1
            or disposition not in _TERMINAL_DISPOSITIONS
            or any(payload.get(key) != value for key, value in expected_common.items())
            or (receipt is not None and (not isinstance(receipt, str) or _RECEIPT_RE.fullmatch(receipt) is None))
            or set(payload) != set(expected_common) | {"disposition", "providerReceiptId"}
        ):
            raise NotificationDispatchError("pcf_notification_event_invalid")
        core = dict(payload)
        core["terminalEventId"] = str(event.get("event_id"))
        return NotificationDispatchProof(
            disposition,
            attempt,
            receipt,
            _digest(core),
            replay,
        )

    def _append_terminal(
        self,
        claim: EffectClaim,
        effect: EffectInput,
        result: NotificationProviderResult,
        *,
        effect_attempt: int | None = None,
    ) -> NotificationDispatchProof:
        dispatch_attempt = claim.effect_attempt if effect_attempt is None else effect_attempt
        if (
            not isinstance(result, NotificationProviderResult)
            or result.disposition not in _TERMINAL_DISPOSITIONS
            or type(dispatch_attempt) is not int
            or dispatch_attempt < 1
            or dispatch_attempt > claim.effect_attempt
            or (
                result.receipt_id is not None
                and (
                    not isinstance(result.receipt_id, str)
                    or _RECEIPT_RE.fullmatch(result.receipt_id) is None
                )
            )
        ):
            raise NotificationDispatchError("pcf_notification_provider_result_invalid")
        payload = {
            **{
                **self._common(claim, effect),
                "effectAttempt": dispatch_attempt,
            },
            "disposition": result.disposition,
            "providerReceiptId": result.receipt_id,
        }
        with self._conn.cursor() as cur:
            event_id = _event(
                cur,
                event_type="notification_dispatch_terminal",
                actor_id=self._actor_id,
                actor_type="cluster",
                work_id=claim.work_id,
                attempt_number=claim.work_attempt,
                source_event_id=self._source_id("terminal", claim, dispatch_attempt),
                payload=payload,
            )
        self._conn.commit()
        return NotificationDispatchProof(
            result.disposition,
            dispatch_attempt,
            result.receipt_id,
            _digest({**payload, "terminalEventId": str(event_id)}),
            False,
        )

    def _append_start(self, claim: EffectClaim, effect: EffectInput) -> None:
        """Fence dispatch against the transaction-current effect attempt."""
        payload = self._common(claim, effect)
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT status, effect_step, effect_attempt_count
                FROM pcf_work_items
                WHERE work_id = %s
                FOR UPDATE
                """,
                (str(claim.work_id),),
            )
            current = cur.fetchone()
            if (
                current is None
                or current.get("status") != "effects_pending"
                or current.get("effect_step") != "notifying"
                or current.get("effect_attempt_count") != claim.effect_attempt
            ):
                raise NotificationDispatchError("pcf_notification_attempt_stale")
            _event(
                cur,
                event_type="notification_dispatch_started",
                actor_id=self._actor_id,
                actor_type="cluster",
                work_id=claim.work_id,
                attempt_number=claim.work_attempt,
                source_event_id=self._source_id(
                    "start", claim, claim.effect_attempt
                ),
                payload=payload,
            )
        self._conn.commit()

    def dispatch(
        self,
        claim: EffectClaim,
        effect: EffectInput,
    ) -> NotificationDispatchProof:
        if claim.work_id != effect.work_id or claim.step != "notifying":
            raise ValueError("invalid PCF notification work identity")
        envelope = notification_envelope(effect)
        events = self._events(claim)
        starts: dict[int, Mapping[str, Any]] = {}
        terminals: dict[int, Mapping[str, Any]] = {}
        for event in events:
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise NotificationDispatchError("pcf_notification_event_invalid")
            attempt = payload.get("effectAttempt")
            if type(attempt) is not int or attempt < 1:
                raise NotificationDispatchError("pcf_notification_event_invalid")
            target = starts if event.get("event_type") == "notification_dispatch_started" else terminals
            if attempt in target:
                raise NotificationDispatchError("pcf_notification_event_duplicate")
            target[attempt] = event

        common = self._common(claim, effect)
        for attempt, event in starts.items():
            payload = event["payload"]
            expected = {**common, "effectAttempt": attempt}
            if set(payload) != set(expected) or any(
                payload.get(key) != value for key, value in expected.items()
            ):
                raise NotificationDispatchError("pcf_notification_event_invalid")

        for attempt in sorted(terminals, reverse=True):
            terminal = self._terminal_from_event(
                terminals[attempt], claim=claim, effect=effect, replay=True
            )
            if terminal.disposition in {"delivered", "outcome_unknown"}:
                return terminal
            if attempt >= claim.effect_attempt:
                return terminal

        unmatched = sorted(set(starts) - set(terminals))
        if len(unmatched) > 1:
            raise NotificationDispatchError("pcf_notification_event_invalid")
        if unmatched:
            # The provider may have accepted before the process disappeared.
            # Recording ambiguity and not resending is the safe automatic action.
            return self._append_terminal(
                claim,
                effect,
                NotificationProviderResult("outcome_unknown"),
                effect_attempt=unmatched[0],
            )
        if claim.effect_attempt in terminals:
            return self._terminal_from_event(
                terminals[claim.effect_attempt], claim=claim, effect=effect, replay=True
            )

        self._append_start(claim, effect)

        try:
            result = self._provider(envelope)
        except Exception:
            # Once dispatch was attempted, an exception cannot prove that the
            # provider accepted zero messages.
            result = NotificationProviderResult("outcome_unknown")
        return self._append_terminal(claim, effect, result)

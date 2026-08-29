"""Deterministic, body-free compatibility alerts for severe projection rejects."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any

from firebase_admin import firestore
from google.api_core.exceptions import AlreadyExists


SEVERE_PROJECTION_REJECTION_REASONS = frozenset({
    "revision_conflict",
    "source_event_conflict",
    "unknown_schema_or_event_type",
})
_RECEIPT_ID_RE = re.compile(r"^projection-rejection:[0-9a-f]{64}$")


def build_projection_rejection_alert(
    *,
    receipt_source_event_id: str,
    reason: str,
    actor_id: str,
    observed_at: datetime,
) -> tuple[str, dict[str, Any]]:
    if reason not in SEVERE_PROJECTION_REJECTION_REASONS:
        raise ValueError("projection rejection is not alertable")
    if _RECEIPT_ID_RE.fullmatch(receipt_source_event_id) is None:
        raise ValueError("projection rejection receipt identity is invalid")
    if not isinstance(actor_id, str) or not actor_id or len(actor_id) > 255:
        raise ValueError("projection rejection actor is invalid")
    if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
        raise ValueError("projection rejection observed_at is invalid")
    observed = observed_at.astimezone(timezone.utc)
    receipt_sha256 = hashlib.sha256(receipt_source_event_id.encode("utf-8")).hexdigest()
    alert_id = f"pcf-projection-rejection-{receipt_sha256}"
    return alert_id, {
        "type": "pcf_projection_rejection",
        "severity": "critical",
        "status": "pending",
        "title": "PCF Projection Alert",
        "message": f"Cluster ingress rejected a PCF event: {reason}",
        "reason": reason,
        "receiptSourceEventIdSha256": receipt_sha256,
        "actorIdSha256": hashlib.sha256(actor_id.encode("utf-8")).hexdigest(),
        "source": "cluster_pcf_ingress",
        "observedAt": observed,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "notificationStatus": "pending",
    }


def publish_projection_rejection_alert(
    firestore_client: Any,
    *,
    receipt_source_event_id: str,
    reason: str,
    actor_id: str,
    observed_at: datetime,
) -> str:
    """Create one exact compatibility alert; replay is an acknowledged no-op."""
    alert_id, fields = build_projection_rejection_alert(
        receipt_source_event_id=receipt_source_event_id,
        reason=reason,
        actor_id=actor_id,
        observed_at=observed_at,
    )
    reference = firestore_client.collection("admin_alerts").document(alert_id)
    try:
        reference.create(fields)
    except AlreadyExists:
        pass
    return alert_id

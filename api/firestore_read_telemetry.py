"""Privacy-safe counters for shared Firestore reads performed by the API.

The events deliberately accept no user, route, token, path, or document
identifier. They count SDK document-get calls; billing export calibration owns
the later conversion from calls to billed reads.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import json
import logging
from typing import Iterator, Optional


logger = logging.getLogger("api.firestore_read_tax")

_SOURCES = {"feature_access", "route_access", "usage_identity"}
_OUTCOMES = {
    "access_denied",
    "cache_hit",
    "cache_miss_context",
    "cache_miss_no_route",
    "cache_miss_user_not_found",
    "enforcement_disabled",
    "entitlement_granted",
    "entitlement_required",
    "invalid_route",
    "plan_downgraded_route_locked",
    "read_error",
    "route_access_granted",
    "user_not_found",
}
_FEATURE_KEYS = {
    "forecasting",
    "managementDashboard",
    "multiRoute",
    "ordering",
    "pcfEmailImport",
    "scanner",
}


@dataclass
class FirestoreReadMeter:
    document_get_calls: int = 0


_CURRENT_METER: ContextVar[Optional[FirestoreReadMeter]] = ContextVar(
    "api_firestore_read_meter",
    default=None,
)


@contextmanager
def count_firestore_document_gets() -> Iterator[FirestoreReadMeter]:
    """Count document ``get`` calls in one authorization branch."""

    meter = FirestoreReadMeter()
    token = _CURRENT_METER.set(meter)
    try:
        yield meter
    finally:
        _CURRENT_METER.reset(token)


def metered_document_get(db, collection: str, document_id: str):
    """Perform one document get without retaining its collection or identity."""

    meter = _CURRENT_METER.get()
    if meter is not None:
        meter.document_get_calls += 1
    return db.collection(collection).document(document_id).get()


def emit_firestore_read_tax(
    *,
    source: str,
    outcome: str,
    document_get_calls: int,
    feature_key: Optional[str] = None,
) -> None:
    """Emit one bounded, identity-free measurement without affecting the request."""

    try:
        safe_source = source if source in _SOURCES else "unknown"
        safe_outcome = outcome if outcome in _OUTCOMES else "unknown"
        safe_calls = max(0, min(int(document_get_calls), 100))
        event = {
            "event": "api_firestore_shared_read_tax",
            "source": safe_source,
            "outcome": safe_outcome,
            "documentGetCalls": safe_calls,
        }
        if feature_key is not None:
            event["featureKey"] = feature_key if feature_key in _FEATURE_KEYS else "unknown"
        logger.info(json.dumps(event, sort_keys=True, separators=(",", ":")))
    except Exception:
        # Cost telemetry is subordinate to authentication and authorization.
        logger.exception("Failed to emit privacy-safe Firestore read telemetry")

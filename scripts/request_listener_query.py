"""Fail-closed Firestore query builders for transient request listeners."""

from __future__ import annotations

from google.cloud.firestore_v1.base_query import FieldFilter


_REQUEST_COLLECTIONS = frozenset({"deliveryRequests", "orderRequests"})


def pending_request_query(firestore_client, collection_name: str):
    """Return the exact pending-work query; terminal history is never watched."""
    if collection_name not in _REQUEST_COLLECTIONS:
        raise ValueError("unsupported request collection")
    return firestore_client.collection(collection_name).where(
        filter=FieldFilter("status", "==", "pending")
    )

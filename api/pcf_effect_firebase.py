"""Exact Firestore transaction and independent read-back for PCF effects."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Mapping

from firebase_admin import firestore

from .pcf_event_contract import firestore_revision, normalize_google_update_time
from .pcf_effect_inputs import EffectInput
from .pcf_effect_projection import (
    ProjectionPlanError,
    ProjectionSnapshot,
    ProjectionWriteSet,
    build_projection_write_set,
    projection_document_paths,
    validate_projection_source,
    verify_projection_documents,
)
from .pcf_effect_storage import CanonicalObjectProof


@dataclass(frozen=True)
class ProjectionWriteReceipt:
    disposition: Literal["applied", "already_correct"]
    write_set: ProjectionWriteSet
    evidence_sha256: str
    verified_at: datetime


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ExactFirestoreProjectionStore:
    """No-query adapter scoped to one frozen Route + Delivery result."""

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
    def _queue_path(effect: EffectInput) -> str:
        return f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}"

    @classmethod
    def _admission_path(cls, effect: EffectInput) -> str:
        return f"{cls._queue_path(effect)}/server/admission"

    @staticmethod
    def _data(snapshot: Any) -> dict[str, Any]:
        if snapshot is None or not bool(getattr(snapshot, "exists", False)):
            return {}
        value = snapshot.to_dict()
        if not isinstance(value, Mapping):
            raise ProjectionPlanError("pcf_projection_document_invalid")
        return dict(value)

    @staticmethod
    def _revision(snapshot: Any) -> str | None:
        if snapshot is None or not bool(getattr(snapshot, "exists", False)):
            return None
        update_time = getattr(snapshot, "update_time", None)
        if update_time is None:
            return None
        rfc3339 = getattr(update_time, "rfc3339", None)
        if callable(rfc3339):
            timestamp = normalize_google_update_time(rfc3339())
        elif isinstance(update_time, datetime) and update_time.tzinfo is not None:
            timestamp = normalize_google_update_time(
                update_time.astimezone(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            )
        else:
            raise ProjectionPlanError("pcf_projection_document_revision_invalid")
        return firestore_revision(timestamp, "upsert")

    def _read_snapshot(
        self,
        effect: EffectInput,
        *,
        transaction: Any | None,
    ) -> ProjectionSnapshot:
        parent_paths, container_paths = projection_document_paths(effect)
        queue_ref = self._db.document(self._queue_path(effect))
        admission_ref = self._db.document(self._admission_path(effect))
        parent_refs = {path: self._db.document(path) for path in parent_paths}
        container_refs = {path: self._db.document(path) for path in container_paths}

        def read(ref: Any) -> Any:
            return ref.get(transaction=transaction) if transaction is not None else ref.get()

        # Keep all transaction reads before the caller begins any writes.
        queue_snapshot = read(queue_ref)
        admission_snapshot = read(admission_ref)
        parent_snapshots = {path: read(ref) for path, ref in parent_refs.items()}
        container_snapshots = {
            path: read(ref) for path, ref in container_refs.items()
        }
        all_snapshots = {
            self._queue_path(effect): queue_snapshot,
            self._admission_path(effect): admission_snapshot,
            **parent_snapshots,
            **container_snapshots,
        }
        revisions = {
            path: revision
            for path, snapshot in all_snapshots.items()
            if (revision := self._revision(snapshot)) is not None
        }
        return ProjectionSnapshot(
            self._data(queue_snapshot),
            self._data(admission_snapshot),
            {path: self._data(snapshot) for path, snapshot in parent_snapshots.items()},
            {
                path: self._data(snapshot)
                for path, snapshot in container_snapshots.items()
            },
            revisions,
        )

    def read_exact(self, effect: EffectInput) -> ProjectionSnapshot:
        """Read only the paths derived from one accepted immutable result."""
        return self._read_snapshot(effect, transaction=None)

    def preflight(self, effect: EffectInput) -> ProjectionSnapshot:
        """Read and validate source state without writing any document."""
        snapshot = self.read_exact(effect)
        validate_projection_source(effect, snapshot)
        return snapshot

    def apply(
        self,
        effect: EffectInput,
        canonical_proofs: tuple[CanonicalObjectProof, ...],
    ) -> ProjectionWriteReceipt:
        """Read/validate/merge/write the complete exact set in one transaction."""
        if not isinstance(effect, EffectInput):
            raise ValueError("invalid PCF effect input")
        transaction = self._db.transaction()

        @self._transactional
        def apply_transaction(current: Any) -> tuple[str, ProjectionWriteSet]:
            snapshot = self._read_snapshot(effect, transaction=current)
            write_set = build_projection_write_set(
                effect,
                snapshot,
                canonical_proofs,
                now=effect.accepted_at,
            )
            try:
                verify_projection_documents(write_set, snapshot)
                return "already_correct", write_set
            except ProjectionPlanError:
                pass
            # Full transaction-fresh documents preserve every unrelated field;
            # overwrite is what proves the TTL field is absent atomically.
            for path, document in write_set.parents.items():
                current.set(self._db.document(path), document)
            for path, document in write_set.containers.items():
                current.set(self._db.document(path), document)
            current.set(self._db.document(write_set.queue_path), write_set.queue)
            return "applied", write_set

        disposition, write_set = apply_transaction(transaction)
        verified_at = self._clock().astimezone(timezone.utc)
        evidence = _digest(
            {
                "disposition": disposition,
                "projectionEvidenceSha256": write_set.evidence_sha256,
                "verifiedAt": verified_at.isoformat().replace("+00:00", "Z"),
                "workId": str(effect.work_id),
            }
        )
        return ProjectionWriteReceipt(
            disposition,
            write_set,
            evidence,
            verified_at,
        )

    def verify_exact(self, expected: ProjectionWriteSet, effect: EffectInput) -> str:
        """Perform a fresh exact read and compare cluster-owned fields."""
        if expected.work_id != effect.work_id:
            raise ValueError("PCF projection work identity mismatch")
        return verify_projection_documents(expected, self.read_exact(effect))

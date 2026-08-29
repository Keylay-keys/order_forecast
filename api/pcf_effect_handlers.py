"""Concrete fixed PCF projection, verification, cleanup, and alert handlers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .pcf_effect_firebase import ExactFirestoreProjectionStore
from .pcf_effect_inputs import (
    EVIDENCE_MOUNT_ROOT,
    EffectInput,
    load_effect_input,
    verify_external_evidence,
)
from .pcf_effect_notification import (
    ExactReviewAlertStore,
    NotificationDispatchProof,
    PcfNotificationDispatcher,
)
from .pcf_effect_projection import (
    build_projection_write_set,
    verify_projection_documents,
)
from .pcf_effect_runtime import EffectClaim, EffectProof
from .pcf_effect_storage import ExactPcfObjectStore, source_object
from .pcf_effect_summary import (
    PcfEffectSummaryStore,
    PcfEffectSummaryVerifier,
    SummaryProof,
)
from .pcf_effect_worker import EffectSkip, RetryableEffectError


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _targets(effect: EffectInput) -> dict[str, str]:
    targets: dict[str, str] = {}
    for assignment in effect.proposal["deliveryAssignments"]:
        delivery = str(assignment["deliveryNumber"])
        for raw in assignment["containerCodes"]:
            container = str(raw)
            if container in targets:
                raise RuntimeError("pcf_effect_target_duplicate")
            targets[container] = delivery
    return targets


class FixedPcfEffectHandlers:
    """Handlers with deliberately separate write and verification clients."""

    def __init__(
        self,
        *,
        firestore_writer: ExactFirestoreProjectionStore,
        firestore_verifier: ExactFirestoreProjectionStore,
        object_projector: ExactPcfObjectStore,
        object_verifier: ExactPcfObjectStore,
        object_cleaner: ExactPcfObjectStore,
        review_alert_writer: ExactReviewAlertStore,
        review_alert_verifier: ExactReviewAlertStore,
        notification_dispatcher: PcfNotificationDispatcher,
        summary_writer: PcfEffectSummaryStore,
        summary_verifier: PcfEffectSummaryVerifier,
        successor_exists: Callable[[EffectClaim], bool],
        evidence_mount: Path = EVIDENCE_MOUNT_ROOT,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if firestore_writer is firestore_verifier:
            raise ValueError("PCF projection verification requires a separate client")
        if len({id(object_projector), id(object_verifier), id(object_cleaner)}) != 3:
            raise ValueError("PCF Storage verification requires a separate client")
        if review_alert_writer is review_alert_verifier:
            raise ValueError("PCF review alert verification requires a separate client")
        if not callable(getattr(notification_dispatcher, "dispatch", None)):
            raise ValueError("invalid PCF notification dispatcher")
        if not callable(getattr(summary_writer, "apply", None)) or not callable(
            getattr(summary_verifier, "verify", None)
        ):
            raise ValueError("invalid PCF summary dependencies")
        if summary_writer.connection is summary_verifier.connection:
            raise ValueError("PCF summary verification requires a separate connection")
        if not callable(successor_exists) or not callable(clock):
            raise ValueError("invalid PCF effect handler dependencies")
        self._firestore_writer = firestore_writer
        self._firestore_verifier = firestore_verifier
        self._object_projector = object_projector
        self._object_verifier = object_verifier
        self._object_cleaner = object_cleaner
        self._review_alert_writer = review_alert_writer
        self._review_alert_verifier = review_alert_verifier
        self._notification_dispatcher = notification_dispatcher
        self._summary_writer = summary_writer
        self._summary_verifier = summary_verifier
        self._successor_exists = successor_exists
        self._evidence_mount = evidence_mount
        self._clock = clock

    @staticmethod
    def _step(claim: EffectClaim, expected: str) -> None:
        if not isinstance(claim, EffectClaim) or claim.step != expected:
            raise ValueError("PCF effect handler received the wrong step")

    def _input_and_evidence(self, claim: EffectClaim):
        effect = load_effect_input(claim)
        evidence = verify_external_evidence(
            effect,
            mount_root=self._evidence_mount,
            now=self._clock(),
        )
        return effect, evidence

    @staticmethod
    def _sources(effect: EffectInput):
        return tuple(
            source_object(dict(page), route=effect.route, delivery=effect.delivery)
            for page in effect.pages
        )

    def _prove_canonical(self, effect: EffectInput):
        targets = _targets(effect)
        return tuple(
            self._object_verifier.prove_canonical(
                source,
                target_delivery=targets[source.container],
                target_page=source.page,
            )
            for source in self._sources(effect)
        )

    def projecting(self, claim: EffectClaim) -> EffectProof:
        self._step(claim, "projecting")
        effect, evidence = self._input_and_evidence(claim)
        # Reject queue/admission drift before even a reversible canonical copy;
        # the later transaction repeats this check to close the race window.
        self._firestore_writer.preflight(effect)
        targets = _targets(effect)
        copies = tuple(
            self._object_projector.ensure_canonical_copy(
                source,
                target_delivery=targets[source.container],
                target_page=source.page,
            )
            for source in self._sources(effect)
        )
        receipt = self._firestore_writer.apply(effect, copies)
        readback = self._firestore_verifier.verify_exact(receipt.write_set, effect)
        independent_objects = self._prove_canonical(effect)
        if {
            (item.canonical_path, item.size_bytes, item.md5_hash)
            for item in copies
        } != {
            (item.canonical_path, item.size_bytes, item.md5_hash)
            for item in independent_objects
        }:
            raise RuntimeError("pcf_projection_storage_readback_mismatch")
        disposition = (
            "applied"
            if receipt.disposition == "applied"
            or any(item.disposition == "applied" for item in copies)
            else "already_correct"
        )
        verified_at = self._clock().astimezone(timezone.utc)
        return EffectProof(
            claim.work_id,
            claim.step,
            claim.effect_attempt,
            disposition,
            f"pcf-projection:{claim.work_id}:{claim.effect_attempt}",
            _digest(
                {
                    "externalEvidence": evidence.evidence_sha256,
                    "firestoreReadback": readback,
                    "objectProofs": sorted(
                        item.evidence_sha256 for item in independent_objects
                    ),
                    "projectionReceipt": receipt.evidence_sha256,
                    "workId": str(claim.work_id),
                }
            ),
            verified_at,
        )

    def verifying(self, claim: EffectClaim) -> EffectProof:
        self._step(claim, "verifying")
        effect, evidence = self._input_and_evidence(claim)
        objects = self._prove_canonical(effect)
        snapshot = self._firestore_verifier.read_exact(effect)
        expected = build_projection_write_set(
            effect,
            snapshot,
            objects,
            now=effect.accepted_at,
        )
        readback = verify_projection_documents(expected, snapshot)
        verified_at = self._clock().astimezone(timezone.utc)
        return EffectProof(
            claim.work_id,
            claim.step,
            claim.effect_attempt,
            "already_correct",
            f"pcf-verification:{claim.work_id}:{claim.effect_attempt}",
            _digest(
                {
                    "externalEvidence": evidence.evidence_sha256,
                    "firestoreReadback": readback,
                    "objectProofs": sorted(item.evidence_sha256 for item in objects),
                    "workId": str(claim.work_id),
                }
            ),
            verified_at,
        )

    def cleaning(self, claim: EffectClaim) -> EffectProof | EffectSkip:
        self._step(claim, "cleaning")
        effect, evidence = self._input_and_evidence(claim)
        objects = self._prove_canonical(effect)
        snapshot = self._firestore_verifier.read_exact(effect)
        expected = build_projection_write_set(
            effect,
            snapshot,
            objects,
            now=effect.accepted_at,
        )
        readback = verify_projection_documents(expected, snapshot)
        successor = self._successor_exists(claim)
        if type(successor) is not bool:
            raise RuntimeError("pcf_effect_successor_guard_invalid")
        if successor:
            return EffectSkip("superseded_input")
        sources = self._sources(effect)
        preflight = tuple(self._object_cleaner.preflight_source(item) for item in sources)
        if any(item.disposition == "superseded_input" for item in preflight):
            return EffectSkip("superseded_input")
        cleanup = tuple(self._object_cleaner.cleanup_source(item) for item in sources)
        if any(item.disposition == "superseded_input" for item in cleanup):
            return EffectSkip("superseded_input")
        after_objects = self._prove_canonical(effect)
        if {
            (item.canonical_path, item.size_bytes, item.md5_hash)
            for item in objects
        } != {
            (item.canonical_path, item.size_bytes, item.md5_hash)
            for item in after_objects
        }:
            raise RuntimeError("pcf_cleanup_canonical_changed")
        disposition = (
            "applied"
            if any(item.disposition == "applied" for item in cleanup)
            else "already_correct"
        )
        verified_at = self._clock().astimezone(timezone.utc)
        return EffectProof(
            claim.work_id,
            claim.step,
            claim.effect_attempt,
            disposition,
            f"pcf-cleanup:{claim.work_id}:{claim.effect_attempt}",
            _digest(
                {
                    "cleanup": sorted(item.evidence_sha256 for item in cleanup),
                    "externalEvidence": evidence.evidence_sha256,
                    "firestoreReadback": readback,
                    "objectProofs": sorted(
                        item.evidence_sha256 for item in after_objects
                    ),
                    "preflight": sorted(item.evidence_sha256 for item in preflight),
                    "workId": str(claim.work_id),
                }
            ),
            verified_at,
        )

    def notifying(self, claim: EffectClaim) -> EffectProof | EffectSkip:
        self._step(claim, "notifying")
        if claim.confidence >= 92:
            return EffectSkip("notification_not_required")
        effect, evidence = self._input_and_evidence(claim)
        objects = self._prove_canonical(effect)
        snapshot = self._firestore_verifier.read_exact(effect)
        expected = build_projection_write_set(
            effect,
            snapshot,
            objects,
            now=effect.accepted_at,
        )
        readback = verify_projection_documents(expected, snapshot)
        alert_write = self._review_alert_writer.ensure(effect)
        alert_before = self._review_alert_verifier.verify(effect)
        if alert_write.path != alert_before.path:
            raise RuntimeError("pcf_review_alert_readback_mismatch")
        dispatch = self._notification_dispatcher.dispatch(claim, effect)
        if not isinstance(dispatch, NotificationDispatchProof):
            raise RuntimeError("pcf_notification_dispatch_proof_invalid")
        self._review_alert_writer.record_notification(effect, dispatch)
        alert_after = self._review_alert_verifier.verify(effect)
        if alert_after.path != alert_before.path:
            raise RuntimeError("pcf_review_alert_readback_mismatch")
        if dispatch.disposition == "failed_before_acceptance":
            raise RetryableEffectError(
                "notification_pre_acceptance_failed",
                delay_seconds=60,
            )
        verified_at = self._clock().astimezone(timezone.utc)
        return EffectProof(
            claim.work_id,
            claim.step,
            claim.effect_attempt,
            (
                "applied"
                if alert_write.disposition == "applied" or not dispatch.idempotent_replay
                else "already_correct"
            ),
            alert_after.path,
            _digest(
                {
                    "alertBefore": alert_before.evidence_sha256,
                    "alertAfter": alert_after.evidence_sha256,
                    "canonical": sorted(item.evidence_sha256 for item in objects),
                    "dispatch": dispatch.evidence_sha256,
                    "externalEvidence": evidence.evidence_sha256,
                    "firestoreReadback": readback,
                    "workId": str(claim.work_id),
                }
            ),
            verified_at,
        )

    def summarizing(self, claim: EffectClaim) -> EffectProof:
        self._step(claim, "summarizing")
        effect, evidence = self._input_and_evidence(claim)
        objects = self._prove_canonical(effect)
        snapshot = self._firestore_verifier.read_exact(effect)
        expected = build_projection_write_set(
            effect,
            snapshot,
            objects,
            now=effect.accepted_at,
        )
        readback = verify_projection_documents(expected, snapshot)
        write = self._summary_writer.apply(effect, snapshot)
        if not isinstance(write, SummaryProof):
            raise RuntimeError("pcf_summary_write_proof_invalid")
        independent = self._summary_verifier.verify(effect, snapshot, write)
        if not isinstance(independent, SummaryProof):
            raise RuntimeError("pcf_summary_readback_proof_invalid")
        verified_at = self._clock().astimezone(timezone.utc)
        return EffectProof(
            claim.work_id,
            claim.step,
            claim.effect_attempt,
            write.disposition,
            f"pcf-summary:{claim.work_id}:{claim.effect_attempt}",
            _digest(
                {
                    "canonical": sorted(item.evidence_sha256 for item in objects),
                    "externalEvidence": evidence.evidence_sha256,
                    "firestoreReadback": readback,
                    "postgresReadback": independent.evidence_sha256,
                    "postgresWrite": write.evidence_sha256,
                    "routeRevision": independent.route_revision,
                    "summarySha256": independent.summary_sha256,
                    "workId": str(claim.work_id),
                }
            ),
            verified_at,
        )

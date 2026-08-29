"""Hermetic composition tests for projection, verification, and cleanup."""

from __future__ import annotations

import dataclasses
import copy
import tempfile
import unittest
from pathlib import Path

from order_forecast.api.pcf_effect_firebase import ExactFirestoreProjectionStore
from order_forecast.api.pcf_effect_handlers import FixedPcfEffectHandlers
from order_forecast.api.pcf_effect_inputs import EffectInputError, load_effect_input
from order_forecast.api.pcf_effect_notification import (
    ExactReviewAlertStore,
    NotificationDispatchProof,
)
from order_forecast.api.pcf_effect_runtime import EffectProof
from order_forecast.api.pcf_effect_storage import ExactPcfObjectStore, source_object
from order_forecast.api.pcf_effect_summary import SummaryProof
from order_forecast.api.pcf_effect_worker import EffectSkip, RetryableEffectError
from order_forecast.api.tests.test_pcf_effect_firebase import _Db, _docs
from order_forecast.api.tests.test_pcf_effect_inputs import (
    NOW,
    materialized_claim,
    stored_result,
)
from order_forecast.api.tests.test_pcf_effect_storage import _Blob, _Bucket


def _store(db):
    return ExactFirestoreProjectionStore(
        db,
        transactional=lambda function: function,
        clock=lambda: NOW,
    )


def _alert_store(db):
    return ExactReviewAlertStore(
        db,
        transactional=lambda function: function,
        clock=lambda: NOW,
    )


class _Dispatcher:
    def __init__(self, disposition="delivered", *, replay=False):
        self.disposition = disposition
        self.replay = replay
        self.calls = []

    def dispatch(self, claim, effect):
        self.calls.append((claim, effect))
        return NotificationDispatchProof(
            self.disposition,
            claim.effect_attempt,
            "telegram-ticket-1" if self.disposition == "delivered" else None,
            "d" * 64,
            self.replay,
        )


class _SummaryWriter:
    def __init__(self):
        self.connection = object()
        self.calls = []

    def apply(self, effect, snapshot):
        self.calls.append((effect, snapshot))
        return SummaryProof(
            "applied",
            "fsut1|2026-08-25T18:00:00.000000000Z|0",
            1,
            "a" * 64,
            "b" * 64,
            NOW,
        )


class _SummaryVerifier:
    def __init__(self):
        self.connection = object()
        self.calls = []

    def verify(self, effect, snapshot, proof):
        self.calls.append((effect, snapshot, proof))
        return dataclasses.replace(
            proof,
            disposition="already_correct",
            evidence_sha256="c" * 64,
        )


class _Harness:
    def __init__(self, root: Path, *, successor=False, confidence=96, dispatch="delivered"):
        self.claim = materialized_claim(root)
        if confidence != 96:
            request = copy.deepcopy(self.claim.result["request"])
            request["proposal"]["confidence"] = confidence
            request["proposal"]["confidenceTier"] = (
                "medium" if confidence >= 75 else "low"
            )
            self.claim = dataclasses.replace(
                self.claim,
                confidence=confidence,
                result=stored_result(request),
            )
        self.effect = load_effect_input(self.claim)
        self.writer_db = _Db(_docs(self.effect))
        self.reader_db = _Db({})
        self.reader_db.docs = self.writer_db.docs
        self.alert_writer_db = _Db({})
        self.alert_reader_db = _Db({})
        self.alert_reader_db.docs = self.alert_writer_db.docs
        self.dispatcher = _Dispatcher(dispatch)
        self.summary_writer = _SummaryWriter()
        self.summary_verifier = _SummaryVerifier()
        self.bucket = _Bucket()
        for page in self.effect.pages:
            source = source_object(
                dict(page), route=self.effect.route, delivery=self.effect.delivery
            )
            self.bucket.add(
                source.object_path,
                source.generation,
                size=source.size_bytes,
                md5_hash=source.md5_hash,
            )
        self.projector = ExactPcfObjectStore(self.bucket, clock=lambda: NOW)
        self.verifier = ExactPcfObjectStore(self.bucket, clock=lambda: NOW)
        self.cleaner = ExactPcfObjectStore(self.bucket, clock=lambda: NOW)
        self.handlers = FixedPcfEffectHandlers(
            firestore_writer=_store(self.writer_db),
            firestore_verifier=_store(self.reader_db),
            object_projector=self.projector,
            object_verifier=self.verifier,
            object_cleaner=self.cleaner,
            review_alert_writer=_alert_store(self.alert_writer_db),
            review_alert_verifier=_alert_store(self.alert_reader_db),
            notification_dispatcher=self.dispatcher,
            summary_writer=self.summary_writer,
            summary_verifier=self.summary_verifier,
            successor_exists=lambda _claim: successor,
            evidence_mount=root,
            clock=lambda: NOW,
        )


class PcfEffectHandlerTests(unittest.TestCase):
    def test_projection_verification_cleanup_sequence_is_exact_and_replayable(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            projected = harness.handlers.projecting(harness.claim)
            self.assertIsInstance(projected, EffectProof)
            self.assertEqual(projected.disposition, "applied")
            self.assertNotIn(
                "pcfQueueExpiresAt",
                harness.writer_db.docs[
                    f"pcf_processing_queue/{harness.effect.route}/queue/"
                    f"{harness.effect.delivery}"
                ],
            )

            verifying_claim = dataclasses.replace(harness.claim, step="verifying")
            verified = harness.handlers.verifying(verifying_claim)
            self.assertEqual(verified.disposition, "already_correct")

            cleaning_claim = dataclasses.replace(harness.claim, step="cleaning")
            cleaned = harness.handlers.cleaning(cleaning_claim)
            self.assertIsInstance(cleaned, EffectProof)
            self.assertEqual(cleaned.disposition, "applied")
            self.assertEqual(len(harness.bucket.delete_calls), len(harness.effect.pages))

            replay = harness.handlers.cleaning(cleaning_claim)
            self.assertIsInstance(replay, EffectProof)
            self.assertEqual(replay.disposition, "already_correct")
            self.assertEqual(len(harness.bucket.delete_calls), len(harness.effect.pages))

    def test_successor_blocks_all_cleanup_before_any_delete(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw), successor=True)
            harness.handlers.projecting(harness.claim)
            result = harness.handlers.cleaning(
                dataclasses.replace(harness.claim, step="cleaning")
            )
            self.assertEqual(result, EffectSkip("superseded_input"))
            self.assertEqual(harness.bucket.delete_calls, [])

    def test_newer_source_generation_blocks_all_cleanup_before_any_delete(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            harness.handlers.projecting(harness.claim)
            source = source_object(
                dict(harness.effect.pages[0]),
                route=harness.effect.route,
                delivery=harness.effect.delivery,
            )
            newer = _Blob(
                harness.bucket,
                source.object_path,
                source.generation + 1,
                source.size_bytes,
                source.md5_hash,
            )
            harness.bucket.objects[source.object_path] = newer
            harness.bucket.versions[(source.object_path, newer.generation)] = newer
            result = harness.handlers.cleaning(
                dataclasses.replace(harness.claim, step="cleaning")
            )
            self.assertEqual(result, EffectSkip("superseded_input"))
            self.assertEqual(harness.bucket.delete_calls, [])

    def test_evidence_tamper_blocks_projection_before_storage_or_firestore(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            harness = _Harness(root)
            artifact = harness.effect.artifacts[0]
            path = root / harness.effect.evidence_run_relative_path / artifact.relative_path
            path.write_bytes(b"tampered")
            with self.assertRaises(EffectInputError):
                harness.handlers.projecting(harness.claim)
            self.assertEqual(harness.bucket.copy_calls, [])
            self.assertEqual(harness.writer_db.writes, [])

    def test_queue_drift_blocks_projection_before_canonical_copy(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            queue_path = (
                f"pcf_processing_queue/{harness.effect.route}/queue/"
                f"{harness.effect.delivery}"
            )
            harness.writer_db.docs[queue_path]["backendProcessingStatus"] = "done"
            with self.assertRaisesRegex(Exception, "source_drift"):
                harness.handlers.projecting(harness.claim)
            self.assertEqual(harness.bucket.copy_calls, [])
            self.assertEqual(harness.writer_db.writes, [])

    def test_writer_verifier_and_storage_identities_must_be_distinct(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            writer = _store(harness.writer_db)
            with self.assertRaises(ValueError):
                FixedPcfEffectHandlers(
                    firestore_writer=writer,
                    firestore_verifier=writer,
                    object_projector=harness.projector,
                    object_verifier=harness.verifier,
                    object_cleaner=harness.cleaner,
                    review_alert_writer=_alert_store(harness.alert_writer_db),
                    review_alert_verifier=_alert_store(harness.alert_reader_db),
                    notification_dispatcher=harness.dispatcher,
                    summary_writer=harness.summary_writer,
                    summary_verifier=harness.summary_verifier,
                    successor_exists=lambda _claim: False,
                )
            with self.assertRaises(ValueError):
                FixedPcfEffectHandlers(
                    firestore_writer=_store(harness.writer_db),
                    firestore_verifier=_store(harness.reader_db),
                    object_projector=harness.projector,
                    object_verifier=harness.verifier,
                    object_cleaner=harness.projector,
                    review_alert_writer=_alert_store(harness.alert_writer_db),
                    review_alert_verifier=_alert_store(harness.alert_reader_db),
                    notification_dispatcher=harness.dispatcher,
                    summary_writer=harness.summary_writer,
                    summary_verifier=harness.summary_verifier,
                    successor_exists=lambda _claim: False,
                )

    def test_summary_requires_distinct_postgres_connections(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            harness.summary_verifier.connection = harness.summary_writer.connection
            with self.assertRaisesRegex(ValueError, "separate connection"):
                FixedPcfEffectHandlers(
                    firestore_writer=_store(harness.writer_db),
                    firestore_verifier=_store(harness.reader_db),
                    object_projector=harness.projector,
                    object_verifier=harness.verifier,
                    object_cleaner=harness.cleaner,
                    review_alert_writer=_alert_store(harness.alert_writer_db),
                    review_alert_verifier=_alert_store(harness.alert_reader_db),
                    notification_dispatcher=harness.dispatcher,
                    summary_writer=harness.summary_writer,
                    summary_verifier=harness.summary_verifier,
                    successor_exists=lambda _claim: False,
                )

    def test_high_confidence_notification_is_a_deterministic_skip(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            result = harness.handlers.notifying(
                dataclasses.replace(harness.claim, step="notifying")
            )
            self.assertEqual(result, EffectSkip("notification_not_required"))
            self.assertEqual(harness.dispatcher.calls, [])
            self.assertEqual(harness.alert_writer_db.writes, [])

    def test_low_confidence_notification_creates_alert_then_records_delivery(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw), confidence=86)
            harness.handlers.projecting(harness.claim)
            claim = dataclasses.replace(harness.claim, step="notifying")
            proof = harness.handlers.notifying(claim)
            self.assertIsInstance(proof, EffectProof)
            self.assertTrue(proof.receipt_id.startswith("admin_alerts/ocr_post_send_review_"))
            alert = harness.alert_writer_db.docs[proof.receipt_id]
            self.assertEqual(alert["notificationStatus"], "delivered")
            self.assertEqual(alert["status"], "pending")
            self.assertEqual(len(harness.dispatcher.calls), 1)

    def test_proven_pre_acceptance_failure_updates_alert_but_does_not_advance(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(
                Path(raw), confidence=86, dispatch="failed_before_acceptance"
            )
            harness.handlers.projecting(harness.claim)
            claim = dataclasses.replace(harness.claim, step="notifying")
            with self.assertRaisesRegex(
                RetryableEffectError, "notification_pre_acceptance_failed"
            ):
                harness.handlers.notifying(claim)
            alert_path = next(iter(harness.alert_writer_db.docs))
            self.assertEqual(
                harness.alert_writer_db.docs[alert_path]["notificationStatus"],
                "failed_before_acceptance",
            )

    def test_summary_reproves_firebase_then_independently_verifies_postgres(self):
        with tempfile.TemporaryDirectory() as raw:
            harness = _Harness(Path(raw))
            harness.handlers.projecting(harness.claim)
            claim = dataclasses.replace(harness.claim, step="summarizing")

            proof = harness.handlers.summarizing(claim)

            self.assertIsInstance(proof, EffectProof)
            self.assertEqual(proof.disposition, "applied")
            self.assertEqual(len(harness.summary_writer.calls), 1)
            self.assertEqual(len(harness.summary_verifier.calls), 1)
            writer_input = harness.summary_writer.calls[0]
            verifier_input = harness.summary_verifier.calls[0]
            self.assertIs(writer_input[0], verifier_input[0])
            self.assertIs(writer_input[1], verifier_input[1])
            self.assertTrue(writer_input[1].revisions)


if __name__ == "__main__":
    unittest.main()

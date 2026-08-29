#!/usr/bin/env python3
"""Exact PCF queue TTL disarm proof against the isolated Firestore emulator."""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore as gcf


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from order_forecast.api.pcf_queue_ttl import (
    QueueTtlCandidate,
    repair_bounded_exact_queue_ttls,
)
from order_forecast.api.pcf_queue_ttl_guard import run_queue_ttl_guard_cycle
from order_forecast.api.pcf_queue_ttl_arming import (
    QueueTtlArmError,
    QueueTtlArmIntent,
    QueueTtlHoldSnapshot,
    arm_exact_queue_ttl,
)


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _statement, _params):
        return None

    def fetchall(self):
        return list(self.rows)


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self, **_kwargs):
        return _Cursor(self.rows)


class QueueTtlDisarmEmulatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        host = os.environ.get("FIRESTORE_EMULATOR_HOST", "")
        project = os.environ.get("PCF_ADMISSION_EMULATOR_PROJECT", "")
        if not host:
            raise unittest.SkipTest("FIRESTORE_EMULATOR_HOST is required")
        if project != "routespark-pcf-admission-test":
            raise RuntimeError("refusing to run outside the isolated emulator project")
        cls.db = gcf.Client(project=project, credentials=AnonymousCredentials())

    def test_bounded_exact_queue_fields_are_deleted_and_other_state_is_preserved(self):
        route = "988200"
        deliveries = tuple(
            str(1_800_000_000 + (int(uuid.uuid4().hex[:8], 16) % 900_000_000))
            for _ in range(2)
        )
        candidates = tuple(
            QueueTtlCandidate(
                source_queue_path=f"pcf_processing_queue/{route}/queue/{delivery}",
                route=route,
                delivery=delivery,
            )
            for delivery in deliveries
        )
        refs = tuple(self.db.document(candidate.source_queue_path) for candidate in candidates)
        try:
            for delivery, ref in zip(deliveries, refs):
                ref.set(
                    {
                        "routeNumber": route,
                        "deliveryNumber": delivery,
                        "status": "unprocessed",
                        "pcfQueueExpiresAt": datetime(2026, 8, 30, tzinfo=timezone.utc),
                    }
                )
            proofs = repair_bounded_exact_queue_ttls(
                self.db,
                [*candidates, candidates[0]],
            )
            self.assertEqual(
                tuple(proof.source_queue_path for proof in proofs),
                tuple(candidate.source_queue_path for candidate in candidates),
            )
            for ref in refs:
                after = ref.get()
                self.assertTrue(after.exists)
                self.assertEqual(after.to_dict().get("status"), "unprocessed")
                self.assertNotIn("pcfQueueExpiresAt", after.to_dict())
        finally:
            # Exact fixture paths only; never collection, route, or recursive cleanup.
            for ref in refs:
                ref.delete()
                self.assertFalse(ref.get().exists)

    def test_full_candidate_union_repairs_work_hold_and_unresolved_alert_paths(self):
        route = "988200"
        deliveries = tuple(
            str(1_800_000_000 + (int(uuid.uuid4().hex[:8], 16) % 900_000_000))
            for _ in range(3)
        )
        paths = tuple(
            f"pcf_processing_queue/{route}/queue/{delivery}"
            for delivery in deliveries
        )
        queue_refs = tuple(self.db.document(path) for path in paths)
        hold_ref = self.db.document(f"routeEntitlementHolds/{route}/features/scanner")
        alert_refs = (
            self.db.collection("admin_alerts").document(f"ttl-main-{uuid.uuid4().hex}"),
            self.db.collection("admin_alerts").document(f"ttl-grace-{uuid.uuid4().hex}"),
            self.db.collection("admin_alerts").document(f"ttl-legacy-review-{uuid.uuid4().hex}"),
        )
        try:
            for delivery, ref in zip(deliveries, queue_refs):
                ref.set(
                    {
                        "routeNumber": route,
                        "deliveryNumber": delivery,
                        "status": "unprocessed",
                        "pcfQueueExpiresAt": datetime(2026, 8, 30, tzinfo=timezone.utc),
                    }
                )
            hold_ref.set(
                {
                    "status": "entitlement_hold",
                    "featureKey": "scanner",
                    "routeNumber": route,
                    "heldDeliveryNumber": deliveries[1],
                    "queueDocPath": paths[1],
                }
            )
            alert_refs[0].set(
                {
                    "type": "ocr_failure",
                    "status": "pending",
                    "routeNumber": route,
                    "deliveryNumber": deliveries[2],
                    "queueDocPath": paths[2],
                }
            )
            alert_refs[1].set(
                {
                    "type": "grace_capture_hold",
                    "status": "reviewed",
                    "feature": "scanner",
                    "routeNumber": route,
                    "workItemId": deliveries[1],
                    "queueDocPath": paths[1],
                }
            )
            alert_refs[2].set(
                {
                    "type": "ocr_post_send_review",
                    "status": "pending",
                    "routeNumber": route,
                    "deliveryNumber": deliveries[2],
                }
            )
            report = run_queue_ttl_guard_cycle(
                _Connection(
                    [
                        {
                            "route_number": route,
                            "delivery_number": deliveries[0],
                            "source_queue_path": paths[0],
                            "status": "queued",
                        }
                    ]
                ),
                self.db,
            )
            self.assertEqual(report.candidate_count, 3)
            self.assertEqual(report.verified_disarms, 3)
            self.assertEqual(
                (report.work_rows, report.hold_rows, report.alert_rows),
                (1, 1, 3),
            )
            for ref in queue_refs:
                self.assertNotIn("pcfQueueExpiresAt", ref.get().to_dict())
        finally:
            # Exact fixture documents only; never collection or recursive cleanup.
            for ref in (*alert_refs, hold_ref, *queue_refs):
                ref.delete()
                self.assertFalse(ref.get().exists)

    def test_verified_closure_arms_exact_timestamp_idempotently_and_fresh_hold_blocks(self):
        route = "988200"
        delivery = str(1_800_000_000 + (int(uuid.uuid4().hex[:8], 16) % 900_000_000))
        path = f"pcf_processing_queue/{route}/queue/{delivery}"
        queue_ref = self.db.document(path)
        hold_ref = self.db.document(f"routeEntitlementHolds/{route}/features/scanner")
        eligible = datetime(2026, 8, 29, 22, 0, tzinfo=timezone.utc)
        intent = QueueTtlArmIntent(
            work_id=uuid.uuid4(),
            route=route,
            delivery=delivery,
            source_queue_path=path,
            source_revision="fsut1|2026-08-29T20:00:00.000000000Z|0",
            input_hash="a" * 64,
            work_attempt=1,
            work_status="complete",
            work_completed_at=eligible - timedelta(hours=2),
            backup_event_id=uuid.uuid4(),
            backup_covers_through=eligible - timedelta(hours=1),
            effect_event_ids=tuple(uuid.uuid4() for _ in range(5)),
            review_alert_path=None,
            eligible_at=eligible,
            expires_at=eligible + timedelta(days=7),
            intent_event_id=uuid.uuid4(),
            evidence_sha256="b" * 64,
        )
        try:
            queue_ref.set(
                {
                    "routeNumber": route,
                    "deliveryNumber": delivery,
                    "pendingBackendProcessing": False,
                    "backendProcessingStatus": "done",
                    "processingStatus": "processed",
                    "status": "processed",
                    "completedAt": "2026-08-29T20:00:00Z",
                    "preserved": "yes",
                }
            )
            first = arm_exact_queue_ttl(
                self.db,
                intent,
                QueueTtlHoldSnapshot(frozenset(), 0, 0),
                now_fn=lambda: eligible,
            )
            self.assertFalse(first.idempotent_replay)
            after = queue_ref.get().to_dict()
            self.assertEqual(after["pcfQueueExpiresAt"], intent.expires_at)
            self.assertEqual(after["preserved"], "yes")
            replay = arm_exact_queue_ttl(
                self.db,
                intent,
                QueueTtlHoldSnapshot(frozenset(), 0, 0),
                now_fn=lambda: eligible + timedelta(days=1),
            )
            self.assertTrue(replay.idempotent_replay)
            self.assertEqual(queue_ref.get().get("pcfQueueExpiresAt"), intent.expires_at)

            hold_ref.set(
                {
                    "status": "entitlement_hold",
                    "featureKey": "scanner",
                    "routeNumber": route,
                    "heldDeliveryNumber": delivery,
                    "queueDocPath": path,
                }
            )
            with self.assertRaisesRegex(QueueTtlArmError, "unresolved_firestore_hold"):
                arm_exact_queue_ttl(
                    self.db,
                    intent,
                    QueueTtlHoldSnapshot(frozenset(), 0, 0),
                    now_fn=lambda: eligible + timedelta(days=1),
                )
            self.assertEqual(queue_ref.get().get("pcfQueueExpiresAt"), intent.expires_at)
        finally:
            for ref in (hold_ref, queue_ref):
                ref.delete()
                self.assertFalse(ref.get().exists)


if __name__ == "__main__":
    unittest.main()

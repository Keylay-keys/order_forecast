"""Opt-in adversarial state-machine tests on the isolated external-drive database."""

from __future__ import annotations

import copy
import grp
import importlib.util
import json
import os
import pwd
import select
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from order_forecast.api.pcf_worker_contract import (
    BackupReceiptRequest,
    ClaimRequest,
    ResultRequest,
)
from order_forecast.api.pcf_effect_runtime import (
    EFFECT_STEPS,
    EffectProof,
    advance_effect_step,
    claim_due_effect,
    schedule_effect_retry,
)
from order_forecast.api.pcf_work_runtime import (
    EXPECTED_BACKUP_PRINCIPAL,
    EXPECTED_MAC_PRINCIPAL,
    WorkApiError,
    accept_backup_receipt,
    accept_result,
    claim_one,
    release_unmaterialized_claim,
    recover_expired_claims,
    renew_lease,
    wait_for_claim,
)
from order_forecast.api.pcf_work_ingress import (
    WorkIngressConflict,
    commit_pcf_upload_event,
)
from order_forecast.api.pcf_queue_ttl_arming import (
    QueueTtlArmError,
    QueueTtlArmProof,
    load_queue_ttl_arm_candidates,
    prepare_queue_ttl_arm_intent,
    record_queue_ttl_armed,
)
from order_forecast.api.tests.test_pcf_worker_contract import (
    CONTAINER,
    DELIVERY,
    NOW,
    ROUTE,
    manifest,
    ocr_context,
    result_payload,
)
from order_forecast.scripts.pcf_work_schema_migration import apply, inspect


TEST_DSN = os.environ.get("TEST_PCF_POSTGRES_DSN", "").strip()
ROOT = Path(__file__).resolve().parents[3]


def upload_event(
    *,
    event_id="firestore-upload-event-01",
    source_revision="fsut1|2026-08-29T18:00:00.000000000Z|0",
    generation="123456789",
    automation_excluded=False,
    loading_date="08/01/2026",
):
    context = ocr_context()
    context["validatedLoadingDate"] = loading_date
    context["containers"][0]["loadingDate"] = loading_date
    value = {
        "eventId": event_id,
        "eventType": "pcf.upload.settled",
        "schemaVersion": 1,
        "route": ROUTE,
        "delivery": DELIVERY,
        "occurredAt": "2026-08-29T18:00:01Z",
        "sourceRevision": source_revision,
        "quietPeriodElapsed": True,
        "coverageScope": "uploaded_manifest",
        "manifest": manifest(),
        "ocrContext": context,
        "sourceQueuePath": f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}",
        "automationExcluded": automation_excluded,
        "fixture": (
            {"identity": "pcf-fixture-01", "generation": "fixture-generation-01"}
            if automation_excluded
            else None
        ),
    }
    value["manifest"]["objects"][0]["generation"] = generation
    return value


@unittest.skipUnless(TEST_DSN, "TEST_PCF_POSTGRES_DSN is not set")
class PcfWorkPostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = psycopg2.connect(TEST_DSN)
        try:
            result = apply(conn)
            if not result["ready"]:
                raise AssertionError(result)
        finally:
            conn.close()

    def setUp(self):
        self._truncate()

    def tearDown(self):
        self._truncate()

    @staticmethod
    def connect():
        return psycopg2.connect(
            TEST_DSN,
            options="-c statement_timeout=5000 -c lock_timeout=2000",
        )

    def _truncate(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE pcf_work_events, pcf_work_items")
            conn.commit()
        finally:
            conn.close()

    def _seed(
        self,
        *,
        route=ROUTE,
        delivery=DELIVERY,
        created_at=NOW,
        max_attempts=3,
        work_manifest=None,
        input_hash="a" * 64,
        source_revision=None,
    ):
        work_id = uuid.uuid4()
        work_manifest = copy.deepcopy(work_manifest or manifest())
        if source_revision is None:
            source_revision = (
                "fsut1|"
                + created_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
                + "000Z|0"
            )
        if route != ROUTE or delivery != DELIVERY:
            for item in work_manifest["objects"]:
                item["objectPath"] = item["objectPath"].replace(
                    f"routes/{ROUTE}/unprocessed/{DELIVERY}_",
                    f"routes/{route}/unprocessed/{delivery}_",
                )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_work_items (
                        work_id, route_number, delivery_number, source_revision, input_hash,
                        input_hash_version, status, required_capability,
                        manifest, ocr_context, source_queue_path, not_before, max_attempts,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 'pcf-work-input-v1', 'queued',
                        'macos_vision', %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        str(work_id),
                        route,
                        delivery,
                        source_revision,
                        input_hash,
                        Json(work_manifest),
                        Json(ocr_context()),
                        f"pcf_processing_queue/{route}/queue/{delivery}",
                        created_at,
                        max_attempts,
                        created_at,
                        created_at,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return work_id

    @staticmethod
    def claim_request(wait_seconds=0):
        return ClaimRequest.model_validate(
            {
                "workerId": EXPECTED_MAC_PRINCIPAL,
                "requiredCapability": "macos_vision",
                "supportedResultSchemaVersions": [1],
                "waitSeconds": wait_seconds,
            }
        )

    def _claim(self, *, now=NOW):
        conn = self.connect()
        try:
            grant = claim_one(
                conn,
                self.claim_request(),
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=now,
            )
            conn.commit()
            return grant
        finally:
            conn.close()

    def _accept_result(self, *, confidence=96, now=NOW + timedelta(seconds=60)):
        work_id = self._seed()
        grant = self._claim()
        raw = result_payload(token=grant.claim_token, work_id=work_id)
        raw["proposal"]["confidence"] = confidence
        raw["proposal"]["confidenceTier"] = "high" if confidence >= 92 else "medium"
        request = ResultRequest.model_validate(raw)
        conn = self.connect()
        try:
            accepted = accept_result(
                conn,
                work_id,
                request,
                raw,
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=now,
            )
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(accepted.status, "effects_pending")
        return work_id

    @staticmethod
    def _effect_proof(claim, *, verified_at):
        return EffectProof(
            work_id=claim.work_id,
            step=claim.step,
            effect_attempt=claim.effect_attempt,
            disposition="applied",
            receipt_id=f"integration-effect:{claim.step}:{claim.effect_attempt}",
            evidence_sha256="f" * 64,
            verified_at=verified_at,
        )

    def _record_verified_backup(self, *, suffix: str, covers: datetime, verified: datetime):
        backup_id = f"routespark-nightly-{suffix}"
        raw = {
            "backupId": backup_id,
            "dumpName": f"routespark_nightly_{suffix}.dump",
            "sha256": "e" * 64,
            "sizeBytes": 1436256,
            "coversThrough": covers.isoformat().replace("+00:00", "Z"),
            "verifiedAt": verified.isoformat().replace("+00:00", "Z"),
            "offBoxTargets": [
                "dell:/srv/routespark/backups/postgres",
                "dell:/mnt/archive/backups/postgres",
            ],
            "restoreInventoryVerified": True,
        }
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_work_events (
                        event_id, source_event_id, event_type, actor_type,
                        actor_id, payload
                    ) VALUES (%s, %s, 'backup_manifest_recorded', 'cluster', %s, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        f"pcf-backup-manifest:{backup_id}",
                        "nightly-backup-producer",
                        Json({key: raw[key] for key in (
                            "backupId", "dumpName", "sha256", "sizeBytes", "coversThrough"
                        )}),
                    ),
                )
            conn.commit()
            receipt = accept_backup_receipt(
                conn,
                BackupReceiptRequest.model_validate(raw),
                raw,
                actor_id=EXPECTED_BACKUP_PRINCIPAL,
                now=verified + timedelta(minutes=1),
            )
            conn.commit()
            return receipt
        finally:
            conn.close()

    def test_schema_trigger_and_runtime_privilege_surface_are_ready(self):
        conn = self.connect()
        try:
            state = inspect(conn)
        finally:
            conn.close()
        self.assertTrue(state["ready"])
        self.assertEqual(state["triggers"], ["pcf_work_available_notify"])
        self.assertNotIn(("pcf_work_events", "UPDATE"), state["runtimeTableGrants"])

    def test_settled_upload_atomically_creates_one_queued_work_identity(self):
        conn = self.connect()
        try:
            result = commit_pcf_upload_event(
                conn, upload_event(), actor_id="cloud-tasks-dispatcher", now_utc=NOW
            )
            conn.commit()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM pcf_work_items")
                work = cur.fetchone()
                cur.execute("SELECT * FROM pcf_work_events")
                events = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(result.disposition, "queued")
        self.assertFalse(result.duplicate_transport_event)
        self.assertEqual(uuid.UUID(str(work["work_id"])), result.work_id)
        self.assertEqual(work["source_revision"], upload_event()["sourceRevision"])
        self.assertEqual(work["status"], "queued")
        self.assertEqual(work["manifest"], upload_event()["manifest"])
        self.assertEqual(work["ocr_context"], upload_event()["ocrContext"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["source_event_id"], upload_event()["eventId"])

    def test_transport_replay_and_distinct_event_same_input_share_one_work(self):
        first = upload_event()
        second = upload_event(
            event_id="firestore-upload-event-02",
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
        )
        conn = self.connect()
        try:
            initial = commit_pcf_upload_event(conn, first, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            replay = commit_pcf_upload_event(conn, first, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            duplicate_work = commit_pcf_upload_event(
                conn, second, actor_id="dispatcher", now_utc=NOW + timedelta(minutes=1)
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), MAX(source_revision) FROM pcf_work_items")
                work_count, latest_revision = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM pcf_work_events")
                event_count = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(initial.work_id, replay.work_id)
        self.assertEqual(initial.work_id, duplicate_work.work_id)
        self.assertTrue(replay.duplicate_transport_event)
        self.assertEqual(duplicate_work.disposition, "duplicate_work")
        self.assertEqual(work_count, 1)
        self.assertEqual(latest_revision, second["sourceRevision"])
        self.assertEqual(event_count, 2)

    def test_corrected_loading_date_is_new_work_and_supersedes_image_identical_input(self):
        original = upload_event()
        corrected = upload_event(
            event_id="firestore-upload-date-corrected",
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
            loading_date="08/02/2026",
        )
        conn = self.connect()
        try:
            first = commit_pcf_upload_event(conn, original, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            second = commit_pcf_upload_event(
                conn, corrected, actor_id="dispatcher", now_utc=NOW + timedelta(minutes=1)
            )
            conn.commit()
            grant = claim_one(
                conn,
                self.claim_request(),
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=NOW + timedelta(minutes=2),
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, ocr_context->>'validatedLoadingDate' "
                    "FROM pcf_work_items ORDER BY source_revision"
                )
                states = cur.fetchall()
        finally:
            conn.close()
        self.assertNotEqual(first.input_hash, second.input_hash)
        self.assertEqual(first.disposition, "queued")
        self.assertEqual(second.disposition, "queued")
        self.assertEqual(grant.work_id, second.work_id)
        self.assertEqual(grant.ocr_context["validatedLoadingDate"], "08/02/2026")
        self.assertEqual(states, [("superseded", "08/01/2026"), ("leased", "08/02/2026")])

    def test_reused_event_or_source_revision_with_changed_input_conflicts(self):
        original = upload_event()
        changed_event = upload_event(generation="123456790")
        changed_revision = upload_event(
            event_id="firestore-upload-event-02", generation="123456790"
        )
        conn = self.connect()
        try:
            commit_pcf_upload_event(conn, original, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            with self.assertRaises(WorkIngressConflict):
                commit_pcf_upload_event(
                    conn, changed_event, actor_id="dispatcher", now_utc=NOW
                )
            conn.rollback()
            with self.assertRaises(WorkIngressConflict):
                commit_pcf_upload_event(
                    conn, changed_revision, actor_id="dispatcher", now_utc=NOW
                )
            conn.rollback()
        finally:
            conn.close()

    def test_excluded_fixture_is_audited_but_never_creates_work(self):
        conn = self.connect()
        try:
            result = commit_pcf_upload_event(
                conn,
                upload_event(automation_excluded=True),
                actor_id="dispatcher",
                now_utc=NOW,
            )
            conn.commit()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) FROM pcf_work_items")
                work_count = cur.fetchone()["count"]
                cur.execute("SELECT work_id, payload FROM pcf_work_events")
                receipt = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(result.disposition, "fixture_suppressed")
        self.assertIsNone(result.work_id)
        self.assertEqual(work_count, 0)
        self.assertIsNone(receipt["work_id"])
        self.assertIn("fixtureIdentitySha256", receipt["payload"])
        self.assertNotIn("pcf-fixture-01", str(receipt["payload"]))

    def test_source_revision_not_arrival_time_controls_successor(self):
        newer = upload_event(
            event_id="firestore-upload-newer",
            source_revision="fsut1|2026-08-29T18:02:00.000000000Z|0",
            generation="123456790",
        )
        older = upload_event(
            event_id="firestore-upload-older",
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
        )
        conn = self.connect()
        try:
            newest = commit_pcf_upload_event(conn, newer, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            stale = commit_pcf_upload_event(
                conn, older, actor_id="dispatcher", now_utc=NOW + timedelta(minutes=1)
            )
            conn.commit()
            grant = claim_one(
                conn, self.claim_request(), actor_id=EXPECTED_MAC_PRINCIPAL,
                now=NOW + timedelta(minutes=2),
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT source_revision, status FROM pcf_work_items ORDER BY source_revision")
                states = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(stale.disposition, "stale_superseded")
        self.assertEqual(grant.work_id, newest.work_id)
        self.assertEqual(states, [(older["sourceRevision"], "superseded"), (newer["sourceRevision"], "leased")])

    def test_newer_arrival_supersedes_only_the_older_queued_work(self):
        older = upload_event(
            event_id="firestore-upload-older",
            source_revision="fsut1|2026-08-29T18:00:00.000000000Z|0",
        )
        newer = upload_event(
            event_id="firestore-upload-newer",
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
            generation="123456790",
        )
        conn = self.connect()
        try:
            prior = commit_pcf_upload_event(conn, older, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            successor = commit_pcf_upload_event(
                conn, newer, actor_id="dispatcher", now_utc=NOW + timedelta(minutes=1)
            )
            conn.commit()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT work_id, status FROM pcf_work_items")
                states = {str(row["work_id"]): row["status"] for row in cur.fetchall()}
                cur.execute(
                    "SELECT payload FROM pcf_work_events "
                    "WHERE event_type = 'work_superseded' AND work_id = %s",
                    (str(prior.work_id),),
                )
                superseded_event = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(states[str(prior.work_id)], "superseded")
        self.assertEqual(states[str(successor.work_id)], "queued")
        self.assertEqual(
            superseded_event["payload"]["successorSourceRevision"],
            newer["sourceRevision"],
        )

    def test_concurrent_transport_replay_creates_one_work_and_one_receipt(self):
        event = upload_event()
        barrier = threading.Barrier(2)

        def ingest_after_barrier():
            conn = self.connect()
            try:
                barrier.wait(timeout=2)
                result = commit_pcf_upload_event(
                    conn, event, actor_id="dispatcher", now_utc=NOW
                )
                conn.commit()
                return result
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: ingest_after_barrier(), range(2)))
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pcf_work_items")
                work_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM pcf_work_events")
                event_count = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual({result.work_id for result in results}, {results[0].work_id})
        self.assertEqual(
            sorted(result.duplicate_transport_event for result in results),
            [False, True],
        )
        self.assertEqual((work_count, event_count), (1, 1))

    def test_expired_predecessor_with_newer_input_is_superseded_not_requeued(self):
        older = upload_event(
            event_id="firestore-upload-older",
            source_revision="fsut1|2026-08-29T18:00:00.000000000Z|0",
        )
        newer = upload_event(
            event_id="firestore-upload-newer",
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
            generation="123456790",
        )
        conn = self.connect()
        try:
            prior = commit_pcf_upload_event(conn, older, actor_id="dispatcher", now_utc=NOW)
            conn.commit()
            grant = claim_one(
                conn, self.claim_request(), actor_id=EXPECTED_MAC_PRINCIPAL, now=NOW
            )
            conn.commit()
            successor = commit_pcf_upload_event(
                conn, newer, actor_id="dispatcher", now_utc=NOW + timedelta(seconds=1)
            )
            conn.commit()
            recovered = recover_expired_claims(conn, now=NOW + timedelta(seconds=121))
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT work_id, status FROM pcf_work_items")
                states = dict(cur.fetchall())
        finally:
            conn.close()
        self.assertEqual(grant.work_id, prior.work_id)
        self.assertEqual(recovered, 1)
        self.assertEqual(states[str(prior.work_id)], "superseded")
        self.assertEqual(states[str(successor.work_id)], "queued")

    def test_claim_heartbeat_accept_and_exact_replay_never_store_raw_token(self):
        work_id = self._seed()
        grant = self._claim()
        self.assertEqual(grant.work_id, work_id)
        self.assertEqual(grant.attempt_number, 1)
        conn = self.connect()
        try:
            renewal = renew_lease(
                conn,
                work_id,
                attempt_number=1,
                claim_token=grant.claim_token,
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=NOW + timedelta(seconds=30),
            )
            conn.commit()
            self.assertEqual(renewal.lease_expires_at, NOW + timedelta(seconds=150))
            raw = result_payload(token=grant.claim_token, work_id=work_id)
            request = ResultRequest.model_validate(raw)
            accepted = accept_result(
                conn,
                work_id,
                request,
                raw,
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=NOW + timedelta(seconds=60),
            )
            conn.commit()
            replay = accept_result(
                conn,
                work_id,
                request,
                raw,
                actor_id=EXPECTED_MAC_PRINCIPAL,
                now=NOW + timedelta(minutes=30),
            )
            conn.commit()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT status, effect_step, claim_token_hash, result::text AS result_text "
                    "FROM pcf_work_items WHERE work_id = %s",
                    (str(work_id),),
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT payload::text AS payload_text FROM pcf_work_events "
                    "WHERE work_id = %s",
                    (str(work_id),),
                )
                event_payloads = [value["payload_text"] for value in cur.fetchall()]
        finally:
            conn.close()
        self.assertFalse(accepted.idempotent_replay)
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(accepted.acceptance_event_id, replay.acceptance_event_id)
        self.assertEqual((row["status"], row["effect_step"]), ("effects_pending", "projecting"))
        self.assertIsNone(row["claim_token_hash"])
        self.assertNotIn(grant.claim_token, row["result_text"])
        self.assertTrue(all(grant.claim_token not in payload for payload in event_payloads))

    def test_changed_replay_is_result_conflict_even_after_acceptance(self):
        work_id = self._seed()
        grant = self._claim()
        raw = result_payload(token=grant.claim_token, work_id=work_id)
        conn = self.connect()
        try:
            accept_result(
                conn, work_id, ResultRequest.model_validate(raw), raw,
                actor_id=EXPECTED_MAC_PRINCIPAL, now=NOW + timedelta(seconds=60),
            )
            conn.commit()
            changed = copy.deepcopy(raw)
            changed["proposal"]["confidenceReasons"].append("Changed after acceptance.")
            with self.assertRaises(WorkApiError) as raised:
                accept_result(
                    conn, work_id, ResultRequest.model_validate(changed), changed,
                    actor_id=EXPECTED_MAC_PRINCIPAL, now=NOW + timedelta(seconds=61),
                )
            conn.rollback()
        finally:
            conn.close()
        self.assertEqual(raised.exception.code, "result_conflict")

    def test_effect_claim_uses_skip_locked_under_two_worker_contention(self):
        work_id = self._accept_result()
        first_conn = self.connect()
        second_conn = self.connect()
        try:
            first = claim_due_effect(
                first_conn,
                actor_id="pcf-effect-worker-01",
                now=NOW + timedelta(seconds=61),
                visibility_seconds=30,
            )
            self.assertIsNotNone(first)
            second = claim_due_effect(
                second_conn,
                actor_id="pcf-effect-worker-02",
                now=NOW + timedelta(seconds=61),
                visibility_seconds=30,
            )
            self.assertIsNone(second)
            second_conn.commit()
            first_conn.commit()
            with first_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT effect_step, effect_attempt_count, effect_not_before "
                    "FROM pcf_work_items WHERE work_id = %s",
                    (str(work_id),),
                )
                row = cur.fetchone()
        finally:
            first_conn.close()
            second_conn.close()
        self.assertEqual(row["effect_step"], "projecting")
        self.assertEqual(row["effect_attempt_count"], 1)
        self.assertEqual(row["effect_not_before"], NOW + timedelta(seconds=91))

    def test_effect_claim_exact_allowlist_skips_an_earlier_unlisted_row(self):
        first_id = self._seed(delivery="1806248060", created_at=NOW)
        second_id = self._seed(
            delivery="1806248061",
            created_at=NOW + timedelta(seconds=1),
            input_hash="b" * 64,
        )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pcf_work_items
                    SET status = 'effects_pending', attempt_count = 1,
                        result = %s, confidence = 96,
                        effect_step = 'projecting', effect_not_before = %s,
                        updated_at = %s
                    WHERE work_id = ANY(%s::uuid[])
                    """,
                    (
                        Json({"request": {"proposal": {"confidence": 96}}}),
                        NOW + timedelta(seconds=2),
                        NOW + timedelta(seconds=2),
                        [str(first_id), str(second_id)],
                    ),
                )
            conn.commit()
            claimed = claim_due_effect(
                conn,
                actor_id="pcf-effect-worker-01",
                now=NOW + timedelta(seconds=3),
                visibility_seconds=30,
                admitted_work_ids=(second_id,),
            )
            conn.commit()
        finally:
            conn.close()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.work_id, second_id)

    def test_listen_notify_wakes_when_result_enters_effects_pending(self):
        work_id = self._seed()
        listener = self.connect()
        writer = self.connect()
        try:
            with listener.cursor() as cur:
                cur.execute("LISTEN pcf_work_available")
            listener.commit()
            with writer.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pcf_work_items
                    SET status = 'effects_pending', attempt_count = 1,
                        result = %s, confidence = 96,
                        effect_step = 'projecting', effect_not_before = %s,
                        updated_at = %s
                    WHERE work_id = %s
                    """,
                    (
                        Json({"request": {"proposal": {"confidence": 96}}}),
                        NOW + timedelta(seconds=1),
                        NOW + timedelta(seconds=1),
                        str(work_id),
                    ),
                )
            writer.commit()
            ready, _, _ = select.select([listener], [], [], 2)
            self.assertTrue(ready)
            listener.poll()
            self.assertIn(str(work_id), [item.payload for item in listener.notifies])
        finally:
            listener.close()
            writer.close()

    def test_effect_visibility_reclaim_fences_crashed_worker_and_resets_per_step(self):
        work_id = self._accept_result()
        first_conn = self.connect()
        try:
            first = claim_due_effect(
                first_conn,
                actor_id="pcf-effect-worker-01",
                now=NOW + timedelta(seconds=61),
                visibility_seconds=30,
            )
            first_conn.commit()
        finally:
            first_conn.close()
        self.assertIsNotNone(first)
        assert first is not None

        second_conn = self.connect()
        try:
            second = claim_due_effect(
                second_conn,
                actor_id="pcf-effect-worker-02",
                now=NOW + timedelta(seconds=92),
                visibility_seconds=30,
            )
            second_conn.commit()
        finally:
            second_conn.close()
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.effect_attempt, 2)

        stale_conn = self.connect()
        try:
            with self.assertRaises(WorkApiError) as raised:
                advance_effect_step(
                    stale_conn,
                    first,
                    actor_id="pcf-effect-worker-01",
                    now=NOW + timedelta(seconds=93),
                    proof=self._effect_proof(
                        first, verified_at=NOW + timedelta(seconds=93)
                    ),
                )
            stale_conn.rollback()
        finally:
            stale_conn.close()
        self.assertEqual(raised.exception.code, "stale_effect_attempt")

        winner_conn = self.connect()
        try:
            advanced = advance_effect_step(
                winner_conn,
                second,
                actor_id="pcf-effect-worker-02",
                now=NOW + timedelta(seconds=93),
                proof=self._effect_proof(
                    second, verified_at=NOW + timedelta(seconds=93)
                ),
            )
            winner_conn.commit()
            with winner_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT status, effect_step, effect_attempt_count, effect_not_before "
                    "FROM pcf_work_items WHERE work_id = %s",
                    (str(work_id),),
                )
                row = cur.fetchone()
        finally:
            winner_conn.close()
        self.assertEqual(advanced.next_step, "verifying")
        self.assertEqual(
            (row["status"], row["effect_step"], row["effect_attempt_count"]),
            ("effects_pending", "verifying", 0),
        )
        self.assertEqual(row["effect_not_before"], NOW + timedelta(seconds=93))

    def test_fixed_effect_sequence_reaches_only_confidence_selected_terminal(self):
        for confidence, expected_status in ((92, "complete"), (91, "review_scheduled")):
            with self.subTest(confidence=confidence):
                self._truncate()
                work_id = self._accept_result(confidence=confidence)
                observed_steps = []
                for offset, expected_step in enumerate(EFFECT_STEPS, start=61):
                    conn = self.connect()
                    try:
                        claim = claim_due_effect(
                            conn,
                            actor_id="pcf-effect-worker-01",
                            now=NOW + timedelta(seconds=offset),
                            visibility_seconds=30,
                        )
                        conn.commit()
                        self.assertIsNotNone(claim)
                        assert claim is not None
                        observed_steps.append(claim.step)
                        if claim.step == "notifying" and confidence >= 92:
                            advance_effect_step(
                                conn,
                                claim,
                                actor_id="pcf-effect-worker-01",
                                now=NOW + timedelta(seconds=offset),
                                outcome="skipped",
                                reason="notification_not_required",
                            )
                        else:
                            advance_effect_step(
                                conn,
                                claim,
                                actor_id="pcf-effect-worker-01",
                                now=NOW + timedelta(seconds=offset),
                                proof=self._effect_proof(
                                    claim,
                                    verified_at=NOW + timedelta(seconds=offset),
                                ),
                            )
                        conn.commit()
                    finally:
                        conn.close()
                check = self.connect()
                try:
                    with check.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT status, effect_step, effect_attempt_count, completed_at "
                            "FROM pcf_work_items WHERE work_id = %s",
                            (str(work_id),),
                        )
                        row = cur.fetchone()
                finally:
                    check.close()
                self.assertEqual(tuple(observed_steps), EFFECT_STEPS)
                self.assertEqual(row["status"], expected_status)
                self.assertIsNone(row["effect_step"])
                self.assertEqual(row["effect_attempt_count"], 0)
                self.assertIsNotNone(row["completed_at"])

    def test_effect_attempt_budget_dead_letters_only_after_current_step_budget(self):
        work_id = self._accept_result()
        for number in range(1, 4):
            conn = self.connect()
            try:
                claim = claim_due_effect(
                    conn,
                    actor_id="pcf-effect-worker-01",
                    now=NOW + timedelta(seconds=60 + number * 31),
                    visibility_seconds=30,
                )
                conn.commit()
                self.assertIsNotNone(claim)
                assert claim is not None
                self.assertEqual(claim.effect_attempt, number)
                schedule_effect_retry(
                    conn,
                    claim,
                    actor_id="pcf-effect-worker-01",
                    now=NOW + timedelta(seconds=60 + number * 31),
                    delay_seconds=5,
                    error_code="firebase_unavailable",
                )
                conn.commit()
            finally:
                conn.close()

        terminal_conn = self.connect()
        try:
            claim = claim_due_effect(
                terminal_conn,
                actor_id="pcf-effect-worker-01",
                now=NOW + timedelta(seconds=60 + 4 * 31),
                visibility_seconds=30,
            )
            terminal_conn.commit()
            self.assertIsNone(claim)
            with terminal_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT status, effect_step, effect_attempt_count, last_error_code "
                    "FROM pcf_work_items WHERE work_id = %s",
                    (str(work_id),),
                )
                row = cur.fetchone()
        finally:
            terminal_conn.close()
        self.assertEqual(
            (
                row["status"],
                row["effect_step"],
                row["effect_attempt_count"],
                row["last_error_code"],
            ),
            ("dead_letter", None, 0, "effect_attempts_exhausted"),
        )

    def test_ttl_intent_requires_full_effect_proofs_and_current_covering_backup(self):
        for confidence, expected_status in ((96, "complete"), (91, "review_scheduled")):
            with self.subTest(confidence=confidence):
                self._truncate()
                work_id = self._accept_result(confidence=confidence)
                preflight = self.connect()
                try:
                    with self.assertRaises(QueueTtlArmError) as incomplete:
                        prepare_queue_ttl_arm_intent(
                            preflight,
                            work_id,
                            actor_id="pcf-queue-ttl-worker",
                            now=NOW + timedelta(minutes=2),
                        )
                    preflight.rollback()
                finally:
                    preflight.close()
                self.assertEqual(
                    str(incomplete.exception),
                    "pcf_queue_ttl_work_not_closed",
                )

                for offset, _expected_step in enumerate(EFFECT_STEPS, start=61):
                    conn = self.connect()
                    try:
                        claim = claim_due_effect(
                            conn,
                            actor_id="pcf-effect-worker-01",
                            now=NOW + timedelta(seconds=offset),
                            visibility_seconds=30,
                        )
                        conn.commit()
                        self.assertIsNotNone(claim)
                        assert claim is not None
                        if claim.step == "notifying" and confidence >= 92:
                            advance_effect_step(
                                conn,
                                claim,
                                actor_id="pcf-effect-worker-01",
                                now=NOW + timedelta(seconds=offset),
                                outcome="skipped",
                                reason="notification_not_required",
                            )
                        else:
                            proof = self._effect_proof(
                                claim,
                                verified_at=NOW + timedelta(seconds=offset),
                            )
                            if claim.step == "notifying" and confidence < 92:
                                proof = EffectProof(
                                    work_id=claim.work_id,
                                    step=claim.step,
                                    effect_attempt=claim.effect_attempt,
                                    disposition="applied",
                                    receipt_id="admin_alerts/ocr-review-988200-1806248060",
                                    evidence_sha256=proof.evidence_sha256,
                                    verified_at=proof.verified_at,
                                )
                            advance_effect_step(
                                conn,
                                claim,
                                actor_id="pcf-effect-worker-01",
                                now=NOW + timedelta(seconds=offset),
                                proof=proof,
                            )
                        conn.commit()
                    finally:
                        conn.close()

                covers = NOW + timedelta(hours=2)
                verified = NOW + timedelta(hours=2, minutes=10)
                self._record_verified_backup(
                    suffix=f"20260829-{confidence}",
                    covers=covers,
                    verified=verified,
                )
                arm_conn = self.connect()
                try:
                    prepared = prepare_queue_ttl_arm_intent(
                        arm_conn,
                        work_id,
                        actor_id="pcf-queue-ttl-worker",
                        now=verified + timedelta(minutes=2),
                    )
                    arm_conn.commit()
                    replay = prepare_queue_ttl_arm_intent(
                        arm_conn,
                        work_id,
                        actor_id="pcf-queue-ttl-worker",
                        now=verified + timedelta(hours=26),
                    )
                    arm_conn.commit()
                    arm_proof = QueueTtlArmProof(
                        source_queue_path=prepared.source_queue_path,
                        intent_event_id=prepared.intent_event_id,
                        expires_at=prepared.expires_at,
                        verified_at=verified + timedelta(minutes=2),
                        idempotent_replay=False,
                    )
                    receipt = record_queue_ttl_armed(
                        arm_conn,
                        prepared,
                        arm_proof,
                        actor_id="pcf-queue-ttl-worker",
                        now=verified + timedelta(minutes=4),
                    )
                    arm_conn.commit()
                    receipt_replay = record_queue_ttl_armed(
                        arm_conn,
                        prepared,
                        QueueTtlArmProof(
                            source_queue_path=prepared.source_queue_path,
                            intent_event_id=prepared.intent_event_id,
                            expires_at=prepared.expires_at,
                            verified_at=verified + timedelta(hours=26, minutes=1),
                            idempotent_replay=True,
                        ),
                        actor_id="pcf-queue-ttl-worker",
                        now=verified + timedelta(hours=26, minutes=2),
                    )
                    arm_conn.commit()
                finally:
                    arm_conn.close()
                self.assertEqual(prepared.intent_event_id, replay.intent_event_id)
                self.assertEqual(prepared.expires_at, replay.expires_at)
                self.assertEqual(prepared.work_status, expected_status)
                self.assertEqual(prepared.backup_covers_through, covers)
                self.assertEqual(len(prepared.effect_event_ids), 5)
                self.assertFalse(receipt.idempotent_replay)
                self.assertTrue(receipt_replay.idempotent_replay)
                self.assertEqual(receipt.event_id, receipt_replay.event_id)
                self.assertEqual(
                    prepared.review_alert_path,
                    (
                        "admin_alerts/ocr-review-988200-1806248060"
                        if confidence < 92
                        else None
                    ),
                )

    def test_ttl_candidates_exclude_terminal_work_shadowed_by_newer_revision(self):
        older = self._seed(
            input_hash="1" * 64,
            source_revision="fsut1|2026-08-29T18:00:00.000000000Z|0",
        )
        newer = self._seed(
            created_at=NOW + timedelta(minutes=1),
            input_hash="2" * 64,
            source_revision="fsut1|2026-08-29T18:01:00.000000000Z|0",
        )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pcf_work_items
                    SET status = 'complete', completed_at = created_at,
                        updated_at = created_at
                    WHERE work_id IN (%s, %s)
                    """,
                    (str(older), str(newer)),
                )
            conn.commit()
            candidates = load_queue_ttl_arm_candidates(conn)
        finally:
            conn.close()

        self.assertEqual(tuple(item.work_id for item in candidates), (newer,))

    def test_nightly_backup_records_exact_database_watermarked_manifest(self):
        script = ROOT / "deploy/runtime/routespark-postgres-nightly-backup.sh"
        mount = Path("/Volumes/RouteSparkPostgresTests")
        self.assertTrue(mount.is_mount())
        with tempfile.TemporaryDirectory(dir=mount) as raw_temp:
            test_root = Path(raw_temp)
            backup_dir = test_root / "backups"
            fake_bin = test_root / "bin"
            backup_dir.mkdir()
            fake_bin.mkdir()

            def command(name: str, body: str) -> None:
                path = fake_bin / name
                path.write_text(
                    "#!/bin/bash\nset -euo pipefail\n" + body,
                    encoding="utf-8",
                )
                path.chmod(path.stat().st_mode | stat.S_IXUSR)

            command(
                "sudo",
                '[[ "$1" == "-u" && "$2" == "postgres" ]]\n'
                'shift 2\nexec "$@"\n',
            )
            command("flock", "exit 0\n")
            command(
                "stat",
                '[[ "${1:-}" == "-c" && "${2:-}" == "%s" ]]\n'
                '/usr/bin/wc -c < "$3" | /usr/bin/tr -d " "\n',
            )

            dsn = psycopg2.extensions.parse_dsn(TEST_DSN)
            env = dict(os.environ)
            env.update(
                {
                    "PATH": f"{fake_bin}:{env['PATH']}",
                    "PGHOST": dsn["host"],
                    "PGPORT": dsn["port"],
                    "PGUSER": dsn["user"],
                    "ROUTESPARK_BACKUP_DIR": str(backup_dir),
                    "ROUTESPARK_BACKUP_DB_NAME": dsn["dbname"],
                    "ROUTESPARK_BACKUP_LOCK_FILE": str(test_root / "backup.lock"),
                    "ROUTESPARK_BACKUP_OWNER_USER": pwd.getpwuid(os.getuid()).pw_name,
                    "ROUTESPARK_BACKUP_OWNER_GROUP": grp.getgrgid(os.getgid()).gr_name,
                }
            )
            completed = subprocess.run(
                ["bash", str(script)],
                check=False,
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

            manifests = list(backup_dir.glob("routespark_nightly_*.dump.manifest.json"))
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            dump_path = backup_dir / manifest["dumpName"]

            second_target = test_root / "second-target" / manifest["dumpName"]
            second_target.parent.mkdir()
            shutil.copyfile(dump_path, second_target)
            builder_path = ROOT / "deploy/runtime/build-pcf-offbox-backup-receipt.py"
            specification = importlib.util.spec_from_file_location(
                "external_pcf_offbox_builder", builder_path
            )
            assert specification is not None and specification.loader is not None
            builder = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(builder)
            verified_receipt = builder.verify_and_freeze_receipt(
                manifests[0],
                dump_path,
                second_target,
                now=datetime.now(timezone.utc),
                pg_restore=shutil.which("pg_restore") or "pg_restore",
            )
            self.assertEqual(verified_receipt["backupId"], manifest["backupId"])
            self.assertEqual(verified_receipt["coversThrough"], manifest["coversThrough"])

            conn = self.connect()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT event_type, actor_type, actor_id, payload
                        FROM pcf_work_events
                        WHERE source_event_id = %s
                        """,
                        (f"pcf-backup-manifest:{manifest['backupId']}",),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()

            self.assertEqual(
                (row["event_type"], row["actor_type"], row["actor_id"]),
                ("backup_manifest_recorded", "cluster", "nightly-backup-producer"),
            )
            self.assertEqual(row["payload"], manifest)

    def test_expired_attempt_requeues_then_old_attempt_is_stale(self):
        work_id = self._seed(max_attempts=3)
        first = self._claim()
        second = self._claim(now=NOW + timedelta(seconds=121))
        self.assertEqual(second.work_id, work_id)
        self.assertEqual(second.attempt_number, 2)
        conn = self.connect()
        try:
            with self.assertRaises(WorkApiError) as raised:
                renew_lease(
                    conn,
                    work_id,
                    attempt_number=first.attempt_number,
                    claim_token=first.claim_token,
                    actor_id=EXPECTED_MAC_PRINCIPAL,
                    now=NOW + timedelta(seconds=122),
                )
            conn.rollback()
        finally:
            conn.close()
        self.assertEqual(raised.exception.code, "stale_attempt")

    def test_heartbeat_conflicts_distinguish_attempt_token_lease_and_deadline(self):
        work_id = self._seed()
        grant = self._claim()
        conn = self.connect()
        try:
            cases = (
                (2, grant.claim_token, NOW + timedelta(seconds=1), "stale_attempt"),
                (1, "x" * 43, NOW + timedelta(seconds=1), "claim_token_mismatch"),
                (1, grant.claim_token, NOW + timedelta(seconds=120), "lease_expired"),
                (
                    1,
                    grant.claim_token,
                    NOW + timedelta(seconds=600),
                    "hard_deadline_exceeded",
                ),
            )
            for attempt, token, observed_at, expected in cases:
                with self.assertRaises(WorkApiError) as raised:
                    renew_lease(
                        conn,
                        work_id,
                        attempt_number=attempt,
                        claim_token=token,
                        actor_id=EXPECTED_MAC_PRINCIPAL,
                        now=observed_at,
                    )
                conn.rollback()
                self.assertEqual(raised.exception.code, expected)
        finally:
            conn.close()

    def test_unreturned_signer_failure_releases_claim_without_spending_attempt(self):
        work_id = self._seed()
        grant = self._claim()
        conn = self.connect()
        try:
            self.assertTrue(
                release_unmaterialized_claim(
                    conn,
                    grant,
                    actor_id=EXPECTED_MAC_PRINCIPAL,
                    now=NOW + timedelta(seconds=1),
                )
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, attempt_count, claim_token_hash FROM pcf_work_items "
                    "WHERE work_id = %s",
                    (str(work_id),),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(row, ("queued", 0, None))

    def test_two_workers_skip_locked_and_claim_distinct_routes(self):
        first_id = self._seed(route="988200", delivery="1806242572")
        second_id = self._seed(route="989262", delivery="1806242573")
        barrier = threading.Barrier(2)

        def claim_after_barrier():
            conn = self.connect()
            try:
                barrier.wait(timeout=2)
                grant = claim_one(
                    conn, self.claim_request(), actor_id=EXPECTED_MAC_PRINCIPAL, now=NOW
                )
                conn.commit()
                return grant.work_id
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            claimed = set(executor.map(lambda _: claim_after_barrier(), range(2)))
        self.assertEqual(claimed, {first_id, second_id})

    def test_newer_successor_is_claimed_and_blocks_predecessor_result(self):
        old_id = self._seed(created_at=NOW)
        old_grant = self._claim(now=NOW + timedelta(seconds=1))
        self.assertEqual(old_grant.work_id, old_id)
        successor_id = self._seed(
            created_at=NOW + timedelta(seconds=2), input_hash="b" * 64
        )
        raw = result_payload(token=old_grant.claim_token, work_id=old_id)
        conn = self.connect()
        try:
            with self.assertRaises(WorkApiError) as raised:
                accept_result(
                    conn,
                    old_id,
                    ResultRequest.model_validate(raw),
                    raw,
                    actor_id=EXPECTED_MAC_PRINCIPAL,
                    now=NOW + timedelta(seconds=60),
                )
            self.assertTrue(raised.exception.commit)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT work_id, status FROM pcf_work_items ORDER BY created_at, work_id"
                )
                states = dict(cur.fetchall())
        finally:
            conn.close()
        self.assertEqual(raised.exception.code, "invalid_work_state")
        self.assertEqual(states[str(old_id)], "superseded")
        self.assertEqual(states[str(successor_id)], "queued")

    def test_listen_notify_wakes_for_a_durable_row(self):
        started = threading.Event()

        def waiter():
            conn = self.connect()
            try:
                started.set()
                return wait_for_claim(
                    conn,
                    self.claim_request(wait_seconds=3),
                    actor_id=EXPECTED_MAC_PRINCIPAL,
                )
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(waiter)
            self.assertTrue(started.wait(timeout=1))
            time.sleep(0.2)
            work_id = self._seed()
            grant = future.result(timeout=4)
        self.assertEqual(grant.work_id, work_id)

    def test_backup_receipt_matches_cluster_manifest_and_is_idempotent(self):
        backup_id = "routespark-nightly-20260829"
        sha256 = "f" * 64
        covers = "2026-08-29T02:14:00Z"
        verified = "2026-08-29T03:00:00Z"
        raw = {
            "backupId": backup_id,
            "dumpName": "routespark_nightly_20260829T021500Z.dump",
            "sha256": sha256,
            "sizeBytes": 1436256,
            "coversThrough": covers,
            "verifiedAt": verified,
            "offBoxTargets": [
                "dell:/srv/routespark/backups/postgres",
                "dell:/mnt/archive/backups/postgres",
            ],
            "restoreInventoryVerified": True,
        }
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_work_events (
                        event_id, source_event_id, event_type, actor_type,
                        actor_id, payload
                    ) VALUES (%s, %s, 'backup_manifest_recorded', 'cluster', %s, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        f"pcf-backup-manifest:{backup_id}",
                        "nightly-backup-producer",
                        Json({key: raw[key] for key in (
                            "backupId", "dumpName", "sha256", "sizeBytes", "coversThrough"
                        )}),
                    ),
                )
            conn.commit()
            mismatched_raw = dict(raw)
            mismatched_raw["sizeBytes"] += 1
            with self.assertRaises(WorkApiError) as mismatch:
                accept_backup_receipt(
                    conn,
                    BackupReceiptRequest.model_validate(mismatched_raw),
                    mismatched_raw,
                    actor_id=EXPECTED_BACKUP_PRINCIPAL,
                    now=datetime(2026, 8, 29, 3, 1, tzinfo=timezone.utc),
                )
            conn.rollback()
            self.assertEqual(mismatch.exception.code, "backup_manifest_mismatch")
            receipt = BackupReceiptRequest.model_validate(raw)
            first = accept_backup_receipt(
                conn,
                receipt,
                raw,
                actor_id=EXPECTED_BACKUP_PRINCIPAL,
                now=datetime(2026, 8, 29, 3, 1, tzinfo=timezone.utc),
            )
            conn.commit()
            second = accept_backup_receipt(
                conn,
                receipt,
                raw,
                actor_id=EXPECTED_BACKUP_PRINCIPAL,
                now=datetime(2026, 8, 29, 4, 0, tzinfo=timezone.utc),
            )
            conn.commit()
            conflicting_raw = dict(raw)
            conflicting_raw["verifiedAt"] = "2026-08-29T03:00:01Z"
            with self.assertRaises(WorkApiError) as conflict:
                accept_backup_receipt(
                    conn,
                    BackupReceiptRequest.model_validate(conflicting_raw),
                    conflicting_raw,
                    actor_id=EXPECTED_BACKUP_PRINCIPAL,
                    now=datetime(2026, 8, 29, 4, 0, tzinfo=timezone.utc),
                )
            conn.rollback()
            self.assertEqual(conflict.exception.code, "backup_receipt_conflict")
        finally:
            conn.close()
        self.assertFalse(first.idempotent_replay)
        self.assertTrue(second.idempotent_replay)
        self.assertEqual(first.event_id, second.event_id)


if __name__ == "__main__":
    unittest.main()

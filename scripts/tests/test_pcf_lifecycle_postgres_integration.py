"""Opt-in integration tests for the isolated external-drive PCF database."""

from __future__ import annotations

import json
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2 import errors
from psycopg2.extensions import make_dsn
from psycopg2.extras import Json

from order_forecast.api.pcf_event_contract import firestore_revision
from order_forecast.api.pcf_projection_ingress import (
    ProjectionIngressConflict,
    commit_projection_event,
    record_projection_rejection,
)
from order_forecast.scripts.pcf_due_work_schema_migration import apply, inspect
from order_forecast.scripts.pcf_work_schema_migration import apply as apply_work_schema
from order_forecast.scripts.pcf_work_schema_migration import inspect as inspect_work_schema
from order_forecast.scripts.pcf_lifecycle_due import (
    apply_container_projection,
    block_lifecycle_claim,
    claim_one_lifecycle_work,
    complete_lifecycle_claim,
    refresh_lifecycle_claim_authority,
    plan_projected_delivery,
    recover_expired_lifecycle_leases,
)


TEST_DSN = os.environ.get("TEST_PCF_POSTGRES_DSN", "").strip()
NOW = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)


def revision(day: int, hour: int = 12, mutation_kind: str = "upsert") -> str:
    return firestore_revision(
        f"2026-08-{day:02d}T{hour:02d}:00:00.000000000Z",
        mutation_kind,
    )


def container_event(
    *,
    event_id: str,
    route: str = "989262",
    delivery: str = "1806242572",
    container: str = "C01",
    source_revision: str | None = None,
    expiries: tuple[str | None, ...] = ("2026-08-20",),
    mutation_kind: str = "upsert",
    excluded: bool = False,
) -> dict:
    payload = None
    if mutation_kind == "upsert":
        payload = {
            "active": True,
            "loadingDate": "2026-08-20",
            "pageNumbers": [1],
            "items": [
                {
                    "pageNumber": 1,
                    "itemPosition": index,
                    "product": str(100 + index),
                    "description": f"Item {index}",
                    "expiryDate": expiry,
                    "visibleOnList": True,
                    "isLowQuantity": False,
                    "isShortCoded": False,
                    "guaranteed": None,
                }
                for index, expiry in enumerate(expiries)
            ],
        }
    return {
        "schemaVersion": 1,
        "sourceEventId": event_id,
        "eventType": "projection.pcf_container.changed",
        "mutationKind": mutation_kind,
        "occurredAt": "2026-08-25T10:00:00.000000000Z",
        "sourcePath": f"routes/{route}/pcfs/{delivery}/containers/{container}",
        "sourceRevision": source_revision
        or revision(20, mutation_kind=mutation_kind),
        "entity": {"route": route, "delivery": delivery, "container": container},
        "automationExcluded": excluded,
        "fixture": {"identity": "fixture-one", "generation": "1"} if excluded else None,
        "payload": payload,
    }


@unittest.skipUnless(TEST_DSN, "TEST_PCF_POSTGRES_DSN is not set")
class PcfLifecyclePostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = psycopg2.connect(TEST_DSN)
        try:
            result = apply(conn)
            if not result["ready"]:
                raise AssertionError(result)
            work_result = apply_work_schema(conn)
            if not work_result["ready"]:
                raise AssertionError(work_result)
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
                cur.execute(
                    "TRUNCATE pcf_work_events, pcf_work_items, due_work, "
                    "items_current RESTART IDENTITY"
                )
            conn.commit()
        finally:
            conn.close()

    def _project(self, *events):
        conn = self.connect()
        try:
            results = [apply_container_projection(conn, event) for event in events]
            conn.commit()
            return results
        finally:
            conn.close()

    def _plan(self, route="989262", delivery="1806242572"):
        conn = self.connect()
        try:
            plan = plan_projected_delivery(
                conn,
                route_number=route,
                delivery_id=delivery,
                retention_days=4,
                policy_revision=revision(21),
                deadline_at=NOW + timedelta(days=30),
                now_utc=NOW,
            )
            conn.commit()
            return plan
        finally:
            conn.close()

    def test_migration_is_checksum_verified_idempotent_and_ready(self):
        conn = self.connect()
        try:
            first = inspect(conn)
            second = apply(conn)
            first_work = inspect_work_schema(conn)
            second_work = apply_work_schema(conn)
        finally:
            conn.close()
        self.assertTrue(first["ready"])
        self.assertTrue(second["ready"])
        self.assertTrue(first_work["ready"])
        self.assertTrue(second_work["ready"])
        self.assertRegex(first["checksum"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["checksum"], second["checksum"])
        self.assertRegex(first_work["checksum"], r"^[0-9a-f]{64}$")
        self.assertEqual(first_work["checksum"], second_work["checksum"])

    def test_runtime_role_is_least_privilege_and_not_owner_member(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT has_table_privilege('routespark_pcf_app', 'items_current', 'SELECT'),
                           has_table_privilege('routespark_pcf_app', 'items_current', 'INSERT'),
                           has_table_privilege('routespark_pcf_app', 'items_current', 'UPDATE'),
                           has_table_privilege('routespark_pcf_app', 'items_current', 'DELETE'),
                           has_table_privilege('routespark_pcf_app', 'due_work', 'TRUNCATE'),
                           pg_has_role('routespark_pcf_app', 'routespark_pcf_owner', 'MEMBER')
                    """
                )
                privileges = cur.fetchone()
                cur.execute(
                    """
                    SELECT app.rolcanlogin, app.rolsuper, app.rolcreatedb,
                           app.rolcreaterole, owner.rolcanlogin
                    FROM pg_roles app
                    CROSS JOIN pg_roles owner
                    WHERE app.rolname = 'routespark_pcf_app'
                      AND owner.rolname = 'routespark_pcf_owner'
                    """
                )
                role_attributes = cur.fetchone()
                cur.execute(
                    """
                    SELECT tableowner
                    FROM pg_tables
                    WHERE schemaname = 'public' AND tablename = 'due_work'
                    """
                )
                owner = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(privileges, (True, True, True, False, False, False))
        self.assertEqual(role_attributes, (True, False, False, False, False))
        self.assertEqual(owner, "routespark_pcf_owner")

    def test_runtime_role_can_authenticate_directly_and_stays_append_only(self):
        conn = psycopg2.connect(make_dsn(TEST_DSN, user="routespark_pcf_app"))
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_user")
                self.assertEqual(cur.fetchone()[0], "routespark_pcf_app")
                cur.execute("SELECT COUNT(*) FROM pcf_work_events")
                self.assertEqual(cur.fetchone()[0], 0)
                with self.assertRaises(errors.InsufficientPrivilege):
                    cur.execute("UPDATE pcf_work_events SET payload = payload")
            conn.rollback()
        finally:
            conn.close()

    def test_event_receipts_are_append_only_for_runtime_role(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT has_table_privilege(
                               'routespark_pcf_app', 'pcf_work_events', 'SELECT'
                           ),
                           has_table_privilege(
                               'routespark_pcf_app', 'pcf_work_events', 'INSERT'
                           ),
                           has_table_privilege(
                               'routespark_pcf_app', 'pcf_work_events', 'UPDATE'
                           ),
                           has_table_privilege(
                               'routespark_pcf_app', 'pcf_work_events', 'DELETE'
                           ),
                           has_table_privilege(
                               'routespark_pcf_app', 'pcf_work_events', 'TRUNCATE'
                           )
                    """
                )
                privileges = cur.fetchone()
                cur.execute(
                    """
                    SELECT tableowner
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename = 'pcf_work_events'
                    """
                )
                owner = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(privileges, (True, True, False, False, False))
        self.assertEqual(owner, "routespark_pcf_owner")

    def test_owner_default_table_privileges_do_not_leak_to_runtime_or_public(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_owner")
                cur.execute("CREATE TABLE pcf_default_privilege_probe (id INTEGER)")
                cur.execute("RESET ROLE")
                cur.execute(
                    """
                    SELECT has_table_privilege(
                               'routespark_pcf_app',
                               'pcf_default_privilege_probe',
                               'SELECT'
                           ),
                           NOT EXISTS (
                               SELECT 1
                               FROM pg_class AS class
                               CROSS JOIN LATERAL aclexplode(
                                   COALESCE(
                                       class.relacl,
                                       acldefault('r', class.relowner)
                                   )
                               ) AS privilege
                               WHERE class.oid = 'pcf_default_privilege_probe'::regclass
                                 AND privilege.grantee = 0
                           )
                    """
                )
                self.assertEqual(cur.fetchone(), (False, True))
            conn.rollback()
        finally:
            conn.close()

    def test_event_foreign_key_restricts_owner_work_deletion(self):
        conn = self.connect()
        work_id = str(uuid.uuid4())
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
                cur.execute(
                    """
                    INSERT INTO pcf_work_items (
                        work_id, route_number, delivery_number, input_hash,
                        input_hash_version, status, required_capability,
                        manifest, source_queue_path, not_before
                    ) VALUES (
                        %s, '989262', '1806242572', %s,
                        'pcf-work-input-v1', 'queued', 'macos_vision',
                        '{}'::jsonb,
                        'pcf_processing_queue/989262/queue/1806242572', NOW()
                    )
                    """,
                    (work_id, "a" * 64),
                )
                cur.execute(
                    """
                    INSERT INTO pcf_work_events (
                        event_id, work_id, attempt_number, event_type,
                        actor_type, actor_id, payload
                    ) VALUES (%s, %s, 0, 'work_queued', 'cluster', 'test', '{}'::jsonb)
                    """,
                    (str(uuid.uuid4()), work_id),
                )
                cur.execute("RESET ROLE")
                cur.execute("SET ROLE routespark_pcf_owner")
                with self.assertRaises(errors.ForeignKeyViolation):
                    cur.execute("DELETE FROM pcf_work_items WHERE work_id = %s", (work_id,))
            conn.rollback()
        finally:
            conn.close()

    def test_ingress_receipt_and_projection_commit_atomically_and_deduplicate(self):
        event = container_event(event_id="event-ingress-atomic1")
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
            first = commit_projection_event(
                conn,
                event,
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
            )
            conn.commit()
            second = commit_projection_event(
                conn,
                event,
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM items_current")
                item_count = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*), MIN(payload->>'disposition') "
                    "FROM pcf_work_events WHERE source_event_id = %s",
                    (event["sourceEventId"],),
                )
                receipt = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(first.disposition, "apply")
        self.assertFalse(first.duplicate_transport_event)
        self.assertEqual(second.disposition, "apply")
        self.assertTrue(second.duplicate_transport_event)
        self.assertEqual(item_count, 1)
        self.assertEqual(receipt, (1, "apply"))

    def test_deterministic_rejection_receipt_is_idempotent_by_source_event(self):
        source_event_id = "event-rejected-postgres1"
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
            first = record_projection_rejection(
                conn,
                body_sha256="b" * 64,
                reason="contract_validation_failed",
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
                source_event_id=source_event_id,
            )
            conn.commit()
            second = record_projection_rejection(
                conn,
                body_sha256="b" * 64,
                reason="contract_validation_failed",
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
                source_event_id=source_event_id,
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pcf_work_events WHERE source_event_id = %s",
                    (source_event_id,),
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()

        self.assertFalse(first)
        self.assertTrue(second)
        self.assertEqual(count, 1)

    def test_reused_source_id_cannot_change_projection(self):
        event = container_event(event_id="event-ingress-conflict1")
        changed = container_event(
            event_id=event["sourceEventId"],
            expiries=("2026-09-30",),
        )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
            commit_projection_event(
                conn,
                event,
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
            )
            conn.commit()
            with self.assertRaises(ProjectionIngressConflict):
                commit_projection_event(
                    conn,
                    changed,
                    actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
                )
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT latest_expiry_date FROM items_current")
                expiry = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(expiry, datetime(2026, 8, 20).date())

    def test_runtime_can_append_bounded_rejection_but_not_modify_it(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
            record_projection_rejection(
                conn,
                body_sha256="a" * 64,
                reason="contract_validation_failed",
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
            )
            conn.commit()
            with conn.cursor() as cur:
                with self.assertRaises(errors.InsufficientPrivilege):
                    cur.execute("UPDATE pcf_work_events SET payload = '{}'::jsonb")
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT payload->>'reason' FROM pcf_work_events")
                reason = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(reason, "contract_validation_failed")

    def test_runtime_delete_is_denied_by_postgres(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SET ROLE routespark_pcf_app")
                with self.assertRaises(errors.InsufficientPrivilege):
                    cur.execute("DELETE FROM due_work")
            conn.rollback()
        finally:
            conn.close()

    def test_projection_rejects_bad_counts_at_database_boundary(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                with self.assertRaises(errors.CheckViolation):
                    cur.execute(
                        """
                        INSERT INTO items_current (
                            route_number, delivery_id, container_id, active,
                            items, item_count, valid_expiry_count, source_path,
                            source_revision, source_business_hash,
                            source_mutation_kind
                        ) VALUES (
                            '989262', '1806242572', 'C01', TRUE,
                            '[]'::jsonb, 1, 0,
                            'routes/989262/pcfs/1806242572/containers/C01',
                            %s, %s, 'upsert'
                        )
                        """,
                        (revision(20), "a" * 64),
                    )
            conn.rollback()
        finally:
            conn.close()

    def test_projection_is_monotonic_idempotent_and_suppresses_fixtures(self):
        original = container_event(event_id="event-original-0001")
        duplicate = dict(original)
        stale = container_event(
            event_id="event-stale-000001",
            source_revision=revision(19),
            expiries=("2026-09-01",),
        )
        no_op = container_event(
            event_id="event-noop-0000001",
            source_revision=revision(21),
        )
        changed = container_event(
            event_id="event-change-00001",
            source_revision=revision(22),
            expiries=("2026-09-01",),
        )
        fixture = container_event(
            event_id="event-fixture-0001",
            delivery="1806242999",
            excluded=True,
        )
        self.assertEqual(
            self._project(original, duplicate, stale, no_op, changed, fixture),
            [
                "apply",
                "duplicate_noop",
                "stale_noop",
                "advance_watermark_only",
                "apply",
                "fixture_suppressed",
            ],
        )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_revision, latest_expiry_date FROM items_current"
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(rows, [(revision(22), datetime(2026, 9, 1).date())])

    def test_delete_becomes_tombstone_outside_active_index(self):
        self._project(container_event(event_id="event-create-00001"))
        self._project(
            container_event(
                event_id="event-delete-00001",
                source_revision=revision(21, mutation_kind="delete"),
                mutation_kind="delete",
            )
        )
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_mutation_kind, deleted_at IS NOT NULL,
                           active, items, item_count
                    FROM items_current
                    """
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM items_current WHERE deleted_at IS NULL"
                )
                active_count = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(row, ("delete", True, False, [], 0))
        self.assertEqual(active_count, 0)

    def test_planner_writes_one_unique_due_row_and_supersedes_changed_schedule(self):
        self._project(
            container_event(event_id="event-c01-create01", container="C01"),
            container_event(
                event_id="event-c02-create01",
                container="C02",
                source_revision=revision(20, 13),
                expiries=("2026-08-19",),
            ),
        )
        first = self._plan()
        second = self._plan()
        self.assertEqual(first.candidate.semantic_key, second.candidate.semantic_key)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), MIN(status), MAX(status) FROM due_work")
                self.assertEqual(cur.fetchone(), (1, "scheduled", "scheduled"))
        finally:
            conn.close()

        self._project(
            container_event(
                event_id="event-c01-change01",
                container="C01",
                source_revision=revision(22),
                expiries=("2026-09-01",),
            )
        )
        changed = self._plan()
        self.assertNotEqual(first.candidate.semantic_key, changed.candidate.semantic_key)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, terminal_outcome FROM due_work ORDER BY id")
                rows = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(rows, [("cancelled", "superseded"), ("scheduled", None)])

    def test_claim_token_is_hashed_and_every_completion_is_cas_fenced(self):
        self._project(container_event(event_id="event-claim-create1"))
        plan = self._plan()
        conn = self.connect()
        try:
            claim = claim_one_lifecycle_work(
                conn,
                worker_id="worker-one",
                now_utc=NOW,
                allowed_semantic_keys=[plan.candidate.semantic_key],
            )
            self.assertIsNotNone(claim)
            conn.commit()
        finally:
            conn.close()
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT claim_token_hash FROM due_work WHERE id = %s", (claim.id,))
                stored_hash = cur.fetchone()[0]
            self.assertNotEqual(stored_hash, claim.claim_token)
            self.assertNotIn(claim.claim_token, stored_hash)
            self.assertFalse(
                complete_lifecycle_claim(
                    conn,
                    work_id=claim.id,
                    claim_token="wrong-token",
                    now_utc=NOW + timedelta(seconds=1),
                )
            )
            self.assertTrue(
                complete_lifecycle_claim(
                    conn,
                    work_id=claim.id,
                    claim_token=claim.claim_token,
                    now_utc=NOW + timedelta(seconds=1),
                )
            )
            self.assertFalse(
                complete_lifecycle_claim(
                    conn,
                    work_id=claim.id,
                    claim_token=claim.claim_token,
                    now_utc=NOW + timedelta(seconds=2),
                )
            )
            conn.commit()
        finally:
            conn.close()

    def test_effect_boundary_refresh_uses_server_clock_and_cas_fence(self):
        self._project(container_event(event_id="event-effect-boundary-create1"))
        plan = self._plan()
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT clock_timestamp()")
                server_now = cur.fetchone()[0]
                cur.execute(
                    """
                    UPDATE due_work
                    SET next_attempt_at = %s, deadline_at = %s
                    WHERE semantic_key = %s
                    """,
                    (
                        server_now - timedelta(seconds=1),
                        server_now + timedelta(hours=1),
                        plan.candidate.semantic_key,
                    ),
                )
            conn.commit()
            claim = claim_one_lifecycle_work(
                conn,
                worker_id="worker-effect-boundary",
                now_utc=server_now,
                allowed_semantic_keys=[plan.candidate.semantic_key],
                lease_seconds=60,
            )
            conn.commit()
            self.assertIsNone(
                refresh_lifecycle_claim_authority(
                    conn,
                    work_id=claim.id,
                    claim_token="wrong-token",
                    lease_seconds=60,
                    begin_effect=True,
                )
            )
            conn.rollback()
            checked_at = refresh_lifecycle_claim_authority(
                conn,
                work_id=claim.id,
                claim_token=claim.claim_token,
                lease_seconds=60,
                begin_effect=True,
            )
            self.assertIsNotNone(checked_at)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, lease_expires_at, dispatch_deadline_at
                    FROM due_work WHERE id = %s
                    """,
                    (claim.id,),
                )
                status, lease_expires_at, dispatch_deadline_at = cur.fetchone()
            self.assertEqual(status, "dispatching")
            self.assertEqual(lease_expires_at, dispatch_deadline_at)
            self.assertGreater(lease_expires_at, checked_at)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE due_work
                    SET claimed_at = clock_timestamp() - INTERVAL '2 minutes',
                        lease_expires_at = clock_timestamp() - INTERVAL '1 second',
                        dispatch_deadline_at = clock_timestamp() - INTERVAL '1 second'
                    WHERE id = %s
                    """,
                    (claim.id,),
                )
            conn.commit()
            self.assertIsNone(
                refresh_lifecycle_claim_authority(
                    conn,
                    work_id=claim.id,
                    claim_token=claim.claim_token,
                    lease_seconds=60,
                    begin_effect=False,
                )
            )
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SELECT clock_timestamp()")
                recovery_now = cur.fetchone()[0]
            self.assertEqual(
                recover_expired_lifecycle_leases(conn, now_utc=recovery_now), 1
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, claim_token_hash, dispatch_deadline_at
                    FROM due_work WHERE id = %s
                    """,
                    (claim.id,),
                )
                self.assertEqual(cur.fetchone(), ("retryable", None, None))
        finally:
            conn.close()

    def test_block_reclaim_and_expired_lease_recovery_are_fenced(self):
        self._project(container_event(event_id="event-retry-create1"))
        plan = self._plan()
        conn = self.connect()
        try:
            claim = claim_one_lifecycle_work(
                conn,
                worker_id="worker-one",
                now_utc=NOW,
                allowed_semantic_keys=[plan.candidate.semantic_key],
                lease_seconds=30,
            )
            conn.commit()
            self.assertTrue(
                block_lifecycle_claim(
                    conn,
                    work_id=claim.id,
                    claim_token=claim.claim_token,
                    now_utc=NOW + timedelta(seconds=1),
                    next_attempt_at=NOW + timedelta(seconds=10),
                    reason="authority_retry",
                )
            )
            conn.commit()
            reclaimed = claim_one_lifecycle_work(
                conn,
                worker_id="worker-two",
                now_utc=NOW + timedelta(seconds=10),
                allowed_semantic_keys=[plan.candidate.semantic_key],
                lease_seconds=30,
            )
            conn.commit()
            self.assertEqual(reclaimed.attempt_count, 2)
            self.assertEqual(
                recover_expired_lifecycle_leases(
                    conn, now_utc=NOW + timedelta(seconds=41)
                ),
                1,
            )
            conn.commit()
            self.assertFalse(
                complete_lifecycle_claim(
                    conn,
                    work_id=reclaimed.id,
                    claim_token=reclaimed.claim_token,
                    now_utc=NOW + timedelta(seconds=42),
                )
            )
        finally:
            conn.close()

    def _seed_delivery(self, delivery: str, event_suffix: str):
        self._project(
            container_event(
                event_id=f"event-seed-{event_suffix}-0001",
                delivery=delivery,
            )
        )
        return self._plan(delivery=delivery).candidate.semantic_key

    def test_skip_locked_claims_different_deliveries_without_waiting(self):
        keys = [
            self._seed_delivery("1806242572", "a"),
            self._seed_delivery("1806242573", "b"),
        ]
        barrier = threading.Barrier(2)

        def claim(worker_id):
            conn = self.connect()
            try:
                result = claim_one_lifecycle_work(
                    conn,
                    worker_id=worker_id,
                    now_utc=NOW,
                    allowed_semantic_keys=keys,
                )
                barrier.wait(timeout=3)
                conn.commit()
                return result
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, ("worker-one", "worker-two")))
        self.assertEqual(len({claim.id for claim in claims if claim}), 2)

    def test_delivery_advisory_fence_prevents_two_simultaneous_leases(self):
        key = self._seed_delivery("1806242572", "one")
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO due_work (
                        kind, semantic_key, route_number, delivery_id, container_id,
                        transition_kind, effective_business_date, source_revision,
                        source_business_hash, decision_watermark, due_at,
                        next_attempt_at, deadline_at
                    )
                    SELECT kind, semantic_key || '-successor', route_number,
                           delivery_id, container_id, transition_kind,
                           effective_business_date, source_revision,
                           source_business_hash, decision_watermark, due_at,
                           next_attempt_at, deadline_at
                    FROM due_work WHERE semantic_key = %s
                    """,
                    (key,),
                )
            conn.commit()
        finally:
            conn.close()
        barrier = threading.Barrier(2)

        def claim(worker_id):
            conn = self.connect()
            try:
                result = claim_one_lifecycle_work(
                    conn,
                    worker_id=worker_id,
                    now_utc=NOW,
                    all_proven_work=True,
                )
                barrier.wait(timeout=3)
                conn.commit()
                return result
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, ("worker-one", "worker-two")))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_partial_indexes_are_used_with_large_terminal_history(self):
        self._seed_delivery("1806242572", "index")
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO due_work (
                        kind, semantic_key, route_number, delivery_id, container_id,
                        transition_kind, effective_business_date, source_revision,
                        source_business_hash, decision_watermark, due_at,
                        next_attempt_at, deadline_at, status, terminal_outcome,
                        terminal_at, retain_until
                    )
                    SELECT 'lifecycle', 'terminal-' || value::text, '989262',
                           (1900000000 + value)::text, 'C01', 'archive_delivery',
                           DATE '2026-08-25', %s, %s, '{}'::jsonb,
                           %s, %s, %s, 'complete', 'verified', %s, %s
                    FROM generate_series(1, 10000) value
                    """,
                    (
                        revision(20),
                        "b" * 64,
                        NOW - timedelta(days=1),
                        NOW - timedelta(days=1),
                        NOW + timedelta(days=30),
                        NOW,
                        NOW + timedelta(days=90),
                    ),
                )
                cur.execute("ANALYZE due_work")
                cur.execute(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT id
                    FROM due_work
                    WHERE kind = 'lifecycle'
                      AND status IN ('scheduled', 'retryable', 'blocked')
                      AND next_attempt_at <= %s
                    ORDER BY next_attempt_at, priority DESC, created_at, id
                    LIMIT 1
                    """,
                    (NOW,),
                )
                plan = json.dumps(cur.fetchone()[0])
            conn.rollback()
        finally:
            conn.close()
        self.assertIn("due_work_claim_ready_idx", plan)


if __name__ == "__main__":
    unittest.main()

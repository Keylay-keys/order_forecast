"""Opt-in integration tests for the isolated external-drive PCF database."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2 import errors
from psycopg2.extensions import make_dsn
from psycopg2.extras import Json, RealDictCursor

from order_forecast.api.pcf_event_contract import firestore_revision
from order_forecast.api.pcf_projection_ingress import (
    ProjectionIngressConflict,
    commit_projection_event,
    record_projection_rejection,
)
from order_forecast.api.pcf_projection_reconciliation import (
    ExactFirestoreAbsenceProof,
    ProjectionReconciliationError,
    RouteEntityKey,
    build_absence_tombstone,
    reconcile_route_projection,
    summarize_source_route,
    targeted_repair_events,
)
from order_forecast.api.pcf_projection_reconciliation_runtime import (
    ProjectionReconciliationRuntime,
    record_reconciliation_failure,
    reserve_due_reconciliation_route,
)
from order_forecast.api.pcf_projection_source import FirestoreRouteSnapshot
from order_forecast.scripts.pcf_due_work_schema_migration import apply, inspect
from order_forecast.scripts.pcf_work_schema_migration import apply as apply_work_schema
from order_forecast.scripts.pcf_work_schema_migration import inspect as inspect_work_schema
from order_forecast.scripts.pcf_lifecycle_due import (
    LifecycleConflictError,
    apply_route_retention_policy,
    apply_container_projection,
    block_lifecycle_claim,
    claim_one_lifecycle_work,
    complete_lifecycle_claim,
    load_route_retention_policy,
    refresh_lifecycle_claim_authority,
    plan_projected_delivery,
    plan_projected_delivery_from_current_policy,
    recover_expired_lifecycle_leases,
)
from order_forecast.scripts.pcf_projection_route_registry import (
    RegistryEvidence,
    RegistryRoute,
    initialize_registry,
    load_signed_census,
)


TEST_DSN = os.environ.get("TEST_PCF_POSTGRES_DSN", "").strip()
SIGNED_RETENTION_CENSUS = os.environ.get("PCF_SIGNED_RETENTION_CENSUS", "").strip()
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


def retention_event(
    *,
    event_id: str,
    route: str = "989262",
    retention_days: int | None = 4,
    source_revision: str | None = None,
) -> dict:
    mutation_kind = "delete" if retention_days is None else "upsert"
    return {
        "schemaVersion": 1,
        "sourceEventId": event_id,
        "eventType": "projection.pcf_retention.changed",
        "mutationKind": mutation_kind,
        "occurredAt": "2026-08-25T10:00:00.000000000Z",
        "sourcePath": f"pcfProjectionRoutePolicies/{route}",
        "sourceRevision": source_revision
        or revision(21, mutation_kind=mutation_kind),
        "entity": {"route": route},
        "automationExcluded": False,
        "fixture": None,
        "payload": (
            None
            if retention_days is None
            else {"retentionDays": retention_days, "ownerUserId": f"owner-{route}"}
        ),
    }


def registry_evidence() -> RegistryEvidence:
    routes = [
        RegistryRoute(route_number=str(970000 + index), eligible=True)
        for index in range(58)
    ]
    routes.extend(
        RegistryRoute(route_number=route, eligible=True)
        for route in ("900000", "988200", "989262")
    )
    routes.extend(
        RegistryRoute(route_number=route, eligible=False)
        for route in ("960462", "961636", "988580")
    )
    return RegistryEvidence(
        census_sha256="a" * 64,
        observed_at="2026-08-29T16:53:46.297Z",
        routes=tuple(sorted(routes, key=lambda row: row.route_number)),
    )


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
                    "items_current, pcf_projection_routes RESTART IDENTITY"
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

    def test_route_reconciliation_records_match_without_enabling_serving(self):
        projected = container_event(
            event_id="reconcile-match-event-0001",
            source_revision=revision(20),
        )
        snapshot_revision = revision(24)
        source = summarize_source_route("989262", snapshot_revision, [projected])
        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, last_projected_revision,
                        last_projected_at
                    ) VALUES ('989262', TRUE, %s, %s)
                    """,
                    (revision(20), NOW),
                )
            result = reconcile_route_projection(
                conn,
                source,
                observed_at=NOW,
                healthy_until=NOW + timedelta(minutes=5),
            )
            conn.commit()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT eligible, serving_enabled, reconciliation_status,
                           parity_source_revision, parity_projection_revision,
                           source_entity_count, projection_entity_count,
                           source_business_hash, projection_business_hash,
                           healthy_until
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                control = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(result.status, "matched")
        self.assertTrue(control["eligible"])
        self.assertFalse(control["serving_enabled"])
        self.assertEqual(control["reconciliation_status"], "matched")
        self.assertEqual(control["parity_source_revision"], snapshot_revision)
        self.assertEqual(control["parity_projection_revision"], snapshot_revision)
        self.assertEqual(control["source_entity_count"], 1)
        self.assertEqual(control["projection_entity_count"], 1)
        self.assertEqual(control["source_business_hash"], control["projection_business_hash"])
        self.assertEqual(control["healthy_until"], NOW + timedelta(minutes=5))

    def test_reconciliation_reservation_is_daily_fair_and_excludes_ineligible_routes(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, last_reconciliation_attempt_at
                    ) VALUES
                        ('900000', FALSE, NULL),
                        ('988200', TRUE, NULL),
                        ('989262', TRUE, %s)
                    """,
                    (NOW - timedelta(days=2),),
                )
            conn.commit()
            first = reserve_due_reconciliation_route(conn, observed_at=NOW)
            conn.commit()
            second = reserve_due_reconciliation_route(conn, observed_at=NOW)
            conn.commit()
            third = reserve_due_reconciliation_route(conn, observed_at=NOW)
            conn.commit()
            next_day = reserve_due_reconciliation_route(
                conn,
                observed_at=NOW + timedelta(days=1),
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT route_number, last_reconciliation_attempt_at
                    FROM pcf_projection_routes
                    ORDER BY route_number
                    """
                )
                attempts = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual((first, second, third, next_day), ("988200", "989262", None, "988200"))
        self.assertIsNone(attempts[0][1])
        self.assertEqual(attempts[1][1], NOW + timedelta(days=1))
        self.assertEqual(attempts[2][1], NOW)

    def test_reconciliation_failure_clears_health_without_enabling_serving(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, serving_enabled,
                        reconciliation_status, parity_source_revision,
                        parity_projection_revision, source_entity_count,
                        projection_entity_count, source_business_hash,
                        projection_business_hash, last_reconciled_at, healthy_until
                    ) VALUES (
                        '989262', TRUE, FALSE, 'matched', %s, %s,
                        1, 1, %s, %s, %s, %s
                    )
                    """,
                    (revision(24), revision(24), "a" * 64, "a" * 64, NOW, NOW + timedelta(hours=1)),
                )
            conn.commit()
            record_reconciliation_failure(conn, "989262")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT serving_enabled, reconciliation_status,
                           last_reconciled_at, healthy_until
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                control = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(control, (False, "unknown", NOW, None))

    def test_route_reconciliation_mismatch_clears_health_and_plans_exact_replay(self):
        projected = container_event(
            event_id="reconcile-old-event-0001",
            source_revision=revision(20),
            expiries=("2026-08-20",),
        )
        current = container_event(
            event_id="reconcile-current-event-0001",
            source_revision=revision(23),
            expiries=("2026-09-30",),
        )
        source = summarize_source_route("989262", revision(24), [current])
        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, serving_enabled,
                        reconciliation_status, last_projected_revision,
                        last_projected_at, healthy_until
                    ) VALUES (
                        '989262', TRUE, TRUE, 'unknown', %s, %s, %s
                    )
                    """,
                    (revision(20), NOW, NOW + timedelta(hours=1)),
                )
            result = reconcile_route_projection(conn, source, observed_at=NOW)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT reconciliation_status, healthy_until
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                status, healthy_until = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(result.status, "mismatched")
        self.assertEqual(status, "mismatched")
        self.assertIsNone(healthy_until)
        repairs = targeted_repair_events(result)
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0]["sourceEventId"], "reconcile-current-event-0001")

    def test_route_reconciliation_detects_actual_row_corruption_with_stale_hash(self):
        projected = container_event(
            event_id="reconcile-integrity-event-0001",
            source_revision=revision(20),
        )
        source = summarize_source_route("989262", revision(24), [projected])
        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, serving_enabled,
                        reconciliation_status, last_projected_revision,
                        last_projected_at, healthy_until
                    ) VALUES (
                        '989262', TRUE, TRUE, 'unknown', %s, %s, %s
                    )
                    """,
                    (revision(20), NOW, NOW + timedelta(hours=1)),
                )
                cur.execute(
                    """
                    UPDATE items_current
                    SET items = jsonb_set(items, '{0,product}', '"corrupt"'::jsonb)
                    WHERE route_number = '989262'
                      AND delivery_id = '1806242572'
                      AND container_id = 'C01'
                    """
                )
            result = reconcile_route_projection(conn, source, observed_at=NOW)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT reconciliation_status, healthy_until
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                status, healthy_until = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(result.status, "mismatched")
        self.assertEqual(result.reason, "projection_integrity_mismatch")
        self.assertEqual(status, "mismatched")
        self.assertIsNone(healthy_until)
        repairs = targeted_repair_events(result)
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0]["sourceEventId"], "reconcile-integrity-event-0001")
        conn = self.connect()
        try:
            repaired = commit_projection_event(
                conn,
                repairs[0],
                actor_id="pcf-projection-reconciler",
                now_utc=NOW,
            )
            self.assertEqual(repaired.disposition, "integrity_repaired")
            verified = reconcile_route_projection(conn, source, observed_at=NOW)
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(verified.status, "matched")

    def test_same_revision_repairs_corrupt_stored_hash_without_accepting_new_content(self):
        projected = container_event(
            event_id="reconcile-hash-integrity-event-0001",
            source_revision=revision(20),
        )
        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE items_current
                    SET source_business_hash = %s
                    WHERE route_number = '989262'
                      AND delivery_id = '1806242572'
                      AND container_id = 'C01'
                    """,
                    ("f" * 64,),
                )
            self.assertEqual(
                apply_container_projection(conn, projected),
                "integrity_repaired",
            )
            changed = container_event(
                event_id="reconcile-hash-conflict-event-0001",
                source_revision=revision(20),
                expiries=("2026-09-30",),
            )
            with self.assertRaises(LifecycleConflictError):
                apply_container_projection(conn, changed)
            conn.rollback()
        finally:
            conn.close()

    def test_exact_absence_tombstone_is_applied_and_later_recreation_wins(self):
        original = container_event(
            event_id="reconcile-absence-original-0001",
            source_revision=revision(20),
        )
        tombstone = build_absence_tombstone(
            "989262",
            RouteEntityKey("1806242572", "C01"),
            ExactFirestoreAbsenceProof(
                source_path="routes/989262/pcfs/1806242572/containers/C01",
                read_time="2026-08-24T12:00:00Z",
                exists=False,
            ),
        )
        recreated = container_event(
            event_id="reconcile-absence-recreated-0001",
            source_revision=revision(25),
            expiries=("2026-10-01",),
        )
        conn = self.connect()
        try:
            applied = apply_container_projection(conn, original)
            deleted = apply_container_projection(conn, tombstone)
            restored = apply_container_projection(conn, recreated)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_revision, deleted_at, items->0->>'expiryDate'
                    FROM items_current
                    WHERE route_number = '989262'
                      AND delivery_id = '1806242572'
                      AND container_id = 'C01'
                    """
                )
                revision_value, deleted_at, expiry = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(applied, "apply")
        self.assertEqual(deleted, "apply")
        self.assertEqual(restored, "apply")
        self.assertEqual(revision_value, revision(25))
        self.assertIsNone(deleted_at)
        self.assertEqual(expiry, "2026-10-01")

    def test_route_reconciliation_rejects_ineligible_route(self):
        source = summarize_source_route("989262", revision(24), [])
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pcf_projection_routes (route_number) VALUES ('989262')"
                )
            with self.assertRaisesRegex(ProjectionReconciliationError, "route_not_eligible"):
                reconcile_route_projection(conn, source, observed_at=NOW)
            conn.rollback()
        finally:
            conn.close()

    def test_reconciliation_runtime_repairs_missing_projection_then_matches(self):
        projected = container_event(
            event_id="runtime-reconcile-source-0001",
            source_revision=revision(20),
        )
        source = summarize_source_route("989262", revision(24), [projected])

        class SourceAdapter:
            def __init__(self):
                self.reads = 0

            def read_route(self, route):
                self.reads += 1
                self.assert_route = route
                return FirestoreRouteSnapshot(source, 1, 1, 1, 2)

            def read_exact_absence(self, route, key):
                raise AssertionError((route, key))

        source_adapter = SourceAdapter()
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (route_number, eligible)
                    VALUES ('989262', TRUE)
                    """
                )
            conn.commit()
        finally:
            conn.close()
        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=source_adapter,
        )
        report = runtime.run_route("989262", observed_at=NOW)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT reconciliation_status, serving_enabled, healthy_until
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                status, serving_enabled, healthy_until = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM items_current WHERE deleted_at IS NULL"
                )
                active_count = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(report.status, "matched")
        self.assertEqual(report.source_snapshot_count, 2)
        self.assertEqual(report.upsert_repairs, 1)
        self.assertEqual(report.tombstone_repairs, 0)
        self.assertEqual(source_adapter.reads, 2)
        self.assertEqual(status, "matched")
        self.assertFalse(serving_enabled)
        self.assertIsNone(healthy_until)
        self.assertEqual(active_count, 1)

    def test_review_route_reconciliation_observes_but_never_repairs(self):
        projected_review_event = container_event(
            event_id="runtime-review-existing-0001",
            route="900000",
            delivery="REVIEW-OLD",
            source_revision=revision(19),
        )
        review_event = container_event(
            event_id="runtime-review-observe-only-0001",
            route="900000",
            delivery="REVIEW-NEW",
            source_revision=revision(20),
        )
        source = summarize_source_route("900000", revision(24), [review_event])

        class SourceAdapter:
            def __init__(self):
                self.reads = 0
                self.absence_reads = 0

            def read_route(self, route):
                self.reads += 1
                self.assert_route = route
                return FirestoreRouteSnapshot(
                    source=source,
                    route_reads=1,
                    delivery_reads=1,
                    container_reads=1,
                    query_count=2,
                    minimum_billed_document_reads=3,
                )

            def read_exact_absence(self, route, key):
                self.absence_reads += 1
                raise AssertionError((route, key))

        source_adapter = SourceAdapter()
        conn = self.connect()
        try:
            apply_container_projection(conn, projected_review_event)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (route_number, eligible)
                    VALUES ('900000', TRUE)
                    """
                )
            conn.commit()
        finally:
            conn.close()
        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=source_adapter,
        )
        report = runtime.run_route("900000", observed_at=NOW)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM items_current WHERE route_number = '900000'"
                )
                projected_count = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(report.repair_mode, "observe_only")
        self.assertEqual(report.status, "mismatched")
        self.assertEqual(report.source_snapshot_count, 1)
        self.assertEqual(report.upsert_repairs, 0)
        self.assertEqual(report.tombstone_repairs, 0)
        self.assertEqual(report.unresolved_extras, 1)
        self.assertEqual(len(report.final_result.missing_in_projection), 1)
        self.assertEqual(len(report.final_result.extra_in_projection), 1)
        self.assertEqual(report.minimum_firestore_reads, 3)
        self.assertEqual(source_adapter.reads, 1)
        self.assertEqual(source_adapter.absence_reads, 0)
        self.assertEqual(projected_count, 1)

    def test_reconciliation_runtime_requires_absence_before_exact_tombstone(self):
        projected = container_event(
            event_id="runtime-reconcile-extra-0001",
            source_revision=revision(20),
        )
        first_source = summarize_source_route("989262", revision(24), [])
        second_source = summarize_source_route(
            "989262",
            firestore_revision("2026-08-24T14:00:00Z", "upsert"),
            [],
        )

        class SourceAdapter:
            def __init__(self):
                self.sources = [first_source, second_source]

            def read_route(self, route):
                return FirestoreRouteSnapshot(self.sources.pop(0), 1, 1, 0, 2)

            def read_exact_absence(self, route, key):
                return ExactFirestoreAbsenceProof(
                    source_path=(
                        f"routes/{route}/pcfs/{key.delivery_id}/containers/"
                        f"{key.container_id}"
                    ),
                    read_time="2026-08-24T13:00:00Z",
                    exists=False,
                )

        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, last_projected_revision,
                        last_projected_at
                    ) VALUES ('989262', TRUE, %s, %s)
                    """,
                    (revision(20), NOW),
                )
            conn.commit()
        finally:
            conn.close()
        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=SourceAdapter(),
        )
        report = runtime.run_route("989262", observed_at=NOW)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT deleted_at FROM items_current WHERE route_number = '989262'"
                )
                deleted_at = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(report.status, "matched")
        self.assertEqual(report.upsert_repairs, 0)
        self.assertEqual(report.tombstone_repairs, 1)
        self.assertEqual(report.unresolved_extras, 0)
        self.assertIsNotNone(deleted_at)

    def test_reconciliation_runtime_reads_no_firestore_for_ineligible_route(self):
        class SourceAdapter:
            def read_route(self, route):
                raise AssertionError(f"unexpected Firestore read for {route}")

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pcf_projection_routes (route_number) VALUES ('989262')"
                )
            conn.commit()
        finally:
            conn.close()
        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=SourceAdapter(),
        )
        with self.assertRaisesRegex(ProjectionReconciliationError, "route_not_eligible"):
            runtime.run_route("989262", observed_at=NOW)

    def test_reconciliation_runtime_never_tombstones_when_source_still_exists(self):
        projected = container_event(
            event_id="runtime-reconcile-existing-extra-0001",
            source_revision=revision(20),
        )
        sources = [
            summarize_source_route("989262", revision(24), []),
            summarize_source_route("989262", revision(25), []),
        ]

        class SourceAdapter:
            def read_route(self, route):
                return FirestoreRouteSnapshot(sources.pop(0), 1, 1, 0, 2)

            def read_exact_absence(self, route, key):
                return ExactFirestoreAbsenceProof(
                    source_path=(
                        f"routes/{route}/pcfs/{key.delivery_id}/containers/"
                        f"{key.container_id}"
                    ),
                    read_time="2026-08-24T13:00:00Z",
                    exists=True,
                )

        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, last_projected_revision,
                        last_projected_at
                    ) VALUES ('989262', TRUE, %s, %s)
                    """,
                    (revision(20), NOW),
                )
            conn.commit()
        finally:
            conn.close()
        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=SourceAdapter(),
        )
        report = runtime.run_route("989262", observed_at=NOW)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT deleted_at FROM items_current WHERE route_number = '989262'"
                )
                deleted_at = cur.fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(report.status, "mismatched")
        self.assertEqual(report.tombstone_repairs, 0)
        self.assertEqual(report.unresolved_extras, 1)
        self.assertIsNone(deleted_at)

    def test_reconciliation_runtime_two_consecutive_projection_ahead_snapshots_fail_closed(self):
        projected = container_event(
            event_id="runtime-reconcile-ahead-0001",
            source_revision=revision(20),
        )
        sources = [
            summarize_source_route("989262", revision(24), [projected]),
            summarize_source_route("989262", revision(25), [projected]),
        ]

        class SourceAdapter:
            def __init__(self):
                self.reads = 0
                self.absence_reads = 0

            def read_route(self, route):
                self.reads += 1
                return FirestoreRouteSnapshot(
                    source=sources.pop(0),
                    route_reads=1,
                    delivery_reads=1,
                    container_reads=1,
                    query_count=2,
                    minimum_billed_document_reads=3,
                )

            def read_exact_absence(self, route, key):
                self.absence_reads += 1
                raise AssertionError((route, key))

        source_adapter = SourceAdapter()
        conn = self.connect()
        try:
            apply_container_projection(conn, projected)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (
                        route_number, eligible, last_projected_revision,
                        last_projected_at
                    ) VALUES ('989262', TRUE, %s, %s)
                    ON CONFLICT (route_number) DO UPDATE
                    SET eligible = TRUE,
                        last_projected_revision = EXCLUDED.last_projected_revision,
                        last_projected_at = EXCLUDED.last_projected_at
                    """,
                    (revision(26), NOW),
                )
            conn.commit()
        finally:
            conn.close()

        runtime = ProjectionReconciliationRuntime(
            connection_provider=self.connect,
            connection_releaser=lambda current: current.close(),
            source_adapter=source_adapter,
        )
        report = runtime.run_route("989262", observed_at=NOW)

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT reconciliation_status, serving_enabled, healthy_until,
                           last_projected_revision
                    FROM pcf_projection_routes
                    WHERE route_number = '989262'
                    """
                )
                status, serving_enabled, healthy_until, last_revision = cur.fetchone()
                cur.execute(
                    """
                    SELECT COUNT(*) FROM items_current
                    WHERE route_number = '989262' AND deleted_at IS NULL
                    """
                )
                active_count = cur.fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(report.status, "mismatched")
        self.assertEqual(report.reason, "projection_ahead_of_snapshot")
        self.assertEqual(report.source_snapshot_count, 2)
        self.assertEqual(report.upsert_repairs, 0)
        self.assertEqual(report.tombstone_repairs, 0)
        self.assertEqual(report.unresolved_extras, 0)
        self.assertEqual(report.minimum_firestore_reads, 6)
        self.assertEqual(source_adapter.reads, 2)
        self.assertEqual(source_adapter.absence_reads, 0)
        self.assertEqual(status, "mismatched")
        self.assertFalse(serving_enabled)
        self.assertIsNone(healthy_until)
        self.assertEqual(last_revision, revision(26))
        self.assertEqual(active_count, 1)

    def test_route_retention_policy_is_monotonic_idempotent_and_fail_closed(self):
        conn = self.connect()
        try:
            self.assertIsNone(load_route_retention_policy(conn, "989262"))
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=4,
                    policy_revision=revision(20),
                ),
                "inserted",
            )
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=4,
                    policy_revision=revision(20),
                ),
                "duplicate_noop",
            )
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=4,
                    policy_revision=revision(21),
                ),
                "advance_watermark_only",
            )
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=9,
                    policy_revision=revision(19),
                ),
                "stale_noop",
            )
            with self.assertRaises(LifecycleConflictError):
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=6,
                    policy_revision=revision(21),
                )
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=6,
                    policy_revision=revision(22),
                ),
                "updated",
            )
            self.assertEqual(
                load_route_retention_policy(conn, "989262"),
                (6, revision(22)),
            )
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=None,
                    policy_revision=revision(23, mutation_kind="delete"),
                ),
                "updated",
            )
            self.assertIsNone(load_route_retention_policy(conn, "989262"))
            self.assertEqual(
                apply_route_retention_policy(
                    conn,
                    route_number="989262",
                    retention_days=9,
                    policy_revision=revision(22),
                ),
                "stale_noop",
            )
            conn.commit()
        finally:
            conn.close()

    def test_policy_event_replans_route_and_container_delete_cancels_exact_delivery(self):
        conn = self.connect()
        try:
            for delivery in ("1806242572", "1806242573"):
                projected = commit_projection_event(
                    conn,
                    container_event(
                        event_id=f"event-{delivery}",
                        delivery=delivery,
                        expiries=("2026-08-30",),
                    ),
                    actor_id="dispatcher@example.com",
                    now_utc=NOW,
                )
                self.assertEqual(projected.planning_disposition, "policy_missing")

            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM due_work")
                self.assertEqual(cur.fetchone()[0], 0)

            policy = commit_projection_event(
                conn,
                retention_event(event_id="event-policy-route"),
                actor_id="dispatcher@example.com",
                now_utc=NOW,
            )
            self.assertEqual(policy.planning_disposition, "route_replanned")
            self.assertEqual(policy.planned_delivery_count, 2)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT delivery_id, status
                    FROM due_work
                    WHERE kind = 'lifecycle'
                    ORDER BY delivery_id
                    """
                )
                self.assertEqual(
                    cur.fetchall(),
                    [("1806242572", "scheduled"), ("1806242573", "scheduled")],
                )

            deleted = commit_projection_event(
                conn,
                container_event(
                    event_id="event-container-delete",
                    delivery="1806242572",
                    source_revision=revision(22, mutation_kind="delete"),
                    mutation_kind="delete",
                ),
                actor_id="dispatcher@example.com",
                now_utc=NOW,
            )
            self.assertEqual(deleted.planning_disposition, "no_containers")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT delivery_id, status, terminal_outcome
                    FROM due_work
                    WHERE kind = 'lifecycle'
                    ORDER BY delivery_id
                    """
                )
                self.assertEqual(
                    cur.fetchall(),
                    [
                        ("1806242572", "cancelled", "projection_no_containers"),
                        ("1806242573", "scheduled", None),
                    ],
                )
            conn.commit()
        finally:
            conn.close()

    def test_overdue_replan_deadline_is_stable_across_wall_clock_time(self):
        conn = self.connect()
        try:
            apply_container_projection(
                conn,
                container_event(
                    event_id="stable-deadline-container-0001",
                    expiries=("2026-08-01",),
                ),
            )
            apply_route_retention_policy(
                conn,
                route_number="989262",
                retention_days=4,
                policy_revision=revision(21),
            )
            first = plan_projected_delivery_from_current_policy(
                conn,
                route_number="989262",
                delivery_id="1806242572",
                now_utc=NOW,
            )
            second = plan_projected_delivery_from_current_policy(
                conn,
                route_number="989262",
                delivery_id="1806242572",
                now_utc=NOW + timedelta(days=2),
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MIN(deadline_at), MAX(deadline_at)
                    FROM due_work
                    WHERE kind = 'lifecycle'
                    """
                )
                count, minimum, maximum = cur.fetchone()
            conn.commit()
        finally:
            conn.close()
        self.assertEqual(first.candidate.semantic_key, second.candidate.semantic_key)
        self.assertEqual(count, 1)
        self.assertEqual(minimum, maximum)

    def test_container_ingress_holds_shared_route_plan_lock_until_commit(self):
        first = self.connect()
        observer = self.connect()
        lock_key = "pcf-lifecycle-route:989262"
        try:
            commit_projection_event(
                first,
                container_event(event_id="event-route-lock"),
                actor_id="dispatcher@example.com",
                now_utc=NOW,
            )
            with observer.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                )
                self.assertFalse(cur.fetchone()[0])
            observer.rollback()
            first.commit()
            with observer.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                )
                self.assertTrue(cur.fetchone()[0])
            observer.rollback()
        finally:
            first.close()
            observer.close()

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

    def test_owner_only_registry_initializer_sets_exact_signed_eligibility(self):
        conn = self.connect()
        try:
            first = initialize_registry(conn, registry_evidence())
            second = initialize_registry(conn, registry_evidence())
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*),
                           COUNT(*) FILTER (WHERE eligible),
                           COUNT(*) FILTER (WHERE serving_enabled),
                           COUNT(*) FILTER (WHERE lifecycle_policy_active),
                           COUNT(*) FILTER (WHERE reconciliation_status <> 'unknown')
                    FROM pcf_projection_routes
                    """
                )
                counts = cur.fetchone()
                cur.execute(
                    """
                    SELECT route_number, eligible
                    FROM pcf_projection_routes
                    WHERE route_number = ANY(%s)
                    ORDER BY route_number
                    """,
                    (["960462", "961636", "988580"],),
                )
                unresolved = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(first["eligibleCount"], 61)
        self.assertEqual(second["eligibleCount"], 61)
        self.assertEqual(counts, (64, 61, 0, 0, 0))
        self.assertEqual(
            unresolved,
            [("960462", False), ("961636", False), ("988580", False)],
        )

    @unittest.skipUnless(
        SIGNED_RETENTION_CENSUS,
        "PCF_SIGNED_RETENTION_CENSUS is not set",
    )
    def test_exact_production_census_initializes_only_its_proven_routes(self):
        evidence = load_signed_census(Path(SIGNED_RETENTION_CENSUS))
        conn = self.connect()
        try:
            result = initialize_registry(conn, evidence)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT route_number, eligible, serving_enabled,
                           lifecycle_policy_active,
                           last_reconciliation_attempt_at
                    FROM pcf_projection_routes
                    ORDER BY route_number
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        self.assertEqual(result["censusSha256"], evidence.census_sha256)
        self.assertEqual((len(rows), sum(row[1] for row in rows)), (64, 61))
        self.assertTrue(all(not row[2] and not row[3] and row[4] is None for row in rows))
        self.assertEqual(
            [(row[0], row[1]) for row in rows if not row[1]],
            [("960462", False), ("961636", False), ("988580", False)],
        )

    def test_runtime_role_cannot_initialize_route_registry(self):
        conn = psycopg2.connect(make_dsn(TEST_DSN, user="routespark_pcf_app"))
        try:
            with self.assertRaises(errors.InsufficientPrivilege):
                initialize_registry(conn, registry_evidence())
        finally:
            conn.close()

    def test_registry_initializer_rejects_unproven_existing_eligibility(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pcf_projection_routes (route_number, eligible) "
                    "VALUES ('999999', TRUE)"
                )
            conn.commit()
            with self.assertRaisesRegex(RuntimeError, "unproven eligible"):
                initialize_registry(conn, registry_evidence())
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT eligible FROM pcf_projection_routes "
                    "WHERE route_number = '999999'"
                )
                self.assertTrue(cur.fetchone()[0])
        finally:
            conn.close()

    def test_registry_initializer_refuses_any_serving_enabled_route(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pcf_projection_routes "
                    "(route_number, eligible, serving_enabled) "
                    "VALUES ('989262', TRUE, TRUE)"
                )
            conn.commit()
            with self.assertRaisesRegex(RuntimeError, "serving-enabled"):
                initialize_registry(conn, registry_evidence())
        finally:
            conn.close()

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
                cur.execute(
                    """
                    SELECT
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'route_number', 'INSERT'
                        ),
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'last_reconciliation_attempt_at', 'UPDATE'
                        ),
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'eligible', 'INSERT'
                        ),
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'eligible', 'UPDATE'
                        ),
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'serving_enabled', 'INSERT'
                        ),
                        has_column_privilege(
                            'routespark_pcf_app', 'pcf_projection_routes',
                            'serving_enabled', 'UPDATE'
                        )
                    """
                )
                route_privileges = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(privileges, (True, True, True, False, False, False))
        self.assertEqual(route_privileges, (True, True, False, False, False, False))
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

    def test_runtime_role_can_project_and_reconcile_but_cannot_arm_route(self):
        owner_conn = self.connect()
        try:
            with owner_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pcf_projection_routes (route_number, eligible)
                    VALUES ('989262', TRUE)
                    """
                )
            owner_conn.commit()
        finally:
            owner_conn.close()

        app_conn = psycopg2.connect(make_dsn(TEST_DSN, user="routespark_pcf_app"))
        projected = container_event(
            event_id="runtime-column-grant-event-0001",
            source_revision=revision(20),
        )
        try:
            applied = apply_container_projection(app_conn, projected)
            policy = apply_route_retention_policy(
                app_conn,
                route_number="989262",
                retention_days=4,
                policy_revision=revision(21),
            )
            reserved = reserve_due_reconciliation_route(app_conn, observed_at=NOW)
            source = summarize_source_route("989262", revision(24), [projected])
            reconciliation = reconcile_route_projection(
                app_conn,
                source,
                observed_at=NOW,
            )
            app_conn.commit()
            self.assertEqual(applied, "apply")
            self.assertEqual(policy, "updated")
            self.assertEqual(reserved, "989262")
            self.assertEqual(reconciliation.status, "matched")

            with self.assertRaises(errors.InsufficientPrivilege):
                with app_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE pcf_projection_routes SET eligible = FALSE "
                        "WHERE route_number = '989262'"
                    )
            app_conn.rollback()
            with self.assertRaises(errors.InsufficientPrivilege):
                with app_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO pcf_projection_routes "
                        "(route_number, eligible) VALUES ('999999', TRUE)"
                    )
            app_conn.rollback()
        finally:
            app_conn.close()

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
                        work_id, route_number, delivery_number, source_revision, input_hash,
                        input_hash_version, status, required_capability,
                        manifest, ocr_context, source_queue_path, not_before
                    ) VALUES (
                        %s, '989262', '1806242572',
                        'fsut1|2026-08-29T18:00:00.000000000Z|0', %s,
                        'pcf-work-input-v1', 'queued', 'macos_vision',
                        '{}'::jsonb, '{}'::jsonb,
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
            accepted = commit_projection_event(
                conn,
                container_event(event_id=source_event_id),
                actor_id="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
            )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM pcf_work_events
                    WHERE payload->>'rejectedSourceEventIdSha256' = %s
                    """,
                    (hashlib.sha256(source_event_id.encode("utf-8")).hexdigest(),),
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()

        self.assertFalse(first)
        self.assertTrue(second)
        self.assertEqual(count, 1)
        self.assertFalse(accepted.duplicate_transport_event)

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

"""Opt-in PCF finalization tests on the isolated external-drive database."""

from __future__ import annotations

import os
import unittest

import psycopg2
from psycopg2.extras import RealDictCursor

from order_forecast.api.pcf_effect_projection import (
    ProjectionSnapshot,
    build_projection_write_set,
)
from order_forecast.api.pcf_effect_summary import (
    PcfEffectSummaryStore,
    PcfEffectSummaryVerifier,
    PcfSummaryError,
)
from order_forecast.api.tests.test_pcf_effect_projection import (
    NOW,
    _effect,
    _proofs,
    _snapshot,
)
from order_forecast.scripts.pcf_due_work_schema_migration import apply
from order_forecast.scripts.pcf_work_schema_migration import apply as apply_work_schema


TEST_DSN = os.environ.get("TEST_PCF_POSTGRES_DSN", "").strip()
REVISION = "fsut1|2026-08-25T18:00:00.000000000Z|0"


def _verified_snapshot():
    effect = _effect()
    before = _snapshot(effect)
    write_set = build_projection_write_set(
        effect,
        before,
        _proofs(effect),
        now=NOW,
    )
    return effect, ProjectionSnapshot(
        write_set.queue,
        before.admission,
        write_set.parents,
        write_set.containers,
        {path: REVISION for path in write_set.containers},
    )


@unittest.skipUnless(TEST_DSN, "TEST_PCF_POSTGRES_DSN is not set")
class PcfEffectSummaryPostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = psycopg2.connect(TEST_DSN)
        try:
            if not apply(conn)["ready"]:
                raise AssertionError("PCF projection schema is not ready")
            if not apply_work_schema(conn)["ready"]:
                raise AssertionError("PCF work schema is not ready")
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
                    """
                    TRUNCATE pcf_work_events, pcf_work_items, due_work,
                             route_dashboard_summaries, items_current,
                             pcf_projection_routes
                    RESTART IDENTITY
                    """
                )
            conn.commit()
        finally:
            conn.close()

    def test_atomic_projection_summary_replay_and_independent_readback(self):
        effect, snapshot = _verified_snapshot()
        writer_conn = self.connect()
        verifier_conn = self.connect()
        try:
            writer = PcfEffectSummaryStore(
                writer_conn,
                actor_id="pcf-effect-worker",
                clock=lambda: NOW,
            )
            verifier = PcfEffectSummaryVerifier(verifier_conn, clock=lambda: NOW)

            first = writer.apply(effect, snapshot)
            checked = verifier.verify(effect, snapshot, first)
            replay = writer.apply(effect, snapshot)

            self.assertEqual(first.disposition, "applied")
            self.assertEqual(checked.disposition, "already_correct")
            self.assertEqual(replay.disposition, "already_correct")
            with writer_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT eligible, serving_enabled, reconciliation_status,
                           healthy_until, last_projected_revision
                    FROM pcf_projection_routes
                    WHERE route_number = %s
                    """,
                    (effect.route,),
                )
                route = cur.fetchone()
                cur.execute(
                    """
                    SELECT source_revision, mirrored_revision, payload
                    FROM route_dashboard_summaries
                    WHERE route_number = %s
                    """,
                    (effect.route,),
                )
                summary = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) AS count FROM items_current WHERE route_number = %s",
                    (effect.route,),
                )
                count = int(cur.fetchone()["count"])
            writer_conn.rollback()

            self.assertFalse(route["eligible"])
            self.assertFalse(route["serving_enabled"])
            self.assertEqual(route["reconciliation_status"], "unknown")
            self.assertIsNone(route["healthy_until"])
            self.assertEqual(route["last_projected_revision"], REVISION)
            self.assertEqual(summary["source_revision"], REVISION)
            self.assertEqual(summary["mirrored_revision"], REVISION)
            self.assertEqual(summary["payload"]["routeNumber"], effect.route)
            self.assertEqual(count, 1)
        finally:
            writer_conn.close()
            verifier_conn.close()

    def test_independent_summary_tamper_is_rejected(self):
        effect, snapshot = _verified_snapshot()
        writer_conn = self.connect()
        verifier_conn = self.connect()
        try:
            proof = PcfEffectSummaryStore(
                writer_conn,
                actor_id="pcf-effect-worker",
                clock=lambda: NOW,
            ).apply(effect, snapshot)
            with writer_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE route_dashboard_summaries
                    SET payload = jsonb_set(payload, '{counts,totalPCFs}', '999'::jsonb)
                    WHERE route_number = %s
                    """,
                    (effect.route,),
                )
            writer_conn.commit()

            with self.assertRaisesRegex(PcfSummaryError, "readback_mismatch"):
                PcfEffectSummaryVerifier(
                    verifier_conn,
                    clock=lambda: NOW,
                ).verify(effect, snapshot, proof)
        finally:
            writer_conn.close()
            verifier_conn.close()


if __name__ == "__main__":
    unittest.main()

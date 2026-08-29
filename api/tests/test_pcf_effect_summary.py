"""Hermetic transaction tests for PCF Postgres/dashboard finalization."""

from __future__ import annotations

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from order_forecast.api.pcf_effect_projection import (
    ProjectionSnapshot,
    build_projection_write_set,
)
from order_forecast.api.pcf_effect_summary import (
    PcfEffectSummaryStore,
    PcfEffectSummaryVerifier,
    PcfSummaryError,
    _events,
)
from order_forecast.api.pcf_event_contract import projection_event_business_hash
from order_forecast.api.tests.test_pcf_effect_projection import (
    NOW,
    _effect,
    _proofs,
    _snapshot,
)


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
    revisions = {path: REVISION for path in write_set.containers}
    return effect, ProjectionSnapshot(
        write_set.queue,
        before.admission,
        write_set.parents,
        write_set.containers,
        revisions,
    )


class _State:
    def __init__(self, effect, snapshot):
        event = _events(effect, snapshot)[0]
        payload = event["payload"]
        self.rows = [
            {
                "route_number": effect.route,
                "delivery_id": event["entity"]["delivery"],
                "container_id": event["entity"]["container"],
                "active": payload["active"],
                "loading_date": payload["loadingDate"],
                "page_numbers": payload["pageNumbers"],
                "items": payload["items"],
                "source_revision": event["sourceRevision"],
                "source_business_hash": projection_event_business_hash(event),
                "updated_at": NOW,
            }
        ]
        self.route_revision = REVISION
        self.summary = None


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split()).lower()
        self.connection.statements.append(normalized)
        self.result = None
        if normalized.startswith("select last_projected_revision"):
            self.result = {"last_projected_revision": self.connection.state.route_revision}
            return
        if normalized.startswith("select route_number, delivery_id"):
            self.result = copy.deepcopy(self.connection.state.rows)
            return
        if normalized.startswith("select route_number, payload"):
            self.result = copy.deepcopy(self.connection.state.summary)
            return
        if normalized.startswith("insert into route_dashboard_summaries"):
            payload = getattr(params[1], "adapted", params[1])
            self.connection.state.summary = {
                "route_number": params[0],
                "payload": copy.deepcopy(payload),
                "source_revision": params[2],
                "source_updated_at": params[3],
                "mirrored_revision": params[4],
                "mirrored_at": params[5],
                "updated_at": params[6],
            }
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self):
        if isinstance(self.result, list):
            return self.result[0] if self.result else None
        return self.result

    def fetchall(self):
        return self.result or []


class _Connection:
    def __init__(self, state):
        self.state = state
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args, **kwargs):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class PcfEffectSummaryTests(unittest.TestCase):
    def test_events_use_verified_firestore_revision_and_shared_normalizer(self):
        effect, snapshot = _verified_snapshot()

        event = _events(effect, snapshot)[0]

        self.assertEqual(event["sourceRevision"], REVISION)
        self.assertEqual(event["sourcePath"], next(iter(snapshot.containers)))
        self.assertEqual(event["payload"]["pageNumbers"], [1])
        self.assertEqual(event["payload"]["items"][0]["expiryDate"], "2026-08-31")

    def test_apply_then_independent_verify_is_replayable(self):
        effect, snapshot = _verified_snapshot()
        state = _State(effect, snapshot)
        writer_conn = _Connection(state)
        verifier_conn = _Connection(state)
        dispositions = iter(("applied", "duplicate_noop"))

        def commit_event(*_args, **_kwargs):
            disposition = next(dispositions)
            return SimpleNamespace(
                disposition=disposition,
                duplicate_transport_event=disposition == "duplicate_noop",
            )

        with patch(
            "order_forecast.api.pcf_effect_summary.commit_projection_event",
            side_effect=commit_event,
        ), patch("order_forecast.api.pcf_effect_summary.lock_lifecycle_route"):
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
        self.assertEqual(first.summary_sha256, checked.summary_sha256)
        self.assertEqual(state.summary["source_revision"], REVISION)
        self.assertEqual(state.summary["mirrored_revision"], REVISION)
        self.assertEqual(writer_conn.commits, 2)
        self.assertEqual(verifier_conn.commits, 1)
        self.assertFalse(
            any("serving_enabled" in statement for statement in writer_conn.statements)
        )

    def test_verifier_accepts_a_strictly_newer_projection_but_requires_current_summary(self):
        effect, snapshot = _verified_snapshot()
        state = _State(effect, snapshot)
        writer_conn = _Connection(state)
        verifier_conn = _Connection(state)
        with patch(
            "order_forecast.api.pcf_effect_summary.commit_projection_event",
            return_value=SimpleNamespace(
                disposition="applied",
                duplicate_transport_event=False,
            ),
        ), patch("order_forecast.api.pcf_effect_summary.lock_lifecycle_route"):
            expected = PcfEffectSummaryStore(
                writer_conn,
                actor_id="pcf-effect-worker",
                clock=lambda: NOW,
            ).apply(effect, snapshot)
            newer = "fsut1|2026-08-25T18:01:00.000000000Z|0"
            state.rows[0]["source_revision"] = newer
            state.route_revision = newer
            state.summary = None
            with self.assertRaisesRegex(PcfSummaryError, "readback_mismatch"):
                PcfEffectSummaryVerifier(
                    verifier_conn,
                    clock=lambda: NOW,
                ).verify(effect, snapshot, expected)

        self.assertEqual(verifier_conn.rollbacks, 1)

    def test_missing_firestore_revision_fails_before_postgres(self):
        effect, snapshot = _verified_snapshot()
        state = _State(effect, snapshot)
        connection = _Connection(state)
        missing = ProjectionSnapshot(
            snapshot.queue,
            snapshot.admission,
            snapshot.parents,
            snapshot.containers,
            {},
        )

        with self.assertRaisesRegex(PcfSummaryError, "revision_missing"):
            PcfEffectSummaryStore(
                connection,
                actor_id="pcf-effect-worker",
                clock=lambda: NOW,
            ).apply(effect, missing)

        self.assertEqual(connection.statements, [])
        self.assertEqual(connection.commits, 0)


if __name__ == "__main__":
    unittest.main()

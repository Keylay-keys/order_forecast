from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from order_forecast.api.pcf_event_contract import business_hash, firestore_revision
from order_forecast.api.pcf_projection_reconciliation import (
    ExactFirestoreAbsenceProof,
    MAX_ROUTE_ENTITIES,
    ProjectionReconciliationError,
    RouteEntityKey,
    build_absence_tombstone,
    compare_route_checksums,
    summarize_projection_rows,
    summarize_source_route,
    targeted_repair_events,
)
from order_forecast.api.pcf_projection_reconciliation_runtime import (
    ProjectionReconciliationRuntime,
)


ROUTE = "988200"
SNAPSHOT = firestore_revision("2026-08-28T19:00:00.000000000Z", "upsert")


def event(
    delivery: str,
    container: str,
    *,
    product: str = "1001",
    revision: str | None = None,
    excluded: bool = False,
) -> dict:
    return {
        "schemaVersion": 1,
        "sourceEventId": f"reconcile:{ROUTE}:{delivery}:{container}:0001",
        "eventType": "projection.pcf_container.changed",
        "mutationKind": "upsert",
        "occurredAt": "2026-08-28T18:00:00.000000000Z",
        "sourcePath": f"routes/{ROUTE}/pcfs/{delivery}/containers/{container}",
        "sourceRevision": revision
        or firestore_revision("2026-08-28T18:00:00.000000000Z", "upsert"),
        "entity": {"route": ROUTE, "delivery": delivery, "container": container},
        "automationExcluded": excluded,
        "fixture": (
            {"identity": "reconcile-fixture", "generation": "one"}
            if excluded
            else None
        ),
        "payload": {
            "active": True,
            "loadingDate": "2026-08-28",
            "pageNumbers": [1],
            "items": [
                {
                    "pageNumber": 1,
                    "itemPosition": 0,
                    "product": product,
                    "description": "Item",
                    "expiryDate": "2026-09-01",
                    "visibleOnList": True,
                    "isLowQuantity": False,
                    "isShortCoded": False,
                    "guaranteed": None,
                }
            ],
        },
    }


def rows(source):
    output = []
    for entity in source.source_entities:
        payload = deepcopy(entity.event["payload"])
        output.append({
            "route_number": ROUTE,
            "delivery_id": entity.key.delivery_id,
            "container_id": entity.key.container_id,
            "active": payload["active"],
            "loading_date": payload["loadingDate"],
            "page_numbers": payload["pageNumbers"],
            "items": payload["items"],
            "source_business_hash": entity.business_hash,
        })
    return output


class PcfProjectionReconciliationTests(unittest.TestCase):
    def test_order_independent_match_and_fixture_suppression(self):
        source = summarize_source_route(
            ROUTE,
            SNAPSHOT,
            [event("D2", "C2"), event("D1", "C1"), event("FIX", "C9", excluded=True)],
        )
        projection = summarize_projection_rows(ROUTE, list(reversed(rows(source))))
        result = compare_route_checksums(source, projection)
        self.assertEqual(source.entity_count, 2)
        self.assertEqual(result.status, "matched")
        self.assertEqual(result.reason, "matched")
        self.assertEqual(targeted_repair_events(result), ())

    def test_exact_mismatch_classes_and_targeted_upserts(self):
        source = summarize_source_route(
            ROUTE,
            SNAPSHOT,
            [event("D1", "C1"), event("D2", "C2", product="new")],
        )
        projection_rows = rows(source)
        projection_rows[1]["source_business_hash"] = business_hash({"old": True})
        projection_rows.append(
            {
                "route_number": ROUTE,
                "delivery_id": "D3",
                "container_id": "C3",
                "active": True,
                "loading_date": "2026-08-28",
                "page_numbers": [],
                "items": [],
                "source_business_hash": business_hash(
                    {
                        "entity": {"route": ROUTE, "delivery": "D3", "container": "C3"},
                        "payload": {
                            "active": True,
                            "loadingDate": "2026-08-28",
                            "pageNumbers": [],
                            "items": [],
                        },
                    }
                ),
            }
        )
        projection_rows.pop(0)
        result = compare_route_checksums(
            source,
            summarize_projection_rows(ROUTE, projection_rows),
        )
        self.assertEqual(result.status, "mismatched")
        self.assertEqual(result.missing_in_projection, (RouteEntityKey("D1", "C1"),))
        self.assertEqual(result.changed_in_projection, (RouteEntityKey("D2", "C2"),))
        self.assertEqual(result.extra_in_projection, (RouteEntityKey("D3", "C3"),))
        repairs = targeted_repair_events(result)
        self.assertEqual(
            [(row["entity"]["delivery"], row["entity"]["container"]) for row in repairs],
            [("D1", "C1"), ("D2", "C2")],
        )

    def test_projection_ahead_requires_fresh_snapshot_and_no_repair(self):
        source = summarize_source_route(ROUTE, SNAPSHOT, [event("D1", "C1")])
        result = compare_route_checksums(
            source,
            summarize_projection_rows(ROUTE, rows(source)),
            projection_last_revision=firestore_revision(
                "2026-08-28T20:00:00.000000000Z", "upsert"
            ),
        )
        self.assertEqual(result.reason, "projection_ahead_of_snapshot")
        self.assertEqual(targeted_repair_events(result), ())

    def test_extra_projection_requires_exact_absence_proof_for_tombstone(self):
        key = RouteEntityKey("D1", "C1")
        path = f"routes/{ROUTE}/pcfs/D1/containers/C1"
        proof = ExactFirestoreAbsenceProof(
            source_path=path,
            read_time="2026-08-28T20:00:00Z",
            exists=False,
        )
        tombstone = build_absence_tombstone(ROUTE, key, proof)
        self.assertEqual(tombstone["sourcePath"], path)
        self.assertEqual(tombstone["mutationKind"], "delete")
        self.assertEqual(tombstone["payload"], None)
        self.assertEqual(
            tombstone["sourceRevision"],
            firestore_revision("2026-08-28T20:00:00Z", "delete"),
        )
        recreated = firestore_revision("2026-08-28T20:00:01Z", "upsert")
        self.assertLess(
            tombstone["sourceRevision"],
            recreated,
        )
        self.assertEqual(tombstone, build_absence_tombstone(ROUTE, key, proof))

        with self.assertRaisesRegex(ProjectionReconciliationError, "absence_not_proven"):
            build_absence_tombstone(
                ROUTE,
                key,
                ExactFirestoreAbsenceProof(path, proof.read_time, True),
            )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "absence_proof_path_mismatch"
        ):
            build_absence_tombstone(
                ROUTE,
                key,
                ExactFirestoreAbsenceProof(
                    f"routes/{ROUTE}/pcfs/D2/containers/C1",
                    proof.read_time,
                    False,
                ),
            )

    def test_actual_projection_payload_and_stored_hash_must_both_match(self):
        source = summarize_source_route(ROUTE, SNAPSHOT, [event("D1", "C1")])
        corrupted_payload = rows(source)
        corrupted_payload[0]["items"][0]["product"] = "corrupted"
        payload_result = compare_route_checksums(
            source,
            summarize_projection_rows(ROUTE, corrupted_payload),
        )
        self.assertEqual(payload_result.reason, "projection_integrity_mismatch")
        self.assertEqual(
            payload_result.changed_in_projection,
            (RouteEntityKey("D1", "C1"),),
        )

        corrupted_hash = rows(source)
        corrupted_hash[0]["source_business_hash"] = "f" * 64
        hash_result = compare_route_checksums(
            source,
            summarize_projection_rows(ROUTE, corrupted_hash),
        )
        self.assertEqual(hash_result.reason, "projection_integrity_mismatch")
        self.assertEqual(len(targeted_repair_events(hash_result)), 1)

    def test_invalid_source_boundaries_fail_closed(self):
        with self.assertRaisesRegex(ProjectionReconciliationError, "source_entity_duplicate"):
            summarize_source_route(ROUTE, SNAPSHOT, [event("D1", "C1"), event("D1", "C1")])
        with self.assertRaisesRegex(ProjectionReconciliationError, "source_event_after_snapshot"):
            summarize_source_route(
                ROUTE,
                SNAPSHOT,
                [
                    event(
                        "D1",
                        "C1",
                        revision=firestore_revision(
                            "2026-08-28T20:00:00.000000000Z", "upsert"
                        ),
                    )
                ],
            )

    def test_one_failed_repair_does_not_rollback_an_unrelated_repair(self):
        source = summarize_source_route(
            ROUTE,
            SNAPSHOT,
            [event("D1", "C1"), event("D2", "C2")],
        )
        result = compare_route_checksums(
            source,
            summarize_projection_rows(ROUTE, []),
        )

        class Connection:
            def __init__(self):
                self.commits = 0
                self.rollbacks = 0

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        connections = []

        def provide():
            connection = Connection()
            connections.append(connection)
            return connection

        runtime = ProjectionReconciliationRuntime(
            connection_provider=provide,
            connection_releaser=lambda _connection: None,
            source_adapter=object(),
        )
        with patch(
            "order_forecast.api.pcf_projection_reconciliation_runtime.commit_projection_event",
            side_effect=[RuntimeError("first repair failed"), object()],
        ) as commit:
            with self.assertRaisesRegex(
                ProjectionReconciliationError,
                "projection_repair_failed",
            ):
                runtime._commit_repairs(
                    result,
                    datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(commit.call_count, 2)
        self.assertEqual(
            [(connection.commits, connection.rollbacks) for connection in connections],
            [(0, 1), (1, 0)],
        )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "snapshot_revision_must_be_read_boundary"
        ):
            summarize_source_route(
                ROUTE,
                firestore_revision("2026-08-28T19:00:00.000000000Z", "delete"),
                [],
            )

    def test_source_and_projection_limits_are_hard(self):
        source_events = [event(f"D{index}", "C1") for index in range(MAX_ROUTE_ENTITIES + 1)]
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "source_route_entity_limit_exceeded"
        ):
            summarize_source_route(ROUTE, SNAPSHOT, source_events)
        projection_rows = [
            {
                "route_number": ROUTE,
                "delivery_id": f"D{index}",
                "container_id": "C1",
                "source_business_hash": "a" * 64,
            }
            for index in range(MAX_ROUTE_ENTITIES + 1)
        ]
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "projection_route_entity_limit_exceeded"
        ):
            summarize_projection_rows(ROUTE, projection_rows)


if __name__ == "__main__":
    unittest.main()

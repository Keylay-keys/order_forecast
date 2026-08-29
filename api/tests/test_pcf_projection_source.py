from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest

from order_forecast.api.pcf_projection_reconciliation import (
    ProjectionReconciliationError,
    RouteEntityKey,
)
from order_forecast.api.pcf_projection_source import (
    FirestoreProjectionSourceAdapter,
    build_reconciliation_source_event,
    load_production_fixture_registry,
)


ROUTE = "988200"
READ_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
UPDATE_TIME = datetime(2026, 8, 28, 19, 0, tzinfo=timezone.utc)


def container_data(**overrides):
    value = {
        "pages": [
            {
                "pageNumber": 1,
                "items": [
                    {
                        "product": " 1001 ",
                        "description": " Item ",
                        "bestBefore": "09/01/2026",
                        "days": "3*",
                    }
                ],
            }
        ]
    }
    value.update(overrides)
    return value


class FakeSnapshot:
    def __init__(
        self,
        document_id,
        data,
        *,
        read_time=READ_TIME,
        update_time=UPDATE_TIME,
        exists=True,
        reference=None,
    ):
        self.id = document_id
        self._data = data
        self.read_time = read_time
        self.update_time = update_time
        self.exists = exists
        self.reference = reference

    def to_dict(self):
        return self._data


class FakeQuery:
    def __init__(self, snapshots):
        self._snapshots = snapshots
        self._limit = None

    def order_by(self, _field):
        return self

    def limit(self, value):
        self._limit = value
        return self

    def stream(self, **_kwargs):
        rows = self._snapshots
        return iter(rows if self._limit is None else rows[: self._limit])


class FakeReference:
    def __init__(self, path, snapshot=None, collections=None):
        self.path = path
        self._snapshot = snapshot
        self._collections = collections or {}

    def get(self, **_kwargs):
        return self._snapshot

    def collection(self, name):
        return FakeQuery(self._collections.get(name, []))


class FakeDatabase:
    def __init__(self, references):
        self._references = references
        self.requested_paths = []

    def document(self, path):
        self.requested_paths.append(path)
        return self._references[path]


def source_adapter(*, changed_container_read_time=READ_TIME):
    first_ref = FakeReference("routes/988200/pcfs/D1")
    first = FakeSnapshot(
        "D1",
        {"loadingDate": "08/28/2026", "active": True},
        reference=first_ref,
    )
    first_ref._collections = {
        "containers": [
            FakeSnapshot(
                "C1",
                container_data(),
                read_time=changed_container_read_time,
                reference=FakeReference("routes/988200/pcfs/D1/containers/C1"),
            )
        ]
    }
    fixture_ref = FakeReference("routes/988200/pcfs/FIX")
    fixture_marker = {"identity": "fixture-one", "generation": "1"}
    fixture = FakeSnapshot(
        "FIX",
        {"automationExcluded": True, "fixture": fixture_marker},
        reference=fixture_ref,
    )
    fixture_ref._collections = {
        "containers": [
            FakeSnapshot(
                "FX1",
                container_data(
                    automationExcluded=True,
                    fixture=fixture_marker,
                ),
                reference=FakeReference("routes/988200/pcfs/FIX/containers/FX1"),
            )
        ]
    }
    route_ref = FakeReference("routes/988200")
    route = FakeSnapshot("988200", {}, reference=route_ref)
    route_ref._snapshot = route
    route_ref._collections = {"pcfs": [first, fixture]}
    missing = FakeSnapshot(
        "MISSING",
        None,
        exists=False,
        reference=FakeReference("routes/988200/pcfs/D2/containers/C2"),
    )
    missing_ref = FakeReference(
        "routes/988200/pcfs/D2/containers/C2",
        snapshot=missing,
    )
    database = FakeDatabase(
        {
            "routes/988200": route_ref,
            "routes/988200/pcfs/D2/containers/C2": missing_ref,
        }
    )
    return FirestoreProjectionSourceAdapter(
        database,
        transaction_runner=lambda read: read(object()),
    ), database


class PcfProjectionSourceTests(unittest.TestCase):
    def test_signed_exact_registry_admits_only_observed_legacy_fixture_shape(self):
        registry = load_production_fixture_registry(
            Path("order_forecast/contracts/pcf/fixture-identity-v1.json"),
            production_project="routespark-1f47d",
        )
        exact = registry[(ROUTE, "1806116556", "100737310336040934")]
        parent = {
            "metadata": {"automationExcluded": True, "fixture": True},
            "loadingDate": "08/28/2026",
        }
        result = build_reconciliation_source_event(
            route_number=ROUTE,
            delivery_id="1806116556",
            container_id="100737310336040934",
            parent_data=parent,
            container_data=container_data(),
            container_update_time=UPDATE_TIME,
            snapshot_read_time=READ_TIME,
            exact_fixture=exact,
        )
        self.assertTrue(result["automationExcluded"])
        self.assertEqual(
            result["fixture"],
            {
                "identity": "ocr-correction-988200-1806116556",
                "generation": "legacy-v1",
            },
        )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "fixture_registry_state_mismatch"
        ):
            build_reconciliation_source_event(
                route_number=ROUTE,
                delivery_id="1806116556",
                container_id="100737310336040934",
                parent_data={"loadingDate": "08/28/2026"},
                container_data=container_data(),
                container_update_time=UPDATE_TIME,
                snapshot_read_time=READ_TIME,
                exact_fixture=exact,
            )

    def test_event_normalization_matches_trigger_business_fields(self):
        event = build_reconciliation_source_event(
            route_number=ROUTE,
            delivery_id="D1",
            container_id="C1",
            parent_data={"loadingDate": "08/28/2026", "active": True},
            container_data=container_data(active=True),
            container_update_time=UPDATE_TIME,
            snapshot_read_time=READ_TIME,
        )
        self.assertEqual(event["payload"]["loadingDate"], "2026-08-28")
        self.assertEqual(event["payload"]["items"][0]["product"], "1001")
        self.assertEqual(event["payload"]["items"][0]["expiryDate"], "2026-09-01")
        self.assertTrue(event["payload"]["items"][0]["isShortCoded"])
        self.assertEqual(
            event,
            build_reconciliation_source_event(
                route_number=ROUTE,
                delivery_id="D1",
                container_id="C1",
                parent_data={"loadingDate": "08/28/2026", "active": True},
                container_data=container_data(active=True),
                container_update_time=UPDATE_TIME,
                snapshot_read_time=READ_TIME,
            ),
        )

    def test_invalid_item_expiry_is_null_but_invalid_loading_date_fails(self):
        malformed_item = container_data()
        malformed_item["pages"][0]["items"][0]["bestBefore"] = "not-a-date"
        event = build_reconciliation_source_event(
            route_number=ROUTE,
            delivery_id="D1",
            container_id="C1",
            parent_data={"loadingDate": "08/28/2026"},
            container_data=malformed_item,
            container_update_time=UPDATE_TIME,
            snapshot_read_time=READ_TIME,
        )
        self.assertIsNone(event["payload"]["items"][0]["expiryDate"])
        with self.assertRaisesRegex(ProjectionReconciliationError, "loading_date_invalid"):
            build_reconciliation_source_event(
                route_number=ROUTE,
                delivery_id="D1",
                container_id="C1",
                parent_data={"loadingDate": "not-a-date"},
                container_data=container_data(),
                container_update_time=UPDATE_TIME,
                snapshot_read_time=READ_TIME,
            )

    def test_route_read_is_bounded_consistent_and_suppresses_exact_fixture(self):
        adapter, _database = source_adapter()
        result = adapter.read_route(ROUTE)
        self.assertEqual(result.source.entity_count, 1)
        self.assertEqual(result.route_reads, 1)
        self.assertEqual(result.delivery_reads, 2)
        self.assertEqual(result.container_reads, 2)
        self.assertEqual(result.query_count, 3)
        self.assertEqual(result.minimum_billed_document_reads, 5)
        self.assertEqual(result.source.source_entities[0].key, RouteEntityKey("D1", "C1"))

    def test_changed_transaction_read_time_fails_closed(self):
        adapter, _database = source_adapter(
            changed_container_read_time=datetime(
                2026, 8, 28, 20, 0, 1, tzinfo=timezone.utc
            )
        )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "source_snapshot_read_time_changed"
        ):
            adapter.read_route(ROUTE)

    def test_zero_result_container_query_is_included_in_billed_minimum(self):
        parent_ref = FakeReference(f"routes/{ROUTE}/pcfs/D1")
        parent_ref._collections = {"containers": []}
        parent = FakeSnapshot("D1", {}, reference=parent_ref)
        route_ref = FakeReference(f"routes/{ROUTE}")
        route_ref._snapshot = FakeSnapshot(ROUTE, {}, reference=route_ref)
        route_ref._collections = {"pcfs": [parent]}
        adapter = FirestoreProjectionSourceAdapter(
            FakeDatabase({f"routes/{ROUTE}": route_ref}),
            transaction_runner=lambda read: read(object()),
        )
        result = adapter.read_route(ROUTE)
        self.assertEqual(result.route_reads + result.delivery_reads + result.container_reads, 2)
        self.assertEqual(result.minimum_billed_document_reads, 3)
        self.assertEqual(result.query_count, 2)

    def test_exact_absence_path_is_constructed_from_validated_identity(self):
        adapter, database = source_adapter()
        proof = adapter.read_exact_absence(ROUTE, RouteEntityKey("D2", "C2"))
        self.assertFalse(proof.exists)
        self.assertEqual(
            proof.source_path,
            "routes/988200/pcfs/D2/containers/C2",
        )
        self.assertEqual(database.requested_paths[-1], proof.source_path)
        with self.assertRaisesRegex(ProjectionReconciliationError, "absence_identity_invalid"):
            adapter.read_exact_absence(ROUTE, RouteEntityKey("../users", "C2"))
        self.assertNotIn("routes/988200/pcfs/../users/containers/C2", database.requested_paths)

    def test_invalid_route_is_rejected_before_firestore_access(self):
        adapter, database = source_adapter()
        with self.assertRaisesRegex(ProjectionReconciliationError, "route_invalid"):
            adapter.read_route("988200/users")
        self.assertEqual(database.requested_paths, [])

    def test_delivery_and_container_limits_stop_route_reads(self):
        parent_rows = []
        for index in range(501):
            parent_ref = FakeReference(f"routes/{ROUTE}/pcfs/D{index}")
            parent_rows.append(
                FakeSnapshot(
                    f"D{index}",
                    {},
                    reference=parent_ref,
                )
            )
        route_ref = FakeReference(f"routes/{ROUTE}")
        route_ref._snapshot = FakeSnapshot(ROUTE, {}, reference=route_ref)
        route_ref._collections = {"pcfs": parent_rows}
        adapter = FirestoreProjectionSourceAdapter(
            FakeDatabase({f"routes/{ROUTE}": route_ref}),
            transaction_runner=lambda read: read(object()),
        )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "source_route_delivery_limit_exceeded"
        ):
            adapter.read_route(ROUTE)

        parent_ref = FakeReference(f"routes/{ROUTE}/pcfs/D1")
        parent_ref._collections = {
            "containers": [
                FakeSnapshot(
                    f"C{index}",
                    {"pages": []},
                    reference=FakeReference(
                        f"routes/{ROUTE}/pcfs/D1/containers/C{index}"
                    ),
                )
                for index in range(501)
            ]
        }
        parent = FakeSnapshot("D1", {}, reference=parent_ref)
        route_ref = FakeReference(f"routes/{ROUTE}")
        route_ref._snapshot = FakeSnapshot(ROUTE, {}, reference=route_ref)
        route_ref._collections = {"pcfs": [parent]}
        adapter = FirestoreProjectionSourceAdapter(
            FakeDatabase({f"routes/{ROUTE}": route_ref}),
            transaction_runner=lambda read: read(object()),
        )
        with self.assertRaisesRegex(
            ProjectionReconciliationError, "source_route_entity_limit_exceeded"
        ):
            adapter.read_route(ROUTE)

    def test_maximum_successful_parent_container_topology_prices_empty_queries(self):
        parent_rows = []
        for parent_index in range(500):
            parent_ref = FakeReference(f"routes/{ROUTE}/pcfs/D{parent_index:03d}")
            parent_ref._collections = {
                "containers": (
                    [
                        FakeSnapshot(
                            f"C{container_index:03d}",
                            {"pages": []},
                            reference=FakeReference(
                                f"routes/{ROUTE}/pcfs/D000/containers/C{container_index:03d}"
                            ),
                        )
                        for container_index in range(500)
                    ]
                    if parent_index == 0
                    else []
                )
            }
            parent_rows.append(
                FakeSnapshot(
                    f"D{parent_index:03d}",
                    {},
                    reference=parent_ref,
                )
            )
        route_ref = FakeReference(f"routes/{ROUTE}")
        route_ref._snapshot = FakeSnapshot(ROUTE, {}, reference=route_ref)
        route_ref._collections = {"pcfs": parent_rows}
        adapter = FirestoreProjectionSourceAdapter(
            FakeDatabase({f"routes/{ROUTE}": route_ref}),
            transaction_runner=lambda read: read(object()),
        )

        result = adapter.read_route(ROUTE)

        self.assertEqual(result.source.entity_count, 500)
        self.assertEqual(result.route_reads, 1)
        self.assertEqual(result.delivery_reads, 500)
        self.assertEqual(result.container_reads, 500)
        self.assertEqual(result.query_count, 501)
        # One route read + 500 returned parents + 500 returned containers +
        # one minimum billed read for each of the other 499 empty child queries.
        self.assertEqual(result.minimum_billed_document_reads, 1500)


if __name__ == "__main__":
    unittest.main()

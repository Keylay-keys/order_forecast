from __future__ import annotations

import inspect
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from order_forecast.scripts import order_adjustment_scheduled_day_migration as migration
from order_forecast.scripts import order_archive_receipt


class _Snapshot:
    def __init__(self, ref):
        self.reference = ref
        self.id = ref.path.rsplit("/", 1)[-1]
        self.exists = ref.data is not None

    def to_dict(self):
        return dict(self.reference.data or {})


class _Ref:
    def __init__(self, path, data):
        self.path = path
        self.id = path.rsplit("/", 1)[-1]
        self.data = None if data is None else dict(data)

    def get(self, transaction=None):
        return _Snapshot(self)


class _Transaction:
    def update(self, ref, fields):
        ref.data.update(fields)


class _Db:
    def __init__(self, documents):
        self.refs = {path: _Ref(path, data) for path, data in documents.items()}

    def document(self, path):
        return self.refs.setdefault(path, _Ref(path, None))

    def transaction(self):
        return _Transaction()


class ScheduledOrderDayMigrationTests(unittest.TestCase):
    def setUp(self):
        self.context = {
            "status": "ready",
            "timezone": "America/Denver",
            "cycles": [{
                "orderDay": 2,
                "loadDay": 4,
                "deliveryDay": 4,
                "loadOffsetDays": 2,
                "deliveryOffsetDays": 2,
                "scheduleVersion": 2,
            }],
        }
        self.source = {
            "routeNumber": "988200",
            "status": "finalized",
            "scheduleKey": "tuesday",
            "expectedDeliveryDate": "2026-08-20",
        }
        self.stamp = {
            "version": 2,
            "date": "2026-08-18",
            "timezone": "America/Denver",
            "scheduleKey": "tuesday",
            "orderDay": 2,
            "loadOffsetDays": 2,
            "deliveryOffsetDays": 2,
            "provenance": "backfill",
        }
        self.adjustment = {
            "status": "draft",
            "sourceOrderId": "order-988200",
            "reminderStatus": "skipped",
            "reminderSkipReason": "reminder_time_passed",
            "lines": [{"sap": "100", "direction": "add", "caseQuantity": 1}],
        }
        self.settings = {
            "reminderEnabled": True,
            "cutoffTimeLocal": "11:00",
            "reminderLeadMinutes": 15,
        }

    def test_unique_delivery_round_trip_builds_exact_backfill_stamp(self):
        stamp, classification = migration.reconstruct_stamp(self.source, self.context)
        self.assertEqual(classification, "reconstructable")
        self.assertEqual(stamp, self.stamp)
        self.assertTrue(migration.is_v2_stamp(stamp))

    def test_duplicate_cycle_and_off_cycle_fail_closed(self):
        _, duplicate = migration.reconstruct_stamp(
            self.source,
            {**self.context, "cycles": self.context["cycles"] * 2},
        )
        self.assertEqual(duplicate, "ambiguous")
        _, off_cycle = migration.reconstruct_stamp(
            {**self.source, "scheduleKey": "monday"},
            self.context,
        )
        self.assertEqual(off_cycle, "off_cycle")

    def test_route_authority_conflict_inside_one_document_fails_closed(self):
        db = _Db({
            "routes/988200": {"ownerUid": "owner-a", "userId": "owner-b"},
        })
        context = migration.load_route_context(db, "988200")
        self.assertEqual(context["status"], "missing_owner")
        self.assertEqual(context["reason"], "owner_sources_disagree")

    def test_exact_stamp_shape_rejects_partial_or_extra_fields(self):
        self.assertFalse(migration.is_v2_stamp({"version": 2, "date": "2026-08-18"}))
        self.assertFalse(migration.is_v2_stamp({**self.stamp, "extra": True}))
        self.assertFalse(migration.is_v2_stamp({**self.stamp, "deliveryOffsetDays": 1}))
        self.assertFalse(migration.is_v2_stamp({**self.stamp, "date": "not-a-date"}))
        self.assertFalse(migration.is_v2_stamp({**self.stamp, "timezone": "Mars/Olympus"}))
        self.assertFalse(migration.is_v2_stamp({**self.stamp, "scheduleKey": "monday"}))

    def test_present_invalid_stamp_is_reported_and_never_reconstructed(self):
        for existing in (None, {}, {"version": 1}, {**self.stamp, "date": "bad"}):
            with self.subTest(existing=existing):
                stamp, classification = migration.reconstruct_stamp(
                    {**self.source, "scheduledOrder": existing},
                    self.context,
                )
                self.assertIsNone(stamp)
                self.assertEqual(classification, "invalid_existing_stamp")

    def test_finalized_scope_uses_stamp_timezone_and_delivery_boundary(self):
        now_ms = int(
            datetime(2026, 8, 19, 12, 0, tzinfo=ZoneInfo("America/Denver")).timestamp()
            * 1000
        )
        self.assertTrue(migration._order_in_scope(
            {**self.source, "expectedDeliveryDate": "2026-08-19"},
            self.stamp,
            now_ms,
        ))
        self.assertTrue(migration._order_in_scope(
            {**self.source, "expectedDeliveryDate": "2026-08-20"},
            self.stamp,
            now_ms,
        ))
        self.assertFalse(migration._order_in_scope(
            {**self.source, "expectedDeliveryDate": "2026-08-18"},
            self.stamp,
            now_ms,
        ))
        self.assertFalse(migration._order_in_scope(self.source, None, now_ms))
        self.assertTrue(migration._order_in_scope(
            {**self.source, "status": "draft"},
            None,
            now_ms,
        ))

    def test_late_open_rearm_keeps_original_due_timestamp_pending(self):
        source = {**self.source, "scheduledOrder": self.stamp}
        now_ms = int(
            datetime(2026, 8, 19, 10, 50, tzinfo=ZoneInfo("America/Denver")).timestamp()
            * 1000
        )
        classification, fields, side = migration._adjustment_candidate(
            self.adjustment,
            source,
            self.stamp,
            self.settings,
            now_ms,
        )
        self.assertEqual(classification, "rearmed")
        self.assertEqual(side, "already_due")
        self.assertEqual(fields["reminderStatus"], "pending")
        self.assertLess(fields["reminderAtMs"], now_ms)
        self.assertGreater(fields["cutoffAtMs"], now_ms)

    def test_exact_cutoff_expires_recoverable_but_never_rearms_past_cutoff(self):
        source = {**self.source, "scheduledOrder": self.stamp}
        cutoff_ms = int(
            datetime(2026, 8, 19, 11, 0, tzinfo=ZoneInfo("America/Denver")).timestamp()
            * 1000
        )
        classification, fields, side = migration._adjustment_candidate(
            self.adjustment,
            source,
            self.stamp,
            self.settings,
            cutoff_ms,
        )
        self.assertEqual((classification, side), ("expired_patched", "expired"))
        self.assertEqual(fields["reminderSkipReason"], "past_cutoff")

        untouched = {**self.adjustment, "reminderSkipReason": "past_cutoff"}
        classification, fields, side = migration._adjustment_candidate(
            untouched, source, self.stamp, self.settings, cutoff_ms
        )
        self.assertEqual(classification, "past_cutoff_untouched")
        self.assertIsNone(fields)
        self.assertIsNone(side)

    def test_989262_incident_resolves_tuesday_then_expires_wednesday(self):
        context = {
            "status": "ready",
            "timezone": "America/Denver",
            "cycles": [{
                "orderDay": 2,
                "loadDay": 7,
                "deliveryDay": 1,
                "loadOffsetDays": 5,
                "deliveryOffsetDays": 6,
                "scheduleVersion": 2,
            }],
        }
        source = {
            "routeNumber": "989262",
            "status": "finalized",
            "scheduleKey": "tuesday",
            "expectedDeliveryDate": "2026-08-31",
        }
        stamp, classification = migration.reconstruct_stamp(source, context)
        self.assertEqual(classification, "reconstructable")
        self.assertEqual(stamp["date"], "2026-08-25")
        now_ms = int(
            datetime(2026, 8, 28, 6, 47, tzinfo=ZoneInfo("America/Denver")).timestamp()
            * 1000
        )
        candidate, fields, side = migration._adjustment_candidate(
            self.adjustment,
            {**source, "scheduledOrder": stamp},
            stamp,
            self.settings,
            now_ms,
        )
        self.assertEqual((candidate, side), ("expired_patched", "expired"))
        self.assertEqual(fields["adjustmentDate"], "2026-08-26")
        self.assertEqual(fields["reminderSkipReason"], "past_cutoff")

    def test_claimed_failed_sent_and_terminal_rows_are_untouched(self):
        source = {**self.source, "scheduledOrder": self.stamp}
        for adjustment in (
            {**self.adjustment, "reminderStatus": "sending"},
            {**self.adjustment, "reminderStatus": "failed"},
            {**self.adjustment, "reminderClaimedAtMs": 1},
            {**self.adjustment, "status": "sent"},
        ):
            classification, fields, _ = migration._adjustment_candidate(
                adjustment, source, self.stamp, self.settings, 0
            )
            self.assertIn(classification, {"claimed_untouched", "terminal_untouched"})
            self.assertIsNone(fields)

    def test_proposal_hash_is_stable_and_excludes_stamp_transition_class(self):
        before = {
            "path": "routes/988200/orders/a",
            "classification": "archive_receipt_update",
            "stampClassification": "reconstructable",
            "sourceBasisHash": "basis",
            "proposedStamp": self.stamp,
        }
        after = {**before, "stampClassification": "already_stamped"}
        first = migration._finish_report("archive", ["988200"], [before])
        second = migration._finish_report("archive", ["988200"], [after])
        self.assertEqual(first["proposalHash"], second["proposalHash"])

    def test_apply_requires_reviewed_dry_run_hash(self):
        with self.assertRaises(SystemExit):
            migration.parse_args([
                "--route", "988200", "--phase", "orders", "--apply",
                "--output", "/tmp/out.json",
            ])
        with self.assertRaises(SystemExit):
            migration.parse_args([
                "--route", "988200", "--phase", "orders", "--apply",
                "--expected-dry-run-hash", "reviewed", "--output", "/tmp/out.json",
            ])
        with patch.dict("os.environ", {"FIRESTORE_EMULATOR_HOST": "127.0.0.1:8080"}):
            args = migration.parse_args([
                "--route", "988200", "--phase", "orders", "--apply",
                "--expected-dry-run-hash", "reviewed",
                "--expected-document", "routes/988200/orders/order-1",
                "--output", "/tmp/out.json",
            ])
        self.assertTrue(args.apply)
        self.assertEqual(args.expected_document, ["routes/988200/orders/order-1"])

    def test_production_apply_is_disabled_even_with_exact_review_inputs(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(SystemExit):
                migration.parse_args([
                    "--route", "988200", "--phase", "orders", "--apply",
                    "--expected-dry-run-hash", "reviewed",
                    "--expected-document", "routes/988200/orders/order-1",
                    "--output", "/tmp/out.json",
                ])
            with self.assertRaisesRegex(RuntimeError, "production_migration_disabled"):
                migration.apply_orders(_Db({}), {"entries": []})

    def test_remote_host_cannot_masquerade_as_firestore_emulator(self):
        apply_args = [
            "--route", "988200", "--phase", "orders", "--apply",
            "--expected-dry-run-hash", "reviewed",
            "--expected-document", "routes/988200/orders/order-1",
            "--output", "/tmp/out.json",
        ]
        for emulator_host in (
            "firestore.googleapis.com:443",
            "10.0.0.5:8080",
            "localhost",
            "localhost:not-a-port",
        ):
            with self.subTest(emulator_host=emulator_host):
                with patch.dict(
                    "os.environ", {"FIRESTORE_EMULATOR_HOST": emulator_host}, clear=True
                ):
                    with self.assertRaises(SystemExit):
                        migration.parse_args(apply_args)
                    with self.assertRaisesRegex(
                        RuntimeError, "production_migration_disabled"
                    ):
                        migration.apply_orders(_Db({}), {"entries": []})

    def test_archive_apply_cannot_target_production_postgres(self):
        args = [
            "--route", "988200", "--phase", "archive", "--apply",
            "--expected-dry-run-hash", "reviewed",
            "--expected-document", "routes/988200/orders/order-1",
            "--output", "/tmp/out.json",
        ]
        base = {"FIRESTORE_EMULATOR_HOST": "127.0.0.1:8080"}
        unsafe_configs = (
            base,
            {**base, "POSTGRES_HOST": "db.production", "POSTGRES_DB": "routespark"},
            {
                **base,
                "ORDER_ADJUSTMENT_MIGRATION_TEST_POSTGRES": "true",
                "POSTGRES_HOST": "127.0.0.1",
                "POSTGRES_DB": "routespark",
            },
        )
        for config in unsafe_configs:
            with self.subTest(config=config):
                with patch.dict("os.environ", config, clear=True):
                    with self.assertRaises(SystemExit):
                        migration.parse_args(args)
                    with self.assertRaisesRegex(
                        RuntimeError, "production_migration_disabled"
                    ):
                        migration.apply_archive(object(), {"entries": []}, object())

        safe_test_config = {
            **base,
            "ORDER_ADJUSTMENT_MIGRATION_TEST_POSTGRES": "true",
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_DB": "routespark_order_adjustment_migration_test",
        }
        with patch.dict("os.environ", safe_test_config, clear=True):
            parsed = migration.parse_args(args)
            self.assertTrue(parsed.apply)
            self.assertEqual(migration.apply_archive(object(), {"entries": []}, object()), [])

    def test_all_routes_apply_is_forbidden(self):
        with self.assertRaises(SystemExit):
            migration.parse_args([
                "--all-routes", "--phase", "orders", "--apply",
                "--expected-dry-run-hash", "reviewed",
                "--expected-document", "routes/988200/orders/order-1",
                "--output", "/tmp/out.json",
            ])

    def test_apply_target_paths_are_exact_and_phase_specific(self):
        entries = [
            {"path": "routes/988200/orders/a", "classification": "reconstructable"},
            {"path": "routes/988200/orders/b", "classification": "already_stamped"},
        ]
        report = migration._finish_report("orders", ["988200"], entries)
        self.assertEqual(
            migration._apply_target_paths(report),
            ["routes/988200/orders/a"],
        )

        entries = [
            {
                "path": "routes/988200/orderAdjustments/a",
                "classification": "rearmed",
                "proposedFields": {"x": 1},
            },
            {
                "path": "routes/988200/orderAdjustments/b",
                "classification": "canonical_untouched",
                "proposedFields": None,
            },
        ]
        report = migration._finish_report("adjustments", ["988200"], entries)
        self.assertEqual(
            migration._apply_target_paths(report),
            ["routes/988200/orderAdjustments/a"],
        )

    def test_order_apply_rechecks_route_owner_authority_documents(self):
        route = "988200"
        order_path = f"routes/{route}/orders/order-1"
        owner_path = "users/owner-old"
        owner_data = {
            "profile": {"timezone": self.context["timezone"]},
            "userSettings": {
                "notifications": {"scheduling": {"orderCycles": self.context["cycles"]}}
            },
        }
        order_data = {**self.source, "id": "order-1"}
        expected_owner_state = [
            {"path": f"routes/{route}", "ownerUid": "owner-old", "ownerUids": ["owner-old"]},
            {"path": f"routeEntitlements/{route}", "ownerUid": None, "ownerUids": []},
            {"path": f"routeNumbers/{route}", "ownerUid": None, "ownerUids": []},
        ]
        db = _Db({
            order_path: order_data,
            owner_path: owner_data,
            # Ownership changed after the reviewed dry run.
            f"routes/{route}": {"ownerUid": "owner-new"},
        })
        report = {
            "entries": [{
                "classification": "reconstructable",
                "path": order_path,
                "routeNumber": route,
                "ownerPath": owner_path,
                "ownerSourcePaths": migration._owner_source_paths(route),
                "ownerSourceHash": migration.stable_hash(expected_owner_state),
                "documentHash": migration.stable_hash(order_data),
                "contextHash": migration.stable_hash(
                    migration._context_source(owner_data, "owner-old")
                ),
                "proposedStamp": self.stamp,
            }],
        }

        with patch.dict("os.environ", {"FIRESTORE_EMULATOR_HOST": "127.0.0.1:8080"}):
            with patch.object(migration.gcf_firestore, "transactional", side_effect=lambda fn: fn):
                with self.assertRaisesRegex(RuntimeError, "changed_since_dry_run:route_owner"):
                    migration.apply_orders(db, report)
        self.assertNotIn("scheduledOrder", db.document(order_path).data)

    def test_order_apply_refuses_even_null_existing_stamp(self):
        route = "988200"
        order_path = f"routes/{route}/orders/order-1"
        owner_path = "users/owner-old"
        owner_data = {
            "profile": {"timezone": self.context["timezone"]},
            "userSettings": {
                "notifications": {"scheduling": {"orderCycles": self.context["cycles"]}}
            },
        }
        order_data = {**self.source, "id": "order-1", "scheduledOrder": None}
        owner_state = [
            {"path": f"routes/{route}", "ownerUid": "owner-old", "ownerUids": ["owner-old"]},
            {"path": f"routeEntitlements/{route}", "ownerUid": None, "ownerUids": []},
            {"path": f"routeNumbers/{route}", "ownerUid": None, "ownerUids": []},
        ]
        db = _Db({
            order_path: order_data,
            owner_path: owner_data,
            f"routes/{route}": {"ownerUid": "owner-old"},
        })
        report = {"entries": [{
            "classification": "reconstructable",
            "path": order_path,
            "routeNumber": route,
            "ownerPath": owner_path,
            "ownerSourcePaths": migration._owner_source_paths(route),
            "ownerSourceHash": migration.stable_hash(owner_state),
            "documentHash": migration.stable_hash(order_data),
            "contextHash": migration.stable_hash(migration._context_source(owner_data, "owner-old")),
            "proposedStamp": self.stamp,
        }]}

        with patch.dict("os.environ", {"FIRESTORE_EMULATOR_HOST": "127.0.0.1:8080"}):
            with patch.object(migration.gcf_firestore, "transactional", side_effect=lambda fn: fn):
                with self.assertRaisesRegex(RuntimeError, "changed_since_dry_run"):
                    migration.apply_orders(db, report)
        self.assertIn("scheduledOrder", db.document(order_path).data)
        self.assertIsNone(db.document(order_path).data["scheduledOrder"])

    def test_firebase_migration_contract_contains_no_delete_call(self):
        source = "\n".join((
            inspect.getsource(migration),
            inspect.getsource(migration.project_scheduled_order_archive_metadata),
            inspect.getsource(order_archive_receipt.write_verified_order_archive_receipt),
        ))
        self.assertEqual(migration.FIREBASE_MUTATION_CONTRACT, "update_only_no_delete")
        self.assertEqual(migration.EMULATOR_APPLY_CONTRACT, "firestore_emulator_only")
        self.assertEqual(migration.ARCHIVE_APPLY_CONTRACT, "loopback_test_postgres_only")
        for forbidden in (
            ".delete(",
            "transaction.delete",
            "batch.delete",
            "DELETE_FIELD",
            "delete_field",
            "firebase_admin.storage",
        ):
            self.assertNotIn(forbidden, source)

        report = migration._finish_report("orders", ["988200"], [])
        self.assertEqual(report["firebaseMutationContract"], "update_only_no_delete")
        self.assertEqual(report["applyContract"], "firestore_emulator_only")
        self.assertEqual(report["archiveApplyContract"], "loopback_test_postgres_only")


if __name__ == "__main__":
    unittest.main()

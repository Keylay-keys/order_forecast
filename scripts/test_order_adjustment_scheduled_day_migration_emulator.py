from __future__ import annotations

from datetime import datetime
import os
import unittest
from zoneinfo import ZoneInfo

from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore

from order_forecast.scripts import order_adjustment_scheduled_day_migration as migration


@unittest.skipUnless(
    os.environ.get("FIRESTORE_EMULATOR_HOST"),
    "requires the Firestore emulator",
)
class ScheduledOrderDayMigrationEmulatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = firestore.Client(
            project=os.environ.get("GCLOUD_PROJECT", "routespark-adjustment-v2-migration-test"),
            credentials=AnonymousCredentials(),
        )
        cls.route = "988200"
        cls.owner_uid = "owner-988200-migration-emulator"
        cls.order_id = "order-988200-migration-emulator"
        cls.adjustment_id = "adjustment-988200-migration-emulator"
        cls.terminal_adjustment_id = "adjustment-988200-terminal-emulator"
        cls.now_ms = int(
            datetime(
                2026, 8, 19, 10, 50, tzinfo=ZoneInfo("America/Denver")
            ).timestamp()
            * 1000
        )
        cycles = [{
            "orderDay": 2,
            "loadDay": 4,
            "deliveryDay": 4,
            "loadOffsetDays": 2,
            "deliveryOffsetDays": 2,
            "scheduleVersion": 2,
        }]
        cls.db.document(f"routes/{cls.route}").set({"ownerUid": cls.owner_uid})
        cls.db.document(f"routeEntitlements/{cls.route}").set(
            {"ownerUid": cls.owner_uid}
        )
        cls.db.document(f"routeNumbers/{cls.route}").set({"ownerUid": cls.owner_uid})
        cls.db.document(f"users/{cls.owner_uid}").set({
            "profile": {"timezone": "America/Denver"},
            "userSettings": {
                "notifications": {
                    "scheduling": {"orderCycles": cycles},
                    "orderAdjustmentSettings": {
                        "reminderEnabled": True,
                        "cutoffTimeLocal": "11:00",
                        "reminderLeadMinutes": 15,
                    },
                }
            },
        })
        cls.order_ref = cls.db.document(
            f"routes/{cls.route}/orders/{cls.order_id}"
        )
        cls.order_ref.set({
            "routeNumber": cls.route,
            "status": "finalized",
            "scheduleKey": "tuesday",
            "expectedDeliveryDate": "2026-08-20",
        })
        cls.adjustment_ref = cls.db.document(
            f"routes/{cls.route}/orderAdjustments/{cls.adjustment_id}"
        )
        cls.adjustment_ref.set({
            "routeNumber": cls.route,
            "userId": cls.owner_uid,
            "status": "draft",
            "sourceOrderId": cls.order_id,
            "reminderStatus": "skipped",
            "reminderSkipReason": "reminder_time_passed",
            "lines": [{"sap": "100", "direction": "add", "caseQuantity": 1}],
        })
        cls.terminal_adjustment_ref = cls.db.document(
            f"routes/{cls.route}/orderAdjustments/{cls.terminal_adjustment_id}"
        )
        cls.terminal_adjustment_ref.set({
            "routeNumber": cls.route,
            "userId": cls.owner_uid,
            "status": "sent",
            "sourceOrderId": cls.order_id,
            "reminderStatus": "sent",
            "lines": [{"sap": "200", "direction": "add", "caseQuantity": 1}],
        })

    def _route_counts(self) -> tuple[int, int]:
        route_ref = self.db.document(f"routes/{self.route}")
        return (
            len(list(route_ref.collection("orders").stream())),
            len(list(route_ref.collection("orderAdjustments").stream())),
        )

    def test_exact_update_only_apply_round_trip_is_idempotent(self):
        counts_before = self._route_counts()

        order_report = migration.scan_orders(self.db, [self.route], self.now_ms)
        order_entry = next(
            entry for entry in order_report["entries"]
            if entry["orderId"] == self.order_id
        )
        self.assertEqual(order_entry["classification"], "reconstructable")
        self.assertEqual(
            migration.apply_orders(self.db, order_report),
            [self.order_ref.path],
        )
        stamped_order = self.order_ref.get().to_dict()
        self.assertEqual(stamped_order["scheduledOrder"], order_entry["proposedStamp"])
        self.assertEqual(stamped_order["scheduledOrder"]["date"], "2026-08-18")

        adjustment_report = migration.scan_adjustments(
            self.db, [self.route], self.now_ms
        )
        adjustment_entry = next(
            entry for entry in adjustment_report["entries"]
            if entry["adjustmentId"] == self.adjustment_id
        )
        terminal_entry = next(
            entry for entry in adjustment_report["entries"]
            if entry["adjustmentId"] == self.terminal_adjustment_id
        )
        self.assertEqual(adjustment_entry["classification"], "rearmed")
        self.assertEqual(adjustment_entry["windowClass"], "already_due")
        self.assertEqual(terminal_entry["classification"], "terminal_untouched")
        terminal_before = self.terminal_adjustment_ref.get().to_dict()

        self.assertEqual(
            migration.apply_adjustments(self.db, adjustment_report, self.now_ms),
            [self.adjustment_ref.path],
        )
        adjustment = self.adjustment_ref.get().to_dict()
        self.assertEqual(adjustment["reminderStatus"], "pending")
        self.assertLess(adjustment["reminderAtMs"], self.now_ms)
        self.assertGreater(adjustment["cutoffAtMs"], self.now_ms)
        self.assertEqual(
            self.terminal_adjustment_ref.get().to_dict(), terminal_before
        )
        self.assertEqual(self._route_counts(), counts_before)

        second_orders = migration.scan_orders(self.db, [self.route], self.now_ms)
        second_adjustments = migration.scan_adjustments(
            self.db, [self.route], self.now_ms
        )
        self.assertEqual(
            next(
                entry["classification"] for entry in second_orders["entries"]
                if entry["orderId"] == self.order_id
            ),
            "already_stamped",
        )
        self.assertEqual(
            next(
                entry["classification"] for entry in second_adjustments["entries"]
                if entry["adjustmentId"] == self.adjustment_id
            ),
            "already_canonical",
        )
        self.assertEqual(migration.apply_orders(self.db, second_orders), [])
        self.assertEqual(
            migration.apply_adjustments(self.db, second_adjustments, self.now_ms),
            [],
        )
        self.assertEqual(self._route_counts(), counts_before)


if __name__ == "__main__":
    unittest.main()

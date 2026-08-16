from __future__ import annotations

import unittest
from unittest import mock

from order_forecast.scripts import order_sync_listener as listener


ROUTE_NUMBER = "988200"


class _FakeAdjustmentRef:
    def __init__(self):
        self.updates = []

    def update(self, fields):
        self.updates.append(fields)


class TestReallocationListenerRouting(unittest.TestCase):
    def test_route_988200_order_modified_event_never_replays_finalization(self):
        order = {
            "routeNumber": ROUTE_NUMBER,
            "userId": "user-988200",
            "scheduleKey": "tuesday",
            "status": "finalized",
            "orderRevision": 2,
            "lastMutation": {
                "kind": "store_reallocation",
                "mutationId": "reallocation-988200-1",
                "atMs": 1787068860000,
            },
        }
        with (
            mock.patch.object(listener, "handle_sync_order") as sync_order,
            mock.patch.object(listener, "register_finalize_event") as register,
            mock.patch.object(listener, "enqueue_finalize_jobs") as enqueue,
            mock.patch.object(listener, "_maybe_generate_next_forecast_after_finalization") as legacy_generate,
        ):
            listener.handle_finalized_order(mock.Mock(), "order-988200", order)

        sync_order.assert_not_called()
        register.assert_not_called()
        enqueue.assert_not_called()
        legacy_generate.assert_not_called()

    def test_route_988200_pending_receipt_projects_target_revision(self):
        ref = _FakeAdjustmentRef()
        receipt = {
            "routeNumber": ROUTE_NUMBER,
            "sourceOrderId": "order-988200",
            "projection": {"status": "pending", "targetOrderRevision": 2, "attemptCount": 0},
        }
        db = mock.Mock()
        with (
            mock.patch.object(listener, "get_pg_connection", return_value=mock.Mock()),
            mock.patch.object(
                listener,
                "handle_sync_order",
                return_value={"success": True, "projectedRevision": 2, "alreadyProjected": False},
            ) as sync_order,
            mock.patch.object(listener, "update_firebase_sync_status") as update_status,
        ):
            result = listener.handle_pending_adjustment_projection(db, ref, receipt)

        self.assertTrue(result["success"])
        sync_order.assert_called_once_with(
            mock.ANY,
            db,
            {"orderId": "order-988200", "routeNumber": ROUTE_NUMBER},
        )
        self.assertEqual(ref.updates[0]["projection.status"], "succeeded")
        self.assertEqual(ref.updates[0]["projection.projectedOrderRevision"], 2)
        update_status.assert_called_once_with(db, ROUTE_NUMBER, True)

    def test_route_988200_projection_failure_uses_stable_public_code(self):
        ref = _FakeAdjustmentRef()
        receipt = {
            "routeNumber": ROUTE_NUMBER,
            "sourceOrderId": "order-988200",
            "projection": {"status": "pending", "targetOrderRevision": 3},
        }
        with (
            mock.patch.object(listener, "get_pg_connection", return_value=mock.Mock()),
            mock.patch.object(
                listener,
                "handle_sync_order",
                return_value={"error": "password=secret SQL connection failed"},
            ),
        ):
            result = listener.handle_pending_adjustment_projection(mock.Mock(), ref, receipt)

        self.assertEqual(result, {"error": "ORDER_PROJECTION_FAILED"})
        self.assertEqual(ref.updates[0]["projection.status"], "failed")
        self.assertEqual(ref.updates[0]["projection.lastErrorCode"], "ORDER_PROJECTION_FAILED")
        self.assertNotIn("password", str(ref.updates[0]))


if __name__ == "__main__":
    unittest.main()

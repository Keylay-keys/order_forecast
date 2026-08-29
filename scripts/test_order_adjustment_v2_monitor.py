from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from order_forecast.scripts import order_adjustment_v2_monitor as monitor


class OrderAdjustmentV2MonitorTests(unittest.TestCase):
    def test_extracts_only_structured_events_from_prefixed_logs(self):
        events = monitor._json_events(
            '2026-08-28 INFO {"event":"order_adjustment_reminder_run","failed":0}\n'
            'plain text\n'
            '{"event":"order_sync_listener_initial_prime","primedFinalizations":2}\n'
        )
        self.assertEqual(
            [event["event"] for event in events],
            ["order_adjustment_reminder_run", "order_sync_listener_initial_prime"],
        )

    def test_direct_image_pin_parser_ignores_other_containers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "patch.yaml"
            path.write_text(
                "- name: other\n  image: registry/other:1\n"
                "- name: order-adjustment-reminders\n"
                "  image: registry/workers:v2\n"
            )
            self.assertEqual(
                monitor._pinned_image(path, "order-adjustment-reminders"),
                "registry/workers:v2",
            )

    def test_container_lookup_uses_name_instead_of_list_position(self):
        cron = {
            "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [
                {"name": "sidecar", "image": "registry/sidecar:1"},
                {"name": "order-adjustment-reminders", "image": "registry/workers:v2"},
            ]}}}}}
        }
        deployment = {
            "spec": {"template": {"spec": {"containers": [
                {"name": "sidecar", "image": "registry/sidecar:1"},
                {"name": "order-sync-listener", "image": "registry/listener:v2"},
            ]}}}
        }
        self.assertEqual(
            monitor._container_by_name(cron, "order-adjustment-reminders")["image"],
            "registry/workers:v2",
        )
        self.assertEqual(
            monitor._container_by_name(deployment, "order-sync-listener")["image"],
            "registry/listener:v2",
        )

    def test_success_age_is_timezone_safe(self):
        now = datetime(2026, 8, 28, 12, 5, tzinfo=timezone.utc)
        self.assertEqual(
            monitor._age_seconds("2026-08-28T12:03:00Z", now),
            120,
        )
        self.assertIsNone(monitor._age_seconds(None, now))

    def test_open_gate_requires_an_explicit_v2_route_source(self):
        empty_audit = {"results": [], "remainingDueIds": []}
        route, alerts = monitor._route_v2_health(empty_audit, require_v2=True)
        self.assertEqual(route["v2Count"], 0)
        self.assertEqual(alerts, ["route_has_no_v2_source"])

        route, alerts = monitor._route_v2_health(empty_audit, require_v2=False)
        self.assertEqual(route["v2Count"], 0)
        self.assertEqual(alerts, [])

    def test_route_v2_health_reports_drift_unresolved_and_backlog(self):
        audit = {
            "results": [
                {
                    "id": "drift",
                    "classification": "v2",
                    "eligibleForReminder": True,
                    "storedFutureScheduleDrift": True,
                    "schedule": {"cutoffAtMs": 1},
                },
                {
                    "id": "unresolved",
                    "classification": "v2",
                    "eligibleForReminder": True,
                    "storedFutureScheduleDrift": False,
                    "schedule": None,
                },
            ],
            "remainingDueIds": ["due"],
        }
        route, alerts = monitor._route_v2_health(audit, require_v2=True)
        self.assertEqual(route["v2Count"], 2)
        self.assertEqual(route["wrongFutureIds"], ["drift"])
        self.assertEqual(route["unresolvedV2Ids"], ["unresolved"])
        self.assertEqual(route["remainingDueIds"], ["due"])
        self.assertEqual(
            alerts,
            [
                "stored_future_v1_rows_for_v2_source",
                "v2_source_unexpectedly_unresolved",
                "route_due_backlog_remaining",
            ],
        )


if __name__ == "__main__":
    unittest.main()

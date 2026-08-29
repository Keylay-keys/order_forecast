from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from order_forecast.scripts import pcf_lifecycle_worker as worker


ORDER_FORECAST = Path(__file__).resolve().parents[2]
REPO_ROOT = ORDER_FORECAST.parent
PYTHON = ORDER_FORECAST / "venv/bin/python"
WORKER = ORDER_FORECAST / "scripts/pcf_lifecycle_worker.py"


class LifecycleWorkerAdmissionTests(TestCase):
    def run_worker(self, control, *, environment=None, service_account=None):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "control.json"
            path.write_text(json.dumps(control), encoding="utf-8")
            command = [
                str(PYTHON),
                str(WORKER),
                "--control-file",
                str(path),
            ]
            if service_account is not None:
                command.extend(["--service-account", str(service_account)])
            return subprocess.run(
                command,
                cwd=REPO_ROOT,
                env={
                    **os.environ,
                    "PYTHONPATH": str(REPO_ROOT),
                    "PCF_RECONCILIATION_MODE": "off",
                    **(environment or {}),
                },
                capture_output=True,
                text=True,
                timeout=30,
            )

    def test_disabled_worker_exits_before_credentials_or_database(self):
        result = self.run_worker(
            {
                "schemaVersion": 1,
                "effectsEnabled": False,
                "allProvenWork": False,
                "fences": [],
            }
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            json.loads(result.stdout), {"event": "pcf_lifecycle_worker_disabled"}
        )

    def test_enabled_empty_control_fails_before_credentials_or_database(self):
        result = self.run_worker(
            {
                "schemaVersion": 1,
                "effectsEnabled": True,
                "allProvenWork": False,
                "fences": [],
            }
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("require an exact fence", result.stderr)

    def test_all_proven_work_cannot_bypass_exact_fence_yet(self):
        result = self.run_worker(
            {
                "schemaVersion": 1,
                "effectsEnabled": True,
                "allProvenWork": True,
                "fences": [],
            }
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fleet-wide mutation fence", result.stderr)

    def test_unknown_reconciliation_mode_fails_before_credentials_or_database(self):
        result = self.run_worker(
            {
                "schemaVersion": 1,
                "effectsEnabled": False,
                "allProvenWork": False,
                "fences": [],
            },
            environment={"PCF_RECONCILIATION_MODE": "enabled"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exactly 'off' or 'bounded'", result.stderr)

    def test_bounded_reconciliation_requires_service_account_before_database(self):
        result = self.run_worker(
            {
                "schemaVersion": 1,
                "effectsEnabled": False,
                "allProvenWork": False,
                "fences": [],
            },
            environment={
                "PCF_RECONCILIATION_MODE": "bounded",
                "PCF_RECONCILIATION_PROJECT_ID": "routespark-1f47d",
            },
            service_account="/definitely/missing/service-account.json",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Firebase service account is required", result.stderr)

    def test_bounded_reconciliation_rejects_wrong_project_before_database(self):
        with TemporaryDirectory() as temp:
            service_account = Path(temp) / "service-account.json"
            service_account.write_text(
                json.dumps({"project_id": "wrong-project"}), encoding="utf-8"
            )
            result = self.run_worker(
                {
                    "schemaVersion": 1,
                    "effectsEnabled": False,
                    "allProvenWork": False,
                    "fences": [],
                },
                environment={
                    "PCF_RECONCILIATION_MODE": "bounded",
                    "PCF_RECONCILIATION_PROJECT_ID": "routespark-1f47d",
                    "PCF_RECONCILIATION_FIXTURE_REGISTRY": str(
                        ORDER_FORECAST / "contracts/pcf/fixture-identity-v1.json"
                    ),
                },
                service_account=service_account,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("project identity mismatch", result.stderr)


class LifecycleWorkerReconciliationTests(TestCase):
    def control_path(self, root: str) -> Path:
        path = Path(root) / "control.json"
        path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "effectsEnabled": False,
                    "allProvenWork": False,
                    "fences": [],
                }
            ),
            encoding="utf-8",
        )
        return path

    def worker_args(self, control_path: Path) -> list[str]:
        return [
            str(WORKER),
            "--control-file",
            str(control_path),
            "--service-account",
            "/fake/service-account.json",
        ]

    def bounded_environment(self):
        return {
            "PCF_RECONCILIATION_MODE": "bounded",
            "PCF_RECONCILIATION_PROJECT_ID": "routespark-1f47d",
            "PCF_RECONCILIATION_FIXTURE_REGISTRY": str(
                ORDER_FORECAST / "contracts/pcf/fixture-identity-v1.json"
            ),
        }

    def test_no_due_route_initializes_no_firebase_client(self):
        with TemporaryDirectory() as temp:
            control_path = self.control_path(temp)
            stdout = StringIO()
            with (
                patch.dict(os.environ, self.bounded_environment(), clear=False),
                patch.object(sys, "argv", self.worker_args(control_path)),
                patch.object(worker, "_service_account_project", return_value="routespark-1f47d"),
                patch.object(worker, "load_production_fixture_registry", return_value={}),
                patch.object(worker, "_reserve_reconciliation_route", return_value=None),
                patch.object(worker.firebase_admin, "initialize_app") as initialize_app,
                redirect_stdout(stdout),
            ):
                result = worker.main()
        self.assertEqual(result, 0)
        initialize_app.assert_not_called()
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "event": "pcf_lifecycle_worker_complete",
                "effectsEnabled": False,
                "reconciliation": {"status": "not_due"},
            },
        )

    def test_due_route_emits_sanitized_comparison_counts(self):
        final_result = SimpleNamespace(
            missing_in_projection=(object(),),
            changed_in_projection=(),
            extra_in_projection=(object(),),
        )
        report = SimpleNamespace(
            route_number="900000",
            repair_mode="observe_only",
            status="mismatched",
            reason="checksum_mismatch",
            source_snapshot_count=1,
            upsert_repairs=0,
            tombstone_repairs=0,
            unresolved_extras=1,
            minimum_firestore_reads=3,
            final_result=final_result,
        )
        runtime = SimpleNamespace(run_route=lambda route, observed_at: report)
        with TemporaryDirectory() as temp:
            control_path = self.control_path(temp)
            stdout = StringIO()
            with (
                patch.dict(os.environ, self.bounded_environment(), clear=False),
                patch.object(sys, "argv", self.worker_args(control_path)),
                patch.object(worker, "_service_account_project", return_value="routespark-1f47d"),
                patch.object(worker, "load_production_fixture_registry", return_value={}),
                patch.object(worker, "_reserve_reconciliation_route", return_value="900000"),
                patch.object(worker.credentials, "Certificate", return_value=object()),
                patch.object(worker.firebase_admin, "initialize_app", return_value=object()),
                patch.object(worker.firestore, "client", return_value=object()),
                patch.object(worker, "FirestoreProjectionSourceAdapter", return_value=object()),
                patch.object(worker, "ProjectionReconciliationRuntime", return_value=runtime),
                redirect_stdout(stdout),
            ):
                result = worker.main()
        self.assertEqual(result, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["reconciliation"]["routeNumber"], "900000")
        self.assertEqual(payload["reconciliation"]["repairMode"], "observe_only")
        self.assertEqual(payload["reconciliation"]["missingInProjection"], 1)
        self.assertEqual(payload["reconciliation"]["extraInProjection"], 1)
        self.assertNotIn("finalResult", payload["reconciliation"])

    def test_due_route_failure_records_fail_closed_health(self):
        runtime = SimpleNamespace(
            run_route=lambda route, observed_at: (_ for _ in ()).throw(
                RuntimeError("source failed")
            )
        )
        with TemporaryDirectory() as temp:
            control_path = self.control_path(temp)
            with (
                patch.dict(os.environ, self.bounded_environment(), clear=False),
                patch.object(sys, "argv", self.worker_args(control_path)),
                patch.object(worker, "_service_account_project", return_value="routespark-1f47d"),
                patch.object(worker, "load_production_fixture_registry", return_value={}),
                patch.object(worker, "_reserve_reconciliation_route", return_value="989262"),
                patch.object(worker.credentials, "Certificate", return_value=object()),
                patch.object(worker.firebase_admin, "initialize_app", return_value=object()),
                patch.object(worker.firestore, "client", return_value=object()),
                patch.object(worker, "FirestoreProjectionSourceAdapter", return_value=object()),
                patch.object(worker, "ProjectionReconciliationRuntime", return_value=runtime),
                patch.object(worker, "_record_failed_reconciliation") as record_failure,
            ):
                with self.assertRaisesRegex(RuntimeError, "source failed"):
                    worker.main()
        record_failure.assert_called_once_with("989262")

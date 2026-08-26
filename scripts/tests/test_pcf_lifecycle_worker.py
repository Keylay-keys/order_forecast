from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase


ORDER_FORECAST = Path(__file__).resolve().parents[2]
REPO_ROOT = ORDER_FORECAST.parent
PYTHON = ORDER_FORECAST / "venv/bin/python"
WORKER = ORDER_FORECAST / "scripts/pcf_lifecycle_worker.py"


class LifecycleWorkerAdmissionTests(TestCase):
    def run_worker(self, control):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "control.json"
            path.write_text(json.dumps(control), encoding="utf-8")
            return subprocess.run(
                [str(PYTHON), str(WORKER), "--control-file", str(path)],
                cwd=REPO_ROOT,
                env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
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

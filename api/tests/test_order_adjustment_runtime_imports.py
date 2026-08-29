from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import unittest


class OrderAdjustmentRuntimeImportTests(unittest.TestCase):
    def test_api_router_imports_from_container_working_directory(self):
        order_forecast = Path(__file__).resolve().parents[2]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(order_forecast)
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "from api.routers.orders import router; "
                "from api.order_adjustment_schedule_v2_release import "
                "OrderAdjustmentScheduleV2ReleaseControls; "
                "assert len(router.routes) > 0",
            ],
            cwd=order_forecast,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()

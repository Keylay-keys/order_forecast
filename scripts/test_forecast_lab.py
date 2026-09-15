#!/usr/bin/env python3
"""Safety and reproducibility checks for the forecast test lab."""

from __future__ import annotations

import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

try:
    from . import forecast_lab
    from . import walk_forward_backtest as backtest
except ImportError:
    import forecast_lab
    import walk_forward_backtest as backtest


def _manifest() -> dict:
    return {
        "schema_version": "routespark-forecast-lab-snapshot-v1",
        "exported_at_utc": "2026-09-02T12:00:00+00:00",
        "snapshot_as_of_date": "2026-09-02",
        "route_count": 1,
        "tables": {
            table: {"columns": ["id"], "rows": 0, "sha256": "unused"}
            for table in forecast_lab.IMPORT_ORDER
        },
    }


class ForecastLabTests(unittest.TestCase):
    def test_manifest_reader_requires_every_import_table(self) -> None:
        manifest = _manifest()
        del manifest["tables"][forecast_lab.IMPORT_ORDER[-1]]
        with tempfile.TemporaryDirectory() as temp_name:
            archive_path = Path(temp_name) / "snapshot.tar.gz"
            payload = json.dumps(manifest).encode()
            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo("manifest.json")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            with self.assertRaises(forecast_lab.LabError):
                forecast_lab._read_snapshot_manifest(archive_path)

    def test_archive_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            archive_path = Path(temp_name) / "unsafe.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo("../escape.csv")
                info.size = 0
                archive.addfile(info, io.BytesIO())
            with tarfile.open(archive_path, "r:gz") as archive:
                with self.assertRaises(forecast_lab.LabError):
                    forecast_lab._safe_archive_members(archive)

    def test_lab_root_rejects_non_external_path(self) -> None:
        with self.assertRaises(forecast_lab.LabError):
            forecast_lab._lab_root("/private/tmp/not-the-forecast-lab")

    def test_route_discovery_uses_fixed_as_of_date(self) -> None:
        from datetime import date

        with patch.object(backtest, "fetch_all", return_value=[]) as fetch:
            backtest._distinct_routes_from_history(365, date(2026, 9, 2))
        sql, params = fetch.call_args.args
        self.assertNotIn("CURRENT_DATE", sql)
        self.assertEqual(params, [date(2026, 9, 2), 365])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import archive_purge_worker as worker


class TestArchivePurgeWorkerHddDates(unittest.TestCase):
    def test_latest_hdd_run_date_ignores_canonical_dirs_when_timestamp_runs_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            (delivery_dir / "20260416_173753").mkdir(parents=True)
            (delivery_dir / "20260417_101112").mkdir()
            (delivery_dir / "pages").mkdir()
            (delivery_dir / "reports").mkdir()

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 17),
                )

    def test_latest_hdd_run_date_uses_manifest_source_run_ids_for_canonical_only_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            delivery_dir.mkdir(parents=True)
            (delivery_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "generatedAt": "2026-04-20T00:00:00Z",
                        "sourceRunIds": ["20260416_173753", "20260418_091011"],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 18),
                )

    def test_latest_hdd_run_date_falls_back_to_manifest_generated_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            delivery_dir = root / "987318" / "1805949370"
            delivery_dir.mkdir(parents=True)
            (delivery_dir / "manifest.json").write_text(
                json.dumps({"generatedAt": "2026-04-20T14:15:16Z"}),
                encoding="utf-8",
            )

            with mock.patch.object(worker, "HDD_ARCHIVE_BASE", root):
                self.assertEqual(
                    worker._latest_hdd_run_date("987318", "1805949370"),
                    date(2026, 4, 20),
                )


if __name__ == "__main__":
    unittest.main()

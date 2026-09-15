import tempfile
import unittest
from pathlib import Path

try:
    from .build_settlement_metrics_lab import safe_manifest_pdf, select_weekly_rows
except ImportError:
    from build_settlement_metrics_lab import safe_manifest_pdf, select_weekly_rows


class SettlementMetricsLabBuilderTests(unittest.TestCase):
    def test_selects_final_and_uses_preliminary_only_as_fallback(self):
        rows = [
            {"route": "989262", "week_ending": "2026-01-07", "revision": "preliminary"},
            {"route": "989262", "week_ending": "2026-01-07", "revision": "final"},
            {"route": "989262", "week_ending": "2026-03-11", "revision": "preliminary"},
            {"route": "988200", "week_ending": "2026-01-07", "revision": "final"},
        ]

        selected = select_weekly_rows(rows, "989262")

        self.assertEqual(
            [(row["week_ending"], row["revision"]) for row in selected],
            [("2026-01-07", "final"), ("2026-03-11", "preliminary")],
        )

    def test_rejects_duplicate_week_revision(self):
        rows = [
            {"route": "989262", "week_ending": "2026-01-07", "revision": "final"},
            {"route": "989262", "week_ending": "2026-01-07", "revision": "final"},
        ]

        with self.assertRaisesRegex(ValueError, "Duplicate revision"):
            select_weekly_rows(rows, "989262")

    def test_manifest_path_cannot_escape_archive_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Unsafe normalized PDF path"):
                safe_manifest_pdf(root, "../outside.pdf")
            with self.assertRaisesRegex(ValueError, "Unsafe normalized PDF path"):
                safe_manifest_pdf(root, "/tmp/outside.pdf")


if __name__ == "__main__":
    unittest.main()

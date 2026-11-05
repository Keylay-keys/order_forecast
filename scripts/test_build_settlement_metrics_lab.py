import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

try:
    from .build_settlement_metrics_lab import optional_zero_money, safe_manifest_pdf, select_weekly_rows
except ImportError:
    from build_settlement_metrics_lab import optional_zero_money, safe_manifest_pdf, select_weekly_rows


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

    def test_optional_zero_money_distinguishes_absent_from_unreadable_rows(self):
        pattern = r"^\s*SBTv1 Gross\s+([-0-9,]+\.\d{2})\s*$"
        label = r"^\s*SBTv1 Gross\b"
        self.assertEqual(optional_zero_money(pattern, label, "Gross Sales - Bybk 13,010.85", "SBT gross"), Decimal("0.00"))
        self.assertEqual(optional_zero_money(pattern, label, "SBTv1 Gross 1,234.56", "SBT gross"), Decimal("1234.56"))
        with self.assertRaisesRegex(ValueError, "Missing required settlement field: SBT gross"):
            optional_zero_money(pattern, label, "SBTv1 Gross unreadable", "SBT gross")


if __name__ == "__main__":
    unittest.main()

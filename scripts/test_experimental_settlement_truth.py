#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime, timezone
import unittest

import pandas as pd

try:
    from .experimental_settlement_truth import apply_settlement_store_share
except ImportError:
    from experimental_settlement_truth import apply_settlement_store_share


class SettlementStoreShareTests(unittest.TestCase):
    def _predictions(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "store": "Smith's FD SBT #0135 Farmington",
                    "store_id": "farmington-id",
                    "sap": "28934",
                    "pred_units": 60.0,
                    "p10_units": 45.0,
                    "p50_units": 60.0,
                    "p90_units": 75.0,
                    "model_branch": "store_first_case_hierarchy_v1",
                },
                {
                    "store": "Smith's MKT SBT #0277 Kaysville",
                    "store_id": "kaysville-id",
                    "sap": "28934",
                    "pred_units": 40.0,
                    "p10_units": 30.0,
                    "p50_units": 40.0,
                    "p90_units": 50.0,
                    "model_branch": "store_first_case_hierarchy_v1",
                },
            ]
        )

    def _settlement(self) -> pd.DataFrame:
        rows = []
        for week, available in (
            ("2026-08-12", "2026-08-17T12:00:00+00:00"),
            ("2026-08-19", "2026-08-24T12:00:00+00:00"),
        ):
            rows.extend(
                [
                    {
                        "route_number": "989262",
                        "store_number": "0135",
                        "sap": "28934",
                        "settlement_week_ending": week,
                        "simulated_available_at": available,
                        "sale_units": 20,
                    },
                    {
                        "route_number": "989262",
                        "store_number": "0277",
                        "sap": "28934",
                        "settlement_week_ending": week,
                        "simulated_available_at": available,
                        "sale_units": 80,
                    },
                ]
            )
        return pd.DataFrame(rows)

    def test_blends_store_share_and_preserves_combined_units(self) -> None:
        result, stats = apply_settlement_store_share(
            self._predictions(),
            self._settlement(),
            datetime(2026, 8, 25, tzinfo=timezone.utc),
        )
        self.assertEqual(100, int(result["pred_units"].sum()))
        self.assertEqual([47, 53], result["pred_units"].astype(int).tolist())
        self.assertEqual(1, stats["reallocated_saps"])
        self.assertEqual(13, stats["units_moved_between_stores"])

    def test_future_statement_is_not_used(self) -> None:
        result, stats = apply_settlement_store_share(
            self._predictions(),
            self._settlement(),
            datetime(2026, 8, 20, tzinfo=timezone.utc),
        )
        self.assertEqual([60, 40], result["pred_units"].astype(int).tolist())
        self.assertEqual(0, stats["reallocated_saps"])


if __name__ == "__main__":
    unittest.main()

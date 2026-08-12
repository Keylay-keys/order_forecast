import unittest
from unittest.mock import patch

import forecast_engine as engine
from models import ForecastItem, Product, StoreConfig


class ForecastDenseOutputTests(unittest.TestCase):
    def test_dense_completion_emits_explicit_zero_for_every_active_carry_key(self):
        stores = [StoreConfig(
            store_id="store-1",
            store_name="Store One",
            delivery_days=["thursday"],
            active_saps=["100", "200"],
        )]
        products = [
            Product(sap="100", name="One", case_pack=12),
            Product(sap="200", name="Two", case_pack=18),
        ]
        existing = [ForecastItem(
            store_id="store-1",
            store_name="Store One",
            sap="100",
            recommended_units=7,
            source="model",
        )]

        rows = engine._complete_dense_active_carry(
            existing, stores, products, {"store-1"}
        )
        by_sap = {row.sap: row for row in rows}
        self.assertEqual(set(by_sap), {"100", "200"})
        self.assertEqual(by_sap["200"].recommended_units, 0)
        self.assertEqual(by_sap["200"].recommended_cases, 0)
        self.assertEqual(by_sap["200"].source, "dense_zero")

    def test_generation_fails_closed_when_firebase_authority_drifted(self):
        rows = [ForecastItem(
            store_id="store-1",
            store_name="Store One",
            sap="100",
            recommended_units=0,
        )]
        with patch.object(
            engine,
            "load_authority_generation_state",
            return_value=({("store-1", "100"), ("store-1", "200")}, "revision"),
        ):
            with self.assertRaisesRegex(ValueError, "active_carry_changed_during_generation"):
                engine._validate_dense_items_against_authority(
                    object(), "988200", "2026-08-13", "tuesday", rows
                )


if __name__ == "__main__":
    unittest.main()

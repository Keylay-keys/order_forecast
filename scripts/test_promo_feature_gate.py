from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from order_forecast.scripts import forecast_engine
from order_forecast.scripts import promo_sync_listener
from order_forecast.scripts.promo_feature_gate import promos_enabled


class PromoFeatureGateTest(unittest.TestCase):
    def test_gate_defaults_off(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(promos_enabled())

    def test_gate_requires_literal_true(self):
        for value in ("1", "yes", "on", "false", "", "unexpected"):
            with self.subTest(value=value), patch.dict(
                os.environ, {"PROMOS_ENABLED": value}, clear=True
            ):
                self.assertFalse(promos_enabled())

        with patch.dict(os.environ, {"PROMOS_ENABLED": " TRUE "}, clear=True):
            self.assertTrue(promos_enabled())

    def test_disabled_listener_opens_no_clients(self):
        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "false"}, clear=True),
            patch.object(promo_sync_listener, "get_firestore_client") as firestore_client,
            patch.object(promo_sync_listener, "get_pg_connection") as pg_connection,
        ):
            self.assertEqual(promo_sync_listener.main("unused.json"), 0)

        firestore_client.assert_not_called()
        pg_connection.assert_not_called()

    def test_disabled_forecast_lookup_reads_no_firestore(self):
        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "false"}, clear=True),
            patch.object(forecast_engine, "load_promotions") as load_promotions,
        ):
            result = forecast_engine._build_active_promo_lookup(
                db=object(),
                route_number="989262",
                delivery_date="2026-08-21",
                stores_cfg=[SimpleNamespace(store_id="s1", store_name="Store 1")],
            )

        self.assertEqual(result, {})
        load_promotions.assert_not_called()

    def test_disabled_history_lookup_reads_no_postgres(self):
        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "false"}, clear=True),
            patch.object(forecast_engine, "fetch_all") as fetch_all,
        ):
            result = forecast_engine._get_last_promo_quantities(
                route_number="989262",
                schedule_keys=["monday"],
                delivery_date="2026-08-21",
            )

        self.assertEqual(result, {})
        fetch_all.assert_not_called()

    def test_enabled_forecast_lookup_keeps_existing_behavior(self):
        with (
            patch.dict(os.environ, {"PROMOS_ENABLED": "true"}, clear=True),
            patch.object(forecast_engine, "load_promotions", return_value=[]) as load_promotions,
        ):
            result = forecast_engine._build_active_promo_lookup(
                db=object(),
                route_number="989262",
                delivery_date="2026-08-21",
                stores_cfg=[],
            )

        self.assertEqual(result, {})
        load_promotions.assert_called_once()


if __name__ == "__main__":
    unittest.main()

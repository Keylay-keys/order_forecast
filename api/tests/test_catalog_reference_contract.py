import unittest

from order_forecast.api.routers import catalog, reference
from order_forecast.scripts import load_reference_catalog


class CatalogReferenceContractTests(unittest.TestCase):
    def test_catalog_payload_accepts_upc_and_preserves_legacy_sku(self):
        payload = catalog.CatalogProductPayload(
            sap="31032",
            fullName="Mission Tortilla Chips",
            upc="075202303167",
            sku="legacy-barcode",
            casePack=22,
        )

        doc = catalog._payload_to_doc(payload)

        self.assertEqual(doc["upc"], "075202303167")
        self.assertEqual(doc["sku"], "legacy-barcode")

    def test_catalog_normalize_uses_sku_as_upc_bridge(self):
        product = catalog._normalize_product(
            {
                "fullName": "Legacy Barcode Item",
                "sku": "11110-08472",
                "isActive": False,
            },
            "54511",
        )

        self.assertEqual(product["upc"], "11110-08472")
        self.assertEqual(product["sku"], "11110-08472")
        self.assertFalse(product["active"])

    def test_catalog_normalize_prefers_active_over_is_active(self):
        product = catalog._normalize_product(
            {
                "fullName": "Active Field Wins",
                "active": True,
                "isActive": False,
            },
            "54773",
        )

        self.assertTrue(product["active"])

    def test_reference_item_normalization_matches_api_contract(self):
        item = reference._normalize_reference_item(
            {
                "catalog_id": "routespark-starter-catalog",
                "sap": "54773",
                "upc": "075202303167",
                "full_name": "Hint of Lime Deli Fresh Chips",
                "brand": "Deli Fresh",
                "category": "chips",
                "case_pack": 10,
                "display_order": 204,
                "source": "repo",
                "active": True,
            }
        )

        self.assertEqual(item["sap"], "54773")
        self.assertEqual(item["upc"], "075202303167")
        self.assertEqual(item["fullName"], "Hint of Lime Deli Fresh Chips")
        self.assertEqual(item["casePack"], 10)

    def test_reference_like_escape_escapes_wildcards(self):
        self.assertEqual(reference._escape_like(r"100%_chips\\"), r"100\%\_chips\\\\")

    def test_reference_loader_rows_require_positive_case_pack(self):
        with self.assertRaises(ValueError):
            load_reference_catalog._rows(
                [{"sap": "52881", "fullName": "Missing Case Pack", "casePack": 0}],
                catalog_id="routespark-starter-catalog",
                source_label="test",
            )

    def test_reference_loader_rows_accept_starter_shape(self):
        rows = load_reference_catalog._rows(
            [
                {
                    "sap": "54511",
                    "upc": "11110-08472",
                    "fullName": "Kroger Zero Net Carb Street Taco",
                    "casePack": 16,
                    "brand": "Kroger",
                    "category": "tortillas",
                    "displayOrder": 203,
                    "active": True,
                }
            ],
            catalog_id="routespark-starter-catalog",
            source_label="test",
        )

        self.assertEqual(rows[0][0], "routespark-starter-catalog")
        self.assertEqual(rows[0][1], "54511")
        self.assertEqual(rows[0][2], "11110-08472")
        self.assertEqual(rows[0][6], 16)


if __name__ == "__main__":
    unittest.main()

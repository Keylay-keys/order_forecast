import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from order_forecast.api.routers import catalog, reference
from order_forecast.scripts import load_reference_catalog


class _FakeRequest:
    def __init__(self, headers=None, scheme="http", netloc="internal:8000", base_url="http://internal:8000/"):
        self.headers = headers or {}
        self.base_url = base_url

        class Url:
            pass

        self.url = Url()
        self.url.scheme = scheme
        self.url.netloc = netloc


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
                "image_path": "routespark-starter-catalog/54773.png",
                "image_thumb_path": "routespark-starter-catalog/54773.png",
                "source": "repo",
                "active": True,
            },
            base_url="https://api.routespark.pro/",
        )

        self.assertEqual(item["sap"], "54773")
        self.assertEqual(item["upc"], "075202303167")
        self.assertEqual(item["fullName"], "Hint of Lime Deli Fresh Chips")
        self.assertEqual(item["casePack"], 10)
        self.assertEqual(item["imageUrl"], "https://api.routespark.pro/api/catalog/starter/images/54773.png")
        self.assertEqual(item["imageThumbUrl"], "https://api.routespark.pro/api/catalog/starter/images/54773.png")

    def test_reference_item_without_image_has_null_image_urls(self):
        item = reference._normalize_reference_item(
            {
                "catalog_id": "routespark-starter-catalog",
                "sap": "54511",
                "full_name": "Kroger Zero Net Carb Street Taco",
                "case_pack": 16,
                "active": True,
            },
            base_url="https://api.routespark.pro/",
        )

        self.assertIsNone(item["imageUrl"])
        self.assertIsNone(item["imageThumbUrl"])

    def test_reference_public_base_url_uses_forwarded_https_headers(self):
        request = _FakeRequest(
            headers={
                "x-forwarded-proto": "https",
                "x-forwarded-host": "api.routespark.pro",
                "host": "web-api:8000",
            }
        )

        self.assertEqual(reference._public_base_url(request), "https://api.routespark.pro/")

    def test_reference_like_escape_escapes_wildcards(self):
        self.assertEqual(reference._escape_like(r"100%_chips\\"), r"100\%\_chips\\\\")

    def test_reference_catalog_list_defaults_to_active_only(self):
        rows = [
            {
                "catalog_id": "routespark-starter-catalog",
                "sap": "31032",
                "full_name": "Mission Yellow Corn Tortilla",
                "case_pack": 22,
                "active": True,
            }
        ]
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = rows

        with patch.object(reference, "get_pg_connection", return_value=conn), patch.object(reference, "return_pg_connection"):
            items = reference._fetch_reference_catalog_items()

        self.assertEqual(items[0]["sap"], "31032")
        self.assertIn("active = TRUE", cursor.execute.call_args.args[0])
        self.assertEqual(cursor.execute.call_args.args[1], ["routespark-starter-catalog", False, 250])

    def test_reference_catalog_list_can_include_inactive_rows(self):
        rows = [
            {
                "catalog_id": "routespark-starter-catalog",
                "sap": "99999",
                "full_name": "Inactive Reference Item",
                "case_pack": 12,
                "active": False,
            }
        ]
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = rows

        with patch.object(reference, "get_pg_connection", return_value=conn), patch.object(reference, "return_pg_connection"):
            items = reference._fetch_reference_catalog_items(include_inactive=True)

        self.assertFalse(items[0]["active"])
        self.assertIn("active = TRUE", cursor.execute.call_args.args[0])
        self.assertEqual(cursor.execute.call_args.args[1], ["routespark-starter-catalog", True, 250])

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

    def test_reference_loader_rows_include_manifest_image_paths(self):
        rows = load_reference_catalog._rows(
            [
                {
                    "sap": "31032",
                    "upc": "73731-00328",
                    "fullName": "Mission Yellow Corn Tortilla",
                    "casePack": 22,
                }
            ],
            catalog_id="routespark-starter-catalog",
            source_label="test",
            image_paths={
                "31032": {
                    "imagePath": "routespark-starter-catalog/31032.png",
                    "imageThumbPath": "routespark-starter-catalog/31032.png",
                }
            },
        )

        self.assertEqual(rows[0][8], "routespark-starter-catalog/31032.png")
        self.assertEqual(rows[0][9], "routespark-starter-catalog/31032.png")

    def test_reference_loader_strips_manifest_path_to_catalog_relative_image_path(self):
        self.assertEqual(
            load_reference_catalog._catalog_image_path(
                "data/catalogs/product_images/routespark-starter-catalog/31032.png"
            ),
            "routespark-starter-catalog/31032.png",
        )

    def test_reference_image_path_rejects_path_escape(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = root / "routespark-starter-catalog" / "31032.png"
            image.parent.mkdir(parents=True)
            image.write_bytes(b"png")

            safe = reference._safe_catalog_image_file(root, "routespark-starter-catalog/31032.png")
            escape = reference._safe_catalog_image_file(root, "../outside.png")

            self.assertEqual(safe, image.resolve())
            self.assertIsNone(escape)


if __name__ == "__main__":
    unittest.main()

import inspect
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
import psycopg2

from order_forecast.api.errors import StructuredApiError
from order_forecast.api.routers import catalog, reference
from order_forecast.scripts import load_reference_catalog

ROOT_DIR = Path(__file__).resolve().parents[3]


class _FakeRequest:
    def __init__(self, headers=None, scheme="http", netloc="internal:8000", base_url="http://internal:8000/"):
        self.headers = headers or {}
        self.base_url = base_url
        self.state = SimpleNamespace(request_id="catalog-test-request")

        class Url:
            pass

        self.url = Url()
        self.url.scheme = scheme
        self.url.netloc = netloc
        self.url.path = "/api/catalog/starter"


class _FakeSnapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _FakeCollection:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)

    def document(self, doc_id):
        return _FakeDocument(self.db, self.path + (doc_id,))

    def stream(self):
        data = self.db.collection_data(self.path)
        return [_FakeSnapshot(doc_id, doc_data) for doc_id, doc_data in data.items()]


class _FakeDocument:
    def __init__(self, db, path):
        self.db = db
        self.path = tuple(path)
        self.id = self.path[-1]

    def collection(self, name):
        return _FakeCollection(self.db, self.path + (name,))

    def get(self):
        return _FakeSnapshot(self.id, self.db.document_data(self.path))

    def set(self, data, merge=False):
        self.db.set_document(self.path, data, merge=merge)

    def update(self, data):
        self.db.update_document(self.path, data)

    def delete(self):
        self.db.delete_document(self.path)


class _FakeBatch:
    def __init__(self, db):
        self.db = db
        self.operations = []

    def set(self, doc_ref, data, merge=False):
        self.operations.append(("set", doc_ref.path, dict(data), merge))

    def update(self, doc_ref, data):
        self.operations.append(("update", doc_ref.path, dict(data), False))

    def commit(self):
        for op, path, data, merge in self.operations:
            if op == "set":
                self.db.set_document(path, data, merge=merge)
            elif op == "update":
                self.db.update_document(path, data)
        self.db.batch_commits += 1
        self.db.batch_write_counts.append(len(self.operations))


class _FakeDB:
    def __init__(self):
        self.root = {"masterCatalog": {}, "routes": {}, "sharedCatalogs": {}}
        self.batch_commits = 0
        self.batch_write_counts = []

    def collection(self, name):
        self.root.setdefault(name, {})
        return _FakeCollection(self, (name,))

    def batch(self):
        return _FakeBatch(self)

    def collection_data(self, path):
        if len(path) == 1:
            return self.root.setdefault(path[0], {})
        if len(path) == 3:
            parent = self.document_data(path[:2])
            if parent is None:
                return {}
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        raise AssertionError(f"Unsupported collection path: {path}")

    def document_data(self, path):
        if len(path) == 2:
            return self.root.setdefault(path[0], {}).get(path[1])
        if len(path) == 4:
            parent = self.document_data(path[:2])
            if parent is None:
                return None
            return parent.setdefault("_subcollections", {}).setdefault(path[2], {}).get(path[3])
        raise AssertionError(f"Unsupported document path: {path}")

    def set_document(self, path, data, merge=False):
        if len(path) == 2:
            collection = self.root.setdefault(path[0], {})
        elif len(path) == 4:
            parent = self.root.setdefault(path[0], {}).setdefault(path[1], {})
            collection = parent.setdefault("_subcollections", {}).setdefault(path[2], {})
        else:
            raise AssertionError(f"Unsupported document path: {path}")
        current = collection.get(path[-1]) if merge else None
        collection[path[-1]] = {**(current or {}), **data}

    def update_document(self, path, data):
        current = self.document_data(path)
        if current is None:
            raise AssertionError(f"Missing document for update: {path}")
        current.update(data)

    def delete_document(self, path):
        if len(path) == 4:
            parent = self.document_data(path[:2])
            if parent is not None:
                parent.setdefault("_subcollections", {}).setdefault(path[2], {}).pop(path[3], None)
            return
        if len(path) == 2:
            self.root.setdefault(path[0], {}).pop(path[1], None)
            return
        raise AssertionError(f"Unsupported document path: {path}")


def _unwrap(endpoint):
    return inspect.unwrap(endpoint)


def _reference_payload(version=12, items=None):
    return {
        "version": version,
        "items": items
        if items is not None
        else [
            {
                "sap": "31032",
                "upc": "73731-00328",
                "brand": "Mission",
                "category": "Tortillas",
                "fullName": "Mission Yellow Corn Tortillas",
                "casePack": 22,
                "displayOrder": 1,
                "active": True,
            },
            {
                "sap": "54511",
                "upc": "11110-08472",
                "brand": "Kroger",
                "category": "Tortillas",
                "fullName": "Kroger Zero Net Carb Street Taco",
                "casePack": 16,
                "displayOrder": 2,
                "active": True,
            },
            {
                "sap": "54773",
                "upc": "075202303167",
                "brand": "Deli Fresh",
                "category": "Chips",
                "fullName": "Hint of Lime Deli Fresh Chips",
                "casePack": 8,
                "displayOrder": 3,
                "active": True,
            },
        ],
    }


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
                "tags": ["better_for_you"],
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
        self.assertEqual(item["tags"], ["better_for_you"])
        self.assertTrue(item["imageUrl"].startswith("https://api.routespark.pro/api/catalog/starter/images/54773.png"))
        self.assertTrue(item["imageThumbUrl"].startswith("https://api.routespark.pro/api/catalog/starter/images/54773.png"))

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

    def test_reference_meta_normalization_uses_top_level_contract_names(self):
        meta = reference._normalize_reference_meta(
            {
                "version": 12,
                "product_count": 204,
                "updated_at": "2026-07-20T12:00:00Z",
            }
        )

        self.assertEqual(meta["version"], 12)
        self.assertEqual(meta["productCount"], 204)
        self.assertEqual(meta["updatedAt"], "2026-07-20T12:00:00Z")

    def test_reference_catalog_response_adds_metadata_without_changing_items(self):
        with patch.object(reference, "_fetch_reference_catalog_meta", return_value={
            "version": 12,
            "productCount": 204,
            "updatedAt": "2026-07-20T12:00:00Z",
        }):
            response = reference._reference_catalog_response(
                items=[{"sap": "31032"}],
                extra={"query": "31032"},
            )

        self.assertEqual(response["catalogId"], "routespark-starter-catalog")
        self.assertEqual(response["version"], 12)
        self.assertEqual(response["productCount"], 204)
        self.assertEqual(response["updatedAt"], "2026-07-20T12:00:00Z")
        self.assertEqual(response["items"], [{"sap": "31032"}])
        self.assertEqual(response["query"], "31032")

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

    def test_reference_tag_aliases_align_walmart_search_terms(self):
        self.assertEqual(reference._tag_alias_like_query("walmart"), "%walmart%")
        self.assertEqual(reference._tag_alias_like_query("wal mart"), "%walmart%")
        self.assertEqual(reference._tag_alias_like_query("gv"), "%walmart%")
        self.assertEqual(reference._tag_alias_like_query("wm"), "%walmart%")

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
                    "tags": ["better_for_you"],
                    "unitPack": 8,
                    "searchPriority": 100,
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
        self.assertEqual(rows[0][6], ["better_for_you"])
        self.assertEqual(rows[0][7], 16)
        self.assertEqual(rows[0][8], 8)
        self.assertEqual(rows[0][9], 100)

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

        self.assertEqual(rows[0][11], "routespark-starter-catalog/31032.png")
        self.assertEqual(rows[0][12], "routespark-starter-catalog/31032.png")

    def test_reference_nullable_integer_cleaners_do_not_coerce_unknown_to_zero(self):
        for value in (None, "", "not-a-number", 0, "0", -1):
            self.assertIsNone(load_reference_catalog._clean_optional_positive_int(value))
            self.assertIsNone(reference._clean_optional_positive_int(value))

        self.assertEqual(load_reference_catalog._clean_optional_positive_int("10"), 10)
        self.assertEqual(reference._clean_optional_positive_int("10"), 10)
        self.assertIsNone(load_reference_catalog._clean_optional_int(""))
        self.assertIsNone(reference._clean_optional_int(""))
        self.assertEqual(load_reference_catalog._clean_optional_int("100"), 100)
        self.assertEqual(reference._clean_optional_int("100"), 100)

    def test_reference_normalizer_returns_nullable_search_metadata(self):
        item = reference._normalize_reference_item(
            {
                "sap": "28934",
                "full_name": "Mission Flour Soft Taco",
                "case_pack": 15,
                "unit_pack": 10,
                "search_priority": 100,
            }
        )
        old_item = reference._normalize_reference_item(
            {
                "sap": "99999",
                "full_name": "Legacy Product",
                "case_pack": 12,
            }
        )

        self.assertEqual(item["unitPack"], 10)
        self.assertEqual(item["searchPriority"], 100)
        self.assertIsNone(old_item["unitPack"])
        self.assertIsNone(old_item["searchPriority"])

    def test_reference_loader_signature_changes_when_upc_changes(self):
        image_paths = {}
        signature_a = load_reference_catalog._catalog_signature(
            [
                {
                    "sap": "54511",
                    "upc": "11110-08472",
                    "fullName": "Kroger Zero Net Carb Street Taco",
                    "casePack": 16,
                }
            ],
            image_paths,
        )
        signature_b = load_reference_catalog._catalog_signature(
            [
                {
                    "sap": "54511",
                    "upc": "11110-99999",
                    "fullName": "Kroger Zero Net Carb Street Taco",
                    "casePack": 16,
                }
            ],
            image_paths,
        )

        self.assertNotEqual(signature_a, signature_b)

    def test_reference_loader_signature_changes_when_tags_change(self):
        image_paths = {}
        signature_a = load_reference_catalog._catalog_signature(
            [
                {
                    "sap": "37983",
                    "upc": "73731-07140",
                    "fullName": "Mission Gluten Free Tortilla 6ct",
                    "casePack": 15,
                    "tags": [],
                }
            ],
            image_paths,
        )
        signature_b = load_reference_catalog._catalog_signature(
            [
                {
                    "sap": "37983",
                    "upc": "73731-07140",
                    "fullName": "Mission Gluten Free Tortilla 6ct",
                    "casePack": 15,
                    "tags": ["better_for_you"],
                }
            ],
            image_paths,
        )

        self.assertNotEqual(signature_a, signature_b)

    def test_reference_loader_signature_changes_when_search_metadata_changes(self):
        image_paths = {}
        base_product = {
            "sap": "28934",
            "upc": "73731-00415",
            "fullName": "Mission Flour Soft Taco",
            "casePack": 15,
        }
        base_signature = load_reference_catalog._catalog_signature([base_product], image_paths)
        unit_pack_signature = load_reference_catalog._catalog_signature(
            [{**base_product, "unitPack": 10}],
            image_paths,
        )
        priority_signature = load_reference_catalog._catalog_signature(
            [{**base_product, "searchPriority": 100}],
            image_paths,
        )

        self.assertNotEqual(base_signature, unit_pack_signature)
        self.assertNotEqual(base_signature, priority_signature)

    def test_reference_loader_keeps_version_when_signature_is_unchanged(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (4, "same-signature")

        version = load_reference_catalog._next_catalog_version(
            cursor,
            catalog_id="routespark-starter-catalog",
            signature="same-signature",
        )

        self.assertEqual(version, 4)

    def test_reference_loader_increments_version_when_signature_changes(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (4, "old-signature")

        version = load_reference_catalog._next_catalog_version(
            cursor,
            catalog_id="routespark-starter-catalog",
            signature="new-signature",
        )

        self.assertEqual(version, 5)

    def test_reference_loader_schema_validation_accepts_required_columns(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (table_name, column_name)
            for table_name, columns in load_reference_catalog.REFERENCE_SCHEMA_COLUMNS.items()
            for column_name in columns
        ]

        load_reference_catalog._validate_reference_schema(cursor)

        self.assertIn("information_schema.columns", cursor.execute.call_args.args[0])

    def test_reference_loader_schema_validation_fails_without_running_ddl(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("reference_catalog_items", "catalog_id"),
            ("reference_catalog_items", "sap"),
        ]

        with self.assertRaisesRegex(RuntimeError, "apply the PostgreSQL schema migration"):
            load_reference_catalog._validate_reference_schema(cursor)

        executed_sql = cursor.execute.call_args.args[0].upper()
        self.assertNotIn("CREATE TABLE", executed_sql)
        self.assertNotIn("ALTER TABLE", executed_sql)

    def test_reference_loader_upsert_writes_search_metadata_columns(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "catalog.json"
            source.write_text(
                json.dumps(
                    [
                        {
                            "sap": "28934",
                            "fullName": "Mission Flour Soft Taco",
                            "casePack": 15,
                            "unitPack": 10,
                            "searchPriority": 100,
                        }
                    ]
                )
            )
            connection = MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.fetchall.return_value = [
                (table_name, column_name)
                for table_name, columns in load_reference_catalog.REFERENCE_SCHEMA_COLUMNS.items()
                for column_name in columns
            ]
            cursor.fetchone.return_value = None

            with patch.object(load_reference_catalog, "get_connection", return_value=connection), patch.object(
                load_reference_catalog,
                "execute_values",
            ) as execute_values:
                count = load_reference_catalog.load_reference_catalog(
                    source,
                    source_label="test",
                    image_manifest=None,
                )

        self.assertEqual(count, 1)
        upsert_sql = execute_values.call_args.args[1]
        upsert_rows = execute_values.call_args.args[2]
        self.assertIn("unit_pack", upsert_sql)
        self.assertIn("search_priority", upsert_sql)
        self.assertEqual(upsert_rows[0][8], 10)
        self.assertEqual(upsert_rows[0][9], 100)
        connection.commit.assert_called_once()

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

    def test_reference_image_root_honors_deployment_configuration(self):
        with TemporaryDirectory() as tmp, patch.dict(
            reference.os.environ,
            {"REFERENCE_CATALOG_IMAGE_ROOT": tmp},
        ):
            self.assertEqual(reference._image_root(), Path(tmp))


class ReferenceCatalogAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    def _demo_db(self):
        db = _FakeDB()
        db.set_document(("masterCatalog", "900000"), {"routeNumber": "900000"})
        db.set_document(
            ("masterCatalog", "900000", "products", "90001"),
            {
                "sap": "90001",
                "fullName": "Generic Inventory Item A",
                "brand": "Generic",
                "category": "general",
                "casePack": 6,
                "displayOrder": 1,
                "active": True,
            },
        )
        db.set_document(
            ("masterCatalog", "900000", "products", "90002"),
            {
                "sap": "90002",
                "fullName": "Generic Inventory Item B",
                "brand": "Generic",
                "category": "general",
                "casePack": 8,
                "displayOrder": 2,
                "active": False,
            },
        )
        return db

    def test_reference_override_parser_accepts_only_uid_route_pairs(self):
        self.assertEqual(
            reference._parse_reference_catalog_route_overrides(
                "apple-review=900000, malformed,missing-route=,bad-route=abc"
            ),
            {"apple-review": "900000"},
        )

    async def test_starter_catalog_override_returns_only_protected_route_products(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        db = self._demo_db()

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}), patch.object(
            reference,
            "_fetch_reference_catalog_items",
        ) as fetch_postgres:
            response = await endpoint(
                _FakeRequest(),
                500,
                True,
                {"uid": "apple-review"},
                db,
            )

        fetch_postgres.assert_not_called()
        self.assertEqual(response["catalogId"], "routespark-starter-catalog")
        self.assertEqual(response["version"], 1)
        self.assertEqual(response["productCount"], 2)
        self.assertEqual([item["sap"] for item in response["items"]], ["90001", "90002"])
        self.assertEqual(response["items"][0]["source"], "protected-demo-route")

    async def test_starter_catalog_override_hides_inactive_products_by_default(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        db = self._demo_db()

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}):
            response = await endpoint(
                _FakeRequest(),
                500,
                False,
                {"uid": "apple-review"},
                db,
            )

        self.assertEqual(response["productCount"], 1)
        self.assertEqual([item["sap"] for item in response["items"]], ["90001"])

    async def test_reference_search_override_never_returns_global_products(self):
        endpoint = inspect.unwrap(reference.search_reference_catalog)
        db = self._demo_db()

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}), patch.object(
            reference,
            "_fetch_reference_items_by_search",
        ) as fetch_postgres:
            response = await endpoint(
                _FakeRequest(),
                "Inventory Item B",
                25,
                True,
                {"uid": "apple-review"},
                db,
            )

        fetch_postgres.assert_not_called()
        self.assertEqual([item["sap"] for item in response["items"]], ["90002"])

    async def test_reference_item_override_rejects_non_demo_sap(self):
        endpoint = inspect.unwrap(reference.get_reference_catalog_item)
        db = self._demo_db()

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}), patch.object(
            reference,
            "_fetch_reference_item_by_sap",
        ) as fetch_postgres:
            with self.assertRaises(HTTPException) as raised:
                await endpoint(
                    _FakeRequest(),
                    "31032",
                    True,
                    {"uid": "apple-review"},
                    db,
                )

        fetch_postgres.assert_not_called()
        self.assertEqual(raised.exception.status_code, 404)

    async def test_reference_item_override_returns_generic_item(self):
        endpoint = inspect.unwrap(reference.get_reference_catalog_item)
        db = self._demo_db()

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}):
            response = await endpoint(
                _FakeRequest(),
                "90001",
                True,
                {"uid": "apple-review"},
                db,
            )

        self.assertEqual(response["item"]["sap"], "90001")
        self.assertEqual(response["item"]["fullName"], "Generic Inventory Item A")
        self.assertEqual(response["item"]["casePack"], 6)

    async def test_reference_image_override_never_exposes_global_brand_image(self):
        endpoint = inspect.unwrap(reference.get_reference_catalog_image)

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}), patch.object(
            reference,
            "_fetch_reference_image_path",
        ) as fetch_postgres:
            with self.assertRaises(HTTPException) as raised:
                await endpoint(
                    _FakeRequest(),
                    "31032",
                    {"uid": "apple-review"},
                )

        fetch_postgres.assert_not_called()
        self.assertEqual(raised.exception.status_code, 404)

    async def test_unmapped_user_still_uses_global_postgres_reference(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        db = self._demo_db()
        global_items = [{"sap": "31032"}]

        with patch.object(reference, "REFERENCE_CATALOG_ROUTE_OVERRIDES", {"apple-review": "900000"}), patch.object(
            reference,
            "_fetch_reference_catalog_items",
            return_value=global_items,
        ) as fetch_postgres, patch.object(
            reference,
            "_reference_catalog_response",
            return_value={"catalogId": "routespark-starter-catalog", "items": global_items},
        ):
            response = await endpoint(
                _FakeRequest(),
                500,
                True,
                {"uid": "normal-user"},
                db,
            )

        fetch_postgres.assert_called_once()
        self.assertEqual(response["items"], global_items)

    async def test_starter_catalog_reports_item_query_database_failure_as_retryable(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        with patch.object(
            reference,
            "_fetch_reference_catalog_items",
            side_effect=psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
        ):
            with self.assertRaises(StructuredApiError) as raised:
                await endpoint(_FakeRequest(), 500, True, {})

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.code, "REFERENCE_CATALOG_UNAVAILABLE")
        self.assertEqual(raised.exception.details, {"stage": "items"})

    async def test_starter_catalog_reports_metadata_database_failure_as_retryable(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        with patch.object(reference, "_fetch_reference_catalog_items", return_value=[]), patch.object(
            reference,
            "_reference_catalog_response",
            side_effect=psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
        ):
            with self.assertRaises(StructuredApiError) as raised:
                await endpoint(_FakeRequest(), 500, True, {})

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.code, "REFERENCE_CATALOG_UNAVAILABLE")
        self.assertEqual(raised.exception.details, {"stage": "metadata"})

    async def test_starter_catalog_does_not_misclassify_unexpected_defects(self):
        endpoint = inspect.unwrap(reference.get_starter_catalog)
        with patch.object(
            reference,
            "_fetch_reference_catalog_items",
            side_effect=RuntimeError("programming defect"),
        ):
            with self.assertRaisesRegex(RuntimeError, "programming defect"):
                await endpoint(_FakeRequest(), 500, True, {})


class SapListActivationApiTests(unittest.IsolatedAsyncioTestCase):
    def test_python_planner_matches_shared_fixture_summary(self):
        fixture_path = ROOT_DIR / "scripts" / "fixtures" / "sap-list-activation" / "basic-summary.json"
        fixture = json.loads(fixture_path.read_text())

        plan = catalog.build_sap_activation_plan(
            route_number=fixture["routeNumber"],
            reference_version=fixture["referenceVersion"],
            reference_products=fixture["referenceProducts"],
            route_products=fixture["routeProducts"],
            uploaded_saps=catalog.SapListActivationRequest(saps=fixture["uploadedSaps"]).saps,
            hide_missing_reference_items=fixture["hideMissingReferenceItems"],
        )

        self.assertEqual(plan["summary"], fixture["expectedSummary"])

    async def test_preview_returns_summary_and_writes_nothing(self):
        db = _FakeDB()
        db.set_document(
            ("masterCatalog", "988200", "products", "54511"),
            {
                "sap": "54511",
                "fullName": "Kroger Zero Net Carb Street Taco",
                "casePack": 16,
                "active": True,
                "catalogOrigin": "routespark-reference",
                "referenceSap": "54511",
                "referenceVersion": 11,
            },
        )
        payload = catalog.SapListActivationRequest(saps=["31032", "54511", "99999"])

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
        ):
            response = await _unwrap(catalog.preview_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        summary = response["summary"]
        self.assertTrue(summary["dryRun"])
        self.assertFalse(summary["applied"])
        self.assertEqual(summary["addedSaps"], ["31032"])
        self.assertEqual(summary["alreadyActiveCount"], 1)
        self.assertEqual(summary["unknownSaps"], ["99999"])
        self.assertEqual(db.batch_commits, 0)

    async def test_apply_adds_reference_doc_with_origin_fields(self):
        db = _FakeDB()
        payload = catalog.SapListActivationRequest(saps=["54773"])

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=14)
        ):
            response = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        doc = db.document_data(("masterCatalog", "988200", "products", "54773"))
        self.assertTrue(response["summary"]["applied"])
        self.assertEqual(response["summary"]["addedSaps"], ["54773"])
        self.assertEqual(doc["catalogOrigin"], "routespark-reference")
        self.assertEqual(doc["referenceSap"], "54773")
        self.assertEqual(doc["referenceVersion"], 14)
        self.assertEqual(doc["casePack"], 8)
        self.assertEqual(doc["upc"], "075202303167")
        self.assertIn("createdAt", doc)
        self.assertIn("updatedAt", doc)

    async def test_apply_reactivates_inactive_reference_row_and_refreshes_fields(self):
        db = _FakeDB()
        db.set_document(
            ("masterCatalog", "988200", "products", "54773"),
            {
                "sap": "54773",
                "fullName": "Old Deli Fresh Name",
                "casePack": 10,
                "active": False,
                "catalogOrigin": "routespark-reference",
                "referenceSap": "54773",
                "referenceVersion": 9,
                "createdAt": 123,
            },
        )
        payload = catalog.SapListActivationRequest(saps=["54773"])

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=15)
        ):
            response = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        doc = db.document_data(("masterCatalog", "988200", "products", "54773"))
        self.assertEqual(response["summary"]["activatedSaps"], ["54773"])
        self.assertTrue(doc["active"])
        self.assertEqual(doc["fullName"], "Hint of Lime Deli Fresh Chips")
        self.assertEqual(doc["casePack"], 8)
        self.assertEqual(doc["referenceVersion"], 15)
        self.assertEqual(doc["createdAt"], 123)

    async def test_hide_missing_hides_only_reference_origin_and_preserves_user_product(self):
        db = _FakeDB()
        db.set_document(
            ("masterCatalog", "988200", "products", "31032"),
            {"sap": "31032", "active": True, "catalogOrigin": "routespark-reference", "referenceSap": "31032"},
        )
        db.set_document(
            ("masterCatalog", "988200", "products", "54511"),
            {"sap": "54511", "active": True, "catalogOrigin": "routespark-reference", "referenceSap": "54511"},
        )
        db.set_document(
            ("masterCatalog", "988200", "products", "77777"),
            {"sap": "77777", "active": True, "catalogOrigin": "user-added", "fullName": "Local Item"},
        )
        payload = catalog.SapListActivationRequest(saps=["31032"], hideMissingReferenceItems=True)

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
        ):
            response = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertEqual(response["summary"]["hiddenReferenceSaps"], ["54511"])
        self.assertFalse(db.document_data(("masterCatalog", "988200", "products", "54511"))["active"])
        self.assertTrue(db.document_data(("masterCatalog", "988200", "products", "77777"))["active"])

    async def test_team_member_is_rejected_before_product_writes(self):
        db = _FakeDB()
        payload = catalog.SapListActivationRequest(saps=["31032"])

        with patch.object(catalog, "_require_catalog_owner", side_effect=HTTPException(403, "Catalog changes require route owner access")):
            with self.assertRaises(HTTPException) as raised:
                await _unwrap(catalog.apply_sap_list_activation)(
                    request=_FakeRequest(),
                    payload=payload,
                    route="988200",
                    decoded_token={"uid": "member-1"},
                    db=db,
                )

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(db.batch_commits, 0)

    async def test_zero_match_hide_missing_is_blocked(self):
        db = _FakeDB()
        db.set_document(
            ("masterCatalog", "988200", "products", "31032"),
            {"sap": "31032", "active": True, "catalogOrigin": "routespark-reference", "referenceSap": "31032"},
        )
        payload = catalog.SapListActivationRequest(saps=["99999"], hideMissingReferenceItems=True)

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
        ):
            with self.assertRaises(HTTPException) as raised:
                await _unwrap(catalog.apply_sap_list_activation)(
                    request=_FakeRequest(),
                    payload=payload,
                    route="988200",
                    decoded_token={"uid": "owner-1"},
                    db=db,
                )

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail["code"], "hide_missing_blocked")
        self.assertTrue(db.document_data(("masterCatalog", "988200", "products", "31032"))["active"])

    async def test_empty_and_all_already_active_apply_are_no_ops(self):
        db = _FakeDB()
        db.set_document(
            ("masterCatalog", "988200", "products", "31032"),
            {"sap": "31032", "active": True, "catalogOrigin": "routespark-reference", "referenceSap": "31032"},
        )

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
        ):
            empty = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=catalog.SapListActivationRequest(saps=[]),
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )
            already = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=catalog.SapListActivationRequest(saps=["31032"]),
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertTrue(empty["summary"]["applied"])
        self.assertTrue(already["summary"]["applied"])
        self.assertEqual(already["summary"]["alreadyActiveCount"], 1)
        self.assertEqual(db.batch_commits, 0)

    async def test_chunked_apply_splits_large_write_plan(self):
        db = _FakeDB()
        items = [
            {
                "sap": str(10000 + index),
                "fullName": f"Reference Item {index}",
                "casePack": 12,
                "displayOrder": index,
                "active": True,
            }
            for index in range(catalog.MAX_ROUTE_CATALOG_BATCH_WRITES + 1)
        ]
        payload = catalog.SapListActivationRequest(saps=[item["sap"] for item in items])

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=16, items=items)
        ):
            response = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertEqual(len(response["summary"]["addedSaps"]), catalog.MAX_ROUTE_CATALOG_BATCH_WRITES + 1)
        self.assertEqual(db.batch_commits, 2)
        self.assertEqual(db.batch_write_counts, [catalog.MAX_ROUTE_CATALOG_BATCH_WRITES, 1])

    async def test_adopted_route_forks_before_first_portal_write(self):
        db = _FakeDB()
        db.set_document(
            ("routes", "988200"),
            {
                "catalog": {
                    "mode": "adopted",
                    "sourceCatalogId": "shared-source",
                    "adoptedVersion": 3,
                    "shareEligible": False,
                    "publishRequired": False,
                }
            },
        )
        db.set_document(("sharedCatalogs", "shared-source", "adopters", "988200"), {"routeNumber": "988200"})
        db.set_document(("sharedCatalogs", "shared-source", "adopters", "owner-1"), {"ownerUid": "owner-1"})
        payload = catalog.SapListActivationRequest(saps=["31032"])

        with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
            catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
        ):
            response = await _unwrap(catalog.apply_sap_list_activation)(
                request=_FakeRequest(),
                payload=payload,
                route="988200",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        route_doc = db.document_data(("routes", "988200"))
        self.assertTrue(response["summary"]["forked"])
        self.assertEqual(route_doc["catalog"]["mode"], "forked")
        self.assertTrue(route_doc["catalog"]["shareEligible"])
        self.assertTrue(route_doc["catalog"]["publishRequired"])
        self.assertIsNone(db.document_data(("sharedCatalogs", "shared-source", "adopters", "988200")))
        self.assertIsNone(db.document_data(("sharedCatalogs", "shared-source", "adopters", "owner-1")))

    async def test_source_and_forked_routes_do_not_rewrite_lineage(self):
        for mode in ("source", "forked"):
            db = _FakeDB()
            db.set_document(("routes", "988200"), {"catalog": {"mode": mode, "sourceCatalogId": "988200"}})

            with patch.object(catalog, "_require_catalog_owner", return_value={"profile": {"role": "owner"}}), patch.object(
                catalog, "fetch_reference_catalog_for_activation", return_value=_reference_payload(version=12)
            ):
                response = await _unwrap(catalog.apply_sap_list_activation)(
                    request=_FakeRequest(),
                    payload=catalog.SapListActivationRequest(saps=["31032"]),
                    route="988200",
                    decoded_token={"uid": "owner-1"},
                    db=db,
                )

            self.assertFalse(response["summary"]["forked"])
            self.assertEqual(db.document_data(("routes", "988200"))["catalog"]["mode"], mode)


if __name__ == "__main__":
    unittest.main()

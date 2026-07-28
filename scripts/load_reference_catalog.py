#!/usr/bin/env python3
"""Load the repo reference catalog into PostgreSQL.

This seeds the cluster-backed lookup table from the canonical starter catalog
JSON. It does not touch Firebase or any route-scoped user catalog.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from psycopg2.extras import execute_values

try:
    from .pg_schema import get_connection
except ImportError:
    from pg_schema import get_connection


DEFAULT_CATALOG_ID = "routespark-starter-catalog"
DEFAULT_SOURCE = Path(__file__).resolve().parents[2] / "data" / "catalogs" / "new_user_starter_master_products.json"
DEFAULT_IMAGE_MANIFEST = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "catalogs"
    / "product_images"
    / "routespark-starter-catalog-manifest.json"
)
REFERENCE_SCHEMA_COLUMNS = {
    "reference_catalog_items": {
        "catalog_id",
        "sap",
        "upc",
        "full_name",
        "brand",
        "category",
        "tags",
        "case_pack",
        "display_order",
        "image_path",
        "image_thumb_path",
        "source",
        "active",
    },
    "reference_catalog_meta": {
        "catalog_id",
        "version",
        "product_count",
        "signature",
        "source",
        "updated_at",
    },
}


def _clean_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _clean_int(value: Any, fallback: int = 0) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return number if number >= 0 else fallback


def _clean_tags(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    tags: List[str] = []
    seen = set()
    for entry in value:
        tag = _clean_text(entry)
        if tag and tag not in seen:
            tags.append(tag)
            seen.add(tag)
    return tags


def _catalog_image_path(value: Any) -> Optional[str]:
    text = _clean_text(value)
    if not text:
        return None
    marker = "product_images/"
    if marker in text:
        return text.split(marker, 1)[1].lstrip("/")
    return text.lstrip("/")


def _image_paths_by_sap(manifest_path: Optional[Path]) -> Dict[str, Dict[str, Optional[str]]]:
    if not manifest_path or not manifest_path.exists():
        return {}
    manifest = json.loads(manifest_path.read_text())
    entries = manifest.get("entries", []) if isinstance(manifest, dict) else []
    image_paths: Dict[str, Dict[str, Optional[str]]] = {}
    for entry in entries:
        sap = _clean_text(entry.get("sap"))
        if not sap:
            continue
        image_paths[sap] = {
            "imagePath": _catalog_image_path(entry.get("imagePath")),
            "imageThumbPath": _catalog_image_path(entry.get("imageThumbPath") or entry.get("imagePath")),
        }
    return image_paths


def _rows(
    products: Iterable[Dict[str, Any]],
    *,
    catalog_id: str,
    source_label: str,
    image_paths: Optional[Dict[str, Dict[str, Optional[str]]]] = None,
) -> List[tuple]:
    rows: List[tuple] = []
    image_paths = image_paths or {}
    for index, product in enumerate(products, start=1):
        sap = _clean_text(product.get("sap"))
        full_name = _clean_text(
            product.get("fullName") or product.get("itemDescription") or product.get("description")
        )
        case_pack = _clean_int(product.get("casePack") or product.get("caseCount"), 0)
        if not sap or not full_name or case_pack <= 0:
            raise ValueError(f"Invalid reference catalog row {index}: sap, name, and positive case pack are required")
        rows.append(
            (
                catalog_id,
                sap,
                _clean_text(product.get("upc")) or None,
                full_name,
                _clean_text(product.get("brand")) or None,
                _clean_text(product.get("category")) or None,
                _clean_tags(product.get("tags")),
                case_pack,
                _clean_int(product.get("displayOrder"), index),
                image_paths.get(sap, {}).get("imagePath"),
                image_paths.get(sap, {}).get("imageThumbPath"),
                source_label,
                bool(product.get("active", True)),
            )
        )
    return rows


def _catalog_signature(products: Iterable[Dict[str, Any]], image_paths: Dict[str, Dict[str, Optional[str]]]) -> str:
    normalized = []
    for product in products:
        sap = _clean_text(product.get("sap"))
        images = image_paths.get(sap, {})
        normalized.append(
            {
                "sap": sap,
                "upc": _clean_text(product.get("upc")),
                "fullName": _clean_text(
                    product.get("fullName") or product.get("itemDescription") or product.get("description")
                ),
                "brand": _clean_text(product.get("brand")),
                "category": _clean_text(product.get("category")),
                "tags": _clean_tags(product.get("tags")),
                "casePack": _clean_int(product.get("casePack") or product.get("caseCount"), 0),
                "displayOrder": _clean_int(product.get("displayOrder"), 0),
                "imagePath": images.get("imagePath") or None,
                "imageThumbPath": images.get("imageThumbPath") or None,
                "active": bool(product.get("active", True)),
            }
        )
    payload = json.dumps(sorted(normalized, key=lambda row: row["sap"]), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _next_catalog_version(cur: Any, *, catalog_id: str, signature: str) -> int:
    cur.execute(
        """
        SELECT version, signature
        FROM reference_catalog_meta
        WHERE catalog_id = %s
        LIMIT 1
        """,
        [catalog_id],
    )
    row = cur.fetchone()
    if not row:
        return 1
    existing_version = int(row[0] if not isinstance(row, dict) else row.get("version") or 0)
    existing_signature = row[1] if not isinstance(row, dict) else row.get("signature")
    if existing_signature == signature:
        return max(existing_version, 1)
    return max(existing_version, 0) + 1


def _validate_reference_schema(cur: Any) -> None:
    cur.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ANY(%s)
        """,
        [list(REFERENCE_SCHEMA_COLUMNS)],
    )
    present: Dict[str, set[str]] = {}
    for row in cur.fetchall():
        table_name = row[0] if not isinstance(row, dict) else row.get("table_name")
        column_name = row[1] if not isinstance(row, dict) else row.get("column_name")
        if table_name and column_name:
            present.setdefault(str(table_name), set()).add(str(column_name))

    missing = []
    for table_name, required_columns in REFERENCE_SCHEMA_COLUMNS.items():
        for column_name in sorted(required_columns - present.get(table_name, set())):
            missing.append(f"{table_name}.{column_name}")

    if missing:
        raise RuntimeError(
            "Reference catalog schema is not ready; apply the PostgreSQL schema migration "
            f"before loading data. Missing: {', '.join(missing)}"
        )


def load_reference_catalog(
    path: Path,
    *,
    catalog_id: str = DEFAULT_CATALOG_ID,
    source_label: str = "repo",
    image_manifest: Optional[Path] = DEFAULT_IMAGE_MANIFEST,
) -> int:
    products = json.loads(path.read_text())
    if not isinstance(products, list):
        raise ValueError(f"Expected a JSON array in {path}")

    image_paths = _image_paths_by_sap(image_manifest)
    rows = _rows(products, catalog_id=catalog_id, source_label=source_label, image_paths=image_paths)
    signature = _catalog_signature(products, image_paths)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '15s'")
            _validate_reference_schema(cur)
            version = _next_catalog_version(cur, catalog_id=catalog_id, signature=signature)
            execute_values(
                cur,
                """
                INSERT INTO reference_catalog_items (
                    catalog_id, sap, upc, full_name, brand, category, tags,
                    case_pack, display_order, image_path, image_thumb_path,
                    source, active
                )
                VALUES %s
                ON CONFLICT (catalog_id, sap) DO UPDATE SET
                    upc = EXCLUDED.upc,
                    full_name = EXCLUDED.full_name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags,
                    case_pack = EXCLUDED.case_pack,
                    display_order = EXCLUDED.display_order,
                    image_path = EXCLUDED.image_path,
                    image_thumb_path = EXCLUDED.image_thumb_path,
                    source = EXCLUDED.source,
                    active = EXCLUDED.active,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
            cur.execute(
                """
                INSERT INTO reference_catalog_meta (
                    catalog_id, version, product_count, signature, source, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (catalog_id) DO UPDATE SET
                    version = EXCLUDED.version,
                    product_count = EXCLUDED.product_count,
                    signature = EXCLUDED.signature,
                    source = EXCLUDED.source,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [catalog_id, version, len(rows), signature, source_label],
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the RouteSpark reference catalog into PostgreSQL")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--catalog-id", default=DEFAULT_CATALOG_ID)
    parser.add_argument("--source-label", default="repo")
    parser.add_argument("--image-manifest", type=Path, default=DEFAULT_IMAGE_MANIFEST)
    args = parser.parse_args()

    count = load_reference_catalog(
        args.source,
        catalog_id=args.catalog_id,
        source_label=args.source_label,
        image_manifest=args.image_manifest,
    )
    print(f"Loaded {count} reference catalog items into {args.catalog_id}")


if __name__ == "__main__":
    main()

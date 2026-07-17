#!/usr/bin/env python3
"""Load the repo reference catalog into PostgreSQL.

This seeds the cluster-backed lookup table from the canonical starter catalog
JSON. It does not touch Firebase or any route-scoped user catalog.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from psycopg2.extras import execute_values

try:
    from .pg_schema import create_schema, get_connection
except ImportError:
    from pg_schema import create_schema, get_connection


DEFAULT_CATALOG_ID = "routespark-starter-catalog"
DEFAULT_SOURCE = Path(__file__).resolve().parents[2] / "data" / "catalogs" / "new_user_starter_master_products.json"
DEFAULT_IMAGE_MANIFEST = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "catalogs"
    / "product_images"
    / "routespark-starter-catalog-manifest.json"
)


def _clean_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _clean_int(value: Any, fallback: int = 0) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return number if number >= 0 else fallback


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
                case_pack,
                _clean_int(product.get("displayOrder"), index),
                image_paths.get(sap, {}).get("imagePath"),
                image_paths.get(sap, {}).get("imageThumbPath"),
                source_label,
                bool(product.get("active", True)),
            )
        )
    return rows


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
    conn = get_connection()
    try:
        create_schema(conn)
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO reference_catalog_items (
                    catalog_id, sap, upc, full_name, brand, category,
                    case_pack, display_order, image_path, image_thumb_path,
                    source, active
                )
                VALUES %s
                ON CONFLICT (catalog_id, sap) DO UPDATE SET
                    upc = EXCLUDED.upc,
                    full_name = EXCLUDED.full_name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
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

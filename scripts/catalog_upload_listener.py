"""Catalog Upload Listener - watches Firebase for order guide uploads.

Uses Firebase as the communication bus:
1. App uploads file to Firebase Storage at catalogs/{routeNumber}/{requestId}/{filename}
2. App creates request doc at catalogRequests/{routeNumber}/uploads/{requestId} with status: 'pending'
3. This listener watches for pending requests, downloads and parses the file
4. Results are written back to the request doc, status updated to 'processed' or 'failed'
5. App listens via onSnapshot and gets the parsed products

Usage:
    python scripts/catalog_upload_listener.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

from google.cloud import firestore, storage  # type: ignore
import csv
import re

# Add pcf_pipeline directory for order_guide imports
# Try multiple possible locations
PCF_PATHS = [
    '/Users/kylemacmini/projects/pcf_pipeline',
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../projects/pcf_pipeline')),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../projects/pcf_pipeline')),
]
for path in PCF_PATHS:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)
        break

try:
    from order_guide.product_catalog_parser import parse_pdf  # Auto-detects format
    from order_guide.xlsx_parser import parse_xlsx
except ImportError as e:
    print(f"Warning: Could not import order_guide parsers: {e}")
    parse_pdf = None  # type: ignore
    parse_xlsx = None  # type: ignore


def log(msg: str):
    print(f"[CatalogUpload] {msg}", flush=True)


def download_from_storage(storage_url: str, temp_dir: Path, storage_client: storage.Client) -> Optional[Path]:
    """Download file from Firebase Storage URL (gs://bucket/path)."""
    if not storage_url.startswith('gs://'):
        log(f"Invalid storage URL: {storage_url}")
        return None
    
    # Parse gs://bucket/path format
    url_without_prefix = storage_url[5:]  # Remove 'gs://'
    parts = url_without_prefix.split('/', 1)
    if len(parts) != 2:
        log(f"Invalid storage URL format: {storage_url}")
        return None
    
    bucket_name, blob_path = parts
    filename = blob_path.split('/')[-1]
    
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        local_path = temp_dir / filename
        blob.download_to_filename(str(local_path))
        log(f"Downloaded {filename} ({local_path.stat().st_size} bytes)")
        return local_path
    except Exception as e:
        log(f"Error downloading from storage: {e}")
        return None


def parse_file(file_path: Path, catalog_type: str = 'order_guide') -> List[Dict[str, Any]]:
    """Parse catalog file (PDF or Excel/CSV) and return list of products.
    
    Args:
        file_path: Path to the file to parse
        catalog_type: 'order_guide' or 'product_catalog' - determines which parser to use
    """
    ext = file_path.suffix.lower()
    
    if ext == '.pdf':
        if parse_pdf is None:
            raise RuntimeError("PDF parser not available. Check pcf_pipeline installation.")
        # For PDFs, use auto-detect which will pick the right parser
        # based on content (Product Catalog has "MISSION - GUERRERO" header)
        products = parse_pdf(str(file_path))
    elif ext in ('.xlsx', '.xls'):
        if parse_xlsx is None:
            raise RuntimeError("XLSX parser not available. Check pcf_pipeline installation.")
        # Spreadsheets are always Order Guide format
        products = parse_xlsx(str(file_path))
    elif ext == '.csv':
        # CSV is NOT supported by openpyxl (xlsx_parser). Parse it directly.
        return _parse_price_list_csv(file_path)
    else:
        # Try spreadsheet first, fall back to PDF
        try:
            products = parse_xlsx(str(file_path))
        except Exception:
            products = parse_pdf(str(file_path))
    
    log(f"Parsed as {catalog_type}: {len(products)} products")
    
    # Convert to dict format for Firebase
    result = []
    for p in products:
        product_dict = {
            'sap': p.sap,
            'fullName': getattr(p, 'full_name', None) or getattr(p, 'fullName', ''),
            'casePack': getattr(p, 'case_pack', None) or getattr(p, 'casePack', 0),
            'brand': p.brand,
            'category': p.category,
            'displayOrder': getattr(p, 'display_order', None) or getattr(p, 'displayOrder', 0),
        }
        # Include UPC if available (from Product Catalogs)
        upc = getattr(p, 'upc', None) or getattr(p, 'UPC', '')
        if upc:
            product_dict['upc'] = upc
        result.append(product_dict)
    
    return result


SAP_RE = re.compile(r"^\d{4,6}$")


def _infer_category_from_header(header: str) -> str:
    h = (header or "").strip().lower()
    if not h:
        return "uncategorized"
    retailer_words = ("walmart", "target", "kroger", "safeway", "albertsons", "chef", "us foods")
    if any(w in h for w in retailer_words):
        return "private_label"
    if "club" in h:
        return "club"
    if "chip" in h or "chicharr" in h or "salsa" in h or "dip" in h:
        return "chips"
    if "tostad" in h:
        return "tostada"
    if "wrap" in h:
        return "wraps"
    if "corn" in h or "maiz" in h:
        return "corn"
    if "flour" in h or "wheat" in h:
        return "flour"
    return "uncategorized"


def _infer_category_from_desc(desc: str) -> str:
    d = (desc or "").strip().lower()
    if any(w in d for w in ("target", "walmart", "kroger", "safeway", "s select", "signature select", "great value")):
        return "private_label"
    if "tostada" in d:
        return "tostada"
    if "chip" in d or "chicharr" in d or "salsa" in d or "dip" in d or "cracklin" in d:
        return "chips"
    if "wrap" in d:
        return "wraps"
    if "corn" in d or "maiz" in d:
        return "corn"
    if any(tok in d for tok in ("flour", "wheat", "burrito", "fajita", "taco", "tortilla")):
        return "flour"
    return "other"


def _infer_brand(desc: str) -> str:
    d = (desc or "").strip()
    if not d:
        return "Unknown"
    for mw in ("La Providencia", "La Fiesta", "S Select", "Great Value", "Deli Sonora"):
        if d.lower().startswith(mw.lower()):
            return mw
    if d.lower().startswith("deli tortillas") or d.lower().startswith("deli "):
        return "Deli Tortillas"
    return d.split()[0] if d.split() else "Unknown"


def _clean_upc(upc: str) -> str:
    u = (upc or "").strip()
    if not u:
        return ""
    return u.replace("-", "").replace(" ", "")


def _open_csv_reader(path: Path):
    # Price list CSVs are commonly cp1252. Try utf-8 first, then cp1252.
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            f = open(path, newline="", encoding=enc)
            reader = csv.reader(f)
            header = next(reader)
            return f, reader, header
        except Exception:
            try:
                f.close()
            except Exception:
                pass
            continue
    raise RuntimeError(f"Could not open CSV with supported encodings: {path}")


def _parse_price_list_csv(file_path: Path) -> List[Dict[str, Any]]:
    """Parse the two-up price list CSV format into Firebase product dicts.

    This supports CSVs exported from the single-page price list PDF that contain
    two products per row with repeated header blocks:
      Left:  SAP #, UPC #, ITEM DESCRIPTION, UNIT PACK, CASE PACK
      Right: SAP #, UPC #, ITEM DESCRIPTION, UNIT PACK, CASE PACK
    """
    f, reader, header = _open_csv_reader(file_path)
    try:
        # Find column indices by header occurrences.
        sap_idxs = [i for i, h in enumerate(header) if (h or "").strip().upper() == "SAP #"]
        desc_idxs = [i for i, h in enumerate(header) if (h or "").strip().upper() == "ITEM DESCRIPTION"]
        case_idxs = [i for i, h in enumerate(header) if "CASE" in (h or "").upper() and "PACK" in (h or "").upper()]
        upc_idxs = [i for i, h in enumerate(header) if (h or "").strip().upper() in ("UPC #", "UPC")]

        if len(sap_idxs) < 1 or len(desc_idxs) < 1 or len(case_idxs) < 1:
            raise RuntimeError(f"Unrecognized CSV header format ({len(header)} cols): {header[:12]}")

        # Heuristic mapping for the common two-up file
        left_sap = sap_idxs[0]
        left_upc = upc_idxs[0] if upc_idxs else left_sap + 1
        left_desc = desc_idxs[0]
        left_case = case_idxs[0]

        right_sap = sap_idxs[1] if len(sap_idxs) > 1 else None
        # Right UPC/desc/case should be after right_sap; pick first occurrence after that.
        right_upc = None
        right_desc = None
        right_case = None
        if right_sap is not None:
            for idx in upc_idxs:
                if idx > right_sap:
                    right_upc = idx
                    break
            for idx in desc_idxs:
                if idx > right_sap:
                    right_desc = idx
                    break
            for idx in case_idxs:
                if idx > right_sap:
                    right_case = idx
                    break
            # Fallbacks for known layout (SAP, ORDER, UPC, DESC, UNIT, CASE)
            right_upc = right_upc if right_upc is not None else right_sap + 2
            right_desc = right_desc if right_desc is not None else right_sap + 3
            right_case = right_case if right_case is not None else right_sap + 5

        products: List[Dict[str, Any]] = []
        seen = set()
        display = 0
        left_cat = "uncategorized"
        right_cat = "uncategorized"

        for row in reader:
            if not row:
                continue

            # Category headers show up in description columns when SAP is empty.
            if (row[left_sap] if left_sap < len(row) else "").strip() == "" and (row[left_desc] if left_desc < len(row) else "").strip():
                left_cat = _infer_category_from_header(row[left_desc])
            if right_sap is not None:
                rs = (row[right_sap] if right_sap < len(row) else "").strip()
                rd = (row[right_desc] if right_desc is not None and right_desc < len(row) else "").strip()
                if rs == "" and rd and not SAP_RE.match(rd):
                    right_cat = _infer_category_from_header(rd)

            # Left product
            sap = (row[left_sap] if left_sap < len(row) else "").strip()
            if SAP_RE.match(sap) and sap not in seen:
                case_s = (row[left_case] if left_case < len(row) else "").strip()
                if case_s.isdigit() and int(case_s) > 0:
                    desc = (row[left_desc] if left_desc < len(row) else "").strip()
                    if desc:
                        upc = _clean_upc(row[left_upc] if left_upc < len(row) else "")
                        cat = left_cat if left_cat != "uncategorized" else _infer_category_from_desc(desc)
                        prod = {
                            "sap": sap,
                            "fullName": desc,
                            "casePack": int(case_s),
                            "brand": _infer_brand(desc),
                            "category": cat,
                            "displayOrder": display,
                        }
                        if upc:
                            prod["upc"] = upc
                        products.append(prod)
                        seen.add(sap)
                        display += 1

            # Right product
            if right_sap is not None and right_desc is not None and right_case is not None:
                sap2 = (row[right_sap] if right_sap < len(row) else "").strip()
                if SAP_RE.match(sap2) and sap2 not in seen:
                    case2_s = (row[right_case] if right_case < len(row) else "").strip()
                    if case2_s.isdigit() and int(case2_s) > 0:
                        desc2 = (row[right_desc] if right_desc < len(row) else "").strip()
                        if desc2:
                            upc2 = _clean_upc(row[right_upc] if right_upc is not None and right_upc < len(row) else "")
                            cat2 = right_cat if right_cat != "uncategorized" else _infer_category_from_desc(desc2)
                            prod2 = {
                                "sap": sap2,
                                "fullName": desc2,
                                "casePack": int(case2_s),
                                "brand": _infer_brand(desc2),
                                "category": cat2,
                                "displayOrder": display,
                            }
                            if upc2:
                                prod2["upc"] = upc2
                            products.append(prod2)
                            seen.add(sap2)
                            display += 1

        log(f"Parsed CSV price list: {len(products)} products")
        return products
    finally:
        try:
            f.close()
        except Exception:
            pass


def process_upload_request(
    db: firestore.Client,
    storage_client: storage.Client,
    route_number: str,
    request_id: str,
    request_data: Dict[str, Any]
):
    """Process a single upload request."""
    log(f"Processing upload {request_id} for route {route_number}")
    
    request_ref = db.collection('catalogRequests').document(route_number).collection('uploads').document(request_id)
    
    try:
        storage_url = request_data.get('storageUrl', '')
        filename = request_data.get('fileName', 'unknown')
        catalog_type = request_data.get('catalogType', 'order_guide')
        
        if not storage_url:
            raise ValueError("No storageUrl in request")
        
        log(f"Processing {catalog_type}: {filename}")
        
        # Download file to temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            local_file = download_from_storage(storage_url, temp_path, storage_client)
            
            if not local_file:
                raise ValueError(f"Failed to download file from {storage_url}")
            
            # Parse the file with the specified catalog type
            products = parse_file(local_file, catalog_type)
            
            if not products:
                raise ValueError("No products found in file")
            
            log(f"Parsed {len(products)} products from {filename}")
            
            # Update request with results
            request_ref.update({
                'status': 'processed',
                'catalogType': catalog_type,
                'products': products,
                'productCount': len(products),
                'processedAt': firestore.SERVER_TIMESTAMP,
            })
            
            log(f"✅ Successfully processed {request_id}: {len(products)} products")
            
    except Exception as e:
        log(f"❌ Error processing {request_id}: {e}")
        request_ref.update({
            'status': 'failed',
            'error': str(e),
            'processedAt': firestore.SERVER_TIMESTAMP,
        })


def on_snapshot(col_snapshot, changes, read_time, db, storage_client):
    """Handle Firestore collection group snapshot updates."""
    for change in changes:
        if change.type.name != 'ADDED':
            continue
            
        doc = change.document
        data = doc.to_dict() or {}
        
        # Only process pending requests
        if data.get('status') != 'pending':
            continue
        
        # Extract route number and request ID from path
        # Path: catalogRequests/{routeNumber}/uploads/{requestId}
        path_parts = doc.reference.path.split('/')
        
        # Only process catalogRequests (not promoRequests or other uploads)
        if len(path_parts) < 4 or path_parts[0] != 'catalogRequests':
            continue
            
        route_number = path_parts[1]
        request_id = path_parts[3]
        
        log(f"📤 Catalog upload detected: {data.get('fileName', 'unknown')} for route {route_number}")
        process_upload_request(db, storage_client, route_number, request_id, data)


def main():
    parser = argparse.ArgumentParser(description="Catalog Upload Listener")
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    args = parser.parse_args()

    log("Starting Catalog Upload Listener...")

    # Initialize Firebase
    db = firestore.Client.from_service_account_json(args.serviceAccount)
    storage_client = storage.Client.from_service_account_json(args.serviceAccount)

    log("Connected to Firebase")

    # Watch the uploads collection group for all uploads
    # Filter for pending status in the callback (avoids needing a collection group index)
    col_group = db.collection_group('uploads')

    # Set up snapshot listener
    def handle_snapshot(col_snapshot, changes, read_time):
        on_snapshot(col_snapshot, changes, read_time, db, storage_client)

    watch = col_group.on_snapshot(handle_snapshot)
    log("Listening for catalog upload requests...")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Shutting down...")
        watch.unsubscribe()


if __name__ == "__main__":
    main()

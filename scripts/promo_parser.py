"""Promo attachment parser for Excel and PDF files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

try:
    import pdfplumber  # type: ignore
except ImportError:  # pragma: no cover
    pdfplumber = None


def parse_excel(path: str | Path) -> List[Dict]:
    import re
    from datetime import datetime
    
    # Try reading with header row 0 first to see the structure
    df = pd.read_excel(path, header=0)
    
    # Check if first row contains actual headers like "Account", "SAP Codes"
    # (Weekly Executables format has merged header cells, real headers in row 1)
    first_row = df.iloc[0].astype(str).str.lower().tolist() if len(df) > 0 else []
    if any("account" in val or "sap" in val for val in first_row):
        # Real headers are in the first data row - use row 1 as header
        df = pd.read_excel(path, header=1)
    
    if df is None or df.empty:
        return []
    
    # Store original column names for positional access
    orig_columns = list(df.columns)
    
    # Clean column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Find SAP column - could be "sap codes", "week X", or last column
    sap_col = next((c for c in df.columns if "sap" in c), None)
    if not sap_col:
        # Try "week" columns (Weekly Executables format)
        sap_col = next((c for c in df.columns if c.startswith("week")), None)
    if not sap_col:
        # Fall back to last column
        sap_col = df.columns[-1]
    
    # Find description column - "items on promo", "description", or second-to-last
    desc_col = next((c for c in df.columns if "item" in c and "promo" in c), None)
    if not desc_col:
        desc_col = next((c for c in df.columns if "desc" in c), None)
    if not desc_col and len(df.columns) >= 2:
        # Second to last column often has description in Weekly Executables
        desc_col = df.columns[-2]
    
    # Find price column
    price_col = next((c for c in df.columns if "price" in c), None)
    
    # Find account column - try "account" or first column (Weekly Executables format)
    account_col = next((c for c in df.columns if "account" in c), None)
    if not account_col:
        # First column often has account name in Weekly Executables format
        account_col = df.columns[0]
    
    # Find date columns - Weekly Executables uses positional columns 3 and 4 for start/end dates
    # Try to find by name first, then fall back to position
    start_date_col = next((c for c in df.columns if "start" in c), None)
    end_date_col = next((c for c in df.columns if "end" in c), None)
    
    # If not found by name, use positional (columns 3 and 4 in Weekly Executables format)
    if not start_date_col and len(df.columns) > 3:
        start_date_col = df.columns[3]
    if not end_date_col and len(df.columns) > 4:
        end_date_col = df.columns[4]
    
    def parse_date(val) -> str:
        """Convert date value to ISO string (YYYY-MM-DD)."""
        if pd.isna(val):
            return ""
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d")
        if isinstance(val, pd.Timestamp):
            return val.strftime("%Y-%m-%d")
        # Try parsing string dates
        val_str = str(val).strip()
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"]:
            try:
                return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    items: List[Dict] = []
    for _, row in df.iterrows():
        sap_raw = str(row.get(sap_col, "")).strip() if sap_col else ""
        desc = str(row.get(desc_col, "")).strip() if desc_col else ""
        price = str(row.get(price_col, "")).strip() if price_col else ""
        account = str(row.get(account_col, "")).strip() if account_col else ""
        
        # Extract dates
        start_date = parse_date(row.get(start_date_col)) if start_date_col else ""
        end_date = parse_date(row.get(end_date_col)) if end_date_col else ""
        
        # Skip empty rows or header-like rows
        if not sap_raw or sap_raw.lower() in ('nan', 'sap codes', 'sap', ''):
            continue
        if sap_raw.lower().startswith('week') or sap_raw.lower().startswith('unnamed'):
            continue
        
        # Parse multiple SAP codes (can be separated by "-", "/", ",", or spaces)
        # Examples: "32820", "7751 - 7752", "51534- 51538- 51531- 38460"
        sap_codes = re.findall(r'\b(\d{4,6})\b', sap_raw)
        
        if not sap_codes:
            continue
        
        # Create an item for each SAP code
        for sap in sap_codes:
            items.append({
                "sap_raw": sap,
                "sap_code": sap,
                "description": desc if desc.lower() != 'nan' else "",
                "price": price if price.lower() != 'nan' else "",
                "account": account if account.lower() != 'nan' else "",
                "start_date": start_date,
                "end_date": end_date,
            })
    
    return items


def parse_pdf(path: str | Path) -> List[Dict]:
    """Parse Weekly Executables PDF with table extraction.
    
    The PDF has tables with multiline cells where each line corresponds to a different
    promo item. This parser properly aligns SAP codes with their corresponding dates.
    """
    import re
    
    if pdfplumber is None:
        raise ImportError("pdfplumber is required for PDF parsing")

    items: List[Dict] = []
    
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            
            for table in tables:
                for row in table:
                    if not row or len(row) < 8:
                        continue
                    
                    # Parse multiline cells - each line is a different promo item
                    account = (row[0] or '').split('\n')[0].strip()
                    sap_lines = (row[7] or '').split('\n')
                    start_lines = (row[2] or '').split('\n')
                    end_lines = (row[3] or '').split('\n')
                    price_lines = (row[5] or '').split('\n')
                    desc_lines = (row[6] or '').split('\n')
                    
                    # Skip header rows
                    if 'Account' in account or 'SAP Codes' in (row[7] or ''):
                        continue
                    
                    # Process each DESCRIPTION line (not SAP line) to catch items with blank SAPs
                    for i, desc in enumerate(desc_lines):
                        desc = desc.strip()
                        if not desc:
                            continue
                        
                        # Skip header-like descriptions
                        if desc.lower() in ('items on promo', 'description'):
                            continue
                        
                        # Get SAP for this line (may be blank)
                        sap_raw = sap_lines[i].strip() if i < len(sap_lines) else ''
                        
                        # Extract SAP codes from this line (4-6 digit numbers)
                        sap_codes = re.findall(r'\b(\d{4,6})\b', sap_raw)
                        
                        # Get corresponding dates (use index, fallback to first)
                        start = start_lines[i].strip() if i < len(start_lines) else (start_lines[0].strip() if start_lines else '')
                        end = end_lines[i].strip() if i < len(end_lines) else (end_lines[0].strip() if end_lines else '')
                        price = price_lines[i].strip() if i < len(price_lines) else ''
                        
                        # Clean up price (remove extra spaces from PDF extraction)
                        price = re.sub(r'\s+', '', price)
                        
                        # Normalize dates to YYYY-MM-DD format
                        start_date = _normalize_date(start)
                        end_date = _normalize_date(end)
                        
                        if sap_codes:
                            # Has SAP codes - try to split variants and match
                            variants = _split_variants(desc)
                            
                            if len(variants) > 1 and len(variants) == len(sap_codes):
                                # Perfect match: each SAP corresponds to a variant
                                for sap, variant_desc in zip(sap_codes, variants):
                                    items.append({
                                        "sap_raw": sap,
                                        "sap_code": sap,
                                        "description": variant_desc,
                                        "price": price,
                                        "account": account,
                                        "start_date": start_date,
                                        "end_date": end_date,
                                    })
                            else:
                                # SAP count doesn't match variant count - keep original description
                                for sap in sap_codes:
                                    items.append({
                                        "sap_raw": sap,
                                        "sap_code": sap,
                                        "description": desc,
                                        "price": price,
                                        "account": account,
                                        "start_date": start_date,
                                        "end_date": end_date,
                                    })
                        else:
                            # No SAP - split variants and mark each for catalog matching
                            variants = _split_variants(desc)
                            for variant_desc in variants:
                                items.append({
                                    "sap_raw": "",
                                    "sap_code": "",
                                    "description": variant_desc,
                                    "price": price,
                                    "account": account,
                                    "start_date": start_date,
                                    "end_date": end_date,
                                    "needs_sap_match": True,
                                })
    
    return items


def _normalize_date(date_str: str) -> str:
    """Convert date string to YYYY-MM-DD format."""
    from datetime import datetime
    
    if not date_str:
        return ""
    
    # Try various formats
    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    return ""


def _split_variants(description: str) -> List[str]:
    """Split a description with variants into individual product descriptions.
    
    Examples:
        "Guerrero Zero Net Carb Original, Chipotle & Jalapeno" 
        -> ["Guerrero Zero Net Carb Original", "Guerrero Zero Net Carb Chipotle", "Guerrero Zero Net Carb Jalapeno"]
        
        "Mission 30ct Corn White/Yellow"
        -> ["Mission 30ct Corn White", "Mission 30ct Corn Yellow"]
    """
    import re
    
    # Known variant keywords that indicate where to split
    # Include common typos (orginal = original)
    variant_keywords = [
        'original', 'orginal',  # orginal is common typo
        'chipotle', 'jalapeno', 'jalapeño', 'spinach', 'tomato basil',
        'sundried', 'sun dried', 'white', 'yellow', 'plain', 'picante', 'mild', 'medium',
        'whole wheat', 'ww', 'flour', 'corn', 'butter', 'red pepper', 'tender',
        'chile limon', 'classica', 'casera', 'nortena', 'tajin'
    ]
    
    # Check for slash pattern like "White/Yellow" at end
    slash_pattern = r'^(.+?)\s+([\w]+(?:/[\w]+)+)$'
    match = re.match(slash_pattern, description)
    
    if match:
        base = match.group(1).strip()
        variants_str = match.group(2).strip()
        variants = variants_str.split('/')
        
        if len(variants) > 1:
            return [f"{base} {v.strip()}" for v in variants]
    
    # Check for parenthetical variants like "Corn (White & Yellow)"
    paren_pattern = r'^(.+?)\s*\(([^)]+)\)(.*)$'
    match = re.match(paren_pattern, description)
    
    if match:
        before = match.group(1).strip()
        inside = match.group(2).strip()
        after = match.group(3).strip()
        
        # Split contents of parentheses
        if '&' in inside or ',' in inside or '/' in inside:
            paren_variants = re.split(r'\s*[,&/]\s*|\s+and\s+', inside)
            paren_variants = [v.strip() for v in paren_variants if v.strip()]
            
            if len(paren_variants) > 1:
                # Handle remaining text after parentheses (more variants)
                if after and (',' in after or '&' in after):
                    after_variants = re.split(r'\s*[,&]\s*|\s+and\s+', after)
                    after_variants = [v.strip() for v in after_variants if v.strip()]
                    all_variants = paren_variants + after_variants
                else:
                    all_variants = paren_variants
                    if after.strip():
                        all_variants = [f"{v} {after.strip()}" for v in all_variants]
                
                return [f"{before} {v}".strip() for v in all_variants]
    
    # Find the last known variant keyword and use that as split point
    # "Guerrero Zero Net Carb Original, Chipotle & Jalapeno"
    # -> Base = "Guerrero Zero Net Carb", Variants = "Original, Chipotle & Jalapeno"
    
    desc_lower = description.lower()
    
    # Find first variant keyword to determine where base ends
    first_variant_pos = len(description)
    for kw in variant_keywords:
        pos = desc_lower.find(kw)
        if pos > 0 and pos < first_variant_pos:
            first_variant_pos = pos
    
    if first_variant_pos < len(description):
        base = description[:first_variant_pos].strip()
        # Remove trailing punctuation from base
        base = base.rstrip(' ,')
        variants_str = description[first_variant_pos:].strip()
        
        # Split by comma, & and 'and' - handle all separators uniformly
        variants = re.split(r'\s*[,&]\s*|\s+and\s+', variants_str)
        variants = [v.strip() for v in variants if v.strip()]
        
        # Further split any variants that contain '/' (e.g., "White/Yellow")
        expanded_variants = []
        for v in variants:
            if '/' in v and ' ' not in v:  # "White/Yellow" but not "A/B thing"
                expanded_variants.extend(v.split('/'))
            else:
                expanded_variants.append(v)
        
        if len(expanded_variants) >= 1:
            # Clean up variants - remove duplicates and standardize
            seen = set()
            unique_variants = []
            for v in expanded_variants:
                v_clean = v.strip()
                v_lower = v_clean.lower()
                if v_clean and v_lower not in seen:
                    seen.add(v_lower)
                    unique_variants.append(v_clean)
            
            # Only return splits if we have 2+ variants
            if len(unique_variants) >= 2:
                return [f"{base} {v}" for v in unique_variants]
    
    # No variants found, return original
    return [description]


def _match_description_to_sap(description: str, catalog: List[Dict], threshold: float = 0.5) -> Optional[str]:
    """Match a promo description to a SAP from the catalog using fuzzy matching.
    
    Args:
        description: Product description from promo
        catalog: List of catalog products with 'sap' and 'full_name' keys
        threshold: Minimum similarity score (0-1) to consider a match
        
    Returns:
        SAP code if match found, None otherwise
    """
    import difflib
    
    if not description or not catalog:
        return None
    
    desc_lower = description.lower()
    
    # Extract brand
    desc_brand = 'mission' if 'mission' in desc_lower else ('guerrero' if 'guerrero' in desc_lower else '')
    
    # Extract variant keywords (include common typos)
    variant_keywords = ['original', 'orginal', 'chipotle', 'jalapeno', 'jalapeño', 'spinach', 'tomato', 
                        'white', 'yellow', 'plain', 'picante', 'sundried', 'whole wheat', 'ww',
                        'znc', 'zero net', 'carb balance', 'protein', 'chickpea', 'gf']
    
    desc_variants = [kw for kw in variant_keywords if kw in desc_lower]
    
    # Expand abbreviations for matching
    expanded_desc = desc_lower.replace('zero net carb', 'znc').replace('ww', 'whole wheat')
    
    best_sap = None
    best_score = 0.0
    
    for product in catalog:
        full_name = (product.get('full_name') or product.get('fullName') or '').lower()
        if not full_name:
            continue
        
        # Extract catalog brand
        cat_brand = 'mission' if 'mission' in full_name else ('guerrero' if 'guerrero' in full_name else '')
        
        # Brand must match if both have brands
        if desc_brand and cat_brand and desc_brand != cat_brand:
            continue  # Skip - brand mismatch
        
        # Calculate base similarity
        score = difflib.SequenceMatcher(None, desc_lower, full_name).ratio()
        
        # Also try with expanded abbreviations
        expanded_name = full_name.replace('znc', 'zero net carb')
        score2 = difflib.SequenceMatcher(None, desc_lower, expanded_name).ratio()
        score = max(score, score2)
        
        # Boost for brand match
        if desc_brand and cat_brand and desc_brand == cat_brand:
            score += 0.15
        
        # Boost for variant keyword matches
        cat_variants = [kw for kw in variant_keywords if kw in full_name]
        common_variants = set(desc_variants) & set(cat_variants)
        if common_variants:
            score += 0.2 * len(common_variants)
        
        # Extra boost for exact variant match (Chipotle -> Chipotle)
        for kw in ['chipotle', 'jalapeno', 'jalapeño', 'spinach', 'original', 'orginal', 'white', 'yellow']:
            if kw in desc_lower and kw in full_name:
                score += 0.25
                break
        
        # Handle Original/Orginal matching to White (common naming convention)
        if ('original' in desc_lower or 'orginal' in desc_lower) and 'white' in full_name:
            score += 0.2
        
        if score > best_score:
            best_score = score
            best_sap = product.get('sap')
    
    if best_score >= threshold:
        return best_sap
    
    return None


def _validate_sap_description_match(sap: str, promo_desc: str, catalog: List[Dict], threshold: float = 0.3) -> bool:
    """Check if a SAP's catalog description matches the promo description.
    
    Returns True if they match reasonably well, False if mismatch (wrong SAP).
    """
    import difflib
    
    # Find catalog entry for this SAP
    catalog_entry = next((p for p in catalog if str(p.get('sap')) == str(sap)), None)
    if not catalog_entry:
        return True  # SAP not in catalog, can't validate
    
    catalog_name = (catalog_entry.get('full_name') or catalog_entry.get('fullName') or '').lower()
    promo_lower = promo_desc.lower()
    
    # Check similarity
    similarity = difflib.SequenceMatcher(None, promo_lower, catalog_name).ratio()
    
    # Also check if key brand words match
    promo_words = set(promo_lower.split())
    catalog_words = set(catalog_name.split())
    
    # Brand must match (Mission vs Guerrero)
    promo_brand = 'mission' if 'mission' in promo_lower else ('guerrero' if 'guerrero' in promo_lower else '')
    catalog_brand = 'mission' if 'mission' in catalog_name else ('guerrero' if 'guerrero' in catalog_name else '')
    
    if promo_brand and catalog_brand and promo_brand != catalog_brand:
        return False  # Brand mismatch - definitely wrong SAP
    
    return similarity >= threshold


def enrich_promo_items_with_catalog(
    items: List[Dict],
    catalog: List[Dict],
    route_number: str = ""
) -> List[Dict]:
    """Enrich promo items by matching descriptions to catalog SAPs.
    
    This handles:
    1. Items with missing SAPs - finds SAP from catalog by description
    2. Items with variant descriptions - splits and matches each variant
    3. Validates existing SAPs against catalog descriptions (catches wrong SAPs)
    
    Args:
        items: Parsed promo items
        catalog: Product catalog with sap/full_name
        route_number: Route number for context
        
    Returns:
        Enriched list of promo items with validated/filled SAPs
    """
    enriched = []
    
    for item in items:
        sap = item.get('sap_code') or item.get('sap_raw') or ''
        desc = item.get('description', '')
        
        if sap and sap.isdigit():
            # Has SAP - validate it matches the description
            if _validate_sap_description_match(sap, desc, catalog):
                # SAP is valid
                enriched.append(item)
            else:
                # SAP doesn't match description - try to find correct SAP
                variants = _split_variants(desc)
                matched_any = False
                
                for variant_desc in variants:
                    matched_sap = _match_description_to_sap(variant_desc, catalog)
                    if matched_sap:
                        new_item = item.copy()
                        new_item['sap_code'] = matched_sap
                        new_item['sap_raw'] = matched_sap
                        new_item['description'] = variant_desc
                        new_item['corrected_from_catalog'] = True
                        new_item['original_wrong_sap'] = sap
                        enriched.append(new_item)
                        matched_any = True
                
                if not matched_any:
                    # Couldn't correct - keep original but flag it
                    item['sap_mismatch_warning'] = True
                    enriched.append(item)
                    
        elif desc:
            # No SAP - try to match from description
            variants = _split_variants(desc)
            
            for variant_desc in variants:
                matched_sap = _match_description_to_sap(variant_desc, catalog)
                
                if matched_sap:
                    new_item = item.copy()
                    new_item['sap_code'] = matched_sap
                    new_item['sap_raw'] = matched_sap
                    new_item['description'] = variant_desc
                    new_item['matched_from_catalog'] = True
                    enriched.append(new_item)
    
    return enriched


def parse_promo_attachment(path: str | Path) -> List[Dict]:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        return parse_excel(path)
    if suffix == ".pdf":
        return parse_pdf(path)
    raise ValueError(f"Unsupported promo attachment type: {suffix}")


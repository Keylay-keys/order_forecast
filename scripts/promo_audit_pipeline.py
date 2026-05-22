"""Promo audit pipeline for email-backed promo evidence.

Builds a structured inventory of source emails and attachments, extracts
high-confidence weekly Excel rows, compares them against a route catalog,
and isolates lower-confidence PDF/manual-review buckets.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import imaplib
import json
import os
import re
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import firebase_admin
import pandas as pd
import pdfplumber
from firebase_admin import credentials, firestore

try:
    from order_forecast.scripts.promo_parser import (
        _validate_sap_description_match,
        parse_promo_attachment,
    )
    from order_forecast.scripts.promo_audit_paths import default_audit_dir
except ImportError:
    from promo_parser import (  # type: ignore
        _validate_sap_description_match,
        parse_promo_attachment,
    )
    from promo_audit_paths import default_audit_dir  # type: ignore


COMMON_WORDS = {
    "mission",
    "guerrero",
    "calidad",
    "the",
    "and",
    "or",
    "ct",
    "8ct",
    "10ct",
    "20ct",
    "soft",
    "taco",
    "flour",
    "tortilla",
    "tortillas",
}
BRANDS = {"mission", "guerrero", "calidad"}
VARIANT_KEYWORDS = [
    "original",
    "orginal",
    "chipotle",
    "jalapeno",
    "jalapeño",
    "spinach",
    "tomato",
    "white",
    "yellow",
    "plain",
    "picante",
    "sundried",
    "whole wheat",
    "ww",
    "znc",
    "zero net",
    "carb balance",
    "protein",
    "chickpea",
    "gf",
]


@dataclass
class SourceEmail:
    date: str
    subject: str
    attachment_categories: List[Dict]
    has_xlsx: bool = False
    has_pdf: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promo audit pipeline")
    parser.add_argument("--route", required=True, help="Route number, e.g. 989262")
    parser.add_argument("--imap-user", default=os.environ.get("PROMO_IMAP_USER"))
    parser.add_argument("--imap-pass", default=os.environ.get("PROMO_IMAP_PASS"))
    parser.add_argument("--mailbox", default="INBOX")
    parser.add_argument("--since", default="2025-01-01", help="Inclusive date in YYYY-MM-DD")
    parser.add_argument(
        "--firebase-creds",
        default=str(Path(__file__).resolve().parents[2] / "routespark-firebase-adminsdk.json"),
    )
    parser.add_argument("--output-dir", default=None)
    parser.add_argument(
        "--supplemental-catalog-pdf",
        default=None,
        help="Optional broader product catalog PDF to merge into the audit catalog",
    )
    args = parser.parse_args()
    if not args.output_dir:
        args.output_dir = str(default_audit_dir(args.route))
    if not args.imap_user or not args.imap_pass:
        parser.error("IMAP credentials required via --imap-user/--imap-pass or PROMO_IMAP_USER/PROMO_IMAP_PASS")
    return args


def init_firestore(creds_path: str) -> firestore.Client:
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_path))
    return firestore.client()


def fetch_catalog(client: firestore.Client, route_number: str) -> tuple[Dict[str, str], List[Dict]]:
    catalog_ref = client.collection("masterCatalog").document(route_number).collection("products")
    catalog_map: Dict[str, str] = {}
    catalog_list: List[Dict] = []
    for doc in catalog_ref.stream():
        data = doc.to_dict() or {}
        sap = str(data.get("sap") or doc.id)
        full_name = (data.get("fullName") or data.get("full_name") or data.get("name") or "").lower()
        catalog_map[sap] = full_name
        catalog_list.append({"sap": sap, "fullName": full_name})
    return catalog_map, catalog_list


def _is_catalog_upc_line(line: str) -> bool:
    return bool(re.fullmatch(r"\d{5,6}-\d{4,6}(?:\s+.*)?", line.strip()))


def _is_catalog_header_line(line: str) -> bool:
    lowered = line.lower().strip()
    if not lowered:
        return True
    prefixes = (
        "catalog",
        "sap upc item description",
        "picture",
        "mission -",
        "guerrero -",
        "calidad -",
        "other brands",
        "private lab",
        "private label",
        "club items",
    )
    exact = {
        "corn tortillas",
        "flour tortillas",
        "tostadas",
        "chips",
        "chicharrones",
        "guerrero tortillas",
        "la providencia totopos",
        "generic chips & deli style totopo chips",
        "dips, salsas & generic chips",
        "sonora style - bakery items",
        "heb tortillas",
        "heb tostadas",
    }
    return lowered.startswith(prefixes) or lowered in exact


def _clean_catalog_pdf_name(name: str) -> str:
    cleaned = " ".join(name.split())
    cleaned = re.sub(r"\b(?:\d+\.\d+|\d+/\d+|\d+)\b", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:SAMS CLUB|COSTCO CLUB|RESTAURANT DEPOT|KROGER CLUB PACK|HEB ONLY|WIC ITEM|PALLET DROP)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(?:[A-Za-z]{1,4}\s+){0,3}(?=(Mission|Guerrero|Calidad|La Providencia|Generic|Great Value|H\.E\.B\.|Mi Tienda|Kroger|Signature|Rouses|Market Pantry|Good & Gather|Brookshire|BKY))",
        "",
        cleaned,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
    return cleaned


def parse_catalog_pdf(path: Path) -> tuple[Dict[str, str], List[Dict]]:
    supplemental_map: Dict[str, str] = {}
    supplemental_list: List[Dict] = []
    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            page_width, page_height = page.width, page.height
            for side, bbox in (("left", (0, 0, page_width / 2, page_height)), ("right", (page_width / 2, 0, page_width, page_height))):
                lines = [line.strip() for line in (page.crop(bbox).extract_text() or "").splitlines() if line.strip()]
                for index, line in enumerate(lines):
                    if _is_catalog_upc_line(line):
                        continue
                    match = re.match(r"^(\d{4,6})\b(.*)$", line)
                    if not match:
                        continue
                    sap = match.group(1)
                    rest = match.group(2).strip()
                    description_parts: List[str] = []

                    back_parts: List[str] = []
                    cursor = index - 1
                    while cursor >= 0 and len(back_parts) < 3:
                        previous = lines[cursor]
                        if _is_catalog_upc_line(previous) or re.match(r"^\d{4,6}\b", previous) or _is_catalog_header_line(previous):
                            break
                        back_parts.append(previous)
                        cursor -= 1
                    description_parts.extend(reversed(back_parts))

                    if rest:
                        same_line = re.sub(r"\s+\d+(?:\.\d+)?(?:\s+\S+){0,3}$", "", rest).strip()
                        if same_line:
                            description_parts.append(same_line)

                    forward_parts: List[str] = []
                    cursor = index + 1
                    while cursor < len(lines) and len(forward_parts) < 3:
                        following = lines[cursor]
                        if _is_catalog_upc_line(following) or re.match(r"^\d{4,6}\b", following) or _is_catalog_header_line(following):
                            break
                        forward_parts.append(following)
                        cursor += 1
                    description_parts.extend(forward_parts)

                    cleaned_name = _clean_catalog_pdf_name(" ".join(description_parts))
                    if len(cleaned_name) < 4:
                        continue
                    current = supplemental_map.get(sap, "")
                    if len(cleaned_name) > len(current):
                        supplemental_map[sap] = cleaned_name.lower()

    for sap, full_name in supplemental_map.items():
        supplemental_list.append({"sap": sap, "fullName": full_name, "catalog_source": "supplemental_pdf"})
    return supplemental_map, supplemental_list


def merge_catalogs(route_map: Dict[str, str], route_list: List[Dict], supplemental_map: Dict[str, str], supplemental_list: List[Dict]) -> tuple[Dict[str, str], List[Dict]]:
    merged_map = dict(route_map)
    merged_list = [dict(item, catalog_source=item.get("catalog_source", "route_firestore")) for item in route_list]
    index_by_sap = {item.get("sap"): idx for idx, item in enumerate(merged_list)}
    for sap, full_name in supplemental_map.items():
        merged_map[sap] = full_name
        if sap in index_by_sap:
            merged_list[index_by_sap[sap]]["fullName"] = full_name
            merged_list[index_by_sap[sap]]["catalog_source"] = "supplemental_pdf"
        if sap not in index_by_sap:
            merged_list.append({"sap": sap, "fullName": full_name, "catalog_source": "supplemental_pdf"})
            index_by_sap[sap] = len(merged_list) - 1
    return merged_map, merged_list


def classify_excel(path: Path) -> str:
    xl = pd.ExcelFile(path)
    lower_sheets = " | ".join(sheet.lower() for sheet in xl.sheet_names)
    first_cols: List[str] = []
    for sheet in xl.sheet_names[:3]:
        try:
            df = pd.read_excel(path, sheet_name=sheet, header=0)
            first_cols.extend(str(c).strip().lower() for c in df.columns.tolist()[:10])
        except Exception:
            continue

    if "price list" in lower_sheets or "all_upc" in lower_sheets or "tradenet_pricing" in lower_sheets:
        return "price_list_workbook"
    if any("one on one sheet" in col for col in first_cols):
        return "weekly_executable_workbook"
    if "chip calendar" in lower_sheets:
        return "quarterly_chip_calendar_workbook"
    if "tortilla calendar" in lower_sheets:
        return "quarterly_tortilla_calendar_workbook"
    return "other_excel"


def classify_pdf(path: Path) -> str:
    with pdfplumber.open(path) as pdf:
        text = "\n".join((pdf.pages[0].extract_text() or "").splitlines()[:12]).lower() if pdf.pages else ""
    if "one on one sheet" in text:
        return "weekly_executable_pdf"
    if "chip calendar" in text:
        return "quarterly_chip_calendar_pdf"
    if "tortilla calendar" in text:
        return "quarterly_tortilla_calendar_pdf"
    return "other_pdf"


def normalized_tokens(text: str) -> List[str]:
    text = re.sub(r"[^a-z0-9\s/&-]+", " ", text.lower())
    parts = re.split(r"[\s/&,-]+", text)
    return [p for p in parts if p and p not in COMMON_WORDS]


def morphology_root(token: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "", token.lower())
    while len(token) > 4:
        original = token
        if token.endswith("es"):
            token = token[:-2]
        elif token.endswith("s"):
            token = token[:-1]

        if len(token) > 4 and token.endswith(("a", "o")):
            token = token[:-1]

        if token == original:
            break
    return token


def edit_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            cur.append(min(cur[-1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = cur
    return prev[-1]


def classify_token_typos(description: str, catalog_name: str) -> tuple[List[str], List[str]]:
    desc_tokens = normalized_tokens(description)
    cat_tokens = normalized_tokens(catalog_name)
    cat_set = set(cat_tokens)
    spelling_typos: List[str] = []
    lexical_variants: List[str] = []
    for token in desc_tokens:
        if token in cat_set or len(token) < 4:
            continue
        matches = difflib.get_close_matches(token, cat_tokens, n=1, cutoff=0.84)
        if matches:
            match = matches[0]
            pair = f"{token}->{match}"
            if morphology_root(token) == morphology_root(match):
                lexical_variants.append(pair)
            elif edit_distance(token, match) <= 2:
                spelling_typos.append(pair)
            else:
                lexical_variants.append(pair)
    return sorted(set(spelling_typos)), sorted(set(lexical_variants))


def extract_brand(text: str) -> str:
    lowered = text.lower()
    for brand in ("mission", "guerrero", "calidad", "la providencia"):
        if brand in lowered:
            return brand
    return ""


def extract_pack_markers(text: str) -> List[str]:
    lowered = text.lower()
    markers = re.findall(r"\b(\d+(?:/\d+)?)\s*(ct|oz)\b", lowered)
    return [f"{count}{unit}" for count, unit in markers]


def score_description_against_name(description: str, full_name: str) -> float:
    desc_lower = description.lower()
    full_name = full_name.lower()

    desc_brand = "mission" if "mission" in desc_lower else ("guerrero" if "guerrero" in desc_lower else "")
    cat_brand = "mission" if "mission" in full_name else ("guerrero" if "guerrero" in full_name else "")
    if desc_brand and cat_brand and desc_brand != cat_brand:
        return -1.0

    desc_variants = [kw for kw in VARIANT_KEYWORDS if kw in desc_lower]
    score = difflib.SequenceMatcher(None, desc_lower, full_name).ratio()
    expanded_name = full_name.replace("znc", "zero net carb")
    score = max(score, difflib.SequenceMatcher(None, desc_lower, expanded_name).ratio())

    if desc_brand and cat_brand and desc_brand == cat_brand:
        score += 0.15

    cat_variants = [kw for kw in VARIANT_KEYWORDS if kw in full_name]
    common_variants = set(desc_variants) & set(cat_variants)
    if common_variants:
        score += 0.2 * len(common_variants)

    for kw in ["chipotle", "jalapeno", "jalapeño", "spinach", "original", "orginal", "white", "yellow"]:
        if kw in desc_lower and kw in full_name:
            score += 0.25
            break

    if ("original" in desc_lower or "orginal" in desc_lower) and "white" in full_name:
        score += 0.2

    return score


def best_catalog_match(description: str, catalog_list: List[Dict], current_sap: str) -> Dict:
    best_sap = None
    best_score = -1.0
    second_score = -1.0
    current_score = -1.0

    for product in catalog_list:
        sap = str(product.get("sap", ""))
        full_name = (product.get("fullName") or product.get("full_name") or product.get("name") or "").lower()
        if not full_name:
            continue
        score = score_description_against_name(description, full_name)
        if sap == current_sap:
            current_score = score
        if score > best_score:
            second_score = best_score
            best_score = score
            best_sap = sap
        elif score > second_score:
            second_score = score

    return {
        "best_sap": best_sap or "",
        "best_score": round(best_score, 4) if best_score >= 0 else 0.0,
        "current_score": round(current_score, 4) if current_score >= 0 else 0.0,
        "score_margin": round(best_score - current_score, 4) if current_score >= 0 and best_score >= 0 else 0.0,
        "runner_up_gap": round(best_score - second_score, 4) if second_score >= 0 and best_score >= 0 else 0.0,
    }


def listener_mismatch(item: Dict, catalog_map: Dict[str, str]) -> bool:
    sap = item.get("sap_raw", "") or item.get("sap_code", "")
    if not sap or sap not in catalog_map:
        return False
    desc = (item.get("description", "") or "").lower()
    catalog_name = catalog_map.get(sap, "")
    desc_words = set(desc.replace(",", " ").replace("&", " ").split())
    catalog_words = set(catalog_name.replace(",", " ").replace("&", " ").split())
    desc_key = desc_words - COMMON_WORDS
    catalog_key = catalog_words - COMMON_WORDS
    overlap = desc_key & catalog_key
    if not overlap and desc_key and catalog_key:
        desc_brand = desc_words & BRANDS
        catalog_brand = catalog_words & BRANDS
        if desc_brand != catalog_brand:
            return True
    return False


def compare_weekly_row(
    item: Dict,
    route_number: str,
    route_catalog_map: Dict[str, str],
    route_catalog_list: List[Dict],
    merged_catalog_map: Dict[str, str],
    merged_catalog_list: List[Dict],
) -> Dict:
    sap = str(item.get("sap_raw", "") or item.get("sap_code", "")).strip()
    description = (item.get("description", "") or "").strip()
    sap_count_in_cell = int(item.get("sap_count_in_cell") or 1)
    route_catalog_name = route_catalog_map.get(sap, "")
    merged_catalog_name = merged_catalog_map.get(sap, "")
    similarity = difflib.SequenceMatcher(None, description.lower(), route_catalog_name).ratio() if route_catalog_name else 0.0
    route_match_debug = best_catalog_match(description, route_catalog_list, sap)
    merged_match_debug = best_catalog_match(description, merged_catalog_list, sap)
    suggested_sap = route_match_debug["best_sap"]
    merged_suggested_sap = merged_match_debug["best_sap"]
    parser_match = _validate_sap_description_match(sap, description, route_catalog_list) if sap in route_catalog_map else False
    strict_mismatch = listener_mismatch(item, route_catalog_map)
    spelling_typos, lexical_variants = classify_token_typos(description, merged_catalog_name or route_catalog_name)
    description_brand = extract_brand(description)
    route_brand = extract_brand(route_catalog_name)
    merged_brand = extract_brand(merged_catalog_name)
    description_pack_markers = extract_pack_markers(description)
    merged_pack_markers = extract_pack_markers(merged_catalog_name)

    issue_flags: List[str] = []
    if not sap:
        issue_flags.append("missing_sap")
    elif sap not in route_catalog_map:
        if sap in merged_catalog_map:
            issue_flags.append("sap_not_in_route_catalog")
        else:
            issue_flags.append("sap_not_in_any_catalog")
    else:
        # Multi-SAP grouped rows are common in weekly executables; their shared
        # description often legitimately covers several sibling SAPs.
        if sap_count_in_cell == 1 and not parser_match and route_match_debug["current_score"] < 0.35:
            issue_flags.append("description_mismatch_for_sap")
        if sap_count_in_cell == 1 and strict_mismatch:
            issue_flags.append("listener_strict_mismatch")
        if (
            sap_count_in_cell == 1
            and suggested_sap
            and suggested_sap != sap
            and route_match_debug["best_score"] >= 0.9
            and route_match_debug["score_margin"] >= 0.15
            and route_match_debug["runner_up_gap"] >= 0.05
            and route_match_debug["current_score"] <= 0.78
        ):
            issue_flags.append("wrong_sap_for_description")
        if (
            sap_count_in_cell == 1
            and merged_suggested_sap
            and merged_suggested_sap != sap
            and merged_match_debug["best_score"] >= 0.9
            and merged_match_debug["score_margin"] >= 0.15
            and merged_match_debug["runner_up_gap"] >= 0.05
            and merged_match_debug["current_score"] <= 0.78
        ):
            issue_flags.append("wrong_sap_for_description_global")
        if description_brand and merged_brand and description_brand != merged_brand:
            issue_flags.append("brand_mismatch_for_sap")
        if description_pack_markers and merged_pack_markers and set(description_pack_markers) != set(merged_pack_markers):
            issue_flags.append("pack_size_mismatch_for_sap")
        if spelling_typos:
            issue_flags.append("possible_misspelling")
        if lexical_variants:
            issue_flags.append("lexical_variant")

    return {
        "route_number": route_number,
        "sap": sap,
        "sap_count_in_cell": sap_count_in_cell,
        "description": description,
        "account": item.get("account", ""),
        "start_date": item.get("start_date", ""),
        "end_date": item.get("end_date", ""),
        "price": item.get("price", ""),
        "catalog_name": route_catalog_name,
        "merged_catalog_name": merged_catalog_name,
        "suggested_sap": suggested_sap or "",
        "merged_suggested_sap": merged_suggested_sap or "",
        "similarity": round(similarity, 4),
        "best_match_score": route_match_debug["best_score"],
        "current_match_score": route_match_debug["current_score"],
        "match_score_margin": route_match_debug["score_margin"],
        "runner_up_gap": route_match_debug["runner_up_gap"],
        "merged_best_match_score": merged_match_debug["best_score"],
        "merged_current_match_score": merged_match_debug["current_score"],
        "merged_match_score_margin": merged_match_debug["score_margin"],
        "merged_runner_up_gap": merged_match_debug["runner_up_gap"],
        "parser_match": parser_match,
        "listener_mismatch": strict_mismatch,
        "description_brand": description_brand,
        "catalog_brand": route_brand,
        "merged_catalog_brand": merged_brand,
        "description_pack_markers": description_pack_markers,
        "merged_catalog_pack_markers": merged_pack_markers,
        "spelling_typos": spelling_typos,
        "token_typos": lexical_variants,
        "issue_flags": issue_flags,
    }


def iter_source_emails(client: imaplib.IMAP4_SSL, mailbox: str, since_dt: datetime) -> Iterable[tuple[object, str, bytes]]:
    client.select(mailbox, readonly=True)
    status, data = client.search(None, "ALL")
    if status != "OK":
        return
    for msg_id in [x for x in data[0].split() if x]:
        status, fetched = client.fetch(msg_id, "(RFC822)")
        if status != "OK" or not fetched or not fetched[0]:
            continue
        raw = fetched[0][1]
        if not raw:
            continue
        outer = BytesParser(policy=policy.default).parsebytes(raw)
        for part in outer.walk():
            ctype = part.get_content_type()
            fname = (part.get_filename() or "").lower()
            disp = part.get_content_disposition()
            if ctype == "message/rfc822" and disp == "attachment" and fname.endswith(".eml"):
                payload = part.get_payload()
                inner = payload[0] if isinstance(payload, list) and payload else None
                if inner is None:
                    continue
                dt = parsedate_to_datetime(inner.get("Date")) if inner.get("Date") else None
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if not dt or dt.astimezone(timezone.utc) < since_dt:
                    continue
                yield inner, dt.date().isoformat(), msg_id


def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = row.copy()
            for key, value in normalized.items():
                if isinstance(value, list):
                    normalized[key] = json.dumps(value, ensure_ascii=True)
            writer.writerow(normalized)


def dedupe_issue_rows(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[tuple, Dict] = {}
    for row in rows:
        key = (
            row.get("source_date", ""),
            row.get("source_subject", ""),
            row.get("attachment_name", ""),
            row.get("attachment_category", ""),
            row.get("route_number", ""),
            row.get("sap", ""),
            row.get("sap_count_in_cell", 0),
            row.get("description", ""),
            row.get("start_date", ""),
            row.get("end_date", ""),
            row.get("price", ""),
            row.get("catalog_name", ""),
            row.get("suggested_sap", ""),
            tuple(row.get("spelling_typos", [])),
            tuple(row.get("token_typos", [])),
            tuple(row.get("issue_flags", [])),
        )
        if key not in grouped:
            deduped = row.copy()
            deduped["accounts"] = []
            deduped["account_occurrences"] = 0
            grouped[key] = deduped

        current = grouped[key]
        account = (row.get("account", "") or "").strip()
        if account:
            current["accounts"].append(account)
        current["account_occurrences"] += 1

    deduped_rows: List[Dict] = []
    for row in grouped.values():
        unique_accounts = sorted(set(row.get("accounts", [])))
        row["accounts"] = unique_accounts
        row["unique_account_count"] = len(unique_accounts)
        deduped_rows.append(row)

    deduped_rows.sort(
        key=lambda row: (
            row.get("source_date", ""),
            row.get("attachment_name", ""),
            row.get("sap", ""),
            row.get("description", ""),
        )
    )
    return deduped_rows


def summarize_issue_patterns(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[tuple, Dict] = {}
    for row in rows:
        issue_flags = row.get("issue_flags", [])
        primary_issue = issue_flags[0] if issue_flags else ""
        key = (
            primary_issue,
            row.get("sap", ""),
            row.get("sap_count_in_cell", 0),
            row.get("description", ""),
            row.get("catalog_name", ""),
            row.get("suggested_sap", ""),
            tuple(row.get("spelling_typos", [])),
            tuple(row.get("token_typos", [])),
        )
        if key not in grouped:
            grouped[key] = {
                "primary_issue": primary_issue,
                "issue_flags": issue_flags,
                "sap": row.get("sap", ""),
                "sap_count_in_cell": row.get("sap_count_in_cell", 0),
                "description": row.get("description", ""),
                "catalog_name": row.get("catalog_name", ""),
                "suggested_sap": row.get("suggested_sap", ""),
                "spelling_typos": row.get("spelling_typos", []),
                "token_typos": row.get("token_typos", []),
                "occurrence_count": 0,
                "account_occurrences": 0,
                "source_dates": set(),
                "attachments": set(),
            }

        current = grouped[key]
        current["occurrence_count"] += 1
        current["account_occurrences"] += int(row.get("account_occurrences", 0))
        if row.get("source_date"):
            current["source_dates"].add(row["source_date"])
        if row.get("attachment_name"):
            current["attachments"].add(row["attachment_name"])

    pattern_rows: List[Dict] = []
    for row in grouped.values():
        source_dates = sorted(row.pop("source_dates"))
        attachments = sorted(row.pop("attachments"))
        row["distinct_source_dates"] = len(source_dates)
        row["distinct_attachments"] = len(attachments)
        row["first_seen"] = source_dates[0] if source_dates else ""
        row["last_seen"] = source_dates[-1] if source_dates else ""
        row["source_dates"] = source_dates
        row["attachments"] = attachments
        pattern_rows.append(row)

    pattern_rows.sort(
        key=lambda row: (
            row.get("primary_issue", ""),
            -int(row.get("distinct_source_dates", 0)),
            -int(row.get("occurrence_count", 0)),
            row.get("description", ""),
            row.get("sap", ""),
        )
    )
    return pattern_rows


def main() -> None:
    args = parse_args()
    since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    firestore_client = init_firestore(args.firebase_creds)
    route_catalog_map, route_catalog_list = fetch_catalog(firestore_client, args.route)
    supplemental_catalog_map: Dict[str, str] = {}
    supplemental_catalog_list: List[Dict] = []
    if args.supplemental_catalog_pdf:
        supplemental_catalog_map, supplemental_catalog_list = parse_catalog_pdf(Path(args.supplemental_catalog_pdf))
    merged_catalog_map, merged_catalog_list = merge_catalogs(
        route_catalog_map,
        route_catalog_list,
        supplemental_catalog_map,
        supplemental_catalog_list,
    )

    imap_client = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    imap_client.login(args.imap_user, args.imap_pass)

    inventory: List[Dict] = []
    weekly_excel_rows: List[Dict] = []
    issue_candidates: List[Dict] = []
    pdf_only_sources: List[Dict] = []
    manual_review_sources: List[Dict] = []
    attachment_counts = Counter()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        for inner, source_date, _msg_id in iter_source_emails(imap_client, args.mailbox, since_dt):
            source = SourceEmail(date=source_date, subject=(inner.get("Subject") or "").strip(), attachment_categories=[])
            for part in inner.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                fname = part.get_filename() or ""
                low = fname.lower()
                disp = part.get_content_disposition()
                if disp != "attachment" or not (low.endswith(".xlsx") or low.endswith(".xls") or low.endswith(".pdf")):
                    continue

                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                target = tmpdir_path / fname.replace("/", "_")
                target.write_bytes(payload)

                if low.endswith(".xlsx") or low.endswith(".xls"):
                    category = classify_excel(target)
                    source.has_xlsx = True
                else:
                    category = classify_pdf(target)
                    source.has_pdf = True

                attachment_counts[category] += 1
                source.attachment_categories.append({"file": fname, "category": category})

                if category == "weekly_executable_workbook":
                    parsed_items = parse_promo_attachment(target)
                    for parsed_item in parsed_items:
                        row = {
                            "source_date": source.date,
                            "source_subject": source.subject,
                            "attachment_name": fname,
                            "attachment_category": category,
                        }
                        row.update(
                            compare_weekly_row(
                                parsed_item,
                                args.route,
                                route_catalog_map,
                                route_catalog_list,
                                merged_catalog_map,
                                merged_catalog_list,
                            )
                        )
                        weekly_excel_rows.append(row)
                        if row["issue_flags"]:
                            issue_candidates.append(row)

            inventory.append(
                {
                    "date": source.date,
                    "subject": source.subject,
                    "has_xlsx": source.has_xlsx,
                    "has_pdf": source.has_pdf,
                    "attachments": source.attachment_categories,
                }
            )
            if not source.has_xlsx and source.has_pdf:
                pdf_only_sources.append({"date": source.date, "subject": source.subject, "attachments": source.attachment_categories})
            if not source.has_xlsx and not source.has_pdf:
                manual_review_sources.append({"date": source.date, "subject": source.subject})

    imap_client.logout()

    deduped_issue_candidates = dedupe_issue_rows(issue_candidates)
    issue_patterns = summarize_issue_patterns(deduped_issue_candidates)

    summary = {
        "route": args.route,
        "since": args.since,
        "route_catalog_count": len(route_catalog_map),
        "supplemental_catalog_count": len(supplemental_catalog_map),
        "merged_catalog_count": len(merged_catalog_map),
        "source_email_count": len(inventory),
        "attachment_counts": dict(attachment_counts),
        "weekly_excel_row_count": len(weekly_excel_rows),
        "issue_candidate_count": len(issue_candidates),
        "deduped_issue_candidate_count": len(deduped_issue_candidates),
        "issue_pattern_count": len(issue_patterns),
        "pdf_only_source_count": len(pdf_only_sources),
        "manual_review_count": len(manual_review_sources),
        "issue_breakdown": dict(Counter(flag for row in issue_candidates for flag in row["issue_flags"])),
        "deduped_issue_breakdown": dict(
            Counter(flag for row in deduped_issue_candidates for flag in row["issue_flags"])
        ),
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (output_dir / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    (output_dir / "pdf_only_sources.json").write_text(json.dumps(pdf_only_sources, indent=2), encoding="utf-8")
    (output_dir / "manual_review_sources.json").write_text(json.dumps(manual_review_sources, indent=2), encoding="utf-8")

    row_fields = [
        "source_date",
        "source_subject",
        "attachment_name",
        "attachment_category",
        "route_number",
        "sap",
        "sap_count_in_cell",
        "description",
        "account",
        "start_date",
        "end_date",
        "price",
        "catalog_name",
        "merged_catalog_name",
        "suggested_sap",
        "merged_suggested_sap",
        "similarity",
        "best_match_score",
        "current_match_score",
        "match_score_margin",
        "runner_up_gap",
        "merged_best_match_score",
        "merged_current_match_score",
        "merged_match_score_margin",
        "merged_runner_up_gap",
        "parser_match",
        "listener_mismatch",
        "description_brand",
        "catalog_brand",
        "merged_catalog_brand",
        "description_pack_markers",
        "merged_catalog_pack_markers",
        "spelling_typos",
        "token_typos",
        "issue_flags",
    ]
    write_csv(output_dir / "weekly_excel_rows.csv", weekly_excel_rows, row_fields)
    write_csv(output_dir / "issue_candidates.csv", issue_candidates, row_fields)
    write_csv(
        output_dir / "issue_candidates_deduped.csv",
        deduped_issue_candidates,
        row_fields + ["account_occurrences", "unique_account_count", "accounts"],
    )
    write_csv(
        output_dir / "issue_patterns.csv",
        issue_patterns,
        [
            "primary_issue",
            "issue_flags",
            "sap",
            "sap_count_in_cell",
            "description",
            "catalog_name",
            "suggested_sap",
            "spelling_typos",
            "token_typos",
            "occurrence_count",
            "account_occurrences",
            "distinct_source_dates",
            "distinct_attachments",
            "first_seen",
            "last_seen",
            "source_dates",
            "attachments",
        ],
    )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

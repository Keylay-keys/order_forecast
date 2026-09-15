#!/usr/bin/env python3
"""Test-lab-only Gruma SBT settlement extraction and allocation helpers.

This module is deliberately separate from the production forecast engine.  It
turns the stable weekly-movement and weekly-inventory tables in route 989262's
current settlement PDFs into reviewable facts and provides one conservative
store-allocation experiment for H4 predictions.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import hashlib
import re
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


STORE_NAMES = {
    "0135": "Farmington Smith's",
    "0277": "Kaysville Smith's",
}

_DATE_RE = re.compile(r"Settlement Week Ending\s+(\d{1,2}/\d{1,2}/\d{4})")
_STORE_RE = re.compile(r"#(\d{4})\b")
_NUMBER_RE = re.compile(r"^-?\d+$")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _nearest_slot(value: float, expected: Sequence[float], tolerance: float = 5.0) -> Optional[int]:
    if not expected:
        return None
    index = int(np.argmin([abs(value - candidate) for candidate in expected]))
    if abs(value - expected[index]) > tolerance:
        return None
    return index


def _words_on_row(words: Sequence[Mapping[str, object]], top: float) -> List[Mapping[str, object]]:
    return [word for word in words if abs(float(word["top"]) - top) <= 0.8]


def _sku_rows(words: Sequence[Mapping[str, object]]) -> Iterable[Tuple[str, float]]:
    seen: set[Tuple[str, int]] = set()
    for word in words:
        text = str(word.get("text") or "")
        x0 = float(word.get("x0") or 0.0)
        top = float(word.get("top") or 0.0)
        # Continuation pages begin product rows near the top margin, whereas
        # first pages begin below the repeated report header.
        if not (25.0 <= top <= 755.0 and x0 < 42.0 and text.isdigit() and len(text) <= 5):
            continue
        key = (text, round(top))
        if key in seen:
            continue
        seen.add(key)
        yield text, top


def extract_daily_movement_rows(pdf_path: Path) -> Tuple[pd.DataFrame, date]:
    """Extract pages 5-9 into daily store/SAP movement facts.

    The current Gruma layout uses 28 fixed right-aligned numeric positions:
    HHC In, HHC Out, Sale, and Returns for each Thursday-Wednesday day.  Missing
    cells inside a present product row are explicit zeroes; missing pages never
    reach this function because the 12-page contract is required.
    """
    import pdfplumber

    with pdfplumber.open(str(pdf_path)) as pdf:
        if len(pdf.pages) != 12:
            raise ValueError(f"expected 12 settlement pages, found {len(pdf.pages)}: {pdf_path}")
        first_text = pdf.pages[0].extract_text() or ""
        match = _DATE_RE.search(first_text)
        if not match:
            raise ValueError(f"settlement week ending not found: {pdf_path}")
        week_end = datetime.strptime(match.group(1), "%m/%d/%Y").date()
        week_start = week_end - timedelta(days=6)
        source_hash = _sha256(pdf_path)
        records: List[Dict[str, object]] = []
        current_store: Optional[str] = None
        # Right edges of the 28 daily numeric cells in the landscape report.
        expected_edges = [198.4 + (18.7 * index) for index in range(28)]

        for page_number in range(5, 10):
            page = pdf.pages[page_number - 1]
            text = page.extract_text() or ""
            store_match = _STORE_RE.search(text)
            if store_match:
                current_store = store_match.group(1)
            if current_store not in STORE_NAMES:
                raise ValueError(
                    f"unresolved SBT store on page {page_number} of {pdf_path.name}"
                )
            words = page.extract_words()
            for sap, top in _sku_rows(words):
                slots = [0] * 28
                for word in _words_on_row(words, top):
                    value_text = str(word.get("text") or "")
                    if not _NUMBER_RE.match(value_text):
                        continue
                    x0 = float(word.get("x0") or 0.0)
                    if x0 < 180.0:
                        continue
                    slot = _nearest_slot(float(word["x1"]), expected_edges)
                    if slot is not None:
                        slots[slot] = int(value_text)
                for day_offset in range(7):
                    base = day_offset * 4
                    records.append(
                        {
                            "route_number": "989262",
                            "settlement_week_ending": week_end.isoformat(),
                            "simulated_available_at": datetime.combine(
                                week_end + timedelta(days=5),
                                time(12, 0),
                                tzinfo=timezone.utc,
                            ).isoformat(),
                            "availability_basis": "workflow_simulation_monday_noon_not_observed_upload",
                            "store_number": current_store,
                            "store_name": STORE_NAMES[current_store],
                            "sap": sap,
                            "date": (week_start + timedelta(days=day_offset)).isoformat(),
                            "hhc_in_units": slots[base],
                            "hhc_out_units": slots[base + 1],
                            "sale_units": slots[base + 2],
                            "store_return_units": slots[base + 3],
                            "source_document": pdf_path.name,
                            "source_sha256": source_hash,
                            "review_status": "machine_extracted_reconciled_pending_spot_review",
                        }
                    )
        return pd.DataFrame.from_records(records), week_end


def extract_weekly_inventory_rows(pdf_path: Path) -> pd.DataFrame:
    """Extract the route-level SBT weekly inventory control totals."""
    import pdfplumber

    with pdfplumber.open(str(pdf_path)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        match = _DATE_RE.search(first_text)
        if not match:
            raise ValueError(f"settlement week ending not found: {pdf_path}")
        week_end = datetime.strptime(match.group(1), "%m/%d/%Y").date()
        records: List[Dict[str, object]] = []
        field_centers = {
            "hhc_in_units": 475.0,
            "hhc_out_units": 520.0,
            "sale_units": 570.0,
            "store_return_units": 628.0,
        }
        for page_number in range(10, 13):
            words = pdf.pages[page_number - 1].extract_words()
            for sap, top in _sku_rows(words):
                row_words = _words_on_row(words, top)
                record: Dict[str, object] = {
                    "settlement_week_ending": week_end.isoformat(),
                    "sap": sap,
                }
                for field, center in field_centers.items():
                    candidates = [
                        word
                        for word in row_words
                        if _NUMBER_RE.match(str(word.get("text") or ""))
                        and abs((float(word["x0"]) + float(word["x1"])) / 2.0 - center) <= 18.0
                    ]
                    record[field] = int(candidates[0]["text"]) if candidates else 0
                records.append(record)
        return pd.DataFrame.from_records(records)


def reconcile_daily_to_weekly(daily: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    fields = ["hhc_in_units", "hhc_out_units", "sale_units", "store_return_units"]
    totals = daily.groupby(["settlement_week_ending", "sap"], as_index=False)[fields].sum()
    merged = totals.merge(
        weekly,
        on=["settlement_week_ending", "sap"],
        how="outer",
        suffixes=("_daily", "_weekly"),
        indicator=True,
    )
    for field in fields:
        merged[f"{field}_matches"] = (
            merged[f"{field}_daily"].fillna(0).astype(int)
            == merged[f"{field}_weekly"].fillna(0).astype(int)
        )
    merged["all_fields_match"] = merged[[f"{field}_matches" for field in fields]].all(axis=1)
    return merged


def _store_number_from_prediction(store_name: object, store_id: object) -> Optional[str]:
    haystack = f"{store_name or ''} {store_id or ''}".lower()
    if "0135" in haystack or "farmington" in haystack:
        return "0135"
    if "0277" in haystack or "kaysville" in haystack:
        return "0277"
    return None


def apply_settlement_store_share(
    predictions: pd.DataFrame,
    settlement_daily: pd.DataFrame,
    forecast_as_of: datetime,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Blend past settlement sell-through into H4's two-Smith's allocation.

    The experiment preserves H4's combined predicted units for the two SBT
    stores and changes only their split.  With fewer than two prior statement
    weeks for a SAP it makes no change.  At most half of H4's allocation weight
    is replaced, limiting the influence of this deliberately small dataset.
    """
    stats = {
        "eligible_saps": 0,
        "reallocated_saps": 0,
        "units_moved_between_stores": 0,
        "settlement_weeks_used": 0,
    }
    if predictions.empty or settlement_daily.empty:
        return predictions, stats

    result = predictions.copy()
    result["settlement_store_number"] = [
        _store_number_from_prediction(row.get("store"), row.get("store_id"))
        for _, row in result.iterrows()
    ]
    available = settlement_daily.copy()
    available["simulated_available_at"] = pd.to_datetime(
        available["simulated_available_at"], utc=True, errors="coerce"
    )
    cutoff = pd.Timestamp(forecast_as_of)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    available = available[available["simulated_available_at"] < cutoff]
    if available.empty:
        return result, stats
    latest_weeks = sorted(available["settlement_week_ending"].unique())[-4:]
    available = available[available["settlement_week_ending"].isin(latest_weeks)]
    stats["settlement_weeks_used"] = len(latest_weeks)

    sales = available.groupby(["sap", "store_number"], as_index=False)["sale_units"].sum()
    week_counts = available.groupby("sap")["settlement_week_ending"].nunique().to_dict()

    for sap, indexes in result.groupby("sap").groups.items():
        sbt_indexes = [
            index
            for index in indexes
            if result.at[index, "settlement_store_number"] in STORE_NAMES
        ]
        if len(sbt_indexes) != 2:
            continue
        if {result.at[index, "settlement_store_number"] for index in sbt_indexes} != set(STORE_NAMES):
            continue
        weeks = int(week_counts.get(str(sap), 0) or 0)
        sap_sales = sales[sales["sap"].astype(str) == str(sap)]
        total_sales = float(sap_sales["sale_units"].sum())
        combined_prediction = int(round(float(result.loc[sbt_indexes, "pred_units"].sum())))
        if weeks < 2 or total_sales <= 0 or combined_prediction <= 0:
            continue
        stats["eligible_saps"] += 1
        original = {
            result.at[index, "settlement_store_number"]: float(result.at[index, "pred_units"])
            for index in sbt_indexes
        }
        original_share = {
            store: units / combined_prediction for store, units in original.items()
        }
        sold = {
            store: float(
                sap_sales.loc[sap_sales["store_number"] == store, "sale_units"].sum()
            )
            for store in STORE_NAMES
        }
        observed_share = {store: sold[store] / total_sales for store in STORE_NAMES}
        blend_weight = min(0.50, weeks / 6.0)
        target_share = {
            store: (1.0 - blend_weight) * original_share[store]
            + blend_weight * observed_share[store]
            for store in STORE_NAMES
        }
        exact = {store: combined_prediction * target_share[store] for store in STORE_NAMES}
        allocated = {store: int(np.floor(value)) for store, value in exact.items()}
        remainder = combined_prediction - sum(allocated.values())
        for store in sorted(STORE_NAMES, key=lambda key: exact[key] - allocated[key], reverse=True)[:remainder]:
            allocated[store] += 1

        moved = int(round(sum(abs(allocated[store] - original[store]) for store in STORE_NAMES) / 2.0))
        if moved <= 0:
            continue
        for index in sbt_indexes:
            store = result.at[index, "settlement_store_number"]
            prior = max(0.0, float(result.at[index, "pred_units"]))
            next_units = float(allocated[store])
            scale = next_units / prior if prior > 0 else 0.0
            result.at[index, "pred_units"] = next_units
            result.at[index, "p10_units"] = max(0.0, float(result.at[index, "p10_units"]) * scale)
            result.at[index, "p50_units"] = next_units
            result.at[index, "p90_units"] = max(next_units, float(result.at[index, "p90_units"]) * scale)
            result.at[index, "model_branch"] = "settlement_store_share_v1"
        stats["reallocated_saps"] += 1
        stats["units_moved_between_stores"] += moved

    return result.drop(columns=["settlement_store_number"]), stats

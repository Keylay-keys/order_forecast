#!/usr/bin/env python3
"""Extract and reconcile route 989262 settlement SBT facts for the test lab."""

from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta, timezone
import hashlib
from pathlib import Path

import pandas as pd

try:
    from .experimental_settlement_truth import (
        extract_daily_movement_rows,
        extract_weekly_inventory_rows,
        reconcile_daily_to_weekly,
    )
except ImportError:
    from experimental_settlement_truth import (
        extract_daily_movement_rows,
        extract_weekly_inventory_rows,
        reconcile_daily_to_weekly,
    )


# Official Mission/Gruma training material stored alongside the private lab
# statements. It explains how to read a statement but is not itself a weekly
# statement. Keep this content-addressed so an unrelated PDF is never silently
# skipped just because of its filename.
REFERENCE_PDF_SHA256 = {
    "9de276686ab427fc9e547684289390328d741c7bbb26b8a10b60b2fcbce939d0":
        "Mission/Gruma Route Settlements training guide (2024)",
}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _partition_statement_pdfs(paths: list[Path]) -> tuple[list[Path], list[tuple[Path, str]]]:
    statements: list[Path] = []
    references: list[tuple[Path, str]] = []
    for path in paths:
        label = REFERENCE_PDF_SHA256.get(_file_sha256(path))
        if label:
            references.append((path, label))
        else:
            statements.append(path)
    return statements, references


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputDir", required=True)
    parser.add_argument("--outputCsv", required=True)
    parser.add_argument("--reconciliationCsv", required=True)
    parser.add_argument(
        "--manualDailyCsv",
        help="Optional agent-reviewed daily rows for a screenshot-only statement",
    )
    args = parser.parse_args()

    input_dir = Path(args.inputDir)
    discovered_pdfs = sorted(
        path for path in input_dir.glob("*.pdf") if not path.name.startswith("._")
    )
    if not discovered_pdfs:
        raise SystemExit("no settlement PDFs found")
    pdfs, reference_pdfs = _partition_statement_pdfs(discovered_pdfs)
    for reference_path, label in reference_pdfs:
        print(f"excluded reference PDF: {reference_path.name} ({label})")
    if not pdfs:
        raise SystemExit("no weekly settlement PDFs found after excluding reference material")

    daily_frames = []
    reconciliation_frames = []
    for pdf_path in pdfs:
        daily, _ = extract_daily_movement_rows(pdf_path)
        weekly = extract_weekly_inventory_rows(pdf_path)
        reconciliation = reconcile_daily_to_weekly(daily, weekly)
        daily_frames.append(daily)
        reconciliation_frames.append(reconciliation)

    if args.manualDailyCsv:
        manual_path = Path(args.manualDailyCsv)
        manual = pd.read_csv(manual_path, dtype={"route_number": str, "store_number": str, "sap": str})
        manual["store_number"] = manual["store_number"].str.zfill(4)
        manual_week_end = pd.to_datetime(manual["settlement_week_ending"]).dt.date
        manual["simulated_available_at"] = [
            datetime.combine(value + timedelta(days=5), time(12, 0), tzinfo=timezone.utc).isoformat()
            for value in manual_week_end
        ]
        manual["availability_basis"] = "workflow_simulation_monday_noon_not_observed_upload"
        manual["source_document"] = manual_path.name
        manual["source_sha256"] = "see_review_manifest"
        manual["review_status"] = manual.get("validation", "agent_reviewed")
        daily_frames.append(manual)

    all_daily = pd.concat(daily_frames, ignore_index=True).sort_values(
        ["settlement_week_ending", "store_number", "sap", "date"]
    )
    all_reconciliation = pd.concat(reconciliation_frames, ignore_index=True).sort_values(
        ["settlement_week_ending", "sap"]
    )
    output_path = Path(args.outputCsv)
    reconciliation_path = Path(args.reconciliationCsv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    reconciliation_path.parent.mkdir(parents=True, exist_ok=True)
    all_daily.to_csv(output_path, index=False)
    all_reconciliation.to_csv(reconciliation_path, index=False)

    matched = int(all_reconciliation["all_fields_match"].sum())
    total = len(all_reconciliation)
    print(f"wrote {len(all_daily)} daily rows from {len(pdfs)} PDFs to {output_path}")
    print(f"weekly reconciliation: {matched}/{total} SAP-weeks matched all four fields")
    print(f"wrote reconciliation details to {reconciliation_path}")


if __name__ == "__main__":
    main()

"""Build a broader promo evidence pack from extracted weekly promo rows.

Uses the already-extracted weekly Excel rows, merges in an optional broader
catalog PDF, and emits exhaustive issue rows, deduped findings, recurring
patterns, and timing summaries without re-reading IMAP mail.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, List

from promo_audit_pipeline import (  # type: ignore
    compare_weekly_row,
    dedupe_issue_rows,
    fetch_catalog,
    init_firestore,
    merge_catalogs,
    parse_catalog_pdf,
    summarize_issue_patterns,
    write_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a broader promo evidence pack from extracted rows")
    parser.add_argument("--route", required=True)
    parser.add_argument("--weekly-rows-csv", required=True)
    parser.add_argument("--firebase-creds", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--supplemental-catalog-pdf", default=None)
    parser.add_argument("--timing-summary-json", default=None)
    parser.add_argument("--likely-late-incidents-json", default=None)
    return parser.parse_args()


def parse_date(value: str) -> date | None:
    if not value:
        return None
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def classify_timing_bucket(row: Dict[str, str]) -> tuple[str | None, Dict]:
    source_date = parse_date(row.get("source_date", ""))
    start_date = parse_date(row.get("start_date", ""))
    end_date = parse_date(row.get("end_date", ""))
    if not (source_date and start_date and start_date < source_date):
        return None, {}

    days_before_send = (source_date - start_date).days
    duration_days = (end_date - start_date).days if end_date else None
    invalid_reasons: List[str] = []
    if start_date.year < source_date.year - 1:
        invalid_reasons.append("start_year_far_past")
    if end_date and end_date < start_date:
        invalid_reasons.append("end_before_start")
    if duration_days is not None and duration_days > 180:
        invalid_reasons.append("duration_over_180d")

    if invalid_reasons:
        return "invalid_bad_date", {
            "days_before_send": days_before_send,
            "duration_days": duration_days,
            "invalid_reasons": invalid_reasons,
        }
    if end_date and end_date < source_date:
        return "expired_before_send", {
            "days_before_send": days_before_send,
            "duration_days": duration_days,
            "invalid_reasons": [],
        }
    if days_before_send <= 7:
        return "likely_late_send", {
            "days_before_send": days_before_send,
            "duration_days": duration_days,
            "invalid_reasons": [],
        }
    return "carry_forward_active", {
        "days_before_send": days_before_send,
        "duration_days": duration_days,
        "invalid_reasons": [],
    }


def build_master_patterns(issue_patterns: List[Dict], timing_rows: List[Dict]) -> List[Dict]:
    timing_patterns: Dict[tuple, Dict] = {}
    for row in timing_rows:
        key = (
            row["timing_bucket"],
            row.get("sap", ""),
            row.get("sap_count_in_cell", 0),
            row.get("description", ""),
            tuple(row.get("invalid_reasons", [])),
        )
        current = timing_patterns.setdefault(
            key,
            {
                "primary_issue": row["timing_bucket"],
                "issue_flags": [row["timing_bucket"]],
                "sap": row.get("sap", ""),
                "sap_count_in_cell": row.get("sap_count_in_cell", 0),
                "description": row.get("description", ""),
                "catalog_name": row.get("merged_catalog_name", "") or row.get("catalog_name", ""),
                "suggested_sap": row.get("merged_suggested_sap", "") or row.get("suggested_sap", ""),
                "spelling_typos": [],
                "token_typos": [],
                "occurrence_count": 0,
                "account_occurrences": 0,
                "distinct_source_dates": 0,
                "distinct_attachments": 0,
                "first_seen": "",
                "last_seen": "",
                "source_dates": set(),
                "attachments": set(),
                "invalid_reasons": row.get("invalid_reasons", []),
            },
        )
        current["occurrence_count"] += 1
        current["account_occurrences"] += 1
        if row.get("source_date"):
            current["source_dates"].add(row["source_date"])
        if row.get("attachment_name"):
            current["attachments"].add(row["attachment_name"])

    normalized_timing_patterns: List[Dict] = []
    for row in timing_patterns.values():
        source_dates = sorted(row.pop("source_dates"))
        attachments = sorted(row.pop("attachments"))
        row["distinct_source_dates"] = len(source_dates)
        row["distinct_attachments"] = len(attachments)
        row["first_seen"] = source_dates[0] if source_dates else ""
        row["last_seen"] = source_dates[-1] if source_dates else ""
        row["source_dates"] = source_dates
        row["attachments"] = attachments
        normalized_timing_patterns.append(row)

    all_patterns = [dict(row) for row in issue_patterns] + normalized_timing_patterns
    all_patterns.sort(
        key=lambda row: (
            row.get("primary_issue", ""),
            -int(row.get("distinct_source_dates", 0)),
            -int(row.get("occurrence_count", 0)),
            row.get("description", ""),
        )
    )
    return all_patterns


def main() -> None:
    args = parse_args()
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

    raw_rows = list(csv.DictReader(open(args.weekly_rows_csv)))
    recomputed_rows: List[Dict] = []
    issue_rows: List[Dict] = []
    timing_rows: List[Dict] = []
    compare_cache: Dict[tuple, Dict] = {}

    for raw in raw_rows:
        item = {
            "sap_raw": raw.get("sap", ""),
            "sap_code": raw.get("sap", ""),
            "sap_count_in_cell": raw.get("sap_count_in_cell", "1"),
            "description": raw.get("description", ""),
            "price": raw.get("price", ""),
            "account": raw.get("account", ""),
            "start_date": raw.get("start_date", ""),
            "end_date": raw.get("end_date", ""),
        }
        base = {
            "source_date": raw.get("source_date", ""),
            "source_subject": raw.get("source_subject", ""),
            "attachment_name": raw.get("attachment_name", ""),
            "attachment_category": raw.get("attachment_category", ""),
        }
        cache_key = (
            item["sap_raw"],
            item["sap_count_in_cell"],
            item["description"],
        )
        if cache_key not in compare_cache:
            compare_cache[cache_key] = compare_weekly_row(
                item,
                args.route,
                route_catalog_map,
                route_catalog_list,
                merged_catalog_map,
                merged_catalog_list,
            )
        recomputed = dict(compare_cache[cache_key])
        combined = dict(base)
        combined.update(recomputed)
        recomputed_rows.append(combined)
        if combined["issue_flags"]:
            issue_rows.append(combined)

        timing_bucket, timing_meta = classify_timing_bucket(combined)
        if timing_bucket:
            timing_row = dict(combined)
            timing_row["timing_bucket"] = timing_bucket
            timing_row["days_before_send"] = timing_meta["days_before_send"]
            timing_row["duration_days"] = timing_meta["duration_days"] if timing_meta["duration_days"] is not None else ""
            timing_row["invalid_reasons"] = timing_meta["invalid_reasons"]
            timing_rows.append(timing_row)

    deduped_issue_rows = dedupe_issue_rows(issue_rows)
    issue_patterns = summarize_issue_patterns(deduped_issue_rows)
    master_patterns = build_master_patterns(issue_patterns, timing_rows)

    timing_bucket_counts = Counter(row["timing_bucket"] for row in timing_rows)
    timing_bucket_dedup_counts = Counter(
        row["timing_bucket"]
        for row in {
            (
                row["source_date"],
                row["source_subject"],
                row["attachment_name"],
                row["sap"],
                row["description"],
                row["start_date"],
                row["end_date"],
                row["timing_bucket"],
            ): row
            for row in timing_rows
        }.values()
    )

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

    write_csv(output_dir / "full_issue_rows.csv", issue_rows, row_fields)
    write_csv(output_dir / "full_issue_rows_deduped.csv", deduped_issue_rows, row_fields + ["account_occurrences", "unique_account_count", "accounts"])
    write_csv(
        output_dir / "full_timing_rows.csv",
        timing_rows,
        row_fields + ["timing_bucket", "days_before_send", "duration_days", "invalid_reasons"],
    )
    write_csv(
        output_dir / "full_issue_patterns.csv",
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
    write_csv(
        output_dir / "master_dirt_patterns.csv",
        master_patterns,
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
            "invalid_reasons",
        ],
    )

    summary = {
        "route": args.route,
        "route_catalog_count": len(route_catalog_map),
        "supplemental_catalog_count": len(supplemental_catalog_map),
        "merged_catalog_count": len(merged_catalog_map),
        "weekly_excel_row_count": len(recomputed_rows),
        "issue_row_count": len(issue_rows),
        "deduped_issue_row_count": len(deduped_issue_rows),
        "issue_breakdown": dict(Counter(flag for row in issue_rows for flag in row["issue_flags"])),
        "deduped_issue_breakdown": dict(Counter(flag for row in deduped_issue_rows for flag in row["issue_flags"])),
        "timing_breakdown": dict(timing_bucket_counts),
        "timing_dedup_breakdown": dict(timing_bucket_dedup_counts),
        "issue_pattern_count": len(issue_patterns),
        "master_pattern_count": len(master_patterns),
    }

    if args.timing_summary_json and Path(args.timing_summary_json).exists():
        summary["prior_timing_summary"] = json.loads(Path(args.timing_summary_json).read_text())
    if args.likely_late_incidents_json and Path(args.likely_late_incidents_json).exists():
        incidents = json.loads(Path(args.likely_late_incidents_json).read_text())
        summary["likely_late_incident_email_count"] = len(incidents)

    (output_dir / "full_dirt_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

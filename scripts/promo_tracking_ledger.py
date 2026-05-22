"""Build a stable item/date ledger from parsed weekly promo rows.

The existing audit outputs are row-oriented. This script builds a stronger
tracking model around promo-line identity so recurring carryover rows can be
distinguished from first-observed incidents.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List

try:
    from order_forecast.scripts.promo_audit_paths import default_audit_dir, default_tracking_dir
except ImportError:
    from promo_audit_paths import default_audit_dir, default_tracking_dir  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build promo tracking ledger from weekly_excel_rows.csv")
    parser.add_argument(
        "--input",
        default=None,
        help="Path to weekly_excel_rows.csv",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for ledger outputs",
    )
    parser.add_argument(
        "--account",
        default="",
        help="Optional exact account filter, e.g. Smiths",
    )
    parser.add_argument(
        "--route",
        default="989262",
        help="Route number used for default input/output paths",
    )
    args = parser.parse_args()
    if not args.input:
        args.input = str(default_audit_dir(args.route) / "weekly_excel_rows.csv")
    if not args.output_dir:
        args.output_dir = str(default_tracking_dir(args.route))
    return args


@dataclass(frozen=True)
class LineIdentity:
    account: str
    sap: str
    description_key: str
    start_date: str
    end_date: str
    price: str

    @property
    def line_key(self) -> str:
        return "|".join(
            [
                self.account,
                self.sap,
                self.description_key,
                self.start_date,
                self.end_date,
                self.price,
            ]
        )


def parse_iso_date(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_space(value: str) -> str:
    return " ".join((value or "").split()).strip()


def normalize_description_key(value: str) -> str:
    return normalize_space((value or "").lower())


def parse_jsonish_list(value: str) -> List[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return [raw]
    if isinstance(loaded, list):
        return [str(item) for item in loaded if str(item).strip()]
    if loaded in (None, ""):
        return []
    return [str(loaded)]


def valid_date_window(start_dt: date | None, end_dt: date | None) -> bool:
    if not start_dt or not end_dt:
        return False
    if end_dt < start_dt:
        return False
    if start_dt.year < 2024 or start_dt.year > 2027:
        return False
    if end_dt.year < 2024 or end_dt.year > 2027:
        return False
    if (end_dt - start_dt).days > 180:
        return False
    return True


def classify_timing(source_dt: date | None, start_dt: date | None, end_dt: date | None, occurrence_index: int) -> str:
    if not source_dt or not valid_date_window(start_dt, end_dt):
        return "invalid_date_window"
    assert start_dt is not None
    assert end_dt is not None
    if source_dt < start_dt:
        return "advance_notice"
    if source_dt == start_dt:
        return "starts_same_day"
    if source_dt > end_dt:
        return "first_observed_after_end" if occurrence_index == 1 else "repeated_after_end"
    return "first_observed_after_start" if occurrence_index == 1 else "repeated_after_start"


def write_csv(path: Path, rows: Iterable[Dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = dict(row)
            for key, value in list(normalized.items()):
                if isinstance(value, list):
                    normalized[key] = json.dumps(value, ensure_ascii=True)
            writer.writerow(normalized)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    rows = list(csv.DictReader(input_path.open()))

    if args.account:
        rows = [row for row in rows if normalize_space(row.get("account", "")) == args.account]

    for row in rows:
        row["account"] = normalize_space(row.get("account", ""))
        row["description"] = normalize_space(row.get("description", ""))
        row["price"] = normalize_space(row.get("price", ""))
        row["issue_flags"] = parse_jsonish_list(row.get("issue_flags", ""))
        row["spelling_typos"] = parse_jsonish_list(row.get("spelling_typos", ""))
        row["token_typos"] = parse_jsonish_list(row.get("token_typos", ""))
        row["source_dt"] = parse_iso_date(row.get("source_date", ""))
        row["start_dt"] = parse_iso_date(row.get("start_date", ""))
        row["end_dt"] = parse_iso_date(row.get("end_date", ""))
        identity = LineIdentity(
            account=row["account"],
            sap=(row.get("sap", "") or "").strip(),
            description_key=normalize_description_key(row["description"]),
            start_date=(row.get("start_date", "") or "").strip(),
            end_date=(row.get("end_date", "") or "").strip(),
            price=row["price"],
        )
        row["line_key"] = identity.line_key

    rows.sort(
        key=lambda row: (
            row.get("line_key", ""),
            row.get("source_date", ""),
            row.get("source_subject", ""),
            row.get("attachment_name", ""),
        )
    )

    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        grouped[row["line_key"]].append(row)

    appearance_rows: List[Dict] = []
    line_summary_rows: List[Dict] = []
    email_summary: Dict[tuple[str, str], Counter] = defaultdict(Counter)

    for line_key, line_rows in grouped.items():
        line_rows.sort(
            key=lambda row: (
                row.get("source_date", ""),
                row.get("source_subject", ""),
                row.get("attachment_name", ""),
            )
        )
        first = line_rows[0]
        first_dt = first["source_dt"]
        start_dt = first["start_dt"]
        end_dt = first["end_dt"]

        issue_union = sorted({flag for row in line_rows for flag in row["issue_flags"]})
        issue_dates = sorted({row["source_date"] for row in line_rows if row["issue_flags"]})
        spelling_union = sorted({flag for row in line_rows for flag in row["spelling_typos"]})
        lexical_union = sorted({flag for row in line_rows for flag in row["token_typos"]})
        source_dates = [row["source_date"] for row in line_rows]
        source_subjects = [row["source_subject"] for row in line_rows]
        attachments = [row["attachment_name"] for row in line_rows]

        for occurrence_index, row in enumerate(line_rows, start=1):
            source_dt = row["source_dt"]
            timing_status = classify_timing(source_dt, row["start_dt"], row["end_dt"], occurrence_index)
            days_from_start = ""
            days_to_end = ""
            if source_dt and row["start_dt"]:
                days_from_start = (source_dt - row["start_dt"]).days
            if source_dt and row["end_dt"]:
                days_to_end = (row["end_dt"] - source_dt).days

            appearance = {
                "line_key": line_key,
                "appearance_index": occurrence_index,
                "appearance_count": len(line_rows),
                "source_date": row["source_date"],
                "source_subject": row["source_subject"],
                "attachment_name": row["attachment_name"],
                "account": row["account"],
                "sap": row.get("sap", ""),
                "description": row["description"],
                "start_date": row.get("start_date", ""),
                "end_date": row.get("end_date", ""),
                "price": row["price"],
                "first_seen_date": first["source_date"],
                "first_seen_subject": first["source_subject"],
                "days_from_start": days_from_start,
                "days_to_end": days_to_end,
                "timing_status": timing_status,
                "issue_flags": row["issue_flags"],
                "line_issue_flags": issue_union,
                "line_issue_dates": issue_dates,
                "spelling_typos": row["spelling_typos"],
                "token_typos": row["token_typos"],
            }
            appearance_rows.append(appearance)

            email_key = (row["source_date"], row["source_subject"])
            email_summary[email_key]["appearance_rows"] += 1
            email_summary[email_key][timing_status] += 1
            if occurrence_index == 1:
                email_summary[email_key]["new_lines"] += 1
            else:
                email_summary[email_key]["repeat_lines"] += 1
            if row["issue_flags"]:
                email_summary[email_key]["appearance_rows_with_issue_flags"] += 1
            if issue_union:
                email_summary[email_key]["line_appearances_with_any_issue_history"] += 1

        first_timing_status = classify_timing(first_dt, start_dt, end_dt, 1)
        line_summary_rows.append(
            {
                "line_key": line_key,
                "account": first["account"],
                "sap": first.get("sap", ""),
                "description": first["description"],
                "start_date": first.get("start_date", ""),
                "end_date": first.get("end_date", ""),
                "price": first["price"],
                "first_seen_date": first["source_date"],
                "first_seen_subject": first["source_subject"],
                "last_seen_date": line_rows[-1]["source_date"],
                "last_seen_subject": line_rows[-1]["source_subject"],
                "appearance_count": len(line_rows),
                "distinct_email_count": len(set(source_dates)),
                "first_timing_status": first_timing_status,
                "first_seen_days_from_start": (first_dt - start_dt).days if first_dt and start_dt else "",
                "first_seen_days_to_end": (end_dt - first_dt).days if first_dt and end_dt else "",
                "issue_flags": issue_union,
                "issue_dates": issue_dates,
                "spelling_typos": spelling_union,
                "token_typos": lexical_union,
                "all_source_dates": source_dates,
                "all_source_subjects": source_subjects,
                "all_attachments": attachments,
            }
        )

    line_summary_rows.sort(
        key=lambda row: (
            row.get("account", ""),
            row.get("first_seen_date", ""),
            row.get("sap", ""),
            row.get("description", ""),
        )
    )
    appearance_rows.sort(
        key=lambda row: (
            row.get("source_date", ""),
            row.get("account", ""),
            row.get("sap", ""),
            row.get("description", ""),
            int(row.get("appearance_index", 0)),
        )
    )

    email_summary_rows: List[Dict] = []
    for (source_date, source_subject), counter in sorted(email_summary.items()):
        row = {"source_date": source_date, "source_subject": source_subject}
        row.update(counter)
        email_summary_rows.append(row)

    first_seen_incident_rows = [
        row
        for row in line_summary_rows
        if row["first_timing_status"] in {"first_observed_after_start", "first_observed_after_end", "invalid_date_window"}
    ]
    repeated_carryover_rows = [
        row for row in appearance_rows if row["timing_status"] == "repeated_after_start"
    ]

    summary = {
        "input": str(input_path),
        "account_filter": args.account,
        "appearance_row_count": len(appearance_rows),
        "tracked_line_count": len(line_summary_rows),
        "timing_breakdown_by_appearance": dict(Counter(row["timing_status"] for row in appearance_rows)),
        "first_timing_breakdown_by_line": dict(Counter(row["first_timing_status"] for row in line_summary_rows)),
        "issue_line_breakdown": dict(Counter(flag for row in line_summary_rows for flag in row["issue_flags"])),
        "accounts": dict(Counter(row["account"] for row in line_summary_rows)),
    }

    if args.account:
        prefix = args.account.lower().replace("'", "").replace(" ", "_") + "_"
    else:
        prefix = ""

    write_csv(
        output_dir / f"{prefix}promo_appearance_ledger.csv",
        appearance_rows,
        [
            "line_key",
            "appearance_index",
            "appearance_count",
            "source_date",
            "source_subject",
            "attachment_name",
            "account",
            "sap",
            "description",
            "start_date",
            "end_date",
            "price",
            "first_seen_date",
            "first_seen_subject",
            "days_from_start",
            "days_to_end",
            "timing_status",
            "issue_flags",
            "line_issue_flags",
            "line_issue_dates",
            "spelling_typos",
            "token_typos",
        ],
    )
    write_csv(
        output_dir / f"{prefix}promo_line_summary.csv",
        line_summary_rows,
        [
            "line_key",
            "account",
            "sap",
            "description",
            "start_date",
            "end_date",
            "price",
            "first_seen_date",
            "first_seen_subject",
            "last_seen_date",
            "last_seen_subject",
            "appearance_count",
            "distinct_email_count",
            "first_timing_status",
            "first_seen_days_from_start",
            "first_seen_days_to_end",
            "issue_flags",
            "issue_dates",
            "spelling_typos",
            "token_typos",
            "all_source_dates",
            "all_source_subjects",
            "all_attachments",
        ],
    )
    write_csv(
        output_dir / f"{prefix}promo_email_summary.csv",
        email_summary_rows,
        [
            "source_date",
            "source_subject",
            "appearance_rows",
            "new_lines",
            "repeat_lines",
            "advance_notice",
            "starts_same_day",
            "first_observed_after_start",
            "repeated_after_start",
            "first_observed_after_end",
            "repeated_after_end",
            "invalid_date_window",
            "appearance_rows_with_issue_flags",
            "line_appearances_with_any_issue_history",
        ],
    )
    write_csv(
        output_dir / f"{prefix}promo_first_seen_incidents.csv",
        first_seen_incident_rows,
        [
            "line_key",
            "account",
            "sap",
            "description",
            "start_date",
            "end_date",
            "price",
            "first_seen_date",
            "first_seen_subject",
            "last_seen_date",
            "last_seen_subject",
            "appearance_count",
            "distinct_email_count",
            "first_timing_status",
            "first_seen_days_from_start",
            "first_seen_days_to_end",
            "issue_flags",
            "issue_dates",
            "spelling_typos",
            "token_typos",
            "all_source_dates",
            "all_source_subjects",
            "all_attachments",
        ],
    )
    write_csv(
        output_dir / f"{prefix}promo_repeated_carryovers.csv",
        repeated_carryover_rows,
        [
            "line_key",
            "appearance_index",
            "appearance_count",
            "source_date",
            "source_subject",
            "attachment_name",
            "account",
            "sap",
            "description",
            "start_date",
            "end_date",
            "price",
            "first_seen_date",
            "first_seen_subject",
            "days_from_start",
            "days_to_end",
            "timing_status",
            "issue_flags",
            "line_issue_flags",
            "line_issue_dates",
            "spelling_typos",
            "token_typos",
        ],
    )
    (output_dir / f"{prefix}summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        f"# Promo Tracking Summary{f' ({args.account})' if args.account else ''}",
        "",
        f"- Tracked promo lines: `{summary['tracked_line_count']}`",
        f"- Appearance rows: `{summary['appearance_row_count']}`",
        "",
        "## First Seen Timing",
        "",
    ]
    for key, count in sorted(summary["first_timing_breakdown_by_line"].items()):
        lines.append(f"- {key}: `{count}`")
    lines.extend(["", "## Issue Lines", ""])
    for key, count in sorted(summary["issue_line_breakdown"].items()):
        lines.append(f"- {key}: `{count}`")
    (output_dir / f"{prefix}README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

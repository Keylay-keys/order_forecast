"""Build a deeper, evidence-grade anomaly report from the promo warehouse.

This report separates:
- hard defects: date defects, impossible timing, strong SAP/description mismatches
- review buckets: grouped multi-SAP rows and description/SAP drift that need judgment
- pipeline defects: warehouse-side lossiness such as blank prices
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

try:
    from order_forecast.scripts.promo_audit_paths import default_tracking_dir, default_warehouse_db
except ImportError:
    from promo_audit_paths import default_tracking_dir, default_warehouse_db  # type: ignore


HARD_FLAG_PREFIXES = (
    "description_mismatch_for_sap",
    "listener_strict_mismatch",
    "wrong_sap_for_description",
    "brand_mismatch_for_sap",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build deep anomaly report from promo warehouse")
    parser.add_argument("--route", default="989262")
    parser.add_argument("--db", default=None, help="DuckDB warehouse path")
    parser.add_argument("--output-dir", default=None, help="Directory for report outputs")
    return parser.parse_args()


def default_db_path(route: str) -> Path:
    base = Path(default_warehouse_db(route))
    rebuild = base.with_name(base.stem + "_rebuild" + base.suffix)
    return rebuild if rebuild.exists() else base


def normalize_json_list(raw: object) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(parsed, list):
        return [str(v) for v in parsed if str(v).strip()]
    if parsed in ("", None):
        return []
    return [str(parsed)]


def write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def summarize_examples(df: pd.DataFrame, columns: Iterable[str], limit: int = 5) -> list[dict]:
    if df.empty:
        return []
    cols = [c for c in columns if c in df.columns]
    return df.loc[:, cols].head(limit).to_dict(orient="records")


def build_markdown(summary: dict, examples: dict[str, list[dict]]) -> str:
    lines: list[str] = []
    lines.append("# Deep Promo Anomaly Report")
    lines.append("")
    lines.append(f"Warehouse: `{summary['db_path']}`")
    lines.append("")
    lines.append("## Summary")
    for key, value in summary["counts"].items():
        lines.append(f"- `{key}`: `{value}`")

    sections = [
        ("Hard Defects", "hard_defects"),
        ("Review Buckets", "review_buckets"),
        ("Pipeline Defects", "pipeline_defects"),
    ]
    for title, group_key in sections:
        lines.append("")
        lines.append(f"## {title}")
        for item in summary[group_key]:
            lines.append(f"- `{item['name']}`: `{item['count']}`")
            example_key = item["name"]
            sample_rows = examples.get(example_key, [])
            if sample_rows:
                for sample in sample_rows[:3]:
                    pretty = ", ".join(f"{k}={v}" for k, v in sample.items())
                    lines.append(f"  - {pretty}")
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    db_path = Path(args.db) if args.db else default_db_path(args.route)
    output_dir = Path(args.output_dir) if args.output_dir else Path("output") / f"promo_deep_audit_{args.route}"
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path), read_only=True)
    weekly = conn.execute("select * from audit_weekly_excel_rows").fetchdf()
    line_summary = conn.execute("select * from audit_promo_line_summary").fetchdf()
    manual_items = conn.execute("select * from audit_manual_extract_items").fetchdf()
    source_emails = conn.execute("select * from audit_source_emails").fetchdf()
    conn.close()

    for df in (weekly, line_summary, manual_items, source_emails):
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].fillna("")

    weekly["start_dt"] = to_date(weekly["start_date"])
    weekly["end_dt"] = to_date(weekly["end_date"])
    weekly["source_dt"] = to_date(weekly["source_date"])
    weekly["issue_flag_list"] = weekly["issue_flags"].map(normalize_json_list)

    line_summary["start_dt"] = to_date(line_summary["start_date"])
    line_summary["end_dt"] = to_date(line_summary["end_date"])
    line_summary["first_seen_dt"] = to_date(line_summary["first_seen_date"])
    line_summary["issue_flag_list"] = line_summary["issue_flags"].map(normalize_json_list)
    line_summary["spelling_typo_list"] = line_summary["spelling_typos"].map(normalize_json_list)
    line_summary["lexical_variant_list"] = line_summary["token_typos"].map(normalize_json_list)

    manual_items["start_dt"] = to_date(manual_items["start_date"])
    manual_items["end_dt"] = to_date(manual_items["end_date"])
    manual_items["source_dt"] = to_date(manual_items["source_date"])

    bad_year_weekly = weekly[
        (weekly["start_dt"].notna() & ((weekly["start_dt"].dt.year < 2024) | (weekly["start_dt"].dt.year > 2027)))
        | (weekly["end_dt"].notna() & ((weekly["end_dt"].dt.year < 2024) | (weekly["end_dt"].dt.year > 2027)))
    ].copy()
    bad_year_manual = manual_items[
        (manual_items["start_dt"].notna() & ((manual_items["start_dt"].dt.year < 2024) | (manual_items["start_dt"].dt.year > 2027)))
        | (manual_items["end_dt"].notna() & ((manual_items["end_dt"].dt.year < 2024) | (manual_items["end_dt"].dt.year > 2027)))
    ].copy()
    bad_year_rows = pd.concat(
        [
            bad_year_weekly.assign(source_kind="weekly_excel"),
            bad_year_manual.rename(columns={"extract_file": "attachment_name"}).assign(source_kind="manual_extract"),
        ],
        ignore_index=True,
        sort=False,
    )

    end_before_start = weekly[
        weekly["start_dt"].notna() & weekly["end_dt"].notna() & (weekly["end_dt"] < weekly["start_dt"])
    ].copy()
    overlong_windows = weekly[
        weekly["start_dt"].notna()
        & weekly["end_dt"].notna()
        & ((weekly["end_dt"] - weekly["start_dt"]).dt.days > 180)
    ].copy()
    expired_before_send = line_summary[line_summary["first_timing_status"] == "first_observed_after_end"].copy()
    invalid_windows = line_summary[line_summary["first_timing_status"] == "invalid_date_window"].copy()

    def has_hard_flag(flags: list[str]) -> bool:
        return any(flag.startswith(HARD_FLAG_PREFIXES) for flag in flags)

    hard_flag_rows = weekly[weekly["issue_flag_list"].map(has_hard_flag)].copy()
    route_missing_rows = weekly[weekly["issue_flag_list"].map(lambda vals: "sap_not_in_route_catalog" in vals)].copy()
    any_catalog_missing_rows = weekly[weekly["issue_flag_list"].map(lambda vals: "sap_not_in_any_catalog" in vals)].copy()

    spelling_rows = line_summary[line_summary["spelling_typo_list"].map(bool)].copy()
    lexical_rows = line_summary[line_summary["lexical_variant_list"].map(bool)].copy()

    same_sap_multi_desc = (
        weekly[weekly["sap"].ne("") & weekly["description"].ne("")]
        .assign(description_key=weekly["description"].str.lower())
        .groupby(["account", "sap"], as_index=False)
        .agg(
            description_count=("description_key", "nunique"),
            descriptions=("description", lambda s: sorted(set(s))),
            source_subjects=("source_subject", lambda s: sorted(set(s))),
            row_count=("description", "size"),
        )
    )
    same_sap_multi_desc = same_sap_multi_desc[same_sap_multi_desc["description_count"] > 1].copy()

    same_desc_multi_sap = (
        weekly[weekly["sap"].ne("") & weekly["description"].ne("")]
        .assign(description_key=weekly["description"].str.lower())
        .groupby(["account", "description_key"], as_index=False)
        .agg(
            sap_count=("sap", "nunique"),
            saps=("sap", lambda s: sorted(set(s))),
            sample_description=("description", "first"),
            source_subjects=("source_subject", lambda s: sorted(set(s))),
            row_count=("sap", "size"),
        )
    )
    same_desc_multi_sap = same_desc_multi_sap[same_desc_multi_sap["sap_count"] > 1].copy()

    multi_sap_rows = weekly[pd.to_numeric(weekly["sap_count_in_cell"], errors="coerce").fillna(0) > 1].copy()
    blank_price_rows = weekly[weekly["price"].eq("")].copy()
    body_only_sources = source_emails[(source_emails["has_xlsx"] == "false") & (source_emails["has_pdf"] == "false")].copy()

    outputs = {
        "bad_year_rows": bad_year_rows,
        "end_before_start_rows": end_before_start,
        "overlong_windows": overlong_windows,
        "expired_before_send": expired_before_send,
        "invalid_windows": invalid_windows,
        "hard_flag_rows": hard_flag_rows,
        "route_missing_rows": route_missing_rows,
        "any_catalog_missing_rows": any_catalog_missing_rows,
        "spelling_rows": spelling_rows,
        "lexical_rows": lexical_rows,
        "same_sap_multi_desc": same_sap_multi_desc,
        "same_desc_multi_sap": same_desc_multi_sap,
        "multi_sap_rows": multi_sap_rows,
        "blank_price_rows": blank_price_rows,
        "body_only_sources": body_only_sources,
    }

    for name, df in outputs.items():
        serializable = df.copy()
        for col in serializable.columns:
            if any(isinstance(v, list) for v in serializable[col].tolist()):
                serializable[col] = serializable[col].map(lambda v: json.dumps(v, ensure_ascii=True) if isinstance(v, list) else v)
        write_csv(output_dir / f"{name}.csv", serializable)

    counts = {name: int(len(df)) for name, df in outputs.items()}
    summary = {
        "route": args.route,
        "db_path": str(db_path),
        "counts": counts,
        "hard_defects": [
            {"name": "bad_year_rows", "count": counts["bad_year_rows"]},
            {"name": "end_before_start_rows", "count": counts["end_before_start_rows"]},
            {"name": "overlong_windows", "count": counts["overlong_windows"]},
            {"name": "expired_before_send", "count": counts["expired_before_send"]},
            {"name": "invalid_windows", "count": counts["invalid_windows"]},
            {"name": "hard_flag_rows", "count": counts["hard_flag_rows"]},
            {"name": "route_missing_rows", "count": counts["route_missing_rows"]},
            {"name": "any_catalog_missing_rows", "count": counts["any_catalog_missing_rows"]},
            {"name": "spelling_rows", "count": counts["spelling_rows"]},
        ],
        "review_buckets": [
            {"name": "lexical_rows", "count": counts["lexical_rows"]},
            {"name": "same_sap_multi_desc", "count": counts["same_sap_multi_desc"]},
            {"name": "same_desc_multi_sap", "count": counts["same_desc_multi_sap"]},
            {"name": "multi_sap_rows", "count": counts["multi_sap_rows"]},
        ],
        "pipeline_defects": [
            {"name": "blank_price_rows", "count": counts["blank_price_rows"]},
            {"name": "body_only_sources", "count": counts["body_only_sources"]},
        ],
    }

    examples = {
        "bad_year_rows": summarize_examples(
            bad_year_rows,
            ["source_kind", "source_date", "source_subject", "attachment_name", "account", "sap", "description", "start_date", "end_date"],
        ),
        "end_before_start_rows": summarize_examples(
            end_before_start,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description", "start_date", "end_date"],
        ),
        "overlong_windows": summarize_examples(
            overlong_windows.assign(span_days=(overlong_windows["end_dt"] - overlong_windows["start_dt"]).dt.days),
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description", "start_date", "end_date", "span_days"],
        ),
        "expired_before_send": summarize_examples(
            expired_before_send,
            ["account", "sap", "description", "start_date", "end_date", "first_seen_date", "first_seen_subject"],
        ),
        "invalid_windows": summarize_examples(
            invalid_windows,
            ["account", "sap", "description", "start_date", "end_date", "first_seen_date", "first_seen_subject"],
        ),
        "hard_flag_rows": summarize_examples(
            hard_flag_rows,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description", "catalog_name", "suggested_sap", "issue_flags"],
        ),
        "route_missing_rows": summarize_examples(
            route_missing_rows,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description", "issue_flags"],
        ),
        "any_catalog_missing_rows": summarize_examples(
            any_catalog_missing_rows,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description", "issue_flags"],
        ),
        "spelling_rows": summarize_examples(
            spelling_rows,
            ["account", "sap", "description", "spelling_typos", "first_seen_date", "first_seen_subject"],
        ),
        "lexical_rows": summarize_examples(
            lexical_rows,
            ["account", "sap", "description", "token_typos", "first_seen_date", "first_seen_subject"],
        ),
        "same_sap_multi_desc": summarize_examples(same_sap_multi_desc, ["account", "sap", "description_count", "descriptions"]),
        "same_desc_multi_sap": summarize_examples(same_desc_multi_sap, ["account", "sample_description", "sap_count", "saps"]),
        "multi_sap_rows": summarize_examples(
            multi_sap_rows,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "sap_count_in_cell", "description"],
        ),
        "blank_price_rows": summarize_examples(
            blank_price_rows,
            ["source_date", "source_subject", "attachment_name", "account", "sap", "description"],
        ),
        "body_only_sources": summarize_examples(body_only_sources, ["source_date", "source_subject"]),
    }

    write_json(output_dir / "summary.json", summary)
    write_json(output_dir / "examples.json", examples)
    (output_dir / "report.md").write_text(build_markdown(summary, examples), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

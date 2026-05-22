"""Build a DuckDB warehouse for promo-audit analysis.

This materializes the CSV/JSON outputs from the promo audit pipeline into
stable DuckDB tables so the data can be queried directly instead of relying
on ad hoc CSV inspection.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

try:
    from order_forecast.scripts.promo_audit_paths import default_audit_dir, default_tracking_dir, default_warehouse_db
except ImportError:
    from promo_audit_paths import default_audit_dir, default_tracking_dir, default_warehouse_db  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build promo audit DuckDB warehouse")
    parser.add_argument(
        "--audit-dir",
        default=None,
        help="Directory containing promo audit CSV/JSON outputs",
    )
    parser.add_argument(
        "--tracking-dir",
        default=None,
        help="Directory containing promo tracking ledger CSV/JSON outputs",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database to build",
    )
    parser.add_argument(
        "--route",
        default="989262",
        help="Route number label to attach to warehouse metadata",
    )
    args = parser.parse_args()
    if not args.audit_dir:
        args.audit_dir = str(default_audit_dir(args.route))
    if not args.tracking_dir:
        args.tracking_dir = str(default_tracking_dir(args.route))
    if not args.db:
        args.db = str(default_warehouse_db(args.route))
    return args


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def normalize_csv_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for column in df.columns:
        df[column] = df[column].astype(str)
    return df


def load_json_records(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = [payload]
    return pd.json_normalize(payload)


def parse_json_list(value: object) -> list[str]:
    if value is None:
        return []
    raw = str(value).strip()
    if not raw or raw.lower() in {"nan", "none"}:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return [raw]
    if isinstance(loaded, list):
        return [str(item) for item in loaded if str(item).strip()]
    if loaded in ("", None):
        return []
    return [str(loaded)]


def explode_list_column(df: pd.DataFrame, table_key: str, list_column: str, value_column: str) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for row in df.to_dict(orient="records"):
        base_key = str(row.get(table_key, "")).strip()
        for index, value in enumerate(parse_json_list(row.get(list_column, "")), start=1):
            rows.append(
                {
                    table_key: base_key,
                    "ordinal": str(index),
                    value_column: value,
                }
            )
    return pd.DataFrame(rows)


def normalize_space(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def explode_manual_extracts(manual_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_rows: list[dict] = []
    item_rows: list[dict] = []
    if not manual_dir.exists():
        return pd.DataFrame(), pd.DataFrame()

    for csv_path in sorted(manual_dir.glob("*.csv")):
        # Ignore macOS AppleDouble sidecars copied onto external drives.
        if csv_path.name.startswith("."):
            continue
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        for idx, row in enumerate(df.to_dict(orient="records"), start=1):
            row = {k: str(v) for k, v in row.items()}
            row["extract_file"] = csv_path.name
            row["extract_row_id"] = f"{csv_path.stem}:{idx}"
            raw_rows.append(row)

            sap_codes = re.findall(r"\b(\d{4,6})\b", row.get("sap_codes", ""))
            for sap_index, sap in enumerate(sap_codes, start=1):
                item_description = normalize_space(row.get("item_description", ""))
                account = normalize_space(row.get("account", ""))
                start_date = normalize_space(row.get("start_date", ""))
                end_date = normalize_space(row.get("end_date", ""))
                normalized_description = item_description.lower()
                normalized_key = "|".join([account, sap, normalized_description, start_date, end_date])
                item_rows.append(
                    {
                        "extract_row_id": row["extract_row_id"],
                        "extract_file": csv_path.name,
                        "source_subject": row.get("source_subject", ""),
                        "source_date": row.get("source_date", ""),
                        "source_type": row.get("source_type", ""),
                        "source_image": row.get("source_image", ""),
                        "account": account,
                        "activity": row.get("activity", ""),
                        "start_date": start_date,
                        "end_date": end_date,
                        "ad_date": row.get("ad_date", ""),
                        "price": row.get("price", ""),
                        "description": item_description,
                        "sap": sap,
                        "sap_codes": row.get("sap_codes", ""),
                        "sap_count_in_cell": str(len(sap_codes)),
                        "transcription_confidence": row.get("transcription_confidence", ""),
                        "notes": row.get("notes", ""),
                        "normalized_key_no_price": normalized_key,
                    }
                )

    return pd.DataFrame(raw_rows), pd.DataFrame(item_rows)


def build_source_email_frames(inventory_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    email_rows: list[dict[str, str]] = []
    attachment_rows: list[dict[str, str]] = []

    for item in payload:
        source_date = str(item.get("date", ""))
        subject = str(item.get("subject", ""))
        email_key = f"{source_date}|{subject}"
        email_rows.append(
            {
                "email_key": email_key,
                "source_date": source_date,
                "source_subject": subject,
                "has_xlsx": str(bool(item.get("has_xlsx"))).lower(),
                "has_pdf": str(bool(item.get("has_pdf"))).lower(),
                "attachment_count": str(len(item.get("attachments", []) or [])),
            }
        )
        for index, attachment in enumerate(item.get("attachments", []) or [], start=1):
            attachment_name = str(attachment.get("file", ""))
            attachment_category = str(attachment.get("category", ""))
            attachment_rows.append(
                {
                    "email_key": email_key,
                    "attachment_key": f"{email_key}|{index}",
                    "attachment_ordinal": str(index),
                    "attachment_name": attachment_name,
                    "attachment_category": attachment_category,
                }
            )

    return pd.DataFrame(email_rows), pd.DataFrame(attachment_rows)


def register_frame(conn: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> None:
    if df.empty:
        conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM (SELECT 1 AS _dummy) WHERE 1=0")
        return
    conn.register(f"tmp_{name}", df)
    conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmp_{name}")
    conn.unregister(f"tmp_{name}")


def main() -> None:
    args = parse_args()
    audit_dir = Path(args.audit_dir)
    tracking_dir = Path(args.tracking_dir)
    db_path = Path(args.db)
    ensure_parent(db_path)

    conn = duckdb.connect(str(db_path))
    conn.execute("PRAGMA threads=4")

    source_emails_df, email_attachments_df = build_source_email_frames(audit_dir / "inventory.json")
    weekly_excel_rows_df = normalize_csv_frame(audit_dir / "weekly_excel_rows.csv")
    issue_candidates_df = normalize_csv_frame(audit_dir / "issue_candidates_deduped.csv")
    promo_line_summary_df = normalize_csv_frame(tracking_dir / "promo_line_summary.csv")
    promo_appearance_ledger_df = normalize_csv_frame(tracking_dir / "promo_appearance_ledger.csv")
    promo_first_seen_incidents_df = normalize_csv_frame(tracking_dir / "promo_first_seen_incidents.csv")
    promo_repeated_carryovers_df = normalize_csv_frame(tracking_dir / "promo_repeated_carryovers.csv")
    promo_email_summary_df = normalize_csv_frame(tracking_dir / "promo_email_summary.csv")
    smiths_line_summary_df = normalize_csv_frame(tracking_dir / "smiths_promo_line_summary.csv")
    smiths_first_seen_incidents_df = normalize_csv_frame(tracking_dir / "smiths_promo_first_seen_incidents.csv")
    smiths_repeated_carryovers_df = normalize_csv_frame(tracking_dir / "smiths_promo_repeated_carryovers.csv")
    smiths_email_summary_df = normalize_csv_frame(tracking_dir / "smiths_promo_email_summary.csv")
    manual_extract_rows_df, manual_extract_items_df = explode_manual_extracts(tracking_dir / "manual_extracts")
    audit_summary_df = load_json_records(audit_dir / "summary.json")
    tracking_summary_df = load_json_records(tracking_dir / "summary.json")
    smiths_summary_df = load_json_records(tracking_dir / "smiths_summary.json")

    register_frame(conn, "audit_source_emails", source_emails_df)
    register_frame(conn, "audit_email_attachments", email_attachments_df)
    register_frame(conn, "audit_weekly_excel_rows", weekly_excel_rows_df)
    register_frame(conn, "audit_issue_candidates", issue_candidates_df)
    register_frame(conn, "audit_promo_line_summary", promo_line_summary_df)
    register_frame(conn, "audit_promo_appearance_ledger", promo_appearance_ledger_df)
    register_frame(conn, "audit_promo_first_seen_incidents", promo_first_seen_incidents_df)
    register_frame(conn, "audit_promo_repeated_carryovers", promo_repeated_carryovers_df)
    register_frame(conn, "audit_promo_email_summary", promo_email_summary_df)
    register_frame(conn, "audit_smiths_promo_line_summary", smiths_line_summary_df)
    register_frame(conn, "audit_smiths_first_seen_incidents", smiths_first_seen_incidents_df)
    register_frame(conn, "audit_smiths_repeated_carryovers", smiths_repeated_carryovers_df)
    register_frame(conn, "audit_smiths_email_summary", smiths_email_summary_df)
    register_frame(conn, "audit_manual_extract_rows", manual_extract_rows_df)
    register_frame(conn, "audit_manual_extract_items", manual_extract_items_df)
    register_frame(conn, "audit_summary", audit_summary_df)
    register_frame(conn, "tracking_summary", tracking_summary_df)
    register_frame(conn, "smiths_summary", smiths_summary_df)

    register_frame(
        conn,
        "audit_line_issue_flags",
        explode_list_column(promo_line_summary_df, "line_key", "issue_flags", "issue_flag"),
    )
    register_frame(
        conn,
        "audit_line_issue_dates",
        explode_list_column(promo_line_summary_df, "line_key", "issue_dates", "issue_date"),
    )
    register_frame(
        conn,
        "audit_line_spelling_typos",
        explode_list_column(promo_line_summary_df, "line_key", "spelling_typos", "spelling_typo"),
    )
    register_frame(
        conn,
        "audit_line_lexical_variants",
        explode_list_column(promo_line_summary_df, "line_key", "token_typos", "lexical_variant"),
    )
    register_frame(
        conn,
        "audit_smiths_line_issue_flags",
        explode_list_column(smiths_line_summary_df, "line_key", "issue_flags", "issue_flag"),
    )
    register_frame(
        conn,
        "audit_smiths_line_issue_dates",
        explode_list_column(smiths_line_summary_df, "line_key", "issue_dates", "issue_date"),
    )

    conn.execute(
        """
        CREATE OR REPLACE VIEW v_late_report_lines AS
        SELECT *
        FROM audit_promo_line_summary
        WHERE first_timing_status = 'first_observed_after_start'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_repeated_carryovers AS
        SELECT *
        FROM audit_promo_appearance_ledger
        WHERE timing_status = 'repeated_after_start'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_first_seen_bad_dates AS
        SELECT *
        FROM audit_promo_line_summary
        WHERE first_timing_status IN ('invalid_date_window', 'first_observed_after_end')
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_smiths_late_report_lines AS
        SELECT *
        FROM audit_smiths_promo_line_summary
        WHERE first_timing_status = 'first_observed_after_start'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_smiths_issue_lines AS
        SELECT s.*, f.issue_flag
        FROM audit_smiths_promo_line_summary s
        LEFT JOIN audit_smiths_line_issue_flags f USING (line_key)
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_augmented_promo_appearances AS
        SELECT
            account,
            sap,
            lower(description) AS normalized_description,
            start_date,
            end_date,
            source_date,
            source_subject,
            attachment_name AS source_artifact,
            'excel_attachment' AS source_kind,
            '' AS transcription_confidence,
            issue_flags,
            line_key AS original_line_key,
            account || '|' || sap || '|' || lower(description) || '|' || start_date || '|' || end_date AS normalized_key_no_price
        FROM audit_promo_appearance_ledger
        UNION ALL
        SELECT
            account,
            sap,
            lower(description) AS normalized_description,
            start_date,
            end_date,
            source_date,
            source_subject,
            source_image AS source_artifact,
            source_type AS source_kind,
            transcription_confidence,
            '[]' AS issue_flags,
            '' AS original_line_key,
            normalized_key_no_price
        FROM audit_manual_extract_items
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_augmented_line_first_seen AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_key_no_price
                    ORDER BY source_date, source_subject, source_kind, source_artifact
                ) AS rn
            FROM v_augmented_promo_appearances
        )
        SELECT
            normalized_key_no_price,
            account,
            sap,
            normalized_description AS description,
            start_date,
            end_date,
            source_date AS first_seen_date,
            source_subject AS first_seen_subject,
            source_kind AS first_seen_source_kind,
            source_artifact AS first_seen_source_artifact,
            transcription_confidence,
            CASE
                WHEN start_date IS NULL OR start_date = '' THEN NULL
                ELSE DATE_DIFF('day', CAST(start_date AS DATE), CAST(source_date AS DATE))
            END AS first_seen_days_from_start
        FROM ranked
        WHERE rn = 1
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_augmented_actual_late_notifications AS
        SELECT *
        FROM v_augmented_line_first_seen
        WHERE first_seen_days_from_start BETWEEN 1 AND 7
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_augmented_smiths_actual_late_notifications AS
        SELECT *
        FROM v_augmented_actual_late_notifications
        WHERE account = 'Smiths'
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_email_attachment_summary AS
        SELECT
            e.source_date,
            e.source_subject,
            e.has_xlsx,
            e.has_pdf,
            e.attachment_count,
            COUNT(a.attachment_key) AS attachment_rows,
            STRING_AGG(a.attachment_name, ' | ' ORDER BY a.attachment_ordinal) AS attachment_names
        FROM audit_source_emails e
        LEFT JOIN audit_email_attachments a USING (email_key)
        GROUP BY 1,2,3,4,5
        ORDER BY source_date, source_subject
        """
    )

    metadata = pd.DataFrame(
        [
            {
                "route_number": args.route,
                "audit_dir": str(audit_dir),
                "tracking_dir": str(tracking_dir),
                "db_path": str(db_path),
            }
        ]
    )
    register_frame(conn, "audit_warehouse_metadata", metadata)

    counts = {
        "audit_source_emails": conn.execute("SELECT COUNT(*) FROM audit_source_emails").fetchone()[0],
        "audit_email_attachments": conn.execute("SELECT COUNT(*) FROM audit_email_attachments").fetchone()[0],
        "audit_weekly_excel_rows": conn.execute("SELECT COUNT(*) FROM audit_weekly_excel_rows").fetchone()[0],
        "audit_promo_line_summary": conn.execute("SELECT COUNT(*) FROM audit_promo_line_summary").fetchone()[0],
        "audit_promo_appearance_ledger": conn.execute("SELECT COUNT(*) FROM audit_promo_appearance_ledger").fetchone()[0],
        "audit_promo_first_seen_incidents": conn.execute("SELECT COUNT(*) FROM audit_promo_first_seen_incidents").fetchone()[0],
        "audit_smiths_promo_line_summary": conn.execute("SELECT COUNT(*) FROM audit_smiths_promo_line_summary").fetchone()[0],
        "audit_smiths_first_seen_incidents": conn.execute("SELECT COUNT(*) FROM audit_smiths_first_seen_incidents").fetchone()[0],
        "audit_line_issue_flags": conn.execute("SELECT COUNT(*) FROM audit_line_issue_flags").fetchone()[0],
        "audit_manual_extract_items": conn.execute("SELECT COUNT(*) FROM audit_manual_extract_items").fetchone()[0],
        "augmented_actual_late_notifications": conn.execute("SELECT COUNT(*) FROM v_augmented_actual_late_notifications").fetchone()[0],
        "augmented_smiths_actual_late_notifications": conn.execute("SELECT COUNT(*) FROM v_augmented_smiths_actual_late_notifications").fetchone()[0],
    }
    (db_path.with_suffix(".summary.json")).write_text(json.dumps(counts, indent=2), encoding="utf-8")

    print(json.dumps({"db_path": str(db_path), "counts": counts}, indent=2))
    conn.close()


if __name__ == "__main__":
    main()

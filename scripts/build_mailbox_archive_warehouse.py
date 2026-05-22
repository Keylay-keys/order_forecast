"""Build a DuckDB warehouse from the raw mailbox archive manifests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build mailbox archive warehouse")
    parser.add_argument(
        "--archive-root",
        default="/Volumes/Extreme SSD/routespark_promo_audit/restore-2025-09-24/raw_mailbox_archive/mission_forms",
        help="Root directory of raw mailbox archive",
    )
    parser.add_argument(
        "--db",
        default="/Volumes/Extreme SSD/routespark_promo_audit/restore-2025-09-24/raw_mailbox_archive/mission_forms/mailbox_archive.duckdb",
        help="DuckDB file to build",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    if not path.exists():
        return pd.DataFrame()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.json_normalize(rows) if rows else pd.DataFrame()


def register_df(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM (SELECT 1 AS _dummy) WHERE 1=0")
        return
    conn.register(f"tmp_{table}", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM tmp_{table}")
    conn.unregister(f"tmp_{table}")


def main() -> None:
    args = parse_args()
    archive_root = Path(args.archive_root)
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    wrapper_df = load_jsonl(archive_root / "manifests" / "wrapper_messages.jsonl")
    source_df = load_jsonl(archive_root / "manifests" / "source_messages.jsonl")
    attachment_df = load_jsonl(archive_root / "manifests" / "attachments.jsonl")
    summary_payload = json.loads((archive_root / "manifests" / "summary.json").read_text(encoding="utf-8"))
    summary_df = pd.DataFrame([summary_payload])

    conn = duckdb.connect(str(db_path))
    register_df(conn, "archive_wrapper_messages", wrapper_df)
    register_df(conn, "archive_source_messages", source_df)
    register_df(conn, "archive_attachments", attachment_df)
    register_df(conn, "archive_summary", summary_df)

    conn.execute(
        """
        CREATE OR REPLACE VIEW v_archive_source_coverage AS
        SELECT
            s.source_key,
            s.wrapper_key,
            s.date AS source_date,
            s.subject AS source_subject,
            s."from" AS source_from,
            w.date AS wrapper_date,
            w.subject AS wrapper_subject,
            COUNT(a.relative_path) AS attachment_count,
            SUM(CASE WHEN lower(a.filename) LIKE '%.xlsx' OR lower(a.filename) LIKE '%.xls' THEN 1 ELSE 0 END) AS excel_count,
            SUM(CASE WHEN lower(a.filename) LIKE '%.pdf' THEN 1 ELSE 0 END) AS pdf_count
        FROM archive_source_messages s
        LEFT JOIN archive_wrapper_messages w USING (wrapper_key)
        LEFT JOIN archive_attachments a USING (source_key)
        GROUP BY 1,2,3,4,5,6,7
        ORDER BY source_date, source_subject
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW v_archive_subject_rollup AS
        SELECT
            source_date,
            source_subject,
            COUNT(*) AS source_message_count,
            SUM(excel_count) AS total_excel_attachments,
            SUM(pdf_count) AS total_pdf_attachments,
            SUM(attachment_count) AS total_attachments
        FROM v_archive_source_coverage
        GROUP BY 1,2
        ORDER BY source_date, source_subject
        """
    )

    counts = {
        "archive_wrapper_messages": conn.execute("SELECT COUNT(*) FROM archive_wrapper_messages").fetchone()[0],
        "archive_source_messages": conn.execute("SELECT COUNT(*) FROM archive_source_messages").fetchone()[0],
        "archive_attachments": conn.execute("SELECT COUNT(*) FROM archive_attachments").fetchone()[0],
        "coverage_rows": conn.execute("SELECT COUNT(*) FROM v_archive_source_coverage").fetchone()[0],
    }
    (db_path.with_suffix(".summary.json")).write_text(json.dumps(counts, indent=2), encoding="utf-8")
    print(json.dumps({"db_path": str(db_path), "counts": counts}, indent=2))
    conn.close()


if __name__ == "__main__":
    main()

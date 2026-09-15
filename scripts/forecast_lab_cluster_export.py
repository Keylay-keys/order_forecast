#!/usr/bin/env python3
"""Stream a read-only forecast-lab snapshot from the production PostgreSQL DB.

This file is sent to the order-forecast pod over stdin by ``forecast_lab.py``.
It writes only a gzip tar archive to stdout; diagnostics go to stderr. Database
credentials stay in the pod environment and are never included in the archive.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
from datetime import datetime, timezone

import psycopg2
from psycopg2 import sql


TABLES = {
    "orders_historical": {
        "route_column": "route_number",
        "order_by": ("delivery_date", "order_id"),
        "columns": (
            "order_id", "route_number", "user_id", "schedule_key", "delivery_date",
            "order_date", "finalized_at", "total_cases", "total_units", "store_count",
            "status", "is_holiday_week", "order_revision", "last_mutation_kind",
            "last_mutation_id", "last_mutation_at", "reallocation_count",
            "last_reallocated_at", "last_reallocation_id", "scheduled_order", "synced_at",
        ),
    },
    "order_line_items": {
        "route_column": "route_number",
        "order_by": ("delivery_date", "order_id", "store_id", "sap", "line_item_id"),
        "columns": (
            "line_item_id", "order_id", "route_number", "schedule_key", "delivery_date",
            "store_id", "store_name", "sap", "product_name", "quantity", "cases",
            "promo_id", "promo_active", "is_first_weekend_of_month", "is_holiday_week",
            "is_month_end", "day_of_week", "week_of_year", "month", "synced_at",
        ),
    },
    "forecast_corrections": {
        "route_column": "route_number",
        "order_by": ("delivery_date", "order_id", "store_id", "sap", "submitted_at", "correction_id"),
        "columns": (
            "correction_id", "forecast_id", "order_id", "route_number", "schedule_key",
            "delivery_date", "store_id", "store_name", "sap", "predicted_units",
            "predicted_cases", "prediction_confidence", "prediction_source", "final_units",
            "final_cases", "correction_delta", "correction_ratio", "was_removed", "promo_id",
            "promo_active", "is_first_weekend_of_month", "is_holiday_week", "submitted_at",
        ),
    },
    "product_catalog": {
        "route_column": "route_number",
        "order_by": ("route_number", "sap"),
        "columns": (
            "sap", "route_number", "full_name", "short_name", "upc", "brand", "category",
            "sub_category", "case_pack", "tray", "unit_weight", "is_active", "created_at",
            "updated_at", "synced_at",
        ),
    },
    "user_schedules": {
        "route_column": "route_number",
        "order_by": ("route_number", "schedule_key", "id"),
        "columns": (
            "id", "route_number", "user_id", "order_day", "load_day", "delivery_day",
            "load_offset_days", "delivery_offset_days", "schedule_version",
            "needs_schedule_review", "schedule_key", "is_active", "created_at", "updated_at",
            "synced_at",
        ),
    },
    "stores": {
        "route_column": "route_number",
        "order_by": ("route_number", "store_id"),
        "columns": (
            "store_id", "route_number", "store_name", "store_number", "address",
            "delivery_days", "is_active", "created_at", "updated_at", "synced_at",
        ),
    },
    "store_items": {
        "route_column": "route_number",
        "order_by": ("route_number", "store_id", "sap"),
        "columns": (
            "id", "store_id", "route_number", "sap", "is_active", "added_at", "removed_at",
            "synced_at",
        ),
    },
    "forecast_band_calibration": {
        "route_column": "route_number",
        "order_by": ("route_number", "schedule_key", "interval_name"),
        "columns": (
            "route_number", "schedule_key", "interval_name", "band_scale", "target_coverage",
            "observed_coverage", "under_rate", "over_rate", "sample_lines", "fold_count",
            "notes", "last_backtest_at", "updated_at", "band_center_offset_units",
        ),
    },
    "forecast_band_center_calibration": {
        "route_column": "route_number",
        "order_by": ("route_number", "schedule_key", "interval_name"),
        "columns": (
            "route_number", "schedule_key", "interval_name", "center_offset_units",
            "observed_under_rate", "observed_over_rate", "sample_lines", "fold_count", "notes",
            "last_backtest_at", "updated_at",
        ),
    },
    "forecast_band_source_calibration": {
        "route_column": "route_number",
        "order_by": ("route_number", "schedule_key", "source", "interval_name"),
        "columns": (
            "route_number", "schedule_key", "source", "interval_name", "band_scale_mult",
            "center_offset_units", "target_coverage", "observed_coverage",
            "observed_under_rate", "observed_over_rate", "sample_lines", "fold_count", "notes",
            "last_backtest_at", "updated_at",
        ),
    },
    "forecast_finalize_events": {
        "route_column": "route_number",
        "order_by": ("finalized_at", "finalize_key"),
        "columns": (
            "finalize_key", "route_number", "order_id", "schedule_key", "finalized_at", "status",
            "job_keys", "last_error", "source_worker_id", "created_at", "updated_at", "processed_at",
        ),
    },
    "forecast_generation_jobs": {
        "route_column": "route_number",
        "order_by": ("delivery_date", "job_key"),
        "columns": (
            "job_key", "route_number", "schedule_key", "delivery_date", "job_type", "source",
            "finalize_key", "status", "attempts", "max_attempts", "trigger_count", "available_at",
            "claimed_by", "claimed_at", "started_at", "finished_at", "skipped_reason", "last_error",
            "created_at", "updated_at", "last_triggered_at",
        ),
    },
    "prediction_log": {
        "route_column": "route_number",
        "order_by": ("delivery_date", "prediction_id"),
        "columns": (
            "prediction_id", "forecast_id", "route_number", "schedule_key", "delivery_date",
            "store_id", "sap", "model_id", "predicted_quantity", "prediction_confidence",
            "actual_quantity", "prediction_error", "created_at",
        ),
    },
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _where_clause(connection, route_column: str, routes: list[str]) -> sql.Composed:
    if not routes:
        return sql.SQL("")
    literals = sql.SQL(", ").join(sql.Literal(route) for route in routes)
    return sql.SQL(" WHERE {} IN ({})").format(sql.Identifier(route_column), literals)


def _select_query(connection, table: str, spec: dict, routes: list[str]) -> str:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in spec["columns"])
    ordering = sql.SQL(", ").join(sql.Identifier(column) for column in spec["order_by"])
    query = sql.SQL("SELECT {} FROM {}{} ORDER BY {}").format(
        columns,
        sql.Identifier(table),
        _where_clause(connection, spec["route_column"], routes),
        ordering,
    )
    return query.as_string(connection)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--routes", help="Optional comma-separated route allowlist")
    args = parser.parse_args()
    routes = sorted({value.strip() for value in (args.routes or "").split(",") if value.strip()})

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        application_name="routespark-forecast-lab-snapshot",
        connect_timeout=15,
    )
    connection.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)

    try:
        with tempfile.TemporaryDirectory(prefix="forecast-lab-export-") as temp_name:
            temp_root = Path(temp_name)
            table_manifest = {}
            with connection.cursor() as cursor:
                for table, spec in TABLES.items():
                    query = _select_query(connection, table, spec, routes)
                    output = temp_root / f"{table}.csv"
                    copy_sql = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')"
                    with output.open("w", encoding="utf-8", newline="") as handle:
                        cursor.copy_expert(copy_sql, handle)

                    count_query = sql.SQL("SELECT COUNT(*) FROM {}{}").format(
                        sql.Identifier(table),
                        _where_clause(connection, spec["route_column"], routes),
                    )
                    cursor.execute(count_query)
                    row_count = int(cursor.fetchone()[0])
                    table_manifest[table] = {
                        "file": output.name,
                        "rows": row_count,
                        "bytes": output.stat().st_size,
                        "sha256": _sha256(output),
                        "columns": list(spec["columns"]),
                    }

                order_where = _where_clause(connection, "route_number", routes)
                cursor.execute(
                    sql.SQL(
                        "SELECT COUNT(DISTINCT route_number), MIN(delivery_date), MAX(delivery_date) "
                        "FROM orders_historical{}"
                    ).format(order_where)
                )
                route_count, first_delivery, last_delivery = cursor.fetchone()

            exported_at = datetime.now(timezone.utc)
            manifest = {
                "schema_version": "routespark-forecast-lab-snapshot-v1",
                "exported_at_utc": exported_at.isoformat(timespec="seconds"),
                "snapshot_as_of_date": exported_at.date().isoformat(),
                "database": os.environ["POSTGRES_DB"],
                "database_host_label": os.environ["POSTGRES_HOST"],
                "postgres_server_version": connection.server_version,
                "transaction_isolation": "repeatable read, read only",
                "route_filter": routes or "all",
                "route_count": int(route_count or 0),
                "first_delivery_date": first_delivery.isoformat() if first_delivery else None,
                "last_delivery_date": last_delivery.isoformat() if last_delivery else None,
                "contains_production_derived_data": True,
                "credentials_included": False,
                "tables": table_manifest,
            }
            manifest_path = temp_root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

            with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as archive:
                archive.add(manifest_path, arcname="manifest.json")
                for table in sorted(TABLES):
                    archive.add(temp_root / f"{table}.csv", arcname=f"tables/{table}.csv")
    finally:
        connection.rollback()
        connection.close()


if __name__ == "__main__":
    main()

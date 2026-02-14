#!/usr/bin/env python3
"""Migrate data from DuckDB to PostgreSQL.

This script:
1. Reads all data from existing DuckDB database
2. Creates PostgreSQL schema (if not exists)
3. Inserts all data into PostgreSQL
4. Verifies row counts match

Usage:
    python migrate_duckdb_to_postgres.py \
        --duckdb /path/to/analytics.duckdb \
        --pg-host localhost --pg-port 5432 \
        --pg-database routespark --pg-user routespark

Prerequisites:
    1. PostgreSQL database created:
       sudo -u postgres createuser -P routespark
       sudo -u postgres createdb -O routespark routespark
    
    2. pip install duckdb psycopg2-binary
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import duckdb
import psycopg2
from psycopg2.extras import execute_batch

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from pg_schema import create_schema as create_pg_schema, get_connection as get_pg_connection


# Tables to migrate in order (respecting foreign key constraints)
TABLES_TO_MIGRATE = [
    # Order data
    'orders_historical',
    'order_line_items',
    'forecast_corrections',
    
    # User params
    'store_id_aliases',
    'user_schedules',
    'stores',
    'store_items',
    'product_catalog',
    'order_guide',
    
    # Promos
    'promo_history',
    'promo_items',
    'promo_order_history',
    'promo_email_queue',
    'sap_corrections',
    
    # Calendar
    'calendar_features',
    'seasonal_adjustments',
    
    # ML
    'feature_cache',
    'model_metadata',
    'prediction_log',
    
    # Case allocation
    'item_allocation_cache',
    'store_item_shares',
    
    # Delivery
    'delivery_allocations',
    
    # Route sync
    'routes_synced',
    'sync_log',
    
    # Notifications
    'low_qty_notifications_sent',
]


def get_duckdb_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Get read-only DuckDB connection."""
    return duckdb.connect(db_path, read_only=True)


def get_table_columns(duck_conn: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    """Get column names for a table."""
    try:
        result = duck_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        return [row[1] for row in result]
    except Exception:
        return []


def fetch_all_rows(duck_conn: duckdb.DuckDBPyConnection, table: str) -> tuple:
    """Fetch all rows from a DuckDB table."""
    try:
        result = duck_conn.execute(f"SELECT * FROM {table}").fetchall()
        columns = [desc[0] for desc in duck_conn.description] if duck_conn.description else []
        return columns, result
    except Exception as e:
        print(f"  ⚠️  Error reading {table}: {e}")
        return [], []


def convert_value(val: Any) -> Any:
    """Convert DuckDB value to PostgreSQL-compatible value."""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val.decode('utf-8', errors='replace')
    return val


def migrate_table(
    duck_conn: duckdb.DuckDBPyConnection,
    pg_conn: psycopg2.extensions.connection,
    table: str,
    batch_size: int = 1000
) -> int:
    """Migrate a single table from DuckDB to PostgreSQL."""
    
    columns, rows = fetch_all_rows(duck_conn, table)
    
    if not columns or not rows:
        return 0
    
    # Build INSERT statement with ON CONFLICT DO NOTHING
    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
    # For tables with composite keys or unique constraints, handle conflicts
    insert_sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    
    # Convert rows
    converted_rows = []
    for row in rows:
        converted_rows.append(tuple(convert_value(v) for v in row))
    
    # Insert in batches
    cur = pg_conn.cursor()
    try:
        execute_batch(cur, insert_sql, converted_rows, page_size=batch_size)
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        print(f"  ❌ Error inserting into {table}: {e}")
        # Try row by row to find problematic rows
        success_count = 0
        for row in converted_rows:
            try:
                cur.execute(insert_sql, row)
                pg_conn.commit()
                success_count += 1
            except Exception:
                pg_conn.rollback()
        cur.close()
        return success_count
    
    cur.close()
    return len(converted_rows)


def verify_migration(
    duck_conn: duckdb.DuckDBPyConnection,
    pg_conn: psycopg2.extensions.connection,
    table: str
) -> tuple:
    """Verify row counts match between DuckDB and PostgreSQL."""
    try:
        duck_count = duck_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        duck_count = 0
    
    try:
        cur = pg_conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        pg_count = cur.fetchone()[0]
        cur.close()
    except Exception:
        pg_count = 0
    
    return duck_count, pg_count


def main():
    parser = argparse.ArgumentParser(description='Migrate DuckDB to PostgreSQL')
    parser.add_argument('--duckdb', required=True, help='Path to DuckDB database')
    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--pg-database', default='routespark', help='PostgreSQL database')
    parser.add_argument('--pg-user', default='routespark', help='PostgreSQL user')
    parser.add_argument('--pg-password', default=os.environ.get('PGPASSWORD', ''),
                        help='PostgreSQL password (or set PGPASSWORD env var)')
    parser.add_argument('--tables', nargs='*', help='Specific tables to migrate (default: all)')
    parser.add_argument('--verify-only', action='store_true', help='Only verify, don\'t migrate')
    parser.add_argument('--create-schema', action='store_true', help='Create PostgreSQL schema first')
    args = parser.parse_args()
    
    print("=" * 60)
    print("DuckDB → PostgreSQL Migration")
    print("=" * 60)
    print(f"Source: {args.duckdb}")
    print(f"Target: {args.pg_user}@{args.pg_host}:{args.pg_port}/{args.pg_database}")
    print()
    
    # Check DuckDB exists
    if not Path(args.duckdb).exists():
        print(f"❌ DuckDB file not found: {args.duckdb}")
        return 1
    
    # Connect to DuckDB
    try:
        duck_conn = get_duckdb_connection(args.duckdb)
        print("✅ Connected to DuckDB")
    except Exception as e:
        print(f"❌ Failed to connect to DuckDB: {e}")
        return 1
    
    # Connect to PostgreSQL
    try:
        pg_conn = get_pg_connection(
            host=args.pg_host,
            port=args.pg_port,
            database=args.pg_database,
            user=args.pg_user,
            password=args.pg_password,
        )
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        print("\nMake sure PostgreSQL is running and the database exists:")
        print(f"  sudo -u postgres createuser -P {args.pg_user}")
        print(f"  sudo -u postgres createdb -O {args.pg_user} {args.pg_database}")
        return 1
    
    # Create schema if requested
    if args.create_schema:
        print("\n📋 Creating PostgreSQL schema...")
        try:
            create_pg_schema(pg_conn)
        except Exception as e:
            print(f"❌ Failed to create schema: {e}")
            return 1
    
    # Determine tables to migrate
    tables = args.tables if args.tables else TABLES_TO_MIGRATE
    
    # Verify only mode
    if args.verify_only:
        print("\n📊 Verification (row counts)")
        print("-" * 50)
        all_match = True
        for table in tables:
            duck_count, pg_count = verify_migration(duck_conn, pg_conn, table)
            match = "✅" if duck_count == pg_count else "❌"
            if duck_count != pg_count:
                all_match = False
            print(f"  {table}: DuckDB={duck_count:,} | PostgreSQL={pg_count:,} {match}")
        
        duck_conn.close()
        pg_conn.close()
        return 0 if all_match else 1
    
    # Migrate tables
    print("\n🚀 Migrating tables...")
    print("-" * 50)
    
    total_rows = 0
    start_time = datetime.now()
    
    for table in tables:
        try:
            # Check if table exists in DuckDB
            columns, rows = fetch_all_rows(duck_conn, table)
            if not columns:
                print(f"  {table}: skipped (not in DuckDB)")
                continue
            
            row_count = migrate_table(duck_conn, pg_conn, table)
            total_rows += row_count
            print(f"  ✅ {table}: {row_count:,} rows")
            
        except Exception as e:
            print(f"  ❌ {table}: {e}")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print("-" * 50)
    print(f"\n✅ Migration complete!")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Duration: {duration:.1f}s")
    
    # Verify
    print("\n📊 Verification...")
    print("-" * 50)
    
    mismatches = []
    for table in tables:
        duck_count, pg_count = verify_migration(duck_conn, pg_conn, table)
        if duck_count > 0 or pg_count > 0:
            match = "✅" if duck_count == pg_count else "⚠️"
            if duck_count != pg_count:
                mismatches.append((table, duck_count, pg_count))
            print(f"  {table}: {pg_count:,}/{duck_count:,} {match}")
    
    if mismatches:
        print("\n⚠️  Row count mismatches (may be due to conflicts):")
        for table, duck, pg in mismatches:
            print(f"    {table}: expected {duck:,}, got {pg:,}")
    
    duck_conn.close()
    pg_conn.close()
    
    print("\n" + "=" * 60)
    print("Migration finished. Next steps:")
    print("  1. Update services to use db_manager_pg.py")
    print("  2. Update docker-compose.yml for PostgreSQL")
    print("  3. Test all endpoints")
    print("=" * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
